# Lakehouse CDC project - common commands. Run `make help`.
SHELL := /bin/bash
DC    := docker compose
TOOLS := $(DC) --profile tools run --rm tools python3 -m

.DEFAULT_GOAL := help
.PHONY: help jars pull build up down clean status load workload-start workload-stop validate \
        freshness optimize bench report demo-schema demo-timetravel demo-gdpr trino psql \
        logs maintenance-now test

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

jars: ## Download + verify the Spark extension jars (resumable)
	./scripts/fetch-jars.sh

pull: ## Pull third-party images (retries on flaky networks)
	./scripts/pull-images.sh

build: jars ## Build the Spark / tools / Airflow images
	$(DC) --profile tools build

up: jars ## Start the whole stack (Postgres, Kafka, Debezium, MinIO, Iceberg REST, Spark, Trino, Airflow)
	$(DC) --profile tools build
	$(DC) up -d
	@echo "Grafana http://localhost:3000 | Trino http://localhost:8080 | Airflow http://localhost:8088 | MinIO http://localhost:9001 | Spark UI http://localhost:4040"

down: ## Stop the stack (keeps data)
	$(DC) --profile tools --profile workload down

clean: ## Stop the stack and DELETE all data volumes
	$(DC) --profile tools --profile workload down -v

status: ## Containers + Debezium connector status + Kafka topics
	@$(DC) ps --format 'table {{.Service}}\t{{.Status}}'
	@echo; curl -s http://localhost:8083/connectors/postgres-cdc/status | python3 -m json.tool || true
	@echo; $(DC) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

load: ## Load TPC-H into PostgreSQL and time how long until it is queryable in Iceberg
	$(TOOLS) producer.load_tpch

workload-start: ## Start the synthetic OLTP workload (inserts/updates/deletes)
	$(DC) --profile workload up -d generator

workload-stop: ## Stop the synthetic OLTP workload
	$(DC) --profile workload stop generator

validate: ## Compare PostgreSQL and Iceberg (row counts + checksums), waiting up to 10 min
	$(TOOLS) consumers.validate --wait 600

freshness: ## Measure CDC end-to-end freshness (runs the workload during the test)
	$(DC) --profile workload up -d generator
	sleep 20
	$(TOOLS) consumers.bench freshness --probes 40 --interval 2

optimize: ## Before/after benchmark around the Airflow optimisation DAG (stops the workload)
	-$(DC) --profile workload stop generator
	$(TOOLS) consumers.bench optimize --runs 5

bench: freshness optimize ## Full evaluation: freshness, then before/after optimisation

report: ## Rebuild charts + summary.md of the latest benchmark
	$(TOOLS) consumers.bench report

maintenance-now: ## Run the hourly maintenance job once, right now (spark-submit in Airflow)
	$(DC) exec airflow spark-submit --master 'local[2]' --driver-memory 1200m --conf 'spark.driver.extraJavaOptions=-Daws.region=us-east-1 -XX:MaxDirectMemorySize=256m -XX:MaxMetaspaceSize=256m' /opt/jobs/maintenance.py \
	  --compact --rewrite-deletes --rewrite-manifests --expire --orphans

demo-schema: ## Demo: schema evolution (add column, widen type) flowing into Iceberg
	./scripts/demos/schema_evolution.sh

demo-timetravel: ## Demo: Iceberg snapshots and time travel
	./scripts/demos/time_travel.sh

demo-gdpr: ## Demo: GDPR erasure (row-level delete, merge-on-read, physical purge)
	./scripts/demos/gdpr_delete.sh

trino: ## Interactive Trino CLI
	$(DC) exec trino trino --catalog lakehouse --schema shop

psql: ## Interactive psql on the source database
	$(DC) exec postgres psql -U app -d oltp

logs: ## Follow the Spark streaming job logs
	$(DC) logs -f spark-streaming

test: ## Unit tests (pytest)
	python3 -m pytest -q tests/unit

test-integration: ## Full pipeline integration test (stack must be built)
	./tests/integration/test_pipeline.sh
