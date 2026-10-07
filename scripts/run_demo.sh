#!/usr/bin/env bash
# End-to-end demo: start the stack, load TPC-H into PostgreSQL, stream a live
# OLTP workload through Debezium/Kafka/Spark into Iceberg, verify correctness
# and run sample Trino queries.
#
#   ./scripts/run_demo.sh            # quick demo (TPC-H tiny, 60 s workload)
#   ./scripts/run_demo.sh --full     # TPC-H sf1 + all demos + full benchmark
set -euo pipefail
cd "$(dirname "$0")/.."

FULL=false
[[ "${1:-}" == "--full" ]] && FULL=true
SCALE=$($FULL && echo sf1 || echo tiny)
step() { printf '\n\033[1;34m### %s\033[0m\n' "$*"; }

step "1/7 Setup (prerequisites, jars, images)"
./scripts/setup.sh

step "2/7 Start the stack"
docker compose up -d
echo "waiting for Debezium connector + Spark streaming job ..."
for _ in $(seq 1 90); do
  state=$(docker compose ps -a --format '{{.Service}} {{.State}} {{.ExitCode}}' | awk '$1=="connect-init"{print $2, $3}')
  [[ "$state" == "exited 0" ]] && break
  sleep 5
done
[[ "$state" == "exited 0" ]] || { echo "Debezium connector registration failed:"; docker compose logs connect-init; exit 1; }

step "3/7 Load TPC-H ($SCALE) into PostgreSQL and wait until it is queryable in Iceberg"
docker compose --profile tools run --rm -e TPCH_SCALE="$SCALE" tools python3 -m producer.load_tpch

step "4/7 Live OLTP workload (inserts / updates / deletes) for 60 s"
docker compose --profile workload up -d generator
sleep 60
docker compose --profile workload stop generator

step "5/7 Verify: PostgreSQL vs Iceberg (row counts + checksums)"
docker compose --profile tools run --rm tools python3 -m consumers.validate --wait 600

step "6/7 Sample Trino queries on the lakehouse"
for q in q02_single_month q04_revenue_by_nation; do
  echo "-- $q"
  docker compose exec -T trino trino --output-format ALIGNED --file "/opt/queries/$q.sql"
done
docker compose exec -T trino trino --output-format ALIGNED --execute \
  "SELECT committed_at, operation, element_at(summary, 'added-records') AS added FROM lakehouse.shop.\"orders\$snapshots\" ORDER BY committed_at DESC LIMIT 5"

if $FULL; then
  step "7/7 Demos + benchmark"
  ./scripts/demos/schema_evolution.sh
  ./scripts/demos/time_travel.sh
  ./scripts/demos/gdpr_delete.sh
  make bench
else
  step "7/7 Done"
fi
cat <<'MSG'

Explore:
  Grafana    http://localhost:3000   (pipeline dashboard)
  Trino      http://localhost:8080   (make trino for the CLI)
  Airflow    http://localhost:8088   (admin / admin)
  MinIO      http://localhost:9001   (admin / password)
  Spark UI   http://localhost:4040
Stop with ./scripts/cleanup.sh (add --volumes to delete all data).
MSG
