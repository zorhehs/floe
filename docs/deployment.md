# Deployment guide

## Requirements

* Docker Desktop 4.x (macOS / Windows) or Docker Engine 24+ with the compose plugin (Linux).
* **Memory:** at least 8 GB for Docker; 10–12 GB recommended (Docker Desktop → Settings → Resources).
* **Disk:** about 10 GB free for images and data.
* `make`, `bash` and `curl`. On Windows, use WSL2.
* Free ports: 3000, 4040, 5433, 8080, 8083, 8088, 8181, 9000, 9001, 9090, 29092.

## Step by step

```bash
git clone <repo-url> floe && cd floe
./scripts/setup.sh        # .env from .env.example, Spark jars (verified), images, builds
./scripts/run_demo.sh     # stack up, TPC-H tiny load, live workload, validation, sample queries
```

The same steps with `make`:

| Step | Command | Expected result |
|---|---|---|
| 1 | `make up` | All services running. `make status` shows the `postgres-cdc` connector `RUNNING` and topics `pg.shop.*`. |
| 2 | `make load` | Prints rows loaded and `CONSISTENT: N rows queryable in Iceberg after X s`. |
| 3 | `make workload-start` | The Grafana dashboard shows ingest rate and freshness. |
| 4 | `make validate` | `[OK]` for every table. |
| 5 | `make demo-schema`, `make demo-timetravel`, `make demo-gdpr` | Scripted demonstrations. |
| 6 | `make bench` | `results/benchmark-<ts>/summary.md` with charts. |
| 7 | `./scripts/cleanup.sh [--volumes]` | Stops the stack and optionally deletes all data. |

## Configuration (`.env`)

| Variable | Default | Meaning |
|---|---|---|
| `TPCH_SCALE` | `sf1` | `tiny` (15k orders) or `sf1` (1.5M orders) |
| `TRIGGER_INTERVAL` | `10 seconds` | Spark micro-batch interval: lower means fresher data but more small files |
| `MAX_OFFSETS_PER_TRIGGER` | `100000` | Upper bound on the records per micro-batch (back-pressure) |
| `WORKLOAD_OPS_PER_SEC` | `100` | Generator rate |
| Credentials | see `.env.example` | Local development only; change them for any shared deployment |

## Troubleshooting

| Symptom | Fix |
|---|---|
| A container is restarting with exit code 137 | Out of memory. Give Docker more memory, or set `TPCH_SCALE=tiny`. |
| `connect-init` failed | Run `docker compose logs connect-init`. Usually PostgreSQL wasn't ready yet; re-run `docker compose up -d connect-init`. |
| Validation says `NOT CONSISTENT` while the generator runs | Expected: the lake trails the source by a few seconds. Stop the generator and validate again. |
| Image pulls fail on a slow network | `make pull` retries every image until it succeeds. `make jars` resumes partial downloads. |
| Start completely fresh | `./scripts/cleanup.sh --volumes && make up` |

## Cloud deployment notes

The repository targets local reproducibility (Docker Compose). For a cloud run, use the mapping in `docs/architecture.md` ("Scalability"):

1. Point the Spark catalog at ADLS / S3:
   * set `spark.sql.catalog.lakehouse.warehouse`;
   * remove the `s3.endpoint` setting;
   * use a managed identity or IAM role.
2. Use a managed REST catalog, or keep `iceberg-rest` backed by a PostgreSQL JDBC catalog.
3. Swap the bootstrap servers for Event Hubs / MSK, and the PostgreSQL host for the managed instance. Logical replication must be enabled on that instance.
