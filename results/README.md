# Results

All files are produced by the tools in `src/consumers` and `src/producer`. Nothing here is edited by hand.

| File / folder | Produced by | Content |
|---|---|---|
| `ingest_load.json` | `make load` | Initial TPC-H sf1 load: rows, PostgreSQL load time, seconds until the data is queryable in Iceberg (checksum-verified), end-to-end rows/s |
| `freshness.json` | `make freshness` | 40 probe rows inserted under a 100 ops/s workload. Each one is timed from PostgreSQL commit until it is visible in Trino. |
| `benchmark-<timestamp>/` | `make optimize` | Before/after the Airflow `iceberg_layout_optimization` DAG. Contains `results.json` (raw), `summary.md`, and the charts `query_latency.png`, `small_files.png`, `storage.png`, `ingestion.png` and `freshness.png`. |
| `ablation-compacted-vs-monthly-partitions/` | `make optimize` on an already compacted table | Same data, one compact file vs monthly partitions. Shows the partition-granularity trade-off discussed in the report (§5.3). |
| `freshness-with-spark-restart.json` | `make freshness` | An earlier freshness run during which the Docker VM ran out of memory and killed the Spark driver. The job recovered from its checkpoint with no data loss. Kept as evidence for the fault-tolerance section (§5.5). Memory limits were reduced afterwards. |
| `maintenance/` | Airflow DAGs | Per-task JSON reports of the Spark maintenance jobs (not committed) |

The final measurements were taken on one laptop (Apple silicon, 8 CPU cores, Docker limited to 8 GB of RAM) with the configuration committed in this repository.
