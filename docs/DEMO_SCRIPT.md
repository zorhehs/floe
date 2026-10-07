# Demo video script (target 7 minutes)

Prepare beforehand: `make up && make load`, start `make workload-start` ~2 minutes before
recording, and run `make bench` once so a report exists. Use a large terminal font. Open
browser tabs for Airflow, MinIO, Spark UI and the latest `reports/benchmark-*/summary.md`.

| Time | Show | Say (key points) |
|---|---|---|
| 0:00–0:40 | README architecture diagram | Problem: analytics on live OLTP data without loading the database. Pipeline is PostgreSQL → Debezium → Kafka → Spark → Iceberg on S3 (MinIO) → Trino, with Airflow for optimisation. |
| 0:40–1:30 | `make status`; Kafka Connect status JSON; `docker compose logs generator` | Debezium reads the WAL through a logical replication slot; one topic per table; the generator issues about 100 inserts/updates/deletes per second. |
| 1:30–2:30 | `make logs` (Spark batches) + Spark UI | Each micro-batch does schema discovery → dedupe per key → one MERGE. Merge-on-read writes only small delete files. Point at the records/s and merge time per batch in the log. |
| 2:30–3:10 | `make trino`: `SELECT count(*) FROM orders;` twice, a few seconds apart; then `make validate` | The data is live, and Postgres and Iceberg checksums match, so the copy is correct and not just close. |
| 3:10–4:10 | `make demo-schema` | `ALTER TABLE` in Postgres shows up in Iceberg automatically: a new column, then int → bigint, with no restart and no rewrite. Old rows read NULL (schema-on-read). |
| 4:10–4:50 | `make demo-timetravel` | Every batch is a snapshot. Query the order now, as of a snapshot id, and as of a timestamp, then do a change audit with EXCEPT. |
| 4:50–5:40 | `make demo-gdpr` | The delete arrives as a position-delete file and the row disappears, but time travel still exposes it. The purge DAG rewrites the files and expires history, after which the data is truly erased. |
| 5:40–6:40 | Airflow UI (DAG graph + task durations), then `summary.md` charts | Streaming produced many small files plus delete files. After partitioning by month, sorting and compaction, show the file count, storage and query-latency charts, and quote the speed-ups and compaction cost. |
| 6:40–7:00 | Freshness histogram + summary | Quote the p50/p95 commit-to-query freshness and the throughput. Close with the limitations and future work (Hudi comparison, Atlas lineage). |
