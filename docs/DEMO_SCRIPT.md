# Demo video script (target 7 minutes)

Before recording:
1. Run `make up && make load`, then open the **Floe Console** at http://localhost:8090.
2. Start the workload about 2 minutes before recording, so the charts have data: on the Overview page click **Start** at 100 ops/s.
3. Run `make bench` once, so the Benchmark page has results.
4. Use full-screen browser zoom at 100 %. Keep one terminal ready for the `make` fallbacks.

| Time | Show | Say (key points) |
|---|---|---|
| 0:00–0:40 | README architecture diagram, then the console **Overview** | Problem: analytics on live OLTP data without loading the database. The pipeline runs PostgreSQL → Debezium → Kafka → Spark → Iceberg on S3 (MinIO) → Trino, with Airflow for optimisation. The console checks all 10 components live. |
| 0:40–1:30 | **Overview**: KPI cards, freshness and ingest charts, the workload card, recent micro-batches | Debezium reads the WAL through a logical replication slot, one topic per table. The workload issues about 100 inserts, updates and deletes per second. Each Spark micro-batch dedupes per key, then runs one MERGE: point at the events and MERGE time per batch. |
| 1:30–2:40 | **Live CDC**: update a customer's balance and name, then insert an order and delete one | The trace shows the PostgreSQL commit, the WAL position, the micro-batch that merged it and the moment Trino sees it, with the measured latency. Both rows are shown side by side, including the CDC metadata columns. |
| 2:40–3:20 | **Validation**: run with "wait until consistent" | Trino compares PostgreSQL and Iceberg on row counts and an order-independent checksum of every column. The copy is identical, not just close. |
| 3:20–4:10 | **Tables & schema**: add column `loyalty_tier`, then widen `o_shippriority` to bigint | An `ALTER TABLE` in PostgreSQL reaches Iceberg as a new schema version, with no restart and no data rewritten. Old rows read NULL (schema-on-read). |
| 4:10–4:50 | **Time travel**: open a snapshot, then the history of the customer changed in the Live CDC step | Every micro-batch is a snapshot. Compare a snapshot with now, open it in the SQL workbench (`FOR VERSION AS OF`), and show the highlighted versions of one row. |
| 4:50–5:30 | **GDPR erasure**: show the customer, erase, then run the purge (or `make demo-gdpr`) | The delete arrives as a position-delete file: the row disappears from every query within seconds, but `FOR VERSION AS OF` still returns the name and phone. The purge DAG rewrites the files and expires history; the same query now fails because the snapshot no longer exists. |
| 5:30–6:30 | **Maintenance** (file layout table, a DAG run's tasks), then **Benchmark** | Streaming creates small files and delete files. After partitioning by month, sorting and compaction there are 105 → 0 delete files, 89 % fewer objects, and the query suite runs 3.25× faster. Q06 got slower and why. |
| 6:30–7:00 | **SQL workbench**: run Q04, then Grafana | Every result comes from Trino over Iceberg, read-only for the workbench user. Close with p50 7.3 s freshness, 4,844 rows/s ingest, the limitations, and future work (a Hudi comparison, lineage). |
