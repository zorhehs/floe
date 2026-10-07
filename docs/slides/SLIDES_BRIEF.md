# Brief: presentation slides (for Cowork / any slide author)

Build a **12–14 slide** deck (PPTX, plus a PDF export) for the SE-315 Cloud Computing semester project. Save the outputs as `docs/slides/floe.pptx` and `docs/slides/floe.pdf` in this repository.

## Required content (course rubric)
The course requires these sections:
* problem statement
* solution architecture
* technical challenges
* results & metrics
* future work

The deck supports a 5–8 minute talk and demo, so keep each slide to about 3–5 bullets or a single visual.

## Facts to use (read these files and do not invent numbers)
| Source | Use for |
|---|---|
| `README.md` | Overview, components, deliverables |
| `docs/architecture.md` | Architecture diagram (re-draw the mermaid flow as a clean diagram), design-decision table, failure handling, cloud mapping |
| `results/benchmark-*/summary.md` + its PNG charts (latest folder) | **All performance numbers and charts**: query latency before/after, small files, storage, compaction overhead, ingestion, freshness |
| `results/ingest_load.json`, `results/freshness.json` | Ingestion throughput and CDC freshness |
| `docs/DEMO_SCRIPT.md` | Order of the live demo |
| Section "Technical challenges" below | Challenges slide(s) |

If `results/benchmark-*` does not exist yet, stop and say so. Do not use placeholder numbers.

## Suggested outline
1. **Title.**
   * Product name **Floe**, subtitle "A CDC lakehouse on Apache Iceberg, Spark and Trino" (the course project title, "Lakehouse Architecture with Apache Iceberg, Spark and Trino using CDC Ingestion", in small text).
   * Group 04: Aliyan Zulfiqar (leader) and Sehrosh Riaz.
   * Course and term: SE-315 Cloud Computing, NBC, Department of Computer Science.
2. **Problem statement.** Analytics on live OLTP data without loading the operational database. Traditional batch ETL is stale and copies whole tables. We need fresh, correct, cheap-to-query history.
3. **Goals and metrics.**
   * Goals: CDC freshness, ingestion throughput, query latency, small-file and storage reduction, correctness.
   * Datasets: TPC-H sf1 (1.65M rows) and a synthetic OLTP workload.
4. **Architecture.** A diagram of:
   * PostgreSQL → Debezium (Kafka Connect) → Kafka → Spark Structured Streaming → Iceberg (REST catalog + MinIO/S3) → Trino
   * Airflow running maintenance jobs
   * Prometheus/Grafana for monitoring
5. **CDC and ingestion design.**
   * Logical replication with pgoutput.
   * One topic per table.
   * `foreachBatch` handles schema discovery, dedupe by key and a single MERGE.
   * Merge-on-read with an idempotent MERGE, giving effectively-once results.
6. **Schema evolution and time travel.**
   * ADD COLUMN and int→bigint widening flow through automatically.
   * Snapshots support `FOR VERSION/TIMESTAMP AS OF`.
7. **Optimisation with Airflow + Spark.**
   * Partition evolution to `months(o_orderdate)`, plus a sort order.
   * Sorted compaction.
   * Rewrites of delete files and manifests.
   * Snapshot expiry and orphan cleanup.
8. **GDPR erasure (extra credit).** A merge-on-read delete still leaves the data reachable through time travel. The purge DAG removes it physically.
9. **Monitoring.**
   * Use a Grafana dashboard screenshot if one is available in `docs/slides/` or `docs/report/`. Otherwise describe the panels.
   * Show the alert rules.
10. **Results: query latency** (chart plus the headline speed-up).
11. **Results: small files and storage** (charts).
12. **Results: ingestion and freshness** (throughput rows/s, freshness p50/p95), plus correctness (checksums match).
13. **Technical challenges** (see below).
14. **Conclusion and future work.** Hudi vs Iceberg comparison, Apache Atlas lineage, Kubernetes/AKS deployment, Avro + Schema Registry, Flink.

## Technical challenges actually encountered (use these, they are real)
| Challenge | Fix |
|---|---|
| The REST catalog on SQLite failed under concurrent commits (`SQLITE_BUSY`, HTTP 500) | Moved catalog metadata to a PostgreSQL JDBC store |
| Spark MERGE failed on Iceberg tables with identifier fields (Iceberg 1.9) | Primary key stored as a table property instead |
| Iceberg `remove_orphan_files` needs a Hadoop S3 filesystem, which the stack doesn't have | Supplied the S3 file listing through `file_list_view` (boto3). Iceberg enforces a 24 h minimum age. |
| Trino's default per-node memory (432 MB) was too small for the 1.5M-row joins | Tuned the memory limits and enabled spill-to-disk |
| The sorted rewrite of 1.5M rows ran out of Java heap in a 1 GB Spark driver | Resized the maintenance job and container memory |
| Debezium JSON events carry their schema (about 3 KB per message) | zstd compression in Kafka, and schema discovery by string prefix instead of a full JSON parse |
| MinIO's official Docker images were withdrawn in 2025 | Used a maintained community build |
| The whole stack had to fit in Docker's 8 GB of memory | Per-service memory limits; local-mode Spark |

## Style — must match the technical report (`docs/report/report.pdf`), NOT a generic blue/white template
* **Palette:** near-black `#0d0e10`, ink `#121417`, greys `#6b7280` / `#e3e6ea` / panel `#f3f4f6`, white; ONE accent
  `#3b8ff3` used sparingly (short underline under titles, one highlight panel, chart highlight). Green `#2f9e44` only for
  ✓ PASS, red `#d6336c` only for regressions. No navy-filled table headers, no blue gradients.
* **Type:** Avenir Next (titles bold, subtitles light), big numerals in DIN Alternate Bold; generous whitespace.
* **Title & section-divider slides:** full-bleed near-black background, large white bold title + light subtitle, short
  accent rule, small page number with an accent line at the bottom-left (like the report cover).
* **Content slides:** white background, bold black headline with a 20 mm accent underline; tables with small grey
  uppercase labels and hairline rules; grey rounded info panels; results as a split hero block (accent panel with one big
  number + black panel listing 3–4 headline numbers); speed-ups as thin stat bars (grey track, black fill, red if < 1×).
* **Charts:** reuse the PNGs in `results/benchmark-*/` (grey = before, black = after) — they already match.
* Every number must come from the results files; add 2–4 sentence speaker notes per slide.
