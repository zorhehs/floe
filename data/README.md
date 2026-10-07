# Data

No large files are stored in the repository; all data is generated reproducibly.

| Dataset | Source | How it gets into the pipeline |
|---|---|---|
| TPC-H `region`, `nation`, `customer`, `orders` | Trino `tpch` connector (official TPC-H dbgen algorithm), scale `tiny` (15k orders) or `sf1` (1.5M orders) | `make load` → INSERT into PostgreSQL → Debezium CDC |
| Synthetic OLTP workload | `src/producer/generator.py` (inserts, updates, deletes at a configurable rate) | `make workload-start` |
| Heartbeat / freshness probes | `shop.cdc_probe`, written by the exporter and the benchmark | automatic |

`sample_orders.csv` is a 20-row extract of the TPC-H `tiny` orders table, included so readers can see the data shape without running the stack.
