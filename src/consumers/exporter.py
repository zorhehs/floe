"""
Prometheus exporter for pipeline health (scraped by Prometheus, shown in Grafana).

Every --interval seconds it:
  * updates a heartbeat row (shop.cdc_probe, probe_id = 0) in PostgreSQL and
    reports how old the newest heartbeat visible in Iceberg is  -> true
    end-to-end CDC freshness, continuously
  * reads replication-slot lag (WAL bytes Debezium has not confirmed yet)
  * reads Debezium connector/task state from Kafka Connect
  * reads per-table Iceberg file statistics (data / delete files, bytes,
    snapshots) and row counts vs. PostgreSQL through Trino
  * summarises the streaming job's recent micro-batches (ops.ingest_batches)

Each source is collected independently: one failing source sets
lakehouse_exporter_up{source=...} to 0 but never stops the others.
"""

from __future__ import annotations

import argparse
import logging
import os
import time

import requests
from prometheus_client import Gauge, start_http_server

from common.db import LAKE, Trino, pg_connect

log = logging.getLogger("exporter")
TABLES = ["orders", "customer", "nation", "region", "cdc_probe"]
CONNECT_URL = os.getenv("CONNECT_URL", "http://connect:8083")

UP = Gauge("lakehouse_exporter_up", "1 if the last collection from a source succeeded", ["source"])
FRESHNESS = Gauge("lakehouse_cdc_freshness_seconds",
                  "Age of the newest PostgreSQL heartbeat visible in Iceberg (end-to-end CDC lag)")
SLOT_LAG = Gauge("lakehouse_replication_slot_lag_bytes", "WAL bytes not yet confirmed by Debezium", ["slot"])
SLOT_ACTIVE = Gauge("lakehouse_replication_slot_active", "1 if the replication slot is in use", ["slot"])
CONNECTOR = Gauge("lakehouse_connector_running", "1 if the connector and all its tasks are RUNNING", ["connector"])
PG_ROWS = Gauge("lakehouse_source_rows", "Live rows in PostgreSQL (pg_stat estimate)", ["table"])
PG_TX = Gauge("lakehouse_source_commits_total", "Committed transactions in the source database")
LAKE_ROWS = Gauge("lakehouse_iceberg_rows", "Rows in the current Iceberg snapshot", ["table"])
FILES = Gauge("lakehouse_iceberg_files", "Live files in the current snapshot", ["table", "content"])
BYTES = Gauge("lakehouse_iceberg_bytes", "Bytes of live files in the current snapshot", ["table", "content"])
SNAPSHOTS = Gauge("lakehouse_iceberg_snapshots", "Snapshots retained in table metadata", ["table"])
AVG_FILE = Gauge("lakehouse_iceberg_avg_data_file_bytes", "Average data file size", ["table"])
INGEST_RATE = Gauge("lakehouse_ingest_records_per_second", "CDC records merged per second (last 5 minutes)")
INGEST_BATCHES = Gauge("lakehouse_ingest_batches_5m", "Micro-batches committed in the last 5 minutes")
MERGE_MS = Gauge("lakehouse_ingest_merge_ms", "MERGE duration of the latest batch", ["table"])
LAST_BATCH_AGE = Gauge("lakehouse_ingest_last_batch_age_seconds", "Seconds since the last committed micro-batch")


def guarded(source: str):
    def wrap(fn):
        def inner(*a, **kw):
            try:
                fn(*a, **kw)
                UP.labels(source).set(1)
            except Exception as exc:  # noqa: BLE001 - keep exporting other sources
                UP.labels(source).set(0)
                log.warning("%s collection failed: %s", source, str(exc).splitlines()[0][:200])
        return inner
    return wrap


@guarded("postgres")
def collect_postgres():
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO shop.cdc_probe (probe_id) VALUES (0) "
                    "ON CONFLICT (probe_id) DO UPDATE SET created_at = clock_timestamp()")
        cur.execute("SELECT slot_name, active, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) "
                    "FROM pg_replication_slots")
        for slot, active, lag in cur.fetchall():
            SLOT_ACTIVE.labels(slot).set(1 if active else 0)
            SLOT_LAG.labels(slot).set(float(lag or 0))
        cur.execute("SELECT relname, n_live_tup FROM pg_stat_user_tables WHERE schemaname = 'shop'")
        for table, rows in cur.fetchall():
            PG_ROWS.labels(table).set(rows)
        cur.execute("SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()")
        PG_TX.set(cur.fetchone()[0])


@guarded("kafka_connect")
def collect_connect():
    names = requests.get(f"{CONNECT_URL}/connectors", timeout=10).json()
    for name in names:
        st = requests.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=10).json()
        ok = st["connector"]["state"] == "RUNNING" and all(t["state"] == "RUNNING" for t in st["tasks"])
        CONNECTOR.labels(name).set(1 if ok and st["tasks"] else 0)


@guarded("iceberg")
def collect_iceberg(t: Trino):
    present = {r[0] for r in t.rows("SELECT table_name FROM lakehouse.information_schema.tables "
                                    "WHERE table_schema = 'shop'")}
    if "cdc_probe" in present:
        age = t.scalar(f"SELECT date_diff('millisecond', max(created_at), current_timestamp) / 1000.0 "
                       f"FROM {LAKE}.cdc_probe WHERE probe_id = 0")
        if age is not None:
            FRESHNESS.set(float(age))
    for table in TABLES:
        if table not in present:
            continue
        LAKE_ROWS.labels(table).set(t.scalar(f"SELECT count(*) FROM {LAKE}.{table}"))
        by = {c: (n, b) for c, n, b in t.rows(
            f'SELECT content, count(*), coalesce(sum(file_size_in_bytes), 0) FROM {LAKE}."{table}$files" '
            "GROUP BY content")}
        for content, label in ((0, "data"), (1, "position_deletes"), (2, "equality_deletes")):
            n, b = by.get(content, (0, 0))
            FILES.labels(table, label).set(n)
            BYTES.labels(table, label).set(b)
        n, b = by.get(0, (0, 0))
        AVG_FILE.labels(table).set(b / n if n else 0)
        SNAPSHOTS.labels(table).set(t.scalar(f'SELECT count(*) FROM {LAKE}."{table}$snapshots"'))


@guarded("ingest")
def collect_ingest(t: Trino):
    rate, batches, age = t.rows("""
        SELECT coalesce(sum(kafka_records), 0) / 300.0, count(DISTINCT batch_id),
               date_diff('millisecond', max(batch_finished_at), current_timestamp) / 1000.0
        FROM lakehouse.ops.ingest_batches
        WHERE batch_finished_at > current_timestamp - INTERVAL '5' MINUTE""")[0]
    INGEST_RATE.set(float(rate))
    INGEST_BATCHES.set(batches)
    if age is None:
        age = t.scalar("SELECT date_diff('millisecond', max(batch_finished_at), current_timestamp) / 1000.0 "
                       "FROM lakehouse.ops.ingest_batches")
    if age is not None:
        LAST_BATCH_AGE.set(float(age))
    for table, ms in t.rows("""
        SELECT table_name, max_by(merge_ms, batch_id) FROM lakehouse.ops.ingest_batches
        WHERE batch_finished_at > current_timestamp - INTERVAL '1' HOUR GROUP BY table_name"""):
        MERGE_MS.labels(table).set(ms)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=9108)
    p.add_argument("--interval", type=float, default=15)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    start_http_server(args.port)
    log.info("exporter listening on :%d, interval %ss", args.port, args.interval)
    trino = None
    while True:
        started = time.time()
        collect_postgres()
        collect_connect()
        try:
            trino = trino or Trino()
            collect_iceberg(trino)
            collect_ingest(trino)
        except Exception as exc:  # noqa: BLE001 - reconnect next round
            log.warning("trino unavailable: %s", exc)
            trino = None
        time.sleep(max(1.0, args.interval - (time.time() - started)))


if __name__ == "__main__":
    main()
