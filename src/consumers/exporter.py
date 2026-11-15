"""
Prometheus exporter for pipeline health (scraped by Prometheus, shown in Grafana).

Two collection loops, so slow Iceberg metadata queries never delay the freshness signal:

  fast (every --interval, 15 s by default)
    * writes a heartbeat row (shop.cdc_probe, probe_id = 0) in PostgreSQL and reports how
      old the newest heartbeat visible in Iceberg is  -> true end-to-end CDC freshness
    * replication-slot lag (WAL bytes Debezium has not confirmed yet) and source commit rate
    * Debezium connector/task state from Kafka Connect
  slow (every --slow-interval, 60 s by default, in its own thread)
    * per-table Iceberg file statistics (data / delete files, bytes, snapshots) and row
      counts, each as ONE Trino query across all tables instead of one per table
    * a summary of the streaming job's recent micro-batches (ops.ingest_batches)

Each source is collected independently: one failing source sets
lakehouse_exporter_up{source=...} to 0 but never stops the others.
"""

from __future__ import annotations

import argparse
import logging
import os
import threading
import time

import psycopg
import requests
from prometheus_client import Gauge, start_http_server

from common.db import LAKE, Trino, pg_connect

log = logging.getLogger("exporter")
CONNECT_URL = os.getenv("CONNECT_URL", "http://connect:8083")

UP = Gauge("lakehouse_exporter_up", "1 if the last collection from a source succeeded", ["source"])
COLLECT_SECONDS = Gauge("lakehouse_exporter_collect_seconds", "Wall time of the last collection", ["source"])
FRESHNESS = Gauge("lakehouse_cdc_freshness_seconds",
                  "Age of the newest PostgreSQL heartbeat visible in Iceberg (end-to-end CDC lag)")
SLOT_LAG = Gauge("lakehouse_replication_slot_lag_bytes", "WAL bytes not yet confirmed by Debezium", ["slot"])
SLOT_ACTIVE = Gauge("lakehouse_replication_slot_active", "1 if the replication slot is in use", ["slot"])
CONNECTOR = Gauge("lakehouse_connector_running", "1 if the connector and all its tasks are RUNNING", ["connector"])
PG_ROWS = Gauge("lakehouse_source_rows", "Live rows in PostgreSQL (pg_stat estimate, lags autovacuum)", ["table"])
PG_CHANGES = Gauge("lakehouse_source_row_changes_total",
                   "Rows inserted, updated or deleted in the source schema (pg_stat_user_tables)")
LAKE_ROWS = Gauge("lakehouse_iceberg_rows", "Rows in the current Iceberg snapshot", ["table"])
FILES = Gauge("lakehouse_iceberg_files", "Live files in the current snapshot", ["table", "content"])
BYTES = Gauge("lakehouse_iceberg_bytes", "Bytes of live files in the current snapshot", ["table", "content"])
SNAPSHOTS = Gauge("lakehouse_iceberg_snapshots", "Snapshots retained in table metadata", ["table"])
AVG_FILE = Gauge("lakehouse_iceberg_avg_data_file_bytes", "Average data file size", ["table"])
INGEST_RATE = Gauge("lakehouse_ingest_records_per_second",
                    "CDC change events (Kafka records) processed per second, last 5 minutes")
INGEST_BATCHES = Gauge("lakehouse_ingest_batches_5m", "Micro-batches committed in the last 5 minutes")
MERGE_MS = Gauge("lakehouse_ingest_merge_ms", "MERGE duration of the latest batch", ["table"])
LAST_BATCH_AGE = Gauge("lakehouse_ingest_last_batch_age_seconds", "Seconds since the last committed micro-batch")
CONTENT = {0: "data", 1: "position_deletes", 2: "equality_deletes"}


def guarded(source: str):
    def wrap(fn):
        def inner(*a, **kw):
            t0 = time.perf_counter()
            try:
                fn(*a, **kw)
                UP.labels(source).set(1)
            except Exception as exc:  # noqa: BLE001 - keep exporting other sources
                UP.labels(source).set(0)
                log.warning("%s collection failed: %s", source, str(exc).splitlines()[0][:200])
            finally:
                COLLECT_SECONDS.labels(source).set(round(time.perf_counter() - t0, 3))
        return inner
    return wrap


class Pg:
    """One PostgreSQL connection, reopened on error instead of per cycle."""

    def __init__(self):
        self.conn: psycopg.Connection | None = None

    def cursor(self):
        if self.conn is None or self.conn.closed:
            self.conn = pg_connect()
        return self.conn.cursor()

    def reset(self):
        try:
            if self.conn is not None:
                self.conn.close()
        finally:
            self.conn = None


PG = Pg()


@guarded("postgres")
def collect_postgres():
    try:
        with PG.cursor() as cur:
            cur.execute("INSERT INTO shop.cdc_probe (probe_id) VALUES (0) "
                        "ON CONFLICT (probe_id) DO UPDATE SET created_at = clock_timestamp()")
            cur.execute("SELECT slot_name, active, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) "
                        "FROM pg_replication_slots")
            for slot, active, lag in cur.fetchall():
                SLOT_ACTIVE.labels(slot).set(1 if active else 0)
                SLOT_LAG.labels(slot).set(float(lag or 0))
            cur.execute("SELECT relname, n_live_tup, n_tup_ins + n_tup_upd + n_tup_del "
                        "FROM pg_stat_user_tables WHERE schemaname = 'shop'")
            changes = 0
            for table, rows, changed in cur.fetchall():
                PG_ROWS.labels(table).set(rows)
                changes += changed
            # xact_commit would count every read-only SELECT (ours included): count row changes instead
            PG_CHANGES.set(changes)
    except Exception:
        PG.reset()
        raise


@guarded("kafka_connect")
def collect_connect():
    names = requests.get(f"{CONNECT_URL}/connectors", timeout=10).json()
    for name in names:
        st = requests.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=10).json()
        ok = st["connector"]["state"] == "RUNNING" and all(t["state"] == "RUNNING" for t in st["tasks"])
        CONNECTOR.labels(name).set(1 if ok and st["tasks"] else 0)


def lake_tables(t: Trino) -> list[str]:
    """Every captured table (new source tables appear here as soon as the first batch creates them)."""
    return sorted(r[0] for r in t.rows("SELECT table_name FROM lakehouse.information_schema.tables "
                                       "WHERE table_schema = 'shop'"))


@guarded("freshness")
def collect_freshness(t: Trino):
    if "cdc_probe" not in lake_tables(t):
        return
    age = t.scalar(f"SELECT date_diff('millisecond', max(created_at), current_timestamp) / 1000.0 "
                   f"FROM {LAKE}.cdc_probe WHERE probe_id = 0")
    if age is not None:
        FRESHNESS.set(float(age))


@guarded("iceberg")
def collect_iceberg(t: Trino):
    tables = lake_tables(t)
    if not tables:
        return
    # one round trip per statistic for every table, instead of three queries per table
    files = t.rows(" UNION ALL ".join(
        f"SELECT '{table}', content, count(*), coalesce(sum(file_size_in_bytes), 0) "
        f"FROM {LAKE}.\"{table}$files\" GROUP BY content" for table in tables))
    by = {(table, content): (n, b) for table, content, n, b in files}
    for table in tables:
        for content, label in CONTENT.items():
            n, b = by.get((table, content), (0, 0))
            FILES.labels(table, label).set(n)
            BYTES.labels(table, label).set(b)
        n, b = by.get((table, 0), (0, 0))
        AVG_FILE.labels(table).set(b / n if n else 0)
    for table, n in t.rows(" UNION ALL ".join(
            f"SELECT '{table}', count(*) FROM {LAKE}.\"{table}$snapshots\"" for table in tables)):
        SNAPSHOTS.labels(table).set(n)
    for table, n in t.rows(" UNION ALL ".join(
            f"SELECT '{table}', count(*) FROM {LAKE}.{table}" for table in tables)):
        LAKE_ROWS.labels(table).set(n)


@guarded("ingest")
def collect_ingest(t: Trino):
    # One scan of the last hour: a replayed micro-batch appends its rows twice, so aggregate per
    # (batch_id, table) first; the 5-minute window and the newest batch age come from the same rows.
    rows = t.rows("""
        WITH b AS (SELECT batch_id, table_name, max(kafka_records) AS kafka_records, max(merge_ms) AS merge_ms,
                          max(batch_finished_at) AS finished
                   FROM lakehouse.ops.ingest_batches
                   WHERE batch_finished_at > current_timestamp - INTERVAL '1' HOUR
                   GROUP BY batch_id, table_name)
        SELECT coalesce(sum(CASE WHEN finished > current_timestamp - INTERVAL '5' MINUTE THEN kafka_records END), 0) / 300.0,
               count(DISTINCT CASE WHEN finished > current_timestamp - INTERVAL '5' MINUTE THEN batch_id END),
               date_diff('millisecond', max(finished), current_timestamp) / 1000.0
        FROM b""")
    rate, batches, age = rows[0]
    INGEST_RATE.set(float(rate))
    INGEST_BATCHES.set(batches)
    if age is None:  # nothing in the last hour: fall back to the newest batch ever
        age = t.scalar("SELECT date_diff('millisecond', max(batch_finished_at), current_timestamp) / 1000.0 "
                       "FROM lakehouse.ops.ingest_batches")
    if age is not None:
        LAST_BATCH_AGE.set(float(age))
    for table, ms in t.rows("""
        SELECT table_name, max_by(merge_ms, batch_id) FROM lakehouse.ops.ingest_batches
        WHERE batch_finished_at > current_timestamp - INTERVAL '1' HOUR GROUP BY table_name"""):
        MERGE_MS.labels(table).set(ms)


def loop(name: str, interval: float, steps):
    """Run the collectors every `interval` seconds with one Trino connection, reopened on failure."""
    trino = None
    while True:
        started = time.time()
        for step in steps:
            if step.needs_trino:
                try:
                    trino = trino or Trino()
                    step(trino)
                except Exception as exc:  # noqa: BLE001 - reconnect next round
                    log.warning("%s: trino unavailable: %s", name, str(exc).splitlines()[0][:200])
                    trino = None
            else:
                step()
        spent = time.time() - started
        if spent > interval:
            log.warning("%s loop took %.1f s, longer than its %.0f s interval", name, spent, interval)
        time.sleep(max(1.0, interval - spent))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=9108)
    p.add_argument("--interval", type=float, default=15, help="seconds between freshness / source checks")
    p.add_argument("--slow-interval", type=float, default=60, help="seconds between Iceberg file statistics")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    start_http_server(args.port)
    log.info("exporter listening on :%d, fast interval %ss, slow interval %ss", args.port, args.interval,
             args.slow_interval)
    for fn, needs in ((collect_postgres, False), (collect_connect, False), (collect_freshness, True),
                      (collect_iceberg, True), (collect_ingest, True)):
        fn.needs_trino = needs
    threading.Thread(target=loop, args=("slow", args.slow_interval, [collect_iceberg, collect_ingest]),
                     name="slow", daemon=True).start()
    loop("fast", args.interval, [collect_postgres, collect_connect, collect_freshness])


if __name__ == "__main__":
    main()
