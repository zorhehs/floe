"""
Floe Console: the web interface of the lakehouse (http://localhost:8090).

One page to operate and demonstrate the whole pipeline:
  * health of every component and the live CDC metrics (via Prometheus)
  * start / stop the synthetic OLTP workload
  * change a row in PostgreSQL and trace it until Trino sees it in Iceberg
  * tables, files, schema comparison and schema evolution
  * snapshots, time travel and the history of a single row
  * a read-only Trino SQL workbench with the benchmark query suite
  * trigger and follow the Airflow maintenance / GDPR purge DAGs
  * PostgreSQL vs Iceberg validation, and the recorded benchmark results

Standard library HTTP server + the clients the tools image already has, so the
console needs no extra dependency. Long operations run as background jobs that
the browser polls (GET /api/jobs/<id>).

  python3 -m console.app [--port 8090]
"""

from __future__ import annotations

import argparse
import json
import logging
import mimetypes
import os
import random
import re
import socket
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from decimal import Decimal
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests

from common.db import LAKE, OLTP, QUERIES_DIR, REPORTS_DIR, Airflow, Trino, pg_connect
from consumers import validate
from producer.generator import Workload, run_transaction
from producer.load_tpch import LOADS

log = logging.getLogger("console")
STATIC = Path(__file__).resolve().parent / "static"

CONNECT_URL = os.getenv("CONNECT_URL", "http://connect:8083")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
SPARK_UI_URL = os.getenv("SPARK_UI_URL", "http://spark-streaming:4040")
ICEBERG_REST_URL = os.getenv("ICEBERG_REST_URL", "http://iceberg-rest:8181")
MINIO_URL = os.getenv("S3_ENDPOINT", "http://minio:9000")
TRINO_URL = f"http://{os.getenv('TRINO_HOST', 'trino')}:{os.getenv('TRINO_PORT', '8080')}"
AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://airflow:8080")
GRAFANA_URL = os.getenv("GRAFANA_URL", "http://grafana:3000")
CONNECTOR = "postgres-cdc"

DAGS = ["iceberg_maintenance", "iceberg_layout_optimization", "iceberg_gdpr_purge"]
SQL_ROW_LIMIT = 1000
WORKBENCH_USER = "workbench"
PRIMARY_KEYS = {"orders": "o_orderkey", "customer": "c_custkey", "nation": "n_nationkey",
                "region": "r_regionkey", "cdc_probe": "probe_id"}
INTERNAL_TABLES = {"cdc_probe"}
IDENT = re.compile(r"^[a-z_][a-z0-9_]{0,40}$")
COLUMN_TYPES = {"integer", "bigint", "varchar(50)", "numeric(12,2)", "boolean", "date", "timestamp"}
READ_ONLY_START = ("select", "with", "show", "describe", "explain", "values", "table")


class ApiError(Exception):
    def __init__(self, message: str, status: int = HTTPStatus.BAD_REQUEST):
        super().__init__(message)
        self.status = status


def short_error(exc: Exception) -> str:
    msg = getattr(exc, "message", None) or str(exc) or exc.__class__.__name__
    return str(msg).strip().splitlines()[0][:400]


def to_json(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    return str(value)


def clean(value):
    """JSON-safe value; integers beyond 2^53 (Iceberg snapshot ids) become strings for JavaScript."""
    if value is None or isinstance(value, (bool, float, str)):
        return value
    if isinstance(value, int):
        return value if abs(value) < 2**53 else str(value)
    return to_json(value)


def clean_row(row: dict) -> dict:
    return {k: clean(v) for k, v in row.items()}


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def ident(name: str, what: str = "identifier") -> str:
    if not isinstance(name, str) or not IDENT.match(name):
        raise ApiError(f"invalid {what}: {name!r}")
    return name


# ---------------------------------------------------------------------------
# Small helpers: cache, background jobs
# ---------------------------------------------------------------------------
class TTLCache:
    def __init__(self):
        self._data: dict[str, tuple[float, object]] = {}
        self._lock = threading.Lock()

    def get(self, key: str, ttl: float, fn):
        with self._lock:
            hit = self._data.get(key)
            if hit and time.time() - hit[0] < ttl:
                return hit[1]
        value = fn()
        with self._lock:
            self._data[key] = (time.time(), value)
        return value

    def clear(self, prefix: str = ""):
        with self._lock:
            for key in [k for k in self._data if k.startswith(prefix)]:
                del self._data[key]


CACHE = TTLCache()


class Jobs:
    """Background operations the browser polls for progress."""

    def __init__(self, keep: int = 60):
        self._jobs: dict[str, dict] = {}
        self._lock = threading.Lock()
        self.keep = keep

    def start(self, kind: str, fn, *args) -> dict:
        job_id = uuid.uuid4().hex[:12]
        job = {"id": job_id, "kind": kind, "state": "running", "log": [], "result": None,
               "error": None, "started_at": utcnow(), "finished_at": None}
        with self._lock:
            self._jobs[job_id] = job
            for old in sorted(self._jobs.values(), key=lambda j: j["started_at"])[:-self.keep]:
                self._jobs.pop(old["id"], None)

        def say(message: str):
            job["log"].append({"t": utcnow(), "msg": message})

        def run():
            try:
                job["result"] = fn(say, *args)
                job["state"] = "done"
            except Exception as exc:  # noqa: BLE001 - reported to the browser
                log.exception("job %s failed", kind)
                job["error"] = short_error(exc)
                job["state"] = "failed"
            finally:
                job["finished_at"] = utcnow()

        threading.Thread(target=run, name=f"job-{kind}", daemon=True).start()
        return job

    def get(self, job_id: str) -> dict:
        job = self._jobs.get(job_id)
        if not job:
            raise ApiError("unknown job", HTTPStatus.NOT_FOUND)
        return job

    def running(self, kind: str) -> dict | None:
        return next((j for j in self._jobs.values() if j["kind"] == kind and j["state"] == "running"), None)


JOBS = Jobs()


def trino_rows(sql: str, params=None) -> list:
    return Trino().rows(sql, params)


def trino_dicts(sql: str, params=None) -> list[dict]:
    cur = Trino().conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in rows]


def lake_tables() -> set[str]:
    return {r[0] for r in trino_rows(
        "SELECT table_name FROM lakehouse.information_schema.tables WHERE table_schema = 'shop'")}


def source_tables() -> list[str]:
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'shop' AND table_type = 'BASE TABLE' ORDER BY 1")
        return [r[0] for r in cur.fetchall()]


def user_table(name: str) -> str:
    ident(name, "table")
    if name in INTERNAL_TABLES or name not in source_tables():
        raise ApiError(f"unknown table: {name}", HTTPStatus.NOT_FOUND)
    return name


def primary_key(table: str) -> str:
    try:
        rows = trino_rows(f'SELECT value FROM {LAKE}."{table}$properties" WHERE key = \'cdc.primary-key\'')
        if rows and rows[0][0]:
            return ident(rows[0][0].split(",")[0].strip(), "primary key")
    except Exception:  # noqa: BLE001 - fall back to the known schema
        pass
    if table in PRIMARY_KEYS:
        return PRIMARY_KEYS[table]
    raise ApiError(f"no primary key known for {table}")


# ---------------------------------------------------------------------------
# Health of every component
# ---------------------------------------------------------------------------
def _get(url: str, **kw):
    r = requests.get(url, timeout=kw.pop("timeout", 3), **kw)
    r.raise_for_status()
    return r


def check_postgres():
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FILTER (WHERE active), count(*) FROM pg_replication_slots")
        active, total = cur.fetchone()
        cur.execute("SELECT current_setting('server_version')")
        version = cur.fetchone()[0].split()[0]
    if not total:
        return False, "no replication slot yet"
    return True, f"v{version} · logical slot {'active' if active else 'idle'}"


def check_debezium():
    st = _get(f"{CONNECT_URL}/connectors/{CONNECTOR}/status").json()
    tasks = st.get("tasks", [])
    ok = st["connector"]["state"] == "RUNNING" and tasks and all(t["state"] == "RUNNING" for t in tasks)
    states = ", ".join(t["state"].lower() for t in tasks) or "no task"
    return bool(ok), f"connector {st['connector']['state'].lower()} · task {states}"


def check_kafka():
    with socket.create_connection(("kafka", 9092), timeout=3):
        pass
    topics = [t for t in _get(f"{CONNECT_URL}/connectors/{CONNECTOR}/topics").json()
              .get(CONNECTOR, {}).get("topics", []) if t.startswith("pg.")]
    return True, f"KRaft broker · {len(topics)} CDC topics"


def check_spark():
    apps = _get(f"{SPARK_UI_URL}/api/v1/applications").json()
    if not apps:
        return False, "no application"
    try:
        age = trino_rows("SELECT date_diff('second', max(batch_finished_at), current_timestamp) "
                         "FROM lakehouse.ops.ingest_batches")[0][0]
    except Exception:  # noqa: BLE001
        age = None
    detail = "streaming job running"
    if age is not None:
        detail += f" · last batch {human_age(age)} ago"
    return True, detail


def check_iceberg():
    ns = _get(f"{ICEBERG_REST_URL}/v1/namespaces").json().get("namespaces", [])
    return True, f"REST catalog · {len(ns)} namespaces"


def check_minio():
    _get(f"{MINIO_URL}/minio/health/live")
    return True, "S3 warehouse bucket"


def check_trino():
    info = _get(f"{TRINO_URL}/v1/info").json()
    if info.get("starting"):
        return False, "starting"
    return True, f"v{info.get('nodeVersion', {}).get('version', '?')} · lakehouse, postgresql, tpch"


def check_airflow():
    h = _get(f"{AIRFLOW_URL}/health").json()
    sched = h.get("scheduler", {}).get("status")
    return sched == "healthy", f"scheduler {sched or 'unknown'}"


def check_prometheus():
    _get(f"{PROMETHEUS_URL}/-/ready")
    return True, "scraping exporter, Spark, MinIO"


def check_grafana():
    _get(f"{GRAFANA_URL}/api/health")
    return True, "pipeline dashboard"


COMPONENTS = [
    # id, name, role, check, link on the host
    ("postgres", "PostgreSQL", "Source OLTP database", check_postgres, None),
    ("debezium", "Debezium", "Change data capture", check_debezium, None),
    ("kafka", "Kafka", "Change event log", check_kafka, None),
    ("spark", "Spark", "Structured Streaming MERGE", check_spark, "http://localhost:4040"),
    ("iceberg", "Iceberg", "REST catalog, table format", check_iceberg, None),
    ("minio", "MinIO", "Object storage", check_minio, "http://localhost:9001"),
    ("trino", "Trino", "SQL query engine", check_trino, "http://localhost:8080"),
    ("airflow", "Airflow", "Maintenance orchestration", check_airflow, "http://localhost:8088"),
    ("prometheus", "Prometheus", "Metrics", check_prometheus, "http://localhost:9090"),
    ("grafana", "Grafana", "Dashboards", check_grafana, "http://localhost:3000"),
]
POOL = ThreadPoolExecutor(max_workers=12, thread_name_prefix="check")


def human_age(seconds: float) -> str:
    seconds = float(seconds)
    if seconds < 90:
        return f"{seconds:.0f}s"
    if seconds < 5400:
        return f"{seconds / 60:.0f} min"
    if seconds < 172800:
        return f"{seconds / 3600:.0f} h"
    return f"{seconds / 86400:.0f} days"


def component_status() -> dict:
    def run(spec):
        cid, name, role, fn, link = spec
        t0 = time.perf_counter()
        try:
            ok, detail = fn()
        except Exception as exc:  # noqa: BLE001
            ok, detail = False, short_error(exc)
        return {"id": cid, "name": name, "role": role, "ok": bool(ok), "detail": detail, "link": link,
                "ms": round((time.perf_counter() - t0) * 1000)}

    items = list(POOL.map(run, COMPONENTS))
    return {"components": items, "healthy": sum(c["ok"] for c in items), "total": len(items),
            "checked_at": utcnow()}


# ---------------------------------------------------------------------------
# Metrics (Prometheus) and recent micro-batches
# ---------------------------------------------------------------------------
def prom_instant(expr: str):
    r = _get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": expr}).json()
    res = r["data"]["result"]
    return float(res[0]["value"][1]) if res else None


def prom_range(expr: str, minutes: int = 30, step: int = 15) -> list:
    end = time.time()
    r = _get(f"{PROMETHEUS_URL}/api/v1/query_range",
             params={"query": expr, "start": end - minutes * 60, "end": end, "step": step}).json()
    res = r["data"]["result"]
    return [[float(t), float(v)] for t, v in res[0]["values"]] if res else []


def metrics() -> dict:
    out = {"error": None}
    try:
        out.update(
            freshness_s=prom_instant("lakehouse_cdc_freshness_seconds"),
            ingest_rps=prom_instant("lakehouse_ingest_records_per_second"),
            slot_lag_bytes=prom_instant("sum(lakehouse_replication_slot_lag_bytes)"),
            last_batch_age_s=prom_instant("lakehouse_ingest_last_batch_age_seconds"),
            batches_5m=prom_instant("lakehouse_ingest_batches_5m"),
            lake_rows=prom_instant('sum(lakehouse_iceberg_rows{table!="cdc_probe"})'),
            source_tps=prom_instant("rate(lakehouse_source_commits_total[2m])"),
            series={
                "freshness": prom_range("lakehouse_cdc_freshness_seconds"),
                "ingest": prom_range("lakehouse_ingest_records_per_second"),
                "slot_lag": prom_range("sum(lakehouse_replication_slot_lag_bytes)"),
            },
        )
    except Exception as exc:  # noqa: BLE001
        out["error"] = f"Prometheus unavailable: {short_error(exc)}"
    return out


def alert_rules() -> dict:
    """Prometheus alert rules (config/monitoring/alerts.yml) with their live state."""
    r = _get(f"{PROMETHEUS_URL}/api/v1/rules", params={"type": "alert"}).json()
    rules = []
    for group in r["data"].get("groups", []):
        for rule in group.get("rules", []):
            if rule.get("type") != "alerting":
                continue
            active = [a.get("activeAt") for a in rule.get("alerts", []) if a.get("activeAt")]
            rules.append({"name": rule["name"], "state": rule.get("state") or "inactive",
                          "summary": (rule.get("annotations") or {}).get("summary"),
                          "expr": rule.get("query"), "for_seconds": rule.get("duration"),
                          "since": min(active) if active else None})
    order = {"firing": 0, "pending": 1, "inactive": 2}
    rules.sort(key=lambda a: (order.get(a["state"], 3), a["name"]))
    return {"rules": rules, "firing": sum(a["state"] == "firing" for a in rules),
            "pending": sum(a["state"] == "pending" for a in rules), "checked_at": utcnow()}


def recent_batches(limit: int = 25) -> list[dict]:
    limit = max(1, min(int(limit), 200))
    return trino_dicts(f"""
        SELECT batch_id, table_name, kafka_records, rows_merged, inserts, updates, deletes, merge_ms,
               batch_finished_at, schema_changes
        FROM lakehouse.ops.ingest_batches
        ORDER BY batch_id DESC, table_name LIMIT {limit}""")


# ---------------------------------------------------------------------------
# Workload generator (runs inside the console process)
# ---------------------------------------------------------------------------
class WorkloadRunner:
    MAX_RATE = 500

    def __init__(self):
        self.lock = threading.Lock()
        self.thread: threading.Thread | None = None
        self.stop_flag = threading.Event()
        self.state = {"running": False, "ops_per_sec": 0, "done": {}, "total": 0, "errors": 0,
                      "started_at": None, "last_error": None, "actual_ops_per_sec": 0.0}

    def snapshot(self) -> dict:
        s = dict(self.state)
        s["done"] = dict(self.state["done"])
        return s

    def start(self, rate: float) -> dict:
        rate = max(1.0, min(float(rate), self.MAX_RATE))
        with self.lock:
            if self.thread and self.thread.is_alive():
                self.state["ops_per_sec"] = rate  # change the rate of the running workload
                return self.snapshot()
            conn = pg_connect(autocommit=False)
            try:
                wl = Workload(conn)
            except SystemExit as exc:
                conn.close()
                raise ApiError(str(exc)) from None
            self.stop_flag.clear()
            self.state.update(running=True, ops_per_sec=rate, done={}, total=0, errors=0,
                              started_at=utcnow(), last_error=None, actual_ops_per_sec=0.0)
            self.thread = threading.Thread(target=self._loop, args=(conn, wl), name="workload", daemon=True)
            self.thread.start()
        return self.snapshot()

    def stop(self) -> dict:
        self.stop_flag.set()
        t = self.thread
        if t:
            t.join(timeout=5)
        self.state["running"] = False
        return self.snapshot()

    def _loop(self, conn, wl):
        tick, carry, started = 0.1, 0.0, time.time()
        try:
            while not self.stop_flag.is_set():
                t0 = time.time()
                carry += self.state["ops_per_sec"] * tick
                n, carry = int(carry), carry - int(carry)
                if n:
                    try:
                        run_transaction(conn, wl, n, self.state["done"])
                        self.state["total"] += n
                    except Exception as exc:  # noqa: BLE001 - keep the workload alive
                        self.state["errors"] += 1
                        self.state["last_error"] = short_error(exc)
                        if conn.closed:
                            conn = pg_connect(autocommit=False)
                            wl.conn = conn
                elapsed = time.time() - started
                self.state["actual_ops_per_sec"] = round(self.state["total"] / elapsed, 1) if elapsed else 0
                self.stop_flag.wait(max(0.0, tick - (time.time() - t0)))
        finally:
            self.state["running"] = False
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


WORKLOAD = WorkloadRunner()


# ---------------------------------------------------------------------------
# Live CDC trace: write to PostgreSQL, wait until Trino sees it in Iceberg
# ---------------------------------------------------------------------------
def pg_row(cur, table: str, pk: str, key) -> dict | None:
    cur.execute(f"SELECT * FROM shop.{table} WHERE {pk} = %s", [key])
    row = cur.fetchone()
    if row is None:
        return None
    return {d.name: clean(v) for d, v in zip(cur.description, row, strict=True)}


def pg_exists(table: str, pk: str, key) -> bool:
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM shop.{table} WHERE {pk} = %s", [key])
        return cur.fetchone() is not None


def random_key(cur, table: str, pk: str):
    """A uniformly random existing key (keys have gaps, so min..max sampling would be biased)."""
    cur.execute(f"SELECT {pk} FROM shop.{table} TABLESAMPLE SYSTEM (1) LIMIT 200")
    keys = [r[0] for r in cur.fetchall()]
    if not keys:  # small tables: a 1 % page sample can be empty
        cur.execute(f"SELECT {pk} FROM shop.{table} ORDER BY random() LIMIT 1")
        keys = [r[0] for r in cur.fetchall()]
    if not keys:
        raise ApiError(f"shop.{table} is empty - load data first")
    return random.choice(keys)


def same(lake_value, expected) -> bool:
    if isinstance(expected, Decimal):
        try:
            return lake_value is not None and Decimal(str(lake_value)) == expected
        except ArithmeticError:
            return False
    return lake_value == expected


def run_trace(say, action: str, params: dict) -> dict:
    table, pk = {"update_customer": ("customer", "c_custkey"), "insert_order": ("orders", "o_orderkey"),
                 "delete_order": ("orders", "o_orderkey")}[action]
    expect: dict = {}
    with pg_connect(autocommit=False) as conn, conn.cursor() as cur:
        key = params.get("key")
        if action == "insert_order":
            cust = params.get("custkey") or random_key(cur, "customer", "c_custkey")
            price = round(float(params.get("total_price") or random.uniform(900, 500000)), 2)
            cur.execute(
                "INSERT INTO shop.orders VALUES (nextval('shop.order_key_seq'), %s, 'O', %s, current_date, "
                "%s, %s, 0, %s, localtimestamp) RETURNING o_orderkey",
                [int(cust), price, params.get("priority") or "3-MEDIUM", "Clerk#000000001",
                 "inserted from the Floe console"])
            key = cur.fetchone()[0]
            what = f"INSERT order {key} (customer {cust}, {price:,.2f})"
        else:
            key = int(key) if key not in (None, "") else random_key(cur, table, pk)
            before = pg_row(cur, table, pk, key)
            if before is None:
                raise ApiError(f"{table} {key} does not exist in PostgreSQL")
            if action == "update_customer":
                balance = params.get("balance")
                balance = round(float(balance), 2) if balance not in (None, "") \
                    else round(random.uniform(-999, 9999), 2)
                name = (params.get("name") or "").strip()[:25] or before["c_name"]
                cur.execute("UPDATE shop.customer SET c_acctbal = %s, c_name = %s, updated_at = localtimestamp "
                            "WHERE c_custkey = %s", [balance, name, key])
                expect = {"c_acctbal": Decimal(f"{balance:.2f}"), "c_name": name}
                what = f"UPDATE customer {key}: balance {before['c_acctbal']} → {balance:.2f}"
                if name != before["c_name"]:
                    what += f", name → {name}"
            else:
                cur.execute("DELETE FROM shop.orders WHERE o_orderkey = %s", [key])
                what = f"DELETE order {key}"
        cur.execute("SELECT now()")  # transaction start: every later commit has a newer timestamp
        txn_start = cur.fetchone()[0]
        conn.commit()
        committed = time.time()
        say(f"PostgreSQL commit: {what}")
        source = pg_row(cur, table, pk, key)

    expect_gone = action == "delete_order"
    deadline, polls, row, note = committed + 120, 0, None, None
    while time.time() < deadline:
        polls += 1
        raw = trino_dicts(f"SELECT * FROM {LAKE}.{table} WHERE {pk} = ?", [key])
        row = clean_row(raw[0]) if raw else None
        if expect_gone and row is None:
            break
        if not expect_gone and row is not None and all(same(row.get(c), v) for c, v in expect.items()):
            break
        # Another session (e.g. the workload) changed the same row after us: once Iceberg holds
        # a change committed after our transaction started, ours has been applied as well.
        if raw and raw[0].get("_cdc_source_ts") and raw[0]["_cdc_source_ts"] >= txn_start:
            note = "a later change to the same row (another session) was already applied"
            break
        if row is None and not expect_gone and polls > 1 and not pg_exists(table, pk, key):
            note = "the row was deleted by another session right after this change"
            break
        time.sleep(0.4)
    else:
        raise RuntimeError(f"not visible in Iceberg after 120 s ({polls} polls) - is the streaming job running?")
    if note:
        say(f"note: {note}")
    visible = time.time()
    latency = visible - committed
    say(f"visible in Iceberg via Trino after {latency:.1f} s")

    result = {"action": action, "table": table, "key": key, "description": what,
              "committed_at": datetime.fromtimestamp(committed, timezone.utc).isoformat(),
              "visible_at": datetime.fromtimestamp(visible, timezone.utc).isoformat(),
              "latency_s": round(latency, 2), "polls": polls, "source_row": source, "lake_row": row,
              "note": note}
    if row:
        result["cdc"] = {k: row.get(k) for k in ("_cdc_op", "_cdc_source_ts", "_cdc_lsn", "_ingested_at")}
    try:
        batch = trino_dicts(
            "SELECT batch_id, kafka_records, rows_merged, merge_ms, batch_started_at, batch_finished_at "
            "FROM lakehouse.ops.ingest_batches WHERE table_name = ? AND batch_finished_at >= "
            "from_unixtime(?) ORDER BY batch_id LIMIT 1", [f"shop.{table}", committed - 1])
        if batch:
            result["batch"] = clean_row(batch[0])
    except Exception:  # noqa: BLE001 - batch details are optional
        pass
    return result


# ---------------------------------------------------------------------------
# Tables, schema, snapshots, time travel
# ---------------------------------------------------------------------------
def table_overview() -> list[dict]:
    lake = lake_tables()
    out = []
    with pg_connect() as conn, conn.cursor() as cur:
        for table in source_tables():
            if table in INTERNAL_TABLES:
                continue
            cur.execute(f"SELECT count(*) FROM shop.{table}")
            item = {"table": table, "source_rows": cur.fetchone()[0], "in_lake": table in lake}
            out.append(item)
    t = Trino()
    for item in out:
        if not item["in_lake"]:
            continue
        name = item["table"]
        item["lake_rows"] = t.scalar(f"SELECT count(*) FROM {LAKE}.{name}")
        files = {c: (n, b) for c, n, b in t.rows(
            f'SELECT content, count(*), coalesce(sum(file_size_in_bytes), 0) FROM {LAKE}."{name}$files" '
            "GROUP BY content")}
        data_n, data_b = files.get(0, (0, 0))
        item.update(data_files=data_n, data_bytes=data_b,
                    delete_files=sum(files.get(c, (0, 0))[0] for c in (1, 2)),
                    delete_bytes=sum(files.get(c, (0, 0))[1] for c in (1, 2)),
                    avg_file_bytes=round(data_b / data_n) if data_n else 0)
        snaps = t.rows(f'SELECT count(*), max(committed_at) FROM {LAKE}."{name}$snapshots"')[0]
        item.update(snapshots=snaps[0], last_commit=to_json(snaps[1]) if snaps[1] else None)
        part = t.rows(f'SELECT count(*) FROM {LAKE}."{name}$partitions"')[0][0]
        item["partitions"] = part
    return out


def table_schema(table: str) -> dict:
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale "
                    "FROM information_schema.columns WHERE table_schema = 'shop' AND table_name = %s "
                    "ORDER BY ordinal_position", [table])
        src = []
        for name, dtype, length, prec, scale in cur.fetchall():
            if dtype == "character varying":
                dtype = f"varchar({length})" if length else "varchar"
            elif dtype == "numeric" and prec is not None:
                dtype = f"numeric({prec},{scale})"
            src.append((name, dtype))
        cur.execute("""
            SELECT a.attname FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary""", [f"shop.{table}"])
        pks = {r[0] for r in cur.fetchall()}
        cur.execute("""
            SELECT DISTINCT kcu.column_name FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = 'shop' AND tc.table_name = %s AND tc.constraint_type = 'FOREIGN KEY'""",
                    [table])
        fks = {r[0] for r in cur.fetchall()}
    lake = {}
    if table in lake_tables():
        lake = {c: d for c, d in trino_rows(
            "SELECT column_name, data_type FROM lakehouse.information_schema.columns "
            "WHERE table_schema = 'shop' AND table_name = ? ORDER BY ordinal_position", [table])}
    cols = []
    for name, dtype in src:
        cols.append({"name": name, "source_type": dtype, "lake_type": lake.get(name),
                     "primary_key": name in pks, "foreign_key": name in fks,
                     "widenable": dtype == "integer" and name not in pks and name not in fks,
                     "status": "synced" if name in lake else "pending"})
    for name, dtype in lake.items():
        if name not in dict(src):
            cols.append({"name": name, "source_type": None, "lake_type": dtype, "primary_key": False,
                         "foreign_key": False, "widenable": False,
                         "status": "metadata" if name in validate.META else "lake only"})
    history = []
    if lake:
        history = [clean_row(r) for r in trino_dicts(
            f'SELECT timestamp, latest_schema_id, latest_snapshot_id FROM {LAKE}."{table}$metadata_log_entries" '
            "ORDER BY timestamp DESC LIMIT 200")]
        changes, last = [], None
        for h in reversed(history):
            if h["latest_schema_id"] != last:
                changes.append(h)
                last = h["latest_schema_id"]
        history = list(reversed(changes))[:20]
    return {"table": table, "columns": cols, "schema_versions": history}


def run_add_column(say, table: str, column: str, ctype: str, value) -> dict:
    pk = primary_key(table)
    with pg_connect(autocommit=False) as conn, conn.cursor() as cur:
        cur.execute(f"ALTER TABLE shop.{table} ADD COLUMN IF NOT EXISTS {column} {ctype}")
        say(f"ALTER TABLE shop.{table} ADD COLUMN {column} {ctype}")
        # Debezium ships a table's schema with its next change event: change a few rows.
        cur.execute(f"SELECT {pk} FROM shop.{table} ORDER BY {pk} LIMIT 5")
        keys = [r[0] for r in cur.fetchall()]
        if not keys:
            raise ApiError(f"shop.{table} is empty - load data first")
        if value in (None, ""):
            cur.execute(f"UPDATE shop.{table} SET {column} = {column} WHERE {pk} = ANY(%s)", [keys])
        else:
            cur.execute(f"UPDATE shop.{table} SET {column} = %s::{ctype} WHERE {pk} = ANY(%s)", [value, keys])
        conn.commit()
        say(f"updated {len(keys)} rows so the new schema travels with a change event")
    t0 = time.time()
    return wait_schema(say, table, column, None, t0)


def run_widen(say, table: str, column: str) -> dict:
    pk = primary_key(table)
    with pg_connect(autocommit=False) as conn, conn.cursor() as cur:
        cur.execute(f"ALTER TABLE shop.{table} ALTER COLUMN {column} TYPE bigint")
        say(f"ALTER TABLE shop.{table} ALTER COLUMN {column} TYPE bigint")
        cur.execute(f"SELECT {pk} FROM shop.{table} ORDER BY {pk} LIMIT 1")
        key = cur.fetchone()
        if not key:
            raise ApiError(f"shop.{table} is empty - load data first")
        cur.execute(f"UPDATE shop.{table} SET {column} = 3000000000 WHERE {pk} = %s", [key[0]])
        conn.commit()
        say(f"set {column} = 3,000,000,000 (beyond the integer range) on {pk} = {key[0]}")
    return wait_schema(say, table, column, "bigint", time.time())


def wait_schema(say, table: str, column: str, dtype: str | None, t0: float) -> dict:
    while time.time() - t0 < 180:
        rows = trino_rows("SELECT data_type FROM lakehouse.information_schema.columns "
                          "WHERE table_schema = 'shop' AND table_name = ? AND column_name = ?", [table, column])
        if rows and (dtype is None or rows[0][0] == dtype):
            secs = time.time() - t0
            say(f"Iceberg schema has {column} {rows[0][0]} after {secs:.1f} s - no data files rewritten")
            CACHE.clear("schema:")
            return {"table": table, "column": column, "lake_type": rows[0][0], "seconds": round(secs, 1)}
        time.sleep(1)
    raise RuntimeError("schema change not visible in Iceberg after 180 s")


def snapshots(table: str, limit: int = 60) -> list[dict]:
    rows = trino_dicts(f"""
        SELECT committed_at, snapshot_id, parent_id, operation,
               element_at(summary, 'added-records') AS added_records,
               element_at(summary, 'deleted-records') AS deleted_records,
               element_at(summary, 'added-position-deletes') AS added_position_deletes,
               element_at(summary, 'added-data-files') AS added_data_files,
               element_at(summary, 'deleted-data-files') AS deleted_data_files,
               element_at(summary, 'total-records') AS total_records,
               element_at(summary, 'total-data-files') AS total_data_files,
               element_at(summary, 'total-delete-files') AS total_delete_files
        FROM {LAKE}."{table}$snapshots" ORDER BY committed_at DESC LIMIT {int(limit)}""")
    return [clean_row(r) for r in rows]


def snapshot_detail(table: str, snapshot_id: int) -> dict:
    t = Trino()
    then = t.scalar(f"SELECT count(*) FROM {LAKE}.{table} FOR VERSION AS OF {int(snapshot_id)}")
    now = t.scalar(f"SELECT count(*) FROM {LAKE}.{table}")
    changed = t.rows(f"""
        SELECT _cdc_op, count(*) FROM {LAKE}.{table}
        WHERE _ingested_at > (SELECT committed_at FROM {LAKE}."{table}$snapshots" WHERE snapshot_id = {int(snapshot_id)})
        GROUP BY 1 ORDER BY 1""")
    return {"table": table, "snapshot_id": str(snapshot_id), "rows_then": then, "rows_now": now,
            "changed_since": {op or "?": n for op, n in changed}}


def row_history(table: str, key: int, depth: int = 12) -> dict:
    pk = primary_key(table)
    snaps = trino_rows(f'SELECT snapshot_id, committed_at, operation FROM {LAKE}."{table}$snapshots" '
                       f"ORDER BY committed_at DESC LIMIT {int(depth)}")
    if not snaps:
        return {"table": table, "key": key, "versions": []}
    parts = [f"SELECT CAST({s} AS varchar) AS _snapshot, * FROM {LAKE}.{table} FOR VERSION AS OF {s} "
             f"WHERE {pk} = {int(key)}" for s, _, _ in snaps]
    found = {}
    for r in trino_dicts(" UNION ALL ".join(parts)):
        sid = r.pop("_snapshot")
        found[sid] = clean_row(r)
    versions = []
    for sid, committed, op in snaps:
        versions.append({"snapshot_id": str(sid), "committed_at": to_json(committed), "operation": op,
                         "row": found.get(str(sid))})
    return {"table": table, "key": key, "primary_key": pk, "versions": versions}


# ---------------------------------------------------------------------------
# SQL workbench
# ---------------------------------------------------------------------------
def strip_sql(sql: str) -> str:
    sql = re.sub(r"--[^\n]*", "", sql)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
    return sql.strip().rstrip(";").strip()


def run_sql(sql: str) -> dict:
    body = strip_sql(sql or "")
    if not body:
        raise ApiError("empty query")
    if ";" in re.sub(r"'(?:[^']|'')*'", "", body):
        raise ApiError("one statement at a time")
    if not body.lower().startswith(READ_ONLY_START):
        raise ApiError("the workbench is read-only: SELECT, WITH, SHOW, DESCRIBE, EXPLAIN, VALUES or TABLE")
    # The keyword check gives a friendly message; Trino enforces it: the "workbench"
    # user is read-only on every catalog (config/trino/rules.json).
    cur = Trino(WORKBENCH_USER).conn.cursor()
    t0 = time.perf_counter()
    cur.execute(body)
    rows = cur.fetchmany(SQL_ROW_LIMIT + 1)
    truncated = len(rows) > SQL_ROW_LIMIT
    if truncated:
        rows = rows[:SQL_ROW_LIMIT]
        try:
            cur.cancel()
        except Exception:  # noqa: BLE001
            pass
    wall = (time.perf_counter() - t0) * 1000
    stats = cur.stats or {}
    return {
        "columns": [{"name": d[0], "type": d[1]} for d in (cur.description or [])],
        "rows": [[clean(v) for v in r] for r in rows],
        "truncated": truncated, "wall_ms": round(wall, 1),
        "stats": {k: stats.get(k) for k in ("queryId", "elapsedTimeMillis", "cpuTimeMillis", "processedRows",
                                            "processedBytes", "physicalInputBytes", "peakMemoryBytes")},
    }


def sample_queries() -> list[dict]:
    out = []
    for f in sorted(Path(QUERIES_DIR).glob("q*.sql")):
        text = f.read_text()
        comments = [ln[2:].strip() for ln in text.splitlines() if ln.startswith("--")]
        out.append({"id": f.stem, "title": comments[0] if comments else f.stem, "sql": text.strip()})
    return out


# ---------------------------------------------------------------------------
# Airflow
# ---------------------------------------------------------------------------
def dag_overview() -> list[dict]:
    af = Airflow()
    out = []
    for dag_id in DAGS:
        try:
            dag = af._req("GET", f"/dags/{dag_id}")
            runs = af._req("GET", f"/dags/{dag_id}/dagRuns", params={"order_by": "-start_date", "limit": 6})
            out.append({"dag_id": dag_id, "description": dag.get("description"), "is_paused": dag["is_paused"],
                        "schedule": (dag.get("schedule_interval") or {}).get("value"),
                        "runs": [run_summary(r) for r in runs.get("dag_runs", [])]})
        except Exception as exc:  # noqa: BLE001
            out.append({"dag_id": dag_id, "error": short_error(exc), "runs": []})
    return out


def run_summary(r: dict) -> dict:
    return {k: r.get(k) for k in ("dag_run_id", "state", "start_date", "end_date", "conf", "run_type")}


def dag_run(dag_id: str, run_id: str) -> dict:
    af = Airflow()
    run = af._req("GET", f"/dags/{dag_id}/dagRuns/{run_id}")
    tis = af._req("GET", f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")["task_instances"]
    order = {t: i for i, t in enumerate(["compact", "apply_layout_and_compact", "purge",
                                         "rewrite_deletes_and_manifests", "expire_snapshots",
                                         "remove_orphan_files"])}
    tasks = sorted(({k: ti.get(k) for k in ("task_id", "state", "start_date", "end_date", "duration",
                                             "try_number")} for ti in tis),
                   key=lambda t: order.get(t["task_id"], 99))
    return {"dag_id": dag_id, "run": run_summary(run), "tasks": tasks}


def trigger_dag(dag_id: str, conf: dict) -> dict:
    if dag_id not in DAGS:
        raise ApiError(f"unknown DAG {dag_id}")
    clean = {}
    tables = (conf or {}).get("tables")
    if tables:
        names = [t.strip() for t in str(tables).split(",") if t.strip()]
        for n in names:
            if not re.match(r"^(shop\.)?[a-z_][a-z0-9_]*$", n):
                raise ApiError(f"invalid table name: {n}")
        clean["tables"] = ",".join(n if n.startswith("shop.") else f"shop.{n}" for n in names)
    elif dag_id == "iceberg_gdpr_purge":
        clean["tables"] = "shop.customer,shop.orders"
    af = Airflow()
    if af._req("GET", f"/dags/{dag_id}")["is_paused"]:
        # Airflow does not start runs of a paused DAG, not even manual ones
        af._req("PATCH", f"/dags/{dag_id}", json={"is_paused": False})
    run_id = f"console__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}"
    run = af._req("POST", f"/dags/{dag_id}/dagRuns", json={"dag_run_id": run_id, "conf": clean})
    return run_summary(run)


def set_dag_paused(dag_id: str, paused: bool) -> dict:
    if dag_id not in DAGS:
        raise ApiError(f"unknown DAG {dag_id}")
    Airflow()._req("PATCH", f"/dags/{dag_id}", json={"is_paused": bool(paused)})
    return {"dag_id": dag_id, "is_paused": bool(paused)}


# ---------------------------------------------------------------------------
# Validation, data loading, recorded results
# ---------------------------------------------------------------------------
def run_validation(say, wait: bool) -> dict:
    t = Trino()
    t0 = time.time()
    if wait:
        say("waiting until row counts agree, then comparing checksums (up to 5 min)")
        ok, secs, results = validate.wait_consistent(t, 300, quiet=True)
    else:
        say("comparing row counts and checksums of every table")
        results = validate.compare(t)
        ok, secs = all(r["ok"] for r in results), time.time() - t0
    say(("consistent" if ok else "differences found") + f" after {secs:.1f} s")
    return {"consistent": ok, "seconds": round(secs, 1), "tables": results,
            "workload_running": WORKLOAD.state["running"]}


def run_load(say, scale: str) -> dict:
    t = Trino()
    loaded, t0 = {}, time.time()
    for table, sql in LOADS:
        existing = t.scalar(f"SELECT count(*) FROM {OLTP}.{table}")
        if existing:
            say(f"{table}: already has {existing:,} rows, skipped")
            continue
        s = time.time()
        t.rows(sql.format(oltp=OLTP, scale=scale))
        loaded[table] = t.scalar(f"SELECT count(*) FROM {OLTP}.{table}")
        say(f"{table}: {loaded[table]:,} rows written to PostgreSQL in {time.time() - s:.1f} s")
    if not loaded:
        return {"loaded": {}, "consistent": True, "seconds": 0}
    say("streaming through Debezium → Kafka → Spark into Iceberg …")
    deadline = time.time() + 3600
    while time.time() < deadline:
        same, lag = validate.counts_match(t)
        if same:
            break
        say("catching up: " + ", ".join(f"{k} {d:,}/{s:,}" for k, (s, d) in lag.items()))
        time.sleep(5)
    secs = time.time() - t0
    total = sum(loaded.values())
    say(f"{total:,} rows queryable in Iceberg after {secs:.0f} s ({total / secs:,.0f} rows/s)")
    CACHE.clear()
    return {"loaded": loaded, "seconds": round(secs, 1), "rows_per_second": round(total / secs, 1)}


def recorded_results() -> dict:
    root = Path(REPORTS_DIR)
    benches = sorted(p for p in root.glob("benchmark-*") if (p / "results.json").exists())
    out = {"benchmark": None}
    for name in ("ingest_load", "freshness"):
        f = root / f"{name}.json"
        out[name] = json.loads(f.read_text()) if f.exists() else None
    if benches:
        r = json.loads((benches[-1] / "results.json").read_text())
        out["benchmark"] = {
            "name": benches[-1].name, "runs_per_query": r.get("runs_per_query"),
            "queries": [{"id": q, "before_ms": r["before"]["queries"][q]["median_wall_ms"],
                         "after_ms": r["after"]["queries"][q]["median_wall_ms"],
                         "before_rows": r["before"]["queries"][q]["processed_rows"],
                         "after_rows": r["after"]["queries"][q]["processed_rows"]}
                        for q in r["before"]["queries"]],
            "files": {"before": r["before"]["files"], "after": r["after"]["files"]},
            "storage": {"before": r["before"].get("storage"), "after": r["after"].get("storage")},
            "optimization": {"wall_seconds": r["optimization"].get("wall_seconds"),
                             "tasks": r["optimization"].get("tasks")},
            "ingest": r.get("ingest"),
        }
    return out



# ---------------------------------------------------------------------------
# GDPR right to erasure: delete in PostgreSQL, show the merge-on-read delete and
# the time-travel loophole, then purge the history physically
# ---------------------------------------------------------------------------
def file_counts(t: Trino, table: str) -> dict:
    files = {c: n for c, n in t.rows(f'SELECT content, count(*) FROM {LAKE}."{table}$files" GROUP BY content')}
    snapshots = t.scalar(f'SELECT count(*) FROM {LAKE}."{table}$snapshots"')
    return {"data_files": files.get(0, 0), "delete_files": files.get(1, 0) + files.get(2, 0),
            "snapshots": snapshots}


def gdpr_candidate(key=None) -> dict:
    """The data subject of an erasure request: a customer (random: one with orders) and what we hold."""
    with pg_connect() as conn, conn.cursor() as cur:
        if key in (None, ""):
            # sample the orders, not the customers: keys are skewed (TPC-H 1..N, generated ones far
            # above), and a customer drawn from the orders holds data in both tables by construction
            cur.execute("SELECT DISTINCT o.o_custkey FROM shop.orders o TABLESAMPLE SYSTEM (1) "
                        "JOIN shop.customer c ON c.c_custkey = o.o_custkey LIMIT 50")
            keys = [r[0] for r in cur.fetchall()]
            if not keys:  # small tables: a 1 % page sample can be empty
                cur.execute("SELECT o.o_custkey FROM shop.orders o JOIN shop.customer c ON c.c_custkey = o.o_custkey "
                            "ORDER BY random() LIMIT 1")
                keys = [r[0] for r in cur.fetchall()]
            if not keys:
                cur.execute("SELECT c_custkey FROM shop.customer ORDER BY random() LIMIT 1")
                keys = [r[0] for r in cur.fetchall()]
            if not keys:
                raise ApiError("shop.customer is empty - load data first")
            key = random.choice(keys)
        customer = pg_row(cur, "customer", "c_custkey", key)
        if customer is None:
            raise ApiError(f"customer {key} does not exist in PostgreSQL", HTTPStatus.NOT_FOUND)
        cur.execute("SELECT count(*), coalesce(sum(o_totalprice), 0) FROM shop.orders WHERE o_custkey = %s", [key])
        orders, total = cur.fetchone()
    return {"key": key, "customer": customer, "orders": orders, "order_total": clean(total)}


def ghost_row(t: Trino, snapshot_id: int, key: int) -> dict | None:
    rows = trino_dicts(f"SELECT c_custkey, c_name, c_phone, c_address, c_acctbal FROM {LAKE}.customer "
                       f"FOR VERSION AS OF {int(snapshot_id)} WHERE c_custkey = ?", [int(key)])
    return clean_row(rows[0]) if rows else None


def run_gdpr_erase(say, key) -> dict:
    subject = gdpr_candidate(key)
    key = subject["key"]
    t = Trino()
    snap = t.rows(f'SELECT snapshot_id FROM {LAKE}."customer$snapshots" ORDER BY committed_at DESC LIMIT 1')
    if not snap:
        raise ApiError("lakehouse.shop.customer has no snapshot yet - load data first")
    snapshot_id = int(snap[0][0])
    before = file_counts(t, "customer")
    say(f"erasure request for customer {key} ({subject['orders']} orders); remembering snapshot {snapshot_id}")

    with pg_connect(autocommit=False) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM shop.orders WHERE o_custkey = %s", [key])
        orders_deleted = cur.rowcount
        cur.execute("DELETE FROM shop.customer WHERE c_custkey = %s", [key])
        conn.commit()
    committed = time.time()
    say(f"PostgreSQL commit: DELETE {orders_deleted} orders and customer {key} in one transaction")

    deadline, polls = committed + 120, 0
    while time.time() < deadline:
        polls += 1
        c = t.scalar(f"SELECT count(*) FROM {LAKE}.customer WHERE c_custkey = ?", [key])
        o = t.scalar(f"SELECT count(*) FROM {LAKE}.orders WHERE o_custkey = ?", [key])
        if c == 0 and o == 0:
            break
        time.sleep(0.5)
    else:
        raise RuntimeError(f"still in Iceberg after 120 s ({polls} polls) - is the streaming job running?")
    latency = time.time() - committed
    say(f"gone from the current Iceberg snapshot after {latency:.1f} s")

    after = file_counts(t, "customer")
    ghost = ghost_row(t, snapshot_id, key)
    say("time travel to the remembered snapshot still returns the personal data"
        if ghost else "the remembered snapshot no longer holds the row")
    return {"key": key, "customer": subject["customer"], "orders_deleted": orders_deleted,
            "snapshot_id": str(snapshot_id),
            "committed_at": datetime.fromtimestamp(committed, timezone.utc).isoformat(),
            "latency_s": round(latency, 2), "polls": polls, "files_before": before, "files_after": after,
            "still_readable": ghost, "workload_running": WORKLOAD.state["running"]}


def run_gdpr_purge(say, key, snapshot_id: int, pause_workload: bool) -> dict:
    resume_rate = None
    if pause_workload and WORKLOAD.state["running"]:
        resume_rate = WORKLOAD.state["ops_per_sec"]
        WORKLOAD.stop()
        say(f"workload paused ({resume_rate:g} ops/s) so the file counts before and after are comparable")
    t = Trino()
    before = file_counts(t, "customer")
    t0 = time.time()
    run = trigger_dag("iceberg_gdpr_purge", {"tables": "shop.customer,shop.orders"})
    run_id = run["dag_run_id"]
    say(f"Airflow run {run_id}: rewrite every file that has deletes, then expire the history")
    seen: dict[str, str] = {}
    detail = None
    try:
        deadline = time.time() + 1800
        while time.time() < deadline:
            detail = dag_run("iceberg_gdpr_purge", run_id)
            for task in detail["tasks"]:
                state = task.get("state")
                if state and seen.get(task["task_id"]) != state:
                    seen[task["task_id"]] = state
                    say(f"task {task['task_id']}: {state.replace('_', ' ')}")
            if detail["run"]["state"] in ("success", "failed"):
                break
            time.sleep(3)
    finally:
        if resume_rate is not None:
            WORKLOAD.start(resume_rate)
            say("workload resumed")
    state = detail["run"]["state"] if detail else "unknown"
    if state != "success":
        raise RuntimeError(f"purge run {run_id} ended in state {state} - see Airflow")
    after = file_counts(t, "customer")
    try:
        ghost = ghost_row(t, snapshot_id, key) if key is not None else None
        history = {"readable": ghost is not None, "row": ghost, "error": None}
    except Exception as exc:  # noqa: BLE001 - the expected outcome: the snapshot is gone
        history = {"readable": False, "row": None, "error": short_error(exc)}
    say("snapshot expired: " + history["error"] if history["error"] else
        ("the personal data is STILL readable" if history["readable"] else "the snapshot no longer holds the row"))
    CACHE.clear()
    return {"key": key, "snapshot_id": str(snapshot_id), "run": detail["run"], "tasks": detail["tasks"],
            "wall_seconds": round(time.time() - t0, 1), "files_before": before, "files_after": after,
            "history": history, "workload_resumed": resume_rate}


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------
ROUTES: list[tuple[str, re.Pattern, object]] = []


def route(method: str, pattern: str):
    def deco(fn):
        ROUTES.append((method, re.compile(f"^{pattern}$"), fn))
        return fn
    return deco


@route("GET", "/api/ping")
def api_ping(q, body):
    return {"ok": True, "time": utcnow()}


@route("GET", "/api/status")
def api_status(q, body):
    return CACHE.get("status", 4, component_status)


@route("GET", "/api/metrics")
def api_metrics(q, body):
    return CACHE.get("metrics", 4, metrics)


@route("GET", "/api/batches")
def api_batches(q, body):
    limit = int(q.get("limit", ["25"])[0])
    return {"batches": [clean_row(r) for r in CACHE.get(f"batches:{limit}", 3, lambda: recent_batches(limit))]}


@route("GET", "/api/overview")
def api_overview(q, body):
    def counts():
        with pg_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT relname, n_live_tup FROM pg_stat_user_tables WHERE schemaname = 'shop'")
            est = dict(cur.fetchall())
            cur.execute("SELECT EXISTS (SELECT 1 FROM shop.orders)")
            return {"has_data": cur.fetchone()[0], "estimated_rows": est}
    return CACHE.get("overview", 5, counts)


@route("GET", "/api/workload")
def api_workload(q, body):
    return WORKLOAD.snapshot()


@route("POST", "/api/workload")
def api_workload_set(q, body):
    action = body.get("action")
    if action == "start":
        return WORKLOAD.start(body.get("ops_per_sec", 50))
    if action == "stop":
        return WORKLOAD.stop()
    raise ApiError("action must be start or stop")


@route("POST", "/api/trace")
def api_trace(q, body):
    action = body.get("action")
    if action not in ("update_customer", "insert_order", "delete_order"):
        raise ApiError("unknown action")
    for f in ("key", "custkey"):
        if body.get(f) not in (None, "") and not str(body[f]).lstrip("-").isdigit():
            raise ApiError(f"{f} must be a number")
    if body.get("balance") not in (None, ""):
        try:
            if abs(float(body["balance"])) >= 1e10:
                raise ApiError("balance out of range")
        except ValueError:
            raise ApiError("balance must be a number") from None
    return JOBS.start("trace", run_trace, action, body)


@route("GET", "/api/jobs/(?P<job_id>[a-f0-9]{12})")
def api_job(q, body, job_id):
    return JOBS.get(job_id)


@route("GET", "/api/tables")
def api_tables(q, body):
    return {"tables": CACHE.get("tables", 5, table_overview)}


@route("GET", "/api/tables/(?P<table>[a-z_0-9]+)/schema")
def api_schema(q, body, table):
    user_table(table)
    return CACHE.get(f"schema:{table}", 3, lambda: table_schema(table))


@route("POST", "/api/tables/(?P<table>[a-z_0-9]+)/columns")
def api_add_column(q, body, table):
    user_table(table)
    column = ident(str(body.get("column", "")).strip().lower(), "column name")
    ctype = str(body.get("type", "")).strip().lower()
    if ctype not in COLUMN_TYPES:
        raise ApiError(f"type must be one of {sorted(COLUMN_TYPES)}")
    if column in {c["name"] for c in table_schema(table)["columns"]}:
        raise ApiError(f"{table}.{column} already exists")
    return JOBS.start("schema", run_add_column, table, column, ctype, body.get("value"))


@route("POST", "/api/tables/(?P<table>[a-z_0-9]+)/widen")
def api_widen(q, body, table):
    user_table(table)
    column = ident(str(body.get("column", "")), "column name")
    col = next((c for c in table_schema(table)["columns"] if c["name"] == column), None)
    if not col or not col["widenable"]:
        raise ApiError(f"{table}.{column} is not an integer column that can be widened")
    return JOBS.start("schema", run_widen, table, column)


@route("GET", "/api/tables/(?P<table>[a-z_0-9]+)/snapshots")
def api_snapshots(q, body, table):
    user_table(table)
    return {"table": table, "snapshots": snapshots(table)}


@route("GET", "/api/tables/(?P<table>[a-z_0-9]+)/snapshots/(?P<sid>-?[0-9]+)")
def api_snapshot(q, body, table, sid):
    user_table(table)
    return snapshot_detail(table, int(sid))


@route("GET", "/api/tables/(?P<table>[a-z_0-9]+)/rows/(?P<key>-?[0-9]+)/history")
def api_row_history(q, body, table, key):
    user_table(table)
    return row_history(table, int(key), min(int(q.get("depth", ["12"])[0]), 30))


@route("GET", "/api/tables/(?P<table>[a-z_0-9]+)/sample-key")
def api_sample_key(q, body, table):
    user_table(table)
    pk = primary_key(table)
    with pg_connect() as conn, conn.cursor() as cur:
        return {"table": table, "primary_key": pk, "key": random_key(cur, table, pk)}


@route("GET", "/api/queries")
def api_queries(q, body):
    return {"queries": sample_queries()}


@route("POST", "/api/sql")
def api_sql(q, body):
    return run_sql(body.get("sql", ""))


@route("GET", "/api/dags")
def api_dags(q, body):
    return {"dags": dag_overview()}


@route("POST", "/api/dags/(?P<dag_id>[a-z_]+)/runs")
def api_trigger(q, body, dag_id):
    CACHE.clear("tables")
    return trigger_dag(dag_id, body.get("conf") or {})


@route("GET", "/api/dags/(?P<dag_id>[a-z_]+)/runs/(?P<run_id>[A-Za-z0-9_:.+-]+)")
def api_dag_run(q, body, dag_id, run_id):
    if dag_id not in DAGS:
        raise ApiError("unknown DAG", HTTPStatus.NOT_FOUND)
    return dag_run(dag_id, run_id)


@route("POST", "/api/dags/(?P<dag_id>[a-z_]+)/paused")
def api_pause(q, body, dag_id):
    return set_dag_paused(dag_id, body.get("paused", True))


@route("POST", "/api/validate")
def api_validate(q, body):
    running = JOBS.running("validate")
    return running or JOBS.start("validate", run_validation, bool(body.get("wait")))


@route("POST", "/api/load")
def api_load(q, body):
    scale = body.get("scale", "tiny")
    if scale not in ("tiny", "sf1"):
        raise ApiError("scale must be tiny or sf1")
    running = JOBS.running("load")
    return running or JOBS.start("load", run_load, scale)


@route("GET", "/api/results")
def api_results(q, body):
    return CACHE.get("results", 30, recorded_results)


@route("GET", "/api/alerts")
def api_alerts(q, body):
    return CACHE.get("alerts", 5, alert_rules)


def _number(value, what: str):
    if value in (None, ""):
        return None
    if not str(value).lstrip("-").isdigit():
        raise ApiError(f"{what} must be a number")
    return int(value)


@route("GET", "/api/gdpr/candidate")
def api_gdpr_candidate(q, body):
    return gdpr_candidate(_number(q.get("key", [""])[0], "key"))


@route("POST", "/api/gdpr/erase")
def api_gdpr_erase(q, body):
    key = _number(body.get("key"), "key")
    running = JOBS.running("gdpr_erase")
    return running or JOBS.start("gdpr_erase", run_gdpr_erase, key)


@route("POST", "/api/gdpr/purge")
def api_gdpr_purge(q, body):
    snapshot_id = _number(body.get("snapshot_id"), "snapshot_id")
    if snapshot_id is None:
        raise ApiError("snapshot_id is required")
    key = _number(body.get("key"), "key")
    running = JOBS.running("gdpr_purge")
    return running or JOBS.start("gdpr_purge", run_gdpr_purge, key, snapshot_id, bool(body.get("pause_workload", True)))


class Handler(BaseHTTPRequestHandler):
    server_version = "FloeConsole/1.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # quiet access log; errors are logged explicitly
        pass

    def _send(self, status: int, payload: bytes, ctype: str, extra: dict | None = None):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("X-Content-Type-Options", "nosniff")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)

    def _json(self, status: int, data):
        self._send(status, json.dumps(data, default=to_json).encode(), "application/json; charset=utf-8",
                   {"Cache-Control": "no-store"})

    def _dispatch(self, method: str):
        url = urlparse(self.path)
        if not url.path.startswith("/api/"):
            return self._static(url.path)
        body = {}
        if method == "POST":
            length = int(self.headers.get("Content-Length") or 0)
            if length > 1_000_000:
                return self._json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"error": "request too large"})
            raw = self.rfile.read(length) if length else b""
            try:
                body = json.loads(raw or b"{}")
                if not isinstance(body, dict):
                    raise ValueError
            except ValueError:
                return self._json(HTTPStatus.BAD_REQUEST, {"error": "body must be a JSON object"})
        for m, pattern, fn in ROUTES:
            match = pattern.match(url.path)
            if match and m == method:
                try:
                    return self._json(HTTPStatus.OK, fn(parse_qs(url.query), body, **match.groupdict()))
                except ApiError as exc:
                    return self._json(exc.status, {"error": str(exc)})
                except Exception as exc:  # noqa: BLE001
                    log.warning("%s %s failed: %s", method, url.path, short_error(exc))
                    return self._json(HTTPStatus.BAD_GATEWAY, {"error": short_error(exc)})
        self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def _static(self, path: str):
        if path in ("", "/"):
            path = "/index.html"
        target = (STATIC / path.lstrip("/")).resolve()
        if STATIC not in target.parents or not target.is_file():
            target = STATIC / "index.html"  # single-page app: unknown paths load the shell
        ctype = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
        if ctype.startswith("text/") or ctype in ("application/javascript", "image/svg+xml"):
            ctype += "; charset=utf-8"
        self._send(HTTPStatus.OK, target.read_bytes(), ctype, {"Cache-Control": "no-cache"})

    def do_GET(self):  # noqa: N802
        self._dispatch("GET")

    def do_HEAD(self):  # noqa: N802
        self._dispatch("GET")

    def do_POST(self):  # noqa: N802
        self._dispatch("POST")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=int(os.getenv("CONSOLE_PORT", "8090")))
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    mimetypes.add_type("application/javascript", ".js")
    mimetypes.add_type("text/css", ".css")
    mimetypes.add_type("image/svg+xml", ".svg")
    mimetypes.add_type("font/ttf", ".ttf")
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    server.daemon_threads = True
    log.info("Floe console on http://localhost:%d", args.port)
    try:
        server.serve_forever()
    finally:
        WORKLOAD.stop()


if __name__ == "__main__":
    main()
