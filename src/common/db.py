"""Shared connection helpers for the lakehouse tools."""

from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import requests
import trino

REPORTS_DIR = Path(os.getenv("REPORTS_DIR", "/opt/results"))
QUERIES_DIR = Path(os.getenv("QUERIES_DIR", "/opt/queries"))
LAKE = "lakehouse.shop"
OLTP = "postgresql.shop"


def pg_connect(autocommit: bool = True) -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "oltp"),
        user=os.getenv("APP_DB_USER", "app"),
        password=os.getenv("APP_DB_PASSWORD", "app"),
        autocommit=autocommit,
    )


def trino_connect() -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user="lakehouse",
        catalog="lakehouse",
        schema="shop",
        request_timeout=600,
    )


class Trino:
    """Thin wrapper returning rows and Trino query statistics."""

    def __init__(self):
        self.conn = trino_connect()

    def query(self, sql: str, params=None):
        cur = self.conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        return rows, (cur.stats or {})

    def rows(self, sql: str, params=None):
        return self.query(sql, params)[0]

    def scalar(self, sql: str, params=None):
        rows = self.rows(sql, params)
        return rows[0][0] if rows else None


def wait_for_trino(timeout: int = 300) -> Trino:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            t = Trino()
            # needs an active worker (SELECT 1 is answered by the coordinator alone,
            # which can still raise NO_NODES_AVAILABLE for real queries at startup)
            t.rows("SELECT count(*) FROM tpch.tiny.region")
            return t
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(3)
    raise RuntimeError(f"Trino not reachable: {last}")


def lake_table_exists(t: Trino, table: str) -> bool:
    return bool(t.scalar(
        "SELECT count(*) FROM lakehouse.information_schema.tables "
        "WHERE table_schema = 'shop' AND table_name = ?", [table]))


class Airflow:
    def __init__(self):
        self.base = os.getenv("AIRFLOW_URL", "http://airflow:8080").rstrip("/") + "/api/v1"
        self.auth = (os.getenv("AIRFLOW_ADMIN_USER", "admin"), os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin"))

    def _req(self, method, path, **kw):
        r = requests.request(method, self.base + path, auth=self.auth, timeout=30, **kw)
        r.raise_for_status()
        return r.json() if r.content else {}

    def wait_ready(self, dag_id: str, timeout: int = 300):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                self._req("GET", f"/dags/{dag_id}")
                return
            except Exception:  # noqa: BLE001
                time.sleep(5)
        raise RuntimeError(f"Airflow DAG {dag_id} not available")

    def set_paused(self, dag_id: str, paused: bool) -> bool:
        before = self._req("GET", f"/dags/{dag_id}")["is_paused"]
        self._req("PATCH", f"/dags/{dag_id}", json={"is_paused": paused})
        return before

    def run(self, dag_id: str, conf: dict, timeout: int = 3600) -> dict:
        """Trigger a DAG run and block until it finishes. Returns run + task timings."""
        run_id = f"{dag_id}__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}"
        self._req("POST", f"/dags/{dag_id}/dagRuns", json={"dag_run_id": run_id, "conf": conf})
        deadline = time.time() + timeout
        state = "queued"
        while time.time() < deadline:
            state = self._req("GET", f"/dags/{dag_id}/dagRuns/{run_id}")["state"]
            if state in ("success", "failed"):
                break
            time.sleep(5)
        tasks = self._req("GET", f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")["task_instances"]
        result = {
            "dag_id": dag_id, "run_id": run_id, "state": state,
            "tasks": {ti["task_id"]: {"state": ti["state"], "duration_s": ti["duration"],
                                      "try_number": ti["try_number"]} for ti in tasks},
        }
        if state != "success":
            raise RuntimeError(f"Airflow run {run_id} ended in state {state}: {json.dumps(result)}")
        return result


def save_json(path: Path, data) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))
    return path


@contextmanager
def stopwatch():
    t = {"start": time.perf_counter()}
    yield t
    t["seconds"] = time.perf_counter() - t["start"]
