"""Unit tests of the Floe Console server (no Trino / PostgreSQL needed).

The HTTP tests start the real handler on a free local port and only call
endpoints that do not reach the backing services.
"""

import json
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

pytest.importorskip("psycopg")
pytest.importorskip("trino")
pytest.importorskip("requests")

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from console import app  # noqa: E402


# ------------------------------------------------------------------ helpers
def test_strip_sql_removes_comments_and_trailing_semicolon():
    sql = "-- Q01 point lookup\n/* block\ncomment */ SELECT 1 ;  "
    assert app.strip_sql(sql) == "SELECT 1"


@pytest.mark.parametrize("sql, message", [
    ("", "empty"),
    ("DELETE FROM lakehouse.shop.orders", "read-only"),
    ("drop table lakehouse.shop.orders", "read-only"),
    ("SELECT 1; SELECT 2", "one statement"),
    ("SELECT 1; DELETE FROM lakehouse.shop.orders", "one statement"),
])
def test_workbench_rejects_writes_before_reaching_trino(sql, message):
    with pytest.raises(app.ApiError) as exc:
        app.run_sql(sql)
    assert message in str(exc.value)


def test_semicolon_inside_a_string_literal_is_allowed(monkeypatch):
    seen = {}

    class FakeCursor:
        description = [("x", "varchar")]
        stats = {"queryId": "q1"}

        def execute(self, sql):
            seen["sql"] = sql

        def fetchmany(self, n):
            return [("a;b",)]

    class FakeTrino:
        def __init__(self, user):
            seen["user"] = user
            self.conn = type("C", (), {"cursor": lambda _self: FakeCursor()})()

    monkeypatch.setattr(app, "Trino", FakeTrino)
    out = app.run_sql("SELECT 'a;b' AS x;")
    assert seen == {"user": app.WORKBENCH_USER, "sql": "SELECT 'a;b' AS x"}
    assert out["rows"] == [["a;b"]] and out["truncated"] is False


def test_workbench_runs_as_the_read_only_trino_user():
    rules = json.loads((ROOT / "config/trino/rules.json").read_text())
    first = rules["catalogs"][0]
    assert first == {"user": app.WORKBENCH_USER, "catalog": ".*", "allow": "read-only"}


def test_clean_keeps_snapshot_ids_exact_for_javascript():
    big = 7430993998702828328  # Iceberg snapshot id, beyond 2^53
    assert app.clean(big) == str(big)
    assert app.clean(42) == 42
    assert app.clean(Decimal("777.70")) == "777.70"
    assert app.clean(None) is None
    ts = datetime(2026, 10, 9, 16, 36, 11, 693788, tzinfo=timezone.utc)
    assert app.clean(ts) == "2026-10-09 16:36:11.693788+00:00"


@pytest.mark.parametrize("name, ok", [
    ("loyalty_tier", True), ("c1", True), ("Robert'); DROP", False), ("a b", False), ("1abc", False), ("", False),
])
def test_identifiers_are_validated(name, ok):
    if ok:
        assert app.ident(name) == name
    else:
        with pytest.raises(app.ApiError):
            app.ident(name)


def test_same_compares_decimals_by_value():
    assert app.same("777.7", Decimal("777.70"))
    assert not app.same("777.71", Decimal("777.70"))
    assert not app.same(None, Decimal("1"))
    assert app.same("Floe Demo", "Floe Demo")


def test_ttl_cache_reuses_value_within_ttl():
    cache, calls = app.TTLCache(), []
    fn = lambda: calls.append(1) or len(calls)  # noqa: E731
    assert cache.get("k", 60, fn) == 1
    assert cache.get("k", 60, fn) == 1
    cache.clear("k")
    assert cache.get("k", 60, fn) == 2


def _wait(job, jobs):
    for _ in range(100):
        if jobs.get(job["id"])["state"] != "running":
            return jobs.get(job["id"])
        time.sleep(0.02)
    raise AssertionError("job did not finish")


def test_jobs_report_progress_result_and_errors():
    jobs = app.Jobs()

    def work(say, x):
        say("step one")
        return x * 2

    done = _wait(jobs.start("t", work, 21), jobs)
    assert done["state"] == "done" and done["result"] == 42
    assert [line["msg"] for line in done["log"]] == ["step one"]

    def boom(say):
        raise RuntimeError("broken pipe\nstack details")

    failed = _wait(jobs.start("t", boom), jobs)
    assert failed["state"] == "failed" and failed["error"] == "broken pipe"


# ------------------------------------------------------------------ HTTP layer
@pytest.fixture(scope="module")
def base_url():
    server = ThreadingHTTPServer(("127.0.0.1", 0), app.Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def _get(url):
    with urllib.request.urlopen(url, timeout=5) as r:
        return r.status, r.headers.get("Content-Type"), r.read()


def _post(url, body):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def test_ping_and_index(base_url):
    status, ctype, body = _get(base_url + "/api/ping")
    assert status == 200 and ctype.startswith("application/json") and json.loads(body)["ok"] is True
    status, ctype, body = _get(base_url + "/")
    assert status == 200 and ctype.startswith("text/html") and b"<title>Floe Console</title>" in body


def test_static_files_cannot_escape_the_static_folder(base_url):
    status, ctype, body = _get(base_url + "/../app.py")
    assert status == 200 and b"<title>Floe Console</title>" in body  # falls back to the app shell
    assert b"ThreadingHTTPServer" not in body


def test_unknown_api_is_404_and_bad_input_is_400(base_url):
    with pytest.raises(urllib.error.HTTPError) as exc:
        _get(base_url + "/api/does-not-exist")
    assert exc.value.code == 404
    status, body = _post(base_url + "/api/sql", {"sql": "DELETE FROM lakehouse.shop.orders"})
    assert status == 400 and "read-only" in body["error"]
    status, body = _post(base_url + "/api/workload", {"action": "explode"})
    assert status == 400
    status, body = _post(base_url + "/api/load", {"scale": "sf1000"})
    assert status == 400


def test_index_references_existing_assets():
    static = ROOT / "src/console/static"
    html = (static / "index.html").read_text()
    for asset in ("app.js", "app.css", "fonts.css", "favicon.svg"):
        assert f"/{asset}" in html and (static / asset).exists()
    for line in (static / "fonts.css").read_text().splitlines():
        if "url(" in line:
            assert (static / line.split("url(/")[1].split(")")[0]).exists()
    assert (static / "fonts/OFL-Outfit.txt").exists()
