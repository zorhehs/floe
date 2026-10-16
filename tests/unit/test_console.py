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

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from console import app  # noqa: E402  (psycopg, trino, requests: see tests/requirements.txt)


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
        def __init__(self, user, session_properties=None):
            seen["user"] = user
            seen["session"] = session_properties
            self.conn = type("C", (), {"cursor": lambda _self: FakeCursor()})()

        def close(self):
            seen["closed"] = True

    monkeypatch.setattr(app, "Trino", FakeTrino)
    out = app.run_sql("SELECT 'a;b' AS x;")
    assert seen["user"] == app.WORKBENCH_USER and seen["sql"] == "SELECT 'a;b' AS x"
    assert seen["session"] == {"query_max_execution_time": f"{app.WORKBENCH_MAX_SECONDS}s"} and seen["closed"]
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
    server.server_close()


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
    for path in ("/../app.py", "/%2e%2e/app.py", "/static/../../app.py"):
        status, ctype, body = _get(base_url + path)
        assert status == 200 and b"<title>Floe Console</title>" in body  # falls back to the app shell
        assert b"ThreadingHTTPServer" not in body
    status, ctype, body = _get(base_url + "/app.js")
    assert ctype == "application/javascript; charset=utf-8"


def test_unknown_api_is_404_and_bad_input_is_400(base_url):
    with pytest.raises(urllib.error.HTTPError) as exc:
        _get(base_url + "/api/does-not-exist")
    assert exc.value.code == 404
    status, body = _post(base_url + "/api/sql", {"sql": "DELETE FROM lakehouse.shop.orders"})
    assert status == 400 and "read-only" in body["error"]
    status, body = _post(base_url + "/api/sql", {"sql": ["not", "a", "string"]})
    assert status == 400
    status, body = _post(base_url + "/api/workload", {"action": "explode"})
    assert status == 400
    status, body = _post(base_url + "/api/load", {"scale": "sf1000"})
    assert status == 400
    with pytest.raises(urllib.error.HTTPError) as exc:
        _get(base_url + "/api/batches?limit=abc")
    assert exc.value.code == 400
    status, body = _post(base_url + "/api/trace", {"action": "insert_order", "total_price": "lots"})
    assert status == 400
    status, body = _post(base_url + "/api/trace", {"action": "insert_order", "priority": "9-WHENEVER"})
    assert status == 400


def test_writes_require_same_origin_json(base_url):
    def raw_post(headers, data=b'{"action":"stop"}'):
        req = urllib.request.Request(base_url + "/api/workload", data=data, method="POST", headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                return r.status
        except urllib.error.HTTPError as e:
            return e.code

    assert raw_post({"Content-Type": "text/plain"}) == 403                      # cross-site simple request
    assert raw_post({"Content-Type": "application/json", "Origin": "http://evil.example"}) == 403
    assert raw_post({"Content-Type": "application/json", "Sec-Fetch-Site": "cross-site"}) == 403
    assert raw_post({"Content-Type": "application/json"}) == 200                # the console page itself
    assert raw_post({"Content-Type": "application/json", "Content-Length": "x"}) in (400, 411)


def test_head_and_oversized_bodies(base_url):
    req = urllib.request.Request(base_url + "/api/ping", method="HEAD")
    with urllib.request.urlopen(req, timeout=5) as r:
        assert r.status == 200 and int(r.headers["Content-Length"]) > 0 and r.read() == b""
    req = urllib.request.Request(base_url + "/api/workload", data=b"{}", method="POST",
                                 headers={"Content-Type": "application/json", "Content-Length": "2000000"})
    try:
        urllib.request.urlopen(req, timeout=5)
        raise AssertionError("expected 413")
    except urllib.error.HTTPError as e:
        assert e.code == 413


def test_encoded_airflow_run_ids_route(base_url, monkeypatch):
    seen = {}

    def fake_dag_run(dag_id, run_id):
        seen["run_id"] = run_id
        return {"dag_id": dag_id, "run": {"state": "success"}, "tasks": []}

    monkeypatch.setattr(app, "dag_run", fake_dag_run)
    status, _, body = _get(base_url + "/api/dags/iceberg_maintenance/runs/scheduled__2026-10-15T10%3A00%3A00%2B00%3A00")
    assert status == 200 and seen["run_id"] == "scheduled__2026-10-15T10:00:00+00:00"


def test_strip_sql_keeps_comment_markers_inside_string_literals():
    assert app.strip_sql("SELECT '--' AS x, '/* not a comment */' AS y -- real comment") == \
        "SELECT '--' AS x, '/* not a comment */' AS y"


def test_explain_analyze_only_for_read_only_statements():
    with pytest.raises(app.ApiError) as exc:
        app.run_sql("EXPLAIN ANALYZE DELETE FROM lakehouse.shop.orders")
    assert "EXPLAIN ANALYZE" in str(exc.value)


def test_trino_enforces_the_workbench_read_only_rule():
    """The keyword filter is cosmetic: compose must mount the access-control files into Trino."""
    compose = (ROOT / "docker-compose.yml").read_text()
    assert "config/trino/access-control.properties:/etc/trino/access-control.properties" in compose
    assert "config/trino/rules.json:/etc/trino/rules.json" in compose
    props = (ROOT / "config/trino/access-control.properties").read_text()
    assert "access-control.name=file" in props and "rules.json" in props


def test_metrics_never_emit_nan(monkeypatch):
    class R:
        def __init__(self, payload):
            self.payload = payload

        def json(self):
            return self.payload

    def fake_get(url, **kw):
        if "query_range" in url:
            return R({"data": {"result": [{"values": [[1, "NaN"], [2, "+Inf"], [3, "7.5"]]}]}})
        if '__name__=~' in kw.get("params", {}).get("query", ""):
            return R({"data": {"result": [{"metric": {"__name__": "lakehouse_ingest_batches_5m"}, "value": [1, "NaN"]}]}})
        return R({"data": {"result": [{"value": [1, "NaN"]}]}})

    monkeypatch.setattr(app, "_get", fake_get)
    out = app.metrics()
    assert out["error"] is None
    assert out["freshness_s"] == 7.5 and out["batches_5m"] is None and out["lake_rows"] is None
    assert out["series"]["freshness"] == [[1.0, None], [2.0, None], [3.0, 7.5]]
    json.dumps(out, allow_nan=False)  # must not raise


def test_jobs_never_evict_a_running_job():
    jobs = app.Jobs(keep=2)
    gate = threading.Event()

    def slow(say):
        gate.wait(5)
        return "done"

    running = jobs.start("slow", slow)
    for _ in range(4):
        _wait(jobs.start("quick", lambda say: 1), jobs)
    assert jobs.get(running["id"])["state"] == "running"
    assert jobs.running("slow")["id"] == running["id"]
    gate.set()
    assert _wait(running, jobs)["result"] == "done"


def test_ttl_cache_single_flight():
    cache, calls, release = app.TTLCache(), [], threading.Event()

    def fn():
        calls.append(1)
        release.wait(2)
        return "v"

    results = []
    threads = [threading.Thread(target=lambda: results.append(cache.get("k", 60, fn))) for _ in range(3)]
    for t in threads:
        t.start()
    time.sleep(0.1)
    release.set()
    for t in threads:
        t.join()
    assert results == ["v", "v", "v"] and len(calls) == 1


def test_workload_rejects_bad_rate_and_stop_without_start():
    runner = app.WorkloadRunner()
    with pytest.raises(app.ApiError):
        runner.start("abc")
    assert runner.stop()["running"] is False


def test_trigger_dag_normalises_table_names(monkeypatch):
    calls = []

    class FakeAirflow:
        def _req(self, method, path, **kw):
            calls.append((method, path, kw.get("json")))
            if method == "GET":
                return {"is_paused": False}
            return {"dag_run_id": "console__x", "state": "queued"}

    monkeypatch.setattr(app, "Airflow", FakeAirflow)
    app.trigger_dag("iceberg_maintenance", {"tables": "orders, shop.customer"})
    assert calls[-1][2]["conf"] == {"tables": "shop.orders,shop.customer"}
    app.trigger_dag("iceberg_gdpr_purge", {})
    assert calls[-1][2]["conf"] == {"tables": "shop.customer,shop.orders"}
    with pytest.raises(app.ApiError):
        app.trigger_dag("iceberg_maintenance", {"tables": "a b; drop"})
    with pytest.raises(app.ApiError):
        app.trigger_dag("not_a_dag", {})


def test_component_status_survives_failing_checks(monkeypatch):
    def boom():
        raise RuntimeError("down\nmore")

    monkeypatch.setattr(app, "COMPONENTS", [("x", "X", "role", boom, None)])
    out = app.component_status()
    assert out["healthy"] == 0 and out["total"] == 1
    assert out["components"][0]["ok"] is False and out["components"][0]["detail"] == "down"


def test_recorded_results_without_a_benchmark(monkeypatch, tmp_path):
    monkeypatch.setattr(app, "REPORTS_DIR", tmp_path)
    assert app.recorded_results() == {"benchmark": None, "ingest_load": None, "freshness": None}


def test_gdpr_routes_validate_input(base_url):
    with pytest.raises(urllib.error.HTTPError) as exc:
        _get(base_url + "/api/gdpr/candidate?key=abc")
    assert exc.value.code == 400
    status, body = _post(base_url + "/api/gdpr/purge", {})
    assert status == 400 and "snapshot_id" in body["error"]
    status, body = _post(base_url + "/api/gdpr/purge", {"snapshot_id": "not-a-number"})
    assert status == 400
    status, body = _post(base_url + "/api/gdpr/erase", {"key": "1 OR 1=1"})
    assert status == 400


def test_alert_rules_are_sorted_firing_first(monkeypatch):
    payload = {"data": {"groups": [{"rules": [
        {"type": "alerting", "name": "Quiet", "state": "inactive", "query": "x > 1", "duration": 120,
         "annotations": {"summary": "nothing"}, "alerts": []},
        {"type": "recording", "name": "rec:rate"},
        {"type": "alerting", "name": "Loud", "state": "firing", "query": "y > 2", "duration": 60,
         "annotations": {"summary": "bad"}, "alerts": [{"activeAt": "2026-10-15T10:00:00Z"}]},
        {"type": "alerting", "name": "Maybe", "state": "pending", "query": "z > 3", "alerts": []},
    ]}]}}

    class R:
        def json(self):
            return payload

    monkeypatch.setattr(app, "_get", lambda url, **kw: R())
    out = app.alert_rules()
    assert [r["name"] for r in out["rules"]] == ["Loud", "Maybe", "Quiet"]
    assert out["firing"] == 1 and out["pending"] == 1
    assert out["rules"][0]["since"] == "2026-10-15T10:00:00Z" and out["rules"][0]["for_seconds"] == 60
    assert out["rules"][2]["summary"] == "nothing"


def test_index_references_existing_assets():
    static = ROOT / "src/console/static"
    html = (static / "index.html").read_text()
    for asset in ("app.js", "app.css", "fonts.css", "favicon.svg"):
        assert f"/{asset}" in html and (static / asset).exists()
    for line in (static / "fonts.css").read_text().splitlines():
        if "url(" in line:
            assert (static / line.split("url(/")[1].split(")")[0]).exists()
    assert (static / "fonts/OFL-Outfit.txt").exists()
