"""Static checks of configuration files (no services needed)."""

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_debezium_connector_config():
    cfg = json.loads((ROOT / "config/debezium/postgres-cdc.json").read_text())
    assert cfg["plugin.name"] == "pgoutput"
    assert cfg["decimal.handling.mode"] == "string"          # exact decimals, mapped by the job
    assert cfg["topic.prefix"] == "pg"                       # job subscribes to pg.shop.*
    assert "${env:" in cfg["database.password"]              # no literal secrets


def test_query_suite_is_readonly_and_targets_lakehouse():
    queries = sorted((ROOT / "src/consumers/queries").glob("q*.sql"))
    assert len(queries) >= 8
    for q in queries:
        sql = q.read_text().lower()
        assert "lakehouse.shop." in sql
        for verb in ("insert ", "update ", "delete ", "drop ", "alter "):
            assert verb not in sql, f"{q.name} must be read-only"


def test_grafana_dashboard_is_valid():
    dash = json.loads((ROOT / "config/monitoring/grafana/dashboards/lakehouse.json").read_text())
    assert dash["uid"] == "lakehouse-cdc"
    exprs = [t["expr"] for p in dash["panels"] for t in p["targets"]]
    assert any("lakehouse_cdc_freshness_seconds" in e for e in exprs)


def test_env_example_covers_every_compose_variable():
    example = {line.split("=")[0] for line in (ROOT / ".env.example").read_text().splitlines()
               if "=" in line and not line.startswith("#")}
    compose = (ROOT / "docker-compose.yml").read_text()
    referenced = set(re.findall(r"\$\{([A-Z_][A-Z0-9_]*)", compose))
    assert referenced, "no ${VAR} references found in docker-compose.yml"
    assert referenced <= example, f"missing from .env.example: {sorted(referenced - example)}"


def test_dashboard_and_alerts_only_use_metrics_the_exporter_exports():
    exporter = (ROOT / "src/consumers/exporter.py").read_text()
    gauges = set(re.findall(r'Gauge\("(lakehouse_\w+)"', exporter))
    assert len(gauges) >= 15
    for rel in ("config/monitoring/grafana/dashboards/lakehouse.json", "config/monitoring/alerts.yml"):
        used = set(re.findall(r"\b(lakehouse_[a-z0-9_]+)", (ROOT / rel).read_text()))
        assert used, rel
        assert used <= gauges, f"{rel} references metrics the exporter never exports: {sorted(used - gauges)}"


def test_bench_formatting(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "src"))
    from consumers.bench import fmt_bytes
    assert fmt_bytes(512) == "512.0 B"
    assert fmt_bytes(3 * 1024 * 1024) == "3.0 MB"
