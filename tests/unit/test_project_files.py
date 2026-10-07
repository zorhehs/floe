"""Static checks of configuration files (no services needed)."""

import json
from pathlib import Path

import pytest

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


def test_env_example_matches_env_keys():
    example = {line.split("=")[0] for line in (ROOT / ".env.example").read_text().splitlines()
               if "=" in line and not line.startswith("#")}
    for required in ("POSTGRES_DB", "MINIO_ROOT_USER", "AIRFLOW_ADMIN_USER", "TPCH_SCALE"):
        assert required in example


def test_bench_formatting():
    pytest.importorskip("boto3")
    import sys
    sys.path.insert(0, str(ROOT / "src"))
    from consumers.bench import fmt_bytes
    assert fmt_bytes(512) == "512.0 B"
    assert fmt_bytes(3 * 1024 * 1024) == "3.0 MB"
