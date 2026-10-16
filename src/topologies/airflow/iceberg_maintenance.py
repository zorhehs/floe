"""
Airflow orchestration of Iceberg optimisation jobs.

Each task runs /opt/jobs/maintenance.py with spark-submit (Spark local mode
inside the Airflow container) against the same REST catalog + MinIO warehouse
the streaming job writes to. Commit conflicts with the streaming job are
resolved by Iceberg optimistic concurrency + task retries.

Airflow runs on SQLite with the SequentialExecutor (one task at a time), so a running
maintenance task delays a purge or layout run triggered meanwhile.

DAGs
  iceberg_maintenance         hourly housekeeping (active from the start: the
                              pipeline's heartbeat and metrics tables commit every
                              few seconds and need it; the benchmark pauses it)
  iceberg_layout_optimization manual: partition evolution + sorted compaction
                              + cleanup (used by the benchmark)
  iceberg_gdpr_purge          manual: physically erase deleted rows from a table

All parameters can be overridden with "Trigger DAG w/ config".
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

# Spark sizing lives in /opt/jobs/maintenance.sh (shared with `make maintenance-now` and the
# integration test): heap + capped off-heap memory must fit next to Airflow itself (~0.7 GB)
# inside the container limit; uncapped direct/metaspace memory got the job SIGKILLed.
MAINTENANCE = "/opt/jobs/maintenance.sh"
REPORT_DIR = "/opt/results/maintenance"

default_args = {
    "owner": "lakehouse",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


def spark_task(task_id: str, args: str) -> BashOperator:
    """One maintenance step. DAG params reach the job only through environment variables and
    numeric arguments, never by interpolation into the shell command: `params.tables` is
    free text from "Trigger DAG w/ config" and maintenance.py validates it."""
    out = f"{REPORT_DIR}/{{{{ dag.dag_id }}}}__{{{{ ts_nodash }}}}__{task_id}.json"
    return BashOperator(
        task_id=task_id,
        bash_command=f'mkdir -p {REPORT_DIR} && {MAINTENANCE} {args} --json-out {out}',
        env={"TABLES": "{{ params.tables }}", "RETAIN_LAST": "{{ params.get('retain_last', 10) | int }}"},
        append_env=True,  # keep PATH and the AWS/catalog variables of the container
        execution_timeout=timedelta(minutes=45),
    )


TABLE_ARG = '${TABLES:+--tables "$TABLES"}'


with DAG(
    dag_id="iceberg_maintenance",
    description="Hourly compaction, delete-file + manifest rewrite, snapshot expiry, orphan cleanup",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
    params={
        "tables": Param("", type="string", description="comma separated, empty = all"),
        "expire_older_than_minutes": Param(24 * 60, type="integer", minimum=0),
        "retain_last": Param(10, type="integer", minimum=1),
        "orphan_older_than_minutes": Param(24 * 60, type="integer", minimum=24 * 60,
                                           description="Iceberg enforces a minimum of 24 h"),
    },
    tags=["iceberg", "maintenance"],
) as maintenance:
    compact = spark_task("compact", f"{TABLE_ARG} --compact")
    rewrite = spark_task("rewrite_deletes_and_manifests",
                         f"{TABLE_ARG} --rewrite-deletes --rewrite-manifests")
    expire = spark_task("expire_snapshots",
                        f"{TABLE_ARG} --expire "
                        "--expire-older-than-minutes {{ params.expire_older_than_minutes | int }}")
    orphans = spark_task("remove_orphan_files",
                         f"{TABLE_ARG} --orphans "
                         "--orphan-older-than-minutes {{ params.orphan_older_than_minutes | int }}")
    compact >> rewrite >> expire >> orphans


with DAG(
    dag_id="iceberg_layout_optimization",
    description="Partition evolution + sort order, full sorted rewrite, cleanup",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
    params={
        "tables": Param("", type="string", description="comma separated, empty = all"),
        "expire_older_than_minutes": Param(0, type="integer", minimum=0),
        "retain_last": Param(1, type="integer", minimum=1),
        "orphan_older_than_minutes": Param(24 * 60, type="integer", minimum=24 * 60),
    },
    tags=["iceberg", "optimization"],
) as layout:
    apply_layout = spark_task("apply_layout_and_compact", f"{TABLE_ARG} --layout")
    rewrite = spark_task("rewrite_deletes_and_manifests",
                         f"{TABLE_ARG} --rewrite-deletes --rewrite-manifests")
    expire = spark_task("expire_snapshots",
                        f"{TABLE_ARG} --expire "
                        "--expire-older-than-minutes {{ params.expire_older_than_minutes | int }}")
    orphans = spark_task("remove_orphan_files",
                         f"{TABLE_ARG} --orphans "
                         "--orphan-older-than-minutes {{ params.orphan_older_than_minutes | int }}")
    apply_layout >> rewrite >> expire >> orphans


with DAG(
    dag_id="iceberg_gdpr_purge",
    description="Physically remove deleted rows (rewrite files with deletes, expire history)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
    params={"tables": Param("shop.customer,shop.orders", type="string")},
    tags=["iceberg", "gdpr"],
) as purge:
    spark_task("purge", '--tables "$TABLES" --purge')
