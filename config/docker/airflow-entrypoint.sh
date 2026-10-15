#!/usr/bin/env bash
# Airflow single-container setup: metadata DB migration, admin user, then
# scheduler + webserver + triggerer via `airflow standalone`.
set -euo pipefail

airflow db migrate
airflow users create \
  --username "${AIRFLOW_ADMIN_USER}" --password "${AIRFLOW_ADMIN_PASSWORD}" \
  --firstname Lakehouse --lastname Admin --role Admin --email admin@example.com \
  >/dev/null 2>&1 || true   # already exists after the first start

# The hourly housekeeping DAG must run for the pipeline to stay healthy: the heartbeat
# and metrics tables commit every few seconds and bloat without it (slow MERGEs, high
# freshness). `is_paused_upon_creation` only applies when a DAG is first registered, so
# a metadata database from an earlier version keeps it paused; unpause it on every start,
# once the scheduler has parsed the DAG folder.
(
  for _ in $(seq 1 90); do
    airflow dags unpause iceberg_maintenance >/dev/null 2>&1 && break
    sleep 10
  done
) &

exec airflow standalone
