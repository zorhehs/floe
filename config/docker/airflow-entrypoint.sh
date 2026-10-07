#!/usr/bin/env bash
# Airflow single-container setup: metadata DB migration, admin user, then
# scheduler + webserver + triggerer via `airflow standalone`.
set -euo pipefail

airflow db migrate
airflow users create \
  --username "${AIRFLOW_ADMIN_USER}" --password "${AIRFLOW_ADMIN_PASSWORD}" \
  --firstname Lakehouse --lastname Admin --role Admin --email admin@example.com \
  >/dev/null 2>&1 || true   # already exists after the first start

exec airflow standalone
