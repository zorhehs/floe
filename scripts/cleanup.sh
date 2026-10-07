#!/usr/bin/env bash
# Stop the stack. --volumes also deletes all data (PostgreSQL, Kafka, MinIO,
# catalog, checkpoints, Airflow, Prometheus, Grafana) for a fresh start.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ "${1:-}" == "--volumes" ]]; then
  docker compose --profile tools --profile workload down -v --remove-orphans
  rm -rf results/maintenance
  echo "Stack stopped and all data volumes removed."
else
  docker compose --profile tools --profile workload down --remove-orphans
  echo "Stack stopped (data kept). Use --volumes to delete data."
fi
