#!/bin/sh
# Registers (or updates) the Debezium PostgreSQL connector. Idempotent: PUT on
# /connectors/<name>/config creates the connector or replaces its config. A task
# that is FAILED (for example after a transient database outage) is restarted,
# because an unchanged config PUT does not restart failed tasks on its own.
set -eu

CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
NAME="postgres-cdc"

echo "Waiting for Kafka Connect at ${CONNECT_URL} ..."
until curl -fsS "${CONNECT_URL}/connector-plugins" | grep -q PostgresConnector; do
  sleep 3
done

echo "Registering connector ${NAME}"
curl -fsS -X PUT -H "Content-Type: application/json" \
  --data @/debezium/postgres-cdc.json \
  "${CONNECT_URL}/connectors/${NAME}/config"
echo

# Wait until the connector task is RUNNING; restart FAILED tasks along the way.
i=0
status=""
while [ $i -lt 40 ]; do
  status="$(curl -fs "${CONNECT_URL}/connectors/${NAME}/status" || true)"
  case "$status" in
    *'"tasks":[{"id":0,"state":"RUNNING"'*) echo "Connector RUNNING"; exit 0 ;;
    *FAILED*)
      echo "Connector or task FAILED, restarting it"
      curl -fsS -X POST "${CONNECT_URL}/connectors/${NAME}/restart?includeTasks=true&onlyFailed=true" >/dev/null || true
      sleep 5 ;;
  esac
  i=$((i + 1)); sleep 3
done
echo "Connector did not reach RUNNING: $status"
exit 1
