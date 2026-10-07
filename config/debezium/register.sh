#!/bin/sh
# Registers (or updates) the Debezium PostgreSQL connector. Idempotent: PUT on
# /connectors/<name>/config creates the connector or replaces its config.
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

# Wait until the connector task is RUNNING (fails loudly otherwise).
i=0
while [ $i -lt 40 ]; do
  status="$(curl -fs "${CONNECT_URL}/connectors/${NAME}/status" || true)"
  case "$status" in
    *'"tasks":[{"id":0,"state":"RUNNING"'*) echo "Connector RUNNING"; exit 0 ;;
    *FAILED*) echo "Connector FAILED: $status"; exit 1 ;;
  esac
  i=$((i + 1)); sleep 3
done
echo "Connector did not reach RUNNING: $status"
exit 1
