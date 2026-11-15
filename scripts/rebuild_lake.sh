#!/usr/bin/env bash
# Rebuild the Iceberg lakehouse from a fresh Debezium snapshot of PostgreSQL.
#
# Use it when the stream cannot resume, for example when the Spark job fails with
# OffsetOutOfRange / "Cannot fetch offset": Kafka no longer holds change events the
# checkpoint still needs, so the lake would silently miss them. PostgreSQL, the
# source of truth, is not touched.
#
#   ./scripts/rebuild_lake.sh [--yes]
#
# Steps: stop the streaming job -> stop Debezium and reset its offsets -> drop the
# replication slot -> delete the CDC topics -> drop the Iceberg tables -> delete the
# Spark checkpoint -> resume Debezium (it takes a new consistent snapshot) -> start
# the streaming job -> wait until PostgreSQL and Iceberg are identical.
set -euo pipefail
cd "$(dirname "$0")/.."

DC="docker compose"
CONNECT=http://localhost:8083
NAME=postgres-cdc
SLOT=debezium_shop
step() { printf '\n\033[1;34m### %s\033[0m\n' "$*"; }
connector_state() { curl -fsS "$CONNECT/connectors/$NAME/status" | python3 -c 'import json,sys; print(json.load(sys.stdin)["connector"]["state"])'; }
trino() { $DC exec -T trino trino --output-format TSV --execute "$1" 2>/dev/null; }

if [[ "${1:-}" != "--yes" ]]; then
  echo "This drops every Iceberg table and the Kafka CDC topics, then re-imports all rows from PostgreSQL."
  read -r -p "Continue? [y/N] " answer
  [[ "$answer" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 1; }
fi

step "1/8 Stop the streaming job"
$DC rm -sf spark-streaming

step "2/8 Stop Debezium and reset its offsets"
curl -fsS -X PUT "$CONNECT/connectors/$NAME/stop" >/dev/null
for _ in $(seq 1 30); do [[ "$(connector_state)" == STOPPED ]] && break; sleep 2; done
[[ "$(connector_state)" == STOPPED ]] || { echo "connector did not stop"; exit 1; }
curl -fsS -X DELETE "$CONNECT/connectors/$NAME/offsets" >/dev/null

step "3/8 Drop the replication slot (Debezium recreates it with a consistent snapshot)"
for _ in $(seq 1 30); do
  active=$($DC exec -T postgres psql -U postgres -d "${POSTGRES_DB:-oltp}" -tAc \
    "SELECT coalesce(bool_or(active), false) FROM pg_replication_slots WHERE slot_name = '$SLOT'")
  [[ "$active" == f ]] && break; sleep 2
done
$DC exec -T postgres psql -U postgres -d "${POSTGRES_DB:-oltp}" -tAc \
  "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '$SLOT'" >/dev/null

step "4/8 Delete the CDC topics"
topics=$($DC exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -E '^pg\.' || true)
for t in $topics; do
  $DC exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$t"
done
for _ in $(seq 1 30); do
  $DC exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -qE '^pg\.' || break
  sleep 2
done

step "5/8 Drop the Iceberg tables"
for ns in shop ops; do
  for t in $(trino "SELECT table_name FROM lakehouse.information_schema.tables WHERE table_schema = '$ns'"); do
    echo "DROP TABLE lakehouse.$ns.$t"
    trino "DROP TABLE lakehouse.$ns.\"$t\"" >/dev/null
  done
done

step "6/8 Delete the Spark checkpoint"
project=$($DC config --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["name"])')
docker volume rm "${project}_spark-checkpoints" >/dev/null 2>&1 || true

step "7/8 Resume Debezium with the current config (initial snapshot), start the streaming job"
curl -fsS -X PUT -H "Content-Type: application/json" --data @config/debezium/postgres-cdc.json \
  "$CONNECT/connectors/$NAME/config" >/dev/null
curl -fsS -X PUT "$CONNECT/connectors/$NAME/resume" >/dev/null
$DC up -d spark-streaming

step "8/8 Wait until PostgreSQL and Iceberg are identical"
$DC --profile tools run --rm tools python3 -m consumers.validate --wait 3600
echo "Lakehouse rebuilt from PostgreSQL."
