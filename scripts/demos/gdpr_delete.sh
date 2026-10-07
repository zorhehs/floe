#!/usr/bin/env bash
# Demo: GDPR "right to erasure" on a lakehouse.
#  - the source DELETE arrives via CDC as a row-level delete (merge-on-read:
#    a small position-delete file, the data file is untouched)
#  - the row is gone from current queries but still readable via time travel
#  - the purge job rewrites affected files and expires history -> physically gone
source "$(dirname "$0")/lib.sh"

CUST=$($DC exec -T postgres psql -U app -d oltp -tAc "SELECT c_custkey FROM shop.customer WHERE c_custkey > 1000 ORDER BY c_custkey LIMIT 1")

banner "1. Customer $CUST and their orders in the lakehouse"
trino_run "SELECT c_custkey, c_name, c_phone, c_address FROM customer WHERE c_custkey = $CUST"
trino_run "SELECT count(*) AS orders FROM orders WHERE o_custkey = $CUST"
SNAP=$(trino_value "SELECT snapshot_id FROM \"customer\$snapshots\" ORDER BY committed_at DESC LIMIT 1")

banner "2. Erasure request executed in the operational database"
psql_run "BEGIN; DELETE FROM shop.orders WHERE o_custkey = $CUST; DELETE FROM shop.customer WHERE c_custkey = $CUST; COMMIT;"
wait_until "SELECT count(*) FROM customer WHERE c_custkey = $CUST" "0"
wait_until "SELECT count(*) FROM orders WHERE o_custkey = $CUST" "0"

banner "3. Merge-on-read: deletes are recorded in delete files, data files untouched"
trino_run "SELECT content, count(*) AS files FROM \"customer\$files\" GROUP BY content ORDER BY content"
note "content 0 = data files, 1 = position delete files"

banner "4. Problem: the personal data is still readable through time travel"
trino_run "SELECT c_custkey, c_name, c_phone FROM customer FOR VERSION AS OF $SNAP WHERE c_custkey = $CUST"

banner "5. Purge: Airflow DAG iceberg_gdpr_purge (rewrite files with deletes + expire snapshots)"
AIRFLOW="http://localhost:8088/api/v1"
AUTH="${AIRFLOW_ADMIN_USER:-admin}:${AIRFLOW_ADMIN_PASSWORD:-admin}"
RUN_ID="gdpr_purge_$(date +%s)"
curl -fsS -u "$AUTH" -H 'Content-Type: application/json' -X POST "$AIRFLOW/dags/iceberg_gdpr_purge/dagRuns" \
  -d "{\"dag_run_id\": \"$RUN_ID\", \"conf\": {\"tables\": \"shop.customer,shop.orders\"}}" >/dev/null
note "triggered Airflow run $RUN_ID - waiting for the Spark purge job ..."
STATE=queued
for _ in $(seq 1 240); do
  STATE=$(curl -fsS -u "$AUTH" "$AIRFLOW/dags/iceberg_gdpr_purge/dagRuns/$RUN_ID" | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])')
  [[ "$STATE" == "success" || "$STATE" == "failed" ]] && break
  sleep 5
done
note "purge run state: $STATE"
[[ "$STATE" == "success" ]] || { echo "purge failed - see Airflow UI (http://localhost:8088)" >&2; exit 1; }

banner "6. After purge"
trino_run "SELECT content, count(*) AS files FROM \"customer\$files\" GROUP BY content ORDER BY content"
trino_run "SELECT count(*) AS snapshots FROM \"customer\$snapshots\""
note "time travel to the pre-erasure snapshot is no longer possible:"
trino_run "SELECT c_name FROM customer FOR VERSION AS OF $SNAP WHERE c_custkey = $CUST" || note "-> snapshot expired: the erased data no longer exists in storage"
