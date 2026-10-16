#!/usr/bin/env bash
# Integration test of the whole pipeline (used by CI, ~15 min). It leaves the stack running
# and the orders table re-laid out (partition evolution); use scripts/cleanup.sh --volumes
# to start fresh afterwards.
#  stack up -> TPC-H tiny load -> workload -> consistency -> schema evolution
#  -> update/delete propagation -> maintenance job keeps data identical.
set -euo pipefail
cd "$(dirname "$0")/../.."
DC="docker compose"
TOOLS="$DC --profile tools run --rm tools python3 -m"
fail() { echo "FAIL: $*"; $DC logs --tail 80 spark-streaming connect; exit 1; }
trino() { $DC exec -T trino trino --output-format TSV --execute "$1" | tail -n 1; }
wait_for() { for _ in $(seq 1 90); do [[ "$(trino "$1" 2>/dev/null)" == "$2" ]] && return 0; sleep 4; done; return 1; }

RUN=$(date +%s)   # run-unique values: a re-run against a kept stack must not pass on stale lake rows
$DC up -d
for _ in $(seq 1 120); do
  state=$($DC ps -a --format '{{.Service}} {{.State}} {{.ExitCode}}' | awk '$1=="connect-init"{print $2,$3}')
  [[ "$state" == "exited 0" ]] && break
  sleep 5
done
[[ "$state" == "exited 0" ]] || fail "Debezium connector registration did not succeed (connect-init: ${state:-missing})"

echo "== load"
$TOOLS producer.load_tpch --scale tiny --timeout 900 || fail "initial load not consistent"

echo "== workload"
$DC --profile tools run --rm tools python3 -m producer.generator --ops-per-sec 50 --duration 45
$TOOLS consumers.validate --wait 600 || fail "inconsistent after workload"

echo "== update + delete propagation"
$DC exec -T postgres psql -U app -d oltp -c "UPDATE shop.customer SET c_name='IT-$RUN' WHERE c_custkey=1; DELETE FROM shop.orders WHERE o_orderkey=(SELECT min(o_orderkey) FROM shop.orders);"
wait_for "SELECT c_name FROM lakehouse.shop.customer WHERE c_custkey=1" "IT-$RUN" || fail "update not propagated"
$TOOLS consumers.validate --wait 300 || fail "delete not propagated"

echo "== console (web interface)"
CONSOLE=http://localhost:8090
json() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
for _ in $(seq 1 30); do curl -fsS "$CONSOLE/api/ping" >/dev/null 2>&1 && break; sleep 2; done
curl -fsS "$CONSOLE/" | grep -q "<title>Floe Console</title>" || fail "console page not served"
for _ in $(seq 1 30); do
  health=$(curl -fsS "$CONSOLE/api/status" | json 'f"{d[\"healthy\"]}/{d[\"total\"]}"')
  [[ "${health%/*}" == "${health#*/}" ]] && break; sleep 5
done
[[ "${health%/*}" == "${health#*/}" ]] || fail "console reports $health components healthy"
curl -fsS -X POST "$CONSOLE/api/sql" -d '{"sql":"SELECT count(*) FROM lakehouse.shop.region"}' | grep -q '"rows": \[\[5\]\]' \
  || fail "console SQL workbench"
denied=$(curl -sS -X POST "$CONSOLE/api/sql" -d '{"sql":"EXPLAIN ANALYZE INSERT INTO lakehouse.shop.region SELECT * FROM lakehouse.shop.region"}')
grep -q "Access Denied" <<<"$denied" || fail "workbench user is not read-only: $denied"
balance="$((RUN % 9000 + 100)).$((RUN % 89 + 10))"
job=$(curl -fsS -X POST "$CONSOLE/api/trace" -d "{\"action\":\"update_customer\",\"key\":3,\"balance\":\"$balance\"}" | json 'd["id"]')
for _ in $(seq 1 90); do
  state=$(curl -fsS "$CONSOLE/api/jobs/$job" | json 'd["state"]'); [[ "$state" != running ]] && break; sleep 2
done
[[ "$state" == done ]] || fail "console live CDC trace: $(curl -fsS "$CONSOLE/api/jobs/$job")"
wait_for "SELECT CAST(c_acctbal AS varchar) FROM lakehouse.shop.customer WHERE c_custkey=3" "$balance" || fail "trace change not in lake"
rules=$(grep -c "^      - alert:" config/monitoring/alerts.yml)
[[ "$(curl -fsS "$CONSOLE/api/alerts" | json 'len(d["rules"])')" == "$rules" ]] || fail "console does not list the $rules Prometheus alert rules"
[[ "$(curl -fsS "$CONSOLE/api/gdpr/candidate" | json 'd["orders"] > 0')" == "True" ]] || fail "console GDPR candidate has no orders"

echo "== schema evolution"
marker=$((RUN % 100000 + 1))
$DC exec -T postgres psql -U app -d oltp -c "ALTER TABLE shop.customer ADD COLUMN IF NOT EXISTS it_col integer; UPDATE shop.customer SET it_col = $marker WHERE c_custkey = 2;"
wait_for "SELECT it_col FROM lakehouse.shop.customer WHERE c_custkey=2" "$marker" || fail "new column not propagated"

echo "== maintenance keeps data identical"
$DC exec -T -e RETAIN_LAST=1 airflow /opt/jobs/maintenance.sh \
  --layout --rewrite-deletes --rewrite-manifests --expire --expire-older-than-minutes 0 \
  || fail "maintenance job failed"
$TOOLS consumers.validate --wait 120 || fail "data changed by maintenance"
echo "ALL INTEGRATION CHECKS PASSED"
