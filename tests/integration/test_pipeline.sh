#!/usr/bin/env bash
# Integration test of the whole pipeline (used by CI, ~15 min):
#  stack up -> TPC-H tiny load -> workload -> consistency -> schema evolution
#  -> update/delete propagation -> maintenance job keeps data identical.
set -euo pipefail
cd "$(dirname "$0")/../.."
DC="docker compose"
TOOLS="$DC --profile tools run --rm tools python3 -m"
fail() { echo "FAIL: $*"; $DC logs --tail 80 spark-streaming connect; exit 1; }
trino() { $DC exec -T trino trino --output-format TSV --execute "$1" | tail -n 1; }
wait_for() { for _ in $(seq 1 90); do [[ "$(trino "$1" 2>/dev/null)" == "$2" ]] && return 0; sleep 4; done; return 1; }

$DC up -d
for _ in $(seq 1 120); do
  [[ "$($DC ps -a --format '{{.Service}} {{.State}} {{.ExitCode}}' | awk '$1=="connect-init"{print $2,$3}')" == "exited 0" ]] && break
  sleep 5
done

echo "== load"
$TOOLS producer.load_tpch --scale tiny --timeout 900 || fail "initial load not consistent"

echo "== workload"
$DC --profile tools run --rm tools python3 -m producer.generator --ops-per-sec 50 --duration 45
$TOOLS consumers.validate --wait 600 || fail "inconsistent after workload"

echo "== update + delete propagation"
$DC exec -T postgres psql -U app -d oltp -c "UPDATE shop.customer SET c_name='IT-UPDATED' WHERE c_custkey=1; DELETE FROM shop.orders WHERE o_orderkey=(SELECT min(o_orderkey) FROM shop.orders);"
wait_for "SELECT c_name FROM lakehouse.shop.customer WHERE c_custkey=1" "IT-UPDATED" || fail "update not propagated"
$TOOLS consumers.validate --wait 300 || fail "delete not propagated"

echo "== schema evolution"
$DC exec -T postgres psql -U app -d oltp -c "ALTER TABLE shop.customer ADD COLUMN IF NOT EXISTS it_col integer; UPDATE shop.customer SET it_col = 7 WHERE c_custkey = 2;"
wait_for "SELECT it_col FROM lakehouse.shop.customer WHERE c_custkey=2" "7" || fail "new column not propagated"

echo "== maintenance keeps data identical"
$DC exec -T airflow spark-submit --master 'local[2]' --driver-memory 1200m --conf 'spark.driver.extraJavaOptions=-Daws.region=us-east-1 -XX:MaxDirectMemorySize=256m -XX:MaxMetaspaceSize=256m' /opt/jobs/maintenance.py \
  --layout --rewrite-deletes --rewrite-manifests --expire --expire-older-than-minutes 0 --retain-last 1 \
  || fail "maintenance job failed"
$TOOLS consumers.validate --wait 120 || fail "data changed by maintenance"
echo "ALL INTEGRATION CHECKS PASSED"
