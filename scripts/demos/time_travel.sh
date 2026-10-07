#!/usr/bin/env bash
# Demo: every streaming MERGE is an Iceberg snapshot; query any of them.
source "$(dirname "$0")/lib.sh"

KEY=$($DC exec -T postgres psql -U app -d oltp -tAc "SELECT min(o_orderkey) FROM shop.orders WHERE o_orderkey > 100 AND o_orderkey < 1000")

banner "1. Snapshot history of shop.orders (one per micro-batch / maintenance job)"
trino_run "SELECT committed_at, snapshot_id, operation, element_at(summary, 'added-records') AS added, element_at(summary, 'added-position-deletes') AS pos_deletes FROM \"orders\$snapshots\" ORDER BY committed_at DESC LIMIT 8"

SNAP=$(trino_value "SELECT snapshot_id FROM \"orders\$snapshots\" ORDER BY committed_at DESC LIMIT 1")
TS=$(trino_value "SELECT CAST(current_timestamp AS varchar)")
note "remembering snapshot $SNAP and time $TS"
trino_run "SELECT o_orderkey, o_orderstatus, o_totalprice FROM orders WHERE o_orderkey = $KEY"

banner "2. Change the order in PostgreSQL"
PRICE="$(( RANDOM % 90000 + 10000 )).$(( RANDOM % 90 + 10 ))"   # new value on every run
psql_run "UPDATE shop.orders SET o_totalprice = $PRICE, o_orderstatus = 'F', updated_at = localtimestamp WHERE o_orderkey = $KEY"
wait_until "SELECT CAST(o_totalprice AS varchar) FROM orders WHERE o_orderkey = $KEY" "$PRICE"

banner "3. Current state vs. the past"
trino_run "SELECT 'current' AS version, o_orderstatus, o_totalprice FROM orders WHERE o_orderkey = $KEY
UNION ALL SELECT 'snapshot $SNAP', o_orderstatus, o_totalprice FROM orders FOR VERSION AS OF $SNAP WHERE o_orderkey = $KEY
UNION ALL SELECT 'as of $TS', o_orderstatus, o_totalprice FROM orders FOR TIMESTAMP AS OF TIMESTAMP '$TS' WHERE o_orderkey = $KEY"

banner "4. Audit: what changed in the lake since that snapshot (CDC metadata columns)"
trino_run "SELECT _cdc_op AS op, count(*) AS rows_changed FROM orders WHERE _ingested_at > (SELECT committed_at FROM \"orders\$snapshots\" WHERE snapshot_id = $SNAP) GROUP BY 1"
note "row-level diff of the two versions (a key range keeps it cheap):"
trino_run "SELECT * FROM orders WHERE o_orderkey BETWEEN 1 AND 1000 EXCEPT SELECT * FROM orders FOR VERSION AS OF $SNAP WHERE o_orderkey BETWEEN 1 AND 1000"
