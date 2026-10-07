#!/usr/bin/env bash
# Demo: schema evolution travels from PostgreSQL through Debezium into Iceberg
# without stopping the pipeline (new column + type widening).
source "$(dirname "$0")/lib.sh"

banner "1. Current Iceberg schema of shop.customer and shop.orders"
trino_run "DESCRIBE customer"
trino_run "SELECT data_type FROM information_schema.columns WHERE table_schema='shop' AND table_name='orders' AND column_name='o_shippriority'"

banner "2. Add a column in the source database (an application release)"
psql_run "ALTER TABLE shop.customer ADD COLUMN IF NOT EXISTS c_loyalty_tier varchar(10)"
note "Debezium sends the new schema with the next change to the table:"
psql_run "UPDATE shop.customer SET c_loyalty_tier = CASE WHEN c_acctbal > 5000 THEN 'GOLD' ELSE 'SILVER' END, updated_at = localtimestamp WHERE c_custkey BETWEEN 1 AND 20"
wait_until "SELECT count(*) FROM information_schema.columns WHERE table_schema='shop' AND table_name='customer' AND column_name='c_loyalty_tier'" "1"
trino_run "SELECT c_custkey, c_name, c_acctbal, c_loyalty_tier FROM customer WHERE c_custkey <= 5 ORDER BY 1"
note "Old rows that were not changed read NULL for the new column (schema-on-read):"
trino_run "SELECT c_loyalty_tier IS NULL AS tier_missing, count(*) FROM customer GROUP BY 1"

banner "3. Widen a column type in the source: integer -> bigint"
psql_run "ALTER TABLE shop.orders ALTER COLUMN o_shippriority TYPE bigint"
psql_run "UPDATE shop.orders SET o_shippriority = 3000000000, updated_at = localtimestamp WHERE o_orderkey = (SELECT min(o_orderkey) FROM shop.orders)"
wait_until "SELECT data_type FROM information_schema.columns WHERE table_schema='shop' AND table_name='orders' AND column_name='o_shippriority'" "bigint"
trino_run "SELECT o_orderkey, o_shippriority FROM orders WHERE o_shippriority > 2147483647"

banner "4. Every schema change is an Iceberg metadata version (no data rewritten)"
trino_run "SELECT file, latest_schema_id FROM \"customer\$metadata_log_entries\" ORDER BY timestamp DESC LIMIT 5"
trino_run "SELECT batch_id, table_name, schema_changes FROM lakehouse.ops.ingest_batches WHERE schema_changes IS NOT NULL ORDER BY batch_id"
