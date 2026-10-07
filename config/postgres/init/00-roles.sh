#!/usr/bin/env bash
# Creates the application, Debezium and Iceberg-catalog roles (+ catalog database), then runs the schema script with
# the role names passed as psql variables (no credentials hard-coded in SQL).
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v app_user="$APP_DB_USER" -v app_password="$APP_DB_PASSWORD" \
  -v debezium_user="$DEBEZIUM_DB_USER" -v debezium_password="$DEBEZIUM_DB_PASSWORD" \
  -v catalog_user="$CATALOG_DB_USER" -v catalog_password="$CATALOG_DB_PASSWORD" <<'SQL'
CREATE ROLE :"app_user" LOGIN PASSWORD :'app_password';
CREATE ROLE :"debezium_user" LOGIN REPLICATION PASSWORD :'debezium_password';
GRANT CONNECT ON DATABASE :"DBNAME" TO :"debezium_user";
-- Separate database for the Iceberg REST catalog metadata (not CDC-captured).
CREATE ROLE :"catalog_user" LOGIN PASSWORD :'catalog_password';
CREATE DATABASE iceberg_catalog OWNER :"catalog_user";
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v app_user="$APP_DB_USER" -v debezium_user="$DEBEZIUM_DB_USER" \
  -f /docker-entrypoint-initdb.d/sql/01-schema.sql
