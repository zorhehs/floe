-- ---------------------------------------------------------------------------
-- Operational (OLTP) source database for the CDC pipeline.
-- Runs once, on first start of an empty data directory, as the superuser.
-- Uses psql variables passed in by 00-roles.sh.
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS shop AUTHORIZATION :"app_user";
SET ROLE :"app_user";

-- TPC-H shaped tables (loaded from Trino's tpch connector by `make load`).
CREATE TABLE shop.region (
    r_regionkey  integer      PRIMARY KEY,
    r_name       varchar(25)  NOT NULL,
    r_comment    varchar(152)
);

CREATE TABLE shop.nation (
    n_nationkey  integer      PRIMARY KEY,
    n_name       varchar(25)  NOT NULL,
    n_regionkey  integer      NOT NULL REFERENCES shop.region (r_regionkey),
    n_comment    varchar(152)
);

CREATE TABLE shop.customer (
    c_custkey     bigint         PRIMARY KEY,
    c_name        varchar(25)    NOT NULL,
    c_address     varchar(40)    NOT NULL,
    c_nationkey   integer        NOT NULL REFERENCES shop.nation (n_nationkey),
    c_phone       varchar(15)    NOT NULL,
    c_acctbal     numeric(12, 2) NOT NULL,
    c_mktsegment  varchar(10)    NOT NULL,
    c_comment     varchar(117),
    updated_at    timestamp(6)   NOT NULL DEFAULT localtimestamp
);

-- No FK from orders to customer on purpose: the workload generator deletes
-- customers (GDPR-style erasure) independently of their order history.
CREATE TABLE shop.orders (
    o_orderkey       bigint         PRIMARY KEY,
    o_custkey        bigint         NOT NULL,
    o_orderstatus    varchar(1)     NOT NULL,
    o_totalprice     numeric(12, 2) NOT NULL,
    o_orderdate      date           NOT NULL,
    o_orderpriority  varchar(15)    NOT NULL,
    o_clerk          varchar(15)    NOT NULL,
    o_shippriority   integer        NOT NULL,
    o_comment        varchar(79),
    updated_at       timestamp(6)   NOT NULL DEFAULT localtimestamp
);
CREATE INDEX orders_custkey_idx ON shop.orders (o_custkey);

-- Heartbeat table used by the benchmark to measure end-to-end CDC freshness.
CREATE TABLE shop.cdc_probe (
    probe_id    bigint        PRIMARY KEY,
    created_at  timestamptz   NOT NULL DEFAULT clock_timestamp()
);

-- Keys for rows created by the workload generator (above TPC-H sf1 ranges).
CREATE SEQUENCE shop.order_key_seq START WITH 10000001;
CREATE SEQUENCE shop.customer_key_seq START WITH 1000001;

RESET ROLE;

-- Debezium: read-only access + logical replication.
GRANT USAGE ON SCHEMA shop TO :"debezium_user";
GRANT SELECT ON ALL TABLES IN SCHEMA shop TO :"debezium_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"app_user" IN SCHEMA shop GRANT SELECT ON TABLES TO :"debezium_user";

-- Publication covers every current *and future* table in the schema,
-- so tables created later are captured without reconfiguring Debezium.
CREATE PUBLICATION dbz_publication FOR TABLES IN SCHEMA shop;
