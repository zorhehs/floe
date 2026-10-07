"""
Debezium CDC (Kafka) -> Apache Iceberg ingestion with Spark Structured Streaming.

For every micro-batch and every captured table (one Kafka topic per table):

1. Discover the row schema from the Debezium JSON envelope (`schema` part of
   each message). Schemas can change inside a batch, so all distinct schemas are
   merged; the newest one wins for types.
2. Create the Iceberg table on first sight, or evolve it: new source columns
   are added and compatible type changes (int->bigint, float->double, wider
   decimal) are applied with ALTER TABLE (schema evolution).
3. Keep only the latest change per primary key (ordered by Kafka offset).
4. Apply the batch with one MERGE INTO: deletes, updates and inserts. MERGE is
   idempotent, so a replayed micro-batch after a crash yields the same table
   state (effectively exactly-once together with the Spark checkpoint).
5. Append per-table batch statistics to `lakehouse.ops.ingest_batches`.

Tables use Iceberg format v2 with merge-on-read so the streaming writes stay
cheap; the resulting small/delete files are cleaned up by the maintenance jobs.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_PATTERN = os.getenv("CDC_TOPIC_PATTERN", r"pg\.shop\..*")
CATALOG = os.getenv("ICEBERG_CATALOG", "lakehouse")
CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/opt/checkpoints/cdc_to_iceberg")
TRIGGER = os.getenv("TRIGGER_INTERVAL", "10 seconds")
MAX_OFFSETS = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "100000"))
METRICS_TABLE = f"{CATALOG}.ops.ingest_batches"
COMMIT_RETRIES = 6

# Columns added by the pipeline to every lakehouse table.
META_FIELDS = [
    T.StructField("_cdc_op", T.StringType()),           # c / r / u (latest op applied)
    T.StructField("_cdc_source_ts", T.TimestampType()),  # commit time in PostgreSQL
    T.StructField("_cdc_lsn", T.LongType()),             # PostgreSQL WAL position
    T.StructField("_ingested_at", T.TimestampType()),    # time the batch was merged
]
META_NAMES = [f.name for f in META_FIELDS]

TABLE_PROPERTIES = {
    "format-version": "2",
    "write.delete.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.merge.mode": "merge-on-read",
    "write.parquet.compression-codec": "zstd",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "100",
}

# Errors caused by a concurrent commit (e.g. compaction running in Airflow).
RETRYABLE_MARKERS = (
    "CommitFailedException",
    "ValidationException",
    "Found conflicting",
    "Cannot commit",
    "CommitStateUnknownException",
)

log = logging.getLogger("cdc_to_iceberg")


# ---------------------------------------------------------------------------
# Debezium schema -> Spark types
# ---------------------------------------------------------------------------
def q(name: str) -> str:
    """Quote an identifier for Spark SQL."""
    return "`" + name.replace("`", "``") + "`"


def raw_type(field: dict) -> T.DataType:
    """Type used to read the JSON value as Debezium serialises it."""
    typ = field["type"]
    if typ in ("int8", "int16", "int32", "int64"):
        return T.LongType()
    if typ in ("float32", "float64"):
        return T.DoubleType()
    if typ == "boolean":
        return T.BooleanType()
    return T.StringType()  # string, bytes (base64), nested struct/array as JSON text


def target_type(field: dict) -> T.DataType:
    """Lakehouse column type for a Debezium field."""
    name = field.get("name")
    typ = field["type"]
    params = field.get("parameters") or {}

    if name == "io.debezium.time.Date":
        return T.DateType()
    if name in ("io.debezium.time.MicroTimestamp", "io.debezium.time.Timestamp",
                "io.debezium.time.NanoTimestamp"):
        return T.TimestampNTZType()
    if name == "io.debezium.time.ZonedTimestamp":
        return T.TimestampType()

    source_type = params.get("__debezium.source.column.type", "").upper()
    if typ == "string" and source_type in ("NUMERIC", "DECIMAL"):
        length = params.get("__debezium.source.column.length")
        scale = params.get("__debezium.source.column.scale")
        # Unconstrained NUMERIC is reported with a huge length: keep it exact as text.
        if length is not None and scale is not None and 0 < int(length) <= 38:
            return T.DecimalType(int(length), int(scale))
        return T.StringType()

    return {
        "int8": T.IntegerType(), "int16": T.IntegerType(), "int32": T.IntegerType(),
        "int64": T.LongType(), "float32": T.FloatType(), "float64": T.DoubleType(),
        "boolean": T.BooleanType(), "string": T.StringType(), "bytes": T.BinaryType(),
    }.get(typ, T.StringType())


def convert(col, field: dict, dtype: T.DataType):
    """Convert a raw JSON column to its lakehouse type."""
    name = field.get("name")
    if name == "io.debezium.time.Date":
        return F.date_from_unix_date(col.cast("int"))
    if name == "io.debezium.time.MicroTimestamp":
        return F.timestamp_micros(col).cast(T.TimestampNTZType())
    if name == "io.debezium.time.Timestamp":
        return F.timestamp_millis(col).cast(T.TimestampNTZType())
    if name == "io.debezium.time.NanoTimestamp":
        return F.timestamp_micros(F.floor(col / 1000)).cast(T.TimestampNTZType())
    if name == "io.debezium.time.ZonedTimestamp":
        return col.cast(T.TimestampType())
    if field["type"] == "bytes" and isinstance(dtype, T.BinaryType):
        return F.unbase64(col)
    return col.cast(dtype)


def widen_ok(old: T.DataType, new: T.DataType) -> bool:
    """Type promotions Iceberg allows without rewriting data."""
    if isinstance(old, T.IntegerType) and isinstance(new, T.LongType):
        return True
    if isinstance(old, T.FloatType) and isinstance(new, T.DoubleType):
        return True
    if isinstance(old, T.DecimalType) and isinstance(new, T.DecimalType):
        return new.scale == old.scale and new.precision > old.precision
    return False


# ---------------------------------------------------------------------------
# Iceberg helpers
# ---------------------------------------------------------------------------
def with_retries(fn, what: str):
    for attempt in range(1, COMMIT_RETRIES + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - Py4J wraps the Java exception
            message = str(exc)
            if attempt < COMMIT_RETRIES and any(m in message for m in RETRYABLE_MARKERS):
                wait = min(2 ** attempt, 30)
                cause = next((line.strip() for line in message.splitlines() if "Exception" in line),
                             message.splitlines()[0])
                # Safe to retry even after CommitStateUnknown: the MERGE is idempotent.
                log.warning("%s: concurrent/failed commit (attempt %d), retrying in %ss: %s",
                            what, attempt, wait, cause[:300])
                time.sleep(wait)
                continue
            raise


def ensure_table(spark: SparkSession, table: str, namespace: str,
                 pks: list[str], fields: list[T.StructField]) -> list[str]:
    """Create the table or evolve its schema. Returns the DDL changes applied."""
    changes: list[str] = []
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{q(namespace)}")

    if not spark.catalog.tableExists(table):
        cols = []
        for f in fields + META_FIELDS:
            not_null = " NOT NULL" if f.name in pks else ""
            cols.append(f"{q(f.name)} {f.dataType.simpleString()}{not_null}")
        # The primary key is recorded as a table property. (Iceberg identifier
        # fields are not used: Spark MERGE on such tables fails in Iceberg 1.9 with
        # "Cannot add fieldId ... as an identifier field".)
        props = dict(TABLE_PROPERTIES, **{"cdc.primary-key": ",".join(pks)})
        props_sql = ", ".join(f"'{k}'='{v}'" for k, v in props.items())
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(cols)}) "
                  f"USING iceberg TBLPROPERTIES ({props_sql})")
        changes.append(f"CREATE TABLE ({len(fields)} columns, pk={pks})")
        return changes

    existing = {f.name: f.dataType for f in spark.table(table).schema.fields}
    for f in fields:
        old = existing.get(f.name)
        if old is None:
            spark.sql(f"ALTER TABLE {table} ADD COLUMN {q(f.name)} {f.dataType.simpleString()}")
            changes.append(f"ADD COLUMN {f.name} {f.dataType.simpleString()}")
        elif old != f.dataType:
            if widen_ok(old, f.dataType):
                spark.sql(f"ALTER TABLE {table} ALTER COLUMN {q(f.name)} "
                          f"TYPE {f.dataType.simpleString()}")
                changes.append(f"ALTER COLUMN {f.name} {old.simpleString()} -> "
                               f"{f.dataType.simpleString()}")
            else:
                log.warning("%s.%s: source type changed %s -> %s, which Iceberg cannot "
                            "promote in place; values are cast to %s", table, f.name,
                            old.simpleString(), f.dataType.simpleString(), old.simpleString())
    return changes


# ---------------------------------------------------------------------------
# Per-table processing
# ---------------------------------------------------------------------------
def after_fields(value_schema: dict) -> list[dict]:
    for f in value_schema.get("fields", []):
        if f.get("field") == "after":
            return f.get("fields", [])
    return []


def schema_of(prefix: str) -> dict:
    """Parse the '{"schema":{...}' prefix of a Debezium JSON message."""
    return json.loads(prefix + "}")["schema"]


def process_topic(spark: SparkSession, batch_id: int, topic: str, rows: DataFrame) -> dict | None:
    # topic = <prefix>.<schema>.<table>
    _, namespace, name = topic.split(".", 2)
    table = f"{CATALOG}.{q(namespace)}.{q(name)}"

    # 1. schemas present in this batch, oldest first. Debezium serialises
    #    {"schema":{...},"payload":{...}}: cutting the text at the payload key is
    #    much cheaper than JSON-parsing every message just to read its schema.
    schemas = (rows.groupBy(F.substring_index("value", ',"payload":', 1).alias("vs"),
                            F.substring_index("key", ',"payload":', 1).alias("ks"))
               .agg(F.max("offset").alias("last_offset"))
               .orderBy("last_offset")
               .collect())
    key_schema_text = next((r.ks for r in reversed(schemas) if r.ks), None)
    if not key_schema_text:
        log.warning("topic %s has no primary key (message key is null); skipped. "
                    "Add a primary key to the source table to capture it.", topic)
        return None
    key_fields = schema_of(key_schema_text).get("fields", [])
    pks = [f["field"] for f in key_fields]

    merged: dict[str, dict] = {}
    for r in schemas:  # oldest -> newest, so newer definitions overwrite older ones
        for f in after_fields(schema_of(r.vs)):
            merged[f["field"]] = f
    for f in key_fields:
        merged.setdefault(f["field"], f)
    newest = [f["field"] for f in after_fields(schema_of(schemas[-1].vs))]
    order = [c for c in newest if c in merged] + [c for c in merged if c not in newest]
    order = pks + [c for c in order if c not in pks]
    fields = [merged[c] for c in order]

    # 2. parse envelope + key with exact raw types
    envelope = T.StructType([T.StructField("payload", T.StructType([
        T.StructField("op", T.StringType()),
        T.StructField("source", T.StructType([
            T.StructField("ts_ms", T.LongType()),
            T.StructField("lsn", T.LongType()),
        ])),
        T.StructField("after", T.StructType(
            [T.StructField(f["field"], raw_type(f)) for f in fields])),
    ]))])
    key_struct = T.StructType([T.StructField("payload", T.StructType(
        [T.StructField(f["field"], raw_type(f)) for f in key_fields]))])

    parsed = (rows
              .select("offset",
                      F.from_json("value", envelope).getField("payload").alias("e"),
                      F.from_json("key", key_struct).getField("payload").alias("k"))
              .where(F.col("e.op").isin("c", "r", "u", "d")))

    spark_fields = [T.StructField(f["field"], target_type(f)) for f in fields]
    changes = ensure_table(spark, table, namespace, pks, spark_fields)
    table_types = {f.name: f.dataType for f in spark.table(table).schema.fields}

    cols = []
    for f in fields:
        c = f["field"]
        src = F.col("k").getField(c) if c in pks else F.col("e").getField("after").getField(c)
        value = convert(src, f, target_type(f))
        cols.append(value.cast(table_types[c]).alias(c))
    cols += [
        F.col("e.op").alias("_cdc_op"),
        F.timestamp_millis(F.col("e.source.ts_ms")).alias("_cdc_source_ts"),
        F.col("e.source.lsn").alias("_cdc_lsn"),
        F.current_timestamp().alias("_ingested_at"),
        F.col("offset").alias("_offset"),
    ]

    # 3. latest change per primary key (same key -> same partition, offsets ordered)
    latest = Window.partitionBy(*pks).orderBy(F.col("_offset").desc())
    source = (parsed.select(*cols)
              .withColumn("_rn", F.row_number().over(latest))
              .where("_rn = 1")
              .drop("_rn", "_offset")
              .persist())
    try:
        counts = {r["_cdc_op"]: r["n"] for r in source.groupBy("_cdc_op").agg(F.count("*").alias("n")).collect()}
        n_rows = sum(counts.values())
        if n_rows == 0:
            return None

        # 4. MERGE
        view = f"cdc_src_{batch_id}_{namespace}_{name}".replace("-", "_")
        source.createOrReplaceTempView(view)
        on = " AND ".join(f"t.{q(p)} = s.{q(p)}" for p in pks)
        data_cols = [c for c in source.columns]
        update_set = ", ".join(f"t.{q(c)} = s.{q(c)}" for c in data_cols if c not in pks)
        insert_cols = ", ".join(q(c) for c in data_cols)
        insert_vals = ", ".join(f"s.{q(c)}" for c in data_cols)
        merge_sql = f"""
            MERGE INTO {table} t
            USING {view} s
            ON {on}
            WHEN MATCHED AND s._cdc_op = 'd' THEN DELETE
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED AND s._cdc_op <> 'd' THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        started = time.time()
        with_retries(lambda: spark.sql(merge_sql), f"MERGE {table}")
        spark.catalog.dropTempView(view)
        duration_ms = int((time.time() - started) * 1000)
    finally:
        source.unpersist()

    return {
        "table": f"{namespace}.{name}",
        "rows": n_rows,
        "inserts": counts.get("c", 0) + counts.get("r", 0),
        "updates": counts.get("u", 0),
        "deletes": counts.get("d", 0),
        "merge_ms": duration_ms,
        "schema_changes": "; ".join(changes) if changes else None,
    }


# ---------------------------------------------------------------------------
# Micro-batch entry point
# ---------------------------------------------------------------------------
METRICS_SCHEMA = T.StructType([
    T.StructField("batch_id", T.LongType()),
    T.StructField("table_name", T.StringType()),
    T.StructField("kafka_records", T.LongType()),
    T.StructField("rows_merged", T.LongType()),
    T.StructField("inserts", T.LongType()),
    T.StructField("updates", T.LongType()),
    T.StructField("deletes", T.LongType()),
    T.StructField("merge_ms", T.LongType()),
    T.StructField("batch_started_at", T.TimestampType()),
    T.StructField("batch_finished_at", T.TimestampType()),
    T.StructField("schema_changes", T.StringType()),
])


def process_batch(batch: DataFrame, batch_id: int) -> None:
    spark = batch.sparkSession
    started = datetime.now(timezone.utc)
    rows = (batch.where(F.col("value").isNotNull())
            .select("topic", "partition", "offset",
                    F.col("key").cast("string").alias("key"),
                    F.col("value").cast("string").alias("value"))
            .persist())
    try:
        per_topic = {r.topic: r.n for r in rows.groupBy("topic").agg(F.count("*").alias("n")).collect()}
        results = []
        for topic in sorted(per_topic):
            stats = process_topic(spark, batch_id, topic, rows.where(F.col("topic") == topic))
            if stats:
                stats["kafka_records"] = per_topic[topic]
                results.append(stats)
    finally:
        rows.unpersist()

    if not results:
        return
    finished = datetime.now(timezone.utc)
    metric_rows = [(batch_id, s["table"], s["kafka_records"], s["rows"], s["inserts"],
                    s["updates"], s["deletes"], s["merge_ms"], started, finished,
                    s["schema_changes"]) for s in results]
    with_retries(lambda: spark.createDataFrame(metric_rows, METRICS_SCHEMA)
                 .writeTo(METRICS_TABLE).append(), "metrics append")
    for s in results:
        log.info("batch %d %-16s records=%-7d merged=%-7d (+%d ~%d -%d) merge=%dms%s",
                 batch_id, s["table"], s["kafka_records"], s["rows"], s["inserts"],
                 s["updates"], s["deletes"], s["merge_ms"],
                 f" schema: {s['schema_changes']}" if s["schema_changes"] else "")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = SparkSession.builder.appName("cdc_to_iceberg").getOrCreate()

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.ops")
    cols = ", ".join(f"{q(f.name)} {f.dataType.simpleString()}" for f in METRICS_SCHEMA.fields)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {METRICS_TABLE} ({cols}) USING iceberg "
              "TBLPROPERTIES ('format-version'='2')")

    stream = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
              .option("subscribePattern", TOPIC_PATTERN)
              .option("startingOffsets", "earliest")
              .option("maxOffsetsPerTrigger", MAX_OFFSETS)
              # discover topics of newly captured tables quickly
              .option("kafka.metadata.max.age.ms", "15000")
              .load())

    query = (stream.writeStream
             .queryName("cdc_to_iceberg")
             .foreachBatch(process_batch)
             .option("checkpointLocation", CHECKPOINT)
             .trigger(processingTime=TRIGGER)
             .start())
    log.info("Streaming %s -> %s (trigger=%s, maxOffsetsPerTrigger=%d)",
             TOPIC_PATTERN, CATALOG, TRIGGER, MAX_OFFSETS)
    query.awaitTermination()


if __name__ == "__main__":
    main()
