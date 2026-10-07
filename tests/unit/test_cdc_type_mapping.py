"""Unit tests for the Debezium -> Spark/Iceberg type mapping of the streaming job.

Runs a local SparkSession (no Kafka / Iceberg needed): `pytest tests/unit`.
"""

import importlib.util
import json
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

JOB = Path(__file__).resolve().parents[2] / "src" / "topologies" / "spark" / "cdc_to_iceberg.py"
spec = importlib.util.spec_from_file_location("cdc_to_iceberg", JOB)
cdc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cdc)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    # isolate from any spark-defaults.conf (e.g. the lakehouse catalog in the image)
    os.environ["SPARK_CONF_DIR"] = str(tmp_path_factory.mktemp("spark-conf"))
    s = (SparkSession.builder.master("local[1]").appName("unit")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


# Debezium field schemas as produced with decimal.handling.mode=string and
# column.propagate.source.type enabled.
F_DATE = {"type": "int32", "name": "io.debezium.time.Date", "field": "d"}
F_TS = {"type": "int64", "name": "io.debezium.time.MicroTimestamp", "field": "ts"}
F_TSZ = {"type": "string", "name": "io.debezium.time.ZonedTimestamp", "field": "tsz"}
F_DEC = {"type": "string", "field": "price", "parameters": {
    "__debezium.source.column.type": "NUMERIC", "__debezium.source.column.length": "12",
    "__debezium.source.column.scale": "2"}}
F_NUM_UNBOUNDED = {"type": "string", "field": "n", "parameters": {
    "__debezium.source.column.type": "NUMERIC", "__debezium.source.column.length": "131089",
    "__debezium.source.column.scale": "0"}}
F_INT = {"type": "int32", "field": "i"}
F_LONG = {"type": "int64", "field": "l"}
F_BYTES = {"type": "bytes", "field": "b"}


@pytest.mark.parametrize("field, expected", [
    (F_DATE, T.DateType()),
    (F_TS, T.TimestampNTZType()),
    (F_TSZ, T.TimestampType()),
    (F_DEC, T.DecimalType(12, 2)),
    (F_NUM_UNBOUNDED, T.StringType()),
    (F_INT, T.IntegerType()),
    (F_LONG, T.LongType()),
    (F_BYTES, T.BinaryType()),
    ({"type": "boolean", "field": "x"}, T.BooleanType()),
    ({"type": "float64", "field": "x"}, T.DoubleType()),
    ({"type": "struct", "field": "x"}, T.StringType()),
])
def test_target_type(field, expected):
    assert cdc.target_type(field) == expected


def test_raw_types_read_json_losslessly():
    assert cdc.raw_type(F_INT) == T.LongType()
    assert cdc.raw_type(F_DEC) == T.StringType()
    assert cdc.raw_type({"type": "float32"}) == T.DoubleType()


@pytest.mark.parametrize("old, new, ok", [
    (T.IntegerType(), T.LongType(), True),
    (T.FloatType(), T.DoubleType(), True),
    (T.DecimalType(10, 2), T.DecimalType(12, 2), True),
    (T.DecimalType(10, 2), T.DecimalType(12, 3), False),
    (T.LongType(), T.IntegerType(), False),
    (T.StringType(), T.LongType(), False),
])
def test_widening_rules(old, new, ok):
    assert cdc.widen_ok(old, new) is ok


def test_convert_values(spark):
    fields = [F_DATE, F_TS, F_TSZ, F_DEC, F_INT, F_BYTES]
    payload = {"d": 9131,                       # 1995-01-01
               "ts": 1700000000123456,          # 2023-11-14 22:13:20.123456
               "tsz": "2026-10-06T08:00:00.5Z",
               "price": "12345.67", "i": 42, "b": "aGk="}  # base64("hi")
    raw = T.StructType([T.StructField(f["field"], cdc.raw_type(f)) for f in fields])
    df = spark.createDataFrame([(json.dumps(payload),)], ["v"]).select(F.from_json("v", raw).alias("a"))
    row = df.select(*[cdc.convert(F.col("a").getField(f["field"]), f, cdc.target_type(f)).alias(f["field"])
                      for f in fields]).first()
    assert row.d == date(1995, 1, 1)
    assert row.ts == datetime(2023, 11, 14, 22, 13, 20, 123456)
    assert row.tsz.astimezone(timezone.utc) == datetime(2026, 10, 6, 8, 0, 0, 500000, tzinfo=timezone.utc)
    assert row.price == Decimal("12345.67")
    assert row.i == 42
    assert bytes(row.b) == b"hi"


def test_after_fields_extracts_row_schema():
    schema = {"fields": [{"field": "before", "fields": [F_INT]}, {"field": "after", "fields": [F_INT, F_DEC]},
                         {"field": "op", "type": "string"}]}
    assert [f["field"] for f in cdc.after_fields(schema)] == ["i", "price"]


def test_quoting():
    assert cdc.q("weird`name") == "`weird``name`"


def test_schema_prefix_parsing():
    message = json.dumps({"schema": {"type": "struct", "fields": [{"field": "after", "fields": [F_INT]}]},
                          "payload": {"op": "c", "after": {"i": 1}}}, separators=(",", ":"))
    prefix = message.split(',"payload":', 1)[0]          # what substring_index produces
    assert cdc.after_fields(cdc.schema_of(prefix)) == [F_INT]
