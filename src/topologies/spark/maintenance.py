"""
Iceberg table maintenance / optimisation jobs (Spark + Iceberg procedures).

Run one or more steps against one or more tables, e.g.

  spark-submit maintenance.py --tables shop.orders --layout --compact
  spark-submit maintenance.py --compact --rewrite-deletes --rewrite-manifests
  spark-submit maintenance.py --expire --expire-older-than-minutes 0 --retain-last 1
  spark-submit maintenance.py --orphans --orphan-older-than-minutes 60
  spark-submit maintenance.py --tables shop.customer --purge          # GDPR erasure

Steps (executed in this order when combined):
  --layout           partition evolution + table sort order from LAYOUT_PLAN
  --compact          rewrite_data_files: merges small files and applies delete files
                     (sort strategy when the table has a sort order, else binpack;
                     after --layout every file is rewritten into the new layout)
  --rewrite-deletes  rewrite_position_delete_files: merges small delete files
  --rewrite-manifests  rewrite_manifests: fewer, better clustered manifests
  --expire           expire_snapshots: drops old snapshots + files only they reference
  --orphans          remove_orphan_files: deletes files no snapshot references
  --purge            physically erase deleted rows: rewrite every data file that has
                     deletes, then expire all older snapshots (time travel is lost)

Every step logs and returns statistics; --json-out writes them to a file.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

CATALOG = "lakehouse"
DEFAULT_NAMESPACE = "shop"
TARGET_FILE_SIZE = str(128 * 1024 * 1024)

# Physical layout applied by --layout (the "after optimisation" design).
LAYOUT_PLAN = {
    "shop.orders": {
        "partition_by": ["months(o_orderdate)"],
        "sort_by": ["o_orderdate", "o_custkey"],
    },
    "shop.customer": {
        "partition_by": [],
        "sort_by": ["c_nationkey", "c_custkey"],
    },
}

log = logging.getLogger("maintenance")


def ts_literal(dt: datetime) -> str:
    return "TIMESTAMP '" + dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f") + "'"


def first_row(df) -> dict:
    rows = df.collect()
    return rows[0].asDict() if rows else {}


def file_stats(spark: SparkSession, table: str) -> dict:
    """Live files of the current snapshot, split by content type."""
    spark.sql(f"REFRESH TABLE {CATALOG}.{table}")  # never report from a stale cached table
    rows = spark.sql(f"""
        SELECT content, count(*) AS files, coalesce(sum(file_size_in_bytes), 0) AS bytes,
               coalesce(sum(record_count), 0) AS records
        FROM {CATALOG}.{table}.files GROUP BY content""").collect()
    by = {r.content: r for r in rows}
    data = by.get(0)
    deletes = [by[c] for c in (1, 2) if c in by]
    snapshots = spark.sql(f"SELECT count(*) AS n FROM {CATALOG}.{table}.snapshots").first().n
    return {
        "data_files": data.files if data else 0,
        "data_bytes": data.bytes if data else 0,
        "delete_files": sum(r.files for r in deletes),
        "delete_bytes": sum(r.bytes for r in deletes),
        "avg_data_file_kb": round(data.bytes / data.files / 1024, 1) if data and data.files else 0,
        "snapshots": snapshots,
    }


def has_sort_order(spark: SparkSession, table: str) -> bool:
    """True if the table itself has a sort order (Spark exposes it as 'sort-order')."""
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {CATALOG}.{table}").collect()}
    return bool(props.get("sort-order"))


def step_layout(spark, table):
    plan = LAYOUT_PLAN.get(table)
    if not plan:
        return {"skipped": "no layout plan for table"}
    applied = []
    for transform in plan["partition_by"]:
        try:
            spark.sql(f"ALTER TABLE {CATALOG}.{table} ADD PARTITION FIELD {transform}")
            applied.append(f"ADD PARTITION FIELD {transform}")
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            if "already" in msg.lower() or "duplicate" in msg.lower() or "redundant" in msg.lower():
                applied.append(f"partition field {transform} already present")
            else:
                raise
    if plan["sort_by"]:
        spark.sql(f"ALTER TABLE {CATALOG}.{table} WRITE ORDERED BY {', '.join(plan['sort_by'])}")
        applied.append(f"WRITE ORDERED BY {', '.join(plan['sort_by'])}")
    return {"applied": applied}


def step_compact(spark, table, rewrite_all: bool, delete_file_threshold: int = 2):
    strategy = "sort" if has_sort_order(spark, table) else "binpack"
    options = {
        "target-file-size-bytes": TARGET_FILE_SIZE,
        "min-input-files": "2",
        "delete-file-threshold": str(delete_file_threshold),
        "partial-progress.enabled": "true",
        "partial-progress.max-commits": "10",
        "max-concurrent-file-group-rewrites": "2",
        "remove-dangling-deletes": "true",
    }
    if rewrite_all:
        options["rewrite-all"] = "true"
    opts = ", ".join(f"'{k}', '{v}'" for k, v in options.items())
    result = first_row(spark.sql(
        f"CALL {CATALOG}.system.rewrite_data_files(table => '{table}', "
        f"strategy => '{strategy}', options => map({opts}))"))
    result["strategy"] = strategy
    result["rewrite_all"] = rewrite_all
    return result


def step_rewrite_deletes(spark, table):
    return first_row(spark.sql(
        f"CALL {CATALOG}.system.rewrite_position_delete_files(table => '{table}', "
        "options => map('rewrite-all', 'true'))"))


def step_rewrite_manifests(spark, table):
    return first_row(spark.sql(f"CALL {CATALOG}.system.rewrite_manifests(table => '{table}')"))


def step_expire(spark, table, older_than_minutes: int, retain_last: int):
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=older_than_minutes)
    result = first_row(spark.sql(
        f"CALL {CATALOG}.system.expire_snapshots(table => '{table}', "
        f"older_than => {ts_literal(cutoff)}, retain_last => {retain_last}, "
        "stream_results => true)"))
    result["older_than"] = cutoff.isoformat()
    return result


def list_table_files(spark: SparkSession, table: str) -> list[tuple[str, datetime]]:
    """All objects under the table location, listed through the S3 API.

    remove_orphan_files normally lists storage through a Hadoop FileSystem; this
    stack uses Iceberg's S3FileIO without hadoop-aws, so the listing is supplied
    to the procedure as a view instead (file_list_view)."""
    import boto3  # available in the Airflow/tools image that runs maintenance

    location = next(r.data_type for r in spark.sql(f"DESCRIBE TABLE EXTENDED {CATALOG}.{table}").collect()
                    if r.col_name == "Location")
    bucket, _, prefix = location.removeprefix("s3://").partition("/")
    s3 = boto3.client("s3", endpoint_url=spark.conf.get(f"spark.sql.catalog.{CATALOG}.s3.endpoint"),
                      region_name=os.getenv("AWS_REGION", "us-east-1"))
    files = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        files += [(f"s3://{bucket}/{o['Key']}", o["LastModified"]) for o in page.get("Contents", [])]
    return files


def step_orphans(spark, table, older_than_minutes: int):
    # Iceberg refuses thresholds below 24 h: files of in-flight commits (e.g. the
    # streaming job) are not referenced yet and must never be deleted.
    older_than_minutes = max(older_than_minutes, 24 * 60)
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=older_than_minutes)
    files = list_table_files(spark, table)
    view = "orphan_candidates_" + table.replace(".", "_")
    spark.createDataFrame(files, "file_path string, last_modified timestamp").createOrReplaceTempView(view)
    rows = spark.sql(
        f"CALL {CATALOG}.system.remove_orphan_files(table => '{table}', "
        f"older_than => {ts_literal(cutoff)}, file_list_view => '{view}')").collect()
    return {"files_listed": len(files), "orphan_files_deleted": len(rows), "older_than": cutoff.isoformat()}


def run(spark: SparkSession, args) -> dict:
    tables = args.tables
    if not tables:
        namespaces = [r[0] for r in spark.sql(f"SHOW NAMESPACES IN {CATALOG}").collect()]
        tables = ([f"{DEFAULT_NAMESPACE}.{r.tableName}"
                   for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{DEFAULT_NAMESPACE}").collect()]
                  if DEFAULT_NAMESPACE in namespaces else [])
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "tables": {}}

    for table in tables:
        if not spark.catalog.tableExists(f"{CATALOG}.{table}"):
            log.warning("table %s does not exist, skipped", table)
            continue
        entry = {"before": file_stats(spark, table), "steps": {}}

        def timed(name, fn, *a, **kw):
            t0 = time.time()
            res = fn(spark, table, *a, **kw)
            res = {k: (str(v) if not isinstance(v, (int, float, str, bool, list, type(None))) else v)
                   for k, v in res.items()}
            res["seconds"] = round(time.time() - t0, 2)
            entry["steps"][name] = res
            log.info("%s %s: %s", table, name, res)

        if args.layout:
            timed("layout", step_layout)
        if args.compact or args.layout:
            timed("compact", step_compact, rewrite_all=args.layout)
        if args.purge:
            timed("purge_rewrite", step_compact, rewrite_all=False, delete_file_threshold=1)
        if args.rewrite_deletes or args.purge:
            timed("rewrite_deletes", step_rewrite_deletes)
        if args.rewrite_manifests:
            timed("rewrite_manifests", step_rewrite_manifests)
        if args.purge:
            timed("purge_expire", step_expire, older_than_minutes=0, retain_last=1)
        elif args.expire:
            timed("expire", step_expire, args.expire_older_than_minutes, args.retain_last)
        if args.orphans:
            timed("orphans", step_orphans, args.orphan_older_than_minutes)

        entry["after"] = file_stats(spark, table)
        report["tables"][table] = entry
        log.info("%s before=%s after=%s", table, entry["before"], entry["after"])

    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    return report


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--tables", type=lambda s: [t.strip() for t in s.split(",") if t.strip()],
                   help="comma separated <namespace>.<table>; default: all tables in 'shop'")
    p.add_argument("--layout", action="store_true")
    p.add_argument("--compact", action="store_true")
    p.add_argument("--rewrite-deletes", action="store_true")
    p.add_argument("--rewrite-manifests", action="store_true")
    p.add_argument("--expire", action="store_true")
    p.add_argument("--expire-older-than-minutes", type=int, default=24 * 60)
    p.add_argument("--retain-last", type=int, default=5)
    p.add_argument("--orphans", action="store_true")
    p.add_argument("--orphan-older-than-minutes", type=int, default=24 * 60)
    p.add_argument("--purge", action="store_true")
    p.add_argument("--json-out", help="write the statistics report to this path")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = SparkSession.builder.appName("iceberg_maintenance").getOrCreate()
    report = run(spark, args)
    if args.json_out:
        with open(args.json_out, "w") as fh:
            json.dump(report, fh, indent=2, default=str)
    print("MAINTENANCE_REPORT " + json.dumps(report, default=str))
    spark.stop()


if __name__ == "__main__":
    main()
