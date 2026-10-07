"""
Load TPC-H data (Trino `tpch` connector) into the PostgreSQL source database
and measure how long the CDC pipeline needs to make it queryable in Iceberg.

  python3 -m producer.load_tpch [--scale sf1] [--no-wait]

Idempotent: tables that already contain rows are skipped.
Writes results/ingest_load.json (rows loaded, load time, catch-up time, throughput).
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timezone

from common.db import OLTP, REPORTS_DIR, save_json, wait_for_trino
from consumers.validate import print_results, wait_consistent

TS = "CAST(localtimestamp AS timestamp(6))"
LOADS = [
    ("region", "INSERT INTO {oltp}.region SELECT CAST(regionkey AS integer), name, comment "
               "FROM tpch.{scale}.region"),
    ("nation", "INSERT INTO {oltp}.nation SELECT CAST(nationkey AS integer), name, "
               "CAST(regionkey AS integer), comment FROM tpch.{scale}.nation"),
    ("customer", "INSERT INTO {oltp}.customer SELECT custkey, name, address, "
                 "CAST(nationkey AS integer), phone, CAST(acctbal AS decimal(12,2)), mktsegment, "
                 "comment, " + TS + " FROM tpch.{scale}.customer"),
    ("orders", "INSERT INTO {oltp}.orders SELECT orderkey, custkey, orderstatus, "
               "CAST(totalprice AS decimal(12,2)), orderdate, orderpriority, clerk, "
               "CAST(shippriority AS integer), comment, " + TS + " FROM tpch.{scale}.orders"),
]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--scale", default=os.getenv("TPCH_SCALE", "sf1"), choices=["tiny", "sf1"])
    p.add_argument("--no-wait", action="store_true", help="do not wait for the lakehouse to catch up")
    p.add_argument("--timeout", type=int, default=3600)
    args = p.parse_args()

    t = wait_for_trino()
    started = time.time()
    loaded = {}
    for table, sql in LOADS:
        existing = t.scalar(f"SELECT count(*) FROM {OLTP}.{table}")
        if existing:
            print(f"{table}: already has {existing} rows, skipped")
            continue
        t0 = time.time()
        t.rows(sql.format(oltp=OLTP, scale=args.scale))
        n = t.scalar(f"SELECT count(*) FROM {OLTP}.{table}")
        loaded[table] = n
        print(f"{table}: loaded {n} rows in {time.time() - t0:.1f}s")
    load_seconds = time.time() - started
    if not loaded:
        print("Nothing to load.")
        return
    total = sum(loaded.values())
    print(f"PostgreSQL load finished: {total} rows in {load_seconds:.1f}s")
    if args.no_wait:
        return

    print("Waiting until Iceberg matches PostgreSQL (row counts + checksums) ...")
    ok, _, results = wait_consistent(t, args.timeout, quiet=False)
    end_to_end = time.time() - started
    print_results(results)
    report = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "scale": args.scale,
        "rows_loaded": loaded,
        "total_rows": total,
        "postgres_load_seconds": round(load_seconds, 1),
        "seconds_until_queryable_in_lakehouse": round(end_to_end, 1),
        "end_to_end_rows_per_second": round(total / end_to_end, 1),
        "consistent": ok,
    }
    path = save_json(REPORTS_DIR / "ingest_load.json", report)
    print(f"{'CONSISTENT' if ok else 'NOT CONSISTENT'}: {total} rows queryable in Iceberg "
          f"{end_to_end:.1f}s after load start ({report['end_to_end_rows_per_second']} rows/s). "
          f"Report: {path}")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
