"""
Correctness check: PostgreSQL (source of truth) vs. the Iceberg lakehouse.

For every captured table, compares row count and an order-independent
checksum over all shared columns (Trino `checksum()`), both computed by Trino
over the `postgresql` and `lakehouse` catalogs.

  python3 -m consumers.validate            # one comparison, exit 1 on mismatch
  python3 -m consumers.validate --wait 600 # poll until consistent (pipeline caught up)
"""

from __future__ import annotations

import argparse
import sys
import time

from common.db import LAKE, OLTP, Trino, wait_for_trino

META = {"_cdc_op", "_cdc_source_ts", "_cdc_lsn", "_ingested_at"}


def columns(t: Trino, catalog: str, table: str) -> list[str]:
    return [r[0] for r in t.rows(
        f"SELECT column_name FROM {catalog}.information_schema.columns "
        "WHERE table_schema = 'shop' AND table_name = ? ORDER BY ordinal_position", [table])]


def tables(t: Trino) -> list[str]:
    src = {r[0] for r in t.rows(
        "SELECT table_name FROM postgresql.information_schema.tables "
        "WHERE table_schema = 'shop' AND table_type = 'BASE TABLE'")}
    # cdc_probe is the monitoring heartbeat table (rewritten every few seconds)
    return sorted(src - {"cdc_probe"})


def compare(t: Trino) -> list[dict]:
    lake_tables = {r[0] for r in t.rows(
        "SELECT table_name FROM lakehouse.information_schema.tables WHERE table_schema = 'shop'")}
    results = []
    for table in tables(t):
        src_cols = columns(t, "postgresql", table)
        res = {"table": table, "source_columns": len(src_cols)}
        if table not in lake_tables:
            src_count = t.scalar(f"SELECT count(*) FROM {OLTP}.{table}")
            res.update(ok=src_count == 0, source_rows=src_count, lake_rows=None,
                       note="not in lakehouse yet" if src_count else "empty, not created yet")
            results.append(res)
            continue
        lake_cols = set(columns(t, "lakehouse", table)) - META
        shared = [c for c in src_cols if c in lake_cols]
        missing = [c for c in src_cols if c not in lake_cols]
        expr = "checksum(row(" + ", ".join(f'CAST("{c}" AS varchar)' for c in shared) + "))"
        s_count, s_sum = t.rows(f"SELECT count(*), {expr} FROM {OLTP}.{table}")[0]
        l_count, l_sum = t.rows(f"SELECT count(*), {expr} FROM {LAKE}.{table}")[0]
        res.update(source_rows=s_count, lake_rows=l_count, compared_columns=len(shared),
                   checksum_match=s_sum == l_sum, ok=(s_count == l_count and s_sum == l_sum))
        if missing:
            res["note"] = ("columns not yet in lakehouse (added in source, no row changed "
                           f"since): {missing}")
        results.append(res)
    return results


def print_results(results: list[dict]) -> None:
    for r in results:
        status = "OK " if r["ok"] else "DIFF"
        print(f"[{status}] {r['table']:<10} source={r.get('source_rows')} lake={r.get('lake_rows')} "
              f"checksum_match={r.get('checksum_match')} {r.get('note', '')}")


def counts_match(t: Trino) -> tuple[bool, dict]:
    """Cheap pre-check (row counts only) used while the pipeline is catching up."""
    lake = {r[0] for r in t.rows(
        "SELECT table_name FROM lakehouse.information_schema.tables WHERE table_schema = 'shop'")}
    lag = {}
    for table in tables(t):
        src = t.scalar(f"SELECT count(*) FROM {OLTP}.{table}")
        dst = t.scalar(f"SELECT count(*) FROM {LAKE}.{table}") if table in lake else 0
        if src != dst:
            lag[table] = (src, dst)
    return not lag, lag


def wait_consistent(t: Trino, timeout: int, quiet: bool = False) -> tuple[bool, float, list[dict]]:
    start = time.time()
    while True:
        # Full checksums scan every row, so only run them once the counts agree.
        same_counts, lag = counts_match(t)
        if not same_counts and time.time() - start <= timeout:
            if not quiet:
                print(f"  waiting for pipeline to catch up: {lag}")
            time.sleep(5)
            continue
        results = compare(t)
        if all(r["ok"] for r in results):
            return True, time.time() - start, results
        if time.time() - start > timeout:
            return False, time.time() - start, results
        if not quiet:
            lag = {r["table"]: (r.get("source_rows"), r.get("lake_rows")) for r in results if not r["ok"]}
            print(f"  waiting for pipeline to catch up: {lag}")
        time.sleep(5)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--wait", type=int, default=0, help="seconds to wait for consistency")
    args = p.parse_args()
    t = wait_for_trino()
    if args.wait:
        ok, secs, results = wait_consistent(t, args.wait)
        print_results(results)
        print(f"{'CONSISTENT' if ok else 'NOT CONSISTENT'} after {secs:.1f}s")
    else:
        results = compare(t)
        print_results(results)
        ok = all(r["ok"] for r in results)
        print("CONSISTENT" if ok else "NOT CONSISTENT (pipeline may still be catching up; use --wait)")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
