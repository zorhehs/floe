"""
Benchmark suite for the lakehouse (evaluation metrics of the project).

  python3 -m consumers.bench freshness [--probes 40 --interval 2]
      CDC end-to-end freshness: insert probe rows in PostgreSQL and measure how
      long until each one is visible through Trino/Iceberg. Run it while the
      workload generator is running for realistic numbers.

  python3 -m consumers.bench optimize [--runs 5]
      Before/after comparison around the Airflow `iceberg_layout_optimization`
      DAG (partition evolution, sorted compaction, delete-file rewrite,
      manifest rewrite, snapshot expiry, orphan cleanup):
        * query latency of the Trino suite (median of N runs, 1 warm-up)
        * small files: live data/delete files and average file size
        * storage footprint in MinIO (all objects incl. old snapshots)
        * compaction overhead (Airflow task durations + Spark step timings)
        * result equivalence (PostgreSQL vs Iceberg checksums before + after)
      Stop the workload generator first so both sides see identical data.

  python3 -m consumers.bench report [results_dir]
      (Re)build charts + summary.md from a results directory.

Results go to results/benchmark-<timestamp>/.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from common.db import LAKE, QUERIES_DIR, REPORTS_DIR, Airflow, Trino, pg_connect, save_json, wait_for_trino
from consumers.validate import print_results, wait_consistent

TABLES = ["orders", "customer", "nation", "region"]


# ---------------------------------------------------------------------------
# measurements
# ---------------------------------------------------------------------------
def file_stats(t: Trino) -> dict:
    out = {}
    for table in TABLES:
        rows = t.rows(f'SELECT content, count(*), coalesce(sum(file_size_in_bytes), 0), '
                      f'coalesce(sum(record_count), 0) FROM {LAKE}."{table}$files" GROUP BY content')
        by = {r[0]: r for r in rows}
        data = by.get(0, (0, 0, 0, 0))
        dels = [by[c] for c in (1, 2) if c in by]
        snaps = t.scalar(f'SELECT count(*) FROM {LAKE}."{table}$snapshots"')
        spec = t.rows(f'SELECT count(DISTINCT spec_id) FROM {LAKE}."{table}$files"')[0][0]
        out[table] = {
            "data_files": data[1], "data_bytes": data[2], "data_records": data[3],
            "delete_files": sum(r[1] for r in dels), "delete_bytes": sum(r[2] for r in dels),
            "avg_data_file_kb": round(data[2] / data[1] / 1024, 1) if data[1] else 0,
            "snapshots": snaps, "partition_specs_in_use": spec,
        }
    return out


def storage_stats() -> dict:
    s3 = boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
                      aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                      aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                      region_name=os.getenv("AWS_REGION", "us-east-1"))
    bucket = os.getenv("WAREHOUSE_BUCKET", "warehouse")
    out: dict[str, dict] = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="shop/"):
        for obj in page.get("Contents", []):
            parts = obj["Key"].split("/")
            if len(parts) < 3:
                continue
            table = parts[1]
            kind = "metadata" if "metadata" in parts[2:-1] else "data"
            entry = out.setdefault(table, {"objects": 0, "bytes": 0, "data_objects": 0, "data_bytes": 0,
                                           "metadata_objects": 0, "metadata_bytes": 0})
            entry["objects"] += 1
            entry["bytes"] += obj["Size"]
            entry[f"{kind}_objects"] += 1
            entry[f"{kind}_bytes"] += obj["Size"]
    return out


def ingest_stats(t: Trino) -> dict:
    rows = t.rows("""
        SELECT table_name, count(*) AS batches, sum(rows_merged), sum(kafka_records), sum(merge_ms),
               approx_percentile(merge_ms, 0.5), approx_percentile(merge_ms, 0.95), max(merge_ms)
        FROM lakehouse.ops.ingest_batches GROUP BY table_name ORDER BY table_name""")
    per_table = {r[0]: {"batches": r[1], "rows_merged": r[2], "kafka_records": r[3],
                        "merge_seconds": round(r[4] / 1000, 1),
                        "merge_rows_per_sec": round(r[2] / (r[4] / 1000), 1) if r[4] else None,
                        "merge_ms_p50": r[5], "merge_ms_p95": r[6], "merge_ms_max": r[7]} for r in rows}
    batch = t.rows("""
        SELECT count(*), sum(records), sum(secs), approx_percentile(secs, 0.5), approx_percentile(secs, 0.95),
               max(records / nullif(secs, 0))
        FROM (SELECT batch_id, sum(kafka_records) AS records,
                     date_diff('millisecond', min(batch_started_at), max(batch_finished_at)) / 1000.0 AS secs
              FROM lakehouse.ops.ingest_batches GROUP BY batch_id)""")[0]
    timeline = t.rows("""
        SELECT batch_id, max(batch_finished_at), sum(kafka_records),
               date_diff('millisecond', min(batch_started_at), max(batch_finished_at))
        FROM lakehouse.ops.ingest_batches GROUP BY batch_id ORDER BY batch_id""")
    return {
        "per_table": per_table,
        "batches": batch[0], "records": batch[1],
        "processing_seconds": round(float(batch[2] or 0), 1),
        "records_per_sec_while_processing": round(batch[1] / float(batch[2]), 1) if batch[2] else None,
        "batch_seconds_p50": float(batch[3]) if batch[3] is not None else None,
        "batch_seconds_p95": float(batch[4]) if batch[4] is not None else None,
        "peak_batch_records_per_sec": round(float(batch[5]), 1) if batch[5] else None,
        "timeline": [{"batch_id": r[0], "finished_at": r[1], "records": r[2], "duration_ms": r[3]}
                     for r in timeline],
    }


def run_queries(t: Trino, runs: int) -> dict:
    results = {}
    for path in sorted(QUERIES_DIR.glob("q*.sql")):
        sql = "\n".join(line for line in path.read_text().splitlines()
                        if not line.startswith("--")).strip().rstrip(";")
        t.query(sql)  # warm-up (metadata caches, JIT)
        samples = []
        for _ in range(runs):
            t0 = time.perf_counter()
            rows, stats = t.query(sql)
            wall = (time.perf_counter() - t0) * 1000
            samples.append({"wall_ms": wall, "elapsed_ms": stats.get("elapsedTimeMillis"),
                            "cpu_ms": stats.get("cpuTimeMillis"),
                            "processed_rows": stats.get("processedRows"),
                            "processed_bytes": stats.get("processedBytes"),
                            "physical_input_bytes": stats.get("physicalInputBytes"),
                            "result_rows": len(rows)})
        def med(k):
            values = [s[k] for s in samples if s[k] is not None]
            return statistics.median(values) if values else None
        results[path.stem] = {
            "median_wall_ms": round(med("wall_ms"), 1), "median_elapsed_ms": med("elapsed_ms"),
            "median_cpu_ms": med("cpu_ms"), "processed_rows": samples[-1]["processed_rows"],
            "processed_bytes": samples[-1]["processed_bytes"],
            "physical_input_bytes": samples[-1]["physical_input_bytes"],
            "result_rows": samples[-1]["result_rows"], "runs": samples,
        }
        print(f"  {path.stem:<26} median {results[path.stem]['median_wall_ms']:>8.1f} ms  "
              f"rows scanned {samples[-1]['processed_rows']}")
    return results


def snapshot_state(t: Trino, label: str) -> dict:
    print(f"Collecting {label} file + storage statistics ...")
    return {"files": file_stats(t), "storage": storage_stats()}


# ---------------------------------------------------------------------------
# commands
# ---------------------------------------------------------------------------
def cmd_freshness(args) -> Path:
    t = wait_for_trino()
    conn = pg_connect()
    base = int(time.time() * 1000) * 1000
    pending: dict[int, float] = {}
    latencies: list[float] = []
    timed_out = 0
    next_probe = time.time()
    sent = 0
    print(f"Sending {args.probes} probes every {args.interval}s ...")
    deadline = None
    while sent < args.probes or pending:
        now = time.time()
        if sent < args.probes and now >= next_probe:
            pid = base + sent
            with conn.cursor() as cur:
                cur.execute("INSERT INTO shop.cdc_probe (probe_id) VALUES (%s)", [pid])
            pending[pid] = time.time()  # autocommit: committed when execute returns
            sent += 1
            next_probe += args.interval
            if sent == args.probes:
                deadline = time.time() + args.timeout
        if pending:
            try:
                ids = ", ".join(str(p) for p in pending)
                visible = {r[0] for r in t.rows(f"SELECT probe_id FROM {LAKE}.cdc_probe WHERE probe_id IN ({ids})")}
            except Exception:  # noqa: BLE001 - table appears with the first probe
                visible = set()
            seen_at = time.time()
            for pid in list(pending):
                if pid in visible:
                    latencies.append(seen_at - pending.pop(pid))
                elif seen_at - pending[pid] > args.timeout:
                    pending.pop(pid)
                    timed_out += 1
        if deadline and time.time() > deadline:
            timed_out += len(pending)
            break
        time.sleep(0.25)

    pipeline = t.rows(f"""
        SELECT approx_percentile(date_diff('millisecond', _cdc_source_ts, _ingested_at), 0.5),
               approx_percentile(date_diff('millisecond', _cdc_source_ts, _ingested_at), 0.95)
        FROM {LAKE}.cdc_probe WHERE probe_id >= {base}""")[0]
    lat = sorted(latencies)
    pct = lambda p: round(lat[min(len(lat) - 1, int(p * len(lat)))], 2) if lat else None  # noqa: E731
    result = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "probes": args.probes, "interval_s": args.interval, "visible": len(lat), "timed_out": timed_out,
        "end_to_end_seconds": {"min": round(lat[0], 2) if lat else None, "p50": pct(0.5),
                               "p95": pct(0.95), "max": round(lat[-1], 2) if lat else None,
                               "mean": round(statistics.mean(lat), 2) if lat else None},
        "pipeline_commit_to_merge_ms": {"p50": pipeline[0], "p95": pipeline[1]},
        "samples_seconds": [round(x, 3) for x in latencies],
        "note": "end-to-end = PostgreSQL commit -> row visible to a Trino query on Iceberg",
    }
    out = REPORTS_DIR / "freshness.json"
    save_json(out, result)
    e = result["end_to_end_seconds"]
    print(f"Freshness: p50={e['p50']}s p95={e['p95']}s max={e['max']}s "
          f"({len(lat)}/{args.probes} visible, {timed_out} timed out). Saved {out}")
    return out


def cmd_optimize(args) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    out_dir = REPORTS_DIR / f"benchmark-{stamp}"
    t = wait_for_trino()
    af = Airflow()
    af.wait_ready("iceberg_layout_optimization")
    af.wait_ready("iceberg_maintenance")
    was_paused = af.set_paused("iceberg_maintenance", True)  # no background compaction during "before"
    try:
        print("Checking that Iceberg matches PostgreSQL (stop the generator first) ...")
        ok, secs, check_before = wait_consistent(t, args.consistency_timeout)
        print_results(check_before)
        if not ok:
            raise SystemExit("Lakehouse not consistent with PostgreSQL; is the workload generator still running?")

        results = {"started_at": datetime.now(timezone.utc).isoformat(), "runs_per_query": args.runs,
                   "consistency_before": check_before}
        results["ingest"] = ingest_stats(t)
        results["before"] = snapshot_state(t, "BEFORE")
        print("Running query suite BEFORE optimisation ...")
        results["before"]["queries"] = run_queries(t, args.runs)

        print("Triggering Airflow DAG iceberg_layout_optimization ...")
        t0 = time.time()
        results["optimization"] = af.run("iceberg_layout_optimization",
                                         {"tables": "", "expire_older_than_minutes": 0, "retain_last": 1,
                                          "orphan_older_than_minutes": 24 * 60})
        results["optimization"]["wall_seconds"] = round(time.time() - t0, 1)
        results["optimization"]["spark_steps"] = collect_maintenance_reports(results["optimization"]["run_id"], t0)
        print(f"Optimisation finished in {results['optimization']['wall_seconds']}s")

        print("Re-checking PostgreSQL vs Iceberg after optimisation ...")
        ok_after, _, check_after = wait_consistent(t, 120)
        print_results(check_after)
        results["consistency_after"] = check_after
        if not ok_after:
            raise SystemExit("Data changed during optimisation - results would not be comparable")

        results["after"] = snapshot_state(t, "AFTER")
        print("Running query suite AFTER optimisation ...")
        results["after"]["queries"] = run_queries(t, args.runs)
        results["finished_at"] = datetime.now(timezone.utc).isoformat()
        for name in ("ingest_load.json", "freshness.json"):
            if (REPORTS_DIR / name).exists():
                results[name.removesuffix(".json")] = json.loads((REPORTS_DIR / name).read_text())
        save_json(out_dir / "results.json", results)
    finally:
        if not was_paused:
            af.set_paused("iceberg_maintenance", False)

    build_report(out_dir)
    print(f"Benchmark complete: {out_dir}")
    return out_dir


def collect_maintenance_reports(run_id: str, since: float) -> dict:
    steps = {}
    folder = REPORTS_DIR / "maintenance"
    for path in sorted(folder.glob("iceberg_layout_optimization__*.json")):
        if path.stat().st_mtime < since - 5:
            continue
        task = path.stem.split("__")[-1]
        data = json.loads(path.read_text())
        steps[task] = {tbl: entry["steps"] for tbl, entry in data.get("tables", {}).items()}
    return steps


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------
def fmt_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def build_report(out_dir: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    r = json.loads((out_dir / "results.json").read_text())
    before, after = r["before"], r["after"]
    queries = list(before["queries"])
    b_ms = [before["queries"][q]["median_wall_ms"] for q in queries]
    a_ms = [after["queries"][q]["median_wall_ms"] for q in queries]
    # report palette: neutral before (grey) vs after (ink), one accent for the timeline
    grey, ink, accent, muted = "#c3c8cf", "#121417", "#3b8ff3", "#6b7280"
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9, "axes.edgecolor": "#c9cdd3",
                         "axes.labelcolor": muted, "xtick.color": muted, "ytick.color": muted,
                         "axes.titlesize": 10, "axes.titleweight": "bold", "axes.titlecolor": ink,
                         "axes.titlelocation": "left", "axes.grid": True, "axes.grid.axis": "y",
                         "grid.color": "#eceef1", "grid.linewidth": 0.8, "axes.axisbelow": True,
                         "legend.frameon": False, "legend.fontsize": 8})
    blue, orange = grey, ink

    def bars(ax, labels, b, a, title, ylabel, fmt="{:.0f}"):
        x = range(len(labels))
        w = 0.38
        ax.bar([i - w / 2 for i in x], b, w, label="before", color=blue)
        ax.bar([i + w / 2 for i in x], a, w, label="after", color=orange)
        ax.set_xticks(list(x))
        ax.set_xticklabels(labels, rotation=30, ha="right")
        ax.set_title(title)
        ax.set_ylabel(ylabel)
        ax.legend(loc="upper right")
        ax.spines[["top", "right", "left"]].set_visible(False)
        ax.tick_params(length=0)
        for i, (vb, va) in enumerate(zip(b, a, strict=True)):
            ax.annotate(fmt.format(vb), (i - w / 2, vb), ha="center", va="bottom", fontsize=7, color=muted)
            ax.annotate(fmt.format(va), (i + w / 2, va), ha="center", va="bottom", fontsize=7, color=ink)

    fig, ax = plt.subplots(figsize=(10, 4.5))
    bars(ax, [q.split("_", 1)[1] for q in queries], b_ms, a_ms,
         "", "milliseconds")
    fig.tight_layout()
    fig.savefig(out_dir / "query_latency.png", dpi=150)
    plt.close(fig)

    tables = [tb for tb in TABLES if tb in before["files"]]
    fig, axes = plt.subplots(1, 3, figsize=(13, 4))
    bars(axes[0], tables, [before["files"][x]["data_files"] for x in tables],
         [after["files"][x]["data_files"] for x in tables], "Live data files", "files")
    bars(axes[1], tables, [before["files"][x]["delete_files"] for x in tables],
         [after["files"][x]["delete_files"] for x in tables], "Live delete files", "files")
    bars(axes[2], tables, [before["files"][x]["avg_data_file_kb"] for x in tables],
         [after["files"][x]["avg_data_file_kb"] for x in tables], "Average data file size", "KB")
    fig.tight_layout()
    fig.savefig(out_dir / "small_files.png", dpi=150)
    plt.close(fig)

    st = [tb for tb in tables if tb in before["storage"]]
    fig, ax = plt.subplots(figsize=(7, 4))
    bars(ax, st, [before["storage"][x]["bytes"] / 2**20 for x in st],
         [after["storage"].get(x, {"bytes": 0})["bytes"] / 2**20 for x in st],
         "", "MiB", fmt="{:.1f}")
    fig.tight_layout()
    fig.savefig(out_dir / "storage.png", dpi=150)
    plt.close(fig)

    timeline = r["ingest"]["timeline"]
    if timeline:
        fig, ax = plt.subplots(figsize=(10, 3.8))
        ax.plot([p["batch_id"] for p in timeline], [p["duration_ms"] / 1000 for p in timeline],
                color=accent, lw=1.6)
        ax2 = ax.twinx()
        ax2.bar([p["batch_id"] for p in timeline], [p["records"] for p in timeline], color=grey, alpha=.7)
        ax2.grid(False)
        for a in (ax, ax2):
            a.spines[["top"]].set_visible(False)
            a.tick_params(length=0)
        ax.set_zorder(ax2.get_zorder() + 1)
        ax.patch.set_visible(False)
        ax.set_xlabel("micro-batch")
        ax.set_ylabel("batch duration (s)", color=accent)
        ax2.set_ylabel("CDC records in batch")
        fig.tight_layout()
        fig.savefig(out_dir / "ingestion.png", dpi=150)
        plt.close(fig)

    fresh = r.get("freshness")
    if fresh and fresh["samples_seconds"]:
        fig, ax = plt.subplots(figsize=(7, 3.8))
        ax.hist(fresh["samples_seconds"], bins=20, color=ink, rwidth=0.85)
        ax.set_xlabel("seconds from PostgreSQL commit to visible in Trino")
        ax.set_ylabel("probes")
        ax.spines[["top", "right", "left"]].set_visible(False)
        ax.tick_params(length=0)
        fig.tight_layout()
        fig.savefig(out_dir / "freshness.png", dpi=150)
        plt.close(fig)

    # ---- markdown summary
    L = [f"# Benchmark results ({r['started_at'][:19]} UTC)", ""]
    L += ["## Query latency (Trino, median of "
          f"{r['runs_per_query']} runs)", "",
          "| Query | Before (ms) | After (ms) | Speed-up | Rows scanned before | Rows scanned after |",
          "|---|---:|---:|---:|---:|---:|"]
    for q, vb, va in zip(queries, b_ms, a_ms, strict=True):
        L.append(f"| {q} | {vb:.0f} | {va:.0f} | {vb / va:.2f}x | "
                 f"{before['queries'][q]['processed_rows']} | {after['queries'][q]['processed_rows']} |")
    L.append(f"| **total** | {sum(b_ms):.0f} | {sum(a_ms):.0f} | {sum(b_ms) / sum(a_ms):.2f}x | | |")
    L += ["", "![query latency](query_latency.png)", "", "## Small files and delete files", "",
          "| Table | Data files | Delete files | Avg data file | Snapshots |", "|---|---|---|---|---|"]
    for tb in tables:
        fb, fa = before["files"][tb], after["files"][tb]
        L.append(f"| {tb} | {fb['data_files']} → {fa['data_files']} | {fb['delete_files']} → {fa['delete_files']} | "
                 f"{fb['avg_data_file_kb']} KB → {fa['avg_data_file_kb']} KB | {fb['snapshots']} → {fa['snapshots']} |")
    L += ["", "![small files](small_files.png)", "", "## Storage footprint (MinIO, all objects)", "",
          "| Table | Before | After | Saved |", "|---|---:|---:|---:|"]
    for tb in st:
        b_, a_ = before["storage"][tb]["bytes"], after["storage"].get(tb, {"bytes": 0})["bytes"]
        L.append(f"| {tb} | {fmt_bytes(b_)} ({before['storage'][tb]['objects']} objects) | "
                 f"{fmt_bytes(a_)} ({after['storage'].get(tb, {}).get('objects', 0)} objects) | "
                 f"{(1 - a_ / b_) * 100 if b_ else 0:.1f}% |")
    L += ["", "![storage](storage.png)", "", "## Compaction overhead", "",
          f"Airflow DAG `iceberg_layout_optimization` wall time: **{r['optimization']['wall_seconds']} s**", "",
          "| Task | Duration (s) |", "|---|---:|"]
    for task, info in r["optimization"]["tasks"].items():
        L.append(f"| {task} | {info['duration_s']:.1f} |")
    steps = r["optimization"].get("spark_steps", {})
    if steps:
        L += ["", "| Spark step | Table | Seconds | Details |", "|---|---|---:|---|"]
        for per_table in steps.values():
            for tb, st_ in per_table.items():
                for step, info in st_.items():
                    details = ", ".join(f"{k}={v}" for k, v in info.items() if k != "seconds")
                    L.append(f"| {step} | {tb} | {info['seconds']} | {details[:160]} |")
    ing = r["ingest"]
    L += ["", "## Ingestion throughput (Spark Structured Streaming → Iceberg MERGE)", "",
          f"* micro-batches: {ing['batches']}, CDC records: {ing['records']}",
          f"* throughput while processing: **{ing['records_per_sec_while_processing']} records/s** "
          f"(peak batch {ing['peak_batch_records_per_sec']} records/s)",
          f"* batch duration p50 / p95: {ing['batch_seconds_p50']} s / {ing['batch_seconds_p95']} s", ""]
    if r.get("ingest_load"):
        il = r["ingest_load"]
        L.append(f"* initial TPC-H load ({il['total_rows']} rows): queryable in Iceberg after "
                 f"{il['seconds_until_queryable_in_lakehouse']} s → **{il['end_to_end_rows_per_second']} rows/s end-to-end**")
    if timeline:
        L += ["", "![ingestion](ingestion.png)"]
    if fresh:
        e = fresh["end_to_end_seconds"]
        L += ["", "## CDC end-to-end freshness", "",
              f"PostgreSQL commit → visible in Trino: p50 **{e['p50']} s**, p95 {e['p95']} s, max {e['max']} s "
              f"({fresh['visible']} probes, {fresh['timed_out']} timed out). "
              f"Pipeline-internal (commit → MERGE) p50 {fresh['pipeline_commit_to_merge_ms']['p50']} ms.", "",
              "![freshness](freshness.png)"]
    L += ["", "## Correctness", "",
          "PostgreSQL vs Iceberg row counts and checksums matched before and after optimisation:", ""]
    for c in r["consistency_after"]:
        L.append(f"* {c['table']}: {c.get('source_rows')} rows, checksum match = {c.get('checksum_match')}")
    (out_dir / "summary.md").write_text("\n".join(L) + "\n")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("freshness")
    f.add_argument("--probes", type=int, default=40)
    f.add_argument("--interval", type=float, default=2.0)
    f.add_argument("--timeout", type=float, default=180.0)
    o = sub.add_parser("optimize")
    o.add_argument("--runs", type=int, default=5)
    o.add_argument("--consistency-timeout", type=int, default=900)
    rp = sub.add_parser("report")
    rp.add_argument("results_dir", nargs="?")
    args = p.parse_args()

    if args.cmd == "freshness":
        cmd_freshness(args)
    elif args.cmd == "optimize":
        cmd_optimize(args)
    else:
        target = Path(args.results_dir) if args.results_dir else max(REPORTS_DIR.glob("benchmark-*"))
        build_report(target)
        print(f"Report rebuilt: {target / 'summary.md'}")


if __name__ == "__main__":
    sys.exit(main())
