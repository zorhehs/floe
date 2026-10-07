"""
Builds the technical report (docs/report/report.pdf) from report_template.html
and the measured results in results/. Every number in the report comes from
the results files, nothing is typed in by hand.

  python3 docs/report/build_report.py [results/benchmark-<ts>]

Rendering uses headless Google Chrome (--print-to-pdf); no LaTeX needed.
"""

from __future__ import annotations

import base64
import html
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HERE = ROOT / "docs" / "report"
RESULTS = ROOT / "results"
CHROME_CANDIDATES = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    shutil.which("google-chrome") or "", shutil.which("chromium") or "", shutil.which("chromium-browser") or "",
]


def fmt_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def img(path: Path, alt: str) -> str:
    data = base64.b64encode(path.read_bytes()).decode()
    return f'<img src="data:image/png;base64,{data}" alt="{html.escape(alt)}">'


def table(headers: list[str], rows: list[list], align: str = "") -> str:
    th = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f'<table class="{align}"><thead><tr>{th}</tr></thead><tbody>{body}</tbody></table>'


def build(bench_dir: Path) -> dict:
    r = json.loads((bench_dir / "results.json").read_text())
    before, after = r["before"], r["after"]
    queries = list(before["queries"])
    b_ms = [before["queries"][q]["median_wall_ms"] for q in queries]
    a_ms = [after["queries"][q]["median_wall_ms"] for q in queries]
    v: dict[str, str] = {}

    # ---- query latency
    rows = []
    for q, vb, va in zip(queries, b_ms, a_ms):
        rows.append([q.split("_", 1)[0].upper(), q.split("_", 1)[1].replace("_", " "),
                     f"{vb:,.0f}", f"{va:,.0f}", f"{vb / va:.2f}×",
                     f"{before['queries'][q]['processed_rows']:,}", f"{after['queries'][q]['processed_rows']:,}"])
    rows.append(["", "<b>total</b>", f"<b>{sum(b_ms):,.0f}</b>", f"<b>{sum(a_ms):,.0f}</b>",
                 f"<b>{sum(b_ms) / sum(a_ms):.2f}×</b>", "", ""])
    v["query_table"] = table(["#", "Query", "Before (ms)", "After (ms)", "Speed-up", "Rows read before", "Rows read after"],
                             rows, "num")
    v["total_speedup"] = f"{sum(b_ms) / sum(a_ms):.2f}"
    best = max(zip(queries, b_ms, a_ms), key=lambda x: x[1] / x[2])
    v["best_query"] = best[0].split("_", 1)[1].replace("_", " ")
    v["best_speedup"] = f"{best[1] / best[2]:.1f}"
    v["runs"] = str(r["runs_per_query"])
    slower = [(q, vb / va) for q, vb, va in zip(queries, b_ms, a_ms) if vb / va < 1.05]
    v["slower_note"] = (
        "The exception is " + ", ".join(f"<i>{q.split('_', 1)[1].replace('_', ' ')}</i> ({s:.2f}×)" for q, s in slower)
        + ": a scan of the whole table gains nothing from pruning and now opens "
          f"{after['files']['orders']['data_files']} monthly files instead of "
          f"{before['files']['orders']['data_files']} streaming files, which offsets the removed delete files "
          "— the granularity trade-off examined below."
        if slower else "Every query in the suite became faster.")

    # ---- files + storage
    frows = []
    for t in ("orders", "customer"):
        fb, fa = before["files"][t], after["files"][t]
        frows.append([t, f"{fb['data_files']:,} → {fa['data_files']:,}", f"{fb['delete_files']:,} → {fa['delete_files']:,}",
                      f"{fb['avg_data_file_kb']:,.0f} KB → {fa['avg_data_file_kb']:,.0f} KB",
                      f"{fb['snapshots']:,} → {fa['snapshots']:,}"])
    v["files_table"] = table(["Table", "Data files", "Delete files", "Avg data file", "Snapshots"], frows)
    srows, tot_b, tot_a = [], 0, 0
    for t in ("orders", "customer", "nation", "region"):
        if t not in before["storage"]:
            continue
        b_, a_ = before["storage"][t]["bytes"], after["storage"].get(t, {"bytes": 0})["bytes"]
        tot_b, tot_a = tot_b + b_, tot_a + a_
        srows.append([t, f"{fmt_bytes(b_)} ({before['storage'][t]['objects']:,} obj.)",
                      f"{fmt_bytes(a_)} ({after['storage'].get(t, {}).get('objects', 0):,} obj.)",
                      f"{(a_ / b_ - 1) * 100:+.1f} %" if b_ else "–"])
    v["storage_table"] = table(["Table", "Before", "After", "Bytes change"], srows)
    tpch = [t for t in ("orders", "customer", "nation", "region") if t in before["storage"]]
    obj_b = sum(before["storage"][t]["objects"] for t in tpch)
    obj_a = sum(after["storage"].get(t, {"objects": 0})["objects"] for t in tpch)
    v["objects_before"], v["objects_after"] = f"{obj_b:,}", f"{obj_a:,}"
    v["objects_saved_pct"] = f"{(1 - obj_a / obj_b) * 100:.0f}"
    v["bytes_before"], v["bytes_after"] = fmt_bytes(tot_b), fmt_bytes(tot_a)
    v["bytes_change_pct"] = f"{(tot_a / tot_b - 1) * 100:+.1f}"
    del_b = sum(before["files"][t]["delete_files"] for t in before["files"])
    v["delete_files_before"] = f"{del_b:,}"
    ob, oa = before["files"]["orders"], after["files"]["orders"]
    v["orders_files_before"], v["orders_files_after"] = f"{ob['data_files']:,}", f"{oa['data_files']:,}"
    v["orders_del_before"] = f"{ob['delete_files']:,}"
    v["orders_avg_before"] = f"{ob['avg_data_file_kb']:,.0f} KB"
    v["orders_avg_after"] = f"{oa['avg_data_file_kb'] / 1024:,.1f} MB"

    # ---- compaction overhead
    opt = r["optimization"]
    v["opt_wall"] = f"{opt['wall_seconds']:.0f}"
    v["opt_tasks"] = table(["Airflow task", "Duration (s)"],
                           [[t, f"{i['duration_s']:.1f}"] for t, i in opt["tasks"].items()], "num")
    # one row per maintenance step, summed over all tables
    agg: dict[str, dict] = {}
    for per_table in opt.get("spark_steps", {}).values():
        for steps in per_table.values():
            for step, info in steps.items():
                if step == "layout":
                    continue
                a_ = agg.setdefault(step, {"seconds": 0.0, "tables": 0})
                a_["seconds"] += info["seconds"]
                a_["tables"] += 1
                for k, val in info.items():
                    if k.endswith("count") and isinstance(val, (int, float)):
                        a_[k] = a_.get(k, 0) + val
                if step == "orphans":
                    a_["files listed"] = a_.get("files listed", 0) + info.get("files_listed", 0)
                    a_["orphans deleted"] = a_.get("orphans deleted", 0) + info.get("orphan_files_deleted", 0)
    step_rows = []
    for step, a_ in agg.items():
        detail = ", ".join(f"{k.replace('_count', '').replace('_', ' ')} {int(val):,}" for k, val in a_.items()
                           if k not in ("seconds", "tables") and val and "failed" not in k)
        step_rows.append([step.replace("_", " "), str(a_["tables"]), f"{a_['seconds']:.1f}", detail])
    v["spark_steps"] = table(["Spark step", "Tables", "s", "Totals"], step_rows) if step_rows else ""

    # ---- ingestion + freshness
    il = json.loads((RESULTS / "ingest_load.json").read_text())
    v["load_rows"] = f"{il['total_rows']:,}"
    v["load_pg_s"] = f"{il['postgres_load_seconds']:.0f}"
    v["load_e2e_s"] = f"{il['seconds_until_queryable_in_lakehouse']:.0f}"
    v["load_rps"] = f"{il['end_to_end_rows_per_second']:,.0f}"
    ing = r["ingest"]
    v["ing_batches"] = f"{ing['batches']:,}"
    v["ing_records"] = f"{ing['records']:,}"
    v["ing_rps"] = f"{float(ing['records_per_sec_while_processing']):,.0f}"
    v["ing_peak"] = f"{float(ing['peak_batch_records_per_sec']):,.0f}"
    v["ing_p50"] = f"{float(ing['batch_seconds_p50']):.1f}"
    v["ing_p95"] = f"{float(ing['batch_seconds_p95']):.1f}"
    po = ing["per_table"]["shop.orders"]
    v["merge_p50"] = f"{po['merge_ms_p50'] / 1000:.1f}"
    v["merge_p95"] = f"{po['merge_ms_p95'] / 1000:.1f}"
    fr = json.loads((RESULTS / "freshness.json").read_text())
    e = fr["end_to_end_seconds"]
    v.update(fr_p50=f"{e['p50']:.1f}", fr_p95=f"{e['p95']:.1f}", fr_max=f"{e['max']:.1f}", fr_min=f"{e['min']:.1f}",
             fr_n=str(fr["visible"]), fr_timeout=str(fr["timed_out"]),
             fr_pipe=f"{fr['pipeline_commit_to_merge_ms']['p50'] / 1000:.1f}")

    # ---- fault-tolerance incident (Spark driver killed during the workload)
    inc = json.loads((RESULTS / "freshness-with-spark-restart.json").read_text())
    ie = inc["end_to_end_seconds"]
    v.update(inc_p50=f"{ie['p50']:.1f}", inc_max=f"{ie['max']:.1f}", inc_n=str(inc["visible"]),
             inc_timeout=str(inc["timed_out"]))

    # ---- correctness
    v["consistency_rows"] = table(["Table", "Rows", "Checksum match (before / after optimisation)"],
                                  [[c["table"], f"{c['source_rows']:,}",
                                    f"{next(x for x in r['consistency_before'] if x['table'] == c['table'])['checksum_match']} / "
                                    f"{c['checksum_match']}"] for c in r["consistency_after"]])

    # ---- ablation: one compact file vs monthly partitions (same data)
    ab = RESULTS / "ablation-compacted-vs-monthly-partitions" / "results.json"
    if ab.exists():
        a = json.loads(ab.read_text())
        qb, qa = a["before"]["queries"], a["after"]["queries"]
        def pct(q):
            return f"{(qa[q]['median_wall_ms'] / qb[q]['median_wall_ms'] - 1) * 100:+.0f}"
        v.update(ab_files=f"{a['after']['files']['orders']['data_files']}",
                 ab_avg=f"{a['after']['files']['orders']['avg_data_file_kb']:,.0f}",
                 ab_q02_rows_b=f"{qb['q02_single_month']['processed_rows']:,}",
                 ab_q02_rows_a=f"{qa['q02_single_month']['processed_rows']:,}",
                 ab_q02_pct=pct("q02_single_month"), ab_q07_pct=pct("q07_segment_priority"),
                 ab_q05_pct=pct("q05_top_customers"), ab_q06_pct=pct("q06_full_aggregate"))
    else:
        raise SystemExit("ablation results missing")

    # ---- quality-control panel (PASS per table) and per-query speed-up bars
    qc = []
    for c in r["consistency_after"]:
        b_ok = next(x for x in r["consistency_before"] if x["table"] == c["table"])["ok"]
        ok = b_ok and c["ok"]
        qc.append(f'<div><span>{c["table"]} · {c["source_rows"]:,} rows</span>'
                  f'<span class="{"pass" if ok else "fail"}">{"PASS" if ok else "FAIL"}</span></div>')
    for name, okv in (("Initial load (checksums)", il.get("consistent")),
                      ("Freshness probes visible", fr["timed_out"] == 0)):
        qc.append(f'<div><span>{name}</span><span class="{"pass" if okv else "fail"}">{"PASS" if okv else "FAIL"}</span></div>')
    v["qc_panel"] = '<div class="qc">' + "".join(qc) + "</div>"
    top = max(vb / va for vb, va in zip(b_ms, a_ms))
    bars = []
    for q, vb, va in zip(queries, b_ms, a_ms):
        sp = vb / va
        label = q.split("_", 1)[1].replace("_", " ")
        bars.append(f'<div class="bar{" neg" if sp < 1 else ""}"><div class="row"><span>{label}</span>'
                    f'<b>{sp:.2f}×</b></div><div class="track"><div class="fill" style="width:{min(sp / top, 1) * 100:.0f}%">'
                    "</div></div></div>")
    v["speedup_bars"] = '<div class="bars">' + "".join(bars) + "</div>"

    # ---- figures
    for name in ("query_latency", "small_files", "storage", "ingestion", "freshness"):
        p = bench_dir / f"{name}.png"
        v[f"fig_{name}"] = img(p, name) if p.exists() else ""
    for name in ("grafana_dashboard",):
        p = HERE / f"{name}.png"
        v[f"fig_{name}"] = img(p, name) if p.exists() else ""
    return v


def main():
    bench_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else max(RESULTS.glob("benchmark-*"))
    values = build(bench_dir)
    page = (HERE / "report_template.html").read_text()
    for k, val in values.items():
        page = page.replace("{{" + k + "}}", val)
    missing = [line for line in page.splitlines() if "{{" in line]
    if missing:
        raise SystemExit(f"unfilled placeholders: {missing[:3]}")
    out_html = HERE / "report.html"
    out_html.write_text(page)
    chrome = next((c for c in CHROME_CANDIDATES if c and Path(c).exists()), None)
    if not chrome:
        raise SystemExit(f"Chrome not found; open {out_html} in a browser and print to PDF")
    pdf = HERE / "report.pdf"
    subprocess.run([chrome, "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
                    f"--print-to-pdf={pdf}", out_html.as_uri()], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"wrote {pdf} (from {bench_dir.name})")


if __name__ == "__main__":
    main()
