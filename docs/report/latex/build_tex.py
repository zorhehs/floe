"""
Builds the IEEE-format technical report (LaTeX) from the measured results.

  python3 docs/report/latex/build_tex.py [results/benchmark-<ts>]

Writes docs/report/latex/main.tex, copies the figures into
docs/report/latex/figures/ and packs everything into
docs/report/latex/floe-report-overleaf.zip (upload to Overleaf: New Project ->
Upload Project). Every number comes from results/, none is typed by hand.
Compile with pdflatex (Overleaf default) or `tectonic main.tex`.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
HERE = ROOT / "docs" / "report" / "latex"
RESULTS = ROOT / "results"
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"


def fmt_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f}\\,{unit}"
        n /= 1024
    return f"{n:.1f}\\,TB"


def tex(s: str) -> str:
    return s.replace("_", "\\_").replace("%", "\\%").replace("&", "\\&")


def architecture_pdf(out: Path) -> None:
    """Vector PDF of the architecture diagram (SVG shared with the HTML report)."""
    template = (ROOT / "docs" / "report" / "report_template.html").read_text()
    svg = re.search(r"<svg viewBox=\"0 0 760 300\".*?</svg>", template, re.S).group(0)
    svg = svg.replace('width="100%"', 'width="760" height="300"')
    svg = svg.replace("Avenir Next, Helvetica Neue, Arial", "Helvetica, Arial")
    html = HERE / "_arch.html"
    html.write_text("<!doctype html><html><head><style>@page{size:760px 300px;margin:0}"
                    "html,body{margin:0}</style></head><body>" + svg + "</body></html>")
    subprocess.run([CHROME, "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
                    f"--print-to-pdf={out}", html.as_uri()], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    html.unlink()


def values(bench: Path) -> dict:
    r = json.loads((bench / "results.json").read_text())
    il = json.loads((RESULTS / "ingest_load.json").read_text())
    fr = json.loads((RESULTS / "freshness.json").read_text())
    inc = json.loads((RESULTS / "freshness-with-spark-restart.json").read_text())
    ab = json.loads((RESULTS / "ablation-compacted-vs-monthly-partitions" / "results.json").read_text())
    before, after = r["before"], r["after"]
    qs = list(before["queries"])
    b = [before["queries"][q]["median_wall_ms"] for q in qs]
    a = [after["queries"][q]["median_wall_ms"] for q in qs]
    v: dict[str, str] = {}

    rows = []
    for q, vb, va in zip(qs, b, a):
        rows.append(f"Q{q[1:3]} & {tex(q.split('_', 1)[1].replace('_', ' '))} & {vb:,.0f} & {va:,.0f} & "
                    f"{vb / va:.2f}$\\times$ & {before['queries'][q]['processed_rows'] / 1e6:.2f} & "
                    f"{after['queries'][q]['processed_rows'] / 1e6:.2f} \\\\")
    rows.append("\\midrule")
    rows.append(f" & \\textbf{{total}} & \\textbf{{{sum(b):,.0f}}} & \\textbf{{{sum(a):,.0f}}} & "
                f"\\textbf{{{sum(b) / sum(a):.2f}$\\times$}} & & \\\\")
    v["query_rows"] = "\n".join(rows)
    v["speedup"] = f"{sum(b) / sum(a):.2f}"
    best = max(zip(qs, b, a), key=lambda x: x[1] / x[2])
    v["best_q"], v["best_speedup"] = tex(best[0].split("_", 1)[1].replace("_", " ")), f"{best[1] / best[2]:.1f}"
    slower = [(q, vb / va) for q, vb, va in zip(qs, b, a) if vb / va < 1.05]
    v["slower"] = ", ".join(f"{tex(q.split('_', 1)[1].replace('_', ' '))} ({s:.2f}$\\times$)" for q, s in slower) or "none"
    v["runs"] = str(r["runs_per_query"])
    q2b, q2a = before["queries"]["q02_single_month"], after["queries"]["q02_single_month"]
    v["q02_rows_b"], v["q02_rows_a"] = f"{q2b['processed_rows']:,}", f"{q2a['processed_rows']:,}"

    frows, del_total = [], 0
    for t in ("orders", "customer"):
        fb, fa = before["files"][t], after["files"][t]
        del_total += fb["delete_files"]
        frows.append(f"{t} & {fb['data_files']} $\\rightarrow$ {fa['data_files']} & "
                     f"{fb['delete_files']} $\\rightarrow$ {fa['delete_files']} & "
                     f"{fb['avg_data_file_kb']:,.0f} $\\rightarrow$ {fa['avg_data_file_kb']:,.0f} & "
                     f"{fb['snapshots']} $\\rightarrow$ {fa['snapshots']} \\\\")
    v["file_rows"] = "\n".join(frows)
    v["delete_before"] = str(sum(before["files"][t]["delete_files"] for t in before["files"]))
    tpch = [t for t in ("orders", "customer", "nation", "region") if t in before["storage"]]
    ob = sum(before["storage"][t]["objects"] for t in tpch)
    oa = sum(after["storage"].get(t, {"objects": 0})["objects"] for t in tpch)
    bb = sum(before["storage"][t]["bytes"] for t in tpch)
    ba = sum(after["storage"].get(t, {"bytes": 0})["bytes"] for t in tpch)
    v.update(obj_b=f"{ob:,}", obj_a=f"{oa:,}", obj_pct=f"{(1 - oa / ob) * 100:.0f}",
             bytes_b=fmt_bytes(bb), bytes_a=fmt_bytes(ba), bytes_pct=f"{(ba / bb - 1) * 100:+.1f}")
    ofb, ofa = before["files"]["orders"], after["files"]["orders"]
    v.update(o_files_b=str(ofb["data_files"]), o_files_a=str(ofa["data_files"]),
             o_del_b=str(ofb["delete_files"]), o_avg_b=f"{ofb['avg_data_file_kb']:,.0f}",
             o_avg_a=f"{ofa['avg_data_file_kb']:,.0f}")

    opt = r["optimization"]
    v["opt_wall"] = f"{opt['wall_seconds']:.0f}"
    v["task_rows"] = "\n".join(f"{tex(t)} & {i['duration_s']:.1f} \\\\" for t, i in opt["tasks"].items())

    ing = r["ingest"]
    po = ing["per_table"]["shop.orders"]
    v.update(load_rows=f"{il['total_rows']:,}", load_pg=f"{il['postgres_load_seconds']:.0f}",
             load_e2e=f"{il['seconds_until_queryable_in_lakehouse']:.0f}",
             load_rps=f"{il['end_to_end_rows_per_second']:,.0f}", batches=f"{ing['batches']}",
             records=f"{ing['records']:,}", ing_rps=f"{float(ing['records_per_sec_while_processing']):,.0f}",
             ing_peak=f"{float(ing['peak_batch_records_per_sec']):,.0f}",
             b_p50=f"{float(ing['batch_seconds_p50']):.1f}", b_p95=f"{float(ing['batch_seconds_p95']):.1f}",
             m_p50=f"{po['merge_ms_p50'] / 1000:.1f}", m_p95=f"{po['merge_ms_p95'] / 1000:.1f}")
    e = fr["end_to_end_seconds"]
    v.update(f_p50=f"{e['p50']:.1f}", f_p95=f"{e['p95']:.1f}", f_max=f"{e['max']:.1f}", f_min=f"{e['min']:.1f}",
             f_n=str(fr["visible"]), f_to=str(fr["timed_out"]),
             f_pipe=f"{fr['pipeline_commit_to_merge_ms']['p50'] / 1000:.1f}")
    ie = inc["end_to_end_seconds"]
    v.update(i_p50=f"{ie['p50']:.1f}", i_max=f"{ie['max']:.1f}")

    qb, qa = ab["before"]["queries"], ab["after"]["queries"]
    def pct(q):
        return f"{(qa[q]['median_wall_ms'] / qb[q]['median_wall_ms'] - 1) * 100:+.0f}"
    v.update(ab_files=str(ab["after"]["files"]["orders"]["data_files"]),
             ab_avg=f"{ab['after']['files']['orders']['avg_data_file_kb']:,.0f}",
             ab_q02=pct("q02_single_month"), ab_q05=pct("q05_top_customers"), ab_q06=pct("q06_full_aggregate"))

    cons = []
    for c in r["consistency_after"]:
        cb = next(x for x in r["consistency_before"] if x["table"] == c["table"])
        cons.append(f"{c['table']} & {c['source_rows']:,} & "
                    f"{'match' if cb['checksum_match'] else 'differ'} / {'match' if c['checksum_match'] else 'differ'} \\\\")
    v["cons_rows"] = "\n".join(cons)
    return v


def main():
    bench = Path(sys.argv[1]) if len(sys.argv) > 1 else max(RESULTS.glob("benchmark-*"))
    figs = HERE / "figures"
    figs.mkdir(parents=True, exist_ok=True)
    for name in ("ingestion", "freshness", "small_files"):
        shutil.copy(bench / f"{name}.png", figs / f"{name}.png")
    shutil.copy(ROOT / "docs" / "report" / "grafana_dashboard.png", figs / "grafana_dashboard.png")
    architecture_pdf(figs / "architecture.pdf")

    v = values(bench)
    src = (HERE / "main_template.tex").read_text()
    for k, val in v.items():
        src = src.replace("<<" + k + ">>", val)
    left = re.findall(r"<<[a-z_0-9]+>>", src)
    if left:
        raise SystemExit(f"unfilled placeholders: {sorted(set(left))}")
    (HERE / "main.tex").write_text(src)

    zpath = HERE / "floe-report-overleaf.zip"
    with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(HERE / "main.tex", "main.tex")
        for f in sorted(figs.iterdir()):
            z.write(f, f"figures/{f.name}")
    print(f"wrote {HERE / 'main.tex'} and {zpath.name} (from {bench.name})")


if __name__ == "__main__":
    main()
