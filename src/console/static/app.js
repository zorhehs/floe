/* Floe Console - single-page app, no dependencies. */
"use strict";

// ------------------------------------------------------------------ DOM helpers
const SVG_NS = "http://www.w3.org/2000/svg";

function h(tag, attrs, ...kids) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === undefined || v === null || v === false) continue;
    if (k === "class") el.className = v;
    else if (k === "text") el.textContent = v;
    else if (k.startsWith("on")) el.addEventListener(k.slice(2), v);
    else if (k === "style") el.style.cssText = v;
    else if (k === "value") el.value = v;
    else el.setAttribute(k, v === true ? "" : v);
  }
  append(el, kids);
  return el;
}

function append(el, kids) {
  for (const kid of kids.flat(Infinity)) {
    if (kid === null || kid === undefined || kid === false) continue;
    el.append(kid instanceof Node ? kid : document.createTextNode(String(kid)));
  }
  return el;
}

function s(tag, attrs, ...kids) {
  const el = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs || {})) if (v !== undefined && v !== null) el.setAttribute(k, v);
  for (const kid of kids.flat()) if (kid) el.append(kid instanceof Node ? kid : document.createTextNode(String(kid)));
  return el;
}

const ICON_PATHS = {
  live: "M2 12h4l3-8 4 16 3-8h6",
  time: "M12 7v5l3 2M21 12a9 9 0 1 1-3-6.7M21 4v4h-4",
  sql: "M8 9 4 12l4 3M16 9l4 3-4 3M13 6l-2 12",
  check: "M4 12.5 9 17.5 20 6.5",
  external: "M14 4h6v6M20 4l-9 9M18 14v6H4V6h6",
  play: "M7 5v14l11-7z",
  stop: "M6 6h12v12H6z",
  refresh: "M20 11a8 8 0 1 0-2.3 5.7M20 4v7h-7",
  dice: "M4 4h16v16H4zM8.5 8.5h.01M15.5 15.5h.01M15.5 8.5h.01M8.5 15.5h.01M12 12h.01",
  download: "M12 4v11M7 10l5 5 5-5M5 20h14",
  up: "M12 19V5M5 12l7-7 7 7",
  down: "M12 5v14M19 12l-7 7-7-7",
};

function icon(name, size) {
  return s("svg", { viewBox: "0 0 24 24", width: size || 16, height: size || 16, fill: "none", stroke: "currentColor",
    "stroke-width": "1.8", "stroke-linecap": "round", "stroke-linejoin": "round", "aria-hidden": "true" },
    s("path", { d: ICON_PATHS[name] }));
}

// ------------------------------------------------------------------ formatting
const nf = new Intl.NumberFormat("en-US");
const fmt = {
  int: (v) => (v === null || v === undefined || v === "" ? "—" : nf.format(Math.round(Number(v)))),
  num: (v, d = 1) => (v === null || v === undefined ? "—" : Number(v).toLocaleString("en-US", { maximumFractionDigits: d, minimumFractionDigits: d })),
  bytes(v) {
    if (v === null || v === undefined) return "—";
    let n = Number(v);
    for (const u of ["B", "KB", "MB", "GB"]) {
      if (Math.abs(n) < 1024) return `${n.toFixed(u === "B" ? 0 : 1)} ${u}`;
      n /= 1024;
    }
    return `${n.toFixed(1)} TB`;
  },
  ms: (v) => (v === null || v === undefined ? "—" : v >= 1000 ? `${(v / 1000).toFixed(2)} s` : `${Math.round(v)} ms`),
  secs(v) {
    if (v === null || v === undefined || Number.isNaN(Number(v))) return "—";
    v = Number(v);
    if (v < 60) return `${v.toFixed(1)} s`;
    if (v < 3600) return `${Math.floor(v / 60)} min ${Math.round(v % 60)} s`;
    if (v < 172800) return `${(v / 3600).toFixed(1)} h`;
    return `${(v / 86400).toFixed(1)} days`;
  },
  date(v) { return parseTs(v); },
  time(v) {
    const d = parseTs(v);
    return d ? d.toLocaleTimeString("en-GB", { hour12: false }) + "." + String(d.getMilliseconds()).padStart(3, "0") : "—";
  },
  datetime(v) {
    const d = parseTs(v);
    return d ? d.toLocaleString("en-GB", { day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false }) : "—";
  },
  ago(v) {
    const d = parseTs(v);
    if (!d) return "—";
    const sec = Math.max(0, (Date.now() - d.getTime()) / 1000);
    if (sec < 60) return `${Math.round(sec)} s ago`;
    if (sec < 3600) return `${Math.round(sec / 60)} min ago`;
    if (sec < 172800) return `${Math.round(sec / 3600)} h ago`;
    return `${Math.round(sec / 86400)} days ago`;
  },
  short: (id) => (id ? `${String(id).slice(0, 6)}…${String(id).slice(-4)}` : "—"),
};

function parseTs(v) {
  if (!v) return null;
  if (v instanceof Date) return v;
  let t = String(v).trim().replace(" ", "T").replace(/(\.\d{3})\d+/, "$1");
  if (!/[zZ]|[+-]\d\d:?\d\d$/.test(t)) t += "Z"; // timestamps without zone are UTC
  const d = new Date(t);
  return Number.isNaN(d.getTime()) ? null : d;
}

// ------------------------------------------------------------------ API + jobs
async function api(path, options = {}) {
  const init = { method: options.method || "GET", headers: {} };
  if (options.body !== undefined) {
    init.method = options.method || "POST";
    init.headers["Content-Type"] = "application/json";
    init.body = JSON.stringify(options.body);
  }
  let res;
  try {
    res = await fetch(path, init);
  } catch (e) {
    throw new Error("Console server unreachable - is the stack running?");
  }
  let data = null;
  try { data = await res.json(); } catch (e) { /* non-JSON error page */ }
  if (!res.ok) throw new Error((data && data.error) || `${res.status} ${res.statusText}`);
  return data;
}

async function followJob(job, onUpdate, ctx) {
  let current = job;
  onUpdate(current);
  while (current.state === "running") {
    await sleep(500);
    if (ctx && !ctx.alive) return current;
    try {
      current = await api(`/api/jobs/${current.id}`);
    } catch (e) {
      current = { ...current, state: "failed", error: e.message };
    }
    onUpdate(current);
  }
  return current;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function toast(message, kind) {
  const el = h("div", { class: `toast ${kind || ""}`, text: message });
  document.getElementById("toasts").append(el);
  setTimeout(() => el.remove(), kind === "bad" ? 7000 : 4000);
}

function logView(job) {
  const pre = h("pre", { class: "log" });
  for (const line of job.log || []) {
    pre.append(h("span", { class: "t", text: fmt.time(line.t) + "  " }), line.msg + "\n");
  }
  if (job.error) pre.append(h("span", { class: "error-text", text: "error: " + job.error }));
  return pre;
}

// ------------------------------------------------------------------ building blocks
function card(title, hint, body, right) {
  return h("section", { class: "card" },
    h("div", { class: "card-head" }, h("h2", { text: title }), hint ? h("span", { class: "hint", text: hint }) : null,
      right ? h("div", { class: "right" }, right) : null),
    body);
}

function chip(text, kind) { return h("span", { class: `chip ${kind || ""}`, text }); }
function dot(kind, live) { return h("span", { class: `dot ${kind || ""} ${live ? "live" : ""}` }); }

function stateChip(state) {
  const kind = { success: "ok", done: "ok", failed: "bad", upstream_failed: "bad", running: "accent",
    queued: "", scheduled: "", up_for_retry: "warn", skipped: "" }[state] ?? "";
  return chip(state ? state.replace(/_/g, " ") : "pending", kind);
}

function button(label, onclick, opts = {}) {
  return h("button", { class: `btn ${opts.kind || ""} ${opts.small ? "small" : ""}`, type: "button", onclick,
    disabled: opts.disabled, title: opts.title }, opts.icon ? icon(opts.icon) : null, label);
}

async function busy(btn, fn) {
  const label = [...btn.childNodes];
  btn.disabled = true;
  btn.replaceChildren(h("span", { class: "spinner" }), ...label.filter((n) => n.nodeType === 3));
  try { return await fn(); } finally { btn.disabled = false; btn.replaceChildren(...label); }
}

function dataTable(columns, rows, opts = {}) {
  const thead = h("thead", {}, h("tr", {}, columns.map((c) => h("th", { class: c.num ? "num" : "", text: c.label }))));
  const tbody = h("tbody");
  if (!rows.length) {
    tbody.append(h("tr", {}, h("td", { colspan: columns.length, class: "empty", text: opts.empty || "No rows" })));
  }
  for (const row of rows) {
    const tr = h("tr", { class: opts.onRow ? "click" : "" });
    if (opts.onRow) tr.addEventListener("click", () => opts.onRow(row, tr));
    if (opts.selected && opts.selected(row)) tr.classList.add("selected");
    for (const c of columns) {
      const v = c.render ? c.render(row) : row[c.key];
      const td = h("td", { class: [c.num ? "num" : "", c.mono ? "mono" : "", c.wrap ? "wrap" : ""].join(" ") });
      if (v === null || v === undefined) { td.classList.add("null"); td.textContent = c.nullText ?? "—"; }
      else if (c.bar) {
        const frac = Math.max(0, Math.min(1, Number(c.bar(row)) || 0));
        const alert = c.alert ? c.alert(row) : false;
        td.classList.add("barcell");
        if (alert) td.classList.add("alert-text");
        td.append(h("i", { class: `cellbar ${alert ? "alert" : ""}`, style: `width:calc(${(frac * 100).toFixed(1)}% - 8px)` }), append(h("span"), [v]));
      }
      else append(td, [v]);
      tr.append(td);
    }
    tbody.append(tr);
  }
  return h("div", { class: `table-wrap ${opts.tall ? "tall" : ""}` }, h("table", { class: "data" }, thead, tbody));
}

function niceMax(v) {
  if (!(v > 0)) return 1;
  const p = Math.pow(10, Math.floor(Math.log10(v)));
  for (const m of [1, 2, 2.5, 5, 10]) if (v <= m * p) return m * p;
  return 10 * p;
}

function lineChart(host, points, opts = {}) {
  host.replaceChildren();
  host.classList.add("chart-host");
  const pts = (points || []).filter((p) => Number.isFinite(p[1]));
  if (pts.length < 2) {
    host.append(h("div", { class: "empty-chart", text: opts.empty || "Collecting data - the exporter reports every 15 s" }));
    return;
  }
  const W = Math.max(320, Math.round(host.clientWidth || 600)), H = 196, L = 50, R = 16, T = 18, B = 26;
  const vals = pts.map((p) => p[1]).slice().sort((a, b) => a - b);
  const p90 = vals[Math.floor(vals.length * 0.9)];
  const median = vals[Math.floor(vals.length / 2)];
  let max = vals[vals.length - 1];
  let clipped = false;
  if (p90 > 0 && max > p90 * 8) { max = p90 * 2; clipped = true; } // one outlier must not flatten the chart
  const thr = opts.threshold;
  if (thr && thr.value <= max * 1.6) max = Math.max(max, thr.value);
  const yMax = niceMax(max * 1.08);
  const t0 = pts[0][0], t1 = pts[pts.length - 1][0];
  const x = (t) => L + ((t - t0) / Math.max(1, t1 - t0)) * (W - L - R);
  const y = (v) => T + (1 - Math.min(v, yMax) / yMax) * (H - T - B);
  const f = opts.fmt || ((n, d = 0) => fmt.num(n, d));
  const svgEl = s("svg", { class: "chart", viewBox: `0 0 ${W} ${H}`, width: W, height: H, role: "img", "aria-label": opts.label || "chart" });
  for (let i = 0; i <= 4; i++) {
    const v = (yMax / 4) * i, yy = y(v);
    svgEl.append(s("line", { class: `grid-line ${i === 0 ? "base" : ""}`, x1: L, x2: W - R, y1: yy, y2: yy }));
    const digits = yMax < 1 ? 2 : yMax < 10 ? 1 : 0;
    svgEl.append(s("text", { class: "axis", x: L - 8, y: yy + 3.5, "text-anchor": "end" }, f(v, digits)));
  }
  for (const frac of [0, 0.25, 0.5, 0.75, 1]) {
    const t = t0 + (t1 - t0) * frac;
    const label = new Date(t * 1000).toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", hour12: false });
    svgEl.append(s("text", { class: "axis", x: x(t), y: H - 6, "text-anchor": frac === 0 ? "start" : frac === 1 ? "end" : "middle" }, label));
  }
  const d = pts.map((p, i) => `${i ? "L" : "M"}${x(p[0]).toFixed(1)},${y(p[1]).toFixed(1)}`).join("");
  svgEl.append(s("path", { class: "area", d: `${d}L${x(t1).toFixed(1)},${y(0)}L${x(t0).toFixed(1)},${y(0)}Z` }));
  if (median > 0 && median < yMax) {
    svgEl.append(s("line", { class: "median", x1: L, x2: W - R, y1: y(median), y2: y(median) }));
    svgEl.append(s("text", { class: "guide-label", x: W - R, y: y(median) + 13, "text-anchor": "end" }, `median ${f(median, 1)}`));
  }
  if (thr) {
    if (thr.value <= yMax) {
      svgEl.append(s("line", { class: "threshold", x1: L, x2: W - R, y1: y(thr.value), y2: y(thr.value) }));
      svgEl.append(s("text", { class: "guide-label alert", x: L + 4, y: y(thr.value) - 4 }, thr.label || `alert ${f(thr.value)}`));
    } else {
      svgEl.append(s("text", { class: "guide-label alert", x: L + 4, y: T + 2 }, `${thr.label || "alert"} - off scale`));
    }
  }
  svgEl.append(s("path", { class: "line", d, pathLength: 1 }));
  if (clipped) {
    for (const p of pts) if (p[1] > yMax) {
      svgEl.append(s("path", { class: "clip-mark", d: `M${x(p[0]) - 4},${T + 7}L${x(p[0]) + 4},${T + 7}L${x(p[0])},${T}Z` },
        s("title", {}, `${f(p[1], 1)} (off scale)`)));
    }
  }
  const last = pts[pts.length - 1];
  svgEl.append(s("line", { class: "now-line", x1: x(last[0]), x2: x(last[0]), y1: T, y2: y(0) }));
  svgEl.append(s("circle", { class: "chart-pulse", cx: x(last[0]), cy: y(last[1]), r: 4.5 }));
  svgEl.append(s("circle", { class: "chart-dot", cx: x(last[0]), cy: y(last[1]), r: 4.5 }));
  const label = f(last[1], 1);
  const pw = label.length * 7 + 14, px = Math.min(x(last[0]) - pw / 2, W - R - pw), py = Math.max(T - 4, y(last[1]) - 30);
  svgEl.append(s("g", { class: "now-pill" }, s("rect", { x: px, y: py, width: pw, height: 20, rx: 10 }),
    s("text", { x: px + pw / 2, y: py + 14, "text-anchor": "middle" }, label)));

  // hover: crosshair + tooltip on the nearest sample
  const cross = s("line", { class: "crosshair", x1: 0, x2: 0, y1: T, y2: y(0), visibility: "hidden" });
  const focus = s("circle", { class: "focus", r: 4, visibility: "hidden" });
  const zone = s("rect", { class: "hover-zone", x: L, y: 0, width: W - L - R, height: H });
  const tip = h("div", { class: "chart-tip", hidden: true });
  svgEl.append(cross, focus, zone);
  zone.addEventListener("mousemove", (ev) => {
    const rect = svgEl.getBoundingClientRect();
    const mx = (ev.clientX - rect.left) * (W / rect.width);
    let best = pts[0], bd = Infinity;
    for (const p of pts) { const dd = Math.abs(x(p[0]) - mx); if (dd < bd) { bd = dd; best = p; } }
    const cx = x(best[0]);
    cross.setAttribute("x1", cx); cross.setAttribute("x2", cx); cross.setAttribute("visibility", "visible");
    focus.setAttribute("cx", cx); focus.setAttribute("cy", y(best[1])); focus.setAttribute("visibility", "visible");
    tip.hidden = false;
    tip.style.left = `${Math.max(60, Math.min(rect.width - 60, cx * (rect.width / W)))}px`;
    tip.replaceChildren(h("b", { text: f(best[1], 1) }), h("small", { text: new Date(best[0] * 1000).toLocaleTimeString("en-GB", { hour12: false }) }));
  });
  zone.addEventListener("mouseleave", () => { cross.setAttribute("visibility", "hidden"); focus.setAttribute("visibility", "hidden"); tip.hidden = true; });
  host.append(svgEl, tip);
}

function sparkline(points) {
  const pts = (points || []).filter((p) => Number.isFinite(p[1]));
  if (pts.length < 2) return null;
  const W = 200, H = 44, T = 4, B = 4;
  const vals = pts.map((p) => p[1]).slice().sort((a, b) => a - b);
  const p90 = vals[Math.floor(vals.length * 0.9)];
  let max = vals[vals.length - 1];
  if (p90 > 0 && max > p90 * 8) max = p90 * 2;
  max = max > 0 ? max * 1.05 : 1;
  const t0 = pts[0][0], t1 = pts[pts.length - 1][0];
  const x = (t) => ((t - t0) / Math.max(1, t1 - t0)) * W;
  const y = (v) => T + (1 - Math.min(v, max) / max) * (H - T - B);
  const d = pts.map((p, i) => `${i ? "L" : "M"}${x(p[0]).toFixed(1)},${y(p[1]).toFixed(1)}`).join("");
  const last = pts[pts.length - 1];
  return s("svg", { class: "spark", viewBox: `0 0 ${W} ${H}`, preserveAspectRatio: "none", "aria-hidden": "true" },
    s("line", { class: "base", x1: 0, x2: W, y1: H - B + 0.5, y2: H - B + 0.5 }),
    s("path", { class: "area", d: `${d}L${W},${H - B}L0,${H - B}Z` }),
    s("path", { class: "line", d, pathLength: 1, "vector-effect": "non-scaling-stroke" }),
    s("circle", { class: "now", cx: x(last[0]), cy: y(last[1]), r: 3.5, "vector-effect": "non-scaling-stroke" }));
}

// change over the last five minutes of a series: [t, v] samples
function delta(points, window = 300) {
  const pts = (points || []).filter((p) => Number.isFinite(p[1]));
  if (pts.length < 2) return null;
  const last = pts[pts.length - 1];
  const ref = [...pts].reverse().find((p) => p[0] <= last[0] - window) || pts[0];
  if (last[0] - ref[0] < window * 0.5) return null;
  return { now: last[1], before: ref[1], abs: last[1] - ref[1] };
}

function deltaChip(d, opts = {}) {
  if (!d) return null;
  const f = opts.fmt || ((v) => fmt.num(v, 1));
  const eps = opts.eps ?? 0;
  if (Math.abs(d.abs) <= eps) return h("span", { class: "delta", title: "no change over the last 5 min", text: "steady" });
  const up = d.abs > 0;
  const good = opts.good === "down" ? !up : opts.good === "up" ? up : null;
  const kind = good === null ? "" : good ? "good" : "bad";
  return h("span", { class: `delta ${kind}`, title: `${f(d.before)} five minutes ago` }, icon(up ? "up" : "down", 10), f(Math.abs(d.abs)));
}

// stacked columns: inserts / updates / deletes per micro-batch
function stackChart(host, batches, opts = {}) {
  host.replaceChildren();
  const by = new Map();
  for (const b of batches) {
    const e = by.get(b.batch_id) || { id: b.batch_id, ins: 0, upd: 0, del: 0, at: b.batch_finished_at };
    e.ins += b.inserts || 0; e.upd += b.updates || 0; e.del += b.deletes || 0;
    by.set(b.batch_id, e);
  }
  const rows = [...by.values()].sort((a, b) => a.id - b.id).slice(-opts.limit || -28);
  if (rows.length < 2) return;
  const W = Math.max(320, Math.round(host.clientWidth || 600)), H = 130, L = 44, R = 8, T = 10, B = 20;
  const totals = rows.map((r) => r.ins + r.upd + r.del).sort((a, b) => a - b);
  const p90 = totals[Math.floor(totals.length * 0.9)];
  let top = totals[totals.length - 1];
  if (p90 > 0 && top > p90 * 4) top = p90 * 2.2; // the initial load must not flatten the live batches
  const max = niceMax(Math.max(1, top));
  const slot = (W - L - R) / rows.length, bw = Math.max(3, Math.min(22, slot - 4));
  const y = (v) => T + (1 - Math.min(v, max) / max) * (H - T - B);
  const svgEl = s("svg", { class: "stack-chart", viewBox: `0 0 ${W} ${H}`, width: W, height: H, role: "img", "aria-label": "Change events per micro-batch" });
  for (let i = 0; i <= 2; i++) {
    const v = (max / 2) * i;
    svgEl.append(s("line", { class: "grid-line", x1: L, x2: W - R, y1: y(v), y2: y(v) }));
    svgEl.append(s("text", { class: "axis", x: L - 8, y: y(v) + 3.5, "text-anchor": "end" }, fmt.int(v)));
  }
  rows.forEach((r, i) => {
    const cx = L + slot * i + slot / 2 - bw / 2;
    let acc = 0;
    for (const [k, cls] of [["ins", "ins"], ["upd", "upd"], ["del", "del"]]) {
      if (!r[k]) continue;
      const y1 = y(acc + r[k]), y2 = y(acc);
      svgEl.append(s("rect", { class: cls, x: cx, y: y1, width: bw, height: Math.max(1, y2 - y1 - (acc ? 1.5 : 0)), rx: 2 },
        s("title", {}, `batch ${r.id} · ${fmt.int(r.ins)} inserts, ${fmt.int(r.upd)} updates, ${fmt.int(r.del)} deletes · ${fmt.ago(r.at)}`)));
      acc += r[k];
    }
    if (acc > max) svgEl.append(s("path", { class: "del", d: `M${cx + bw / 2 - 4},${T + 6}L${cx + bw / 2 + 4},${T + 6}L${cx + bw / 2},${T - 1}Z` },
      s("title", {}, `batch ${r.id}: ${fmt.int(acc)} events (off scale)`)));
    if (i === 0 || i === rows.length - 1 || i % Math.ceil(rows.length / 6) === 0) {
      svgEl.append(s("text", { class: "axis", x: cx + bw / 2, y: H - 5, "text-anchor": "middle" }, `#${r.id}`));
    }
  });
  host.append(
    h("div", { class: "legend-row" },
      h("span", {}, h("i", { style: "background:var(--accent-strong)" }), "inserts"),
      h("span", {}, h("i", { style: "background:var(--accent)" }), "updates"),
      h("span", {}, h("i", { style: "background:var(--alert)" }), "deletes")),
    svgEl);
}

// snapshot timeline: added / deleted records per Iceberg snapshot, oldest first
function snapshotChart(host, snaps, opts = {}) {
  host.replaceChildren();
  host.classList.add("chart-host");
  const rows = [...(snaps || [])].reverse().map((snap) => ({
    snap, ins: Number(snap.added_records) || 0, del: Number(snap.deleted_records) || 0, pd: Number(snap.added_position_deletes) || 0 }));
  if (rows.length < 2) return;
  const W = Math.max(320, Math.round(host.clientWidth || 600)), H = 130, L = 44, R = 8, T = 10, B = 20;
  const totals = rows.map((r) => r.ins + r.del).sort((a, b) => a - b);
  const p90 = totals[Math.floor(totals.length * 0.9)];
  let top = totals[totals.length - 1];
  if (p90 > 0 && top > p90 * 4) top = p90 * 2.2; // the initial load must not flatten the micro-batch snapshots
  const max = niceMax(Math.max(1, top));
  const slot = (W - L - R) / rows.length, bw = Math.max(3, Math.min(22, slot - 4));
  const y = (v) => T + (1 - Math.min(v, max) / max) * (H - T - B);
  const timeOf = (v) => { const d = fmt.date(v); return d ? d.toLocaleTimeString("en-GB", { hour12: false }) : "—"; };
  const svgEl = s("svg", { class: "stack-chart", viewBox: `0 0 ${W} ${H}`, width: W, height: H, role: "img", "aria-label": "Records added and deleted per snapshot" });
  for (let i = 0; i <= 2; i++) {
    const v = (max / 2) * i;
    svgEl.append(s("line", { class: "grid-line", x1: L, x2: W - R, y1: y(v), y2: y(v) }));
    svgEl.append(s("text", { class: "axis", x: L - 8, y: y(v) + 3.5, "text-anchor": "end" }, fmt.int(v)));
  }
  const tip = h("div", { class: "chart-tip", hidden: true });
  const every = Math.ceil(rows.length / 6);
  rows.forEach((r, i) => {
    const x0 = L + slot * i, cx = x0 + slot / 2 - bw / 2;
    const selected = !!(opts.selected && opts.selected(r.snap));
    const g = s("g", { class: `col ${selected ? "selected" : ""}` });
    if (selected) g.append(s("rect", { class: "sel", x: x0 + 1, y: T - 4, width: slot - 2, height: H - T - B + 6, rx: 4 }));
    g.append(s("rect", { class: "hit", x: x0, y: 0, width: slot, height: H }));
    let acc = 0;
    for (const k of ["ins", "del"]) {
      if (!r[k]) continue;
      const y1 = y(acc + r[k]), y2 = y(acc);
      g.append(s("rect", { class: k, x: cx, y: y1, width: bw, height: Math.max(1, y2 - y1 - (acc ? 1.5 : 0)), rx: 2 }));
      acc += r[k];
    }
    if (acc > max) g.append(s("path", { class: "del", d: `M${cx + bw / 2 - 4},${T + 6}L${cx + bw / 2 + 4},${T + 6}L${cx + bw / 2},${T - 1}Z` }));
    if (r.pd > 0) g.append(s("rect", { class: "pd", x: cx - 2, y: y(r.pd) - 1.25, width: bw + 4, height: 2.5, rx: 1 }));
    if (i === 0 || i === rows.length - 1 || i % every === 0) {
      svgEl.append(s("text", { class: "axis", x: x0 + slot / 2, y: H - 5, "text-anchor": i === 0 ? "start" : i === rows.length - 1 ? "end" : "middle" }, timeOf(r.snap.committed_at)));
    }
    g.addEventListener("mouseenter", () => {
      const rect = svgEl.getBoundingClientRect(), hostRect = host.getBoundingClientRect();
      const parts = [`${fmt.int(r.ins)} added`, `${fmt.int(r.del)} deleted`];
      if (r.pd) parts.push(`${fmt.int(r.pd)} position deletes`);
      tip.replaceChildren(h("b", { text: parts.join(" · ") }),
        h("small", { text: `${r.snap.operation} · ${fmt.datetime(r.snap.committed_at)}` }),
        h("small", { text: `snapshot ${r.snap.snapshot_id}` }));
      tip.hidden = false;
      const half = tip.offsetWidth / 2 + 4;
      tip.style.top = `${rect.top - hostRect.top + 2}px`;
      tip.style.left = `${Math.max(half, Math.min(rect.width - half, (x0 + slot / 2) * (rect.width / W)))}px`;
    });
    g.addEventListener("mouseleave", () => { tip.hidden = true; });
    if (opts.onSelect) g.addEventListener("click", () => opts.onSelect(r.snap));
    svgEl.append(g);
  });
  host.append(
    h("div", { class: "legend-row" },
      h("span", {}, h("i", { style: "background:var(--accent-strong)" }), "added records"),
      h("span", {}, h("i", { style: "background:var(--alert)" }), "deleted records"),
      h("span", {}, h("i", { style: "background:var(--alert-strong);height:3px;vertical-align:3px" }), "position deletes")),
    svgEl, tip);
}

// ------------------------------------------------------------------ router
const ROUTES = {
  overview: { eyebrow: "01 · Operate", title: "Overview", sub: "Health, freshness and throughput of the CDC pipeline", render: viewOverview },
  live: { eyebrow: "02 · Operate", title: "Live CDC", sub: "Change a row in PostgreSQL and trace it into the Iceberg lakehouse", render: viewLive },
  tables: { eyebrow: "03 · Inspect", title: "Tables & schema", sub: "Source vs lakehouse, files, and schema evolution", render: viewTables },
  "time-travel": { eyebrow: "04 · Inspect", title: "Time travel", sub: "Every micro-batch is an Iceberg snapshot - query any of them", render: viewTimeTravel },
  sql: { eyebrow: "05 · Query", title: "SQL workbench", sub: "Read-only Trino queries over lakehouse, postgresql and tpch catalogs", render: viewSql },
  maintenance: { eyebrow: "06 · Optimise", title: "Maintenance", sub: "Airflow DAGs that compact, re-layout and purge the Iceberg tables", render: viewMaintenance },
  gdpr: { eyebrow: "07 · Comply", title: "Right to erasure", sub: "A GDPR delete reaches Iceberg as a merge-on-read delete - the purge makes it physical", render: viewGdpr },
  validate: { eyebrow: "08 · Verify", title: "Validation", sub: "PostgreSQL vs Iceberg: row counts and order-independent checksums", render: viewValidate },
  results: { eyebrow: "09 · Evaluate", title: "Benchmark", sub: "Recorded results of make bench (TPC-H sf1)", render: viewResults },
};

let ctx = null;

function currentRoute() {
  const raw = location.hash.replace(/^#\/?/, "");
  const [name, query] = raw.split("?");
  return { name: ROUTES[name] ? name : "overview", params: new URLSearchParams(query || "") };
}

function navigate() {
  const { name, params } = currentRoute();
  if (ctx) ctx.close();
  ctx = makeContext();
  const route = ROUTES[name];
  document.getElementById("page-title").textContent = route.title;
  document.getElementById("page-eyebrow").textContent = route.eyebrow;
  document.getElementById("page-sub").textContent = route.sub;
  document.title = `${route.title} · Floe`;
  document.querySelectorAll(".nav a").forEach((a) => {
    if (a.getAttribute("href") === `#/${name}`) a.setAttribute("aria-current", "page");
    else a.removeAttribute("aria-current");
  });
  const view = document.getElementById("view");
  view.replaceChildren();
  setHero(null);
  window.scrollTo(0, 0);
  // entrance motion only on the first paint of a page: later refreshes must not flicker
  view.classList.add("fresh");
  clearTimeout(navigate.freshTimer);
  navigate.freshTimer = setTimeout(() => view.classList.remove("fresh"), 2600);
  Promise.resolve(route.render(view, ctx, params)).catch((e) => view.append(errorBanner(e)));
}

function makeContext() {
  const timers = [];
  const listeners = [];
  const ticks = [];
  const c = {
    alive: true,
    every(ms, fn) {
      let running = false;
      const tick = async (force) => {
        // background tabs skip polling; the first load and a return to the tab always run
        if (!c.alive || running || (document.hidden && force !== true)) return;
        running = true;
        try { await fn(); } catch (e) { console.warn(e); } finally { running = false; }
      };
      ticks.push(tick);
      tick(true);
      timers.push(setInterval(tick, ms));
    },
    refresh() { ticks.forEach((t) => t(true)); },
    on(target, event, fn) { target.addEventListener(event, fn); listeners.push([target, event, fn]); },
    close() {
      c.alive = false;
      timers.forEach(clearInterval);
      listeners.forEach(([t, e, f]) => t.removeEventListener(e, f));
    },
  };
  return c;
}

function errorBanner(e) {
  return h("div", { class: "banner bad" }, h("div", {}, h("h3", { text: "Something went wrong" }), h("p", { text: e.message || String(e) })));
}

// ------------------------------------------------------------------ top bar
const globalState = { status: null, workload: null };

async function refreshTopbar() {
  try {
    const [status, wl] = await Promise.all([api("/api/status"), api("/api/workload")]);
    globalState.status = status;
    globalState.workload = wl;
    const hp = document.getElementById("health-pill");
    const allOk = status.healthy === status.total;
    hp.replaceChildren(dot(allOk ? "ok" : status.healthy >= status.total - 2 ? "warn" : "bad", true),
      h("strong", { text: `${status.healthy}/${status.total}` }), allOk ? "components healthy" : "components healthy");
    const wp = document.getElementById("workload-pill");
    wp.replaceChildren(dot(wl.running ? "ok" : "", wl.running), "Workload ",
      h("strong", { text: wl.running ? `${fmt.int(wl.ops_per_sec)} ops/s` : "off" }));
  } catch (e) {
    const hp = document.getElementById("health-pill");
    hp.replaceChildren(dot("bad"), h("strong", { text: "offline" }));
  }
}

// ------------------------------------------------------------------ Overview
const MAIN_FLOW = ["postgres", "debezium", "kafka", "spark", "iceberg", "trino"];
const SIDE = ["minio", "airflow", "prometheus", "grafana"];

function nodeEl(c) {
  return h("div", { class: `node ${c.ok ? "" : "bad"}` },
    h("i", { class: "packet", "aria-hidden": "true" }),
    h("div", { class: "node-top" }, dot(c.ok ? "ok" : "bad"), h("span", { class: "node-name", text: c.name }),
      c.link ? h("a", { class: "open", href: c.link, target: "_blank", rel: "noopener", title: `Open the ${c.name} UI`, "aria-label": `Open the ${c.name} UI` }, icon("external", 13)) : null),
    h("div", { class: "node-role", text: c.role }),
    h("div", { class: "node-detail", text: c.detail, title: c.detail }));
}

function kpi(label, value, unit, note, kind, fraction, opts = {}) {
  const spark = opts.series ? sparkline(opts.series) : null;
  return h("div", { class: "kpi" },
    h("div", { class: "kpi-label" }, kind ? dot(kind) : null, label, opts.series ? deltaChip(delta(opts.series), opts) : null),
    h("div", { class: "kpi-value" }, value, unit ? h("small", { text: unit }) : null),
    note ? h("div", { class: "kpi-note", text: note }) : null,
    spark || (fraction === undefined || fraction === null ? null : meter(fraction)));
}

// filled share + highlight + tick track (scale of the KPI against its target)
function meter(fraction) {
  const pct = Math.max(0, Math.min(1, Number(fraction) || 0)) * 100;
  return h("div", { class: "meter", role: "presentation" },
    h("i", { class: "fill", style: `width:${pct.toFixed(1)}%` }), h("i", { class: "mark" }), h("span", { class: "ticks" }));
}

// ------------------------------------------------------------------ hero insight (top layout)
let heroTimer = null;
function setHero(items) {
  const host = document.getElementById("hero-card");
  clearInterval(heroTimer);
  if (!items || !items.length) { host.replaceChildren(); host.hidden = true; return; }
  host.hidden = false;
  let i = Number(host.dataset.index || 0) % items.length;
  const render = () => {
    const it = items[i];
    host.replaceChildren(
      h("div", { class: "hero-head" }, icon("live", 15), h("span", { text: it.title }), h("a", { href: it.href, class: "hero-go", "aria-label": "Open" }, icon("external", 14))),
      h("p", { class: `hero-text ${it.text.map((x) => (x instanceof Node ? x.textContent : String(x))).join("").length > 95 ? "long" : ""}` }, ...it.text),
      h("div", { class: "hero-dots" }, items.map((_, j) => h("i", { class: j === i ? "on" : "" }))));
    host.dataset.index = String(i);
  };
  render();
  heroTimer = setInterval(() => { i = (i + 1) % items.length; render(); }, 6000);
}

async function viewOverview(view, ctx) {
  const banner = h("div");
  const kpis = h("div", { class: "kpis" }, [1, 2, 3, 4, 5].map(() => h("div", { class: "kpi" }, h("div", { class: "skeleton", style: "height:52px" }))));
  const flow = h("div", { class: "flow" });
  const services = h("div", { class: "services" });
  const checked = h("span", { class: "hint" });
  const chartF = h("div"), chartI = h("div");
  const batchesHost = h("div");
  const batchChartHost = h("div");
  const alertsHost = h("div", { class: "alerts" }, [1, 2, 3, 4, 5].map(() => h("div", { class: "alert" }, h("div", { class: "skeleton", style: "height:44px" }))));
  const alertsChip = h("span");
  let hideProbe = true;
  const probeBtn = button("Show heartbeat", () => {
    hideProbe = !hideProbe;
    probeBtn.lastChild.textContent = hideProbe ? "Show heartbeat" : "Hide heartbeat";
    loadBatches();
  }, { small: true, kind: "ghost" });

  view.append(
    banner,
    kpis,
    card("Pipeline", null, [h("div", { class: "flow-caption", style: "margin-top:0" }, h("span", { text: "Change data path" }), h("span", { text: "PostgreSQL → Trino" })), flow,
      h("div", { class: "flow-caption" }, h("span", { text: "Storage · orchestration · observability" })), services], checked),
    h("div", { class: "grid cols-2" },
      card("End-to-end freshness", "heartbeat age in Iceberg · last 30 min", chartF),
      card("Ingest rate", "records merged per second · last 30 min", chartI)),
    h("div", { class: "split" }, workloadCard(ctx),
      card("Recent micro-batches", "lakehouse.ops.ingest_batches", [batchChartHost, batchesHost], probeBtn)),
    card("Alert rules", "Prometheus · config/monitoring/alerts.yml", alertsHost, alertsChip));

  let metrics = null;
  const lastKpiText = {};
  ctx.every(5000, async () => {
    const status = await api("/api/status");
    globalState.status = status;
    const by = Object.fromEntries(status.components.map((c) => [c.id, c]));
    flow.replaceChildren(...MAIN_FLOW.map((id) => nodeEl(by[id])));
    services.replaceChildren(...SIDE.map((id) => nodeEl(by[id])));
    checked.textContent = `checked ${fmt.time(status.checked_at)}`;
    const live = (metrics && metrics.ingest_rps > 0.5) || (globalState.workload && globalState.workload.running);
    flow.classList.toggle("live", Boolean(live));
  });

  ctx.every(10000, async () => {
    const ov = await api("/api/overview");
    banner.replaceChildren();
    if (!ov.has_data) banner.append(loadBanner(ctx));
  });

  ctx.every(5000, async () => {
    metrics = await api("/api/metrics");
    if (metrics.error) {
      kpis.replaceChildren(h("div", { class: "banner bad", style: "grid-column:1/-1" },
        h("div", {}, h("h3", { text: "Metrics unavailable" }), h("p", { text: metrics.error }))));
      return;
    }
    const f = metrics.freshness_s;
    const fKind = f === null ? "" : f < 30 ? "ok" : f < 120 ? "warn" : "bad";
    const [lagNum, lagUnit] = fmt.bytes(metrics.slot_lag_bytes).split(" ");
    const lagKind = metrics.slot_lag_bytes === null ? "" : metrics.slot_lag_bytes < 64 * 1024 * 1024 ? "ok" : "warn";
    kpis.replaceChildren(
      kpi("Freshness", f === null ? "—" : f >= 120 ? fmt.secs(f) : fmt.num(f, 1), f === null || f >= 120 ? null : "s",
        f >= 120 ? "last heartbeat merged into Iceberg this long ago" : "PostgreSQL commit → queryable in Trino", fKind, f === null ? null : f / 60,
        { series: metrics.series.freshness, good: "down", fmt: (v) => fmt.secs(v), eps: 0.5 }),
      kpi("Ingest rate", fmt.num(metrics.ingest_rps || 0, 1), "rec/s", `${fmt.int(metrics.batches_5m)} micro-batches in 5 min`,
        null, (metrics.ingest_rps || 0) / Math.max(1, ...metrics.series.ingest.map((p) => p[1])),
        { series: metrics.series.ingest, good: "up", fmt: (v) => `${fmt.num(v, 1)} rec/s`, eps: 0.2 }),
      kpi("Replication lag", lagNum, lagUnit || null, "WAL not yet confirmed by Debezium", lagKind, (metrics.slot_lag_bytes || 0) / (64 * 1024 * 1024),
        { series: metrics.series.slot_lag, good: "down", fmt: (v) => fmt.bytes(v), eps: 4096 }),
      kpi("Last micro-batch", metrics.last_batch_age_s === null ? "—" : fmt.num(metrics.last_batch_age_s, 0), "s ago", "Spark trigger every 10 s",
        null, metrics.last_batch_age_s === null ? null : metrics.last_batch_age_s / 30),
      kpi("Rows in lakehouse", fmt.int(metrics.lake_rows), null, `source ${fmt.num(metrics.source_tps || 0, 1)} commits/s`));
    kpis.querySelectorAll(".kpi-value").forEach((el, i) => {
      const text = el.textContent;
      if (lastKpiText[i] !== undefined && lastKpiText[i] !== text) el.classList.add("tick");
      lastKpiText[i] = text;
    });
    setHero([
      { title: "Freshness", href: "#/live", text: f === null ? ["Waiting for the first heartbeat."] :
        f >= 120 ? ["Iceberg is ", h("b", { text: fmt.secs(f) }), " behind PostgreSQL - the streaming job is catching up on the heartbeat table"] :
        ["Right now, a change committed in PostgreSQL is ", h("b", { text: "queryable in Iceberg" }), " after ", h("b", { text: `${fmt.num(f, 1)} seconds` })] },
      { title: "Throughput", href: "#/overview", text: ["The streaming job merged ", h("b", { text: `${fmt.int(metrics.batches_5m)} micro-batches` }), " in the last five minutes, ", h("b", { text: `${fmt.num(metrics.ingest_rps || 0, 1)} records/s` })] },
      { title: "Lakehouse", href: "#/tables", text: [h("b", { text: `${fmt.int(metrics.lake_rows)} rows` }), " live in Apache Iceberg, every change kept as a ", h("b", { text: "queryable snapshot" })] },
    ]);
    drawCharts();
  });

  async function loadBatches() {
    const data = await api("/api/batches?limit=120");
    const all = data.batches.filter((b) => !hideProbe || b.table_name !== "shop.cdc_probe");
    const rows = all.slice(0, 10);
    stackChart(batchChartHost, all, { limit: 30 });
    const maxEvents = Math.max(1, ...rows.map((r) => r.kafka_records || 0));
    const maxMerge = Math.max(1, ...rows.map((r) => r.merge_ms || 0));
    batchesHost.replaceChildren(dataTable([
      { label: "Batch", key: "batch_id", mono: true },
      { label: "Table", render: (r) => r.table_name.replace("shop.", "") },
      { label: "Events", num: true, render: (r) => fmt.int(r.kafka_records), bar: (r) => r.kafka_records / maxEvents },
      { label: "Ins / upd / del", num: true, render: (r) => `${fmt.int(r.inserts)} / ${fmt.int(r.updates)} / ${fmt.int(r.deletes)}` },
      { label: "MERGE", num: true, render: (r) => fmt.ms(r.merge_ms), bar: (r) => r.merge_ms / maxMerge, alert: (r) => r.merge_ms >= 10000 },
      { label: "Finished", render: (r) => fmt.ago(r.batch_finished_at) },
    ], rows, { empty: hideProbe ? "No data micro-batches yet - start the workload" : "No micro-batches yet" }));
  }
  ctx.every(5000, loadBatches);

  ctx.every(10000, async () => {
    try {
      const a = await api("/api/alerts");
      const kind = a.firing ? "bad" : a.pending ? "warn" : "ok";
      alertsChip.replaceChildren(chip(a.firing ? `${a.firing} firing` : a.pending ? `${a.pending} pending` : "all quiet", kind));
      alertsHost.replaceChildren(...a.rules.map((r) => h("div", { class: `alert ${r.state}` },
        h("div", { class: "alert-top" }, dot(r.state === "firing" ? "bad" : r.state === "pending" ? "warn" : "ok", r.state !== "inactive"),
          h("span", { class: "name", text: r.name }), chip(r.state === "inactive" ? "ok" : r.state, r.state === "firing" ? "bad" : r.state === "pending" ? "warn" : "")),
        h("p", { text: r.summary || "" }),
        h("code", { title: r.expr, text: `${r.expr}${r.for_seconds ? ` for ${fmt.secs(r.for_seconds)}` : ""}` }))));
    } catch (e) {
      alertsChip.replaceChildren(chip("unavailable", "warn"));
      alertsHost.replaceChildren(h("div", { class: "empty", text: e.message }));
    }
  });

  function drawCharts() {
    if (!metrics || metrics.error) return;
    lineChart(chartF, metrics.series.freshness, { fmt: (v, d = 0) => `${fmt.num(v, d)} s`, label: "End-to-end freshness",
      threshold: { value: 120, label: "alert · 2 min behind" } });
    lineChart(chartI, metrics.series.ingest, { fmt: (v, d = 0) => `${fmt.num(v, d)}`, label: "Ingest rate" });
  }
  ctx.on(window, "resize", drawCharts);
}

function loadBanner(ctx) {
  const out = h("div", { class: "stack" });
  const go = (scale) => async (ev) => {
    await busy(ev.currentTarget, async () => {
      const job = await api("/api/load", { body: { scale } });
      const logHost = h("div");
      out.append(logHost);
      const done = await followJob(job, (j) => logHost.replaceChildren(logView(j)), ctx);
      if (done.state === "done") toast("Data loaded and streamed into Iceberg", "ok");
      else if (done.state === "failed") toast(done.error, "bad");
    });
  };
  out.append(h("div", { class: "banner" },
    h("div", {}, h("h3", { text: "The source database is empty" }),
      h("p", { text: "Load TPC-H into PostgreSQL. Debezium captures every row and Spark merges it into Iceberg." })),
    h("div", { class: "right" },
      button("Load tiny (16k rows)", go("tiny"), { kind: "primary" }),
      button("Load sf1 (1.65M rows)", go("sf1")))));
  return out;
}

function workloadCard(ctx) {
  const body = h("div", { class: "stack" });
  const range = h("input", { class: "range", type: "range", min: "10", max: "500", step: "10", value: "50", "aria-label": "Operations per second" });
  const rateLabel = h("b", { text: "50 ops/s" });
  range.addEventListener("input", () => { rateLabel.textContent = `${range.value} ops/s`; });
  const chipHost = h("span");
  const counters = h("div", { class: "counters" });
  const ops = h("div");
  const startBtn = button("Start", async (ev) => {
    await busy(ev.currentTarget, async () => {
      try {
        const wl = await api("/api/workload", { body: { action: "start", ops_per_sec: Number(range.value) } });
        render(wl);
        refreshTopbar();
      } catch (e) { toast(e.message, "bad"); }
    });
  }, { kind: "primary", icon: "play" });
  const stopBtn = button("Stop", async (ev) => {
    await busy(ev.currentTarget, async () => {
      try { render(await api("/api/workload", { body: { action: "stop" } })); refreshTopbar(); } catch (e) { toast(e.message, "bad"); }
    });
  }, { icon: "stop" });
  let touched = false;
  range.addEventListener("change", () => { touched = true; });

  function render(wl) {
    globalState.workload = wl;
    chipHost.replaceChildren(wl.running ? chip("running", "ok") : chip("stopped"));
    if (!touched && wl.running) { range.value = String(wl.ops_per_sec); rateLabel.textContent = `${wl.ops_per_sec} ops/s`; }
    startBtn.lastChild.textContent = wl.running ? "Apply rate" : "Start";
    stopBtn.disabled = !wl.running;
    counters.replaceChildren(
      h("div", { class: "counter" }, h("b", { text: fmt.int(wl.total) }), h("span", { text: "operations" })),
      h("div", { class: "counter" }, h("b", { text: fmt.num(wl.actual_ops_per_sec, 1) }), h("span", { text: "actual ops/s" })),
      h("div", { class: "counter" }, h("b", { text: fmt.int(wl.errors) }), h("span", { text: "rolled back" })));
    const done = wl.done || {};
    const names = { insert_order: "new orders", update_order: "order updates", delete_order: "order deletes",
      update_customer: "balance updates", insert_customer: "new customers", delete_customer: "customer deletes" };
    ops.replaceChildren(h("dl", { class: "kv" }, Object.keys(names).map((k) => [h("dt", { text: names[k] }), h("dd", { text: fmt.int(done[k] || 0) })])));
    if (wl.last_error) ops.append(h("p", { class: "error-text", text: wl.last_error }));
  }

  body.append(
    h("p", { class: "dim", style: "margin:0", text: "Synthetic OLTP traffic on PostgreSQL: 45% new orders, 20% order updates, 10% order deletes, 15% balance updates, 5% new and 5% deleted customers. One transaction per 100 ms tick." }),
    h("div", { class: "row" }, h("span", { class: "muted", text: "Rate" }), rateLabel),
    range,
    h("div", { class: "row" }, startBtn, stopBtn),
    counters, ops);
  ctx.every(2000, async () => render(await api("/api/workload")));
  return card("Workload generator", null, body, chipHost);
}

// ------------------------------------------------------------------ Live CDC
const traceHistory = [];

async function viewLive(view, ctx) {
  let mode = "update_customer";
  const seg = h("div", { class: "seg", role: "group", "aria-label": "Change type" });
  const modes = [["update_customer", "Update a customer"], ["insert_order", "Insert an order"], ["delete_order", "Delete an order"]];
  const form = h("form", { class: "stack" });
  const result = h("div", { class: "stack" }, h("div", { class: "empty", text: "Commit a change on the left - the trace appears here." }));
  const historyHost = h("div");

  for (const [m, label] of modes) {
    seg.append(h("button", { type: "button", "aria-pressed": String(m === mode), text: label, onclick: () => {
      mode = m;
      seg.querySelectorAll("button").forEach((b, i) => b.setAttribute("aria-pressed", String(modes[i][0] === m)));
      renderForm();
    } }));
  }

  const inputs = {};
  function field(name, label, attrs, small) {
    inputs[name] = h("input", { class: "input", name, ...attrs });
    return h("label", { class: "field" }, h("span", { text: label }), inputs[name], small ? h("small", { text: small }) : null);
  }

  async function randomInto(input, table) {
    try { input.value = (await api(`/api/tables/${table}/sample-key`)).key; } catch (e) { toast(e.message, "bad"); }
  }

  function renderForm() {
    for (const k of Object.keys(inputs)) delete inputs[k];
    const submit = h("button", { class: "btn primary", type: "submit" }, "Commit to PostgreSQL and trace");
    let fields;
    if (mode === "update_customer") {
      fields = [
        h("div", { class: "row", style: "align-items:end" },
          field("key", "Customer key", { inputmode: "numeric", placeholder: "random" }),
          button("Random", () => randomInto(inputs.key, "customer"), { icon: "dice" })),
        h("div", { class: "fields" },
          field("balance", "New account balance", { inputmode: "decimal", placeholder: "random" }),
          field("name", "New name (optional)", { maxlength: "25", placeholder: "unchanged" })),
      ];
    } else if (mode === "insert_order") {
      const pr = h("select", { class: "input", name: "priority" },
        ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"].map((p) => h("option", { value: p, text: p, selected: p === "3-MEDIUM" })));
      inputs.priority = pr;
      fields = [h("div", { class: "fields" },
        field("custkey", "Customer key", { inputmode: "numeric", placeholder: "random" }),
        field("total_price", "Total price", { inputmode: "decimal", placeholder: "random" }),
        h("label", { class: "field" }, h("span", { text: "Priority" }), pr))];
    } else {
      fields = [h("div", { class: "row", style: "align-items:end" },
        field("key", "Order key", { inputmode: "numeric", placeholder: "random" }),
        button("Random", () => randomInto(inputs.key, "orders"), { icon: "dice" }))];
    }
    const explain = {
      update_customer: "UPDATE shop.customer in PostgreSQL. Debezium reads it from the WAL, Spark MERGEs it into lakehouse.shop.customer.",
      insert_order: "INSERT into shop.orders. The new row arrives in Iceberg as an insert in the next micro-batch.",
      delete_order: "DELETE from shop.orders. Iceberg records a row-level delete (merge-on-read), the data file is untouched.",
    }[mode];
    form.replaceChildren(h("p", { class: "dim", style: "margin:0", text: explain }), ...fields, h("div", {}, submit));
  }

  form.addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const btn = form.querySelector("button[type=submit]");
    const body = { action: mode };
    for (const [k, el] of Object.entries(inputs)) if (el.value !== "") body[k] = el.value;
    await busy(btn, async () => {
      let job;
      try { job = await api("/api/trace", { body }); } catch (e) { toast(e.message, "bad"); return; }
      const started = Date.now();
      const tick = setInterval(() => {
        const el = result.querySelector(".latency b");
        if (el) el.textContent = ((Date.now() - started) / 1000).toFixed(1);
      }, 100);
      try {
        const done = await followJob(job, (j) => renderTrace(j), ctx);
        if (done.state === "done") {
          traceHistory.unshift(done.result);
          renderHistory();
        } else if (done.state === "failed") toast(done.error, "bad");
      } finally { clearInterval(tick); }
    });
  });

  function renderTrace(job) {
    const r = job.result;
    if (job.state === "running") {
      const committed = job.log.length > 0;
      result.replaceChildren(
        h("div", { class: "latency" }, h("b", { text: "0.0" }), h("span", { text: "s - waiting for the lakehouse" })),
        h("ol", { class: "steps" },
          step(committed ? "done" : "active", "Committed in PostgreSQL", committed ? job.log[0].msg.replace("PostgreSQL commit: ", "") : "writing…"),
          step(committed ? "active" : "", "Captured by Debezium", "reads the change from the WAL (logical replication slot) and publishes it to Kafka"),
          step(committed ? "active" : "", "Merged into Iceberg", "Spark Structured Streaming MERGEs the micro-batch (trigger every 10 s)"),
          step("", "Visible in Trino", "polling lakehouse.shop every 400 ms")));
      return;
    }
    if (job.state === "failed") {
      result.replaceChildren(h("div", { class: "banner bad" }, h("div", {}, h("h3", { text: "Trace failed" }), h("p", { text: job.error }))));
      return;
    }
    const c0 = parseTs(r.committed_at);
    const rel = (ts) => { const d = parseTs(ts); return d && c0 ? `${((d - c0) / 1000 >= 0 ? "+" : "")}${((d - c0) / 1000).toFixed(2)} s` : ""; };
    const cdc = r.cdc || {};
    const batch = r.batch;
    result.replaceChildren(
      h("div", { class: "row" },
        h("div", { class: "latency" }, h("b", { text: r.latency_s.toFixed(1) }), h("span", { text: "s PostgreSQL commit → visible in Trino" })),
        h("span", { style: "margin-left:auto" }, chip(r.action.replace("_", " "), "accent"))),
      h("ol", { class: "steps" },
        step("done", "Committed in PostgreSQL", r.description, fmt.time(r.committed_at)),
        step("done", "Captured by Debezium",
          cdc._cdc_lsn ? `WAL position ${cdc._cdc_lsn} · topic pg.shop.${r.table}` : `delete event on topic pg.shop.${r.table}`, ""),
        step("done", "Merged into Iceberg",
          batch ? `micro-batch #${batch.batch_id} · ${fmt.int(batch.kafka_records)} change events · MERGE ${fmt.ms(batch.merge_ms)}` : "merged by the streaming job",
          batch ? rel(batch.batch_finished_at) : cdc._ingested_at ? rel(cdc._ingested_at) : ""),
        step("done", "Visible in Trino", `lakehouse.shop.${r.table} · ${r.polls} polls`, `+${r.latency_s.toFixed(2)} s`)),
      rowCompare(r));
  }

  function step(state, title, text, time) {
    return h("li", { class: `step ${state}` }, h("span", { class: "mark" }), h("div", {}, h("h4", { text: title }), h("p", { text })), h("time", { text: time || "" }));
  }

  function rowCompare(r) {
    const src = r.source_row || {}, lake = r.lake_row || {};
    if (!r.source_row && !r.lake_row) {
      return h("div", { class: "banner ok" }, h("div", {}, h("h3", { text: `Row ${r.key} is gone from both sides` }),
        h("p", { text: "Open Time travel to read it from an earlier snapshot." })));
    }
    const cols = [...new Set([...Object.keys(src), ...Object.keys(lake)])];
    const rows = cols.map((c) => ({ column: c, pg: src[c], lake: lake[c], meta: c.startsWith("_cdc") || c === "_ingested_at" }));
    return dataTable([
      { label: "Column", render: (x) => (x.meta ? h("span", { class: "muted", text: x.column }) : x.column), mono: true },
      { label: "PostgreSQL", key: "pg", mono: true, nullText: "" },
      { label: "Iceberg (Trino)", key: "lake", mono: true },
    ], rows);
  }

  function renderHistory() {
    historyHost.replaceChildren(dataTable([
      { label: "Change", key: "description", wrap: true },
      { label: "Committed", render: (r) => fmt.time(r.committed_at) },
      { label: "Latency", num: true, render: (r) => h("b", { text: `${r.latency_s.toFixed(1)} s` }) },
    ], traceHistory, { empty: "No traces yet in this session" }));
  }

  renderForm();
  renderHistory();
  view.append(
    h("div", { class: "split" },
      h("div", { class: "stack", style: "gap:20px" },
        card("Make a change", null, h("div", { class: "stack" }, seg, form)),
        card("Session traces", "most recent first", historyHost)),
      card("Trace", "every timestamp is measured, not estimated", result)));
}

// ------------------------------------------------------------------ Tables & schema
async function viewTables(view, ctx, params) {
  let selected = params.get("table") || null;
  const cards = h("div", { class: "tcards" }, h("div", { class: "skeleton", style: "height:150px" }));
  const detail = h("div");
  view.append(cards, detail);

  async function loadCards() {
    const { tables } = await api("/api/tables");
    if (!selected && tables.length) selected = (tables.find((t) => t.table === "orders") || tables[0]).table;
    cards.replaceChildren(...tables.map((t) => {
      const match = t.in_lake && t.source_rows === t.lake_rows;
      return h("button", { class: "tcard", type: "button", "aria-pressed": String(t.table === selected), onclick: () => { selected = t.table; loadCards(); loadDetail(); } },
        h("div", { class: "tcard-name" }, t.table, h("code", { text: "shop" }),
          h("span", { style: "margin-left:auto" }, t.in_lake ? chip(match ? "in sync" : "catching up", match ? "ok" : "warn") : chip("not in lake", "warn"))),
        h("div", { class: "stats" },
          h("div", { class: "stat" }, h("span", { text: "PostgreSQL rows" }), h("b", { text: fmt.int(t.source_rows) })),
          h("div", { class: "stat" }, h("span", { text: "Iceberg rows" }), h("b", { text: fmt.int(t.lake_rows) })),
          h("div", { class: "stat" }, h("span", { text: "Data files" }), h("b", { text: fmt.int(t.data_files) })),
          h("div", { class: "stat" }, h("span", { text: "Delete files" }), h("b", { class: t.delete_files ? "bad" : "", text: fmt.int(t.delete_files) })),
          h("div", { class: "stat" }, h("span", { text: "Avg file" }), h("b", { text: fmt.bytes(t.avg_file_bytes) })),
          h("div", { class: "stat" }, h("span", { text: "Snapshots" }), h("b", { text: fmt.int(t.snapshots) }))));
    }));
  }

  async function loadDetail() {
    if (!selected) return;
    const table = selected;
    let schema;
    try { schema = await api(`/api/tables/${table}/schema`); } catch (e) { detail.replaceChildren(errorBanner(e)); return; }
    if (table !== selected) return;
    const cols = dataTable([
      { label: "Column", mono: true, render: (c) => h("span", {}, c.name, c.primary_key ? h("span", { class: "chip accent", style: "margin-left:8px", text: "PK" }) : null) },
      { label: "PostgreSQL type", key: "source_type", mono: true, nullText: "" },
      { label: "Iceberg type", key: "lake_type", mono: true, nullText: "not yet" },
      { label: "Status", render: (c) => chip(c.status, { synced: "ok", pending: "warn", metadata: "", "lake only": "" }[c.status]) },
    ], schema.columns);

    const addLog = h("div");
    const colName = h("input", { class: "input", placeholder: "e.g. loyalty_tier", maxlength: "41", "aria-label": "Column name" });
    const colType = h("select", { class: "input", "aria-label": "Column type" },
      ["varchar(50)", "integer", "bigint", "numeric(12,2)", "boolean", "date", "timestamp"].map((t) => h("option", { value: t, text: t })));
    const colValue = h("input", { class: "input", placeholder: "optional, set on 5 rows", "aria-label": "Value" });
    const addBtn = button("Add column", async (ev) => {
      await busy(ev.currentTarget, async () => runSchemaJob(`/api/tables/${table}/columns`,
        { column: colName.value.trim(), type: colType.value, value: colValue.value || null }, addLog));
    }, { kind: "primary" });

    const widenable = schema.columns.filter((c) => c.widenable);
    const widenSel = h("select", { class: "input", "aria-label": "Column to widen" }, widenable.map((c) => h("option", { value: c.name, text: c.name })));
    const widenLog = h("div");
    const widenBtn = button("Widen to bigint", async (ev) => {
      await busy(ev.currentTarget, async () => runSchemaJob(`/api/tables/${table}/widen`, { column: widenSel.value }, widenLog));
    }, { disabled: !widenable.length });

    async function runSchemaJob(url, body, host) {
      let job;
      try { job = await api(url, { body }); } catch (e) { toast(e.message, "bad"); return; }
      const done = await followJob(job, (j) => host.replaceChildren(logView(j)), ctx);
      if (done.state === "done") { toast(`Schema change visible in Iceberg after ${done.result.seconds} s`, "ok"); loadDetail(); }
      else if (done.state === "failed") toast(done.error, "bad");
    }

    const versions = dataTable([
      { label: "Schema id", key: "latest_schema_id", mono: true },
      { label: "Since", render: (v) => fmt.datetime(v.timestamp) },
    ], schema.schema_versions, { empty: "No schema history" });

    detail.replaceChildren(h("div", { class: "split-r" },
      card(`shop.${table}`, "PostgreSQL schema vs Iceberg schema (Trino information_schema)", cols,
        h("a", { class: "btn small", href: `#/time-travel?table=${table}` }, icon("time"), "Snapshots")),
      h("div", { class: "stack", style: "gap:20px" },
        card("Add a column in PostgreSQL", "Iceberg adds it without rewriting data", h("div", { class: "stack" },
          h("div", { class: "fields" },
            h("label", { class: "field" }, h("span", { text: "Name" }), colName),
            h("label", { class: "field" }, h("span", { text: "Type" }), colType)),
          h("label", { class: "field" }, h("span", { text: "Value for the first 5 rows" }), colValue,
            h("small", { text: "Debezium ships the new schema with the next change event, so a few rows are touched." })),
          h("div", {}, addBtn), addLog)),
        card("Widen a column type", "integer → bigint is a safe Iceberg type promotion", h("div", { class: "stack" },
          widenable.length ? h("div", { class: "row" }, widenSel, widenBtn) : h("p", { class: "muted", style: "margin:0", text: "No integer columns left to widen in this table." }),
          widenLog)),
        card("Schema versions", "from $metadata_log_entries", versions))));
  }

  ctx.every(10000, loadCards);
  await loadCards().catch(() => {});
  loadDetail();
}

// ------------------------------------------------------------------ Time travel
async function viewTimeTravel(view, ctx, params) {
  const { tables } = await api("/api/tables");
  const lake = tables.filter((t) => t.in_lake);
  if (!lake.length) { view.append(h("div", { class: "empty", text: "No Iceberg tables yet - load data first." })); return; }
  let table = params.get("table") && lake.some((t) => t.table === params.get("table")) ? params.get("table") : (lake.find((t) => t.table === "orders") || lake[0]).table;
  let selectedSnap = null;

  const tableSel = h("select", { class: "input", "aria-label": "Table" }, lake.map((t) => h("option", { value: t.table, text: `shop.${t.table}`, selected: t.table === table })));
  tableSel.addEventListener("change", () => { table = tableSel.value; selectedSnap = null; history.replaceState(null, "", `#/time-travel?table=${table}`); loadSnaps(); detailHost.replaceChildren(placeholder()); rowHost.replaceChildren(); keyInput.value = ""; });
  const snapsHost = h("div");
  const chartHost = h("div");
  const detailHost = h("div");
  const placeholder = () => h("div", { class: "empty", text: "Select a snapshot to compare it with the current table." });
  detailHost.append(placeholder());
  const keyInput = h("input", { class: "input", inputmode: "numeric", placeholder: "primary key", "aria-label": "Primary key" });
  const rowHost = h("div");

  const isSelected = (r) => selectedSnap && r.snapshot_id === selectedSnap.snapshot_id;
  function select(r) { selectedSnap = r; loadSnaps(); showSnapshot(r); }

  async function loadSnaps() {
    const data = await api(`/api/tables/${table}/snapshots`);
    snapshotChart(chartHost, data.snapshots, { selected: isSelected, onSelect: select });
    snapsHost.replaceChildren(chartHost, dataTable([
      { label: "Committed", render: (r) => fmt.datetime(r.committed_at) },
      { label: "Snapshot", mono: true, render: (r) => fmt.short(r.snapshot_id) },
      { label: "Operation", render: (r) => chip(r.operation, { append: "ok", overwrite: "accent", replace: "", delete: "bad" }[r.operation]) },
      { label: "Added rows", num: true, render: (r) => fmt.int(r.added_records) },
      { label: "Pos. deletes", num: true, render: (r) => fmt.int(r.added_position_deletes) },
      { label: "Records in data files", num: true, render: (r) => fmt.int(r.total_records) },
      { label: "Files", num: true, render: (r) => `${fmt.int(r.total_data_files)} + ${fmt.int(r.total_delete_files)}` },
    ], data.snapshots, { tall: true, onRow: select, selected: isSelected }));
  }

  async function showSnapshot(snap) {
    detailHost.replaceChildren(h("div", { class: "row" }, h("span", { class: "spinner" }), "Reading the snapshot…"));
    try {
      const d = await api(`/api/tables/${table}/snapshots/${snap.snapshot_id}`);
      const changes = Object.entries(d.changed_since);
      const opName = { c: "inserted", u: "updated", r: "snapshot-loaded" };
      const sql = `SELECT *\nFROM lakehouse.shop.${table} FOR VERSION AS OF ${snap.snapshot_id}\nLIMIT 100`;
      detailHost.replaceChildren(h("div", { class: "stack" },
        h("dl", { class: "kv" },
          h("dt", { text: "Snapshot" }), h("dd", { class: "mono", text: d.snapshot_id }),
          h("dt", { text: "Committed" }), h("dd", { text: `${fmt.datetime(snap.committed_at)} (${fmt.ago(snap.committed_at)})` }),
          h("dt", { text: "Operation" }), h("dd", { text: snap.operation }),
          h("dt", { text: "Rows then" }), h("dd", { text: fmt.int(d.rows_then) }),
          h("dt", { text: "Rows now" }), h("dd", { text: `${fmt.int(d.rows_now)} (${d.rows_now - d.rows_then >= 0 ? "+" : ""}${fmt.int(d.rows_now - d.rows_then)})` }),
          h("dt", { text: "Changed since" }), h("dd", { text: changes.length ? changes.map(([op, n]) => `${fmt.int(n)} ${opName[op] || op}`).join(", ") : "nothing" })),
        h("pre", { class: "log", text: sql }),
        h("div", { class: "row" }, button("Open in SQL workbench", () => { sessionStorage.setItem("floe.sql", sql); location.hash = "#/sql"; }, { kind: "primary", icon: "sql" }))));
    } catch (e) { detailHost.replaceChildren(errorBanner(e)); }
  }

  async function showRow() {
    if (!/^-?\d+$/.test(keyInput.value.trim())) { toast("Enter a numeric primary key", "bad"); return; }
    rowHost.replaceChildren(h("div", { class: "row" }, h("span", { class: "spinner" }), "Reading the row in each of the last 12 snapshots - a few seconds…"));
    try {
      const d = await api(`/api/tables/${table}/rows/${keyInput.value.trim()}/history`);
      const vers = d.versions;
      const cols = [...new Set(vers.flatMap((v) => Object.keys(v.row || {})))];
      const shown = cols.filter((c) => c !== d.primary_key);
      const columns = [
        { label: "Snapshot", mono: true, render: (v) => fmt.short(v.snapshot_id) },
        { label: "Committed", render: (v) => fmt.datetime(v.committed_at) },
        ...shown.map((c) => ({ label: c, mono: true, key: c })),
      ];
      const table_ = dataTable(columns, vers.map((v) => ({ ...v, ...(v.row || {}) })), { empty: "No snapshots" });
      // highlight cells that differ from the next older snapshot; mark snapshots where the row did not exist
      const trs = table_.querySelectorAll("tbody tr");
      vers.forEach((v, i) => {
        const older = vers[i + 1];
        const tds = trs[i] && trs[i].querySelectorAll("td");
        if (!tds) return;
        if (!v.row) {
          for (let j = 2; j < tds.length; j++) tds[j].textContent = "";
          if (tds[2]) { tds[2].textContent = "row not present"; tds[2].classList.add("null"); }
          return;
        }
        shown.forEach((c, j) => {
          if (older && (!older.row || String(older.row[c]) !== String(v.row[c]))) tds[j + 2].classList.add("changed");
        });
      });
      const changes = vers.filter((v, i) => vers[i + 1] && JSON.stringify(v.row) !== JSON.stringify(vers[i + 1].row)).length;
      rowHost.replaceChildren(h("p", { class: "dim", style: "margin:0 0 10px", text: `${d.primary_key} = ${d.key}: ${changes} change${changes === 1 ? "" : "s"} across the last ${vers.length} snapshots (highlighted).` }), table_);
    } catch (e) { rowHost.replaceChildren(errorBanner(e)); }
  }
  keyInput.addEventListener("keydown", (e) => { if (e.key === "Enter") showRow(); });

  view.append(
    h("div", { class: "row" }, h("span", { class: "muted", text: "Table" }), tableSel),
    h("div", { class: "split-r" },
      card("Snapshots", "newest first · click one to inspect", snapsHost),
      card("Snapshot", null, detailHost)),
    card("History of one row", "the same key read FOR VERSION AS OF each snapshot", h("div", { class: "stack" },
      h("div", { class: "row" }, keyInput,
        button("Random key", async () => { try { keyInput.value = (await api(`/api/tables/${table}/sample-key`)).key; showRow(); } catch (e) { toast(e.message, "bad"); } }, { icon: "dice" }),
        button("Show history", showRow, { kind: "primary" })),
      h("p", { class: "muted", style: "margin:0", text: "Tip: change this row on the Live CDC page, then come back - every micro-batch that touched it shows up as a highlighted version." }),
      rowHost)));
  await loadSnaps();
  ctx.every(15000, loadSnaps);
}

// ------------------------------------------------------------------ SQL workbench
const SNIPPETS = [
  ["Snapshots of orders", "SELECT committed_at, snapshot_id, operation,\n       element_at(summary, 'added-records') AS added_records,\n       element_at(summary, 'added-position-deletes') AS position_deletes\nFROM lakehouse.shop.\"orders$snapshots\"\nORDER BY committed_at DESC"],
  ["Data and delete files", "SELECT content, count(*) AS files, sum(record_count) AS records,\n       round(avg(file_size_in_bytes) / 1024.0, 1) AS avg_kb\nFROM lakehouse.shop.\"orders$files\"\nGROUP BY content\nORDER BY content"],
  ["Partitions", "SELECT * FROM lakehouse.shop.\"orders$partitions\" LIMIT 50"],
  ["Recent micro-batches", "SELECT batch_id, table_name, kafka_records, inserts, updates, deletes, merge_ms, batch_finished_at\nFROM lakehouse.ops.ingest_batches\nWHERE table_name <> 'shop.cdc_probe'\nORDER BY batch_id DESC\nLIMIT 50"],
  ["Source vs lakehouse counts", "SELECT 'postgresql' AS side, count(*) AS orders FROM postgresql.shop.orders\nUNION ALL\nSELECT 'iceberg', count(*) FROM lakehouse.shop.orders"],
  ["CDC metadata of recent changes", "SELECT o_orderkey, o_orderstatus, o_totalprice, _cdc_op, _cdc_source_ts, _ingested_at\nFROM lakehouse.shop.orders\nORDER BY _ingested_at DESC\nLIMIT 50"],
  ["Time travel (1 hour ago)", "SELECT count(*) AS orders_one_hour_ago\nFROM lakehouse.shop.orders FOR TIMESTAMP AS OF current_timestamp - INTERVAL '1' HOUR"],
  ["Tables in the lakehouse", "SHOW TABLES FROM lakehouse.shop"],
];

async function viewSql(view, ctx) {
  const editor = h("textarea", { class: "editor", spellcheck: "false", "aria-label": "SQL query" });
  editor.value = sessionStorage.getItem("floe.sql") || "SELECT o_orderpriority, count(*) AS orders, round(sum(o_totalprice), 2) AS revenue\nFROM lakehouse.shop.orders\nGROUP BY 1\nORDER BY 1";
  editor.addEventListener("input", () => sessionStorage.setItem("floe.sql", editor.value));
  const out = h("div", { class: "stack" }, h("div", { class: "empty" }, "Run a query with ", h("kbd", { text: "⌘/Ctrl" }), " + ", h("kbd", { text: "Enter" })));
  const list = h("div", { class: "qlist" });
  let last = null;

  function setSql(sql) { editor.value = sql.trim(); sessionStorage.setItem("floe.sql", editor.value); editor.focus(); }

  list.append(h("div", { class: "group", text: "Benchmark suite" }));
  try {
    const { queries } = await api("/api/queries");
    for (const q of queries) {
      const [id, ...rest] = q.title.split(" ");
      list.append(h("button", { type: "button", onclick: () => setSql(q.sql) }, h("b", { text: id }), h("small", { text: rest.join(" ") })));
    }
  } catch (e) { list.append(h("p", { class: "error-text", text: e.message })); }
  list.append(h("div", { class: "group", text: "Lakehouse internals" }));
  for (const [title, sql] of SNIPPETS) list.append(h("button", { type: "button", onclick: () => setSql(sql) }, h("b", { text: title })));

  const runBtn = button("Run", () => run(), { kind: "primary", icon: "play" });
  const csvBtn = button("CSV", () => exportCsv(), { icon: "download", disabled: true, title: "Download the result as CSV" });

  async function run() {
    await busy(runBtn, async () => {
      out.replaceChildren(h("div", { class: "row" }, h("span", { class: "spinner" }), "Running on Trino…"));
      try {
        const r = await api("/api/sql", { body: { sql: editor.value } });
        last = r;
        csvBtn.disabled = !r.rows.length;
        const st = r.stats || {};
        out.replaceChildren(
          h("div", { class: "statbar" },
            h("span", {}, h("b", { text: fmt.int(r.rows.length) }), r.truncated ? ` rows (first ${fmt.int(r.rows.length)})` : " rows"),
            h("span", {}, "elapsed ", h("b", { text: fmt.ms(st.elapsedTimeMillis ?? r.wall_ms) })),
            st.cpuTimeMillis != null ? h("span", {}, "CPU ", h("b", { text: fmt.ms(st.cpuTimeMillis) })) : null,
            st.processedRows != null ? h("span", {}, "scanned ", h("b", { text: `${fmt.int(st.processedRows)} rows` }), ` · ${fmt.bytes(st.physicalInputBytes ?? st.processedBytes)}`) : null,
            st.queryId ? h("a", { href: `http://localhost:8080/ui/query.html?${st.queryId}`, target: "_blank", rel: "noopener", class: "muted" }, st.queryId) : null),
          dataTable(r.columns.map((c, i) => ({ label: c.name, mono: true, num: /int|double|decimal|real/.test(c.type || ""), render: (row) => row[i] })), r.rows, { tall: true, empty: "Query returned no rows" }));
      } catch (e) {
        last = null;
        csvBtn.disabled = true;
        out.replaceChildren(h("div", { class: "banner bad" }, h("div", {}, h("h3", { text: "Query failed" }), h("p", { class: "mono", text: e.message }))));
      }
    });
  }

  function exportCsv() {
    if (!last) return;
    const esc = (v) => (v === null || v === undefined ? "" : /[",\n]/.test(String(v)) ? `"${String(v).replace(/"/g, '""')}"` : String(v));
    const text = [last.columns.map((c) => esc(c.name)).join(","), ...last.rows.map((r) => r.map(esc).join(","))].join("\n");
    const a = h("a", { href: URL.createObjectURL(new Blob([text], { type: "text/csv" })), download: "floe-query.csv" });
    document.body.append(a); a.click(); a.remove();
  }

  editor.addEventListener("keydown", (e) => {
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") { e.preventDefault(); run(); }
    if (e.key === "Tab" && !e.shiftKey) { e.preventDefault(); editor.setRangeText("  ", editor.selectionStart, editor.selectionEnd, "end"); }
  });

  view.append(h("div", { class: "sql-layout" },
    card("Queries", null, list),
    h("div", { class: "stack", style: "gap:20px" },
      card("Query", "read-only · max 1,000 rows · default catalog lakehouse.shop", h("div", { class: "stack" }, editor,
        h("div", { class: "row" }, runBtn, csvBtn, h("span", { class: "muted", style: "font-size:12px" }, h("kbd", { text: "⌘/Ctrl" }), " ", h("kbd", { text: "Enter" }), " runs the query")))),
      card("Result", null, out))));
  editor.focus();
}

// ------------------------------------------------------------------ Maintenance
const DAG_TEXT = {
  iceberg_maintenance: { title: "Hourly housekeeping", text: "Bin-pack compaction, delete-file and manifest rewrite, snapshot expiry (24 h), orphan-file cleanup.", tables: "" },
  iceberg_layout_optimization: { title: "Layout optimisation", text: "Partition evolution to months(o_orderdate) + sort order, full sorted rewrite, then cleanup. The before/after benchmark runs this DAG.", tables: "" },
  iceberg_gdpr_purge: { title: "GDPR purge", text: "Physically erases deleted rows: rewrites every file that has deletes and expires the history that could still read them.", tables: "shop.customer,shop.orders" },
};

async function viewMaintenance(view, ctx) {
  const dagsHost = h("div", { class: "grid cols-3" }, [1, 2, 3].map(() => h("div", { class: "card" }, h("div", { class: "skeleton", style: "height:200px" }))));
  const runHost = h("div");
  const filesHost = h("div");
  let selected = null;
  const tableInputs = {};

  view.append(dagsHost, runHost, card("File layout now", "small files and delete files are what maintenance removes", filesHost,
    button("Refresh", () => loadFiles(), { small: true, icon: "refresh", kind: "ghost" })));

  async function loadDags() {
    const { dags } = await api("/api/dags");
    dagsHost.replaceChildren(...dags.map((d) => {
      const meta = DAG_TEXT[d.dag_id] || { title: d.dag_id, text: d.description, tables: "" };
      if (!(d.dag_id in tableInputs)) tableInputs[d.dag_id] = h("input", { class: "input", value: meta.tables, placeholder: "all tables", "aria-label": "Tables" });
      const runNow = button("Run now", async (ev) => {
        await busy(ev.currentTarget, async () => {
          try {
            const r = await api(`/api/dags/${d.dag_id}/runs`, { body: { conf: { tables: tableInputs[d.dag_id].value } } });
            toast(`Triggered ${d.dag_id}`, "ok");
            selected = { dag: d.dag_id, run: r.dag_run_id };
            await loadDags();
            loadRun();
          } catch (e) { toast(e.message, "bad"); }
        });
      }, { kind: "primary", icon: "play" });
      const scheduleBtn = d.schedule ? button(d.is_paused ? "Enable hourly" : "Pause hourly", async (ev) => {
        await busy(ev.currentTarget, async () => {
          try { await api(`/api/dags/${d.dag_id}/paused`, { body: { paused: !d.is_paused } }); await loadDags(); } catch (e) { toast(e.message, "bad"); }
        });
      }, { small: true }) : null;
      return h("section", { class: "card dag" },
        h("div", { class: "card-head", style: "margin:0" }, h("h2", { text: meta.title }),
          h("div", { class: "right" }, d.schedule ? chip(d.is_paused ? "schedule paused" : `${d.schedule}`, d.is_paused ? "" : "ok") : chip("manual"))),
        h("code", { class: "muted", text: d.dag_id }),
        h("p", { class: "dim", style: "margin:0;font-size:13px", text: meta.text }),
        d.error ? h("p", { class: "error-text", text: d.error }) : null,
        h("label", { class: "field" }, h("span", { text: "Tables" }), tableInputs[d.dag_id]),
        h("div", { class: "row" }, runNow, scheduleBtn),
        d.schedule ? h("small", { class: "muted", text: "Airflow only runs unpaused DAGs: Run now enables the hourly schedule." }) : null,
        h("div", { class: "runs" }, d.runs.length ? d.runs.map((r) => h("button", { type: "button", class: "run",
          "aria-pressed": String(selected && selected.run === r.dag_run_id),
          onclick: () => { selected = { dag: d.dag_id, run: r.dag_run_id }; loadDags(); loadRun(); } },
          stateChip(r.state), h("code", { text: r.dag_run_id }), h("span", { class: "muted", text: fmt.ago(r.start_date) })))
          : h("p", { class: "muted", style: "margin:0;font-size:12.5px", text: "No runs yet" })));
    }));
  }

  async function loadRun() {
    if (!selected) { runHost.replaceChildren(); return; }
    try {
      const d = await api(`/api/dags/${selected.dag}/runs/${encodeURIComponent(selected.run)}`);
      const maxDur = Math.max(1, ...d.tasks.map((t) => t.duration || 0));
      const running = ["running", "queued"].includes(d.run.state);
      const total = d.run.start_date && d.run.end_date ? (parseTs(d.run.end_date) - parseTs(d.run.start_date)) / 1000 : null;
      runHost.replaceChildren(card(`Run ${d.run.dag_run_id}`, `${selected.dag}${total ? ` · ${fmt.secs(total)} wall` : ""}`,
        h("div", { class: "tasks" }, d.tasks.length ? d.tasks.map((t) => h("div", { class: "task" },
          h("div", { class: "stack", style: "gap:4px" }, h("span", { class: "mono", text: t.task_id }),
            h("div", { class: "bar" }, h("i", { style: `width:${Math.max(2, ((t.duration || 0) / maxDur) * 100)}%` }))),
          h("span", { class: "muted", text: t.duration ? fmt.secs(t.duration) : "" }),
          stateChip(t.state))) : h("p", { class: "muted", text: "Waiting for the scheduler…" })),
        h("div", { class: "row" }, running ? h("span", { class: "spinner" }) : null, stateChip(d.run.state),
          h("a", { class: "btn small ghost", href: `http://localhost:8088/dags/${selected.dag}/grid`, target: "_blank", rel: "noopener" }, icon("external"), "Airflow"))));
      if (!running && selected.wasRunning) { toast(`${selected.dag} finished: ${d.run.state}`, d.run.state === "success" ? "ok" : "bad"); loadFiles(); loadDags(); }
      selected.wasRunning = running;
    } catch (e) { runHost.replaceChildren(errorBanner(e)); }
  }

  async function loadFiles() {
    try {
      const { tables } = await api("/api/tables");
      filesHost.replaceChildren(dataTable([
        { label: "Table", render: (t) => `shop.${t.table}` },
        { label: "Rows", num: true, render: (t) => fmt.int(t.lake_rows) },
        { label: "Data files", num: true, render: (t) => fmt.int(t.data_files) },
        { label: "Avg data file", num: true, render: (t) => fmt.bytes(t.avg_file_bytes) },
        { label: "Delete files", num: true, render: (t) => (t.delete_files ? chip(fmt.int(t.delete_files), "warn") : "0") },
        { label: "Partitions", num: true, render: (t) => fmt.int(t.partitions) },
        { label: "Snapshots", num: true, render: (t) => fmt.int(t.snapshots) },
      ], tables.filter((t) => t.in_lake)));
    } catch (e) { filesHost.replaceChildren(errorBanner(e)); }
  }

  ctx.every(8000, loadDags);
  ctx.every(3000, loadRun);
  loadFiles();
}

// ------------------------------------------------------------------ GDPR: right to erasure
async function viewGdpr(view, ctx) {
  const keyInput = h("input", { class: "input", inputmode: "numeric", placeholder: "random customer with orders", "aria-label": "Customer key" });
  const subjectHost = h("div");
  const eraseBtn = button("Erase in PostgreSQL", onErase, { kind: "primary", icon: "stop", disabled: true });
  const pause = h("input", { type: "checkbox", id: "pause-wl", checked: true });
  const purgeBtn = button("Run the purge DAG", onPurge, { kind: "primary", icon: "play", disabled: true });
  const purgeBox = h("div", { class: "stack" },
    h("p", { class: "dim", style: "margin:0", text: "The purge DAG rewrites every data file that has deletes and expires the snapshots that could still read the row. Time travel across the erasure is lost on purpose." }),
    h("label", { class: "check", for: "pause-wl" }, pause, "Pause the workload while it runs (comparable before/after)"),
    h("div", {}, purgeBtn));
  const purgeCard = card("Purge", "make it physical", purgeBox);
  purgeCard.hidden = true;
  const story = h("div", { class: "stack" }, h("div", { class: "empty", text: "Pick a customer on the left. Every step on this page is measured against the running stack." }));
  let subject = null, erased = null;

  async function loadSubject(key) {
    subjectHost.replaceChildren(h("div", { class: "skeleton", style: "height:110px" }));
    try {
      subject = await api(`/api/gdpr/candidate${key ? `?key=${encodeURIComponent(key)}` : ""}`);
      keyInput.value = subject.key;
      const c = subject.customer;
      subjectHost.replaceChildren(h("div", { class: "subject" },
        stat("Customer", `#${subject.key} · ${c.c_name}`), stat("Phone", c.c_phone),
        stat("Address", c.c_address, true), stat("Balance", c.c_acctbal),
        stat("Orders held", `${fmt.int(subject.orders)} · ${fmt.num(subject.order_total, 2)} total`, true)));
      eraseBtn.disabled = false;
      erased = null;
      purgeCard.hidden = true;
      setHero([{ title: "Right to erasure", href: "#/gdpr", text: ["Customer ", h("b", { text: `#${subject.key}` }), " asks to be forgotten: ", h("b", { text: `${fmt.int(subject.orders)} orders` }), " and a row held in every Iceberg snapshot."] }]);
    } catch (e) { subject = null; eraseBtn.disabled = true; subjectHost.replaceChildren(errorBanner(e)); }
  }

  function stat(label, value, wide) {
    return h("div", { class: `stat ${wide ? "wide" : ""}` }, h("span", { text: label }), h("b", { text: value ?? "—" }));
  }

  async function onErase(ev) {
    if (!subject) return;
    await busy(ev.currentTarget, async () => {
      let job;
      try { job = await api("/api/gdpr/erase", { body: { key: subject.key } }); } catch (e) { toast(e.message, "bad"); return; }
      const done = await followJob(job, (j) => renderErase(j), ctx);
      if (done.state === "done") {
        erased = done.result;
        purgeCard.hidden = false;
        purgeBtn.disabled = false;
        eraseBtn.disabled = true;
        setHero([{ title: "Right to erasure", href: "#/gdpr", text: ["Customer ", h("b", { text: `#${erased.key}` }), " is gone from every query after ", h("b", { text: `${erased.latency_s.toFixed(1)} s` }), erased.still_readable ? " - but time travel still reads the data" : ""] }]);
      } else if (done.state === "failed") toast(done.error, "bad");
    });
  }

  async function onPurge(ev) {
    if (!erased) return;
    await busy(ev.currentTarget, async () => {
      let job;
      try { job = await api("/api/gdpr/purge", { body: { key: erased.key, snapshot_id: erased.snapshot_id, pause_workload: pause.checked } }); }
      catch (e) { toast(e.message, "bad"); return; }
      const done = await followJob(job, (j) => renderPurge(j), ctx);
      if (done.state === "done") {
        purgeBtn.disabled = true;
        const r = done.result;
        setHero([{ title: "Right to erasure", href: "#/gdpr", text: r.history.readable ? ["The personal data of ", h("b", { text: `#${r.key}` }), " is ", h("b", { text: "still readable" }), " - the purge did not expire the snapshot"] :
          ["The personal data of customer ", h("b", { text: `#${r.key}` }), " no longer exists ", h("b", { text: "anywhere in storage" }), " - ", fmt.secs(r.wall_seconds), " of maintenance"] }]);
        toast("Erasure complete", "ok");
      } else if (done.state === "failed") toast(done.error, "bad");
    });
  }

  function step(state, title, text, time) {
    return h("li", { class: `step ${state}` }, h("span", { class: "mark" }), h("div", {}, h("h4", { text: title }), text instanceof Node ? text : h("p", { text })), h("time", { text: time || "" }));
  }

  function filesText(f) { return `${fmt.int(f.data_files)} data files · ${fmt.int(f.delete_files)} delete files · ${fmt.int(f.snapshots)} snapshots`; }

  function renderErase(job) {
    const log = job.log || [];
    const r = job.result;
    if (job.state === "failed") { story.replaceChildren(h("div", { class: "banner bad" }, h("div", {}, h("h3", { text: "Erasure failed" }), h("p", { text: job.error })))); return; }
    const committed = log.some((l) => l.msg.startsWith("PostgreSQL commit"));
    const gone = Boolean(r);
    story.replaceChildren();
    append(story, [
      h("div", { class: "row" },
        h("div", { class: "latency" }, h("b", { text: r ? r.latency_s.toFixed(1) : "…" }), h("span", { text: "s until the row is gone from every Iceberg query" })),
        h("span", { style: "margin-left:auto" }, chip(gone ? "erased from queries" : "erasing", gone ? "ok" : "accent"))),
      h("ol", { class: "steps" },
        step(committed ? "done" : "active", "Committed in PostgreSQL", committed ? log.find((l) => l.msg.startsWith("PostgreSQL commit")).msg.replace("PostgreSQL commit: ", "") : "one transaction: the orders, then the customer", r ? fmt.time(r.committed_at) : ""),
        step(gone ? "done" : committed ? "active" : "", "Gone from the current snapshot", gone ? `lakehouse.shop.customer and .orders · ${r.polls} polls` : "Debezium publishes delete events; Spark MERGEs them (WHEN MATCHED AND op = 'd' THEN DELETE)", r ? `+${r.latency_s.toFixed(2)} s` : ""),
        step(gone ? "done" : "", "Merge-on-read: nothing rewritten", gone ? h("div", {}, h("p", { text: `customer: ${filesText(r.files_after)}` }), h("p", { class: "muted", text: `before the delete: ${filesText(r.files_before)}. The delete only added a position-delete file; the data file still holds the row.` })) : "the delete is recorded in a small position-delete file"),
        step(gone ? (r.still_readable ? "done" : "") : "", "Still readable through time travel", gone ? (r.still_readable
          ? h("div", {}, h("p", { text: `SELECT … FROM customer FOR VERSION AS OF ${r.snapshot_id} WHERE c_custkey = ${r.key}` }),
            h("dl", { class: "ghost-row" }, ...Object.entries(r.still_readable).flatMap(([k, v]) => [h("dt", { text: k }), h("dd", { text: v ?? "—" })])))
          : h("p", { text: "the remembered snapshot no longer holds the row" })) : "the pre-erasure snapshot can still be queried")),
      gone ? h("div", { class: "banner" }, h("div", {}, h("h3", { text: "The right to erasure is not satisfied yet" }),
        h("p", { text: "Delete files hide the row, they do not remove it. Run the purge DAG to rewrite the files and expire the history." }))) : null]);
  }

  function renderPurge(job) {
    const r = job.result;
    const log = job.log || [];
    if (job.state === "failed") { story.append(h("div", { class: "banner bad" }, h("div", {}, h("h3", { text: "Purge failed" }), h("p", { text: job.error })))); return; }
    let host = story.querySelector(".purge");
    if (!host) { host = h("div", { class: "purge stack" }); story.append(host); }
    const tasks = log.filter((l) => l.msg.startsWith("task ")).map((l) => l.msg.slice(5));
    const banner = story.querySelector(".banner:not(.ok):not(.bad)");
    if (banner) banner.remove();
    host.replaceChildren();
    append(host, [
      h("ol", { class: "steps" },
        step(r ? "done" : "active", "Purge DAG iceberg_gdpr_purge", r ? `${r.tasks.map((t) => `${t.task_id} ${t.state}`).join(" · ")} · ${fmt.secs(r.wall_seconds)} wall` : (tasks.length ? tasks[tasks.length - 1] : "triggering the Airflow run…")),
        step(r ? "done" : "", "Files rewritten, history expired", r ? h("div", { class: "pair-stat" },
          h("div", {}, "Data files", h("b", { text: `${fmt.int(r.files_before.data_files)} → ${fmt.int(r.files_after.data_files)}` })),
          h("div", {}, "Delete files", h("b", { text: `${fmt.int(r.files_before.delete_files)} → ${fmt.int(r.files_after.delete_files)}` })),
          h("div", {}, "Snapshots", h("b", { text: `${fmt.int(r.files_before.snapshots)} → ${fmt.int(r.files_after.snapshots)}` }))) : "rewrite_data_files with a delete-file threshold of 1, then expire_snapshots keeping only the newest"),
        step(r ? "done" : "", "Time travel to the old snapshot", r ? (r.history.readable
          ? h("p", { class: "error-text", text: "the row is still readable - the snapshot was not expired" })
          : h("p", { text: `FOR VERSION AS OF ${r.snapshot_id} → ${r.history.error || "the snapshot no longer holds the row"}` })) : "will fail once the snapshot is expired")),
      r ? h("div", { class: `banner ${r.history.readable ? "bad" : "ok"}` }, h("div", {},
        h("h3", { text: r.history.readable ? "Not erased" : `Customer #${r.key} is erased from storage` }),
        h("p", { text: r.history.readable ? "Check the Airflow run." : "No current query, no time-travel query and no file in MinIO holds the personal data any more." }))) : null,
      logView(job)]);
  }

  const randomBtn = button("Random", () => loadSubject(""), { icon: "dice" });
  const showBtn = button("Show customer", () => loadSubject(keyInput.value.trim()), { kind: "" });
  view.append(h("div", { class: "split" },
    h("div", { class: "stack", style: "gap:20px" },
      card("Erasure request", "the data subject", h("div", { class: "stack" },
        h("p", { class: "dim", style: "margin:0", text: "A customer asks to be forgotten. The operational database deletes their row and their orders; the lakehouse must forget them too, including its history." }),
        h("div", { class: "row", style: "align-items:end" }, h("label", { class: "field", style: "flex:1" }, h("span", { text: "Customer key" }), keyInput), showBtn, randomBtn),
        subjectHost,
        h("div", {}, eraseBtn))),
      purgeCard),
    card("What the lakehouse sees", "every timestamp is measured, not estimated", story)));
  keyInput.addEventListener("keydown", (ev) => { if (ev.key === "Enter") { ev.preventDefault(); loadSubject(keyInput.value.trim()); } });
  loadSubject("");
}

// ------------------------------------------------------------------ Validation
async function viewValidate(view, ctx) {
  const wait = h("input", { type: "checkbox", id: "wait" });
  const warn = h("div");
  const out = h("div", { class: "stack" });
  const runBtn = button("Run validation", async (ev) => {
    await busy(ev.currentTarget, async () => {
      let job;
      try { job = await api("/api/validate", { body: { wait: wait.checked } }); } catch (e) { toast(e.message, "bad"); return; }
      const logHost = h("div");
      out.replaceChildren(logHost);
      const done = await followJob(job, (j) => logHost.replaceChildren(logView(j)), ctx);
      if (done.state === "done") render(done.result);
    });
  }, { kind: "primary", icon: "check" });

  function render(r) {
    out.replaceChildren(
      h("div", { class: `banner ${r.consistent ? "ok" : "bad"}` }, h("div", {},
        h("h3", { text: r.consistent ? "PostgreSQL and Iceberg are identical" : "Differences found" }),
        h("p", { text: r.consistent ? `Every table has the same row count and checksum (${fmt.secs(r.seconds)}).`
          : r.workload_running ? "The workload is still writing - in-flight changes make the sides differ. Stop it or tick “wait until consistent”." : "The pipeline may still be catching up. Run again with “wait until consistent”." }))),
      dataTable([
        { label: "Table", render: (t) => `shop.${t.table}` },
        { label: "PostgreSQL rows", num: true, render: (t) => fmt.int(t.source_rows) },
        { label: "Iceberg rows", num: true, render: (t) => fmt.int(t.lake_rows) },
        { label: "Columns compared", num: true, render: (t) => t.compared_columns ?? "—" },
        { label: "Checksum", render: (t) => (t.checksum_match === undefined ? "—" : chip(t.checksum_match ? "match" : "differs", t.checksum_match ? "ok" : "bad")) },
        { label: "Result", render: (t) => chip(t.ok ? "OK" : "DIFF", t.ok ? "ok" : "bad") },
        { label: "Note", wrap: true, render: (t) => (t.note ? h("span", { class: "muted", text: t.note }) : "") },
      ], r.tables));
  }

  view.append(card("PostgreSQL vs Iceberg", null, h("div", { class: "stack" },
    h("p", { class: "dim", style: "margin:0", text: "Trino reads both catalogs and compares, for every table, the row count and an order-independent checksum over every shared column: checksum(row(CAST(col AS varchar), …)). The pipeline's own metadata columns are excluded." }),
    warn,
    h("div", { class: "row" }, runBtn, h("label", { class: "check", for: "wait" }, wait, "Wait until consistent (up to 5 min)")),
    out)));

  ctx.every(4000, async () => {
    const wl = await api("/api/workload");
    warn.replaceChildren(wl.running ? h("div", { class: "banner" }, h("div", {}, h("h3", { text: "The workload is running" }),
      h("p", { text: "Rows change while they are compared. Stop the workload on the Overview page, or tick “wait until consistent”." }))) : "");
  });
}

// ------------------------------------------------------------------ Benchmark results
async function viewResults(view) {
  const r = await api("/api/results");
  const b = r.benchmark;
  if (!b) { view.append(h("div", { class: "empty", text: "No benchmark yet - run make bench." })); return; }
  const il = r.ingest_load, fr = r.freshness;
  const sumB = b.queries.reduce((a, q) => a + q.before_ms, 0), sumA = b.queries.reduce((a, q) => a + q.after_ms, 0);
  const delB = Object.values(b.files.before).reduce((a, f) => a + (f.delete_files || 0), 0);
  const delA = Object.values(b.files.after).reduce((a, f) => a + (f.delete_files || 0), 0);
  const tpch = ["orders", "customer", "nation", "region"];
  const objB = tpch.reduce((a, t) => a + ((b.storage.before || {})[t]?.objects || 0), 0);
  const objA = tpch.reduce((a, t) => a + ((b.storage.after || {})[t]?.objects || 0), 0);

  const maxMs = Math.max(...b.queries.map((q) => Math.max(q.before_ms, q.after_ms)));
  const bars = h("div", { class: "bars" }, b.queries.map((q) => {
    const sp = q.before_ms / q.after_ms;
    return h("div", { class: "q" },
      h("span", { class: "label", title: q.id, text: q.id.replace(/^q(\d+)_/, "Q$1 ").replace(/_/g, " ") }),
      h("div", { class: "pair" },
        h("div", { class: "bar alt", title: `before ${fmt.ms(q.before_ms)}` }, h("i", { style: `width:${(q.before_ms / maxMs) * 100}%` })),
        h("div", { class: "bar", title: `after ${fmt.ms(q.after_ms)}` }, h("i", { style: `width:${(q.after_ms / maxMs) * 100}%` }))),
      h("span", { class: `speed ${sp < 1 ? "slower" : ""}`, text: `${sp.toFixed(2)}×` }));
  }));

  view.append(
    h("div", { class: "kpis" },
      kpi("Bulk ingestion", il ? fmt.int(il.end_to_end_rows_per_second) : "—", "rows/s", il ? `${fmt.int(il.total_rows)} rows queryable in ${fmt.secs(il.seconds_until_queryable_in_lakehouse)}` : ""),
      kpi("Freshness p50", fr ? fmt.num(fr.end_to_end_seconds.p50, 1) : "—", "s", fr ? `p95 ${fmt.num(fr.end_to_end_seconds.p95, 1)} s under 100 ops/s` : ""),
      kpi("Query suite", `${(sumB / sumA).toFixed(2)}×`, "faster", `${fmt.ms(sumB)} → ${fmt.ms(sumA)} total, ${b.runs_per_query} runs each`),
      kpi("Delete files", `${fmt.int(delB)} → ${fmt.int(delA)}`, null, "merge-on-read deletes compacted away"),
      kpi("Objects in MinIO", objB ? `−${Math.round((1 - objA / objB) * 100)}` : "—", objB ? "%" : null, `${fmt.int(objB)} → ${fmt.int(objA)} objects, bytes unchanged`)),
    h("div", { class: "split-r" },
      card("Query latency before and after optimisation", "median wall time", h("div", { class: "stack" },
        h("div", { class: "legend" }, h("span", {}, h("i", { style: "background:var(--line-2)" }), "before"), h("span", {}, h("i", { style: "background:var(--accent-strong)" }), "after layout optimisation")),
        bars,
        h("p", { class: "muted", style: "margin:0;font-size:12.5px", text: "Q06 scans the whole table either way, so sorting and partitioning cannot help it." }))),
      card("Optimisation run", `${fmt.secs(b.optimization.wall_seconds)} wall, Airflow DAG`, dataTable([
        { label: "Task", mono: true, key: "task" },
        { label: "Duration", num: true, render: (t) => fmt.secs(t.duration_s) },
      ], Object.entries(b.optimization.tasks || {}).map(([task, v]) => ({ task, ...v }))))),
    card("Files per table", b.name, dataTable([
      { label: "Table", render: (t) => `shop.${t.table}` },
      { label: "Data files", num: true, render: (t) => `${fmt.int(t.b.data_files)} → ${fmt.int(t.a.data_files)}` },
      { label: "Delete files", num: true, render: (t) => `${fmt.int(t.b.delete_files)} → ${fmt.int(t.a.delete_files)}` },
      { label: "Avg data file", num: true, render: (t) => `${fmt.int(t.b.avg_data_file_kb)} → ${fmt.int(t.a.avg_data_file_kb)} KB` },
      { label: "Snapshots", num: true, render: (t) => `${fmt.int(t.b.snapshots)} → ${fmt.int(t.a.snapshots)}` },
    ], Object.keys(b.files.before).map((t) => ({ table: t, b: b.files.before[t], a: b.files.after[t] || {} })))));
}

// ------------------------------------------------------------------ boot
document.getElementById("workload-pill").addEventListener("click", () => { location.hash = "#/overview"; });
document.getElementById("theme-toggle").addEventListener("click", () => {
  const next = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
  document.documentElement.dataset.theme = next;
  try { localStorage.setItem("floe-theme", next); } catch (e) { /* no storage */ }
});
window.addEventListener("hashchange", navigate);
document.addEventListener("visibilitychange", () => {
  if (document.hidden) return;
  refreshTopbar();
  if (ctx) ctx.refresh();
});
if (!location.hash) history.replaceState(null, "", "#/overview");
navigate();
refreshTopbar();
setInterval(() => { if (!document.hidden) refreshTopbar(); }, 5000);
