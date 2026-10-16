#!/usr/bin/env python3
"""Render benchmark JSONL (from `phhtp-bench run --json`) as an HTML report of
inline-SVG charts plus the full dataset table, with no dependencies.

    scripts/report.py bench/results/dataset/*.jsonl -o bench/report.html
    scripts/report.py ... --svg-dir bench/charts      # also standalone SVGs for the README

Charts follow a fixed method: color encodes the I/O model family (three
validated categorical hues), runtime identity is always a visible label, bars
are thin with rounded data-ends, hairline grid, hover tooltips, and every chart
has its table twin in the dataset section.
"""
import argparse
import html
import json
import os
import sys
from collections import OrderedDict, defaultdict

FAMILY = {
    "threads": "blocking", "pool": "blocking",
    "mio": "epoll", "tokio": "epoll", "tokio-st": "epoll",
    "uring": "io_uring", "uring-async": "io_uring",
    "monoio": "io_uring", "compio": "io_uring", "glommio": "io_uring", "tokio-uring": "io_uring",
}
OURS = {"threads", "pool", "mio", "tokio", "tokio-st", "uring", "uring-async"}
FROM_SCRATCH = {"uring", "uring-async"}
FAMILY_LABEL = OrderedDict([
    ("io_uring", "io_uring (completion)"),
    ("epoll", "epoll / readiness"),
    ("blocking", "blocking threads"),
])
# Validated categorical slots 1-3 (light / dark), see dataviz palette.
SLOT = {"io_uring": ("#2a78d6", "#3987e5"), "epoll": ("#eb6834", "#d95926"), "blocking": ("#1baf7a", "#199e70")}
# Ordinal blue ramp for worker counts (steps 250 / 450 / 650).
ORDINAL = ["#86b6ef", "#2a78d6", "#104281"]
ORDINAL_DARK = ["#6da7ec", "#3987e5", "#184f95"]

RUNTIME_ORDER = ["uring-async", "uring", "monoio", "compio", "glommio", "tokio-uring", "tokio", "tokio-st", "mio", "threads", "pool"]


def endpoint_of(row):
    url = row["url"]
    path = "/" + url.split("/", 3)[3] if url.count("/") >= 3 else "/"
    return f'{row["method"]} {path}'


def k(v):
    return f"{v/1000:.0f}k" if v >= 10000 else (f"{v/1000:.1f}k" if v >= 1000 else f"{v:.0f}")


def ms(us):
    return f"{us/1000:.2f} ms" if us < 10000 else f"{us/1000:.0f} ms"


def esc(s):
    return html.escape(str(s), quote=True)


def load(paths):
    rows = []
    for p in paths:
        with open(p) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                r["_file"] = os.path.basename(p)
                r.setdefault("note", "")
                r.setdefault("reps", 1)
                rows.append(r)
    return rows


# ------------------------------------------------------------------ SVG pieces

def bar_path(x, y, w, h, r=4):
    """Horizontal bar: square at the baseline (left), 4px rounded data-end (right)."""
    if w <= r:
        return f"M{x},{y} h{w} v{h} h{-w} z"
    return (f"M{x},{y} h{w-r} a{r},{r} 0 0 1 {r},{r} v{h-2*r} a{r},{r} 0 0 1 {-r},{r} h{-(w-r)} z")


def nice_max(v):
    if v <= 0:
        return 1
    import math
    mag = 10 ** math.floor(math.log10(v))
    for m in (1, 2, 2.5, 5, 10):
        if v <= m * mag:
            return m * mag
    return 10 * mag


def hbar_chart(title, subtitle, items, value_fmt, unit, width=560, sorted_desc=True):
    """items: list of dict(label, value, family, ours, tip). Sorted best-first. Returns svg string."""
    items = sorted(items, key=lambda d: -d["value"] if sorted_desc else d["value"])
    n = len(items)
    row_h, bar_h = 28, 18
    left, right, top, bottom = 118, 70, 44, 26
    plot_w = width - left - right
    height = top + n * row_h + bottom
    vmax = nice_max(max((d["value"] for d in items), default=1))
    ticks = [vmax * i / 4 for i in range(5)]
    out = [f'<svg class="viz" viewBox="0 0 {width} {height}" width="100%" style="max-width:{width}px" role="img" aria-label="{esc(title)}">']
    out.append(f'<text x="0" y="16" class="t-title">{esc(title)}</text>')
    out.append(f'<text x="0" y="32" class="t-sub">{esc(subtitle)} · {esc(unit)}</text>')
    for t in ticks:
        x = left + plot_w * (t / vmax)
        out.append(f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{top + n*row_h}" class="grid"/>')
        out.append(f'<text x="{x:.1f}" y="{top + n*row_h + 16}" class="t-tick" text-anchor="middle">{esc(value_fmt(t))}</text>')
    out.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + n*row_h}" class="axis"/>')
    for i, d in enumerate(items):
        y = top + i * row_h + (row_h - bar_h) / 2
        w = plot_w * (d["value"] / vmax)
        fam = d["family"]
        label = d["label"]
        mark = "◆ " if d.get("ours") else ""
        cls = f'bar fam-{fam}'
        out.append(f'<g class="row" data-tip="{esc(d["tip"])}">')
        out.append(f'<rect x="0" y="{top + i*row_h}" width="{width}" height="{row_h}" class="hit"/>')
        out.append(f'<text x="{left-8}" y="{y + bar_h/2 + 4}" class="t-label{" t-ours" if d.get("ours") else ""}" text-anchor="end">{esc(mark + label)}</text>')
        out.append(f'<path d="{bar_path(left, y, w, bar_h)}" class="{cls}"/>')
        out.append(f'<text x="{left + w + 6:.1f}" y="{y + bar_h/2 + 4}" class="t-value">{esc(value_fmt(d["value"]))}</text>')
        out.append('</g>')
    out.append('</svg>')
    return "\n".join(out)


def small_multiples_lines(title, subtitle, series, xcats, value_fmt, cols=4, cell_w=200, cell_h=130):
    """series: list of dict(label, family, ours, points: {xcat: value}). Shared y scale."""
    vmax = nice_max(max((v for s in series for v in s["points"].values()), default=1))
    rows = (len(series) + cols - 1) // cols
    top = 40
    width = cols * cell_w
    height = top + rows * cell_h + 10
    pad_l, pad_r, pad_t, pad_b = 40, 44, 22, 24
    out = [f'<svg class="viz" viewBox="0 0 {width} {height}" width="100%" style="max-width:{width}px" role="img" aria-label="{esc(title)}">']
    out.append(f'<text x="0" y="16" class="t-title">{esc(title)}</text>')
    out.append(f'<text x="0" y="32" class="t-sub">{esc(subtitle)}</text>')
    for idx, s in enumerate(series):
        cx = (idx % cols) * cell_w
        cy = top + (idx // cols) * cell_h
        pw = cell_w - pad_l - pad_r
        ph = cell_h - pad_t - pad_b
        x0, y0 = cx + pad_l, cy + pad_t
        mark = "◆ " if s.get("ours") else ""
        out.append(f'<text x="{x0}" y="{cy + 14}" class="t-label{" t-ours" if s.get("ours") else ""}">{esc(mark + s["label"])}</text>')
        for j in range(3):
            yy = y0 + ph - ph * j / 2
            out.append(f'<line x1="{x0}" y1="{yy:.1f}" x2="{x0 + pw}" y2="{yy:.1f}" class="grid"/>')
            out.append(f'<text x="{x0 - 4}" y="{yy + 3:.1f}" class="t-tick" text-anchor="end">{esc(value_fmt(vmax * j / 2))}</text>')
        pts = []
        for i, xc in enumerate(xcats):
            if xc not in s["points"]:
                continue
            x = x0 + pw * (i / (len(xcats) - 1)) if len(xcats) > 1 else x0
            y = y0 + ph - ph * (s["points"][xc] / vmax)
            pts.append((x, y, xc, s["points"][xc]))
        if pts:
            d = "M" + " L".join(f"{x:.1f},{y:.1f}" for x, y, _, _ in pts)
            out.append(f'<path d="{d}" class="line fam-{s["family"]}"/>')
            for x, y, xc, v in pts:
                tip = f'{s["label"]} · {xc} connections · {value_fmt(v)}'
                out.append(f'<g class="pt" data-tip="{esc(tip)}"><circle cx="{x:.1f}" cy="{y:.1f}" r="12" class="hit"/>'
                           f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" class="dot fam-{s["family"]}"/></g>')
            x, y, _, v = pts[-1]
            out.append(f'<text x="{x + 8:.1f}" y="{y + 4:.1f}" class="t-value">{esc(value_fmt(v))}</text>')
        for i, xc in enumerate(xcats):
            x = x0 + pw * (i / (len(xcats) - 1)) if len(xcats) > 1 else x0
            out.append(f'<text x="{x:.1f}" y="{y0 + ph + 14}" class="t-tick" text-anchor="middle">{esc(k(xc) if xc >= 10000 else xc)}</text>')
    out.append('</svg>')
    return "\n".join(out)


def grouped_columns(title, subtitle, groups, keys, value_fmt, width=720):
    """groups: list of dict(label, ours, values: {key: v}); keys ordinal (worker counts)."""
    vmax = nice_max(max((v for g in groups for v in g["values"].values()), default=1))
    left, right, top, bottom = 48, 10, 44, 34
    plot_w = width - left - right
    plot_h = 160
    height = top + plot_h + bottom + 18
    gw = plot_w / max(len(groups), 1)
    bw = min(18, (gw - 12) / len(keys))
    out = [f'<svg class="viz" viewBox="0 0 {width} {height}" width="100%" style="max-width:{width}px" role="img" aria-label="{esc(title)}">']
    out.append(f'<text x="0" y="16" class="t-title">{esc(title)}</text>')
    out.append(f'<text x="0" y="32" class="t-sub">{esc(subtitle)}</text>')
    for j in range(5):
        yy = top + plot_h - plot_h * j / 4
        out.append(f'<line x1="{left}" y1="{yy:.1f}" x2="{left + plot_w}" y2="{yy:.1f}" class="grid"/>')
        out.append(f'<text x="{left - 6}" y="{yy + 3:.1f}" class="t-tick" text-anchor="end">{esc(value_fmt(vmax * j / 4))}</text>')
    for gi, g in enumerate(groups):
        gx = left + gi * gw + (gw - bw * len(keys) - 2 * (len(keys) - 1)) / 2
        for ki, key in enumerate(keys):
            v = g["values"].get(key)
            if v is None:
                continue
            h = plot_h * (v / vmax)
            x = gx + ki * (bw + 2)
            y = top + plot_h - h
            tip = f'{g["label"]} · {key} worker{"s" if key != 1 else ""} · {value_fmt(v)}'
            out.append(f'<g class="row" data-tip="{esc(tip)}"><rect x="{x:.1f}" y="{top}" width="{bw:.1f}" height="{plot_h}" class="hit"/>')
            out.append(f'<path d="{col_path(x, y, bw, h)}" class="bar" style="fill:var(--ord-{ki})"/></g>')
        mark = "◆ " if g.get("ours") else ""
        out.append(f'<text x="{left + gi*gw + gw/2:.1f}" y="{top + plot_h + 16}" class="t-label{" t-ours" if g.get("ours") else ""}" text-anchor="middle">{esc(mark + g["label"])}</text>')
    lx = left
    for ki, key in enumerate(keys):
        out.append(f'<rect x="{lx}" y="{height - 14}" width="10" height="10" rx="2" style="fill:var(--ord-{ki})"/>')
        out.append(f'<text x="{lx + 14}" y="{height - 5}" class="t-tick">{key} worker{"s" if key != 1 else ""}</text>')
        lx += 80
    out.append('</svg>')
    return "\n".join(out)


def col_path(x, y, w, h, r=4):
    if h <= r:
        return f"M{x},{y} h{w} v{h} h{-w} z"
    return f"M{x},{y+r} a{r},{r} 0 0 1 {r},{-r} h{w-2*r} a{r},{r} 0 0 1 {r},{r} v{h-r} h{-w} z"


# ------------------------------------------------------------------ sections

def tip_for(r):
    parts = [r["label"], endpoint_of(r), f'{r["connections"]} conns', f'{k(r["rps"])} req/s',
             f'p50 {ms(r["p50_us"])}', f'p90 {ms(r["p90_us"])}', f'p99 {ms(r["p99_us"])}']
    if r.get("rss_mb"):
        parts.append(f'RSS {r["rss_mb"]} MB')
    if r.get("reps", 1) > 1:
        parts.append(f'median of {r["reps"]} ({k(r["rps_min"])}–{k(r["rps_max"])})')
    return " · ".join(parts)


def matrix_section(rows):
    rows = [r for r in rows if not r.get("note", "").startswith("sweep=")]
    if not rows:
        return ""
    by_ep = OrderedDict()
    for r in rows:
        by_ep.setdefault(endpoint_of(r), {})[r["label"]] = r  # last wins
    out = ['<section id="matrix"><h2>Endpoint matrix</h2>',
           '<p class="lead">Throughput and tail latency per endpoint, every runtime on the same handler code. '
           'Bars are sorted; color is the I/O model family, the name is the runtime, ◆ marks the runtimes built in this repository.</p>']
    for ep, by_rt in by_ep.items():
        items_rps, items_p99 = [], []
        for rt, r in by_rt.items():
            base = dict(label=rt, family=FAMILY.get(rt, "io_uring"), ours=rt in OURS, tip=tip_for(r))
            items_rps.append(dict(base, value=r["rps"]))
            items_p99.append(dict(base, value=r["p99_us"] / 1000))
        conns = next(iter(by_rt.values()))["connections"]
        out.append('<div class="pair">')
        out.append(hbar_chart(f"{ep} — throughput", f"{conns} keep-alive connections · higher is better", items_rps, k, "req/s"))
        out.append(hbar_chart(f"{ep} — p99 latency", f"{conns} connections · lower is better", items_p99, lambda v: f"{v:.1f}", "ms", sorted_desc=False))
        out.append('</div>')
    out.append('</section>')
    return "\n".join(out)


def conns_section(rows):
    rows = [r for r in rows if r.get("note") == "sweep=conns"]
    if not rows:
        return ""
    xcats = sorted({r["connections"] for r in rows})
    series = []
    pts = defaultdict(dict)
    for r in rows:
        pts[r["label"]][r["connections"]] = r["rps"]
    for rt in sorted(pts, key=lambda x: RUNTIME_ORDER.index(x) if x in RUNTIME_ORDER else 99):
        series.append(dict(label=rt, family=FAMILY.get(rt, "io_uring"), ours=rt in OURS, points=pts[rt]))
    out = ['<section id="conns"><h2>Connection sweep</h2>',
           '<p class="lead">Throughput as keep-alive connections grow (one request in flight per connection), '
           'server pinned to 2 cores. Shared y scale; one panel per runtime because eleven converging lines on one plot cannot be read.</p>']
    out.append(small_multiples_lines("GET / throughput vs. connections", "req/s · 2-core server · connects paced", series, xcats, k))
    # p99 panel
    series99 = []
    p99 = defaultdict(dict)
    for r in rows:
        p99[r["label"]][r["connections"]] = r["p99_us"] / 1000
    for rt in sorted(p99, key=lambda x: RUNTIME_ORDER.index(x) if x in RUNTIME_ORDER else 99):
        series99.append(dict(label=rt, family=FAMILY.get(rt, "io_uring"), ours=rt in OURS, points=p99[rt]))
    out.append(small_multiples_lines("GET / p99 latency vs. connections", "ms · lower is better", series99, xcats, lambda v: f"{v:.0f}"))
    out.append('</section>')
    return "\n".join(out)


def workers_section(rows):
    rows = [r for r in rows if r.get("note") == "sweep=workers"]
    if not rows:
        return ""
    keys = sorted({int(r["workers"]) for r in rows if str(r["workers"]).isdigit()})
    vals = defaultdict(dict)
    for r in rows:
        if str(r["workers"]).isdigit():
            vals[r["label"]][int(r["workers"])] = r["rps"]
    groups = [dict(label=rt, ours=rt in OURS, values=vals[rt])
              for rt in sorted(vals, key=lambda x: RUNTIME_ORDER.index(x) if x in RUNTIME_ORDER else 99)]
    conns = rows[0]["connections"]
    out = ['<section id="workers"><h2>Worker sweep</h2>',
           f'<p class="lead">Throughput by worker count at {conns} connections on a 2-core server. '
           'One worker is the 1-vCPU droplet case; four workers on two cores is oversubscription.</p>']
    out.append(grouped_columns("GET / throughput by worker count", f"{conns} connections · 2-core server", groups, keys, k))
    out.append('</section>')
    return "\n".join(out)


def dataset_section(rows, files):
    cols = [("label", "runtime"), ("endpoint", "endpoint"), ("connections", "conns"), ("workers", "workers"),
            ("rps", "req/s"), ("p50_us", "p50"), ("p90_us", "p90"), ("p99_us", "p99"), ("max_us", "max"),
            ("errors", "errors"), ("rss_mb", "RSS MB"), ("threads", "threads"), ("reps", "reps"), ("note", "note"), ("_file", "file")]
    out = ['<section id="dataset"><h2>Dataset</h2>',
           '<p class="lead">Every row behind the charts, as recorded by <code>phhtp-bench --json</code>. Click a header to sort. Source files: '
           + ", ".join(f"<code>{esc(f)}</code>" for f in files) + '.</p>',
           '<div class="tablewrap"><table id="data"><thead><tr>' + "".join(f'<th data-k="{i}">{esc(h)}</th>' for i, (_, h) in enumerate(cols)) + '</tr></thead><tbody>']
    for r in rows:
        r = dict(r, endpoint=endpoint_of(r))
        cells = []
        for key, _ in cols:
            v = r.get(key, "")
            if key == "rps":
                cells.append(f'<td data-v="{v}">{v:.0f}</td>')
            elif key.endswith("_us"):
                cells.append(f'<td data-v="{v}">{ms(v) if v != "" else ""}</td>')
            else:
                cells.append(f'<td data-v="{esc(v)}">{esc(v)}</td>')
        out.append("<tr>" + "".join(cells) + "</tr>")
    out.append('</tbody></table></div></section>')
    return "\n".join(out)


CSS = """
:root { color-scheme: light; --surface:#fcfcfb; --surface-2:#f3f2ef; --text:#0b0b0b; --text-2:#52514e; --muted:#8a8984; --grid:#e6e5e1;
  --io_uring:#2a78d6; --epoll:#eb6834; --blocking:#1baf7a; --ord-0:#86b6ef; --ord-1:#2a78d6; --ord-2:#104281; }
@media (prefers-color-scheme: dark) { :root:where(:not([data-theme="light"])) { color-scheme: dark; --surface:#1a1a19; --surface-2:#242422; --text:#f0efec; --text-2:#c3c2b7; --muted:#8e8d86; --grid:#333331;
  --io_uring:#3987e5; --epoll:#d95926; --blocking:#199e70; --ord-0:#6da7ec; --ord-1:#3987e5; --ord-2:#184f95; } }
:root[data-theme="dark"] { color-scheme: dark; --surface:#1a1a19; --surface-2:#242422; --text:#f0efec; --text-2:#c3c2b7; --muted:#8e8d86; --grid:#333331;
  --io_uring:#3987e5; --epoll:#d95926; --blocking:#199e70; --ord-0:#6da7ec; --ord-1:#3987e5; --ord-2:#184f95; }
* { box-sizing: border-box; }
body { margin:0; background:var(--surface); color:var(--text); font: 15px/1.5 -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
main { max-width: 1180px; margin: 0 auto; padding: 24px 16px 64px; }
h1 { font-size: 26px; margin: 0 0 4px; } h2 { font-size: 20px; margin: 40px 0 6px; }
.lead { color: var(--text-2); margin: 0 0 14px; max-width: 80ch; }
.meta { color: var(--muted); font-size: 13px; }
.legend { display:flex; flex-wrap:wrap; gap: 14px 22px; margin: 14px 0 6px; font-size: 13px; color: var(--text-2); }
.legend span::before { content:""; display:inline-block; width:12px; height:12px; border-radius:3px; margin-right:6px; vertical-align:-1px; background: var(--c); }
.pair { display:grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 16px 0 28px; }
@media (max-width: 760px) { .pair { grid-template-columns: 1fr; } }
svg.viz { display:block; overflow: visible; }
.t-title { font-size: 15px; font-weight: 600; fill: var(--text); } .t-sub { font-size: 12px; fill: var(--text-2); }
.t-label { font-size: 12px; fill: var(--text); } .t-ours { font-weight: 600; }
.t-value { font-size: 11.5px; fill: var(--text-2); font-variant-numeric: tabular-nums; }
.t-tick { font-size: 11px; fill: var(--muted); font-variant-numeric: tabular-nums; }
.grid { stroke: var(--grid); stroke-width: 1; } .axis { stroke: var(--grid); stroke-width: 1; }
.bar.fam-io_uring { fill: var(--io_uring); } .bar.fam-epoll { fill: var(--epoll); } .bar.fam-blocking { fill: var(--blocking); }
.line { fill:none; stroke-width:2; stroke-linejoin:round; stroke-linecap:round; }
.line.fam-io_uring { stroke: var(--io_uring); } .line.fam-epoll { stroke: var(--epoll); } .line.fam-blocking { stroke: var(--blocking); }
.dot { stroke: var(--surface); stroke-width: 2; } .dot.fam-io_uring { fill: var(--io_uring); } .dot.fam-epoll { fill: var(--epoll); } .dot.fam-blocking { fill: var(--blocking); }
.hit { fill: transparent; } .row:hover .bar, .pt:hover .dot { filter: brightness(0.85); }
#tip { position: fixed; pointer-events:none; background: var(--surface-2); color: var(--text); border: 1px solid var(--grid); border-radius: 6px; padding: 6px 9px; font-size: 12px; max-width: 420px; display:none; z-index: 9; }
.tablewrap { overflow:auto; border: 1px solid var(--grid); border-radius: 6px; }
table { border-collapse: collapse; font-size: 12.5px; width: 100%; font-variant-numeric: tabular-nums; }
th, td { padding: 5px 8px; text-align: right; border-bottom: 1px solid var(--grid); white-space: nowrap; }
th { position: sticky; top: 0; background: var(--surface-2); cursor: pointer; text-align: right; }
td:first-child, th:first-child, td:nth-child(2), th:nth-child(2), td:nth-child(14), th:nth-child(14), td:nth-child(15), th:nth-child(15) { text-align: left; }
code { background: var(--surface-2); padding: 1px 4px; border-radius: 3px; font-size: 12px; }
"""

JS = """
const tip = document.getElementById('tip');
document.querySelectorAll('[data-tip]').forEach(el => {
  el.addEventListener('mousemove', e => { tip.textContent = el.dataset.tip; tip.style.display = 'block';
    tip.style.left = Math.min(e.clientX + 14, window.innerWidth - tip.offsetWidth - 8) + 'px'; tip.style.top = (e.clientY + 14) + 'px'; });
  el.addEventListener('mouseleave', () => tip.style.display = 'none');
});
document.querySelectorAll('#data th').forEach(th => th.addEventListener('click', () => {
  const k = +th.dataset.k, tb = document.querySelector('#data tbody'), rows = [...tb.rows];
  const asc = th.dataset.asc !== '1'; th.dataset.asc = asc ? '1' : '0';
  rows.sort((a, b) => { const x = a.cells[k].dataset.v, y = b.cells[k].dataset.v; const nx = parseFloat(x), ny = parseFloat(y);
    const c = (!isNaN(nx) && !isNaN(ny)) ? nx - ny : String(x).localeCompare(String(y)); return asc ? c : -c; });
  rows.forEach(r => tb.appendChild(r));
}));
"""


def build_html(rows, files, title):
    legend = "".join(f'<span style="--c:var(--{f})">{esc(lbl)}</span>' for f, lbl in FAMILY_LABEL.items())
    legend += '<span style="--c:transparent">◆ built in this repository (uring / uring-async are the from-scratch runtimes)</span>'
    reps = max((r.get("reps", 1) for r in rows), default=1)
    body = [f'<main><h1>{esc(title)}</h1>',
            f'<p class="meta">{len(rows)} measurements · {len(files)} source file{"s" if len(files)!=1 else ""} · '
            f'{"medians of " + str(reps) + " repetitions where noted" if reps > 1 else "single runs"} · '
            'identical runs on the shared benchmark box differ by up to ±15%, so treat smaller gaps as noise</p>',
            f'<div class="legend">{legend}</div>',
            matrix_section(rows), conns_section(rows), workers_section(rows), dataset_section(rows, files),
            '</main><div id="tip"></div>']
    return ("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
            f"<title>{esc(title)}</title><style>{CSS}</style></head><body>" + "\n".join(body) + f"<script>{JS}</script></body></html>")


def static_svg(svg):
    """Standalone SVG for README embedding: fixed light values, no CSS variables."""
    style = ("<style>" + CSS.split("* { box-sizing")[0].replace(":root {", ".x{") + "\n" +
             "\n".join(l for l in CSS.splitlines() if l.startswith((".t-", ".grid", ".axis", ".bar", ".line", ".dot", ".hit"))) +
             "</style>")
    fixed = {"var(--io_uring)": "#2a78d6", "var(--epoll)": "#eb6834", "var(--blocking)": "#1baf7a", "var(--text)": "#0b0b0b",
             "var(--text-2)": "#52514e", "var(--muted)": "#8a8984", "var(--grid)": "#e6e5e1", "var(--surface)": "#fcfcfb",
             "var(--ord-0)": "#86b6ef", "var(--ord-1)": "#2a78d6", "var(--ord-2)": "#104281"}
    for a, b in fixed.items():
        style = style.replace(a, b)
        svg = svg.replace(a, b)
    svg = svg.replace('width="100%"', 'width="100%" style="font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;background:#fcfcfb"', 1)
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + svg.replace("<svg ", "<svg xmlns=\"http://www.w3.org/2000/svg\" ", 1).replace(">", ">" + style, 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+")
    ap.add_argument("-o", "--out", default="bench/report.html")
    ap.add_argument("--title", default="phhtp runtime benchmarks")
    ap.add_argument("--svg-dir", help="also write standalone SVG charts here")
    a = ap.parse_args()
    rows = load(a.files)
    if not rows:
        sys.exit("no rows")
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    with open(a.out, "w") as f:
        f.write(build_html(rows, [os.path.basename(p) for p in a.files], a.title))
    print(f"wrote {a.out} ({len(rows)} rows)")
    if a.svg_dir:
        os.makedirs(a.svg_dir, exist_ok=True)
        mrows = [r for r in rows if not r.get("note", "").startswith("sweep=")]
        by_ep = OrderedDict()
        for r in mrows:
            by_ep.setdefault(endpoint_of(r), {})[r["label"]] = r
        for ep, by_rt in by_ep.items():
            items = [dict(label=rt, family=FAMILY.get(rt, "io_uring"), ours=rt in OURS, tip=tip_for(r), value=r["rps"]) for rt, r in by_rt.items()]
            name = ep.split()[1].strip("/").split("?")[0] or "root"
            conns = next(iter(by_rt.values()))["connections"]
            with open(os.path.join(a.svg_dir, f"matrix-{name}-rps.svg"), "w") as f:
                f.write(static_svg(hbar_chart(f"{ep} — throughput", f"{conns} keep-alive connections · higher is better", items, k, "req/s")))
            items99 = [dict(label=rt, family=FAMILY.get(rt, "io_uring"), ours=rt in OURS, tip=tip_for(r), value=r["p99_us"] / 1000) for rt, r in by_rt.items()]
            with open(os.path.join(a.svg_dir, f"matrix-{name}-p99.svg"), "w") as f:
                f.write(static_svg(hbar_chart(f"{ep} — p99 latency", f"{conns} connections · lower is better", items99, lambda v: f"{v:.1f}", "ms", sorted_desc=False)))
        crows = [r for r in rows if r.get("note") == "sweep=conns"]
        if crows:
            xcats = sorted({r["connections"] for r in crows})
            pts = defaultdict(dict)
            for r in crows:
                pts[r["label"]][r["connections"]] = r["rps"]
            series = [dict(label=rt, family=FAMILY.get(rt, "io_uring"), ours=rt in OURS, points=pts[rt])
                      for rt in sorted(pts, key=lambda x: RUNTIME_ORDER.index(x) if x in RUNTIME_ORDER else 99)]
            with open(os.path.join(a.svg_dir, "conns-rps.svg"), "w") as f:
                f.write(static_svg(small_multiples_lines("GET / throughput vs. connections", "req/s · 2-core server · connects paced", series, xcats, k)))
        print(f"wrote SVGs to {a.svg_dir}")


if __name__ == "__main__":
    main()
