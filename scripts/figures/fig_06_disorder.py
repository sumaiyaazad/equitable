#!/usr/bin/env python3
"""
fig_06_disorder.py -- how much disorder does an arrival-ordered latest tolerate?

One line per domain, at the finest grain the query family scores for it: the
fraction of (bucket, entity, metric) keys whose arrival-ordered latest row
differs from the event-time latest row, as the delivered log is reordered by up
to w distinct event-time steps (the D2 fixture's own perturbation). The curve is
computed from the oracle alone, so it is a property of the data and the
disorder; each engine that can express both formulations is overlaid as hollow
markers, which lie on the curve when its arrival-ordered view behaves exactly
as its documentation says. The dotted line marks the window D2 uses.

Colors are the documented default categorical slots 1-4, each line also carries
its own marker and legend entry, so identity never rests on color alone.

Inputs  : results/disorder_sweep (or --from-run for a preview)
Outputs : paper/figures/fig_06_disorder.{pdf,png}
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from figures import load_module  # noqa: E402

_io = load_module("_io")
_style = load_module("_style")

#: domain -> (grain shown, label, color, marker). Each domain at the finest
#: grain the family scores, since that is where a bucket holds the most
#: competing rows per key relative to its span.
#: Colours are the paper's own hues one step darker, chosen to avoid red and
#: green, which mean "wrong" and "correct" everywhere else in the paper. The
#: set passes the dataviz palette checks (lightness, chroma, colour-blind and
#: normal-vision separation, contrast); markers and the legend add a
#: second encoding.
SERIES = {
    "finance_markets": ("hourly", "finance, hourly", "#3F77B5", "o"),
    "environment_airquality": ("daily", "air quality, daily", "#C9782E", "s"),
    "sports_football": ("hourly", "football, hourly", "#8455B8", "^"),
    "transportation_taxi": ("hourly", "taxi, hourly", "#23906F", "D"),
}
MUTED = "#6B7682"
D2_WINDOW = "100"


def render(summary: Dict[str, Any], out_dir: Path) -> Path:
    _style.apply_style()
    rows: List[Dict[str, Any]] = summary.get("rows") or []
    windows = [str(w) for w in summary.get("windows") or []]
    xpos = {w: i for i, w in enumerate(windows)}
    fig, ax = plt.subplots(figsize=(_style.COL_WIDTH, 1.95))
    ends = []
    ringed = False
    for domain, (grain, label, color, marker) in SERIES.items():
        pts = sorted(
            ((xpos[str(r["window"])], r["wrong_fraction"]) for r in rows
             if r.get("kind") == "exposure" and r.get("domain") == domain
             and r.get("grain") == grain and str(r.get("window")) in xpos))
        if not pts:
            continue
        xs, ys = zip(*pts)
        ax.plot(xs, ys, color=color, marker=marker, markersize=4.2,
                markeredgecolor="white", markeredgewidth=0.6,
                linewidth=1.4, label=label, zorder=3, solid_capstyle="round")
        engine_pts = [(xpos[str(r["window"])], r["arrival_wrong_fraction"])
                      for r in rows if r.get("kind") == "engine"
                      and r.get("status") == "measured"
                      and r.get("domain") == domain and r.get("grain") == grain
                      and str(r.get("window")) in xpos]
        if engine_pts:
            # One thin dark ring per measured point, not one per engine: the
            # engines land on the same value, and four stacked coloured rings
            # read as a filled dot that hides the line's own marker.
            ex, ey = zip(*sorted(set(engine_pts)))
            ax.scatter(ex, ey, s=62, facecolors="none", edgecolors=_style.INK,
                       linewidths=0.65, zorder=4, clip_on=False)
            ringed = True
        ends.append([ys[-1], xs[-1], label, color, marker])
    if D2_WINDOW in xpos:
        ax.axvline(xpos[D2_WINDOW], color=_style.LIGHT_EDGE["gray"],
                   linestyle=(0, (2, 2)), linewidth=0.9, zorder=1)
        ax.text(xpos[D2_WINDOW], 1.02, "D2", ha="center", va="bottom",
                fontsize=7, color=MUTED, transform=ax.get_xaxis_transform())
    ax.set_xticks(range(len(windows)),
                  ["all" if w == "shuffle" else w for w in windows])
    ax.set_xlabel("maximum displacement, in distinct event-time steps",
                  color=_style.INK)
    ax.set_ylabel("keys with a wrong latest value", color=_style.INK)
    ax.set_ylim(-0.045, 1.0)
    ax.set_xlim(-0.3, len(windows) - 1 + 0.5)
    ax.yaxis.grid(True, color=_style.LIGHT_EDGE["gray"], alpha=0.5,
                  linestyle=(0, (2, 2)), linewidth=0.5)
    ax.set_axisbelow(True)
    ax.set_yticks([0, 0.25, 0.5, 0.75, 1.0],
                  ["0%", "25%", "50%", "75%", "100%"])
    ax.spines["left"].set_bounds(0, 1.0)
    # One legend, upper left: each domain as its line and marker, in the
    # order the lines end, then the ring key. Direct labels at the line ends
    # were tried first, but three lines end within a few percent of each
    # other, so a reader could not tell which label belonged to which line.
    from matplotlib.lines import Line2D
    handles = [Line2D([], [], color=color, marker=marker, markersize=4.2,
                      markeredgecolor="white", markeredgewidth=0.6,
                      linewidth=1.4, label=label)
               for _y, _x, label, color, marker in sorted(ends, reverse=True)]
    if ringed:
        handles.append(Line2D([], [], linestyle="none", marker="o",
                              markersize=7.2, markerfacecolor="none",
                              markeredgecolor=_style.INK, markeredgewidth=0.65,
                              label="measured on four engines"))
    leg = ax.legend(handles=handles, loc="upper left", frameon=False,
                    fontsize=6.8, handletextpad=0.5, handlelength=2.0,
                    labelspacing=0.35, borderaxespad=0.1,
                    bbox_to_anchor=(0.01, 1.0))
    for text in leg.get_texts():
        text.set_color(_style.INK)
    # Soft axes in the paper's ink, as in the design-space matrix.
    for side in ("left", "bottom"):
        ax.spines[side].set_color(_style.LIGHT_EDGE["gray"])
    ax.tick_params(colors=_style.INK, which="both", length=2.5)
    return _style.save(fig, "fig_06_disorder", out_dir)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-run", help="disorder_sweep summary.json (preview)")
    args = parser.parse_args()
    if args.from_run:
        summary = json.loads(Path(args.from_run).read_text())
    else:
        summaries = _io.load_summaries("disorder_sweep")
        if not summaries:
            print("FAIL: promote disorder_sweep before rendering Fig. disorder")
            return 1
        summary = summaries[-1]
    render(summary, _io.figure_output_dir())
    print("PASS: wrote the disorder figure")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
