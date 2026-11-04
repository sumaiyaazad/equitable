#!/usr/bin/env python3
"""
fig_05_attribution.py -- whose pass is it? (requirement R2, as one figure)

Three aligned panels share one engine axis, one bar per engine in each:

  a  the scored grid: the 15 order-sensitive cells per system under test
     (5 domains x D1, D2, D4), split by how the arrival-ordered formulation
     fared beside the scored repair;
  b  the generated query family under shuffled delivery;
  c  the same family under event-time delivery, where the two formulations
     coincide, so a correct engine must be attributable on every member.

Reading b against c is the argument: blue (repaired) in b turns green
(attributable) in c, except where an engine diverges on its own (red).

The reference backend is not drawn: it has no arrival-ordered view by
construction and is not a system under test, so the panel a bars sum to the 90
cells the abstract counts. An engine the family did not run is an empty row
marked "not run".

Style follows Figure 1 and the design-space matrix: pale fills, soft borders,
slate ink, dark round panel badges. The repaired tile is also hatched and
every tile carries its count, so the figure reads without colour.

Inputs  : results/design_space, results/query_family (or --from-run previews)
Outputs : paper/figures/fig_05_attribution.{pdf,png}
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from figures import load_module  # noqa: E402

_io = load_module("_io")
_style = load_module("_style")

#: state -> (label, hue, hatch). Hues come from Figure 1's own palette
#: (_style.LIGHT_FILL for the pale box interior, _style.LIGHT_EDGE for its
#: border): green is a correct table, red a wrong one, blue a pass the
#: benchmark supplied, gray a member the engine cannot express.
STATES = [
    ("attributable", "attributable", "green", ""),
    ("repaired", "repaired", "blue", "///"),
    ("repair_divergent", "repair divergent", "red", ""),
    ("unsupported", "cannot express", "gray", ""),
]
#: The grid records an engine with no arrival-ordered view under its own key.
ALIASES = {"idiomatic_unsupported": "unsupported"}
REFERENCE = "reference"
MUTED = "#5F6B78"
GRID_DIAGNOSTICS = ("D1", "D2", "D4")


def _fill(hue: str) -> str:
    return _style.LIGHT_FILL[hue]


def _edge(hue: str) -> str:
    return _style.LIGHT_EDGE[hue]


def _aspect(fig, ax) -> float:
    """Data-y per data-x at equal on-page length, so a rounded corner drawn in
    data coordinates stays round rather than squashed."""
    pos = ax.get_position()
    w_in = pos.width * fig.get_figwidth()
    h_in = pos.height * fig.get_figheight()
    x0, x1 = ax.get_xlim()
    y0, y1 = ax.get_ylim()
    return (abs(y1 - y0) / h_in) / (abs(x1 - x0) / w_in)


def _bar(ax, y, counts, states, height, aspect, span):
    """One stacked bar as a row of rounded tiles with small white gaps, the
    cell idiom of the design-space matrix. Pale fill, soft border (Figure 1's
    palette); the repaired tile carries a light hatch in its border colour."""
    left = 0.0
    gap = 0.006 * span
    radius = 0.018 * span
    for key, hue, hatch in states:
        n = counts.get(key, 0)
        if not n:
            continue
        ax.add_patch(FancyBboxPatch(
            (left + gap, y - height / 2), n - 2 * gap, height,
            boxstyle=f"round,pad=0,rounding_size={radius}",
            mutation_aspect=aspect, facecolor=_fill(hue), edgecolor=_edge(hue),
            linewidth=1.0, hatch=hatch, zorder=2))
        if n >= 2 or (n == 1 and height > 0.5):
            ax.text(left + n / 2, y, str(n), ha="center", va="center",
                    fontsize=7, color=_style.INK, zorder=3,
                    bbox=(dict(boxstyle="round,pad=0.15", facecolor=_fill(hue),
                               edgecolor="none") if hatch else None))
        left += n


def _panel_title(ax, letter, title):
    """Figure 1's dark round badge, then the panel title in ink."""
    ax.text(0.025, 1.12, letter, transform=ax.transAxes, ha="center",
            va="center", fontsize=7, color="white", zorder=5,
            bbox=dict(boxstyle="circle,pad=0.28", facecolor=_style.INK,
                      edgecolor="none"))
    ax.text(0.085, 1.12, title, transform=ax.transAxes, ha="left",
            va="center", fontsize=8.3, color=_style.INK)


def _soft_axes(ax):
    ax.tick_params(axis="y", length=0, colors=_style.INK)
    ax.tick_params(axis="x", length=2.5, colors=_style.INK)
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_color(_style.LIGHT_EDGE["gray"])
    ax.xaxis.grid(True, color=_style.LIGHT_EDGE["gray"], alpha=0.5,
                  linestyle=(0, (2, 2)), linewidth=0.5)
    ax.set_axisbelow(True)
    ax.xaxis.label.set_color(_style.INK)


def check_family(family: Dict[str, Any]) -> None:
    """Refuse a family cohort the promotion gate should have refused.

    Under event-time delivery the two formulations coincide, so a member with
    a gap on EVERY engine that expresses it measures the family's own
    construction, not an engine. The generator is the last line of defence."""
    sys.path.insert(0, str(ROOT))
    from src.diagnostics.shapes import control_verdict
    verdict = control_verdict(family.get("rows") or [])
    if not verdict["construction_clean"]:
        raise ValueError(
            "the ordered-delivery control shows a gap on every engine for "
            f"{verdict['construction_failures']}; the family measures its own "
            "construction there and cannot be plotted")


def _counts(rows) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for row in rows:
        key = ALIASES.get(row.get("attribution"), row.get("attribution"))
        out[key] = out.get(key, 0) + 1
    return out


def render(cells: List[Dict[str, Any]], family: Dict[str, Any], out_dir: Path) -> Path:
    check_family(family)
    _style.apply_style()
    engines = [e for e in _io.ENGINE_ORDER if e != REFERENCE]
    fam_rows = family.get("rows") or []
    ran = {r.get("engine") for r in fam_rows}
    members = max((sum(1 for r in fam_rows if r.get("engine") == e
                       and r.get("delivery") == "shuffled") for e in ran),
                  default=1)
    fig, axes = plt.subplots(
        1, 3, figsize=(7.16, 2.0), sharey=True,
        gridspec_kw={"width_ratios": [1, 1.12, 1.12], "wspace": 0.09})
    ax_a, ax_b, ax_c = axes
    ax_a.set_xlim(0, 15)
    ax_b.set_xlim(0, members)
    ax_c.set_xlim(0, members)
    ax_a.set_ylim(len(engines) - 0.45, -0.55)
    fig.subplots_adjust(left=0.105, right=0.992, top=0.86, bottom=0.30)
    states = [(k, hue, h) for k, _, hue, h in STATES]
    height = 0.62

    for i, engine in enumerate(engines):
        grid = [c for c in cells if c.get("engine") == engine
                and c.get("diagnostic") in GRID_DIAGNOSTICS]
        _bar(ax_a, i, _counts(grid), states, height, _aspect(fig, ax_a), 15)
        for ax, delivery in ((ax_b, "shuffled"), (ax_c, "ordered")):
            rows = [r for r in fam_rows if r.get("engine") == engine
                    and r.get("delivery") == delivery]
            if rows:
                _bar(ax, i, _counts(rows), states, height, _aspect(fig, ax),
                     members)
            else:
                ax.text(members / 2, i, "not run", ha="center", va="center",
                        fontsize=7, style="italic", color=MUTED, zorder=3)

    ax_a.set_yticks(range(len(engines)), engines)
    ax_a.set_xticks([0, 5, 10, 15])
    ax_a.set_xlabel("cells")
    ax_b.set_xlabel("family members")
    ax_c.set_xlabel("family members")
    _panel_title(ax_a, "a", "Scored grid: D1, D2, D4")
    _panel_title(ax_b, "b", "Query family, shuffled")
    _panel_title(ax_c, "c", "Query family, in order")
    for ax in axes:
        _soft_axes(ax)
    handles = [Patch(facecolor=_fill(hue), edgecolor=_edge(hue), hatch=h,
                     linewidth=1.0, label=label)
               for _, label, hue, h in STATES]
    leg = fig.legend(handles=handles, loc="lower center", ncol=4, frameon=False,
                     fontsize=7.5, bbox_to_anchor=(0.55, -0.02),
                     handlelength=1.6, columnspacing=1.8)
    for text in leg.get_texts():
        text.set_color(_style.INK)
    return _style.save(fig, "fig_05_attribution", out_dir)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-run", help="design-space collector summary.json (preview)")
    parser.add_argument("--family-run", help="query_family summary.json (preview)")
    args = parser.parse_args()
    cells = (json.loads(Path(args.from_run).read_text())["rows"] if args.from_run
             else _io.load_cells("design_space"))
    if args.family_run:
        family = json.loads(Path(args.family_run).read_text())
    else:
        summaries = _io.load_summaries("query_family")
        if not summaries:
            print("FAIL: promote query_family before rendering the attribution figure")
            return 1
        family = summaries[-1]
    render(cells, family, _io.figure_output_dir())
    print("PASS: wrote the attribution figure")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
