#!/usr/bin/env python3
"""
fig_02_design_space.py -- emit Fig 4: design-space dot matrix (engines × diagnostics).

Builds a per-domain verdict matrix from promoted design_space results. Each
scored cell shows one dot per domain in a fixed alphabetical order, so the
figure preserves WHICH domains diverge rather than only how many. A divergence
that follows one domain across engines reads as a data-specific case; one that
follows an engine across domains reads as an engine defect. That distinction is
the paper's own attribution argument, and the fraction alone destroyed it.

Inputs  : results/design_space/*/summary.json
Outputs : paper/figures/fig_02_design_space.{pdf,png}
Runtime : instant
Run as  : python scripts/figures/fig_02_design_space.py
"""
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Tuple

import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from figures import load_module  # noqa: E402

_io = load_module("_io")
_style = load_module("_style")
DIAGNOSTIC_ORDER = _io.DIAGNOSTIC_ORDER
figure_output_dir = _io.figure_output_dir
load_cells = _io.load_cells
sort_diagnostics = _io.sort_diagnostics
matrix_diagnostics = _io.matrix_diagnostics
sort_engines = _io.sort_engines
level_bands = _io.level_bands
LEVEL_LABEL = _io.LEVEL_LABEL
COL_WIDTH = _style.COL_WIDTH
apply_style = _style.apply_style
save = _style.save
LF = _style.LIGHT_FILL
LE = _style.LIGHT_EDGE

#: Fixed left-to-right dot order inside every cell; the caption states it.
DOMAIN_ORDER = sorted(_io.CONTRACT_DOMAINS)
#: Names for the dot key under the legend. A reviewer read the caption's order
#: as "five unlabeled dots"; the key puts the order in the figure itself.
DOMAIN_KEY = {
    "datacenter_drives": "drives", "environment_airquality": "air quality",
    "finance_markets": "finance", "sports_football": "football",
    "transportation_taxi": "taxi",
}

#: Dot colors are the Figure 1 hues (LIGHT_EDGE red and green) darkened one
#: step, because a 4pt marker drawn in the pale border tones disappears in
#: print. Same hue family, one step more ink; the box fill and border keep
#: the exact Figure 1 palette.
DOT_GREEN = "#4E9459"
DOT_RED = "#C0706E"
DOT_GRAY = LE["gray"]


def _status(row: dict) -> str:
    return _io.presentation_status(row)


def _matrix(rows: List[dict]) -> Tuple[List[str], List[str], np.ndarray, Dict[Tuple[int, int], str]]:
    """Pass-rate matrix plus per-cell annotations.

    Only domains that actually produced a verdict enter the pass rate. Domains
    the engine cannot express ("unsupported") or that never reached a comparison
    ("blocked") are shown as their own label rather than folded into a rate.
    """
    rows = _io.validate_design_space_cells(rows)
    by_cell: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    engines: Set[str] = set()
    diags: Set[str] = set()
    for row in rows:
        engine = row.get("engine")
        diag = row.get("diagnostic")
        if not engine or not diag:
            continue
        engines.add(engine)
        diags.add(diag)
        by_cell[(engine, diag)].append(_status(row))

    engine_list = sort_engines(engines)
    diag_list = matrix_diagnostics(diags)
    # Cell vocabulary, kept identical in wording to the paper's text and
    # free of em dashes and slashes per the prose rule atop paper/*/main.tex:
    #   "n.a."  an explicit D5 capability cell says lifecycle is not applicable
    #   "n.e."  every run reported unsupported; the engine cannot express the view
    #   "err"   every run was blocked by an adapter or engine error; no verdict
    #   "*"     scored, but the verdict is not stable across repeats
    mat = np.full((len(engine_list), len(diag_list)), np.nan)
    notes: Dict[Tuple[int, int], str] = {}
    for i, engine in enumerate(engine_list):
        for j, diag in enumerate(diag_list):
            statuses = by_cell.get((engine, diag))
            if not statuses:
                raise ValueError(f"missing aggregate cell for {engine}/{diag}")
            scored = [s for s in statuses if s in ("pass", "fail", "repeat-unstable")]
            if not scored:
                if all(s == "not_applicable" for s in statuses):
                    notes[(i, j)] = "n.a."
                else:
                    notes[(i, j)] = (
                        "n.e." if all(s == "unsupported" for s in statuses) else "err")
                continue
            mat[i, j] = sum(1 for s in scored if s == "pass") / len(scored)
            if any(s == "repeat-unstable" for s in scored):
                notes[(i, j)] = "repeat-unstable"
    return engine_list, diag_list, mat, notes


def _domain_grid(rows: List[dict]) -> Dict[Tuple[str, str], Dict[str, str]]:
    """Per-cell domain -> presentation status, for the dots.

    Assumes `rows` already passed validate_design_space_cells via _matrix, so
    every (engine, diagnostic) holds exactly one row per contract domain.
    """
    grid: Dict[Tuple[str, str], Dict[str, str]] = defaultdict(dict)
    for row in rows:
        grid[(str(row.get("engine")), str(row.get("diagnostic")))][
            str(row.get("domain"))] = _status(row)
    return grid


def render(rows: List[dict], out_dir) -> int:
    """Draw the dot matrix from validated design-space rows."""
    try:
        engine_list, diag_list, mat, notes = _matrix(rows)
    except ValueError as exc:
        print(f"FAIL: cannot generate Fig 2: {exc}")
        return 1
    if mat.size == 0 or np.all(np.isnan(mat)):
        print("FAIL: design_space summaries lack engine/diagnostic pass data.")
        return 1
    grid = _domain_grid(rows)

    apply_style()
    from matplotlib.patches import FancyBboxPatch

    fig, ax = plt.subplots(figsize=(COL_WIDTH, COL_WIDTH * 0.85))
    ax.set_xlim(-0.5, len(diag_list) - 0.5)
    # Inverted y keeps the reference row on top, as the heatmap had; the extra
    # headroom above row 0 holds the claim-level band labels.
    ax.set_ylim(len(engine_list) - 0.5, -1.15)
    ax.set_xticks(range(len(diag_list)))
    ax.set_xticklabels(diag_list)
    ax.set_yticks(range(len(engine_list)))
    ax.set_yticklabels(engine_list)
    ax.set_xlabel("Diagnostic")
    ax.set_ylabel("Engine")
    ax.tick_params(length=0)
    for spine in ax.spines.values():
        spine.set_visible(False)

    inset = 0.07
    box_kw = dict(boxstyle="round,pad=0.0,rounding_size=0.08", linewidth=1.1,
                  zorder=2, mutation_aspect=0.8)
    for i, engine in enumerate(engine_list):
        for j, diag in enumerate(diag_list):
            origin = (j - 0.5 + inset, i - 0.5 + inset)
            size = 1 - 2 * inset
            if np.isnan(mat[i, j]):
                # Capability state: a neutral cell that carries a word, not a
                # verdict, exactly as Tab 6 prints it.
                ax.add_patch(FancyBboxPatch(
                    origin, size, size, edgecolor=LE["white"],
                    facecolor="#EFEFEF", **box_kw))
                ax.text(j, i, notes.get((i, j), "n.a."), ha="center",
                        va="center", fontsize=7, color="#666666",
                        style="italic")
                continue
            statuses = grid[(engine, diag)]
            clean = all(statuses.get(d) == "pass" for d in DOMAIN_ORDER)
            # Box fill and border use the exact Figure 1 palette: the pale
            # green box is the right hour, the pale red box the wrong one.
            hue = "green" if clean else "red"
            ax.add_patch(FancyBboxPatch(
                origin, size, size, edgecolor=LE[hue], facecolor=LF[hue],
                **box_kw))
            for m, domain in enumerate(DOMAIN_ORDER):
                x = j - 0.32 + m * 0.16
                status = statuses.get(domain)
                dot = dict(marker="o", ls="None", ms=3.1, mew=0.7, zorder=3)
                if status == "pass":
                    ax.plot([x], [i], mfc=DOT_GREEN, mec=DOT_GREEN, **dot)
                elif status == "fail":
                    ax.plot([x], [i], mfc="white", mec=DOT_RED, **dot)
                elif status == "repeat-unstable":
                    # Half-filled: the identical-input repeats disagree, so
                    # the domain is neither a stable pass nor a stable fail.
                    ax.plot([x], [i], fillstyle="left", mfc=DOT_RED,
                            mfcalt="white", mec=DOT_RED, **dot)
                else:
                    # A domain without a verdict inside a scored cell:
                    # unsupported, blocked, or not applicable.
                    ax.plot([x], [i], mfc=LF["gray"], mec=DOT_GRAY, **dot)
            if notes.get((i, j)) == "repeat-unstable":
                # The asterisk convention is shared with Tab 6 and the caption.
                ax.text(j + 0.40, i - 0.30, "*", ha="center", va="center",
                        fontsize=9, color="#333333", zorder=4)

    # Columns are ordered by claim level and each band is separated and named,
    # because a portable verdict and a lifecycle verdict are independent claims
    # and a row of this matrix is not one engine-wide score.
    edge = 0.0
    bands = level_bands(diag_list)
    for k, (level, width) in enumerate(bands):
        if k:
            ax.axvline(edge - 0.5, color="#333333", linewidth=1.0)
        ax.text(edge + (width - 1) / 2.0, -0.78, LEVEL_LABEL[level],
                ha="center", va="bottom", fontsize=6, color="#333333")
        edge += width

    # The key names the dot states; shape alone separates them, so the figure
    # reads without colour and in greyscale print.
    proxies = [
        plt.Line2D([], [], marker="o", ls="None", ms=3.1, mew=0.7,
                   mfc=DOT_GREEN, mec=DOT_GREEN, label="equivalent"),
        plt.Line2D([], [], marker="o", ls="None", ms=3.1, mew=0.7,
                   mfc="white", mec=DOT_RED, label="divergent"),
        plt.Line2D([], [], marker="o", ls="None", ms=3.1, mew=0.7,
                   fillstyle="left", mfc=DOT_RED, mfcalt="white",
                   mec=DOT_RED, label="repeat-unstable"),
    ]
    # Below the x-axis label, so the key never collides with "Diagnostic".
    _style.legend(ax, handles=proxies, loc="upper center", ncol=3,
                  bbox_to_anchor=(0.5, -0.20), fontsize=6.5)
    ax.text(0.5, -0.285, "dots in each box, left to right: "
            + ", ".join(DOMAIN_KEY.get(d, d) for d in DOMAIN_ORDER),
            transform=ax.transAxes, ha="center", va="top", fontsize=6.5,
            color="#333333")

    save(fig, "fig_02_design_space", out_dir)
    print("PASS: wrote the design-space dot matrix")
    return 0


def main() -> int:
    rows = load_cells("design_space")
    if not rows:
        print("FAIL: no promoted results under results/design_space/")
        print("  Fig 4 needs promoted diagnostic runs.")
        print("  Promote runs first, e.g.:")
        print("    python scripts/results/promote_results.py runs/<run_id> --block design_space")
        return 1
    return render(rows, figure_output_dir())


if __name__ == "__main__":
    raise SystemExit(main())
