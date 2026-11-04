"""
Shared style for Step 10 paper figures/tables.

Re-exports scripts/figures/_plot_style.py and adds a fixed engine palette +
ordering used consistently across every generator under scripts/figures/.
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt

# This directory lives on sys.path so sibling modules (_plot_style) resolve
# whether a generator is run as a script or imported as a module.
_FIGURES = Path(__file__).resolve().parent
if str(_FIGURES) not in sys.path:
    sys.path.insert(0, str(_FIGURES))

from _plot_style import (  # noqa: E402
    C_ACCENT,
    C_PRIMARY,
    C_SECONDARY,
    COL_WIDTH,
    DPI,
    RC,
    apply_style as _apply_style,
)

__all__ = [
    "C_ACCENT",
    "C_PRIMARY",
    "C_SECONDARY",
    "COL_WIDTH",
    "DPI",
    "RC",
    "ENGINE_COLORS",
    "ENGINE_EDGE",
    "ENGINE_ORDER",
    "INK",
    "LIGHT_FILL",
    "LIGHT_EDGE",
    "apply_style",
    "engine_color",
    "engine_edge",
    "legend",
    "save",
]

# Fixed ordering and colors — must match across all figures/tables.
ENGINE_ORDER = [
    "reference",
    "materialize",
    "risingwave",
    "ksqldb",
    "flink",
    "spark",
    "feldera",
]

# Dark slate ink for figure text (poster-style navy-charcoal, not pure black).
INK = "#33404D"

# "Lightmost" hue palette — a whisper of colour inside each box (~96% lightness),
# paired with a soft but defined border. This is the muted, poster-style look:
# subtle tint, never a saturated fill. FILL is the box interior; EDGE the outline
# (and, for line plots, the line colour — a medium tone so pale series stay
# visible). Keyed by hue name so figures can pick a role-appropriate colour.
LIGHT_FILL = {
    "blue":   "#EDF3FC",
    "green":  "#EDF7EF",
    "purple": "#F3EEFA",
    "yellow": "#FCF8E8",
    "orange": "#FCF1E7",
    "red":    "#FBEDED",   # plum / red
    "gray":   "#F3F3F3",
    "white":  "#FFFFFF",
}
LIGHT_EDGE = {
    "blue":   "#7FB0DE",
    "green":  "#86C08C",
    "purple": "#A78BD0",
    "yellow": "#D8C86A",
    "orange": "#DDA46A",
    "red":    "#D99A9A",
    "gray":   "#B0B0B0",
    "white":  "#C9C9C9",
}

# Per-engine assignment drawn from the lightmost palette. Fills are the palest
# tints (NOT colour-alone — always paired with the darker EDGE + the label).
_ENGINE_HUE = {
    "reference":   "gray",
    "materialize": "orange",
    "risingwave":  "green",
    "ksqldb":      "blue",
    "flink":       "red",
    "spark":       "yellow",
    "feldera":     "purple",
}
ENGINE_COLORS = {e: LIGHT_FILL[h] for e, h in _ENGINE_HUE.items()}
# reference line/edge slightly darker so the neutral baseline reads on a line plot.
ENGINE_EDGE = {e: LIGHT_EDGE[h] for e, h in _ENGINE_HUE.items()}
ENGINE_EDGE["reference"] = "#8A8A8A"


def engine_edge(name: str) -> str:
    """Return the canonical outline color for `name` (for pale-fill definition)."""
    return ENGINE_EDGE.get(name, "#969696")


def apply_style() -> None:
    """Apply IEEE/eScience rcParams plus embedded-font settings."""
    _apply_style()


def legend(ax, **kwargs):
    """Consistent legend across every figure: frameless, compact, tight handles.
    Pass loc=/ncol=/bbox_to_anchor= to place it; defaults suit an in-axes legend."""
    opts = dict(frameon=False, fontsize=8, handlelength=1.2, handletextpad=0.5,
                columnspacing=1.0, labelspacing=0.3, borderaxespad=0.2)
    opts.update(kwargs)
    return ax.legend(**opts)


def engine_color(name: str) -> str:
    """Return the canonical color for `name`, falling back to gray."""
    return ENGINE_COLORS.get(name, "#969696")


def save(fig, stem: str, fig_dir):
    """Write `fig_dir/<stem>.pdf` (vector, embedded fonts) and `<stem>.png`.

    EQUITABLE_FIGURE_FORMATS, a comma-separated subset of "pdf,png", limits the
    formats; the public artifact sets it to "png". The first written path is
    returned.
    """
    import os
    formats = [f.strip().lower() for f in
               os.environ.get("EQUITABLE_FIGURE_FORMATS", "pdf,png").split(",")
               if f.strip()]
    fig_dir = Path(fig_dir)
    fig_dir.mkdir(parents=True, exist_ok=True)
    written = []
    if "pdf" in formats:
        pdf = fig_dir / f"{stem}.pdf"
        fig.savefig(pdf, format="pdf", bbox_inches="tight", pad_inches=0.02)
        written.append(pdf)
    if "png" in formats:
        png = fig_dir / f"{stem}.png"
        fig.savefig(png, format="png", bbox_inches="tight", pad_inches=0.02,
                    dpi=DPI)
        written.append(png)
    plt.close(fig)
    for path in written:
        print(f"output: {path}")
    return written[0] if written else None
