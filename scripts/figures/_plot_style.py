"""
Shared matplotlib style for all paper figures.

IEEE / eScience standard:
  - Serif font (Times-like, matching IEEE paper typography)
  - Single-column width: 3.45 in  (88 mm)
  - 8 pt tick labels, 9 pt axis labels
  - No figure titles (captions go in LaTeX)
  - Top/right spines removed
  - DPI 300 for PNG output
"""

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

COL_WIDTH = 3.45  # single-column inches
DPI = 300

C_PRIMARY = "#2171b5"
C_SECONDARY = "#6baed6"
C_ACCENT = "#b30000"

BLUE_CMAP = "Blues"

RC = {
    "font.family": "serif",
    "font.serif": [
        "Times New Roman",
        "Times",
        "Nimbus Roman",
        "TeX Gyre Termes",
        "STIX Two Text",
        "STIXGeneral",
        "DejaVu Serif",
    ],
    "font.size": 9,
    "axes.titlesize": 9.5,
    "axes.labelsize": 9,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
    "legend.fontsize": 8,
    "legend.frameon": False,
    "axes.linewidth": 0.6,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "xtick.major.width": 0.5,
    "ytick.major.width": 0.5,
    "xtick.direction": "out",
    "ytick.direction": "out",
    "grid.linewidth": 0.4,
    "grid.alpha": 0.3,
    "grid.linestyle": "--",
    "lines.linewidth": 1.4,
    "lines.markersize": 5,
    "mathtext.fontset": "stix",
    "mathtext.default": "regular",
    "axes.unicode_minus": False,
    "pdf.fonttype": 42,
    "ps.fonttype": 42,
    "svg.fonttype": "none",
    "savefig.dpi": DPI,
    "savefig.bbox": "tight",
    "savefig.pad_inches": 0.02,
}


def apply_style():
    plt.rcParams.update(RC)


def save(fig, stem, fig_dir):
    from pathlib import Path
    fig_dir = Path(fig_dir)
    png = fig_dir / f"{stem}.png"
    fig.savefig(png, format="png")
    plt.close(fig)
    print(f"output: {png}")
    return png
