#!/usr/bin/env python3
"""
tab_08_scaling.py -- emit Tab 8: workload-size scaling (verdict stability).

Workload-SIZE sweep on REAL data: every diagnostic is re-run at
10k / 50k / 200k / 500k / 1M rows from each domain's frozen fixture (no
synthesis; RUNBOOK Step 7). Most diagnostics use prefixes; D4 uses a
deterministic uniform selection so every requested size contains the seeded
lateness fault. This table reports, per size, how many (engine, domain,
diagnostic) cells keep their verdict -- i.e. whether the design-space map is
stable as the workload grows.

Missing promoted evidence is a hard failure. The generator never writes a placeholder or invented value.

Inputs  : results/scaling/*/summary.json  (required row fields: engine, size, stable)
Outputs : paper/.../tables/tab_08_scaling.tex
Runtime : instant
Run as  : python scripts/figures/tab_08_scaling.py
"""
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from figures import load_module  # noqa: E402

_io = load_module("_io")
load_cells = _io.load_cells
write_table = _io.write_table

RUN_HINT = "./reproduce_all.sh scaling <engine>"
SCALING_SIZES = _io.SCALING_SIZES
SCALING_DIAGNOSTICS = _io.SCALING_DIAGNOSTICS


def _aggregate_by_size(rows: List[Dict[str, Any]]):
    """Group per-cell scaling rows into one summary line per workload size."""
    by_size: Dict[int, Dict[str, Any]] = defaultdict(
        lambda: {"cells": 0, "stable": 0, "stable_known": 0})
    for r in rows:
        size = r.get("size")
        if size is None:
            continue
        agg = by_size[int(size)]
        agg["cells"] += 1
        if r.get("stable") is not None:
            agg["stable_known"] += 1
            if r.get("stable"):
                agg["stable"] += 1
    return by_size


#: The design point every other size is compared with (the primary grid).
DESIGN_SIZE = 50_000
DOMAIN_LABEL = {
    "datacenter_drives": "drives", "environment_airquality": "air quality",
    "finance_markets": "finance", "sports_football": "football",
    "transportation_taxi": "taxi",
}
STATUS_WORD = {
    "equivalent": "equivalent", "divergence": "divergent",
    "known_divergence": "divergent", "unsupported": "unsupported",
}


def _english(items: List[str]) -> str:
    items = list(items)
    if len(items) <= 1:
        return "".join(items)
    return ", ".join(items[:-1]) + " and " + items[-1]


def _unstable_txt(rows: List[Dict[str, Any]]) -> str:
    """Name every cell whose verdict differs from the design point.

    A count such as 242/245 invites the question of which three cells moved,
    and the result prose names only the engines and diagnostics. The caption
    names each cell, derived from the cohort, so it cannot drift from the
    table.
    """
    design = {(r.get("engine"), r.get("domain"), r.get("diagnostic")):
              str(r.get("status")) for r in rows
              if r.get("size") is not None and int(r["size"]) == DESIGN_SIZE}
    moved: Dict[int, Dict[tuple, Dict[tuple, List[str]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list)))
    for r in rows:
        if r.get("stable") is not False or r.get("size") is None:
            continue
        key = (r.get("engine"), r.get("domain"), r.get("diagnostic"))
        words = (STATUS_WORD.get(str(r.get("status")), str(r.get("status"))),
                 STATUS_WORD.get(design.get(key, ""), design.get(key, "unknown")))
        moved[int(r["size"])][words][(r.get("engine"), r.get("diagnostic"))].append(
            DOMAIN_LABEL.get(str(r.get("domain")), str(r.get("domain"))))
    sentences = []
    for size in sorted(moved):
        parts, count = [], 0
        for (here, there), cells in sorted(moved[size].items()):
            names = [f"{engine} {diag} on {_english(sorted(domains))}"
                     for (engine, diag), domains in sorted(cells.items())]
            count += sum(len(d) for d in cells.values())
            parts.append(f"{_english(names)} {{verb}} {here} where the design "
                         f"point is {there}")
        verb = "is" if count == 1 else "are"
        sentences.append(f"At {size:,} rows, "
                         + "; ".join(x.replace("{verb}", verb) for x in parts) + ".")
    return " ".join(sentences)


def _render(rows: List[Dict[str, Any]]) -> str:
    # Validate against the universe the cohort DECLARES, not the full engine
    # list. A cohort that declares an exclusion would otherwise have its absent
    # cells reported as missing evidence and refuse to render a complete,
    # correctly narrowed cohort. results_narrative reads the declared set the
    # same way.
    declared = _io.promoted_engines("scaling") or None
    rows = _io.validate_scaling_cells(rows, engines=declared)
    by_size = _aggregate_by_size(rows)
    engines = sorted({r.get("engine") for r in rows if r.get("engine")})
    # State the grid the Cells column counts, derived from the cohort rather
    # than written by hand, so a narrowed sweep cannot leave the caption
    # claiming a shape the table no longer has.
    domains = sorted({r.get("domain") for r in rows if r.get("domain")})
    diagnostics = sorted({r.get("diagnostic") for r in rows if r.get("diagnostic")})
    grid_txt = (f"Each size scores {len(engines) * len(domains) * len(diagnostics)} "
                f"cells, {len(engines)} engines $\\times$ {len(domains)} domains "
                f"$\\times$ {len(diagnostics)} portable diagnostics. ")
    unstable_txt = _unstable_txt(rows)
    lines = [
        "% Auto-generated by scripts/figures/tab_08_scaling.py — do not edit by hand.",
        "\\begin{table}[!tb]",
        "\\centering",
        "\\caption{Verdict stability across workload sizes. " + grid_txt +
        "A cell is stable when its verdict matches the $5{\\times}10^4$ "
        "design point. " + (unstable_txt + " " if unstable_txt else "") +
        "Lanes run concurrently and are not load-isolated, so "
        "this is verdict stability, not throughput.}",
        "\\label{tab:scaling}",
        "\\small",
        "\\begin{tabular}{@{}rrr@{}}",
        "\\toprule",
        "Rows selected & Cells & Verdict-stable \\\\",
        "\\midrule",
    ]
    for size in sorted(by_size):
        agg = by_size[size]
        cells = agg["cells"]
        stable_txt = (f"{agg['stable']}/{agg['stable_known']}"
                      if agg["stable_known"] else "--")
        lines.append(f"{size:,} & {cells} & {stable_txt} \\\\")
    lines += [
        "\\bottomrule",
        "\\end{tabular}",
        "\\end{table}",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    rows = load_cells("scaling")
    if not rows:
        print("FAIL: no promoted contract-v2 scaling evidence; refusing to write Tab 8.")
        print(f"  Populate: {RUN_HINT}")
        return 1
    try:
        table = _render(rows)
    except ValueError as exc:
        print(f"FAIL: cannot generate Table 8: {exc}")
        return 1
    write_table(table, "tab_08_scaling")
    print(f"PASS: wrote Tab 8 ({len(SCALING_SIZES)} size point(s))")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
