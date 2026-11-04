#!/usr/bin/env python3
"""
tab_01_related_work.py -- emit Tab 1: related-work feature comparison.

Hand-curated feature matrix positioning EquiTable against prior streaming
benchmarks, data-exploration benchmarks, and black-box correctness-checking
tools. One of the two structural HAND tables: its cells are
editorial judgements about published systems, not measured numbers, so it carries
no results dependency. The generator exists only so both drafts stay
byte-identical and re-emit on demand.

DESIGN RULE -- this table must not be an all-check victory column. An
all-$\\cmark$ EquiTable row against all-$\\times$ competitors reads as
self-serving and reviewers say so. Two axes therefore exist specifically because
prior work beats us on them:

  * "Distributed"   -- EquiTable is single-host by construction ($\\times$).
  * "Query breadth" -- EquiTable fixes one grouped-aggregate contract, where
                       NEXMark / Linear Road / DEBS span many query shapes
                       ($\\sim$ for us).

Keep it that way. If a future axis is added, check whether some competitor
should win it before adding it.

Symbols (portable across acmart + IEEEtran, no extra packages):
  $\\cmark$ full support   $\\sim$ partial / adjacent-layer   $\\times$ not evaluated in the cited work

Inputs  : none (curated below; \\cite keys must exist in paper/shared/ref.bib)
Outputs : paper/.../tables/tab_01_related_work.tex
Runtime : instant
Run as  : python scripts/figures/tab_01_related_work.py

Missing \\cite keys are reported as a loud WARNING (not a hard failure) so the
paper still renders during a benchmark run; fix ref.bib before submitting.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from figures import load_module  # noqa: E402

_io = load_module("_io")
write_table = _io.write_table

FULL = "$\\cmark$"
PART = "$\\sim$"
NONE = "$\\times$"

# Column axes (short headers keep the table* within a two-column page).
AXES = [
    "Real data",      # domain realism: real event logs vs synthetic generators
    "R1 answer",      # independent recomputation, not the engine's own report
    "R2 attribution", # a pass separated from the benchmark-supplied repair
    "R3 capability",  # an inexpressible formulation reported, never scored
    "Distributed",    # multi-node execution is part of the evaluated setup
    "Query breadth",  # many query shapes vs one fixed contract
    "Dup (D1)",       # source-identity duplicate suppression
    "Order (D2)",     # arrival-order invariance
    "Det. (D3)",      # deterministic floating-point reduction
    "Late (D4)",      # event-time lateness conformance
    "Recov. (D5)",    # post-recovery state equivalence
    "Top-$k$ (D6)",   # non-monotonic top-k maintenance
    "Retr. (D7)",     # retraction-consistent summarization
    "Wind. (D8)",     # overlapping event-time (hopping) window maintenance
]

# One row per prior system + EquiTable. cite = ref.bib key.
# Cells are deliberate editorial calls; the caption footnote carries the
# rationale for every PART ($\sim$).
ROWS = [
    # -- performance-oriented streaming benchmarks -------------------------
    ("Linear Road", "arasu2004linear",
     [NONE, PART, NONE, NONE, PART, FULL, NONE, NONE, NONE, NONE, NONE, NONE, NONE, NONE]),
    ("NEXMark", "tucker2008nexmark",
     [NONE, NONE, NONE, NONE, PART, FULL, NONE, NONE, NONE, NONE, NONE, NONE, NONE, NONE]),
    # Recovery is PART, not NONE: StreamBench ships a fault-tolerance workload
    # suite alongside its performance suites and reports recovery ability. It
    # measures throughput degradation after a node failure, not post-recovery
    # state equivalence against an oracle, so the cell is partial.
    ("StreamBench", "lu2014streambench",
     [NONE, NONE, NONE, NONE, FULL, PART, NONE, NONE, NONE, NONE, PART, NONE, NONE, NONE]),
    ("Karimov et al.", "karimov2018benchmarking",
     [PART, NONE, NONE, NONE, FULL, PART, NONE, NONE, NONE, PART, PART, NONE, NONE, NONE]),
    # The most-cited streaming benchmark: synthetic ad events, one windowed
    # count. It wins Distributed and loses Query breadth on its single query.
    ("Yahoo Streaming", "chintapalli2016benchmarking",
     [NONE, NONE, NONE, NONE, FULL, NONE, NONE, NONE, NONE, NONE, NONE, NONE, NONE, NONE]),
    ("Theodolite", "henning2021theodolite",
     [PART, NONE, NONE, NONE, FULL, PART, NONE, NONE, NONE, NONE, NONE, NONE, NONE, NONE]),
    # DSPBench and RIoTBench both beat EquiTable on real data AND query
    # breadth: 15 applications and 27 IoT tasks respectively, on real traces.
    ("DSPBench", "bordin2020dspbench",
     [FULL, NONE, NONE, NONE, FULL, FULL, NONE, NONE, NONE, NONE, NONE, NONE, NONE, NONE]),
    ("RIoTBench", "shukla2017riotbench",
     [FULL, NONE, NONE, NONE, FULL, FULL, NONE, NONE, NONE, NONE, NONE, NONE, NONE, NONE]),
    ("ShuffleBench", "pfandzelter2024shufflebench",
     [PART, NONE, NONE, NONE, FULL, PART, NONE, NONE, NONE, NONE, NONE, NONE, NONE, NONE]),
    ("DEBS GC", "mutschler2013debs",
     [FULL, PART, NONE, NONE, PART, FULL, NONE, NONE, NONE, NONE, NONE, NONE, NONE, NONE]),
    # -- oracle-based accuracy benchmarks ---------------------------------
    # ESPBench is the nearest prior work on R1 and the one a reviewer will
    # raise: it validates query results against expected output. PART, not
    # FULL, because the comparison is against precomputed expected output for a
    # fixed workload, not against an answer recomputed from the admitted events
    # of the same replay under perturbation.
    ("ESPBench", "hesse2021espbench",
     [PART, PART, NONE, NONE, FULL, PART, NONE, NONE, NONE, NONE, NONE, NONE, NONE, NONE]),
    ("IDEBench", "eichmann2020idebench",
     [FULL, PART, NONE, NONE, NONE, PART, NONE, NONE, NONE, NONE, NONE, NONE, NONE, NONE]),
    ("SIMBA", "purich2025simba",
     [FULL, PART, NONE, NONE, NONE, PART, NONE, NONE, NONE, NONE, NONE, NONE, NONE, NONE]),
    # -- fault injection and black-box correctness checking ---------------
    # Dup is PART, not NONE: Algorithm 1 searches each produced output ID in the
    # oracle's expected list and marks an ID seen twice as a duplicate. That
    # detects duplicated output under at-least-once delivery; it does not
    # deduplicate by source identity as part of the query semantics, so partial.
    ("PGVal", "tahir2024streams",
     [PART, PART, NONE, NONE, FULL, PART, PART, NONE, NONE, NONE, PART, NONE, NONE, NONE]),
    ("Elle", "kingsbury2020elle",
     [NONE, FULL, PART, NONE, FULL, NONE, NONE, PART, NONE, NONE, PART, NONE, NONE, NONE]),
    # DiffStream compares a program's output streams with a sequential reference
    # implementation of the same program, so R1 is PART: the reference is a
    # second implementation, not a recomputation from the admitted events. Real
    # data is NONE: inputs are QuickCheck-generated streams, MapReduce programs
    # adapted from the literature, and the Yahoo Streaming Benchmark generator.
    # Distributed is PART: Flink parallelism 2 to 6, no multi-node claim. Query
    # breadth is PART: four case studies plus a set of MapReduce programs. Order
    # is PART: it checks output equivalence under parallel reordering, not
    # invariance of a maintained table to input arrival order. Recovery is NONE:
    # node and network faults are named as future work.
    ("DiffStream", "kallas2020diffstream",
     [NONE, PART, NONE, NONE, PART, PART, NONE, PART, NONE, NONE, NONE, NONE, NONE, NONE]),
    ("SQLancer", "rigger2020testing",
     [NONE, FULL, PART, NONE, NONE, FULL, NONE, NONE, NONE, NONE, NONE, NONE, NONE, NONE]),
    ("\\textbf{EquiTable}", None,
     [FULL, FULL, FULL, FULL, NONE, PART, FULL, FULL, FULL, FULL, FULL, FULL, FULL, FULL]),
]

CAPTION = (
    "Position against prior streaming benchmarks. "
    "$\\cmark$~full, $\\sim$~partial, $\\times$~not evaluated in the cited "
    "work. EquiTable loses on distributed execution and query breadth."
)


def _bib_keys() -> set[str]:
    path = ROOT / "paper" / "shared" / "ref.bib"
    if not path.is_file():
        return set()
    text = path.read_text(encoding="utf-8", errors="replace")
    return {m.group(1) for m in re.finditer(r"^@[a-zA-Z]+\s*\{\s*([^,\s]+)", text,
                                            re.MULTILINE)}


def _check_citations() -> list[str]:
    """Report curated \\cite keys that ref.bib does not define."""
    have = _bib_keys()
    wanted = [cite for _, cite, _ in ROWS if cite]
    return [key for key in wanted if key not in have]


#: The printed table groups the eight per-diagnostic columns into the three a
#: reader needs to place prior work: whether it perturbs event identity or
#: order (D1, D2, D4), recovery (D5), or retraction (D7). The per-diagnostic
#: columns where no prior system scores anything (D3, D6, D8) only repeated
#: EquiTable's own design back at the reader, so they are not printed.
PRINTED_AXES = [
    "Real data", "R1 answer", "R2 attribution", "R3 capability",
    "Identity, order", "Recovery", "Retraction",
    "Distributed", "Query breadth",
]


def _group(cells: list[str]) -> str:
    if all(c == FULL for c in cells):
        return FULL
    if any(c != NONE for c in cells):
        return PART
    return NONE


def _printed(flags: list[str]) -> list[str]:
    by = dict(zip(AXES, flags))
    return [
        by["Real data"], by["R1 answer"], by["R2 attribution"],
        by["R3 capability"],
        _group([by["Dup (D1)"], by["Order (D2)"], by["Late (D4)"]]),
        by["Recov. (D5)"], by["Retr. (D7)"],
        by["Distributed"], by["Query breadth"],
    ]


def _render() -> str:
    ncol = len(PRINTED_AXES)
    # System column left, feature columns centred and tight.
    tabular = "@{}l" + "c" * ncol + "@{}"
    # Rotated short headers keep the body narrow enough for one IEEE column.
    header_cells = " & ".join(f"\\rotatebox{{90}}{{{a}}}" for a in PRINTED_AXES)
    lines = [
        "% Auto-generated by scripts/figures/tab_01_related_work.py — do not edit by hand.",
        "% HAND table: cells are editorial judgements, not measured results.",
        "% Requires \\usepackage{graphicx} (\\rotatebox) — present in both drafts.",
        "% The Distributed and Query-breadth axes exist so this is not an",
        "% all-check victory column; EquiTable loses both. Keep it that way.",
        "\\begin{table}[!t]",
        "\\centering",
        f"\\caption{{{CAPTION}}}",
        "\\label{tab:related-work}",
        "\\scriptsize",
        "\\setlength{\\tabcolsep}{2.4pt}",
        f"\\begin{{tabular}}{{{tabular}}}",
        "\\toprule",
        "System & " + header_cells + " \\\\",
        "\\midrule",
    ]
    for name, cite, flags in ROWS:
        if len(flags) != len(AXES):
            raise ValueError(f"row {name!r} has {len(flags)} cells, expected {len(AXES)}")
        label = name if cite is None else f"{name}~\\cite{{{cite}}}"
        lines.append(label + " & " + " & ".join(_printed(flags)) + " \\\\")
    lines += [
        "\\bottomrule",
        "\\end{tabular}",
        "\\end{table}",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    table = _render()
    write_table(table, "tab_01_related_work")
    missing = _check_citations()
    if missing:
        print("WARNING: paper/shared/ref.bib is missing these Tab 1 citation keys:")
        for key in missing:
            print(f"  - {key}")
        print("WARNING: the built PDF will show '?' for them until they are added.")
    print(f"PASS: wrote Tab 1 ({len(ROWS)} systems x {len(AXES)} axes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
