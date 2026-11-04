#!/usr/bin/env python3
"""Emit the root-cause table: every divergent cell, grouped by what caused it.

A count of divergent cells says how often the contract failed, not why. One
engine-level cause can reach many cells (a reduction defect shows under every
diagnostic that reads a sum), so a reader who sees "62 divergent cells" cannot
tell whether that is one defect or sixty. This table answers that.

Each divergent or repeat-unstable cell is assigned a SIGNATURE from the fields
that disagreed, which the promoted evidence records per cell:

  replay     the table matched the oracle but not a second replay (D3 only)
  count      the bucket count differs: a contribution was added or lost
  sum_mean   only sum and mean differ: the reduction, not the rows
  latest     only the latest value or its event time differs: the wrong row
             was selected as latest
  values     other reductions (minimum, maximum, sum) differ with the count
             intact: a superseded value was kept

The explanation for an (engine, signature) pair comes from CAUSES below, each
tied to the evidence that established it. A divergent cell whose pair has no
entry is a generation ERROR, so a new benchmark run cannot publish a divergence
nobody has explained. `--preview` lists such cells instead of failing.

The table is the paper's defect ledger. Each cause has a KIND (an engine
defect, documented engine semantics, a precision or reproducibility trade-off,
or a defect of our own harness) and an upstream status, so a reader can see
which divergences are bugs, which are design choices the contract disagrees
with, and which are ours. Two rows come from the query family rather than the
grid; FAMILY_CAUSES holds them, and their counts are family members that
diverge under event-time-ordered delivery, where the two formulations coincide.

Inputs  : results/design_space/*/summary.json, results/query_family
          (or --from-run and --family-run for a preview)
Outputs : paper/*/tables/tab_21_root_causes.tex
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from figures import load_module  # noqa: E402

_io = load_module("_io")

LATEST = {"latest_value", "latest_event_time"}
SUM_MEAN = {"sum", "mean"}

#: Kinds, in the order the ledger prints them.
KINDS = (
    ("defect", "Engine defects"),
    ("documented", "Documented engine semantics"),
    ("precision", "Precision and reproducibility trade-offs"),
    ("harness", "Defects in our harness"),
)

#: (engine, signature) -> (what differs, cause, kind, upstream). Keep each
#: cause one short clause; the body text carries the evidence.
CAUSES: Dict[Tuple[str, str], Tuple[str, str, str, str]] = {
    ("materialize", "sum_mean"): (
        "sum, mean",
        r"float sums accumulate in fixed point with 24 fractional bits, "
        r"truncating each value~\cite{materialize_reduce_src}",
        "precision",
        # Merged 2026-09-29 in response to discussion 39315.
        r"documented~\cite{mz_pr39330} after our report~\cite{mz_discussion39315}"),
    ("ksqldb", "count"): (
        "count",
        r"deduplication must re-read a table's changelog as a stream, so a "
        r"late redelivery is counted again",
        "defect",
        r"reported~\cite{ksql_issue11210}"),
    ("ksqldb", "latest"): (
        "latest row",
        r"the only latest operator, \texttt{LATEST\_BY\_OFFSET}, follows "
        r"arrival order",
        "documented",
        r"acknowledged in review~\cite{ksql_pr4782}"),
    # The docs: "Updates on the table side don't produce updated join output."
    ("ksqldb", "values"): (
        "min, max, sum",
        r"a stream-table join is not retroactive, so a correction that follows "
        r"its row never replaces the value",
        "documented",
        r"documented~\cite{ksqldb_join_docs}"),
    ("flink", "replay"): (
        "replay bits",
        r"parallel partial sums combine in a run-dependent order; "
        r"bit-identical at parallelism one",
        "precision",
        r"none needed"),
    # Not parallel combination: replays differ at parallelism one too. The
    # GroupTopN that implements DISTINCT ON emits each chunk's rows in std
    # HashMap iteration order (group_top_n.rs, v2.8.4, lines 173 and 234), and
    # a sum read straight from the table with no dedup is bit-identical.
    ("risingwave", "replay"): (
        "replay bits",
        r"deduplication emits rows in hash-map order, so sums add in a "
        r"run-dependent order~\cite{risingwave_group_topn_src}",
        "precision",
        r"reported~\cite{risingwave_issue27347}"),
}

#: engine -> (what differs, cause, kind, upstream, shapes) for query-family
#: members that diverge under event-time-ordered delivery. `shapes` selects
#: the members the row explains; any other ordered-delivery divergence is a
#: generation error, exactly as an unexplained grid cell is.
FAMILY_CAUSES: Dict[str, Tuple[str, str, str, str, Tuple[str, ...]]] = {
    # Witness attached to FLINK-40964; its plan shows the outer
    # Rank with AppendFastStrategy over an updating inner Rank.
    "flink": (
        "selected rows",
        r"a ranking or filter over an updating latest-row selection never "
        r"retracts a row that a later update replaced (\exref{ex:topn})",
        "defect",
        # Posted to user@flink.apache.org 2026-10-02; filed as FLINK-40964 2026-10-08.
        # A contributor replied 2026-10-23: fixed by FLINK-40368 (2.2.2, 2.3.1, 2.4.0).
        r"reported~\cite{flink_issue40964}; "
        r"fixed from 2.2.2~\cite{flink_issue40368}",
        ("S3_rank_by_latest", "S5_filter_on_latest")),
}


def signature(row: Dict[str, Any]) -> str:
    fields = set(row.get("diverging_fields") or [])
    if int(row.get("structural_key_mismatches") or 0):
        return "keys"
    if not fields:
        return "replay" if row.get("diagnostic") == "D3" else "ranking"
    if "count" in fields:
        return "count"
    if fields <= SUM_MEAN:
        return "sum_mean"
    if fields <= LATEST:
        return "latest"
    return "values"


def _diag_list(diags: List[str]) -> str:
    """Compress consecutive portable diagnostics into ranges: D1--D5, D7, D8."""
    numbered = sorted({int(d[1:]) for d in diags if d[1:].isdigit()})
    other = sorted({d for d in diags if not d[1:].isdigit()})
    parts, run = [], []
    for n in numbered:
        if run and n == run[-1] + 1:
            run.append(n)
            continue
        if run:
            parts.append(f"D{run[0]}--D{run[-1]}" if len(run) > 2 else
                         ", ".join(f"D{x}" for x in run))
        run = [n]
    if run:
        parts.append(f"D{run[0]}--D{run[-1]}" if len(run) > 2 else
                     ", ".join(f"D{x}" for x in run))
    return ", ".join(parts + other)


def family_groups(family: Dict[str, Any] | None) -> Dict[str, List[Dict[str, Any]]]:
    """Ordered-delivery family members that diverge, by engine.

    Under ordered delivery the two formulations coincide, so a member counts
    whenever either view misses the oracle: the contract view
    (repair_divergent) or the engine's own view (repaired). The RQ5 prose
    counts the same members."""
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in (family or {}).get("rows") or []:
        if (row.get("delivery") == "ordered"
                and row.get("attribution") in ("repaired", "repair_divergent")):
            groups[str(row.get("engine"))].append(row)
    return groups


def render(rows: List[Dict[str, Any]], *, preview: bool = False,
           family: Dict[str, Any] | None = None) -> str:
    _io.validate_design_space_cells(rows)
    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if _io.presentation_status(row) in ("fail", "repeat-unstable"):
            groups[(row["engine"], signature(row))].append(row)
    fam = family_groups(family)
    missing = sorted(k for k in groups if k not in CAUSES)
    missing += sorted(
        (engine, "family:" + str(r.get("shape")))
        for engine, members in fam.items() for r in members
        if engine not in FAMILY_CAUSES
        or r.get("shape") not in FAMILY_CAUSES[engine][4])
    if missing and not preview:
        raise ValueError(
            f"divergent cells with no recorded cause: {missing}; add the "
            "cause to CAUSES with its evidence before publishing")
    engines = list(_io.ENGINE_ORDER)
    total = sum(len(v) for v in groups.values())
    # One printed row per (kind, engine, cause): signatures that share a cause
    # (Feldera's incomplete reads show as missing keys or as low counts) merge.
    printed: Dict[str, List[Tuple[str, str, str, str, List[Dict[str, Any]], bool]]] = defaultdict(list)
    for engine in engines:
        merged: Dict[Tuple[str, str, str], List[Any]] = {}
        for key in (k for k in groups if k[0] == engine):
            what, cause, kind, upstream = CAUSES.get(
                key, (key[1], r"\textbf{unexplained}", "defect", "--"))
            slot = merged.setdefault((kind, cause, upstream), [[], []])
            slot[0].append(what)
            slot[1].extend(groups[key])
        for (kind, cause, upstream), (whats, cells) in merged.items():
            what = whats[0] if len(set(whats)) == 1 else ", ".join(dict.fromkeys(whats))
            printed[kind].append((engine, what, cause, upstream, cells, False))
        if engine in fam and engine in FAMILY_CAUSES:
            what, cause, kind, upstream, _ = FAMILY_CAUSES[engine]
            printed[kind].append((engine, what, cause, upstream, fam[engine], True))
    lines = [
        r"% Auto-generated by scripts/figures/tab_21_root_causes.py -- do not edit.",
        # Full width: the ledger opens the results, and at column width its
        # cause and upstream cells wrapped to three lines a row.
        r"\begin{table*}[!tb]",
        r"\centering",
        rf"\caption{{Defect ledger. All {total} divergent or repeat-unstable "
        r"cells of the grid, grouped by kind and cause, with upstream status. "
        r"\emph{What differs} names the fields that disagreed with the oracle; "
        r"an asterisk counts repeat-unstable cells. A \emph{query family} "
        r"row counts generated family members that diverge under ordered "
        r"delivery, which lie outside the grid.}",
        r"\label{tab:root-causes}",
        r"\footnotesize",
        r"\setlength{\tabcolsep}{4pt}",
        r"\begin{tabular}{@{}l>{\raggedright\arraybackslash}p{0.09\textwidth}"
        r">{\raggedright\arraybackslash}p{0.48\textwidth}"
        r">{\raggedright\arraybackslash}p{0.09\textwidth}r"
        r">{\raggedright\arraybackslash}p{0.16\textwidth}@{}}",
        r"\toprule",
        r"Engine & What differs & Cause & Diag. & Cells & Upstream \\",
    ]
    for kind, title in KINDS:
        entries = printed.get(kind)
        if not entries:
            continue
        lines += [r"\midrule", rf"\multicolumn{{6}}{{@{{}}l}}{{\emph{{{title}}}}} \\"]
        entries.sort(key=lambda e: (e[5], -len(e[4])))
        for engine, what, cause, upstream, cells, from_family in entries:
            if from_family:
                # The Diag. column already says where these come from, so the
                # count needs no superscript of its own (a reviewer read the
                # old 7^f as one notation too many).
                diags, count = "query family", f"{len(cells)}"
            else:
                unstable = sum(_io.presentation_status(c) == "repeat-unstable" for c in cells)
                diags = _diag_list([c["diagnostic"] for c in cells])
                # Repeat-unstable cells are counted with an asterisk each, so
                # the cell stays notation and the prose rule's no-parentheses holds.
                count = f"{len(cells)}" + ("*" * unstable if unstable else "")
            lines.append(
                f"{engine} & {what} & {cause} & {diags} & {count} & {upstream} \\\\")
    lines += [r"\bottomrule", r"\end{tabular}", r"\end{table*}", ""]
    return "\n".join(lines)


def harness_keys() -> set:
    """(engine, signature) pairs whose cause is our own harness."""
    return {key for key, value in CAUSES.items() if value[2] == "harness"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-run", help="preview from a collector summary.json")
    parser.add_argument("--family-run", help="query_family summary.json (preview)")
    parser.add_argument("--preview", action="store_true",
                        help="list unexplained divergences instead of failing")
    args = parser.parse_args()
    if args.from_run:
        rows = json.loads(Path(args.from_run).read_text())["rows"]
    else:
        rows = _io.load_cells("design_space")
    if args.family_run:
        family = json.loads(Path(args.family_run).read_text())
    else:
        summaries = _io.load_summaries("query_family")
        family = summaries[-1] if summaries else None
    try:
        table = render(rows, preview=args.preview, family=family)
    except ValueError as exc:
        print(f"FAIL: cannot generate the root-cause table: {exc}")
        return 1
    _io.write_table(table, "tab_21_root_causes")
    print("PASS: wrote the root-cause table")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
