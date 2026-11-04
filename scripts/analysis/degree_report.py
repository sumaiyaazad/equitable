#!/usr/bin/env python3
"""
degree_report.py -- the graded design-space map, from runs already on disk.

WHY THIS EXISTS
---------------
The design-space map reports one of four verdicts per cell. Two engines that both
read `divergence` are not equally wrong, and the verdict cannot separate them.

This report recovers what the binary verdict discards, using only fields already
written to every summary.json:

  extent   emr = mismatched_keys / total_keys        how many keys are wrong
  locus    which contract fields the divergence hits  where the failure sits

Extent and locus are available for every run ever recorded. The third component,
*magnitude* -- how far past the tolerance each key sits -- is not, because
`compare_bucket_tables` reduced each field to a boolean and the engine's table was
never persisted. Runs recorded after `non_equivalence` landed in the comparator
carry magnitude too, and this report prints it when present.

Reading the output: a cell confined to `sum`/`mean` is a REDUCTION-ONLY divergence;
one that moves `count` is a COUNT-INVOLVING divergence. The two do not partition the
space -- a divergence in `min`, `max`, `latest_value`, or `latest_event_time` alone
is neither, and is counted under OTHER. All three labels describe which fields moved,
not which events were admitted: equal counts do not establish that the same events
entered, since one missing and one extra event cancel.

Inputs  : runs/<UTC>_<domain>_<engine>_<diag>/summary.json
Outputs : stdout table; --json writes the full per-cell records
Runtime : seconds
Run as  : python scripts/analysis/degree_report.py
          python scripts/analysis/degree_report.py --campaign <campaign id>
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.oracle.degree import locus  # noqa: E402

ENGINE_ORDER = ["reference", "materialize", "risingwave", "ksqldb",
                "flink", "spark", "feldera"]
GRAINS = ("hourly", "daily")


def _verdict(summary: Dict[str, Any]) -> str:
    if summary.get("error") or summary.get("artifact_valid") is not True:
        return "blocked"
    primary = summary.get("primary") or {}
    if summary.get("supported") is False or primary.get("supported") is False:
        return "unsupported"
    if summary.get("pass") is True:
        return "equivalent"
    if primary.get("known_finding") or summary.get("known_finding"):
        return "known_divergence"
    return "divergence"


def _cells(campaign: Optional[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for path in (ROOT / "runs").glob("*/summary.json"):
        try:
            s = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if not s.get("engine") or not s.get("diagnostic"):
            continue
        if campaign and s.get("campaign_id") != campaign:
            continue
        primary = s.get("primary") or {}
        verdict = _verdict(s)
        for grain in GRAINS:
            block = primary.get(grain)
            if not isinstance(block, dict) or "emr" not in block:
                continue
            record = {
                "engine": s["engine"], "domain": s.get("domain"),
                "diagnostic": s["diagnostic"], "grain": grain,
                "verdict": verdict,
                "extent": float(block.get("emr") or 0.0),
                "mismatched_keys": int(block.get("mismatched_keys") or 0),
                "total_keys": int(block.get("total_keys") or 0),
            }
            record.update(locus(block.get("field_mismatches") or {}))
            # Present only for runs recorded after the comparator was instrumented.
            ne = block.get("non_equivalence")
            if isinstance(ne, dict) and ne.get("magnitude"):
                record["magnitude"] = ne["magnitude"]
            out.append(record)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--campaign", default=None,
                    help="Restrict to one campaign id (default: every run on disk).")
    ap.add_argument("--json", type=Path, default=None,
                    help="Write the full per-cell records here.")
    args = ap.parse_args()

    cells = _cells(args.campaign)
    if not cells:
        print("FAIL: no comparable cells found.")
        return 1

    diverging = [c for c in cells
                 if c["verdict"] in ("divergence", "known_divergence")
                 and c["mismatched_keys"] > 0]

    # D5 (recovery), D6 (top-k) and D7 (restatement) compare something other than
    # a bucket table, so their primary block carries no emr and extent/locus do
    # not apply. Say so rather than letting those engines silently vanish.
    out_of_scope = 0
    for path in (ROOT / "runs").glob("*/summary.json"):
        try:
            s = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if args.campaign and s.get("campaign_id") != args.campaign:
            continue
        if not s.get("engine") or _verdict(s) != "divergence":
            continue
        primary = s.get("primary") or {}
        if not any(isinstance(primary.get(g), dict) and "emr" in primary[g]
                   for g in GRAINS):
            out_of_scope += 1

    print(f"cells with a comparison : {len(cells)}")
    print(f"diverging cells         : {len(diverging)}")
    print(f"diverging, out of scope : {out_of_scope}"
          "   (D5/D6/D7 compare no bucket table)")
    print(f"cells carrying magnitude: {sum(1 for c in cells if 'magnitude' in c)}"
          "   (recorded after the comparator was instrumented)")
    print()

    by_engine: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for c in diverging:
        by_engine[c["engine"]].append(c)

    head = (f"{'engine':<13}{'cells':>6}{'median extent':>15}{'worst extent':>14}"
            f"{'count-inv':>11}{'reduc-only':>12}{'other':>8}"
            f"  {'fields most often wrong'}")
    print(head)
    print("-" * len(head))
    for engine in ENGINE_ORDER:
        rows = by_engine.get(engine)
        if not rows:
            continue
        extents = sorted(r["extent"] for r in rows)
        cnt = sum(1 for r in rows if r["count_involving"])
        red = sum(1 for r in rows if r["reduction_only"])
        # The three are mutually exclusive and exhaustive over diverging cells:
        # `count` is not in {sum, mean}, so no cell is both of the first two.
        oth = len(rows) - cnt - red
        tally: Dict[str, int] = defaultdict(int)
        for r in rows:
            for f in r["fields"]:
                tally[f] += 1
        top = ", ".join(f"{f} ({n})" for f, n in
                        sorted(tally.items(), key=lambda kv: (-kv[1], kv[0]))[:4])
        print(f"{engine:<13}{len(rows):>6}{statistics.median(extents):>15.6f}"
              f"{extents[-1]:>14.6f}{cnt/len(rows):>11.0%}{red/len(rows):>12.0%}"
              f"{oth/len(rows):>8.0%}  {top}")

    print()
    print("count-inv  = share of diverging cells whose divergence moves `count`")
    print("reduc-only = share confined to `sum`/`mean`")
    print("other      = everything else: `min`, `max`, `latest_value`, or")
    print("             `latest_event_time` moved, but not `count`.")
    print("             The three are exclusive and sum to 100%.")
    print("             Descriptive, not causal: equal counts do not establish")
    print("             that the same events were admitted.")

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(cells, indent=2, sort_keys=True))
        print(f"\nwrote {len(cells)} records to {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
