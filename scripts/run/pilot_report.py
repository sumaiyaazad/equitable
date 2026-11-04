#!/usr/bin/env python3
"""Summarize and A/B-compare pilot runs.

A pilot is a one-domain, one-repeat sweep run OUTSIDE the campaign machinery, so
its summaries carry no ``campaign_id`` and can never be promoted into the paper.
Its only job is to answer, cheaply, the question that decides the paper: do
released engines actually diverge under the contract?

    # one pilot
    python scripts/run/pilot_report.py --invocation pilot_baseline_...

    # A/B two pilots (e.g. before vs after a design change)
    python scripts/run/pilot_report.py \\
        --invocation pilot_baseline_... --against pilot_arm_...

Verdict vocabulary matches scripts/run/benchmark_status.py: a cell is `blocked`
when no valid comparison was produced, and blocked cells are never counted as
passes.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _cells(invocation: str) -> dict[tuple[str, str], dict]:
    out: dict[tuple[str, str], dict] = {}
    for path in sorted((ROOT / "runs").glob("*/summary.json")):
        try:
            summary = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if summary.get("invocation_id") != invocation:
            continue
        key = (str(summary.get("engine")), str(summary.get("diagnostic")))
        # Later run directory wins, so a re-run supersedes an earlier attempt.
        out[key] = summary
    return out


def _state(summary: dict) -> str:
    if summary.get("error") or summary.get("artifact_valid") is not True:
        return "blocked"
    primary = summary.get("primary") or {}
    if primary.get("supported") is False or summary.get("supported") is False:
        return "unsupported"
    if summary.get("pass") is True:
        return "equivalent"
    known = primary.get("known_finding") or summary.get("known_finding")
    return "known-div" if known else "divergent"


def _emr(summary: dict) -> str:
    primary = summary.get("primary") or {}
    value = (primary.get("hourly") or {}).get("emr")
    return f"{value:.4g}" if isinstance(value, (int, float)) else "-"


def _idiomatic(summary: dict) -> str:
    arm = summary.get("idiomatic")
    if not isinstance(arm, dict):
        return "-"
    if arm.get("arm_error"):
        return "ARM-ERR"
    if arm.get("supported") is False:
        return "n/a"
    flag = summary.get("repair_is_load_bearing")
    verdict = "equivalent" if arm.get("pass") else "divergent"
    return f"{verdict}{'*' if flag else ''}"


def _report(invocation: str, cells: dict) -> None:
    print(f"\n=== {invocation}: {len(cells)} cells ===")
    print(f"{'engine':12} {'diag':5} {'state':10} {'emr':>10}  {'idiomatic':10}")
    for (engine, diag) in sorted(cells):
        summary = cells[(engine, diag)]
        print(f"{engine:12} {diag:5} {_state(summary):10} {_emr(summary):>10}  "
              f"{_idiomatic(summary):10}")
    tally: dict[str, int] = {}
    for summary in cells.values():
        state = _state(summary)
        tally[state] = tally.get(state, 0) + 1
    print("  totals: " + ", ".join(f"{k}={v}" for k, v in sorted(tally.items())))
    load_bearing = [k for k, s in cells.items() if s.get("repair_is_load_bearing")]
    if load_bearing:
        print("  repair is load-bearing (safe rule expressible, idiomatic one is not):")
        for engine, diag in sorted(load_bearing):
            print(f"    {engine} {diag}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--invocation", required=True)
    parser.add_argument("--against", help="second invocation id to diff against")
    args = parser.parse_args()

    left = _cells(args.invocation)
    if not left:
        print(f"no cells found for {args.invocation}")
        return 1
    _report(args.invocation, left)
    if not args.against:
        return 0

    right = _cells(args.against)
    if not right:
        print(f"no cells found for {args.against}")
        return 1
    _report(args.against, right)

    print(f"\n=== changed cells: {args.invocation} -> {args.against} ===")
    changed = 0
    for key in sorted(set(left) | set(right)):
        a = _state(left[key]) if key in left else "absent"
        b = _state(right[key]) if key in right else "absent"
        if a != b:
            changed += 1
            print(f"{key[0]:12} {key[1]:5} {a:10} -> {b}")
    print(f"{changed} cell(s) changed state")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
