#!/usr/bin/env python3
"""Health verdict for one pilot: complete, and free of blocked/errored cells."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--invocation", required=True)
    ap.add_argument("--expected", type=int, default=42)
    ap.add_argument("--running", action="store_true",
                    help="A pilot process is still live; suppress any final verdict.")
    ap.add_argument("--since", default="",
                    help="Run-dir stamp; cells older than this are labelled stale.")
    args = ap.parse_args()

    blocked, arm_errors, states = [], [], {}

    # One cell = one (engine, domain, diagnostic). A re-run supersedes the
    # earlier attempt, so collapse to the newest run directory per cell before
    # tallying -- otherwise a repaired cell is still counted at its old blocked
    # state and a healthy pre-flight reports as UNHEALTHY.
    newest: dict[tuple, tuple[str, dict]] = {}
    for path in sorted((ROOT / "runs").glob("*/summary.json")):
        try:
            s = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if s.get("invocation_id") != args.invocation:
            continue
        ident = (s.get("engine"), s.get("domain"), s.get("diagnostic"))
        # Non-cell runs (boundary, sweeps, mutation) carry no cell identity;
        # key those by run directory so they neither collide nor sort on None.
        key = tuple(str(x) for x in ident) if all(ident) else (path.parent.name,)
        stamp = path.parent.name
        prior = newest.get(key)
        if prior is None or stamp > prior[0]:
            newest[key] = (stamp, s)

    cells = len(newest)
    for key, (dirname, s) in sorted(newest.items()):
        cell = "/".join(str(x) for x in key)
        stamp = dirname.split("_")[0]
        primary = s.get("primary") or {}
        if s.get("error") or s.get("artifact_valid") is not True:
            mark = " [STALE, superseded by a later fix]" if (
                args.since and stamp < args.since) else ""
            blocked.append(f"{stamp} {cell}{mark}: {str(s.get('error'))[:70]}")
            state = "blocked"
        elif primary.get("supported") is False or s.get("supported") is False:
            # A capability limit, not a correctness result: never a failure.
            state = "unsupported"
        elif s.get("pass") is True:
            state = "exact"
        else:
            state = ("known-div"
                     if (primary.get("known_finding") or s.get("known_finding"))
                     else "divergent")
        states[state] = states.get(state, 0) + 1
        arm = s.get("idiomatic")
        if isinstance(arm, dict) and arm.get("arm_error"):
            arm_errors.append(f"{cell}: {arm['arm_error'][:80]}")

    print("  states: " + ", ".join(f"{k}={v}" for k, v in sorted(states.items())))
    if blocked:
        print(f"  BLOCKED cells ({len(blocked)}) -- these are harness failures, "
              f"not engine findings:")
        for line in blocked[:10]:
            print(f"    {line}")
    if arm_errors:
        print(f"  idiomatic arm errors ({len(arm_errors)}) -- the arm's SQL is "
              f"invalid on that engine; the cell itself is unaffected:")
        for line in arm_errors[:10]:
            print(f"    {line}")

    if args.running:
        print(f"  VERDICT: IN PROGRESS ({cells} cells so far) -- a run is still "
              f"live, so nothing here is final")
        return 3
    if cells < args.expected:
        print(f"  VERDICT: INCOMPLETE ({cells}/{args.expected})")
        return 2
    if blocked:
        print("  VERDICT: COMPLETE BUT UNHEALTHY -- investigate blocked cells "
              "before launching the campaign")
        return 1
    note = " (arm errors to fix, but campaign-safe)" if arm_errors else ""
    print(f"  VERDICT: OK -- {cells} cells, none blocked{note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
