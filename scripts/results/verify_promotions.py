#!/usr/bin/env python3
"""Verify that paper evidence is complete, consistent, and unchanged."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _evidence import verify_evidence  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--require-lock", action="store_true")
    args = parser.parse_args()
    failures, records, lock = verify_evidence(ROOT, require_lock=args.require_lock)

    oracle = [record for record in records
              if record["_block"] == "oracle_verification"]
    if oracle and (
        {record.get("domain") for record in oracle} != {
            "environment_airquality", "transportation_taxi",
            "datacenter_drives", "finance_markets", "sports_football"
        }
        or any(record.get("rows") != 1_000_000
               or record.get("all_equivalent") is not True
               for record in oracle)
    ):
        failures.append("oracle evidence is not five equivalent one-million-row domains")

    for block in ("mutation", "native_semantics"):
        controls = [record for record in records if record["_block"] == block]
        if controls and controls[0].get("pass") is not True:
            failures.append(f"required control block {block} did not pass")

    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        print(f"FAIL: paper evidence has {len(failures)} validation error(s).")
        return 1
    state = "locked official" if lock else "complete promotable"
    print(f"PASS: {len(records)} promoted summaries form one {state} benchmark run.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
