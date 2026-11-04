#!/usr/bin/env python3
"""Lock one complete promoted benchmark run as the paper's official evidence."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _evidence import LOCK_NAME, build_lock, load_lock, verify_evidence  # noqa: E402


def main() -> int:
    destination = ROOT / "results" / LOCK_NAME
    existing = load_lock(ROOT)
    failures, records, _ = verify_evidence(ROOT)
    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        return 1
    expected = build_lock(records)
    if existing is not None:
        comparable = {key: value for key, value in existing.items()
                      if not key.startswith("_")}
        if comparable != expected:
            print("FAIL: official results are already locked to different evidence.")
            return 1
        print(f"PASS: official results already locked to {expected['benchmark_run_id']}.")
        return 0
    destination.write_text(json.dumps(expected, indent=2, sort_keys=True) + "\n")
    print(f"PASS: locked official paper evidence to {expected['benchmark_run_id']}.")
    print(f"output: {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
