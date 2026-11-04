#!/usr/bin/env python3
"""Fail fast unless the active interpreter matches the artifact lock."""
from __future__ import annotations

import importlib.metadata
import platform
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _verify_ingest_helpers() -> list[str]:
    """Exercise unit-sensitive source conversion before long ingestion runs."""
    sys.path.insert(0, str(ROOT))
    import pandas as pd
    from scripts.ingest.ingest_finance_markets import _epoch_to_iso

    raw = pd.Series([
        "1717200005871",       # milliseconds
        "1717200005871000",    # microseconds
        "bad",
        "999999999999999999999",
        "1717200005871.5",
        None,
    ])
    converted = _epoch_to_iso(raw)
    expected = "2024-06-01T00:00:05.871000Z"
    errors: list[str] = []
    if converted.iloc[0] != expected:
        errors.append("Binance millisecond epoch conversion failed")
    if converted.iloc[1] != expected:
        errors.append("Binance microsecond epoch conversion failed")
    if not converted.iloc[2:].isna().all():
        errors.append("Binance malformed/out-of-range epochs were not rejected")
    return errors


def main() -> int:
    expected_python = (ROOT / ".python-version").read_text().strip()
    actual_python = platform.python_version()
    errors = []
    if actual_python != expected_python:
        errors.append(f"Python {actual_python}; expected {expected_python}")

    for raw in (ROOT / "requirements.txt").read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = re.match(r"^([A-Za-z0-9_.-]+)(?:\[[^]]+\])?==([^\s\\;]+)", line)
        if match is None:
            continue
        dist_name, expected = match.groups()
        try:
            actual = importlib.metadata.version(dist_name)
        except importlib.metadata.PackageNotFoundError:
            errors.append(f"{dist_name} is not installed (expected {expected})")
            continue
        if actual != expected:
            errors.append(f"{dist_name}=={actual}; expected {expected}")

    pip_check = subprocess.run(
        [sys.executable, "-m", "pip", "check"],
        cwd=ROOT, text=True, capture_output=True,
    )
    if pip_check.returncode:
        errors.append(pip_check.stdout.strip() or pip_check.stderr.strip())

    try:
        errors.extend(_verify_ingest_helpers())
    except Exception as exc:
        errors.append(f"ingest-helper preflight raised {type(exc).__name__}: {exc}")

    if errors:
        print("FAIL: reproduction environment does not match the lock:")
        for error in errors:
            print(f"  - {error}")
        print("Install with: ./reproduce_all.sh setup")
        return 1
    print(f"PASS: Python {actual_python}; all reproduction pins match; pip and ingest-helper checks clean.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
