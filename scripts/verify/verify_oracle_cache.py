#!/usr/bin/env python3
"""Verify every expected-table cache artifact referenced by one campaign."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CACHE = ROOT / "data" / "oracle_cache"
RUNS = ROOT / "runs"
ORACLE_DIAGNOSTICS = {"D1", "D2", "D4", "D5", "D6", "D7"}
CORE_PHASES = {"grid50k", "confirm1m", "scaling"}
LEGACY_PHASES = {"ladder": "scaling"}


def canonical_phase(phase: object) -> object:
    return LEGACY_PHASES.get(phase, phase)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-id", required=True)
    args = parser.parse_args()

    referenced: set[str] = set()
    missing_run_keys: list[str] = []
    for path in RUNS.glob("*/summary.json"):
        try:
            summary = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if summary.get("campaign_id") != args.campaign_id:
            continue
        keys = summary.get("oracle_cache_keys") or []
        referenced.update(str(key) for key in keys)
        primary = summary.get("primary") or {}
        supported = (summary.get("supported") is not False
                     and primary.get("supported") is not False)
        if (canonical_phase(summary.get("campaign_phase")) in CORE_PHASES
                and summary.get("diagnostic") in ORACLE_DIAGNOSTICS
                and summary.get("artifact_valid") is True
                and supported
                and not keys):
            missing_run_keys.append(path.parent.name)

    failures: list[str] = []
    if missing_run_keys:
        failures.append(
            f"{len(missing_run_keys)} oracle-backed runs have no cache key")
    for key in sorted(referenced):
        cache_dir = CACHE / key
        metadata_path = cache_dir / "metadata.json"
        if not metadata_path.is_file():
            failures.append(f"{key}: metadata.json missing")
            continue
        try:
            metadata = json.loads(metadata_path.read_text())
            spec = metadata["spec"]
            expected_key = hashlib.sha256(json.dumps(
                spec, sort_keys=True, separators=(",", ":"),
            ).encode("utf-8")).hexdigest()
            if expected_key != key:
                failures.append(f"{key}: key does not match specification")
            co_oracle = metadata.get("co_oracle_result")
            if spec.get("co_oracle") and not (
                    isinstance(co_oracle, dict) and co_oracle.get("agree") is True):
                failures.append(f"{key}: pandas/DuckDB agreement is not true")
            for name, expected in metadata["files_sha256"].items():
                artifact = cache_dir / name
                if not artifact.is_file():
                    failures.append(f"{key}: {name} missing")
                elif sha256(artifact) != expected:
                    failures.append(f"{key}: {name} hash mismatch")
        except (KeyError, OSError, TypeError, ValueError) as exc:
            failures.append(f"{key}: invalid metadata ({exc})")

    if failures:
        print(f"FAIL: oracle cache for campaign {args.campaign_id}")
        for failure in failures[:30]:
            print(f"  {failure}")
        return 1
    print(f"PASS: {len(referenced)} oracle cache artifact(s) verified for "
          f"campaign {args.campaign_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
