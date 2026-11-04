#!/usr/bin/env python3
"""Validate normalized logs and optional perturbation fixtures against manifests."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.schema import NORMALIZED_COLUMNS
from src.provenance import run_provenance

DOMAINS = [
    "environment_airquality", "transportation_taxi", "datacenter_drives",
    "finance_markets", "sports_football",
]
TIME_RE = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z")


@lru_cache(maxsize=None)
def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def snapshot_record(path: Path, digest: str, rows: int | None = None) -> dict:
    stat = path.stat()
    record = {
        "path": path.relative_to(ROOT).as_posix(),
        "sha256": digest,
        "bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }
    if rows is not None:
        record["rows"] = rows
    return record


def check_base(domain: str, records: list[dict]) -> list[str]:
    root = ROOT / "data" / "normalized" / domain
    path = root / "log.parquet"
    manifest = json.loads((root / "manifest.json").read_text())
    frame = pd.read_parquet(path)
    errors = []
    if list(frame.columns) != NORMALIZED_COLUMNS:
        errors.append("canonical column order differs")
    if len(frame) != manifest.get("rows"):
        errors.append("manifest row count differs")
    digest = sha256(path)
    if digest != manifest.get("sha256"):
        errors.append("manifest sha256 differs")
    if manifest.get("contract_version") != 2:
        errors.append("contract_version is not 2")
    if manifest.get("dedupe_identity") != "source_observation":
        errors.append("dedupe_identity is not source_observation")
    if not frame["event_time_utc"].map(
        lambda value: bool(TIME_RE.fullmatch(str(value)))
    ).all():
        errors.append("event_time_utc is not canonical six-digit UTC")
    ids = frame["dedupe_id"]
    if ids.isna().any() or ids.astype(str).eq("").any() or not ids.is_unique:
        errors.append("base dedupe_id is empty or non-unique")
    expected_order = np.arange(len(frame), dtype=np.int64)
    if not np.array_equal(frame["processing_order"].to_numpy(), expected_order):
        errors.append("processing_order is not contiguous from zero")
    # The base log is replayed in the total order (event_time_utc, dedupe_id);
    # disorder may enter only through a fixture. Canonical six-digit UTC strings
    # sort chronologically, so a string comparison is exact here.
    ordered = frame.sort_values(["event_time_utc", "dedupe_id"], kind="mergesort")
    if not np.array_equal(ordered.index.to_numpy(), frame.index.to_numpy()):
        errors.append("base log is not in (event_time_utc, dedupe_id) order")
    if not np.isfinite(pd.to_numeric(frame["value"], errors="coerce")).all():
        errors.append("value contains non-finite data")
    records.append(snapshot_record(path, digest, len(frame)))
    records.append(snapshot_record(
        root / "manifest.json", sha256(root / "manifest.json")))
    return errors


def check_fixtures(domain: str, records: list[dict]) -> list[str]:
    root = ROOT / "data" / "fixtures" / domain
    manifest = json.loads((root / "manifest.json").read_text())
    errors = []
    base = ROOT / manifest["base_log"]
    if sha256(base) != manifest.get("base_sha256"):
        errors.append("fixture base_sha256 differs from normalized log")
    if manifest.get("contract_version") != 2:
        errors.append("fixture contract_version is not 2")
    if manifest.get("dedupe_identity") != "source_observation":
        errors.append("fixture dedupe_identity is not source_observation")
    if manifest.get("self_check") != "PASS":
        errors.append("fixture self_check is not PASS")
    for entry in manifest.get("fixtures", []):
        path = root / entry["name"]
        if not path.is_file():
            errors.append(f"missing {entry['name']}")
            continue
        digest = sha256(path)
        if digest != entry.get("sha256"):
            errors.append(f"sha256 differs for {entry['name']}")
        rows = pq.ParquetFile(path).metadata.num_rows
        if rows != entry.get("rows"):
            errors.append(f"row count differs for {entry['name']}")
        records.append(snapshot_record(path, digest, rows))
    records.append(snapshot_record(
        root / "manifest.json", sha256(root / "manifest.json")))
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixtures", action="store_true")
    parser.add_argument("--snapshot", type=Path,
                        help="Write an immutable data identity snapshot after a full pass.")
    args = parser.parse_args()
    failed = False
    records: list[dict] = []
    for domain in DOMAINS:
        errors = check_base(domain, records)
        if args.fixtures:
            errors.extend(check_fixtures(domain, records))
        if errors:
            failed = True
            print(f"FAIL {domain}: {'; '.join(errors)}")
        else:
            suffix = " + fixtures" if args.fixtures else ""
            print(f"PASS {domain}{suffix}")
    if failed:
        return 1
    if args.snapshot:
        args.snapshot.parent.mkdir(parents=True, exist_ok=True)
        snapshot = {
            "kind": "data_snapshot",
            "created_utc": datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"),
            **run_provenance(ROOT),
            "files": sorted(records, key=lambda item: item["path"]),
        }
        args.snapshot.write_text(
            json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
        print(f"snapshot: {args.snapshot}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
