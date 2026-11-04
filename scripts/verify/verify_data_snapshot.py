#!/usr/bin/env python3
"""Verify that benchmark data still matches its frozen identity snapshot."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    parser.add_argument(
        "--full", action="store_true",
        help="Recompute every SHA-256 instead of checking file metadata only.")
    args = parser.parse_args()

    snapshot = json.loads(args.snapshot.read_text())
    failures: list[str] = []
    rehashed = 0
    for entry in snapshot.get("files", []):
        path = ROOT / entry["path"]
        if not path.is_file():
            failures.append(f"{entry['path']}: missing")
            continue
        stat = path.stat()
        if stat.st_size != entry.get("bytes"):
            failures.append(f"{entry['path']}: byte size changed")
            continue
        if args.full:
            if sha256(path) != entry.get("sha256"):
                failures.append(f"{entry['path']}: SHA-256 changed")
        elif stat.st_mtime_ns != entry.get("mtime_ns"):
            # A rewrite that preserves the bytes (re-ingest, restore, rsync)
            # only moves the mtime. Confirm identity by content before failing,
            # so metadata drift alone cannot abort a multi-day campaign.
            if sha256(path) != entry.get("sha256"):
                failures.append(f"{entry['path']}: SHA-256 changed")
            else:
                rehashed += 1
        if "rows" in entry:
            rows = int(pq.ParquetFile(path).metadata.num_rows)
            if rows != entry["rows"]:
                failures.append(f"{entry['path']}: row count changed")

    if failures:
        print(f"FAIL: data snapshot {args.snapshot}")
        for failure in failures[:30]:
            print(f"  {failure}")
        return 1
    mode = "full hashes" if args.full else "file identity"
    note = f"; {rehashed} confirmed by content after mtime drift" if rehashed else ""
    print(f"PASS: {len(snapshot.get('files', []))} frozen data files, {mode}{note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
