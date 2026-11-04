#!/usr/bin/env python3
"""Diff a fresh-run output tree against the committed reference tree.

Walks every JSON file under <reference_dir> and tries to find a corresponding
file under <fresh_dir>. For each pair, reports per-field PASS/FAIL with an
epsilon tolerance for floats. Designed for the publication reproducibility
review where the committee runs reproduce_all.sh, gets fresh outputs under
data/processed/experiments/, and wants to confirm they match the paper's
shipped reference under results/.

Usage:
    python scripts/results/diff_against_reference.py <fresh_dir> <reference_dir>
    python scripts/results/diff_against_reference.py data/processed/experiments results

Exit code 0 if every checked numeric field matches within epsilon; non-zero
otherwise. Per-file PASS/FAIL lines go to stdout; a final summary follows.
"""
from __future__ import annotations

import argparse
import json
import math
import pathlib
import sys
from typing import Any, Iterable

EPS = 1e-6  # tolerated absolute difference between floats
REL = 1e-3  # tolerated relative difference (covers float rounding + minor
            # nondeterminism in latency tails)

# Fields that are environment-dependent and should not be diffed.
IGNORE_KEYS = {
    "exact_dir", "oracle_dir", "exact_replay_dir", "offline_oracle_dir",
    "hourly_mismatch_report", "daily_mismatch_report",
    "output_dir", "input_path", "input", "timestamp", "wall_time_seconds",
    "elapsed_seconds", "started_at", "ended_at", "host", "hostname",
    "completed_at",
    # per-row latency arrays, too volatile to diff field-by-field; the
    # aggregate percentiles (p50/p95/p99) are kept and diffed.
    "per_row_update_latency_ms_samples",
    "query_bundle_latency_samples_ms",
    "processing_lag_samples_ms",
}


def is_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def close(a: float, b: float) -> bool:
    if not math.isfinite(a) or not math.isfinite(b):
        return False
    if abs(a - b) <= EPS:
        return True
    denom = max(abs(a), abs(b))
    return denom > 0 and abs(a - b) / denom <= REL


def walk_compare(ref: Any, fresh: Any, path: str, mismatches: list[str]) -> None:
    if isinstance(ref, dict):
        if not isinstance(fresh, dict):
            mismatches.append(f"{path}: type mismatch (ref=dict, fresh={type(fresh).__name__})")
            return
        for k in ref:
            if k in IGNORE_KEYS:
                continue
            if k not in fresh:
                mismatches.append(f"{path}.{k}: missing in fresh")
                continue
            walk_compare(ref[k], fresh[k], f"{path}.{k}", mismatches)
        return
    if isinstance(ref, list):
        if not isinstance(fresh, list):
            mismatches.append(f"{path}: type mismatch (ref=list, fresh={type(fresh).__name__})")
            return
        if len(ref) != len(fresh):
            mismatches.append(f"{path}: list length {len(ref)} vs {len(fresh)}")
            return
        for i, (a, b) in enumerate(zip(ref, fresh)):
            walk_compare(a, b, f"{path}[{i}]", mismatches)
        return
    if is_number(ref) and is_number(fresh):
        if not close(float(ref), float(fresh)):
            mismatches.append(f"{path}: {ref} vs {fresh}  (Δ={float(fresh)-float(ref):+.6g})")
        return
    if ref != fresh:
        # Allow string differences only in fields we explicitly ignore.
        mismatches.append(f"{path}: {ref!r} vs {fresh!r}")


def iter_reference_jsons(reference_dir: pathlib.Path) -> Iterable[pathlib.Path]:
    yield from (p for p in sorted(reference_dir.rglob("*.json")) if p.name != "promotion_manifest.json")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("fresh_dir", help="Directory of fresh-run outputs (e.g., data/processed/experiments)")
    ap.add_argument("reference_dir", help="Directory of committed reference outputs (e.g., results)")
    ap.add_argument("--strict", action="store_true",
                    help="Fail on first mismatch instead of summarizing")
    ap.add_argument("--quiet", action="store_true", help="Only print summary line")
    args = ap.parse_args()

    fresh_root = pathlib.Path(args.fresh_dir).resolve()
    ref_root = pathlib.Path(args.reference_dir).resolve()
    if not fresh_root.is_dir():
        print(f"ERROR: fresh dir does not exist: {fresh_root}", file=sys.stderr)
        return 2
    if not ref_root.is_dir():
        print(f"ERROR: reference dir does not exist: {ref_root}", file=sys.stderr)
        return 2


    total = 0
    matched_files = 0
    missing_files = 0
    diff_files = 0
    all_mismatches: list[tuple[str, list[str]]] = []

    for ref_file in iter_reference_jsons(ref_root):
        rel = ref_file.relative_to(ref_root)
        fresh_file = fresh_root / rel
        total += 1
        if not fresh_file.exists():
            missing_files += 1
            if not args.quiet:
                print(f"MISS  {rel}  (reviewer did not regenerate this file)")
            continue
        try:
            with open(ref_file) as f:
                ref = json.load(f)
            with open(fresh_file) as f:
                fresh = json.load(f)
        except json.JSONDecodeError as e:
            all_mismatches.append((str(rel), [f"json decode error: {e}"]))
            diff_files += 1
            continue
        mismatches: list[str] = []
        walk_compare(ref, fresh, "", mismatches)
        if mismatches:
            diff_files += 1
            all_mismatches.append((str(rel), mismatches))
            if not args.quiet:
                print(f"DIFF  {rel}  ({len(mismatches)} mismatches)")
            if args.strict:
                break
        else:
            matched_files += 1
            if not args.quiet:
                print(f"PASS  {rel}")

    print()
    print("=" * 70)
    print(f"Reference root : {ref_root}")
    print(f"Fresh root     : {fresh_root}")
    print(f"JSON files     : {total} reference  |  {matched_files} pass  "
          f"|  {diff_files} diff  |  {missing_files} missing")
    print("=" * 70)
    if diff_files == 0 and missing_files == 0:
        print("OK: every reference JSON has a matching fresh-run counterpart.")
        return 0
    if all_mismatches:
        print("\nFirst 5 mismatches per diffed file:")
        for fname, mlist in all_mismatches[:20]:
            print(f"\n  {fname}")
            for m in mlist[:5]:
                print(f"    {m}")
            if len(mlist) > 5:
                print(f"    ... {len(mlist) - 5} more")
    return 1


if __name__ == "__main__":
    sys.exit(main())
