#!/usr/bin/env python3
"""Collect one balanced five-size, seven-engine scaling invocation."""
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from figures import load_module  # noqa: E402

_io = load_module("_io")
_oracle_provenance = load_module("_oracle_provenance")
RUNS = _io.RUNS

SCALE_ENGINES = set(_io.ENGINE_ORDER)
SCALE_DIAGNOSTICS = set(_io.SCALING_DIAGNOSTICS)
SCALE_SIZES = {10_000, 50_000, 200_000, 500_000, 1_000_000}
BASE_SIZE = 50_000
DOMAINS = {
    "environment_airquality", "transportation_taxi", "datacenter_drives",
    "finance_markets", "sports_football",
}

CSV_FIELDS = [
    "engine", "domain", "diagnostic", "size", "status", "pass", "stable",
    "repeats", "executed", "throughput_eligible", "duration_s",
    "throughput_eps", "run_id",
]


def _stamp(summary: Dict[str, Any]) -> str:
    return str(summary.get("_run_id", "")).split("_", 1)[0]


def _parse(ts: str) -> Optional[datetime]:
    try:
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def _throughput(summary: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    try:
        duration = float(summary["duration_s"])
    except (KeyError, TypeError, ValueError):
        start = _parse(summary.get("started_utc", ""))
        finish = _parse(summary.get("finished_utc", ""))
        duration = (finish - start).total_seconds() if start and finish else None
    if duration is None or not math.isfinite(duration) or duration <= 0:
        return duration, None
    if summary.get("executed") is False:
        return duration, None
    return duration, float(summary.get("rows", 0)) / duration


def _status(summary: Dict[str, Any]) -> str:
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


def _select_runs(invocation_id: Optional[str]) -> Tuple[
        Dict[Tuple[str, str, str, int], List[Dict[str, Any]]], Optional[str]]:
    grouped = _io.all_runs_by_key(
        "*_*_*_d*", key_fields=("engine", "domain", "diagnostic", "rows"))
    if invocation_id:
        selected_id = invocation_id
        grouped = {
            key: [s for s in summaries
                  if s.get("invocation_id") == invocation_id]
            for key, summaries in grouped.items()
            if any(s.get("invocation_id") == invocation_id for s in summaries)
        }
    else:
        grouped, selected_id = _io.restrict_to_latest_invocation(grouped)

    selected: Dict[Tuple[str, str, str, int], List[Dict[str, Any]]] = {}
    for raw_key, summaries in grouped.items():
        engine, domain, diagnostic, rows = raw_key
        if (engine not in SCALE_ENGINES or domain not in DOMAINS
                or diagnostic not in SCALE_DIAGNOSTICS
                or int(rows) not in SCALE_SIZES):
            continue
        by_repeat: Dict[int, List[Dict[str, Any]]] = {}
        for summary in summaries:
            by_repeat.setdefault(
                int(summary.get("repeat_index", 0)), []).append(summary)
        # Newest VALID attempt per repeat, not simply the newest -- see
        # _io.best_attempt: a retry that fails must not erase a verdict the
        # cell had already produced.
        selected[(str(engine), str(domain), str(diagnostic), int(rows))] = [
            chosen for chosen in (
                _io.best_attempt(attempts, _stamp)
                for attempts in by_repeat.values()
            ) if chosen is not None
        ]
    return selected, selected_id


def _cohort_rows(grouped: Iterable[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    return [summary for summaries in grouped for summary in summaries]


def _resolve_engines(spec: str, known) -> set:
    """Engines an artifact must cover before it counts as complete.

    Defaults to every engine. A narrower scope is useful for exploratory
    progress reports, but the promotion gate rejects it: official evidence must
    contain all seven engines before the immutable lock can be created.
    """
    if not spec.strip():
        return set(known)
    chosen = {name.strip() for name in spec.split(",") if name.strip()}
    unknown = chosen - set(known)
    if unknown:
        raise SystemExit(
            f"unknown engine(s): {sorted(unknown)}; known: {sorted(known)}")
    return chosen


def _canonical_provenance(
    runs: List[Dict[str, Any]],
) -> Tuple[Optional[str], List[str]]:
    input_values = {summary.get("input_sha256") for summary in runs}
    input_sha256 = next(iter(input_values)) if len(input_values) == 1 else None
    if (
        not isinstance(input_sha256, str)
        or len(input_sha256) != 64
        or any(char not in "0123456789abcdefABCDEF" for char in input_sha256)
    ):
        input_sha256 = None
    oracle_values = {
        tuple(summary.get("oracle_cache_keys") or []) for summary in runs
    }
    oracle_tuple = next(iter(oracle_values)) if len(oracle_values) == 1 else ()
    if not oracle_tuple or not all(
        isinstance(key, str) and bool(key) for key in oracle_tuple
    ):
        oracle_tuple = ()
    oracle_keys = _oracle_provenance.cache_keys(runs, list(oracle_tuple), ROOT)
    return input_sha256, oracle_keys


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--invocation-id",
                        help="Collect this exact scaling invocation.")
    parser.add_argument("--min-repeats", type=int, default=3)
    parser.add_argument(
        "--repeats", type=int, default=3,
        help="Exact repeat indices required for an explicit invocation.")
    parser.add_argument(
        "--engines", default="",
        help="Comma-separated engines to require (default: all seven). A "
             "narrowed collection is exploratory and cannot be promoted as "
             "official evidence.")
    parser.add_argument(
        "--exclude-engines", default="",
        help="Comma-separated engines left out of this cohort, "
             "e.g. 'flink'. The exclusion is recorded in the summary as "
             "excluded_engines and is the ONLY way a narrowed universe can be "
             "promoted: an undeclared narrowing stays exploratory.")
    args = parser.parse_args()
    engines = _resolve_engines(args.engines, _io.ENGINE_ORDER)
    excluded = {name.strip() for name in args.exclude_engines.split(",")
                if name.strip()}
    unknown_excluded = excluded - set(_io.ENGINE_ORDER)
    if unknown_excluded:
        parser.error(
            f"unknown engine(s) in --exclude-engines: {sorted(unknown_excluded)}")
    if excluded and args.engines.strip():
        parser.error("pass either --engines or --exclude-engines, not both")
    if excluded:
        engines = set(_io.ENGINE_ORDER) - excluded
        if not engines:
            parser.error("--exclude-engines cannot exclude every engine")
    if args.min_repeats < 1 or args.repeats < 1:
        parser.error("--min-repeats and --repeats must be >= 1")

    grouped, invocation_id = _select_runs(args.invocation_id)
    if not grouped:
        print("FAIL: no current, fingerprinted scaling benchmark found.")
        return 1

    # A declared exclusion must remove the engine's CELLS, not merely narrow the
    # required set. Leaving its rows in produced a collection whose cell count
    # exceeded the declared universe, which the promotion gate rejects with
    # "must declare the complete canonical collection protocol". The universe a
    # cohort declares and the rows it carries have to be the same universe.
    grouped = {key: runs for key, runs in grouped.items() if key[0] in engines}
    if not grouped:
        print("FAIL: no runs remain after restricting to the declared engines.")
        return 1


    source = _cohort_rows(grouped.values())
    cohort = _io.cohort_metadata(source)
    expected = {
        (engine, domain, diagnostic, size)
        for engine in engines
        for domain in DOMAINS
        for diagnostic in SCALE_DIAGNOSTICS
        for size in SCALE_SIZES
    }
    missing = sorted(expected - set(grouped))
    thin = sorted(key for key, repeats in grouped.items()
                  if len(repeats) < args.min_repeats)

    rows: List[Dict[str, Any]] = []
    input_inconsistent: List[Tuple[str, str, str, int]] = []
    oracle_inconsistent: List[Tuple[str, str, str, int]] = []
    repeat_mismatch: List[Tuple[str, str, str, int]] = []
    expected_repeat_indices = set(range(1, args.repeats + 1))
    for (engine, domain, diagnostic, size), repeats in grouped.items():
        input_sha256, oracle_cache_keys = _canonical_provenance(repeats)
        if input_sha256 is None:
            input_inconsistent.append((engine, domain, diagnostic, size))
        # An unsupported cell never built an expected table, so it names no
        # oracle. See _scope.names_an_oracle: the exemption is the same one the
        # promotion gate applies, so collection and promotion cannot disagree.
        if not oracle_cache_keys and any(
                _status(summary) != "unsupported" for summary in repeats):
            oracle_inconsistent.append((engine, domain, diagnostic, size))
        if args.invocation_id and {
                int(s.get("repeat_index", 0)) for s in repeats
                } != expected_repeat_indices:
            repeat_mismatch.append((engine, domain, diagnostic, size))
        statuses = {_status(s) for s in repeats}
        status = next(iter(statuses)) if len(statuses) == 1 else "nondeterministic"
        durations: List[float] = []
        throughputs: List[float] = []
        for summary in repeats:
            duration, throughput = _throughput(summary)
            if duration is not None and math.isfinite(duration):
                durations.append(duration)
            if throughput is not None and math.isfinite(throughput):
                throughputs.append(throughput)
        executed = any(s.get("executed", True) for s in repeats)
        rows.append({
            "engine": engine, "domain": domain, "diagnostic": diagnostic,
            "size": size, "status": status, "pass": status == "equivalent",
            "stable": None, "repeats": len(repeats), "executed": executed,
            "throughput_eligible": executed and status != "unsupported",
            "input_sha256": input_sha256,
            "oracle_cache_keys": oracle_cache_keys,
            "duration_s": round(statistics.median(durations), 3)
                if durations else None,
            "throughput_eps": round(statistics.median(throughputs), 1)
                if throughputs else None,
            "run_id": min(
                (s["_run_id"] for s in repeats), default=""),
            "run_ids": [s["_run_id"] for s in sorted(repeats, key=_stamp)],
        })

    baseline = {
        (row["engine"], row["domain"], row["diagnostic"]): row["status"]
        for row in rows if row["size"] == BASE_SIZE
    }
    for row in rows:
        base_status = baseline.get(
            (row["engine"], row["domain"], row["diagnostic"]))
        # A cell whose verdict is not reproducible across its own repeats is
        # not stable at any size. Deciding stability only by comparison with
        # the base size makes that comparison self-referential at the base
        # size itself, which reported a nondeterministic cell as stable and
        # contradicted the promotion gate in scripts/results/_scope.py.
        row["stable"] = (
            False if row["status"] == "nondeterministic"
            else (None if base_status is None
                  else row["status"] == base_status)
        )
    rows.sort(key=lambda row: (
        _io.ENGINE_ORDER.index(row["engine"]), row["domain"],
        row["diagnostic"], row["size"]))

    blocked = [row for row in rows if row["status"] == "blocked"]
    nondeterministic = [
        row for row in rows if row["status"] == "nondeterministic"]
    timing_missing = [row for row in rows
                      if row["throughput_eligible"]
                      and row["throughput_eps"] is None]
    cross_engine_input: List[Tuple[str, str, int]] = []
    cross_engine_oracle: List[Tuple[str, str, int]] = []
    for domain in DOMAINS:
        for diagnostic in SCALE_DIAGNOSTICS:
            for size in SCALE_SIZES:
                cell_runs = [
                    summary
                    for (engine, got_domain, got_diagnostic, got_size), repeats
                    in grouped.items()
                    if (got_domain, got_diagnostic, got_size) == (
                        domain, diagnostic, size)
                    for summary in repeats
                ]
                if len({s.get("input_sha256") for s in cell_runs}) > 1:
                    cross_engine_input.append((domain, diagnostic, size))
                oracle_keys = {
                    tuple(s.get("oracle_cache_keys") or [])
                    for s in cell_runs
                    if s.get("oracle_cache_keys")
                    and _status(s) != "unsupported"
                }
                if len(oracle_keys) > 1:
                    cross_engine_oracle.append((domain, diagnostic, size))
    artifact_valid = bool(rows) and cohort["cohort_consistent"] and not any((
        missing, thin, input_inconsistent, oracle_inconsistent,
        repeat_mismatch, cross_engine_input, cross_engine_oracle,
        blocked,
        timing_missing,
    ))

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    out_dir = RUNS / f"{stamp}_scaling_collect"
    out_dir.mkdir(parents=True, exist_ok=False)
    summary = {
        "kind": "scaling_collect", **cohort,
        "contract_version": 2, "git_dirty": _io.git_dirty(),
        "finalizer_commit": _io.finalizer_commit(),
        "started_utc": stamp, "invocation_id": invocation_id,
        "cells": len(rows), "source_runs": len(source),
        "engines": _io.sort_engines({r["engine"] for r in rows}),
        "sizes": sorted({r["size"] for r in rows}),
        "base_size": BASE_SIZE, "min_repeats": args.min_repeats,
        "required_engines": sorted(engines),
        "excluded_engines": sorted(excluded),
        "required_repeats": args.repeats,
        "expected_cells": len(expected),
        "nondeterministic_cells": [
            {"engine": r["engine"], "domain": r["domain"],
             "diagnostic": r["diagnostic"], "size": r["size"]}
            for r in nondeterministic],
        "missing_cells": [
            {"engine": e, "domain": d, "diagnostic": g, "size": z}
            for e, d, g, z in missing],
        "thin_cells": [
            {"engine": e, "domain": d, "diagnostic": g, "size": z}
            for e, d, g, z in thin],
        "input_inconsistent_cells": [
            {"engine": e, "domain": d, "diagnostic": g, "size": z}
            for e, d, g, z in input_inconsistent],
        "oracle_inconsistent_cells": [
            {"engine": e, "domain": d, "diagnostic": g, "size": z}
            for e, d, g, z in oracle_inconsistent],
        "repeat_mismatch_cells": [
            {"engine": e, "domain": d, "diagnostic": g, "size": z}
            for e, d, g, z in repeat_mismatch],
        "cross_engine_input_inconsistent": [
            {"domain": d, "diagnostic": g, "size": z}
            for d, g, z in cross_engine_input],
        "cross_engine_oracle_inconsistent": [
            {"domain": d, "diagnostic": g, "size": z}
            for d, g, z in cross_engine_oracle],
        "throughput_metric": (
            "events per cache-normalized diagnostic second; median of repeats"),
        "all_verdicts_stable": all(
            r["stable"] for r in rows if r["stable"] is not None),
        "rows": rows, "artifact_valid": artifact_valid,
        "pass": artifact_valid,
    }
    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n")
    with (out_dir / "scaling.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in CSV_FIELDS})

    print(f"{'PASS' if artifact_valid else 'FAIL'}: {len(rows)}/{len(expected)} "
          f"balanced scaling cells from invocation {invocation_id}; "
          f"{len(source)} source runs")
    if missing:
        print(f"  missing cells: {len(missing)}")
    if thin:
        print(f"  cells below {args.min_repeats} repeats: {len(thin)}")
    if blocked:
        print(f"  blocked cells: {len(blocked)}")
    if nondeterministic:
        print(f"  nondeterministic cells: {len(nondeterministic)}")
    print(f"output: {out_dir / 'summary.json'}")
    print(f"output: {out_dir / 'scaling.csv'}")
    return 0 if artifact_valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
