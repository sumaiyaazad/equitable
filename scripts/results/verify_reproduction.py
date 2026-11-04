#!/usr/bin/env python3
"""Compare one fresh benchmark run with the locked official evidence.

Correctness and control outcomes are compared cell by cell. A cell whose
outcome the locked evidence itself documents as not repeatable is tolerated
rather than compared exactly, and every tolerance is named in the report:

* D3 replays the same log twice and compares the two sums bit for bit. Flink
  combines parallel partial sums in a run-dependent order and RisingWave's
  deduplication emits rows in hash-map order, so their two replays agree on
  some runs and not on others. Their D3 cells, and any grid cell the lock
  records as repeat-unstable, may land on pass, fail or repeat-unstable.
* Flink's streaming planner never retracts a row that a later update replaced
  in a ranking or filter over an updating latest-row selection. Which family
  members show it depends on how updates fall into batches, so those members
  may score attributable, repaired or repair_divergent.
* Only Flink's replays are bit-identical at parallelism one, so only Flink
  must reproduce zero drift keys there; every engine must stay equivalent to
  the oracle at every level.

Throughput ratios are descriptive because reviewer hardware may differ.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[2]
RUNS = ROOT / "runs"
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _evidence import (  # noqa: E402
    sha256_file, stage_identities, verify_evidence,
)

#: (engine, diagnostic) pairs whose replay comparison is run dependent.
REPLAY_UNSTABLE = frozenset({("flink", "D3"), ("risingwave", "D3")})
#: Outcomes a replay-unstable grid cell may take from one run to the next.
GRID_UNSTABLE_STATUSES = frozenset({"pass", "fail", "repeat-unstable"})
#: Outcomes a replay-unstable scaling cell may take.
SCALING_UNSTABLE_STATUSES = frozenset(
    {"equivalent", "divergence", "known_divergence", "nondeterministic"})
#: (engine, shape) family members whose divergence is batch dependent.
BATCH_DEPENDENT_FAMILY = frozenset(
    {("flink", "S3_rank_by_latest"), ("flink", "S5_filter_on_latest")})
#: Attributions a scored family member may take.
FAMILY_SCORED = frozenset({"attributable", "repaired", "repair_divergent"})
#: Engines whose D3 replays are bit-identical at parallelism one.
DETERMINISTIC_AT_PARALLELISM_ONE = frozenset({"flink"})

Key = Tuple[Any, ...]


def _find(
    kind: str, invocation_id: str | tuple[str, ...], domain: str | None = None,
) -> dict[str, Any]:
    invocation_ids = (
        (invocation_id,) if isinstance(invocation_id, str) else invocation_id
    )
    matches: list[tuple[str, dict[str, Any]]] = []
    for path in RUNS.glob("*/summary.json"):
        try:
            summary = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if (summary.get("kind") == kind
                and summary.get("invocation_id") in invocation_ids
                and (domain is None or summary.get("domain") == domain)
                and summary.get("artifact_valid") is True):
            matches.append((path.parent.name, summary))
    if not matches:
        suffix = f"/{domain}" if domain else ""
        raise ValueError(
            f"no valid fresh {kind}{suffix} for {', '.join(invocation_ids)}"
        )
    return max(matches, key=lambda item: item[0])[1]


def _row_map(
    summary: dict[str, Any], keys: tuple[str, ...], values: tuple[str, ...],
) -> dict[tuple[Any, ...], tuple[Any, ...]]:
    return {
        tuple(row.get(key) for key in keys): tuple(row.get(value) for value in values)
        for row in summary.get("rows", [])
        if isinstance(row, dict)
    }


def _design(summary: dict[str, Any]) -> Any:
    return _row_map(summary, ("engine", "domain", "diagnostic"), ("status",))


def _scaling(summary: dict[str, Any]) -> Any:
    return _row_map(
        summary, ("engine", "domain", "diagnostic", "size"),
        ("status", "stable"),
    )


def _oracle(summary: dict[str, Any]) -> Any:
    return (
        summary.get("domain"), summary.get("rows"), summary.get("hourly_keys"),
        summary.get("daily_keys"), summary.get("all_equivalent"),
    )


def _boundary(summary: dict[str, Any]) -> Any:
    return _row_map(
        summary, ("domain",),
        ("rows", "injected", "rejected", "engine_rejected",
         "aggregates_unchanged", "pass"),
    )


def _mutation(summary: dict[str, Any]) -> Any:
    return (
        summary.get("pass"), summary.get("diagonal_caught"),
        summary.get("diagonal_total"),
        _row_map(
            summary, ("domain", "mutant", "diagnostic"),
            ("is_target", "detected", "vacuous", "seeded_events"),
        ),
    )


def _d7s(summary: dict[str, Any]) -> Any:
    return (
        summary.get("pass"), summary.get("separating_by_domain"),
        _row_map(summary, ("domain", "alpha", "kll_k"), ("separates",)),
    )


def _native(summary: dict[str, Any]) -> Any:
    topk = {
        (row.get("engine"), row.get("domain")): (
            row.get("status"), row.get("pass"),
            tuple((point.get("rows"), point.get("pass"))
                  for point in row.get("checkpoints", [])),
        )
        for row in summary.get("topk", []) if isinstance(row, dict)
    }
    watermark = {
        row.get("engine"): (row.get("status"), row.get("pass"))
        for row in summary.get("watermark", []) if isinstance(row, dict)
    }
    return summary.get("pass"), topk, watermark


def _disorder_sweep(summary: dict[str, Any]) -> Any:
    """The oracle exposure is deterministic given the inputs, so it must
    reproduce exactly; engine cells reproduce as contract verdicts."""
    rows = summary.get("rows") or []
    exposure = sorted(
        (r.get("domain"), r.get("grain"), str(r.get("window")), r.get("wrong_keys"))
        for r in rows if r.get("kind") == "exposure")
    engines = sorted(
        (r.get("engine"), r.get("domain"), r.get("grain"), str(r.get("window")),
         r.get("contract_wrong_keys") == 0)
        for r in rows if r.get("kind") == "engine" and r.get("status") == "measured")
    return {"exposure": exposure, "engines": engines}


def _parallelism_sweep(summary: dict[str, Any]) -> Any:
    """Drift counts at parallelism above one are nondeterministic by nature.
    What must reproduce is oracle equivalence at every level and, for the
    engines whose replays are bit-identical at parallelism one, zero drift
    there. RisingWave drifts at every level, parallelism one included, so its
    drift count is not compared."""
    rows = summary.get("rows") or []
    return sorted(
        (r.get("engine"), r.get("parallelism"), r.get("domain"),
         bool(r.get("oracle_equivalent")),
         (r.get("drift_keys") == 0)
         if r.get("parallelism") == 1
         and r.get("engine") in DETERMINISTIC_AT_PARALLELISM_ONE else None)
        for r in rows)


def _formulation_cost(summary: dict[str, Any]) -> Any:
    """Timings are host-specific and never compared; the measured cells are."""
    return sorted((r.get("engine"), r.get("domain"))
                  for r in summary.get("ratios") or [])


def _query_family_header(summary: dict[str, Any]) -> Any:
    return (
        summary.get("oracle_disagreements"), summary.get("blocked"),
        summary.get("expressible_cells"),
    )


def _query_family(summary: dict[str, Any]) -> Any:
    return _row_map(
        summary, ("engine", "domain", "grain", "shape", "delivery"),
        ("supported", "contract_equivalent", "arrival_equivalent",
         "attribution"))


def _cell_diff(
    official: Dict[Key, Tuple[Any, ...]], fresh: Dict[Key, Tuple[Any, ...]],
    tolerate: Callable[[Key, Tuple[Any, ...], Tuple[Any, ...]], bool],
) -> Tuple[List[Key], List[Key]]:
    """Return (mismatched, tolerated) cells. A cell is tolerated when it
    differs but `tolerate(key, official_value, fresh_value)` accepts the
    fresh value; a cell missing on either side is always a mismatch."""
    mismatched: List[Key] = []
    tolerated: List[Key] = []
    for key in sorted(set(official) | set(fresh), key=repr):
        left, right = official.get(key), fresh.get(key)
        if left == right:
            continue
        if left is not None and right is not None and tolerate(key, left, right):
            tolerated.append(key)
        else:
            mismatched.append(key)
    return mismatched, tolerated


def _tolerate_grid(key: Key, official: Tuple[Any, ...], fresh: Tuple[Any, ...]) -> bool:
    engine, _domain, diagnostic = key
    unstable = (engine, diagnostic) in REPLAY_UNSTABLE or official[0] == "repeat-unstable"
    return unstable and fresh[0] in GRID_UNSTABLE_STATUSES


def _tolerate_scaling(key: Key, official: Tuple[Any, ...], fresh: Tuple[Any, ...]) -> bool:
    engine, _domain, diagnostic, _size = key
    unstable = (engine, diagnostic) in REPLAY_UNSTABLE or official[1] is False
    return unstable and fresh[0] in SCALING_UNSTABLE_STATUSES


def _tolerate_family(key: Key, official: Tuple[Any, ...], fresh: Tuple[Any, ...]) -> bool:
    engine, _domain, _grain, shape, _delivery = key
    supported, _c, _a, attribution = fresh
    return ((engine, shape) in BATCH_DEPENDENT_FAMILY
            and supported == official[0]
            and attribution in FAMILY_SCORED
            and official[3] in FAMILY_SCORED)


def diff_design(official: dict[str, Any], fresh: dict[str, Any]) -> Tuple[List[Key], List[Key]]:
    return _cell_diff(_design(official), _design(fresh), _tolerate_grid)


def diff_scaling(official: dict[str, Any], fresh: dict[str, Any]) -> Tuple[List[Key], List[Key]]:
    return _cell_diff(_scaling(official), _scaling(fresh), _tolerate_scaling)


def diff_query_family(
    official: dict[str, Any], fresh: dict[str, Any],
) -> Tuple[bool, List[Key], List[Key]]:
    header_equal = _query_family_header(official) == _query_family_header(fresh)
    mismatched, tolerated = _cell_diff(
        _query_family(official), _query_family(fresh), _tolerate_family)
    return header_equal, mismatched, tolerated


def _throughput_ratios(
    official: dict[str, Any], fresh: dict[str, Any],
) -> dict[str, float | None]:
    def medians(summary: dict[str, Any]) -> dict[str, float]:
        values: dict[str, list[float]] = {}
        for row in summary.get("rows", []):
            engine = row.get("engine")
            throughput = row.get("throughput_eps")
            if isinstance(engine, str) and isinstance(throughput, (int, float)):
                values.setdefault(engine, []).append(float(throughput))
        return {engine: statistics.median(rows) for engine, rows in values.items()}

    expected = medians(official)
    observed = medians(fresh)
    return {
        engine: (observed[engine] / value
                 if engine in observed and value else None)
        for engine, value in sorted(expected.items())
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    failures, official_records, lock = verify_evidence(ROOT, require_lock=True)
    if failures or lock is None:
        for failure in failures:
            print(f"FAIL: {failure}")
        return 1
    official = {record["_block"]: record for record in official_records
                if record["_block"] != "oracle_verification"}
    official_oracles = {record["domain"]: record for record in official_records
                        if record["_block"] == "oracle_verification"}

    run_id = args.run_id
    try:
        fresh = {
            "design_space": _find("design_space_collect", f"{run_id}_grid50k"),
            "design_space_confirmation": _find(
                "design_space_collect", f"{run_id}_confirm1m"),
            "scaling": _find(
                "scaling_collect",
                tuple(invocation for invocation, _ in
                      stage_identities("scaling", run_id)),
            ),
            "boundary": _find("boundary", f"{run_id}_validation"),
            "mutation": _find("detection_power", f"{run_id}_validation"),
            "d7s_sweep": _find("d7s_sweep", f"{run_id}_validation"),
            "native_semantics": _find("native_semantics", f"{run_id}_validation"),
            "query_family": _find("query_family", f"{run_id}_validation"),
            "disorder_sweep": _find("disorder_sweep", f"{run_id}_validation"),
            "parallelism_sweep": _find(
                "parallelism_sweep", f"{run_id}_validation"),
            "formulation_cost": _find(
                "formulation_cost", f"{run_id}_validation"),
        }
        fresh_oracles = {
            domain: _find("oracle_verification", f"{run_id}_prepare", domain)
            for domain in sorted(official_oracles)
        }
    except ValueError as exc:
        print(f"FAIL: {exc}")
        return 1

    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, **detail: Any) -> None:
        entry: dict[str, Any] = {"name": name, "pass": passed}
        entry.update({k: v for k, v in detail.items() if v})
        checks.append(entry)
        if not passed:
            print(f"FAIL: reproduction check failed: {name}.")
            for cell in detail.get("mismatched") or []:
                print(f"      differs: {cell}")
        for cell in detail.get("tolerated") or []:
            print(f"      tolerated (documented nondeterminism): {name} {cell}")

    def compare(name: str, left: Any, right: Any) -> None:
        check(name, left == right)

    def cells(keys: List[Key]) -> List[List[Any]]:
        return [list(key) for key in keys]

    fresh_named = {**fresh, **{
        f"oracle_verification/{domain}": summary
        for domain, summary in fresh_oracles.items()
    }}
    for name, summary in fresh_named.items():
        block = name.split("/", 1)[0]
        compare(f"run/{name}/benchmark_id", run_id, summary.get("campaign_id"))
        identity = (summary.get("invocation_id"), summary.get("campaign_phase"))
        check(f"run/{name}/phase", identity in stage_identities(block, run_id))
        compare(f"run/{name}/contract", (2, False),
                (summary.get("contract_version"), summary.get("git_dirty")))

    all_fresh = list(fresh_named.values())
    for field in ("platform", "hostname", "machine"):
        values = {summary.get(field) for summary in all_fresh}
        check(f"run/shared_{field}", len(values) == 1 and None not in values)

    for block in ("design_space", "design_space_confirmation"):
        mismatched, tolerated = diff_design(official[block], fresh[block])
        check(block, not mismatched, mismatched=cells(mismatched),
              tolerated=cells(tolerated))
    mismatched, tolerated = diff_scaling(official["scaling"], fresh["scaling"])
    check("scaling", not mismatched, mismatched=cells(mismatched),
          tolerated=cells(tolerated))
    header_equal, mismatched, tolerated = diff_query_family(
        official["query_family"], fresh["query_family"])
    check("query_family", header_equal and not mismatched,
          mismatched=cells(mismatched), tolerated=cells(tolerated),
          header_mismatch=not header_equal)

    projections: dict[str, Callable[[dict[str, Any]], Any]] = {
        "boundary": _boundary,
        "mutation": _mutation,
        "d7s_sweep": _d7s,
        "native_semantics": _native,
        "disorder_sweep": _disorder_sweep,
        "parallelism_sweep": _parallelism_sweep,
        "formulation_cost": _formulation_cost,
    }
    for block, projection in projections.items():
        compare(block, projection(official[block]), projection(fresh[block]))
    for domain in sorted(official_oracles):
        compare(f"oracle/{domain}", _oracle(official_oracles[domain]),
                _oracle(fresh_oracles[domain]))

    official_first = official_records[0]
    for field in (
        "git_commit", "benchmark_config_sha256", "dependency_lock_sha256",
        "python_version",
    ):
        values = {summary.get(field) for summary in all_fresh}
        compare(f"provenance/{field}", official_first.get(field),
                next(iter(values)) if len(values) == 1 else None)

    passed = all(check["pass"] for check in checks)
    report = {
        "kind": "reproduction_report",
        "official_benchmark_run_id": lock["benchmark_run_id"],
        "official_results_lock_sha256": sha256_file(
            ROOT / "results" / "official_results.json"),
        "reproduction_run_id": run_id,
        "reproduction_host": {
            field: fresh["design_space"].get(field)
            for field in ("hostname", "platform", "machine", "processor",
                          "cpu_count", "memory_bytes")
        },
        "checks": checks,
        "tolerances": {
            "replay_unstable_cells": sorted(map(list, REPLAY_UNSTABLE)),
            "batch_dependent_family_members": sorted(
                map(list, BATCH_DEPENDENT_FAMILY)),
            "deterministic_at_parallelism_one": sorted(
                DETERMINISTIC_AT_PARALLELISM_ONE),
        },
        "throughput_ratio_fresh_over_official": _throughput_ratios(
            official["scaling"], fresh["scaling"]),
        "pass": passed,
        "artifact_valid": True,
        "note": (
            "Correctness and control outcomes are compared cell by cell; a "
            "cell the locked evidence documents as not repeatable is "
            "tolerated and listed. Throughput ratios are descriptive because "
            "reviewer hardware may differ."
        ),
    }
    output = RUNS / "benchmark_runs" / run_id / "reproduction_report.json"
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(f"{'PASS' if passed else 'FAIL'}: reviewer run compared with locked evidence.")
    print(f"output: {output}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
