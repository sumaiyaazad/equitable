#!/usr/bin/env python3
"""
collect_design_space.py -- aggregate diagnostic repeats per
(engine, domain, diagnostic) from runs/ into one design-space table.

Aggregation rule (this is the rule the paper states):

  A cell PASSES iff every selected repeat at the pinned workload scale passes.
  0 < repeats_pass < repeats  =>  status "repeat-unstable"  (reported, never rounded away).

Every collection first selects the newest explicit invocation_id and rejects
cross-invocation mixing. Within that benchmark run, --scope latest caps each cell
at --repeats; --scope all folds every repeat from that same run.
Blocked/error repeats and missing expected repeat indices remain non-verdict
evidence: they block the aggregate cell rather than disappearing behind the
available pass/fail repeats.

Each run is classified before aggregation, so three distinct outcomes are not
collapsed into one "fail":

  pass         the engine.s view matched the oracle under the strict comparator
  fail         the engine ran the view and DIVERGED from the oracle
  unsupported  the engine cannot express the view at all (e.g. Spark has no
               `restate` view) -- a capability result, not a correctness result
  blocked      the harness/engine errored before a comparison existed (adapter
               SQL rejected, container unreachable) -- carries no verdict

Workload scale is PINNED (--rows, default 50000) so the matrix is a single-scale
map; runs at other sizes belong to the scaling sweep (collect_scaling.py). D5 has
its own scale (--d5-rows, default 20000) because the crash/restart runner uses a
smaller log. Runs that errored carry no `rows` field and are always considered.

Inputs  : runs/*_<domain>_<engine>_d*/summary.json
Outputs : runs/<UTC>_design_space_collect/{summary.json,design_space.csv}
Runtime : instant (reads JSON only)
Run as  : python scripts/figures/collect_design_space.py
          python scripts/figures/collect_design_space.py --rows 200000
          python scripts/figures/collect_design_space.py --scope all
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from figures import load_module  # noqa: E402

_io = load_module("_io")
_oracle_provenance = load_module("_oracle_provenance")
all_runs_by_key = _io.all_runs_by_key
sort_diagnostics = _io.sort_diagnostics
sort_engines = _io.sort_engines

CSV_FIELDS = [
    "engine",
    "domain",
    "diagnostic",
    "status",
    "pass",
    "repeat-unstable",
    "repeats",
    "repeats_pass",
    "runs_unsupported",
    "runs_blocked",
    "runs_missing",
    "metric",
    "rows",
    "hourly_emr",
    "metric_min",
    "metric_max",
    "recovers",
    "known_finding",
    "known_finding_runs",
    "reason",
    "run_id",
]

#: Status precedence when a cell mixes outcomes across repeats.
STATUS_PASS = "pass"
STATUS_FAIL = "fail"
STATUS_REPEAT_UNSTABLE = "repeat-unstable"
STATUS_UNSUPPORTED = "unsupported"
STATUS_BLOCKED = "blocked"

MATRIX_DOMAINS = {
    "environment_airquality", "transportation_taxi", "datacenter_drives",
    "finance_markets", "sports_football",
}
MATRIX_DIAGNOSTICS = set(_io.CONTRACT_DIAGNOSTICS) - {"D5"}
D5_ENGINES = set(_io.ENGINE_ORDER)


def _git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def _metric_value(summary: Dict[str, Any]) -> Optional[float]:
    metric = summary.get("metric")
    if metric == "EMR":
        primary = summary.get("primary") or {}
        hourly = primary.get("hourly") or {}
        return hourly.get("emr")
    if metric == "DDR":
        return summary.get("ddr")
    if metric == "TOPK":
        hourly = summary.get("hourly") or {}
        return hourly.get("set_emr")
    if metric == "QER":
        bd = summary.get("bounded_deletion") or {}
        return bd.get("qer")
    if metric is None and summary.get("diagnostic") == "D5":
        recovery = summary.get("recovery") or {}
        hourly = recovery.get("hourly") or {}
        return hourly.get("emr")
    return None


def _known_finding(summary: Dict[str, Any]) -> Any:
    primary = summary.get("primary") or {}
    kf = primary.get("known_finding")
    if kf is None:
        kf = summary.get("known_finding")
    if kf is None:
        return False
    if isinstance(kf, dict):
        return kf.get("summary") or kf.get("id") or True
    return bool(kf)


def _classify(summary: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    """Classify ONE run as pass / fail / unsupported / blocked (+ reason)."""
    if summary.get("error") or summary.get("artifact_valid") is not True:
        reason = summary.get("error") or "run artifact is not valid"
        return STATUS_BLOCKED, str(reason)[:200]

    # An EMR diagnostic reports supportedness on its primary variant; TOPK/QER
    # report it at the top level.
    for holder in (summary.get("primary"), summary):
        if isinstance(holder, dict) and holder.get("supported") is False:
            return STATUS_UNSUPPORTED, holder.get("reason")

    # A run that produced no comparison at all is blocked, not a failure.
    if summary.get("pass") is not True:
        primary = summary.get("primary") or {}
        has_compare = bool(primary.get("hourly")) or any(
            k in summary for k in ("hourly", "ddr", "bounded_deletion", "recovery")
        )
        if not has_compare:
            return STATUS_BLOCKED, primary.get("reason") or summary.get("reason")
        return STATUS_FAIL, primary.get("reason") or summary.get("reason")
    return STATUS_PASS, None


def _scale_ok(summary: Dict[str, Any], rows: int, d5_rows: int) -> bool:
    """Keep only runs at the pinned workload scale for this diagnostic."""
    got = summary.get("rows")
    if got is None:
        return True  # errored/blocked run: no scale to check, still evidence
    want = d5_rows if summary.get("diagnostic") == "D5" else rows
    return int(got) == int(want)


def _d1_hourly_count_evidence(
    classified: List[Tuple[Dict[str, Any], str, Optional[str]]],
) -> List[Dict[str, Any]]:
    """Preserve the measured D1 count comparison for every selected repeat.

    Table 18 needs this detail to show the motivating duplicate-delivery defect.
    Keeping every repeat in the promoted collector output lets that table verify
    its identical-repeat claim without reading mutable run directories.
    """
    evidence: List[Dict[str, Any]] = []
    for summary, _, _ in classified:
        primary = (summary.get("primary") or {}).get("hourly") or {}
        contrast = (summary.get("contrast") or {}).get("hourly") or {}
        evidence.append({
            "run_id": summary.get("_run_id"),
            "repeat_index": summary.get("repeat_index"),
            "primary_total_keys": primary.get("total_keys"),
            "contrast_total_keys": contrast.get("total_keys"),
            "primary_count_mismatches": (
                primary.get("field_mismatches") or {}).get("count"),
            "contrast_count_mismatches": (
                contrast.get("field_mismatches") or {}).get("count"),
        })
    return evidence


def _cell_names_an_oracle(runs: List[Dict[str, Any]]) -> bool:
    """Whether any repeat of this cell got far enough to build an oracle."""
    return any(
        _classify(summary)[0] != STATUS_UNSUPPORTED for summary in runs)


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


# Fields the contract compares. The exact ones admit no tolerance at all, so a
# mismatch in any of them is a semantic disagreement about which rows were
# aggregated; the metric ones carry a magnitude, so a mismatch there may be
# either a semantic error or finite-precision drift, and only its size tells
# them apart.
EXACT_FIELDS = ("count", "latest_event_time")
METRIC_FIELDS = ("sum", "min", "max", "mean", "latest_value")


def _comparisons(summary: Dict[str, Any]):
    """Every key-by-key comparison one run produced, whatever shape it took.

    D1/D2/D4/D7 report under `primary`, D3 under `oracle_comparisons`, and D5
    under `recovery`. Yielding them uniformly keeps the divergence shape a
    property of the cell rather than of the diagnostic that produced it.
    """
    holders = [summary.get("primary"), summary.get("recovery"), summary]
    oracle_comparisons = summary.get("oracle_comparisons")
    if isinstance(oracle_comparisons, dict):
        holders.extend(
            replay for replay in oracle_comparisons.values()
            if isinstance(replay, dict))
    for holder in holders:
        if not isinstance(holder, dict):
            continue
        for grain in ("hourly", "daily"):
            comparison = holder.get(grain)
            if isinstance(comparison, dict) and "field_mismatches" in comparison:
                yield comparison


def _divergence_shape(runs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Which fields moved, whether any key went missing, and by how much.

    This is what separates a semantic divergence from finite-precision drift,
    and it has to be recorded HERE: the promoted block is the only thing the
    paper may read, so a classification recomputed later from runs/ would not
    be evidence the promotion gate ever saw. Degree is the comparator's own
    unit -- deviation measured in multiples of the contract tolerance -- so a
    value of 1 sits exactly on the boundary and 1e9 is a wholly wrong number.
    """
    fields: set[str] = set()
    structural = 0
    worst_degree = None
    for summary in runs:
        for comparison in _comparisons(summary):
            for field, count in (comparison.get("field_mismatches") or {}).items():
                if count:
                    fields.add(str(field))
            structural += int(comparison.get("only_in_online") or 0)
            structural += int(comparison.get("only_in_oracle") or 0)
            structural += int(comparison.get("duplicate_online_keys") or 0)
            structural += int(comparison.get("duplicate_oracle_keys") or 0)
            magnitude = (
                comparison.get("non_equivalence") or {}).get("magnitude") or {}
            observed = magnitude.get("max")
            if isinstance(observed, (int, float)) and math.isfinite(observed):
                worst_degree = (observed if worst_degree is None
                                else max(worst_degree, observed))
    return {
        "diverging_fields": sorted(fields),
        "diverging_exact_fields": sorted(fields & set(EXACT_FIELDS)),
        "diverging_metric_fields": sorted(fields & set(METRIC_FIELDS)),
        "structural_key_mismatches": structural,
        "max_degree": worst_degree,
    }


# R2, the attributable pass. An order-sensitive diagnostic runs two views over
# the same log: the REPAIR, which states the contract's event-time rule and is
# the only view ever scored, and the IDIOMATIC view, which keeps the repair's
# deduplication but takes the per-bucket latest in arrival order -- what
# LATEST_BY_OFFSET, last() and offset-ordered DISTINCT ON express, and what a
# query author writes when the engine's own documentation is the guide. Comparing
# the two is what separates "this engine computed the answer" from "this engine
# was handed the answer", which no pass mark can show on its own.
ATTRIBUTION_DIAGNOSTICS = ("D1", "D2", "D4")

ATTRIBUTION_ATTRIBUTABLE = "attributable"      # both views equal the oracle
ATTRIBUTION_REPAIRED = "repaired"              # only the supplied repair does
ATTRIBUTION_DIVERGENT = "repair_divergent"     # the repair itself diverges
ATTRIBUTION_INEXPRESSIBLE = "idiomatic_unsupported"


def _attribution(runs: List[Dict[str, Any]]) -> Optional[str]:
    """How much of this cell's verdict the benchmark's own formulation carries.

    Every repeat must agree, exactly as the verdict does: a cell is attributable
    only if the idiomatic view matched the oracle on all of them. Returns None
    for a diagnostic that runs no second view, so absence is never read as a
    result.
    """
    views = [s for s in runs if isinstance(s.get("idiomatic"), dict)]
    if not views:
        return None
    if any(s["idiomatic"].get("supported") is False for s in views):
        return ATTRIBUTION_INEXPRESSIBLE
    if not all((s.get("primary") or {}).get("pass") for s in views):
        return ATTRIBUTION_DIVERGENT
    if all(s["idiomatic"].get("pass") for s in views):
        return ATTRIBUTION_ATTRIBUTABLE
    return ATTRIBUTION_REPAIRED


def _aggregate_cell(
    runs: List[Dict[str, Any]],
    expected_repeat_indices: Optional[set[int]] = None,
) -> Dict[str, Any]:
    """Fold every repeat of one (engine, domain, diagnostic) cell into a row."""
    classified = [(s, *_classify(s)) for s in runs]
    verdicts = [(s, st) for s, st, _ in classified
                if st in (STATUS_PASS, STATUS_FAIL)]
    n_unsupported = sum(1 for _, st, _ in classified if st == STATUS_UNSUPPORTED)
    n_blocked = sum(1 for _, st, _ in classified if st == STATUS_BLOCKED)
    n_pass = sum(1 for _, st in verdicts if st == STATUS_PASS)
    n_verdict = len(verdicts)

    observed_repeat_indices = {
        int(s.get("repeat_index", 0)) for s, _, _ in classified
    }
    missing_repeat_indices = sorted(
        (expected_repeat_indices or set()) - observed_repeat_indices)

    if n_blocked or missing_repeat_indices:
        status = STATUS_BLOCKED
    elif n_unsupported and n_unsupported != len(classified):
        # Supportedness cannot legitimately change between repeats of one cell.
        # Treat a mixed capability observation as invalid evidence, not PASS.
        status = STATUS_BLOCKED
    elif n_verdict == 0:
        status = STATUS_UNSUPPORTED if n_unsupported else STATUS_BLOCKED
    elif n_pass == n_verdict:
        status = STATUS_PASS
    elif n_pass == 0:
        status = STATUS_FAIL
    else:
        status = STATUS_REPEAT_UNSTABLE

    metrics = [m for m in (_metric_value(s) for s, _, _ in classified)
               if isinstance(m, (int, float))]
    first = classified[0][0]
    input_sha256, oracle_cache_keys = _canonical_provenance(runs)

    reasons = [r for _, _, r in classified if r]
    if missing_repeat_indices:
        reasons.append(
            "missing expected repeat indices: "
            + ", ".join(map(str, missing_repeat_indices)))
    if n_unsupported and n_unsupported != len(classified):
        reasons.append(
            "supportedness changed across repeats in the same invocation")

    row = {
        "engine": first["engine"],
        "domain": first["domain"],
        "diagnostic": first["diagnostic"],
        "status": status,
        # Back-compatible boolean: a cell passes only if EVERY repeat passed.
        "pass": status == STATUS_PASS,
        "repeat_unstable": status == STATUS_REPEAT_UNSTABLE,
        "repeats": len(classified),
        "repeats_pass": n_pass,
        "runs_unsupported": n_unsupported,
        "runs_blocked": n_blocked,
        "runs_missing": len(missing_repeat_indices),
        "missing_repeat_indices": missing_repeat_indices,
        "metric": first.get("metric") or ("EMR" if first["diagnostic"] == "D5" else ""),
        "rows": first.get("rows"),
        "input_sha256": input_sha256,
        "oracle_cache_keys": oracle_cache_keys,
        "hourly_emr": statistics.median(metrics) if metrics else None,
        "metric_min": min(metrics) if metrics else None,
        "metric_max": max(metrics) if metrics else None,
        "recovers": first.get("recovers"),
        "known_finding": next(
            (kf for kf in (_known_finding(s) for s, _, _ in classified) if kf), False),
        "known_finding_runs": sum(1 for s, _, _ in classified if _known_finding(s)),
        **_divergence_shape(runs),
        "attribution": _attribution(runs),
        "reason": reasons[0] if reasons else None,
        "run_id": first["_run_id"],
        "run_ids": [s["_run_id"] for s, _, _ in classified],
    }
    if first["diagnostic"] == "D1":
        row["d1_hourly_count_evidence"] = _d1_hourly_count_evidence(classified)
    return row


def _artifact_flags(
    structurally_valid: bool,
    repeat_unstable_cells: List[Dict[str, Any]],
) -> Tuple[bool, bool]:
    """Keep complete repeat-unstable evidence promotable without calling it passing."""
    artifact_valid = bool(structurally_valid)
    return artifact_valid, artifact_valid and not repeat_unstable_cells



def _run_stamp(summary: Dict[str, Any]) -> str:
    """Sortable run timestamp: the run_id's leading UTC stamp (else started_utc)."""
    rid = str(summary.get("_run_id", ""))
    return rid.split("_", 1)[0] if rid[:8].isdigit() else str(summary.get("started_utc", ""))


def _run_time(summary: Dict[str, Any]) -> Optional[datetime]:
    stamp = _run_stamp(summary)
    try:
        if len(stamp) == 16 and stamp.endswith("Z"):
            return datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").replace(
                tzinfo=timezone.utc)
        return datetime.fromisoformat(stamp.replace("Z", "+00:00")).astimezone(
            timezone.utc)
    except (TypeError, ValueError):
        return None


def _scope_latest(
    runs: List[Dict[str, Any]],
    repeats: int,
    invocation_gap_hours: float,
) -> List[Dict[str, Any]]:
    """Keep the newest attempts without filtering away non-verdict outcomes.

    The caller has already restricted every cell to one explicit invocation_id,
    so time-gap inference is no longer needed for cohort isolation. The argument
    remains for CLI/output compatibility with older collections.
    """
    _ = invocation_gap_hours
    return sorted(runs, key=_run_stamp, reverse=True)[:repeats]


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


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rows", type=int, default=50_000,
                    help="Pinned workload size for the matrix (default 50000).")
    ap.add_argument("--d5-rows", type=int, default=20_000,
                    help="Pinned workload size for D5 crash/restart (default 20000).")
    ap.add_argument("--min-repeats", type=int, default=3,
                    help="Warn about cells with fewer repeats than this (default 3).")
    ap.add_argument("--repeats", type=int, default=3,
                    help="Repeats per cell to keep under --scope latest (default 3).")
    ap.add_argument("--scope", choices=["latest", "all"], default="latest",
                    help="'latest' (default): cap each cell at the latest "
                         "--repeats attempts from one invocation. 'all': fold "
                         "every repeat from that same selected invocation.")
    ap.add_argument("--invocation-gap-hours", type=float, default=12.0,
                    help="Deprecated compatibility option; explicit invocation "
                         "IDs now define cohort boundaries.")
    ap.add_argument("--invocation-id",
                    help="Collect this exact benchmark invocation instead of the newest one.")
    ap.add_argument(
        "--engines", default="",
        help="Comma-separated engines to require (default: all seven). A "
             "narrowed collection is exploratory and cannot be promoted as "
             "official evidence.")
    ap.add_argument(
        "--exclude-engines", default="",
        help="Comma-separated engines left out of this cohort, "
             "e.g. 'flink'. The exclusion is recorded in the summary as "
             "excluded_engines and is the ONLY way a narrowed universe can be "
             "promoted: an undeclared narrowing stays exploratory. The 50k "
             "design-space block never permits one.")
    args = ap.parse_args()
    engines = _resolve_engines(args.engines, _io.ENGINE_ORDER)
    excluded = {name.strip() for name in args.exclude_engines.split(",")
                if name.strip()}
    unknown_excluded = excluded - set(_io.ENGINE_ORDER)
    if unknown_excluded:
        ap.error(
            f"unknown engine(s) in --exclude-engines: {sorted(unknown_excluded)}")
    if excluded and args.engines.strip():
        ap.error("pass either --engines or --exclude-engines, not both")
    if excluded:
        engines = set(_io.ENGINE_ORDER) - excluded
        if not engines:
            ap.error("--exclude-engines cannot exclude every engine")
    if args.repeats < 1:
        ap.error("--repeats must be >= 1")
    if args.invocation_gap_hours <= 0:
        ap.error("--invocation-gap-hours must be > 0")

    grouped = all_runs_by_key("*_*_*_d*")
    if args.invocation_id:
        invocation_id = args.invocation_id
        grouped = {
            key: [s for s in summaries
                  if s.get("invocation_id") == args.invocation_id]
            for key, summaries in grouped.items()
            if any(s.get("invocation_id") == args.invocation_id
                   for s in summaries)
        }
    else:
        grouped, invocation_id = _io.restrict_to_latest_invocation(grouped)
    # A declared exclusion must remove the engine's CELLS, not merely narrow the
    # required set: a collection carrying rows outside its declared universe is
    # rejected by the promotion gate, and rightly so.
    grouped = {key: runs for key, runs in grouped.items() if key[0] in engines}
    if not grouped:
        print("FAIL: no current, fingerprinted diagnostic benchmark found under runs/*_*_*_d*/")
        print("  Run diagnostics first, e.g.:")
        print("    python scripts/run/run_diagnostic.py --engine reference --diagnostic all")
        return 1

    kept: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
    dropped = 0
    off_scale_only: List[Tuple[Tuple[Any, ...], List[int]]] = []
    for key, runs in grouped.items():
        # An interrupted shard may retry one repeat. Keep its newest completed
        # attempt so retries cannot inflate the scientific repeat count.
        attempts_by_cell: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
        for summary in runs:
            attempt = (
                summary.get("requested_max_rows", summary.get("rows")),
                summary.get("repeat_index", 0),
            )
            attempts_by_cell.setdefault(attempt, []).append(summary)
        # Newest valid attempt, falling back to the newest of any kind -- see
        # _io.best_attempt. Retries still cannot inflate the repeat count,
        # and a failed retry cannot replace a recorded verdict.
        runs = [
            chosen for chosen in (
                _io.best_attempt(attempts, _run_stamp)
                for attempts in attempts_by_cell.values()
            ) if chosen is not None
        ]
        in_scale = [s for s in runs if _scale_ok(s, args.rows, args.d5_rows)]
        dropped += len(runs) - len(in_scale)
        if in_scale:
            if args.scope == "latest" and not args.invocation_id:
                in_scale = _scope_latest(
                    in_scale, args.repeats, args.invocation_gap_hours)
            kept[key] = in_scale
        else:
            # A cell that exists only at some other workload size must be
            # announced, not silently dropped -- it looks identical to a cell
            # that was never run.
            sizes = sorted({s.get("rows") for s in runs if s.get("rows")})
            off_scale_only.append((key, sizes))

    if not kept:
        print(f"FAIL: no diagnostic runs at the pinned scale (rows={args.rows}, "
              f"D5 rows={args.d5_rows}).")
        return 1

    cohort = _io.cohort_metadata(
        summary for summaries in kept.values() for summary in summaries)
    input_inconsistent = [
        {"engine": key[0], "domain": key[1], "diagnostic": key[2]}
        for key, summaries in kept.items()
        if _canonical_provenance(summaries)[0] is None
    ]
    # An unsupported cell never built an expected table, so it has no oracle to
    # name. Requiring a cache key of it would turn a truthfully recorded
    # capability state into a promotion failure. See _scope.names_an_oracle.
    oracle_inconsistent = [
        {"engine": key[0], "domain": key[1], "diagnostic": key[2]}
        for key, summaries in kept.items()
        if _cell_names_an_oracle(summaries)
        and not _canonical_provenance(summaries)[1]
    ]
    expected_repeat_indices = set(range(1, args.repeats + 1))
    repeat_mismatch = [
        {"engine": key[0], "domain": key[1], "diagnostic": key[2],
         "repeat_indices": sorted({
             int(summary.get("repeat_index", 0)) for summary in summaries})}
        for key, summaries in kept.items()
        if invocation_id and {
            int(summary.get("repeat_index", 0)) for summary in summaries
        } != expected_repeat_indices
    ]
    cross_engine_input: List[Dict[str, Any]] = []
    cross_engine_oracle: List[Dict[str, Any]] = []
    for domain in MATRIX_DOMAINS:
        for diagnostic in MATRIX_DIAGNOSTICS | {"D5"}:
            cell_runs = [
                summary
                for (engine, got_domain, got_diagnostic), summaries in kept.items()
                if got_domain == domain and got_diagnostic == diagnostic
                for summary in summaries
            ]
            input_hashes = {summary.get("input_sha256") for summary in cell_runs}
            if len(input_hashes) > 1:
                cross_engine_input.append({
                    "domain": domain, "diagnostic": diagnostic})
            oracle_keys = {
                tuple(summary.get("oracle_cache_keys") or [])
                for summary in cell_runs
                if summary.get("oracle_cache_keys")
                and _classify(summary)[0] != STATUS_UNSUPPORTED
            }
            if len(oracle_keys) > 1:
                cross_engine_oracle.append({
                    "domain": domain, "diagnostic": diagnostic})
    rows = [
        _aggregate_cell(runs, expected_repeat_indices if invocation_id else None)
        for runs in kept.values()
    ]
    rows.sort(key=lambda r: (r["domain"], r["engine"], r["diagnostic"]))

    present = {(r["engine"], r["domain"], r["diagnostic"]) for r in rows}
    expected = {
        (engine, domain, diagnostic)
        for engine in engines
        for domain in MATRIX_DOMAINS
        for diagnostic in MATRIX_DIAGNOSTICS
    } | {
        (engine, domain, "D5")
        for engine in (engines & D5_ENGINES)
        for domain in MATRIX_DOMAINS
    }
    missing = sorted(expected - present)
    by_status = defaultdict(int)
    for r in rows:
        by_status[r["status"]] += 1
    thin = [r for r in rows if 0 < r["repeats"] < args.min_repeats]
    repeat_unstable = [r for r in rows if r["repeat_unstable"]]
    structurally_valid = (
        bool(rows)
        and cohort["cohort_consistent"]
        and not input_inconsistent
        and not oracle_inconsistent
        and not cross_engine_input
        and not cross_engine_oracle
        and not repeat_mismatch
        and not missing
        and not thin
        and not by_status.get(STATUS_BLOCKED)
    )
    artifact_valid, passed = _artifact_flags(structurally_valid, repeat_unstable)


    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    out_dir = ROOT / "runs" / f"{stamp}_design_space_collect"
    out_dir.mkdir(parents=True, exist_ok=True)

    summary = {
        "kind": "design_space_collect",
        **cohort,
        "started_utc": stamp,
        "pinned_rows": args.rows,
        "contract_version": 2,
        "git_dirty": _io.git_dirty(),
        "finalizer_commit": _io.finalizer_commit(),
        "pinned_d5_rows": args.d5_rows,
        "scope": args.scope,
        "repeats_kept": args.repeats if args.scope == "latest" else None,
        "invocation_gap_hours": (
            args.invocation_gap_hours if args.scope == "latest" else None),
        "runs_out_of_scale_dropped": dropped,
        "cells_only_off_scale": [
            {"engine": k[0], "domain": k[1], "diagnostic": k[2], "sizes_run": sizes}
            for k, sizes in sorted(off_scale_only, key=lambda x: tuple(map(str, x[0])))
        ],
        "aggregation_rule": (
            "a cell passes iff every selected repeat at the pinned scale passes; "
            "0 < repeats_pass < repeats is reported as status='repeat-unstable'; "
            "blocked/error or missing expected repeats block the cell, and mixed "
            "supportedness is invalid evidence"),
        "cells": len(rows),
        "source_runs": sum(len(summaries) for summaries in kept.values()),
        "input_inconsistent_cells": input_inconsistent,
        "oracle_inconsistent_cells": oracle_inconsistent,
        "cross_engine_input_inconsistent": cross_engine_input,
        "cross_engine_oracle_inconsistent": cross_engine_oracle,
        "repeat_mismatch_cells": repeat_mismatch,
        "cells_by_status": dict(by_status),
        "cells_below_min_repeats": len(thin),
        "min_repeats": args.min_repeats,
        "required_engines": sorted(engines),
        "excluded_engines": sorted(excluded),
        "expected_cells": len(expected),
        "missing_cells": [{"engine": e, "domain": d, "diagnostic": g} for e, d, g in missing],
        "all_cells_conform": all(r["status"] == STATUS_PASS for r in rows),
        "engines": sort_engines({r["engine"] for r in rows}),
        "domains": sorted({r["domain"] for r in rows}),
        "diagnostics": sort_diagnostics({r["diagnostic"] for r in rows}),
        "rows": rows,
        "artifact_valid": artifact_valid,
        "pass": passed,
    }
    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n"
    )

    with (out_dir / "design_space.csv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k) for k in CSV_FIELDS})

    print(f"PASS: collected {len(rows)} design-space cell(s) at rows={args.rows} "
          f"(D5 rows={args.d5_rows}); {dropped} out-of-scale run(s) excluded")
    for status in (STATUS_PASS, STATUS_FAIL, STATUS_REPEAT_UNSTABLE,
                   STATUS_UNSUPPORTED, STATUS_BLOCKED):
        if by_status.get(status):
            print(f"  {status:<12} {by_status[status]}")
    if repeat_unstable:
        print(f"  [warn] {len(repeat_unstable)} repeat-unstable cell(s) -- verdict not stable across repeats:")
        for r in repeat_unstable:
            print(f"         {r['engine']}/{r['domain']}/{r['diagnostic']}: "
                  f"{r['repeats_pass']}/{r['repeats']} passed")
    if thin:
        print(f"  [warn] {len(thin)} cell(s) below --min-repeats={args.min_repeats}")
    if off_scale_only:
        by_engine_diag = defaultdict(set)
        for (engine, _domain, diag), sizes in off_scale_only:
            by_engine_diag[(engine, diag)].update(sizes)
        print(f"  [warn] {len(off_scale_only)} cell(s) exist only off the pinned "
              f"scale and are absent from the matrix:")
        for (engine, diag), sizes in sorted(by_engine_diag.items()):
            print(f"         {engine}/{diag}: only at rows={sorted(sizes)} "
                  f"-- rerun at the pinned scale to fill this cell")
    print(f"output: {out_dir / 'summary.json'}")
    print(f"output: {out_dir / 'design_space.csv'}")
    return 0 if summary["artifact_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
