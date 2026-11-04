"""D7: retraction-consistent summarization (docs/SEMANTICS.md, Restatements).

Two halves.

Exact (EMR). Drive an engine over the `d7_restate` fixture and compare its
bucket view to the restatement-aware oracle (each base observation resolved
through its `supersedes` pointer; valid fixtures permit at most one restatement
per base observation). An engine that treats a restatement as a fresh insert
double-counts and fails; the `restate` view (retract-latest) recovers
equivalence. `run_d7_exact` runs one variant; `run_d7_design_cell` runs the
naive/restate pair to produce the design-space finding.

Approximate (QER). `run_d7_sketch`, with reference sketches in
`src/diagnostics/sketches.py`, replays the d7 fixture per (day, metric) group
through two maintained quantile summaries: an insert-only KLL (a restatement's
superseded value is retained) and a bounded-deletion KLL± (the superseded value
is retracted). Each is queried at the config quantiles and scored against the
exact restatement-corrected multiset; QER is the fraction of (group, quantile)
queries whose normalized rank error exceeds `sketch.rank_tol`. The D7 fixture
perturbs values only (a restatement keeps entity/metric/event_time), so the QER
signal lives in the quantile domain; the SpaceSaving± frequent-items reference
ships alongside and is validated by `sketches.self_check` (recorded in the run
summary).
"""
from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from src.adapters import EngineAdapter, canonical_input
from src.oracle.comparator import compare_bucket_tables
from src.oracle.pandas_oracle import build_oracle_tables_from_rows

#: The QER / sketch half landed with src/diagnostics/sketches.py.
SKETCH_HALF_IMPLEMENTED = True


def _restatement_oracle(df: pd.DataFrame) -> Dict[str, Any]:
    """Restatement-aware ground truth over the same rows the engine ingests."""
    return build_oracle_tables_from_rows(df.to_dict("records"), restatement_aware=True, include_quantiles=False)


def run_d7_exact(
    adapter: EngineAdapter,
    fixture: "str | pd.DataFrame",
    *,
    variant: str = "restate",
    epsilon: float = 1e-9,
) -> Dict[str, Any]:
    """Exact-mode D7: ingest the d7 fixture under `variant`, compare hourly+daily
    to the restatement-aware oracle. Returns EMR summaries + a pass flag.

    `fixture` is a d7_restate.parquet path or an already-loaded frame. `variant`
    is typically "restate" (expected PASS) or "dedup" (expected FAIL: the naive
    view double-counts restatements)."""
    df = pd.read_parquet(fixture) if isinstance(fixture, str) else fixture
    df = canonical_input(df, allow_restatements=True)
    adapter.options["allow_restatements"] = True

    if variant not in adapter.supported_variants:
        return {
            "engine": adapter.name, "variant": variant, "rows": int(len(df)),
            "supported": False,
            "reason": f"{adapter.name} does not implement the {variant!r} view",
            "pass": False,
        }

    oracle = _restatement_oracle(df)
    with adapter.connect() as eng:
        eng.setup(variant=variant)
        stats = eng.ingest_log(df)
        engine_hourly = eng.read_view("hourly")
        engine_daily = eng.read_view("daily")

    cmp_hourly = compare_bucket_tables(engine_hourly, oracle["hourly"], epsilon=epsilon)
    cmp_daily = compare_bucket_tables(engine_daily, oracle["daily"], epsilon=epsilon)
    return {
        "engine": adapter.name,
        "variant": variant,
        "rows": int(len(df)),
        "supported": True,
        "epsilon": epsilon,
        "ingest": stats.as_dict(),
        "hourly": cmp_hourly,
        "daily": cmp_daily,
        "pass": bool(cmp_hourly["equivalent"] and cmp_daily["equivalent"]),
    }


def run_d7_design_cell(
    adapter: EngineAdapter, fixture: "str | pd.DataFrame", *, epsilon: float = 1e-9
) -> Dict[str, Any]:
    """Run the naive (`dedup`) and patched (`restate`) views on the d7 fixture to
    produce one design-space cell: the naive view is expected to FAIL (restated
    values double-counted), the retract-latest patch to PASS. `smallest_patch`
    names the recovering change (the DISTINCT-ON-cell view)."""
    df = pd.read_parquet(fixture) if isinstance(fixture, str) else fixture
    df = canonical_input(df, allow_restatements=True)
    out: Dict[str, Any] = {"engine": adapter.name, "rows": int(len(df))}
    for variant in ("dedup", "restate"):
        # fresh adapter instance per variant keeps engine state isolated
        inst = type(adapter)(**adapter.options)
        out[variant] = run_d7_exact(inst, df, variant=variant, epsilon=epsilon)
    out["recovers"] = bool(
        not out["dedup"].get("pass", False) and out["restate"].get("pass", False)
    ) if out["restate"].get("supported") and out["dedup"].get("supported") else None
    out["smallest_patch"] = "resolve supersedes ids before aggregation"
    return out


def run_d7_sketch(
    fixture: "str | pd.DataFrame",
    *,
    kll_k: int = 200,
    quantiles: "tuple[float, ...]" = (0.5, 0.9, 0.99),
    rank_tol: float = 0.01,
    min_group: int = 20,
    seed: int = 1337,
) -> Dict[str, Any]:
    """QER / approximate half of D7 (docs/SEMANTICS.md, Restatements).

    Replays the d7_restate fixture in processing_order, per (day-bucket, metric)
    group, through two maintained quantile summaries:

      insert_only       -- KLL that treats every row (incl. restatements) as an
                           insert: the superseded value is silently retained.
      bounded_deletion  -- KLL± that, on a restatement, retracts the superseded
                           value and inserts the corrected one.

    Both are queried at `quantiles` and scored against the exact
    restatement-corrected multiset of the group, control-relative: a third,
    same-seed KLL (the control) is fed the corrected values directly, so its
    tie-aware rank error is the baseline sketch noise on that distribution.
    A query violates when the variant's error exceeds the control's by more
    than `rank_tol` -- i.e. only *retraction-induced* error counts, never the
    noise any KLL of this size has on the domain's value distribution
    (discrete sensor readings are heavily tied). QER = violations / queries.
    PASS iff the bounded-deletion sketch's QER is 0 (the KLL± guarantee holds
    after retraction); `recovers` reports the design cell (insert-only fails,
    KLL± passes -- how visibly depends on alpha vs rank_tol per domain; the
    crisp exact-mode cell is D7's EMR half). This diagnostic tests the
    reference summaries themselves (the oracle for condition (7)), so it
    is engine-independent -- the runner executes it once per domain, on the
    reference engine's row."""
    from src.diagnostics import sketches

    df = pd.read_parquet(fixture) if isinstance(fixture, str) else fixture
    df = canonical_input(df, allow_restatements=True).sort_values("processing_order", kind="stable")

    day = pd.to_datetime(df["event_time_utc"], utc=True).dt.strftime("%Y-%m-%d")
    groups = list(zip(day, df["metric"]))

    ins_only: Dict[Any, sketches.KLLSketch] = {}
    bounded: Dict[Any, sketches.BoundedDeletionKLL] = {}
    authoritative: Dict[str, float] = {}      # base dedupe_id -> current value
    group_of: Dict[str, Any] = {}             # base dedupe_id -> group key
    arrival: Dict[Any, List[str]] = {}        # group -> base ids in arrival order
    seen: set = set()                          # source-identity dedup (condition 1)
    n_restatements = 0

    for key, dedupe_id, sup, value in zip(
            groups, df["dedupe_id"], df["supersedes"], df["value"]):
        if dedupe_id in seen:
            continue
        seen.add(dedupe_id)
        if key not in ins_only:
            ins_only[key] = sketches.KLLSketch(kll_k, seed=seed)
            bounded[key] = sketches.BoundedDeletionKLL(kll_k, seed=seed)
        if sup is None or (isinstance(sup, float) and pd.isna(sup)):
            # base observation
            ins_only[key].update(value)
            bounded[key].update(value)
            authoritative[dedupe_id] = value
            group_of[dedupe_id] = key
            arrival.setdefault(key, []).append(dedupe_id)
        else:
            # restatement: consumed, replaces the superseded observation's value
            n_restatements += 1
            old = authoritative.get(str(sup))
            gkey = group_of.get(str(sup), key)
            ins_only[gkey].update(value)               # retains the old value
            if old is not None:
                bounded[gkey].delete(old)              # retract, then apply
            bounded[gkey].update(value)
            authoritative[str(sup)] = value

    # exact corrected multiset per group
    corrected: Dict[Any, List[float]] = {}
    for dedupe_id, value in authoritative.items():
        corrected.setdefault(group_of[dedupe_id], []).append(value)
    for vals in corrected.values():
        vals.sort()

    results = {name: {"queries": 0, "violations": 0, "max_rank_err": 0.0,
                      "max_excess_err": 0.0, "sum_rank_err": 0.0}
               for name in ("insert_only", "bounded_deletion")}
    scored_groups = 0
    for key, vals in corrected.items():
        if len(vals) < min_group:
            continue
        scored_groups += 1
        # Control: the same sketch fed the corrected values directly (in base-row
        # arrival order) -- its error is the baseline KLL noise on this group's
        # value distribution, which the variants must not be blamed for.
        control = sketches.KLLSketch(kll_k, seed=seed)
        for dedupe_id in arrival[key]:
            control.update(authoritative[dedupe_id])
        for q in quantiles:
            ctrl_err = sketches.quantile_rank_error(vals, control.quantile(q), q)
            for name, sk in (("insert_only", ins_only[key]),
                             ("bounded_deletion", bounded[key])):
                est = sk.quantile(q)
                err = sketches.quantile_rank_error(vals, est, q)
                excess = max(0.0, err - ctrl_err)
                r = results[name]
                r["queries"] += 1
                r["sum_rank_err"] += err
                r["max_rank_err"] = max(r["max_rank_err"], err)
                r["max_excess_err"] = max(r["max_excess_err"], excess)
                if excess > rank_tol:
                    r["violations"] += 1

    for r in results.values():
        r["qer"] = (r["violations"] / r["queries"]) if r["queries"] else 0.0
        r["mean_rank_err"] = (r["sum_rank_err"] / r["queries"]) if r["queries"] else 0.0
        del r["sum_rank_err"]

    bounded_ok = results["bounded_deletion"]["violations"] == 0
    recovers = bool(results["insert_only"]["violations"] > 0 and bounded_ok and scored_groups > 0)
    return {
        "metric": "QER",
        "rows": int(len(df)),
        "restatements": int(n_restatements),
        "kll_k": int(kll_k),
        "quantiles": list(quantiles),
        "rank_tol": float(rank_tol),
        "min_group": int(min_group),
        "groups_total": int(len(corrected)),
        "groups_scored": int(scored_groups),
        "insert_only": results["insert_only"],
        "bounded_deletion": results["bounded_deletion"],
        "sketch_self_check": sketches.self_check(seed),
        "recovers": recovers,
        "pass": recovers,
    }
