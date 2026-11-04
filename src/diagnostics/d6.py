"""D6: non-monotonic top-k maintenance.

Oracle-verified top-k. For each (bucket, metric) the top-k entities ranked by
`rank_metric` (default `mean`, config `d6.rank_metric`) are compared against the
exact oracle -- both as a SET (are the same k entities present?) and as an ordered
RANK (are they in the same order?). Existing streaming benchmarks measure top-k
throughput; none checks the top-k is externally *correct*. That verification is
D6's contribution.

"Non-monotonic": an entity's windowed mean rises and falls as rows accumulate, so
top-k membership is not monotone -- an engine must re-rank on every update, not
assume an entity, once in the top-k, stays. Ranking is by `rank_metric` DESC with
a deterministic `entity` ASC tiebreak, so ranks are total and reproducible.

Ties within tolerance. The contract (C3) accepts two numeric values as equal
when they differ by at most tau = epsilon * max(1, |a|, |b|). A ranking check that
compared exact values would contradict that: two means differing only in the
last bit, which C3 accepts, could swap places and fail D6 on every backend
including the reference (16.533333333333335 against 16.53333333333333 on
taxi at 200k rows and up). So when the oracle's full aggregate and epsilon
are given,
`compare_topk` treats two entities whose oracle values are within tau as tied and
accepts either order between them. The engine's entity at rank i must have an
oracle value tied with the oracle's value at rank i. Value accuracy itself stays
with C3; D6 checks membership and order only.

Scope: the portable D6 result derives top-k from every engine's maintained
aggregate, which keeps the seven-engine comparison uniform. The separate D6N benchmark run
in `scripts/run/run_native_semantics.py` creates engine-maintained Top-N views before
ingestion and compares their trajectory after each append checkpoint. Native claims are
made only for adapters whose D6N status is measured.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Tuple

import pandas as pd

TOPK_COLUMNS = ["bucket", "metric", "rank", "entity", "rank_value"]


def topk_table(bucket_df: pd.DataFrame, k: int, rank_metric: str) -> pd.DataFrame:
    """Top-k entities per (bucket, metric), ranked by `rank_metric` DESC with an
    `entity` ASC tiebreak. Returns TOPK_COLUMNS with rank in 1..k."""
    if bucket_df is None or bucket_df.empty:
        return pd.DataFrame(columns=TOPK_COLUMNS)
    if rank_metric not in bucket_df.columns:
        raise ValueError(f"rank_metric {rank_metric!r} not in view columns {list(bucket_df.columns)}")
    d = bucket_df[["bucket", "metric", "entity", rank_metric]].rename(
        columns={rank_metric: "rank_value"}).copy()
    # total order: value DESC, then entity ASC (deterministic tiebreak).
    d = d.sort_values(["bucket", "metric", "rank_value", "entity"],
                      ascending=[True, True, False, True], kind="mergesort")
    d["rank"] = d.groupby(["bucket", "metric"]).cumcount() + 1
    d = d[d["rank"] <= k]
    return d[TOPK_COLUMNS].reset_index(drop=True)


def _ordered_by_group(tk: pd.DataFrame) -> Dict[Tuple[Any, Any], List[str]]:
    out: Dict[Tuple[Any, Any], List[str]] = {}
    if tk.empty:
        return out
    for (b, m), sub in tk.sort_values("rank").groupby(["bucket", "metric"], sort=False):
        out[(b, m)] = sub["entity"].tolist()
    return out


def _rows_by_group(tk: pd.DataFrame) -> Dict[Tuple[Any, Any], List[Dict[str, Any]]]:
    """Same grouping as `_ordered_by_group`, but keeping rank and rank_value so a
    mismatch can be shown rather than only counted."""
    out: Dict[Tuple[Any, Any], List[Dict[str, Any]]] = {}
    if tk.empty:
        return out
    for (b, m), sub in tk.sort_values("rank").groupby(["bucket", "metric"], sort=False):
        out[(b, m)] = [
            {"rank": int(r["rank"]), "entity": str(r["entity"]),
             "rank_value": float(r["rank_value"])}
            for _i, r in sub.iterrows()
        ]
    return out


def _tied(a: float, b: float, epsilon: float) -> bool:
    """C3's symmetric tolerance: |a - b| <= epsilon * max(1, |a|, |b|).
    Non-finite values are never tied, as in the bucket comparator."""
    if not (math.isfinite(a) and math.isfinite(b)):
        return False
    return abs(a - b) <= epsilon * max(1.0, abs(a), abs(b))


def _oracle_values(oracle_view: pd.DataFrame, rank_metric: str
                   ) -> Dict[Tuple[Any, Any], Dict[str, float]]:
    """(bucket, metric) -> {entity: oracle rank value} over the FULL oracle
    aggregate, so an entity just outside the oracle's top-k can still be judged
    tied with the entity at the boundary."""
    out: Dict[Tuple[Any, Any], Dict[str, float]] = {}
    if oracle_view is None or oracle_view.empty:
        return out
    for b, m, e, v in oracle_view[["bucket", "metric", "entity", rank_metric]].itertuples(
            index=False, name=None):
        out.setdefault((b, m), {})[str(e)] = float(v)
    return out


def _consistent_up_to_ties(el: List[str], ol_values: List[float],
                           ov: Dict[str, float], epsilon: float) -> bool:
    """True when the engine ranking `el` is a valid ranking of the ORACLE values
    once values within tolerance count as tied: same length, no repeats, and the
    engine's entity at every rank has an oracle value tied with the oracle's value
    at that rank."""
    if len(el) != len(ol_values) or len(set(el)) != len(el):
        return False
    for i, entity in enumerate(el):
        if entity not in ov or not _tied(ov[entity], ol_values[i], epsilon):
            return False
    return True


def compare_topk(engine_tk: pd.DataFrame, oracle_tk: pd.DataFrame,
                 detail_limit: int = 2, *,
                 oracle_view: pd.DataFrame | None = None,
                 rank_metric: str = "mean",
                 epsilon: float | None = None) -> Dict[str, Any]:
    """Per (bucket, metric): SET equality of the top-k entities, and RANK (ordered)
    equality. Returns mismatch counts + set/rank EMR + an equivalent flag.

    With `oracle_view` (the full oracle aggregate) and `epsilon`, a group whose
    rankings differ only by the order of entities tied within tolerance, or by
    which of several tied entities fills the last rank, is equivalent and counted
    in `tie_resolved_groups`. Without them the comparison is exact, as before.

    Also returns `mismatch_detail`: up to `detail_limit` diverging groups with both
    rankings and their rank values, so a divergence can be reported as the actual
    two rankings instead of only a count. Detail is evidence, never a verdict --
    the counts above are unchanged by it.
    """
    e = _ordered_by_group(engine_tk)
    o = _ordered_by_group(oracle_tk)
    keys = set(e) | set(o)
    groups = len(keys)
    set_mm = 0
    rank_mm = 0
    tie_resolved = 0
    diverging: List[Tuple[Any, Any]] = []
    tie_aware = oracle_view is not None and epsilon is not None
    if tie_aware:
        ovals = _oracle_values(oracle_view, rank_metric)
        o_rows_all = _rows_by_group(oracle_tk)
    for key in keys:
        el = e.get(key, [])
        ol = o.get(key, [])
        if el == ol:
            continue
        if tie_aware:
            ol_values = [r["rank_value"] for r in o_rows_all.get(key, [])]
            if _consistent_up_to_ties(el, ol_values, ovals.get(key, {}), float(epsilon)):
                tie_resolved += 1
                continue
        if set(el) != set(ol):
            set_mm += 1
            rank_mm += 1           # a set mismatch is also a rank mismatch
            diverging.append(key)
        elif el != ol:
            rank_mm += 1
            diverging.append(key)
    detail: List[Dict[str, Any]] = []
    if diverging and detail_limit > 0:
        e_rows = _rows_by_group(engine_tk)
        o_rows = _rows_by_group(oracle_tk)
        for bucket, metric in sorted(diverging, key=lambda kv: (str(kv[0]), str(kv[1])))[:detail_limit]:
            detail.append({
                "bucket": str(bucket),
                "metric": str(metric),
                "engine": e_rows.get((bucket, metric), []),
                "oracle": o_rows.get((bucket, metric), []),
            })
    return {
        "groups": int(groups),
        "set_mismatches": int(set_mm),
        "rank_mismatches": int(rank_mm),
        "set_emr": (set_mm / groups) if groups else 0.0,
        "rank_emr": (rank_mm / groups) if groups else 0.0,
        "equivalent": bool(set_mm == 0 and rank_mm == 0),
        "tie_aware": bool(tie_aware),
        "tie_resolved_groups": int(tie_resolved),
        "mismatch_detail": detail,
    }
