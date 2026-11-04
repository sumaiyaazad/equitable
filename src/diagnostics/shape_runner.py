"""Score one member of the order-sensitive query family on one engine.

A member is a pair: the same shape read under the contract order and under
arrival order. Scoring the pair is what separates what the engine computes from
what the question supplied, which is requirement R2 measured over a family
rather than over the single shape D1, D2 and D4 share.

The verdict vocabulary is the grid's, from
collect_design_space._attribution, so the generated family and the
hand-written grid can be read on one scale:

    attributable      both orders reproduce the oracle
    repaired          only the contract order does
    repair_divergent  the contract order itself diverges
    unsupported       the adapter cannot express the shape under both orders

An oracle disagreement is a harness error, never an engine finding, exactly as
in the scored path.
"""
from __future__ import annotations

import time
from typing import Any, Dict

import numpy as np
import pandas as pd

from src.diagnostics.shapes import (ARRIVAL_ORDER, CONTRACT_ORDER, SHAPES,
                                    TOPK_SCHEMA, gap)
from src.oracle import shape_oracle
from src.adapters.base import canonical_shape_frame
from src.oracle.comparator import compare_bucket_tables


def _prepare(df: pd.DataFrame, unit: str) -> pd.DataFrame:
    """The oracle's view of the input: bucketed, with a comparable timestamp."""
    out = df.copy()
    out["event_time_ts"] = pd.to_datetime(
        out["event_time_utc"], utc=True, format="mixed")
    out["bucket"] = out["event_time_ts"].dt.floor("h" if unit == "hourly" else "D")
    return out


def _compare_topk(engine: pd.DataFrame, oracle: pd.DataFrame,
                  epsilon: float) -> Dict[str, Any]:
    """Rank equality for the top-k schema, as D6 defines it.

    A rank is wrong if it holds a different entity or a different value, so
    both are checked; comparing only the entity would miss a selection that
    moved the value without moving the order. A value differs when it lies
    outside the contract's tolerance, epsilon * max(1, |e|, |o|), exactly as
    the scored comparator decides. Bit equality would be wrong here: an engine
    can return a double one unit in the last place off the oracle's (41.8642
    against 41.864200000000004), which the contract accepts and the family
    would then wrongly score as repair_divergent.
    """
    keys = ["bucket", "metric", "rank"]
    merged = engine.merge(oracle, on=keys, how="outer",
                          suffixes=("_e", "_o"), indicator=True)
    only_one_side = int((merged["_merge"] != "both").sum())
    both = merged[merged["_merge"] == "both"]
    entity_differs = (both["entity_e"].astype(str)
                      != both["entity_o"].astype(str)).to_numpy()
    e = both["rank_value_e"].astype(float).to_numpy()
    o = both["rank_value_o"].astype(float).to_numpy()
    value_differs = np.abs(e - o) > epsilon * np.maximum(
        1.0, np.maximum(np.abs(e), np.abs(o)))
    entity_moved = int(entity_differs.sum())
    value_moved = int(value_differs.sum())
    mismatched = only_one_side + int((entity_differs | value_differs).sum())
    total = int(len(merged))
    return {
        "equivalent": mismatched == 0,
        "total_keys": total,
        "mismatched_keys": mismatched,
        "only_in_one": only_one_side,
        "entity_mismatches": entity_moved,
        "value_mismatches": value_moved,
        "emr": (mismatched / total) if total else 0.0,
    }


#: The bucket-schema fields the selection order actually determines. count,
#: sum, min, max and mean cannot move under either order, so including them
#: would make the family re-measure the reduction findings the main grid
#: already reports, and on an engine with a known numeric divergence that
#: signal would swamp the order signal entirely. Restricting the comparison to
#: the selected row plus the key set is what keeps this experiment about
#: order-sensitivity.
ORDER_DETERMINED_FIELDS = ("latest_value", "latest_event_time")


def _compare(schema: str, engine: pd.DataFrame, oracle: pd.DataFrame,
             epsilon: float, grain: str) -> Dict[str, Any]:
    """Compare after putting BOTH sides through the adapters' canonicalizer.

    An oracle frame carries a timestamp bucket and an engine frame a canonical
    string, so comparing them raw fails on dtype before it can fail on content.
    Canonicalizing both is what makes the comparison about the answer.
    """
    left = canonical_shape_frame(engine, grain)
    right = canonical_shape_frame(oracle, grain)
    if schema == TOPK_SCHEMA:
        return _compare_topk(left, right, epsilon)
    # The contract's own comparator, unmodified, so the key-union rule and the
    # epsilon are exactly the scored ones. The family verdict then reads only
    # the part of its output the selection order governs.
    full = compare_bucket_tables(left, right, epsilon=epsilon)
    fields = full.get("field_mismatches") or {}
    structural = (int(full.get("only_in_online", 0))
                  + int(full.get("only_in_oracle", 0))
                  + int(full.get("duplicate_online_keys", 0))
                  + int(full.get("duplicate_oracle_keys", 0)))
    order_mismatches = sum(int(fields.get(f, 0)) for f in ORDER_DETERMINED_FIELDS)
    return {
        **full,
        "equivalent": structural == 0 and order_mismatches == 0,
        "contract_equivalent_all_fields": bool(full.get("equivalent")),
        "order_field_mismatches": order_mismatches,
        "structural_mismatches": structural,
    }


def run_shape_pair(adapter, df: pd.DataFrame, *, shape: str, grain: str,
                   epsilon: float = 1e-9, k: int = 10,
                   quantile: float = 0.5) -> Dict[str, Any]:
    """Execute both arms of one family member and return its cell."""
    if shape not in SHAPES:
        raise ValueError(f"unknown shape: {shape!r}")
    definition = SHAPES[shape]
    if shape not in getattr(adapter, "supported_shapes", ()):
        return {"shape": shape, "grain": grain, "schema": definition.schema,
                "supported": False, "attribution": "unsupported",
                "reason": f"{adapter.name} cannot express {shape}"}

    prepared = _prepare(df, grain)
    params = {"k": k, "quantile": quantile}
    # ONE oracle, the contract's. Both arms are scored against it, exactly as
    # the paper's repair and arrival-ordered formulations are. Scoring each arm
    # against an oracle computed under its own order asks whether the engine
    # executes each formulation faithfully, which is a different and much
    # weaker question: it calls every arm correct and reports no gap at all.
    expected = definition.expected(prepared, CONTRACT_ORDER, **params)
    sql_expected = shape_oracle.expected(prepared, shape, CONTRACT_ORDER, **params)
    agreement = _compare(definition.schema, expected, sql_expected, epsilon, grain)
    if not agreement["equivalent"]:
        return {"shape": shape, "grain": grain, "schema": definition.schema,
                "supported": True, "attribution": None,
                "oracle_disagreement": True,
                "reason": "the pandas and SQL shape oracles disagree; "
                          "this is a harness error, not an engine finding"}

    arms: Dict[str, Any] = {}
    for order in (CONTRACT_ORDER, ARRIVAL_ORDER):
        started = time.perf_counter()
        adapter.setup_shape(shape=shape, order=order)
        adapter.ingest_log(df)
        observed = adapter.read_shape(grain)
        elapsed = time.perf_counter() - started
        comparison = _compare(definition.schema, observed, expected,
                              epsilon, grain)
        arms[order] = {"comparison": comparison, "seconds": round(elapsed, 3),
                       "rows": int(len(observed))}

    contract_ok = bool(arms[CONTRACT_ORDER]["comparison"]["equivalent"])
    arrival_ok = bool(arms[ARRIVAL_ORDER]["comparison"]["equivalent"])
    return {
        "shape": shape, "grain": grain, "schema": definition.schema,
        "plan_position": definition.plan_position, "supported": True,
        "oracle_disagreement": False,
        "contract_equivalent": contract_ok,
        "arrival_equivalent": arrival_ok,
        "attribution": gap(contract_ok, arrival_ok),
        "arms": arms,
    }
