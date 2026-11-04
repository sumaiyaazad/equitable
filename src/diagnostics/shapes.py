"""The order-sensitive query family (R2 generalization).

D1, D2 and D4 all score one view: a grouped aggregate whose per-bucket
``latest`` is chosen by a total order over the row content. The reported R2 gap
is therefore measured on one query shape under three interventions, and a
reader may reasonably ask whether the gap is a property of streaming engines or
of the one query we wrote.

This module answers that by enumerating the family the gap should hold over.
The rule every member instantiates:

    A query is order-sensitive when its answer depends on choosing one
    distinguished row from a group, and the engine's vocabulary offers two
    total orders to choose by: one derived from event content, and one derived
    from arrival order. The two coincide only when delivery is ordered.

In the contract those orders are ``(event_time_utc, dedupe_id)`` and
``processing_order``. Every shape below is that one substitution placed
somewhere different in the query plan, which is the axis that makes the family
a family rather than a list of rewrites:

    S1  latest per bucket          the distinguished row is the answer
    S2  first per bucket           the same selection, opposite direction
    S3  rank entities by latest    the answer becomes a sort key
    S5  keep buckets by latest     the answer becomes a predicate on membership

S1 is the shape the paper already reports; it is included so the generated
family contains its own baseline and the gap can be compared against it.

Every shape emits one of the two output schemas the comparator already
understands, so nothing downstream of the adapter changes: no new comparison
code, no new promotion rule, no new provenance. That constraint is deliberate
and shapes that would need a third schema were left out of this family.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Sequence, Tuple

import pandas as pd

#: Where the family can discriminate at all, measured on the frozen logs by
#: computing both orders with the oracle and comparing them. A shape can only
#: expose an order difference where a bucket folds more than one row, because
#: with one row per group there is no distinguished row to choose:
#:
#:     domain                  rows/group (hour, day)   discriminates
#:     transportation_taxi          7.9      66.8       both grains, 57-81%
#:     sports_football           5555.6   10000.0       both grains, 100%
#:     finance_markets             69.4    1666.7       both grains, 94-100%
#:     environment_airquality       1.0      10.3       DAY grain only, 81-99%
#:     datacenter_drives            1.0       1.0       never
#:
#: Backblaze telemetry reports each drive once per snapshot interval, so
#: (bucket, drive, metric) is unique at every grain this contract offers and no
#: choice of order can move its answer. Excluding it is a property of the
#: domain, not a limitation of the family, and the same reasoning excludes the
#: hourly grain of the air-quality log. Running those cells would spend engine
#: time to learn nothing, so the family declares its scope instead.
APPLICABLE: Dict[str, Tuple[str, ...]] = {
    "transportation_taxi": ("hourly", "daily"),
    "sports_football": ("hourly", "daily"),
    "finance_markets": ("hourly", "daily"),
    "environment_airquality": ("daily",),
    "datacenter_drives": (),
}

#: The two orders the rule contrasts. `contract` is invariant to replay
#: position; `arrival` is exactly the replay position, which is what
#: LATEST_BY_OFFSET, last() and offset-ordered DISTINCT ON express.
CONTRACT_ORDER = "contract"
ARRIVAL_ORDER = "arrival"
ORDERS = (CONTRACT_ORDER, ARRIVAL_ORDER)

#: Output schemas, both already produced and compared elsewhere in the harness.
BUCKET_SCHEMA = "bucket"
TOPK_SCHEMA = "topk"

BUCKET_COLUMNS = ["bucket", "entity", "metric", "count", "sum", "min", "max",
                  "mean", "latest_value", "latest_event_time"]
TOPK_COLUMNS = ["bucket", "metric", "rank", "entity", "rank_value"]


def _sort_keys(order: str) -> Tuple[List[str], List[bool]]:
    """The total order a shape selects its distinguished row by.

    The contract order is a function of row content alone, so it survives any
    permutation of delivery. The arrival order is the replay position, so it
    does not. Selecting by one or the other is the whole of the difference the
    family measures.
    """
    if order == CONTRACT_ORDER:
        return ["event_time_ts", "dedupe_id"], [True, True]
    if order == ARRIVAL_ORDER:
        return ["processing_order"], [True]
    raise ValueError(f"unknown order: {order!r}")


def _distinguished(frame: pd.DataFrame, keys: Sequence[str], order: str,
                   *, last: bool) -> pd.DataFrame:
    """One row per group, taken from the end or the start of `order`."""
    by, ascending = _sort_keys(order)
    ordered = frame.sort_values(list(by), ascending=ascending, kind="mergesort")
    grouped = ordered.groupby(list(keys), as_index=False, sort=True)
    return (grouped.tail(1) if last else grouped.head(1))


def _bucket_aggregate(frame: pd.DataFrame, keys: Sequence[str]) -> pd.DataFrame:
    """The order-insensitive half of the bucket table.

    count, sum, min, max and mean do not depend on either order, so they are
    computed once and shared by every bucket-schema shape. Only the
    distinguished row differs, which is precisely the property under test.
    """
    return (
        frame.groupby(list(keys), as_index=False, sort=True)["value"]
        .agg(count="count", sum="sum", min="min", max="max", mean="mean")
    )


def _bucket_table(frame: pd.DataFrame, order: str, *, last: bool) -> pd.DataFrame:
    keys = ["bucket", "entity", "metric"]
    aggregate = _bucket_aggregate(frame, keys)
    chosen = (
        _distinguished(frame, keys, order, last=last)[
            keys + ["value", "event_time_utc"]]
        .rename(columns={"value": "latest_value",
                         "event_time_utc": "latest_event_time"})
    )
    result = aggregate.merge(chosen, on=keys, how="left")
    return result[BUCKET_COLUMNS].sort_values(keys, kind="stable").reset_index(drop=True)


def _shape_latest(frame: pd.DataFrame, order: str, **_: Any) -> pd.DataFrame:
    """S1: the distinguished row is the answer."""
    return _bucket_table(frame, order, last=True)


def _shape_first(frame: pd.DataFrame, order: str, **_: Any) -> pd.DataFrame:
    """S2: the same selection taken from the other end of the order.

    Engines expose this as a separate primitive rather than as a parameter,
    EARLIEST_BY_OFFSET beside LATEST_BY_OFFSET and first() beside last(), so it
    exercises a different code path even though the rule is identical.
    """
    return _bucket_table(frame, order, last=False)


def _shape_rank_by_latest(frame: pd.DataFrame, order: str, *,
                          k: int = 10, **_: Any) -> pd.DataFrame:
    """S3: the distinguished row becomes a sort key.

    D6 ranks by mean, which no order can move. Ranking by the distinguished
    value makes the ranking itself order-sensitive, so a wrong selection stops
    being a wrong number in one column and becomes a wrong set of entities in
    a position a decision is read off.
    """
    keys = ["bucket", "entity", "metric"]
    chosen = _distinguished(frame, keys, order, last=True)[keys + ["value"]]
    chosen = chosen.rename(columns={"value": "rank_value"})
    # Ranking order matches src/diagnostics/d6.py exactly: value descending
    # with an entity-ascending tiebreak, so ranks are total and reproducible.
    ordered = chosen.sort_values(
        ["bucket", "metric", "rank_value", "entity"],
        ascending=[True, True, False, True], kind="mergesort")
    ordered["rank"] = ordered.groupby(["bucket", "metric"]).cumcount() + 1
    ordered = ordered[ordered["rank"] <= k]
    return ordered[TOPK_COLUMNS].reset_index(drop=True)


def _shape_filter_on_latest(frame: pd.DataFrame, order: str,
                            **_: Any) -> pd.DataFrame:
    """S5: the distinguished row becomes a predicate on group membership.

    Every other shape returns the same key set under both orders and differs
    only in a value. Here a wrong selection removes a group or admits one, so
    the divergence is structural rather than metric, and it is the one shape
    whose failure the key-union comparison catches directly.

    The threshold is the mean of the metric's admitted values, so it is a
    property of the input rather than a constant that suits one domain. A
    quantile would be a better splitter but is not portable: Materialize has no
    `WITHIN GROUP`, and a shape only belongs in the family if every engine can
    be asked the same question. The mean is order-insensitive either way, so it
    contributes nothing to the divergence the shape measures.
    """
    keys = ["bucket", "entity", "metric"]
    table = _bucket_table(frame, order, last=True)
    if table.empty:
        return table
    thresholds = (
        frame.groupby("metric", as_index=False, sort=True)["value"]
        .mean().rename(columns={"value": "_threshold"})
    )
    merged = table.merge(thresholds, on="metric", how="left")
    kept = merged[merged["latest_value"] >= merged["_threshold"]]
    return kept[BUCKET_COLUMNS].sort_values(keys, kind="stable").reset_index(drop=True)


@dataclass(frozen=True)
class Shape:
    """One position in the plan where the order-sensitivity rule can sit."""

    name: str
    schema: str
    plan_position: str
    oracle: Callable[..., pd.DataFrame]
    #: True when the two orders must agree on the key set. S5 is the exception:
    #: it can drop or admit a group, so its divergence is structural.
    key_set_stable: bool = True

    def expected(self, frame: pd.DataFrame, order: str, **params: Any) -> pd.DataFrame:
        return self.oracle(frame, order, **params)


SHAPES: Dict[str, Shape] = {
    "S1_latest": Shape(
        "S1_latest", BUCKET_SCHEMA,
        "the distinguished row is the answer", _shape_latest),
    "S2_first": Shape(
        "S2_first", BUCKET_SCHEMA,
        "the same selection, opposite direction", _shape_first),
    "S3_rank_by_latest": Shape(
        "S3_rank_by_latest", TOPK_SCHEMA,
        "the answer becomes a sort key", _shape_rank_by_latest),
    "S5_filter_on_latest": Shape(
        "S5_filter_on_latest", BUCKET_SCHEMA,
        "the answer becomes a predicate on membership",
        _shape_filter_on_latest, key_set_stable=False),
}

#: The shape whose gap the paper already reports. Kept in the family so the
#: generated result carries its own baseline.
BASELINE_SHAPE = "S1_latest"


def shape_names() -> List[str]:
    return sorted(SHAPES)


def enumerate_family(
    metrics: Sequence[str],
    grains: Sequence[str] = ("hourly", "daily"),
    shapes: Sequence[str] | None = None,
) -> List[Dict[str, Any]]:
    """Every (shape, metric, grain) member of the family, as query pairs.

    Enumerated rather than written one by one: that is what makes the reported
    gap a property of the family instead of a property of the queries we chose.
    Each member is one pair, the same shape read under both orders, and a pair
    is what a cell scores.
    """
    selected = list(shapes) if shapes is not None else shape_names()
    unknown = [name for name in selected if name not in SHAPES]
    if unknown:
        raise ValueError(f"unknown shape(s): {unknown}")
    if not metrics:
        raise ValueError("a family needs at least one metric")
    family: List[Dict[str, Any]] = []
    for shape in selected:
        for metric in metrics:
            for grain in grains:
                family.append({
                    "shape": shape,
                    "schema": SHAPES[shape].schema,
                    "metric": metric,
                    "grain": grain,
                    "orders": list(ORDERS),
                })
    return family


def gap(contract_matches: bool, arrival_matches: bool) -> str:
    """What one pair contributes to the R2 accounting.

    Mirrors collect_design_space._attribution so the generated family and the
    hand-written grid are read on one scale: a pair is attributable when both
    orders reproduce the oracle, repaired when only the contract order does,
    and divergent when the contract order itself fails.
    """
    if not contract_matches:
        return "repair_divergent"
    return "attributable" if arrival_matches else "repaired"


#: Attributions that mean the engine expressed the member and was scored.
SCORED_ATTRIBUTIONS = ("attributable", "repaired", "repair_divergent")


def control_verdict(rows) -> dict:
    """Separate a broken family construction from an engine's own divergence.

    Under ordered delivery the contract and arrival formulations coincide, so a
    correctly constructed member cannot show a gap. Every engine receives the
    same frames, so a construction error shows on every engine that expresses
    the member. The construction is therefore sound for a member when at least
    one engine expressing it is attributable there; a gap confined to other
    engines is those engines' divergence and is reported, not voided. A member
    only one engine expresses cannot be separated this way and counts against
    the construction.

    The earlier rule required every engine to be attributable everywhere, so
    one engine's wrong answer (Flink 2.2.1 keeps stale rows in a Top-N over a
    replaceable selection) failed validation and stopped the whole campaign.
    """
    members: dict = {}
    for row in rows:
        if row.get("delivery") != "ordered":
            continue
        if row.get("attribution") not in SCORED_ATTRIBUTIONS:
            continue
        key = (row.get("domain"), row.get("grain"), row.get("shape"))
        members.setdefault(key, []).append(row)
    failures = sorted(
        key for key, cells in members.items()
        if not any(c.get("attribution") == "attributable" for c in cells))
    gaps = sorted(
        (str(c.get("engine")), str(key[0]), str(key[1]), str(key[2]),
         str(c.get("attribution")))
        for key, cells in members.items() if key not in failures
        for c in cells if c.get("attribution") != "attributable")
    return {
        "construction_clean": not failures,
        "construction_failures": [list(k) for k in failures],
        "engine_gaps": [
            {"engine": e, "domain": d, "grain": g, "shape": sh, "attribution": a}
            for e, d, g, sh, a in gaps],
    }
