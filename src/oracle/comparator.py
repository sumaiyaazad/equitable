"""Field-by-field comparator: online engine tables vs. offline oracle tables.

Enforces the equivalence contract: exact value equality for integer/string fields
(count, latest_event_time, entity, metric, bucket) and epsilon-bounded equality
(default 1e-9, MAGNITUDE-RELATIVE) for floating-point fields (sum, min, max,
mean, latest_value). The float tolerance is `epsilon * max(1, |a|, |b|)`:
absolute below 1, relative above it. This is required because floating-point
addition is non-associative -- two correct independent recomputations of `sum`
differ by a few ULP, and at |v| ~ 1e7 one ULP already exceeds an absolute 1e-9.
Returns the per-field mismatch counts that feed the Equivalence Mismatch Rate
(EMR).
"""
from __future__ import annotations

from typing import Any, Dict, List

import numpy as np
import pandas as pd

from src.oracle import degree

KEY_COLUMNS = ["bucket", "entity", "metric"]
EXACT_FIELDS = ["count", "latest_event_time"]
FLOAT_FIELDS = ["sum", "min", "max", "mean", "latest_value"]


def compare_bucket_tables(
    online: pd.DataFrame,
    oracle: pd.DataFrame,
    *,
    epsilon: float = 1e-9,
) -> Dict[str, Any]:
    """Compare two bucket-aggregate tables key-by-key.

    Returns a summary dict: matched key count, keys only-in-online /
    only-in-oracle, and per-field mismatch counts (beyond epsilon for floats,
    exact for ints/strings)."""
    o = online[KEY_COLUMNS + EXACT_FIELDS + FLOAT_FIELDS].copy()
    r = oracle[KEY_COLUMNS + EXACT_FIELDS + FLOAT_FIELDS].copy()

    online_duplicate_keys = set(map(tuple, o.loc[o.duplicated(KEY_COLUMNS, keep=False), KEY_COLUMNS]
                                    .drop_duplicates().itertuples(index=False, name=None)))
    oracle_duplicate_keys = set(map(tuple, r.loc[r.duplicated(KEY_COLUMNS, keep=False), KEY_COLUMNS]
                                    .drop_duplicates().itertuples(index=False, name=None)))
    # A view contract requires one row per key. Keep one only to make the field
    # diff well-defined; duplicate keys remain structural mismatches below.
    o = o.drop_duplicates(KEY_COLUMNS, keep="first")
    r = r.drop_duplicates(KEY_COLUMNS, keep="first")

    merged = o.merge(r, on=KEY_COLUMNS, how="outer", suffixes=("_online", "_oracle"), indicator=True)

    only_online = int((merged["_merge"] == "left_only").sum())
    only_oracle = int((merged["_merge"] == "right_only").sum())
    both = merged[merged["_merge"] == "both"].copy()

    field_mismatches: Dict[str, int] = {}
    masks: List[pd.Series] = []
    exact_masks: List[pd.Series] = []
    # Per-field deviation in units of the tolerance, kept so the binary verdict
    # can be reported alongside how far past the boundary the failure sits.
    # Purely additive: nothing below reads these back into a mismatch decision.
    degree_columns: List[np.ndarray] = []
    for field in EXACT_FIELDS:
        m = both[f"{field}_online"] != both[f"{field}_oracle"]
        field_mismatches[field] = int(m.sum())
        masks.append(m)
        exact_masks.append(m)
    for field in FLOAT_FIELDS:
        a = both[f"{field}_online"].astype(float)
        b = both[f"{field}_oracle"].astype(float)
        # epsilon is MAGNITUDE-RELATIVE: absolute for |v| <= 1, scaled above it.
        # Floating-point addition is non-associative, so two *correct* independent
        # recomputations of `sum` differ by a few ULP. At |v| ~ 1e7 one ULP is
        # already ~1.9e-9, so a purely absolute 1e-9 is impossible to satisfy and
        # would flag correct engines. Relative epsilon keeps the contract tight
        # (a genuine aggregation error is orders of magnitude larger) while being
        # achievable in floating-point arithmetic.
        tol = epsilon * np.maximum(1.0, np.maximum(a.abs(), b.abs()))
        finite = np.isfinite(a) & np.isfinite(b)
        m = (~finite) | ((a - b).abs() > tol)
        field_mismatches[field] = int(m.sum())
        masks.append(m)
        if len(both):
            degree_columns.append(
                degree.field_degree(a.to_numpy(), b.to_numpy(), epsilon))

    field_bad_mask = (pd.concat(masks, axis=1).any(axis=1)
                      if masks and len(both) else pd.Series(False, index=both.index))
    # After duplicate removal, the outer merge has exactly one row per union key.
    # The three base mismatch categories are disjoint, so their counts can be
    # added without materializing million-element Python key sets. Duplicate
    # keys are exceptional and may overlap a base mismatch; only then construct
    # the small sets needed for exact union accounting.
    base_mismatched = int(field_bad_mask.sum()) + only_online + only_oracle
    if online_duplicate_keys or oracle_duplicate_keys:
        key_tuples = lambda frame: set(
            frame[KEY_COLUMNS].itertuples(index=False, name=None))
        base_bad_keys = (
            key_tuples(both.loc[field_bad_mask])
            | key_tuples(merged.loc[merged["_merge"] == "left_only"])
            | key_tuples(merged.loc[merged["_merge"] == "right_only"])
        )
        total_mismatched_keys = len(
            base_bad_keys | online_duplicate_keys | oracle_duplicate_keys)
    else:
        total_mismatched_keys = base_mismatched
    total_keys = int(len(merged))
    matched_keys = total_keys - total_mismatched_keys
    emr = (total_mismatched_keys / total_keys) if total_keys else 0.0

    # Graded reading of the same comparison. `extent` reproduces EMR exactly, so
    # this is a conservative extension: the verdict stays `mismatched_keys == 0`.
    if degree_columns:
        key_degree = np.max(np.vstack(degree_columns), axis=0)
        exact_only = (pd.concat(exact_masks, axis=1).any(axis=1).to_numpy()
                      if exact_masks else np.zeros(len(both), dtype=bool))
        # A key failing an exact field is a violation with no metric magnitude;
        # count it once, and only where no metric field already failed.
        exact_violations = int((exact_only & ~(key_degree > degree.ADMITTED)).sum())
    else:
        key_degree = np.zeros(0, dtype=float)
        exact_violations = 0
    structural = total_mismatched_keys - (
        int((key_degree > degree.ADMITTED).sum()) + exact_violations)
    non_equivalence = degree.summarize(
        key_degree,
        exact_violations=exact_violations,
        structural_violations=max(0, structural),
        total_keys=total_keys,
    )

    return {
        "matched_keys": matched_keys,
        "total_keys": total_keys,
        "non_equivalence": non_equivalence,
        "overlapping_keys": int(len(both)),
        "duplicate_online_keys": len(online_duplicate_keys),
        "duplicate_oracle_keys": len(oracle_duplicate_keys),
        "only_in_online": only_online,
        "only_in_oracle": only_oracle,
        "field_mismatches": field_mismatches,
        "mismatched_keys": total_mismatched_keys,
        "emr": emr,
        "epsilon": epsilon,
        "equivalent": total_mismatched_keys == 0,
    }


def engine_bucket_table(engine_rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """Turn ExactEngine.get_hourly()/get_daily() output into a comparable frame."""
    cols = KEY_COLUMNS + ["count"] + FLOAT_FIELDS + ["latest_event_time"]
    if not engine_rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(engine_rows)[cols]
