"""Quantitative non-equivalence: how far a maintained table is from the contract.

WHY THIS EXISTS
---------------
`comparator.compare_bucket_tables` answers a yes/no question: is this key within
the contract's tolerance? That verdict is what the design-space map reports, and
it is deliberately binary -- an equivalence either holds or it does not.

But two engines that both fail a cell are not equally wrong. One may miss a
single key by two tolerance units; another may miss a third of the keys by six
orders of magnitude. The binary verdict cannot separate them, so the map reports
`divergence` for both and the reader cannot tell which engine is nearly right.

This module supplies the missing quantity. It is a conservative extension: the
verdict is recovered exactly as `extent > 0`, so nothing already reported
changes. What it adds is a graded reading of the same comparison.

THE MEASURE
-----------
The contract admits a key when every field lies within a magnitude-relative
tolerance (see comparator: `tol = epsilon * max(1, |a|, |b|)`). Rather than
inventing a scale, we measure deviation *in units of that same tolerance*:

    degree(a, b) = |a - b| / tol(a, b)

So `degree <= 1` is admitted, `degree = 3` is three times past the line, and the
quantity is dimensionless -- fares in dollars and particulate readings in
micrograms per cubic metre land on one scale, and it is comparable across
domains, fields, and engines without normalization.

A key's degree is the maximum over its metric fields, because the contract
admits a key only if *every* field is admitted. Two components then summarize a
comparison:

    extent     fraction of keys whose degree exceeds 1   (how many are wrong)
    magnitude  distribution of degree over those keys    (how wrong they are)

`extent` is exactly the Equivalence Mismatch Rate the comparator already
reports, so the existing measure is the first component of the new one.

EXACT VERSUS METRIC FIELDS
--------------------------
The contract has two kinds of field. Metric fields (sum, min, max, mean,
latest_value) carry a magnitude, so `degree` is meaningful. Exact fields (count,
latest_event_time) admit no tolerance at all: a count is right or it is wrong,
and calling a count that is off by one "one tolerance unit wrong" would be a
category error. Exact-field violations are therefore counted separately and
carry an unbounded degree -- they contribute to `extent` but are excluded from
the `magnitude` quantiles, which describe metric deviation only.

Reported by scripts/analysis/degree_report.py.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

import numpy as np

# The contract boundary, in the units this module measures. A key is admitted
# when its degree does not exceed this value.
ADMITTED = 1.0

METRIC_FIELDS = ["sum", "min", "max", "mean", "latest_value"]
EXACT_FIELDS = ["count", "latest_event_time"]


def tolerance(a: np.ndarray, b: np.ndarray, epsilon: float) -> np.ndarray:
    """The comparator's own magnitude-relative tolerance, reproduced exactly.

    Absolute below 1, relative above it. Kept identical to
    `comparator.compare_bucket_tables` so `degree > ADMITTED` and that
    function's mismatch mask agree key for key.
    """
    return epsilon * np.maximum(1.0, np.maximum(np.abs(a), np.abs(b)))


def field_degree(a: np.ndarray, b: np.ndarray, epsilon: float) -> np.ndarray:
    """Deviation of one metric field in units of the contract's tolerance.

    Non-finite values on either side are unbounded violations: the contract
    admits no NaN or infinity in a maintained aggregate, and no finite multiple
    of the tolerance describes the distance to one.
    """
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    finite = np.isfinite(a) & np.isfinite(b)
    out = np.full(a.shape, np.inf, dtype=float)
    if finite.any():
        tol = tolerance(a[finite], b[finite], epsilon)
        out[finite] = np.abs(a[finite] - b[finite]) / tol
    return out


def summarize(degrees: Sequence[float],
              *,
              exact_violations: int = 0,
              structural_violations: int = 0,
              total_keys: int) -> Dict[str, Any]:
    """Reduce per-key degrees to the two components of the measure.

    `degrees` holds one degree per overlapping key (max over metric fields).
    `exact_violations` counts keys failing an exact field, and
    `structural_violations` counts keys present on one side only or delivered
    twice; both are violations with no metric magnitude, so they raise `extent`
    without entering the quantiles.
    """
    d = np.asarray(list(degrees), dtype=float)
    metric_bad = d > ADMITTED if d.size else np.zeros(0, dtype=bool)
    violating = int(metric_bad.sum()) + exact_violations + structural_violations

    graded = d[metric_bad & np.isfinite(d)] if d.size else np.zeros(0)
    unbounded = int((np.isinf(d) & metric_bad).sum()) if d.size else 0

    result: Dict[str, Any] = {
        "extent": (violating / total_keys) if total_keys else 0.0,
        "violating_keys": violating,
        "total_keys": int(total_keys),
        "metric_violations": int(metric_bad.sum()) if d.size else 0,
        "exact_violations": int(exact_violations),
        "structural_violations": int(structural_violations),
        "unbounded_violations": unbounded,
        "magnitude": None,
    }
    if graded.size:
        result["magnitude"] = {
            "p50": float(np.percentile(graded, 50)),
            "p95": float(np.percentile(graded, 95)),
            "max": float(graded.max()),
            "geometric_mean": float(np.exp(np.log(graded).mean())),
        }
    return result


def locus(field_mismatches: Dict[str, int]) -> Dict[str, Any]:
    """Which fields of the contract a divergence actually touches.

    Recoverable from any summary.json already on disk, so the graded map can be
    built without re-running a cell. Says *where* a cell
    fails, which the binary verdict also discards, though not *how far* -- that
    needs `field_degree` at comparison time.
    """
    touched = {f: int(n) for f, n in (field_mismatches or {}).items() if int(n) > 0}
    metric = sorted(f for f in touched if f in METRIC_FIELDS)
    exact = sorted(f for f in touched if f in EXACT_FIELDS)
    # Two named shapes, both descriptive: they say which fields moved, never which
    # events were admitted -- one missing and one extra event cancel in the count.
    #
    # `count_involving` keys on `count` ALONE. EXACT_FIELDS also holds
    # latest_event_time, and a timestamp-only divergence moves no count, so
    # bool(exact) would mislabel it.
    #
    # `reduction_only` is the strict complement it is usually read against: the
    # divergence is confined to the additive reduction. A divergence in `min`,
    # `max`, or `latest_value` is neither shape, and both flags are False -- the
    # two do not partition the space, and callers must not treat "not
    # count_involving" as "reduction_only".
    return {
        "fields": dict(sorted(touched.items(), key=lambda kv: (-kv[1], kv[0]))),
        "metric_fields": metric,
        "exact_fields": exact,
        "breadth": len(touched),
        "count_involving": "count" in touched,
        "reduction_only": bool(touched) and set(touched) <= {"sum", "mean"},
    }
