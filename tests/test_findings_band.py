"""Acceptance-band regression tests for the materialize known-finding matcher.

The band decides whether a strict-epsilon failure may be attributed to the
documented float32 finding instead of being reported as a divergence. Every
case here is one where a looser band would certify the signature without
having evidence for it.
"""
from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from src.diagnostics import findings
from src.diagnostics.findings import KNOWN_FINDINGS, _within_accumulation_band

U = findings.FLOAT32_ULP
MZ = KNOWN_FINDINGS["materialize"]


def frame(buckets, sums, counts, means=None):
    means = means if means is not None else [s / max(c, 1) for s, c in zip(sums, counts)]
    return pd.DataFrame({
        "bucket": buckets,
        "entity": ["e"] * len(buckets),
        "metric": ["m"] * len(buckets),
        "count": counts,
        "sum": sums,
        "mean": means,
    })


class AccumulationBandTests(unittest.TestCase):
    def test_disjoint_keys_do_not_certify_the_finding(self) -> None:
        # No key survives the join, so no value is ever compared. Certifying the
        # signature from zero evidence would be wrong.
        e = frame(["b1"], [1.0], [1])
        o = frame(["b2"], [1.0], [1])
        self.assertFalse(_within_accumulation_band(MZ, e, o))

    def test_nan_is_rejected_rather_than_silently_admitted(self) -> None:
        # NaN compares False against every bound, so it would slip through an
        # `(a - b).abs() > tol` test and be reported as a documented finding.
        e = frame(["b1"], [float("nan")], [1], means=[float("nan")])
        o = frame(["b1"], [1.0], [1])
        self.assertFalse(_within_accumulation_band(MZ, e, o))

    def test_infinity_is_rejected(self) -> None:
        e = frame(["b1"], [float("inf")], [1], means=[float("inf")])
        o = frame(["b1"], [1.0], [1])
        self.assertFalse(_within_accumulation_band(MZ, e, o))

    def test_genuine_single_op_quantization_still_matches(self) -> None:
        # Half an ULP on a single-row group is exactly what float32 storage
        # quantization looks like; the band must not reject it.
        v = 1000.0
        e = frame(["b1"], [v * (1 + 0.5 * U)], [1], means=[v * (1 + 0.5 * U)])
        o = frame(["b1"], [v], [1], means=[v])
        self.assertTrue(_within_accumulation_band(MZ, e, o))

    def test_accumulation_scaled_drift_still_matches(self) -> None:
        v, n = 1e6, 4000
        drift = v * (n - 1) * U * 0.5
        e = frame(["b1"], [v + drift], [n], means=[(v + drift) / n])
        o = frame(["b1"], [v], [n], means=[v / n])
        self.assertTrue(_within_accumulation_band(MZ, e, o))


class UnitFloorMaskingTests(unittest.TestCase):
    """The old `max(1.0, |a|, |b|)` floor made the bound absolute below 1.0."""

    def setUp(self) -> None:
        self._saved = findings.STRICT_ACCUMULATION_MODEL

    def tearDown(self) -> None:
        findings.STRICT_ACCUMULATION_MODEL = self._saved

    def test_small_value_gross_error_is_masked_by_the_legacy_floor(self) -> None:
        # |sum| = 1e-4 in a 7000-row bucket. The legacy tolerance is absolute at
        # 4 * 7000 * u = 1.7e-3, so an error 10x the value itself is admitted.
        findings.STRICT_ACCUMULATION_MODEL = False
        e = frame(["b1"], [1.1e-3], [7000], means=[1.1e-3 / 7000])
        o = frame(["b1"], [1.0e-4], [7000], means=[1.0e-4 / 7000])
        self.assertTrue(_within_accumulation_band(MZ, e, o),
                        "legacy floor is expected to mask this")

    def test_strict_model_rejects_that_same_error(self) -> None:
        findings.STRICT_ACCUMULATION_MODEL = True
        e = frame(["b1"], [1.1e-3], [7000], means=[1.1e-3 / 7000])
        o = frame(["b1"], [1.0e-4], [7000], means=[1.0e-4 / 7000])
        self.assertFalse(_within_accumulation_band(MZ, e, o))

    def test_strict_model_holds_single_row_groups_to_one_ulp(self) -> None:
        # n=1 means no additions happened, so only storage quantization is
        # explicable. The legacy band allowed 4 ULP here.
        findings.STRICT_ACCUMULATION_MODEL = True
        v = 1000.0
        ok = frame(["b1"], [v * (1 + 0.9 * U)], [1], means=[v * (1 + 0.9 * U)])
        bad = frame(["b1"], [v * (1 + 3.0 * U)], [1], means=[v * (1 + 3.0 * U)])
        o = frame(["b1"], [v], [1], means=[v])
        self.assertTrue(_within_accumulation_band(MZ, ok, o))
        self.assertFalse(_within_accumulation_band(MZ, bad, o))

    def test_legacy_model_is_the_default(self) -> None:
        # Guards the campaign: the calibration must not shift mid-flight.
        self.assertFalse(findings.STRICT_ACCUMULATION_MODEL)


if __name__ == "__main__":
    unittest.main()
