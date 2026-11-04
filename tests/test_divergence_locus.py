"""Regression tests for the divergence shape classifier (src.oracle.degree.locus).

The labels are DESCRIPTIVE: they say which contract fields a divergence moved,
never which events an engine admitted. Two mistakes are easy to reintroduce and
are what these tests pin down.

1. `count_involving` once read `bool(exact_fields)`, and EXACT_FIELDS holds both
   `count` and `latest_event_time`. That labelled a timestamp-only divergence
   count-involving, which asserts the wrong events were admitted for a
   divergence that moves no count at all. Feldera's cells are exactly this
   shape.
2. `reduction_only` is not the complement of `count_involving`. A divergence in
   `min`, `max`, `latest_value`, or `latest_event_time` is neither, and callers
   must not infer one flag from the other.
"""
from __future__ import annotations

import unittest

from src.oracle.degree import locus


class LocusClassificationTests(unittest.TestCase):
    def _shape(self, **fields: int) -> tuple:
        r = locus(fields)
        return r["count_involving"], r["reduction_only"]

    def test_count_is_count_involving(self) -> None:
        self.assertEqual(self._shape(count=1), (True, False))
        self.assertEqual(self._shape(count=1, sum=3, mean=3), (True, False))

    def test_sum_and_mean_only_is_reduction_only(self) -> None:
        self.assertEqual(self._shape(sum=3), (False, True))
        self.assertEqual(self._shape(mean=3), (False, True))
        self.assertEqual(self._shape(sum=3, mean=3), (False, True))

    def test_timestamp_only_is_other_not_count_involving(self) -> None:
        # latest_event_time is an exact field but is not `count`.
        self.assertEqual(self._shape(latest_event_time=2), (False, False))

    def test_latest_value_and_min_only_are_other(self) -> None:
        self.assertEqual(self._shape(latest_value=4), (False, False))
        self.assertEqual(self._shape(min=1), (False, False))
        self.assertEqual(self._shape(max=1), (False, False))
        # A float field outside {sum, mean} is not a reduction-only divergence
        # even when it appears alongside them.
        self.assertEqual(self._shape(sum=3, min=1), (False, False))

    def test_labels_are_mutually_exclusive(self) -> None:
        for fields in ({"count": 1}, {"sum": 1}, {"mean": 1}, {"min": 1},
                       {"latest_event_time": 1}, {"latest_value": 1},
                       {"count": 1, "latest_event_time": 1},
                       {"sum": 1, "mean": 1, "count": 1}):
            cnt, red = self._shape(**fields)
            self.assertFalse(cnt and red, f"both flags set for {fields}")

    def test_zero_counts_and_empty_input_touch_nothing(self) -> None:
        for empty in ({}, {"count": 0, "sum": 0}, None):
            r = locus(empty)  # type: ignore[arg-type]
            self.assertEqual(r["fields"], {})
            self.assertFalse(r["count_involving"])
            self.assertFalse(r["reduction_only"])


if __name__ == "__main__":
    unittest.main()
