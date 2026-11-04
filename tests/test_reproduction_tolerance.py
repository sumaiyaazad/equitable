"""The reproduction check tolerates only the nondeterminism the evidence documents.

A reviewer's campaign lands differently from the locked one on the cells whose
outcome is run dependent: D3 replays of Flink and RisingWave, Flink's
batch-dependent family members, and RisingWave's drift at parallelism one.
Those are tolerated and named; every other difference is a failure.
"""
from __future__ import annotations

import unittest

from scripts.results import verify_reproduction as vr


def _design(rows):
    return {"rows": [
        {"engine": e, "domain": d, "diagnostic": g, "status": s}
        for e, d, g, s in rows]}


def _scaling(rows):
    return {"rows": [
        {"engine": e, "domain": d, "diagnostic": g, "size": n,
         "status": s, "stable": st}
        for e, d, g, n, s, st in rows]}


def _family(rows, **header):
    base = {"oracle_disagreements": 0, "blocked": 0, "expressible_cells": 98}
    base.update(header)
    base["rows"] = [
        {"engine": e, "domain": d, "grain": g, "shape": sh, "delivery": dl,
         "supported": True, "contract_equivalent": c, "arrival_equivalent": a,
         "attribution": at}
        for e, d, g, sh, dl, c, a, at in rows]
    return base


class GridTolerance(unittest.TestCase):

    def test_identical_grids_have_nothing_to_report(self):
        rows = [("spark", "finance_markets", "D1", "pass")]
        self.assertEqual(vr.diff_design(_design(rows), _design(rows)), ([], []))

    def test_a_replay_cell_may_move_between_pass_fail_and_repeat_unstable(self):
        official = _design([("risingwave", "environment_airquality", "D3", "repeat-unstable"),
                            ("flink", "finance_markets", "D3", "fail")])
        fresh = _design([("risingwave", "environment_airquality", "D3", "pass"),
                         ("flink", "finance_markets", "D3", "repeat-unstable")])
        mismatched, tolerated = vr.diff_design(official, fresh)
        self.assertEqual(mismatched, [])
        self.assertEqual(len(tolerated), 2)

    def test_a_replay_cell_cannot_become_unsupported(self):
        official = _design([("flink", "finance_markets", "D3", "fail")])
        fresh = _design([("flink", "finance_markets", "D3", "unsupported")])
        mismatched, tolerated = vr.diff_design(official, fresh)
        self.assertEqual(mismatched, [("flink", "finance_markets", "D3")])
        self.assertEqual(tolerated, [])

    def test_a_locked_repeat_unstable_cell_is_tolerated_whatever_its_diagnostic(self):
        official = _design([("ksqldb", "finance_markets", "D4", "repeat-unstable")])
        fresh = _design([("ksqldb", "finance_markets", "D4", "fail")])
        self.assertEqual(vr.diff_design(official, fresh),
                         ([], [("ksqldb", "finance_markets", "D4")]))

    def test_any_other_cell_must_match_exactly(self):
        official = _design([("feldera", "datacenter_drives", "D8", "pass")])
        fresh = _design([("feldera", "datacenter_drives", "D8", "repeat-unstable")])
        self.assertEqual(vr.diff_design(official, fresh),
                         ([("feldera", "datacenter_drives", "D8")], []))

    def test_a_missing_cell_is_a_mismatch_not_a_tolerance(self):
        official = _design([("flink", "finance_markets", "D3", "fail")])
        self.assertEqual(vr.diff_design(official, _design([])),
                         ([("flink", "finance_markets", "D3")], []))


class ScalingTolerance(unittest.TestCase):

    def test_replay_cells_may_change_status_and_stability(self):
        official = _scaling([("risingwave", "environment_airquality", "D3",
                              50000, "equivalent", False)])
        fresh = _scaling([("risingwave", "environment_airquality", "D3",
                           50000, "divergence", True)])
        self.assertEqual(vr.diff_scaling(official, fresh),
                         ([], [("risingwave", "environment_airquality", "D3", 50000)]))

    def test_disagreeing_repeats_are_labelled_nondeterministic_and_tolerated(self):
        official = _scaling([("risingwave", "environment_airquality", "D3",
                              10000, "equivalent", False)])
        fresh = _scaling([("risingwave", "environment_airquality", "D3",
                           10000, "nondeterministic", False)])
        self.assertEqual(vr.diff_scaling(official, fresh)[0], [])

    def test_an_unstable_locked_cell_is_tolerated_for_any_diagnostic(self):
        official = _scaling([("ksqldb", "finance_markets", "D1", 10000,
                              "equivalent", False)])
        fresh = _scaling([("ksqldb", "finance_markets", "D1", 10000,
                           "known_divergence", True)])
        self.assertEqual(vr.diff_scaling(official, fresh)[0], [])

    def test_a_stable_non_replay_cell_must_match(self):
        official = _scaling([("spark", "finance_markets", "D1", 10000,
                              "equivalent", True)])
        fresh = _scaling([("spark", "finance_markets", "D1", 10000,
                           "divergence", True)])
        self.assertEqual(vr.diff_scaling(official, fresh)[0],
                         [("spark", "finance_markets", "D1", 10000)])


class FamilyTolerance(unittest.TestCase):

    def test_flink_batch_dependent_members_may_take_any_scored_attribution(self):
        official = _family([
            ("flink", "sports_football", "daily", "S5_filter_on_latest",
             "ordered", False, False, "repair_divergent"),
            ("flink", "transportation_taxi", "hourly", "S3_rank_by_latest",
             "shuffled", False, True, "repair_divergent")])
        fresh = _family([
            ("flink", "sports_football", "daily", "S5_filter_on_latest",
             "ordered", True, True, "attributable"),
            ("flink", "transportation_taxi", "hourly", "S3_rank_by_latest",
             "shuffled", True, False, "repaired")])
        header_equal, mismatched, tolerated = vr.diff_query_family(official, fresh)
        self.assertTrue(header_equal)
        self.assertEqual(mismatched, [])
        self.assertEqual(len(tolerated), 2)

    def test_other_engines_and_shapes_must_match(self):
        official = _family([
            ("risingwave", "sports_football", "daily", "S3_rank_by_latest",
             "ordered", True, True, "attributable"),
            ("flink", "finance_markets", "hourly", "S1_latest_value",
             "ordered", True, True, "attributable")])
        fresh = _family([
            ("risingwave", "sports_football", "daily", "S3_rank_by_latest",
             "ordered", False, False, "repair_divergent"),
            ("flink", "finance_markets", "hourly", "S1_latest_value",
             "ordered", False, True, "repair_divergent")])
        _, mismatched, tolerated = vr.diff_query_family(official, fresh)
        self.assertEqual(len(mismatched), 2)
        self.assertEqual(tolerated, [])

    def test_a_member_that_stops_being_scored_is_a_mismatch(self):
        official = _family([
            ("flink", "sports_football", "daily", "S5_filter_on_latest",
             "ordered", False, False, "repair_divergent")])
        fresh = _family([
            ("flink", "sports_football", "daily", "S5_filter_on_latest",
             "ordered", None, None, "unsupported")])
        self.assertEqual(len(vr.diff_query_family(official, fresh)[1]), 1)

    def test_the_header_counts_must_still_match(self):
        official = _family([], blocked=0)
        fresh = _family([], blocked=1)
        self.assertFalse(vr.diff_query_family(official, fresh)[0])


class ParallelismProjection(unittest.TestCase):

    def _rows(self, engine, drift):
        return {"rows": [{"engine": engine, "parallelism": 1,
                          "domain": "environment_airquality",
                          "oracle_equivalent": True, "drift_keys": drift}]}

    def test_flink_must_be_drift_free_at_parallelism_one(self):
        self.assertNotEqual(vr._parallelism_sweep(self._rows("flink", 0)),
                            vr._parallelism_sweep(self._rows("flink", 1)))

    def test_risingwave_drift_at_parallelism_one_is_not_compared(self):
        self.assertEqual(vr._parallelism_sweep(self._rows("risingwave", 0)),
                         vr._parallelism_sweep(self._rows("risingwave", 1)))

    def test_oracle_equivalence_is_always_compared(self):
        rows = self._rows("risingwave", 0)
        other = {"rows": [dict(rows["rows"][0], oracle_equivalent=False)]}
        self.assertNotEqual(vr._parallelism_sweep(rows),
                            vr._parallelism_sweep(other))


if __name__ == "__main__":
    unittest.main()
