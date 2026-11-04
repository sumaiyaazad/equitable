"""The query-family control separates a broken construction from an engine's divergence.

One engine can keep stale rows in a shape under ordered delivery while the
others are attributable on the same members. A rule requiring every engine to
be attributable would turn that one engine's divergence into a validation
failure that stops the campaign before any engine cell runs.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts" / "results"))

from src.diagnostics.shapes import control_verdict  # noqa: E402
from _scope import _query_family_scope_failure  # noqa: E402


def _cell(engine, attribution, delivery="ordered", shape="S3_rank_by_latest"):
    return {"engine": engine, "domain": "transportation_taxi", "grain": "hourly",
            "shape": shape, "delivery": delivery, "attribution": attribution}


def _summary(rows):
    return {"kind": "query_family", "rows": rows, "oracle_disagreements": 0,
            "blocked": 0, "expressible_cells": 2}


class FamilyControlTests(unittest.TestCase):
    def test_one_engine_diverging_is_an_engine_gap_not_a_broken_test(self):
        rows = [_cell("materialize", "attributable"), _cell("flink", "repair_divergent"),
                _cell("materialize", "repaired", "shuffled"),
                _cell("flink", "repaired", "shuffled")]
        verdict = control_verdict(rows)
        self.assertTrue(verdict["construction_clean"])
        self.assertEqual([g["engine"] for g in verdict["engine_gaps"]], ["flink"])
        self.assertIsNone(_query_family_scope_failure(_summary(rows)))

    def test_a_gap_on_every_engine_voids_the_member(self):
        rows = [_cell("materialize", "repaired"), _cell("flink", "repair_divergent"),
                _cell("materialize", "repaired", "shuffled")]
        verdict = control_verdict(rows)
        self.assertFalse(verdict["construction_clean"])
        self.assertIn("own construction", _query_family_scope_failure(_summary(rows)))

    def test_a_member_only_one_engine_expresses_cannot_clear_the_construction(self):
        rows = [_cell("flink", "repaired"), _cell("ksqldb", "unsupported"),
                _cell("flink", "repaired", "shuffled")]
        self.assertFalse(control_verdict(rows)["construction_clean"])

    def test_capability_states_are_not_scored(self):
        rows = [_cell("materialize", "attributable"), _cell("ksqldb", "unsupported")]
        verdict = control_verdict(rows)
        self.assertTrue(verdict["construction_clean"])
        self.assertEqual(verdict["engine_gaps"], [])


if __name__ == "__main__":
    unittest.main()
