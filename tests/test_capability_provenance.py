"""An unsupported cell names no oracle, and nothing weaker than that passes.

The promotion gate requires every cell to carry the oracle cache key it was
scored against. An unsupported cell was never scored: the adapter has no
formulation of the view, so the harness stops before an expected table is
built. Requiring a key of it made a truthfully recorded capability state
unpromotable, which is the failure these tests pin shut. The exemption is
narrow on purpose, so the tests check both directions: the capability state is
admitted, and every cell that did produce a comparison still has to name its
oracle.
"""
from __future__ import annotations

import unittest

from scripts.results._scope import (REQUIRED_DOMAINS, REQUIRED_DIAGNOSTICS,
                                    REQUIRED_ENGINES, official_scope_failure)

SHA = "a" * 64
KEY = "oracle-cache-key"


def _design_summary():
    rows = [
        {
            "engine": engine, "domain": domain, "diagnostic": diagnostic,
            "status": "pass", "pass": True, "repeat_unstable": False,
            "repeats": 3, "repeats_pass": 3, "rows": 50_000,
            "runs_blocked": 0, "runs_missing": 0, "runs_unsupported": 0,
            "missing_repeat_indices": [],
            "input_sha256": SHA, "oracle_cache_keys": [KEY],
        }
        for engine in sorted(REQUIRED_ENGINES)
        for domain in REQUIRED_DOMAINS
        for diagnostic in REQUIRED_DIAGNOSTICS
    ]
    return {
        "kind": "design_space_collect",
        "required_engines": sorted(REQUIRED_ENGINES),
        "pinned_rows": 50_000, "pinned_d5_rows": 50_000,
        "min_repeats": 3, "scope": "latest", "repeats_kept": 3,
        "cells": len(rows), "expected_cells": len(rows), "rows": rows,
        "missing_cells": [], "input_inconsistent_cells": [],
        "oracle_inconsistent_cells": [],
        "cross_engine_input_inconsistent": [],
        "cross_engine_oracle_inconsistent": [],
        "repeat_mismatch_cells": [], "cells_below_min_repeats": 0,
    }


def _d5_rows(summary):
    return [row for row in summary["rows"] if row["diagnostic"] == "D5"]


class CapabilityProvenanceTests(unittest.TestCase):
    def test_the_complete_scored_grid_is_admitted(self) -> None:
        self.assertIsNone(
            official_scope_failure("design_space", _design_summary()))

    def test_an_unsupported_cell_need_not_name_an_oracle(self) -> None:
        """D5 on an in-process backend: no crash to pose, so no expected table.

        This is the case that blocked promotion. The cell is complete and
        truthful; it simply has no oracle, because nothing was ever compared.
        """
        summary = _design_summary()
        for row in _d5_rows(summary):
            row.update({"status": "unsupported", "pass": False,
                        "runs_unsupported": row["repeats"], "repeats_pass": 0,
                        "oracle_cache_keys": []})
        self.assertIsNone(official_scope_failure("design_space", summary))

    def test_an_unsupported_cell_must_still_name_its_input(self) -> None:
        """The input hash is assigned before the adapter is ever consulted."""
        summary = _design_summary()
        for row in _d5_rows(summary):
            row.update({"status": "unsupported", "pass": False,
                        "oracle_cache_keys": [], "input_sha256": None})
        failure = official_scope_failure("design_space", summary)
        self.assertIsNotNone(failure)
        self.assertIn("input hash", failure)

    def test_a_scored_cell_may_not_drop_its_oracle(self) -> None:
        """The exemption is for capability states only, not for verdicts."""
        for status in ("pass", "fail", "repeat-unstable"):
            with self.subTest(status=status):
                summary = _design_summary()
                row = _d5_rows(summary)[0]
                row.update({"status": status, "oracle_cache_keys": []})
                failure = official_scope_failure("design_space", summary)
                self.assertIsNotNone(failure)
                self.assertIn("oracle cache key", failure)

    def test_a_blocked_cell_may_not_drop_its_oracle(self) -> None:
        """Blocked is an execution failure, not a declared capability limit."""
        summary = _design_summary()
        row = _d5_rows(summary)[0]
        row.update({"status": "blocked", "oracle_cache_keys": []})
        failure = official_scope_failure("design_space", summary)
        self.assertIsNotNone(failure)


if __name__ == "__main__":
    unittest.main()
