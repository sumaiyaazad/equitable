"""The declared-exclusion contract for narrowed cohorts.

An engine may be left out of the confirmation or scaling cohort only when the
collection names it in ``excluded_engines``. These tests pin the two halves of
that contract: a declared exclusion is accepted and reported, and an undeclared
narrowing is still rejected. They also pin the scaling counterpart of the
design-space "repeat-unstable" verdict, so a complete-but-unstable repeat set stays
promotable, as docs/RUNBOOK.md states, instead of voiding the collection.
"""
from __future__ import annotations

import unittest

from scripts.results._scope import (REQUIRED_DOMAINS, REQUIRED_DIAGNOSTICS,
                                    REQUIRED_ENGINES, SCALING_DIAGNOSTICS,
                                    SCALING_SIZES, official_scope_failure)

SHA = "a" * 64
KEY = "oracle-cache-key"
SIX = sorted(REQUIRED_ENGINES - {"flink"})


def _scaling_summary(engines, excluded):
    rows = [
        {
            "engine": engine, "domain": domain, "diagnostic": diagnostic,
            "size": size, "status": "equivalent", "pass": True,
            "stable": True, "repeats": 3, "executed": True,
            "throughput_eligible": True,
            "input_sha256": SHA, "oracle_cache_keys": [KEY],
        }
        for engine in engines
        for domain in REQUIRED_DOMAINS
        for diagnostic in SCALING_DIAGNOSTICS
        for size in SCALING_SIZES
    ]
    summary = {
        "kind": "scaling_collect", "required_engines": sorted(engines),
        "base_size": 50_000, "min_repeats": 3, "required_repeats": 3,
        "sizes": sorted(SCALING_SIZES), "cells": len(rows),
        "expected_cells": len(rows), "rows": rows,
        "missing_cells": [], "thin_cells": [],
        "input_inconsistent_cells": [], "oracle_inconsistent_cells": [],
        "repeat_mismatch_cells": [],
        "cross_engine_input_inconsistent": [],
        "cross_engine_oracle_inconsistent": [],
    }
    if excluded is not None:
        summary["excluded_engines"] = excluded
    return summary


def _confirmation_summary(engines, excluded):
    rows = [
        {
            "engine": engine, "domain": domain, "diagnostic": diagnostic,
            "status": "pass", "pass": True, "repeat_unstable": False,
            "repeats": 1, "repeats_pass": 1, "rows": 1_000_000,
            "runs_blocked": 0, "runs_missing": 0, "runs_unsupported": 0,
            "missing_repeat_indices": [],
            "input_sha256": SHA, "oracle_cache_keys": [KEY],
        }
        for engine in engines
        for domain in REQUIRED_DOMAINS
        for diagnostic in REQUIRED_DIAGNOSTICS
    ]
    summary = {
        "kind": "design_space_collect", "required_engines": sorted(engines),
        "pinned_rows": 1_000_000, "pinned_d5_rows": 1_000_000,
        "min_repeats": 1, "scope": "latest", "repeats_kept": 1,
        "cells": len(rows), "expected_cells": len(rows), "rows": rows,
        "missing_cells": [], "input_inconsistent_cells": [],
        "oracle_inconsistent_cells": [],
        "cross_engine_input_inconsistent": [],
        "cross_engine_oracle_inconsistent": [],
        "repeat_mismatch_cells": [], "cells_below_min_repeats": 0,
    }
    if excluded is not None:
        summary["excluded_engines"] = excluded
    return summary


class DeclaredExclusionTests(unittest.TestCase):
    def test_declared_scaling_exclusion_is_accepted(self) -> None:
        summary = _scaling_summary(SIX, ["flink"])
        self.assertIsNone(official_scope_failure("scaling", summary))

    def test_undeclared_scaling_narrowing_is_still_rejected(self) -> None:
        summary = _scaling_summary(SIX, None)
        failure = official_scope_failure("scaling", summary)
        self.assertIsNotNone(failure)
        self.assertIn("declares invalid engines", failure)

    def test_exclusion_must_name_a_real_engine(self) -> None:
        summary = _scaling_summary(SIX, ["not-an-engine"])
        failure = official_scope_failure("scaling", summary)
        self.assertIsNotNone(failure)
        self.assertIn("invalid excluded_engines", failure)

    def test_exclusion_cannot_empty_the_universe(self) -> None:
        summary = _scaling_summary([], sorted(REQUIRED_ENGINES))
        failure = official_scope_failure("scaling", summary)
        self.assertIsNotNone(failure)
        self.assertIn("cannot exclude every engine", failure)

    def test_rows_must_match_the_declared_universe(self) -> None:
        """Declaring an exclusion does not excuse a missing cell."""
        summary = _scaling_summary(SIX, ["flink"])
        # The declared counts stay at the full six-engine universe; only the
        # rows fall short, which is exactly how a truncated cohort would look.
        summary["rows"] = summary["rows"][:-1]
        failure = official_scope_failure("scaling", summary)
        self.assertIsNotNone(failure)
        self.assertIn("scaling universe", failure)

    def test_excluded_engine_rows_may_not_appear_in_the_cohort(self) -> None:
        """Declaring an exclusion must remove the engine's cells, not just the name.

        The first implementation narrowed required_engines but still collected
        the excluded engine's runs, so the cohort carried 239 cells while
        declaring a 210-cell universe. The gate must reject any cohort whose
        rows fall outside the universe it declares.
        """
        summary = _scaling_summary(SIX, ["flink"])
        summary["rows"] += [
            dict(row, engine="flink") for row in summary["rows"][:30]
        ]
        summary["cells"] = len(summary["rows"])
        failure = official_scope_failure("scaling", summary)
        self.assertIsNotNone(failure)

    def test_confirmation_accepts_a_declared_exclusion(self) -> None:
        summary = _confirmation_summary(SIX, ["flink"])
        self.assertIsNone(
            official_scope_failure("design_space_confirmation", summary))

    def test_the_50k_design_space_never_permits_an_exclusion(self) -> None:
        """Correctness is the one cohort that must hold all seven engines."""
        summary = _confirmation_summary(SIX, ["flink"])
        summary.update(pinned_rows=50_000, pinned_d5_rows=50_000,
                       min_repeats=3, repeats_kept=3)
        for row in summary["rows"]:
            row.update(rows=50_000, repeats=3, repeats_pass=3)
        failure = official_scope_failure("design_space", summary)
        self.assertIsNotNone(failure)
        self.assertIn("cannot exclude an engine", failure)

    def test_unstable_scaling_cell_is_reported_not_fatal(self) -> None:
        summary = _scaling_summary(SIX, ["flink"])
        summary["rows"][0].update(
            status="nondeterministic", **{"pass": False}, stable=False)
        self.assertIsNone(official_scope_failure("scaling", summary))

    def test_unstable_scaling_cell_must_be_marked_unstable(self) -> None:
        summary = _scaling_summary(SIX, ["flink"])
        summary["rows"][0].update(
            status="nondeterministic", **{"pass": False}, stable=True)
        failure = official_scope_failure("scaling", summary)
        self.assertIsNotNone(failure)
        self.assertIn("must be marked unstable", failure)


if __name__ == "__main__":
    unittest.main()
