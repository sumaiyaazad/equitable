"""A fixture builder must accept a frame that has been through the contract.

`canonical_input` keeps only what the contract reads, so it drops the optional
source metadata `topic` and blanks `ingest_time_utc`. `run_d7s_sweep` calls it
before building its restatement fixture, which made `_finalize` raise
`KeyError: ['topic'] not in index` and took the whole validation stage down with
it. The failure was invisible until a campaign re-ran the sweep, because the
promoted D7S evidence predated the change.

These tests pin the recovery so the two paths cannot drift apart again.
"""
from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from scripts.perturb import build_fixtures as bf
from src.adapters.base import canonical_input
from src.schema.record import NORMALIZED_COLUMNS, REQUIRED_FIELDS, SUPERSEDES, TOPIC


LOG = (Path(__file__).resolve().parents[1]
       / "data/normalized/transportation_taxi/log.parquet")


def _log(rows: int = 800) -> pd.DataFrame:
    # benchmark init runs this suite before a campaign's data stage has built
    # the log, so its absence is a skip, not a failure.
    if not LOG.is_file():
        raise unittest.SkipTest("no normalized taxi log; run the data stage")
    return pd.read_parquet(LOG).head(rows)


class FixtureColumnRecoveryTests(unittest.TestCase):
    def test_canonical_input_really_does_drop_the_optional_metadata(self) -> None:
        """Pin the premise: if this stops being true the recovery is dead code."""
        raw = _log()
        self.assertIn(TOPIC, raw.columns)
        self.assertNotIn(TOPIC, canonical_input(raw).columns)

    def test_the_dropped_columns_are_not_part_of_the_contract(self) -> None:
        """Filling them with null is only honest because nothing reads them."""
        self.assertNotIn(TOPIC, REQUIRED_FIELDS)

    def test_a_restatement_fixture_builds_from_a_canonicalized_frame(self) -> None:
        """The exact call run_d7s_sweep makes must accept a canonicalized frame."""
        base = canonical_input(_log())
        fixture, restated = bf.perturb_d7_restate(
            base, 0.25, bf._rng(1337, bf._SUBSTREAM["d7"]))
        self.assertGreater(restated, 0)
        self.assertEqual(list(fixture.columns), NORMALIZED_COLUMNS + [SUPERSEDES])
        self.assertTrue(fixture[TOPIC].isna().all(),
                        "a recovered column must be null, not a placeholder")

    def test_a_raw_frame_still_keeps_its_real_metadata(self) -> None:
        """Recovery must not overwrite a topic the caller actually supplied."""
        raw = _log()
        fixture, _ = bf.perturb_d7_restate(
            raw, 0.25, bf._rng(1337, bf._SUBSTREAM["d7"]))
        self.assertTrue(fixture[TOPIC].notna().any(),
                        "a real topic was replaced by the null recovery")


if __name__ == "__main__":
    unittest.main()
