"""The hopping-window oracle, in both implementations.

A sliding window is the one query-breadth axis that needs no new output
schema: it emits the existing bucket table with the window start in the bucket
column. What it does need is a window assignment that both oracles agree on,
which is what these tests pin.

The UTC test exists because the first DuckDB implementation formatted window
labels with strftime over a TIMESTAMPTZ, which renders in the SESSION
timezone. On a host west of Greenwich every label came out shifted by the local
offset, so all 150 rows disagreed with the pandas path. Nothing else in the
harness had caught this, because the rest of the SQL oracle takes `bucket` from
the input frame rather than formatting a timestamp itself.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

from src.oracle.cache import build_or_load_hopping_oracle
from src.oracle.hopping import (HOP_H, WINDOW_SIZE_H, build_hopping_table,
                                build_hopping_table_sql, explode_to_windows,
                                window_count)


def _log(n: int = 300, seed: int = 11) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    base = pd.Timestamp("2026-03-01T00:00:00Z")
    frame = pd.DataFrame({
        "event_time_utc": [
            (base + pd.Timedelta(minutes=int(m))).strftime("%Y-%m-%dT%H:%M:%SZ")
            for m in rng.integers(0, 60 * 24, n)],
        "entity": rng.choice(["e1", "e2", "e3"], n),
        "metric": rng.choice(["m1", "m2"], n),
        "value": rng.normal(100, 25, n).round(6),
        "dedupe_id": [f"d{i}" for i in range(n)],
        "processing_order": range(n),
    })
    frame["event_time_ts"] = pd.to_datetime(frame["event_time_utc"], utc=True)
    return frame


class HoppingGeometry(unittest.TestCase):
    def test_k_is_size_over_hop(self) -> None:
        self.assertEqual(window_count(2, 1), 2)
        self.assertEqual(window_count(6, 2), 3)
        self.assertEqual(window_count(WINDOW_SIZE_H, HOP_H), 2)

    def test_a_partial_multiple_is_refused(self) -> None:
        # Events at different offsets would land in different numbers of
        # windows, which no engine HOP primitive reproduces.
        with self.assertRaises(ValueError):
            window_count(3, 2)

    def test_every_event_lands_in_exactly_k_windows(self) -> None:
        frame = _log()
        exploded = explode_to_windows(frame)
        self.assertEqual(len(exploded), len(frame) * window_count())
        per_event = exploded.groupby("dedupe_id").size()
        self.assertTrue((per_event == window_count()).all())

    def test_every_assigned_window_actually_contains_the_event(self) -> None:
        exploded = explode_to_windows(_log())
        start = pd.to_datetime(exploded["window_bucket"], utc=True,
                               format="%Y-%m-%dT%H:00:00Z")
        end = start + pd.Timedelta(hours=WINDOW_SIZE_H)
        self.assertTrue((start <= exploded["event_time_ts"]).all())
        self.assertTrue((exploded["event_time_ts"] < end).all())


class BothOraclesAgree(unittest.TestCase):
    def test_pandas_and_duckdb_produce_the_same_hopping_table(self) -> None:
        frame = _log()
        left = build_hopping_table(frame).reset_index(drop=True)
        right = build_hopping_table_sql(frame).reset_index(drop=True)
        self.assertEqual(len(left), len(right))
        self.assertTrue(
            (left[["bucket", "entity", "metric"]].astype(str).values
             == right[["bucket", "entity", "metric"]].astype(str).values).all())
        for column in ("count", "sum", "min", "max", "mean", "latest_value"):
            delta = (left[column].astype(float)
                     - right[column].astype(float)).abs().max()
            self.assertLess(float(delta), 1e-9, column)
        self.assertTrue((left["latest_event_time"].values
                         == right["latest_event_time"].values).all())

    def test_window_labels_are_utc_not_session_local(self) -> None:
        # A label must name the UTC hour the window opens.
        frame = _log(n=1)
        frame.loc[0, "event_time_utc"] = "2026-03-01T05:30:00Z"
        frame["event_time_ts"] = pd.to_datetime(frame["event_time_utc"], utc=True)
        for table in (build_hopping_table(frame), build_hopping_table_sql(frame)):
            self.assertEqual(
                sorted(table["bucket"].astype(str)),
                ["2026-03-01T04:00:00Z", "2026-03-01T05:00:00Z"])

    def test_an_empty_log_yields_an_empty_table_on_both_paths(self) -> None:
        empty = _log().iloc[0:0]
        self.assertTrue(build_hopping_table(empty).empty)
        self.assertTrue(build_hopping_table_sql(empty).empty)

    def test_verified_hopping_tables_are_content_addressed_and_reused(self) -> None:
        provenance = {
            "git_commit": "a" * 40,
            "benchmark_config_sha256": "b" * 64,
            "dependency_lock_sha256": "c" * 64,
        }
        with tempfile.TemporaryDirectory() as directory, mock.patch(
            "src.oracle.cache.run_provenance", return_value=provenance,
        ):
            root = Path(directory)
            kwargs = {
                "root": root,
                "source_sha256": "d" * 64,
                "selection": {"mode": "prefix", "actual_rows": 300},
                "co_oracle": True,
            }
            first_tables, first_co, first_timing = build_or_load_hopping_oracle(
                _log(), **kwargs)
            second_tables, second_co, second_timing = build_or_load_hopping_oracle(
                _log(), **kwargs)

            self.assertTrue(first_co["agree"])
            self.assertEqual(first_co, second_co)
            self.assertFalse(first_timing["oracle_cache_hit"])
            self.assertTrue(second_timing["oracle_cache_hit"])
            self.assertEqual(first_timing["oracle_cache_key"],
                             second_timing["oracle_cache_key"])
            self.assertTrue((first_tables["hourly"] ==
                             second_tables["hourly"]).all().all())
            cache = root / "data" / "oracle_cache" / first_timing["oracle_cache_key"]
            self.assertTrue((cache / "metadata.json").is_file())


class _HoppingAdapter:
    """An engine that maintains an empty hopping view: enough to reach the
    comparison, which is all the oracle provenance depends on."""
    hopping_mode = "native"

    def connect(self):  # noqa: ANN201
        return self

    def __enter__(self):  # noqa: ANN204
        return self

    def __exit__(self, *_args):  # noqa: ANN002, ANN204
        return None

    def setup_hopping(self) -> None:
        return None

    def ingest_log(self, _df: pd.DataFrame):  # noqa: ANN201
        return mock.Mock(as_dict=lambda: {})

    def read_hopping(self, _grain: str) -> pd.DataFrame:
        return pd.DataFrame(columns=build_hopping_table(_log()).columns)


class RunnerRecordsItsOracle(unittest.TestCase):
    """A D8 cell that records no oracle key cannot attest what it was compared
    against. The runner builds D8's oracle through the shared cache, like every
    other oracle-backed diagnostic, so each cell names its expected table."""

    def test_a_d8_cell_names_its_content_addressed_oracle(self) -> None:
        from scripts.run import run_diagnostic
        provenance = {
            "git_commit": "a" * 40,
            "benchmark_config_sha256": "b" * 64,
            "dependency_lock_sha256": "c" * 64,
        }
        frame = _log().drop(columns=["event_time_ts"])
        frame.attrs["_equitable_input_sha256"] = "d" * 64
        frame.attrs["_equitable_selection"] = {"mode": "prefix",
                                               "actual_rows": len(frame)}
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch("src.oracle.cache.run_provenance",
                           return_value=provenance), \
                mock.patch.object(run_diagnostic, "ROOT", Path(directory)), \
                mock.patch.object(run_diagnostic, "get_adapter",
                                  return_value=_HoppingAdapter()):
            first = run_diagnostic._run_hop("reference", frame, 1e-9, True)
            second = run_diagnostic._run_hop("reference", frame, 1e-9, True)
            cached = (Path(directory) / "data" / "oracle_cache"
                      / first["oracle_cache_key"] / "metadata.json")
            self.assertTrue(cached.is_file())

        self.assertTrue(first["co_oracle"]["agree"])
        self.assertFalse(first["oracle_cache_hit"])
        self.assertTrue(second["oracle_cache_hit"])
        self.assertEqual(first["oracle_cache_key"], second["oracle_cache_key"])


if __name__ == "__main__":
    unittest.main()
