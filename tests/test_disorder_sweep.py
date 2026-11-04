"""The disorder sweep counts a key wrong by the contract's tolerance.

Its exposure curve and the engine rings on it are compared on the selected
row only; a latest value that differs from the oracle's by one unit in the
last place is within tau and must not count as a wrong key, or an engine
lands off the curve for a difference the scored cells do not see.
"""
from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def _sweep():
    spec = importlib.util.spec_from_file_location(
        "run_disorder_sweep", ROOT / "scripts" / "run" / "run_disorder_sweep.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _table(latest_value: float) -> pd.DataFrame:
    return pd.DataFrame({
        "bucket": ["2024-06-01T08:00:00Z"], "entity": ["Home_1"],
        "metric": ["speed"], "count": [3], "sum": [12.0], "min": [1.0],
        "max": [41.8642], "mean": [4.0], "latest_value": [latest_value],
        "latest_event_time": ["2024-06-01T08:05:00.000000Z"]})


class DisorderWrongFractionTests(unittest.TestCase):
    def test_a_last_bit_difference_is_not_a_wrong_key(self) -> None:
        sweep = _sweep()
        expected = _table(41.8642)
        self.assertEqual(
            sweep._wrong_fraction(_table(41.864200000000004), expected, "hourly")["wrong_keys"], 0)
        self.assertEqual(
            sweep._wrong_fraction(_table(41.9), expected, "hourly")["wrong_keys"], 1)

    def test_the_tolerance_is_the_contract_epsilon(self) -> None:
        self.assertEqual(_sweep().EPSILON, 1e-9)


if __name__ == "__main__":
    unittest.main()
