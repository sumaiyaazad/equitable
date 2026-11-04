from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from scripts.run import run_native_semantics
from src.adapters import NativeNotExpressible


class _SetupFailureAdapter:
    native_topk_mode = "incremental"

    def __init__(self, failure: Exception) -> None:
        self.failure = failure
        self.closed = False

    def connect(self):
        return self

    def configure_native_topk(self, _k: int, _rank_metric: str) -> None:
        return None

    def setup(self, *, variant: str = "dedup") -> None:
        self.variant = variant
        raise self.failure

    def close(self) -> None:
        self.closed = True


class NativeCapabilityClassificationTests(unittest.TestCase):
    def _run_setup_failure(self, failure: Exception) -> dict:
        adapter = _SetupFailureAdapter(failure)
        frame = pd.DataFrame({"sentinel": [1]})
        with tempfile.TemporaryDirectory(
            prefix="native-classification-", dir="/tmp"
        ) as directory, mock.patch.object(
            run_native_semantics, "ROOT", Path(directory)
        ), mock.patch.object(
            run_native_semantics, "get_adapter", return_value=adapter
        ), mock.patch.object(
            run_native_semantics, "_base_prefix", return_value=frame
        ), mock.patch.object(
            run_native_semantics, "_path_sha256", return_value="a" * 64
        ):
            result = run_native_semantics._topk_cell(
                "materialize", "finance_markets", 20_000, 4, 10, "mean"
            )
        self.assertTrue(adapter.closed)
        self.assertEqual(result["phase"], "setup")
        return result

    def test_transient_setup_failure_is_blocked_and_invalid(self) -> None:
        result = self._run_setup_failure(RuntimeError("HTTP 500"))

        self.assertEqual(result["status"], "blocked")
        self.assertIs(result["supported"], True)
        self.assertIs(result["artifact_valid"], False)
        self.assertIsNone(result["pass"])

    def test_explicit_semantic_rejection_is_publishable_capability_state(self) -> None:
        result = self._run_setup_failure(
            NativeNotExpressible("ROW_NUMBER is not supported")
        )

        self.assertEqual(result["status"], "not_expressible")
        self.assertIs(result["supported"], False)
        self.assertIs(result["artifact_valid"], True)
        self.assertIsNone(result["pass"])


if __name__ == "__main__":
    unittest.main()
