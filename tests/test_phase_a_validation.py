from __future__ import annotations

import json
import multiprocessing
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest import mock

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]

from scripts.figures import load_module
from scripts.run import (
    run_boundary, run_d5_recovery, run_d7s_sweep, run_diagnostic,
)
from src.adapters.base import canonical_input
from src.adapters.reference import ReferenceAdapter
from src.engine.exact_engine import ExactEngine
from src.oracle.pandas_oracle import build_oracle_tables_from_rows
from src.diagnostics.findings import KNOWN_FINDINGS, reproduces_finding
from src.oracle.comparator import compare_bucket_tables
from src.schema.record import (
    DEDUPE_ID,
    ENTITY,
    EVENT_TIME,
    INGEST_TIME,
    METRIC,
    PROCESSING_ORDER,
    REQUIRED_FIELDS,
    SOURCE_RECORD_TYPE,
    VALUE,
    validate_row,
)


collect_design_space = load_module("collect_design_space")
collect_scaling = load_module("collect_scaling")

VIEW_COLUMNS = [
    "bucket", "entity", "metric", "count", "sum", "min", "max", "mean",
    "latest_value", "latest_event_time",
]


def _bucket_table(bucket: str, *, total: float = 3.0) -> pd.DataFrame:
    return pd.DataFrame([{
        "bucket": bucket,
        "entity": "entity-1",
        "metric": "metric-1",
        "count": 2,
        "sum": total,
        "min": 1.0,
        "max": 2.0,
        "mean": total / 2.0,
        "latest_value": 2.0,
        "latest_event_time": "2026-01-01T00:30:00.000000Z",
    }], columns=VIEW_COLUMNS)


def _empty_bucket_table() -> pd.DataFrame:
    return pd.DataFrame(columns=VIEW_COLUMNS)


class DesignSpaceAggregationTests(unittest.TestCase):
    def _summary(
        self,
        repeat: int,
        *,
        passed: bool = True,
        artifact_valid: bool = True,
        error: str | None = None,
        supported: bool = True,
    ) -> dict:
        summary = {
            "engine": "reference",
            "domain": "finance_markets",
            "diagnostic": "D1",
            "metric": "EMR",
            "rows": 50_000,
            "repeat_index": repeat,
            "_run_id": f"20260831T12000{repeat}Z_reference_d1",
            "artifact_valid": artifact_valid,
            "pass": passed,
            "input_sha256": "a" * 64,
            "oracle_cache_keys": ["oracle-key"],
            "primary": {
                "supported": supported,
                "hourly": {"emr": 0.0},
            },
        }
        if error is not None:
            summary["error"] = error
        return summary

    def test_blocked_repeat_prevents_cell_pass(self) -> None:
        runs = [
            self._summary(1),
            self._summary(2),
            self._summary(
                3, passed=False, artifact_valid=False,
                error="engine timed out before comparison"),
        ]

        cell = collect_design_space._aggregate_cell(runs, {1, 2, 3})

        self.assertEqual(cell["status"], collect_design_space.STATUS_BLOCKED)
        self.assertFalse(cell["pass"])
        self.assertEqual(cell["repeats_pass"], 2)
        self.assertEqual(cell["runs_blocked"], 1)

    def test_missing_expected_repeat_prevents_cell_pass(self) -> None:
        cell = collect_design_space._aggregate_cell(
            [self._summary(1), self._summary(2)], {1, 2, 3})

        self.assertEqual(cell["status"], collect_design_space.STATUS_BLOCKED)
        self.assertFalse(cell["pass"])
        self.assertEqual(cell["runs_missing"], 1)
        self.assertEqual(cell["missing_repeat_indices"], [3])

    def test_latest_scope_does_not_filter_non_verdict_attempts(self) -> None:
        blocked = self._summary(
            3, passed=False, artifact_valid=False, error="connection refused")
        runs = [self._summary(1), self._summary(2), blocked]

        kept = collect_design_space._scope_latest(runs, 3, 12.0)

        self.assertEqual(len(kept), 3)
        self.assertIn(blocked, kept)

    def test_complete_repeat_unstable_cell_is_valid_artifact_but_not_a_pass(self) -> None:
        runs = [
            self._summary(1),
            self._summary(2, passed=False),
            self._summary(3),
        ]

        cell = collect_design_space._aggregate_cell(runs, {1, 2, 3})
        artifact_valid, passed = collect_design_space._artifact_flags(
            True, [cell])

        self.assertEqual(cell["status"], collect_design_space.STATUS_REPEAT_UNSTABLE)
        self.assertTrue(cell["repeat_unstable"])
        self.assertEqual(cell["input_sha256"], "a" * 64)
        self.assertEqual(cell["oracle_cache_keys"], ["oracle-key"])
        self.assertTrue(artifact_valid)
        self.assertFalse(passed)

    def test_collectors_reject_inconsistent_repeat_provenance(self) -> None:
        runs = [self._summary(1), self._summary(2), self._summary(3)]
        runs[1]["input_sha256"] = "b" * 64
        runs[2]["oracle_cache_keys"] = ["different-oracle-key"]

        cell = collect_design_space._aggregate_cell(runs, {1, 2, 3})
        scaling_input, scaling_oracles = collect_scaling._canonical_provenance(
            runs)

        self.assertIsNone(cell["input_sha256"])
        self.assertEqual(cell["oracle_cache_keys"], [])
        self.assertIsNone(scaling_input)
        self.assertEqual(scaling_oracles, [])

    def test_collectors_materialize_legacy_d8_oracle_provenance(self) -> None:
        runs = [self._summary(index) for index in (1, 2, 3)]
        for summary in runs:
            summary.update({
                "diagnostic": "D8",
                "oracle_cache_keys": [],
                "co_oracle": {"agree": True},
            })

        with mock.patch.object(
            collect_design_space._oracle_provenance,
            "cache_keys",
            return_value=["d8-oracle-key"],
        ) as design_cache:
            cell = collect_design_space._aggregate_cell(runs, {1, 2, 3})
        with mock.patch.object(
            collect_scaling._oracle_provenance,
            "cache_keys",
            return_value=["d8-oracle-key"],
        ) as scaling_cache:
            _, scaling_keys = collect_scaling._canonical_provenance(runs)

        self.assertEqual(cell["oracle_cache_keys"], ["d8-oracle-key"])
        self.assertEqual(scaling_keys, ["d8-oracle-key"])
        design_cache.assert_called_once()
        scaling_cache.assert_called_once()

    def test_unsupported_d5_repeats_preserve_oracle_provenance(self) -> None:
        runs = [self._summary(index) for index in (1, 2, 3)]
        for summary in runs:
            summary.update({
                "diagnostic": "D5",
                "supported": False,
                "reason": "no external lifecycle to crash",
                "pass": False,
            })

        cell = collect_design_space._aggregate_cell(runs, {1, 2, 3})

        self.assertEqual(cell["status"], collect_design_space.STATUS_UNSUPPORTED)
        self.assertEqual(cell["runs_unsupported"], 3)
        self.assertEqual(cell["input_sha256"], "a" * 64)
        self.assertEqual(cell["oracle_cache_keys"], ["oracle-key"])

    def test_unsupported_d5_runs_the_same_h1_two_oracle_control(self) -> None:
        frame = pd.DataFrame({"placeholder": list(range(10))})
        co_oracle = {"agree": True}
        timing = {"oracle_cache_key": "h1-oracle-key"}
        with (
            mock.patch.object(run_d5_recovery, "_parquet_rows", return_value=10),
            mock.patch.object(
                run_d5_recovery, "_sha256_file", return_value="a" * 64),
            mock.patch.object(
                run_d5_recovery, "read_parquet_prefix", return_value=frame),
            mock.patch.object(
                run_d5_recovery, "canonical_input", return_value=frame),
            mock.patch.object(
                run_d5_recovery, "_cfg", return_value={
                    "kill_at_fraction": 0.5, "kill_signal": "SIGKILL",
                }),
            mock.patch.object(
                run_d5_recovery, "_oracle_tables",
                return_value=({}, co_oracle, timing),
            ) as oracle_tables,
            mock.patch.object(
                run_d5_recovery, "run_provenance", return_value={}),
        ):
            summary = run_d5_recovery._unsupported_summary(
                "reference", "finance_markets", 50_000,
                "in-process adapter has no external process to crash",
            )

        self.assertIs(summary["supported"], False)
        self.assertTrue(summary["artifact_valid"])
        self.assertEqual(summary["rows"], 10)
        self.assertEqual(summary["committed_prefix_rows"], 5)
        self.assertEqual(summary["input_sha256"], "a" * 64)
        self.assertEqual(summary["co_oracle"], co_oracle)
        self.assertEqual(summary["oracle_cache_keys"], ["h1-oracle-key"])
        h1, input_sha256, selection = oracle_tables.call_args.args
        self.assertEqual(len(h1), 5)
        self.assertEqual(input_sha256, "a" * 64)
        self.assertEqual(selection, {"mode": "prefix", "actual_rows": 5})


class D7SSweepPolicyTests(unittest.TestCase):
    def test_complete_all_negative_grid_exits_successfully(self) -> None:
        self.assertEqual(run_d7s_sweep._campaign_exit_code(
            artifact_valid=True, separates=False), 0)
        self.assertEqual(run_d7s_sweep._campaign_exit_code(
            artifact_valid=False, separates=False), 1)


class KnownFindingIntegrityTests(unittest.TestCase):
    def _tables(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        oracle = _bucket_table("2026-01-01T00:00:00Z", total=100.0)
        engine = oracle.copy(deep=True)
        engine.loc[0, "sum"] += 1e-6
        engine.loc[0, "mean"] += 1e-6
        return engine, oracle

    def test_expected_magnitude_signature_still_matches(self) -> None:
        engine, oracle = self._tables()
        comparison = compare_bucket_tables(engine, oracle)

        self.assertTrue(reproduces_finding(
            KNOWN_FINDINGS["materialize"],
            engine, engine, oracle, oracle, comparison, comparison))

    def test_duplicate_output_key_cannot_hide_behind_known_finding(self) -> None:
        engine, oracle = self._tables()
        duplicate_engine = pd.concat([engine, engine], ignore_index=True)
        comparison = compare_bucket_tables(duplicate_engine, oracle)
        self.assertEqual(comparison["duplicate_online_keys"], 1)

        self.assertFalse(reproduces_finding(
            KNOWN_FINDINGS["materialize"],
            duplicate_engine, duplicate_engine,
            oracle, oracle, comparison, comparison))

    def test_clean_tables_are_not_a_reproduced_divergence(self) -> None:
        oracle = _bucket_table("2026-01-01T00:00:00Z", total=100.0)
        comparison = compare_bucket_tables(oracle, oracle)

        self.assertFalse(reproduces_finding(
            KNOWN_FINDINGS["materialize"],
            oracle, oracle, oracle, oracle, comparison, comparison))


class SchemaBoundaryTests(unittest.TestCase):
    def _row(self) -> dict:
        return {
            EVENT_TIME: "2026-01-01T00:00:00.123456Z",
            INGEST_TIME: "2026-01-01T00:00:01Z",
            PROCESSING_ORDER: 1,
            ENTITY: "entity-1",
            METRIC: "metric-1",
            VALUE: 1.5,
            DEDUPE_ID: "dedupe-1",
            SOURCE_RECORD_TYPE: "data",
        }

    def test_valid_base_row_is_accepted(self) -> None:
        self.assertIsNone(validate_row(self._row()))

    def test_event_and_optional_ingest_timestamps_are_parsed(self) -> None:
        bad_event = {**self._row(), EVENT_TIME: "not-a-timestamp"}
        bad_ingest = {**self._row(), INGEST_TIME: "not-a-timestamp"}

        self.assertEqual(validate_row(bad_event), "invalid_event_time_utc")
        self.assertEqual(validate_row(bad_ingest), "invalid_ingest_time_utc")

    def test_naive_and_non_utc_timestamps_are_rejected(self) -> None:
        naive_event = {**self._row(), EVENT_TIME: "2026-01-01T00:00:00"}
        naive_ingest = {**self._row(), INGEST_TIME: "2026-01-01T00:00:01"}
        non_utc = {**self._row(), EVENT_TIME: "2026-01-01T01:00:00+01:00"}
        non_iso = {**self._row(), EVENT_TIME: "January 1, 2026 00:00:00+00:00"}
        date_only = {**self._row(), EVENT_TIME: "2026-01-01"}

        self.assertEqual(
            validate_row(naive_event), "invalid_event_time_utc")
        self.assertEqual(
            validate_row(naive_ingest), "invalid_ingest_time_utc")
        self.assertEqual(
            validate_row(non_utc), "invalid_event_time_utc")
        self.assertEqual(
            validate_row(non_iso), "invalid_event_time_utc")
        self.assertEqual(
            validate_row(date_only), "invalid_event_time_utc")

    def test_source_record_type_is_required_and_base_rows_must_be_data(self) -> None:
        missing = self._row()
        del missing[SOURCE_RECORD_TYPE]
        restatement = {**self._row(), SOURCE_RECORD_TYPE: "restatement"}

        self.assertIn(SOURCE_RECORD_TYPE, REQUIRED_FIELDS)
        self.assertEqual(
            validate_row(missing), "missing_source_record_type")
        self.assertEqual(
            validate_row(restatement), "invalid_source_record_type")


class CanonicalInputBoundaryTests(unittest.TestCase):
    def _row(self, **updates) -> dict:
        row = {
            EVENT_TIME: "2026-01-01T00:00:00.123456Z",
            PROCESSING_ORDER: 1,
            ENTITY: "entity-1",
            METRIC: "metric-1",
            VALUE: 1.5,
            DEDUPE_ID: "dedupe-1",
            SOURCE_RECORD_TYPE: "data",
        }
        row.update(updates)
        return row

    def test_valid_base_row_is_validated_before_projection(self) -> None:
        result = canonical_input(pd.DataFrame([self._row()]))

        self.assertEqual(result[SOURCE_RECORD_TYPE].tolist(), ["data"])
        self.assertTrue(result["supersedes"].isna().all())

    def test_explicit_zero_offset_is_normalized_to_utc(self) -> None:
        result = canonical_input(pd.DataFrame([self._row(**{
            EVENT_TIME: "2026-01-01T00:00:00.123456+00:00"})]))

        self.assertEqual(
            result.iloc[0][EVENT_TIME],
            pd.Timestamp("2026-01-01T00:00:00.123456Z"))
    def test_missing_and_unknown_source_record_types_are_rejected(self) -> None:
        missing = pd.DataFrame([self._row()]).drop(
            columns=[SOURCE_RECORD_TYPE])
        unknown = pd.DataFrame([
            self._row(**{SOURCE_RECORD_TYPE: "control"})])

        with self.assertRaisesRegex(ValueError, "source_record_type"):
            canonical_input(missing)
        with self.assertRaisesRegex(ValueError, "invalid source_record_type"):
            canonical_input(unknown)

    def test_d7s_sweep_validates_base_without_supersedes_column(self) -> None:
        invalid = pd.DataFrame([
            self._row(**{SOURCE_RECORD_TYPE: "control"})])

        with (
            mock.patch.object(
                run_d7s_sweep, "_load_build_fixtures",
                return_value=mock.Mock()),
            mock.patch.object(
                run_d7s_sweep.pd, "read_parquet",
                return_value=invalid),
        ):
            with self.assertRaisesRegex(ValueError, "source_record_type"):
                run_d7s_sweep._sweep_domain(
                    "finance_markets", [], [], 0, {}, [])

    def test_malformed_timestamp_is_rejected_before_projection(self) -> None:
        malformed = pd.DataFrame([
            self._row(**{EVENT_TIME: "not-a-timestamp"})])

        with self.assertRaisesRegex(ValueError, "invalid event_time_utc"):
            canonical_input(malformed)

    def test_naive_and_non_utc_timestamps_are_not_silently_localized(self) -> None:
        naive_event = pd.DataFrame([
            self._row(**{EVENT_TIME: "2026-01-01T00:00:00"})])
        naive_ingest = pd.DataFrame([
            self._row(**{INGEST_TIME: "2026-01-01T00:00:01"})])

        with self.assertRaisesRegex(ValueError, "invalid event_time_utc"):
            canonical_input(naive_event)
        with self.assertRaisesRegex(ValueError, "invalid ingest_time_utc"):
            canonical_input(naive_ingest)
        with self.assertRaisesRegex(ValueError, "invalid event_time_utc"):
            canonical_input(pd.DataFrame([self._row(**{
                EVENT_TIME: "2026-01-01T01:00:00+01:00"})]))
        with self.assertRaisesRegex(ValueError, "invalid event_time_utc"):
            canonical_input(pd.DataFrame([self._row(**{
                EVENT_TIME: "January 1, 2026 00:00:00+00:00"})]))
        with self.assertRaisesRegex(ValueError, "invalid event_time_utc"):
            canonical_input(pd.DataFrame([
                self._row(**{EVENT_TIME: "2026-01-01"})]))

    def test_d7_restatement_requires_and_preserves_its_pointer(self) -> None:
        base = {**self._row(), "supersedes": None}
        restatement = self._row(**{
            PROCESSING_ORDER: 2,
            VALUE: 2.0,
            DEDUPE_ID: "dedupe-restatement",
            SOURCE_RECORD_TYPE: "restatement",
            "supersedes": "dedupe-1",
        })

        frame = pd.DataFrame([base, restatement])
        with self.assertRaisesRegex(ValueError, "allow_restatements=True"):
            canonical_input(frame)
        result = canonical_input(
            frame, allow_restatements=True)

        self.assertEqual(
            result[SOURCE_RECORD_TYPE].tolist(), ["data", "restatement"])
        self.assertEqual(result.loc[1, "supersedes"], "dedupe-1")

    def test_restatement_without_pointer_and_data_with_pointer_are_rejected(self) -> None:
        restatement_without_pointer = pd.DataFrame([self._row(**{
            SOURCE_RECORD_TYPE: "restatement",
            "supersedes": None,
        })])
        data_with_pointer = pd.DataFrame([self._row(**{
            "supersedes": "some-dedupe-id",
        })])

        with self.assertRaisesRegex(ValueError, "requires a supersedes"):
            canonical_input(restatement_without_pointer, allow_restatements=True)
        with self.assertRaisesRegex(ValueError, "cannot carry supersedes"):
            canonical_input(data_with_pointer)


class RuntimeBoundaryTests(unittest.TestCase):
    def _row(self, **updates) -> dict:
        row = {
            EVENT_TIME: "2026-01-01T00:00:00.123456Z",
            INGEST_TIME: "2026-01-01T00:00:01Z",
            PROCESSING_ORDER: 1,
            ENTITY: "entity-1",
            METRIC: "metric-1",
            VALUE: 1.5,
            DEDUPE_ID: "dedupe-1",
            SOURCE_RECORD_TYPE: "data",
        }
        row.update(updates)
        return row

    def _assert_rejected_by_both(self, row: dict) -> None:
        counts = ExactEngine().ingest_rows([row])
        oracle = build_oracle_tables_from_rows(
            [row], include_quantiles=False)

        self.assertEqual(counts, {"accepted": 0, "rejected": 1})
        self.assertEqual(oracle["summary"]["rows_accepted"], 0)
        self.assertEqual(oracle["summary"]["rows_malformed"], 1)

    def test_exact_engine_and_oracle_reject_missing_or_invalid_source_type(
        self,
    ) -> None:
        missing = self._row()
        del missing[SOURCE_RECORD_TYPE]
        for row in (
            missing,
            self._row(**{SOURCE_RECORD_TYPE: "control"}),
        ):
            with self.subTest(source_type=row.get(SOURCE_RECORD_TYPE)):
                self._assert_rejected_by_both(row)

    def test_exact_engine_and_oracle_reject_non_explicit_utc(self) -> None:
        for row in (
            self._row(**{EVENT_TIME: "2026-01-01T00:00:00"}),
            self._row(**{EVENT_TIME: "2026-01-01"}),
            self._row(**{INGEST_TIME: "2026-01-01T00:00:01"}),
            self._row(**{EVENT_TIME: "2026-01-01T01:00:00+01:00"}),
            self._row(**{EVENT_TIME: "January 1, 2026 00:00:00+00:00"}),
        ):
            with self.subTest(row=row):
                self._assert_rejected_by_both(row)

    def test_boundary_injection_covers_every_violation_class_equally(
        self,
    ) -> None:
        """A count that skips a class would pass without testing it.

        The breakers are cycled, so a count below the number of classes leaves
        the tail classes uninjected and a count that is not a multiple injects
        them unequally. With a default of 5 against 6 classes, `missing_metric`
        would never be exercised and the control would report five of five
        rejected regardless.
        """
        classes = {row["_break"]
                   for row in run_boundary._malformed_rows(self._row(), 60)}
        self.assertIn("invalid_source_type", classes)
        self.assertIn("missing_metric", classes)
        self.assertEqual(len(classes), 6)

        counts = Counter(
            row["_break"] for row in run_boundary._malformed_rows(self._row(), 60))
        self.assertEqual(set(counts.values()), {10})

        with self.assertRaises(ValueError):        # fewer than the classes
            run_boundary._malformed_rows(self._row(), 5)
        with self.assertRaises(ValueError):        # not a multiple of them
            run_boundary._malformed_rows(self._row(), 8)

    def test_configured_boundary_count_covers_every_class(self) -> None:
        """The shipped configuration must satisfy the guard it is read into."""
        config = yaml.safe_load(
            (ROOT / "config" / "default.yaml").read_text()) or {}
        configured = int(
            (config.get("perturbation") or {}).get("malformed_rows", 0))
        classes = {row["_break"]
                   for row in run_boundary._malformed_rows(self._row(), 60)}
        self.assertGreaterEqual(configured, len(classes))
        self.assertEqual(configured % len(classes), 0)

    def test_d7_restatements_require_engine_opt_in_and_keep_negative_control(
        self,
    ) -> None:
        base = {**self._row(), "supersedes": None}
        restatement = self._row(**{
            PROCESSING_ORDER: 2,
            VALUE: 2.0,
            DEDUPE_ID: "dedupe-restatement",
            SOURCE_RECORD_TYPE: "restatement",
            "supersedes": "dedupe-1",
        })
        frame = canonical_input(
            pd.DataFrame([base, restatement]), allow_restatements=True)
        rows = frame.to_dict("records")

        self.assertEqual(
            ExactEngine().ingest_rows([rows[1]]),
            {"accepted": 0, "rejected": 1})
        self.assertEqual(
            ExactEngine(allow_restatements=True).ingest_rows([rows[1]]),
            {"accepted": 1, "rejected": 0})

        dedup = ReferenceAdapter(allow_restatements=True)
        dedup.setup(variant="dedup")
        dedup.ingest_log(frame)
        dedup_view = dedup.read_view("hourly")
        dedup.close()

        restate = ReferenceAdapter(allow_restatements=True)
        restate.setup(variant="restate")
        restate.ingest_log(frame)
        restate_view = restate.read_view("hourly")
        restate.close()

        self.assertEqual(int(dedup_view.iloc[0]["count"]), 2)
        self.assertAlmostEqual(float(dedup_view.iloc[0]["sum"]), 3.5)
        self.assertEqual(int(restate_view.iloc[0]["count"]), 1)
        self.assertAlmostEqual(float(restate_view.iloc[0]["sum"]), 2.0)

    def test_restatement_aware_oracle_does_not_consume_unknown_source_type(
        self,
    ) -> None:
        base = {**self._row(), "supersedes": None}
        invalid = self._row(**{
            PROCESSING_ORDER: 2,
            DEDUPE_ID: "invalid-control",
            SOURCE_RECORD_TYPE: "control",
            "supersedes": "dedupe-1",
        })

        result = build_oracle_tables_from_rows(
            [base, invalid], restatement_aware=True,
            include_quantiles=False)

        self.assertEqual(result["summary"]["rows_accepted"], 1)
        self.assertEqual(result["summary"]["rows_malformed"], 1)



def _d3_test_provenance() -> dict:
    return {
        "contract_version": 2,
        "git_commit": "a" * 40,
        "invocation_id": "d3-concurrent-test",
        "benchmark_config_sha256": "b" * 64,
        "dependency_lock_sha256": "c" * 64,
    }


def _d3_ledger_process_worker(
    path_text: str,
    start_event,
    result_queue,
    worker_index: int,
) -> None:
    try:
        if not start_event.wait(timeout=10):
            raise TimeoutError("D3 ledger test start event was not released")
        result = run_diagnostic._ledger_transaction(
            Path(path_text),
            {"hourly|bucket|entity|metric": "3ff0000000000000"},
            _d3_test_provenance(),
            engine="flink",
            domain="finance_markets",
            rows=50_000,
            input_sha256="d" * 64,
            run_id=f"worker-{worker_index}",
        )
        result_queue.put({"result": result})
    except BaseException as exc:
        result_queue.put({"error": f"{type(exc).__name__}: {exc}"})


class _FakeAdapter:
    def __init__(self, tables: dict[str, pd.DataFrame]) -> None:
        self.tables = tables

    def connect(self):  # noqa: ANN201
        return self

    def __enter__(self):  # noqa: ANN204
        return self

    def __exit__(self, *_args):  # noqa: ANN002, ANN204
        return None

    def setup(self, *, variant: str) -> None:
        if variant != "dedup":
            raise AssertionError(variant)

    def ingest_log(self, _df: pd.DataFrame) -> None:
        return None

    def read_view(self, grain: str) -> pd.DataFrame:
        return self.tables[grain].copy(deep=True)


class D3ContractTests(unittest.TestCase):
    def _run(
        self,
        replay1: dict[str, pd.DataFrame],
        replay2: dict[str, pd.DataFrame],
        oracle: dict[str, pd.DataFrame],
        *,
        input_rows: int = 1,
        ledger: bool = False,
    ) -> dict:
        df = pd.DataFrame({"placeholder": range(input_rows)})
        timing = {"oracle_build_s": 0.0, "oracle_cache_hit": True}
        with (
            mock.patch.object(
                run_diagnostic, "get_adapter",
                side_effect=[_FakeAdapter(replay1), _FakeAdapter(replay2)]),
            mock.patch.object(
                run_diagnostic, "_build_oracle",
                return_value=(oracle, None, timing)),
        ):
            return run_diagnostic._run_ddr(
                "reference", df, "finance_markets", "input-sha", ledger=ledger)

    def test_deterministic_but_wrong_replays_fail_oracle_contract(self) -> None:
        oracle = {
            "hourly": _bucket_table("2026-01-01T00:00:00Z"),
            "daily": _bucket_table("2026-01-01"),
        }
        wrong = {grain: table.copy(deep=True) for grain, table in oracle.items()}
        for table in wrong.values():
            table.loc[0, "sum"] += 1.0

        result = self._run(wrong, wrong, oracle)

        self.assertTrue(result["intra_invocation_pass"])
        self.assertFalse(result["oracle_equivalent"])
        self.assertFalse(result["pass"])

    def test_empty_empty_comparison_is_not_a_vacuous_pass(self) -> None:
        empty = {"hourly": _empty_bucket_table(), "daily": _empty_bucket_table()}

        with mock.patch.object(run_diagnostic, "_append_ledger") as append:
            result = self._run(empty, empty, empty, input_rows=0, ledger=True)
        append.assert_not_called()

        self.assertFalse(result["non_vacuous"])
        self.assertFalse(result["artifact_valid"])
        self.assertFalse(result["pass"])

    def test_correct_bitwise_stable_replays_pass_both_parts_of_c3(self) -> None:
        oracle = {
            "hourly": _bucket_table("2026-01-01T00:00:00Z"),
            "daily": _bucket_table("2026-01-01"),
        }

        result = self._run(oracle, oracle, oracle)

        self.assertTrue(result["non_vacuous"])
        self.assertTrue(result["oracle_equivalent"])
        self.assertTrue(result["intra_invocation_pass"])
        self.assertTrue(result["pass"])



    def test_ledger_transaction_serializes_concurrent_processes(self) -> None:
        worker_count = 12
        with tempfile.TemporaryDirectory(prefix="d3-ledger-race-") as tmp:
            path = Path(tmp) / "same-cohort.jsonl"
            context = multiprocessing.get_context("spawn")
            start_event = context.Event()
            result_queue = context.Queue()
            processes = [
                context.Process(
                    target=_d3_ledger_process_worker,
                    args=(str(path), start_event, result_queue, index),
                )
                for index in range(worker_count)
            ]
            try:
                for process in processes:
                    process.start()
                start_event.set()
                for process in processes:
                    process.join(timeout=20)
                timed_out = [
                    process.pid for process in processes if process.is_alive()
                ]
                self.assertFalse(timed_out, f"timed out workers: {timed_out}")
                self.assertTrue(
                    all(process.exitcode == 0 for process in processes),
                    [process.exitcode for process in processes],
                )
                messages = [
                    result_queue.get(timeout=5) for _ in range(worker_count)
                ]
            finally:
                for process in processes:
                    if process.is_alive():
                        process.terminate()
                        process.join(timeout=5)
                result_queue.close()
                result_queue.join_thread()

            errors = [message["error"] for message in messages if "error" in message]
            self.assertFalse(errors, errors)
            results = [message["result"] for message in messages]
            self.assertEqual(
                sorted(result["prior_invocations"] for result in results),
                list(range(worker_count)),
            )
            first = [
                result for result in results
                if result["baseline_run_id"] is None
            ]
            self.assertEqual(len(first), 1)
            self.assertEqual(first[0]["prior_invocations"], 0)
            self.assertTrue(all(result["pass"] for result in results))

            lines = path.read_text().splitlines()
            self.assertEqual(len(lines), worker_count)
            records = [json.loads(line) for line in lines]
            self.assertEqual(
                {record["run_id"] for record in records},
                {f"worker-{index}" for index in range(worker_count)},
            )
            self.assertTrue(all(
                record["ledger_record_version"] == 1 for record in records))

    def test_ledger_transaction_rejects_corrupt_json_and_records(self) -> None:
        sums = {"hourly|bucket|entity|metric": "3ff0000000000000"}
        corruptions = {
            "invalid_json": "{not-json}\n",
            "invalid_record": json.dumps({"sums": sums}) + "\n",
        }
        with tempfile.TemporaryDirectory(prefix="d3-ledger-corrupt-") as tmp:
            for name, payload in corruptions.items():
                with self.subTest(corruption=name):
                    path = Path(tmp) / f"{name}.jsonl"
                    path.write_text(payload)
                    with self.assertRaisesRegex(
                        ValueError, "corrupt D3 ledger .* line 1",
                    ):
                        run_diagnostic._ledger_transaction(
                            path,
                            sums,
                            _d3_test_provenance(),
                            engine="flink",
                            domain="finance_markets",
                            rows=50_000,
                            input_sha256="d" * 64,
                            run_id="should-not-append",
                        )
                    self.assertEqual(path.read_text(), payload)


if __name__ == "__main__":
    unittest.main()
