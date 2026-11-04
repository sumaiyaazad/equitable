from __future__ import annotations

import inspect
import json
import time
import unittest
from types import SimpleNamespace
from unittest import mock

import pandas as pd

from src.adapters import _flink_worker
from src.adapters.feldera import FelderaAdapter
from src.adapters.ksqldb import KsqlDbAdapter
from src.adapters.materialize import MaterializeAdapter


class _JsonResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class _CountCursor:
    def __init__(self, rows):
        self._rows = iter(rows)
        self.executed: list[str] = []

    def execute(self, sql: str) -> None:
        self.executed.append(sql)

    def fetchone(self):
        return next(self._rows)


class _Future:
    def __init__(self, value):
        self._value = value

    def result(self, timeout=None):  # noqa: ANN001
        return self._value


class _AdminClient:
    def __init__(self, *, committed: int, latest: int):
        self.committed = committed
        self.latest = latest
        self.groups = []
        self.require_stable = []
        self.latest_partitions = []

    def list_consumer_group_offsets(self, requests, **kwargs):
        if len(requests) != 1:
            raise AssertionError("the client accepts one group per call")
        group_id = requests[0].group_id
        self.groups.append(group_id)
        self.require_stable.append(kwargs.get("require_stable"))
        partition = SimpleNamespace(
            topic="input", partition=0, offset=self.committed, error=None)
        result = SimpleNamespace(topic_partitions=[partition])
        return {group_id: _Future(result)}

    def list_offsets(self, requests, **_kwargs):
        self.latest_partitions = [
            (partition.topic, partition.partition) for partition in requests
        ]
        return {
            partition: _Future(SimpleNamespace(offset=self.latest))
            for partition in requests
        }


class FelderaAdapterReadinessTests(unittest.TestCase):
    def test_ingress_preserves_canonical_microseconds(self) -> None:
        adapter = FelderaAdapter(process_timeout_s=0)
        requests = []

        def request(method, path, **kwargs):  # noqa: ANN001
            requests.append((method, path, kwargs))
            return _JsonResponse({})

        adapter._req = request  # type: ignore[method-assign]
        adapter._await_processed = lambda _expected: None  # type: ignore[method-assign]
        frame = pd.DataFrame({
            "event_time_utc": [
                pd.Timestamp("2026-08-31T12:34:56.123456Z"),
            ],
            "entity": ["alice"],
            "metric": ["fare"],
            "value": [3.5],
            "processing_order": [0],
            "dedupe_id": ["row-1"],
            "supersedes": [None],
        })

        stats = adapter.ingest_log(frame)

        self.assertEqual(stats.rows_sent, 1)
        payload = json.loads(requests[0][2]["data"].decode())
        self.assertEqual(
            payload["insert"]["event_time_utc"],
            "2026-08-31T12:34:56.123456Z",
        )

    @staticmethod
    def _one_row() -> pd.DataFrame:
        return pd.DataFrame({
            "event_time_utc": [pd.Timestamp("2026-08-31T12:34:56.123456Z")],
            "entity": ["alice"], "metric": ["fare"], "value": [3.5],
            "processing_order": [0], "dedupe_id": ["row-1"], "supersedes": [None],
        })

    @staticmethod
    def _complete_metrics() -> _JsonResponse:
        return _JsonResponse({"global_metrics": {
            "buffered_input_records": 0, "total_processed_records": 1,
            "total_input_records": 1}})

    def test_ingress_waits_on_the_completion_token_before_the_metrics(self) -> None:
        """The token is the readiness signal; the metrics check only confirms it.

        The metrics move when records leave the input buffer, before the step
        that folds them into the views ends, so a read gated on them alone can
        see a view one step behind under load. The token completes with the step.
        """
        adapter = FelderaAdapter(process_timeout_s=5)
        calls: list[tuple[str, str, object]] = []
        status = iter(["inprogress", "inprogress", "complete"])

        def request(method, path, **kwargs):  # noqa: ANN001
            leaf = path.rsplit("/", 1)[-1]
            calls.append((method, leaf, (kwargs.get("params") or {}).get("token")))
            if method == "POST":
                return _JsonResponse({"token": "tok-1"})
            if leaf == "completion_status":
                return _JsonResponse({"status": next(status)})
            return self._complete_metrics()

        adapter._req = request  # type: ignore[method-assign]
        with mock.patch("src.adapters.feldera.time.sleep"):
            adapter.ingest_log(self._one_row())

        leaves = [leaf for _, leaf, _ in calls]
        self.assertEqual(leaves.count("completion_status"), 3)
        self.assertEqual({token for _, leaf, token in calls
                          if leaf == "completion_status"}, {"tok-1"})
        self.assertLess(max(i for i, leaf in enumerate(leaves) if leaf == "completion_status"),
                        leaves.index("stats"))

    def test_ingress_without_a_token_falls_back_to_the_metrics_check(self) -> None:
        adapter = FelderaAdapter(process_timeout_s=5)
        leaves: list[str] = []

        def request(method, path, **_kwargs):  # noqa: ANN001
            leaves.append(path.rsplit("/", 1)[-1])
            return _JsonResponse({}) if method == "POST" else self._complete_metrics()

        adapter._req = request  # type: ignore[method-assign]
        with mock.patch("src.adapters.feldera.time.sleep"):
            adapter.ingest_log(self._one_row())
        self.assertNotIn("completion_status", leaves)
        self.assertIn("stats", leaves)

    def test_an_incomplete_token_raises_at_deadline(self) -> None:
        adapter = FelderaAdapter(process_timeout_s=1)
        adapter._req = lambda *_a, **_kw: _JsonResponse(  # type: ignore[method-assign]
            {"status": "inprogress"})
        with mock.patch(
            "src.adapters.feldera.time.time", side_effect=[0.0, 0.0, 2.0]
        ), mock.patch("src.adapters.feldera.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "inprogress"):
                adapter._await_complete("tok-1")

    def test_incomplete_metrics_raise_at_deadline(self) -> None:
        adapter = FelderaAdapter(process_timeout_s=1)
        adapter._req = lambda *_a, **_kw: _JsonResponse({  # type: ignore[method-assign]
            "global_metrics": {
                "buffered_input_records": 2,
                "total_processed_records": 3,
                "total_input_records": 5,
            },
        })

        with mock.patch(
            "src.adapters.feldera.time.time", side_effect=[0.0, 0.0, 2.0]
        ), mock.patch("src.adapters.feldera.time.sleep"):
            with self.assertRaisesRegex(
                RuntimeError, "total_processed_records=3"
            ):
                adapter._await_processed(5)

    def test_stats_errors_are_not_silently_swallowed(self) -> None:
        adapter = FelderaAdapter(process_timeout_s=1)

        def unavailable(*_args, **_kwargs):
            raise RuntimeError("stats unavailable")

        adapter._req = unavailable  # type: ignore[method-assign]
        with mock.patch(
            "src.adapters.feldera.time.time", side_effect=[0.0, 0.0, 2.0]
        ), mock.patch("src.adapters.feldera.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "stats unavailable"):
                adapter._await_processed(5)


class MaterializeAdapterReadinessTests(unittest.TestCase):
    def test_barrier_uses_all_relations_and_waits_for_expected_rows(self) -> None:
        cursor = _CountCursor([
            (4, 2, 2),
            (4, 2, 2),
            (5, 2, 2),
        ])
        adapter = MaterializeAdapter(
            hydrate_timeout_s=2,
            hydrate_poll_interval_s=0,
        )

        adapter._await_hydrated(cursor, expected_rows=5)

        self.assertEqual(len(cursor.executed), 3)
        sql = cursor.executed[0]
        for relation in ("stream_rows", "v_hourly", "v_daily"):
            self.assertIn(relation, sql)

    def test_incomplete_source_count_raises_at_deadline(self) -> None:
        cursor = _CountCursor([(4, 2, 2)])
        adapter = MaterializeAdapter(
            hydrate_timeout_s=1,
            hydrate_poll_interval_s=0,
        )

        with mock.patch(
            "src.adapters.materialize.time.time",
            side_effect=[0.0, 0.0, 2.0],
        ), mock.patch("src.adapters.materialize.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "source_rows=4"):
                adapter._await_hydrated(cursor, expected_rows=5)


class KsqlDbAdapterReadinessTests(unittest.TestCase):
    def test_same_cardinality_value_change_is_not_stable(self) -> None:
        snapshots = [
            [{"KEY": "alice", "CNT": 1}],
            [{"KEY": "alice", "CNT": 2}],
            [{"KEY": "alice", "CNT": 2}],
        ]
        adapter = KsqlDbAdapter(
            run_suffix="readiness_test",
            read_stable_timeout_s=2,
            read_poll_interval_s=0,
        )

        def pull(_sql, *, deadline=None):  # noqa: ANN001
            self.assertIsNotNone(deadline)
            return snapshots.pop(0)

        adapter._persistent_query_lags = (  # type: ignore[method-assign]
            lambda *, deadline: {"query/group/input[0]": 0}
        )
        adapter._pull_ready = pull  # type: ignore[method-assign]
        rows = adapter._pull_stable("SELECT * FROM view;")

        self.assertEqual(rows, [{"KEY": "alice", "CNT": 2}])
        self.assertEqual(snapshots, [])

    def test_row_order_does_not_prevent_stability(self) -> None:
        first = [{"KEY": "alice", "CNT": 1}, {"KEY": "bob", "CNT": 2}]
        snapshots = [first, list(reversed(first))]
        adapter = KsqlDbAdapter(
            run_suffix="readiness_test",
            read_stable_timeout_s=2,
            read_poll_interval_s=0,
        )
        adapter._persistent_query_lags = (  # type: ignore[method-assign]
            lambda *, deadline: {"query/group/input[0]": 0}
        )
        adapter._pull_ready = (  # type: ignore[method-assign]
            lambda _sql, *, deadline=None: snapshots.pop(0)
        )

        self.assertEqual(
            adapter._pull_stable("SELECT * FROM view;"),
            list(reversed(first)),
        )
        self.assertEqual(snapshots, [])

    def test_nonzero_lag_blocks_reads_until_two_zero_lag_polls(self) -> None:
        lag_snapshots = [
            {"query/group/input[0]": 1},
            {"query/group/input[0]": 0},
            {"query/group/input[0]": 0},
        ]
        pull_calls = []
        adapter = KsqlDbAdapter(
            run_suffix="readiness_test",
            read_stable_timeout_s=2,
            read_poll_interval_s=0,
        )

        adapter._persistent_query_lags = (  # type: ignore[method-assign]
            lambda *, deadline: lag_snapshots.pop(0)
        )

        def pull(_sql, *, deadline=None):  # noqa: ANN001
            pull_calls.append(deadline)
            return [{"KEY": "alice", "CNT": 2}]

        adapter._pull_ready = pull  # type: ignore[method-assign]

        self.assertEqual(
            adapter._pull_stable("SELECT * FROM view;"),
            [{"KEY": "alice", "CNT": 2}],
        )
        self.assertEqual(lag_snapshots, [])
        self.assertEqual(len(pull_calls), 2)

    def test_explain_discovers_only_persistent_query_groups(self) -> None:
        adapter = KsqlDbAdapter(run_suffix="readiness_test")

        def post(_path, sql, **_kwargs):  # noqa: ANN001
            if sql == "SHOW QUERIES;":
                return _JsonResponse([{"queries": [
                    {"id": "QUERY_1", "queryType": "PERSISTENT"},
                    {"id": "PUSH_1", "queryType": "PUSH"},
                ]}])
            self.assertEqual(sql, "EXPLAIN QUERY_1;")
            return _JsonResponse([{
                "queryDescription": {"consumerGroupId": "group-1"},
            }])

        adapter._post = post  # type: ignore[method-assign]

        self.assertEqual(
            adapter._query_consumer_groups(),
            {"QUERY_1": "group-1"},
        )

    def test_admin_offsets_report_partition_lag(self) -> None:
        adapter = KsqlDbAdapter(run_suffix="readiness_test")
        admin = _AdminClient(committed=8, latest=10)
        adapter._admin = admin
        adapter._query_consumer_groups = (  # type: ignore[method-assign]
            lambda *, deadline=None: {"QUERY_1": "group-1"}
        )

        lags = adapter._persistent_query_lags(
            deadline=time.time() + 5)

        self.assertEqual(list(lags.values()), [2])
        self.assertEqual(admin.groups, ["group-1"])
        self.assertEqual(admin.require_stable, [True])
        self.assertEqual(admin.latest_partitions, [("input", 0)])

    def test_admin_offsets_query_each_consumer_group_separately(self) -> None:
        adapter = KsqlDbAdapter(run_suffix="readiness_test")
        admin = _AdminClient(committed=10, latest=10)
        adapter._admin = admin
        adapter._query_consumer_groups = (  # type: ignore[method-assign]
            lambda *, deadline=None: {
                "QUERY_1": "group-1",
                "QUERY_2": "group-2",
            }
        )

        lags = adapter._persistent_query_lags(
            deadline=time.time() + 5)

        self.assertEqual(list(lags.values()), [0, 0])
        self.assertEqual(admin.groups, ["group-1", "group-2"])
        self.assertEqual(admin.require_stable, [True, True])

    def test_uncommitted_query_waits_on_the_startup_budget(self) -> None:
        """A query that has not committed offsets yet is starting, not stuck.

        The steady-state extension keys off a completed zero-lag pull, so it
        cannot fire before the first commit. Without a separate startup budget
        a loaded ksqlDB that needs longer than `read_stable_timeout_s` to
        rebalance fails the cell regardless of how small the input is.
        """
        from src.adapters.ksqldb import _QueryStarting

        attempts = []

        def lags(*, deadline):  # noqa: ANN001, ANN202
            attempts.append(deadline)
            if len(attempts) < 6:
                raise _QueryStarting(
                    "consumer group group-1 has no committed partitions yet")
            return {"query/group-1/input[0]": 0}

        adapter = KsqlDbAdapter(
            run_suffix="readiness_test",
            read_stable_timeout_s=0.05,
            read_startup_timeout_s=30,
            read_poll_interval_s=0,
        )
        adapter._persistent_query_lags = lags  # type: ignore[method-assign]
        adapter._pull_ready = (  # type: ignore[method-assign]
            lambda _sql, *, deadline=None: [{"KEY": "alice", "CNT": 1}]
        )

        self.assertEqual(
            adapter._pull_stable("SELECT * FROM view;"),
            [{"KEY": "alice", "CNT": 1}],
        )
        # The base budget alone would have expired after the first poll.
        self.assertGreaterEqual(len(attempts), 6)

    def test_startup_budget_is_bounded(self) -> None:
        """A query that never commits still fails; it does not hang forever."""
        from src.adapters.ksqldb import _QueryStarting

        adapter = KsqlDbAdapter(
            run_suffix="readiness_test",
            read_stable_timeout_s=0.01,
            read_startup_timeout_s=0.05,
            read_poll_interval_s=0,
        )
        adapter._persistent_query_lags = (  # type: ignore[method-assign]
            lambda *, deadline: (_ for _ in ()).throw(_QueryStarting(
                "consumer group group-1 has no committed partitions yet"))
        )

        with self.assertRaisesRegex(RuntimeError, "no committed partitions"):
            adapter._pull_stable("SELECT * FROM view;")

    def test_real_fault_is_not_granted_the_startup_budget(self) -> None:
        """An unreachable broker keeps failing at the base timeout."""
        started = time.time()
        adapter = KsqlDbAdapter(
            run_suffix="readiness_test",
            read_stable_timeout_s=0.05,
            read_startup_timeout_s=30,
            read_poll_interval_s=0,
        )
        adapter._persistent_query_lags = (  # type: ignore[method-assign]
            lambda *, deadline: (_ for _ in ()).throw(
                RuntimeError("broker is unreachable"))
        )

        with self.assertRaisesRegex(RuntimeError, "broker is unreachable"):
            adapter._pull_stable("SELECT * FROM view;")
        self.assertLess(time.time() - started, 5)

    def test_unstable_or_empty_result_raises_at_deadline(self) -> None:
        adapter = KsqlDbAdapter(
            run_suffix="readiness_test",
            read_stable_timeout_s=0,
            read_poll_interval_s=0,
        )

        with self.assertRaisesRegex(RuntimeError, "stable zero-lag"):
            adapter._pull_stable("SELECT * FROM view;")


class KsqlDbStuckQueryTests(unittest.TestCase):
    """A view query that never starts is recreated; nothing else is."""

    VIEW = "CTAS_VS_TEST_DAILY_"

    def _adapter(self, **options) -> KsqlDbAdapter:
        adapter = KsqlDbAdapter(
            run_suffix="readiness_test",
            read_stable_timeout_s=0.05,
            read_startup_timeout_s=30,
            read_stable_max_timeout_s=30,
            read_restart_after_s=0.05,
            read_poll_interval_s=0,
            **options,
        )
        adapter._query_states = lambda: "stubbed"  # type: ignore[method-assign]
        adapter._pull_ready = (  # type: ignore[method-assign]
            lambda _sql, *, deadline=None: [{"KEY": "alice", "CNT": 1}]
        )
        return adapter

    def test_stuck_view_is_recreated_and_recorded(self) -> None:
        from src.adapters.ksqldb import _QueryStarting

        restarted = []

        def lags(*, deadline):  # noqa: ANN001, ANN202
            if not restarted:
                raise _QueryStarting(
                    "consumer group g has no committed partitions yet",
                    query_ids=(self.VIEW + "7",))
            return {"q/g/t[0]": 0}

        adapter = self._adapter()
        adapter._persistent_query_lags = lags  # type: ignore[method-assign]
        rows = adapter._pull_stable(
            "SELECT * FROM view;",
            restart_view=lambda: restarted.append(True),
            view_query_prefix=self.VIEW,
        )

        self.assertEqual(rows, [{"KEY": "alice", "CNT": 1}])
        self.assertEqual(len(restarted), 1)
        self.assertEqual(len(adapter.view_restarts), 1)
        self.assertEqual(adapter.view_restarts[0]["queries"], [self.VIEW + "7"])
        self.assertIn("without a first commit", adapter.view_restarts[0]["reason"])

    def test_view_restarts_are_capped(self) -> None:
        from src.adapters.ksqldb import _QueryStarting

        restarted = []
        adapter = self._adapter(read_max_view_restarts=2)
        adapter.options["read_startup_timeout_s"] = 0.5
        adapter.options["read_stable_max_timeout_s"] = 0.5
        adapter._persistent_query_lags = (  # type: ignore[method-assign]
            lambda *, deadline: (_ for _ in ()).throw(_QueryStarting(
                "consumer group g has no committed partitions yet",
                query_ids=(self.VIEW + "7",)))
        )

        with self.assertRaisesRegex(RuntimeError, "view restarts 2/2"):
            adapter._pull_stable(
                "SELECT * FROM view;",
                restart_view=lambda: restarted.append(True),
                view_query_prefix=self.VIEW,
            )
        self.assertEqual(len(restarted), 2)

    def test_existing_view_is_never_recreated(self) -> None:
        """The D5 read-existing path passes no restart hook."""
        from src.adapters.ksqldb import _QueryStarting

        adapter = self._adapter()
        adapter.options["read_startup_timeout_s"] = 0.3
        adapter.options["read_stable_max_timeout_s"] = 0.3
        adapter._persistent_query_lags = (  # type: ignore[method-assign]
            lambda *, deadline: (_ for _ in ()).throw(_QueryStarting(
                "consumer group g has no committed partitions yet",
                query_ids=(self.VIEW + "7",)))
        )

        with self.assertRaisesRegex(RuntimeError, "no committed partitions"):
            adapter._pull_stable(
                "SELECT * FROM view;", restart_view=None,
                view_query_prefix=self.VIEW)
        self.assertEqual(adapter.view_restarts, [])

    def test_stuck_upstream_query_fails_fast_without_restart(self) -> None:
        from src.adapters.ksqldb import _QueryStarting

        restarted = []
        adapter = self._adapter()
        adapter._persistent_query_lags = (  # type: ignore[method-assign]
            lambda *, deadline: (_ for _ in ()).throw(_QueryStarting(
                "consumer group g has no committed partitions yet",
                query_ids=("CSAS_VS_TEST_CORRECTED_3",)))
        )
        started = time.time()

        with self.assertRaisesRegex(RuntimeError, "stuck for .*queries: stubbed"):
            adapter._pull_stable(
                "SELECT * FROM view;",
                restart_view=lambda: restarted.append(True),
                view_query_prefix=self.VIEW,
            )
        self.assertEqual(restarted, [])
        self.assertLess(time.time() - started, 5)

    def test_query_in_error_reports_ksqldb_message(self) -> None:
        from src.adapters.ksqldb import _QueryFailed

        adapter = self._adapter()
        adapter._persistent_query_lags = (  # type: ignore[method-assign]
            lambda *, deadline: (_ for _ in ()).throw(_QueryFailed(
                "ksqlDB query CSAS_VS_TEST_CORRECTED_3 is in ERROR: disk full",
                query_ids=("CSAS_VS_TEST_CORRECTED_3",)))
        )

        with self.assertRaisesRegex(RuntimeError, "is in ERROR: disk full"):
            adapter._pull_stable(
                "SELECT * FROM view;", restart_view=None,
                view_query_prefix=self.VIEW)

    def test_explain_error_state_raises_query_failed(self) -> None:
        from src.adapters.ksqldb import _QueryFailed

        adapter = KsqlDbAdapter(run_suffix="readiness_test")

        def post(_path, sql, **_kwargs):  # noqa: ANN001
            if sql == "SHOW QUERIES;":
                return _JsonResponse([{"queries": [
                    {"id": "CTAS_VS_TEST_DAILY_7", "queryType": "PERSISTENT"},
                ]}])
            return _JsonResponse([{"queryDescription": {
                "consumerGroupId": "group-7",
                "state": "ERROR",
                "queryErrors": [{"errorMessage": "RocksDB open failed"}],
            }}])

        adapter._post = post  # type: ignore[method-assign]
        with self.assertRaises(_QueryFailed) as caught:
            adapter._query_consumer_groups(deadline=time.time() + 5)
        self.assertIn("RocksDB open failed", str(caught.exception))
        self.assertEqual(caught.exception.query_ids, ("CTAS_VS_TEST_DAILY_7",))

class FlinkTimestampContractTests(unittest.TestCase):
    def test_worker_uses_microsecond_timestamp_precision(self) -> None:
        source = inspect.getsource(_flink_worker.main)
        self.assertIn(
            'DataTypes.FIELD("event_time_utc", DataTypes.TIMESTAMP(6))',
            source,
        )


if __name__ == "__main__":
    unittest.main()
