from __future__ import annotations

import contextlib
import io
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import types
import unittest

from scripts.adapters.smoke import smoke_gate_accepts
from src.adapters.ksqldb import KsqlDbAdapter
from src.provenance import _is_benchmark_output


class SmokeGateTests(unittest.TestCase):
    def test_accepts_exact_equivalence(self) -> None:
        self.assertTrue(smoke_gate_accepts({"artifact_valid": True, "pass": True}))

    def test_accepts_reproduced_known_finding_without_changing_verdict(self) -> None:
        summary = {
            "artifact_valid": True,
            "pass": False,
            "known_finding": {"reproduced": True},
        }
        self.assertTrue(smoke_gate_accepts(summary))
        self.assertFalse(summary["pass"])

    def test_blocks_unknown_divergence_and_execution_error(self) -> None:
        self.assertFalse(smoke_gate_accepts({"artifact_valid": True, "pass": False}))
        self.assertFalse(smoke_gate_accepts({
            "artifact_valid": False,
            "pass": False,
            "known_finding": {"reproduced": True},
            "error": "connection failed",
        }))

class ProvenanceTests(unittest.TestCase):
    def test_generated_roots_and_descendants_are_allowed(self) -> None:
        for path in (
            ".venv",
            "runs",
            "runs/cell/summary.json",
            "data/raw",
            "data/normalized/environment_airquality",
        ):
            self.assertTrue(_is_benchmark_output(path), path)

    def test_similarly_named_source_path_is_not_allowed(self) -> None:
        self.assertFalse(_is_benchmark_output("runs_backup/tool.py"))

class KsqlTimestampTests(unittest.TestCase):
    def test_source_accepts_producer_fractional_timestamp_format(self) -> None:
        adapter = KsqlDbAdapter(run_suffix="timestamp_test")
        statements: list[str] = []
        adapter._terminate_all_queries = lambda: None  # type: ignore[method-assign]
        adapter._drop_retry = lambda _stmt: None  # type: ignore[method-assign]
        adapter._post = lambda _path, sql: statements.append(sql)  # type: ignore[method-assign]

        adapter.setup(variant="dedup")

        source = next(sql for sql in statements if "CREATE STREAM" in sql)
        self.assertIn(
            "TIMESTAMP_FORMAT='yyyy-MM-dd''T''HH:mm:ss.SSSSSSX'", source
        )


class KsqlProducerBackpressureTests(unittest.TestCase):
    """A full local producer queue is backpressure, not a failed cell.

    librdkafka raises BufferError from produce() once the queue is full. Left
    unhandled, the BufferError blocks the cell, exits the phase nonzero, and
    cancels the engine's later phases.
    """

    class _FakeProducer:
        """Accepts a produce() only after `full_for` rejections, like a queue
        that drains when poll() is served."""

        def __init__(self, full_for: int) -> None:
            self.remaining_full = full_for
            self.polls = 0
            self.produced: list[bytes] = []

        def __len__(self) -> int:
            return len(self.produced)

        def produce(self, _topic, value, callback):  # noqa: ANN001
            if self.remaining_full > 0:
                raise BufferError("Local: Queue full")
            self.produced.append(value)
            callback(None, None)

        def poll(self, _timeout=0):  # noqa: ANN001
            self.polls += 1
            if self.remaining_full > 0:
                self.remaining_full -= 1
            return 0

    def test_retries_the_same_row_until_the_queue_drains(self) -> None:
        adapter = KsqlDbAdapter(run_suffix="backpressure_test")
        producer = self._FakeProducer(full_for=3)
        adapter._producer = producer  # type: ignore[assignment]

        adapter._produce_with_backpressure(b"row", lambda _e, _m: None)

        # The row is enqueued exactly once, not dropped and not duplicated.
        self.assertEqual(producer.produced, [b"row"])
        self.assertGreaterEqual(producer.polls, 3)

    def test_a_wedged_broker_still_raises_rather_than_hanging(self) -> None:
        adapter = KsqlDbAdapter(
            run_suffix="backpressure_test", produce_backpressure_timeout_s=0)
        adapter._producer = self._FakeProducer(full_for=10**9)  # type: ignore[assignment]

        with self.assertRaises(RuntimeError) as caught:
            adapter._produce_with_backpressure(b"row", lambda _e, _m: None)
        self.assertIn("not draining", str(caught.exception))


class KsqlPullRetryTests(unittest.TestCase):
    """Transient pull-query failures are safe to retry because reads are inert."""

    def test_read_timeout_is_retried_within_the_deadline(self) -> None:
        import requests

        adapter = KsqlDbAdapter(
            run_suffix="pull_retry_test",
            pull_retry_interval_s=0,
            pull_ready_timeout_s=5,
        )
        attempts = [requests.exceptions.ReadTimeout("busy"), [{"CNT": 1}]]

        def pull(_sql, *, timeout_s=None):  # noqa: ANN001
            outcome = attempts.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            self.assertIsNotNone(timeout_s)
            return outcome

        adapter._pull = pull  # type: ignore[method-assign]
        self.assertEqual(adapter._pull_ready("SELECT 1;"), [{"CNT": 1}])
        self.assertEqual(attempts, [])

    def test_non_transient_parser_error_is_not_retried(self) -> None:
        adapter = KsqlDbAdapter(
            run_suffix="pull_retry_test", pull_retry_interval_s=0)
        calls = [0]

        def pull(_sql, *, timeout_s=None):  # noqa: ANN001
            calls[0] += 1
            raise ValueError("malformed response")

        adapter._pull = pull  # type: ignore[method-assign]
        with self.assertRaises(ValueError):
            adapter._pull_ready("SELECT 1;")
        self.assertEqual(calls[0], 1)


class KsqlUnmaintainedViewTests(unittest.TestCase):
    """A table with no persistent query behind it can never reach zero lag.

    A D5 cell reads such a table after the harness has terminated its queries;
    waiting for it to commit would spend the whole startup budget before
    failing.
    """

    def test_no_persistent_query_fails_after_the_restart_grace(self) -> None:
        import time
        from src.adapters.ksqldb import _QueryStarting

        adapter = KsqlDbAdapter(
            run_suffix="orphan_test",
            read_restart_after_s=0.2,
            read_startup_timeout_s=60,
            read_stable_max_timeout_s=60,
            read_poll_interval_s=0.02,
        )

        def lags(*, deadline):  # noqa: ANN001
            raise _QueryStarting(
                "ksqlDB reported no persistent queries for the materialized view")

        adapter._persistent_query_lags = lags  # type: ignore[method-assign]
        started = time.time()
        with self.assertRaisesRegex(RuntimeError, "no persistent query"):
            adapter._pull_stable(
                "SELECT * FROM vs_orphan_test_hourly;",
                view_query_prefix="CTAS_VS_ORPHAN_TEST_HOURLY_")
        self.assertLess(time.time() - started, 10)


class KsqlStreamRetentionTests(unittest.TestCase):
    """Delete-policy topics must never be time-deleted under a running cell.

    Stream sinks carry event time, so the broker's 7-day retention would remove
    D7's `corrected` stream within minutes of its creation; daily views would
    then read nothing or part of it.
    """

    def _setup_statements(self, variant: str) -> list:
        adapter = KsqlDbAdapter(run_suffix="retention_test")
        posted = []
        adapter._terminate_all_queries = lambda: None  # type: ignore[method-assign]
        adapter._drop_retry = lambda _stmt: None  # type: ignore[method-assign]
        adapter._post = (  # type: ignore[method-assign]
            lambda _path, ksql, **_kw: posted.append(" ".join(ksql.split())))
        adapter.setup(variant=variant)
        return posted

    def test_every_created_stream_topic_is_kept(self) -> None:
        for variant in ("naive", "dedup", "restate", "arrival_latest"):
            with self.subTest(variant=variant):
                for stmt in self._setup_statements(variant):
                    if stmt.startswith("CREATE STREAM") and "KAFKA_TOPIC" in stmt:
                        creates_topic = (
                            " AS SELECT" in stmt or "PARTITIONS=" in stmt)
                        if creates_topic:
                            self.assertIn("RETENTION_MS=-1", stmt)


class D5ResumeReadTests(unittest.TestCase):
    """D5's post-restart read must leave the recovered queries running."""

    def test_read_after_restart_detaches_and_never_closes(self) -> None:
        import pandas as pd
        from scripts.run import run_d5_recovery

        calls = []

        class Engine:
            def connect(self):
                return self

            def resume(self) -> None:
                calls.append("resume")

            def read_view(self, grain: str):
                calls.append(f"read_{grain}")
                return pd.DataFrame({"count": [1]})

            def detach(self) -> None:
                calls.append("detach")

            def close(self) -> None:
                calls.append("close")

        original = run_d5_recovery.get_adapter
        run_d5_recovery.get_adapter = lambda _engine: Engine()
        try:
            run_d5_recovery._read_views_after_restart("ksqldb", 5)
        finally:
            run_d5_recovery.get_adapter = original
        self.assertIn("detach", calls)
        self.assertNotIn("close", calls)

    def test_ksqldb_detach_leaves_persistent_queries_alone(self) -> None:
        adapter = KsqlDbAdapter(run_suffix="detach_test")
        terminated = []
        adapter._terminate_all_queries = (  # type: ignore[method-assign]
            lambda: terminated.append(True))
        adapter.detach()
        self.assertEqual(terminated, [])


class SparkDriverCallbackTests(unittest.TestCase):
    """The driver must not keep one socket per finished cell.

    foreachBatch queries call back into Python over Py4J, one connection per
    micro-batch thread, and stopping a query closes neither end, so an unclosed
    connection per cell exhausts the driver's descriptor limit within a scaling
    phase.
    """

    ROOT = pathlib.Path(__file__).resolve().parents[1]

    def _driver(self):
        import importlib.util

        path = self.ROOT / "docker" / "spark-cluster" / "driver_service.py"
        spec = importlib.util.spec_from_file_location("driver_service_test", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_stale_callback_sockets_are_shut_down_without_closing_the_reader(self) -> None:
        import socket
        import threading

        driver = self._driver()
        ours, peer = socket.socketpair()
        # The reader thread is blocked in readline() on this stream; closing the
        # stream from another thread deadlocks on the reader's lock, so the
        # connection object must never be closed from here.
        connection = types.SimpleNamespace(
            socket=ours, close=lambda: self.fail("connection.close() called"))
        server = types.SimpleNamespace(lock=threading.RLock(),
                                       connections=[connection])
        gateway = types.SimpleNamespace(_callback_server=server)
        original = driver.SparkContext._gateway
        driver.SparkContext._gateway = gateway
        try:
            driver.close_callback_connections()
        finally:
            driver.SparkContext._gateway = original
        peer.settimeout(5)
        self.assertEqual(peer.recv(1), b"")
        ours.close()
        peer.close()

    def test_status_reports_open_descriptors(self) -> None:
        driver = self._driver()
        self.assertIsInstance(driver.open_fds(), int)


class CollectorEngineScopeTests(unittest.TestCase):
    """Collectors may report a subset, while official promotion requires all."""

    def _resolver(self, module_name: str):
        import sys
        sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scripts"))
        from figures import load_module
        return load_module(module_name)._resolve_engines

    KNOWN = ["reference", "materialize", "risingwave", "ksqldb", "flink", "spark", "feldera"]

    def test_empty_spec_requires_every_engine(self) -> None:
        for module in ("collect_scaling", "collect_design_space"):
            with self.subTest(module=module):
                self.assertEqual(
                    self._resolver(module)("", self.KNOWN), set(self.KNOWN))

    def test_narrowing_drops_only_the_named_engine(self) -> None:
        resolve = self._resolver("collect_scaling")
        six = resolve("reference,materialize,risingwave,ksqldb,spark,feldera", self.KNOWN)
        self.assertNotIn("flink", six)
        self.assertEqual(len(six), 6)

    def test_a_typo_is_rejected_rather_than_silently_shrinking_the_set(self) -> None:
        resolve = self._resolver("collect_scaling")
        with self.assertRaises(SystemExit):
            resolve("reference,flnik", self.KNOWN)


class WorkflowCompletenessTests(unittest.TestCase):
    """The public workflow must run and collect every declared execution."""

    ROOT = pathlib.Path(__file__).resolve().parents[1]

    def test_full_workflow_runs_every_engine_in_every_phase(self) -> None:
        """Seven ordinary lanes, each running correctness, confirmation, scaling.

        The checkpointed Flink cluster needs about a minute per formulation, so
        Flink runs as an ordinary lane, every cohort covers all seven engines,
        and no phase may silently narrow.
        """
        source = (self.ROOT / "reproduce_all.sh").read_text()
        self.assertIn(
            'ENGINE_LANES="reference materialize risingwave ksqldb flink spark feldera"',
            source)
        self.assertIn('REQUIRED_LANES="$ENGINE_LANES"', source)
        self.assertIn("wait_for_lanes $REQUIRED_LANES", source)
        self.assertNotIn("BENCHMARK_ALLOW_INCOMPLETE_FINALIZE", source)
        self.assertIn("bash scripts/run/benchmark.sh lane $engine", source)
        for gone in ("SLOW_ENGINE", "SHARD_DOMAINS", "BENCHMARK_DOMAIN=$d"):
            self.assertNotIn(gone, source)
        # No lane may block on another lane's status before starting.
        self.assertNotIn("_correctness/status", source)
        self.assertIn('EXCLUDED_SCALING_ENGINES="${EXCLUDED_SCALING_ENGINES-}"', source)
        self.assertIn('EXCLUDED_CONFIRM_ENGINES="${EXCLUDED_CONFIRM_ENGINES-}"', source)

    def test_full_workflow_does_not_force_parallel_scaling(self) -> None:
        source = (self.ROOT / "reproduce_all.sh").read_text()
        start = source.index("launch_engine_lanes()")
        launch = source[start:source.index("\n}\n", start) + 3]
        self.assertNotIn("BENCHMARK_PARALLEL_SCALING=1", launch)

    def test_staged_run_requires_successful_durability(self) -> None:
        def check(markers: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
            with tempfile.TemporaryDirectory() as directory:
                marker_root = pathlib.Path(directory)
                for marker in markers:
                    (marker_root / f"{marker}.complete").write_text("fixture-commit\n")
                script = f'''
cd "{self.ROOT}"
export EQUITABLE_BENCHMARK_SOURCE_ONLY=1
source scripts/run/benchmark.sh
CURRENT="$1"
EQUITABLE_SOURCE_COMMIT=fixture-commit
require_data_snapshot() {{ :; }}
require_benchmark_ready
'''
                return subprocess.run(
                    ["bash", "-c", script, "readiness-test", directory],
                    capture_output=True,
                    text=True,
                    check=False,
                )

        without_durability = check(("reset-engines", "smoke", "validation"))
        self.assertNotEqual(without_durability.returncode, 0)
        self.assertIn("durability has not completed successfully",
                      without_durability.stderr)
        complete = check(("reset-engines", "durability", "smoke", "validation"))
        self.assertEqual(complete.returncode, 0, complete.stderr)

    def test_startup_readiness_executes_a_real_query(self) -> None:
        source = (self.ROOT / "reproduce_all.sh").read_text()
        start = source.index("_probe()")
        probe = source[start:source.index("\n}\n", start) + 3]
        self.assertIn("engine_probe.py", probe)


class EngineProbeTests(unittest.TestCase):
    """A readiness probe that cannot fail is worse than no probe at all.

    An engine whose internal services have died can keep accepting pgwire
    connections; a TCP-only probe passes while the lane sits blocked on a
    query that never returns. The probe must reject an engine that is
    listening but not serving.
    """

    def _probe(self):
        from scripts.run.engine_probe import probe
        return probe

    def _spark_status(self, body=None, error=None):  # noqa: ANN001
        import requests
        from unittest import mock

        response = mock.Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = body
        get = mock.patch.object(
            requests, "get",
            side_effect=error, return_value=None if error else response)
        with get:
            self._probe()("spark", 3)

    def test_spark_probe_accepts_a_ready_driver(self) -> None:
        self._spark_status({"ready": True, "queries": {}, "open_fds": 12})

    def test_spark_probe_rejects_a_driver_that_drops_requests(self) -> None:
        import requests

        with self.assertRaises(requests.exceptions.ConnectionError):
            self._spark_status(error=requests.exceptions.ConnectionError(
                "Remote end closed connection without response"))

    def test_spark_probe_rejects_a_driver_that_is_not_ready(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "not ready"):
            self._spark_status({"ready": False, "error": "recovery failed"})

    def test_a_listening_but_unserving_engine_is_rejected(self) -> None:
        from scripts.run import engine_probe

        original = dict(engine_probe._PG_WIRE["risingwave"])
        engine_probe._PG_WIRE["risingwave"] = dict(original, port=4599)
        try:
            with self.assertRaises(Exception):
                engine_probe.probe("risingwave", 3)
        finally:
            engine_probe._PG_WIRE["risingwave"] = original

    def test_main_reports_failure_as_a_nonzero_exit(self) -> None:
        from scripts.run import engine_probe

        original = dict(engine_probe._PG_WIRE["risingwave"])
        engine_probe._PG_WIRE["risingwave"] = dict(original, port=4599)
        argv = sys.argv
        sys.argv = ["engine_probe", "--engine", "risingwave", "--timeout", "3"]
        try:
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(engine_probe.main(), 1)
        finally:
            sys.argv = argv
            engine_probe._PG_WIRE["risingwave"] = original

    def test_risingwave_is_probed_past_its_frontend(self) -> None:
        """SELECT 1 is served by the frontend alone, so it cannot be the check.

        When RisingWave's meta service dies the frontend keeps answering
        SELECT 1 while FLUSH blocks, so a SELECT 1 probe passes every time and
        the watchdog never strikes.
        """
        from scripts.run import engine_probe

        self.assertEqual(engine_probe._PG_BARRIER.get("risingwave"), "FLUSH")

        issued = []

        class _Cursor:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def execute(self, sql): issued.append(sql)
            def fetchone(self): return (1,)

        class _Conn:
            def cursor(self): return _Cursor()
            def set_session(self, **kw): pass
            def close(self): pass

        psycopg2 = types.ModuleType("psycopg2")
        psycopg2.connect = lambda **kw: _Conn()
        original = sys.modules.get("psycopg2")
        sys.modules["psycopg2"] = psycopg2
        try:
            engine_probe.probe("risingwave", 5)
            self.assertIn("FLUSH", issued)
            # Materialize plans SELECT 1 through the coordinator already, so it
            # needs no barrier and must not pay for one on every readiness poll.
            issued.clear()
            engine_probe.probe("materialize", 5)
            self.assertNotIn("FLUSH", issued)
        finally:
            if original is None:
                del sys.modules["psycopg2"]
            else:
                sys.modules["psycopg2"] = original

    def test_engines_without_a_query_endpoint_do_not_claim_liveness(self) -> None:
        # kafka/flink/spark/reference have no query endpoint here; the probe must
        # return cleanly and leave the caller's TCP check authoritative rather
        # than inventing a check it cannot make.
        for engine in ("kafka", "flink", "spark", "reference"):
            with self.subTest(engine=engine):
                self.assertIsNone(self._probe()(engine, 1))


class StatusListingTests(unittest.TestCase):
    """`status` has to stay readable without hiding a diverging shard.

    Many shards plus jobs left over from earlier runs would print a screen of
    lines repeating the same status, which is how a wedged lane goes unnoticed.
    Agreeing shards collapse; a shard that differs must still be named.
    """

    ROOT = pathlib.Path(__file__).resolve().parents[1]
    DOMAINS = ("environment_airquality", "transportation_taxi",
               "datacenter_drives", "finance_markets", "sports_football")

    def _status(self, statuses: dict) -> str:
        """Run job.sh status against a throwaway repo holding these jobs."""
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            for name, status in statuses.items():
                directory = root / "runs" / "jobs" / name
                directory.mkdir(parents=True)
                (directory / "status").write_text(status + "\n")
                (directory / "pid").write_text("1\n")
            result = subprocess.run(
                ["bash", str(self.ROOT / "scripts" / "run" / "job.sh"), "status"],
                cwd=root, text=True, capture_output=True, check=False)
            self.assertEqual(result.returncode, 0, result.stderr)
            return result.stdout

    def test_agreeing_shards_collapse_to_one_line(self) -> None:
        out = self._status({
            f"lane_flink_correctness_{d}": "DONE" for d in self.DOMAINS})
        self.assertIn("lane_flink_correctness_* (5)", out)
        # One grouped line plus its domain roster, not five status lines.
        self.assertEqual(out.count("DONE"), 1)
        for domain in self.DOMAINS:
            self.assertIn(domain, out)

    def test_a_diverging_shard_is_still_named(self) -> None:
        statuses = {
            f"lane_flink_correctness_{d}": "DONE" for d in self.DOMAINS}
        statuses["lane_flink_correctness_finance_markets"] = "FAILED (exit 1)"
        out = self._status(statuses)
        self.assertIn("FAILED (exit 1)", out)
        # The odd one out must be attributable to its domain, not folded into
        # a count that would let it pass for one of the four that succeeded.
        failing_line = [ln for ln in out.splitlines() if "FAILED" in ln]
        self.assertTrue(failing_line, out)
        self.assertIn("lane_flink_correctness_finance_markets",
                      "".join(failing_line))
        self.assertIn("lane_flink_correctness_* (4)", out)

    def test_unsharded_jobs_are_listed_verbatim(self) -> None:
        # RUNNING is avoided here: a status file claiming RUNNING
        # with a pid this process does not own is correctly rewritten to LOST,
        # which is existing behaviour and not what this test is about.
        out = self._status({"lane_ksqldb": "STOPPED", "data": "DONE"})
        self.assertRegex(out, r"lane_ksqldb\s+STOPPED")
        self.assertRegex(out, r"data\s+DONE")
        self.assertNotIn("(1)", out)


class FinalizeGuardTests(unittest.TestCase):
    """`benchmark finish` locks the evidence the first time it succeeds.

    wait_for_lanes returns as soon as no lane is still RUNNING, which a FAILED or
    STOPPED lane satisfies just as well as a finished one. A lane stopped
    mid-campaign would drop off that wait and leave its engine with a partial
    grid and nothing between it and a permanent lock.
    """

    ROOT = pathlib.Path(__file__).resolve().parents[1]

    def _run_guard(self, statuses: dict) -> subprocess.CompletedProcess:
        source = (self.ROOT / "reproduce_all.sh").read_text()
        start = source.index("require_lanes_complete()")
        body = source[start:source.index("\n}\n", start) + 3]
        with tempfile.TemporaryDirectory() as tmp:
            for lane, status in statuses.items():
                if status is None:
                    continue
                directory = pathlib.Path(tmp) / "runs" / "jobs" / f"lane_{lane}"
                directory.mkdir(parents=True)
                (directory / "status").write_text(status + "\n")
            script = f"{body}\nrequire_lanes_complete {' '.join(statuses)}\n"
            return subprocess.run(
                ["bash", "-c", script], cwd=tmp, text=True, capture_output=True)

    def test_all_lanes_done_allows_finalizing(self) -> None:
        result = self._run_guard({"alpha": "DONE", "beta": "DONE"})
        self.assertEqual(result.returncode, 0)

    def test_a_failed_lane_refuses_the_irreversible_lock(self) -> None:
        result = self._run_guard({"alpha": "DONE", "beta": "FAILED (exit 1)"})
        self.assertEqual(result.returncode, 1)
        self.assertIn("lane_beta", result.stderr)

    def test_a_stopped_or_missing_lane_is_named_not_skipped(self) -> None:
        result = self._run_guard(
            {"alpha": "DONE", "beta": "STOPPED", "gamma": None})
        self.assertEqual(result.returncode, 1)
        self.assertIn("lane_beta: STOPPED", result.stderr)
        self.assertIn("lane_gamma: MISSING", result.stderr)


class EngineWatchdogTests(unittest.TestCase):
    """The watchdog kills a running phase, so its restraint matters most.

    It must fire on an engine that cannot answer a query, and must never fire
    on one that is merely busy or on an engine it cannot honestly probe at all;
    some engines legitimately run for hours without a word.
    """

    ROOT = pathlib.Path(__file__).resolve().parents[1]

    def _run(self, probe_exit: int, engine: str) -> bool:
        """Return whether the watchdog fired for this engine."""
        script = f"""
        cd {self.ROOT}
        export EQUITABLE_BENCHMARK_SOURCE_ONLY=1
        source scripts/run/benchmark.sh
        tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
        printf '#!/usr/bin/env bash\\nexit {probe_exit}\\n' > "$tmp/py"
        chmod +x "$tmp/py"; PY="$tmp/py"
        EQUITABLE_WATCHDOG_INTERVAL=1
        EQUITABLE_WATCHDOG_STRIKES=1
        sleep 20 & target=$!
        start_engine_watchdog {engine} "$target" "$tmp/marker" >/dev/null 2>&1
        for _ in 1 2 3 4 5 6; do
          [ -f "$tmp/marker" ] && break
          sleep 1
        done
        [ -f "$tmp/marker" ] && echo FIRED || echo QUIET
        kill "$target" 2>/dev/null; stop_engine_watchdog
        """
        out = subprocess.run(["bash", "-c", script], text=True,
                             capture_output=True, timeout=90)
        return "FIRED" in out.stdout

    def test_fires_when_the_engine_cannot_answer_a_query(self) -> None:
        self.assertTrue(self._run(probe_exit=1, engine="risingwave"))

    def test_does_not_fire_on_an_engine_that_still_answers(self) -> None:
        self.assertFalse(self._run(probe_exit=0, engine="risingwave"))

    def test_every_probe_call_site_bounds_the_probe_itself(self) -> None:
        """An unwrapped probe that blocks is silently identical to no watchdog.

        A wedged engine may not honour statement_timeout, so an unwrapped probe
        can block forever and freeze the watchdog loop that is supposed to be
        counting strikes against it.
        """
        for name in ("scripts/run/benchmark.sh", "reproduce_all.sh"):
            source = (self.ROOT / name).read_text()
            for line in source.splitlines():
                if "engine_probe.py" not in line or line.lstrip().startswith("#"):
                    continue
                self.assertRegex(
                    line, r"timeout \d+ .*engine_probe\.py",
                    f"{name}: unbounded probe call: {line.strip()}")

    def test_never_arms_for_engines_it_cannot_query_probe(self) -> None:
        # Flink has no liveness endpoint here; a cell can legitimately take
        # hours, so the watchdog must not watch it at all.
        self.assertFalse(self._run(probe_exit=1, engine="flink"))

    def test_watches_the_spark_driver(self) -> None:
        # The driver's /status answers without the setup lock, so a busy driver
        # still answers it; one that stopped answering cost 223 cells.
        self.assertTrue(self._run(probe_exit=1, engine="spark"))
        self.assertFalse(self._run(probe_exit=0, engine="spark"))


class StallDetectionTests(unittest.TestCase):
    """`status` must flag a wedged lane without crying wolf on a working one.

    Log mtime cannot tell these apart. Python block-buffers a redirected stdout,
    so a working lane can go a day without writing a line while its worker
    holds a core, and a wedged engine looks identical from the log. CPU
    progress separates them.
    """

    ROOT = pathlib.Path(__file__).resolve().parents[1]

    def _stall_note(self, name: str, sample_age: int, busy: bool,
                    epoch: str = "") -> str:
        """Return what status would append for a job in the given state.

        `epoch` overrides the worker-set stamp written into the baseline, which
        is how a test simulates the group having been restarted since.
        """
        stamp = epoch or '$(job_cpu_epoch "$target")'
        script = f"""
        STALL_SECONDS=3600
        cd {self.ROOT}
        eval "$(sed -n '/^fmt_duration()/,/^}}/p;/^job_cpu_seconds()/,/^}}/p;/^job_cpu_epoch()/,/^}}/p;/^stall_note()/,/^}}/p' scripts/run/job.sh)"
        tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
        mkdir -p "$tmp/{name}"
        {'setsid bash -c "while :; do :; done" & target=$!'
         if busy else 'setsid sleep 60 & target=$!'}
        sleep 0.3
        echo "$(( $(date +%s) - {sample_age} )) $(job_cpu_seconds "$target") {stamp}" \
            > "$tmp/{name}/.cpu_sample"
        {'sleep 1.2' if busy else 'true'}
        stall_note "$tmp/{name}" {name} "$target"
        kill $target 2>/dev/null
        """
        out = subprocess.run(["bash", "-c", script], text=True,
                             capture_output=True, timeout=60)
        return out.stdout

    def test_flags_a_lane_making_no_cpu_progress(self) -> None:
        note = self._stall_note("lane_wedged", sample_age=7200, busy=False)
        self.assertIn("STALLED", note)

    def test_does_not_flag_a_lane_that_is_still_burning_cpu(self) -> None:
        # The Flink case: silent log, real work. Must stay quiet.
        note = self._stall_note("lane_busy", sample_age=7200, busy=True)
        self.assertEqual(note.strip(), "")

    def test_never_flags_a_supervisor_that_idle_waits(self) -> None:
        # reproduce_all sleeps in wait_for_lanes for the whole campaign.
        note = self._stall_note("reproduce_all", sample_age=7200, busy=False)
        self.assertEqual(note.strip(), "")

    def test_does_not_flag_a_lane_whose_workers_restarted(self) -> None:
        """A restarted worker set must not read as a stall.

        CPU seconds count only the live processes in the group, so an engine
        reset starts the total again from zero and sweeps back through the same
        small integers. A baseline written hours earlier by a different worker
        can hold the same value, which would report a lane as STALLED while it
        is steadily producing results. A baseline from a different worker set
        is not comparable, and must re-baseline rather than accuse.
        """
        note = self._stall_note("lane_restarted", sample_age=7200, busy=False,
                                epoch="1")
        self.assertEqual(note.strip(), "")

    def test_legacy_two_field_sample_rebaselines_instead_of_accusing(self) -> None:
        """A sample written before the stamp existed cannot prove a stall."""
        script = f"""
        STALL_SECONDS=3600
        cd {self.ROOT}
        eval "$(sed -n '/^fmt_duration()/,/^}}/p;/^job_cpu_seconds()/,/^}}/p;/^job_cpu_epoch()/,/^}}/p;/^stall_note()/,/^}}/p' scripts/run/job.sh)"
        tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
        mkdir -p "$tmp/lane_legacy"
        setsid sleep 60 & target=$!
        sleep 0.3
        echo "$(( $(date +%s) - 7200 )) $(job_cpu_seconds "$target")" \
            > "$tmp/lane_legacy/.cpu_sample"
        stall_note "$tmp/lane_legacy" lane_legacy "$target"
        wc -w < "$tmp/lane_legacy/.cpu_sample"
        kill $target 2>/dev/null
        """
        out = subprocess.run(["bash", "-c", script], text=True,
                             capture_output=True, timeout=60)
        self.assertNotIn("STALLED", out.stdout)
        # and it migrated the file to the three-field format
        self.assertEqual(out.stdout.strip().splitlines()[-1].strip(), "3")

    def test_stays_quiet_until_the_window_is_damning(self) -> None:
        note = self._stall_note("lane_young", sample_age=60, busy=False)
        self.assertEqual(note.strip(), "")

class StructuralConfirmationTests(unittest.TestCase):
    """The pre-finish pass that re-measures structurally-disagreeing cells.

    `benchmark finish` locks evidence permanently, so a cell that scored a
    verdict against an engine being reset underneath it has exactly one chance
    to be caught. Such a cell records a FAIL with keys the oracle never had.
    """

    ROOT = pathlib.Path(__file__).resolve().parents[1]

    @classmethod
    def _detector(cls) -> str:
        """The inline python the stage runs, lifted out of the shell script."""
        source = (cls.ROOT / "reproduce_all.sh").read_text().splitlines()
        start = next(i for i, l in enumerate(source)
                     if l.rstrip().endswith("<<'PYEOF'"))
        end = next(i for i in range(start + 1, len(source))
                   if source[i].strip() == "PYEOF")
        return "\n".join(source[start + 1:end])

    def _run(self, summaries, cap=25):
        """Run the detector over a synthetic campaign; return stdout rows."""
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            current = root / "runs" / "benchmark_runs" / "current"
            current.mkdir(parents=True)
            (current / "benchmark.env").write_text(
                "EQUITABLE_BENCHMARK_RUN_ID=camp1\n")
            for run_id, payload in summaries:
                d = root / "runs" / run_id
                d.mkdir(parents=True)
                (d / "summary.json").write_text(json.dumps(payload))
            script = root / "detect.py"
            script.write_text(self._detector())
            out = subprocess.run([sys.executable, str(script), str(cap)],
                                 cwd=tmp, text=True, capture_output=True,
                                 timeout=60)
            return ([l for l in out.stdout.splitlines() if l.strip()],
                    out.stderr)

    @staticmethod
    def _cell(domain, *, phantom=0, rows=500, repeat=1, valid=True):
        return {
            "campaign_id": "camp1", "campaign_phase": "scaling",
            "invocation_id": "camp1_scaling", "engine": "risingwave",
            "domain": domain, "diagnostic": "D4", "requested_max_rows": rows,
            "repeat_index": repeat, "artifact_valid": valid,
            "primary": {"hourly": {
                "only_in_online": phantom, "only_in_oracle": 0,
                "duplicate_online_keys": 0, "duplicate_oracle_keys": 0,
                "non_equivalence": {"structural_violations": phantom}}},
        }

    def test_flags_a_cell_with_keys_the_oracle_never_had(self) -> None:
        rows, _ = self._run(
            [("20260908T100000Z_a", self._cell("taxi", phantom=4320))])
        self.assertEqual(len(rows), 1)
        self.assertIn("risingwave", rows[0])
        self.assertIn("taxi", rows[0])

    def test_ignores_a_clean_cell(self) -> None:
        rows, _ = self._run([("20260908T100000Z_a", self._cell("taxi"))])
        self.assertEqual(rows, [])

    def test_skips_a_cell_a_later_clean_run_already_superseded(self) -> None:
        """The sports_football case: already re-run, so not a candidate."""
        rows, _ = self._run([
            ("20260908T101634Z_a", self._cell("football", phantom=4320)),
            ("20260908T103428Z_b", self._cell("football")),
        ])
        self.assertEqual(rows, [])

    def test_still_flags_when_the_later_run_is_the_damaged_one(self) -> None:
        rows, _ = self._run([
            ("20260908T090000Z_a", self._cell("football")),
            ("20260908T101634Z_b", self._cell("football", phantom=4320)),
        ])
        self.assertEqual(len(rows), 1)

    def test_ignores_a_blocked_cell(self) -> None:
        # A blocked cell has no verdict and the lane retries it already.
        rows, _ = self._run([("20260908T100000Z_a",
                              self._cell("taxi", phantom=4320, valid=False))])
        self.assertEqual(rows, [])

    def test_a_sweep_aggregate_is_not_a_candidate(self) -> None:
        # Its `rows` is a list; it is not a cell and cannot be re-measured.
        agg = self._cell("taxi", phantom=4320)
        agg["requested_max_rows"] = [10, 20, 30]
        rows, _ = self._run([("20260908T100000Z_a", agg)])
        self.assertEqual(rows, [])

    def test_cap_declines_to_act_on_widespread_disagreement(self) -> None:
        """Widespread structural disagreement is a property of the run.

        Re-running it would be futile and would launder a systematic problem
        into a clean-looking table, so above the cap the stage reports only.
        """
        many = [("20260908T10%04dZ_x" % i, self._cell("d%d" % i, phantom=9))
                for i in range(6)]
        rows, err = self._run(many, cap=3)
        self.assertEqual(rows, [])
        self.assertIn("exceeds the cap", err)

    def test_stage_runs_before_the_evidence_is_locked(self) -> None:
        source = (self.ROOT / "reproduce_all.sh").read_text()
        called = source.index("\n  confirm_structural_outliers")
        finish = source.index("benchmark finish")
        self.assertLess(called, finish,
                        "re-measurement must happen before finish locks results")
