"""The campaign stops cleanly on low disk, and D5 barriers on Spark's commit log.

A disk that fills mid-phase takes Kafka and then the ksqlDB and RisingWave
lanes down without a status; a lane stopped for a brief dip below the floor
must be restarted or the reproduction refuses to finalize; and a stale progress
file must not block a Spark D5 cell whose batches are already committed. These
tests pin all three guards.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from src.adapters.spark_cluster import SparkClusterAdapter

ROOT = Path(__file__).resolve().parents[1]
BENCHMARK = ROOT / "scripts" / "run" / "benchmark.sh"
REPRODUCE = ROOT / "reproduce_all.sh"


def _function(source: str, name: str) -> str:
    start = source.index(f"{name}() {{")
    return source[start:source.index("\n}\n", start) + 3]


def _source_benchmark(body: str) -> subprocess.CompletedProcess[str]:
    script = f'''set -euo pipefail
export EQUITABLE_BENCHMARK_SOURCE_ONLY=1
source "{BENCHMARK}"
reset_one_engine_volume() {{ echo "RESET $1"; }}
{body}
'''
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True,
                          timeout=30, check=False, env=os.environ.copy())


class LaneDiskGuardTests(unittest.TestCase):
    def test_enough_space_passes_without_a_reset(self) -> None:
        result = _source_benchmark('''
equitable_free_gib() { echo 200; }
require_free_disk ksqldb scaling
echo passed
''')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("passed", result.stdout)
        self.assertNotIn("RESET", result.stdout)

    def test_low_space_stops_the_lane_with_exit_three(self) -> None:
        result = _source_benchmark('''
equitable_free_gib() { echo 10; }
BENCHMARK_DISK_WAIT_S=0
code=0
require_free_disk ksqldb scaling || code=$?
echo "code=$code"
''')
        self.assertIn("code=3", result.stdout)
        self.assertIn("STOPPING before scaling", result.stderr)
        self.assertNotIn("RESET", result.stdout)

    def test_a_short_dip_waits_instead_of_stopping_the_lane(self) -> None:
        """A brief dip: 39 GiB at a phase boundary, back within minutes."""
        with tempfile.TemporaryDirectory(prefix="vs-disk-wait-") as directory:
            counter = Path(directory) / "calls"
            result = _source_benchmark(f'''
sleep() {{ echo "SLEEP $1"; }}
equitable_free_gib() {{
  echo x >> "{counter}"
  if [ "$(wc -l < "{counter}")" -ge 4 ]; then echo 120; else echo 39; fi
}}
BENCHMARK_DISK_WAIT_S=600
BENCHMARK_DISK_POLL_S=60
require_free_disk flink scaling
echo passed
''')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("waiting up to 600s", result.stdout)
        self.assertEqual(result.stdout.count("SLEEP 60"), 3)
        self.assertIn("120 GiB free after 180s; starting scaling", result.stdout)
        self.assertIn("passed", result.stdout)
        self.assertNotIn("STOPPING", result.stderr)

    def test_a_dip_longer_than_the_wait_still_stops_with_exit_three(self) -> None:
        result = _source_benchmark('''
sleep() { :; }
equitable_free_gib() { echo 39; }
BENCHMARK_DISK_WAIT_S=180
BENCHMARK_DISK_POLL_S=60
code=0
require_free_disk flink scaling || code=$?
echo "code=$code"
''')
        self.assertIn("code=3", result.stdout)
        self.assertIn("STOPPING before scaling: 39 GiB free after waiting 180s", result.stderr)

    def test_a_zero_poll_interval_cannot_spin(self) -> None:
        result = _source_benchmark('''
sleep() { echo "SLEEP $1"; }
equitable_free_gib() { echo 39; }
BENCHMARK_DISK_WAIT_S=2
BENCHMARK_DISK_POLL_S=0
require_free_disk flink scaling || echo "code=$?"
''')
        self.assertIn("code=3", result.stdout)
        self.assertEqual(result.stdout.count("SLEEP 1"), 2)

    def test_feldera_reclaims_its_compiled_programs_before_giving_up(self) -> None:
        result = _source_benchmark('''
calls=0
equitable_free_gib() {
  if [ -e "$TMPDIR_MARK" ]; then echo 300; else echo 10; fi
}
reset_one_engine_volume() { echo "RESET $1"; : > "$TMPDIR_MARK"; }
require_free_disk feldera scaling
echo passed
''' .replace("$TMPDIR_MARK", tempfile.mktemp(prefix="vs-disk-guard-")))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("RESET feldera", result.stdout)
        self.assertIn("passed", result.stdout)

    def test_the_lane_resets_feldera_after_every_phase_and_checks_disk_first(self) -> None:
        source = BENCHMARK.read_text()
        lane = source[source.index("run_lane() {"):]
        lane = lane[:lane.index("\n}\n")]
        self.assertEqual(lane.count('require_free_disk "$engine" "$phase"'), 2)
        self.assertIn('if [ "$engine" = feldera ]; then\n      reset_one_engine_volume feldera', lane)
        self.assertIn('if [ "$code" = 3 ]; then', lane)


class CoordinatorDiskRestartTests(unittest.TestCase):
    """The coordinator restarts a lane that stopped for disk once space is back."""

    def _run(self, body: str, lanes: dict[str, str]) -> subprocess.CompletedProcess[str]:
        source = REPRODUCE.read_text()
        directory = tempfile.mkdtemp(prefix="vs-disk-restart-")
        self.addCleanup(shutil.rmtree, directory, True)
        for engine, code in lanes.items():
            lane = Path(directory) / "runs" / "jobs" / f"lane_{engine}"
            lane.mkdir(parents=True)
            (lane / "exit_code").write_text(f"{code}\n")
            (lane / "log.txt").write_text(f"old log of {engine}\n")
        script = f'''set -euo pipefail
cd "{directory}"
BENCHMARK_MIN_FREE_GIB=40
BENCHMARK_DISK_RESTARTS=2
disk_restarts=""
lane_job_live() {{ return 1; }}
launch_lane() {{ echo "LAUNCH $1"; rm -f "runs/jobs/lane_$1/exit_code"; }}
guard_disk_while_waiting() {{ echo "GUARD"; }}
{_function(source, "restart_disk_stopped_lanes")}
{body}
echo "dir={directory}"
'''
        return subprocess.run(["bash", "-c", script], capture_output=True, text=True,
                              timeout=30, check=False)

    def test_only_a_disk_stopped_lane_restarts_and_its_log_is_kept(self) -> None:
        result = self._run('''
free_gib() { echo 120; }
restart_disk_stopped_lanes flink ksqldb materialize
''', {"flink": "3", "ksqldb": "1", "materialize": "0"})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("LAUNCH flink", result.stdout)
        self.assertNotIn("LAUNCH ksqldb", result.stdout)
        self.assertNotIn("LAUNCH materialize", result.stdout)
        self.assertIn("lane_flink stopped for disk; 120 GiB free now, restarting it (1/2)",
                      result.stdout)
        directory = Path(result.stdout.rsplit("dir=", 1)[1].strip())
        kept = directory / "runs" / "jobs" / "lane_flink" / "log.disk-stop-1.txt"
        self.assertEqual(kept.read_text(), "old log of flink\n")

    def test_a_lane_stays_stopped_while_the_disk_is_still_low(self) -> None:
        result = self._run('''
free_gib() { echo 39; }
restart_disk_stopped_lanes flink
''', {"flink": "3"})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("GUARD", result.stdout)
        self.assertNotIn("LAUNCH", result.stdout)

    def test_restarts_are_capped(self) -> None:
        result = self._run('''
free_gib() { echo 120; }
for attempt in 1 2 3 4; do
  echo 3 > runs/jobs/lane_flink/exit_code
  restart_disk_stopped_lanes flink
done
''', {"flink": "3"})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.count("LAUNCH flink"), 2)

    def test_the_wait_loop_restarts_before_deciding_the_lanes_are_finished(self) -> None:
        waiting = _function(REPRODUCE.read_text(), "wait_for_lanes")
        restart = waiting.index('restart_disk_stopped_lanes "$@" || return 1')
        self.assertLess(restart, waiting.index('[ "$live" = 0 ] && break'))


class CoordinatorDiskWatchTests(unittest.TestCase):
    def test_waiting_checks_disk_and_a_failed_wait_stops_the_workflow(self) -> None:
        source = (ROOT / "reproduce_all.sh").read_text()
        waiting = source[source.index("wait_for_lanes() {"):]
        waiting = waiting[:waiting.index("\n}\n")]
        self.assertIn('guard_disk_while_waiting "$@" || return 1', waiting)
        self.assertIn("wait_for_lanes $REQUIRED_LANES || return 1", source)
        guard = source[source.index("guard_disk_while_waiting() {"):]
        guard = guard[:guard.index("\n}\n")]
        self.assertIn("reset_one_engine_volume feldera", guard)
        self.assertIn('scripts/run/job.sh stop "lane_$engine"', guard)

    def test_feldera_compiler_cleanup_is_enabled(self) -> None:
        compose = (ROOT / "docker" / "streaming-engines.yml").read_text()
        feldera = compose[compose.index("  feldera:"):]
        self.assertIn("FELDERA_UNSTABLE_FEATURES: rust_compiler_full_cleanup",
                      feldera[:feldera.index("healthcheck:")])

    def test_status_names_a_job_that_died_without_a_status(self) -> None:
        jobs = (ROOT / "scripts" / "run" / "job.sh").read_text()
        self.assertEqual(jobs.count('[ -n "$st" ] || st="DIED'), 2)


class SparkClusterBarrierTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="vs-spark-cluster-")
        self.root = Path(self.temporary.name)
        self.adapter = SparkClusterAdapter(data_root=str(self.root), fold_timeout_s=2)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _commit(self, filename: str, batch: int, *, commit: bool = True) -> None:
        for grain in ("hourly", "daily"):
            sources = self.root / "ckpt" / grain / "sources" / "0"
            sources.mkdir(parents=True, exist_ok=True)
            (sources / str(batch)).write_text(
                "v1\n" + json.dumps({"path": f"/data/input/{filename}", "batchId": batch}))
            if commit:
                commits = self.root / "ckpt" / grain / "commits"
                commits.mkdir(parents=True, exist_ok=True)
                (commits / str(batch)).write_text("v1\n{}")

    def test_barrier_passes_on_the_commit_log(self) -> None:
        self._commit("part_1.parquet", 0)
        self.adapter._await_fold("part_1.parquet")
        self.adapter.checkpoint()

    def test_barrier_refuses_before_anything_was_committed(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "no ingested batch"):
            self.adapter.checkpoint()

    def test_barrier_refuses_when_a_commit_disappears(self) -> None:
        self._commit("part_1.parquet", 4)
        self.adapter._await_fold("part_1.parquet")
        (self.root / "ckpt" / "daily" / "commits" / "4").unlink()
        with self.assertRaisesRegex(RuntimeError, "batch 4 is not in commits"):
            self.adapter.checkpoint()

    def test_rereading_input_after_a_restart_fails_the_cell(self) -> None:
        """A driver that lost its checkpoint re-reads files and still totals right."""
        self._commit("part_1.parquet", 0)
        self.adapter._await_fold("part_1.parquet")
        for grain in ("hourly", "daily"):
            sources = self.root / "ckpt" / grain / "sources" / "0"
            (sources / "1").write_text(
                "v1\n" + json.dumps({"path": "/data/input/part_1.parquet", "batchId": 1}))
        with self.assertRaisesRegex(RuntimeError, "re-read input"):
            self.adapter._assert_no_reprocessing()


if __name__ == "__main__":
    unittest.main()
