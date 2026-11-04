from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BENCHMARK = ROOT / "scripts/run/benchmark.sh"


class BenchmarkKernelLockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(
            prefix="equitable-lock-test-", dir="/tmp"
        )
        self.lock_root = Path(self.temporary.name)
        self.environment = os.environ.copy()
        self.environment.update({
            "BENCHMARK_PATH": str(BENCHMARK),
            "LOCK_TEST_ROOT": str(self.lock_root),
        })

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def _script(body: str) -> str:
        return f'''set -euo pipefail
export EQUITABLE_BENCHMARK_SOURCE_ONLY=1
source "$BENCHMARK_PATH"
LOCK_ROOT="$LOCK_TEST_ROOT"
{body}
'''

    def _run(self, body: str, **environment: str) -> subprocess.CompletedProcess[str]:
        env = self.environment.copy()
        env.update(environment)
        return subprocess.run(
            ["bash", "-c", self._script(body)],
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )

    def _popen(self, body: str, **environment: str) -> subprocess.Popen[str]:
        env = self.environment.copy()
        env.update(environment)
        return subprocess.Popen(
            ["bash", "-c", self._script(body)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def test_no_lane_level_serialization_survives(self) -> None:
        """Lanes must never queue behind one another.

        A global flock around the scaling phase would leave an engine whose
        grid and confirmation were already finished blocked for hours, burning
        no CPU and tripping the stall detector. Nothing measured needs it:
        Table 8 reports verdict stability, not throughput, and
        reset_one_engine_volume is scoped to one engine's services and volume.
        This test fails if any lane-wide mutex is introduced.
        """
        source = BENCHMARK.read_text()
        for forbidden in (
            "acquire_scaling_lock",
            "release_scaling_lock",
            "SCALING_LOCK",
            "BENCHMARK_PARALLEL_SCALING",
        ):
            self.assertNotIn(forbidden, source)

    def test_shard_locks_are_scoped_per_domain(self) -> None:
        """Two domains of one phase+engine are different shards, not rivals.

        Sharding Flink five ways only works if the lock name, the watchdog
        marker, and the ksqlDB run suffix all carry the domain tag; sharing any
        one of them would make the second shard either abort or clobber the
        first.
        """
        source = BENCHMARK.read_text()
        self.assertIn('local domain="${BENCHMARK_DOMAIN:-all}" dtag=""', source)
        self.assertIn('[ "$domain" = all ] || dtag="-$domain"', source)
        self.assertIn(
            'lock="$LOCK_ROOT/.benchmark-${phase}-${engine}${dtag}.flock"', source)
        self.assertIn(
            'WATCHDOG_MARKER="$LOCK_ROOT/.watchdog-${phase}-${engine}${dtag}"',
            source)
        self.assertIn('DOMAINS="$domain"', source)

    def test_live_shard_rejects_all_concurrent_starts_then_reopens(self) -> None:
        holder = self._popen(r'''
acquire_shard_lock "$LOCK_ROOT/shard.flock" scaling reference
: > "$LOCK_ROOT/ready"
sleep 0.8
release_shard_lock
''')
        deadline = time.monotonic() + 5
        while not (self.lock_root / "ready").exists():
            if holder.poll() is not None:
                stdout, stderr = holder.communicate()
                self.fail(f"holder exited early: {stdout}{stderr}")
            if time.monotonic() >= deadline:
                holder.kill()
                self.fail("holder did not acquire shard lock")
            time.sleep(0.01)

        contender_body = r'''
set +e
acquire_shard_lock "$LOCK_ROOT/shard.flock" scaling reference
status=$?
set -e
if [ "$status" -eq 0 ]; then
  printf '%s\n' "$CONTENDER_ID" >> "$LOCK_ROOT/overlap"
  release_shard_lock
  exit 90
fi
[ "$status" -eq 1 ]
'''
        contenders = [
            self._popen(contender_body, CONTENDER_ID=str(index))
            for index in range(10)
        ]
        outputs = [process.communicate(timeout=10) for process in contenders]
        for process, (stdout, stderr) in zip(contenders, outputs):
            self.assertEqual(process.returncode, 0, stdout + stderr)
            self.assertIn("shard already running", stderr)
        self.assertFalse((self.lock_root / "overlap").exists())

        holder_stdout, holder_stderr = holder.communicate(timeout=5)
        self.assertEqual(holder.returncode, 0, holder_stdout + holder_stderr)
        reopened = self._run(r'''
acquire_shard_lock "$LOCK_ROOT/shard.flock" scaling reference
release_shard_lock
''')
        self.assertEqual(reopened.returncode, 0, reopened.stdout + reopened.stderr)
        self.assertTrue((self.lock_root / "shard.flock").is_file())


if __name__ == "__main__":
    unittest.main()
