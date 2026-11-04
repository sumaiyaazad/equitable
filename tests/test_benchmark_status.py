from __future__ import annotations

import contextlib
import io
import json
import re
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import scripts.run.benchmark_status as status

RUN_ID = "benchmark_test"


def _write(root: Path, name: str, **summary) -> None:
    run = root / name
    run.mkdir(parents=True)
    (run / "summary.json").write_text(json.dumps({"campaign_id": RUN_ID, **summary}))


def _cell(engine, domain, diagnostic, *, phase="confirm1m", rows=1_000_000,
          repeat=1, **extra):
    return {"campaign_phase": phase, "engine": engine, "domain": domain,
            "diagnostic": diagnostic, "requested_max_rows": rows,
            "repeat_index": repeat, "artifact_valid": True, "pass": True, **extra}


class BenchmarkStatusCountTests(unittest.TestCase):
    def _render(self, root: Path) -> str:
        buffer = io.StringIO()
        with mock.patch.object(status, "RUNS", root), \
                contextlib.redirect_stdout(buffer):
            latest = status.latest_cells(RUN_ID)
            universe = status.phase_universes()["confirm1m"]
            status.print_phase("confirm1m", universe, latest.get("confirm1m", {}))
        return buffer.getvalue()

    def test_counts_partition_the_universe_and_ignore_non_cells(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            n = 0
            for engine in status.ENGINES:
                for domain in status.DOMAINS:
                    for diag in status.DIAGNOSTICS:
                        if (engine, domain, diag) == ("ksqldb", "finance_markets", "D7"):
                            continue                      # missing
                        n += 1
                        extra = {}
                        if (engine, diag) == ("materialize", "D3"):
                            extra = {"pass": False}       # divergence
                        _write(root, f"r{n:05d}", **_cell(engine, domain, diag, **extra))
            # A blocked attempt later replaced by a valid run counts once.
            _write(root, "a0000_blocked", **_cell(
                "ksqldb", "sports_football", "D1", artifact_valid=False, error="x"))
            # The newest run of a cell is blocked: it counts as blocked.
            _write(root, "z9999_blocked", **_cell(
                "flink", "sports_football", "D2", artifact_valid=False, error="x"))
            # Not cells of this grid.
            _write(root, "c_collect", campaign_phase="confirm1m")

            text = self._render(root)

        head = text.splitlines()[0]
        complete, total = map(int, re.search(r"(\d+)/(\d+)", head).groups())
        counts = {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", head)}
        self.assertEqual(total, 280)
        self.assertEqual(counts["missing"], 1)
        self.assertEqual(counts["blocked"], 1)
        self.assertEqual(complete + counts["blocked"] + counts["missing"], total)
        self.assertEqual(
            sum(counts.get(k, 0) for k in status.OUTCOME_ORDER), complete)
        self.assertEqual(counts["divergence"], 5)
        self.assertIn("ksqldb=39/40", text)
        self.assertIn("flink=39/40", text)
        self.assertNotIn("None", text)
        self.assertNotIn("WARNING", text)


if __name__ == "__main__":
    unittest.main()
