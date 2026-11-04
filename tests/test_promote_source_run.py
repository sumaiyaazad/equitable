from __future__ import annotations

import importlib.util
import os
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load_promote():
    spec = importlib.util.spec_from_file_location(
        "promote_results", ROOT / "scripts" / "results" / "promote_results.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PromoteSourceRunTests(unittest.TestCase):
    def test_snapshot_and_primary_record_the_same_source_run(self) -> None:
        """A snapshot's runs/ is a symlink; promotion must not need the primary."""
        promote = _load_promote()
        with tempfile.TemporaryDirectory() as tmp:
            primary = Path(tmp, "primary")
            (primary / "runs" / "20260917T000000Z_collect").mkdir(parents=True)
            snapshot = Path(tmp, "snapshot")
            snapshot.mkdir()
            os.symlink(primary / "runs", snapshot / "runs")
            recorded = []
            for root in (primary, snapshot):
                promote.ROOT = root
                run_dir = (root / "runs" / "20260917T000000Z_collect").resolve()
                recorded.append(promote._source_run(run_dir))
        self.assertEqual(recorded, ["runs/20260917T000000Z_collect"] * 2)


if __name__ == "__main__":
    unittest.main()
