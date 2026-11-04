"""A fresh clone's single-command reproduction builds its environment.

Inside the source snapshot, .venv is a symlink to the primary checkout's .venv,
and `python -m venv` refuses a symlink, so setup_environment builds at the
resolved path. These tests pin the mechanism and the call.
"""
from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _setup_function() -> str:
    source = (ROOT / "reproduce_all.sh").read_text()
    start = source.index("setup_environment() {")
    return source[start:source.index("\n}\n", start) + 3]


class VenvThroughSymlinkTests(unittest.TestCase):
    def test_setup_resolves_the_venv_path_before_creating_it(self) -> None:
        body = _setup_function()
        self.assertNotIn('-m venv "$ROOT/.venv"', body)
        self.assertIn("os.path.realpath(sys.argv[1])", body)

    def test_venv_builds_through_a_symlinked_path_once_resolved(self) -> None:
        directory = Path(tempfile.mkdtemp(prefix="vs-venv-link-"))
        self.addCleanup(shutil.rmtree, directory, True)
        real, link = directory / "real", directory / "link"
        real.mkdir()
        link.symlink_to(real)
        refused = subprocess.run([sys.executable, "-m", "venv", "--without-pip", str(link)],
                                 capture_output=True, text=True, check=False)
        self.assertIn("Unable to create directory", refused.stdout + refused.stderr)
        resolved = subprocess.run(
            [sys.executable, "-c", "import os, sys; print(os.path.realpath(sys.argv[1]))", str(link)],
            capture_output=True, text=True, check=True).stdout.strip()
        subprocess.run([sys.executable, "-m", "venv", "--without-pip", resolved], check=True)
        self.assertTrue((link / "bin" / "python").exists())


if __name__ == "__main__":
    unittest.main()
