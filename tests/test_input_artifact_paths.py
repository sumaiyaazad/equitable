"""Input-artifact containment across an isolated source snapshot.

A campaign executes from an immutable worktree under runs/source_snapshots/
whose data/ entries are symlinks into the primary checkout. Every run therefore
records an input path that lives inside the snapshot but resolves outside it.
Resolving before the containment check rejected all of them, so a collection run
from inside the snapshot found zero runs and the finalize path stopped with
"no current, fingerprinted diagnostic benchmark found". These tests pin the
lexical check that fixes it, and the traversal guard that keeps it honest.
"""
from __future__ import annotations

import hashlib
import json
import pathlib
import tempfile
import unittest

from scripts.figures import load_module

_io = load_module("_io")


class InputArtifactPathTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = pathlib.Path(self._tmp.name)
        self.primary = self.tmp / "primary"
        (self.primary / "data" / "fixtures").mkdir(parents=True)
        self.payload = self.primary / "data" / "fixtures" / "d1_dup.parquet"
        self.payload.write_bytes(b"fixture-bytes")
        self.sha = hashlib.sha256(self.payload.read_bytes()).hexdigest()
        # A snapshot whose data/fixtures points back into the primary checkout.
        self.snapshot = self.tmp / "snapshot"
        (self.snapshot / "data").mkdir(parents=True)
        (self.snapshot / "data" / "fixtures").symlink_to(
            self.primary / "data" / "fixtures")
        self._root = _io.ROOT
        _io._INPUT_SHA_CACHE.clear()

    def tearDown(self) -> None:
        _io.ROOT = self._root
        _io._INPUT_SHA_CACHE.clear()
        self._tmp.cleanup()

    def _summary(self, path: pathlib.Path) -> dict:
        return {"input": str(path), "input_sha256": self.sha}

    def test_snapshot_symlinked_input_is_accepted(self) -> None:
        _io.ROOT = self.snapshot
        inside = self.snapshot / "data" / "fixtures" / "d1_dup.parquet"
        self.assertTrue(_io.valid_input_artifact(self._summary(inside)))

    def test_path_outside_the_root_is_rejected(self) -> None:
        _io.ROOT = self.snapshot
        self.assertFalse(_io.valid_input_artifact(self._summary(self.payload)))

    def test_parent_traversal_is_rejected(self) -> None:
        _io.ROOT = self.snapshot
        sneaky = self.snapshot / "data" / ".." / ".." / "primary" / "data" \
            / "fixtures" / "d1_dup.parquet"
        self.assertFalse(_io.valid_input_artifact(self._summary(sneaky)))

    def test_wrong_hash_is_rejected(self) -> None:
        _io.ROOT = self.snapshot
        inside = self.snapshot / "data" / "fixtures" / "d1_dup.parquet"
        summary = self._summary(inside)
        summary["input_sha256"] = "b" * 64
        self.assertFalse(_io.valid_input_artifact(summary))

    def test_missing_file_is_rejected(self) -> None:
        _io.ROOT = self.snapshot
        absent = self.snapshot / "data" / "fixtures" / "absent.parquet"
        self.assertFalse(_io.valid_input_artifact(self._summary(absent)))


if __name__ == "__main__":
    unittest.main()
