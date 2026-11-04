"""A finalization-only commit must never orphan a finished campaign.

Every stage after measurement checks that HEAD matches the commit that measured
the cells. scripts/results/_finalizer.py relaxes that for a descendant that
changes only finalization code, so a collector bug found after a complete
campaign is fixed by one commit instead of by measuring every cell again. These
tests pin both halves of that rule: such a descendant is accepted, and anything
that touches measurement code is not.
"""
from __future__ import annotations

import ast
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.figures import load_module
from scripts.results import _finalizer

ROOT = Path(__file__).resolve().parents[1]
_io = load_module("_io")


def _git(repo: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", *args],
        cwd=repo, text=True).strip()


def _commit(repo: Path, files: dict[str, str], message: str) -> str:
    for relative, text in files.items():
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


class FinalizerPaths(unittest.TestCase):

    def test_reading_code_is_finalization_and_everything_else_is_not(self):
        for path in (
            "scripts/figures/collect_design_space.py",
            "scripts/results/promote_results.py",
            "scripts/results/finalize.sh",
            "scripts/verify/verify_result_pipeline.py",
            "tests/test_finalizer.py",
            "docs/RUNBOOK.md",
            "README.md",
        ):
            self.assertTrue(_finalizer.is_finalizer_path(path), path)
        for path in (
            "src/oracle/cache.py",
            "src/provenance.py",
            "scripts/run/run_diagnostic.py",
            "scripts/run/benchmark.sh",
            "scripts/verify/verify_oracle_cache.py",
            "scripts/verify/verify_data_snapshot.py",
            "config/default.yaml",
            "requirements.txt",
            "reproduce_all.sh",
            "docker/streaming-engines.yml",
            "scripts/figuresX/a.py",
            "README.md.orig",
        ):
            self.assertFalse(_finalizer.is_finalizer_path(path), path)

    def test_no_measurement_code_imports_a_finalization_module(self):
        """The rule is sound only while measurement never runs these paths.

        benchmark_status.py reads _io for its phase totals; it reports progress
        and writes no evidence, so it is the one reader allowed.
        """
        roots = [ROOT / "src", ROOT / "scripts" / "run",
                 ROOT / "scripts" / "ingest", ROOT / "scripts" / "perturb",
                 ROOT / "scripts" / "adapters", ROOT / "scripts" / "verify"]
        allowed = {ROOT / "scripts" / "run" / "benchmark_status.py"}
        offenders = []
        for directory in roots:
            for path in sorted(directory.rglob("*.py")):
                relative = path.relative_to(ROOT).as_posix()
                if path in allowed or _finalizer.is_finalizer_path(relative):
                    continue
                tree = ast.parse(path.read_text(), filename=relative)
                for node in ast.walk(tree):
                    names: list[str] = []
                    if isinstance(node, ast.Import):
                        names = [alias.name for alias in node.names]
                    elif isinstance(node, ast.ImportFrom) and node.module:
                        names = [node.module]
                    elif (isinstance(node, ast.Call)
                          and getattr(node.func, "id", None) == "load_module"):
                        names = ["figures"]
                    for name in names:
                        head = name.split(".")[:2]
                        if (head[:1] in (["figures"], ["results"])
                                or head in (["scripts", "figures"],
                                            ["scripts", "results"])):
                            offenders.append(f"{relative}: {name}")
        self.assertEqual(offenders, [])


class MeasurementChanges(unittest.TestCase):

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self._tmp.name)
        _git(self.repo, "init", "-q")
        self.evidence = _commit(self.repo, {
            "src/engine.py": "A = 1\n",
            "scripts/figures/collect.py": "EXPECTED = 7\n",
        }, "measured")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_the_evidence_commit_itself_may_finalize(self):
        self.assertEqual(
            _finalizer.measurement_changes(self.evidence, root=self.repo), [])

    def test_a_collector_fix_may_finalize(self):
        _commit(self.repo, {"scripts/figures/collect.py": "EXPECTED = 8\n"},
                "fix the collector")
        self.assertTrue(_finalizer.may_finalize(self.evidence, root=self.repo))

    def test_a_measurement_change_anywhere_since_is_refused(self):
        _commit(self.repo, {"scripts/figures/collect.py": "EXPECTED = 8\n"},
                "fix the collector")
        _commit(self.repo, {"src/engine.py": "A = 2\n"}, "change the engine")
        self.assertEqual(
            _finalizer.measurement_changes(self.evidence, root=self.repo),
            ["src/engine.py"])
        self.assertFalse(_finalizer.may_finalize(self.evidence, root=self.repo))

    def test_a_reverted_measurement_change_leaves_nothing_to_refuse(self):
        _commit(self.repo, {"src/engine.py": "A = 2\n"}, "change")
        _commit(self.repo, {"src/engine.py": "A = 1\n"}, "revert")
        self.assertTrue(_finalizer.may_finalize(self.evidence, root=self.repo))

    def test_a_commit_off_the_evidence_line_is_refused(self):
        _git(self.repo, "checkout", "-q", "--orphan", "other")
        _commit(self.repo, {"scripts/figures/collect.py": "EXPECTED = 8\n"},
                "unrelated history")
        self.assertIsNone(
            _finalizer.measurement_changes(self.evidence, root=self.repo))
        self.assertIsNone(
            _finalizer.measurement_changes("f" * 40, root=self.repo))


class CollectorDirtyFlag(unittest.TestCase):
    """The flag a collector writes is the one promotion refuses on."""

    def _dirty(self, *, may_finalize: bool, source_dirty: bool,
               head: str = "f" * 40) -> bool:
        with mock.patch.object(_io, "git_head", return_value="c" * 40), \
                mock.patch.object(_io._finalizer, "head", return_value=head), \
                mock.patch.object(_io._finalizer, "may_finalize",
                                  return_value=may_finalize), \
                mock.patch.object(_io, "provenance_source_dirty",
                                  return_value=source_dirty), \
                mock.patch.object(_io, "provenance_git_dirty",
                                  return_value=True):
            return _io.git_dirty()

    def test_clean_finalizer_only_descendant_is_not_dirty(self):
        self.assertFalse(self._dirty(may_finalize=True, source_dirty=False))

    def test_measurement_drift_or_a_dirty_tree_stays_dirty(self):
        self.assertTrue(self._dirty(may_finalize=False, source_dirty=False))
        self.assertTrue(self._dirty(may_finalize=True, source_dirty=True))

    def test_at_the_evidence_commit_the_ordinary_rule_applies(self):
        self.assertTrue(self._dirty(may_finalize=True, source_dirty=False,
                                    head="c" * 40))


class PromotableCollection(unittest.TestCase):

    def test_only_a_clean_collection_from_the_evidence_commit_is_found(self):
        """A dirty re-collection once shadowed the clean one and blocked promotion."""
        evidence = "c" * 40
        base = {"kind": "design_space_collect", "invocation_id": "x_grid50k",
                "required_engines": ["reference"], "artifact_valid": True,
                "git_dirty": False, "git_commit": evidence}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, overrides in (
                ("20260926T010000Z_design_space_collect", {}),
                ("20260926T020000Z_design_space_collect", {"git_dirty": True}),
                ("20260926T030000Z_design_space_collect",
                 {"git_commit": "d" * 40}),
                ("20260926T040000Z_design_space_collect",
                 {"artifact_valid": False}),
            ):
                run = root / "runs" / name
                run.mkdir(parents=True)
                (run / "summary.json").write_text(
                    json.dumps({**base, **overrides}))
            args = mock.Mock(kind="design_space_collect",
                             invocation_id="x_grid50k", domain=None,
                             required_engines="reference",
                             evidence_commit=evidence)
            with mock.patch.object(_finalizer, "ROOT", root), \
                    mock.patch("builtins.print") as printed:
                self.assertEqual(_finalizer._find(args), 0)
            self.assertEqual(Path(printed.call_args.args[0]).name,
                             "20260926T010000Z_design_space_collect")


class MeasuredCampaign(unittest.TestCase):
    """reproduce_all.sh finalizes a campaign at a later HEAD only once it is
    measured; anything short of that must resume measuring at its own commit."""

    COMMIT = "c" * 40
    RUN_ID = "benchmark_x"

    def _measured(self, mutate=lambda root: None) -> bool:
        source = (ROOT / "reproduce_all.sh").read_text()
        start = source.index("campaign_measured() {")
        body = source[start:source.index("\n}\n", start) + 3]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            current = root / "runs" / "benchmark_runs" / "current"
            current.mkdir(parents=True)
            (current / "prepare.complete").write_text("d" * 64 + "\n")
            for marker in ("reset-engines", "durability", "smoke", "validation",
                           "structural-confirmation"):
                (current / f"{marker}.complete").write_text(self.COMMIT + "\n")
            for engine in ("reference", "flink"):
                lane = root / "runs" / "jobs" / f"lane_{engine}"
                lane.mkdir(parents=True)
                (lane / "owner").write_text(self.RUN_ID + "\n")
                (lane / "status").write_text("DONE\n")
            mutate(root)
            script = (f'REQUIRED_LANES="reference flink"\n{body}\n'
                      f"campaign_measured {self.RUN_ID} {self.COMMIT}\n")
            result = subprocess.run(["bash", "-c", script], cwd=root,
                                    capture_output=True, text=True, check=False)
        return result.returncode == 0

    def test_every_gate_and_lane_finished_is_measured(self):
        self.assertTrue(self._measured())

    def test_a_skipped_structural_confirmation_is_not(self):
        def drop(root: Path) -> None:
            (root / "runs/benchmark_runs/current"
             / "structural-confirmation.complete").unlink()
        self.assertFalse(self._measured(drop))

    def test_a_gate_passed_at_another_commit_is_not(self):
        def stale(root: Path) -> None:
            (root / "runs/benchmark_runs/current/smoke.complete").write_text(
                "e" * 40 + "\n")
        self.assertFalse(self._measured(stale))

    def test_a_running_or_foreign_lane_is_not(self):
        def running(root: Path) -> None:
            (root / "runs/jobs/lane_flink/status").write_text("RUNNING\n")
        def foreign(root: Path) -> None:
            (root / "runs/jobs/lane_flink/owner").write_text("benchmark_y\n")
        self.assertFalse(self._measured(running))
        self.assertFalse(self._measured(foreign))


if __name__ == "__main__":
    unittest.main()
