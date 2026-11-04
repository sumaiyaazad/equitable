"""Reproducibility metadata shared by experiment and collection scripts."""
from __future__ import annotations

import hashlib
import os
import platform
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, Optional

CONTRACT_VERSION = 2


def _root(root: Optional[Path] = None) -> Path:
    return (root or Path(__file__).resolve().parents[1]).resolve()


_BENCHMARK_OUTPUTS = (
    ".venv/",
    "runs/",
    "data/raw/",
    "data/normalized/",
    "data/fixtures/",
    "data/oracle_cache/",
    "results/",
)


def _is_benchmark_output(path: str) -> bool:
    """Match a generated path or an isolated-worktree symlink to its root."""
    return any(
        path == declared.rstrip("/") or path.startswith(declared)
        for declared in _BENCHMARK_OUTPUTS
    )


def _actual_git_commit(root: Optional[Path] = None) -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=_root(root), text=True
        ).strip()
    except Exception:
        return "unknown"


def _campaign_commit() -> Optional[str]:
    value = os.environ.get("EQUITABLE_SOURCE_COMMIT", "").strip().lower()
    enabled = os.environ.get("EQUITABLE_SOURCE_CLEAN") == "1"
    if enabled and len(value) == 40 and all(c in "0123456789abcdef" for c in value):
        return value
    return None


def git_commit(root: Optional[Path] = None) -> str:
    """Return the immutable campaign source commit when one is active."""
    return _campaign_commit() or _actual_git_commit(root)


def git_dirty(root: Optional[Path] = None) -> bool:
    """Detect source drift while allowing declared generated benchmark outputs.

    ``reproduce_all.sh`` starts only from a clean source tree, then
    exports its source commit.  Tracked manifests/results necessarily change as
    the benchmark runs; those paths do not make later cells dirty. Any source,
    configuration, lock, or HEAD change still invalidates the benchmark.
    """
    repo = _root(root)
    campaign = _campaign_commit()
    try:
        actual = _actual_git_commit(repo)
        if campaign is not None and actual != campaign:
            return True
        lines = subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=normal"],
            cwd=repo, text=True,
        ).splitlines()
        if campaign is None:
            return bool(lines)
        for line in lines:
            path = line[3:].split(" -> ")[-1]
            if path == "docs/RESULTS.md":
                continue
            if not _is_benchmark_output(path):
                return True
        return False
    except Exception:
        return True


def source_dirty(root: Optional[Path] = None) -> bool:
    """Return whether source/configuration changed, ignoring generated outputs."""
    repo = _root(root)
    try:
        lines = subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=normal"],
            cwd=repo, text=True,
        ).splitlines()
        for line in lines:
            path = line[3:].split(" -> ")[-1]
            if path == "docs/RESULTS.md":
                continue
            if not _is_benchmark_output(path):
                return True
        return False
    except Exception:
        return True


def _hash_files(root: Path, files: list[Path]) -> str:
    """Hash file names and bytes so configuration drift is detectable."""
    digest = hashlib.sha256()
    for path in sorted(files):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(relative + b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def benchmark_config_sha256(root: Optional[Path] = None) -> str:
    repo = _root(root)
    return _hash_files(repo, list((repo / "config").rglob("*.yaml")))


def dependency_lock_sha256(root: Optional[Path] = None) -> str:
    repo = _root(root)
    candidates = [repo / "requirements.txt", repo / ".python-version"]
    return _hash_files(repo, [path for path in candidates if path.is_file()])


def run_provenance(root: Optional[Path] = None) -> Dict[str, Any]:
    repo = _root(root)
    memory_bytes = None
    try:
        memory_bytes = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, OSError, ValueError):
        pass
    disk = shutil.disk_usage(repo)
    return {
        "contract_version": CONTRACT_VERSION,
        "git_commit": git_commit(repo),
        "git_dirty": git_dirty(repo),
        "invocation_id": os.environ.get("EQUITABLE_INVOCATION_ID"),
        "campaign_id": os.environ.get("EQUITABLE_CAMPAIGN_ID"),
        "campaign_phase": os.environ.get("EQUITABLE_CAMPAIGN_PHASE"),
        "repeat_index": int(os.environ.get("EQUITABLE_REPEAT_INDEX", "0")),
        "benchmark_config_sha256": benchmark_config_sha256(repo),
        "dependency_lock_sha256": dependency_lock_sha256(repo),
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "hostname": platform.node(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "memory_bytes": memory_bytes,
        "disk_total_bytes": disk.total,
    }
