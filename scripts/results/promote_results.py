#!/usr/bin/env python3
"""
promote_results.py -- promote a finished run from runs/ into results/ (protocol
step: RUNBOOK "Promoting a run"; plan section 10.7). results/ is the SOLE input
to the paper: figure/table generators read nothing else, so a number reaches the
paper only through this script's audit trail.

What it does (append-only and resumable):
  1. copies runs/<run_id>/ atomically to results/<block>/<run_id>/
  2. appends one provenance row (date, run id, git commit, domain, engine,
     block, feeds) to docs/RESULTS.md

Inputs  : a run directory under runs/ containing summary.json
Outputs : results/<block>/<run_id>/ + one appended row in docs/RESULTS.md
Runtime : instant
Run as  : python scripts/results/promote_results.py runs/<run_id> --block oracle_verification
          python scripts/results/promote_results.py runs/<run_id> --block design_space --feeds "Tab 6, Fig 2"
"""
from __future__ import annotations

import argparse
import json
import hashlib
import shutil
import sys
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.provenance import _is_benchmark_output, run_provenance  # noqa: E402
from scripts.results._finalizer import measurement_changes  # noqa: E402
from scripts.results._scope import official_scope_failure  # noqa: E402

RESULTS = ROOT / "results"
RESULTS_MD = ROOT / "docs" / "RESULTS.md"

# One folder per experiment block.
BLOCKS = [
    "oracle_verification", "design_space", "design_space_confirmation",
    "scaling", "boundary",
    "d7s_sweep", "mutation", "native_semantics",
    "query_family", "disorder_sweep", "parallelism_sweep", "formulation_cost",
    "evidence", "smoke",
]


def _file_hashes(directory: Path) -> dict[str, str]:
    return {
        str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(directory.rglob("*"))
        if path.is_file() and path.name != "promotion_manifest.json"
    }


def _source_run(run_dir: Path) -> str:
    """Record a run as `runs/<name>`, whichever checkout promotes it.

    A campaign promotes from its source snapshot, whose `runs` entry is a
    symlink into the primary checkout. `run_dir` is resolved through that link,
    so it is not under the snapshot ROOT and `relative_to(ROOT)` would raise
    ValueError. Anchoring on the resolved runs directory gives the same string
    from either checkout, which is what verify_promotions checks.
    """
    return str(Path("runs") / run_dir.relative_to((ROOT / "runs").resolve()))


def _already_promoted(
    destination: Path, run_dir: Path, block: str, summary: dict,
) -> bool:
    manifest_path = destination / "promotion_manifest.json"
    if not manifest_path.is_file():
        return False
    try:
        manifest = json.loads(manifest_path.read_text())
    except (OSError, ValueError):
        return False
    source_hashes = _file_hashes(run_dir)
    return (
        manifest.get("source_run") == _source_run(run_dir)
        and manifest.get("block") == block
        and manifest.get("git_commit") == summary.get("git_commit")
        and manifest.get("invocation_id") == summary.get("invocation_id")
        and manifest.get("benchmark_config_sha256")
            == summary.get("benchmark_config_sha256")
        and manifest.get("dependency_lock_sha256")
            == summary.get("dependency_lock_sha256")
        and manifest.get("files_sha256") == source_hashes
        and _file_hashes(destination) == source_hashes
    )


def _log_row(
    run_dir: Path, summary: dict, block: str, feeds: str, promoted_utc: str,
) -> str:
    return "| {date} | `{run}` | `{commit}` | {domain} | {engine} | {block} | {feeds} |\n".format(
        date=promoted_utc[:10],
        run=run_dir.name,
        commit=str(summary.get("git_commit", "unknown"))[:12],
        domain=summary.get("domain", "-"),
        engine=summary.get("engine", "-"),
        block=block,
        feeds=feeds or "-",
    )


def _ensure_log_row(row: str, run_id: str, block: str) -> None:
    existing = RESULTS_MD.read_text() if RESULTS_MD.is_file() else ""
    marker = f"| `{run_id}` |"
    if any(marker in line and f"| {block} |" in line
           for line in existing.splitlines()):
        return
    with RESULTS_MD.open("a") as fh:
        fh.write(row)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run_dir", type=Path, help="Run directory under runs/ to promote.")
    ap.add_argument("--block", required=True, choices=BLOCKS,
                    help="Experiment block (destination folder under results/).")
    ap.add_argument("--feeds", default="",
                    help="Figure/table this run feeds, e.g. 'Tab 6, Fig 2'.")
    args = ap.parse_args()

    run_dir = args.run_dir.resolve()
    runs_root = (ROOT / "runs").resolve()
    if runs_root not in run_dir.parents:
        print(f"FAIL: {run_dir} is not a run directory under {runs_root}.")
        return 1
    summary_path = run_dir / "summary.json"
    if not summary_path.exists():
        print(f"FAIL: {summary_path} not found -- only completed runs (with a "
              f"summary.json) can be promoted.")
        return 1

    summary = json.loads(summary_path.read_text())
    if summary.get("contract_version") != 2:
        print("FAIL: summary.json must use contract_version=2.")
        return 1
    if summary.get("artifact_valid") is not True:
        print("FAIL: only a run with artifact_valid=true can be promoted.")
        return 1
    scope_failure = official_scope_failure(args.block, summary)
    if scope_failure:
        print(f"FAIL: {scope_failure}")
        return 1
    if summary.get("git_dirty") is not False:
        print("FAIL: run was produced from a dirty or unknown worktree.")
        return 1
    commit = str(summary.get("git_commit", ""))
    if len(commit) != 40 or any(c not in "0123456789abcdef" for c in commit.lower()):
        print("FAIL: summary.json must contain a full 40-character git_commit.")
        return 1

    expected_provenance = run_provenance(ROOT)
    invocation_id = summary.get("invocation_id")
    if not isinstance(invocation_id, str) or not invocation_id.strip():
        print("FAIL: summary.json must contain a non-empty invocation_id.")
        return 1
    for field in ("benchmark_config_sha256", "dependency_lock_sha256"):
        if summary.get(field) != expected_provenance[field]:
            print(f"FAIL: {field} does not match the current repository fingerprint.")
            return 1

    current_commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    if commit != current_commit:
        # A descendant that changed only finalization code may promote the
        # evidence (scripts/results/_finalizer.py); anything else would let a
        # checkout vouch for cells measured by code it no longer contains.
        changes = measurement_changes(commit, current_commit, ROOT)
        if changes is None:
            print(f"FAIL: run commit {commit} is not an ancestor of HEAD "
                  f"{current_commit}.")
            return 1
        if changes:
            print(f"FAIL: HEAD {current_commit} changes code that measured run "
                  f"commit {commit}: {', '.join(changes[:10])}")
            return 1
    dirty_lines = subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=normal"],
        cwd=ROOT, text=True,
    ).splitlines()
    disallowed = []
    for line in dirty_lines:
        path = line[3:].split(" -> ")[-1]
        if path != "docs/RESULTS.md" and not _is_benchmark_output(path):
            disallowed.append(line)
    if disallowed:
        print("FAIL: refuse to promote with source/config changes in the worktree:")
        for line in disallowed[:20]:
            print(f"  {line}")
        return 1

    dest = RESULTS / args.block / run_dir.name
    if dest.exists():
        if _already_promoted(dest, run_dir, args.block, summary):
            manifest = json.loads((dest / "promotion_manifest.json").read_text())
            row = _log_row(
                run_dir, summary, args.block, args.feeds,
                str(manifest["promoted_utc"]),
            )
            _ensure_log_row(row, run_dir.name, args.block)
            print(f"PASS: identical evidence is already promoted at {dest}.")
            return 0
        print(f"FAIL: {dest} exists but does not match this source run.")
        return 1

    dest.parent.mkdir(parents=True, exist_ok=True)
    file_hashes = _file_hashes(run_dir)
    promoted_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    manifest = {
        "promoted_utc": promoted_utc,
        "source_run": _source_run(run_dir),
        "git_commit": commit,
        "finalizer_commit": current_commit,
        "invocation_id": invocation_id,
        "benchmark_config_sha256": summary["benchmark_config_sha256"],
        "dependency_lock_sha256": summary["dependency_lock_sha256"],
        "block": args.block,
        "files_sha256": file_hashes,
    }
    staging = Path(tempfile.mkdtemp(prefix=f".{run_dir.name}.", dir=dest.parent))
    try:
        shutil.copytree(run_dir, staging, dirs_exist_ok=True)
        (staging / "promotion_manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        staging.rename(dest)
    finally:
        if staging.exists():
            shutil.rmtree(staging)

    row = _log_row(run_dir, summary, args.block, args.feeds, promoted_utc)
    _ensure_log_row(row, run_dir.name, args.block)

    print(f"promoted : {dest}")
    print(f"logged   : {RESULTS_MD} (+1 row)")
    print("PASS: run promoted; results/ remains append-only.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
