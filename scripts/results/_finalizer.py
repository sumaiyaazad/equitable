#!/usr/bin/env python3
"""Decide whether this checkout may finalize evidence measured at another commit.

A campaign's cells are bound to the commit that measured them, and collection,
promotion, locking and packing check HEAD against that commit. Those stages
only read evidence, yet a bug in one of them can only be fixed by a commit, and
requiring HEAD to equal the measuring commit would make every cell foreign to
the fix, leaving a multi-day re-run as the only remedy.

A descendant of the measuring commit may finalize its evidence when nothing a
measurement stage executes has changed since. FINALIZER_PATHS lists the paths
no measurement stage runs (init, prepare, check, the engine lanes, and the
structural re-measurement). Every other path counts as measurement code, so a
file nobody thought to classify errs toward refusing. The comparison covers
committed trees only; callers refuse a dirty working tree separately.

    _finalizer.py check <evidence_commit>
    _finalizer.py find --evidence-commit C --kind K --invocation-id I [...]
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.provenance import source_dirty  # noqa: E402

#: Paths that only read finished evidence. A directory entry ends in "/".
#: Keep scripts/run/, src/, config/, docker/, reproduce_all.sh and the lock
#: files out of this list: a lane executes them, so a change there means the
#: evidence was measured by code the checkout no longer contains.
FINALIZER_PATHS = (
    "scripts/figures/",
    "scripts/results/",
    "scripts/verify/verify_result_pipeline.py",
    "tests/",
    "docs/",
    "README.md",
)


def is_finalizer_path(path: str) -> bool:
    return any(
        path.startswith(entry) if entry.endswith("/") else path == entry
        for entry in FINALIZER_PATHS
    )


def _git(root: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", *args], cwd=root, text=True, stderr=subprocess.DEVNULL,
    ).strip()


def head(root: Path = ROOT) -> str:
    """The commit actually checked out, whatever the campaign environment says."""
    try:
        return _git(root, "rev-parse", "--verify", "HEAD^{commit}")
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def measurement_changes(
    evidence_commit: str, head_commit: str | None = None, root: Path = ROOT,
) -> list[str] | None:
    """Measurement paths that differ between the evidence commit and HEAD.

    None means HEAD does not descend from the evidence commit, or either commit
    is unknown here. No path list can make that safe: the evidence would then
    carry history this checkout does not contain.
    """
    head_commit = head_commit or head(root)
    try:
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", evidence_commit, head_commit],
            cwd=root, check=True, capture_output=True,
        )
        changed = _git(
            root, "diff", "--no-renames", "--name-only",
            evidence_commit, head_commit,
        ).splitlines()
    except (OSError, subprocess.CalledProcessError):
        return None
    return sorted(path for path in changed if path and not is_finalizer_path(path))


def may_finalize(evidence_commit: str, root: Path = ROOT) -> bool:
    """HEAD is the evidence commit, or a descendant changing finalization only."""
    return measurement_changes(evidence_commit, root=root) == []


def _check(evidence_commit: str) -> int:
    current = head(ROOT)
    changes = measurement_changes(evidence_commit, current, ROOT)
    if changes is None:
        print(f"ERROR: HEAD {current[:12]} does not descend from the commit that "
              f"measured this campaign ({evidence_commit[:12]}).", file=sys.stderr)
        return 1
    if changes:
        print(f"ERROR: HEAD {current[:12]} changes code that measured this "
              f"campaign at {evidence_commit[:12]}:", file=sys.stderr)
        for path in changes[:20]:
            print(f"  {path}", file=sys.stderr)
        if len(changes) > 20:
            print(f"  ... and {len(changes) - 20} more", file=sys.stderr)
        print("Its evidence can only be finalized from a commit whose measurement "
              "code matches. Move these changes to a later commit, or finalize "
              f"from a worktree at {evidence_commit[:12]} plus finalization-only "
              "commits.", file=sys.stderr)
        return 1
    if source_dirty(ROOT):
        print("ERROR: commit or remove source changes before finalizing.",
              file=sys.stderr)
        subprocess.run(["git", "status", "--short"], cwd=ROOT, check=False)
        return 1
    if current != evidence_commit:
        print(f"finalizing evidence measured at {evidence_commit[:12]} from "
              f"{current[:12]}, which changes only finalization code")
    return 0


def _find(args: argparse.Namespace) -> int:
    """Newest finished collection a promotion can accept.

    scripts/run/find_collection.py takes the newest artifact_valid summary, so a
    collection run from a dirty tree, or at another commit, shadows the clean
    one and promotion then refuses it. Only clean summaries from the evidence
    commit are candidates here.
    """
    required = {engine.strip() for engine in args.required_engines.split(",")
                if engine.strip()}
    matches = []
    for path in (ROOT / "runs").glob("*/summary.json"):
        try:
            summary = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if (summary.get("kind") == args.kind
                and summary.get("invocation_id") == args.invocation_id
                and (args.domain is None or summary.get("domain") == args.domain)
                and (not required
                     or set(summary.get("required_engines") or []) == required)
                and summary.get("artifact_valid") is True
                and summary.get("git_dirty") is False
                and summary.get("git_commit") == args.evidence_commit):
            matches.append(path.parent)
    if not matches:
        print(f"no clean {args.kind} for {args.invocation_id} at "
              f"{args.evidence_commit[:12]}")
        return 1
    print(max(matches, key=lambda path: path.name))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check", help="may this checkout finalize?")
    check.add_argument("evidence_commit")
    find = commands.add_parser("find", help="newest promotable collection")
    find.add_argument("--evidence-commit", required=True)
    find.add_argument("--kind", required=True)
    find.add_argument("--invocation-id", required=True)
    find.add_argument("--domain")
    find.add_argument("--required-engines", default="")
    args = parser.parse_args()
    if args.command == "check":
        return _check(args.evidence_commit)
    return _find(args)


if __name__ == "__main__":
    raise SystemExit(main())
