"""Shared validation for promoted and locked paper evidence."""
from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any

try:
    from ._scope import collection_scope_failure, official_scope_failure
except ImportError:  # Direct execution from scripts/results.
    from _scope import collection_scope_failure, official_scope_failure

REQUIRED_COUNTS = {
    "oracle_verification": 5,
    "design_space": 1,
    "design_space_confirmation": 1,
    "scaling": 1,
    "boundary": 1,
    "d7s_sweep": 1,
    "mutation": 1,
    "native_semantics": 1,
    "query_family": 1,
    "disorder_sweep": 1,
    "parallelism_sweep": 1,
    "formulation_cost": 1,
}

STAGE_SUFFIX = {
    "oracle_verification": ("prepare", "prepare"),
    "design_space": ("grid50k", "grid50k"),
    "design_space_confirmation": ("confirm1m", "confirm1m"),
    "scaling": ("scaling", "scaling"),
    "boundary": ("validation", "validation"),
    "d7s_sweep": ("validation", "validation"),
    "mutation": ("validation", "validation"),
    "native_semantics": ("validation", "validation"),
    "query_family": ("validation", "validation"),
    "disorder_sweep": ("validation", "validation"),
    "parallelism_sweep": ("validation", "validation"),
    "formulation_cost": ("validation", "validation"),
}

# Older campaigns wrote the scaling stage with the `ladder` suffix and phase.
# Readers accept that immutable evidence, while every writer above emits only
# the canonical scaling identity.
LEGACY_STAGE_SUFFIX = {
    "scaling": (("ladder", "ladder"),),
}


def stage_identities(
    block: str, benchmark_id: str,
) -> tuple[tuple[str, str], ...]:
    stages = (STAGE_SUFFIX[block], *LEGACY_STAGE_SUFFIX.get(block, ()))
    return tuple(
        (f"{benchmark_id}_{suffix}", phase) for suffix, phase in stages
    )


COMMON_FIELDS = (
    "campaign_id",
    "git_commit",
    "benchmark_config_sha256",
    "dependency_lock_sha256",
    "python_version",
    "platform",
    "hostname",
    "machine",
)

LOCK_NAME = "official_results.json"


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def file_hashes(directory: Path) -> dict[str, str]:
    return {
        path.relative_to(directory).as_posix(): sha256_file(path)
        for path in sorted(directory.rglob("*"))
        if path.is_file() and path.name != "promotion_manifest.json"
    }


def load_lock(root: Path) -> dict[str, Any] | None:
    path = root / "results" / LOCK_NAME
    if not path.is_file():
        return None
    try:
        lock = json.loads(path.read_text())
    except (OSError, ValueError):
        return {"_invalid": True}
    if not isinstance(lock, dict):
        return {"_invalid": True}
    lock["_path"] = str(path)
    return lock


def _all_manifest_paths(root: Path) -> list[Path]:
    return sorted((root / "results").glob("*/*/promotion_manifest.json"))


def selected_manifest_paths(
    root: Path, lock: dict[str, Any] | None,
) -> tuple[list[Path], list[str]]:
    failures: list[str] = []
    all_paths = _all_manifest_paths(root)
    if lock is None:
        return all_paths, failures
    if lock.get("_invalid") or lock.get("schema_version") != 1:
        return [], ["official result lock is invalid or uses an unknown schema"]
    promotions = lock.get("promotions")
    if not isinstance(promotions, list):
        return [], ["official result lock has no promotions list"]
    selected: list[Path] = []
    seen: set[Path] = set()
    for item in promotions:
        if not isinstance(item, dict):
            failures.append("official result lock contains a malformed promotion")
            continue
        block = item.get("block")
        run_id = item.get("run_id")
        if (not isinstance(block, str) or block not in REQUIRED_COUNTS
                or not isinstance(run_id, str)
                or not run_id or Path(run_id).name != run_id):
            failures.append("official result lock contains an invalid block or run ID")
            continue
        path = root / "results" / block / run_id / "promotion_manifest.json"
        if path in seen:
            failures.append(f"official result lock repeats {block}/{run_id}")
            continue
        seen.add(path)
        if not path.is_file():
            failures.append(f"locked promotion is missing: {path.relative_to(root)}")
            continue
        if sha256_file(path) != item.get("manifest_sha256"):
            failures.append(f"locked promotion manifest changed: {path.relative_to(root)}")
            continue
        selected.append(path)
    if set(selected) != set(all_paths):
        extras = sorted(set(all_paths) - set(selected))
        if extras:
            failures.append(
                "unlocked promoted evidence exists: "
                + ", ".join(str(path.relative_to(root)) for path in extras)
            )
    return selected, failures


def verify_evidence(
    root: Path, *, require_lock: bool = False,
) -> tuple[list[str], list[dict[str, Any]], dict[str, Any] | None]:
    failures: list[str] = []
    lock = load_lock(root)
    if require_lock and lock is None:
        failures.append(f"results/{LOCK_NAME} is required")
    manifest_paths, selection_failures = selected_manifest_paths(root, lock)
    failures.extend(selection_failures)
    if not manifest_paths:
        failures.append("no promoted evidence manifests were found")

    records: list[dict[str, Any]] = []
    for manifest_path in manifest_paths:
        destination = manifest_path.parent
        try:
            manifest = json.loads(manifest_path.read_text())
        except (OSError, ValueError):
            failures.append(f"invalid manifest: {manifest_path.relative_to(root)}")
            continue
        block = manifest.get("block")
        if not isinstance(block, str) or block not in REQUIRED_COUNTS:
            failures.append(f"unexpected paper evidence block: {block}")
            continue
        if block != manifest_path.parent.parent.name:
            failures.append(
                f"manifest block does not match its result directory: "
                f"{manifest_path.relative_to(root)}"
            )
        expected_hashes = manifest.get("files_sha256")
        if not isinstance(expected_hashes, dict):
            failures.append(f"manifest has no file hashes: {manifest_path.relative_to(root)}")
            continue
        actual_hashes = file_hashes(destination)
        if actual_hashes != expected_hashes:
            failures.append(f"promoted files changed: {destination.relative_to(root)}")

        source_run = manifest.get("source_run")
        if (not isinstance(source_run, str) or not source_run
                or Path(source_run).is_absolute()
                or Path(source_run).parts[:1] != ("runs",)
                or ".." in Path(source_run).parts):
            failures.append(
                f"manifest has an invalid source run: {manifest_path.relative_to(root)}"
            )
        else:
            source = root / source_run
            if source.is_dir() and file_hashes(source) != expected_hashes:
                failures.append(
                    f"promoted files differ from available source run: {source_run}"
                )

        summary_path = destination / "summary.json"
        if not summary_path.is_file():
            failures.append(f"promoted summary is missing: {summary_path.relative_to(root)}")
            continue
        try:
            summary = json.loads(summary_path.read_text())
        except (OSError, ValueError):
            failures.append(f"promoted summary is invalid: {summary_path.relative_to(root)}")
            continue
        for field in (
            "git_commit",
            "invocation_id",
            "benchmark_config_sha256",
            "dependency_lock_sha256",
        ):
            if manifest.get(field) != summary.get(field):
                failures.append(
                    f"{destination.relative_to(root)} mismatches manifest field {field}"
                )
        if not (
            summary.get("contract_version") == 2
            and summary.get("artifact_valid") is True
            and summary.get("git_dirty") is False
        ):
            failures.append(
                f"{destination.relative_to(root)} is not clean valid contract-v2 evidence"
            )
        scope_failure = official_scope_failure(block, summary)
        if scope_failure:
            failures.append(
                f"{destination.relative_to(root)} is outside official {block} scope: "
                f"{scope_failure}"
            )
        summary["_block"] = block
        summary["_run_id"] = destination.name
        summary["_manifest_path"] = str(manifest_path)
        records.append(summary)

    counts = Counter(record["_block"] for record in records)
    for block, expected in REQUIRED_COUNTS.items():
        if counts[block] != expected:
            failures.append(
                f"paper block {block} has {counts[block]} promoted summaries; "
                f"expected {expected}"
            )

    for block in REQUIRED_COUNTS:
        block_records = [record for record in records if record["_block"] == block]
        scope_failure = collection_scope_failure(block, block_records)
        if scope_failure:
            failures.append(
                f"paper block {block} has incomplete collection scope: "
                f"{scope_failure}"
            )

    benchmark_ids = {record.get("campaign_id") for record in records}
    benchmark_id = next(iter(benchmark_ids)) if len(benchmark_ids) == 1 else None
    if not isinstance(benchmark_id, str) or not benchmark_id:
        failures.append("paper evidence has no single non-empty benchmark run ID")
    for field in COMMON_FIELDS:
        values = {record.get(field) for record in records}
        if len(values) != 1 or None in values:
            failures.append(f"paper evidence mixes {field}: {sorted(map(str, values))}")

    if isinstance(benchmark_id, str) and benchmark_id:
        for record in records:
            block = record["_block"]
            accepted = stage_identities(block, benchmark_id)
            observed = (
                record.get("invocation_id"), record.get("campaign_phase"),
            )
            if observed not in accepted:
                expected = " or ".join(
                    f"{invocation}/{phase}" for invocation, phase in accepted
                )
                failures.append(
                    f"{block} uses invocation/phase {observed[0]}/{observed[1]}; "
                    f"expected {expected}"
                )

    if lock and not lock.get("_invalid"):
        if lock.get("benchmark_run_id") != benchmark_id:
            failures.append("official result lock names a different benchmark run")
        if records:
            first = records[0]
            for lock_field, summary_field in (
                ("source_commit", "git_commit"),
                ("benchmark_config_sha256", "benchmark_config_sha256"),
                ("dependency_lock_sha256", "dependency_lock_sha256"),
                ("python_version", "python_version"),
                ("platform", "platform"),
                ("hostname", "hostname"),
                ("machine", "machine"),
            ):
                if lock.get(lock_field) != first.get(summary_field):
                    failures.append(f"official result lock mismatches {lock_field}")
    return failures, records, lock


def build_lock(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not records:
        raise ValueError("cannot lock an empty evidence set")
    first = records[0]
    promotions = []
    for record in sorted(records, key=lambda row: (row["_block"], row["_run_id"])):
        manifest_path = Path(record["_manifest_path"])
        promotions.append({
            "block": record["_block"],
            "run_id": record["_run_id"],
            "manifest_sha256": sha256_file(manifest_path),
        })
    return {
        "schema_version": 1,
        "benchmark_run_id": first["campaign_id"],
        "source_commit": first["git_commit"],
        "benchmark_config_sha256": first["benchmark_config_sha256"],
        "dependency_lock_sha256": first["dependency_lock_sha256"],
        "python_version": first["python_version"],
        "platform": first["platform"],
        "hostname": first["hostname"],
        "machine": first["machine"],
        "promotions": promotions,
    }
