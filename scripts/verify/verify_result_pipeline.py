#!/usr/bin/env python3
"""Exercise evidence locking and retry checks with small synthetic artifacts."""
from __future__ import annotations

import contextlib
import io
import json
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RESULT_SCRIPTS = ROOT / "scripts" / "results"
sys.path.insert(0, str(RESULT_SCRIPTS))

import promote_results  # noqa: E402
import verify_reproduction  # noqa: E402
from _evidence import (  # noqa: E402
    REQUIRED_COUNTS,
    STAGE_SUFFIX,
    build_lock,
    file_hashes,
    verify_evidence,
)

from _scope import (  # noqa: E402
    D7S_ALPHAS,
    D7S_KLL_KS,
    MUTATION_DIAGNOSTICS,
    MUTANT_TARGETS,
    REQUIRED_DIAGNOSTICS,
    REQUIRED_DOMAINS,
    ORACLE_COMPARISONS,
    REQUIRED_ENGINES,
    REQUIRED_MUTANTS,
    SCALING_DIAGNOSTICS,
    SCALING_SIZES,
)

DOMAINS = (
    "environment_airquality",
    "transportation_taxi",
    "datacenter_drives",
    "finance_markets",
    "sports_football",
)
LIFECYCLE_NA_ENGINES = {"reference", "feldera"}


def _design_payload(block: str) -> dict:
    expected_rows = 50_000 if block == "design_space" else 1_000_000
    repeats = 3 if block == "design_space" else 1
    rows = []
    for engine in sorted(REQUIRED_ENGINES):
        for domain in sorted(REQUIRED_DOMAINS):
            for diagnostic in sorted(REQUIRED_DIAGNOSTICS):
                unsupported = (
                    diagnostic == "D5"
                    and engine in LIFECYCLE_NA_ENGINES
                )
                rows.append({
                    "engine": engine,
                    "domain": domain,
                    "diagnostic": diagnostic,
                    "status": "unsupported" if unsupported else "pass",
                    "pass": not unsupported,
                    "repeat_unstable": False,
                    "repeats": repeats,
                    "repeats_pass": 0 if unsupported else repeats,
                    "runs_unsupported": repeats if unsupported else 0,
                    "runs_blocked": 0,
                    "runs_missing": 0,
                    "missing_repeat_indices": [],
                    "rows": expected_rows,
                    "input_sha256": "d" * 64,
                    "oracle_cache_keys": ["oracle-key"],
                })
    return {
        "kind": "design_space_collect",
        "pinned_rows": expected_rows,
        "pinned_d5_rows": expected_rows,
        "scope": "latest",
        "repeats_kept": repeats,
        "min_repeats": repeats,
        "required_engines": sorted(REQUIRED_ENGINES),
        "engines": sorted(REQUIRED_ENGINES),
        "domains": sorted(REQUIRED_DOMAINS),
        "diagnostics": sorted(REQUIRED_DIAGNOSTICS),
        "cells": len(rows),
        "expected_cells": len(rows),
        "missing_cells": [],
        "cells_below_min_repeats": 0,
        "input_inconsistent_cells": [],
        "oracle_inconsistent_cells": [],
        "cross_engine_input_inconsistent": [],
        "cross_engine_oracle_inconsistent": [],
        "repeat_mismatch_cells": [],
        "rows": rows,
        "pass": True,
    }


def _scaling_payload() -> dict:
    rows = [
        {
            "engine": engine,
            "domain": domain,
            "diagnostic": diagnostic,
            "size": size,
            "status": "equivalent",
            "stable": True,
            "repeats": 3,
            "executed": True,
            "throughput_eligible": True,
            "pass": True,
            "input_sha256": "d" * 64,
            "oracle_cache_keys": ["oracle-key"],
        }
        for engine in sorted(REQUIRED_ENGINES)
        for domain in sorted(REQUIRED_DOMAINS)
        for diagnostic in sorted(SCALING_DIAGNOSTICS)
        for size in sorted(SCALING_SIZES)
    ]
    return {
        "kind": "scaling_collect",
        "base_size": 50_000,
        "min_repeats": 3,
        "required_repeats": 3,
        "sizes": sorted(SCALING_SIZES),
        "required_engines": sorted(REQUIRED_ENGINES),
        "cells": len(rows),
        "expected_cells": len(rows),
        "missing_cells": [],
        "thin_cells": [],
        "input_inconsistent_cells": [],
        "oracle_inconsistent_cells": [],
        "repeat_mismatch_cells": [],
        "cross_engine_input_inconsistent": [],
        "cross_engine_oracle_inconsistent": [],
        "rows": rows,
        "pass": True,
    }


def _boundary_payload() -> dict:
    return {
        "kind": "boundary",
        "max_rows": 5_000,
        "malformed_rows": 5,
        "domains": sorted(REQUIRED_DOMAINS),
        "rows": [
            {
                "domain": domain,
                "input_sha256": f"{index + 1:064x}",
                "rows": 5_000,
                "injected": 5,
                "violation_class_count": 5,
                "rejected": 5,
                "engine_rejected": 5,
                "engine_accepted_malformed": 0,
                "aggregates_unchanged": True,
                "pass": True,
            }
            for index, domain in enumerate(sorted(REQUIRED_DOMAINS))
        ],
        "pass": True,
    }


def _mutation_payload() -> dict:
    rows = []
    for domain in sorted(REQUIRED_DOMAINS):
        for mutant in sorted(REQUIRED_MUTANTS):
            target = MUTANT_TARGETS[mutant]
            for diagnostic in sorted(MUTATION_DIAGNOSTICS):
                is_target = diagnostic == target
                rows.append({
                    "domain": domain,
                    "mutant": mutant,
                    "diagnostic": diagnostic,
                    "target_diagnostic": target,
                    "is_target": is_target,
                    "detected": is_target,
                    "vacuous": False,
                    "error": None,
                })
    return {
        "kind": "detection_power",
        "max_rows": 50_000,
        "epsilon": 1e-9,
        "domains": sorted(REQUIRED_DOMAINS),
        "mutants": sorted(REQUIRED_MUTANTS),
        "diagnostics": sorted(MUTATION_DIAGNOSTICS),
        "cells": len(rows),
        "diagonal_caught": 35,
        "diagonal_total": 35,
        "sensitivity": 1.0,
        "specificity": 1.0,
        "missed": [],
        "vacuous": [],
        "errors": [],
        "rows": rows,
        "pass": True,
    }


def _d7s_payload() -> dict:
    rows = [
        {
            "domain": domain,
            "alpha": alpha,
            "kll_k": kll_k,
            "rows": 200_001,
            "restatements": 1,
            "groups_scored": 10,
            "separates": False,
        }
        for domain in sorted(REQUIRED_DOMAINS)
        for alpha in sorted(D7S_ALPHAS)
        for kll_k in sorted(D7S_KLL_KS)
    ]
    return {
        "kind": "d7s_sweep",
        "max_rows": 200_000,
        "domains": sorted(REQUIRED_DOMAINS),
        "alphas": sorted(D7S_ALPHAS),
        "kll_ks": sorted(D7S_KLL_KS),
        "cells": len(rows),
        "separating_cells": 0,
        "separating_by_domain": {
            domain: 0 for domain in sorted(REQUIRED_DOMAINS)
        },
        "rows": rows,
        "pass": False,
    }


def _native_payload() -> dict:
    checkpoints = [
        {
            "checkpoint": checkpoint,
            "rows": checkpoint * 5_000,
            "hourly": {"equivalent": True},
            "daily": {"equivalent": True},
            "pass": True,
        }
        for checkpoint in range(1, 5)
    ]
    topk = [
        {
            "kind": "native_topk",
            "engine": engine,
            "domain": domain,
            "status": "measured",
            "supported": True,
            "artifact_valid": True,
            "k": 10,
            "rank_metric": "mean",
            "rows": 20_000,
            "trajectory_checkpoints": 4,
            "checkpoints": checkpoints,
            "pass": True,
        }
        for engine in sorted(REQUIRED_ENGINES)
        for domain in sorted(REQUIRED_DOMAINS)
    ]
    watermark = [
        {
            "kind": "native_watermark",
            "engine": engine,
            "status": "unsupported",
            "supported": False,
            "artifact_valid": True,
            "allowed_lateness_s": 300,
            "pass": None,
        }
        for engine in sorted(REQUIRED_ENGINES)
    ]
    return {
        "kind": "native_semantics",
        "probe": "all",
        "engines": sorted(REQUIRED_ENGINES),
        "domains": sorted(REQUIRED_DOMAINS),
        "max_rows": 20_000,
        "trajectory_checkpoints": 4,
        "k": 10,
        "rank_metric": "mean",
        "allowed_lateness_s": 300,
        "topk": topk,
        "watermark": watermark,
        "measured_cells": len(topk),
        "classified_cells": len(topk) + len(watermark),
        "pass": True,
    }


def _oracle_payload(index: int) -> dict:
    return {
        "kind": "oracle_verification",
        "domain": DOMAINS[index],
        "rows": 1_000_000,
        "hourly_keys": 10,
        "daily_keys": 5,
        "comparisons": {
            name: {"equivalent": True, "mismatched_keys": 0}
            for name in ORACLE_COMPARISONS
        },
        "all_equivalent": True,
        "pass": True,
    }


def _query_family_payload() -> dict:
    rows = [
        {
            "engine": "materialize",
            "domain": "transportation_taxi",
            "grain": "hourly",
            "shape": "S1_latest",
            "delivery": delivery,
            "attribution": (
                "repaired" if delivery == "shuffled" else "attributable"),
            "contract_equivalent": True,
            "arrival_equivalent": delivery == "ordered",
        }
        for delivery in ("shuffled", "ordered")
    ]
    return {
        "kind": "query_family",
        "rows": rows,
        "expressible_cells": 1,
        "repaired_cells": 1,
        "control_all_attributable": True,
        "oracle_disagreements": 0,
        "blocked": 0,
        "pass": True,
    }


def _disorder_sweep_payload() -> dict:
    rows = [
        {"kind": "exposure", "engine": "oracle", "domain": "transportation_taxi",
         "grain": "hourly", "window": window, "keys": 10,
         "wrong_keys": wrong, "wrong_fraction": wrong / 10}
        for window, wrong in (("0", 0), ("100", 3))
    ] + [
        {"kind": "engine", "engine": "materialize",
         "domain": "transportation_taxi", "grain": "hourly", "window": "100",
         "status": "measured", "keys": 10, "contract_wrong_keys": 0,
         "arrival_wrong_keys": 3, "arrival_wrong_fraction": 0.3},
    ]
    return {"kind": "disorder_sweep", "rows": rows, "oracle_disagreements": 0,
            "blocked": 0, "pass": True}


def _parallelism_sweep_payload() -> dict:
    rows = [
        {"engine": "flink", "parallelism": level, "domain": "finance_markets",
         "keys": 100, "drift_keys": drift, "ddr": drift / 100,
         "oracle_equivalent": True, "status": "measured"}
        for level, drift in ((1, 0), (2, 5))
    ]
    return {"kind": "parallelism_sweep", "rows": rows, "blocked": 0,
            "pass": True}


def _formulation_cost_payload() -> dict:
    return {"kind": "formulation_cost", "serialized": True, "blocked": 0,
            "ratios": [{"engine": "materialize", "domain": "finance_markets",
                        "repair_median_s": 11.0, "arrival_median_s": 10.0,
                        "repair_over_arrival": 1.1}],
            "rows": [], "pass": True}


def _official_payload(block: str, index: int) -> dict:
    if block in {"design_space", "design_space_confirmation"}:
        return _design_payload(block)
    if block == "scaling":
        return _scaling_payload()
    if block == "boundary":
        return _boundary_payload()
    if block == "mutation":
        return _mutation_payload()
    if block == "d7s_sweep":
        return _d7s_payload()
    if block == "native_semantics":
        return _native_payload()
    if block == "query_family":
        return _query_family_payload()
    if block == "disorder_sweep":
        return _disorder_sweep_payload()
    if block == "parallelism_sweep":
        return _parallelism_sweep_payload()
    if block == "formulation_cost":
        return _formulation_cost_payload()
    if block == "oracle_verification":
        return _oracle_payload(index)
    raise AssertionError(f"unsupported synthetic block: {block}")



def _summary(block: str, index: int) -> dict:
    suffix, phase = STAGE_SUFFIX[block]
    summary = {
        "kind": block,
        "contract_version": 2,
        "artifact_valid": True,
        "git_dirty": False,
        "git_commit": "a" * 40,
        "invocation_id": f"benchmark_test_{suffix}",
        "campaign_id": "benchmark_test",
        "campaign_phase": phase,
        "benchmark_config_sha256": "b" * 64,
        "dependency_lock_sha256": "c" * 64,
        "python_version": "3.12.12",
        "platform": "Linux-test",
        "hostname": "benchmark-host",
        "machine": "x86_64",
        "pass": True,
    }
    summary.update(_official_payload(block, index))
    return summary


def _write_promotion(root: Path, block: str, index: int) -> Path:
    run_id = f"synthetic_{block}_{index}"
    destination = root / "results" / block / run_id
    destination.mkdir(parents=True)
    summary = _summary(block, index)
    (destination / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n")
    (destination / "payload.txt").write_text(f"{block}/{index}\n")
    manifest = {
        "promoted_utc": "2026-08-16T00:00:00Z",
        "source_run": f"runs/{run_id}",
        "git_commit": summary["git_commit"],
        "invocation_id": summary["invocation_id"],
        "benchmark_config_sha256": summary["benchmark_config_sha256"],
        "dependency_lock_sha256": summary["dependency_lock_sha256"],
        "block": block,
        "files_sha256": file_hashes(destination),
    }
    manifest_path = destination / "promotion_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest_path


def _rewrite_promotion_summary(path: Path, summary: dict) -> None:
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    manifest_path = path.parent / "promotion_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["invocation_id"] = summary["invocation_id"]
    manifest["files_sha256"] = file_hashes(path.parent)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n")

def _assert_scope_rejected(
    root: Path,
    summary_path: Path,
    mutate,
    expected_failure: str,
) -> None:
    original = json.loads(summary_path.read_text())
    changed = json.loads(json.dumps(original))
    mutate(changed)
    _rewrite_promotion_summary(summary_path, changed)
    failures, _, _ = verify_evidence(root)
    assert any(expected_failure in failure for failure in failures), failures
    _rewrite_promotion_summary(summary_path, original)
    failures, _, _ = verify_evidence(root)
    assert not failures, failures



def main() -> int:
    with tempfile.TemporaryDirectory(prefix="equitable-result-test-") as tmp:
        root = Path(tmp)
        paths: dict[str, list[Path]] = {}
        for block, count in REQUIRED_COUNTS.items():
            paths[block] = [_write_promotion(root, block, i) for i in range(count)]

        failures, records, lock = verify_evidence(root)
        assert not failures, failures
        assert lock is None
        assert len(records) == sum(REQUIRED_COUNTS.values()) == 16
        assert len({record["invocation_id"] for record in records}) == 5

        design_summary_path = (
            paths["design_space"][0].parent / "summary.json")
        design_summary = json.loads(design_summary_path.read_text())
        unsupported_d5 = [
            row for row in design_summary["rows"]
            if row["diagnostic"] == "D5"
            and row["engine"] in LIFECYCLE_NA_ENGINES
        ]
        assert len(unsupported_d5) == 10
        assert all(
            row["status"] == "unsupported"
            and row["oracle_cache_keys"] == ["oracle-key"]
            for row in unsupported_d5
        )

        scaling_summary_path = paths["scaling"][0].parent / "summary.json"
        scaling_summary = json.loads(scaling_summary_path.read_text())

        # Old campaigns are immutable, so readers continue to accept their
        # historical scaling identity while writers use the canonical one.
        scaling_summary["invocation_id"] = "benchmark_test_ladder"
        scaling_summary["campaign_phase"] = "ladder"
        _rewrite_promotion_summary(scaling_summary_path, scaling_summary)
        failures, _, _ = verify_evidence(root)
        assert not failures, failures
        suffix, phase = STAGE_SUFFIX["scaling"]
        scaling_summary["invocation_id"] = f"benchmark_test_{suffix}"
        scaling_summary["campaign_phase"] = phase
        _rewrite_promotion_summary(scaling_summary_path, scaling_summary)

        def partial_grid(summary: dict) -> None:
            summary["rows"].pop()
            summary["cells"] = len(summary["rows"])
            summary["expected_cells"] = len(summary["rows"])

        _assert_scope_rejected(
            root,
            paths["design_space"][0].parent / "summary.json",
            partial_grid,
            "outside official design_space scope",
        )
        _assert_scope_rejected(
            root,
            design_summary_path,
            lambda summary: summary["rows"][0].pop("input_sha256"),
            "every cell must preserve a nonempty input hash and oracle cache key",
        )

        def drop_executed_oracle_provenance(summary: dict) -> None:
            target = next(
                row for row in summary["rows"]
                if row["status"] != "unsupported"
            )
            target["oracle_cache_keys"] = []

        _assert_scope_rejected(
            root, design_summary_path, drop_executed_oracle_provenance,
            "every cell must preserve a nonempty input hash and oracle cache key",
        )

        _assert_scope_rejected(
            root,
            scaling_summary_path,
            partial_grid,
            "outside official scaling scope",
        )

        def narrow_boundary(summary: dict) -> None:
            domain = summary["domains"][0]
            summary["domains"] = [domain]
            summary["rows"] = [
                row for row in summary["rows"] if row["domain"] == domain
            ]

        boundary_path = paths["boundary"][0].parent / "summary.json"
        _assert_scope_rejected(
            root, boundary_path, narrow_boundary,
            "outside official boundary scope",
        )

        def wrong_boundary_size(summary: dict) -> None:
            summary["max_rows"] = 50_000
            for row in summary["rows"]:
                row["rows"] = 50_000

        _assert_scope_rejected(
            root, boundary_path, wrong_boundary_size,
            "outside official boundary scope",
        )
        _assert_scope_rejected(
            root, boundary_path,
            lambda summary: summary["rows"][0].pop("input_sha256"),
            "every boundary row must preserve one 64-hex input_sha256",
        )
        _assert_scope_rejected(
            root, boundary_path,
            lambda summary: summary["rows"][0].__setitem__(
                "input_sha256", "g" * 64),
            "every boundary row must preserve one 64-hex input_sha256",
        )

        def reuse_boundary_hash(summary: dict) -> None:
            summary["rows"][1]["input_sha256"] = (
                summary["rows"][0]["input_sha256"])

        _assert_scope_rejected(
            root, boundary_path, reuse_boundary_hash,
            "boundary input_sha256 values must be unique across domains",
        )

        def narrow_mutation(summary: dict) -> None:
            domain = summary["domains"][0]
            summary["domains"] = [domain]
            summary["rows"] = [
                row for row in summary["rows"] if row["domain"] == domain
            ]
            summary["cells"] = len(summary["rows"])
            summary["diagonal_caught"] = 7
            summary["diagonal_total"] = 7

        _assert_scope_rejected(
            root,
            paths["mutation"][0].parent / "summary.json",
            narrow_mutation,
            "outside official mutation scope",
        )

        def narrow_d7s(summary: dict) -> None:
            domain = summary["domains"][0]
            summary["domains"] = [domain]
            summary["rows"] = [
                row for row in summary["rows"] if row["domain"] == domain
            ]
            summary["cells"] = len(summary["rows"])
            summary["separating_by_domain"] = {domain: 0}

        d7s_path = paths["d7s_sweep"][0].parent / "summary.json"
        _assert_scope_rejected(
            root, d7s_path, narrow_d7s,
            "outside official d7s_sweep scope",
        )
        _assert_scope_rejected(
            root, d7s_path,
            lambda summary: summary.__setitem__("pass", True),
            "outside official d7s_sweep scope",
        )
        _assert_scope_rejected(
            root, d7s_path,
            lambda summary: summary["rows"][0].__setitem__("rows", 200_000),
            "outside official d7s_sweep scope",
        )


        def narrow_native(summary: dict) -> None:
            engine = summary["engines"][0]
            summary["engines"] = [engine]
            summary["topk"] = [
                row for row in summary["topk"] if row["engine"] == engine
            ]
            summary["watermark"] = [
                row for row in summary["watermark"] if row["engine"] == engine
            ]
            summary["measured_cells"] = len(summary["topk"])
            summary["classified_cells"] = (
                len(summary["topk"]) + len(summary["watermark"])
            )

        _assert_scope_rejected(
            root,
            paths["native_semantics"][0].parent / "summary.json",
            narrow_native,
            "outside official native_semantics scope",
        )
        _assert_scope_rejected(
            root,
            paths["oracle_verification"][1].parent / "summary.json",
            lambda summary: summary.__setitem__("domain", DOMAINS[0]),
            "incomplete collection scope",
        )

        narrow_run = root / "runs" / "narrow_boundary"
        narrow_run.mkdir(parents=True)
        narrow_summary = _summary("boundary", 0)
        narrow_boundary(narrow_summary)
        (narrow_run / "summary.json").write_text(
            json.dumps(narrow_summary, indent=2, sort_keys=True) + "\n")
        original_promote_root = promote_results.ROOT
        original_promote_results = promote_results.RESULTS
        original_results_md = promote_results.RESULTS_MD
        original_argv = sys.argv
        try:
            promote_results.ROOT = root
            promote_results.RESULTS = root / "results"
            promote_results.RESULTS_MD = root / "docs" / "RESULTS.md"
            sys.argv = [
                "promote_results.py", str(narrow_run), "--block", "boundary",
            ]
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                assert promote_results.main() == 1
            assert "all five domains" in output.getvalue()
            assert not (
                root / "results" / "boundary" / narrow_run.name
            ).exists()
        finally:
            promote_results.ROOT = original_promote_root
            promote_results.RESULTS = original_promote_results
            promote_results.RESULTS_MD = original_results_md
            sys.argv = original_argv
        confirmation = paths["design_space_confirmation"][0].parent
        hidden = root / "hidden_confirmation"
        confirmation.rename(hidden)
        failures, _, _ = verify_evidence(root)
        assert any("design_space_confirmation" in failure for failure in failures)
        hidden.rename(confirmation)

        failures, records, _ = verify_evidence(root)
        assert not failures, failures
        official = build_lock(records)
        lock_path = root / "results" / "official_results.json"
        lock_path.write_text(json.dumps(official, indent=2, sort_keys=True) + "\n")
        failures, locked_records, loaded_lock = verify_evidence(root, require_lock=True)
        assert not failures, failures
        assert loaded_lock is not None
        assert len(locked_records) == 16

        kind_by_block = {
            "design_space": "design_space_collect",
            "design_space_confirmation": "design_space_collect",
            "scaling": "scaling_collect",
            "boundary": "boundary",
            "mutation": "detection_power",
            "d7s_sweep": "d7s_sweep",
            "native_semantics": "native_semantics",
            "query_family": "query_family",
            "disorder_sweep": "disorder_sweep",
            "parallelism_sweep": "parallelism_sweep",
            "formulation_cost": "formulation_cost",
            "oracle_verification": "oracle_verification",
        }
        for number, record in enumerate(locked_records):
            block = record["_block"]
            suffix, phase = STAGE_SUFFIX[block]
            fresh = {
                key: value for key, value in record.items()
                if not key.startswith("_")
            }
            fresh.update({
                "kind": kind_by_block[block],
                "campaign_id": "benchmark_reviewer",
                "campaign_phase": phase,
                "invocation_id": f"benchmark_reviewer_{suffix}",
                "platform": "Linux-reviewer",
                "hostname": "reviewer-host",
                "machine": "x86_64",
            })
            fresh_dir = root / "runs" / f"fresh_{number:02d}"
            fresh_dir.mkdir(parents=True)
            (fresh_dir / "summary.json").write_text(
                json.dumps(fresh, indent=2, sort_keys=True) + "\n")
        (root / "runs" / "benchmark_runs" / "benchmark_reviewer").mkdir(
            parents=True)
        original_reproduction_root = verify_reproduction.ROOT
        original_reproduction_runs = verify_reproduction.RUNS
        original_argv = sys.argv
        try:
            verify_reproduction.ROOT = root
            verify_reproduction.RUNS = root / "runs"
            sys.argv = ["verify_reproduction.py", "--run-id", "benchmark_reviewer"]
            assert verify_reproduction.main() == 0
        finally:
            verify_reproduction.ROOT = original_reproduction_root
            verify_reproduction.RUNS = original_reproduction_runs
            sys.argv = original_argv
        report = json.loads((
            root / "runs" / "benchmark_runs" / "benchmark_reviewer"
            / "reproduction_report.json"
        ).read_text())
        assert report["pass"] is True
        assert report["official_benchmark_run_id"] == "benchmark_test"

        extra = root / "results" / "boundary" / "unlocked_copy"
        shutil.copytree(paths["boundary"][0].parent, extra)
        failures, _, _ = verify_evidence(root, require_lock=True)
        assert any("unlocked promoted evidence" in failure for failure in failures)
        shutil.rmtree(extra)

        run_dir = root / "runs" / "retry_source"
        destination = root / "results" / "retry" / "retry_source"
        run_dir.mkdir(parents=True)
        destination.mkdir(parents=True)
        retry_summary = _summary("design_space", 0)
        for directory in (run_dir, destination):
            (directory / "summary.json").write_text(
                json.dumps(retry_summary, sort_keys=True) + "\n")
        retry_manifest = {
            "source_run": "runs/retry_source",
            "block": "retry",
            "git_commit": retry_summary["git_commit"],
            "invocation_id": retry_summary["invocation_id"],
            "benchmark_config_sha256": retry_summary["benchmark_config_sha256"],
            "dependency_lock_sha256": retry_summary["dependency_lock_sha256"],
            "files_sha256": promote_results._file_hashes(run_dir),
        }
        (destination / "promotion_manifest.json").write_text(
            json.dumps(retry_manifest) + "\n")
        original_root = promote_results.ROOT
        try:
            promote_results.ROOT = root
            assert promote_results._already_promoted(
                destination, run_dir, "retry", retry_summary)
        finally:
            promote_results.ROOT = original_root

    print("PASS: result promotion, locking, confirmation coverage, and retry checks")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
