"""Canonical scope contracts for official paper evidence.

Experiment runners support narrowed exploratory scopes.  These
validators are the boundary that prevents a successful one-domain or one-cell
exploration from licensing a full-universe paper claim.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

REQUIRED_ENGINES = {
    "reference", "materialize", "risingwave", "ksqldb",
    "flink", "spark", "feldera",
}
REQUIRED_DOMAINS = {
    "environment_airquality", "transportation_taxi", "datacenter_drives",
    "finance_markets", "sports_football",
}
REQUIRED_DIAGNOSTICS = {"D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8"}
SCALING_DIAGNOSTICS = REQUIRED_DIAGNOSTICS - {"D5"}
MUTATION_DIAGNOSTICS = REQUIRED_DIAGNOSTICS - {"D8"}
SCALING_SIZES = {10_000, 50_000, 200_000, 500_000, 1_000_000}
REQUIRED_MUTANTS = {
    "mutant_no_dedup", "mutant_order_sensitive", "mutant_ulp_drift",
    "mutant_drop_late", "mutant_ignore_supersedes",
    "mutant_recovery_loss", "mutant_topk_rank",
}
MUTANT_TARGETS = {
    "mutant_no_dedup": "D1",
    "mutant_order_sensitive": "D2",
    "mutant_ulp_drift": "D3",
    "mutant_drop_late": "D4",
    "mutant_ignore_supersedes": "D7",
    "mutant_recovery_loss": "D5",
    "mutant_topk_rank": "D6",
}
D7S_ALPHAS = {0.001, 0.01, 0.05, 0.10, 0.25, 0.50}
D7S_KLL_KS = {64, 200, 800}

ORACLE_COMPARISONS = {
    "engine_vs_pandas_hourly",
    "engine_vs_pandas_daily",
    "engine_vs_duckdb_hourly",
    "engine_vs_duckdb_daily",
    "pandas_vs_duckdb_hourly",
    "pandas_vs_duckdb_daily",
}


def declared_engines(
    summary: dict[str, Any], block: str, *, allow_exclusion: bool = True,
) -> tuple[set[str] | None, str | None]:
    """Engines this cohort must contain, after any *declared* exclusion.

    An engine may be left out of a cohort only when the collection names it in
    ``excluded_engines``.  A cohort that simply omits an engine, or that names
    an exclusion the block does not permit, is still rejected: the point of the
    guard is that a narrowed universe is a recorded decision, not an accident.
    """
    excluded = summary.get("excluded_engines")
    if excluded in (None, []):
        return set(REQUIRED_ENGINES), None
    if not allow_exclusion:
        return None, (
            f"{block} evidence cannot exclude an engine; "
            f"it declares excluded_engines {excluded!r}"
        )
    if (
        not isinstance(excluded, list)
        or not all(isinstance(engine, str) for engine in excluded)
        or len(set(excluded)) != len(excluded)
        or not set(excluded) <= REQUIRED_ENGINES
    ):
        return None, (
            f"{block} declares invalid excluded_engines {excluded!r}; "
            f"each entry must be one of {sorted(REQUIRED_ENGINES)}"
        )
    engines = set(REQUIRED_ENGINES) - set(excluded)
    if not engines:
        return None, f"{block} cannot exclude every engine"
    return engines, None


def _exact_list(value: Any, expected: set[Any]) -> bool:
    """Whether value names each expected member exactly once."""
    if not isinstance(value, list) or len(value) != len(expected):
        return False
    try:
        typed = [(type(item), item) for item in value]
        typed_expected = {(type(item), item) for item in expected}
        return (
            len(set(typed)) == len(typed)
            and set(typed) == typed_expected
        )
    except TypeError:
        return False


def _rows(summary: dict[str, Any]) -> list[dict[str, Any]] | None:
    value = summary.get("rows")
    if not isinstance(value, list) or not all(
        isinstance(row, dict) for row in value
    ):
        return None
    return value


def _exact_grid(
    rows: list[dict[str, Any]],
    fields: tuple[str, ...],
    expected: set[tuple[Any, ...]],
) -> bool:
    keys = [tuple(row.get(field) for field in fields) for row in rows]
    try:
        typed_keys = [tuple((type(value), value) for value in key) for key in keys]
        typed_expected = {
            tuple((type(value), value) for value in key) for key in expected
        }
        return (
            len(typed_keys) == len(typed_expected)
            and len(set(typed_keys)) == len(typed_keys)
            and set(typed_keys) == typed_expected
        )
    except TypeError:
        return False


def _is_sha256(value: Any) -> bool:
    """Whether *value* is one SHA-256 digest in hexadecimal form."""
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(char in "0123456789abcdefABCDEF" for char in value)
    )


def names_an_oracle(row: dict[str, Any]) -> bool:
    """Whether this cell was scored against an oracle, so one must be named.

    An unsupported cell is the exception. The adapter has no formulation of the
    view, so the harness stops before an expected table is ever built: there is
    no oracle to name, and requiring a cache key of it would make a truthfully
    recorded capability state unpromotable. Every cell that did produce a
    comparison, a divergent one included, still has to carry its key.
    """
    return str(row.get("status")) != "unsupported"


def _provenance_failure(
    rows: list[dict[str, Any]],
    group_fields: tuple[str, ...],
) -> str | None:
    grouped: dict[tuple[Any, ...], set[tuple[str, tuple[str, ...]]]] = {}
    for row in rows:
        input_sha256 = row.get("input_sha256")
        oracle_cache_keys = row.get("oracle_cache_keys")
        # The input hash binds every cell without exception: the harness
        # selects and hashes the rows before it asks the adapter whether it can
        # express the view at all.
        if not _is_sha256(input_sha256):
            return "every cell must preserve a nonempty input hash and oracle cache key"
        if not names_an_oracle(row):
            continue
        if (
            not isinstance(oracle_cache_keys, list)
            or not oracle_cache_keys
            or not all(
                isinstance(key, str) and bool(key)
                for key in oracle_cache_keys
            )
        ):
            return "every cell must preserve a nonempty input hash and oracle cache key"
        group = tuple(row.get(field) for field in group_fields)
        grouped.setdefault(group, set()).add(
            (input_sha256, tuple(oracle_cache_keys)))
    if any(len(values) != 1 for values in grouped.values()):
        return "cell provenance is inconsistent across engines"
    return None


def _design_scope_failure(
    block: str, summary: dict[str, Any],
) -> str | None:
    engines, exclusion_failure = declared_engines(
        summary, block, allow_exclusion=block == "design_space_confirmation")
    if exclusion_failure:
        return exclusion_failure
    required = summary.get("required_engines")
    if not _exact_list(required, engines):
        return (
            f"{block} declares invalid engines {required!r}; official evidence "
            f"requires exactly {sorted(engines)}"
        )
    expected_rows = 50_000 if block == "design_space" else 1_000_000
    expected_repeats = 3 if block == "design_space" else 1
    expected_cells = (
        len(engines) * len(REQUIRED_DOMAINS) * len(REQUIRED_DIAGNOSTICS)
    )
    if summary.get("kind") != "design_space_collect":
        return "kind must be design_space_collect"
    if (
        summary.get("pinned_rows") != expected_rows
        or summary.get("pinned_d5_rows") != expected_rows
        or summary.get("min_repeats") != expected_repeats
    ):
        return (
            f"{block} must use the canonical {expected_rows}-row, "
            f"{expected_repeats}-repeat protocol"
        )
    if (
        summary.get("scope") != "latest"
        or summary.get("repeats_kept") != expected_repeats
        or summary.get("cells") != expected_cells
        or summary.get("expected_cells") != expected_cells
    ):
        return f"{block} must declare the complete canonical collection protocol"
    rows = _rows(summary)
    if rows is None:
        return "rows must be a list of cell objects"
    expected = {
        (engine, domain, diagnostic)
        for engine in engines
        for domain in REQUIRED_DOMAINS
        for diagnostic in REQUIRED_DIAGNOSTICS
    }
    if not _exact_grid(rows, ("engine", "domain", "diagnostic"), expected):
        return (
            f"rows must contain the exact {len(engines)} x "
            f"{len(REQUIRED_DOMAINS)} x {len(REQUIRED_DIAGNOSTICS)} "
            "design-space universe"
        )
    provenance_failure = _provenance_failure(rows, ("domain", "diagnostic"))
    if provenance_failure:
        return provenance_failure

    if any(
        not isinstance(row.get("status"), str)
        or row.get("status") not in {"pass", "fail", "repeat-unstable", "unsupported"}
        for row in rows
    ):
        return (
            "every design-space cell must have a final "
            "pass/fail/repeat-unstable/unsupported status"
        )
    if summary.get("missing_cells") not in ([], None):
        return "design-space summary reports missing cells"
    for field in (
        "input_inconsistent_cells",
        "oracle_inconsistent_cells",
        "cross_engine_input_inconsistent",
        "cross_engine_oracle_inconsistent",
        "repeat_mismatch_cells",
    ):
        if summary.get(field) not in ([], None):
            return f"design-space summary reports {field}"
    if summary.get("cells_below_min_repeats") not in (0, None):
        return "design-space summary reports cells below the repeat contract"
    for row in rows:
        status = row.get("status")
        repeats = row.get("repeats")
        repeats_pass = row.get("repeats_pass")
        if (
            isinstance(repeats, bool)
            or repeats != expected_repeats
            or isinstance(repeats_pass, bool)
            or not isinstance(repeats_pass, int)
            or row.get("rows") != expected_rows
            or row.get("runs_blocked") != 0
            or row.get("runs_missing") != 0
            or row.get("missing_repeat_indices") not in ([], None)
            or row.get("pass") is not (status == "pass")
            or row.get("repeat_unstable") is not (status == "repeat-unstable")
        ):
            return "every design-space cell must contain the exact completed repeats"
        if status == "pass" and repeats_pass != expected_repeats:
            return "passing design-space cell has a non-passing repeat"
        if status == "fail" and repeats_pass != 0:
            return "failing design-space cell has an inconsistent repeat count"
        if status == "repeat-unstable" and not (0 < repeats_pass < expected_repeats):
            return "repeat-unstable design-space cell must contain both pass and fail repeats"
        if status == "unsupported":
            if repeats_pass != 0 or row.get("runs_unsupported") != expected_repeats:
                return "unsupported design-space cell has inconsistent repeats"
        elif row.get("runs_unsupported") != 0:
            return "supportedness changed across design-space repeats"
    return None


def _scaling_scope_failure(summary: dict[str, Any]) -> str | None:
    engines, exclusion_failure = declared_engines(summary, "scaling")
    if exclusion_failure:
        return exclusion_failure
    required = summary.get("required_engines")
    if not _exact_list(required, engines):
        return (
            f"scaling declares invalid engines {required!r}; official evidence "
            f"requires exactly {sorted(engines)}"
        )
    if summary.get("kind") != "scaling_collect":
        return "kind must be scaling_collect"
    expected_cells = (
        len(engines)
        * len(REQUIRED_DOMAINS)
        * len(SCALING_DIAGNOSTICS)
        * len(SCALING_SIZES)
    )
    if (
        summary.get("base_size") != 50_000
        or summary.get("min_repeats") != 3
        or summary.get("required_repeats") != 3
        or not _exact_list(summary.get("sizes"), SCALING_SIZES)
        or summary.get("cells") != expected_cells
        or summary.get("expected_cells") != expected_cells
    ):
        return "scaling must use the canonical five sizes and three-repeat protocol"
    rows = _rows(summary)
    if rows is None:
        return "rows must be a list of cell objects"
    expected = {
        (engine, domain, diagnostic, size)
        for engine in engines
        for domain in REQUIRED_DOMAINS
        for diagnostic in SCALING_DIAGNOSTICS
        for size in SCALING_SIZES
    }
    if not _exact_grid(
        rows, ("engine", "domain", "diagnostic", "size"), expected
    ):
        return (
            f"rows must contain the exact {len(engines)} x "
            f"{len(REQUIRED_DOMAINS)} x {len(SCALING_DIAGNOSTICS)} x "
            f"{len(SCALING_SIZES)} scaling universe"
        )
    provenance_failure = _provenance_failure(
        rows, ("domain", "diagnostic", "size"))
    if provenance_failure:
        return provenance_failure

    for field in (
        "missing_cells",
        "thin_cells",
        "input_inconsistent_cells",
        "oracle_inconsistent_cells",
        "repeat_mismatch_cells",
        "cross_engine_input_inconsistent",
        "cross_engine_oracle_inconsistent",
    ):
        if summary.get(field) not in ([], None):
            return f"scaling summary reports {field}"
    # "nondeterministic" is the scaling counterpart of the design-space
    # "repeat-unstable" verdict: a complete repeat set whose verdict is not reproducible.
    # docs/RUNBOOK.md states such a set stays promotable, and the cell carries
    # stable=false so Table 8 counts it as unstable rather than hiding it.
    allowed = {"equivalent", "known_divergence", "divergence", "unsupported",
               "nondeterministic"}
    for row in rows:
        status = row.get("status")
        if (
            not isinstance(status, str)
            or status not in allowed
            or not isinstance(row.get("stable"), bool)
            or row.get("repeats") != 3
            or not isinstance(row.get("executed"), bool)
            or not isinstance(row.get("throughput_eligible"), bool)
            or row.get("pass") is not (status == "equivalent")
        ):
            return "every scaling cell must be complete and classified"
        if status == "nondeterministic" and row.get("stable") is not False:
            return "a nondeterministic scaling cell must be marked unstable"
        if status == "unsupported" and row.get("throughput_eligible") is not False:
            return "unsupported scaling cells cannot carry throughput evidence"
    return None


def _boundary_scope_failure(summary: dict[str, Any]) -> str | None:
    if summary.get("kind") != "boundary":
        return "kind must be boundary"
    # malformed_rows is configuration, not a constant: the per-row check below
    # binds what matters, that every injected row was rejected and every
    # violation class was covered equally. Pinning a literal here made
    # strengthening the control a promotion failure.
    malformed = summary.get("malformed_rows")
    if (summary.get("max_rows") != 5_000
            or isinstance(malformed, bool)
            or not isinstance(malformed, int)
            or malformed < 1
            or summary.get("pass") is not True):
        return (
            "boundary evidence must be the passing canonical 5,000-row "
            "control with a positive malformed-row count"
        )
    if not _exact_list(summary.get("domains"), REQUIRED_DOMAINS):
        return "boundary evidence must declare all five domains exactly once"
    rows = _rows(summary)
    if rows is None or not _exact_grid(
        rows, ("domain",), {(domain,) for domain in REQUIRED_DOMAINS}
    ):
        return "boundary rows must contain each of the five domains exactly once"
    for row in rows:
        if not _is_sha256(row.get("input_sha256")):
            return "every boundary row must preserve one 64-hex input_sha256"
        # injected is config/default.yaml perturbation.malformed_rows, not a
        # constant. The gate requires a control that covers every violation
        # class equally and rejects everything it injected, not one of a
        # particular size, so raising the count strengthens the control instead
        # of failing promotion.
        injected = row.get("injected")
        classes = row.get("violation_class_count")
        if (
            isinstance(injected, bool)
            or not isinstance(injected, int)
            or injected < 1
            or isinstance(classes, bool)
            or not isinstance(classes, int)
            or classes < 1
            or injected < classes
            or injected % classes
            or row.get("rows") != 5_000
            or row.get("pass") is not True
            or row.get("aggregates_unchanged") is not True
            or row.get("rejected") != injected
            or row.get("engine_rejected") != injected
            or row.get("engine_accepted_malformed") != 0
        ):
            return (
                "every boundary row must reject every injection "
                "before state changes"
            )
    input_hashes = {row["input_sha256"].lower() for row in rows}
    if len(input_hashes) != len(rows):
        return "boundary input_sha256 values must be unique across domains"
    return None


def _mutation_scope_failure(summary: dict[str, Any]) -> str | None:
    if summary.get("kind") != "detection_power":
        return "kind must be detection_power"
    if (
        summary.get("max_rows") != 50_000
        or summary.get("epsilon") != 1e-9
        or summary.get("pass") is not True
    ):
        return (
            "mutation evidence must be the passing canonical "
            "50,000-row epsilon=1e-9 control"
        )
    if not _exact_list(summary.get("domains"), REQUIRED_DOMAINS):
        return "mutation evidence must declare all five domains exactly once"
    if not _exact_list(summary.get("mutants"), REQUIRED_MUTANTS):
        return "mutation evidence must declare all seven mutants exactly once"
    if not _exact_list(summary.get("diagnostics"), MUTATION_DIAGNOSTICS):
        return "mutation evidence must declare diagnostics D1--D7 exactly once"
    rows = _rows(summary)
    expected = {
        (domain, mutant, diagnostic)
        for domain in REQUIRED_DOMAINS
        for mutant in REQUIRED_MUTANTS
        for diagnostic in MUTATION_DIAGNOSTICS
    }
    if rows is None or not _exact_grid(
        rows, ("domain", "mutant", "diagnostic"), expected
    ):
        return "mutation rows must contain the exact 5 x 7 x 7 control universe"
    for row in rows:
        target = MUTANT_TARGETS[row["mutant"]]
        if (
            row.get("target_diagnostic") != target
            or row.get("is_target")
            is not (row.get("diagnostic") == target)
            or row.get("vacuous") is not False
            or row.get("error") is not None
            or not isinstance(row.get("detected"), bool)
        ):
            return (
                "mutation rows contain a malformed, vacuous, "
                "or errored control cell"
            )
        if row.get("is_target") and row.get("detected") is not True:
            return "every targeted mutation must be detected"
    if (
        summary.get("cells") != len(expected)
        or summary.get("diagonal_caught") != 35
        or summary.get("diagonal_total") != 35
        or summary.get("sensitivity") != 1.0
        or summary.get("missed") not in ([], None)
        or summary.get("vacuous") not in ([], None)
        or summary.get("errors") not in ([], None)
    ):
        return "mutation summary must report a non-vacuous 35/35 diagonal"
    return None


def _d7s_scope_failure(summary: dict[str, Any]) -> str | None:
    if summary.get("kind") != "d7s_sweep":
        return "kind must be d7s_sweep"
    max_rows = summary.get("max_rows")
    if (
        isinstance(max_rows, bool)
        or not isinstance(max_rows, int)
        or max_rows != 200_000
    ):
        return "D7S evidence must be the canonical 200,000-row sweep"
    if not _exact_list(summary.get("domains"), REQUIRED_DOMAINS):
        return "D7S evidence must declare all five domains exactly once"
    if not _exact_list(summary.get("alphas"), D7S_ALPHAS):
        return "D7S evidence must use the canonical six alpha values"
    if not _exact_list(summary.get("kll_ks"), D7S_KLL_KS):
        return "D7S evidence must use the canonical three sketch sizes"
    rows = _rows(summary)
    expected = {
        (domain, alpha, kll_k)
        for domain in REQUIRED_DOMAINS
        for alpha in D7S_ALPHAS
        for kll_k in D7S_KLL_KS
    }
    if rows is None or not _exact_grid(
        rows, ("domain", "alpha", "kll_k"), expected
    ):
        return "D7S rows must contain the exact 5 x 6 x 3 sensitivity grid"
    separating = [row for row in rows if row.get("separates") is True]
    if (
        summary.get("cells") != len(expected)
        or summary.get("separating_cells") != len(separating)
    ):
        return "D7S summary counters must match its complete separation grid"
    if summary.get("pass") is not bool(separating):
        return "D7S pass must report whether the complete grid separates"
    expected_by_domain = {
        domain: sum(row.get("domain") == domain for row in separating)
        for domain in REQUIRED_DOMAINS
    }
    if summary.get("separating_by_domain") != expected_by_domain:
        return "D7S per-domain separation counts do not match its grid"
    for row in rows:
        row_count = row.get("rows")
        restatements = row.get("restatements")
        if (
            not isinstance(row.get("separates"), bool)
            or isinstance(row_count, bool)
            or not isinstance(row_count, int)
            or isinstance(restatements, bool)
            or not isinstance(restatements, int)
            or restatements <= 0
            or row_count != max_rows + restatements
            or isinstance(row.get("groups_scored"), bool)
            or not isinstance(row.get("groups_scored"), int)
            or row.get("groups_scored") <= 0
        ):
            return "every D7S cell must be non-vacuous and carry a separation verdict"
    return None


def _native_scope_failure(summary: dict[str, Any]) -> str | None:
    if (
        summary.get("kind") != "native_semantics"
        or summary.get("probe") != "all"
    ):
        return "native evidence must be a complete native_semantics probe=all run"
    if summary.get("pass") is not True:
        return "native probe control did not pass"
    if not _exact_list(summary.get("engines"), REQUIRED_ENGINES):
        return "native evidence must declare all seven engines exactly once"
    if not _exact_list(summary.get("domains"), REQUIRED_DOMAINS):
        return "native evidence must declare all five domains exactly once"
    if (
        summary.get("max_rows") != 20_000
        or summary.get("trajectory_checkpoints") != 4
        or summary.get("k") != 10
        or summary.get("rank_metric") != "mean"
        or summary.get("allowed_lateness_s") != 300
    ):
        return "native evidence must use the canonical probe parameters"
    topk = summary.get("topk")
    watermark = summary.get("watermark")
    if not isinstance(topk, list) or not all(
        isinstance(row, dict) for row in topk
    ):
        return "native Top-N rows must be a list of cell objects"
    if not isinstance(watermark, list) or not all(
        isinstance(row, dict) for row in watermark
    ):
        return "native watermark rows must be a list of cell objects"
    expected_topk = {
        (engine, domain)
        for engine in REQUIRED_ENGINES
        for domain in REQUIRED_DOMAINS
    }
    measured_count = sum(
        row.get("status") == "measured" for row in topk + watermark)
    if (summary.get("classified_cells") != len(expected_topk) + len(REQUIRED_ENGINES)
            or summary.get("measured_cells") != measured_count):
        return "native summary cell counts do not match its complete probe grid"
    if not _exact_grid(topk, ("engine", "domain"), expected_topk):
        return "native Top-N rows must contain the exact 7 x 5 universe"
    if not _exact_grid(
        watermark, ("engine",), {(engine,) for engine in REQUIRED_ENGINES}
    ):
        return "native watermark rows must contain all seven engines exactly once"
    allowed = {"measured", "unsupported", "not_expressible"}
    for row in topk:
        status = row.get("status")
        if (
            row.get("kind") != "native_topk"
            or row.get("k") != 10
            or row.get("rank_metric") != "mean"
            or not isinstance(status, str)
            or status not in allowed
            or row.get("artifact_valid") is not True
        ):
            return (
                "native Top-N row has an invalid, blocked, "
                "or unclassified status"
            )
        checkpoints = row.get("checkpoints")
        if status == "measured":
            checkpoint_rows = (
                checkpoints
                if isinstance(checkpoints, list)
                and all(isinstance(point, dict) for point in checkpoints)
                else []
            )
            points = [point.get("checkpoint") for point in checkpoint_rows]
            point_rows = [point.get("rows") for point in checkpoint_rows]
            point_passes = []
            for point in checkpoint_rows:
                hourly = point.get("hourly")
                daily = point.get("daily")
                if (
                    not isinstance(hourly, dict)
                    or not isinstance(daily, dict)
                    or not isinstance(hourly.get("equivalent"), bool)
                    or not isinstance(daily.get("equivalent"), bool)
                    or point.get("pass")
                    is not (hourly["equivalent"] and daily["equivalent"])
                ):
                    return "measured native Top-N checkpoint is malformed"
                point_passes.append(point["pass"])
            if (
                not isinstance(row.get("pass"), bool)
                or row.get("supported") is not True
                or row.get("rows") != 20_000
                or row.get("trajectory_checkpoints") != 4
                or points != [1, 2, 3, 4]
                or point_rows != [5_000, 10_000, 15_000, 20_000]
                or row.get("pass") is not all(point_passes)
            ):
                return "measured native Top-N row has an incomplete trajectory"
        elif (
            row.get("pass") is not None
            or row.get("supported") is not False
            or checkpoints not in ([], None)
        ):
            return (
                "unmeasured native Top-N row must be an explicit unsupported "
                "classification without trajectory points"
            )
    for row in watermark:
        status = row.get("status")
        if (
            row.get("kind") != "native_watermark"
            or row.get("allowed_lateness_s") != 300
            or not isinstance(status, str)
            or status not in {"measured", "unsupported"}
            or row.get("artifact_valid") is not True
            or (
                status == "measured"
                and (
                    row.get("supported") is not True
                    or not isinstance(row.get("late_event_dropped"), bool)
                    or not isinstance(row.get("on_time_event_included"), bool)
                    or row.get("pass") is not (
                        row["late_event_dropped"] and row["on_time_event_included"])
                )
            )
            or (
                status == "unsupported"
                and (row.get("supported") is not False or row.get("pass") is not None)
            )
        ):
            return (
                "native watermark row has an invalid, blocked, "
                "or unclassified status"
            )
    if not any(
        row.get("status") == "measured" for row in topk + watermark
    ):
        return "native evidence must contain at least one measured cell"
    return None


def official_scope_failure(
    block: str, summary: dict[str, Any],
) -> str | None:
    """Return why one summary cannot be used as official paper evidence."""
    if block in {"design_space", "design_space_confirmation"}:
        return _design_scope_failure(block, summary)
    if block == "scaling":
        return _scaling_scope_failure(summary)
    if block == "oracle_verification":
        domain = summary.get("domain")
        comparisons = summary.get("comparisons")
        if (
            summary.get("kind") != "oracle_verification"
            or not isinstance(domain, str)
            or domain not in REQUIRED_DOMAINS
            or summary.get("rows") != 1_000_000
            or isinstance(summary.get("hourly_keys"), bool)
            or not isinstance(summary.get("hourly_keys"), int)
            or summary.get("hourly_keys") <= 0
            or isinstance(summary.get("daily_keys"), bool)
            or not isinstance(summary.get("daily_keys"), int)
            or summary.get("daily_keys") <= 0
            or summary.get("all_equivalent") is not True
            or summary.get("pass") is not True
        ):
            return (
                "oracle evidence must be one equivalent canonical-domain "
                "1,000,000-row run"
            )
        if (
            not isinstance(comparisons, dict)
            or set(comparisons) != ORACLE_COMPARISONS
        ):
            return "oracle evidence must contain all six pairwise comparisons"
        if any(
            not isinstance(comparison, dict)
            or comparison.get("equivalent") is not True
            or comparison.get("mismatched_keys") != 0
            for comparison in comparisons.values()
        ):
            return "every oracle pairwise comparison must be exactly equivalent"
        return None
    if block == "boundary":
        return _boundary_scope_failure(summary)
    if block == "mutation":
        return _mutation_scope_failure(summary)
    if block == "d7s_sweep":
        return _d7s_scope_failure(summary)
    if block == "native_semantics":
        return _native_scope_failure(summary)
    if block == "query_family":
        return _query_family_scope_failure(summary)
    if block == "disorder_sweep":
        return _disorder_sweep_scope_failure(summary)
    if block == "parallelism_sweep":
        return _parallelism_sweep_scope_failure(summary)
    if block == "formulation_cost":
        return _formulation_cost_scope_failure(summary)
    return None


def _disorder_sweep_scope_failure(summary: dict[str, Any]) -> str | None:
    """The disorder sweep is promotable only with its ordered-delivery point,
    agreeing oracles, and at least one engine measured beside the oracle.

    The ordered point is the control: with no disorder the arrival-ordered and
    event-time selections coincide, so the exposure there must be zero. A
    non-zero exposure at window 0 means the sweep measured its own
    construction."""
    if summary.get("kind") != "disorder_sweep":
        return "kind must be disorder_sweep"
    rows = _rows(summary)
    if rows is None or not rows:
        return "disorder_sweep evidence must carry its cells"
    exposure = [r for r in rows if r.get("kind") == "exposure"]
    if not exposure:
        return "disorder_sweep evidence must carry the oracle exposure curve"
    if summary.get("oracle_disagreements"):
        return "an oracle disagreement voids the sweep; it is a harness error"
    if summary.get("blocked"):
        return "a blocked engine cell voids the sweep"
    ordered = [r for r in exposure if str(r.get("window")) == "0"]
    if not ordered or any(r.get("wrong_keys") for r in ordered):
        return ("the ordered-delivery point must show zero exposure; the "
                "sweep is measuring its own construction")
    if not any(r.get("kind") == "engine" and r.get("status") == "measured"
               for r in rows):
        return "disorder_sweep evidence must measure at least one engine"
    return None


def _parallelism_sweep_scope_failure(summary: dict[str, Any]) -> str | None:
    if summary.get("kind") != "parallelism_sweep":
        return "kind must be parallelism_sweep"
    rows = _rows(summary)
    if rows is None or not rows:
        return "parallelism_sweep evidence must carry its cells"
    if summary.get("blocked"):
        return "a blocked cell voids the parallelism sweep"
    levels = {r.get("parallelism") for r in rows}
    if 1 not in levels or len(levels) < 2:
        return ("parallelism_sweep evidence must include parallelism 1 and at "
                "least one higher level")
    return None


def _formulation_cost_scope_failure(summary: dict[str, Any]) -> str | None:
    """Cost evidence is promotable only if it was serialized and complete:
    timings shared with a concurrent lane would measure contention, not the
    formulation."""
    if summary.get("kind") != "formulation_cost":
        return "kind must be formulation_cost"
    if summary.get("serialized") is not True:
        return "formulation_cost evidence must come from a serialized run"
    if summary.get("blocked"):
        return "a blocked timing voids the cost comparison"
    ratios = summary.get("ratios")
    if not isinstance(ratios, list) or not ratios:
        return "formulation_cost evidence must carry at least one ratio"
    return None


def _query_family_scope_failure(summary: dict[str, Any]) -> str | None:
    """The generated family is promotable only with its control.

    The reported gap is the fraction of members matching the oracle under the
    contract formulation alone. That number is meaningless without the
    ordered-delivery control, where the two formulations coincide and the gap
    must vanish: a family that reports a gap there is detecting its own
    construction rather than the engine's behaviour. An oracle disagreement is
    a harness error and voids the cohort for the same reason it does elsewhere.
    """
    if summary.get("kind") != "query_family":
        return "kind must be query_family"
    rows = _rows(summary)
    if rows is None or not rows:
        return "query_family evidence must carry its cells"
    deliveries = {row.get("delivery") for row in rows}
    if not {"shuffled", "ordered"} <= deliveries:
        return ("query_family evidence must carry both the shuffled treatment "
                "and the ordered-delivery control")
    if summary.get("oracle_disagreements"):
        return "an oracle disagreement voids the cohort; it is a harness error"
    if summary.get("blocked"):
        return "a blocked member voids the cohort; no gap may be read from it"
    # Recomputed from the cells rather than trusted from a summary field.
    from src.diagnostics.shapes import control_verdict
    control = control_verdict(rows)
    if not control["construction_clean"]:
        return ("the ordered-delivery control shows a gap on every engine for "
                f"{control['construction_failures']}; the family is measuring "
                "its own construction there")
    expressible = summary.get("expressible_cells")
    if not isinstance(expressible, int) or expressible < 1:
        return "query_family evidence must score at least one expressible member"
    return None


def collection_scope_failure(
    block: str, summaries: list[dict[str, Any]],
) -> str | None:
    """Validate scope properties that span multiple promoted summaries."""
    if block == "oracle_verification":
        domains = [summary.get("domain") for summary in summaries]
        if not _exact_list(domains, REQUIRED_DOMAINS):
            return (
                "oracle evidence must contain each of the five "
                "canonical domains exactly once"
            )
    return None
