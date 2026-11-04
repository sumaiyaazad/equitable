"""
I/O helpers for Step 10 figure/table generators.

Reads promoted results from results/<block>/ and (optionally) fresh runs/
for development iteration.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.provenance import (git_commit as provenance_git_commit,
                            git_dirty as provenance_git_dirty, run_provenance,
                            source_dirty as provenance_source_dirty)  # noqa: E402
from scripts.results import _finalizer  # noqa: E402
RESULTS = ROOT / "results"
RUNS = ROOT / "runs"

ENGINE_ORDER = [
    "reference",
    "materialize",
    "risingwave",
    "ksqldb",
    "flink",
    "spark",
    "feldera",
]

CONTRACT_DOMAINS = frozenset({
    "environment_airquality",
    "transportation_taxi",
    "datacenter_drives",
    "finance_markets",
    "sports_football",
})
CONTRACT_DIAGNOSTICS = frozenset({"D1", "D2", "D3", "D4", "D5", "D6", "D7",
                                 "D8"})
CONTRACT_STATUSES = frozenset({"pass", "fail", "repeat-unstable", "unsupported", "blocked"})
SCALING_DIAGNOSTICS = frozenset(CONTRACT_DIAGNOSTICS - {"D5"})
#: D8 is a query shape, not a perturbation, so no mutant targets it and the
#: detection-power grid never runs it (scripts/run/run_detection_power.py).
MUTATION_DIAGNOSTICS = frozenset(CONTRACT_DIAGNOSTICS - {"D8"})
SCALING_SIZES = frozenset({10_000, 50_000, 200_000, 500_000, 1_000_000})
D7S_ALPHAS = frozenset({0.001, 0.01, 0.05, 0.10, 0.25, 0.50})
D7S_KLL_KS = frozenset({64, 200, 800})
MUTATION_TARGETS = {
    "mutant_no_dedup": (1, "D1"),
    "mutant_order_sensitive": (2, "D2"),
    "mutant_ulp_drift": (3, "D3"),
    "mutant_drop_late": (4, "D4"),
    "mutant_ignore_supersedes": (7, "D7"),
    "mutant_recovery_loss": (5, "D5"),
    "mutant_topk_rank": (6, "D6"),
}
ORACLE_COMPARISONS = frozenset({
    "engine_vs_pandas_hourly",
    "engine_vs_pandas_daily",
    "engine_vs_duckdb_hourly",
    "engine_vs_duckdb_daily",
    "pandas_vs_duckdb_hourly",
    "pandas_vs_duckdb_daily",
})

DIAGNOSTIC_ORDER = ["D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8", "D7S"]

# D7S validates the benchmark's OWN bounded-deletion reference sketches (the
# oracle for correctness condition C7); it is engine-INDEPENDENT and runs once per
# domain on the reference row. So it does NOT belong in the per-engine
# design-space matrix (Fig 2 / Tab 6 / Tab 7) -- its full result is Tab 10.
ENGINE_INDEPENDENT_DIAGNOSTICS = {"D7S"}

# Claim level each diagnostic supports, per the "Unit of claim" paragraph of the
# paper. The levels are INDEPENDENT: a portable pass licenses no lifecycle or
# native claim, and a missing native interface is an absent measurement rather
# than a failure. D7S carries no engine claim at any level (it is an
# engine-independent oracle control on the benchmark's own sketches) and is
# kept out of the per-engine matrix by ENGINE_INDEPENDENT_DIAGNOSTICS.
DIAGNOSTIC_LEVEL = {
    "D1": "portable",
    "D2": "portable",
    "D3": "portable",
    "D4": "portable",
    "D5": "lifecycle",
    "D6": "portable",
    "D7": "portable",
    # D8 is a query SHAPE rather than a perturbation, following D6, which is
    # also a shape. A hopping window emits the same bucket table keyed by
    # window start, so the claim it licenses is portable like the rest.
    "D8": "portable",
    "D4W": "native",
    "D6N": "native",
    "D7S": "oracle",
}

#: Left-to-right order of the claim-level bands in the design-space matrix.
LEVEL_ORDER = ["portable", "lifecycle", "native", "oracle"]

LEVEL_LABEL = {
    "portable": "Portable",
    "lifecycle": "Lifecycle",
    "native": "Native",
    "oracle": "Oracle",
}

_RUN_STAMP_RE = re.compile(r"^(\d{8}T\d{6}(?:\d{6})?Z)")
_INPUT_SHA_CACHE: Dict[Path, str] = {}

def git_head() -> str:
    return provenance_git_commit(ROOT)


def git_dirty() -> bool:
    """Whether an aggregate built here would rest on unrecorded source.

    Inside a benchmark run, git_head() is the commit that measured it. A clean
    checkout whose HEAD descends from that commit and changes only finalization
    code (scripts/results/_finalizer.py) is not dirty: a collector fix can be
    committed and run over finished evidence without re-measuring a cell.
    """
    evidence = git_head()
    if (finalizer_commit() != evidence
            and not provenance_source_dirty(ROOT)
            and _finalizer.may_finalize(evidence, ROOT)):
        return False
    return provenance_git_dirty(ROOT)


def finalizer_commit() -> str:
    """The commit whose finalization code ran; git_head() names the evidence."""
    return _finalizer.head(ROOT)


def valid_current_run(summary: Dict[str, Any]) -> bool:
    """True only for a v2 run from the current source, config, and lock."""
    expected = run_provenance(ROOT)
    invocation_id = summary.get("invocation_id")
    return (
        summary.get("contract_version") == 2
        and summary.get("git_commit") == git_head()
        and summary.get("git_dirty") is False
        and isinstance(invocation_id, str) and bool(invocation_id.strip())
        and summary.get("benchmark_config_sha256")
            == expected["benchmark_config_sha256"]
        and summary.get("dependency_lock_sha256")
            == expected["dependency_lock_sha256"]
    )


def valid_input_artifact(summary: Dict[str, Any]) -> bool:
    """Verify that a run names an existing in-repository input with its SHA-256."""
    raw_path = summary.get("input")
    expected = summary.get("input_sha256")
    if not isinstance(raw_path, str) or not isinstance(expected, str):
        return False
    path = Path(raw_path)
    if not path.is_absolute():
        path = ROOT / path
    # Containment is checked LEXICALLY, before following symlinks, because a
    # benchmark run executes from an immutable source snapshot whose data/ entries
    # are symlinks into the primary checkout (scripts/run/run_isolated.sh).
    # Resolving first made every recorded input look like an escape, so a
    # collection run from inside the snapshot saw zero runs and the whole
    # finalize path reported "no fingerprinted benchmark found". Traversal is
    # still refused: the normalized path must contain no "..", must stay under
    # the repository root, and must still match its recorded hash below.
    try:
        normalized = Path(os.path.normpath(str(path)))
        if ".." in normalized.parts:
            return False
        normalized.relative_to(ROOT)
    except (OSError, ValueError):
        return False
    path = normalized
    if not path.is_file():
        return False
    actual = _INPUT_SHA_CACHE.get(path)
    if actual is None:
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        _INPUT_SHA_CACHE[path] = actual
    return actual == expected


def _valid_promoted_summary(
    summary_path: Path, summary: Dict[str, Any], block: str,
) -> bool:
    manifest_path = summary_path.parent / "promotion_manifest.json"
    if not manifest_path.is_file():
        return False
    try:
        manifest = json.loads(manifest_path.read_text())
        expected = manifest["files_sha256"]["summary.json"]
    except (KeyError, TypeError, ValueError, OSError):
        return False
    actual = hashlib.sha256(summary_path.read_bytes()).hexdigest()
    return (
        summary.get("contract_version") == 2
        and summary.get("artifact_valid") is True
        and summary.get("git_dirty") is False
        and isinstance(summary.get("invocation_id"), str)
        and bool(summary.get("invocation_id", "").strip())
        and manifest.get("invocation_id") == summary.get("invocation_id")
        and manifest.get("benchmark_config_sha256") == summary.get("benchmark_config_sha256")
        and manifest.get("dependency_lock_sha256") == summary.get("dependency_lock_sha256")
        and manifest.get("block") == block
        and manifest.get("git_commit") == summary.get("git_commit")
        and actual == expected
    )



def load_summaries(block: str) -> List[Dict[str, Any]]:
    """Load every results/<block>/*/summary.json (empty list if block missing)."""
    block_dir = RESULTS / block
    if not block_dir.is_dir():
        return []
    rows: List[Dict[str, Any]] = []
    for summary_path in sorted(block_dir.glob("*/summary.json")):
        summary = json.loads(summary_path.read_text())
        if not _valid_promoted_summary(summary_path, summary, block):
            continue
        summary["_run_id"] = summary_path.parent.name
        summary["_summary_path"] = str(summary_path)
        rows.append(summary)
    return rows


def _is_collector(summary: Dict[str, Any]) -> bool:
    rows = summary.get("rows")
    return isinstance(rows, list) and bool(rows)


def load_cells(block: str) -> List[Dict[str, Any]]:
    """Load promoted summaries for `block`, flattening collector summaries.

    A collector run (e.g. design_space_collect) stores its per-cell records under
    a top-level "rows" list; a per-cell run summary stores those fields directly.
    Either way this returns a flat list of cell dicts, so figure/table generators
    can iterate cells uniformly.

    Only the NEWEST promoted collector run is used. `results/` is append-only, so
    superseding a collection means promoting a newer one -- concatenating every
    collection would double-count cells and let a stale verdict win.
    """
    summaries = load_summaries(block)
    collectors = [s for s in summaries if _is_collector(s)]
    if collectors:
        newest = max(collectors, key=lambda s: _run_stamp(s["_run_id"]))
        summaries = [s for s in summaries if not _is_collector(s)] + [newest]

    cells: List[Dict[str, Any]] = []
    for summary in summaries:
        if _is_collector(summary):
            cells.extend(r for r in summary["rows"] if isinstance(r, dict))
        else:
            cells.append(summary)
    return cells


def _run_stamp(run_id: str) -> str:
    m = _RUN_STAMP_RE.match(run_id)
    return m.group(1) if m else run_id


def best_attempt(
    candidates: Iterable[Dict[str, Any]],
    stamp: Callable[[Dict[str, Any]], Any],
) -> Optional[Dict[str, Any]]:
    """The attempt that decides one cell: newest VALID, else newest of any kind.

    A blocked cell (artifact_valid=false) is an infrastructure fault, not a
    measurement -- the lane retry loop exists precisely to turn one into a
    verdict. Selecting the newest attempt outright made that loop able to run
    backwards: in benchmark_20260907T185404Z a RisingWave wedge stopped the
    scaling phase after finance_markets/D6/500k/rep1 had already recorded a
    verdict, and the retry re-ran that cell against a still-restarting engine
    and replaced a good result with a blocked one.

    Preferring the newest valid attempt cannot hide a real divergence: a
    divergence IS a verdict and so is itself artifact_valid. It only stops a
    later fault from erasing an earlier measurement. A cell with no valid
    attempt at all still resolves to its newest failure, so it stays blocked
    and the promotion gate still refuses it.
    """
    newest: Optional[Dict[str, Any]] = None
    newest_valid: Optional[Dict[str, Any]] = None
    for summary in candidates:
        if newest is None or stamp(summary) > stamp(newest):
            newest = summary
        if summary.get("artifact_valid") is True and not summary.get("error"):
            if newest_valid is None or stamp(summary) > stamp(newest_valid):
                newest_valid = summary
    return newest_valid if newest_valid is not None else newest


def all_runs_by_key(
    pattern: str = "*_*_*_d*",
    key_fields: Tuple[str, ...] = ("engine", "domain", "diagnostic"),
) -> Dict[Tuple[Any, ...], List[Dict[str, Any]]]:
    """
    Scan runs/ for directories matching `pattern` and return EVERY summary.json
    per composite key, oldest first.

    This is the collector's input: a design-space cell is decided by all of its
    repeats, not by whichever run happened to finish last. Quarantined runs
    (runs/_quarantine*/, runs/_pregate*/) are excluded -- the glob only matches
    timestamped run directories at the top level.
    """
    grouped: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
    if not RUNS.is_dir():
        return grouped
    for run_dir in sorted(RUNS.glob(pattern), key=lambda p: _run_stamp(p.name)):
        if not run_dir.is_dir():
            continue
        summary_path = run_dir / "summary.json"
        if not summary_path.is_file():
            continue
        try:
            summary = json.loads(summary_path.read_text())
        except (ValueError, OSError):
            continue
        if not valid_current_run(summary) or not valid_input_artifact(summary):
            continue
        key = tuple(summary.get(f) for f in key_fields)
        if any(v is None for v in key):
            continue
        summary["_run_id"] = run_dir.name
        summary["_summary_path"] = str(summary_path)
        grouped.setdefault(key, []).append(summary)
    return grouped


def latest_invocation_id(summaries: Iterable[Dict[str, Any]]) -> Optional[str]:
    """Newest explicit invocation among summaries; never infer cohorts by time."""
    rows = [s for s in summaries
            if isinstance(s.get("invocation_id"), str)
            and s.get("invocation_id", "").strip()]
    if not rows:
        return None
    by_id: Dict[str, List[Dict[str, Any]]] = {}
    for summary in rows:
        by_id.setdefault(summary["invocation_id"], []).append(summary)
    return max(by_id, key=lambda invocation: max(
        _run_stamp(str(s.get("_run_id", ""))) for s in by_id[invocation]))


def restrict_to_latest_invocation(
    grouped: Dict[Tuple[Any, ...], List[Dict[str, Any]]],
) -> Tuple[Dict[Tuple[Any, ...], List[Dict[str, Any]]], Optional[str]]:
    """Filter every key to one newest explicit benchmark invocation."""
    invocation = latest_invocation_id(
        s for summaries in grouped.values() for s in summaries)
    if invocation is None:
        return {}, None
    return ({
        key: [s for s in summaries if s.get("invocation_id") == invocation]
        for key, summaries in grouped.items()
        if any(s.get("invocation_id") == invocation for s in summaries)
    }, invocation)


def cohort_metadata(summaries: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Return cohort fingerprints plus a consistency verdict."""
    rows = list(summaries)
    required = ("invocation_id", "git_commit", "benchmark_config_sha256",
                "dependency_lock_sha256", "python_version", "platform",
                "hostname", "machine")
    optional = ("campaign_id", "campaign_phase")
    fields = required + optional
    values = {field: {s.get(field) for s in rows} for field in fields}
    consistent = bool(rows) and all(
        len(field_values) == 1 and None not in field_values
        for field, field_values in values.items() if field in required
    ) and all(len(values[field]) == 1 for field in optional)
    metadata = {field: next(iter(field_values)) if len(field_values) == 1 else None
                for field, field_values in values.items()}
    metadata["cohort_consistent"] = consistent
    return metadata


def latest_runs_by_key(
    pattern: str = "*_*_*_d*",
    key_fields: Tuple[str, ...] = ("engine", "domain", "diagnostic"),
) -> Dict[Tuple[Any, ...], Dict[str, Any]]:
    """
    Scan runs/ for directories matching `pattern` and return the latest
    summary.json per composite key.

    NOTE: the design-space collector deliberately does NOT use this -- taking the
    newest run per cell hides repeat disagreement. Use `all_runs_by_key`.
    """
    return {k: v[-1] for k, v in all_runs_by_key(pattern, key_fields).items()}


def table_output_dirs() -> List[Path]:
    """Return the table destinations.

    EQUITABLE_TABLES_DIR names one directory and wins when set; the public
    artifact uses it to write under tables/ without a paper/ tree. Otherwise
    the tables go to both manuscript drafts, or to paper/tables/ when no draft
    exists.
    """
    override = os.environ.get("EQUITABLE_TABLES_DIR")
    if override:
        return [Path(override)]
    dirs: List[Path] = []
    for draft in ("journal_draft", "conference_draft"):
        draft_dir = ROOT / "paper" / draft
        if draft_dir.is_dir():
            dirs.append(draft_dir / "tables")
    if not dirs:
        dirs.append(ROOT / "paper" / "tables")
    return dirs


def figure_output_dir() -> Path:
    """Return the figure destination: EQUITABLE_FIGURES_DIR, else paper/figures/."""
    override = os.environ.get("EQUITABLE_FIGURES_DIR")
    if override:
        return Path(override)
    return ROOT / "paper" / "figures"


def write_table(tex: str, stem: str) -> List[Path]:
    """Write `stem`.tex to every table output directory."""
    written: List[Path] = []
    for out_dir in table_output_dirs():
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{stem}.tex"
        path.write_text(tex)
        written.append(path)
        print(f"output: {path}")
    return written


def require_summaries(block: str, purpose: str) -> List[Dict[str, Any]]:
    """Load summaries or exit non-zero with promote instructions."""
    rows = load_summaries(block)
    if not rows:
        print(f"FAIL: no promoted results under results/{block}/")
        print(f"  {purpose}")
        print("  Promote a completed run first, e.g.:")
        print(f"    python scripts/results/promote_results.py runs/<run_id> --block {block}")
        raise SystemExit(1)
    return rows


def presentation_status(row: Dict[str, Any]) -> str:
    """Map one explicit evidence status to the paper's accounting vocabulary."""
    status = str(row.get("status"))
    if row.get("diagnostic") == "D5" and status == "unsupported":
        return "not_applicable"
    return status

# Unit roundoff of IEEE-754 single precision. A sum of N single-precision
# summands carries a worst-case relative error of about (N-1) times this, so a
# bucket that folds many rows may legitimately drift far more than one that
# folds a handful. That is why the numeric band below is scaled by the
# workload's own bucket sizes rather than fixed: a flat threshold either calls
# a low-cardinality domain's honest reassociation a semantic error, or gives a
# high-cardinality domain a licence it does not need.
FLOAT32_UNIT_ROUNDOFF = 2.0 ** -24
# Headroom over the worst case, matching src/diagnostics/findings.py so the
# reporting split and the engine signature cannot drift apart.
ACCUMULATION_SAFETY = 4.0

DIVERGENCE_CLASSES = ("key_set", "exact_field", "semantic_metric",
                      "numeric_metric", "nondeterministic")


def numeric_degree_ceilings(
    contract_epsilon: float = 1e-9,
) -> Dict[str, float]:
    """Largest deviation, per domain, that single-precision folding explains.

    Returned in the comparator's own unit: deviation divided by the contract
    tolerance, so 1 is the equivalence boundary. The bound for a domain is
    `safety * N * u / epsilon`, where N is that domain's largest bucket in the
    promoted oracle block -- rows divided by the smaller of its hourly and
    daily key counts, which is the coarsest grain and so the deepest fold.

    This is a REPORTING convention and changes no verdict: a cell past the
    bound and a cell inside it are both divergent, and the table prints the
    observed deviation beside the class so a reader can move the line without
    rerunning anything. Deriving it from the promoted oracle block rather than
    hardcoding a round number means it tracks the workload: change a domain's
    grain or size and the band that domain is read against moves with it.
    """
    ceilings: Dict[str, float] = {}
    for summary in load_summaries("oracle_verification"):
        domain = summary.get("domain")
        rows = summary.get("rows")
        keys = [summary.get("hourly_keys"), summary.get("daily_keys")]
        keys = [k for k in keys if isinstance(k, int) and k > 0]
        if not isinstance(domain, str) or not isinstance(rows, int) or not keys:
            continue
        summands = max(1.0, rows / min(keys))
        ceilings[domain] = (
            ACCUMULATION_SAFETY * summands * FLOAT32_UNIT_ROUNDOFF
            / contract_epsilon)
    return ceilings


def divergence_class(
    row: Dict[str, Any],
    ceilings: Optional[Dict[str, float]] = None,
) -> Optional[str]:
    """Which kind of disagreement one divergent cell exhibits.

    The classes are ordered by how much they constrain an explanation, and the
    first that applies wins:

    key_set          the maintained table holds rows the oracle does not, or
                     lacks rows it has. Whatever else is true, the wrong set of
                     events reached the view.
    exact_field      count or latest_event_time disagree. Neither admits any
                     tolerance, so a mismatch means different rows were folded
                     into the bucket; no amount of arithmetic care produces it.
    semantic_metric  only magnitudes moved, but by more than single-precision
                     folding of that domain's buckets can explain. The
                     reduction saw the right rows and still returned a
                     different number.
    numeric_metric   only magnitudes moved, within that bound. The view is
                     consistent with a correct reduction summed in a different
                     order and at a narrower width.
    nondeterministic the table matches the oracle but not a second replay of
                     the same input, which is what D3 alone can observe.

    Returns None for a cell that is not divergent. The classification never
    changes a verdict; it says what the divergence looks like, which is the
    difference between "this engine miscounted" and "this engine associates
    additions differently". A cell whose domain has no promoted ceiling is
    reported as semantic, so a missing bound can only understate the numeric
    class and never manufacture it.
    """
    if str(row.get("status")) not in ("fail", "repeat-unstable"):
        return None
    if int(row.get("structural_key_mismatches") or 0):
        return "key_set"
    if row.get("diverging_exact_fields"):
        return "exact_field"
    if row.get("diverging_metric_fields"):
        degree = row.get("max_degree")
        ceiling = (ceilings or {}).get(str(row.get("domain")))
        if (isinstance(degree, (int, float)) and ceiling is not None
                and degree <= ceiling):
            return "numeric_metric"
        return "semantic_metric"
    return "nondeterministic"


def _has_cell_provenance(row: Dict[str, Any]) -> bool:
    """Whether a cell carries the provenance its own status can support.

    The input hash binds every cell: the harness selects and hashes the rows
    before it asks the adapter anything. The oracle cache key binds only cells
    that were actually scored -- an unsupported cell has no expected table to
    name, because the harness stopped before building one. This mirrors
    scripts/results/_scope.names_an_oracle, so a block the promotion gate
    accepts is one the figure generators can also read.
    """
    input_sha256 = row.get("input_sha256")
    oracle_cache_keys = row.get("oracle_cache_keys")
    if not (
        isinstance(input_sha256, str)
        and len(input_sha256) == 64
        and all(
            char in "0123456789abcdefABCDEF" for char in input_sha256)
    ):
        return False
    if str(row.get("status")) == "unsupported":
        return True
    return (
        isinstance(oracle_cache_keys, list)
        and bool(oracle_cache_keys)
        and all(
            isinstance(key, str) and bool(key) for key in oracle_cache_keys)
    )



def promoted_engines(block: str) -> List[str]:
    """Engines the newest promoted collection for `block` claims to cover.

    A cohort may leave an engine out only by naming it in `excluded_engines`
    (see scripts/results/_scope.py). Generators validate against that declared
    universe rather than assuming all seven, so an excluded engine is a stated
    scope, never a silent gap.
    """
    summaries = [s for s in load_summaries(block) if _is_collector(s)]
    if not summaries:
        return list(ENGINE_ORDER)
    newest = max(summaries, key=lambda s: _run_stamp(s["_run_id"]))
    excluded = newest.get("excluded_engines") or []
    if not isinstance(excluded, list):
        return list(ENGINE_ORDER)
    return [e for e in ENGINE_ORDER if e not in set(excluded)]


def validate_design_space_cells(
    rows: Iterable[Dict[str, Any]],
    *,
    context: str = "design-space evidence",
    engines: Optional[Iterable[str]] = None,
) -> List[Dict[str, Any]]:
    """Return rows after requiring the complete 7 x 5 x 7 contract universe.

    An explicit unsupported D5 row is lifecycle-not-applicable at presentation
    time. An absent row is missing evidence and therefore a generation error.
    """
    materialized = list(rows)
    universe = list(engines) if engines is not None else list(ENGINE_ORDER)
    expected = {
        (engine, domain, diagnostic)
        for engine in universe
        for domain in CONTRACT_DOMAINS
        for diagnostic in CONTRACT_DIAGNOSTICS
    }
    keys = [
        (str(row.get("engine")), str(row.get("domain")), str(row.get("diagnostic")))
        for row in materialized
    ]
    counts = Counter(keys)
    got = set(counts)
    missing = sorted(expected - got)
    unexpected = sorted(got - expected)
    duplicates = sorted(key for key, count in counts.items() if count > 1)
    invalid = sorted(
        key for key, row in zip(keys, materialized)
        if row.get("status") not in CONTRACT_STATUSES
    )
    problems = []
    invalid_provenance = sorted(
        key for key, row in zip(keys, materialized)
        if not _has_cell_provenance(row)
    )
    if missing:
        problems.append(f"{len(missing)} missing cell(s), first={missing[0]}")
    if unexpected:
        problems.append(f"{len(unexpected)} unexpected cell(s), first={unexpected[0]}")
    if duplicates:
        problems.append(f"{len(duplicates)} duplicate cell(s), first={duplicates[0]}")
    if invalid:
        problems.append(f"{len(invalid)} invalid-status cell(s), first={invalid[0]}")
    if invalid_provenance:
        problems.append(
            f"{len(invalid_provenance)} missing-provenance cell(s), "
            f"first={invalid_provenance[0]}")
    if problems:
        raise ValueError(f"incomplete {context}: " + "; ".join(problems))
    return materialized


def validate_scaling_cells(
    rows: Iterable[Dict[str, Any]],
    *,
    context: str = "scaling evidence",
    engines: Optional[Iterable[str]] = None,
) -> List[Dict[str, Any]]:
    """Return rows after requiring the complete portable scaling universe."""
    materialized = list(rows)
    universe = list(engines) if engines is not None else list(ENGINE_ORDER)
    expected = {
        (engine, domain, diagnostic, size)
        for engine in universe
        for domain in CONTRACT_DOMAINS
        for diagnostic in SCALING_DIAGNOSTICS
        for size in SCALING_SIZES
    }
    keys = []
    invalid_key_types = []
    invalid_stable = []
    invalid_provenance = []
    for row in materialized:
        key = (
            row.get("engine"),
            row.get("domain"),
            row.get("diagnostic"),
            row.get("size"),
        )
        if (
            not all(isinstance(value, str) for value in key[:3])
            or isinstance(key[3], bool)
            or not isinstance(key[3], int)
        ):
            invalid_key_types.append(repr(key))
        else:
            keys.append(key)
        if not isinstance(row.get("stable"), bool):
            invalid_stable.append(repr(key))

        if not _has_cell_provenance(row):
            invalid_provenance.append(repr(key))
    counts = Counter(keys)
    got = set(counts)
    missing = sorted(expected - got)
    unexpected = sorted(got - expected)
    duplicates = sorted(key for key, count in counts.items() if count > 1)
    problems = []
    if missing:
        problems.append(f"{len(missing)} missing cell(s), first={missing[0]}")
    if unexpected:
        problems.append(
            f"{len(unexpected)} unexpected cell(s), first={unexpected[0]}")
    if duplicates:
        problems.append(
            f"{len(duplicates)} duplicate cell(s), first={duplicates[0]}")
    if invalid_key_types:
        problems.append(
            f"{len(invalid_key_types)} invalid-key cell(s), "
            f"first={invalid_key_types[0]}")
    if invalid_stable:
        problems.append(
            f"{len(invalid_stable)} non-boolean stable value(s), "
            f"first={invalid_stable[0]}")
    if invalid_provenance:
        problems.append(
            f"{len(invalid_provenance)} missing-provenance cell(s), "
            f"first={invalid_provenance[0]}")
    if problems:
        raise ValueError(f"incomplete {context}: " + "; ".join(problems))
    return materialized


def validate_native_summary(
    summary: Dict[str, Any],
    *,
    context: str = "native-semantics evidence",
) -> Dict[str, Any]:
    """Require the complete native-probe scope before presentation.

    Capability states must be explicit. Missing Top-N or watermark rows are
    evidence failures, never aliases for unsupported or not applicable.
    """
    problems = []
    expected_engines = set(ENGINE_ORDER)
    expected_domains = set(CONTRACT_DOMAINS)

    if summary.get("kind") != "native_semantics":
        problems.append("kind must be native_semantics")
    if summary.get("probe") != "all":
        problems.append("probe must be all")
    if summary.get("artifact_valid") is not True:
        problems.append("summary artifact_valid must be true")
    if summary.get("pass") is not True:
        problems.append("summary pass must be true")
    for field, expected in (
        ("max_rows", 20_000),
        ("k", 10),
        ("rank_metric", "mean"),
        ("allowed_lateness_s", 300),
    ):
        if summary.get(field) != expected:
            problems.append(f"{field} must equal {expected}")

    engines = summary.get("engines")
    if (
        not isinstance(engines, list)
        or len(engines) != len(expected_engines)
        or any(not isinstance(engine, str) for engine in engines)
        or set(engines) != expected_engines
    ):
        problems.append("engines must contain each of the 7 contract engines exactly once")
    domains = summary.get("domains")
    if (
        not isinstance(domains, list)
        or len(domains) != len(expected_domains)
        or any(not isinstance(domain, str) for domain in domains)
        or set(domains) != expected_domains
    ):
        problems.append("domains must contain each of the 5 contract domains exactly once")

    declared_points = summary.get("trajectory_checkpoints")
    valid_declared_points = (
        isinstance(declared_points, int)
        and not isinstance(declared_points, bool)
        and declared_points == 4
    )
    if not valid_declared_points:
        problems.append("trajectory_checkpoints must equal 4")

    topk = summary.get("topk")
    if not isinstance(topk, list):
        problems.append("topk must be a list")
        topk = []
    watermark = summary.get("watermark")
    if not isinstance(watermark, list):
        problems.append("watermark must be a list")
        watermark = []

    topk_keys = []
    measured_count = 0
    diverging_count = 0
    for index, row in enumerate(topk):
        where = f"topk[{index}]"
        if not isinstance(row, dict):
            problems.append(f"{where} must be an object")
            continue
        engine = row.get("engine")
        domain = row.get("domain")
        if isinstance(engine, str) and isinstance(domain, str):
            topk_keys.append((engine, domain))
        else:
            problems.append(f"{where} engine and domain must be strings")
        if row.get("kind") != "native_topk":
            problems.append(f"{where} kind must be native_topk")
        status = row.get("status")
        if status not in {"measured", "unsupported", "not_expressible"}:
            problems.append(f"{where} has invalid or blocked status {status!r}")
        if row.get("artifact_valid") is not True:
            problems.append(f"{where} artifact_valid must be true")

        points = row.get("checkpoints")
        if status == "measured":
            measured_count += 1
            if row.get("supported") is not True:
                problems.append(f"{where} measured row must be supported")
            cell_pass = row.get("pass")
            if not isinstance(cell_pass, bool):
                problems.append(f"{where} measured pass must be boolean")
            elif not cell_pass:
                diverging_count += 1
            if not isinstance(points, list) or not points:
                problems.append(f"{where} measured checkpoints must be a non-empty list")
                points = []
            if (
                valid_declared_points
                and row.get("trajectory_checkpoints") != declared_points
            ):
                problems.append(
                    f"{where} trajectory_checkpoints must match the summary")
            if valid_declared_points and len(points) != declared_points:
                problems.append(
                    f"{where} must contain {declared_points} checkpoints")

            point_indices = []
            point_rows = []
            point_passes = []
            for point_index, point in enumerate(points):
                point_where = f"{where}.checkpoints[{point_index}]"
                if not isinstance(point, dict):
                    problems.append(f"{point_where} must be an object")
                    continue
                number = point.get("checkpoint")
                if isinstance(number, int) and not isinstance(number, bool):
                    point_indices.append(number)
                else:
                    problems.append(f"{point_where} checkpoint must be an integer")
                rows_seen = point.get("rows")
                if (
                    isinstance(rows_seen, int)
                    and not isinstance(rows_seen, bool)
                    and rows_seen > 0
                ):
                    point_rows.append(rows_seen)
                else:
                    problems.append(f"{point_where} rows must be a positive integer")
                point_pass = point.get("pass")
                if isinstance(point_pass, bool):
                    point_passes.append(point_pass)
                else:
                    problems.append(f"{point_where} pass must be boolean")

                equivalent = []
                for grain in ("hourly", "daily"):
                    block = point.get(grain)
                    grain_pass = (
                        block.get("equivalent") if isinstance(block, dict) else None)
                    if isinstance(grain_pass, bool):
                        equivalent.append(grain_pass)
                    else:
                        problems.append(
                            f"{point_where} {grain}.equivalent must be boolean")
                if (
                    isinstance(point_pass, bool)
                    and len(equivalent) == 2
                    and point_pass != all(equivalent)
                ):
                    problems.append(
                        f"{point_where} pass disagrees with hourly/daily comparisons")

            if valid_declared_points and point_indices != list(
                range(1, declared_points + 1)
            ):
                problems.append(
                    f"{where} checkpoint indices must be 1..{declared_points}")
            if (
                len(point_rows) == len(points)
                and point_rows != sorted(set(point_rows))
            ):
                problems.append(f"{where} checkpoint row counts must strictly increase")
            if (
                isinstance(cell_pass, bool)
                and len(point_passes) == len(points)
                and cell_pass != all(point_passes)
            ):
                problems.append(f"{where} pass disagrees with checkpoint verdicts")
        else:
            if row.get("supported") is not False:
                problems.append(f"{where} capability state must set supported=false")
            if row.get("pass") is not None:
                problems.append(f"{where} capability state must not carry a verdict")
            if points != []:
                problems.append(f"{where} capability state must have no checkpoints")

    expected_topk = {
        (engine, domain)
        for engine in ENGINE_ORDER
        for domain in CONTRACT_DOMAINS
    }
    topk_counts = Counter(topk_keys)
    topk_got = set(topk_counts)
    missing_topk = sorted(expected_topk - topk_got)
    unexpected_topk = sorted(topk_got - expected_topk)
    duplicate_topk = sorted(
        key for key, count in topk_counts.items() if count > 1)
    if missing_topk:
        problems.append(
            f"{len(missing_topk)} missing Top-N cell(s), first={missing_topk[0]}")
    if unexpected_topk:
        problems.append(
            f"{len(unexpected_topk)} unexpected Top-N cell(s), "
            f"first={unexpected_topk[0]}")
    if duplicate_topk:
        problems.append(
            f"{len(duplicate_topk)} duplicate Top-N cell(s), "
            f"first={duplicate_topk[0]}")

    watermark_keys = []
    for index, row in enumerate(watermark):
        where = f"watermark[{index}]"
        if not isinstance(row, dict):
            problems.append(f"{where} must be an object")
            continue
        engine = row.get("engine")
        if isinstance(engine, str):
            watermark_keys.append(engine)
        else:
            problems.append(f"{where} engine must be a string")
        if row.get("kind") != "native_watermark":
            problems.append(f"{where} kind must be native_watermark")
        status = row.get("status")
        if status not in {"measured", "unsupported"}:
            problems.append(f"{where} has invalid or blocked status {status!r}")
        if row.get("artifact_valid") is not True:
            problems.append(f"{where} artifact_valid must be true")
        if status == "measured":
            measured_count += 1
            if row.get("supported") is not True:
                problems.append(f"{where} measured row must be supported")
            passed = row.get("pass")
            dropped = row.get("late_event_dropped")
            retained = row.get("on_time_event_included")
            if not isinstance(passed, bool):
                problems.append(f"{where} measured pass must be boolean")
            elif not passed:
                diverging_count += 1
            if not isinstance(dropped, bool) or not isinstance(retained, bool):
                problems.append(
                    f"{where} measured controls must carry boolean verdicts")
            elif isinstance(passed, bool) and passed != (dropped and retained):
                problems.append(f"{where} pass disagrees with paired controls")
        else:
            if row.get("supported") is not False:
                problems.append(f"{where} unsupported row must set supported=false")
            if row.get("pass") is not None:
                problems.append(f"{where} unsupported row must not carry a verdict")

    watermark_counts = Counter(watermark_keys)
    watermark_got = set(watermark_counts)
    missing_watermark = sorted(expected_engines - watermark_got)
    unexpected_watermark = sorted(watermark_got - expected_engines)
    duplicate_watermark = sorted(
        key for key, count in watermark_counts.items() if count > 1)
    if missing_watermark:
        problems.append(
            f"{len(missing_watermark)} missing watermark cell(s), "
            f"first={missing_watermark[0]}")
    if unexpected_watermark:
        problems.append(
            f"{len(unexpected_watermark)} unexpected watermark cell(s), "
            f"first={unexpected_watermark[0]}")
    if duplicate_watermark:
        problems.append(
            f"{len(duplicate_watermark)} duplicate watermark cell(s), "
            f"first={duplicate_watermark[0]}")

    if summary.get("classified_cells") != 42:
        problems.append("classified_cells must equal 42")
    if summary.get("measured_cells") != measured_count:
        problems.append("measured_cells disagrees with explicit measured rows")
    if summary.get("diverging_cells") != diverging_count:
        problems.append("diverging_cells disagrees with measured row verdicts")
    if measured_count == 0:
        problems.append("at least one native cell must be measured")

    if problems:
        raise ValueError(f"incomplete {context}: " + "; ".join(problems))
    return summary


def validate_oracle_summaries(
    rows: Iterable[Dict[str, Any]],
    *,
    context: str = "oracle-verification evidence",
) -> List[Dict[str, Any]]:
    """Require one equivalent one-million-row oracle result per domain."""
    materialized = list(rows)
    problems = []
    domains = []
    for index, row in enumerate(materialized):
        where = f"oracle[{index}]"
        if not isinstance(row, dict):
            problems.append(f"{where} must be an object")
            continue
        domain = row.get("domain")
        if isinstance(domain, str):
            domains.append(domain)
        else:
            problems.append(f"{where} domain must be a string")
        if row.get("kind") != "oracle_verification":
            problems.append(f"{where} kind must be oracle_verification")
        if row.get("rows") != 1_000_000:
            problems.append(f"{where} rows must equal 1000000")
        for field in ("artifact_valid", "pass", "all_equivalent"):
            if row.get(field) is not True:
                problems.append(f"{where} {field} must be true")
        for field in ("hourly_keys", "daily_keys"):
            value = row.get(field)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value <= 0
            ):
                problems.append(f"{where} {field} must be a positive integer")
        comparisons = row.get("comparisons")
        if not isinstance(comparisons, dict):
            problems.append(f"{where} comparisons must be an object")
            comparisons = {}
        if set(comparisons) != set(ORACLE_COMPARISONS):
            problems.append(
                f"{where} comparisons must contain all six pairwise checks")
        for name in ORACLE_COMPARISONS:
            comparison = comparisons.get(name)
            if not isinstance(comparison, dict):
                continue
            if comparison.get("equivalent") is not True:
                problems.append(f"{where} {name} equivalent must be true")
            if comparison.get("mismatched_keys") != 0:
                problems.append(f"{where} {name} mismatched_keys must equal 0")

    counts = Counter(domains)
    got = set(counts)
    missing = sorted(set(CONTRACT_DOMAINS) - got)
    unexpected = sorted(got - set(CONTRACT_DOMAINS))
    duplicates = sorted(key for key, count in counts.items() if count > 1)
    if missing:
        problems.append(
            f"{len(missing)} missing oracle domain(s), first={missing[0]}")
    if unexpected:
        problems.append(
            f"{len(unexpected)} unexpected oracle domain(s), first={unexpected[0]}")
    if duplicates:
        problems.append(
            f"{len(duplicates)} duplicate oracle domain(s), first={duplicates[0]}")
    if problems:
        raise ValueError(f"incomplete {context}: " + "; ".join(problems))
    return materialized


def validate_boundary_cells(
    rows: Iterable[Dict[str, Any]],
    *,
    context: str = "boundary evidence",
) -> List[Dict[str, Any]]:
    """Require five canonical controls with distinct source-input digests."""
    materialized = list(rows)
    problems = []
    domains = []
    input_hashes = []
    injected_counts: List[Any] = []
    for index, row in enumerate(materialized):
        where = f"boundary[{index}]"
        if not isinstance(row, dict):
            problems.append(f"{where} must be an object")
            continue
        domain = row.get("domain")
        if isinstance(domain, str):
            domains.append(domain)
        else:
            problems.append(f"{where} domain must be a string")
        if row.get("rows") != 5_000:
            problems.append(f"{where} rows must equal 5000")
        input_sha256 = row.get("input_sha256")
        if not (
            isinstance(input_sha256, str)
            and len(input_sha256) == 64
            and all(
                char in "0123456789abcdefABCDEF" for char in input_sha256)
        ):
            problems.append(
                f"{where} input_sha256 must be one 64-hex digest")
        else:
            input_hashes.append(input_sha256.lower())
        # The injected count is configuration, not a constant: it is
        # config/default.yaml perturbation.malformed_rows. Pinning a literal
        # here made STRENGTHENING the control a validator failure. What must
        # hold is that the control is uniform across domains, covers every
        # violation class equally, and rejects every row it injected.
        injected = row.get("injected")
        if (not isinstance(injected, int) or isinstance(injected, bool)
                or injected < 1):
            problems.append(f"{where} injected must be a positive integer")
            injected = None
        classes = row.get("violation_class_count")
        if (not isinstance(classes, int) or isinstance(classes, bool)
                or classes < 1):
            problems.append(
                f"{where} violation_class_count must be a positive integer")
        elif injected is not None and (injected < classes or injected % classes):
            problems.append(
                f"{where} injected={injected} does not cover its {classes} "
                "violation classes equally")
        injected_counts.append(injected)
        for field in ("rejected", "engine_rejected"):
            value = row.get(field)
            if (not isinstance(value, int) or isinstance(value, bool)
                    or injected is None or value != injected):
                problems.append(f"{where} {field} must equal injected")
        value = row.get("engine_accepted_malformed")
        if not isinstance(value, int) or isinstance(value, bool) or value != 0:
            problems.append(f"{where} engine_accepted_malformed must equal 0")
        for field in ("aggregates_unchanged", "pass"):
            if row.get(field) is not True:
                problems.append(f"{where} {field} must be true")

    # One domain injecting fewer rows than the others would let a weak control
    # hide behind four strong ones.
    if len({count for count in injected_counts if count is not None}) > 1:
        problems.append(
            "every boundary domain must inject the same number of rows")

    counts = Counter(domains)
    got = set(counts)
    missing = sorted(set(CONTRACT_DOMAINS) - got)
    unexpected = sorted(got - set(CONTRACT_DOMAINS))
    duplicates = sorted(key for key, count in counts.items() if count > 1)
    if missing:
        problems.append(
            f"{len(missing)} missing boundary domain(s), first={missing[0]}")
    if unexpected:
        problems.append(
            f"{len(unexpected)} unexpected boundary domain(s), first={unexpected[0]}")
    if duplicates:
        problems.append(
            f"{len(duplicates)} duplicate boundary domain(s), first={duplicates[0]}")
    duplicate_hashes = sorted(
        digest for digest, count in Counter(input_hashes).items() if count > 1)
    if duplicate_hashes:
        problems.append(
            "boundary input_sha256 values must be unique across domains")
    if problems:
        raise ValueError(f"incomplete {context}: " + "; ".join(problems))
    return materialized


def validate_d7s_summary(
    summary: Dict[str, Any],
    *,
    context: str = "D7S sweep evidence",
) -> Dict[str, Any]:
    """Require the complete five-domain canonical D7S sensitivity grid."""
    problems = []
    if summary.get("kind") != "d7s_sweep":
        problems.append("kind must be d7s_sweep")
    if summary.get("artifact_valid") is not True:
        problems.append("artifact_valid must be true")
    max_rows = summary.get("max_rows")
    if (
        isinstance(max_rows, bool)
        or not isinstance(max_rows, int)
        or max_rows != 200_000
    ):
        problems.append("max_rows must equal 200000")

    domains = summary.get("domains")
    if (
        not isinstance(domains, list)
        or len(domains) != len(CONTRACT_DOMAINS)
        or any(not isinstance(domain, str) for domain in domains)
        or set(domains) != set(CONTRACT_DOMAINS)
    ):
        problems.append("domains must contain all five contract domains exactly once")
    alphas = summary.get("alphas")
    if (
        not isinstance(alphas, list)
        or len(alphas) != len(D7S_ALPHAS)
        or any(
            isinstance(alpha, bool) or not isinstance(alpha, (int, float))
            for alpha in alphas
        )
        or {float(alpha) for alpha in alphas} != set(D7S_ALPHAS)
    ):
        problems.append("alphas must contain the six canonical values exactly once")
    kll_ks = summary.get("kll_ks")
    if (
        not isinstance(kll_ks, list)
        or len(kll_ks) != len(D7S_KLL_KS)
        or any(
            isinstance(kll_k, bool) or not isinstance(kll_k, int)
            for kll_k in kll_ks
        )
        or set(kll_ks) != set(D7S_KLL_KS)
    ):
        problems.append("kll_ks must contain 64, 200, and 800 exactly once")

    rows = summary.get("rows")
    if not isinstance(rows, list):
        problems.append("rows must be a list")
        rows = []
    keys = []
    separating_by_domain = {domain: 0 for domain in CONTRACT_DOMAINS}
    separating_alphas = []
    for index, row in enumerate(rows):
        where = f"d7s[{index}]"
        if not isinstance(row, dict):
            problems.append(f"{where} must be an object")
            continue
        domain = row.get("domain")
        alpha = row.get("alpha")
        kll_k = row.get("kll_k")
        if (
            isinstance(domain, str)
            and isinstance(alpha, (int, float))
            and not isinstance(alpha, bool)
            and isinstance(kll_k, int)
            and not isinstance(kll_k, bool)
        ):
            keys.append((domain, float(alpha), kll_k))
        else:
            problems.append(f"{where} has invalid domain/alpha/kll_k key")
        for field in ("rows", "restatements", "groups_scored"):
            value = row.get(field)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value <= 0
            ):
                problems.append(f"{where} {field} must be a positive integer")
        row_count = row.get("rows")
        restatements = row.get("restatements")
        if (
            isinstance(row_count, int)
            and not isinstance(row_count, bool)
            and isinstance(restatements, int)
            and not isinstance(restatements, bool)
            and row_count != max_rows + restatements
        ):
            problems.append(
                f"{where} rows must equal max_rows + restatements")
        for field in ("io_qer", "bd_qer"):
            value = row.get(field)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or not 0.0 <= float(value) <= 1.0
            ):
                problems.append(f"{where} {field} must be a finite rate in [0,1]")
        violations = {}
        for field in ("io_violations", "bd_violations"):
            value = row.get(field)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
            ):
                problems.append(f"{where} {field} must be a nonnegative integer")
            else:
                violations[field] = value
        separates = row.get("separates")
        if not isinstance(separates, bool):
            problems.append(f"{where} separates must be boolean")
        elif len(violations) == 2:
            expected_separates = (
                violations["io_violations"] > 0
                and violations["bd_violations"] == 0
            )
            if separates != expected_separates:
                problems.append(f"{where} separates disagrees with violations")
            if (
                separates
                and domain in separating_by_domain
                and isinstance(alpha, (int, float))
                and not isinstance(alpha, bool)
            ):
                separating_by_domain[domain] += 1
                separating_alphas.append(float(alpha))

    expected = {
        (domain, alpha, kll_k)
        for domain in CONTRACT_DOMAINS
        for alpha in D7S_ALPHAS
        for kll_k in D7S_KLL_KS
    }
    counts = Counter(keys)
    got = set(counts)
    missing = sorted(expected - got)
    unexpected = sorted(got - expected)
    duplicates = sorted(key for key, count in counts.items() if count > 1)
    if missing:
        problems.append(f"{len(missing)} missing D7S cell(s), first={missing[0]}")
    if unexpected:
        problems.append(
            f"{len(unexpected)} unexpected D7S cell(s), first={unexpected[0]}")
    if duplicates:
        problems.append(
            f"{len(duplicates)} duplicate D7S cell(s), first={duplicates[0]}")

    separating_count = sum(separating_by_domain.values())
    if summary.get("cells") != len(expected):
        problems.append(f"cells must equal {len(expected)}")
    if summary.get("separating_cells") != separating_count:
        problems.append("separating_cells disagrees with explicit rows")
    if summary.get("separating_by_domain") != separating_by_domain:
        problems.append("separating_by_domain disagrees with explicit rows")
    expected_min = min(separating_alphas, default=None)
    if summary.get("min_separating_alpha") != expected_min:
        problems.append("min_separating_alpha disagrees with explicit rows")
    if summary.get("pass") is not bool(separating_count):
        problems.append("pass disagrees with whether the grid separates")
    if problems:
        raise ValueError(f"incomplete {context}: " + "; ".join(problems))
    return summary


def validate_mutation_summary(
    summary: Dict[str, Any],
    *,
    context: str = "mutation evidence",
) -> Dict[str, Any]:
    """Require the complete 5 x 7 x 7 diagnostic-mutation control grid."""
    problems = []
    expected_mutants = set(MUTATION_TARGETS)
    if summary.get("kind") != "detection_power":
        problems.append("kind must be detection_power")
    if summary.get("artifact_valid") is not True:
        problems.append("artifact_valid must be true")
    if summary.get("max_rows") != 50_000:
        problems.append("max_rows must equal 50000")
    epsilon = summary.get("epsilon")
    if (
        isinstance(epsilon, bool)
        or not isinstance(epsilon, (int, float))
        or not math.isclose(float(epsilon), 1e-9, rel_tol=0.0, abs_tol=0.0)
    ):
        problems.append("epsilon must equal 1e-9")

    for field, values, expected in (
        ("domains", summary.get("domains"), set(CONTRACT_DOMAINS)),
        ("mutants", summary.get("mutants"), expected_mutants),
        ("diagnostics", summary.get("diagnostics"), set(MUTATION_DIAGNOSTICS)),
    ):
        if (
            not isinstance(values, list)
            or len(values) != len(expected)
            or any(not isinstance(value, str) for value in values)
            or set(values) != expected
        ):
            problems.append(f"{field} must contain its canonical scope exactly once")

    rows = summary.get("rows")
    if not isinstance(rows, list):
        problems.append("rows must be a list")
        rows = []
    keys = []
    diagonal = []
    caught = []
    off_target = []
    collateral = []
    for index, row in enumerate(rows):
        where = f"mutation[{index}]"
        if not isinstance(row, dict):
            problems.append(f"{where} must be an object")
            continue
        domain = row.get("domain")
        mutant = row.get("mutant")
        diagnostic = row.get("diagnostic")
        if all(isinstance(value, str) for value in (domain, mutant, diagnostic)):
            keys.append((domain, mutant, diagnostic))
        else:
            problems.append(f"{where} domain/mutant/diagnostic must be strings")
        target = MUTATION_TARGETS.get(mutant)
        if target is not None:
            condition, target_diagnostic = target
            if row.get("condition") != condition:
                problems.append(f"{where} condition disagrees with mutant")
            if row.get("target_diagnostic") != target_diagnostic:
                problems.append(
                    f"{where} target_diagnostic disagrees with mutant")
            if row.get("is_target") is not (diagnostic == target_diagnostic):
                problems.append(f"{where} is_target disagrees with diagnostic")
        if not isinstance(row.get("is_target"), bool):
            problems.append(f"{where} is_target must be boolean")
        if not isinstance(row.get("detected"), bool):
            problems.append(f"{where} detected must be boolean")
        if not isinstance(row.get("vacuous"), bool):
            problems.append(f"{where} vacuous must be boolean")
        if row.get("error"):
            problems.append(f"{where} contains an execution error")

        if row.get("is_target") is True and row.get("vacuous") is False:
            diagonal.append(row)
            if row.get("detected") is True:
                caught.append(row)
        elif row.get("is_target") is False and row.get("vacuous") is False:
            off_target.append(row)
            if row.get("detected") is True:
                collateral.append(row)

    expected = {
        (domain, mutant, diagnostic)
        for domain in CONTRACT_DOMAINS
        for mutant in MUTATION_TARGETS
        for diagnostic in MUTATION_DIAGNOSTICS
    }
    counts = Counter(keys)
    got = set(counts)
    missing = sorted(expected - got)
    unexpected = sorted(got - expected)
    duplicates = sorted(key for key, count in counts.items() if count > 1)
    if missing:
        problems.append(
            f"{len(missing)} missing mutation cell(s), first={missing[0]}")
    if unexpected:
        problems.append(
            f"{len(unexpected)} unexpected mutation cell(s), first={unexpected[0]}")
    if duplicates:
        problems.append(
            f"{len(duplicates)} duplicate mutation cell(s), first={duplicates[0]}")

    if summary.get("cells") != len(expected):
        problems.append(f"cells must equal {len(expected)}")
    if summary.get("diagonal_total") != len(diagonal):
        problems.append("diagonal_total disagrees with explicit rows")
    if summary.get("diagonal_caught") != len(caught):
        problems.append("diagonal_caught disagrees with explicit rows")
    expected_pass = bool(diagonal) and len(caught) == len(diagonal)
    if summary.get("pass") is not expected_pass:
        problems.append("pass disagrees with diagonal mutation outcomes")
    expected_sensitivity = len(caught) / len(diagonal) if diagonal else 0.0
    expected_specificity = (
        1.0 - len(collateral) / len(off_target) if off_target else 1.0)
    for field, expected_value in (
        ("sensitivity", expected_sensitivity),
        ("specificity", expected_specificity),
    ):
        value = summary.get(field)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isclose(
                float(value), expected_value, rel_tol=1e-12, abs_tol=1e-12)
        ):
            problems.append(f"{field} disagrees with explicit rows")
    if problems:
        raise ValueError(f"incomplete {context}: " + "; ".join(problems))
    return summary


def sort_engines(names: Iterable[str]) -> List[str]:
    order = {e: i for i, e in enumerate(ENGINE_ORDER)}
    return sorted(set(names), key=lambda e: order.get(e, len(ENGINE_ORDER)))


def sort_diagnostics(names: Iterable[str]) -> List[str]:
    order = {d: i for i, d in enumerate(DIAGNOSTIC_ORDER)}
    return sorted(set(names), key=lambda d: order.get(d, len(DIAGNOSTIC_ORDER)))


def matrix_diagnostics(names: Iterable[str]) -> List[str]:
    """Per-engine diagnostics for the design-space matrix, EXCLUDING
    engine-independent ones (D7S -> Tab 10).

    Ordered by claim level first (portable, then lifecycle), canonical order
    within a level, so Fig 2, Tab 6 and Tab 7 all show contiguous level bands
    and no reader reads a portable verdict and a lifecycle one as one score.
    """
    keep = [d for d in names
            if (d in DIAGNOSTIC_ORDER or d.startswith("D"))
            and d not in ENGINE_INDEPENDENT_DIAGNOSTICS]
    level = {lv: i for i, lv in enumerate(LEVEL_ORDER)}
    canonical = {d: i for i, d in enumerate(DIAGNOSTIC_ORDER)}
    return sorted(
        set(keep),
        key=lambda d: (level.get(DIAGNOSTIC_LEVEL.get(d, "portable"), len(LEVEL_ORDER)),
                       canonical.get(d, len(DIAGNOSTIC_ORDER)),
                       d),
    )


def level_bands(diag_list: Iterable[str]) -> List[Tuple[str, int]]:
    """Consecutive (claim level, column count) bands over an ordered diagnostic
    list, for the group header of the design-space matrix."""
    bands: List[Tuple[str, int]] = []
    for diag in diag_list:
        lv = DIAGNOSTIC_LEVEL.get(diag, "portable")
        if bands and bands[-1][0] == lv:
            bands[-1] = (lv, bands[-1][1] + 1)
        else:
            bands.append((lv, 1))
    return bands
