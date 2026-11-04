#!/usr/bin/env python3
"""Show progress for one fixed benchmark run.

Every count is taken against the exact cell universe of its phase, built from
the same constants the collectors validate against (scripts/figures/_io.py):

  correctness   engines x domains x D1-D7 x 50,000 rows x repeats 1-3
  confirmation  engines x domains x D1-D7 x 1,000,000 rows x repeat 1
  scaling       engines x domains x D1-D4,D6,D7 x five sizes x repeats 1-3

A cell is counted once, by its newest run, so a blocked attempt that a retry
replaced does not count twice. For each phase, complete + blocked + missing
equals the expected total, and the outcome counts add up to complete.

Summaries outside a phase universe are not cells and never enter a total;
collection summaries, for example, carry no engine. Any such summary inside a
phase is named in a warning rather than counted.
"""
from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNS = ROOT / "runs"

import sys  # noqa: E402

sys.path.insert(0, str(ROOT / "scripts"))
from figures import load_module  # noqa: E402

_io = load_module("_io")

ENGINES = list(_io.ENGINE_ORDER)
DOMAINS = sorted(_io.CONTRACT_DOMAINS)
DIAGNOSTICS = sorted(_io.CONTRACT_DIAGNOSTICS)
SCALING_DIAGNOSTICS = sorted(_io.SCALING_DIAGNOSTICS)
SCALING_SIZES = sorted(_io.SCALING_SIZES)

LABELS = {"grid50k": "correctness", "confirm1m": "confirmation",
          "scaling": "scaling"}
LEGACY_PHASES = {"ladder": "scaling"}
OUTCOME_ORDER = ["equivalent", "divergence", "known_divergence", "unsupported"]


def _excluded(primary: str, fallback: str, default: str) -> set[str]:
    """Engines a phase declares out of scope, as reproduce_all.sh sets them."""
    value = os.environ.get(primary)
    if value is None:
        value = os.environ.get(fallback, default)
    return {name for name in value.replace(",", " ").split() if name}


def phase_universes() -> dict:
    confirm_excluded = _excluded(
        "BENCHMARK_EXCLUDE_CONFIRM", "EXCLUDED_CONFIRM_ENGINES", "")
    scaling_excluded = _excluded(
        "BENCHMARK_EXCLUDE_SCALING", "EXCLUDED_SCALING_ENGINES", "")
    return {
        "grid50k": {
            "engines": ENGINES, "excluded": [],
            "diagnostics": DIAGNOSTICS, "rows": [50_000], "repeats": [1, 2, 3],
        },
        "confirm1m": {
            "engines": [e for e in ENGINES if e not in confirm_excluded],
            "excluded": [e for e in ENGINES if e in confirm_excluded],
            "diagnostics": DIAGNOSTICS, "rows": [1_000_000], "repeats": [1],
        },
        "scaling": {
            "engines": [e for e in ENGINES if e not in scaling_excluded],
            "excluded": [e for e in ENGINES if e in scaling_excluded],
            "diagnostics": SCALING_DIAGNOSTICS, "rows": SCALING_SIZES,
            "repeats": [1, 2, 3],
        },
    }


def expected_keys(universe: dict, engines: list[str], diagnostics: list[str]):
    return {
        (engine, domain, diagnostic, rows, repeat)
        for engine in engines
        for domain in DOMAINS
        for diagnostic in diagnostics
        for rows in universe["rows"]
        for repeat in universe["repeats"]
    }


def canonical_phase(phase: object) -> object:
    return LEGACY_PHASES.get(phase, phase)


def outcome(summary: dict) -> str:
    if summary.get("error") or summary.get("artifact_valid") is not True:
        return "blocked"
    primary = summary.get("primary") or {}
    if summary.get("supported") is False or primary.get("supported") is False:
        return "unsupported"
    if summary.get("pass") is True:
        return "equivalent"
    if primary.get("known_finding") or summary.get("known_finding"):
        return "known_divergence"
    return "divergence"


def _cell_key(summary: dict):
    rows = summary.get("requested_max_rows", summary.get("rows"))
    repeat = summary.get("repeat_index", 0)
    if not isinstance(rows, int) or not isinstance(repeat, int):
        return None
    return (summary.get("engine"), summary.get("domain"),
            summary.get("diagnostic"), rows, repeat)


def latest_cells(run_id: str) -> dict:
    """Newest summary per (phase, cell); the highest run id is the live one."""
    latest: dict = {}
    for path in RUNS.glob("*/summary.json"):
        try:
            summary = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if not isinstance(summary, dict) or summary.get("campaign_id") != run_id:
            continue
        key = _cell_key(summary)
        if key is None:
            continue
        phase = canonical_phase(summary.get("campaign_phase"))
        run = path.parent.name
        slot = latest.setdefault(phase, {})
        if key not in slot or run > slot[key][0]:
            slot[key] = (run, summary)
    return latest


def _tally(cells: dict, expected: set) -> tuple[Counter, Counter]:
    """Outcome counts over expected cells, and per-engine complete counts."""
    outcomes: Counter = Counter()
    complete_by_engine: Counter = Counter()
    for key in expected:
        item = cells.get(key)
        if item is None:
            outcomes["missing"] += 1
            continue
        state = outcome(item[1])
        outcomes[state] += 1
        if state != "blocked":
            complete_by_engine[key[0]] += 1
    return outcomes, complete_by_engine


def _pct(part: int, whole: int) -> str:
    return f"{100.0 * part / whole:5.1f}%" if whole else "  n/a"


def print_phase(phase: str, universe: dict, cells: dict) -> None:
    expected = expected_keys(universe, universe["engines"], universe["diagnostics"])
    outcomes, by_engine = _tally(cells, expected)
    total = len(expected)
    blocked, missing = outcomes["blocked"], outcomes["missing"]
    complete = total - blocked - missing
    details = "  ".join(
        f"{name}={outcomes[name]}" for name in OUTCOME_ORDER if outcomes[name])
    tail = f"blocked={blocked}  missing={missing}"
    print(f"{LABELS[phase]:12s} {complete:4d}/{total} ({_pct(complete, total)})  "
          f"{details + '  |  ' if details else ''}{tail}")
    per_engine = total // len(universe["engines"]) if universe["engines"] else 0
    print("  " + "  ".join(
        f"{engine}={by_engine[engine]}/{per_engine}"
        for engine in universe["engines"]))
    if universe["excluded"]:
        print(f"  declared out of scope: {', '.join(universe['excluded'])}")

    stray = [key for key in cells if key not in expected]
    if stray:
        print(f"  WARNING: {len(stray)} cell summary(ies) outside this phase's "
              f"grid, not counted; first: {sorted(stray, key=str)[0]}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    latest = latest_cells(args.run_id)
    print(f"benchmark run: {args.run_id}")
    run_dir = RUNS / "benchmark_runs" / args.run_id
    gates = []
    for label, name in (("data", "prepare"), ("engine reset", "reset-engines"),
                        ("adapters", "smoke"), ("validation", "validation")):
        marker = run_dir / f"{name}.complete"
        state = "done" if marker.is_file() and marker.read_text().strip() else "pending"
        gates.append(f"{label}={state}")
    print("checks: " + "  ".join(gates))
    for phase, universe in phase_universes().items():
        print_phase(phase, universe, latest.get(phase, {}))
    cache_root = ROOT / "data" / "oracle_cache"
    cache_count = sum(1 for _ in cache_root.glob("*/metadata.json"))
    print(f"oracle caches: {cache_count} completed expected-table artifacts")
    lock_path = ROOT / "results" / "official_results.json"
    if lock_path.is_file():
        try:
            lock = json.loads(lock_path.read_text())
            official_id = lock.get("benchmark_run_id")
            mode = "official source run" if args.run_id == official_id else "reproduction run"
            print(f"locked evidence: {official_id} ({mode})")
        except (OSError, ValueError):
            print("locked evidence: invalid official result lock")
    report_path = run_dir / "reproduction_report.json"
    if report_path.is_file():
        try:
            report = json.loads(report_path.read_text())
            print(f"reproduction comparison: "
                  f"{'PASS' if report.get('pass') is True else 'FAIL'}")
        except (OSError, ValueError):
            print("reproduction comparison: invalid report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
