#!/usr/bin/env python3
"""
run_detection_power.py -- does the diagnostic suite actually detect the defects it
claims to detect, and does it stay quiet otherwise?

WHY THIS EXISTS
---------------
The design-space map reports which engines violate the contract. That result is
only as strong as the diagnostics producing it: a suite that never fires would
report "all pass", and a suite that fires at everything would report "all fail".
Neither could be distinguished from the real matrix without a control.

This is that control, and it is the benchmark analogue of mutation testing. Each
mutant (src/adapters/mutants.py) wraps the reference engine and injects exactly
one violation of one correctness condition C1--C7. The suite is then run against
every mutant on every domain, and two quantities are reported:

  sensitivity  the diagnostic that targets the injected condition FAILS
               (the defect was caught) -- the diagonal of the matrix
  specificity  every other diagnostic still PASSES
               (no false alarm) -- the off-diagonal

Off-diagonal detections are not automatically bugs: D2 and D4 read the same
deduplicated view as D1, so a dedup defect is genuinely visible to them. The
report therefore lists collateral detections explicitly instead of scoring them
as failures, and the paper should quote the diagonal as sensitivity and the
collateral list as the suite's coupling structure.

D5's mutation control simulates the restart boundary in-process; the production
D5 runner still performs a real container SIGKILL. D6 mutates only the derived
top-k surface, keeping the underlying aggregate unchanged.

Inputs  : data/normalized/<domain>/log.parquet ; data/fixtures/<domain>/*.parquet
Outputs : runs/<UTC>_<domain>_mutants_detection/{summary.json,detection.csv,log.txt}
Runtime : ~1-3 min per domain at --max-rows 50000 (in-process, no containers)
Run as  : python scripts/run/run_detection_power.py --domain all
          python scripts/run/run_detection_power.py --domain environment_airquality \
              --max-rows 50000
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import subprocess
import sys
import traceback
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.adapters import MUTANT_NAMES  # noqa: E402
from src.adapters.mutants import MUTANT_TARGETS  # noqa: E402
from src.schema.benchmark import (  # noqa: E402
    effective_perturbation, load_yaml, row_positions,
)
from src.provenance import run_provenance  # noqa: E402


def _load_runner():
    """Import scripts/run/run_diagnostic.py so mutants go through the EXACT
    comparison path the engines do -- a bespoke checker here would prove nothing
    about the suite that actually produces the paper's matrix."""
    path = ROOT / "scripts" / "run" / "run_diagnostic.py"
    spec = importlib.util.spec_from_file_location("run_diagnostic", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    # @dataclass resolves annotations through sys.modules, so the module has to be
    # registered before it executes.
    sys.modules["run_diagnostic"] = module
    spec.loader.exec_module(module)
    return module


DOMAINS = [
    "environment_airquality",
    "transportation_taxi",
    "datacenter_drives",
    "finance_markets",
    "sports_football",
]

#: One isolated mutant exists for every condition, including D5 and D6.
DIAGNOSTICS = ["D1", "D2", "D3", "D4", "D5", "D6", "D7"]

CSV_FIELDS = ["domain", "mutant", "condition", "target_diagnostic", "diagnostic",
              "is_target", "detected", "seeded_events", "vacuous", "metric",
              "value", "error"]


def _git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


#: What each mutant needs to be present in the prefix it is actually given, and
#: how to count it. A mutant can only be caught if the fixture prefix contains the
#: perturbation it corrupts: mutant_drop_late drops records behind the watermark,
#: so on a prefix with no out-of-order records it is a no-op and the reference
#: engine's own output comes back unchanged. Scoring that as a MISS would blame
#: the diagnostic for a fixture that was never seeded -- the harness must be able
#: to tell "the suite failed to detect" from "there was nothing to detect".
VACUITY_CHECKS: Dict[str, Dict[str, str]] = {
    "mutant_no_dedup": {"fixture": "d1_dup.parquet", "needs": "duplicate rows"},
    "mutant_order_sensitive": {"fixture": "d2_reorder.parquet",
                               "needs": "event-time inversions"},
    "mutant_drop_late": {"fixture": "d4_late.parquet",
                         "needs": "injected records beyond the domain allowance"},
    "mutant_ignore_supersedes": {"fixture": "d7_restate.parquet",
                                 "needs": "restatements"},
    # mutant_ulp_drift perturbs the reduction itself on the base log, so there is
    # no seeded fixture feature it can be starved of.
}


def _manifest_injected_sha(domain: str) -> Optional[str]:
    """The D4 injected-target digest recorded when the fixture was built."""
    path = ROOT / "data" / "fixtures" / domain / "manifest.json"
    if not path.is_file():
        return None
    for entry in json.loads(path.read_text()).get("fixtures", []):
        if entry.get("name") == "d4_late.parquet":
            return entry.get("injected_target_ids_sha256")
    return None


def _check_injected_targets(domain: str, injected_ids: set) -> None:
    """Fail loudly when this re-derivation disagrees with the built fixture.

    The draw is reproduced here rather than read from the fixture, so any future
    change to how build_fixtures selects targets silently turns this check into a
    lookup for ids that were never seeded -- which reports a correctly seeded
    fixture as vacuous. Comparing against the digest the builder recorded turns
    that silent drift into an error.
    """
    expected = _manifest_injected_sha(domain)
    if expected is None:
        return
    actual = hashlib.sha256(
        "\n".join(sorted(injected_ids)).encode("utf-8")).hexdigest()
    if actual != expected:
        raise RuntimeError(
            f"{domain}: re-derived D4 injected-target set does not match the "
            f"fixture manifest ({actual[:12]} != {expected[:12]}). The vacuity "
            "check and scripts/perturb/build_fixtures.py have drifted apart; "
            "reconcile them before trusting any D4 detection result.")


@lru_cache(maxsize=None)
def _seeded_count(mutant: str, domain: str, max_rows: int) -> Optional[int]:
    """How many seeded events the mutant can actually act on in this prefix."""
    check = VACUITY_CHECKS.get(mutant)
    if check is None:
        return None
    path = ROOT / "data" / "fixtures" / domain / check["fixture"]
    if not path.is_file():
        return None
    df = pd.read_parquet(path)
    selection_mode = "uniform" if mutant == "mutant_drop_late" else "prefix"
    df = df.iloc[row_positions(len(df), max_rows, selection_mode)].copy()
    if mutant == "mutant_no_dedup":
        return int(df["dedupe_id"].duplicated().sum())
    if mutant == "mutant_order_sensitive":
        times = pd.to_datetime(df["event_time_utc"], utc=True)
        return int((times < times.cummax()).sum())
    if mutant == "mutant_drop_late":
        config = load_yaml(ROOT / "config" / "default.yaml")
        perturbation = effective_perturbation(ROOT, domain)
        allowed_ns = int(perturbation["allowed_lateness_s"]) * 1_000_000_000
        # The fixture draws its targets from the D4 *control* -- the base log in
        # event-time order with the contract's dedupe-id tie break -- not from
        # storage order (build_fixtures._d4_control). Re-deriving the draw from
        # the unsorted log picks a disjoint set of ids: the two sets overlapped
        # by ~0.1%, so this check saw ~4 of the 2000 seeded rows and reported
        # three domains vacuous for fixtures the build-time guard had already
        # verified as correctly seeded.
        base = pd.read_parquet(
            ROOT / "data" / "normalized" / domain / "log.parquet",
            columns=["event_time_utc", "dedupe_id"],
        ).sort_values(["event_time_utc", "dedupe_id"], kind="mergesort")
        k = int(round(len(base) * float(perturbation["late_rate"])))
        rng = np.random.Generator(np.random.PCG64(
            np.random.SeedSequence([int(config["seed"]), 4])))
        positions = np.sort(rng.choice(len(base), size=k, replace=False))
        injected_ids = set(base.iloc[positions]["dedupe_id"].astype(str))
        _check_injected_targets(domain, injected_ids)
        times = (pd.to_datetime(df["event_time_utc"], utc=True)
                 .astype("int64").to_numpy())
        if len(times) < 2:
            return 0
        prior_max = np.maximum.accumulate(times)
        beyond = np.zeros(len(df), dtype=bool)
        beyond[1:] = times[1:] < prior_max[:-1] - allowed_ns
        selected = df["dedupe_id"].astype(str).isin(injected_ids).to_numpy()
        return int((beyond & selected).sum())
    if mutant == "mutant_ignore_supersedes":
        for col in ("supersedes", "supersedes_id"):
            if col in df.columns:
                return int(df[col].notna().sum())
        return None
    return None


def _metric_value(summary: Dict[str, Any]) -> Any:
    if summary.get("metric") == "DDR":
        return summary.get("ddr")
    primary = summary.get("primary") or {}
    hourly = primary.get("hourly") or {}
    return hourly.get("emr")



def _run_d5_mutation_cell(runner, mutant: str, domain: str, max_rows: int,
                          epsilon: float) -> Dict[str, Any]:
    """Exercise D5's comparison path with an in-process restart boundary."""
    path = ROOT / "data" / "normalized" / domain / "log.parquet"
    df = runner._load(path, max_rows)
    cut = max(1, len(df) // 2)
    h1 = df.iloc[:cut].copy()
    h1.attrs["_equitable_input_key"] = (
        "mutation_d5", str(path.resolve()), int(max_rows), int(cut))
    adapter = runner.get_adapter(mutant)
    with adapter.connect() as eng:
        eng.setup(variant="dedup")
        eng.ingest_log(h1)
        if hasattr(eng, "simulate_restart"):
            eng.simulate_restart()
        rh = eng.read_view("hourly")
        rd = eng.read_view("daily")
    oracle, _, _ = runner._build_oracle(h1, restatement_aware=False, co_oracle=False)
    ch = runner.compare_bucket_tables(rh, oracle["hourly"], epsilon=epsilon)
    cd = runner.compare_bucket_tables(rd, oracle["daily"], epsilon=epsilon)
    passed = bool(ch["equivalent"] and cd["equivalent"])
    value = max(float(ch["emr"]), float(cd["emr"]))
    return {
        "detected": not passed,
        "metric": "EMR",
        "value": value,
        "error": None,
    }


def _run_cell(runner, mutant: str, domain: str, diag: str, max_rows: int,
              epsilon: float) -> Dict[str, Any]:
    try:
        if diag == "D5":
            return _run_d5_mutation_cell(runner, mutant, domain, max_rows, epsilon)
        summary = runner.run_one(mutant, domain, diag, max_rows, epsilon,
                                 co_oracle=False, d3_ledger=False)
    except Exception as exc:  # a mutant that crashes is a harness fault, not a detection
        return {"detected": None, "metric": None, "value": None,
                "error": f"{type(exc).__name__}: {exc}",
                "traceback": traceback.format_exc()[-800:]}
    return {"detected": not bool(summary.get("pass")),
            "metric": summary.get("metric"),
            "value": _metric_value(summary), "error": None}

def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--domain", default="all", help="Domain name or 'all'.")
    ap.add_argument("--mutant", default="all",
                    help=f"Mutant name or 'all'. Known: {', '.join(MUTANT_NAMES)}")
    ap.add_argument("--max-rows", type=int, default=50_000,
                    help="Rows per diagnostic (default 50000).")
    ap.add_argument("--epsilon", type=float, default=1e-9,
                    help="Comparator epsilon (default 1e-9, the contract value).")
    args = ap.parse_args()

    domains = DOMAINS if args.domain == "all" else [args.domain]
    mutants = list(MUTANT_NAMES) if args.mutant == "all" else [args.mutant]
    unknown_d = [d for d in domains if d not in DOMAINS]
    unknown_m = [m for m in mutants if m not in MUTANT_NAMES]
    if unknown_d:
        print(f"FAIL: unknown domain(s) {unknown_d}; known: {DOMAINS}")
        return 1
    if unknown_m:
        print(f"FAIL: unknown mutant(s) {unknown_m}; known: {list(MUTANT_NAMES)}")
        return 1

    runner = _load_runner()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    tag = args.domain if args.domain != "all" else "all"
    out_dir = ROOT / "runs" / f"{stamp}_{tag}_mutants_detection"
    out_dir.mkdir(parents=True, exist_ok=True)

    log: List[str] = []
    rows: List[Dict[str, Any]] = []
    for domain in domains:
        header = f"==== detection power : {domain} (rows<={args.max_rows}) ===="
        log.append(header)
        print(header, flush=True)
        for mutant in mutants:
            target = MUTANT_TARGETS[mutant]
            seeded = _seeded_count(mutant, domain, args.max_rows)
            vacuous = seeded == 0
            if vacuous:
                note = (f"  {mutant:<26} SKIPPED: prefix of {args.max_rows} rows has "
                        f"0 {VACUITY_CHECKS[mutant]['needs']} -- nothing to detect")
                log.append(note)
                print(note, flush=True)
            for diag in DIAGNOSTICS:
                res = _run_cell(runner, mutant, domain, diag, args.max_rows,
                                args.epsilon)
                is_target = diag == target["diagnostic"]
                row = {
                    "domain": domain, "mutant": mutant,
                    "condition": target["condition"],
                    "target_diagnostic": target["diagnostic"],
                    "diagnostic": diag, "is_target": is_target,
                    "detected": res["detected"], "metric": res["metric"],
                    "value": res["value"], "error": res["error"],
                    "seeded_events": seeded, "vacuous": vacuous,
                }
                rows.append(row)
                if is_target and vacuous:
                    mark = "VACUOUS (no seeded defect in prefix)"
                elif is_target:
                    mark = "CAUGHT" if res["detected"] else "MISSED"
                else:
                    mark = "collateral" if res["detected"] else "quiet"
                if res["error"]:
                    mark = f"ERROR {res['error'][:60]}"
                line = (f"  {mutant:<26} {diag:<4} "
                        f"{'<-target' if is_target else '        '} {mark}")
                log.append(line)
                print(line, flush=True)

    # Diagonal: one (mutant, domain) pair per targeted diagnostic. Vacuous trials
    # are invalid experiments, not negative results, so they leave the denominator.
    diagonal_all = [r for r in rows if r["is_target"]]
    vacuous = [r for r in diagonal_all if r["vacuous"]]
    diagonal = [r for r in diagonal_all if not r["vacuous"]]
    caught = [r for r in diagonal if r["detected"] is True]
    missed = [r for r in diagonal if r["detected"] is False]
    errored = [r for r in rows if r["error"]]
    off = [r for r in rows if not r["is_target"] and not r["vacuous"]]
    collateral = [r for r in off if r["detected"] is True]

    sensitivity = len(caught) / len(diagonal) if diagonal else 0.0
    specificity = (1 - len(collateral) / len(off)) if off else 1.0

    by_mutant: Dict[str, Dict[str, Any]] = {}
    for m in mutants:
        d = [r for r in diagonal if r["mutant"] == m]
        c = [r for r in collateral if r["mutant"] == m]
        by_mutant[m] = {
            "condition": MUTANT_TARGETS[m]["condition"],
            "target_diagnostic": MUTANT_TARGETS[m]["diagnostic"],
            "defect": MUTANT_TARGETS[m]["defect"],
            "domains_caught": sum(1 for r in d if r["detected"] is True),
            "domains_run": len(d),
            "domains_vacuous": sum(1 for r in vacuous if r["mutant"] == m),
            "collateral_detections": sorted({r["diagnostic"] for r in c}),
        }

    summary = {
        "kind": "detection_power",
        **run_provenance(ROOT),
        "started_utc": stamp,
        "domains": domains,
        "mutants": mutants,
        "diagnostics": DIAGNOSTICS,
        "max_rows": args.max_rows,
        "epsilon": args.epsilon,
        "cells": len(rows),
        "sensitivity": sensitivity,
        "specificity": specificity,
        "diagonal_caught": len(caught),
        "diagonal_total": len(diagonal),
        "missed": [f"{r['mutant']}/{r['domain']}/{r['diagnostic']}" for r in missed],
        "vacuous": [f"{r['mutant']}/{r['domain']} (0 seeded events in prefix)"
                    for r in vacuous],
        "vacuous_note": (
            "excluded from sensitivity: the fixture prefix at this --max-rows "
            "contains none of the perturbation the mutant corrupts, so the trial "
            "cannot distinguish a working diagnostic from a broken one"),
        "collateral": sorted({f"{r['mutant']}->{r['diagnostic']}" for r in collateral}),
        "errors": [f"{r['mutant']}/{r['domain']}/{r['diagnostic']}: {r['error']}"
                   for r in errored],
        "by_mutant": by_mutant,
        "scope_note": (
            "D5 mutation simulates state loss at the restart boundary; the production D5 runner uses SIGKILL. "
            "D6 mutation is isolated to the derived top-k output."),
        "rows": rows,
        # The control passes when every injected defect is caught by its own
        # diagnostic. Collateral detections are reported, not penalised.
        "pass": bool(diagonal) and not missed and not errored,
        "artifact_valid": bool(diagonal) and not errored,
    }
    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n")
    with (out_dir / "detection.csv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        w.writeheader()
        for row in rows:
            w.writerow({k: row[k] for k in CSV_FIELDS})
    (out_dir / "log.txt").write_text("\n".join(log) + "\n")

    verdict = "PASS" if summary["pass"] else "FAIL"
    if not diagonal:
        print(f"\n{verdict}: no valid trials -- every diagonal cell was vacuous at "
              f"--max-rows {args.max_rows}. This measures nothing; re-run at a scale "
              f"where the fixtures carry their seeded defects.")
    elif missed:
        print(f"\n{verdict}: sensitivity {len(caught)}/{len(diagonal)} "
              f"({sensitivity:.0%}); MISSED {summary['missed']}")
    else:
        print(f"\n{verdict}: sensitivity {len(caught)}/{len(diagonal)} "
              f"({sensitivity:.0%}) -- every seeded defect caught by its own diagnostic")
    print(f"specificity {specificity:.0%} "
          f"({len(collateral)}/{len(off)} off-target detections)")
    if vacuous:
        print(f"vacuous (excluded, no seeded defect in prefix): {summary['vacuous']}")
    if summary["collateral"]:
        print(f"collateral (expected where views are shared): {summary['collateral']}")
    if errored:
        print(f"errors: {summary['errors'][:5]}")
    print(f"output: {out_dir / 'summary.json'}")
    print(f"output: {out_dir / 'detection.csv'}")
    print(f"output: {out_dir / 'log.txt'}")
    return 0 if summary["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
