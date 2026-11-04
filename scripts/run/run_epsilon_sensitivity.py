#!/usr/bin/env python3
"""
run_epsilon_sensitivity.py -- are the design-space verdicts an artifact of the
comparator tolerance epsilon?

WHY THIS EXISTS
---------------
The equivalence contract compares floating-point fields with a magnitude-relative
tolerance epsilon = 1e-9. Every EMR verdict in the paper depends on that one
constant, and a reviewer is entitled to ask whether it was chosen to produce the
reported result. This sweep answers that in advance: it re-scores the SAME
engine output at a range of epsilon and reports, per cell, the epsilon interval
over which the verdict is unchanged.

A verdict that holds across several orders of magnitude is a property of the
engine. A verdict that flips near the chosen epsilon is a property of the choice,
and must be reported as such. The sweep derives that conclusion from the current
cell rather than assuming an engine-specific outcome.

Cheap by construction: the engine is driven ONCE per cell and its view is
re-compared at every epsilon, so the sweep costs one ingest, not one per epsilon.

Inputs  : data/fixtures/<domain>/*.parquet ; data/normalized/<domain>/log.parquet
Outputs : runs/<UTC>_<domain>_<engine>_epsilon/{summary.json,epsilon.csv,log.txt}
Runtime : ~1 min per (engine, domain) for the in-process engines. Containerized
          engines require their stack to be running; the public stack-managed
          campaign command is ./reproduce_all.sh scaling <engine>.
Run as  : python scripts/run/run_epsilon_sensitivity.py --engine reference --domain all
          python scripts/run/run_epsilon_sensitivity.py --engine materialize \
              --domain all --diagnostic D1,D2,D4,D7
"""
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import subprocess
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.adapters import ADAPTER_NAMES, get_adapter  # noqa: E402
from src.oracle.comparator import compare_bucket_tables  # noqa: E402


def _load_runner():
    path = ROOT / "scripts" / "run" / "run_diagnostic.py"
    spec = importlib.util.spec_from_file_location("run_diagnostic", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
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

#: EMR diagnostics only -- D3 (bitwise) and D6/D7S do not consume epsilon.
DIAGNOSTICS = ["D1", "D2", "D4", "D7"]

DEFAULT_EPSILONS = [1e-12, 1e-11, 1e-10, 1e-9, 1e-8, 1e-7, 1e-6, 1e-5, 1e-4]

CSV_FIELDS = ["engine", "domain", "diagnostic", "epsilon", "hourly_emr",
              "daily_emr", "equivalent", "error"]


def _git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def _cell(runner, engine: str, domain: str, diag: str, max_rows: int,
          epsilons: List[float], log: List[str]) -> List[Dict[str, Any]]:
    """Drive the engine ONCE, then re-compare its view at every epsilon."""
    spec = runner.DIAGNOSTICS[diag]
    src = (ROOT / "data/normalized" / domain / "log.parquet" if spec.fixture is None
           else ROOT / "data/fixtures" / domain / spec.fixture)
    df = runner._load(
        src, max_rows, allow_restatements=diag == "D7")

    adapter = get_adapter(engine, allow_restatements=diag == "D7")
    variant = spec.primary
    if variant not in adapter.supported_variants:
        log.append(f"  {diag}: unsupported variant {variant!r} for {engine}")
        return [{"engine": engine, "domain": domain, "diagnostic": diag,
                 "epsilon": e, "hourly_emr": None, "daily_emr": None,
                 "equivalent": None,
                 "error": f"{engine} does not implement the {variant!r} view"}
                for e in epsilons]

    oracle = runner._build_oracle(df, spec.restatement_aware, False)[0]
    with adapter.connect() as eng:
        eng.setup(variant=variant)
        eng.ingest_log(df)
        engine_hourly = eng.read_view("hourly")
        engine_daily = eng.read_view("daily")

    rows: List[Dict[str, Any]] = []
    for eps in epsilons:
        ch = compare_bucket_tables(engine_hourly, oracle["hourly"], epsilon=eps)
        cd = compare_bucket_tables(engine_daily, oracle["daily"], epsilon=eps)
        rows.append({
            "engine": engine, "domain": domain, "diagnostic": diag,
            "epsilon": eps, "hourly_emr": ch.get("emr"), "daily_emr": cd.get("emr"),
            "equivalent": bool(ch["equivalent"] and cd["equivalent"]), "error": None,
        })
    return rows


def _stability(rows: List[Dict[str, Any]], contract_eps: float) -> Dict[str, Any]:
    """Widest contiguous epsilon interval around the contract value over which the
    verdict does not change."""
    scored = [r for r in rows if r["equivalent"] is not None]
    if not scored:
        return {"stable": None, "verdict_at_contract": None,
                "stable_from": None, "stable_to": None, "flips": []}
    scored.sort(key=lambda r: r["epsilon"])
    at = next((r for r in scored if r["epsilon"] == contract_eps), None)
    if at is None:
        return {"stable": None, "verdict_at_contract": None,
                "stable_from": None, "stable_to": None, "flips": []}
    verdict = at["equivalent"]
    idx = scored.index(at)
    lo = hi = idx
    while lo > 0 and scored[lo - 1]["equivalent"] == verdict:
        lo -= 1
    while hi < len(scored) - 1 and scored[hi + 1]["equivalent"] == verdict:
        hi += 1
    flips = [r["epsilon"] for i, r in enumerate(scored)
             if i > 0 and r["equivalent"] != scored[i - 1]["equivalent"]]
    decades = 0
    if scored[lo]["epsilon"] > 0 and scored[hi]["epsilon"] > 0:
        import math
        decades = round(math.log10(scored[hi]["epsilon"] / scored[lo]["epsilon"]))
    return {
        "verdict_at_contract": verdict,
        "stable_from": scored[lo]["epsilon"],
        "stable_to": scored[hi]["epsilon"],
        "stable_decades": decades,
        # A verdict that survives the whole grid is epsilon-independent.
        "stable": lo == 0 and hi == len(scored) - 1,
        "flips": flips,
    }


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--engine", default="reference",
                    help=f"Engine or 'all'. Known: {', '.join(ADAPTER_NAMES)}")
    ap.add_argument("--domain", default="all", help="Domain or 'all'.")
    ap.add_argument("--diagnostic", default=",".join(DIAGNOSTICS),
                    help="Comma-separated EMR diagnostics (default D1,D2,D4,D7).")
    ap.add_argument("--epsilon", default=",".join(str(e) for e in DEFAULT_EPSILONS),
                    help="Comma-separated epsilon grid.")
    ap.add_argument("--contract-epsilon", type=float, default=1e-9,
                    help="The contract's epsilon (default 1e-9).")
    ap.add_argument("--max-rows", type=int, default=50_000,
                    help="Rows per cell (default 50000, the matrix scale).")
    args = ap.parse_args()

    engines = list(ADAPTER_NAMES) if args.engine == "all" else args.engine.split(",")
    domains = DOMAINS if args.domain == "all" else args.domain.split(",")
    diags = [d.strip() for d in args.diagnostic.split(",") if d.strip()]
    epsilons = sorted(float(e) for e in args.epsilon.split(",") if e.strip())
    if args.contract_epsilon not in epsilons:
        epsilons = sorted(epsilons + [args.contract_epsilon])

    runner = _load_runner()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    etag = args.engine if args.engine != "all" else "all"
    dtag = args.domain if args.domain != "all" else "all"
    out_dir = ROOT / "runs" / f"{stamp}_{dtag}_{etag}_epsilon"
    out_dir.mkdir(parents=True, exist_ok=True)

    log: List[str] = []
    rows: List[Dict[str, Any]] = []
    stability: Dict[str, Any] = {}
    for engine in engines:
        for domain in domains:
            header = f"==== epsilon sensitivity : {engine} on {domain} ===="
            log.append(header)
            print(header, flush=True)
            for diag in diags:
                try:
                    cell = _cell(runner, engine, domain, diag, args.max_rows,
                                 epsilons, log)
                except Exception as exc:
                    msg = f"{type(exc).__name__}: {exc}"
                    log.append(f"  {diag}: ERROR {msg}")
                    log.append(traceback.format_exc()[-600:])
                    print(f"  {diag}: ERROR {msg}", flush=True)
                    cell = [{"engine": engine, "domain": domain, "diagnostic": diag,
                             "epsilon": e, "hourly_emr": None, "daily_emr": None,
                             "equivalent": None, "error": msg} for e in epsilons]
                rows.extend(cell)
                st = _stability(cell, args.contract_epsilon)
                stability[f"{engine}/{domain}/{diag}"] = st
                if st["verdict_at_contract"] is None:
                    line = f"  {diag}: no verdict (unsupported or errored)"
                elif st["stable"]:
                    line = (f"  {diag}: {'PASS' if st['verdict_at_contract'] else 'FAIL'} "
                            f"stable across the whole grid "
                            f"[{epsilons[0]:g}, {epsilons[-1]:g}]")
                else:
                    line = (f"  {diag}: {'PASS' if st['verdict_at_contract'] else 'FAIL'} "
                            f"only on [{st['stable_from']:g}, {st['stable_to']:g}] "
                            f"({st['stable_decades']} decades); flips at "
                            f"{[f'{f:g}' for f in st['flips']]}")
                log.append(line)
                print(line, flush=True)

    scored = [s for s in stability.values() if s["verdict_at_contract"] is not None]
    fragile = {k: v for k, v in stability.items()
               if v["verdict_at_contract"] is not None and not v["stable"]}

    summary = {
        "kind": "epsilon_sensitivity",
        "git_commit": _git_commit(),
        "started_utc": stamp,
        "engines": engines, "domains": domains, "diagnostics": diags,
        "epsilons": epsilons, "contract_epsilon": args.contract_epsilon,
        "max_rows": args.max_rows,
        "cells_scored": len(scored),
        "cells_stable_across_grid": sum(1 for s in scored if s["stable"]),
        "fragile_cells": fragile,
        "stability": stability,
        "rows": rows,
        # The sweep passes when no verdict depends on the choice of epsilon.
        "pass": bool(scored) and not fragile,
    }
    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n")
    with (out_dir / "epsilon.csv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        w.writeheader()
        for row in rows:
            w.writerow({k: row[k] for k in CSV_FIELDS})
    (out_dir / "log.txt").write_text("\n".join(log) + "\n")

    verdict = "PASS" if summary["pass"] else "FAIL"
    print(f"\n{verdict}: {summary['cells_stable_across_grid']}/{len(scored)} cell(s) "
          f"hold their verdict across the entire epsilon grid "
          f"[{epsilons[0]:g}, {epsilons[-1]:g}]")
    if fragile:
        print(f"epsilon-dependent cell(s) -- must be disclosed: {sorted(fragile)}")
    print(f"output: {out_dir / 'summary.json'}")
    print(f"output: {out_dir / 'epsilon.csv'}")
    print(f"output: {out_dir / 'log.txt'}")
    return 0 if summary["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
