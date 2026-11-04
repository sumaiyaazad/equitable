#!/usr/bin/env python3
"""
run_parallelism_sweep.py -- does replay determinism (C3, D3) depend on how
many parallel workers maintain the view?

D3 replays the same log twice and compares per-key sums bit for bit. In the
promoted grid Flink runs at parallelism 2 and RisingWave at its default, and
both drift on some domains while staying within the contract tolerance of the
oracle. A reader will ask whether that is a property of the engine or of the
setting. This experiment varies only the setting: the same D3 cell at
parallelism 1, 2, 4 and 8, per domain, recording drifting keys, the drift rate,
and whether both replays still satisfy the oracle.

Floating-point addition is not associative, so partial sums combined in a
data-dependent order can differ in the last bits between runs. At parallelism 1
there is one combining order and replays should be bit-identical.

Inputs  : data/normalized/<domain>/log.parquet
Outputs : runs/<UTC>_parallelism_sweep/{summary.json, parallelism_sweep.csv}
Run as  : python scripts/run/run_parallelism_sweep.py [--engine all] [--rows 50000]
Promote : python scripts/results/promote_results.py runs/<run_id> --block parallelism_sweep
"""
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.provenance import run_provenance                 # noqa: E402


def _diagnostic_module():
    path = ROOT / "scripts" / "run" / "run_diagnostic.py"
    spec = importlib.util.spec_from_file_location("_equitable_run_diagnostic", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    # Register before executing: dataclasses resolve their module by name.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


#: engine -> environment variable its adapter reads for parallelism
PARALLELISM_ENV = {
    "flink": "EQUITABLE_FLINK_PARALLELISM",
    "risingwave": "EQUITABLE_RISINGWAVE_PARALLELISM",
}
LEVELS = (1, 2, 4, 8)
DOMAINS = ("datacenter_drives", "environment_airquality", "finance_markets",
           "sports_football", "transportation_taxi")
DEFAULT_ROWS = 50_000


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--engine", default="all")
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--domain", default="all")
    parser.add_argument("--epsilon", type=float, default=1e-9)
    args = parser.parse_args()
    engines = list(PARALLELISM_ENV) if args.engine == "all" else [args.engine]
    domains = list(DOMAINS) if args.domain == "all" else [args.domain]
    rd = _diagnostic_module()

    started = datetime.now(timezone.utc)
    cells: List[Dict[str, Any]] = []
    blocked = 0
    for engine in engines:
        var = PARALLELISM_ENV[engine]
        previous = os.environ.get(var)
        for level in LEVELS:
            os.environ[var] = str(level)
            for domain in domains:
                cell: Dict[str, Any] = {"engine": engine, "parallelism": level,
                                        "domain": domain}
                try:
                    s = rd.run_one(engine, domain, "D3", args.rows, args.epsilon,
                                   co_oracle=False, d3_ledger=False)
                    cell.update({
                        "rows": s.get("rows"),
                        "keys": s.get("keys"),
                        "drift_keys": s.get("drift_keys"),
                        "ddr": s.get("ddr"),
                        "oracle_equivalent": s.get("oracle_equivalent"),
                        "artifact_valid": s.get("artifact_valid"),
                        "status": "measured" if s.get("artifact_valid") else "blocked",
                    })
                    if not s.get("artifact_valid"):
                        blocked += 1
                        cell["reason"] = str(s.get("reason") or s.get("error"))[:200]
                except Exception as exc:
                    blocked += 1
                    cell.update({"status": "blocked",
                                 "reason": f"{type(exc).__name__}: {exc}"[:200]})
                cells.append(cell)
                print(json.dumps(cell), flush=True)
        if previous is None:
            os.environ.pop(var, None)
        else:
            os.environ[var] = previous

    finished = datetime.now(timezone.utc)
    stamp = started.strftime("%Y%m%dT%H%M%SZ")
    out_dir = ROOT / "runs" / f"{stamp}_parallelism_sweep"
    out_dir.mkdir(parents=True, exist_ok=True)
    measured = [c for c in cells if c.get("status") == "measured"]
    summary = {
        "kind": "parallelism_sweep",
        "diagnostic": "D3",
        **run_provenance(ROOT),
        "started_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "finished_utc": finished.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "prefix_rows": args.rows,
        "levels": list(LEVELS),
        "engines": engines,
        "domains": domains,
        "cells": len(cells),
        "measured_cells": len(measured),
        "blocked": blocked,
        "rows": cells,
        "pass": bool(measured) and not blocked,
        "artifact_valid": bool(measured) and not blocked,
    }
    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n")
    fields = ["engine", "parallelism", "domain", "rows", "keys", "drift_keys",
              "ddr", "oracle_equivalent", "status"]
    with (out_dir / "parallelism_sweep.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(cells)
    print(f"\nmeasured {len(measured)}/{len(cells)}; blocked {blocked}")
    print(f"output: {out_dir}/summary.json")
    return 0 if summary["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
