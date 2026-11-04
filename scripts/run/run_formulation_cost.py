#!/usr/bin/env python3
"""
run_formulation_cost.py -- what does the event-time repair cost?

R2 shows that several engines match the contract only when the benchmark writes
the event-time selection (the repair) instead of the arrival-ordered selection
their documentation leads to. An engineer deciding whether to adopt the repair
needs its price. This experiment measures it, one engine at a time on an idle
host, so the timings are not shared with any other lane.

For each engine that can express both formulations, it replays the same
reordered log (the campaign's D2 fixture, so delivery is out of order as in
production) through the repaired view and through the arrival-ordered view, and
records the wall time from view creation until both grains have converged and
been read. The two formulations share deduplication, bucketing and every
reduction; they differ only in the sort key of the latest-row selection
(Listing 1), so the difference is the cost of that key.

Formulations alternate between repeats so warm-up and caching do not favour
one of them, and each formulation first runs once UNTIMED. The warm-up matters:
Feldera compiles every pipeline and caches the build, so without it a timed run
took about 6 s or about 14 s depending only on whether it hit the cache, and a
median of three could report the repair as 2.4 times faster or slower for no
reason in the formulation.

Inputs  : data/fixtures/<domain>/d2_reorder.parquet
Outputs : runs/<UTC>_formulation_cost/{summary.json, formulation_cost.csv}
Run as  : python scripts/run/run_formulation_cost.py [--engine all] [--rows 200000]
Promote : python scripts/results/promote_results.py runs/<run_id> --block formulation_cost
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.adapters import get_adapter                      # noqa: E402
from src.adapters.base import canonical_input             # noqa: E402
from src.provenance import run_provenance                 # noqa: E402

DEFAULT_ENGINES = ("materialize", "risingwave", "flink", "spark", "feldera")
DOMAINS = ("finance_markets", "transportation_taxi")
VARIANTS = ("dedup", "arrival_latest")       # repair, arrival-ordered
DEFAULT_ROWS = 200_000
REPEATS = 3


def _frame(domain: str, rows: int) -> pd.DataFrame:
    path = ROOT / "data" / "fixtures" / domain / "d2_reorder.parquet"
    df = canonical_input(pd.read_parquet(path).head(rows))
    df["processing_order"] = np.arange(len(df), dtype="int64")
    return df


def _one(engine: str, df: pd.DataFrame, variant: str) -> Dict[str, Any]:
    adapter = get_adapter(engine)
    with adapter.connect() as eng:
        started = time.perf_counter()
        eng.setup(variant=variant)
        eng.ingest_log(df)
        hourly = eng.read_view("hourly")
        daily = eng.read_view("daily")
        seconds = time.perf_counter() - started
    return {"seconds": round(seconds, 3), "hourly_rows": int(len(hourly)),
            "daily_rows": int(len(daily))}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--engine", default="all")
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--repeats", type=int, default=REPEATS)
    args = parser.parse_args()
    engines = list(DEFAULT_ENGINES) if args.engine == "all" else [args.engine]

    started = datetime.now(timezone.utc)
    cells: List[Dict[str, Any]] = []
    blocked = 0
    for engine in engines:
        for domain in DOMAINS:
            df = _frame(domain, args.rows)
            for variant in VARIANTS:            # untimed warm-up, see docstring
                try:
                    _one(engine, df.copy(deep=True), variant)
                except Exception as exc:
                    print(f"warm-up {engine}/{domain}/{variant} failed: {exc}",
                          flush=True)
            for repeat in range(args.repeats):
                order = VARIANTS if repeat % 2 == 0 else tuple(reversed(VARIANTS))
                for variant in order:
                    cell: Dict[str, Any] = {"engine": engine, "domain": domain,
                                            "variant": variant, "repeat": repeat,
                                            "rows": int(len(df))}
                    try:
                        cell.update(_one(engine, df.copy(deep=True), variant))
                        cell["status"] = "measured"
                    except Exception as exc:
                        blocked += 1
                        cell.update({"status": "blocked",
                                     "reason": f"{type(exc).__name__}: {exc}"[:200]})
                    cells.append(cell)
                    print(json.dumps(cell), flush=True)

    # Median seconds per (engine, domain, variant) and the repair's ratio.
    medians: Dict[tuple, float] = {}
    for key in {(c["engine"], c["domain"], c["variant"]) for c in cells}:
        secs = [c["seconds"] for c in cells if c.get("status") == "measured"
                and (c["engine"], c["domain"], c["variant"]) == key]
        if secs:
            medians[key] = statistics.median(secs)
    ratios = []
    for engine in engines:
        for domain in DOMAINS:
            rep = medians.get((engine, domain, "dedup"))
            arr = medians.get((engine, domain, "arrival_latest"))
            if rep and arr:
                ratios.append({"engine": engine, "domain": domain,
                               "repair_median_s": rep, "arrival_median_s": arr,
                               "repair_over_arrival": round(rep / arr, 4)})

    finished = datetime.now(timezone.utc)
    stamp = started.strftime("%Y%m%dT%H%M%SZ")
    out_dir = ROOT / "runs" / f"{stamp}_formulation_cost"
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "kind": "formulation_cost",
        "diagnostic": "D2",
        **run_provenance(ROOT),
        "started_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "finished_utc": finished.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "prefix_rows": args.rows,
        "repeats": args.repeats,
        "engines": engines,
        "domains": list(DOMAINS),
        "variants": {"dedup": "repair (event-time latest)",
                     "arrival_latest": "arrival-ordered latest"},
        "serialized": True,
        "warmup_runs_per_variant": 1,
        "cells": len(cells),
        "blocked": blocked,
        "ratios": ratios,
        "rows": cells,
        "pass": bool(ratios) and not blocked,
        "artifact_valid": bool(ratios) and not blocked,
    }
    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n")
    with (out_dir / "formulation_cost.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=[
            "engine", "domain", "variant", "repeat", "rows", "seconds", "status"],
            extrasaction="ignore")
        writer.writeheader()
        writer.writerows(cells)
    for r in ratios:
        print(f"{r['engine']:12s} {r['domain']:22s} repair {r['repair_median_s']:8.1f}s "
              f"arrival {r['arrival_median_s']:8.1f}s ratio {r['repair_over_arrival']:.2f}")
    print(f"output: {out_dir}/summary.json")
    return 0 if summary["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
