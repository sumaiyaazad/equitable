#!/usr/bin/env python3
"""
run_query_family.py -- the order-sensitive query family (R2 generalization).

D1, D2 and D4 all score ONE view: a grouped aggregate whose per-bucket latest
is chosen by a total order over row content. The R2 gap the paper reports is
therefore measured on one query shape under three interventions, and a reader
may reasonably ask whether the gap is a property of streaming engines or of the
one query we wrote.

This diagnostic answers that. It enumerates a family of order-sensitive shapes
(src/diagnostics/shapes.py), each placing the same rule in a different position
of the query plan, and scores every member the way R2 scores a cell: both the
contract-ordered and the arrival-ordered formulation are compared against the
SAME contract oracle. A member where only the contract formulation matches is a
repaired pass, meaning the engine can state the event-time rule when the
benchmark writes it, while the formulation its own documentation leads to would
have published a different table.

Two controls make the number meaningful:
  * two independently written oracles must agree before any engine is scored,
    and a disagreement is a harness error rather than a finding;
  * with the SAME log delivered in event-time order the two formulations
    coincide, so every member must report attributable. A family that still
    reported repaired there would be detecting its own construction.

Inputs  : data/normalized/<domain>/log.parquet
Outputs : runs/<UTC>_query_family/{summary.json, query_family.csv}
Run as  : python scripts/run/run_query_family.py [--rows 5000] [--engine all]
Promote : python scripts/results/promote_results.py runs/<run_id> --block query_family
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.adapters import get_adapter                      # noqa: E402
from src.adapters.base import canonical_input             # noqa: E402
from src.diagnostics.shapes import APPLICABLE, SHAPES, control_verdict  # noqa: E402
from src.diagnostics.shape_runner import run_shape_pair   # noqa: E402
from src.provenance import run_provenance                 # noqa: E402

#: Every engine the family is PUT TO, not only those that can answer. An engine
#: that cannot express a member declines through its adapter and the cell
#: records unsupported, which is a capability result and belongs in the
#: denominator. ksqlDB is the case that matters: it has no event-time selection
#: primitive at all, since LATEST_BY_OFFSET is arrival ordered, so it can never
#: state the contract formulation. Leaving it out would hide the strongest
#: single observation the family makes.
#:
#: One omission, for a stated reason. Flink was once left out because in-process
#: PyFlink at parallelism one could not absorb a view rebuild per member per arm;
#: the Flink cluster can, so it is in the family. Feldera's shape SQL is
#: implemented in its adapter but is not claimed here: the pinned v0.324.0 image
#: recreates and recompiles its whole pipeline on every setup, which the family
#: does once per member per arm, and the resulting sequence did not complete
#: reliably enough to stand behind. Enable it explicitly with --engine feldera
#: once that path is verified; leaving it out is a scope statement, not a
#: finding about the engine.
DEFAULT_ENGINES = ("materialize", "risingwave", "flink", "spark", "ksqldb")

#: Matches the primary grid's pinned size, so the family is not read beside a
#: cohort an order of magnitude larger without saying so.
DEFAULT_ROWS = 50_000

#: The delivery permutation is fixed so a rerun selects the same arrival order
#: and the family is reproducible byte for byte.
PERMUTATION_SEED = 5


def _frames(domain: str, rows: int) -> Dict[str, pd.DataFrame]:
    """The treatment and its control: shuffled delivery, and ordered delivery."""
    base = canonical_input(
        pd.read_parquet(ROOT / "data" / "normalized" / domain / "log.parquet")
        .head(rows))
    shuffled = base.copy()
    shuffled["processing_order"] = np.random.default_rng(
        PERMUTATION_SEED).permutation(len(base))
    ordered = base.sort_values(
        ["event_time_utc", "dedupe_id"]).reset_index(drop=True).copy()
    ordered["processing_order"] = range(len(ordered))
    return {"shuffled": shuffled, "ordered": ordered}


def _cell(adapter, frame, *, engine, domain, grain, shape, delivery,
          rows) -> Dict[str, Any]:
    try:
        result = run_shape_pair(adapter, frame, shape=shape, grain=grain)
    except Exception as exc:                       # execution blocker
        return {"engine": engine, "domain": domain, "grain": grain,
                "shape": shape, "delivery": delivery, "rows": rows,
                "attribution": "blocked",
                "reason": f"{type(exc).__name__}: {exc}"[:200]}
    return {
        "engine": engine, "domain": domain, "grain": grain, "shape": shape,
        "delivery": delivery, "rows": rows,
        "schema": result.get("schema"),
        "plan_position": result.get("plan_position"),
        "supported": result.get("supported"),
        "oracle_disagreement": bool(result.get("oracle_disagreement")),
        "contract_equivalent": result.get("contract_equivalent"),
        "arrival_equivalent": result.get("arrival_equivalent"),
        "attribution": result.get("attribution"),
        "reason": result.get("reason"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS,
                        help="prefix of each log to replay")
    parser.add_argument("--engine", default="all")
    parser.add_argument("--domain", default="all")
    args = parser.parse_args()

    engines = (list(DEFAULT_ENGINES) if args.engine == "all"
               else [args.engine])
    started = datetime.now(timezone.utc)
    cells: List[Dict[str, Any]] = []

    for engine in engines:
        adapter = get_adapter(engine)
        with adapter.connect() as connected:
            for domain, grains in sorted(APPLICABLE.items()):
                if not grains or (args.domain != "all" and domain != args.domain):
                    continue
                frames = _frames(domain, args.rows)
                for delivery, frame in frames.items():
                    for grain in grains:
                        for shape in sorted(SHAPES):
                            cell = _cell(connected, frame, engine=engine,
                                         domain=domain, grain=grain,
                                         shape=shape, delivery=delivery,
                                         rows=int(len(frame)))
                            cells.append(cell)
                            print(json.dumps(cell), flush=True)

    finished = datetime.now(timezone.utc)
    treatment = [c for c in cells if c["delivery"] == "shuffled"]
    control = [c for c in cells if c["delivery"] == "ordered"]
    expressible = [c for c in treatment
                   if c["attribution"] in ("attributable", "repaired",
                                           "repair_divergent")]
    repaired = sum(1 for c in expressible if c["attribution"] == "repaired")

    # The control is what makes the treatment number mean anything: delivered
    # in event-time order the two formulations coincide, so every expressible
    # member must be attributable.
    control_expressible = [c for c in control
                           if c["attribution"] in ("attributable", "repaired",
                                                   "repair_divergent")]
    control_clean = all(c["attribution"] == "attributable"
                        for c in control_expressible)
    control = control_verdict(cells)
    disagreements = sum(1 for c in cells if c.get("oracle_disagreement"))
    blocked = sum(1 for c in cells if c["attribution"] == "blocked")

    stamp = started.strftime("%Y%m%dT%H%M%SZ")
    out_dir = ROOT / "runs" / f"{stamp}_query_family"
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "kind": "query_family",
        "diagnostic": "query_family",
        **run_provenance(ROOT),
        "started_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "finished_utc": finished.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "prefix_rows": args.rows,
        "permutation_seed": PERMUTATION_SEED,
        "engines": engines,
        "shapes": sorted(SHAPES),
        "applicable": {d: list(g) for d, g in sorted(APPLICABLE.items())},
        "cells": len(cells),
        "expressible_cells": len(expressible),
        "repaired_cells": repaired,
        "control_all_attributable": bool(control_clean),
        "control_construction_clean": control["construction_clean"],
        "control_construction_failures": control["construction_failures"],
        "control_engine_gaps": control["engine_gaps"],
        "oracle_disagreements": disagreements,
        "blocked": blocked,
        "rows": cells,
        # Gate on the construction, not on every engine being right: an
        # engine's own divergence under ordered delivery is a finding.
        "pass": bool(expressible) and control["construction_clean"]
                and not disagreements and not blocked,
        "artifact_valid": bool(cells) and not blocked and not disagreements,
    }
    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n")
    fields = ["engine", "domain", "grain", "shape", "delivery", "rows",
              "attribution", "contract_equivalent", "arrival_equivalent"]
    with (out_dir / "query_family.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(cells)

    pct = (100.0 * repaired / len(expressible)) if expressible else 0.0
    print(f"\n{repaired}/{len(expressible)} expressible members ({pct:.0f}%) "
          f"match the oracle only under the contract formulation")
    print(f"control (ordered delivery) all attributable: {control_clean}")
    print(f"control construction clean: {control['construction_clean']}"
          + (f"; engine divergences under ordered delivery: "
             f"{len(control['engine_gaps'])} cell(s) in "
             f"{sorted({g['engine'] for g in control['engine_gaps']})}"
             if control["engine_gaps"] else ""))
    print(f"oracle disagreements: {disagreements} | blocked: {blocked}")
    print(f"output: {out_dir}/summary.json")
    return 0 if summary["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
