#!/usr/bin/env python3
"""
run_disorder_sweep.py -- how much disorder does an arrival-ordered latest need
before it publishes a wrong value?

R2 separates a repaired pass from an attributable one: an engine whose
documented "latest" operator follows arrival order matches the oracle only
because the benchmark supplied the event-time rule. A reader can object that
this is true by definition, since an arrival-ordered selection must differ from
an event-time one once events arrive out of order. The objection is right about
the direction and silent about the amount. This experiment measures the amount.

For each domain and grain the family scores (src/diagnostics/shapes.APPLICABLE),
it replays the same 50,000 rows under a ladder of bounded reorderings, built
exactly as the D2 fixture is (scripts/perturb/build_fixtures.perturb_d2_reorder,
event-time mode), from ordered delivery up to a full random permutation. At each
step it records:

  * the realised event-time inversion rate of the delivered log, the fraction
    of adjacent arrivals whose event times run backwards, so the x axis is a
    property of the delivered log rather than of the knob;
  * the EXPOSURE: the fraction of keys whose arrival-ordered latest row differs
    from the event-time latest row, computed from the oracle alone, so it is a
    property of the data and the disorder and of no engine;
  * for every engine that can express both formulations of the latest-per-bucket
    member (S1_latest), the fraction of keys its arrival-ordered view gets wrong
    and whether its event-time view matches the oracle.

If each engine's arrival-ordered error equals the exposure, the gap R2 reports
is the operator's semantics applied to real data, and the exposure curve says
how much disorder a deployment can tolerate before the published table changes.

Inputs  : data/normalized/<domain>/log.parquet
Outputs : runs/<UTC>_disorder_sweep/{summary.json, disorder_sweep.csv}
Run as  : python scripts/run/run_disorder_sweep.py [--engine all] [--rows 50000]
Promote : python scripts/results/promote_results.py runs/<run_id> --block disorder_sweep
"""
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.adapters import get_adapter                      # noqa: E402
from src.adapters.base import canonical_input, canonical_shape_frame  # noqa: E402
from src.diagnostics.shapes import (APPLICABLE, ARRIVAL_ORDER,        # noqa: E402
                                    CONTRACT_ORDER, SHAPES)
from src.diagnostics.shape_runner import _prepare         # noqa: E402
from src.oracle import shape_oracle                       # noqa: E402
from src.provenance import run_provenance                 # noqa: E402


def _fixtures_module():
    """build_fixtures is a script, not a package module; load it by path so the
    sweep reuses the D2 perturbation itself rather than a copy of it."""
    path = ROOT / "scripts" / "perturb" / "build_fixtures.py"
    spec = importlib.util.spec_from_file_location("_equitable_build_fixtures", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    # Register before executing: dataclasses resolve their module by name.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


FIX = _fixtures_module()

#: Engines that can state S1_latest under BOTH orders. ksqlDB cannot state the
#: event-time order at all (the query family records that as unsupported), and
#: Feldera is outside the family for the reason run_query_family.py gives.
DEFAULT_ENGINES = ("materialize", "risingwave", "flink", "spark")

#: Reorder windows in distinct-event-time steps, the D2 fixture's unit. 0 is
#: ordered delivery; "shuffle" is the query family's full random permutation.
#: perturb_d2_reorder treats a window of 1 as no reordering, so the ladder
#: starts at 2.
#: The campaign's D2 fixture uses 100 (config/default.yaml reorder_window).
WINDOWS: tuple = (0, 2, 3, 5, 10, 30, 100, "shuffle")
#: Engines are replayed at a subset of the ladder. The exposure is computed
#: from the oracle at every window; an engine's role is only to show that its
#: arrival-ordered view lands on that curve, which ordered delivery, a small
#: displacement, D2's own window, and a full shuffle establish. Replaying every
#: engine at all eight windows took about 1.6 minutes per cell.
ENGINE_WINDOWS: tuple = (0, 3, 100, "shuffle")
SHAPE = "S1_latest"
DEFAULT_ROWS = 50_000
SEED = 1337
SHUFFLE_SEED = 5                     # run_query_family.PERMUTATION_SEED
KEY = ["bucket", "entity", "metric"]
PARAMS = {"k": 10, "quantile": 0.5}          # run_shape_pair defaults


def _base(domain: str, rows: int) -> pd.DataFrame:
    df = pd.read_parquet(ROOT / "data" / "normalized" / domain / "log.parquet").head(rows)
    ordered = df.sort_values(["event_time_utc", "dedupe_id"], kind="mergesort")
    return FIX._finalize(ordered)


def _delivered(base: pd.DataFrame, window) -> pd.DataFrame:
    """The log as delivered: rows physically in arrival order, processing_order
    contiguous over that order, exactly as a D2 fixture is written."""
    if window == "shuffle":
        order = np.random.default_rng(SHUFFLE_SEED).permutation(len(base))
        return FIX._finalize(base.iloc[order])
    rng = FIX._rng(SEED, FIX._SUBSTREAM["d2"])
    return FIX.perturb_d2_reorder(base, int(window), rng, mode="event_time")


def _late_fraction(frame: pd.DataFrame) -> float:
    """Fraction of rows delivered after a row with a LATER event time, i.e.
    rows that arrive behind the running maximum. Unlike the adjacent inversion
    rate, which saturates near one half, this keeps rising with disorder and
    reads the same way on every domain."""
    t = pd.to_datetime(frame["event_time_utc"], utc=True, format="mixed"
                       ).astype("int64").to_numpy()
    if len(t) == 0:
        return 0.0
    running = np.maximum.accumulate(t)
    return float((t < running).sum()) / float(len(t))


#: The contract's epsilon (body.tex, "Repeats and controls"; the scored
#: comparator's default). A latest value counts as wrong only outside
#: epsilon * max(1, |a|, |b|), as it does in every scored cell. Under exact
#: comparison a last-bit difference (41.8642 against 41.864200000000004)
#: would read as a wrong key off the oracle curve.
EPSILON = 1e-9


def _wrong_fraction(observed: pd.DataFrame, expected: pd.DataFrame,
                    grain: str, epsilon: float = EPSILON) -> Dict[str, Any]:
    """Fraction of keys whose SELECTED row differs: latest value or latest event
    time, plus any key present on one side only. Count, sum and the other
    reductions are ignored, as in the family's comparison, so a
    known numeric divergence cannot masquerade as an order effect."""
    left = canonical_shape_frame(observed, grain)
    right = canonical_shape_frame(expected, grain)
    merged = left[KEY + ["latest_value", "latest_event_time"]].merge(
        right[KEY + ["latest_value", "latest_event_time"]], on=KEY, how="outer",
        suffixes=("_o", "_e"), indicator=True)
    one_side = merged["_merge"] != "both"
    both = merged[~one_side]
    a = both["latest_value_o"].astype(float).to_numpy()
    b = both["latest_value_e"].astype(float).to_numpy()
    value_diff = np.abs(a - b) > epsilon * np.maximum(
        1.0, np.maximum(np.abs(a), np.abs(b)))
    time_diff = (both["latest_event_time_o"].astype(str)
                 != both["latest_event_time_e"].astype(str)).to_numpy()
    wrong = int(one_side.sum()) + int((value_diff | time_diff).sum())
    total = int(len(merged))
    return {"keys": total, "wrong_keys": wrong,
            "wrong_fraction": (wrong / total) if total else 0.0}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--engine", default="all",
                        help="one engine, 'all' (the default set) or 'none' "
                             "(oracle exposure only)")
    parser.add_argument("--domain", default="all")
    args = parser.parse_args()
    engines: List[str] = (list(DEFAULT_ENGINES) if args.engine == "all"
                          else [] if args.engine == "none" else [args.engine])

    started = datetime.now(timezone.utc)
    rows_out: List[Dict[str, Any]] = []
    oracle_disagreements = 0
    blocked = 0

    # Oracle exposure first: it needs no engine and fixes the reference curve.
    slots = [(d, g) for d, grains in sorted(APPLICABLE.items()) for g in grains
             if args.domain in ("all", d)]
    frames: Dict[tuple, pd.DataFrame] = {}
    expected: Dict[tuple, pd.DataFrame] = {}
    for domain, grain in slots:
        base = _base(domain, args.rows)
        for window in WINDOWS:
            frame = _delivered(base, window)
            frames[(domain, window)] = frame
            prepared = _prepare(canonical_input(frame), grain)
            contract = SHAPES[SHAPE].expected(prepared, CONTRACT_ORDER, **PARAMS)
            sql_contract = shape_oracle.expected(prepared, SHAPE, CONTRACT_ORDER, **PARAMS)
            agree = _wrong_fraction(sql_contract, contract, grain)["wrong_keys"] == 0
            if not agree:
                oracle_disagreements += 1
            arrival = SHAPES[SHAPE].expected(prepared, ARRIVAL_ORDER, **PARAMS)
            exposure = _wrong_fraction(arrival, contract, grain)
            expected[(domain, grain, window)] = contract
            row = {"kind": "exposure", "engine": "oracle", "domain": domain,
                   "grain": grain, "window": str(window), "rows": int(len(frame)),
                   "inversion_rate": FIX._event_time_inversion_rate(frame),
                   "late_fraction": _late_fraction(frame),
                   "oracles_agree": bool(agree),
                   "keys": exposure["keys"], "wrong_keys": exposure["wrong_keys"],
                   "wrong_fraction": exposure["wrong_fraction"]}
            rows_out.append(row)
            print(json.dumps(row), flush=True)

    for engine in engines:
        adapter = get_adapter(engine)
        if SHAPE not in getattr(adapter, "supported_shapes", ()):
            rows_out.append({"kind": "engine", "engine": engine,
                             "status": "unsupported"})
            continue
        with adapter.connect() as eng:
            for domain, grain in slots:
                for window in ENGINE_WINDOWS:
                    frame = frames[(domain, window)]
                    exp = expected[(domain, grain, window)]
                    cell: Dict[str, Any] = {
                        "kind": "engine", "engine": engine, "domain": domain,
                        "grain": grain, "window": str(window),
                        "rows": int(len(frame)),
                        "inversion_rate": FIX._event_time_inversion_rate(frame),
                        "late_fraction": _late_fraction(frame)}
                    try:
                        for order, tag in ((CONTRACT_ORDER, "contract"),
                                           (ARRIVAL_ORDER, "arrival")):
                            eng.setup_shape(shape=SHAPE, order=order)
                            eng.ingest_log(canonical_input(frame))
                            got = eng.read_shape(grain)
                            res = _wrong_fraction(got, exp, grain)
                            cell[f"{tag}_wrong_keys"] = res["wrong_keys"]
                            cell[f"{tag}_wrong_fraction"] = res["wrong_fraction"]
                            cell["keys"] = res["keys"]
                        cell["status"] = "measured"
                    except Exception as exc:          # execution blocker
                        blocked += 1
                        cell["status"] = "blocked"
                        cell["reason"] = f"{type(exc).__name__}: {exc}"[:200]
                    rows_out.append(cell)
                    print(json.dumps(cell), flush=True)

    finished = datetime.now(timezone.utc)
    exposure_rows = [r for r in rows_out if r["kind"] == "exposure"]
    engine_rows = [r for r in rows_out
                   if r["kind"] == "engine" and r.get("status") == "measured"]
    # An engine TRACKS the exposure when its arrival-ordered error equals the
    # oracle's arrival-ordered error on the same delivered log.
    by_slot = {(r["domain"], r["grain"], r["window"]): r for r in exposure_rows}
    tracks = [r for r in engine_rows
              if r["arrival_wrong_keys"]
              == by_slot[(r["domain"], r["grain"], r["window"])]["wrong_keys"]]
    stamp = started.strftime("%Y%m%dT%H%M%SZ")
    out_dir = ROOT / "runs" / f"{stamp}_disorder_sweep"
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "kind": "disorder_sweep",
        "diagnostic": "disorder_sweep",
        **run_provenance(ROOT),
        "started_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "finished_utc": finished.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "shape": SHAPE,
        "prefix_rows": args.rows,
        "windows": [str(w) for w in WINDOWS],
        "engine_windows": [str(w) for w in ENGINE_WINDOWS],
        "reorder_mode": "event_time",
        "seed": SEED,
        "shuffle_seed": SHUFFLE_SEED,
        "engines": engines,
        "applicable": {d: list(g) for d, g in sorted(APPLICABLE.items())},
        "exposure_cells": len(exposure_rows),
        "engine_cells": len(engine_rows),
        "engine_cells_tracking_exposure": len(tracks),
        "oracle_disagreements": oracle_disagreements,
        "blocked": blocked,
        "rows": rows_out,
        "pass": bool(exposure_rows) and not oracle_disagreements and not blocked,
        "artifact_valid": bool(exposure_rows) and not oracle_disagreements
                          and not blocked,
    }
    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n")
    fields = ["kind", "engine", "domain", "grain", "window", "rows",
              "inversion_rate", "late_fraction", "keys", "wrong_keys", "wrong_fraction",
              "contract_wrong_keys", "arrival_wrong_keys",
              "arrival_wrong_fraction", "status"]
    with (out_dir / "disorder_sweep.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows_out)
    print(f"\nexposure cells: {len(exposure_rows)}; engine cells: {len(engine_rows)}; "
          f"tracking the exposure: {len(tracks)}; oracle disagreements: "
          f"{oracle_disagreements}; blocked: {blocked}")
    print(f"output: {out_dir}/summary.json")
    return 0 if summary["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
