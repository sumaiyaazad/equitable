#!/usr/bin/env python3
"""
run_d7s_sweep.py -- D7S sensitivity sweep: does the bounded-deletion sketch (KLL±)
actually separate from the insert-only sketch (KLL), and where?

WHY THIS EXISTS
---------------
The single-point D7S result reports io_qer == bd_qer == 0 on all five domains,
i.e. the diagnostic does not discriminate between the two summaries it exists to
compare. Two causes, both measurement artifacts rather than properties of KLL±:

  1. Prefix-controlled alpha. The sweep creates restatements after truncating
     the base log, so each tested prefix realizes exactly the requested deletion
     ratio instead of depending on fixture-placement boundaries.

  2. Sketch size. At d7s_kll_k=800 a KLL is near-exact on groups of this size, so
     both variants sit far below rank_tol and neither can violate.

The sweep therefore varies alpha and kll_k and reports the threshold at which
retained deleted mass becomes visible. A separation region is a positive result
(it validates the KLL±/SpaceSaving± motivation for D7); no separation anywhere is
a publishable negative result and must be reported as one.

Inputs  : data/normalized/<domain>/log.parquet ; config/default.yaml
Outputs : runs/<UTC>_<domain>_reference_d7s_sweep/{summary.json,sweep.csv,log.txt}
Runtime : ~1-4 min per domain (grid of alpha x kll_k, in-process, no containers)
Run as  : python scripts/run/run_d7s_sweep.py --domain all
          python scripts/run/run_d7s_sweep.py --domain environment_airquality \
              --alpha 0.01,0.1,0.3,0.5 --kll-k 64,200,800 --max-rows 200000
"""
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.adapters import canonical_input  # noqa: E402
from src.diagnostics.d7 import run_d7_sketch  # noqa: E402
from src.provenance import run_provenance  # noqa: E402


def _load_build_fixtures():
    """Import scripts/perturb/build_fixtures.py so the sweep reuses the SAME
    restatement generator the promoted fixtures use -- a sweep against a
    re-implementation would measure the re-implementation."""
    path = ROOT / "scripts" / "perturb" / "build_fixtures.py"
    spec = importlib.util.spec_from_file_location("build_fixtures", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


DOMAINS = [
    "environment_airquality",
    "transportation_taxi",
    "datacenter_drives",
    "finance_markets",
    "sports_football",
]

DEFAULT_ALPHA = [0.001, 0.01, 0.05, 0.10, 0.25, 0.50]
DEFAULT_KLL_K = [64, 200, 800]

CSV_FIELDS = [
    "domain", "rows", "alpha", "kll_k", "restatements", "groups_scored",
    "io_qer", "bd_qer", "io_violations", "bd_violations",
    "io_max_excess_err", "bd_max_excess_err",
    "io_mean_rank_err", "bd_mean_rank_err", "separates",
]


def _git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def _config() -> Dict[str, Any]:
    import yaml
    return yaml.safe_load((ROOT / "config/default.yaml").read_text()) or {}


def _sweep_domain(domain: str, alphas: List[float], kll_ks: List[int],
                  max_rows: int, cfg: Dict[str, Any],
                  log: List[str]) -> List[Dict[str, Any]]:
    bf = _load_build_fixtures()
    seed = int(cfg.get("seed", 1337))
    sk = cfg.get("sketch", {}) or {}
    quantiles = tuple(float(q) for q in sk.get("quantiles", (0.5, 0.9, 0.99)))
    rank_tol = float(sk.get("rank_tol", 0.01))
    min_group = int(sk.get("min_group", 20))

    log_path = ROOT / "data/normalized" / domain / "log.parquet"
    base = pd.read_parquet(log_path)
    # Truncate FIRST, then restate: this is the whole point -- restatements are
    # generated inside the window the diagnostic actually reads, so the realised
    # deletion ratio equals the requested alpha.
    if max_rows and len(base) > max_rows:
        base = base.head(max_rows).copy()
    base = canonical_input(base)

    rows: List[Dict[str, Any]] = []
    for alpha in alphas:
        fixture, k_restate = bf.perturb_d7_restate(
            base, alpha, bf._rng(seed, bf._SUBSTREAM["d7"]))
        for kll_k in kll_ks:
            res = run_d7_sketch(fixture, kll_k=kll_k, quantiles=quantiles,
                                rank_tol=rank_tol, min_group=min_group, seed=seed)
            io, bd = res["insert_only"], res["bounded_deletion"]
            # The claim D7 makes: retaining deleted mass costs accuracy that
            # retracting it does not. It is demonstrated only when the
            # insert-only sketch violates while KLL± does not.
            separates = bool(io["violations"] > 0 and bd["violations"] == 0)
            row = {
                "domain": domain, "rows": int(len(fixture)), "alpha": alpha,
                "kll_k": kll_k, "restatements": int(res["restatements"]),
                "groups_scored": int(res["groups_scored"]),
                "io_qer": io["qer"], "bd_qer": bd["qer"],
                "io_violations": io["violations"], "bd_violations": bd["violations"],
                "io_max_excess_err": io["max_excess_err"],
                "bd_max_excess_err": bd["max_excess_err"],
                "io_mean_rank_err": io["mean_rank_err"],
                "bd_mean_rank_err": bd["mean_rank_err"],
                "separates": separates,
            }
            rows.append(row)
            log.append(
                f"  alpha={alpha:<6} k={kll_k:<4} restate={res['restatements']:<6} "
                f"groups={res['groups_scored']:<3} "
                f"io_qer={io['qer']:.3f} bd_qer={bd['qer']:.3f} "
                f"io_excess={io['max_excess_err']:.5f} "
                f"bd_excess={bd['max_excess_err']:.5f} "
                f"{'SEPARATES' if separates else ''}")
            print(log[-1], flush=True)
    return rows


def _campaign_exit_code(*, artifact_valid: bool, separates: bool) -> int:
    """A complete negative grid is evidence, not an execution failure."""
    del separates  # Scientific verdict is reported separately in summary.pass.
    return 0 if artifact_valid else 1


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--domain", default="all",
                    help="Domain name or 'all' (default: all).")
    ap.add_argument("--alpha", default=",".join(str(a) for a in DEFAULT_ALPHA),
                    help="Comma-separated bounded-deletion ratios.")
    ap.add_argument("--kll-k", default=",".join(str(k) for k in DEFAULT_KLL_K),
                    help="Comma-separated KLL sketch sizes.")
    ap.add_argument("--max-rows", type=int, default=200_000,
                    help="Prefix of the base log to restate and replay.")
    args = ap.parse_args()

    domains = DOMAINS if args.domain == "all" else [args.domain]
    unknown = [d for d in domains if d not in DOMAINS]
    if unknown:
        print(f"FAIL: unknown domain(s) {unknown}; known: {DOMAINS}")
        return 1

    alphas = [float(a) for a in args.alpha.split(",") if a.strip()]
    kll_ks = [int(k) for k in args.kll_k.split(",") if k.strip()]
    cfg = _config()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    tag = args.domain if args.domain != "all" else "all"
    out_dir = ROOT / "runs" / f"{stamp}_{tag}_reference_d7s_sweep"
    out_dir.mkdir(parents=True, exist_ok=True)

    log: List[str] = []
    rows: List[Dict[str, Any]] = []
    for domain in domains:
        header = f"==== D7S sweep : {domain} (rows<={args.max_rows}) ===="
        log.append(header)
        print(header, flush=True)
        rows.extend(_sweep_domain(domain, alphas, kll_ks, args.max_rows, cfg, log))

    separating = [r for r in rows if r["separates"]]
    by_domain = {d: sum(1 for r in separating if r["domain"] == d) for d in domains}
    min_alpha = min((r["alpha"] for r in separating), default=None)

    summary = {
        "kind": "d7s_sweep",
        **run_provenance(ROOT),
        "started_utc": stamp,
        "domains": domains,
        "alphas": alphas,
        "kll_ks": kll_ks,
        "max_rows": args.max_rows,
        "cells": len(rows),
        "separating_cells": len(separating),
        "separating_by_domain": by_domain,
        "min_separating_alpha": min_alpha,
        "note": (
            "restatements are generated on each replayed prefix, so the realised "
            "deletion ratio equals alpha; this sweep, not the fixed canonical "
            "fixture point, licenses the D7S sensitivity claim"),
        "rows": rows,
        # A separation region anywhere in the grid licenses the D7S claim: the
        # sweep reports a threshold, not a per-domain guarantee. Domains with no
        # separating cell are reported as such in separating_by_domain.
        "pass": bool(separating),
        "artifact_valid": len(rows) == len(domains) * len(alphas) * len(kll_ks),
    }
    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n")
    with (out_dir / "sweep.csv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        w.writeheader()
        for row in rows:
            w.writerow({k: row[k] for k in CSV_FIELDS})
    (out_dir / "log.txt").write_text("\n".join(log) + "\n")

    passed = bool(separating)
    verdict = "PASS" if passed else "FAIL"
    print(f"\n{verdict}: {len(separating)}/{len(rows)} grid cell(s) separate "
          f"KLL± from insert-only KLL"
          + (f"; smallest separating alpha = {min_alpha}" if separating else
             "; no separation anywhere in the grid -- report as a negative result"))
    silent = [d for d in domains if by_domain[d] == 0]
    if silent:
        noun = "those domains" if len(silent) > 1 else "that domain"
        print(f"no separating cell in: {', '.join(silent)} "
              f"-- report as a negative result for {noun}")
    if not passed:
        print("artifact_valid=True: the complete all-negative grid is retained "
              "as scientific evidence rather than retried until positive")
    print(f"output: {out_dir / 'summary.json'}")
    print(f"output: {out_dir / 'sweep.csv'}")
    print(f"output: {out_dir / 'log.txt'}")
    return _campaign_exit_code(
        artifact_valid=summary["artifact_valid"],
        separates=passed,
    )


if __name__ == "__main__":
    raise SystemExit(main())
