#!/usr/bin/env python3
"""Run native semantic probes that are intentionally separate from D4 and D6.

D4 remains a final-state delayed-arrival inclusion diagnostic and D6 remains a
portable downstream check over each engine aggregate. This runner adds the two
stronger claims only where the adapter executes them natively:

* WM: an event-time watermark closes a window, drops a later arrival, and retains
  a paired on-time control event.
* D6N: an engine-created Top-N materialized view is queried after every append
  checkpoint and compared with the offline oracle trajectory.

Unsupported and not-expressible are capability results. Connection, ingestion,
and read failures are blocked experiments and make the artifact invalid.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sys
import traceback
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.adapters import (  # noqa: E402
    ADAPTER_NAMES, NativeNotExpressible, canonical_input, get_adapter,
)
from src.diagnostics.d6 import compare_topk, topk_table  # noqa: E402
from src.oracle.pandas_oracle import build_oracle_tables_from_rows  # noqa: E402
from src.provenance import run_provenance  # noqa: E402

DOMAINS = (
    "environment_airquality",
    "transportation_taxi",
    "datacenter_drives",
    "finance_markets",
    "sports_football",
)


@lru_cache(maxsize=None)
def _path_sha256(path_text: str) -> str:
    return hashlib.sha256(Path(path_text).read_bytes()).hexdigest()


@lru_cache(maxsize=5)
def _base_prefix(path_text: str, max_rows: int) -> pd.DataFrame:
    frame = pd.read_parquet(path_text)
    if max_rows > 0:
        frame = frame.head(max_rows)
    result = canonical_input(frame)
    result.attrs["_equitable_input_key"] = (path_text, int(max_rows), int(len(result)))
    return result


_ORACLE_CACHE: dict[tuple, dict[str, pd.DataFrame]] = {}


def _oracle(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    identity = df.attrs.get(
        "_equitable_input_key", ("object", id(df), int(len(df))))
    cached = _ORACLE_CACHE.get(identity)
    if cached is not None:
        return cached
    rows = df.copy()
    rows["event_time_utc"] = pd.to_datetime(
        rows["event_time_utc"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    result = build_oracle_tables_from_rows(
        rows.to_dict("records"), include_quantiles=False)
    if len(_ORACLE_CACHE) >= 8:
        _ORACLE_CACHE.clear()
    _ORACLE_CACHE[identity] = result
    return result


def _contract_epsilon() -> float:
    """The contract tolerance from config/default.yaml (default 1e-9), so D6N
    ranks entities tied within tolerance the same way portable D6 does."""
    import yaml
    try:
        cfg = yaml.safe_load((ROOT / "config/default.yaml").read_text()) or {}
    except Exception:
        cfg = {}
    for section in (cfg, cfg.get("comparator", {}) or {}, cfg.get("contract", {}) or {}):
        if isinstance(section, dict) and "epsilon" in section:
            return float(section["epsilon"])
    return 1e-9


def _cuts(n: int, checkpoints: int) -> list[int]:
    return sorted({min(n, max(1, math.ceil(n * i / checkpoints)))
                   for i in range(1, checkpoints + 1)})


def _topk_cell(engine: str, domain: str, max_rows: int, checkpoints: int,
               k: int, rank_metric: str) -> dict[str, Any]:
    adapter = get_adapter(engine)
    base = {
        "kind": "native_topk",
        "engine": engine,
        "domain": domain,
        "mode": adapter.native_topk_mode,
        "k": k,
        "rank_metric": rank_metric,
    }
    if adapter.native_topk_mode != "incremental":
        return {
            **base,
            "status": "unsupported",
            "supported": False,
            "artifact_valid": True,
            "pass": None,
            "reason": "adapter has no engine-maintained incremental Top-N formulation",
            "checkpoints": [],
        }

    path = ROOT / "data" / "normalized" / domain / "log.parquet"
    df = _base_prefix(str(path.resolve()), int(max_rows)).copy(deep=True)
    base.update({
        "input": str(path.relative_to(ROOT)),
        "input_sha256": _path_sha256(str(path.resolve())),
        "rows": int(len(df)),
    })

    eng = None
    phase = "connect"
    points: list[dict[str, Any]] = []
    try:
        eng = adapter.connect()
        phase = "setup"
        eng.configure_native_topk(k, rank_metric)
        eng.setup(variant="dedup")
        start = 0
        for number, cut in enumerate(_cuts(len(df), checkpoints), 1):
            phase = "ingest"
            stats = eng.ingest_log(df.iloc[start:cut])
            phase = "read"
            online_h = eng.read_native_topk("hourly")
            online_d = eng.read_native_topk("daily")
            phase = "oracle"
            prefix = df.iloc[:cut].copy()
            prefix.attrs["_equitable_input_key"] = (
                str(path.resolve()), int(max_rows), int(cut))
            oracle = _oracle(prefix)
            expected_h = topk_table(oracle["hourly"], k, rank_metric)
            expected_d = topk_table(oracle["daily"], k, rank_metric)
            eps = _contract_epsilon()
            hourly = compare_topk(online_h, expected_h, oracle_view=oracle["hourly"],
                                  rank_metric=rank_metric, epsilon=eps)
            daily = compare_topk(online_d, expected_d, oracle_view=oracle["daily"],
                                 rank_metric=rank_metric, epsilon=eps)
            points.append({
                "checkpoint": number,
                "rows": cut,
                "ingest": stats.as_dict(),
                "hourly": hourly,
                "daily": daily,
                "pass": bool(hourly["equivalent"] and daily["equivalent"]),
            })
            start = cut
        passed = bool(points) and all(p["pass"] for p in points)
        return {
            **base,
            "status": "measured",
            "supported": True,
            "artifact_valid": True,
            "trajectory_checkpoints": len(points),
            "checkpoints": points,
            "pass": passed,
        }
    except NativeNotExpressible as exc:
        return {
            **base,
            "status": "not_expressible",
            "supported": False,
            "artifact_valid": True,
            "pass": None,
            "phase": phase,
            "reason": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc()[-2000:],
            "checkpoints": points,
        }
    except Exception as exc:
        return {
            **base,
            "status": "blocked",
            "supported": True,
            "artifact_valid": False,
            "pass": None,
            "phase": phase,
            "reason": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc()[-2000:],
            "checkpoints": points,
        }
    finally:
        if eng is not None:
            try:
                eng.close()
            except Exception:
                pass


def _watermark_cell(engine: str, allowed_lateness_s: int) -> dict[str, Any]:
    adapter = get_adapter(engine)
    base = {
        "kind": "native_watermark",
        "engine": engine,
        "mode": adapter.native_watermark_mode,
        "allowed_lateness_s": allowed_lateness_s,
    }
    if adapter.native_watermark_mode == "unsupported":
        return {
            **base,
            "status": "unsupported",
            "supported": False,
            "artifact_valid": True,
            "pass": None,
            "reason": "adapter has no real event-time watermark probe",
        }
    eng = None
    phase = "connect"
    try:
        eng = adapter.connect()
        phase = "execute"
        result = eng.run_native_watermark_probe(allowed_lateness_s)
        return {
            **base,
            "status": "measured",
            "supported": True,
            "artifact_valid": True,
            **result,
        }
    except Exception as exc:
        return {
            **base,
            "status": "blocked",
            "supported": True,
            "artifact_valid": False,
            "pass": None,
            "phase": phase,
            "reason": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc()[-2000:],
        }
    finally:
        if eng is not None:
            try:
                eng.close()
            except Exception:
                pass


def _flat_rows(topk: list[dict[str, Any]],
               watermark: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for result in topk:
        points = result.get("checkpoints") or [None]
        for point in points:
            row = {
                "kind": "native_topk",
                "engine": result["engine"],
                "domain": result["domain"],
                "status": result["status"],
                "mode": result["mode"],
                "checkpoint": None if point is None else point["checkpoint"],
                "rows": result.get("rows") if point is None else point["rows"],
                "pass": result.get("pass") if point is None else point["pass"],
                "hourly_set_emr": None if point is None else point["hourly"]["set_emr"],
                "hourly_rank_emr": None if point is None else point["hourly"]["rank_emr"],
                "daily_set_emr": None if point is None else point["daily"]["set_emr"],
                "daily_rank_emr": None if point is None else point["daily"]["rank_emr"],
                "reason": result.get("reason"),
            }
            rows.append(row)
    for result in watermark:
        rows.append({
            "kind": "native_watermark",
            "engine": result["engine"],
            "domain": "-",
            "status": result["status"],
            "mode": result["mode"],
            "checkpoint": None,
            "rows": None,
            "pass": result.get("pass"),
            "hourly_set_emr": None,
            "hourly_rank_emr": None,
            "daily_set_emr": None,
            "daily_rank_emr": None,
            "reason": result.get("reason"),
        })
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--engine", default="all",
                    help="one adapter name or all")
    ap.add_argument("--domain", default="all",
                    help="one domain or all")
    ap.add_argument("--probe", choices=("all", "topk", "watermark"), default="all")
    ap.add_argument("--max-rows", type=int, default=20_000)
    ap.add_argument("--checkpoints", type=int, default=4)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--rank-metric", default="mean")
    ap.add_argument("--allowed-lateness-s", type=int, default=300)
    ap.add_argument("--sweep", action="store_true",
                    help="Return success when all cells produced valid artifacts.")
    args = ap.parse_args()

    engines = list(ADAPTER_NAMES) if args.engine == "all" else [args.engine]
    domains = list(DOMAINS) if args.domain == "all" else [args.domain]
    if any(e not in ADAPTER_NAMES for e in engines):
        ap.error(f"unknown engine; known: {ADAPTER_NAMES}")
    if any(d not in DOMAINS for d in domains):
        ap.error(f"unknown domain; known: {DOMAINS}")
    if args.checkpoints < 2:
        ap.error("--checkpoints must be at least 2 to measure a trajectory")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    topk: list[dict[str, Any]] = []
    watermark: list[dict[str, Any]] = []

    if args.probe in ("all", "topk"):
        for domain in domains:
            for engine in engines:
                print(f"native Top-N: {engine} / {domain}", flush=True)
                result = _topk_cell(
                    engine, domain, args.max_rows, args.checkpoints,
                    args.k, args.rank_metric)
                topk.append(result)
                print(f"  {result['status']} pass={result.get('pass')}", flush=True)

    if args.probe in ("all", "watermark"):
        for engine in engines:
            print(f"native watermark: {engine}", flush=True)
            result = _watermark_cell(engine, args.allowed_lateness_s)
            watermark.append(result)
            print(f"  {result['status']} pass={result.get('pass')}", flush=True)

    all_results = topk + watermark
    artifact_valid = bool(all_results) and all(
        r.get("artifact_valid") is True for r in all_results)
    measured = [r for r in all_results if r.get("status") == "measured"]
    # This block is a control on the probe, not on the engines under test. It
    # passes when every cell was classified (nothing blocked) and at least one
    # native formulation was actually measured. An engine whose native view
    # diverges from the oracle is a finding, reported per cell in Tables 13/14 --
    # requiring every engine to agree would make a divergence unpublishable.
    diverging = [r for r in measured if r.get("pass") is not True]
    probe_valid = artifact_valid and bool(measured)
    summary = {
        "kind": "native_semantics",
        **run_provenance(ROOT),
        "started_utc": stamp,
        "finished_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "engines": engines,
        "domains": domains,
        "probe": args.probe,
        "max_rows": args.max_rows,
        "trajectory_checkpoints": args.checkpoints,
        "k": args.k,
        "rank_metric": args.rank_metric,
        "allowed_lateness_s": args.allowed_lateness_s,
        "topk": topk,
        "watermark": watermark,
        "measured_cells": len(measured),
        "classified_cells": len(all_results),
        "diverging_cells": len(diverging),
        "divergences": [
            {k: r.get(k) for k in ("kind", "engine", "domain")} for r in diverging],
        "pass": probe_valid,
        "artifact_valid": artifact_valid,
        "scope_note": (
            "D4 and D6 portable final-state diagnostics remain separate. "
            "This block licenses native watermark and incremental Top-N claims "
            "only for rows whose status is measured. Measured cells whose "
            "native view diverges from the oracle are reported as findings in "
            "diverging_cells and do not invalidate the probe."),
    }

    tag = args.engine if args.engine != "all" else "all"
    out = ROOT / "runs" / f"{stamp}_{tag}_native_semantics"
    out.mkdir(parents=True, exist_ok=True)
    (out / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n")
    flat = _flat_rows(topk, watermark)
    fields = [
        "kind", "engine", "domain", "status", "mode", "checkpoint", "rows",
        "pass", "hourly_set_emr", "hourly_rank_emr", "daily_set_emr",
        "daily_rank_emr", "reason",
    ]
    with (out / "native_semantics.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(flat)
    log_lines = [
        f"{r['kind']} {r['engine']} {r.get('domain', '-')}: "
        f"{r['status']} pass={r.get('pass')} {r.get('reason', '')}"
        for r in all_results
    ]
    (out / "log.txt").write_text("\n".join(log_lines) + "\n")

    print(f"artifact_valid={artifact_valid} probe_valid={probe_valid} "
          f"measured={len(measured)} diverging={len(diverging)}")
    for r in diverging:
        print(f"  DIVERGENCE (reported, non-blocking): {r.get('kind')} "
              f"{r.get('engine')} / {r.get('domain')}")
    print(f"output: {out}")
    return 0 if (artifact_valid if args.sweep
                 else probe_valid) else 1


if __name__ == "__main__":
    raise SystemExit(main())
