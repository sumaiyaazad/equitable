#!/usr/bin/env python3
"""
smoke.py -- per-engine adapter smoke test (Step 5 acceptance).

Prove that one engine's adapter is correctly wired end-to-end: it connects,
builds its views, ingests a small slice of a real normalized log, and its
hourly+daily aggregate views match the in-process oracle computed on the SAME
slice. This is the "does the adapter compute the contract at all" check that
gates every engine before the full diagnostic matrix (Step 6/7).

The slice is fed in event-time order (arrival == event order), so a PASS is pure
aggregation correctness with no perturbation in play -- duplicates, reorder, and
lateness are the job of the Step 3 fixtures + the Step 6 diagnostics, not this
smoke test. An engine's genuine perturbation behavior (e.g. ksqlDB offset-latest)
is therefore not exercised here and must not fail a clean smoke.

Engines and what they need running first:
  reference   -- nothing (in-process); runnable immediately.
  materialize -- docker compose -f docker/streaming-engines.yml up -d materialize
  risingwave  -- ... up -d risingwave
  ksqldb      -- ... up -d kafka ksqldb
  flink       -- ... up -d kafka flink-jobmanager flink-taskmanager flink-sql-gateway
  spark       -- ... up -d spark-master spark-worker spark-driver
  feldera     -- docker compose -f docker/streaming-engines.yml up -d feldera

Documented findings identify reproducible divergence signatures. Attribution
never changes the scientific verdict: only clean equivalence passes. A valid
artifact that reproduces a known signature is nevertheless accepted by the
operational gate, because the benchmark matrix records that divergence as
evidence. Unknown divergence and execution errors block.

Inputs  : data/normalized/<domain>/log.parquet (a slice of it)
Outputs : runs/<UTC>_<domain>_<engine>_smoke/{summary.json,log.txt} ; PASS/FAIL
Runtime : seconds (reference) to a couple of minutes (containers)
Run as  : python scripts/adapters/smoke.py --engine reference
          python scripts/adapters/smoke.py --engine materialize --domain finance_markets
          python scripts/adapters/smoke.py --all           # every engine, continue on failure
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.adapters import ADAPTER_NAMES, get_adapter, canonical_input  # noqa: E402
from src.oracle.pandas_oracle import build_oracle_tables_from_rows  # noqa: E402
from src.oracle.comparator import compare_bucket_tables  # noqa: E402
from src.provenance import run_provenance  # noqa: E402


def _git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _load_slice(domain: str, rows: int) -> tuple[pd.DataFrame, Path]:
    log_path = ROOT / "data/normalized" / domain / "log.parquet"
    if not log_path.exists():
        raise FileNotFoundError(f"{log_path} not found -- run the Step 2 ingest for '{domain}' first.")
    df = pd.read_parquet(log_path)
    if rows > 0:
        df = df.head(rows)
    # Feed in event-time order so a clean smoke isolates aggregation correctness.
    df = canonical_input(df).sort_values(
        ["event_time_utc", "processing_order"], kind="stable"
    ).reset_index(drop=True)
    return df, log_path


# Known signatures explain a strict failure; they never turn it into a pass.
from src.diagnostics.findings import match_known_finding  # noqa: E402


def smoke_gate_accepts(summary: dict) -> bool:
    """Whether a smoke result proves the adapter is safe to benchmark.

    Keep this distinct from ``pass``: that field is the strict scientific
    equivalence verdict and must remain false for a documented divergence.
    """
    finding = summary.get("known_finding") or {}
    return bool(
        summary.get("artifact_valid") is True
        and not summary.get("error")
        and (
            summary.get("pass") is True
            or finding.get("reproduced") is True
        )
    )


def run_one(engine: str, domain: str, rows: int, variant: str) -> dict:
    df, log_path = _load_slice(domain, rows)
    epsilon = 1e-9

    adapter = get_adapter(engine)
    if variant not in adapter.supported_variants:
        variant = adapter.supported_variants[0]

    started = datetime.now(timezone.utc)
    with adapter.connect() as eng:
        eng.setup(variant=variant)
        stats = eng.ingest_log(df)
        engine_hourly = eng.read_view("hourly")
        engine_daily = eng.read_view("daily")

    oracle = build_oracle_tables_from_rows(df.to_dict("records"), include_quantiles=False)
    cmp_hourly = compare_bucket_tables(engine_hourly, oracle["hourly"], epsilon=epsilon)
    cmp_daily = compare_bucket_tables(engine_daily, oracle["daily"], epsilon=epsilon)
    clean = bool(cmp_hourly["equivalent"] and cmp_daily["equivalent"])

    # Attribute a known signature while retaining the strict failure verdict.
    known_finding = None
    if not clean:
        known_finding = match_known_finding(engine, engine_hourly, engine_daily,
                                            oracle["hourly"], oracle["daily"],
                                            cmp_hourly, cmp_daily)
    passed = clean

    return {
        "engine": engine,
        "domain": domain,
        "variant": variant,
        "rows": int(len(df)),
        "input_log": str(log_path),
        "input_sha256": _sha256_file(log_path),
        "epsilon": epsilon,
        "seed": None,
        **run_provenance(ROOT),
        "started_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "finished_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ingest": stats.as_dict(),
        "hourly": cmp_hourly,
        "daily": cmp_daily,
        "clean_equivalence": clean,
        "known_finding": known_finding,
        "pass": passed,
        "artifact_valid": True,
    }


def _write_run(summary: dict, log_lines: list[str]) -> Path:
    out_dir = ROOT / "runs" / f"{_utc_stamp()}_{summary['domain']}_{summary['engine']}_smoke"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    (out_dir / "log.txt").write_text("\n".join(log_lines) + "\n")
    return out_dir


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--engine", choices=ADAPTER_NAMES, help="Single engine to smoke-test.")
    g.add_argument("--all", action="store_true", help="Run every registered engine (continue on failure).")
    ap.add_argument("--domain", default="environment_airquality")
    ap.add_argument("--rows", type=int, default=20000, help="Slice size (0 = whole log).")
    ap.add_argument("--variant", default="dedup", choices=["dedup", "naive"])
    args = ap.parse_args()

    engines = list(ADAPTER_NAMES) if args.all else [args.engine]
    overall_ok = True
    for engine in engines:
        line = f"==== smoke: {engine} on {args.domain} (rows={args.rows}, variant={args.variant}) ===="
        print(line)
        try:
            summary = run_one(engine, args.domain, args.rows, args.variant)
            kf = summary.get("known_finding")
            if not summary["pass"] and kf:
                verdict = f"FAIL (known divergence: {kf['summary']})"
            elif summary["pass"]:
                verdict = "PASS"
            else:
                verdict = "FAIL"
            body = [
                line,
                f"ingest: {summary['ingest']['rows_sent']} rows @ {summary['ingest']['rows_per_sec']:.1f}/s",
                f"hourly: equivalent={summary['hourly']['equivalent']} emr={summary['hourly']['emr']:.3g} "
                f"only_online={summary['hourly']['only_in_online']} only_oracle={summary['hourly']['only_in_oracle']} "
                f"field_mismatches={summary['hourly']['field_mismatches']}",
                f"daily:  equivalent={summary['daily']['equivalent']} emr={summary['daily']['emr']:.3g}",
            ]
            if kf:
                body.append(f"known finding: only {sorted(kf['diverging_fields'])} diverge, "
                            f"within the accumulation-model band "
                            f"({kf['count_safety']:g}*N*2^-24, per-bucket count N); "
                            f"patch: {kf['recovering_patch']}")
            body.append(f"VERDICT: {verdict}")
            out_dir = _write_run(summary, body)
            for b in body[1:]:
                print("  " + b)
            print(f"  run: {out_dir}")
            accepted = smoke_gate_accepts(summary)
            if not summary["pass"]:
                print(f"  GATE: {'ACCEPTED' if accepted else 'BLOCKED'}")
            overall_ok = overall_ok and accepted
        except Exception as exc:  # connection refused, missing driver, etc.
            overall_ok = False
            tb = traceback.format_exc()
            summary = {"engine": engine, "domain": args.domain, "pass": False,
                       "artifact_valid": False, "error": str(exc),
                       **run_provenance(ROOT)}
            out_dir = _write_run(summary, [line, "ERROR:", tb])
            print(f"  ERROR: {exc}")
            print(f"  (is the {engine} service up / driver installed? see the header.)")
            print(f"  run: {out_dir}")

    print(f"\n{'PASS' if overall_ok else 'FAIL'}: smoke-tested {len(engines)} engine(s)")
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
