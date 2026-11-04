#!/usr/bin/env python3
"""
run_oracle_verification.py -- Real-scale online--oracle equivalence on a
normalized domain log. Feeds the log to the incremental engine and to BOTH
independent oracles (pandas + DuckDB), then compares all three pairwise on the
hourly and daily aggregate tables. Zero mismatches => the equivalence contract
holds on real data (a headline paper number).

Inputs  : data/normalized/<domain>/log.parquet  (produced by scripts/ingest/)
Outputs : runs/<UTC>_oracle_verification_<domain>/summary.json ; PASS/FAIL
Runtime : ~seconds-minutes depending on row count
Run as  : python scripts/run/run_oracle_verification.py --domain environment_airquality
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.engine.exact_engine import ExactEngine, load_rows_from_path            # noqa: E402
from src.oracle.cache import store_oracle_tables                                # noqa: E402
from src.oracle.pandas_oracle import build_oracle_tables_from_rows as pandas_oracle   # noqa: E402
from src.oracle.duckdb_oracle import build_oracle_tables_from_rows as duckdb_oracle   # noqa: E402
from src.oracle.comparator import compare_bucket_tables, engine_bucket_table    # noqa: E402
from src.provenance import run_provenance                                      # noqa: E402
from src.schema.artifacts import artifact_sha256                               # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--domain", default="environment_airquality")
    ap.add_argument("--log", help="override path to normalized log.parquet")
    ap.add_argument("--max-rows", type=int, default=0, help="0 = all rows")
    args = ap.parse_args()
    started_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    log_path = args.log or f"data/normalized/{args.domain}/log.parquet"
    if not Path(log_path).exists():
        raise SystemExit(f"FAIL: {log_path} not found -- run the ingest for '{args.domain}' first")
    log_file = Path(log_path).resolve()
    input_sha256 = artifact_sha256(log_file)

    print(f"loading {log_path} ...", flush=True)
    rows = load_rows_from_path(log_path)
    if args.max_rows:
        rows = rows[: args.max_rows]
    n = len(rows)
    print(f"  {n:,} rows")

    # --- online engine (chunked, with progress) ------------------------------
    t0 = time.perf_counter()
    engine = ExactEngine()
    CHUNK = 100_000
    for i in range(0, n, CHUNK):
        engine.ingest_rows(rows[i:i + CHUNK])
        done = min(i + CHUNK, n)
        rate = done / max(time.perf_counter() - t0, 1e-9)
        print(f"    engine ingested {done:,}/{n:,}  ({rate:,.0f} rows/s)", flush=True)
    t_engine = time.perf_counter() - t0
    health = engine.get_stream_health()
    online_h = engine_bucket_table(engine.get_hourly())
    online_d = engine_bucket_table(engine.get_daily())
    print(f"  engine: {t_engine:.1f}s  ({n / max(t_engine, 1e-9):,.0f} rows/s), "
          f"hourly_keys={len(online_h):,} daily_keys={len(online_d):,} "
          f"dupes={health['duplicate_rows']:,}", flush=True)

    # --- offline oracles -----------------------------------------------------
    t0 = time.perf_counter()
    pan = pandas_oracle(rows, include_quantiles=False)
    t_pandas = time.perf_counter() - t0
    t0 = time.perf_counter()
    duck = duckdb_oracle(rows)
    t_duck = time.perf_counter() - t0
    print(f"  pandas oracle: {t_pandas:.1f}s   duckdb oracle: {t_duck:.1f}s", flush=True)

    # --- pairwise comparisons ------------------------------------------------
    pairs = {
        "engine_vs_pandas_hourly": compare_bucket_tables(online_h, pan["hourly"]),
        "engine_vs_pandas_daily": compare_bucket_tables(online_d, pan["daily"]),
        "engine_vs_duckdb_hourly": compare_bucket_tables(online_h, duck["hourly"]),
        "engine_vs_duckdb_daily": compare_bucket_tables(online_d, duck["daily"]),
        "pandas_vs_duckdb_hourly": compare_bucket_tables(pan["hourly"], duck["hourly"]),
        "pandas_vs_duckdb_daily": compare_bucket_tables(pan["daily"], duck["daily"]),
    }
    all_ok = all(c["equivalent"] for c in pairs.values())
    co_result = {
        "hourly_equivalent": pairs["pandas_vs_duckdb_hourly"]["equivalent"],
        "daily_equivalent": pairs["pandas_vs_duckdb_daily"]["equivalent"],
        "hourly_mismatched_keys":
            pairs["pandas_vs_duckdb_hourly"]["mismatched_keys"],
        "daily_mismatched_keys":
            pairs["pandas_vs_duckdb_daily"]["mismatched_keys"],
        "agree": bool(
            pairs["pandas_vs_duckdb_hourly"]["equivalent"]
            and pairs["pandas_vs_duckdb_daily"]["equivalent"]),
    }
    oracle_cache = store_oracle_tables(
        pan, co_result,
        root=ROOT,
        source_sha256=input_sha256,
        selection={"mode": "prefix", "actual_rows": n},
        restatement_aware=False,
        build_s=t_pandas + t_duck,
    )

    # --- report --------------------------------------------------------------
    finished_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    out_dir = ROOT / "runs" / f"{stamp}_oracle_verification_{args.domain}"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "summary.json").write_text(json.dumps({
        "domain": args.domain, "log_path": str(log_file),
        "input": str(log_file),
        "rows": n, "hourly_keys": int(len(online_h)), "daily_keys": int(len(online_d)),
        "kind": "oracle_verification",
        "pass": all_ok,
        "artifact_valid": all_ok,
        "started_utc": started_utc,
        "finished_utc": finished_utc,
        **run_provenance(ROOT),
        "input_sha256": input_sha256,
        "oracle_cache_keys": [oracle_cache["oracle_cache_key"]],
        "oracle_cache": oracle_cache,
        "engine_health": health,
        "timings_s": {"engine": t_engine, "pandas": t_pandas, "duckdb": t_duck},
        "comparisons": pairs, "all_equivalent": all_ok,
    }, indent=2, default=str) + "\n")

    print(f"\nOracle verification -- domain={args.domain}, rows={n:,}")
    for name, c in pairs.items():
        tag = "PASS" if c["equivalent"] else "FAIL"
        print(f"  [{tag}] {name:26s} matched={c['matched_keys']:,} "
              f"mismatched={c['mismatched_keys']} EMR={c['emr']:.2e}")
    print(f"\nwrote {out_dir / 'summary.json'}")
    print("RESULT:", "PASS -- engine == pandas == duckdb (zero mismatches)" if all_ok else "FAIL")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
