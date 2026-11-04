#!/usr/bin/env python3
"""
verify_engine_oracle.py -- Prove online--oracle equivalence on the neutral schema.

Purpose : Build a small synthetic normalized log (entity/metric) with a
          byte-identical duplicate and an out-of-order row, feed it to the
          online ExactEngine and to two independent offline oracles (pandas +
          DuckDB), and assert all three agree (EMR = 0) on the hourly and daily
          tables. The DuckDB oracle shares no code with the engine, so
          engine==DuckDB is a genuine cross-check. Also checks duplicate
          suppression. It independently corrupts each of the three outputs and requires the other two implementations plus the union-key and non-finite comparator to reject the fault. DuckDB checks are skipped with a notice if the package is absent.
Inputs  : none (synthetic in-memory rows; requires pandas)
Outputs : runs/<UTC>_verify_engine_oracle/summary.json  + PASS/FAIL to stdout
Runtime : < 2 seconds
Run as  : python scripts/verify/verify_engine_oracle.py
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.schema import normalize_records, DomainConfig                            # noqa: E402
from src.engine.exact_engine import ExactEngine                                   # noqa: E402
from src.oracle.pandas_oracle import build_oracle_tables_from_rows as pandas_oracle  # noqa: E402
from src.oracle.comparator import compare_bucket_tables, engine_bucket_table       # noqa: E402

checks: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    checks.append((name, bool(ok), detail))


# --- Build a synthetic normalized log via the schema module (finance_markets "long") --
cfg = DomainConfig(
    name="finance_markets", entity_field="sym", event_time_field="ts",
    source_id_field="event_id",
    layout="long", metric_field="field", value_field="px",
    metric_map={"P": "trade_price"}, topic_const="iex",
)
raw = [
    {"event_id": "t1", "sym": "AAPL", "ts": "2026-04-01T12:00:05.001Z", "field": "P", "px": 101.0},
    {"event_id": "t2", "sym": "AAPL", "ts": "2026-04-01T12:15:00.002Z", "field": "P", "px": 103.0},
    {"event_id": "t2", "sym": "AAPL", "ts": "2026-04-01T12:15:00.002Z", "field": "P", "px": 103.0},
    {"event_id": "t3", "sym": "MSFT", "ts": "2026-04-01T12:05:00.003Z", "field": "P", "px": 200.0},
    {"event_id": "t4", "sym": "AAPL", "ts": "2026-04-01T12:02:00.004Z", "field": "P", "px": 100.0},
    {"event_id": "t5", "sym": "AAPL", "ts": "2026-04-01T13:30:00.005Z", "field": "P", "px": 105.0},
]
rows = normalize_records(raw, cfg)
check("normalizer_emitted_rows", len(rows) == 6, f"got {len(rows)} (expected 6, incl. dup)")

# --- Online: feed rows one at a time to the engine ---------------------------
engine = ExactEngine()
engine.ingest_rows(rows)
health = engine.get_stream_health()
check("engine_suppressed_duplicate", health["duplicate_rows"] == 1, f"dupes={health['duplicate_rows']}")
check("engine_counted_5_unique", health["rows_ingested"] == 5, f"ingested={health['rows_ingested']}")

online_hourly = engine_bucket_table(engine.get_hourly())
online_daily = engine_bucket_table(engine.get_daily())

# --- Offline oracle 1: pandas -------------------------------------------------
pan = pandas_oracle(rows, include_quantiles=False)
pan_with_quantiles = pandas_oracle(rows)
check("pandas_exact_only_skips_unused_quantiles",
      pan["summary"]["quantiles_included"] is False and
      pan["hourly_quantiles"].empty and pan["daily_quantiles"].empty,
      "exact-only mode emitted a quantile table")
check("pandas_quantile_mode_remains_available",
      pan_with_quantiles["summary"]["quantiles_included"] is True and
      not pan_with_quantiles["hourly_quantiles"].empty and
      not pan_with_quantiles["daily_quantiles"].empty and
      pan_with_quantiles["hourly"].equals(pan["hourly"]) and
      pan_with_quantiles["daily"].equals(pan["daily"]),
      "quantile mode changed exact tables or emitted no quantiles")
check("pandas_oracle_suppressed_duplicate", pan["summary"]["duplicate_rows"] == 1,
      f"dupes={pan['summary']['duplicate_rows']}")

# Structural comparator controls: duplicate keys are one bad union key even if
# that same key also carries a field mismatch.
one = online_hourly.head(1).copy()
duplicate = pd.concat([one, one], ignore_index=True)
duplicate_cmp = compare_bucket_tables(duplicate, one)
check("comparator_duplicate_key_is_structural_mismatch",
      duplicate_cmp["duplicate_online_keys"] == 1 and
      duplicate_cmp["total_keys"] == 1 and
      duplicate_cmp["mismatched_keys"] == 1 and
      duplicate_cmp["matched_keys"] == 0, str(duplicate_cmp))
duplicate_bad = duplicate.copy()
duplicate_bad.loc[0, "sum"] = float(duplicate_bad.loc[0, "sum"]) + 1.0
duplicate_bad_cmp = compare_bucket_tables(duplicate_bad, one)
check("comparator_duplicate_and_field_fault_count_once",
      duplicate_bad_cmp["duplicate_online_keys"] == 1 and
      duplicate_bad_cmp["field_mismatches"]["sum"] == 1 and
      duplicate_bad_cmp["mismatched_keys"] == 1, str(duplicate_bad_cmp))

# --- Compare online engine vs pandas oracle (the equivalence contract) --------
hourly_cmp = compare_bucket_tables(online_hourly, pan["hourly"])
daily_cmp = compare_bucket_tables(online_daily, pan["daily"])
check("engine_vs_pandas_hourly_EMR_zero", hourly_cmp["equivalent"], f"cmp={hourly_cmp}")
check("engine_vs_pandas_daily_EMR_zero", daily_cmp["equivalent"], f"cmp={daily_cmp}")

# --- Offline oracle 2: DuckDB (independent SQL) -> triangulation ---------------
duck_cmp_h = duck_cmp_d = None
duckdb_available = True
try:
    from src.oracle.duckdb_oracle import build_oracle_tables_from_rows as duckdb_oracle  # noqa: E402
    duck = duckdb_oracle(rows)
    duck_cmp_h = compare_bucket_tables(online_hourly, duck["hourly"])
    duck_cmp_d = compare_bucket_tables(online_daily, duck["daily"])
    pan_vs_duck_h = compare_bucket_tables(pan["hourly"], duck["hourly"])
    check("engine_vs_duckdb_hourly_EMR_zero", duck_cmp_h["equivalent"], f"cmp={duck_cmp_h}")
    check("engine_vs_duckdb_daily_EMR_zero", duck_cmp_d["equivalent"], f"cmp={duck_cmp_d}")
    check("pandas_vs_duckdb_hourly_EMR_zero", pan_vs_duck_h["equivalent"], f"cmp={pan_vs_duck_h}")

    # Fault-injection guard: independently corrupt each implementation output.
    # The other two implementations and the strict comparator must reject it.
    pan_fault = pan["hourly"].copy()
    pan_fault.loc[pan_fault.index[0], "sum"] += 1.0
    pan_fault_engine = compare_bucket_tables(online_hourly, pan_fault)
    pan_fault_duck = compare_bucket_tables(duck["hourly"], pan_fault)
    check("pandas_fault_rejected_by_engine_and_duckdb",
          not pan_fault_engine["equivalent"] and not pan_fault_duck["equivalent"],
          f"engine={pan_fault_engine}; duckdb={pan_fault_duck}")

    duck_fault = duck["hourly"].iloc[1:].copy()
    duck_fault_engine = compare_bucket_tables(online_hourly, duck_fault)
    duck_fault_pan = compare_bucket_tables(pan["hourly"], duck_fault)
    check("duckdb_missing_key_rejected_by_engine_and_pandas",
          not duck_fault_engine["equivalent"] and not duck_fault_pan["equivalent"],
          f"engine={duck_fault_engine}; pandas={duck_fault_pan}")

    online_fault = online_hourly.copy()
    online_fault.loc[online_fault.index[0], "mean"] = float("nan")
    online_fault_pan = compare_bucket_tables(online_fault, pan["hourly"])
    online_fault_duck = compare_bucket_tables(online_fault, duck["hourly"])
    check("online_nonfinite_fault_rejected_by_both_oracles",
          not online_fault_pan["equivalent"] and not online_fault_duck["equivalent"],
          f"pandas={online_fault_pan}; duckdb={online_fault_duck}")
except ImportError:
    duckdb_available = False
    print("\n  NOTE: duckdb not installed -- independent co-oracle checks SKIPPED."
          "\n        install with: pip install duckdb\n")

# --- Spot-check a known aggregate: AAPL trade_price, hour 12:00 ---------------
# unique AAPL@12:xx prices: 101, 103, 100  (dup 103 suppressed) -> count=3, sum=304,
# min=100, max=103; latest by event_time = 103 (12:15).
aapl_12 = [r for r in engine.get_hourly(entity="AAPL", metric="trade_price")
           if r["bucket"] == "2026-04-01T12:00:00Z"]
ok_spot = (len(aapl_12) == 1 and aapl_12[0]["count"] == 3
           and abs(aapl_12[0]["sum"] - 304.0) < 1e-9
           and abs(aapl_12[0]["min"] - 100.0) < 1e-9
           and abs(aapl_12[0]["max"] - 103.0) < 1e-9
           and abs(aapl_12[0]["latest_value"] - 103.0) < 1e-9)
check("spot_check_AAPL_hour12", ok_spot, f"row={aapl_12}")

# --- Report -------------------------------------------------------------------
passed = sum(1 for _, ok, _ in checks if ok)
total = len(checks)
all_ok = passed == total

stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
out_dir = Path(__file__).resolve().parents[2] / "runs" / f"{stamp}_verify_engine_oracle"
out_dir.mkdir(parents=True, exist_ok=True)
(out_dir / "summary.json").write_text(json.dumps({
    "script": "scripts/verify/verify_engine_oracle.py",
    "timestamp_utc": stamp,
    "passed": passed, "total": total, "all_ok": all_ok,
    "duckdb_available": duckdb_available,
    "engine_vs_pandas_hourly": hourly_cmp, "engine_vs_pandas_daily": daily_cmp,
    "engine_vs_duckdb_hourly": duck_cmp_h, "engine_vs_duckdb_daily": duck_cmp_d,
    "engine_health": health,
    "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in checks],
}, indent=2, default=str) + "\n")

print(f"\nEngine/oracle equivalence: {passed}/{total} checks passed")
for name, ok, detail in checks:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  -- {detail}" if not ok else ""))
print(f"\nwrote {out_dir / 'summary.json'}")
print("RESULT:", "PASS" if all_ok else "FAIL")
sys.exit(0 if all_ok else 1)
