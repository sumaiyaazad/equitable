#!/usr/bin/env python3
"""
verify_schema.py -- Prove the domain-neutral schema module works and
generalizes across differently-shaped domains.

Purpose : Exercise src/schema on two synthetic domains with DIFFERENT raw
          layouts (environment_airquality "wide" + finance_markets "long") and assert the neutral
          schema, deduplication, bucketing, and latest-value tiebreak.
Inputs  : none (synthetic in-memory records; no download, stdlib only)
Outputs : runs/<UTC>_verify_schema/summary.json  + PASS/FAIL to stdout
Runtime : < 1 second
Run as  : python scripts/verify/verify_schema.py
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# make 'src' importable when run from repo root
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.schema import (  # noqa: E402
    DomainConfig, normalize_records, build_dedupe_id,
    to_hour_bucket, to_day_bucket, is_newer, validate_row,
    NORMALIZED_COLUMNS, ENTITY, METRIC, VALUE, PROCESSING_ORDER, DEDUPE_ID,
)

checks: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    checks.append((name, bool(ok), detail))


# --- Domain A: environment_airquality, "wide" layout (pm25 + ozone columns per record) ---
aq_config = DomainConfig(
    name="environment_airquality",
    entity_field="station_id",
    event_time_field="obs_time",
    source_id_field="event_id",
    layout="wide",
    metric_fields={"pm25": "pm25_ugm3", "ozone": "ozone_ppb"},
    topic_const="airnow",
)
aq_raw = [
    {"event_id": "aq-1", "station_id": "S1", "obs_time": "2026-04-01T12:34:00Z", "pm25_ugm3": 8.0, "ozone_ppb": 30.0},
    {"event_id": "aq-1", "station_id": "S1", "obs_time": "2026-04-01T12:34:00Z", "pm25_ugm3": 8.0, "ozone_ppb": 30.0},
    {"event_id": "aq-2", "station_id": "S1", "obs_time": "2026-04-01T13:05:00Z", "pm25_ugm3": 9.5, "ozone_ppb": None},
]
aq_rows = normalize_records(aq_raw, aq_config)

# --- Domain B: finance_markets, "long" layout (metric-name + value columns) ---
fin_config = DomainConfig(
    name="finance_markets",
    entity_field="sym",
    event_time_field="ts",
    source_id_field="event_id",
    layout="long",
    metric_field="field",
    value_field="px",
    metric_map={"P": "trade_price", "S": "trade_size"},
    topic_const="iex",
)
fin_raw = [
    {"event_id": "trade-p1", "sym": "AAPL", "ts": "2026-04-01T12:00:01.001Z", "field": "P", "px": 101.25},
    {"event_id": "trade-s1", "sym": "AAPL", "ts": "2026-04-01T12:00:01.002Z", "field": "S", "px": 100},
    {"event_id": "trade-p1", "sym": "AAPL", "ts": "2026-04-01T12:00:01.001Z", "field": "P", "px": 101.25},
]
fin_rows = normalize_records(fin_raw, fin_config)

# --- Assertions ---------------------------------------------------------------
# 1. Neutral schema: every row has exactly the canonical columns.
schema_ok = all(set(r.keys()) == set(NORMALIZED_COLUMNS) for r in aq_rows + fin_rows)
check("neutral_schema_columns", schema_ok, f"expected {NORMALIZED_COLUMNS}")

# 2. Wide explode: record 1 -> 2 rows (pm25+ozone); missing ozone dropped.
#    aq_raw has 3 records: (2 metrics) + (dup, 2 metrics) + (1 metric, ozone None)
#    dedupe happens in the engine, not the normalizer, so normalizer emits all valid rows:
#    row0 -> pm25,ozone (2); row1 -> pm25,ozone (2); row2 -> pm25 only (1) = 5
check("wide_explode_and_skip_missing", len(aq_rows) == 5, f"got {len(aq_rows)} rows (expected 5)")

# 3. entity/metric mapped correctly.
aq_metrics = sorted({r[METRIC] for r in aq_rows})
check("wide_metric_mapping", aq_metrics == ["ozone", "pm25"], f"got {aq_metrics}")
fin_metrics = sorted({r[METRIC] for r in fin_rows})
check("long_metric_mapping", fin_metrics == ["trade_price", "trade_size"], f"got {fin_metrics}")
check("entity_mapping", {r[ENTITY] for r in aq_rows} == {"S1"}
      and {r[ENTITY] for r in fin_rows} == {"AAPL"}, "entity field")

# 4. Deterministic dedupe id: byte-identical inputs -> identical id; distinct -> distinct.
d_first = build_dedupe_id("aq-1", "pm25", namespace="environment_airquality|S1")
d_dup = build_dedupe_id("aq-1", "pm25", namespace="environment_airquality|S1")
d_diff = build_dedupe_id("aq-2", "pm25", namespace="environment_airquality|S1")
check("dedupe_id_deterministic", d_first == d_dup and d_first != d_diff, "sha256 identity")
# the duplicate finance_markets trade must share its dedupe id with the original
fin_price_ids = [r[DEDUPE_ID] for r in fin_rows if r[METRIC] == "trade_price"]
check("duplicate_shares_dedupe_id", len(fin_price_ids) == 2 and fin_price_ids[0] == fin_price_ids[1],
      "identical trade -> identical id")

# 5. Bucketing.
check("hour_bucket", to_hour_bucket("2026-04-01T12:34:56Z") == "2026-04-01T12:00:00Z")
check("day_bucket", to_day_bucket("2026-04-01T12:34:56Z") == "2026-04-01")
check("bucket_rejects_garbage", to_hour_bucket("not-a-time") is None)

# 6. Latest-value tiebreak: newer event wins; equal event -> stable id wins.
check("is_newer_by_time", is_newer("2026-04-01T13:00:00Z", "a", "2026-04-01T12:00:00Z", "z") is True)
check("is_newer_tiebreak_id", is_newer("2026-04-01T12:00:00Z", "z", "2026-04-01T12:00:00Z", "a") is True)
check("is_newer_older_loses", is_newer("2026-04-01T11:00:00Z", "z", "2026-04-01T12:00:00Z", "a") is False)

# 7. validate_row accepts good rows, flags bad.
check("validate_accepts_good", validate_row(aq_rows[0]) is None)
d_equal_content_other_source = build_dedupe_id(
    "aq-3", "pm25", namespace="environment_airquality|S1"
)
check("equal_content_distinct_source_identity", d_equal_content_other_source != d_first,
      "source identity, not content, defines a duplicate")
check("validate_flags_missing", validate_row({ENTITY: "x"}) is not None)

fractional_order = {**aq_rows[0], PROCESSING_ORDER: 1.5}
check("validate_rejects_fractional_processing_order",
      validate_row(fractional_order) == "invalid_processing_order")
# --- Report -------------------------------------------------------------------
passed = sum(1 for _, ok, _ in checks if ok)
total = len(checks)
all_ok = passed == total

stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
out_dir = Path(__file__).resolve().parents[2] / "runs" / f"{stamp}_verify_schema"
out_dir.mkdir(parents=True, exist_ok=True)
summary = {
    "script": "scripts/verify/verify_schema.py",
    "timestamp_utc": stamp,
    "passed": passed,
    "total": total,
    "all_ok": all_ok,
    "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in checks],
    "sample_environment_airquality_row": aq_rows[0],
    "sample_finance_markets_row": fin_rows[0],
}
(out_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

print(f"\nSchema verification: {passed}/{total} checks passed")
for name, ok, detail in checks:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  -- {detail}" if not ok else ""))
print(f"\nwrote {out_dir / 'summary.json'}")
print("RESULT:", "PASS" if all_ok else "FAIL")
sys.exit(0 if all_ok else 1)
