#!/usr/bin/env python3
"""Does ksqlDB's D1 divergence come from the commit interval?

The `dedup` formulation aggregates the deduplication TABLE's changelog as an
append stream. Kafka Streams buffers table updates and flushes on a commit
interval, so two updates to one row inside one interval collapse into a single
changelog record and the duplicate disappears, while two updates across an
interval boundary reach the aggregate as two records and the value is counted
twice. A four-event witness shows both outcomes.

This script asks whether that mechanism accounts for the whole 50k divergence.
It replays the same D1 taxi fixture twice against the same oracle table: once at
the server's own settings, once with the commit interval and cache raised above
the ingest duration so every duplicate update coalesces.

    default settings -> expect the promoted number, 3722 of 7013 keys
    raised interval  -> if this collapses toward 0, the mechanism is the whole
                        story; if it stays high, something else is also wrong

Reads only. Writes nothing under runs/ or results/, so it cannot touch promoted
evidence or a campaign in progress. It does create ksqlDB objects under a
run-scoped name prefix on the shared server and drops them afterwards.

Run as: .venv/bin/python scripts/diagnose_ksqldb_d1.py
"""
from __future__ import annotations

import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

from src.adapters.ksqldb import KsqlDbAdapter  # noqa: E402
from src.oracle.comparator import compare_bucket_tables  # noqa: E402

FIXTURE = ROOT / "data/fixtures/transportation_taxi/d1_dup.parquet"
ORACLE = ROOT / ("data/oracle_cache/"
                 "5a851420834d1063e7fb3ca4700108e8cf289fdad4969878a0c8d7651e6f759d/"
                 "hourly.parquet")
ROWS = 50_000

#: Ingest measured at ~3.1k rows/s, so 50k rows take ~16s. Both values must
#: exceed that window: the interval so no flush is due, the cache so no
#: eviction forces one anyway.
RAISED = {
    "ksql.streams.commit.interval.ms": "120000",
    "ksql.streams.cache.max.bytes.buffering": "268435456",
}


def run(label: str, props: dict) -> None:
    log = pd.read_parquet(FIXTURE).head(ROWS)
    # D1 fixtures carry no restatement column; the adapter's producer reads it.
    if "supersedes" not in log.columns:
        log = log.assign(supersedes=None)
    oracle = pd.read_parquet(ORACLE)
    adapter = KsqlDbAdapter(
        run_suffix=f"dx{uuid.uuid4().hex[:8]}",
        streams_properties=props,
        materialize_wait_s=15,
        read_stable_timeout_s=300,
    )
    try:
        adapter.connect()
        adapter.setup(variant="dedup")
        stats = adapter.ingest_log(log)
        online = adapter.read_view("hourly")
        result = compare_bucket_tables(online, oracle)
        wrong = result.get("mismatched_keys")
        total = result.get("total_keys")
        print(f"{label:18s} ingest={stats.seconds:6.1f}s  "
              f"mismatched={wrong} of {total}")
    finally:
        try:
            adapter.close()
        except Exception:
            pass


def main() -> int:
    if not FIXTURE.exists() or not ORACLE.exists():
        print("missing fixture or cached oracle table; nothing to compare")
        return 1
    print(f"D1 taxi, {ROWS:,} rows, ksqlDB dedup formulation")
    print("promoted for this cell: 12 of 14 duplicate-corrupted keys survive the")
    print("repair. The other 3,710 divergent keys are the arrival-ordered latest")
    print("rule, which this test does not touch.\n")
    run("server default", {})
    run("raised interval", RAISED)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
