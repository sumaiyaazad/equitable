#!/usr/bin/env python3
"""
ingest_transportation_taxi.py -- Download + normalize NYC TLC yellow-taxi trip records to the
neutral schema, using the SAME folder + code convention as every other domain:
    data/raw/transportation_taxi/        <- raw monthly parquet files (downloaded)
    data/normalized/transportation_taxi/ <- log.parquet + manifest.json

Source: NYC TLC Trip Record Data (public), monthly yellow-taxi parquet files:
    https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet
Lineage: DEBS Grand Challenge 2015; IDEBench (transportation).

One completed trip is one event, timed at its dropoff. Each trip explodes into
three neutral metrics (WIDE layout):
    fare          <- fare_amount        (USD)
    trip_distance <- trip_distance      (miles)
    duration      <- dropoff - pickup   (seconds, derived before the contract)
keyed by pickup zone (PULocationID, ~265 TLC zones).

Time basis: TLC publishes naive America/New_York wall-clock timestamps. We
localize them with the IANA timezone and convert them to UTC. Ambiguous or
nonexistent DST wall times are rejected because they do not identify one
unambiguous source event instant. The rule is recorded in the output manifest
and docs/DATASETS.md.

Primary mode (like every other domain):
  --from-raw --start YYYY-MM --end YYYY-MM
        Download each monthly parquet in [start, end] (inclusive) into
        data/raw/transportation_taxi/ and normalize via the generic src.schema path. Needs
        internet.

Optional shortcut:
  --from-parquet PATH [PATH ...]
        Normalize already-downloaded monthly parquet file(s) (no download).

Inputs  : NYC TLC monthly parquet over the network (or local parquet files)
Outputs : data/normalized/transportation_taxi/log.parquet + manifest.json ; PASS/FAIL
Runtime : from-raw ~ a few seconds per month to download + parse (~3M trips/mo
          -> ~9M normalized rows/mo; pick a short window for a first run)
Run as  : python scripts/ingest/ingest_transportation_taxi.py --from-raw \
              --start 2024-01 --end 2024-01
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.schema import normalize_records, DomainConfig  # noqa: E402
from scripts.ingest._ingest_common import write_normalized_log  # noqa: E402

DOMAIN = "transportation_taxi"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# TLC yellow-taxi columns we rely on.
COL_PICKUP = "tpep_pickup_datetime"
COL_DROPOFF = "tpep_dropoff_datetime"
COL_ZONE = "PULocationID"
COL_FARE = "fare_amount"
COL_DIST = "trip_distance"

# Sanity bounds -- reject out-of-contract trips at the normalization boundary.
MAX_DURATION_S = 24 * 3600          # trips longer than a day are data errors
MIN_ZONE, MAX_ZONE = 1, 265         # valid TLC taxi-zone ids


def _domain_config() -> DomainConfig:
    # WIDE layout: one trip carries several metric columns; each explodes into
    # its own neutral row. Field names below are the columns produced by _parse.
    return DomainConfig(
        name=DOMAIN,
        entity_field=COL_ZONE,
        event_time_field="dropoff_utc",
        source_id_field="_source_event_id",
        layout="wide",
        metric_fields={"fare": COL_FARE, "trip_distance": COL_DIST, "duration": "duration"},
        topic_const="tlc_yellow",
    )


def _month_range(start: str, end: str) -> list[str]:
    s = datetime.strptime(start, "%Y-%m")
    e = datetime.strptime(end, "%Y-%m")
    if e < s:
        raise SystemExit(f"FAIL: --end {end} is before --start {start}")
    out, y, m = [], s.year, s.month
    while (y, m) <= (e.year, e.month):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            m, y = 1, y + 1
    return out


def _fetch_month(ym: str, raw_dir: Path, session) -> Path | None:
    name = f"yellow_tripdata_{ym}.parquet"
    out = raw_dir / name
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists() and out.stat().st_size > 0:
        return out
    r = session.get(f"{BASE_URL}/{name}", timeout=300)
    if r.status_code != 200:
        print(f"  WARN {ym}: HTTP {r.status_code} -- skipping")
        return None
    out.write_bytes(r.content)
    return out


def _parse(path: Path, ym: str | None = None) -> list[dict]:
    """Read one monthly parquet -> list of raw dicts for the generic normalizer.

    Produces derived columns dropoff_utc (ISO8601) and duration (seconds), and
    applies boundary sanity filters (positive fare/distance/duration, valid
    zone, and -- when ym is given -- the dropoff falls inside that month).
    """
    cols = [COL_PICKUP, COL_DROPOFF, COL_ZONE, COL_FARE, COL_DIST]
    try:
        df = pd.read_parquet(path, columns=cols)
    except Exception as exc:  # pragma: no cover
        print(f"  WARN failed to read {path.name}: {exc}")
        return []

    pickup_local = pd.to_datetime(df[COL_PICKUP], errors="coerce")
    dropoff_local = pd.to_datetime(df[COL_DROPOFF], errors="coerce")
    # TLC timestamps are New York wall time. Ambiguous/nonexistent DST wall
    # times have no unique UTC instant and are rejected at this boundary.
    pickup = pickup_local.dt.tz_localize("America/New_York", ambiguous="NaT", nonexistent="NaT").dt.tz_convert("UTC")
    dropoff = dropoff_local.dt.tz_localize("America/New_York", ambiguous="NaT", nonexistent="NaT").dt.tz_convert("UTC")
    duration = (dropoff - pickup).dt.total_seconds()
    df["_source_event_id"] = [f"{path.name}:{i}" for i in range(len(df))]
    fare = pd.to_numeric(df[COL_FARE], errors="coerce")
    dist = pd.to_numeric(df[COL_DIST], errors="coerce")
    zone = pd.to_numeric(df[COL_ZONE], errors="coerce")

    keep = (
        dropoff.notna() & pickup.notna()
        & fare.gt(0) & dist.gt(0)
        & duration.gt(0) & duration.le(MAX_DURATION_S)
        & zone.ge(MIN_ZONE) & zone.le(MAX_ZONE)
    )
    if ym is not None:
        keep &= (dropoff_local.dt.strftime("%Y-%m") == ym)

    df = df[keep].copy()
    df["dropoff_utc"] = dropoff[keep].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    df["duration"] = duration[keep].astype(int)
    df[COL_ZONE] = zone[keep].astype(int).astype(str)
    df[COL_FARE] = fare[keep]
    df[COL_DIST] = dist[keep]
    return df[[COL_ZONE, "_source_event_id", "dropoff_utc", COL_FARE, COL_DIST, "duration"]].to_dict("records")


def _write(raw_records: list[dict], *, source: str, extra: dict,
           max_rows: int | None, select: str, input_paths: list[str]) -> dict:
    rows = normalize_records(raw_records, _domain_config())
    return write_normalized_log(rows, DOMAIN, source=source, extra_manifest=extra,
                                max_rows=max_rows, select=select, input_paths=input_paths)


def from_raw(start: str, end: str, raw_dir: str, max_rows: int | None, select: str) -> dict:
    import requests

    months = _month_range(start, end)
    raw_path = Path(raw_dir).resolve()
    session = requests.Session()
    raw_records: list[dict] = []
    fetched: list[str] = []
    ok = miss = 0
    for ym in months:
        p = _fetch_month(ym, raw_path, session)
        if p is None:
            miss += 1
            continue
        recs = _parse(p, ym=ym)
        ok += 1
        fetched.append(str(p))
        print(f"  {ym}: trips={len(recs):,}")
        raw_records.extend(recs)
    print(f"months_ok={ok} months_missing={miss} trips={len(raw_records):,}")
    return _write(
        raw_records,
        source="NYC TLC yellow-taxi trip records (d37ci6vzurychx.cloudfront.net)",
        extra={
            "mode": "from_raw",
            "window": [start, end],
            "months_ok": ok,
            "months_missing": miss,
            "time_basis": "TLC wall-clock localized to America/New_York then converted to UTC; ambiguous/nonexistent DST times rejected",
        },
        max_rows=max_rows, select=select, input_paths=fetched,
    )


def from_parquet(paths: list[str], max_rows: int | None, select: str) -> dict:
    raw_records: list[dict] = []
    for p in paths:
        recs = _parse(Path(p))
        print(f"  {Path(p).name}: trips={len(recs):,}")
        raw_records.extend(recs)
    return _write(
        raw_records,
        source=f"NYC TLC yellow-taxi (local parquet: {', '.join(Path(p).name for p in paths)})",
        extra={"mode": "from_parquet", "inputs": [str(Path(p).resolve()) for p in paths]},
        max_rows=max_rows, select=select, input_paths=[str(p) for p in paths],
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-raw", action="store_true", help="Download + normalize from the TLC archive")
    ap.add_argument("--start", help="YYYY-MM (from-raw, inclusive)")
    ap.add_argument("--end", help="YYYY-MM (from-raw, inclusive)")
    ap.add_argument("--raw-dir", default="data/raw/transportation_taxi")
    ap.add_argument("--from-parquet", nargs="+", metavar="PARQUET",
                    help="Normalize existing local monthly parquet file(s)")
    ap.add_argument("--max-rows", type=int, default=1_000_000,
                    help="Base workload size: keep this many normalized rows (0 = keep all)")
    ap.add_argument("--select", choices=["uniform", "tail", "head"], default="uniform",
                    help="uniform = evenly spaced across the full window (default); tail/head are endpoints")
    args = ap.parse_args()
    max_rows = args.max_rows or None

    if args.from_parquet:
        manifest = from_parquet(args.from_parquet, max_rows, args.select)
    elif args.from_raw:
        if not (args.start and args.end):
            raise SystemExit("FAIL: --from-raw requires --start and --end (YYYY-MM)")
        manifest = from_raw(args.start, args.end, args.raw_dir, max_rows, args.select)
    else:
        raise SystemExit("FAIL: choose --from-raw --start YYYY-MM --end YYYY-MM, or --from-parquet PATH ...")

    print("\nwrote", manifest["log_path"])
    print(f"  rows={manifest['rows']:,}  entities={manifest['entities']}  metrics={manifest['metrics']}")
    if manifest.get("selection"):
        print(f"  selection={manifest['selection']}  (capped from {manifest['rows_before_cap']:,})")
    print(f"  sha256={manifest['sha256'][:16]}...")
    print("RESULT: PASS" if manifest["rows"] > 0 else "RESULT: FAIL (0 rows)")
    return 0 if manifest["rows"] > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
