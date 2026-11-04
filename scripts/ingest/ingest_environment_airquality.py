#!/usr/bin/env python3
"""
ingest_environment_airquality.py -- Download + normalize EPA AirNow air-quality data to the
neutral schema, using the SAME folder convention as every other domain:
    data/raw/environment_airquality/        <- raw hourly archive files (downloaded)
    data/normalized/environment_airquality/ <- log.parquet + manifest.json

Primary mode (like every other domain):
  --from-raw --start YYYY-MM-DD --end YYYY-MM-DD
        Download the AirNow hourly archive into data/raw/environment_airquality/ and
        normalize via the generic src.schema.normalize_records path (long
        layout). Needs internet.

Optional shortcut:
  --from-normalized PATH
        Re-key an already-normalized AirNow log to the neutral schema (no
        download). Kept only as a convenience; the default workflow is
        --from-raw so air quality is produced exactly like the other domains.

Inputs  : AirNow archive over the network (or an existing normalized parquet)
Outputs : data/normalized/environment_airquality/log.parquet + manifest.json ; PASS/FAIL
Runtime : from-raw depends on the window (~1-2 s per hourly file)
Run as  : python scripts/ingest/ingest_environment_airquality.py --from-raw \
              --start 2026-06-01 --end 2026-06-17
"""
from __future__ import annotations

import argparse
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.schema import build_dedupe_id, normalize_records, DomainConfig, NORMALIZED_COLUMNS  # noqa: E402
from scripts.ingest._ingest_common import write_normalized_log  # noqa: E402

DOMAIN = "environment_airquality"
PARAM_MAP = {"OZONE": "ozone", "PM2.5": "pm25"}
BASE_URL = "https://files.airnowtech.org/airnow"


# ---------------------------------------------------------------- re-key mode
def from_normalized(path: str, max_rows: int | None, select: str) -> dict:
    df = pd.read_parquet(path)
    rename = {}
    if "stid" in df.columns and "entity" not in df.columns:
        rename["stid"] = "entity"
    if "sensor" in df.columns and "metric" not in df.columns:
        rename["sensor"] = "metric"
    df = df.rename(columns=rename)

    missing = [c for c in ("event_time_utc", "entity", "metric", "value") if c not in df.columns]
    if missing:
        raise SystemExit(f"FAIL: input missing required columns {missing}; got {list(df.columns)}")

    df = df.copy()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    event_ts = pd.to_datetime(df["event_time_utc"], utc=True, errors="coerce")
    valid = df["value"].map(lambda value: pd.notna(value) and math.isfinite(value)) & event_ts.notna()
    df = df[valid].copy()
    df["event_time_utc"] = event_ts[valid].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    if "processing_order" not in df.columns:
        df["processing_order"] = range(len(df))
    df["processing_order"] = df["processing_order"].astype(int)
    if "ingest_time_utc" not in df.columns:
        df["ingest_time_utc"] = None
    if "topic" not in df.columns:
        df["topic"] = None
    df["source_record_type"] = "data"
    # Preserve an upstream source identity when present. Legacy inputs without
    # one receive a deterministic file/row identity; content is never identity.
    if "dedupe_id" not in df.columns:
        df["dedupe_id"] = [
            build_dedupe_id(f"{Path(path).name}:{i}", str(m), namespace=f"{DOMAIN}|{e}")
            for i, (e, m) in enumerate(zip(df["entity"], df["metric"]))
        ]
    return write_normalized_log(
        df, DOMAIN,
        source=f"EPA AirNow (re-keyed from {Path(path).name})",
        extra_manifest={"mode": "from_normalized", "input_path": str(Path(path).resolve())},
        max_rows=max_rows, select=select, input_paths=[str(path)],
    )


# ---------------------------------------------------------------- from-raw mode
def _fetch_hour(dt, raw_dir: Path, session):
    yyyymmdd, yyyy, hh = dt.strftime("%Y%m%d"), dt.strftime("%Y"), dt.strftime("%H")
    name = f"HourlyData_{yyyymmdd}{hh}.dat"
    out = raw_dir / yyyymmdd / name
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists() and out.stat().st_size > 0:
        return out
    r = session.get(f"{BASE_URL}/{yyyy}/{yyyymmdd}/{name}", timeout=60)
    if r.status_code != 200:
        return None
    out.write_bytes(r.content)
    return out


def _parse_raw(path: Path) -> list[dict]:
    cols = ["date", "hhmm", "stid", "name", "gmt_off", "param", "unit", "value", "agency"]
    try:
        df = pd.read_csv(path, sep="|", header=None, names=cols, dtype=str,
                         on_bad_lines="skip", encoding="latin-1")
    except Exception:
        return []
    local_ts = pd.to_datetime(df["date"] + " " + df["hhmm"], format="%m/%d/%y %H:%M", errors="coerce")
    gmt_offset = pd.to_numeric(df["gmt_off"], errors="coerce")
    utc_ts = (local_ts - pd.to_timedelta(gmt_offset, unit="h")).dt.tz_localize("UTC")
    keep = df["param"].isin(PARAM_MAP) & utc_ts.notna() & gmt_offset.notna()
    source_ids = pd.Series([f"{path.name}:{i}" for i in range(len(df))], index=df.index)
    df = df[keep]
    df = df.assign(event_time_utc=utc_ts[keep].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                   _source_event_id=source_ids[keep],
                   stid=df["stid"].str.strip().str.upper())
    # raw dicts for the generic normalizer (long layout via the domain config)
    return df[["stid", "param", "value", "event_time_utc", "_source_event_id"]].to_dict("records")


def from_raw(start: str, end: str, raw_dir: str, max_rows: int | None, select: str) -> dict:
    import requests
    cfg = DomainConfig(
        name=DOMAIN, entity_field="stid", event_time_field="event_time_utc", source_id_field="_source_event_id",
        layout="long", metric_field="param", value_field="value",
        metric_map=PARAM_MAP, topic_const="airnow",
    )
    raw_path = Path(raw_dir).resolve()
    session = requests.Session()
    dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    end_dt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    raw_records: list[dict] = []
    fetched: list[str] = []
    ok = miss = 0
    while dt < end_dt:
        p = _fetch_hour(dt, raw_path, session)
        if p is None:
            miss += 1
        else:
            ok += 1
            fetched.append(str(p))
            raw_records.extend(_parse_raw(p))
        dt += timedelta(hours=1)
    print(f"fetched={ok} missing={miss} raw_records={len(raw_records):,}")
    rows = normalize_records(raw_records, cfg)
    return write_normalized_log(
        rows, DOMAIN, source="EPA AirNow hourly archive (files.airnowtech.org)",
        extra_manifest={"mode": "from_raw", "window": [start, end], "files_ok": ok, "files_missing": miss},
        max_rows=max_rows, select=select, input_paths=fetched,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-normalized", metavar="PARQUET",
                    help="Re-key an existing normalized AirNow log to the neutral schema")
    ap.add_argument("--from-raw", action="store_true", help="Download + normalize from the AirNow archive")
    ap.add_argument("--start", help="YYYY-MM-DD UTC (from-raw)")
    ap.add_argument("--end", help="YYYY-MM-DD UTC exclusive (from-raw)")
    ap.add_argument("--raw-dir", default="data/raw/environment_airquality")
    ap.add_argument("--max-rows", type=int, default=1_000_000,
                    help="Base workload size: keep this many normalized rows (0 = keep all)")
    ap.add_argument("--select", choices=["uniform", "tail", "head"], default="uniform",
                    help="uniform = evenly spaced across the full window (default); tail/head are endpoints")
    args = ap.parse_args()
    max_rows = args.max_rows or None

    if args.from_normalized:
        manifest = from_normalized(args.from_normalized, max_rows, args.select)
    elif args.from_raw:
        if not (args.start and args.end):
            raise SystemExit("FAIL: --from-raw requires --start and --end")
        manifest = from_raw(args.start, args.end, args.raw_dir, max_rows, args.select)
    else:
        raise SystemExit("FAIL: choose --from-normalized PATH or --from-raw --start ... --end ...")

    print("\nwrote", manifest["log_path"])
    print(f"  rows={manifest['rows']:,}  entities={manifest['entities']}  metrics={manifest['metrics']}")
    if manifest.get("selection"):
        print(f"  selection={manifest['selection']}  (capped from {manifest['rows_before_cap']:,})")
    print(f"  sha256={manifest['sha256'][:16]}...")
    print("RESULT: PASS" if manifest["rows"] > 0 else "RESULT: FAIL (0 rows)")
    return 0 if manifest["rows"] > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
