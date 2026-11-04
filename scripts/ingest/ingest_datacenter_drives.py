#!/usr/bin/env python3
"""
ingest_datacenter_drives.py -- Normalize Backblaze Drive Stats (SMART telemetry)
to the neutral schema, using the SAME folder + code convention as every other
domain:
    data/raw/datacenter_drives/        <- the quarterly zip (downloaded/placed)
    data/normalized/datacenter_drives/ <- log.parquet + manifest.json

Source: Backblaze Hard Drive Test Data (public), one quarterly ZIP of daily
CSV snapshots, one row per drive per day.
    https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data
Lineage: DEBS Grand Challenge 2024; SIMBA IT Monitor.

One drive/day snapshot is one event, timed at that day's date (daily cadence).
Each snapshot explodes into three neutral metrics (WIDE layout), keyed by the
drive serial number (~300K drives -> the high-cardinality domain):
    temperature     <- smart_194_raw   (Celsius)
    power_on_hours  <- smart_9_raw      (hours)
    realloc_sectors <- smart_5_raw      (count)

The quarterly zip is large (~GB) and its download URL changes between releases,
so the PRIMARY mode reads a zip you already downloaded:

  --from-zip PATH --month YYYY-MM
        Parse an already-downloaded quarterly zip, extracting only the daily
        CSVs for the pinned month. (Download once from the page above.)

Optional:
  --url URL --month YYYY-MM
        Download that quarterly zip into data/raw/datacenter_drives/ first,
        then parse. Needs internet + disk for the full quarter.

Base workload: like every domain, capped to --max-rows (default 1,000,000)
selected as the latest rows by event-time (--select tail). Because event-time
here is daily, the tail is the last day(s) of the month. To bound memory we
retain only the newest days needed for the cap, but we still SCAN every day to
record the true full-month row total (manifest `rows_before_cap`), so that
field is consistent with what the other domains report over their full window.

Inputs  : Backblaze quarterly zip (local or via --url)
Outputs : data/normalized/datacenter_drives/log.parquet + manifest.json ; PASS/FAIL
Runtime : minutes (reading a few daily CSVs of ~300K rows each)
Run as  : python scripts/ingest/ingest_datacenter_drives.py \
              --from-zip data/raw/datacenter_drives/data_Q2_2024.zip --month 2024-06
"""
from __future__ import annotations

import argparse
import math
import os
import re
import sys
import zipfile
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.schema import normalize_records, DomainConfig  # noqa: E402
from scripts.ingest._ingest_common import write_normalized_log  # noqa: E402

DOMAIN = "datacenter_drives"
# neutral metric -> Backblaze SMART raw column
SMART = {"temperature": "smart_194_raw", "power_on_hours": "smart_9_raw", "realloc_sectors": "smart_5_raw"}
NEEDED = ["date", "serial_number", *SMART.values()]
NEEDED_SET = set(NEEDED)


def _domain_config() -> DomainConfig:
    return DomainConfig(
        name=DOMAIN,
        entity_field="serial_number",
        event_time_field="event_time_utc",
        source_id_field="date",
        layout="wide",
        metric_fields=dict(SMART),
        topic_const="backblaze",
    )


def _daily_members(zf: zipfile.ZipFile, month: str) -> list[str]:
    """Zip member names for that month's daily CSVs (YYYY-MM-DD.csv), newest first."""
    pat = re.compile(rf"(?:.*/)?{re.escape(month)}-\d\d\.csv$")
    return sorted((n for n in zf.namelist() if pat.match(n)), reverse=True)


def _read_day(zf: zipfile.ZipFile, name: str):
    """Read one daily CSV -> filtered DataFrame (serial/date present) with a
    derived event_time_utc column, or None if unusable."""
    with zf.open(name) as fh:
        try:
            df = pd.read_csv(fh, usecols=lambda c: c in NEEDED_SET, dtype=str)
        except Exception as exc:  # pragma: no cover
            print(f"  WARN failed to read {name}: {exc}")
            return None
    if "serial_number" not in df.columns or "date" not in df.columns:
        return None
    for col in SMART.values():                       # tolerate a missing SMART column
        if col not in df.columns:
            df[col] = None
    df = df[df["serial_number"].notna() & df["date"].notna()].copy()
    if df.empty:
        return None
    df["event_time_utc"] = df["date"].str.strip() + "T00:00:00Z"
    return df


def _day_count(df) -> int:
    """Exact number of neutral rows this day would emit (one per numeric SMART cell)."""
    return int(sum(pd.to_numeric(df[c], errors="coerce").notna().sum() for c in SMART.values()))


def _day_records(df) -> list[dict]:
    keep = ["serial_number", "date", "event_time_utc", *SMART.values()]
    # NaN -> None so the normalizer skips empty SMART readings (wide layout)
    return df[keep].where(pd.notna(df[keep]), None).to_dict("records")


def _scan_month(zip_path: Path, month: str, max_rows: int | None, select: str):
    """Scan EVERY daily CSV to record the true full-month row total (so
    rows_before_cap is consistent with the other domains), but only retain in
    memory the newest days needed to satisfy the tail cap. Returns
    (records, total_before_cap, days_scanned, days_kept)."""
    with zipfile.ZipFile(zip_path) as zf:
        members = _daily_members(zf, month)            # newest day first
        if not members:
            raise SystemExit(f"FAIL: no {month}-*.csv daily files found inside {zip_path.name}")
        if select == "head":                           # earliest -> scan oldest first
            members = list(reversed(members))
        records: list[dict] = []
        per_day = (None if not max_rows else math.ceil(
            max_rows * 1.10 / (len(members) * len(SMART))))
        total = scanned = kept = 0
        enough = False
        for name in members:
            df = _read_day(zf, name)
            if df is None:
                continue
            scanned += 1
            total += _day_count(df)                     # counted for every day
            if select == "uniform":
                sample = df
                if per_day and len(sample) > per_day:
                    identity = (sample["serial_number"].astype(str)
                                + "|" + sample["date"].astype(str))
                    sample = sample.assign(
                        _priority=pd.util.hash_pandas_object(identity, index=False)
                    ).nsmallest(per_day, "_priority").drop(columns=["_priority"])
                records.extend(_day_records(sample))
                kept += 1
            elif not enough:                            # endpoint modes only
                records.extend(_day_records(df))
                kept += 1
                if max_rows and len(records) >= max_rows:
                    enough = True
            print(f"  {Path(name).name}: kept_rows={len(records):,}  full_month_rows={total:,}")
    return records, total, scanned, kept, per_day


def _write(raw_records: list[dict], *, source: str, extra: dict,
           max_rows: int | None, select: str, input_paths: list[str]) -> dict:
    rows = normalize_records(raw_records, _domain_config())
    return write_normalized_log(rows, DOMAIN, source=source, extra_manifest=extra,
                                max_rows=max_rows, select=select, input_paths=input_paths)


def from_zip(zip_path: str, month: str, max_rows: int | None, select: str) -> dict:
    p = Path(zip_path)
    if not p.exists():
        raise SystemExit(f"FAIL: {zip_path} not found")
    records, total, scanned, kept, per_day = _scan_month(p, month, max_rows, select)
    print(f"days_scanned={scanned} days_kept={kept} "
          f"full_month_rows={total:,} rows_in_window={len(records):,}")
    return _write(
        records, source=f"Backblaze Drive Stats ({p.name})",
        extra={
            "mode": "from_zip", "zip": os.path.relpath(p.resolve(), ROOT), "month": month,
            "days_scanned": scanned, "days_kept": kept,
            "source_preselection": ({
                "method": "lowest pandas uint64 hash(serial_number|date) per day",
                "raw_records_per_day": per_day,
                "days": scanned,
            } if select == "uniform" and per_day else None),
            # override the writer's local count so rows_before_cap = TRUE full month,
            # consistent with the full-window total the other domains record
            "rows_before_cap": total,
        },
        max_rows=max_rows, select=select, input_paths=[str(p.resolve())],
    )


def from_url(url: str, month: str, raw_dir: str, max_rows: int | None, select: str) -> dict:
    import requests

    raw_path = Path(raw_dir).resolve()
    raw_path.mkdir(parents=True, exist_ok=True)
    dest = raw_path / Path(url).name
    if not (dest.exists() and dest.stat().st_size > 0):
        print(f"downloading {url} -> {dest} ...")
        with requests.get(url, stream=True, timeout=1800) as r:
            r.raise_for_status()
            with open(dest, "wb") as fh:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)
    return from_zip(str(dest), month, max_rows, select)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-zip", metavar="ZIP", help="Local Backblaze quarterly zip to parse")
    ap.add_argument("--url", help="Download the quarterly zip from this URL, then parse")
    ap.add_argument("--month", required=True, help="Pinned month to extract, YYYY-MM (e.g. 2024-06)")
    ap.add_argument("--raw-dir", default="data/raw/datacenter_drives")
    ap.add_argument("--max-rows", type=int, default=1_000_000,
                    help="Base workload size: keep this many normalized rows (0 = keep all)")
    ap.add_argument("--select", choices=["uniform", "tail", "head"], default="uniform",
                    help="uniform = evenly spaced across the full window (default); tail/head are endpoints")
    args = ap.parse_args()
    if not re.fullmatch(r"\d{4}-\d{2}", args.month):
        raise SystemExit("FAIL: --month must be YYYY-MM")
    max_rows = args.max_rows or None

    if args.from_zip:
        manifest = from_zip(args.from_zip, args.month, max_rows, args.select)
    elif args.url:
        manifest = from_url(args.url, args.month, args.raw_dir, max_rows, args.select)
    else:
        raise SystemExit("FAIL: choose --from-zip PATH --month YYYY-MM, or --url URL --month YYYY-MM")

    print("\nwrote", manifest["log_path"])
    print(f"  rows={manifest['rows']:,}  entities={manifest['entities']}  metrics={manifest['metrics']}")
    if manifest.get("selection"):
        print(f"  selection={manifest['selection']}  (capped from {manifest['rows_before_cap']:,})")
    print(f"  sha256={manifest['sha256'][:16]}...")
    print("RESULT: PASS" if manifest["rows"] > 0 else "RESULT: FAIL (0 rows)")
    return 0 if manifest["rows"] > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
