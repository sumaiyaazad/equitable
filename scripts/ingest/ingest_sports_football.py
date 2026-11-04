#!/usr/bin/env python3
"""
ingest_sports_football.py -- Normalize Metrica Sports open tracking data to the
neutral schema, using the SAME folder + code convention as every other domain:
    data/raw/sports_football/        <- Home/Away RawTrackingData CSVs (downloaded)
    data/normalized/sports_football/ <- log.parquet + manifest.json

Source: Metrica Sports sample data (public, CC), per-frame player+ball positions:
    https://github.com/metrica-sports/sample-data
Lineage: player positional tracking. Substitutes the original DEBS 2013 plan
(dead mirrors); the neutral mapping is the same idea -- player / speed / distance
derived from raw positions, which is the documented contract-boundary step.

Metrica RawTrackingData CSV structure (one file per team):
  row 1: team name at column index 3 ("Home" / "Away")
  row 2: player jersey numbers (non-empty cells, at columns 3,5,7,...)
  row 3: header (skipped)
  row 4+: Period, Frame, Time[s], then (x,y) per player, then ball (x,y)
Coordinates are normalized 0..1; the pitch is 105 x 68 m.

Per player (and the ball) we derive, per frame (WIDE layout):
    pos_x, pos_y  <- position in metres
    distance      <- metres moved since the previous frame
    speed         <- distance / dt  (m/s)
The match has no calendar date; event_time is a fixed synthetic epoch + Time[s],
so the pinned "window" is the match itself and the log is reproducible.

Inputs  : Metrica Home + Away tracking CSVs (local or downloaded)
Outputs : data/normalized/sports_football/log.parquet + manifest.json ; PASS/FAIL
Runtime : ~a minute (one game ~ a few million frame-rows)
Run as  : python scripts/ingest/ingest_sports_football.py --game 1
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.schema import normalize_records, DomainConfig  # noqa: E402
from scripts.ingest._ingest_common import write_normalized_log  # noqa: E402

DOMAIN = "sports_football"
RAW_BASE = "https://raw.githubusercontent.com/metrica-sports/sample-data/master/data"
PITCH_X, PITCH_Y = 105.0, 68.0                       # metres
MATCH_EPOCH = datetime(2019, 1, 1, tzinfo=timezone.utc)  # fixed synthetic match start


def _domain_config() -> DomainConfig:
    return DomainConfig(
        name=DOMAIN,
        entity_field="player",
        event_time_field="event_time_utc",
        source_id_field="_source_event_id",
        layout="wide",
        metric_fields={"speed": "speed", "distance": "distance", "pos_x": "pos_x", "pos_y": "pos_y"},
        topic_const="metrica",
    )


def _fetch(game: int, side: str, raw_dir: Path, session) -> Path:
    name = f"Sample_Game_{game}_RawTrackingData_{side}_Team.csv"
    out = raw_dir / name
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists() and out.stat().st_size > 0:
        return out
    url = f"{RAW_BASE}/Sample_Game_{game}/{name}"
    with session.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(out, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    return out


def _parse_team(path: Path, *, include_ball: bool) -> pd.DataFrame:
    """Read one team's tracking CSV -> long frame with columns
    player, frame, time, pos_x, pos_y (metres); positions may be NaN off-pitch."""
    with open(path, newline="") as fh:
        reader = csv.reader(fh)
        team = next(reader)[3].strip()               # row 1: team name at col 3
        jerseys = [x for x in next(reader) if x.strip() != ""]  # row 2: jersey numbers
    df = pd.read_csv(path, skiprows=3, header=None)   # row 4+: data
    time_s = pd.to_numeric(df[2], errors="coerce")
    frame = pd.to_numeric(df[1], errors="coerce")
    source_event_id = frame.map(lambda x: f"{path.stem}:{int(x)}" if pd.notna(x) else "")
    parts = []
    for i, j in enumerate(jerseys):
        xcol, ycol = 3 + 2 * i, 3 + 2 * i + 1
        parts.append(pd.DataFrame({
            "player": f"{team}_{j}",
            "frame": frame,
            "_source_event_id": source_event_id,
            "time": time_s,
            "pos_x": pd.to_numeric(df[xcol], errors="coerce") * PITCH_X,
            "pos_y": pd.to_numeric(df[ycol], errors="coerce") * PITCH_Y,
        }))
    if include_ball:
        bx, by = df.columns[-2], df.columns[-1]
        parts.append(pd.DataFrame({
            "player": "ball",
            "frame": frame,
            "_source_event_id": source_event_id,
            "time": time_s,
            "pos_x": pd.to_numeric(df[bx], errors="coerce") * PITCH_X,
            "pos_y": pd.to_numeric(df[by], errors="coerce") * PITCH_Y,
        }))
    return pd.concat(parts, ignore_index=True)


def _derive(df: pd.DataFrame) -> pd.DataFrame:
    """Add distance (m since last frame) and speed (m/s) per player."""
    df = df.sort_values(["player", "frame"], kind="mergesort").reset_index(drop=True)
    g = df.groupby("player", sort=False)
    dt = g["time"].diff()
    dx = g["pos_x"].diff()
    dy = g["pos_y"].diff()
    df["distance"] = (dx * dx + dy * dy) ** 0.5
    df["speed"] = df["distance"] / dt
    # event_time = synthetic match start + Time[s]
    df["event_time_utc"] = (
        pd.to_datetime(MATCH_EPOCH) + pd.to_timedelta(df["time"], unit="s")
    ).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return df


def from_csvs(home: Path, away: Path, max_rows: int | None, select: str,
              inputs: list[str], extra: dict) -> dict:
    long = pd.concat([_parse_team(home, include_ball=True), _parse_team(away, include_ball=False)], ignore_index=True)
    long = _derive(long)
    keep = ["player", "_source_event_id", "event_time_utc", "speed", "distance", "pos_x", "pos_y"]
    long = long[long["event_time_utc"].notna()]
    # NaN metrics -> None so the normalizer skips them (first frame / off-pitch)
    records = long[keep].where(pd.notna(long[keep]), None).to_dict("records")
    print(f"frame_rows={len(records):,} players={long['player'].nunique()}")
    rows = normalize_records(records, _domain_config())
    return write_normalized_log(
        rows, DOMAIN, source="Metrica Sports sample tracking data (github.com/metrica-sports/sample-data)",
        extra_manifest=extra, max_rows=max_rows, select=select, input_paths=inputs,
    )


def from_raw(game: int, raw_dir: str, max_rows: int | None, select: str) -> dict:
    import requests

    raw_path = Path(raw_dir).resolve()
    session = requests.Session()
    home = _fetch(game, "Home", raw_path, session)
    away = _fetch(game, "Away", raw_path, session)
    return from_csvs(home, away, max_rows, select,
                     inputs=[str(home), str(away)],
                     extra={"mode": "from_raw", "game": game})


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--game", type=int, default=1, help="Metrica sample game number (1 or 2)")
    ap.add_argument("--home", help="Local Home tracking CSV (with --away)")
    ap.add_argument("--away", help="Local Away tracking CSV (with --home)")
    ap.add_argument("--raw-dir", default="data/raw/sports_football")
    ap.add_argument("--max-rows", type=int, default=1_000_000,
                    help="Base workload size: keep this many normalized rows (0 = keep all)")
    ap.add_argument("--select", choices=["uniform", "tail", "head"], default="uniform",
                    help="uniform = evenly spaced across the full window (default); tail/head are endpoints")
    args = ap.parse_args()
    max_rows = args.max_rows or None

    if args.home and args.away:
        manifest = from_csvs(Path(args.home), Path(args.away), max_rows, args.select,
                             inputs=[args.home, args.away], extra={"mode": "from_csvs"})
    else:
        manifest = from_raw(args.game, args.raw_dir, max_rows, args.select)

    print("\nwrote", manifest["log_path"])
    print(f"  rows={manifest['rows']:,}  entities={manifest['entities']}  metrics={manifest['metrics']}")
    if manifest.get("selection"):
        print(f"  selection={manifest['selection']}  (capped from {manifest['rows_before_cap']:,})")
    print(f"  sha256={manifest['sha256'][:16]}...")
    print("RESULT: PASS" if manifest["rows"] > 0 else "RESULT: FAIL (0 rows)")
    return 0 if manifest["rows"] > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
