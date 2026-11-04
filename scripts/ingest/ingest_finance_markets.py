#!/usr/bin/env python3
"""
ingest_finance_markets.py -- Normalize Binance spot per-trade data to the neutral
schema, using the SAME folder + code convention as every other domain:
    data/raw/finance_markets/        <- monthly per-symbol trade zips (downloaded)
    data/normalized/finance_markets/ <- log.parquet + manifest.json

Source: Binance public data (free, no auth), monthly per-symbol SPOT trades:
    https://data.binance.vision/data/spot/monthly/trades/<SYMBOL>/<SYMBOL>-trades-<YYYY-MM>.zip
Lineage: high-volume market trade tape (crypto). Substitutes the original IEX
DEEP plan (binary pcap, not freely available for 2024); the neutral mapping is
identical -- symbol / trade_price / trade_size.

One trade is one event. Each trade explodes into two neutral metrics (WIDE):
    trade_price <- price   (quote currency, e.g. USDT)
    trade_size  <- qty     (base asset)
keyed by symbol (one file per symbol -> multiple `entity` symbols in the log).

Binance trades CSV has no header; columns are:
    trade_id, price, qty, quoteQty, time, isBuyerMaker, isBestMatch
`time` is epoch ms before 2025-01-01 and epoch us after; we auto-detect.

Memory: monthly files are huge (BTCUSDT ~ tens of millions of trades). We read
each symbol in chunks and keep only the newest `--max-rows` trades (the tail is
all the global 1M cap can need, since files are time-sorted ascending), while
still counting the full-month total for a consistent rows_before_cap.

Inputs  : Binance monthly trade zips (local or downloaded)
Outputs : data/normalized/finance_markets/log.parquet + manifest.json ; PASS/FAIL
Runtime : minutes per symbol to download + scan
Run as  : python scripts/ingest/ingest_finance_markets.py --month 2024-06 \
              --symbols BTCUSDT ETHUSDT BNBUSDT SOLUSDT XRPUSDT
"""
from __future__ import annotations

import argparse
import math
import re
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.schema import normalize_records, DomainConfig  # noqa: E402
from scripts.ingest._ingest_common import write_normalized_log  # noqa: E402

DOMAIN = "finance_markets"
BASE_URL = "https://data.binance.vision/data/spot/monthly/trades"
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT"]
CHUNK = 2_000_000
# Keep the exchange trade id: equal-content trades are distinct observations.
USECOLS = [0, 1, 2, 4]
COLNAMES = ["trade_id", "price", "qty", "time"]


def _domain_config() -> DomainConfig:
    return DomainConfig(
        name=DOMAIN,
        entity_field="symbol",
        event_time_field="event_time_utc",
        source_id_field="trade_id",
        layout="wide",
        metric_fields={"trade_price": "price", "trade_size": "qty"},
        topic_const="binance_spot",
    )


def _epoch_to_iso(series: pd.Series) -> pd.Series:
    """Binance `time` -> ISO8601 UTC with bounded ms/us conversion.

    Convert only the populated unit branch.  Converting a full masked vector
    (including an all-NaN branch) can overflow inside pandas under long,
    memory-heavy multi-symbol scans even though the populated values are valid.
    """
    integer_text = series.astype("string").str.fullmatch(r"[+-]?\d+", na=False)
    t = pd.to_numeric(series.where(integer_text), errors="coerce")
    is_us = t.ge(1e14)
    dt = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns, UTC]")
    min_ns, max_ns = pd.Timestamp.min.value, pd.Timestamp.max.value

    for unit, scale, unit_mask in (("ms", 1_000_000, ~is_us),
                                   ("us", 1_000, is_us)):
        lower = -((-min_ns) // scale)  # ceil(min_ns / scale)
        upper = max_ns // scale
        valid = integer_text & unit_mask & t.between(lower, upper, inclusive="both")
        if valid.any():
            dt.loc[valid] = pd.to_datetime(
                t.loc[valid].astype("int64"), unit=unit, utc=True, errors="coerce"
            )
    return dt.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _fetch(symbol: str, month: str, raw_dir: Path, session) -> Path | None:
    name = f"{symbol}-trades-{month}.zip"
    out = raw_dir / name
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists() and out.stat().st_size > 0:
        return out
    url = f"{BASE_URL}/{symbol}/{name}"
    with session.get(url, stream=True, timeout=1800) as r:
        if r.status_code != 200:
            print(f"  WARN {symbol}: HTTP {r.status_code} -- skipping")
            return None
        with open(out, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    return out


def _scan_symbol(
    zip_path: Path, symbol: str, keep: int | None, select: str,
) -> tuple[list[dict], int]:
    """Return a memory-bounded deterministic sample and full source count."""
    with zipfile.ZipFile(zip_path) as zf:
        csvname = next((n for n in zf.namelist() if n.endswith(".csv")), None)
        if csvname is None:
            print(f"  WARN {symbol}: no CSV in zip")
            return [], 0
        total = 0
        retained: pd.DataFrame | None = None
        with zf.open(csvname) as fh:
            for chunk in pd.read_csv(
                fh, header=None, usecols=USECOLS, names=COLNAMES,
                dtype=str, chunksize=CHUNK,
            ):
                total += len(chunk)
                if keep and select == "uniform":
                    chunk = chunk.copy()
                    identity = symbol + "|" + chunk["trade_id"].astype(str)
                    chunk["_priority"] = pd.util.hash_pandas_object(
                        identity, index=False).astype("uint64")
                    chunk = chunk.nsmallest(min(keep, len(chunk)), "_priority")
                    retained = (chunk if retained is None else pd.concat(
                        [retained, chunk], ignore_index=True).nsmallest(
                            keep, "_priority"))
                elif keep and select == "head":
                    needed = keep if retained is None else keep - len(retained)
                    if needed > 0:
                        piece = chunk.head(needed)
                        retained = piece if retained is None else pd.concat(
                            [retained, piece], ignore_index=True)
                else:
                    retained = chunk if retained is None else pd.concat(
                        [retained, chunk], ignore_index=True)
                    if keep and len(retained) > keep:
                        retained = retained.tail(keep)
    if retained is None or retained.empty:
        return [], total
    retained = retained.drop(columns=["_priority"], errors="ignore").copy()
    retained["symbol"] = symbol
    retained["event_time_utc"] = _epoch_to_iso(retained["time"])
    retained = retained[retained["event_time_utc"].notna()]
    columns = ["symbol", "trade_id", "event_time_utc", "price", "qty"]
    return retained[columns].to_dict("records"), total


def from_raw(symbols: list[str], month: str, raw_dir: str, max_rows: int | None, select: str) -> dict:
    import requests

    raw_path = Path(raw_dir).resolve()
    session = requests.Session()
    keep = max_rows
    if max_rows and select == "uniform":
        # Two metrics per trade; 5% slack absorbs any malformed source values.
        keep = math.ceil(max_rows * 1.05 / (2 * max(len(symbols), 1)))
    records: list[dict] = []
    inputs: list[str] = []
    grand_total = 0
    for sym in symbols:
        p = _fetch(sym, month, raw_path, session)
        if p is None:
            continue
        inputs.append(str(p))
        recs, total = _scan_symbol(p, sym, keep, select)
        grand_total += total
        records.extend(recs)
        print(f"  {sym}: trades={total:,}  kept={len(recs):,}")
    print(f"symbols={len(inputs)} full_month_trades={grand_total:,} rows_in_window={len(records):,}")
    rows = normalize_records(records, _domain_config())
    return write_normalized_log(
        rows, DOMAIN, source="Binance spot monthly trades (data.binance.vision)",
        extra_manifest={
            "mode": "from_raw", "month": month, "symbols": symbols,
            "source_preselection": ({
                "method": "lowest pandas uint64 hash(symbol|trade_id) per symbol",
                "trades_per_symbol": keep,
                "symbols_requested": len(symbols),
            } if select == "uniform" and keep else None),
            # two neutral rows (price + size) per trade, over the full month
            "rows_before_cap": grand_total * 2,
        },
        max_rows=max_rows, select=select, input_paths=inputs,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", required=True, help="Pinned month, YYYY-MM (e.g. 2024-06)")
    ap.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS, help="Binance spot symbols")
    ap.add_argument("--raw-dir", default="data/raw/finance_markets")
    ap.add_argument("--max-rows", type=int, default=1_000_000,
                    help="Base workload size: keep this many normalized rows (0 = keep all)")
    ap.add_argument("--select", choices=["uniform", "tail", "head"], default="uniform",
                    help="uniform = evenly spaced across the full window (default); tail/head are endpoints")
    args = ap.parse_args()
    if not re.fullmatch(r"\d{4}-\d{2}", args.month):
        raise SystemExit("FAIL: --month must be YYYY-MM")
    max_rows = args.max_rows or None

    manifest = from_raw([s.upper() for s in args.symbols], args.month, args.raw_dir, max_rows, args.select)

    print("\nwrote", manifest["log_path"])
    print(f"  rows={manifest['rows']:,}  entities={manifest['entities']}  metrics={manifest['metrics']}")
    if manifest.get("selection"):
        print(f"  selection={manifest['selection']}  (capped from {manifest['rows_before_cap']:,})")
    print(f"  sha256={manifest['sha256'][:16]}...")
    print("RESULT: PASS" if manifest["rows"] > 0 else "RESULT: FAIL (0 rows)")
    return 0 if manifest["rows"] > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
