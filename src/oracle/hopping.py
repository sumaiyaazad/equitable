"""Hopping-window expected tables, in both oracle implementations.

A hopping (sliding) window of size S advancing by hop H assigns each event to
k = S/H overlapping windows instead of the single window a tumbling bucket
gives it. That is the only semantic change: the emitted table is keyed by
(window_start, entity, metric) and carries the same count/sum/min/max/mean and
latest columns as every other bucket table, so it is the existing bucket
schema with the window start in the bucket column. Nothing downstream needs a
new comparator, promotion rule, or provenance record.

The window start is a multiple of the hop measured from the epoch, which is
what every engine's HOP/HOPPING/window primitive does, so the oracle and the
engines agree on window boundaries without either side being told them.

For an event at time t the containing windows are the multiples of H in
(t - S, t]. With S = k*H those are exactly floor(t/H)*H - i*H for i in
0..k-1, which is why the explosion below needs no filter.

Both paths here must agree, for the same reason the rest of the contract runs
two oracles: the pandas path expresses the explosion as a row-wise repeat and
the DuckDB path as a range join, and their agreement is what makes a hopping
divergence an engine finding rather than an oracle artifact.
"""
from __future__ import annotations

from typing import Any, Dict

import duckdb
import pandas as pd

#: Default geometry. Two hours advancing by one gives k=2, the smallest hop
#: that actually overlaps, on the same hourly scale the contract already uses.
#: Kept small on purpose: k multiplies oracle rows and engine state by k.
WINDOW_SIZE_H = 2
HOP_H = 1

#: D8 keeps the two grains every other diagnostic reports. A grain fixes the
#: unit: "hourly" is a two-hour window hopping by one hour, "daily" is two days
#: hopping by one day. Sizes are carried in hours because pandas floors on an
#: hour multiple, and floor("24h") is midnight UTC, which is exactly the day
#: boundary date_trunc('day') and a native HOP both use.
GRAIN_GEOMETRY = {
    "hourly": (WINDOW_SIZE_H, HOP_H),
    "daily": (WINDOW_SIZE_H * 24, HOP_H * 24),
}
_BUCKET_FORMAT = {"hourly": "%Y-%m-%dT%H:00:00Z", "daily": "%Y-%m-%d"}


def geometry(grain: str) -> tuple:
    """(size_h, hop_h) for a grain."""
    if grain not in GRAIN_GEOMETRY:
        raise ValueError(f"grain must be hourly or daily, got {grain!r}")
    return GRAIN_GEOMETRY[grain]


def window_count(size_h: int = WINDOW_SIZE_H, hop_h: int = HOP_H) -> int:
    """k, the number of windows each event belongs to."""
    if size_h <= 0 or hop_h <= 0:
        raise ValueError("window size and hop must be positive")
    if size_h % hop_h:
        raise ValueError(
            f"window size {size_h}h must be a whole multiple of hop {hop_h}h; "
            "a partial multiple gives events at different offsets different "
            "window counts, which no engine primitive reproduces")
    return size_h // hop_h


def explode_to_windows(frame: pd.DataFrame, *, size_h: int = WINDOW_SIZE_H,
                       hop_h: int = HOP_H,
                       grain: str = "hourly") -> pd.DataFrame:
    """Repeat each row once per window that contains it.

    Expects `event_time_ts` (tz-aware) and returns the frame with a
    `window_bucket` column formatted exactly as the hourly bucket is, so the
    comparator sees one vocabulary of bucket labels.
    """
    k = window_count(size_h, hop_h)
    if frame.empty:
        out = frame.copy()
        out["window_bucket"] = pd.Series(dtype="object")
        return out
    hop = pd.Timedelta(hours=hop_h)
    base = frame["event_time_ts"].dt.floor(f"{hop_h}h")
    parts = []
    for i in range(k):
        part = frame.copy()
        part["window_bucket"] = (base - i * hop).dt.strftime(
            _BUCKET_FORMAT[grain])
        parts.append(part)
    return pd.concat(parts, ignore_index=True)


def build_hopping_table(frame: pd.DataFrame, *, grain: str = "hourly",
                        size_h: int = 0, hop_h: int = 0) -> pd.DataFrame:
    """Expected hopping table in pandas, reusing the tumbling aggregation."""
    from src.oracle.pandas_oracle import _build_bucket_table, _empty_bucket_frame
    if not size_h or not hop_h:
        size_h, hop_h = geometry(grain)
    if frame.empty:
        return _empty_bucket_frame()
    return _build_bucket_table(
        explode_to_windows(frame, size_h=size_h, hop_h=hop_h, grain=grain),
        "window_bucket")


_SQL = """
WITH b AS (
    SELECT entity, metric, value, event_time_utc, dedupe_id,
           CAST(event_time_utc AS TIMESTAMPTZ) AS ts
    FROM rows_in
),
w AS (
    SELECT b.*,
           strftime(
             time_bucket(INTERVAL '{hop} hours', b.ts) - (o.i * INTERVAL '{hop} hours'),
             '{fmt}') AS bucket
    FROM b CROSS JOIN (SELECT UNNEST(range(0, {k})) AS i) o
),
agg AS (
    SELECT bucket, entity, metric,
           COUNT(*)::BIGINT AS count, SUM(value) AS sum, MIN(value) AS min,
           MAX(value) AS max, AVG(value) AS mean
    FROM w GROUP BY bucket, entity, metric
),
chosen AS (
    SELECT bucket, entity, metric, value AS latest_value,
           event_time_utc AS latest_event_time
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY bucket, entity, metric
            ORDER BY ts DESC, dedupe_id DESC) AS rn
        FROM w
    ) WHERE rn = 1
)
SELECT a.bucket, a.entity, a.metric, a.count, a.sum, a.min, a.max, a.mean,
       c.latest_value, c.latest_event_time
FROM agg a JOIN chosen c USING (bucket, entity, metric)
ORDER BY a.bucket, a.entity, a.metric
"""


def build_hopping_table_sql(frame: pd.DataFrame, *, grain: str = "hourly",
                            size_h: int = 0, hop_h: int = 0) -> pd.DataFrame:
    """The same expected table computed independently in DuckDB."""
    from src.oracle.pandas_oracle import _empty_bucket_frame
    if not size_h or not hop_h:
        size_h, hop_h = geometry(grain)
    if frame.empty:
        return _empty_bucket_frame()
    k = window_count(size_h, hop_h)
    con = duckdb.connect()
    try:
        # strftime over a TIMESTAMPTZ renders in the SESSION timezone, so on a
        # machine west of Greenwich every window label came out shifted by the
        # local offset while the pandas path stayed in UTC. The two oracles
        # disagreeing on every row is what caught it. Pin the session to UTC.
        con.execute("SET TimeZone='UTC'")
        con.register("rows_in", frame)
        return con.execute(_SQL.format(hop=hop_h, k=k,
                                        fmt=_BUCKET_FORMAT[grain])).fetch_df()
    finally:
        con.close()
