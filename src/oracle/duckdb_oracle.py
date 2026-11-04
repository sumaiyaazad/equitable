"""Second, fully independent offline oracle (DuckDB / SQL), domain-neutral.

This oracle shares no Python code with the engine or the pandas oracle: it does
its own deduplication, bucketing, and aggregation entirely in SQL. It is the
independent third implementation that makes the online--oracle comparison a
genuine cross-check rather than self-consistency ("who checks the oracle?").

Output schema matches ``pandas_oracle`` so the same comparator works on it.
Requires the ``duckdb`` package.
"""
from __future__ import annotations

from typing import Any, Dict, Sequence

import pandas as pd

BUCKET_COLUMNS = ["bucket", "entity", "metric", "count", "sum", "min", "max",
                  "mean", "latest_value", "latest_event_time"]

# grain -> DuckDB strftime format producing the same bucket string the engine uses
_GRAIN = {
    "hour": "%Y-%m-%dT%H:%M:%SZ",   # date_trunc('hour') zeroes min/sec -> ...T12:00:00Z
    "day": "%Y-%m-%d",
}


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=BUCKET_COLUMNS)


def _aggregate(con, grain: str, restatement_aware: bool = False) -> pd.DataFrame:
    fmt = _GRAIN[grain]
    if restatement_aware:
        # D7 (docs/SEMANTICS.md, Restatements): after content-dedup, resolve each restatement's
        # `supersedes` pointer -- replace the superseded base row's value with the
        # superseding restatement of greatest processing_order (COALESCE), and drop
        # the restatement rows. Keyed on dedupe_id, so it works regardless of how
        # many events share a timestamp.
        prefix = """dedup AS (
        SELECT entity, metric, value, processing_order, event_time_utc, dedupe_id,
               supersedes,
               CAST(event_time_utc AS TIMESTAMPTZ) AS ts
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY dedupe_id ORDER BY processing_order) AS rn
            FROM raw
        ) WHERE rn = 1
    ),
    latest_restatement AS (
        SELECT supersedes, value AS new_value FROM (
            SELECT supersedes, value,
                   ROW_NUMBER() OVER (PARTITION BY supersedes ORDER BY processing_order DESC) AS rr
            FROM dedup WHERE supersedes IS NOT NULL
        ) WHERE rr = 1
    ),
    src AS (
        SELECT b.entity, b.metric, COALESCE(lr.new_value, b.value) AS value,
               b.processing_order, b.event_time_utc, b.dedupe_id, b.ts
        FROM dedup b LEFT JOIN latest_restatement lr ON b.dedupe_id = lr.supersedes
        WHERE b.supersedes IS NULL
    )"""
        b_source = "src"
    else:
        prefix = """dedup AS (
        SELECT entity, metric, value, processing_order, event_time_utc, dedupe_id,
               CAST(event_time_utc AS TIMESTAMPTZ) AS ts
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY dedupe_id ORDER BY processing_order) AS rn
            FROM raw
        ) WHERE rn = 1
    )"""
        b_source = "dedup"
    sql = f"""
    WITH {prefix},
    b AS (
        SELECT *, strftime(date_trunc('{grain}', ts), '{fmt}') AS bucket FROM {b_source}
    ),
    agg AS (
        SELECT bucket, entity, metric,
               COUNT(*)::BIGINT AS count, SUM(value) AS sum, MIN(value) AS min,
               MAX(value) AS max, AVG(value) AS mean
        FROM b GROUP BY bucket, entity, metric
    ),
    latest AS (
        SELECT bucket, entity, metric, value AS latest_value, event_time_utc AS latest_event_time
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                       PARTITION BY bucket, entity, metric
                       ORDER BY ts DESC, dedupe_id DESC) AS rn
            FROM b
        ) WHERE rn = 1
    )
    SELECT a.bucket, a.entity, a.metric, a.count, a.sum, a.min, a.max, a.mean,
           l.latest_value, l.latest_event_time
    FROM agg a JOIN latest l USING (bucket, entity, metric)
    ORDER BY a.bucket, a.entity, a.metric
    """
    return con.execute(sql).df()


def build_oracle_tables_from_rows(
    rows: Sequence[Dict[str, Any]], *, restatement_aware: bool = False
) -> Dict[str, Any]:
    import duckdb

    df = pd.DataFrame(list(rows))  # pandas used only as a data container; all logic is SQL
    if df.empty:
        return {"hourly": _empty(), "daily": _empty(),
                "summary": {"rows_loaded": 0, "engine": "duckdb"}}

    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.register("raw", df)
    hourly = _aggregate(con, "hour", restatement_aware)
    daily = _aggregate(con, "day", restatement_aware)
    con.close()

    return {
        "hourly": hourly,
        "daily": daily,
        "summary": {
            "rows_loaded": int(len(df)),
            "hourly_keys": int(len(hourly)),
            "daily_keys": int(len(daily)),
            "restatement_aware": bool(restatement_aware),
            "engine": "duckdb",
        },
    }
