"""Second, independent oracle for the order-sensitive query family.

`src/diagnostics/shapes.py` computes each shape's expected table in pandas.
This module computes the same tables in SQL, through DuckDB, so a shape gets
the same two-implementation discipline the rest of the contract already has: an
engine comparison is invalid if the two offline paths disagree, which turns an
oracle disagreement into a harness error rather than a finding.

The two paths share the rule and the input and nothing else. One expresses the
selection as a sort and a group tail in dataframes; the other as a window
function in a columnar SQL engine with its own planner. Left to themselves they
disagree on tie handling and null ordering, which is what makes their agreement
informative.
"""
from __future__ import annotations

from typing import Any, Dict

import duckdb
import pandas as pd

from src.diagnostics.shapes import (ARRIVAL_ORDER, BUCKET_COLUMNS,
                                    CONTRACT_ORDER, TOPK_COLUMNS)

#: The window ordering each contrast selects its distinguished row by. These
#: are the SQL spelling of shapes._sort_keys, and the two must stay in step.
_ORDER_SQL: Dict[str, str] = {
    CONTRACT_ORDER: "ts DESC, dedupe_id DESC",
    ARRIVAL_ORDER: "processing_order DESC",
}
_ORDER_SQL_ASC: Dict[str, str] = {
    CONTRACT_ORDER: "ts ASC, dedupe_id ASC",
    ARRIVAL_ORDER: "processing_order ASC",
}

_BASE = """
WITH b AS (
    SELECT bucket, entity, metric, value, event_time_utc, dedupe_id,
           processing_order, CAST(event_time_utc AS TIMESTAMPTZ) AS ts
    FROM rows_in
),
agg AS (
    SELECT bucket, entity, metric,
           COUNT(*)::BIGINT AS count, SUM(value) AS sum, MIN(value) AS min,
           MAX(value) AS max, AVG(value) AS mean
    FROM b GROUP BY bucket, entity, metric
),
chosen AS (
    SELECT bucket, entity, metric, value AS latest_value,
           event_time_utc AS latest_event_time
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY bucket, entity, metric ORDER BY {order}) AS rn
        FROM b
    ) WHERE rn = 1
)
"""


def _bucket(con, order: str, *, last: bool) -> pd.DataFrame:
    ordering = (_ORDER_SQL if last else _ORDER_SQL_ASC)[order]
    sql = _BASE.format(order=ordering) + """
    SELECT a.bucket, a.entity, a.metric, a.count, a.sum, a.min, a.max, a.mean,
           c.latest_value, c.latest_event_time
    FROM agg a JOIN chosen c USING (bucket, entity, metric)
    ORDER BY a.bucket, a.entity, a.metric
    """
    return con.execute(sql).fetch_df()[BUCKET_COLUMNS]


def _rank_by_latest(con, order: str, *, k: int = 10, **_: Any) -> pd.DataFrame:
    sql = _BASE.format(order=_ORDER_SQL[order]) + f"""
    SELECT bucket, metric, rank, entity, rank_value FROM (
        SELECT bucket, metric, entity, latest_value AS rank_value,
               ROW_NUMBER() OVER (PARTITION BY bucket, metric
                                  ORDER BY latest_value DESC, entity ASC) AS rank
        FROM chosen
    ) WHERE rank <= {int(k)}
    ORDER BY bucket, metric, rank
    """
    return con.execute(sql).fetch_df()[TOPK_COLUMNS]


def _filter_on_latest(con, order: str, *, quantile: float = 0.5,
                      **_: Any) -> pd.DataFrame:
    sql = _BASE.format(order=_ORDER_SQL[order]) + f"""
    , thresh AS (
        SELECT metric, AVG(value) AS t
        FROM b GROUP BY metric
    )
    SELECT a.bucket, a.entity, a.metric, a.count, a.sum, a.min, a.max, a.mean,
           c.latest_value, c.latest_event_time
    FROM agg a
    JOIN chosen c USING (bucket, entity, metric)
    JOIN thresh th ON th.metric = a.metric
    WHERE c.latest_value >= th.t
    ORDER BY a.bucket, a.entity, a.metric
    """
    return con.execute(sql).fetch_df()[BUCKET_COLUMNS]


_DISPATCH = {
    "S1_latest": lambda con, order, **p: _bucket(con, order, last=True),
    "S2_first": lambda con, order, **p: _bucket(con, order, last=False),
    "S3_rank_by_latest": _rank_by_latest,
    "S5_filter_on_latest": _filter_on_latest,
}


def expected(frame: pd.DataFrame, shape: str, order: str, **params: Any) -> pd.DataFrame:
    """The shape's expected table, computed independently of the pandas path."""
    if shape not in _DISPATCH:
        raise ValueError(f"no SQL oracle for shape {shape!r}")
    con = duckdb.connect()
    try:
        # Match src/oracle/duckdb_oracle.py: without this the session renders
        # TIMESTAMPTZ in local time, so latest_event_time comes back as the
        # same instant with a different offset and every key looks divergent.
        con.execute("SET TimeZone='UTC'")
        con.register("rows_in", frame)
        return _DISPATCH[shape](con, order, **params).reset_index(drop=True)
    finally:
        con.close()
