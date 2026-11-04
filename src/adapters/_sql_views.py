"""Neutral streaming-aggregation SQL shared by the SQL-dialect engines.

Materialize and RisingWave speak the PostgreSQL wire protocol and accept nearly
identical view SQL; Flink SQL, Spark SQL and Feldera SQL reuse the same logical
shape with connector-specific DDL. Centralizing the aggregation here keeps every
engine computing the *same* windowed reduction; the only intended difference
between engines is how each executes it, which is what the benchmark measures.

The view produced per (bucket, entity, metric):
    count, sum, min, max, mean, latest_value, latest_event_time
where "latest" is the row with the greatest (event_time_utc, dedupe_id)
inside the bucket -- matching the oracle's tiebreak exactly.

variant:
  "dedup" -- collapse byte-identical duplicates first (DISTINCT ON dedupe_id),
             i.e. enforce Lemma-1 condition 1. D1 passes.
  "naive" -- aggregate raw rows; injected duplicates inflate count/sum. D1 fails.
  "arrival_latest" -- the IDIOMATIC arm. Source-identity dedup exactly as
             "dedup", but per-bucket "latest" is taken in ARRIVAL order
             (processing_order DESC) instead of by (event_time_utc, dedupe_id).
             This is the formulation engineers actually write -- ksqlDB
             LATEST_BY_OFFSET, Spark last(), `DISTINCT ON ... ORDER BY offset
             DESC` -- and it is the KAFKA-10493 failure mode. It is reported
             alongside the repaired view and never replaces its verdict, so the
             matrix can show that expressibility of the safe rule, not engine
             quality alone, decides D2.
"""
from __future__ import annotations

STREAM_TABLE = "stream_rows"

# Column list (order matters for INSERT ... VALUES). `supersedes` is the D7
# pointer (null on base rows); the restate view reads it, dedup/naive ignore it.
INSERT_COLUMNS = ("event_time_utc", "entity", "metric", "value", "processing_order",
                  "dedupe_id", "supersedes")


def create_table_sql(table: str = STREAM_TABLE) -> str:
    return f"""
CREATE TABLE {table} (
  event_time_utc   timestamptz      NOT NULL,
  entity           text             NOT NULL,
  metric           text             NOT NULL,
  value            double precision NOT NULL,
  processing_order bigint           NOT NULL,
  dedupe_id        text             NOT NULL,
  supersedes       text
);
""".strip()


def _source_cte(variant: str, table: str) -> str:
    """CTE `src` = the rows the aggregate reads: deduplicated or raw."""
    if variant in ("dedup", "arrival_latest"):
        return (
            f"src AS (\n"
            f"  SELECT DISTINCT ON (dedupe_id)\n"
            f"    event_time_utc, entity, metric, value, processing_order, dedupe_id\n"
            f"  FROM {table}\n"
            f"  ORDER BY dedupe_id\n"
            f")"
        )
    if variant == "naive":
        return (
            f"src AS (\n"
            f"  SELECT event_time_utc, entity, metric, value, processing_order, dedupe_id\n"
            f"  FROM {table}\n"
            f")"
        )
    if variant == "restate":
        # D7 (docs/SEMANTICS.md, Restatements): content-dedup, then resolve each restatement's
        # `supersedes` pointer -- replace the superseded base row's value with the
        # superseding restatement of greatest processing_order (COALESCE), and drop
        # the restatement rows. A restatement is any row with supersedes IS NOT NULL.
        return (
            f"dedup AS (\n"
            f"  SELECT DISTINCT ON (dedupe_id)\n"
            f"    event_time_utc, entity, metric, value, processing_order, dedupe_id, supersedes\n"
            f"  FROM {table}\n"
            f"  ORDER BY dedupe_id\n"
            f"),\n"
            f"latest_restatement AS (\n"
            f"  SELECT DISTINCT ON (supersedes) supersedes, value AS new_value\n"
            f"  FROM dedup WHERE supersedes IS NOT NULL\n"
            f"  ORDER BY supersedes, processing_order DESC\n"
            f"),\n"
            f"src AS (\n"
            f"  SELECT b.event_time_utc, b.entity, b.metric,\n"
            f"         COALESCE(lr.new_value, b.value) AS value, b.processing_order, b.dedupe_id\n"
            f"  FROM dedup b LEFT JOIN latest_restatement lr ON b.dedupe_id = lr.supersedes\n"
            f"  WHERE b.supersedes IS NULL\n"
            f")"
        )
    raise ValueError(f"unknown variant: {variant!r}")


def _latest_order(variant: str) -> str:
    """Tiebreak for the per-bucket `latest` row.

    The contract rule is (event_time_utc, dedupe_id) DESC, which is invariant to
    replay order. The idiomatic arm deliberately uses arrival order instead, so a
    bounded reorder changes the answer -- that is the point of the arm.
    """
    if variant == "arrival_latest":
        return "processing_order DESC"
    return "event_time_utc DESC, dedupe_id DESC"


def bucket_view_sql(view: str, *, unit: str, variant: str, table: str = STREAM_TABLE) -> str:
    """CREATE MATERIALIZED VIEW SQL for one grain ('hour' | 'day')."""
    if unit not in ("hour", "day"):
        raise ValueError(f"unit must be 'hour' or 'day', got {unit!r}")
    return f"""
CREATE MATERIALIZED VIEW {view} AS
WITH {_source_cte(variant, table)},
b AS (
  SELECT date_trunc('{unit}', event_time_utc) AS bucket,
         entity, metric, value, event_time_utc, processing_order, dedupe_id
  FROM src
),
agg AS (
  SELECT bucket, entity, metric,
         count(*)::bigint            AS count,
         sum(value)::double precision AS sum,
         min(value)::double precision AS min,
         max(value)::double precision AS max,
         avg(value)::double precision AS mean
  FROM b
  GROUP BY bucket, entity, metric
),
latest AS (
  SELECT DISTINCT ON (bucket, entity, metric)
         bucket, entity, metric,
         value          AS latest_value,
         event_time_utc AS latest_event_time
  FROM b
  ORDER BY bucket, entity, metric, {_latest_order(variant)}
)
SELECT a.bucket, a.entity, a.metric,
       a.count, a.sum, a.min, a.max, a.mean,
       l.latest_value, l.latest_event_time
FROM agg a
JOIN latest l USING (bucket, entity, metric);
""".strip()



#: How each Postgres-wire engine spells a hopping window. Measured against
#: live engines, not assumed:
#:
#:   materialize  has no native hopping primitive, but expresses the window as
#:                a cross join against hop offsets. It is therefore supported
#:                for D8; a native-only formulation would have recorded a false
#:                capability gap.
#:   risingwave   rejects that cross join ("Not supported: streaming
#:                nested-loop join") and needs its native HOP instead.
#:
#: So neither formulation covers both engines, and the dialect is per engine
#: rather than shared. Both compute the same table, so the cell still compares
#: maintained state rather than syntax.
HOPPING_DIALECTS = ("crossjoin", "native_hop")

#: D8 keeps the two grains every other diagnostic reports. A grain fixes the
#: unit, and the window is SIZE units advancing by HOP units, so "hourly" is a
#: two-hour window hopping by one hour and "daily" is two days hopping by one.
#: Reporting both is not decoration: it is what lets the readiness barrier each
#: engine already implements watch the same two relations it always watches.
HOPPING_UNITS = {"hourly": "hour", "daily": "day"}


def hopping_view_sql(view: str, *, grain: str, size_n: int, hop_n: int,
                     variant: str, dialect: str = "crossjoin",
                     table: str = STREAM_TABLE) -> str:
    """CREATE MATERIALIZED VIEW SQL for a hopping window (D8).

    Emits exactly the bucket schema, with the window start in `bucket`, so no
    new comparison code, promotion rule, or provenance is needed downstream.
    Each row is assigned to the k = size/hop windows that contain it instead of
    to one; that explosion is the entire semantic difference from a tumbling
    bucket view.
    """
    if grain not in HOPPING_UNITS:
        raise ValueError(f"grain must be hourly or daily, got {grain!r}")
    if dialect not in HOPPING_DIALECTS:
        raise ValueError(f"unknown hopping dialect {dialect!r}")
    if size_n <= 0 or hop_n <= 0 or size_n % hop_n:
        raise ValueError(
            f"hopping window {size_n} must be a positive whole multiple of "
            f"hop {hop_n}")
    unit = HOPPING_UNITS[grain]
    k = size_n // hop_n
    if dialect == "native_hop":
        # The native primitive supplies window_start; the latest row per window
        # still needs the same total order the tumbling view uses, or the D8
        # table would be missing the two latest_* columns the comparator reads.
        return f"""
CREATE MATERIALIZED VIEW {view} AS
WITH {_source_cte(variant, table)},
w AS (
  SELECT window_start AS bucket, entity, metric, value, event_time_utc,
         processing_order, dedupe_id
  FROM HOP(src, event_time_utc, INTERVAL '{hop_n} {unit}',
           INTERVAL '{size_n} {unit}')
),
agg AS (
  SELECT bucket, entity, metric,
         count(*)::bigint            AS count,
         sum(value)::double precision AS sum,
         min(value)::double precision AS min,
         max(value)::double precision AS max,
         avg(value)::double precision AS mean
  FROM w
  GROUP BY bucket, entity, metric
),
latest AS (
  SELECT bucket, entity, metric, value AS latest_value,
         event_time_utc AS latest_event_time
  FROM (
    SELECT *, row_number() OVER (
      PARTITION BY bucket, entity, metric
      ORDER BY {_latest_order(variant)}) AS rn
    FROM w
  ) ranked
  WHERE rn = 1
)
SELECT a.bucket, a.entity, a.metric,
       a.count, a.sum, a.min, a.max, a.mean,
       l.latest_value, l.latest_event_time
FROM agg a
JOIN latest l USING (bucket, entity, metric);
""".strip()
    return f"""
CREATE MATERIALIZED VIEW {view} AS
WITH {_source_cte(variant, table)},
b AS (
  SELECT date_trunc('{unit}', src.event_time_utc)
           - (o.i * INTERVAL '{hop_n} {unit}') AS bucket,
         src.entity, src.metric, src.value, src.event_time_utc,
         src.processing_order, src.dedupe_id
  FROM src CROSS JOIN (SELECT generate_series(0, {k - 1}) AS i) o
),
agg AS (
  SELECT bucket, entity, metric,
         count(*)::bigint            AS count,
         sum(value)::double precision AS sum,
         min(value)::double precision AS min,
         max(value)::double precision AS max,
         avg(value)::double precision AS mean
  FROM b
  GROUP BY bucket, entity, metric
),
latest AS (
  SELECT DISTINCT ON (bucket, entity, metric)
         bucket, entity, metric,
         value          AS latest_value,
         event_time_utc AS latest_event_time
  FROM b
  ORDER BY bucket, entity, metric, {_latest_order(variant)}
)
SELECT a.bucket, a.entity, a.metric,
       a.count, a.sum, a.min, a.max, a.mean,
       l.latest_value, l.latest_event_time
FROM agg a
JOIN latest l USING (bucket, entity, metric);
""".strip()


# ---------------------------------------------------------------------------
# The order-sensitive query family (src/diagnostics/shapes.py).
#
# D1, D2 and D4 all score one shape, the per-bucket latest, under three
# interventions. These views place the same order-sensitivity rule in three
# other positions of the plan so the R2 gap can be measured over a family
# rather than over one query. Each is emitted twice, once selecting by the
# contract order and once by arrival order; the pair is what a cell scores.
#
# Every shape emits one of the two schemas the comparator already understands,
# so no shape requires new comparison code.
# ---------------------------------------------------------------------------

#: How each contrast selects its distinguished row. The SQL spelling of
#: shapes._sort_keys; the two must stay in step or the family measures nothing.
SHAPE_ORDER_SQL = {
    ("contract", True): "event_time_utc DESC, dedupe_id DESC",
    ("contract", False): "event_time_utc ASC, dedupe_id ASC",
    ("arrival", True): "processing_order DESC",
    ("arrival", False): "processing_order ASC",
}


def _shape_order(order: str, *, last: bool) -> str:
    try:
        return SHAPE_ORDER_SQL[(order, last)]
    except KeyError:
        raise ValueError(f"unknown selection order: {order!r}") from None


def _shape_base(unit: str, order: str, *, last: bool,
                table: str = STREAM_TABLE) -> str:
    """Source rows, the order-insensitive aggregate, and the chosen row.

    count, sum, min, max and mean cannot move under either order, so they are
    computed once and shared. Only `chosen` differs between the two arms, which
    is exactly the difference the family exists to measure.
    """
    return f"""
WITH {_source_cte("dedup", table)},
b AS (
  SELECT date_trunc('{unit}', event_time_utc) AS bucket,
         entity, metric, value, event_time_utc, processing_order, dedupe_id
  FROM src
),
agg AS (
  SELECT bucket, entity, metric,
         count(*)::bigint             AS count,
         sum(value)::double precision AS sum,
         min(value)::double precision AS min,
         max(value)::double precision AS max,
         avg(value)::double precision AS mean
  FROM b
  GROUP BY bucket, entity, metric
),
chosen AS (
  SELECT DISTINCT ON (bucket, entity, metric)
         bucket, entity, metric,
         value          AS latest_value,
         event_time_utc AS latest_event_time
  FROM b
  ORDER BY bucket, entity, metric, {_shape_order(order, last=last)}
)"""


def _bucket_projection() -> str:
    return """
SELECT a.bucket, a.entity, a.metric,
       a.count, a.sum, a.min, a.max, a.mean,
       c.latest_value, c.latest_event_time
FROM agg a JOIN chosen c USING (bucket, entity, metric)"""


def shape_view_sql(view: str, *, shape: str, unit: str, order: str,
                   k: int = 10, quantile: float = 0.5,
                   table: str = STREAM_TABLE) -> str:
    """CREATE MATERIALIZED VIEW SQL for one member of the family."""
    if unit not in ("hour", "day"):
        raise ValueError(f"unit must be 'hour' or 'day', got {unit!r}")

    if shape in ("S1_latest", "S2_first"):
        last = shape == "S1_latest"
        body = _shape_base(unit, order, last=last, table=table) + _bucket_projection()

    elif shape == "S3_rank_by_latest":
        # The chosen value becomes a sort key, so a wrong selection stops being
        # a wrong number in one column and becomes a wrong entity at a rank.
        # Ranking order matches src/diagnostics/d6.py: value DESC, entity ASC.
        body = _shape_base(unit, order, last=True, table=table) + f"""
SELECT bucket, metric, rank, entity, rank_value FROM (
  SELECT bucket, metric, entity,
         latest_value AS rank_value,
         row_number() OVER (PARTITION BY bucket, metric
                            ORDER BY latest_value DESC, entity ASC) AS rank
  FROM chosen
) ranked
WHERE rank <= {int(k)}"""

    elif shape == "S5_filter_on_latest":
        # The chosen value becomes a predicate, so a wrong selection changes
        # which groups exist rather than what one column holds. This is the one
        # shape whose divergence the key-union comparison catches directly.
        body = _shape_base(unit, order, last=True, table=table) + f"""
, thresh AS (
  SELECT metric, avg(value)::double precision AS t
  FROM b GROUP BY metric
)
SELECT a.bucket, a.entity, a.metric,
       a.count, a.sum, a.min, a.max, a.mean,
       c.latest_value, c.latest_event_time
FROM agg a
JOIN chosen c USING (bucket, entity, metric)
JOIN thresh th ON th.metric = a.metric
WHERE c.latest_value >= th.t"""

    else:
        raise ValueError(f"no SQL for shape {shape!r}")

    return f"CREATE MATERIALIZED VIEW {view} AS{body};".strip()
