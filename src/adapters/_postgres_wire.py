"""Shared base for the two PostgreSQL-wire streaming engines.

Materialize (port 6877) and RisingWave (port 4566) both expose the Postgres wire
protocol and accept the neutral view SQL in `_sql_views.py`. They differ only in
connection parameters and in how a reader waits for the materialized view to
reflect just-ingested rows (`_await_hydrated`), so both are thin subclasses of
`PostgresWireAdapter`.
"""
from __future__ import annotations

import time

import pandas as pd

from .base import (EngineAdapter, IngestStats, canonical_bucket,
                   canonical_event_time, canonical_shape_frame, coerce_view)
from ._sql_views import (
    INSERT_COLUMNS,
    STREAM_TABLE,
    bucket_view_sql,
    hopping_view_sql,
    create_table_sql,
    shape_view_sql,
)

_VIEW = {"hourly": "v_hourly", "daily": "v_daily"}
_TOPK_VIEW = {"hourly": "v_topk_hourly", "daily": "v_topk_daily"}
_UNIT = {"hourly": "hour", "daily": "day"}


class PostgresWireAdapter(EngineAdapter):
    """Common connect/setup/ingest/read_view over psycopg2. Subclasses set
    connection defaults and (optionally) override `_await_hydrated`."""

    #: default connection kwargs for psycopg2.connect; subclasses override.
    conn_kwargs: dict = {}
    #: SQL view supports the D7 retract-latest ("restate") variant.
    supported_variants = ("dedup", "naive", "restate", "arrival_latest")
    native_topk_mode = "incremental"

    def __init__(self, **options) -> None:
        super().__init__(**options)
        self._conn = None

    # --- connection -------------------------------------------------------
    def connect(self) -> "PostgresWireAdapter":
        try:
            import psycopg2  # noqa: F401
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                f"{self.name} needs psycopg2 (pip install psycopg2-binary)"
            ) from exc
        import psycopg2

        kwargs = {**self.conn_kwargs, **self.options.get("conn_kwargs", {})}
        # Quiet Materialize's connect-time NOTICE banner (docs/chat spam) without
        # hiding real errors. Callers can still override via conn_kwargs.options.
        kwargs.setdefault("options", "-c client_min_messages=warning")
        self._conn = psycopg2.connect(**kwargs)
        self._conn.autocommit = True
        # Drain any residual notices so they don't pile up on the connection.
        try:
            self._conn.notices.clear()
        except Exception:
            pass
        return self

    def _cursor(self):
        if self._conn is None:
            raise RuntimeError("call connect() before use")
        return self._conn.cursor()

    # --- lifecycle --------------------------------------------------------
    def setup(self, *, variant: str = "dedup") -> None:
        self._check_variant(variant)
        cur = self._cursor()
        cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {_TOPK_VIEW['hourly']} CASCADE;")
        cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {_TOPK_VIEW['daily']} CASCADE;")
        cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {_VIEW['hourly']} CASCADE;")
        cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {_VIEW['daily']} CASCADE;")
        cur.execute(f"DROP TABLE IF EXISTS {STREAM_TABLE} CASCADE;")
        cur.execute(create_table_sql())
        cur.execute(bucket_view_sql(_VIEW["hourly"], unit="hour", variant=variant))
        cur.execute(bucket_view_sql(_VIEW["daily"], unit="day", variant=variant))
        k = self.options.get("native_topk_k")
        if k is not None:
            metric = self.options["native_topk_metric"]
            for grain in ("hourly", "daily"):
                cur.execute(self._native_topk_sql(grain, int(k), metric))
        cur.close()


    # --- order-sensitive query family -------------------------------------
    #: Both engines speak enough of the PostgreSQL surface to build every shape
    #: under either selection order, so they opt into the whole family.
    supported_shapes = ("S1_latest", "S2_first", "S3_rank_by_latest",
                        "S5_filter_on_latest")

    def setup_shape(self, *, shape: str, order: str) -> None:
        """Build the family member under the SCORED view names.

        Reusing `_VIEW` rather than a parallel set of names is deliberate: the
        readiness barrier each engine already implements watches exactly those
        relations, so a shape run gets the same real barrier as a scored run
        instead of a weaker one written for this path.
        """
        if shape not in self.supported_shapes:
            raise NotImplementedError(
                f"{self.name} cannot express shape {shape!r}")
        cur = self._cursor()
        for view in (_TOPK_VIEW["hourly"], _TOPK_VIEW["daily"],
                     _VIEW["hourly"], _VIEW["daily"]):
            cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view} CASCADE;")
        cur.execute(f"DROP TABLE IF EXISTS {STREAM_TABLE} CASCADE;")
        cur.execute(create_table_sql())
        for grain, unit in (("hourly", "hour"), ("daily", "day")):
            cur.execute(shape_view_sql(
                _VIEW[grain], shape=shape, unit=unit, order=order,
                k=int(self.options.get("shape_k", 10)),
                quantile=float(self.options.get("shape_quantile", 0.5))))
        cur.close()

    # --- D8: hopping window ------------------------------------------------
    def setup_hopping(self, *, size_n: int = 2, hop_n: int = 1) -> None:
        """Build the hopping view under the SCORED view name.

        Same reasoning as setup_shape: the engine's readiness barrier watches
        `_VIEW["hourly"]`, so building here gets the real barrier rather than a
        weaker one written for this path.
        """
        if self.hopping_mode == "unsupported":
            raise NotImplementedError(
                f"{self.name} cannot express a hopping window")
        cur = self._cursor()
        for view in (_TOPK_VIEW["hourly"], _TOPK_VIEW["daily"],
                     _VIEW["hourly"], _VIEW["daily"]):
            cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view} CASCADE;")
        cur.execute(f"DROP TABLE IF EXISTS {STREAM_TABLE} CASCADE;")
        cur.execute(create_table_sql())
        for grain in ("hourly", "daily"):
            cur.execute(hopping_view_sql(
                _VIEW[grain], grain=grain, size_n=size_n, hop_n=hop_n,
                variant=self.variant, dialect=self.hopping_mode))
        cur.close()

    def read_hopping(self, grain: str) -> pd.DataFrame:
        self._check_grain(grain)
        cur = self._cursor()
        cur.execute(f"SELECT * FROM {_VIEW[grain]}")
        cols = [d[0] for d in cur.description]
        records = cur.fetchall()
        cur.close()
        if not records:
            from .base import empty_view
            return empty_view()
        frame = pd.DataFrame(records, columns=cols)
        frame["latest_event_time"] = canonical_event_time(
            frame["latest_event_time"])
        # The hopping table is the bucket table, keyed by window start, so the
        # grain's own coercion is the right one.
        return coerce_view(frame, grain)

    def read_shape(self, grain: str) -> pd.DataFrame:
        self._check_grain(grain)
        cur = self._cursor()
        cur.execute(f"SELECT * FROM {_VIEW[grain]};")
        columns = [d[0] for d in cur.description]
        frame = pd.DataFrame(cur.fetchall(), columns=columns)
        cur.close()
        return canonical_shape_frame(frame, grain)

    @staticmethod
    def _native_topk_sql(grain: str, k: int, metric: str) -> str:
        return f"""
CREATE MATERIALIZED VIEW {_TOPK_VIEW[grain]} AS
SELECT bucket, metric, rank, entity, rank_value
FROM (
  SELECT bucket, metric, entity, {metric} AS rank_value,
         ROW_NUMBER() OVER (
           PARTITION BY bucket, metric
           ORDER BY {metric} DESC, entity ASC
         ) AS rank
  FROM {_VIEW[grain]}
) ranked
WHERE rank <= {k};
""".strip()

    def ingest_log(self, df: pd.DataFrame) -> IngestStats:
        import psycopg2.extras

        batch_size = int(self.options.get("batch_size", 5000))
        rows = list(zip(
            [t.to_pydatetime() for t in df["event_time_utc"]],
            df["entity"], df["metric"], df["value"].astype(float),
            df["processing_order"].astype("int64"), df["dedupe_id"],
            df["supersedes"].where(df["supersedes"].notna(), None),  # null on base rows
        ))
        cols = ",".join(INSERT_COLUMNS)
        insert = f"INSERT INTO {STREAM_TABLE}({cols}) VALUES %s"
        cur = self._cursor()
        t0 = time.perf_counter()
        for i in range(0, len(rows), batch_size):
            psycopg2.extras.execute_values(cur, insert, rows[i:i + batch_size])
        self._await_hydrated(cur, len(rows))
        seconds = time.perf_counter() - t0
        cur.close()
        return IngestStats(rows_sent=len(rows), seconds=seconds)

    def read_view(self, grain: str) -> pd.DataFrame:
        self._check_grain(grain)
        # Fetch through the DB-API cursor (not pd.read_sql, which warns unless
        # given a SQLAlchemy connectable).
        cur = self._cursor()
        cur.execute(f"SELECT * FROM {_VIEW[grain]}")
        cols = [d[0] for d in cur.description]
        records = cur.fetchall()
        cur.close()
        if not records:
            from .base import empty_view
            return empty_view()
        df = pd.DataFrame(records, columns=cols)
        df["latest_event_time"] = canonical_event_time(df["latest_event_time"])
        return coerce_view(df, grain)

    def read_native_topk(self, grain: str) -> pd.DataFrame:
        self._check_grain(grain)
        from src.diagnostics.d6 import TOPK_COLUMNS
        cur = self._cursor()
        cur.execute(f"SELECT bucket, metric, rank, entity, rank_value FROM {_TOPK_VIEW[grain]}")
        records = cur.fetchall()
        cur.close()
        if not records:
            return pd.DataFrame(columns=TOPK_COLUMNS)
        out = pd.DataFrame(records, columns=TOPK_COLUMNS)
        out["bucket"] = canonical_bucket(out["bucket"], grain)
        out["rank"] = out["rank"].astype("int64")
        out["rank_value"] = out["rank_value"].astype(float)
        return out.sort_values(["bucket", "metric", "rank"], kind="stable").reset_index(drop=True)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def checkpoint(self) -> None:
        """Durability barrier (D5): reuse the engine's hydration barrier --
        RisingWave FLUSHes through the checkpoint barrier, Materialize polls the
        maintained view until it is stable (its persist layer commits INSERTs
        synchronously, so a settled view implies durable state)."""
        cur = self._cursor()
        try:
            self._await_hydrated(cur, 0)
        finally:
            cur.close()

    # --- hydration hook ---------------------------------------------------
    def _await_hydrated(self, cur, expected_rows: int) -> None:
        """Block until the views reflect every ingested row. Default: read the
        base table count back (a same-session SELECT is consistent with prior
        INSERTs on both engines). Subclasses add engine-specific barriers."""
        cur.execute(f"SELECT count(*) FROM {STREAM_TABLE};")
        cur.fetchone()
