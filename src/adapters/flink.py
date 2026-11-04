"""Apache Flink adapter (pyflink Table API).

Flink is driven in-process through pyflink's TableEnvironment. The neutral
windowed aggregate is expressed in Flink SQL and `read_view` executes the grain's
query, returning the maintained bucket table.

EXECUTION MODE (`mode` option, default "streaming")
---------------------------------------------------
"streaming" -- the default and the mode the paper reports. The query runs through
    Flink's *streaming* planner: `ROW_NUMBER() OVER (...) WHERE rn = 1` becomes a
    keyed Top-N/deduplication operator, `GROUP BY bucket, entity, metric` becomes
    an unbounded keyed aggregation emitting retractions, and the result arrives as
    a changelog that `_collect_changelog` folds by primary key into final state.
    This is what makes Flink's cells informative: duplicate suppression,
    arrival-order dependence and rank churn are then decided by incremental
    stateful operators, not by a batch planner reading a complete table.

"batch" -- the original path, retained for A/B comparison and as a fallback. Its
    cells test Flink's SQL *semantics* over a finite table; the streaming failure
    modes the benchmark targets cannot arise in it, so it must not be reported as
    a streaming result.

Flink SQL differences from the Postgres dialect: hour/day truncation via
`FLOOR(ts TO HOUR|DAY)`, dedup and per-bucket "latest" via `ROW_NUMBER() OVER`
(Flink's Top-N pattern). Requires `apache-flink` (pyflink) in the venv; no
external container is needed, but a cluster can be targeted via the usual Flink
config.

NOT YET IMPLEMENTED: a native event-time watermark probe (D4W). That needs a
rowtime attribute declared through connector DDL rather than `from_pandas`, so
`native_watermark_mode` stays "unsupported" here and Spark remains the only
adapter that answers D4W. Declaring otherwise would overstate the capability.
"""
from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pandas as pd

from .base import EngineAdapter, IngestStats, canonical_event_time, coerce_view, empty_view

ROOT = Path(__file__).resolve().parents[2]

#: Column order returned by every grain query; index 0..2 are the primary key.
_VIEW_FIELDS = [
    "bucket", "entity", "metric", "count", "sum", "min", "max", "mean",
    "latest_value", "latest_event_time",
]
_MODES = ("streaming", "batch")

def _row_kinds():
    """RowKind enum; its import path moved across pyflink releases."""
    try:
        from pyflink.common import RowKind
    except ImportError:  # pragma: no cover
        from pyflink.common.types import RowKind  # type: ignore
    return RowKind


def fold_changelog(result) -> list:
    """Fold a streaming changelog into final state, keyed by the view's PK.

    A streaming aggregation emits retractions: an updated bucket arrives as
    UPDATE_BEFORE then UPDATE_AFTER, a vanished key as DELETE. Applying them in
    arrival order leaves exactly the rows the maintained view holds once the
    bounded source is exhausted.
    """
    kinds = _row_kinds()
    additive = {kinds.INSERT, kinds.UPDATE_AFTER}
    state: dict = {}
    with result.collect() as iterator:
        for row in iterator:
            values = list(row)
            key = tuple(values[:3])
            if row.get_row_kind() in additive:
                state[key] = values
            else:
                state.pop(key, None)
    return list(state.values())


_FLOOR = {"hourly": "HOUR", "daily": "DAY"}
_SRC = "flink_stream_rows"


def _dedup_cte() -> str:
    """`src`: one row per dedupe_id, the first to arrive. Shared by every view."""
    # `value` is a reserved word in Flink/Calcite SQL -> must be back-quoted
    # everywhere it appears as an identifier.
    return (
        "src AS (SELECT event_time_utc, entity, metric, `value`, processing_order, dedupe_id FROM ("
        "  SELECT *, ROW_NUMBER() OVER (PARTITION BY dedupe_id ORDER BY processing_order) AS rn"
        f"  FROM {_SRC}) WHERE rn = 1)"
    )


def _hopping_sql(unit: str, variant: str, size_n: int = 2,
                 hop_n: int = 1) -> str:
    """The D8 hopping window in Flink SQL.

    Derived from `_view_sql` by replacing ONLY its `b` CTE, so the source CTE,
    the aggregate, the latest-row tiebreak and the emitted columns cannot drift
    away from the scored view. The explosion is a cross join against a VALUES
    list of hop offsets rather than Flink's HOP table function, because HOP
    needs a declared time attribute with a watermark and the scored table is
    read without one; the cross join computes the same windows from the same
    rows.
    """
    k = size_n // hop_n
    floor_unit = _FLOOR[unit]
    base = _view_sql(unit, variant)
    old = (f"b AS (\n"
           f"  SELECT FLOOR(event_time_utc TO {floor_unit}) AS bucket,\n"
           f"         entity, metric, `value`, event_time_utc, processing_order, dedupe_id\n"
           f"  FROM src\n"
           f"),")
    if old not in base:
        raise RuntimeError(
            "the Flink bucket CTE moved; _hopping_sql derives from it and must "
            "be updated with it")
    branches = "\n  UNION ALL\n".join(
        f"  SELECT FLOOR(event_time_utc TO {floor_unit})"
        f" - INTERVAL '{i * hop_n}' {floor_unit} AS bucket,\n"
        f"         entity, metric, `value`, event_time_utc, processing_order,"
        f" dedupe_id\n"
        f"  FROM src"
        for i in range(k))
    new = f"b AS (\n{branches}\n),"
    return base.replace(old, new)


def _view_sql(unit: str, variant: str) -> str:
    dedup = _dedup_cte()
    naive = f"src AS (SELECT event_time_utc, entity, metric, `value`, processing_order, dedupe_id FROM {_SRC})"
    # D7 restate (docs/SEMANTICS.md, Restatements): content-dedup, resolve each restatement's
    # supersedes pointer (COALESCE with the latest superseding value), drop the
    # restatement rows. A restatement is any row with supersedes IS NOT NULL.
    restate = (
        "dedup AS (SELECT event_time_utc, entity, metric, `value`, processing_order, dedupe_id, supersedes"
        "  FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY dedupe_id ORDER BY processing_order) AS rn"
        f"        FROM {_SRC}) WHERE rn = 1),"
        " latest_restatement AS (SELECT supersedes, `value` AS new_value FROM ("
        "   SELECT supersedes, `value`, ROW_NUMBER() OVER (PARTITION BY supersedes ORDER BY processing_order DESC) AS rr"
        "   FROM dedup WHERE supersedes IS NOT NULL) WHERE rr = 1),"
        " src AS (SELECT d.event_time_utc, d.entity, d.metric,"
        "   COALESCE(lr.new_value, d.`value`) AS `value`, d.processing_order, d.dedupe_id"
        "   FROM dedup d LEFT JOIN latest_restatement lr ON d.dedupe_id = lr.supersedes"
        "   WHERE d.supersedes IS NULL)"
    )
    # arrival_latest reads the same deduplicated source as "dedup"; only the
    # per-bucket latest tiebreak below differs.
    source = {"dedup": dedup, "naive": naive, "restate": restate,
              "arrival_latest": dedup}[variant]
    latest_order = ("processing_order DESC" if variant == "arrival_latest"
                    else "event_time_utc DESC, dedupe_id DESC")
    return f"""
WITH {source},
b AS (
  SELECT FLOOR(event_time_utc TO {_FLOOR[unit]}) AS bucket,
         entity, metric, `value`, event_time_utc, processing_order, dedupe_id
  FROM src
),
agg AS (
  SELECT bucket, entity, metric,
         COUNT(*) AS `count`, SUM(`value`) AS `sum`,
         MIN(`value`) AS `min`, MAX(`value`) AS `max`, AVG(`value`) AS `mean`
  FROM b GROUP BY bucket, entity, metric
),
latest AS (
  SELECT bucket, entity, metric, `value` AS latest_value, event_time_utc AS latest_event_time
  FROM (
    SELECT b.*, ROW_NUMBER() OVER (
             PARTITION BY bucket, entity, metric
             ORDER BY {latest_order}) AS rk
    FROM b
  ) WHERE rk = 1
)
SELECT a.bucket, a.entity, a.metric, a.`count`, a.`sum`, a.`min`, a.`max`, a.`mean`,
       l.latest_value, l.latest_event_time
FROM agg a JOIN latest l
  ON a.bucket = l.bucket AND a.entity = l.entity AND a.metric = l.metric
""".strip()


class FlinkAdapter(EngineAdapter):
    """Drives Flink by running each cell in a dedicated worker process.

    See src/adapters/_flink_worker.py for why the JVM is not shared: Calcite's
    planner metadata state does not survive alongside the other adapters in a
    long-lived matrix process, and a fresh JVM per cell is the only arrangement
    that held across all five domains.
    """

    name = "flink"
    # Streaming SQL has the full Top-N/deduplication patterns, so the D7
    # retract-latest view is expressible (unlike Spark SS / ksqlDB).
    supported_variants = ("dedup", "naive", "restate", "arrival_latest")

    def __init__(self, **options) -> None:
        super().__init__(**options)
        mode = str(options.get("mode", "streaming"))
        if mode not in _MODES:
            raise ValueError(f"flink mode must be one of {_MODES}, got {mode!r}")
        self.mode = mode
        self._work_dir: Path | None = None
        self._input_path: Path | None = None
        self._views: dict[str, pd.DataFrame] = {}

    def connect(self) -> "FlinkAdapter":
        # Fail here, not in the worker, if pyflink is missing at all.
        try:
            import pyflink  # noqa: F401
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "flink adapter needs apache-flink (pip install apache-flink)") from exc
        self._work_dir = Path(tempfile.mkdtemp(prefix="equitable_flink_"))
        return self

    def setup(self, *, variant: str = "dedup") -> None:
        self._check_variant(variant)
        self._views = {}

    def ingest_log(self, df: pd.DataFrame) -> IngestStats:
        assert self._work_dir is not None, "connect() first"
        t0 = time.perf_counter()
        stage = df[["event_time_utc", "entity", "metric", "value",
                    "processing_order", "dedupe_id", "supersedes"]].copy()
        self._input_path = self._work_dir / "input.parquet"
        stage.to_parquet(self._input_path, index=False)
        self._views = {}
        return IngestStats(rows_sent=len(df), seconds=time.perf_counter() - t0)

    def _run_worker(self) -> None:
        """Compute both grains in one child process, so one JVM start per cell."""
        assert self._work_dir is not None and self._input_path is not None
        prefix = self._work_dir / "view"
        completed = subprocess.run(
            [sys.executable, str(ROOT / "src" / "adapters" / "_flink_worker.py"),
             str(self._input_path), self.variant, self.mode, str(prefix)],
            capture_output=True, text=True, cwd=str(ROOT),
        )
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout or "").strip()
            raise RuntimeError(
                f"flink worker failed (exit {completed.returncode}): {detail[-2000:]}")
        for grain in ("hourly", "daily"):
            self._views[grain] = pd.read_parquet(f"{prefix}.{grain}.parquet")

    def read_view(self, grain: str) -> pd.DataFrame:
        self._check_grain(grain)
        if not self._views:
            self._run_worker()
        view = self._views.get(grain)
        if view is None or view.empty:
            return empty_view()
        return coerce_view(view, grain)

    def close(self) -> None:
        if self._work_dir is not None:
            shutil.rmtree(self._work_dir, ignore_errors=True)
            self._work_dir = None
        self._input_path = None
        self._views = {}
