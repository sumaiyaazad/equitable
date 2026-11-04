"""Engine-adapter contract.

One small interface every system under test (SUT) implements, so the diagnostic
runner (Step 6) can drive the reference engine, four ported streaming engines
(Materialize, RisingWave, ksqlDB, Flink) and two new ones (Spark Structured
Streaming, Feldera) through the *identical* code path:

    with get_adapter("materialize").connect() as eng:
        eng.setup(variant="dedup")
        eng.ingest_log(df)                 # df = a normalized log / fixture
        hourly = eng.read_view("hourly")   # comparator-schema bucket table
        daily  = eng.read_view("daily")

`read_view` returns a pandas DataFrame in the SAME schema the oracle emits
(`src/oracle/pandas_oracle.BUCKET_COLUMNS` == `comparator` key+fields), so a
result can be diffed against the oracle with `compare_bucket_tables` with no
per-engine special-casing:

    ["bucket", "entity", "metric",              # key (exact)
     "count",                                    # exact
     "sum", "min", "max", "mean", "latest_value",# epsilon-bounded floats
     "latest_event_time"]                        # exact string

The `variant` knob selects whether the engine's view enforces source-identity
deduplication ("dedup") or not ("naive"). D1 is exactly the gap between the two:
a correct engine's naive view double-counts injected duplicates, its dedup view
does not. Engines that dedupe unconditionally (the reference engine) support
only "dedup".

Adapters own only *connection + translation*; they contain no aggregation logic
of their own beyond the standard windowed reduction, and no diagnostic logic
(that lives in the runner). Optional heavy drivers (psycopg2, pyflink, pyspark,
confluent-kafka, requests) are imported lazily inside each adapter so importing
this package never requires all seven engines' clients to be installed.
"""
from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import Any, Dict, List

import pandas as pd
import numpy as np

from src.schema.record import (
    INGEST_TIME,
    SOURCE_RECORD_TYPE,
    coerce_processing_order,
    is_explicit_utc_timestamp,
)

# The canonical view schema. Identical to src/oracle/pandas_oracle.BUCKET_COLUMNS
# and to src/oracle/comparator KEY_COLUMNS + count + FLOAT_FIELDS + latest_event_time.
KEY_COLUMNS: List[str] = ["bucket", "entity", "metric"]
VIEW_COLUMNS: List[str] = KEY_COLUMNS + [
    "count", "sum", "min", "max", "mean", "latest_value", "latest_event_time",
]

GRAINS: Dict[str, str] = {"hourly": "hour", "daily": "day"}
# dedup: source-identity dedupe only (D1). naive: no dedupe (D1 fails).
# restate: dedupe, resolve each base observation through its `supersedes` pointer,
#   and aggregate the corrected observations (D7). Valid fixtures permit at most
#   one restatement per base observation.
# arrival_latest: the IDIOMATIC arm -- dedupe as "dedup" but pick per-bucket
#   latest in arrival order, the formulation engineers actually write. Reported
#   beside the repaired view; never replaces its verdict.
VARIANTS = ("dedup", "naive", "restate", "arrival_latest")

# Columns of the normalized log an adapter may rely on being present.
INPUT_COLUMNS: List[str] = [
    "event_time_utc", "entity", "metric", "value", "processing_order",
    "dedupe_id", SOURCE_RECORD_TYPE,
]
# D7-only: a restatement's pointer to the dedupe_id it supersedes. Present only in
# the d7_restate fixture; canonical_input synthesizes it as null for every other
# input so the column always exists (the restate view reads it, others ignore it).
SUPERSEDES = "supersedes"


@dataclass
class IngestStats:
    """What an adapter reports back after loading a log."""
    rows_sent: int = 0
    seconds: float = 0.0
    extra: Dict[str, Any] = field(default_factory=dict)

    @property
    def rows_per_sec(self) -> float:
        return self.rows_sent / self.seconds if self.seconds > 0 else 0.0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "rows_sent": self.rows_sent,
            "seconds": self.seconds,
            "rows_per_sec": self.rows_per_sec,
            **self.extra,
        }


def _parse_explicit_utc_series(
    series: pd.Series, field: str, *, optional: bool = False,
) -> pd.Series:
    """Parse an ISO timestamp series without silently localizing naive values."""
    text = series.astype("string").str.strip()
    missing = series.isna() | text.fillna("").eq("")
    if not optional and bool(missing.any()):
        raise ValueError(f"input log contains invalid {field}")

    present = ~missing
    valid_utc = series.loc[present].map(is_explicit_utc_timestamp)
    if not bool(valid_utc.all()):
        raise ValueError(f"input log contains invalid {field}")
    parsed = pd.to_datetime(series, utc=True, errors="coerce", format="mixed")
    if bool(parsed.loc[present].isna().any()):
        raise ValueError(f"input log contains invalid {field}")
    return parsed


def canonical_input(
    df: pd.DataFrame, *, allow_restatements: bool = False,
) -> pd.DataFrame:
    """Validate, project, and type a normalized log/fixture for adapters.

    Event time is returned as a timezone-aware timestamp; entity, metric,
    dedupe ID, and source-record type as strings; value as float; and replay
    order as int64.

    Validation deliberately happens against ``df`` *before* projection. This
    keeps a missing/invalid ``source_record_type`` or malformed timestamp from
    disappearing merely because an engine does not use that field in its SQL.
    Ordinary normalized rows must be ``data``. D7 fixture rows may be admitted
    only through the explicit ``allow_restatements=True`` exception, and then
    only when they carry a non-empty ``supersedes`` pointer; conversely, a
    ``data`` row may not carry such a pointer.
    """
    missing = [c for c in INPUT_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"input log is missing required columns: {missing}")

    for col in ("entity", "metric", "dedupe_id"):
        if df[col].isna().any() or df[col].astype(str).str.strip().eq("").any():
            raise ValueError(f"input log contains empty {col}")

    event_time = _parse_explicit_utc_series(
        df["event_time_utc"], "event_time_utc")
    if INGEST_TIME in df.columns:
        _parse_explicit_utc_series(
            df[INGEST_TIME], INGEST_TIME, optional=True)

    value = pd.to_numeric(df["value"], errors="coerce")
    if (~np.isfinite(value)).any():
        raise ValueError("input log contains non-finite value")
    try:
        processing_order = df["processing_order"].map(
            coerce_processing_order).astype("int64")
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("input log contains invalid processing_order") from exc

    source_type_raw = df[SOURCE_RECORD_TYPE]
    if (source_type_raw.isna().any()
            or source_type_raw.astype(str).str.strip().eq("").any()):
        raise ValueError("input log contains empty source_record_type")
    source_type = source_type_raw.astype(str)
    is_data = source_type.eq("data")
    is_restatement = source_type.eq("restatement")
    invalid_type = ~(is_data | is_restatement)
    if invalid_type.any():
        values = sorted(source_type[invalid_type].unique().tolist())
        raise ValueError(
            f"input log contains invalid source_record_type: {values}")

    if SUPERSEDES in df.columns:
        supersedes = df[SUPERSEDES]
        has_supersedes = (
            supersedes.notna()
            & supersedes.astype(str).str.strip().ne("")
        )
    else:
        supersedes = pd.Series(None, index=df.index, dtype=object)
        has_supersedes = pd.Series(False, index=df.index, dtype=bool)
    if bool((is_restatement & ~has_supersedes).any()):
        raise ValueError(
            "restatement source_record_type requires a supersedes pointer")
    if bool((is_data & has_supersedes).any()):
        raise ValueError("data source_record_type cannot carry supersedes")
    if bool(is_restatement.any()) and not allow_restatements:
        raise ValueError(
            "restatement source_record_type requires "
            "allow_restatements=True (D7 only)")

    entity = df["entity"].astype(str)
    metric = df["metric"].astype(str)
    dedupe_id = df["dedupe_id"].astype(str)
    typed = pd.DataFrame({
        "dedupe_id": dedupe_id, "entity": entity, "metric": metric,
        "event_time_utc": event_time, "value": value,
    }, index=df.index)
    logical = ["entity", "metric", "event_time_utc", "value"]
    collisions = (typed.groupby("dedupe_id", dropna=False)[logical]
                  .nunique(dropna=False).max(axis=1) > 1)
    if collisions.any():
        raise ValueError(
            "dedupe_id maps to conflicting source payloads: "
            f"{int(collisions.sum())}")

    out = df[INPUT_COLUMNS].copy()
    out["event_time_utc"] = event_time
    out["entity"] = entity
    out["metric"] = metric
    out["value"] = value
    out["processing_order"] = processing_order
    out["dedupe_id"] = dedupe_id
    out[SOURCE_RECORD_TYPE] = source_type
    # Every adapter sees one uniform nullable D7 pointer column.
    out[SUPERSEDES] = supersedes.where(has_supersedes, None).astype(object)
    return out


def canonical_event_time(series: pd.Series) -> pd.Series:
    """Format a timestamp series to the log's canonical string
    '%Y-%m-%dT%H:%M:%S.%fZ' so latest_event_time matches the oracle exactly
    regardless of how an engine round-trips timestamps.

    `format="ISO8601"` parses each element on its own rather than inferring one
    format from the first and requiring the rest to match it. An engine may
    return mixed precision within a single view -- Feldera v0.324.0 drops the
    fractional part when it is zero and keeps it otherwise -- which inference
    rejects outright. Values that already parsed are unaffected: this widens
    what is accepted, it does not change how anything is interpreted."""
    ts = pd.to_datetime(series, utc=True, format="ISO8601")
    return ts.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# The oracle/engine encode the bucket as a STRING, and the format differs by
# grain: hour floor keeps the time ('...THH:00:00Z'), day is a bare calendar
# date ('YYYY-MM-DD'). Matches src/schema/bucketing.to_hour_bucket/to_day_bucket.
_BUCKET_FMT = {"hourly": "%Y-%m-%dT%H:00:00Z", "daily": "%Y-%m-%d"}


def canonical_bucket(series: pd.Series, grain: str) -> pd.Series:
    """Render a bucket column (timestamps OR already-canonical strings) to the
    exact string the oracle uses for `grain`, so the key merge lines up."""
    if grain not in _BUCKET_FMT:
        raise ValueError(f"grain must be one of {list(_BUCKET_FMT)}, got {grain!r}")
    ts = pd.to_datetime(series, utc=True)
    return ts.dt.strftime(_BUCKET_FMT[grain])


def empty_view() -> pd.DataFrame:
    return pd.DataFrame(columns=VIEW_COLUMNS)


def coerce_view(df: pd.DataFrame, grain: str) -> pd.DataFrame:
    """Return df restricted + ordered to VIEW_COLUMNS, sorted by key, with the
    bucket rendered to the oracle's canonical STRING for `grain`. Every
    adapter's read_view runs its result through this so the comparator sees a
    uniform frame."""
    for col in VIEW_COLUMNS:
        if col not in df.columns:
            raise ValueError(f"read_view result missing column '{col}'")
    out = df[VIEW_COLUMNS].copy()
    out["bucket"] = canonical_bucket(out["bucket"], grain)
    out["count"] = out["count"].astype("int64")
    for col in ("sum", "min", "max", "mean", "latest_value"):
        out[col] = out[col].astype(float)
    out["entity"] = out["entity"].astype(str)
    out["metric"] = out["metric"].astype(str)
    out["latest_event_time"] = out["latest_event_time"].astype(str)
    return out.sort_values(KEY_COLUMNS, kind="stable").reset_index(drop=True)


#: Schemas the order-sensitive family emits. Both are already produced and
#: compared elsewhere in the harness, which is why no shape needs new
#: comparison code: see src/diagnostics/shapes.py.
SHAPE_BUCKET_COLUMNS: List[str] = list(VIEW_COLUMNS)
SHAPE_TOPK_COLUMNS: List[str] = ["bucket", "metric", "rank", "entity",
                                 "rank_value"]


def canonical_shape_frame(df: pd.DataFrame, grain: str = "hourly") -> pd.DataFrame:
    """Canonicalize a shape view in whichever of the two schemas it emits.

    The bucket schema is coerced exactly as `coerce_view` does, so a shape
    result and a scored view are the same kind of frame. The top-k schema is
    coerced the way the D6 comparison expects. Detecting the schema from the
    columns keeps the adapters from having to declare it twice, once in the SQL
    and once in Python.
    """
    if "rank" in df.columns:
        for col in SHAPE_TOPK_COLUMNS:
            if col not in df.columns:
                raise ValueError(f"shape view missing column '{col}'")
        out = df[SHAPE_TOPK_COLUMNS].copy()
        out["bucket"] = canonical_bucket(out["bucket"], grain)
        out["metric"] = out["metric"].astype(str)
        out["entity"] = out["entity"].astype(str)
        out["rank"] = out["rank"].astype("int64")
        out["rank_value"] = out["rank_value"].astype(float)
        return out.sort_values(["bucket", "metric", "rank"],
                               kind="stable").reset_index(drop=True)
    # The oracle carries `latest_event_time` as a pandas Timestamp and each
    # engine returns it in its own rendering, so both sides must be put in the
    # contract's canonical form before they are compared. Doing it here rather
    # than per caller is what keeps an accidental agreement between two
    # non-canonical renderings from reading as a passing cell.
    out = df.copy()
    if "latest_event_time" in out.columns:
        out["latest_event_time"] = canonical_event_time(out["latest_event_time"])
    return coerce_view(out, grain)


class NativeNotExpressible(RuntimeError):
    """An engine explicitly rejected a native semantic formulation.

    Adapters must raise this only for a compiler or feature-capability rejection.
    Transport, authentication, timeout, service, and unexpected setup failures
    are execution blockers and must use their ordinary exception types.
    """


class EngineAdapter(abc.ABC):
    """Abstract base for every system under test.

    Lifecycle: ``connect()`` (or use as a context manager) -> ``setup(variant)``
    -> ``ingest_log(df)`` -> ``read_view(grain)`` -> ``close()``. Subclasses
    override the three lifecycle hooks; the context-manager sugar is shared.
    """

    #: short stable id used in registries, output paths, and summaries.
    name: str = "abstract"
    #: variants this engine can express. Default excludes "restate" (D7); an
    #: adapter opts in only once it implements the retract-latest view.
    supported_variants: tuple[str, ...] = ("dedup", "naive")
    #: Native semantics are opt-in capabilities. Unsupported is an explicit
    #: experimental classification, not a failed adapter invocation.
    native_topk_mode: str = "unsupported"
    native_watermark_mode: str = "unsupported"
    #: Members of the order-sensitive query family this engine can express
    #: (src/diagnostics/shapes.py). Empty by default: an adapter opts in once
    #: it can build the view under BOTH selection orders, because a shape it
    #: can only express one way measures nothing. An engine with no event-time
    #: selection primitive therefore stays empty, and that is a capability
    #: result rather than a failure.
    supported_shapes: tuple[str, ...] = ()
    #: How this engine expresses the D8 hopping window, or "unsupported".
    #: Values are src.adapters._sql_views.HOPPING_DIALECTS plus engine-specific
    #: modes. This is per engine because no single formulation covers them all:
    #: Materialize has no native hopping primitive but accepts a cross join
    #: against hop offsets, while RisingWave rejects that cross join as a
    #: streaming nested-loop join and needs its native HOP. Both compute the
    #: same table, so a D8 cell still compares maintained state, not syntax.
    hopping_mode: str = "unsupported"

    def __init__(self, **options: Any) -> None:
        self.options = options
        self.variant: str = "dedup"

    # --- lifecycle hooks (subclasses implement) ---------------------------
    def connect(self) -> "EngineAdapter":
        """Establish a connection / session. Default: nothing to do."""
        return self

    @abc.abstractmethod
    def setup(self, *, variant: str = "dedup") -> None:
        """(Re)create the stream table and the hourly/daily views for `variant`.
        Must be idempotent: dropping any prior state first."""

    @abc.abstractmethod
    def ingest_log(self, df: pd.DataFrame) -> IngestStats:
        """Feed a normalized log/fixture (canonical_input columns) into the
        engine and block until its views reflect every row."""

    @abc.abstractmethod
    def read_view(self, grain: str) -> pd.DataFrame:
        """Return the materialized bucket table for grain in {'hourly','daily'}
        as a VIEW_COLUMNS frame (run through ``coerce_view``)."""

    def setup_shape(self, *, shape: str, order: str) -> None:
        """(Re)create the views for one member of the order-sensitive family.

        `order` is "contract" or "arrival". Raising NotImplementedError is the
        documented way to decline: the harness records the cell as unsupported,
        exactly as it does for a missing restatement or checkpoint interface.
        """
        raise NotImplementedError(
            f"{self.name} cannot express the order-sensitive query family")

    def read_shape(self, grain: str) -> pd.DataFrame:
        """Return the shape view for `grain`, in whichever schema it emits."""
        raise NotImplementedError(
            f"{self.name} cannot express the order-sensitive query family")

    def setup_hopping(self, *, size_n: int = 2, hop_n: int = 1) -> None:
        """(Re)create the D8 hopping-window views, one per grain.

        Declining with NotImplementedError records the cell as unsupported,
        exactly as a missing restatement or checkpoint interface does.
        """
        raise NotImplementedError(
            f"{self.name} cannot express a hopping window")

    def read_hopping(self, grain: str) -> pd.DataFrame:
        """Return the hopping table: the bucket schema keyed by window start."""
        raise NotImplementedError(
            f"{self.name} cannot express a hopping window")

    def configure_native_topk(self, k: int, rank_metric: str) -> None:
        """Configure an engine-maintained Top-N view before setup."""
        if self.native_topk_mode != "incremental":
            raise NotImplementedError(f"{self.name} has no incremental native Top-N adapter")
        if k < 1:
            raise ValueError("k must be positive")
        if rank_metric not in ("count", "sum", "min", "max", "mean",
                               "latest_value"):
            raise ValueError(f"unsupported Top-N rank metric: {rank_metric}")
        self.options["native_topk_k"] = int(k)
        self.options["native_topk_metric"] = rank_metric

    def read_native_topk(self, grain: str) -> pd.DataFrame:
        """Read an engine-maintained Top-N view in D6 TOPK_COLUMNS form."""
        raise NotImplementedError(f"{self.name} has no incremental native Top-N adapter")

    def run_native_watermark_probe(self, allowed_lateness_s: int) -> Dict[str, Any]:
        """Execute a real event-time watermark probe in the native runtime."""
        raise NotImplementedError(f"{self.name} has no native watermark probe")

    def close(self) -> None:
        """Release the connection / tear down resources. Default: no-op."""

    def detach(self) -> None:
        """Release the CLIENT connection but leave engine-side state running.

        A durability or recovery check must not tear down the very state whose
        survival it is about to measure. `close()` is allowed to stop the
        engine's maintaining queries; `detach()` never is. Default: close(),
        which is correct for every adapter whose close only drops a socket.
        """
        self.close()

    def checkpoint(self) -> None:
        """Force a durability barrier: block until every ingested row is
        durably committed, so the state a crash-and-restart must restore is
        deterministic (D5, scripts/run/run_d5_recovery.py). Default: no-op (in-process
        engines have no durable state to barrier)."""

    def resume(self) -> None:
        """D5 recovery hook: after a crash+restart, bring the ALREADY-EXISTING
        materialized state back online WITHOUT re-running setup() (which would
        wipe it), so read_view() returns the restored state. Default: no-op --
        engines whose view is a live server object (Materialize, RisingWave) are
        queryable again as soon as the container is healthy. Feldera overrides
        this: its pipeline comes back stopped and must be restarted to restore
        computed state from the last checkpoint."""

    # --- shared helpers ---------------------------------------------------
    def _check_variant(self, variant: str) -> str:
        if variant not in self.supported_variants:
            raise ValueError(
                f"{self.name} does not support variant {variant!r}; "
                f"supported: {self.supported_variants}"
            )
        self.variant = variant
        return variant

    @staticmethod
    def _check_grain(grain: str) -> str:
        if grain not in GRAINS:
            raise ValueError(f"grain must be one of {list(GRAINS)}, got {grain!r}")
        return grain

    def __enter__(self) -> "EngineAdapter":
        return self.connect()

    def __exit__(self, *exc: Any) -> None:
        self.close()
