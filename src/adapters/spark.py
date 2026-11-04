"""Spark Structured Streaming adapter (pyspark) -- new SUT for catalog coverage.

Spark is exercised as a genuine Structured Streaming job, not a batch query: the
normalized log is staged to a directory, read back with `readStream`, and folded
by a streaming aggregation written to an in-memory sink in *complete* output
mode. `read_view` triggers one micro-batch over all currently-available files
(`Trigger.AvailableNow`), waits for it to finish, and reads the memory table.

The per-bucket "latest" uses Spark's `max_by(value, struct(event_time,
dedupe_id))`, matching the oracle's tiebreak; only the arrival-ordered arm
tiebreaks on `processing_order`. Deduplication (variant
"dedup") uses `dropDuplicates(["dedupe_id"])`, a stateful streaming operator.
Requires `pyspark` in the venv (Spark runs in local mode; no external cluster
needed).
"""
from __future__ import annotations

import tempfile
import time
from pathlib import Path

import pandas as pd

from .base import EngineAdapter, IngestStats, canonical_event_time, coerce_view, empty_view

_TRUNC = {"hourly": "hour", "daily": "day"}


class SparkAdapter(EngineAdapter):
    name = "spark"
    # No "restate" (D7, supersedes model): the view must join the base rows against
    # an *aggregated* restatement relation (grouped by supersedes; valid fixtures
    # give one row per group). Three documented Structured Streaming rules compose
    # to block that as a MAINTAINED view: (1) joins run only in append output mode;
    # (2) an aggregation emits under append only if a watermark can retire its
    # state, which requires the grouping to carry the event-time column or a window
    # over it; (3) `supersedes` is an identity column, neither of those. Windowing
    # the grouping satisfies (2) but breaks the semantics, because a restatement
    # need not land in the same window as the row it corrects.
    # A stream-static join (restatement side read as a batch DataFrame) does run,
    # but it resolves restatements outside the streaming query -- the same reason
    # supported_shapes below declines the ranking and threshold shapes.
    # not the earlier reason: "SS cannot use a streaming aggregation as a join
    # input" is false, and the SS guide ships a counterexample joining two windowed
    # aggregations. Keep the conclusion, not that argument.
    # Measured, not assumed: scripts/run/verify_spark_restate.py ran all four
    # formulations on pinned Spark 4.2.0 and Spark rejected each streaming one,
    # each for a different one of the rules above:
    #   append   -> STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION: "not supported
    #               for streaming aggregations without watermark"
    #   complete -> "LeftOuter join between two streaming DataFrames/Datasets is
    #               not supported in Complete output mode, only in Append"
    #   windowed -> "Stream-stream LeftOuter join ... not supported without a
    #               watermark in the join keys, or a watermark on the nullable
    #               side and an appropriate range condition"
    # The join key is dedupe_id = supersedes, an identity, so no watermark can sit
    # in it. Only the stream-static formulation ran, and it matched the oracle,
    # which is why it is called out above rather than left for a reviewer to find.
    # Re-run the probe before trusting this negative on a new Spark pin.
    supported_variants = ("dedup", "naive", "arrival_latest")
    native_watermark_mode = "event_time_incremental"

    _shape: str | None = None
    _shape_order: str | None = None

    def __init__(self, **options) -> None:
        super().__init__(**options)
        self._spark = None
        self._stage_dir: Path | None = None
        self._schema = None

    def connect(self) -> "SparkAdapter":
        try:
            from pyspark.sql import SparkSession
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("spark adapter needs pyspark (pip install pyspark)") from exc
        builder = SparkSession.builder.appName("equitable-spark-adapter")
        builder = builder.master(self.options.get("master", "local[*]"))
        # read_view() collects the maintained view through Structured Streaming's
        # *memory* sink, so the whole result table lands in the driver heap, and
        # local[*] runs every task thread in that same JVM.  Spark's 1g default
        # is not enough for the larger domains: the driver hits
        # java.lang.OutOfMemoryError, Spark's handler shuts the SparkContext
        # down, and every later Spark cell in the matrix process then dies with
        # "Connection refused" / "SparkConf does not exist in the JVM".
        builder = builder.config("spark.driver.memory",
                                 str(self.options.get("driver_memory", "16g")))
        # Deterministic, single-partition shuffle keeps the fold order stable.
        builder = builder.config("spark.sql.shuffle.partitions",
                                 str(self.options.get("shuffle_partitions", 1)))
        # date_trunc() truncates in the session zone; the oracle buckets in UTC.
        # Without this, every hourly/daily bucket is offset by the JVM-local zone
        # and no key matches the oracle.
        builder = builder.config("spark.sql.session.timeZone", "UTC")
        # Avoid the noisy Arrow-version-mismatch fallback warnings (pandas<->Spark
        # conversion works fine without Arrow at these sizes).
        builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "false")
        self._spark = builder.getOrCreate()
        self._spark.sparkContext.setLogLevel("ERROR")
        return self

    #: Spark expresses the selection itself, through max_by and min_by over a
    #: tiebreak struct, so the two shapes whose answer is the selected row are
    #: available. The ranking and threshold shapes are not: structured
    #: streaming has no window function and no cross-group join inside a
    #: complete-mode aggregation, so building them would mean computing the
    #: shape in the harness rather than in the engine, which is the opposite of
    #: what the family is asking. Declining is a capability result.
    supported_shapes = ("S1_latest", "S2_first")

    def setup_shape(self, *, shape: str, order: str) -> None:
        if shape not in self.supported_shapes:
            raise NotImplementedError(
                f"{self.name} cannot express shape {shape!r} inside a "
                "structured-streaming aggregation")
        self._shape, self._shape_order = shape, order
        self.setup(variant="dedup")

    def read_shape(self, grain: str):
        from .base import canonical_shape_frame
        return canonical_shape_frame(self.read_view(grain), grain)

    def setup(self, *, variant: str = "dedup") -> None:
        self._check_variant(variant)
        self._stage_dir = Path(tempfile.mkdtemp(prefix="spark_stage_"))

    def ingest_log(self, df: pd.DataFrame) -> IngestStats:
        from pyspark.sql.types import (
            StructType, StructField, StringType, DoubleType, LongType, TimestampType,
        )
        assert self._spark is not None and self._stage_dir is not None
        t0 = time.perf_counter()
        self._schema = StructType([
            StructField("event_time_utc", TimestampType(), False),
            StructField("entity", StringType(), False),
            StructField("metric", StringType(), False),
            StructField("value", DoubleType(), False),
            StructField("processing_order", LongType(), False),
            StructField("dedupe_id", StringType(), False),
        ])
        sdf = self._spark.createDataFrame(df[[
            "event_time_utc", "entity", "metric", "value", "processing_order", "dedupe_id",
        ]])
        sdf.write.mode("overwrite").parquet(str(self._stage_dir))
        return IngestStats(rows_sent=len(df), seconds=time.perf_counter() - t0)

    def read_view(self, grain: str) -> pd.DataFrame:
        from pyspark.sql import functions as F
        self._check_grain(grain)
        assert self._spark is not None and self._stage_dir is not None
        stream = self._spark.readStream.schema(self._schema).parquet(str(self._stage_dir))
        if self.variant in ("dedup", "arrival_latest"):
            stream = stream.dropDuplicates(["dedupe_id"])
        # The contract tiebreak is (event_time, dedupe_id) and is replay-order
        # invariant. The idiomatic arm takes whatever arrived last instead --
        # Spark's last()/offset-style "latest" -- so a reorder changes it.
        # The order the distinguished row is selected by. A shape run overrides
        # the variant, because the family contrasts the two orders directly
        # rather than through the scored and idiomatic variants.
        shape_order = getattr(self, "_shape_order", None)
        arrival = (shape_order == "arrival" if shape_order
                   else self.variant == "arrival_latest")
        tiebreak = (F.struct(F.col("processing_order")) if arrival
                    else F.struct(F.col("event_time_utc"), F.col("dedupe_id")))
        # S2 takes the row at the other end of the same order. Spark exposes
        # that as a separate primitive, min_by beside max_by, so it exercises a
        # different code path even though the rule is identical.
        pick = (F.min_by if getattr(self, "_shape", None) == "S2_first"
                else F.max_by)
        agg = (
            stream
            .withColumn("bucket", F.date_trunc(_TRUNC[grain], F.col("event_time_utc")))
            .groupBy("bucket", "entity", "metric")
            .agg(
                F.count(F.lit(1)).alias("count"),
                F.sum("value").alias("sum"),
                F.min("value").alias("min"),
                F.max("value").alias("max"),
                F.avg("value").alias("mean"),
                pick("value", tiebreak).alias("latest_value"),
                pick("event_time_utc", tiebreak).alias("latest_event_time"),
            )
        )
        table = f"spark_view_{grain}"
        query = (
            agg.writeStream.format("memory").queryName(table)
            .outputMode("complete").trigger(availableNow=True).start()
        )
        query.awaitTermination()
        out = self._spark.sql(f"SELECT * FROM {table}").toPandas()
        try:
            self._spark.catalog.dropTempView(table)
        except Exception:
            pass
        if out.empty:
            return empty_view()
        out["latest_event_time"] = canonical_event_time(out["latest_event_time"])
        return coerce_view(out, grain)

    def run_native_watermark_probe(self, allowed_lateness_s: int) -> dict:
        """Run two genuine Structured Streaming event-time scenarios.

        Both streams use withWatermark and append-mode window aggregation. The
        treatment injects an event after its window is closed; the paired control
        injects an on-time event into an open window.
        """
        from datetime import datetime
        import shutil
        import uuid
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructField, StructType, StringType, TimestampType

        if allowed_lateness_s < 1:
            raise ValueError("allowed_lateness_s must be positive")
        assert self._spark is not None
        schema = StructType([
            StructField("event_time_utc", TimestampType(), False),
            StructField("metric", StringType(), False),
        ])

        def scenario(label: str, third_time: str) -> dict:
            root = Path(tempfile.mkdtemp(prefix=f"spark_watermark_{label}_"))
            source = root / "source"
            checkpoint = root / "checkpoint"
            source.mkdir()
            table = f"wm_{label}_{uuid.uuid4().hex[:10]}"
            stream = self._spark.readStream.schema(schema).parquet(str(source))
            aggregate = (
                stream.withWatermark(
                    "event_time_utc", f"{allowed_lateness_s} seconds")
                .groupBy(F.window("event_time_utc", "1 hour"), "metric")
                .count()
            )
            query = (aggregate.writeStream.format("memory").queryName(table)
                     .outputMode("append")
                     .option("checkpointLocation", str(checkpoint)).start())
            progress = []

            def add_batch(number: int, stamps: list[str]) -> None:
                stage = root / f"stage_{number}"
                rows = [(datetime.fromisoformat(ts), "m") for ts in stamps]
                self._spark.createDataFrame(rows, schema).coalesce(1).write.parquet(str(stage))
                for part in stage.glob("part-*.parquet"):
                    shutil.move(str(part), source / f"batch_{number}_{part.name}")
                shutil.rmtree(stage)
                query.processAllAvailable()
                if query.lastProgress is not None:
                    progress.append(query.lastProgress)

            try:
                add_batch(1, ["2024-01-01 00:10:00", "2024-01-01 00:20:00"])
                add_batch(2, ["2024-01-01 02:00:00"])
                add_batch(3, [third_time])
                add_batch(4, ["2024-01-01 04:00:00"])
                rows = self._spark.sql(
                    f"SELECT window.start AS start, count FROM {table} "
                    "WHERE metric = \x27m\x27 ORDER BY start").collect()
                counts = {r.start.strftime("%Y-%m-%dT%H:%M:%SZ"): int(r["count"])
                          for r in rows}
                dropped = 0
                for item in progress:
                    for op in item.get("stateOperators", []):
                        dropped += int(op.get("numRowsDroppedByWatermark", 0))
                return {"counts": counts, "rows_dropped_by_watermark": dropped,
                        "micro_batches": len(progress)}
            finally:
                query.stop()
                try:
                    self._spark.catalog.dropTempView(table)
                except Exception:
                    pass
                shutil.rmtree(root, ignore_errors=True)

        treatment = scenario("late", "2024-01-01 00:30:00")
        control = scenario("control", "2024-01-01 02:10:00")
        treatment_ok = (
            treatment["counts"].get("2024-01-01T00:00:00Z") == 2
            and treatment["counts"].get("2024-01-01T02:00:00Z") == 1
            and treatment["rows_dropped_by_watermark"] >= 1
        )
        control_ok = (
            control["counts"].get("2024-01-01T00:00:00Z") == 2
            and control["counts"].get("2024-01-01T02:00:00Z") == 2
            and control["rows_dropped_by_watermark"] == 0
        )
        return {
            "engine_api": "Structured Streaming withWatermark",
            "allowed_lateness_s": allowed_lateness_s,
            "treatment": treatment, "paired_on_time_control": control,
            "late_event_dropped": treatment_ok,
            "on_time_event_included": control_ok,
            "pass": bool(treatment_ok and control_ok),
        }

    def close(self) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
        if self._stage_dir is not None:
            import shutil
            shutil.rmtree(self._stage_dir, ignore_errors=True)
            self._stage_dir = None
