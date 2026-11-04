#!/usr/bin/env python3
"""
verify_spark_restate.py -- D7 capability check (docs/SEMANTICS.md, Restatements): can Spark
Structured Streaming express the restate view AS A MAINTAINED STREAMING VIEW?

The restate view (src/adapters/_sql_views.py) is a base relation LEFT JOINed
against an aggregate grouped by `supersedes`:

    latest_restatement = SELECT supersedes, argmax(value BY processing_order)
                         FROM dedup WHERE supersedes IS NOT NULL
                         GROUP BY supersedes
    resolved           = SELECT COALESCE(lr.new_value, b.value) ...
                         FROM dedup b LEFT JOIN latest_restatement lr
                              ON b.dedupe_id = lr.supersedes
                         WHERE b.supersedes IS NULL

So the question is whether a streaming aggregation grouped by an IDENTITY column
can feed a join. This script answers it by running four formulations and
recording what Spark actually does with each, rather than inferring the answer
from the Structured Streaming restriction list.

Why this exists: the negative was previously asserted in a code comment on the
reasoning "Structured Streaming does not support using a streaming aggregation
as a join input". The Structured Streaming guide contradicts that reasoning
directly -- it ships a worked example joining two windowed aggregations
(clicksWindow.join(impressionsWindow, "window", "inner")) -- so the reason was
wrong even though the conclusion may hold. A published capability limit needs a
measurement, the same way D5's Feldera N/A rests on verify_persistence.py
rather than on the image contents.

Formulations, in order of how faithful they are to a maintained view:

  F1 streaming_append   base stream LEFT JOIN (streaming agg GROUP BY supersedes),
                        append mode. The faithful formulation.
  F2 streaming_complete same join under complete mode, which is the output mode
                        the scored Spark view already uses.
  F3 windowed_agg       F1 with a watermark and a time window added to the
                        aggregate's grouping key, the only documented way to make
                        an aggregation emit under append. Watch the ANSWER here,
                        not just whether it runs: a restatement need not share a
                        window with the row it corrects.
  F4 stream_static      restatement side computed as a BATCH DataFrame and joined
                        stream-to-static. Expected to run and to be correct, but
                        it resolves restatements outside the maintained view, so
                        it answers a different question. Recorded so the negative
                        cannot be dismissed with "you could have used a static
                        join".

Each formulation is compared against a pandas oracle that mirrors
src/adapters/reference.py: for every base row, its value is replaced by the
value of the superseding row with the greatest processing_order.

PASS (meaning: the negative is confirmed) iff no formulation both runs AND
matches the oracle AND keeps the resolution inside the streaming query.

Wired into reproduce_all.sh, so a fresh run re-derives this negative rather than
inheriting it. Exits non-zero if any streaming formulation starts working, which
would mean the paper's D7 cell for Spark is wrong.

WARNING: starts a local Spark JVM. It is small (2g, local[1], 6 rows), but do not
run it against a machine already saturated by a campaign sweep -- check
`pgrep -af run_diagnostic` and the load average first.

Inputs  : none (synthetic fixture, 6 rows)
Outputs : runs/<UTC>_spark_restate/summary.json ; PASS/FAIL per formulation
Run as  : python scripts/run/verify_spark_restate.py
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

BASE_TIME = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def _fixture() -> pd.DataFrame:
    """Base rows plus restatements, including the case F3 is built to expose.

    b3 sits at 12:59 and its restatement r3 arrives at 13:05, so the two fall in
    DIFFERENT one-hour windows. Any formulation that puts a time window in the
    aggregate's grouping key splits that pair and silently fails to resolve it.
    """
    rows = [
        # (offset_min, entity, metric, value, order, dedupe_id, supersedes)
        (0,  "e1", "m", 10.0, 1, "b1", None),
        (1,  "e1", "m", 20.0, 2, "b2", None),
        (59, "e1", "m", 30.0, 3, "b3", None),
        # r1 corrects b1 twice; the greater processing_order wins.
        (2,  "e1", "m", 11.0, 4, "r1a", "b1"),
        (3,  "e1", "m", 12.0, 5, "r1b", "b1"),
        # r3 corrects b3 from the NEXT hour.
        (65, "e1", "m", 33.0, 6, "r3", "b3"),
    ]
    return pd.DataFrame([
        {
            "event_time_utc": BASE_TIME + timedelta(minutes=off),
            "entity": ent,
            "metric": met,
            "value": val,
            "processing_order": order,
            "dedupe_id": did,
            "supersedes": sup,
        }
        for off, ent, met, val, order, did, sup in rows
    ])


def _oracle(df: pd.DataFrame) -> pd.DataFrame:
    """Mirror of the restate resolution in src/adapters/reference.py."""
    sup = df["supersedes"]
    is_rest = sup.notna() & (sup.astype(object) != "")
    latest = (df[is_rest].sort_values("processing_order")
              .groupby("supersedes")["value"].last())
    base = df[~is_rest].copy()
    base["value"] = base["dedupe_id"].map(latest).where(
        base["dedupe_id"].isin(latest.index), base["value"])
    return (base[["dedupe_id", "value"]]
            .sort_values("dedupe_id").reset_index(drop=True))


def _spark_session(driver_memory: str, master: str):
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder
             .appName("equitable-spark-restate-probe")
             .master(master)
             .config("spark.driver.memory", driver_memory)
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.execution.arrow.pyspark.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _schema():
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, LongType, TimestampType,
    )
    return StructType([
        StructField("event_time_utc", TimestampType(), False),
        StructField("entity", StringType(), False),
        StructField("metric", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("processing_order", LongType(), False),
        StructField("dedupe_id", StringType(), False),
        StructField("supersedes", StringType(), True),
    ])


def _collect(spark, plan, table: str, mode: str, ckpt: Path) -> pd.DataFrame:
    """Drain a bounded stream into the memory sink and return it as pandas."""
    query = (plan.writeStream.format("memory").queryName(table)
             .outputMode(mode)
             .option("checkpointLocation", str(ckpt))
             .trigger(availableNow=True).start())
    try:
        query.awaitTermination()
    finally:
        if query.isActive:
            query.stop()
    out = spark.sql(f"SELECT * FROM {table}").toPandas()
    try:
        spark.catalog.dropTempView(table)
    except Exception:
        pass
    return out


def _resolved(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise a formulation's output to the oracle's (dedupe_id, value) shape."""
    if df.empty:
        return pd.DataFrame(columns=["dedupe_id", "value"])
    return (df[["dedupe_id", "value"]]
            .sort_values("dedupe_id").reset_index(drop=True))


# --- formulations --------------------------------------------------------

def f1_streaming_append(spark, stage: Path, ckpt: Path):
    """The faithful formulation: aggregate grouped by the identity column."""
    from pyspark.sql import functions as F
    base = (spark.readStream.schema(_schema()).parquet(str(stage))
            .where(F.col("supersedes").isNull()).alias("b"))
    rest = (spark.readStream.schema(_schema()).parquet(str(stage))
            .where(F.col("supersedes").isNotNull()).alias("r"))
    latest = rest.groupBy("supersedes").agg(
        F.max_by("value", F.col("processing_order")).alias("new_value")).alias("lr")
    joined = base.join(latest, F.col("b.dedupe_id") == F.col("lr.supersedes"), "left")
    plan = joined.select(
        F.col("b.dedupe_id").alias("dedupe_id"),
        F.coalesce(F.col("lr.new_value"), F.col("b.value")).alias("value"))
    return _collect(spark, plan, "f1_out", "append", ckpt)


def f2_streaming_complete(spark, stage: Path, ckpt: Path):
    """The same join under the output mode the scored Spark view already uses."""
    from pyspark.sql import functions as F
    base = (spark.readStream.schema(_schema()).parquet(str(stage))
            .where(F.col("supersedes").isNull()).alias("b"))
    rest = (spark.readStream.schema(_schema()).parquet(str(stage))
            .where(F.col("supersedes").isNotNull()).alias("r"))
    latest = rest.groupBy("supersedes").agg(
        F.max_by("value", F.col("processing_order")).alias("new_value")).alias("lr")
    joined = base.join(latest, F.col("b.dedupe_id") == F.col("lr.supersedes"), "left")
    plan = joined.select(
        F.col("b.dedupe_id").alias("dedupe_id"),
        F.coalesce(F.col("lr.new_value"), F.col("b.value")).alias("value"))
    return _collect(spark, plan, "f2_out", "complete", ckpt)


def f3_windowed_agg(spark, stage: Path, ckpt: Path):
    """Satisfy append by windowing the aggregate. Check the ANSWER, not the run.

    Adding window(event_time) to the grouping key is the documented way to let a
    streaming aggregation emit under append mode, but it changes what is being
    grouped: the restatement joins only if it lands in the same window as the row
    it corrects. The fixture's b3/r3 pair straddles an hour boundary precisely so
    a silently wrong answer shows up as a mismatch rather than as an exception.
    """
    from pyspark.sql import functions as F
    base = (spark.readStream.schema(_schema()).parquet(str(stage))
            .where(F.col("supersedes").isNull())
            .withWatermark("event_time_utc", "1 hour").alias("b"))
    rest = (spark.readStream.schema(_schema()).parquet(str(stage))
            .where(F.col("supersedes").isNotNull())
            .withWatermark("event_time_utc", "1 hour").alias("r"))
    latest = (rest.groupBy(F.window(F.col("event_time_utc"), "1 hour"), "supersedes")
              .agg(F.max_by("value", F.col("processing_order")).alias("new_value"))
              .alias("lr"))
    joined = base.join(latest, F.col("b.dedupe_id") == F.col("lr.supersedes"), "left")
    plan = joined.select(
        F.col("b.dedupe_id").alias("dedupe_id"),
        F.coalesce(F.col("lr.new_value"), F.col("b.value")).alias("value"))
    return _collect(spark, plan, "f3_out", "append", ckpt)


def f4_stream_static(spark, stage: Path, ckpt: Path):
    """Restatement side as a BATCH read: supported, but outside the maintained view."""
    from pyspark.sql import functions as F
    base = (spark.readStream.schema(_schema()).parquet(str(stage))
            .where(F.col("supersedes").isNull()).alias("b"))
    latest = (spark.read.schema(_schema()).parquet(str(stage))
              .where(F.col("supersedes").isNotNull())
              .groupBy("supersedes")
              .agg(F.max_by("value", F.col("processing_order")).alias("new_value"))
              .alias("lr"))
    joined = base.join(latest, F.col("b.dedupe_id") == F.col("lr.supersedes"), "left")
    plan = joined.select(
        F.col("b.dedupe_id").alias("dedupe_id"),
        F.coalesce(F.col("lr.new_value"), F.col("b.value")).alias("value"))
    return _collect(spark, plan, "f4_out", "append", ckpt)


FORMULATIONS = [
    ("F1_streaming_append", f1_streaming_append, True),
    ("F2_streaming_complete", f2_streaming_complete, True),
    ("F3_windowed_agg", f3_windowed_agg, True),
    ("F4_stream_static", f4_stream_static, False),
]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--driver-memory", default="2g",
                    help="Spark driver heap. The fixture is 6 rows; keep this "
                         "small so the probe cannot disturb a running campaign.")
    ap.add_argument("--master", default="local[1]")
    ap.add_argument("--out-root", default=str(ROOT / "runs"))
    args = ap.parse_args()

    df = _fixture()
    oracle = _oracle(df)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.out_root) / f"{stamp}_spark_restate"
    out_dir.mkdir(parents=True, exist_ok=True)

    work = Path(tempfile.mkdtemp(prefix="spark_restate_probe_"))
    stage = work / "stage"
    results = []
    spark = None
    try:
        spark = _spark_session(args.driver_memory, args.master)
        spark.createDataFrame(df).write.mode("overwrite").parquet(str(stage))

        for label, fn, in_stream in FORMULATIONS:
            ckpt = work / f"ckpt_{label}"
            record = {"formulation": label, "resolution_inside_stream": in_stream}
            try:
                got = _resolved(fn(spark, stage, ckpt))
                matches = got.equals(oracle)
                record.update(ran=True, matches_oracle=bool(matches),
                              rows=got.to_dict("records"))
            except Exception as exc:  # noqa: BLE001 - the exception is the result
                msg = str(exc).strip()
                # Guard against confirming the negative for the wrong reason. An
                # ambiguous-column error means both sides came off one plan, which
                # is a defect in THIS script, not a Structured Streaming limit.
                probe_bug = "ambiguous" in msg.lower()
                record.update(ran=False, matches_oracle=False,
                              probe_bug=probe_bug,
                              error_type=type(exc).__name__,
                              error=msg.splitlines()[0][:500],
                              traceback_tail=traceback.format_exc()[-1200:])
            results.append(record)
            status = ("ran, matches oracle" if record.get("matches_oracle")
                      else "ran, DIVERGES from oracle" if record.get("ran")
                      else f"rejected: {record.get('error_type')}")
            print(f"{label:<24} {status}")
            if not record.get("ran"):
                print(f"    {record.get('error')}")
    finally:
        if spark is not None:
            spark.stop()
        shutil.rmtree(work, ignore_errors=True)

    # The negative holds iff nothing expressed the resolution inside the stream
    # AND got the right answer.
    valid = [r for r in results
             if r.get("ran") and r.get("matches_oracle") and r["resolution_inside_stream"]]
    bugged = [r for r in results if r.get("probe_bug")]
    # A negative is only confirmed when every streaming formulation was actually
    # exercised. If any died of a probe defect, the run proves nothing.
    confirmed = not valid and not bugged
    summary = {
        "probe": "spark_restate_d7",
        "utc": stamp,
        "oracle": oracle.to_dict("records"),
        "formulations": results,
        "negative_confirmed": confirmed,
        "probe_bugs": [r["formulation"] for r in bugged],
        "verdict": ("INCONCLUSIVE: probe defect in "
                    f"{', '.join(r['formulation'] for r in bugged)}; fix and re-run"
                    if bugged else
                    "CONFIRMED: no streaming formulation expresses the restate view"
                    if confirmed else
                    f"REFUTED: {valid[0]['formulation']} expresses it correctly"),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    print(f"\n{summary['verdict']}")
    print(f"summary: {out_dir / 'summary.json'}")
    return 0 if confirmed else 1


if __name__ == "__main__":
    raise SystemExit(main())
