#!/usr/bin/env python3
"""Long-lived Spark Structured Streaming driver for the Spark standalone cluster.

The scored Spark adapter (src/adapters/spark_cluster.py) controls this service
over HTTP. The driver runs in client mode against spark://spark-master:7077, so
executors on the worker container do the work, and every query checkpoints to the
volume the driver, the worker, and the host share.

    POST /setup      {"variant", "shape", "shape_order"}
                     stop the running queries, wipe input/output/checkpoints,
                     record the cell in current.json, start the hourly and daily
                     queries
    POST /watermark  {"allowed_lateness_s"} -> native D4W probe result
    GET  /status     readiness, the current cell, and each query's state

The queries are the in-process adapter's queries (src/adapters/spark.py), line for
line: dropDuplicates on dedupe_id, date_trunc buckets, count/sum/min/max/avg, and
max_by or min_by over the contract or arrival tiebreak. Only the deployment moves.

RECOVERY. On start, if current.json exists, the service restarts that cell's
queries against the same checkpoint directories. Structured Streaming restores the
dropDuplicates and aggregation state from the last committed batch. Killing this
container is therefore D5's crash, and its restart is the recovery under test.
The output parquet also survives the crash, so only new input after the restart
shows that the restored state is correct; D5 runs with --resume for that reason.
"""
from __future__ import annotations

import json
import os
import resource
import shutil
import socket
import tempfile
import threading
import time
import traceback
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType, TimestampType,
)

ROOT = Path(os.environ.get("SPARK_CLUSTER_ROOT", "/data"))
INPUT, OUTPUT, CKPT = ROOT / "input", ROOT / "output", ROOT / "ckpt"
CURRENT = ROOT / "current.json"
#: One checkpoint directory per cell. After a query stops, its executors can
#: still flush RocksDB state for a moment; at 500,000 rows and above that flush
#: can land after a wipe and fail the next cell with
#: STATE_STORE_CHECKPOINT_LOCATION_NOT_EMPTY. A fresh directory per cell cannot
#: collide with a stopped query's late writes. CKPT/<grain> stays a relative
#: symlink to the
#: current cell's directory, so the host adapter reads the same layout.
CKPT_CELLS = ROOT / "ckpt_cells"
TRUNC = {"hourly": "hour", "daily": "day"}
#: D8 hopping geometry per grain, as (window duration, slide duration). Spark's
#: F.window takes the slide directly and expands each row into every overlapping
#: window, but only when it is a GROUPING expression, so the hopping path groups
#: by the window and projects window.start rather than deriving a bucket column.
HOP = {"hourly": ("2 hours", "1 hour"), "daily": ("2 days", "1 day")}
VARIANTS = ("dedup", "naive", "arrival_latest")
SHAPES = ("S1_latest", "S2_first")

SCHEMA = StructType([
    StructField("event_time_utc", TimestampType(), False),
    StructField("entity", StringType(), False),
    StructField("metric", StringType(), False),
    StructField("value", DoubleType(), False),
    StructField("processing_order", LongType(), False),
    StructField("dedupe_id", StringType(), False),
])

LOCK = threading.RLock()
STATE: dict = {"ready": False, "queries": {}, "error": None}


def spark_session() -> SparkSession:
    return (SparkSession.builder
            .appName("equitable-spark-cluster")
            .master(os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"))
            .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "spark-driver"))
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.driver.port", "7078")
            .config("spark.blockManager.port", "7079")
            .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
            .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "6g"))
            .config("spark.executor.cores", os.environ.get("SPARK_EXECUTOR_CORES", "4"))
            .config("spark.sql.shuffle.partitions",
                    os.environ.get("SPARK_SHUFFLE_PARTITIONS", "2"))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            .config("spark.sql.streaming.stateStore.providerClass",
                    "org.apache.spark.sql.execution.streaming.state."
                    "RocksDBStateStoreProvider")
            .getOrCreate())


SPARK: SparkSession | None = None


def _open_tree() -> None:
    for d in (INPUT, OUTPUT, CKPT):
        d.mkdir(parents=True, exist_ok=True)
        try:
            # The host adapter writes input parquet as its own uid.
            d.chmod(0o777)
        except OSError:
            pass


def build(grain: str, cell: dict):
    """One maintained view; mirrors src/adapters/spark.py read_view()."""
    variant, shape, order = cell["variant"], cell.get("shape"), cell.get("shape_order")
    stream = SPARK.readStream.schema(SCHEMA).parquet(str(INPUT))
    if variant in ("dedup", "arrival_latest"):
        stream = stream.dropDuplicates(["dedupe_id"])
    arrival = (order == "arrival") if order else variant == "arrival_latest"
    tiebreak = (F.struct(F.col("processing_order")) if arrival
                else F.struct(F.col("event_time_utc"), F.col("dedupe_id")))
    pick = F.min_by if shape == "S2_first" else F.max_by
    aggregates = [
        F.count(F.lit(1)).alias("count"),
        F.sum("value").alias("sum"),
        F.min("value").alias("min"),
        F.max("value").alias("max"),
        F.avg("value").alias("mean"),
        pick("value", tiebreak).alias("latest_value"),
        pick("event_time_utc", tiebreak).alias("latest_event_time"),
    ]
    if cell.get("hopping"):
        # D8. The window is the grouping expression, which is what makes Spark
        # expand one row into each overlapping window; window.start is then the
        # bucket label, in the same vocabulary date_trunc produces.
        size, slide = HOP[grain]
        agg = (
            stream
            .groupBy(F.window(F.col("event_time_utc"), size, slide),
                     "entity", "metric")
            .agg(*aggregates)
            .select(F.col("window.start").alias("bucket"), "entity", "metric",
                    "count", "sum", "min", "max", "mean", "latest_value",
                    "latest_event_time")
        )
    else:
        agg = (
            stream
            .withColumn("bucket",
                        F.date_trunc(TRUNC[grain], F.col("event_time_utc")))
            .groupBy("bucket", "entity", "metric")
            .agg(*aggregates)
        )
    target = OUTPUT / grain

    def sink(batch_df, batch_id: int) -> None:
        # Complete mode hands over the whole result table, so overwrite is the
        # exact semantics. The marker follows the data it describes.
        batch_df.write.mode("overwrite").parquet(str(target / "data"))
        (target / "_batch").write_text(str(batch_id))

    location = (CKPT_CELLS / cell["cell_id"] / grain) if cell.get("cell_id") else (CKPT / grain)
    return (agg.writeStream
            .outputMode("complete")
            .queryName(f"equitable_{grain}")
            .option("checkpointLocation", str(location))
            .foreachBatch(sink)
            .trigger(processingTime="1 second")
            .start())


def stop_queries() -> None:
    for query in list(SPARK.streams.active):
        query.stop()
    STATE["queries"] = {}
    close_callback_connections()


def close_callback_connections() -> None:
    """Close the JVM-to-Python callback connections the stopped queries left.

    foreachBatch runs the sink in Python: each query's micro-batch thread in the
    JVM opens its own Py4J connection to this process's callback server, and a
    Python thread blocks reading it. Stopping the query ends the JVM thread but
    closes neither end, so without this every cell leaves two sockets and two
    threads behind until the driver hits its descriptor limit, can no longer
    open current.json, and drops every request.

    Only foreachBatch calls back into Python, and every query has just been
    stopped, so no live JVM thread owns any of these connections. The callback
    server itself keeps listening for the next cell's queries.

    Shut the sockets down rather than calling CallbackServer.close(): that closes
    each connection's buffered reader while its thread is blocked reading it,
    and deadlocks on the reader's lock. A shutdown hands the blocked thread an
    EOF instead, and Py4J's own loop then closes the connection and exits.
    """
    server = getattr(SparkContext._gateway, "_callback_server", None)
    if server is None:
        return
    with server.lock:
        connections = list(server.connections)
    for connection in connections:
        sock = getattr(connection, "socket", None)
        if sock is None:
            continue
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


def raise_descriptor_limit() -> None:
    """Lift the soft RLIMIT_NOFILE to the hard limit, for this process and the JVM.

    The JVM raises its own limit; Python does not, so without this the control
    service runs at Docker's default soft limit of 1,024.
    close_callback_connections prevents the known leak; this is headroom, so a
    leak nobody has found yet shows up as a climbing open_fds in /status rather
    than as a wedged driver.
    """
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft != hard:
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
        except (ValueError, OSError):
            pass


def open_fds() -> int | None:
    try:
        return len(os.listdir("/proc/self/fd"))
    except OSError:
        return None


def start_cell(cell: dict) -> None:
    STATE["queries"] = {grain: build(grain, cell) for grain in TRUNC}


def setup(cell: dict) -> None:
    if cell.get("variant") not in VARIANTS:
        raise ValueError(f"unsupported variant {cell.get('variant')!r}")
    if cell.get("shape") not in (None, *SHAPES):
        raise ValueError(f"unsupported shape {cell.get('shape')!r}")
    with LOCK:
        STATE["ready"] = False
        previous = None
        if CURRENT.exists():
            try:
                previous = json.loads(CURRENT.read_text()).get("cell_id")
            except (OSError, ValueError):
                previous = None
        stop_queries()
        # Stop before wiping: a live query recreates its checkpoint within
        # milliseconds, which is the race the single-container driver hit.
        # CKPT now holds only symlinks, so this removes links, not state.
        for d in (INPUT, OUTPUT, CKPT):
            shutil.rmtree(d, ignore_errors=True)
        CURRENT.unlink(missing_ok=True)
        _open_tree()
        cell = dict(cell, cell_id=uuid.uuid4().hex)
        CKPT_CELLS.mkdir(parents=True, exist_ok=True)
        for grain in TRUNC:
            (CKPT_CELLS / cell["cell_id"] / grain).mkdir(parents=True, exist_ok=True)
            (CKPT / grain).symlink_to(Path("..") / "ckpt_cells" / cell["cell_id"] / grain)
        # Older cells' directories are removed, except the one just stopped,
        # whose executors may still be flushing; it goes at the next setup.
        for old in CKPT_CELLS.iterdir():
            if old.name not in (cell["cell_id"], previous):
                shutil.rmtree(old, ignore_errors=True)
        tmp = CURRENT.with_suffix(".tmp")
        tmp.write_text(json.dumps(cell))
        tmp.replace(CURRENT)
        start_cell(cell)
        STATE["ready"] = True


def watermark_probe(allowed_lateness_s: int) -> dict:
    """The in-process adapter's D4W probe, run on the cluster."""
    if allowed_lateness_s < 1:
        raise ValueError("allowed_lateness_s must be positive")
    schema = StructType([
        StructField("event_time_utc", TimestampType(), False),
        StructField("metric", StringType(), False),
    ])

    def scenario(label: str, third_time: str) -> dict:
        root = Path(tempfile.mkdtemp(prefix=f"wm_{label}_", dir=str(ROOT)))
        source, checkpoint = root / "source", root / "checkpoint"
        source.mkdir()
        table = f"wm_{label}_{uuid.uuid4().hex[:10]}"
        stream = SPARK.readStream.schema(schema).parquet(str(source))
        aggregate = (stream.withWatermark("event_time_utc", f"{allowed_lateness_s} seconds")
                     .groupBy(F.window("event_time_utc", "1 hour"), "metric").count())
        query = (aggregate.writeStream.format("memory").queryName(table)
                 .outputMode("append").option("checkpointLocation", str(checkpoint)).start())
        progress = []

        def add_batch(number: int, stamps: list) -> None:
            stage = root / f"stage_{number}"
            rows = [(datetime.fromisoformat(ts), "m") for ts in stamps]
            SPARK.createDataFrame(rows, schema).coalesce(1).write.parquet(str(stage))
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
            rows = SPARK.sql(
                f"SELECT window.start AS start, count FROM {table} "
                "WHERE metric = 'm' ORDER BY start").collect()
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
                SPARK.catalog.dropTempView(table)
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


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, body: dict) -> None:
        data = json.dumps(body, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *_args) -> None:  # quiet
        return

    def do_GET(self) -> None:
        if self.path != "/status":
            return self._send(404, {"error": "not found"})
        queries = {}
        for grain, query in (STATE.get("queries") or {}).items():
            exc = query.exception()
            queries[grain] = {"active": bool(query.isActive),
                              "exception": None if exc is None else str(exc)}
        cell = json.loads(CURRENT.read_text()) if CURRENT.exists() else None
        self._send(200, {"ready": STATE["ready"], "cell": cell,
                         "queries": queries, "error": STATE["error"],
                         "started_utc": STATE.get("started_utc"),
                         "open_fds": open_fds()})

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length) or b"{}")
        try:
            if self.path == "/setup":
                setup(body)
                return self._send(200, {"ok": True})
            if self.path == "/watermark":
                with LOCK:
                    return self._send(200, watermark_probe(int(body["allowed_lateness_s"])))
            return self._send(404, {"error": "not found"})
        except Exception as exc:
            return self._send(500, {"error": str(exc), "trace": traceback.format_exc()[-4000:]})


def main() -> int:
    global SPARK
    STATE["started_utc"] = time.time()
    raise_descriptor_limit()
    SPARK = spark_session()
    SPARK.sparkContext.setLogLevel("ERROR")
    _open_tree()
    if CURRENT.exists():
        # Recovery: same cell, same checkpoints.
        try:
            start_cell(json.loads(CURRENT.read_text()))
        except Exception as exc:
            STATE["error"] = f"recovery failed: {exc}"
    STATE["ready"] = STATE["error"] is None
    server = ThreadingHTTPServer(("0.0.0.0", 8090), Handler)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
