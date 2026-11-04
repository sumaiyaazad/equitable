"""Spark Structured Streaming on a standalone cluster (the scored Spark deployment).

The in-process adapter (src/adapters/spark.py) ran each cell in a local-mode JVM
inside the benchmark process and drained the view once with availableNow, so it
had no process to kill and no state to restore, and D5 was not applicable. This
adapter drives the same queries on the cluster in docker/streaming-engines.yml:

    host adapter --HTTP--> spark-driver (client mode) --> executors on spark-worker
         |                        |
         +---- shared volume: input parquet, checkpoints, output parquet ----+

    setup()       driver stops the running queries, wipes the shared tree, and
                  starts the hourly and daily queries for the variant or shape
    ingest_log()  drop one parquet file into input/ and block until both queries
                  have COMMITTED the batch that consumed it
    read_view()   read the complete-mode result the query overwrote
    checkpoint()  re-assert that the committed batches cover every ingested file
    resume()      after the driver is killed and restarted, wait until it has
                  restarted the cell's queries from their checkpoints

The barrier reads Spark's own bookkeeping: the file source log records which
batch consumed each file, and commits/<batch> appears once that batch is durable.

D5 VALIDITY. The output parquet survives a driver crash whether or not Spark
restored its state, so reading it back proves nothing on its own. D5 runs with
--resume: rows ingested after recovery must combine with the restored
dropDuplicates and aggregation state.
"""
from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from .base import EngineAdapter, IngestStats, canonical_event_time, coerce_view, empty_view

GRAINS = ("hourly", "daily")
_COLUMNS = ["event_time_utc", "entity", "metric", "value", "processing_order", "dedupe_id"]


class SparkClusterAdapter(EngineAdapter):
    name = "spark"
    # Same capability set as the in-process adapter, for the reasons recorded
    # there: restatement is not expressible as a maintained Structured Streaming
    # view, and only the two shapes whose answer is the selected row are.
    supported_variants = ("dedup", "naive", "arrival_latest")
    supported_shapes = ("S1_latest", "S2_first")
    native_watermark_mode = "event_time_incremental"

    def __init__(self, **options: Any) -> None:
        super().__init__(**options)
        self._url = str(options.get("driver_url", "http://localhost:8090")).rstrip("/")
        self._root = Path(options.get("data_root", "/tmp/equitable_spark_cluster"))
        self._ready_timeout = float(options.get("ready_timeout_s", 600))
        self._fold_timeout = float(options.get("fold_timeout_s", 3600))
        self._committed: Dict[str, int] = {}

    # --- driver service -----------------------------------------------------
    def _call(self, method: str, path: str, body: Optional[dict] = None,
              timeout: float = 600) -> Dict[str, Any]:
        import requests
        r = requests.request(method, f"{self._url}{path}", json=body, timeout=timeout)
        payload = r.json()
        if r.status_code >= 400:
            raise RuntimeError(f"spark driver {path} failed: {payload.get('error')}\n"
                               f"{payload.get('trace', '')[-2000:]}")
        return payload

    def _await_queries(self) -> Dict[str, Any]:
        deadline = time.time() + self._ready_timeout
        last: Any = "unreachable"
        while time.time() < deadline:
            try:
                status = self._call("GET", "/status", timeout=10)
            except Exception as exc:
                last = exc
                time.sleep(2)
                continue
            last = status
            if status.get("error"):
                raise RuntimeError(f"spark driver reports: {status['error']}")
            queries = status.get("queries") or {}
            for grain, query in queries.items():
                if query.get("exception"):
                    raise RuntimeError(
                        f"spark {grain} query failed: {query['exception'][-2000:]}")
            if status.get("ready") and all(
                    (queries.get(g) or {}).get("active") for g in GRAINS):
                return status
            time.sleep(1)
        raise RuntimeError(f"spark driver queries not active within "
                           f"{self._ready_timeout:g}s (last status {last})")

    # --- lifecycle ----------------------------------------------------------
    def connect(self) -> "SparkClusterAdapter":
        deadline = time.time() + self._ready_timeout
        while time.time() < deadline:
            try:
                if self._call("GET", "/status", timeout=10).get("ready"):
                    return self
            except Exception:
                pass
            time.sleep(2)
        raise RuntimeError(f"spark driver at {self._url} not ready within "
                           f"{self._ready_timeout:g}s")

    def setup(self, *, variant: str = "dedup") -> None:
        self._check_variant(variant)
        self._start({"variant": variant, "shape": None, "shape_order": None})

    def setup_shape(self, *, shape: str, order: str) -> None:
        if shape not in self.supported_shapes:
            raise NotImplementedError(
                f"{self.name} cannot express shape {shape!r} inside a "
                "structured-streaming aggregation")
        self._check_variant("dedup")
        self._start({"variant": "dedup", "shape": shape, "shape_order": order})

    def read_shape(self, grain: str) -> pd.DataFrame:
        from .base import canonical_shape_frame
        return canonical_shape_frame(self.read_view(grain), grain)

    # --- D8: hopping window ------------------------------------------------
    #: Spark's F.window takes a slide duration directly, so the hopping window
    #: is native. The query lives in the driver image, not here.
    hopping_mode = "native_hop"

    def setup_hopping(self, *, size_n: int = 2, hop_n: int = 1) -> None:
        self._check_variant("dedup")
        self._start({"variant": "dedup", "shape": None, "shape_order": None,
                     "hopping": True})

    def read_hopping(self, grain: str) -> pd.DataFrame:
        """The hopping table is the bucket table, so read_view's path applies."""
        return self.read_view(grain)

    def _start(self, cell: dict) -> None:
        self._call("POST", "/setup", cell)
        self._committed = {}
        self._await_queries()

    def ingest_log(self, df: pd.DataFrame) -> IngestStats:
        t0 = time.perf_counter()
        # A unique name per file: the file source skips any name it has seen, so
        # a counter that restarts with a reconnected adapter would make the rows
        # ingested after a recovery invisible.
        name = f"part_{time.time_ns()}_{uuid.uuid4().hex[:8]}.parquet"
        tmp = self._root / "input" / f".{name}"
        final = self._root / "input" / name
        # Microsecond timestamps: Spark's TimestampType reader rejects the
        # nanosecond INT64 pandas writes by default.
        df[_COLUMNS].to_parquet(tmp, index=False,
                                coerce_timestamps="us", allow_truncated_timestamps=True)
        tmp.rename(final)
        self._await_fold(name)
        return IngestStats(rows_sent=len(df), seconds=time.perf_counter() - t0)

    def _await_fold(self, filename: str) -> None:
        deadline = time.time() + self._fold_timeout
        pending: Dict[str, Optional[int]] = {g: None for g in GRAINS}
        while time.time() < deadline:
            for grain in GRAINS:
                if pending[grain] is None:
                    pending[grain] = self._batch_for(grain, filename)
            if all(b is not None for b in pending.values()) and all(
                    (self._root / "ckpt" / g / "commits" / str(pending[g])).exists()
                    for g in GRAINS):
                self._committed = {g: int(b) for g, b in pending.items()}
                self._assert_no_reprocessing()
                return
            status = self._call("GET", "/status", timeout=30)
            for grain, query in (status.get("queries") or {}).items():
                if query.get("exception"):
                    raise RuntimeError(
                        f"spark {grain} query failed: {query['exception'][-2000:]}")
            time.sleep(0.5)
        raise RuntimeError(f"{filename} was not committed by both views within "
                           f"{self._fold_timeout:g}s (batches seen: {pending})")

    def _file_batches(self, grain: str) -> Dict[str, set]:
        """Every input file in the source log, with each batch that consumed it."""
        seen: Dict[str, set] = {}
        src = self._root / "ckpt" / grain / "sources" / "0"
        if not src.is_dir():
            return seen
        for entry in src.iterdir():
            try:
                text = entry.read_text(errors="ignore")
            except (OSError, IsADirectoryError):
                continue
            for line in text.splitlines():
                line = line.strip()
                if not line.startswith("{"):
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                name = Path(record.get("path", "")).name
                seen.setdefault(name, set()).add(int(record["batchId"]))
        return seen

    def _assert_no_reprocessing(self) -> None:
        """Fail if any input file was consumed by more than one batch.

        A driver that lost its checkpoint re-reads every file and still reaches
        the right totals, so a correct final table alone cannot show that state
        was restored. Spark's own source log can: a restored query never assigns
        an already-consumed file to a new batch.
        """
        for grain in GRAINS:
            again = {name: sorted(batches)
                     for name, batches in self._file_batches(grain).items()
                     if len(batches) > 1}
            if again:
                raise RuntimeError(
                    f"spark {grain} query re-read input after a restart instead of "
                    f"restoring state: {again}")

    def _batch_for(self, grain: str, filename: str) -> Optional[int]:
        """The batchId that consumed `filename`, from the file source log."""
        src = self._root / "ckpt" / grain / "sources" / "0"
        if not src.is_dir():
            return None
        for entry in sorted(src.iterdir()):
            try:
                text = entry.read_text(errors="ignore")
            except (OSError, IsADirectoryError):
                continue
            for line in text.splitlines():
                line = line.strip()
                if not line.startswith("{") or filename not in line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if Path(record.get("path", "")).name == filename:
                    return int(record["batchId"])
        return None

    def read_view(self, grain: str) -> pd.DataFrame:
        self._check_grain(grain)
        data = self._root / "output" / grain / "data"
        if not data.exists():
            return empty_view()
        out = pd.read_parquet(data)
        if out.empty:
            return empty_view()
        out["latest_event_time"] = canonical_event_time(out["latest_event_time"])
        return coerce_view(out, grain)

    def run_native_watermark_probe(self, allowed_lateness_s: int) -> Dict[str, Any]:
        return self._call("POST", "/watermark",
                          {"allowed_lateness_s": int(allowed_lateness_s)}, timeout=1800)

    # --- D5 hooks -----------------------------------------------------------
    def checkpoint(self) -> None:
        if set(self._committed) != set(GRAINS):
            raise RuntimeError("spark: no ingested batch has been committed to barrier on")
        for grain, batch in self._committed.items():
            if not (self._root / "ckpt" / grain / "commits" / str(batch)).exists():
                raise RuntimeError(f"spark: {grain} batch {batch} is not in commits/")

    def resume(self) -> None:
        """Wait for the restarted driver to resume the cell's queries."""
        status = self._await_queries()
        if not status.get("cell"):
            raise RuntimeError("spark driver restarted without a cell to resume")
        self._assert_no_reprocessing()

    def detach(self) -> None:
        """No client connection; the driver owns the queries D5 measures."""

    def close(self) -> None:
        """The compose stack owns the driver; setup() of the next cell resets it."""
