"""Feldera adapter (DBSP incremental view maintenance, REST API, port 8080).

Feldera is the benchmark's *positive control*: DBSP is a formally grounded
incremental computation model, so a clean pass on D1-D3 validates the diagnostics
themselves (a suite that fails Feldera would be measuring the harness, not the
engine). The full aggregate program is declared once as SQL views; Feldera
maintains them incrementally as rows are pushed over HTTP.

Flow against the Feldera REST API (paths overridable via options):
  1. PUT  /v0/pipelines/{name}        -- create pipeline from inline SQL program
  2. POST /v0/pipelines/{name}/start  -- deploy; poll until Running
  3. POST /v0/pipelines/{name}/ingress/{table}?format=json&force=false
     -- newline-delimited {"insert": {...}} records
  4. GET  /v0/pipelines/{name}/query?sql=...&format=json     -- ad-hoc read of a view
  5. POST /v0/pipelines/{name}/stop?force=true               -- teardown

API level: pinned against v0.324.0. Relative to the previous 0.26 pin, POST
/shutdown is gone (replaced by POST /stop), the terminal deployment status is
`Stopped` rather than `Shutdown`, and ingress requires an explicit `force`
query parameter.

Feldera speaks a Calcite SQL dialect, so the view SQL matches the Flink shape
(`FLOOR(ts TO HOUR|DAY)`, `ROW_NUMBER() OVER` for dedup and per-bucket latest).
Requires `requests`.
"""
from __future__ import annotations

import json
import os
import time
from typing import Any, List, Optional

import pandas as pd

from .base import EngineAdapter, IngestStats, canonical_event_time, coerce_view, empty_view

_TABLE = "stream_rows"
_VIEW = {"hourly": "v_hourly", "daily": "v_daily"}
_TOPK_VIEW = {"hourly": "v_topk_hourly", "daily": "v_topk_daily"}
_FLOOR = {"hourly": "HOUR", "daily": "DAY"}


def _view_sql(view: str, unit: str, variant: str) -> str:
    # arrival_latest reads the same deduplicated source as "dedup"; only the
    # per-bucket latest tiebreak differs (see _sql_views._latest_order).
    latest_order = ("processing_order DESC" if variant == "arrival_latest"
                    else "event_time_utc DESC, dedupe_id DESC")
    if variant in ("dedup", "arrival_latest"):
        src = (
            f"src AS (SELECT event_time_utc, entity, metric, value, processing_order, dedupe_id FROM ("
            f" SELECT *, ROW_NUMBER() OVER (PARTITION BY dedupe_id ORDER BY processing_order) AS rn"
            f" FROM {_TABLE}) WHERE rn = 1)"
        )
    elif variant == "restate":
        # D7 (docs/SEMANTICS.md, Restatements): content-dedup, resolve each restatement's
        # supersedes pointer (COALESCE with the latest superseding value), drop the
        # restatement rows. A restatement is any row with supersedes IS NOT NULL.
        src = (
            f"dedup AS (SELECT event_time_utc, entity, metric, value, processing_order, dedupe_id, supersedes"
            f" FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY dedupe_id ORDER BY processing_order) AS rn"
            f"       FROM {_TABLE}) WHERE rn = 1),"
            f" latest_restatement AS (SELECT supersedes, value AS new_value FROM ("
            f"   SELECT supersedes, value, ROW_NUMBER() OVER (PARTITION BY supersedes ORDER BY processing_order DESC) AS rr"
            f"   FROM dedup WHERE supersedes IS NOT NULL) WHERE rr = 1),"
            f" src AS (SELECT d.event_time_utc, d.entity, d.metric,"
            f"   COALESCE(lr.new_value, d.value) AS value, d.processing_order, d.dedupe_id"
            f"   FROM dedup d LEFT JOIN latest_restatement lr ON d.dedupe_id = lr.supersedes"
            f"   WHERE d.supersedes IS NULL)"
        )
    else:
        src = f"src AS (SELECT event_time_utc, entity, metric, value, processing_order, dedupe_id FROM {_TABLE})"
    return f"""
CREATE MATERIALIZED VIEW {view} AS
WITH {src},
b AS (
  SELECT TIMESTAMP_TRUNC(event_time_utc, {unit}) AS bucket,
         entity, metric, value, event_time_utc, processing_order, dedupe_id
  FROM src
),
agg AS (
  SELECT bucket, entity, metric,
         COUNT(*) AS "count", SUM(value) AS "sum",
         MIN(value) AS "min", MAX(value) AS "max", AVG(value) AS "mean"
  FROM b GROUP BY bucket, entity, metric
),
latest AS (
  SELECT bucket, entity, metric, value AS latest_value, event_time_utc AS latest_event_time
  FROM (
    SELECT b.*, ROW_NUMBER() OVER (
             PARTITION BY bucket, entity, metric
             ORDER BY {latest_order}) AS rk
    FROM b
  ) WHERE rk = 1
)
SELECT a.bucket, a.entity, a.metric, a."count", a."sum", a."min", a."max", a."mean",
       l.latest_value, l.latest_event_time
FROM agg a JOIN latest l
  ON a.bucket = l.bucket AND a.entity = l.entity AND a.metric = l.metric;
""".strip()


def _hopping_view_sql(view: str, unit: str, variant: str,
                      size_n: int = 2, hop_n: int = 1) -> str:
    """The D8 hopping window in Feldera SQL.

    Differs from `_view_sql` only in the `b` CTE, which assigns each row to the
    k windows containing it instead of one. Feldera is Calcite-based, so the
    explosion is a cross join against a VALUES list and the offset is an
    integer times an interval literal.
    """
    k = size_n // hop_n
    offsets = ", ".join(f"({i})" for i in range(k))
    if variant == "dedup":
        src = (f"src AS (SELECT event_time_utc, entity, metric, value, "
               f"processing_order, dedupe_id FROM (SELECT *, ROW_NUMBER() OVER "
               f"(PARTITION BY dedupe_id ORDER BY processing_order) AS rn FROM "
               f"{_TABLE}) WHERE rn = 1)")
    else:
        src = (f"src AS (SELECT event_time_utc, entity, metric, value, "
               f"processing_order, dedupe_id FROM {_TABLE})")
    return f"""
CREATE MATERIALIZED VIEW {view} AS
WITH {src},
b AS (
  SELECT TIMESTAMP_TRUNC(src.event_time_utc, {unit})
           - o.i * INTERVAL '{hop_n}' {unit} AS bucket,
         src.entity, src.metric, src.value, src.event_time_utc,
         src.processing_order, src.dedupe_id
  FROM src CROSS JOIN (VALUES {offsets}) AS o(i)
),
agg AS (
  SELECT bucket, entity, metric,
         COUNT(*) AS "count", SUM(value) AS "sum",
         MIN(value) AS "min", MAX(value) AS "max", AVG(value) AS "mean"
  FROM b GROUP BY bucket, entity, metric
),
latest AS (
  SELECT bucket, entity, metric, value AS latest_value,
         event_time_utc AS latest_event_time
  FROM (
    SELECT b.*, ROW_NUMBER() OVER (
             PARTITION BY bucket, entity, metric
             ORDER BY event_time_utc DESC, dedupe_id DESC) AS rn
    FROM b) WHERE rn = 1
)
SELECT a.bucket, a.entity, a.metric, a."count", a."sum", a."min", a."max",
       a."mean", l.latest_value, l.latest_event_time
FROM agg a JOIN latest l
  ON a.bucket = l.bucket AND a.entity = l.entity AND a.metric = l.metric;
""".strip()


def _shape_view_sql(view: str, unit: str, shape: str, order: str,
                    k: int = 10) -> str:
    """One member of the order-sensitive query family, in Feldera SQL.

    The same structure as `_view_sql`, with the selection order taken from the
    family's contrast rather than from the variant, and with the chosen value
    used where the shape places it. DBSP's dialect differs from PostgreSQL in
    the two ways that matter here: bucketing is TIMESTAMP_TRUNC rather than
    date_trunc, and the reserved words count, sum, min and max must be quoted.
    """
    if order == "contract":
        chosen_order = ("event_time_utc DESC, dedupe_id DESC"
                        if shape != "S2_first"
                        else "event_time_utc ASC, dedupe_id ASC")
    elif order == "arrival":
        chosen_order = ("processing_order DESC" if shape != "S2_first"
                        else "processing_order ASC")
    else:
        raise ValueError(f"unknown selection order: {order!r}")

    base = f"""
WITH src AS (
  SELECT event_time_utc, entity, metric, value, processing_order, dedupe_id
  FROM (SELECT *, ROW_NUMBER() OVER (
          PARTITION BY dedupe_id ORDER BY processing_order) AS rn
        FROM {_TABLE}) WHERE rn = 1
),
b AS (
  SELECT TIMESTAMP_TRUNC(event_time_utc, {unit}) AS bucket,
         entity, metric, value, event_time_utc, processing_order, dedupe_id
  FROM src
),
agg AS (
  SELECT bucket, entity, metric,
         COUNT(*) AS "count", SUM(value) AS "sum",
         MIN(value) AS "min", MAX(value) AS "max", AVG(value) AS "mean"
  FROM b GROUP BY bucket, entity, metric
),
chosen AS (
  SELECT bucket, entity, metric, value AS latest_value,
         event_time_utc AS latest_event_time
  FROM (
    SELECT b.*, ROW_NUMBER() OVER (
             PARTITION BY bucket, entity, metric
             ORDER BY {chosen_order}) AS rk
    FROM b
  ) WHERE rk = 1
)"""

    bucket_projection = """
SELECT a.bucket, a.entity, a.metric, a."count", a."sum", a."min", a."max",
       a."mean", c.latest_value, c.latest_event_time
FROM agg a JOIN chosen c
  ON a.bucket = c.bucket AND a.entity = c.entity AND a.metric = c.metric;"""

    if shape in ("S1_latest", "S2_first"):
        body = base + bucket_projection
    elif shape == "S3_rank_by_latest":
        body = base + f"""
SELECT bucket, metric, rank, entity, rank_value FROM (
  SELECT bucket, metric, entity, latest_value AS rank_value,
         ROW_NUMBER() OVER (PARTITION BY bucket, metric
                            ORDER BY latest_value DESC, entity ASC) AS rank
  FROM chosen
) ranked WHERE rank <= {int(k)};"""
    elif shape == "S5_filter_on_latest":
        body = base + """
, thresh AS (SELECT metric, AVG(value) AS t FROM b GROUP BY metric)
SELECT a.bucket, a.entity, a.metric, a."count", a."sum", a."min", a."max",
       a."mean", c.latest_value, c.latest_event_time
FROM agg a
JOIN chosen c
  ON a.bucket = c.bucket AND a.entity = c.entity AND a.metric = c.metric
JOIN thresh th ON th.metric = a.metric
WHERE c.latest_value >= th.t;"""
    else:
        raise ValueError(f"no Feldera SQL for shape {shape!r}")
    return f"CREATE MATERIALIZED VIEW {view} AS{body}".strip()

def _topk_view_sql(view: str, source_view: str, k: int, metric: str) -> str:
    return f"""
CREATE MATERIALIZED VIEW {view} AS
SELECT bucket, metric, rank, entity, rank_value
FROM (
  SELECT bucket, metric, entity, {metric} AS rank_value,
         ROW_NUMBER() OVER (
           PARTITION BY bucket, metric
           ORDER BY {metric} DESC, entity ASC
         ) AS rank
  FROM {source_view}
) ranked
WHERE rank <= {k};
""".strip()


def _program_sql(variant: str, topk_k: int | None = None,
                 topk_metric: str = "mean",
                 shape: str | None = None,
                 order: str | None = None,
                 hopping: bool = False) -> str:
    """The pipeline's SQL. With `shape` set it builds one family member instead
    of the scored view, under the SAME view names, so the readiness and read
    paths written for the scored run apply without a parallel implementation."""
    ddl = f"""
CREATE TABLE {_TABLE} (
  event_time_utc   TIMESTAMP NOT NULL,
  entity           VARCHAR   NOT NULL,
  metric           VARCHAR   NOT NULL,
  value            DOUBLE    NOT NULL,
  processing_order BIGINT    NOT NULL,
  dedupe_id        VARCHAR   NOT NULL,
  supersedes       VARCHAR
);
""".strip()
    if hopping:
        return "\n\n".join([
            ddl,
            _hopping_view_sql(_VIEW["hourly"], _FLOOR["hourly"], variant),
            _hopping_view_sql(_VIEW["daily"], _FLOOR["daily"], variant),
        ])
    if shape is not None:
        return "\n\n".join([
            ddl,
            _shape_view_sql(_VIEW["hourly"], _FLOOR["hourly"], shape, order),
            _shape_view_sql(_VIEW["daily"], _FLOOR["daily"], shape, order),
        ])
    statements = [
        ddl,
        _view_sql(_VIEW["hourly"], _FLOOR["hourly"], variant),
        _view_sql(_VIEW["daily"], _FLOOR["daily"], variant),
    ]
    if topk_k is not None:
        statements.extend(
            _topk_view_sql(_TOPK_VIEW[g], _VIEW[g], topk_k, topk_metric)
            for g in ("hourly", "daily")
        )
    return "\n\n".join(statements)


def _completion_token(response) -> Optional[str]:
    """The completion token an ingress response carries, if the server sent one.

    Older servers answer with an empty body; they fall back to the metrics
    check alone, as before."""
    try:
        payload = response.json()
    except Exception:
        return None
    token = payload.get("token") if isinstance(payload, dict) else None
    return token if isinstance(token, str) and token else None


class FelderaAdapter(EngineAdapter):
    name = "feldera"
    # DBSP maintains the D7 retract-latest view incrementally (Z-set retraction).
    supported_variants = ("dedup", "naive", "restate", "arrival_latest")
    native_topk_mode = "incremental"

    def __init__(self, **options) -> None:
        super().__init__(**options)
        # FELDERA_URL lets a validation run target an isolated deployment (a
        # different container, port and volume) without disturbing the shared
        # compose stack a campaign lane may be using. Same pattern as the
        # ksqlDB adapter's KSQL_RUN_SUFFIX. An explicit option still wins.
        self._base = options.get(
            "feldera_url", os.environ.get("FELDERA_URL", "http://localhost:8080"))
        self._pipeline = options.get("pipeline", "equitable")
        self._ingested_total = 0

    def connect(self) -> "FelderaAdapter":
        try:
            import requests  # noqa: F401
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("feldera adapter needs requests (pip install requests)") from exc
        return self

    def _req(self, method: str, path: str, **kw):
        import requests
        kw.setdefault("timeout", self.options.get("http_timeout_s", 120))
        r = requests.request(method, f"{self._base}{path}", **kw)
        if r.status_code >= 400:
            # Surface Feldera's error body (raise_for_status hides it).
            raise RuntimeError(f"feldera {r.status_code} on {method} {path}: {r.text[:400]}")
        return r

    def _stop(self) -> None:
        """Stop the pipeline. v0.324.0 removed POST /shutdown; the replacement is
        POST /stop, and `force=true` is the hard stop that /shutdown used to be
        (a graceful stop can suspend rather than terminate). Callers that only
        want a best-effort teardown wrap this in try/except."""
        self._req("POST", f"/v0/pipelines/{self._pipeline}/stop",
                  params={"force": "true"})

    def _await_stopped(self) -> None:
        """A pipeline can only be redefined (PUT) once it is fully stopped;
        PUTting a Running/Stopping pipeline returns 400. Poll until the
        deployment is Stopped (or the pipeline no longer exists). v0.324.0
        renamed this terminal state from `Shutdown` to `Stopped`; both are
        accepted so a rollback to an older pin still works."""
        deadline = time.time() + float(self.options.get("shutdown_timeout_s", 120))
        while time.time() < deadline:
            try:
                j = self._req("GET", f"/v0/pipelines/{self._pipeline}").json()
            except Exception:
                return  # gone == stopped
            status = j.get("deployment_status") or j.get("status")
            if status in (None, "Stopped", "Shutdown"):
                return
            time.sleep(1.0)

    def _runtime_config(self) -> dict:
        """Runtime config for the pipeline PUT.

        `fault_tolerance` is omitted unless explicitly requested, because the
        open-source edition rejects the whole PUT when it is present. Setting
        options["fault_tolerance"] (e.g. {"model": "exactly_once"}) is the single
        switch that makes D5 expressible on an edition that supports it."""
        config = {
            "workers": int(self.options.get("workers", 1)),
            "storage": bool(self.options.get("storage", False)),
        }
        fault_tolerance = self.options.get("fault_tolerance")
        if fault_tolerance:
            config["fault_tolerance"] = fault_tolerance
        return config

    supported_shapes = ("S1_latest", "S2_first", "S3_rank_by_latest",
                        "S5_filter_on_latest")

    def setup_shape(self, *, shape: str, order: str) -> None:
        if shape not in self.supported_shapes:
            raise NotImplementedError(
                f"{self.name} cannot express shape {shape!r}")
        self._shape, self._shape_order = shape, order
        try:
            self.setup(variant="dedup")
        finally:
            self._shape = self._shape_order = None

    # --- D8: hopping window ------------------------------------------------
    #: Calcite has no hopping primitive here either, so the window is a cross
    #: join against a VALUES list of hop offsets, as for Materialize.
    hopping_mode = "crossjoin"

    def setup_hopping(self, *, size_n: int = 2, hop_n: int = 1) -> None:
        """Rebuild the pipeline with the hopping views under the scored names."""
        self._hopping = True
        try:
            self.setup(variant="dedup")
        finally:
            self._hopping = False

    def read_hopping(self, grain: str) -> pd.DataFrame:
        """The hopping table is the bucket table, so read_view's path applies."""
        return self.read_view(grain)

    def read_shape(self, grain: str) -> pd.DataFrame:
        """Read the shape view, which occupies the scored view's name.

        Feldera and Calcite fold unquoted output aliases to upper case while
        double-quoted ones stay lower, so the same normalization read_view
        applies is needed here before either schema can be canonicalized.
        """
        from .base import canonical_shape_frame
        self._check_grain(grain)
        response = self._req(
            "GET", f"/v0/pipelines/{self._pipeline}/query",
            params={"sql": f"SELECT * FROM {_VIEW[grain]}", "format": "json"})
        records = self._parse_query(response.text)
        if not records:
            return empty_view()
        frame = pd.DataFrame(records).rename(columns=lambda c: c.lower())
        if "latest_event_time" in frame.columns:
            frame["latest_event_time"] = canonical_event_time(
                frame["latest_event_time"])
        return canonical_shape_frame(frame, grain)

    def setup(self, *, variant: str = "dedup") -> None:
        self._check_variant(variant)
        self._ingested_total = 0
        name = self._pipeline
        # Recreate the pipeline from scratch so the SQL program reflects `variant`.
        try:
            self._stop()
        except Exception:
            pass
        self._await_stopped()
        self._req("PUT", f"/v0/pipelines/{name}", json={
            "name": name,
            "description": "EquiTable diagnostic pipeline",
            # storage=true would spill computed state so a checkpoint() could be
            # restored after a crash+restart (D5). Left OFF by default: the pinned
            # open-source Feldera v0.324.0 image gates the configuration D5 needs
            # behind its Enterprise edition. `/v0/config` reports
            # edition="Open source", and any runtime_config carrying a
            # `fault_tolerance` block is rejected with
            # InvalidRuntimeConfig / {"EnterpriseFeature": "fault tolerance"},
            # so no durable checkpoint-and-recovery configuration can be
            # expressed here (run_d5_recovery N/A
            # classification). Opt in via options["storage"] and
            # options["fault_tolerance"] on an edition that exposes them.
            "runtime_config": self._runtime_config(),
            "program_config": {},
            "program_code": _program_sql(
                variant, self.options.get("native_topk_k"),
                self.options.get("native_topk_metric", "mean"),
                shape=getattr(self, "_shape", None),
                order=getattr(self, "_shape_order", None),
                hopping=getattr(self, "_hopping", False)),
        })
        # A pipeline cannot start until its SQL program has compiled; starting
        # early returns 400. Wait for compilation to succeed first.
        self._await_compiled()
        self._req("POST", f"/v0/pipelines/{name}/start")
        self._await_status("Running")

    def _await_compiled(self) -> None:
        deadline = time.time() + float(self.options.get("compile_timeout_s", 180))
        while time.time() < deadline:
            j = self._req("GET", f"/v0/pipelines/{self._pipeline}").json()
            status = str(j.get("program_status") or j.get("status") or "").lower()
            if "success" in status:
                return
            if "error" in status or "fail" in status:
                raise RuntimeError(
                    f"feldera program failed to compile: {status} :: {j.get('program_error')}")
            time.sleep(1.0)

    def _await_status(self, want: str) -> None:
        deadline = time.time() + float(self.options.get("deploy_timeout_s", 120))
        while time.time() < deadline:
            r = self._req("GET", f"/v0/pipelines/{self._pipeline}")
            status = r.json().get("deployment_status") or r.json().get("status")
            if status == want:
                return
            time.sleep(1.0)
        raise RuntimeError(f"feldera pipeline did not reach {want} within timeout")

    def ingest_log(self, df: pd.DataFrame) -> IngestStats:
        name = self._pipeline
        t0 = time.perf_counter()
        lines: List[str] = []
        for rec in df.itertuples(index=False):
            sup = rec.supersedes
            lines.append(json.dumps({"insert": {
                # Keep the normalized log's full microsecond precision. Dropping
                # the fractional component changes the contract's latest-row
                # tiebreak before Feldera ever sees the record.
                "event_time_utc": pd.Timestamp(rec.event_time_utc).strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"),
                "entity": str(rec.entity), "metric": str(rec.metric),
                "value": float(rec.value), "processing_order": int(rec.processing_order),
                "dedupe_id": str(rec.dedupe_id),
                "supersedes": None if sup is None or (isinstance(sup, float) and pd.isna(sup)) else str(sup),
            }}))
        # Push in chunks so a large fixture doesn't exceed request limits.
        chunk = int(self.options.get("ingress_chunk", 50000))
        token: Optional[str] = None
        for i in range(0, len(lines), chunk):
            body = "\n".join(lines[i:i + chunk])
            # v0.324.0 makes `force` a required query parameter on ingress.
            # false = respect backpressure, the pre-0.324 default.
            response = self._req(
                "POST", f"/v0/pipelines/{name}/ingress/{_TABLE}",
                params={"format": "json", "update_format": "insert_delete",
                        "force": "false"},
                data=body.encode(), headers={"Content-Type": "application/json"})
            # A token covers every record the endpoint received before it was
            # issued, so the last chunk's token stands for the whole log.
            token = _completion_token(response) or token
        # Ingress returns once rows are queued, not once the incremental views have
        # caught up; querying now yields a partially-materialized view (the larger
        # hourly view lags behind daily). Block until the step that consumed the
        # last record has completed, then confirm the pipeline metrics agree.
        target = self._ingested_total + len(df)
        if token is not None:
            self._await_complete(token)
        self._await_processed(target)
        self._ingested_total = target
        return IngestStats(rows_sent=len(df), seconds=time.perf_counter() - t0)

    def _await_complete(self, token: str) -> None:
        """Block until the pipeline reports the ingress `token` complete.

        v0.324.0 answers every ingress request with a completion token, and
        GET completion_status turns "complete" once the step that consumed
        those records has run to its end. The global metrics that
        `_await_processed` reads move earlier, when records leave the input
        buffer for the circuit, so under host load a read that follows a
        satisfied metrics check can still see a view one step behind: tables
        short of keys or undercounting in the view read first, while an idle
        replay of the same cell is exact. The token is tied to the step, so it
        cannot be early.
        """
        timeout = float(self.options.get("process_timeout_s", 180))
        deadline = time.time() + timeout
        last: Any = None
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            try:
                last = self._req(
                    "GET", f"/v0/pipelines/{self._pipeline}/completion_status",
                    params={"token": token},
                    timeout=min(float(self.options.get("http_timeout_s", 120)),
                                max(0.001, remaining))).json()
                if isinstance(last, dict) and last.get("status") == "complete":
                    return
            except Exception as exc:
                last = exc
            time.sleep(0.25)
        raise RuntimeError(
            f"feldera did not complete the ingress token within {timeout:g}s "
            f"(last completion status: {last})")

    def _await_processed(self, expected_rows: int) -> None:
        timeout = float(self.options.get("process_timeout_s", 180))
        deadline = time.time() + timeout
        last_metrics = None
        last_error = None
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            try:
                gm = self._req(
                    "GET", f"/v0/pipelines/{self._pipeline}/stats",
                    timeout=min(float(self.options.get("http_timeout_s", 120)),
                                max(0.001, remaining))) \
                    .json().get("global_metrics", {})
            except Exception as exc:
                last_error = exc
                time.sleep(1.0)
                continue
            buffered = gm.get("buffered_input_records", 0)
            processed = gm.get("total_processed_records", 0)
            total_in = gm.get("total_input_records", 0)
            last_metrics = (total_in, processed, buffered)
            if total_in >= expected_rows and buffered == 0 and processed >= total_in:
                return
            time.sleep(1.0)
        if last_metrics is not None:
            total_in, processed, buffered = last_metrics
            detail = (
                f"last metrics: total_input_records={total_in}, "
                f"total_processed_records={processed}, "
                f"buffered_input_records={buffered}"
            )
        elif last_error is not None:
            detail = f"last stats error: {last_error}"
        else:
            detail = "no pipeline statistics were observed"
        raise RuntimeError(
            f"feldera did not process {expected_rows} input records within "
            f"{timeout:g}s ({detail})")

    def checkpoint(self) -> None:
        """Durability barrier (D5): ask Feldera to checkpoint the pipeline's
        computed state so a restart can restore it (scripts/run/run_d5_recovery.py).
        Raises RuntimeError when the endpoint is unavailable (e.g. checkpointing
        not supported by the deployed Feldera edition) -- the D5 runner records
        that as `checkpoint_unsupported` rather than a crash."""
        r = self._req("POST", f"/v0/pipelines/{self._pipeline}/checkpoint")
        # Newer APIs return {"checkpoint_sequence_number": N}; poll status until
        # that checkpoint has completed (best-effort; older APIs return nothing).
        try:
            seq = r.json().get("checkpoint_sequence_number")
        except Exception:
            seq = None
        if seq is None:
            return
        deadline = time.time() + float(self.options.get("checkpoint_timeout_s", 120))
        while time.time() < deadline:
            j = self._req("GET", f"/v0/pipelines/{self._pipeline}/checkpoint_status").json()
            if j.get("success") is not None and j["success"] >= seq:
                return
            if j.get("failure") and j["failure"].get("sequence_number") == seq:
                raise RuntimeError(f"feldera checkpoint {seq} failed: {j['failure']}")
            time.sleep(1.0)
        raise RuntimeError(f"feldera checkpoint {seq} did not complete within timeout")

    def resume(self) -> None:
        """D5: after a hard kill + container restart the pipeline comes back in a
        Failed/stopped state. Feldera refuses to start a Failed pipeline, so first
        /stop to clear the error (this does NOT delete storage), then /start
        the EXISTING pipeline (no PUT/setup) so it restores its computed state
        from the last checkpoint in storage, and wait until it is queryable. If
        no usable checkpoint exists it restarts empty, which the D5 comparison
        correctly records as state-not-recovered."""
        try:
            self._stop()
            self._await_stopped()
        except Exception:
            pass
        self._req("POST", f"/v0/pipelines/{self._pipeline}/start")
        self._await_status("Running")

    def read_view(self, grain: str) -> pd.DataFrame:
        self._check_grain(grain)
        r = self._req("GET", f"/v0/pipelines/{self._pipeline}/query",
                      params={"sql": f"SELECT * FROM {_VIEW[grain]}", "format": "json"})
        records = self._parse_query(r.text)
        if not records:
            return empty_view()
        df = pd.DataFrame(records)
        # Feldera/Calcite folds unquoted output aliases to UPPERCASE (BUCKET,
        # LATEST_EVENT_TIME, ...) while the double-quoted ones ("count", "sum")
        # stay lowercase. coerce_view expects all-lowercase VIEW_COLUMNS, so
        # normalize the case before touching any column.
        df = df.rename(columns={c: c.lower() for c in df.columns})
        df["latest_event_time"] = canonical_event_time(df["latest_event_time"])
        return coerce_view(df, grain)

    def read_native_topk(self, grain: str) -> pd.DataFrame:
        self._check_grain(grain)
        from src.diagnostics.d6 import TOPK_COLUMNS
        r = self._req("GET", f"/v0/pipelines/{self._pipeline}/query",
                      params={"sql": f"SELECT * FROM {_TOPK_VIEW[grain]}",
                              "format": "json"})
        records = self._parse_query(r.text)
        if not records:
            return pd.DataFrame(columns=TOPK_COLUMNS)
        out = pd.DataFrame(records).rename(columns=lambda c: c.lower())
        from .base import canonical_bucket
        out["bucket"] = canonical_bucket(out["bucket"], grain)
        out["rank"] = out["rank"].astype("int64")
        out["rank_value"] = out["rank_value"].astype(float)
        return out[TOPK_COLUMNS].sort_values(
            ["bucket", "metric", "rank"], kind="stable").reset_index(drop=True)

    @staticmethod
    def _parse_query(text: str) -> List[dict]:
        text = text.strip()
        if not text:
            return []
        try:                                  # a single JSON array or object
            data = json.loads(text)
            records = data if isinstance(data, list) else [data]
        except json.JSONDecodeError:          # newline-delimited JSON objects
            records = [json.loads(x) for x in text.splitlines() if x.strip()]
        # An ad-hoc query error comes back as {"error": "..."} rather than rows;
        # surface it instead of letting it masquerade as a data row (which then
        # fails downstream with a cryptic KeyError on a missing view column).
        if len(records) == 1 and isinstance(records[0], dict) and "error" in records[0]:
            raise RuntimeError(f"feldera query error: {records[0]['error']}")
        return records

    def close(self) -> None:
        try:
            self._stop()
        except Exception:
            pass
