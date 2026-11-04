"""ksqlDB adapter (Kafka producer + ksqlDB REST, ports 19092 / 8088).

Rows are produced as JSON to a Kafka topic; a ksqlDB STREAM reads the topic and a
derived TABLE holds the per-(bucket, entity, metric) aggregate. Two structural
choices are deliberate, because they are the engine's *real* behavior that the
benchmark measures:

  * Dedup ("dedup" variant) is expressed with `LATEST_BY_OFFSET(...) GROUP BY
    dedupe_id`, then re-aggregated. This is ksqlDB's idiomatic dedup and is
    exactly where the documented residual failure lives: LATEST_BY_OFFSET keeps
    the latest row *by Kafka offset*, so duplicates landing in different windows
    are not always collapsed.
  * Per-bucket "latest" uses `LATEST_BY_OFFSET(value/event_time)`, i.e. latest by
    arrival offset, not by event_time. On an in-order log this equals the oracle;
    under D2 (bounded reorder) it diverges -- the arrival-order-invariance
    finding.

Buckets are emitted as truncated epoch-millis (BIGINT) and formatted to the
oracle's canonical string in Python, avoiding ksqlDB/oracle date-format skew.
Requires `confluent-kafka` and `requests`.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
import uuid
from typing import Any, Dict, List

import pandas as pd

from .base import (
    EngineAdapter, IngestStats, canonical_bucket, canonical_event_time,
    coerce_view, empty_view,
)

_TRUNC_MS = {"hourly": 3600000, "daily": 86400000}
#: D8 window geometry per grain, in ksqlDB's WINDOW HOPPING vocabulary. ksqlDB
#: is the one engine here with a real hopping primitive that also windows by
#: EVENT time, because the stream declares TIMESTAMP='event_time_utc', so
#: ROWTIME is event time and no arithmetic bucketing is needed.
_HOP_WINDOW = {"hourly": ("2 HOURS", "1 HOUR"), "daily": ("2 DAYS", "1 DAY")}
#: Retention and grace must be stated, and stated large, or the comparison is
#: against a truncated table. With ksqlDB's defaults a two-day log yields only
#: the last 24 hours of windows: 24 window labels where the oracle has 48, a
#: strict subset, so the geometry is right and the older windows have simply
#: been expired. Grace matters for the same reason in
#: the other direction: the benchmark delivers events shuffled, and a short
#: grace period drops the out-of-order ones instead of folding them in. Both
#: are set far beyond any real log's event-time span so that what is compared
#: is the engine's arithmetic, not its retention policy. ksqlDB requires
#: retention >= size + grace.
_HOP_RETENTION = "7300 DAYS"

#: Kafka retention for the delete-policy topics this adapter creates: never.
#: A stream sink carries its records' event time, and Kafka's time retention
#: judges a segment by its largest record timestamp, so the broker's default
#: 7 days would delete a benchmark stream within one retention check (5
#: minutes) of its creation, whatever its age on the wall clock. A daily view
#: created after such a deletion sees no records and blocks; one still reading
#: when it strikes aggregates part of the input and is scored as a verdict.
#: Every topic here is dropped with DELETE TOPIC by the next setup(), so
#: nothing accumulates. Table sinks are compacted and never time-deleted.
_KEEP_FOREVER_MS = -1
_HOP_GRACE = "3650 DAYS"
_TIMESTAMP_FORMAT = "yyyy-MM-dd''T''HH:mm:ss.SSSSSSX"


class _QueryStarting(RuntimeError):
    """A persistent query exists but has not committed its first offsets yet.

    Distinct from a real fault (an unreachable broker, a failed EXPLAIN): the
    topology is coming up and the barrier has nothing to measure yet. Kafka
    Streams commits only after its first poll returns, so on a host running
    seven engines at once the gap between CREATE TABLE ... AS SELECT and the
    first commit is dominated by rebalance and RocksDB startup, not by data
    volume -- which is why it also hit a 10k-row cell.
    """

    def __init__(self, message: str, *, query_ids: tuple = ()) -> None:
        super().__init__(message)
        self.query_ids = tuple(query_ids)


class _QueryFailed(RuntimeError):
    """ksqlDB reports a persistent query in ERROR: waiting cannot help.

    Carries ksqlDB's own query errors so the blocked cell records why, instead
    of a generic "no committed partitions" after the whole startup budget.
    """

    def __init__(self, message: str, *, query_ids: tuple = ()) -> None:
        super().__init__(message)
        self.query_ids = tuple(query_ids)


class KsqlDbAdapter(EngineAdapter):
    name = "ksqldb"
    # "restate" (D7, supersedes model) is expressible in ksqlDB as a key-join: a
    # TABLE keyed by `supersedes` joined to the deduped base rows on dedupe_id.
    # Implemented in setup() so the diagnostic can settle empirically whether the
    # non-retroactive stream-table join silently keeps stale values (expressible but
    # wrong -- a stronger D7 finding than "unsupported"). See docs/SEMANTICS.md, Restatements.
    # NOTE: ksqlDB has no event-time "latest" primitive -- LATEST_BY_OFFSET is
    # offset (arrival) ordered -- so its "dedup" view is the arrival_latest
    # formulation. The alias makes that structural limit explicit in the matrix
    # instead of leaving it buried in this adapter's SQL.
    supported_variants = ("dedup", "naive", "restate", "arrival_latest")
    #: `dedup_table` is retained for reproduction only and is not in
    #: supported_variants: ksqlDB 0.29.0 rejects it at CREATE TABLE with
    #:   "The aggregation functions MIN, MAX and LATEST_BY_OFFSET cannot be
    #:    applied to a table" (error_code 40001).
    #: The contract's reduction needs MIN, MAX and a latest value, so aggregating
    #: the deduplication table directly is not expressible here. That leaves the
    #: `dedup` formulation, which re-reads the table's changelog as an append
    #: stream. Measured on a four-event witness: a redelivery produced inside the
    #: same commit interval as its original is coalesced into one changelog record
    #: and the hour is exact (count 3, sum 48.00); a redelivery produced after the
    #: interval emits a second changelog record and the hour double counts
    #: (count 4, sum 74.80). This is why ksqlDB's D1 repair does not reduce the
    #: divergence, and it is a language limit rather than an adapter defect.
    #: variants this engine can only express as its arrival-ordered equivalent.
    arrival_ordered_variants = ("dedup", "arrival_latest")

    def __init__(self, **options) -> None:
        super().__init__(**options)
        self._url = options.get("ksqldb_url", "http://localhost:8088")
        self._broker = options.get("kafka", "localhost:19092")
        self._admin = None
        self._producer = None
        # Run-scoped object/topic names avoid cross-run stale-state collisions.
        # Precedence: explicit run_suffix option > KSQL_RUN_SUFFIX env (the matrix
        # sets this ONCE per invocation from the run stamp, so every ksqlDB cell in
        # a run shares deterministic names -- each cell's setup drops+recreates the
        # previous one's, so objects don't pile up within a run) > random uuid
        # fallback for ad-hoc single-adapter use.
        suffix = str(
            options.get("run_suffix")
            or os.environ.get("KSQL_RUN_SUFFIX")
            or uuid.uuid4().hex[:10]
        ).lower()
        suffix = re.sub(r"[^a-z0-9_]", "_", suffix)
        if not suffix or suffix[0].isdigit():
            suffix = f"r_{suffix}"
        prefix = f"vs_{suffix}"
        self._topic = f"{prefix}_obs"
        self._stream = f"{prefix}_obs"
        self._view = {"hourly": f"{prefix}_hourly", "daily": f"{prefix}_daily"}
        self._dedup_t = f"{prefix}_dedup"
        self._dedup_s = f"{prefix}_dedup_s"
        self._rbase_t = f"{prefix}_rbase"
        self._rbase_s = f"{prefix}_rbase_s"
        self._restate_t = f"{prefix}_restate"
        self._corrected = f"{prefix}_corrected"
        #: One record per aggregate view recreated because its query never
        #: committed; run_diagnostic copies this into the run summary.
        self.view_restarts: List[Dict[str, Any]] = []
        # Default source so read_view works on a fresh adapter (D5 read-after-
        # restart reconnects without setup(), which is what sets _source per
        # variant). After a restart the view already exists, so read_view reads
        # it and never touches _source.
        self._source = self._stream

    # --- REST helpers -----------------------------------------------------
    def _post(
        self, path: str, ksql: str, *, timeout_s: float | None = None,
    ) -> Any:
        import requests
        r = requests.post(
            f"{self._url}/{path}",
            json={"ksql": ksql,
                  "streamsProperties": {
                      "ksql.streams.auto.offset.reset": "earliest",
                      # Diagnosis hook, unused by the matrix: lets a reproduction
                      # raise ksql.streams.commit.interval.ms and
                      # ksql.streams.cache.max.bytes.buffering, which decide
                      # whether two updates to one dedup-table row reach the
                      # changelog as one record or two.
                      **dict(self.options.get("streams_properties") or {}),
                  }},
            timeout=(
                float(timeout_s) if timeout_s is not None
                else float(self.options.get("http_timeout_s", 120))
            ),
        )
        if r.status_code >= 400:
            # Surface ksqlDB's actual error message (raise_for_status hides the body).
            raise RuntimeError(
                f"ksqlDB {r.status_code} on /{path}: {r.text[:500]} :: statement={ksql.strip()[:200]}"
            )
        return r

    def connect(self) -> "KsqlDbAdapter":
        try:
            from confluent_kafka import Producer  # noqa: F401
            from confluent_kafka.admin import AdminClient  # noqa: F401
            import requests  # noqa: F401
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "ksqldb adapter needs confluent-kafka + requests "
                "(pip install confluent-kafka requests)"
            ) from exc
        from confluent_kafka import Producer
        from confluent_kafka.admin import AdminClient
        # librdkafka's default queue is 100k messages / 1GiB. The confirmation
        # cohort produces 1M rows per cell (more on D7, which appends
        # restatements), so at the default the queue overflows long before the
        # broker drains it and produce() raises "Local: Queue full". That scored
        # as a blocked cell, not a verdict. Raising the bound is not sufficient
        # on its own -- ingest_log also blocks on a full queue rather than
        # failing -- but it keeps the common case from ever reaching backpressure.
        self._producer = Producer({
            "bootstrap.servers": self._broker,
            "queue.buffering.max.messages": int(
                self.options.get("producer_queue_messages", 2_000_000)),
            "queue.buffering.max.kbytes": int(
                self.options.get("producer_queue_kbytes", 2_097_152)),
        })
        self._admin = AdminClient({"bootstrap.servers": self._broker})
        return self

    # --- lifecycle --------------------------------------------------------
    def _list_persistent_queries(self) -> List[str]:
        try:
            payload = self._post("ksql", "SHOW QUERIES;").json()
        except Exception:
            return []
        ids: List[str] = []
        for item in payload if isinstance(payload, list) else []:
            for q in item.get("queries", []) if isinstance(item, dict) else []:
                qid = q.get("id")
                if qid and str(q.get("queryType", "PERSISTENT")).upper() != "PUSH":
                    ids.append(qid)
        return ids

    def _terminate_all_queries(self) -> None:
        """A stream/table cannot be dropped while a persistent query reads from or
        writes to it, so terminate every running query first (otherwise the DROPs
        below are silently swallowed and CREATE STREAM fails 'already exists').

        TERMINATE is asynchronous: poll until no persistent queries remain before
        returning, or the DROPs can race a still-stopping query."""
        deadline = time.time() + float(self.options.get("terminate_timeout_s", 60))
        while time.time() < deadline:
            qids = self._list_persistent_queries()
            if not qids:
                return
            for qid in qids:
                # TERMINATE takes an unquoted query-id identifier; quoting it
                # makes ksqlDB reject the statement and the query survives.
                try:
                    self._post("ksql", f"TERMINATE {qid};")
                except Exception:
                    pass
            time.sleep(1.0)

    def _drop_retry(self, stmt: str) -> None:
        """Retry a DROP until it truly takes effect. Right after TERMINATE, a
        source's backing topic/consumer can still be releasing, so DROP ... DELETE
        TOPIC can fail transiently; a fire-and-forget DROP would swallow that
        and leave the object behind, breaking the next run's CREATE. 'Does not
        exist' counts as success (idempotent teardown)."""
        deadline = time.time() + float(self.options.get("drop_timeout_s", 60))
        last = ""
        while time.time() < deadline:
            try:
                self._post("ksql", stmt)
                return
            except Exception as exc:
                last = str(exc)
                if "does not exist" in last:
                    return
                time.sleep(1.5)
        raise RuntimeError(f"ksqlDB DROP did not complete: {stmt} :: last error {last[:200]}")

    def setup(self, *, variant: str = "dedup") -> None:
        self._check_variant(variant)
        # Terminate persistent queries first so the DROPs below actually take
        # effect (a source with dependent queries cannot be dropped).
        self._terminate_all_queries()
        # Tear down every object AND its backing topic, including the source, so a
        # re-run starts from an empty topic. Keeping the source topic across runs
        # would replay stale rows into the aggregate (idempotent for the dedup
        # variant, but it inflates COUNT/SUM for the naive variant).
        # Order matters: drop consumers before the source whose topic they read;
        # otherwise ksqlDB refuses to delete a topic that is still used by another
        # run-scoped stream/table.
        for stmt in (
            f"DROP TABLE IF EXISTS {self._view['hourly']} DELETE TOPIC;",
            f"DROP TABLE IF EXISTS {self._view['daily']} DELETE TOPIC;",
            f"DROP STREAM IF EXISTS {self._corrected} DELETE TOPIC;",
            f"DROP TABLE IF EXISTS {self._restate_t} DELETE TOPIC;",
            f"DROP STREAM IF EXISTS {self._rbase_s};",
            f"DROP TABLE IF EXISTS {self._rbase_t} DELETE TOPIC;",
            f"DROP STREAM IF EXISTS {self._dedup_s};",
            f"DROP TABLE IF EXISTS {self._dedup_t} DELETE TOPIC;",
            f"DROP STREAM IF EXISTS {self._stream} DELETE TOPIC;",
        ):
            self._drop_retry(stmt)
        self._post("ksql", f"""
            CREATE STREAM {self._stream} (
              event_time_utc   VARCHAR,
              processing_order BIGINT,
              entity           VARCHAR,
              metric           VARCHAR,
              value            DOUBLE,
              dedupe_id        VARCHAR,
              supersedes       VARCHAR
            ) WITH (
              KAFKA_TOPIC='{self._topic}', PARTITIONS=1, VALUE_FORMAT='JSON',
              TIMESTAMP='event_time_utc',
              TIMESTAMP_FORMAT='{_TIMESTAMP_FORMAT}',
              RETENTION_MS={_KEEP_FOREVER_MS}
            );
        """)
        if variant in ("dedup", "dedup_table"):
            self._post("ksql", f"""
                CREATE TABLE {self._dedup_t} WITH (KAFKA_TOPIC='{self._dedup_t}', KEY_FORMAT='JSON') AS
                SELECT dedupe_id,
                       LATEST_BY_OFFSET(event_time_utc) AS event_time_utc,
                       LATEST_BY_OFFSET(processing_order) AS processing_order,
                       LATEST_BY_OFFSET(entity) AS entity,
                       LATEST_BY_OFFSET(metric) AS metric,
                       LATEST_BY_OFFSET(value)  AS value
                FROM {self._stream} GROUP BY dedupe_id;
            """)
        if variant == "dedup":
            # `dedup` re-reads the dedup TABLE's changelog as an append stream. A
            # table changelog carries one record per row UPDATE, so a redelivered
            # source event yields a second record and the downstream aggregate
            # counts it again. `dedup_table` aggregates the table directly and is
            # the control that tells a ksqlDB semantic limit apart from this
            # formulation's own defect.
            self._post("ksql", f"""
                CREATE STREAM {self._dedup_s} (
                  dedupe_id VARCHAR KEY, event_time_utc VARCHAR,
                  processing_order BIGINT, entity VARCHAR, metric VARCHAR, value DOUBLE
                ) WITH (KAFKA_TOPIC='{self._dedup_t}', VALUE_FORMAT='JSON');
            """)
        elif variant == "restate":
            # D7 (docs/SEMANTICS.md, Restatements). Contract: content-dedup, THEN resolve each
            # restatement's `supersedes` pointer. Chain:
            #   vs_rbase   -- base rows only (supersedes IS NULL), keyed by dedupe_id
            #   vs_rbase_s -- its changelog as a stream (keyed dedupe_id)
            #   vs_restate -- latest superseding value, keyed by `supersedes`
            #   vs_corrected -- stream-table LEFT JOIN on dedupe_id = supersedes
            # NOTE (the open question this is built to settle): a ksqlDB stream-table
            # join is not retroactive -- each stream row joins the table state as of
            # its own offset. A restatement arrives after the row it corrects, so the
            # join may see an empty table and silently keep the stale value. If so
            # the view is expressible but wrong, which is a stronger D7 finding than
            # "unsupported". Let the diagnostic decide; do not tune this to pass.
            self._post("ksql", f"""
                CREATE TABLE {self._rbase_t} WITH (KAFKA_TOPIC='{self._rbase_t}', KEY_FORMAT='JSON') AS
                SELECT dedupe_id,
                       LATEST_BY_OFFSET(event_time_utc)   AS event_time_utc,
                       LATEST_BY_OFFSET(processing_order) AS processing_order,
                       LATEST_BY_OFFSET(entity)           AS entity,
                       LATEST_BY_OFFSET(metric)           AS metric,
                       LATEST_BY_OFFSET(value)            AS value
                FROM {self._stream} WHERE supersedes IS NULL GROUP BY dedupe_id;
            """)
            self._post("ksql", f"""
                CREATE STREAM {self._rbase_s} (
                  dedupe_id VARCHAR KEY, event_time_utc VARCHAR,
                  processing_order BIGINT, entity VARCHAR, metric VARCHAR, value DOUBLE
                ) WITH (KAFKA_TOPIC='{self._rbase_t}', VALUE_FORMAT='JSON');
            """)
            self._post("ksql", f"""
                CREATE TABLE {self._restate_t} WITH (KAFKA_TOPIC='{self._restate_t}', KEY_FORMAT='JSON') AS
                SELECT supersedes, LATEST_BY_OFFSET(value) AS new_value
                FROM {self._stream} WHERE supersedes IS NOT NULL GROUP BY supersedes;
            """)
            # `b.dedupe_id` is projected only to satisfy ksqlDB, which rejects a
            # join whose key appears in neither side of the projection:
            #   error_code 40001, "Key missing from projection (ie, SELECT). The
            #   query ... must include the join expressions B.DEDUPE_ID or
            #   R.SUPERSEDES in its projection".
            # Without it every ksqlDB D7 cell was BLOCKED at setup, so the matrix
            # reported `err` rather than a verdict on all five domains. The column
            # is inert downstream: the aggregate view selects event_time_utc,
            # entity, metric and value by name and groups by them.
            self._post("ksql", f"""
                CREATE STREAM {self._corrected} WITH (
                  KAFKA_TOPIC='{self._corrected}', VALUE_FORMAT='JSON',
                  RETENTION_MS={_KEEP_FOREVER_MS}) AS
                SELECT b.dedupe_id        AS dedupe_id,
                       b.event_time_utc   AS event_time_utc,
                       b.processing_order AS processing_order,
                       b.entity           AS entity,
                       b.metric           AS metric,
                       COALESCE(r.new_value, b.value) AS value
                FROM {self._rbase_s} b LEFT JOIN {self._restate_t} r ON b.dedupe_id = r.supersedes;
            """)
        self._source = {"dedup": self._dedup_s,
                        "dedup_table": self._dedup_t,
                        "restate": self._corrected}.get(variant, self._stream)
        self._variant = variant

    def _view_exists(self, grain: str) -> bool:
        """True iff the aggregate table already exists (e.g. a persistent query
        resumed from Kafka after a D5 restart), so read_view reads it instead of
        recreating (which would wipe the recovered state)."""
        try:
            r = self._post("ksql", "LIST TABLES;")
            body = r.json() if hasattr(r, "json") else r
            want = self._view[grain].upper()
            for stmt in (body if isinstance(body, list) else [body]):
                for t in (stmt.get("tables") or []):
                    if str(t.get("name", "")).upper() == want:
                        return True
        except Exception:
            pass
        return False

    def _create_hopping_view(self, grain: str) -> None:
        """The D8 hopping aggregate, using ksqlDB's native hopping window.

        Unlike the tumbling view this does not compute a bucket arithmetically:
        the window supplies WINDOWSTART, which ksqlDB puts in the table KEY, so
        read_hopping takes the label from there rather than from a projected
        column.
        """
        size, advance = _HOP_WINDOW[grain]
        self._post("ksql", f"""
            CREATE TABLE IF NOT EXISTS {self._view[grain]} WITH (KEY_FORMAT='JSON') AS
            SELECT
              entity, metric,
              COUNT(*)                       AS cnt,
              SUM(value)                     AS sum_v,
              MIN(value)                     AS min_v,
              MAX(value)                     AS max_v,
              AVG(value)                     AS mean_v,
              LATEST_BY_OFFSET(value)          AS latest_value,
              LATEST_BY_OFFSET(event_time_utc) AS latest_event_time
            FROM {self._source}
            WINDOW HOPPING (SIZE {size}, ADVANCE BY {advance},
                            RETENTION {_HOP_RETENTION},
                            GRACE PERIOD {_HOP_GRACE})
            GROUP BY entity, metric;
        """)

    def _create_view(self, grain: str) -> None:
        ms = _TRUNC_MS[grain]
        self._post("ksql", f"""
            CREATE TABLE IF NOT EXISTS {self._view[grain]} WITH (KEY_FORMAT='JSON') AS
            SELECT
              (STRINGTOTIMESTAMP(event_time_utc, '{_TIMESTAMP_FORMAT}') / {ms}) * {ms} AS bucket_ms,
              entity, metric,
              COUNT(*)                       AS cnt,
              SUM(value)                     AS sum_v,
              MIN(value)                     AS min_v,
              MAX(value)                     AS max_v,
              AVG(value)                     AS mean_v,
              LATEST_BY_OFFSET(value)          AS latest_value,
              LATEST_BY_OFFSET(event_time_utc) AS latest_event_time
            FROM {self._source}
            GROUP BY
              (STRINGTOTIMESTAMP(event_time_utc, '{_TIMESTAMP_FORMAT}') / {ms}) * {ms},
              entity, metric;
        """)

    def _produce_with_backpressure(self, value: bytes, callback) -> None:
        """Enqueue one row, waiting out a full producer queue instead of failing.

        A full local queue means the producer is ahead of the broker -- ordinary
        backpressure, not an error. librdkafka signals it by raising BufferError
        from produce(); the remedy is to serve delivery callbacks (poll) until space
        frees, then retry the same row. Without this, one over-long batch turns a
        diagnostic cell into a blocked cell and takes the rest of the lane's
        phases with it. The bound is time, not attempts, so a genuinely wedged
        broker still surfaces as an error rather than hanging forever.
        """
        assert self._producer is not None
        deadline = time.perf_counter() + float(
            self.options.get("produce_backpressure_timeout_s", 300))
        while True:
            try:
                self._producer.produce(self._topic, value=value, callback=callback)
                break
            except BufferError:
                if time.perf_counter() >= deadline:
                    raise RuntimeError(
                        "Kafka producer queue stayed full for "
                        f"{self.options.get('produce_backpressure_timeout_s', 300)}s "
                        f"({len(self._producer)} messages pending); the broker is "
                        "not draining."
                    ) from None
                # Blocking poll: serves delivery reports and frees queue slots.
                self._producer.poll(1.0)
        self._producer.poll(0)

    def ingest_log(self, df: pd.DataFrame) -> IngestStats:
        assert self._producer is not None
        n = len(df)
        delivered = [0]
        errors: List[str] = []

        def _cb(err, _msg):
            if err is not None:
                errors.append(str(err))
            else:
                delivered[0] += 1

        t0 = time.perf_counter()
        for rec in df.itertuples(index=False):
            sup = rec.supersedes
            payload = {
                "event_time_utc": pd.Timestamp(rec.event_time_utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "processing_order": int(rec.processing_order),
                "entity": str(rec.entity),
                "metric": str(rec.metric),
                "value": float(rec.value),
                "dedupe_id": str(rec.dedupe_id),
                # null on base rows; set only on D7 restatements
                "supersedes": None if sup is None or (isinstance(sup, float) and pd.isna(sup)) else str(sup),
            }
            self._produce_with_backpressure(
                json.dumps(payload).encode(), _cb)
        self._producer.flush(self.options.get("flush_timeout_s", 120))
        seconds = time.perf_counter() - t0
        if errors:
            raise RuntimeError(f"Kafka delivery errors: {len(errors)}; first: {errors[:3]}")
        if delivered[0] != n:
            raise RuntimeError(f"Kafka acked {delivered[0]} of {n} produced rows.")
        # Let the ksqlDB tables materialize the produced rows.
        time.sleep(self.options.get("materialize_wait_s", 3))
        return IngestStats(rows_sent=n, seconds=seconds)

    # --- read -------------------------------------------------------------
    def read_view(self, grain: str) -> pd.DataFrame:
        self._check_grain(grain)
        # Read-existing path (D5): if the aggregate already exists -- a persistent
        # query that resumed from the durable Kafka changelog after a restart --
        # read it as-is; only create it when absent (the normal post-setup flow).
        created = False
        if not self._view_exists(grain):
            self._create_view(grain)
            time.sleep(self.options.get("materialize_wait_s", 3))
            created = True
        # Only a view created by this call may be recreated. On the D5 path the
        # existing view holds recovered state, and recreating it would erase the
        # very thing D5 measures.
        rows = self._pull_stable(
            f"SELECT * FROM {self._view[grain]};",
            restart_view=(lambda: self._restart_view(grain)) if created else None,
            view_query_prefix=f"CTAS_{self._view[grain].upper()}_",
        )
        if not rows:
            return empty_view()
        df = pd.DataFrame(rows)
        # ksqlDB lower-cases nothing but returns declared names; normalize.
        df = df.rename(columns={c: c.lower() for c in df.columns})
        out = pd.DataFrame({
            "bucket": canonical_bucket(
                pd.to_datetime(df["bucket_ms"].astype("int64"), unit="ms", utc=True), grain),
            "entity": df["entity"], "metric": df["metric"],
            "count": df["cnt"], "sum": df["sum_v"], "min": df["min_v"],
            "max": df["max_v"], "mean": df["mean_v"],
            "latest_value": df["latest_value"],
            "latest_event_time": canonical_event_time(df["latest_event_time"]),
        })
        return coerce_view(out, grain)

    # --- D8: hopping window --------------------------------------------------
    #: ksqlDB has a real hopping primitive that windows by EVENT time here,
    #: because the stream declares TIMESTAMP='event_time_utc'.
    hopping_mode = "native_hop"

    def setup_hopping(self, *, size_n: int = 2, hop_n: int = 1) -> None:
        """Build the streams, then let read_hopping create the windowed table.

        ksqlDB creates its aggregate lazily on first read (the D5 read-existing
        path depends on that), so the flag is what read_hopping keys off rather
        than a view built here.
        """
        self._hopping = True
        self.setup(variant="dedup")

    def read_hopping(self, grain: str) -> pd.DataFrame:
        self._check_grain(grain)
        if not self._view_exists(grain):
            self._create_hopping_view(grain)
            time.sleep(self.options.get("materialize_wait_s", 3))
            created = True
        else:
            created = False
        rows = self._pull_stable(
            f"SELECT * FROM {self._view[grain]};",
            restart_view=(lambda: self._restart_view(grain)) if created else None,
            view_query_prefix=f"CTAS_{self._view[grain].upper()}_",
        )
        if not rows:
            return empty_view()
        df = pd.DataFrame(rows).rename(columns={c: c.lower() for c in rows[0]})
        # The window label lives in the KEY as WINDOWSTART, not in a projected
        # column: a hopping aggregate has no arithmetic bucket to select.
        if "windowstart" not in df.columns:
            raise RuntimeError(
                "ksqlDB returned no WINDOWSTART for the hopping table; the "
                f"columns were {sorted(df.columns)}")
        out = pd.DataFrame({
            "bucket": canonical_bucket(
                pd.to_datetime(df["windowstart"].astype("int64"), unit="ms",
                               utc=True), grain),
            "entity": df["entity"], "metric": df["metric"],
            "count": df["cnt"], "sum": df["sum_v"], "min": df["min_v"],
            "max": df["max_v"], "mean": df["mean_v"],
            "latest_value": df["latest_value"],
            "latest_event_time": canonical_event_time(df["latest_event_time"]),
        })
        return coerce_view(out, grain)

    def _query_consumer_groups(
        self, *, deadline: float | None = None,
    ) -> Dict[str, str]:
        """Map every current persistent query ID to its Kafka consumer group."""
        def request_timeout() -> float | None:
            if deadline is None:
                return None
            remaining = deadline - time.time()
            if remaining <= 0:
                raise RuntimeError(
                    "ksqlDB query discovery reached its deadline")
            return min(
                float(self.options.get("lag_http_timeout_s", 30)),
                max(0.001, remaining),
            )

        payload = self._post(
            "ksql", "SHOW QUERIES;", timeout_s=request_timeout()).json()
        query_ids: List[str] = []
        for item in payload if isinstance(payload, list) else [payload]:
            if not isinstance(item, dict):
                continue
            for query in item.get("queries", []) or []:
                if not isinstance(query, dict):
                    continue
                if str(query.get("queryType", "PERSISTENT")).upper() == "PUSH":
                    continue
                query_id = query.get("id") or query.get("queryId")
                if query_id:
                    query_ids.append(str(query_id))
        if not query_ids:
            raise _QueryStarting(
                "ksqlDB reported no persistent queries for the materialized view")

        groups: Dict[str, str] = {}
        for query_id in sorted(set(query_ids)):
            explained = self._post(
                "ksql", f"EXPLAIN {query_id};",
                timeout_s=request_timeout()).json()
            group_id = None
            for item in explained if isinstance(explained, list) else [explained]:
                if not isinstance(item, dict):
                    continue
                description = item.get("queryDescription")
                if isinstance(description, dict):
                    group_id = description.get("consumerGroupId")
                    state = str(description.get("state") or "").upper()
                    if state == "ERROR":
                        errors = description.get("queryErrors") or []
                        last = errors[-1] if errors else {}
                        detail = (last.get("errorMessage") if isinstance(last, dict)
                                  else str(last)) or "no error message recorded"
                        raise _QueryFailed(
                            f"ksqlDB query {query_id} is in ERROR: {detail}",
                            query_ids=(query_id,))
                if group_id is None:
                    group_id = item.get("consumerGroupId")
                if group_id:
                    break
            if not group_id:
                raise RuntimeError(
                    f"ksqlDB EXPLAIN {query_id} did not report consumerGroupId")
            groups[query_id] = str(group_id)
        return groups

    def _persistent_query_lags(self, *, deadline: float) -> Dict[str, int]:
        """Return end-offset lag for every partition read by every query.

        Committed offsets are the next records each Kafka Streams query will
        consume; latest offsets are the partitions' high watermarks. Their
        difference is therefore the unprocessed-record count.
        """
        if self._admin is None:
            raise RuntimeError("ksqlDB lag barrier requires connect() first")

        from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
        from confluent_kafka.admin import OffsetSpec

        groups_by_query = self._query_consumer_groups(deadline=deadline)
        queries_by_group: Dict[str, List[str]] = {}
        for query_id, group_id in groups_by_query.items():
            queries_by_group.setdefault(group_id, []).append(query_id)

        committed = []
        for group_id, query_ids in sorted(queries_by_group.items()):
            remaining = deadline - time.time()
            if remaining <= 0:
                raise RuntimeError("ksqlDB lag barrier reached its deadline")
            request_timeout = min(
                float(self.options.get("lag_admin_timeout_s", 30)),
                max(0.001, remaining),
            )
            # confluent-kafka 2.14 accepts only one group per invocation.
            # Kafka Streams can use transactions, so request stable committed
            # offsets rather than offsets from an open transaction.
            group_futures = self._admin.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(group_id)],
                require_stable=True,
                request_timeout=request_timeout,
            )
            future = group_futures.get(group_id)
            if future is None:
                raise RuntimeError(
                    f"Kafka returned no offset future for consumer group {group_id}")
            result = future.result(timeout=max(0.001, deadline - time.time()))
            partitions = result.topic_partitions or []
            if not partitions:
                raise _QueryStarting(
                    f"consumer group {group_id} has no committed partitions yet",
                    query_ids=tuple(sorted(query_ids)))
            for partition in partitions:
                if partition.error is not None:
                    raise RuntimeError(
                        f"cannot read {group_id} offset for "
                        f"{partition.topic}[{partition.partition}]: "
                        f"{partition.error}")
                offset = int(partition.offset)
                if offset < 0:
                    raise _QueryStarting(
                        f"consumer group {group_id} has no committed offset for "
                        f"{partition.topic}[{partition.partition}]",
                        query_ids=tuple(sorted(query_ids)))
                committed.append((
                    group_id, tuple(sorted(query_ids)), partition.topic,
                    int(partition.partition), offset,
                ))

        remaining = deadline - time.time()
        if remaining <= 0:
            raise RuntimeError("ksqlDB lag barrier reached its deadline")
        request_timeout = min(
            float(self.options.get("lag_admin_timeout_s", 30)),
            max(0.001, remaining),
        )
        latest_requests = {
            TopicPartition(topic, partition): OffsetSpec.latest()
            for _group, _queries, topic, partition, _offset in committed
        }
        latest_futures = self._admin.list_offsets(
            latest_requests, request_timeout=request_timeout)
        latest = {}
        for partition, future in latest_futures.items():
            info = future.result(timeout=max(0.001, deadline - time.time()))
            offset = int(info.offset)
            if offset < 0:
                raise RuntimeError(
                    f"Kafka returned no latest offset for "
                    f"{partition.topic}[{partition.partition}]")
            latest[(partition.topic, int(partition.partition))] = offset

        lags: Dict[str, int] = {}
        for group_id, query_ids, topic, partition, offset in committed:
            key = (topic, partition)
            if key not in latest:
                raise RuntimeError(
                    f"Kafka returned no latest-offset future for "
                    f"{topic}[{partition}]")
            label = (
                f"{','.join(query_ids)}/{group_id}/{topic}[{partition}]")
            lag = latest[key] - offset
            if lag < 0:
                raise RuntimeError(
                    f"consumer group {group_id} committed offset {offset} "
                    f"is ahead of latest offset {latest[key]} for "
                    f"{topic}[{partition}]")
            lags[label] = lag
        return lags

    def _restart_view(self, grain: str) -> None:
        """Recreate one aggregate view whose query never started consuming.

        The view is a pure function of its source topic, which is complete by
        the time a read begins, so a fresh CREATE TABLE ... AS SELECT computes
        the same table from offset zero. Only the aggregate is recreated: an
        upstream CREATE STREAM ... AS SELECT would append its output a second
        time, so upstream queries are never restarted here.
        """
        view = self._view[grain]
        prefix = f"CTAS_{view.upper()}_"
        deadline = time.time() + float(self.options.get("terminate_timeout_s", 60))
        while time.time() < deadline:
            stale = [q for q in self._list_persistent_queries()
                     if q.upper().startswith(prefix)]
            if not stale:
                break
            for qid in stale:
                try:
                    self._post("ksql", f"TERMINATE {qid};")
                except Exception:
                    pass
            time.sleep(1.0)
        self._drop_retry(f"DROP TABLE IF EXISTS {view} DELETE TOPIC;")
        self._create_view(grain)
        time.sleep(self.options.get("materialize_wait_s", 3))

    def _query_states(self) -> str:
        """One line naming every persistent query and its state, for errors."""
        try:
            payload = self._post("ksql", "SHOW QUERIES;", timeout_s=30).json()
            states = []
            for item in payload if isinstance(payload, list) else [payload]:
                for q in (item.get("queries", []) if isinstance(item, dict) else []):
                    if str(q.get("queryType", "PERSISTENT")).upper() != "PUSH":
                        states.append(f"{q.get('id')}={q.get('state')}")
            return ", ".join(states) or "no persistent queries"
        except Exception as exc:  # the report must never mask the real error
            return f"query states unavailable ({exc})"

    def _pull_stable(
        self, sql: str, *, restart_view=None, view_query_prefix: str = "",
    ) -> List[Dict[str, Any]]:
        """A ksqlDB materialized aggregate is eventually consistent: a pull query
        can return a valid but partial snapshot while Kafka Streams is behind.
        Require every persistent query partition to report zero consumer lag on
        two consecutive polls, and require the corresponding complete snapshots
        to match. The second zero-lag poll closes races in a multi-query topology
        where an upstream query can append downstream work after the first poll.
        """
        # A fixed budget does not survive contact with the 1M-row phases. The
        # barrier's cost grows with the TABLE (each poll pulls every row and
        # serialises it for the snapshot comparison) while the budget stayed at
        # 120s, so on the two largest domains it timed out with every consumer
        # lag already at zero and ~990k rows in hand -- the query was fine, the
        # clock was not. Two changes fix that without weakening the barrier:
        #   1. the budget is tunable from the environment, so a run can raise it
        #      without editing pinned source; and
        #   2. the deadline is EXTENDED whenever a complete zero-lag snapshot is
        #      actually pulled, i.e. whenever the barrier is making real progress,
        #      up to a hard ceiling.
        # A query that is genuinely broken -- an unreachable broker, a failed
        # EXPLAIN -- makes no progress, never extends, and still fails at the
        # base timeout. Merely-not-yet-committed is handled separately below.
        timeout = float(self.options.get(
            "read_stable_timeout_s",
            os.environ.get("EQUITABLE_KSQL_READ_STABLE_TIMEOUT_S", 120)))
        # The ceiling also bounds the startup budget below, because both are
        # clamped to hard_deadline. Raising the startup window without raising
        # this one silently does nothing, so the two defaults move together.
        max_timeout = float(self.options.get(
            "read_stable_max_timeout_s",
            os.environ.get("EQUITABLE_KSQL_READ_STABLE_MAX_TIMEOUT_S", 3600)))
        # Coming up is not the same as being stuck. The extension above keys off
        # a completed zero-lag pull, so it never fires before the first offset
        # commit -- and a query that has not committed yet reports no partitions
        # at all, which the loop below treats as an error and retries until the
        # base budget runs out. On a host running seven engines at once that gap
        # is rebalance and RocksDB startup, not data volume, so it exceeded 120s
        # on a 10k-row cell as readily as on a 1M-row one. Give the pre-commit
        # window its own budget: generous enough to absorb a loaded startup,
        # separate from the ceiling, and still bounded so a query that never
        # starts fails rather than hanging.
        # On a loaded host a 1M-row D7 cell can wait well over 900 s for the
        # consumer group to commit its first offsets, so the default is sized
        # for a host running all seven engines rather than an idle one.
        startup_timeout = float(self.options.get(
            "read_startup_timeout_s",
            os.environ.get("EQUITABLE_KSQL_READ_STARTUP_TIMEOUT_S", 2400)))
        # A new aggregate query that has not committed after this long is hung,
        # not slow. Kafka Streams commits within its commit interval (seconds)
        # of processing its first record, and a view created for this read has
        # no changelog to restore. A view that sits at "no committed
        # partitions" for the whole budget typically finishes in minutes when
        # recreated, so recreating it inside the cell does that re-run without
        # costing a lane retry.
        restart_after = float(self.options.get(
            "read_restart_after_s",
            os.environ.get("EQUITABLE_KSQL_READ_RESTART_AFTER_S", 600)))
        max_restarts = int(self.options.get(
            "read_max_view_restarts",
            os.environ.get("EQUITABLE_KSQL_READ_MAX_VIEW_RESTARTS", 2)))
        started = time.time()
        stalled: Dict[tuple, float] = {}
        hard_deadline = started + max(timeout, max_timeout)
        startup_deadline = started + max(timeout, startup_timeout)
        deadline = started + timeout
        interval = float(self.options.get("read_poll_interval_s", 3))
        last_snapshot = None
        last_lag_keys = None
        zero_lag_polls = 0
        last_lags: Dict[str, int] = {}
        last_lag_error = None
        rows: List[Dict[str, Any]] = []
        while time.time() < deadline:
            try:
                lags = self._persistent_query_lags(deadline=deadline)
            except (_QueryFailed, _QueryStarting) as exc:
                last_lag_error = exc
                zero_lag_polls = 0
                last_lag_keys = None
                last_snapshot = None
                now = time.time()
                stalled_since = stalled.setdefault(exc.query_ids, now)
                stalled_for = now - stalled_since
                is_view = bool(view_query_prefix) and any(
                    q.upper().startswith(view_query_prefix) for q in exc.query_ids)
                if not exc.query_ids and stalled_for >= restart_after:
                    # No persistent query at all, so nothing will ever commit
                    # and there is no query id to restart. ksqlDB replays its
                    # command topic within seconds of a restart, so this is not
                    # a slow start. A D5 cell reading a table whose queries
                    # the harness has terminated would otherwise spend the
                    # whole startup budget waiting for a commit that cannot
                    # come.
                    raise RuntimeError(
                        f"ksqlDB has run no persistent query for "
                        f"{stalled_for:.0f}s, so nothing maintains "
                        f"{sql.strip()[:120]!r}: {exc}"
                    ) from exc
                if exc.query_ids and stalled_for >= restart_after:
                    if (is_view and restart_view is not None
                            and len(self.view_restarts) < max_restarts):
                        state = ("in ERROR" if isinstance(exc, _QueryFailed)
                                 else "without a first commit")
                        self._record_restart(
                            exc, started, f"{state} for {stalled_for:.0f}s")
                        restart_view()
                        stalled.clear()
                        startup_deadline = min(
                            hard_deadline, time.time() + max(timeout, startup_timeout))
                        deadline = min(hard_deadline, max(deadline, startup_deadline))
                        continue
                    if not is_view or isinstance(exc, _QueryFailed):
                        # An upstream query cannot be recreated without writing
                        # its output twice, and a query still in ERROR after the
                        # grace period is not recovering. Fail now with the
                        # evidence instead of spending the rest of the budget.
                        raise RuntimeError(
                            f"ksqlDB query stuck for {stalled_for:.0f}s: {exc}; "
                            f"view restarts {len(self.view_restarts)}/"
                            f"{max_restarts}; queries: {self._query_states()}"
                        ) from exc
                # Still starting: hold the barrier open on the startup budget
                # rather than spending the steady-state one.
                deadline = min(
                    hard_deadline,
                    max(deadline, min(startup_deadline,
                                      time.time() + timeout)))
            except Exception as exc:
                last_lag_error = exc
                zero_lag_polls = 0
                last_lag_keys = None
                last_snapshot = None
            else:
                last_lag_error = None
                stalled.clear()
                last_lags = lags
                if lags and all(lag == 0 for lag in lags.values()):
                    lag_keys = tuple(sorted(lags))
                    zero_lag_polls = (
                        zero_lag_polls + 1
                        if lag_keys == last_lag_keys else 1
                    )
                    last_lag_keys = lag_keys
                    # Zero lag is progress, so extend before the pull rather than
                    # after it: the pull is the expensive step (it retrieves and
                    # serialises every row), and a nearly spent budget fails a
                    # healthy million-row table.
                    deadline = min(hard_deadline, max(deadline, time.time() + timeout))
                    rows = self._pull_ready(sql, deadline=deadline)
                    # Pull-query row order is not a contract, so compare a
                    # canonical multiset representation.
                    snapshot = tuple(sorted(
                        json.dumps(
                            row, sort_keys=True, separators=(",", ":"),
                            default=str)
                        for row in rows
                    ))
                    if (zero_lag_polls >= 2 and rows
                            and snapshot == last_snapshot):
                        return rows
                    last_snapshot = snapshot
                else:
                    zero_lag_polls = 0
                    last_lag_keys = None
                    last_snapshot = None
            time.sleep(min(interval, max(0.0, deadline - time.time())))

        if last_lag_error is not None:
            lag_detail = f"last lag error: {last_lag_error}"
        elif last_lags:
            lag_detail = (
                "last consumer lags: " + json.dumps(last_lags, sort_keys=True))
        else:
            lag_detail = "no consumer lag was observed"
        raise RuntimeError(
            f"ksqlDB queries did not reach a stable zero-lag snapshot within "
            f"{time.time() - started:.0f}s (base {timeout:g}s, startup "
            f"{max(timeout, startup_timeout):g}s, ceiling "
            f"{max(timeout, max_timeout):g}s); {lag_detail}; last snapshot "
            f"contained {len(rows)} rows; view restarts "
            f"{len(self.view_restarts)}/{max_restarts}; queries: "
            f"{self._query_states()}")

    def _record_restart(self, exc: Exception, started: float, reason: str) -> None:
        record = {
            "queries": list(getattr(exc, "query_ids", ())),
            "reason": reason,
            "error": str(exc),
            "seconds_into_read": round(time.time() - started, 1),
        }
        self.view_restarts.append(record)
        print(f"  ksqlDB: recreating view ({reason}): {exc} "
              f"[restart {len(self.view_restarts)}]", file=sys.stderr, flush=True)

    @staticmethod
    def _transient_pull_error(exc: Exception) -> bool:
        """Whether retrying this read-only pull query is safe and useful."""
        import requests

        message = str(exc).lower()
        return (
            isinstance(exc, requests.exceptions.RequestException)
            or "40001" in message
            or "may be restarting" in message
            or ("ksqldb 5" in message and "on /query" in message)
        )

    def _pull_ready(
        self, sql: str, *, deadline: float | None = None,
    ) -> List[Dict[str, Any]]:
        """Retry transient failures while a materialized table becomes readable.

        Pull queries are read-only, so retrying a socket timeout or HTTP 5xx
        cannot duplicate input or recreate state. Each request has a short bound
        and all attempts share the caller's stable-read deadline.
        """
        if deadline is None:
            deadline = time.time() + float(
                self.options.get("pull_ready_timeout_s", 120))
        interval = float(self.options.get("pull_retry_interval_s", 2))
        request_timeout = float(self.options.get("pull_http_timeout_s", 30))
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                raise RuntimeError(
                    "ksqlDB pull query did not become ready before its deadline")
            try:
                return self._pull(
                    sql, timeout_s=min(request_timeout, max(1.0, remaining)))
            except Exception as exc:
                if (not self._transient_pull_error(exc)
                        or time.time() >= deadline):
                    raise
                time.sleep(min(interval, max(0.0, deadline - time.time())))

    def _pull(
        self, sql: str, *, timeout_s: float | None = None,
    ) -> List[Dict[str, Any]]:
        """Run a pull query; return a list of {column_name: value} dicts.

        Handles both ksqlDB `/query` response shapes:
          * newer (>=0.10): a header object {"queryId","columnNames","columnTypes"}
            followed by bare JSON arrays, one per row;
          * legacy: a JSON array of {"header":{"schema":...}} / {"row":{"columns":...}}.
        A parser that expects only the legacy shape drops every row on newer
        servers (the header lacks a "header" key and rows are lists, not dicts),
        yielding an empty view.
        """
        r = self._post("query", sql, timeout_s=timeout_s)
        lines = [json.loads(x) for x in r.text.strip().splitlines() if x.strip()]
        col_names: List[str] = []
        rows: List[Dict[str, Any]] = []
        for item in lines:
            if isinstance(item, dict) and "columnNames" in item:   # newer header
                col_names = list(item["columnNames"])
            elif isinstance(item, dict) and "header" in item:      # legacy header
                schema = item["header"].get("schema", "")
                # schema looks like "`BUCKET_MS` BIGINT, `ENTITY` STRING, ..."
                col_names = [seg.strip().split("`")[1] for seg in schema.split(",") if "`" in seg]
            elif isinstance(item, dict) and "row" in item:         # legacy row
                cols = item["row"].get("columns", [])
                rows.append({col_names[i]: cols[i] for i in range(min(len(col_names), len(cols)))})
            elif isinstance(item, list):                           # newer bare-array row
                rows.append({col_names[i]: item[i] for i in range(min(len(col_names), len(item)))})
        return rows

    def detach(self) -> None:
        """Drop the producer only. Unlike close(), leave the persistent queries
        running: they are what maintains the table, and a caller that is about
        to restart the container is measuring whether that state survives."""
        if self._producer is not None:
            self._producer.flush(5)
            self._producer = None
        self._admin = None

    def close(self) -> None:
        try:
            self._terminate_all_queries()
        except Exception:
            pass
        if self._producer is not None:
            self._producer.flush(5)
            self._producer = None
        self._admin = None
