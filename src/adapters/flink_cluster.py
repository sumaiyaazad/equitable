"""Apache Flink adapter for a checkpointed session cluster (Flink SQL Gateway).

This is the scored Flink deployment. The in-process PyFlink adapter
(src/adapters/flink.py) ran every cell in a MiniCluster with parallelism 1 and no
durable state, which is why D5 was not applicable to Flink and why a million-row
cell took hours. Here the same view SQL (`_view_sql`, shared with that adapter so
the query does not change with the deployment) runs as a long-lived streaming job
on the cluster in docker/streaming-engines.yml:

    Kafka input topic --> JobManager/TaskManager job --> upsert result topics
                          (RocksDB state, exactly-once checkpoints every 5 s,
                           retained on the flink_checkpoints volume)

    setup()       new SQL Gateway session; source, sinks, and one STATEMENT SET
                  job that maintains the hourly and daily views
    ingest_log()  produce the rows to the single-partition input topic, then block
                  until the job has checkpointed past the last one
    read_view()   fold the compacted result topic (latest record per key,
                  tombstone deletes) into the maintained table
    checkpoint()  the same barrier: a checkpoint that covers every ingested row
    resume()      after the TaskManager is killed and restarted, wait until the
                  JobManager has restored the job from a retained checkpoint and
                  completed a new checkpoint on top of it

COMPLETION BARRIER. The Kafka source commits its offsets to the consumer group
only when a checkpoint completes. A committed offset equal to the input topic's
end offset therefore means a completed checkpoint whose state includes every
input row, and the upsert-kafka sink flushes before that checkpoint completes.
Reading the result topic after the barrier returns exactly the maintained table.

D5 VALIDITY. The result topic lives in Kafka and survives a TaskManager crash
whether or not Flink restored anything, so reading it back after a restart proves
nothing on its own. D5 for this adapter is meaningful only with the resume step:
rows ingested after recovery must combine with the restored aggregate state.
resume() also requires the job's restore counter to show a restore from a
retained checkpoint, so an unrestored job cannot pass as recovered.

Concurrent lanes share one cluster, so every job, topic, and consumer group
carries a per-process suffix, and setup() cancels jobs left by dead processes.
"""
from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import EngineAdapter, IngestStats, canonical_event_time, coerce_view, empty_view
from .flink import (_FLOOR, _SRC, _VIEW_FIELDS, _dedup_cte, _hopping_sql,
                    _view_sql)

_GRAINS = ("hourly", "daily")
_JOB_PREFIX = "equitable-flink-"

#: Session settings each job is planned with. The gateway also carries these as
#: cluster defaults (docker/streaming-engines.yml); stating them per session
#: makes a job's durability independent of how the gateway was started, which
#: matters because a gateway without them ran every job with checkpointing off.
SESSION_SETTINGS = {
    "execution.runtime-mode": "streaming",
    "parallelism.default": "2",
    "state.backend.type": "rocksdb",
    "execution.checkpointing.dir": "file:///checkpoints",
    "execution.checkpointing.interval": "5 s",
    "execution.checkpointing.mode": "EXACTLY_ONCE",
    "execution.checkpointing.externalized-checkpoint-retention": "RETAIN_ON_CANCELLATION",
    "restart-strategy.type": "fixed-delay",
    "restart-strategy.fixed-delay.attempts": "100",
    "restart-strategy.fixed-delay.delay": "5 s",
}

#: Selection order per (order, last). The Flink spelling of
#: src/adapters/_sql_views.py SHAPE_ORDER_SQL; the two must stay in step.
_SHAPE_ORDER = {
    ("contract", True): "event_time_utc DESC, dedupe_id DESC",
    ("contract", False): "event_time_utc ASC, dedupe_id ASC",
    ("arrival", True): "processing_order DESC",
    ("arrival", False): "processing_order ASC",
}
_TOPK_FIELDS = ["bucket", "metric", "rank", "entity", "rank_value"]


def shape_sql(grain: str, shape: str, order: str, k: int = 10) -> str:
    """One member of the order-sensitive query family, in Flink SQL.

    Mirrors shape_view_sql in src/adapters/_sql_views.py: the order-insensitive
    aggregate is shared, and only the chosen row differs between the two arms.
    """
    last = shape != "S2_first"
    base = f"""
WITH {_dedup_cte()},
b AS (
  SELECT FLOOR(event_time_utc TO {_FLOOR[grain]}) AS bucket,
         entity, metric, `value`, event_time_utc, processing_order, dedupe_id
  FROM src
),
agg AS (
  SELECT bucket, entity, metric,
         COUNT(*) AS `count`, SUM(`value`) AS `sum`,
         MIN(`value`) AS `min`, MAX(`value`) AS `max`, AVG(`value`) AS `mean`
  FROM b GROUP BY bucket, entity, metric
),
chosen AS (
  SELECT bucket, entity, metric, `value` AS latest_value,
         event_time_utc AS latest_event_time
  FROM (
    SELECT b.*, ROW_NUMBER() OVER (
             PARTITION BY bucket, entity, metric
             ORDER BY {_SHAPE_ORDER[(order, last)]}) AS rk
    FROM b
  ) WHERE rk = 1
)"""
    bucket_projection = """
SELECT a.bucket, a.entity, a.metric, a.`count`, a.`sum`, a.`min`, a.`max`, a.`mean`,
       c.latest_value, c.latest_event_time
FROM agg a JOIN chosen c
  ON a.bucket = c.bucket AND a.entity = c.entity AND a.metric = c.metric"""
    if shape in ("S1_latest", "S2_first"):
        return base + bucket_projection
    if shape == "S3_rank_by_latest":
        return base + f"""
SELECT bucket, metric, `rank`, entity, rank_value FROM (
  SELECT bucket, metric, entity, latest_value AS rank_value,
         ROW_NUMBER() OVER (PARTITION BY bucket, metric
                            ORDER BY latest_value DESC, entity ASC) AS `rank`
  FROM chosen
) WHERE `rank` <= {int(k)}"""
    if shape == "S5_filter_on_latest":
        return base + """,
thresh AS (SELECT metric, AVG(`value`) AS t FROM b GROUP BY metric)
SELECT a.bucket, a.entity, a.metric, a.`count`, a.`sum`, a.`min`, a.`max`, a.`mean`,
       c.latest_value, c.latest_event_time
FROM agg a
JOIN chosen c ON a.bucket = c.bucket AND a.entity = c.entity AND a.metric = c.metric
JOIN thresh th ON th.metric = a.metric
WHERE c.latest_value >= th.t"""
    raise ValueError(f"no Flink SQL for shape {shape!r}")


#: Applied to the order-sensitive query family only. Those shapes join a
#: per-metric aggregate to every key, so one input row re-emits every matching
#: key: 50,000 rows wrote 5.7 GB of changelog per cell and filled the host.
#: Mini-batch coalesces that to one change per key per batch. It changes how
#: often intermediate results are emitted, never the final table, and it costs
#: about 90 s per cell, which is why the scored diagnostics keep Flink's default
#: per-record streaming instead.
SHAPE_SETTINGS = {
    "table.exec.mini-batch.enabled": "true",
    "table.exec.mini-batch.allow-latency": "10 s",
    "table.exec.mini-batch.size": "200000",
}

_SOURCE_COLUMNS = ("event_time_utc", "entity", "metric", "value",
                   "processing_order", "dedupe_id", "supersedes")


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class FlinkClusterAdapter(EngineAdapter):
    name = "flink"
    supported_variants = ("dedup", "naive", "restate", "arrival_latest")
    #: Every member of the order-sensitive family: Flink's streaming planner
    #: maintains ROW_NUMBER selections, Top-N over an updating input, and joins.
    supported_shapes = ("S1_latest", "S2_first", "S3_rank_by_latest",
                        "S5_filter_on_latest")

    def __init__(self, **options: Any) -> None:
        super().__init__(**options)
        self._gateway = str(options.get("gateway_url", "http://localhost:8083")).rstrip("/")
        self._rest = str(options.get("rest_url", "http://localhost:8081")).rstrip("/")
        self._broker = str(options.get("kafka", "localhost:19092"))
        self._internal_broker = str(options.get("kafka_internal", "kafka:9092"))
        base = str(
            options.get("run_suffix")
            or os.environ.get("EQUITABLE_RUN_SUFFIX")
            or os.environ.get("KSQL_RUN_SUFFIX")
            or "adhoc"
        ).lower()
        base = re.sub(r"[^a-z0-9_]", "_", base)[:60]
        # The pid keeps concurrent lanes apart and lets setup() recognise the
        # jobs of processes that no longer exist. D5 reconnects in the same
        # process, so the names it derives match the ones setup() created.
        self._pid = int(options.get("pid", os.getpid()))
        self._prefix = f"vsflink_{base}_{self._pid}"
        self._job_name = f"{_JOB_PREFIX}{base}-{self._pid}"
        self._topics = {"input": f"{self._prefix}_in",
                        "hourly": f"{self._prefix}_hourly",
                        "daily": f"{self._prefix}_daily"}
        self._group = f"{self._prefix}_source"
        self._session: Optional[str] = None
        self._schema = "bucket"
        #: True only for the instance whose setup() created the job. D5 reconnects
        #: with fresh instances and closes them while the job must keep running.
        self._owns_job = False
        self._admin = None
        self._producer = None
        self._barrier_timeout = float(options.get("barrier_timeout_s", 3600))
        self._poll = float(options.get("poll_interval_s", 1.0))

    # --- HTTP helpers -------------------------------------------------------
    @staticmethod
    def _requests():
        import requests
        return requests

    def _rest_get(self, path: str) -> Any:
        r = self._requests().get(f"{self._rest}{path}", timeout=30)
        r.raise_for_status()
        return r.json()

    def _sql(self, statement: str, timeout_s: float = 300) -> Dict[str, Any]:
        """Run one statement in the adapter's gateway session; return its result."""
        requests = self._requests()
        assert self._session is not None, "connect() first"
        r = requests.post(f"{self._gateway}/v2/sessions/{self._session}/statements",
                          json={"statement": statement}, timeout=60)
        body = r.json()
        if r.status_code >= 400 or "operationHandle" not in body:
            raise RuntimeError(f"Flink SQL Gateway rejected statement: "
                               f"{' '.join(body.get('errors', [str(body)]))[-2000:]}")
        handle = body["operationHandle"]
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            r = requests.get(
                f"{self._gateway}/v2/sessions/{self._session}/operations/{handle}"
                f"/result/0?rowFormat=JSON", timeout=60)
            body = r.json()
            if r.status_code >= 400 or "errors" in body:
                raise RuntimeError(
                    f"Flink statement failed: "
                    f"{' '.join(body.get('errors', [str(body)]))[-3000:]}")
            if body.get("resultType") in ("PAYLOAD", "EOS"):
                return body
            time.sleep(0.2)
        raise RuntimeError(f"Flink statement did not finish within {timeout_s:g}s")

    # --- Kafka helpers ------------------------------------------------------
    def _kafka(self) -> None:
        if self._admin is None:
            from confluent_kafka import Producer
            from confluent_kafka.admin import AdminClient
            self._admin = AdminClient({"bootstrap.servers": self._broker})
            self._producer = Producer({
                "bootstrap.servers": self._broker,
                "linger.ms": 20,
                "enable.idempotence": True,
                "queue.buffering.max.messages": 1_000_000,
            })

    def _end_offset(self, topic: str) -> int:
        from confluent_kafka import TopicPartition
        from confluent_kafka.admin import OffsetSpec
        futures = self._admin.list_offsets(
            {TopicPartition(topic, 0): OffsetSpec.latest()}, request_timeout=30)
        return int(next(iter(futures.values())).result(timeout=60).offset)

    def _committed_offset(self) -> Optional[int]:
        from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
        futures = self._admin.list_consumer_group_offsets(
            [ConsumerGroupTopicPartitions(self._group,
                                          [TopicPartition(self._topics["input"], 0)])],
            require_stable=True, request_timeout=30)
        result = futures[self._group].result(timeout=60)
        for part in result.topic_partitions or []:
            if part.error is None and int(part.offset) >= 0:
                return int(part.offset)
        return None

    def _delete_topics(self, topics: List[str]) -> None:
        existing = set(self._admin.list_topics(timeout=30).topics)
        doomed = [t for t in topics if t in existing]
        if doomed:
            for future in self._admin.delete_topics(doomed, operation_timeout=60).values():
                try:
                    future.result()
                except Exception:
                    pass
        deadline = time.time() + 120
        while time.time() < deadline:
            if not set(doomed) & set(self._admin.list_topics(timeout=30).topics):
                break
            time.sleep(1)
        try:
            for future in self._admin.delete_consumer_groups(
                    [self._group], request_timeout=30).values():
                future.result()
        except Exception:
            pass  # a group that never existed is the common case

    #: Result topics are COMPACTED, so their size follows the number of view keys
    #: rather than the number of updates. A maintained view can emit an update
    #: storm: the S5 family shape joins a per-metric average to every key, so one
    #: input row re-emits every matching key. Writing that changelog uncompacted
    #: put 317 GB in Kafka during one validation stage and filled the host.
    _COMPACTED = {
        "cleanup.policy": "compact",
        "segment.bytes": str(64 * 1024 * 1024),
        "min.cleanable.dirty.ratio": "0.05",
        "max.compaction.lag.ms": "60000",
        "delete.retention.ms": "60000",
        "min.compaction.lag.ms": "0",
    }

    def _create_topics(self) -> None:
        from confluent_kafka.admin import NewTopic
        topics = [
            NewTopic(name, num_partitions=1, replication_factor=1,
                     config=dict(self._COMPACTED) if key != "input" else {})
            for key, name in self._topics.items()
        ]
        for future in self._admin.create_topics(topics, operation_timeout=60).values():
            future.result()

    def _reap_orphan_topics(self) -> None:
        """Delete result topics left behind by benchmark processes that are gone."""
        try:
            names = list(self._admin.list_topics(timeout=30).topics)
        except Exception:
            return
        doomed = []
        for name in names:
            match = re.match(r"^vsflink_.*_(\d+)_(in|hourly|daily)$", name)
            if match and not _pid_alive(int(match.group(1))) and name not in self._topics.values():
                doomed.append(name)
        if doomed:
            self._delete_topics(doomed)

    # --- jobs ---------------------------------------------------------------
    def _jobs(self) -> List[Dict[str, Any]]:
        return self._rest_get("/jobs/overview").get("jobs", [])

    def _own_job(self) -> Optional[Dict[str, Any]]:
        live = [j for j in self._jobs() if j.get("name") == self._job_name
                and j.get("state") not in ("CANCELED", "FAILED", "FINISHED")]
        return max(live, key=lambda j: j.get("start-time", 0)) if live else None

    def _cancel(self, jid: str) -> None:
        requests = self._requests()
        requests.patch(f"{self._rest}/jobs/{jid}?mode=cancel", timeout=30)
        deadline = time.time() + 120
        while time.time() < deadline:
            state = self._rest_get(f"/jobs/{jid}").get("state")
            if state in ("CANCELED", "FAILED", "FINISHED"):
                return
            time.sleep(1)
        raise RuntimeError(f"Flink job {jid} did not cancel within 120s")

    def _reap_orphans(self) -> None:
        """Cancel jobs whose owning benchmark process is gone."""
        for job in self._jobs():
            name = str(job.get("name", ""))
            if not name.startswith(_JOB_PREFIX) or job.get("state") in (
                    "CANCELED", "FAILED", "FINISHED"):
                continue
            match = re.search(r"-(\d+)$", name)
            if name == self._job_name or (match and not _pid_alive(int(match.group(1)))):
                self._cancel(job["jid"])

    # --- lifecycle ----------------------------------------------------------
    def connect(self) -> "FlinkClusterAdapter":
        self._kafka()
        self._open_session()
        return self

    def _open_session(self) -> None:
        """A fresh gateway session, so a relaunch never meets its own tables."""
        self._close_session()
        r = self._requests().post(f"{self._gateway}/v2/sessions",
                                  json={"sessionName": self._job_name}, timeout=60)
        r.raise_for_status()
        self._session = r.json()["sessionHandle"]

    def _declare_tables(self, extra: Optional[Dict[str, str]] = None) -> None:
        settings = dict(SESSION_SETTINGS)
        settings.update(extra or {})
        settings["parallelism.default"] = str(self.options.get(
            "parallelism",
            os.environ.get("EQUITABLE_FLINK_PARALLELISM",
                           SESSION_SETTINGS["parallelism.default"])))
        for key, value in settings.items():
            self._sql(f"SET '{key}' = '{value}'")
        self._sql(f"SET 'pipeline.name' = '{self._job_name}'")
        self._sql(f"""
            CREATE TABLE src (
              event_time_utc TIMESTAMP(6), entity STRING, metric STRING,
              `value` DOUBLE, processing_order BIGINT, dedupe_id STRING,
              supersedes STRING
            ) WITH (
              'connector' = 'kafka', 'topic' = '{self._topics["input"]}',
              'properties.bootstrap.servers' = '{self._internal_broker}',
              'properties.group.id' = '{self._group}',
              'properties.isolation.level' = 'read_committed',
              'scan.startup.mode' = 'earliest-offset',
              'format' = 'json', 'json.timestamp-format.standard' = 'SQL',
              'json.fail-on-missing-field' = 'false'
            )""")
        self._sql(f"CREATE TEMPORARY VIEW {_SRC} AS SELECT * FROM src")

    def _declare_sinks(self, schema: str = "bucket") -> None:
        for grain in _GRAINS:
            if schema == "topk":
                self._sql(f"""
                    CREATE TABLE sink_{grain} (
                      bucket TIMESTAMP(6), metric STRING, `rank` BIGINT,
                      entity STRING, rank_value DOUBLE,
                      PRIMARY KEY (bucket, metric, `rank`) NOT ENFORCED
                    ) WITH (
                      'connector' = 'upsert-kafka', 'topic' = '{self._topics[grain]}',
                      'properties.bootstrap.servers' = '{self._internal_broker}',
                      'key.format' = 'json', 'value.format' = 'json',
                      'key.json.timestamp-format.standard' = 'SQL',
                      'value.json.timestamp-format.standard' = 'SQL'
                    )""")
                continue
            self._sql(f"""
                CREATE TABLE sink_{grain} (
                  bucket TIMESTAMP(6), entity STRING, metric STRING,
                  `count` BIGINT, `sum` DOUBLE, `min` DOUBLE, `max` DOUBLE,
                  `mean` DOUBLE, latest_value DOUBLE, latest_event_time TIMESTAMP(6),
                  PRIMARY KEY (bucket, entity, metric) NOT ENFORCED
                ) WITH (
                  'connector' = 'upsert-kafka', 'topic' = '{self._topics[grain]}',
                  'properties.bootstrap.servers' = '{self._internal_broker}',
                  'key.format' = 'json', 'value.format' = 'json',
                  'key.json.timestamp-format.standard' = 'SQL',
                  'value.json.timestamp-format.standard' = 'SQL'
                )""")

    def setup(self, *, variant: str = "dedup") -> None:
        self._check_variant(variant)
        self._schema = "bucket"
        self._launch({grain: _view_sql(grain, variant) for grain in _GRAINS}, "bucket")

    # --- D8: hopping window ------------------------------------------------
    #: Flink's HOP table function needs a declared time attribute with a
    #: watermark, which the scored table is read without, so the window is a
    #: cross join against hop offsets over the same rows.
    hopping_mode = "crossjoin"

    def setup_hopping(self, *, size_n: int = 2, hop_n: int = 1) -> None:
        self._check_variant("dedup")
        self._schema = "bucket"
        self._launch(
            {grain: _hopping_sql(grain, "dedup", size_n, hop_n)
             for grain in _GRAINS}, "bucket")

    def read_hopping(self, grain: str) -> pd.DataFrame:
        """The hopping table is the bucket table, so read_view's path applies."""
        return self.read_view(grain)

    def setup_shape(self, *, shape: str, order: str) -> None:
        if shape not in self.supported_shapes:
            raise NotImplementedError(f"{self.name} cannot express shape {shape!r}")
        self._check_variant("dedup")
        schema = "topk" if shape == "S3_rank_by_latest" else "bucket"
        self._schema = schema
        self._launch({grain: shape_sql(grain, shape, order) for grain in _GRAINS}, schema,
                     settings=SHAPE_SETTINGS)

    def read_shape(self, grain: str) -> pd.DataFrame:
        from .base import canonical_shape_frame
        if self._schema != "topk":
            return canonical_shape_frame(self.read_view(grain), grain)
        self._check_grain(grain)
        self._barrier()
        rows = self._fold_topic(self._topics[grain])
        frame = (pd.DataFrame(rows)[_TOPK_FIELDS] if rows
                 else pd.DataFrame(columns=_TOPK_FIELDS))
        frame["bucket"] = pd.to_datetime(frame["bucket"], format="ISO8601")
        return canonical_shape_frame(frame, grain)

    def _launch(self, queries: Dict[str, str], schema: str,
                settings: Optional[Dict[str, str]] = None) -> None:
        self._open_session()
        self._reap_orphans()
        self._reap_orphan_topics()
        self._delete_topics(list(self._topics.values()))
        self._create_topics()
        self._declare_tables(settings)
        self._declare_sinks(schema)
        inserts = "\n".join(
            f"INSERT INTO sink_{grain} {sql};" for grain, sql in queries.items())
        body = self._sql(f"EXECUTE STATEMENT SET BEGIN\n{inserts}\nEND;")
        jid = (body.get("jobID")
               or ((body.get("results") or {}).get("data") or [{}])[0].get("fields", [None])[0])
        self._owns_job = True
        self._await_running(jid)

    def _await_running(self, jid: Optional[str], timeout_s: float = 300) -> str:
        deadline = time.time() + timeout_s
        last = None
        while time.time() < deadline:
            job = self._rest_get(f"/jobs/{jid}") if jid else self._own_job()
            if job:
                jid = job.get("jid", jid)
                detail = self._rest_get(f"/jobs/{jid}")
                last = detail.get("state")
                if last == "RUNNING" and all(
                        v.get("status") == "RUNNING" for v in detail.get("vertices", [])):
                    return str(jid)
                if last in ("FAILED", "CANCELED", "FINISHED"):
                    raise RuntimeError(f"Flink job {jid} is {last}, not RUNNING")
            time.sleep(self._poll)
        raise RuntimeError(f"Flink job {jid} did not reach RUNNING within "
                           f"{timeout_s:g}s (last state {last})")

    def ingest_log(self, df: pd.DataFrame) -> IngestStats:
        self._kafka()
        t0 = time.perf_counter()
        stage = df[list(_SOURCE_COLUMNS)].copy()
        stage["event_time_utc"] = pd.to_datetime(
            stage["event_time_utc"], utc=True).dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        stage["supersedes"] = stage["supersedes"].astype(object).where(
            stage["supersedes"].notna(), None)
        topic = self._topics["input"]
        failures: List[str] = []

        def on_delivery(err, _msg):  # noqa: ANN001
            if err is not None:
                failures.append(str(err))

        for record in stage.to_dict(orient="records"):
            payload = json.dumps(record, separators=(",", ":")).encode()
            while True:
                try:
                    self._producer.produce(topic, value=payload, callback=on_delivery)
                    break
                except BufferError:
                    self._producer.poll(1.0)
            self._producer.poll(0)
        remaining = self._producer.flush(300)
        if remaining or failures:
            raise RuntimeError(f"Kafka did not accept every row: {remaining} pending, "
                               f"errors {failures[:3]}")
        self._barrier()
        return IngestStats(rows_sent=len(df), seconds=time.perf_counter() - t0)

    def _barrier(self) -> None:
        """Block until a completed checkpoint covers every row in the input topic."""
        jid = self._await_running(None)
        end = self._end_offset(self._topics["input"])
        deadline = time.time() + self._barrier_timeout
        committed = None
        while time.time() < deadline:
            committed = self._committed_offset()
            if committed is not None and committed >= end:
                return
            state = self._rest_get(f"/jobs/{jid}").get("state")
            if state in ("FAILED", "CANCELED", "FINISHED"):
                raise RuntimeError(f"Flink job {jid} is {state} before the barrier")
            time.sleep(self._poll)
        raise RuntimeError(
            f"Flink job {jid} did not checkpoint past offset {end} within "
            f"{self._barrier_timeout:g}s (committed {committed})")

    def read_view(self, grain: str) -> pd.DataFrame:
        self._check_grain(grain)
        self._barrier()
        rows = self._fold_topic(self._topics[grain])
        if not rows:
            return empty_view()
        view = pd.DataFrame(rows)[list(_VIEW_FIELDS)]
        # Flink's SQL timestamp format drops trailing fractional zeros, so one
        # column mixes "08:00:00" with "08:44:00.32"; parse as ISO 8601.
        view["bucket"] = pd.to_datetime(view["bucket"], format="ISO8601")
        view["latest_event_time"] = canonical_event_time(
            pd.to_datetime(view["latest_event_time"], format="ISO8601"))
        return coerce_view(view, grain)

    def _fold_topic(self, topic: str, key_fields: tuple = ()) -> List[Dict[str, Any]]:
        """Latest record per key up to the topic's end; a tombstone deletes.

        The sink is upsert-kafka and the topic is compacted, so the topic holds
        the view keyed by its primary key rather than every update that produced
        it. That is what bounds the topic by key count: a maintained view can
        re-emit every key on one input row (the S5 family shape does), and the
        uncompacted changelog of one validation stage filled the host.
        """
        from confluent_kafka import Consumer, TopicPartition
        end = self._end_offset(topic)
        if end <= 0:
            return []
        consumer = Consumer({
            "bootstrap.servers": self._broker,
            "group.id": f"{self._prefix}_reader",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
        })
        state: Dict[bytes, Any] = {}
        try:
            consumer.assign([TopicPartition(topic, 0, 0)])
            deadline = time.time() + self._barrier_timeout
            position = 0
            while position < end and time.time() < deadline:
                for message in consumer.consume(num_messages=10_000, timeout=1.0):
                    if message.error():
                        raise RuntimeError(f"reading {topic}: {message.error()}")
                    position = message.offset() + 1
                    value = message.value()
                    state[message.key()] = None if value is None else json.loads(value)
            if position < end:
                raise RuntimeError(f"read {position} of {end} records from {topic}")
        finally:
            consumer.close()
        return [row for row in state.values() if row is not None]

    # --- D5 hooks -----------------------------------------------------------
    def checkpoint(self) -> None:
        self._barrier()

    def resume(self) -> None:
        """Wait for a restore from a retained checkpoint, then a new checkpoint."""
        timeout = float(self.options.get("resume_timeout_s", 600))
        deadline = time.time() + timeout
        last = "no job"
        while time.time() < deadline:
            job = self._own_job()
            if job is not None:
                jid = job["jid"]
                detail = self._rest_get(f"/jobs/{jid}")
                stats = self._rest_get(f"/jobs/{jid}/checkpoints")
                restored = (stats.get("latest") or {}).get("restored") or {}
                completed = (stats.get("latest") or {}).get("completed") or {}
                running = detail.get("state") == "RUNNING" and all(
                    v.get("status") == "RUNNING" for v in detail.get("vertices", []))
                last = (f"state {detail.get('state')}, restores "
                        f"{(stats.get('counts') or {}).get('restored')}")
                if (running and restored and not restored.get("is_savepoint")
                        and completed.get("latest_ack_timestamp", 0)
                        > restored.get("restore_timestamp", 0)):
                    return
            time.sleep(self._poll)
        raise RuntimeError(
            f"Flink job {self._job_name} was not restored from a retained "
            f"checkpoint within {timeout:g}s ({last})")

    def detach(self) -> None:
        """Leave the job and topics in place; D5 is about to measure them."""
        self._close_session()

    def _close_session(self) -> None:
        if self._session is not None:
            try:
                self._requests().delete(
                    f"{self._gateway}/v2/sessions/{self._session}", timeout=30)
            except Exception:
                pass
            self._session = None

    def close(self) -> None:
        try:
            if self._owns_job and self._admin is not None:
                job = self._own_job()
                if job is not None:
                    self._cancel(job["jid"])
                self._delete_topics(list(self._topics.values()))
        finally:
            self._close_session()
