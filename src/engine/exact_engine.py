"""Reference incremental exact-aggregation engine (domain-neutral).

Faithful port of the original air-quality engine with two changes only:
  * neutral field names (entity/metric instead of stid/sensor), and
  * bucketing, the latest-value tiebreak, and the dedupe key are imported from
    ``src.schema`` (single source of truth) instead of being redefined here.

Aggregation semantics are unchanged: O(1) amortized per row, keyed by
(bucket, entity, metric); source-identity dedupe; arrival-order-invariant
latest-value via the (event_time, dedupe_id) tiebreak.
"""
from __future__ import annotations

from dataclasses import dataclass
from threading import RLock
import math
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from src.schema.bucketing import to_hour_bucket, to_day_bucket, is_newer
from src.schema.record import (
    SOURCE_RECORD_TYPE,
    SUPERSEDES,
    coerce_processing_order,
    is_explicit_utc_timestamp,
)

try:
    from datasketches import kll_floats_sketch
except Exception:  # pragma: no cover - optional at import time
    kll_floats_sketch = None


def _parse_timestamp(value: Any) -> Optional[pd.Timestamp]:
    if value is None or not is_explicit_utc_timestamp(value):
        return None
    try:
        ts = pd.to_datetime(value, utc=True, errors="raise")
    except Exception:
        return None
    if pd.isna(ts):
        return None
    return ts


def _isoformat_utc(ts: pd.Timestamp) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _has_nonempty_scalar(value: Any) -> bool:
    if value is None:
        return False
    try:
        return not pd.isna(value) and bool(str(value).strip())
    except (TypeError, ValueError):
        return False


def _dedupe_payload(row: Dict[str, Any]) -> Tuple[str, str, str, float]:
    """Logical source payload; delivery metadata may differ on redelivery."""
    return (row["entity"], row["metric"], row["event_time_utc"], row["value"])


def normalize_row_for_exact(
    row: Dict[str, Any],
    *,
    ingest_sequence: int,
    allow_restatements: bool = False,
) -> Tuple[Optional[Dict[str, Any]], int]:
    normalized, next_ingest_sequence, _ = normalize_row_for_exact_with_reason(
        row,
        ingest_sequence=ingest_sequence,
        allow_restatements=allow_restatements,
    )
    return normalized, next_ingest_sequence


def normalize_row_for_exact_with_reason(
    row: Dict[str, Any],
    *,
    ingest_sequence: int,
    allow_restatements: bool = False,
) -> Tuple[Optional[Dict[str, Any]], int, Optional[str]]:
    """Validate one normalized-log row and attach hour/day buckets.

    Reads the neutral schema (entity/metric/value/event_time_utc/
    processing_order/dedupe_id). Returns (normalized_or_None, next_sequence,
    reject_reason)."""
    if not isinstance(row, dict):
        return None, ingest_sequence, "row_not_dict"

    source_type = row.get(SOURCE_RECORD_TYPE)
    supersedes = row.get(SUPERSEDES)
    has_supersedes = _has_nonempty_scalar(supersedes)
    if source_type not in ("data", "restatement"):
        return None, ingest_sequence, "invalid_source_record_type"
    if source_type == "restatement":
        if not allow_restatements:
            return None, ingest_sequence, "invalid_source_record_type"
        if not has_supersedes:
            return None, ingest_sequence, "invalid_supersedes"
    elif has_supersedes:
        return None, ingest_sequence, "invalid_supersedes"

    entity = str(row.get("entity") or "").strip()
    metric = str(row.get("metric") or "").strip()
    event_time = row.get("event_time_utc")
    parsed_event_time = _parse_timestamp(event_time) if event_time is not None else None
    hour_bucket = to_hour_bucket(str(event_time)) if event_time is not None else None
    day_bucket = to_day_bucket(str(event_time)) if event_time is not None else None
    if not entity:
        return None, ingest_sequence, "missing_entity"
    if not metric:
        return None, ingest_sequence, "missing_metric"
    if parsed_event_time is None or not hour_bucket or not day_bucket:
        return None, ingest_sequence, "invalid_event_time"

    try:
        value = float(row.get("value"))
    except Exception:
        return None, ingest_sequence, "invalid_value"

    processing_order_raw = row.get("processing_order")
    if not math.isfinite(value):
        return None, ingest_sequence, "non_finite_value"
    try:
        processing_order = coerce_processing_order(processing_order_raw)
    except Exception:
        return None, ingest_sequence + 1, "invalid_processing_order"
    next_ingest_sequence = ingest_sequence + 1

    dedupe_id = str(row.get("dedupe_id") or "").strip()
    if not dedupe_id:
        return None, next_ingest_sequence, "missing_dedupe_id"

    ingest_time = row.get("ingest_time_utc")
    ingest_time_present = _has_nonempty_scalar(ingest_time)
    if ingest_time_present:
        parsed_ingest_time = _parse_timestamp(ingest_time)
        if parsed_ingest_time is None:
            return None, next_ingest_sequence, "invalid_ingest_time"
    else:
        parsed_ingest_time = None
    ingest_time_utc = _isoformat_utc(parsed_ingest_time) if parsed_ingest_time is not None else None

    return (
        {
            "entity": entity,
            "metric": metric,
            "value": value,
            "event_time_utc": _isoformat_utc(parsed_event_time),
            "hour_bucket": hour_bucket,
            "day_bucket": day_bucket,
            "topic": row.get("topic"),
            "ingest_time_utc": ingest_time_utc,
            "processing_order": processing_order,
            "dedupe_id": dedupe_id,
            SOURCE_RECORD_TYPE: source_type,
            SUPERSEDES: supersedes if has_supersedes else None,
        },
        next_ingest_sequence,
        None,
    )


def prepare_rows_for_exact(
    rows: Sequence[Dict[str, Any]], *, allow_restatements: bool = False,
) -> Dict[str, Any]:
    accepted_rows: List[Dict[str, Any]] = []
    seen_dedupe_ids: set[str] = set()
    dedupe_payloads: Dict[str, Tuple[str, str, str, float]] = {}
    ingest_sequence = 0
    duplicate_rows = 0
    dedupe_conflicts = 0
    malformed_rows = 0
    out_of_order_rows = 0
    latest_event_seen: Optional[str] = None

    for row in rows:
        normalized, ingest_sequence = normalize_row_for_exact(
            row, ingest_sequence=ingest_sequence,
            allow_restatements=allow_restatements)
        if normalized is None:
            malformed_rows += 1
            continue

        dedupe_id = normalized["dedupe_id"]
        if dedupe_id in seen_dedupe_ids:
            duplicate_rows += 1
            if dedupe_payloads[dedupe_id] != _dedupe_payload(normalized):
                dedupe_conflicts += 1
            continue
        seen_dedupe_ids.add(dedupe_id)

        dedupe_payloads[dedupe_id] = _dedupe_payload(normalized)
        event_time = normalized["event_time_utc"]
        if latest_event_seen is not None and event_time < latest_event_seen:
            out_of_order_rows += 1
        if latest_event_seen is None or event_time > latest_event_seen:
            latest_event_seen = event_time

        accepted_rows.append(normalized)

    return {
        "rows": accepted_rows,
        "rows_loaded": len(rows),
        "rows_accepted": len(accepted_rows),
        "rows_malformed": malformed_rows,
        "duplicate_rows": duplicate_rows,
        "out_of_order_rows": out_of_order_rows,
        "dedupe_conflicts": dedupe_conflicts,
    }


@dataclass
class BucketAggregate:
    bucket: str
    entity: str
    metric: str
    count: int
    sum: float
    min: float
    max: float
    latest_value: float
    latest_event_time: str
    latest_dedupe_id: str

    def as_dict(self) -> Dict[str, Any]:
        mean = self.sum / self.count if self.count else None
        return {
            "bucket": self.bucket,
            "entity": self.entity,
            "metric": self.metric,
            "count": self.count,
            "sum": self.sum,
            "min": self.min,
            "max": self.max,
            "mean": mean,
            "latest_value": self.latest_value,
            "latest_event_time": self.latest_event_time,
        }


@dataclass
class LatestAggregate:
    entity: str
    metric: str
    latest_value: float
    latest_event_time: str
    latest_dedupe_id: str
    topic: Optional[str]
    ingest_time_utc: Optional[str]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "entity": self.entity,
            "metric": self.metric,
            "latest_value": self.latest_value,
            "latest_event_time": self.latest_event_time,
            "topic": self.topic,
            "ingest_time_utc": self.ingest_time_utc,
        }


class ExactEngine:
    def __init__(
        self, *, enable_sketches: bool = False, kll_k: int = 200,
        allow_restatements: bool = False,
    ) -> None:
        self._lock = RLock()
        self._allow_restatements = bool(allow_restatements)
        self._enable_sketches = bool(enable_sketches)
        self._kll_k = max(8, int(kll_k))
        if self._enable_sketches and kll_floats_sketch is None:
            raise RuntimeError("datasketches is required when enable_sketches=True")
        self.reset()

    def reset(self) -> Dict[str, Any]:
        with self._lock:
            self._hourly: Dict[Tuple[str, str, str], BucketAggregate] = {}
            self._daily: Dict[Tuple[str, str, str], BucketAggregate] = {}
            self._latest: Dict[Tuple[str, str], LatestAggregate] = {}
            self._hourly_sketches: Dict[Tuple[str, str, str], Any] = {}
            self._daily_sketches: Dict[Tuple[str, str, str], Any] = {}
            self._seen_dedupe_ids: set[str] = set()
            self._dedupe_payloads: Dict[str, Tuple[str, str, str, float]] = {}
            self._rows_ingested = 0
            self._rows_rejected = 0
            self._duplicate_rows = 0
            self._dedupe_conflicts = 0
            self._out_of_order_rows = 0
            self._latest_event_seen: Optional[str] = None
            self._latest_ingest_seen: Optional[str] = None
            self._first_event_seen: Optional[str] = None
            self._first_ingest_seen: Optional[str] = None
            self._rows_by_entity: Dict[str, int] = {}
            self._rows_by_metric: Dict[str, int] = {}
            self._ingest_sequence = 0
            return {
                "status": "reset",
                "rows_ingested": 0,
                "rows_rejected": 0,
                "duplicate_rows": 0,
                "dedupe_conflicts": 0,
                "out_of_order_rows": 0,
                "sketches_enabled": self._enable_sketches,
            }

    def ingest_rows(self, rows: Sequence[Dict[str, Any]]) -> Dict[str, int]:
        accepted = 0
        rejected = 0
        for row in rows:
            if self.ingest_row(row):
                accepted += 1
            else:
                rejected += 1
        return {"accepted": accepted, "rejected": rejected}

    def ingest_row(self, row: Dict[str, Any]) -> bool:
        with self._lock:
            normalized = self._normalize_row(row)
            if normalized is None:
                self._rows_rejected += 1
                return False

            dedupe_id = normalized["dedupe_id"]
            if dedupe_id in self._seen_dedupe_ids:
                self._duplicate_rows += 1
                if self._dedupe_payloads[dedupe_id] != _dedupe_payload(normalized):
                    self._dedupe_conflicts += 1
                return False
            self._seen_dedupe_ids.add(dedupe_id)

            self._dedupe_payloads[dedupe_id] = _dedupe_payload(normalized)
            event_time = normalized["event_time_utc"]
            if self._latest_event_seen and event_time < self._latest_event_seen:
                self._out_of_order_rows += 1

            self._rows_ingested += 1
            self._rows_by_entity[normalized["entity"]] = self._rows_by_entity.get(normalized["entity"], 0) + 1
            self._rows_by_metric[normalized["metric"]] = self._rows_by_metric.get(normalized["metric"], 0) + 1

            if self._first_event_seen is None or event_time < self._first_event_seen:
                self._first_event_seen = event_time
            if self._latest_event_seen is None or event_time > self._latest_event_seen:
                self._latest_event_seen = event_time

            ingest_time = normalized["ingest_time_utc"]
            if ingest_time is not None:
                if self._first_ingest_seen is None or ingest_time < self._first_ingest_seen:
                    self._first_ingest_seen = ingest_time
                if self._latest_ingest_seen is None or ingest_time > self._latest_ingest_seen:
                    self._latest_ingest_seen = ingest_time

            hour_key = (normalized["hour_bucket"], normalized["entity"], normalized["metric"])
            day_key = (normalized["day_bucket"], normalized["entity"], normalized["metric"])
            self._update_bucket(self._hourly, hour_key, normalized)
            self._update_bucket(self._daily, day_key, normalized)
            self._update_sketch(self._hourly_sketches, hour_key, normalized["value"])
            self._update_sketch(self._daily_sketches, day_key, normalized["value"])
            self._update_latest(normalized)
            return True

    def get_hourly(
        self, *, hour_bucket: Optional[str] = None,
        entity: Optional[str] = None, metric: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        with self._lock:
            items = [
                aggregate.as_dict()
                for aggregate in self._hourly.values()
                if (hour_bucket is None or aggregate.bucket == hour_bucket)
                and (entity is None or aggregate.entity == entity)
                and (metric is None or aggregate.metric == metric)
            ]
        return sorted(items, key=lambda item: (item["bucket"], item["entity"], item["metric"]))

    def get_daily(
        self, *, day_bucket: Optional[str] = None,
        entity: Optional[str] = None, metric: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        with self._lock:
            items = [
                aggregate.as_dict()
                for aggregate in self._daily.values()
                if (day_bucket is None or aggregate.bucket == day_bucket)
                and (entity is None or aggregate.entity == entity)
                and (metric is None or aggregate.metric == metric)
            ]
        return sorted(items, key=lambda item: (item["bucket"], item["entity"], item["metric"]))

    def get_latest(
        self, *, entity: Optional[str] = None, metric: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        with self._lock:
            items = [
                aggregate.as_dict()
                for aggregate in self._latest.values()
                if (entity is None or aggregate.entity == entity)
                and (metric is None or aggregate.metric == metric)
            ]
        return sorted(items, key=lambda item: (item["entity"], item["metric"]))

    def get_hourly_quantiles(
        self, *, hour_bucket: Optional[str] = None,
        entity: Optional[str] = None, metric: Optional[str] = None,
        quantiles: Sequence[float] = (0.5, 0.95, 0.99),
    ) -> List[Dict[str, Any]]:
        return self._get_quantiles(self._hourly_sketches, bucket=hour_bucket,
                                   entity=entity, metric=metric, quantiles=quantiles)

    def get_daily_quantiles(
        self, *, day_bucket: Optional[str] = None,
        entity: Optional[str] = None, metric: Optional[str] = None,
        quantiles: Sequence[float] = (0.5, 0.95, 0.99),
    ) -> List[Dict[str, Any]]:
        return self._get_quantiles(self._daily_sketches, bucket=day_bucket,
                                   entity=entity, metric=metric, quantiles=quantiles)

    def get_stream_health(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "rows_ingested": self._rows_ingested,
                "rows_rejected": self._rows_rejected,
                "duplicate_rows": self._duplicate_rows,
                "out_of_order_rows": self._out_of_order_rows,
                "dedupe_conflicts": self._dedupe_conflicts,
                "first_event_time_seen": self._first_event_seen,
                "latest_event_time_seen": self._latest_event_seen,
                "first_ingest_time_seen": self._first_ingest_seen,
                "latest_ingest_time_seen": self._latest_ingest_seen,
                "entities_seen": sorted(self._rows_by_entity.keys()),
                "metrics_seen": sorted(self._rows_by_metric.keys()),
                "rows_by_entity": dict(sorted(self._rows_by_entity.items())),
                "rows_by_metric": dict(sorted(self._rows_by_metric.items())),
                "hourly_keys": len(self._hourly),
                "daily_keys": len(self._daily),
                "latest_keys": len(self._latest),
                "sketches_enabled": self._enable_sketches,
                "hourly_quantile_keys": len(self._hourly_sketches),
                "daily_quantile_keys": len(self._daily_sketches),
                "kll_k": self._kll_k if self._enable_sketches else None,
            }

    def _normalize_row(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        normalized, next_ingest_sequence = normalize_row_for_exact(
            row, ingest_sequence=self._ingest_sequence,
            allow_restatements=self._allow_restatements)
        self._ingest_sequence = next_ingest_sequence
        return normalized

    def _update_bucket(
        self, state: Dict[Tuple[str, str, str], BucketAggregate],
        key: Tuple[str, str, str], row: Dict[str, Any],
    ) -> None:
        aggregate = state.get(key)
        if aggregate is None:
            state[key] = BucketAggregate(
                bucket=key[0], entity=key[1], metric=key[2],
                count=1, sum=row["value"], min=row["value"], max=row["value"],
                latest_value=row["value"], latest_event_time=row["event_time_utc"],
                latest_dedupe_id=row["dedupe_id"],
            )
            return

        aggregate.count += 1
        aggregate.sum += row["value"]
        aggregate.min = min(aggregate.min, row["value"])
        aggregate.max = max(aggregate.max, row["value"])
        if is_newer(row["event_time_utc"], row["dedupe_id"],
                    aggregate.latest_event_time, aggregate.latest_dedupe_id):
            aggregate.latest_value = row["value"]
            aggregate.latest_event_time = row["event_time_utc"]
            aggregate.latest_dedupe_id = row["dedupe_id"]

    def _update_latest(self, row: Dict[str, Any]) -> None:
        key = (row["entity"], row["metric"])
        aggregate = self._latest.get(key)
        if aggregate is None:
            self._latest[key] = LatestAggregate(
                entity=row["entity"], metric=row["metric"],
                latest_value=row["value"], latest_event_time=row["event_time_utc"],
                latest_dedupe_id=row["dedupe_id"],
                topic=row.get("topic"), ingest_time_utc=row.get("ingest_time_utc"),
            )
            return

        if is_newer(row["event_time_utc"], row["dedupe_id"],
                    aggregate.latest_event_time, aggregate.latest_dedupe_id):
            aggregate.latest_value = row["value"]
            aggregate.latest_event_time = row["event_time_utc"]
            aggregate.latest_dedupe_id = row["dedupe_id"]
            aggregate.topic = row.get("topic")
            aggregate.ingest_time_utc = row.get("ingest_time_utc")

    def _update_sketch(
        self, state: Dict[Tuple[str, str, str], Any],
        key: Tuple[str, str, str], value: float,
    ) -> None:
        if not self._enable_sketches:
            return
        sketch = state.get(key)
        if sketch is None:
            sketch = kll_floats_sketch(k=self._kll_k)
            state[key] = sketch
        sketch.update(float(value))

    def _get_quantiles(
        self, state: Dict[Tuple[str, str, str], Any], *,
        bucket: Optional[str], entity: Optional[str], metric: Optional[str],
        quantiles: Sequence[float],
    ) -> List[Dict[str, Any]]:
        if not self._enable_sketches:
            return []
        with self._lock:
            items = []
            for (bucket_value, entity_value, metric_value), sketch in state.items():
                if bucket is not None and bucket_value != bucket:
                    continue
                if entity is not None and entity_value != entity:
                    continue
                if metric is not None and metric_value != metric:
                    continue
                payload = {
                    "bucket": bucket_value,
                    "entity": entity_value,
                    "metric": metric_value,
                    "count": int(sketch.n),
                    "sketch_type": "kll_floats_sketch",
                    "kll_k": self._kll_k,
                    "num_retained": int(sketch.num_retained),
                    "serialized_bytes": int(len(sketch.serialize())),
                }
                for q in quantiles:
                    label = _quantile_field_name(q)
                    payload[label] = float(sketch.get_quantile(float(q))) if sketch.n else None
                items.append(payload)
        return sorted(items, key=lambda item: (item["bucket"], item["entity"], item["metric"]))


def _quantile_field_name(value: float) -> str:
    if value <= 0 or value >= 1:
        raise ValueError("Quantiles must be between 0 and 1.")
    pct = value * 100.0
    if float(pct).is_integer():
        return f"p{int(pct)}"
    return f"p{str(pct).replace('.', 'p')}"


def load_rows_from_path(path: str) -> List[Dict[str, Any]]:
    if path.endswith(".parquet"):
        frame = pd.read_parquet(path)
    else:
        frame = pd.read_json(path, orient="records", lines=True)
    return frame.to_dict(orient="records")
