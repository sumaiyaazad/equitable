"""Generic, config-driven normalizer: raw domain records -> neutral rows.

The SAME function normalizes every domain; only the DomainConfig differs. This
replaces the air-quality-specific ``normalize_message`` in the retired
``phase1_station_log.py``.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from .config import DomainConfig
from .dedupe import build_dedupe_id
from .bucketing import canonicalize_event_time
from . import record as S


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _coerce_float(value: Any) -> Optional[float]:
    # Reject absent/empty and the boolean False, but not numeric zero:
    # `0.0 == False` is True in Python, so `in (None, "", False)` would have
    # silently dropped every exactly-zero measurement -- use identity for False.
    if value is None or value == "" or value is False:
        return None
    try:
        f = float(value)
    except Exception:
        return None
    # NaN/inf are out of contract: they aggregate inconsistently across the
    # engine and the two oracles.
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def _emit(
    rows: List[Dict[str, Any]],
    *,
    entity: str,
    metric: str,
    value: float,
    event_time_utc: str,
    topic: Optional[str],
    ingest_time_utc: str,
    order: int,
    source_event_id: str,
    identity_namespace: str,
) -> None:
    rows.append(
        {
            S.EVENT_TIME: event_time_utc,
            S.INGEST_TIME: ingest_time_utc,
            S.PROCESSING_ORDER: order,
            S.ENTITY: entity,
            S.METRIC: metric,
            S.VALUE: value,
            S.TOPIC: topic,
            S.DEDUPE_ID: build_dedupe_id(
                source_event_id, metric, namespace=identity_namespace
            ),
            S.SOURCE_RECORD_TYPE: "data",
        }
    )


def normalize_records(
    raw_records: Iterable[Dict[str, Any]],
    config: DomainConfig,
    *,
    ingest_time_utc: Optional[str] = None,
    processing_order_start: int = 0,
) -> List[Dict[str, Any]]:
    """Map an iterable of raw domain records to normalized neutral rows.

    Invalid records (missing entity/time or non-numeric value) are skipped, not
    raised; the normalization boundary rejects out-of-contract input before it
    reaches aggregate state.
    """
    ingest_time_utc = ingest_time_utc or _iso_utc_now()
    rows: List[Dict[str, Any]] = []

    for raw_index, rec in enumerate(raw_records):
        if not isinstance(rec, dict):
            continue
        entity = str(rec.get(config.entity_field) or "").strip()
        event_time_raw = rec.get(config.event_time_field)
        if not entity or not event_time_raw:
            continue
        event_time_utc = canonicalize_event_time(str(event_time_raw))
        if event_time_utc is None:
            continue

        if config.source_id_field is not None:
            source_event_id = str(rec.get(config.source_id_field) or "").strip()
            if not source_event_id:
                continue
        else:
            source_event_id = str(raw_index)
        identity_namespace = f"{config.name}|{entity}"

        if config.topic_const is not None:
            topic = config.topic_const
        elif config.topic_field is not None:
            topic = rec.get(config.topic_field)
        else:
            topic = None

        if config.layout == "wide":
            for canonical_metric, raw_col in config.metric_fields.items():
                value = _coerce_float(rec.get(raw_col))
                if value is None:
                    continue
                _emit(
                    rows, entity=entity, metric=canonical_metric, value=value,
                    event_time_utc=event_time_utc, topic=topic,
                    ingest_time_utc=ingest_time_utc,
                    order=processing_order_start + len(rows),
                    source_event_id=source_event_id,
                    identity_namespace=identity_namespace,
                )
        else:  # long
            raw_metric = str(rec.get(config.metric_field) or "").strip()
            metric = config.metric_map.get(raw_metric, raw_metric) if config.metric_map else raw_metric
            if not metric:
                continue
            value = _coerce_float(rec.get(config.value_field))
            if value is None:
                continue
            _emit(
                rows, entity=entity, metric=metric, value=value,
                event_time_utc=event_time_utc, topic=topic,
                ingest_time_utc=ingest_time_utc,
                order=processing_order_start + len(rows),
                source_event_id=source_event_id,
                identity_namespace=identity_namespace,
            )

    return rows
