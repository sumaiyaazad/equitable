"""Domain-neutral normalized schema.

Single source of truth for the normalized-record schema, deduplication,
bucketing, and config-driven normalization shared by every domain, the
reference engine, both oracles, and every diagnostic.
"""
from .record import (
    ENTITY, METRIC, VALUE, EVENT_TIME, INGEST_TIME, PROCESSING_ORDER,
    TOPIC, DEDUPE_ID, SOURCE_RECORD_TYPE, SUPERSEDES, NORMALIZED_COLUMNS,
    REQUIRED_FIELDS, NormalizedRow, validate_row, coerce_processing_order,
)
from .dedupe import build_dedupe_id
from .bucketing import canonicalize_event_time, to_hour_bucket, to_day_bucket, is_newer
from .config import DomainConfig
from .normalize import normalize_records

__all__ = [
    "ENTITY", "METRIC", "VALUE", "EVENT_TIME", "INGEST_TIME", "PROCESSING_ORDER",
    "TOPIC", "DEDUPE_ID", "SOURCE_RECORD_TYPE", "SUPERSEDES", "NORMALIZED_COLUMNS", "REQUIRED_FIELDS",
    "NormalizedRow", "validate_row", "coerce_processing_order",
    "build_dedupe_id",
    "canonicalize_event_time", "to_hour_bucket", "to_day_bucket", "is_newer",
    "DomainConfig", "normalize_records",
]
