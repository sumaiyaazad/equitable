"""Canonical, domain-neutral normalized record.

Single source of truth for the field names of the normalized record, shared by
every domain (finance_markets, transportation_taxi, sports_football, datacenter_drives, environment_airquality), the reference
engine, both oracles, and every diagnostic. Replaces the air-quality-specific
field names ``stid``/``sensor`` with the neutral ``entity``/``metric``.

Stdlib-only on purpose: this module is imported everywhere and must not drag in
pandas/pyarrow.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import math
from typing import Any, Dict, List, Optional

# --- Canonical field names (use these constants, never string literals) ------
ENTITY = "entity"                    # was: stid   (station / symbol / player / drive)
METRIC = "metric"                    # was: sensor (pm25 / trade_price / speed / temperature)
VALUE = "value"
EVENT_TIME = "event_time_utc"
INGEST_TIME = "ingest_time_utc"
PROCESSING_ORDER = "processing_order"
TOPIC = "topic"
DEDUPE_ID = "dedupe_id"
SOURCE_RECORD_TYPE = "source_record_type"
# D7 only: a restatement row's pointer to the dedupe_id of the base observation
# it corrects (null on base rows). NOT part of NORMALIZED_COLUMNS -- base logs
# never carry it; it appears only in the d7_restate fixture. See docs/SEMANTICS.md, Restatements.
SUPERSEDES = "supersedes"

# Column order for the normalized Parquet log (contract artifact).
NORMALIZED_COLUMNS: List[str] = [
    EVENT_TIME,
    INGEST_TIME,
    PROCESSING_ORDER,
    ENTITY,
    METRIC,
    VALUE,
    TOPIC,
    DEDUPE_ID,
    SOURCE_RECORD_TYPE,
]

REQUIRED_FIELDS: List[str] = [
    EVENT_TIME,
    ENTITY,
    METRIC,
    VALUE,
    PROCESSING_ORDER,
    DEDUPE_ID,
    SOURCE_RECORD_TYPE,
]


@dataclass
class NormalizedRow:
    """One normalized observation. Domain-neutral by construction."""
    event_time_utc: str
    entity: str
    metric: str
    value: float
    processing_order: int
    dedupe_id: str
    ingest_time_utc: Optional[str] = None
    topic: Optional[str] = None
    source_record_type: str = "data"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def coerce_processing_order(value: Any) -> int:
    """Return an exact integer replay position or raise ``ValueError``.

    ``int(1.5)`` silently truncates, so every contract boundary uses this
    helper instead of accepting merely int-convertible values.
    """
    if isinstance(value, bool):
        raise ValueError("processing_order must be an integer, not bool")
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("processing_order must be an integer") from exc
    if not math.isfinite(numeric) or not numeric.is_integer():
        raise ValueError("processing_order must be a finite integer")
    return int(value)


def is_explicit_utc_timestamp(value: Any) -> bool:
    """Return whether *value* is ISO-8601 with an explicit UTC offset.

    This stdlib-only schema module remains the single source of truth for both
    runtime validation and the generated schema table.
    Naive and date-only values are rejected: silently localizing them as UTC
    would make the persisted-timestamp contract depend on reader behavior.
    """
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return False
    return bool(
        parsed.tzinfo is not None
        and parsed.utcoffset() == timedelta(0)
    )


def validate_row(row: Dict[str, Any]) -> Optional[str]:
    """Return None if a base normalized row satisfies the contract.

    This is the ingest-boundary validator for the normalized *base* log. D7's
    synthetic restatement rows deliberately use ``source_record_type=
    "restatement"`` and enter through that diagnostic's fixture path; they are
    not base source records and therefore do not weaken this boundary.
    """
    if not isinstance(row, dict):
        return "row_not_dict"
    for field in REQUIRED_FIELDS:
        if row.get(field) in (None, ""):
            return f"missing_{field}"
    if not is_explicit_utc_timestamp(row[EVENT_TIME]):
        return "invalid_event_time_utc"
    ingest_time = row.get(INGEST_TIME)
    if (ingest_time not in (None, "")
            and not is_explicit_utc_timestamp(ingest_time)):
        return "invalid_ingest_time_utc"
    if row[SOURCE_RECORD_TYPE] != "data":
        return "invalid_source_record_type"
    try:
        value = float(row[VALUE])
    except Exception:
        return "invalid_value"
    if not math.isfinite(value):
        return "non_finite_value"
    try:
        coerce_processing_order(row[PROCESSING_ORDER])
    except Exception:
        return "invalid_processing_order"
    return None
