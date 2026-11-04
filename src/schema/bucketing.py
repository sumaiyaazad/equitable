"""Event-time bucketing and the latest-value tiebreak (domain-neutral).

Stdlib datetime only (no pandas) so the contract stays dependency-light.
Timestamps are ISO-8601 UTC, e.g. ``2026-04-01T12:34:56Z``.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


def _parse(event_time_utc: str) -> Optional[datetime]:
    if event_time_utc is None:
        return None
    s = str(event_time_utc).strip()
    if not s:
        return None
    # Accept trailing 'Z' (Python 3.11+ fromisoformat also accepts it, but be explicit).
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def canonicalize_event_time(event_time_utc: str) -> Optional[str]:
    """Normalize an ISO-8601 timestamp to UTC without discarding sub-seconds.

    Fixed six-digit fractional seconds keep lexical order equal to temporal
    order, including data such as market trades and 25 Hz tracking frames.
    """
    dt = _parse(event_time_utc)
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def to_hour_bucket(event_time_utc: str) -> Optional[str]:
    """UTC hour floor, e.g. 2026-04-01T12:34:56Z -> 2026-04-01T12:00:00Z."""
    dt = _parse(event_time_utc)
    if dt is None:
        return None
    return dt.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:00:00Z")


def to_day_bucket(event_time_utc: str) -> Optional[str]:
    """UTC calendar date, e.g. 2026-04-01T12:34:56Z -> 2026-04-01."""
    dt = _parse(event_time_utc)
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%d")


def is_newer(
    incoming_event_time: str,
    incoming_dedupe_id: str,
    current_event_time: str,
    current_dedupe_id: str,
) -> bool:
    """Latest-value rule with an arrival-order-independent total order.

    Newer event time wins. Equal-time source observations are resolved by the
    stable ``dedupe_id`` rather than delivery/processing order.
    """
    if incoming_event_time > current_event_time:
        return True
    if incoming_event_time < current_event_time:
        return False
    return str(incoming_dedupe_id) >= str(current_dedupe_id)
