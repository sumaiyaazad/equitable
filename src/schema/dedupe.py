"""Source-observation deduplication keys.

Values and timestamps are *not* identities: two distinct source observations
may legitimately have the same entity, metric, timestamp, and value. A
normalized row therefore derives its ``dedupe_id`` from the source system's
stable event identifier (or a deterministic file/row identifier at ingest).
"""
from __future__ import annotations

import hashlib


def build_dedupe_id(source_event_id: str, metric: str, *, namespace: str = "") -> str:
    """Return a stable row identity for one metric of a source observation.

    ``namespace`` distinguishes source streams (the generic normalizer uses
    ``domain|entity``). ``metric`` distinguishes rows exploded from one wide
    source observation. Re-delivery preserves all three inputs and therefore
    the hash; a separate observation with identical content does not collapse.
    """
    source_event_id = str(source_event_id).strip()
    metric = str(metric).strip()
    namespace = str(namespace).strip()
    if not source_event_id:
        raise ValueError("source_event_id must be non-empty")
    if not metric:
        raise ValueError("metric must be non-empty")
    raw = f"{namespace}|{source_event_id}|{metric}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
