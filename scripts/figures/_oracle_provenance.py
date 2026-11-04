"""Give D8 cells the oracle provenance their runs did not record.

The D8 runner now records the cache key of its hopping oracle, as every other
oracle-backed diagnostic does. Before it did, it cross-checked the pandas and
DuckDB hopping oracles in every cell but recorded no key: that is
benchmark_20260923T191512Z, whose 35 D8 cells the collectors rejected as having
no oracle provenance. For such a cell this rebuilds the same expected table
once per input from the immutable log, through the runner's own row selection,
checks that the two oracles still agree, stores it content-addressed, and
attaches its key to the aggregate. No engine is re-executed and no recorded
verdict changes. A cell that recorded its key is left as it is.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(char in "0123456789abcdefABCDEF" for char in value)
    )


@lru_cache(maxsize=None)
def _materialize(
    root_text: str,
    domain: str,
    source_sha256: str,
    requested_rows: int,
    selection_mode: str,
    actual_rows: int,
) -> str:
    # Imported lazily so ordinary collection keeps its small import surface.
    # _load is the runner's own row selection, so the rebuilt table covers
    # exactly the rows the engine was given.
    from run.run_diagnostic import _load
    from src.adapters.base import canonical_event_time
    from src.oracle.cache import build_or_load_hopping_oracle

    root = Path(root_text)
    source = root / "data" / "normalized" / domain / "log.parquet"
    frame = _load(source, requested_rows, selection_mode)
    if frame.attrs.get("_equitable_input_sha256") != source_sha256:
        raise ValueError(f"D8 input hash changed for {domain}")
    if len(frame) != actual_rows:
        raise ValueError(
            f"D8 {domain}: selected {len(frame)} rows, runs recorded {actual_rows}")

    # The same two conversions _run_hop applies before building its oracle.
    oracle_frame = frame.copy()
    oracle_frame["event_time_ts"] = pd.to_datetime(
        oracle_frame["event_time_utc"], utc=True, format="ISO8601")
    oracle_frame["event_time_utc"] = canonical_event_time(
        oracle_frame["event_time_utc"])
    _, co_oracle, timing = build_or_load_hopping_oracle(
        oracle_frame,
        root=root,
        source_sha256=source_sha256,
        selection=dict(frame.attrs["_equitable_selection"]),
        co_oracle=True,
    )
    if not isinstance(co_oracle, dict) or co_oracle.get("agree") is not True:
        raise ValueError(f"D8 pandas/DuckDB oracles disagree for {domain}")
    return str(timing["oracle_cache_key"])


def cache_keys(
    runs: List[Dict[str, Any]], existing: List[str], root: Path,
) -> List[str]:
    """Return the recorded keys, or a D8 cell's key rebuilt from its input.

    Only a cell whose repeats are all D8, all recorded pandas/DuckDB agreement,
    and all read the same input selection qualifies. Anything else keeps what
    its runs recorded, so the collector's ordinary checks still decide it.
    """
    if existing or not runs:
        return existing
    if {summary.get("diagnostic") for summary in runs} != {"D8"}:
        return existing
    if any((summary.get("co_oracle") or {}).get("agree") is not True
           for summary in runs):
        return existing

    fields = {
        (
            summary.get("domain"),
            summary.get("input_sha256"),
            summary.get("requested_max_rows"),
            summary.get("row_selection"),
            summary.get("rows"),
        )
        for summary in runs
    }
    if len(fields) != 1:
        return existing
    domain, source_sha256, requested_rows, selection_mode, actual_rows = fields.pop()
    if (
        not isinstance(domain, str)
        or not _is_sha256(source_sha256)
        or isinstance(requested_rows, bool)
        or not isinstance(requested_rows, int)
        or selection_mode not in {"prefix", "uniform"}
        or isinstance(actual_rows, bool)
        or not isinstance(actual_rows, int)
    ):
        return existing
    key = _materialize(
        str(root), domain, source_sha256, requested_rows, selection_mode,
        actual_rows)
    return [key]
