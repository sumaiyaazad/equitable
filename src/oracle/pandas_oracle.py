"""Independent offline oracle (pandas), domain-neutral.

Batch recomputation of the same aggregates the online engine maintains, over
the deduplicated normalized log, keyed by (bucket, entity, metric). Shares no
aggregation code with the engine; that independence is what makes the
online--oracle comparison meaningful.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, Sequence

import pandas as pd

BUCKET_COLUMNS = ["bucket", "entity", "metric", "count", "sum", "min", "max",
                  "mean", "latest_value", "latest_event_time"]
QUANTILE_COLUMNS = ["bucket", "entity", "metric", "count", "p50", "p95", "p99"]


def _coerce_processing_order_independently(value: Any) -> int:
    """Validate replay order here, without importing the online contract code."""
    if isinstance(value, bool):
        raise ValueError("bool is not a replay position")
    numeric = float(value)
    if not math.isfinite(numeric) or not numeric.is_integer():
        raise ValueError("replay position must be a finite integer")
    return int(value)


def _has_nonempty_scalar_independently(value: Any) -> bool:
    if value is None:
        return False
    try:
        return not bool(pd.isna(value)) and bool(str(value).strip())
    except (TypeError, ValueError):
        return False


def _parse_utc_timestamp_independently(value: Any) -> "pd.Timestamp | None":
    if not _has_nonempty_scalar_independently(value):
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        return None
    return pd.Timestamp(parsed).tz_convert("UTC")


def _supersedes_independently(row: Dict[str, Any]) -> Any:
    value = row.get("supersedes")
    return value if _has_nonempty_scalar_independently(value) else None


def _empty_bucket_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=BUCKET_COLUMNS)


def _empty_quantile_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=QUANTILE_COLUMNS)


def _build_bucket_table(frame: pd.DataFrame, bucket_column: str) -> pd.DataFrame:
    if frame.empty:
        return _empty_bucket_frame()

    keys = [bucket_column, "entity", "metric"]
    aggregate = (
        frame.groupby(keys, as_index=False, sort=True)["value"]
        .agg(count="count", sum="sum", min="min", max="max", mean="mean")
    )
    latest_rows = (
        frame.sort_values(["event_time_ts", "dedupe_id"])
        .groupby(keys, as_index=False, sort=True)
        .tail(1)[keys + ["value", "event_time_utc"]]
        .rename(columns={bucket_column: "bucket", "value": "latest_value",
                         "event_time_utc": "latest_event_time"})
    )
    aggregate = aggregate.rename(columns={bucket_column: "bucket"})
    result = aggregate.merge(latest_rows, on=["bucket", "entity", "metric"], how="left")
    return result[BUCKET_COLUMNS].sort_values(["bucket", "entity", "metric"], kind="stable")


def _build_quantile_table(frame: pd.DataFrame, bucket_column: str) -> pd.DataFrame:
    if frame.empty:
        return _empty_quantile_frame()
    keys = [bucket_column, "entity", "metric"]
    quantiles = (
        frame.groupby(keys, as_index=False, sort=True)["value"]
        .agg(
            count="count",
            p50=lambda s: s.quantile(0.50, interpolation="linear"),
            p95=lambda s: s.quantile(0.95, interpolation="linear"),
            p99=lambda s: s.quantile(0.99, interpolation="linear"),
        )
        .rename(columns={bucket_column: "bucket"})
    )
    return quantiles[QUANTILE_COLUMNS].sort_values(["bucket", "entity", "metric"], kind="stable")


def _apply_restatements(rows: Sequence[Dict[str, Any]]) -> list:
    """D7: resolve the supersedes pointers (docs/SEMANTICS.md, Restatements). For each base
    observation, if one or more restatement rows supersede it (by its dedupe_id),
    replace its value with the superseding restatement of greatest
    processing_order; drop the restatement rows (they replace, they do not add).
    The base row keeps its original dedupe_id, so downstream dedup does not
    re-collapse corrected rows that now share content."""

    rows = list(rows)
    latest: Dict[str, tuple] = {}  # superseded dedupe_id -> (processing_order, value)
    for r in rows:
        sid = _supersedes_independently(r)
        if r.get("source_record_type") == "restatement" and sid is not None:
            po = _coerce_processing_order_independently(r.get("processing_order"))
            if sid not in latest or po > latest[sid][0]:
                latest[sid] = (po, r.get("value"))
    out = []
    for r in rows:
        if (r.get("source_record_type") == "restatement"
                and _supersedes_independently(r) is not None):
            continue  # restatement rows are consumed, not counted
        did = r.get("dedupe_id")
        if did in latest:
            r = {**r, "value": latest[did][1]}
        out.append(r)
    return out


def build_oracle_tables_from_rows(
    rows: Sequence[Dict[str, Any]], *, restatement_aware: bool = False,
    include_quantiles: bool = True,
) -> Dict[str, Any]:
    """Build exact tables and, by default, quantile tables.

    Exact-only benchmark paths pass ``include_quantiles=False`` so they do not
    construct unused high-cardinality quantiles; the public default and output
    schema remain backward compatible.
    """
    if restatement_aware:
        rows = _apply_restatements(rows)
    prepared = _prepare_rows_independently(rows)
    frame = pd.DataFrame(prepared["rows"])

    if frame.empty:
        hourly = daily = _empty_bucket_frame()
        hourly_quantiles = daily_quantiles = _empty_quantile_frame()
    else:
        frame = frame.copy()
        frame["event_time_ts"] = pd.to_datetime(frame["event_time_utc"], utc=True)
        frame["processing_order"] = frame["processing_order"].astype(int)
        frame["hour_bucket"] = frame["event_time_ts"].dt.floor("h").dt.strftime("%Y-%m-%dT%H:00:00Z")
        frame["day_bucket"] = frame["event_time_ts"].dt.strftime("%Y-%m-%d")
        hourly = _build_bucket_table(frame, "hour_bucket")
        daily = _build_bucket_table(frame, "day_bucket")
        if include_quantiles:
            hourly_quantiles = _build_quantile_table(frame, "hour_bucket")
            daily_quantiles = _build_quantile_table(frame, "day_bucket")
        else:
            hourly_quantiles = daily_quantiles = _empty_quantile_frame()

    summary = {
        "rows_loaded": prepared["rows_loaded"],
        "rows_accepted": prepared["rows_accepted"],
        "rows_malformed": prepared["rows_malformed"],
        "duplicate_rows": prepared["duplicate_rows"],
        "out_of_order_rows": prepared["out_of_order_rows"],
        "hourly_keys": int(len(hourly)),
        "dedupe_conflicts": prepared["dedupe_conflicts"],
        "daily_keys": int(len(daily)),
        "restatement_aware": bool(restatement_aware),
        "quantiles_included": bool(include_quantiles),
        "entities_seen": sorted(hourly["entity"].unique().tolist()) if not hourly.empty else [],
        "metrics_seen": sorted(hourly["metric"].unique().tolist()) if not hourly.empty else [],
    }
    return {"hourly": hourly, "daily": daily,
            "hourly_quantiles": hourly_quantiles, "daily_quantiles": daily_quantiles,
            "summary": summary}


def build_oracle_tables_from_path(
    path: str, *, max_rows: int = 0, restatement_aware: bool = False
) -> Dict[str, Any]:
    rows = _load_rows_from_path(path)
    total_loaded = len(rows)
    if rows and "processing_order" in rows[0]:
        rows = sorted(rows, key=lambda row: (row.get("processing_order") is None,
                                             row.get("processing_order", 0)))
    if max_rows > 0:
        rows = list(rows[:max_rows])
    result = build_oracle_tables_from_rows(rows, restatement_aware=restatement_aware)
    result["input_path"] = str(Path(path).resolve())
    extra: Dict[str, Any] = {"rows_loaded_from_file": total_loaded, "rows_used_for_oracle": len(rows)}
    if max_rows > 0:
        extra["max_rows"] = max_rows
    result["summary"] = {**result["summary"], **extra}
    return result


def write_oracle_outputs(*, result: Dict[str, Any], output_dir: str,
                         write_csv: bool = False) -> Dict[str, str]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    paths = {
        "hourly_oracle": output_path / "hourly_oracle.parquet",
        "daily_oracle": output_path / "daily_oracle.parquet",
        "hourly_quantiles_oracle": output_path / "hourly_quantiles_oracle.parquet",
        "daily_quantiles_oracle": output_path / "daily_quantiles_oracle.parquet",
    }
    result["hourly"].to_parquet(paths["hourly_oracle"], index=False)
    result["daily"].to_parquet(paths["daily_oracle"], index=False)
    result["hourly_quantiles"].to_parquet(paths["hourly_quantiles_oracle"], index=False)
    result["daily_quantiles"].to_parquet(paths["daily_quantiles_oracle"], index=False)
    summary_path = output_path / "oracle_summary.json"
    summary_path.write_text(
        json.dumps({"input_path": result.get("input_path"), **result["summary"]},
                   indent=2, sort_keys=True) + "\n", encoding="utf-8")
    written = {k: str(v) for k, v in paths.items()}
    written["oracle_summary"] = str(summary_path)
    if write_csv:
        for name, frame_key in [("hourly_oracle", "hourly"), ("daily_oracle", "daily"),
                                ("hourly_quantiles_oracle", "hourly_quantiles"),
                                ("daily_quantiles_oracle", "daily_quantiles")]:
            csv_path = output_path / f"{frame_key}_oracle.csv"
            result[frame_key].to_csv(csv_path, index=False)
            written[f"{name}_csv"] = str(csv_path)
    return written

def _load_rows_from_path(path: str) -> list[Dict[str, Any]]:
    """Load input without importing the online engine's convenience path."""
    if path.endswith(".parquet"):
        frame = pd.read_parquet(path)
    else:
        frame = pd.read_json(path, orient="records", lines=True)
    return frame.to_dict(orient="records")


def _prepare_rows_independently(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Validate and deduplicate without sharing the online preprocessing code."""
    accepted: list[Dict[str, Any]] = []
    seen: Dict[str, tuple] = {}
    malformed = duplicate = conflicts = out_of_order = 0
    latest_event_seen: str | None = None

    for row in rows:
        if not isinstance(row, dict):
            malformed += 1
            continue
        if (row.get("source_record_type") != "data"
                or _supersedes_independently(row) is not None):
            malformed += 1
            continue
        entity = str(row.get("entity") or "").strip()
        metric = str(row.get("metric") or "").strip()
        dedupe_id = str(row.get("dedupe_id") or "").strip()
        event_ts = _parse_utc_timestamp_independently(row.get("event_time_utc"))
        ingest_time = row.get("ingest_time_utc")
        ingest_time_present = _has_nonempty_scalar_independently(ingest_time)
        ingest_ts = _parse_utc_timestamp_independently(ingest_time)
        if ingest_time_present and ingest_ts is None:
            malformed += 1
            continue
        try:
            value = float(row.get("value"))
            processing_order = _coerce_processing_order_independently(row.get("processing_order"))
        except (TypeError, ValueError, OverflowError):
            malformed += 1
            continue
        if not entity or not metric or not dedupe_id or event_ts is None or not math.isfinite(value):
            malformed += 1
            continue

        event_time_utc = event_ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        payload = (entity, metric, event_time_utc, value)
        if dedupe_id in seen:
            duplicate += 1
            if seen[dedupe_id] != payload:
                conflicts += 1
            continue
        seen[dedupe_id] = payload

        if latest_event_seen is not None and event_time_utc < latest_event_seen:
            out_of_order += 1
        if latest_event_seen is None or event_time_utc > latest_event_seen:
            latest_event_seen = event_time_utc

        accepted.append({
            "entity": entity,
            "metric": metric,
            "value": value,
            "event_time_utc": event_time_utc,
            "processing_order": processing_order,
            "dedupe_id": dedupe_id,
            "topic": row.get("topic"),
            "ingest_time_utc": None if ingest_ts is None else ingest_ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        })

    return {
        "rows": accepted,
        "rows_loaded": len(rows),
        "rows_accepted": len(accepted),
        "rows_malformed": malformed,
        "duplicate_rows": duplicate,
        "dedupe_conflicts": conflicts,
        "out_of_order_rows": out_of_order,
    }
