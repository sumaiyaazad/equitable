#!/usr/bin/env python3
"""Run one Flink cell in its own process, then exit.

WHY A SUBPROCESS
----------------
Calcite keeps planner metadata in JVM-global/thread-local state
(``RelMetadataQueryBase.THREAD_PROVIDERS``). In a long-lived matrix process the
other adapters -- pyspark in particular, which boots its own JVM through a second
py4j gateway -- leave that state unusable, and every later Flink plan dies with::

    java.lang.NullPointerException: metadataHandlerProvider
      at org.apache.calcite.rel.metadata.RelMetadataQueryBase.getMetadataHandlerProvider

Measured, not assumed: it reproduced both with a fresh TableEnvironment per cell
and with one cached per process, always after the first domain, and it blocked
24 of 30 Flink cells in a five-domain pre-flight. A fresh JVM per cell is the only
arrangement that holds, so the adapter pays one JVM start per cell to get it.

Both grains are computed in a single invocation so the cost is paid once, not
twice. Contract: read the input parquet, emit ``<out_prefix>.hourly.parquet`` and
``<out_prefix>.daily.parquet`` in the comparator's VIEW_COLUMNS schema, exit 0.
Any failure exits non-zero with the traceback on stderr, which the parent turns
into a normal blocked cell rather than a crash.
"""
from __future__ import annotations

import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def main() -> int:
    if len(sys.argv) != 5:
        print("usage: _flink_worker.py <input.parquet> <variant> <mode> <out_prefix>",
              file=sys.stderr)
        return 2
    input_path, variant, mode, out_prefix = sys.argv[1:5]

    import pandas as pd
    from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment

    from src.adapters.flink import _SRC, _VIEW_FIELDS, _view_sql, fold_changelog
    from src.adapters.base import canonical_event_time, coerce_view

    df = pd.read_parquet(input_path)

    settings = (EnvironmentSettings.in_streaming_mode() if mode == "streaming"
                else EnvironmentSettings.in_batch_mode())
    t_env = TableEnvironment.create(settings)
    # Single-partition fold keeps reduction order stable for D3's bitwise replay
    # comparison, and keeps the network-buffer demand of the D7 restate plan
    # inside what a MiniCluster provides.
    t_env.get_config().set("parallelism.default", "1")

    stage = df[["event_time_utc", "entity", "metric", "value",
                "processing_order", "dedupe_id", "supersedes"]].copy()
    stage["event_time_utc"] = pd.to_datetime(
        stage["event_time_utc"], utc=True).dt.tz_localize(None)
    stage["supersedes"] = stage["supersedes"].astype(object).where(
        stage["supersedes"].notna(), None)
    row_type = DataTypes.ROW([
        # The neutral event-time contract has six fractional digits. TIMESTAMP(3)
        # would round away valid source distinctions before aggregation.
        DataTypes.FIELD("event_time_utc", DataTypes.TIMESTAMP(6)),
        DataTypes.FIELD("entity", DataTypes.STRING()),
        DataTypes.FIELD("metric", DataTypes.STRING()),
        DataTypes.FIELD("value", DataTypes.DOUBLE()),
        DataTypes.FIELD("processing_order", DataTypes.BIGINT()),
        DataTypes.FIELD("dedupe_id", DataTypes.STRING()),
        DataTypes.FIELD("supersedes", DataTypes.STRING()),
    ])
    t_env.create_temporary_view(_SRC, t_env.from_pandas(stage, row_type))

    for grain in ("hourly", "daily"):
        result = t_env.execute_sql(_view_sql(grain, variant))
        if mode == "streaming":
            rows = fold_changelog(result)
        else:
            with result.collect() as iterator:
                rows = [list(row) for row in iterator]
        out = Path(f"{out_prefix}.{grain}.parquet")
        if not rows:
            pd.DataFrame(columns=_VIEW_FIELDS).to_parquet(out, index=False)
            continue
        view = pd.DataFrame(rows, columns=list(_VIEW_FIELDS))
        view["latest_event_time"] = canonical_event_time(view["latest_event_time"])
        coerce_view(view, grain).to_parquet(out, index=False)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        raise SystemExit(1)
