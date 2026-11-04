"""Reference-engine adapter: the in-process exact incremental engine.

The reference engine (`src/engine/exact_engine.py`) is the correctness anchor of
the benchmark, not a system under test in the usual sense: it dedupes by content
and folds deterministically by construction, so it always passes D1-D3. Wrapping
it behind the same EngineAdapter interface lets the diagnostic runner include the
reference row in every table through the identical code path, and gives every
other adapter's smoke test a ground truth to diff against without standing up a
container.

Because deduplication is intrinsic, this adapter supports only variant="dedup".
No connection, no teardown; everything is in-process.
"""
from __future__ import annotations

import time

import pandas as pd

from src.engine.exact_engine import ExactEngine
from src.oracle.comparator import engine_bucket_table

from .base import EngineAdapter, IngestStats, coerce_view


class ReferenceAdapter(EngineAdapter):
    name = "reference"
    # source-identity dedupe is intrinsic (so no "naive"); "restate" is the D7
    # retract-latest view, expressed by collapsing cells before the fold.
    supported_variants = ("dedup", "restate")

    def __init__(self, **options) -> None:
        super().__init__(**options)
        self._engine: ExactEngine | None = None
        self._rows: pd.DataFrame | None = None

    def setup(self, *, variant: str = "dedup") -> None:
        self._check_variant(variant)
        self._engine = ExactEngine(
            allow_restatements=bool(self.options.get("allow_restatements", False)))

    def ingest_log(self, df: pd.DataFrame) -> IngestStats:
        if self._engine is None:
            raise RuntimeError("call setup() before ingest_log()")
        if self.variant == "restate":
            # D7 (docs/SEMANTICS.md, Restatements): resolve each restatement's supersedes pointer
            # -- replace the superseded base row's value with the superseding
            # restatement of greatest processing_order, and drop the restatement
            # rows. A restatement is any row with a non-null supersedes.
            sup = df["supersedes"]
            is_rest = sup.notna() & (sup.astype(object) != "")
            latest = (df[is_rest].sort_values("processing_order")
                      .groupby("supersedes")["value"].last())
            base = df[~is_rest].copy()
            base["value"] = base["dedupe_id"].map(latest).where(
                base["dedupe_id"].isin(latest.index), base["value"])
            df = base
        self._rows = df
        rows = df.to_dict("records")
        t0 = time.perf_counter()
        result = self._engine.ingest_rows(rows)
        seconds = time.perf_counter() - t0
        return IngestStats(rows_sent=len(rows), seconds=seconds, extra=dict(result))

    def read_view(self, grain: str) -> pd.DataFrame:
        if self._engine is None:
            raise RuntimeError("call setup()/ingest_log() before read_view()")
        self._check_grain(grain)
        rows = self._engine.get_hourly() if grain == "hourly" else self._engine.get_daily()
        return coerce_view(engine_bucket_table(rows), grain)

    # --- D8: hopping window ------------------------------------------------
    #: The reference computes the hopping table by the same explode-then-fold
    #: rule the oracle uses, so its D8 cell is equivalent BY CONSTRUCTION, in
    #: exactly the way its D1--D3 cells are. It is an anchor that proves the
    #: path runs end to end, not an independent check, which is also why the
    #: reference is excluded from the abstract's headline denominators.
    hopping_mode = "reference"

    def setup_hopping(self, *, size_n: int = 2, hop_n: int = 1) -> None:
        self.setup(variant="dedup")
        self.options["hopping_size_n"] = int(size_n)
        self.options["hopping_hop_n"] = int(hop_n)

    def read_hopping(self, grain: str) -> pd.DataFrame:
        from src.oracle.hopping import build_hopping_table, geometry
        self._check_grain(grain)
        if self._rows is None:
            raise RuntimeError("call setup_hopping()/ingest_log() first")
        size_h, hop_h = geometry(grain)
        from .base import canonical_event_time
        frame = self._rows.copy()
        frame["event_time_ts"] = pd.to_datetime(
            frame["event_time_utc"], utc=True, format="ISO8601")
        # Canonical precision, not whole seconds: truncating here makes the
        # (event_time, dedupe_id) tiebreak pick a different row than the SQL
        # oracle does.
        frame["event_time_utc"] = canonical_event_time(frame["event_time_utc"])
        table = build_hopping_table(frame, grain=grain,
                                    size_h=size_h, hop_h=hop_h)
        return coerce_view(table, grain)

    def close(self) -> None:
        self._engine = None
        self._rows = None
