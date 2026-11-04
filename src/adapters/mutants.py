"""Seeded-defect ("mutant") adapters -- the benchmark's detection-power control.

A diagnostic suite that reports failures is only credible if it is also shown to
*catch defects it was designed to catch* and to stay quiet otherwise. These
adapters wrap the reference engine and inject exactly one violation of one
correctness condition C1-C7 each, so `scripts/run/run_detection_power.py` can
measure sensitivity (does the targeted diagnostic fire?) and specificity (do the
others stay silent?).

Every mutant is a WRAPPER. The reference engine's aggregation semantics are
frozen (plan section 15) and none of this touches them: defects are injected in
the adapter's ingest or view path, which is exactly where a real engine's defect
lives relative to the contract.

  mutant_no_dedup           condition 1  source-identity duplicate suppression
  mutant_order_sensitive    condition 2  arrival-order invariance
  mutant_ulp_drift          condition 3  deterministic reduction
  mutant_drop_late          condition 4  event-time lateness conformance
  mutant_ignore_supersedes  condition 7  retraction-consistent summarization
  mutant_recovery_loss      condition 5  post-barrier recovery durability
  mutant_topk_rank          condition 6  downstream top-k rank correctness

Conditions 5 and 6 use dedicated mutation surfaces. D5 simulates acknowledged
state loss at an in-process restart boundary, while the production D5 benchmark run
still uses a real durability barrier and SIGKILL. D6 corrupts only the downstream
ranked view, leaving the maintained aggregate unchanged.
"""
from __future__ import annotations

import random
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from .base import IngestStats
from .reference import ReferenceAdapter

#: Which Lemma-1 condition each mutant violates, and the diagnostic that should
#: therefore fire. Used by the detection-power harness to score the diagonal.
MUTANT_TARGETS: Dict[str, Dict[str, Any]] = {
    "mutant_no_dedup": {"condition": 1, "diagnostic": "D1",
                        "defect": "source-identity duplicates are not suppressed"},
    "mutant_order_sensitive": {"condition": 2, "diagnostic": "D2",
                               "defect": "bucket and latest time follow the arrival frontier, not event time"},
    "mutant_ulp_drift": {"condition": 3, "diagnostic": "D3",
                         "defect": "sum drifts by one ULP between replays"},
    "mutant_drop_late": {"condition": 4, "diagnostic": "D4",
                         "defect": "records behind the watermark are dropped"},
    "mutant_ignore_supersedes": {"condition": 7, "diagnostic": "D7",
                                 "defect": "restatements are applied as fresh inserts"},
    "mutant_recovery_loss": {"condition": 5, "diagnostic": "D5",
                             "defect": "acknowledged state is lost on restart"},
    "mutant_topk_rank": {"condition": 6, "diagnostic": "D6",
                         "defect": "one entity's maintained rank score is corrupted"},
}


class NoDedupMutant(ReferenceAdapter):
    """Condition 1: give every row a unique dedupe_id so content-dedup never fires.

    This is the ksqlDB-style defect -- the engine has a dedup step, but it keys on
    something that a byte-identical redelivery does not collide on.
    """

    name = "mutant_no_dedup"

    def ingest_log(self, df: pd.DataFrame) -> IngestStats:
        df = df.copy()
        df["dedupe_id"] = [f"{d}#{i}" for i, d in enumerate(df["dedupe_id"])]
        return super().ingest_log(df)


class OrderSensitiveMutant(ReferenceAdapter):
    """Condition 2: let the arrival-time frontier replace event time.

    The correct rule is the (event_time, dedupe_id) tiebreak, which is
    invariant to arrival order. Taking whatever landed last makes the view a
    function of delivery order -- the defect a bounded reorder exposes.
    """

    name = "mutant_order_sensitive"

    def __init__(self, **options: Any) -> None:
        super().__init__(**options)
        self._ingested: Optional[pd.DataFrame] = None

    def ingest_log(self, df: pd.DataFrame) -> IngestStats:
        self._ingested = df.copy()
        mutated = df.copy()
        event_times = pd.to_datetime(mutated["event_time_utc"], utc=True)
        # A processing-time implementation assigns an out-of-order event the
        # current arrival frontier. This is observational even when a domain has
        # one row per entity/day: latest_event_time changes, and a frontier that
        # crosses a bucket boundary also changes membership. Sorted controls are
        # unchanged because their frontier equals event time.
        mutated["event_time_utc"] = event_times.cummax().dt.strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ")
        return super().ingest_log(mutated)

    def read_view(self, grain: str) -> pd.DataFrame:
        view = super().read_view(grain)
        if self._ingested is None:
            return view
        src = self._ingested.copy()
        times = pd.to_datetime(src["event_time_utc"], utc=True)
        fmt = "%Y-%m-%dT%H:00:00Z" if grain == "hourly" else "%Y-%m-%d"
        src["bucket"] = times.dt.strftime(fmt)
        # Arrival order is the frame's own order, tie-broken by processing_order.
        last = (src.sort_values("processing_order", kind="stable")
                   .groupby(["bucket", "entity", "metric"], as_index=False)
                   .last()[["bucket", "entity", "metric", "value", "event_time_utc"]])
        merged = view.merge(last, on=["bucket", "entity", "metric"], how="left")
        got = merged["value"].notna()
        view = view.copy()
        view.loc[got.to_numpy(), "latest_value"] = merged.loc[got, "value"].to_numpy()
        return view


class UlpDriftMutant(ReferenceAdapter):
    """Condition 3: perturb each `sum` by one ULP, freshly at every read.

    One ULP is ~2.2e-16 relative -- three orders of magnitude tighter than the
    contract's epsilon = 1e-9 -- so the epsilon-bounded EMR diagnostics are
    unaffected. Only D3, which compares replays bitwise, can see it. That is the
    point: it isolates determinism from accuracy.
    """

    name = "mutant_ulp_drift"

    def read_view(self, grain: str) -> pd.DataFrame:
        view = super().read_view(grain).copy()
        # Unseeded on purpose: two replays inside one invocation must disagree.
        rng = random.Random()
        view["sum"] = [
            float(np.nextafter(v, np.inf if rng.random() < 0.5 else -np.inf))
            if pd.notna(v) else v
            for v in view["sum"]
        ]
        return view


class DropLateMutant(ReferenceAdapter):
    """Condition 4: drop any record that arrives behind the watermark.

    Maintains watermark = max(event_time seen) - allowed_lateness over the arrival
    sequence and discards records older than it. This is standard windowing
    behaviour in engines that treat allowed lateness as a hard drop, and it makes
    the cumulative view depend on delivery timing.
    """

    name = "mutant_drop_late"

    def __init__(self, allowed_lateness_s: int = 300, **options: Any) -> None:
        super().__init__(**options)
        self.allowed_lateness_s = int(allowed_lateness_s)

    def ingest_log(self, df: pd.DataFrame) -> IngestStats:
        times = pd.to_datetime(df["event_time_utc"], utc=True).to_numpy()
        allowed = np.timedelta64(self.allowed_lateness_s, "s")
        keep: List[bool] = []
        watermark = None
        for t in times:
            if watermark is not None and t < watermark:
                keep.append(False)
                continue
            keep.append(True)
            candidate = t - allowed
            if watermark is None or candidate > watermark:
                watermark = candidate
        return super().ingest_log(df[np.array(keep)])


class IgnoreSupersedesMutant(ReferenceAdapter):
    """Condition 7: apply a restatement as a fresh insert instead of a retraction.

    The corrected value is added to the aggregate while the superseded value stays
    in it, so the summary reflects a multiset that never existed. This is the
    insert-only failure mode D7 exists to detect.
    """

    name = "mutant_ignore_supersedes"

    def ingest_log(self, df: pd.DataFrame) -> IngestStats:
        if self.variant == "restate":
            # Bypass ReferenceAdapter's supersedes resolution by running the rows
            # through the plain (dedup) path.
            saved, self.variant = self.variant, "dedup"
            try:
                return super().ingest_log(df)
            finally:
                self.variant = saved
        return super().ingest_log(df)



class RecoveryLossMutant(ReferenceAdapter):
    """Condition 5: discard acknowledged state at the simulated restart."""

    name = "mutant_recovery_loss"

    def simulate_restart(self) -> None:
        from src.engine.exact_engine import ExactEngine
        self._engine = ExactEngine()


class TopKRankMutant(ReferenceAdapter):
    """Condition 6: corrupt one rank score per bucket/metric group."""

    name = "mutant_topk_rank"

    def mutate_topk(self, table: pd.DataFrame) -> pd.DataFrame:
        out = table.copy()
        if out.empty:
            return out
        for _, indices in out.groupby(["bucket", "metric"]).groups.items():
            target = list(indices)[0]
            out.at[target, "entity"] = f"{out.at[target, 'entity']}__rank_corrupt"
        return out


MUTANT_CLASSES = (
    NoDedupMutant,
    OrderSensitiveMutant,
    UlpDriftMutant,
    DropLateMutant,
    RecoveryLossMutant,
    TopKRankMutant,
    IgnoreSupersedesMutant,
)

MUTANT_NAMES = tuple(cls.name for cls in MUTANT_CLASSES)
