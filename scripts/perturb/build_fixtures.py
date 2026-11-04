#!/usr/bin/env python3
"""
build_fixtures.py -- Build rate-based perturbation fixtures (Step 3).

For every domain, read the frozen normalized log and emit a set of perturbed
replay logs -- one per diagnostic -- into data/fixtures/<domain>/. The
perturbations are parameterized as RATES read from config/default.yaml (never
hard-coded here), so a fixture built for a 1k-row log and a 1M-row log inject
the same *fraction* of anomalies and stay comparable across workload sizes
This replaces the retired absolute-count builders
(build_duplicate_parquet.py / build_d2_fixture.py: 67 duplicates, +25 injected).

Fixtures produced per domain (all share the base log's column order + null
ingest_time; ingest time is assigned at REPLAY, not baked in):
  d1_dup.parquet     D1 -- source-identity duplicate suppression.
                     A `duplicate_rate` fraction of rows is copied byte-for-byte
                     (same dedupe_id) and re-inserted at a later, uniformly
                     random arrival position, so many duplicates cross a window
                     boundary -- the regime that exposes residual/cross-window
                     dedup failures (e.g. ksqlDB LATEST_BY_OFFSET). A correct
                     engine collapses every copy: aggregates == base.
  d2_reorder.parquet D2 -- arrival-order invariance.
                     Arrival order is locally shuffled within non-overlapping
                     windows of `reorder_window` positions; event_time is
                     untouched. Same multiset of rows, different arrival order.
                     An order-invariant engine yields aggregates == base.
  d4_late.parquet    D4 -- delayed-arrival inclusion in final state.
                     Historical archive order is not an arrival-time signal, so
                     D4 first builds an event-time-ordered control and relocates
                     a `late_rate` fraction within bounded arrival windows. The
                     multiset is unchanged; D4 does not claim watermark or
                     trigger conformance.
  d7_restate.parquet D7 -- retraction-consistent summarization (docs/SEMANTICS.md, Restatements).
                     A `restate_rate` fraction of base observations get a later
                     *corrected value* (new value, new dedupe_id,
                     source_record_type='restatement') that points at the row it
                     supersedes via the `supersedes` column (= that base row's
                     dedupe_id). Targeting a specific observation by id -- not a
                     timestamp cell -- makes D7 valid on all 5 domains, including
                     those where many events share a timestamp. An engine that
                     treats the restatement as an insert double-counts. Adds
                     `restate_rate * base_rows` rows (the bounded-deletion count).

Determinism: (base-log digest, seed, rates) -> bit-identical
parquet + manifest. All randomness comes from numpy Generators seeded from the
config `seed` with a per-fixture sub-stream, so re-running yields identical
sha256. D3 (repeated replay) and D5 (crash/restart) need no perturbed input --
they replay the base log -- so no fixture is emitted for them here. D6 (top-k)
is a semantics-level diagnostic over the base log.

Inputs  : data/normalized/<domain>/log.parquet (+ manifest.json) ; config/default.yaml
Outputs : data/fixtures/<domain>/{d1_dup,d2_reorder,d4_late,d7_restate}.parquet + manifest.json ; PASS/FAIL
Runtime : a few seconds per 1M-row domain
Run as  : python scripts/perturb/build_fixtures.py            # all 5 domains
          python scripts/perturb/build_fixtures.py --domain finance_markets
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.schema import (  # noqa: E402
    NORMALIZED_COLUMNS, PROCESSING_ORDER, INGEST_TIME, DEDUPE_ID,
    ENTITY, METRIC, VALUE, EVENT_TIME, SOURCE_RECORD_TYPE, SUPERSEDES, build_dedupe_id,
)
from src.schema.benchmark import effective_perturbation, row_positions  # noqa: E402
from src.provenance import run_provenance  # noqa: E402

ALL_DOMAINS = [
    "environment_airquality",
    "transportation_taxi",
    "datacenter_drives",
    "finance_markets",
    "sports_football",
]

# Per-fixture sub-stream ids -> independent, reproducible Generators from one seed.
_SUBSTREAM = {"d1": 1, "d2": 2, "d4": 4, "d7": 7}
D4_CHECK_SIZES = (10_000, 50_000, 200_000, 500_000, 1_000_000)


# --- provenance helpers (mirror scripts/ingest/_ingest_common.py) -------------
def _git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _rng(seed: int, substream: int) -> np.random.Generator:
    """A Generator whose stream is fully determined by (seed, substream)."""
    return np.random.Generator(np.random.PCG64(np.random.SeedSequence([seed, substream])))


def _load_config(path: Path) -> Dict[str, Any]:
    import yaml
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _finalize(df: pd.DataFrame, extra_cols: tuple = ()) -> pd.DataFrame:
    """Canonical column order, contiguous processing_order over the new arrival
    sequence, null ingest_time (assigned at replay). Row order in the frame is
    the arrival order. `extra_cols` appends non-canonical columns (D7 supersedes).

    Callers may hand in a frame that has already been through
    `canonical_input`, which keeps only what the contract reads and so drops the
    optional source metadata. run_d7s_sweep does exactly that. Those columns are
    restored as null here, the same way ingest_time already is: none of them
    takes part in source identity, bucketing, or the reduction, and each is read
    with `.get` downstream, so a null is the honest value rather than a
    placeholder. Requiring them instead made the D7S sweep die on a frame that
    was otherwise entirely valid.
    """
    out = df.reset_index(drop=True).copy()
    out[PROCESSING_ORDER] = np.arange(len(out), dtype="int64")
    out[INGEST_TIME] = None
    for column in NORMALIZED_COLUMNS:
        if column not in out.columns:
            out[column] = None
    return out[NORMALIZED_COLUMNS + list(extra_cols)]


# --- the three perturbations --------------------------------------------------
def perturb_d1_duplicates(base: pd.DataFrame, rate: float, rng: np.random.Generator,
                          window: Optional[int] = None) -> tuple[pd.DataFrame, int]:
    """Copy a `rate` fraction of rows byte-for-byte and re-insert each at a later
    arrival slot within `window` slots. Returns (fixture, n_injected).

    The window is what makes the declared rate reachable in a prefix. Drawing the
    copy uniformly over the whole remaining stream requires BOTH the original and
    the copy to fall inside a prefix, so a prefix of fraction f realises only
    f*rate: at 50k rows of the 1M taxi log that was 14 duplicate rows against the
    ~250 the configured 0.005 asks for. A bounded delay realises the rate in every
    prefix longer than the window, the way restate_window does for D7.
    """
    n = len(base)
    k = int(round(n * rate))
    if k <= 0:
        return _finalize(base), 0

    w = n if window is None else max(1, int(window))
    src_positions = np.sort(rng.choice(n, size=k, replace=False))
    # Arrival key: originals occupy integer slots 0..n-1; each copy lands at a
    # slot strictly after its original and at most `w` slots later, so placement
    # still spans same-bucket and cross-bucket offsets.
    dup_keys = np.array(
        [rng.integers(int(p) + 1, min(int(p) + w, n) + 1) + rng.random()
         for p in src_positions],
        dtype="float64",
    )
    base_keys = np.arange(n, dtype="float64")

    rows = pd.concat([base, base.iloc[src_positions]], ignore_index=True)
    keys = np.concatenate([base_keys, dup_keys])
    # Stable sort by arrival key keeps ties in (originals-then-duplicates) order.
    order = np.argsort(keys, kind="stable")
    return _finalize(rows.iloc[order]), k


def _event_time_inversion_rate(df: pd.DataFrame) -> float:
    """Fraction of adjacent arrival pairs whose event times run backwards.

    This is the only honest measure of how hard D2 is pushing: arrival order that
    is still event-time monotonic tests nothing, no matter how many positions were
    permuted.
    """
    if len(df) < 2:
        return 0.0
    t = pd.to_datetime(df[EVENT_TIME], utc=True).astype("int64").to_numpy()
    return float((np.diff(t) < 0).sum()) / float(len(df) - 1)


def _behind_watermark(df: pd.DataFrame) -> int:
    """Records arriving with an event time older than the running maximum.

    This, not the requested late_rate, is what D4 has to work with: a fixture
    whose replayed prefix contains none of these is testing nothing.
    """
    if len(df) < 2:
        return 0
    t = pd.to_datetime(df[EVENT_TIME], utc=True).astype("int64").to_numpy()
    return int((t < np.maximum.accumulate(t)).sum())


def _beyond_allowed_lateness(df: pd.DataFrame, allowed_lateness_s: int) -> int:
    """Count rows that the configured drop-late fault would discard."""
    if len(df) < 2:
        return 0
    times = pd.to_datetime(df[EVENT_TIME], utc=True).astype("int64").to_numpy()
    prior_max = np.maximum.accumulate(times)
    allowance_ns = int(allowed_lateness_s) * 1_000_000_000
    return int((times[1:] < prior_max[:-1] - allowance_ns).sum())


def _injected_beyond_allowed_lateness(
    df: pd.DataFrame,
    injected_ids: set[str],
    allowed_lateness_s: int,
) -> int:
    """Count selected D4 targets, not naturally late source rows."""
    if len(df) < 2 or not injected_ids:
        return 0
    times = pd.to_datetime(df[EVENT_TIME], utc=True).astype("int64").to_numpy()
    prior_max = np.maximum.accumulate(times)
    allowance_ns = int(allowed_lateness_s) * 1_000_000_000
    beyond = np.zeros(len(df), dtype=bool)
    beyond[1:] = times[1:] < prior_max[:-1] - allowance_ns
    selected = df[DEDUPE_ID].astype(str).isin(injected_ids).to_numpy()
    return int((beyond & selected).sum())


def _select_rows(df: pd.DataFrame, rows: int,
                 mode: str = "prefix") -> pd.DataFrame:
    return df.iloc[row_positions(len(df), rows, mode)]


def _d4_control(base: pd.DataFrame) -> pd.DataFrame:
    """Derive the on-time D4 control from a historical archive.

    Storage order across files, symbols, stations, and daily snapshots is not a
    measured arrival sequence. D4 therefore isolates the injected delay by using
    a stable event-time order with the contract's dedupe-id tie break.
    """
    ordered = base.sort_values([EVENT_TIME, DEDUPE_ID], kind="mergesort")
    return _finalize(ordered)


def _d4_scale_counts(
    control: pd.DataFrame,
    fixture: pd.DataFrame,
    injected_ids: set[str],
    allowed_lateness_s: int,
) -> Dict[str, Dict[str, int]]:
    checks: Dict[str, Dict[str, int]] = {}
    for requested in D4_CHECK_SIZES:
        if requested > len(control):
            continue
        selected_control = _select_rows(control, requested, "uniform")
        selected_fixture = _select_rows(fixture, requested, "uniform")
        control_beyond = _beyond_allowed_lateness(
            selected_control, allowed_lateness_s)
        fixture_beyond = _beyond_allowed_lateness(
            selected_fixture, allowed_lateness_s)
        injected_beyond = _injected_beyond_allowed_lateness(
            selected_fixture, injected_ids, allowed_lateness_s)
        checks[str(requested)] = {
            "control_beyond_allowed_lateness": control_beyond,
            "fixture_beyond_allowed_lateness": fixture_beyond,
            "net_beyond_allowed_lateness": fixture_beyond - control_beyond,
            "injected_targets_present": int(
                selected_fixture[DEDUPE_ID].astype(str).isin(injected_ids).sum()),
            "injected_beyond_allowed_lateness": injected_beyond,
        }
    return checks


def perturb_d2_reorder(base: pd.DataFrame, window: int, rng: np.random.Generator,
                       mode: str = "event_time") -> pd.DataFrame:
    """Perturb arrival order so that event time is delivered out of order.

    mode="positional" (the original) shuffles non-overlapping blocks of `window`
    ROW POSITIONS. That is only a reorder if the block spans more than one
    distinct event time, which is false on high-frequency domains: a 100-position
    window covers ~1 timestamp on sports_football (115 rows/timestamp) and less
    than one on datacenter_drives (all 50k rows share a daily timestamp), so the
    fixture came out event-time monotonic and D2 passed vacuously.

    mode="event_time" measures the window in DISTINCT EVENT TIMES instead: rows
    are displaced by up to `window` distinct-timestamp steps, so the realised
    inversion rate is a property of the requested window rather than of the
    domain's tie density. Where a domain has a single distinct timestamp there is
    no event-time order to invert and D2 is genuinely undefined -- the manifest's
    recorded inversion rate makes that visible instead of silent.
    """
    n = len(base)
    if not window or window <= 1:
        return _finalize(base)

    if mode == "positional":
        order = np.arange(n)
        for start in range(0, n, window):
            block = order[start:start + window].copy()
            order[start:start + window] = block[rng.permutation(len(block))]
        return _finalize(base.iloc[order])

    if mode != "event_time":
        raise ValueError(f"unknown reorder mode {mode!r}")

    # Rank each row by its distinct event time, then jitter that rank by up to
    # +/- window steps. Ties keep a stable relative order under the sort below.
    times = pd.to_datetime(base[EVENT_TIME], utc=True).astype("int64").to_numpy()
    rank = pd.Series(times).rank(method="dense").to_numpy() - 1.0
    jitter = rng.uniform(-float(window), float(window), size=n)
    order = np.argsort(rank + jitter, kind="stable")
    return _finalize(base.iloc[order])


def perturb_d4_late(control: pd.DataFrame, rate: float, rng: np.random.Generator,
                    window: int | None = None) -> tuple[pd.DataFrame, int, set[str]]:
    """Delay a `rate` fraction of an ordered control behind the watermark.

    `window` bounds the delay: a late row is re-inserted within `window` arrival
    slots of where it belongs, rather than after every on-time arrival.

    The bound is not cosmetic. The fixture is built over the full 1M-row log while
    diagnostics replay a leading prefix of it, so tail placement put every delayed
    row past position ~1M and a 50k prefix truncated the entire perturbation away:
    three of five domains showed ZERO records behind the watermark at 50k, and D4
    was running on an unperturbed log while reporting a pass. (This is the same
    prefix-truncation failure already fixed for D7's restatements and D2's
    reordering; D4 was the third instance.)

    A bounded delay is also the more faithful model -- a late arrival is late by
    some bounded lag, not by the remaining length of the stream.
    `window=None` restores the original tail placement.

    Returns (fixture, n_late, injected_dedupe_ids).
    """
    n = len(control)
    k = int(round(n * rate))
    if k <= 0:
        return _finalize(control), 0, set()
    late_positions = np.sort(rng.choice(n, size=k, replace=False))
    injected_ids = set(control.iloc[late_positions][DEDUPE_ID].astype(str))

    if window is None:
        late_mask = np.zeros(n, dtype=bool)
        late_mask[late_positions] = True
        on_time = control.iloc[~late_mask]
        late = control.iloc[late_mask]       # appended after all on-time arrivals
        return (_finalize(pd.concat([on_time, late], ignore_index=True)),
                k, injected_ids)

    # Sort by a delayed arrival key: on-time rows keep their slot, delayed rows
    # move forward by 1..window slots. A stable sort preserves relative order
    # among rows that land on the same key.
    delay = np.zeros(n, dtype=np.int64)
    delay[late_positions] = rng.integers(1, int(window) + 1, size=k)
    order = np.argsort(np.arange(n, dtype=np.int64) + delay, kind="stable")
    return _finalize(control.iloc[order].reset_index(drop=True)), k, injected_ids


def perturb_d7_restate(base: pd.DataFrame, rate: float, rng: np.random.Generator,
                       window: int | None = None) -> tuple[pd.DataFrame, int]:
    """D7: for a `rate` fraction of base observations, synthesize a later
    *restatement* row carrying a corrected value. The restatement points at the
    row it supersedes by that row's dedupe_id (the `supersedes` column), carries a
    new value + new dedupe_id (so it is not a D1 duplicate), source_record_type=
    'restatement', and lands at a later arrival slot (larger processing_order than
    the superseded row). Because it targets a specific observation by id -- not a
    (entity, metric, event_time) cell -- it works even where many events share a
    timestamp (finance/taxi/football). Base rows carry supersedes=null.
    Returns (fixture, n_restatements). See docs/SEMANTICS.md, Restatements.

    `window` bounds the correction delay: a restatement lands within `window`
    arrival slots of the row it supersedes, instead of uniformly over the whole
    remaining stream. This matters because the fixture is built over the full 1M
    log while diagnostics replay a leading prefix of it. Under uniform placement
    a prefix keeps the base row but almost always loses its restatement, so a
    50k-row prefix of the 1M fixture realised only 7 restatements against a
    configured rate of 1% -- D7 was, in effect, running on an unperturbed log.
    A bounded delay also models corrections more faithfully: a correction follows
    its observation by a bounded lag, it does not arrive an arbitrary distance
    later. `window=None` restores the original unbounded placement."""
    n = len(base)
    base = base.reset_index(drop=True).copy()
    base[SUPERSEDES] = None

    # A base log MAY contain byte-identical rows (same dedupe_id) -- e.g. repeated
    # identical ticks: 641k of finance_markets' 1M rows. Content-dedup collapses
    # each such group to the row with the LOWEST processing_order, so the view holds
    # one observation per DISTINCT dedupe_id. Restate a `rate` fraction of those
    # observations, targeting the surviving row: this keeps every `supersedes`
    # pointer unambiguous, guarantees no two restatements target the same
    # observation, and makes `rate` exactly the bounded-deletion ratio alpha the
    # KLL±/SpaceSaving± space bound is stated in (deletions / maintained items).
    surviving = (base.sort_values(PROCESSING_ORDER, kind="mergesort")
                     .drop_duplicates(subset=[DEDUPE_ID], keep="first"))
    m = int(len(surviving))
    k = int(round(m * rate))
    if k <= 0:
        return _finalize(base, extra_cols=(SUPERSEDES,)), 0

    picked = surviving.iloc[np.sort(rng.choice(m, size=k, replace=False))]
    src_positions = picked.index.to_numpy()      # positions in `base` (arrival order)
    orig = picked.reset_index(drop=True)
    v0 = orig[VALUE].to_numpy(dtype=float)
    sign = rng.choice(np.array([-1.0, 1.0]), size=k)
    frac = rng.uniform(0.05, 0.5, size=k)
    mag = np.maximum(np.abs(v0), 1.0)
    v1 = v0 + sign * frac * mag            # corrected value, guaranteed != v0

    restate = orig.copy()
    restate[SUPERSEDES] = orig[DEDUPE_ID].to_numpy()   # point at the base row (by its id)
    restate[VALUE] = v1
    restate[SOURCE_RECORD_TYPE] = "restatement"
    restate[DEDUPE_ID] = [
        build_dedupe_id(str(superseded), str(metric), namespace="d7-restatement")
        for superseded, metric in zip(orig[DEDUPE_ID], orig[METRIC])
    ]

    # Arrival placement: each restatement lands after its original, uniformly
    # within `window` slots of it (or over the whole remaining stream when
    # window is None). Some still cross a bucket boundary, which is the case D7
    # cares about.
    base_keys = np.arange(n, dtype="float64")
    rest_keys = np.array(
        [rng.integers(int(p) + 1,
                      n + 1 if window is None else min(int(p) + 1 + int(window), n + 1))
         + rng.random()
         for p in src_positions],
        dtype="float64",
    )
    rows = pd.concat([base, restate], ignore_index=True)
    keys = np.concatenate([base_keys, rest_keys])
    order = np.argsort(keys, kind="stable")
    return _finalize(rows.iloc[order], extra_cols=(SUPERSEDES,)), k


# --- self-check invariants ----------------------------------------------------
def _contiguous_order(df: pd.DataFrame) -> bool:
    return df[PROCESSING_ORDER].tolist() == list(range(len(df)))


def _multiset(df: pd.DataFrame) -> Counter:
    return Counter(df[DEDUPE_ID].tolist())


def _check(base: pd.DataFrame, d4_control: pd.DataFrame,
           fx: Dict[str, pd.DataFrame],
           k_dup: int, k_late: int, k_restate: int,
           allowed_lateness_s: int, d4_injected_ids: set[str]) -> List[str]:
    """Return a list of failure messages; empty == PASS."""
    fails: List[str] = []
    base_ms = _multiset(base)
    n = len(base)

    if not _contiguous_order(d4_control):
        fails.append("d4_control: processing_order not contiguous 0..N-1")
    if _multiset(d4_control) != base_ms:
        fails.append("d4_control: dedupe_id multiset differs from base")
    if _event_time_inversion_rate(d4_control) != 0.0:
        fails.append("d4_control: event time is not monotonic")

    for name, df in fx.items():
        if not _contiguous_order(df):
            fails.append(f"{name}: processing_order not contiguous 0..N-1")
        # d7_restate carries the extra supersedes column; the others are canonical.
        expected = NORMALIZED_COLUMNS + ([SUPERSEDES] if name == "d7_restate.parquet" else [])
        if list(df.columns) != expected:
            fails.append(f"{name}: column order != expected schema")

    # D1: base + k_dup rows; every extra copy is an id that already existed.
    d1 = fx["d1_dup.parquet"]
    if len(d1) != n + k_dup:
        fails.append(f"d1_dup: rows {len(d1)} != base {n} + injected {k_dup}")
    extra = _multiset(d1) - base_ms
    if sum(extra.values()) != k_dup:
        fails.append(f"d1_dup: injected-copy count {sum(extra.values())} != {k_dup}")
    if not set(extra).issubset(set(base_ms)):
        fails.append("d1_dup: injected duplicates introduced a new dedupe_id")

    # D2/D4: pure permutation / relocation -> identical multiset, same row count.
    for name in ("d2_reorder.parquet", "d4_late.parquet"):
        df = fx[name]
        if len(df) != n:
            fails.append(f"{name}: rows {len(df)} != base {n}")
        if _multiset(df) != base_ms:
            fails.append(f"{name}: dedupe_id multiset changed (not a pure reorder)")

    # Absolute lateness is not evidence of injection. Check the delayed fixture
    # against its ordered control and inspect injected targets at every paper scale.
    for rows, counts in _d4_scale_counts(
            d4_control, fx["d4_late.parquet"], d4_injected_ids,
            allowed_lateness_s).items():
        if counts["control_beyond_allowed_lateness"]:
            fails.append(
                f"d4_late: {rows}-row control selection has "
                f"{counts['control_beyond_allowed_lateness']} row(s) beyond the allowance")
        if k_late > 0 and counts["injected_beyond_allowed_lateness"] == 0:
            fails.append(
                f"d4_late: {rows}-row selection contains no injected target "
                "beyond allowed lateness")
        if (k_late > 0
                and counts["fixture_beyond_allowed_lateness"]
                    <= counts["control_beyond_allowed_lateness"]):
            fails.append(
                f"d4_late: {rows}-row fixture does not increase "
                "beyond-allowance rows over control")

    # D7 (supersedes model): base + k_restate rows; each restatement has a NEW
    # dedupe_id, points at an existing base dedupe_id via `supersedes`, and arrives
    # after the row it supersedes. Works regardless of cell multiplicity.
    d7 = fx["d7_restate.parquet"]
    base_ids = set(base_ms)
    if len(d7) != n + k_restate:
        fails.append(f"d7_restate: rows {len(d7)} != base {n} + restatements {k_restate}")
    r = d7[d7[SOURCE_RECORD_TYPE] == "restatement"]
    if len(r) != k_restate:
        fails.append(f"d7_restate: restatement rows {len(r)} != {k_restate}")
    if not set(r[DEDUPE_ID]).isdisjoint(base_ids):
        fails.append("d7_restate: a restatement reused an existing dedupe_id (not a correction)")
    if not set(r[SUPERSEDES].dropna()).issubset(base_ids):
        fails.append("d7_restate: a restatement supersedes an unknown dedupe_id")
    if r[SUPERSEDES].isna().any():
        fails.append("d7_restate: a restatement has a null supersedes pointer")
    if r[SUPERSEDES].duplicated().any():
        fails.append("d7_restate: two restatements share a supersedes pointer (ambiguous correction)")
    base_rows = d7[d7[SOURCE_RECORD_TYPE] != "restatement"]
    if base_rows[SUPERSEDES].notna().any():
        fails.append("d7_restate: a base row carries a non-null supersedes")
    # Each restatement must arrive after the source observation it supersedes.
    # Exact source-identity duplicates retain the lowest processing order.
    if k_restate > 0:
        surviving_po = (base_rows.sort_values(PROCESSING_ORDER, kind="mergesort")
                        .drop_duplicates(subset=[DEDUPE_ID], keep="first")
                        .set_index(DEDUPE_ID)[PROCESSING_ORDER])
        late = sum(1 for sup, po_r in zip(r[SUPERSEDES], r[PROCESSING_ORDER])
                   if surviving_po.get(sup, -1) >= po_r)
        if late:
            fails.append(f"d7_restate: {late} restatement(s) do not arrive after the superseded row")
    return fails


# --- per-domain driver --------------------------------------------------------
def build_domain(domain: str, cfg: Dict[str, Any], norm_root: Path, out_root: Path) -> Dict[str, Any]:
    log_path = norm_root / domain / "log.parquet"
    if not log_path.exists():
        raise FileNotFoundError(
            f"{log_path} not found -- run the Step 2 ingest for '{domain}' first "
            f"(see reproduce_all.sh Stage 2)."
        )
    base = pd.read_parquet(log_path)
    base_sha = _sha256_bytes(log_path.read_bytes())

    seed = int(cfg["seed"])
    p = effective_perturbation(ROOT, domain, cfg["perturbation"])
    dup_rate = float(p["duplicate_rate"])
    dup_window = p.get("duplicate_window")
    dup_window = int(dup_window) if dup_window is not None else None
    window = int(p["reorder_window"])
    reorder_mode = str(p.get("reorder_mode", "event_time"))
    late_rate = float(p["late_rate"])
    late_window = p.get("late_window")
    late_window = int(late_window) if late_window is not None else None
    allowed_lateness_s = int(p["allowed_lateness_s"])
    restate_rate = float(p["restate_rate"])
    restate_window = p.get("restate_window")
    restate_window = int(restate_window) if restate_window is not None else None

    d1, k_dup = perturb_d1_duplicates(base, dup_rate, _rng(seed, _SUBSTREAM["d1"]),
                                      window=dup_window)
    d2 = perturb_d2_reorder(base, window, _rng(seed, _SUBSTREAM["d2"]),
                            mode=reorder_mode)
    d4_control = _d4_control(base)
    d4, k_late, d4_injected_ids = perturb_d4_late(
        d4_control, late_rate, _rng(seed, _SUBSTREAM["d4"]), window=late_window)
    d7, k_restate = perturb_d7_restate(base, restate_rate, _rng(seed, _SUBSTREAM["d7"]),
                                       window=restate_window)

    fixtures = {
        "d1_dup.parquet": d1,
        "d2_reorder.parquet": d2,
        "d4_late.parquet": d4,
        "d7_restate.parquet": d7,
    }

    fails = _check(base, d4_control, fixtures, k_dup, k_late, k_restate,
                   allowed_lateness_s, d4_injected_ids)

    out_dir = out_root / domain
    out_dir.mkdir(parents=True, exist_ok=True)
    fixture_meta: List[Dict[str, Any]] = []
    diag_of = {"d1_dup.parquet": "D1", "d2_reorder.parquet": "D2",
               "d4_late.parquet": "D4", "d7_restate.parquet": "D7"}
    for name, df in fixtures.items():
        fpath = out_dir / name
        df.to_parquet(fpath, index=False)
        entry: Dict[str, Any] = {
            "diagnostic": diag_of[name],
            "name": name,
            "sha256": _sha256_bytes(fpath.read_bytes()),
            "rows": int(len(df)),
        }
        if name == "d1_dup.parquet":
            entry.update(perturbation="duplicate", rate=dup_rate,
                         window=dup_window, injected_duplicates=k_dup)
        elif name == "d2_reorder.parquet":
            # The realised inversion rate is recorded because the requested
            # window alone does not say whether anything was actually reordered.
            entry.update(perturbation="bounded_reorder", reorder_window=window,
                         reorder_mode=reorder_mode,
                         event_time_inversion_rate=_event_time_inversion_rate(df),
                         distinct_event_times=int(df[EVENT_TIME].nunique()))
        elif name == "d4_late.parquet":
            # As with D2, the requested rate does not say whether the prefix a
            # diagnostic replays actually contains anything behind the watermark,
            # so the realised inversion rate is recorded alongside it.
            prefix = _select_rows(df, 50_000, "uniform")
            control_prefix = _select_rows(d4_control, 50_000, "uniform")
            control_beyond = _beyond_allowed_lateness(
                control_prefix, allowed_lateness_s)
            fixture_beyond = _beyond_allowed_lateness(
                prefix, allowed_lateness_s)
            entry.update(perturbation="late_arrival", rate=late_rate,
                         late_window=late_window, late_rows=k_late,
                         allowed_lateness_s=allowed_lateness_s,
                         control_order="event_time_utc,dedupe_id",
                         injected_target_ids_sha256=_sha256_bytes(
                             "\n".join(sorted(d4_injected_ids)).encode("utf-8")),
                         event_time_inversion_rate=_event_time_inversion_rate(df),
                         behind_watermark=_behind_watermark(df),
                         behind_watermark_first_50k=_behind_watermark(prefix),
                         control_beyond_allowed_lateness=_beyond_allowed_lateness(
                             d4_control, allowed_lateness_s),
                         control_beyond_allowed_lateness_at_50k=control_beyond,
                         beyond_allowed_lateness=_beyond_allowed_lateness(
                             df, allowed_lateness_s),
                         beyond_allowed_lateness_at_50k=fixture_beyond,
                         injected_targets_present_at_50k=int(
                             prefix[DEDUPE_ID].astype(str).isin(d4_injected_ids).sum()),
                         injected_beyond_allowed_lateness_at_50k=
                             _injected_beyond_allowed_lateness(
                                 prefix, d4_injected_ids, allowed_lateness_s),
                         net_beyond_allowed_lateness_at_50k=
                             fixture_beyond - control_beyond,
                         scale_selection="uniform_full_log",
                         scale_checks=_d4_scale_counts(
                             d4_control, df, d4_injected_ids,
                             allowed_lateness_s))
        else:
            # base_observations = distinct source-observation ids held by the view;
            # alpha = restatements / base_observations == restate_rate.
            n_obs = int(base[DEDUPE_ID].nunique())
            entry.update(perturbation="restatement", rate=restate_rate,
                         restate_window=restate_window,
                         restatements=k_restate, base_observations=n_obs,
                         alpha=(k_restate / n_obs if n_obs else 0.0))
        fixture_meta.append(entry)

    manifest = {
        "domain": domain,
        # repo-relative so the manifest is portable + carries no home path
        "base_log": os.path.relpath(log_path, ROOT),
        "base_sha256": base_sha,
        "base_rows": int(len(base)),
        "seed": seed,
        "config_sha256": run_provenance(ROOT)["benchmark_config_sha256"],
        "domain_config": os.path.relpath(
            ROOT / "config" / "domains" / f"{domain}.yaml", ROOT),
        "rates": {
            "duplicate_rate": dup_rate,
            "reorder_window": window,
            "late_rate": late_rate,
            "late_window": late_window,
            "allowed_lateness_s": allowed_lateness_s,
            "restate_rate": restate_rate,
            "restate_window": restate_window,
        },
        "columns": NORMALIZED_COLUMNS,
        "dedupe_identity": "source_observation",
        **run_provenance(ROOT),
        "written_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "fixtures": fixture_meta,
        "self_check": "PASS" if not fails else "FAIL",
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    manifest["_fails"] = fails
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--domain", action="append", choices=ALL_DOMAINS,
                        help="Domain to build (repeatable). Default: all 5.")
    parser.add_argument("--config", default=str(ROOT / "config/default.yaml"),
                        help="Perturbation-rate config (default: config/default.yaml).")
    parser.add_argument("--normalized-root", default=str(ROOT / "data/normalized"),
                        help="Root holding <domain>/log.parquet.")
    parser.add_argument("--out-root", default=str(ROOT / "data/fixtures"),
                        help="Root for <domain>/ fixture outputs.")
    args = parser.parse_args()

    domains = args.domain or ALL_DOMAINS
    cfg = _load_config(Path(args.config))
    norm_root = Path(args.normalized_root)
    out_root = Path(args.out_root)

    all_ok = True
    for domain in domains:
        manifest = build_domain(domain, cfg, norm_root, out_root)
        fails = manifest.pop("_fails")
        status = "PASS" if not fails else "FAIL"
        all_ok = all_ok and not fails
        print(f"\n==== {domain} [{status}] ====")
        for fx in manifest["fixtures"]:
            extra = {k: v for k, v in fx.items() if k not in ("diagnostic", "name", "sha256", "rows")}
            print(f"  {fx['diagnostic']}  {fx['name']:<18} rows={fx['rows']:<9} "
                  f"sha256={fx['sha256'][:12]}...  {extra}")
        print(f"  manifest: {out_root / domain / 'manifest.json'}")
        for msg in fails:
            print(f"  FAIL: {msg}")

    print(f"\n{'PASS' if all_ok else 'FAIL'}: built fixtures for {len(domains)} domain(s) under {out_root}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
