#!/usr/bin/env python3
"""
run_diagnostic.py -- drive one engine adapter through the diagnostic suite and
write the result to runs/ (Step 6). Each diagnostic ingests a fixture (or the
base log) through the engine's materialized view and compares it to the oracle.

Implemented now (works for any adapter):
  D1  source-identity duplicate suppression    d1_dup.parquet     EMR (dedup view)
  D2  arrival-order invariance                d2_reorder.parquet EMR (dedup view)
  D3  accurate deterministic summation        base log x2        DDR (oracle
      equivalence plus bitwise intra-invocation AND cross-invocation replay via
      runs/_ledger/d3_determinism/)
  D4  bounded late-arrival inclusion           d4_late.parquet    EMR (dedup view)
  D6  downstream top-k over maintained views   base log          TOPK (set + rank EMR)
  D7  retraction-consistent summarization     d7_restate.parquet EMR (restate view)
  D7S retraction-consistent approx. summaries d7_restate.parquet QER (KLL± vs KLL;
      engine-independent -- runs once per domain, on the reference row)

Each EMR diagnostic also runs its *contrast* variant when the engine supports it
(naive for D1/D2/D4, dedup for D7) to capture the design-space cell: the naive
view is expected to FAIL, the patched view to PASS -- and `smallest_patch` names
the recovering change. D6 ranks entities per (bucket, metric) by `d6.rank_metric`
(config) and compares the top-`d6.k` SET and RANK to the oracle; it is sensitive
to numeric error (float32 means can flip near-ties -> rank_emr).

Known signatures provide attribution but never alter verdicts. Only clean
online--oracle equivalence passes.

D5 (crash/restart fault injection) has its own runner --
scripts/run/run_d5_recovery.py (scripts/run/run_d5_recovery.py).

Inputs  : data/fixtures/<domain>/*.parquet, data/normalized/<domain>/log.parquet
Outputs : runs/<UTC>_<domain>_<engine>_<diag>/summary.json + log.txt ; PASS/FAIL
Runtime : seconds (reference/flink/spark) to minutes (containers), per diagnostic
Run as  : python scripts/run/run_diagnostic.py --engine reference --diagnostic D1
          python scripts/run/run_diagnostic.py --engine reference --diagnostic all
          python scripts/run/run_diagnostic.py --engine all --domain all --diagnostic all
          python scripts/run/run_diagnostic.py --engine ksqldb --domain finance_markets --diagnostic D7 --co-oracle

`--engine all` loops every engine and `--domain all` every domain (continuing on
failure), so one invocation can sweep the whole grid; the full run matrix (Step 7)
adds repeats + scaling on top. `--co-oracle` also builds the independent DuckDB
oracle and requires pandas==DuckDB before trusting the comparison -- so a
diagnostic never rests on a single oracle implementation.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import struct
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.adapters import ADAPTER_NAMES, get_adapter, canonical_input  # noqa: E402
from src.schema.artifacts import artifact_sha256, read_parquet_prefix  # noqa: E402
from src.schema.benchmark import effective_perturbation, row_positions  # noqa: E402
from src.diagnostics.findings import match_known_finding  # noqa: E402
from src.oracle.cache import (build_or_load_hopping_oracle,
                              build_or_load_oracle)  # noqa: E402
from src.oracle.comparator import compare_bucket_tables  # noqa: E402
from src.provenance import run_provenance  # noqa: E402


@dataclass
class Diag:
    fixture: Optional[str]      # fixture filename, or None => base log
    primary: str                # variant whose PASS/FAIL is the verdict
    contrast: Optional[str]     # failing-view variant for the design cell
    restatement_aware: bool     # oracle mode
    metric: str
    desc: str
    smallest_patch: Optional[str] = None
    # The idiomatic arm: the formulation a query author would actually
    # write for this engine, run beside the benchmark-supplied repair. It is
    # reported only -- `pass` stays the repair's verdict -- so promotion,
    # cohort validation and every existing table are unaffected. Its purpose is
    # to answer a question the repair cannot: are the failure modes
    # in Table 11 absent because engines are correct, or because the benchmark
    # handed them the fix?
    idiomatic: Optional[str] = None


DIAGNOSTICS: Dict[str, Diag] = {
    # idiomatic="arrival_latest" on the three order-perturbing fixtures: their
    # interventions all change arrival order relative to event time, so an
    # offset-ordered "latest" (LATEST_BY_OFFSET / last()) is exactly the
    # formulation at risk. D7 needs no extra arm -- its "dedup" contrast already
    # is the idiomatic insert-only formulation.
    "D1": Diag("d1_dup.parquet", "dedup", "naive", False, "EMR",
               "source-identity duplicate suppression",
               "DISTINCT ON (dedupe_id)", idiomatic="arrival_latest"),
    "D2": Diag("d2_reorder.parquet", "dedup", None, False, "EMR",
               "arrival-order invariance",
               "event-time bucketing + (event_time, dedupe_id) latest rule",
               idiomatic="arrival_latest"),
    "D3": Diag(None, "dedup", None, False, "DDR",
               "deterministic summation (replay x2)"),
    "D4": Diag("d4_late.parquet", "dedup", None, False, "EMR",
               "late-arrival inclusion in final cumulative state",
               idiomatic="arrival_latest"),
    "D6": Diag(None, "dedup", None, False, "TOPK",
               "downstream top-k over maintained aggregates (set + rank)"),
    "D8": Diag(None, "dedup", None, False, "HOP",
               "hopping-window maintenance (overlapping event-time windows)",
               "per-engine hopping formulation; see adapter.hopping_mode"),
    "D7": Diag("d7_restate.parquet", "restate", "dedup", True, "EMR",
               "retraction-consistent summarization",
               "supersedes-id resolution before aggregation"),
    # D7S is engine-INDEPENDENT (it validates the bounded-deletion reference
    # sketches, the oracle for condition (7)); the sweep runs it only on the
    # reference engine's row so the grid carries it exactly once per domain.
    "D7S": Diag("d7_restate.parquet", "bounded_deletion", "insert_only", True, "QER",
                "retraction-consistent approximate summaries (KLL± vs insert-only KLL)",
                "bounded-deletion sketch (KLL± / SpaceSaving±)"),
}
ELSEWHERE = {
    "D5": "post-recovery state equivalence -- own runner: "
          "python scripts/run/run_d5_recovery.py (durable compose overlay)",
}


def _git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


@lru_cache(maxsize=None)
def _sha256_file(path: Path) -> str:
    return artifact_sha256(path)


@lru_cache(maxsize=8)
def _load_cached(
    path_text: str, max_rows: int, selection_mode: str,
    allow_restatements: bool,
) -> pd.DataFrame:
    path = Path(path_text)
    df = (read_parquet_prefix(path, max_rows)
          if selection_mode == "prefix" else pd.read_parquet(path))
    positions = row_positions(len(df), max_rows, selection_mode)
    df = df.iloc[positions].copy()
    result = canonical_input(df, allow_restatements=allow_restatements)
    result["processing_order"] = np.arange(len(result), dtype="int64")
    result.attrs["_equitable_input_sha256"] = _sha256_file(path)
    result.attrs["_equitable_selection"] = {
        "mode": selection_mode,
        "actual_rows": int(len(result)),
    }
    result.attrs["_equitable_input_key"] = (
        path_text, int(max_rows), selection_mode, int(len(result)), bool(allow_restatements))
    return result


def _load(
    path: Path, max_rows: int, selection_mode: str = "prefix", *,
    allow_restatements: bool = False,
) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found -- build fixtures / ingest first "
            f"(scripts/perturb/build_fixtures.py, scripts/ingest/).")
    # Adjacent engine cells share one immutable canonical selection. Return a deep
    # copy because adapters are allowed to normalize their input in-place.
    return _load_cached(
        str(path.resolve()), int(max_rows), selection_mode,
        bool(allow_restatements)).copy(deep=True)


_ORACLE_CACHE: Dict[tuple, tuple] = {}


def _build_oracle(df: pd.DataFrame, restatement_aware: bool, co_oracle: bool):
    """Build and cache one immutable oracle per input prefix and mode.

    The process cache avoids repeated reads for adjacent engine cells. The
    provenance-keyed disk cache lets independent engine shards and repeats use
    the same cross-checked expected table.
    """
    identity = df.attrs.get(
        "_equitable_input_key", ("object", id(df), int(len(df))))
    cache_key = (identity, bool(restatement_aware), bool(co_oracle))
    cached = _ORACLE_CACHE.get(cache_key)
    if cached is not None:
        timing = dict(cached[2])
        timing["oracle_cache_hit"] = True
        return cached[0], cached[1], timing

    pan, co, timing = build_or_load_oracle(
        df,
        root=ROOT,
        source_sha256=str(df.attrs["_equitable_input_sha256"]),
        selection=dict(df.attrs["_equitable_selection"]),
        restatement_aware=restatement_aware,
        co_oracle=co_oracle,
    )
    result = (pan, co, timing)
    if len(_ORACLE_CACHE) >= 8:
        _ORACLE_CACHE.clear()
    _ORACLE_CACHE[cache_key] = result
    return pan, co, timing


def _emr_once(engine_name: str, df: pd.DataFrame, variant: str,
              restatement_aware: bool, epsilon: float, co_oracle: bool,
              adapter_options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Ingest df through one engine under `variant`, compare hourly+daily to the
    oracle. Returns the two comparator summaries + a pass flag (or supported=False)."""
    adapter = get_adapter(engine_name, **(adapter_options or {}))
    if variant not in adapter.supported_variants:
        return {"variant": variant, "supported": False,
                "reason": f"{engine_name} does not implement the {variant!r} view",
                "pass": False}
    oracle, co, oracle_timing = _build_oracle(df, restatement_aware, co_oracle)
    if co is not None and not co["agree"]:
        # The oracle itself is inconsistent -- an engine comparison would be
        # meaningless. Fail loudly; this is a harness bug, not an engine finding.
        return {"variant": variant, "supported": True, "co_oracle": co, "pass": False,
                "reason": "pandas and DuckDB oracles disagree -- oracle inconsistent, not an engine result",
                **oracle_timing}
    with adapter.connect() as eng:
        eng.setup(variant=variant)
        stats = eng.ingest_log(df)
        h = eng.read_view("hourly")
        d = eng.read_view("daily")
    view_restarts = list(getattr(eng, "view_restarts", None) or [])
    ch = compare_bucket_tables(h, oracle["hourly"], epsilon=epsilon)
    cd = compare_bucket_tables(d, oracle["daily"], epsilon=epsilon)
    clean = bool(ch["equivalent"] and cd["equivalent"])
    # Attribute a known signature while retaining the strict failure verdict.
    known_finding = None
    if not clean:
        known_finding = match_known_finding(engine_name, h, d,
                                            oracle["hourly"], oracle["daily"], ch, cd)
    out = {"variant": variant, "supported": True, "ingest": stats.as_dict(),
           "hourly": ch, "daily": cd,
           "clean_equivalence": clean,
           "known_finding": known_finding,
           "pass": clean, **oracle_timing}
    if co is not None:
        out["co_oracle"] = co
    if view_restarts:
        out["view_restarts"] = view_restarts
    return out


def _run_emr(engine_name: str, diag: Diag, df: pd.DataFrame, epsilon: float,
             co_oracle: bool,
             adapter_options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {"metric": "EMR"}
    out["primary"] = _emr_once(
        engine_name, df, diag.primary, diag.restatement_aware, epsilon,
        co_oracle, adapter_options)
    out["pass"] = out["primary"]["pass"]
    if diag.contrast:
        out["contrast"] = _emr_once(
            engine_name, df, diag.contrast, diag.restatement_aware, epsilon,
            co_oracle, adapter_options)
        # design-space cell: contrast (naive/dedup) FAILs, primary (patched) PASSes.
        if out["contrast"].get("supported") and out["primary"].get("supported"):
            out["recovers"] = bool(not out["contrast"]["pass"] and out["primary"]["pass"])
        out["smallest_patch"] = diag.smallest_patch
    if diag.idiomatic:
        # Reported observation, not a verdict input. `out["pass"]` above is
        # already fixed to the repaired view, so nothing downstream changes.
        #
        # Isolated on purpose: this arm is the newest SQL in the suite, and an
        # invalid view on one engine would otherwise raise, blow up the whole
        # cell as artifact_valid=False, and cost that engine D1/D2/D4 across
        # every domain and repeat of the campaign. A broken arm must degrade to
        # a missing observation, never to a blocked scientific cell.
        try:
            arm = _emr_once(
                engine_name, df, diag.idiomatic, diag.restatement_aware, epsilon,
                co_oracle, adapter_options)
        except Exception as exc:  # noqa: BLE001 -- see comment above
            arm = {"variant": diag.idiomatic, "supported": True, "pass": False,
                   "arm_error": f"{type(exc).__name__}: {exc}"}
        out["idiomatic"] = arm
        if (arm.get("supported") and not arm.get("arm_error")
                and out["primary"].get("supported")):
            # True == the safe rule is expressible AND the natural formulation
            # is not: the benchmark's repair is doing the work, not the engine.
            out["repair_is_load_bearing"] = bool(
                out["primary"]["pass"] and not arm["pass"])
    return out


#: Longitudinal record of every D3 replay, keyed by (engine, domain, rows, input).
#: Lives outside any single run directory because its whole purpose is to compare
#: ACROSS invocations; append-only, like runs/ itself.
D3_LEDGER = ROOT / "runs" / "_ledger" / "d3_determinism"


def _bitwise_sums(table: pd.DataFrame, grain: str) -> Dict[str, str]:
    """Per-key ``sum`` as its exact IEEE-754 bit pattern.

    Comparing float text or float equality would hide the very quantization D3
    exists to detect. Grain is part of the key because C3 covers both maintained
    hourly and daily reductions.
    """
    out: Dict[str, str] = {}
    for bucket, entity, metric, value in zip(
            table["bucket"], table["entity"], table["metric"], table["sum"]):
        key = f"{grain}|{bucket}|{entity}|{metric}"
        out[key] = struct.pack(">d", float(value)).hex() if pd.notna(value) else "nan"
    return out


def _ledger_path(engine: str, domain: str, rows: int, input_sha: str) -> Path:
    return D3_LEDGER / f"{engine}__{domain}__{rows}__{input_sha[:12]}.jsonl"


_LEDGER_COHORT_FIELDS = (
    "contract_version",
    "git_commit",
    "invocation_id",
    "benchmark_config_sha256",
    "dependency_lock_sha256",
)
_LEDGER_IDENTITY_FIELDS = ("engine", "domain", "rows", "input_sha256")


def _ledger_failure(path: Path, line_number: int, reason: str) -> ValueError:
    location = f" line {line_number}" if line_number > 0 else ""
    return ValueError(f"corrupt D3 ledger {path}{location}: {reason}")


def _no_duplicate_json_keys(
    pairs: list[tuple[str, Any]],
) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON constant {value!r}")


def _validate_ledger_record(
    record: Any,
    path: Path,
    line_number: int,
    expected_identity: Dict[str, Any],
) -> Dict[str, Any]:
    if not isinstance(record, dict):
        raise _ledger_failure(path, line_number, "record must be a JSON object")
    required = (
        "utc", *_LEDGER_COHORT_FIELDS, *_LEDGER_IDENTITY_FIELDS, "sums",
    )
    missing = [field for field in required if field not in record]
    if missing:
        raise _ledger_failure(
            path, line_number,
            "record is missing required field(s): " + ", ".join(missing),
        )
    if (
        isinstance(record["contract_version"], bool)
        or not isinstance(record["contract_version"], int)
        or record["contract_version"] <= 0
    ):
        raise _ledger_failure(
            path, line_number, "contract_version must be a positive integer")
    for field in ("utc", "git_commit", "engine", "domain"):
        if not isinstance(record[field], str) or not record[field]:
            raise _ledger_failure(
                path, line_number, f"{field} must be a nonempty string")
    invocation_id = record["invocation_id"]
    if invocation_id is not None and (
        not isinstance(invocation_id, str) or not invocation_id.strip()
    ):
        raise _ledger_failure(
            path, line_number, "invocation_id must be null or a nonempty string")
    for field in (
        "benchmark_config_sha256", "dependency_lock_sha256", "input_sha256",
    ):
        value = record[field]
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(char not in "0123456789abcdefABCDEF" for char in value)
        ):
            raise _ledger_failure(
                path, line_number, f"{field} must be one 64-hex digest")
    rows = record["rows"]
    if isinstance(rows, bool) or not isinstance(rows, int) or rows <= 0:
        raise _ledger_failure(path, line_number, "rows must be a positive integer")
    if record.get("ledger_record_version", 1) != 1:
        raise _ledger_failure(
            path, line_number, "ledger_record_version must equal 1")
    sums = record["sums"]
    if not isinstance(sums, dict) or not sums:
        raise _ledger_failure(path, line_number, "sums must be a nonempty object")
    for key, value in sums.items():
        valid_bits = (
            isinstance(value, str)
            and (
                value == "nan"
                or (
                    len(value) == 16
                    and all(
                        char in "0123456789abcdefABCDEF" for char in value)
                )
            )
        )
        if not isinstance(key, str) or not key or not valid_bits:
            raise _ledger_failure(
                path, line_number,
                "sums entries must map nonempty string keys to IEEE-754 bits",
            )
    for field, expected in expected_identity.items():
        if record.get(field) != expected:
            raise _ledger_failure(
                path, line_number,
                f"{field} does not match the ledger identity",
            )
    run_id = record.get("run_id")
    if run_id is not None and (
        not isinstance(run_id, str) or not run_id.strip()
    ):
        raise _ledger_failure(
            path, line_number, "run_id must be a nonempty string when present")
    return record


def _read_ledger_locked(
    handle: Any,
    path: Path,
    expected_identity: Dict[str, Any],
) -> list[Dict[str, Any]]:
    handle.seek(0)
    records: list[Dict[str, Any]] = []
    for line_number, line in enumerate(handle, start=1):
        if not line.endswith("\n"):
            raise _ledger_failure(
                path, line_number, "record is not newline-terminated")
        if not line.strip():
            raise _ledger_failure(path, line_number, "blank record")
        try:
            record = json.loads(
                line,
                object_pairs_hook=_no_duplicate_json_keys,
                parse_constant=_reject_json_constant,
            )
        except ValueError as exc:
            raise _ledger_failure(
                path, line_number, f"invalid JSON: {exc}") from exc
        records.append(_validate_ledger_record(
            record, path, line_number, expected_identity))
    return records


def _cross_invocation_drift(
    records: list[Dict[str, Any]],
    current: Dict[str, str],
    provenance: Dict[str, Any],
) -> Dict[str, Any]:
    """Compare with the earliest valid record in this provenance cohort."""
    baseline: Optional[Dict[str, Any]] = None
    prior = 0
    for record in records:
        if any(
            record[field] != provenance.get(field)
            for field in _LEDGER_COHORT_FIELDS
        ):
            continue
        prior += 1
        if baseline is None:
            baseline = record
    if baseline is None:
        return {
            "baseline_run_id": None,
            "prior_invocations": prior,
            "keys": len(current),
            "drift_keys": 0,
            "key_set_mismatch": 0,
            "ddr": 0.0,
            "pass": True,
            "note": (
                "first replay in this explicit provenance cohort -- "
                "no prior baseline"
            ),
        }

    base_sums: Dict[str, str] = baseline["sums"]
    shared = set(base_sums) & set(current)
    drift = sum(1 for key in shared if base_sums[key] != current[key])
    only = len(set(base_sums) ^ set(current))
    return {
        "baseline_run_id": baseline.get("run_id"),
        "baseline_utc": baseline["utc"],
        "prior_invocations": prior,
        "keys": len(shared),
        "drift_keys": drift,
        "key_set_mismatch": only,
        "ddr": (drift / len(shared)) if shared else 0.0,
        "pass": bool(drift == 0 and only == 0),
    }


def _append_ledger(handle: Any, record: Dict[str, Any]) -> None:
    handle.seek(0, os.SEEK_END)
    handle.write(json.dumps(
        record, sort_keys=True, allow_nan=False) + "\n")
    handle.flush()
    os.fsync(handle.fileno())


def _ledger_transaction(
    path: Path,
    current: Dict[str, str],
    provenance: Dict[str, Any],
    *,
    engine: str,
    domain: str,
    rows: int,
    input_sha256: str,
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Strictly read, compare, and durably append under one exclusive lock."""
    identity = {
        "engine": engine,
        "domain": domain,
        "rows": rows,
        "input_sha256": input_sha256,
    }
    record: Dict[str, Any] = {
        "ledger_record_version": 1,
        "utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        **provenance,
        **identity,
        "sums": dict(current),
    }
    if run_id is not None:
        record["run_id"] = run_id

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            records = _read_ledger_locked(handle, path, identity)
            _validate_ledger_record(record, path, 0, identity)
            cross = _cross_invocation_drift(records, current, provenance)
            _append_ledger(handle, record)
            return cross
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _run_ddr(
    engine_name: str,
    df: pd.DataFrame,
    domain: str,
    input_sha: str,
    *,
    epsilon: float = 1e-9,
    co_oracle: bool = False,
    ledger: bool = True,
) -> Dict[str, Any]:
    """D3: accurate deterministic summation, at two replay scopes.

    intra-invocation  replay twice back to back in this process (the original
                      test) -- catches nondeterministic reduction order.
    cross-invocation  compare against the earliest recorded replay of the same
                      input in this explicit commit/config/invocation cohort;
                      catches accumulator state that differs across processes.

    Both replays must also satisfy the ordinary hourly+daily oracle equivalence
    contract. Self-consistent but wrong output is not a C3 pass, and an empty
    input with empty engine and oracle tables is invalid rather than vacuously
    deterministic.
    """
    oracle, co, oracle_timing = _build_oracle(
        df, restatement_aware=False, co_oracle=co_oracle)
    if co is not None and not co["agree"]:
        return {
            "metric": "DDR", "keys": 0, "key_set_mismatch": 0,
            "drift_keys": 0, "ddr": 0.0, "non_vacuous": False,
            "oracle_equivalent": False, "intra_invocation_pass": False,
            "artifact_valid": False, "pass": False, "co_oracle": co,
            "reason": "pandas and DuckDB oracles disagree -- oracle inconsistent",
            **oracle_timing,
        }

    view_restarts: List[Dict[str, Any]] = []

    def once() -> Dict[str, pd.DataFrame]:
        a = get_adapter(engine_name)
        with a.connect() as eng:
            eng.setup(variant="dedup")
            eng.ingest_log(df)
            views = {
                "hourly": eng.read_view("hourly"),
                "daily": eng.read_view("daily"),
            }
            view_restarts.extend(getattr(eng, "view_restarts", None) or [])
            return views

    replay1, replay2 = once(), once()
    hourly_bits1 = _bitwise_sums(replay1["hourly"], "hourly")
    daily_bits1 = _bitwise_sums(replay1["daily"], "daily")
    hourly_bits2 = _bitwise_sums(replay2["hourly"], "hourly")
    daily_bits2 = _bitwise_sums(replay2["daily"], "daily")
    bits1 = {**hourly_bits1, **daily_bits1}
    bits2 = {**hourly_bits2, **daily_bits2}
    shared = set(bits1) & set(bits2)
    only = len(set(bits1) ^ set(bits2))
    drift = sum(1 for key in shared if bits1[key] != bits2[key])
    keys = len(shared)
    ddr = (drift / keys) if keys else 0.0

    comparisons = {
        "replay_1": {
            grain: compare_bucket_tables(replay1[grain], oracle[grain], epsilon=epsilon)
            for grain in ("hourly", "daily")
        },
        "replay_2": {
            grain: compare_bucket_tables(replay2[grain], oracle[grain], epsilon=epsilon)
            for grain in ("hourly", "daily")
        },
    }
    oracle_equivalent = all(
        comparison["equivalent"]
        for replay in comparisons.values()
        for comparison in replay.values()
    )
    oracle_key_count = sum(len(oracle[grain]) for grain in ("hourly", "daily"))
    non_vacuous = bool(len(df) > 0 and oracle_key_count > 0)
    replay_nonempty = bool(bits1 and bits2)
    intra_pass = bool(replay_nonempty and drift == 0 and only == 0)
    known_finding = None
    if non_vacuous and not oracle_equivalent:
        known_finding = match_known_finding(
            engine_name,
            replay1["hourly"], replay1["daily"],
            oracle["hourly"], oracle["daily"],
            comparisons["replay_1"]["hourly"],
            comparisons["replay_1"]["daily"],
        )

    out: Dict[str, Any] = {
        "metric": "DDR", "keys": keys, "key_set_mismatch": only,
        "drift_keys": drift, "ddr": ddr,
        "hourly_keys": len(set(hourly_bits1) & set(hourly_bits2)),
        "daily_keys": len(set(daily_bits1) & set(daily_bits2)),
        "oracle_keys": oracle_key_count,
        "non_vacuous": non_vacuous,
        "oracle_equivalent": bool(oracle_equivalent),
        "oracle_comparisons": comparisons,
        "known_finding": known_finding,
        **({"view_restarts": view_restarts} if view_restarts else {}),
        "intra_invocation_pass": intra_pass,
        "artifact_valid": non_vacuous,
        **oracle_timing,
    }
    if co is not None:
        out["co_oracle"] = co
    if not non_vacuous:
        out["reason"] = "D3 requires a non-empty input and non-empty oracle state"
    elif not replay_nonempty:
        out["reason"] = "both engine replays exposed no aggregate keys"

    if not ledger or not non_vacuous or not replay_nonempty:
        out["pass"] = bool(non_vacuous and intra_pass and oracle_equivalent)
        return out

    path = _ledger_path(engine_name, domain, int(len(df)), input_sha)
    provenance = run_provenance(ROOT)
    cross = _ledger_transaction(
        path, bits1, provenance,
        engine=engine_name,
        domain=domain,
        rows=int(len(df)),
        input_sha256=input_sha,
    )

    out["cross_invocation"] = cross
    out["ledger"] = str(path.relative_to(ROOT))
    out["pass"] = bool(
        non_vacuous and intra_pass and oracle_equivalent and cross["pass"])
    return out


def _d6_params() -> tuple[int, str]:
    """Read D6's k and rank_metric from config/default.yaml (defaults 10, mean)."""
    import yaml
    try:
        cfg = yaml.safe_load((ROOT / "config/default.yaml").read_text()) or {}
    except Exception:
        cfg = {}
    d6 = cfg.get("d6", {}) or {}
    return int(d6.get("k", 10)), str(d6.get("rank_metric", "mean"))


def _run_topk(engine_name: str, df: pd.DataFrame, k: int, rank_metric: str,
              co_oracle: bool, epsilon: float = 1e-9) -> Dict[str, Any]:
    """D6: ingest the base log (dedup view), derive top-k per (bucket, metric) from
    the engine's aggregate AND the oracle's, and compare set + rank."""
    from src.diagnostics.d6 import topk_table, compare_topk
    adapter = get_adapter(engine_name)
    variant = "dedup"
    if variant not in adapter.supported_variants:
        return {"metric": "TOPK", "supported": False, "pass": False,
                "reason": f"{engine_name} does not implement the {variant!r} view"}
    oracle, co, oracle_timing = _build_oracle(df, restatement_aware=False, co_oracle=co_oracle)
    if co is not None and not co["agree"]:
        return {"metric": "TOPK", "supported": True, "co_oracle": co, "pass": False,
                "reason": "pandas and DuckDB oracles disagree -- oracle inconsistent, not an engine result",
                **oracle_timing}
    with adapter.connect() as eng:
        eng.setup(variant=variant)
        stats = eng.ingest_log(df)
        eh = eng.read_view("hourly")
        ed = eng.read_view("daily")
    view_restarts = list(getattr(eng, "view_restarts", None) or [])
    eht = topk_table(eh, k, rank_metric)
    edt = topk_table(ed, k, rank_metric)
    if hasattr(eng, "mutate_topk"):
        eht = eng.mutate_topk(eht)
        edt = eng.mutate_topk(edt)
    # Tie-aware: entities whose oracle values are within the contract tolerance
    # rank as tied (see src/diagnostics/d6.py), so C3-accepted last-bit noise
    # cannot fail a ranking check.
    ch = compare_topk(eht, topk_table(oracle["hourly"], k, rank_metric),
                      oracle_view=oracle["hourly"], rank_metric=rank_metric,
                      epsilon=epsilon)
    cd = compare_topk(edt, topk_table(oracle["daily"], k, rank_metric),
                      oracle_view=oracle["daily"], rank_metric=rank_metric,
                      epsilon=epsilon)
    out = {"metric": "TOPK", "supported": True, "k": k, "rank_metric": rank_metric,
           "epsilon": epsilon, "tie_rule": "oracle values within tau rank as tied",
           "ingest": stats.as_dict(), "hourly": ch, "daily": cd,
           "pass": bool(ch["equivalent"] and cd["equivalent"]),
           **oracle_timing}
    if co is not None:
        out["co_oracle"] = co
    if view_restarts:
        out["view_restarts"] = view_restarts
    return out


def _run_hop(engine_name: str, df: pd.DataFrame, epsilon: float,
             co_oracle: bool) -> Dict[str, Any]:
    """D8: compare the engine's maintained HOPPING window against the oracle.

    A hopping window puts each event in k overlapping windows instead of one
    and emits the bucket schema keyed by window start, so this reuses the
    ordinary bucket comparator. What it does not reuse is the tumbling oracle:
    the expected table comes from src/oracle/hopping.py, computed twice, in
    pandas and in DuckDB. A disagreement between those two is a harness error
    and is reported as such, never as an engine finding, exactly as elsewhere.

    An engine that cannot express the window declines through
    NotImplementedError and the cell is recorded unsupported. No engine does
    today: all seven express it, though not one of them in the same way.
    """
    from src.oracle.comparator import compare_bucket_tables
    adapter = get_adapter(engine_name)
    if adapter.hopping_mode == "unsupported":
        return {"metric": "HOP", "supported": False, "pass": False,
                "reason": f"{engine_name} cannot express a hopping window"}
    # Keep the canonical '%Y-%m-%dT%H:%M:%S.%fZ' string exactly as the log
    # carries it. Reformatting it to whole seconds truncates sub-second
    # precision, and then the two oracles disagree: the pandas path sorts on
    # the full-precision timestamp while the SQL path casts the truncated
    # string, so rows that are distinct to one are ties to the other and the
    # dedupe_id tiebreak picks different rows. That showed up as matching
    # latest_event_time beside a latest_value differing by 1.22.
    from src.adapters.base import canonical_event_time
    frame = df.copy()
    frame["event_time_ts"] = pd.to_datetime(frame["event_time_utc"], utc=True,
                                            format="ISO8601")
    frame["event_time_utc"] = canonical_event_time(frame["event_time_utc"])
    expected, co, oracle_timing = build_or_load_hopping_oracle(
        frame,
        root=ROOT,
        source_sha256=str(df.attrs["_equitable_input_sha256"]),
        selection=dict(df.attrs["_equitable_selection"]),
        co_oracle=co_oracle,
    )
    if co is not None and not co["agree"]:
        return {"metric": "HOP", "supported": True, "co_oracle": co,
                "pass": False,
                "reason": ("pandas and DuckDB hopping oracles disagree -- "
                           "oracle inconsistent, not an engine result"),
                **oracle_timing}
    try:
        with adapter.connect() as eng:
            eng.setup_hopping()
            stats = eng.ingest_log(df)
            read = {g: eng.read_hopping(g) for g in ("hourly", "daily")}
    except NotImplementedError as exc:
        return {"metric": "HOP", "supported": False, "pass": False,
                "reason": str(exc)}
    compared = {
        g: compare_bucket_tables(read[g].reset_index(drop=True),
                                 expected[g].reset_index(drop=True),
                                 epsilon=epsilon)
        for g in ("hourly", "daily")
    }
    out = {"metric": "HOP", "supported": True,
           "formulation": adapter.hopping_mode,
           "ingest": stats.as_dict(),
           "hourly": compared["hourly"], "daily": compared["daily"],
           "pass": bool(compared["hourly"]["equivalent"]
                        and compared["daily"]["equivalent"]),
           **oracle_timing}
    if co is not None:
        out["co_oracle"] = co
    return out


def _sketch_params() -> Dict[str, Any]:
    """Read the D7S sketch parameters from config/default.yaml."""
    import yaml
    try:
        cfg = yaml.safe_load((ROOT / "config/default.yaml").read_text()) or {}
    except Exception:
        cfg = {}
    sk = cfg.get("sketch", {}) or {}
    return {"kll_k": int(sk.get("d7s_kll_k", sk.get("kll_k", 200))),
            "quantiles": tuple(float(q) for q in sk.get("quantiles", (0.5, 0.9, 0.99))),
            "rank_tol": float(sk.get("rank_tol", 0.01)),
            "min_group": int(sk.get("min_group", 20)),
            "seed": int(cfg.get("seed", 1337))}


def _run_qer(df: pd.DataFrame) -> Dict[str, Any]:
    """D7S: bounded-deletion reference sketches over the d7 fixture (QER)."""
    from src.diagnostics.d7 import run_d7_sketch
    return run_d7_sketch(df, **_sketch_params())


def _timing_metadata(result: Dict[str, Any], metric: str) -> tuple[float, bool]:
    """Return cache-normalization credit and whether the primary cell executed.

    Every executed scaling cell is charged for one independent oracle build. A
    cache miss is already inside the measured wall time; a cache hit receives the
    cached build duration as a logical credit. This keeps sequential engine order
    from biasing throughput while retaining the process-local performance win.
    """
    candidates = [result, result.get("primary"), result.get("contrast")]
    infos = [c for c in candidates
             if isinstance(c, dict) and isinstance(c.get("oracle_build_s"), (int, float))]
    oracle_build_s = max((float(c["oracle_build_s"]) for c in infos), default=0.0)
    observed_miss = any(c.get("oracle_cache_hit") is False for c in infos)
    oracle_credit_s = 0.0 if observed_miss else oracle_build_s

    if metric == "EMR":
        primary = result.get("primary", {})
        executed = bool(primary.get("supported") and "hourly" in primary)
    elif metric in ("TOPK", "HOP"):
        executed = bool(result.get("supported") and "hourly" in result)
    else:
        executed = True
    return oracle_credit_s, executed


def run_one(engine: str, domain: str, name: str, max_rows: int, epsilon: float,
            co_oracle: bool = False, d3_ledger: bool = True) -> Dict[str, Any]:
    diag = DIAGNOSTICS[name]
    if diag.fixture is None:
        src = ROOT / "data/normalized" / domain / "log.parquet"
    else:
        src = ROOT / "data/fixtures" / domain / diag.fixture
    # A head-only D4 scale can be temporally constant for coarse-cadence
    # domains. A deterministic uniform selection spans the frozen source while
    # preserving arrival order, so every requested D4 scale contains the fault.
    selection_mode = "uniform" if name == "D4" else "prefix"
    df = _load(
        src, max_rows, selection_mode, allow_restatements=name in ("D7", "D7S"))

    base = {
        "engine": engine, "domain": domain, "diagnostic": name,
        "description": diag.desc, "metric": diag.metric,
        "input": str(src), "input_sha256": _sha256_file(src), "rows": int(len(df)),
        "row_selection": selection_mode,
        "requested_max_rows": int(max_rows),
        "repeat_index": int(os.environ.get("EQUITABLE_REPEAT_INDEX", "0")),
        "campaign_id": os.environ.get("EQUITABLE_CAMPAIGN_ID"),
        "campaign_phase": os.environ.get("EQUITABLE_CAMPAIGN_PHASE"),
        "epsilon": epsilon, **run_provenance(ROOT),
        "started_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    adapter_options: Dict[str, Any] = {}
    if name == "D7":
        adapter_options["allow_restatements"] = True
    if name == "D4":
        allowance = int(effective_perturbation(
            ROOT, domain)["allowed_lateness_s"])
        base["allowed_lateness_s"] = allowance
        if engine == "mutant_drop_late":
            adapter_options["allowed_lateness_s"] = allowance
    cell_t0 = time.perf_counter()
    if diag.metric == "DDR":
        result = _run_ddr(
            engine, df, domain, base["input_sha256"], epsilon=epsilon,
            co_oracle=co_oracle, ledger=d3_ledger)
    elif diag.metric == "TOPK":
        k, rank_metric = _d6_params()
        result = _run_topk(engine, df, k, rank_metric, co_oracle, epsilon)
    elif diag.metric == "HOP":
        result = _run_hop(engine, df, epsilon, co_oracle)
    elif diag.metric == "QER":
        result = _run_qer(df)          # reference sketches; engine-independent
    else:
        result = _run_emr(
            engine, diag, df, epsilon, co_oracle, adapter_options)
    measured_wall_s = time.perf_counter() - cell_t0
    oracle_credit_s, executed = _timing_metadata(result, diag.metric)
    base.update(result)
    oracle_keys = sorted({
        str(holder["oracle_cache_key"])
        for holder in (result, result.get("primary"), result.get("contrast"))
        if isinstance(holder, dict) and holder.get("oracle_cache_key")
    })
    base["oracle_cache_keys"] = oracle_keys
    base["measured_wall_s"] = measured_wall_s
    base["oracle_cache_credit_s"] = oracle_credit_s
    base["duration_s"] = measured_wall_s + oracle_credit_s
    base["executed"] = executed
    base["finished_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    base["artifact_valid"] = bool(result.get("artifact_valid", True))
    return base


def _write_run(summary: Dict[str, Any], log_lines: list[str]) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    out_dir = ROOT / "runs" / f"{stamp}_{summary['domain']}_{summary['engine']}_{summary['diagnostic'].lower()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n")
    (out_dir / "log.txt").write_text("\n".join(log_lines) + "\n")
    return out_dir


def _fmt(summary: Dict[str, Any]) -> list[str]:
    lines = [f"engine={summary['engine']} domain={summary['domain']} "
             f"diag={summary['diagnostic']} ({summary['description']})",
             f"metric={summary['metric']} rows={summary['rows']}"]
    if summary["metric"] == "DDR":
        lines.append(f"intra-invocation: DDR={summary['ddr']:.3g} "
                     f"drift_keys={summary['drift_keys']} "
                     f"key_set_mismatch={summary['key_set_mismatch']}")
        lines.append(
            f"oracle-equivalent={summary.get('oracle_equivalent')} "
            f"non-vacuous={summary.get('non_vacuous')}")
        cross = summary.get("cross_invocation")
        if cross:
            if cross.get("baseline_run_id") is None and cross.get("note"):
                lines.append(f"cross-invocation: {cross['note']} "
                             f"(ledger {summary.get('ledger')})")
            else:
                lines.append(
                    f"cross-invocation: DDR={cross['ddr']:.3g} "
                    f"drift_keys={cross['drift_keys']} "
                    f"key_set_mismatch={cross['key_set_mismatch']} "
                    f"vs baseline {cross.get('baseline_utc')} "
                    f"({cross['prior_invocations']} prior invocation(s))")
    elif summary["metric"] == "QER":
        io, bd = summary["insert_only"], summary["bounded_deletion"]
        lines.append(f"groups_scored={summary['groups_scored']}/{summary['groups_total']} "
                     f"restatements={summary['restatements']} rank_tol={summary['rank_tol']}")
        lines.append(f"primary[bounded_deletion KLL±]: pass={summary['pass']} "
                     f"qer={bd['qer']:.3g} max_rank_err={bd['max_rank_err']:.3g}")
        lines.append(f"contrast[insert-only KLL]: qer={io['qer']:.3g} "
                     f"max_rank_err={io['max_rank_err']:.3g}  recovers={summary.get('recovers')}")
    elif summary["metric"] == "HOP":
        if not summary.get("supported", True):
            lines.append(f"HOP: UNSUPPORTED ({summary.get('reason')})")
        elif "hourly" not in summary:
            lines.append(f"HOP: no-compare ({summary.get('reason')})")
        else:
            co = summary.get("co_oracle")
            if co is not None:
                lines.append(f"  co-oracle (pandas==DuckDB): agree={co['agree']}")
            lines.append(f"  formulation: {summary.get('formulation')}")
            for grain in ("hourly", "daily"):
                cell = summary[grain]
                lines.append(
                    f"  {grain}: equivalent={cell['equivalent']} "
                    f"emr={cell.get('emr', 0):.6g} "
                    f"only_engine={cell.get('only_engine', 0)} "
                    f"only_oracle={cell.get('only_oracle', 0)}")
    elif summary["metric"] == "TOPK":
        if not summary.get("supported", True):
            lines.append(f"TOPK: UNSUPPORTED ({summary.get('reason')})")
        elif "hourly" not in summary:
            lines.append(f"TOPK: no-compare ({summary.get('reason')})")
        else:
            co = summary.get("co_oracle")
            if co is not None:
                lines.append(f"co-oracle (pandas==DuckDB): agree={co['agree']}"
                             + ("" if co["agree"] else "  *** ORACLE INCONSISTENT ***"))
            h, d = summary["hourly"], summary["daily"]
            lines.append(f"top-k (k={summary['k']} by {summary['rank_metric']}): "
                         f"hourly set_emr={h['set_emr']:.3g} rank_emr={h['rank_emr']:.3g} | "
                         f"daily set_emr={d['set_emr']:.3g} rank_emr={d['rank_emr']:.3g}")
    else:
        p = summary["primary"]
        co = p.get("co_oracle")
        if co is not None:
            lines.append(f"co-oracle (pandas==DuckDB): agree={co['agree']}"
                         + ("" if co["agree"] else "  *** ORACLE INCONSISTENT ***"))
        if p.get("supported") and "hourly" in p:
            kf = p.get("known_finding")
            note = f"  [documented finding: {kf['summary']}]" if kf else ""
            lines.append(f"primary[{p['variant']}]: pass={p['pass']} "
                         f"hourly_emr={p['hourly']['emr']:.3g} daily_emr={p['daily']['emr']:.3g}{note}")
        else:
            lines.append(f"primary[{p['variant']}]: {'UNSUPPORTED' if not p.get('supported') else 'no-compare'} ({p.get('reason')})")
        if "contrast" in summary:
            c = summary["contrast"]
            ckf = c.get("known_finding")
            cnote = f"  [documented finding]" if ckf else ""
            state = (f"pass={c['pass']} hourly_emr={c['hourly']['emr']:.3g}{cnote}"
                     if c.get("supported") else f"UNSUPPORTED")
            lines.append(f"contrast[{c['variant']}]: {state}  "
                         f"recovers={summary.get('recovers')}")
        if "idiomatic" in summary:
            i = summary["idiomatic"]
            if i.get("supported"):
                state = (f"pass={i['pass']} hourly_emr={i['hourly']['emr']:.3g}"
                         if "hourly" in i else f"pass={i['pass']}")
            else:
                state = f"UNSUPPORTED ({i.get('reason')})"
            lines.append(f"idiomatic[{i.get('variant')}]: {state}  "
                         f"repair_load_bearing={summary.get('repair_is_load_bearing')}")
    # A capability gap is not a failed comparison: the collector records it as
    # unsupported, and the printed verdict says the same.
    lines.append("VERDICT: " + ("UNSUPPORTED" if _is_unsupported(summary)
                                else "PASS" if summary.get("pass") else "FAIL"))
    return lines


def _is_unsupported(summary: dict) -> bool:
    primary = summary.get("primary")
    return summary.get("supported") is False or (
        isinstance(primary, dict) and primary.get("supported") is False)


ALL_DOMAINS = ["environment_airquality", "transportation_taxi", "datacenter_drives",
               "finance_markets", "sports_football"]


def _completed_cells() -> set[tuple[str, str, str, int, int]]:
    """Completed cells for this invocation, used to resume interrupted shards."""
    invocation = os.environ.get("EQUITABLE_INVOCATION_ID")
    if not invocation:
        return set()
    cells: set[tuple[str, str, str, int, int]] = set()
    for path in (ROOT / "runs").glob("*/summary.json"):
        try:
            summary = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if summary.get("invocation_id") != invocation:
            continue
        if summary.get("artifact_valid") is not True or summary.get("error"):
            continue
        try:
            cells.add((
                str(summary["engine"]), str(summary["domain"]),
                str(summary["diagnostic"]),
                int(summary.get("requested_max_rows", summary["rows"])),
                int(summary.get("repeat_index", 0)),
            ))
        except (KeyError, TypeError, ValueError):
            continue
    return cells


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--engine", required=True, choices=list(ADAPTER_NAMES) + ["all"],
                    help="One engine, or 'all' (loops every engine, continues on failure).")
    ap.add_argument("--domain", default="environment_airquality",
                    help=f"One domain, or 'all'. Known: {ALL_DOMAINS}")
    ap.add_argument("--diagnostic", default="all",
                    help="D1|D2|D3|D4|D7 or 'all' (implemented ones).")
    ap.add_argument("--co-oracle", action="store_true",
                    help="Cross-check pandas vs DuckDB oracle before comparing the engine "
                         "(EMR diagnostics only); a disagreement is flagged as a harness bug.")
    ap.add_argument("--max-rows", type=int, default=0, help="0 = full fixture.")
    ap.add_argument("--epsilon", type=float, default=1e-9)
    ap.add_argument("--sweep", action="store_true",
                    help="Exit zero when every cell produced a valid comparison, even if a scientific verdict is FAIL.")
    ap.add_argument("--resume-existing", action="store_true",
                    help="Skip completed cells from this explicit invocation/repeat.")
    args = ap.parse_args()

    if args.diagnostic == "core":
        names = ["D1", "D2", "D3", "D4", "D6", "D7", "D8"]
    elif args.diagnostic == "all":
        names = list(DIAGNOSTICS)
    elif args.diagnostic in DIAGNOSTICS:
        names = [args.diagnostic]
    elif args.diagnostic in ELSEWHERE:
        print(f"{args.diagnostic}: has its own runner -- {ELSEWHERE[args.diagnostic]}")
        return 0
    else:
        ap.error(f"unknown diagnostic {args.diagnostic!r}; "
                 f"implemented here: {list(DIAGNOSTICS)}; elsewhere: {list(ELSEWHERE)}")

    engines = list(ADAPTER_NAMES) if args.engine == "all" else [args.engine]
    domains = ALL_DOMAINS if args.domain == "all" else [args.domain]

    overall_ok = True
    campaign_ok = True
    # Cell states for the closing summary line. A divergent cell is a finding
    # about the engine, not a failed run, so the line counts states instead of
    # collapsing them into PASS or FAIL.
    n_equivalent = n_divergent = n_unsupported = n_error = 0
    completed = _completed_cells() if args.resume_existing else set()
    # Group adjacent cells by input so the independently computed oracle is
    # reused across engines without changing sequential engine execution.
    for domain in domains:
        for name in names:
            for engine in engines:
                # D7S validates the reference sketches (engine-independent);
                # carry it once per domain on the reference row only.
                if name == "D7S" and engine != "reference":
                    if args.diagnostic == "D7S":
                        print(f"[skip] D7S on {engine}: engine-independent "
                              f"(reference-sketch diagnostic; run with --engine reference)")
                    continue
                cell = (engine, domain, name, int(args.max_rows),
                        int(os.environ.get("EQUITABLE_REPEAT_INDEX", "0")))
                if cell in completed:
                    print(f"[resume] {name} : {engine} on {domain}")
                    continue
                header = f"==== {name} : {engine} on {domain}{' [co-oracle]' if args.co_oracle else ''} ===="
                print(header)
                try:
                    summary = run_one(engine, domain, name, args.max_rows, args.epsilon, args.co_oracle)
                    body = _fmt(summary)
                    out_dir = _write_run(summary, [header] + body)
                    for b in body:
                        print("  " + b)
                    print(f"  run: {out_dir}")
                    overall_ok = overall_ok and bool(summary.get("pass"))
                    campaign_ok = campaign_ok and bool(summary.get("artifact_valid"))
                    if _is_unsupported(summary):
                        n_unsupported += 1
                    elif summary.get("pass"):
                        n_equivalent += 1
                    else:
                        n_divergent += 1
                except Exception as exc:
                    overall_ok = False
                    campaign_ok = False
                    n_error += 1
                    summary = {"engine": engine, "domain": domain, "diagnostic": name,
                               "pass": False, "error": str(exc),
                               "artifact_valid": False,
                               "requested_max_rows": int(args.max_rows),
                               "repeat_index": int(os.environ.get(
                                   "EQUITABLE_REPEAT_INDEX", "0")),
                               "campaign_id": os.environ.get(
                                   "EQUITABLE_CAMPAIGN_ID"),
                               "campaign_phase": os.environ.get(
                                   "EQUITABLE_CAMPAIGN_PHASE"),
                               **run_provenance(ROOT)}
                    out_dir = _write_run(summary, [header, "ERROR:", traceback.format_exc()])
                    print(f"  ERROR: {exc}")
                    print(f"  run: {out_dir}")

    for name, why in ELSEWHERE.items():
        if args.diagnostic in ("all", name):
            print(f"  [note] {name}: {why}")

    scope = f"{args.engine} / {args.domain} / {args.diagnostic}"
    n_cells = n_equivalent + n_divergent + n_unsupported + n_error
    print(f"\n{scope}: {n_equivalent} equivalent, {n_divergent} divergent, "
          f"{n_unsupported} unsupported, {n_error} error(s) of {n_cells} cell(s) "
          f"({len(engines)} engine(s) x {len(domains)} domain(s) x {len(names)} diagnostic(s))")
    if n_divergent and not n_error:
        print("A divergent cell is a finding about the engine's table, not a failed run; "
              "the locked verdict for each cell is in results/official_results.json.")
    return 0 if (campaign_ok if args.sweep else overall_ok) else 1


if __name__ == "__main__":
    raise SystemExit(main())
