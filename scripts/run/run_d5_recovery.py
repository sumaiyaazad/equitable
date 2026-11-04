#!/usr/bin/env python3
"""
run_d5_recovery.py -- D5: post-recovery state equivalence.

After a crash immediately following an explicit durability barrier, the engine's
materialized view must equal the rows acknowledged before that barrier --
no loss, no duplication, no corruption of the maintained aggregate. This is a
separate runner from run_diagnostic.py because it needs process-lifecycle
control (docker kill/restart) and durable-state configuration.

Procedure (params from config/default.yaml `d5`):
  1. split the base log at `kill_at_fraction` -> H1 (committed prefix), H2
  2. setup(variant=dedup) + ingest H1
  3. read the live H1 view as the paired no-crash control, then call
     adapter.checkpoint()  -- durability barrier, so "committed" == exactly H1
  4. docker kill --signal=<kill_signal> <container>   (the crash)
  5. docker restart <container> + wait for its healthcheck / readiness
  6. reconnect WITHOUT setup() (setup would wipe the view) and read the view
  7. compare recovered hourly+daily both to the pre-crash control and to
     oracle(H1): PASS iff state is preserved and oracle EMR = 0
     documented signatures are recorded for attribution but never convert a failure to PASS
  8. optional --resume: ingest H2 and compare the final view to oracle(H1+H2)

Covered engines: Materialize, RisingWave, ksqlDB, Flink, and Spark. Feldera is N/A on the pinned open-source v0.324.0 image because checkpointing is gated to the enterprise edition. The reference engine is in-process and therefore N/A.

The campaign always runs step 8. Flink's result topic and Spark's output files outlive the crash whether or not state was restored, so reading them back alone cannot fail; rows ingested after recovery must combine with the restored state. Bring the stack up with the durability
overlay first and confirm survival:
    docker compose -f docker/streaming-engines.yml \
                   -f docker/streaming-engines.durable.yml up -d
    python scripts/run/verify_persistence.py --engine <name>

WARNING: kills + restarts the engine container -- do NOT run while a diagnostic
sweep is in flight.

Inputs  : data/normalized/<domain>/log.parquet ; a running durable engine container
Outputs : runs/<UTC>_<domain>_<engine>_d5/{summary.json,log.txt} ; PASS/FAIL
Runtime : ~1-5 min per (engine, domain) at the default --rows
Run as  : python scripts/run/run_d5_recovery.py --engine materialize
          python scripts/run/run_d5_recovery.py --engine all --domain all --rows 20000
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.adapters import (ADAPTER_NAMES, DEPLOYMENT_NAMES, get_adapter,  # noqa: E402
                          canonical_input)
from src.diagnostics.findings import match_known_finding  # noqa: E402
from src.oracle.cache import build_or_load_oracle  # noqa: E402
from src.oracle.comparator import compare_bucket_tables  # noqa: E402
from src.provenance import run_provenance  # noqa: E402
from src.schema.artifacts import artifact_sha256, read_parquet_prefix  # noqa: E402

# engine -> compose container name (project "docker"). Override with --container.
CONTAINER = {
    "materialize": "docker-materialize-1",
    "risingwave": "docker-risingwave-1",
    "feldera": "docker-feldera-1",
    # ksqlDB rebuilds its materialized tables from the durable Kafka changelog on
    # restart; the adapter's read-existing path (read_view reads the resumed table
    # instead of recreating it) makes D5 meaningful. Needs the durable overlay so
    # Kafka survives (docker/streaming-engines.durable.yml: kafka_data).
    "ksqldb": "docker-ksqldb-1",
    # Flink session cluster. The TaskManager holds the operator state, so it is
    # the process to kill; the JobManager restores the job from its latest
    # retained checkpoint when the TaskManager returns.
    "flink": "docker-flink-taskmanager-1",
    # Spark standalone cluster. The driver holds the streaming queries; its
    # restart resumes them from their checkpoints.
    "spark": "docker-spark-driver-1",
}
IN_PROCESS = ("reference",)
# D5 verified on materialize + risingwave + ksqlDB. feldera implements the
# checkpoint+resume path (adapter.resume()), and on the pinned open-source
# v0.324.0 image runtime_config.storage does activate a backend
# (storage_status becomes "InUse"). The blocker moved: checkpointing itself is
# gated to the enterprise edition. Measured on the pinned image:
#   POST /v0/pipelines/{name}/checkpoint -> 501
#   {"error_code":"EnterpriseFeature","details":{"feature":"checkpoint"}}
# and any runtime_config carrying `fault_tolerance` is rejected with
# InvalidRuntimeConfig / {"EnterpriseFeature":"fault tolerance"}. End to end,
# verify_persistence reads back 0 hourly keys after a restart, so there is no
# committed prefix to compare. feldera D5 is therefore N/A on the evaluated
# open-source deployment; `--engine all` skips it. To attempt it on an edition
# that exposes checkpointing, pass options["storage"]=True and
# options["fault_tolerance"], then run `--engine feldera --container
# docker-feldera-1` after removing it from BLOCKED.
BLOCKED: Dict[str, str] = {
    "feldera": "evaluated open-source Feldera v0.324.0 deployment does not expose "
               "the durable checkpoint-and-recovery configuration D5 requires "
               "(POST /checkpoint -> 501 EnterpriseFeature)"}
ALL_DOMAINS = ["environment_airquality", "transportation_taxi", "datacenter_drives",
               "finance_markets", "sports_football"]
# Deployment aliases (flink_inprocess, spark_inprocess) are valid targets for ad hoc
# comparison even though they are absent from ADAPTER_NAMES.
ALL_ENGINES = list(ADAPTER_NAMES) + list(DEPLOYMENT_NAMES)


@lru_cache(maxsize=None)
def _sha256_file(path_text: str) -> str:
    return artifact_sha256(Path(path_text))


@lru_cache(maxsize=None)
def _parquet_rows(path_text: str) -> int:
    return int(pq.ParquetFile(path_text).metadata.num_rows)


def _campaign_fields(rows: int) -> Dict[str, Any]:
    return {
        "requested_max_rows": int(rows),
        "repeat_index": int(os.environ.get("EQUITABLE_REPEAT_INDEX", "0")),
        "campaign_id": os.environ.get("EQUITABLE_CAMPAIGN_ID"),
        "campaign_phase": os.environ.get("EQUITABLE_CAMPAIGN_PHASE"),
    }


def _cfg() -> Dict[str, Any]:
    import yaml
    cfg = yaml.safe_load((ROOT / "config/default.yaml").read_text()) or {}
    d5 = cfg.get("d5", {}) or {}
    return {"kill_at_fraction": float(d5.get("kill_at_fraction", 0.5)),
            "kill_signal": str(d5.get("kill_signal", "SIGKILL"))}


def _git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def _docker(*args: str) -> None:
    subprocess.run(["docker", *args], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def _oracle_tables(
    df: pd.DataFrame,
    input_sha256: str,
    selection: Dict[str, Any],
):
    return build_or_load_oracle(
        df,
        root=ROOT,
        source_sha256=input_sha256,
        selection=selection,
        restatement_aware=False,
        co_oracle=True,
    )


def _verified_h1_oracle(
    h1: pd.DataFrame, input_sha256: str,
):
    """Build/load the canonical two-oracle H1 control and require agreement."""
    oracle, co_oracle, timing = _oracle_tables(
        h1, input_sha256,
        {"mode": "prefix", "actual_rows": int(len(h1))},
    )
    if co_oracle is None or not co_oracle.get("agree"):
        raise RuntimeError("pandas and DuckDB D5 oracles disagree")
    return oracle, co_oracle, timing


def _read_views_after_restart(engine: str, wait_s: float):
    """Reconnect (no setup -- that would wipe the view) and read both grains,
    retrying while the engine is still coming back / rebuilding state.

    Every connection here ends in detach(), never close(). The resume step reads
    the same recovered views again after ingesting H2, so this read must leave
    the maintaining queries running. ksqlDB's close() TERMINATEs every persistent
    query, so a cell closed here would recover its state and then wait out the
    whole read budget on a table that nothing maintains any more.
    """
    deadline = time.time() + wait_s
    last_exc: Exception | None = None
    while time.time() < deadline:
        try:
            eng = get_adapter(engine).connect()
            try:
                # Bring the restored state back online without setup() (a no-op
                # for engines with a live server view; Feldera restarts its
                # checkpointed pipeline here).
                eng.resume()
                h = eng.read_view("hourly")
                d = eng.read_view("daily")
                if len(h) > 0:
                    return h, d
            finally:
                eng.detach()
        except Exception as exc:
            last_exc = exc
        time.sleep(3.0)
    if last_exc is not None:
        raise RuntimeError(f"engine did not come back within {wait_s:.0f}s: {last_exc}")
    # Came back reachable but with an empty view: that is the D5 result (state
    # lost), so return the empty frames and let the comparison record it.
    eng = get_adapter(engine).connect()
    try:
        eng.resume()
        return eng.read_view("hourly"), eng.read_view("daily")
    finally:
        eng.detach()


def run_one(engine: str, domain: str, rows: int, container: str, epsilon: float,
            wait_s: float, resume: bool) -> Dict[str, Any]:
    cfg = _cfg()
    log = ROOT / "data/normalized" / domain / "log.parquet"
    if not log.exists():
        raise FileNotFoundError(f"{log} not found -- ingest '{domain}' first.")
    df = read_parquet_prefix(log, rows)
    df = canonical_input(df)
    cut = int(len(df) * cfg["kill_at_fraction"])
    h1, h2 = df.iloc[:cut], df.iloc[cut:]
    input_sha256 = _sha256_file(str(log))

    summary: Dict[str, Any] = {
        "engine": engine, "domain": domain, "diagnostic": "D5",
        "description": "post-barrier state durability (SIGKILL + restart)",
        "metric": "EMR", "rows": int(len(df)), "committed_prefix_rows": int(len(h1)),
        "kill_at_fraction": cfg["kill_at_fraction"], "kill_signal": cfg["kill_signal"],
        "container": container, "epsilon": epsilon,
        "input": str(log), "input_sha256": input_sha256,
        **_campaign_fields(rows),
        **run_provenance(ROOT),
        "started_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    # 1-3) ingest the committed prefix, capture a paired no-crash control, and
    # force durability. Capturing the live view before the kill separates an
    # engine pre-existing query divergence from crash-induced state loss.
    eng = get_adapter(engine).connect()
    eng.setup(variant="dedup")
    stats = eng.ingest_log(h1)
    summary["ingest_h1"] = stats.as_dict()
    bh, bd = eng.read_view("hourly"), eng.read_view("daily")
    oracle, co_oracle, oracle_timing = _verified_h1_oracle(
        h1, input_sha256)
    summary["co_oracle"] = co_oracle
    summary["oracle_cache_keys"] = [oracle_timing["oracle_cache_key"]]
    summary["oracle_timing"] = oracle_timing
    bch = compare_bucket_tables(bh, oracle["hourly"], epsilon=epsilon)
    bcd = compare_bucket_tables(bd, oracle["daily"], epsilon=epsilon)
    baseline_clean = bool(bch["equivalent"] and bcd["equivalent"])
    baseline_finding = None if baseline_clean else match_known_finding(
        engine, bh, bd, oracle["hourly"], oracle["daily"], bch, bcd)
    summary["no_crash_control"] = {
        "hourly": bch, "daily": bcd,
        "clean_equivalence": baseline_clean,
        "known_finding": baseline_finding,
        "pass": baseline_clean,
    }
    try:
        eng.checkpoint()
        summary["checkpoint"] = "ok"
    except Exception as exc:
        raise RuntimeError("D5 requires a successful durability barrier") from exc
    # Not eng.close(): close() may tear down engine-side state
    # (e.g. Feldera shuts the pipeline down); the kill severs the session anyway.

    # 4-5) crash + restart (restart preserves the container filesystem).
    _docker("kill", f"--signal={cfg['kill_signal']}", container)
    summary["killed_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _docker("restart", container)

    # 6) reconnect WITHOUT setup and read the recovered views.
    rh, rd = _read_views_after_restart(engine, wait_s)

    # 7) recovered view vs both the paired pre-crash state and oracle(H1).
    # The first comparison isolates durability; the second preserves the strict
    # online--oracle contract. A documented finding is attribution, not a pass.
    pch = compare_bucket_tables(rh, bh, epsilon=epsilon)
    pcd = compare_bucket_tables(rd, bd, epsilon=epsilon)
    state_preserved = bool(pch["equivalent"] and pcd["equivalent"])
    ch = compare_bucket_tables(rh, oracle["hourly"], epsilon=epsilon)
    cd = compare_bucket_tables(rd, oracle["daily"], epsilon=epsilon)
    clean = bool(ch["equivalent"] and cd["equivalent"])
    kf = None if clean else match_known_finding(engine, rh, rd,
                                                oracle["hourly"], oracle["daily"], ch, cd)
    summary.update({
        "recovery": {
            "state_vs_pre_crash": {"hourly": pch, "daily": pcd,
                                   "state_preserved": state_preserved},
            "hourly": ch, "daily": cd,
            "clean_equivalence": clean,
            "known_finding": kf,
            "pass": bool(state_preserved and clean),
        },
    })
    summary["pass"] = summary["recovery"]["pass"]

    # 8) optional v1.1: resume with H2 and check the final view (H1+H2).
    if resume and state_preserved:
        eng2 = get_adapter(engine).connect()
        try:
            stats2 = eng2.ingest_log(h2)
            fh, fd = eng2.read_view("hourly"), eng2.read_view("daily")
        finally:
            eng2.close()
        oracle_full, co_full, full_timing = _oracle_tables(
            df, input_sha256,
            {"mode": "prefix", "actual_rows": int(len(df))},
        )
        if co_full is None or not co_full.get("agree"):
            raise RuntimeError("pandas and DuckDB resumed D5 oracles disagree")
        summary["oracle_cache_keys"] = sorted(set(
            summary["oracle_cache_keys"] + [full_timing["oracle_cache_key"]]))
        c2h = compare_bucket_tables(fh, oracle_full["hourly"], epsilon=epsilon)
        c2d = compare_bucket_tables(fd, oracle_full["daily"], epsilon=epsilon)
        clean2 = bool(c2h["equivalent"] and c2d["equivalent"])
        kf2 = None if clean2 else match_known_finding(engine, fh, fd,
                                                      oracle_full["hourly"], oracle_full["daily"],
                                                      c2h, c2d)
        summary["resume"] = {"ingest_h2": stats2.as_dict(), "hourly": c2h, "daily": c2d,
                             "clean_equivalence": clean2, "known_finding": kf2,
                             "pass": clean2}
        summary["pass"] = bool(summary["pass"] and summary["resume"]["pass"])

    summary["finished_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    summary["artifact_valid"] = True
    return summary


def _write_run(summary: Dict[str, Any], log_lines: list[str]) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    out_dir = ROOT / "runs" / f"{stamp}_{summary['domain']}_{summary['engine']}_d5"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n")
    (out_dir / "log.txt").write_text("\n".join(log_lines) + "\n")
    return out_dir


def _unsupported_summary(engine: str, domain: str, rows: int,
                         reason: str) -> Dict[str, Any]:
    """Record lifecycle N/A after materializing the shared H1 oracle control."""
    started_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    log = ROOT / "data" / "normalized" / domain / "log.parquet"
    available = _parquet_rows(str(log))
    used = min(available, rows) if rows > 0 else available
    input_sha256 = _sha256_file(str(log))
    df = canonical_input(read_parquet_prefix(log, used))
    cfg = _cfg()
    committed_prefix_rows = int(len(df) * cfg["kill_at_fraction"])
    _, co_oracle, oracle_timing = _verified_h1_oracle(
        df.iloc[:committed_prefix_rows], input_sha256)
    return {
        "engine": engine, "domain": domain, "diagnostic": "D5",
        "description": "post-barrier state durability (SIGKILL + restart)",
        "metric": "EMR", "rows": int(len(df)),
        "committed_prefix_rows": committed_prefix_rows,
        "kill_at_fraction": cfg["kill_at_fraction"],
        "kill_signal": cfg["kill_signal"],
        "supported": False,
        "reason": reason, "pass": False, "artifact_valid": True,
        "input": str(log), "input_sha256": input_sha256,
        "co_oracle": co_oracle,
        "oracle_cache_keys": [oracle_timing["oracle_cache_key"]],
        "oracle_timing": oracle_timing,
        **_campaign_fields(rows), **run_provenance(ROOT),
        "started_utc": started_utc,
        "finished_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _completed_cells() -> set[tuple[str, str, int, int]]:
    invocation = os.environ.get("EQUITABLE_INVOCATION_ID")
    if not invocation:
        return set()
    cells: set[tuple[str, str, int, int]] = set()
    for path in (ROOT / "runs").glob("*/summary.json"):
        try:
            summary = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if (summary.get("invocation_id") != invocation
                or summary.get("diagnostic") != "D5"
                or summary.get("artifact_valid") is not True
                or summary.get("error")):
            continue
        try:
            cells.add((
                str(summary["engine"]), str(summary["domain"]),
                int(summary.get("requested_max_rows", summary["rows"])),
                int(summary.get("repeat_index", 0)),
            ))
        except (KeyError, TypeError, ValueError):
            continue
    return cells


def _fmt(s: Dict[str, Any]) -> list[str]:
    r = s["recovery"]
    lines = [f"engine={s['engine']} domain={s['domain']} diag=D5 ({s['description']})",
             f"rows={s['rows']} committed_prefix={s['committed_prefix_rows']} "
             f"signal={s['kill_signal']} checkpoint={s['checkpoint']}",
             f"no-crash control: pass={s['no_crash_control']['pass']} "
             f"hourly_emr={s['no_crash_control']['hourly']['emr']:.3g}",
             f"recovery: pass={r['pass']} state_preserved="
             f"{r['state_vs_pre_crash']['state_preserved']} hourly_emr={r['hourly']['emr']:.3g} "
             f"daily_emr={r['daily']['emr']:.3g} "
             f"only_online={r['hourly']['only_in_online']} only_oracle={r['hourly']['only_in_oracle']}"
             + (f"  [documented finding: {r['known_finding']['summary']}]"
                if r.get("known_finding") else "")]
    if "resume" in s:
        lines.append(f"resume(H2): pass={s['resume']['pass']} "
                     f"hourly_emr={s['resume']['hourly']['emr']:.3g}")
    lines.append(f"VERDICT: {'PASS' if s.get('pass') else 'FAIL'}")
    return lines


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--engine", required=True,
                    help=f"One of {sorted(CONTAINER)} (containerized), or 'all' (= all containerized).")
    ap.add_argument("--domain", default="environment_airquality",
                    help=f"One domain, or 'all'. Known: {ALL_DOMAINS}")
    ap.add_argument("--rows", type=int, default=20000, help="Rows of the base log to use.")
    ap.add_argument("--container", default=None, help="Override the container name.")
    ap.add_argument("--epsilon", type=float, default=1e-9)
    # 120s was too short: RisingWave rebuilds its state store on restart and
    # blocked all five domains with "engine did not come back within 120s" in the
    # pre-flight, which scores as a harness failure rather than a recovery result.
    # The wait is an upper bound, not a fixed sleep, so raising it costs nothing
    # for engines that come back quickly.
    ap.add_argument("--wait", type=float, default=420.0,
                    help="Max seconds to wait for recovery.")
    ap.add_argument("--resume", action="store_true",
                    help="After state-preserving recovery, ingest H2 and compare to oracle(H1+H2).")
    ap.add_argument("--sweep", action="store_true",
                    help="Exit zero when every cell produced a valid comparison, even if a scientific verdict is FAIL.")
    ap.add_argument("--resume-existing", action="store_true",
                    help="Skip completed cells from this explicit invocation/repeat.")
    args = ap.parse_args()

    if args.engine == "all":
        engines = ALL_ENGINES
    else:
        engines = [args.engine]
    unknown = [e for e in engines if e not in ALL_ENGINES]
    if unknown:
        ap.error(f"unknown engine(s) {unknown}; known: {ALL_ENGINES}")
    domains = ALL_DOMAINS if args.domain == "all" else [args.domain]

    overall_ok = True
    campaign_ok = True
    completed = _completed_cells() if args.resume_existing else set()
    ksql_suffix_base = os.environ.get("KSQL_RUN_SUFFIX", "d5")
    repeat = os.environ.get("EQUITABLE_REPEAT_INDEX", "0")
    for engine in engines:
        for domain in domains:
            cell = (engine, domain, int(args.rows),
                    int(os.environ.get("EQUITABLE_REPEAT_INDEX", "0")))
            if cell in completed:
                print(f"[resume] D5 : {engine} on {domain}")
                continue
            header = f"==== D5 : {engine} on {domain} ===="
            print(header)
            try:
                # Both ksqlDB connections in run_one must resolve the same
                # recovered objects. Derive each cell from the original base so
                # a multi-domain sweep never accumulates prior domain suffixes.
                if engine == "ksqldb":
                    os.environ["KSQL_RUN_SUFFIX"] = (
                        f"{ksql_suffix_base}_{domain}_r{repeat}")
                if engine in IN_PROCESS:
                    summary = _unsupported_summary(
                        engine, domain, args.rows,
                        "in-process adapter has no external process to crash")
                    body = [f"engine={engine} domain={domain} diag=D5",
                            f"UNSUPPORTED: {summary['reason']}"]
                elif engine in BLOCKED:
                    summary = _unsupported_summary(
                        engine, domain, args.rows, BLOCKED[engine])
                    body = [f"engine={engine} domain={domain} diag=D5",
                            f"UNSUPPORTED: {summary['reason']}"]
                else:
                    summary = run_one(engine, domain, args.rows,
                                      args.container or CONTAINER[engine],
                                      args.epsilon, args.wait, args.resume)
                    body = _fmt(summary)
                out_dir = _write_run(summary, [header] + body)
                for b in body:
                    print("  " + b)
                print(f"  run: {out_dir}")
                if summary.get("supported") is not False:
                    overall_ok = overall_ok and bool(summary.get("pass"))
                campaign_ok = campaign_ok and bool(summary.get("artifact_valid"))
            except Exception as exc:
                overall_ok = False
                campaign_ok = False
                summary = {"engine": engine, "domain": domain, "diagnostic": "D5",
                           "pass": False, "artifact_valid": False, "error": str(exc),
                           **_campaign_fields(args.rows),
                           **run_provenance(ROOT)}
                out_dir = _write_run(summary, [header, "ERROR:", traceback.format_exc()])
                print(f"  ERROR: {exc}")
                print(f"  run: {out_dir}")

    print(f"\n{'PASS' if overall_ok else 'FAIL'}: D5 over {len(engines)} engine(s) x {len(domains)} domain(s)")
    return 0 if (campaign_ok if args.sweep else overall_ok) else 1


if __name__ == "__main__":
    raise SystemExit(main())
