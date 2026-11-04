#!/usr/bin/env python3
"""
verify_persistence.py -- D5 prerequisite check (scripts/run/run_d5_recovery.py): does a
containerized engine survive `docker restart` with its materialized-view state
INTACT? D5 can only produce a meaningful result on engines whose state is durable
across a crash+restart; this settles that empirically, per engine.

    ingest N rows -> read view (K1) -> docker restart <container> -> wait healthy
    -> reconnect without setup() -> read view (K2) -> ingest M further rows on a
    fresh connection, still without setup() -> read view again (C3).
    PASS iff K1 > 0, K2 == K1, and the view's total count grew past C1.

The last step is D5's resume step in miniature. A restored view that nothing
maintains any more still passes K2 == K1, so a harness that terminated the
recovered queries between the two reads would pass this check and still block
every D5 cell.

Skipping setup() on reconnect is the whole point: setup() would recreate (wipe)
the view, so reading it after a bare reconnect proves the engine restored it.

A FAIL where K2 == 0 means the engine came back empty -> D5 is N/A for it until a
durable profile is configured. Bring the stack up with the durability overlay
first:
    docker compose -f docker/streaming-engines.yml \
                   -f docker/streaming-engines.durable.yml up -d

Coverage: Materialize, RisingWave, ksqlDB, Feldera, Flink, and Spark. Their adapters reconnect and read existing views without setup. The pinned open-source Feldera image activates storage but gates checkpointing to the enterprise edition, so its empirical check is expected to fail and production D5 records N/A. Flink and Spark must also show a restore from their checkpoints before the read counts, because their outputs outlive a restart. The reference engine is in-process and therefore N/A.

WARNING: restarts containers -- do NOT run while a diagnostic sweep is in flight.

Inputs  : a running (durably-configured) engine container
Outputs : runs/<UTC>_persistence_<engine>/summary.json ; PASS/FAIL
Run as  : python scripts/run/verify_persistence.py --engine risingwave
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.adapters import get_adapter, canonical_input  # noqa: E402

# engine -> compose container name (project "docker"). Override with --container.
CONTAINER = {
    "materialize": "docker-materialize-1",
    "risingwave": "docker-risingwave-1",
    "ksqldb": "docker-ksqldb-1",
    "feldera": "docker-feldera-1",
    "flink": "docker-flink-taskmanager-1",
    "spark": "docker-spark-driver-1",
}
COVERED = ("materialize", "risingwave", "ksqldb", "feldera", "flink", "spark")
IN_PROCESS = ("reference",)


def _docker(*args: str) -> None:
    subprocess.run(["docker", *args], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def _count_total(view: pd.DataFrame) -> int:
    return int(view["count"].sum()) if len(view) else 0


def _read_hourly_keys(engine: str, retries: int, delay: float) -> int:
    """Reconnect (no setup) and read the persisted hourly view; retry while the
    engine is still coming back / rebuilding state.

    detach(), not close(): the resume step after this reads the same view and
    needs its maintaining queries still running."""
    last = None
    for _ in range(retries):
        try:
            eng = get_adapter(engine).connect()
            try:
                # Flink and Spark must show a restore from their checkpoints
                # before a read counts; their outputs survive a restart either way.
                if engine in ("flink", "spark"):
                    eng.resume()
                n = int(len(eng.read_view("hourly")))
                if n > 0:
                    return n
                last = 0
            finally:
                eng.detach()
        except Exception as exc:  # container still restarting / not ready
            last = exc
        time.sleep(delay)
    return last if isinstance(last, int) else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--engine", required=True)
    ap.add_argument("--container", default=None, help="override the container name")
    ap.add_argument("--domain", default="environment_airquality")
    ap.add_argument("--rows", type=int, default=2000, help="marker rows to ingest")
    ap.add_argument("--resume-rows", type=int, default=500,
                    help="further rows ingested after the restart")
    ap.add_argument("--wait", type=float, default=90.0, help="max seconds to wait for recovery")
    args = ap.parse_args()

    if args.engine in IN_PROCESS:
        print(f"{args.engine}: N/A -- in-process engine, no container to restart.")
        return 0
    if args.engine not in COVERED:
        print(f"{args.engine}: unsupported persistence target. Covered: {COVERED}.")
        return 0
    container = args.container or CONTAINER[args.engine]

    # Pin the ksqlDB object suffix. The adapter's precedence is
    # run_suffix option > KSQL_RUN_SUFFIX > a RANDOM uuid, so without this every
    # reconnect invents a new name and reads a table that was never created:
    # K2 would be 0 for reasons that have nothing to do with durability. Both
    # connections here must resolve the same objects, exactly as
    # run_d5_recovery.py pins them per cell.
    if args.engine == "ksqldb":
        os.environ.setdefault(
            "KSQL_RUN_SUFFIX", f"persist_{args.domain}")

    log = ROOT / "data/normalized" / args.domain / "log.parquet"
    if not log.exists():
        raise SystemExit(f"FAIL: {log} not found -- ingest '{args.domain}' first.")
    both = canonical_input(pd.read_parquet(log).head(args.rows + args.resume_rows))
    df, tail = both.iloc[:args.rows], both.iloc[args.rows:]

    # 1) ingest + record the maintained state
    eng = get_adapter(args.engine).connect()
    eng.setup(variant="dedup")
    eng.ingest_log(df)
    before = eng.read_view("hourly")
    k1, c1 = int(len(before)), _count_total(before)
    # detach, NOT close: ksqlDB's close() issues TERMINATE for every persistent
    # query, and those queries are what maintains the table. Closing here would
    # stop the state we are about to test, making K2=0 a certainty regardless of
    # durability. run_d5_recovery.py never hit this because it SIGKILLs the
    # container instead of shutting the adapter down gracefully.
    eng.detach()
    print(f"ingested {len(df)} rows -> hourly keys K1={k1}")

    # 2) crash+restart the container (D5 uses restart: preserves the FS)
    print(f"docker restart {container} ...")
    _docker("restart", container)

    # 3) reconnect WITHOUT setup and read the persisted view
    k2 = _read_hourly_keys(args.engine, retries=int(args.wait // 3) + 1, delay=3.0)
    print(f"after restart -> hourly keys K2={k2}")

    survived = bool(k1 > 0 and k2 == k1)

    # 4) resume: rows the view has never seen, on a fresh connection and still
    # without setup(). A view whose queries survived takes them in; one that
    # nothing maintains keeps its restored total, or fails the read outright.
    c3, resume_error = None, None
    if survived and len(tail):
        eng = get_adapter(args.engine).connect()
        try:
            eng.ingest_log(tail)
            c3 = _count_total(eng.read_view("hourly"))
        except Exception as exc:  # noqa: BLE001 - recorded, and fails the check
            resume_error = f"{type(exc).__name__}: {exc}"
        finally:
            eng.close()
        print(f"after resuming with {len(tail)} rows -> hourly count total "
              f"C3={c3} (C1={c1})" + (f"; {resume_error}" if resume_error else ""))
    resumed = bool(len(tail) == 0 or (c3 is not None and c3 > c1))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = ROOT / "runs" / f"{stamp}_persistence_{args.engine}"
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "engine": args.engine, "container": container, "domain": args.domain,
        "rows": int(len(df)), "hourly_keys_before": k1, "hourly_keys_after": k2,
        "state_survived_restart": survived,
        "resume_rows": int(len(tail)), "hourly_count_before": c1,
        "hourly_count_after_resume": c3, "resume_error": resume_error,
        "view_maintained_after_restart": resumed,
        "note": ("state persisted across docker restart and kept being "
                 "maintained -> D5 applicable" if survived and resumed else
                 "state persisted but the view stopped taking new rows -> D5 "
                 "resume would block" if survived else
                 "state did NOT survive restart -> D5 N/A until durable profile fixed"),
        "utc": stamp,
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    ok = survived and resumed
    print(f"\nRESULT: {'PASS' if ok else 'FAIL'} -- {summary['note']}")
    print(f"wrote {out_dir / 'summary.json'}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
