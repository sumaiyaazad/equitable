#!/usr/bin/env python3
"""Liveness probe that proves an engine can still answer a query.

A TCP connect only proves something is holding the port open. An engine whose
internal meta service has stopped answering heartbeats can keep accepting
pgwire connections with every submitted query blocked forever, `docker ps`
reporting `healthy` and the shell probe

    timeout 2 bash -c '</dev/tcp/localhost/4566'

succeeding throughout, so a campaign would drive every remaining cell into an
engine that cannot run any of them.

This probe issues real statements and requires correct answers back. "Real" has
to be judged per engine rather than in general: RisingWave's frontend answers
SELECT 1 out of its own process while every meta-dependent statement blocks,
so a probe that only reads is not enough on its own. See _PG_BARRIER. It is still cheap enough to run in a five-second
readiness loop.

Exit status: 0 = engine answered, 1 = engine did not (reason on stderr).
"""
from __future__ import annotations

import argparse
import sys

# Connection settings mirror the adapters (src/adapters/*.py); keep them in step.
_PG_WIRE = {
    "risingwave": {"host": "localhost", "port": 4566,
                   "dbname": "dev", "user": "root"},
    "materialize": {"host": "localhost", "port": 6877,
                    "dbname": "materialize", "user": "materialize"},
}


# A statement the engine cannot answer without the component that actually dies.
#
# SELECT 1 is not that statement for RisingWave. When the meta service stops
# answering heartbeats the frontend keeps serving SELECT 1 out of its own
# process while a FLUSH issued by the adapter sits unanswered, so a SELECT 1
# watchdog passes every probe and never strikes while the lane wedges in
# exactly the way it was written to prevent. FLUSH waits on a barrier
# collected through meta, which is the path every cell depends on.
#
# Materialize needs no second statement: its SELECT 1 is planned and sequenced
# by the coordinator already, so a wedged coordinator fails it outright.
_PG_BARRIER = {"risingwave": "FLUSH"}


def _probe_pg_wire(engine: str, timeout: int) -> None:
    """A read and, where one exists, a barrier the engine cannot fake."""
    import psycopg2

    kwargs = dict(_PG_WIRE[engine], connect_timeout=timeout)
    conn = psycopg2.connect(**kwargs)
    try:
        # Without this a wedged engine accepts the statement and blocks in the
        # cursor rather than in connect(), which is precisely the hang we are
        # trying to detect.
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = '{timeout}s'")
            cur.execute("SELECT 1")
            row = cur.fetchone()
            if row is None or row[0] != 1:
                raise RuntimeError(f"expected 1, got {row!r}")
            barrier = _PG_BARRIER.get(engine)
            if barrier is not None:
                # statement_timeout is enforced by the engine, so an engine
                # wedged deeply enough can swallow it and leave this blocked.
                # Callers run this process under a hard external timeout so
                # that case is a failed probe rather than a silent probe.
                cur.execute(barrier)
    finally:
        conn.close()


def _probe_ksqldb(timeout: int) -> None:
    """Run a statement, not /info: /info is served without touching Kafka."""
    import requests

    response = requests.post(
        "http://localhost:8088/ksql",
        json={"ksql": "SHOW STREAMS;"},
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        timeout=timeout,
    )
    response.raise_for_status()
    body = response.json()
    if not isinstance(body, list) or not body:
        raise RuntimeError(f"unexpected SHOW STREAMS response: {body!r}")


def _probe_spark_driver(timeout: int) -> None:
    """The control service's /status, which calls into the driver's JVM.

    A port check is not enough here either: a driver that has run out of file
    descriptors keeps its container up and port 8090 open while accepting and
    then dropping every request. /status answers without taking the setup
    lock, so a busy driver still answers it.
    """
    import requests

    response = requests.get("http://localhost:8090/status", timeout=timeout)
    response.raise_for_status()
    body = response.json()
    if not isinstance(body, dict) or body.get("ready") is not True:
        raise RuntimeError(f"spark driver not ready: {body!r}"[:500])


def _probe_feldera(timeout: int) -> None:
    import requests

    requests.get("http://localhost:8080/healthz",
                 timeout=timeout).raise_for_status()


def probe(engine: str, timeout: int) -> None:
    """Raise if the engine is reachable but not serving; return if it is fine.

    A missing client library means this check could not be made, not that the
    engine is down. Failing closed on ImportError would make every readiness
    wait time out and strand the whole campaign, which is a far worse failure
    than the one this script exists to catch, so that case defers to the
    caller's TCP-level check instead.
    """
    try:
        if engine in _PG_WIRE:
            _probe_pg_wire(engine, timeout)
        elif engine == "ksqldb":
            _probe_ksqldb(timeout)
        elif engine == "feldera":
            _probe_feldera(timeout)
        elif engine == "spark":
            _probe_spark_driver(timeout)
        else:
            # kafka, the Flink cluster services, the individual Spark services,
            # reference: their HTTP readiness answer in the caller is already a
            # serving check, or they have no query endpoint of their own here.
            # Returning cleanly keeps the caller's TCP-level check
            # authoritative rather than inventing a check we cannot make.
            return
    except ImportError as exc:
        print(f"{engine}: cannot query-probe ({exc}); "
              "falling back to the caller's TCP check", file=sys.stderr)
        return


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--engine", required=True)
    ap.add_argument("--timeout", type=int, default=10,
                    help="Seconds allowed for both connect and statement.")
    args = ap.parse_args()
    try:
        probe(args.engine, args.timeout)
    except Exception as exc:  # noqa: BLE001 - any failure means "not serving"
        print(f"{args.engine}: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
