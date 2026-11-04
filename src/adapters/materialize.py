"""Materialize adapter (PostgreSQL wire, port 6877).

Materialize maintains incrementally-updated materialized views over a base table.
After each INSERT batch, `_await_hydrated` reads the base table and both views in
one statement. Materialize must serve that statement at one logical timestamp,
which makes it a frontier barrier rather than a cardinality-based quiet-period
heuristic. Empirically re-derived finding to reproduce here: the
IEEE-754 reduction-order drift on parallel dataflow folds (D3), surfaced by the
`sum` field under the magnitude-relative epsilon.
"""
from __future__ import annotations

import time

from ._postgres_wire import PostgresWireAdapter, _VIEW
from ._sql_views import STREAM_TABLE


class MaterializeAdapter(PostgresWireAdapter):
    name = "materialize"
    # No native hopping primitive, but the cross-join formulation plans and
    # maintains correctly, so Materialize is SUPPORTED for D8. Verified
    # against v26.25.0 rather than assumed from the absence of HOP.
    hopping_mode = "crossjoin"

    conn_kwargs = {
        "host": "localhost",
        "port": 6877,
        "dbname": "materialize",
        "user": "materialize",
    }

    def _await_hydrated(self, cur, expected_rows: int) -> None:
        """Wait for one timestamp shared by the input and both maintained views.

        Materialize serves every relation referenced by one SELECT at a common
        logical timestamp. Reading the source and both views in one statement is
        therefore a real frontier barrier; observing an unchanged number of view
        keys twice is not (values can still change at constant cardinality).
        """
        timeout = float(self.options.get("hydrate_timeout_s", 30))
        deadline = time.time() + timeout
        interval = float(self.options.get("hydrate_poll_interval_s", 0.3))
        last_counts = None
        while time.time() < deadline:
            cur.execute(f"""
                SELECT
                  (SELECT count(*) FROM {STREAM_TABLE}) AS source_rows,
                  (SELECT count(*) FROM {_VIEW['hourly']}) AS hourly_keys,
                  (SELECT count(*) FROM {_VIEW['daily']}) AS daily_keys;
            """)
            source_rows, hourly_keys, daily_keys = map(int, cur.fetchone())
            last_counts = (source_rows, hourly_keys, daily_keys)
            if source_rows >= expected_rows:
                return
            time.sleep(interval)
        detail = "no counts observed" if last_counts is None else (
            "last counts: source_rows={}, hourly_keys={}, daily_keys={}".format(
                *last_counts))
        raise RuntimeError(
            f"materialize views did not reflect at least {expected_rows} input "
            f"records within {timeout:g}s ({detail})")
