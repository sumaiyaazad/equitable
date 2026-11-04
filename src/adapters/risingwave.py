"""RisingWave adapter (PostgreSQL wire, port 4566).

RisingWave updates materialized views asynchronously behind a barrier; the
`FLUSH` statement forces all preceding DML through the barrier so a subsequent
SELECT observes every ingested row. Connection defaults to the playground
image's `root@localhost:4566/dev`.
"""
from __future__ import annotations

import os

from ._postgres_wire import PostgresWireAdapter


class RisingWaveAdapter(PostgresWireAdapter):
    name = "risingwave"
    # RisingWave REJECTS the cross-join formulation ("Not supported:
    # streaming nested-loop join") and needs its native HOP, which
    # reproduces the oracle exactly.
    hopping_mode = "native_hop"

    conn_kwargs = {
        "host": "localhost",
        "port": 4566,
        "dbname": "dev",
        "user": "root",
    }

    def connect(self) -> "RisingWaveAdapter":
        """Connect, then apply an explicit streaming parallelism when one is
        requested (option `parallelism` or EQUITABLE_RISINGWAVE_PARALLELISM).
        Unset, the engine default applies, which is what every scored cell
        uses; only the parallelism sweep sets it."""
        super().connect()
        parallelism = self.options.get(
            "parallelism", os.environ.get("EQUITABLE_RISINGWAVE_PARALLELISM"))
        if parallelism:
            cur = self._cursor()
            cur.execute(f"SET streaming_parallelism = {int(parallelism)};")
            cur.close()
        return self

    def _await_hydrated(self, cur, expected_rows: int) -> None:
        # FLUSH blocks until the checkpoint barrier that includes our INSERTs has
        # propagated to every downstream materialized view.
        cur.execute("FLUSH;")
