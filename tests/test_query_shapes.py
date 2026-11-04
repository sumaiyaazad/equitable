"""The order-sensitive query family and its two oracles.

The reported R2 gap is measured on one query shape under three interventions,
so a reader may ask whether it is a property of engines or of the one query we
wrote. `src/diagnostics/shapes.py` answers by enumerating the family the gap
should hold over. These tests pin the three properties that make the family
worth reporting, because a shape failing any of them would inflate or deflate
the gap without anyone noticing:

  soundness      the contract order is invariant to delivery permutation
  sensitivity    the arrival order is not
  independence   two separately written oracles agree on every shape

Without soundness a shape is not a contract; without sensitivity it is not
order-sensitive and contributes a free `attributable`; without independence a
shared bug in one oracle would be indistinguishable from an engine finding.
"""
from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from src.diagnostics import shapes
from src.diagnostics.shapes import ARRIVAL_ORDER, CONTRACT_ORDER, SHAPES
from src.oracle import shape_oracle


def _log(rows: int = 4_000, *, seed: int) -> pd.DataFrame:
    """A synthetic log with ties, repeated entities, and shuffled delivery.

    Ties on event time matter: they are where the contract's dedupe_id
    tiebreak and an engine's arrival tiebreak visibly part company.
    """
    rng = np.random.default_rng(seed)
    start = pd.Timestamp("2026-06-01T00:00:00Z")
    minutes = rng.integers(0, 60 * 24, size=rows)
    frame = pd.DataFrame({
        "event_time_ts": [start + pd.Timedelta(minutes=int(m)) for m in minutes],
        "entity": rng.choice([f"e{i}" for i in range(12)], size=rows),
        "metric": rng.choice(["fare", "distance"], size=rows),
        "value": np.round(rng.uniform(1.0, 100.0, size=rows), 4),
        "dedupe_id": [f"d{i:06d}" for i in range(rows)],
        "processing_order": rng.permutation(rows),
    })
    frame["event_time_utc"] = frame["event_time_ts"].dt.strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ")
    frame["bucket"] = frame["event_time_ts"].dt.floor("h")
    return frame


def _repermute(frame: pd.DataFrame, seed: int) -> pd.DataFrame:
    out = frame.copy()
    out["processing_order"] = np.random.default_rng(seed).permutation(len(out))
    return out


def _canonical(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.reset_index(drop=True).copy()
    for column in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[column]):
            out[column] = pd.to_datetime(out[column], utc=True).dt.strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ")
        elif out[column].dtype == object:
            out[column] = out[column].astype(str)
        elif pd.api.types.is_float_dtype(out[column]):
            out[column] = out[column].round(9)
    return out


class QueryFamilyTests(unittest.TestCase):
    def test_every_shape_is_invariant_under_the_contract_order(self) -> None:
        """Permuting delivery must not move a contract answer."""
        a, b = _repermute(_log(seed=1), 21), _repermute(_log(seed=1), 22)
        for name, shape in sorted(SHAPES.items()):
            with self.subTest(shape=name):
                self.assertTrue(
                    _canonical(shape.expected(a, CONTRACT_ORDER)).equals(
                        _canonical(shape.expected(b, CONTRACT_ORDER))),
                    f"{name} is not replay-invariant under the contract order")

    def test_every_shape_moves_under_the_arrival_order(self) -> None:
        """A shape blind to delivery order contributes nothing to the gap."""
        a, b = _repermute(_log(seed=2), 31), _repermute(_log(seed=2), 32)
        for name, shape in sorted(SHAPES.items()):
            with self.subTest(shape=name):
                self.assertFalse(
                    _canonical(shape.expected(a, ARRIVAL_ORDER)).equals(
                        _canonical(shape.expected(b, ARRIVAL_ORDER))),
                    f"{name} is insensitive to arrival order, so it does not "
                    "belong in an order-sensitive family")

    def test_the_two_oracles_agree_on_every_shape_and_order(self) -> None:
        frame = _repermute(_log(seed=3), 41)
        for name, shape in sorted(SHAPES.items()):
            for order in (CONTRACT_ORDER, ARRIVAL_ORDER):
                with self.subTest(shape=name, order=order):
                    self.assertTrue(
                        _canonical(shape.expected(frame, order)).equals(
                            _canonical(shape_oracle.expected(frame, name, order))),
                        f"the pandas and SQL oracles disagree on {name}/{order}")

    def test_the_filter_shape_can_move_the_key_set(self) -> None:
        """S5 is the one shape whose divergence is structural, not metric.

        Every other shape returns the same keys under both orders and differs
        only in a value. S5 changes group membership, which is the case the
        key-union comparison catches directly, so the family covers both kinds
        of failure rather than only the numeric one.
        """
        frame = _repermute(_log(seed=4), 51)
        shape = SHAPES["S5_filter_on_latest"]
        self.assertFalse(shape.key_set_stable)
        contract = shape.expected(frame, CONTRACT_ORDER)
        arrival = shape.expected(frame, ARRIVAL_ORDER)
        keys = ["bucket", "entity", "metric"]
        only_one_side = (
            set(map(tuple, contract[keys].values))
            ^ set(map(tuple, arrival[keys].values))
        )
        self.assertTrue(only_one_side, "S5 never moved a key on this input")

    def test_the_family_is_enumerated_not_hand_written(self) -> None:
        family = shapes.enumerate_family(["fare", "distance"], ("hourly", "daily"))
        self.assertEqual(len(family), len(SHAPES) * 2 * 2)
        self.assertEqual({m["shape"] for m in family}, set(SHAPES))
        for member in family:
            self.assertEqual(member["orders"], list(shapes.ORDERS))
        with self.assertRaises(ValueError):
            shapes.enumerate_family(["fare"], shapes=["S9_not_a_shape"])
        with self.assertRaises(ValueError):
            shapes.enumerate_family([])

    def test_the_baseline_shape_is_the_one_the_paper_reports(self) -> None:
        """The family carries its own baseline so the gap is comparable."""
        self.assertIn(shapes.BASELINE_SHAPE, SHAPES)
        self.assertEqual(SHAPES[shapes.BASELINE_SHAPE].schema, shapes.BUCKET_SCHEMA)

    def test_gap_matches_the_grid_attribution_vocabulary(self) -> None:
        """One scale for the generated family and the hand-written grid."""
        self.assertEqual(shapes.gap(True, True), "attributable")
        self.assertEqual(shapes.gap(True, False), "repaired")
        self.assertEqual(shapes.gap(False, True), "repair_divergent")
        self.assertEqual(shapes.gap(False, False), "repair_divergent")

    def test_every_shape_emits_a_schema_the_comparator_understands(self) -> None:
        """No shape may need a third output schema."""
        for name, shape in sorted(SHAPES.items()):
            with self.subTest(shape=name):
                self.assertIn(shape.schema, (shapes.BUCKET_SCHEMA, shapes.TOPK_SCHEMA))
                expected = (shapes.BUCKET_COLUMNS if shape.schema == shapes.BUCKET_SCHEMA
                            else shapes.TOPK_COLUMNS)
                table = shape.expected(_repermute(_log(seed=5), 61), CONTRACT_ORDER)
                self.assertEqual(list(table.columns), expected)


if __name__ == "__main__":
    unittest.main()


class _FakeAdapter:
    """An adapter that returns whatever the shape's own oracle would.

    `order_it_actually_uses` decides which selection order the fake engine
    honours, so a test can build an engine that is faithful to the contract, or
    one that silently resolves by arrival order, without a container.
    """

    name = "fake"
    supported_shapes = tuple(SHAPES)

    def __init__(self, order_it_actually_uses: str | None = None) -> None:
        self.forced = order_it_actually_uses
        self._shape = None
        self._order = None
        self._frame = None

    def setup_shape(self, *, shape: str, order: str) -> None:
        self._shape, self._order = shape, order

    def ingest_log(self, frame):
        self._frame = frame
        return None

    def read_shape(self, grain: str):
        from src.diagnostics.shape_runner import _prepare
        order = self.forced or self._order
        prepared = _prepare(self._frame, grain)
        return SHAPES[self._shape].expected(prepared, order)


class ShapeScoringTests(unittest.TestCase):
    """R2 scores BOTH arms against the contract oracle.

    Scoring each arm against an oracle computed under its own order asks only
    whether the engine executes each formulation faithfully. Every arm passes
    that, so the measured gap collapses to zero and the experiment silently
    reports nothing. These tests pin the scoring so that regression is loud.

    They also pin what each verdict means, which is easy to get backwards: an
    engine that honours the order it is asked for is `repaired` whenever
    delivery is unordered, because its arrival-ordered arm still disagrees with
    the contract. `attributable` requires delivery in which the two orders
    coincide, and that is exactly the condition under which the gap vanishes.
    """

    def _run(self, adapter, shape: str, frame):
        from src.diagnostics.shape_runner import run_shape_pair
        return run_shape_pair(adapter, frame, shape=shape, grain="hourly")

    def test_ordered_delivery_makes_every_shape_attributable(self) -> None:
        """When arrival and event time agree, no formulation can differ."""
        frame = _log(seed=6).sort_values(["event_time_ts", "dedupe_id"]).copy()
        frame["processing_order"] = range(len(frame))
        for name in sorted(SHAPES):
            with self.subTest(shape=name):
                result = self._run(_FakeAdapter(), name, frame)
                self.assertFalse(result.get("oracle_disagreement"))
                self.assertEqual(result["attribution"], "attributable")

    def test_unordered_delivery_makes_every_shape_repaired(self) -> None:
        """The gap: the contract arm passes only because we wrote the rule."""
        frame = _repermute(_log(seed=6), 71)
        for name in sorted(SHAPES):
            with self.subTest(shape=name):
                result = self._run(_FakeAdapter(), name, frame)
                self.assertTrue(result["contract_equivalent"])
                self.assertFalse(result["arrival_equivalent"])
                self.assertEqual(result["attribution"], "repaired")

    def test_an_engine_that_ignores_the_requested_order_is_divergent(self) -> None:
        """An engine with no event-time selection fails the scored arm too."""
        frame = _repermute(_log(seed=6), 71)
        for name in sorted(SHAPES):
            with self.subTest(shape=name):
                result = self._run(_FakeAdapter(ARRIVAL_ORDER), name, frame)
                self.assertFalse(result["contract_equivalent"])
                self.assertEqual(result["attribution"], "repair_divergent")

    def test_an_engine_that_cannot_express_a_shape_is_unsupported(self) -> None:
        adapter = _FakeAdapter()
        adapter.supported_shapes = ()
        result = self._run(adapter, "S1_latest", _repermute(_log(seed=6), 71))
        self.assertEqual(result["attribution"], "unsupported")
        self.assertFalse(result["supported"])

    def test_a_ranked_value_one_ulp_off_is_within_the_contract_tolerance(self) -> None:
        """The rank check applies the contract's tau, not bit equality.

        RisingWave returns some double values one unit in the last place off
        the oracle's (41.8642 against 41.864200000000004). The scored
        comparator accepts that, so the family must too, or the member reads
        as repair_divergent for a difference the contract does not see.
        """
        from src.diagnostics.shape_runner import _compare
        from src.diagnostics.shapes import TOPK_SCHEMA
        oracle = pd.DataFrame({
            "bucket": [pd.Timestamp("2024-06-01T08:00:00Z")] * 2,
            "metric": ["speed", "speed"], "rank": [1, 2],
            "entity": ["Home_1", "Away_2"], "rank_value": [41.8642, 10.0]})
        engine = oracle.copy()
        engine.loc[0, "rank_value"] = 41.864200000000004
        self.assertTrue(
            _compare(TOPK_SCHEMA, engine, oracle, 1e-9, "hourly")["equivalent"])
        engine.loc[0, "rank_value"] = 41.8643
        verdict = _compare(TOPK_SCHEMA, engine, oracle, 1e-9, "hourly")
        self.assertFalse(verdict["equivalent"])
        self.assertEqual(verdict["value_mismatches"], 1)

    def test_the_order_insensitive_reductions_do_not_decide_the_verdict(self) -> None:
        """count, sum, min, max and mean cannot move under either order.

        Letting them decide would make the family re-measure the reduction
        findings the main grid already reports, and on an engine with a known
        numeric divergence that signal swamps the order signal entirely.
        """
        from src.diagnostics.shape_runner import ORDER_DETERMINED_FIELDS
        self.assertEqual(set(ORDER_DETERMINED_FIELDS),
                         {"latest_value", "latest_event_time"})
        for field in ("count", "sum", "min", "max", "mean"):
            self.assertNotIn(field, ORDER_DETERMINED_FIELDS)


class QueryFamilyPromotionTests(unittest.TestCase):
    """The family is promotable only with the control that gives it meaning.

    The reported gap is the fraction of members matching the oracle under the
    contract formulation alone. Replayed in event-time order the two
    formulations coincide, so the same members must show no gap; a cohort that
    reports one there is detecting how the family was built rather than how the
    engine behaves, and must not reach a table.
    """

    def _summary(self, **overrides):
        rows = []
        for delivery in ("shuffled", "ordered"):
            for shape in sorted(SHAPES):
                rows.append({
                    "engine": "materialize", "domain": "transportation_taxi",
                    "grain": "hourly", "shape": shape, "delivery": delivery,
                    "attribution": ("repaired" if delivery == "shuffled"
                                    else "attributable"),
                    "contract_equivalent": True,
                    "arrival_equivalent": delivery == "ordered",
                })
        summary = {
            "kind": "query_family", "rows": rows,
            "expressible_cells": len(SHAPES),
            "repaired_cells": len(SHAPES),
            "control_all_attributable": True,
            "oracle_disagreements": 0, "blocked": 0,
        }
        summary.update(overrides)
        return summary

    def test_a_complete_cohort_with_its_control_is_promotable(self) -> None:
        from scripts.results._scope import official_scope_failure
        self.assertIsNone(
            official_scope_failure("query_family", self._summary()))

    def test_a_cohort_without_the_control_is_rejected(self) -> None:
        from scripts.results._scope import official_scope_failure
        summary = self._summary()
        summary["rows"] = [r for r in summary["rows"]
                           if r["delivery"] == "shuffled"]
        failure = official_scope_failure("query_family", summary)
        self.assertIsNotNone(failure)
        self.assertIn("control", failure)

    def test_a_control_that_shows_a_gap_is_rejected(self) -> None:
        # The verdict is recomputed from the cells, so the gap must be in them:
        # every engine that expresses the member diverges under ordered delivery.
        from scripts.results._scope import official_scope_failure
        summary = self._summary(control_all_attributable=False)
        for row in summary["rows"]:
            if row["delivery"] == "ordered":
                row["attribution"] = "repaired"
        failure = official_scope_failure("query_family", summary)
        self.assertIsNotNone(failure)
        self.assertIn("own construction", failure)

    def test_an_oracle_disagreement_voids_the_cohort(self) -> None:
        from scripts.results._scope import official_scope_failure
        failure = official_scope_failure(
            "query_family", self._summary(oracle_disagreements=1))
        self.assertIsNotNone(failure)
        self.assertIn("harness error", failure)

    def test_a_blocked_member_voids_the_cohort(self) -> None:
        from scripts.results._scope import official_scope_failure
        failure = official_scope_failure("query_family", self._summary(blocked=1))
        self.assertIsNotNone(failure)

