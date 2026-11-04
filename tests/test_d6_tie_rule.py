"""D6 ranks entities whose oracle values are within the contract tolerance as
tied, so noise that C3 accepts cannot fail a ranking check, while a real rank or
membership error still does."""
import unittest

import pandas as pd

from src.diagnostics.d6 import compare_topk, topk_table

EPS = 1e-9


def _view(values):
    return pd.DataFrame([
        {"bucket": "2024-06-04T06:00:00Z", "metric": "fare", "entity": e, "mean": v}
        for e, v in values.items()
    ])


def _cmp(engine_vals, oracle_vals, k, **kw):
    ev, ov = _view(engine_vals), _view(oracle_vals)
    return compare_topk(topk_table(ev, k, "mean"), topk_table(ov, k, "mean"),
                        oracle_view=ov, rank_metric="mean", epsilon=EPS, **kw)


class D6TieRule(unittest.TestCase):
    def test_campaign_witness_last_bit_swap_is_equivalent(self):
        # The reference backend's own divergence at 1M taxi rows: C3 accepts both
        # means, and exact ranking swapped zones 164 and 186.
        engine = {"231": 21.9, "164": 16.533333333333335, "186": 16.533333333333335,
                  "249": 15.716666666666669}
        oracle = {"231": 21.9, "186": 16.533333333333335, "164": 16.53333333333333,
                  "249": 15.716666666666667}
        exact = compare_topk(topk_table(_view(engine), 4, "mean"),
                             topk_table(_view(oracle), 4, "mean"))
        assert not exact["equivalent"]
        tie = _cmp(engine, oracle, 4)
        assert tie["equivalent"] and tie["tie_resolved_groups"] == 1


    def test_boundary_swap_between_tied_entities_is_equivalent(self):
        oracle = {"a": 3.0, "b": 2.0, "c": 2.0 + 1e-12}
        engine = {"a": 3.0, "b": 2.0 + 1e-12, "c": 2.0}
        # top-2: oracle keeps c (larger), engine keeps b; both tied with the boundary
        assert _cmp(engine, oracle, 2)["equivalent"]


    def test_real_rank_error_still_fails(self):
        oracle = {"a": 3.0, "b": 2.0, "c": 1.0}
        engine = {"a": 3.0, "b": 1.0, "c": 2.0}
        r = _cmp(engine, oracle, 3)
        assert not r["equivalent"] and r["rank_mismatches"] == 1


    def test_real_membership_error_still_fails(self):
        oracle = {"a": 3.0, "b": 2.0, "c": 1.0}
        engine = {"a": 3.0, "b": 0.5, "c": 1.0}
        r = _cmp(engine, oracle, 2)
        assert not r["equivalent"] and r["set_mismatches"] == 1


    def test_difference_beyond_tolerance_is_not_a_tie(self):
        oracle = {"a": 26.8, "b": 26.8 - 1e-6}
        engine = {"a": 26.8 - 2e-6, "b": 26.8 - 1e-6}
        assert not _cmp(engine, oracle, 2)["equivalent"]


    def test_corrupted_entity_is_detected(self):
        oracle = {"a": 3.0, "b": 2.0}
        ev = topk_table(_view(oracle), 2, "mean")
        ev.loc[0, "entity"] = "a__rank_corrupt"
        r = compare_topk(ev, topk_table(_view(oracle), 2, "mean"), oracle_view=_view(oracle),
                         rank_metric="mean", epsilon=EPS)
        assert not r["equivalent"]


    def test_non_finite_is_never_tied(self):
        oracle = {"a": float("nan"), "b": 1.0}
        engine = {"a": 1.0, "b": float("nan")}
        ev, ov = _view(engine), _view(oracle)
        r = compare_topk(topk_table(ev, 2, "mean"), topk_table(ov, 2, "mean"),
                         oracle_view=ov, rank_metric="mean", epsilon=EPS)
        assert not r["equivalent"]


if __name__ == "__main__":
    unittest.main()
