"""Candidate engine-finding signatures -- shared attribution logic.

These signatures preserve hypotheses from earlier exploratory observations so a
new campaign can test whether the same output shape recurs. Their presence in
source code is not evidence that an engine diverges in the promoted cohort.
Every scorer records a signature only after the current output reproduces its
full structural and magnitude envelope, while the contract-equivalence verdict
remains a failure. Generated paper text may report the attribution only from
that reproduced record. Signature matching distinguishes a repeated output
shape; anything outside the signature also fails.

The signatures are observational. A match says the divergence has the recorded
shape and magnitude; it does not establish how the engine accumulates
internally, and no code here should be read as a claim about that. The
acceptance band below is a magnitude bound, not a test of grid membership: a
divergence of the right shape and size matches even if its values do not lie on
the 2^-24 grid exactly. Tightening the band into an actual grid test would make
the signature stricter and is not done mid-campaign, because it
would classify later shards differently from earlier ones.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from src.oracle.comparator import KEY_COLUMNS

FLOAT32_ULP = 2.0 ** -24   # unit roundoff of IEEE-754 single precision (~5.96e-8)

# Magnitude floor for the strict band. Only a genuine zero should reach it;
# it exists so an exact 0.0 does not collapse the tolerance to nothing.
TINY_ABS = 1e-12

# False keeps the calibration every scored shard shares. Flip it only for a
# rescore that covers every shard at once, not mid-campaign.
STRICT_ACCUMULATION_MODEL = False

# ksqlDB's two candidate signatures are recorded but not attributed by default.
# Turning them on moves blocks out of `divergence` and into
# `known_divergence`, which is a change to what the campaign reports, so it is
# a decision to rescore every shard under, not a mid-campaign flip. See KSQLDB_CANDIDATES for what each one claims.
ENABLE_KSQLDB_FINDINGS = False

KNOWN_FINDINGS: Dict[str, Dict[str, Any]] = {
    "materialize": {
        # States exactly what `reproduces_finding` verified: shape (only these
        # fields moved) and size (within the band). not grid membership -- the
        # matcher never tests that -- and not an internal mechanism.
        "summary": "sum/mean-only divergence within the declared magnitude "
                   "envelope (deterministic, even on single-row groups)",
        "diverging_fields": {"sum", "mean"},   # ONLY these may diverge from the oracle
        # The band is sized from a float32 *accumulation* model, not a single-op
        # ULP: a float32 sum of N summands would carry worst-case relative error
        # ~(N-1)*u. The model sizes the band; it is not asserted to be what the
        # engine does. So the per-key tolerance scales with that bucket's summand
        # count (`count`), which is why a low-cardinality domain like
        # finance_markets (6 entities => ~2.4e4 rows/bucket) drifts ~1e-6 while a
        # high-cardinality domain (tiny buckets) stays under a single ULP. safety
        # is headroom over the worst case; a genuine arithmetic error additionally
        # shows up as a count/key/min/max mismatch, which is rejected regardless.
        "accum_ulp": FLOAT32_ULP,
        "count_safety": 4.0,
        "rel_tol": 1e-6,   # representative single-op float32 band (display only)
        "recovering_patch": "sum(value::numeric)::double precision",
    },
}


# Shape-only signatures: a field set plus structural cleanliness, with no
# magnitude bound. A magnitude band is meaningful only when the divergence is
# a rounding artefact. These two are not -- the values differ outright (degree
# ~1e9, i.e. the contract tolerance exceeded a billionfold) -- so a band would
# be theatre. What distinguishes them is WHICH fields move and that nothing
# structural does.
#
# Both are observational, in the same sense as the materialize entry: they
# record a recurring output shape. Neither establishes how ksqlDB orders or
# suppresses anything internally, and no code here should be read as a claim
# about that.
KSQLDB_CANDIDATES: Dict[str, Dict[str, Any]] = {
    "latest_arrival_ordered": {
        # Confined to D2, D4 and D5, the diagnostics that vary arrival order
        # and late arrival, and never structural. sum/min/max/count all agree;
        # only the two order-sensitive fields move.
        "summary": "latest_event_time/latest_value-only divergence, "
                   "structurally clean (order-sensitive fields only)",
        "diverging_fields": {"latest_event_time", "latest_value"},
        "shape_only": True,
        "recovering_patch": None,
    },
    "duplicate_suppression_count": {
        # 68 blocks, 66 of them D1 (source-identity duplicate suppression).
        # count moves, which is an EXACT field, so this is a genuine
        # disagreement about which rows exist -- recorded to be reported as a
        # named divergence, not to soften it.
        "summary": "count/sum divergence under source-identity duplicate "
                   "suppression (exact-field disagreement)",
        "diverging_fields": {"count", "sum", "mean"},
        "shape_only": True,
        "recovering_patch": None,
    },
}


def _findings_for(engine: str) -> Optional[Dict[str, Any]]:
    """The active signature for `engine`, honouring the ksqlDB gate."""
    if engine == "ksqldb":
        return None if not ENABLE_KSQLDB_FINDINGS else KSQLDB_CANDIDATES["latest_arrival_ordered"]
    return KNOWN_FINDINGS.get(engine)


def _within_accumulation_band(finding: Dict[str, Any],
                              engine_df: pd.DataFrame, oracle_df: pd.DataFrame) -> bool:
    """True iff every diverging field is within the accumulation-model band,
    with the per-key tolerance scaled by that bucket's summand count.

    This is a magnitude bound only. It does not check that values lie on the
    2^-24 grid, and a match is not evidence of any particular internal design.

    Tolerance for key k and field f, with n_k the number of rows aggregated into
    the bucket (the number of float32 additions behind its maintained `sum`, and
    hence `mean`):

        strict model    u * (1 + safety * (n_k - 1)) * scale
        legacy model    safety * n_k * u * max(1, |a|, |b|)

    The strict model is the one the prose describes: one unit of storage
    quantization always, plus the (n-1) additions that actually happen. At n=1
    it admits a single ULP -- storage quantization and nothing more -- so a
    single-row group cannot absorb four ULP of unexplained error. The legacy
    model is retained because tightening the band mid-campaign would classify
    later shards differently from earlier ones (see module docstring);
    STRICT_ACCUMULATION_MODEL stays False until every shard can be rescored
    together.

    `scale` is the value magnitude, floored only at TINY_ABS rather than at 1.0.
    A unit floor would turn the bound absolute for every |v| <= 1 (8.3% of
    real buckets), so at n in the thousands it would admit absolute error near
    1e-3 on values orders of magnitude smaller than that, the opposite of a
    tight envelope.
    """
    u = float(finding["accum_ulp"])
    safety = float(finding["count_safety"])
    m = engine_df.merge(oracle_df, on=KEY_COLUMNS, suffixes=("_e", "_o"))
    if len(m) == 0:
        # No overlapping key survived the join, so not one value was compared.
        # Returning True here would certify the signature without evidence; an
        # empty join means the frames could not be aligned, which is itself a
        # reason to withhold the attribution.
        return False
    n = np.maximum(np.maximum(m["count_e"].astype(float), m["count_o"].astype(float)), 1.0)
    for field in finding["diverging_fields"]:
        a = m[f"{field}_e"].astype(float)
        b = m[f"{field}_o"].astype(float)
        # A non-finite value is never "within" a magnitude band. It has to be
        # rejected explicitly: NaN compares False against every bound, so a NaN
        # sum would otherwise slip through the `>` test below and be certified
        # as a documented finding.
        if not bool((np.isfinite(a) & np.isfinite(b)).all()):
            return False
        if STRICT_ACCUMULATION_MODEL:
            scale = np.maximum(TINY_ABS, np.maximum(a.abs(), b.abs()))
            tol = u * (1.0 + safety * np.maximum(n - 1.0, 0.0)) * scale
        else:
            tol = safety * n * u * np.maximum(1.0, np.maximum(a.abs(), b.abs()))
        if bool(((a - b).abs() > tol).any()):
            return False
    return True


def reproduces_finding(finding: Dict[str, Any],
                       engine_hourly: pd.DataFrame, engine_daily: pd.DataFrame,
                       oracle_hourly: pd.DataFrame, oracle_daily: pd.DataFrame,
                       cmp_hourly: Dict[str, Any], cmp_daily: Dict[str, Any]) -> bool:
    """True iff the strict-epsilon failure matches the finding's signature:
    every key is present exactly once, only the finding's fields diverge at the
    contract epsilon, and every diverging value is within the float32
    accumulation band (a per-key, count-scaled tolerance -- see
    _within_accumulation_band).

    Comparator structural failures are checked explicitly. In particular, a
    duplicate output key can otherwise disappear behind the comparator's
    representative row and accidentally resemble a clean magnitude match.
    """
    frames = (engine_hourly, engine_daily, oracle_hourly, oracle_daily)
    for frame in frames:
        if any(column not in frame.columns for column in KEY_COLUMNS):
            return False
        if bool(frame.duplicated(KEY_COLUMNS, keep=False).any()):
            return False

    observed_signature_field = False
    for c in (cmp_hourly, cmp_daily):
        if (int(c.get("only_in_online", 0)) != 0
                or int(c.get("only_in_oracle", 0)) != 0
                or int(c.get("duplicate_online_keys", 0)) != 0
                or int(c.get("duplicate_oracle_keys", 0)) != 0):
            return False
        non_equivalence = c.get("non_equivalence") or {}
        if int(non_equivalence.get("structural_violations", 0)) != 0:
            return False
        mismatches = c.get("field_mismatches")
        if not isinstance(mismatches, dict):
            return False
        for field, n in mismatches.items():
            if n and field not in finding["diverging_fields"]:
                return False
            if n:
                observed_signature_field = True
        if int(c.get("mismatched_keys", 0)) and not any(mismatches.values()):
            return False
    if not observed_signature_field:
        return False
    if finding.get("shape_only"):
        # The field set and the structural checks above are the whole signature.
        return True
    return bool(_within_accumulation_band(finding, engine_hourly, oracle_hourly)
                and _within_accumulation_band(finding, engine_daily, oracle_daily))


def match_known_finding(engine: str,
                        engine_hourly: pd.DataFrame, engine_daily: pd.DataFrame,
                        oracle_hourly: pd.DataFrame, oracle_daily: pd.DataFrame,
                        cmp_hourly: Dict[str, Any], cmp_daily: Dict[str, Any]
                        ) -> Optional[Dict[str, Any]]:
    """If `engine` has a documented finding and the strict-epsilon failure
    reproduces it exactly, return a JSON-safe record of the finding; else None."""
    f = _findings_for(engine)
    if f is None:
        return None
    if not reproduces_finding(f, engine_hourly, engine_daily,
                              oracle_hourly, oracle_daily, cmp_hourly, cmp_daily):
        return None
    return {"reproduced": True, **f, "diverging_fields": sorted(f["diverging_fields"])}
