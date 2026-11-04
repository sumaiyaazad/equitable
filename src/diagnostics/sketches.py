"""Bounded-deletion reference sketches for the D7 QER half (docs/SEMANTICS.md, Restatements).

KLL± [zhao2021kll] and SpaceSaving± [zhao2022spacesaving] solve quantiles and
frequent items in the *bounded-deletion* model: a bounded fraction alpha of
previously-inserted elements may later be deleted. A D7 restatement is exactly
one bounded deletion (retract the superseded value) plus one insertion (the
corrected value), with alpha == config `perturbation.restate_rate`.

This module provides deterministic, dependency-free reference implementations:

  KLLSketch              -- standard insert-only KLL quantile sketch.
  BoundedDeletionKLL     -- KLL±-style: one KLL over insertions, one over
                            deletions; rank(x) = rank_ins(x) - rank_del(x).
                            (The two-sketch difference estimator is the
                            textbook bounded-deletion construction; its rank
                            error is eps*(n_ins + n_del) = eps*n*(1+2*alpha).)
  SpaceSaving            -- classic insert-only frequent-items summary.
  SpaceSavingPM          -- SpaceSaving±-style deletion handling: a delete of a
                            monitored item decrements its counter; a delete of
                            an unmonitored item is charged to a global residual
                            that widens the error bound (bounded by alpha*n).

All randomness (KLL compaction offsets) is seeded, so replays are bit-stable.
These are REFERENCE oracles: they define what a retraction-correct sketch
must produce, they are not systems under test.
"""
from __future__ import annotations

import bisect
import math
import random
from typing import Dict, Iterable, List, Tuple


class KLLSketch:
    """Insert-only KLL quantile sketch (Karnin-Lang-Liberty), deterministic
    under `seed`. Rank error is O(n/k) with high probability; for n <= k the
    sketch stores everything and is exact."""

    def __init__(self, k: int = 200, seed: int = 0) -> None:
        if k < 8:
            raise ValueError("k must be >= 8")
        self.k = int(k)
        self._rng = random.Random(seed)
        self.compactors: List[List[float]] = [[]]
        self.n = 0

    def _capacity(self, h: int) -> int:
        depth = len(self.compactors) - 1 - h
        return max(2, int(math.ceil(self.k * (2.0 / 3.0) ** depth)))

    def update(self, v: float) -> None:
        self.compactors[0].append(float(v))
        self.n += 1
        self._compress()

    def _compress(self) -> None:
        while sum(len(c) for c in self.compactors) > sum(
                self._capacity(h) for h in range(len(self.compactors))):
            for h, c in enumerate(self.compactors):
                if len(c) >= self._capacity(h):
                    if h + 1 == len(self.compactors):
                        self.compactors.append([])
                    c.sort()
                    offset = self._rng.randint(0, 1)
                    self.compactors[h + 1].extend(c[offset::2])
                    c.clear()
                    break
            else:
                return

    def rank(self, x: float) -> float:
        """Estimated number of inserted items <= x."""
        r = 0.0
        for h, c in enumerate(self.compactors):
            w = float(1 << h)
            r += w * sum(1 for v in c if v <= x)
        return r

    def items(self) -> List[float]:
        out: List[float] = []
        for c in self.compactors:
            out.extend(c)
        return out

    def weighted_items(self) -> List[Tuple[float, float]]:
        """(value, weight) pairs; weight of level h is 2**h."""
        out: List[Tuple[float, float]] = []
        for h, c in enumerate(self.compactors):
            w = float(1 << h)
            out.extend((v, w) for v in c)
        return out

    def quantile(self, q: float) -> float:
        """Value whose estimated rank is closest to q*n (single weighted
        cumulative pass, O(items log items))."""
        if self.n == 0:
            raise ValueError("empty sketch")
        return _weighted_quantile(self.weighted_items(), q * self.n)


def _weighted_quantile(pairs: List[Tuple[float, float]], target: float) -> float:
    """Value from `pairs` [(value, weight), weight may be negative for
    deletions] whose cumulative weight (in value order) is closest to
    `target`. One sort + one cumulative pass."""
    pairs.sort(key=lambda vw: vw[0])
    best, best_err, cum = pairs[0][0], float("inf"), 0.0
    i, n = 0, len(pairs)
    while i < n:
        v = pairs[i][0]
        while i < n and pairs[i][0] == v:   # all weight at equal values
            cum += pairs[i][1]
            i += 1
        err = abs(cum - target)
        if err < best_err:
            best, best_err = v, err
    return best


class BoundedDeletionKLL:
    """KLL±-style bounded-deletion quantile sketch: an insertion KLL and a
    deletion KLL; every query is the difference of the two rank estimates."""

    def __init__(self, k: int = 200, seed: int = 0) -> None:
        self._ins = KLLSketch(k, seed=seed)
        self._del = KLLSketch(k, seed=seed + 1)

    @property
    def n(self) -> int:
        return self._ins.n - self._del.n

    def update(self, v: float) -> None:
        self._ins.update(v)

    def delete(self, v: float) -> None:
        self._del.update(v)

    def rank(self, x: float) -> float:
        return self._ins.rank(x) - self._del.rank(x)

    def quantile(self, q: float) -> float:
        if self.n <= 0:
            raise ValueError("empty (or fully deleted) sketch")
        pairs = self._ins.weighted_items()
        pairs.extend((v, -w) for v, w in self._del.weighted_items())
        return _weighted_quantile(pairs, q * self.n)


class SpaceSaving:
    """Classic SpaceSaving frequent-items summary with m counters. Estimated
    count over-approximates the true count by at most n/m."""

    def __init__(self, m: int = 100) -> None:
        self.m = int(m)
        self.counts: Dict[str, float] = {}
        self.errors: Dict[str, float] = {}
        self.n = 0

    def update(self, item: str, weight: float = 1.0) -> None:
        self.n += weight
        if item in self.counts:
            self.counts[item] += weight
            return
        if len(self.counts) < self.m:
            self.counts[item] = weight
            self.errors[item] = 0.0
            return
        victim = min(self.counts, key=lambda i: self.counts[i])
        floor = self.counts.pop(victim)
        self.errors.pop(victim)
        self.counts[item] = floor + weight
        self.errors[item] = floor

    def estimate(self, item: str) -> float:
        return self.counts.get(item, 0.0)

    def topk(self, k: int) -> List[Tuple[str, float]]:
        return sorted(self.counts.items(), key=lambda kv: (-kv[1], kv[0]))[:k]


class SpaceSavingPM(SpaceSaving):
    """SpaceSaving±-style bounded-deletion frequent-items summary: a delete of
    a monitored item decrements its counter (never below its error floor); a
    delete of an unmonitored item accrues to `residual_deletions`, which widens
    the estimation band by at most alpha*n -- the bounded-deletion guarantee."""

    def __init__(self, m: int = 100) -> None:
        super().__init__(m)
        self.residual_deletions = 0.0

    def delete(self, item: str, weight: float = 1.0) -> None:
        self.n -= weight
        if item in self.counts:
            self.counts[item] = max(self.counts[item] - weight, self.errors[item])
        else:
            self.residual_deletions += weight


def exact_rank_fraction(sorted_values: List[float], x: float) -> float:
    """Fraction of `sorted_values` <= x (the exact normalized rank of x)."""
    if not sorted_values:
        return 0.0
    return bisect.bisect_right(sorted_values, x) / len(sorted_values)


def quantile_rank_error(sorted_values: List[float], estimate: float, q: float) -> float:
    """Normalized rank error of `estimate` as the q-quantile of
    `sorted_values`, TIE-AWARE: a value occupies the whole rank interval
    [bisect_left, bisect_right] of its duplicates, so the error is 0 whenever
    q*n falls inside that interval, else the distance to its nearest end.
    (Without this, a heavily-tied multiset -- e.g. discrete sensor readings --
    penalizes an EXACT quantile answer for the mass of its own tie group.)"""
    n = len(sorted_values)
    if n == 0:
        return 0.0
    lo = bisect.bisect_left(sorted_values, estimate) / n
    hi = bisect.bisect_right(sorted_values, estimate) / n
    if lo <= q <= hi:
        return 0.0
    return min(abs(q - lo), abs(q - hi))


def self_check(seed: int = 1337) -> Dict[str, float]:
    """Sanity self-test used by the D7S runner's log: on a synthetic stream with
    10% deletions, the bounded-deletion sketch must track the corrected multiset
    and the insert-only sketch must not. Returns the two max rank errors."""
    rng = random.Random(seed)
    values = [rng.gauss(0.0, 1.0) for _ in range(20000)]
    deleted = set(rng.sample(range(len(values)), k=2000))
    corrected = sorted(v for i, v in enumerate(values) if i not in deleted)

    bd = BoundedDeletionKLL(200, seed=seed)
    ins_only = KLLSketch(200, seed=seed)
    for i, v in enumerate(values):
        bd.update(v)
        ins_only.update(v)
    for i in deleted:
        bd.delete(values[i])

    def max_err(sk) -> float:
        return max(quantile_rank_error(corrected, sk.quantile(q), q)
                   for q in (0.1, 0.25, 0.5, 0.75, 0.9, 0.99))

    return {"bounded_deletion_max_rank_err": max_err(bd),
            "insert_only_max_rank_err": max_err(ins_only)}
