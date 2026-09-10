"""
The continuous, cheap, deterministic observer — and the candidate grid.

Two jobs, both of them the engine's and neither of them the model's:

1. **Watch the market and decide when something is worth waking a model for.**
   Autonomy is *continuous observation, intelligent activation*: this runs on every
   session for nothing, and fires a `Trigger` only when a market-state series sits in
   the tail of **its own history up to that day** (`expanding_percentile` — a
   full-sample rank here would be the single most common look-ahead bug in this kind
   of code, and it would be invisible).

2. **Evaluate a PRE-REGISTERED candidate grid on the discovery window only**, so the
   model has real, computed evidence to reason about instead of vibes.

On the grid and multiple testing — said out loud because it is the thing most easily
hidden: evaluating N candidates and reporting the best one inflates the best one.
`CandidateScan.n_candidates` is therefore recorded, published as a fact, and carried
into the gauntlet: a candidate's placebo p-value is compared against a Bonferroni-
adjusted bar (`alpha / n_candidates`), not the naked one. The grid is also fixed in
code — it is not re-drawn after seeing results, which is the other half of the same
problem.
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from datetime import date
from typing import Optional, Sequence

import numpy as np
import pandas as pd

from .config import EngineConfig
from .costs import CostModel
from .evidence import Metrics, compute_metrics
from .hypothesis import HypothesisSpec
from .market import PriceFrames, expanding_percentile
from .replay import ExitGrid, baseline_and_placebo, replay


# ── 1. market state + triggers ───────────────────────────────────────────────

@dataclass(frozen=True)
class MarketState:
    """What the observer saw at `as_of`. Every value computed from bars ≤ as_of."""
    as_of: date
    breadth_above_50dma: float
    breadth_above_200dma: float
    index_realised_vol_20d: float
    index_vol_percentile: float        # against its OWN expanding history
    breadth_percentile: float
    median_volume_ratio: float
    volume_percentile: float
    index_above_200dma: bool
    liquid_universe_size: int


def observe(frames: PriceFrames) -> MarketState:
    d = frames.dates[-1]
    vol = frames.index_realised_vol_20d
    breadth = frames.breadth_above_50dma
    volr = frames.median_vol_ratio
    return MarketState(
        as_of=d.date(),
        breadth_above_50dma=float(breadth.iloc[-1]),
        breadth_above_200dma=float(frames.breadth_above_200dma.iloc[-1]),
        index_realised_vol_20d=float(vol.iloc[-1]),
        index_vol_percentile=float(expanding_percentile(vol).iloc[-1]),
        breadth_percentile=float(expanding_percentile(breadth).iloc[-1]),
        median_volume_ratio=float(volr.iloc[-1]),
        volume_percentile=float(expanding_percentile(volr).iloc[-1]),
        index_above_200dma=bool(frames.index_above_200dma.iloc[-1]),
        liquid_universe_size=int(frames.liquid_mask.iloc[-1].sum()),
    )


@dataclass(frozen=True)
class Observation:
    """One unusual thing, with the number that made it unusual."""
    key: str
    description: str
    value: float
    percentile: float
    unit: str


def unusual_conditions(state: MarketState, *, threshold: float) -> list[Observation]:
    """
    Tail readings only. `threshold` comes from the Constitution
    (`autonomy.unusual_condition_percentile`); the observer does not choose its own bar.
    """
    lo = 1.0 - threshold
    candidates = [
        Observation("index_volatility", "trailing index volatility",
                    state.index_realised_vol_20d, state.index_vol_percentile, "ratio"),
        Observation("breadth", "share of the universe above its 50-session average",
                    state.breadth_above_50dma, state.breadth_percentile, "ratio"),
        Observation("volume", "median volume against each name's own 20-session median",
                    state.median_volume_ratio, state.volume_percentile, "ratio"),
    ]
    return [
        o for o in candidates
        if np.isfinite(o.percentile) and (o.percentile >= threshold or o.percentile <= lo)
    ]


# ── 2. the pre-registered candidate grid ─────────────────────────────────────
# Fixed in code. Not re-drawn after seeing a result. Changing it is a code change
# with a diff, which is the point.

GRID_TRIGGERS: list[tuple[str, dict[str, float | int]]] = [
    ("pullback_in_uptrend", {"trend_lookback": 50, "pullback_pct": 8.0}),
    ("pullback_in_uptrend", {"trend_lookback": 100, "pullback_pct": 12.0}),
    ("volume_dry_up", {"sessions": 3, "ratio": 0.7}),
    ("volume_dry_up", {"sessions": 5, "ratio": 0.6}),
    ("oversold", {"lookback": 3, "thr_pct": 8.0}),
    ("oversold", {"lookback": 5, "thr_pct": 12.0}),
    ("overbought", {"lookback": 3, "thr_pct": 10.0}),
    ("breakout", {"lookback": 20}),
    ("breakout", {"lookback": 120}),
    ("breakdown", {"lookback": 20}),
    ("gap_up", {"thr_pct": 3.0}),
    ("gap_down", {"thr_pct": 3.0}),
    ("streak_up", {"sessions": 4}),
    ("streak_down", {"sessions": 4}),
    ("inside_day", {"sessions": 3}),
    ("range_expansion", {"mult": 2.0}),
]
GRID_CONTEXTS = ["any", "index_above_200dma", "index_below_200dma"]
GRID_DIRECTIONS = ["long", "short"]
#: (horizon, stop%, target%) — the exit structures the book is willing to run.
GRID_EXITS: list[tuple[int, float, Optional[float]]] = [
    (3, 4.0, 6.0),
    (5, 5.0, 8.0),
    (10, 8.0, None),
]


@dataclass
class Candidate:
    spec: HypothesisSpec
    metrics: Metrics
    n_candidates_in_scan: int = 0

    @property
    def signature(self) -> str:
        return self.spec.signature


@dataclass
class CandidateScan:
    """The whole discovery-window sweep. `n_candidates` is the multiple-testing count."""
    as_of: date
    window_start: date
    window_end: date
    n_candidates: int
    evaluated: list[Candidate] = field(default_factory=list)
    skipped_too_few: int = 0

    def ranked(self) -> list[Candidate]:
        """
        Ranked by EDGE OVER BASELINE, not by raw expectancy.

        Raw expectancy over a survivor-biased universe mostly measures the market. The
        part that is about the signal is the part the baseline does not explain.
        """
        return sorted(
            [c for c in self.evaluated if np.isfinite(c.metrics.edge_vs_baseline_pct)],
            key=lambda c: -c.metrics.edge_vs_baseline_pct,
        )


def build_grid() -> list[HypothesisSpec]:
    """Every composable candidate. Deterministic order, so a scan is reproducible."""
    out: list[HypothesisSpec] = []
    for (trig, params), ctx, direction, (h, stop, tgt) in itertools.product(
        GRID_TRIGGERS, GRID_CONTEXTS, GRID_DIRECTIONS, GRID_EXITS
    ):
        out.append(HypothesisSpec(
            trigger=trig, params=dict(params), context=ctx, direction=direction,  # type: ignore[arg-type]
            horizon_sessions=h, stop_pct=stop, target_pct=tgt,
        ))
    return out


def scan_discovery(
    frames: PriceFrames,
    cfg: EngineConfig,
    costs: CostModel,
    *,
    window_start: date,
    window_end: date,
    min_n: int = 60,
    grid: Optional[Sequence[HypothesisSpec]] = None,
    placebo_draws: Optional[int] = None,
) -> CandidateScan:
    """
    Evaluate the grid on the DISCOVERY window only.

    `frames` must already be sealed at `window_end` — the caller passes
    `full.sealed_at(cfg.discovery_end)` and this function asserts it, so a future bar
    cannot reach the selection step even by accident.
    """
    if frames.as_of > window_end:
        raise ValueError(
            f"discovery scan given a frame sealed at {frames.as_of}, later than the "
            f"discovery window end {window_end}: selection could see the future"
        )
    specs = list(grid or build_grid())
    scan = CandidateScan(frames.as_of, window_start, window_end, len(specs))
    draws = cfg.placebo_draws if placebo_draws is None else placebo_draws

    # The exit arithmetic depends only on (direction, horizon, stop, target) — not on
    # the trigger — so 288 candidates need 6 grids, not 288. Same for the baseline
    # pool, which additionally depends on the regime context. Caching here is a speed
    # decision only: every candidate is still measured by identical code.
    grids: dict[tuple, ExitGrid] = {}
    pools: dict[tuple, tuple[float, int, np.ndarray]] = {}
    rng = np.random.default_rng(cfg.rng_seed)

    for spec in specs:
        gkey = (spec.direction, spec.horizon_sessions, spec.stop_pct, spec.target_pct)
        g = grids.get(gkey)
        if g is None:
            g = grids[gkey] = ExitGrid(spec, frames, costs)
        res = replay(spec, frames, window_start=window_start, window_end=window_end,
                     costs=costs, grid=g)
        if res.n < min_n:
            scan.skipped_too_few += 1
            continue

        pkey = gkey + (spec.context,)
        if pkey not in pools:
            from .replay import eligible_mask
            elig = eligible_mask(spec, frames, g, window_start=window_start, window_end=window_end)
            pool = g.net[elig]
            pool = pool[np.isfinite(pool)]
            pools[pkey] = (float(pool.mean()) if pool.size else float("nan"), int(pool.size), pool)
        base, base_n, pool = pools[pkey]

        placebo = (
            np.array([float(rng.choice(pool, size=res.n, replace=False).mean()) for _ in range(draws)])
            if pool.size >= res.n > 0 else np.array([], dtype=float)
        )
        m = compute_metrics(res, costs_2x=costs.at_slippage_multiple(2.0),
                            baseline_pct=base, baseline_n=base_n, placebo_means=placebo)
        scan.evaluated.append(Candidate(spec, m, len(specs)))
    return scan
