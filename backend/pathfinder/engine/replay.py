"""
Deterministic STRATEGY REPLAY — the only thing in Pathfinder allowed to produce a
return number.

Ported from `Kanida_Falcon/arena/arena.py::sim_trade` and
`scripts/agent_arena.py::fwd_net`, with the product's conventions made explicit
rather than implied:

* **Entry = the OPEN of the session after the signal bar.** Never the signal bar.
* **The exit is the one that is traded** — hard stop, optional profit exit, else the
  close of the horizon session. The historical claim is a replay of that exact
  strategy, never a hold-to-close statistic.
* **Both sides of a same-day stop-and-target are resolved as the STOP.** Daily bars
  cannot tell us which came first; assuming the good one is how a backtest lies.
* **A gap through the stop fills at the open**, not at the stop level. The stop is a
  trigger, not a guarantee.
* **Costs and slippage are charged to every closed trade**, winners and losers alike.
* **A trade only becomes evidence once it is fully resolved inside the frame's seal.**
  A signal within `horizon` sessions of the frame end produces NaNs and is dropped —
  the mechanical form of `entry_idx + horizon <= today_idx`.

The exit arithmetic is computed ONCE as a (date × symbol) grid, so the signal set,
the baseline and the placebo control are all measured by identical code. A control
that used a different code path would not be a control.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional, Sequence

import numpy as np
import pandas as pd

from .costs import CostModel
from .hypothesis import CONTEXTS, HypothesisSpec
from .market import LookAheadError, PriceFrames

STOP, TARGET, HORIZON = 1, 2, 3
REASON = {STOP: "stop", TARGET: "target", HORIZON: "horizon"}


@dataclass(frozen=True)
class Trade:
    symbol: str
    direction: str
    signal_date: date
    entry_date: date
    entry_price: float
    exit_date: date
    exit_price: float
    exit_reason: str          # stop | target | horizon
    holding_sessions: int
    pnl_pct_gross: float
    pnl_pct_net: float
    costs_pct: float
    slippage_bps: float
    mfe_pct: float
    mae_pct: float


@dataclass(frozen=True)
class ReplayResult:
    """Everything the evidence layer needs, and no interpretation of it."""
    spec: HypothesisSpec
    window_start: date
    window_end: date
    as_of: date
    trades: tuple[Trade, ...]
    occurrences: int           # raw signal firings, incl. those not tradeable
    cost_convention: str

    @property
    def n(self) -> int:
        return len(self.trades)

    @property
    def net_returns(self) -> np.ndarray:
        return np.array([t.pnl_pct_net for t in self.trades], dtype=float)


class ExitGrid:
    """
    Vectorised replay of `spec`'s exit rules for a trade entered after EVERY bar.

    Cells where the path does not fully resolve inside the frame's seal are NaN, and
    every consumer drops them. That is the point-in-time availability rule, applied
    once, in one place.
    """

    def __init__(self, spec: HypothesisSpec, frames: PriceFrames, costs: CostModel) -> None:
        self.spec, self.frames, self.costs = spec, frames, costs
        h = spec.horizon_sessions
        o = frames.o.to_numpy(float); hi = frames.h.to_numpy(float)
        lo = frames.l.to_numpy(float); cl = frames.c.to_numpy(float)

        def fwd(a: np.ndarray, k: int) -> np.ndarray:
            out = np.full_like(a, np.nan)
            if k < a.shape[0]:
                out[: a.shape[0] - k] = a[k:]
            return out

        self.f_o = [fwd(o, k) for k in range(1, h + 1)]
        self.f_h = [fwd(hi, k) for k in range(1, h + 1)]
        self.f_l = [fwd(lo, k) for k in range(1, h + 1)]
        self.f_c = [fwd(cl, k) for k in range(1, h + 1)]

        long = spec.direction == "long"
        sgn = 1.0 if long else -1.0
        self.entry = self.f_o[0]
        stop_level = self.entry * (1 - sgn * spec.stop_pct / 100.0)
        tgt_level = None if spec.target_pct is None else self.entry * (1 + sgn * spec.target_pct / 100.0)

        def first_hit(masks: list[np.ndarray]) -> np.ndarray:
            out = np.zeros(masks[0].shape, dtype=np.int16)
            for k in range(len(masks) - 1, -1, -1):
                out = np.where(masks[k], k + 1, out)
            return out

        with np.errstate(invalid="ignore"):
            stop_k = first_hit([
                (self.f_l[k] <= stop_level) if long else (self.f_h[k] >= stop_level)
                for k in range(h)
            ])
            tgt_k = (
                np.zeros_like(stop_k) if tgt_level is None else first_hit([
                    (self.f_h[k] >= tgt_level) if long else (self.f_l[k] <= tgt_level)
                    for k in range(h)
                ])
            )
        # Same-session stop AND target -> resolve as the STOP. Daily bars cannot order them.
        use_stop = (stop_k > 0) & ((tgt_k == 0) | (stop_k <= tgt_k))
        use_tgt = (tgt_k > 0) & ~use_stop

        self.k = np.where(use_stop, stop_k, np.where(use_tgt, tgt_k, h)).astype(np.int16)
        self.reason = np.where(use_stop, STOP, np.where(use_tgt, TARGET, HORIZON)).astype(np.int8)

        # Exit price, taking the gap into account: a gap through the stop fills at the
        # open (worse than the level); a gap through the target fills at the open (better).
        stack_o = np.stack(self.f_o); stack_c = np.stack(self.f_c)
        idx = (self.k - 1).astype(np.intp)
        r_idx, c_idx = np.indices(self.entry.shape)
        open_at_k = stack_o[idx, r_idx, c_idx]
        close_at_h = stack_c[h - 1]
        with np.errstate(invalid="ignore"):
            if long:
                px_stop = np.minimum(open_at_k, stop_level)
                px_tgt = None if tgt_level is None else np.maximum(open_at_k, tgt_level)
            else:
                px_stop = np.maximum(open_at_k, stop_level)
                px_tgt = None if tgt_level is None else np.minimum(open_at_k, tgt_level)
        self.exit_px = np.where(
            use_stop, px_stop,
            np.where(use_tgt, px_tgt if px_tgt is not None else close_at_h, close_at_h),
        )

        # Resolved-inside-the-seal test: the entry must exist AND the full horizon must.
        self.resolved = (
            np.isfinite(self.entry) & (self.entry > 0)
            & np.isfinite(close_at_h) & np.isfinite(self.exit_px) & (self.exit_px > 0)
        )

        with np.errstate(invalid="ignore", divide="ignore"):
            self.gross = sgn * (self.exit_px / self.entry - 1.0) * 100.0
        self.net = self.gross - costs.round_trip_pct
        self.net = np.where(self.resolved, self.net, np.nan)

        # Excursions over the held path only.
        run_hi = np.full_like(self.entry, np.nan)
        run_lo = np.full_like(self.entry, np.nan)
        for k in range(h):
            held = self.k > k
            with np.errstate(invalid="ignore"):
                run_hi = np.where(held, np.fmax(run_hi, self.f_h[k]), run_hi)
                run_lo = np.where(held, np.fmin(run_lo, self.f_l[k]), run_lo)
        with np.errstate(invalid="ignore", divide="ignore"):
            if long:
                self.mfe = (run_hi / self.entry - 1.0) * 100.0
                self.mae = (run_lo / self.entry - 1.0) * 100.0
            else:
                self.mfe = (1.0 - run_lo / self.entry) * 100.0
                self.mae = (1.0 - run_hi / self.entry) * 100.0

    def trades_for(self, mask: np.ndarray) -> tuple[Trade, ...]:
        dates = self.frames.dates
        symbols = self.frames.c.columns.to_numpy()
        rows, cols = np.nonzero(mask & self.resolved)
        out: list[Trade] = []
        for i, j in zip(rows, cols):
            k = int(self.k[i, j])
            out.append(Trade(
                symbol=str(symbols[j]), direction=self.spec.direction,
                signal_date=dates[i].date(), entry_date=dates[i + 1].date(),
                entry_price=float(self.entry[i, j]),
                exit_date=dates[i + k].date(), exit_price=float(self.exit_px[i, j]),
                exit_reason=REASON[int(self.reason[i, j])], holding_sessions=k,
                pnl_pct_gross=float(self.gross[i, j]), pnl_pct_net=float(self.net[i, j]),
                costs_pct=float(self.costs.round_trip_pct),
                slippage_bps=float(self.costs.slippage_bps_per_side),
                mfe_pct=float(self.mfe[i, j]), mae_pct=float(self.mae[i, j]),
            ))
        out.sort(key=lambda t: (t.signal_date, t.symbol))
        return tuple(out)


def window_mask(frames: PriceFrames, start: date, end: date) -> np.ndarray:
    d = frames.dates
    return np.asarray((d >= pd.Timestamp(start)) & (d <= pd.Timestamp(end)))


def replay(
    spec: HypothesisSpec,
    frames: PriceFrames,
    *,
    window_start: date,
    window_end: date,
    costs: CostModel,
    grid: Optional[ExitGrid] = None,
    signals: Optional[np.ndarray] = None,
) -> ReplayResult:
    """Replay `spec` over [window_start, window_end] against a frame sealed at/before it."""
    if window_end > frames.as_of:
        raise LookAheadError(f"replay window ends {window_end}, frame is sealed at {frames.as_of}")
    g = grid or ExitGrid(spec, frames, costs)
    sig = spec.signals(frames).to_numpy(bool) if signals is None else signals
    sig = sig & window_mask(frames, window_start, window_end)[:, None]
    return ReplayResult(
        spec, window_start, window_end, frames.as_of,
        g.trades_for(sig), int(sig.sum()), costs.convention,
    )


def eligible_mask(spec: HypothesisSpec, frames: PriceFrames, grid: ExitGrid,
                  *, window_start: date, window_end: date) -> np.ndarray:
    """
    The comparison population: every point-in-time-liquid name, on every session in
    the SAME window under the SAME regime context, whose path resolves inside the
    seal. The signal set is a subset of this by construction.
    """
    dates = frames.dates
    ctx = CONTEXTS[spec.context](frames).reindex(dates).fillna(False).to_numpy(bool)
    inw = window_mask(frames, window_start, window_end)
    return frames.liquid_mask.to_numpy(bool) & (inw & ctx)[:, None] & grid.resolved


def signal_day_counts(spec: HypothesisSpec, frames: PriceFrames, grid: "ExitGrid",
                      *, window_start: date, window_end: date,
                      signals: Optional[np.ndarray] = None) -> np.ndarray:
    """How many trades the signal produced on each session it fired. Its clustering."""
    sig = spec.signals(frames).to_numpy(bool) if signals is None else signals
    sig = sig & window_mask(frames, window_start, window_end)[:, None] & grid.resolved
    per_day = sig.sum(axis=1)
    return per_day[per_day > 0]


def block_placebo(
    spec: HypothesisSpec,
    frames: PriceFrames,
    grid: "ExitGrid",
    *,
    window_start: date,
    window_end: date,
    day_counts: np.ndarray,
    draws: int,
    seed: int,
) -> np.ndarray:
    """
    The null, resampled BY DAY — which is the only null this signal has.

    The i.i.d. version of this test (draw n outcomes at random from the eligible pool)
    is wrong here, and wrong in the direction that flatters: these signals are not n
    independent bets. A gap-up screen fires on a handful of market-wide event days —
    1,702 discovery "trades" arrive on 184 sessions — so the effective sample is the
    number of EVENTS, not the number of trades. An i.i.d. null understates its own
    standard deviation by roughly the square root of the average cluster size, which
    turns a p-value of a few percent into a p-value of zero.

    So: draw as many days as the signal used, take from each drawn day as many outcomes
    as the signal took on one of its days, and compare the mean. The comparison is then
    like-for-like in shape as well as in size.
    """
    elig = eligible_mask(spec, frames, grid, window_start=window_start, window_end=window_end)
    net = grid.net
    rows, cols = np.nonzero(elig)
    if rows.size == 0 or day_counts.size == 0:
        return np.array([], dtype=float)

    # Flatten the eligible population into per-day contiguous blocks so a draw is a
    # handful of vectorised ops rather than a Python loop over days.
    order = np.argsort(rows, kind="stable")
    rows, cols = rows[order], cols[order]
    vals = net[rows, cols]
    finite = np.isfinite(vals)
    rows, vals = rows[finite], vals[finite]
    if vals.size == 0:
        return np.array([], dtype=float)

    day_ids, starts, lengths = np.unique(rows, return_index=True, return_counts=True)
    n_days = day_ids.size
    if n_days < 2:
        return np.array([], dtype=float)

    counts = day_counts.astype(np.int64)
    total = int(counts.sum())
    rng = np.random.default_rng(seed)
    means = np.empty(draws, dtype=float)
    for d in range(draws):
        picked_days = rng.integers(0, n_days, size=counts.size)
        # Sample WITH replacement inside a day: a drawn day may hold fewer names than
        # the signal's day did, and refusing those draws would bias toward busy days.
        offs = (rng.random(total) * np.repeat(lengths[picked_days], counts)).astype(np.int64)
        idx = np.repeat(starts[picked_days], counts) + offs
        means[d] = float(vals[idx].mean())
    return means


def cluster_robust_t(trades: Sequence[Trade]) -> tuple[float, int]:
    """
    `(t, n_signal_days)` for the mean, with observations clustered on the SIGNAL DATE.

    The naive t treats every trade as an independent draw. When a screen fires on 300
    names in one morning, that is one event, not three hundred — and the naive t was
    reporting 7.2 where the clustered t is 1.7. This is the number that should be
    believed, and the difference between them is the whole argument.
    """
    if len(trades) < 2:
        return float("nan"), 0
    r = np.array([t.pnl_pct_net for t in trades], dtype=float)
    days = np.array([t.signal_date.toordinal() for t in trades])
    mean = float(r.mean())
    resid = r - mean
    uniq = np.unique(days)
    # CR0: the variance of the mean is driven by the between-cluster sums of residuals.
    cluster_sums = np.array([resid[days == d].sum() for d in uniq], dtype=float)
    se = float(np.sqrt((cluster_sums ** 2).sum())) / len(r)
    if not np.isfinite(se) or se <= 0:
        return float("nan"), int(uniq.size)
    return mean / se, int(uniq.size)


def baseline_and_placebo(
    spec: HypothesisSpec,
    frames: PriceFrames,
    grid: ExitGrid,
    *,
    window_start: date,
    window_end: date,
    n_signals: int,
    draws: int,
    seed: int,
) -> tuple[float, int, np.ndarray]:
    """
    Returns `(baseline_expectancy_pct, baseline_n, placebo_sample_means)`.

    * **baseline** — the mean net outcome of the whole eligible population under the
      same exit rules. It carries the same survivorship, the same market drift and
      the same cost model as the signal set, so `signal − baseline` is the part of
      the result that is actually about the signal.
    * **placebo** — `draws` random same-size samples from that population. The
      fraction of them beating the signal is a permutation p-value: it answers
      "would a coin flip drawn from the same pond have looked this good?"
    """
    elig = eligible_mask(spec, frames, grid, window_start=window_start, window_end=window_end)
    pool = grid.net[elig]
    pool = pool[np.isfinite(pool)]
    if pool.size == 0:
        return float("nan"), 0, np.array([], dtype=float)
    baseline = float(pool.mean())
    if n_signals <= 0 or pool.size < n_signals:
        return baseline, int(pool.size), np.array([], dtype=float)
    rng = np.random.default_rng(seed)
    means = np.array(
        [float(rng.choice(pool, size=n_signals, replace=False).mean()) for _ in range(draws)],
        dtype=float,
    )
    return baseline, int(pool.size), means
