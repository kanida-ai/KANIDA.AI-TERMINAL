"""
Evidence: replayed trades → the numbers the product publishes.

Everything here is arithmetic over a list of closed, costed trades. Nothing here
knows what a model is. This module is the *only* producer of `PerformanceBlock`
values, which is why `computed_by` is stamped here and why the DB has a CHECK that
it never names a model.

The equity curve deserves a note, because a drawdown computed the lazy way is a lie:
trades are accumulated **in exit-date order** and the peak-to-trough is taken on that
cumulative series. Sorting by anything else — signal date, or worse, by P&L — would
produce a drawdown that never happened.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable, Optional, Sequence

import numpy as np

from .config import EngineConfig, now_ist
from .costs import CostModel
from .replay import ReplayResult, Trade


@dataclass(frozen=True)
class Metrics:
    """The computed book. Serialises 1:1 onto the API's `PerformanceBlock`."""
    expectancy_pct_per_trade: float
    expectancy_2x_slippage_pct_per_trade: float
    total_return_pct: Optional[float]
    max_drawdown_pct: float
    current_drawdown_pct: float
    win_rate_pct: Optional[float]
    avg_win_pct: Optional[float]
    avg_loss_pct: Optional[float]
    payoff_ratio: Optional[float]
    n: int
    occurrences: Optional[int]

    #: Supporting statistics the API does not carry but the gauntlet needs.
    baseline_pct_per_trade: float = float("nan")
    edge_vs_baseline_pct: float = float("nan")
    baseline_n: int = 0
    placebo_p_value: float = float("nan")
    placebo_draws: int = 0
    #: How the null was built. `iid` is the cheap ranking null; `day_block` is the one
    #: the significance gate uses, because these signals cluster on event days.
    placebo_kind: str = "iid"
    t_stat: float = float("nan")
    #: The t that should be believed: observations clustered on the signal date.
    cluster_t: float = float("nan")
    signal_days: int = 0


#: What the replay drawdown actually measures. Travels on the fact and the evidence
#: bundle so a reader cannot mistake it for a drawdown on capital.
REPLAY_DRAWDOWN_NOTE = (
    "Drawdown of a single unit-stake sequence taking every signal in exit order, in "
    "cumulative percentage points of one position — NOT a drawdown on capital. These "
    "signals overlap in time; no book could hold them all. The virtual book's "
    "drawdown is the one that is a track record."
)


def _equity_curve(trades: Sequence[Trade]) -> np.ndarray:
    """
    Cumulative net % in EXIT order — the order the money actually arrived.

    A per-trade sequence at unit stake, NOT a capital curve: overlapping signals mean
    a real book could not have taken all of these. `book.py` computes the capital
    curve. Both are reported and they are labelled differently, because a reader who
    confuses them would be badly misled.
    """
    if not trades:
        return np.array([], dtype=float)
    ordered = sorted(trades, key=lambda t: (t.exit_date, t.symbol))
    return np.cumsum([t.pnl_pct_net for t in ordered], dtype=float)


def drawdowns(curve: np.ndarray) -> tuple[float, float]:
    """`(max_drawdown_pct, current_drawdown_pct)`, both reported positive."""
    if curve.size == 0:
        return 0.0, 0.0
    peak = np.maximum.accumulate(curve)
    dd = peak - curve
    return float(dd.max()), float(dd[-1])


def compute_metrics(
    result: ReplayResult,
    *,
    costs_2x: CostModel,
    baseline_pct: float = float("nan"),
    baseline_n: int = 0,
    placebo_means: Optional[np.ndarray] = None,
    placebo_kind: str = "iid",
) -> Metrics:
    """
    All of it, from the trades. Note what is NOT here: no annualisation, no Sharpe,
    no projection. A number the product does not publish is a number nobody has to
    defend.
    """
    r = result.net_returns
    n = int(r.size)
    if n == 0:
        return Metrics(0.0, 0.0, None, 0.0, 0.0, None, None, None, None, 0, result.occurrences)

    # The 2x-slippage gate: same trades, same exits, one more slippage side each way.
    extra = costs_2x.round_trip_pct - (result.trades[0].costs_pct if result.trades else 0.0)
    r2 = r - extra

    wins, losses = r[r > 0], r[r <= 0]
    curve = _equity_curve(result.trades)
    mdd, cdd = drawdowns(curve)

    placebo_p = float("nan")
    draws = 0
    if placebo_means is not None and placebo_means.size:
        draws = int(placebo_means.size)
        placebo_p = float((placebo_means >= r.mean()).mean())

    sd = float(r.std(ddof=1)) if n > 1 else float("nan")
    t_stat = float(r.mean() / (sd / np.sqrt(n))) if (n > 1 and sd > 0) else float("nan")
    from .replay import cluster_robust_t
    ct, days = cluster_robust_t(result.trades)

    return Metrics(
        expectancy_pct_per_trade=float(r.mean()),
        expectancy_2x_slippage_pct_per_trade=float(r2.mean()),
        # DELIBERATELY NULL. A replay is a per-trade study, not a book: its signals
        # overlap, so there is no capital curve and therefore no return. Only
        # `book.py` produces a `total_return_pct`, because only a book has capital.
        total_return_pct=None,
        max_drawdown_pct=mdd,
        current_drawdown_pct=cdd,
        win_rate_pct=float((r > 0).mean() * 100.0),
        avg_win_pct=float(wins.mean()) if wins.size else None,
        avg_loss_pct=float(losses.mean()) if losses.size else None,
        payoff_ratio=(float(wins.mean() / abs(losses.mean()))
                      if (wins.size and losses.size and losses.mean() != 0) else None),
        n=n,
        occurrences=result.occurrences,
        baseline_pct_per_trade=baseline_pct,
        edge_vs_baseline_pct=float(r.mean() - baseline_pct) if np.isfinite(baseline_pct) else float("nan"),
        baseline_n=baseline_n,
        # An empirical p-value cannot resolve below 1/draws. Reporting the observed
        # zero as zero would be a claim the test cannot support, so the floor is the
        # honest value and every consumer compares against the floor.
        placebo_p_value=(max(placebo_p, 1.0 / draws) if draws else placebo_p),
        placebo_draws=draws,
        placebo_kind=placebo_kind,
        t_stat=t_stat,
        cluster_t=ct,
        signal_days=days,
    )


@dataclass(frozen=True)
class Provenance:
    """Serialises onto the API's `Provenance`. `computed_by` never names a model."""
    data_source: str
    range_start: date
    range_end: date
    as_of: date
    cost_convention: str
    computed_by: str
    computed_at: datetime
    universe: Optional[str] = None

    def __post_init__(self) -> None:
        if self.range_end > self.as_of:
            raise ValueError("point-in-time: range_end may not post-date as_of")
        banned = ("claude", "gpt", "gemini", "sonnet", "haiku", "opus", "llm")
        if any(b in self.computed_by.lower() for b in banned):
            raise ValueError(f"computed_by names a model: {self.computed_by!r} — the LLM never calculates")


def provenance_for(
    cfg: EngineConfig, *, component: str, start: date, end: date, as_of: date,
    costs: CostModel,
) -> Provenance:
    return Provenance(
        data_source=cfg.data_source, range_start=start, range_end=end, as_of=as_of,
        cost_convention=costs.convention, computed_by=cfg.component(component),
        computed_at=now_ist(), universe=cfg.universe_id,
    )


# ── regime split (an evidence bundle, not a selection tool) ──────────────────

def split_by(trades: Iterable[Trade], key) -> dict[str, list[Trade]]:
    out: dict[str, list[Trade]] = {}
    for t in trades:
        out.setdefault(key(t), []).append(t)
    return out


def expectancy_of(trades: Sequence[Trade]) -> tuple[float, int]:
    if not trades:
        return float("nan"), 0
    arr = np.array([t.pnl_pct_net for t in trades], dtype=float)
    return float(arr.mean()), int(arr.size)


def cost_sensitivity_ladder(
    result: ReplayResult, base_costs: CostModel, multiples: Sequence[float] = (0.0, 1.0, 2.0, 3.0)
) -> list[tuple[float, float]]:
    """
    `[(slippage_multiple, expectancy_pct_net)]`.

    Where the ladder crosses zero is the honest headline: it is the slippage the idea
    can survive, stated instead of assumed.
    """
    r = result.net_returns
    if r.size == 0:
        return []
    charged = result.trades[0].costs_pct
    out = []
    for m in multiples:
        c = base_costs.at_slippage_multiple(m)
        out.append((float(m), float((r - (c.round_trip_pct - charged)).mean())))
    return out
