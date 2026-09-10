"""
The virtual book — forward tracking with virtual money, under real constraints.

`replay.py` answers "what did this rule do?". This module answers the harder and
more honest question: **"what would a book that could only hold ten positions and had
₹10 lakh have actually done?"** Those differ a lot. A rule firing 14 times a day has
an expectancy; a book that can take five of them has a *return*, and only the second
one is a track record.

Constraints applied, all from the Constitution:
  * `max_concurrent_positions`, `max_new_positions_per_session`, one position per symbol;
  * fixed fraction of capital per position, no leverage;
  * when more signals fire than there are slots, they are ranked by an explicit,
    point-in-time, deterministic `selection_rule`. It is a NAMED PARAMETER, not a
    detail, because an undocumented tie-break is a free parameter. Measured on this
    data, the spread across all three approved rules is about a quarter of a
    percentage point per trade — small, and *much* smaller than the gap between the
    book and the unconstrained rule, which is why `loop.py` no longer blames it for
    that gap. The alternatives exist so the choice can be measured instead of assumed.

The equity curve is marked to the CLOSE every session, including days with no
activity, so the drawdown is the drawdown a person would have lived through rather
than a trade-to-trade artefact.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional, Sequence

import numpy as np
import pandas as pd

from .config import EngineConfig
from .costs import CostModel
from .evidence import Metrics, drawdowns
from .hypothesis import HypothesisSpec
from .market import PriceFrames
from .replay import ExitGrid, Trade, window_mask


def _rank_liquidity_desc(cols: np.ndarray, rank_row: np.ndarray, rng) -> np.ndarray:
    return cols[np.argsort(-np.nan_to_num(rank_row[cols], nan=-1.0))]


def _rank_liquidity_asc(cols: np.ndarray, rank_row: np.ndarray, rng) -> np.ndarray:
    return cols[np.argsort(np.nan_to_num(rank_row[cols], nan=np.inf))]


def _rank_random(cols: np.ndarray, rank_row: np.ndarray, rng) -> np.ndarray:
    out = cols.copy()
    rng.shuffle(out)
    return out


#: The approved ways to choose which signals a capacity-limited book takes.
#: Named, point-in-time, and measurable against each other.
SELECTION_RULES = {
    "liquidity_desc": _rank_liquidity_desc,
    "liquidity_asc": _rank_liquidity_asc,
    "random": _rank_random,
}


@dataclass(frozen=True)
class BookResult:
    capital_inr: float
    closed: tuple[Trade, ...]              # in exit order
    open_at_end: tuple[Trade, ...]         # entry recorded, exit not yet reached
    equity: pd.Series                      # daily mark-to-close, in INR
    signals_seen: int
    signals_taken: int
    sessions_walked: int
    window_start: date
    window_end: date
    selection_rule: str = "liquidity_desc"

    @property
    def ledger_losers_first(self) -> tuple[Trade, ...]:
        """L-7. The only ordering this product ships for a closed ledger."""
        return tuple(sorted(self.closed, key=lambda t: t.pnl_pct_net))

    @property
    def equity_pct_curve(self) -> np.ndarray:
        if self.equity.empty:
            return np.array([], dtype=float)
        base = float(self.equity.iloc[0])
        return ((self.equity.to_numpy(float) / base) - 1.0) * 100.0

    def metrics(self, *, costs_2x: CostModel) -> Metrics:
        """Book metrics: expectancy per trade, but drawdown from the CAPITAL curve."""
        r = np.array([t.pnl_pct_net for t in self.closed], dtype=float)
        curve = self.equity_pct_curve
        mdd, cdd = drawdowns(curve)
        if r.size == 0:
            return Metrics(0.0, 0.0, None, mdd, cdd, None, None, None, None, 0, self.signals_seen)
        extra = costs_2x.round_trip_pct - self.closed[0].costs_pct
        wins, losses = r[r > 0], r[r <= 0]
        return Metrics(
            expectancy_pct_per_trade=float(r.mean()),
            expectancy_2x_slippage_pct_per_trade=float((r - extra).mean()),
            total_return_pct=(float(curve[-1]) if curve.size else None),
            max_drawdown_pct=mdd, current_drawdown_pct=cdd,
            win_rate_pct=float((r > 0).mean() * 100.0),
            avg_win_pct=float(wins.mean()) if wins.size else None,
            avg_loss_pct=float(losses.mean()) if losses.size else None,
            payoff_ratio=(float(wins.mean() / abs(losses.mean()))
                          if (wins.size and losses.size and losses.mean() != 0) else None),
            n=int(r.size), occurrences=self.signals_seen,
        )

    def spark(self, max_points: int = 200) -> list[float]:
        """Cumulative % to date. Never a projection — the last point is `as_of`."""
        c = self.equity_pct_curve
        if c.size == 0:
            return []
        if c.size <= max_points:
            return [round(float(x), 4) for x in c]
        idx = np.linspace(0, c.size - 1, max_points).astype(int)
        return [round(float(c[i]), 4) for i in idx]


def run_book(
    spec: HypothesisSpec,
    frames: PriceFrames,
    *,
    window_start: date,
    window_end: date,
    costs: CostModel,
    cfg: EngineConfig,
    grid: Optional[ExitGrid] = None,
    selection_rule: str = "liquidity_desc",
    max_new_positions_per_session: int = 5,
) -> BookResult:
    """Walk the sealed window one session at a time, under the Constitution's limits."""
    if selection_rule not in SELECTION_RULES:
        raise ValueError(f"unknown selection_rule {selection_rule!r}; approved: {sorted(SELECTION_RULES)}")
    g = grid or ExitGrid(spec, frames, costs)
    sig = spec.signals(frames).to_numpy(bool) & window_mask(frames, window_start, window_end)[:, None]

    dates = frames.dates
    symbols = frames.c.columns.to_numpy()
    closes = frames.c.to_numpy(float)
    rank_by = frames.turnover.rolling(cfg.turnover_lookback, min_periods=cfg.turnover_lookback).median().to_numpy(float)

    rng = np.random.default_rng(cfg.rng_seed)
    per_position = cfg.capital_inr * cfg.risk_fraction_per_trade
    cash = cfg.capital_inr
    open_pos: dict[str, dict] = {}                   # symbol -> position
    closed: list[Trade] = []
    equity_dates: list[pd.Timestamp] = []
    equity_vals: list[float] = []
    signals_taken = 0

    first = int(np.searchsorted(dates.values, np.datetime64(pd.Timestamp(window_start))))
    last = int(np.searchsorted(dates.values, np.datetime64(pd.Timestamp(window_end)), side="right")) - 1
    if last < first:
        raise ValueError(
            f"book window [{window_start}, {window_end}] contains no sessions in a frame "
            f"spanning {dates[0].date()}..{dates[-1].date()}"
        )
    # Counted from the mask, not accumulated in the loop: an empty book must be a
    # fact about the signal, never an artefact of the walk. The assertion below is
    # what turns a silent zero into a crash.
    tradeable = sig & g.resolved
    signals_seen = int(tradeable[first:last + 1].sum())
    if signals_seen == 0 and int(sig[first:last + 1].sum()) > 0:
        raise AssertionError(
            "book saw signals but none were tradeable: "
            f"sig={int(sig[first:last+1].sum())} resolved={int(g.resolved[first:last+1].sum())} "
            f"window=[{window_start},{window_end}] frame_as_of={frames.as_of} spec={spec.signature}"
        )

    for i in range(first, last + 1):
        today = dates[i]

        # 1. Close anything whose exit session is today.
        for sym in [s for s, p in open_pos.items() if p["exit_i"] == i]:
            p = open_pos.pop(sym)
            trade: Trade = p["trade"]
            cash += p["qty_value"] * (1.0 + p["sign"] * (trade.exit_price / trade.entry_price - 1.0)) \
                - p["qty_value"] * trade.costs_pct / 100.0
            closed.append(trade)

        # 2. Mark the book to today's close.
        marked = 0.0
        for sym, p in open_pos.items():
            j = p["col"]
            px = closes[i, j]
            if not np.isfinite(px):
                px = p["last_px"]
            p["last_px"] = px
            marked += p["qty_value"] * (1.0 + p["sign"] * (px / p["trade"].entry_price - 1.0))
        equity_dates.append(today)
        equity_vals.append(cash + marked)

        # 3. Open new positions from today's signals (fill at TOMORROW's open).
        cols = np.nonzero(tradeable[i])[0]
        if cols.size == 0:
            continue
        order = SELECTION_RULES[selection_rule](cols, rank_by[i], rng)
        opened_today = 0
        for j in order:
            if len(open_pos) >= cfg.max_concurrent_positions:
                break
            if opened_today >= max_new_positions_per_session:
                break
            sym = str(symbols[j])
            if sym in open_pos:
                continue
            if cash < per_position:
                break
            k = int(g.k[i, j])
            trade = Trade(
                symbol=sym, direction=spec.direction,
                signal_date=today.date(), entry_date=dates[i + 1].date(),
                entry_price=float(g.entry[i, j]),
                exit_date=dates[i + k].date(), exit_price=float(g.exit_px[i, j]),
                exit_reason={1: "stop", 2: "target", 3: "horizon"}[int(g.reason[i, j])],
                holding_sessions=k, pnl_pct_gross=float(g.gross[i, j]),
                pnl_pct_net=float(g.net[i, j]), costs_pct=float(costs.round_trip_pct),
                slippage_bps=float(costs.slippage_bps_per_side),
                mfe_pct=float(g.mfe[i, j]), mae_pct=float(g.mae[i, j]),
            )
            cash -= per_position
            open_pos[sym] = {
                "col": int(j), "trade": trade, "exit_i": i + k,
                "qty_value": per_position, "last_px": float(g.entry[i, j]),
                "sign": 1.0 if spec.direction == "long" else -1.0,
            }
            opened_today += 1
            signals_taken += 1

    open_at_end = tuple(p["trade"] for p in open_pos.values())
    return BookResult(
        capital_inr=cfg.capital_inr,
        closed=tuple(closed),
        open_at_end=open_at_end,
        equity=pd.Series(equity_vals, index=pd.DatetimeIndex(equity_dates)),
        signals_seen=signals_seen, signals_taken=signals_taken,
        sessions_walked=last - first + 1,
        window_start=window_start, window_end=window_end, selection_rule=selection_rule,
    )
