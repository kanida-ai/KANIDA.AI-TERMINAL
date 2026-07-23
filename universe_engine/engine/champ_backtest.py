"""
Concentrated-portfolio walk-forward backtest, single 5-month window.

Mechanics:
  - At window start: rank eligible universe by signal, take top-N (3-8), enter
    equal-weight base size each, all signal scores computed STRICTLY from bars
    BEFORE window_start (no peeking).
  - Each trading day: mark-to-market, check kill-switch, evaluate per-position
    risk rules (stops, trail, time-stop). Close if triggered.
  - Each trading day, also evaluate pyramid adds for open positions (+5%, +10%
    triggers). Adds use signal NOT involved — purely price-based.
  - When a slot frees (a position closes), the harness refills from the
    next-best ex-ante candidate that hasn't already been used in this window.
    The replacement signal is recomputed using bars strictly before today.
  - At window end: forced exit at last available close, marked as "window_end".

Returns: dict with summary metrics + per-position trade log + equity curve.
"""
from __future__ import annotations
import sqlite3
from collections import deque
from datetime import date, timedelta
from pathlib import Path
from typing import Callable, Deque, Dict, List, Optional

from engine.champ_universe   import UniverseFilter, get_active_universe
from engine.champ_portfolio  import Portfolio
from engine.champ_risk       import (
    evaluate_position, kill_switch_active, TRAIL_LOOKBACK_DAYS
)
from engine.champ_signals    import SignalContext, get_signal, load_universe_bars


def trading_days_between(con: sqlite3.Connection,
                          start: str, end: str) -> List[str]:
    rows = con.execute("""
        SELECT DISTINCT trade_date FROM ohlc_daily
        WHERE trade_date >= ? AND trade_date <= ? ORDER BY trade_date
    """, (start, end)).fetchall()
    return [r[0] for r in rows]


def run_window(con: sqlite3.Connection,
                signal_name: str,
                window_start: str,
                window_end: str,
                top_n: int = 5,
                starting_capital: float = 1_000_000.0,
                cost_bps: float = 30.0,
                slippage_bps: float = 5.0,
                index_col: str = "in_nifty200",
                universe_filter: Optional[UniverseFilter] = None) -> dict:
    """
    Run one rolling-window backtest.
      window_start, window_end: ISO dates inclusive. Signals use bars STRICTLY
      before window_start. Trading happens [window_start, window_end].
    """
    universe = get_active_universe(con, index_col=index_col)
    if universe_filter is None:
        universe_filter = UniverseFilter(con, universe)

    # Load bars from 2 years before window_start through window_end + 30d slack.
    # Signals need ~252 bars of history; load generously.
    load_from = date.fromisoformat(window_start) - timedelta(days=400)
    load_to   = date.fromisoformat(window_end) + timedelta(days=10)
    bars = load_universe_bars(con, universe,
                                from_date=load_from.isoformat(),
                                to_date=load_to.isoformat())

    trading_days = trading_days_between(con, window_start, window_end)
    if not trading_days:
        return {"error": f"No trading days in {window_start} -> {window_end}"}

    signal_fn = get_signal(signal_name)
    portfolio = Portfolio(starting_capital, cost_bps, slippage_bps)
    # Initial deployment uses ~67% of capital so winners can pyramid up to 2x base
    # without immediately running out of cash. Remaining 33% is a working reserve
    # — pyramids that don't fit are gracefully skipped and refilled when other
    # positions close.
    base_size = (starting_capital * 0.66) / top_n
    used_symbols: set = set()    # don't reuse a symbol that was already cut

    # 10-day low cache per open position for trail stop
    recent_lows: Dict[str, Deque[float]] = {}

    # ── Day-1 entries: pick top_n ───────────────────────────────────────
    eligible = universe_filter.filter_universe(universe, date.fromisoformat(window_start))
    ctx = SignalContext(bars=bars, on_date=window_start)
    scores = signal_fn(ctx, eligible)
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    # Day-1 prices: use that day's open as fill (causal: open of window_start)
    day1_prices = _prices_on_date(bars, trading_days[0])
    picked = []
    for sym, score in ranked:
        if len(picked) >= top_n: break
        if sym not in day1_prices: continue
        if portfolio.open(sym, trading_days[0], day1_prices[sym], base_size):
            picked.append((sym, score))
            used_symbols.add(sym)
            recent_lows[sym] = deque(maxlen=TRAIL_LOOKBACK_DAYS)

    # ── Day loop ────────────────────────────────────────────────────────
    for di, today in enumerate(trading_days):
        prices = _prices_on_date(bars, today)
        # Update trail-stop lows for all currently open positions
        for sym in list(portfolio.positions.keys()):
            bar = _bar_on_date(bars.get(sym, []), today)
            if bar:
                recent_lows.setdefault(sym, deque(maxlen=TRAIL_LOOKBACK_DAYS)).append(bar["low"])

        # 1) Per-position rules
        for sym in list(portfolio.positions.keys()):
            px = prices.get(sym)
            if px is None: continue
            pos = portfolio.positions[sym]
            reason = evaluate_position(pos, px, today, recent_lows.get(sym, deque()))
            if reason:
                portfolio.close(sym, today, px, reason)

        # 2) Mark-to-market + kill switch
        portfolio.mark_to_market(today, prices)
        if kill_switch_active(portfolio):
            for sym in list(portfolio.positions.keys()):
                px = prices.get(sym)
                if px is not None:
                    portfolio.close(sym, today, px, "kill_switch")
            # No more entries this window after kill switch
            for d_remaining in trading_days[di+1:]:
                portfolio.mark_to_market(d_remaining, _prices_on_date(bars, d_remaining))
            break

        # 3) Pyramid adds
        for sym in list(portfolio.positions.keys()):
            px = prices.get(sym)
            if px is None: continue
            pos = portfolio.positions[sym]
            add_rupees = pos.should_pyramid(px)
            if add_rupees:
                portfolio.pyramid(sym, today, px, add_rupees)

        # 4) Refill empty slots — pick next best ex-ante candidate
        slots_open = top_n - len(portfolio.positions)
        if slots_open > 0 and di > 0:
            ctx_today = SignalContext(bars=bars, on_date=today)
            elig_today = universe_filter.filter_universe(universe, date.fromisoformat(today))
            scores_today = signal_fn(ctx_today, elig_today)
            ranked_today = sorted(scores_today.items(), key=lambda x: x[1], reverse=True)
            for sym, _score in ranked_today:
                if slots_open <= 0: break
                if sym in portfolio.positions or sym in used_symbols: continue
                if sym not in prices: continue
                pos_obj = portfolio.open(sym, today, prices[sym], base_size)
                if pos_obj:
                    used_symbols.add(sym)
                    recent_lows[sym] = deque(maxlen=TRAIL_LOOKBACK_DAYS)
                    slots_open -= 1

    # ── Forced exit at window end ──────────────────────────────────────
    last_day = trading_days[-1]
    last_prices = _prices_on_date(bars, last_day)
    for sym in list(portfolio.positions.keys()):
        px = last_prices.get(sym)
        if px is not None:
            portfolio.close(sym, last_day, px, "window_end")

    # ── Pack results ────────────────────────────────────────────────────
    summ = portfolio.summary()
    trades = [_pos_to_trade(p) for p in portfolio.closed_positions]
    return {
        "signal":          signal_name,
        "window_start":    window_start,
        "window_end":      window_end,
        "top_n":           top_n,
        "summary":         summ,
        "trades":          trades,
        "equity_curve":    portfolio.equity_curve,
        "first_picks":     [{"symbol": s, "score": round(sc, 4)} for s, sc in picked],
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _prices_on_date(bars: Dict[str, List[dict]], on_date: str) -> Dict[str, float]:
    """Use closing price as the day's reference price."""
    out = {}
    for sym, blist in bars.items():
        b = _bar_on_date(blist, on_date)
        if b is not None:
            out[sym] = b["close"]
    return out


def _bar_on_date(bars: List[dict], on_date: str) -> Optional[dict]:
    """Linear search through sorted-asc bars; returns bar matching on_date or None."""
    for b in bars:
        if b["trade_date"] == on_date:
            return b
        if b["trade_date"] > on_date:
            return None
    return None


def _pos_to_trade(pos) -> dict:
    """Extract a flat trade record from a closed Position."""
    if pos.close_price is None or pos.avg_entry <= 0:
        return {"symbol": pos.symbol, "open": pos.open_date, "close": pos.close_date,
                "ret_pct": 0, "reason": pos.close_reason}
    ret = (pos.close_price / pos.avg_entry - 1) * 100
    n_adds = sum(1 for f in pos.fills if f["kind"].startswith("add"))
    return {
        "symbol":         pos.symbol,
        "open":           pos.open_date,
        "close":          pos.close_date,
        "avg_entry":      round(pos.avg_entry, 2),
        "close_price":    round(pos.close_price, 2),
        "ret_pct":        round(ret, 2),
        "high_water_pct": round(pos.high_water * 100, 2),
        "n_pyramid_adds": n_adds,
        "capital":        round(pos.capital_deployed, 0),
        "reason":         pos.close_reason,
    }
