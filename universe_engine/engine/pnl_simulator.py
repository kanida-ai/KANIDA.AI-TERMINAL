"""
P&L simulator for Engine V3 production signals.

Rules (per Pudhuraja's spec):
  - ₹1L deployed PER TRADE on entry. No total-capital cap — capital scales.
  - Entry: Method B fill at em_b_entry (next-day buy-stop with 9:30 vol confirm).
  - Exit:  T+5 close (matches our 5d outcome measurement).
  - All ₹1L per trade fills (no fractional sizing). One position per (symbol, signal_date).

Outputs:
  - Total P&L ₹
  - Return on capital % (two views: sum-of-deployed vs peak-deployed)
  - Max drawdown (on mark-to-market equity curve, daily)
  - Monthly P&L (by exit month)
  - Win rate
  - Average win, average loss, win/loss ratio
  - Max concurrent positions used
  - Capital utilization (avg deployed / peak deployed)

Approach: rebuild a daily mark-to-market equity curve by loading per-position
daily close from ohlc_daily, walking each position from entry day to exit day,
and bookkeeping cash + open MTM each trading day across the simulation window.
"""
from __future__ import annotations
import sqlite3, statistics
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple


PER_TRADE_RUPEES   = 100_000.0
HOLD_DAYS          = 5            # T+5 close exit
COST_BPS_RT        = 30.0         # round-trip 30bps fee total
SLIPPAGE_BPS       = 5.0          # one-side


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _trading_days_after(con: sqlite3.Connection, symbol: str,
                          start_date: str, n: int) -> List[str]:
    rows = con.execute("""
        SELECT trade_date FROM ohlc_daily
        WHERE symbol=? AND trade_date>=?
        ORDER BY trade_date LIMIT ?
    """, (symbol, start_date, n + 1)).fetchall()
    return [r[0] for r in rows]


def _close_on(con: sqlite3.Connection, symbol: str, trade_date: str) -> Optional[float]:
    r = con.execute("SELECT close FROM ohlc_daily WHERE symbol=? AND trade_date=?",
                     (symbol, trade_date)).fetchone()
    return r[0] if r else None


def _all_trading_days(con: sqlite3.Connection, start: str, end: str) -> List[str]:
    rows = con.execute("""
        SELECT DISTINCT trade_date FROM ohlc_daily
        WHERE trade_date>=? AND trade_date<=? ORDER BY trade_date
    """, (start, end)).fetchall()
    return [r[0] for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
# Build per-trade ledger
# ─────────────────────────────────────────────────────────────────────────────

def build_trades(signals: List[Dict], con: sqlite3.Connection,
                  per_trade: float = PER_TRADE_RUPEES,
                  cost_bps: float = COST_BPS_RT,
                  slip_bps: float = SLIPPAGE_BPS) -> List[Dict]:
    """For each Method-B-filled signal, build a fully-specced trade with daily
    MTM closes for the 5-day holding window."""
    trades = []
    for s in signals:
        if not s.get("em_b_filled"):
            continue
        entry_date_iso = (date.fromisoformat(s["signal_date"]) + timedelta(days=1)).isoformat()
        # Get 1+HOLD_DAYS trading days from signal_date onwards (entry + holds)
        days = _trading_days_after(con, s["symbol"], s["signal_date"], HOLD_DAYS + 5)
        # First day in days is the signal day itself; entry is the FIRST trading day AFTER signal
        days_after = [d for d in days if d > s["signal_date"]]
        if len(days_after) < HOLD_DAYS:
            continue

        entry_date = days_after[0]
        exit_date  = days_after[HOLD_DAYS - 1] if len(days_after) >= HOLD_DAYS else days_after[-1]
        hold_days  = days_after[:HOLD_DAYS]    # entry day through exit day inclusive

        # Apply slippage to exec price
        slip = slip_bps / 10_000.0
        entry_price = s["em_b_entry"] * (1 + slip)
        shares = per_trade / entry_price

        # Daily closes during holding period
        daily_closes = {d: _close_on(con, s["symbol"], d) for d in hold_days}
        if any(v is None for v in daily_closes.values()):
            continue   # data hole — skip

        # Realized exit at last close, with exit slippage
        exit_close = daily_closes[exit_date]
        exit_price = exit_close * (1 - slip)
        gross_pnl  = shares * (exit_price - entry_price)
        fees       = per_trade * (cost_bps / 10_000.0)
        net_pnl    = gross_pnl - fees

        trades.append({
            "symbol":        s["symbol"],
            "signal_date":   s["signal_date"],
            "patterns":      "+".join(sorted(s["patterns"])),
            "entry_date":    entry_date,
            "exit_date":     exit_date,
            "entry_price":   round(entry_price, 4),
            "exit_price":    round(exit_price, 4),
            "shares":        round(shares, 4),
            "deployed":      round(per_trade, 2),
            "gross_pnl":     round(gross_pnl, 2),
            "fees":          round(fees, 2),
            "net_pnl":       round(net_pnl, 2),
            "ret_pct":       round(net_pnl / per_trade * 100, 3),
            "hold_days":     hold_days,
            "daily_closes":  daily_closes,
        })
    return trades


# ─────────────────────────────────────────────────────────────────────────────
# Daily mark-to-market timeline
# ─────────────────────────────────────────────────────────────────────────────

def build_daily_timeline(trades: List[Dict],
                          con: sqlite3.Connection,
                          per_trade: float = PER_TRADE_RUPEES,
                          slip_bps: float = SLIPPAGE_BPS) -> List[Dict]:
    """Walk every trading day in the simulation window. For each day:
       - count open positions
       - sum capital deployed (positions × per_trade)
       - sum realized P&L (trades that exited today)
       - sum unrealized MTM (open trades, value at today's close)
       - compute equity = cumulative_realized + unrealized
       - track running peak equity & drawdown."""
    if not trades: return []

    # Earliest entry, latest exit
    start_date = min(t["entry_date"] for t in trades)
    end_date   = max(t["exit_date"]  for t in trades)
    all_days   = _all_trading_days(con, start_date, end_date)

    # Index trades by entry_date and exit_date for fast lookup
    by_entry: Dict[str, List[Dict]] = defaultdict(list)
    by_exit:  Dict[str, List[Dict]] = defaultdict(list)
    for t in trades:
        by_entry[t["entry_date"]].append(t)
        by_exit[t["exit_date"]].append(t)

    open_trades: List[Dict] = []
    cumulative_realized = 0.0
    timeline = []
    peak_equity = 0.0

    for d in all_days:
        # Add new entries
        for t in by_entry.get(d, []):
            open_trades.append(t)

        # Remove exits AFTER booking realized P&L
        exiting_today = by_exit.get(d, [])
        realized_today = sum(t["net_pnl"] for t in exiting_today)
        cumulative_realized += realized_today

        # Compute unrealized MTM for still-open trades (those entered <= d, exit > d)
        # Note: trades that exit TODAY are still in open_trades at this point — we'll
        # remove them after MTM, since their P&L is now "realized today".
        open_today = [t for t in open_trades if t["exit_date"] >= d]
        unrealized = 0.0
        for t in open_today:
            if t["exit_date"] == d:
                continue   # exit today already booked as realized
            cl = t["daily_closes"].get(d) or _close_on(con, t["symbol"], d)
            if cl is None:
                continue
            slip = slip_bps / 10_000.0
            mtm_price = cl * (1 - slip)
            unrealized += t["shares"] * (mtm_price - t["entry_price"])

        # Strip out positions that exited today
        open_trades = [t for t in open_trades if t["exit_date"] > d]

        n_open = len(open_trades)
        deployed_now = n_open * per_trade
        equity = cumulative_realized + unrealized
        if equity > peak_equity:
            peak_equity = equity
        drawdown = equity - peak_equity   # ₹ drawdown from running peak

        timeline.append({
            "date": d,
            "n_open_after": n_open,
            "deployed_after": deployed_now,
            "realized_today": realized_today,
            "cum_realized": cumulative_realized,
            "unrealized": unrealized,
            "equity": equity,
            "drawdown": drawdown,
        })
    return timeline


# ─────────────────────────────────────────────────────────────────────────────
# Aggregate metrics
# ─────────────────────────────────────────────────────────────────────────────

def compute_metrics(trades: List[Dict], timeline: List[Dict],
                     per_trade: float = PER_TRADE_RUPEES) -> Dict:
    if not trades:
        return {"error": "no trades"}

    n = len(trades)
    pnls = [t["net_pnl"] for t in trades]
    rets = [t["ret_pct"] for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    total_pnl = sum(pnls)
    sum_deployed = n * per_trade

    # Peak deployed (max concurrent positions × per_trade)
    peak_deployed = max((tl["deployed_after"] for tl in timeline), default=0)
    avg_deployed = statistics.mean([tl["deployed_after"] for tl in timeline]) if timeline else 0
    max_concurrent = max((tl["n_open_after"] for tl in timeline), default=0)

    # Drawdown — minimum of timeline['drawdown'] (most-negative ₹ value)
    dd_min_rupees = min((tl["drawdown"] for tl in timeline), default=0)
    # Express as % of peak equity, or peak deployed if equity is small
    peak_equity = max((tl["equity"] for tl in timeline), default=0)
    dd_pct_of_peak_equity = (dd_min_rupees / peak_equity * 100) if peak_equity > 0 else 0
    dd_pct_of_peak_deployed = (dd_min_rupees / peak_deployed * 100) if peak_deployed > 0 else 0

    # Monthly P&L by exit month
    monthly = defaultdict(float)
    for t in trades:
        m = t["exit_date"][:7]
        monthly[m] += t["net_pnl"]

    # Returns
    roc_sum = (total_pnl / sum_deployed * 100) if sum_deployed else 0
    roc_peak = (total_pnl / peak_deployed * 100) if peak_deployed else 0
    # Capital utilization (avg / peak)
    cap_util = (avg_deployed / peak_deployed) if peak_deployed > 0 else 0

    return {
        "n_trades":           n,
        "total_pnl":          total_pnl,
        "sum_deployed":       sum_deployed,
        "peak_deployed":      peak_deployed,
        "avg_deployed":       avg_deployed,
        "max_concurrent":     max_concurrent,
        "capital_utilization": cap_util,
        "roc_sum_deployed":   roc_sum,
        "roc_peak_deployed":  roc_peak,
        "max_drawdown_rupees": dd_min_rupees,
        "max_dd_pct_peak_equity":   dd_pct_of_peak_equity,
        "max_dd_pct_peak_deployed": dd_pct_of_peak_deployed,
        "win_rate":           sum(1 for p in pnls if p > 0) / n * 100,
        "avg_win":            statistics.mean(wins) if wins else 0,
        "avg_loss":           statistics.mean(losses) if losses else 0,
        "win_loss_ratio":     (statistics.mean(wins) / abs(statistics.mean(losses)))
                                if (wins and losses and statistics.mean(losses) != 0) else None,
        "best_trade":         max(pnls),
        "worst_trade":        min(pnls),
        "avg_ret_pct":        statistics.mean(rets),
        "monthly_pnl":        dict(monthly),
    }
