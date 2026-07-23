"""
Real-cash-constrained P&L simulator for V3 production signals.

Constraints (per Pudhuraja's spec):
  - Starting capital: ₹30,00,000 (configurable)
  - Per-trade slug: ₹1,00,000
  - No margin / no borrowing
  - Max concurrent positions: 30
  - Skip new trade if available cash < ₹1L OR open >= 30
  - On exit: release ₹1L + net P&L back to cash

Tracks daily: cash, deployed, equity, utilization, skipped count.
Outputs full timeline + summary metrics.
"""
from __future__ import annotations
import sqlite3, statistics
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional


PER_TRADE_RUPEES   = 100_000.0
HOLD_DAYS          = 5
COST_BPS_RT        = 30.0
SLIPPAGE_BPS       = 5.0
MAX_CONCURRENT     = 30
START_CAPITAL      = 30_00_000.0


def _trading_days_after(con, symbol, start_date, n):
    rows = con.execute("""SELECT trade_date FROM ohlc_daily
                          WHERE symbol=? AND trade_date>? ORDER BY trade_date LIMIT ?""",
                       (symbol, start_date, n)).fetchall()
    return [r[0] for r in rows]


def _close_on(con, symbol, trade_date):
    r = con.execute("SELECT close FROM ohlc_daily WHERE symbol=? AND trade_date=?",
                     (symbol, trade_date)).fetchone()
    return r[0] if r else None


def _all_trading_days(con, start, end):
    rows = con.execute("""SELECT DISTINCT trade_date FROM ohlc_daily
                          WHERE trade_date>=? AND trade_date<=? ORDER BY trade_date""",
                       (start, end)).fetchall()
    return [r[0] for r in rows]


def simulate(signals: List[Dict], con: sqlite3.Connection,
              starting_capital: float = START_CAPITAL,
              per_trade: float = PER_TRADE_RUPEES,
              max_concurrent: int = MAX_CONCURRENT,
              cost_bps: float = COST_BPS_RT,
              slip_bps: float = SLIPPAGE_BPS,
              hold_days: int = HOLD_DAYS) -> Dict:
    """Walk forward day-by-day. Returns trades, daily timeline, metrics, skipped log."""

    # Filter to Method-B-filled signals only and pre-resolve trade dates
    candidates: List[Dict] = []
    for s in signals:
        if not s.get("em_b_filled"):
            continue
        days_after = _trading_days_after(con, s["symbol"], s["signal_date"], hold_days + 5)
        if len(days_after) < hold_days:
            continue
        entry_date = days_after[0]
        exit_date  = days_after[hold_days - 1]
        # Pre-fetch closes for the holding window
        closes = {d: _close_on(con, s["symbol"], d) for d in days_after[:hold_days]}
        if any(v is None for v in closes.values()):
            continue
        candidates.append({**s,
                            "entry_date": entry_date,
                            "exit_date":  exit_date,
                            "hold_dates": days_after[:hold_days],
                            "closes":     closes})

    if not candidates:
        return {"trades": [], "skipped": [], "timeline": [], "metrics": {"error": "no candidates"}}

    # Index entries by date
    entries_by_date: Dict[str, List[Dict]] = defaultdict(list)
    for c in candidates:
        entries_by_date[c["entry_date"]].append(c)

    # Trading-day window
    sim_start = min(c["entry_date"] for c in candidates)
    sim_end   = max(c["exit_date"]  for c in candidates)
    all_days  = _all_trading_days(con, sim_start, sim_end)

    # State
    cash             = starting_capital
    open_positions: List[Dict] = []     # active trades
    closed_trades:  List[Dict] = []     # completed trades with P&L booked
    skipped:        List[Dict] = []     # signals we couldn't take

    timeline: List[Dict] = []
    peak_equity = starting_capital      # for DD calc
    max_concurrent_seen = 0

    slip = slip_bps / 10_000.0
    fee_per_trade = per_trade * (cost_bps / 10_000.0)

    for d in all_days:
        # 1) PROCESS EXITS — close any position whose exit_date == today
        exiters = [p for p in open_positions if p["exit_date"] == d]
        for p in exiters:
            exit_close = p["closes"][d]
            exit_price = exit_close * (1 - slip)
            gross_pnl  = p["shares"] * (exit_price - p["entry_price"])
            net_pnl    = gross_pnl - fee_per_trade
            # Release capital + P&L (positive or negative) back to cash
            cash += per_trade + net_pnl
            closed_trades.append({
                **{k: p[k] for k in ("symbol", "signal_date", "patterns",
                                       "entry_date", "exit_date", "entry_price",
                                       "shares")},
                "exit_price":  round(exit_price, 4),
                "deployed":    round(per_trade, 2),
                "gross_pnl":   round(gross_pnl, 2),
                "fees":        round(fee_per_trade, 2),
                "net_pnl":     round(net_pnl, 2),
                "ret_pct":     round(net_pnl / per_trade * 100, 3),
            })
        open_positions = [p for p in open_positions if p["exit_date"] != d]

        # 2) PROCESS ENTRIES — try to enter each signal whose entry_date == today
        for sig in entries_by_date.get(d, []):
            if cash < per_trade:
                skipped.append({**sig, "reason": "insufficient_cash",
                                  "cash_at_skip": cash, "open_at_skip": len(open_positions)})
                continue
            if len(open_positions) >= max_concurrent:
                skipped.append({**sig, "reason": "max_concurrent",
                                  "cash_at_skip": cash, "open_at_skip": len(open_positions)})
                continue
            entry_price = sig["em_b_entry"] * (1 + slip)
            shares = per_trade / entry_price
            cash -= per_trade
            patterns_str = "+".join(sorted(sig["patterns"]))
            open_positions.append({
                "symbol":      sig["symbol"],
                "signal_date": sig["signal_date"],
                "patterns":    patterns_str,
                "entry_date":  sig["entry_date"],
                "exit_date":   sig["exit_date"],
                "entry_price": round(entry_price, 4),
                "shares":      round(shares, 4),
                "closes":      sig["closes"],
            })

        # 3) MARK-TO-MARKET open positions
        unrealized = 0.0
        deployed = len(open_positions) * per_trade
        for p in open_positions:
            cl = p["closes"].get(d) or _close_on(con, p["symbol"], d)
            if cl is None: continue
            mtm_price = cl * (1 - slip)    # what we'd realise if we sold
            unrealized += p["shares"] * (mtm_price - p["entry_price"])

        equity = cash + deployed + unrealized
        if equity > peak_equity:
            peak_equity = equity
        drawdown = equity - peak_equity
        utilization = (deployed + unrealized) / equity if equity > 0 else 0
        n_open = len(open_positions)
        if n_open > max_concurrent_seen:
            max_concurrent_seen = n_open

        timeline.append({
            "date":              d,
            "cash":              round(cash, 2),
            "deployed":          round(deployed, 2),
            "unrealized":        round(unrealized, 2),
            "equity":            round(equity, 2),
            "n_open":            n_open,
            "drawdown":          round(drawdown, 2),
            "drawdown_pct":      round(drawdown / peak_equity * 100, 3) if peak_equity > 0 else 0,
            "utilization":       round(utilization, 4),
        })

    # ── Metrics ──
    pnls = [t["net_pnl"] for t in closed_trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    final = timeline[-1] if timeline else {"equity": starting_capital, "cash": starting_capital}
    total_pnl = final["equity"] - starting_capital
    monthly = defaultdict(float)
    for t in closed_trades:
        monthly[t["exit_date"][:7]] += t["net_pnl"]

    dd_min = min((tl["drawdown"] for tl in timeline), default=0)
    dd_pct_min = min((tl["drawdown_pct"] for tl in timeline), default=0)
    avg_util = statistics.mean([tl["utilization"] for tl in timeline]) if timeline else 0
    peak_util = max((tl["utilization"] for tl in timeline), default=0)
    avg_deployed = statistics.mean([tl["deployed"] for tl in timeline]) if timeline else 0
    avg_idle    = statistics.mean([tl["cash"]      for tl in timeline]) if timeline else 0

    metrics = {
        "starting_capital":  starting_capital,
        "ending_equity":     final["equity"],
        "ending_cash":       final["cash"],
        "total_pnl":         total_pnl,
        "return_on_starting_pct": (total_pnl / starting_capital * 100) if starting_capital > 0 else 0,
        "trades_taken":      len(closed_trades),
        "trades_skipped":    len(skipped),
        "skipped_breakdown": {
            "insufficient_cash": sum(1 for s in skipped if s["reason"]=="insufficient_cash"),
            "max_concurrent":    sum(1 for s in skipped if s["reason"]=="max_concurrent"),
        },
        "win_rate":          (sum(1 for p in pnls if p > 0) / len(pnls) * 100) if pnls else 0,
        "avg_win":           statistics.mean(wins) if wins else 0,
        "avg_loss":          statistics.mean(losses) if losses else 0,
        "best_trade":        max(pnls) if pnls else 0,
        "worst_trade":       min(pnls) if pnls else 0,
        "win_loss_ratio":    (statistics.mean(wins) / abs(statistics.mean(losses)))
                                if (wins and losses and statistics.mean(losses) != 0) else None,
        "max_drawdown_rupees": dd_min,
        "max_drawdown_pct":    dd_pct_min,
        "max_concurrent":      max_concurrent_seen,
        "avg_utilization":     avg_util,
        "peak_utilization":    peak_util,
        "avg_deployed":        avg_deployed,
        "avg_idle_cash":       avg_idle,
        "monthly_pnl":         dict(monthly),
    }

    return {"trades": closed_trades, "skipped": skipped,
             "timeline": timeline, "metrics": metrics}
