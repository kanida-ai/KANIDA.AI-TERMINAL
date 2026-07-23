"""
Risk rules for the championship harness. Pure functions over Position state.

Per the brief: fast exits for losers, let winners run, time-stop for stagnation,
portfolio-level kill switch.

Each rule returns (should_close, reason) — None if no action.
"""
from __future__ import annotations
from collections import deque
from datetime import date
from typing import Deque, Dict, Optional, Tuple

from engine.champ_portfolio import Position, Portfolio


# ── Default thresholds ────────────────────────────────────────────────────────

INITIAL_STOP_PCT      = 0.07       # cut a fresh position at -7% from avg entry
BREAKEVEN_TRIGGER_PCT = 0.10       # once up +10%, tighten stop to entry
TRAIL_TRIGGER_PCT     = 0.20       # once up +20%, switch to trailing stop
TRAIL_LOOKBACK_DAYS   = 10         # trail = 10-day low
TIME_STOP_DAYS        = 30         # cut flat positions after 30 trading days
TIME_STOP_MIN_RETURN  = 0.03       # ... unless they're at least +3%
KILL_SWITCH_LOSS      = -0.15      # liquidate when equity drops 15% below STARTING capital
                                    # (absolute, not peak-relative — avoids cutting winners
                                    # on normal pullbacks after pyramiding inflates peak)


# ── Helpers ───────────────────────────────────────────────────────────────────

def days_since(open_date: str, today: str) -> int:
    a = date.fromisoformat(open_date)
    b = date.fromisoformat(today)
    return (b - a).days


# ── Per-position rules ────────────────────────────────────────────────────────

def check_initial_stop(pos: Position, last_price: float) -> Optional[Tuple[bool, str]]:
    if pos.high_water >= BREAKEVEN_TRIGGER_PCT:
        return None    # promoted to breakeven stop, handled below
    cur = (last_price / pos.avg_entry) - 1.0
    if cur <= -INITIAL_STOP_PCT:
        return True, "initial_stop"
    return None


def check_breakeven_stop(pos: Position, last_price: float) -> Optional[Tuple[bool, str]]:
    """After +10%, never let it go red. Trail-stop takes over above +20%."""
    if pos.high_water < BREAKEVEN_TRIGGER_PCT:
        return None
    if pos.high_water >= TRAIL_TRIGGER_PCT:
        return None    # delegated to trail
    if last_price <= pos.avg_entry:
        return True, "breakeven_stop"
    return None


def check_trail_stop(pos: Position, last_price: float,
                      recent_lows: Deque[float]) -> Optional[Tuple[bool, str]]:
    """Once up +20%, cut on close below the rolling 10-day low."""
    if pos.high_water < TRAIL_TRIGGER_PCT:
        return None
    if not recent_lows:
        return None
    floor = min(recent_lows)
    if last_price < floor:
        return True, "trail_stop"
    return None


def check_time_stop(pos: Position, last_price: float, today: str) -> Optional[Tuple[bool, str]]:
    if days_since(pos.open_date, today) < TIME_STOP_DAYS:
        return None
    cur = (last_price / pos.avg_entry) - 1.0
    if cur < TIME_STOP_MIN_RETURN:
        return True, "time_stop"
    return None


def evaluate_position(pos: Position, last_price: float, today: str,
                       recent_lows: Deque[float]) -> Optional[str]:
    """Run all per-position rules in order; return reason for first hit, else None."""
    for fn in (check_initial_stop, check_breakeven_stop,
               check_time_stop):
        r = fn(pos, last_price) if fn is not check_time_stop else fn(pos, last_price, today)
        if r and r[0]:
            return r[1]
    r = check_trail_stop(pos, last_price, recent_lows)
    if r and r[0]:
        return r[1]
    return None


# ── Portfolio-level kill switch ───────────────────────────────────────────────

def kill_switch_active(portfolio: Portfolio) -> bool:
    if not portfolio.equity_curve:
        return False
    last = portfolio.equity_curve[-1]
    abs_loss = (last["equity"] / portfolio.starting_capital) - 1.0
    return abs_loss <= KILL_SWITCH_LOSS
