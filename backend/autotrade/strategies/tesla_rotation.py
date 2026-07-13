"""Falcon Tesla — PURE capital-rotation seat model (no I/O, no orders).

The "seat" model: the session has a fixed `n_seats` and an equal allocation per
seat = total_allocated_capital / n_seats. Each seat holds at most one MIS SHORT
position. When a seat is FREE (a position exited, or never filled), it is
back-filled on the next tick with the best available live Tesla signal.

This module DECIDES ONLY. It never touches a broker, a DB, or the network — the
session places/exits through the hardened _place_one / _exit_single_position
paths. Everything here is deterministic and unit-tested.

CORRECTNESS NOTE (the denominator): a rotating book has NO single frozen basket
notional, so the per-stock step-lock's frozen-basket-notional slice is WRONG for
rotation. The correct per-seat stop/trail basis is the FIXED seat allocation
(total / n_seats) — see seat_allocation() + seat_g() below. Using anything else
mis-scales the stop as seats turn over (a real-money bug).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Sequence

# grade → rank (higher = stronger). Mirrors the batch select_trade_lifecycle.
_GRADE_RANK = {"A++": 2, "A+++": 3}


@dataclass
class BackfillEntry:
    """One seat to fill this tick."""
    symbol: str
    grade: str
    ref_price: float
    allocation: float          # ₹ per seat (total / n_seats), FIXED
    short_drive: float = 0.0
    setup: str = ""


def seat_allocation(total_capital: float, n_seats: int) -> float:
    """Equal ₹ allocation per seat. n_seats>=1 enforced by config.validate()."""
    if n_seats <= 0:
        raise ValueError("n_seats must be >= 1")
    return float(total_capital) / int(n_seats)


def seat_g(position_upnl: float, allocation: float) -> float:
    """The per-seat capital-basis return: uPnL / the seat's FIXED allocation.
    This is the leverage-correct denominator for the per-seat stop/trail — it does
    NOT shrink as sibling seats turn over (unlike a Σ-notional basket slice)."""
    if allocation <= 0:
        return 0.0
    return float(position_upnl) / float(allocation)


def _grade_ok(grade: str, min_grade: str) -> bool:
    if str(min_grade).upper() in ("A+++", "STRONG"):
        return grade == "A+++"
    return grade in ("A++", "A+++")


def _rank_key(s):
    """Secondary rank key = v3_rank_score when the DiD layer supplied one
    (rank_score is not None), else short_drive. For the v2 path rank_score is
    None, so this is byte-identical to the pre-DiD ordering."""
    rs = getattr(s, "rank_score", None)
    secondary = float(rs) if rs is not None else float(getattr(s, "short_drive", 0.0))
    return (_GRADE_RANK.get(str(s.grade), 0), secondary)


def rank_signals(signals: Sequence) -> List:
    """Best-first: strongest grade, then the seat rank key. Stable + pure.
    `signals` items must expose .grade and .short_drive (TeslaSignal-shaped);
    an optional .rank_score (v3 DiD) is preferred as the secondary key when set."""
    return sorted(signals, key=_rank_key, reverse=True)


def plan_backfill(
    *,
    signals: Sequence,
    open_symbols: Sequence[str],
    n_seats: int,
    total_capital: float,
    now: datetime,
    last_entry_at: Optional[Dict[str, datetime]] = None,
    held_ever: Optional[Sequence[str]] = None,
    cooldown_minutes: int = 30,
    min_grade: str = "A++",
    allow_reentry: bool = False,
) -> List[BackfillEntry]:
    """Decide which FREE seats to fill this tick.

    Rules (all conservative, real-money-safe):
      - free_seats = n_seats - (currently OPEN positions). Never over-fill.
      - a signal already OPEN (held on a seat) is skipped (no duplicate/averaging).
      - a signal for a name already TRADED this session is skipped unless
        allow_reentry (default False — avoid churn / the double-cover class).
      - per-instrument COOLDOWN: skip a name entered < cooldown_minutes ago.
      - only signals meeting min_grade are eligible.
      - best-first (grade, then short_drive); at most `free_seats` returned; each
        name at most once per plan.

    Returns the list of BackfillEntry (symbol, grade, ref_price, allocation). The
    caller places each through _place_one at `allocation` = total/n_seats.
    """
    open_set = {str(s) for s in open_symbols}
    held = {str(s) for s in (held_ever or [])}
    last = last_entry_at or {}
    free = int(n_seats) - len(open_set)
    if free <= 0 or not signals:
        return []
    alloc = seat_allocation(total_capital, n_seats)
    out: List[BackfillEntry] = []
    chosen: set = set()
    for sig in rank_signals(signals):
        if len(out) >= free:
            break
        sym = str(sig.instrument)
        if sym in open_set or sym in chosen:
            continue
        if not allow_reentry and sym in held:
            continue
        if not _grade_ok(str(sig.grade), min_grade):
            continue
        prev = last.get(sym)
        if prev is not None and (now - prev).total_seconds() < cooldown_minutes * 60:
            continue
        ref = float(getattr(sig, "entry_ref_price", 0.0) or 0.0)
        if ref <= 0:
            continue
        out.append(BackfillEntry(
            symbol=sym, grade=str(sig.grade), ref_price=ref, allocation=alloc,
            short_drive=float(getattr(sig, "short_drive", 0.0) or 0.0),
            setup=str(getattr(sig, "setup", "") or "")))
        chosen.add(sym)
    return out
