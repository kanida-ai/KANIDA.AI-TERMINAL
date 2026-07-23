"""
Trade simulator — long-only.

Two entry modes:
  - blind: market BUY at entry-day open
  - smart: wait until ~9:30; enter only if price >= entry-day open AND price <= entry-day open * (1 + max_above_pct)
           AND volume in first 15 min > average. Uses 1-min bars if available; else
           falls back to a daily proxy:
              "filled" iff entry_high >= entry_open (price went up at some point)
                     AND entry_high <= entry_open * (1 + max_above_pct)  (didn't gap-chase)
                     AND entry_close > entry_open (closed above open — direction confirm)
              entry_price = entry_open * (1 + 0.5 * (entry_close - entry_open) / entry_open)
                            (proxy: ~midway between open and close)

TP/SL/time-cap simulation walks forward bars from entry+1 onward.
Day-12 hard cap: if not exited by bar 12, force-close at bar 12's close.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional


# ── ATR (re-implemented locally to avoid importing strategies into here) ──

def atr(bars: list[dict], idx: int, n: int = 14) -> float:
    """bars: list of dicts with high/low/close keys. idx must be >= 1."""
    if idx < 1:
        return 0.0
    start = max(1, idx - n + 1)
    trs = []
    for j in range(start, idx + 1):
        h = float(bars[j]["high"]); l = float(bars[j]["low"])
        pc = float(bars[j-1]["close"])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / max(len(trs), 1)


# ── Sizing helpers ──

def compute_stop_pct(bar: dict, prev_bars: list[dict], idx: int,
                      atr_period: int = 14, atr_mult: float = 1.5,
                      stop_min_pct: float = 1.5, stop_max_pct: float = 8.0) -> float:
    """ATR-based stop as % of entry price, clamped."""
    a = atr(prev_bars, idx, atr_period) if idx >= 1 else 0
    if a <= 0:
        return stop_min_pct
    pct = a / float(bar["close"]) * 100 * atr_mult
    return max(stop_min_pct, min(stop_max_pct, pct))


def compute_target_pct(stop_pct: float, rr: float = 1.5) -> float:
    return stop_pct * rr


# ── Simulator outputs ──

@dataclass
class SimResult:
    entry_taken:    bool   = False
    entry_price:    float  = 0.0
    target_price:   float  = 0.0
    stop_price:     float  = 0.0
    exit_date:      str    = ""
    exit_price:     float  = 0.0
    exit_reason:    str    = ""        # tp | sl | timeout | cap12 | rejected_smart
    days_held:      int    = 0
    pnl_pct:        float  = 0.0


# ── Core simulator ──

def simulate_long(
    forward_bars: list[dict],
    entry_price: float,
    stop_price: float,
    target_price: float,
    hold_cap_days: int = 12,
) -> SimResult:
    """Walk forward bars; first bar is entry-day +1. Return SimResult."""
    if not forward_bars or entry_price <= 0:
        return SimResult()

    exit_price = entry_price
    exit_date  = forward_bars[0]["trade_date"] if forward_bars else ""
    exit_reason = "timeout"
    days = 0

    cap = min(hold_cap_days, len(forward_bars))

    for j, b in enumerate(forward_bars[:cap], start=1):
        bh = float(b["high"]); bl = float(b["low"])
        # SL hit before TP if both inside (worst-case fill — pessimistic-correct)
        if bl <= stop_price:
            exit_price = stop_price
            exit_date  = b["trade_date"]
            exit_reason = "sl"
            days = j
            break
        if bh >= target_price:
            exit_price = target_price
            exit_date  = b["trade_date"]
            exit_reason = "tp"
            days = j
            break
    else:
        # Reached cap without TP/SL
        last = forward_bars[cap - 1] if cap > 0 else None
        if last:
            exit_price = float(last["close"])
            exit_date  = last["trade_date"]
            exit_reason = f"cap{hold_cap_days}" if cap == hold_cap_days else "timeout"
            days = cap

    pnl = (exit_price - entry_price) / entry_price * 100 if entry_price > 0 else 0.0
    return SimResult(
        entry_taken=True, entry_price=round(entry_price, 4),
        target_price=round(target_price, 4), stop_price=round(stop_price, 4),
        exit_date=exit_date, exit_price=round(exit_price, 4),
        exit_reason=exit_reason, days_held=days, pnl_pct=round(pnl, 4),
    )


# ── Smart-entry filter (daily-proxy) ──

def smart_entry_proxy(entry_bar: dict, max_above_open_pct: float = 3.0) -> tuple[bool, float]:
    """
    Daily-bar proxy for smart entry. Returns (taken, entry_price).
    Conditions:
      - entry_high >= entry_open               (price went up at some point during the day)
      - entry_high <= entry_open * (1 + cap)   (didn't gap-chase; cap at +3% above open)
      - entry_close > entry_open               (direction confirms by close)
    Entry price proxy: midway between open and close (≈ early-session entry).
    """
    o = float(entry_bar["open"]); h = float(entry_bar["high"])
    c = float(entry_bar["close"])
    if o <= 0:
        return False, 0.0
    if h < o:                                       # never went above open
        return False, 0.0
    if h > o * (1 + max_above_open_pct / 100):     # gapped/ran too far
        return False, 0.0
    if c <= o:                                      # didn't close above open
        return False, 0.0
    proxy_entry = round(o + 0.5 * (c - o), 4)
    return True, proxy_entry


# ── Top-level: run both modes for a single signal ──

@dataclass
class TradeOutcome:
    blind: SimResult = field(default_factory=SimResult)
    smart: SimResult = field(default_factory=SimResult)


def simulate_signal(
    entry_bar: dict,                        # entry-day OHLCV
    forward_bars_after_entry: list[dict],   # bars from entry+1 onward
    prev_bars_for_atr: list[dict],          # bars up to and including signal day (for ATR)
    signal_day_idx: int,                    # idx of signal day in prev_bars (last index)
    rr: float = 1.5,
    atr_period: int = 14,
    atr_mult: float = 1.5,
    stop_min_pct: float = 1.5,
    stop_max_pct: float = 8.0,
    hold_cap_days: int = 12,
    smart_max_above_open_pct: float = 3.0,
) -> TradeOutcome:
    out = TradeOutcome()
    if not entry_bar or entry_bar.get("open", 0) <= 0:
        return out

    # Stop pct from ATR computed on signal day
    stop_pct = compute_stop_pct(
        entry_bar, prev_bars_for_atr, signal_day_idx,
        atr_period=atr_period, atr_mult=atr_mult,
        stop_min_pct=stop_min_pct, stop_max_pct=stop_max_pct,
    )
    target_pct = compute_target_pct(stop_pct, rr=rr)

    # ── BLIND ENTRY ──
    blind_entry = float(entry_bar["open"])
    blind_stop  = blind_entry * (1 - stop_pct / 100)
    blind_tgt   = blind_entry * (1 + target_pct / 100)
    out.blind = simulate_long(
        forward_bars=forward_bars_after_entry,
        entry_price=blind_entry,
        stop_price=blind_stop,
        target_price=blind_tgt,
        hold_cap_days=hold_cap_days,
    )

    # ── SMART ENTRY ──
    taken, smart_entry = smart_entry_proxy(entry_bar, max_above_open_pct=smart_max_above_open_pct)
    if not taken:
        out.smart = SimResult(entry_taken=False, exit_reason="rejected_smart")
    else:
        smart_stop = smart_entry * (1 - stop_pct / 100)
        smart_tgt  = smart_entry * (1 + target_pct / 100)
        out.smart = simulate_long(
            forward_bars=forward_bars_after_entry,
            entry_price=smart_entry,
            stop_price=smart_stop,
            target_price=smart_tgt,
            hold_cap_days=hold_cap_days,
        )
    return out


# ── Cost-adjusted outcome helpers ──

def cost_adjusted_pnl_pct(pnl_pct: float, round_trip_bps: float = 30.0) -> float:
    """Subtract round-trip cost in basis points from P&L %."""
    return pnl_pct - round_trip_bps / 100.0


def is_win_after_cost(pnl_pct: float, round_trip_bps: float = 30.0) -> bool:
    return cost_adjusted_pnl_pct(pnl_pct, round_trip_bps) > 0
