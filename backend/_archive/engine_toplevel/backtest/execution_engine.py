"""
KANIDA.AI Execution Intelligence Engine  (v2 — adaptive)

Separates SIGNAL generation from TRADE ENTRY.

A signal tells you WHAT to trade (from the pattern engine).
This engine tells you WHEN to enter — and whether to enter at all.

With only 1D daily OHLCV available, we use proxy analysis:
  - Gap vs prior close          → open sentiment
  - Day-close vs day-open       → intraday direction confirmation
  - Day's low / high placement  → pullback / breakout structure
  - NIFTY context               → index headwind / tailwind
  - Stock Relative Strength     → stock vs NIFTY, overrides index veto

Key design principles (v2):
  1. NO_TRADE_VOLATILE is converted to DELAYED entry — data shows 45% win
     rate on volatile open days, those wins should not be abandoned.
  2. BIG_GAP_UP that continued (no pullback): allow delayed entry if the
     day's momentum confirmed direction (50% win rate in backtest).
  3. NIFTY_WEAK veto is overridden when stock Relative Strength vs NIFTY
     exceeds RS_OVERRIDE_THRESHOLD — stock-specific momentum matters more.
  4. RECLAIM entries use prev_close as the anchor price, not open + fraction
     — a stock "reclaiming" prior close should be entered near that level.

Entry price is estimated for each window using proportion of the day's range.
These are proxies, not exact intraday prices — the direction of improvement
is reliable even if the exact price has ±0.3% noise.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

# ── Thresholds (all tunable) ──────────────────────────────────────────────────

GAP_BIG_UP    =  3.0   # % — gap too large for blind entry (long)
GAP_SMALL_UP  =  0.5   # %
GAP_FLAT_BAND =  0.5   # ±% — considered flat
GAP_SMALL_DN  = -0.5   # %
GAP_BIG_DN    = -2.0   # % — significant downside gap

DAY_BULLISH   =  0.5   # % close-vs-open confirming upward day
DAY_BEARISH   = -0.5   # % confirming downward day
DAY_STRONG_DN = -1.5   # % strongly bearish day

NIFTY_WEAK    = -1.0   # % NIFTY day move below this = weak index
NIFTY_STRONG  =  0.5   # % NIFTY above this = supportive

# Relative strength override: if stock outperforms NIFTY by this much
# on the entry day, override the NIFTY_WEAK index veto.
RS_OVERRIDE_THRESHOLD = 1.5   # %

# Volatile day threshold — above this range, shift to delayed entry (not skip)
VOLATILE_RANGE    = 8.0    # % day range triggers volatility handling
VERY_VOLATILE_RANGE = 12.0 # % above this, genuine skip

# Fraction of day's move used to estimate delayed entry prices
# (entry_day_open + frac × (entry_day_close - entry_day_open))
DELAY_FRAC = {
    "9:30":  0.15,
    "10:00": 0.30,
    "11:00": 0.55,
}

# For pullback entries: fraction of down-move from open to low
PULLBACK_FRAC  = 0.45   # proxy for "entered near pullback lows"
# For reclaim entries: fraction of day move from open
RECLAIM_FRAC_10 = 0.40
RECLAIM_FRAC_11 = 0.60


# ── Decision codes ────────────────────────────────────────────────────────────

class Exec:
    EARLY_ENTRY          = "EARLY_ENTRY"            # Clean open, enter at 9:15
    DELAYED_9_30         = "DELAYED_9_30"           # Small gap/wait, enter ~9:30
    DELAYED_10_00        = "DELAYED_10_00"          # Waited for direction clarity
    DELAYED_11_00        = "DELAYED_11_00"          # Late confirmation
    PULLBACK_ENTRY       = "PULLBACK_ENTRY"         # Gap-up → wait for pullback
    RECLAIM_ENTRY_10     = "RECLAIM_ENTRY_10"       # Gap-down → reclaimed by 10:00
    RECLAIM_ENTRY_11     = "RECLAIM_ENTRY_11"       # Gap-down → reclaimed by 11:00
    NO_TRADE_GAP_CHASE   = "NO_TRADE_GAP_CHASE"    # Gap-up, no pullback, skip
    NO_TRADE_WEAK_OPEN   = "NO_TRADE_WEAK_OPEN"    # Opened/closed weak, skip
    NO_TRADE_INDEX_WEAK  = "NO_TRADE_INDEX_WEAK"   # NIFTY context hostile
    NO_TRADE_VOLATILE    = "NO_TRADE_VOLATILE"      # Day range too wild for entry

NO_TRADE_CODES = {
    Exec.NO_TRADE_GAP_CHASE,
    Exec.NO_TRADE_WEAK_OPEN,
    Exec.NO_TRADE_INDEX_WEAK,
    Exec.NO_TRADE_VOLATILE,
}

# Human-readable labels
EXEC_LABELS = {
    Exec.EARLY_ENTRY:         "Early Entry (9:15)",
    Exec.DELAYED_9_30:        "Delayed Entry (~9:30)",
    Exec.DELAYED_10_00:       "Delayed Entry (~10:00)",
    Exec.DELAYED_11_00:       "Delayed Entry (~11:00)",
    Exec.PULLBACK_ENTRY:      "Pullback Entry",
    Exec.RECLAIM_ENTRY_10:    "Reclaim Entry (~10:00)",
    Exec.RECLAIM_ENTRY_11:    "Reclaim Entry (~11:00)",
    Exec.NO_TRADE_GAP_CHASE:  "No Trade — Gap Chase",
    Exec.NO_TRADE_WEAK_OPEN:  "No Trade — Weak Open",
    Exec.NO_TRADE_INDEX_WEAK: "No Trade — Index Weak",
    Exec.NO_TRADE_VOLATILE:   "No Trade — Too Volatile",
}

EXEC_GROUPS = {
    "entry": {Exec.EARLY_ENTRY, Exec.DELAYED_9_30, Exec.DELAYED_10_00,
              Exec.DELAYED_11_00, Exec.PULLBACK_ENTRY,
              Exec.RECLAIM_ENTRY_10, Exec.RECLAIM_ENTRY_11},
    "no_trade": NO_TRADE_CODES,
}


@dataclass
class ExecResult:
    exec_code:        str
    trade_taken:      bool
    entry_price:      Optional[float]   # None = no trade
    entry_window:     Optional[str]     # "9:15", "~9:30", etc.
    notes:            str
    # Raw inputs
    gap_pct:          float
    gap_category:     str
    day_move_pct:     float
    day_range_pct:    float
    nifty_day_move:   Optional[float]
    nifty_is_weak:    bool
    rs_vs_nifty:      Optional[float] = None   # stock day_move - nifty day_move


def _gap_category(gap: float) -> str:
    if   gap >  GAP_BIG_UP:    return "BIG_GAP_UP"
    elif gap >  GAP_FLAT_BAND: return "GAP_UP"
    elif gap >= GAP_SMALL_DN:  return "FLAT"
    elif gap >= GAP_BIG_DN:    return "GAP_DOWN"
    else:                       return "BIG_GAP_DOWN"


def _est_entry(open_: float, close: float, low: float, high: float,
               exec_code: str, prev_close: float = 0.0) -> float:
    """
    Estimate entry price for a given execution code using day's OHLC proportions.

    Reclaim entries use prev_close as the anchor: a stock 'reclaiming' its prior
    close should be entered at or just above that level, not higher in the day's range.
    """
    if exec_code == Exec.EARLY_ENTRY:
        return round(open_, 2)
    if exec_code == Exec.DELAYED_9_30:
        return round(open_ + DELAY_FRAC["9:30"]  * (close - open_), 2)
    if exec_code == Exec.DELAYED_10_00:
        return round(open_ + DELAY_FRAC["10:00"] * (close - open_), 2)
    if exec_code == Exec.DELAYED_11_00:
        return round(open_ + DELAY_FRAC["11:00"] * (close - open_), 2)
    if exec_code == Exec.PULLBACK_ENTRY:
        # Entered near intraday low — midpoint of open-to-low range
        return round(open_ + PULLBACK_FRAC * (low - open_), 2)
    if exec_code == Exec.RECLAIM_ENTRY_10:
        # Entry at prior close + small buffer (1/4 of open-to-close move)
        # Stock has just reclaimed this level around 10:00
        anchor = prev_close if prev_close > 0 else open_
        buf    = abs(close - open_) * 0.10  # 10% of day's move as buffer
        return round(anchor + buf, 2)
    if exec_code == Exec.RECLAIM_ENTRY_11:
        anchor = prev_close if prev_close > 0 else open_
        buf    = abs(close - open_) * 0.15
        return round(anchor + buf, 2)
    return round(open_, 2)


def analyze(
    direction:      str,
    prev_close:     float,
    entry_open:     float,
    entry_high:     float,
    entry_low:      float,
    entry_close:    float,
    nifty_open:     Optional[float] = None,
    nifty_close:    Optional[float] = None,
) -> ExecResult:
    """
    Core execution decision for a single signal.

    Inputs are all daily OHLCV values:
      prev_close  — signal day's closing price (what the pattern saw)
      entry_*     — next trading day's OHLCV (the execution canvas)
      nifty_*     — NIFTY 50 same day open/close (index context, optional)
    """
    is_long = direction != "short"

    if prev_close <= 0 or entry_open <= 0:
        return ExecResult(
            exec_code=Exec.EARLY_ENTRY, trade_taken=True,
            entry_price=round(entry_open, 2), entry_window="9:15",
            notes="Insufficient price data — blind entry",
            gap_pct=0.0, gap_category="FLAT",
            day_move_pct=0.0, day_range_pct=0.0,
            nifty_day_move=None, nifty_is_weak=False, rs_vs_nifty=None,
        )

    gap_pct      = (entry_open  - prev_close) / prev_close   * 100
    day_move_pct = (entry_close - entry_open) / entry_open   * 100
    day_range_pct = (entry_high - entry_low)  / entry_open   * 100
    gap_cat      = _gap_category(gap_pct)

    # NIFTY context
    nifty_move: Optional[float] = None
    nifty_weak  = False
    nifty_note  = ""
    if nifty_open and nifty_close and nifty_open > 0:
        nifty_move = (nifty_close - nifty_open) / nifty_open * 100
        nifty_weak = nifty_move < NIFTY_WEAK
        nifty_note = f" · NIFTY {nifty_move:+.1f}%"

    # ── Relative Strength vs NIFTY ────────────────────────────────────────────
    # If the stock outperforms NIFTY by RS_OVERRIDE_THRESHOLD on the entry day,
    # its own momentum overrides an index-weak veto.
    rs_vs_nifty: Optional[float] = None
    rs_overrides_index = False
    if nifty_move is not None:
        rs_vs_nifty = day_move_pct - nifty_move
        if rs_vs_nifty > RS_OVERRIDE_THRESHOLD:
            rs_overrides_index = True
            nifty_note += f" · RS+{rs_vs_nifty:.1f}% vs NIFTY (override)"

    # ─────────────────────────────────────────────────────────────────────────
    # Volatility handling (v2):
    #   Very volatile (>12%): skip — market structure is broken
    #   Moderate volatile (8-12%): shift to delayed entry, wider window
    # Data shows 45% win rate on 8-12% range days — do not skip blindly.
    # ─────────────────────────────────────────────────────────────────────────
    if day_range_pct > VERY_VOLATILE_RANGE:
        return ExecResult(
            exec_code=Exec.NO_TRADE_VOLATILE, trade_taken=False,
            entry_price=None, entry_window=None,
            notes=f"Day range {day_range_pct:.1f}% — extreme volatility, skip",
            gap_pct=gap_pct, gap_category=gap_cat,
            day_move_pct=day_move_pct, day_range_pct=day_range_pct,
            nifty_day_move=nifty_move, nifty_is_weak=nifty_weak, rs_vs_nifty=None,
        )
    volatile_day = day_range_pct > VOLATILE_RANGE  # 8-12%: enter but later

    def result(code, notes_extra="", **kw) -> ExecResult:
        # On moderate volatile days, don't use early entry — shift one window later
        effective_code = code
        if volatile_day and code == Exec.EARLY_ENTRY:
            effective_code = Exec.DELAYED_9_30
        elif volatile_day and code == Exec.DELAYED_9_30:
            effective_code = Exec.DELAYED_10_00

        taken = effective_code not in NO_TRADE_CODES
        ep = _est_entry(entry_open, entry_close, entry_low, entry_high,
                        effective_code, prev_close) if taken else None
        win = {
            Exec.EARLY_ENTRY: "9:15", Exec.DELAYED_9_30: "~9:30",
            Exec.DELAYED_10_00: "~10:00", Exec.DELAYED_11_00: "~11:00",
            Exec.PULLBACK_ENTRY: "~10:00 (pullback)", Exec.RECLAIM_ENTRY_10: "~10:00 (reclaim)",
            Exec.RECLAIM_ENTRY_11: "~11:00 (reclaim)",
        }
        vol_note = f" [volatile {day_range_pct:.0f}% range]" if volatile_day else ""
        return ExecResult(
            exec_code=effective_code, trade_taken=taken, entry_price=ep,
            entry_window=win.get(effective_code) if taken else None,
            notes=EXEC_LABELS.get(effective_code, effective_code)
                  + ((" — " + notes_extra) if notes_extra else "")
                  + vol_note + nifty_note,
            gap_pct=gap_pct, gap_category=gap_cat,
            day_move_pct=day_move_pct, day_range_pct=day_range_pct,
            nifty_day_move=nifty_move, nifty_is_weak=nifty_weak,
            rs_vs_nifty=rs_vs_nifty,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # LONG SIGNAL LOGIC
    # ─────────────────────────────────────────────────────────────────────────
    if is_long:

        if gap_cat == "BIG_GAP_UP":
            # Stock opened significantly higher — don't blindly chase
            if day_move_pct < -0.5:
                # Opened high, pulled back — valid pullback entry near intraday low
                return result(Exec.PULLBACK_ENTRY,
                              f"Gap +{gap_pct:.1f}%, intraday pullback {day_move_pct:.1f}%")
            elif day_move_pct > DAY_BULLISH:
                # Opened high AND continued higher — wait for first consolidation.
                # Data shows 50% win rate in these cases (v2: delayed not skip).
                return result(Exec.DELAYED_10_00,
                              f"Gap +{gap_pct:.1f}% continued, wait for 10:00 consolidation")
            else:
                # Opened high, flat/neutral day — indeterminate, skip
                return result(Exec.NO_TRADE_GAP_CHASE,
                              f"Gap +{gap_pct:.1f}%, neutral follow-through — skip")

        elif gap_cat == "GAP_UP":
            # Mild gap-up — manageable, needs confirmation
            if nifty_weak and not rs_overrides_index:
                if day_move_pct > DAY_BULLISH:
                    return result(Exec.DELAYED_9_30,
                                  f"Gap +{gap_pct:.1f}%, NIFTY weak but stock held")
                return result(Exec.NO_TRADE_INDEX_WEAK,
                              f"Gap +{gap_pct:.1f}% + NIFTY weak + stock soft")

            if day_move_pct > DAY_BULLISH:
                return result(Exec.EARLY_ENTRY,
                              f"Gap +{gap_pct:.1f}%, bullish day confirmed")
            elif day_move_pct < DAY_BEARISH:
                return result(Exec.NO_TRADE_WEAK_OPEN,
                              f"Gap +{gap_pct:.1f}% faded — bearish day")
            else:
                return result(Exec.DELAYED_9_30,
                              f"Gap +{gap_pct:.1f}%, neutral day — wait for direction")

        elif gap_cat == "FLAT":
            # Best scenario — stock opened near yesterday's close
            if nifty_weak and not rs_overrides_index and day_move_pct < DAY_BEARISH:
                return result(Exec.NO_TRADE_INDEX_WEAK,
                              f"Flat open, NIFTY weak, stock soft")

            if day_move_pct > DAY_BULLISH:
                return result(Exec.EARLY_ENTRY,
                              f"Flat open, bullish day — cleanest entry")
            elif day_move_pct < DAY_STRONG_DN:
                return result(Exec.NO_TRADE_WEAK_OPEN,
                              f"Flat open but strongly bearish day — signal invalidated")
            else:
                return result(Exec.DELAYED_9_30,
                              f"Flat open, mild-to-neutral day — wait for 9:30")

        elif gap_cat == "GAP_DOWN":
            # Stock opened weaker — signal in trouble but may recover
            if nifty_weak and not rs_overrides_index:
                return result(Exec.NO_TRADE_INDEX_WEAK,
                              f"Gap {gap_pct:.1f}% + NIFTY weak — double headwind")

            recovery_pct = day_move_pct / abs(gap_pct) * 100 if abs(gap_pct) > 0.1 else 0

            if day_move_pct > 1.5 and recovery_pct > 70:
                return result(Exec.RECLAIM_ENTRY_10,
                              f"Gap {gap_pct:.1f}%, strong recovery +{day_move_pct:.1f}%")
            elif day_move_pct > 0.5:
                return result(Exec.DELAYED_10_00,
                              f"Gap {gap_pct:.1f}%, partial recovery +{day_move_pct:.1f}%")
            else:
                return result(Exec.NO_TRADE_WEAK_OPEN,
                              f"Gap {gap_pct:.1f}%, failed to recover — stay out")

        else:  # BIG_GAP_DOWN
            if nifty_weak and not rs_overrides_index:
                return result(Exec.NO_TRADE_INDEX_WEAK,
                              f"Large gap {gap_pct:.1f}% + NIFTY weak — skip")

            v_reversal = entry_close > prev_close
            if v_reversal and day_move_pct > 2.5:
                return result(Exec.RECLAIM_ENTRY_11,
                              f"V-reversal: gap {gap_pct:.1f}%, closed above prior close")
            elif day_move_pct > 1.5:
                return result(Exec.DELAYED_11_00,
                              f"Large gap {gap_pct:.1f}%, partial recovery — late entry")
            else:
                return result(Exec.NO_TRADE_WEAK_OPEN,
                              f"Large gap {gap_pct:.1f}%, no meaningful recovery")

    # ─────────────────────────────────────────────────────────────────────────
    # SHORT SIGNAL LOGIC (mirror of long, with RS override)
    # ─────────────────────────────────────────────────────────────────────────
    else:
        # For shorts, relative strength vs NIFTY works in reverse:
        # if stock is sharply underperforming NIFTY, that confirms the short
        rs_confirms_short = (rs_vs_nifty is not None and rs_vs_nifty < -RS_OVERRIDE_THRESHOLD)

        if gap_cat == "BIG_GAP_DOWN":
            # Opened significantly lower — don't chase
            if day_move_pct > 0.5:
                # Bounced after gap — short entry on bounce top
                return result(Exec.PULLBACK_ENTRY,
                              f"Gap {gap_pct:.1f}%, intraday bounce {day_move_pct:.1f}%")
            elif day_move_pct < DAY_BEARISH:
                # Continued lower — still valid at 10:00 window
                return result(Exec.DELAYED_10_00,
                              f"Gap {gap_pct:.1f}%, continued lower — delayed short entry")
            else:
                return result(Exec.NO_TRADE_GAP_CHASE,
                              f"Gap {gap_pct:.1f}% + flat follow-through — indeterminate")

        elif gap_cat == "GAP_DOWN":
            # Gap down favors short — NIFTY weakness confirms
            if nifty_weak or rs_confirms_short:
                if day_move_pct < DAY_BEARISH:
                    return result(Exec.EARLY_ENTRY,
                                  f"Gap {gap_pct:.1f}%, index/RS weak + stock bearish — confirmed")
                return result(Exec.DELAYED_9_30,
                              f"Gap {gap_pct:.1f}%, weak context but stock bouncing")

            if day_move_pct < DAY_BEARISH:
                return result(Exec.EARLY_ENTRY,
                              f"Gap {gap_pct:.1f}%, bearish day confirmed")
            elif day_move_pct > DAY_BULLISH:
                return result(Exec.NO_TRADE_WEAK_OPEN,
                              f"Gap {gap_pct:.1f}% reversed to bullish — signal failed")
            else:
                return result(Exec.DELAYED_9_30,
                              f"Gap {gap_pct:.1f}%, neutral day — wait for direction")

        elif gap_cat == "FLAT":
            # Short needs bearish confirmation — NIFTY strong + stock explicitly bullish = headwind.
            # Threshold is DAY_STRONG_DN * -1 = 1.5%: mild up days (+0 to +1.5%) are not enough
            # to invalidate a short signal. Only strong reversal days (>+1.5%) get skipped.
            index_supportive = (not nifty_weak) and (not rs_confirms_short)
            if index_supportive and day_move_pct > DAY_STRONG_DN * -1:
                return result(Exec.NO_TRADE_INDEX_WEAK,
                              f"Flat open, index supportive + strong bullish day — short not confirmed")
            if day_move_pct < DAY_BEARISH:
                return result(Exec.EARLY_ENTRY, "Flat open, bearish day — clean short entry")
            elif day_move_pct > DAY_STRONG_DN * -1:
                return result(Exec.NO_TRADE_WEAK_OPEN, "Flat open but strong bullish reversal")
            else:
                return result(Exec.DELAYED_9_30, "Flat open, mild weakness — wait for 9:30")

        elif gap_cat == "GAP_UP":
            # Gap up against short signal
            index_strong = (not nifty_weak) and (not rs_confirms_short)
            if index_strong and day_move_pct > 0:
                return result(Exec.NO_TRADE_INDEX_WEAK,
                              f"Gap +{gap_pct:.1f}% against short + index strong")
            recovery_pct = (-day_move_pct) / abs(gap_pct) * 100 if abs(gap_pct) > 0.1 else 0
            if day_move_pct < -1.5 and recovery_pct > 70:
                return result(Exec.RECLAIM_ENTRY_10,
                              f"Gap +{gap_pct:.1f}%, reversed {day_move_pct:.1f}% — short reclaim")
            elif day_move_pct < -0.5:
                return result(Exec.DELAYED_10_00,
                              f"Gap +{gap_pct:.1f}%, fading — delayed short entry")
            else:
                return result(Exec.NO_TRADE_WEAK_OPEN,
                              f"Gap +{gap_pct:.1f}% held — short not confirmed")

        else:  # BIG_GAP_UP
            index_strong = (not nifty_weak) and (not rs_confirms_short)
            if index_strong and day_move_pct > 0:
                return result(Exec.NO_TRADE_GAP_CHASE,
                              f"Large gap +{gap_pct:.1f}% + continued — skip short")
            if day_move_pct < -2.5:
                return result(Exec.RECLAIM_ENTRY_11,
                              f"Large gap +{gap_pct:.1f}%, strong reversal — late short entry")
            elif day_move_pct < -1.0:
                return result(Exec.DELAYED_11_00,
                              f"Large gap +{gap_pct:.1f}%, fading — very late short entry")
            else:
                return result(Exec.NO_TRADE_INDEX_WEAK,
                              f"Large gap +{gap_pct:.1f}%, no confirmation — skip")
