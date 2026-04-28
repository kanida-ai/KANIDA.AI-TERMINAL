"""
KANIDA Signals — Paper Trade Logger and Tracker
================================================
Two responsibilities:

1. log_paper_trade(signal_event_id, ...)   (alias: log_if_active)
   Called when a live signal fires. Checks signal_roster. The paper trade
   is written at a tier-dependent size_multiplier:

       roster status  | roster_tier  | size_multiplier
       -------------- | ------------ | ---------------
       active         | 'active'     | 1.00
       watchlist      | 'watchlist'  | 0.25   ← Path 2: small-size participation
       test           | 'test'       | 0.10   ← probationary, minimal notional
       retired        | (gated out)  | —
       (not in roster)| (gated out)  | —

   Paper trades are an analytical view, not capital allocation. The tier
   column lets roster_feedback and downstream analytics segment realized
   returns by promotion state WITHOUT touching signal_roster itself.

2. update_open_trades(conn)
   Called daily after OHLC close. Checks all open paper_trades against
   today's ohlc_daily. Marks trades as hit_target_1, stopped_out, or expired.

Stop and target levels use simple ATR-based defaults if not provided
by the calling signal detection layer.
"""

import sqlite3
from datetime import datetime, date, timedelta, timezone
from typing import Optional

from .db import get_conn, SIGNALS_DB_PATH


# ── Trade lifecycle constants ──────────────────────────────────────
DEFAULT_STOP_PCT   = 0.05   # 5% below entry
DEFAULT_TARGET_1_PCT = 0.10  # 10% above entry (bullish)
DEFAULT_TARGET_2_PCT = 0.20  # 20% above entry
EXPIRY_DAYS        = 30     # expire open trades after 30 trading days

# ── Tier → size_multiplier (Path 2, 2-axis tier system) ───────────
# New tier taxonomy from signal_roster.tier:
#   core_active     → full size (top quality + high frequency)
#   high_conviction → 0.75      (top quality, lower frequency)
#   steady          → 0.40      (mid quality, high frequency)
#   emerging        → 0.15      (building track record)
#   experimental    → 0.05      (small-sample probes)
#   retired         → gated out
#
# Legacy status values still map cleanly when `tier` is NULL (pre-migration):
#   active → core_active, watchlist → steady, test → emerging.
TIER_SIZE_MULTIPLIERS: dict[str, float] = {
    "core_active":     1.00,
    "high_conviction": 0.75,
    "steady":          0.40,
    "emerging":        0.15,
    "experimental":    0.05,
}
# Statuses/tiers that do NOT log paper trades at any size.
TIER_GATED_OUT: set[str] = {"retired"}

# Legacy status → new tier fallback (used only if tier column is NULL)
LEGACY_STATUS_TO_TIER: dict[str, str] = {
    "active":    "core_active",
    "watchlist": "steady",
    "test":      "emerging",
    "retired":   "retired",
}


# ──────────────────────────────────────────────────────────────────
# ROSTER GATE CHECK
# ──────────────────────────────────────────────────────────────────

def is_index(ticker: str, market: str, conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT is_index FROM sector_mapping WHERE ticker=? AND market=?",
        (ticker, market),
    ).fetchone()
    return row is not None and row[0] == 1


def _lookup_roster(
    ticker: str,
    market: str,
    timeframe: str,
    strategy_name: str,
    bias: str,
    conn: sqlite3.Connection,
    current_trend_state: Optional[str] = None,
) -> Optional[str]:
    """
    Return the roster TIER (core_active | high_conviction | steady |
    emerging | experimental | retired) if the signal exists on the roster
    AND passes the trend gate; None otherwise.

    Prefers signal_roster.tier. Falls back to legacy `status` when tier is
    NULL (pre-migration rows).

    trend_gate=NULL  → no restriction.
    current_trend_state=None → gate not applied (lenient; we lack the info).
    """
    if is_index(ticker, market, conn):
        return None
    row = conn.execute(
        """SELECT tier, status, trend_gate FROM signal_roster
           WHERE ticker=? AND market=? AND timeframe=?
             AND strategy_name=? AND bias=?""",
        (ticker, market, timeframe, strategy_name, bias),
    ).fetchone()
    if row is None:
        return None

    tier, status, trend_gate = row[0], row[1], row[2]
    if trend_gate and current_trend_state:
        allowed = {s.strip() for s in trend_gate.split(",")}
        if current_trend_state not in allowed:
            return None

    if tier:
        return tier
    # Legacy fallback
    return LEGACY_STATUS_TO_TIER.get(status)


def is_active(
    ticker: str,
    market: str,
    timeframe: str,
    strategy_name: str,
    bias: str,
    conn: sqlite3.Connection,
    current_trend_state: Optional[str] = None,
) -> bool:
    """Back-compat: True iff roster tier is full-size executable
    (core_active or high_conviction) and trend gate passes."""
    tier = _lookup_roster(
        ticker, market, timeframe, strategy_name, bias, conn, current_trend_state
    )
    return tier in ("core_active", "high_conviction")


# ──────────────────────────────────────────────────────────────────
# LOG A PAPER TRADE  (tier-aware, Path 2)
# ──────────────────────────────────────────────────────────────────

def log_paper_trade(
    signal_event_id: int,
    ticker: str,
    market: str,
    timeframe: str,
    strategy_name: str,
    bias: str,
    entry_date: str,
    entry_price: float,
    trend_state: Optional[str],
    trend_strength: Optional[float],
    conn: sqlite3.Connection,
    stop_price: Optional[float] = None,
    target_1: Optional[float] = None,
    target_2: Optional[float] = None,
) -> Optional[str]:
    """
    Logs a paper trade at the tier-appropriate size_multiplier.

    Returns:
        'active' / 'watchlist' / 'test' — the tier the trade was logged under
        None — gated out (not on roster, retired, trend-gate blocked, or
               duplicate signal_event_id)
    """
    tier = _lookup_roster(
        ticker, market, timeframe, strategy_name, bias, conn, trend_state
    )
    if tier is None or tier in TIER_GATED_OUT:
        return None
    if tier not in TIER_SIZE_MULTIPLIERS:
        # Unknown tier — fail closed
        return None

    size_mult = TIER_SIZE_MULTIPLIERS[tier]

    # Already logged for this signal event?
    existing = conn.execute(
        "SELECT id FROM paper_trades WHERE signal_event_id=?",
        (signal_event_id,)
    ).fetchone()
    if existing:
        return None

    # Default levels if not provided
    if bias == "bullish":
        stop  = stop_price   or round(entry_price * (1 - DEFAULT_STOP_PCT), 4)
        tgt_1 = target_1     or round(entry_price * (1 + DEFAULT_TARGET_1_PCT), 4)
        tgt_2 = target_2     or round(entry_price * (1 + DEFAULT_TARGET_2_PCT), 4)
    else:  # bearish
        stop  = stop_price   or round(entry_price * (1 + DEFAULT_STOP_PCT), 4)
        tgt_1 = target_1     or round(entry_price * (1 - DEFAULT_TARGET_1_PCT), 4)
        tgt_2 = target_2     or round(entry_price * (1 - DEFAULT_TARGET_2_PCT), 4)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    with conn:
        conn.execute(
            """INSERT OR IGNORE INTO paper_trades (
                signal_event_id, ticker, market, timeframe,
                strategy_name, bias,
                trend_state_entry, trend_strength_entry,
                entry_date, entry_price, stop_price, target_1, target_2,
                status, roster_tier, size_multiplier, logged_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'open',?,?,?)""",
            (
                signal_event_id, ticker, market, timeframe,
                strategy_name, bias,
                trend_state, trend_strength,
                entry_date, entry_price, stop, tgt_1, tgt_2,
                tier, size_mult,
                now_str,
            ),
        )
    return tier


def log_if_active(
    signal_event_id: int,
    ticker: str,
    market: str,
    timeframe: str,
    strategy_name: str,
    bias: str,
    entry_date: str,
    entry_price: float,
    trend_state: Optional[str],
    trend_strength: Optional[float],
    conn: sqlite3.Connection,
    stop_price: Optional[float] = None,
    target_1: Optional[float] = None,
    target_2: Optional[float] = None,
) -> bool:
    """
    Back-compat wrapper. Returns True iff a paper trade was logged at ANY
    tier (active/watchlist/test). Callers that rely on the old bool contract
    keep working; new callers should use log_paper_trade() for the tier.
    """
    tier = log_paper_trade(
        signal_event_id=signal_event_id,
        ticker=ticker, market=market, timeframe=timeframe,
        strategy_name=strategy_name, bias=bias,
        entry_date=entry_date, entry_price=entry_price,
        trend_state=trend_state, trend_strength=trend_strength,
        conn=conn,
        stop_price=stop_price, target_1=target_1, target_2=target_2,
    )
    return tier is not None


# ──────────────────────────────────────────────────────────────────
# DAILY UPDATE — CHECK OPEN TRADES
# ──────────────────────────────────────────────────────────────────

def update_open_trades(
    conn: sqlite3.Connection,
    as_of_date: Optional[str] = None,
) -> dict:
    """
    For every open paper trade, check today's OHLC:
      - hit_target_1  : high (bullish) or low (bearish) reached target_1
      - stopped_out   : low (bullish) or high (bearish) breached stop
      - expired       : trade age > EXPIRY_DAYS
    Returns counts of each outcome.
    """
    today = as_of_date or date.today().isoformat()
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    open_trades = conn.execute(
        """SELECT id, ticker, market, bias, entry_date, entry_price,
                  stop_price, target_1, target_2
           FROM paper_trades WHERE status = 'open'"""
    ).fetchall()

    counts = {"hit_target_1": 0, "stopped_out": 0, "expired": 0, "still_open": 0}

    for trade in open_trades:
        tid, ticker, market, bias, entry_date, entry_price, stop, tgt1, tgt2 = trade

        # Check expiry first
        try:
            age_days = (date.fromisoformat(today) - date.fromisoformat(entry_date)).days
        except Exception:
            age_days = 0

        if age_days >= EXPIRY_DAYS:
            # Use today's close as exit
            close_row = conn.execute(
                "SELECT close FROM ohlc_daily WHERE ticker=? AND market=? AND trade_date<=? ORDER BY trade_date DESC LIMIT 1",
                (ticker, market, today)
            ).fetchone()
            exit_price = close_row[0] if close_row else entry_price
            outcome = _outcome_pct(entry_price, exit_price, bias)
            with conn:
                conn.execute(
                    """UPDATE paper_trades SET status='expired', exit_date=?,
                       exit_price=?, outcome_pct=?, win=?, days_held=?, last_checked_at=?
                       WHERE id=?""",
                    (today, exit_price, outcome,
                     1 if outcome > 0 else 0, age_days, now_str, tid)
                )
            counts["expired"] += 1
            continue

        # Check today's OHLC
        ohlc = conn.execute(
            "SELECT high, low, close FROM ohlc_daily WHERE ticker=? AND market=? AND trade_date=?",
            (ticker, market, today)
        ).fetchone()

        if not ohlc:
            counts["still_open"] += 1
            conn.execute("UPDATE paper_trades SET last_checked_at=? WHERE id=?", (now_str, tid))
            continue

        high, low, close = ohlc

        if bias == "bullish":
            if tgt1 and high >= tgt1:
                _close_trade(conn, tid, today, tgt1, entry_price, bias, age_days, "hit_target_1", now_str)
                counts["hit_target_1"] += 1
            elif stop and low <= stop:
                _close_trade(conn, tid, today, stop, entry_price, bias, age_days, "stopped_out", now_str)
                counts["stopped_out"] += 1
            else:
                conn.execute("UPDATE paper_trades SET last_checked_at=? WHERE id=?", (now_str, tid))
                counts["still_open"] += 1
        else:  # bearish
            if tgt1 and low <= tgt1:
                _close_trade(conn, tid, today, tgt1, entry_price, bias, age_days, "hit_target_1", now_str)
                counts["hit_target_1"] += 1
            elif stop and high >= stop:
                _close_trade(conn, tid, today, stop, entry_price, bias, age_days, "stopped_out", now_str)
                counts["stopped_out"] += 1
            else:
                conn.execute("UPDATE paper_trades SET last_checked_at=? WHERE id=?", (now_str, tid))
                counts["still_open"] += 1

    return counts


def _outcome_pct(entry: float, exit_p: float, bias: str) -> float:
    if entry == 0:
        return 0.0
    raw = (exit_p - entry) / entry * 100
    return round(raw if bias == "bullish" else -raw, 4)


def _close_trade(conn, tid, exit_date, exit_price, entry_price, bias, days, status, now_str):
    outcome = _outcome_pct(entry_price, exit_price, bias)
    win = 1 if status in ("hit_target_1", "hit_target_2") else 0
    with conn:
        conn.execute(
            """UPDATE paper_trades SET status=?, exit_date=?, exit_price=?,
               outcome_pct=?, win=?, days_held=?, last_checked_at=? WHERE id=?""",
            (status, exit_date, exit_price, outcome, win, days, now_str, tid)
        )
