"""
KANIDA Signals — Live Detection Emitter
========================================
Bridges the live agent (custom_agent.run) and the learning pipeline.

When a live scan fires N strategies on a stock, this module:
  1. Inserts one signal_events row per firing strategy (source='live_custom_agent')
  2. Looks up trend_state / trend_strength from stock_trend_state (latest)
  3. Calls paper_trades.log_if_active() with the new signal_event_id
     → roster-gated: writes paper_trades ONLY if signal_roster.status='active'
        AND the roster trend_gate allows the current trend_state.

Before this module existed, the live agent wrote to a local paper_ledger.json
with its own STRONG/DEVELOPING gate, and never touched signal_events or
paper_trades — leaving the learning and action pipelines disconnected.

Returns per-call counts so callers can log what happened.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Iterable, Optional, Tuple

from .db import get_conn
from .paper_trades import log_paper_trade


# ──────────────────────────────────────────────────────────────────
# TREND STATE LOOKUP
# ──────────────────────────────────────────────────────────────────

def _latest_trend_state(
    ticker: str,
    market: str,
    conn: sqlite3.Connection,
    as_of_date: Optional[str] = None,
) -> Tuple[Optional[str], Optional[float]]:
    """Return (trend_state, trend_strength) for the latest row ≤ as_of_date.
    Returns (None, None) if no row exists."""
    if as_of_date:
        row = conn.execute(
            """SELECT trend_state, trend_strength FROM stock_trend_state
               WHERE ticker=? AND market=? AND trade_date<=?
               ORDER BY trade_date DESC LIMIT 1""",
            (ticker, market, as_of_date),
        ).fetchone()
    else:
        row = conn.execute(
            """SELECT trend_state, trend_strength FROM stock_trend_state
               WHERE ticker=? AND market=?
               ORDER BY trade_date DESC LIMIT 1""",
            (ticker, market),
        ).fetchone()
    if not row:
        return (None, None)
    return (row[0], row[1])


# ──────────────────────────────────────────────────────────────────
# EMIT LIVE SIGNALS
# ──────────────────────────────────────────────────────────────────

def emit_live_signals(
    ticker: str,
    market: str,
    timeframe: str,
    bias: str,
    firing_strategy_names: Iterable[str],
    signal_date: str,
    entry_price: float,
    conn: Optional[sqlite3.Connection] = None,
) -> dict:
    """
    Emit one signal_events row per firing strategy, then roster-gate each
    through paper_trades.log_if_active().

    Parameters
    ----------
    ticker, market, timeframe, bias : identifiers matching signal_roster
        primary key shape.
    firing_strategy_names : iterable of strategy_name strings currently firing
        at the live bar (from custom_agent.LiveScore.firing_strategies).
    signal_date : 'YYYY-MM-DD' — typically the last closed-bar date.
    entry_price : live or last-close price at the signal bar.
    conn : optional open SQLite connection. If None, one is opened and closed
        inside this call.

    Returns
    -------
    dict with keys:
        events_inserted      — new signal_events rows (UNIQUE constraint = OR IGNORE)
        events_already_existed — duplicates skipped by UNIQUE
        paper_trades_logged  — rows written to paper_trades (roster active + gate ok)
        paper_trades_gated_out — firing strategies NOT on active roster or blocked
                                  by trend_gate.
        strategies_total     — len(firing_strategy_names)
    """
    own_conn = False
    if conn is None:
        conn = get_conn()
        own_conn = True

    try:
        trend_state, trend_strength = _latest_trend_state(
            ticker, market, conn, as_of_date=signal_date
        )

        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        counts = {
            "events_inserted":        0,
            "events_already_existed": 0,
            "paper_trades_logged":    0,
            "paper_trades_gated_out": 0,
            "paper_trades_active":    0,
            "paper_trades_watchlist": 0,
            "paper_trades_test":      0,
            "strategies_total":       0,
        }

        firing_list = list(firing_strategy_names)
        counts["strategies_total"] = len(firing_list)
        if not firing_list:
            return counts

        for strategy_name in firing_list:
            # 1. Insert signal_events (INSERT OR IGNORE on UNIQUE)
            with conn:
                cur = conn.execute(
                    """INSERT OR IGNORE INTO signal_events
                       (ticker, market, timeframe, signal_date, strategy_name, bias,
                        entry_price, trend_state, trend_strength, source, detected_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (ticker, market, timeframe, signal_date, strategy_name, bias,
                     float(entry_price), trend_state, trend_strength,
                     "live_custom_agent", now_str),
                )
                inserted = cur.rowcount > 0

            # Resolve signal_event_id regardless of insert vs already-existed
            row = conn.execute(
                """SELECT id FROM signal_events
                   WHERE ticker=? AND market=? AND timeframe=?
                     AND signal_date=? AND strategy_name=? AND bias=?""",
                (ticker, market, timeframe, signal_date, strategy_name, bias),
            ).fetchone()
            if not row:
                # Should be impossible if insert succeeded or row already existed,
                # but guard against it.
                continue
            signal_event_id = row[0]

            if inserted:
                counts["events_inserted"] += 1
            else:
                counts["events_already_existed"] += 1

            # 2. Roster-gated paper trade log (tier-aware: Path 2)
            tier = log_paper_trade(
                signal_event_id=signal_event_id,
                ticker=ticker,
                market=market,
                timeframe=timeframe,
                strategy_name=strategy_name,
                bias=bias,
                entry_date=signal_date,
                entry_price=float(entry_price),
                trend_state=trend_state,
                trend_strength=trend_strength,
                conn=conn,
            )
            if tier is not None:
                counts["paper_trades_logged"] += 1
                if tier == "active":
                    counts["paper_trades_active"] += 1
                elif tier == "watchlist":
                    counts["paper_trades_watchlist"] += 1
                elif tier == "test":
                    counts["paper_trades_test"] += 1
            else:
                counts["paper_trades_gated_out"] += 1

        return counts

    finally:
        if own_conn:
            conn.close()
