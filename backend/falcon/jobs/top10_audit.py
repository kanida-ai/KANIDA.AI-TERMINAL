"""Falcon Top 10 live-audit trail — populate + close-job (F4, 2026-05-24).

Internal-only audit table. Records every Top 10 pick the system actually
emitted live, then walks each position forward through the SAME exit
logic the persona simulator uses (init_stop -7%, +12% trail trigger,
10-day Donchian trail, 7-day time stop) so the live and backtest
exit-reason mixes can be compared apples-to-apples for drift detection.

NO user-visible side effects. The /power/today card still reads from
the persona-simulator backtest (one unified track record per the
2026-05-23 spec). This table is for ops monitoring + regulatory audit.

This module is bolted onto the V7 pipeline as the 4th step. Failures
here MUST NOT halt the chain — daily_signals success is what users
need; audit is a separate concern. The pipeline orchestrator wraps
top10_audit.run() in a try/except for that reason.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from ..db import falcon_conn

log = logging.getLogger("kanida.falcon.jobs.top10_audit")
IST = timezone(timedelta(hours=5, minutes=30))

# Match the persona simulator's cost model so live audit is apples-to-apples
# with backtest trades. (Constants mirrored from persona_engine_core.py:
# SLIP_BPS=5.0 → SLIP=0.0005; COST_BPS=30.0 → FEE=0.0030.)
SLIP = 0.0005
FEE  = 0.0030


# ════════════════════════════════════════════════════════════════════════
# Step 1 — populate today's Top 10 into the audit table
# ════════════════════════════════════════════════════════════════════════

def _populate_today(con: sqlite3.Connection) -> int:
    """INSERT the user-facing Top 10 for the most recent signal_date.

    Selection mirrors `_fetch_raw_picks` in the explainer EXACTLY:
      - n_fires >= min_fires (from PERSONA_CONFIGS, enforced by F1)
      - ORDER BY (score / n_fires) DESC
      - LIMIT 10

    Idempotent: INSERT OR IGNORE on (signal_date, symbol) primary key.
    Re-running for the same day skips already-recorded rows.

    Returns: number of NEW rows inserted (0 if the day is already populated).
    """
    # min_fires from the locked persona — same lazy-import pattern as F1.
    try:
        from power_user.services.persona_simulator import PERSONA_CONFIGS
        min_fires = int(PERSONA_CONFIGS["falcon-top-10"]["run_cfg"].min_fires)
    except Exception as e:
        log.warning("populate: PERSONA_CONFIGS unreachable (%s) — falling back to min_fires=10", e)
        min_fires = 10

    latest = con.execute(
        "SELECT MAX(signal_date) FROM falcon_signals_live"
    ).fetchone()
    signal_date = latest[0] if latest else None
    if not signal_date:
        log.info("populate: falcon_signals_live empty, nothing to record")
        return 0

    # Pull entry_date from the signal row itself (authoritative) — the
    # engine writes it at signal-emission time, accounting for holidays.
    rows = con.execute("""
        SELECT signal_date, entry_date, symbol, sector, n_fires, score, close_at_signal
        FROM falcon_signals_live
        WHERE signal_date = ?
          AND n_fires >= ?
        ORDER BY (score * 1.0 / n_fires) DESC
        LIMIT 10
    """, (signal_date, min_fires)).fetchall()

    if not rows:
        log.warning("populate: no rows passed min_fires=%d on %s", min_fires, signal_date)
        return 0

    inserted = 0
    for rank_pos, r in enumerate(rows, start=1):
        result = con.execute("""
            INSERT OR IGNORE INTO falcon_top10_audit
                (signal_date, entry_date, rank, symbol, sector,
                 avg_lift, n_fires, close_at_signal)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            r["signal_date"], r["entry_date"], rank_pos, r["symbol"], r["sector"],
            float(r["score"]) / max(r["n_fires"], 1),
            int(r["n_fires"]),
            float(r["close_at_signal"]) if r["close_at_signal"] is not None else None,
        ))
        inserted += result.rowcount
    con.commit()
    log.info("populate: signal_date=%s rank-set=%d new_rows=%d (rest were already recorded)",
              signal_date, len(rows), inserted)
    return inserted


# ════════════════════════════════════════════════════════════════════════
# Step 2 — close due positions (entry_date + hold_days has passed)
# ════════════════════════════════════════════════════════════════════════

def _due_positions(con: sqlite3.Connection, hold_days: int) -> List[sqlite3.Row]:
    """Return open positions whose hold horizon has expired.

    "Expired" means: the last trading day of the hold window is <= today.
    We discover the exit boundary via ohlc_daily (handles holidays) rather
    than calendar-day arithmetic.

    Uses ohlc_daily as the trading-day source: for each open position,
    find the (hold_days)th trading day at or after entry_date and check
    if a bar exists for that date (= window has elapsed AND we have data
    to close on).
    """
    today_ist = datetime.now(IST).date().isoformat()
    open_rows = con.execute("""
        SELECT signal_date, entry_date, symbol, sector, close_at_signal
        FROM falcon_top10_audit
        WHERE exit_date IS NULL
        ORDER BY entry_date
    """).fetchall()
    due: List[sqlite3.Row] = []
    for r in open_rows:
        # Find the time_exit_date for this position: the (hold_days)th
        # trading day at or after entry_date.
        time_exit = con.execute("""
            SELECT trade_date FROM (
                SELECT DISTINCT trade_date FROM ohlc_daily
                 WHERE symbol = ? AND trade_date >= ?
                 ORDER BY trade_date ASC
                 LIMIT ?
            ) ORDER BY trade_date DESC LIMIT 1
        """, (r["symbol"], r["entry_date"], hold_days)).fetchone()
        if not time_exit:
            continue                          # not enough bars yet
        if time_exit[0] <= today_ist:
            due.append(r)
    return due


def _resolve_exit(
    con: sqlite3.Connection,
    symbol: str,
    entry_date: str,
    entry_px: float,
    init_stop_pct: float,           # -0.07
    trail_trigger_pct: float,        # 0.12
    trail_lookback: int,             # 10
    hold_days: int,                  # 7
) -> Optional[Dict[str, Any]]:
    """Walk one position day-by-day applying the simulator's exit logic.

    Mirrors `simulate_year` in power_user.services.persona_engine_core
    (lines ~384-438 as of 2026-05-24). Returns:
      {exit_date, exit_price, net_return_pct, exit_reason}  on resolution
      None  if there isn't enough OHLC data to fully walk the window

    Slippage + fees use the same SLIP=5bps / FEE=30bps constants as the
    simulator. The Falcon Top 10 persona uses target=None (no fixed
    profit target), trail=True, so the priority is:
        1. SL (init or trailed)  → INIT_STOP / TRAIL_GIVEBACK
        2. Time stop at hold_days → TIME_STOP
    (TARGET branch removed — never fires for this persona.)
    """
    bars = con.execute("""
        SELECT trade_date, open, high, low, close
        FROM ohlc_daily
        WHERE symbol = ? AND trade_date >= ?
        ORDER BY trade_date ASC
        LIMIT ?
    """, (symbol, entry_date, hold_days)).fetchall()

    if len(bars) < hold_days:
        # Window hasn't fully elapsed yet — should NOT happen for "due"
        # positions but defensive: don't claim an exit on incomplete data.
        log.debug("resolve_exit: %s has %d bars, need %d — skipping",
                   symbol, len(bars), hold_days)
        return None

    high_water = 0.0     # max cur_ret observed
    init_stop_lvl = entry_px * (1 + init_stop_pct)

    # The persona uses entry at the open of entry_date — for the audit we
    # use close_at_signal as entry_px (the proxy we display to users until
    # 09:15 IST live ticks land). The exit walk starts from entry_date's bar.
    for i, bar in enumerate(bars):
        bar_close = float(bar["close"])
        bar_open  = float(bar["open"])
        bar_high  = float(bar["high"])
        bar_low   = float(bar["low"])

        cur_ret = bar_close / entry_px - 1
        if cur_ret > high_water:
            high_water = cur_ret

        # Arm trail when high_water crosses the trigger
        armed = (trail_trigger_pct is not None) and (high_water >= trail_trigger_pct)
        stop_lvl = init_stop_lvl
        if armed:
            window = bars[max(0, i - trail_lookback + 1) : i + 1]
            trail_stop = max(entry_px, min(float(b["low"]) for b in window))
            stop_lvl = max(init_stop_lvl, trail_stop)

        exit_reason = None
        exit_px_raw = None
        # Priority: SL first (conservative), then time exit
        if bar_low <= stop_lvl:
            exit_reason = "TRAIL_GIVEBACK" if armed else "INIT_STOP"
            exit_px_raw = min(stop_lvl, bar_open)
        elif i == hold_days - 1:                # last bar of the hold window
            exit_reason = "TIME_STOP"
            exit_px_raw = bar_close

        if exit_reason:
            exit_px = exit_px_raw * (1 - SLIP)
            # Net return after fees + slippage on both sides
            # (entry was assumed at entry_px without slip — matches simulator).
            gross_pct  = (exit_px / entry_px - 1) * 100.0
            fee_drag_pct = FEE * 100.0           # 0.30%, applied once on round-trip
            net_pct = gross_pct - fee_drag_pct
            return {
                "exit_date":      bar["trade_date"],
                "exit_price":     round(exit_px, 2),
                "net_return_pct": round(net_pct, 2),
                "exit_reason":    exit_reason,
            }
    # Shouldn't reach here — time stop always fires on i == hold_days-1
    return None


def _close_due(con: sqlite3.Connection) -> int:
    """Resolve every open position whose hold horizon expired. UPDATE the row."""
    # Read persona-locked rules — same source of truth as the explainer's
    # _locked_rules(). Direct PERSONA_CONFIGS lookup here so we don't have
    # to import the explainer (avoids unnecessary heavy module imports).
    try:
        from power_user.services.persona_simulator import PERSONA_CONFIGS
        cfg = PERSONA_CONFIGS["falcon-top-10"]["run_cfg"]
        init_stop      = float(cfg.init_stop)
        trail_trigger  = float(cfg.trail_trigger) if cfg.trail_trigger else None
        trail_lookback = int(cfg.trail_lookback)
        hold_days      = int(cfg.hold_days)
    except Exception as e:
        log.warning("close_due: PERSONA_CONFIGS unreachable (%s) — using locked defaults", e)
        init_stop, trail_trigger, trail_lookback, hold_days = -0.07, 0.12, 10, 7

    due = _due_positions(con, hold_days)
    if not due:
        log.info("close_due: no positions due for closure")
        return 0

    n_closed = 0
    n_skipped = 0
    for r in due:
        entry_px = float(r["close_at_signal"]) if r["close_at_signal"] is not None else None
        if entry_px is None or entry_px <= 0:
            log.warning("close_due: skipping %s — no entry price",  r["symbol"])
            n_skipped += 1
            continue
        result = _resolve_exit(
            con,
            symbol=r["symbol"],
            entry_date=r["entry_date"],
            entry_px=entry_px,
            init_stop_pct=init_stop,
            trail_trigger_pct=trail_trigger,
            trail_lookback=trail_lookback,
            hold_days=hold_days,
        )
        if result is None:
            n_skipped += 1
            continue
        con.execute("""
            UPDATE falcon_top10_audit
               SET exit_date      = ?,
                   exit_price     = ?,
                   net_return_pct = ?,
                   exit_reason    = ?,
                   closed_at      = datetime('now')
             WHERE signal_date = ? AND symbol = ?
        """, (
            result["exit_date"], result["exit_price"], result["net_return_pct"],
            result["exit_reason"], r["signal_date"], r["symbol"],
        ))
        n_closed += 1
    con.commit()
    log.info("close_due: closed=%d skipped=%d (of %d due)", n_closed, n_skipped, len(due))
    return n_closed


# ════════════════════════════════════════════════════════════════════════
# Orchestrator — called by the V7 pipeline as step 4
# ════════════════════════════════════════════════════════════════════════

def run() -> Dict[str, Any]:
    """Pipeline-callable entry point. Populates today's picks + closes due ones.

    Returns a dict for the orchestrator's logger; never raises. Audit-table
    failure must NOT halt daily_signals success.
    """
    t0 = datetime.now(IST)
    try:
        with falcon_conn() as con:
            con.row_factory = sqlite3.Row
            n_inserted = _populate_today(con)
            n_closed   = _close_due(con)
        elapsed = (datetime.now(IST) - t0).total_seconds()
        return {
            "status":         "success",
            "n_inserted":     n_inserted,
            "n_closed":       n_closed,
            "elapsed_sec":    round(elapsed, 2),
        }
    except Exception as e:
        log.exception("top10_audit.run failed (non-fatal): %s", e)
        return {
            "status":  "failed",
            "error":   str(e)[:300],
        }
