"""End-of-day session flush.

Fires once after market close (15:30 IST) on weekdays. Performs:
  1. Final fill reconciliation (catches 15:25–15:30 fills the next 60s monitor
     poll would miss because by then the market is closed).
  2. Marks any order still in PLACING > 15 minutes as STALE_PLACING (operator
     should investigate; Kite may have accepted it but our HTTP retry dropped
     the response).
  3. Computes a daily P&L snapshot — counts of new entries, exits, partials,
     trail bumps, and realised P&L on positions that closed today.
  4. Logs a single SESSION_CLOSE event (severity=info) to the events feed so
     the operator sees the EOD summary in /falcon/positions trigger feed.

Idempotency: writes the date in `falcon_eod_runs`. If the row exists for today,
flush short-circuits.

Triggered from position_monitor._loop after every poll — cheap idempotency
check, fires the heavy work once.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ...db import falcon_conn

log = logging.getLogger("kanida.falcon.trade.eod")
IST = timezone(timedelta(hours=5, minutes=30))


def _now_ist() -> datetime:
    return datetime.now(IST)


def _ensure_eod_table() -> None:
    """Idempotent — creates the EOD-runs table on first call."""
    with falcon_conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS falcon_eod_runs (
                run_date    TEXT PRIMARY KEY,    -- YYYY-MM-DD IST
                ran_at      TEXT NOT NULL,
                summary_json TEXT NOT NULL
            )
        """)
        con.commit()


def _already_ran_today() -> bool:
    today_ist = _now_ist().date().isoformat()
    with falcon_conn() as con:
        row = con.execute("SELECT 1 FROM falcon_eod_runs WHERE run_date=?",
                          (today_ist,)).fetchone()
    return row is not None


def _is_after_close() -> bool:
    """True after 15:35 IST on weekdays. 5-minute grace window after 15:30 close
    so we capture any final fills that took a moment to settle."""
    n = _now_ist()
    if n.weekday() >= 5:
        return False
    if n.hour < 15 or (n.hour == 15 and n.minute < 35):
        return False
    return True


def maybe_run(kite) -> Optional[Dict[str, Any]]:
    """Run EOD flush iff (a) past 15:35 IST weekday and (b) not yet run today.
    Returns the summary dict on first-run, None otherwise."""
    _ensure_eod_table()
    if not _is_after_close():
        return None
    if _already_ran_today():
        return None

    log.info("EOD flush starting")
    summary = run_flush(kite)

    today_ist = _now_ist().date().isoformat()
    with falcon_conn() as con:
        con.execute("""
            INSERT OR REPLACE INTO falcon_eod_runs (run_date, ran_at, summary_json)
            VALUES (?,?,?)
        """, (today_ist, _now_ist().isoformat(), json.dumps(summary)))
        con.commit()

    log.info("EOD flush complete: %s", summary)
    return summary


def run_flush(kite) -> Dict[str, Any]:
    """Body of the EOD flush. Safe to call manually for testing."""
    summary: Dict[str, Any] = {"date": _now_ist().date().isoformat()}

    # 1. Final fill reconciliation
    try:
        from . import fill_reconciler  # noqa: WPS433
        summary["fill_reconcile"] = fill_reconciler.reconcile(kite)
    except Exception as e:
        log.exception("EOD: fill_reconciler failed: %s", e)
        summary["fill_reconcile"] = {"error": str(e)}

    # 2. Mark stale PLACING orders. Anything PLACING for more than 15 minutes is
    # abandoned — Kite may have it, our DB row is stuck.
    cutoff_iso = (_now_ist() - timedelta(minutes=15)).isoformat()
    with falcon_conn() as con:
        stale_rows = con.execute("""
            SELECT id, symbol, role, kite_order_id, qty
            FROM falcon_trade_orders
            WHERE status = 'PLACING' AND placed_at IS NOT NULL AND placed_at < ?
        """, (cutoff_iso,)).fetchall()
        if stale_rows:
            con.executemany("""
                UPDATE falcon_trade_orders
                SET status = 'STALE_PLACING', error = COALESCE(error, 'EOD flush: still PLACING after 15min')
                WHERE id = ?
            """, [(r["id"],) for r in stale_rows])
            con.commit()
    summary["stale_placing"] = len(stale_rows)
    if stale_rows:
        log.warning("EOD: marked %d orders as STALE_PLACING — operator should reconcile",
                    len(stale_rows))

    # 3. Daily activity snapshot
    today_ist = _now_ist().date().isoformat()
    with falcon_conn() as con:
        # Orders touched today (any status change in last 24h)
        ord_counts = con.execute("""
            SELECT role,
                   SUM(CASE WHEN status IN ('PLACED','PARTIAL') THEN 1 ELSE 0 END) AS placed,
                   SUM(CASE WHEN status='REJECTED' THEN 1 ELSE 0 END) AS rejected,
                   SUM(CASE WHEN status='CANCELLED' THEN 1 ELSE 0 END) AS cancelled,
                   SUM(CASE WHEN status='PARTIAL' THEN 1 ELSE 0 END) AS partial,
                   SUM(filled_qty) AS total_filled_qty
            FROM falcon_trade_orders
            WHERE DATE(placed_at) = ?
            GROUP BY role
        """, (today_ist,)).fetchall()
        # Trail bumps today
        trail_count = con.execute("""
            SELECT COUNT(*) AS n FROM falcon_trade_events
            WHERE kind='SL_REPLACED' AND DATE(detected_at) = ?
        """, (today_ist,)).fetchone()
        # Exits today (auto + manual)
        exit_count = con.execute("""
            SELECT COUNT(*) AS n FROM falcon_trade_events
            WHERE kind IN ('EXIT_PLACED','EXIT_FILLED') AND DATE(detected_at) = ?
        """, (today_ist,)).fetchone()
        # Partial fills detected today
        partial_count = con.execute("""
            SELECT COUNT(*) AS n FROM falcon_trade_events
            WHERE kind='PARTIAL_FILL' AND DATE(detected_at) = ?
        """, (today_ist,)).fetchone()

    summary["orders_by_role"] = [dict(r) for r in ord_counts]
    summary["trail_bumps"]    = int(trail_count["n"]) if trail_count else 0
    summary["exits"]          = int(exit_count["n"]) if exit_count else 0
    summary["partial_fills"]  = int(partial_count["n"]) if partial_count else 0

    # 4. Log a single SESSION_CLOSE event so it surfaces in the trigger feed.
    try:
        from . import position_monitor  # noqa: WPS433
        position_monitor.log_event(
            symbol="_SESSION_",
            kind="SESSION_CLOSE",
            severity="info",
            detail=(f"EOD: trail_bumps={summary['trail_bumps']} "
                    f"exits={summary['exits']} partials={summary['partial_fills']} "
                    f"stale_placing={summary['stale_placing']}"),
            auto_action_taken=False,
        )
    except Exception as e:
        log.exception("EOD: failed to log SESSION_CLOSE event: %s", e)

    return summary


def get_last_summary() -> Optional[Dict[str, Any]]:
    """Return the most recent EOD summary, or None."""
    _ensure_eod_table()
    with falcon_conn() as con:
        row = con.execute("""
            SELECT run_date, ran_at, summary_json FROM falcon_eod_runs
            ORDER BY run_date DESC LIMIT 1
        """).fetchone()
    if not row:
        return None
    return {
        "run_date": row["run_date"],
        "ran_at":   row["ran_at"],
        "summary":  json.loads(row["summary_json"]),
    }
