"""The SINGLE exit gate. Every exit — the 4 existing per-position / day-bound
exits AND the new portfolio kill switch — claims a position through here so two
mechanisms can never double-exit the same position.

Mechanism: an atomic, conditional UPDATE on falcon_position_state sets
exit_lock=1 only when it is currently 0/NULL. SQLite serialises writes, so the
UPDATE...WHERE exit_lock=0 is the row lock — exactly one caller flips it from 0
to 1 and sees rowcount==1; everyone else sees 0 and is blocked.

claim_exit(symbol, reason) → bool
    True  → caller owns the exit, proceed to place the market exit.
    False → another mechanism already claimed it; do NOT place an order.

try_exit_position(symbol, reason, broker, qty=None) → dict
    Convenience wrapper: claim, then (if won) place the market exit via the
    broker (dry-run-safe). Used by the kill switch. The existing exit executors
    integrate by calling claim_exit() FIRST (minimal wrap — they keep their own
    Kite placement logic intact).

release_exit(symbol) — for tests / recovery only. Resets the lock.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from falcon.db import falcon_conn

log = logging.getLogger("kanida.autotrade.exit_gate")
IST = timezone(timedelta(hours=5, minutes=30))

VALID_REASONS = {
    "TRAILING_STOP", "TRAILING_PROFIT", "TIME_BOUND", "DAY_BOUND",
    "KILL_SWITCH", "MANUAL",
    # Map the existing trail_manager reasons onto the gate vocabulary too, so a
    # minimal wrap there can pass its native reason string through unchanged.
    "BREACHED_SL", "TIME_STOP", "TARGET_HIT",
}


def _ensure_lock_columns(con) -> None:
    """Defensive: make sure exit_lock / exit_initiated_by exist. The migration
    adds them at boot; this guards tests that build the table standalone."""
    cols = [r[1] for r in con.execute("PRAGMA table_info(falcon_position_state)")]
    if "exit_lock" not in cols:
        con.execute("ALTER TABLE falcon_position_state ADD COLUMN exit_lock INTEGER DEFAULT 0")
    if "exit_initiated_by" not in cols:
        con.execute("ALTER TABLE falcon_position_state ADD COLUMN exit_initiated_by TEXT")


def claim_exit(symbol: str, reason: str) -> bool:
    """Atomically claim the exit for `symbol`. Returns True iff this caller may
    place the exit order.

    Semantics:
      * Lock currently free → set it to this reason, return True (caller owns it).
      * Lock already held by a DIFFERENT mechanism → return False (do NOT place
        another order — the spec's "first one wins, lock the position").
      * Lock already held by the SAME reason → return True (re-entrant). This is
        what lets the kill switch pre-claim a position and then have the shared
        execute_exit_at_market path claim again without being blocked, and lets
        a single mechanism's retry proceed.
    """
    if reason not in VALID_REASONS:
        log.warning("exit_gate: unrecognised reason %r for %s (allowing)", reason, symbol)
    now = datetime.now(IST).isoformat()
    with falcon_conn() as con:
        _ensure_lock_columns(con)
        # First attempt: claim a free lock atomically.
        cur = con.execute(
            """UPDATE falcon_position_state
               SET exit_lock = 1,
                   exit_initiated_by = ?,
                   last_event_kind = 'EXIT_CLAIMED',
                   last_event_at = ?
               WHERE symbol = ?
                 AND COALESCE(exit_lock, 0) = 0""",
            (reason, now, symbol),
        )
        con.commit()
        if cur.rowcount == 1:
            log.info("exit_gate: %s claimed by %s", symbol, reason)
            return True
        # Lock not free (or no row). Inspect the current owner.
        row = con.execute(
            "SELECT exit_initiated_by FROM falcon_position_state WHERE symbol=?",
            (symbol,),
        ).fetchone()
    owner = row["exit_initiated_by"] if row else "NO_SUCH_POSITION"
    if owner == reason:
        # Re-entrant claim by the same mechanism (e.g. kill switch → broker exit).
        log.info("exit_gate: %s re-entrant claim by %s (already owned)", symbol, reason)
        return True
    log.info("exit_gate: %s exit blocked — already owned by %s (wanted %s)",
             symbol, owner, reason)
    return False


def release_exit(symbol: str) -> None:
    """Reset the lock. For tests / operator recovery only."""
    with falcon_conn() as con:
        _ensure_lock_columns(con)
        con.execute(
            "UPDATE falcon_position_state SET exit_lock=0, exit_initiated_by=NULL "
            "WHERE symbol=?", (symbol,),
        )
        con.commit()


def is_locked(symbol: str) -> bool:
    with falcon_conn() as con:
        row = con.execute(
            "SELECT COALESCE(exit_lock,0) AS l FROM falcon_position_state WHERE symbol=?",
            (symbol,),
        ).fetchone()
    return bool(row and row["l"])


def try_exit_position(symbol: str, reason: str, broker,
                      qty: Optional[int] = None,
                      instrument_type: str = "EQ") -> Dict[str, Any]:
    """Claim + (if won) flatten via broker. Returns a result dict.

    Used by the kill switch. The existing per-position exit executors integrate
    more cheaply by calling claim_exit() directly and keeping their own placement
    code (see the minimal wrap in trail_manager / eod_flush).
    """
    won = claim_exit(symbol, reason)
    if not won:
        return {"symbol": symbol, "status": "BLOCKED", "claimed": False,
                "reason": reason}

    if qty is None:
        # Recompute open qty from state (spec: kill switch recalculates).
        from falcon.trade.services import position_monitor
        st = position_monitor.get_state(symbol) or {}
        qty = int(st.get("qty") or 0)

    if qty <= 0:
        return {"symbol": symbol, "status": "NO_QTY", "claimed": True,
                "reason": reason}

    res = None
    try:
        # broker.place_market_exit is async; run it inline if there's no loop.
        import asyncio
        coro = broker.place_market_exit(symbol, qty, instrument_type)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop and loop.is_running():
            # Shouldn't happen on the sync path; caller should await instead.
            raise RuntimeError("try_exit_position called inside a running loop; "
                               "await broker.place_market_exit directly")
        res = asyncio.run(coro)
    except Exception as e:
        log.error("try_exit_position placement failed for %s: %s", symbol, e)
        return {"symbol": symbol, "status": "FAILED", "claimed": True,
                "reason": reason, "error": str(e)}

    return {"symbol": symbol, "status": res.status, "claimed": True,
            "reason": reason, "broker_order_id": res.broker_order_id,
            "error": res.error}
