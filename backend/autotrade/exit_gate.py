"""The exit gate(s) — atomic exit-claim locks so two mechanisms can never
double-exit the same position.

TWO ISOLATED SCOPES:

1. The EXISTING Falcon swing system (trail_manager) locks on
   falcon_position_state keyed by `symbol`. That is left UNCHANGED — it is a
   separate system and legitimately owns falcon_position_state. The functions
   claim_exit / release_exit / is_locked / try_exit_position keep operating on
   falcon_position_state for that caller.

2. The NEW autotrade kill switch + per-session exits lock on the ISOLATED
   autotrade_positions table, keyed by (session_id, symbol). These are the
   *_session functions. The autotrade system NEVER touches falcon_position_state.

Mechanism (both scopes): an atomic, conditional UPDATE sets exit_lock=1 only
when it is currently 0/NULL. SQLite serialises writes, so UPDATE ... WHERE
exit_lock=0 is the row lock — exactly one caller flips it from 0 to 1 and sees
rowcount==1; everyone else sees 0 and is blocked.

Autotrade API (session-scoped):
    claim_exit_session(session_id, symbol, reason) → bool
    release_exit_session(session_id, symbol)
    is_locked_session(session_id, symbol) → bool

Falcon API (legacy, falcon_position_state — DO NOT route autotrade through it):
    claim_exit(symbol, reason) → bool
    release_exit(symbol)
    is_locked(symbol) → bool
    try_exit_position(symbol, reason, broker, qty=None) → dict
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Set, Tuple

from falcon.db import falcon_conn

log = logging.getLogger("kanida.autotrade.exit_gate")
IST = timezone(timedelta(hours=5, minutes=30))

# ════════════════════════════════════════════════════════════════════════════
# SINGLE-FLIGHT EXIT MUTEX (Fix B2, 2026-07-10) — in-process, per (session,symbol).
# The DB gate (claim_exit_session) grants a RE-ENTRANT claim to a second caller
# with the SAME reason (so a kill-switch pre-claim can re-enter the shared exec
# path). But the 5s tick_driver and the sub-second ws_driver BOTH run the per-
# stock step-lock in SEPARATE threads, so both could fire STOP_STOCK for the same
# symbol at the same instant: both got a re-entrant TRUE and both placed a market
# exit → a short's buy-to-cover fired TWICE (938 covered + 938 extra) → a NAKED
# long (BRIGADE/LODHA/MAPMYINDIA, ~₹15L, 2026-07-10). This mutex makes ORDER
# PLACEMENT single-flight: while an exit is in flight for a (session,symbol), a
# second concurrent placement — even the same reason, even from the other driver
# — is a NO-OP. It does NOT block the SEQUENTIAL EXIT_FAILED retry: the previous
# flight has ended (slot cleared) before the next tick re-attempts.
_INFLIGHT_LOCK = threading.Lock()
# CLUSTER 9 ITEM 1 (2026-07-11) — the flight slot is keyed by the FULL leg
# identity (session_id, symbol, broker_profile), not (session_id, symbol). A
# single session holding the SAME symbol on TWO broker profiles has two distinct
# autotrade_positions rows (the composite unique key is
# (session_id, symbol, broker_profile)); their exits must fly INDEPENDENTLY — one
# profile's in-flight exit must never block the other profile's exit. The third
# tuple element is COALESCE('') so broker_profile=None (the single-profile norm)
# is byte-for-byte the old 2-tuple behaviour.
_INFLIGHT: Set[Tuple[str, str, str]] = set()


def _flight_key(session_id: str, symbol: str,
                broker_profile: Optional[str]) -> str:
    """The cross-process durable-claim key. Byte-for-byte the old string when
    broker_profile is None (the single-profile norm); profile-scoped otherwise."""
    if broker_profile:
        return f"exitflight:{session_id}:{symbol}:{broker_profile}"
    return f"exitflight:{session_id}:{symbol}"


def begin_exit_flight(session_id: str, symbol: str,
                      broker_profile: Optional[str] = None) -> bool:
    """Atomically acquire the single-flight slot for
    (session_id, symbol, broker_profile).

    True  → the caller MAY place the exit order (no other placement in flight).
    False → an exit order is ALREADY in flight for this key → the caller MUST NOT
            place a second order (return a NO-OP). Must be paired with
            end_exit_flight() in a finally so the slot is always released.

    ITEM 2 (C3 I3a): backed by a cross-process DURABLE claim (CAS + lease). The
    in-process set is the FAST first check; the DB claim is the AUTHORITY so a
    second PROCESS (or a restart mid-flight) can't place a duplicate exit. The
    lease reclaims a crashed holder after EXIT_FLIGHT_LEASE_SEC.

    CLUSTER 9 ITEM 1: keyed by (session_id, symbol, broker_profile) so the same
    symbol on two profiles flies independently (broker_profile=None ⇒ unchanged)."""
    from . import durable_claims
    key = (session_id, symbol, broker_profile or "")
    with _INFLIGHT_LOCK:
        if key in _INFLIGHT:
            return False
        if not durable_claims.claim(
                _flight_key(session_id, symbol, broker_profile),
                durable_claims.EXIT_FLIGHT_LEASE_SEC):
            return False
        _INFLIGHT.add(key)
        return True


def end_exit_flight(session_id: str, symbol: str,
                    broker_profile: Optional[str] = None) -> None:
    """Release the single-flight slot for (session_id, symbol, broker_profile).
    Idempotent."""
    from . import durable_claims
    with _INFLIGHT_LOCK:
        _INFLIGHT.discard((session_id, symbol, broker_profile or ""))
    durable_claims.release(_flight_key(session_id, symbol, broker_profile))


def is_exit_in_flight(session_id: str, symbol: str,
                      broker_profile: Optional[str] = None) -> bool:
    with _INFLIGHT_LOCK:
        return (session_id, symbol, broker_profile or "") in _INFLIGHT

VALID_REASONS = {
    "TRAILING_STOP", "TRAILING_PROFIT", "TIME_BOUND", "DAY_BOUND",
    "KILL_SWITCH", "MANUAL",
    # Intraday-basket trail engine reasons (strategy=="intraday_basket").
    "TRAIL_EXIT", "FLOOR_EXIT", "STOP", "SQUARE_OFF", "STEP_LOCK_EXIT",
    # Per-stock software stop (Fix 3: individual stock exits before the GTT fires).
    "STOP_STOCK",
    # Map the existing trail_manager reasons onto the gate vocabulary too, so a
    # minimal wrap there can pass its native reason string through unchanged.
    "BREACHED_SL", "TIME_STOP", "TARGET_HIT",
    # EXIT_FAILED retry path — tick() re-attempts exits whose gate was released
    # by registry.mark_exit_failed after a prior placement failure.
    "EXIT_RETRY",
}


# ════════════════════════════════════════════════════════════════════════════
# AUTOTRADE session-scoped exit gate — locks autotrade_positions, never
# falcon_position_state. Keyed by (session_id, symbol).
# ════════════════════════════════════════════════════════════════════════════

def claim_exit_session(session_id: str, symbol: str, reason: str,
                       broker_profile: Optional[str] = None) -> bool:
    """Atomically claim the exit for (session_id, symbol[, broker_profile]) in
    autotrade_positions.

    True  → this caller owns the exit (lock was free, or re-entrant same reason).
    False → another mechanism already claimed it (do NOT place another order),
            or no such open position.

    CLUSTER 9 ITEM 1 (2026-07-11): when broker_profile is given the claim UPDATE +
    the owner re-read are scoped to (session_id, symbol, broker_profile) with the
    same COALESCE(broker_profile,'') pattern used elsewhere, so a session holding
    the SAME symbol on two profiles locks ONLY the firing leg — the other profile's
    exit is never blocked. broker_profile=None keeps the old symbol-wide claim
    (byte-for-byte for the single-profile norm)."""
    if reason not in VALID_REASONS:
        log.warning("exit_gate: unrecognised reason %r for %s/%s (allowing)",
                    reason, session_id, symbol)
    now = datetime.now(IST).isoformat()
    with falcon_conn() as con:
        if broker_profile is not None:
            cur = con.execute(
                """UPDATE autotrade_positions
                   SET exit_lock = 1, exit_initiated_by = ?
                   WHERE session_id = ? AND symbol = ?
                     AND COALESCE(broker_profile,'')=COALESCE(?,'')
                     AND COALESCE(exit_lock, 0) = 0""",
                (reason, session_id, symbol, broker_profile),
            )
        else:
            cur = con.execute(
                """UPDATE autotrade_positions
                   SET exit_lock = 1, exit_initiated_by = ?
                   WHERE session_id = ? AND symbol = ?
                     AND COALESCE(exit_lock, 0) = 0""",
                (reason, session_id, symbol),
            )
        con.commit()
        if cur.rowcount == 1:
            log.info("exit_gate: %s/%s claimed by %s", session_id, symbol, reason)
            return True
        if broker_profile is not None:
            row = con.execute(
                """SELECT exit_initiated_by FROM autotrade_positions
                   WHERE session_id=? AND symbol=?
                     AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                (session_id, symbol, broker_profile),
            ).fetchone()
        else:
            row = con.execute(
                """SELECT exit_initiated_by FROM autotrade_positions
                   WHERE session_id=? AND symbol=?""",
                (session_id, symbol),
            ).fetchone()
    owner = row["exit_initiated_by"] if row else "NO_SUCH_POSITION"
    if owner == reason:
        log.info("exit_gate: %s/%s re-entrant claim by %s", session_id, symbol, reason)
        return True
    log.info("exit_gate: %s/%s blocked — owned by %s (wanted %s)",
             session_id, symbol, owner, reason)
    return False


def release_exit_session(session_id: str, symbol: str,
                         broker_profile: Optional[str] = None) -> None:
    """Reset the session lock. For tests / operator recovery only. Scoped by
    broker_profile when given (CLUSTER 9 ITEM 1)."""
    with falcon_conn() as con:
        if broker_profile is not None:
            con.execute(
                """UPDATE autotrade_positions
                   SET exit_lock=0, exit_initiated_by=NULL
                   WHERE session_id=? AND symbol=?
                     AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                (session_id, symbol, broker_profile),
            )
        else:
            con.execute(
                """UPDATE autotrade_positions
                   SET exit_lock=0, exit_initiated_by=NULL
                   WHERE session_id=? AND symbol=?""",
                (session_id, symbol),
            )
        con.commit()


def is_locked_session(session_id: str, symbol: str,
                      broker_profile: Optional[str] = None) -> bool:
    with falcon_conn() as con:
        if broker_profile is not None:
            row = con.execute(
                """SELECT COALESCE(exit_lock,0) AS l FROM autotrade_positions
                   WHERE session_id=? AND symbol=?
                     AND COALESCE(broker_profile,'')=COALESCE(?,'')""",
                (session_id, symbol, broker_profile),
            ).fetchone()
        else:
            row = con.execute(
                """SELECT COALESCE(exit_lock,0) AS l FROM autotrade_positions
                   WHERE session_id=? AND symbol=?""",
                (session_id, symbol),
            ).fetchone()
    return bool(row and row["l"])


# ════════════════════════════════════════════════════════════════════════════
# FALCON legacy exit gate — falcon_position_state (existing swing system only).
# ════════════════════════════════════════════════════════════════════════════

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
