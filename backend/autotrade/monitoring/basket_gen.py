"""Basket reconcile-validation stamp (R4).

The sub-second ws_driver updates marks + runs the trail/kill decision, but it
NEVER runs the 5s broker reconcile (only session.tick() does). So the ws can see
a STALE basket — e.g. a leg that closed at the broker but whose DB row hasn't been
reconciled yet — and fire a real-money exit on a phantom position.

This module lets the tick path STAMP the open-position set it just reconciled, and
lets the ws ASK whether the current DB open-set still matches that validated set.
When it does NOT match (the set changed since the last reconcile), the ws defers
its FIRE to the next 5s tick (which reconciles then fires) — it still updates
marks. Process-local + tiny (one hash per session); NO broker calls added to the
ws hot path.

Fresh sessions (no stamp yet) return validated=True so the fast sub-second kill is
NOT delayed on a brand-new session — the guard only ever ADDS caution when there
is positive evidence the basket diverged from the last broker-validated view.
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

_IST = timezone(timedelta(hours=5, minutes=30))

_LOCK = threading.Lock()
# session_id -> hash of the open-position set the reconciler last validated.
_LAST_RECONCILED: Dict[str, str] = {}
# CLUSTER 3 ITEM 2 — reconcile HEALTH. The sub-second ws must never stamp/act on a
# basket that a NO-OP reconcile (broker book None/unreachable) never actually
# validated. reconcile_broker_positions records, per session:
#   _RECONCILE_HEALTHY[sid]  = True  (a successful net-book read for EVERY relevant
#                                     broker profile this cycle)
#                            = False (a live reconcile ATTEMPTED but at least one
#                                     relevant profile's net book was None/unreachable)
#                            = <absent> (paper / reconcile disabled / never run →
#                                     no health opinion → gating unchanged = today)
# A False forces basket_reconcile_validated → False so the fast-path fire DEFERS
# until a healthy reconcile re-validates. _RECONCILE_SUCCESS_TS keeps the last
# HEALTHY timestamp for age-based alerting (Cluster 6).
_RECONCILE_HEALTHY: Dict[str, bool] = {}
_RECONCILE_SUCCESS_TS: Dict[str, str] = {}


def open_position_hash(positions: List[Dict[str, Any]]) -> str:
    """Deterministic hash of the open-position set: sorted (symbol, qty) pairs.
    A close, an add, or a qty change all move the hash."""
    items = sorted(
        (str(p.get("symbol")), int(p.get("qty") or 0)) for p in positions)
    return repr(items)


def stamp_reconciled(session_id: str, positions: List[Dict[str, Any]]) -> None:
    """Record the open-position set that a tick reconcile just validated."""
    with _LOCK:
        _LAST_RECONCILED[session_id] = open_position_hash(positions)


def basket_reconcile_validated(session_id: str,
                               positions: List[Dict[str, Any]]) -> bool:
    """True when the current basket is safe for the sub-second fast-path fire.

    False (DEFER) when EITHER:
      * the last live reconcile was UNHEALTHY (broker book None/unreachable — the
        no-op never validated anything, CLUSTER 3 ITEM 2), OR
      * the open-position set CHANGED since the last reconcile-validated stamp
        (a leg closed at the broker but the DB row isn't reconciled yet — R4).
    True when there is NO stamp AND no unhealthy signal (a fresh / paper session —
    don't block the fast kill)."""
    with _LOCK:
        healthy: Optional[bool] = _RECONCILE_HEALTHY.get(session_id)
        stamp: Optional[str] = _LAST_RECONCILED.get(session_id)
    # A live reconcile that could NOT read the broker book → the basket is NOT
    # broker-validated this cycle → the ws must defer (never fire on an unvalidated
    # basket after an outage). Paper / never-run (healthy is None) → unchanged.
    if healthy is False:
        return False
    if stamp is None:
        return True
    return open_position_hash(positions) == stamp


def record_reconcile_health(session_id: str, healthy: bool,
                            ts: Optional[str] = None) -> None:
    """CLUSTER 3 ITEM 2 — the reconciler records whether it got a successful
    net-book read for every relevant broker profile this cycle. On a HEALTHY
    reconcile the last-success timestamp is advanced (for age-based alerting).
    Called ONLY from a LIVE reconcile attempt (paper / disabled never call this,
    so the fast-path gating is byte-for-byte unchanged for paper)."""
    now = ts or datetime.now(_IST).isoformat()
    with _LOCK:
        _RECONCILE_HEALTHY[session_id] = bool(healthy)
        if healthy:
            _RECONCILE_SUCCESS_TS[session_id] = now


def last_successful_reconcile_ts(session_id: str) -> Optional[str]:
    """The ISO timestamp of the last HEALTHY reconcile for a session, or None."""
    with _LOCK:
        return _RECONCILE_SUCCESS_TS.get(session_id)


def last_successful_reconcile_age_seconds(
        session_id: str, now: Optional[datetime] = None) -> Optional[float]:
    """Seconds since the last HEALTHY reconcile, or None when there has never been
    one (paper / never run / only-ever-unhealthy). Cluster 6 pages on this."""
    ts = last_successful_reconcile_ts(session_id)
    if ts is None:
        return None
    try:
        then = datetime.fromisoformat(ts)
    except ValueError:  # pragma: no cover - defensive
        return None
    ref = now or datetime.now(_IST)
    if then.tzinfo is None:
        then = then.replace(tzinfo=_IST)
    return max(0.0, (ref - then).total_seconds())


def reconcile_healthy(session_id: str) -> Optional[bool]:
    """The last recorded reconcile health for a session (True/False), or None when
    no live reconcile has run (paper / disabled)."""
    with _LOCK:
        return _RECONCILE_HEALTHY.get(session_id)


def reset(session_id: str) -> None:
    """Clear the stamp + health for a session (tests / operator recovery)."""
    with _LOCK:
        _LAST_RECONCILED.pop(session_id, None)
        _RECONCILE_HEALTHY.pop(session_id, None)
        _RECONCILE_SUCCESS_TS.pop(session_id, None)
