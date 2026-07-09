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
from typing import Any, Dict, List, Optional

_LOCK = threading.Lock()
# session_id -> hash of the open-position set the reconciler last validated.
_LAST_RECONCILED: Dict[str, str] = {}


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
    """True when the current open-position set matches the last reconcile-validated
    set (or there is NO stamp yet — a fresh session, don't block the fast kill).
    False when the set CHANGED since the last tick reconcile → the ws should defer
    firing until the next 5s reconcile validates the new basket."""
    with _LOCK:
        stamp: Optional[str] = _LAST_RECONCILED.get(session_id)
    if stamp is None:
        return True
    return open_position_hash(positions) == stamp


def reset(session_id: str) -> None:
    """Clear the stamp for a session (tests / operator recovery)."""
    with _LOCK:
        _LAST_RECONCILED.pop(session_id, None)
