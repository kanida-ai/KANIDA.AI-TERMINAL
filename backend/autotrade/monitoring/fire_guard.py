"""Per-session kill-switch fire guard — idempotent single-fire across paths.

FEATURE 2: the portfolio kill switch is now driven from TWO paths:
  * the existing 5s tick_driver heartbeat (session.tick), and
  * the sub-second WebSocket tick path (ws_driver).

Both can observe the threshold crossing at almost the same instant. This guard
ensures the flatten fires EXACTLY ONCE per session: whoever wins the per-session
lock fires; the other path sees the lock already taken (or the session already
KILLING/CLOSED) and backs off. The lock is process-local (in-memory) — the same
process owns both the poll thread and the WS callback, so a threading lock is
sufficient and avoids racing on the DB.

Usage:
    with claim_fire(session_id) as won:
        if won:
            await kill_switch.fire(...)
        # else: another path already fired / is firing — do nothing.

claim_fire is re-entrant-safe ONLY in the sense that a second concurrent caller
gets won=False; it is NOT reentrant for the same thread holding it twice.
"""
from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Dict, Iterator

_LOCK = threading.Lock()
# session_id -> True once a fire has been claimed (never reset within a session;
# a session fires at most once then goes CLOSED).
_FIRED: Dict[str, bool] = {}
# session_id -> True once ENTRY placement has been claimed (C5). SEPARATE from
# _FIRED so the entry idempotency guard never blocks the later kill-switch fire.
_ENTRY_CLAIMED: Dict[str, bool] = {}


def claim_entry(session_id: str) -> bool:
    """Claim the right to place THIS session's ENTRIES exactly once (C5).

    Returns True to the FIRST caller, False to any concurrent / subsequent caller
    — so two racing start()/scheduled-fire invocations place orders ONCE. Kept
    separate from the exit _FIRED guard so a later kill-switch fire is unaffected.
    The durable idempotency check is the DB (a session with OPEN positions already
    fired); this in-memory claim closes the concurrent double-invocation window."""
    with _LOCK:
        if _ENTRY_CLAIMED.get(session_id):
            return False
        _ENTRY_CLAIMED[session_id] = True
        return True


def entry_claimed(session_id: str) -> bool:
    with _LOCK:
        return bool(_ENTRY_CLAIMED.get(session_id))


@contextmanager
def claim_fire(session_id: str) -> Iterator[bool]:
    """Context manager yielding True iff THIS caller won the right to fire the
    session's kill switch. If another path already claimed it, yields False."""
    with _LOCK:
        if _FIRED.get(session_id):
            won = False
        else:
            _FIRED[session_id] = True
            won = True
    yield won


def already_fired(session_id: str) -> bool:
    with _LOCK:
        return bool(_FIRED.get(session_id))


def reset(session_id: str) -> None:
    """Clear the guard for a session. For tests / operator recovery only."""
    with _LOCK:
        _FIRED.pop(session_id, None)
        _ENTRY_CLAIMED.pop(session_id, None)
