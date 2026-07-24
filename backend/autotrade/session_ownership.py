"""Stage 4b — SESSION OWNERSHIP LEASES (safe horizontal scaling).

PROBLEM
  entry_scheduler and tick_driver each run a daemon thread PER SESSION, inside
  the process. With more than one container every task would spawn drivers for
  EVERY session: duplicate monitoring, duplicate exit attempts, N× broker calls.
  The Postgres claim (Stage 4a) stops duplicate ORDERS, but work still has to be
  DIVIDED or scaling buys nothing.

MODEL
  Each session is owned by exactly one container, via a short lease in the
  shared claim table (`driver:<session_id>`):

    * try_own()  — claim the session. Only the winner starts its drivers.
    * renew()    — the owner extends its lease on a background heartbeat.
    * If a container dies, its lease EXPIRES and another container claims the
      session on its next sweep → automatic failover, no manual intervention.

  Lease (90s) is deliberately >> renew interval (30s), so a slow tick or brief
  DB hiccup cannot hand a live session to another container. Losing a renewal
  means we genuinely lost ownership → the caller must stop its drivers, and the
  new owner takes over.

SAFETY / ROLLOUT
  Default OFF. With KANIDA_MULTI_CONTAINER unset, try_own()/owns() return True
  unconditionally — single-container behaviour is byte-for-byte unchanged. Only
  enable it once KANIDA_PG_CLAIMS is on, because leases are only meaningful on
  the shared Postgres claim table (container-local SQLite would let every task
  "own" everything — the very bug this prevents).
"""
from __future__ import annotations

import logging
import os
import socket
import threading
import time
import uuid
from typing import Set

from . import durable_claims

log = logging.getLogger("kanida.autotrade.session_ownership")

# Stable identity for THIS container/process for the life of the task.
OWNER_ID = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"

LEASE_SEC = 90.0
RENEW_SEC = 30.0

_OWNED: Set[str] = set()
_LOCK = threading.Lock()
_RENEWER: threading.Thread | None = None
_STOP = threading.Event()


def multi_container() -> bool:
    """True when several tasks may run concurrently (leases required)."""
    return os.environ.get("KANIDA_MULTI_CONTAINER", "").strip().lower() in (
        "1", "true", "yes", "on")


def _key(session_id: str) -> str:
    return f"driver:{session_id}"


def try_own(session_id: str) -> bool:
    """Claim this session for THIS container. True => start its drivers.

    Single-container mode always returns True (unchanged behaviour).
    """
    if not multi_container():
        return True
    won = durable_claims.claim(_key(session_id), LEASE_SEC, holder=OWNER_ID)
    if won:
        with _LOCK:
            _OWNED.add(session_id)
        log.info("session_ownership: OWNED %s (owner=%s)", session_id, OWNER_ID)
        _ensure_renewer()
    return won


def owns(session_id: str) -> bool:
    if not multi_container():
        return True
    with _LOCK:
        return session_id in _OWNED


def release(session_id: str) -> None:
    """Give up a session (closed/stopped) so another container may take it."""
    with _LOCK:
        _OWNED.discard(session_id)
    if multi_container():
        durable_claims.release(_key(session_id))


def owned_snapshot() -> list:
    with _LOCK:
        return sorted(_OWNED)


def _renew_loop() -> None:
    while not _STOP.wait(RENEW_SEC):
        for sid in owned_snapshot():
            try:
                if not durable_claims.renew(_key(sid), LEASE_SEC, OWNER_ID):
                    # Lost the lease (expired + taken over). Drop ownership; the
                    # driver modules check owns() and will stop themselves.
                    with _LOCK:
                        _OWNED.discard(sid)
                    log.warning("session_ownership: LOST lease for %s — another "
                                "container now owns it", sid)
            except Exception as e:  # pragma: no cover - never kill the thread
                log.warning("session_ownership: renew failed for %s: %s", sid, e)


def _ensure_renewer() -> None:
    global _RENEWER
    if not multi_container():
        return
    with _LOCK:
        if _RENEWER is not None and _RENEWER.is_alive():
            return
        _STOP.clear()
        _RENEWER = threading.Thread(target=_renew_loop, daemon=True,
                                    name="session-ownership-renewer")
        _RENEWER.start()
    log.info("session_ownership: renewer started (owner=%s lease=%ss renew=%ss)",
             OWNER_ID, LEASE_SEC, RENEW_SEC)


def stop_renewer() -> None:
    _STOP.set()
