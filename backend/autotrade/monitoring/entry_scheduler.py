"""Per-session background ENTRY scheduler.

A lightweight daemon thread per SCHEDULED session that sleeps (interruptibly)
until the session's configured entry_time (IST) and then fires the entry leg —
exactly the same order-firing loop that session.start(when="now") runs — and
flips the session to RUNNING (which in turn arms the tick driver from inside
_fire_entries()).

This mirrors tick_driver.py's daemon-thread pattern on purpose:
  * Per-session thread, IST-aware target time.
  * Interruptible: stop() sets an Event; the sleep wakes immediately.
  * Idempotent: start_for_session() is a no-op if a scheduler is already armed
    for that session_id (single registry guarded by a lock).
  * Self-stopping: if the session is no longer SCHEDULED (cancelled / killed /
    closed) when the timer wakes, it places NOTHING and exits.

SAFETY:
  * Paper sessions place NO real orders — _fire_entries() builds brokers with
    dry_run=True (reuses the existing dry-run path). Live entries remain gated by
    FALCON_AUTOTRADE_ENABLED + mode='live' at the broker layer.
  * DATA-ISOLATION is inherited from _fire_entries(): scheduled fires write ONLY
    to autotrade_positions, NEVER falcon_position_state.

CAVEAT (documented, not fixed — same limitation as tick_driver): a scheduled
fire lives only in this process. If the backend restarts AFTER a session is
SCHEDULED but BEFORE entry_time, the in-memory timer is lost and the session
will NOT auto-fire on its own — the operator must re-start it. This is the same
restart caveat the tick driver has.
"""
from __future__ import annotations

import asyncio
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

from oltp_db import oltp_conn as falcon_conn  # OLTP: SQLite(flag off)/Postgres(KANIDA_PG_ENABLED). pure-OLTP module.

log = logging.getLogger("kanida.autotrade.entry_scheduler")

IST = timezone(timedelta(hours=5, minutes=30))

# PHASE-2 WARM: how far before the fire target to subscribe the basket to WS FULL
# + prime the circuit day-cache, so the 09:15 fire prices network-free.
_PREWARM_LEAD_SEC = 60.0

# session_id -> _Scheduler
_SCHEDULERS: Dict[str, "_Scheduler"] = {}
_LOCK = threading.Lock()


def _session_status(session_id: str) -> Optional[str]:
    with falcon_conn() as con:
        row = con.execute(
            "SELECT status FROM autotrade_sessions WHERE session_id=?",
            (session_id,),
        ).fetchone()
    return row["status"] if row else None


class _Scheduler:
    def __init__(self, session_id: str, target_ist: datetime, now_fn=None):
        self.session_id = session_id
        self.target_ist = target_ist
        # now_fn is the clock used to compute the remaining sleep. Production
        # passes None → real datetime.now(IST). Tests may inject a frozen clock
        # so an "armed but never fires" scheduler doesn't fire on a fake target.
        self._now_fn = now_fn or (lambda: datetime.now(IST))
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name=f"autotrade-entry-{session_id[:8]}",
            daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def _prewarm(self) -> None:
        """Best-effort pre-open warm (~60s before fire): subscribe the basket to
        WS FULL + prime the circuit day-cache. No-op for market mode / paper /
        disabled brokers. NEVER raises — a failed prewarm just means the fire
        falls back to the batched REST quote."""
        try:
            if _session_status(self.session_id) != "SCHEDULED":
                return
            from ..session import TradingSession, prewarm_execution
            sess = TradingSession.load(self.session_id)
            if sess is None:
                return
            prewarm_execution(sess)
        except Exception as e:  # pragma: no cover - never block the fire
            log.debug("entry_scheduler prewarm failed for %s: %s",
                      self.session_id, e)

    def _seconds_remaining(self) -> float:
        # Production: real wall-clock. Tests may inject a frozen now_fn. The
        # trading-day / market-open GATE is re-evaluated at WAKE inside
        # _fire_entries() (clock-seam aware) — this only governs the sleep length.
        return (self.target_ist - self._now_fn()).total_seconds()

    def _run(self) -> None:
        log.info("entry_scheduler armed for %s — fires at %s (in %.1fs)",
                 self.session_id, self.target_ist.isoformat(),
                 max(0.0, self._seconds_remaining()))
        try:
            # Interruptible sleep until the target time. wait() returns True if
            # stop() was called (cancel/kill), in which case we place NOTHING.
            # PHASE-2 WARM: wake ~PREWARM_LEAD_SEC early to subscribe the basket to
            # FULL + prime circuits so the fire prices network-free (no-op for
            # market mode / paper). If the target is already inside the lead
            # window, prewarm immediately then wait the remainder.
            remaining = self._seconds_remaining()
            if remaining > _PREWARM_LEAD_SEC:
                if self._stop.wait(remaining - _PREWARM_LEAD_SEC):
                    log.info("entry_scheduler: session %s stopped before fire — "
                             "placing nothing", self.session_id)
                    return
                remaining = self._seconds_remaining()
            self._prewarm()
            if remaining > 0:
                if self._stop.wait(remaining):
                    log.info("entry_scheduler: session %s stopped before fire — "
                             "placing nothing", self.session_id)
                    return

            # Re-check status at wake: only fire if STILL scheduled. A cancelled
            # / killed / closed session must not place anything.
            status = _session_status(self.session_id)
            if status != "SCHEDULED":
                log.info("entry_scheduler: session %s status=%s at fire time — "
                         "placing nothing", self.session_id, status)
                return

            try:
                from ..session import TradingSession
                sess = TradingSession.load(self.session_id)
            except Exception as e:  # pragma: no cover - defensive
                log.error("entry_scheduler: failed to load session %s: %s",
                          self.session_id, e)
                return
            if sess is None:
                log.warning("entry_scheduler: session %s not found at fire time",
                            self.session_id)
                return

            log.info("entry_scheduler: FIRING scheduled entries for %s",
                     self.session_id)
            try:
                # _fire_entries() flips status to RUNNING and arms the tick
                # driver. Paper = no real orders (brokers dry_run=True).
                asyncio.run(sess._fire_entries())
            except Exception as e:  # pragma: no cover - never crash the thread
                log.exception("entry_scheduler: fire failed for %s: %s",
                              self.session_id, e)
        finally:
            self._deregister()
            log.info("entry_scheduler stopped for %s", self.session_id)

    def _deregister(self) -> None:
        with _LOCK:
            if _SCHEDULERS.get(self.session_id) is self:
                _SCHEDULERS.pop(self.session_id, None)


def start_for_session(session_id: str, target_ist: datetime,
                      now_fn=None) -> bool:
    """Arm an entry scheduler for session_id that fires at target_ist (an
    IST-aware datetime). Idempotent — returns False if one is already armed for
    this session_id, True if a new scheduler was armed. `now_fn` (tests only)
    injects a frozen clock for the sleep computation.

    MULTI-CONTAINER (Stage 4b): only the container that OWNS this session's lease
    arms the scheduler. Without this, every task would arm a scheduler for every
    scheduled session and all of them would wake at 09:15 — the duplicate-order
    claim would stop the extra fires, but only after N containers had raced. No-op
    (always True) in single-container mode."""
    from ..session_ownership import try_own
    if not try_own(session_id):
        log.info("entry_scheduler: %s owned by another container — not arming",
                 session_id)
        return False
    with _LOCK:
        existing = _SCHEDULERS.get(session_id)
        if existing is not None and existing.is_alive():
            return False
        sch = _Scheduler(session_id, target_ist, now_fn=now_fn)
        _SCHEDULERS[session_id] = sch
    sch.start()
    return True


def stop_for_session(session_id: str) -> None:
    """Request the session's entry scheduler to stop (idempotent). The thread
    will place NOTHING and exit."""
    with _LOCK:
        sch = _SCHEDULERS.get(session_id)
    if sch is not None:
        sch.stop()


def is_running(session_id: str) -> bool:
    with _LOCK:
        sch = _SCHEDULERS.get(session_id)
    return bool(sch and sch.is_alive())


def target_for_session(session_id: str) -> Optional[datetime]:
    """The armed fire time (IST-aware) for a scheduled session, or None."""
    with _LOCK:
        sch = _SCHEDULERS.get(session_id)
    return sch.target_ist if sch else None
