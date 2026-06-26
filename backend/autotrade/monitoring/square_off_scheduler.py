"""Per-session SQUARE-OFF scheduler (strategy=="intraday_basket").

A lightweight daemon thread per RUNNING intraday_basket session that sleeps
(interruptibly) until the session's configured square_off_time (IST) and then
flattens the basket — REUSING the existing flatten path
(KillSwitchExecutor.fire) with close_reason="SQUARE_OFF" — and the session goes
CLOSED. NEVER overnight.

This mirrors entry_scheduler.py's daemon-thread pattern:
  * Per-session thread, IST-aware target time.
  * Interruptible: stop() sets an Event; the sleep wakes immediately.
  * Idempotent: start_for_session() is a no-op if already armed for that session.
  * Self-stopping: if the session is no longer RUNNING (killed / already
    squared-off / closed) when the timer wakes, it flattens NOTHING and exits.

DEFENCE IN DEPTH: the trail engine ALSO enforces square-off inside session.tick()
(decide() returns EXIT "SQUARE_OFF" once now>=square_off_time), so even if this
in-memory timer is dropped by a restart the 5s tick driver squares the basket off
on its next tick. This scheduler is the precise-time path; the tick driver is the
backstop. Both go through the single per-session fire guard so they can never
double-fire.

SAFETY:
  * Paper sessions place NO real orders on fire (brokers built dry_run=True).
  * Live exits remain gated by FALCON_AUTOTRADE_ENABLED + mode='live'.
  * DATA-ISOLATION inherited from kill_switch.fire — writes ONLY
    autotrade_positions, NEVER falcon_position_state.
  * Never raises out of the thread (defence in depth around live money).
"""
from __future__ import annotations

import asyncio
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

from falcon.db import falcon_conn

log = logging.getLogger("kanida.autotrade.square_off_scheduler")

IST = timezone(timedelta(hours=5, minutes=30))

# Production leaves this on; tests flip it off so the daemon thread can't race
# assertions on the shared temp DB. The dedicated square-off test re-enables it.
_AUTOSTART = True


def set_autostart(enabled: bool) -> None:
    global _AUTOSTART
    _AUTOSTART = bool(enabled)


def autostart_enabled() -> bool:
    import os
    if os.environ.get("FALCON_AUTOTRADE_SQUAREOFF_DISABLE") == "1":
        return False
    return _AUTOSTART


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
    def __init__(self, session_id: str, target_ist: datetime):
        self.session_id = session_id
        self.target_ist = target_ist
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name=f"autotrade-squareoff-{session_id[:8]}",
            daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def _seconds_remaining(self) -> float:
        return (self.target_ist - datetime.now(IST)).total_seconds()

    def _run(self) -> None:
        from . import fire_guard
        log.info("square_off_scheduler armed for %s — fires at %s (in %.1fs)",
                 self.session_id, self.target_ist.isoformat(),
                 max(0.0, self._seconds_remaining()))
        try:
            remaining = self._seconds_remaining()
            if remaining > 0:
                if self._stop.wait(remaining):
                    log.info("square_off_scheduler: session %s stopped before "
                             "square-off — placing nothing", self.session_id)
                    return

            # Re-check status at wake: only square off a STILL-RUNNING session.
            status = _session_status(self.session_id)
            if status != "RUNNING":
                log.info("square_off_scheduler: session %s status=%s at fire "
                         "time — placing nothing", self.session_id, status)
                return

            try:
                from ..session import TradingSession
                sess = TradingSession.load(self.session_id)
            except Exception as e:  # pragma: no cover - defensive
                log.error("square_off_scheduler: failed to load session %s: %s",
                          self.session_id, e)
                return
            if sess is None:
                log.warning("square_off_scheduler: session %s not found at fire "
                            "time", self.session_id)
                return

            sess._build_brokers()
            sess.monitor.refresh_ltps(sess.brokers)
            gr = sess.monitor.compute_gross_return_invested()
            log.info("square_off_scheduler: SQUARING OFF %s (gross_return=%.4f)",
                     self.session_id, gr)
            # Single-fire guard so the precise-time square-off and the tick-driver
            # backstop (or a manual kill) can never double-fire.
            with fire_guard.claim_fire(self.session_id) as won:
                if not won:
                    log.info("square_off_scheduler: %s already firing — skipping",
                             self.session_id)
                    return
                try:
                    asyncio.run(sess.kill_switch.fire(
                        f"INTRADAY_BASKET SQUARE_OFF gross_return={gr:.4f}",
                        gross_return=gr, close_reason="SQUARE_OFF"))
                except Exception as e:  # pragma: no cover - never crash thread
                    log.exception("square_off_scheduler: fire failed for %s: %s",
                                  self.session_id, e)
        finally:
            self._deregister()
            log.info("square_off_scheduler stopped for %s", self.session_id)

    def _deregister(self) -> None:
        with _LOCK:
            if _SCHEDULERS.get(self.session_id) is self:
                _SCHEDULERS.pop(self.session_id, None)


def start_for_session(session_id: str, target_ist: datetime) -> bool:
    """Arm a square-off scheduler that fires at target_ist (IST-aware). Idempotent
    — returns False if one is already armed or autostart is disabled, True if a
    new scheduler was armed."""
    if not autostart_enabled():
        return False
    with _LOCK:
        existing = _SCHEDULERS.get(session_id)
        if existing is not None and existing.is_alive():
            return False
        sch = _Scheduler(session_id, target_ist)
        _SCHEDULERS[session_id] = sch
    sch.start()
    return True


def stop_for_session(session_id: str) -> None:
    """Request the session's square-off scheduler to stop (idempotent)."""
    with _LOCK:
        sch = _SCHEDULERS.get(session_id)
    if sch is not None:
        sch.stop()


def is_running(session_id: str) -> bool:
    with _LOCK:
        sch = _SCHEDULERS.get(session_id)
    return bool(sch and sch.is_alive())


def target_for_session(session_id: str) -> Optional[datetime]:
    with _LOCK:
        sch = _SCHEDULERS.get(session_id)
    return sch.target_ist if sch else None
