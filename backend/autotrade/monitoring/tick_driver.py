"""Per-session background tick driver.

A lightweight daemon thread per RUNNING session that, on a fixed interval:
  1. refreshes every open position's ltp (broker LTP → ohlc close → entry),
  2. recomputes gross_return + writes a portfolio snapshot,
  3. calls the kill switch check_threshold, and FIRES it if the threshold is
     crossed — so the kill switch actually AUTO-fires in production, not only
     when an external caller happens to poll tick().

SAFETY:
  * Paper sessions place NO real orders (brokers built dry_run=True). Live exits
    are still gated by FALCON_AUTOTRADE_ENABLED + mode='live' at the broker layer.
  * Idempotent: start_for_session() is a no-op if a driver is already running
    for that session_id (single registry guarded by a lock).
  * Self-stopping: the loop exits as soon as the session status is no longer
    RUNNING (kill switch sets CLOSED), and stop_for_session() requests a stop.
  * Each tick runs the async session.tick() in a fresh event loop; exceptions
    are logged and never kill the thread (defence in depth around live money).
"""
from __future__ import annotations

import asyncio
import logging
import os
import threading
from typing import Dict, Optional

from falcon.db import falcon_conn

log = logging.getLogger("kanida.autotrade.tick_driver")

DEFAULT_INTERVAL_SEC = 5.0

# Global autostart guard. Production leaves this on. Tests that drive tick()
# manually flip it off (so a background thread can't race the assertions); the
# dedicated auto-fire test flips it back on for its scope.
_AUTOSTART = True


def set_autostart(enabled: bool) -> None:
    """Enable/disable automatic driver start from session.start(). For tests."""
    global _AUTOSTART
    _AUTOSTART = bool(enabled)


def autostart_enabled() -> bool:
    if os.environ.get("FALCON_AUTOTRADE_TICK_DISABLE") == "1":
        return False
    return _AUTOSTART


# session_id -> _Driver
_DRIVERS: Dict[str, "_Driver"] = {}
_LOCK = threading.Lock()


def _session_status(session_id: str) -> Optional[str]:
    with falcon_conn() as con:
        row = con.execute(
            "SELECT status FROM autotrade_sessions WHERE session_id=?",
            (session_id,),
        ).fetchone()
    return row["status"] if row else None


class _Driver:
    def __init__(self, session_id: str, interval_sec: float):
        self.session_id = session_id
        self.interval_sec = interval_sec
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name=f"autotrade-tick-{session_id[:8]}", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def _run(self) -> None:
        # Build the session ONCE (brokers, kill switch) and reuse across ticks.
        try:
            from ..session import TradingSession
            sess = TradingSession.load(self.session_id)
        except Exception as e:  # pragma: no cover - defensive
            log.error("tick_driver: failed to load session %s: %s", self.session_id, e)
            self._deregister()
            return
        if sess is None:
            log.warning("tick_driver: session %s not found, exiting", self.session_id)
            self._deregister()
            return

        log.info("tick_driver started for %s (interval=%ss)",
                 self.session_id, self.interval_sec)
        try:
            while not self._stop.is_set():
                status = _session_status(self.session_id)
                if status != "RUNNING":
                    log.info("tick_driver: session %s status=%s — stopping",
                             self.session_id, status)
                    break
                try:
                    out = asyncio.run(sess.tick())
                    if out.get("kill_switch_fired"):
                        log.critical("tick_driver: kill switch AUTO-fired for %s (%s)",
                                     self.session_id, out.get("kill_reason"))
                        break  # session now CLOSED; nothing left to tick
                except Exception as e:  # pragma: no cover - never kill the thread
                    log.exception("tick_driver: tick error for %s: %s",
                                  self.session_id, e)
                # Interruptible sleep.
                self._stop.wait(self.interval_sec)
        finally:
            self._deregister()
            log.info("tick_driver stopped for %s", self.session_id)

    def _deregister(self) -> None:
        with _LOCK:
            if _DRIVERS.get(self.session_id) is self:
                _DRIVERS.pop(self.session_id, None)


def start_for_session(session_id: str,
                      interval_sec: float = DEFAULT_INTERVAL_SEC) -> bool:
    """Start a tick driver for session_id. Idempotent — returns False if one is
    already running or autostart is disabled. Returns True if a new driver was
    started."""
    if not autostart_enabled():
        return False
    env_interval = os.environ.get("FALCON_AUTOTRADE_TICK_INTERVAL")
    if env_interval:
        try:
            interval_sec = float(env_interval)
        except ValueError:
            pass
    with _LOCK:
        existing = _DRIVERS.get(session_id)
        if existing is not None and existing.is_alive():
            return False
        drv = _Driver(session_id, interval_sec)
        _DRIVERS[session_id] = drv
    drv.start()
    return True


def stop_for_session(session_id: str) -> None:
    """Request the session's tick driver to stop (idempotent)."""
    with _LOCK:
        drv = _DRIVERS.get(session_id)
    if drv is not None:
        drv.stop()


def is_running(session_id: str) -> bool:
    with _LOCK:
        drv = _DRIVERS.get(session_id)
    return bool(drv and drv.is_alive())
