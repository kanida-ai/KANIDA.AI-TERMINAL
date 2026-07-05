"""Process-wide LADDER daily-tick scheduler.

A single daemon thread that, once per trading day at/after the ladder open time
(default 09:15 IST), runs ladder.tick_all_running() — which opens ONE positional
basket per RUNNING ladder that has a free capital slice, refreshes the 5-day
downturn alert, and auto-completes month-end campaigns.

RESTART-DURABLE by construction (mirrors the session drivers):
  * There is NO per-ladder in-memory timer to lose. The loop derives everything
    from the persisted autotrade_ladders rows every wake.
  * daily_tick() is IDEMPOTENT per ladder+day (last_tick_date guard), so a
    restart mid-day, a double wake, or the boot resume + the scheduled fire on
    the same day all converge to "at most one basket per ladder per day".
  * On boot, ladder.resume_active_ladders() (wired in main.py) runs one tick
    immediately so a campaign that came up after 09:15 still opens today's basket;
    this scheduler then handles every SUBSEQUENT trading day.

SAFETY: children default to their ladder's mode (paper unless the operator chose
live). Live children stay gated by FALCON_AUTOTRADE_ENABLED at the broker layer.
The loop is fully wrapped — it can never crash the backend.
"""
from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional

log = logging.getLogger("kanida.autotrade.ladder_scheduler")

IST = timezone(timedelta(hours=5, minutes=30))

# When (IST clock) the daily tick runs. 09:15 = market open. Env-overridable so
# ops can shift it without a code change; the tick is idempotent so an exact
# alignment isn't required.
_OPEN_CLOCK = os.environ.get("FALCON_LADDER_OPEN_TIME", "09:15:00")

# Backstop poll: even without a precise wake, re-check at this cadence so a
# missed target (sleep, clock jump) still fires the day's tick.
_POLL_SECONDS = float(os.environ.get("FALCON_LADDER_POLL_SECONDS", "60"))

_thread: Optional[threading.Thread] = None
_stop = threading.Event()
_lock = threading.Lock()
_autostart = True  # tests set False so the daemon can't race a shared temp DB


def set_autostart(enabled: bool) -> None:
    global _autostart
    _autostart = bool(enabled)


def _parse_clock(clock: str) -> tuple:
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            t = datetime.strptime(clock, fmt)
            return (t.hour, t.minute, t.second)
        except ValueError:
            continue
    return (9, 15, 0)


def _now() -> datetime:
    return datetime.now(IST)


def _run() -> None:
    from .. import ladder as _ladder
    from .. import trading_calendar as _cal
    hh, mm, ss = _parse_clock(_OPEN_CLOCK)
    log.info("ladder_scheduler started (open=%s poll=%.0fs)", _OPEN_CLOCK,
             _POLL_SECONDS)
    while not _stop.is_set():
        try:
            now = _now()
            today = now.date()
            target = now.replace(hour=hh, minute=mm, second=ss, microsecond=0)
            # Fire the day's tick once we're on a trading day, at/after the open
            # target. daily_tick is idempotent per ladder+day, so a re-check after
            # the tick is harmless.
            if _cal.is_trading_day(today) and now >= target:
                res = _ladder.tick_all_running(ref_now=now)
                if res.get("opened"):
                    log.info("ladder_scheduler: opened %d basket(s) at %s",
                             res.get("opened"), now.isoformat())
        except Exception as e:  # never crash the daemon
            log.exception("ladder_scheduler tick failed: %s", e)
        # Interruptible backstop sleep.
        if _stop.wait(_POLL_SECONDS):
            break
    log.info("ladder_scheduler stopped")


def start() -> bool:
    """Arm the process-wide ladder scheduler (idempotent). Returns True if a new
    thread was started, False if one is already running or autostart is off."""
    global _thread
    if not _autostart:
        return False
    with _lock:
        if _thread is not None and _thread.is_alive():
            return False
        _stop.clear()
        _thread = threading.Thread(target=_run, name="autotrade-ladder-sched",
                                   daemon=True)
        _thread.start()
    return True


def stop() -> None:
    _stop.set()


def is_running() -> bool:
    with _lock:
        return bool(_thread is not None and _thread.is_alive())
