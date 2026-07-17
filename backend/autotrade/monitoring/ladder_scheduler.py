"""Process-wide LADDER daily-tick scheduler.

A single daemon thread that, once per trading day at/after the ladder open time
(default 09:15 IST), runs ladder.tick_all_running() — which opens ONE positional
basket per RUNNING ladder that has a free capital slice, refreshes the 5-day
downturn alert, and auto-completes month-end campaigns.

ALIGNED WAKE (2026-07-17 entry-latency fix): the loop sleeps TO the open target
rather than polling past it, so the day's tick fires at 09:15:00.000 regardless
of what second the process booted at. Previously the poll PHASE was the boot
second — a 07:44:15 restart entered the market at 09:15:15, and a restart 45s
later would have entered at 09:15:50. Entry time was a lottery decided by the
last restart. The 60s backstop poll is RETAINED for a missed target (machine
sleep, clock jump). See _next_wait_seconds.

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

# Floor on the interruptible sleep. Guards against a busy-spin if the wake lands
# a hair BEFORE the target (Event.wait may return marginally early): we'd compute
# a ~0s remainder and re-loop. Bounds any such spin to ~20 iterations/second for
# the few ms before the target, instead of a hot loop.
_MIN_WAIT_SECONDS = 0.05

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


def _next_wait_seconds(now: datetime, target: datetime) -> float:
    """How long to sleep before the next wake — the ALIGNED-WAKE rule.

    * BEFORE the target: sleep exactly the remainder, capped at the backstop
      poll. This is what walks the loop's phase onto the target: from any boot
      phase we keep taking 60s hops until the last hop is the exact remainder,
      so the final wake lands ON 09:15:00.000 — not on "whatever :ss the process
      happened to boot at". (Pre-fix, the phase WAS the boot second: a 07:44:15
      restart entered at 09:15:15; 45s later would have entered at 09:15:50.)
    * AT/AFTER the target: the plain backstop poll. `now >= target` stays true
      for the rest of the day, so this is what stops a busy-loop — the pre-fix
      code relied on the same unconditional poll sleep for that.
    """
    remaining = (target - now).total_seconds()
    if remaining > 0:
        return min(_POLL_SECONDS, max(_MIN_WAIT_SECONDS, remaining))
    return _POLL_SECONDS


def _run() -> None:
    from .. import ladder as _ladder
    from .. import trading_calendar as _cal
    from .. import nse_holiday_source as _nse
    hh, mm, ss = _parse_clock(_OPEN_CLOCK)
    log.info("ladder_scheduler started (open=%s poll=%.0fs)", _OPEN_CLOCK,
             _POLL_SECONDS)
    _last_holiday_refresh_date = None  # dedupe the daily NSE holiday refresh
    while not _stop.is_set():
        wait_s = _POLL_SECONDS
        try:
            now = _now()
            today = now.date()
            # DAILY NSE HOLIDAY REFRESH (real-money safety): once per calendar day
            # keep the self-updating holiday cache fresh so the trading calendar
            # stays authoritative for the current (and, near year-end, next) year.
            # Cheap when fresh (refresh_if_stale no-ops); wrapped so it can never
            # break the scheduler. Reuses this existing daily daemon = least-
            # invasive wiring (no new thread).
            if _last_holiday_refresh_date != today:
                try:
                    _nse.refresh_if_stale(max_age_days=7)
                    need = [today.year]
                    if (datetime(today.year, 12, 31, tzinfo=IST) - now).days <= 56:
                        need.append(today.year + 1)
                    uncovered = _nse.ensure_years_covered(need, max_age_days=7)
                    if uncovered:
                        log.error("NSE HOLIDAY COVERAGE GAP: years %s uncovered "
                                  "— add them to data/config/nse_holidays.txt",
                                  sorted(uncovered))
                except Exception as e:
                    log.warning("daily NSE holiday refresh failed: %s", e)
                _last_holiday_refresh_date = today
            target = now.replace(hour=hh, minute=mm, second=ss, microsecond=0)
            # Fire the day's tick once we're on a trading day, at/after the open
            # target. daily_tick is idempotent per ladder+day, so a re-check after
            # the tick is harmless.
            if _cal.is_trading_day(today) and now >= target:
                res = _ladder.tick_all_running(ref_now=now)
                if res.get("opened"):
                    log.info("ladder_scheduler: opened %d basket(s) at %s",
                             res.get("opened"), now.isoformat())
            # ALIGNED WAKE: recomputed from a FRESH `now` each iteration (so a
            # clock jump in either direction self-corrects on the next beat) and
            # AFTER the tick (so a slow tick doesn't overshoot the next target).
            wait_s = _next_wait_seconds(_now(), target)
        except Exception as e:  # never crash the daemon
            log.exception("ladder_scheduler tick failed: %s", e)
            wait_s = _POLL_SECONDS  # degrade to the plain backstop poll
        # Interruptible sleep: to the target when it's ahead, else the backstop.
        if _stop.wait(wait_s):
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
