"""Live tier scheduler — daemon thread driving the 3 daily cycles.

Mirrors the architecture of `backend/falcon/trade/services/premarket_deployer.py`:
single global lock, IST-aware scheduling, weekday gate, graceful shutdown via
threading.Event, restart-idempotent (UPSERT on (entry_date, cycle, rank)).

Schedule (operator policy 2026-05-14):
  09:30:30 IST — first cycle, evaluates all 100 ranks
  09:45:00 IST — re-evaluates ONLY rows that were WAIT at 0930
  10:00:00 IST — re-evaluates ONLY rows that were WAIT at 0945 / 0930
  Off-hours / weekends → thread sleeps until next valid window

Boot-time catch-up: on backend restart, if today is a weekday and a cycle's
fire time has passed but no rows exist for that cycle, run it now. This is
the structural fix for the "16:05 cron skipped on restart" class of bugs
the operator hit on 2026-05-13 (see falcon_gtm_reliability.md).

Failure modes:
  * Kite client unavailable → log, mark all rows action=WAIT reason=kite_unavailable,
    cycle is recorded as ran (NOT silently skipped — next cycle will retry)
  * Per-symbol Kite error → that row WAIT, never poisons the rest of the cycle
  * Lock contention (multiple instances) → second instance fails the
    non-blocking acquire and exits cleanly, no double-fire
  * SIGTERM mid-cycle → BEGIN IMMEDIATE transactions roll back uncommitted work;
    next start picks up via boot-time catch-up
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from .live_tier import CYCLES, run_cycle

log = logging.getLogger("kanida.power_user.scheduler")
IST = timezone(timedelta(hours=5, minutes=30))

# Single global lock — prevents two scheduler threads (e.g. accidental double-
# start) from racing on the same cycle.
_scheduler_lock = threading.Lock()
_stop_event = threading.Event()
_thread: Optional[threading.Thread] = None

_state: Dict[str, Any] = {
    "started":         False,
    "started_at":      None,    # IST ISO
    "last_cycle_at":   None,
    "last_cycle_name": None,
    "last_summary":    None,
    "last_error":      None,
}


# ──────────────────────────────────────────────────────────────────────────
#  Public API — start / stop / status
# ──────────────────────────────────────────────────────────────────────────

def start() -> bool:
    """Launch the daemon thread once. Idempotent.

    Returns True if started fresh, False if already running.
    """
    global _thread
    if _thread is not None and _thread.is_alive():
        log.info("scheduler: already running — start() is no-op")
        return False
    _stop_event.clear()
    _thread = threading.Thread(target=_run, name="power-live-scheduler", daemon=True)
    _thread.start()
    _state["started"]    = True
    _state["started_at"] = _now_ist().isoformat()
    log.info("scheduler: thread launched")
    return True


def stop(timeout: float = 5.0) -> None:
    """Signal the loop to exit on the next iteration. Idempotent.

    Note: a cycle currently in flight will COMPLETE before checking the stop
    event — we don't want to leave half-committed rows. Worst case, we wait
    up to ~60s for the cycle to finish.
    """
    _stop_event.set()
    if _thread and _thread.is_alive():
        _thread.join(timeout=timeout)


def status() -> Dict[str, Any]:
    """Snapshot of the scheduler's state for /admin observability."""
    return dict(_state)


# ──────────────────────────────────────────────────────────────────────────
#  Loop internals
# ──────────────────────────────────────────────────────────────────────────

def _now_ist() -> datetime:
    return datetime.now(IST)


def _is_market_weekday(now: Optional[datetime] = None) -> bool:
    """Mon-Fri only. Holiday detection is implicit via Kite returning no bars
    on the day (those cycles complete with all WAIT/kite_unavailable rows)."""
    n = now or _now_ist()
    return n.weekday() < 5


def _today_iso() -> str:
    return _now_ist().date().isoformat()


def _cycle_already_ran(con, cycle_name: str, today_iso: str) -> bool:
    """Has this cycle already written rows for today? Restart-idempotency check."""
    row = con.execute(
        "SELECT 1 FROM falcon_live_decisions "
        "WHERE entry_date = ? AND cycle = ? LIMIT 1",
        (today_iso, cycle_name)
    ).fetchone()
    return row is not None


def _seconds_until_next_target(now: datetime, cycle_time: tuple) -> float:
    """How many seconds until the next occurrence of (hh, mm, ss) IST?"""
    hh, mm, ss = cycle_time
    target = now.replace(hour=hh, minute=mm, second=ss, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    # Skip weekends
    while target.weekday() >= 5:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def _next_cycle(now: datetime) -> Optional[tuple]:
    """Return (cycle_name, target_dt) for the very next cycle. None on weekend."""
    # If non-weekday, find next Monday's first cycle
    while True:
        if _is_market_weekday(now):
            for name, (hh, mm, ss) in CYCLES:
                t = now.replace(hour=hh, minute=mm, second=ss, microsecond=0)
                if t > now:
                    return (name, t)
        # All cycles for today have passed (or it's a weekend) — bump to tomorrow
        now = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        if (now - _now_ist()).total_seconds() > 7 * 86400:  # safety: never loop more than a week
            return None


def _run_cycle_guarded(cycle_name: str) -> Dict[str, Any]:
    """Wrap live_tier.run_cycle in a try/except. Never raises to the loop."""
    try:
        summary = run_cycle(cycle_name)
        _state["last_summary"] = summary
        _state["last_error"]   = None
        return summary
    except Exception as e:
        _state["last_error"] = f"{cycle_name}: {e}"
        log.exception("scheduler: run_cycle(%s) crashed", cycle_name)
        return {"cycle": cycle_name, "error": str(e)[:200], "crashed": True}


def _boot_catchup() -> None:
    """On boot, if today is a weekday and a cycle's fire time has passed but
    no rows exist for that cycle, run it now. Same structural pattern as
    backend/main.py boot catch-up for the 16:05 IST pipeline cron."""
    import sqlite3
    from ..config import POWER_DB_PATH
    if not _is_market_weekday():
        log.info("scheduler: boot catch-up skipped (weekend)")
        return
    now = _now_ist()
    today = _today_iso()
    try:
        with sqlite3.connect(POWER_DB_PATH, timeout=30.0) as con:
            for name, (hh, mm, ss) in CYCLES:
                cycle_dt = now.replace(hour=hh, minute=mm, second=ss, microsecond=0)
                if now < cycle_dt:
                    continue  # not yet time
                if _cycle_already_ran(con, name, today):
                    continue
                log.warning("scheduler: boot catch-up — cycle %s missed (past %s IST), "
                             "running now async", name, cycle_dt.strftime("%H:%M:%S"))
                summary = _run_cycle_guarded(name)
                _state["last_cycle_at"]   = _now_ist().isoformat()
                _state["last_cycle_name"] = name
                log.info("scheduler: catch-up %s done: %s", name, summary)
    except Exception as e:
        log.exception("scheduler: boot catch-up crashed (non-fatal): %s", e)


def _run() -> None:
    """Daemon loop. Sleeps until next cycle, runs it, repeats."""
    if not _scheduler_lock.acquire(blocking=False):
        log.warning("scheduler: another instance holds the lock — exiting")
        return
    try:
        _boot_catchup()
        while not _stop_event.is_set():
            try:
                now = _now_ist()
                nxt = _next_cycle(now)
                if nxt is None:
                    log.warning("scheduler: no next cycle in 7 days — exiting")
                    return
                cycle_name, target_dt = nxt
                wait_sec = (target_dt - now).total_seconds()
                if wait_sec > 1.0:
                    log.info("scheduler: next cycle %s at %s IST (%.0f min)",
                             cycle_name, target_dt.strftime("%Y-%m-%d %H:%M:%S"),
                             wait_sec / 60.0)
                    if _stop_event.wait(wait_sec):
                        return  # stop signal received
                # Run the cycle
                log.info("scheduler: running cycle %s", cycle_name)
                summary = _run_cycle_guarded(cycle_name)
                _state["last_cycle_at"]   = _now_ist().isoformat()
                _state["last_cycle_name"] = cycle_name
                log.info("scheduler: cycle %s complete: %s", cycle_name, summary)
                # Sleep 2 seconds to avoid re-triggering the same cycle on a clock fuzz
                if _stop_event.wait(2.0):
                    return
            except Exception as e:
                _state["last_error"] = str(e)[:200]
                log.exception("scheduler: loop crashed, retrying in 60s: %s", e)
                if _stop_event.wait(60.0):
                    return
    finally:
        _scheduler_lock.release()
        log.info("scheduler: thread exiting")
