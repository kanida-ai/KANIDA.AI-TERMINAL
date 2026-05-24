"""Zerodha auto-auth scheduler — Layer 1 daemon.

Runs on a single background thread. Wakes at 06:30 / 07:30 / 08:30 / 09:00 IST
weekdays. Each attempt:
  1. Check falcon_auth_log: is there a success entry for today already? → skip
  2. Run zerodha_auto_auth.run_auth_attempt() (async, in its own loop)
  3. Write result to falcon_auth_log
  4. On final attempt failure (4/4) → fire Web Push notification (Layer 2)

Boot catch-up:
  On startup, if today is a weekday and any cycle time has passed but no
  successful attempt is recorded, run the latest-passed cycle's attempt
  immediately. Same structural fix as the 16:05 IST pipeline catch-up
  (falcon_gtm_reliability.md).

Production target: 99% of trading days, attempt #1 succeeds at 06:30 IST.

Why 4 morning cycles instead of "every 6 hours":
  Zerodha tokens are issued for the calendar day in IST and expire at the
  next 06:00 IST. We MUST land a token between 06:00 and 09:15 IST. Spacing
  attempts hourly inside that window covers transient Zerodha outages.
"""
from __future__ import annotations

import asyncio
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("kanida.services.auth_scheduler")
IST = timezone(timedelta(hours=5, minutes=30))

# (cycle_name, (hour, minute, second)) IST.
CYCLES: List[Tuple[str, Tuple[int, int, int]]] = [
    ("0630", (6, 30, 0)),
    ("0730", (7, 30, 0)),
    ("0830", (8, 30, 0)),
    ("0900", (9,  0, 0)),
]


# ──────────────────────────────────────────────────────────────────────────
# State + lock
# ──────────────────────────────────────────────────────────────────────────

_lock = threading.Lock()
_stop = threading.Event()
_thread: Optional[threading.Thread] = None

_state: Dict[str, Any] = {
    "started":           False,
    "started_at":        None,
    "last_attempt_at":   None,
    "last_attempt_name": None,
    "last_result":       None,
    "last_error":        None,
    "next_attempt_at":   None,
}


# ──────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────

def start() -> bool:
    """Launch the daemon thread once. Returns True if started fresh."""
    global _thread
    if _thread is not None and _thread.is_alive():
        return False
    _stop.clear()
    _thread = threading.Thread(target=_run, name="zerodha-auth-scheduler", daemon=True)
    _thread.start()
    _state["started"]    = True
    _state["started_at"] = _now_ist().isoformat()
    log.info("auth_scheduler: thread launched")
    return True


def stop(timeout: float = 5.0) -> None:
    _stop.set()
    if _thread and _thread.is_alive():
        _thread.join(timeout=timeout)


def status() -> Dict[str, Any]:
    return dict(_state)


# ──────────────────────────────────────────────────────────────────────────
# Loop internals
# ──────────────────────────────────────────────────────────────────────────

def _now_ist() -> datetime:
    return datetime.now(IST)


def _is_weekday(now: Optional[datetime] = None) -> bool:
    return (now or _now_ist()).weekday() < 5


def _attempt_number(cycle_name: str) -> int:
    """0630 -> 1, 0730 -> 2, 0830 -> 3, 0900 -> 4."""
    return [c[0] for c in CYCLES].index(cycle_name) + 1


def _next_cycle(now: datetime) -> Optional[Tuple[str, datetime]]:
    """Find the very next cycle time (today or future weekday). None if >7d out."""
    # Look in the next 7 days for the next valid cycle slot.
    for _ in range(8):
        if _is_weekday(now):
            for name, (hh, mm, ss) in CYCLES:
                target = now.replace(hour=hh, minute=mm, second=ss, microsecond=0)
                if target > now:
                    return (name, target)
        now = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return None


async def _run_attempt_async(cycle_name: str, trigger_kind: str = "scheduled") -> None:
    """Execute one auth attempt, log it, fire push on final failure."""
    from .zerodha_auto_auth import (
        run_auth_attempt, log_attempt, today_already_succeeded
    )
    from power_user.config import POWER_DB_PATH    # noqa: WPS433

    attempt_n = _attempt_number(cycle_name) if trigger_kind == "scheduled" else 0

    if trigger_kind == "scheduled" and today_already_succeeded(POWER_DB_PATH):
        log.info("auth_scheduler[%s]: today already succeeded — skipping", cycle_name)
        # We still log the skip so the audit trail is complete
        from .zerodha_auto_auth import AuthAttemptResult
        skip = AuthAttemptResult(
            status="skipped", stage=None,
            error_code="ALREADY_OK",
            error_detail="today's token already refreshed; later attempt unnecessary",
            token_preview=None, elapsed_ms=0,
        )
        log_attempt(POWER_DB_PATH, attempt_n, trigger_kind, skip)
        return

    result = await run_auth_attempt(
        attempt_of_day=attempt_n,
        trigger_kind=trigger_kind,
    )
    log_attempt(POWER_DB_PATH, attempt_n, trigger_kind, result)

    _state["last_attempt_at"]   = _now_ist().isoformat()
    _state["last_attempt_name"] = cycle_name
    _state["last_result"]       = {
        "status":       result.status,
        "stage":        result.stage,
        "error_code":   result.error_code,
        "token_preview": result.token_preview,
    }
    _state["last_error"] = result.error_code if result.status == "failed" else None

    log.info("auth_scheduler[%s]: status=%s stage=%s code=%s elapsed=%dms",
              cycle_name, result.status, result.stage,
              result.error_code, result.elapsed_ms)

    # Layer 2 trigger: if this is the LAST scheduled attempt and we failed,
    # fire a Web Push to the admin.
    if (trigger_kind == "scheduled" and result.status == "failed"
            and cycle_name == CYCLES[-1][0]):
        try:
            from power_user.services.web_push import notify_auth_needed
            await asyncio.to_thread(notify_auth_needed)
        except Exception as e:
            log.exception("auth_scheduler: web_push notify failed (non-fatal): %s", e)


def _run_attempt_sync(cycle_name: str, trigger_kind: str = "scheduled") -> None:
    """Sync wrapper: run the async attempt in its own event loop. Daemon thread
    can't use FastAPI's loop, so we own one here."""
    try:
        asyncio.run(_run_attempt_async(cycle_name, trigger_kind))
    except Exception as e:
        log.exception("auth_scheduler: attempt %s crashed (non-fatal): %s", cycle_name, e)


def _boot_catchup() -> None:
    """On boot, check if today's cycles were missed (e.g. backend was down).
    If a cycle time has passed and no successful attempt is logged yet, run
    the latest-passed cycle immediately. Same defensive pattern as the
    16:05 IST pipeline (see falcon_gtm_reliability.md)."""
    if not _is_weekday():
        return
    try:
        from .zerodha_auto_auth import today_already_succeeded
        from power_user.config import POWER_DB_PATH

        if today_already_succeeded(POWER_DB_PATH):
            log.info("auth_scheduler: boot catch-up skipped — today already succeeded")
            return

        now = _now_ist()
        # Find the latest cycle whose time has passed today
        passed = [name for name, (hh, mm, ss) in CYCLES
                  if now >= now.replace(hour=hh, minute=mm, second=ss, microsecond=0)]
        if not passed:
            return

        # Run the LATEST passed cycle (not all of them — we only need one success)
        catch_cycle = passed[-1]
        log.warning("auth_scheduler: boot catch-up — %s missed, running now async",
                    catch_cycle)
        _run_attempt_sync(catch_cycle, trigger_kind="scheduled")
    except Exception as e:
        log.exception("auth_scheduler: boot catch-up crashed (non-fatal): %s", e)


def _run() -> None:
    """Daemon loop. Sleeps to next cycle, runs it, repeats."""
    if not _lock.acquire(blocking=False):
        log.warning("auth_scheduler: another instance holds the lock — exiting")
        return
    try:
        _boot_catchup()
        while not _stop.is_set():
            try:
                now = _now_ist()
                nxt = _next_cycle(now)
                if nxt is None:
                    log.warning("auth_scheduler: no next cycle found — exiting")
                    return
                cycle_name, target_dt = nxt
                _state["next_attempt_at"] = target_dt.isoformat()
                wait_sec = (target_dt - now).total_seconds()
                if wait_sec > 1.0:
                    log.info("auth_scheduler: next attempt %s at %s IST (%.0f min)",
                              cycle_name, target_dt.strftime("%Y-%m-%d %H:%M"),
                              wait_sec / 60.0)
                    if _stop.wait(wait_sec):
                        return
                _run_attempt_sync(cycle_name, trigger_kind="scheduled")
                # Sleep ~10s to avoid re-triggering on clock fuzz
                if _stop.wait(10.0):
                    return
            except Exception as e:
                _state["last_error"] = str(e)[:200]
                log.exception("auth_scheduler: loop crashed, retrying in 60s")
                if _stop.wait(60.0):
                    return
    finally:
        _lock.release()
        log.info("auth_scheduler: thread exiting")


# ──────────────────────────────────────────────────────────────────────────
# Manual trigger (admin override) — exposed via API
# ──────────────────────────────────────────────────────────────────────────

def trigger_manual(trigger_kind: str = "manual") -> Dict[str, Any]:
    """Operator clicked 'refresh now' in /power/admin. Runs an attempt in
    the daemon's loop pattern but flagged trigger_kind='manual' so the audit
    log distinguishes it from scheduled cycles."""
    valid = ("manual", "magic_link")
    if trigger_kind not in valid:
        raise ValueError(f"trigger_kind must be one of {valid}")
    threading.Thread(
        target=_run_attempt_sync,
        args=("0000", trigger_kind),    # '0000' is the synthetic name for manual
        daemon=True, name="auth-manual-trigger",
    ).start()
    return {"triggered": True, "trigger_kind": trigger_kind}
