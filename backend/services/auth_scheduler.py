"""Zerodha auto-auth scheduler — Layer 1 daemon.

Runs on a single background thread. Wakes every 30 min from 06:30 to 16:30 IST
on weekdays (21 cycles). Each attempt:
  1. Skip gate — proceed past it only if BOTH conditions fail:
       (a) falcon_auth_log records a success for today, AND
       (b) live Kite profile() call confirms the stored token actually works.
     This dual check (added 2026-05-27) catches mid-day token invalidations
     that the audit-log-only check used to mask.
  2. If we proceed: run zerodha_auto_auth.run_auth_attempt() (Playwright,
     async, in its own loop)
  3. Write result to falcon_auth_log
  4. On scheduled failure at/after 09:00 IST → fire Web Push (Layer 2).
     notify_auth_needed dedupes by date, so multiple cycle failures in one
     day produce ONE push, not 12.

Boot catch-up:
  On startup, if today is a weekday and any cycle time has passed but no
  valid token is established, run the latest-passed cycle's attempt
  immediately. Same defensive pattern as the 16:05 IST pipeline catch-up
  (falcon_gtm_reliability.md).

Production target: 99% of trading days, attempt #1 succeeds at 06:30 IST.
Remaining 1%: mid-day token invalidations recoverable within 30 min.

Schedule history:
  2026-05-12  initial 06:30/07:30/08:30/09:00 IST four-slot morning sequence
  2026-05-27  expanded to every-30-min 06:30–16:30 IST after a mid-day
              token invalidation between 09:00 and 16:05 IST left the EOD
              pipeline tokenless with no in-system recovery path.

Why 30-min cadence not every 5 min:
  Each cycle's skip path costs ~one Kite profile() call (200-500ms). 30-min
  cadence × 21 slots = ~10 seconds total profile() time per day, comfortably
  under Kite's quota AND fast enough to recover before the 16:05 EOD run.
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
#
# 2026-05-27 expansion: the original 4-slot morning sequence (06:30/07:30/
# 08:30/09:00 IST) left a 21-hour blind spot covering the entire trading
# day. If Zerodha invalidated the access_token mid-session (which they do,
# unpredictably, on suspected anomalies / IP changes / security events),
# nothing in the system could recover the token until 06:30 next morning —
# and the 16:05 IST EOD pipeline ran into a brick wall.
#
# New schedule: every 30 min from 06:30 IST to 16:30 IST = 21 cycles.
# Cost: after the first success of the day, all subsequent cycles either
#   (a) skip cheaply (~one Kite profile() call, <500ms) if today_already_
#       succeeded AND the live token health check passes, OR
#   (b) actually run an auth attempt if either gate fails (i.e. mid-day
#       token invalidation detected).
# This bounds mid-day token-breakage recovery to ≤30 min — comfortably
# before the 16:05 EOD pipeline tries to use it.
def _build_cycles() -> List[Tuple[str, Tuple[int, int, int]]]:
    out: List[Tuple[str, Tuple[int, int, int]]] = []
    for total_min in range(6 * 60 + 30, 16 * 60 + 31, 30):  # 06:30 → 16:30 step 30
        h, m = divmod(total_min, 60)
        out.append((f"{h:02d}{m:02d}", (h, m, 0)))
    return out

CYCLES: List[Tuple[str, Tuple[int, int, int]]] = _build_cycles()

# Push-notification trigger boundary: cycles at/after this time that fail
# (either initially OR because a previously-OK token went bad) trigger the
# Layer 2 admin push. Before this boundary, we silently let the morning
# sequence keep trying — push spam is worse than morning quiet.
# notify_auth_needed is internally idempotent (one magic-link per day).
_PUSH_TRIGGER_TIME = (9, 0)  # 09:00 IST


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


def _live_token_is_healthy() -> bool:
    """Live Kite API check — calls KiteConnect.profile() with the currently
    stored access_token. Returns True iff Kite accepts the token RIGHT NOW.

    Why this matters (2026-05-27 incident): falcon_auth_log can have a
    'success' row for today's morning attempt, but Kite can invalidate the
    token mid-session for reasons we can't predict (anomaly detection, IP
    change, security event). The auth_log says "we got a token", but the
    token is dead. Without a live check, every subsequent cycle SKIPs with
    ALREADY_OK and the system stays broken until tomorrow morning.

    Cost: one Kite profile() call per skip path (~200-500ms). At 21 cycles
    /day with 20 skips, that's ≤10s of total profile() time per day — well
    under Kite's 200K req/day quota and the 3 req/sec rate limit.

    Errors are conservative: any exception or non-valid response → False
    (treat as "needs refresh"). Better to refresh once unnecessarily than
    miss a real invalidation.
    """
    try:
        from services.kite_auth import get_token_status     # noqa: WPS433
        s = get_token_status()
        return bool(s.get("valid"))
    except Exception as e:
        log.warning("auth_scheduler: live token health check failed (%s) — "
                    "treating as unhealthy", e)
        return False


async def _run_attempt_async(cycle_name: str, trigger_kind: str = "scheduled") -> None:
    """Execute one auth attempt, log it, fire push on failure (post-09:00)."""
    from .zerodha_auto_auth import (
        run_auth_attempt, log_attempt, today_already_succeeded
    )
    from power_user.config import POWER_DB_PATH    # noqa: WPS433

    attempt_n = _attempt_number(cycle_name) if trigger_kind == "scheduled" else 0

    # ── PRE-FLIGHT GATE (2026-05-29 fix) ─────────────────────────────────
    # If Playwright is known-broken, REFUSE to run a doomed cycle. The
    # 2026-05-29 incident burned 21 consecutive 30-sec failures because
    # each cycle blindly tried Playwright. With preflight, the first failure
    # transitions us to BROKEN state and fires immediate Web Push; further
    # cycles skip until preflight self-heals or the operator runs
    # scripts\repair_playwright.bat. We still re-check preflight hourly
    # in case the env recovers on its own.
    if trigger_kind == "scheduled":
        try:
            from .playwright_preflight import is_broken, check_now, get_health
            health = get_health()
            recheck_due = (not health.checked_at) or (
                health.next_recheck_at and _now_ist().isoformat() >= health.next_recheck_at
            )
            if recheck_due:
                # Run check inline (subprocess capped at 20s)
                health = await asyncio.to_thread(check_now, fire_push_on_break=True)
            if not health.is_healthy:
                log.warning("auth_scheduler[%s]: SKIPPING — Playwright broken "
                              "(class=%s, next recheck %s)",
                              cycle_name, health.failure_class, health.next_recheck_at)
                from .zerodha_auto_auth import AuthAttemptResult
                skip = AuthAttemptResult(
                    status="skipped", stage=None,
                    error_code=f"PLAYWRIGHT_{health.failure_class}",
                    error_detail=(health.error_summary or "")[:300],
                    token_preview=None, elapsed_ms=0,
                )
                log_attempt(POWER_DB_PATH, attempt_n, trigger_kind, skip)
                return
        except Exception as e:
            log.warning("auth_scheduler[%s]: preflight check itself failed "
                          "(%s) — proceeding optimistically", cycle_name, e)

    # Skip gate — must satisfy BOTH:
    #   (1) audit log says today already succeeded, AND
    #   (2) the stored token actually works against Kite RIGHT NOW.
    # Splitting these catches mid-day invalidations that the log alone would
    # have masked (2026-05-27 incident).
    if trigger_kind == "scheduled" and today_already_succeeded(POWER_DB_PATH):
        if _live_token_is_healthy():
            log.info("auth_scheduler[%s]: today already succeeded AND token "
                      "healthy — skipping", cycle_name)
            from .zerodha_auto_auth import AuthAttemptResult
            skip = AuthAttemptResult(
                status="skipped", stage=None,
                error_code="ALREADY_OK",
                error_detail="today's token already refreshed and live check passed",
                token_preview=None, elapsed_ms=0,
            )
            log_attempt(POWER_DB_PATH, attempt_n, trigger_kind, skip)
            return
        log.warning("auth_scheduler[%s]: log says success today but live token "
                     "FAILED Kite profile() — refreshing", cycle_name)
        # Fall through and run a real refresh.

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

    # Layer 2 push trigger: with 21 cycles spanning the trading day, firing
    # only on the LAST attempt (16:30) would push AFTER the 16:05 EOD pipeline
    # has already failed. Instead, fire on ANY scheduled failure at/after
    # 09:00 IST. notify_auth_needed dedupes per-day so we won't spam.
    if trigger_kind == "scheduled" and result.status == "failed":
        cycle_hh_mm = next(((hh, mm) for name, (hh, mm, _ss) in CYCLES
                             if name == cycle_name), None)
        if cycle_hh_mm is not None and cycle_hh_mm >= _PUSH_TRIGGER_TIME:
            try:
                from power_user.services.web_push import notify_auth_needed
                await asyncio.to_thread(notify_auth_needed)
            except Exception as e:
                log.exception("auth_scheduler: web_push notify failed "
                              "(non-fatal): %s", e)


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
