"""Standalone Zerodha auth worker — Fix 1 (2026-06-02).

WHY THIS EXISTS — the 9-incident root cause:
  The auth bot used to run INSIDE the long-lived FastAPI backend process.
  That process survives for days on a laptop that sleeps. After enough
  sleep/wake cycles the aged process loses the ability to spawn Playwright's
  Chromium subprocess — os.path.exists() returns phantom False, the launch
  fails with "Executable doesn't exist" — even though the binary, env vars,
  and permissions are all fine. A FRESHLY started process on the same
  machine always works. Proven empirically 2026-06-02:
      aged backend (36h old): launch FAILS
      fresh python probe:     launch OK, same machine, same files

THE FIX — run auth in a fresh short-lived process every time.
  Windows Task Scheduler invokes this script every 30 min (06:30-16:30 IST,
  weekdays) via run_auth_worker.bat. Each invocation is a brand-new process
  that:
    1. Loads config/.env
    2. Skips if today's token is already valid (cheap live Kite check)
    3. Otherwise runs ONE Playwright login → exchanges → writes token to DB
    4. Logs the attempt to falcon_auth_log (admin panel reads this)
    5. Fires Web Push on failure at/after 09:00 IST
    6. Exits (0 = token healthy, 1 = failed)
  Because the process is always fresh, it physically cannot hit the
  aged-process pathology. The backend no longer runs auth — it just READS
  the token from the DB.

This reuses the SAME tested primitives the old in-backend scheduler used
(run_auth_attempt, log_attempt, today_already_succeeded) so behaviour is
identical — only the EXECUTION CONTEXT changed (fresh process vs aged).
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

# ── Make the backend package importable ──────────────────────────────────
# This script lives in scripts/; the backend package is in backend/. Insert
# backend/ on sys.path so `from services... import` and `from power_user...`
# resolve exactly as they do when uvicorn runs with cwd=backend.
_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT = os.path.dirname(_HERE)
_BACKEND = os.path.join(_PROJECT, "backend")
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

IST = timezone(timedelta(hours=5, minutes=30))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("kanida.auth_worker")

# 09:00 IST — failures at/after this fire the Layer-2 push (matches old
# scheduler behaviour). Before this we stay quiet (morning retries are normal).
_PUSH_TRIGGER_HOUR = 9

# IST active window. The task scheduler fires this worker on a simple local-time
# interval (no fragile PDT↔IST↔DST math in the task definition); the worker
# self-gates to the window where auth actually matters:
#   - Zerodha tokens expire at 06:00 IST daily.
#   - The EOD pipeline needs a valid token by 16:05 IST.
# So we act between 06:00 and 16:30 IST on weekdays. Outside this window the
# worker exits in <1 sec without touching Playwright. On-demand refresh
# outside the window still works via the /power/admin "Refresh token now"
# button (that path runs in the backend, not here).
_WINDOW_START = (6, 0)    # 06:00 IST
_WINDOW_END   = (16, 30)  # 16:30 IST


def _now_ist() -> datetime:
    return datetime.now(IST)


def _in_active_window(now: datetime) -> bool:
    if now.weekday() >= 5:           # Sat/Sun
        return False
    hm = (now.hour, now.minute)
    return _WINDOW_START <= hm <= _WINDOW_END


def main() -> int:
    log.info("auth_worker START (pid=%s, fresh process)", os.getpid())

    # 1. Load credentials from config/.env exactly like the backend does.
    try:
        from services.kite_auth import _load_env_file  # noqa: WPS433
        _load_env_file()
    except Exception as e:
        log.exception("auth_worker: could not load .env: %s", e)
        return 1

    required = ["KITE_API_KEY", "KITE_API_SECRET", "ZERODHA_USERNAME",
                "ZERODHA_PASSWORD", "ZERODHA_TOTP_SECRET"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        log.error("auth_worker: missing env vars: %s — aborting", missing)
        return 1

    from power_user.config import POWER_DB_PATH  # noqa: WPS433
    from services.zerodha_auto_auth import (  # noqa: WPS433
        run_auth_attempt, log_attempt, today_already_succeeded, AuthAttemptResult,
    )

    now = _now_ist()

    # Self-gate to the IST active window (weekday 06:00-16:30). Outside it,
    # exit immediately — the morning token is still valid until 06:00 tomorrow,
    # and markets are closed so there's nothing to refresh for. This lets the
    # Scheduled Task fire on a dumb local-time interval without PDT↔IST math.
    if not _in_active_window(now):
        log.info("auth_worker: %s IST is outside active window "
                 "(weekday 06:00-16:30 IST) — nothing to do, exiting",
                 now.strftime("%a %H:%M"))
        return 0

    # 2. Skip gate — token already valid for today AND live check passes.
    #    Reuse the scheduler's live-token health check (one Kite profile call).
    try:
        from services.auth_scheduler import _live_token_is_healthy  # noqa: WPS433
        live_ok = _live_token_is_healthy()
    except Exception as e:
        log.warning("auth_worker: live token check unavailable (%s) — assuming not healthy", e)
        live_ok = False

    already = False
    try:
        already = today_already_succeeded(POWER_DB_PATH)
    except Exception as e:
        log.warning("auth_worker: today_already_succeeded check failed (%s)", e)

    force = "--force" in sys.argv
    if force:
        log.info("auth_worker: --force flag set — bypassing skip gate, "
                 "running a real auth attempt to prove the Playwright path")

    if already and live_ok and not force:
        log.info("auth_worker: token already valid today AND live check passed — skipping")
        try:
            skip = AuthAttemptResult(
                status="skipped", stage=None, error_code="ALREADY_OK",
                error_detail="standalone worker: token healthy, skip",
                token_preview=None, elapsed_ms=0,
            )
            log_attempt(POWER_DB_PATH, 0, "scheduled", skip)
        except Exception:
            pass
        return 0

    if already and not live_ok:
        log.warning("auth_worker: log says success today but LIVE token failed "
                    "Kite profile() — refreshing")

    # 3. Run ONE auth attempt (fresh-process Playwright login).
    log.info("auth_worker: running auth attempt...")
    try:
        result = asyncio.run(run_auth_attempt(attempt_of_day=0, trigger_kind="scheduled"))
    except Exception as e:
        log.exception("auth_worker: run_auth_attempt crashed: %s", e)
        return 1

    # 4. Log to falcon_auth_log (admin panel timeline reads this).
    try:
        log_attempt(POWER_DB_PATH, 0, "scheduled", result)
    except Exception as e:
        log.warning("auth_worker: log_attempt failed (%s)", e)

    log.info("auth_worker: attempt status=%s stage=%s code=%s elapsed=%dms",
             result.status, result.stage, result.error_code, result.elapsed_ms)

    # 5. On failure at/after 09:00 IST, fire Layer-2 Web Push (idempotent/day).
    if result.status == "failed" and now.hour >= _PUSH_TRIGGER_HOUR:
        try:
            from power_user.services.web_push import notify_auth_needed  # noqa: WPS433
            push = notify_auth_needed()
            log.info("auth_worker: push fired -> %s", push)
        except Exception as e:
            log.warning("auth_worker: web push failed (non-fatal): %s", e)

    # 6. Exit code reflects outcome (Task Scheduler shows last result).
    if result.status == "success":
        log.info("auth_worker: SUCCESS — token written (preview=%s)", result.token_preview)
        return 0
    log.warning("auth_worker: FAILED — %s", result.error_code)
    return 1


if __name__ == "__main__":
    sys.exit(main())
