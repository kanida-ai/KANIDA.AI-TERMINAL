import logging
import os
import sqlite3
import subprocess
import sys
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
sys.path.insert(0, os.path.join(_HERE, ".."))

log = logging.getLogger("kanida.main")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")

# ── Load .env file if present ─────────────────────────────────────────────────
_env = os.path.join(_HERE, ".env")
if not os.path.exists(_env):
    _env = os.path.join(_HERE, "..", "config", ".env")
if os.path.exists(_env):
    try:
        from dotenv import load_dotenv
        load_dotenv(_env, override=True)
    except ImportError:
        pass
    for _line in open(_env).read().splitlines():
        if '=' in _line and not _line.startswith('#'):
            _k, _v = _line.split('=', 1)
            _k = _k.strip()
            _v = _v.strip().strip('"').strip("'")
            if _k and _v:
                os.environ[_k] = _v

# ── DB path ───────────────────────────────────────────────────────────────────
if not os.environ.get("KANIDA_DB_PATH"):
    os.environ["KANIDA_DB_PATH"] = os.path.normpath(
        os.path.join(_HERE, "..", "data", "db", "kanida_quant.db")
    )
_DB  = os.environ["KANIDA_DB_PATH"]
ROOT = os.path.normpath(os.path.join(_HERE, ".."))

# ── Pipeline state (shared) ───────────────────────────────────────────────────
_pipeline_lock   = threading.Lock()
_pipeline_status = {"running": False, "last_run": None, "last_result": None, "next_run": None}

IST = timezone(timedelta(hours=5, minutes=30))


def _compute_next_run(hour: int = 16, minute: int = 5) -> str:
    """Next HH:MM IST on a weekday (Mon–Fri), as ISO-8601 string."""
    now = datetime.now(IST)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    while target.weekday() >= 5:
        target += timedelta(days=1)
    return target.isoformat()

PIPELINE_STEPS = [
    {"name": "OHLCV Fetch",        "cmd": [sys.executable, "data/ingest/fetch_fno_kite.py"]},
    {"name": "Pattern Learning",   "cmd": [sys.executable, "engine/jobs/run_learning.py"]},
    {"name": "Backtest",           "cmd": [sys.executable, "engine/backtest/run_backtest.py"]},
    {"name": "Execution Analysis", "cmd": [sys.executable, "engine/backtest/run_execution_analysis.py"]},
    {"name": "Pending Entries",    "cmd": [sys.executable, "engine/jobs/create_pending_entries.py"]},
]


def _run_pipeline_sync():
    """Run the full pipeline synchronously. Called in a background thread."""
    if not _pipeline_lock.acquire(blocking=False):
        log.warning("Pipeline already running — skipping.")
        return

    # Stage 4c — SINGLETON ACROSS CONTAINERS. _pipeline_lock is a threading.Lock,
    # so it only serialises within ONE process. Every task runs its own 16:05 IST
    # scheduler thread, so with desired_count>1 the EOD pipeline would run N times
    # concurrently — N Kite fetches and N writers racing on the same tables. A
    # dated claim (pipeline:<YYYY-MM-DD>) means exactly one container runs it.
    # Lease > worst-case runtime so a slow run can't be double-started; a crashed
    # holder's lease expires and tomorrow's key differs anyway.
    # No-op in single-container mode (KANIDA_MULTI_CONTAINER unset).
    try:
        from autotrade.session_ownership import multi_container, OWNER_ID
        if multi_container():
            from autotrade import durable_claims
            _pkey = f"pipeline:{datetime.now(IST).date().isoformat()}"
            if not durable_claims.claim(_pkey, 4 * 3600, holder=OWNER_ID):
                log.info("Pipeline: %s already claimed by another container — "
                         "skipping (single run per day, cluster-wide).", _pkey)
                _pipeline_lock.release()
                return
    except Exception as e:  # never let the guard block the pipeline
        log.warning("Pipeline singleton guard failed (running anyway): %s", e)

    _pipeline_status["running"] = True
    _pipeline_status["last_run"] = datetime.now(IST).isoformat()
    log.info("Pipeline starting — %s", _pipeline_status["last_run"])

    try:
        # Token preflight
        sys.path.insert(0, _HERE)
        from services.kite_auth import get_token_status
        status = get_token_status()
        if not status.get("valid"):
            log.error("Pipeline aborted — Kite token invalid: %s", status.get("reason"))
            _pipeline_status["last_result"] = f"ABORTED: token invalid — {status.get('reason')}"
            # 2026-05-27: legacy aborted, but V7 still gets a shot via the
            # unconditional kickoff in `finally`. V7's own preflight inside
            # daily_data_refresh will surface the same token issue if it's
            # still bad — but if the admin manually refreshes the token
            # between this call and the V7 retry 15 min later, today's
            # signals will land.
            return

        for step in PIPELINE_STEPS:
            log.info("Running: %s", step["name"])
            result = subprocess.run(
                step["cmd"], cwd=ROOT,
                capture_output=True, text=True, timeout=3600,
            )
            if result.returncode != 0:
                log.error("%s FAILED (rc=%d):\n%s", step["name"], result.returncode, result.stderr[-2000:])
                _pipeline_status["last_result"] = f"FAILED at {step['name']}"
                return
            log.info("%s OK", step["name"])

        # Phase 2.3: pre-market staging right after pipeline finishes — uses
        # the falcon_signals_live rows the pipeline just populated, plus a
        # holdings scan to stage bulk-adopt items for the next trading day.
        try:
            from falcon.trade.services import eod_orchestrator
            kite = None
            try:
                from services.kite_auth import get_kite_client
                kite = get_kite_client(check=True)
            except Exception:
                kite = None  # Stage NEW_ENTRY only; bulk-adopt scan needs Kite
            summary = eod_orchestrator.run_eod(target_date=None, kite=kite)
            log.info("EOD pre-market staging: %d items staged for %s",
                     summary.get("total_staged", 0), summary.get("target_date"))
        except Exception as e:
            log.exception("EOD pre-market staging crashed (pipeline still SUCCESS): %s", e)

        # 2026-05-27: REMOVED the Co-Trader EOD call that used to live here.
        # Co-Trader is now Step 5 of the V7 pipeline (see falcon/jobs/_pipeline.py).
        # Keeping it here as well meant two writers updating portfolio_positions
        # in the same EOD window — idempotent today (INSERT OR IGNORE) but the
        # exact kind of "legacy + new code mixed in the same path" the operator
        # explicitly flagged for cleanup. V7's step 5 is the sole owner now.
        #
        # The success-path V7 kickoff below is ALSO removed (was redundant with
        # the unconditional kickoff in `finally`). Single source of truth for V7
        # invocation: the finally block. Same kick_off_v7_pipeline_if_stale call,
        # but it now runs on EVERY pipeline iteration regardless of legacy outcome.

        _pipeline_status["last_result"] = "SUCCESS"
        log.info("Pipeline complete.")
    except Exception as e:
        _pipeline_status["last_result"] = f"ERROR: {e}"
        log.exception("Pipeline error")
    finally:
        # 2026-05-27 (V7 decoupling): unconditionally call V7 from `finally` so
        # legacy aborts (bad token, step crash) no longer block today's V7 run.
        # `kick_off_v7_pipeline_if_stale` is idempotent — if V7 already ran
        # earlier in this _run_pipeline_sync (success path line 159), it
        # returns kicked_off=False here and does nothing. The retry loop in
        # `_schedule_daily_pipeline` will retry every 15 min until 19:00 IST
        # if V7 itself fails (e.g. token still bad on this attempt).
        try:
            from falcon.jobs._pipeline import kick_off_v7_pipeline_if_stale
            v7d = kick_off_v7_pipeline_if_stale(reason="unconditional_post_legacy")
            log.info("V7 unconditional kickoff: kicked_off=%s reason=%s",
                      v7d.get("kicked_off"), v7d.get("reason_skipped") or v7d.get("reason_kicked"))
        except Exception as ee:
            log.exception("V7 unconditional kickoff crashed (non-fatal): %s", ee)
        _pipeline_status["running"] = False
        _pipeline_lock.release()


# Retry policy for the daily V7 pipeline (added 2026-05-25 after the
# 2026-05-25 outage). When the 16:05 IST run fails (token invalid, step
# crash, etc.) the scheduler used to jump straight to tomorrow's 16:05 IST,
# leaving the day's signals blank even if the operator fixed the token
# 10 minutes later. New behaviour: retry every 15 min until 19:00 IST
# (~3 hours of recovery window). Past 19:00 IST → wait for tomorrow.
_PIPELINE_RETRY_CUTOFF_HOUR    = 19              # IST. After this, give up for today.
_PIPELINE_RETRY_INTERVAL_SEC   = 15 * 60         # 15 minutes between retries.


def _v7_succeeded_today() -> bool:
    """Has V7 produced FRESH signals for today's IST EOD window?

    History:
      Fix #4 (2026-05-25): original implementation read falcon_signal_runs
      and counted "success" rows by date(started_at). That was wrong in two
      ways:
        1. A "success" run-log row doesn't prove the run emitted today's
           data — it just proves no exception was raised. daily_data_refresh
           can succeed-with-no-new-bars (bad token, market open, etc.) and
           daily_signals can upsert yesterday's date again.
        2. A 13:46 IST manual run on 2026-05-27 set all 3 success rows with
           started_at=today but emitted signal_date=2026-05-26. The function
           returned True → retry loop concluded "today is done" → 16:05 IST
           EOD cron was suppressed → signals frozen at yesterday all day.

    Fix 2026-05-27: replaced the run-log scan with the single source of
    truth used by the V7 kick-off gate — falcon_signals_live.MAX(signal_date)
    vs expected. Both functions agree on "is today done?", eliminating the
    legacy/V7 divergence that caused the 2026-05-27 stale-frozen incident.
    """
    try:
        from falcon.jobs._pipeline import _signals_fresh_for_now
        return _signals_fresh_for_now()
    except Exception as e:
        log.warning("v7_succeeded_today: freshness check failed (%s) — fail-open", e)
        return False     # fail-open → retry triggered; safer than skipping


def _schedule_daily_pipeline():
    """Block until 16:05 IST on a weekday, then run the pipeline. On failure,
    retry every 15 min until 19:00 IST. After 19:00 IST or on success, wait
    for tomorrow's 16:05 IST. Loops forever.

    Fix #5 (2026-05-25): wrap each loop iteration in try/except so an unexpected
    exception can't silently kill the scheduler thread. Previously, any uncaught
    raise (clock skew, DB lock, import error after hot-reload, etc.) would exit
    `_schedule_daily_pipeline()` permanently — the thread was `daemon=True` and
    never restarted. Daily signals would silently stop firing until manual
    backend restart.

    Fix #4 (2026-05-25): a "successful" run for retry purposes now requires
    BOTH the legacy pipeline AND all 3 V7 steps to succeed today. Previously
    V7-only failure was invisible because `_pipeline_status["last_result"]`
    only tracked legacy; the scheduler skipped to tomorrow even though
    today's user-facing V7 signals never landed.
    """
    import time
    while True:
        try:
            now           = datetime.now(IST)
            last_result   = (_pipeline_status.get("last_result") or "")
            last_run_iso  = (_pipeline_status.get("last_run") or "")
            today_iso     = now.date().isoformat()

            # Legacy-side: did today's run finish with SUCCESS in the dict?
            legacy_succeeded_today = (
                last_run_iso.startswith(today_iso) and last_result == "SUCCESS"
            )
            legacy_failed_today = (
                last_run_iso.startswith(today_iso)
                and bool(last_result)
                and last_result != "SUCCESS"
            )

            # Fix #4: combine with V7's authoritative state from falcon_signal_runs.
            # Today is "done" only if BOTH legacy + V7 succeeded.
            v7_done_today    = _v7_succeeded_today()
            today_succeeded  = legacy_succeeded_today and v7_done_today
            # Treat as "needs retry" if legacy failed OR (legacy succeeded but V7 didn't).
            today_failed_recently = (
                legacy_failed_today
                or (last_run_iso.startswith(today_iso) and not v7_done_today)
            )

            is_weekday    = now.weekday() < 5
            past_1605     = (now.hour, now.minute) >= (16, 5)
            # Prospective retry target — used to gate the retry window so we
            # never schedule a retry that would land past the cutoff.
            prospective_retry = now + timedelta(seconds=_PIPELINE_RETRY_INTERVAL_SEC)
            in_retry_window = (is_weekday and past_1605
                                and prospective_retry.hour < _PIPELINE_RETRY_CUTOFF_HOUR
                                and prospective_retry.date() == now.date())

            # 2026-05-27 boot catch-up: if backend booted AFTER 16:05 IST on a
            # weekday with no prior pipeline attempt today AND V7 didn't
            # succeed today either, fire NOW instead of sleeping until tomorrow.
            # This is the bug that left Tuesday 2026-05-26 with zero signals:
            # the previous backend died sometime between Mon 17:27 PDT and
            # Tue 03:35 PDT (16:05 IST Tue). When a fresh process eventually
            # came up, `_pipeline_status` was empty (in-memory state lost),
            # `last_run_iso` didn't start with today, today_failed_recently
            # was False, and the default branch scheduled tomorrow's 16:05 IST
            # — skipping today entirely.
            today_attempted_legacy = last_run_iso.startswith(today_iso)
            boot_catchup_needed = (
                is_weekday
                and past_1605
                and not today_attempted_legacy
                and not v7_done_today
            )

            if boot_catchup_needed:
                # Fire in 10 seconds so the rest of `_run` finishes setting up
                target = now + timedelta(seconds=10)
                reason = "boot-catchup-missed-1605"
            elif today_failed_recently and in_retry_window:
                # Retry slot — schedule another attempt 15 min from now.
                target = prospective_retry
                if not v7_done_today and legacy_succeeded_today:
                    reason = "retry-v7-only-failed"
                elif ":" in last_result:
                    reason = f"retry-after-{last_result.split(':')[0][:20]}"
                else:
                    reason = "retry-after-fail"
            else:
                # Default: next 16:05 IST weekday.
                target = now.replace(hour=16, minute=5, second=0, microsecond=0)
                if target <= now:
                    target += timedelta(days=1)
                while target.weekday() >= 5:
                    target += timedelta(days=1)
                reason = "next-day" if today_succeeded else (
                    "post-cutoff-wait-tomorrow" if today_failed_recently else "scheduled"
                )

            _pipeline_status["next_run"] = target.isoformat()
            _pipeline_status["v7_succeeded_today"] = v7_done_today
            wait = max((target - datetime.now(IST)).total_seconds(), 1.0)
            log.info("Scheduler: next pipeline run at %s IST (%.0f min) — %s "
                      "(legacy_ok=%s v7_ok=%s)",
                      target.strftime("%Y-%m-%d %H:%M"), wait / 60, reason,
                      legacy_succeeded_today, v7_done_today)
            time.sleep(wait)
            _run_pipeline_sync()
        except Exception as e:
            # Fix #5: defensive outer guard. Never let an unexpected exception
            # kill the scheduler thread silently. Record the error for admin
            # observability and sleep 60s before re-attempting the loop body.
            _pipeline_status["last_scheduler_error"] = (
                f"{type(e).__name__}: {str(e)[:200]} @ {datetime.now(IST).isoformat()}"
            )
            log.exception("Pipeline scheduler iteration crashed (retrying in 60s): %s", e)
            try:
                time.sleep(60)
            except Exception:
                pass


def _apply_postgres_schema():
    """Create all tables in Postgres if they don't exist yet (idempotent)."""
    import pathlib
    sql_path = pathlib.Path(_HERE).parent / "db" / "migrations" / "0001_initial.sql"
    if not sql_path.exists():
        log.warning("Schema migration not found at %s — skipping", sql_path)
        return
    sql = sql_path.read_text()
    try:
        from db import get_conn
        with get_conn() as conn:
            conn.executescript(sql)
        log.info("Postgres schema applied from %s", sql_path)
    except Exception as exc:
        log.error("Failed to apply Postgres schema: %s", exc)


# ── FastAPI lifespan: start scheduler thread on startup ──────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── A2: production DB preflight — FAIL LOUD, never a silent R&D fallback ──
    # The silent fallback in falcon/config.py + power_user/config.py is removed;
    # this is the startup gate that turns a mis-mounted volume into a refusal to
    # boot instead of a quiet boot against the wrong (research) database.
    #
    # Escape hatch: KANIDA_SKIP_DB_PREFLIGHT=true downgrades the raise to a
    # warning. A false-positive "DB missing" refusing to start the trading
    # backend would be worse than the bug it prevents, so the operator keeps a
    # documented way out. Resolution is __file__-based + absolute (verified
    # cwd-independent under both launch styles), so false positives are not
    # expected.
    try:
        from falcon.config import ProductionDBMissingError, verify_falcon_db
        from power_user.config import verify_power_db
    except Exception as e:                      # pragma: no cover — import guard
        # If the preflight itself can't even load, do NOT strand the trading
        # backend: a false-positive refusal to boot is worse than the bug this
        # check prevents. Log loudly and continue.
        log.exception("DB preflight unavailable (non-fatal, continuing): %s", e)
    else:
        try:
            verify_falcon_db()
            verify_power_db()
            log.info("Production DB preflight OK (falcon + power_user).")
        except ProductionDBMissingError as e:
            # ONLY a genuinely missing/unreadable production DB blocks boot.
            if os.environ.get("KANIDA_SKIP_DB_PREFLIGHT", "").strip().lower() in (
                "1", "true", "yes"
            ):
                log.error("Production DB preflight FAILED but KANIDA_SKIP_DB_PREFLIGHT "
                          "is set — continuing anyway: %s", e)
            else:
                log.critical("%s", e)
                raise
        except Exception as e:                  # pragma: no cover — defensive
            log.exception("DB preflight crashed (non-fatal, continuing): %s", e)

    from db import IS_POSTGRES
    if IS_POSTGRES:
        _apply_postgres_schema()

    t = threading.Thread(target=_schedule_daily_pipeline, daemon=True, name="pipeline-scheduler")
    t.start()
    log.info("Daily pipeline scheduler started (16:05 IST weekdays).")

    # Phase 2.3: pre-market deployer thread (fires QUEUED items at 9:15 IST)
    try:
        from falcon.trade.services import premarket_deployer
        if premarket_deployer.start():
            log.info("Pre-market deployer thread started (window: 9:14-9:30 IST weekdays).")
    except Exception as e:
        log.exception("Pre-market deployer failed to start: %s", e)

    # 2026-05-29: Playwright preflight at boot. Detects browser-unlaunchable
    # state ON STARTUP (before any cycle runs and wastes 30 sec/attempt). If
    # broken at boot, fires Web Push to admin immediately so they can use the
    # manual Zerodha OAuth path. Subprocess-isolated, ≤20 sec wall time. Never
    # blocks boot — wrapped in try/except, runs in a daemon thread so even a
    # hung Playwright doesn't delay the rest of startup.
    def _boot_preflight():
        try:
            from services.playwright_preflight import check_now
            h = check_now(fire_push_on_break=True)
            if h.is_healthy:
                log.info("Playwright preflight: HEALTHY (elapsed=%dms)", h.elapsed_ms)
            else:
                log.warning("Playwright preflight: BROKEN class=%s — pushed "
                              "notification to admin; auth cycles will SKIP "
                              "until recovery", h.failure_class)
        except Exception as e:
            log.exception("Playwright boot preflight crashed (non-fatal): %s", e)
    threading.Thread(target=_boot_preflight, daemon=True,
                     name="playwright-boot-preflight").start()

    # Zerodha auto-auth — MOVED OUT of the backend (Fix 1, 2026-06-02).
    #
    # The auth bot used to run here, inside the long-lived backend process.
    # That was the root cause of 9 "token failed / EOD didn't fire" incidents:
    # a process that survives for days on a sleeping laptop loses the ability
    # to spawn Playwright's Chromium subprocess (proven empirically — a fresh
    # process on the same machine always works; the aged one fails with a
    # phantom "Executable doesn't exist"). No in-process code change can fix a
    # degraded process.
    #
    # Auth now runs as a STANDALONE Windows Scheduled Task ("KanidaZerodhaAuth")
    # that invokes scripts/run_auth_worker.bat -> scripts/auth_worker.py every
    # 30 min. Each invocation is a brand-new short-lived process that cannot
    # age, writes the token to the DB, and exits. The backend just READS the
    # token from the DB (services.kite_auth.get_token_status / get_kite_client).
    #
    # Register the task once with:  scripts/register_auth_task.ps1
    #
    # The in-backend services.auth_scheduler is intentionally NOT started here
    # anymore. It remains importable (admin status reads, the manual
    # /admin/auth/refresh-now path still works for on-demand refresh), but the
    # recurring 30-min loop is owned by the Scheduled Task.
    # CLOUD self-minting (FALCON_INPROCESS_AUTH=true): start the in-process auth
    # scheduler so the CLOUD mints its own Zerodha token (Playwright login + TOTP,
    # creds from Secrets) — no laptop. The "aged process loses Playwright" root
    # cause above was SLEEP-specific (a Fargate task never sleeps), so it does not
    # apply here. IMPORTANT: only ONE minter per Kite account (a fresh
    # generate_session invalidates the prior token) — so when this is on, the
    # laptop's KanidaZerodhaAuth task MUST be disabled. The laptop leaves this
    # flag unset and keeps its standalone task.
    if os.environ.get("FALCON_INPROCESS_AUTH", "").strip().lower() in ("1", "true", "yes"):
        try:
            from services.auth_scheduler import start as _inproc_auth_start
            if _inproc_auth_start():
                log.info("Zerodha auto-auth scheduler STARTED in-process "
                         "(FALCON_INPROCESS_AUTH) — cloud self-mints its token.")
            else:
                log.warning("in-process auth scheduler start() returned False "
                            "(already running or disabled).")
        except Exception as e:  # never let auth wiring crash boot
            log.exception("in-process auth scheduler failed to start: %s", e)
    else:
        log.info("Zerodha auto-auth scheduler NOT started in-process — owned by "
                  "standalone Scheduled Task 'KanidaZerodhaAuth' (Fix 1, 2026-06-02). "
                  "Backend reads token from DB only. (set FALCON_INPROCESS_AUTH=true "
                  "for cloud self-minting.)")

    # Postgres (SQLite->PG migration, Stage 1). Probe-only: reports whether the
    # OLTP domain can reach RDS. Routing does NOT change until KANIDA_PG_ENABLED
    # is set AND the per-module cutover lands, so this is safe to ship dormant.
    # Never raises — a DB probe must not be able to crash boot.
    try:
        import pgdb as _pgdb
        _h = _pgdb.health()
        if not _h.get("enabled") and not _h.get("ok") and "detail" in _h:
            log.info("pgdb: NOT CONFIGURED (SQLite only) — %s", _h.get("detail"))
        elif _h.get("ok"):
            log.info("pgdb: REACHABLE -> %s db=%s public_tables=%s routing=%s %s",
                     _h.get("target"), _h.get("database"),
                     _h.get("public_tables"), _h.get("routing"),
                     _h.get("version"))
        else:
            log.error("pgdb: CONFIGURED BUT UNREACHABLE -> %s :: %s",
                      _h.get("target"), _h.get("error"))
    except Exception as e:  # pragma: no cover - probe must never break boot
        log.warning("pgdb: health probe failed to run: %s", e)

    # Sprint 5c-2: Featured replay pre-warmer. 6h daemon that keeps the 3
    # landing-page replay cache rows hot — kills the 2.26s cold-load that
    # an unlucky first visitor would otherwise pay.
    try:
        from power_user.services.replay_warmer import start as _start_replay_warmer
        if _start_replay_warmer():
            log.info("Featured replay pre-warmer started (6h interval).")
        else:
            log.info("Featured replay pre-warmer already running.")
    except Exception as e:
        log.exception("Featured replay pre-warmer failed to start (non-fatal): %s", e)

    # GTM Phase: structural preflight — run at boot so the result is cached
    # and visible in /admin before market opens. If RED, every auto-trade
    # entry point will refuse to call broker APIs with the specific blocking
    # check name. See backend/falcon/preflight.py.
    try:
        from falcon import preflight
        result = preflight.run(force=True)
        n_red = sum(1 for c in result.checks if c.status == preflight.RED)
        n_yel = sum(1 for c in result.checks if c.status == preflight.YELLOW)
        if result.ok:
            log.info("Falcon preflight: OK (%d checks, %d yellow warn)",
                     len(result.checks), n_yel)
        else:
            red_names = [c.name for c in result.checks if c.status == preflight.RED]
            log.warning("Falcon preflight: BLOCKED — %d red: %s", n_red, red_names)
            for c in result.checks:
                if c.status == preflight.RED:
                    log.warning("  [%s] %s → %s", c.name, c.detail, c.remediation)
    except Exception as e:
        log.exception("Falcon preflight crashed at boot — continuing in degraded mode: %s", e)

    # GTM: missed-cron catch-up. The 16:05 IST scheduler is in-process — a
    # backend restart past 16:05 IST silently skipped that day's pipeline
    # (next-fire pointer jumped to tomorrow). This is the 2026-05-13 bug
    # the operator hit twice. Catch-up logic: on every boot, if today is a
    # weekday past 16:05 IST AND today's daily_signals hasn't succeeded yet,
    # kick off the pipeline in a daemon thread. Idempotent — won't double-fire.
    try:
        n = datetime.now(IST)
        past_1605 = (n.hour, n.minute) >= (16, 5)
        is_weekday = n.weekday() < 5
        if past_1605 and is_weekday:
            from falcon.jobs._pipeline import kick_off_v7_pipeline_if_stale
            decision = kick_off_v7_pipeline_if_stale(reason="boot_catchup")
            if decision.get("kicked_off"):
                log.warning("Boot catch-up: today's 16:05 IST pipeline never ran "
                            "(or didn't complete). Triggering now async.")
            else:
                log.info("Boot catch-up: skipped (%s)", decision.get("reason_skipped"))
    except Exception as e:
        log.exception("Boot catch-up crashed (non-fatal): %s", e)

    # NSE HOLIDAY AUTO-FETCH (real-money safety): kick a BACKGROUND best-effort
    # refresh of the self-updating NSE holiday cache so the trading calendar stays
    # authoritative for the current year (and the next year near year-end). This
    # closes the hole where an un-seeded future year would silently treat every
    # NSE holiday as a trading day. MUST NOT block or crash boot — it runs on a
    # daemon thread, is fully wrapped, and only re-fetches when the cache is stale.
    try:
        import threading as _threading
        from datetime import datetime as _dt

        def _nse_holiday_boot_refresh():
            try:
                from autotrade import nse_holiday_source as _nse
                from autotrade import trading_calendar as _cal
                _IST = timezone(timedelta(hours=5, minutes=30))
                now = _dt.now(_IST)
                # Always ensure the CURRENT year and — within ~8 weeks of year-end
                # — the NEXT year are covered (the daily ladder + scheduled entries
                # can roll into next year). refresh_if_stale is cheap when fresh.
                _nse.refresh_if_stale(max_age_days=7)
                need_years = [now.year]
                # ~8 weeks before Dec 31 → start pulling next year's circular.
                if (_dt(now.year, 12, 31, tzinfo=_IST) - now).days <= 56:
                    need_years.append(now.year + 1)
                uncovered = _nse.ensure_years_covered(need_years, max_age_days=7)
                covered = _cal.covered_years()
                if uncovered:
                    log.error(
                        "NSE HOLIDAY COVERAGE GAP after fetch: years %s still "
                        "uncovered — scheduling/firing into them will be REFUSED. "
                        "Add them to data/config/nse_holidays.txt. (covered=%s)",
                        sorted(uncovered), sorted(covered))
                else:
                    log.info("NSE holiday auto-fetch OK — covered years=%s",
                             sorted(covered))
            except Exception as e:  # never let the refresh crash anything
                log.warning("NSE holiday boot refresh failed (non-fatal): %s", e)

        _threading.Thread(target=_nse_holiday_boot_refresh,
                          name="nse-holiday-boot-refresh", daemon=True).start()
    except Exception as e:
        log.warning("NSE holiday boot refresh could not start (non-fatal): %s", e)

    # AutoTrade: resume active paper/live sessions after a restart. The per-
    # session tick driver + entry scheduler are in-memory daemon threads that
    # die on restart, so RUNNING sessions stop refreshing LTP/gross_return and
    # SCHEDULED sessions silently never fire. resume_active_sessions() re-arms
    # them from the persisted autotrade_sessions state (idempotent; paper = no
    # real orders; writes ONLY autotrade_positions). Wrapped so it can never
    # block boot. See backend/autotrade/recovery.py.
    try:
        from autotrade.recovery import resume_active_sessions
        _at_resume = resume_active_sessions()
        log.info("AutoTrade resume: running=%d scheduled=%d fired=%d rearmed=%d errors=%d",
                 _at_resume.get("running", 0), _at_resume.get("scheduled", 0),
                 _at_resume.get("fired", 0), _at_resume.get("rearmed", 0),
                 _at_resume.get("errors", 0))
    except Exception as e:
        log.exception("AutoTrade resume crashed at boot (non-fatal): %s", e)

    # AutoTrade LADDER ORCHESTRATOR: resume campaign state from persisted
    # autotrade_ladders rows (open today's basket if due, refresh the 5-day alert,
    # auto-complete month-end) then arm the process-wide daily-tick scheduler so
    # every SUBSEQUENT trading day opens a basket with no operator input. Both are
    # idempotent + restart-durable + paper-safe (children inherit the ladder mode;
    # live stays gated by FALCON_AUTOTRADE_ENABLED). Wrapped so it can't block boot.
    try:
        from autotrade.ladder import resume_active_ladders
        from autotrade.monitoring import ladder_scheduler
        _lad_resume = resume_active_ladders()
        _lad_armed = ladder_scheduler.start()
        log.info("AutoTrade ladder resume: resumed=%d opened=%d completed=%d "
                 "errors=%d scheduler_armed=%s",
                 _lad_resume.get("resumed", 0), _lad_resume.get("opened", 0),
                 _lad_resume.get("completed", 0), _lad_resume.get("errors", 0),
                 _lad_armed)
    except Exception as e:
        log.exception("AutoTrade ladder resume crashed at boot (non-fatal): %s", e)

    yield


# ── Routers ───────────────────────────────────────────────────────────────────
from db import db_url as _db_url
log.info("DB: %s", _db_url())

from routers.quant_router      import router as quant_router
from routers.backtest_router   import router as backtest_router
from routers.live_router       import router as live_router
from routers.execution_router  import router as execution_router
from routers.swing_router      import router as swing_router
from routers.admin_router      import router as admin_router
from routers.jobs_router       import router as jobs_router
from routers.universe_router   import router as universe_router
from routers.ai_router         import router as ai_router
# 2026-06-02: orders_router + strategy_router UNMOUNTED — usage audit confirmed
# zero frontend consumers. Source archived to backend/_archive/routers/.
# The remaining legacy routers above are still consumed by the active Falcon
# operator pages (via lib/admin-api.ts + lib/backtest-api.ts) — do NOT remove.

# Falcon V7.1 production routers (coexists with legacy)
from falcon.routers.signals_router    import router as falcon_signals_router
from falcon.routers.portfolio_router  import router as falcon_portfolio_router
from falcon.routers.patterns_router   import router as falcon_patterns_router
from falcon.routers.admin_router      import router as falcon_admin_router
from falcon.trade.routers.trade_router import router as falcon_trade_router
from autotrade.api.autotrade_routes import router as autotrade_router  # AutoTrade portfolio + kill switch (operator-token gated)
from autotrade.api.pnl_routes import router as autotrade_pnl_router  # AutoTrade Performance dashboard P&L (power_jwt-gated, read-only, /api/power/autotrade/pnl/*)
from autotrade.api.config_edit_routes import router as autotrade_config_edit_router  # AutoTrade LIVE CONFIG EDIT (power_jwt-gated) — PATCH /api/power/autotrade/{session|ladder}/{id}/config
from autotrade.api.health_routes import router as sysagents_health_router  # System-Engineering Agent Hierarchy Phase 1 — GET /api/health/system (operator-token gated, read-only) — ships DISABLED (SYSAGENTS_ENABLED default-off)
# P1PUB (2026-06-14): laptop → cloud "publish intelligence" ingest. Self-auth
# (X-Publish-Secret), atomic full-replace. See CLOUD_ARCHITECTURE.md §6.
from falcon.routers.publish_router     import router as falcon_publish_router

# Power User Portal (Phase 1 — invite-only beta).
# Public surface namespaced under /api/power/* — strict separation from
# operator-only /api/falcon/*. See backend/power_user/SPEC/Design.md.
from power_user.routers.auth_router         import router as power_auth_router
from power_user.routers.invites_router      import router as power_invites_router
from power_user.routers.admin_router        import router as power_admin_router
from power_user.routers.picks_router        import router as power_picks_router
from power_user.routers.auth_refresh_router import router as power_auth_refresh_router
from power_user.routers.portfolios_router   import router as power_portfolios_router
# Persona backtest refactor (2026-05-16): replaces Excel-upload flow with a
# live simulator. /api/power/personas/* is the new single source of truth.
from power_user.routers.persona_backtest_router import router as power_persona_router
# Falcon Top 20 (2026-05-23): institutional 3-bucket explainability for the
# /power/today page. /api/power/today/falcon-top-20.
from power_user.routers.falcon_top20_router       import router as power_top20_router
# Billing (M3, 2026-06-13): Razorpay subscription lifecycle + webhook.
# /api/power/billing/*. PUBLIC surface (no paywall) — webhook is HMAC-verified.
from power_user.routers.billing_router            import router as power_billing_router
# Ask-Falcon read-only API (2026-06-22): universe search, last-EOD-close quotes,
# and per-stock analysis for any covered stock. /api/power/universe|quote|ask/*.
from power_user.routers.ask_router               import router as power_ask_router
# Co-Trading virtual-portfolio sim (2026-06-22): follow Falcon with virtual
# capital. /api/power/cotrade/simulate|portfolio. Reuses the falcon-top-10
# walk-forward engine (single continuous cash pool scaled to user capital).
# Read-only on market data; NEVER touches the auto-trade execution path.
from power_user.routers.cotrade_router           import router as power_cotrade_router

app = FastAPI(title="KANIDA.AI Swing Trading Terminal", version="3.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.include_router(quant_router,     prefix="/api", tags=["Quant"])
app.include_router(backtest_router,  prefix="/api", tags=["Backtest"])
app.include_router(live_router,      prefix="/api", tags=["Live"])
app.include_router(execution_router, prefix="/api", tags=["Execution"])
app.include_router(swing_router,     prefix="/api", tags=["Swing"])
app.include_router(admin_router,     prefix="/api", tags=["Admin"])
app.include_router(jobs_router,      prefix="/api", tags=["Jobs"])
app.include_router(universe_router,  prefix="/api", tags=["Universe"])
app.include_router(ai_router,        prefix="/api", tags=["AI"])
# orders_router + strategy_router intentionally NOT mounted (2026-06-02 audit).

# Falcon mounted under /api — endpoints live under /api/falcon/*
app.include_router(falcon_signals_router,   prefix="/api", tags=["Falcon"])
app.include_router(falcon_portfolio_router, prefix="/api", tags=["Falcon"])
app.include_router(falcon_patterns_router,  prefix="/api", tags=["Falcon"])
app.include_router(falcon_admin_router,     prefix="/api", tags=["Falcon"])
app.include_router(falcon_trade_router,     prefix="/api", tags=["Falcon-Trade"])
app.include_router(falcon_publish_router,   prefix="/api", tags=["Falcon-Publish"])  # P1PUB laptop→cloud ingest

# Power User Portal — endpoints already self-prefix with /api/power/*
app.include_router(power_auth_router,         tags=["Power-User"])
app.include_router(power_invites_router,      tags=["Power-User"])
app.include_router(power_admin_router,        tags=["Power-User"])
app.include_router(power_picks_router,        tags=["Power-User"])
app.include_router(power_auth_refresh_router, tags=["Power-User"])
app.include_router(power_portfolios_router,   tags=["Power-User"])
app.include_router(power_persona_router,      tags=["Power-User"])    # new persona simulator endpoints
app.include_router(power_top20_router,         tags=["Power-User"])    # Falcon Top 20 + 3-bucket explainability
app.include_router(power_billing_router,       tags=["Power-User"])    # Razorpay billing + webhook (M3)
app.include_router(power_ask_router,            tags=["Power-User"])    # Ask-Falcon: universe / quote / analyze-stock
app.include_router(power_cotrade_router,         tags=["Power-User"])    # Co-Trading virtual-portfolio sim (falcon-top-10)
app.include_router(autotrade_router,             prefix="/api", tags=["AutoTrade"])    # AutoTrade: multi-broker portfolio sessions + kill switch (/api/autotrade/*) — ships DISABLED
app.include_router(autotrade_pnl_router,          tags=["Power-User"])    # AutoTrade Performance dashboard P&L (power_jwt-gated, read-only) — /api/power/autotrade/pnl/*
app.include_router(autotrade_config_edit_router,  tags=["Power-User"])    # AutoTrade LIVE CONFIG EDIT (power_jwt-gated) — PATCH /api/power/autotrade/{session|ladder}/{id}/config
app.include_router(sysagents_health_router,      prefix="/api", tags=["SysAgents"])    # System-Engineering Agent Hierarchy Phase 1 — GET /api/health/system (operator-token gated, read-only) — DISABLED unless SYSAGENTS_ENABLED=true

# Power User schema init — idempotent, creates tables on first boot.
# Uses POWER_DB_PATH resolver — same DB as the engine read-only tables
# (kanida_universe.db), NOT the legacy quant DB.
try:
    from power_user.db_init import init_power_user_schema
    from power_user.config import POWER_DB_PATH as _power_db
    _power_manifest = init_power_user_schema(_power_db)
    if _power_manifest["ok"]:
        log.info("Power User schema OK: %d tables, %d indices (db=%s)",
                 len(_power_manifest["tables_present"]),
                 len(_power_manifest["indices_present"]),
                 _power_db)
    else:
        log.warning("Power User schema INCOMPLETE: missing tables=%s indices=%s",
                     _power_manifest["tables_missing"], _power_manifest["indices_missing"])

    # Sprint 5d: keep the 5 portfolio_definitions rows in sync with the locked
    # Python constants in portfolio_defs.py on every boot. Cheap + idempotent.
    try:
        from power_user.services.portfolio_engine import seed_portfolio_definitions
        _seed_con = sqlite3.connect(_power_db, timeout=10.0)
        try:
            n = seed_portfolio_definitions(_seed_con)
            log.info("Co-Trader portfolio definitions: %d portfolios upserted.", n)
        finally:
            _seed_con.close()
    except Exception as _e:
        log.warning("Co-Trader portfolio seed skipped: %s", _e)

    # Phase 1b (2026-05-23): ensure the admin row exists for the operator's
    # email. Idempotent — promotes an existing row to 'admin' if needed, or
    # inserts a fresh admin row. Required for the new invite-code login flow
    # since admin auth bypasses the invite_codes table (admin uses
    # POWER_ADMIN_SECRET as their login code).
    try:
        from power_user.services.auth import bootstrap_admin_user
        _admin_con = sqlite3.connect(_power_db, timeout=10.0)
        _admin_con.row_factory = sqlite3.Row
        try:
            _admin = bootstrap_admin_user(_admin_con)
            if _admin:
                log.info("Power User admin bootstrapped: id=%s email=%s role=%s",
                         _admin.get("id"), _admin.get("email"), _admin.get("role"))
        finally:
            _admin_con.close()
    except Exception as _e:
        log.warning("Power User admin bootstrap skipped: %s", _e)

    # Sprint 5d Fix 4 (2026-05-16): import the operator's V3 audit Excel files
    # → year-by-year + month-by-month performance tables. Idempotent. Soft-fail
    # if Excels aren't accessible (e.g. cloud deploy with no Desktop access);
    # the previously imported rows stay in place.
    try:
        import sys as _sys
        _scripts = os.path.join(_HERE, "..", "scripts")
        if _scripts not in _sys.path:
            _sys.path.insert(0, _scripts)
        from import_persona_excels import import_all as _import_excels
        _excel_summary = _import_excels(_power_db)
        _n_ok = sum(1 for r in _excel_summary["personas"] if not r.get("skipped"))
        log.info("Co-Trader performance import: %d/%d personas synced from Excel files.",
                 _n_ok, len(_excel_summary["personas"]))
    except Exception as _e:
        log.warning("Co-Trader Excel import skipped (will use last-loaded rows): %s", _e)
except Exception as _e:
    log.warning("Power User schema init skipped: %s", _e)

# Apply Falcon schema extensions on startup (idempotent)
try:
    from falcon.db_init import apply_extensions as _falcon_apply_ext
    _falcon_apply_ext()
    log.info("Falcon schema extensions applied.")
except Exception as _e:
    log.warning("Falcon schema extensions skipped: %s", _e)

# AutoTrade schema migration — idempotent, additive (adds exit_lock +
# exit_initiated_by to falcon_position_state and the new autotrade_* tables).
# Never modifies existing tables; safe on every boot.
try:
    from autotrade.db_migrations import run_migrations as _run_autotrade_migrations
    _at_manifest = _run_autotrade_migrations()
    log.info("AutoTrade schema OK: +cols=%s tables=%s",
             _at_manifest.get("added_columns"), _at_manifest.get("tables_present"))
except Exception as _at_e:  # pragma: no cover - never block boot
    log.warning("AutoTrade migration skipped/failed (non-fatal): %s", _at_e)

# Start Phase 2 position monitor (background thread, idempotent)
try:
    from falcon.trade.services.position_monitor import start_background_monitor as _falcon_start_monitor
    _falcon_start_monitor()
except Exception as _e:
    log.warning("Falcon position monitor not started: %s", _e)

# Start Phase 3 KiteTicker (real-time LTP stream, idempotent).
# Soft-fail: if access token isn't loaded yet, ticker stays off and monitor
# falls back to kite.holdings() snapshots — no correctness loss, just latency.
try:
    from falcon.trade.services import kite_ticker as _falcon_ticker
    if _falcon_ticker.start():
        log.info("Falcon KiteTicker connect issued.")
    else:
        log.info("Falcon KiteTicker not connected (auth not ready); will poll-only.")
except Exception as _e:
    log.warning("Falcon KiteTicker not started: %s", _e)

# System-Engineering Agent Hierarchy Phase 1 (platform HEALTH monitors) — the
# run-loop is DEFAULT-OFF: start() is a hard no-op unless SYSAGENTS_ENABLED=true
# AND the kill-switch is off. On a normal (flag-off) boot no thread starts and the
# whole layer is inert. Observation-only: it never touches a trade/order/position.
try:
    from autotrade.sysagents import runner as _sysagents_runner
    if _sysagents_runner.start():
        log.info("SysAgents health run-loop started (SYSAGENTS_ENABLED=true).")
    else:
        log.info("SysAgents health layer disabled (default-off); run-loop not started.")
except Exception as _e:  # pragma: no cover - never block boot
    log.warning("SysAgents health layer not started (non-fatal): %s", _e)


@app.get("/")
def root():
    return {
        "product":         "KANIDA.AI Quant Terminal",
        "version":         "3.0.0",
        "db":              _DB,
        "pipeline_status": _pipeline_status,
        "docs":            "/docs",
    }
