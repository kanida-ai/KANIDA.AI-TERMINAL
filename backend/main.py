import logging
import os
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
_pipeline_status = {"running": False, "last_run": None, "last_result": None}

IST = timezone(timedelta(hours=5, minutes=30))

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

        _pipeline_status["last_result"] = "SUCCESS"
        log.info("Pipeline complete.")
    except Exception as e:
        _pipeline_status["last_result"] = f"ERROR: {e}"
        log.exception("Pipeline error")
    finally:
        _pipeline_status["running"] = False
        _pipeline_lock.release()


def _schedule_daily_pipeline():
    """Block until 16:05 IST on a weekday, then run the pipeline. Loops forever."""
    import time
    while True:
        now = datetime.now(IST)
        # Target: 16:05 IST, Mon–Fri
        target = now.replace(hour=16, minute=5, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        while target.weekday() >= 5:
            target += timedelta(days=1)
        wait = (target - datetime.now(IST)).total_seconds()
        log.info("Scheduler: next pipeline run at %s IST (%.0f min)",
                 target.strftime("%Y-%m-%d %H:%M"), wait / 60)
        time.sleep(wait)
        _run_pipeline_sync()


# ── FastAPI lifespan: start scheduler thread on startup ──────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
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

    # Sprint 5c-1: Zerodha auto-auth scheduler (Layer 1 = Playwright bot).
    # Wakes at 06:30/07:30/08:30/09:00 IST weekdays, runs a Playwright login,
    # writes the token to kanida_quant.db.kite_tokens. On 4th-attempt failure,
    # fires Web Push (Layer 2) to admin with a magic-link CTA.
    try:
        from services.auth_scheduler import start as _start_auth_scheduler, status as _auth_sched_status
        if _start_auth_scheduler():
            nxt = _auth_sched_status().get("next_attempt_at")
            log.info("Zerodha auto-auth scheduler started (next: %s IST).", nxt)
        else:
            log.info("Zerodha auto-auth scheduler already running.")
    except Exception as e:
        log.exception("Zerodha auto-auth scheduler failed to start (non-fatal): %s", e)

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
from routers.orders_router     import router as orders_router
from routers.universe_router   import router as universe_router
from routers.strategy_router   import router as strategy_router

# Falcon V7.1 production routers (coexists with legacy)
from falcon.routers.signals_router    import router as falcon_signals_router
from falcon.routers.portfolio_router  import router as falcon_portfolio_router
from falcon.routers.patterns_router   import router as falcon_patterns_router
from falcon.routers.admin_router      import router as falcon_admin_router
from falcon.trade.routers.trade_router import router as falcon_trade_router

# Power User Portal (Phase 1 — invite-only beta).
# Public surface namespaced under /api/power/* — strict separation from
# operator-only /api/falcon/*. See backend/power_user/SPEC/Design.md.
from power_user.routers.auth_router         import router as power_auth_router
from power_user.routers.invites_router      import router as power_invites_router
from power_user.routers.admin_router        import router as power_admin_router
from power_user.routers.picks_router        import router as power_picks_router
from power_user.routers.auth_refresh_router import router as power_auth_refresh_router

app = FastAPI(title="KANIDA.AI Swing Trading Terminal", version="3.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.include_router(quant_router,     prefix="/api", tags=["Quant"])
app.include_router(backtest_router,  prefix="/api", tags=["Backtest"])
app.include_router(live_router,      prefix="/api", tags=["Live"])
app.include_router(execution_router, prefix="/api", tags=["Execution"])
app.include_router(swing_router,     prefix="/api", tags=["Swing"])
app.include_router(admin_router,     prefix="/api", tags=["Admin"])
app.include_router(jobs_router,      prefix="/api", tags=["Jobs"])
app.include_router(orders_router,    prefix="/api", tags=["Orders"])
app.include_router(universe_router,  prefix="/api", tags=["Universe"])
app.include_router(strategy_router,  prefix="/api", tags=["Strategy"])

# Falcon mounted under /api — endpoints live under /api/falcon/*
app.include_router(falcon_signals_router,   prefix="/api", tags=["Falcon"])
app.include_router(falcon_portfolio_router, prefix="/api", tags=["Falcon"])
app.include_router(falcon_patterns_router,  prefix="/api", tags=["Falcon"])
app.include_router(falcon_admin_router,     prefix="/api", tags=["Falcon"])
app.include_router(falcon_trade_router,     prefix="/api", tags=["Falcon-Trade"])

# Power User Portal — endpoints already self-prefix with /api/power/*
app.include_router(power_auth_router,         tags=["Power-User"])
app.include_router(power_invites_router,      tags=["Power-User"])
app.include_router(power_admin_router,        tags=["Power-User"])
app.include_router(power_picks_router,        tags=["Power-User"])
app.include_router(power_auth_refresh_router, tags=["Power-User"])

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
except Exception as _e:
    log.warning("Power User schema init skipped: %s", _e)

# Apply Falcon schema extensions on startup (idempotent)
try:
    from falcon.db_init import apply_extensions as _falcon_apply_ext
    _falcon_apply_ext()
    log.info("Falcon schema extensions applied.")
except Exception as _e:
    log.warning("Falcon schema extensions skipped: %s", _e)

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


@app.get("/")
def root():
    return {
        "product":         "KANIDA.AI Quant Terminal",
        "version":         "3.0.0",
        "db":              _DB,
        "pipeline_status": _pipeline_status,
        "docs":            "/docs",
    }
