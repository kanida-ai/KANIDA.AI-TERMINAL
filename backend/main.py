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
        _pipeline_status["next_run"] = target.isoformat()
        wait = (target - datetime.now(IST)).total_seconds()
        log.info("Scheduler: next pipeline run at %s IST (%.0f min)",
                 target.strftime("%Y-%m-%d %H:%M"), wait / 60)
        time.sleep(wait)
        _run_pipeline_sync()


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
    from db import IS_POSTGRES
    if IS_POSTGRES:
        _apply_postgres_schema()

    t = threading.Thread(target=_schedule_daily_pipeline, daemon=True, name="pipeline-scheduler")
    t.start()
    log.info("Daily pipeline scheduler started (16:05 IST weekdays).")
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
from routers.ai_router         import router as ai_router

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
app.include_router(ai_router,        prefix="/api", tags=["AI"])


@app.get("/")
def root():
    return {
        "product":         "KANIDA.AI Quant Terminal",
        "version":         "3.0.0",
        "db":              _DB,
        "pipeline_status": _pipeline_status,
        "docs":            "/docs",
    }
