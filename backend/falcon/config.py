"""Falcon — central config and paths.

All Falcon code reads paths from here. Override via env vars in Railway.
"""
import os
from pathlib import Path


def _project_root() -> Path:
    """Resolve project root regardless of CWD. backend/falcon/config.py → .."""
    here = Path(__file__).resolve()
    return here.parent.parent.parent


ROOT          = _project_root()
DATA_DIR      = Path(os.environ.get("KANIDA_DATA_DIR",  str(ROOT / "data")))
DB_DIR        = DATA_DIR / "db"

# DBs
LEGACY_DB     = Path(os.environ.get("KANIDA_DB_PATH",        str(DB_DIR / "kanida_quant.db")))


def _resolve_falcon_db() -> Path:
    """Look for Falcon DB at the canonical production path; fall back to the
    universe_engine R&D path for local dev."""
    env = os.environ.get("FALCON_DB_PATH")
    if env:
        return Path(env)
    canonical = DB_DIR / "kanida_universe.db"
    if canonical.exists():
        return canonical
    rnd_path = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
    if rnd_path.exists():
        return rnd_path
    return canonical    # default; entrypoint.sh seeds this in production


FALCON_DB     = _resolve_falcon_db()

# Tables
T_SIGNALS_LIVE       = "falcon_signals_live"          # NEW — emitted picks per day
T_SIGNAL_RUNS        = "falcon_signal_runs"           # NEW — cron run audit log
T_NOTIFICATIONS_OUT  = "falcon_notifications_out"     # NEW — email + in-app queue

# Engine version (used in API responses / DB rows)
FALCON_VERSION       = "7.1.0"
DROPPED_FAMILIES     = ["drawdown_bounce"]            # V7.1 default exclusion

# Cron schedule (IST). All times documented; Railway cron uses UTC.
DAILY_DATA_REFRESH_IST  = "16:30"
DAILY_FEATURES_IST      = "16:32"
DAILY_SIGNALS_IST       = "16:35"
WEEKLY_REMINE_IST_SUN   = "18:00"

# Notification settings — read from env
NOTIFY_EMAIL_FROM     = os.environ.get("FALCON_EMAIL_FROM",   "")
NOTIFY_EMAIL_TO       = os.environ.get("FALCON_EMAIL_TO",     "")
SMTP_HOST             = os.environ.get("FALCON_SMTP_HOST",    "")
SMTP_PORT             = int(os.environ.get("FALCON_SMTP_PORT", "587"))
SMTP_USER             = os.environ.get("FALCON_SMTP_USER",    "")
SMTP_PASS             = os.environ.get("FALCON_SMTP_PASS",    "")

# Feature flags
TOP_N_DEFAULT         = int(os.environ.get("FALCON_TOP_N",     "100"))
MIN_FIRES_DEFAULT     = int(os.environ.get("FALCON_MIN_FIRES", "2"))
MIN_LIQUIDITY_CR      = float(os.environ.get("FALCON_MIN_LIQ_CR", "5.0"))
