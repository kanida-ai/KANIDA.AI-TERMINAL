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


RND_DB_PATH   = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"

# Cloud-migration A2 (2026-07-16): the SILENT R&D fallback is REMOVED.
#
# Previously, if the production DB was absent this resolver silently returned
# the ~35 GB universe_engine R&D DB — so the portal AND the live trading path
# would boot happily on the WRONG database. In a container with a mis-mounted
# volume that fails QUIETLY: the app serves, reading research data as if it
# were production. The R&D file is also single-writer SQLite; live trading
# silently sharing it is a contention + correctness hazard.
#
# The rule is narrow: the production/serving path must never IMPLICITLY fall
# back to the research DB. Explicit opt-in remains fully supported:
#   • FALCON_DB_PATH=<path>              — point anywhere, incl. the R&D DB
#   • KANIDA_ALLOW_RND_DB_FALLBACK=true  — restore the legacy fallback (loud)
# Offline research/backtest tooling that hardcodes the R&D path is untouched.
def _rnd_fallback_allowed() -> bool:
    return os.environ.get(
        "KANIDA_ALLOW_RND_DB_FALLBACK", ""
    ).strip().lower() in ("1", "true", "yes")


def _resolve_falcon_db() -> Path:
    """Resolve the Falcon production DB.

    Explicit FALCON_DB_PATH wins. Otherwise the canonical production path is
    returned UNCONDITIONALLY — existence is validated at startup by
    verify_falcon_db(), which fails loud rather than substituting the R&D DB.
    """
    env = os.environ.get("FALCON_DB_PATH")
    if env:
        return Path(env)
    canonical = DB_DIR / "kanida_universe.db"
    if canonical.exists():
        return canonical
    if _rnd_fallback_allowed() and RND_DB_PATH.exists():
        # EXPLICIT opt-in only. Never reached in a default configuration.
        return RND_DB_PATH
    # No silent substitution. Return the canonical path so the error names the
    # path the operator actually expects; verify_falcon_db() raises on it.
    return canonical


FALCON_DB     = _resolve_falcon_db()


class ProductionDBMissingError(FileNotFoundError, RuntimeError):
    """Raised at startup when the production DB is absent/unreadable.

    Deliberately fatal: booting on the wrong database is worse than not
    booting at all.

    Subclasses FileNotFoundError for strict backward compatibility — the old
    connect_falcon() raised FileNotFoundError, so any existing handler keeps
    working unchanged.
    """


def _db_error_message(path, var: str) -> str:
    # ASCII-ONLY, deliberately. start_backend.bat redirects stdout/stderr into
    # logs\backend.log on a cp1252 console; box-drawing chars or bullets raise
    # UnicodeEncodeError while printing, which would MASK this fatal error
    # behind an encoding traceback. Verified: the em-dash/box version crashed.
    rnd_note = ""
    if RND_DB_PATH.exists():
        rnd_note = (
            f"\n  NOTE: the R&D database exists at\n    {RND_DB_PATH}\n"
            "  It is NOT a substitute and will NOT be used automatically. It is a\n"
            "  research warehouse (single-writer SQLite); serving live traffic or\n"
            "  live trading from it is a correctness + contention hazard."
        )
    bar = "=" * 66
    return (
        "\n"
        f"{bar}\n"
        " FATAL: Kanida production database not found - refusing to start.\n"
        f"{bar}\n"
        f"  Expected at : {path}\n"
        f"  Set via     : {var} (absolute path)\n"
        "\n"
        "  Likely causes:\n"
        "    - Container volume not mounted / mounted at the wrong target\n"
        f"    - {var} unset or pointing at a stale path\n"
        "    - The DB file was moved, renamed, or not seeded into the image\n"
        "\n"
        "  This used to fall back SILENTLY to the R&D database, booting the\n"
        "  portal and the LIVE TRADING path against the wrong data. That\n"
        "  fallback is removed on purpose - fix the mount/path instead.\n"
        "\n"
        "  Escape hatch (only if this is a FALSE POSITIVE and you must boot):\n"
        "    set KANIDA_SKIP_DB_PREFLIGHT=true"
        f"{rnd_note}\n"
        f"{bar}"
    )


def verify_falcon_db(path: Path | None = None) -> Path:
    """Fail loud if the Falcon production DB is missing or unreadable.

    Call at startup. Returns the verified path so callers can log it.
    """
    p = Path(path) if path is not None else FALCON_DB
    if not p.exists():
        raise ProductionDBMissingError(_db_error_message(p, "FALCON_DB_PATH"))
    if not os.access(str(p), os.R_OK):
        raise ProductionDBMissingError(
            _db_error_message(p, "FALCON_DB_PATH")
            + "\n  (File EXISTS but is not readable - check permissions/ownership.)"
        )
    return p

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
