"""
KANIDA.AI — PostgreSQL connection layer (Stage 1 of the SQLite→PG migration).

WHY THIS EXISTS (and why not backend/db.py):
  backend/db.py is the legacy Railway-era helper. It is a GLOBAL switch
  (IS_POSTGRES flips the WHOLE app to PG) and its SQL translator rewrites
  `INSERT OR IGNORE/REPLACE INTO` -> plain `INSERT INTO`, which SILENTLY DROPS
  conflict handling (duplicate-key errors instead of ignore/replace). Neither is
  acceptable for this migration, so the PG path is rebuilt here.

DESIGN
  * HYBRID SPLIT, not a global switch. Two data domains:
      - MARKET  (ohlc_daily, falcon_features, patterns, signals ...) — one writer
        (the daily pipeline), read by everyone. STAYS on SQLite (local disk):
        fast reads, no concurrency problem, ~2.9M rows not worth moving.
      - OLTP    (power_user_*, broker_accounts, autotrade_*, portfolio_*,
        falcon_trade_* ...) — CONCURRENT writes from many users. Moves to PG.
    Callers ask for a domain; the router decides. Market never reaches PG.
  * POOLED. The runtime is thread-per-session (entry_scheduler + tick_driver),
    so connections must be pooled, not opened per call. A pool also bounds load
    on db.t4g.micro (which has a low max_connections).
  * FAIL LOUD, NEVER SILENT-FALLBACK. If PG is enabled but unreachable, raise.
    A silent fall back to SQLite would split writes across two stores — the
    worst outcome for order/position state.
  * DEFAULT OFF. Nothing changes until KANIDA_PG_ENABLED=true, so this module
    can ship to production with zero behavioural impact.

CONFIG
  KANIDA_PG_ENABLED   "true" to route OLTP at PG (default: false)
  DATABASE_URL        postgresql://user:pass@host:5432/kanida
                      (or PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD)
  KANIDA_PG_POOL_MIN  minimum pooled connections (default 1)
  KANIDA_PG_POOL_MAX  maximum pooled connections (default 8)
"""
from __future__ import annotations

import logging
import os
import threading
from contextlib import contextmanager
from typing import Any, Iterator, Optional
from urllib.parse import quote_plus

log = logging.getLogger(__name__)

# ── Data domains ─────────────────────────────────────────────────────────────
DOMAIN_MARKET = "market"   # engine/market data — SQLite, single writer
DOMAIN_OLTP = "oltp"       # per-user transactional state — Postgres target

_POOL: Optional[Any] = None
_POOL_LOCK = threading.Lock()


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def pg_enabled() -> bool:
    """True when OLTP reads/writes should be routed to Postgres."""
    return _env_flag("KANIDA_PG_ENABLED", False)


def dsn() -> str:
    """Resolve the Postgres DSN from DATABASE_URL, else from PG* parts.

    Returns "" when nothing is configured (caller decides whether that's fatal).
    The password is URL-quoted so RDS-generated passwords containing '/', '@'
    or '+' cannot corrupt the URL.
    """
    url = (os.environ.get("DATABASE_URL") or "").strip()
    if url.startswith(("postgres://", "postgresql://")):
        return url
    host = os.environ.get("PGHOST", "").strip()
    if not host:
        return ""
    user = quote_plus(os.environ.get("PGUSER", "kanida_admin"))
    pwd = quote_plus(os.environ.get("PGPASSWORD", ""))
    port = os.environ.get("PGPORT", "5432")
    name = os.environ.get("PGDATABASE", "kanida")
    return f"postgresql://{user}:{pwd}@{host}:{port}/{name}"


def safe_target() -> str:
    """host:port/db for logging — never includes credentials."""
    d = dsn()
    if not d:
        return "(unconfigured)"
    tail = d.split("@")[-1] if "@" in d else d
    return tail


def _build_pool() -> Any:
    try:
        from psycopg2.pool import ThreadedConnectionPool
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "KANIDA_PG_ENABLED is set but psycopg2 is not installed "
            "(pip install psycopg2-binary)."
        ) from e
    d = dsn()
    if not d:
        raise RuntimeError(
            "KANIDA_PG_ENABLED is set but no DATABASE_URL / PGHOST is configured."
        )
    mn = int(os.environ.get("KANIDA_PG_POOL_MIN", "1"))
    mx = int(os.environ.get("KANIDA_PG_POOL_MAX", "8"))
    # connect_timeout keeps a wrong SG / down instance from hanging boot.
    pool = ThreadedConnectionPool(mn, mx, dsn=d, connect_timeout=10)
    log.info("pgdb: pool ready (min=%s max=%s) -> %s", mn, mx, safe_target())
    return pool


def get_pool() -> Any:
    """Lazily create the process-wide connection pool (thread-safe)."""
    global _POOL
    if _POOL is None:
        with _POOL_LOCK:
            if _POOL is None:
                _POOL = _build_pool()
    return _POOL


@contextmanager
def pg_conn() -> Iterator[Any]:
    """Borrow a pooled Postgres connection.

    Commits on clean exit, rolls back on exception, and ALWAYS returns the
    connection to the pool. Raises if PG is not configured — never silently
    degrades to SQLite (split-brain writes are worse than a hard failure).
    """
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:  # pragma: no cover - defensive
            pass
        raise
    finally:
        pool.putconn(conn)


def health() -> dict:
    """Connectivity probe used at boot + by the admin route.

    Probes whenever a DSN is CONFIGURED, even while routing is still disabled —
    that way wiring DATABASE_URL alone is enough to verify RDS reachability
    (SG path, credentials, database exists) BEFORE any traffic is routed.
    `routing` reports whether OLTP actually reads/writes PG yet.

    Never raises — returns a structured result so a failure is visible in logs
    and the admin panel instead of crashing the app.
    """
    if not dsn():
        return {"enabled": pg_enabled(), "routing": pg_enabled(), "ok": False,
                "target": "(unconfigured)",
                "detail": "no DATABASE_URL / PGHOST configured"}
    try:
        with pg_conn() as c:
            cur = c.cursor()
            cur.execute("SELECT version(), current_database(), NOW()")
            ver, db, now = cur.fetchone()
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'public'"
            )
            ntables = cur.fetchone()[0]
        return {"enabled": True, "routing": pg_enabled(), "ok": True,
                "target": safe_target(),
                "database": db, "server_time": str(now),
                "public_tables": ntables,
                "version": (ver or "").split(",")[0]}
    except Exception as e:
        return {"enabled": True, "routing": pg_enabled(), "ok": False,
                "target": safe_target(),
                "error": f"{type(e).__name__}: {str(e)[:300]}"}


def close_pool() -> None:
    """Close all pooled connections (shutdown hook / tests)."""
    global _POOL
    with _POOL_LOCK:
        if _POOL is not None:
            try:
                _POOL.closeall()
            except Exception:  # pragma: no cover - defensive
                pass
            _POOL = None
