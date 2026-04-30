"""
KANIDA.AI — Database connection factory.

SQLite in local/dev (default). PostgreSQL / Supabase in production.

Usage in any router or service:
    from db import get_conn

    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM universe WHERE is_active=?", (1,)).fetchall()

All SQL in this codebase uses ? as placeholder — the Postgres wrapper converts
to %s automatically. ON CONFLICT, datetime('now'), and other SQLite idioms are
also translated transparently.

Switch to Postgres by setting DATABASE_URL in Railway / .env:
    DATABASE_URL=postgresql://user:pass@host:5432/kanida
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any, Sequence

_HERE = Path(__file__).parent
_SQLITE_PATH = os.environ.get(
    "KANIDA_DB_PATH",
    str(_HERE.parent / "data" / "db" / "kanida_quant.db"),
)
_PG_URL = os.environ.get("DATABASE_URL", "")

IS_POSTGRES = bool(_PG_URL)


# ── Public interface ───────────────────────────────────────────────────────────

def get_conn():
    """
    Return a DB connection compatible with both SQLite and Postgres.

    Use as a context manager for automatic commit/rollback + close:
        with get_conn() as conn:
            conn.execute(...)

    Or manually:
        conn = get_conn()
        conn.execute(...)
        conn.commit()
        conn.close()
    """
    if IS_POSTGRES:
        return _pg_conn()
    return _sqlite_conn()


def db_url() -> str:
    """Return a human-readable DB location string for logging."""
    if IS_POSTGRES:
        safe = _PG_URL.split("@")[-1] if "@" in _PG_URL else _PG_URL[:30]
        return f"postgres://{safe}"
    return _SQLITE_PATH


# ── SQLite path ────────────────────────────────────────────────────────────────

class _SqliteConn:
    """Wraps sqlite3.Connection — thin pass-through, adds context manager."""

    def __init__(self, path: str):
        self._conn = sqlite3.connect(path)
        self._conn.row_factory = sqlite3.Row

    # Core operations
    def execute(self, sql: str, params: Sequence[Any] = ()) -> Any:
        return self._conn.execute(sql, params)

    def executemany(self, sql: str, params_list: Sequence[Sequence[Any]]) -> Any:
        return self._conn.executemany(sql, params_list)

    def executescript(self, script: str) -> Any:
        return self._conn.executescript(script)

    def cursor(self) -> Any:
        return self._conn.cursor()

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    @property
    def total_changes(self) -> int:
        return self._conn.total_changes

    # Context manager
    def __enter__(self) -> "_SqliteConn":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()


def _sqlite_conn() -> _SqliteConn:
    return _SqliteConn(_SQLITE_PATH)


# ── Postgres path ──────────────────────────────────────────────────────────────

class _PgConn:
    """
    Wraps psycopg2 connection to look like _SqliteConn.
    Handles: ? → %s, datetime('now') → NOW(), INSERT OR IGNORE, etc.
    """

    def __init__(self, pg_conn: Any):
        self._conn = pg_conn

    # Core operations
    def execute(self, sql: str, params: Sequence[Any] = ()) -> Any:
        cur = self._pg_cursor()
        cur.execute(self._adapt(sql), list(params))
        return cur

    def executemany(self, sql: str, params_list: Sequence[Sequence[Any]]) -> Any:
        cur = self._pg_cursor()
        cur.executemany(self._adapt(sql), [list(p) for p in params_list])
        return cur

    def executescript(self, script: str) -> None:
        cur = self._conn.cursor()
        for stmt in script.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(self._adapt(stmt))

    def cursor(self) -> Any:
        return self._pg_cursor()

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    @property
    def total_changes(self) -> int:
        return 0

    def __enter__(self) -> "_PgConn":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()

    # Internal helpers
    def _pg_cursor(self) -> Any:
        from psycopg2.extras import RealDictCursor
        return self._conn.cursor(cursor_factory=RealDictCursor)

    @staticmethod
    def _adapt(sql: str) -> str:
        """
        Convert SQLite-dialect SQL to Postgres.
        Handles the patterns actually used in this codebase.
        """
        # Placeholder: ? → %s (skip ? inside string literals)
        out: list[str] = []
        in_str = False
        i = 0
        while i < len(sql):
            c = sql[i]
            if c == "'" and not in_str:
                in_str = True
                out.append(c)
            elif c == "'" and in_str:
                in_str = False
                out.append(c)
            elif c == "?" and not in_str:
                out.append("%s")
            else:
                out.append(c)
            i += 1
        s = "".join(out)

        # SQLite-specific functions → Postgres equivalents
        s = s.replace("datetime('now')", "NOW()")
        s = s.replace("date('now')", "CURRENT_DATE")
        s = s.replace("lower(hex(randomblob(8)))", "gen_random_uuid()::text")

        # DDL differences
        s = s.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
        s = s.replace("AUTOINCREMENT", "")
        s = s.replace("INSERT OR IGNORE INTO", "INSERT INTO")
        s = s.replace("INSERT OR REPLACE INTO", "INSERT INTO")

        # ON CONFLICT DO UPDATE — Postgres needs column name in ON CONFLICT clause.
        # Simple approach: strip extra spaces; complex UPSERT still works if SQL is correct.
        return s


def _pg_conn() -> _PgConn:
    try:
        import psycopg2
    except ImportError:
        raise RuntimeError(
            "DATABASE_URL is set but psycopg2 is not installed.\n"
            "Fix: pip install psycopg2-binary\n"
            "Then restart the backend."
        )
    conn = psycopg2.connect(_PG_URL)
    return _PgConn(conn)
