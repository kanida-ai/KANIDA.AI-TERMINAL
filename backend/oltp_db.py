"""
OLTP routing layer — the SQLite→Postgres cutover foundation (Stage 3b).

GOAL
  Let the ~44 OLTP call sites switch from SQLite to Postgres with a ONE-LINE
  change (swap the connection factory), NOT a per-query rewrite. So this exposes
  the exact sqlite3 API the code already uses — `con.execute("... ?", params)`,
  `cur.fetchone()/fetchall()`, `cur.lastrowid`, `row["col"]` and `row[0]` — but
  backed by Postgres when KANIDA_PG_ENABLED is set.

  Concentrating the dialect translation HERE (one well-tested module) is far
  safer than 44 hand-rewrites on live order paths. The translation is
  fail-LOUD: anything it does not understand raises, rather than silently
  emitting wrong SQL (the exact bug in the legacy backend/db.py, which turned
  `INSERT OR IGNORE` into a plain INSERT and dropped conflict handling).

SCOPE
  OLTP tables only (per-user transactional state). MARKET/engine tables stay on
  SQLite via falcon_conn() — unchanged. Use oltp_conn() ONLY for the OLTP domain.

TRANSLATION (only the patterns this codebase actually uses)
  * `?` -> `%s`                                   (outside string literals)
  * `INSERT OR IGNORE INTO t ...`  -> `... ON CONFLICT (<pk>) DO NOTHING`
  * `INSERT OR REPLACE INTO t (cols) ...` -> `... ON CONFLICT (<pk>)
       DO UPDATE SET col=EXCLUDED.col, ...`
  * `datetime('now')`->`NOW()`, `date('now')`->`CURRENT_DATE`,
    `CURRENT_TIMESTAMP`->`NOW()`
  * lastrowid: an INSERT with no RETURNING gets `RETURNING <pk>` appended and the
    value exposed as cur.lastrowid (matches SQLite's autoincrement contract).
  Conflict targets / PKs are read from Postgres' own catalog (cached), so they
  can never drift from the real schema.

DEFAULT OFF: KANIDA_PG_ENABLED unset -> oltp_conn() returns the SAME SQLite
connection the code uses today. Byte-for-byte unchanged until deliberately
enabled and load-tested.
"""
from __future__ import annotations

import logging
import os
import re
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

log = logging.getLogger(__name__)

# ── PK / conflict-target registry (lazy, from PG catalog) ────────────────────
_PK_CACHE: Dict[str, List[str]] = {}
_PK_LOCK = threading.Lock()


def _load_pk_map() -> Dict[str, List[str]]:
    """PRIMARY KEY columns for every public table, from PG. Cached."""
    global _PK_CACHE
    if _PK_CACHE:
        return _PK_CACHE
    with _PK_LOCK:
        if _PK_CACHE:
            return _PK_CACHE
        import pgdb
        m: Dict[str, List[str]] = {}
        with pgdb.pg_conn() as c:
            cur = c.cursor()
            cur.execute("""
                SELECT tc.table_name, kcu.column_name, kcu.ordinal_position
                  FROM information_schema.table_constraints tc
                  JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                 WHERE tc.constraint_type = 'PRIMARY KEY'
                   AND tc.table_schema = 'public'
                 ORDER BY tc.table_name, kcu.ordinal_position
            """)
            for tbl, col, _pos in cur.fetchall():
                m.setdefault(tbl, []).append(col)
        _PK_CACHE = m
        return m


def _pk_for(table: str) -> List[str]:
    pk = _load_pk_map().get(table.lower())
    if not pk:
        raise OltpTranslateError(
            f"no PRIMARY KEY known for table {table!r} — cannot translate "
            f"INSERT OR IGNORE/REPLACE to ON CONFLICT")
    return pk


class OltpTranslateError(RuntimeError):
    """Raised when SQL cannot be safely translated (fail loud, never guess)."""


# ── SQL translation ──────────────────────────────────────────────────────────

def _qmark_to_pct(sql: str) -> str:
    """`?` -> `%s` (outside string literals), and escape EVERY literal `%` to
    `%%`. psycopg2 scans the whole query for `%` when params are bound — even
    inside string literals (e.g. `LIKE '50%'`) — so an unescaped `%` there raises
    at execute time. The `%s` we emit for `?` is appended after this check, so it
    is never double-escaped."""
    out: List[str] = []
    in_str = False
    for ch in sql:
        if ch == "'":
            in_str = not in_str
            out.append(ch)
        elif ch == "%":
            out.append("%%")            # escape literal % EVERYWHERE
        elif ch == "?" and not in_str:
            out.append("%s")
        else:
            out.append(ch)
    return "".join(out)


_RE_INSERT = re.compile(
    r"^\s*INSERT\s+OR\s+(IGNORE|REPLACE)\s+INTO\s+([\"`\[]?)(\w+)\2\s*",
    re.IGNORECASE)
_RE_COLS = re.compile(r"^\s*INSERT[^(]*\(([^)]*)\)", re.IGNORECASE | re.DOTALL)


def _translate_insert_conflict(sql: str) -> str:
    """INSERT OR IGNORE/REPLACE -> INSERT ... ON CONFLICT(pk) ..."""
    m = _RE_INSERT.match(sql)
    if not m:
        return sql
    kind = m.group(1).upper()
    table = m.group(3)
    pk = _pk_for(table)
    pk_list = ", ".join(f'"{c}"' for c in pk)
    body = sql[m.end():]                      # "(cols) VALUES (...)" / "SELECT ..."
    base = f"INSERT INTO {table} {body}"
    if kind == "IGNORE":
        return f"{base} ON CONFLICT ({pk_list}) DO NOTHING"
    # REPLACE — update every inserted column except the PK to EXCLUDED.
    cm = _RE_COLS.match(sql)
    if not cm:
        raise OltpTranslateError(
            f"INSERT OR REPLACE without an explicit (col,...) list is not "
            f"translatable: {sql[:80]}")
    cols = [c.strip().strip('"`[]') for c in cm.group(1).split(",")]
    setters = ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in cols if c not in pk)
    if not setters:                            # all-PK insert -> nothing to update
        return f"{base} ON CONFLICT ({pk_list}) DO NOTHING"
    return f"{base} ON CONFLICT ({pk_list}) DO UPDATE SET {setters}"


def _translate_funcs(sql: str) -> str:
    sql = re.sub(r"datetime\(\s*'now'\s*\)", "NOW()", sql, flags=re.IGNORECASE)
    sql = re.sub(r"date\(\s*'now'\s*\)", "CURRENT_DATE", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bCURRENT_TIMESTAMP\b", "NOW()", sql, flags=re.IGNORECASE)
    return sql


def translate(sql: str) -> Tuple[str, bool]:
    """SQLite SQL -> Postgres SQL. Returns (sql, is_insert_without_returning).

    The flag tells the cursor whether to append RETURNING <pk> for lastrowid.
    Fail-loud on residual SQLite-only constructs.
    """
    s = _translate_insert_conflict(sql)
    s = _translate_funcs(s)
    s = _qmark_to_pct(s)
    low = s.lower()
    for bad in ("insert or ", " autoincrement", "strftime(", "pragma "):
        if bad in low:
            raise OltpTranslateError(f"untranslated SQLite construct {bad!r}: {sql[:80]}")
    is_insert = low.lstrip().startswith("insert")
    has_returning = " returning " in low
    return s, (is_insert and not has_returning)


# ── PG connection wrapper mimicking the sqlite3 API ──────────────────────────

class _PgCursor:
    def __init__(self, raw_cur, table_pk_lookup):
        self._cur = raw_cur
        self._pk_lookup = table_pk_lookup
        self.lastrowid: Optional[int] = None

    def execute(self, sql: str, params: Sequence[Any] = ()):  # noqa: D401
        s, want_returning = translate(sql)
        if want_returning:
            tbl = _table_of_insert(sql)
            pk = self._pk_lookup(tbl) if tbl else None
            if pk and len(pk) == 1:
                s = s + f' RETURNING "{pk[0]}"'
                self._cur.execute(s, list(params))
                row = self._cur.fetchone()
                # RETURNING pk may be suppressed by ON CONFLICT DO NOTHING (no row)
                self.lastrowid = (row[0] if row else None)
                return self
        self._cur.execute(s, list(params))
        return self

    def executemany(self, sql: str, seq):
        s, _ = translate(sql)
        self._cur.executemany(s, [list(p) for p in seq])
        return self

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    def fetchmany(self, n):
        return self._cur.fetchmany(n)

    @property
    def rowcount(self):
        return self._cur.rowcount

    def __iter__(self):
        return iter(self._cur)


class _PgConn:
    """Quacks like sqlite3.Connection for the OLTP call sites."""

    def __init__(self, raw):
        self._raw = raw
        import pgdb  # noqa
        self._pk_lookup = _pk_for

    def execute(self, sql: str, params: Sequence[Any] = ()):
        cur = _PgCursor(self._raw.cursor(), self._pk_lookup)
        return cur.execute(sql, params)

    def executemany(self, sql: str, seq):
        cur = _PgCursor(self._raw.cursor(), self._pk_lookup)
        return cur.executemany(sql, seq)

    def cursor(self):
        return _PgCursor(self._raw.cursor(), self._pk_lookup)

    def commit(self):
        self._raw.commit()

    def rollback(self):
        self._raw.rollback()

    def close(self):
        # returned to the pool by the oltp_conn() context manager
        pass


def _table_of_insert(sql: str) -> Optional[str]:
    m = re.match(r"^\s*INSERT\s+(?:OR\s+\w+\s+)?INTO\s+[\"`\[]?(\w+)", sql,
                 re.IGNORECASE)
    return m.group(1) if m else None


# ── public factory ───────────────────────────────────────────────────────────

def pg_routing() -> bool:
    return os.environ.get("KANIDA_PG_ENABLED", "").strip().lower() in (
        "1", "true", "yes", "on")


@contextmanager
def oltp_conn() -> Iterator[Any]:
    """OLTP connection. Postgres when KANIDA_PG_ENABLED, else the SAME SQLite
    connection the code uses today (falcon_conn semantics)."""
    if pg_routing():
        import pgdb
        pool = pgdb.get_pool()
        raw = pool.getconn()
        raw.cursor_factory = None
        # dict+index row access to match sqlite3.Row
        from psycopg2.extras import DictCursor
        raw.cursor_factory = DictCursor
        conn = _PgConn(raw)
        try:
            yield conn
            raw.commit()
        except Exception:
            raw.rollback()
            raise
        finally:
            pool.putconn(raw)
    else:
        from falcon.db import connect_falcon
        con = connect_falcon()
        try:
            yield con
        finally:
            con.close()
