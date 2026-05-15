"""Apply Falcon schema extensions on startup.

Idempotent — safe to call repeatedly. Called from main.py at app boot.
"""
from pathlib import Path

from .db import connect_falcon


# Columns added after the table was first introduced. SQLite has no
# ADD COLUMN IF NOT EXISTS, so we PRAGMA-check then ALTER.
_COLUMN_MIGRATIONS = [
    ("falcon_trail_config",  "trail_method",        "TEXT NOT NULL DEFAULT 'percentage'"),
    ("falcon_trail_config",  "trail_lookback",      "INTEGER NOT NULL DEFAULT 10"),
    ("falcon_trade_orders",  "filled_qty",          "INTEGER DEFAULT 0"),
    ("falcon_trade_orders",  "last_reconciled_at",  "TEXT"),
    ("falcon_engine_config", "mining_window_years", "INTEGER NOT NULL DEFAULT 4"),
]


def _existing_columns(con, table: str) -> set[str]:
    return {r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()}


def _run_column_migrations(con) -> None:
    for table, column, definition in _COLUMN_MIGRATIONS:
        existing = _existing_columns(con, table)
        if not existing:
            # Table doesn't exist yet — nothing to migrate; CREATE TABLE handled it.
            continue
        if column in existing:
            continue
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def apply_extensions() -> None:
    sql_path = Path(__file__).parent / "db_schema_extensions.sql"
    if not sql_path.exists():
        return
    sql = sql_path.read_text(encoding="utf-8")
    con = connect_falcon()
    try:
        # ORDER MATTERS: migrate existing tables BEFORE executescript runs the
        # INSERT-OR-IGNORE seeds. The seed statements reference new columns
        # (trail_method, trail_lookback) that older DBs don't have yet — they'd
        # fail mid-script otherwise. On fresh DBs migrations are no-ops because
        # the table doesn't exist yet; executescript then creates it with all
        # columns in one shot.
        _run_column_migrations(con)
        con.executescript(sql)
        con.commit()
    finally:
        con.close()


if __name__ == "__main__":
    apply_extensions()
    print("Falcon schema extensions applied.")
