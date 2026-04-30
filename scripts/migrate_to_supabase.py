"""
KANIDA.AI — SQLite → Supabase (Postgres) Migration
===================================================
Reads the local SQLite database and copies ALL rows into your Supabase Postgres
instance.  Run this ONCE after setting DATABASE_URL.

Usage:
    DATABASE_URL=postgresql://... python scripts/migrate_to_supabase.py

  --apply-schema   Run 0001_initial.sql in Postgres first (safe to re-run; all
                   statements use CREATE TABLE IF NOT EXISTS)
  --dry-run        Print row counts without writing anything
  --tables t1,t2   Only migrate the specified tables
  --batch-size N   Rows per INSERT batch (default 500)

The script stops on the first Postgres write error so you can fix and re-run.
It is idempotent if all target tables were empty: re-running after a partial
migration may produce duplicates — truncate the target tables first.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

ROOT    = Path(__file__).parent.parent
DB_PATH = os.environ.get(
    "KANIDA_DB_PATH",
    str(ROOT / "data" / "db" / "kanida_quant.db"),
)
PG_URL  = os.environ.get("DATABASE_URL", "")
MIGRATION_SQL = ROOT / "db" / "migrations" / "0001_initial.sql"

# Tables in dependency order (FKs satisfied before dependents).
ORDERED_TABLES = [
    "schema_migrations",
    "ohlc_daily",
    "instruments",
    "universe",
    "strategies",
    "behavior_features",
    "snapshot_runs",
    "ingestion_log",
    "pattern_library",
    "atom_stats",
    "signal_events",
    "signal_outcomes",
    "live_opportunities",
    "trade_log",
    "execution_log",
    "stoploss_behavior",
    "target_behavior",
    "pattern_roster",
]


# ── Helpers ────────────────────────────────────────────────────────────────────

def _sqlite(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _pg(url: str):
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
    except ImportError:
        sys.exit("psycopg2 not installed. Run: pip install psycopg2-binary")
    conn = psycopg2.connect(url)
    conn.autocommit = False
    return conn


def _pg_cursor(pg_conn):
    from psycopg2.extras import RealDictCursor
    return pg_conn.cursor(cursor_factory=RealDictCursor)


def _table_exists_sqlite(sq_conn: sqlite3.Connection, table: str) -> bool:
    row = sq_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def _columns(sq_conn: sqlite3.Connection, table: str) -> list[str]:
    rows = sq_conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r["name"] for r in rows]


def _row_count_sqlite(sq_conn: sqlite3.Connection, table: str) -> int:
    return sq_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _row_count_pg(pg_conn, table: str) -> int:
    cur = pg_conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return cur.fetchone()[0]


# ── Schema application ─────────────────────────────────────────────────────────

def apply_schema(pg_conn) -> None:
    print(f"\n── Applying schema from {MIGRATION_SQL} ──")
    if not MIGRATION_SQL.exists():
        sys.exit(f"Migration file not found: {MIGRATION_SQL}")

    sql = MIGRATION_SQL.read_text(encoding="utf-8")
    cur = pg_conn.cursor()
    # Split on semicolons; skip empty/comment-only chunks
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if not stmt or stmt.startswith("--"):
            continue
        try:
            cur.execute(stmt)
        except Exception as e:
            pg_conn.rollback()
            sys.exit(f"Schema error:\n  SQL: {stmt[:120]}…\n  Error: {e}")
    pg_conn.commit()
    print("   Schema applied (or already existed).")


# ── Row migration ──────────────────────────────────────────────────────────────

def _adapt_value(v):
    """Convert SQLite types that Postgres won't accept directly."""
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return v


def migrate_table(
    sq_conn: sqlite3.Connection,
    pg_conn,
    table: str,
    batch_size: int,
    dry_run: bool,
) -> tuple[int, int]:
    """Returns (sqlite_rows, pg_rows_written)."""

    if not _table_exists_sqlite(sq_conn, table):
        print(f"   {table:<30} — not in SQLite, skipped")
        return 0, 0

    cols   = _columns(sq_conn, table)
    total  = _row_count_sqlite(sq_conn, table)

    if total == 0:
        print(f"   {table:<30}   0 rows  (empty)")
        return 0, 0

    if dry_run:
        print(f"   {table:<30}  {total:>8,} rows  (dry-run)")
        return total, 0

    # Build parameterised INSERT
    col_list    = ", ".join(f'"{c}"' for c in cols)
    placeholder = ", ".join(["%s"] * len(cols))
    insert_sql  = (
        f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholder}) '
        f'ON CONFLICT DO NOTHING'
    )

    cur        = _pg_cursor(pg_conn)
    written    = 0
    offset     = 0

    while True:
        rows = sq_conn.execute(
            f"SELECT * FROM {table} LIMIT ? OFFSET ?",
            (batch_size, offset),
        ).fetchall()
        if not rows:
            break

        batch = [tuple(_adapt_value(row[c]) for c in cols) for row in rows]
        try:
            cur.executemany(insert_sql, batch)
            pg_conn.commit()
        except Exception as e:
            pg_conn.rollback()
            raise RuntimeError(f"[{table}] Batch write failed at offset {offset}: {e}") from e

        written += len(rows)
        offset  += batch_size

    print(f"   {table:<30}  {written:>8,} rows  ✓")
    return total, written


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate SQLite → Supabase Postgres")
    parser.add_argument("--apply-schema", action="store_true",
                        help="Run 0001_initial.sql in Postgres before copying data")
    parser.add_argument("--dry-run",      action="store_true",
                        help="Print row counts only, write nothing")
    parser.add_argument("--tables",       default="",
                        help="Comma-separated list of tables to migrate (default: all)")
    parser.add_argument("--batch-size",   type=int, default=500,
                        help="INSERT batch size (default 500)")
    args = parser.parse_args()

    if not PG_URL:
        sys.exit(
            "DATABASE_URL is not set.\n"
            "Export it before running:\n"
            "  export DATABASE_URL=postgresql://user:pass@host:5432/dbname\n"
            "  python scripts/migrate_to_supabase.py --apply-schema"
        )

    if not Path(DB_PATH).exists():
        sys.exit(f"SQLite file not found: {DB_PATH}")

    target_tables = (
        [t.strip() for t in args.tables.split(",") if t.strip()]
        if args.tables else ORDERED_TABLES
    )

    print("=" * 60)
    print("KANIDA.AI — SQLite → Postgres Migration")
    print(f"  Source : {DB_PATH}")
    pg_safe = PG_URL.split("@")[-1] if "@" in PG_URL else PG_URL[:40]
    print(f"  Target : postgres://{pg_safe}")
    print(f"  Tables : {len(target_tables)}")
    print(f"  Batch  : {args.batch_size}")
    if args.dry_run:
        print("  Mode   : DRY-RUN (no writes)")
    print("=" * 60)

    sq_conn = _sqlite(DB_PATH)
    pg_conn = _pg(PG_URL)

    if args.apply_schema and not args.dry_run:
        apply_schema(pg_conn)

    print("\n── Copying tables ──")
    total_sq = total_pg = 0
    errors: list[str] = []

    for table in target_tables:
        try:
            sq, pg = migrate_table(sq_conn, pg_conn, table, args.batch_size, args.dry_run)
            total_sq += sq
            total_pg += pg
        except RuntimeError as e:
            print(f"   ERROR: {e}")
            errors.append(str(e))
            break   # stop on first error — investigate before retrying

    sq_conn.close()
    pg_conn.close()

    print("\n" + "=" * 60)
    print("SUMMARY")
    print(f"  SQLite rows scanned : {total_sq:,}")
    if not args.dry_run:
        print(f"  Postgres rows written: {total_pg:,}")
    if errors:
        print(f"  ERRORS ({len(errors)}):")
        for e in errors:
            print(f"    {e}")
        sys.exit(1)
    else:
        print("  Migration complete — no errors.")

    if not args.dry_run:
        print("""
Next steps:
  1. Set DATABASE_URL on Railway so the backend uses Postgres.
  2. Verify in Supabase: row counts should match the numbers above.
  3. Run the pipeline once to ensure Kite data flows into Postgres.
""")


if __name__ == "__main__":
    main()
