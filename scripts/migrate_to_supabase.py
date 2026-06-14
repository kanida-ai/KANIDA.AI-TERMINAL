"""
KANIDA.AI — App-DB porting: SQLite → Supabase (Postgres)
========================================================
ONE-TIME data port for the Stage-1 launch (module M1 — Infra).

It moves the **App DB** off the laptop into Supabase Postgres:

  • PRIMARY SOURCE  data/db/kanida_universe.db   (573M PROD)
        → signals / features / patterns / ohlc / sectors / persona-portfolio
          tables + every power_user_* / falcon_trade_* / falcon_position_state /
          falcon_premarket_staging table that a user request (or the operator
          surface) touches at runtime.

  • FOLD-IN SOURCE  data/db/kanida_quant.db      (83M legacy)
        → kite_tokens + falcon_auth_log ONLY. Everything else in this file is
          legacy R&D (live_opportunities, pattern_library, trade_log, …) and is
          DROPPED, per MASTER_SPEC §1.5. After this port the file is retired.

  • NOT PORTED      universe_engine/data/db/kanida_universe.db   (14G R&D)
        → stays on the machine as the research warehouse (one-way publish wall).

The schema itself is created by the application's own idempotent initialisers
(power_user/db_init.py, falcon/db_init.py, M2 migration 0001_billing.sql) which
run against DATABASE_URL at boot. This script's job is DATA, not DDL — so it
uses --apply-schema only as a convenience hook to the M2 migration file.

Usage
-----
    # 0. dry run — counts only, writes nothing
    DATABASE_URL=postgresql://... python scripts/migrate_to_supabase.py --dry-run

    # 1. real port (schema must already exist — boot the app once, or pass
    #    --apply-schema to run the M2 billing migration first)
    DATABASE_URL=postgresql://... python scripts/migrate_to_supabase.py

Flags
-----
  --dry-run        Print per-table row counts on both sides, write nothing.
  --apply-schema   Run backend/power_user/migrations/0001_billing.sql first.
  --tables t1,t2   Restrict to these tables (must be in the manifest).
  --batch-size N   Rows per INSERT batch (default 1000).
  --truncate       TRUNCATE each target table before loading (clean re-run).
  --app-db PATH    Override the PROD App-DB source path.
  --legacy-db PATH Override the legacy kanida_quant.db source path.

Idempotency
-----------
Every INSERT uses `ON CONFLICT DO NOTHING`, so re-running after a partial port
is safe: already-present primary keys are skipped, missing rows are added. The
chunked offset loop bounds memory on the 573M PROD DB. For a guaranteed-clean
reload, pass --truncate (children-before-parents order is handled below).

This script never connects to a cloud service unless DATABASE_URL is set by the
operator at runtime — there are no embedded credentials (INV5).
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent

# ── Source DBs ──────────────────────────────────────────────────────────────
# PROD App DB (573M) — the primary source. Override via env or --app-db.
APP_DB_PATH = os.environ.get(
    "POWER_DB_PATH",
    os.environ.get("FALCON_DB_PATH", str(ROOT / "data" / "db" / "kanida_universe.db")),
)
# Legacy auth DB (83M) — we fold in kite_tokens + falcon_auth_log only.
LEGACY_DB_PATH = os.environ.get(
    "KANIDA_DB_PATH", str(ROOT / "data" / "db" / "kanida_quant.db")
)

PG_URL = os.environ.get("DATABASE_URL", "")

# M2 owns the billing migration; --apply-schema runs it as a convenience.
M2_MIGRATION_SQL = ROOT / "backend" / "power_user" / "migrations" / "0001_billing.sql"


# ── Table manifest ──────────────────────────────────────────────────────────
# These are the App-DB tables that live user requests + the operator surface +
# the daily/weekly pipeline touch at runtime. Enumerated from the actual SQL in
# backend/power_user/* and backend/falcon/* (NOT the 14G R&D warehouse).
#
# Order: parents before children where FKs/soft-refs exist, so a --truncate
# reload and the load order both stay consistent.

# Engine read-tables (written by the offline pipeline / weekly publish; read by
# every /power request). Big ones (ohlc_daily) are chunked.
ENGINE_TABLES = [
    "universe_master",            # symbol registry — referenced by explainer joins
    "falcon_sectors",             # sector tags
    "ohlc_daily",                 # daily bars (LARGE — chunked)
    "ohlc_weekly",                # weekly bars
    "falcon_features",            # per-stock daily feature vectors
    "falcon_pattern_candidates",  # mined candidates (published weekly)
    "falcon_promoted_patterns",   # promoted set (published weekly)
    "falcon_pattern_taxonomy",    # plain-English + regime tags (Top-10 persona)
    "falcon_signals_live",        # emitted picks per day — the /power/today core
    "falcon_signal_runs",         # cron run audit log
    "falcon_notifications_out",   # email + in-app queue
    "falcon_top10_audit",         # Top-10 outcome audit
    "falcon_kv_store",            # portfolio KV snapshots
    "falcon_kv_trades",           # portfolio KV trades
]

# Power-User product tables (auth, billing, watchlists, replay, portfolios).
POWER_USER_TABLES = [
    "power_user_users",
    "power_user_invite_codes",
    "power_user_waitlist",
    "power_user_watchlists",
    "power_user_request_log",
    "power_user_push_subscriptions",
    "power_user_magic_links",
    "power_user_subscriptions",        # M2
    "power_user_billing_events",       # M2
    "falcon_live_decisions",           # scheduler-computed live tier
    "falcon_replay_cache",             # featured + on-demand replays
    # Co-Trader virtual portfolios (persona engine)
    "portfolio_definitions",
    "portfolio_positions",
    "portfolio_equity_history",
    "portfolio_event_log",
    "portfolio_yearly_performance",
    "portfolio_monthly_performance",
]

# Falcon operator / auto-trade state (INV2: we PORT the data, never touch the
# execution logic). These hold live position + order + premarket state.
FALCON_TRADE_TABLES = [
    "falcon_engine_config",
    "falcon_trail_config",
    "falcon_trade_runs",
    "falcon_trade_orders",
    "falcon_trade_events",
    "falcon_position_state",
    "falcon_position_first_seen",
    "falcon_premarket_staging",
    "falcon_eod_runs",
    "falcon_job_runs",
]

# Tables that get folded in FROM the legacy kanida_quant.db (the only live
# tables remaining there). Everything else in that file is dropped.
LEGACY_FOLD_TABLES = [
    "kite_tokens",       # admin Kite access token — read by every live-tier req
    "falcon_auth_log",   # Zerodha auto-auth audit log
]

# Full App-DB load order (PROD source).
APP_DB_TABLES = ENGINE_TABLES + POWER_USER_TABLES + FALCON_TRADE_TABLES

# falcon_auth_log already lives in the PROD App DB at runtime (auth_scheduler
# writes it via POWER_DB_PATH). If both sources carry it, the PROD copy wins
# and ON CONFLICT DO NOTHING dedupes the legacy fold-in.


# ── SQLite helpers ──────────────────────────────────────────────────────────

def _sqlite(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


# ── Postgres helpers ────────────────────────────────────────────────────────

def _pg(url: str):
    try:
        import psycopg2  # noqa: WPS433
    except ImportError:
        sys.exit("psycopg2 not installed. Run: pip install psycopg2-binary")
    conn = psycopg2.connect(url)
    conn.autocommit = False
    return conn


def _pg_table_exists(pg_conn, table: str) -> bool:
    cur = pg_conn.cursor()
    cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
    return cur.fetchone()[0] is not None


def _pg_columns(pg_conn, table: str) -> list[str]:
    cur = pg_conn.cursor()
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=%s",
        (table,),
    )
    return [r[0] for r in cur.fetchall()]


def apply_schema(pg_conn) -> None:
    """Convenience hook: run the M2 billing migration so power_user_* tables
    exist before data load. Normal flow is to boot the app once (its db_init
    runners create everything); this is the offline alternative."""
    print(f"\n── Applying M2 migration {M2_MIGRATION_SQL} ──")
    if not M2_MIGRATION_SQL.exists():
        print(f"   WARN: {M2_MIGRATION_SQL} not found — skipping (boot the app to create schema)")
        return
    sql = M2_MIGRATION_SQL.read_text(encoding="utf-8")
    cur = pg_conn.cursor()
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if not stmt or stmt.startswith("--"):
            continue
        try:
            cur.execute(stmt)
        except Exception as e:  # noqa: BLE001
            pg_conn.rollback()
            sys.exit(f"Schema error:\n  SQL: {stmt[:120]}…\n  Error: {e}")
    pg_conn.commit()
    print("   M2 migration applied (or already existed).")


# ── Per-table WHERE filters (keep rows out that would break PG constraints) ──
TABLE_FILTERS: dict[str, str] = {
    # ohlc_daily historically carried yfinance rows that violate the PG source
    # CHECK — exclude them, matching the original port behaviour.
    "ohlc_daily": "source IS NULL OR source NOT IN ('yfinance')",
}


def _adapt_value(v):
    """Convert SQLite types Postgres won't accept directly."""
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return v


def migrate_table(
    sq_conn: sqlite3.Connection,
    pg_conn,
    table: str,
    batch_size: int,
    dry_run: bool,
    truncate: bool,
) -> tuple[int, int]:
    """Copy one table SQLite → Postgres, chunked + idempotent.

    Returns (sqlite_rows_scanned, pg_rows_attempted)."""
    if not _table_exists(sq_conn, table):
        print(f"   {table:<32} — not in source SQLite, skipped")
        return 0, 0

    if not dry_run and not _pg_table_exists(pg_conn, table):
        print(f"   {table:<32} — NOT in Postgres (schema missing) — skipped")
        return 0, 0

    where = TABLE_FILTERS.get(table, "")
    where_clause = f"WHERE {where}" if where else ""
    total = sq_conn.execute(
        f"SELECT COUNT(*) FROM {table} {where_clause}"
    ).fetchone()[0]

    if total == 0:
        print(f"   {table:<32}        0 rows  (empty)")
        return 0, 0

    if dry_run:
        print(f"   {table:<32} {total:>9,} rows  (dry-run)")
        return total, 0

    # Intersect columns so an extra column on either side never breaks the load
    # (additive migrations, INV7 — the PG side may have billing columns the
    # SQLite side lacks, and vice-versa).
    sq_cols = _columns(sq_conn, table)
    pg_cols = set(_pg_columns(pg_conn, table))
    cols = [c for c in sq_cols if c in pg_cols]
    if not cols:
        print(f"   {table:<32} — no shared columns with Postgres, skipped")
        return total, 0

    if truncate:
        cur = pg_conn.cursor()
        cur.execute(f'TRUNCATE TABLE "{table}" CASCADE')
        pg_conn.commit()

    col_list    = ", ".join(f'"{c}"' for c in cols)
    placeholder = ", ".join(["%s"] * len(cols))
    insert_sql  = (
        f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholder}) '
        f'ON CONFLICT DO NOTHING'
    )

    cur     = pg_conn.cursor()
    written = 0
    offset  = 0
    # Stable ordering so LIMIT/OFFSET paging is deterministic across batches.
    order = "ORDER BY rowid" if _has_rowid(sq_conn, table) else ""

    while True:
        rows = sq_conn.execute(
            f"SELECT {col_list} FROM {table} {where_clause} {order} LIMIT ? OFFSET ?",
            (batch_size, offset),
        ).fetchall()
        if not rows:
            break
        batch = [tuple(_adapt_value(r[c]) for c in cols) for r in rows]
        try:
            cur.executemany(insert_sql, batch)
            pg_conn.commit()
        except Exception as e:  # noqa: BLE001
            pg_conn.rollback()
            raise RuntimeError(
                f"[{table}] batch write failed at offset {offset}: {e}"
            ) from e
        written += len(rows)
        offset  += batch_size
        if total > 50_000 and offset % (batch_size * 50) == 0:
            print(f"      {table}: {offset:,}/{total:,} …")

    print(f"   {table:<32} {written:>9,} rows  ✓")
    return total, written


def _has_rowid(conn: sqlite3.Connection, table: str) -> bool:
    """WITHOUT ROWID tables can't be ORDER BY rowid. Detect cheaply."""
    try:
        conn.execute(f"SELECT rowid FROM {table} LIMIT 1").fetchone()
        return True
    except sqlite3.OperationalError:
        return False


# ── Main ────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Port App-DB SQLite → Supabase Postgres")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply-schema", action="store_true",
                        help="Run M2 billing migration before loading data")
    parser.add_argument("--tables", default="",
                        help="Comma-separated subset of the manifest")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--truncate", action="store_true",
                        help="TRUNCATE target tables before load (clean re-run)")
    parser.add_argument("--app-db", default=APP_DB_PATH,
                        help="PROD App-DB source (kanida_universe.db)")
    parser.add_argument("--legacy-db", default=LEGACY_DB_PATH,
                        help="Legacy source (kanida_quant.db) for kite_tokens/auth_log")
    args = parser.parse_args()

    if not PG_URL:
        sys.exit(
            "DATABASE_URL is not set.\n"
            "  export DATABASE_URL=postgresql://user:pass@host:5432/dbname\n"
            "  python scripts/migrate_to_supabase.py --dry-run"
        )

    app_db    = args.app_db
    legacy_db = args.legacy_db
    if not Path(app_db).exists():
        sys.exit(f"App-DB source not found: {app_db}")

    subset = {t.strip() for t in args.tables.split(",") if t.strip()}
    def _selected(tabs: list[str]) -> list[str]:
        return [t for t in tabs if (not subset or t in subset)]

    app_tables    = _selected(APP_DB_TABLES)
    legacy_tables = _selected(LEGACY_FOLD_TABLES)

    print("=" * 64)
    print("KANIDA.AI — App-DB port: SQLite → Postgres")
    print(f"  App source    : {app_db}")
    print(f"  Legacy source : {legacy_db}")
    pg_safe = PG_URL.split("@")[-1] if "@" in PG_URL else PG_URL[:40]
    print(f"  Target        : postgres://{pg_safe}")
    print(f"  App tables    : {len(app_tables)}   Legacy fold-in: {len(legacy_tables)}")
    print(f"  Batch         : {args.batch_size}"
          + ("   TRUNCATE-first" if args.truncate else ""))
    if args.dry_run:
        print("  Mode          : DRY-RUN (no writes)")
    print("=" * 64)

    pg_conn = _pg(PG_URL)
    if args.apply_schema and not args.dry_run:
        apply_schema(pg_conn)

    total_sq = total_pg = 0
    errors: list[str] = []

    # ── PROD App DB ──
    print("\n── PROD App DB (kanida_universe.db) ──")
    sq_app = _sqlite(app_db)
    try:
        for table in app_tables:
            try:
                sq, pg = migrate_table(
                    sq_app, pg_conn, table, args.batch_size, args.dry_run, args.truncate
                )
                total_sq += sq
                total_pg += pg
            except RuntimeError as e:
                print(f"   ERROR: {e}")
                errors.append(str(e))
                break
    finally:
        sq_app.close()

    # ── Legacy fold-in (kite_tokens + falcon_auth_log) ──
    if legacy_tables and not errors:
        print("\n── Legacy fold-in (kanida_quant.db) ──")
        if not Path(legacy_db).exists():
            print(f"   WARN: legacy DB not found at {legacy_db} — skipping fold-in.")
            print("         (kite_tokens will be empty until first token refresh on host.)")
        else:
            sq_leg = _sqlite(legacy_db)
            try:
                for table in legacy_tables:
                    try:
                        sq, pg = migrate_table(
                            sq_leg, pg_conn, table, args.batch_size,
                            args.dry_run, truncate=False,  # never truncate on fold-in
                        )
                        total_sq += sq
                        total_pg += pg
                    except RuntimeError as e:
                        print(f"   ERROR: {e}")
                        errors.append(str(e))
                        break
            finally:
                sq_leg.close()

    pg_conn.close()

    print("\n" + "=" * 64)
    print("SUMMARY")
    print(f"  SQLite rows scanned  : {total_sq:,}")
    if not args.dry_run:
        print(f"  Postgres rows loaded : {total_pg:,}  (ON CONFLICT skips dupes)")
    if errors:
        print(f"  ERRORS ({len(errors)}):")
        for e in errors:
            print(f"    {e}")
        sys.exit(1)
    print("  Done — no errors.")

    if not args.dry_run:
        print("""
Next:
  1. Confirm row counts in Supabase match the numbers above.
  2. Set DATABASE_URL on the host so the backend uses Postgres.
  3. Boot the backend; verify /power/today loads + auth status reads kite_tokens.
  4. Once verified off-laptop, kanida_quant.db can be retired (RUNBOOK §8).
""")


if __name__ == "__main__":
    main()
