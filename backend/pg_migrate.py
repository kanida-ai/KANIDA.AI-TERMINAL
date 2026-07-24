"""
KANIDA.AI — SQLite -> Postgres migration runner (Stage 3).

Runs INSIDE the container (RDS lives in a private subnet and is reachable only
from the app security group), so this is invoked either as a module
(`python -m pg_migrate`) or via the admin route.

WHAT IT DOES
  1. apply_schema()  — executes deploy/migrations/*.sql against PG. Every
     statement is IF NOT EXISTS, so re-running is safe.
  2. backfill()      — copies the OLTP tables SQLite -> PG in batches, with
     ON CONFLICT DO NOTHING, so a re-run tops up rather than duplicating.
  3. fix_sequences() — CRITICAL. Tables whose SQLite `INTEGER PRIMARY KEY`
     became `BIGSERIAL` get a sequence that still starts at 1 after a backfill.
     Without this, the first live INSERT collides with a migrated row. Resets
     every sequence to MAX(pk).
  4. verify()        — per-table row counts on BOTH sides, so a short copy is
     visible instead of assumed-good.

SAFETY
  * READS SQLite read-only (mode=ro). The live SQLite data is never mutated, so
    this is non-destructive and can be run repeatedly while we still serve from
    SQLite.
  * Does NOT flip any routing. Cutover is a separate, per-module change.
  * Market/engine tables are never touched — they stay on SQLite by design.
"""
from __future__ import annotations

import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parent

# Same list the schema generator used (per-user transactional state only).
OLTP_TABLES: List[str] = [
    "power_user_users", "power_user_invite_codes", "power_user_waitlist",
    "power_user_subscriptions", "power_user_watchlists",
    "power_user_push_subscriptions", "power_user_magic_links",
    "power_user_billing_events", "power_user_request_log",
    "broker_accounts",
    "autotrade_sessions", "autotrade_positions", "autotrade_order_events",
    "autotrade_alerts", "autotrade_recon_alerts", "autotrade_slippage",
    "autotrade_claims", "autotrade_kill_switch_log", "autotrade_ladders",
    "autotrade_portfolio_snapshots", "autotrade_session_account_allocations",
    "autotrade_config_presets", "autotrade_config_edits",
    "autotrade_broker_profiles",
    "portfolio_positions", "portfolio_event_log", "portfolio_equity_history",
    "portfolio_monthly_performance", "portfolio_yearly_performance",
    "portfolio_definitions",
    "falcon_trade_events", "falcon_trade_orders", "falcon_trade_runs",
    "falcon_position_state", "falcon_position_first_seen",
    "falcon_premarket_staging", "falcon_live_decisions",
    "falcon_auth_log", "falcon_notifications_out",
    "workflow_health", "sysagent_incidents", "sysagent_health_snapshots",
    "strategy_visibility", "falcon_trail_config",
]

BATCH = 1000


def _sqlite_path() -> str:
    """The SERVING SQLite DB (entrypoint points these at /localdb in cloud)."""
    return (os.environ.get("FALCON_DB_PATH")
            or os.environ.get("POWER_DB_PATH")
            or str(_REPO / "data" / "db" / "kanida_universe.db"))


def _sqlite_ro() -> sqlite3.Connection:
    p = _sqlite_path()
    if not os.path.exists(p):
        raise RuntimeError(f"source SQLite not found: {p}")
    return sqlite3.connect(f"file:{p}?mode=ro", uri=True)


def migrations_dir() -> Path:
    return _REPO / "deploy" / "migrations"


# ── 1. schema ────────────────────────────────────────────────────────────────

def apply_schema() -> Dict[str, Any]:
    """Execute every deploy/migrations/*.sql against Postgres, in name order."""
    import pgdb
    d = migrations_dir()
    files = sorted(d.glob("*.sql")) if d.exists() else []
    if not files:
        return {"ok": False, "error": f"no .sql files in {d}"}
    applied = []
    with pgdb.pg_conn() as c:
        cur = c.cursor()
        for f in files:
            sql = f.read_text(encoding="utf-8")
            cur.execute(sql)
            applied.append(f.name)
            log.info("pg_migrate: applied %s", f.name)
    return {"ok": True, "applied": applied}


# ── 2. backfill ──────────────────────────────────────────────────────────────

def _fk_ordered(tables: List[str]) -> List[str]:
    """Order tables so a FK's target loads before its referrer.

    Only matters when FK enforcement could NOT be disabled (see backfill); with
    session_replication_role=replica the order is irrelevant. Reads the real FK
    graph out of Postgres, so it can't drift from the schema. Falls back to the
    given order on any error or cycle — never blocks the migration.
    """
    try:
        import pgdb
        with pgdb.pg_conn() as c:
            cur = c.cursor()
            cur.execute("""
                SELECT tc.table_name, ccu.table_name AS refs
                  FROM information_schema.table_constraints tc
                  JOIN information_schema.constraint_column_usage ccu
                    ON tc.constraint_name = ccu.constraint_name
                 WHERE tc.constraint_type = 'FOREIGN KEY'
                   AND tc.table_schema = 'public'
            """)
            edges = cur.fetchall()
    except Exception:
        return list(tables)

    want = set(tables)
    deps = {t: set() for t in tables}
    for child, parent in edges:
        if child in want and parent in want and child != parent:
            deps[child].add(parent)

    ordered, placed, remaining = [], set(), list(tables)
    while remaining:
        progress = False
        for t in list(remaining):
            if deps[t] <= placed:
                ordered.append(t); placed.add(t); remaining.remove(t); progress = True
        if not progress:
            ordered.extend(remaining)      # cycle — emit rest as-is
            break
    return ordered


def check_orphans() -> Dict[str, Any]:
    """Report rows whose FK target is missing (pre-existing SQLite data issues).

    These are copied faithfully by the backfill (FK enforcement is off during
    load) — this surfaces them so they can be cleaned deliberately instead of
    being silently dropped or silently corrupting referential integrity.
    """
    import pgdb
    found: Dict[str, Any] = {}
    with pgdb.pg_conn() as c:
        cur = c.cursor()
        cur.execute("""
            SELECT tc.table_name, kcu.column_name,
                   ccu.table_name AS ref_table, ccu.column_name AS ref_col
              FROM information_schema.table_constraints tc
              JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
              JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
             WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'
        """)
        for tbl, col, rtbl, rcol in cur.fetchall():
            try:
                cur.execute(
                    f'SELECT COUNT(*) FROM "{tbl}" c '
                    f'WHERE c."{col}" IS NOT NULL AND NOT EXISTS '
                    f'(SELECT 1 FROM "{rtbl}" p WHERE p."{rcol}" = c."{col}")')
                n = cur.fetchone()[0]
                if n:
                    found[f"{tbl}.{col} -> {rtbl}.{rcol}"] = n
            except Exception as e:
                found[f"{tbl}.{col}"] = f"check failed: {str(e)[:120]}"
    return {"orphan_groups": len(found), "details": found}


def _pg_columns(cur, table: str) -> List[str]:
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position",
        (table,),
    )
    return [r[0] for r in cur.fetchall()]


def backfill(tables: Optional[List[str]] = None) -> Dict[str, Any]:
    """Copy rows SQLite -> PG. Idempotent (ON CONFLICT DO NOTHING).

    TWO HARD-WON DETAILS (both hit live on the first real run):

    1. ONE TRANSACTION PER TABLE. Originally the whole backfill shared a single
       transaction, so the first failure aborted it and every later table died
       with InFailedSqlTransaction — and the rollback discarded the 87k rows
       that HAD copied. A fresh connection per table isolates failures.

    2. FK ENFORCEMENT OFF DURING LOAD. SQLite never enforced foreign keys, so
       the source legitimately contains orphans (e.g. portfolio_positions
       .portfolio_id=481 with no portfolio_definitions row). Postgres rightly
       rejects those. `SET session_replication_role = replica` is the standard
       bulk-load escape hatch: it copies the data FAITHFULLY, orphans included,
       instead of silently dropping rows. Orphans are reported by check_orphans()
       so they can be cleaned deliberately rather than lost in a migration.
       If the role lacks permission we fall back to enforced FKs (and the
       dependency ordering below then matters).
    """
    import pgdb
    from psycopg2.extras import execute_values

    targets = tables or _fk_ordered(OLTP_TABLES)
    src = _sqlite_ro()
    src.row_factory = sqlite3.Row
    out: Dict[str, Any] = {}
    t0 = time.time()

    for t in targets:
        try:
            with pgdb.pg_conn() as c:          # isolated txn per table
                cur = c.cursor()
                try:
                    cur.execute("SET session_replication_role = replica")
                except Exception:
                    pass                        # not permitted -> FKs enforced
                pg_cols = _pg_columns(cur, t)
                if not pg_cols:
                    out[t] = {"skipped": "table absent in PG"}
                    continue
                try:
                    sq_cols = [r[1] for r in src.execute(f'PRAGMA table_info("{t}")')]
                except Exception:
                    sq_cols = []
                if not sq_cols:
                    out[t] = {"skipped": "table absent in SQLite"}
                    continue
                # Only columns present on BOTH sides — tolerates drift.
                cols = [col for col in sq_cols if col in pg_cols]
                if not cols:
                    out[t] = {"skipped": "no overlapping columns"}
                    continue
                collist = ", ".join(f'"{c_}"' for c_ in cols)
                rows = src.execute(f'SELECT {collist} FROM "{t}"')
                n = 0
                while True:
                    chunk = rows.fetchmany(BATCH)
                    if not chunk:
                        break
                    vals = [tuple(r[c_] for c_ in cols) for r in chunk]
                    execute_values(
                        cur,
                        f'INSERT INTO "{t}" ({collist}) VALUES %s '
                        f"ON CONFLICT DO NOTHING",
                        vals,
                        page_size=BATCH,
                    )
                    n += len(vals)
                out[t] = {"copied": n, "columns": len(cols),
                          "dropped_columns": sorted(set(sq_cols) - set(cols))}
                log.info("pg_migrate: %s -> %s rows", t, n)
        except Exception as e:
            out[t] = {"error": f"{type(e).__name__}: {str(e)[:200]}"}
            log.exception("pg_migrate: backfill failed for %s", t)
    src.close()
    return {"ok": True, "elapsed_sec": round(time.time() - t0, 1), "tables": out}


# ── 3. sequences ─────────────────────────────────────────────────────────────

def fix_sequences() -> Dict[str, Any]:
    """Advance every BIGSERIAL sequence past the migrated MAX(pk).

    Without this the first live INSERT reuses id=1 and collides with copied
    data. Uses pg_get_serial_sequence so only real serial columns are touched.
    """
    import pgdb
    fixed: Dict[str, Any] = {}
    with pgdb.pg_conn() as c:
        cur = c.cursor()
        for t in OLTP_TABLES:
            try:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema='public' AND table_name=%s "
                    "AND column_default LIKE 'nextval%%'", (t,))
                serial_cols = [r[0] for r in cur.fetchall()]
                for col in serial_cols:
                    cur.execute(
                        "SELECT setval(pg_get_serial_sequence(%s, %s), "
                        "COALESCE((SELECT MAX(%s) FROM %s), 1), true)"
                        % ("%s", "%s", f'"{col}"', f'"{t}"'), (t, col))
                    fixed[f"{t}.{col}"] = cur.fetchone()[0]
            except Exception as e:
                fixed[t] = f"ERROR {type(e).__name__}: {str(e)[:150]}"
    return {"ok": True, "sequences": fixed}


# ── 4. verify ────────────────────────────────────────────────────────────────

def verify() -> Dict[str, Any]:
    """Row counts on both sides. Mismatches are reported, never swallowed."""
    import pgdb
    src = _sqlite_ro()
    rows: Dict[str, Any] = {}
    mismatches = 0
    with pgdb.pg_conn() as c:
        cur = c.cursor()
        for t in OLTP_TABLES:
            try:
                s = src.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            except Exception:
                s = None
            try:
                cur.execute(f'SELECT COUNT(*) FROM "{t}"')
                p = cur.fetchone()[0]
            except Exception:
                p = None
            ok = (s is not None and p is not None and p >= s)
            if not ok:
                mismatches += 1
            rows[t] = {"sqlite": s, "postgres": p, "ok": ok}
    src.close()
    return {"ok": mismatches == 0, "mismatches": mismatches, "tables": rows}


def run_all() -> Dict[str, Any]:
    """schema -> backfill -> sequences -> verify."""
    res: Dict[str, Any] = {}
    res["schema"] = apply_schema()
    if not res["schema"].get("ok"):
        return res
    res["backfill"] = backfill()
    res["sequences"] = fix_sequences()
    res["orphans"] = check_orphans()
    res["verify"] = verify()
    return res


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(run_all(), indent=2, default=str))
