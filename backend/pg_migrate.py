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

def _pg_columns(cur, table: str) -> List[str]:
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position",
        (table,),
    )
    return [r[0] for r in cur.fetchall()]


def backfill(tables: Optional[List[str]] = None) -> Dict[str, Any]:
    """Copy rows SQLite -> PG. Idempotent (ON CONFLICT DO NOTHING)."""
    import pgdb
    from psycopg2.extras import execute_values

    targets = tables or OLTP_TABLES
    src = _sqlite_ro()
    src.row_factory = sqlite3.Row
    out: Dict[str, Any] = {}
    t0 = time.time()

    with pgdb.pg_conn() as c:
        cur = c.cursor()
        for t in targets:
            try:
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
    res["verify"] = verify()
    return res


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(run_all(), indent=2, default=str))
