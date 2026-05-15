"""Power User schema initialization — idempotent migration runner.

Called from main.py lifespan at every backend boot. Reads db_schema.sql and
executes each statement. CREATE IF NOT EXISTS makes re-runs safe.

Verification: `init_power_user_schema()` returns a manifest dict listing every
expected table + index along with whether it actually exists post-init. The
main.py lifespan logs warnings if anything is missing.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

log = logging.getLogger("kanida.power_user.db_init")

_HERE = Path(__file__).parent
SCHEMA_SQL_PATH = _HERE / "db_schema.sql"

EXPECTED_TABLES = [
    "power_user_users",
    "power_user_invite_codes",
    "power_user_waitlist",
    "power_user_watchlists",
    "falcon_live_decisions",
    "falcon_replay_cache",
    "power_user_request_log",
    # Sprint 5c-1: Zerodha auto-auth
    "falcon_auth_log",
    "power_user_push_subscriptions",
    "power_user_magic_links",
]

EXPECTED_INDICES = [
    "ix_pu_users_email",
    "ix_pu_users_google_sub",
    "ix_pu_invite_used",
    "ix_pu_watchlist_user",
    "ix_live_dec_date_cycle",
    "ix_live_dec_action",
    "ix_replay_featured",
    "ix_pulog_created",
    "ix_pulog_iphash_t",
    "ix_pulog_user",
    # Sprint 5c-1: Zerodha auto-auth
    "ix_auth_log_attempt",
    "ix_auth_log_status",
    "ix_push_subs_active",
    "ix_magic_links_expires",
]


def init_power_user_schema(db_path: str) -> Dict[str, Any]:
    """Run the schema SQL idempotently. Returns a manifest with verification.

    Args:
        db_path — absolute path to the SQLite file (typically kanida_universe.db)

    Returns:
        {
          "ok": bool,                       # True iff every expected object exists
          "tables_present":  [str],
          "tables_missing":  [str],
          "indices_present": [str],
          "indices_missing": [str],
          "statements_executed": int,
        }
    """
    if not SCHEMA_SQL_PATH.exists():
        raise FileNotFoundError(f"Schema SQL not found at {SCHEMA_SQL_PATH}")

    sql = SCHEMA_SQL_PATH.read_text(encoding="utf-8")

    with sqlite3.connect(db_path, timeout=30.0) as con:
        con.executescript(sql)
        con.commit()

        # Verify by querying sqlite_master
        present_tables = {
            r[0] for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        present_indices = {
            r[0] for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }

    tables_present  = [t for t in EXPECTED_TABLES  if t in present_tables]
    tables_missing  = [t for t in EXPECTED_TABLES  if t not in present_tables]
    indices_present = [i for i in EXPECTED_INDICES if i in present_indices]
    indices_missing = [i for i in EXPECTED_INDICES if i not in present_indices]

    ok = not tables_missing and not indices_missing

    manifest: Dict[str, Any] = {
        "ok":                  ok,
        "tables_present":      tables_present,
        "tables_missing":      tables_missing,
        "indices_present":     indices_present,
        "indices_missing":     indices_missing,
        "statements_executed": sql.count(";") - sql.count("--") + sql.count("\n--"),  # rough
    }
    if ok:
        log.info("power_user schema OK: %d tables, %d indices",
                 len(tables_present), len(indices_present))
    else:
        log.error("power_user schema INCOMPLETE — missing tables=%s indices=%s",
                  tables_missing, indices_missing)
    return manifest


def reset_power_user_schema(db_path: str, confirm: str = "") -> int:
    """DROP every power_user_* and falcon_live_/falcon_replay_ table — destructive.

    Used only by tests / admin reset. Requires confirm='I_KNOW_THIS_DROPS_USER_DATA'
    to guard against accidental invocation.
    """
    if confirm != "I_KNOW_THIS_DROPS_USER_DATA":
        raise PermissionError("reset_power_user_schema requires explicit confirmation")
    with sqlite3.connect(db_path, timeout=30.0) as con:
        dropped = 0
        for t in EXPECTED_TABLES:
            con.execute(f"DROP TABLE IF EXISTS {t}")
            dropped += 1
        con.commit()
    log.warning("power_user schema RESET — %d tables dropped", dropped)
    return dropped


if __name__ == "__main__":
    # CLI: python -m power_user.db_init [db_path]
    import os, sys
    db = sys.argv[1] if len(sys.argv) > 1 else \
         os.environ.get("KANIDA_DB_PATH",
                        str(_HERE.parent.parent / "data" / "db" / "kanida_universe.db"))
    print(f"Initializing power_user schema in: {db}")
    manifest = init_power_user_schema(db)
    print(f"  ok: {manifest['ok']}")
    print(f"  tables present: {len(manifest['tables_present'])}/{len(EXPECTED_TABLES)}")
    print(f"  indices present: {len(manifest['indices_present'])}/{len(EXPECTED_INDICES)}")
    if manifest["tables_missing"]:
        print(f"  MISSING TABLES: {manifest['tables_missing']}")
    if manifest["indices_missing"]:
        print(f"  MISSING INDICES: {manifest['indices_missing']}")
