"""
Stage 2: generate the Postgres DDL for the OLTP domain from the live SQLite DDL.

Reads the real CREATE TABLE / CREATE INDEX statements out of kanida_universe.db
and translates SQLite dialect -> Postgres. Deliberately mechanical + explicit:
every translation is listed so it can be reviewed, and anything unrecognised is
reported LOUDLY rather than silently emitted as broken SQL.

MARKET tables (ohlc_*, falcon_features, patterns, signals, universe_master ...)
are intentionally EXCLUDED — they stay on SQLite (single writer, read-heavy).
"""
import re
import sqlite3
import os

ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
SRC = os.path.join(ROOT, "data", "db", "kanida_universe.db")
OUT = os.path.join(ROOT, "deploy", "migrations", "001_oltp_schema.sql")

# Per-user / transactional state -> Postgres.
OLTP_TABLES = [
    # identity + access
    "power_user_users", "power_user_invite_codes", "power_user_waitlist",
    "power_user_subscriptions", "power_user_watchlists",
    "power_user_push_subscriptions", "power_user_magic_links",
    "power_user_billing_events", "power_user_request_log",
    # broker linkage (vault-encrypted creds live here)
    "broker_accounts",
    # autotrade engine state
    "autotrade_sessions", "autotrade_positions", "autotrade_order_events",
    "autotrade_alerts", "autotrade_recon_alerts", "autotrade_slippage",
    "autotrade_claims", "autotrade_kill_switch_log", "autotrade_ladders",
    "autotrade_portfolio_snapshots", "autotrade_session_account_allocations",
    "autotrade_config_presets", "autotrade_config_edits",
    "autotrade_broker_profiles",
    # portfolio / performance
    "portfolio_positions", "portfolio_event_log", "portfolio_equity_history",
    "portfolio_monthly_performance", "portfolio_yearly_performance",
    "portfolio_definitions",
    # live trade lifecycle
    "falcon_trade_events", "falcon_trade_orders", "falcon_trade_runs",
    "falcon_position_state", "falcon_position_first_seen",
    "falcon_premarket_staging", "falcon_live_decisions",
    "falcon_auth_log", "falcon_notifications_out",
    # ops / health
    "workflow_health", "sysagent_incidents", "sysagent_health_snapshots",
    # app config (small, but written at runtime by users/operator)
    "strategy_visibility", "falcon_trail_config",
]


def translate(ddl: str) -> tuple[str, list[str]]:
    """SQLite CREATE ... -> Postgres. Returns (sql, warnings)."""
    warns: list[str] = []
    s = ddl.strip().rstrip(";")

    # Autoincrement PK -> BIGSERIAL (must run before generic INTEGER mapping)
    s = re.sub(r"\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b", "BIGSERIAL PRIMARY KEY",
               s, flags=re.I)
    s = re.sub(r"\bINTEGER\s+PRIMARY\s+KEY\b", "BIGSERIAL PRIMARY KEY", s, flags=re.I)

    # Types
    s = re.sub(r"\bDATETIME\b", "TIMESTAMPTZ", s, flags=re.I)
    s = re.sub(r"\bBLOB\b", "BYTEA", s, flags=re.I)
    s = re.sub(r"\bREAL\b|\bDOUBLE\b|\bFLOAT\b", "DOUBLE PRECISION", s, flags=re.I)
    s = re.sub(r"\bNUMERIC\b", "NUMERIC", s, flags=re.I)
    s = re.sub(r"\bBOOLEAN\b", "BOOLEAN", s, flags=re.I)
    # bare INTEGER (not already rewritten) -> BIGINT
    s = re.sub(r"\bINTEGER\b", "BIGINT", s, flags=re.I)
    s = re.sub(r"\bINT\b(?!\w)", "BIGINT", s, flags=re.I)
    # VARCHAR(n)/TEXT stay as-is

    # Defaults / functions
    s = re.sub(r"datetime\('now'\)", "NOW()", s, flags=re.I)
    s = re.sub(r"CURRENT_TIMESTAMP", "NOW()", s, flags=re.I)
    s = re.sub(r"date\('now'\)", "CURRENT_DATE", s, flags=re.I)

    # SQLite-isms with no PG equivalent
    if re.search(r"\bWITHOUT\s+ROWID\b", s, flags=re.I):
        s = re.sub(r"\bWITHOUT\s+ROWID\b", "", s, flags=re.I)
        warns.append("stripped WITHOUT ROWID")
    if re.search(r"\bAUTOINCREMENT\b", s, flags=re.I):
        s = re.sub(r"\bAUTOINCREMENT\b", "", s, flags=re.I)
        warns.append("stripped stray AUTOINCREMENT")
    if re.search(r"\bCOLLATE\s+NOCASE\b", s, flags=re.I):
        s = re.sub(r"\bCOLLATE\s+NOCASE\b", "", s, flags=re.I)
        warns.append("stripped COLLATE NOCASE (use CITEXT or lower() index if needed)")

    # IF NOT EXISTS so the migration is re-runnable
    s = re.sub(r"^CREATE\s+TABLE\s+(?!IF\s+NOT\s+EXISTS)", "CREATE TABLE IF NOT EXISTS ",
               s, flags=re.I)
    s = re.sub(r"^CREATE\s+(UNIQUE\s+)?INDEX\s+(?!IF\s+NOT\s+EXISTS)",
               lambda m: f"CREATE {m.group(1) or ''}INDEX IF NOT EXISTS ", s, flags=re.I)

    # quoted identifiers: SQLite [x] / `x` -> PG "x"
    s = re.sub(r"\[([A-Za-z_][A-Za-z0-9_]*)\]", r'"\1"', s)
    s = re.sub(r"`([A-Za-z_][A-Za-z0-9_]*)`", r'"\1"', s)

    for kw in ("PRAGMA", "AUTOINCREMENT"):
        if re.search(rf"\b{kw}\b", s, flags=re.I):
            warns.append(f"UNTRANSLATED {kw} remains")
    return s + ";", warns


def dependency_order(ddls: dict) -> list:
    """Order tables so a FOREIGN KEY's target is CREATEd before the referrer.

    SQLite tolerates any creation order; Postgres does not — a REFERENCES to a
    table that doesn't exist yet fails the whole migration (hit live:
    portfolio_positions -> portfolio_definitions). Simple Kahn topological sort;
    on a cycle we fall back to the declared order (PG would then need the FK
    added post-hoc, which we'd see immediately as a loud failure).
    """
    names = set(ddls)
    deps = {}
    for t, ddl in ddls.items():
        refs = set()
        for m in re.finditer(r'REFERENCES\s+"?([A-Za-z_][A-Za-z0-9_]*)"?', ddl, re.I):
            r = m.group(1)
            if r in names and r != t:
                refs.add(r)
        deps[t] = refs

    ordered, placed = [], set()
    remaining = list(ddls)
    while remaining:
        progress = False
        for t in list(remaining):
            if deps[t] <= placed:
                ordered.append(t); placed.add(t); remaining.remove(t); progress = True
        if not progress:            # cycle — emit the rest as-is
            ordered.extend(remaining)
            print(f"  WARN: FK cycle among {remaining} — emitted in declared order")
            break
    return ordered


def main() -> None:
    con = sqlite3.connect(f"file:{SRC}?mode=ro", uri=True)
    os.makedirs(os.path.dirname(OUT), exist_ok=True)

    lines = [
        "-- KANIDA.AI — OLTP schema for PostgreSQL (Stage 2, generated).",
        "-- Source: data/db/kanida_universe.db (SQLite) -> translated to PG.",
        "-- Domain: per-user TRANSACTIONAL state only. Market/engine tables",
        "-- (ohlc_*, falcon_features, patterns, signals) intentionally stay on SQLite.",
        "-- Re-runnable: every statement uses IF NOT EXISTS.",
        "-- NOTE: no BEGIN/COMMIT here on purpose — backend/pg_migrate.py runs this",
        "-- inside pgdb.pg_conn(), which owns the transaction (so a failure rolls",
        "-- the WHOLE migration back). Nesting BEGIN would commit early.",
        "",
    ]
    all_warns: list[str] = []
    found = 0
    missing = []

    # Collect raw DDL first so we can order by FK dependency before emitting.
    raw = {}
    for t in OLTP_TABLES:
        row = con.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (t,)
        ).fetchone()
        if not row or not row[0]:
            missing.append(t)
            continue
        raw[t] = row[0]

    for t in dependency_order(raw):
        found += 1
        sql, warns = translate(raw[t])
        lines.append(f"-- ── {t} ─────────────────────────────────────────────")
        lines.append(sql)
        for w in warns:
            all_warns.append(f"{t}: {w}")
        idx = con.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? "
            "AND sql IS NOT NULL", (t,)
        ).fetchall()
        for (isql,) in idx:
            isql2, iw = translate(isql)
            lines.append(isql2)
            for w in iw:
                all_warns.append(f"{t} index: {w}")
        lines.append("")

    lines.append("-- (transaction owned by pg_migrate.apply_schema)")
    with open(OUT, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))

    print(f"tables translated : {found}/{len(OLTP_TABLES)}")
    if missing:
        print(f"NOT FOUND in SQLite ({len(missing)}): {', '.join(missing)}")
    print(f"written           : {OUT}")
    print(f"bytes             : {os.path.getsize(OUT):,}")
    if all_warns:
        print(f"\nWARNINGS ({len(all_warns)}):")
        for w in all_warns:
            print("  -", w)
    else:
        print("\nno warnings")
    con.close()


if __name__ == "__main__":
    main()
