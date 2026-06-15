#!/usr/bin/env python3
"""apply_schema.py — idempotent applier for the self-improving-engine schema.

Step 1 of the Self-Improving Multi-Persona Trading Intelligence System.

Applies (to a caller-supplied SQLite DB — typically the RND research DB):
  1. schema_self_improving.sql  — CREATE TABLE IF NOT EXISTS for all 10 new
     tables (safe to re-run).
  2. taxonomy_columns.sql       — ADD COLUMN for each new falcon_pattern_taxonomy
     column, applied GUARDED (SQLite has no "ADD COLUMN IF NOT EXISTS"): existing
     columns are read via PRAGMA table_info and only missing columns are added,
     each in its own try/except. Mirrors backend/power_user/db_init.py.

ADDITIVE ONLY. Never drops/renames/retypes a column. Never writes data. Never
touches existing falcon_pattern_taxonomy rows.

Stdlib only: sqlite3, argparse, pathlib, re, sys.

Usage:
    python apply_schema.py --db /path/to/rnd.db            # apply
    python apply_schema.py --db /path/to/rnd.db --dry-run  # report only, no writes
    python apply_schema.py --db /path/to/rnd.db --verify   # assert tables + report

Safety: --db is REQUIRED and has no default — the caller passes the RND path so
this never accidentally targets a live/prod DB.
"""
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from pathlib import Path
from typing import List, Tuple

_HERE = Path(__file__).resolve().parent
SCHEMA_SQL_PATH   = _HERE / "schema_self_improving.sql"
TAXONOMY_SQL_PATH = _HERE / "taxonomy_columns.sql"

TAXONOMY_TABLE = "falcon_pattern_taxonomy"

# The 10 tables this build creates — used by --verify and the summary.
EXPECTED_TABLES = [
    "falcon_baseline_trades",
    "falcon_pattern_contributions",
    "falcon_post_exit_tracking",
    "falcon_near_miss_tracking",
    "falcon_signal_validity",
    "falcon_big_winner_loser_study",
    "falcon_pattern_weekly_state",
    "falcon_weekly_review_log",
    "index_expiry_calendar",
    "fo_stock_master",
]


# ── parsing helpers ──────────────────────────────────────────────────────────

def _strip_line_comments(sql: str) -> str:
    """Remove `-- ...` trailing/standalone comments line-by-line.

    Kept deliberately simple: the schema files use only `--` comments (no `/* */`
    and no `--` inside string literals), so line-wise stripping is safe here.
    """
    out_lines = []
    for line in sql.splitlines():
        idx = line.find("--")
        out_lines.append(line if idx < 0 else line[:idx])
    return "\n".join(out_lines)


def parse_taxonomy_columns(sql: str) -> List[Tuple[str, str]]:
    """Parse `ALTER TABLE falcon_pattern_taxonomy ADD COLUMN <name> <type...>;`.

    Returns a list of (column_name, full_alter_statement) preserving file order.
    Comments are stripped first so inline `-- VARCHAR(20)` notes don't leak into
    the executed statement.
    """
    clean = _strip_line_comments(sql)
    cols: List[Tuple[str, str]] = []
    # Split on ';' — each ALTER is one statement.
    for raw in clean.split(";"):
        stmt = raw.strip()
        if not stmt:
            continue
        m = re.match(
            r"ALTER\s+TABLE\s+falcon_pattern_taxonomy\s+ADD\s+COLUMN\s+([A-Za-z_]\w*)\b",
            stmt,
            flags=re.IGNORECASE,
        )
        if not m:
            # Defensive: ignore any non-matching fragment (should not happen).
            continue
        col = m.group(1)
        # Re-append the ';' for a clean executable statement.
        cols.append((col, stmt + ";"))
    return cols


def existing_columns(con: sqlite3.Connection, table: str) -> List[str]:
    """Return current column names of `table` (empty list if table absent)."""
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]  # r[1] == column name


def table_exists(con: sqlite3.Connection, table: str) -> bool:
    r = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return r is not None


# ── core actions ─────────────────────────────────────────────────────────────

def _load_sql() -> Tuple[str, List[Tuple[str, str]]]:
    if not SCHEMA_SQL_PATH.exists():
        raise FileNotFoundError(f"Schema SQL not found: {SCHEMA_SQL_PATH}")
    if not TAXONOMY_SQL_PATH.exists():
        raise FileNotFoundError(f"Taxonomy SQL not found: {TAXONOMY_SQL_PATH}")
    schema_sql = SCHEMA_SQL_PATH.read_text(encoding="utf-8")
    taxonomy_cols = parse_taxonomy_columns(
        TAXONOMY_SQL_PATH.read_text(encoding="utf-8")
    )
    return schema_sql, taxonomy_cols


def dry_run(db_path: str) -> int:
    """Report what WOULD happen. Opens the DB read-only-ish (no writes)."""
    schema_sql, taxonomy_cols = _load_sql()
    print(f"[dry-run] DB target: {db_path}")
    print(f"[dry-run] would execute schema with {len(EXPECTED_TABLES)} "
          f"CREATE TABLE IF NOT EXISTS statements:")

    # Connect (read-only would block PRAGMA on a missing file; use a normal
    # connection but issue NO writes / NO commit in dry-run).
    con = sqlite3.connect(db_path, timeout=30.0)
    try:
        tax_present = table_exists(con, TAXONOMY_TABLE)
        existing = set(existing_columns(con, TAXONOMY_TABLE)) if tax_present else set()

        for t in EXPECTED_TABLES:
            state = "already present" if table_exists(con, t) else "WOULD CREATE"
            print(f"    - {t:32s} [{state}]")

        print(f"\n[dry-run] {TAXONOMY_TABLE}: "
              f"{'present' if tax_present else 'NOT PRESENT (ALTERs would be SKIPPED)'}")
        would_add, already = [], []
        for col, _stmt in taxonomy_cols:
            (already if (tax_present and col in existing) else would_add).append(col)
        print(f"[dry-run] taxonomy new columns total: {len(taxonomy_cols)}")
        print(f"[dry-run]   would ADD ({len(would_add)}): {would_add or '—'}")
        print(f"[dry-run]   already present ({len(already)}): {already or '—'}")
        if not tax_present:
            print(f"[dry-run]   NOTE: {TAXONOMY_TABLE} does not exist in this DB; "
                  f"ALTERs are skipped (this build never CREATEs the taxonomy "
                  f"table — it only extends an existing one).")
    finally:
        con.close()
    print("\n[dry-run] no changes written.")
    return 0


def apply(db_path: str) -> int:
    """Apply schema + guarded taxonomy ALTERs idempotently."""
    schema_sql, taxonomy_cols = _load_sql()
    print(f"[apply] DB target: {db_path}")

    con = sqlite3.connect(db_path, timeout=30.0)
    try:
        # Snapshot table presence BEFORE creating, for an accurate summary.
        before = {t: table_exists(con, t) for t in EXPECTED_TABLES}

        # 1. CREATE TABLE IF NOT EXISTS block (safe to re-run).
        con.executescript(schema_sql)

        created, present = [], []
        for t in EXPECTED_TABLES:
            (present if before[t] else created).append(t)

        # 2. Guarded taxonomy ALTERs.
        tax_present = table_exists(con, TAXONOMY_TABLE)
        added, already, skipped = [], [], []
        if not tax_present:
            print(f"[apply] WARNING: {TAXONOMY_TABLE} not present — skipping all "
                  f"{len(taxonomy_cols)} taxonomy ALTERs (additive build does NOT "
                  f"create the taxonomy table).")
            skipped = [c for c, _ in taxonomy_cols]
        else:
            existing = set(existing_columns(con, TAXONOMY_TABLE))
            for col, stmt in taxonomy_cols:
                if col in existing:
                    already.append(col)
                    continue
                try:
                    con.execute(stmt)
                    added.append(col)
                except sqlite3.OperationalError as e:
                    # Mirror db_init.py: tolerate duplicate-column races only.
                    if "duplicate column" in str(e).lower():
                        already.append(col)
                    else:
                        raise

        con.commit()

        # ── summary ──
        print(f"\n[apply] tables created   ({len(created)}): {created or '—'}")
        print(f"[apply] tables present   ({len(present)}): {present or '—'}")
        print(f"[apply] taxonomy cols added         ({len(added)}): {added or '—'}")
        print(f"[apply] taxonomy cols already-present ({len(already)}): "
              f"{already or '—'}")
        if skipped:
            print(f"[apply] taxonomy cols SKIPPED (table absent) "
                  f"({len(skipped)}): {skipped}")
        print(f"[apply] done. idempotent — re-running is a no-op.")
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
    return 0


def verify(db_path: str) -> int:
    """Open DB, assert each new table exists, report taxonomy column count."""
    _schema_sql, taxonomy_cols = _load_sql()
    print(f"[verify] DB target: {db_path}")
    con = sqlite3.connect(db_path, timeout=30.0)
    failures: List[str] = []
    try:
        for t in EXPECTED_TABLES:
            ok = table_exists(con, t)
            print(f"    - {t:32s} {'OK' if ok else 'MISSING'}")
            if not ok:
                failures.append(t)

        tax_present = table_exists(con, TAXONOMY_TABLE)
        if tax_present:
            existing = set(existing_columns(con, TAXONOMY_TABLE))
            present = [c for c, _ in taxonomy_cols if c in existing]
            missing = [c for c, _ in taxonomy_cols if c not in existing]
            print(f"\n[verify] {TAXONOMY_TABLE}: present")
            print(f"[verify] taxonomy new columns present: "
                  f"{len(present)}/{len(taxonomy_cols)}")
            if missing:
                print(f"[verify] taxonomy columns MISSING ({len(missing)}): {missing}")
                failures.append(f"{TAXONOMY_TABLE}:{len(missing)}_missing_cols")
        else:
            print(f"\n[verify] {TAXONOMY_TABLE}: NOT PRESENT "
                  f"(expected if the taxonomy table hasn't been created in this DB)")
    finally:
        con.close()

    if failures:
        print(f"\n[verify] FAIL — {len(failures)} problem(s): {failures}")
        return 1
    print(f"\n[verify] PASS — all {len(EXPECTED_TABLES)} new tables exist; "
          f"{len(taxonomy_cols)} taxonomy columns confirmed.")
    return 0


# ── CLI ──────────────────────────────────────────────────────────────────────

def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Idempotent applier for the self-improving-engine SQLite schema."
    )
    p.add_argument(
        "--db", required=True,
        help="Absolute path to the target SQLite DB (RND research DB). REQUIRED.",
    )
    g = p.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true",
                   help="Report what WOULD change; write nothing.")
    g.add_argument("--verify", action="store_true",
                   help="Assert each new table exists + report taxonomy col count.")
    args = p.parse_args(argv)

    if args.dry_run:
        return dry_run(args.db)
    if args.verify:
        return verify(args.db)
    return apply(args.db)


if __name__ == "__main__":
    sys.exit(main())
