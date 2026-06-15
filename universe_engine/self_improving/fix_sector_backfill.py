#!/usr/bin/env python3
"""fix_sector_backfill.py — RND-only sector backfill for the 3 symbols PROD's
falcon_sectors is missing (GUJGASLTD / LTIM / ZOMATO).

WHY:
  The Self-Improving Engine reports (Step 3 classify_patterns + S2B signal-day
  study) read sector membership from `falcon_sectors`. PROD's falcon_sectors does
  NOT contain GUJGASLTD, LTIM or ZOMATO, so those trades got a NULL sector and
  therefore a NULL move_type / sector attribution. The fix (review) is to (a) read
  falcon_sectors from the RND DB in those scripts, and (b) backfill the 3 missing
  mappings into RND's falcon_sectors — this script does (b), plus repairs the
  already-written NULL sector columns on the two output tables so a re-run isn't
  required to see the effect.

WHAT IT DOES (RND DB only, single transaction, idempotent):
  1. INSERT the 3 mappings into RND `falcon_sectors` (INSERT OR IGNORE — existing
     rows are left untouched; column names introspected via PRAGMA).
  2. UPDATE `falcon_baseline_trades`  SET sector=<map> WHERE symbol IN (3) AND
     sector IS NULL AND persona='falcon_top10'.
  3. UPDATE `falcon_signal_day_study` SET sector=<map> WHERE symbol IN (3) AND
     sector IS NULL AND persona='falcon_top10_daily'.
  Prints counts changed for each. --dry-run computes + prints but writes nothing.

SAFETY:
  RND-only (the required --rnd-db is the sole DB opened). Single transaction with
  rollback on any error. Idempotent: re-running inserts nothing new (OR IGNORE)
  and updates 0 rows (sector already non-NULL). stdlib only.

CLI:
  python fix_sector_backfill.py --rnd-db <path> [--dry-run]
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from typing import Dict, List, Tuple

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# The 3 missing symbol -> sector mappings (NSE sector names, matching the style
# already used in falcon_sectors).
SECTOR_MAP: Dict[str, str] = {
    "GUJGASLTD": "Oil Gas & Consumable Fuels",
    "LTIM": "Information Technology",
    "ZOMATO": "Consumer Services",
}

BASELINE_TABLE = "falcon_baseline_trades"
BASELINE_PERSONA = "falcon_top10"
STUDY_TABLE = "falcon_signal_day_study"
STUDY_PERSONA = "falcon_top10_daily"


def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    row = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def _falcon_sectors_columns(con: sqlite3.Connection) -> Tuple[str, str]:
    """Introspect falcon_sectors and return its (symbol_col, sector_col) names.
    The canonical schema is (symbol, sector); we detect them defensively so a
    differently-named-but-equivalent table still works."""
    cols = [r[1] for r in con.execute("PRAGMA table_info(falcon_sectors)").fetchall()]
    if not cols:
        raise RuntimeError("falcon_sectors table not found / has no columns in RND DB.")
    lower = {c.lower(): c for c in cols}
    sym_col = lower.get("symbol")
    sec_col = lower.get("sector")
    if sym_col is None or sec_col is None:
        raise RuntimeError(
            f"falcon_sectors columns not recognised (expected symbol+sector): {cols}"
        )
    return sym_col, sec_col


def run(rnd_db: str, dry_run: bool) -> int:
    print(f"[fix_sector_backfill] RND DB: {rnd_db}")
    print(f"[fix_sector_backfill] mode: {'DRY-RUN (no writes)' if dry_run else 'APPLY'}")
    print(f"[fix_sector_backfill] mappings: {SECTOR_MAP}")

    symbols: List[str] = list(SECTOR_MAP.keys())
    placeholders = ", ".join("?" for _ in symbols)

    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        sym_col, sec_col = _falcon_sectors_columns(con)
        print(f"[fix_sector_backfill] falcon_sectors columns: "
              f"symbol='{sym_col}', sector='{sec_col}'")

        con.execute("BEGIN")

        # ── 1. INSERT OR IGNORE the 3 mappings into falcon_sectors ──
        n_inserted = 0
        for sym, sec in SECTOR_MAP.items():
            cur = con.execute(
                f"INSERT OR IGNORE INTO falcon_sectors ({sym_col}, {sec_col}) "
                f"VALUES (?, ?)",
                (sym, sec),
            )
            if cur.rowcount and cur.rowcount > 0:
                n_inserted += 1
        print(f"[fix_sector_backfill] falcon_sectors: {n_inserted} new mapping(s) "
              f"inserted ({len(SECTOR_MAP) - n_inserted} already present).")

        # ── 2. UPDATE falcon_baseline_trades NULL sectors ──
        n_base = 0
        if _table_exists(con, BASELINE_TABLE):
            for sym, sec in SECTOR_MAP.items():
                cur = con.execute(
                    f"UPDATE {BASELINE_TABLE} SET sector = ? "
                    f"WHERE symbol = ? AND sector IS NULL AND persona = ?",
                    (sec, sym, BASELINE_PERSONA),
                )
                n_base += cur.rowcount or 0
            print(f"[fix_sector_backfill] {BASELINE_TABLE} "
                  f"(persona='{BASELINE_PERSONA}'): {n_base} NULL-sector row(s) set.")
        else:
            print(f"[fix_sector_backfill] {BASELINE_TABLE} absent — skipped.")

        # ── 3. UPDATE falcon_signal_day_study NULL sectors ──
        n_study = 0
        if _table_exists(con, STUDY_TABLE):
            for sym, sec in SECTOR_MAP.items():
                cur = con.execute(
                    f"UPDATE {STUDY_TABLE} SET sector = ? "
                    f"WHERE symbol = ? AND sector IS NULL AND persona = ?",
                    (sec, sym, STUDY_PERSONA),
                )
                n_study += cur.rowcount or 0
            print(f"[fix_sector_backfill] {STUDY_TABLE} "
                  f"(persona='{STUDY_PERSONA}'): {n_study} NULL-sector row(s) set.")
        else:
            print(f"[fix_sector_backfill] {STUDY_TABLE} absent — skipped.")

        if dry_run:
            con.rollback()
            print("[fix_sector_backfill] DRY-RUN — transaction rolled back, "
                  "nothing written.")
        else:
            con.commit()
            print("[fix_sector_backfill] APPLY complete — committed (RND only).")
    except Exception:
        con.rollback()
        print("[fix_sector_backfill] ERROR — rolled back, nothing written.",
              file=sys.stderr)
        raise
    finally:
        con.close()
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Backfill the 3 missing symbol->sector mappings "
                    "(GUJGASLTD/LTIM/ZOMATO) into the RND DB and repair the NULL "
                    "sector columns they left on the two output tables. RND-only, "
                    "idempotent, single transaction."
    )
    p.add_argument("--rnd-db", required=True,
                   help="REQUIRED. RND research DB. The ONLY DB opened/written.")
    p.add_argument("--dry-run", action="store_true",
                   help="Compute + print counts; write NOTHING (rolls back).")
    args = p.parse_args(argv)
    return run(rnd_db=args.rnd_db, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
