#!/usr/bin/env python3
"""build_cloud_bundle.py — produce the cloud-ready DB bundle for the
SQLite-on-a-volume Phase-1 deploy (Path A, see docs/launch/RUNBOOK_deploy.md).

WHAT IT PRODUCES
================
An output directory (default `deploy/cloud-bundle/`) containing the exact
files the host volume needs so EVERY /power request path is self-sufficient
and NOTHING reaches the 14 GB research warehouse at request time:

  1. kanida_universe.db        — a COPY of the app DB with the FULL
                                 `falcon_outcomes` table merged in from the
                                 R&D DB (the app DB's own copy is sparse;
                                 it is DROPped and replaced). This file is
                                 BOTH the app DB (POWER_DB_PATH / FALCON_DB_PATH)
                                 AND the evidence source (POWER_RND_DB_PATH).

  2. kanida_universe_rnd.db    — a SMALL sidecar holding ONLY the two pattern
                                 tables the persona engine reads:
                                   - falcon_promoted_patterns
                                   - falcon_pattern_candidates
                                 copied verbatim (schema + rows + indexes)
                                 from the R&D DB. Personas IGNORE POWER_RND_DB_PATH
                                 and fall back to this filename NEXT TO the app DB.

  3. intraday_mining.db        — copied from the R&D intraday DB ONLY when
                                 --with-intraday is passed (P2 needs a separate
                                 code change to actually use it on the host — see
                                 the host-placement notes printed at the end).

WHY THESE TABLES (verified against source — file:line)
======================================================
  * /power/today Historical Evidence reads `falcon_outcomes` from the RND
    connection, which the router opens at POWER_RND_DB_PATH (= this app DB
    on the host):
        backend/power_user/routers/falcon_top20_router.py:30,94-96
            POWER_RND_DB_PATH → sqlite3.connect(file:...?mode=ro)
        backend/power_user/services/falcon_top20_explainer.py:1103-1108
            SELECT trade_date, ret_20d, hit_10pc_20d, mae_20d, mfe_20d
              FROM falcon_outcomes WHERE symbol=? AND trade_date IN (...)
        backend/power_user/services/falcon_top20_explainer.py:1223-1226
            SELECT AVG(hit_10pc_20d)*100 FROM falcon_outcomes WHERE symbol=?
      → falcon_outcomes (FULL, from R&D) MUST live in the app DB file.

  * /power/personas/* call load_full_patterns(rnd_db) which reads exactly two
    tables joined on pattern_id:
        backend/power_user/services/persona_engine_core.py:69-74
            SELECT c.mined_year, c.rule_json, p.avg_oos_year_lift_pp
              FROM falcon_promoted_patterns p
              JOIN falcon_pattern_candidates c ON p.pattern_id = c.pattern_id
             WHERE p.classification IN ('universal','regime_dependent')
      The rnd path resolver builds its own path and, on the host, falls back
      to `kanida_universe_rnd.db` next to the app DB; POWER_RND_DB_PATH is
      NOT consulted by personas:
        backend/power_user/services/persona_simulator.py:62-71
      → ship those 2 tables as a small sidecar named kanida_universe_rnd.db.

  * patient-trader (P2) intraday filter reads intraday_dataset_v2 from
    intraday_mining.db via a hardcoded relative path with NO env / NO fallback:
        backend/power_user/services/persona_simulator.py:74-79  (_resolve_intraday_db_path)
        backend/power_user/services/persona_simulator.py:98-103 (SELECT ... FROM intraday_dataset_v2)
      The call site is CAUGHT (FileNotFoundError → log.warning), so P2 runs
      DEGRADED (no intraday gate), it does NOT crash:
        backend/power_user/services/persona_simulator.py:428-434
      → intraday_mining.db is OPTIONAL for Phase 1 (--with-intraday) and needs
        a code change before the host actually uses it.

SAFETY / IDEMPOTENCY
====================
  * Source DBs are NEVER mutated in place. The app DB is shutil.copy()'d to the
    output first, then falcon_outcomes is rebuilt inside the COPY. The R&D DB
    is opened read-only (mode=ro) for every read.
  * Schema-preserving: each table is recreated by replaying the source
    `CREATE TABLE` statement from sqlite_master (no hand-written column lists),
    then rows are bulk-inserted with an auto-sized placeholder list. Indexes
    are recreated where present (CREATE INDEX statements replayed too).
  * Idempotent + re-runnable: existing output files are overwritten; the merged
    `falcon_outcomes` is DROPped before recreate; the sidecar is rebuilt fresh.
  * Row counts are verified after each copy and printed in a manifest.

CLI
===
  --app-db PATH        source app DB           (default: data/db/kanida_universe.db)
  --rnd-db PATH        source R&D DB           (default: universe_engine/data/db/kanida_universe.db)
  --intraday-db PATH   source intraday DB      (default: universe_engine/data/db/intraday_mining.db)
  --out-dir PATH       output bundle dir       (default: deploy/cloud-bundle)
  --with-intraday      also copy intraday_mining.db (default: off)
  --dry-run            report what it WOULD do; write nothing
  --verify             open the produced bundle and assert each required table
                       exists + has > 0 rows (run after a real build)

stdlib only: sqlite3, shutil, argparse, pathlib (+ os, sys, time).
NOTE: this environment has no Python — the script is built by careful reading.
--dry-run / --verify are how the operator validates on the host machine.
"""
from __future__ import annotations

import argparse
import shutil
import sqlite3
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

# ════════════════════════════════════════════════════════════════════════
# Verified table requirements (single source of truth in this script).
# Keep these in lockstep with the file:line citations in the module docstring
# and docs/launch/builds/P1BUNDLE-build-log.md. A wrong list crashes the app.
# ════════════════════════════════════════════════════════════════════════

# Merged FULL into the app-DB copy (evidence read, POWER_RND_DB_PATH = app DB).
OUTCOMES_TABLE = "falcon_outcomes"

# The ONLY tables personas read (load_full_patterns JOIN) → small sidecar.
PERSONA_PATTERN_TABLES = [
    "falcon_promoted_patterns",
    "falcon_pattern_candidates",
]

# P2 intraday filter source (optional, --with-intraday).
INTRADAY_TABLE = "intraday_dataset_v2"

# Output filenames (must match the host resolver expectations).
APP_DB_NAME = "kanida_universe.db"
RND_SIDECAR_NAME = "kanida_universe_rnd.db"
INTRADAY_DB_NAME = "intraday_mining.db"

# Repo root = two levels up from scripts/build_cloud_bundle.py
REPO_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_APP_DB = REPO_ROOT / "data" / "db" / "kanida_universe.db"
DEFAULT_RND_DB = REPO_ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
DEFAULT_INTRADAY_DB = REPO_ROOT / "universe_engine" / "data" / "db" / "intraday_mining.db"
DEFAULT_OUT_DIR = REPO_ROOT / "deploy" / "cloud-bundle"


# ════════════════════════════════════════════════════════════════════════
# Small console helpers
# ════════════════════════════════════════════════════════════════════════

def _log(msg: str = "") -> None:
    print(msg, flush=True)


def _section(title: str) -> None:
    _log()
    _log("=" * 72)
    _log(title)
    _log("=" * 72)


def _human_size(n: int) -> str:
    f = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if f < 1024.0 or unit == "TB":
            return f"{f:,.1f} {unit}" if unit != "B" else f"{int(f):,} B"
        f /= 1024.0
    return f"{n} B"


# ════════════════════════════════════════════════════════════════════════
# sqlite schema-introspection + verbatim copy helpers
# ════════════════════════════════════════════════════════════════════════

def _connect_ro(path: Path) -> sqlite3.Connection:
    """Open a DB strictly read-only (never mutate a source)."""
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=120.0)


def _table_exists(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def _table_create_sql(con: sqlite3.Connection, table: str) -> Optional[str]:
    row = con.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row[0] if row and row[0] else None


def _table_index_sqls(con: sqlite3.Connection, table: str) -> List[str]:
    """CREATE INDEX statements for a table (auto-indexes have sql=NULL → skipped)."""
    rows = con.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? "
        "AND sql IS NOT NULL",
        (table,),
    ).fetchall()
    return [r[0] for r in rows if r and r[0]]


def _row_count(con: sqlite3.Connection, table: str) -> int:
    return int(con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])


def _column_count(con: sqlite3.Connection, table: str) -> int:
    return len(con.execute(f'PRAGMA table_info("{table}")').fetchall())


def _copy_table_verbatim(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    table: str,
    batch: int = 5000,
) -> Tuple[int, int]:
    """Recreate `table` in dst by replaying the source CREATE statement, then
    bulk-INSERT every row. Recreates non-auto indexes too. Drops any existing
    dst copy first (idempotent). Returns (rows_copied, indexes_created).

    No hand-written column lists — the placeholder count is derived from the
    source schema via PRAGMA table_info, so the copy can never drift from the
    real column set.
    """
    create_sql = _table_create_sql(src, table)
    if not create_sql:
        raise RuntimeError(f"source table '{table}' has no CREATE statement")

    dst.execute(f'DROP TABLE IF EXISTS "{table}"')
    dst.execute(create_sql)

    ncols = _column_count(src, table)
    placeholders = ",".join("?" * ncols)
    insert_sql = f'INSERT INTO "{table}" VALUES ({placeholders})'

    cur = src.execute(f'SELECT * FROM "{table}"')
    copied = 0
    while True:
        rows = cur.fetchmany(batch)
        if not rows:
            break
        dst.executemany(insert_sql, rows)
        copied += len(rows)

    idx_created = 0
    for idx_sql in _table_index_sqls(src, table):
        try:
            dst.execute(idx_sql)
            idx_created += 1
        except sqlite3.OperationalError as e:
            # An index may already exist if it was embedded in the CREATE TABLE
            # (rare) — non-fatal, the table data is intact.
            _log(f"    [warn] index replay skipped for {table}: {e}")
    dst.commit()
    return copied, idx_created


# ════════════════════════════════════════════════════════════════════════
# Build steps
# ════════════════════════════════════════════════════════════════════════

def _resolve_args(args: argparse.Namespace) -> dict:
    return {
        "app_db": Path(args.app_db).resolve(),
        "rnd_db": Path(args.rnd_db).resolve(),
        "intraday_db": Path(args.intraday_db).resolve(),
        "out_dir": Path(args.out_dir).resolve(),
        "with_intraday": bool(args.with_intraday),
    }


def _preflight(paths: dict) -> None:
    """Validate the source files exist before doing anything."""
    missing = []
    if not paths["app_db"].exists():
        missing.append(f"app DB:  {paths['app_db']}")
    if not paths["rnd_db"].exists():
        missing.append(f"R&D DB:  {paths['rnd_db']}")
    if paths["with_intraday"] and not paths["intraday_db"].exists():
        missing.append(f"intraday DB (--with-intraday): {paths['intraday_db']}")
    if missing:
        raise FileNotFoundError(
            "missing source DB(s):\n  - " + "\n  - ".join(missing)
        )


def build_app_db(paths: dict, manifest: list) -> Path:
    """Copy the app DB to the output, then DROP + rebuild falcon_outcomes from
    the FULL R&D copy. The app DB is the evidence source on the host
    (POWER_RND_DB_PATH = this file)."""
    out_app = paths["out_dir"] / APP_DB_NAME
    _section(f"1/3  App DB  →  {out_app.name}")

    _log(f"  copy   {paths['app_db']}")
    _log(f"    →    {out_app}")
    shutil.copy2(paths["app_db"], out_app)
    _log(f"  copied ({_human_size(out_app.stat().st_size)})")

    # Merge the FULL falcon_outcomes from R&D, replacing any sparse local copy.
    rnd = _connect_ro(paths["rnd_db"])
    dst = sqlite3.connect(str(out_app), timeout=120.0)
    try:
        if not _table_exists(rnd, OUTCOMES_TABLE):
            raise RuntimeError(
                f"R&D DB has no '{OUTCOMES_TABLE}' table — cannot build evidence."
            )
        src_rows = _row_count(rnd, OUTCOMES_TABLE)
        had_local = _table_exists(dst, OUTCOMES_TABLE)
        local_rows = _row_count(dst, OUTCOMES_TABLE) if had_local else 0
        _log(f"  {OUTCOMES_TABLE}: app-DB copy had {local_rows:,} rows "
             f"(sparse) → replacing with R&D {src_rows:,} rows")

        copied, idx = _copy_table_verbatim(rnd, dst, OUTCOMES_TABLE)
        final_rows = _row_count(dst, OUTCOMES_TABLE)
        if final_rows != src_rows:
            raise RuntimeError(
                f"{OUTCOMES_TABLE} row-count mismatch after merge: "
                f"copied {copied}, dst now {final_rows}, src {src_rows}"
            )
        _log(f"  merged {OUTCOMES_TABLE}: {final_rows:,} rows, {idx} index(es)  OK")
        manifest.append((out_app.name, OUTCOMES_TABLE, final_rows))
    finally:
        rnd.close()
        dst.close()
    return out_app


def build_rnd_sidecar(paths: dict, manifest: list) -> Path:
    """Build the small kanida_universe_rnd.db sidecar with ONLY the two persona
    pattern tables, copied verbatim from R&D."""
    out_rnd = paths["out_dir"] / RND_SIDECAR_NAME
    _section(f"2/3  Persona sidecar  →  {out_rnd.name}")

    if out_rnd.exists():
        out_rnd.unlink()  # rebuild fresh (idempotent)

    rnd = _connect_ro(paths["rnd_db"])
    dst = sqlite3.connect(str(out_rnd), timeout=120.0)
    try:
        for table in PERSONA_PATTERN_TABLES:
            if not _table_exists(rnd, table):
                raise RuntimeError(
                    f"R&D DB has no '{table}' — personas would fail on the host."
                )
            src_rows = _row_count(rnd, table)
            copied, idx = _copy_table_verbatim(rnd, dst, table)
            final_rows = _row_count(dst, table)
            if final_rows != src_rows:
                raise RuntimeError(
                    f"{table} row-count mismatch: copied {copied}, "
                    f"dst {final_rows}, src {src_rows}"
                )
            _log(f"  {table}: {final_rows:,} rows, {idx} index(es)  OK")
            manifest.append((out_rnd.name, table, final_rows))
    finally:
        rnd.close()
        dst.close()
    # Reclaim free pages so the sidecar is genuinely small.
    vac = sqlite3.connect(str(out_rnd), timeout=120.0)
    try:
        vac.execute("VACUUM")
    finally:
        vac.close()
    _log(f"  sidecar built ({_human_size(out_rnd.stat().st_size)})")
    return out_rnd


def build_intraday(paths: dict, manifest: list) -> Optional[Path]:
    """Copy intraday_mining.db verbatim (whole file) ONLY when --with-intraday."""
    out_intra = paths["out_dir"] / INTRADAY_DB_NAME
    _section(f"3/3  Intraday DB  →  {out_intra.name}")
    if not paths["with_intraday"]:
        _log("  skipped (--with-intraday not passed).")
        _log("  P2 (patient-trader) will run DEGRADED on the host — no intraday")
        _log("  gate — but will NOT crash (call site is caught).")
        return None

    _log(f"  copy   {paths['intraday_db']}")
    _log(f"    →    {out_intra}")
    shutil.copy2(paths["intraday_db"], out_intra)
    _log(f"  copied ({_human_size(out_intra.stat().st_size)})")

    # Report the intraday table row count for the manifest (read-only check).
    con = _connect_ro(out_intra)
    try:
        if _table_exists(con, INTRADAY_TABLE):
            n = _row_count(con, INTRADAY_TABLE)
            _log(f"  {INTRADAY_TABLE}: {n:,} rows  OK")
            manifest.append((out_intra.name, INTRADAY_TABLE, n))
        else:
            _log(f"  [warn] {INTRADAY_TABLE} not found in intraday DB.")
    finally:
        con.close()
    return out_intra


# ════════════════════════════════════════════════════════════════════════
# Manifest + host-placement instructions
# ════════════════════════════════════════════════════════════════════════

def _print_manifest(paths: dict, manifest: list) -> None:
    _section("MANIFEST")
    out_dir = paths["out_dir"]
    for fname in (APP_DB_NAME, RND_SIDECAR_NAME, INTRADAY_DB_NAME):
        fp = out_dir / fname
        if not fp.exists():
            continue
        _log(f"  {fname}   ({_human_size(fp.stat().st_size)})")
        for mf, table, rows in manifest:
            if mf == fname:
                _log(f"      - {table:<28} {rows:>12,} rows")


def _print_host_placement(paths: dict) -> None:
    _section("HOST PLACEMENT (read carefully — wrong placement breaks the app)")
    _log("""
Let POWER_DB_PATH be the volume path to the app DB, e.g.  /data/kanida_universe.db
(the directory holding it is the "volume dir", e.g.  /data ).

  1. kanida_universe.db        →  POWER_DB_PATH               (the app DB itself)
                                     e.g.  /data/kanida_universe.db
       Also set, to the SAME path:
         FALCON_DB_PATH        =  <POWER_DB_PATH>
         POWER_RND_DB_PATH     =  <POWER_DB_PATH>   ← /power/today evidence reads
                                  falcon_outcomes from THIS file. Personas IGNORE
                                  this var (see #2).

  2. kanida_universe_rnd.db    →  NEXT TO the app DB, in the SAME directory
                                     e.g.  /data/kanida_universe_rnd.db
       Personas (/power/personas/*) resolve their RND path themselves and fall
       back to <volume dir>/kanida_universe_rnd.db. POWER_RND_DB_PATH is NOT
       consulted here. If this sidecar is missing, EVERY persona endpoint fails.

  3. intraday_mining.db        →  OPTIONAL (only present if --with-intraday).
       The host code path _resolve_intraday_db_path() uses a HARDCODED relative
       path with no env + no fallback, and its caller is wrapped in try/except.
       So even if you place this file on the volume, the host will NOT read it
       until a code change adds POWER_INTRADAY_DB_PATH (or a next-to-app-DB
       fallback) to _resolve_intraday_db_path(). Until then P2 runs DEGRADED
       (no intraday gate) but stable. Decide consciously (RUNBOOK §9).

Env recap (Phase 1 / SQLite — do NOT set DATABASE_URL):
  POWER_DB_PATH=/data/kanida_universe.db
  FALCON_DB_PATH=/data/kanida_universe.db
  POWER_RND_DB_PATH=/data/kanida_universe.db
  # sidecar (no env — must physically sit at): /data/kanida_universe_rnd.db
""")


# ════════════════════════════════════════════════════════════════════════
# --dry-run
# ════════════════════════════════════════════════════════════════════════

def do_dry_run(paths: dict) -> int:
    _section("DRY RUN — no files will be written")
    _log(f"  app DB (source)       : {paths['app_db']}")
    _log(f"  R&D DB (source, RO)   : {paths['rnd_db']}")
    _log(f"  intraday DB (source)  : {paths['intraday_db']}"
         f"{'' if paths['with_intraday'] else '  [not used — --with-intraday off]'}")
    _log(f"  output dir            : {paths['out_dir']}")
    _log()

    _preflight(paths)

    # Inspect sources read-only and report what WOULD be produced.
    rnd = _connect_ro(paths["rnd_db"])
    try:
        _log("  WOULD produce:")
        # 1. app DB copy + merged outcomes
        out_rows = _row_count(rnd, OUTCOMES_TABLE) if _table_exists(rnd, OUTCOMES_TABLE) else 0
        _log(f"    {APP_DB_NAME}")
        _log(f"      copy of app DB ({_human_size(paths['app_db'].stat().st_size)}), then")
        _log(f"      DROP + replace {OUTCOMES_TABLE} with R&D's {out_rows:,} rows")
        # 2. sidecar
        _log(f"    {RND_SIDECAR_NAME}  (small sidecar)")
        for t in PERSONA_PATTERN_TABLES:
            n = _row_count(rnd, t) if _table_exists(rnd, t) else 0
            present = "" if _table_exists(rnd, t) else "  [MISSING IN R&D!]"
            _log(f"      {t}: {n:,} rows{present}")
        # 3. intraday
        if paths["with_intraday"]:
            sz = _human_size(paths["intraday_db"].stat().st_size)
            _log(f"    {INTRADAY_DB_NAME}  (verbatim copy, {sz})")
        else:
            _log(f"    {INTRADAY_DB_NAME}  — SKIPPED (--with-intraday not passed)")
    finally:
        rnd.close()
    _log()
    _log("  Dry run complete. Re-run without --dry-run to write the bundle.")
    return 0


# ════════════════════════════════════════════════════════════════════════
# --verify
# ════════════════════════════════════════════════════════════════════════

def do_verify(paths: dict) -> int:
    _section("VERIFY — opening the produced bundle and asserting required tables")
    out_dir = paths["out_dir"]
    ok = True

    checks = [
        (APP_DB_NAME, [OUTCOMES_TABLE], True),
        (RND_SIDECAR_NAME, list(PERSONA_PATTERN_TABLES), True),
    ]
    if paths["with_intraday"]:
        checks.append((INTRADAY_DB_NAME, [INTRADAY_TABLE], True))

    for fname, tables, required in checks:
        fp = out_dir / fname
        if not fp.exists():
            _log(f"  [FAIL] {fname} — file missing")
            ok = False
            continue
        con = _connect_ro(fp)
        try:
            for t in tables:
                if not _table_exists(con, t):
                    _log(f"  [FAIL] {fname}: table '{t}' missing")
                    ok = False
                    continue
                n = _row_count(con, t)
                if n <= 0:
                    _log(f"  [FAIL] {fname}: table '{t}' has 0 rows")
                    ok = False
                else:
                    _log(f"  [ OK ] {fname}: {t} — {n:,} rows")
        finally:
            con.close()

    _log()
    if ok:
        _log("  VERIFY PASSED — bundle is complete.")
        return 0
    _log("  VERIFY FAILED — see [FAIL] lines above.")
    return 1


# ════════════════════════════════════════════════════════════════════════
# main
# ════════════════════════════════════════════════════════════════════════

def build(paths: dict) -> int:
    _preflight(paths)
    paths["out_dir"].mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    manifest: list = []

    build_app_db(paths, manifest)
    build_rnd_sidecar(paths, manifest)
    build_intraday(paths, manifest)

    _print_manifest(paths, manifest)
    _print_host_placement(paths)
    _log()
    _log(f"Bundle built in {time.time() - t0:.1f}s  →  {paths['out_dir']}")
    _log("Next: run with --verify to assert every required table has rows.")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        prog="build_cloud_bundle.py",
        description="Produce the cloud-ready SQLite bundle for the Phase-1 "
                    "SQLite-on-a-volume deploy.",
    )
    p.add_argument("--app-db", default=str(DEFAULT_APP_DB),
                   help=f"source app DB (default: {DEFAULT_APP_DB})")
    p.add_argument("--rnd-db", default=str(DEFAULT_RND_DB),
                   help=f"source R&D DB, read-only (default: {DEFAULT_RND_DB})")
    p.add_argument("--intraday-db", default=str(DEFAULT_INTRADAY_DB),
                   help=f"source intraday DB (default: {DEFAULT_INTRADAY_DB})")
    p.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR),
                   help=f"output bundle directory (default: {DEFAULT_OUT_DIR})")
    p.add_argument("--with-intraday", action="store_true",
                   help="also copy intraday_mining.db (P2 needs a code change to use it)")
    p.add_argument("--dry-run", action="store_true",
                   help="report what it WOULD do; write nothing")
    p.add_argument("--verify", action="store_true",
                   help="open the produced bundle and assert required tables exist + >0 rows")
    args = p.parse_args(argv)

    if args.dry_run and args.verify:
        _log("error: pass only one of --dry-run / --verify (or neither to build).")
        return 2

    paths = _resolve_args(args)
    try:
        if args.dry_run:
            return do_dry_run(paths)
        if args.verify:
            return do_verify(paths)
        return build(paths)
    except Exception as e:  # noqa: BLE001 — top-level guard, print + nonzero exit
        _log()
        _log(f"ERROR: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
