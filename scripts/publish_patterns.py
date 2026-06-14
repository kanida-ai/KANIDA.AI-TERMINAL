"""B4 — Publish R&D-mined patterns into the production runtime DB.

NOTE (P1PUB, 2026-06-14): this script does a LOCAL file-to-file sqlite copy
(R&D DB → local PROD DB on the same machine). For CLOUD production — where the
PROD DB lives on a remote volume the laptop cannot `sqlite3.connect` — use
`scripts/publish_to_cloud.py` instead, which ships the same selection over an
authenticated HTTP endpoint (POST /api/falcon/publish/intelligence). This
script stays for the local R&D→PROD case (dev / single box). The two MUST keep
the same cutoff/selection logic (mining_window_years); if you change it here,
mirror it in publish_to_cloud.py.

Closes the silent dependency where signal_runner.py reads from
`falcon_promoted_patterns` but only the R&D DB actually has rows. Without
this script, the production DB is missing patterns and signals work only
"by accident" via implicit R&D access. After this runs, production is
self-contained: deploy-able to a server without R&D shipped alongside.

Tables synced (full-replace, transactional):
  - falcon_promoted_patterns      (the validated edges)
  - falcon_pattern_candidates     (the linked rule_json bodies)

**Mining-window filter** (added 2026-05-10 per R&D sweep validation gate):
  Reads `mining_window_years` from `falcon_engine_config` (operator-tunable
  at /falcon/config; default 4). Only patterns with
  `mined_year >= (current_calendar_year_IST - mining_window_years)` are
  copied to PROD. R&D 2024 sweep showed:
    * 4-yr (default) wins return + DD in calm trending regimes
    * 5-yr trades ~3% return for ~7pp better DD in stress months
  Operator flips the knob from /falcon/config based on regime view.

Run cadence:
  - Manually after the weekly re-mine completes (Sun ~19:30 IST)
  - On-demand via `python scripts/publish_patterns.py`
  - Future: cron-after-validate so it auto-runs

Audit:
  - Writes one row to falcon_signal_runs with job_name='patterns_publish'
  - Status: 'success' or 'failed'
  - Notes include before/after counts AND the cutoff_year used
  - Time recorded in IST

Failure-safe:
  - Wrapped in a single transaction. Either both tables flip together,
    or neither moves. No half-state.
  - Aborts cleanly if R&D returns 0 patterns (refuse to wipe prod with empty data).
  - Aborts if R&D DB doesn't exist.
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Tuple


IST = timezone(timedelta(hours=5, minutes=30))
ROOT = Path(__file__).resolve().parent.parent

# Default paths — override via env if needed.
RND_DB  = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
PROD_DB = ROOT / "data" / "db" / "kanida_universe.db"

# Tables to sync, in dependency order (candidates is referenced by promoted via FK-style joins).
SYNC_TABLES: Tuple[str, ...] = ("falcon_pattern_candidates", "falcon_promoted_patterns")


def _now_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


def _read_schema_columns(con: sqlite3.Connection, table: str) -> list:
    return [r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]


def _verify_schemas_match(rnd_con: sqlite3.Connection, prod_con: sqlite3.Connection) -> None:
    for t in SYNC_TABLES:
        rnd_cols  = _read_schema_columns(rnd_con,  t)
        prod_cols = _read_schema_columns(prod_con, t)
        if not rnd_cols:
            raise RuntimeError(f"R&D DB missing table: {t}")
        if not prod_cols:
            raise RuntimeError(f"PROD DB missing table: {t} — run db_init.py first")
        if rnd_cols != prod_cols:
            raise RuntimeError(
                f"Schema mismatch on {t}:\n  R&D : {rnd_cols}\n  PROD: {prod_cols}"
            )


def _count(con: sqlite3.Connection, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _audit_row(con: sqlite3.Connection, status: str, notes: str,
                rows: int | None = None, error: str | None = None,
                started_at: str | None = None) -> None:
    """Append a row to falcon_signal_runs for operator visibility."""
    con.execute(
        """INSERT INTO falcon_signal_runs
             (job_name, started_at, finished_at, status, rows_affected, notes, error)
           VALUES ('patterns_publish', ?, ?, ?, ?, ?, ?)""",
        (started_at or _now_ist(), _now_ist(), status, rows, notes, error)
    )


def _read_mining_window_years(prod_con: sqlite3.Connection) -> int:
    """Read `mining_window_years` from PROD's falcon_engine_config singleton.
    Falls back to 4 if the table/column is missing (older deployments)."""
    try:
        row = prod_con.execute(
            "SELECT mining_window_years FROM falcon_engine_config WHERE id=1"
        ).fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except Exception:
        pass
    return 4


def publish_patterns(rnd_path: Path = RND_DB, prod_path: Path = PROD_DB,
                      *, dry_run: bool = False,
                      mining_window_years: int | None = None) -> dict:
    """Copy promoted patterns + candidates from R&D -> production. Transactional.

    `mining_window_years` defaults to the value in `falcon_engine_config`
    (operator-tunable at /falcon/config). Only patterns with mined_year
    >= (current_year_IST - mining_window_years) are published.

    Returns summary dict. Raises on fatal config errors.
    """
    started_at = _now_ist()
    ist_now = datetime.now(IST)
    current_year = ist_now.year
    print(f"[{started_at}] B4 publish_patterns: starting")
    print(f"  R&D : {rnd_path}")
    print(f"  PROD: {prod_path}")

    if not rnd_path.exists():
        raise FileNotFoundError(f"R&D DB not found: {rnd_path}")
    if not prod_path.exists():
        raise FileNotFoundError(f"PROD DB not found: {prod_path}")

    rnd_con  = sqlite3.connect(str(rnd_path))
    prod_con = sqlite3.connect(str(prod_path))
    prod_con.execute("PRAGMA journal_mode = WAL")
    prod_con.execute("PRAGMA foreign_keys = OFF")  # we do full-replace, no need for FK trips

    try:
        _verify_schemas_match(rnd_con, prod_con)

        # Resolve mining-window cutoff: explicit arg > engine_config > default 4
        if mining_window_years is None:
            mining_window_years = _read_mining_window_years(prod_con)
        cutoff_year = current_year - int(mining_window_years)
        print(f"  Mining window: {mining_window_years} years -> mined_year >= {cutoff_year}")

        # Source counts (raw + filtered)
        rnd_total_promoted   = _count(rnd_con, "falcon_promoted_patterns")
        rnd_total_candidates = _count(rnd_con, "falcon_pattern_candidates")
        if rnd_total_promoted == 0:
            raise RuntimeError(
                "R&D DB has 0 promoted patterns. Refusing to wipe PROD with empty data. "
                "Run weekly re-mine + validate first (Workflow B1 + B3)."
            )

        # Compute filtered counts on R&D side (post-cutoff)
        rnd_filtered_candidates = rnd_con.execute(
            "SELECT COUNT(*) FROM falcon_pattern_candidates WHERE mined_year >= ?",
            (cutoff_year,)
        ).fetchone()[0]
        rnd_filtered_promoted = rnd_con.execute(
            """SELECT COUNT(*) FROM falcon_promoted_patterns p
               WHERE p.pattern_id IN (
                 SELECT pattern_id FROM falcon_pattern_candidates
                 WHERE mined_year >= ?
               )""",
            (cutoff_year,)
        ).fetchone()[0]

        # Pre counts on prod (for audit)
        pre_promoted   = _count(prod_con, "falcon_promoted_patterns")
        pre_candidates = _count(prod_con, "falcon_pattern_candidates")
        print(f"  R&D  : {rnd_total_promoted} promoted / {rnd_total_candidates} candidates (raw)")
        print(f"  R&D  : {rnd_filtered_promoted} promoted / {rnd_filtered_candidates} candidates (after cutoff)")
        print(f"  PROD : {pre_promoted} promoted / {pre_candidates} candidates (before)")

        if dry_run:
            print("  [dry_run=True - no writes]")
            return {
                "status":              "dry_run",
                "mining_window_years": mining_window_years,
                "cutoff_year":         cutoff_year,
                "rnd_raw":             {"promoted": rnd_total_promoted, "candidates": rnd_total_candidates},
                "rnd_filtered":        {"promoted": rnd_filtered_promoted, "candidates": rnd_filtered_candidates},
                "prod_before":         {"promoted": pre_promoted, "candidates": pre_candidates},
            }

        # ── Transactional full-replace ────────────────────────────────────────
        # Tables filtered independently:
        #   - falcon_pattern_candidates: WHERE mined_year >= cutoff_year
        #   - falcon_promoted_patterns: WHERE pattern_id IN (eligible candidate IDs)
        # Note: candidates is synced FIRST (it's the parent — referenced by the
        # promoted JOIN). SYNC_TABLES already orders that way.
        prod_con.execute("BEGIN IMMEDIATE")
        try:
            for t in SYNC_TABLES:
                cols = _read_schema_columns(rnd_con, t)
                col_list = ", ".join(cols)
                placeholders = ", ".join("?" * len(cols))

                if t == "falcon_pattern_candidates":
                    rows = rnd_con.execute(
                        f"SELECT {col_list} FROM {t} WHERE mined_year >= ?",
                        (cutoff_year,)
                    ).fetchall()
                elif t == "falcon_promoted_patterns":
                    rows = rnd_con.execute(
                        f"""SELECT {col_list} FROM {t}
                            WHERE pattern_id IN (
                              SELECT pattern_id FROM falcon_pattern_candidates
                              WHERE mined_year >= ?
                            )""",
                        (cutoff_year,)
                    ).fetchall()
                else:
                    rows = rnd_con.execute(f"SELECT {col_list} FROM {t}").fetchall()

                prod_con.execute(f"DELETE FROM {t}")
                if rows:
                    prod_con.executemany(
                        f"INSERT INTO {t} ({col_list}) VALUES ({placeholders})",
                        rows,
                    )
                print(f"  -> wrote {len(rows):>5,} rows to PROD.{t}")

            # Audit row inside the same transaction
            notes = (
                f"window={mining_window_years}yr (cutoff_year>={cutoff_year}). "
                f"R&D->PROD: {rnd_filtered_promoted} promoted (was {pre_promoted}), "
                f"{rnd_filtered_candidates} candidates (was {pre_candidates}). "
                f"R&D raw: {rnd_total_promoted}/{rnd_total_candidates}."
            )
            _audit_row(prod_con, "success",
                       notes=notes,
                       rows=rnd_filtered_promoted + rnd_filtered_candidates,
                       started_at=started_at)
            prod_con.execute("COMMIT")
        except Exception as e:
            prod_con.execute("ROLLBACK")
            # Best-effort audit write outside the failed transaction
            try:
                _audit_row(prod_con, "failed",
                           notes=f"rollback (window={mining_window_years}yr)",
                           error=str(e),
                           started_at=started_at)
                prod_con.commit()
            except Exception:
                pass
            raise

        # Post counts
        post_promoted   = _count(prod_con, "falcon_promoted_patterns")
        post_candidates = _count(prod_con, "falcon_pattern_candidates")
        print(f"  PROD : {post_promoted} promoted / {post_candidates} candidates (after) [OK]")

        return {
            "status":              "success",
            "mining_window_years": mining_window_years,
            "cutoff_year":         cutoff_year,
            "rnd_raw":             {"promoted": rnd_total_promoted, "candidates": rnd_total_candidates},
            "rnd_filtered":        {"promoted": rnd_filtered_promoted, "candidates": rnd_filtered_candidates},
            "prod_before":         {"promoted": pre_promoted,  "candidates": pre_candidates},
            "prod_after":          {"promoted": post_promoted, "candidates": post_candidates},
            "started_at":          started_at,
            "finished_at":         _now_ist(),
        }

    finally:
        rnd_con.close()
        prod_con.close()


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    try:
        result = publish_patterns(dry_run=dry_run)
        print()
        print(f"Result: {result['status']}")
        if result["status"] == "success":
            f = result["rnd_filtered"]
            print(f"  Synced {f['promoted']} promoted patterns + {f['candidates']} candidates "
                  f"(window={result['mining_window_years']}yr, mined_year>={result['cutoff_year']})")
            print(f"  Audit row written to falcon_signal_runs (job_name='patterns_publish')")
        sys.exit(0)
    except Exception as e:
        print(f"\n[FAIL] {e}", file=sys.stderr)
        sys.exit(1)
