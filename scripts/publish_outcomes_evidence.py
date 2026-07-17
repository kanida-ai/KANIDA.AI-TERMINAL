"""Phase-0 / §4.2 — Publish the SERVE-TIME EVIDENCE artifact out of the R&D DB.

WHY THIS EXISTS
---------------
Doc 1 §4.2 ("the serve-time R&D dependency") is a MUST-FIX before cloud cutover:
`falcon_top20_router` opens the ~35 GB R&D DB **read-only at request time**
(falcon_top20_router.py:94 for /falcon-top-20, :214 for /falcon-explain) and the
explainer queries `falcon_outcomes` out of it. Doc 2 states the target rule:
"Signal generation reads only the active artifact version. It should never reach
into the research database at runtime."

This script produces that artifact. It does NOT change any behavior:
  * it READS the R&D DB read-only (mode=ro, query_only),
  * it WRITES a standalone artifact file under data/artifacts/,
  * NOTHING in backend/ reads that artifact yet (grep: no references).
Wiring the explainer to prefer the artifact is a SEPARATE, reviewable change.
By default this script never touches the PROD DB (see --publish-to).

EXACTLY WHAT THE SERVE PATH NEEDS (verified against the code, not assumed)
--------------------------------------------------------------------------
Only TWO reads of `falcon_outcomes` are live in the serve path:

1. falcon_top20_explainer.py:1145 (`_build_bucket2`)
       SELECT trade_date, ret_20d, hit_10pc_20d, mae_20d, mfe_20d
         FROM falcon_outcomes WHERE symbol=? AND trade_date IN (<fire dates>)
   The fire dates come from replaying rules over PROD `falcon_features`
   (explainer.py:1065-1070) bounded by `_BUCKET2_LOOKBACK_START = "2021-01-01"`
   (explainer.py:179). => the needed rows are bounded to trade_date >= 2021-01-01.

2. falcon_top20_explainer.py:1266 (`_stock_lifetime_baseline`)
       SELECT AVG(hit_10pc_20d)*100 FROM falcon_outcomes
        WHERE symbol=? AND hit_10pc_20d IS NOT NULL
   *** ALL-TIME, NO DATE FILTER. *** This is the subtle one: R&D holds
   falcon_outcomes back to 2016-01-01, so if we published only the 2021+ subset
   and let the baseline recompute over it, the denominator would shrink and the
   baseline would SILENTLY CHANGE. That would be a behavior change.
   => the baseline is PRECOMPUTED here over the FULL R&D history and shipped as
      a per-symbol scalar. 499 rows. Exact by construction.

`_load_outcomes` (explainer.py:1274) is DEAD CODE — defined, never called
(verified: no callers in backend/). Its columns are deliberately NOT published.

SIZING (measured 2026-07-16 against the live R&D DB)
----------------------------------------------------
  falcon_outcomes total ...... 827,379 rows / 499 symbols / 2016-01-01..2026-03-04
  needed (>= 2021-01-01) ..... 495,572 rows x 6 cols  -> the evidence table
  baseline ................... 499 rows x 3 cols      -> the scalar table
So a ~35 GB serve-time dependency collapses to a small artifact.

NOTE: `falcon_outcomes` ends 2026-03-04 while PROD `falcon_features` runs to
2026-07-16 — the labeler has not been re-run since ~March. Fire dates after
2026-03-04 already find no outcome row today; the artifact reproduces that miss
identically (a miss is a miss). Publishing therefore loses no freshness.

USAGE
-----
  # build the artifact (safe; reads R&D ro, writes data/artifacts/)
  python scripts/publish_outcomes_evidence.py

  # prove the artifact reproduces R&D's answers EXACTLY, for every symbol
  python scripts/publish_outcomes_evidence.py --verify-parity

  # explicit, opt-in publish into a target DB (NOT done by default)
  python scripts/publish_outcomes_evidence.py --publish-to data/db/kanida_universe.db

Per Doc 2 §5 "Artifact-Driven Pattern Publishing", the artifact carries a
manifest: row counts, checksum, cutoff, source commit, and build time, so a
rollback is a metadata flip to the previous artifact version.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

IST = timezone(timedelta(hours=5, minutes=30))
ROOT = Path(__file__).resolve().parent.parent

RND_DB = Path(os.environ.get(
    "POWER_RND_DB_PATH",
    str(ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"),
))
ARTIFACT_DIR = ROOT / "data" / "artifacts"

# MUST mirror falcon_top20_explainer._BUCKET2_LOOKBACK_START (explainer.py:179).
# Asserted at build time by _assert_cutoff_matches_explainer().
BUCKET2_LOOKBACK_START = "2021-01-01"

EVIDENCE_TABLE = "falcon_outcomes_evidence"
BASELINE_TABLE = "falcon_outcome_baseline"
MANIFEST_TABLE = "falcon_artifact_manifest"

# The exact column set _build_bucket2 selects (explainer.py:1146). No more.
EVIDENCE_COLS: Tuple[str, ...] = (
    "symbol", "trade_date", "ret_20d", "hit_10pc_20d", "mae_20d", "mfe_20d",
)


def _now_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


def _source_commit() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=str(ROOT),
            capture_output=True, text=True, timeout=10,
        )
        return out.stdout.strip()[:12] if out.returncode == 0 else "unknown"
    except Exception:
        return "unknown"


def _connect_ro(path: Path) -> sqlite3.Connection:
    """Read-only, query_only. A publish must never be able to mutate R&D."""
    if not path.exists():
        raise SystemExit(f"FATAL: R&D DB not found at {path}")
    con = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=120.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA query_only=ON")
    return con


def _assert_cutoff_matches_explainer() -> None:
    """Fail loudly if the explainer's lookback drifts from this script's cutoff.

    If someone widens _BUCKET2_LOOKBACK_START past our cutoff, the artifact would
    be missing rows the serve path asks for and evidence would silently degrade.
    Grep the constant rather than importing (importing pulls numpy + the whole
    power_user package; this script must stay dependency-light).
    """
    src = ROOT / "backend" / "power_user" / "services" / "falcon_top20_explainer.py"
    if not src.exists():
        print(f"WARN: cannot verify cutoff — {src} not found")
        return
    for line in src.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.strip().startswith("_BUCKET2_LOOKBACK_START"):
            got = line.split("=", 1)[1].strip().strip('"').strip("'")
            if got != BUCKET2_LOOKBACK_START:
                raise SystemExit(
                    f"FATAL: explainer _BUCKET2_LOOKBACK_START={got!r} but this "
                    f"script publishes from {BUCKET2_LOOKBACK_START!r}. The artifact "
                    f"would be missing rows the serve path reads. Update both."
                )
            print(f"  cutoff cross-check OK: explainer={got} == artifact={BUCKET2_LOOKBACK_START}")
            return
    print("WARN: _BUCKET2_LOOKBACK_START not found in explainer — cutoff unverified")


def _checksum(con: sqlite3.Connection) -> str:
    """Order-stable checksum over the published evidence rows."""
    h = hashlib.sha256()
    for row in con.execute(
        f"SELECT {', '.join(EVIDENCE_COLS)} FROM {EVIDENCE_TABLE} "
        f"ORDER BY symbol, trade_date"
    ):
        h.update(repr(tuple(row)).encode())
    return h.hexdigest()[:32]


# ══════════════════════════════════════════════════════════════════════════════
# BUILD
# ══════════════════════════════════════════════════════════════════════════════

def build_artifact(out_path: Path) -> Dict[str, Any]:
    print(f"Falcon serve-time evidence artifact — build @ {_now_ist()} IST")
    print(f"  source (ro) : {RND_DB}")
    print(f"  target      : {out_path}")
    _assert_cutoff_matches_explainer()

    rnd = _connect_ro(RND_DB)
    try:
        n_total = rnd.execute("SELECT COUNT(*) FROM falcon_outcomes").fetchone()[0]
        if n_total == 0:
            raise SystemExit("FATAL: R&D falcon_outcomes is empty — refusing to publish.")

        out_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = out_path.with_suffix(".building")
        for stale in (tmp_path, Path(str(tmp_path) + "-wal"), Path(str(tmp_path) + "-shm")):
            if stale.exists():
                stale.unlink()

        art = sqlite3.connect(str(tmp_path), timeout=120.0)
        try:
            art.executescript(f"""
                PRAGMA journal_mode=DELETE;
                CREATE TABLE {EVIDENCE_TABLE} (
                    symbol       TEXT NOT NULL,
                    trade_date   TEXT NOT NULL,
                    ret_20d      REAL,
                    hit_10pc_20d INTEGER,
                    mae_20d      REAL,
                    mfe_20d      REAL,
                    PRIMARY KEY (symbol, trade_date)
                );
                -- Precomputed over the FULL R&D history (see module docstring).
                -- Publishing the 2021+ subset alone would change the denominator.
                CREATE TABLE {BASELINE_TABLE} (
                    symbol                TEXT PRIMARY KEY,
                    lifetime_hit_pct      REAL NOT NULL,
                    n_rows_all_time       INTEGER NOT NULL
                );
                CREATE TABLE {MANIFEST_TABLE} (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                );
            """)

            # ── evidence rows (bounded by the explainer's lookback) ──────────
            print(f"  reading falcon_outcomes >= {BUCKET2_LOOKBACK_START} ...")
            rows = rnd.execute(
                f"SELECT {', '.join(EVIDENCE_COLS)} FROM falcon_outcomes "
                f"WHERE trade_date >= ? ORDER BY symbol, trade_date",
                (BUCKET2_LOOKBACK_START,),
            ).fetchall()
            art.executemany(
                f"INSERT INTO {EVIDENCE_TABLE} ({', '.join(EVIDENCE_COLS)}) "
                f"VALUES ({', '.join('?' * len(EVIDENCE_COLS))})",
                [tuple(r) for r in rows],
            )
            n_evidence = len(rows)

            # ── baseline: ALL-TIME, mirrors explainer.py:1266 exactly ───────
            # AVG() ignores NULLs, matching `WHERE hit_10pc_20d IS NOT NULL`.
            print("  precomputing all-time per-symbol baseline ...")
            base_rows = rnd.execute("""
                SELECT symbol,
                       AVG(hit_10pc_20d) * 100.0 AS lifetime_hit_pct,
                       COUNT(hit_10pc_20d)       AS n_rows_all_time
                  FROM falcon_outcomes
                 WHERE hit_10pc_20d IS NOT NULL
                 GROUP BY symbol
            """).fetchall()
            art.executemany(
                f"INSERT INTO {BASELINE_TABLE} "
                f"(symbol, lifetime_hit_pct, n_rows_all_time) VALUES (?, ?, ?)",
                [tuple(r) for r in base_rows],
            )
            art.commit()

            checksum = _checksum(art)
            span = art.execute(
                f"SELECT MIN(trade_date), MAX(trade_date) FROM {EVIDENCE_TABLE}"
            ).fetchone()
            manifest = {
                "artifact_kind":       "falcon_serve_evidence",
                "artifact_version":    datetime.now(IST).strftime("%Y%m%d%H%M%S"),
                "built_at_ist":        _now_ist(),
                "source_db":           str(RND_DB),
                "source_commit":       _source_commit(),
                "cutoff_trade_date":   BUCKET2_LOOKBACK_START,
                "evidence_rows":       str(n_evidence),
                "baseline_symbols":    str(len(base_rows)),
                "rnd_rows_all_time":   str(n_total),
                "evidence_date_min":   str(span[0]),
                "evidence_date_max":   str(span[1]),
                "checksum_sha256_32":  checksum,
                "schema_note": (
                    "Serves falcon_top20_explainer._build_bucket2 (:1145) and "
                    "_stock_lifetime_baseline (:1266). Baseline is precomputed "
                    "ALL-TIME; do not recompute it from this table."
                ),
            }
            art.executemany(
                f"INSERT INTO {MANIFEST_TABLE} (key, value) VALUES (?, ?)",
                list(manifest.items()),
            )
            art.execute(f"CREATE INDEX idx_ev_symbol ON {EVIDENCE_TABLE}(symbol, trade_date)")
            art.commit()
            art.execute("VACUUM")
            art.commit()
        finally:
            art.close()

        os.replace(tmp_path, out_path)   # atomic publish
        size_mb = out_path.stat().st_size / 1e6

        print(f"\n  evidence rows   : {n_evidence:,}  ({span[0]} .. {span[1]})")
        print(f"  baseline symbols: {len(base_rows):,}  (all-time, {n_total:,} source rows)")
        print(f"  checksum        : {checksum}")
        print(f"  artifact size   : {size_mb:.1f} MB   vs R&D {RND_DB.stat().st_size/1e9:.1f} GB")
        print(f"\nOK — artifact written: {out_path}")
        print("Nothing reads it yet. Behavior unchanged. Next: --verify-parity.")
        manifest["artifact_path"] = str(out_path)
        manifest["artifact_size_mb"] = round(size_mb, 1)
        return manifest
    finally:
        rnd.close()


# ══════════════════════════════════════════════════════════════════════════════
# PARITY — the validation gate
# ══════════════════════════════════════════════════════════════════════════════

def verify_parity(artifact: Path, limit_symbols: Optional[int] = None) -> bool:
    """Prove the artifact answers the serve path's two queries IDENTICALLY to R&D.

    Compares, per symbol:
      1. _stock_lifetime_baseline  -> exact float equality
      2. _build_bucket2's outcome lookup, over EVERY date >= cutoff that R&D has
         (a superset of any real fire-date set) -> exact tuple equality
    """
    print(f"Parity check — artifact vs R&D @ {_now_ist()} IST")
    if not artifact.exists():
        raise SystemExit(f"FATAL: artifact not found: {artifact} (build it first)")

    rnd = _connect_ro(RND_DB)
    art = _connect_ro(artifact)
    try:
        symbols = [r[0] for r in rnd.execute(
            "SELECT DISTINCT symbol FROM falcon_outcomes ORDER BY symbol"
        )]
        if limit_symbols:
            symbols = symbols[:limit_symbols]
        print(f"  comparing {len(symbols)} symbols ...")

        base_fail = ev_fail = 0
        for i, sym in enumerate(symbols, 1):
            # 1 — baseline (explainer.py:1266 verbatim vs artifact scalar)
            r = rnd.execute(
                "SELECT AVG(hit_10pc_20d) * 100 FROM falcon_outcomes "
                "WHERE symbol = ? AND hit_10pc_20d IS NOT NULL", (sym,)
            ).fetchone()
            rnd_base = float(r[0]) if r and r[0] is not None else 0.0
            a = art.execute(
                f"SELECT lifetime_hit_pct FROM {BASELINE_TABLE} WHERE symbol = ?", (sym,)
            ).fetchone()
            art_base = float(a[0]) if a and a[0] is not None else 0.0
            if rnd_base != art_base:
                base_fail += 1
                print(f"  BASELINE MISMATCH {sym}: rnd={rnd_base!r} art={art_base!r}")

            # 2 — evidence rows (explainer.py:1146 column order)
            rnd_rows = {
                row[0]: tuple(row)[1:] for row in rnd.execute(
                    "SELECT trade_date, ret_20d, hit_10pc_20d, mae_20d, mfe_20d "
                    "FROM falcon_outcomes WHERE symbol = ? AND trade_date >= ?",
                    (sym, BUCKET2_LOOKBACK_START),
                )
            }
            art_rows = {
                row[0]: tuple(row)[1:] for row in art.execute(
                    f"SELECT trade_date, ret_20d, hit_10pc_20d, mae_20d, mfe_20d "
                    f"FROM {EVIDENCE_TABLE} WHERE symbol = ? AND trade_date >= ?",
                    (sym, BUCKET2_LOOKBACK_START),
                )
            }
            if rnd_rows != art_rows:
                ev_fail += 1
                only_rnd = set(rnd_rows) - set(art_rows)
                only_art = set(art_rows) - set(rnd_rows)
                diff = [d for d in (set(rnd_rows) & set(art_rows))
                        if rnd_rows[d] != art_rows[d]]
                print(f"  EVIDENCE MISMATCH {sym}: missing={len(only_rnd)} "
                      f"extra={len(only_art)} differing={len(diff)}")
            if i % 100 == 0:
                print(f"    ... {i}/{len(symbols)}")

        print(f"\n  baseline mismatches : {base_fail}")
        print(f"  evidence mismatches : {ev_fail}")
        ok = (base_fail == 0 and ev_fail == 0)
        print("\nPARITY PASS — the artifact is a drop-in for the serve-time R&D reads."
              if ok else "\nPARITY FAIL — do NOT wire the serve path to this artifact.")
        return ok
    finally:
        rnd.close()
        art.close()


def publish_into(artifact: Path, target: Path) -> None:
    """Opt-in: copy the artifact tables into a target DB. Never runs by default.

    Additive: creates NEW tables only. Does not touch any table the running
    backend currently reads.
    """
    if not artifact.exists():
        raise SystemExit(f"FATAL: artifact not found: {artifact}")
    if not target.exists():
        raise SystemExit(f"FATAL: target DB not found: {target}")
    print(f"PUBLISH {artifact}  ->  {target}")
    print("  (additive: creates new tables; existing tables untouched)")

    con = sqlite3.connect(str(target), timeout=120.0)
    try:
        con.execute("ATTACH DATABASE ? AS art", (f"file:{artifact.as_posix()}?mode=ro",))
    except sqlite3.OperationalError:
        con.close()
        con = sqlite3.connect(str(target), timeout=120.0)
        con.execute(f"ATTACH DATABASE '{artifact.as_posix()}' AS art")
    try:
        con.execute("BEGIN IMMEDIATE")
        for tbl in (EVIDENCE_TABLE, BASELINE_TABLE, MANIFEST_TABLE):
            con.execute(f"DROP TABLE IF EXISTS main.{tbl}")
            con.execute(f"CREATE TABLE main.{tbl} AS SELECT * FROM art.{tbl}")
            n = con.execute(f"SELECT COUNT(*) FROM main.{tbl}").fetchone()[0]
            print(f"  {tbl}: {n:,} rows")
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_ev_symbol "
            f"ON {EVIDENCE_TABLE}(symbol, trade_date)"
        )
        con.commit()
        print("OK — published (transactional; both tables flipped together).")
        print("Rollback: DROP the three tables; no existing table was modified.")
    except Exception:
        con.rollback()
        raise
    finally:
        con.execute("DETACH DATABASE art")
        con.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", type=Path,
                    default=ARTIFACT_DIR / "falcon_serve_evidence.db",
                    help="artifact path (default: data/artifacts/falcon_serve_evidence.db)")
    ap.add_argument("--verify-parity", action="store_true",
                    help="compare the artifact against R&D for every symbol")
    ap.add_argument("--limit-symbols", type=int, default=None,
                    help="parity: check only the first N symbols (smoke test)")
    ap.add_argument("--publish-to", type=Path, default=None,
                    help="OPT-IN: copy artifact tables into this DB (additive)")
    args = ap.parse_args()

    if args.publish_to:
        publish_into(args.out, args.publish_to)
        return 0
    if args.verify_parity:
        return 0 if verify_parity(args.out, args.limit_symbols) else 1
    manifest = build_artifact(args.out)
    (args.out.parent / "falcon_serve_evidence.manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
