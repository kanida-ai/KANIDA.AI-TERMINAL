"""R&D-DB-split / Leg 2 — publish the UNFILTERED sim pattern set out of R&D.

WHY THIS EXISTS
---------------
The persona + co-trade simulators (`persona_simulator.py`, `cotrade_sim.py`)
call `persona_engine_core.load_full_patterns(rnd_db)`, which opens the ~38 GB
R&D DB at serve time and reads the UNFILTERED promoted-pattern catalog:

    SELECT c.mined_year, c.rule_json, p.avg_oos_year_lift_pp
      FROM falcon_promoted_patterns p
      JOIN falcon_pattern_candidates c ON p.pattern_id = c.pattern_id
     WHERE p.classification IN ('universal','regime_dependent')

This is deliberately UNFILTERED (~1826 R&D rows). PROD's published
`falcon_promoted_patterns`/`falcon_pattern_candidates` are mining-window-filtered
by `publish_patterns.py` (~834 rows) — that is why the sim cannot just repoint
at PROD without changing results. So the sim needs its own small artifact.

This script produces that artifact. It does NOT change any behaviour:
  * it READS the R&D DB read-only (mode=ro, query_only),
  * it WRITES a standalone artifact file under data/artifacts/,
  * NOTHING reads that artifact unless FALCON_SIM_PATTERNS_ARTIFACT is set.
Wiring `load_full_patterns` to prefer the artifact is a SEPARATE, default-off
change already landed in persona_engine_core.py.

ORDER MATTERS (the subtle parity point)
---------------------------------------
`compute_year_signals` accumulates `sum_lift` by float summation over the
pattern list in list order. Float addition is not associative, so a different
row order can produce a bit-different `score` and, on ties, a different
selection. The R&D read has NO `ORDER BY`, so its row order is SQLite's natural
JOIN order — deterministic for a fixed DB file. This script captures that exact
order in a `seq` column and the artifact loader replays `ORDER BY seq`, so the
served pattern list is byte-identical to the R&D read.

The `drawdown_bounce` Python filter in `load_full_patterns` is applied to BOTH
the R&D rows and the artifact rows (post-read), so the artifact stores the RAW
JOIN rows (~1826) and the loader filters identically — parity is exact.

USAGE
-----
  # build the artifact (safe; reads R&D ro, writes data/artifacts/)
  python scripts/publish_full_patterns.py

  # prove the artifact reproduces load_full_patterns()'s output EXACTLY
  python scripts/publish_full_patterns.py --verify-parity
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
from typing import Any, Dict, List, Optional, Tuple

IST = timezone(timedelta(hours=5, minutes=30))
ROOT = Path(__file__).resolve().parent.parent

RND_DB = Path(os.environ.get(
    "POWER_RND_DB_PATH",
    str(ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"),
))
ARTIFACT_DIR = ROOT / "data" / "artifacts"

SIM_PATTERNS_TABLE = "falcon_sim_patterns"
MANIFEST_TABLE = "falcon_artifact_manifest"

# The exact JOIN load_full_patterns runs (persona_engine_core.py). No ORDER BY —
# natural order is captured into `seq`.
SIM_PATTERNS_QUERY = """
    SELECT c.mined_year, c.rule_json, p.avg_oos_year_lift_pp
      FROM falcon_promoted_patterns p
      JOIN falcon_pattern_candidates c ON p.pattern_id = c.pattern_id
     WHERE p.classification IN ('universal','regime_dependent')
"""


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
    con.execute("PRAGMA query_only=ON")
    return con


def _checksum(rows: List[Tuple[Any, Any, Any]]) -> str:
    """Order-stable checksum over the published rows (in seq order)."""
    h = hashlib.sha256()
    for r in rows:
        h.update(repr(tuple(r)).encode())
    return h.hexdigest()[:32]


# ══════════════════════════════════════════════════════════════════════════════
# BUILD
# ══════════════════════════════════════════════════════════════════════════════

def build_artifact(out_path: Path) -> Dict[str, Any]:
    print(f"Falcon sim-patterns artifact — build @ {_now_ist()} IST")
    print(f"  source (ro) : {RND_DB}")
    print(f"  target      : {out_path}")

    rnd = _connect_ro(RND_DB)
    try:
        rows: List[Tuple[Any, Any, Any]] = rnd.execute(SIM_PATTERNS_QUERY).fetchall()
        if not rows:
            raise SystemExit("FATAL: sim-pattern JOIN returned 0 rows — refusing to publish.")

        # Metadata surrogate for "source cutoff": the newest mined_year present.
        mined_years = [int(r[0]) for r in rows]
        cutoff_mined_year = max(mined_years)

        out_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = out_path.with_suffix(".building")
        for stale in (tmp_path, Path(str(tmp_path) + "-wal"), Path(str(tmp_path) + "-shm")):
            if stale.exists():
                stale.unlink()

        art = sqlite3.connect(str(tmp_path), timeout=120.0)
        try:
            art.executescript(f"""
                PRAGMA journal_mode=DELETE;
                -- `seq` PRESERVES R&D's natural JOIN order (see module docstring:
                -- compute_year_signals' float sum is order-sensitive).
                CREATE TABLE {SIM_PATTERNS_TABLE} (
                    seq                  INTEGER PRIMARY KEY,
                    mined_year           INTEGER NOT NULL,
                    rule_json            TEXT    NOT NULL,
                    avg_oos_year_lift_pp REAL
                );
                CREATE TABLE {MANIFEST_TABLE} (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                );
            """)
            art.executemany(
                f"INSERT INTO {SIM_PATTERNS_TABLE} "
                f"(seq, mined_year, rule_json, avg_oos_year_lift_pp) VALUES (?, ?, ?, ?)",
                [(i, int(my), rj, (None if lift is None else float(lift)))
                 for i, (my, rj, lift) in enumerate(rows)],
            )
            art.commit()

            checksum = _checksum(rows)
            manifest = {
                "artifact_kind":       "falcon_sim_patterns",
                "artifact_version":    datetime.now(IST).strftime("%Y%m%d%H%M%S"),
                "built_at_ist":        _now_ist(),
                "source_db":           str(RND_DB),
                "source_commit":       _source_commit(),
                "classification_filter": "universal,regime_dependent",
                "mining_window_filter":  "NONE (unfiltered — this is the point)",
                "row_count":           str(len(rows)),
                "cutoff_mined_year":   str(cutoff_mined_year),
                "mined_year_min":      str(min(mined_years)),
                "checksum_sha256_32":  checksum,
                "schema_note": (
                    "Serves persona_engine_core.load_full_patterns. Rows are the "
                    "RAW JOIN (drawdown_bounce filter applied in Python at load). "
                    "ORDER BY seq reproduces R&D's natural row order exactly."
                ),
            }
            art.executemany(
                f"INSERT INTO {MANIFEST_TABLE} (key, value) VALUES (?, ?)",
                list(manifest.items()),
            )
            art.commit()
            art.execute("VACUUM")
            art.commit()
        finally:
            art.close()

        os.replace(tmp_path, out_path)   # atomic publish
        size_mb = out_path.stat().st_size / 1e6

        print(f"\n  rows            : {len(rows):,}  (unfiltered; mined_year "
              f"{min(mined_years)}..{cutoff_mined_year})")
        print(f"  checksum        : {checksum}")
        print(f"  artifact size   : {size_mb:.2f} MB   vs R&D {RND_DB.stat().st_size/1e9:.1f} GB")
        print(f"\nOK — artifact written: {out_path}")
        print("Nothing reads it unless FALCON_SIM_PATTERNS_ARTIFACT is set. Next: --verify-parity.")
        manifest["artifact_path"] = str(out_path)
        manifest["artifact_size_mb"] = round(size_mb, 3)
        return manifest
    finally:
        rnd.close()


# ══════════════════════════════════════════════════════════════════════════════
# PARITY — the validation gate
# ══════════════════════════════════════════════════════════════════════════════

def verify_parity(artifact: Path) -> bool:
    """Prove `load_full_patterns` yields the IDENTICAL list from artifact vs R&D.

    Imports the real loader so the exact Python post-processing (rule parse +
    drawdown_bounce filter) is exercised for both paths. Compares the full list
    element-by-element (order + values), because downstream float accumulation
    is order-sensitive.
    """
    print(f"Parity check — artifact vs R&D @ {_now_ist()} IST")
    if not artifact.exists():
        raise SystemExit(f"FATAL: artifact not found: {artifact} (build it first)")

    sys.path.insert(0, str(ROOT / "backend"))
    from power_user.services import persona_engine_core as pec  # noqa: E402

    # R&D path (flag unset)
    os.environ.pop("FALCON_SIM_PATTERNS_ARTIFACT", None)
    rnd_out = pec.load_full_patterns(str(RND_DB))

    # Artifact path (flag set)
    os.environ["FALCON_SIM_PATTERNS_ARTIFACT"] = str(artifact)
    try:
        art_out = pec.load_full_patterns(str(RND_DB))
    finally:
        os.environ.pop("FALCON_SIM_PATTERNS_ARTIFACT", None)

    print(f"  R&D patterns     : {len(rnd_out):,}")
    print(f"  artifact patterns: {len(art_out):,}")
    ok = (rnd_out == art_out)
    if not ok:
        if len(rnd_out) != len(art_out):
            print(f"  LENGTH MISMATCH: {len(rnd_out)} vs {len(art_out)}")
        else:
            for i, (a, b) in enumerate(zip(rnd_out, art_out)):
                if a != b:
                    print(f"  FIRST DIFF at index {i}: rnd={a!r} art={b!r}")
                    break
    print("\nPARITY PASS — the artifact is a drop-in for load_full_patterns."
          if ok else "\nPARITY FAIL — do NOT wire the sim to this artifact.")
    return ok


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", type=Path,
                    default=ARTIFACT_DIR / "falcon_sim_patterns.db",
                    help="artifact path (default: data/artifacts/falcon_sim_patterns.db)")
    ap.add_argument("--verify-parity", action="store_true",
                    help="compare load_full_patterns() output: artifact vs R&D")
    args = ap.parse_args()

    if args.verify_parity:
        return 0 if verify_parity(args.out) else 1
    manifest = build_artifact(args.out)
    (args.out.parent / "falcon_sim_patterns.manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
