"""
Falcon Engine Phase 1 — build the (stock, date, features, outcomes) panel.

Steps:
  1. Populate falcon_sectors from Nifty 500 industry list
  2. Run outcome labeler on all symbols (multi-worker)
  3. Run feature extractor on all symbols (multi-worker)
  4. Print coverage & sanity stats

Multi-worker: 48 by default (max), uses ProcessPoolExecutor.
"""
from __future__ import annotations
import argparse, sqlite3, sys, time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.falcon_universe import populate_sectors, all_sectors
from engine.falcon_outcomes import label_universe
from engine.falcon_features import extract_universe_features

DB = ROOT / "data" / "db" / "kanida_universe.db"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2022-01-01")
    ap.add_argument("--end",   default="2026-04-30")
    ap.add_argument("--workers", type=int, default=48)
    ap.add_argument("--skip-sectors", action="store_true")
    ap.add_argument("--skip-outcomes", action="store_true")
    ap.add_argument("--skip-features", action="store_true")
    args = ap.parse_args()

    if not DB.exists(): sys.exit(f"DB not found: {DB}")
    print(f"DB: {DB}")
    print(f"Window: {args.start} -> {args.end}")
    print(f"Workers: {args.workers}")
    print()

    # ── Step 1: sectors ───────────────────────────────────────────────
    if not args.skip_sectors:
        print("[1/3] Populating falcon_sectors from Nifty 500 ...")
        con = sqlite3.connect(DB, timeout=60.0)
        n = populate_sectors(con)
        sectors = all_sectors(con)
        print(f"  {n} symbols mapped, {len(sectors)} unique sectors")
        print(f"  Sectors: {sectors[:8]} ...")
        con.close()

    # Get list of symbols that have data
    con = sqlite3.connect(DB)
    rows = con.execute("""
        SELECT DISTINCT symbol FROM ohlc_daily
        WHERE symbol IN (SELECT symbol FROM falcon_sectors)
    """).fetchall()
    symbols = [r[0] for r in rows]
    print(f"\nSymbols with data + sector mapping: {len(symbols)}")
    con.close()

    # ── Step 2: outcomes ─────────────────────────────────────────────
    if not args.skip_outcomes:
        print(f"\n[2/3] Computing forward outcomes ...")
        t0 = time.time()
        s = label_universe(DB, symbols, n_workers=args.workers)
        print(f"  Done in {(time.time()-t0)/60:.1f} min — {s['rows_total']:,} outcome rows")

    # ── Step 3: features ─────────────────────────────────────────────
    if not args.skip_features:
        print(f"\n[3/3] Extracting features ...")
        t0 = time.time()
        s = extract_universe_features(DB, symbols, args.start, args.end,
                                         n_workers=args.workers)
        print(f"  Done in {(time.time()-t0)/60:.1f} min — {s['rows_total']:,} feature rows")

    # ── Sanity check ────────────────────────────────────────────────
    con = sqlite3.connect(DB)
    print("\n=== Phase 1 panel coverage ===")
    out_n = con.execute("SELECT COUNT(*) FROM falcon_outcomes").fetchone()[0]
    out_syms = con.execute("SELECT COUNT(DISTINCT symbol) FROM falcon_outcomes").fetchone()[0]
    out_rng = con.execute("SELECT MIN(trade_date), MAX(trade_date) FROM falcon_outcomes").fetchone()
    feat_n = con.execute("SELECT COUNT(*) FROM falcon_features").fetchone()[0]
    feat_syms = con.execute("SELECT COUNT(DISTINCT symbol) FROM falcon_features").fetchone()[0]
    print(f"  falcon_outcomes: {out_n:,} rows, {out_syms} symbols, {out_rng[0]} -> {out_rng[1]}")
    print(f"  falcon_features: {feat_n:,} rows, {feat_syms} symbols")

    # Big-move base rates
    print("\n=== Outcome base rates (positive class %) ===")
    for col in ("hit_5pc_10d", "hit_7pc_10d", "hit_10pc_20d",
                  "hit_15pc_20d", "hit_25pc_30d", "hit_40pc_40d"):
        n_pos = con.execute(f"SELECT SUM({col}) FROM falcon_outcomes").fetchone()[0]
        rate = n_pos / out_n * 100 if out_n else 0
        print(f"  {col}: {n_pos:>8,} ({rate:5.2f}% of {out_n:,})")

    print("\n=== Per-year outcome distribution (hit_10pc_20d) ===")
    rows = con.execute("""
        SELECT substr(trade_date,1,4) AS yr,
               COUNT(*) AS n, SUM(hit_10pc_20d) AS hits
        FROM falcon_outcomes GROUP BY yr ORDER BY yr
    """).fetchall()
    for yr, n, h in rows:
        rate = h / n * 100 if n else 0
        print(f"  {yr}: {n:>7,} samples, {h:>5,} hits ({rate:5.2f}%)")

    con.close()
    print("\nPhase 1 complete. Ready for Phase 2 (mining).")


if __name__ == "__main__":
    sys.exit(main() or 0)
