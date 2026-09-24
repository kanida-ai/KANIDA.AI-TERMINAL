"""Falcon Phase 2 — mine pattern candidates."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.falcon_miner import mine_candidates
from engine.falcon_universe import all_sectors

DB = ROOT / "data" / "db" / "kanida_universe.db"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", default="2022,2023,2024,2025")
    ap.add_argument("--targets", default="hit_10pc_20d,hit_15pc_20d,hit_25pc_30d,hit_40pc_40d")
    ap.add_argument("--max-depth", type=int, default=4)
    ap.add_argument("--min-samples-leaf", type=int, default=50)
    ap.add_argument("--min-lift", type=float, default=10.0)
    ap.add_argument("--workers", type=int, default=48)
    ap.add_argument("--include-sectors", action="store_true",
                    help="In addition to universe-wide, mine each sector separately")
    args = ap.parse_args()

    years   = [y.strip() for y in args.years.split(",")]
    targets = [t.strip() for t in args.targets.split(",")]

    con = sqlite3.connect(DB)
    sectors = all_sectors(con) if args.include_sectors else []
    con.close()

    scopes = ["universe"] + [f"sector:{s}" for s in sectors]
    print(f"DB:       {DB}")
    print(f"Years:    {years}")
    print(f"Targets:  {targets}")
    print(f"Scopes:   {len(scopes)} (universe + {len(sectors)} sectors)")
    print(f"Tree:     depth={args.max_depth}, min_leaf={args.min_samples_leaf}, min_lift={args.min_lift}pp")
    print()

    n = mine_candidates(DB, years, targets, scopes,
                          max_depth=args.max_depth,
                          min_samples_leaf=args.min_samples_leaf,
                          min_lift_pct=args.min_lift,
                          n_workers=args.workers)

    print(f"\nPersisted: {n} candidate patterns.")

    # Quick view
    con = sqlite3.connect(DB)
    print("\n=== Top 20 candidates by lift_pct ===")
    rows = con.execute("""
        SELECT mined_year, scope, outcome_target, n_obs, n_hits,
               precision_pct, base_rate_pct, lift_pct, rule_text
        FROM falcon_pattern_candidates
        ORDER BY lift_pct DESC LIMIT 20
    """).fetchall()
    for r in rows:
        print(f"  {r[0]}/{r[1][:30]:30s}/{r[2]:14s} | "
              f"n={r[3]:>5} hits={r[4]:>5} prec={r[5]:5.1f}% base={r[6]:5.1f}% "
              f"lift={r[7]:+.1f}pp | {r[8][:80]}")

    print("\n=== Candidates per year × outcome ===")
    rows = con.execute("""
        SELECT mined_year, outcome_target, COUNT(*) AS n
        FROM falcon_pattern_candidates
        GROUP BY mined_year, outcome_target
        ORDER BY mined_year, outcome_target
    """).fetchall()
    for r in rows: print(f"  {r[0]}/{r[1]}: {r[2]}")
    con.close()


if __name__ == "__main__":
    sys.exit(main() or 0)
