"""Falcon Phase 3 — validate candidates and promote survivors."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.falcon_validator import validate_and_promote

DB = ROOT / "data" / "db" / "kanida_universe.db"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=48)
    args = ap.parse_args()

    summary = validate_and_promote(DB)
    print(f"\n=== Promotion summary ===")
    print(f"  Promoted:              {summary['promoted']}")
    print(f"  By classification:     {summary['by_classification']}")
    print(f"  Rejected breakdown:    {summary['rejected']}")

    # Top 20 promoted by avg OOS year lift
    con = sqlite3.connect(DB)
    print("\n=== Top 30 promoted patterns (by avg OOS year lift) ===")
    rows = con.execute("""
        SELECT p.pattern_id, p.classification, p.avg_oos_year_lift_pp,
               p.n_years_passed, p.avg_cross_sector_lift_pp,
               c.mined_year, c.scope, c.outcome_target,
               c.precision_pct, c.lift_pct, c.rule_text
        FROM falcon_promoted_patterns p
        INNER JOIN falcon_pattern_candidates c ON p.pattern_id = c.pattern_id
        ORDER BY p.avg_oos_year_lift_pp DESC LIMIT 30
    """).fetchall()
    for r in rows:
        print(f"  pid={r[0]} {r[1][:18]:18s} avg_oos_lift={r[2]:+5.1f}pp  "
              f"yrs_pass={r[3]} cross_sec={r[4]:+5.1f}pp | "
              f"{r[5]}/{r[6][:25]:25s}/{r[7]:14s} IS_lift={r[9]:+5.1f}pp | "
              f"{r[10][:80]}")

    print("\n=== Promoted patterns per (mined_year, target) ===")
    rows = con.execute("""
        SELECT c.mined_year, c.outcome_target, COUNT(*) AS n
        FROM falcon_promoted_patterns p
        INNER JOIN falcon_pattern_candidates c ON p.pattern_id = c.pattern_id
        GROUP BY c.mined_year, c.outcome_target ORDER BY c.mined_year, c.outcome_target
    """).fetchall()
    for r in rows: print(f"  {r[0]}/{r[1]}: {r[2]}")

    con.close()


if __name__ == "__main__":
    sys.exit(main() or 0)
