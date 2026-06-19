"""Phase 0 — externalize the tier rulebook into the DB (falcon_tier_rules).

Creates the table in PROD (data/db) if missing and populates the CHAMPION
rulebook as DATA — the exact rules the deployed classify_signal_tier encodes,
with validated full-history metrics. The live classifier reads status='active'
rows (with a hardcoded fallback) so the rules become data, not code — the
foundation for the weekly self-learning loop.

conditions_json shape:  {"priority": <int>, "all": [[field, op, value], ...]}
  field ∈ sret|twoday|rng|avg_lift|trend3_20|turn_pct ; op ∈ <,<=,>,>=
  Rules are evaluated by ascending priority; first whose ALL conds hold wins.
  An empty "all" is the catch-all (lowest priority).

Idempotent: wipes status='active' rows and re-inserts. Read-only on everything
except the falcon_tier_rules table.
Run: "C:\\Users\\SPS\\anaconda3\\python.exe" scripts/tier_rulebook_migrate.py
"""
import os, sqlite3, json

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Operational DB: env override (so it can be run from a worktree against the
# live tree); defaults to ROOT-relative for in-place prod-tree deployment.
PROD = os.environ.get("POWER_DB_PATH") or os.path.join(ROOT, "data", "db", "kanida_universe.db")
AS_OF = "2026-06-19"   # validation date (scripts/validate_tier_feedback.py)

DDL = """
CREATE TABLE IF NOT EXISTS falcon_tier_rules (
  row_pk            INTEGER PRIMARY KEY AUTOINCREMENT,
  rule_id           TEXT,
  tier              TEXT,
  conditions_json   TEXT,
  scaling_path_json TEXT,
  is_wr             REAL,
  is_ret            REAL,
  is_n              INTEGER,
  oos_wr            REAL,
  oos_ret           REAL,
  oos_n             INTEGER,
  per_year_oos_json TEXT,
  status            TEXT,
  reason            TEXT,
  as_of             TEXT,
  created_at        TEXT DEFAULT (datetime('now'))
)
"""

# Champion rulebook — priority order (best/most-specific first), with the
# full-history metrics from the deployed-classifier validation (2026-06-19).
RULES = [
    ("avoid_extended",     "AVOID",               1,  [["sret", ">", 10]],
        41.8, 0.02, 273, "Extended >+10% — 41.8% WR, 38% stop"),
    ("avoid_froth",        "AVOID",               2,  [["sret", ">", 7], ["turn_pct", ">=", 0.75]],
        52.9, 1.12, 442, "Up >7% on heavy turnover — froth (~53% WR)"),
    ("premium_pullback",   "PREMIUM-Pullback",    3,  [["sret", "<=", 2], ["twoday", "<", -5], ["avg_lift", ">", 15]],
        83.2, 8.88, 368, "Bounce-from-drawdown + strong pattern — 83% WR, ~10% stop"),
    ("premium_compress",   "PREMIUM-Compression", 4,  [["sret", "<=", 2], ["rng", "<", 2], ["avg_lift", ">", 15]],
        83.6, 7.07, 500, "Tight-range coil + strong pattern — 84% WR, 6% stop"),
    ("enterprise_dryup",   "ENTERPRISE-Dryup",    5,  [["sret", "<=", 2], ["trend3_20", "<", 0.9]],
        72.3, 5.54, 2022, "Flat/down on drying volume — 72% WR"),
    ("gold",               "GOLD",                6,  [["sret", "<=", 2], ["turn_pct", "<", 0.75]],
        68.9, 4.30, 1504, "Flat/down on light turnover — 69% WR"),
    ("gold_baseline",      "GOLD-baseline",       7,  [["sret", "<=", 2]],
        61.1, 3.31, 1947, "Flat/down signal day — 61% WR (residual)"),
    ("standard",           "STANDARD",            8,  [["sret", "<=", 5]],
        61.3, 3.06, 2315, "Modestly up (≤+5%) — average edge"),
    ("standard_weak",      "STANDARD-weak",       9,  [["sret", "<=", 10]],
        57.4, 1.82, 686, "Up +5..+10% — weak edge"),
    ("catch_all",          "STANDARD-weak",       99, [],
        57.4, 1.82, 686, "Default when no rule matches"),
]


def main():
    con = sqlite3.connect(PROD)
    con.execute(DDL)
    con.execute("DELETE FROM falcon_tier_rules WHERE status = 'active'")
    for rid, tier, prio, conds, wr, ret, n, reason in RULES:
        con.execute(
            "INSERT INTO falcon_tier_rules "
            "(rule_id, tier, conditions_json, is_wr, is_ret, is_n, status, reason, as_of) "
            "VALUES (?,?,?,?,?,?, 'active', ?, ?)",
            (rid, tier, json.dumps({"priority": prio, "all": conds}),
             wr, ret, n, reason, AS_OF),
        )
    con.commit()
    rows = con.execute(
        "SELECT rule_id, tier, is_wr, is_n FROM falcon_tier_rules "
        "WHERE status='active' ORDER BY json_extract(conditions_json,'$.priority')"
    ).fetchall()
    con.close()
    print(f"Wrote {len(rows)} active rules to {PROD}:")
    for rid, tier, wr, n in rows:
        print(f"  {rid:<18} -> {tier:<20} IS_WR={wr}% N={n}")


if __name__ == "__main__":
    main()
