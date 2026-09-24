"""Reproduce falcon_top10_daily ranking from the engine, extended to top-25.
Parity test on one date before trusting regenerated ranks 11-25. Read-only."""
import sys, sqlite3
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
ENGINE = ROOT / "universe_engine"
sys.path.insert(0, str(ENGINE))
from engine.falcon_portfolio import (
    load_panel_with_keys, load_promoted_patterns, rule_mask, FEATURE_COLS,
)

BIG_DB = ENGINE / "data" / "db" / "kanida_universe.db"
SLIM_DB = ROOT / "data" / "db" / "kanida_universe.db"
SIGNAL_DATE = "2026-05-06"
MIN_FIRES = 10

for label, db in (("BIG", BIG_DB), ("SLIM", SLIM_DB)):
    try:
        cc = sqlite3.connect(str(db))
        r = cc.execute("select min(trade_date),max(trade_date),count(*) from falcon_features").fetchone()
        sd = cc.execute("select max(signal_date) from falcon_signal_day_study where persona='falcon_top10_daily'").fetchone()[0]
        print(f"[{label}] falcon_features {r[0]}..{r[1]} rows={r[2]}  | study max signal_date={sd}")
        cc.close()
    except Exception as e:
        print(f"[{label}] err {e}")

print(f"[load] panel from {BIG_DB} ...", flush=True)
X, syms, dates, years = load_panel_with_keys(BIG_DB)
print(f"   {X.shape[0]:,} feature rows; dates {dates.min()}..{dates.max()}", flush=True)

con = sqlite3.connect(str(BIG_DB))
patterns = load_promoted_patterns(con, exclude_families=["drawdown_bounce"])  # V7.1
yr = int(SIGNAL_DATE[:4])
elig = [p for p in patterns if int(p["mined_year"]) < yr]
print(f"   patterns total={len(patterns)} eligible(<{yr})={len(elig)}", flush=True)

mask_today = (dates == SIGNAL_DATE)
today_idx = np.where(mask_today)[0]
today_X = X[today_idx]
n_today = len(today_idx)
print(f"   stocks with features on {SIGNAL_DATE}: {n_today}", flush=True)

n_fires = np.zeros(n_today, dtype=np.int64)
sum_lift = np.zeros(n_today, dtype=np.float64)
for p in elig:
    m = rule_mask(p["rule"], today_X)
    if not m.any():
        continue
    n_fires += m.astype(np.int64)
    sum_lift += m.astype(np.float64) * p["oos_lift"]

avg_lift = np.where(n_fires > 0, sum_lift / np.maximum(n_fires, 1), 0.0)
cand = [(syms[today_idx[j]], int(n_fires[j]), float(sum_lift[j]), float(avg_lift[j]))
        for j in range(n_today) if n_fires[j] >= MIN_FIRES]
cand.sort(key=lambda t: -t[3])   # rank by avg_lift desc

print(f"\n=== REGENERATED top-25 (min_fires={MIN_FIRES}, rank by avg_lift) ===")
print(f"{'rk':>2} {'symbol':12} {'n_fires':>7} {'sum_lift':>10} {'avg_lift':>9}")
for i, (s, nf, sl, al) in enumerate(cand[:25], 1):
    print(f"{i:>2} {s:12} {nf:>7} {sl:>10.2f} {al:>9.4f}")

stored = [r for r in con.execute(
    """select engine_rank, symbol, avg_lift, n_fires from falcon_signal_day_study
       where persona='falcon_top10_daily' and signal_date=? order by engine_rank""",
    (SIGNAL_DATE,))]
con.close()
print("\n=== STORED falcon_top10_daily top-10 ===")
for r in stored:
    print(f"{r[0]:>2} {r[1]:12} avg_lift={r[2]:.4f} n_fires={r[3]}")

regen10 = [s for s, *_ in cand[:10]]
stored10 = [r[1] for r in stored]
print("\n=== PARITY ===")
print("  regen top-10 :", regen10)
print("  stored top-10:", stored10)
print("  EXACT MATCH  :", regen10 == stored10)
print("  set match    :", set(regen10) == set(stored10))
