"""Compare the backtest signal source (falcon_signal_day_study, research DB) vs the
LIVE emissions (falcon_signals_live, live DB) on overlapping dates. Read-only."""
import sqlite3
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
RND = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
LIVE = ROOT / "data" / "db" / "kanida_universe.db"

rc = sqlite3.connect(str(RND)); lc = sqlite3.connect(str(LIVE))

# study = backtest source (research DB)
study = {}
for d, rk, s in rc.execute("""SELECT signal_date, engine_rank, symbol FROM falcon_signal_day_study
        WHERE persona='falcon_top10_daily' AND engine_rank<=10"""):
    study.setdefault(d, {})[rk] = s
# live = production emissions (live DB)
live = {}
for d, rk, s in lc.execute("""SELECT signal_date, rank, symbol FROM falcon_signals_live
        WHERE rank<=10"""):
    live.setdefault(d, {})[rk] = s
rc.close(); lc.close()

print(f"study (research) dates: {len(study)}  range {min(study)}..{max(study)}")
print(f"live (production) dates: {len(live)}  range {min(live)}..{max(live)}")
common = sorted(set(study) & set(live))
print(f"OVERLAPPING dates: {len(common)}  range {min(common) if common else '-'}..{max(common) if common else '-'}")

if common:
    t10_match = t5_match = rank1_match = exact_order = 0
    diffs = []
    for d in common:
        sset10 = set(study[d].get(r) for r in range(1, 11) if study[d].get(r))
        lset10 = set(live[d].get(r) for r in range(1, 11) if live[d].get(r))
        sset5 = set(study[d].get(r) for r in range(1, 6) if study[d].get(r))
        lset5 = set(live[d].get(r) for r in range(1, 6) if live[d].get(r))
        ov10 = len(sset10 & lset10); ov5 = len(sset5 & lset5)
        t10_match += ov10; t5_match += ov5
        if study[d].get(1) == live[d].get(1):
            rank1_match += 1
        if [study[d].get(r) for r in range(1, 11)] == [live[d].get(r) for r in range(1, 11)]:
            exact_order += 1
        if ov10 < 10:
            diffs.append((d, ov10, sorted(sset10 - lset10), sorted(lset10 - sset10)))
    n = len(common)
    print(f"\nAvg Top-10 overlap: {t10_match/n:.2f} / 10  ({t10_match/n/10*100:.1f}%)")
    print(f"Avg Top-5  overlap: {t5_match/n:.2f} / 5   ({t5_match/n/5*100:.1f}%)")
    print(f"Rank-1 identical:   {rank1_match}/{n} days ({rank1_match/n*100:.1f}%)")
    print(f"Exact same order (all 10): {exact_order}/{n} days ({exact_order/n*100:.1f}%)")
    print(f"\nDays with <10/10 overlap: {len(diffs)}")
    for d, ov, only_study, only_live in diffs[:12]:
        print(f"  {d}: {ov}/10  only-in-study={only_study}  only-in-live={only_live}")
