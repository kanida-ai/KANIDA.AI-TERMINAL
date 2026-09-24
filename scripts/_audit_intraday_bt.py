"""Step 0 audit pt4 (fast, single-pass) — coverage gate. Read-only."""
import sqlite3
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
c = sqlite3.connect(str(DB))
OHLC_START = "2024-05-13"

print("Loading Top-10 picks (entry_date > OHLC_START)...", flush=True)
picks = defaultdict(list)
for ed, rk, sym in c.execute("""select entry_date, engine_rank, symbol
        from falcon_signal_day_study
        where engine_rank<=10 and entry_date > ?""", (OHLC_START,)):
    picks[ed].append((rk, sym))
entry_dates = sorted(picks)
print(f"  candidate entry dates: {len(entry_dates)}  "
      f"({entry_dates[0]} .. {entry_dates[-1]})", flush=True)

# Single full scan: for each (symbol, date) collect presence of 09:15 and 15:29 bars.
print("Single-pass scan of ohlc_1min for 09:15 & 15:29 bars (be patient)...", flush=True)
cov = {}        # (date, symbol) -> [has_open, has_close]
trading_dates = set()
sql = """select substr(bar_time,1,10) d, symbol,
                substr(bar_time,12,8) hms
         from ohlc_1min
         where substr(bar_time,12,8) in ('09:15:00','15:29:00')"""
for d, sym, hms in c.execute(sql):
    if hms == '09:15:00':
        trading_dates.add(d)
    rec = cov.get((d, sym))
    if rec is None:
        rec = [0, 0]; cov[(d, sym)] = rec
    if hms == '09:15:00': rec[0] = 1
    else:                 rec[1] = 1
print(f"  distinct trading dates in 1min: {len(trading_dates)} "
      f"({min(trading_dates)} .. {max(trading_dates)})", flush=True)

# Evaluate coverage per candidate entry date
full_days = usable_days = no_data_days = 0
fracs = []
missing = []
for ed in entry_dates:
    if ed not in trading_dates:
        no_data_days += 1
        continue
    syms = [s for _, s in picks[ed]]
    covered = [s for s in syms if cov.get((ed, s), [0, 0]) == [1, 1]]
    fracs.append(len(covered) / len(syms))
    if len(covered) == len(syms): full_days += 1
    if len(covered) >= 1: usable_days += 1
    for s in syms:
        if cov.get((ed, s), [0, 0]) != [1, 1] and len(missing) < 30:
            missing.append((ed, s))

n = len(entry_dates)
td = n - no_data_days
avg = sum(fracs) / len(fracs) if fracs else 0
print("\n=== COVERAGE GATE ===", flush=True)
print(f"  candidate entry dates              : {n}")
print(f"  not a 1min trading day (skip)      : {no_data_days}")
print(f"  trading days (1min present)        : {td}")
print(f"  days ALL 10 picks covered          : {full_days} ({full_days/td*100:.1f}%)")
print(f"  days usable (>=1 pick)             : {usable_days} ({usable_days/td*100:.1f}%)")
print(f"  avg per-day pick coverage          : {avg*100:.1f}%")
print(f"\n  sample missing (entry_date, symbol):")
for m in missing:
    print("   ", m)
