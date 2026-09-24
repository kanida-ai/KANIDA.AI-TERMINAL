"""Definitive: do all strategies use the SAME stocks per day?
Compare (1) Trailing Agent file, (2) Dynamic Agent file, (3) my regenerated Top-5
(used by Buy&Hold + per-stock trailing). Read-only."""
import sqlite3, bisect
from pathlib import Path
import pandas as pd
from falcon_signal_replay import load_patterns, rank_for_date

ROOT = Path(__file__).resolve().parent.parent
SLIM = ROOT / "data" / "db" / "kanida_universe.db"
DESK = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"

trail = pd.read_excel(DESK / "Intraday_Trailing_Agent.xlsx", "Daily_Trade_Log")
dyn = pd.read_excel(DESK / "Dynamic_Agent.xlsx", "Daily_Trade_Log")

def to_map(df, datecol):
    m = {}
    for _, r in df.iterrows():
        d = str(r[datecol])[:10]
        m[d] = set(s.strip() for s in str(r["stocks"]).split(","))
    return m

T = to_map(trail, "entry_date")
D = to_map(dyn, "date")

# regenerate MY top-5 for each entry date (signal_date = prior trading day)
con = sqlite3.connect(str(SLIM))
patterns = load_patterns(con)
cal = [r[0] for r in con.execute("SELECT DISTINCT trade_date FROM ohlc_daily ORDER BY 1")]
def prev_td(d):
    i = bisect.bisect_left(cal, d); return cal[i-1] if i > 0 else None

entry_dates = sorted(T.keys())
MINE = {}
for ed in entry_dates:
    sd = prev_td(ed)
    if not sd:
        continue
    rk = rank_for_date(con, patterns, sd, min_fires=10)
    if rk:
        MINE[ed] = set(c["symbol"] for c in rk[:5])
con.close()

def avg_overlap(a, b, dates):
    ovs = [len(a[d] & b[d]) for d in dates if d in a and d in b]
    return sum(ovs)/len(ovs), sum(1 for o in ovs if o == 5), len(ovs)

common_TD = [d for d in T if d in D]
common_TM = [d for d in T if d in MINE]
o_td, id_td, n_td = avg_overlap(T, D, common_TD)
o_tm, id_tm, n_tm = avg_overlap(T, MINE, common_TM)

print("=== STOCK SELECTION CONSISTENCY (out of 5 per day) ===\n")
print(f"Trailing Agent file dates: {len(T)} ({min(T)}..{max(T)})")
print(f"Dynamic Agent file dates : {len(D)} ({min(D)}..{max(D)})")
print(f"My regenerated dates     : {len(MINE)} ({min(MINE)}..{max(MINE)})\n")
print(f"Trailing vs Dynamic  : avg overlap {o_td:.2f}/5  | identical-5 days {id_td}/{n_td} ({id_td/n_td*100:.0f}%)")
print(f"Trailing vs MY-regen : avg overlap {o_tm:.2f}/5  | identical-5 days {id_tm}/{n_tm} ({id_tm/n_tm*100:.0f}%)")

print("\n=== sample mismatches (Trailing-file vs my-regen) ===")
shown = 0
for d in common_TM:
    if T[d] != MINE[d] and shown < 6:
        print(f"  {d}: file={sorted(T[d])}")
        print(f"         mine={sorted(MINE[d])}")
        shown += 1
