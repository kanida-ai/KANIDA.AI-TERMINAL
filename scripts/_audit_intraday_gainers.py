"""Step 0 audit for the same-day top-gainers research. Read-only."""
import sqlite3
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
START, END = "2024-05-14", "2026-06-15"
c = sqlite3.connect(str(DB))

print(f"window {START} .. {END}", flush=True)

# total trading days in window
days = [r[0] for r in c.execute(
    "SELECT DISTINCT substr(bar_time,1,10) d FROM ohlc_1min "
    "WHERE bar_time BETWEEN ? AND ? ORDER BY d", (START, END + " 23:59:59"))]
nd = len(days)
print(f"trading days in window: {nd}  ({days[0]} .. {days[-1]})", flush=True)

# per-symbol day coverage
rows = c.execute(
    "SELECT symbol, COUNT(DISTINCT substr(bar_time,1,10)) nd FROM ohlc_1min "
    "WHERE bar_time BETWEEN ? AND ? GROUP BY symbol", (START, END + " 23:59:59")).fetchall()
nsym = len(rows)
cov = np.array([r[1] / nd * 100 for r in rows])
print(f"\nsymbols with any data in window: {nsym}", flush=True)
print(f"day-coverage %: mean {cov.mean():.1f}, median {np.median(cov):.1f}, "
      f"min {cov.min():.1f}, max {cov.max():.1f}")
for thr in (99, 95, 90, 80, 50):
    print(f"  symbols >= {thr}% day-coverage: {(cov >= thr).sum()} ({(cov>=thr).mean()*100:.1f}%)")
worst = sorted(rows, key=lambda r: r[1])[:10]
print("  10 lowest-coverage symbols:", [(s, n) for s, n in worst])

# top-100 most traded -> bars per session (gap proxy). full session = 375 one-min bars.
print("\n[*] top-100 by avg daily turnover: bars-per-session check ...", flush=True)
turn = c.execute(
    "SELECT symbol, AVG(close*volume) t FROM ohlc_1min "
    "WHERE bar_time BETWEEN ? AND ? GROUP BY symbol ORDER BY t DESC LIMIT 100",
    (START, END + " 23:59:59")).fetchall()
top100 = [s for s, _ in turn]
ph = ",".join("?" * len(top100))
bars = c.execute(
    f"SELECT symbol, substr(bar_time,1,10) d, COUNT(*) n FROM ohlc_1min "
    f"WHERE bar_time BETWEEN ? AND ? AND symbol IN ({ph}) "
    f"GROUP BY symbol, d", [START, END + " 23:59:59", *top100]).fetchall()
bn = np.array([b[2] for b in bars])
print(f"  session-bar counts (expect ~375): mean {bn.mean():.1f}, median {np.median(bn):.0f}, "
      f"min {bn.min()}, p1 {np.percentile(bn,1):.0f}")
print(f"  sessions with <370 bars: {(bn<370).sum()} of {len(bn)} ({(bn<370).mean()*100:.2f}%)")
print(f"  sessions with <300 bars: {(bn<300).sum()} ({(bn<300).mean()*100:.2f}%)")
print("  -> <370 bars implies >=5 missing minutes somewhere in that session (gap proxy)")
