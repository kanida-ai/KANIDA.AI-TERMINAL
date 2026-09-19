"""Monthly benchmark view: month | days | days_pos | ret1x% | ret5x%
against the KPI benchmark (20%/mo -> 140% at 1x / 700% at 5x on Rs16.8L over 7 mo).
Return % = month P&L / total allocated capital.  ORBlate basket, sealed 2026.
Run: PYTHONIOENCODING=utf-8 python monthly_benchmark.py
"""
import json, sqlite3
from pathlib import Path
import numpy as np, pandas as pd
import finalize
from finalize import load_min, load_atr, day_pack, VAULT

HERE = Path(__file__).resolve().parent
CAP = 30_000.0
port = json.load(open(HERE / "portfolio.json")); syms = port["list"]
ALLOC = CAP * len(syms)
con = sqlite3.connect(str(HERE / "scan_results.db"))
specs, thrs = {}, {}
for s in syms:
    r = con.execute("SELECT spec, thr FROM champ WHERE sym=?", (s,)).fetchone()
    specs[s] = json.loads(r[0]); thrs[s] = r[1]
con.close()

rows = []
for s in syms:
    atr = load_atr(s); vault = day_pack(load_min(s, *VAULT, unlock=True))
    for d, net in finalize.sim_series(s, vault, atr, specs[s], thrs[s]).items():
        rows.append({"date": pd.to_datetime(d), "pnl1x": CAP * net, "pnl5x": CAP * 5 * net})
df = pd.DataFrame(rows)
df["month"] = df["date"].dt.strftime("%Y-%m")
daily = df.groupby("date").agg(pnl1x=("pnl1x", "sum"), pnl5x=("pnl5x", "sum")).reset_index()
daily["month"] = daily["date"].dt.strftime("%Y-%m")

print(f"BENCHMARK: 20%/mo -> 140% (1x) / 700% (5x) on Rs{ALLOC:,.0f} over 7 mo  [= +1%/day]")
print(f"{'month':8}{'days':>6}{'days_pos':>10}{'ret1x%':>10}{'ret5x%':>10}")
tot1 = tot5 = 0; td = tp = 0
for m, g in daily.groupby("month"):
    r1 = g["pnl1x"].sum() / ALLOC * 100; r5 = g["pnl5x"].sum() / ALLOC * 100
    dp = int((g["pnl1x"] > 0).sum()); dd = len(g)
    tot1 += r1; tot5 += r5; td += dd; tp += dp
    print(f"{m:8}{dd:>6}{dp:>10}{r1:>9.2f}%{r5:>9.2f}%")
print("-" * 44)
print(f"{'TOTAL':8}{td:>6}{tp:>10}{tot1:>9.2f}%{tot5:>9.2f}%")
print(f"{'BENCHMARK':8}{'':>6}{'':>10}{'140.00%':>10}{'700.00%':>10}")
print(f"{'% of bmk':8}{'':>6}{'':>10}{tot1/140*100:>9.1f}%{tot5/700*100:>9.1f}%")
