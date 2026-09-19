"""
SPS_V7 — Reproduction of the T15 momentum-pullback screener (from the real trade logs).
Daily pre-open screen -> buy 9:15 open -> hard-stop OR EOD. Leak-free: selection uses
data through the PRIOR close only; entry at the day's open; exit by intraday stop or close.
Capital Rs5,00,000 (matches the logs), equal-weight across N picks, 1x and 5x.

Output: month | days | days_pos | ret5x_pct | ret1x_pct | pnl_rs | cum5x_pct

Run: PYTHONIOENCODING=utf-8 python momentum_repro.py
"""
import sqlite3
from pathlib import Path
import numpy as np, pandas as pd

DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
CAP = 500_000.0
N = 15                    # T15
HARD_STOP = 0.03          # -3% per-stock intraday hard stop
ADV_MIN = 3e7             # >=3 cr/day turnover eligibility
START, END = "2024-05-01", "2026-07-31"     # the trade-log period (for direct comparison)

con = sqlite3.connect(str(DB))
d = pd.read_sql("SELECT symbol,bar_time,open,high,low,close,volume FROM ohlc_daily", con)
con.close()
d["date"] = pd.to_datetime(d["bar_time"])
piv = lambda c: d.pivot_table(index="date", columns="symbol", values=c).sort_index()
O, H, L, C, V = [piv(x) for x in ["open", "high", "low", "close", "volume"]]

# prior-day features (shift 1 => known before entry day's open)
ret5 = C.pct_change(5).shift(1)
ret20 = C.pct_change(20).shift(1)
ret1 = C.pct_change().shift(1)
adv = (C * V).rolling(20).mean().shift(1)

# momentum-pullback score: strong 5d (+20d) momentum, slight prior-day dip
score = ret5.rank(axis=1) + 0.5 * ret20.rank(axis=1)

dates = [dt for dt in C.index if START <= dt.strftime("%Y-%m-%d") <= END]
rows = []
for dt in dates:
    elig = (adv.loc[dt] >= ADV_MIN) & ret5.loc[dt].notna() & ret20.loc[dt].notna() & O.loc[dt].notna()
    # pullback tilt: among momentum names, prefer prior-day dip (ret1 < +1%)
    cand = score.loc[dt][elig & (ret1.loc[dt] < 0.01)]
    if len(cand) < N:
        cand = score.loc[dt][elig]
    picks = cand.sort_values(ascending=False).head(N).index
    if len(picks) == 0:
        continue
    rets = []
    for s in picks:
        e = O.loc[dt, s]; lo = L.loc[dt, s]; cl = C.loc[dt, s]
        if pd.isna(e) or pd.isna(cl):
            continue
        r = -HARD_STOP if (not pd.isna(lo) and lo <= e * (1 - HARD_STOP)) else (cl - e) / e
        rets.append(r)
    if not rets:
        continue
    r1 = float(np.mean(rets))
    rows.append({"date": dt, "n": len(rets), "ret1x": r1, "ret5x": 5 * r1, "pnl": CAP * 5 * r1})

bt = pd.DataFrame(rows)
bt["month"] = bt["date"].dt.strftime("%Y-%m")
print(f"REPRODUCTION — T15 momentum-pullback · Rs{CAP:,.0f} · hard-stop {HARD_STOP*100:.0f}% · "
      f"{len(bt)} trading days {bt['date'].min().date()}..{bt['date'].max().date()}")
print(f"{'month':9}{'days':>5}{'days_pos':>10}{'ret5x_pct':>11}{'ret1x_pct':>11}{'pnl_rs':>12}{'cum5x_pct':>11}")
cum = 0
for m, g in bt.groupby("month"):
    r5 = g["ret5x"].sum() * 100; r1 = g["ret1x"].sum() * 100; pnl = g["pnl"].sum()
    dp = int((g["ret1x"] > 0).sum()); cum += r5
    print(f"{m:9}{len(g):>5}{dp:>10}{r5:>10.1f}{r1:>10.1f}{pnl:>12,.0f}{cum:>10.1f}")
tot1 = bt["ret1x"].sum() * 100
print("-" * 69)
print(f"{'TOTAL':9}{len(bt):>5}{int((bt['ret1x']>0).sum()):>10}{bt['ret5x'].sum()*100:>10.1f}{tot1:>10.1f}{bt['pnl'].sum():>12,.0f}")
print(f"\navg/month 1x: {tot1/ (len(bt.groupby('month'))):.1f}%  |  benchmark 20%/mo  |  their logs ~23-26%/mo")
