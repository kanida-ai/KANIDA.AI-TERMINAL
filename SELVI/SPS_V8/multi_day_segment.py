"""
Multi-day segmentation: does momentum CONTINUE over days (not intraday)?
Signal at close t (leak-free); forward return close_t -> close_{t+H} for H in {1,3,5}.
Segments: MTD bucket, 20d-momentum decile (relative), persistence streak.
Reports total H-day return AND per-day equivalent (vs the +1%/day goal).
"""
import sqlite3
from pathlib import Path
import numpy as np, pandas as pd

DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
HERE = Path(__file__).resolve().parent
P0, P1 = "2024-05-01", "2026-07-31"

con = sqlite3.connect(str(DB))
d = pd.read_sql("SELECT symbol,bar_time,close,volume FROM ohlc_daily", con); con.close()
d["date"] = pd.to_datetime(d["bar_time"])
C = d.pivot_table(index="date", columns="symbol", values="close").sort_index()
V = d.pivot_table(index="date", columns="symbol", values="volume").sort_index()
liq = ((C * V).rolling(20).mean() >= 3e7)                 # tradeable, as of close t
ret20 = (C / C.shift(20) - 1) * 100                       # momentum at close t
month = C.index.to_period("M")
mstart = C.groupby(month).transform(lambda x: x.iloc[0])
mtd = (C / mstart - 1) * 100                              # MTD at close t
dts = [dt for dt in C.index if P0 <= dt.strftime("%Y-%m-%d") <= P1]

# top-decile streak (persistence)
topdec = ret20.rank(axis=1, pct=True) >= 0.9
streak = topdec.astype(int).copy()
for i in range(1, len(streak)):
    streak.iloc[i] = np.where(topdec.iloc[i], streak.iloc[i - 1] + 1, 0)

for H in (1, 3, 5):
    fwd = (C.shift(-H) / C - 1) * 100                     # close_t -> close_{t+H}
    print("\n" + "=" * 62)
    print(f"HORIZON H={H} day(s)  ·  forward = close_t -> close_t+{H}")
    print("=" * 62)
    # stack qualified (liquid) rows
    rows = []
    for dt in dts:
        el = liq.loc[dt]
        for s in C.columns:
            if el.get(s, False) and not pd.isna(fwd.loc[dt, s]) and not pd.isna(mtd.loc[dt, s]) and not pd.isna(ret20.loc[dt, s]):
                rows.append((s, mtd.loc[dt, s], ret20.loc[dt, s], streak.loc[dt, s], fwd.loc[dt, s], dt))
    A = pd.DataFrame(rows, columns=["sym", "mtd", "m20", "streak", "fwd", "date"])
    # A. MTD buckets
    bins = [-1e9, -5, 0, 5, 10, 20, 1e9]; names = ["<-5%", "-5..0%", "0-5%", "5-10%", "10-20%", ">20%"]
    A["bkt"] = pd.cut(A["mtd"], bins, labels=names)
    print("A. by MTD bucket:   " + f"{'bucket':10}{'n':>7}{'fwd%':>9}{'per-day%':>10}{'win%':>7}")
    for b in names:
        g = A[A["bkt"] == b]
        if len(g): print(f"                    {b:10}{len(g):>7}{g['fwd'].mean():>8.2f}%{g['fwd'].mean()/H:>9.3f}%{(g['fwd']>0).mean()*100:>6.0f}%")
    # B. deciles by 20d momentum (per-day cross-section)
    dec = []
    for dt, g in A.groupby("date"):
        if len(g) < 30: continue
        g = g.copy(); g["dec"] = pd.qcut(g["m20"], 10, labels=False, duplicates="drop")
        for dc, gg in g.groupby("dec"): dec.append((dc, gg["fwd"].mean()))
    Dd = pd.DataFrame(dec, columns=["dec", "f"]); mn = Dd.groupby("dec")["f"].mean()
    top, bot = mn.index.max(), mn.index.min()
    print(f"B. momentum deciles: D1(top) fwd {mn[top]:+.2f}% ({mn[top]/H:+.3f}%/day) · "
          f"D10(bot) {mn[bot]:+.2f}% · SPREAD D1-D10 {mn[top]-mn[bot]:+.2f}% ({(mn[top]-mn[bot])/H:+.3f}%/day)")
    # C. persistence
    print("C. persistence:      " + f"{'tenure':22}{'n':>7}{'fwd%':>9}{'per-day%':>10}{'win%':>7}")
    for lbl, lo, hi in [("new (1d)", 1, 1), ("2-4d", 2, 4), ("5+ d survivors", 5, 999)]:
        g = A[(A["streak"] >= lo) & (A["streak"] <= hi)]
        if len(g): print(f"                     {lbl:22}{len(g):>7}{g['fwd'].mean():>8.2f}%{g['fwd'].mean()/H:>9.3f}%{(g['fwd']>0).mean()*100:>6.0f}%")
