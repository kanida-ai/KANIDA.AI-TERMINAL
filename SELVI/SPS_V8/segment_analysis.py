"""
SPS_V8 segmentation: stop averaging, find WHICH segment works.
Forward return = intraday open->EOD (buy 9:15 open, sell close). All ranking measures
as-of PRIOR close (leak-free). Period 2024-05..2026-07 (the trade-log era).

Three layers:
  A. MTD bucket continuation      (winners keep winning?)
  B. Cross-sectional decile by 20d momentum (top10% vs next, monotonic? relative)
  C. Persistence: days-in-top-decile streak (5-day survivors vs today's new entrants)
"""
import sqlite3
from pathlib import Path
import numpy as np, pandas as pd

DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
HERE = Path(__file__).resolve().parent
P0, P1 = "2024-05-01", "2026-07-31"

con = sqlite3.connect(str(DB))
d = pd.read_sql("SELECT symbol,bar_time,open,high,low,close,volume FROM ohlc_daily", con); con.close()
d["date"] = pd.to_datetime(d["bar_time"])
piv = lambda c: d.pivot_table(index="date", columns="symbol", values=c).sort_index()
O, C, V = piv("open"), piv("close"), piv("volume")
adv = (C * V).rolling(20).mean().shift(1)
liq = adv >= 3e7                                   # tradeable
fwd = (C - O) / O * 100                            # intraday open->EOD (the trade)
ret20 = ((C / C.shift(20) - 1) * 100).shift(1)     # momentum, as-of prior close
ret5 = ((C / C.shift(5) - 1) * 100).shift(1)
# MTD as-of prior close
month = C.index.to_period("M")
mstart = C.groupby(month).transform(lambda x: x.iloc[0])   # first close of the month per column
mtd = ((C.shift(1) / mstart.shift(1) - 1) * 100)
dts = [dt for dt in C.index if P0 <= dt.strftime("%Y-%m-%d") <= P1]

def stack(feat):
    rows = []
    for dt in dts:
        el = liq.loc[dt]
        for s in C.columns:
            if not el.get(s, False) or pd.isna(fwd.loc[dt, s]) or pd.isna(feat.loc[dt, s]):
                continue
            rows.append((dt, s, feat.loc[dt, s], fwd.loc[dt, s]))
    return pd.DataFrame(rows, columns=["date", "sym", "feat", "fwd"])

print("=" * 66)
print(f"SEGMENTATION · fwd = intraday open->EOD · {P0}..{P1} · liquid names")
print("=" * 66)

# ---- A. MTD buckets ----
A = stack(mtd)
bins = [-1e9, -5, 0, 5, 10, 20, 1e9]; names = ["<-5%", "-5..0%", "0-5%", "5-10%", "10-20%", ">20%"]
A["bucket"] = pd.cut(A["feat"], bins, labels=names)
print("\nA. CONTINUATION by MONTH-TO-DATE return bucket (winners keep winning?)")
print(f"  {'MTD bucket':10}{'n':>8}{'avg fwd%':>10}{'win%':>8}{'  (fwd = same-day open->EOD)'}")
for b in names:
    g = A[A["bucket"] == b]
    if len(g): print(f"  {b:10}{len(g):>8}{g['fwd'].mean():>9.3f}%{(g['fwd']>0).mean()*100:>7.0f}%")

# ---- B. cross-sectional deciles by 20d momentum ----
print("\nB. CROSS-SECTIONAL DECILES by 20-day momentum (relative; D1=top 10%)")
print(f"  {'decile':8}{'avg fwd%':>10}{'win%':>8}")
dec_rows = []
for dt in dts:
    el = liq.loc[dt]; sub = pd.DataFrame({"m": ret20.loc[dt][el], "f": fwd.loc[dt][el]}).dropna()
    if len(sub) < 30: continue
    sub["dec"] = pd.qcut(sub["m"], 10, labels=False, duplicates="drop")
    for dc, g in sub.groupby("dec"): dec_rows.append((dc, g["f"].mean(), (g["f"] > 0).mean()))
D = pd.DataFrame(dec_rows, columns=["dec", "f", "w"])
means = D.groupby("dec")["f"].mean(); wins = D.groupby("dec")["w"].mean()
for dc in sorted(means.index, reverse=True):
    tag = "  <- top 10%" if dc == means.index.max() else ("  <- bottom 10%" if dc == means.index.min() else "")
    print(f"  D{10-int(dc):<7}{means[dc]:>9.3f}%{wins[dc]*100:>7.0f}%{tag}")
print(f"  TOP-minus-BOTTOM decile spread: {means[means.index.max()]-means[means.index.min()]:+.3f}%/day")

# ---- C. persistence: streak in top decile ----
print("\nC. PERSISTENCE: forward return by days-held in TOP decile (survivors vs new)")
topdec = ret20.rank(axis=1, pct=True) >= 0.9        # top 10% by 20d momentum, as-of prior close
streak = topdec.copy().astype(int)
for i in range(1, len(streak)):
    streak.iloc[i] = np.where(topdec.iloc[i], streak.iloc[i-1] + 1, 0)
buckets = {"new (1 day)": (1, 1), "2-4 days": (2, 4), "5+ days (survivors)": (5, 999)}
print(f"  {'top-decile tenure':22}{'n':>8}{'avg fwd%':>10}{'win%':>8}")
for lbl, (lo, hi) in buckets.items():
    vals = []
    for dt in dts:
        el = liq.loc[dt]; st = streak.loc[dt]
        mask = el & (st >= lo) & (st <= hi)
        for s in C.columns[mask.values]:
            if not pd.isna(fwd.loc[dt, s]): vals.append(fwd.loc[dt, s])
    a = np.array(vals)
    if len(a): print(f"  {lbl:22}{len(a):>8}{a.mean():>9.3f}%{(a>0).mean()*100:>7.0f}%")

# chart of deciles
try:
    import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(9, 4.5))
    xs = [10 - int(dc) for dc in sorted(means.index)]; ys = [means[dc] for dc in sorted(means.index)]
    ax.bar([f"D{x}" for x in xs], ys, color=["#2e7d32" if y > 0 else "#c62828" for y in ys])
    ax.axhline(0, color="k", lw=.6); ax.set_ylabel("avg intraday fwd return %")
    ax.set_title(f"Forward open->EOD by 20d-momentum decile (D1=top) · {P0}..{P1}")
    fig.tight_layout(); fig.savefig(HERE / "segment_deciles.png", dpi=120); plt.close(fig)
    print(f"\n[chart -> {HERE/'segment_deciles.png'}]")
except Exception as e:
    print("plot skipped:", e)
