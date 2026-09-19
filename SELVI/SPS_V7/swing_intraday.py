"""
Test the user's hypothesis: SWING-pattern selection, INTRADAY execution with the trail.
Select swing-flagged stocks (hit_15pc_20d active patterns, point-in-time), rank top-15,
buy 50%@09:15 + 50%@09:16, apply the basket trail (arm6/floor2/give5/hard3, 5x), square 15:29.
Monthly P&L in the standard format. Leak-free (patterns use only past data).
"""
import sqlite3, re, sys
from pathlib import Path
import numpy as np, pandas as pd

DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
PAT = r"C:\Users\SPS\Downloads\Falcon_Promoted_Patterns.xlsx"
CAP = 500_000.0; ARM, FLOOR, GIVEBACK, HARD, RT = 0.06, 0.02, 0.05, 0.03, 0.0012
START, END = "2024-05-01", "2026-07-31"
RANKF = sys.argv[1] if len(sys.argv) > 1 else "roc_20"

con = sqlite3.connect(str(DB))
lab = pd.read_sql("SELECT symbol,sector FROM instrument_labels", con)
d = pd.read_sql("SELECT symbol,bar_time,open,high,low,close,volume FROM ohlc_daily", con)
mk = pd.read_sql("SELECT bar_time,close FROM ohlc_daily WHERE symbol='NIFTY 50'", con); con.close()
d["date"] = pd.to_datetime(d["bar_time"]); piv = lambda c: d.pivot_table(index="date", columns="symbol", values=c).sort_index()
O, H, L, C, V = [piv(x) for x in ["open", "high", "low", "close", "volume"]]
mk["date"] = pd.to_datetime(mk["bar_time"]); MK = mk.set_index("date")["close"].reindex(C.index).ffill()
sector = dict(zip(lab.symbol, lab.sector))
pc = C.shift(1); TR = np.maximum(H - L, np.maximum((H - pc).abs(), (L - pc).abs()))
F = {"atr_20_pct": TR.rolling(20).mean() / C * 100, "atr_5_vs_20": TR.rolling(5).mean() / TR.rolling(20).mean(),
     "close_loc": (C - L) / (H - L).replace(0, np.nan), "vol_vs_20d": V / V.rolling(20).mean()}
for n in (5, 20, 60): F[f"roc_{n}"] = (C / C.shift(n) - 1) * 100
for n in (10, 20, 60, 120, 252): F[f"dist_high_{n}"] = (C / H.rolling(n).max() - 1) * 100
for n in (20, 50, 200): F[f"dist_sma_{n}"] = (C / C.rolling(n).mean() - 1) * 100
for n in (20, 50): sma = C.rolling(n).mean(); F[f"slope_sma_{n}"] = (sma / sma.shift(5) - 1) * 100
for n in (20, 60):
    sr = (C / C.shift(n) - 1) * 100; mr = (MK / MK.shift(n) - 1) * 100; F[f"rs_market_{n}d"] = sr.sub(mr, axis=0)
    sec = pd.DataFrame(index=C.index, columns=C.columns, dtype=float); bs = {}
    for s in C.columns: bs.setdefault(sector.get(s, "NA"), []).append(s)
    for k, mem in bs.items():
        mm = sr[mem].mean(axis=1)
        for s in mem: sec[s] = mm
    F[f"rs_sector_{n}d"] = sr - sec
rng = (H - L) / pc * 100
F["n_sub_3_range_7d"] = (rng < 3).rolling(7).sum(); F["n_sub_2_5_range_7d"] = (rng < 2.5).rolling(7).sum()
lvm = (V < 0.75 * V.rolling(20).mean()); F["n_sub_75v_7d"] = lvm.rolling(7).sum(); F["n_sub_75v_20d"] = lvm.rolling(20).sum()
wkp = C.index.to_period("W")
F["weekly_close_loc"] = (C - L.groupby(wkp).cummin()) / (H.groupby(wkp).cummax() - L.groupby(wkp).cummin()).replace(0, np.nan)
F["weekly_range_pct"] = (H.groupby(wkp).cummax() - L.groupby(wkp).cummin()) / C * 100
Wc = C.resample("W-FRI").last(); wsma = Wc.rolling(20).mean().shift(1).reindex(C.index, method="ffill")
F["weekly_close_vs_sma20"] = (C / wsma - 1) * 100

P = pd.read_excel(PAT, sheet_name="promoted_patterns"); pats = P[(P.target == "hit_15pc_20d") & (P.engine_active)]
def pmask(rule):
    m = None
    for cond in rule.split("&"):
        mm = re.match(r"([a-z0-9_]+)\s*(<=|>=|<|>)\s*(-?\d+\.?\d*)", cond.strip())
        if not mm or mm.group(1) not in F: return None
        f, op, val = mm.group(1), mm.group(2), float(mm.group(3)); x = F[f]
        c = {"<=": x <= val, ">=": x >= val, "<": x < val, ">": x > val}[op]; m = c if m is None else m & c
    return m
flagged = pd.DataFrame(False, index=C.index, columns=C.columns)
for _, r in pats.iterrows():
    m = pmask(str(r["rule"]))
    if m is not None: flagged = flagged | m.fillna(False)
rank = F[RANKF]
# daily picks: flagged at t-1 (point-in-time), top-15 by rank at t-1, enter at t open
picks = {}
for i in range(1, len(C.index)):
    dt = C.index[i]
    if not (START <= dt.strftime("%Y-%m-%d") <= END): continue
    prev = C.index[i - 1]
    pool = [s for s in flagged.columns if flagged.at[prev, s]]
    pool = [s for s in pool if not pd.isna(rank.at[prev, s]) and not pd.isna(O.at[dt, s])]
    if not pool: continue
    picks[dt] = [s for _, s in sorted(((rank.at[prev, s], s) for s in pool), reverse=True)[:15]]

def basket_exit(cp):
    peak = -9; armed = False
    for x in cp:
        peak = max(peak, x)
        if not armed and x <= -HARD: return -HARD
        if x >= ARM: armed = True
        if armed:
            fl = max(FLOOR, peak - GIVEBACK)
            if x <= fl: return fl
    return cp[-1] if len(cp) else 0.0

con = sqlite3.connect(str(DB)); rows = []
for dt, syms in sorted(picks.items()):
    ds = dt.strftime("%Y-%m-%d")
    q = ("SELECT symbol,bar_time,open,close FROM ohlc_1min WHERE symbol IN (%s) AND bar_time>=? AND bar_time<=? ORDER BY bar_time" % ",".join("?" * len(syms)))
    m = pd.read_sql(q, con, params=list(syms) + [ds + " 09:15:00", ds + " 15:29:00"])
    if m.empty: continue
    m["t"] = pd.to_datetime(m["bar_time"]); paths = []
    for s in syms:
        g = m[m.symbol == s].set_index("t")
        try:
            e = (float(g.loc[ds + " 09:15:00", "open"]) + float(g.loc[ds + " 09:16:00", "open"])) / 2
        except KeyError: continue
        if e <= 0: continue
        paths.append(g.loc[ds + " 09:16:00":, "close"].astype(float) / e - 1)
    if not paths: continue
    cp = 5.0 * pd.concat(paths, axis=1).ffill().mean(axis=1).to_numpy()
    ex = basket_exit(cp) - RT * 5
    rows.append({"date": dt, "ret5x": ex, "ret1x": ex / 5, "pnl": CAP * ex})
con.close()
bt = pd.DataFrame(rows); bt["month"] = bt["date"].dt.strftime("%Y-%m")
print(f"SWING-select + INTRADAY-trail · rank={RANKF} · {len(bt)} days · leak-free")
print(f"{'month':9}{'days':>5}{'days_pos':>10}{'ret5x_pct':>11}{'ret1x_pct':>11}{'pnl_rs':>12}{'cum5x_pct':>11}")
cum = 0
for mo, g in bt.groupby("month"):
    r5 = g["ret5x"].sum() * 100; r1 = g["ret1x"].sum() * 100; cum += r5
    print(f"{mo:9}{len(g):>5}{int((g['ret1x']>0).sum()):>10}{r5:>10.1f}{r1:>10.1f}{g['pnl'].sum():>12,.0f}{cum:>10.1f}")
print("-" * 69)
print(f"{'TOTAL':9}{len(bt):>5}{int((bt['ret1x']>0).sum()):>10}{bt['ret5x'].sum()*100:>10.1f}{bt['ret1x'].sum()*100:>10.1f}{bt['pnl'].sum():>12,.0f}")
print(f"avg/mo 1x: {bt['ret1x'].sum()*100/max(len(bt.groupby('month')),1):.1f}%  (their logs ~23-26%/mo)")
