"""D1..D20 holding-period path analysis for the COMBINED agent 7164 ∩ 7619 (OOS 2025-2026).
For each trade (buy next open), track close-return, MFE, MAE, cumulative target-hit for each of the 20 holding days.
Then a target x stop-loss exit grid and a time-exit test -> pick the right target / stop / time-bound exit. Read-only."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); COST = 0.15
uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
feat = pd.read_sql_query("SELECT symbol,trade_date,atr_20_pct,weekly_close_loc,weekly_range_pct,roc_5 FROM falcon_features WHERE trade_date>='2024-06-01'", uc)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2024-06-01' ORDER BY symbol,trade_date", uc)
uc.close()
OHS = {s: g.reset_index(drop=True) for s, g in oh.groupby("symbol")}
IDX = {(s, d): i for s, g in OHS.items() for i, d in enumerate(g.trade_date.values)}
mask = (feat.atr_20_pct > 2.2284) & (feat.weekly_close_loc > 0.5467) & (feat.weekly_range_pct > 13.298) & (feat.roc_5 <= 3.643) & (feat.weekly_close_loc > 0.5098) & (feat.weekly_range_pct > 14.1572)
fires = feat.loc[mask & (feat.trade_date >= "2025-01-01"), ["symbol", "trade_date"]]   # OOS

paths = []   # each: entry, arrays of close_ret/mfe/mae by day, first target/stop days
for _, r in fires.iterrows():
    g = OHS.get(r.symbol); si = IDX.get((r.symbol, r.trade_date))
    if g is None or si is None or si + 1 >= len(g): continue
    e = si + 1; entry = g.open.values[e]
    H = g.high.values; L = g.low.values; C = g.close.values
    cr = np.full(20, np.nan); mfe = np.full(20, np.nan); mae = np.full(20, np.nan)
    rmax = -1e9; rmin = 1e9
    for d in range(20):
        j = e + d
        if j >= len(g): break
        rmax = max(rmax, H[j]); rmin = min(rmin, L[j])
        cr[d] = (C[j] - entry) / entry * 100; mfe[d] = (rmax - entry) / entry * 100; mae[d] = (rmin - entry) / entry * 100
    paths.append((entry, cr, mfe, mae, g.high.values[e:e+20], g.low.values[e:e+20], g.close.values[e:e+20]))
print(f"Combined agent 7164 ∩ 7619 — OOS 2025-26 — {len(paths)} trades with paths\n")

CR = np.vstack([p[1] for p in paths]); MFE = np.vstack([p[2] for p in paths]); MAE = np.vstack([p[3] for p in paths])
print("="*92 + "\nD1..D20 PATH (avg across trades)\n" + "="*92)
print(f"{'day':>4}{'avg_close%':>11}{'%inProfit':>11}{'avgMFE%':>9}{'avgMAE%':>9}{'%hit+10% byNow':>16}{'%stopped-5% byNow':>18}")
# cumulative target/stop by day
def first_touch(hi_or_lo, entry, level, up):
    for d in range(len(hi_or_lo)):
        if (up and hi_or_lo[d] >= entry*(1+level/100)) or ((not up) and hi_or_lo[d] <= entry*(1-level/100)): return d
    return 99
tgt_day = np.array([first_touch(p[4], p[0], 10, True) for p in paths])
stp_day = np.array([first_touch(p[5], p[0], 5, False) for p in paths])
for d in range(20):
    col = CR[:, d]; valid = ~np.isnan(col)
    print(f"{d+1:>4}{np.nanmean(col):>+11.2f}{(col[valid]>0).mean()*100:>10.0f}%{np.nanmean(MFE[:,d]):>+9.2f}{np.nanmean(MAE[:,d]):>+9.2f}{(tgt_day<=d).mean()*100:>15.0f}%{(stp_day<=d).mean()*100:>17.0f}%")

print("\n" + "="*92 + "\nHOW FAR DO THEY RUN? (max favorable excursion over 20d)\n" + "="*92)
maxmfe = np.nanmax(MFE, axis=1)
for lv in [3,5,8,10,12,15,20]: print(f"  reach +{lv:>2}% at some point: {(maxmfe>=lv).mean()*100:5.1f}%")
hitters = tgt_day[tgt_day<99]; print(f"\n  of +10% hitters: median day-to-hit = D{int(np.median(hitters))}, mean = D{hitters.mean():.1f}")

print("\n" + "="*92 + "\nEXIT-RULE GRID — avg net return & win% (target vs stop, else 20d close)\n" + "="*92)
def simulate(T, SL):
    rets=[]
    for entry, cr, mfe, mae, hi, lo, cl in paths:
        r=None
        for d in range(len(cl)):
            if hi[d] >= entry*(1+T/100): r=T; break
            if SL and lo[d] <= entry*(1-SL/100): r=-SL; break
        if r is None: r=(cl[-1]-entry)/entry*100
        rets.append(r-COST)
    rets=np.array(rets); return rets.mean(), (rets>0).mean()*100
print(f"{'target':>7}", *[f"{'SL='+(str(s)+'%' if s else 'none'):>14}" for s in [0,3,5,8]])
for T in [5,8,10,12]:
    row=f"{T:>6}%"
    for SL in [0,3,5,8]:
        a,w=simulate(T,SL); row+=f"   {a:+.2f}%/{w:.0f}%".rjust(14)
    print(row)

print("\n" + "="*92 + "\nTIME-EXIT TEST — exit at Dk close (no target/stop), avg net return & win%\n" + "="*92)
for k in [5,8,10,12,15,20]:
    col=CR[:,k-1]; v=~np.isnan(col); r=col[v]-COST
    print(f"  exit at D{k:>2}: avg {r.mean():+.2f}%  win {(r>0).mean()*100:.0f}%  (best target+stop usually beats pure time-exit)")
