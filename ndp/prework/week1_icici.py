# -*- coding: utf-8 -*-
"""NDP week-1 cheap falsification for ICICIBANK (TOUCH basis). Daily-bar EXEC_APPROX.
Runs the three tests that gate the whole engine build:
  1. Data manifest (REQ-DATA-001)
  2. Gap-variance decomposition (REQ-B-015 / B9) -- sizes the opportunity
  3. Base rates TOUCH & CLOSE for the four buckets (REQ-A / Addendum A A2.2)
  4. MDE / power (REQ-POW-002) -- is a realistic edge even detectable
Arm A is NOT run: it requires 1-minute data, which ICICIBANK does not have.
"""
import sqlite3, hashlib
import numpy as np, pandas as pd
SYM = "ICICIBANK"; UDB = "data/db/kanida_universe.db"
con = sqlite3.connect("file:" + UDB + "?mode=ro", uri=True)
g = pd.read_sql_query("SELECT trade_date,open,high,low,close,volume FROM ohlc_daily WHERE symbol=? ORDER BY trade_date", con, params=[SYM]); con.close()
o, h, l, c = g.open.values, g.high.values, g.low.values, g.close.values
pc = np.roll(c, 1); pc[0] = np.nan

print("="*74); print(f"NDP WEEK-1 FALSIFICATION  |  {SYM}  |  basis: TOUCH (daily EXEC_APPROX)"); print("="*74)
# 1. manifest
sha = hashlib.sha256(pd.util.hash_pandas_object(g, index=True).values.tobytes()).hexdigest()[:16]
print(f"\n[1] DATA MANIFEST  rows={len(g):,}  {g.trade_date.min()} -> {g.trade_date.max()}  sha={sha}")
print("     1-minute data: ABSENT for ICICIBANK -> Arm A / path-tensor / TOUCH-via-1min NOT computable.")

# next-day series (enter at T+1 open, the 09:15 proxy)
o1 = np.roll(o, -1); h1 = np.roll(h, -1); l1 = np.roll(l, -1); c1 = np.roll(c, -1)
for a in (o1, h1, l1, c1): a[-1] = np.nan
gap = o1 / c - 1.0                    # overnight gap
intra = c1 / o1 - 1.0                 # T+1 open->close  (CLOSE-basis next-day return)
total = c1 / c - 1.0                  # close-to-close
m = ~np.isnan(intra)
gap, intra, total = gap[m], intra[m], total[m]
h1m, l1m, o1m = h1[m], l1[m], o1[m]

# 2. gap variance decomposition
print("\n[2] GAP-VARIANCE DECOMPOSITION  (how much of next-day move is the unpredictable overnight gap)")
vg, vi, vt = np.var(gap), np.var(intra), np.var(total)
print(f"     var(gap)/var(total)      = {vg/vt*100:5.1f}%   <- share ODP/confirmation could delete")
print(f"     var(intraday)/var(total) = {vi/vt*100:5.1f}%")
print(f"     corr(gap, intraday)      = {np.corrcoef(gap,intra)[0,1]:+.3f}   (>0 gaps continue, <0 gaps fade)")
# by vol regime
retc = pd.Series(c).pct_change(); vol20 = retc.rolling(20).std().values[1:][ -len(gap):] if False else retc.rolling(20).std().shift(1).values
vol20 = pd.Series(c).pct_change().rolling(20).std().shift(1).values
vol20 = vol20[np.where(m)[0]]
hi = vol20 >= np.nanmedian(vol20)
print(f"     var(gap)/var(total)  LOW-vol regime = {np.var(gap[~hi])/np.var(total[~hi])*100:4.1f}%   HIGH-vol = {np.var(gap[hi])/np.var(total[hi])*100:4.1f}%")

# 3. base rates
def touch_long(thr):  return np.mean(h1m/o1m - 1 >= thr)
def touch_short(thr): return np.mean(l1m/o1m - 1 <= -thr)
def close_long(thr):  return np.mean(intra >= thr)
def close_short(thr): return np.mean(intra <= -thr)
print("\n[3] UNCONDITIONAL BASE RATES  (the bar the 70% must beat; entry = T+1 open)")
print(f"     {'bucket':<16}{'TOUCH':>10}{'CLOSE':>10}   (Addendum A quoted TOUCH ~57/34, ~54/31 for ICICI)")
rows = [("Buy  >=+0.5%", touch_long(.005),  close_long(.005)),
        ("StrongBuy>=+1%", touch_long(.010), close_long(.010)),
        ("Sell <=-0.5%", touch_short(.005),  close_short(.005)),
        ("StrongSell<=-1%", touch_short(.010), close_short(.010))]
for name, t, cl in rows: print(f"     {name:<16}{t*100:9.1f}%{cl*100:9.1f}%")
print(f"     lift needed to reach 70% on TOUCH:  Buy {70-touch_long(.005)*100:+.0f}pp   Sell {70-touch_short(.005)*100:+.0f}pp")

# 4. MDE / power
print("\n[4] MDE / POWER  (min detectable mean-return edge at 80% power, per-trade)")
sd = np.std(intra); yrs = len(gap)/252
print(f"     per-trade return SD = {sd*100:.2f}%   history = {yrs:.1f}yr daily (EXEC_APPROX)")
print(f"     {'trades/yr':>10}{'N(OOS 3.5y)':>13}{'MDE_80 (bps)':>14}{'verdict':>16}")
z = 1.2816 + 1.6449
for tpy in (12, 25, 50, 100):
    N = tpy * 3.5
    mde = z * sd / np.sqrt(N) * 1e4
    verd = "informative" if mde <= 15 else ("marginal" if mde <= 30 else "UNTESTABLE")
    print(f"     {tpy:>10}{N:>13.0f}{mde:>14.1f}{verd:>16}")
print("\n" + "="*74)
print("NOTE: TOUCH here is a DAILY high/low proxy (EXEC_APPROX). True TOUCH needs 1-min VWAP fills.")
print("Arm A deferred (no 1-min). Gap-share + base rates + MDE are valid on daily bars.")
print("="*74)
