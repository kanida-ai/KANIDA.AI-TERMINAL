# -*- coding: utf-8 -*-
"""NDP week-1 falsification for ICICIBANK using TRUE 1-minute data (the 65GB universe_engine DB).
Computes: real TOUCH & CLOSE base rates from the intraday path, Arm A (the trivial baseline), and MDE.
Entry fill = VWAP of first 2 minutes (fill-realism, no naked 09:15 open print). Intraday MIS, hold to close.
"""
import sqlite3
import numpy as np, pandas as pd
DB = "universe_engine/data/db/kanida_universe.db"; SYM = "ICICIBANK"
COST = 0.06  # % MIS intraday round-trip (STT sell 0.025 + brokerage + exch/GST + slippage), pre-verify
con = sqlite3.connect("file:" + DB + "?mode=ro", uri=True)
b = pd.read_sql_query("SELECT bar_time,open,high,low,close,volume FROM ohlc_1min WHERE symbol=? ORDER BY bar_time", con, params=[SYM]); con.close()
b["date"] = b.bar_time.str[:10]; b["hm"] = b.bar_time.str[11:16]
b = b[(b.hm >= "09:15") & (b.hm <= "15:30")]
print("="*72); print(f"NDP WEEK-1 (REAL 1-MIN)  |  {SYM}  |  {b.date.min()} -> {b.date.max()}  |  {b.date.nunique()} sessions"); print("="*72)

rows = []
for d, g in b.groupby("date"):
    g = g.sort_values("bar_time")
    if len(g) < 30: continue
    first2 = g.iloc[:2]
    entry = (first2.close * first2.volume).sum() / max(first2.volume.sum(), 1)  # VWAP first 2 min
    if entry <= 0: entry = g.iloc[0].open
    aft = g.iloc[1:]                                   # path after the entry window
    hi, lo, cl = aft.high.max(), aft.low.min(), g.iloc[-1].close
    o0915 = g.iloc[0].open
    # Arm A: first bar (after entry window) whose close > max(prev_close, open_0915)
    rows.append(dict(date=d, entry=entry, o0915=o0915, hi=hi, lo=lo, close=cl,
                     armA_bars=aft.close.values.tolist() if False else None,
                     _aft_close=aft.close.values, _aft_hi=aft.high.values, _aft_lo=aft.low.values))
S = pd.DataFrame(rows)
S["prev_close"] = S.close.shift(1)
S = S.dropna(subset=["prev_close"]).reset_index(drop=True)

# --- gap decomposition (1-min era) ---
S["gap"] = S.o0915 / S.prev_close - 1
S["intra"] = S.close / S.entry - 1
S["total"] = S.close / S.prev_close - 1
vt = S.total.var()
print(f"\n[GAP]  var(gap)/var(total) = {S.gap.var()/vt*100:.1f}%   corr(gap,intra) = {np.corrcoef(S.gap,S.intra)[0,1]:+.3f}   (1-min era)")

# --- TRUE TOUCH & CLOSE base rates (entry = VWAP first 2 min, hold to close) ---
mfe_l = S.hi / S.entry - 1; mfe_s = S.lo / S.entry - 1
def bt(x): return f"{x*100:5.1f}%"
print("\n[BASE RATES]  entry=VWAP(first 2min), intraday hold-to-close")
print(f"  {'bucket':<16}{'TOUCH':>8}{'CLOSE':>8}   (daily-proxy was 66/72; spec quoted ~57/54)")
print(f"  {'Buy >=+0.5%':<16}{bt((mfe_l>=.005).mean()):>8}{bt((S.intra>=.005).mean()):>8}")
print(f"  {'StrongBuy>=+1%':<16}{bt((mfe_l>=.010).mean()):>8}{bt((S.intra>=.010).mean()):>8}")
print(f"  {'Sell <=-0.5%':<16}{bt((mfe_s<=-.005).mean()):>8}{bt((S.intra<=-.005).mean()):>8}")
print(f"  {'StrongSell<=-1%':<16}{bt((mfe_s<=-.010).mean()):>8}{bt((S.intra<=-.010).mean()):>8}")

# --- Arm A: enter first bar close > max(prev_close, o0915), hold to close, net of cost ---
nets = []; entered = 0
for _, r in S.iterrows():
    trig = max(r.prev_close, r.o0915)
    idx = np.argmax(r._aft_close > trig) if (r._aft_close > trig).any() else -1
    if idx == -1: continue
    entered += 1
    ea = r._aft_close[idx]
    nets.append((r.close / ea - 1) * 100 - COST)     # long, MIS, hold to close, net
nets = np.array(nets)
print(f"\n[ARM A]  rule: enter first bar price>max(prev_close,open); long; hold to close; -{COST}% cost")
print(f"  trades {entered}/{len(S)} ({entered/len(S)*100:.0f}% of days)  ·  WR {(nets>0).mean()*100:.1f}%  ·  avg net {nets.mean():+.3f}%/trade  ·  total {nets.sum():+.1f}%")
print(f"  avg win {nets[nets>0].mean():+.2f}%  avg loss {nets[nets<=0].mean():+.2f}%")

# --- MDE on the 1-min era ---
sd = S.intra.std(); N = len(S); z = 1.2816 + 1.6449
print(f"\n[MDE]  per-trade SD {sd*100:.2f}%  ·  N sessions {N}  ·  {N/252:.1f}yr")
for tpy in (12, 25, 50):
    n = tpy * (N/252); print(f"  {tpy}/yr -> N~{n:.0f}  MDE_80 {z*sd/np.sqrt(n)*1e4:5.1f}bps  {'informative' if z*sd/np.sqrt(n)*1e4<=15 else ('marginal' if z*sd/np.sqrt(n)*1e4<=30 else 'UNTESTABLE')}")
print("="*72)
