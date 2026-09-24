# -*- coding: utf-8 -*-
"""ICICI opening-range breakout bracket, simulated on 1-minute data. At the open place a buy-stop at
+0.5% and a sell-stop at -0.5%. First level touched -> enter that direction (OCO, other cancels), then a
PERCENT TRAILING STOP rides it; square off at 15:29. Rs 1,00,000 notional, MIS intraday, net of cost.
Assumptions: touch = fill at the level; cost 0.06% round-trip; trailing stop fills at the stop level."""
import os, sqlite3, sys
import numpy as np, pandas as pd
MDB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "universe_engine", "data", "db", "kanida_universe.db")
CAP = 100000; COST = 0.06; BRK = 0.005


def simulate(sym, trail):
    con = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
    b = pd.read_sql_query("SELECT bar_time,open,high,low,close FROM ohlc_1min WHERE symbol=? ORDER BY bar_time",
                          con, params=[sym]); con.close()
    b["date"] = b.bar_time.str[:10]; b["hm"] = b.bar_time.str[11:16]
    b = b[(b.hm >= "09:15") & (b.hm <= "15:29")]
    trades = []
    for d, g in b.groupby("date"):
        g = g.sort_values("bar_time").reset_index(drop=True)
        if len(g) < 60: continue
        o = g.open.iloc[0]; buy_lvl = o*(1+BRK); sell_lvl = o*(1-BRK)
        H = g.high.values; L = g.low.values; C = g.close.values
        # find first trigger
        ei = -1; direction = 0; entry = None
        for i in range(len(g)):
            hb = H[i] >= buy_lvl; hs = L[i] <= sell_lvl
            if hb and hs:                                   # same bar both -> follow bar close
                direction = 1 if C[i] >= o else -1; entry = buy_lvl if direction == 1 else sell_lvl; ei = i; break
            if hb: direction = 1; entry = buy_lvl; ei = i; break
            if hs: direction = -1; entry = sell_lvl; ei = i; break
        if ei == -1:
            trades.append(dict(date=d, dir=0, ret=0.0, outcome="no_break")); continue
        # trail from entry bar onward
        exitp = None; outc = "eod"
        if direction == 1:
            peak = entry
            for j in range(ei, len(g)):
                peak = max(peak, H[j]); stop = peak*(1-trail)
                if j > ei and L[j] <= stop: exitp = stop; outc = "trail"; break
            if exitp is None: exitp = C[-1]
        else:
            trough = entry
            for j in range(ei, len(g)):
                trough = min(trough, L[j]); stop = trough*(1+trail)
                if j > ei and H[j] >= stop: exitp = stop; outc = "trail"; break
            if exitp is None: exitp = C[-1]
        ret = direction*(exitp-entry)/entry*100 - COST
        trades.append(dict(date=d, dir=direction, ret=round(ret, 3), outcome=outc))
    T = pd.DataFrame(trades); T["yr"] = T.date.str[:4]; T["pnl"] = T.ret/100*CAP
    return T


def main(sym):
    print("="*80); print(f"OPENING-RANGE BREAKOUT BRACKET — {sym}  (1-min, {'entry ±0.5% from open, trail + EOD'})"); print("="*80)
    for trail in (0.003, 0.004, 0.005, 0.0075, 0.010):
        T = simulate(sym, trail); tr = T[T.dir != 0]
        wr = (tr.ret > 0).mean()*100; tot = tr.pnl.sum()
        print(f"\n  TRAIL {trail*100:.2f}%  ·  trades {len(tr)}/{len(T)} ({len(tr)/len(T)*100:.0f}% of days broke ±0.5%)  ·  WR {wr:.1f}%  ·  avg/trade {tr.ret.mean():+.3f}% (Rs {tr.pnl.mean():+,.0f})")
        print(f"     TOTAL P&L Rs {tot:+,.0f}   ·   long {(tr.dir==1).sum()} (WR {(tr[tr.dir==1].ret>0).mean()*100:.0f}%, Rs{tr[tr.dir==1].pnl.sum():+,.0f})  ·  short {(tr.dir==-1).sum()} (WR {(tr[tr.dir==-1].ret>0).mean()*100:.0f}%, Rs{tr[tr.dir==-1].pnl.sum():+,.0f})")
        yr = tr.groupby("yr").pnl.sum(); print("     yearly: " + "  ".join(f"{y} Rs{p:+,.0f}" for y, p in yr.items()))
    # detail at trail 0.4% (the continuation-implied breathing room)
    T = simulate(sym, 0.004); tr = T[T.dir != 0]
    print("\n" + "-"*80); print("DETAIL @ trail 0.40% (ICICI's ~0.34% natural pullback + margin):")
    print(f"  {len(tr)} trades  ·  WR {(tr.ret>0).mean()*100:.1f}%  ·  avg win {tr[tr.ret>0].ret.mean():+.2f}%  avg loss {tr[tr.ret<=0].ret.mean():+.2f}%")
    print(f"  exits: trail-stop {int((tr.outcome=='trail').sum())}  ·  EOD {int((tr.outcome=='eod').sum())}  ·  no-break days {int((T.dir==0).sum())}")
    print(f"  TOTAL Rs {tr.pnl.sum():+,.0f} over {T.date.nunique()} sessions ({T.date.min()}->{T.date.max()})")
    T.to_csv(os.path.expanduser("~")+f"/Downloads/SIM_BREAKOUT_{sym}.csv", index=False)
    print(f"\nsaved trade log -> ~/Downloads/SIM_BREAKOUT_{sym}.csv")


if __name__ == "__main__":
    main(sys.argv[1].upper() if len(sys.argv) > 1 else "ICICIBANK")
