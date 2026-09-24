# -*- coding: utf-8 -*-
"""Run the PLAYBOOK across the real Nifty-500 (2022-2026) and collect high-conviction signals into a
DAILY BASKET (aggregation of discrete signals, not WR-averaging). Shows the daily P&L of trading the
playbook across all stocks — this is how per-stock rare high-conviction signals become a daily strategy."""
import os, sqlite3, multiprocessing as mp
import numpy as np, pandas as pd
from ndp import playbook as PB
DB = PB.DB


def one(sym):
    try:
        R = PB.run(sym, verbose=False)
        if R is None: return None
        sig = R[(R.direction != 0)][["date", "conviction", "direction", "oc", "ret"]].copy()
        sig["symbol"] = sym
        return sig
    except Exception:
        return None


def main():
    con = sqlite3.connect("file:" + DB.replace("\\", "/") + "?mode=ro", uri=True)
    syms = pd.read_sql_query("SELECT DISTINCT symbol FROM universe_master WHERE in_nifty500=1 AND is_active=1", con).symbol.tolist(); con.close()
    print(f"PLAYBOOK basket across {len(syms)} Nifty-500 names, 2022-2026", flush=True)
    allsig = []
    with mp.Pool(max(2, (os.cpu_count() or 4) - 2)) as pool:
        for i, s in enumerate(pool.imap_unordered(one, syms, chunksize=4)):
            if s is not None and len(s): allsig.append(s)
            if (i+1) % 100 == 0: print(f"  ...{i+1} done", flush=True)
    S = pd.concat(allsig, ignore_index=True)
    print(f"\ntotal trend-matched signals across universe: {len(S):,}")
    print(f"\n{'K>=':<6}{'signals':>9}{'/day':>7}{'WR>=0.5%':>10}{'avg ret':>9}{'total P&L Rs':>15}{'yrs+':>6}")
    for K in (3, 4, 5, 6):
        sig = S[S.conviction >= K].copy()
        if len(sig) < 20: continue
        sig["pnl"] = sig.ret / 100 * 100000                       # Rs1,00,000/signal
        sig["yr"] = sig.date.str[:4]
        day = sig.groupby("date").pnl.sum()
        wr = (sig.ret + PB.COST >= 0.5).mean()*100
        ndays = sig.date.nunique(); span_days = (pd.to_datetime(S.date.max())-pd.to_datetime(S.date.min())).days
        yr = sig.groupby("yr").pnl.sum(); yrs_pos = int((yr > 0).sum())
        print(f"  {K:<4}{len(sig):>9}{len(sig)/max(day.shape[0],1):>7.1f}{wr:>9.1f}%{sig.ret.mean():>+9.3f}{sig.pnl.sum():>+15,.0f}{yrs_pos:>4}/{yr.shape[0]}")
    # detail at K>=5 (deploy level)
    dep = S[S.conviction >= 5].copy(); dep["pnl"] = dep.ret/100*100000; dep["yr"] = dep.date.str[:4]
    print(f"\n===== DEPLOY LEVEL K>=5 (daily basket) =====")
    print(f"  {len(dep):,} signals over 2022-2026  ·  {dep.date.nunique()} trading days with >=1 signal  ·  avg {len(dep)/max(dep.date.nunique(),1):.1f} signals/active-day")
    print(f"  overall WR {(dep.ret>0).mean()*100:.1f}%  ·  avg net/trade {dep.ret.mean():+.3f}%  ·  TOTAL Rs {dep.pnl.sum():,.0f}")
    print("  yearly:"); 
    for y, p in dep.groupby("yr").pnl.sum().items(): print(f"    {y}: Rs {p:>+12,.0f}  ({int((dep.yr==y).sum())} signals)")
    dep.sort_values("date").to_csv(os.path.join(os.path.expanduser("~"),"Downloads","PLAYBOOK_BASKET_K5.csv"), index=False)
    print("\nsaved -> ~/Downloads/PLAYBOOK_BASKET_K5.csv")


if __name__ == "__main__":
    mp.freeze_support(); main()
