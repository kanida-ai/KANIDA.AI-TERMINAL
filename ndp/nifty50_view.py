# -*- coding: utf-8 -*-
"""Baseline behavior dashboard + continuation curve for ALL Nifty-50 stocks, in one clean consolidated
view (same metrics as the ICICI view). Daily 2022-2026. Two tables: (1) BEHAVIOR, (2) CONTINUATION."""
import os, sqlite3
import numpy as np, pandas as pd
DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "db", "kanida_universe.db")


def profile(sym):
    con = sqlite3.connect("file:" + DB.replace("\\", "/") + "?mode=ro", uri=True)
    g = pd.read_sql_query("SELECT open,high,low,close FROM ohlc_daily WHERE symbol=? AND trade_date>='2022-01-01' "
                          "AND trade_date<='2026-12-31' ORDER BY trade_date", con, params=[sym]); con.close()
    if len(g) < 250: return None
    o, h, l, c = g.open.values, g.high.values, g.low.values, g.close.values
    oh = (h/o-1)*100; ol = (l/o-1)*100
    up05, up1 = oh >= 0.5, oh >= 1.0; dn05, dn1 = ol <= -0.5, ol <= -1.0
    def cu(m): return round((oh >= m).sum()/max(up05.sum(), 1)*100)
    def cd(m): return round((ol <= m).sum()/max(dn05.sum(), 1)*100)
    return dict(symbol=sym, days=len(g),
                up_days=round((c > o).mean()*100), dn_days=round((c < o).mean()*100), range=round(((h-l)/o*100).mean(), 2),
                hit_up05=round(up05.mean()*100), hit_up1=round(up1.mean()*100),
                hit_dn05=round(dn05.mean()*100), hit_dn1=round(dn1.mean()*100),
                reten_up=round((c[up1] > o[up1]).mean()*100) if up1.sum() else 0,
                reten_dn=round((c[dn1] < o[dn1]).mean()*100) if dn1.sum() else 0,
                u07=cu(0.7), u10=cu(1.0), u15=cu(1.5), d07=cd(-0.7), d10=cd(-1.0), d15=cd(-1.5))


def main():
    con = sqlite3.connect("file:" + DB.replace("\\", "/") + "?mode=ro", uri=True)
    syms = pd.read_sql_query("SELECT DISTINCT symbol FROM universe_master WHERE in_nifty50=1 AND is_active=1 ORDER BY symbol", con).symbol.tolist(); con.close()
    SC = pd.DataFrame([r for s in syms if (r := profile(s))])
    SC["mom"] = ((SC.u10+SC.d10)/2*0.5 + (SC.reten_up+SC.reten_dn)/2*0.5).round(1)
    SC = SC.sort_values("mom", ascending=False).reset_index(drop=True)
    pd.set_option("display.width", 220, "display.max_rows", 60)
    T1 = SC[["symbol", "days", "up_days", "dn_days", "range", "hit_up05", "hit_up1", "hit_dn05", "hit_dn1", "reten_up", "reten_dn"]]
    T1.columns = ["Stock", "Days", "Up%", "Dn%", "Range%", "Hit+.5", "Hit+1", "Hit-.5", "Hit-1", "RetUp", "RetDn"]
    T2 = SC[["symbol", "u07", "u10", "u15", "d07", "d10", "d15", "mom"]]
    T2.columns = ["Stock", "+.5>+.7", "+.5>+1", "+.5>+1.5", "-.5>-.7", "-.5>-1", "-.5>-1.5", "MomScore"]
    print("="*104); print("TABLE 1 — BASELINE BEHAVIOR DASHBOARD  (Nifty-50, daily 2022-2026, sorted by momentum fitness)"); print("="*104)
    print(T1.to_string(index=False))
    print("\n" + "="*104); print("TABLE 2 — CONTINUATION CURVE  (once it reaches ±0.5% from open, % that extend further)"); print("="*104)
    print(T2.to_string(index=False))
    with pd.ExcelWriter(os.path.expanduser("~")+"/Downloads/NIFTY50_FULL_VIEW.xlsx", engine="openpyxl") as w:
        T1.to_excel(w, "behavior", index=False); T2.to_excel(w, "continuation", index=False); SC.to_excel(w, "raw", index=False)
    print("\nsaved -> ~/Downloads/NIFTY50_FULL_VIEW.xlsx")


if __name__ == "__main__":
    main()
