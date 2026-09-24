# -*- coding: utf-8 -*-
"""Baseline behavior + continuation curve across the Nifty 50, ranked as candidates for the momentum-
continuation setup (enter break with prior-day momentum, target ±1%). Best candidates = high continuation
(+0.5%->+1.0%) + high retention (touches ±1% -> closes same way) + enough intraday range. Daily 2022-2026."""
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
    up1 = oh >= 1.0; dn1 = ol <= -1.0
    ret_up = (c[up1] > o[up1]).mean()*100 if up1.sum() else np.nan       # touched +1% -> closed up
    ret_dn = (c[dn1] < o[dn1]).mean()*100 if dn1.sum() else np.nan
    up05 = oh >= 0.5; dn05 = ol <= -0.5
    cont_up10 = (oh >= 1.0).sum()/max(up05.sum(), 1)*100                 # P(+1.0% | +0.5%)
    cont_up07 = (oh >= 0.7).sum()/max(up05.sum(), 1)*100
    cont_dn10 = (ol <= -1.0).sum()/max(dn05.sum(), 1)*100
    cont_dn07 = (ol <= -0.7).sum()/max(dn05.sum(), 1)*100
    return dict(symbol=sym, days=len(g), dir_up=round((c > o).mean()*100),
                range=round(((h-l)/o*100).mean(), 2),
                hit_p05=round(up05.mean()*100), hit_p1=round(up1.mean()*100),
                hit_m05=round(dn05.mean()*100), hit_m1=round(dn1.mean()*100),
                reten_up=round(ret_up), reten_dn=round(ret_dn),
                cont_up_07=round(cont_up07), cont_up_10=round(cont_up10),
                cont_dn_07=round(cont_dn07), cont_dn_10=round(cont_dn10))


def main():
    con = sqlite3.connect("file:" + DB.replace("\\", "/") + "?mode=ro", uri=True)
    syms = pd.read_sql_query("SELECT DISTINCT symbol FROM universe_master WHERE in_nifty50=1 AND is_active=1", con).symbol.tolist(); con.close()
    rows = [r for s in syms if (r := profile(s))]
    SC = pd.DataFrame(rows)
    # momentum-continuation fitness score: continuation + retention, gated on adequate range
    SC["mom_score"] = ((SC.cont_up_10 + SC.cont_dn_10)/2*0.5 + (SC.reten_up + SC.reten_dn)/2*0.5).round(1)
    SC["range_ok"] = SC.range >= 1.5
    SC = SC.sort_values("mom_score", ascending=False).reset_index(drop=True)
    pd.set_option("display.width", 200, "display.max_columns", 30, "display.max_rows", 60)
    print(f"NIFTY-50 BEHAVIOR + CONTINUATION (2022-2026)  ·  {len(SC)} stocks  ·  ranked by momentum-continuation fitness\n")
    cols = ["symbol", "range", "hit_p05", "hit_m05", "reten_up", "reten_dn", "cont_up_10", "cont_dn_10", "mom_score", "range_ok"]
    print(SC[cols].to_string(index=False))
    print(f"\nICICI reference: retention ~86/87%, cont_up_10 ~53%, range ~1.7%")
    print("\n===== TOP NEXT CANDIDATES (high continuation + retention + range>=1.5%) =====")
    top = SC[SC.range_ok].head(12)
    for _, r in top.iterrows():
        print(f"  {r.symbol:<12} mom {r.mom_score:>5}  ·  retention {r.reten_up:.0f}/{r.reten_dn:.0f}%  ·  +0.5->+1.0 {r.cont_up_10:.0f}%  -0.5->-1.0 {r.cont_dn_10:.0f}%  ·  range {r.range:.1f}%")
    SC.to_excel(os.path.expanduser("~")+"/Downloads/NIFTY50_CANDIDATES.xlsx", index=False)
    print("\nsaved -> ~/Downloads/NIFTY50_CANDIDATES.xlsx")


if __name__ == "__main__":
    main()
