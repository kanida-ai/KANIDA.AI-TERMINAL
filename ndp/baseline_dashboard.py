# -*- coding: utf-8 -*-
"""Baseline behavior dashboard for one stock (2022-2026). The stock's NATURAL intraday tendencies that
any mined pattern must beat. All same-day, from daily OHLC: Directional Bias, Target Hit Probability
(open->high / open->low), Average Intraday Range, Move Retention (momentum vs mean-reversion)."""
import os, sqlite3, sys
import numpy as np, pandas as pd
DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "db", "kanida_universe.db")


def dash(o, h, l, c):
    n = len(o)
    up = c > o; dn = c < o
    oh = (h/o-1)*100; ol = (l/o-1)*100; rng = (h-l)/o*100
    hu05 = oh >= 0.5; hu10 = oh >= 1.0; hd05 = ol <= -0.5; hd10 = ol <= -1.0
    def ret(mask, cond): return (cond[mask].mean()*100) if mask.sum() else np.nan
    return dict(
        days=n, up=up.mean()*100, dn=dn.mean()*100,
        t_up05=hu05.mean()*100, t_up10=hu10.mean()*100, t_dn05=hd05.mean()*100, t_dn10=hd10.mean()*100,
        rng=rng.mean(),
        r_up05=ret(hu05, up), r_up10=ret(hu10, up), r_dn05=ret(hd05, dn), r_dn10=ret(hd10, dn))


def run(sym):
    con = sqlite3.connect("file:" + DB.replace("\\", "/") + "?mode=ro", uri=True)
    g = pd.read_sql_query("SELECT trade_date,open,high,low,close FROM ohlc_daily WHERE symbol=? "
                          "AND trade_date>='2022-01-01' AND trade_date<='2026-12-31' ORDER BY trade_date", con, params=[sym]); con.close()
    o, h, l, c = (g[x].values.astype(float) for x in ("open", "high", "low", "close"))
    D = dash(o, h, l, c)
    print("="*70); print(f"BASELINE BEHAVIOR DASHBOARD — {sym}   (2022-2026, same-day)"); print("="*70)
    print(f"\n  Total trading days      : {D['days']:,}")
    print(f"  Directional bias        : {D['up']:.0f}% UP (close>open)  /  {D['dn']:.0f}% DOWN")
    print(f"  Average intraday range  : {D['rng']:.2f}%   ((High-Low)/Open)")
    print(f"\n  TARGET HIT PROBABILITY (the baseline your pattern must beat):")
    print(f"    Open->High >= +0.5% : {D['t_up05']:.0f}%      Open->High >= +1.0% : {D['t_up10']:.0f}%")
    print(f"    Open->Low  <= -0.5% : {D['t_dn05']:.0f}%      Open->Low  <= -1.0% : {D['t_dn10']:.0f}%")
    print(f"\n  MOVE RETENTION (momentum vs mean-reversion):")
    print(f"    of days that touched +0.5% -> {D['r_up05']:.0f}% closed UP    | touched +1.0% -> {D['r_up10']:.0f}% closed UP")
    print(f"    of days that touched -0.5% -> {D['r_dn05']:.0f}% closed DOWN  | touched -1.0% -> {D['r_dn10']:.0f}% closed DOWN")
    verdict = "MOMENTUM" if (D['r_up10'] > 55 and D['r_dn10'] > 55) else ("MEAN-REVERSION" if (D['r_up10'] < 50 or D['r_dn10'] < 50) else "MIXED")
    print(f"    -> {sym} is a {verdict} stock intraday")
    # yearly
    g["yr"] = g.trade_date.str[:4]; rows = []
    for yr, gg in g.groupby("yr"):
        d = dash(gg.open.values, gg.high.values, gg.low.values, gg.close.values)
        rows.append(dict(year=yr, days=d["days"], up_pct=round(d["up"]), rng=round(d["rng"], 2),
                         hit_up05=round(d["t_up05"]), hit_up10=round(d["t_up10"]), hit_dn05=round(d["t_dn05"]), hit_dn10=round(d["t_dn10"]),
                         ret_up10=round(d["r_up10"]), ret_dn10=round(d["r_dn10"])))
    Y = pd.DataFrame(rows)
    print("\n  YEAR-BY-YEAR:"); print(Y.to_string(index=False))
    out = os.path.expanduser("~") + f"/Downloads/BASELINE_{sym}.xlsx"
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        pd.DataFrame([D]).to_excel(w, "overall", index=False); Y.to_excel(w, "yearly", index=False)
    print(f"\nsaved -> {out}")


if __name__ == "__main__":
    run(sys.argv[1].upper() if len(sys.argv) > 1 else "ICICIBANK")
