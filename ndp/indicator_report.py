# -*- coding: utf-8 -*-
"""Per-indicator report for one stock (2022-2026): every indicator's params, daily buy/sell firing, and
next-day open->close performance (buy WR/avg, sell WR/avg). No shortcuts — all 120+ shown individually."""
import os, sqlite3, sys
import numpy as np, pandas as pd
from ndp import indicator_library as IL
DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "db", "kanida_universe.db")
START, END = "2022-01-01", "2026-12-31"


def run(sym):
    con = sqlite3.connect("file:" + DB.replace("\\", "/") + "?mode=ro", uri=True)
    g = pd.read_sql_query("SELECT trade_date,open,high,low,close,volume FROM ohlc_daily WHERE symbol=? "
                          "AND trade_date>='2020-06-01' ORDER BY trade_date", con, params=[sym]); con.close()
    D = dict(o=g.open.values.astype(float), h=g.high.values.astype(float), l=g.low.values.astype(float),
             c=g.close.values.astype(float), v=g.volume.values.astype(float))
    o1 = np.roll(D["o"], -1); c1 = np.roll(D["c"], -1); oc = (c1-o1)/o1*100.0; oc[-1] = np.nan
    dates = g.trade_date.values; inwin = (dates >= START) & (dates <= END) & ~np.isnan(oc)
    inds = IL.build_all(D)
    print(f"{sym}: {len(inds)} indicators  ·  {int(inwin.sum())} trading days 2022-2026")
    base_up = (oc[inwin] >= 0.5).mean()*100; base_dn = (oc[inwin] <= -0.5).mean()*100
    print(f"base rate next-day open->close: >=+0.5% {base_up:.1f}%   <=-0.5% {base_dn:.1f}%\n")
    rows = []; firing = {"date": dates[inwin], "next_oc": np.round(oc[inwin], 3)}
    for name, params, sig in inds:
        s = sig[inwin]; ocw = oc[inwin]
        bmask = s == 1; smask = s == -1
        bn, sn = int(bmask.sum()), int(smask.sum())
        rows.append(dict(indicator=name, params=params,
                         buy_n=bn, buy_WR=round((ocw[bmask] >= 0.5).mean()*100, 1) if bn else np.nan, buy_avg=round(ocw[bmask].mean(), 3) if bn else np.nan,
                         sell_n=sn, sell_WR=round((ocw[smask] <= -0.5).mean()*100, 1) if sn else np.nan, sell_avg=round(-ocw[smask].mean(), 3) if sn else np.nan))
        firing[f"{name}|{params}"] = s.astype(int)
    SC = pd.DataFrame(rows)
    SC["buy_edge"] = (SC.buy_WR - base_up).round(1); SC["sell_edge"] = (SC.sell_WR - base_dn).round(1)
    dl = os.path.expanduser("~") + "/Downloads"
    with pd.ExcelWriter(f"{dl}/INDICATOR_SCORECARD_{sym}.xlsx", engine="openpyxl") as w:
        SC.sort_values("buy_avg", ascending=False).to_excel(w, "by_buy_edge", index=False)
        SC.sort_values("sell_avg", ascending=False).to_excel(w, "by_sell_edge", index=False)
    pd.DataFrame(firing).to_csv(f"{dl}/DAILY_FIRING_{sym}.csv", index=False)
    # console: top buy & sell indicators
    print("===== TOP 15 BUY indicators (by avg next-day return, buy_n>=15) =====")
    tb = SC[SC.buy_n >= 15].sort_values("buy_avg", ascending=False).head(15)
    print(f"  {'indicator':<22}{'params':<16}{'n':>5}{'WR':>7}{'avg':>7}{'edge':>7}")
    for _, r in tb.iterrows(): print(f"  {r.indicator:<22}{r.params:<16}{r.buy_n:>5}{r.buy_WR:>6.1f}%{r.buy_avg:>+7.2f}{r.buy_edge:>+7.1f}")
    print("\n===== TOP 15 SELL indicators (by avg next-day decline, sell_n>=15) =====")
    ts = SC[SC.sell_n >= 15].sort_values("sell_avg", ascending=False).head(15)
    print(f"  {'indicator':<22}{'params':<16}{'n':>5}{'WR':>7}{'avg':>7}{'edge':>7}")
    for _, r in ts.iterrows(): print(f"  {r.indicator:<22}{r.params:<16}{r.sell_n:>5}{r.sell_WR:>6.1f}%{r.sell_avg:>+7.2f}{r.sell_edge:>+7.1f}")
    print(f"\nsaved -> {dl}/INDICATOR_SCORECARD_{sym}.xlsx  +  DAILY_FIRING_{sym}.csv ({len(inds)} indicator columns)")
    return SC


if __name__ == "__main__":
    run(sys.argv[1].upper() if len(sys.argv) > 1 else "ICICIBANK")
