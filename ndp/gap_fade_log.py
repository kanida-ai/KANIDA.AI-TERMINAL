# -*- coding: utf-8 -*-
"""Gap-fade daily long/short basket — FULL 2026 trade log. Each morning: rank Nifty-50 by overnight gap
(open/prev_close). LONG the 5 biggest gap-DOWNS, SHORT the 5 biggest gap-UPS. Enter open, exit close, 1X.
Rs1,00,000 per stock. Net of Rs120/stock (0.12%) all-in charges. Day-over-day + month-over-month P&L."""
import os, sqlite3
import numpy as np, pandas as pd
DDB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "db", "kanida_universe.db")
CAP = 100000; COSTP = 0.12; N = 5


def main():
    con = sqlite3.connect("file:" + DDB.replace("\\", "/") + "?mode=ro", uri=True)
    syms = pd.read_sql_query("SELECT DISTINCT symbol FROM universe_master WHERE in_nifty50=1 AND is_active=1", con).symbol.tolist()
    d = pd.read_sql_query("SELECT symbol,trade_date,open,close FROM ohlc_daily WHERE symbol IN (%s) AND trade_date>='2025-12-01' ORDER BY symbol,trade_date" % ",".join("?"*len(syms)), con, params=syms); con.close()
    d["pc"] = d.groupby("symbol").close.shift(1)
    d = d[d.trade_date >= "2026-01-01"].dropna(subset=["pc", "open", "close"])
    d["gap"] = (d.open/d.pc-1)*100; d["oc"] = (d.close/d.open-1)*100
    cost = COSTP/100*CAP
    trades = []
    for dt, g in d.groupby("trade_date"):
        if len(g) < 30: continue
        gs = g.sort_values("gap")
        longs = gs.head(N); shorts = gs.tail(N)
        for _, r in longs.iterrows():
            pnl = CAP*r.oc/100 - cost
            trades.append(dict(date=dt, symbol=r.symbol, side="LONG", gap=round(r.gap, 2), open=round(r.open, 1), close=round(r.close, 1),
                               move=round(r.oc, 3), pnl=round(pnl)))
        for _, r in shorts.iterrows():
            pnl = CAP*(-r.oc)/100 - cost
            trades.append(dict(date=dt, symbol=r.symbol, side="SHORT", gap=round(r.gap, 2), open=round(r.open, 1), close=round(r.close, 1),
                               move=round(-r.oc, 3), pnl=round(pnl)))
    T = pd.DataFrame(trades); T["month"] = T.date.str[:7]
    out = os.path.expanduser("~")+"/Downloads/GAPFADE_2026_TRADELOG.csv"; T.to_csv(out, index=False)
    cap_day = 2*N*CAP
    # day P&L
    day = T.groupby("date").pnl.sum().reset_index(); day["ret%"] = day.pnl/cap_day*100; day["month"] = day.date.str[:7]
    print(f"GAP-FADE BASKET — FULL 2026  ·  long {N} gap-downs / short {N} gap-ups  ·  Rs{CAP:,}/stock  ·  Rs{2*N*CAP:,}/day deployed  ·  cost Rs{int(cost)}/stock")
    print(f"\n===== MONTH OVER MONTH =====")
    print(f"  {'month':<9}{'days':>6}{'net P&L Rs':>13}{'avg/day Rs':>12}{'win days':>10}{'ret on capital':>16}")
    for m, gg in day.groupby("month"):
        wd = (gg.pnl > 0).mean()*100
        print(f"  {m:<9}{len(gg):>6}{gg.pnl.sum():>+13,.0f}{gg.pnl.mean():>+12,.0f}{wd:>9.0f}%{gg.pnl.sum()/cap_day*100:>+15.2f}%")
    print(f"  {'-'*66}")
    print(f"  {'TOTAL':<9}{len(day):>6}{day.pnl.sum():>+13,.0f}{day.pnl.mean():>+12,.0f}{(day.pnl>0).mean()*100:>9.0f}%{day.pnl.sum()/cap_day*100:>+15.2f}%")
    print(f"\n===== DAY OVER DAY (P&L per trading day) =====")
    print(f"  {'date':<12}{'net P&L Rs':>13}{'ret%':>8}   top winners / losers")
    for _, r in day.iterrows():
        dts = T[T.date == r.date].sort_values("pnl", ascending=False)
        tw = dts.iloc[0]; tl = dts.iloc[-1]
        print(f"  {r.date:<12}{r.pnl:>+13,.0f}{r['ret%']:>+7.2f}%   +{tw.symbol}({tw.side} {tw.pnl:+,.0f}) / {tl.symbol}({tl.side} {tl.pnl:+,.0f})")
    peak = day.pnl.cumsum().cummax(); ddn = (peak - day.pnl.cumsum()).max()
    print(f"\n  Total net P&L 2026: Rs {day.pnl.sum():+,.0f}   ·   avg {day.pnl.mean():+,.0f}/day ({day.pnl.mean()/cap_day*100:+.3f}%/day)   ·   max drawdown Rs {ddn:,.0f}")
    print(f"  saved full trade log ({len(T)} trades) -> {out}")


if __name__ == "__main__":
    main()
