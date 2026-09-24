# -*- coding: utf-8 -*-
"""Nifty-50 long/short daily basket — test EVERY multi-horizon signal (1-day, WTD, MTD, weekly, monthly,
gap) as CONTINUATION and REVERSION, plus combined reversion (the bounded-range hypothesis). Enter at open,
exit at close, 1X, net of costs. Leak-free: all signals as-of prior close / open. Reports honest best %/day."""
import os, sqlite3
import numpy as np, pandas as pd
DDB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "db", "kanida_universe.db")
COST = 0.11; N = 10


def main():
    con = sqlite3.connect("file:" + DDB.replace("\\", "/") + "?mode=ro", uri=True)
    syms = pd.read_sql_query("SELECT DISTINCT symbol FROM universe_master WHERE in_nifty50=1 AND is_active=1", con).symbol.tolist()
    d = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE symbol IN (%s) AND trade_date>='2021-06-01' ORDER BY symbol,trade_date" % ",".join("?"*len(syms)), con, params=syms); con.close()
    g = d.groupby("symbol")
    d["pc"] = g.close.shift(1)
    d["isoweek"] = pd.to_datetime(d.trade_date).dt.strftime("%G%V"); d["month"] = d.trade_date.str[:7]
    d["wk_base"] = d.groupby(["symbol", "isoweek"]).close.transform("first")
    d["mo_base"] = d.groupby(["symbol", "month"]).close.transform("first")
    # signals AS-OF PRIOR CLOSE (shift 1) — leak-free for an open entry today
    d["ret1"] = g.close.apply(lambda s: (s.shift(1)/s.shift(2)-1)*100).reset_index(level=0, drop=True)
    d["wtd"] = g.apply(lambda x: ((x.close/x.wk_base-1)*100).shift(1)).reset_index(level=0, drop=True)
    d["mtd"] = g.apply(lambda x: ((x.close/x.mo_base-1)*100).shift(1)).reset_index(level=0, drop=True)
    d["wk5"] = g.close.apply(lambda s: (s.shift(1)/s.shift(6)-1)*100).reset_index(level=0, drop=True)
    d["mo21"] = g.close.apply(lambda s: (s.shift(1)/s.shift(22)-1)*100).reset_index(level=0, drop=True)
    d["gap"] = (d.open/d.pc-1)*100
    d["r_oc"] = (d.close/d.open-1)*100          # intraday capture, enter open exit close
    d = d[d.trade_date >= "2022-01-01"].dropna(subset=["ret1", "wtd", "mtd", "wk5", "mo21", "gap", "r_oc"])
    # cross-sectional z within day
    for f in ["ret1", "wtd", "mtd", "wk5", "mo21", "gap"]:
        d[f+"_z"] = d.groupby("trade_date")[f].transform(lambda x: (x-x.mean())/(x.std()+1e-9))
    d["revscore"] = -(d.wtd_z if False else (d["wtd_z"]+d["mtd_z"]+d["mo21_z"])/3)   # reversion: long the stretched-down

    def basket(col, long_high):
        rows = []
        for dt, gg in d.groupby("trade_date"):
            if len(gg) < 30: continue
            gs = gg.sort_values(col, ascending=not long_high)
            L = gs.head(N).r_oc.mean(); S = gs.tail(N).r_oc.mean()
            rows.append(L - S - COST)
        r = np.array(rows); return r.mean(), (r > 0).mean()*100, r.sum(), len(r)

    print(f"NIFTY-50 LONG/SHORT DAILY BASKET — multi-horizon signals  ·  1X  ·  enter open/exit close  ·  cost {COST}%\n")
    print(f"  {'signal':<34}{'net%/day':>10}{'hit%':>7}{'total%':>9}")
    tests = [("1-day (continue)", "ret1_z", True), ("1-day (revert)", "ret1_z", False),
             ("WTD (continue)", "wtd_z", True), ("WTD (revert=bounded)", "wtd_z", False),
             ("MTD (continue)", "mtd_z", True), ("MTD (revert=bounded)", "mtd_z", False),
             ("weekly 5d (continue)", "wk5_z", True), ("weekly 5d (revert)", "wk5_z", False),
             ("monthly 21d (continue)", "mo21_z", True), ("monthly 21d (revert)", "mo21_z", False),
             ("gap (continue)", "gap_z", True), ("gap (fade)", "gap_z", False),
             ("COMBINED reversion (wtd+mtd+mo)", "revscore", True)]
    best = None
    for nm, col, lh in tests:
        avg, hit, tot, n = basket(col, lh)
        print(f"  {nm:<34}{avg:>+10.3f}{hit:>6.0f}%{tot:>+9.0f}")
        if best is None or avg > best[1]: best = (nm, avg, hit, tot)
    print(f"\n  BEST: {best[0]}  ->  {best[1]:+.3f}%/day  ·  hit {best[2]:.0f}%  ·  total {best[3]:+.0f}% over {d.trade_date.nunique()} days")
    print(f"  target +0.8-1.0%/day.  best achieved: {best[1]:+.3f}%/day")


if __name__ == "__main__":
    main()
