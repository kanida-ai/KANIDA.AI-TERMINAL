# -*- coding: utf-8 -*-
"""Nifty-50 daily LONG/SHORT basket, 1X, intraday (enter morning, exit close, no overnight). Fixed 50-stock
universe. Each day: rank stocks by a signal, LONG top-10, SHORT bottom-10, equal weight. Net daily basket
return = mean(long fwd) + mean(short fwd as -move) - costs. Traced from 1-min per stock, full 1-min era.
Tests several ranking signals and reports the honest net %/day. No shortcuts."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
DDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
COST = 0.11              # per-stock intraday round-trip %, post charges+taxes (STT/brokerage/exch/GST/stamp)
N = 10                   # basket size each side


def per_day(sym):
    con = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
    b = pd.read_sql_query("SELECT bar_time,open,close FROM ohlc_1min WHERE symbol=? ORDER BY bar_time", con, params=[sym]); con.close()
    if b.empty: return None
    b["date"] = b.bar_time.str[:10]; b["hm"] = b.bar_time.str[11:16]; b = b[(b.hm >= "09:15") & (b.hm <= "15:29")]
    g = b.groupby("date")
    d = pd.DataFrame({"open": g.open.first(), "close": g.close.last()})
    p0930 = b[b.hm == "09:30"].groupby("date").close.last(); d["px0930"] = p0930
    d = d.reset_index(); d["symbol"] = sym
    return d


def main():
    con = sqlite3.connect("file:" + DDB.replace("\\", "/") + "?mode=ro", uri=True)
    syms = pd.read_sql_query("SELECT DISTINCT symbol FROM universe_master WHERE in_nifty50=1 AND is_active=1", con).symbol.tolist()
    dd = pd.read_sql_query("SELECT symbol,trade_date,close FROM ohlc_daily WHERE symbol IN (%s) ORDER BY symbol,trade_date" % ",".join("?"*len(syms)), con, params=syms); con.close()
    dd["pc"] = dd.groupby("symbol").close.shift(1); pcmap = dd.set_index(["symbol", "trade_date"]).pc.to_dict()
    frames = [x for s in syms if (x := per_day(s)) is not None]
    A = pd.concat(frames, ignore_index=True).dropna(subset=["open", "close", "px0930"])
    A["pc"] = A.apply(lambda r: pcmap.get((r.symbol, r.date), np.nan), axis=1)
    A = A.dropna(subset=["pc"])
    A["prev_ret"] = (A.close / A.pc - 1) * 100 * 0 + (A.pc.pipe(lambda x: 0))  # placeholder removed below
    # signals (computed morning, leak-free)
    A["prev_ret"] = (A.pc / A.groupby("symbol").pc.shift(0) - 1)  # dummy; recompute properly
    A = A.sort_values(["symbol", "date"])
    A["prev_ret"] = A.groupby("symbol").apply(lambda g: (g.pc / g.close.shift(1) - 1) * 100).reset_index(level=0, drop=True)
    A["gap"] = (A.open / A.pc - 1) * 100
    A["drive"] = (A.px0930 / A.open - 1) * 100
    A["r_oc"] = (A.close / A.open - 1) * 100          # open->close (entry at open)
    A["r_930c"] = (A.close / A.px0930 - 1) * 100       # 0930->close (entry at 0930)

    def run_signal(name, col, ret_col, long_high=True):
        rows = []
        for dt, g in A.groupby("date"):
            g = g.dropna(subset=[col, ret_col])
            if len(g) < 30: continue
            gs = g.sort_values(col, ascending=not long_high)
            longs = gs.head(N); shorts = gs.tail(N)
            L = longs[ret_col].mean(); S = -shorts[ret_col].mean()      # short profit = -move
            net = (L + S) - COST                                        # full spread, 1X each side, net
            rows.append(dict(date=dt, L=L, S=S, spread=L+S, net=net))
        R = pd.DataFrame(rows)
        if R.empty: return None
        return dict(name=name, days=len(R), avg=R.net.mean(), hit=(R.net > 0).mean()*100,
                    med=R.net.median(), best=R.net.max(), worst=R.net.min(), total=R.net.sum(), R=R)

    print(f"NIFTY-50 LONG/SHORT DAILY BASKET  ·  1X  ·  top/bottom {N}  ·  cost {COST}%/stock  ·  {A.date.nunique()} days (1-min era)\n")
    print(f"  {'signal':<34}{'days':>6}{'net%/day':>10}{'hit%':>7}{'median':>9}{'total%':>9}")
    tests = [
        ("prev-day momentum (cont), enter OPEN", "prev_ret", "r_oc", True),
        ("prev-day momentum (revert), enter OPEN", "prev_ret", "r_oc", False),
        ("gap (continue), enter OPEN", "gap", "r_oc", True),
        ("gap (fade), enter OPEN", "gap", "r_oc", False),
        ("opening drive (cont), enter 09:30", "drive", "r_930c", True),
        ("opening drive (revert), enter 09:30", "drive", "r_930c", False),
    ]
    best = None
    for nm, col, rc, lh in tests:
        r = run_signal(nm, col, rc, lh)
        if r is None: continue
        print(f"  {nm:<34}{r['days']:>6}{r['avg']:>+10.3f}{r['hit']:>6.0f}%{r['med']:>+9.3f}{r['total']:>+9.0f}")
        if best is None or r['avg'] > best['avg']: best = r
    print(f"\n  BEST: {best['name']}  ->  net {best['avg']:+.3f}%/day  ·  hit {best['hit']:.0f}%  ·  best day {best['best']:+.2f}%  worst {best['worst']:+.2f}%")
    print(f"  target was +0.8 to +1.0%/day.  achieved: {best['avg']:+.3f}%/day")
    best['R'].to_csv(os.path.expanduser("~")+"/Downloads/BASKET_DAILY_PNL.csv", index=False)
    print("\n  saved daily P&L -> ~/Downloads/BASKET_DAILY_PNL.csv")


if __name__ == "__main__":
    main()
