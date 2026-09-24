# -*- coding: utf-8 -*-
"""ODDS ENGINE v1 — shift the 50:50 with evidence. Each morning score every Nifty-50 stock's P(up from
09:30 to close) from 1-min opening behaviour + prior-day context, LEAK-FREE weekly walk-forward learning.
Long the top-10 probabilities, short the bottom-10. MEASURE the achieved odds (long up-rate, short down-rate)
and the net %/day at 1X after costs. This is the honest baseline we then push higher, refinement by refinement."""
import os, sqlite3, warnings
import numpy as np, pandas as pd
from sklearn.linear_model import LogisticRegression
warnings.filterwarnings("ignore")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
DDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
COST = 0.11; N = 10; MIN_TRAIN_WK = 12
FEATS = ["gap", "drive", "or_pos", "or_range", "vol_z", "prev_ret", "prev_rsi", "trend", "prev_range"]


def _rsi(x, n=14):
    d = np.diff(x, prepend=x[0]); u = pd.Series(np.clip(d, 0, None)).ewm(alpha=1/n, adjust=False).mean().values
    dn = pd.Series(-np.clip(d, None, 0)).ewm(alpha=1/n, adjust=False).mean().values
    return 100-100/(1+u/(dn+1e-9))


def stock_panel(sym, dctx):
    con = sqlite3.connect("file:" + MDB.replace("\\", "/") + "?mode=ro", uri=True)
    b = pd.read_sql_query("SELECT bar_time,open,high,low,close,volume FROM ohlc_1min WHERE symbol=? ORDER BY bar_time", con, params=[sym]); con.close()
    if b.empty: return None
    b["date"] = b.bar_time.str[:10]; b["hm"] = b.bar_time.str[11:16]; b = b[(b.hm >= "09:15") & (b.hm <= "15:29")]
    rows = []
    for d, g in b.groupby("date"):
        if d not in dctx.index: continue
        o = g.open.iloc[0]; op = g[g.hm <= "09:30"]
        if len(op) < 10: continue
        px = op.close.iloc[-1]; hi = op.high.max(); lo = op.low.min(); vol15 = op.volume.sum()
        cl = g.close.iloc[-1]; c = dctx.loc[d]
        rows.append(dict(symbol=sym, date=d, px0930=px, close=cl,
                         gap=(o/c.pc-1)*100 if c.pc == c.pc else 0, drive=(px/o-1)*100,
                         or_range=(hi-lo)/o*100, or_pos=(px-lo)/(hi-lo+1e-9), vol15=vol15,
                         prev_ret=c.prev_ret, prev_rsi=c.prsi, trend=int(c.sma50 > c.sma200) if c.sma200 == c.sma200 else 0,
                         prev_range=c.prange, y=int(cl > px)))
    return pd.DataFrame(rows)


def main():
    con = sqlite3.connect("file:" + DDB.replace("\\", "/") + "?mode=ro", uri=True)
    syms = pd.read_sql_query("SELECT DISTINCT symbol FROM universe_master WHERE in_nifty50=1 AND is_active=1", con).symbol.tolist()
    dd = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE symbol IN (%s) ORDER BY symbol,trade_date" % ",".join("?"*len(syms)), con, params=syms); con.close()
    g = dd.groupby("symbol")
    dd["pc"] = g["close"].shift(1)                                            # yesterday's close (for gap; correct)
    # ALL daily-context features LAGGED to yesterday's close (no look-ahead into today's daily bar)
    dd["prev_ret"] = (g["close"].shift(1) / g["close"].shift(2) - 1) * 100    # return ending YESTERDAY
    dd["prange"] = (((dd.high - dd.low) / dd.open * 100).groupby(dd.symbol).shift(1))
    dd["sma50"] = g["close"].transform(lambda s: s.rolling(50).mean()).groupby(dd.symbol).shift(1)
    dd["sma200"] = g["close"].transform(lambda s: s.rolling(200).mean()).groupby(dd.symbol).shift(1)
    dd["prsi"] = g["close"].transform(lambda s: _rsi(s.values)).groupby(dd.symbol).shift(1)
    frames = [x for s in syms if (x := stock_panel(s, dd[dd.symbol == s].set_index("trade_date"))) is not None]
    P = pd.concat(frames, ignore_index=True).dropna(subset=["gap", "drive", "or_pos", "or_range", "vol15", "prev_ret", "prev_rsi", "trend", "prev_range", "y"])
    # cross-sectional z-score within each day
    for f in ["gap", "drive", "or_pos", "or_range", "prev_ret", "prev_rsi", "prev_range"]:
        P[f] = P.groupby("date")[f].transform(lambda x: (x-x.mean())/(x.std()+1e-9))
    P["vol_z"] = P.groupby("date").vol15.transform(lambda x: (x-x.mean())/(x.std()+1e-9))
    P[FEATS] = P[FEATS].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    P["wk"] = pd.to_datetime(P.date).dt.strftime("%G%V")
    weeks = sorted(P.wk.unique()); rows = []
    for wi in range(MIN_TRAIN_WK, len(weeks)):
        tw = weeks[wi]; tr = P[P.wk < tw]; te = P[P.wk == tw]
        if len(tr) < 500 or te.empty: continue
        clf = LogisticRegression(max_iter=200, C=0.5).fit(tr[FEATS].values, tr.y.values)
        te = te.copy(); te["p"] = clf.predict_proba(te[FEATS].values)[:, 1]
        for d, g in te.groupby("date"):
            if len(g) < 30: continue
            gs = g.sort_values("p", ascending=False); L = gs.head(N); S = gs.tail(N)
            lr = (L.close/L.px0930-1)*100; sr = (S.close/S.px0930-1)*100
            rows.append(dict(date=d, long_up=(lr > 0).mean()*100, short_dn=(sr < 0).mean()*100,
                             L=lr.mean(), S=-sr.mean(), net=lr.mean()-sr.mean()-COST))
    R = pd.DataFrame(rows)
    print(f"ODDS ENGINE v1  ·  Nifty-50 long/short  ·  1X  ·  entry 09:30/exit close  ·  {len(R)} days OOS\n")
    print(f"  ACHIEVED ODDS (how far past 50:50):")
    print(f"    Long basket up-rate : {R.long_up.mean():.1f}%   (target of the exercise: push this toward 70%)")
    print(f"    Short basket down-rate: {R.short_dn.mean():.1f}%")
    print(f"\n  BASKET RETURN (1X, net {COST}%/stock):")
    print(f"    net %/day  : {R.net.mean():+.3f}%   ·   hit {int((R.net>0).mean()*100)}%   ·   median {R.net.median():+.3f}%")
    print(f"    total      : {R.net.sum():+.0f}%   over {len(R)} days   ·   best {R.net.max():+.2f}%  worst {R.net.min():+.2f}%")
    print(f"\n  TARGET +0.8 to +1.0%/day.  v1 achieves {R.net.mean():+.3f}%/day at odds {R.long_up.mean():.0f}%/{R.short_dn.mean():.0f}%.")
    R.to_csv(os.path.expanduser("~")+"/Downloads/ODDS_ENGINE_V1.csv", index=False)
    print("  saved -> ~/Downloads/ODDS_ENGINE_V1.csv")


if __name__ == "__main__":
    main()
