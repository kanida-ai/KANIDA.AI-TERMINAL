# -*- coding: utf-8 -*-
"""ONE-STOCK PLAYBOOK — combine ALL indicators into a single daily Buy/Sell signal, trend-matched.
Period 2022-2026. Uptrend (SMA50>SMA200) -> buy the dip (count OVERSOLD indicators). Downtrend -> short
the rip (count OVERBOUGHT). Conviction = how many indicators agree. Higher conviction should mean bigger
next-day open->close. Sweep the conviction threshold to find where avg return hits 0.5-1%/day. Leak-free.
"""
import os, sqlite3, sys
import numpy as np, pandas as pd
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
COST = 0.06
START, END = "2022-01-01", "2026-12-31"


def _ema(s, n): return pd.Series(s).ewm(span=n, adjust=False).mean().values
def _sma(s, n): return pd.Series(s).rolling(n).mean().values
def _std(s, n): return pd.Series(s).rolling(n).std().values
def _rma(s, n): return pd.Series(s).ewm(alpha=1/n, adjust=False).mean().values
def _rsi(c, n):
    d = np.diff(c, prepend=c[0]); return 100 - 100/(1 + _rma(np.clip(d, 0, None), n)/(_rma(-np.clip(d, None, 0), n)+1e-9))
def _atr(h, l, c, n):
    pc = np.roll(c, 1); pc[0] = c[0]; return _rma(np.maximum(h-l, np.maximum(np.abs(h-pc), np.abs(l-pc))), n)


def indicators(D):
    c, h, l, v = D["c"], D["h"], D["l"], D["v"]
    tp = (h+l+c)/3
    # each -> (oversold_bool, overbought_bool)
    ind = {}
    st = (c - _ema(c, 20)) / (_atr(h, l, c, 10) + 1e-9)              # Keltner stretch (ma20/atr10)
    ind["keltner"] = (st <= -2.5, st >= 2.5)
    bb = (c - _sma(c, 20)) / (_std(c, 20) + 1e-9)                     # Bollinger z
    ind["bollinger"] = (bb <= -2.0, bb >= 2.0)
    r = _rsi(c, 14); ind["rsi"] = (r < 30, r > 70)
    ll = pd.Series(l).rolling(14).min().values; hh = pd.Series(h).rolling(14).max().values
    k = (c - ll)/(hh - ll + 1e-9)*100; ind["stoch"] = (k < 20, k > 80)
    ma = _sma(tp, 14); md = pd.Series(np.abs(tp-ma)).rolling(14).mean().values
    cci = (tp-ma)/(0.015*md+1e-9); ind["cci"] = (cci < -150, cci > 150)
    wr = -100*(hh-c)/(hh-ll+1e-9); ind["williams"] = (wr < -85, wr > -15)
    mf = tp*v; pos = np.where(tp > np.roll(tp, 1), mf, 0.0); neg = np.where(tp < np.roll(tp, 1), mf, 0.0)
    pr = pd.Series(pos).rolling(14).sum().values; nr = pd.Series(neg).rolling(14).sum().values
    mfi = 100 - 100/(1 + pr/(nr+1e-9)); ind["mfi"] = (mfi < 25, mfi > 75)
    return ind


def run(sym, verbose=True):
    con = sqlite3.connect("file:" + DB.replace("\\", "/") + "?mode=ro", uri=True)
    g = pd.read_sql_query("SELECT trade_date,open,high,low,close,volume FROM ohlc_daily WHERE symbol=? "
                          "AND trade_date>='2020-06-01' ORDER BY trade_date", con, params=[sym]); con.close()
    if len(g) < 250: return None
    D = {x: g[x].values.astype(float) for x in ("open", "high", "low", "close", "volume")}
    D = dict(o=D["open"], h=D["high"], l=D["low"], c=D["close"], v=D["volume"])
    o1 = np.roll(D["o"], -1); c1 = np.roll(D["c"], -1); oc = (c1 - o1) / o1 * 100.0; oc[-1] = np.nan
    dates = g.trade_date.values
    up = _sma(D["c"], 50) > _sma(D["c"], 200); down = _sma(D["c"], 50) < _sma(D["c"], 200)
    ind = indicators(D)
    os_cnt = np.sum([ind[k][0] for k in ind], axis=0)      # # oversold indicators
    ob_cnt = np.sum([ind[k][1] for k in ind], axis=0)      # # overbought indicators
    m = (dates >= START) & (dates <= END) & ~np.isnan(oc)
    idx = np.where(m)[0]
    rows = []
    for i in idx:
        if up[i]:
            direction, conv = 1, os_cnt[i]
        elif down[i]:
            direction, conv = -1, ob_cnt[i]
        else:
            direction, conv = 0, 0
        ret = (oc[i] if direction == 1 else -oc[i]) - COST if direction != 0 else np.nan
        rows.append(dict(date=dates[i], trend="UP" if up[i] else ("DOWN" if down[i] else "-"),
                         os=int(os_cnt[i]), ob=int(ob_cnt[i]), direction=direction, conviction=int(conv),
                         oc=round(oc[i], 3), ret=round(ret, 3) if direction != 0 else np.nan))
    R = pd.DataFrame(rows)
    if verbose:
        print(f"\n{'='*76}\nPLAYBOOK — {sym}   2022-2026   (7 indicators, trend-matched, combined)\n{'='*76}")
        print(f"trading days {len(R)}   ·   indicators: {', '.join(ind.keys())}")
        print(f"\n{'conviction>=K':<14}{'signals':>9}{'/yr':>6}{'WR>=0.5%':>10}{'avg ret':>9}{'hit1%':>8}{'total%':>9}")
        for K in range(1, 8):
            sig = R[(R.direction != 0) & (R.conviction >= K)]
            if len(sig) == 0: continue
            wr = (sig.ret + COST >= 0.5).mean() * 100         # hit the 0.5% target (gross of the cost we subtracted)
            hit1 = (sig.ret + COST >= 1.0).mean() * 100
            peryr = len(sig) / ((pd.to_datetime(R.date.max()) - pd.to_datetime(R.date.min())).days/365.25)
            print(f"  K>={K:<10}{len(sig):>9}{peryr:>6.0f}{wr:>9.1f}%{sig.ret.mean():>+9.3f}{hit1:>7.1f}%{sig.ret.sum():>+9.1f}")
    return R


if __name__ == "__main__":
    sym = sys.argv[1].upper() if len(sys.argv) > 1 else "ICICIBANK"
    R = run(sym)
    R.to_csv(os.path.join(os.path.expanduser("~"), "Downloads", f"PLAYBOOK_{sym}.csv"), index=False)
    print(f"\nsaved signal log -> ~/Downloads/PLAYBOOK_{sym}.csv")
