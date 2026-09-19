"""
AGENT JARVIS — SWING version. Same base-breakout detector + TYPE classification, but the trade HOLDS
multi-day with a daily trailing stop (delivery / CNC, 1x) instead of squaring off intraday. This is how a
KALYANKJIL-style multi-week breakout is actually captured. Reports expectancy PER TYPE.

Breakout (daily): first day close crosses ABOVE the prior-60-day high (a real base high, closed above =
survived the session — the swing confirmation) with a volume surge. Classified by structure. Then:
  entry = next day's open; hold with a trailing stop (giveback% from peak high) + initial hard stop +
  max-hold cap; exit at the stop or the cap. Return = price move (1x) - cost. Leak-free (all point-in-time).

Run: python scripts/agent_jarvis_swing.py
"""
from __future__ import annotations
import os, sys, sqlite3, time
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import arena_fast as A
KDB = str(ROOT / "db" / "kanida.db")
TRAIL_PCT = 15.0      # trailing stop: exit if price falls this % from the highest high since entry
HARD_PCT = 8.0        # initial hard stop below entry
MAXDAYS = 60          # max swing hold
COST_RT = 0.30        # delivery round-trip cost incl slippage (1x, no leverage)
VOL_MIN = 1.5         # breakout-day volume >= this x 20d avg
N_SLOTS = 15; CAP0 = 1_500_000.0


def classify(clr120, clr252, basew, atrc, gap, trend60):
    if gap >= 3.0:
        return "gap_go"
    if clr252 and (basew < 0.40) and (trend60 < 0.12):
        return "base_breakout"
    if clr120 and (atrc < 0.85):
        return "coil_vcp"
    if trend60 > 0.15:
        return "trend_continuation"
    return "shallow"


def load_daily():
    con = sqlite3.connect("file:" + Path(KDB).as_posix() + "?mode=ro", uri=True)
    df = pd.read_sql_query("SELECT symbol,bar_time,open,high,low,close,volume FROM ohlc_daily WHERE symbol!='NIFTY 50' ORDER BY symbol,bar_time", con)
    con.close()
    df["di"] = df["bar_time"].str[:10].str.replace("-", "").astype(np.int64)
    out = {}
    for sym, g in df.groupby("symbol", sort=False):
        o = g["open"].values.astype(float); h = g["high"].values.astype(float)
        l = g["low"].values.astype(float); c = g["close"].values.astype(float); vol = g["volume"].values.astype(float)
        n = len(c)
        if n < 130:
            continue
        pc = np.roll(c, 1); pc[0] = c[0]
        tr = np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc)))
        atr = pd.Series(tr).rolling(20).mean().values
        S = pd.Series(h); hi = lambda m: S.rolling(m).max().shift(1).values
        hi60, hi120, hi252 = hi(60), hi(120), hi(252)
        cS = pd.Series(c)
        basew = ((cS.rolling(60).max() - cS.rolling(60).min()).shift(1).values) / np.where(c > 0, c, np.nan)
        atrc = atr / np.where(np.roll(atr, 60) > 0, np.roll(atr, 60), np.nan)
        trend60 = c / np.where(np.roll(c, 60) > 0, np.roll(c, 60), np.nan) - 1
        vavg = pd.Series(vol).rolling(20).mean().shift(1).values
        out[sym] = dict(di=g["di"].values, o=o, h=h, l=l, c=c, vol=vol, vavg=vavg,
                        hi60=hi60, hi120=hi120, hi252=hi252, atr=atr, basew=basew, atrc=atrc, trend60=trend60, tr=tr)
    return out


def swing_sim(o, h, l, c, k):
    if k + 1 >= len(c):
        return None
    entry = o[k + 1]
    if not (entry > 0):
        return None
    peak = entry; hard = entry * (1 - HARD_PCT / 100)
    end = min(k + 1 + MAXDAYS, len(c))
    for d in range(k + 1, end):
        if h[d] > peak:
            peak = h[d]
        stop = max(hard, peak * (1 - TRAIL_PCT / 100))
        if l[d] <= stop:
            return (stop / entry - 1) * 100 - COST_RT, d - k
    dd = end - 1
    return (c[dd] / entry - 1) * 100 - COST_RT, dd - k


def events_for(sym, dd):
    if dd is None:
        return []
    c = dd["c"]; hi60 = dd["hi60"]; hi120 = dd["hi120"]; hi252 = dd["hi252"]
    o = dd["o"]; h = dd["h"]; l = dd["l"]; vol = dd["vol"]; vavg = dd["vavg"]
    basew = dd["basew"]; atrc = dd["atrc"]; trend60 = dd["trend60"]; atr = dd["atr"]; tr = dd["tr"]; di = dd["di"]
    n = len(c); out = []
    k = 61
    while k < n - 1:
        if not (np.isfinite(hi60[k]) and c[k] > hi60[k] and c[k - 1] <= hi60[k - 1]):   # fresh close above prior-60d high
            k += 1; continue
        if not (np.isfinite(vavg[k]) and vavg[k] > 0):
            k += 1; continue
        surge = vol[k] / vavg[k]
        if surge < VOL_MIN or not np.isfinite(basew[k]):
            k += 1; continue
        clr120 = bool(np.isfinite(hi120[k]) and c[k] > hi120[k])
        clr252 = bool(np.isfinite(hi252[k]) and c[k] > hi252[k])
        gap = (o[k] / c[k - 1] - 1) * 100
        thrust = float(tr[k] / atr[k]) if (np.isfinite(atr[k]) and atr[k] > 0) else 0.0
        typ = classify(clr120, clr252, basew[k], atrc[k] if np.isfinite(atrc[k]) else 1.0, gap, trend60[k] if np.isfinite(trend60[k]) else 0.0)
        r = swing_sim(o, h, l, c, k)
        if r is not None:
            ret, hold = r
            out.append((sym, int(di[k]), int(di[min(k + hold, n - 1)]), float(ret), typ, float(surge), thrust, int(hold)))
            k = k + 1 + hold                                    # non-overlapping: next signal after this trade exits
        else:
            k += 1
    return out


def swing_portfolio(ev):
    """Proper concurrent N-slot fund: each breakout takes equity/N from cash, held to its exit, then
    returned with its P&L. Capital compounds; positions marked at cost while open. Returns a daily curve."""
    e = ev.sort_values("entry_di").reset_index(drop=True)
    ed = e.entry_di.values; xd = e.exit_di.values; ret = e.cap_pct.values
    evs = []
    for i in range(len(e)):
        evs.append((int(ed[i]), 0, i)); evs.append((int(xd[i]), 1, i))
    evs.sort(key=lambda x: (x[0], -x[1]))                       # same day: exits (free capital) before entries
    cash = CAP0; opencap = {}; curve = {}
    for date, kind, i in evs:
        if kind == 1:
            if i in opencap:
                cash += opencap.pop(i) * (1 + ret[i] / 100.0)
        else:
            if len(opencap) < N_SLOTS and cash > 0:
                slot = (cash + sum(opencap.values())) / N_SLOTS
                if 0 < slot <= cash:
                    cash -= slot; opencap[i] = slot
        curve[date] = cash + sum(opencap.values())
    s = pd.Series(curve).sort_index()
    idx = pd.to_datetime(s.index.astype(str), format="%Y%m%d")
    s = pd.Series(s.values, index=idx)
    s = s[~s.index.duplicated(keep="last")]
    daily = s.resample("D").ffill().dropna()
    return daily.pct_change().dropna() * 100                    # metrics() expects daily RETURNS, not the curve


def main():
    t0 = time.time()
    print("loading daily ...", flush=True)
    daily = load_daily()
    rows = []
    for s in A.universe():
        rows.extend(events_for(s, daily.get(s)))
    df = pd.DataFrame(rows, columns=["symbol", "entry_di", "exit_di", "cap_pct", "type", "surge", "thrust", "hold"])
    dt = time.time() - t0
    print(f"\n===== AGENT JARVIS — SWING (base-breakout + type, multi-day trailing hold) — {dt:.0f}s =====")
    print(f"  breakout trades (close>60d-high, surge>={VOL_MIN}, trail {TRAIL_PCT}% / hard {HARD_PCT}% / max {MAXDAYS}d): {len(df):,}")
    print(f"\n  --- expectancy BY TYPE (per-trade price move, 1x delivery) ---")
    print(f"  {'type':<20}{'trades':>8}{'win%':>7}{'ret/trade':>11}{'avg hold':>10}{'avg surge':>11}")
    stats = []
    for typ, g in df.groupby("type"):
        cp = g["cap_pct"].values
        stats.append((typ, len(g), (cp > 0).mean() * 100, cp.mean(), g["hold"].mean(), g["surge"].mean()))
    for typ, nt, wr, mc, hd, sg in sorted(stats, key=lambda x: -x[3]):
        print(f"  {typ:<20}{nt:>8}{wr:>6.1f}%{mc:>+10.2f}%{hd:>9.0f}d{sg:>11.1f}")
    good = [t for t, _, _, mc, _, _ in stats if mc > 0]
    print(f"\n  positive-expectancy types: {good}")
    for label, sub in [("ALL types", df), ("positive types only", df[df.type.isin(good)] if good else df.iloc[:0])]:
        if sub.empty:
            print(f"\n  [{label}] (none)"); continue
        cur = swing_portfolio(sub)
        m = A.metrics(cur)
        print(f"\n  [{label}] CAGR {m['cagr']}%  total {m['total_ret']}%  maxDD {m['max_dd']}%  Calmar {m['calmar']}  "
              f"avg-mo {m['avg_month']}%  +mo {m['pct_pos_months']}%  Sharpe {m['sharpe']}  ({m['start']}..{m['end']})")
    df.to_csv(ROOT / "reports" / "jarvis_swing_events.csv", index=False)
    print("\n  written: reports/jarvis_swing_events.csv")


if __name__ == "__main__":
    main()
