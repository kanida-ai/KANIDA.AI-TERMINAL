"""
KANIDA Chart Agent — DAILY SCREENER (Horizontal Trendline, daily timeframe).

For a chosen date, scans EVERY stock point-in-time (only candles up to that date) and classifies
each into a live stage of the Horizontal-Trendline setup, using the SAME rules as the detector/replay:

  APPROACHING  price sitting just under a flat resistance it hasn't closed above
  BREAKOUT     today is the first daily close above that level, on above-average volume
  RETEST       a recent breakout pulled back to the level today and closed back up on volume (pattern complete)
  FAILED       a recent breakout lost the level today (fake-out)

Output: reports/chart_agent/screen_<DATE>.json + a ranked console table.  This is the layer that feeds
the agent storyline — "today the Chart Agent found N setups across the market" — instead of hand-picking.

Run:  python scripts/chart_agent_screener.py 2022-08-30
"""
from __future__ import annotations
import os, sys, json, sqlite3
import numpy as np, pandas as pd
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass
sys.path.insert(0, os.path.dirname(__file__))
from chart_agent import load_daily, detect_horizontal_breakout_retest, pattern_evidence, PARAMS, DB, OUT

P = PARAMS
APPROACH_BAND = 0.02   # within 2% below the level = "approaching"


def _pivots(high, L):
    n = len(high); p = np.zeros(n, bool)
    for i in range(L, n - L):
        if high[i] == high[i - L:i + L + 1].max():
            p[i] = True
    return np.where(p)[0]


def _levels(h, c, pv, k):
    """Flat-top resistance candidates as-of bar k (highest first): >=min_touches pivot highs within
    tol, and no daily close above the level between the first touch and k (an unbroken ceiling)."""
    win = [i for i in pv if k - P["level_window"] <= i <= k - P["L"]]
    if len(win) < P["min_touches"]:
        return []
    prices = np.array([h[i] for i in win]); out = []
    used = set()
    for cand in np.sort(prices)[::-1]:
        m = np.abs(prices - cand) <= P["tol"] * cand
        if m.sum() < P["min_touches"]:
            continue
        lvl = float(prices[m].mean())
        if round(lvl, 1) in used:
            continue
        used.add(round(lvl, 1))
        ti = [win[j] for j in range(len(win)) if m[j]]
        if c[min(ti):k].max() > lvl * (1 + P["buffer"]):   # flat top must be unbroken through k
            continue
        out.append((lvl, ti))
    return out


def classify(o, h, l, c, v, avg, pv, di):
    """Return (stage, level, touches, dist_pct, volx) for day di, or None."""
    a = avg[di]
    if not np.isfinite(a) or a <= 0:
        return None
    cands = _levels(h, c, pv, di)
    # 1) BREAKOUT today — first close above, on volume
    for lvl, ti in cands:
        if c[di - 1] <= lvl * (1 + P["buffer"]) < c[di] and v[di] > P["vol_mult"] * a:
            return ("BREAKOUT", lvl, ti, (c[di] / lvl - 1) * 100, v[di] / a)
    # 2) APPROACHING — nearest flat top just above price, not yet broken
    for lvl, ti in cands:
        if lvl * (1 - APPROACH_BAND) <= c[di] <= lvl * (1 + P["buffer"]):
            return ("APPROACHING", lvl, ti, (c[di] / lvl - 1) * 100, v[di] / a)
    # 3) RETEST / FAILED — was there a breakout in the last retest_max sessions?
    for b in range(di - 1, max(P["L"], di - P["retest_max"] - 1), -1):
        for lvl, ti in _levels(h, c, pv, b):
            if c[b - 1] <= lvl * (1 + P["buffer"]) < c[b] and v[b] > P["vol_mult"] * avg[b]:
                if (l[di] <= lvl * (1 + P["retest_tol"]) and c[di] >= lvl * (1 - P["retest_tol"])
                        and c[di] > c[di - 1] and c[di] > lvl and v[di] > P["retest_vol_mult"] * a):
                    return ("RETEST", lvl, ti, (c[di] / lvl - 1) * 100, v[di] / a)
                if c[di] < lvl * (1 - P["buffer"]):
                    return ("FAILED", lvl, ti, (c[di] / lvl - 1) * 100, v[di] / a)
                break
    return None


def evidence_asof(sym, date):
    """Point-in-time historical win-rate for this stock's setup (occurrences resolved on/before date)."""
    try:
        df = load_daily(sym)
    except Exception:
        return None
    if date not in set(df.index.strftime("%Y-%m-%d")):
        return None
    di = df.index.get_loc(pd.Timestamp(date))
    resolved = [e for e in detect_horizontal_breakout_retest(df, **P) if e.entry_idx + 9 <= di]
    if not resolved:
        return {"n": 0, "win_rate": None}
    pv = pattern_evidence(df.iloc[:di + 1], resolved, 10)
    return {"n": pv["summary"]["n"], "win_rate": pv["summary"]["pct_up"]} if pv else {"n": 0, "win_rate": None}


def screen(date="2022-08-30"):
    con = sqlite3.connect(f"file:{os.path.abspath(DB)}?mode=ro", uri=True)
    lo = (pd.Timestamp(date) - pd.Timedelta(days=320)).strftime("%Y-%m-%d")
    q = ("SELECT symbol, substr(bar_time,1,10) d, open,high,low,close,volume FROM ohlc_daily "
         "WHERE substr(bar_time,1,10)<=? AND substr(bar_time,1,10)>=? AND symbol<>'NIFTY 50' "
         "ORDER BY symbol, bar_time")
    df = pd.read_sql_query(q, con, params=(date, lo)); con.close()
    hits = {"RETEST": [], "BREAKOUT": [], "APPROACHING": [], "FAILED": []}
    n_scanned = 0
    for sym, g in df.groupby("symbol", sort=False):
        if g["d"].iloc[-1] != date:          # stock must have traded on the screen date
            continue
        if len(g) < P["level_window"] + P["L"] + 5:
            continue
        n_scanned += 1
        o, h, l, c, v = (g[k].to_numpy(float) for k in ["open", "high", "low", "close", "volume"])
        avg = pd.Series(v).rolling(20).mean().to_numpy()
        pv = _pivots(h, P["L"]); di = len(c) - 1
        r = classify(o, h, l, c, v, avg, pv, di)
        if r:
            stage, lvl, ti, dist, volx = r
            hits[stage].append({"symbol": sym, "level": round(lvl, 1), "close": round(float(c[di]), 1),
                                "dist_pct": round(dist, 2), "volx": round(volx, 1), "touches": len(ti)})
    # rank
    hits["BREAKOUT"].sort(key=lambda x: -x["volx"])
    hits["RETEST"].sort(key=lambda x: -x["volx"])
    hits["APPROACHING"].sort(key=lambda x: (abs(x["dist_pct"]), -x["volx"]))
    hits["FAILED"].sort(key=lambda x: x["dist_pct"])
    # attach point-in-time evidence to the actionable stages (breakout + retest)
    for st in ("RETEST", "BREAKOUT"):
        for row in hits[st][:40]:
            ev = evidence_asof(row["symbol"], date)
            row["hist_n"] = ev["n"] if ev else 0
            row["hist_win"] = ev["win_rate"] if ev else None

    result = {"date": date, "scanned": n_scanned,
              "counts": {k: len(v) for k, v in hits.items()}, "hits": hits}
    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, f"screen_{date}.json")
    json.dump(result, open(path, "w"), separators=(",", ":"))

    print(f"\nCHART AGENT · Horizontal Trendline · daily screen for {date}")
    print(f"scanned {n_scanned} stocks  →  "
          f"{len(hits['RETEST'])} retest-confirmed · {len(hits['BREAKOUT'])} breakout-today · "
          f"{len(hits['APPROACHING'])} approaching · {len(hits['FAILED'])} failed")
    for st in ("RETEST", "BREAKOUT", "APPROACHING", "FAILED"):
        rows = hits[st]
        if not rows:
            continue
        print(f"\n── {st}  ({len(rows)}) " + "─" * 30)
        for x in rows[:12]:
            ev = ""
            if st in ("RETEST", "BREAKOUT"):
                ev = f"  hist {x.get('hist_win')}% (n{x.get('hist_n')})" if x.get("hist_win") is not None else f"  hist n{x.get('hist_n',0)}"
            print(f"  {x['symbol']:12} lvl {x['level']:>9}  close {x['close']:>9}  {x['dist_pct']:+5.1f}%  "
                  f"vol {x['volx']:.1f}x  {x['touches']}tch{ev}")
        if len(rows) > 12:
            print(f"  … +{len(rows)-12} more")
    print(f"\n-> {path}")
    return result


if __name__ == "__main__":
    screen(sys.argv[1] if len(sys.argv) > 1 else "2022-08-30")
