"""
AGENT JARVIS v2 — base-breakout detector with TYPE classification (macro gates + micro confirmation).
No one-size-fits-all: every breakout is captured, CLASSIFIED by structure, and expectancy is reported
PER TYPE so we learn which breakouts carry an edge.

Breakout = intraday cross above the prior 60-day high (real base, not a 20-day noise-high).
Macro gates (daily, point-in-time): which higher tiers it clears (120/252-day high), base tightness,
volatility contraction, prior trend, gap, thrust (range expansion vs ATR).
Micro confirmation (1-min): volume surge at the break + it HOLDS above the level for CONFIRM_MIN minutes
(sustainable), entering only on confirmation — no look-ahead. Then trail + EOD square-off.

Fast: reads the shared payload (cache/bars) + preloaded daily. Run: python scripts/agent_jarvis_v2.py
"""
from __future__ import annotations
import os, sys, sqlite3, time, json
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import arena_fast as A
KDB = str(ROOT / "db" / "kanida.db")
LEV, ARM, FLOOR, GIVE, HARD = 5.0, 8.0, 3.0, 5.0, 6.0     # momentum trail (wider than v1)
COST_PRICE_RT = 0.16
CONFIRM_MIN = 15                                          # bars to wait for the breakout to HOLD before entering
N_SLOTS = 15


def load_all_daily():
    con = sqlite3.connect("file:" + Path(KDB).as_posix() + "?mode=ro", uri=True)
    df = pd.read_sql_query("SELECT symbol,bar_time,open,high,low,close FROM ohlc_daily WHERE symbol!='NIFTY 50' ORDER BY symbol,bar_time", con)
    con.close()
    df["di"] = df["bar_time"].str[:10].str.replace("-", "").astype(np.int32)
    feats = {}
    for sym, g in df.groupby("symbol", sort=False):
        c = g["close"].values.astype(float); h = g["high"].values.astype(float); l = g["low"].values.astype(float); o = g["open"].values.astype(float)
        pc = np.roll(c, 1); pc[0] = c[0]
        tr = np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc)))
        atr = pd.Series(tr).rolling(20).mean().values
        S = pd.Series(h)
        hi = lambda n: S.rolling(n).max().shift(1).values
        hi60, hi120, hi252 = hi(60), hi(120), hi(252)
        cS = pd.Series(c)
        basew = ((cS.rolling(60).max() - cS.rolling(60).min()).shift(1).values) / c
        atrc = atr / np.roll(atr, 60)
        trend60 = c / np.roll(c, 60) - 1
        di = g["di"].values
        feats[sym] = {int(di[k]): (o[k], hi60[k], hi120[k], hi252[k], atr[k], basew[k], atrc[k], trend60[k], tr[k])
                      for k in range(len(c))}
    return feats


def classify(clr120, clr252, basew, atrc, gap, trend60):
    if gap >= 2.5:
        return "gap_go"
    if clr252 and (basew < 0.40) and (trend60 < 0.12):
        return "base_breakout"
    if clr120 and (atrc < 0.85):
        return "coil_vcp"
    if trend60 > 0.15:
        return "trend_continuation"
    return "shallow"


def events_for(sym, dfeat):
    z = A.load(sym)
    if z is None or dfeat is None:
        return []
    date, hm, h, l, c, v = z["date"], z["hm"], z["h"], z["l"], z["c"], z["v"]
    ds = z["day_start"]; n = len(ds); N = len(date); out = []
    for k in range(n):
        a = int(ds[k]); b = int(ds[k + 1]) if k + 1 < n else N
        di = int(date[a]); f = dfeat.get(di)
        if f is None:
            continue
        dopen, hi60, hi120, hi252, atr, basew, atrc, trend60, tr = f
        if not np.isfinite(hi60) or not np.isfinite(basew):
            continue
        dhm = hm[a:b]; dh = h[a:b]; dl = l[a:b]; dc = c[a:b]; dv = v[a:b]
        cand = np.where((dhm >= 920) & (dhm <= 1430) & (dh >= hi60))[0]     # cross prior-60d high
        if len(cand) == 0:
            continue
        i = int(cand[0])
        if i < 3:
            continue
        lvl = float(hi60)
        # micro confirmation: HOLD above the level for CONFIRM_MIN bars, else fakeout -> skip
        ei = i + CONFIRM_MIN
        if ei >= len(dhm) or dhm[ei] > 1520:
            continue
        if float(np.min(dl[i:ei + 1])) < lvl * 0.997:                       # dipped back below the base -> not sustained
            continue
        avgv = dv[:i].mean(); surge = float(dv[i] / avgv) if avgv > 0 else 0.0
        if surge < 1.5:
            continue
        clr120 = bool(np.isfinite(hi120) and dh[i] >= hi120)
        clr252 = bool(np.isfinite(hi252) and dh[i] >= hi252)
        gap = (dopen / (c[a - 1] if a > 0 else dopen) - 1) * 100 if a > 0 else 0.0
        thrust = float(tr / atr) if (np.isfinite(atr) and atr > 0) else 0.0
        typ = classify(clr120, clr252, basew, atrc if np.isfinite(atrc) else 1.0, gap, trend60 if np.isfinite(trend60) else 0.0)
        sqc = np.where(dhm <= 1520)[0]
        endi = int(sqc[-1]) if len(sqc) else b - a - 1
        if endi <= ei:
            continue
        cap, xj = A.trail_long(dh, dl, dc, ei, endi, LEV, ARM, FLOOR, GIVE, HARD)  # enter at confirmed bar ei
        out.append((sym, di, int(dhm[ei]), int(dhm[xj]), float(cap) - COST_PRICE_RT * LEV, typ, surge, thrust))
    return out


def main():
    t0 = time.time()
    print("loading daily features ...", flush=True)
    dfeats = load_all_daily()
    syms = A.universe()
    rows = []
    for s in syms:
        rows.extend(events_for(s, dfeats.get(s)))
    df = pd.DataFrame(rows, columns=["symbol", "date", "entry_hm", "exit_hm", "cap_pct", "type", "surge", "thrust"])
    dt = time.time() - t0
    print(f"\n===== AGENT JARVIS v2 (base-breakout + type + micro-confirm) — {dt:.0f}s =====")
    print(f"  breakout trades (cleared 60d high, held {CONFIRM_MIN}m, surge>=1.5): {len(df):,}")
    print(f"\n  --- expectancy BY TYPE (which breakouts carry an edge) ---")
    print(f"  {'type':<20}{'trades':>8}{'win%':>7}{'cap/trade':>11}{'avg surge':>11}{'avg thrust':>11}")
    stats = []
    for typ, g in df.groupby("type"):
        cp = g["cap_pct"].values
        stats.append((typ, len(g), (cp > 0).mean() * 100, cp.mean(), g["surge"].mean(), g["thrust"].mean()))
    for typ, nt, wr, mc, sg, th in sorted(stats, key=lambda x: -x[3]):
        print(f"  {typ:<20}{nt:>8}{wr:>6.1f}%{mc:>+10.2f}%{sg:>11.1f}{th:>11.1f}")
    # portfolio on the POSITIVE-expectancy types only (a-priori: keep types with mean cap>0)
    good = [t for t, _, _, mc, _, _ in stats if mc > 0]
    print(f"\n  positive-expectancy types: {good}")
    for label, sub in [("ALL types", df), ("positive types only", df[df.type.isin(good)])]:
        if sub.empty:
            continue
        dayret = A.slot_portfolio(sub.sort_values("entry_hm"), N_SLOTS)
        m = A.metrics(dayret)
        print(f"\n  [{label}] CAGR {m['cagr']}%  total {m['total_ret']}%  maxDD {m['max_dd']}%  Calmar {m['calmar']}  "
              f"avg-mo {m['avg_month']}%  +mo {m['pct_pos_months']}%  Sharpe {m['sharpe']}  ({m['start']}..{m['end']})")
    df.to_csv(ROOT / "reports" / "jarvis_v2_events.csv", index=False)
    print("\n  written: reports/jarvis_v2_events.csv")


if __name__ == "__main__":
    main()
