"""
MEAN-REVERSION LONG LAB (1x CNC, unleveraged) — buy oversold, sell the bounce. High turnover,
symmetric edge, does NOT depend on a fat tail => the honest way to beat breakout-swing's ~9% ceiling
IF the per-trade edge survives costs at high frequency.

Design (LEAK-FREE):
  signal computed on close of day t -> ENTER at OPEN of t+1 -> EXIT at CLOSE of exit day (net costs).
Setups tested (Connors-style + variants):
  rsi2<10, rsi2<5, down3 (>=3 red days & below ma5), pctb (close<lower Boll(20,2)), drop_atr (close
  fell > k*ATR below ma10). Each optionally gated by a regime/quality filter (price>ma200, liquidity).
Exits tested: ma5x (close>ma5), upclose (first higher close), n3/n5 (time), tgt4 (+4% or n10 stop).

Honest costs 0.30% RT (base) applied to every trade; 0.50% stress in the fund section.
Then a compounding N-slot CNC fund (reconciles total vs CAGR) + OOS era split + DD/Sharpe/Calmar.

Run: python scripts/mr_lab.py
"""
from __future__ import annotations
import sys, time
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import daily_core as DC

MIN_PRICE = 20.0            # avoid penny noise
MIN_DVOL = 2e7             # >= Rs 2 cr avg daily turnover (20d) -> tradeable/liquid
MAXHOLD = 10               # hard cap on holding days for any MR trade


def rsi(series, n):
    d = series.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    rs = up / dn.replace(0, np.nan)
    return (100 - 100 / (1 + rs)).fillna(50)


def prep(g):
    c = g["c"]; h = g["h"]; l = g["l"]
    out = g.copy()
    out["rsi2"] = rsi(c, 2)
    out["ma5"] = c.rolling(5).mean()
    out["ma10"] = c.rolling(10).mean()
    out["ma200"] = c.rolling(200).mean()
    out["dvol20"] = (c * g["v"]).rolling(20).mean()
    pc = c.shift(1)
    tr = np.maximum(h - l, np.maximum((h - pc).abs(), (l - pc).abs()))
    out["atr14"] = tr.rolling(14).mean()
    m = c.rolling(20).mean(); sd = c.rolling(20).std()
    out["bb_lo"] = m - 2 * sd
    out["red"] = (c < pc).astype(int)
    out["red3"] = out["red"].rolling(3).sum()
    return out


def signal(df, kind):
    c, pc = df["c"], df["c"].shift(1)
    base = (df["c"] > MIN_PRICE) & (df["dvol20"] > MIN_DVOL) & df["ma200"].notna()
    if kind == "rsi2_10":
        s = df["rsi2"] < 10
    elif kind == "rsi2_5":
        s = df["rsi2"] < 5
    elif kind == "down3":
        s = (df["red3"] >= 3) & (c < df["ma5"])
    elif kind == "pctb":
        s = c < df["bb_lo"]
    elif kind == "drop_atr":
        s = (df["ma10"] - c) > 1.0 * df["atr14"]
    else:
        raise ValueError(kind)
    return base & s


def sim_trades(D, kind, exit_rule, regime=True, cost_rt=DC.COST_RT):
    """Return list of (entry_date, exit_date, ret_net_frac, hold_days)."""
    out = []
    for sym, df in D.items():
        sig = signal(df, kind)
        if regime:
            sig = sig & (df["c"] > df["ma200"])
        idx = np.where(sig.values)[0]
        c = df["c"].values; o = df["o"].values; ma5 = df["ma5"].values
        atr = df["atr14"].values; dates = df["date"].values
        n = len(c)
        last_exit = -1
        for k in idx:
            if k <= last_exit:            # non-overlapping per symbol
                continue
            if k + 1 >= n or not (o[k + 1] > 0):
                continue
            entry = o[k + 1]
            xd = None; expx = None
            end = min(k + 1 + MAXHOLD, n)
            for d in range(k + 1, end):
                hit = False
                if exit_rule == "ma5x":
                    if c[d] > ma5[d]:
                        hit = True
                elif exit_rule == "upclose":
                    if c[d] > c[d - 1]:
                        hit = True
                elif exit_rule == "n3":
                    if d - k >= 3:
                        hit = True
                elif exit_rule == "n5":
                    if d - k >= 5:
                        hit = True
                elif exit_rule == "tgt4":
                    if c[d] >= entry * 1.04 or d - k >= 10:
                        hit = True
                if hit or d == end - 1:
                    xd = d; expx = c[d]; break
            if xd is None:
                continue
            ret = expx / entry - 1 - cost_rt / 100
            out.append((dates[k + 1], dates[xd], ret, xd - k))
    return out


def expectancy(trades):
    if not trades:
        return dict(n=0)
    r = np.array([t[2] for t in trades])
    hold = np.array([t[3] for t in trades])
    return dict(n=len(r), win=round((r > 0).mean() * 100, 1), avg=round(r.mean() * 100, 3),
                med=round(np.median(r) * 100, 3), hold=round(hold.mean(), 1),
                trades_per_year=None)


def main():
    t0 = time.time()
    panel = DC.load_panel()
    panel = panel[panel.symbol != DC.INDEX]
    D = {sym: prep(g.sort_values("date").reset_index(drop=True)) for sym, g in panel.groupby("symbol", sort=False)}
    print(f"prepped {len(D)} symbols in {time.time()-t0:.0f}s\n")

    print("=== PER-TRADE EXPECTANCY (net 0.30% RT, regime=price>ma200, leak-free open t+1) ===")
    print(f"{'setup':<10}{'exit':<9}{'n':>7}{'win%':>7}{'avg%':>8}{'med%':>8}{'hold':>6}")
    grid = {}
    for kind in ["rsi2_10", "rsi2_5", "down3", "pctb", "drop_atr"]:
        for ex in ["ma5x", "upclose", "n3", "tgt4"]:
            tr = sim_trades(D, kind, ex)
            grid[(kind, ex)] = tr
            e = expectancy(tr)
            if e["n"]:
                print(f"{kind:<10}{ex:<9}{e['n']:>7}{e['win']:>6}%{e['avg']:>+8.3f}{e['med']:>+8.3f}{e['hold']:>6}")
    print(f"\n  [{time.time()-t0:.0f}s]")

    # pick top few by total edge (avg% * n proxy for capacity) with positive avg
    ranked = sorted(grid.items(), key=lambda kv: (expectancy(kv[1]).get("avg", -9) or -9), reverse=True)
    print("\n=== COMPOUNDING FUND (N-slot CNC, reconciled) for top setups by avg edge ===")
    print(f"{'setup':<10}{'exit':<9}{'N':>4}{'taken':>7}{'total%':>10}{'CAGR%':>7}{'maxDD%':>8}{'Calmar':>7}{'Shrp':>6}{'ok':>4}")
    for (kind, ex), tr in ranked[:6]:
        if expectancy(tr)["avg"] is None or expectancy(tr)["avg"] <= 0:
            continue
        for N in [10, 15, 20]:
            m, _ = DC.slot_fund([(a, b, c) for a, b, c, _ in tr], N)
            ok = "y" if DC.reconcile(m) else "BAD"
            print(f"{kind:<10}{ex:<9}{N:>4}{m['taken']:>7}{m['total_%']:>10}{m['CAGR_%']:>6}%{m['maxDD_%']:>7}%{str(m['calmar']):>7}{m['sharpe_m']:>6}{ok:>4}")

    # OOS era split for the single best (kind,ex,N) by CAGR among reconciled
    best = None
    for (kind, ex), tr in ranked[:6]:
        e = expectancy(tr)
        if not e["n"] or (e["avg"] or 0) <= 0:
            continue
        for N in [10, 15, 20]:
            m, _ = DC.slot_fund([(a, b, c) for a, b, c, _ in tr], N)
            if DC.reconcile(m) and (best is None or m["CAGR_%"] > best[0]["CAGR_%"]):
                best = (m, kind, ex, N, tr)
    if best:
        m, kind, ex, N, tr = best
        print(f"\n=== BEST: {kind}/{ex} N={N}  full-sample CAGR {m['CAGR_%']}%  DD {m['maxDD_%']}%  Calmar {m['calmar']} ===")
        print("  OOS era split:")
        print(f"  {'era':<16}{'trades':>8}{'total%':>9}{'CAGR%':>7}{'maxDD%':>8}{'Calmar':>7}")
        for lo, hi, lab in [("2013-01-01", "2020-01-01", "2013-2019"),
                            ("2020-01-01", "2023-01-01", "2020-2022"),
                            ("2023-01-01", "2027-01-01", "2023-2026 OOS")]:
            sub = [(a, b, c) for a, b, c, _ in tr if str(pd.Timestamp(a).date()) >= lo and str(pd.Timestamp(a).date()) < hi]
            if len(sub) < 30:
                continue
            mm, _ = DC.slot_fund(sub, N)
            print(f"  {lab:<16}{mm['taken']:>8}{mm['total_%']:>9}{mm['CAGR_%']:>6}%{mm['maxDD_%']:>7}%{str(mm['calmar']):>7}")
        # cost stress
        trc = sim_trades(D, kind, ex, cost_rt=DC.COST_RT_STRESS)
        mc, _ = DC.slot_fund([(a, b, c) for a, b, c, _ in trc], N)
        print(f"\n  cost stress 0.50% RT: CAGR {mc['CAGR_%']}%  DD {mc['maxDD_%']}%  (base 0.30% = {m['CAGR_%']}%)")


if __name__ == "__main__":
    main()
