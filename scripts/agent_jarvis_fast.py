"""
AGENT JARVIS — fast, on the shared arena payload (cache/bars/*.npz) + numba trail. Sweeps trade-mechanics
configs in one run so we can find whether a breakout edge exists once the tight-stop bug is fixed.
Prereq: python scripts/arena_fast.py build   (one-time cache)
Run:    python scripts/agent_jarvis_fast.py
"""
from __future__ import annotations
import os, sys, json, time
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import arena_fast as A

COST_PRICE_RT = 0.16                    # round-trip price % (broker+slippage); capital cost = this * leverage
CONFIGS = [
    # name,           lev, arm, floor, give, hard, surge_min, slots
    ("v1 (reproduce)", 5.0, 6,  2, 4, 3.0,  1.5, 15),
    ("wider stop",     5.0, 12, 4, 8, 10.0, 2.5, 15),
    ("wide + selective",5.0,14, 5, 9, 12.0, 3.5, 15),
    ("lower lev 2x",   2.0, 10, 3, 6, 8.0,  2.5, 15),
    ("lev2 selective", 2.0, 12, 4, 8, 10.0, 4.0, 12),
]


def all_events(configs):
    """One pass over the cached payload: detect each day's breakout ONCE, sim the trail for every config."""
    syms = A.universe()
    ev = {i: [] for i in range(len(configs))}
    for s in syms:
        z = A.load(s)
        if z is None:
            continue
        date, hm, h, l, c, v = z["date"], z["hm"], z["h"], z["l"], z["c"], z["v"]
        ds = z["day_start"]; day_p20 = z["day_p20"]; n = len(ds); N = len(date)
        for k in range(n):
            p20 = day_p20[k]
            if not np.isfinite(p20):
                continue
            a = int(ds[k]); b = int(ds[k + 1]) if k + 1 < n else N
            dhm = hm[a:b]
            cand = np.where((dhm >= 920) & (dhm <= 1430) & (h[a:b] >= p20))[0]
            if len(cand) == 0:
                continue
            i = int(cand[0])
            if i < 3:
                continue
            dv = v[a:b]; avgv = dv[:i].mean()
            surge = float(dv[i] / avgv) if avgv > 0 else 0.0
            sqc = np.where(dhm <= 1520)[0]
            endi = int(sqc[-1]) if len(sqc) else b - a - 1
            if endi <= i:
                continue
            hh = h[a:b]; ll = l[a:b]; cc = c[a:b]; dt = int(date[a]); ehm = int(dhm[i])
            for ci, cfg in enumerate(configs):
                if surge < cfg[6]:                       # surge_min
                    continue
                lev = cfg[1]
                cap, xj = A.trail_long(hh, ll, cc, i, endi, lev, cfg[2], cfg[3], cfg[4], cfg[5])
                ev[ci].append((s, dt, ehm, int(dhm[xj]), float(cap) - COST_PRICE_RT * lev, surge))
    return ev


def run_all():
    ev = all_events(CONFIGS)
    res = []
    for ci, cfg in enumerate(CONFIGS):
        rows = ev[ci]
        if not rows:
            res.append((cfg[0], None)); continue
        df = pd.DataFrame(rows, columns=["symbol", "date", "entry_hm", "exit_hm", "cap_pct", "surge"])
        dayret = A.slot_portfolio(df, cfg[7])
        m = A.metrics(dayret)
        m["n_trades"] = len(df); m["mean_cap_per_trade"] = round(float(df.cap_pct.mean()), 2)
        m["win_rate"] = round(float((df.cap_pct > 0).mean() * 100), 1)
        res.append((cfg[0], m))
    return res


def main():
    if not (A.CACHE / (A.universe()[0] + ".npz")).exists():
        print("cache missing — run: python scripts/arena_fast.py build"); return
    t0 = time.time()
    res = run_all()
    print(f"{'config':<18}{'trades':>8}{'win%':>6}{'cap/trade':>10}{'CAGR':>8}{'maxDD':>8}{'Calmar':>8}{'avg_mo':>8}{'+mo%':>6}")
    best = None
    for name, m in res:
        if m is None:
            print(f"{name:<18}  no trades"); continue
        print(f"{name:<18}{m['n_trades']:>8}{m['win_rate']:>6}{m['mean_cap_per_trade']:>10}"
              f"{m['cagr']:>7}%{m['max_dd']:>7}%{str(m['calmar']):>8}{m['avg_month']:>7}%{m['pct_pos_months']:>6}")
        if m['cagr'] and (best is None or m['cagr'] > best[1]['cagr']):
            best = (name, m)
    if best:
        json.dump(best[1], open(ROOT / "reports" / "jarvis_best.json", "w"))
        print(f"\n  best config: '{best[0]}'  -> reports/jarvis_best.json")
    print(f"  total {time.time()-t0:.1f}s  (all {len(CONFIGS)} configs on the cached payload)")


if __name__ == "__main__":
    main()
