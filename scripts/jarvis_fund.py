"""
JARVIS FUND — one clean, VERIFIED CNC (1x) portfolio for the base-breakout swing + pyramiding.
Fixes the earlier CAGR inconsistency (verifies total vs CAGR agree) and diagnoses capital utilisation
(idle cash is the capital-efficiency lever). Sweeps slot count to find the honest best config.
Run: python scripts/jarvis_fund.py
"""
from __future__ import annotations
import os, sys, time
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import exit_lab as E
CAP0 = 1_000_000.0; COST_RT = 0.30; VOL_MIN = 1.5; TRAIL = 25.0; HARD = 7.0; MAXD = 120
ADDS = [40, 100]                                          # pyramid: add when the trade tags +40%, +100%


def trades(pyramid=True):
    D = E.load_daily(); out = []
    for sym, p in D.items():
        o, h, l, c = p["o"], p["h"], p["l"], p["c"]; hi60 = p["hi60"]; di = p["di"]; n = len(c); k = 61
        while k < n - 1:
            if not (np.isfinite(hi60[k]) and c[k] > hi60[k] and c[k - 1] <= hi60[k - 1]):
                k += 1; continue
            va = p["vavg"][k]
            if not (np.isfinite(va) and va > 0) or (p["vol"][k] / va) < VOL_MIN or not (o[k + 1] > 0):
                k += 1; continue
            entry = o[k + 1]; peak = entry; hard = entry * (1 - HARD / 100); end = min(k + 1 + MAXD, n)
            exd = end - 1; expx = c[end - 1]
            for d in range(k + 1, end):
                if h[d] > peak: peak = h[d]
                stop = max(hard, peak * (1 - TRAIL / 100))
                if l[d] <= stop: exd = d; expx = stop; break
            out.append((int(di[k + 1]), int(di[exd]), expx / entry - 1 - COST_RT / 100))   # base slot
            if pyramid:
                for lv in ADDS:
                    tgt = entry * (1 + lv / 100)
                    for d in range(k + 2, exd + 1):
                        if h[d] >= tgt:
                            out.append((int(di[d]), int(di[exd]), expx / tgt - 1 - COST_RT / 100)); break
            k = exd + 1                                    # non-overlapping per stock (trading-day correct)
    return out


def fund(slots, N, diag=False):
    ev = []
    for i, (ed, xd, rf) in enumerate(slots):
        ev.append((ed, 0, i)); ev.append((xd, 1, i))
    ev.sort(key=lambda x: (x[0], -x[1]))
    cash = CAP0; opencap = {}; curve = {}; taken = 0; util = []
    for date, kind, i in ev:
        if kind == 1:
            if i in opencap: cash += opencap.pop(i) * (1 + slots[i][2])
        else:
            if len(opencap) < N and cash > 0:
                s = (cash + sum(opencap.values())) / N
                if 0 < s <= cash + 1e-6: cash -= s; opencap[i] = s; taken += 1
        curve[date] = cash + sum(opencap.values()); util.append(len(opencap))
    s = pd.Series(curve).sort_index()
    idx = pd.to_datetime(s.index.astype(str), format="%Y%m%d")
    eq = pd.Series(s.values, index=idx); eq = eq[~eq.index.duplicated(keep="last")].resample("D").ffill().dropna()
    yrs = (eq.index[-1] - eq.index[0]).days / 365.25
    total = eq.iloc[-1] / CAP0 - 1
    cagr = (eq.iloc[-1] / CAP0) ** (1 / yrs) - 1
    peak = eq.cummax(); dd = float(((eq - peak) / peak).min())
    mo = eq.resample("ME").last().pct_change().dropna()
    d = {"N": N, "taken": taken, "generated": len(slots), "avg_util": np.mean(util),
         "yrs": round(yrs, 1), "total_%": round(total * 100), "CAGR_%": round(cagr * 100, 1),
         "maxDD_%": round(dd * 100, 1), "calmar": round(cagr / -dd, 2) if dd < 0 else None,
         "sharpe": round(mo.mean() / mo.std() * np.sqrt(12), 2) if mo.std() > 0 else 0}
    return d, eq


def main():
    t0 = time.time()
    print("=== VERIFY: total vs CAGR must agree ===")
    base = trades(pyramid=False); pyr = trades(pyramid=True)
    print(f"base slots {len(base):,}  pyramid slots {len(pyr):,}\n")
    print(f"{'set':<10}{'N':>4}{'taken':>7}{'util':>6}{'total%':>8}{'CAGR%':>7}{'maxDD%':>8}{'Calmar':>8}{'Sharpe':>7}")
    for setname, sl in [("base", base), ("pyramid", pyr)]:
        for N in [5, 8, 10, 12, 15, 20]:
            d, _ = fund(sl, N)
            chk = "" if abs((1 + d['CAGR_%'] / 100) ** d['yrs'] - 1 - d['total_%'] / 100) < 0.05 else "  <-BADmath"
            print(f"{setname:<10}{N:>4}{d['taken']:>7}{d['avg_util']:>5.1f}{d['total_%']:>8}{d['CAGR_%']:>6}%{d['maxDD_%']:>7}%{str(d['calmar']):>8}{d['sharpe']:>7}{chk}")
    print(f"\n  [{time.time()-t0:.0f}s]  (util = avg concurrent positions of N; low util = idle capital)")


if __name__ == "__main__":
    main()
