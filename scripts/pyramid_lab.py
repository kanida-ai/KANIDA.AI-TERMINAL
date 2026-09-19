"""
PYRAMID LAB — CNC (1x, no leverage). You can't pick the monster at entry, but once a trade CONFIRMS
(+30/+60/+100%) it's revealing itself — add to it. Each add is its own slot entered at the higher price,
all trailing together (25% from peak). Capital flows to strength; the N-slot cap enforces "only your own
cash". Measures whether pyramiding lifts return vs the flat base, with an OOS era split.
Run: python scripts/pyramid_lab.py
"""
from __future__ import annotations
import os, sys, time
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import arena_fast as A
import exit_lab as E                                      # reuse load_daily
COST_RT = 0.30; VOL_MIN = 1.5; TRAIL = 25.0; HARD = 7.0; MAXD = 120


def trade(p, k, add_levels):
    """Base entry k+1 + adds when the trade first tags each +level% before exit. All exit on the trail."""
    o, h, l, c = p["o"], p["h"], p["l"], p["c"]
    if k + 1 >= len(c) or not (o[k + 1] > 0):
        return None
    entry = o[k + 1]; peak = entry; hard = entry * (1 - HARD / 100); end = min(k + 1 + MAXD, len(c))
    exit_d = end - 1; exit_px = c[end - 1]
    for d in range(k + 1, end):
        if h[d] > peak: peak = h[d]
        stop = max(hard, peak * (1 - TRAIL / 100))
        if l[d] <= stop:
            exit_d = d; exit_px = stop; break
    di = p["di"]
    slots = [(int(di[k + 1]), int(di[exit_d]), exit_px / entry - 1 - COST_RT / 100)]   # base
    for lv in add_levels:
        tgt = entry * (1 + lv / 100)
        for d in range(k + 2, exit_d + 1):
            if h[d] >= tgt:                                # add at the level, once, before exit
                slots.append((int(di[d]), int(di[exit_d]), exit_px / tgt - 1 - COST_RT / 100))
                break
    return slots


def slot_fund(slots, N, cap0=1_500_000.0):
    ev = []
    for i, (ed, xd, rf) in enumerate(slots):
        ev.append((ed, 0, i)); ev.append((xd, 1, i))
    ev.sort(key=lambda x: (x[0], -x[1]))
    cash = cap0; opencap = {}; curve = {}
    for date, kind, i in ev:
        if kind == 1:
            if i in opencap: cash += opencap.pop(i) * (1 + slots[i][2])
        else:
            if len(opencap) < N and cash > 0:
                s = (cash + sum(opencap.values())) / N
                if 0 < s <= cash: cash -= s; opencap[i] = s
        curve[date] = cash + sum(opencap.values())
    s = pd.Series(curve).sort_index()
    idx = pd.to_datetime(s.index.astype(str), format="%Y%m%d")
    s = pd.Series(s.values, index=idx); s = s[~s.index.duplicated(keep="last")]
    return s.resample("D").ffill().dropna().pct_change().dropna() * 100


def build(add_levels):
    D = E.load_daily(); slots = []
    for sym, p in D.items():
        c = p["c"]; hi60 = p["hi60"]; n = len(c); k = 61
        while k < n - 1:
            if not (np.isfinite(hi60[k]) and c[k] > hi60[k] and c[k - 1] <= hi60[k - 1]):
                k += 1; continue
            va = p["vavg"][k]
            if not (np.isfinite(va) and va > 0) or (p["vol"][k] / va) < VOL_MIN:
                k += 1; continue
            t = trade(p, k, add_levels)
            if t:
                slots.extend(t); adv = 5
                # advance past this trade's base hold
                k = k + 1 + max(1, (pd.Timestamp(str(t[0][1])) - pd.Timestamp(str(t[0][0]))).days // 1 or 5)
            else:
                k += 1
    return slots


def main():
    t0 = time.time()
    configs = [("no pyramid (base only)", []), ("+30/+60/+100", [30, 60, 100]), ("+40/+100", [40, 100])]
    print(f"{'config':<24}{'N':>4}{'slots':>8}{'CAGR':>7}{'maxDD':>8}{'Calmar':>8}{'avg-mo':>8}{'Sharpe':>7}")
    cache = {}
    for name, lv in configs:
        cache[name] = build(lv)
    for name, lv in configs:
        slots = cache[name]
        for N in [15, 10]:
            m = A.metrics(slot_fund(slots, N))
            print(f"{name:<24}{N:>4}{len(slots):>8}{m['cagr']:>6}%{m['max_dd']:>7}%{str(m['calmar']):>8}{m['avg_month']:>7}%{m['sharpe']:>7}")
    # OOS era split for the best config (+30/60/100, N=10)
    slots = pd.DataFrame(cache["+30/+60/+100"], columns=["ed", "xd", "rf"])
    print("\n  OOS era split — pyramid +30/60/100, N=10:")
    for lo, hi, lab in [(20130101, 20200101, "2013-2019"), (20200101, 20230101, "2020-2022"), (20230101, 20270101, "2023-2026 OOS")]:
        s = slots[(slots.ed >= lo) & (slots.ed < hi)]
        if len(s) < 20: continue
        m = A.metrics(slot_fund(list(s.itertuples(index=False, name=None)), 10))
        print(f"    {lab:<16} slots {len(s):>6}  CAGR {m['cagr']}%  DD {m['max_dd']}%  Calmar {m['calmar']}")
    print(f"\n  [{time.time()-t0:.0f}s]")


if __name__ == "__main__":
    main()
