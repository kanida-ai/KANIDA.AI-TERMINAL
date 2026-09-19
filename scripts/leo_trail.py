"""
LEO + PROFIT TRAILING. Plain Leo holds each pick a full month with no intra-month exit, so a name that
tops out and rolls over gives all its gains back before rebalance. Here each held position gets a daily
trailing stop (position-specific risk control, NOT market timing — the lever that should actually work).

Per pick, held from entry open (t+1) until the earlier of:
  (a) trailing stop hit  -> exit at the stop level (conservative fill on the day low), or
  (b) next monthly rebalance open.
Variants:
  hard H%        : fixed stop at entry-H% (no trail).
  trail T%       : trail T% below the running peak from entry.
  armed A/T      : hard stop -8% until +A% in profit, then switch to a T% trail (classic 'profit trail').
Costs: 0.30%/side => 0.60% round-trip per pick (conservative: trailing forces a full cycle each month).

Reuses Leo's exact monthly picks (126d/M/top3/12). Fast (numpy daily path). Compares to base + OOS split.
Run: python scripts/leo_trail.py
"""
from __future__ import annotations
import sys, time
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import daily_core as DC, leo as LEO
COST_RT = 0.60 / 100          # 0.30% each side, full round trip per pick


def sim_pick(o, h, l, ei, nxt, p, mode, T, H, A):
    """Return net fractional return for one pick held [ei..nxt] under the given exit mode."""
    entry = o[ei, p]
    if not (entry > 0):
        return None
    peak = entry
    armed = (mode != "armed")           # armed=False means still on hard stop until +A%
    hardstop = entry * (1 - H / 100) if H else 0.0
    for d in range(ei + 1, nxt + 1):
        hi = h[d, p]; lo = l[d, p]
        if hi > peak:
            peak = hi
        if mode == "hard":
            stop = hardstop
        elif mode == "trail":
            stop = peak * (1 - T / 100)
        else:  # armed
            if not armed and hi >= entry * (1 + A / 100):
                armed = True
            stop = (peak * (1 - T / 100)) if armed else hardstop
        if lo <= stop and stop > 0:
            return stop / entry - 1 - COST_RT
        if hi <= 0:                     # bad data guard
            continue
    # exit at next rebalance open
    ox = o[nxt, p]
    if not (ox > 0):
        return None
    return ox / entry - 1 - COST_RT


def leo_picks(o, c, dvol20, sec, lb=126, freq="M", topk=3, nper=12):
    """Yield (edate, ei, nxt, [col indices]) for each rebalance using Leo's exact selection."""
    evs = LEO._events(o, c, dvol20, sec, lb, freq)         # gives per-rebalance eligible mom+sector
    A = LEO._arrays(o, c, dvol20, sec)
    dates = A["dates"]; pos = {d: i for i, d in enumerate(dates)}
    rd = LEO.rebal_dates(dates, freq)
    ent = [(pos[d], pos[d] + 1) for d in rd if d in pos and pos[d] + 1 < len(dates) and pos[d] - lb >= 0]
    ei_of = {dates[ei]: (ei, None) for (si, ei) in ent}
    ent_sorted = [(si, ei) for (si, ei) in ent]
    # map edate -> nxt entry idx
    nxt_of = {}
    for j in range(len(ent_sorted) - 1):
        nxt_of[dates[ent_sorted[j][1]]] = ent_sorted[j + 1][1]
    out = []
    for (edate, idx, m, s_, med, cnt, oe_row, ox_row) in evs:
        order = np.argsort(med)[::-1]
        lead = [k for k in order if cnt[k] >= 3][:topk]
        leadset = set(lead)
        mask = np.array([sc in leadset for sc in s_])
        cidx = idx[mask]; cm = m[mask]
        take = cidx[np.argsort(cm)[::-1][:nper]]
        ei = pos[edate]; nxt = nxt_of.get(edate)
        if nxt is None:
            continue
        out.append((edate, ei, nxt, take))
    return out


def run_trail(picks, o, h, l, mode, T=20, H=8, A=8):
    rets = {}
    for edate, ei, nxt, take in picks:
        pr = [sim_pick(o, h, l, ei, nxt, int(p), mode, T, H, A) for p in take]
        pr = [x for x in pr if x is not None]
        if pr:
            rets[edate] = float(np.mean(pr))
    s = pd.Series(rets).sort_index(); s.index = pd.to_datetime(s.index)
    return s


def met_of(net):
    eq = 1e6 * (1 + net).cumprod(); return DC.curve_metrics(eq, cap0=1e6)


def main():
    t0 = time.time()
    o, c, dvol20, sec = LEO.load()
    fields, _ = DC.wide_all()
    O, H_, L = fields["o"].values, fields["h"].values, fields["l"].values
    picks = leo_picks(o, c, dvol20, sec)

    base = LEO.run(o, c, dvol20, sec, lb=126, freq="M", topk=3, nper=12)
    mbase = met_of(base)
    print("=== LEO + PROFIT TRAILING (best config 126d/M/top3/12, net 0.60% RT/pick) ===")
    print(f"  {'exit rule':<22}{'CAGR%':>8}{'maxDD%':>8}{'Calmar':>7}{'Shrp':>6}")
    print(f"  {'base: month hold':<22}{mbase['CAGR_%']:>7}%{mbase['maxDD_%']:>7}%{str(mbase['calmar']):>7}{mbase['sharpe_m']:>6}")
    results = {"base": (mbase, base)}
    for T in [10, 15, 20, 25]:
        s = run_trail(picks, O, H_, L, "trail", T=T)
        m = met_of(s); results[f"trail {T}%"] = (m, s)
        print(f"  {'trail '+str(T)+'%':<22}{m['CAGR_%']:>7}%{m['maxDD_%']:>7}%{str(m['calmar']):>7}{m['sharpe_m']:>6}")
    for H in [8, 12]:
        s = run_trail(picks, O, H_, L, "hard", H=H)
        m = met_of(s); results[f"hard {H}%"] = (m, s)
        print(f"  {'hard stop '+str(H)+'%':<22}{m['CAGR_%']:>7}%{m['maxDD_%']:>7}%{str(m['calmar']):>7}{m['sharpe_m']:>6}")
    for A in [8, 15]:
        for T in [15, 20]:
            s = run_trail(picks, O, H_, L, "armed", T=T, H=8, A=A)
            m = met_of(s); results[f"arm+{A}/tr{T}"] = (m, s)
            print(f"  {'arm +'+str(A)+'% then trail '+str(T)+'%':<22}{m['CAGR_%']:>7}%{m['maxDD_%']:>7}%{str(m['calmar']):>7}{m['sharpe_m']:>6}")

    best = max(results.items(), key=lambda kv: (kv[1][0].get("calmar") or -9))
    bname, (bm, bs) = best
    print(f"\n=== BEST by Calmar: '{bname}'  CAGR {bm['CAGR_%']}%  DD {bm['maxDD_%']}%  Calmar {bm['calmar']}  "
          f"(base Calmar {mbase['calmar']}) ===")
    print("  OOS era split (best vs base):")
    print(f"  {'era':<16}{'base CAGR/DD':>18}{'trailed CAGR/DD':>20}")
    for lo, hi, lab in [("2014-01-01", "2020-01-01", "2014-2019"), ("2020-01-01", "2023-01-01", "2020-2022"),
                        ("2023-01-01", "2027-01-01", "2023-2026 OOS")]:
        b = base[(base.index >= lo) & (base.index < hi)]
        s = bs[(bs.index >= lo) & (bs.index < hi)]
        if len(b) >= 8:
            mb, ms = met_of(b), met_of(s)
            print(f"  {lab:<16}{str(mb['CAGR_%'])+'% / '+str(mb['maxDD_%'])+'%':>18}"
                  f"{str(ms['CAGR_%'])+'% / '+str(ms['maxDD_%'])+'%':>20}")
    print(f"\n  [{time.time()-t0:.0f}s]")


if __name__ == "__main__":
    main()
