"""Laddering sensitivity — EXACTLY how the fixed-Rs5L monthly figure is modelled, and how
it changes with the capital assumption. This is the load-bearing assumption behind 24.9%/mo.

Every variant is a SINGLE Rs5L account (never more) EXCEPT the last 'unconstrained' row,
which is shown only to prove we did NOT assume extra capital.

  slots=S  -> book split into S sleeves of Rs5L/S; each signal day open ONE new sleeve if
              a sleeve is free; up to S baskets run CONCURRENTLY but total <= Rs5L.
  slots=1  -> single Rs5L basket ROTATING (one at a time; skip signals while in a trade).
  unconstrained -> full Rs5L into a NEW basket EVERY signal day, no cap (needs >>Rs5L).
"""
import numpy as np
from collections import defaultdict
import pos_sim as P
from pos_capital import positional_trades

BOOK = 500000.0


def ladder(trades, slots, cap=True):
    per = BOOK / slots
    days = sorted(set(t["entry"] for t in trades)); tmap = {t["entry"]: t for t in trades}
    openp = []; realised = defaultdict(float); taken = skipped = 0
    dep_series = []; conc_series = []; peak = 0.0
    for d in days:
        still = []; used = 0.0
        for p in openp:
            if p["exit"] <= d:
                realised[p["exit"][:7]] += p["cap"] * p["net"] / 100.0
            else:
                still.append(p); used += p["cap"]
        openp = still
        free = (BOOK - used) if cap else 1e18
        t = tmap[d]
        if (not cap) or free >= per - 1:
            openp.append(dict(exit=t["exit"], cap=per, net=t["net"])); taken += 1; used += per
        else:
            skipped += 1
        dep_series.append(used); conc_series.append(len(openp)); peak = max(peak, used)
    for p in openp:
        realised[p["exit"][:7]] += p["cap"] * p["net"] / 100.0
    nmo = len(realised)
    total = sum(realised.values())
    return dict(slots=slots, per=per, taken=taken, skipped=skipped,
                util=taken / (taken + skipped) * 100, avgdep=float(np.mean(dep_series)),
                avgconc=float(np.mean(conc_series)), peak=peak, total=total,
                permo=total / BOOK / nmo * 100, nmo=nmo)


def main():
    ds = P.load(); ptr = positional_trades(ds)
    print("=" * 100)
    print("LADDERING SENSITIVITY — fixed Rs5,00,000 account (per-trade edge is 4.99% net over ~3.63 sessions)")
    print("=" * 100)
    print(f"{'variant':<34}{'per-basket':>11}{'util%':>7}{'avgConc':>8}{'avgDeploy':>11}{'peakDeploy':>11}{'Rs P&L':>12}{'%/mo':>7}")
    rows = [("single Rs5L rotating (slots=1)", ladder(ptr, 1)),
            ("laddered 2 sleeves", ladder(ptr, 2)),
            ("laddered 3 sleeves", ladder(ptr, 3)),
            ("laddered 4 sleeves  <== reported", ladder(ptr, 4)),
            ("laddered 5 sleeves", ladder(ptr, 5)),
            ("UNCONSTRAINED full-Rs5L/day", ladder(ptr, 1, cap=False))]
    for name, r in rows:
        print(f"{name:<34}{r['per']:>11,.0f}{r['util']:>7.0f}{r['avgconc']:>8.2f}"
              f"{r['avgdep']:>11,.0f}{r['peak']:>11,.0f}{r['total']:>12,.0f}{r['permo']:>7.1f}")
    print("\nReading it:")
    print(" - The reported 24.9%/mo = '4 sleeves': up to 4 overlapping baskets of Rs1.25L, total CAPPED at Rs5L.")
    print("   Avg deployed < Rs5L because you can't always fill the 4th sleeve -> ~91% invested.")
    print(" - 'single Rs5L rotating' is the most real-account-simple: one basket at a time, <=Rs5L, never overlaps.")
    print(" - 'unconstrained' needs its peakDeploy in capital (>>Rs5L) — shown only to prove it is NOT what we used.")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\scripts")
    main()
