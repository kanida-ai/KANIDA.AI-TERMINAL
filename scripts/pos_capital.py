"""The honest capital-fairness check. Per-TRADE positional wins ~2.6x, but each trade
locks capital multiple sessions. Compute:
  (1) per-capital-DAY return (net% / sessions held) vs intraday's ~1.9%/session,
  (2) a realistic FIXED-Rs5L LADDERED book: enter Book/SLOTS into a new basket each
      signal day if capital is free; skip if fully deployed (report utilisation);
      monthly return on the fixed Rs5L -> directly comparable to intraday Rs5L/day.
Chosen positional config: basket trail arm3/fl1/give4/stop6, max-hold 3.
"""
import numpy as np
from collections import defaultdict
import pos_sim as P

ARM, FL, GV, ST, MH = 3.0, 1.0, 4.0, 6.0, 3


def positional_trades(ds):
    tr = []
    for m in ds:
        r = P.basket_trail(m, ARM, FL, GV, ST, MH)
        capdays = r["exit_day"] + 1                 # sessions capital was tied up
        exit_date = m["dates"][r["exit_day"]]
        tr.append(dict(entry=m["signal_date"], exit=exit_date, capdays=capdays,
                       net=r["net"], reason=r["reason"]))
    return tr


def intraday_trades():
    """Recompute intraday validated basket (arm2.5/fl1/give1.5/stop3, next-open fills,
    exit EOD) net% per day on the same window, from the intraday dataset."""
    import pickle
    from pathlib import Path
    ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
    mats = pickle.load(open(ROOT / "docs" / "ops" / "_opt_dataset.pkl", "rb"))
    A, Fl, Gv, St = 2.5, 1.0, 1.5, 3.0
    out = []
    for d, M in mats:
        entry = M["entry"].astype(float); n = M["n"]
        alloc = 500000.0 / M["nstocks"]; qty = np.floor(alloc / entry)
        dep = float((entry * qty).sum())
        close = M["close"].astype(float); opn = M["opn"].astype(float)
        ret = (close @ qty - dep) / dep * 100.0
        armed = False; peak = None; xb = n - 1; reason = "EOD"
        for i in range(n - 1):
            r = ret[i]
            if r <= -St: xb, reason = i, "STOP"; break
            if not armed:
                if r >= A: armed = True; peak = r
                continue
            peak = max(peak, r)
            if r <= max(Fl, peak - Gv): xb, reason = i, "TRAIL"; break
        if reason == "EOD":
            exitpx = close[n - 1]
        else:
            nx = opn[xb + 1] if xb + 1 < n else close[xb]
            exitpx = np.where(np.isfinite(nx) & (nx > 0), nx, close[xb])
        exitval = float((exitpx * qty).sum())
        gross = (exitval - dep) / dep * 100.0
        net = P._net(gross, dep)
        out.append(dict(entry=d, net=net))
    return out


def laddered_book(trades, book=500000.0, slots=4):
    """Event-driven: each signal day put book/slots into a new basket if free capital
    allows; realise P&L on exit; attribute realised P&L to the EXIT month. Report
    utilisation (fraction of signal days a new basket was actually opened)."""
    per_slot = book / slots
    open_pos = []            # list of dict(exit, cap, net)
    realised_by_month = defaultdict(float)
    taken = 0; skipped = 0
    days = sorted(set(t["entry"] for t in trades))
    tmap = {t["entry"]: t for t in trades}
    for d in days:
        # free capital that has exited on/before d
        still = []
        free = book
        for p in open_pos:
            if p["exit"] <= d:
                realised_by_month[p["exit"][:7]] += p["cap"] * p["net"] / 100.0
            else:
                still.append(p); free -= p["cap"]
        open_pos = still
        t = tmap[d]
        if free >= per_slot - 1:
            open_pos.append(dict(exit=t["exit"], cap=per_slot, net=t["net"])); taken += 1
        else:
            skipped += 1
    for p in open_pos:       # close out remaining at their exit month
        realised_by_month[p["exit"][:7]] += p["cap"] * p["net"] / 100.0
    return realised_by_month, taken, skipped


def main():
    ds = P.load()
    ptr = positional_trades(ds)
    itr = intraday_trades()

    pnet = np.array([t["net"] for t in ptr]); pcap = np.array([t["capdays"] for t in ptr])
    inet = np.array([t["net"] for t in itr])
    print("=" * 74)
    print("PER-TRADE vs PER-CAPITAL-DAY (net, same window, same Rs5L Top-5 basket)")
    print("=" * 74)
    print(f"  Positional (trail, max-hold 3): mean {pnet.mean():.3f}%/trade | avg {pcap.mean():.2f} "
          f"sessions held | per-capital-day {pnet.sum()/pcap.sum():.3f}%")
    print(f"  Intraday   (validated, EOD)   : mean {inet.mean():.3f}%/trade | 1.00 session held | "
          f"per-capital-day {inet.mean():.3f}%")
    print(f"  -> per TRADE: positional {pnet.mean()/inet.mean():.2f}x  |  "
          f"per CAPITAL-DAY: positional {(pnet.sum()/pcap.sum())/inet.mean():.2f}x")

    # monthly, fixed Rs5L
    pmon, taken, skipped = laddered_book(ptr, slots=4)
    imon = defaultdict(float)
    for t in itr:
        imon[t["entry"][:7]] += 500000.0 * t["net"] / 100.0
    print("\n" + "=" * 74)
    print(f"FIXED Rs5L MONTHLY  (positional = laddered 4-slot book; utilisation "
          f"{taken}/{taken+skipped} = {taken/(taken+skipped)*100:.0f}% of signal days)")
    print("=" * 74)
    print(f"{'month':>9}{'POS Rs':>12}{'POS %':>8}{'INTRA Rs':>12}{'INTRA %':>9}")
    allm = sorted(set(pmon) | set(imon))
    pt = it = 0.0
    for ym in allm:
        pv = pmon.get(ym, 0.0); iv = imon.get(ym, 0.0); pt += pv; it += iv
        print(f"{ym:>9}{pv:>12,.0f}{pv/5000:>7.1f}%{iv:>12,.0f}{iv/5000:>8.1f}%")
    print("-" * 50)
    print(f"{'TOTAL':>9}{pt:>12,.0f}{'':>8}{it:>12,.0f}")
    print(f"\n  Positional book grew Rs{pt:,.0f} on Rs5L over {len(allm)} months "
          f"(avg {pt/5000/len(allm):.1f}%/mo)")
    print(f"  Intraday   book grew Rs{it:,.0f} on Rs5L/day over {len(allm)} months "
          f"(avg {it/5000/len(allm):.1f}%/mo)")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\scripts")
    main()
