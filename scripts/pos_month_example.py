"""Worked example of the FINAL positional config + 3-sleeve laddering, on real
May-2026 and June-2026 data. Prints, per month:
  (1) every basket entered that month: entry->exit, hold, exit reason, net%,
  (2) the fixed-Rs5L / 3-sleeve book traced day-by-day (which sleeve, deployed, free),
  (3) the month result.
"""
import numpy as np
import pos_sim as P

ARM, FL, GV, ST, MH = 3.0, 1.0, 4.0, 6.0, 3
BOOK = 500000.0
SLOTS = 3
PER = BOOK / SLOTS   # Rs1,66,667 per sleeve


def trades_for(ds, ym):
    out = []
    for m in ds:
        if not m["signal_date"].startswith(ym):
            continue
        r = P.basket_trail(m, ARM, FL, GV, ST, MH)
        out.append(dict(entry=m["signal_date"], exit=m["dates"][r["exit_day"]],
                        hold=r["exit_day"] + 1, reason=r["reason"], net=round(r["net"], 2),
                        syms=m["syms"]))
    return out


def trace_book(all_trades, ym):
    """Day-by-day 3-sleeve book over the calendar days of month ym. all_trades = every
    basket (entered any month) so we correctly free sleeves opened before the month."""
    tmap = {}
    for t in all_trades:
        tmap.setdefault(t["entry"], []).append(t)
    # calendar days that matter = union of entry+exit dates
    days = sorted(set([t["entry"] for t in all_trades] + [t["exit"] for t in all_trades]))
    sleeves = []       # list of dict(entry, exit, net, cap)
    booked = 0.0
    rows = []
    for d in days:
        # free sleeves exiting on/before d
        exits_today = [s for s in sleeves if s["exit"] == d]
        for s in exits_today:
            booked += s["cap"] * s["net"] / 100.0
        sleeves = [s for s in sleeves if s["exit"] > d]
        used = sum(s["cap"] for s in sleeves)
        free = BOOK - used
        opened = None
        if d in tmap and free >= PER - 1:
            t = tmap[d][0]
            sleeves.append(dict(entry=d, exit=t["exit"], net=t["net"], cap=PER))
            opened = t; used += PER; free -= PER
        if d.startswith(ym):
            rows.append(dict(date=d, opened=opened, exits=exits_today,
                             nopen=len(sleeves), used=used, free=free, booked=booked))
    return rows


def main():
    ds = P.load()
    allt = []
    for m in ds:
        r = P.basket_trail(m, ARM, FL, GV, ST, MH)
        allt.append(dict(entry=m["signal_date"], exit=m["dates"][r["exit_day"]], net=round(r["net"], 2), syms=m["syms"]))
    for ym in ["2026-05", "2026-06"]:
        tr = trades_for(ds, ym)
        print("=" * 92)
        print(f"MONTH {ym}  —  {len(tr)} baskets entered")
        print("=" * 92)
        print(f"{'entry':>11}{'exit':>11}{'hold':>5}{'reason':>8}{'net%':>7}   symbols")
        for t in tr:
            print(f"{t['entry']:>11}{t['exit']:>11}{t['hold']:>5}{t['reason']:>8}{t['net']:>7.2f}   "
                  f"{', '.join(t['syms'])}")
        nets = np.array([t["net"] for t in tr])
        print(f"\n  avg net%/basket {nets.mean():.2f} | win {int((nets>0).sum())}/{len(nets)} "
              f"({(nets>0).mean()*100:.0f}%) | best {nets.max():.2f} | worst {nets.min():.2f}")
        # laddered book trace
        rows = trace_book(allt, ym)
        print(f"\n  --- fixed Rs5L / 3-sleeve book, day-by-day (each sleeve = Rs{PER:,.0f}) ---")
        print(f"  {'date':>11}{'open sleeve':>26}{'exits (freed)':>28}{'#open':>6}{'deployed':>10}{'free':>9}")
        for r in rows:
            op = (r["opened"]["syms"][0] + f" +{len(r['opened']['syms'])-1}") if r["opened"] else "-"
            ex = ", ".join(f"{s['entry'][5:]}({s['net']:+.1f}%)" for s in r["exits"]) if r["exits"] else "-"
            print(f"  {r['date']:>11}{op:>26}{ex:>28}{r['nopen']:>6}{r['used']:>10,.0f}{r['free']:>9,.0f}")
        # realized P&L from baskets ENTERED this month (simple, per-sleeve)
        pnl_entered = sum(PER * t["net"] / 100.0 for t in tr)
        print(f"\n  peak deployed this month: Rs{max(r['used'] for r in rows):,.0f} (cap Rs5,00,000)")
        print(f"  P&L from baskets entered this month (Rs{PER:,.0f}/sleeve): Rs{pnl_entered:,.0f} "
              f"= {pnl_entered/BOOK*100:.1f}% on Rs5L\n")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\scripts")
    main()
