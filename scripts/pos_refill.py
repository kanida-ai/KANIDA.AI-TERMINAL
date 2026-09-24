"""Q4 REFILL — explicit sim. When a name hits its per-stock stop, ROTATE the freed
capital into a fresh Falcon name (the highest-rank pick, on the stop date, not already
held) and ride it to the basket's exit. Compare: basket-only vs per-stock-stop->cash
vs per-stock-stop->refill.

Mechanics (documented approximations):
 - Stop detection is at 1-MINUTE (stock close breaches entry*(1-pstop%)).
 - Replacement is chosen from the STOP DATE's own Falcon Top-5 (by_date[cd]); entry at
   that date's CLOSE (1-bar-after-stop approximation, avoids cross-basket minute-align),
   exit at the ORIGINAL basket's exit date CLOSE. Return tracked at day-close granularity.
 - One-level refill (a replacement is not itself re-stopped). Freed capital = qty*stop-close.
 - If the stop date is not itself a signal day, or no unheld name exists -> freed goes to
   cash (no replacement), same as the no-refill arm.
"""
import numpy as np
import pos_sim as P

ALIAS = {"ZOMATO": "ETERNAL"}
ARM, FL, GV, ST, MH = 3.0, 1.0, 4.0, 6.0, 3


def by_date_map(ds):
    return {m["signal_date"]: m for m in ds}


def _repl_return(Mcd, col, cd, de):
    """Replacement return from cd-close to de-close using Mcd's day-close (eod) grid."""
    dates = Mcd["dates"]; eod = Mcd["eod"]
    k0 = 0                                   # cd == Mcd signal_date -> day index 0
    kd = dates.index(de) if de in dates else len(dates) - 1
    c0 = float(Mcd["C"][eod[k0], col]); cN = float(Mcd["C"][eod[kd], col])
    return cN / c0 - 1.0 if c0 > 0 else 0.0


def refill_trail(M, pstop, by_date, refill=True):
    q = M["qty"].astype(np.float64); dep = M["dep"]; e = M["entry"].astype(np.float64)
    C = M["C"].astype(np.float64); O = M["O"].astype(np.float64)
    day = M["day"]; eod = M["eod"]
    kcap = min(MH, int(day.max())); last = int(eod[kcap])
    lvl = e * (1 - pstop / 100.0)
    mask = np.ones(len(e), bool); realized = 0.0
    held = set(ALIAS.get(s, s) for s in M["syms"])
    pending = []                             # (freed, cd, Mcd, col) resolved at exit
    armed = False; peak = None

    def resolve(de):
        tot = 0.0
        for freed, cd, Mcd, col in pending:
            tot += freed * _repl_return(Mcd, col, cd, de)
        return tot

    for i in range(last):
        for j in np.where(mask)[0]:
            if C[i, j] <= lvl[j]:
                realized += (C[i, j] - e[j]) * q[j]; mask[j] = False
                if refill:
                    cd = M["dates"][int(day[i])]; Mcd = by_date.get(cd)
                    if Mcd is not None:
                        col = None
                        for cc, s in enumerate(Mcd["syms"]):
                            if ALIAS.get(s, s) not in held:
                                col = cc; held.add(ALIAS.get(s, s)); break
                        if col is not None:
                            pending.append((q[j] * C[i, j], cd, Mcd, col))
        unreal = float(((C[i] - e) * q * mask).sum())
        G = (realized + unreal) / dep * 100.0
        trig = (not armed and G <= -ST) or (armed and G <= max(FL, (peak if peak else -1e9) - GV))
        if trig:
            j2 = i + 1; px = O[j2] if j2 <= last else C[i]
            realized += float(((px - e) * q * mask).sum())
            de = M["dates"][int(day[i])]
            realized += resolve(de)
            return dict(net=P._net(realized / dep * 100.0, dep), reason="STOP" if not armed else "TRAIL",
                        exit_day=int(day[i]))
        if not armed and G >= ARM:
            armed = True; peak = G
        if armed:
            peak = max(peak, G)
    realized += float(((C[last] - e) * q * mask).sum())
    realized += resolve(M["dates"][int(day[last])])
    return dict(net=P._net(realized / dep * 100.0, dep), reason="TIME", exit_day=int(day[last]))


def main():
    ds = P.load(); dates = [m["signal_date"] for m in ds]; bd = by_date_map(ds)
    print("Q4 REFILL — rotate freed capital into a fresh Falcon name on stop-out")
    print(f"Basket trail arm{ARM}/fl{FL}/give{GV}/stop{ST}, max-hold {MH}\n")
    print(f"{'variant':<32}{'mean%':>7}{'%pos':>6}{'%>=2':>6}{'worst':>7}{'total%':>8}{'maxDD':>7}{'moGrew':>8}")
    base = [P.basket_trail(m, ARM, FL, GV, ST, MH)["net"] for m in ds]
    s = P.summarize(base, dates)
    print(f"{'BASKET-ONLY (chosen)':<32}{s['mean']:>7.3f}{s['pos']:>5.1f}%{s['ge2']:>5.1f}%{s['worst']:>7.2f}{s['total']:>8.0f}{s['maxdd']:>7.1f}{s['months_grew']:>8}")
    for ps in [3.0, 4.0, 5.0]:
        cash = [refill_trail(m, ps, bd, refill=False)["net"] for m in ds]
        fill = [refill_trail(m, ps, bd, refill=True)["net"] for m in ds]
        sc = P.summarize(cash, dates); sf = P.summarize(fill, dates)
        print(f"{'stop -'+str(int(ps))+'% -> CASH':<32}{sc['mean']:>7.3f}{sc['pos']:>5.1f}%{sc['ge2']:>5.1f}%{sc['worst']:>7.2f}{sc['total']:>8.0f}{sc['maxdd']:>7.1f}{sc['months_grew']:>8}")
        print(f"{'stop -'+str(int(ps))+'% -> REFILL':<32}{sf['mean']:>7.3f}{sf['pos']:>5.1f}%{sf['ge2']:>5.1f}%{sf['worst']:>7.2f}{sf['total']:>8.0f}{sf['maxdd']:>7.1f}{sf['months_grew']:>8}")
    print("\n(refill vs cash shows whether rotating into a fresh name beats sitting in cash after a stop)")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\scripts")
    main()
