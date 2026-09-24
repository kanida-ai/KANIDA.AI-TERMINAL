"""Day-by-day intraday backtest results for the live-comparison week (2026-07-06..10).
Validated config: Top-5 @09:15 open, equal split of Rs5L, basket-only trail
arm2.5/floor1/giveback1.5/stop-3, square-off 15:29, next-open fills, net of ~0.10% RT."""
import pickle
from pathlib import Path
import numpy as np
ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
ARM, FLOOR, GIVE, STOP = 2.5, 1.0, 1.5, 3.0
ASSIGNED = 500000.0
CHARGE = 0.0005


def basket_exit_q(close, qty, dep, n):
    ret = (close @ qty - dep) / dep * 100.0
    armed = False; peak = None
    for i in range(n - 1):
        r = ret[i]
        if r <= -STOP:
            return i, "STOP"
        if not armed:
            if r >= ARM:
                armed = True; peak = r
            continue
        peak = max(peak, r)
        if r <= max(FLOOR, peak - GIVE):
            return i, ("FLOOR" if max(FLOOR, peak - GIVE) == FLOOR else "TRAIL")
    return n - 1, "EOD"


def main():
    mats = pickle.load(open(ROOT / "docs" / "ops" / "_opt_dataset.pkl", "rb"))
    print("INTRADAY BACKTEST — live-comparison week (validated config, Rs5,00,000, net of cost)\n")
    print(f"{'date':>11}{'exit':>7}{'reason':>7}{'gross%':>8}{'NET%':>7}{'NET Rs':>9}{'deployed':>9}  symbols(rank)")
    tot = 0.0
    for D, M in mats:
        if D < "2026-07-06" or D > "2026-07-10":
            continue
        entry = M["entry"].astype(float); n = M["n"]; nstk = M["nstocks"]
        qty = np.floor((ASSIGNED / nstk) / entry); dep = float((entry * qty).sum())
        close = M["close"].astype(float); opn = M["opn"].astype(float); grid = M["grid"]
        xb, reason = basket_exit_q(close, qty, dep, n)
        if reason == "EOD":
            exitpx = close[n - 1]; xt = grid[n - 1]
        else:
            nx = opn[xb + 1] if xb + 1 < n else close[xb]
            exitpx = np.where(np.isfinite(nx) & (nx > 0), nx, close[xb]); xt = grid[min(xb + 1, n - 1)]
        exitval = float((exitpx * qty).sum())
        gross = (exitval - dep) / dep * 100.0
        charges = (dep + exitval) * CHARGE
        net = gross - charges / dep * 100.0
        netrs = dep * net / 100.0
        tot += netrs
        symstr = ", ".join(f"{s}({r})" for s, r in zip(M["syms"], M["rank"]))
        print(f"{D:>11}{xt:>7}{reason:>7}{gross:>8.2f}{net:>7.2f}{netrs:>9,.0f}{dep:>9,.0f}  {symstr}")
    print(f"\n  WEEK TOTAL net P&L on Rs5L (additive, fresh Rs5L/day): Rs{tot:,.0f}  ({tot/5000:.1f}% of Rs5L/day basis)")

    # per-stock detail
    print("\nPER-STOCK DETAIL (entry 09:15 open -> basket exit fill):")
    print(f"{'date':>11}{'rank':>5}  {'symbol':<12}{'entry':>9}{'exit':>9}{'stock%':>8}{'qty':>6}{'pnl Rs':>9}")
    for D, M in mats:
        if D < "2026-07-06" or D > "2026-07-10":
            continue
        entry = M["entry"].astype(float); n = M["n"]; nstk = M["nstocks"]
        qty = np.floor((ASSIGNED / nstk) / entry); dep = float((entry * qty).sum())
        close = M["close"].astype(float); opn = M["opn"].astype(float)
        xb, reason = basket_exit_q(close, qty, dep, n)
        if reason == "EOD":
            exitpx = close[n - 1]
        else:
            nx = opn[xb + 1] if xb + 1 < n else close[xb]
            exitpx = np.where(np.isfinite(nx) & (nx > 0), nx, close[xb])
        for j in range(nstk):
            e = entry[j]; xp = float(exitpx[j]); q = int(qty[j])
            print(f"{D:>11}{M['rank'][j]:>5}  {M['syms'][j]:<12}{e:>9.2f}{xp:>9.2f}{(xp/e-1)*100:>8.2f}{q:>6}{q*(xp-e):>9,.0f}")
        print()


if __name__ == "__main__":
    main()
