"""How much does candle-CLOSE vs intra-candle HIGH/LOW change the basket trail?
Runs on the INTRADAY dataset (which keeps high/low). Two models, same config
(arm2.5/floor1/give1.5/stop3):

 CLOSE model (what the validated backtest used): trigger on the 1-min CLOSE, fill next-open.
 TOUCH model (live-like, eager): stop fires when the basket LOW touches -stop (fill at the
   level); arm/peak track the basket HIGH; trail fires when the basket LOW touches (peak-give
   or floor), fill at the level.

CAVEAT: basket-of-per-stock-HIGH/LOW OVERSTATES the true intra-minute basket range (the five
names don't hit their extremes at the same instant), so TOUCH is a PESSIMISTIC-range bound and
CLOSE is an OPTIMISTIC-range bound. True live behaviour sits BETWEEN the two -> this brackets it.
"""
import pickle
from pathlib import Path
from collections import defaultdict
import numpy as np

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
ARM, FLOOR, GIVE, STOP = 2.5, 1.0, 1.5, 3.0
ASSIGNED = 500000.0
CHARGE_SIDE = 0.0005


def net(gross):
    ev = ASSIGNED * (1 + gross / 100.0)
    return gross - (ASSIGNED + ev) * CHARGE_SIDE / ASSIGNED * 100.0


def prep(M):
    entry = M["entry"].astype(float)
    qty = np.floor((ASSIGNED / M["nstocks"]) / entry)
    dep = float((entry * qty).sum())
    retC = (M["close"].astype(float) @ qty - dep) / dep * 100.0
    retO = (M["opn"].astype(float) @ qty - dep) / dep * 100.0
    retH = (M["high"].astype(float) @ qty - dep) / dep * 100.0
    retL = (M["low"].astype(float) @ qty - dep) / dep * 100.0
    return retC, retO, retH, retL, M["n"]


def close_model(M):
    retC, retO, _, _, n = prep(M)
    armed = False; peak = None
    for i in range(n - 1):
        r = retC[i]
        if r <= -STOP: return net(retO[i + 1] if np.isfinite(retO[i + 1]) else retC[i])
        if not armed:
            if r >= ARM: armed = True; peak = r
            continue
        peak = max(peak, r)
        if r <= max(FLOOR, peak - GIVE): return net(retO[i + 1] if np.isfinite(retO[i + 1]) else retC[i])
    return net(retC[n - 1])


def touch_model(M):
    retC, _, retH, retL, n = prep(M)
    armed = False; peak = None
    for i in range(n - 1):
        if not armed:
            if retL[i] <= -STOP: return net(-STOP)                 # stop touched
            if retH[i] >= ARM: armed = True; peak = retH[i]        # arm on the high
            continue
        peak = max(peak, retH[i])
        thr = max(FLOOR, peak - GIVE)
        if retL[i] <= -STOP: return net(-STOP)
        if retL[i] <= thr: return net(thr)                         # trail touched
    return net(retC[n - 1])


def stats(a):
    a = np.array(a)
    eq = np.cumsum(a); dd = float((np.maximum.accumulate(eq) - eq).max())
    return dict(mean=round(float(a.mean()), 3), pos=round(float((a > 0).mean() * 100), 1),
                ge1=round(float((a >= 1).mean() * 100), 1), worst=round(float(a.min()), 2),
                total=round(float(a.sum()), 0), maxdd=round(dd, 1))


def main():
    mats = pickle.load(open(ROOT / "docs" / "ops" / "_opt_dataset.pkl", "rb"))
    cl = [close_model(M) for _, M in mats]
    tc = [touch_model(M) for _, M in mats]
    # intra-minute basket range (how big are the 1-min wicks?)
    ranges = []
    for _, M in mats:
        _, _, retH, retL, n = prep(M)
        ranges.append(float(np.mean(retH[:n] - retL[:n])))
    print(f"Intraday basket, {len(mats)} days. Avg 1-min intra-candle basket range: "
          f"{np.mean(ranges):.3f} pts (median {np.median(ranges):.3f})\n")
    print(f"{'model':<28}{'mean%':>8}{'%pos':>7}{'%>=1':>7}{'worst':>8}{'total%':>9}{'maxDD':>8}")
    for name, a in [("CLOSE (validated backtest)", cl), ("TOUCH (live-like, eager)", tc)]:
        s = stats(a)
        print(f"{name:<28}{s['mean']:>8.3f}{s['pos']:>6.1f}%{s['ge1']:>6.1f}%{s['worst']:>8.2f}{s['total']:>9.0f}{s['maxdd']:>8.1f}")
    dc = np.array(cl); dt = np.array(tc); diff = dt - dc
    print(f"\nPer-day delta (touch - close): mean {diff.mean():+.3f} pts | "
          f"days changed {(np.abs(diff) > 0.001).sum()}/{len(diff)} | "
          f"touch worse on {(diff < -0.001).sum()} / better on {(diff > 0.001).sum()}")
    print("The true live number sits between these two rows (see caveat in the header).")


if __name__ == "__main__":
    main()
