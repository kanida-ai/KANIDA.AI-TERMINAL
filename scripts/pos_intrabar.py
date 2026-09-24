"""Positional: CLOSE vs intra-candle TOUCH, same as the intraday check, now that the
positional dataset carries high/low. Config = chosen basket trail arm3/fl1/give4/stop6,
max-hold 3.

 CLOSE model  = pos_sim.basket_trail (triggers on the 1-min CLOSE, fills next-bar open).
 TOUCH model  = stop fires when the basket LOW touches -6; arm/peak track the basket HIGH;
                trail fires when the basket LOW touches (peak-give or floor); fill at level.

Same caveat as intraday: basket-of-per-stock-HIGH/LOW OVERSTATES the true intra-minute basket
range, so TOUCH is the pessimistic-range bound and CLOSE the optimistic one -> the pair BRACKETS
the true live behaviour. Positional's wider thresholds (stop6/give4) should make the gap smaller
than intraday's (stop3/give1.5).
"""
import numpy as np
import pos_sim as P

ARM, FL, GV, ST, MH = 3.0, 1.0, 4.0, 6.0, 3


def touch(M):
    q = M["qty"].astype(float); dep = M["dep"]
    retC = (M["C"].astype(float) @ q - dep) / dep * 100.0
    retH = (M["H"].astype(float) @ q - dep) / dep * 100.0
    retL = (M["L"].astype(float) @ q - dep) / dep * 100.0
    day = M["day"]; eod = M["eod"]
    kcap = min(MH, int(day.max())); last = int(eod[kcap])
    armed = False; peak = None
    for i in range(last):
        if not armed:
            if retL[i] <= -ST: return P._net(-ST, dep), int(day[i])
            if retH[i] >= ARM: armed = True; peak = retH[i]
            continue
        peak = max(peak, retH[i])
        thr = max(FL, peak - GV)
        if retL[i] <= -ST: return P._net(-ST, dep), int(day[i])
        if retL[i] <= thr: return P._net(thr, dep), int(day[i])
    return P._net(float(retC[last]), dep), int(day[last])


def main():
    ds = P.load()
    if "H" not in ds[0]:
        print("[!] dataset has no H/L — rebuild pos_build_dataset.py first."); return
    dates = [m["signal_date"] for m in ds]
    cl = [P.basket_trail(m, ARM, FL, GV, ST, MH)["net"] for m in ds]
    tc = [touch(m)[0] for m in ds]
    wicks = []
    for m in ds:
        q = m["qty"].astype(float); dep = m["dep"]
        last = int(m["eod"][min(MH, int(m["day"].max()))])
        retH = (m["H"].astype(float) @ q - dep) / dep * 100.0
        retL = (m["L"].astype(float) @ q - dep) / dep * 100.0
        wicks.append(float(np.mean(retH[:last] - retL[:last])))
    print(f"Positional basket, {len(ds)} days. Avg 1-min intra-candle basket range: "
          f"{np.mean(wicks):.3f} pts (median {np.median(wicks):.3f})\n")
    print(f"{'model':<28}{'mean%':>8}{'%pos':>7}{'%>=2':>7}{'worst':>8}{'total%':>9}{'maxDD':>8}")
    for name, a in [("CLOSE (backtest)", cl), ("TOUCH (live-like, eager)", tc)]:
        s = P.summarize(a, dates)
        print(f"{name:<28}{s['mean']:>8.3f}{s['pos']:>6.1f}%{s['ge2']:>6.1f}%{s['worst']:>8.2f}{s['total']:>9.0f}{s['maxdd']:>8.1f}")
    dc = np.array(cl); dt = np.array(tc); diff = dt - dc
    print(f"\nPer-trade delta (touch - close): mean {diff.mean():+.3f} pts | "
          f"trades changed {(np.abs(diff) > 0.001).sum()}/{len(diff)} | "
          f"touch worse on {(diff < -0.001).sum()} / better on {(diff > 0.001).sum()}")
    print("True live number sits between these two rows.")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\scripts")
    main()
