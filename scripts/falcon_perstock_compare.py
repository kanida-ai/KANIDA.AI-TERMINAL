"""Head-to-head: BASKET-ONLY vs TWO-LAYER (per-stock stop + basket trail), on the
optimized config (arm 2.5 / floor 1 / giveback 1.5 / stop 3), all 530 days, cash.
Two-layer matches the live logic: per-stock stop at -pstop (fires intra-bar on the
bar LOW), then basket arm/trail on the FROZEN notional (cut legs' realized loss
stays in the numerator; denominator never shrinks). Basket exits fill next-open.
"""
import sys, pickle
sys.path.insert(0, r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\scripts")
from pathlib import Path
from collections import defaultdict
import numpy as np

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
ARM, FLOOR, GIVE, STOP = 2.5, 1.0, 1.5, 3.0


def sim_layered(M, pstop=None):
    """pstop=None -> basket-only; pstop=x% -> two-layer (per-stock stop at -x%)."""
    entry, qty, close, low, opn, dep, n = M["entry"], M["qty"], M["close"], M["low"], M["opn"], M["dep"], M["n"]
    lvl = entry * (1 - pstop / 100.0) if pstop else None
    mask = np.ones(len(entry), bool)
    realized = 0.0; armed = False; peak = None
    for i in range(1, n):
        if pstop is not None:
            for j in np.where(mask)[0]:
                if low[i, j] <= lvl[j]:
                    realized += (lvl[j] - entry[j]) * qty[j]     # fill at the stop level
                    mask[j] = False
        unreal = float(((close[i] - entry) * qty * mask).sum())
        G = (realized + unreal) / dep * 100.0
        trig = (not armed and G <= -STOP) or (armed and G <= max(FLOOR, peak - GIVE))
        if trig:
            no = opn[i + 1] if i + 1 < n else close[i]
            no = np.where(np.isfinite(no) & (no > 0), no, close[i])
            realized += float(((no - entry) * qty * mask).sum())  # exit remaining at next-open
            return realized / dep * 100.0
        if not armed and G >= ARM:
            armed = True; peak = G
        if armed:
            peak = max(peak, G)
    realized += float(((close[-1] - entry) * qty * mask).sum())    # EOD close
    return realized / dep * 100.0


def stats(rets, months):
    a = np.array(rets)
    eq = np.cumsum(a); dd = float((np.maximum.accumulate(eq) - eq).max())
    md = defaultdict(list)
    for r, m in zip(rets, months):
        md[m].append(r)
    grew = sum(1 for v in md.values() if sum(v) >= 0)
    return dict(mean=round(float(a.mean()), 3), median=round(float(np.median(a)), 3),
                pos=round(float((a > 0).mean() * 100), 1), ge1=round(float((a >= 1).mean() * 100), 1),
                worst=round(float(a.min()), 2), total=round(float(a.sum()), 1), maxdd=round(dd, 1),
                months_grew=f"{grew}/{len(md)}")


def main():
    mats = pickle.load(open(ROOT / "docs" / "ops" / "_opt_dataset.pkl", "rb"))
    months = [d[:7] for d, _ in mats]
    variants = {
        "BASKET-ONLY (documented)": [sim_layered(M, None) for _, M in mats],
        "TWO-LAYER per-stock -1.5%": [sim_layered(M, 1.5) for _, M in mats],
        "TWO-LAYER per-stock -2.0%": [sim_layered(M, 2.0) for _, M in mats],
        "TWO-LAYER per-stock -3.0% (=config stop)": [sim_layered(M, 3.0) for _, M in mats],
        "TWO-LAYER per-stock -4.0%": [sim_layered(M, 4.0) for _, M in mats],
    }
    print(f"{'variant':<42}{'mean%':>7}{'pos%':>6}{'d>=1%':>7}{'worst':>7}{'total%':>8}{'maxDD':>7}{'moGrew':>8}")
    for name, rets in variants.items():
        s = stats(rets, months)
        print(f"{name:<42}{s['mean']:>7.3f}{s['pos']:>6}{s['ge1']:>7}{s['worst']:>7}{s['total']:>8.0f}{s['maxdd']:>7}{s['months_grew']:>8}")
    # delta of best two-layer vs basket-only, day by day
    base = np.array(variants["BASKET-ONLY (documented)"])
    for name in ["TWO-LAYER per-stock -3.0% (=config stop)", "TWO-LAYER per-stock -2.0%"]:
        tl = np.array(variants[name])
        diff = tl - base
        print(f"\n{name} vs basket-only: total {tl.sum()-base.sum():+.0f}% | "
              f"days better {int((diff>0.001).sum())} / worse {int((diff<-0.001).sum())} / same {int((abs(diff)<=0.001).sum())}")


if __name__ == "__main__":
    main()
