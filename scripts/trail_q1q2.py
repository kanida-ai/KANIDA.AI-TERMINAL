"""Q1: can re-tuning the CURRENT arm/floor/give/stop knobs achieve the 'adaptive ratchet'?
Q2: stock-level vs portfolio(basket)-level trailing — does a per-stock catastrophe stop cost return?
Intraday _opt_dataset, Rs5L, net of ~0.10% RT. Returns on PRICE/deployed basis (x leverage = capital)."""
import pickle
from pathlib import Path
from collections import defaultdict
import numpy as np
ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
CH = 0.0005


def net(g):
    return g - (2 + g / 100.0) * CH * 100.0


def load():
    mats = pickle.load(open(ROOT / "docs" / "ops" / "_opt_dataset.pkl", "rb"))
    out = []
    for d, M in mats:
        e = M["entry"].astype(float); q = np.floor((500000.0 / M["nstocks"]) / e); dep = float((e * q).sum())
        out.append((d[:7], M, e, q, dep, (M["close"].astype(float) @ q - dep) / dep * 100.0))
    return out


def cur_exit(rc, n, arm, floor, give, stop):
    armed = False; peak = None
    for i in range(n - 1):
        r = rc[i]
        if r <= -stop:
            return -stop
        if not armed:
            if r >= arm:
                armed = True; peak = r
            continue
        peak = max(peak, r)
        if r <= max(floor, peak - give):
            return r
    return rc[n - 1]


def twolayer(M, e, q, dep, pstop, bstop):
    """RIDE basket (exit EOD) + per-stock CATASTROPHE stop at -pstop% (fills at that level);
    basket hard stop -bstop%. Isolates the cost of the per-stock disaster layer."""
    close = M["close"].astype(float); n = M["n"]
    lvl = e * (1 - pstop / 100.0) if pstop else None
    mask = np.ones(len(e), bool); realized = 0.0
    for i in range(1, n):
        if pstop:
            for j in np.where(mask)[0]:
                if close[i, j] <= lvl[j]:
                    realized += (lvl[j] - e[j]) * q[j]; mask[j] = False
        unreal = float(((close[i] - e) * q * mask).sum())
        G = (realized + unreal) / dep * 100.0
        if G <= -bstop:
            realized += float(((close[i] - e) * q * mask).sum())
            return G
    realized += float(((close[n - 1] - e) * q * mask).sum())
    return realized / dep * 100.0


def stats(vals, months):
    a = np.array([net(v) for v in vals]); eq = np.cumsum(a); dd = float((np.maximum.accumulate(eq) - eq).max())
    md = defaultdict(list)
    for v, m in zip(a, months):
        md[m].append(v)
    return a.mean(), (a > 0).mean() * 100, a.sum(), dd, a.min()


def main():
    D = load(); months = [m for m, *_ in D]
    print("Q1 — CURRENT-MODEL knob variants (arm/floor/give/stop). Can knobs alone reach the ratchet?\n")
    print(f"{'config (arm/floor/give/stop)':<34}{'mean%':>7}{'%pos':>6}{'total%':>8}{'maxDD':>7}{'worst':>7}")
    q1 = [
        ("DEFAULT 2.5/1.0/1.5/3", (2.5, 1.0, 1.5, 3.0)),
        ("retune 1.5/0.5/1.0/3", (1.5, 0.5, 1.0, 3.0)),
        ("retune 1.5/0.5/1.0/2", (1.5, 0.5, 1.0, 2.0)),
        ("retune 1.5/0.0/1.0/2", (1.5, 0.0, 1.0, 2.0)),
        ("retune 2.0/0.5/1.5/2", (2.0, 0.5, 1.5, 2.0)),
    ]
    for name, (a, f, g, s) in q1:
        vals = [cur_exit(rc, M["n"], a, f, g, s) for _, M, e, q, dep, rc in D]
        m, p, t, dd, w = stats(vals, months)
        print(f"{name:<34}{m:>7.3f}{p:>6.1f}{t:>8.0f}{dd:>7.1f}{w:>7.2f}")
    print(f"{'(ref) RATCHET E':<34}{1.643:>7.3f}{74.8:>6}{879:>8}{12.6:>7}{-2.10:>7}   <- structural adds ~+0.05 over best knob-tune")
    print(f"{'(ref) RIDE + stop-2':<34}{1.781:>7.3f}")

    print("\nQ2 — STOCK-level catastrophe stop added to a RIDE basket. Does it cost return?\n")
    print(f"{'design':<40}{'mean%':>7}{'%pos':>6}{'total%':>8}{'maxDD':>7}{'worst':>7}")
    q2 = [("basket-only RIDE (no per-stock)", None), ("+ per-stock catastrophe -15%", 15.0),
          ("+ per-stock catastrophe -10%", 10.0), ("+ per-stock catastrophe -8%", 8.0),
          ("+ per-stock catastrophe -6%", 6.0)]
    for name, ps in q2:
        vals = [twolayer(M, e, q, dep, ps, 3.0) for _, M, e, q, dep, rc in D]
        m, p, t, dd, w = stats(vals, months)
        print(f"{name:<40}{m:>7.3f}{p:>6.1f}{t:>8.0f}{dd:>7.1f}{w:>7.2f}")


if __name__ == "__main__":
    main()
