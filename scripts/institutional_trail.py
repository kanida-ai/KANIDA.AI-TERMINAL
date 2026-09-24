"""Design + validate an institutional intraday trail for 5x-leveraged trading.
Philosophy: ride cheap moves, ratchet-lock big profit with WIDE bands (no whipsaw),
tighten only when momentum breaks or near the close, on a leverage-normalized hard stop.
All on the intraday _opt_dataset basket paths (Rs5L notional, net of ~0.10% RT).
Equity view: at 5x, every notional % = 5% of equity.
"""
import pickle
from pathlib import Path
from collections import defaultdict
import numpy as np
ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
CH = 0.0005
LEV = 5.0


def net(g):
    return g - (2 + g / 100.0) * CH * 100.0


def load_paths():
    mats = pickle.load(open(ROOT / "docs" / "ops" / "_opt_dataset.pkl", "rb"))
    P = []
    for d, M in mats:
        e = M["entry"].astype(float); q = np.floor((500000.0 / M["nstocks"]) / e); dep = float((e * q).sum())
        rc = (M["close"].astype(float) @ q - dep) / dep * 100.0
        P.append((d[:7], rc, M["grid"], M["n"]))
    return P


def run(rc, grid, n, cfg):
    A = cfg.get("arm", 1.5); S = cfg["stop"]; typ = cfg["type"]
    armed = False; peak = -1e9; floor = -1e9
    for i in range(n - 1):
        r = rc[i]
        if r <= -S:
            return i, -S, "STOP", max(peak, r)
        if typ == "ride":
            continue
        # time-phased giveback (close-guard)
        t = grid[i] if i < len(grid) else "15:29"
        if cfg.get("closeguard"):
            g = cfg["give"] if t < "15:00" else (0.6 if t < "15:20" else 0.3)
        else:
            g = cfg["give"]
        if not armed:
            if r >= A:
                armed = True; peak = r; floor = r - g
            continue
        peak = max(peak, r)
        if cfg.get("steps"):
            base = 0.0
            for pk, lk in cfg["steps"]:
                if peak >= pk:
                    base = lk
            newfloor = max(base, peak - g)
        else:
            newfloor = peak - g
        floor = max(floor, newfloor)
        if cfg.get("mom"):
            rh = rc[max(0, i - 15):i + 1].max()
            if r <= floor and r < rh - cfg["mom"]:
                return i, r, "TRAIL", peak
        else:
            if r <= floor:
                return i, r, "TRAIL", peak
    return n - 1, rc[n - 1], "EOD", max(peak, rc[n - 1])


def evaluate(P, cfg):
    rows = [run(rc, grid, n, cfg) for _, rc, grid, n in P]
    nets = np.array([net(g) for _, g, _, _ in rows])
    gb = np.array([max(0.0, pk - g) for _, g, rs, pk in rows if net(g) > 0])  # give-back on winners
    eq = np.cumsum(nets); dd = float((np.maximum.accumulate(eq) - eq).max())
    reasons = defaultdict(int)
    for _, _, rs, _ in rows:
        reasons[rs] += 1
    return dict(mean=nets.mean(), pos=(nets > 0).mean() * 100, total=nets.sum(), dd=dd,
                worst=nets.min(), gb=gb.mean() if len(gb) else 0.0,
                mdd=nets.sum() / dd if dd else 0, sharpe=nets.mean() / nets.std() * np.sqrt(252),
                r=reasons)


def main():
    P = load_paths()
    models = [
        ("RIDE + stop-3 (baseline best)", dict(type="ride", stop=3.0)),
        ("CURRENT arm2.5/floor1/give1.5", dict(type="cur", arm=2.5, give=1.5, stop=3.0, steps=[(2.5, 1.0)])),
        ("A: wide peak-trail give1.0", dict(type="t", arm=1.5, give=1.0, stop=3.0)),
        ("B: wide + close-guard", dict(type="t", arm=1.5, give=1.0, stop=3.0, closeguard=True)),
        ("C: step-ratchet + close-guard", dict(type="t", arm=1.5, give=1.2, stop=3.0, closeguard=True,
                                               steps=[(1.5, 0.5), (2.5, 1.5), (3.5, 2.5), (5.0, 4.0)])),
        ("D: C + momentum-gate", dict(type="t", arm=1.5, give=1.2, stop=3.0, closeguard=True, mom=0.4,
                                      steps=[(1.5, 0.5), (2.5, 1.5), (3.5, 2.5), (5.0, 4.0)])),
        ("E: D + tighter stop-2 (5x-safe)", dict(type="t", arm=1.5, give=1.2, stop=2.0, closeguard=True, mom=0.4,
                                                 steps=[(1.5, 0.5), (2.5, 1.5), (3.5, 2.5), (5.0, 4.0)])),
    ]
    print(f"{'model':<34}{'mean%':>7}{'%pos':>6}{'total%':>8}{'maxDD':>7}{'worst':>7}{'giveB':>7}{'ret/DD':>7}{'Sharpe':>7}")
    print("  (equity view at 5x: multiply mean%/maxDD/worst by 5)")
    for name, cfg in models:
        s = evaluate(P, cfg)
        print(f"{name:<34}{s['mean']:>7.3f}{s['pos']:>6.1f}{s['total']:>8.0f}{s['dd']:>7.1f}{s['worst']:>7.2f}"
              f"{s['gb']:>7.3f}{s['mdd']:>7.1f}{s['sharpe']:>7.1f}")
    # equity-scaled summary for the two finalists
    print("\nEQUITY VIEW (5x) — worst single day and maxDD in % of equity:")
    for name, cfg in [m for m in models if m[0][0] in "RE"]:
        s = evaluate(P, cfg)
        print(f"  {name:<34} worst day {s['worst']*LEV:>6.1f}% equity | maxDD {s['dd']*LEV:>5.1f}% equity | mean/day {s['mean']*LEV:.2f}% equity")


if __name__ == "__main__":
    main()
