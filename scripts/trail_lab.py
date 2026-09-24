"""Trailing-method testbed. Runs multiple profit-locking exit models on the SAME realized
basket paths (intraday _opt_dataset + positional _pos_dataset) so we can compare mechanisms
head-to-head on net return, drawdown, give-back (peak->exit) and premature-exit rate.

All decisions on the 1-min CLOSE basket return (gross %), next-bar fills folded in as the
exit ret; net = gross - ~0.10% round-trip. Intraday exits EOD (15:29); positional caps at
max-hold 3 sessions. Baselines isolate the trail's value: RIDE (no trail), STOPONLY (hard stop
only, else ride).
"""
import pickle, sys
from pathlib import Path
from collections import defaultdict
import numpy as np
sys.path.insert(0, r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\scripts")
import pos_sim as P

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
CHARGE = 0.0005  # per side


def _net(gross):
    return gross - (2 + gross / 100.0) * CHARGE * 100.0  # ~gross - 0.10%


def rolling_vol(ret, w=15):
    d = np.diff(ret, prepend=ret[0])
    v = np.zeros_like(ret)
    for i in range(len(ret)):
        s = max(0, i - w + 1)
        v[i] = d[s:i + 1].std() if i > 0 else 0.0
    return v


def run_model(retC, last, model, vol=None):
    """Return (exit_idx, gross_exit, reason, peak_gross). last=last actionable idx (EOD/cap)."""
    A = model.get("arm", 1.5); S = model.get("stop", 3.0); typ = model["type"]
    armed = False; peak = -1e9
    for i in range(last):
        r = retC[i]
        if r <= -S:
            return i, -S, "STOP", max(peak, r)
        if typ == "ride":
            continue
        if typ == "stoponly":
            continue
        if not armed:
            if r >= A:
                armed = True; peak = r
            continue
        peak = max(peak, r)
        # locking rule -> exit threshold `thr`
        if typ == "current":
            thr = max(model["floor"], peak - model["give"])
        elif typ == "peak":
            thr = peak - model["give"]
        elif typ == "step":
            thr = 0.0
            for pk, lock in model["steps"]:
                if peak >= pk:
                    thr = lock
        elif typ == "pct":
            p = model["pctfn"](peak); thr = peak * p
        elif typ == "vol":
            g = min(max(model["k"] * vol[i], model["gmin"]), model["gmax"]); thr = peak - g
        elif typ == "time":
            f = i / last; g = model["ge"] * (1 - f) + model["gl"] * f; thr = peak - g
        else:
            thr = peak - 1.5
        if r <= thr:
            return i, r, ("TRAIL" if r > model.get("floor", -99) else "TRAIL"), peak
    return last, retC[last], "EOD", max(peak, retC[last])


def metrics(rows, months):
    net = np.array([_net(g) for _, g, _, _ in rows])
    gb = np.array([max(0.0, pk - g) for _, g, _, pk in rows])           # give-back peak->exit
    eq = np.cumsum(net); dd = float((np.maximum.accumulate(eq) - eq).max())
    md = defaultdict(list)
    for n, m in zip(net, months):
        md[m].append(n)
    grew = sum(1 for v in md.values() if sum(v) >= 0)
    reasons = defaultdict(int)
    for _, _, rs, _ in rows:
        reasons[rs] += 1
    return dict(mean=round(float(net.mean()), 3), pos=round(float((net > 0).mean() * 100), 1),
                total=round(float(net.sum()), 0), maxdd=round(dd, 1), worst=round(float(net.min()), 2),
                giveback=round(float(gb.mean()), 3), months=f"{grew}/{len(md)}",
                eod=reasons.get("EOD", 0), stop=reasons.get("STOP", 0), trail=reasons.get("TRAIL", 0))


# ---- exit-model catalogue (intraday params; positional overrides arm/stop) ----
def catalogue(arm, stop, floor, give):
    stepI = [(2.0, 1.0), (4.0, 2.4), (6.0, 4.4), (10.0, 8.0)]

    def pctfn(pk):
        return 0.60 if pk < 4 else (0.70 if pk < 8 else 0.80)
    return [
        ("RIDE to close (no trail)", dict(type="ride", stop=99)),
        ("STOP-only (-%s, else ride)" % stop, dict(type="stoponly", stop=stop)),
        ("CURRENT arm/floor/give", dict(type="current", arm=arm, floor=floor, give=give, stop=stop)),
        ("PEAK-trail (arm/give, no floor)", dict(type="peak", arm=arm, give=give, stop=stop)),
        ("STEP-lock (milestones)", dict(type="step", arm=arm, steps=stepI, stop=stop)),
        ("PCT-of-peak (60/70/80%)", dict(type="pct", arm=arm, pctfn=pctfn, stop=stop)),
        ("VOL-trail (k*vol15)", dict(type="vol", arm=arm, k=3.0, gmin=0.5, gmax=give * 2, stop=stop)),
        ("TIME-adaptive give", dict(type="time", arm=arm, ge=give * 1.5, gl=give * 0.4, stop=stop)),
    ]


def run_intraday():
    mats = pickle.load(open(ROOT / "docs" / "ops" / "_opt_dataset.pkl", "rb"))
    paths = []
    for d, M in mats:
        entry = M["entry"].astype(float); n = M["n"]
        qty = np.floor((500000.0 / M["nstocks"]) / entry); dep = float((entry * qty).sum())
        retC = (M["close"].astype(float) @ qty - dep) / dep * 100.0
        vol = rolling_vol(retC)
        paths.append((d[:7], retC, n - 1, vol))
    print(f"\n{'='*96}\nINTRADAY  ({len(paths)} days, Rs5L, net)  — arm1.5/stop3 common; CURRENT row = arm2.5/floor1/give1.5\n{'='*96}")
    hdr = f"{'model':<34}{'mean%':>7}{'%pos':>6}{'total%':>8}{'maxDD':>7}{'worst':>7}{'givebk':>7}{'EOD/TRL/STP':>13}{'moGrew':>8}"
    print(hdr)
    cat = catalogue(1.5, 3.0, 1.0, 1.5)
    cat[2] = ("CURRENT arm2.5/floor1/give1.5", dict(type="current", arm=2.5, floor=1.0, give=1.5, stop=3.0))
    for name, model in cat:
        rows = [run_model(rc, last, model, vol) for _, rc, last, vol in paths]
        s = metrics(rows, [m for m, _, _, _ in paths])
        rc3 = f"{s['eod']}/{s['trail']}/{s['stop']}"
        print(f"{name:<34}{s['mean']:>7.3f}{s['pos']:>6}{s['total']:>8.0f}{s['maxdd']:>7}{s['worst']:>7}"
              f"{s['giveback']:>7}{rc3:>13}{s['months']:>8}")


def run_positional():
    ds = P.load()
    paths = []
    for m in ds:
        retC, _ = P.paths(m)
        last = int(m["eod"][min(3, int(m["day"].max()))])
        vol = rolling_vol(retC[:last + 1])
        paths.append((m["signal_date"][:7], retC, last, vol))
    print(f"\n{'='*96}\nPOSITIONAL ({len(paths)} baskets, per-trade net, max-hold 3) — arm2/stop6 common; CURRENT = arm3/floor1/give4\n{'='*96}")
    print(f"{'model':<34}{'mean%':>7}{'%pos':>6}{'total%':>8}{'maxDD':>7}{'worst':>7}{'givebk':>7}{'TIME/TRL/STP':>13}{'moGrew':>8}")
    cat = catalogue(2.0, 6.0, 1.0, 4.0)
    cat[2] = ("CURRENT arm3/floor1/give4", dict(type="current", arm=3.0, floor=1.0, give=4.0, stop=6.0))
    # positional step/pct milestones scale bigger
    cat[4] = ("STEP-lock (milestones)", dict(type="step", arm=2.0, steps=[(3.0, 1.5), (6.0, 4.0), (9.0, 6.5), (14.0, 11.0)], stop=6.0))
    for name, model in cat:
        rows = [run_model(rc, last, model, vol) for _, rc, last, vol in paths]
        s = metrics(rows, [m for m, _, _, _ in paths])
        rc3 = f"{s['eod']}/{s['trail']}/{s['stop']}"
        print(f"{name:<34}{s['mean']:>7.3f}{s['pos']:>6}{s['total']:>8.0f}{s['maxdd']:>7}{s['worst']:>7}"
              f"{s['giveback']:>7}{rc3:>13}{s['months']:>8}")


if __name__ == "__main__":
    run_intraday()
    run_positional()
