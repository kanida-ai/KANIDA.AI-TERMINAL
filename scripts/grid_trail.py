"""Systematic grid search for the intraday basket trail, with walk-forward IS/OOS split.
Architecture tested: Early Arm -> Dynamic Floor (init lock) -> Peak Ratchet (max of %-of-peak
and fixed give-back, one-way) -> hard Stop. Vol/momentum overlays tested separately on finalists.

Floor rule once armed: floor = max(init_lock, peak*ratchet_frac, peak - giveback)  [monotone up].
Grid: arm x init x frac x give x stop = 8 x 3 x 3 x 5 x 3 = 1080.
IS = days < 2026-01-01 ; OOS = 2026 (a different, weaker regime incl. the down June).
All net of ~0.10% RT, Rs5L. Capital@5x = notional x 5.
"""
import pickle
from pathlib import Path
from collections import defaultdict
import numpy as np
ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
CH = 0.0005


def _net(g):
    return g - (2 + g / 100.0) * CH * 100.0


def load():
    mats = pickle.load(open(ROOT / "docs" / "ops" / "_opt_dataset.pkl", "rb"))
    days = []
    for d, M in mats:
        e = M["entry"].astype(float); q = np.floor((500000.0 / M["nstocks"]) / e); dep = float((e * q).sum())
        rc = (M["close"].astype(float) @ q - dep) / dep * 100.0
        days.append((d, rc))
    return days


def day_exit(rc, arm, init, frac, give, stop):
    n = len(rc)
    below = rc <= -stop
    stop_idx = int(np.argmax(below)) if below.any() else n
    armv = rc >= arm
    a = int(np.argmax(armv)) if armv.any() else n
    trail_idx = n
    if a < n:
        seg = rc[a:]
        peak = np.maximum.accumulate(seg)
        floor = np.maximum(np.maximum(init, peak * frac), peak - give)
        trig = seg <= floor
        if trig.any():
            trail_idx = a + int(np.argmax(trig))
    cands = [(n - 1, "EOD", float(rc[-1]))]
    if stop_idx < n:
        cands.append((stop_idx, "STOP", -stop))
    if trail_idx < n:
        cands.append((trail_idx, "TRAIL", float(rc[trail_idx])))
    idx, reason, exret = min(cands, key=lambda x: x[0])
    armed = (a < n) and (idx >= a)
    peak_to_exit = float(rc[:idx + 1].max())
    postmax = float(rc[idx + 1:].max()) if idx + 1 < n else -99.0
    return exret, reason, armed, peak_to_exit, postmax, float(rc.max()), float(rc[-1])


def agg(recs):
    """recs: list of per-day tuples (exret,reason,armed,peak2exit,postmax,daymax,dayeod)."""
    net = np.array([_net(r[0]) for r in recs])
    eq = np.cumsum(net); dd = float((np.maximum.accumulate(eq) - eq).max())
    wins = net[net > 0]; losses = net[net < 0]
    pf = wins.sum() / -losses.sum() if losses.sum() < 0 else 99.9
    armed = np.array([r[2] for r in recs])
    gb = np.array([r[3] - r[0] for r in recs])[armed]  # give-back peak->exit on armed
    prem = sum(1 for r in recs if r[2] and r[1] == "TRAIL" and (r[4] - r[0]) > 0.3)
    bigwin = [r for r in recs if r[5] >= 3.0]
    bw_prot = sum(1 for r in bigwin if r[0] >= 2.0)
    bw_kill = sum(1 for r in bigwin if r[1] == "TRAIL" and ((r[6] - r[0]) > 1.0 or (r[4] - r[0]) > 1.0))
    loser_capped = sum(1 for r in recs if r[1] == "STOP" and r[6] < -( -r[0]))  # stop < ride-to-close loss
    prof2loss = sum(1 for r in recs if r[2] and _net(r[0]) < 0)
    return dict(total=float(net.sum()), mean=float(net.mean()), dd=dd, worst=float(net.min()),
                win=float((net > 0).mean() * 100), avgwin=float(wins.mean()) if len(wins) else 0,
                avgloss=float(losses.mean()) if len(losses) else 0, pf=float(pf),
                armedpct=float(armed.mean() * 100), gb=float(gb.mean()) if len(gb) else 0,
                prem=prem, prempct=float(prem / max(1, armed.sum()) * 100),
                bigwin=len(bigwin), bw_prot=bw_prot, bw_kill=bw_kill, loser_capped=loser_capped,
                prof2loss=prof2loss, std=float(net.std()), n=len(net))


def main():
    days = load()
    dates = [d for d, _ in days]; rcs = [rc for _, rc in days]
    is_mask = np.array([d < "2026-01-01" for d in dates])
    ARMS = [0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 2.5]
    INITS = [0.0, 0.1, 0.25]; FRACS = [0.0, 0.3, 0.5]; GIVES = [0.25, 0.5, 0.75, 1.0, 1.5]; STOPS = [1.5, 2.0, 3.0]

    # baseline current: arm2.5 / floor1.0 / give1.5 / stop3  (floor as init=1.0, frac=0)
    def run_cfg(arm, init, frac, give, stop):
        recs = [day_exit(rc, arm, init, frac, give, stop) for rc in rcs]
        A = agg([r for r, m in zip(recs, is_mask) if m])
        O = agg([r for r, m in zip(recs, is_mask) if not m])
        L = agg(recs)
        return A, O, L
    baseA, baseO, baseL = run_cfg(2.5, 1.0, 0.0, 1.5, 3.0)

    results = []
    for arm in ARMS:
        for init in INITS:
            for frac in FRACS:
                for give in GIVES:
                    for stop in STOPS:
                        A, O, L = run_cfg(arm, init, frac, give, stop)
                        results.append(dict(cfg=(arm, init, frac, give, stop), A=A, O=O, L=L))

    def line(tag, cfg, r):
        a, i, f, g, s = cfg
        return (f"{tag:<20}arm{a} init{i} frc{f} giv{g} stp{s}  "
                f"ret{r['total']:>6.0f}({r['total']*5:>6.0f}@5x) DD{r['dd']:>5.1f} gb{r['gb']:>4.2f} "
                f"win{r['win']:>4.1f} PF{r['pf']:>4.2f} arm{r['armedpct']:>4.0f}% prem{r['prempct']:>4.0f}% "
                f"bwP{r['bw_prot']}/bwK{r['bw_kill']} wrst{r['worst']:>5.1f}")

    print("=" * 140)
    print("BASELINE — current arm2.5/floor1.0/give1.5/stop3")
    print("  ALL :", line("", (2.5, 1.0, 0.0, 1.5, 3.0), baseL))
    print("  IS  :", line("", (2.5, 1.0, 0.0, 1.5, 3.0), baseA))
    print("  OOS :", line("", (2.5, 1.0, 0.0, 1.5, 3.0), baseO))

    print("\n" + "=" * 140)
    print("PER-ARM BEST (by risk-adj = ALL total/DD) — how each arm threshold performs at its best floor/give/stop")
    by_arm = defaultdict(list)
    for r in results:
        by_arm[r["cfg"][0]].append(r)
    for arm in ARMS:
        best = max(by_arm[arm], key=lambda r: r["L"]["total"] / r["L"]["dd"] if r["L"]["dd"] else 0)
        print(" ", line(f"arm {arm}", best["cfg"], best["L"]))

    # selections on IS, verified OOS
    best_ret = max(results, key=lambda r: r["A"]["total"])
    best_ra = max(results, key=lambda r: (r["A"]["total"] / r["A"]["dd"]) if r["A"]["dd"] else 0)
    # safest: min worst-day among configs with IS total >= 0.85*baseline IS total, stop<=2
    cand = [r for r in results if r["A"]["total"] >= 0.85 * baseA["total"] and r["cfg"][4] <= 2.0]
    safest = min(cand, key=lambda r: r["A"]["dd"]) if cand else min(results, key=lambda r: r["A"]["dd"])

    print("\n" + "=" * 140)
    print("SELECTED CONFIGS (optimized on IS, verified OOS):")
    for tag, r in [("BEST RETURN", best_ret), ("BEST RISK-ADJ", best_ra), ("SAFEST", safest)]:
        print(f"\n  {tag}: arm{r['cfg'][0]} init{r['cfg'][1]} frac{r['cfg'][2]} give{r['cfg'][3]} stop{r['cfg'][4]}")
        print("   IS :", line("", r["cfg"], r["A"]))
        print("   OOS:", line("", r["cfg"], r["O"]))
        print("   ALL:", line("", r["cfg"], r["L"]))

    # save full grid for the workbook step
    import json
    slim = [dict(cfg=r["cfg"], **{k: r["L"][k] for k in ("total", "dd", "gb", "win", "pf", "armedpct", "prempct", "bw_prot", "bw_kill", "worst", "mean")}) for r in results]
    json.dump(dict(baseline=dict(cfg=(2.5, 1.0, 0.0, 1.5, 3.0), L=baseL, A=baseA, O=baseO), grid=slim,
                   sel=dict(best_ret=best_ret["cfg"], best_ra=best_ra["cfg"], safest=safest["cfg"])),
              open(ROOT / "docs" / "ops" / "trail_grid.json", "w"), default=float)
    print("\n[*] full grid -> docs/ops/trail_grid.json")


if __name__ == "__main__":
    main()
