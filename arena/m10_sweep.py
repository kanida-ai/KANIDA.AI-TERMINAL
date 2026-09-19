"""
M10 diagnostic sweep — isolate WHY the risk-managed book underperforms.
Per stock: load frame + segment fire-masks ONCE, then evaluate several policies on it
(no re-reading frames per policy). Aggregate a true PORTFOLIO equity curve per policy.

Policies (all leverage/routing identical; NRML_LONG on):
  RAW            best-lift segment, no scoring, no stops         (reference book)
  SCORE_NOSTOP   adaptive constitutional score, NO stops         (isolates the SCORING effect)
  WICK_3.0       adaptive + stop on intraday H/L at 3.0x         (current M10)
  CLOSE_3.0      adaptive + stop on DAILY CLOSE breach at 3.0x   (no wick stop-outs)
  CLOSE_MD_3.0   adaptive + close-stop, multiday holds only      (leave 1D MIS unstopped)
  WICK_4.0       adaptive + wick stop at 4.0x                    (wider tail bound)
"""
import sys, json, sqlite3
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\scripts")))
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\kanida_engine")))
from mine_phase1 import apply_rule
import features as FE

SNR = r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db"
KDB = r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db"
CAP = 100_000.0
TGT = {"up_1pct_1d": ("up", 1, 1), "up_2pct_2d": ("up", 2, 2), "up_5pct_5d": ("up", 5, 5),
       "dn_1pct_1d": ("dn", 1, 1), "dn_2pct_2d": ("dn", 2, 2), "dn_5pct_5d": ("dn", 5, 5)}
LEV = {"MIS": 5.0, "CNC": 1.0, "NRML": 5.0}; COST = {"MIS": 0.08, "CNC": 0.15, "NRML": 0.05}
SCORE_WIN = 20; MIN_CONF = 5; CONF_FULL = 40; TEST_WT = 0.25; DRIFT_RECENT = 8; VAR_PEN = 0.3; DRIFT_PEN = 0.5

# policy = (adaptive, stop_mult, stop_basis, multiday_only)   stop_basis in {None,'wick','close'}
POLICIES = {
    "RAW":          (False, None, None,   False),
    "SCORE_NOSTOP": (True,  None, None,   False),
    "WICK_3.0":     (True,  3.0, "wick",  False),
    "CLOSE_3.0":    (True,  3.0, "close", False),
    "CLOSE_MD_3.0": (True,  3.0, "close", True),
    "WICK_4.0":     (True,  4.0, "wick",  False),
}


def route(d, w, is_fno):
    if w == 1: return "MIS", ("LONG" if d == "up" else "SHORT")
    if d == "up": return ("NRML" if is_fno else "CNC"), "LONG"
    return "NRML", "SHORT"


def cscore(closed):
    n = len(closed)
    if n < MIN_CONF: return TEST_WT, 0.0
    r = np.array(closed[-SCORE_WIN:]); etv = r.mean()
    dn = r[r < 0]; dsd = dn.std() if len(dn) > 1 else (abs(dn.mean()) if len(dn) else 0.0)
    drift = r[-DRIFT_RECENT:].mean() - etv if len(r) >= DRIFT_RECENT else 0.0
    sc = etv - VAR_PEN * dsd + DRIFT_PEN * drift
    if sc <= 0: return 0.0, sc
    return TEST_WT + (1 - TEST_WT) * min(1.0, n / CONF_FULL), sc


def sim(fnp, i, d, pct, w, order, n, stop_mult, stop_basis, multiday_only):
    O, H, L, C = fnp
    if i + 1 >= n: return None
    entry = O[i + 1]
    if not np.isfinite(entry) or entry <= 0: return None
    long = (d == "up"); tgt = entry * (1 + pct / 100) if long else entry * (1 - pct / 100)
    use_stop = stop_basis is not None and not (multiday_only and w == 1)
    stop = entry * (1 - pct * (stop_mult or 0) / 100) if long else entry * (1 + pct * (stop_mult or 0) / 100)
    ex = None; exi = min(i + w, n - 1)
    for x in range(i + 1, min(i + 1 + w, n)):
        hi, lo, cl = H[x], L[x], C[x]
        if long:
            if use_stop and ((stop_basis == "wick" and lo <= stop) or (stop_basis == "close" and cl <= stop)):
                ex, exi = (stop if stop_basis == "wick" else cl), x; break
            if hi >= tgt: ex, exi = tgt, x; break
        else:
            if use_stop and ((stop_basis == "wick" and hi >= stop) or (stop_basis == "close" and cl >= stop)):
                ex, exi = (stop if stop_basis == "wick" else cl), x; break
            if lo <= tgt: ex, exi = tgt, x; break
        exi = x
    if ex is None: ex = C[exi]
    gross = (ex / entry - 1) * 100 if long else (1 - ex / entry) * 100
    return exi, (gross - COST[order]) * LEV[order]


def maxdd(pairs):
    if not pairs: return 0.0
    s = pd.Series([p for _, p in sorted(pairs)]).cumsum()
    return float((s - s.cummax()).min())


def run_stock(symbol, con, is_fno, out):
    rows = con.execute("SELECT target,rule_json,lift_tr FROM unified_patterns WHERE symbol=? AND promoted=1",
                       (symbol,)).fetchall()
    if not rows: return
    segs = {}
    for t, rj, lt in rows:
        if t not in TGT: continue
        d, pct, w = TGT[t]; order, dl = route(d, w, is_fno); k = f"{order} {dl} {w}D"
        s = segs.setdefault(k, {"conds": [], "lift": 0, "d": d, "pct": pct, "w": w, "order": order})
        s["conds"].append([tuple(c) for c in json.loads(rj)]); s["lift"] = max(s["lift"], lt or 0)
    frame = FE.load_frame(symbol, lookback_N=5)
    if frame.empty: return
    days = list(frame[frame["year"].isin([2025, 2026])].index)
    if len(days) < 5: return
    fnp = (frame["_o"].values, frame["_h"].values, frame["_l"].values, frame["_c"].values); nfr = len(frame)
    idx = {dt: k for k, dt in enumerate(frame.index)}; day_idx = [idx[dt] for dt in days]
    for s in segs:
        m = np.zeros(len(frame), bool)
        for cds in segs[s]["conds"]: m |= apply_rule(frame, cds).values
        segs[s]["fire"] = m

    def book(adaptive, stop_mult, stop_basis, multiday_only):
        closed = {s: [] for s in segs}; trades = []; i = 0
        while i < len(day_idx) - 1:
            gi = day_idx[i]; firing = [s for s in segs if segs[s]["fire"][gi]]
            if not firing: i += 1; continue
            if adaptive:
                cand = []
                for s in firing:
                    wgt, sc = cscore(closed[s])
                    if wgt > 0: cand.append((sc, wgt, s))
                if not cand: i += 1; continue
                cand.sort(reverse=True); _, weight, seg = cand[0]
            else:
                seg = max(firing, key=lambda s: segs[s]["lift"]); weight = 1.0
            m = segs[seg]
            r = sim(fnp, gi, m["d"], m["pct"], m["w"], m["order"], nfr, stop_mult, stop_basis, multiday_only)
            if r is None: i += 1; continue
            exi, net = r; pnl = CAP * net / 100 * weight
            trades.append((frame.index[exi], pnl)); closed[seg].append(net)
            while i < len(day_idx) and day_idx[i] <= exi: i += 1
        return trades

    for name, (ad, sm, sb, md) in POLICIES.items():
        out[name].append((symbol, book(ad, sm, sb, md)))


def main():
    con = sqlite3.connect(SNR); kc = sqlite3.connect(KDB)
    fno = {r[0]: r[1] for r in kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()}; kc.close()
    syms = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM unified_patterns").fetchall()]
    out = {k: [] for k in POLICIES}
    for n, s in enumerate(syms, 1):
        try: run_stock(s, con, int(fno.get(s, 0)), out)
        except Exception: pass
        if n % 100 == 0: print(f"  ...{n}/{len(syms)}", flush=True)
    con.close()
    print("\n============ M10 STOP/SCORE POLICY SWEEP (441 workers, 2025-26) ============")
    print(f"  {'policy':<14}{'profit Rs':>14}{'portDD Rs':>14}{'ret/DD':>9}{'pos wk':>8}{'trades':>9}")
    base = None
    for name in POLICIES:
        allp = [(d, p) for _, tr in out[name] for d, p in tr]
        prof = sum(p for _, p in allp); dd = maxdd(allp); ntr = len(allp)
        posw = sum(1 for _, tr in out[name] if sum(p for _, p in tr) > 0)
        rr = (prof / -dd) if dd < 0 else float("inf")
        if name == "RAW": base = prof
        tag = "" if name == "RAW" else f"  keep {prof/base*100:>4.0f}%"
        print(f"  {name:<14}{prof:>14,.0f}{dd:>14,.0f}{rr:>9.2f}{posw:>8}{ntr:>9,}{tag}")


if __name__ == "__main__":
    main()
