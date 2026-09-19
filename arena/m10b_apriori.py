"""
M10b — a-priori POINT-IN-TIME selection (the pivot after M10 disproved reactive risk control).
For each worker, score every segment (order.direction.hold) on <=2024 data ONLY, then KEEP a segment
for the 2025-26 book only if its pre-2025 ETV clears a bar. Kept segments trade at FULL breadth
(best-lift among firing, no stops, no reactive weighting) — apples-to-apples with RAW.

Leak-free: the keep/drop decision uses only <=2024; evaluation is 2025-26 (and 2026-only, the sealed
year). Question: can up-front SELECTION beat the raw book's ret/DD 0.73, where reactive control failed?
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

# a-priori keep rules, evaluated on <=2024 segment stats: (min ETV %, min sample n)
RULES = {
    "RAW (all)":        (-1e9, 0),
    "ETV>0":            (0.0,  1),
    "ETV>0 n>=10":      (0.0, 10),
    "ETV>0.3% n>=10":   (0.3, 10),
    "ETV>0.5% n>=10":   (0.5, 10),
    "ETV>0.5% n>=20":   (0.5, 20),
}


def route(d, w, is_fno):
    if w == 1: return "MIS", ("LONG" if d == "up" else "SHORT")
    if d == "up": return ("NRML" if is_fno else "CNC"), "LONG"
    return "NRML", "SHORT"


def sim(fnp, i, d, pct, w, order, n):
    O, H, L, C = fnp
    if i + 1 >= n: return None
    entry = O[i + 1]
    if not np.isfinite(entry) or entry <= 0: return None
    long = (d == "up"); tgt = entry * (1 + pct / 100) if long else entry * (1 - pct / 100)
    ex = None; exi = min(i + w, n - 1)
    for x in range(i + 1, min(i + 1 + w, n)):
        hi, lo = H[x], L[x]
        if long:
            if hi >= tgt: ex, exi = tgt, x; break
        else:
            if lo <= tgt: ex, exi = tgt, x; break
        exi = x
    if ex is None: ex = C[exi]
    gross = (ex / entry - 1) * 100 if long else (1 - ex / entry) * 100
    return exi, (gross - COST[order]) * LEV[order]


def maxdd(pairs):
    if not pairs: return 0.0
    s = pd.Series([p for _, p in sorted(pairs)]).cumsum()
    return float((s - s.cummax()).min())


def run_stock(symbol, con, is_fno, out2, out26):
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
    yr = frame["year"].values
    fnp = (frame["_o"].values, frame["_h"].values, frame["_l"].values, frame["_c"].values); nfr = len(frame)
    for s in segs:
        m = np.zeros(nfr, bool)
        for cds in segs[s]["conds"]: m |= apply_rule(frame, cds).values
        segs[s]["fire"] = m

    # ---- a-priori segment stats on <=2024 signals only (point-in-time) ----
    pre = np.where(yr <= 2024)[0]
    for s in segs:
        m = segs[s]; rocs = []
        for gi in pre:
            if not m["fire"][gi]: continue
            r = sim(fnp, gi, m["d"], m["pct"], m["w"], m["order"], nfr)
            if r is not None: rocs.append(r[1])
        m["pre_n"] = len(rocs); m["pre_etv"] = float(np.mean(rocs)) if rocs else -1e9

    # ---- book over an eval index set, restricted to a KEEP set ----
    def book(eval_idx, keep):
        trades = []; i = 0
        while i < len(eval_idx) - 1:
            gi = eval_idx[i]; firing = [s for s in segs if s in keep and segs[s]["fire"][gi]]
            if not firing: i += 1; continue
            seg = max(firing, key=lambda s: segs[s]["lift"]); m = segs[seg]
            r = sim(fnp, gi, m["d"], m["pct"], m["w"], m["order"], nfr)
            if r is None: i += 1; continue
            exi, net = r; trades.append((frame.index[exi], CAP * net / 100))
            while i < len(eval_idx) and eval_idx[i] <= exi: i += 1
        return trades

    e2 = list(np.where((yr == 2025) | (yr == 2026))[0]); e26 = list(np.where(yr == 2026)[0])
    for name, (mine, mn) in RULES.items():
        keep = {s for s in segs if segs[s]["pre_etv"] >= mine and segs[s]["pre_n"] >= mn} if mn else set(segs)
        out2[name].append((symbol, book(e2, keep)))
        out26[name].append((symbol, book(e26, keep)))


def report(title, out):
    print(f"\n============ {title} ============")
    print(f"  {'keep rule':<18}{'profit Rs':>13}{'portDD Rs':>13}{'ret/DD':>8}{'pos wk':>8}{'trades':>8}")
    base = None
    for name in RULES:
        allp = [(d, p) for _, tr in out[name] for d, p in tr]
        prof = sum(p for _, p in allp); dd = maxdd(allp); ntr = len(allp)
        posw = sum(1 for _, tr in out[name] if sum(p for _, p in tr) > 0)
        rr = (prof / -dd) if dd < 0 else float("inf")
        if name.startswith("RAW"): base = prof
        tag = "" if name.startswith("RAW") else f"  keep {prof/base*100:>4.0f}%"
        print(f"  {name:<18}{prof:>13,.0f}{dd:>13,.0f}{rr:>8.2f}{posw:>8}{ntr:>8,}{tag}")


def main():
    con = sqlite3.connect(SNR); kc = sqlite3.connect(KDB)
    fno = {r[0]: r[1] for r in kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()}; kc.close()
    syms = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM unified_patterns").fetchall()]
    out2 = {k: [] for k in RULES}; out26 = {k: [] for k in RULES}
    for n, s in enumerate(syms, 1):
        try: run_stock(s, con, int(fno.get(s, 0)), out2, out26)
        except Exception: pass
        if n % 100 == 0: print(f"  ...{n}/{len(syms)}", flush=True)
    con.close()
    report("M10b A-PRIORI SELECTION  ·  eval 2025-26  (filter uses <=2024 only)", out2)
    report("M10b A-PRIORI SELECTION  ·  eval 2026 ONLY (sealed year)", out26)


if __name__ == "__main__":
    main()
