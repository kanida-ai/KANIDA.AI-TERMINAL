"""
M10 — risk-managed Arena book on the full universe. Score-conditional leverage + ETV stops +
NRML-long un-cap. Runs off the frame cache (seconds). Produces, for all 441 workers:
  RAW book (no stops, blanket leverage, best-lift)  vs  ARENA book (stops + Constitutional Score +
  proportional/score-conditional sizing + NRML-long) — portfolio return, DRAWDOWN, and the worker
  readiness funnel. Answers M10's impact question: how much upside survives, how much drawdown is cut,
  and how the funnel moves.
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
STOP_MULT = 3.0          # M10: wider stop — keep more upside, still bound the catastrophic tail
SCORE_WIN = 20; MIN_CONF = 5; CONF_FULL = 40; TEST_WT = 0.25; DRIFT_RECENT = 8; VAR_PEN = 0.3; DRIFT_PEN = 0.5


import os
NRML_LONG = os.environ.get("NRML_LONG", "0") == "1"   # default OFF (blanket 5x-long hurt in tests)


def route(d, w, is_fno):
    if w == 1: return "MIS", ("LONG" if d == "up" else "SHORT")
    if d == "up": return ("NRML" if (is_fno and NRML_LONG) else "CNC"), "LONG"
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


def sim(fnp, i, d, pct, w, order, n, stop_on):
    O, H, L, C = fnp
    if i + 1 >= n: return None
    entry = O[i + 1]
    if not np.isfinite(entry) or entry <= 0: return None
    long = (d == "up"); tgt = entry * (1 + pct / 100) if long else entry * (1 - pct / 100)
    stop = entry * (1 - pct * STOP_MULT / 100) if long else entry * (1 + pct * STOP_MULT / 100)
    ex = None; exi = min(i + w, n - 1)
    for x in range(i + 1, min(i + 1 + w, n)):
        hi, lo = H[x], L[x]
        if long:
            if stop_on and lo <= stop: ex, exi = stop, x; break
            if hi >= tgt: ex, exi = tgt, x; break
        else:
            if stop_on and hi >= stop: ex, exi = stop, x; break
            if lo <= tgt: ex, exi = tgt, x; break
        exi = x
    if ex is None: ex = C[exi]
    gross = (ex / entry - 1) * 100 if long else (1 - ex / entry) * 100
    return exi, (gross - COST[order]) * LEV[order]


def maxdd(trades):
    if not trades: return 0.0
    s = pd.Series([p for _, p in sorted(trades)]).cumsum()
    return float((s - s.cummax()).min())


def run_stock(symbol, con, is_fno):
    rows = con.execute("SELECT target,rule_json,lift_tr FROM unified_patterns WHERE symbol=? AND promoted=1",
                       (symbol,)).fetchall()
    if not rows: return None
    segs = {}
    for t, rj, lt in rows:
        if t not in TGT: continue
        d, pct, w = TGT[t]; order, dl = route(d, w, is_fno); k = f"{order} {dl} {w}D"
        s = segs.setdefault(k, {"conds": [], "lift": 0, "d": d, "pct": pct, "w": w, "order": order})
        s["conds"].append([tuple(c) for c in json.loads(rj)]); s["lift"] = max(s["lift"], lt or 0)
    frame = FE.load_frame(symbol, lookback_N=5)
    if frame.empty: return None
    days = list(frame[frame["year"].isin([2025, 2026])].index)
    if len(days) < 5: return None
    fnp = (frame["_o"].values, frame["_h"].values, frame["_l"].values, frame["_c"].values)
    idx = {dt: k for k, dt in enumerate(frame.index)}
    day_idx = [idx[dt] for dt in days]
    for s in segs:
        m = np.zeros(len(frame), bool)
        for cds in segs[s]["conds"]: m |= apply_rule(frame, cds).values
        segs[s]["fire"] = m
    nfr = len(frame)

    def book(adaptive):
        closed = {s: [] for s in segs}; trades = []; total = 0.0; i = 0
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
            m = segs[seg]; r = sim(fnp, gi, m["d"], m["pct"], m["w"], m["order"], nfr, adaptive)
            if r is None: i += 1; continue
            exi, net_roc = r; pnl = CAP * net_roc / 100 * weight
            total += pnl; trades.append((frame.index[exi], pnl, seg)); closed[seg].append(net_roc)
            while i < len(day_idx) and day_idx[i] <= exi: i += 1
        return total, trades
    rt, rtr = book(False); at, atr = book(True)
    return {"symbol": symbol, "raw_pnl": rt, "raw_trades": rtr, "arena_pnl": at, "arena_trades": atr}


def main():
    con = sqlite3.connect(SNR); kc = sqlite3.connect(KDB)
    fno = {r[0]: r[1] for r in kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()}; kc.close()
    syms = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM unified_patterns").fetchall()]
    allraw = []; allar = []; rows = []
    for n, s in enumerate(syms, 1):
        try:
            r = run_stock(s, con, int(fno.get(s, 0)))
        except Exception:
            r = None
        if not r: continue
        allraw += [(d, p) for d, p, _ in r["raw_trades"]]; allar += [(d, p) for d, p, _ in r["arena_trades"]]
        rows.append({"symbol": s, "raw_pnl": round(r["raw_pnl"]), "arena_pnl": round(r["arena_pnl"]),
                     "raw_dd": round(maxdd([(d, p) for d, p, _ in r["raw_trades"]])),
                     "arena_dd": round(maxdd([(d, p) for d, p, _ in r["arena_trades"]]))})
        if n % 100 == 0: print(f"  ...{n}/{len(syms)}", flush=True)
    con.close()
    df = pd.DataFrame(rows); df.to_csv(r"C:\Users\SPS\Documents\Kanida_Falcon\reports\m10_worker_book.csv", index=False)
    rawp, arp = df.raw_pnl.sum(), df.arena_pnl.sum(); rdd, add = maxdd(allraw), maxdd(allar)
    print("\n================ M10 · RISK-MANAGED ARENA vs RAW (441 workers) ================")
    print(f"  RAW   book: Profit Rs{rawp:>14,.0f}  Portfolio maxDD Rs{rdd:>13,.0f}")
    print(f"  ARENA book: Profit Rs{arp:>14,.0f}  Portfolio maxDD Rs{add:>13,.0f}")
    print(f"   -> ARENA keeps {arp/rawp*100:.0f}% of raw profit, cuts drawdown by {(1-add/rdd)*100:.0f}%")
    print(f"\n  WORKER FUNNEL (profitable workers):")
    print(f"    RAW  : {int((df.raw_pnl>0).sum())}/{len(df)} positive")
    print(f"    ARENA: {int((df.arena_pnl>0).sum())}/{len(df)} positive | durable (positive AND smaller DD): "
          f"{int(((df.arena_pnl>0)&(df.arena_dd>df.raw_dd)).sum())}")
    # readiness tiers on the ARENA (risk-managed) book
    df["ret1L"] = df.arena_pnl / CAP * 100
    tiers = {"Cleared (>50%)": (df.ret1L >= 50).sum(), "Meeting (15-50%)": ((df.ret1L >= 15) & (df.ret1L < 50)).sum(),
             "Near (0-15%)": ((df.ret1L > 0) & (df.ret1L < 15)).sum(), "Needs work (<=0)": (df.ret1L <= 0).sum()}
    print("  ARENA readiness tiers:", {k: int(v) for k, v in tiers.items()})


if __name__ == "__main__":
    main()
