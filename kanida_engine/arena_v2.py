"""
KANIDA ARENA v2 — Constitutional Learning layer (per ARENA_SPEC.md).
Fixes v1: (1) single book per stock (no over-trading), (2) TAIL-CONTROL stops (bound the left tail ->
lifts ETV), (3) CONSTITUTIONAL SCORE (ETV + confidence + downside-variance + recent-drift) driving
PROPORTIONAL allocation + Keep/Watch/Test/Retire. Point-in-time: a signal is judged only from its OWN
prior closed trades. FROZEN hyperparameters. Compares ARENA (adaptive) vs STATIC (fixed best-lift,
full size) — both with stops — so the delta isolates the LEARNING.

Run: python arena_v2.py SYM1 SYM2 ...   -> reports/arena_v2_results.csv
"""
import sys, json, sqlite3
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\scripts")))
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\kanida_engine")))
from mine_phase1 import apply_rule
import features as FE

SNR = r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db"
CAP = 100_000.0
TGT = {"up_1pct_1d": ("up", 1, 1), "up_2pct_2d": ("up", 2, 2), "up_5pct_5d": ("up", 5, 5),
       "dn_1pct_1d": ("dn", 1, 1), "dn_2pct_2d": ("dn", 2, 2), "dn_5pct_5d": ("dn", 5, 5)}
LEV = {"MIS": 5.0, "CNC": 1.0, "NRML": 5.0}; COST = {"MIS": 0.08, "CNC": 0.15, "NRML": 0.05}
# ---------- FROZEN CONSTITUTION (a-priori) ----------
STOP_MULT = 2.0        # stop = 2x target adverse (bound tail, avoid whipsaw)
SCORE_WIN = 20         # rolling trades scored
MIN_CONF = 5           # below -> Test (explore small)
CONF_FULL = 40         # confidence ramps to 1.0 at 40 trades
TEST_WT = 0.25         # exploration size
DRIFT_RECENT = 8; VAR_PEN = 0.3; DRIFT_PEN = 0.5


def route(d, w):
    if w == 1: return "MIS", ("LONG" if d == "up" else "SHORT")
    return ("CNC", "LONG") if d == "up" else ("NRML", "SHORT")


def score_seg(closed):
    """Constitutional score from a signal's OWN prior closed net-ROCs (point-in-time)."""
    n = len(closed)
    if n < MIN_CONF:
        return dict(w=TEST_WT, score=0.0, status="Test")
    r = np.array(closed[-SCORE_WIN:]); etv = r.mean()
    dn = r[r < 0]; dsd = dn.std() if len(dn) > 1 else (abs(dn.mean()) if len(dn) else 0.0)
    drift = r[-DRIFT_RECENT:].mean() - etv if len(r) >= DRIFT_RECENT else 0.0
    sc = etv - VAR_PEN * dsd + DRIFT_PEN * drift
    if sc <= 0:
        return dict(w=0.0, score=sc, status="Retire")
    conf = min(1.0, n / CONF_FULL)
    return dict(w=TEST_WT + (1 - TEST_WT) * conf, score=sc, status=("Keep" if conf >= 0.5 else "Watch"))


def sim_trade(frame, days, i, d, pct, w, order):
    O, H, L, C = frame["_o"], frame["_h"], frame["_l"], frame["_c"]
    ed = days[i + 1]; entry = float(O.loc[ed])
    if not np.isfinite(entry) or entry <= 0: return None
    long = (d == "up")
    tgt = entry * (1 + pct / 100) if long else entry * (1 - pct / 100)
    stop = entry * (1 - pct * STOP_MULT / 100) if long else entry * (1 + pct * STOP_MULT / 100)
    wd = days[i + 1: i + 1 + w]; ex = None; exd = wd[-1]
    for x in wd:
        hi, lo = float(H.loc[x]), float(L.loc[x])
        if long:
            if lo <= stop: ex, exd = stop, x; break            # tail-control: stop checked first
            if hi >= tgt: ex, exd = tgt, x; break
        else:
            if hi >= stop: ex, exd = stop, x; break
            if lo <= tgt: ex, exd = tgt, x; break
    if ex is None: ex = float(C.loc[exd])
    gross = (ex / entry - 1) * 100 if long else (1 - ex / entry) * 100
    net_roc = (gross - COST[order]) * LEV[order]
    return days.index(exd), net_roc


def run_book(frame, segs, adaptive):
    days = list(frame[frame["year"].isin([2025, 2026])].index)
    closed = {s: [] for s in segs}; eq = []; total = 0.0; ntr = 0; wins = 0
    i = 0
    while i < len(days) - 1:
        t = days[i]
        firing = [s for s in segs if any(apply_rule(frame.loc[[t]], cds).iloc[0] for cds in segs[s]["conds"])]
        if not firing:
            i += 1; continue
        if adaptive:
            cand = []
            for s in firing:
                sc = score_seg(closed[s])
                if sc["w"] > 0: cand.append((sc["score"], s, sc["w"]))
            if not cand: i += 1; continue
            cand.sort(reverse=True); _, seg, weight = cand[0]
        else:
            seg = max(firing, key=lambda s: segs[s]["lift"]); weight = 1.0
        m = segs[seg]; r = sim_trade(frame, days, i, m["d"], m["pct"], m["w"], m["order"])
        if r is None: i += 1; continue
        exi, net_roc = r; pnl = CAP * net_roc / 100 * weight
        total += pnl; eq.append((days[exi], pnl)); ntr += 1; wins += (pnl > 0)
        closed[seg].append(net_roc)                            # worker learns from its own outcome
        i = exi + 1
    dd = float((pd.Series([e[1] for e in sorted(eq)]).cumsum().pipe(lambda s: s - s.cummax())).min()) if eq else 0.0
    return dict(pnl=round(total), dd=round(dd), trades=ntr, win=round(wins / ntr * 100, 1) if ntr else 0)


def run_stock(symbol, con):
    rows = con.execute("SELECT target,rule_json,lift_tr FROM unified_patterns WHERE symbol=? AND promoted=1",
                       (symbol,)).fetchall()
    if not rows: return None
    segs = {}
    for t, rj, lt in rows:
        if t not in TGT: continue
        d, pct, w = TGT[t]; order, dl = route(d, w); key = f"{order} {dl} {w}D"
        s = segs.setdefault(key, {"conds": [], "lift": 0, "d": d, "pct": pct, "w": w, "order": order})
        s["conds"].append([tuple(c) for c in json.loads(rj)]); s["lift"] = max(s["lift"], lt or 0)
    frame = FE.build_from_db(symbol, lookback_N=5)
    if frame.empty: return None
    stat = run_book(frame, segs, adaptive=False); arena = run_book(frame, segs, adaptive=True)
    return {"symbol": symbol, "static_pnl": stat["pnl"], "static_dd": stat["dd"], "static_win": stat["win"],
            "static_trades": stat["trades"], "arena_pnl": arena["pnl"], "arena_dd": arena["dd"],
            "arena_win": arena["win"], "arena_trades": arena["trades"]}


def main():
    con = sqlite3.connect(SNR)
    syms = sys.argv[1:] or [r[0] for r in con.execute("SELECT DISTINCT symbol FROM unified_patterns").fetchall()]
    res = []
    for s in syms:
        try:
            r = run_stock(s, con)
            if r:
                res.append(r)
                print(f"  {s:11} STATIC Rs{r['static_pnl']:>9,} DD{r['static_dd']:>9,} w{r['static_win']} | "
                      f"ARENA Rs{r['arena_pnl']:>9,} DD{r['arena_dd']:>9,} w{r['arena_win']} "
                      f"tr{r['arena_trades']}/{r['static_trades']}", flush=True)
        except Exception as e:
            print(f"  {s}: ERR {str(e)[:70]}")
    con.close()
    df = pd.DataFrame(res); df.to_csv(r"C:\Users\SPS\Documents\Kanida_Falcon\reports\arena_v2_results.csv", index=False)
    sp, ap, sdd, add = df.static_pnl.sum(), df.arena_pnl.sum(), df.static_dd.sum(), df.arena_dd.sum()
    print("\n============ ARENA v2 (Constitutional Score + tail-control) vs STATIC ============")
    print(f"  STATIC (fixed best-lift, full size, +stop): P&L Rs{sp:>12,}  maxDD Rs{sdd:>12,}")
    print(f"  ARENA  (adaptive ETV-score, proportional) : P&L Rs{ap:>12,}  maxDD Rs{add:>12,}")
    if sp: print(f"   delta: P&L {(ap-sp)/abs(sp)*100:+.1f}%   drawdown {(add-sdd)/abs(sdd)*100:+.1f}% "
                 f"(negative drawdown delta = smaller drawdown = better)")
    print(f"  stocks where ARENA beat STATIC on P&L: {(df.arena_pnl>df.static_pnl).sum()}/{len(df)}")


if __name__ == "__main__":
    main()
