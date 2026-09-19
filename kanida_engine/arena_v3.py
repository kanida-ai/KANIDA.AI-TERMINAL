"""
KANIDA ARENA v3 — PATTERN-level ETV + NRML-long leverage.
Each PATTERN is scored by its own Expected Trade Value over its FULL realized-outcome distribution
(full-win / partial / scratch / small-loss / large-loss — exactly the ETV scenarios), point-in-time,
with confidence (sample size), downside variance and recent drift = the Constitutional Score. The
worker picks the best-scored FIRING pattern each day (single book), sizes proportionally, retires
decayed ones, explores thin ones (Test). Tail-control stop bounds the left tail -> lifts ETV.

NRML-long fix: multi-day LONGS on F&O stocks use NRML futures 5x (not CNC 1x) — longs get the same
leverage as shorts, per the balanced-book point.

Compares ARENA (adaptive pattern-ETV) vs STATIC (fixed best-train-lift, full size) — both with stops
and the NRML-long leverage — so the delta isolates the LEARNING at pattern granularity.

Run: python arena_v3.py SYM1 SYM2 ...   -> reports/arena_v3_results.csv
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
STOP_MULT = 2.0; SCORE_WIN = 30; MIN_CONF = 8; CONF_FULL = 60
TEST_WT = 0.25; DRIFT_RECENT = 10; VAR_PEN = 0.3; DRIFT_PEN = 0.5


def route(d, w, is_fno):
    if w == 1: return "MIS"
    if d == "up": return "NRML" if is_fno else "CNC"   # NRML-long fix: F&O longs get 5x futures
    return "NRML"


def score_pattern(prior):
    n = len(prior)
    if n < MIN_CONF:
        return TEST_WT, None, "Test"
    r = np.array(prior[-SCORE_WIN:]); etv = r.mean()
    dn = r[r < 0]; dsd = dn.std() if len(dn) > 1 else (abs(dn.mean()) if len(dn) else 0.0)
    drift = r[-DRIFT_RECENT:].mean() - etv if len(r) >= DRIFT_RECENT else 0.0
    sc = etv - VAR_PEN * dsd + DRIFT_PEN * drift
    if sc <= 0:
        return 0.0, sc, "Retire"
    conf = min(1.0, n / CONF_FULL)
    return TEST_WT + (1 - TEST_WT) * conf, sc, ("Keep" if conf >= 0.5 else "Watch")


def sim(frame_np, i, d, pct, w, order, days_len):
    O, H, L, C = frame_np
    if i + 1 >= days_len: return None
    entry = O[i + 1]
    if not np.isfinite(entry) or entry <= 0: return None
    long = (d == "up")
    tgt = entry * (1 + pct / 100) if long else entry * (1 - pct / 100)
    stop = entry * (1 - pct * STOP_MULT / 100) if long else entry * (1 + pct * STOP_MULT / 100)
    end = min(i + w, days_len - 1); ex = None; exi = end
    for x in range(i + 1, i + 1 + w):
        if x >= days_len: break
        hi, lo = H[x], L[x]
        if long:
            if lo <= stop: ex, exi = stop, x; break
            if hi >= tgt: ex, exi = tgt, x; break
        else:
            if hi >= stop: ex, exi = stop, x; break
            if lo <= tgt: ex, exi = tgt, x; break
        exi = x
    if ex is None: ex = C[exi]
    gross = (ex / entry - 1) * 100 if long else (1 - ex / entry) * 100
    return exi, (gross - COST[order]) * LEV[order]


def run_stock(symbol, con, is_fno):
    rows = con.execute("SELECT target,rule_json,lift_tr FROM unified_patterns WHERE symbol=? AND promoted=1",
                       (symbol,)).fetchall()
    if not rows: return None
    frame = FE.build_from_db(symbol, lookback_N=5)
    if frame.empty: return None
    days = list(frame.index); n = len(days)
    frame_np = (frame["_o"].values, frame["_h"].values, frame["_l"].values, frame["_c"].values)
    year = frame["year"].values
    pats = []
    for t, rj, lt in rows:
        if t not in TGT: continue
        d, pct, w = TGT[t]; order = route(d, w, is_fno)
        conds = [tuple(c) for c in json.loads(rj)]
        fb = apply_rule(frame, conds).values
        outs = {}
        for i in np.where(fb)[0]:
            r = sim(frame_np, int(i), d, pct, w, order, n)
            if r: outs[int(i)] = r
        if outs:
            pats.append({"lift": lt or 0, "outs": outs, "sig": sorted(outs)})
    if not pats: return None
    start = int(np.argmax(year >= 2025))

    def book(adaptive):
        closed = {id(p): [] for p in pats}; total = 0.0; eq = []; ntr = 0; wins = 0; i = start
        while i < n - 1:
            firing = [p for p in pats if i in p["outs"]]
            if not firing:
                i += 1; continue
            if adaptive:
                keepw = []; tests = []
                for p in firing:
                    prior = closed[id(p)]
                    w_, sc, st = score_pattern(prior)
                    if st in ("Keep", "Watch"): keepw.append((sc, w_, p))
                    elif st == "Test": tests.append((p["lift"], w_, p))
                if keepw: keepw.sort(key=lambda z: z[0], reverse=True); _, weight, chosen = keepw[0]
                elif tests: tests.sort(key=lambda z: z[0], reverse=True); _, weight, chosen = tests[0]
                else: i += 1; continue
            else:
                chosen = max(firing, key=lambda p: p["lift"]); weight = 1.0
            exi, net_roc = chosen["outs"][i]; pnl = CAP * net_roc / 100 * weight
            total += pnl; eq.append((exi, pnl)); ntr += 1; wins += (pnl > 0)
            closed[id(chosen)].append(net_roc)
            i = exi + 1
        dd = float((pd.Series([e[1] for e in sorted(eq)]).cumsum().pipe(lambda s: s - s.cummax())).min()) if eq else 0
        return round(total), round(dd), ntr, round(wins / ntr * 100, 1) if ntr else 0

    sp, sdd, stn, sw = book(False); ap, add, atn, aw = book(True)
    return {"symbol": symbol, "is_fno": is_fno, "static_pnl": sp, "static_dd": sdd, "static_trades": stn,
            "arena_pnl": ap, "arena_dd": add, "arena_win": aw, "arena_trades": atn}


def main():
    con = sqlite3.connect(SNR); kc = sqlite3.connect(KDB)
    fno = {r[0]: r[1] for r in kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()}; kc.close()
    syms = sys.argv[1:] or [r[0] for r in con.execute("SELECT DISTINCT symbol FROM unified_patterns").fetchall()]
    res = []
    for s in syms:
        try:
            r = run_stock(s, con, int(fno.get(s, 0)))
            if r:
                res.append(r)
                print(f"  {s:11} STATIC Rs{r['static_pnl']:>9,} DD{r['static_dd']:>9,} | "
                      f"ARENA Rs{r['arena_pnl']:>9,} DD{r['arena_dd']:>9,} w{r['arena_win']} "
                      f"tr{r['arena_trades']}/{r['static_trades']}", flush=True)
        except Exception as e:
            print(f"  {s}: ERR {str(e)[:70]}")
    con.close()
    df = pd.DataFrame(res); df.to_csv(r"C:\Users\SPS\Documents\Kanida_Falcon\reports\arena_v3_results.csv", index=False)
    sp, ap, sdd, add = df.static_pnl.sum(), df.arena_pnl.sum(), df.static_dd.sum(), df.arena_dd.sum()
    print("\n===== ARENA v3 (pattern-ETV + NRML-long) vs STATIC =====")
    print(f"  STATIC: P&L Rs{sp:>12,}  maxDD Rs{sdd:>12,}")
    print(f"  ARENA : P&L Rs{ap:>12,}  maxDD Rs{add:>12,}")
    if sp: print(f"   delta P&L {(ap-sp)/abs(sp)*100:+.1f}%   drawdown {(add-sdd)/abs(sdd)*100:+.1f}%")
    print(f"  ARENA beat STATIC on P&L: {(df.arena_pnl>df.static_pnl).sum()}/{len(df)} | "
          f"ARENA net-positive: {(df.arena_pnl>0).sum()}/{len(df)}")


if __name__ == "__main__":
    main()
