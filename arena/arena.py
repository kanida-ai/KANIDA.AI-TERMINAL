"""
KANIDA ARENA — the ONE consolidated Autonomous Learning / Constitutional layer.
(Supersedes the throwaway kanida_engine/arena.py [v1], arena_v2.py, arena_v3.py comparison scripts.)

Per-stock worker. Objective = maximize long-term Expected Trade Value (ETV) under the Constitutional
Score (ETV + confidence + downside-variance + recent-drift), with tail-control stops, proportional
allocation, and a Keep/Watch/Test/Retire roster. Strictly point-in-time; frozen hyperparameters.
Reads the FAST frame cache (features.load_frame) so a full run is seconds, not minutes.

Config flags below toggle the roadmap pieces (score-conditional NRML-long leverage, stop width, etc.).

Run: PYTHONIOENCODING=utf-8 python arena.py SYM1 SYM2 ...   (default: all stocks in unified_patterns)
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
# ---------- CANONICAL POLICY (M10b: a-priori point-in-time selection) ----------
# Proven Aug-2026 on all 441 workers: keep a segment only if its ETV on <=2024 signals was positive,
# then trade kept segments at full breadth. Leak-free vs the eval year. Beats "trade everything":
# sealed-2026 ret/DD 0.26 -> 0.61 (profit +76%, DD -25%, more workers positive). See arena/m10b_*.py.
SELECT_ETV_MIN = 0.0      # keep segment iff pre-2025 ETV (net %/trade) > this bar
SELECT_MIN_N   = 1        # ...with at least this many pre-2025 signals (else "no evidence" -> dropped)
STOP_ON        = False    # M10 DISPROVED reactive stops on this book -> off (STOP_MULT kept for audit)
ADAPTIVE       = False    # M10 DISPROVED reactive Constitutional-Score weighting -> off (kept for audit)
NRML_LONG      = False    # blanket 5x-long hurt -> longs run CNC 1x
# ---------- reactive-layer constants (audit only; inactive while STOP_ON/ADAPTIVE=False) ----------
STOP_MULT = 2.0; SCORE_WIN = 20; MIN_CONF = 5; CONF_FULL = 40
TEST_WT = 0.25; DRIFT_RECENT = 8; VAR_PEN = 0.3; DRIFT_PEN = 0.5


def route(d, w, is_fno):
    if w == 1: return "MIS", ("LONG" if d == "up" else "SHORT")
    if d == "up": return ("NRML" if (is_fno and NRML_LONG) else "CNC"), "LONG"
    return "NRML", "SHORT"


def constitutional_score(closed):
    """ETV + confidence + downside-variance + recent-drift -> (weight, score, roster status)."""
    n = len(closed)
    if n < MIN_CONF:
        return TEST_WT, 0.0, "Test"
    r = np.array(closed[-SCORE_WIN:]); etv = r.mean()
    dn = r[r < 0]; dsd = dn.std() if len(dn) > 1 else (abs(dn.mean()) if len(dn) else 0.0)
    drift = r[-DRIFT_RECENT:].mean() - etv if len(r) >= DRIFT_RECENT else 0.0
    sc = etv - VAR_PEN * dsd + DRIFT_PEN * drift
    if sc <= 0:
        return 0.0, sc, "Retire"
    conf = min(1.0, n / CONF_FULL)
    return TEST_WT + (1 - TEST_WT) * conf, sc, ("Keep" if conf >= 0.5 else "Watch")


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
            if STOP_ON and lo <= stop: ex, exd = stop, x; break
            if hi >= tgt: ex, exd = tgt, x; break
        else:
            if STOP_ON and hi >= stop: ex, exd = stop, x; break
            if lo <= tgt: ex, exd = tgt, x; break
    if ex is None: ex = float(C.loc[exd])
    gross = (ex / entry - 1) * 100 if long else (1 - ex / entry) * 100
    return days.index(exd), (gross - COST[order]) * LEV[order]


def segment_etv(frame, days, seg):
    """ETV (mean net ROC %/trade) of a segment over the given point-in-time signal days."""
    rocs = []
    for i in range(len(days) - 1):
        if not seg["fire_all"].loc[days[i]]: continue
        r = sim_trade(frame, days, i, seg["d"], seg["pct"], seg["w"], seg["order"])
        if r is not None: rocs.append(r[1])
    return len(rocs), (float(np.mean(rocs)) if rocs else -1e9)


def book(frame, days, segs, keep, adaptive):
    """Trade kept segments over `days`; best-lift among firing (or adaptive score if ADAPTIVE)."""
    fire = {s: segs[s]["fire_all"].loc[days].values for s in segs}
    closed = {s: [] for s in segs}; total = 0.0; eq = []; ntr = 0; wins = 0; i = 0
    while i < len(days) - 1:
        firing = [s for s in segs if s in keep and fire[s][i]]
        if not firing: i += 1; continue
        if adaptive:
            cand = [(constitutional_score(closed[s])[1], constitutional_score(closed[s])[0], s)
                    for s in firing if constitutional_score(closed[s])[0] > 0]
            if not cand: i += 1; continue
            cand.sort(reverse=True); _, weight, seg = cand[0]
        else:
            seg = max(firing, key=lambda s: segs[s]["lift"]); weight = 1.0
        m = segs[seg]; r = sim_trade(frame, days, i, m["d"], m["pct"], m["w"], m["order"])
        if r is None: i += 1; continue
        exi, net_roc = r; pnl = CAP * net_roc / 100 * weight
        total += pnl; eq.append((days[exi], pnl)); ntr += 1; wins += (pnl > 0)
        closed[seg].append(net_roc); i = exi + 1
    dd = float((pd.Series([e[1] for e in sorted(eq)]).cumsum().pipe(lambda s: s - s.cummax())).min()) if eq else 0
    return round(total), round(dd), ntr, round(wins / ntr * 100, 1) if ntr else 0


def run_stock(symbol, con, is_fno):
    rows = con.execute("SELECT target,rule_json,lift_tr FROM unified_patterns WHERE symbol=? AND promoted=1",
                       (symbol,)).fetchall()
    if not rows: return None
    segs = {}
    for t, rj, lt in rows:
        if t not in TGT: continue
        d, pct, w = TGT[t]; order, dl = route(d, w, is_fno); key = f"{order} {dl} {w}D"
        s = segs.setdefault(key, {"conds": [], "lift": 0, "d": d, "pct": pct, "w": w, "order": order})
        s["conds"].append([tuple(c) for c in json.loads(rj)]); s["lift"] = max(s["lift"], lt or 0)
    frame = FE.load_frame(symbol, lookback_N=5)
    if frame.empty: return None
    for s in segs:                                   # OR-combine each segment's patterns -> one fire mask
        m = np.zeros(len(frame), bool)
        for cds in segs[s]["conds"]: m |= apply_rule(frame, cds).values
        segs[s]["fire_all"] = pd.Series(m, index=frame.index)
    # ---- a-priori selection: keep segments whose <=2024 ETV cleared the bar (point-in-time, leak-free) ----
    pre = list(frame[frame["year"] <= 2024].index)
    keep = set()
    for s in segs:
        n, etv = segment_etv(frame, pre, segs[s]); segs[s]["pre_n"] = n; segs[s]["pre_etv"] = etv
        if etv > SELECT_ETV_MIN and n >= SELECT_MIN_N: keep.add(s)
    days = list(frame[frame["year"].isin([2025, 2026])].index)
    rp, rdd, rtn, _ = book(frame, days, segs, set(segs), False)          # RAW: trade everything
    ap, add, atn, aw = book(frame, days, segs, keep, ADAPTIVE)           # ARENA: a-priori selected
    return {"symbol": symbol, "segments": len(segs), "kept": len(keep),
            "static_pnl": rp, "static_dd": rdd, "static_trades": rtn,
            "arena_pnl": ap, "arena_dd": add, "arena_trades": atn, "arena_win": aw}


def main():
    con = sqlite3.connect(SNR); kc = sqlite3.connect(KDB)
    fno = {r[0]: r[1] for r in kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()}; kc.close()
    syms = sys.argv[1:] or [r[0] for r in con.execute("SELECT DISTINCT symbol FROM unified_patterns").fetchall()]
    res = []
    for s in syms:
        try:
            r = run_stock(s, con, int(fno.get(s, 0)))
            if r: res.append(r)
        except Exception as e:
            print(f"  {s}: ERR {str(e)[:70]}")
    con.close()
    df = pd.DataFrame(res); df.to_csv(r"C:\Users\SPS\Documents\Kanida_Falcon\reports\arena_results.csv", index=False)
    sp, ap = df.static_pnl.sum(), df.arena_pnl.sum()
    print(f"ARENA a-priori selection ({len(df)} workers): "
          f"RAW Profit Rs{sp:,.0f} ({int((df.static_pnl>0).sum())} wk+) | "
          f"SELECTED Profit Rs{ap:,.0f} ({int((df.arena_pnl>0).sum())} wk+) | "
          f"profit x{ap/sp:.2f}, SELECTED beats RAW {int((df.arena_pnl>df.static_pnl).sum())}/{len(df)}")


if __name__ == "__main__":
    main()
