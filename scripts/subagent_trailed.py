"""
Sub-agent selection HEAD-TO-HEAD vs the dashboard +300%, holding the trailing stop CONSTANT.
Three trailed books on sealed 2025-26, all with the worker's validated trail applied:
  1) DASHBOARD  — exact backtest_trailed.worker() (segment-level sel_kept + trail) = the +300% you see
  2) ALL patterns + trail (pattern-level, target dir)   — pattern-level "blend everything"
  3) SUB-AGENTS + trail (pattern-level, top-K by <=2024 earning power, preferred dir)
Leak-free: sub-agent ranking uses <=2024 only. Run: python scripts/subagent_trailed.py FSL
"""
from __future__ import annotations
import os, sys, time
os.environ.setdefault("SLIP_CAP", "0.5")
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
for p in ("scripts", "kanida_engine", "arena"):
    sys.path.insert(0, str(ROOT / p))
from subagent_engine import analyze, MIN_OCC          # noqa: E402
import features as FE, routing, backtest_1min as B, intraday_trail as IT  # noqa: E402
import backtest_trailed as BT                          # noqa: E402
MARGIN = B.MARGIN; REP = ROOT / "reports"


def trailmap(sym):
    f = REP / "trail_optimizer.csv"
    for r in pd.read_csv(f).to_dict("records"):
        if r["symbol"] == sym and int(r.get("validated", 0)) == 1:
            return BT.NAME2CFG.get(r["chosen_trail"], ("baseline", None)), r["chosen_trail"]
    return ("baseline", None), "no-trail"


def trailed_walk(frame, m1, is_fno, pool, method, params):
    """Same campaign+trail machinery as backtest_trailed, driven by a custom pattern pool. Returns leg rocs (chrono)."""
    O = frame["_o"].values; H = frame["_h"].values; L = frame["_l"].values; C = frame["_c"].values
    yr = frame["year"].values; idx = frame.index; n = len(O); dstr = np.array([str(x.date()) for x in idx])
    te = np.where((yr == 2025) | (yr == 2026))[0]
    pats = sorted(pool, key=lambda p: -p["rank"])
    rocs = []; day = 0; pid = 0
    while day < len(te):
        gi = te[day]; chosen = None
        for p in pats:
            if p["mask"][gi]: chosen = p; break
        if chosen is None: day += 1; continue
        d = chosen["dir"]; pct = chosen["pct"]; w = chosen["w"]
        product, mode = routing.route(d, w, is_fno); long = (d == "up")
        r = routing.sim_roc(O, H, L, C, gi, d, pct, w, product, mode)
        if r is None: day += 1; continue
        exi = r[2]; pid += 1
        if mode == "daily":                                     # MIS-Daily short -> apply the intraday trail
            for x in range(gi + 1, min(gi + 1 + w, n)):
                dd = m1.get(dstr[x])
                if dd is None or len(dd["o"]) < 5: continue
                res = IT.sim_day(dd, method, params)
                if res is None: continue
                rocs.append((int(yr[x]), float(res[0]), pid))    # (year, cap% invested-cash roc, campaign id)
            day += max(1, exi - gi)
        else:                                                    # single trade: 1-min fill (matches dashboard)
            eday = dstr[gi + 1]; dd = m1.get(eday)
            if dd is None or len(dd["o"]) == 0: day += max(1, exi - gi); continue
            entry = float(dd["o"][0]); tgt = entry * (1 + pct / 100) if long else entry * (1 - pct / 100)
            expx = None
            for x in range(gi + 1, min(gi + 1 + w, n)):
                dx = m1.get(dstr[x])
                if dx is None: continue
                hit = np.where(dx["h"] >= tgt)[0] if long else np.where(dx["l"] <= tgt)[0]
                if len(hit): expx = tgt; break
            if expx is None:
                lastd = m1.get(dstr[exi]) or dd
                expx = B.px_at(lastd, "15:20")[0] if product == "MIS" else float(lastd["c"][-1])
            gross = (expx / entry - 1) * 100 if long else (1 - expx / entry) * 100
            rocs.append((int(yr[gi + 1]), float((gross - routing.COST[product]) * routing.LEV[product]), pid))
            day += max(1, exi - gi)
    return rocs


def gauntlet(legs):
    """The SAME 6-gate readiness gauntlet, computed on a trailed leg list [(year, roc, campaign_id)]."""
    if not legs:
        return {"tier": "NOT READY", "gates": 0, "r2025": 0.0, "r2026": 0.0, "retdd26": 0.0, "worst": 0.0, "n26": 0}
    yrs = np.array([l[0] for l in legs]); a = np.array([l[1] for l in legs], float); pids = np.array([l[2] for l in legs])
    r25 = float(a[yrs == 2025].sum()); r26 = float(a[yrs == 2026].sum())
    a26 = a[yrs == 2026]
    cr = np.cumsum(a26); dd26 = float((cr - np.maximum.accumulate(cr)).min()) if len(cr) else 0.0
    retdd = (r26 / -dd26) if dd26 < 0 else (float("inf") if r26 > 0 else 0.0)
    n26 = int(len(set(pids[yrs == 2026].tolist())))
    p26_2x = r26 - 0.80 * len(a26)
    worst = float(a26.min()) if len(a26) else 0.0
    cra = np.cumsum(a); eqf = 100000.0 * (1 + cra / 100.0); pk = np.maximum.accumulate(eqf)
    acct_mdd = float(((eqf - pk) / pk * 100).min()) if len(cra) else 0.0        # G7: 2-yr account drawdown
    G = [r26 > 0, n26 >= 8, retdd >= 1.0, (r25 > 0 and r26 > 0), p26_2x > 0, worst >= -40.0, acct_mdd >= -35.0]
    passed = int(sum(G))
    tier = "READY" if passed == 7 else ("NEAR" if passed >= 5 else "NOT READY")
    return {"tier": tier, "gates": passed, "r2025": r25, "r2026": r26, "retdd26": round(retdd, 2),
            "worst": round(worst, 1), "n26": n26, "acct_mdd": round(acct_mdd, 1)}


def metrics(rocs):
    if len(rocs) == 0: return {"total": 0, "acct_mdd": 0, "fixed_dd": 0, "n": 0, "r2025": 0, "r2026": 0}
    if isinstance(rocs[0], (tuple, list)):
        yrs = np.array([r[0] for r in rocs]); a = np.array([r[1] for r in rocs], float)
    else:
        yrs = np.zeros(len(rocs)); a = np.array(rocs, float)
    cr = np.cumsum(a); peak = np.maximum.accumulate(cr)
    fixed_dd = float((cr - peak).min())
    eqf = 100000.0 * (1 + cr / 100.0); pk = np.maximum.accumulate(eqf)
    acct = float(((eqf - pk) / pk * 100).min())
    return {"total": float(a.sum()), "acct_mdd": acct, "fixed_dd": fixed_dd, "n": len(a),
            "r2025": float(a[yrs == 2025].sum()), "r2026": float(a[yrs == 2026].sum())}


def dashboard_ref(sym, is_fno):
    """Exact +300% reference: run the real trailed worker and read its leg rocs."""
    _, legs, _ = BT.worker((sym, is_fno))
    rocs = [(int(str(lg["exit_date"])[:4]), lg["net_pnl_rs"] / MARGIN * 100.0) for lg in legs]
    return metrics(rocs)


def main():
    sym = sys.argv[1] if len(sys.argv) > 1 else "FSL"
    import sqlite3
    kc = sqlite3.connect(str(ROOT / "db" / "kanida.db"))
    is_fno = int(dict(kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()).get(sym, 0)); kc.close()
    (method, params), tname = trailmap(sym)
    t0 = time.time()
    res = analyze(sym, is_fno)
    frame = FE.load_frame(sym, lookback_N=5); m1 = B.load_1min(sym)
    cards = res["cards"]
    tdir = lambda c: "up" if c["target"][0] == "u" else "dn"
    all_pool = [{"mask": c["mask"], "dir": tdir(c), "pct": c["pct"], "w": c["w"], "rank": c["rank_ecr"]}
                for c in cards if c["keep"]]
    kept_pref = sorted([c for c in cards if c["keep"]], key=lambda c: -c["rank_ecr"])
    dash = dashboard_ref(sym, is_fno)
    base = metrics(trailed_walk(frame, m1, is_fno, all_pool, method, params))
    dt = time.time() - t0
    print(f"\n===== {sym}: SUB-AGENT + TRAIL  vs  DASHBOARD +300% (sealed 2025-26, trail='{tname}') =====")
    print(f"  {'book':<44}{'return':>9}{'acctMDD':>9}{'ret/DD':>8}{'legs':>6}")
    def line(name, m):
        rd = m["total"] / -m["acct_mdd"] if m["acct_mdd"] < 0 else float('inf')
        print(f"  {name:<44}{m['total']:>+8.0f}%{m['acct_mdd']:>+8.1f}%{rd:>8.1f}{m['n']:>6}")
    line("1) DASHBOARD (segment sel_kept + trail)", dash)
    line("2) ALL patterns + trail (blend everything)", base)
    for K in [5, 10, 20, len(kept_pref)]:
        pool = [{"mask": c["mask"], "dir": c["pref"], "pct": c["pct"], "w": c["w"], "rank": c["rank_ecr"]}
                for c in kept_pref[:K]]
        m = metrics(trailed_walk(frame, m1, is_fno, pool, method, params))
        tag = "all kept" if K == len(kept_pref) else f"top-{K}"
        line(f"3) SUB-AGENTS + trail ({tag}, preferred dir)", m)
    print(f"\n  [{dt:.2f}s]  dashboard sanity: {dash['total']:+.0f}% (should be ~+300%)")


if __name__ == "__main__":
    main()
