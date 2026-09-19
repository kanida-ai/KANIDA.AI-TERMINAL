"""
TRAILED BOOK + dashboard — the honest source of truth. Applies each worker's WALK-FORWARD-VALIDATED
intraday trail (from trail_optimizer.csv, 0.5% slippage) to its MIS-Daily short days; single trades
(CNC/NRML/MIS-1d) keep their 1-min fills. Rebuilds worker cards + the dashboard on the trailed P&L, so
drawdowns reflect the real, trail-managed book. Parallel + vectorized (sub-minute for the sellable set).
Run: PYTHONIOENCODING=utf-8 SLIP_CAP=0.5 python arena/backtest_trailed.py
"""
import os, sys, json, sqlite3, time
os.environ.setdefault("OMP_NUM_THREADS", "1"); os.environ.setdefault("SLIP_CAP", "0.5")
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "arena")); sys.path.insert(0, str(ROOT / "scripts")); sys.path.insert(0, str(ROOT / "kanida_engine"))
import features as FE
import routing, backtest_1min as B, intraday_trail as IT, worker_cards as W, dashboard_1min as D
SNR = str(ROOT / "db" / "KANIDA_SNR.db"); KDB = str(ROOT / "db" / "kanida.db"); REP = ROOT / "reports"
MARGIN = B.MARGIN
NAME2CFG = {"no-trail": ("baseline", None), "cap 6/2/5/3": ("capital", (6, 2, 5, 3)),
            "cap 8/3/4/2": ("capital", (8, 3, 4, 2)), "cap 10/4/6/3": ("capital", (10, 4, 6, 3)),
            "cap 5/2/3/1.5": ("capital", (5, 2, 3, 1.5)), "atr K1.2 w14": ("atr", (1.2, 14)),
            "atr K2.0 w14": ("atr", (2.0, 14)), "donch N20": ("donchian", (20,)), "donch N40": ("donchian", (40,))}


def load_trailmap():
    tm = {}; f = REP / "trail_optimizer.csv"
    if f.exists():
        for r in pd.read_csv(f).to_dict("records"):
            if int(r.get("validated", 0)) == 1: tm[r["symbol"]] = NAME2CFG.get(r["chosen_trail"], ("baseline", None))
    return tm


TRAILMAP = load_trailmap()


def worker(args):
    sym, is_fno = args
    con = sqlite3.connect(SNR)
    rows = [(t, rj) for t, rj in con.execute("SELECT target,rule_json FROM unified_patterns WHERE symbol=?", (sym,)).fetchall() if t in B.TGT]
    con.close()
    if not rows: return sym, [], 0
    frame = FE.load_frame(sym, lookback_N=5)
    if frame.empty: return sym, [], 0
    kept, net = B.sel_kept(frame, rows, is_fno)
    if not kept: return sym, [], 0
    m1 = B.load_1min(sym)
    if not m1: return sym, [], 0
    O = frame["_o"].values; H = frame["_h"].values; L = frame["_l"].values; C = frame["_c"].values
    yr = frame["year"].values; idx = frame.index; n = len(O); dstr = np.array([str(x.date()) for x in idx])
    method, params = TRAILMAP.get(sym, ("baseline", None))               # validated trail, else no-trail
    te = np.where((yr == 2025) | (yr == 2026))[0]; out = []; day = 0; pid = 0
    while day < len(te):
        gi = te[day]; chosen = None
        for pe, t, w, mask in kept:
            if mask[gi] and np.isfinite(net[t][gi]): chosen = t; break
        if chosen is None: day += 1; continue
        t = chosen; d, pct, w = B.TGT[t]; product, mode = routing.route(d, w, is_fno); long = (d == "up")
        r = routing.sim_roc(O, H, L, C, gi, d, pct, w, product, mode)
        if r is None: day += 1; continue
        exi = r[2]; pid += 1
        if mode == "daily":                                              # MIS-Daily short -> apply the trail
            leg = 0
            for x in range(gi + 1, min(gi + 1 + w, n)):
                dd = m1.get(dstr[x])
                if dd is None or len(dd["o"]) < 5: continue
                res = IT.sim_day(dd, method, params)
                if res is None: continue
                cap, ex_px, ex_hm, reason = res; leg += 1; entry = float(dd["o"][0])
                out.append({"symbol": sym, "position_id": pid, "leg_no": leg, "total_legs": w,
                            "signal_date": dstr[gi], "entry_date": dstr[x], "entry_time": "09:15",
                            "entry_price": round(entry, 2), "exit_date": dstr[x], "exit_time": ex_hm,
                            "exit_price": ex_px, "direction": "Short", "order_type": "MIS-Daily",
                            "net_pnl_rs": round(cap * MARGIN / 100.0), "hold": int(exi - gi), "exit_reason": reason})
            day += max(1, exi - gi)
        else:                                                            # single trade: keep 1-min fill (no intraday trail here)
            eday = dstr[gi + 1]; dd = m1.get(eday)
            if dd is None or len(dd["o"]) == 0: day += max(1, exi - gi); continue
            entry = float(dd["o"][0]); tgt = entry * (1 + pct / 100) if long else entry * (1 - pct / 100)
            ex_px = ex_hm = ex_date = None; reason = "time-exit"
            for x in range(gi + 1, min(gi + 1 + w, n)):
                dx = m1.get(dstr[x])
                if dx is None: continue
                hit = np.where(dx["h"] >= tgt)[0] if long else np.where(dx["l"] <= tgt)[0]
                if len(hit): ex_px = tgt; ex_hm = dx["hm"][hit[0]]; ex_date = dstr[x]; reason = "target-hit"; break
            if ex_px is None:
                lastd = m1.get(dstr[exi]) or dd
                if product == "MIS": ex_px, ex_hm = B.px_at(lastd, "15:20")
                else: ex_px, ex_hm = float(lastd["c"][-1]), "15:30"
                ex_date = dstr[exi]
            out.append(B.mk(sym, pid, 1, 1, dstr[gi], eday, "09:15", entry, ex_date, ex_hm, ex_px,
                            long, product, product, reason, exi - gi))
            day += max(1, exi - gi)
    return sym, out, len(kept)


def main():
    import multiprocessing as mp
    T0 = time.time()
    con = sqlite3.connect(SNR); kc = sqlite3.connect(KDB)
    fno = {r[0]: r[1] for r in kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()}; kc.close()
    syms = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM unified_patterns").fetchall()]; con.close()
    tasks = [(s, int(fno.get(s, 0))) for s in syms]
    tn = {}
    if (REP / "trail_optimizer.csv").exists():
        tn = {r["symbol"]: r["chosen_trail"] for r in pd.read_csv(REP / "trail_optimizer.csv").to_dict("records") if int(r.get("validated", 0)) == 1}
    cards = []; alllegs = []
    with mp.get_context("spawn").Pool(8) as pool:
        for sym, legs, keptn in pool.imap_unordered(worker, tasks):
            if not legs: continue
            alllegs.extend(legs)
            c = D.card_from_legs(sym, legs, keptn)
            if c:
                c["trail"] = tn.get(sym, "no-trail"); cards.append(c)
    cards.sort(key=lambda c: (c["sustained"] and c.get("ret_dd", 0) >= 10, c.get("ret_dd", 0)), reverse=True)
    (REP / "worker_cards.json").write_text(json.dumps(cards), encoding="utf-8")
    W.render_html(cards)
    pd.DataFrame(alllegs).to_csv(REP / "sellable_tradelogs_trailed.csv", index=False)
    sell = sum(1 for c in cards if c.get("ret_dd", 0) >= 10 and c["sustained"])
    dt = time.time() - T0
    print(f"TRAILED DASHBOARD: {len(cards)} cards | sellable(>=+1%): {sell} | {dt:.1f}s [{'PASS' if dt < 300 else 'OVER'} 5-min]")
    print("  written: docs/kanida_workers.html + reports/worker_cards.json + sellable_tradelogs_trailed.csv (trailed source of truth)")


if __name__ == "__main__":
    main()
