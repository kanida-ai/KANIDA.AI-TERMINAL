"""
Lock in the 3 rolling-method workers (HBLENGINE, IDBI, LATENTVIEW) into the dashboard, tagged method='rolling'.
Rebuilds worker_cards.json + docs/kanida_workers.html and refreshes worker_readiness_expanded.csv to 38.
Run: python scripts/rolling_dashboard.py
"""
from __future__ import annotations
import os, sys, json, sqlite3, time
os.environ.setdefault("SLIP_CAP", "0.5"); os.environ.setdefault("OMP_NUM_THREADS", "1")
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
for p in ("scripts", "kanida_engine", "arena"):
    sys.path.insert(0, str(ROOT / p))
import routing, backtest_1min as B, intraday_trail as IT
import dashboard_1min as D, worker_cards as W
from subagent_trailed import trailmap
from rolling_selector import prep, select_at
REP = ROOT / "reports"; MARGIN = B.MARGIN
THREE = ["HBLENGINE", "IDBI", "LATENTVIEW"]


def rolling_card(sym, is_fno):
    d = prep(sym, is_fno)
    frame = d["frame"]; qkey = d["qkey"]; mkey = d["mkey"]; pats = d["pats"]
    O = frame["_o"].values; H = frame["_h"].values; L = frame["_l"].values; C = frame["_c"].values
    yr = frame["year"].values; n = len(O); dstr = np.array([str(x.date()) for x in frame.index])
    m1 = B.load_1min(sym); (method, params), tname = trailmap(sym)
    te = np.where((yr == 2025) | (yr == 2026))[0]
    legs = []; day = 0; pid = 0; cur_m = -1; pool = []; npool = 0
    while day < len(te):
        gi = te[day]
        if mkey[gi] != cur_m:
            cur_m = mkey[gi]; pool = select_at(pats, gi, int(qkey[gi])); npool = max(npool, len(pool))
        chosen = None
        for p in pool:
            if p["mask"][gi]: chosen = p; break
        if chosen is None:
            day += 1; continue
        dd_, pct, w = chosen["dir"], chosen["pct"], chosen["w"]
        product, mode = routing.route(dd_, w, is_fno); long = (dd_ == "up")
        r = routing.sim_roc(O, H, L, C, gi, dd_, pct, w, product, mode)
        if r is None:
            day += 1; continue
        exi = r[2]; pid += 1
        if mode == "daily":
            leg = 0
            for x in range(gi + 1, min(gi + 1 + w, n)):
                g = m1.get(dstr[x])
                if g is None or len(g["o"]) < 5: continue
                res = IT.sim_day(g, method, params)
                if res is None: continue
                cap, expx, exhm, reason = res; leg += 1; entry = float(g["o"][0])
                legs.append({"symbol": sym, "position_id": pid, "leg_no": leg, "total_legs": w,
                             "signal_date": dstr[gi], "entry_date": dstr[x], "entry_time": "09:15",
                             "entry_price": round(entry, 2), "exit_date": dstr[x], "exit_time": exhm,
                             "exit_price": expx, "direction": "Short", "order_type": "MIS-Daily",
                             "net_pnl_rs": round(cap * MARGIN / 100.0), "hold": int(exi - gi), "exit_reason": reason})
            day += max(1, exi - gi)
        else:
            g = m1.get(dstr[gi + 1])
            if g is None or len(g["o"]) == 0: day += max(1, exi - gi); continue
            entry = float(g["o"][0]); tgt = entry * (1 + pct / 100) if long else entry * (1 - pct / 100)
            expx = exhm = exdate = None; reason = "time-exit"
            for x in range(gi + 1, min(gi + 1 + w, n)):
                gx = m1.get(dstr[x])
                if gx is None: continue
                hit = np.where(gx["h"] >= tgt)[0] if long else np.where(gx["l"] <= tgt)[0]
                if len(hit): expx = tgt; exhm = gx["hm"][hit[0]]; exdate = dstr[x]; reason = "target-hit"; break
            if expx is None:
                lastd = m1.get(dstr[exi]) or g
                if product == "MIS": expx, exhm = B.px_at(lastd, "15:20")
                else: expx, exhm = float(lastd["c"][-1]), "15:30"
                exdate = dstr[exi]
            legs.append(B.mk(sym, pid, 1, 1, dstr[gi], dstr[gi + 1], "09:15", entry, exdate, exhm, expx, long, product, product, reason, exi - gi))
            day += max(1, exi - gi)
    card = D.card_from_legs(sym, legs, npool)
    if card:
        card["method"] = "rolling"; card["trail"] = tname
    return card


def main():
    t0 = time.time()
    kc = sqlite3.connect(str(ROOT / "db" / "kanida.db"))
    fno = {r[0]: r[1] for r in kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()}; kc.close()
    cards = json.load(open(REP / "worker_cards.json"))
    for c in cards:
        c.setdefault("method", "blend")
    newc = {}
    for s in THREE:
        c = rolling_card(s, int(fno.get(s, 0)))
        if c:
            newc[s] = c
            print(f"  {s:<11} rolling card: {c['total_return']:+.0f}%  ret/DD {c['ret_dd']}  sustained={c['sustained']}")
    cards = [newc.get(c["symbol"], c) for c in cards]
    cards.sort(key=lambda c: (c["sustained"] and c.get("ret_dd", 0) >= 10, c.get("ret_dd", 0)), reverse=True)
    (REP / "worker_cards.json").write_text(json.dumps(cards), encoding="utf-8")
    W.render_html(cards)
    # refresh expanded sellable list -> 38
    exp = pd.read_csv(REP / "worker_readiness_expanded.csv")
    add = pd.DataFrame([{"symbol": s, "method": "rolling"} for s in THREE if s not in set(exp.symbol)])
    exp2 = pd.concat([exp, add], ignore_index=True)
    exp2.to_csv(REP / "worker_readiness_expanded.csv", index=False)
    bym = exp2.method.value_counts().to_dict()
    print(f"\n  DASHBOARD REBUILT: {len(cards)} cards | sellable list now {len(exp2)} ({bym}) | {time.time()-t0:.1f}s")
    print("  written: docs/kanida_workers.html + reports/worker_cards.json + worker_readiness_expanded.csv")


if __name__ == "__main__":
    main()
