"""
Merge the 8 portfolio-method workers into the Worker Performance dashboard, tagged method='portfolio',
alongside the blend workers. Rebuilds worker_cards.json + docs/kanida_workers.html.
Run: python scripts/subagent_dashboard.py
"""
from __future__ import annotations
import os, sys, json, sqlite3, time
os.environ.setdefault("SLIP_CAP", "0.5"); os.environ.setdefault("OMP_NUM_THREADS", "1")
from pathlib import Path
import numpy as np
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
for p in ("scripts", "kanida_engine", "arena"):
    sys.path.insert(0, str(ROOT / p))
import features as FE, routing, backtest_1min as B, intraday_trail as IT
import dashboard_1min as D, worker_cards as W
from subagent_engine import analyze, MIN_OCC
from subagent_portfolio import greedy_portfolio
from subagent_trailed import trailmap
REP = ROOT / "reports"; MARGIN = B.MARGIN
EIGHT = ["ABFRL", "ABREL", "AMBUJACEM", "SOBHA", "ALKEM", "CENTRALBK", "MARUTI", "IRB"]


def get_picked(sym, is_fno):
    res = analyze(sym, is_fno)
    frame = FE.load_frame(sym, lookback_N=5)
    yrs = frame["year"].values; train_mask = yrs <= 2024; ntrain = int(train_mask.sum())
    cands = []
    for c in res["cards"]:
        tr = c["train"]
        if not (c["keep"] and tr and tr["etv"] > 0 and c["pers"] > 0):
            continue
        tdays = set(np.where(c["mask"] & train_mask)[0].tolist())
        if not tdays:
            continue
        cands.append({"pdv": tr["etv"] / c["w"], "tdays": tdays, "w": c["w"], "pref": c["pref"],
                      "pct": c["pct"], "mask": c["mask"], "card": c})
    cands.sort(key=lambda c: -c["pdv"])
    picked, _ = greedy_portfolio(cands, ntrain)
    return frame, picked


def portfolio_card(sym, is_fno):
    (method, params), tname = trailmap(sym)
    frame, picked = get_picked(sym, is_fno)
    m1 = B.load_1min(sym)
    O = frame["_o"].values; H = frame["_h"].values; L = frame["_l"].values; C = frame["_c"].values
    yr = frame["year"].values; n = len(O); dstr = np.array([str(x.date()) for x in frame.index])
    pool = sorted([{"mask": c["mask"], "dir": c["pref"], "pct": c["pct"], "w": c["w"], "rank": c["pdv"]} for c in picked],
                  key=lambda p: -p["rank"])
    te = np.where((yr == 2025) | (yr == 2026))[0]
    legs = []; day = 0; pid = 0
    while day < len(te):
        gi = te[day]; ch = None
        for p in pool:
            if p["mask"][gi]: ch = p; break
        if ch is None: day += 1; continue
        d = ch["dir"]; pct = ch["pct"]; w = ch["w"]; product, mode = routing.route(d, w, is_fno); long = (d == "up")
        rr = routing.sim_roc(O, H, L, C, gi, d, pct, w, product, mode)
        if rr is None: day += 1; continue
        exi = rr[2]; pid += 1
        if mode == "daily":
            leg = 0
            for x in range(gi + 1, min(gi + 1 + w, n)):
                dd = m1.get(dstr[x])
                if dd is None or len(dd["o"]) < 5: continue
                res = IT.sim_day(dd, method, params)
                if res is None: continue
                cap, expx, exhm, reason = res; leg += 1; entry = float(dd["o"][0])
                legs.append({"symbol": sym, "position_id": pid, "leg_no": leg, "total_legs": w,
                             "signal_date": dstr[gi], "entry_date": dstr[x], "entry_time": "09:15",
                             "entry_price": round(entry, 2), "exit_date": dstr[x], "exit_time": exhm,
                             "exit_price": expx, "direction": "Short", "order_type": "MIS-Daily",
                             "net_pnl_rs": round(cap * MARGIN / 100.0), "hold": int(exi - gi), "exit_reason": reason})
            day += max(1, exi - gi)
        else:
            eday = dstr[gi + 1]; dd = m1.get(eday)
            if dd is None or len(dd["o"]) == 0: day += max(1, exi - gi); continue
            entry = float(dd["o"][0]); tgt = entry * (1 + pct / 100) if long else entry * (1 - pct / 100)
            expx = exhm = exdate = None; reason = "time-exit"
            for x in range(gi + 1, min(gi + 1 + w, n)):
                dx = m1.get(dstr[x])
                if dx is None: continue
                hit = np.where(dx["h"] >= tgt)[0] if long else np.where(dx["l"] <= tgt)[0]
                if len(hit): expx = tgt; exhm = dx["hm"][hit[0]]; exdate = dstr[x]; reason = "target-hit"; break
            if expx is None:
                lastd = m1.get(dstr[exi]) or dd
                if product == "MIS": expx, exhm = B.px_at(lastd, "15:20")
                else: expx, exhm = float(lastd["c"][-1]), "15:30"
                exdate = dstr[exi]
            legs.append(B.mk(sym, pid, 1, 1, dstr[gi], eday, "09:15", entry, exdate, exhm, expx, long, product, product, reason, exi - gi))
            day += max(1, exi - gi)
    card = D.card_from_legs(sym, legs, len(picked))
    if card:
        card["method"] = "portfolio"; card["trail"] = tname; card["n_subagents"] = len(picked)
    return card


def main():
    t0 = time.time()
    kc = sqlite3.connect(str(ROOT / "db" / "kanida.db"))
    fno = {r[0]: r[1] for r in kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()}; kc.close()
    cards = json.load(open(REP / "worker_cards.json"))
    for c in cards:
        c.setdefault("method", "blend")
    newc = {}
    for s in EIGHT:
        c = portfolio_card(s, int(fno.get(s, 0)))
        if c:
            newc[s] = c
            print(f"  {s:<11} portfolio card: {c['total_return']:+.0f}%  ret/DD {c['ret_dd']}  ({c['n_subagents']} subs)")
    cards = [newc.get(c["symbol"], c) for c in cards]
    cards.sort(key=lambda c: (c["sustained"] and c.get("ret_dd", 0) >= 10, c.get("ret_dd", 0)), reverse=True)
    (REP / "worker_cards.json").write_text(json.dumps(cards), encoding="utf-8")
    W.render_html(cards)
    sell = sum(1 for c in cards if c.get("ret_dd", 0) >= 10 and c["sustained"])
    port_sell = sum(1 for c in cards if c.get("method") == "portfolio" and c.get("ret_dd", 0) >= 10 and c["sustained"])
    print(f"\n  DASHBOARD REBUILT: {len(cards)} cards | sellable (ret/DD>=10 & sustained): {sell}  "
          f"(of which portfolio-method: {port_sell}) | {time.time()-t0:.1f}s")
    print("  written: docs/kanida_workers.html + reports/worker_cards.json")


if __name__ == "__main__":
    main()
