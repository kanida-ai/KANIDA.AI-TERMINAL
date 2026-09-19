"""
M14 — export the SELLABLE agents defined by NET EXPECTANCY (supersedes the binary-gauntlet export).
Sellable = sustained positive net expectancy across 2025 & 2026 AND worst-year expectancy >= BAR (%/trade).
Each agent carries the expectancy-selected patterns (<=2024 expectancy>0) and the per-year expectancy track
record a buyer inspects: Pwin, AvgWin, Ploss, AvgLoss, NetExpectancy, occurrences.
Outputs: agents/<SYMBOL>.json (rewritten) + reports/sellable_agents_catalog.csv.
Run: PYTHONIOENCODING=utf-8 python arena/agent_export_exp.py
"""
import sys, json, sqlite3, hashlib, glob, os
from datetime import datetime, timezone
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "arena")); sys.path.insert(0, str(ROOT / "scripts")); sys.path.insert(0, str(ROOT / "kanida_engine"))
from mine_phase1 import apply_rule
import features as FE
from expectancy_scorecard import route, trade_net_roc, expectancy, TGT, LEV, COST, SELECT_MAX_YEAR, MIN_OCC_PRE

SNR = str(ROOT / "db" / "KANIDA_SNR.db"); KDB = str(ROOT / "db" / "kanida.db")
AG = ROOT / "agents"; REP = ROOT / "reports"
BAR = 1.0                                    # worst-year net expectancy bar (%/trade) — the "strong core"
SCHEMA = "kanida-agent-2.0 (net-expectancy, sustained 2025-26)"


def build(symbol, con, is_fno, sc):
    rows = [(t, rj) for t, rj in con.execute("SELECT target,rule_json FROM unified_patterns WHERE symbol=?", (symbol,)).fetchall() if t in TGT]
    if not rows: return None
    frame = FE.load_frame(symbol, lookback_N=5)
    if frame.empty: return None
    yr = frame["year"].values
    import routing
    net = {}
    for t in set(t for t, _ in rows):
        d, pct, w = TGT[t]; net[t] = routing.net_roc_series(frame, d, pct, w, is_fno)[0]   # legal F&O-aware routing
    pre_m = yr <= SELECT_MAX_YEAR; segs = {}
    for t, rj in rows:
        conds = [tuple(c) for c in json.loads(rj)]; mask = apply_rule(frame, conds).values
        e = expectancy(net[t][pre_m & mask])
        if e["n"] >= MIN_OCC_PRE and e["netexp"] > 0:
            d, pct, w = TGT[t]; product, mode = routing.route(d, w, is_fno); order = routing.label(product, mode)
            k = f"{order} {'LONG' if d=='up' else 'SHORT'} {w}D"
            s = segs.setdefault(k, {"order": order, "direction": "LONG" if d == "up" else "SHORT", "hold_days": w,
                                    "pct_target": pct, "leverage": LEV[product], "cost_pct": COST[product], "patterns": []})
            s["patterns"].append({"target": t, "conds": [list(c) for c in conds],
                                  "pre2025_net_expectancy_pct": round(e["netexp"], 3), "pre2025_trades": e["n"]})
    if not segs: return None
    agent = {
        "schema": SCHEMA, "symbol": symbol, "is_fno": bool(is_fno),
        "exported_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "sellable_metric": "net expectancy = (Pwin*AvgWin) - (Ploss*AvgLoss), after costs, per trade",
        "worst_year_net_expectancy_pct": float(sc["min_netexp"]),
        "how_it_trades": {"entry": "next session OPEN after a kept pattern fires at close",
                          "exit": "target touch within hold window (daily H/L) else time-exit at close",
                          "selection": "trade only patterns with positive <=2024 net expectancy",
                          "routing": "1D->MIS 5x; multiday long->CNC 1x; multiday short->NRML 5x"},
        "segments": segs,
        "track_record_net_expectancy": {
            str(Y): {"net_expectancy_pct": sc.get(f"netexp_{Y}"), "win_rate": sc.get(f"pwin_{Y}"),
                     "avg_win_pct": sc.get(f"avgwin_{Y}"), "avg_loss_pct": sc.get(f"avgloss_{Y}"),
                     "trades": int(sc.get(f"n_{Y}", 0))} for Y in (2025, 2026)},
        "sustained_positive_both_years": bool(sc["sustained"]),
    }
    agent["agent_id"] = hashlib.sha1(json.dumps(agent["segments"], sort_keys=True).encode()).hexdigest()[:12]
    return agent


def main():
    _a = REP / "expectancy_scorecard.csv"; _b = REP / "expectancy_scorecard_NEW.csv"
    _sc = _b if (_b.exists() and (not _a.exists() or _b.stat().st_mtime > _a.stat().st_mtime)) else _a
    sc = pd.read_csv(_sc)
    core = sc[(sc.sustained == True) & (sc.min_netexp >= BAR)].set_index("symbol")
    con = sqlite3.connect(SNR); kc = sqlite3.connect(KDB)
    fno = {r[0]: r[1] for r in kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()}; kc.close()
    for old in glob.glob(str(AG / "*.json")):     # rewrite the canonical set (binary -> expectancy)
        try: os.remove(old)
        except Exception: pass
    AG.mkdir(exist_ok=True); cat = []
    for sym, row in core.iterrows():
        try:
            a = build(sym, con, int(fno.get(sym, 0)), row)
        except Exception as e:
            print(f"  {sym}: ERR {str(e)[:70]}"); a = None
        if not a: continue
        (AG / f"{sym}.json").write_text(json.dumps(a, indent=2), encoding="utf-8")
        cat.append({"symbol": sym, "agent_id": a["agent_id"], "is_fno": a["is_fno"], "segments": len(a["segments"]),
                    "worst_year_netexp_pct": a["worst_year_net_expectancy_pct"],
                    "netexp_2025": row.get("netexp_2025"), "netexp_2026": row.get("netexp_2026"),
                    "win_2025": row.get("pwin_2025"), "trades_2025": int(row.get("n_2025", 0))})
    con.close()
    df = pd.DataFrame(cat).sort_values("worst_year_netexp_pct", ascending=False)
    df.to_csv(REP / "sellable_agents_catalog.csv", index=False)
    print(f"\n============ M14 · SELLABLE AGENTS (net expectancy >= +{BAR}%/trade, sustained) ============")
    print(f"  exported {len(df)} agents -> agents/*.json | catalogue -> reports/sellable_agents_catalog.csv")
    print(f"  median worst-year net expectancy: +{df.worst_year_netexp_pct.median():.2f}%/trade")
    print("\n  top 12:")
    print(df.head(12).to_string(index=False))


if __name__ == "__main__":
    main()
