"""
M13 (v1) — export the READY workers as sellable, auditable per-stock AGENTS.
For every worker graded READY by the gauntlet (reports/worker_readiness.csv), write a self-contained
agent spec: the exact kept segments + their patterns (so its live signals are fully reproducible), the
routing/leverage/cost rules, the a-priori selection evidence, and the leak-free track record (the 6-test
gauntlet result a buyer inspects). Outputs:
  agents/<SYMBOL>.json          — one serialized, sellable agent each
  reports/ready_agents_catalog.csv — the sellable catalogue (track record per agent)
Non-destructive; reads the current unified_patterns + worker_readiness.csv. Re-runnable.
"""
import sys, json, sqlite3, hashlib
from datetime import datetime, timezone
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\scripts")))
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\kanida_engine")))
from mine_phase1 import apply_rule
import features as FE

SNR = r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db"
KDB = r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db"
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
AG = ROOT / "agents"; REP = ROOT / "reports"
CAP = 100_000.0
TGT = {"up_1pct_1d": ("up", 1, 1), "up_2pct_2d": ("up", 2, 2), "up_5pct_5d": ("up", 5, 5),
       "dn_1pct_1d": ("dn", 1, 1), "dn_2pct_2d": ("dn", 2, 2), "dn_5pct_5d": ("dn", 5, 5)}
LEV = {"MIS": 5.0, "CNC": 1.0, "NRML": 5.0}; COST = {"MIS": 0.08, "CNC": 0.15, "NRML": 0.05}
POLICY_VERSION = "kanida-agent-1.0 (M10b a-priori selection + M11 6-gate gauntlet)"


def route(d, w, is_fno):
    if w == 1: return "MIS", ("LONG" if d == "up" else "SHORT")
    if d == "up": return ("NRML" if is_fno else "CNC"), "LONG"
    return "NRML", "SHORT"


def sim_net(fnp, i, d, pct, w, order, n):
    O, H, L, C = fnp
    if i + 1 >= n: return None
    entry = O[i + 1]
    if not np.isfinite(entry) or entry <= 0: return None
    long = (d == "up"); tgt = entry * (1 + pct / 100) if long else entry * (1 - pct / 100)
    ex = None; exi = min(i + w, n - 1)
    for x in range(i + 1, min(i + 1 + w, n)):
        if long and H[x] >= tgt: ex, exi = tgt, x; break
        if (not long) and L[x] <= tgt: ex, exi = tgt, x; break
        exi = x
    if ex is None: ex = C[exi]
    gross = (ex / entry - 1) * 100 if long else (1 - ex / entry) * 100
    return (gross - COST[order]) * LEV[order]


def build_agent(symbol, con, is_fno, track):
    rows = con.execute("SELECT target,rule_text,rule_json,lift_tr,prec_te FROM unified_patterns "
                       "WHERE symbol=? AND promoted=1", (symbol,)).fetchall()
    if not rows: return None
    segs = {}
    for t, rt, rj, lt, pte in rows:
        if t not in TGT: continue
        d, pct, w = TGT[t]; order, dl = route(d, w, is_fno); k = f"{order} {dl} {w}D"
        s = segs.setdefault(k, {"order": order, "direction": dl, "hold_days": w, "pct_target": pct,
                                "dir": d, "w": w, "patterns": []})
        s["patterns"].append({"target": t, "rule": rt, "conds": json.loads(rj), "lift_tr": lt, "prec_2026": pte})
    frame = FE.load_frame(symbol, lookback_N=5)
    if frame.empty: return None
    yr = frame["year"].values; nfr = len(frame)
    fnp = (frame["_o"].values, frame["_h"].values, frame["_l"].values, frame["_c"].values)
    pre = np.where(yr <= 2024)[0]
    kept = {}
    for k, s in segs.items():
        m = np.zeros(nfr, bool)
        for p in s["patterns"]: m |= apply_rule(frame, p["conds"]).values
        nets = [r for gi in pre if m[gi] for r in [sim_net(fnp, gi, s["dir"], s["pct_target"], s["w"], s["order"], nfr)] if r is not None]
        etv = float(np.mean(nets)) if nets else -1e9
        if etv > 0.0:                                    # a-priori keep rule (leak-free, <=2024)
            kept[k] = {"order": s["order"], "direction": s["direction"], "hold_days": s["hold_days"],
                       "pct_target": s["pct_target"], "leverage": LEV[s["order"]], "cost_pct": COST[s["order"]],
                       "pre2025_etv_pct": round(etv, 3), "pre2025_trades": len(nets),
                       "n_patterns": len(s["patterns"]), "patterns": s["patterns"]}
    if not kept: return None
    agent = {
        "schema": POLICY_VERSION,
        "symbol": symbol, "is_fno": bool(is_fno),
        "exported_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "how_it_trades": {
            "entry": "next session OPEN after a kept pattern fires at close",
            "exit": "target touch within hold window (daily H/L), else time-exit at close",
            "sizing": f"Rs{int(CAP):,} margin per position", "selection": "trade only kept segments (a-priori <=2024 ETV>0)",
            "routing": "1D->MIS 5x; multiday long->CNC 1x; multiday short->NRML 5x (F&O)"},
        "segments": kept,
        "track_record_leakfree": {
            "sealed_2026": {"profit_rs": int(track["profit_2026"]), "ret_over_maxdd": float(track["retdd_2026"]),
                             "trades": int(track["trades_2026"]), "worst_trade_pct": float(track["worst_trade_2026_pct"]),
                             "profit_at_2x_costs_rs": int(track["profit_2026_2xcost"])},
            "prior_year_2025_profit_rs": int(track["profit_2025"]),
            "stress_gates_passed": f"{int(track['gates_passed'])}/6", "tier": track["tier"],
            "gates": {g: bool(track[g]) for g in ["G1_profitable", "G2_sample", "G3_risk_adj",
                                                  "G4_consistent", "G5_cost_stress", "G6_tail_ok"]}},
    }
    agent["agent_id"] = hashlib.sha1(json.dumps(agent["segments"], sort_keys=True).encode()).hexdigest()[:12]
    return agent


def main():
    rd = pd.read_csv(REP / "worker_readiness.csv")
    ready = rd[rd.tier == "READY"].set_index("symbol")
    con = sqlite3.connect(SNR); kc = sqlite3.connect(KDB)
    fno = {r[0]: r[1] for r in kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()}; kc.close()
    AG.mkdir(exist_ok=True); cat = []
    for sym, tr in ready.iterrows():
        try:
            a = build_agent(sym, con, int(fno.get(sym, 0)), tr)
        except Exception as e:
            print(f"  {sym}: ERR {str(e)[:70]}"); a = None
        if not a: continue
        (AG / f"{sym}.json").write_text(json.dumps(a, indent=2), encoding="utf-8")
        cat.append({"symbol": sym, "agent_id": a["agent_id"], "is_fno": a["is_fno"],
                    "segments": len(a["segments"]),
                    "profit_2026_rs": a["track_record_leakfree"]["sealed_2026"]["profit_rs"],
                    "ret_over_maxdd": a["track_record_leakfree"]["sealed_2026"]["ret_over_maxdd"],
                    "trades_2026": a["track_record_leakfree"]["sealed_2026"]["trades"],
                    "profit_2025_rs": a["track_record_leakfree"]["prior_year_2025_profit_rs"],
                    "gates": a["track_record_leakfree"]["stress_gates_passed"]})
    con.close()
    df = pd.DataFrame(cat).sort_values("profit_2026_rs", ascending=False)
    df.to_csv(REP / "ready_agents_catalog.csv", index=False)
    print(f"\n================ M13 · SELLABLE AGENTS EXPORTED ================")
    print(f"  exported {len(df)} READY agents -> agents/*.json  |  catalogue -> reports/ready_agents_catalog.csv")
    print(f"  cohort profit (sealed 2026): Rs{df.profit_2026_rs.sum():,.0f}  | median ret/DD {df.ret_over_maxdd.median():.2f}")
    print("\n  top 10 sellable agents:")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
