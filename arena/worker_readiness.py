"""
WORKER READINESS GAUNTLET — the single source of truth for "is this worker GTM-ready & sellable?".
No "graduate to paper-trade" step: a worker is READY only after it passes ALL rigorous, leak-free
stress tests up front (like any product shipped to market). Policy = M10b a-priori selection
(keep a segment only if its <=2024 ETV was positive; trade kept segments at full breadth, no stops).

Per worker (one stock) we compute leak-free 2025 & 2026 books and run the gauntlet:
  G1 Profitable      net profit on sealed 2026 > 0
  G2 Enough sample   >= MIN_TRADES trades in 2026 (not a fluke)
  G3 Risk-adjusted   2026 ret/DD >= RETDD_BAR (reward at least covers risk)
  G4 Consistent      profitable in BOTH 2025 AND 2026 (survives a regime change)
  G5 Cost-stress     still profitable on 2026 at 2x costs (slippage/impact buffer)
  G6 Tail-bounded    worst single 2026 trade >= WORST_CAP ROC (no catastrophic blow-up)
Tiers: READY (all 6) · NEAR (4-5) · NOT READY (<=3). Writes reports/worker_readiness.csv.
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
OUT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\reports")
CAP = 100_000.0
TGT = {"up_1pct_1d": ("up", 1, 1), "up_2pct_2d": ("up", 2, 2), "up_5pct_5d": ("up", 5, 5),
       "dn_1pct_1d": ("dn", 1, 1), "dn_2pct_2d": ("dn", 2, 2), "dn_5pct_5d": ("dn", 5, 5)}
LEV = {"MIS": 5.0, "CNC": 1.0, "NRML": 5.0}; COST = {"MIS": 0.08, "CNC": 0.15, "NRML": 0.05}
# ---- gauntlet thresholds (a-priori; strict on purpose) ----
import os
SELECT_ETV_MIN = float(os.environ.get("SELECT_ETV_MIN", "0.0"))            # a-priori keep bar (net %/trade)
MIN_TRADES = 8; RETDD_BAR = 1.0; WORST_CAP = -40.0   # -40% ROC = 8% adverse x5
ACCT_DD_CAP = -35.0                                  # G7: 2-yr account drawdown must be better than this ("safest")
TABLE = os.environ.get("UNIFIED_TABLE", "unified_patterns")               # which pattern table to grade
OUTP = os.environ.get("READINESS_OUT", str(Path(OUT) / "worker_readiness.csv"))
# ---- TRAILED grading (the product single-source-of-truth) ----
# The default/product run grades the REAL trailed book (sellable_tradelogs_trailed.csv) so readiness
# always matches worker_cards.json. Alternative research tables (event/seed) have no trailed log, so
# they fall back to the untrailed pattern grader below. Override with READINESS_MODE=trailed|untrailed.
TRAILED_LOG = OUT / "sellable_tradelogs_trailed.csv"
MARGIN = 150_000.0        # internal scaling from backtest_1min (POS_FRAC*CAP); roc = net_pnl_rs/MARGIN*100 = capital-return %
COST2X = 0.80             # one extra round-trip friction (0.30% cost + 0.50% slippage, capital) for the 2x-cost gate


def _use_trailed() -> bool:
    mode = os.environ.get("READINESS_MODE", "auto")
    if mode == "untrailed":
        return False
    if mode == "trailed":
        return True
    return TABLE == "unified_patterns" and TRAILED_LOG.exists()   # auto: product baseline + trailed book present


def trailed_gauntlet() -> "pd.DataFrame":
    """Grade the SAME 6 gates against the real trailed book — one row per worker, matching worker_cards.json."""
    d = pd.read_csv(TRAILED_LOG)
    d["yr"] = d["exit_date"].astype(str).str[:4]
    d["roc"] = d["net_pnl_rs"] / MARGIN * 100.0                    # exact per-leg card return (invested-cash %)
    d["ord"] = d["entry_date"].astype(str) + " " + d["entry_time"].astype(str)
    rows = []
    for sym, g in d.groupby("symbol"):
        g = g.sort_values("ord")
        r25 = g[g.yr == "2025"]; r26 = g[g.yr == "2026"]
        p25 = float(r25.roc.sum()); p26 = float(r26.roc.sum())
        cr = r26.roc.cumsum().values
        dd26 = float((cr - np.maximum.accumulate(cr)).min()) if len(cr) else 0.0
        retdd = (p26 / -dd26) if dd26 < 0 else (float("inf") if p26 > 0 else 0.0)
        n26 = int(r26.position_id.nunique())
        p26_2x = p26 - COST2X * len(r26)                           # charge one more round-trip per leg
        worst = float(r26.roc.min()) if len(r26) else 0.0         # worst single trailed day (capital %)
        allroc = g.roc.values                                     # full 2-yr book (chronological) for account MDD
        cra = np.cumsum(allroc); eqf = CAP * (1 + cra / 100.0); pk = np.maximum.accumulate(eqf)
        acct_mdd = float(((eqf - pk) / pk * 100).min()) if len(cra) else 0.0
        gt = {"G1_profitable": p26 > 0, "G2_sample": n26 >= MIN_TRADES, "G3_risk_adj": retdd >= RETDD_BAR,
              "G4_consistent": (p25 > 0 and p26 > 0), "G5_cost_stress": p26_2x > 0, "G6_tail_ok": worst >= WORST_CAP,
              "G7_acct_dd_ok": acct_mdd >= ACCT_DD_CAP}
        passed = sum(gt.values())
        tier = "READY" if passed == 7 else ("NEAR" if passed >= 5 else "NOT READY")
        rows.append({"symbol": sym,
                     "profit_2026": round(p26 * CAP / 100), "dd_2026": round(dd26 * CAP / 100),
                     "retdd_2026": round(retdd, 2), "trades_2026": n26, "acct_mdd_2yr_pct": round(acct_mdd, 1),
                     "profit_2025": round(p25 * CAP / 100), "worst_trade_2026_pct": round(worst, 1),
                     "profit_2026_2xcost": round(p26_2x * CAP / 100),
                     "ret_2025_pct": round(p25, 1), "ret_2026_pct": round(p26, 1), "total_2yr_pct": round(p25 + p26, 1),
                     "gates_passed": passed, "tier": tier, **{k: int(v) for k, v in gt.items()}})
    return pd.DataFrame(rows).sort_values(["gates_passed", "profit_2026"], ascending=False)


def route(d, w, is_fno):
    if w == 1: return "MIS", ("LONG" if d == "up" else "SHORT")
    if d == "up": return ("NRML" if is_fno else "CNC"), "LONG"
    return "NRML", "SHORT"


def sim(fnp, i, d, pct, w, order, n):
    """Return (exit_idx, gross_move_pct, order) — cost/leverage applied later so we can cost-stress."""
    O, H, L, C = fnp
    if i + 1 >= n: return None
    entry = O[i + 1]
    if not np.isfinite(entry) or entry <= 0: return None
    long = (d == "up"); tgt = entry * (1 + pct / 100) if long else entry * (1 - pct / 100)
    ex = None; exi = min(i + w, n - 1)
    for x in range(i + 1, min(i + 1 + w, n)):
        hi, lo = H[x], L[x]
        if long and hi >= tgt: ex, exi = tgt, x; break
        if (not long) and lo <= tgt: ex, exi = tgt, x; break
        exi = x
    if ex is None: ex = C[exi]
    gross = (ex / entry - 1) * 100 if long else (1 - ex / entry) * 100
    return exi, gross, order


def pnl_of(tr, cost_mult=1.0):
    """[(exit_idx, gross, order)] -> list of (exit_idx, pnl_rs) at the given cost multiplier."""
    return [(xi, CAP * ((g - COST[o] * cost_mult) * LEV[o]) / 100) for xi, g, o in tr]


def stats(trades):
    if not trades: return 0.0, 0.0, 0, -1e9
    ser = pd.Series([p for _, p in sorted(trades)])
    prof = float(ser.sum()); dd = float((ser.cumsum() - ser.cumsum().cummax()).min())
    worst = float(min(p for _, p in trades)) / CAP * 100   # worst single trade in ROC %
    return prof, dd, len(trades), worst


def run_stock(symbol, con, is_fno):
    rows = con.execute(f"SELECT target,rule_json,lift_tr FROM {TABLE} WHERE symbol=? AND promoted=1",
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
    yr = frame["year"].values; nfr = len(frame)
    fnp = (frame["_o"].values, frame["_h"].values, frame["_l"].values, frame["_c"].values)
    for s in segs:
        m = np.zeros(nfr, bool)
        for cds in segs[s]["conds"]: m |= apply_rule(frame, cds).values
        segs[s]["fire"] = m
    # a-priori keep set from <=2024 ETV
    pre = np.where(yr <= 2024)[0]; keep = set()
    for s in segs:
        m = segs[s]; rocs = [r[1] for gi in pre if m["fire"][gi] for r in [sim(fnp, gi, m["d"], m["pct"], m["w"], m["order"], nfr)] if r]
        # ETV here uses net (1x cost) to decide keep
        net = [(g - COST[m["order"]]) * LEV[m["order"]] for g in rocs]
        if net and float(np.mean(net)) > SELECT_ETV_MIN: keep.add(s)

    def book(eval_idx):
        tr = []; i = 0
        while i < len(eval_idx) - 1:
            gi = eval_idx[i]; firing = [s for s in segs if s in keep and segs[s]["fire"][gi]]
            if not firing: i += 1; continue
            seg = max(firing, key=lambda s: segs[s]["lift"]); m = segs[seg]
            r = sim(fnp, gi, m["d"], m["pct"], m["w"], m["order"], nfr)
            if r is None: i += 1; continue
            exi, g, o = r; tr.append((exi, g, o))
            while i < len(eval_idx) and eval_idx[i] <= exi: i += 1
        return tr

    tr26 = book(list(np.where(yr == 2026)[0])); tr25 = book(list(np.where(yr == 2025)[0]))
    p26, dd26, n26, worst26 = stats(pnl_of(tr26))
    p25, _, n25, _ = stats(pnl_of(tr25))
    p26_2x, _, _, _ = stats(pnl_of(tr26, cost_mult=2.0))
    retdd = (p26 / -dd26) if dd26 < 0 else (float("inf") if p26 > 0 else 0.0)
    g = {
        "G1_profitable":  p26 > 0,
        "G2_sample":      n26 >= MIN_TRADES,
        "G3_risk_adj":    retdd >= RETDD_BAR,
        "G4_consistent":  (p25 > 0 and p26 > 0),
        "G5_cost_stress": p26_2x > 0,
        "G6_tail_ok":     worst26 >= WORST_CAP,
    }
    passed = sum(g.values())
    tier = "READY" if passed == 6 else ("NEAR" if passed >= 4 else "NOT READY")
    return {"symbol": symbol, "kept_segments": len(keep), "total_segments": len(segs),
            "profit_2026": round(p26), "dd_2026": round(dd26), "retdd_2026": round(retdd, 2),
            "trades_2026": n26, "profit_2025": round(p25), "worst_trade_2026_pct": round(worst26, 1),
            "profit_2026_2xcost": round(p26_2x), "gates_passed": passed, "tier": tier,
            **{k: int(v) for k, v in g.items()}}


def main():
    if _use_trailed():                                            # PRODUCT default: grade the real trailed book
        df = trailed_gauntlet()
        OUT.mkdir(exist_ok=True); df.to_csv(OUTP, index=False)
        ready = df[df.tier == "READY"]; near = df[df.tier == "NEAR"]
        kec = df[df.symbol == "KEC"]
        print("\n========= WORKER READINESS GAUNTLET (TRAILED book · invested-cash · single source of truth) =========")
        if not kec.empty:
            k = kec.iloc[0]
            print(f"  sanity KEC: 2025={k['ret_2025_pct']:+.0f}%  2026={k['ret_2026_pct']:+.0f}%  total={k['total_2yr_pct']:+.0f}%  (cards +283/+264/+547)")
        print(f"  workers evaluated : {len(df)}")
        print(f"  READY (all 7)     : {len(ready)}   NEAR (5-6): {len(near)}   NOT READY: {len(df)-len(ready)-len(near)}")
        print("  gate pass counts  : " + "  ".join(f"{g}={int(df[g].sum())}" for g in
              ["G1_profitable", "G2_sample", "G3_risk_adj", "G4_consistent", "G5_cost_stress", "G6_tail_ok", "G7_acct_dd_ok"]))
        print("\n  Top 15 READY (2-yr return on invested cash):")
        print(ready.head(15)[["symbol", "total_2yr_pct", "retdd_2026", "trades_2026", "worst_trade_2026_pct"]].to_string(index=False))
        print(f"\nwritten: {OUTP}  (TRAILED — matches worker_cards.json)")
        return
    con = sqlite3.connect(SNR); kc = sqlite3.connect(KDB)
    fno = {r[0]: r[1] for r in kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()}; kc.close()
    syms = [r[0] for r in con.execute(f"SELECT DISTINCT symbol FROM {TABLE}").fetchall()]
    rows = []
    for n, s in enumerate(syms, 1):
        try:
            r = run_stock(s, con, int(fno.get(s, 0)))
            if r: rows.append(r)
        except Exception: pass
        if n % 100 == 0: print(f"  ...{n}/{len(syms)}", flush=True)
    con.close()
    df = pd.DataFrame(rows).sort_values(["gates_passed", "profit_2026"], ascending=False)
    OUT.mkdir(exist_ok=True); df.to_csv(OUTP, index=False)
    ready = df[df.tier == "READY"]; near = df[df.tier == "NEAR"]
    print("\n================ WORKER READINESS GAUNTLET (single source of truth) ================")
    print(f"  workers evaluated : {len(df)}")
    print(f"  READY (all 6)     : {len(ready)}   NEAR (4-5): {len(near)}   NOT READY: {len(df)-len(ready)-len(near)}")
    print(f"  gate pass counts  : " + "  ".join(f"{g}={int(df[g].sum())}" for g in
          ["G1_profitable","G2_sample","G3_risk_adj","G4_consistent","G5_cost_stress","G6_tail_ok"]))
    print(f"\n  READY cohort: profit(2026) Rs{ready.profit_2026.sum():,.0f}  "
          f"median ret/DD {ready.retdd_2026.median():.2f}  median trades {int(ready.trades_2026.median())}")
    print("\n  Top 15 READY workers:")
    print(ready.head(15)[["symbol","profit_2026","retdd_2026","trades_2026","profit_2025","kept_segments"]].to_string(index=False))
    print("\nwritten: reports/worker_readiness.csv")


if __name__ == "__main__":
    main()
