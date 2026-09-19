"""
Lock-in + validate: grade every worker's COVERAGE-PORTFOLIO book through the SAME 6-gate readiness
gauntlet as the blend workers. Final sellable set = blend-READY (kept, untouched) + portfolio-only-READY
(new, never overlapping a blend-READY worker). Writes reports/worker_readiness_expanded.csv.
Run: python scripts/subagent_lock.py
"""
from __future__ import annotations
import os, sys, json, time
os.environ.setdefault("SLIP_CAP", "0.5")
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
for p in ("scripts", "kanida_engine", "arena"):
    sys.path.insert(0, str(ROOT / p))
REP = ROOT / "reports"
SIX = ["DIXON", "PETRONET", "MARUTI", "AMBUJACEM", "ITC", "ABFRL"]


def run_lock(sym):
    from subagent_portfolio import evaluate
    try:
        r = evaluate(sym)
    except Exception as e:
        return {"sym": sym, "err": str(e)[:70]}
    if r is None:
        return {"sym": sym, "err": "no data"}
    g = r["port_gaunt"]; p = r["port"]
    rd = p["total"] / -p["acct_mdd"] if p["acct_mdd"] < 0 else 999.0
    return {"sym": sym, "n_sub": r["n_picked"], "port_tier": g["tier"], "port_gates": g["gates"],
            "port_ret": round(p["total"]), "port_2025": round(g["r2025"]), "port_2026": round(g["r2026"]),
            "port_acct_mdd": round(p["acct_mdd"], 1), "port_rd": round(rd, 1),
            "port_retdd26": g["retdd26"], "port_worst": g["worst"]}


def main():
    import multiprocessing as mp
    wr = pd.read_csv(REP / "worker_readiness.csv")
    blend_ready = set(wr[wr.tier == "READY"].symbol)
    syms = sorted(set(c["symbol"] for c in json.load(open(REP / "worker_cards.json"))))
    t0 = time.time()
    with mp.get_context("spawn").Pool(8) as pool:
        rows = pool.map(run_lock, syms)
    dt = time.time() - t0
    ok = [r for r in rows if "err" not in r]
    by = {r["sym"]: r for r in ok}
    port_ready = set(r["sym"] for r in ok if r["port_tier"] == "READY")
    expansion = sorted(port_ready - blend_ready)          # NEW sellable the blend never had
    final = blend_ready | set(expansion)

    print(f"\n================ LOCK-IN + VALIDATE (portfolio book, same 6-gate gauntlet) ================")
    print(f"  blend READY (current, kept untouched) : {len(blend_ready)}")
    print(f"  portfolio-only READY (NEW, expansion) : {len(expansion)}")
    print(f"  FINAL sellable (blend + expansion)    : {len(final)}   (+{len(final)-len(blend_ready)} vs current)")

    print(f"\n  --- validation of the 6 named rescues (portfolio book vs the 6 gates) ---")
    for s in SIX:
        r = by.get(s)
        if not r:
            print(f"    {s:<11} : no data"); continue
        newflag = "NEW sellable" if s in expansion else ("already blend-READY" if s in blend_ready else "still not READY")
        print(f"    {s:<11} gauntlet={r['port_tier']:<9}({r['port_gates']}/6)  2yr {r['port_ret']:+5}%  "
              f"2025 {r['port_2025']:+4}% 2026 {r['port_2026']:+4}%  ret/DD26 {r['port_retdd26']:.1f}  worst {r['port_worst']:.0f}%  -> {newflag}")

    print(f"\n  --- ALL {len(expansion)} portfolio-only-READY workers (the full expansion set) ---")
    exp_rows = sorted([by[s] for s in expansion], key=lambda r: -r["port_ret"])
    for r in exp_rows:
        print(f"    {r['sym']:<11} {r['n_sub']:>2} subs | 2yr {r['port_ret']:+5}% | 2025 {r['port_2025']:+4}% 2026 {r['port_2026']:+4}% | "
              f"acctMDD {r['port_acct_mdd']:.0f}% | ret/DD26 {r['port_retdd26']:.1f} | worst-day {r['port_worst']:.0f}%")

    # write expanded readiness (canonical stays; this is the merged view)
    out = []
    for s in sorted(final):
        r = by.get(s, {})
        method = "blend" if s in blend_ready else "portfolio"
        out.append({"symbol": s, "method": method,
                    "port_tier": r.get("port_tier", ""), "port_gates": r.get("port_gates", ""),
                    "port_2yr_ret": r.get("port_ret", ""), "port_2025": r.get("port_2025", ""),
                    "port_2026": r.get("port_2026", ""), "port_retdd26": r.get("port_retdd26", ""),
                    "port_worst_day": r.get("port_worst", ""), "n_subagents": r.get("n_sub", "")})
    pd.DataFrame(out).to_csv(REP / "worker_readiness_expanded.csv", index=False)
    print(f"\n  written: reports/worker_readiness_expanded.csv ({len(final)} sellable)  [{dt:.1f}s]")


if __name__ == "__main__":
    main()
