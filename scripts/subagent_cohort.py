"""
Cohort test: coverage-aware portfolio + trail  vs  dashboard blend + trail, across the 27 premium workers.
Answers one question honestly: does smart sub-agent selection EVER beat the blend on the real trailed book?
Run: python scripts/subagent_cohort.py
"""
from __future__ import annotations
import os, sys, json, time
os.environ.setdefault("SLIP_CAP", "0.5")
from pathlib import Path
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
for p in ("scripts", "kanida_engine", "arena"):
    sys.path.insert(0, str(ROOT / p))


def premium_syms(universe="premium"):
    cards = {c["symbol"]: c for c in json.load(open(ROOT / "reports" / "worker_cards.json"))}
    if universe == "all":
        return sorted(cards.keys())
    return sorted([s for s, c in cards.items() if c.get("ret_dd", 0) >= 10 and c["sustained"]])


def run_one(sym):
    from subagent_portfolio import evaluate
    try:
        r = evaluate(sym)
    except Exception as e:
        return {"sym": sym, "err": str(e)[:80]}
    if r is None:
        return {"sym": sym, "err": "no data"}
    def rd(m):
        return m["total"] / -m["acct_mdd"] if m["acct_mdd"] < 0 else 999.0
    def sellable(m):
        return rd(m) >= 10 and m["r2025"] > 0 and m["r2026"] > 0
    ts = r["train_sig"]
    use_port = ts["port_rd"] > ts["blend_rd"]        # LEAK-FREE decision: portfolio wins on <=2024 risk-adjusted
    return {"sym": sym, "n_sub": r["n_picked"], "use_port": bool(use_port),
            "dash_ret": r["dash"]["total"], "dash_dd": r["dash"]["acct_mdd"], "dash_rd": rd(r["dash"]),
            "dash_sell": sellable(r["dash"]),
            "port_ret": r["port"]["total"], "port_dd": r["port"]["acct_mdd"], "port_rd": rd(r["port"]),
            "port_sell": sellable(r["port"])}


def main():
    import multiprocessing as mp, numpy as np
    universe = sys.argv[1] if len(sys.argv) > 1 else "premium"
    syms = premium_syms(universe)
    t0 = time.time()
    with mp.get_context("spawn").Pool(8) as pool:
        rows = pool.map(run_one, syms)
    dt = time.time() - t0
    ok = [r for r in rows if "err" not in r and abs(r.get("dash_ret", 0)) > 1e-9]
    errs = [r for r in rows if "err" in r]
    for r in ok:
        r["dret"] = r["port_ret"] - r["dash_ret"]; r["drd"] = r["port_rd"] - r["dash_rd"]
    ok.sort(key=lambda r: -r["dret"])
    n = len(ok)
    win_ret = sum(r["dret"] > 0 for r in ok); win_rd = sum(r["drd"] > 0 for r in ok)
    both = sum(1 for r in ok if r["dret"] > 0 and r["drd"] > 0)
    print(f"\n===== COVERAGE PORTFOLIO vs DASHBOARD BLEND — universe='{universe}' ({n} workers, trailed sealed 2025-26) =====")
    print(f"  portfolio beats blend on RETURN: {win_ret}/{n} ({win_ret/n*100:.0f}%)   "
          f"on RET/DD: {win_rd}/{n} ({win_rd/n*100:.0f}%)   on BOTH: {both}/{n} ({both/n*100:.0f}%)")
    print(f"  median return delta (port - dash): {np.median([r['dret'] for r in ok]):+.0f}%   "
          f"mean: {np.mean([r['dret'] for r in ok]):+.0f}%")
    # breakdown by worker quality
    prem = set(premium_syms("premium"))
    for label, sub in [("PREMIUM (ret/DD>=10) workers", [r for r in ok if r["sym"] in prem]),
                       ("NON-premium workers", [r for r in ok if r["sym"] not in prem])]:
        if sub:
            b = sum(1 for r in sub if r["dret"] > 0 and r["drd"] > 0)
            print(f"    {label:<34}: beats on BOTH {b}/{len(sub)} ({b/len(sub)*100:.0f}%)  median Δret {np.median([r['dret'] for r in sub]):+.0f}%")
    # ---- THE SELLABLE COUNT: blend vs portfolio (both leak-free out-of-sample) ----
    sell_blend = [r for r in ok if r["dash_sell"]]
    sell_port = [r for r in ok if r["port_sell"]]
    new_sell = [r for r in ok if r["port_sell"] and not r["dash_sell"]]     # rescued by portfolio
    lost_sell = [r for r in ok if r["dash_sell"] and not r["port_sell"]]    # broken by portfolio
    kept = [r for r in ok if r["dash_sell"] and r["port_sell"]]
    hybrid = len(sell_blend) + len(new_sell)                                # keep blend where sellable, add rescues
    print("\n  ============ SELLABLE COUNT (ret/DD>=10 AND positive both 2025 & 2026) ============")
    print(f"    Current  (everyone on BLEND)        : {len(sell_blend)} sellable")
    print(f"    If everyone SWITCHED to PORTFOLIO    : {len(sell_port)} sellable")
    print(f"      -> portfolio RESCUES (new sellable): {len(new_sell)}  {[r['sym'] for r in new_sell][:20]}")
    print(f"      -> portfolio BREAKS (lost sellable): {len(lost_sell)}  {[r['sym'] for r in lost_sell][:20]}")
    print(f"    Best-of-both per worker (ORACLE upper bound): {hybrid} sellable  (+{hybrid-len(sell_blend)} vs today)")

    # ---- THE ADVANCE-DECISION (leak-free) RULE: switch to portfolio only if it wins on <=2024 ----
    def chosen_sell(r):
        return r["port_sell"] if r["use_port"] else r["dash_sell"]
    hyb_sell = [r for r in ok if chosen_sell(r)]
    captured = [r for r in new_sell if r["use_port"]]        # true rescues the rule correctly switched
    missed = [r for r in new_sell if not r["use_port"]]      # rescues the rule left on blend (missed)
    broke = [r for r in ok if r["use_port"] and r["dash_sell"] and not r["port_sell"]]  # rule wrongly switched a good one
    print("\n  ============ ADVANCE-DECISION RULE (decide on <=2024 only, then apply to sealed) ============")
    print(f"    rule = switch to portfolio when its <=2024 return/DD beats the blend's")
    print(f"    workers the rule switches to portfolio: {sum(r['use_port'] for r in ok)}/{n}")
    print(f"    SELLABLE under the rule: {len(hyb_sell)}   (today {len(sell_blend)}, net {len(hyb_sell)-len(sell_blend):+d})")
    print(f"      of the {len(new_sell)} true rescues, rule CAPTURED: {len(captured)}  {[r['sym'] for r in captured]}")
    print(f"      of the {len(new_sell)} true rescues, rule MISSED  : {len(missed)}  {[r['sym'] for r in missed]}")
    print(f"      good workers the rule BROKE (wrongly switched): {len(broke)}  {[r['sym'] for r in broke]}")
    print(f"\n  evaluated {n} | errors/empty {len(rows)-n} | [{dt:.1f}s]")


if __name__ == "__main__":
    main()
