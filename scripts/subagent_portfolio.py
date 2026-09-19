"""
COVERAGE-AWARE PORTFOLIO optimizer (leak-free) — pick a small, COMPLEMENTARY set of sub-agents that
covers the most profitable UNIQUE trading days, not the highest-earning clones. Then apply the trail
and compare to the dashboard +300%.

Greedy objective (all on <=2024): each pattern's per-day value = ETV / hold  (expected return per day of
capital tied up -> rewards short-hold, high-edge patterns = the hold-packing lever). Repeatedly add the
pattern that adds the most NEW value across days it covers (max(0, its per-day value - best already there)).
Redundant clones add ~0 (days already covered at >= value); complementary patterns add their full coverage.
Freshness filter: candidate must still be positive in 2023-24 (no stale coverage).

Run: python scripts/subagent_portfolio.py FSL
"""
from __future__ import annotations
import os, sys, time, sqlite3
os.environ.setdefault("SLIP_CAP", "0.5")
from pathlib import Path
import numpy as np
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
for p in ("scripts", "kanida_engine", "arena"):
    sys.path.insert(0, str(ROOT / p))
from subagent_engine import analyze, MIN_OCC            # noqa: E402
from subagent_trailed import trailed_walk, metrics, dashboard_ref, trailmap  # noqa: E402
import features as FE, backtest_1min as B               # noqa: E402

CAP_PATTERNS = 15          # keep the auditable set small
MIN_NEW_DAYS = 4           # stop when a pattern adds < this many new covered days


def greedy_portfolio(cands, train_days):
    """cands: list of dicts with 'pdv' (per-day value) and 'tdays' (set of <=2024 firing-day idx)."""
    best = {}; picked = []
    remaining = list(cands)
    while remaining and len(picked) < CAP_PATTERNS:
        scored = []
        for c in remaining:
            new_days = 0; marg = 0.0
            for d in c["tdays"]:
                gain = c["pdv"] - best.get(d, 0.0)
                if gain > 0:
                    marg += gain
                    if d not in best:
                        new_days += 1
            scored.append((marg, new_days, c))
        scored.sort(key=lambda x: -x[0])
        marg, new_days, c = scored[0]
        if marg <= 0 or new_days < MIN_NEW_DAYS:
            break
        for d in c["tdays"]:
            best[d] = max(best.get(d, 0.0), c["pdv"])
        c["marg"], c["new_days"] = marg, new_days
        picked.append(c); remaining.remove(c)
    return picked, len(best)


def _untrailed_walk(pool, cache, idx):
    """Non-overlapping campaign walk on a DAILY (untrailed) net series over a given index set. Returns (total, dd, n)."""
    pats = sorted(pool, key=lambda p: -p["rank"]); rocs = []; pos = 0
    while pos < len(idx):
        gi = idx[pos]; ch = None
        for p in pats:
            if p["mask"][gi]: ch = p; break
        if ch is None: pos += 1; continue
        net = cache[(ch["dir"], ch["pct"], ch["w"])][gi]
        if np.isfinite(net):
            rocs.append(net); pos += max(1, int(ch["w"]))
        else:
            pos += 1
    if not rocs:
        return 0.0, 0.0, 0
    a = np.array(rocs); cr = np.cumsum(a)
    return float(a.sum()), float((cr - np.maximum.accumulate(cr)).min()), len(a)


def evaluate(sym):
    """Per-worker head-to-head: dashboard blend vs coverage portfolio, both trailed. Returns a plain dict."""
    kc = sqlite3.connect(str(ROOT / "db" / "kanida.db"))
    is_fno = int(dict(kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()).get(sym, 0)); kc.close()
    (method, params), tname = trailmap(sym)
    res = analyze(sym, is_fno)
    if res is None:
        return None
    frame = FE.load_frame(sym, lookback_N=5); m1 = B.load_1min(sym)
    yrs = frame["year"].values; train_mask = yrs <= 2024; ntrain_days = int(train_mask.sum())
    cache = res["cache"]; train_idx = np.where(train_mask)[0]
    cands = []
    for c in res["cards"]:
        tr = c["train"]
        if not (c["keep"] and tr and tr["etv"] > 0 and c["pers"] > 0):
            continue
        tdays = set(np.where(c["mask"] & train_mask)[0].tolist())
        if len(tdays) == 0:
            continue
        cands.append({"pdv": tr["etv"] / c["w"], "tdays": tdays, "card": c,
                      "etv": tr["etv"], "w": c["w"], "pref": c["pref"], "target": c["target"],
                      "pct": c["pct"], "mask": c["mask"], "cov": len(tdays)})
    cands.sort(key=lambda c: -c["pdv"])
    picked, days_covered = greedy_portfolio(cands, ntrain_days)
    pool = [{"mask": c["mask"], "dir": c["pref"], "pct": c["pct"], "w": c["w"], "rank": c["pdv"]} for c in picked]
    port_legs = trailed_walk(frame, m1, is_fno, pool, method, params)
    port = metrics(port_legs)
    from subagent_trailed import gauntlet as _gauntlet
    port_gaunt = _gauntlet(port_legs)
    dash = dashboard_ref(sym, is_fno)
    tdir = lambda c: "up" if c["target"][0] == "u" else "dn"
    blend_pool = [{"mask": c["mask"], "dir": tdir(c), "pct": c["pct"], "w": c["w"], "rank": c["rank_ecr"]}
                  for c in res["cards"] if c["keep"]]
    blend = metrics(trailed_walk(frame, m1, is_fno, blend_pool, method, params))
    # ---- LEAK-FREE decision signal: untrailed <=2024 book for each method ----
    bt, bdd, bn = _untrailed_walk(blend_pool, cache, train_idx)
    pt, pdd, pn = _untrailed_walk(pool, cache, train_idx)
    train_sig = {"blend_ret": bt, "blend_rd": (bt / -bdd if bdd < 0 else 999.0),
                 "port_ret": pt, "port_rd": (pt / -pdd if pdd < 0 else 999.0)}
    return {"sym": sym, "n_cands": len(cands), "n_picked": len(picked),
            "days_covered": days_covered, "ntrain_days": ntrain_days,
            "dash": dash, "blend": blend, "port": port, "picked": picked, "train_sig": train_sig,
            "port_gaunt": port_gaunt}


def main():
    sym = sys.argv[1] if len(sys.argv) > 1 else "FSL"
    t0 = time.time()
    r = evaluate(sym)
    dash, blend, port, picked, days_covered, ntrain_days = (
        r["dash"], r["blend"], r["port"], r["picked"], r["days_covered"], r["ntrain_days"])
    dt = time.time() - t0
    tname = trailmap(sym)[1]

    print(f"\n===== {sym}: COVERAGE-AWARE PORTFOLIO + trail  vs  DASHBOARD +300%  (sealed 2025-26, trail='{tname}') =====")
    print(f"  candidates (fresh, +ETV, N>={MIN_OCC}): {r['n_cands']}  ->  greedy portfolio: {len(picked)} sub-agents")
    yrs_span = ntrain_days / 252.0
    print(f"  unique <=2024 days covered by portfolio: {days_covered}/{ntrain_days} ({days_covered/ntrain_days*100:.0f}%)  ~{days_covered/max(yrs_span,1):.0f} days/yr")
    print()
    print(f"  {'book':<46}{'return':>9}{'acctMDD':>9}{'ret/DD':>8}{'legs':>6}")

    def line(name, m):
        rd = m["total"] / -m["acct_mdd"] if m["acct_mdd"] < 0 else float("inf")
        print(f"  {name:<46}{m['total']:>+8.0f}%{m['acct_mdd']:>+8.1f}%{rd:>8.1f}{m['n']:>6}")
    line("DASHBOARD (segment blend + trail)", dash)
    line("ALL patterns + trail (blend everything)", blend)
    line(f"COVERAGE PORTFOLIO + trail ({len(picked)} sub-agents)", port)

    print(f"\n  --- the {len(picked)} selected sub-agents (complementary, ranked by marginal coverage) ---")
    for i, c in enumerate(picked, 1):
        se = c["card"]["sealed"]
        se_s = f"{se['ecr']:+.0f}%" if se else "n/a"
        print(f"   {i:>2}. {c['target']:11} {('LONG' if c['pref']=='up' else 'SHORT'):5} "
              f"| ETV={c['etv']:+.2f}% /day={c['pdv']:+.2f}% | covers {c['cov']:>4} <=2024 days (+{c.get('new_days',0)} NEW) "
              f"| sealed {se_s}")
    print(f"\n  [{dt:.2f}s]  dashboard sanity: {dash['total']:+.0f}%")


if __name__ == "__main__":
    main()
