"""
SUB-AGENT ENGINE — treat each mined pattern as a per-worker "sub-agent", score its earning power on
BOTH long and short (net-after-cost per trade), then keep only the high-earning sub-agents instead of
blending all of them. Leak-free: rank/select on <=2024 ONLY, measure the lift on sealed 2025-26.

Per pattern (the example card): preferred direction, occurrences, win rate, avg win/loss, net expectancy
(ETV), Expected Cumulative Return (ETV x N = strategy earning power), profit factor, max drawdown,
recent persistence, lifecycle status, and rank.

Run one stock:  python scripts/subagent_engine.py FSL
"""
from __future__ import annotations
import sys, sqlite3, json, time
from pathlib import Path
import numpy as np, pandas as pd

ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
for p in ("scripts", "kanida_engine", "arena"):
    sys.path.insert(0, str(ROOT / p))
from mine_phase1 import apply_rule          # noqa: E402
import features as FE                        # noqa: E402
import routing                               # noqa: E402

SNR = str(ROOT / "db" / "KANIDA_SNR.db"); KDB = str(ROOT / "db" / "kanida.db")
TGT = {"up_1pct_1d": ("up", 1, 1), "up_2pct_2d": ("up", 2, 2), "up_5pct_5d": ("up", 5, 5),
       "dn_1pct_1d": ("dn", 1, 1), "dn_2pct_2d": ("dn", 2, 2), "dn_5pct_5d": ("dn", 5, 5)}
TRAIN_MAX = 2024
MIN_OCC = 15                                  # a-priori keep bar: enough <=2024 occurrences


def dstats(net: np.ndarray) -> dict | None:
    net = net[np.isfinite(net)]
    n = len(net)
    if n == 0:
        return None
    wins = net[net > 0]; losses = net[net <= 0]
    wr = len(wins) / n
    aw = float(wins.mean()) if len(wins) else 0.0
    al = float(-losses.mean()) if len(losses) else 0.0
    etv = float(net.mean())                   # == wr*aw - (1-wr)*al
    ecr = float(net.sum())                    # ETV x N = Expected Cumulative Return
    pf = float(wins.sum() / -losses.sum()) if losses.sum() < 0 else (np.inf if wins.sum() > 0 else 0.0)
    c = np.cumsum(net); mdd = float((c - np.maximum.accumulate(c)).min())
    return {"n": n, "wr": wr * 100, "aw": aw, "al": al, "etv": etv, "ecr": ecr, "pf": pf, "mdd": mdd}


def persistence(net_all: np.ndarray, yrs: np.ndarray) -> float:
    """recent (2023-24) ETV / overall <=2024 ETV — >1 = edge strengthening, <1 = drifting."""
    pre = net_all[yrs <= TRAIN_MAX]; rec = net_all[(yrs >= 2023) & (yrs <= TRAIN_MAX)]
    pre = pre[np.isfinite(pre)]; rec = rec[np.isfinite(rec)]
    if len(pre) == 0 or abs(pre.mean()) < 1e-9 or len(rec) == 0:
        return 1.0
    return float(rec.mean() / pre.mean())


def status(pref_train: dict | None, both_neg: bool) -> str:
    if both_neg or pref_train is None or pref_train["etv"] <= 0:
        return "RETIRE"
    if pref_train["n"] < 10:
        return "TEST"
    if pref_train["n"] >= 20 and pref_train["pf"] >= 1.3:
        return "KEEP"
    return "WATCH"


def analyze(symbol: str, is_fno: int, pool: str = "promoted"):
    con = sqlite3.connect(SNR)
    where = "AND promoted=1" if pool == "promoted" else ""
    rows = con.execute(f"SELECT target,rule_json FROM unified_patterns WHERE symbol=? {where}", (symbol,)).fetchall()
    con.close()
    rows = [(t, rj) for t, rj in rows if t in TGT]
    frame = FE.load_frame(symbol, lookback_N=5)
    if frame.empty or not rows:
        return None
    yrs = frame["year"].values; nfr = len(frame)
    train = yrs <= TRAIN_MAX; sealed = (yrs == 2025) | (yrs == 2026)
    # cache the legal net-per-trade series for each (direction, pct, w)
    cache = {}
    for t, (d, pct, w) in TGT.items():
        for dd in ("up", "dn"):
            if (dd, pct, w) not in cache:
                cache[(dd, pct, w)] = routing.net_roc_series(frame, dd, pct, w, is_fno)[0]
    cards = []
    for t, rj in rows:
        d0, pct, w = TGT[t]
        conds = [tuple(c) for c in json.loads(rj)]
        mask = apply_rule(frame, conds).values
        fire = np.where(mask)[0]
        if len(fire) == 0:
            continue
        L = cache[("up", pct, w)]; S = cache[("dn", pct, w)]
        Ltr = dstats(L[fire][train[fire]]); Str = dstats(S[fire][train[fire]])
        etvL = Ltr["etv"] if Ltr else -1e9; etvS = Str["etv"] if Str else -1e9
        pref = "up" if etvL >= etvS else "dn"
        pref_tr = Ltr if pref == "up" else Str
        both_neg = etvL <= 0 and etvS <= 0
        se = dstats((L if pref == "up" else S)[fire][sealed[fire]])   # sealed realized (display only)
        pers = persistence((L if pref == "up" else S)[fire], yrs[fire])
        cards.append({"target": t, "pct": pct, "w": w, "mask": mask, "pref": pref,
                      "train": pref_tr, "sealed": se, "etvL": etvL, "etvS": etvS,
                      "pers": pers, "status": status(pref_tr, both_neg),
                      "rank_ecr": pref_tr["ecr"] if pref_tr else -1e9,
                      "keep": bool(pref_tr and pref_tr["etv"] > 0 and pref_tr["n"] >= MIN_OCC)})
    cards.sort(key=lambda c: -c["rank_ecr"])
    for i, c in enumerate(cards, 1):
        c["rank"] = i
    sealed_idx = np.where(sealed)[0]
    return {"symbol": symbol, "cards": cards, "cache": cache, "sealed_idx": sealed_idx, "is_fno": is_fno}


def run_book(patterns, cache, sealed_idx):
    """Non-overlapping campaign walk: each day pick the highest-ranked firing sub-agent, hold w days."""
    pats = sorted(patterns, key=lambda p: -p["rank_ecr"])
    total = 0.0; trades = []
    pos = 0
    while pos < len(sealed_idx):
        gi = sealed_idx[pos]
        chosen = None
        for p in pats:
            if p["mask"][gi]:
                chosen = p; break
        if chosen is None:
            pos += 1; continue
        d = chosen["pref"] if chosen.get("use_pref") else ("up" if chosen["target"][0] == "u" else "dn")
        net = cache[(d, chosen["pct"], chosen["w"])][gi]
        if np.isfinite(net):
            total += net; trades.append(net)
            pos += max(1, int(chosen["w"]))
        else:
            pos += 1
    return total, trades


def compare(res):
    cards, cache, sidx = res["cards"], res["cache"], res["sealed_idx"]
    # BASELINE = all kept patterns in their TARGET direction (today's "blend everything" approach)
    base_pool = [{**c, "use_pref": False} for c in cards if c["keep"]]
    base_ret, base_tr = run_book(base_pool, cache, sidx)
    # SUB-AGENT = top-K by <=2024 Expected Cumulative Return, in PREFERRED direction
    kept_pref = [{**c, "use_pref": True} for c in cards if c["keep"]]
    kept_pref.sort(key=lambda p: -p["rank_ecr"])
    sweep = {}
    for K in [3, 5, 8, 10, 15, 20, len(kept_pref)]:
        pool = kept_pref[:K]
        r, tr = run_book(pool, cache, sidx)
        sweep[K] = (r, len(tr))
    return base_ret, len(base_tr), sweep, len(kept_pref)


def main():
    sym = sys.argv[1] if len(sys.argv) > 1 else "FSL"
    kc = sqlite3.connect(KDB)
    is_fno = int(dict(kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()).get(sym, 0)); kc.close()
    t0 = time.time()
    res = analyze(sym, is_fno)
    if res is None:
        print(f"{sym}: no data/patterns"); return
    base_ret, base_n, sweep, nkept = compare(res)
    dt = time.time() - t0
    cards = res["cards"]
    print(f"\n===== SUB-AGENT ENGINE — {sym}  (is_fno={is_fno}) =====")
    print(f"patterns evaluated: {len(cards)}  | kept sub-agents (<=2024 ETV>0, N>={MIN_OCC}): {nkept}  | {dt:.2f}s")
    print("\n--- LEAK-FREE SEALED 2025-26 RETURN (invested-cash %, untrailed) ---")
    print(f"  BASELINE  (blend ALL {base_n} kept, target dir)       : {base_ret:+.0f}%")
    print("  SUB-AGENT (top-K by <=2024 earning power, pref dir):")
    for K, (r, n) in sweep.items():
        tag = " (all kept)" if K == nkept else ""
        lift = r - base_ret
        print(f"     top-{K:<3}{tag:11}: {r:+7.0f}%   ({n} trades)   lift vs baseline {lift:+.0f}%")
    print("\n--- TOP 12 SUB-AGENTS (ranked by <=2024 Expected Cumulative Return) ---")
    for c in cards[:12]:
        tr = c["train"]; se = c["sealed"]
        if not tr:
            continue
        conf = "HIGH" if tr["n"] >= 50 else "MED" if tr["n"] >= 20 else "LOW"
        pf = "inf" if np.isinf(tr["pf"]) else f"{tr['pf']:.2f}"
        se_ecr = f"{se['ecr']:+.0f}%" if se else "n/a"
        print(f"  #{c['rank']:<2} {c['target']:11} {('LONG' if c['pref']=='up' else 'SHORT'):5} "
              f"| N={tr['n']:<3}({conf:4}) WR={tr['wr']:4.1f}% AvgW={tr['aw']:+.2f} AvgL={-tr['al']:+.2f} "
              f"| ETV={tr['etv']:+.2f}% ExpCumRet(train)={tr['ecr']:+.0f}% PF={pf} MDD={tr['mdd']:.0f}% "
              f"pers={c['pers']:.2f} [{c['status']}]  sealed={se_ecr}")


if __name__ == "__main__":
    main()
