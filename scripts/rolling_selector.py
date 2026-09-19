"""
ROLLING SELF-LEARNING pattern selector (point-in-time, monthly re-selection, quarterly-smoothed score).
Replaces "keep patterns with best <=2024 average ETV" with a score that rewards PERSISTENCE + RECENCY:
at each month-start it re-ranks every pattern using ONLY data before that month, drops decayers, adds
newly-effective ones. Realizes the chosen patterns with the per-stock intraday trail, then grades the
full 2025-26 book through the SAME 7-gate gauntlet. Leak-free walk-forward.

Score for a pattern (a direction) at decision month T, from its firing-day net returns BEFORE T:
  - bucket into calendar quarters; ETV_q = mean net return that quarter (>= MIN_Q trades)
  - recency-weighted mean S = sum(w_q * ETV_q)/sum(w_q), w_q = 0.5**(quarters_ago / HALF_LIFE_Q)
  - persistence P = fraction of recent quarters with ETV_q > 0
  - no-decay: recent-half mean ETV >= older-half mean ETV
  - robust: S with the single best quarter removed still > 0
  KEEP if S>0 and P>=PERSIST_BAR and no-decay and robust. Rank by S*P. Direction = higher-S side.

Run: python scripts/rolling_selector.py KEC SAPPHIRE FSL DIXON   (or 'all')
"""
from __future__ import annotations
import os, sys, json, sqlite3, time
os.environ.setdefault("SLIP_CAP", "0.5"); os.environ.setdefault("OMP_NUM_THREADS", "1")
from pathlib import Path
import numpy as np
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
for p in ("scripts", "kanida_engine", "arena"):
    sys.path.insert(0, str(ROOT / p))
from mine_phase1 import apply_rule
import features as FE, routing, backtest_1min as B, intraday_trail as IT
from subagent_trailed import trailmap, gauntlet
SNR = str(ROOT / "db" / "KANIDA_SNR.db"); KDB = str(ROOT / "db" / "kanida.db"); MARGIN = B.MARGIN
TGT = {"up_1pct_1d": ("up", 1, 1), "up_2pct_2d": ("up", 2, 2), "up_5pct_5d": ("up", 5, 5),
       "dn_1pct_1d": ("dn", 1, 1), "dn_2pct_2d": ("dn", 2, 2), "dn_5pct_5d": ("dn", 5, 5)}
HALF_LIFE_Q = float(os.environ.get("HALF_LIFE_Q", "4"))     # 4 quarters = ~1yr recency half-life
MIN_Q = int(os.environ.get("MIN_Q", "2"))                   # min trades in a quarter to count it
MIN_QUARTERS = int(os.environ.get("MIN_QUARTERS", "3"))     # need >= this many scored quarters
RECENT_Q = int(os.environ.get("RECENT_Q", "8"))             # persistence/decay window (quarters)
PERSIST_BAR = float(os.environ.get("PERSIST_BAR", "0.55"))  # >= this fraction of recent quarters positive


def score_dir(rets, qk, cur_q):
    """rets/qk = net returns & quarter-keys of a pattern's pre-T firing days (one direction). Returns dict or None."""
    if len(rets) < 10:
        return None
    qs = np.unique(qk); etv = []; qidx = []
    for q in qs:
        r = rets[qk == q]
        if len(r) >= MIN_Q:
            etv.append(float(r.mean())); qidx.append(int(q))
    if len(etv) < MIN_QUARTERS:
        return None
    etv = np.array(etv); qidx = np.array(qidx)
    ago = cur_q - qidx
    w = 0.5 ** (ago / HALF_LIFE_Q)
    S = float(np.sum(w * etv) / np.sum(w))
    rec = ago <= RECENT_Q
    P = float(np.mean(etv[rec] > 0)) if rec.any() else 0.0
    n = len(etv); half = n // 2
    no_decay = (etv[half:].mean() >= etv[:half].mean()) if n >= 2 else True
    if n > 1:
        b = int(np.argmax(etv)); keep = np.ones(n, bool); keep[b] = False
        S_rob = float(np.sum(w[keep] * etv[keep]) / np.sum(w[keep]))
    else:
        S_rob = S
    passes = (S > 0) and (P >= PERSIST_BAR) and no_decay and (S_rob > 0)
    return {"S": S, "P": P, "passes": passes}


def prep(sym, is_fno):
    con = sqlite3.connect(SNR)
    rows = [(t, rj) for t, rj in con.execute("SELECT target,rule_json FROM unified_patterns WHERE symbol=? AND promoted=1", (sym,)).fetchall() if t in TGT]
    con.close()
    frame = FE.load_frame(sym, lookback_N=5)
    if frame.empty or not rows:
        return None
    idx = frame.index
    qkey = np.array([d.year * 4 + (d.month - 1) // 3 for d in idx])
    mkey = np.array([d.year * 12 + (d.month - 1) for d in idx])
    net = {}
    for _t, (d, pct, w) in TGT.items():
        for dd in ("up", "dn"):
            if (dd, pct, w) not in net:
                net[(dd, pct, w)] = routing.net_roc_series(frame, dd, pct, w, is_fno)[0]
    pats = []
    for t, rj in rows:
        d0, pct, w = TGT[t]
        mask = apply_rule(frame, [tuple(c) for c in json.loads(rj)]).values
        fidx = np.where(mask)[0]
        if len(fidx) == 0:
            continue
        Ln = net[("up", pct, w)][fidx]; Sn = net[("dn", pct, w)][fidx]; qf = qkey[fidx]
        pats.append({"mask": mask, "pct": pct, "w": w, "fidx": fidx,
                     "L": Ln, "S": Sn, "qf": qf})
    return {"frame": frame, "qkey": qkey, "mkey": mkey, "pats": pats, "net": net}


def select_at(pats, t_pos, cur_q):
    """Point-in-time selection using only firing days with index < t_pos."""
    out = []
    for p in pats:
        sub = p["fidx"] < t_pos
        if sub.sum() < 10:
            continue
        qf = p["qf"][sub]
        L = p["L"][sub]; S = p["S"][sub]
        okL = np.isfinite(L); okS = np.isfinite(S)
        sl = score_dir(L[okL], qf[okL], cur_q); ss = score_dir(S[okS], qf[okS], cur_q)
        best = None
        if sl and sl["passes"]:
            best = ("up", sl)
        if ss and ss["passes"] and (best is None or ss["S"] > best[1]["S"]):
            best = ("dn", ss)
        if best:
            out.append({"mask": p["mask"], "dir": best[0], "pct": p["pct"], "w": p["w"],
                        "rank": best[1]["S"] * max(best[1]["P"], 0.01)})
    out.sort(key=lambda x: -x["rank"])
    return out


def rolling_book(sym, is_fno):
    d = prep(sym, is_fno)
    if d is None:
        return None
    frame = d["frame"]; qkey = d["qkey"]; mkey = d["mkey"]; pats = d["pats"]
    O = frame["_o"].values; H = frame["_h"].values; L = frame["_l"].values; C = frame["_c"].values
    yr = frame["year"].values; n = len(O); dstr = np.array([str(x.date()) for x in frame.index])
    m1 = B.load_1min(sym)
    if not m1:
        return None
    (method, params), _ = trailmap(sym)
    te = np.where((yr == 2025) | (yr == 2026))[0]
    legs = []; day = 0; pid = 0; cur_m = -1; pool = []
    while day < len(te):
        gi = te[day]
        if mkey[gi] != cur_m:                                   # new month -> re-select on data < this month
            cur_m = mkey[gi]; pool = select_at(pats, gi, int(qkey[gi]))
        chosen = None
        for p in pool:
            if p["mask"][gi]:
                chosen = p; break
        if chosen is None:
            day += 1; continue
        dd_, pct, w = chosen["dir"], chosen["pct"], chosen["w"]
        product, mode = routing.route(dd_, w, is_fno); long = (dd_ == "up")
        r = routing.sim_roc(O, H, L, C, gi, dd_, pct, w, product, mode)
        if r is None:
            day += 1; continue
        exi = r[2]; pid += 1
        if mode == "daily":
            for x in range(gi + 1, min(gi + 1 + w, n)):
                g = m1.get(dstr[x])
                if g is None or len(g["o"]) < 5:
                    continue
                res = IT.sim_day(g, method, params)
                if res is None:
                    continue
                legs.append((int(yr[x]), float(res[0]), pid))
            day += max(1, exi - gi)
        else:
            g = m1.get(dstr[gi + 1])
            if g is None or len(g["o"]) == 0:
                day += max(1, exi - gi); continue
            entry = float(g["o"][0]); tgt = entry * (1 + pct / 100) if long else entry * (1 - pct / 100)
            expx = None
            for x in range(gi + 1, min(gi + 1 + w, n)):
                gx = m1.get(dstr[x])
                if gx is None:
                    continue
                hit = np.where(gx["h"] >= tgt)[0] if long else np.where(gx["l"] <= tgt)[0]
                if len(hit):
                    expx = tgt; break
            if expx is None:
                lastd = m1.get(dstr[exi]) or g
                expx = B.px_at(lastd, "15:20")[0] if product == "MIS" else float(lastd["c"][-1])
            gross = (expx / entry - 1) * 100 if long else (1 - expx / entry) * 100
            legs.append((int(yr[gi + 1]), float((gross - routing.COST[product]) * routing.LEV[product]), pid))
            day += max(1, exi - gi)
    return legs


def run_one(sym):
    kc = sqlite3.connect(KDB)
    is_fno = int(dict(kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()).get(sym, 0)); kc.close()
    try:
        legs = rolling_book(sym, is_fno)
    except Exception as e:
        return {"sym": sym, "err": str(e)[:70]}
    if not legs:
        return {"sym": sym, "err": "no legs"}
    g = gauntlet(legs)
    a = np.array([l[1] for l in legs]); cr = np.cumsum(a)
    eqf = 100000.0 * (1 + cr / 100.0); pk = np.maximum.accumulate(eqf)
    acct = float(((eqf - pk) / pk * 100).min())
    return {"sym": sym, "tier": g["tier"], "gates": g["gates"], "ret": round(a.sum()),
            "r2025": round(g["r2025"]), "r2026": round(g["r2026"]), "acct_mdd": round(acct, 1),
            "worst": g["worst"], "retdd26": g["retdd26"]}


def main():
    args = sys.argv[1:]
    if args == ["all"]:
        syms = sorted(set(c["symbol"] for c in json.load(open(ROOT / "reports" / "worker_cards.json"))))
    elif args:
        syms = args
    else:
        syms = ["KEC", "SAPPHIRE", "FSL", "DIXON", "PETRONET", "ITC", "NEWGEN", "POLYMED"]
    # current sellable set for comparison
    import pandas as pd
    exp = pd.read_csv(ROOT / "reports" / "worker_readiness_expanded.csv")
    sellable_now = set(exp.symbol)
    t0 = time.time()
    if len(syms) > 12:
        import multiprocessing as mp
        with mp.get_context("spawn").Pool(8) as pool:
            rows = pool.map(run_one, syms)
    else:
        rows = [run_one(s) for s in syms]
    ok = [r for r in rows if "err" not in r]
    dt = time.time() - t0
    print(f"\n===== ROLLING SELF-LEARNING SELECTOR (monthly, quarterly-smoothed) — {len(ok)} workers, {dt:.1f}s =====")
    print(f"{'sym':<11}{'now?':>6}{'ROLLING tier':>14}{'ret':>7}{'2025':>7}{'2026':>7}{'acctMDD':>9}{'worst':>7}")
    roll_ready = 0
    for r in sorted(ok, key=lambda r: -r["ret"]):
        now = "SELL" if r["sym"] in sellable_now else "-"
        roll_ready += (r["tier"] == "READY")
        print(f"{r['sym']:<11}{now:>6}{r['tier']:>14}{r['ret']:>+6}%{r['r2025']:>+6}%{r['r2026']:>+6}%{r['acct_mdd']:>8}%{r['worst']:>6}%")
    if len(ok) > 12:
        roll = set(r["sym"] for r in ok if r["tier"] == "READY")
        new = sorted(roll - sellable_now); lost = sorted(sellable_now - roll)
        print(f"\n  rolling-READY: {len(roll)}   currently sellable: {len(sellable_now & set(r['sym'] for r in ok))}")
        print(f"  NEW under rolling (not sellable today): {len(new)}")
        print(f"  {new[:40]}")
        print(f"  current-sellable that rolling KEEPS: {len(sellable_now & roll)} / {len(sellable_now)}")
    errs = [r for r in rows if 'err' in r]
    if errs and len(errs) < 15:
        print("  errors:", {r['sym']: r['err'] for r in errs})


if __name__ == "__main__":
    main()
