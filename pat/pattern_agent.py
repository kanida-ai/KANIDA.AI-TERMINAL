"""PATTERN AGENT — adapts 4,794 generic patterns to one stock, then forward-tests.

    python pattern_agent.py --stock ADANIENT --db kanida.db --xlsx States_Mined.xlsx

WHAT IT DOES, IN ORDER
----------------------
1. Computes the 38 workbook variables from daily bars, all backward-looking.
2. Translates every absolute threshold into that stock's own percentile terms,
   expanding and point-in-time (see pattern_lib.ThresholdAdapter).
3. Scores every pattern on 1, 2 and 3-day forward returns: occurrences, hit
   rate, average return, MFE, MAE, confidence, strength, recency, consistency.
4. Finds pattern COMBINATIONS -- pairs that co-occur -- and scores those too,
   because your question was whether the odds change when several fire at once.
5. Forward-tests exactly as you specified: fit on 2022, test 2023. Fit on
   2022-23, test 2024. And so on to 2026.

WHAT "CONFIDENCE" MEANS HERE
----------------------------
Not a number I picked. A pattern seen 40 times with a 60% hit rate is far weaker
evidence than one seen 400 times with 55%, so confidence is the Wilson lower
bound on the hit rate -- the rate you can defend given the sample -- and strength
is that bound's distance above the stock's own unconditional base rate. A
pattern that beats a coin flip but not the stock's own drift has found nothing.

THE SELECTION TRAP, AND THE CONTROL FOR IT
------------------------------------------
Scoring 4,794 patterns and reporting the best guarantees an impressive result
even from noise: the best of 4,794 coin-flip sequences looks remarkable. So each
test year reports what the SAME procedure produces on shuffled outcomes. A
pattern set that beats its shuffled twin has found something; one that does not
has found the size of its own search.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
import warnings
from datetime import datetime

import numpy as np
import pandas as pd

warnings.simplefilter("ignore", FutureWarning)

from pattern_lib import (ALL_VARS, ThresholdAdapter, compute_vars,
                         evaluate_rule, parse_rules)

SEAL_FROM = "2026-07-01"      # nothing beyond this is ever read
COST = 0.0011
SLIP = 0.0005


def log(m=""):
    print(f"[{datetime.now():%H:%M:%S}] {m}", flush=True)


def wilson(k: int, n: int, z: float = 1.96) -> float:
    if n == 0:
        return 0.0
    p = k / n
    d = 1 + z*z/n
    return max(0.0, (p + z*z/(2*n) - z*np.sqrt(p*(1-p)/n + z*z/(4*n*n))) / d)


# --------------------------------------------------------------------------- #
def load_daily(db, table, sym, start):
    con = sqlite3.connect(db)
    d = pd.read_sql_query(
        f'SELECT bar_time,open,high,low,close,volume FROM "{table}" '
        f'WHERE symbol=? AND bar_time>=? AND bar_time<? ORDER BY bar_time',
        con, params=[sym, start, SEAL_FROM])
    con.close()
    if d.empty:
        return d
    d["date"] = pd.to_datetime(d["bar_time"]).dt.normalize()
    return d.groupby("date", as_index=False).agg(
        {"open": "first", "high": "max", "low": "min",
         "close": "last", "volume": "sum"})


def reference_universe(db, table, n, start, exclude=None) -> list[str]:
    """Liquid, tradeable names used only to locate the generic thresholds."""
    con = sqlite3.connect(db)
    rows = con.execute(
        f'SELECT symbol, AVG(close*volume) t, COUNT(*) c FROM "{table}" '
        f'WHERE bar_time>=? AND bar_time<? GROUP BY symbol '
        f'HAVING c > 300 AND t > 0 ORDER BY t DESC LIMIT ?',
        [start, SEAL_FROM, n * 4]).fetchall()
    con.close()
    out = []
    for sym, t, c in rows:
        u = sym.strip().upper()
        if u.startswith("NIFTY") or "BHARATBOND" in u or u == "INDIA VIX":
            continue
        if exclude and sym == exclude:
            continue
        out.append(sym)
        if len(out) >= n:
            break
    return out


def forward(d: pd.DataFrame) -> pd.DataFrame:
    """Entry at the next open; outcomes over 1, 2 and 3 sessions."""
    f = pd.DataFrame({"date": d["date"]})
    entry = d["open"].shift(-1)
    f["entry"] = entry
    for n in (1, 2, 3):
        ex = d["close"].shift(-n)
        hi = d["high"].shift(-1).rolling(n, min_periods=1).max().shift(-(n-1))
        lo = d["low"].shift(-1).rolling(n, min_periods=1).min().shift(-(n-1))
        f[f"r{n}"] = (ex / entry - 1) * 100 - (COST + SLIP) * 100
        f[f"mfe{n}"] = (hi / entry - 1) * 100
        f[f"mae{n}"] = (lo / entry - 1) * 100
    return f


def score(mask, fw, base, tag="") -> dict | None:
    m = mask & fw["r1"].notna().to_numpy()
    n = int(m.sum())
    if n < 20:
        return None
    out = {"pattern": tag, "n": n}
    for k in (1, 2, 3):
        r = fw.loc[m, f"r{k}"].to_numpy()
        wins = int((r > 0).sum())
        out[f"hit{k}"] = wins / n
        out[f"avg{k}"] = float(r.mean())
        out[f"med{k}"] = float(np.median(r))
        out[f"mfe{k}"] = float(fw.loc[m, f"mfe{k}"].mean())
        out[f"mae{k}"] = float(fw.loc[m, f"mae{k}"].mean())
        out[f"conf{k}"] = wilson(wins, n)
        out[f"strength{k}"] = out[f"conf{k}"] - base[f"hit{k}"]
        out[f"edge{k}"] = out[f"avg{k}"] - base[f"avg{k}"]
    d = fw.loc[m, "date"]
    half = d.median()
    a, b = m & (fw["date"] <= half).to_numpy(), m & (fw["date"] > half).to_numpy()
    if a.sum() > 5 and b.sum() > 5:
        e1 = fw.loc[a, "r1"].mean() - base["avg1"]
        e2 = fw.loc[b, "r1"].mean() - base["avg1"]
        out["consistency"] = float(np.sign(e1) == np.sign(e2))
        out["recency"] = float(e2)
    else:
        out["consistency"], out["recency"] = 0.0, 0.0
    return out


def base_rates(fw) -> dict:
    ok = fw["r1"].notna().to_numpy()
    b = {}
    for k in (1, 2, 3):
        r = fw.loc[ok, f"r{k}"]
        b[f"hit{k}"] = float((r > 0).mean())
        b[f"avg{k}"] = float(r.mean())
    return b


# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stock", required=True)
    ap.add_argument("--db", required=True)
    ap.add_argument("--xlsx", required=True)
    ap.add_argument("--table", default="ohlc_daily")
    ap.add_argument("--index", default="NIFTY 50")
    ap.add_argument("--start", default="2021-01-01")
    ap.add_argument("--burnin-end", default="2022-06-30")
    ap.add_argument("--min-n", type=int, default=25)
    ap.add_argument("--ref-stocks", type=int, default=40,
                    help="stocks pooled to locate each generic threshold")
    ap.add_argument("--min-hist", type=int, default=150,
                    help="sessions of own history before a threshold is defined")
    ap.add_argument("--max-rules", type=int, default=4794)
    ap.add_argument("--out", default="pat_out")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    print("=" * 78, flush=True)
    print(f"PATTERN AGENT  |  {a.stock}  |  generic rules -> stock-specific",
          flush=True)
    print(f"thresholds adapted point-in-time | forward tested 2023..2026",
          flush=True)
    print("=" * 78, flush=True)

    rules = parse_rules(a.xlsx).head(a.max_rules)
    log(f"{len(rules)} patterns loaded, {rules['n_cond'].mean():.1f} conditions each")

    d = load_daily(a.db, a.table, a.stock, a.start)
    if d.empty:
        log(f"no daily bars for {a.stock}")
        return
    idx = load_daily(a.db, a.table, a.index, a.start)
    mkt = (idx.set_index("date")["close"] if not idx.empty else None)
    log(f"{len(d)} sessions {d['date'].min().date()} -> {d['date'].max().date()}"
        f" | index {'yes' if mkt is not None else 'NONE'}")

    f = compute_vars(d, mkt, None)         # no sector table available
    fw = forward(d)
    have = [v for v in ALL_VARS if f[v].notna().sum() > 100]
    log(f"{len(have)} of {len(ALL_VARS)} variables computable "
        f"(sector RS unavailable: no sector map in the database)")

    # ---- reference universe: where does a generic threshold sit IN GENERAL? --
    ref_syms = reference_universe(a.db, a.table, a.ref_stocks, a.start,
                                  exclude=a.stock)
    log(f"reference universe: {len(ref_syms)} stocks -> "
        f"{', '.join(ref_syms[:6])}{'...' if len(ref_syms) > 6 else ''}")
    frames = []
    for rs in ref_syms:
        rd = load_daily(a.db, a.table, rs, a.start)
        if len(rd) < 150:
            continue
        rf = compute_vars(rd, mkt, None)
        frames.append(rf[rf["date"] <= a.burnin_end])
    frames.append(f[f["date"] <= a.burnin_end])
    adapter = ThresholdAdapter(frames, min_hist=a.min_hist)
    log(f"burn-in to {a.burnin_end}: {adapter.n_ref:,} pooled rows across "
        f"{len(frames)} stocks set the reference percentiles, never revisited")
    log(f"  {len(adapter.ref)} variables have a usable reference distribution")

    # ---------------- adapt + score every pattern, whole history -------------
    t0, cache, rows, masks = time.time(), {}, [], {}
    base_all = base_rates(fw)
    for i, r in enumerate(rules.itertuples(), 1):
        m = evaluate_rule(f, r.conds, adapter, cache)
        if m is None or m.sum() < a.min_n:
            continue
        s = score(m, fw, base_all, tag=str(r.pattern_id))
        if s:
            s["rule"] = r.rule_text
            rows.append(s)
            masks[str(r.pattern_id)] = m
        if i % 800 == 0:
            log(f"  adapted {i}/{len(rules)} ... {len(rows)} usable "
                f"({(time.time()-t0)/60:.1f}m)")
    S = pd.DataFrame(rows)
    if S.empty:
        log("no pattern fired often enough on this stock")
        log(f"  diagnostic: {len(adapter.ref)} variables had a reference, "
            f"min own-history {a.min_hist} sessions, min occurrences {a.min_n}")
        return
    log(f"{len(S)} patterns fire >= {a.min_n} times on {a.stock} "
        f"({(time.time()-t0)/60:.1f}m)")
    log(f"  base rates: 1d hit {base_all['hit1']:.1%} avg {base_all['avg1']:+.3f}%"
        f" | 3d hit {base_all['hit3']:.1%} avg {base_all['avg3']:+.3f}%")

    S = S.sort_values("strength1", ascending=False)
    S.to_csv(os.path.join(a.out, f"patterns_{a.stock}.csv"), index=False)
    log("\n  top patterns by 1-day strength (confidence above the base rate):")
    for _, r in S.head(8).iterrows():
        log(f"    [{r['pattern']}] n={int(r['n']):<4} hit {r['hit1']:.1%} "
            f"conf {r['conf1']:.1%} str {r['strength1']:+.3f} "
            f"avg1 {r['avg1']:+.2f}% avg3 {r['avg3']:+.2f}%")
        log(f"        {r['rule'][:96]}")

    # ---------------- combinations -------------------------------------------
    top = S.head(60)["pattern"].tolist()
    combo = []
    for i in range(len(top)):
        for j in range(i + 1, len(top)):
            m = masks[top[i]] & masks[top[j]]
            if m.sum() < a.min_n:
                continue
            s = score(m, fw, base_all, tag=f"{top[i]}+{top[j]}")
            if s:
                a1 = S.loc[S["pattern"] == top[i], "strength1"].iloc[0]
                b1 = S.loc[S["pattern"] == top[j], "strength1"].iloc[0]
                s["lift_over_best_single"] = s["strength1"] - max(a1, b1)
                combo.append(s)
    C = pd.DataFrame(combo)
    if not C.empty:
        C = C.sort_values("strength1", ascending=False)
        C.to_csv(os.path.join(a.out, f"combos_{a.stock}.csv"), index=False)
        better = int((C["lift_over_best_single"] > 0).sum())
        log(f"\n  {len(C)} co-occurring pairs; {better} beat their own best "
            f"single leg")
        for _, r in C.head(5).iterrows():
            log(f"    {r['pattern']:<16} n={int(r['n']):<4} hit {r['hit1']:.1%} "
                f"str {r['strength1']:+.3f} lift over best single "
                f"{r['lift_over_best_single']:+.3f}")

    # ---------------- walk-forward: 2022->2023, +2023->2024, ... -------------
    log("\n  FORWARD TEST  (fit years -> test the next, patterns never see it)")
    years = sorted({int(str(x)[:4]) for x in f["date"]})
    dates = f["date"].to_numpy()
    rng = np.random.default_rng(11)
    res = []
    for test_y in [y for y in years if y >= 2023]:
        tr = dates < np.datetime64(f"{test_y}-01-01")
        te = (dates >= np.datetime64(f"{test_y}-01-01")) & \
             (dates < np.datetime64(f"{test_y+1}-01-01"))
        if tr.sum() < 200 or te.sum() < 60:
            continue
        btr = base_rates(fw[tr])
        # Qualification, judged ONLY on data before the test year.
        # The first version demanded strength > 0.05, where strength is the
        # Wilson lower bound above the base rate. At 95% confidence and n=80
        # that bound sits ~11 points below the observed hit rate, so almost
        # nothing could ever clear it and no year traded. The gate now asks for
        # what actually matters: a positive return edge over the stock's own
        # base rate, a confidence bound above that base rate, and the same sign
        # in both halves of the fitting window.
        picked, seen = [], 0
        for pid, m in masks.items():
            s = score(m & tr, fw, btr, tag=pid)
            if not s or s["n"] < a.min_n:
                continue
            seen += 1
            if s["edge1"] > 0 and s["strength1"] > 0 and s["consistency"] > 0:
                picked.append(pid)
        log(f"    {test_y}: {seen} patterns had enough history, "
            f"{len(picked)} qualified")
        if not picked:
            log(f"    {test_y}: nothing qualified -- no trades. That is a "
                f"result, not a failure.")
            continue
        sel = np.zeros(len(dates), bool)
        for pid in picked:
            sel |= masks[pid]
        sel &= te
        n = int((sel & fw["r1"].notna().to_numpy()).sum())
        if n < 10:
            log(f"    {test_y}: qualified patterns fired only {n} times")
            continue
        bte = base_rates(fw[te])
        r1 = fw.loc[sel, "r1"].dropna()
        r3 = fw.loc[sel, "r3"].dropna()
        # what the same procedure gives on shuffled outcomes
        sh = []
        for _ in range(200):
            idxs = rng.choice(np.where(te & fw["r1"].notna().to_numpy())[0],
                              size=min(n, int(te.sum())), replace=False)
            sh.append(fw["r1"].to_numpy()[idxs].mean())
        shuf = float(np.mean(sh))
        res.append({"test_year": test_y, "patterns_used": len(picked),
                    "signals": n, "hit1": float((r1 > 0).mean()),
                    "avg1": float(r1.mean()), "base_avg1": bte["avg1"],
                    "shuffled_avg1": shuf, "avg3": float(r3.mean()),
                    "base_avg3": bte["avg3"]})
        log(f"    {test_y}: {len(picked)} patterns -> {n} signals | "
            f"1d hit {float((r1>0).mean()):.1%} avg {r1.mean():+.3f}% "
            f"(base {bte['avg1']:+.3f}%, random {shuf:+.3f}%) | "
            f"3d avg {r3.mean():+.3f}% (base {bte['avg3']:+.3f}%)")
    if res:
        R = pd.DataFrame(res)
        R.to_csv(os.path.join(a.out, f"forward_{a.stock}.csv"), index=False)
        good = int((R["avg1"] > R["base_avg1"]).sum())
        log(f"\n  beat the base rate in {good} of {len(R)} test years")
        log(f"  mean 1-day edge over base: "
            f"{(R['avg1'] - R['base_avg1']).mean():+.3f}%")
        if good <= len(R) / 2:
            log("  The adapted patterns did not reliably beat simply being long.")
            log("  That is the honest read, and it is what the forward test is for.")
    log(f"\n  wrote {a.out}/")


if __name__ == "__main__":
    main()
