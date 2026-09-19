"""V6 — pooled across stocks. The change that actually matters.

WHY TWO STOCKS WAS THE CEILING
------------------------------
On one stock a rule only has to survive TIME. There is no way to distinguish
"this works on ADANIENT" from "this works on stocks that behave like ADANIENT",
and with ~250 independent bets the error bars swamp everything. Ten hours of
search on two tapes cannot answer a question that needs many tapes.

Pooled across 100 stocks, a rule must survive TIME *and* CROSS-SECTION. The same
rule is tested on a hundred different tapes at once. One that works on seventy
of them is a market regularity; one that works on three is noise -- and for the
first time you can SEE which it is.

AND THE PORTFOLIO STOPS BEING AN AFTERTHOUGHT
---------------------------------------------
V5 picked the best MINUTE on one stock, then held that one bet at four different
exits and called it a book. Here the model scores every (stock, minute) pair and
takes the top N across the whole universe each session. That is an actual
portfolio: different stocks, genuinely different bets, with the diversification
your +1%/day target depends on.

FEATURES ARE RANKED WITHIN EACH SESSION
---------------------------------------
Raw values cannot be pooled -- a 2% move means something different on a Rs 8
stock and a Rs 3,000 one. Every feature is converted to its cross-sectional
PERCENTILE among the stocks trading that minute. "In the top decile of volume
surge relative to everything else right now" is comparable across the universe;
"volume is 1.7x" is not.

Everything from v5_core stays: purged folds, shuffle control, seal, costs, and
the fixed-capital rule that makes concurrent positions split rather than sum.
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

import v5_core as core

LOOKBACKS = (5, 15, 30, 60)


def log(m: str = "") -> None:
    print(f"[{datetime.now():%H:%M:%S}] {m}", flush=True)


# --------------------------------------------------------------------------- #
def liquid_universe(db: str, table: str, n: int, start: str) -> list[str]:
    """Pick the most tradeable names by median turnover. Excludes indices."""
    con = sqlite3.connect(db)
    q = (f'SELECT symbol, AVG(close*volume) AS turn, COUNT(*) AS n '
         f'FROM "{table}" WHERE bar_time>=? AND bar_time<? '
         f'GROUP BY symbol HAVING n > 50000 AND turn > 0 '
         f'ORDER BY turn DESC LIMIT ?')
    rows = con.execute(q, [start, core.SEAL_FROM, n * 3]).fetchall()
    con.close()
    out = []
    for sym, turn, cnt in rows:
        u = sym.strip().upper()
        if u.startswith("NIFTY") or "BHARATBOND" in u or u == "INDIA VIX":
            continue
        out.append(sym)
        if len(out) >= n:
            break
    return out


def load_syms(db, table, syms, start):
    con = sqlite3.connect(db)
    ph = ",".join("?" * len(syms))
    d = pd.read_sql_query(
        f'SELECT symbol,bar_time,open,high,low,close,volume FROM "{table}" '
        f'WHERE symbol IN ({ph}) AND bar_time>=? AND bar_time<?',
        con, params=list(syms) + [start, core.SEAL_FROM])
    con.close()
    d["ts"] = pd.to_datetime(d["bar_time"])
    d["session"] = d["ts"].dt.normalize()
    return d.sort_values(["symbol", "ts"]).reset_index(drop=True)


def stock_session_points(g: pd.DataFrame, step: int) -> list[dict]:
    d = g.reset_index(drop=True)
    n = len(d)
    if n < 40:
        return []
    o = d["open"].to_numpy(float); h = d["high"].to_numpy(float)
    lo = d["low"].to_numpy(float); c = d["close"].to_numpy(float)
    v = d["volume"].to_numpy(float)
    ret = np.log(c / c[0])
    tp = (h + lo + c) / 3.0
    vwap = np.cumsum(tp * v) / (np.cumsum(v) + 1e-12)
    rng = (h - lo) / np.maximum(c, 1e-12)
    clv = np.where(h > lo, (c - lo) / np.maximum(h - lo, 1e-12), 0.5)
    bar_ret = np.diff(ret, prepend=0.0)
    out = []
    warm = min(60, max(n // 5, 15))
    for t in range(warm, n - 1, step):
        r = {"bar": t, "mins_open": t, "mins_close": n - 1 - t,
             "ret_day": ret[t], "vwap_d": c[t] / vwap[t] - 1.0,
             "clv_now": clv[t], "rng_now": rng[t],
             "pos_range": (c[t] - lo[:t+1].min())
                          / (h[:t+1].max() - lo[:t+1].min() + 1e-12),
             "d_high": c[t] / h[:t+1].max() - 1.0,
             "d_low": c[t] / lo[:t+1].min() - 1.0}
        for w in LOOKBACKS:
            a = slice(max(0, t + 1 - w), t + 1)
            seg = ret[a]
            r[f"mom_{w}"] = ret[t] - ret[max(0, t - w)]
            r[f"vol_{w}"] = float(np.std(bar_ret[a]))
            r[f"vsurge_{w}"] = float(v[a].mean() / (v[:t+1].mean() + 1e-12))
            r[f"clv_{w}"] = float(clv[a].mean())
            r[f"up_{w}"] = float((bar_ret[a] > 0).mean())
            r[f"rngc_{w}"] = float(rng[a].mean() / (rng[:t+1].mean() + 1e-12))
            r[f"acc_{w}"] = float(seg[-1] - 2*seg[len(seg)//2] + seg[0]) \
                if len(seg) > 2 else 0.0
        r["entry_px"] = float(o[t + 1])
        r["close_px"] = float(c[-1])
        r["px_60"] = float(c[min(t + 60, n - 1)])
        out.append(r)
    return out


FEATS = None


def build(db, table, syms, start, step, verbose=True):
    global FEATS
    bars = load_syms(db, table, syms, start)
    core.assert_sealed(bars)
    rows = []
    for (sym, sess), g in bars.groupby(["symbol", "session"], sort=False):
        for p in stock_session_points(g, step):
            p["symbol"] = sym
            p["session"] = sess
            rows.append(p)
    P = pd.DataFrame(rows)
    if P.empty:
        raise RuntimeError("no decision points built")
    # forward targets
    cl = bars.groupby(["symbol", "session"])["close"].last().reset_index()
    cl = cl.sort_values(["symbol", "session"])
    for nd in (1, 3):
        cl[f"fwd{nd}"] = cl.groupby("symbol")["close"].shift(-nd)
    m = cl.set_index(["symbol", "session"])
    P["y_close"] = core.net(P["close_px"] / P["entry_px"] - 1.0)
    P["y_60"] = core.net(P["px_60"] / P["entry_px"] - 1.0)
    for nd in (1, 3):
        f = m[f"fwd{nd}"].reindex(
            pd.MultiIndex.from_arrays([P["symbol"], P["session"]])).to_numpy()
        P[f"y_d{nd}"] = core.net(f / P["entry_px"].to_numpy() - 1.0)

    FEATS = [c for c in P.columns if c not in
             ("symbol", "session", "bar", "entry_px", "close_px", "px_60")
             and not c.startswith("y_")]
    # CROSS-SECTIONAL RANKING: the step that makes pooling valid at all
    g = P.groupby(["session", "bar"])
    for f in FEATS:
        P[f] = g[f].rank(pct=True)
    if verbose:
        log(f"  {len(P):,} decision points | {P['symbol'].nunique()} stocks | "
            f"{P['session'].nunique()} sessions | {len(FEATS)} features "
            f"(cross-sectionally ranked)")
    return P


# --------------------------------------------------------------------------- #
def run_leg(P, target, side, top_n, shuffle=False):
    from sklearn.ensemble import HistGradientBoostingRegressor
    d = P[np.isfinite(P[target].to_numpy(float))]
    # a split or a bad print can leave an infinite forward return; those rows
    # are dropped rather than allowed to poison the fit
    d = d[np.abs(d[target].to_numpy(float)) < 1.0]
    if len(d) < 5000:
        return None
    y = d[target].to_numpy(float) * (1.0 if side == "long" else -1.0)
    if shuffle:
        y = np.random.default_rng(5).permutation(y)
    X = np.nan_to_num(d[FEATS].to_numpy(np.float32), nan=0.5,
                      posinf=1.0, neginf=0.0)
    sess = d["session"].to_numpy()
    picks = []
    for tr_end, a, b in core.purged_folds(sess):
        tr = sess < tr_end
        te = (sess >= a) & (sess < b)
        if tr.sum() < core.MIN_TRAIN or te.sum() < core.MIN_TEST:
            continue
        m = HistGradientBoostingRegressor(
            max_iter=180, max_depth=6, learning_rate=0.07,
            min_samples_leaf=200, l2_regularization=1.0, random_state=7)
        m.fit(X[tr], y[tr])
        pr = m.predict(X[te])
        sub = pd.DataFrame({"session": sess[te], "symbol": d["symbol"].to_numpy()[te],
                            "pred": pr, "real": y[te]})
        # one entry per stock per session, then the best N stocks that session
        best = (sub.sort_values(["session", "symbol", "pred"],
                                ascending=[True, True, False])
                   .groupby(["session", "symbol"]).head(1))
        picks.append(best.sort_values(["session", "pred"], ascending=[True, False])
                         .groupby("session").head(top_n))
    if not picks:
        return None
    pk = pd.concat(picks, ignore_index=True)
    # fixed capital: the session earns the MEAN of the N positions
    per = pk.groupby("session")["real"].mean()
    return {"n": len(pk), "sessions": int(per.size),
            "mean_pct": float(pk["real"].mean() * 100),
            "win": float((pk["real"] > 0).mean()),
            "per_session": float(per.sum() / P["session"].nunique() * 100),
            "picks": pk, "breadth": int(pk["symbol"].nunique())}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--table", default="ohlc_1min")
    ap.add_argument("--stocks", type=int, default=60)
    ap.add_argument("--symbols", help="comma separated, overrides --stocks")
    ap.add_argument("--capital", type=float, default=30000.0)
    ap.add_argument("--start", default="2022-01-01")
    ap.add_argument("--step", type=int, default=15)
    ap.add_argument("--top-n", type=int, default=5, help="positions per session")
    ap.add_argument("--out", default="v6_out")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    print("=" * 76, flush=True)
    print(f"V6 POOLED AGENT  |  many stocks, one model, a real portfolio",
          flush=True)
    print(f"Rs {a.capital:,.0f} fixed | {a.top_n} positions/session | "
          f"2026 SEALED | core {core.CORE_HASH}", flush=True)
    print("=" * 76, flush=True)

    syms = ([s.strip() for s in a.symbols.split(",")] if a.symbols
            else liquid_universe(a.db, a.table, a.stocks, a.start))
    log(f"universe: {len(syms)} stocks -> {', '.join(syms[:8])}"
        f"{'...' if len(syms) > 8 else ''}")
    t0 = time.time()
    P = build(a.db, a.table, syms, a.start, a.step)
    log(f"  built in {(time.time()-t0)/60:.1f} min")

    rows, legs, shl = [], {}, {}
    for target in ("y_60", "y_close", "y_d1", "y_d3"):
        for side in ("long", "short"):
            r = run_leg(P, target, side, a.top_n)
            if r is None:
                continue
            s = run_leg(P, target, side, a.top_n, shuffle=True)
            ok, why = core.verdict(r["per_session"],
                                   s["per_session"] if s else 0.0, r["n"])
            log(f"  {target:<8} {side:<5} n={r['n']:<6} "
                f"{r['mean_pct']:+.3f}%/trade  win {r['win']:.0%}  "
                f"across {r['breadth']} stocks  -> {r['per_session']:+.3f}%/session"
                f"  shuffled {s['per_session'] if s else 0:+.3f}%  "
                f"{'KEEP' if ok else why}")
            rows.append({"target": target, "side": side, **{
                k: v for k, v in r.items() if k != "picks"},
                "shuffled": s["per_session"] if s else 0.0, "keep": ok})
            if ok:
                legs[f"{target}|{side}"] = r["picks"]
                shl[f"{target}|{side}"] = s["picks"] if s else None

    raw = core.book_per_session(legs, P["session"].nunique())
    shb = core.book_per_session(shl, P["session"].nunique())
    log("")
    log(f"  BOOK  {len(legs)} legs: {', '.join(legs) if legs else 'none'}")
    log(f"    raw      {raw:+.3f}%/session   (Rs {a.capital*raw/100:,.0f}/day)")
    log(f"    shuffled {shb:+.3f}%/session")
    log(f"    EDGE     {raw-shb:+.3f}%/session   "
        f"(Rs {a.capital*(raw-shb)/100:,.0f}/day)")
    log(f"    target +1.000% (Rs {a.capital/100:,.0f}/day)   "
        f"gap {1.0-(raw-shb):+.3f}")
    pd.DataFrame(rows).to_csv(os.path.join(a.out, "v6_legs.csv"), index=False)
    log(f"  wrote {a.out}/v6_legs.csv")


if __name__ == "__main__":
    main()
