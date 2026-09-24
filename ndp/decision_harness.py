#!/usr/bin/env python3
"""ICICI intraday breakout DECISION HARNESS (user's script, wired to our 1-min DB + prev_ret selector).
Ordered target-vs-stop resolution (pessimistic tie-break), realized net-of-cost P&L, learn/OOS split,
bootstrap CIs. Adds the prev_ret (prior-day momentum) conditioner we found robust. Index conditioner off."""
import os
import numpy as np
import pandas as pd
from datetime import time as dtime

SYMBOL = "ICICIBANK"
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "universe_engine", "data", "db", "kanida_universe.db")
INDEX_SYMBOL = None
TRIGGER_PCT = 0.005
TARGET_PCT = 0.007
DISASTER_PCT = 0.006
TRAIL_PCT = 0.0045
EOD_EXIT = dtime(15, 20)
ENTRY_WINDOW = (dtime(9, 16), dtime(15, 0))
COST_BPS = 13.0
OOS_SPLIT_FRAC = 0.60
MIN_N_PER_CELL = 40
N_BOOT = 2000
SEED = 7


def load_minutes(symbol):
    import sqlite3
    con = sqlite3.connect("file:" + DB_PATH.replace("\\", "/") + "?mode=ro", uri=True)
    df = pd.read_sql("SELECT bar_time AS ts, open, high, low, close, volume FROM ohlc_1min WHERE symbol=? ORDER BY bar_time",
                     con, params=(symbol,), parse_dates=["ts"])
    con.close()
    return df


def sessionize(df):
    df = df.copy()
    df["date"] = df["ts"].dt.date
    df["tod"] = df["ts"].dt.time
    return {d: g.reset_index(drop=True) for d, g in df.groupby("date")}


def daily_bars(sessions):
    return pd.DataFrame([dict(date=d, open=g["open"].iloc[0], high=g["high"].max(), low=g["low"].min(), close=g["close"].iloc[-1])
                         for d, g in sessions.items()]).sort_values("date").reset_index(drop=True)


def atr_pct_series(daily, n=20):
    pc = daily["close"].shift(1)
    tr = pd.concat([daily["high"] - daily["low"], (daily["high"] - pc).abs(), (daily["low"] - pc).abs()], axis=1).max(axis=1)
    return (tr.rolling(n).mean() / daily["close"]).shift(1)


def find_trigger(sess, side, o):
    lvl = o * (1 + TRIGGER_PCT) if side == "long" else o * (1 - TRIGGER_PCT)
    in_win = (sess["tod"] >= ENTRY_WINDOW[0]) & (sess["tod"] <= ENTRY_WINDOW[1])
    hit = (sess["high"] >= lvl) if side == "long" else (sess["low"] <= lvl)
    idx = sess.index[in_win & hit]
    return (int(idx[0]), lvl) if len(idx) else None


def simulate(sess, i0, entry, o, side):
    s = 1.0 if side == "long" else -1.0
    tgt = o * (1 + s * TARGET_PCT)
    stp = o
    dstp = entry * (1 - s * DISASTER_PCT)
    post = sess.iloc[i0:]
    eod_i = post.index[post["tod"] <= EOD_EXIT]
    if len(eod_i) == 0:
        return None
    post = post.loc[:eod_i[-1]]
    adverse = lambda b: b["low"] if side == "long" else b["high"]
    favour = lambda b: b["high"] if side == "long" else b["low"]
    worse = lambda px, lvl: (px <= lvl if side == "long" else px >= lvl)
    better = lambda px, lvl: (px >= lvl if side == "long" else px <= lvl)
    r = {}
    last_close = post["close"].iloc[-1]
    out = None
    for _, b in post.iterrows():
        if worse(adverse(b), stp):
            out = stp
            break
        if better(favour(b), tgt):
            out = tgt
            break
    r["BRACKET"] = s * ((out if out is not None else last_close) / entry - 1)
    ext = entry
    out = None
    for _, b in post.iterrows():
        trail = ext * (1 - s * TRAIL_PCT)
        if worse(adverse(b), trail):
            out = trail
            break
        ext = max(ext, favour(b)) if side == "long" else min(ext, favour(b))
    r["TRAIL"] = s * ((out if out is not None else last_close) / entry - 1)
    r["CLOSE"] = s * (last_close / entry - 1)
    out = None
    for _, b in post.iterrows():
        if worse(adverse(b), dstp):
            out = dstp
            break
    r["CLOSE_DS"] = s * ((out if out is not None else last_close) / entry - 1)
    r["_mfe"] = s * (post["high"].max() / entry - 1) if side == "long" else s * (post["low"].min() / entry - 1)
    r["_mae"] = s * (post["low"].min() / entry - 1) if side == "long" else s * (post["high"].max() / entry - 1)
    return r


def build_log(side):
    px = load_minutes(SYMBOL)
    sessions = sessionize(px)
    daily = daily_bars(sessions)
    daily["atr_pct"] = atr_pct_series(daily)
    daily["prev_close"] = daily["close"].shift(1)
    daily["prev_ret"] = daily["close"].pct_change().shift(1)          # yesterday's return, knowable at open
    dmap = daily.set_index("date").to_dict("index")
    rows = []
    for d in sorted(sessions):
        sess = sessions[d]
        if len(sess) < 60:
            continue
        o = sess["open"].iloc[0]
        trig = find_trigger(sess, side, o)
        if trig is None:
            continue
        i0, entry = trig
        sim = simulate(sess, i0, entry, o, side)
        if sim is None:
            continue
        meta = dmap.get(d, {})
        t = sess["tod"].iloc[i0]
        gap = o / meta["prev_close"] - 1 if meta.get("prev_close") else np.nan
        rows.append(dict(date=d, side=side, trig_time=t, entry=entry, open=o, atr_pct=meta.get("atr_pct", np.nan),
                         gap=gap, prev_ret=meta.get("prev_ret", np.nan), **{k: v for k, v in sim.items()}))
    log = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    if log.empty:
        return log

    def tbucket(t):
        if t < dtime(9, 45):
            return "1_0915-0945"
        if t < dtime(11, 0):
            return "2_0945-1100"
        if t < dtime(13, 0):
            return "3_1100-1300"
        return "4_1300-1500"
    log["trig_bucket"] = log["trig_time"].map(tbucket)
    n_learn = int(len(log) * OOS_SPLIT_FRAC)
    q = log["atr_pct"].iloc[:n_learn].quantile([1 / 3, 2 / 3]).values
    log["vol_regime"] = pd.cut(log["atr_pct"], [-np.inf, q[0], q[1], np.inf], labels=["1_quiet", "2_mid", "3_volatile"])
    qm = log["prev_ret"].iloc[:n_learn].quantile([1 / 3, 2 / 3]).values     # prev-day momentum terciles (LEARN only)
    log["mom_regime"] = pd.cut(log["prev_ret"], [-np.inf, qm[0], qm[1], np.inf], labels=["1_weak", "2_flat", "3_strong"])
    log["sample"] = np.where(np.arange(len(log)) < n_learn, "LEARN", "OOS")
    return log


EXITS = ["BRACKET", "TRAIL", "CLOSE", "CLOSE_DS"]


def boot_ci(x, n=N_BOOT, seed=SEED):
    if len(x) < 5:
        return (np.nan, np.nan)
    rng = np.random.default_rng(seed)
    m = rng.choice(x, size=(n, len(x)), replace=True).mean(axis=1)
    return tuple(np.percentile(m, [2.5, 97.5]))


def summarise(g, e):
    net = g[e].values * 1e4 - COST_BPS
    lo, hi = boot_ci(net)
    return dict(n=len(g), mean_bps=net.mean(), median_bps=np.median(net), win_pct=100 * (net > 0).mean(),
                ci_lo=lo, ci_hi=hi, tradeable=bool(lo > 0 and len(g) >= MIN_N_PER_CELL))


def report(log, side):
    print("\n" + "=" * 78)
    print(f"{SYMBOL} {side.upper()}  |  n={len(log)}  |  costs={COST_BPS:.0f} bps round trip (UNLEVERED; x5 MIS = x5 gross & cost)")
    print("=" * 78)
    print("\n--- HEADLINE: net expectancy by exit style (bps/trade) ---")
    hdr = f"{'exit':<10}{'sample':<8}{'n':>5}{'mean':>9}{'med':>8}{'win%':>7}{'95% CI':>20}  GO?"
    print(hdr)
    print("-" * len(hdr))
    for e in EXITS:
        for smp in ["LEARN", "OOS"]:
            g = log[log["sample"] == smp]
            if len(g) < 5:
                continue
            r = summarise(g, e)
            print(f"{e:<10}{smp:<8}{r['n']:>5}{r['mean_bps']:>9.1f}{r['median_bps']:>8.1f}{r['win_pct']:>7.1f}   [{r['ci_lo']:>6.1f},{r['ci_hi']:>6.1f}]  {'YES' if r['tradeable'] else '.'}")
    print("\n--- CONDITIONED (LEARN vs OOS; mom_regime = prev-day momentum selector) ---")
    for cond in ["mom_regime", "vol_regime", "trig_bucket"]:
        print(f"\n  [{cond}]")
        print(f"  {'cell':<16}{'exit':<10}{'LEARN n/bps':>16}{'OOS n/bps':>16}  GO?")
        for cell, _ in log.groupby(cond, observed=True):
            for e in EXITS:
                gl = log[(log[cond] == cell) & (log["sample"] == "LEARN")]
                go = log[(log[cond] == cell) & (log["sample"] == "OOS")]
                if len(go) < 5:
                    continue
                rl, ro = summarise(gl, e), summarise(go, e)
                flag = "YES" if (rl["mean_bps"] > 0 and ro["tradeable"]) else "."
                if flag == "." and ro["mean_bps"] < 0:
                    continue
                print(f"  {str(cell):<16}{e:<10}{rl['n']:>6}/{rl['mean_bps']:>8.1f}{ro['n']:>6}/{ro['mean_bps']:>8.1f}  {flag}")
    print("\n--- PATH DIAGNOSTICS ---")
    br = log["BRACKET"].values
    print(f"  P(target BEFORE stop), ordered  : {100 * (br > 0).mean():.1f}%")
    print(f"  Breakeven WR for this bracket   : {100 * TRIGGER_PCT / (TRIGGER_PCT + (TARGET_PCT - TRIGGER_PCT)):.1f}%")
    print(f"  Mean MFE {1e4 * log['_mfe'].mean():.1f} bps  ·  Mean MAE {1e4 * log['_mae'].mean():.1f} bps  ·  P90 MAE {1e4 * log['_mae'].quantile(0.10):.1f} bps")
    best = max(((e, summarise(log[log['sample'] == 'OOS'], e)) for e in EXITS), key=lambda kv: kv[1]["ci_lo"] if not np.isnan(kv[1]["ci_lo"]) else -9e9)
    e, r = best
    print(f"\n  VERDICT ({side}): best OOS exit = {e}, {r['mean_bps']:.1f} bps net, CI [{r['ci_lo']:.1f},{r['ci_hi']:.1f}] -> {'TRADEABLE' if r['tradeable'] else 'NOT TRADEABLE (CI includes 0)'}")


if __name__ == "__main__":
    for side in ["long", "short"]:
        log = build_log(side)
        if log.empty:
            print(f"no {side} trades")
            continue
        log.to_csv(os.path.expanduser("~") + f"/Downloads/{SYMBOL}_{side}_tradelog.csv", index=False)
        report(log, side)
