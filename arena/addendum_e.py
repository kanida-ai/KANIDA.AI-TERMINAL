"""ADDENDUM E — Signal-Calibration Research Agents (deployed).
Calibrate WHICH Falcon signal ATTRIBUTES carry out-of-sample edge, via the shared E2 protocol:
  LEARN (2022-2024, monthly) -> classify attribute-buckets profitable (min sample + majority-of-months)
  VALIDATE (2025, unseen) -> trade only the calibrated library
  FINAL OOS (2026 Jan-Jun, touched ONCE) -> true generalisation.
Runs on the pre-computed falcon_signal_day_study (each Top-10 signal + metadata + full next-day returns).
Agents 1-4 differ only in TARGET/DIRECTION/UNIVERSE. Ranks the RESEARCH DIRECTIONS by OOS edge. READ-ONLY."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
LEV, MIN_N, MAJ = 5, 40, 0.60
oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
S = pd.read_sql_query("SELECT signal_date, symbol, engine_rank rank, avg_lift, n_fires, sector, "
                      "d1_open_ret, d1_close_ret, d1_high_ret, d1_low_ret, d5_close_ret, kept_running_d30, big_winner_flag "
                      "FROM falcon_signal_day_study WHERE d1_close_ret IS NOT NULL", oc)
C = pd.read_sql_query("SELECT signal_date, symbol, entry_context FROM falcon_signal_day_context", oc)
# market regime = breadth on the signal day (fraction of universe up), point-in-time
dd = pd.read_sql_query("SELECT trade_date, symbol, close FROM ohlc_daily WHERE trade_date>='2021-01-01'", oc); oc.close()
piv = dd.pivot_table(index="trade_date", columns="symbol", values="close"); ret = piv.pct_change()
breadth = (ret > 0).sum(axis=1) / ret.notna().sum(axis=1)
S = S.merge(C, on=["signal_date", "symbol"], how="left")
S["entry_context"] = S.entry_context.fillna("NORMAL")
S["breadth"] = S.signal_date.map(breadth); S["regime"] = pd.cut(S.breadth, [0, 0.45, 0.55, 1.01], labels=["down", "neutral", "up"])
S["yr"] = S.signal_date.str[:4]; S["month"] = S.signal_date.str[:7]
S["rank_band"] = pd.cut(S["rank"], [0, 2, 5, 10], labels=["r1-2", "r3-5", "r6-10"])
S["lift_band"] = pd.qcut(S.avg_lift, 3, labels=["lift-lo", "lift-mid", "lift-hi"])
S["nfires_band"] = pd.qcut(S.n_fires.rank(method="first"), 3, labels=["fires-lo", "fires-mid", "fires-hi"])
ATTRS = ["rank_band", "lift_band", "nfires_band", "entry_context", "regime"]
LEARN = ["2022", "2023", "2024"]; VAL = "2025"; OOS = "2026"

def calibrate(df, tgt_col, direction):
    d = df.copy(); d["tgt"] = direction * d[tgt_col]
    learn = d[d.yr.isin(LEARN)]
    best = None
    for attr in ATTRS:                                            # single-attribute cuts (E2.2: start simple)
        lib = []
        for b, g in learn.groupby(attr, observed=True):
            if len(g) < MIN_N: continue
            mm = g.groupby("month").tgt.mean()
            if (mm > 0).mean() >= MAJ and g.tgt.mean() > 0: lib.append(b)
        if not lib: continue
        val = d[(d.yr == VAL) & (d[attr].isin(lib))]
        vedge = val.tgt.mean() if len(val) else -9
        if best is None or vedge > best["vedge"]: best = dict(attr=attr, lib=lib, vedge=vedge)
    return d, best

def perf(sub):
    if not len(sub): return dict(edge=np.nan, net=0, dd=0, n=0, perday=0)
    daily = sub.groupby("signal_date").tgt.mean() * LEV                 # daily 5x portfolio return (%), equal-weight
    cum = daily.cumsum(); dd = float((cum.cummax() - cum).max())
    return dict(edge=sub.tgt.mean(), net=float(daily.mean() * 21), dd=dd, n=len(sub), perday=len(sub) / sub.signal_date.nunique())

AGENTS = {
    "1-Short(fade)":    ("d1_close_ret", -1),   # short next-day intraday; profitable where signals FADE
    "2-RankMap(long)":  ("d1_close_ret", +1),   # long next-day intraday; where continuation lives
    "3-Continuation":   ("d5_close_ret", +1),   # 5-day follow-through, long
    "4-Reversal(fade)": ("d1_close_ret", -1),   # fade — restricted to extended/gap contexts below
}
print("=" * 104)
print("ADDENDUM E — RESEARCH LEADERBOARD  (learn 2022-24 -> validate 2025 -> OOS 2026 touched once)  edge/trade at 1x, net at 5x")
print("=" * 104)
print(f"{'agent':<18}{'best attr':<14}{'VAL_edge%':>9}{'OOS_edge%':>10}{'ALLsig_OOS%':>12}{'calib_adds':>11}{'OOSnet/mo%':>11}{'perday':>7}{'verdict':>8}")
rows = []
for name, (tgt, dirn) in AGENTS.items():
    df = S.copy()
    if name.startswith("4"): df = df[df.entry_context.isin(["GAP_UP_ENTRY", "EXTENDED_MOVE", "SIGNAL_DAY_CIRCUIT", "HIGH_VOL_CONFIRM"])]  # failed-break population
    d, best = calibrate(df, tgt, dirn)
    base = perf(d[d.yr == OOS])                                          # ALL signals, no calibration (the baseline)
    if not best:
        print(f"{name:<18}{'-- no profitable bucket (cannot calibrate a book here)':<56}{base['edge']:>+7.2f} all-sig"); continue
    oos = perf(d[(d.yr == OOS) & (d[best['attr']].isin(best['lib']))])
    val = perf(d[(d.yr == VAL) & (d[best['attr']].isin(best['lib']))])
    up = d[(d.yr == OOS) & (d[best['attr']].isin(best['lib'])) & (d.regime == "up")].tgt.mean()
    dn = d[(d.yr == OOS) & (d[best['attr']].isin(best['lib'])) & (d.regime == "down")].tgt.mean()
    adds = oos["edge"] - base["edge"]                                    # does the calibrated subset beat trading everything?
    verdict = "PURSUE" if (oos["edge"] > 0 and val["edge"] > 0) else "park"
    print(f"{name:<18}{best['attr']:<14}{val['edge']:>+9.2f}{oos['edge']:>+10.2f}{base['edge']:>+12.2f}{adds:>+11.2f}{oos['net']:>+11.0f}{oos['perday']:>7.1f}{verdict:>8}")
    print(f"                  library = {{{','.join(str(x) for x in best['lib'])}}}")
    rows.append((name, best, val, oos, up, dn))
print("\nREGIME SPLIT (OOS 2026 edge/trade, up-breadth vs down-breadth):")
for name, best, val, oos, up, dn in rows:
    print(f"  {name:<18} up-breadth {up:>+6.2f}%   down-breadth {dn:>+6.2f}%   ({'regime-robust' if (up>0 and dn>0) else 'one-sided'})")
print("\n[per-rank edge, OOS 2026, long d1_close_ret — Agent 2's rank-profitability curve]:")
for rb, g in S[S.yr == OOS].groupby("rank_band", observed=True):
    print(f"  {str(rb):<7} long-edge {g.d1_close_ret.mean():>+6.2f}%/trade   n={len(g)}")
print("\nAgent 5 (Accumulation) = ape engine already calibrated: footprint finds movers but beta-neutral ~0; sweep+hold 1.3x lift (see ape_*).")
print("Agent 6 (Zone Engine, Addendum D, no Falcon) = NOT yet deployed — separate per-stock 1-min zone/expiry build.")
