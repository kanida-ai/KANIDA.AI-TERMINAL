"""Falcon V7.1.0 — 865 patterns as individual agents. Walk-forward OOS backtest.
Each pattern = 1 agent. Fires = stock-days where its rule_json matches falcon_features (READ-ONLY). OOS = fire-year > mined_year.
Target-hit swing: buy next open; WIN if High reaches +X% within Y days (target hit_Xpc_Yd), else exit at Y-day close. Rs5L account.
Reports per-pattern: OOS fires, realized hit%, base%, realized lift (vs mined lift), avg return, Rs5L CAGR. Read-only; Falcon untouched."""
import os, sqlite3, json, re
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
CAP = 5e5; COST = 0.15
TARGETS = {"hit_10pc_20d": (10, 20), "hit_15pc_20d": (15, 20), "hit_25pc_30d": (25, 30), "hit_40pc_40d": (40, 40)}
OPS = {">": lambda a, b: a > b, ">=": lambda a, b: a >= b, "<": lambda a, b: a < b, "<=": lambda a, b: a <= b, "==": lambda a, b: a == b}

uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2018-01-01'", uc)
pat = pd.read_sql_query("SELECT pattern_id,mined_year,target,regime,lift_pp,base_rate_pct,oos_hit_rate,rule_json,english FROM falcon_pattern_taxonomy", uc)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,close FROM ohlc_daily WHERE trade_date>='2018-01-01'", uc)
uc.close()
print(f"features {len(feat):,} | patterns {len(pat)} | ohlc {len(oh):,}", flush=True)

# ---- precompute target outcomes per (symbol,date): hit + realized return for each of the 4 targets ----
oh = oh.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
outs = []
for sym, g in oh.groupby("symbol", sort=False):
    O = g.open.values; H = g.high.values; C = g.close.values
    entry = np.roll(O, -1).astype(float); entry[-1] = np.nan            # next-bar open
    d = {"symbol": sym, "trade_date": g.trade_date.values}
    for tname, (pct, Hh) in TARGETS.items():
        fmax = pd.Series(H).rolling(Hh).max().shift(-Hh).values          # max high over [p+1..p+H]
        cH = pd.Series(C).shift(-Hh).values                             # close at p+H
        hit = (fmax >= entry * (1 + pct / 100)).astype(float)
        ret = np.where(hit > 0, float(pct), (cH - entry) / entry * 100) - COST
        bad = np.isnan(entry) | np.isnan(cH)                            # incomplete horizon -> drop
        hit[bad] = np.nan; ret[bad] = np.nan
        d[f"hit_{tname}"] = hit; d[f"ret_{tname}"] = ret
    outs.append(pd.DataFrame(d))
O = pd.concat(outs, ignore_index=True)
M = feat.merge(O, on=["symbol", "trade_date"], how="inner")
M["yr"] = M.trade_date.str[:4].astype(int)
print(f"merged rows {len(M):,}", flush=True)

# base hit/ret per (target, min_year) over ALL rows in the OOS window
BASE = {}
for tname in TARGETS:
    for my in [2022, 2023, 2024, 2025]:
        sub = M[M.yr > my]
        BASE[(tname, my)] = (sub[f"hit_{tname}"].mean(), sub[f"ret_{tname}"].mean())
BASE26 = {tname: M[M.yr == 2026][f"hit_{tname}"].mean() for tname in TARGETS}   # clean OOS base (post-promotion)
# BEAR-REGIME stress: months where universe median monthly return < -3% (market correction)
mlast = oh.assign(ym=oh.trade_date.str[:7]).groupby(["symbol", "ym"]).close.last().reset_index()
mlast["mret"] = mlast.groupby("symbol").close.pct_change() * 100
BEAR_MONTHS = set(mlast.groupby("ym").mret.median().pipe(lambda s: s[s < -3].index))
M["bear"] = M.trade_date.str[:7].isin(BEAR_MONTHS)
BEARBASE = {tname: M[M.bear][f"hit_{tname}"].mean() for tname in TARGETS}
print(f"bear (correction) months: {len(BEAR_MONTHS)}  e.g. {sorted(BEAR_MONTHS)[-5:]}", flush=True)

def evalmask(rj):
    m = np.ones(len(M), dtype=bool)
    for f, op, thr in json.loads(rj):
        if f not in M.columns: return None
        m &= OPS[op](M[f].values, thr)
    return m

rows = []
for _, p in pat.iterrows():
    tname = p.target
    if tname not in TARGETS: continue
    mask = evalmask(p.rule_json)
    if mask is None: continue
    oos = mask & (M.yr.values > p.mined_year)
    n = int(oos.sum())
    if n == 0: continue
    hit = M.loc[oos, f"hit_{tname}"]; ret = M.loc[oos, f"ret_{tname}"]
    bh, br = BASE[(tname, p.mined_year)]
    Hh = TARGETS[tname][1]
    # Rs5L account: ONE position at a time — skip fires within `Hh` trading-days (~Hh*1.5 cal) of the last entry
    sub = M.loc[oos, ["trade_date", f"ret_{tname}"]].dropna().sort_values("trade_date")
    eq = CAP; peak = CAP; mdd = 0.0; taken = 0; free = None
    for dstr, r in zip(sub.trade_date.values, sub[f"ret_{tname}"].values):
        if free is not None and dstr < free: continue
        eq *= (1 + r / 100); eq = max(eq, 1.0)
        peak = max(peak, eq); mdd = max(mdd, (peak - eq) / peak); taken += 1
        free = (pd.Timestamp(dstr) + pd.Timedelta(days=int(Hh * 1.5))).strftime("%Y-%m-%d")
    span = max(1, (M.loc[oos, "yr"].max() - M.loc[oos, "yr"].min() + 1))
    # STRICT clean-OOS: 2026 only (post-promotion for all patterns)
    strict = mask & (M.yr.values == 2026); sf = int(strict.sum())
    strict_lift = (M.loc[strict, f"hit_{tname}"].mean() - BASE26[tname]) * 100 if sf > 0 else np.nan
    bmask = mask & M.bear.values; bf = int(bmask.sum())
    bear_lift = (M.loc[bmask, f"hit_{tname}"].mean() - BEARBASE[tname]) * 100 if bf > 0 else np.nan
    rows.append(dict(pattern_id=p.pattern_id, target=tname, regime=p.regime, mined_year=p.mined_year,
                     oos_fires=n, acct_trades=taken, hit_pct=hit.mean() * 100, base_pct=bh * 100, real_lift_pp=(hit.mean() - bh) * 100,
                     mined_lift_pp=p.lift_pp, avg_ret=ret.mean(), base_ret=br, edge_ret=ret.mean() - br,
                     strict26_fires=sf, strict26_lift=strict_lift, bear_fires=bf, bear_lift=bear_lift,
                     roc5L=eq / CAP * 100 - 100, cagr5L=((eq / CAP) ** (1 / span) - 1) * 100 if eq > 0 else np.nan,
                     maxdd=mdd * 100, english=p.english))
R = pd.DataFrame(rows)
R.to_csv(os.path.join(ROOT, "arena", "agent_population", "falcon_pattern_agents.csv"), index=False)
print(f"\nEvaluated {len(R)} pattern-agents with OOS fires.\n" + "=" * 96)
print(f"POPULATION: {(R.real_lift_pp>0).mean()*100:.0f}% have POSITIVE realized OOS lift | "
      f"median realized lift {R.real_lift_pp.median():+.2f}pp (mined median {R.mined_lift_pp.median():+.2f}pp) | "
      f"median edge_ret {R.edge_ret.median():+.2f}%")
rob = R[R.oos_fires >= 30]
print(f"\nrobust (>=30 OOS fires): {len(rob)} patterns | {(rob.real_lift_pp>0).mean()*100:.0f}% positive lift")
print("\nTOP 15 pattern-agents by realized OOS lift (>=30 fires):")
print(rob.sort_values("real_lift_pp", ascending=False).head(15)[["pattern_id","target","regime","oos_fires","hit_pct","base_pct","real_lift_pp","mined_lift_pp","avg_ret","cagr5L"]].round(2).to_string(index=False))
print("\nBY TARGET (median realized lift, % positive):")
print(R.groupby("target").agg(n=("pattern_id","count"), med_real_lift=("real_lift_pp","median"), med_mined=("mined_lift_pp","median"), pct_pos=("real_lift_pp", lambda x:(x>0).mean()*100)).round(2).to_string())
print("\nMINED vs REALIZED lift (year>mined_year): corr =", round(R[["mined_lift_pp","real_lift_pp"]].corr().iloc[0,1],3))
st = R[R.strict26_fires >= 20]
print("\n" + "="*96 + "\nSTRICT CLEAN-OOS — 2026 ONLY (post-promotion; the honest test)\n" + "="*96)
print(f"patterns with >=20 fires in 2026: {len(st)}")
print(f"  % with POSITIVE 2026 lift: {(st.strict26_lift>0).mean()*100:.0f}%   median 2026 lift: {st.strict26_lift.median():+.2f}pp")
print(f"  (vs 'year>mined' median lift {R.real_lift_pp.median():+.2f}pp — the gap = selection-bias inflation)")
print(f"  corr(mined_lift, 2026_lift): {st[['mined_lift_pp','strict26_lift']].corr().iloc[0,1]:.3f}")
be = R[R.bear_fires >= 15]
print("\n" + "="*96 + "\nBEAR-REGIME STRESS — fires during market-correction months only (the test 2026 can't give)\n" + "="*96)
print(f"patterns with >=15 bear-month fires: {len(be)}")
print(f"  % with POSITIVE bear lift: {(be.bear_lift>0).mean()*100:.0f}%   median bear lift: {be.bear_lift.median():+.2f}pp")
print(f"  % robust BOTH clean-2026 AND bear positive: {((R.strict26_lift>0)&(R.bear_lift>0)).mean()*100:.0f}%")
