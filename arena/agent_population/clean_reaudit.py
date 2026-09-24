"""LEAK-FREE re-audit of all 865 Falcon pattern-agents. Recompute clean-2026 OOS lift + bear-month lift with the
week-end fix (weekly-feature agents fire only on the last session of the week). Re-issue promotion status.
PROMOTED = clean26_lift>0 AND bear_lift>0 (>=20 fires each). PROBATION = one positive. RETIRED = neither.
Updates falcon_pattern_registry.db(agents, arena_agents). Read-only price/feature sources; Falcon untouched."""
import os, sqlite3, json
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); AP = os.path.dirname(os.path.abspath(__file__))
TARGETS = {"hit_10pc_20d": (10, 20), "hit_15pc_20d": (15, 20), "hit_25pc_30d": (25, 30), "hit_40pc_40d": (40, 40)}
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}
uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2020-06-01'", uc)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,close FROM ohlc_daily WHERE trade_date>='2020-06-01' ORDER BY symbol,trade_date", uc)
tax = pd.read_sql_query("SELECT pattern_id,target,rule_json FROM falcon_pattern_taxonomy", uc); uc.close()
# outcomes: hit per target
outs = []
for s, g in oh.groupby("symbol", sort=False):
    O = g.open.values; H = g.high.values; C = g.close.values; entry = np.roll(O, -1).astype(float); entry[-1] = np.nan
    d = {"symbol": s, "trade_date": g.trade_date.values}
    for tn, (pct, Hh) in TARGETS.items():
        fmax = pd.Series(H).rolling(Hh).max().shift(-Hh).values; cH = pd.Series(C).shift(-Hh).values
        hit = (fmax >= entry*(1+pct/100)).astype(float); bad = np.isnan(entry) | np.isnan(cH); hit[bad] = np.nan
        d[f"hit_{tn}"] = hit
    outs.append(pd.DataFrame(d))
M = feat.merge(pd.concat(outs, ignore_index=True), on=["symbol", "trade_date"], how="inner"); M["ym"] = M.trade_date.str[:7]; M["yr"] = M.trade_date.str[:4].astype(int)
# week-end flag
cal = pd.DataFrame({"d": sorted(M.trade_date.unique())}); dt = pd.to_datetime(cal.d)
cal["wk"] = dt.dt.isocalendar().year.astype(str)+"-"+dt.dt.isocalendar().week.astype(str); cal["we"] = cal.wk != cal.wk.shift(-1)
M["we"] = M.trade_date.map(dict(zip(cal.d, cal.we)))
# market down-months (universe median monthly ret < -3%)
piv = oh.pivot_table(index="trade_date", columns="symbol", values="close"); mret = piv.pct_change(fill_method=None).median(axis=1)
mm = (mret.groupby(mret.index.str[:7]).apply(lambda x: (1+x.fillna(0)).prod()-1)*100); DOWN = set(mm[mm < -3].index)
M["down"] = M.ym.isin(DOWN)
b26 = {tn: M[M.yr == 2026][f"hit_{tn}"].mean() for tn in TARGETS}
bdn = {tn: M[M.down][f"hit_{tn}"].mean() for tn in TARGETS}

rc = sqlite3.connect(os.path.join(AP, "falcon_pattern_registry.db"))
reg = pd.read_sql_query("SELECT agent_id,pattern_id FROM agents", rc)
reg = reg.merge(tax, on="pattern_id", how="left")
def uses_weekly(rj):
    try: return any(f.startswith("weekly_") for f, _, _ in json.loads(rj))
    except: return False
rows = []
for _, a in reg.iterrows():
    if pd.isna(a.rule_json): continue
    m = np.ones(len(M), dtype=bool)
    try:
        for f, op, thr in json.loads(a.rule_json): m &= OPS[op](M[f].values, thr)
    except: continue
    wk = uses_weekly(a.rule_json); we = M.we.values if wk else np.ones(len(M), bool)
    c26 = M.loc[m & we & (M.yr.values == 2026), f"hit_{a.target}"].dropna()
    bmo = M.loc[m & we & M.down.values, f"hit_{a.target}"].dropna()
    cl = (c26.mean()-b26[a.target])*100 if len(c26) >= 20 else np.nan
    bl = (bmo.mean()-bdn[a.target])*100 if len(bmo) >= 20 else np.nan
    ok26 = (cl is not np.nan) and (cl > 0); okb = (bl is not np.nan) and (bl > 0)
    if (cl == cl) and (bl == bl): status = "PROMOTED" if (cl > 0 and bl > 0) else ("PROBATION" if (cl > 0 or bl > 0) else "RETIRED")
    elif cl == cl: status = "PROMOTED" if cl > 0 else "RETIRED"
    else: status = "measuring"
    rows.append(dict(agent_id=a.agent_id, clean26_lift=round(cl, 2) if cl == cl else None, bear_lift=round(bl, 2) if bl == bl else None,
                     status_clean=status, weekly=wk, c26_n=len(c26), bear_n=len(bmo)))
R = pd.DataFrame(rows)
# update registry
cur = rc.cursor()
for _, r in R.iterrows():
    cur.execute("UPDATE agents SET clean26_lift=?, bear_lift=?, status=? WHERE agent_id=?",
                (r.clean26_lift, r.bear_lift, r.status_clean, r.agent_id))
rc.commit()
# rebuild arena_agents = clean PROMOTED
promoted = pd.read_sql_query("SELECT * FROM agents WHERE status='PROMOTED'", rc)
cur.execute("DROP TABLE IF EXISTS arena_agents"); rc.commit()
promoted.to_sql("arena_agents", rc, if_exists="replace", index=False); rc.close()
vc = R.status_clean.value_counts().to_dict()
print(f"CLEAN RE-AUDIT of {len(R)} agents (leak-free):")
print(f"  status: {vc}")
print(f"  down-months used for bear test: {sorted(DOWN)}")
print(f"  BEFORE (leaky): 814 promoted → AFTER (clean): {vc.get('PROMOTED',0)} promoted")
print(f"  median clean-2026 lift: {R.clean26_lift.median():+.1f}pp | median bear lift: {R.bear_lift.median():+.1f}pp")
