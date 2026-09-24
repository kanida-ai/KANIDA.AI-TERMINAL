"""Self-learning + INCUBATION layer for the 814 arena agents.
Walk-forward: every month, re-measure each agent on a trailing 6-month OOS window (realized lift vs base + expectancy),
then run a status state machine:  incubating -> active -> probation -> retired  (+ resurrection).
Only ACTIVE agents are in the live book; incubating/probation = paper. Produces a roster timeline + per-agent history.
Read-only sources; Falcon untouched. -> arena_metrics.db: agent_status_history, roster_timeline, agent_status_now"""
import os, sqlite3, json
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
AP = os.path.join(ROOT, "arena", "agent_population")
COST = 0.15
TARGETS = {"hit_10pc_20d": (10, 20), "hit_15pc_20d": (15, 20), "hit_25pc_30d": (25, 30), "hit_40pc_40d": (40, 40)}
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}
# --- state-machine params ---
W = 6                 # trailing months window
MIN_N = 8             # min fires in window to judge (else HOLD)
LIFT_OK = 3.0         # trailing lift (pp) to count as GOOD
PROMO = 3             # consecutive GOOD to graduate incubating->active
DEMOTE = 2            # consecutive BAD active->probation
RECOVER = 2           # consecutive GOOD probation->active
KILL = 3              # consecutive BAD in probation -> retired

uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2020-06-01'", uc)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,close FROM ohlc_daily WHERE trade_date>='2020-06-01' ORDER BY symbol,trade_date", uc)
tax = pd.read_sql_query("SELECT pattern_id,target,rule_json FROM falcon_pattern_taxonomy", uc); uc.close()
rc = sqlite3.connect("file:" + os.path.join(AP, "falcon_pattern_registry.db").replace("\\", "/") + "?mode=ro", uri=True)
agents = pd.read_sql_query("SELECT * FROM arena_agents", rc).merge(tax[["pattern_id", "rule_json"]], on="pattern_id"); rc.close()
cn = pd.read_sql_query("SELECT agent_id,codename FROM agent_summary", sqlite3.connect(os.path.join(AP, "arena_metrics.db")))
CN = dict(zip(cn.agent_id, cn.codename))

outs = []
for s, g in oh.groupby("symbol", sort=False):
    O = g.open.values; H = g.high.values; C = g.close.values; entry = np.roll(O, -1).astype(float); entry[-1] = np.nan
    d = {"symbol": s, "trade_date": g.trade_date.values}
    for tn, (pct, Hh) in TARGETS.items():
        fmax = pd.Series(H).rolling(Hh).max().shift(-Hh).values; cH = pd.Series(C).shift(-Hh).values
        hit = (fmax >= entry*(1+pct/100)); ret = np.where(hit, float(pct), (cH-entry)/entry*100) - COST
        bad = np.isnan(entry) | np.isnan(cH); hh = hit.astype(float); hh[bad] = np.nan; ret[bad] = np.nan
        d[f"hit_{tn}"] = hh; d[f"ret_{tn}"] = ret
    outs.append(pd.DataFrame(d))
M = feat.merge(pd.concat(outs, ignore_index=True), on=["symbol", "trade_date"], how="inner"); M["ym"] = M.trade_date.str[:7]
MONTHS = sorted(m for m in M.ym.unique() if m >= "2021-01")
cal_=pd.DataFrame({"d":sorted(M.trade_date.unique())}); _dt=pd.to_datetime(cal_.d)
cal_["wk"]=_dt.dt.isocalendar().year.astype(str)+"-"+_dt.dt.isocalendar().week.astype(str)
cal_["we"]=cal_.wk!=cal_.wk.shift(-1); WE_FLAG=dict(zip(cal_.d,cal_.we)); M["we"]=M.trade_date.map(WE_FLAG)
# base per (target, ym): hits + rows
BASE = {tn: M.groupby("ym")[f"hit_{tn}"].agg(bh="sum", bn="count") for tn in TARGETS}

# per-agent monthly (n, hits, sum_ret)
AM = {}
for _, a in agents.iterrows():
    m = np.ones(len(M), dtype=bool)
    try:
        for f, op, thr in json.loads(a.rule_json): m &= OPS[op](M[f].values, thr)
    except Exception: continue
    _wk = any(f.startswith("weekly_") for f,_,_ in json.loads(a.rule_json))
    sub = M.loc[m & (M.we.values if _wk else True), ["ym", f"hit_{a.target}", f"ret_{a.target}"]].dropna()
    if len(sub) < 20: continue
    g = sub.groupby("ym").agg(n=(f"hit_{a.target}", "count"), hits=(f"hit_{a.target}", "sum"), sret=(f"ret_{a.target}", "sum"))
    AM[a.agent_id] = (a.target, g)

# state machine
hist = []; roster = {m: {"active": 0, "incubating": 0, "probation": 0, "retired": 0} for m in MONTHS}
now = {}
for aid, (tn, g) in AM.items():
    status = "incubating"; good = bad = 0
    bh = BASE[tn]
    for i, ym in enumerate(MONTHS):
        wnd = MONTHS[max(0, i-W+1):i+1]
        gg = g.reindex(wnd).dropna(); bb = bh.reindex(wnd).dropna()
        n = gg.n.sum()
        if n >= MIN_N and bb.bn.sum() > 0:
            lift = (gg.hits.sum()/gg.n.sum() - bb.bh.sum()/bb.bn.sum())*100
            ret = gg.sret.sum()/gg.n.sum()
            form = "GOOD" if (lift > LIFT_OK and ret > 0) else "BAD"
        else:
            lift = ret = np.nan; form = "HOLD"
        if form == "GOOD": good += 1; bad = 0
        elif form == "BAD": bad += 1; good = 0
        # transitions
        if status == "incubating" and good >= PROMO: status = "active"; good = 0
        elif status == "active" and bad >= DEMOTE: status = "probation"; bad = 0
        elif status == "probation" and good >= RECOVER: status = "active"; good = 0
        elif status == "probation" and bad >= KILL: status = "retired"; bad = 0
        elif status == "retired" and good >= PROMO: status = "incubating"; good = 0
        roster[ym][status] += 1
        hist.append((aid, CN.get(aid, aid), ym, status, round(lift, 1) if lift == lift else None, round(ret, 2) if ret == ret else None, int(n), good))
    now[aid] = dict(agent_id=aid, codename=CN.get(aid, aid), status=status, good_streak=good, bad_streak=bad, trail_lift=lift, trail_ret=ret)

H = pd.DataFrame(hist, columns=["agent_id", "codename", "ym", "status", "trail_lift", "trail_ret", "trail_n", "good_streak"])
RT = pd.DataFrame([{"ym": m, **v} for m, v in roster.items()])
NOW = pd.DataFrame(now.values())
con = sqlite3.connect(os.path.join(AP, "arena_metrics.db"))
H.to_sql("agent_status_history", con, if_exists="replace", index=False)
RT.to_sql("roster_timeline", con, if_exists="replace", index=False)
NOW.to_sql("agent_status_now", con, if_exists="replace", index=False); con.close()

last = RT.iloc[-1]
print(f"Incubation engine run over {len(MONTHS)} months, {len(AM)} agents.")
print(f"\nROSTER NOW ({last.ym}): ACTIVE {int(last.active)} | incubating {int(last.incubating)} | probation {int(last.probation)} | retired {int(last.retired)}")
print(f"\nGraduation/transition rate (last 6 months): active count moved "
      f"{int(RT.iloc[-7].active)} -> {int(last.active)}")
print("\nIncubation bucket (top 8 closest to graduating, by good_streak then trailing lift):")
inc = NOW[NOW.status.isin(["incubating", "probation"])].sort_values(["good_streak", "trail_lift"], ascending=False).head(8)
for _, r in inc.iterrows():
    print(f"  {r.codename:<20} {r.status:<11} good_streak {int(r.good_streak)}/{PROMO} | trail_lift {r.trail_lift if r.trail_lift==r.trail_lift else 0:+.1f}pp")

# --- run manifest -------------------------------------------------------------
import sys as _sys, os as _os; _sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
from run_manifest import stamp as _stamp
_stamp(__file__, ["agent_status_history","roster_timeline","agent_status_now"], rows=len(NOW),
       oos="trailing 6-month walk-forward window (lifecycle, not a performance claim)")
