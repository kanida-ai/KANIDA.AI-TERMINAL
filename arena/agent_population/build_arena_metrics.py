"""Build LEAKAGE-FREE trader metrics + monthly tables for the 814 agents.
FIX 1 (no lookahead): agents whose rule uses any weekly_* feature may fire ONLY on the last session of the week
  (feature is complete then; entry = next open). Daily-only agents fire any day.
FIX 2 (honest metrics): lead with the Rs5L one-position-at-a-time ACCOUNT (real CAGR/MaxDD/final Rs) + per-trade
  stats (win%, avg win/loss, PF). Monthly = AVG return per trade that month (NOT a sum).
Window OOS 2025-01..2026-07. -> arena_metrics.db (agent_summary, agent_monthly). Read-only sources; Falcon untouched."""
import os, sqlite3, json
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); AP = os.path.dirname(os.path.abspath(__file__))
CAP = 5e5; COST = 0.15
TARGETS = {"hit_10pc_20d": (10, 20), "hit_15pc_20d": (15, 20), "hit_25pc_30d": (25, 30), "hit_40pc_40d": (40, 40)}
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}
uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01'", uc)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,close FROM ohlc_daily WHERE trade_date>='2024-06-01' ORDER BY symbol,trade_date", uc)
tax = pd.read_sql_query("SELECT pattern_id,target,regime,mined_year,rule_json,english FROM falcon_pattern_taxonomy", uc); uc.close()
rc = sqlite3.connect(os.path.join(AP, "falcon_pattern_registry.db"))
_aa = pd.read_sql_query("SELECT * FROM arena_agents", rc); rc.close()
_aa = _aa.drop(columns=[c for c in ["mined_year","rule_json","english"] if c in _aa.columns])
agents = _aa.merge(tax[["pattern_id", "mined_year", "rule_json", "english"]], on="pattern_id")

# outcomes: entry next open, hit if High reaches +X% within Y days else exit at Y-day close; net of cost
outs = []
for s, g in oh.groupby("symbol", sort=False):
    O = g.open.values; H = g.high.values; C = g.close.values; entry = np.roll(O, -1).astype(float); entry[-1] = np.nan
    d = {"symbol": s, "trade_date": g.trade_date.values}
    for tn, (pct, Hh) in TARGETS.items():
        fmax = pd.Series(H).rolling(Hh).max().shift(-Hh).values; cH = pd.Series(C).shift(-Hh).values
        hit = fmax >= entry*(1+pct/100); ret = np.where(hit, float(pct), (cH-entry)/entry*100) - COST
        bad = np.isnan(entry) | np.isnan(cH); ret[bad] = np.nan; d[f"ret_{tn}"] = ret
    outs.append(pd.DataFrame(d))
M = feat.merge(pd.concat(outs, ignore_index=True), on=["symbol", "trade_date"], how="inner").reset_index(drop=True)
M["ym"] = M.trade_date.str[:7]; M["yr"] = M.trade_date.str[:4].astype(int)
# week-end flag (last trading session of each ISO week) — for the lookahead fix
cal = pd.DataFrame({"trade_date": sorted(M.trade_date.unique())}); dtt = pd.to_datetime(cal.trade_date)
cal["wk"] = dtt.dt.isocalendar().year.astype(str) + "-" + dtt.dt.isocalendar().week.astype(str)
cal["we"] = cal.wk != cal.wk.shift(-1); WE = dict(zip(cal.trade_date, cal.we)); M["we"] = M.trade_date.map(WE)
WIN = (M.trade_date >= "2025-01-01") & (M.trade_date <= "2026-07-31")
def uses_weekly(rj): return any(f.startswith("weekly_") for f, _, _ in json.loads(rj))

summ = []; monthly = []
for _, a in agents.iterrows():
    m = np.ones(len(M), dtype=bool)
    try:
        for f, op, thr in json.loads(a.rule_json): m &= OPS[op](M[f].values, thr)
    except Exception: continue
    wk = uses_weekly(a.rule_json)
    oos = M.yr.values > int(a.mined_year)                               # TRUE OOS: after this pattern's mining year
    idx = m & WIN.values & oos & (M.we.values if wk else True)          # LOOKAHEAD FIX + true-OOS
    sub = M.loc[idx, ["trade_date", "ym", f"ret_{a.target}"]].rename(columns={f"ret_{a.target}": "r"}).dropna().sort_values("trade_date")
    if len(sub) < 8: continue
    r = sub.r.values; wins = r[r > 0]; losses = r[r <= 0]
    pf = wins.sum() / -losses.sum() if losses.sum() < 0 else 99.0
    mos = sub.ym.nunique(); yrs = max(mos / 12, 0.5); Hh = TARGETS[a.target][1]
    # Rs5L account: one position at a time (cooldown = horizon), compounding
    eq = CAP; peak = CAP; mdd = 0.0; free = None; taken = 0
    for dt, rr in zip(sub.trade_date.values, r):
        if free is not None and dt < free: continue
        eq = max(eq*(1+rr/100), 1.0); peak = max(peak, eq); mdd = max(mdd, (peak-eq)/peak); taken += 1
        free = (pd.Timestamp(dt)+pd.Timedelta(days=int(Hh*1.5))).strftime("%Y-%m-%d")
    cagr = ((eq/CAP)**(1/yrs)-1)*100 if eq > 0 else np.nan
    summ.append(dict(agent_id=a.agent_id, uses_weekly=wk, target=a.target, regime=a.regime,
        trades=len(r), win_pct=(r > 0).mean()*100, avg_win=wins.mean() if len(wins) else 0, avg_loss=losses.mean() if len(losses) else 0,
        profit_factor=round(pf, 2), avg_ret=r.mean(), final_5L_comp=eq, cagr_comp=cagr, maxdd=mdd*100, acct_trades=taken,
        clean26_lift=a.clean26_lift, bear_lift=a.bear_lift, english=a.english))
    mo = sub.groupby("ym").r.agg(trades="count", wins=lambda x: int((x > 0).sum()), losses=lambda x: int((x <= 0).sum()),
                                 avg_win=lambda x: x[x > 0].mean(), avg_loss=lambda x: x[x <= 0].mean(), avg_ret="mean").reset_index()
    mo["agent_id"] = a.agent_id; monthly.append(mo)

S = pd.DataFrame(summ); MO = pd.concat(monthly, ignore_index=True)
con = sqlite3.connect(os.path.join(AP, "arena_metrics.db"))
try:  # PRESERVE identity set by name_agents / reconciliation; recompute verified from the new numbers
    _old = pd.read_sql_query("SELECT agent_id,codename,vibe,promo_status FROM agent_summary", con)
    S = S.merge(_old, on="agent_id", how="left")
except Exception: pass
S["verified"] = ((S.profit_factor > 1.5) & (S.avg_ret > 0)).astype(int)
S.to_sql("agent_summary", con, if_exists="replace", index=False); MO.to_sql("agent_monthly", con, if_exists="replace", index=False); con.close()
print(f"CLEAN metrics built: {len(S)} agents ({int(S.uses_weekly.sum())} weekly-restricted, {int((~S.uses_weekly).sum())} daily)")
print(f"  medians -> win {S.win_pct.median():.0f}% | PF {S.profit_factor.median():.2f} | avg/trade {S.avg_ret.median():+.2f}% | CAGR {S.cagr_comp.median():+.0f}% | maxDD {S.maxdd.median():.0f}%")
print(f"  positive-expectancy agents (avg_ret>0): {(S.avg_ret>0).sum()} of {len(S)}")
print("\n  top 6 by CAGR (honest):")
for _, r in S.sort_values("cagr_comp", ascending=False).head(6).iterrows():
    print(f"    {r.agent_id:<14} CAGR {r.cagr_comp:+.0f}% · DD {r.maxdd:.0f}% · win {r.win_pct:.0f}% · PF {r.profit_factor:.1f} · avg/tr {r.avg_ret:+.1f}% · {int(r.trades)} tr")

# --- run manifest -------------------------------------------------------------
import sys as _sys, os as _os; _sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
from run_manifest import stamp as _stamp
_stamp(__file__, ["agent_summary","agent_monthly"], rows=len(S))
