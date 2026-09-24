"""Per-UNIVERSE agent metrics — 'which agents work best on Nifty 50 / F&O (Nifty 200) / Nifty 500'.
Recomputes each agent's leak-free trades (same model as build_arena_metrics.py) restricted to each index universe,
so the console can re-rank + re-apply the verified gate per universe. NON-DESTRUCTIVE: writes ONLY agent_universe
+ universe_symbols; never rewrites agent_summary (codenames/verified stay intact). Read-only sources; Falcon untouched."""
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
tax = pd.read_sql_query("SELECT pattern_id,target,mined_year,rule_json FROM falcon_pattern_taxonomy", uc)
um = pd.read_sql_query("SELECT symbol,in_nifty50,in_nifty200,in_nifty500 FROM universe_master WHERE is_active=1", uc); uc.close()
rc = sqlite3.connect(os.path.join(AP, "falcon_pattern_registry.db"))
_aa = pd.read_sql_query("SELECT * FROM arena_agents", rc); rc.close()
_aa = _aa.drop(columns=[c for c in ["mined_year","rule_json"] if c in _aa.columns])
agents = _aa.merge(tax[["pattern_id", "mined_year", "rule_json"]], on="pattern_id")

# universes: F&O ≈ Nifty 200 (per spec). ALL is agent_summary itself, not recomputed here.
UNIS = {"N50": set(um[um.in_nifty50 == 1].symbol), "FO": set(um[um.in_nifty200 == 1].symbol), "N500": set(um[um.in_nifty500 == 1].symbol)}

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
cal = pd.DataFrame({"trade_date": sorted(M.trade_date.unique())}); dtt = pd.to_datetime(cal.trade_date)
cal["wk"] = dtt.dt.isocalendar().year.astype(str) + "-" + dtt.dt.isocalendar().week.astype(str)
cal["we"] = cal.wk != cal.wk.shift(-1); WE = dict(zip(cal.trade_date, cal.we)); M["we"] = M.trade_date.map(WE)
WIN = (M.trade_date >= "2025-01-01") & (M.trade_date <= "2026-07-31")
WINYRS = M.loc[WIN, "ym"].nunique() / 12.0   # annualize over the full committed window, not active-months (bursty-safe)
def uses_weekly(rj): return any(f.startswith("weekly_") for f, _, _ in json.loads(rj))

def _grade(cagr, mdd, pf):
    if cagr > 80 and mdd < 20 and pf > 3: return "S"
    if cagr > 40 and pf > 2: return "A"
    if cagr > 15: return "B"
    return "C"
def _stats(sub, Hh):
    r = sub.r.values; wins = r[r > 0]; losses = r[r <= 0]
    pf = wins.sum()/-losses.sum() if losses.sum() < 0 else 99.0
    eq = CAP; peak = CAP; mdd = 0.0; free = None
    for dt, rr in zip(sub.trade_date.values, r):
        if free is not None and dt < free: continue
        eq = max(eq*(1+rr/100), 1.0); peak = max(peak, eq); mdd = max(mdd, (peak-eq)/peak)
        free = (pd.Timestamp(dt)+pd.Timedelta(days=int(Hh*1.5))).strftime("%Y-%m-%d")
    cagr = ((eq/CAP)**(1/WINYRS)-1)*100 if eq > 0 else np.nan
    return dict(trades=len(r), win_pct=(r > 0).mean()*100, avg_win=wins.mean() if len(wins) else 0.0,
        avg_loss=losses.mean() if len(losses) else 0.0, profit_factor=round(float(pf), 2), avg_ret=float(r.mean()),
        cagr_comp=float(cagr), maxdd=float(mdd*100), final_5L_comp=float(eq),
        verified=int(pf > 1.5 and r.mean() > 0), grade=_grade(cagr, mdd*100, pf))

univ = []
for _, a in agents.iterrows():
    m = np.ones(len(M), dtype=bool)
    try:
        for f, op, thr in json.loads(a.rule_json): m &= OPS[op](M[f].values, thr)
    except Exception: continue
    wk = uses_weekly(a.rule_json)
    oos = M.yr.values > int(a.mined_year)                    # TRUE OOS: after this pattern's mining year
    idx = m & WIN.values & oos & (M.we.values if wk else True)
    sub = M.loc[idx, ["trade_date", "ym", "symbol", f"ret_{a.target}"]].rename(columns={f"ret_{a.target}": "r"}).dropna().sort_values("trade_date")
    if len(sub) < 8: continue
    Hh = TARGETS[a.target][1]
    for uk, uset in UNIS.items():
        us = sub[sub.symbol.isin(uset)]
        if len(us) < 8: continue                       # too few trades on this universe to judge
        univ.append(dict(agent_id=a.agent_id, universe=uk, **_stats(us, Hh)))

AU = pd.DataFrame(univ)
USYM = um.rename(columns={"in_nifty50": "n50", "in_nifty200": "fo", "in_nifty500": "n500"})[["symbol", "n50", "fo", "n500"]]
con = sqlite3.connect(os.path.join(AP, "arena_metrics.db"))
AU.to_sql("agent_universe", con, if_exists="replace", index=False)
USYM.to_sql("universe_symbols", con, if_exists="replace", index=False)
con.close()

lab = {"N50": "Nifty 50", "FO": "F&O (Nifty 200)", "N500": "Nifty 500"}
print(f"agent_universe built: {len(AU)} rows across {AU.agent_id.nunique()} agents")
for uk in ["N50", "FO", "N500"]:
    d = AU[AU.universe == uk]
    print(f"  {lab[uk]:<16}: {len(d):>4} agents qualify (>=8 trades) | verified {int(d.verified.sum()):>3} | median PF {d.profit_factor.median():.2f} | median win {d.win_pct.median():.0f}%")
print("\n  Best F&O agents (by PF, >=40 trades):")
fo = AU[(AU.universe == "FO") & (AU.trades >= 40)].merge(
    pd.read_sql_query("SELECT agent_id,codename FROM agent_summary", sqlite3.connect(os.path.join(AP, "arena_metrics.db"))), on="agent_id", how="left")
for _, r in fo.sort_values("profit_factor", ascending=False).head(8).iterrows():
    print(f"    {str(r.codename):<12} PF {r.profit_factor:>4.1f} · win {r.win_pct:.0f}% · {int(r.trades)} F&O trades · CAGR {r.cagr_comp:+.0f}%")

# --- run manifest -------------------------------------------------------------
import sys as _sys, os as _os; _sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
from run_manifest import stamp as _stamp
_stamp(__file__, ["agent_universe","universe_symbols"], rows=len(AU))
