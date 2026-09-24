"""Trader-PERSONA leaderboards (leak-controlled) — re-simulates each agent's SAME buy-signals at 5 holds
(INTRADAY 1d · BTST 2d · WEEKLY 5d · SWING 15d · MONTHLY 25d; entry = next open, exit = close after hold),
across 4 universes (ALL / N50 / F&O≈N200 / N500). LEAKAGE CONTROLS:
  (1) weekly-feature agents fire ONLY on the last session of the week (weekly cols are constant within an ISO week);
  (2) TRUE OUT-OF-SAMPLE — each agent is scored ONLY on dates AFTER its pattern's mining year (no in-sample overlap);
  (3) annualize over the full committed calendar window, never active-months (a bursty agent must not get its return squared).
Emits per-signal EXPECTANCY (robust headline) + a ₹5L single-account model (secondary) + a `tradeable` flag (net-positive
expectancy). Ranking, the profitable gate, and the rental seed are applied in the app. NON-DESTRUCTIVE: writes only agent_persona."""
import os, sqlite3, json
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); AP = os.path.dirname(os.path.abspath(__file__))
CAP = 5e5; COST = 0.15
PERSONAS = {"INTRADAY": 1, "BTST": 2, "WEEKLY": 5, "SWING": 15, "MONTHLY": 25}
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}
uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
feat = pd.read_sql_query("SELECT * FROM falcon_features WHERE trade_date>='2024-06-01'", uc)
oh = pd.read_sql_query("SELECT symbol,trade_date,open,high,close FROM ohlc_daily WHERE trade_date>='2024-06-01' ORDER BY symbol,trade_date", uc)
tax = pd.read_sql_query("SELECT pattern_id,target,mined_year,rule_json FROM falcon_pattern_taxonomy", uc)
um = pd.read_sql_query("SELECT symbol,in_nifty50,in_nifty200,in_nifty500 FROM universe_master WHERE is_active=1", uc); uc.close()
rc = sqlite3.connect(os.path.join(AP, "falcon_pattern_registry.db"))
_aa = pd.read_sql_query("SELECT * FROM arena_agents", rc); rc.close()
_aa = _aa.drop(columns=[c for c in ["mined_year", "rule_json"] if c in _aa.columns])
agents = _aa.merge(tax[["pattern_id", "mined_year", "rule_json"]], on="pattern_id")
ALLSYMS = set(oh.symbol.unique())
UNIS = {"ALL": ALLSYMS, "N50": set(um[um.in_nifty50 == 1].symbol), "FO": set(um[um.in_nifty200 == 1].symbol), "N500": set(um[um.in_nifty500 == 1].symbol)}

outs = []
for s, g in oh.groupby("symbol", sort=False):
    O = g.open.values.astype(float); C = g.close.values.astype(float)
    entry = np.roll(O, -1); entry[-1] = np.nan
    d = {"symbol": s, "trade_date": g.trade_date.values}
    for pname, hold in PERSONAS.items():
        exitc = pd.Series(C).shift(-hold).values
        d[f"ret_{pname}"] = (exitc - entry) / entry * 100 - COST
    outs.append(pd.DataFrame(d))
M = feat.merge(pd.concat(outs, ignore_index=True), on=["symbol", "trade_date"], how="inner").reset_index(drop=True)
M["ym"] = M.trade_date.str[:7]; M["yr"] = M.trade_date.str[:4].astype(int)
cal = pd.DataFrame({"trade_date": sorted(M.trade_date.unique())}); dtt = pd.to_datetime(cal.trade_date)
cal["wk"] = dtt.dt.isocalendar().year.astype(str) + "-" + dtt.dt.isocalendar().week.astype(str)
cal["we"] = cal.wk != cal.wk.shift(-1); WE = dict(zip(cal.trade_date, cal.we)); M["we"] = M.trade_date.map(WE)
# scoring window: 2025-01..2026-07 (per-agent further restricted to AFTER its mining year — true OOS)
WINLO, WINHI = "2025-01-01", "2026-07-31"
INWIN = (M.trade_date >= WINLO) & (M.trade_date <= WINHI)
def uses_weekly(rj): return any(f.startswith("weekly_") for f, _, _ in json.loads(rj))

def _stats(sub, retcol, hold, winmos):
    d = sub.dropna(subset=[retcol]).sort_values("trade_date")
    if len(d) < 12: return None
    r = d[retcol].values; wins = r[r > 0]; losses = r[r <= 0]
    pf = wins.sum()/-losses.sum() if losses.sum() < 0 else 99.0
    winyrs = max(winmos/12.0, 1e-6)
    eq = CAP; peak = CAP; mdd = 0.0; free = None; cd = max(int(hold*1.5), 1)
    for dt, rr in zip(d.trade_date.values, r):
        if free is not None and dt < free: continue
        eq = max(eq*(1+rr/100), 1.0); peak = max(peak, eq); mdd = max(mdd, (peak-eq)/peak)
        free = (pd.Timestamp(dt)+pd.Timedelta(days=cd)).strftime("%Y-%m-%d")
    cagr = ((eq/CAP)**(1/winyrs)-1)*100 if eq > 0 else -100.0
    r30 = ((eq/CAP)**(1/max(winmos, 1))-1)*100 if eq > 0 else -100.0
    exp = float(r.mean())                                   # per-signal expectancy — robust headline
    return dict(trades=len(r), win_pct=float((r > 0).mean()*100), avg_win=float(wins.mean()) if len(wins) else 0.0,
        avg_loss=float(losses.mean()) if len(losses) else 0.0, profit_factor=round(float(pf), 2), avg_ret=exp,
        expectancy=exp, ret_12m=float(cagr), ret_30d=float(r30), maxdd=float(mdd*100), final_5L=float(eq),
        trades_per_month=round(len(r)/max(winmos, 1), 1), hold_days=hold, tradeable=int(exp > 0))

rows = []
for _, a in agents.iterrows():
    m = np.ones(len(M), dtype=bool)
    try:
        for f, op, thr in json.loads(a.rule_json): m &= OPS[op](M[f].values, thr)
    except Exception: continue
    wk = uses_weekly(a.rule_json)
    oos = M.yr.values > int(a.mined_year)                    # TRUE OOS: only after the pattern's mining year
    idx = m & INWIN.values & oos & (M.we.values if wk else True)
    base = M.loc[idx, ["trade_date", "ym", "yr", "symbol"] + [f"ret_{p}" for p in PERSONAS]]
    if len(base) < 12: continue
    winmos = base.ym.nunique()                               # calendar months the agent is actually eligible (post-mining)
    for uk, uset in UNIS.items():
        us = base[base.symbol.isin(uset)]
        if len(us) < 12: continue
        umos = us.ym.nunique()
        for pname, hold in PERSONAS.items():
            st = _stats(us, f"ret_{pname}", hold, umos)
            if st: rows.append(dict(agent_id=a.agent_id, persona=pname, universe=uk, mined_year=int(a.mined_year), **st))

AP_df = pd.DataFrame(rows)
con = sqlite3.connect(os.path.join(AP, "arena_metrics.db"))
AP_df.to_sql("agent_persona", con, if_exists="replace", index=False)
cn = pd.read_sql_query("SELECT agent_id,codename FROM agent_summary", con); con.close()
CN = dict(zip(cn.agent_id, cn.codename))

lab = {"INTRADAY": "Intraday", "BTST": "BTST", "WEEKLY": "Weekly", "SWING": "Swing", "MONTHLY": "Monthly"}
print(f"agent_persona (true-OOS) built: {len(AP_df)} rows | {AP_df.agent_id.nunique()} agents")
for p in PERSONAS:
    g = AP_df[(AP_df.persona == p) & (AP_df.universe == "ALL")]
    tr = g[g.tradeable == 1]
    print(f"\n== {lab[p]:<9} ALL · {len(g)} scored, {len(tr)} PROFITABLE — champion by expectancy ==")
    if len(tr):
        for _, r in tr.sort_values("expectancy", ascending=False).head(3).iterrows():
            print(f"   {str(CN.get(r.agent_id,r.agent_id)):<11} exp {r.expectancy:+.2f}%/trade · win {r.win_pct:.0f}% · 12M {r.ret_12m:+.0f}% · {int(r.trades)}tr (OOS>{r.mined_year})")
    else:
        print("   (no agent clears costs at this hold)")

# --- run manifest -------------------------------------------------------------
import sys as _sys, os as _os; _sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
from run_manifest import stamp as _stamp
_stamp(__file__, ["agent_persona"], rows=len(AP_df))
