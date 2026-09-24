"""Per-agent live signals — which stocks each agent is firing on, for the most recent trading days.
Evaluates each agent's rule on the last ~20 sessions -> (agent_id, date, symbol). -> arena_metrics.db: agent_signals.
Read-only; rule logic never surfaced (IP)."""
import os, sqlite3, json
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); AP = os.path.dirname(os.path.abspath(__file__))
OPS = {">": np.greater, ">=": np.greater_equal, "<": np.less, "<=": np.less_equal}
uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
dates = [d for (d,) in uc.execute("SELECT DISTINCT trade_date FROM falcon_features ORDER BY trade_date DESC LIMIT 20")]
lo = min(dates)
feat = pd.read_sql_query(f"SELECT * FROM falcon_features WHERE trade_date>='{lo}'", uc)
tax = pd.read_sql_query("SELECT pattern_id,target,rule_json FROM falcon_pattern_taxonomy", uc); uc.close()
rc = sqlite3.connect(os.path.join(AP, "falcon_pattern_registry.db"))
ag = pd.read_sql_query("SELECT agent_id,pattern_id FROM arena_agents", rc).merge(tax, on="pattern_id"); rc.close()
mc = sqlite3.connect(os.path.join(AP, "arena_metrics.db"))
cn = dict(pd.read_sql_query("SELECT agent_id,codename FROM agent_summary", mc).values)

rows = []
import pandas as _pd
_c=_pd.DataFrame({"d":sorted(set(feat.trade_date))}); _d=_pd.to_datetime(_c.d)
_c["wk"]=_d.dt.isocalendar().year.astype(str)+"-"+_d.dt.isocalendar().week.astype(str); _c["we"]=_c.wk!=_c.wk.shift(-1)
WE_TODAY=dict(zip(_c.d,_c.we)); feat["_we"]=feat.trade_date.map(WE_TODAY)
sym = feat.symbol.values; dt = feat.trade_date.values; wearr=feat._we.values
for _, a in ag.iterrows():
    m = np.ones(len(feat), dtype=bool)
    try:
        for f, op, thr in json.loads(a.rule_json): m &= OPS[op](feat[f].values, thr)
    except Exception: continue
    _wk = any(f.startswith("weekly_") for f,_,_ in json.loads(a.rule_json))
    if _wk: m = m & wearr
    if m.any():
        for i in np.where(m)[0]:
            rows.append((a.agent_id, cn.get(a.agent_id, a.agent_id), dt[i], sym[i]))
SG = pd.DataFrame(rows, columns=["agent_id", "codename", "date", "symbol"])
SG.to_sql("agent_signals", mc, if_exists="replace", index=False); mc.close()
latest = max(dates)
print(f"agent_signals built: {len(SG)} signal-rows over {SG.date.nunique()} sessions ({SG.date.min()}..{SG.date.max()})")
print(f"\nLATEST SESSION ({latest}): {SG[SG.date==latest].agent_id.nunique()} agents firing on {SG[SG.date==latest].symbol.nunique()} stocks")
ex = SG[SG.date == latest].groupby("codename").symbol.apply(lambda x: ", ".join(sorted(set(x))[:5])).head(6)
print("sample agents' picks today:")
for c, s in ex.items(): print(f"  {c:<12} -> {s}")

# --- run manifest -------------------------------------------------------------
import sys as _sys, os as _os; _sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
from run_manifest import stamp as _stamp
_stamp(__file__, ["agent_signals"], rows=len(SG),
       oos="n/a - live forward signals, no outcome scored", cost=0.0)
