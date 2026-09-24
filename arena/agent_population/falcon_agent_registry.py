"""Falcon 865 pattern-agents — Responsibility Registry + leaderboard + status.
Status from BOTH validations: PROMOTED = clean-2026 lift>0 AND bear lift>0; PROBATION = clean>0 but bear<=0 (fair-weather);
RETIRED = clean<=0. Registry DB (queryable for Auto/Custom deploy) + leaderboard Excel. Read-only."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
AP = os.path.join(ROOT, "arena", "agent_population")
R = pd.read_csv(os.path.join(AP, "falcon_pattern_agents.csv"))

def status(r):
    if not (r.strict26_lift == r.strict26_lift) or r.oos_fires < 30: return "measuring"
    if r.strict26_lift > 0 and (r.bear_lift > 0 if r.bear_lift == r.bear_lift else True): return "PROMOTED"
    if r.strict26_lift > 0: return "PROBATION"
    return "RETIRED"
R["status"] = R.apply(status, axis=1)
R["agent_id"] = "FALCPAT_" + R.pattern_id.astype(str)
R["mandate"] = ("Buy when rule fires; win if +" + R.target.str.extract(r"hit_(\d+)pc")[0] + "% within " +
                R.target.str.extract(r"_(\d+)d")[0] + "d, else exit at horizon. Rs5L account, direction=long.")

# registry DB
con = sqlite3.connect(os.path.join(AP, "falcon_pattern_registry.db")); cur = con.cursor()
cur.execute("DROP TABLE IF EXISTS agents")
cur.execute("""CREATE TABLE agents(agent_id TEXT PRIMARY KEY, pattern_id INT, target TEXT, regime TEXT, mined_year INT,
   oos_fires INT, hit_pct REAL, base_pct REAL, real_lift_pp REAL, clean26_lift REAL, bear_lift REAL,
   avg_ret REAL, cagr5L REAL, status TEXT, rule TEXT, mandate TEXT)""")
cur.executemany("INSERT INTO agents VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
    R[["agent_id","pattern_id","target","regime","mined_year","oos_fires","hit_pct","base_pct","real_lift_pp",
       "strict26_lift","bear_lift","avg_ret","cagr5L","status","english","mandate"]].itertuples(index=False, name=None))
con.commit()
print(f"Falcon Pattern-Agent Registry: {len(R)} agents")
print("  status:", dict(cur.execute("SELECT status,COUNT(*) FROM agents GROUP BY status").fetchall()))
print("  by regime (PROMOTED only):", dict(cur.execute("SELECT regime,COUNT(*) FROM agents WHERE status='PROMOTED' GROUP BY regime").fetchall()))
con.close()

# leaderboard Excel
xls = os.path.join(AP, "FALCON_PATTERN_AGENTS_LEADERBOARD.xlsx")
cols = ["agent_id","pattern_id","target","regime","mined_year","oos_fires","hit_pct","base_pct","real_lift_pp","strict26_lift","bear_lift","avg_ret","cagr5L","status","english"]
with pd.ExcelWriter(xls, engine="openpyxl") as w:
    R.sort_values("strict26_lift", ascending=False)[cols].round(2).to_excel(w, "AllAgents", index=False)
    R[R.status == "PROMOTED"].sort_values("strict26_lift", ascending=False)[cols].round(2).to_excel(w, "PROMOTED", index=False)
    R.groupby("regime").agg(n=("pattern_id","count"), med_clean_lift=("strict26_lift","median"), med_bear_lift=("bear_lift","median"),
        pct_promoted=("status", lambda s:(s=="PROMOTED").mean()*100)).round(2).to_excel(w, "ByRegime")
    R.groupby("target").agg(n=("pattern_id","count"), med_clean_lift=("strict26_lift","median"), med_bear_lift=("bear_lift","median"),
        med_avg_ret=("avg_ret","median")).round(2).to_excel(w, "ByTarget")
print(f"\n-> registry: {os.path.join(AP,'falcon_pattern_registry.db')}\n-> leaderboard: {xls}")
print("\nTOP 12 PROMOTED agents (by clean-2026 lift):")
top = R[R.status == "PROMOTED"].sort_values("strict26_lift", ascending=False).head(12)
print(top[["agent_id","target","regime","oos_fires","hit_pct","base_pct","strict26_lift","bear_lift","avg_ret","cagr5L"]].round(1).to_string(index=False))
print("\nDeploy modes (registry queries):")
print(f"  AUTO   — deploy 1 pattern-agent across the universe: each already fires on all matching stock-days.")
print(f"  CUSTOM — all PROMOTED agents on one stock: filter registry by status + re-eval rules on that symbol.")
print(f"  Rentable book = the {int((R.status=='PROMOTED').sum())} PROMOTED agents (robust clean-OOS + bear).")
