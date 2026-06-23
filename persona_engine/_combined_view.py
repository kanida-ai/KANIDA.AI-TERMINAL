"""Combined view: Baseline vs Model vs Achieved%, same year x month grid, LONG & SHORT.
Model = loop_ml_signals (our Top-10 picks' actual open->close). Baseline = perfect Top-10."""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np, pandas as pd
from persona_engine import db, universe

con = db.connect(read_only=True)
fo, _ = universe.get_universes(con, as_of_date="2026-06-22")

# ---- MODEL: avg open->close of our Top-10 picks per day, by month ----
sig = pd.read_sql_query("SELECT outcome_date,direction,actual_oc FROM loop_ml_signals", con)
sig["ym"] = sig["outcome_date"].str[:7]
ml = sig.groupby(["ym","direction"])["actual_oc"].mean().unstack()
ml.columns = [c.lower() for c in ml.columns]  # long, short (daily avg already per pick; mean over days)

# careful: we need mean over DAYS of (mean of 10 picks). Do two-step:
day = sig.groupby(["outcome_date","direction"])["actual_oc"].mean().reset_index()
day["ym"] = day["outcome_date"].str[:7]
ml = day.groupby(["ym","direction"])["actual_oc"].mean().unstack()
ml = ml.rename(columns={"LONG":"m_long","SHORT":"m_short"})

# ---- BASELINE: perfect Top-10 / Bottom-10 per day, by month ----
op = pd.read_sql_query(
    "SELECT symbol,trade_date,oc_full FROM persona_open_features WHERE symbol IN (%s)"
    % ",".join("?"*len(fo)), con, params=fo).dropna(subset=["oc_full"])
bl=[]
for dt,g in op.groupby("trade_date"):
    if len(g)<10: continue
    bl.append((dt[:7], g.nlargest(10,"oc_full")["oc_full"].mean(), g.nsmallest(10,"oc_full")["oc_full"].mean()))
b = pd.DataFrame(bl,columns=["ym","b_long","b_short"]).groupby("ym").mean()

j = b.join(ml, how="left").reset_index()
j["year"]=j["ym"].str[:4]; j["mon"]=j["ym"].str[5:7].astype(int)
j["ach_long"]=(j["m_long"]/j["b_long"]*100)
j["ach_short"]=(j["m_short"]/j["b_short"]*100)

MON=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
def grid(col,r=2):
    g=j.pivot(index="year",columns="mon",values=col).reindex(columns=range(1,13)); g.columns=MON
    return g.round(r)

def stacked(side):
    """Build a stacked Baseline/Model/Achieved% table for one side."""
    bcol, mcol, acol = (f"b_{side}", f"m_{side}", f"ach_{side}")
    rows=[]
    for y in sorted(j["year"].unique()):
        for label,col,r in [("Baseline %",bcol,2),("Model %",mcol,2),("Achieved %",acol,0)]:
            sub=j[j["year"]==y].set_index("mon")[col].reindex(range(1,13))
            rows.append([y,label]+[round(v,r) if pd.notna(v) else None for v in sub.values])
    return pd.DataFrame(rows, columns=["Year","Metric"]+MON)

L=stacked("long"); S=stacked("short")
pd.set_option("display.width",200)
print("================= LONG (entry 9:15 open -> close) =================")
print(L.to_string(index=False))
print("\n================= SHORT (negative = profit) =================")
print(S.to_string(index=False))

OUT=Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\outputs\persona_findings")
path=OUT/"Baseline_vs_Model_vs_Achieved.xlsx"
with pd.ExcelWriter(path,engine="openpyxl") as xl:
    L.to_excel(xl,sheet_name="LONG_baseline_model_achieved",index=False)
    S.to_excel(xl,sheet_name="SHORT_baseline_model_achieved",index=False)
print("\nXLSX ->",path)
con.close()
