"""Companion to the baseline grid: what OUR model actually captures.

EOD (day T) -> rank Long Top-10 / Short Top-10 with the v2 EOD model -> enter next
day (T+1) at 09:15 open -> exit 15:30 close. Realized return per pick = open->close
(oc_full), same metric as the baseline. Aggregate avg of the 10 picks per active day,
then by year x month. Also report capture % = our_avg / baseline_avg."""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np, pandas as pd
from persona_engine import db, model, universe

con = db.connect(read_only=True)
fo, _ = universe.get_universes(con, as_of_date="2026-06-22")

feats = pd.read_sql_query(
    "SELECT * FROM persona_signal_features WHERE symbol IN (%s)" % ",".join("?"*len(fo)),
    con, params=fo)
openf = pd.read_sql_query(
    "SELECT symbol,trade_date,oc_full FROM persona_open_features WHERE symbol IN (%s)"
    % ",".join("?"*len(fo)), con, params=fo)

cal = sorted(feats["trade_date"].unique())
nxt = {cal[i]: cal[i+1] for i in range(len(cal)-1)}
feats["odate"] = feats["trade_date"].map(nxt)
o = openf.rename(columns={"trade_date": "odate"})
df = feats.merge(o, on=["symbol", "odate"], how="inner").dropna(subset=["oc_full"])

def crank(s):
    return (s.rank(pct=True) - 0.5) * 2.0

wL, wS = model.BASELINE_WEIGHTS["FO_LONG"], model.BASELINE_WEIGHTS["FO_SHORT"]
long_cap, short_cap = [], []
for dt, g in df.groupby("trade_date"):       # dt = EOD signal day; picks act on odate
    if len(g) < 20:
        continue
    g = g.set_index("symbol")
    ls = pd.Series(0.0, index=g.index); ss = pd.Series(0.0, index=g.index)
    for f, w in wL.items():
        if f in g: ls += w * crank(g[f]).fillna(0)
    for f, w in wS.items():
        if f in g: ss += w * crank(g[f]).fillna(0)
    mag = pd.Series(0.0, index=g.index)
    for f in ["atr_20_pct", "vol_ratio_20d"]:
        if f in g: mag += crank(g[f]).fillna(0)
    ls += 1.5*mag; ss += 1.5*mag
    od = g["odate"].iloc[0]
    long10 = g.loc[ls.nlargest(10).index, "oc_full"].mean()
    short10 = g.loc[ss.nlargest(10).index, "oc_full"].mean()
    long_cap.append((od[:4], int(od[5:7]), long10))
    short_cap.append((od[:4], int(od[5:7]), short10))

MON = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
def grid(rows):
    d = pd.DataFrame(rows, columns=["year","mon","val"])
    g = d.groupby(["year","mon"])["val"].mean().reset_index().pivot(
        index="year", columns="mon", values="val").reindex(columns=range(1,13))
    g.columns = MON
    return g.round(2)

gL, gS = grid(long_cap), grid(short_cap)
print("=== OUR MODEL CAPTURED: avg open->close % of our LONG Top-10 picks ===")
print(gL.to_string())
print("\n=== OUR MODEL CAPTURED: avg open->close % of our SHORT Top-10 picks ===")
print(gS.to_string())

# capture % vs baseline (recompute baseline inline)
bl_l, bl_s = [], []
for dt, g in df.groupby("odate"):
    if len(g) < 10: continue
    bl_l.append((dt[:4], int(dt[5:7]), g.nlargest(10,"oc_full")["oc_full"].mean()))
    bl_s.append((dt[:4], int(dt[5:7]), g.nsmallest(10,"oc_full")["oc_full"].mean()))
bL, bS = grid(bl_l), grid(bl_s)
capL = (gL / bL * 100).round(0)
capS = (gS / bS * 100).round(0)   # both negative -> ratio positive = % of short ceiling captured
print("\n=== LONG capture % of baseline ceiling ===")
print(capL.to_string())
print("\n=== SHORT capture % of baseline ceiling ===")
print(capS.to_string())

OUT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\outputs\persona_findings")
path = OUT / "Companion_Model_Captured_byMonth.xlsx"
with pd.ExcelWriter(path, engine="openpyxl") as xl:
    gL.to_excel(xl, sheet_name="LONG_captured_avg_%")
    gS.to_excel(xl, sheet_name="SHORT_captured_avg_%")
    bL.to_excel(xl, sheet_name="LONG_baseline_%")
    bS.to_excel(xl, sheet_name="SHORT_baseline_%")
    capL.to_excel(xl, sheet_name="LONG_capture_pct")
    capS.to_excel(xl, sheet_name="SHORT_capture_pct")
print("\nXLSX ->", path)
con.close()
