"""Baseline month-over-month grid: average daily open->close % of the ACTUAL Top-10
gainers (LONG) and Top-10 losers (SHORT) across the F&O universe, per active trading
day, aggregated by year x month. This is the perfect-pick ceiling."""
import warnings; warnings.filterwarnings("ignore")
from pathlib import Path
import numpy as np, pandas as pd
from persona_engine import db, universe

con = db.connect(read_only=True)
fo, _ = universe.get_universes(con, as_of_date="2026-06-22")
df = pd.read_sql_query(
    "SELECT symbol, trade_date, oc_full FROM persona_open_features WHERE symbol IN (%s)"
    % ",".join("?"*len(fo)), con, params=fo)
df = df.dropna(subset=["oc_full"])
df["year"] = df["trade_date"].str[:4]
df["mon"] = df["trade_date"].str[5:7].astype(int)

rows_long, rows_short, rows_days = [], [], []
for (y, dt), g in df.groupby(["year", "trade_date"]):
    if len(g) < 10:
        continue
    top = g.nlargest(10, "oc_full")["oc_full"].mean()
    bot = g.nsmallest(10, "oc_full")["oc_full"].mean()
    m = int(dt[5:7])
    rows_long.append((y, m, top)); rows_short.append((y, m, bot)); rows_days.append((y, m))

MON = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
def grid(rows):
    d = pd.DataFrame(rows, columns=["year","mon","val"])
    p = d.groupby(["year","mon"])["val"].mean().reset_index()
    g = p.pivot(index="year", columns="mon", values="val").reindex(columns=range(1,13))
    g.columns = MON
    return g.round(2)

gL = grid(rows_long); gS = grid(rows_short)
days = pd.DataFrame(rows_days, columns=["year","mon"]).groupby(["year","mon"]).size().reset_index(name="n")
gD = days.pivot(index="year", columns="mon", values="n").reindex(columns=range(1,13)); gD.columns = MON

print("=== BASELINE: avg daily open->close % of ACTUAL Top-10 GAINERS (LONG) ===")
print(gL.to_string())
print("\n=== BASELINE: avg daily open->close % of ACTUAL Top-10 LOSERS (SHORT) ===")
print(gS.to_string())
print("\n=== active trading days counted per month ===")
print(gD.to_string())

OUT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\outputs\persona_findings")
OUT.mkdir(parents=True, exist_ok=True)
path = OUT / "Baseline_Top10_OpenToClose_byMonth.xlsx"
with pd.ExcelWriter(path, engine="openpyxl") as xl:
    gL.to_excel(xl, sheet_name="LONG_Top10_avg_%")
    gS.to_excel(xl, sheet_name="SHORT_Top10_avg_%")
    gD.to_excel(xl, sheet_name="active_days")
print("\nXLSX ->", path)
con.close()
