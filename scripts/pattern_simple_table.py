"""Simple per-pattern table (no generalizing) — every one of the 865 patterns, exact columns requested:
  pattern, fires, WR% at 1/2/3/5/6-day, avg return 1-day at 1X and 5X, positional avg (2/3/5/6-day) at 1X only,
  # consistent stocks. Saves all 865 to Excel + prints a readable sample. Also reports the ledger period.
"""
import os, sqlite3, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
OUT = os.path.join(ROOT, "research_outputs", "falcon_pattern_backtest")
PH = pd.read_csv(os.path.join(OUT, "pattern_holds.csv"))

# ledger period
L = pd.read_parquet(os.path.join(OUT, "fire_ledger.parquet"))[["signal_date"]]
print(f"LEDGER PERIOD (leak-free): {L.signal_date.min()}  ->  {L.signal_date.max()}   ({len(L):,} trades)\n")

# taxonomy name/family if present
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
tcols = [d[1] for d in con.execute("PRAGMA table_info(falcon_pattern_taxonomy)")]
namecol = next((c for c in ["name", "family", "label", "pattern_name", "archetype"] if c in tcols), None)
tax = pd.read_sql_query(f"SELECT pattern_id{',' + namecol if namecol else ''} FROM falcon_pattern_taxonomy", con); con.close()

T = PH.copy()
T["pattern"] = "FALCPAT_" + T.pattern_id.astype(str)
if namecol: T = T.merge(tax.rename(columns={namecol: "family"}), on="pattern_id", how="left")
T["avg_1d_5X"] = (T.avg1 * 5).round(2)
S = pd.DataFrame({
    "pattern": T.pattern,
    "family": T.get("family", ""),
    "fires": T.n,
    "WR_1d": T.win1, "WR_2d": T.win2, "WR_3d": T.win3, "WR_5d": T.win5, "WR_6d": T.win6,
    "avg_1d_1X": T.avg1, "avg_1d_5X": T.avg_1d_5X,
    "avg_2d_1X": T.avg2, "avg_3d_1X": T.avg3, "avg_5d_1X": T.avg5, "avg_6d_1X": T.avg6,
    "consistent_stocks": T.n_consistent_stocks,
}).sort_values("consistent_stocks", ascending=False).reset_index(drop=True)
xls = os.path.join(os.path.expanduser("~"), "Downloads", "FALCON_865_PATTERN_TABLE.xlsx")
S.to_excel(xls, index=False)
print(f"all 865 patterns -> {xls}\n")
pd.set_option("display.width", 200, "display.max_columns", 30)
show = S.drop(columns=["family"]) if "family" in S else S
print("SAMPLE — top 30 patterns by #consistent stocks (full 865 in the Excel):\n")
print(show.head(30).to_string(index=False))
