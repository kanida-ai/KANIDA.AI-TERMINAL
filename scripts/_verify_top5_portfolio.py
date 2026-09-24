"""(A) Verify the regenerated Top-5 + entry-date match the STORED production
falcon_signals_live across every recorded date (correct DB + workflow + entry).
(B) Portfolio-level WR for Top-5 EOD incl. the >=+1%/day rate. Read-only."""
import sqlite3, bisect
from pathlib import Path
import numpy as np
import pandas as pd
from falcon_signal_replay import load_patterns, rank_for_date

ROOT = Path(__file__).resolve().parent.parent
SLIM = ROOT / "data" / "db" / "kanida_universe.db"
XLSX = ROOT / "outputs" / "Falcon_Top5_EOD_History.xlsx"

con = sqlite3.connect(str(SLIM))
patterns = load_patterns(con)

# ---------- (A) verification vs stored falcon_signals_live ----------
cal = [r[0] for r in con.execute("SELECT DISTINCT trade_date FROM ohlc_daily ORDER BY 1")]
def next_td(d):
    i = bisect.bisect_right(cal, d); return cal[i] if i < len(cal) else None

stored = {}
for sd, rk, sym, ed in con.execute(
        "SELECT signal_date, rank, symbol, entry_date FROM falcon_signals_live WHERE rank<=10"):
    stored.setdefault(sd, {"rows": {}, "entry": ed})["rows"][rk] = sym

print(f"[*] FALCON_DB = {SLIM.name}  (slim/production)")
print(f"[*] stored signal dates in falcon_signals_live: {len(stored)} "
      f"({min(stored)}..{max(stored)})\n")
print("=== VERIFY regenerated vs STORED (top-5 set, rank-1, entry_date) ===")
t5_ok = r1_ok = ent_ok = n = 0
for sd in sorted(stored):
    rk = rank_for_date(con, patterns, sd, min_fires=10)
    if not rk:
        continue
    n += 1
    regen5 = [c["symbol"] for c in rk[:5]]
    stored5 = [stored[sd]["rows"].get(i) for i in range(1, 6)]
    if set(regen5) == set(stored5): t5_ok += 1
    if regen5[0] == stored5[0]: r1_ok += 1
    my_entry = next_td(sd)
    if my_entry == stored[sd]["entry"]: ent_ok += 1
print(f"  dates checked: {n}")
print(f"  Top-5 set identical:   {t5_ok}/{n} ({t5_ok/n*100:.1f}%)")
print(f"  Rank-1 identical:      {r1_ok}/{n} ({r1_ok/n*100:.1f}%)")
print(f"  entry_date == stored:  {ent_ok}/{n} ({ent_ok/n*100:.1f}%)")
print(f"  (entry rule: next trading day after signal_date, 09:15 open -> EOD close)\n")
con.close()

# ---------- (B) portfolio-level WR ----------
tl = pd.read_excel(XLSX, "6_Trade_Log")
tl["dt"] = pd.to_datetime(tl["Date"]); tl["Year"] = tl["dt"].dt.year
port = tl.groupby(["Date", "Year"])["Return %"].mean().reset_index(name="port_ret")

def wr(g):
    return pd.Series({
        "Days": len(g),
        "WR>0 %": round((g.port_ret > 0).mean() * 100, 1),
        "WR>=+1% %": round((g.port_ret >= 1).mean() * 100, 1),
        "WR>=+2% %": round((g.port_ret >= 2).mean() * 100, 1),
        "Avg port day %": round(g.port_ret.mean(), 3),
        "Median %": round(g.port_ret.median(), 3),
    })

overall = wr(port)
yearly = port.groupby("Year").apply(wr, include_groups=False).reset_index()
print("=== PORTFOLIO-LEVEL WR (Top-5 equal-weight, entry 9:15 -> EOD) ===")
print("OVERALL:", overall.to_dict())
print(yearly.to_string(index=False))

with pd.ExcelWriter(XLSX, engine="openpyxl", mode="a", if_sheet_exists="replace") as xl:
    yearly.to_excel(xl, "7_Portfolio_WR", index=False)
print(f"\n[*] added sheet 7_Portfolio_WR to {XLSX.name}")
