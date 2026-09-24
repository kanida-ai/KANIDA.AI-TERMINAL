"""ADDENDUM F — AGENT 2: Rank-Band Profitability Study (the one mis-run on Top-10).
FULL 500 ranking, 6 bands x BOTH sides (long+short) = 12 cells. Next-day 09:15->EOD, MIS 5x.
Protocol: LEARN 2022-24 -> VALIDATE 2025 -> touch-once OOS 2026. Each cell = trading that WHOLE band un-selected
(long or short). Edge/trade vs the universe base rate; a cell PASSES only if it is positive AND beats the universe
base for that side OUT-OF-SAMPLE by a multiple-testing margin. Direction fixed (both sides reported, never swapped).
Reads the read-only research table; Falcon untouched. NET = gross - round-trip friction (~15 bps)."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
DB = os.path.join(ROOT, "data", "db", "falcon_research.db")
BANDS = [(1, 10), (11, 50), (51, 100), (101, 200), (201, 300), (301, 479)]
FRICTION = 0.15                       # ~11bps cost + ~4bps slippage round-trip, as % of notional
N_CELLS = 12; MARGIN = 0.05 * (1 + 0.05 * N_CELLS)   # multiple-testing margin on edge (pp)

con = sqlite3.connect("file:" + DB.replace("\\", "/") + "?mode=ro", uri=True)
df = pd.read_sql_query("SELECT signal_date, rank, symbol, nd_intraday_ret FROM falcon_full_ranking WHERE nd_intraday_ret IS NOT NULL", con)
con.close()
df["yr"] = df.signal_date.str[:4]
df["period"] = np.where(df.yr <= "2024", "LEARN", np.where(df.yr == "2025", "VALIDATE", "OOS"))
ndays = {p: g.signal_date.nunique() for p, g in df.groupby("period")}
ubase = {p: g.nd_intraday_ret.mean() for p, g in df.groupby("period")}   # universe base = avg next-day 09:15->EOD
print(f"rows {len(df):,} | LEARN {ndays.get('LEARN')}d  VALIDATE {ndays.get('VALIDATE')}d  OOS {ndays.get('OOS')}d")
print("universe base (avg next-day 09:15->EOD %):", {p: round(v, 3) for p, v in ubase.items()})
print(f"multiple-testing margin on OOS edge: {MARGIN:.2f}pp  ({N_CELLS} cells tested)\n")

print(f"{'band':<10}{'side':<6}{'breadth/d':>10}{'LEARN':>9}{'VALID':>9}{'OOS_gross':>11}{'OOS_net':>9}{'vs_base':>9}{'GATE':>7}")
results = []
for lo, hi in BANDS:
    b = df[(df["rank"] >= lo) & (df["rank"] <= hi)]
    for side, sgn in [("LONG", 1), ("SHORT", -1)]:
        row = {"band": f"{lo}-{hi}", "side": side}
        for p in ["LEARN", "VALIDATE", "OOS"]:
            sub = b[b.period == p]
            row[p] = (sgn * sub.nd_intraday_ret).mean() if len(sub) else np.nan
        breadth = len(b[b.period == "OOS"]) / max(ndays.get("OOS", 1), 1)
        oos_gross = row["OOS"]; oos_net = oos_gross - FRICTION
        base_side = sgn * ubase.get("OOS", 0)
        passes = (oos_net > 0) and (oos_gross > base_side + MARGIN) and (row["LEARN"] > base_side) and (row["VALIDATE"] > base_side)
        gate = "PASS" if passes else ("weak+" if oos_net > 0 else "FAIL")
        results.append({**row, "breadth": breadth, "oos_net": oos_net, "vs_base": oos_gross - base_side, "gate": gate})
        print(f"{row['band']:<10}{side:<6}{breadth:>10.0f}{row['LEARN']:>+9.2f}{row['VALIDATE']:>+9.2f}{oos_gross:>+11.2f}{oos_net:>+9.2f}{oos_gross-base_side:>+9.2f}{gate:>7}")

R = pd.DataFrame(results)
print("\n=== BAND x SIDE MATRIX read-out ===")
print("LONG edge by band (OOS gross %/trade):", {r.band: round(r.OOS, 2) for _, r in R[R.side == "LONG"].iterrows()})
print("SHORT edge by band (OOS gross %/trade):", {r.band: round(r.OOS, 2) for _, r in R[R.side == "SHORT"].iterrows()})
pas = R[R.gate == "PASS"]
print(f"\nCELLS PASSING all gates OOS: {len(pas)}/{N_CELLS}")
for _, r in pas.iterrows(): print(f"   {r.band} {r.side}: OOS net {r.oos_net:+.2f}%/trade, breadth {r.breadth:.0f}/day, +{r.vs_base:.2f}pp vs base")
print("\n[AGENT 1 answer — shorting Falcon at depth] SHORT Top-50 (11-50) & Top-100 (51-100) OOS gross:",
      {r.band: round(r.OOS, 2) for _, r in R[(R.side == "SHORT") & (R.band.isin(["11-50", "51-100"]))].iterrows()},
      "-> ", "SHORT EDGE FOUND" if len(pas[(pas.side == "SHORT")]) else "no short edge at Top-50/100 (parked, as a valid finding)")
