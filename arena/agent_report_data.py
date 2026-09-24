"""Aggregate all Addendum-F agent results into report_data.pkl for the .docx study report.
Computes (from the read-only falcon_full_ranking): Agent 1 (short top parked), Agent 2 (band x side matrix +
monthly OOS 5x + maxDD), Agent 3 (continuation forward-return curve). Pulls Agent 6 from its pickle, Agent 5 (APE)
summary constants. Builds the unified leaderboard ranked by OOS edge. Monthly view at 5x MIS."""
import os, sqlite3, pickle
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
DB = os.path.join(ROOT, "data", "db", "falcon_research.db")
LEV, FRIC = 5, 0.15
con = sqlite3.connect("file:" + DB.replace("\\", "/") + "?mode=ro", uri=True)
df = pd.read_sql_query("SELECT signal_date, rank, symbol, nd_intraday_ret, fwd1, fwd3, fwd5, fwd10, tier, regime FROM falcon_full_ranking", con)
con.close()
df["yr"] = df.signal_date.str[:4]; df["period"] = np.where(df.yr <= "2024", "LEARN", np.where(df.yr == "2025", "VALIDATE", "OOS"))
df["ym"] = df.signal_date.str[:7]
oos = df[(df.period == "OOS") & df.nd_intraday_ret.notna()]

def strat_stats(sub, sgn):
    """Equal-weight band at 5x MIS, SUM-OF-DAILY convention (non-compounding). Monthly % + OOS total + maxDD + edge/trade."""
    daily_net = (sgn * sub.groupby("signal_date").nd_intraday_ret.mean() - FRIC) / 100 * LEV   # daily 5x net return
    cum = (daily_net.cumsum()); dd = (cum.cummax() - cum).max() * 100                          # sum-basis drawdown
    monthly = daily_net.groupby(daily_net.index.str[:7]).sum() * 100                           # sum-of-daily 5x per month
    edge_1x = (sgn * sub.nd_intraday_ret).mean()
    return dict(edge=edge_1x, net_edge=edge_1x - FRIC, breadth=len(sub) / sub.signal_date.nunique(),
                total=daily_net.sum() * 100, maxdd=dd, monthly={m: round(v, 1) for m, v in monthly.items()})

STRATS = {
    "Agent2 · Top-10 LONG": (oos[oos["rank"] <= 10], 1),
    "Agent2 · Top-50 LONG": (oos[oos["rank"] <= 50], 1),
    "Agent2 · Top-100 LONG": (oos[oos["rank"] <= 100], 1),
    "Agent2 · Tail 201-500 SHORT": (oos[oos["rank"] >= 201], -1),
    "Agent1 · Top-100 SHORT (parked)": (oos[oos["rank"] <= 100], -1),
}
strat = {k: strat_stats(s, g) for k, (s, g) in STRATS.items()}

# Agent 3 continuation: forward-return curve (mean %) for Top-50 long, by period
a3 = {}
for p in ["LEARN", "VALIDATE", "OOS"]:
    sub = df[(df.period == p) & (df["rank"] <= 50)]
    a3[p] = {h: round(sub[c].mean(), 3) for h, c in [("nd", "nd_intraday_ret"), ("fwd1", "fwd1"), ("fwd3", "fwd3"), ("fwd5", "fwd5"), ("fwd10", "fwd10")]}

# Agent 2 full matrix (recompute for the doc table)
ubase = {p: df[df.period == p].nd_intraday_ret.mean() for p in ["LEARN", "VALIDATE", "OOS"]}
BANDS = [(1, 10), (11, 50), (51, 100), (101, 200), (201, 300), (301, 479)]
a2 = []
for lo, hi in BANDS:
    b = df[(df["rank"] >= lo) & (df["rank"] <= hi)]
    for side, sgn in [("LONG", 1), ("SHORT", -1)]:
        cell = {"band": f"{lo}-{hi}", "side": side}
        for p in ["LEARN", "VALIDATE", "OOS"]:
            cell[p] = round((sgn * b[b.period == p].nd_intraday_ret).mean(), 3)
        base = sgn * ubase["OOS"]; cell["net"] = round(cell["OOS"] - FRIC, 3); cell["vs_base"] = round(cell["OOS"] - base, 3)
        cell["breadth"] = round(len(b[b.period == "OOS"]) / oos.signal_date.nunique(), 0)
        cell["gate"] = "PASS" if (cell["net"] > 0 and cell["OOS"] > base + 0.08 and cell["LEARN"] > base and cell["VALIDATE"] > base) else ("weak+" if cell["net"] > 0 else "FAIL")
        a2.append(cell)

a6 = pickle.load(open(os.path.join(ROOT, "arena", "agent6_results.pkl"), "rb"))
out = dict(strat=strat, a2_matrix=a2, a3_curve=a3, a6=a6, ubase={p: round(v, 3) for p, v in ubase.items()},
           oos_days=oos.signal_date.nunique(), oos_span=(oos.signal_date.min(), oos.signal_date.max()),
           regime_split={r: round((oos.regime == r).mean() * 100) for r in oos.regime.unique()})
pickle.dump(out, open(os.path.join(ROOT, "arena", "report_data.pkl"), "wb"))
print("saved report_data.pkl")
print("\nLEADERBOARD (OOS 2026, 5x MIS):")
for k, v in sorted(strat.items(), key=lambda kv: -kv[1]["total"]):
    print(f"  {k:<34} edge/trade {v['edge']:+.3f}% (net {v['net_edge']:+.3f}) | OOS total {v['total']:+.0f}% | maxDD {v['maxdd']:.0f}% | breadth {v['breadth']:.0f}/day")
print("\nAgent3 continuation curve (Top-50 long, mean %):", a3["OOS"])
