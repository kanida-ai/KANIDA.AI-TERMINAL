"""Top 5 @ 09:15 — winning vs losing days by month & year, with avg return on each.
Reads the validated daily log from Falcon_Intraday_Backtest_Results.xlsx (Top 5 basket)."""
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "outputs" / "Falcon_Intraday_Backtest_Results.xlsx"
OUT = ROOT / "outputs" / "Top5_Monthly_Breakdown.xlsx"

log = pd.read_excel(SRC, sheet_name="2_Daily_Trade_Log")
t5 = log[log["basket_label"] == "Top 5"].copy()
t5["entry_date"] = pd.to_datetime(t5["entry_date"])
t5["year"] = t5["entry_date"].dt.year
t5["month"] = t5["entry_date"].dt.month
t5["ym"] = t5["entry_date"].dt.strftime("%Y-%m")
r = t5["portfolio_return_pct"]
t5["win"] = r > 0
t5["loss"] = r < 0
t5["flat"] = r == 0

def agg(g):
    win = g[g["win"]]; loss = g[g["loss"]]
    return pd.Series({
        "trading_days": len(g),
        "winning_days": int(g["win"].sum()),
        "losing_days": int(g["loss"].sum()),
        "flat_days": int(g["flat"].sum()),
        "win_rate_%": round(g["win"].mean() * 100, 1),
        "avg_ret_win_%": round(win["portfolio_return_pct"].mean(), 3) if len(win) else None,
        "avg_ret_loss_%": round(loss["portfolio_return_pct"].mean(), 3) if len(loss) else None,
        "avg_ret_all_%": round(g["portfolio_return_pct"].mean(), 3),
        "best_day_%": round(g["portfolio_return_pct"].max(), 2),
        "worst_day_%": round(g["portfolio_return_pct"].min(), 2),
        "month_sum_%": round(g["portfolio_return_pct"].sum(), 2),
    })

monthly = t5.groupby(["year", "month", "ym"]).apply(agg, include_groups=False).reset_index()
yearly = t5.groupby("year").apply(agg, include_groups=False).reset_index()

overall = agg(t5)
print("=== TOP 5 @ 09:15 — MONTH BY MONTH ===")
print(monthly.drop(columns=["ym"]).to_string(index=False))
print("\n=== YEARLY ===")
print(yearly.to_string(index=False))
print("\n=== OVERALL (all 503 days) ===")
print(overall.to_string())

with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
    monthly.drop(columns=["ym"]).to_excel(xl, sheet_name="Monthly", index=False)
    yearly.to_excel(xl, sheet_name="Yearly", index=False)
    overall.to_frame("value").to_excel(xl, sheet_name="Overall")
    t5[["entry_date", "stocks", "portfolio_return_pct", "exit_reason", "exit_time",
        "hit_1pct", "win", "loss"]].to_excel(xl, sheet_name="Daily_Log", index=False)
print(f"\n[*] wrote {OUT}")
