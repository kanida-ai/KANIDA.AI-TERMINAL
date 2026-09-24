"""Month-over-month performance report from the stock-miner DB. Accuracy + signal counts per tier
(Buy / Strong Buy / Sell / Strong Sell), from the first month of mining, plus the forced-every-day view.
Saves an Excel. Run after miner.py completes.
"""
import os, sqlite3
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); DB = os.path.join(HERE, "stock_miner.db")
db = sqlite3.connect(DB); S = pd.read_sql_query("SELECT * FROM signals", db); db.close()
S["mo"] = S.trade_date.str[:7]
print(f"signals {len(S):,}  ·  stocks {S.symbol.nunique()}  ·  period {S.trade_date.min()} -> {S.trade_date.max()}")

# ---- overall per-tier ----
print("\n===== OVERALL accuracy by tier =====")
for t in ["StrongBuy", "Buy", "Sell", "StrongSell"]:
    s = S[S.tier == t]
    if len(s): print(f"  {t:<12} n={len(s):>7}  accuracy {s.correct_sel.mean()*100:4.1f}%")
print(f"  {'FORCED(all)':<12} n={len(S):>7}  accuracy {S.correct_forced.mean()*100:4.1f}%")

# ---- month over month ----
rows = []
for mo, g in S.groupby("mo"):
    row = {"month": mo, "signals": len(g)}
    for t, key in [("StrongBuy", "SB"), ("Buy", "B"), ("Sell", "S"), ("StrongSell", "SS")]:
        st = g[g.tier == t]
        row[f"{key}_n"] = len(st); row[f"{key}_acc"] = round(st.correct_sel.mean() * 100, 1) if len(st) else np.nan
    row["forced_acc"] = round(g.correct_forced.mean() * 100, 1)
    # a simple long-only "if traded" return: take long on Buy/SB (ret_oc), short on Sell/SS (-ret_oc)
    lng = g[g.tier.isin(["Buy", "StrongBuy"])]; sht = g[g.tier.isin(["Sell", "StrongSell"])]
    row["long_avg_oc"] = round(lng.ret_oc.mean(), 3) if len(lng) else np.nan
    row["short_avg_oc"] = round(-sht.ret_oc.mean(), 3) if len(sht) else np.nan
    rows.append(row)
M = pd.DataFrame(rows)
xls = os.path.join(os.path.expanduser("~"), "Downloads", "STOCK_MINER_MONTHLY.xlsx")
# per-stock short-side accuracy (the edge) for the strongest tier
stk = S[S.tier.isin(["Sell", "StrongSell"])].groupby("symbol").agg(n=("correct_sel", "size"), acc=("correct_sel", "mean")).reset_index()
stk["acc"] = (stk.acc * 100).round(1); stk = stk[stk.n >= 50].sort_values("acc", ascending=False)
with pd.ExcelWriter(xls, engine="openpyxl") as w:
    M.to_excel(w, "monthly", index=False)
    stk.to_excel(w, "short_edge_by_stock", index=False)
print(f"\nsaved month-over-month -> {xls}")
print("\n===== MONTH-OVER-MONTH (accuracy %, n) — selective tiers =====")
pd.set_option("display.width", 240, "display.max_columns", 40, "display.max_rows", 100)
print(M[["month", "signals", "SB_n", "SB_acc", "B_n", "B_acc", "S_n", "S_acc", "SS_n", "SS_acc", "forced_acc"]].to_string(index=False))
# yearly rollup
S["yr"] = S.trade_date.str[:4]
print("\n===== yearly per-tier accuracy =====")
for yr, g in S.groupby("yr"):
    parts = []
    for t, k in [("StrongBuy", "SB"), ("Buy", "B"), ("Sell", "S"), ("StrongSell", "SS")]:
        st = g[g.tier == t]
        parts.append(f"{k} {st.correct_sel.mean()*100:.0f}%/n{len(st)}" if len(st) else f"{k} -")
    print(f"  {yr}: " + "  ".join(parts) + f"   forced {g.correct_forced.mean()*100:.0f}%")
