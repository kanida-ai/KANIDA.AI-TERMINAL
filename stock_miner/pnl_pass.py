"""Long/Short P&L pass on the stock-miner signals. Rs 30,000 per selective signal.
Buy/StrongBuy -> LONG (P&L = +ret_oc*30k); Sell/StrongSell -> SHORT (P&L = -ret_oc*30k). Entry T+1 open,
exit T+1 close (the T+1 open->close the signal was judged on). Monthly + yearly, by side and tier, gross & net.
"""
import os, sqlite3
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); DB = os.path.join(HERE, "stock_miner.db")
CAP = 30000; COST_PCT = 0.10   # intraday round-trip cost assumption (brokerage+STT+slippage), % of turnover
db = sqlite3.connect(DB); S = pd.read_sql_query("SELECT trade_date,tier,ret_oc,correct_sel FROM signals WHERE tier!='Neutral'", db); db.close()
S["side"] = np.where(S.tier.isin(["Buy", "StrongBuy"]), "LONG", "SHORT")
S["gross"] = np.where(S.side == "LONG", S.ret_oc, -S.ret_oc) / 100 * CAP
S["net"] = S.gross - COST_PCT / 100 * CAP          # cost = 0.10% of Rs30k = Rs30/trade round-trip
S["mo"] = S.trade_date.str[:7]; S["yr"] = S.trade_date.str[:4]
print(f"selective signals: {len(S):,}  ·  Rs {CAP:,}/signal  ·  cost {COST_PCT}% (Rs{int(COST_PCT/100*CAP)}/trade)\n")

def blk(df, name):
    g = df.gross.sum(); nt = df.net.sum(); wr = df.correct_sel.mean() * 100
    print(f"  {name:<26} n={len(df):>7}  WR {wr:4.1f}%  gross Rs {g:>+13,.0f}  net Rs {nt:>+13,.0f}  net/trade Rs {nt/len(df):>+6,.0f}")

print("===== OVERALL by side & tier =====")
blk(S[S.side == "SHORT"], "ALL SHORTS (Sell+SS)")
blk(S[S.tier == "StrongSell"], "  StrongSell only")
blk(S[S.tier == "Sell"], "  Sell only")
blk(S[S.side == "LONG"], "ALL LONGS (Buy+SB)")
blk(S[S.tier == "StrongBuy"], "  StrongBuy only")
blk(S, "EVERYTHING (long+short)")

# monthly (shorts-only, since longs lose) + combined
rows = []
for mo, gm in S.groupby("mo"):
    sh = gm[gm.side == "SHORT"]; ss = gm[gm.tier == "StrongSell"]; lo = gm[gm.side == "LONG"]
    rows.append(dict(month=mo, short_n=len(sh), short_net=sh.net.sum(), SS_n=len(ss), SS_net=ss.net.sum(),
                     long_n=len(lo), long_net=lo.net.sum(), all_net=gm.net.sum()))
M = pd.DataFrame(rows)
xls = os.path.join(os.path.expanduser("~"), "Downloads", "STOCK_MINER_PNL.xlsx")
with pd.ExcelWriter(xls, engine="openpyxl") as w: M.to_excel(w, "monthly", index=False)
print(f"\nsaved monthly P&L -> {xls}")
print("\n===== YEARLY net P&L (Rs 30k/signal, net of 0.10% cost) =====")
print(f"  {'year':<6}{'shorts net':>15}{'StrongSell net':>16}{'longs net':>14}{'ALL net':>14}")
for yr, gm in S.groupby("yr"):
    sh = gm[gm.side == "SHORT"]; ss = gm[gm.tier == "StrongSell"]; lo = gm[gm.side == "LONG"]
    print(f"  {yr:<6}{sh.net.sum():>+15,.0f}{ss.net.sum():>+16,.0f}{lo.net.sum():>+14,.0f}{gm.net.sum():>+14,.0f}")
# per-trade economics
print("\n===== per-trade economics (gross) =====")
for t in ["StrongSell", "Sell", "Buy", "StrongBuy"]:
    d = S[S.tier == t]
    if len(d):
        wins = d[d.gross > 0]; loss = d[d.gross <= 0]
        print(f"  {t:<12} avg move {(d.gross/CAP*100).mean():+.3f}%/trade  ·  avg win Rs{wins.gross.mean():+,.0f}  avg loss Rs{loss.gross.mean():+,.0f}")
