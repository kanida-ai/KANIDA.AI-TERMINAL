"""Stock-specific TRADE LOG + TRADE JOURNAL for 2024/2025/2026.
Per-trade: rule, signal, signal-type, entry (T+1 open), exit (T+1 close), favourable move (MFE), adverse move (MAE).
Rolled up daily -> monthly -> yearly, per stock, so consistent winners vs fine-tune candidates are visible.
Plus a DAILY BASKET view: #long / #short, basket size, how many moved favourable vs adverse, day P&L.
Rs 30,000 fixed per signal. Long = Buy/StrongBuy, Short = Sell/StrongSell. Costs = 0.10% round-trip (Rs30/trade).
Outputs: STOCK_MINER_JOURNAL.xlsx (journal) + STOCK_MINER_TRADES_2024_2026.csv (raw log).
"""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
HERE = os.path.dirname(os.path.abspath(__file__)); DB = os.path.join(HERE, "stock_miner.db")
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
CAP = 30000; COST = 0.10 / 100 * CAP           # Rs30 round-trip / trade
DL = os.path.join(os.path.expanduser("~"), "Downloads")

# ---- load selective signals for 2024-2026 (trade_date = execution day T+1) ----
db = sqlite3.connect(DB)
S = pd.read_sql_query("SELECT signal_date,trade_date,symbol,tier,rule,leaf_train_acc,leaf_acc,ret_oc,correct_sel "
                      "FROM signals WHERE tier!='Neutral' AND trade_date>='2024-01-01'", db)
db.close()
S = S.rename(columns={"leaf_acc": "live_acc", "leaf_train_acc": "rule_train_acc"})
S["side"] = np.where(S.tier.isin(["Buy", "StrongBuy"]), "LONG", "SHORT")
print(f"selective trades 2024-2026: {len(S):,}  ·  stocks {S.symbol.nunique()}")

# ---- join execution-day OHLC for entry/exit + MFE (favourable) / MAE (adverse) ----
con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
OH = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close FROM ohlc_daily WHERE trade_date>='2024-01-01'", con)
con.close()
S = S.merge(OH, on=["symbol", "trade_date"], how="left")
miss = S.open.isna().sum()
if miss: print(f"  WARN {miss} trades missing OHLC (dropped)"); S = S.dropna(subset=["open", "high", "low", "close"])
S["entry"] = S.open.round(2); S["exit"] = S.close.round(2)
S["ret_pct"] = ((S.close - S.open) / S.open * 100).round(3)          # T+1 open->close, self-consistent
sign = np.where(S.side == "LONG", 1.0, -1.0)
S["dir_ret"] = (S.ret_pct * sign).round(3)          # return in the trade's own direction
S["pnl_gross"] = (sign * S.ret_pct / 100 * CAP).round(0)
S["pnl_net"] = (S.pnl_gross - COST).round(0)
# favourable move = best the trade went in our direction; adverse = worst against us (intraday of exec day)
long_mfe = (S.high - S.open) / S.open * 100; long_mae = (S.low - S.open) / S.open * 100
shrt_mfe = (S.open - S.low) / S.open * 100; shrt_mae = (S.open - S.high) / S.open * 100
S["fav_move_pct"] = np.where(S.side == "LONG", long_mfe, shrt_mfe).round(3)     # MFE >=0
S["adv_move_pct"] = np.where(S.side == "LONG", long_mae, shrt_mae).round(3)     # MAE <=0
S["outcome"] = np.where(S.pnl_gross > 0, "WIN", "LOSS")
S["year"] = S.trade_date.str[:4]; S["month"] = S.trade_date.str[:7]
TYPE = {"StrongBuy": "Strong Buy", "Buy": "Buy", "Sell": "Sell", "StrongSell": "Strong Sell"}
S["signal_type"] = S.tier.map(TYPE)

# ---- TRADE LOG (raw, every trade) ----
LOGCOLS = ["signal_date", "trade_date", "symbol", "side", "signal_type", "rule", "rule_train_acc", "live_acc",
           "entry", "exit", "ret_pct", "fav_move_pct", "adv_move_pct", "pnl_gross", "pnl_net", "outcome", "year", "month"]
LOG = S[LOGCOLS].sort_values(["trade_date", "symbol"]).reset_index(drop=True)
csv = os.path.join(DL, "STOCK_MINER_TRADES_2024_2026.csv"); LOG.to_csv(csv, index=False)
print(f"trade log -> {csv}  ({len(LOG):,} rows)")


def agg(df):
    r = dict(trades=len(df), longs=int((df.side == "LONG").sum()), shorts=int((df.side == "SHORT").sum()),
             wins=int((df.outcome == "WIN").sum()), wr=round((df.outcome == "WIN").mean() * 100, 1),
             gross=int(df.pnl_gross.sum()), net=int(df.pnl_net.sum()),
             long_net=int(df.loc[df.side == "LONG", "pnl_net"].sum()), short_net=int(df.loc[df.side == "SHORT", "pnl_net"].sum()),
             avg_fav=round(df.fav_move_pct.mean(), 3), avg_adv=round(df.adv_move_pct.mean(), 3),
             avg_ret=round(df.dir_ret.mean(), 3))
    return pd.Series(r)


# ---- per-stock YEARLY & MONTHLY ----
SY = S.groupby(["symbol", "year"]).apply(agg).reset_index()
SM = S.groupby(["symbol", "month"]).apply(agg).reset_index()

# ---- per-stock SCORECARD (2024-2026 verdict, short book = the edge) ----
rows = []
for sym, g in S.groupby("symbol"):
    sh = g[g.side == "SHORT"]; lo = g[g.side == "LONG"]
    yn = {y: int(sh.loc[sh.year == y, "pnl_net"].sum()) for y in ["2024", "2025", "2026"]}
    yrs_present = [y for y in ["2024", "2025", "2026"] if (sh.year == y).any()]
    pos = sum(1 for y in yrs_present if yn[y] > 0)
    short_net = int(sh.pnl_net.sum()); short_wr = round((sh.outcome == "WIN").mean() * 100, 1) if len(sh) else np.nan
    if len(yrs_present) and pos == len(yrs_present) and short_net > 0: rating = "CONSISTENT"
    elif short_net > 0: rating = "MIXED"
    else: rating = "FINE-TUNE"
    rows.append(dict(symbol=sym, short_trades=len(sh), short_wr=short_wr, short_net=short_net,
                     short_2024=yn["2024"], short_2025=yn["2025"], short_2026=yn["2026"], pos_years=pos,
                     long_trades=len(lo), long_net=int(lo.pnl_net.sum()),
                     total_net=int(g.pnl_net.sum()), rating=rating))
SC = pd.DataFrame(rows).sort_values(["rating", "short_net"], ascending=[True, False])

# ---- RULE performance per (stock, side, rule) : realized OOS WR / P&L (fine-tune driver) ----
RP = S.groupby(["symbol", "side", "rule"]).apply(
    lambda d: pd.Series(dict(n=len(d), wr=round((d.outcome == "WIN").mean() * 100, 1),
                             net=int(d.pnl_net.sum()), avg_ret=round((d.ret_pct * (1 if d.side.iloc[0] == "LONG" else -1)).mean(), 3),
                             train_acc=round(d.rule_train_acc.mean(), 3)))).reset_index()
RP = RP[RP.n >= 20].sort_values(["symbol", "net"], ascending=[True, False])

# ---- DAILY BASKET view ----
def basket(df):
    lo = df[df.side == "LONG"]; sh = df[df.side == "SHORT"]
    return pd.Series(dict(basket_size=len(df), longs=len(lo), shorts=len(sh),
                          fav=int((df.pnl_gross > 0).sum()), adverse=int((df.pnl_gross <= 0).sum()),
                          long_fav=int((lo.pnl_gross > 0).sum()), short_fav=int((sh.pnl_gross > 0).sum()),
                          capital_deployed=len(df) * CAP,
                          long_net=int(lo.pnl_net.sum()), short_net=int(sh.pnl_net.sum()), net=int(df.pnl_net.sum()),
                          avg_fav_move=round(df.fav_move_pct.mean(), 3), avg_adv_move=round(df.adv_move_pct.mean(), 3)))
DB_ = S.groupby("trade_date").apply(basket).reset_index()

# ---- overall MONTHLY / YEARLY ----
MO = S.groupby("month").apply(agg).reset_index()
YR = S.groupby("year").apply(agg).reset_index()

# ---- write journal ----
xls = os.path.join(DL, "STOCK_MINER_JOURNAL.xlsx")
with pd.ExcelWriter(xls, engine="openpyxl") as w:
    SC.to_excel(w, "scorecard", index=False)
    YR.to_excel(w, "yearly_summary", index=False)
    MO.to_excel(w, "monthly_summary", index=False)
    DB_.to_excel(w, "daily_basket", index=False)
    SY.to_excel(w, "stock_yearly", index=False)
    SM.to_excel(w, "stock_monthly", index=False)
    RP.to_excel(w, "rule_performance", index=False)
    for y in ["2024", "2025", "2026"]:
        LOG[LOG.year == y].to_excel(w, f"trades_{y}", index=False)
print(f"journal -> {xls}")

# ---- console recap ----
print("\n===== YEARLY (net, Rs30k/signal) =====")
print(YR[["year", "trades", "longs", "shorts", "wr", "long_net", "short_net", "net"]].to_string(index=False))
print("\n===== SCORECARD rating counts =====")
print(SC.rating.value_counts().to_string())
print("\n----- top 12 CONSISTENT short-side stocks -----")
print(SC[SC.rating == "CONSISTENT"].head(12)[["symbol", "short_trades", "short_wr", "short_net", "short_2024", "short_2025", "short_2026"]].to_string(index=False))
print("\n----- 12 worst FINE-TUNE stocks (short net) -----")
print(SC.sort_values("short_net").head(12)[["symbol", "short_trades", "short_wr", "short_net", "long_net", "rating"]].to_string(index=False))
print(f"\ndaily basket: {len(DB_)} trading days · avg basket {DB_.basket_size.mean():.0f} "
      f"(L {DB_.longs.mean():.0f}/S {DB_.shorts.mean():.0f}) · avg net Rs{DB_.net.mean():,.0f}/day")
