"""Raw 1-min order-flow export for NAUKRI / JIOFIN / CUB (cash) + their futures + NIFTY future,
for BOTH 2026-07-08 and 2026-07-09, with a 'Calculations' sheet documenting every formula.

07-08 = base fields (book + depth + OI). 07-09 = base PLUS the extra layers polled from today:
per-level ORDERS-count (whale-vs-crowd), last_qty, and the websocket TICK buy/sell split + block.
Every derived cell's formula is in the Calculations sheet.
"""
import sqlite3
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
OUT = Path.home() / "Downloads" / "RAW_FLOW_NAUKRI_JIOFIN_CUB_NIFTYFUT_2026-07-08_and_09.xlsx"
DAYS = {"0708": "2026-07-08", "0709": "2026-07-09"}

CASH = [("NAUKRI", "NSE:NAUKRI"), ("JIOFIN", "NSE:JIOFIN"), ("CUB", "NSE:CUB")]
FUT = [("NAUKRIFUT", "NFO:NAUKRI26JULFUT"), ("JIOFINFUT", "NFO:JIOFIN26JULFUT"), ("NIFTYFUT", "NFO:NIFTY26JULFUT")]
DEPTH = [f"{s}{i}{x}" for s in ("b", "a") for i in range(1, 6) for x in ("p", "q")]
ORD = [f"{s}{i}o" for s in ("b", "a") for i in range(1, 6)]


def flow_read(imb):
    if pd.isna(imb):
        return ""
    return ("buyers lifting" if imb >= 52 else "neutral" if imb >= 45 else
            "distribution" if imb >= 40 else "sellers hitting")


def oi_read(move_open, oi_open):
    """Persistent price x OI read using session-cumulative (from-open) values."""
    if pd.isna(oi_open) or abs(oi_open) < 0.05:
        return "flat"
    if oi_open > 0:                       # OI built up since open
        return "LONG buildup" if move_open >= 0 else "SHORT buildup"
    return "short covering" if move_open >= 0 else "long unwinding"   # OI declined


def whale_side(ab, aa):
    if not ab or not aa or pd.isna(ab) or pd.isna(aa):
        return ""
    if ab >= 1.5 * aa:
        return "bid (whale buyers)"
    if aa >= 1.5 * ab:
        return "ask (whale sellers)"
    return "balanced"


def build(con, key, day, is_fut, is_09):
    q = f"SELECT * FROM mkt_orderflow_1min WHERE instrument_key=? AND substr(bar_time,1,10)=? AND bar_time>=? ORDER BY bar_time"
    df = pd.read_sql_query(q, con, params=(key, day, f"{day} 09:15"))
    if df.empty:
        return df
    day_open = df["close"].iloc[0]
    df["move_from_open%"] = (df["close"] / day_open - 1) * 100
    df["min_chg%"] = df["close"].pct_change() * 100
    tb, ts = df["total_buy_qty"].fillna(0), df["total_sell_qty"].fillna(0)
    df["buy_imb%"] = np.where(tb + ts > 0, tb / (tb + ts) * 100, np.nan)
    bd = df[[f"b{i}q" for i in range(1, 6)]].sum(axis=1)
    ad = df[[f"a{i}q" for i in range(1, 6)]].sum(axis=1)
    df["depth_bid%"] = np.where(bd + ad > 0, bd / (bd + ad) * 100, np.nan)
    df["spread"] = df["a1p"] - df["b1p"]
    df["flow_read"] = df["buy_imb%"].apply(flow_read)
    cols = ["segment", "bar_time", "open", "high", "low", "close", "volume", "atp", "oi",
            "total_buy_qty", "total_sell_qty", "move_from_open%", "min_chg%", "buy_imb%",
            "depth_bid%", "spread", "flow_read"]
    if is_fut:
        oi_open = df["oi"].iloc[0]
        df["oi_chg%"] = df["oi"].pct_change() * 100
        df["oi_from_open%"] = (df["oi"] / oi_open - 1) * 100 if oi_open else np.nan
        df["oi_read"] = [oi_read(m, o) for m, o in zip(df["move_from_open%"], df["oi_from_open%"])]
        cols += ["oi_chg%", "oi_from_open%", "oi_read"]
    cols += DEPTH
    if is_09:
        df["avg_bid_ord"] = np.where(df["b1o"] > 0, df["b1q"] / df["b1o"], np.nan)   # qty per order @ best bid
        df["avg_ask_ord"] = np.where(df["a1o"] > 0, df["a1q"] / df["a1o"], np.nan)
        df["whale_side"] = [whale_side(a, b) for a, b in zip(df["avg_bid_ord"], df["avg_ask_ord"])]
        tk = pd.read_sql_query(
            "SELECT bar_time,n_ticks,buy_vol,sell_vol,buy_vol_pct,avg_tick_vol,max_tick_vol,max_trade_qty "
            "FROM mkt_trades_1min WHERE instrument_key=? AND substr(bar_time,1,10)=?", con, params=(key, day))
        tk = tk.rename(columns={"n_ticks": "tick_n", "buy_vol": "tick_buy_vol", "sell_vol": "tick_sell_vol",
                                "buy_vol_pct": "tick_buy%", "avg_tick_vol": "tick_avg_size",
                                "max_tick_vol": "tick_max_burst", "max_trade_qty": "tick_max_order"})
        df = df.merge(tk, on="bar_time", how="left")
        df["block_x"] = np.where(df["tick_avg_size"] > 0, df["tick_max_order"] / df["tick_avg_size"], np.nan)
        cols += ["last_qty"] + ORD + ["avg_bid_ord", "avg_ask_ord", "whale_side",
                 "tick_n", "tick_buy_vol", "tick_sell_vol", "tick_buy%", "tick_avg_size",
                 "tick_max_burst", "tick_max_order", "block_x"]
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df[cols].round(3)


def calc_sheet():
    rows = [
        ("bar_time", "as polled (IST)", "Minute stamp; the OHLC/volume are for the whole minute; the book/depth/OI/atp are a SNAPSHOT at the minute's close (~last of ~12 sub-min polls)."),
        ("open/high/low/close", "1-min OHLC of last-traded price", "Aggregated from ~5s quote() polls within the minute."),
        ("volume", "cumulative_volume(t) - cumulative_volume(t-1)", "Shares traded DURING the minute (delta of the day's running volume)."),
        ("atp", "exchange average_price", "Day's volume-weighted avg price so far (VWAP proxy)."),
        ("oi", "open interest (futures only)", "Outstanding contracts; snapshot at minute close."),
        ("total_buy_qty / total_sell_qty", "quote buy_quantity / sell_quantity", "Total pending BUY vs SELL limit orders across the WHOLE book (not just top 5)."),
        ("move_from_open%", "(close - day_open_close) / day_open_close * 100", "Move since the first minute's close of the day."),
        ("min_chg%", "(close - prev_close) / prev_close * 100", "This minute's % change."),
        ("buy_imb%", "total_buy_qty / (total_buy_qty + total_sell_qty) * 100", "Whole-book bid/ask imbalance. >50 = more resting buyers."),
        ("depth_bid%", "sum(b1q..b5q) / (sum(b1q..b5q) + sum(a1q..a5q)) * 100", "Near-touch (top-5) depth lean. >50 = thicker bids."),
        ("spread", "a1p - b1p", "Best ask minus best bid."),
        ("flow_read", "buy_imb% >=52 -> buyers lifting; 45-52 -> neutral; 40-45 -> distribution; <40 -> sellers hitting", "Book-imbalance classification of the minute."),
        ("oi_chg% (fut)", "(oi - prev_oi) / prev_oi * 100", "Minute change in open interest."),
        ("oi_from_open% (fut)", "(oi - day_open_oi) / day_open_oi * 100", "OI change since session open."),
        ("oi_read (fut)", "uses FROM-OPEN cumulatives: oi_from_open%>0 & move_from_open%>=0 -> LONG buildup; oi_from_open%>0 & move<0 -> SHORT buildup; oi_from_open%<0 & move>=0 -> short covering; oi_from_open%<0 & move<0 -> long unwinding; |oi_from_open%|<0.05 -> flat", "Persistent price x OI structural read for the session."),
        ("b1p..b5p / a1p..a5p", "raw depth prices", "5-level bid/ask prices."),
        ("b1q..b5q / a1q..a5q", "raw depth quantities", "Shares resting at each of the 5 levels."),
        ("--- 2026-07-09 ONLY (extra layers) ---", "", ""),
        ("b1o..b5o / a1o..a5o", "orders-count per level", "NUMBER of individual orders at each depth level (new poll field)."),
        ("last_qty", "last_traded_quantity", "Size of the most recent single trade at snapshot."),
        ("avg_bid_ord", "b1q / b1o", "Avg shares PER ORDER at best bid. HIGH = one big (whale) buyer; LOW = many small (crowd) buyers."),
        ("avg_ask_ord", "a1q / a1o", "Avg shares per order at best ask (whale vs crowd sellers)."),
        ("whale_side", "avg_bid_ord >= 1.5*avg_ask_ord -> bid; avg_ask_ord >= 1.5*avg_bid_ord -> ask; else balanced", "Which side has the big single orders."),
        ("tick_n", "count of ticks in the minute (websocket)", "Number of trade prints."),
        ("tick_buy_vol / tick_sell_vol", "sum of per-trade volume classified buy / sell (Lee-Ready: trade vs bid/ask midpoint, tick-rule fallback)", "Volume that was buyer- vs seller-aggressed."),
        ("tick_buy%", "tick_buy_vol / (tick_buy_vol + tick_sell_vol) * 100", "Was the minute's traded volume buyer- or seller-driven (aggressor)."),
        ("tick_avg_size", "minute traded volume / tick_n", "Average trade size."),
        ("tick_max_burst", "largest single-tick volume delta", "Biggest volume in one tick."),
        ("tick_max_order", "largest last_traded_quantity seen", "Biggest single print (block proxy)."),
        ("block_x", "tick_max_order / tick_avg_size", "How many x the average trade the biggest print was (block detection)."),
    ]
    return pd.DataFrame(rows, columns=["Field", "Formula / Rule", "Meaning"])


def main():
    con = sqlite3.connect("file:" + str(DB).replace("\\", "/") + "?mode=ro", uri=True)
    sheets = {}; summ = []
    for tag, day in DAYS.items():
        is09 = (tag == "0709")
        for label, key in CASH + FUT:
            is_fut = (label, key) in FUT
            df = build(con, key, day, is_fut, is09)
            if df.empty:
                continue
            sheets[f"{label}_{tag}"] = df
            summ.append(dict(instrument=label, day=day, close=df["close"].iloc[-1],
                             **{"day%": round(df["move_from_open%"].iloc[-1], 2)},
                             avg_buy_imb=round(df["buy_imb%"].mean(), 1),
                             fut_oi_from_open=round(df["oi_from_open%"].iloc[-1], 2) if is_fut else None,
                             dominant_read=(df["oi_read"] if is_fut else df["flow_read"]).mode().iloc[0]))
    con.close()
    with pd.ExcelWriter(OUT, engine="openpyxl") as w:
        calc_sheet().to_excel(w, "Calculations", index=False)
        pd.DataFrame(summ).to_excel(w, "Summary", index=False)
        for name, df in sheets.items():
            df.to_excel(w, name, index=False)
    print(f"wrote {OUT}  ({len(sheets)} data sheets + Calculations + Summary)")
    print("sheets:", list(sheets.keys()))


if __name__ == "__main__":
    main()
