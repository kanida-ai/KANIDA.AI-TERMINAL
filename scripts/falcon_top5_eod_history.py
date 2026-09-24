"""
Falcon Top-5 HISTORICAL same-day backtest (entry 09:15 next trading day -> EOD close).
Uses the bit-exact engine replica over the slim DB feature history (no leakage:
signal computed on signal_date close, entry on the NEXT trading day's open).
Per-trade MFE/MAE/day-high/low from the entry-day daily OHLC bar.

Output: outputs/Falcon_Top5_EOD_History.xlsx
  - Trade_Log (every Top-5 trade, all requested columns)
  - Yearly / Monthly summary tables
  - Performance_Summary, Return_Distribution, Portfolio_Daily
"""
import sqlite3, bisect
from pathlib import Path
import numpy as np
import pandas as pd

from falcon_signal_replay import load_patterns, rank_for_date

ROOT = Path(__file__).resolve().parent.parent
SLIM = ROOT / "data" / "db" / "kanida_universe.db"
OUT = ROOT / "outputs" / "Falcon_Top5_EOD_History.xlsx"
DESK = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"
START = "2022-06-01"          # generate from here; real coverage reported
TOPN = 5


def main():
    con = sqlite3.connect(str(SLIM))
    patterns = load_patterns(con)
    print(f"[*] patterns {len(patterns)}; mined_year range "
          f"{min(p['mined_year'] for p in patterns)}..{max(p['mined_year'] for p in patterns)}", flush=True)

    cal = [r[0] for r in con.execute(
        "SELECT DISTINCT trade_date FROM ohlc_daily WHERE trade_date>=? ORDER BY 1", (START,))]
    calset = set(cal)

    def next_td(d):
        i = bisect.bisect_right(cal, d)
        return cal[i] if i < len(cal) else None

    feat_days = [r[0] for r in con.execute(
        "SELECT DISTINCT trade_date FROM falcon_features WHERE trade_date>=? ORDER BY 1", (START,))]

    rows = []
    n_emit = 0
    for k, sd in enumerate(feat_days):
        rk = rank_for_date(con, patterns, sd, min_fires=10)
        if not rk:
            continue
        ed = next_td(sd)
        if ed is None:
            continue
        top = rk[:TOPN]
        # fetch entry-day daily bars for these symbols
        syms = [c["symbol"] for c in top]
        ph = ",".join("?" * len(syms))
        bars = {r[0]: r[1:] for r in con.execute(
            f"SELECT symbol, open, high, low, close FROM ohlc_daily "
            f"WHERE trade_date=? AND symbol IN ({ph})", [ed, *syms])}
        any_emit = False
        for c in top:
            b = bars.get(c["symbol"])
            if not b:
                continue
            o, hi, lo, cl = b
            if o is None or not np.isfinite(o) or o <= 0:
                continue
            ret = (cl / o - 1) * 100.0
            rows.append({
                "Date": ed, "signal_date": sd, "Rank": c["rank"], "Stock": c["symbol"],
                "Signal Score": round(c["score"], 1), "avg_lift": round(c["avg_lift"], 3),
                "n_fires": c["n_fires"], "Entry Time": "09:15", "Entry Price": round(o, 2),
                "EOD Price": round(cl, 2), "Return %": round(ret, 3),
                "Day High": round(hi, 2), "Day Low": round(lo, 2),
                "Max Favorable Move %": round((hi / o - 1) * 100, 3),
                "Max Adverse Move %": round((lo / o - 1) * 100, 3),
                "Win/Loss": "WIN" if ret > 0 else "LOSS",
                "Notes": f"avg_lift={c['avg_lift']:.2f}, n_fires={c['n_fires']}",
            })
            any_emit = True
        if any_emit:
            n_emit += 1
        if (k + 1) % 200 == 0:
            print(f"  [{k+1}/{len(feat_days)}] {sd} (emitted days={n_emit})", flush=True)
    con.close()

    tl = pd.DataFrame(rows)
    tl["dt"] = pd.to_datetime(tl["Date"]); tl["Year"] = tl.dt.dt.year
    tl["YM"] = tl.dt.dt.strftime("%Y-%m")
    print(f"[*] trades: {len(tl):,}  trading days: {tl.Date.nunique():,}  "
          f"coverage {tl.Date.min()}..{tl.Date.max()}", flush=True)

    # portfolio-level Top-5 daily return (equal weight)
    port = tl.groupby("Date").agg(port_ret=("Return %", "mean"),
                                  n_stocks=("Return %", "size")).reset_index()
    port["dt"] = pd.to_datetime(port["Date"]); port["Year"] = port.dt.dt.year
    port["YM"] = port.dt.dt.strftime("%Y-%m")
    port = port.sort_values("Date").reset_index(drop=True)
    port["equity"] = (1 + port["port_ret"] / 100).cumprod()
    port["peak"] = port["equity"].cummax()
    port["drawdown%"] = (port["equity"] / port["peak"] - 1) * 100

    def maxdd(sub):
        if sub.empty:
            return 0.0
        eq = (1 + sub["port_ret"] / 100).cumprod()
        return round((eq / eq.cummax() - 1).min() * 100, 2)

    def trade_stats(g):
        w = g[g["Return %"] > 0]; l = g[g["Return %"] < 0]
        return {"Trades": len(g), "Win Rate %": round(len(w) / len(g) * 100, 1),
                "Avg Return %": round(g["Return %"].mean(), 3),
                "Avg Win %": round(w["Return %"].mean(), 3) if len(w) else None,
                "Avg Loss %": round(l["Return %"].mean(), 3) if len(l) else None}

    # Yearly
    yearly = []
    for yr, g in tl.groupby("Year"):
        p = port[port.Year == yr]
        s = trade_stats(g)
        s.update({"Year": yr,
                  "Best Day %": round(p.port_ret.max(), 3) if len(p) else None,
                  "Worst Day %": round(p.port_ret.min(), 3) if len(p) else None,
                  "Max Drawdown %": maxdd(p)})
        yearly.append(s)
    yearly = pd.DataFrame(yearly)[["Year", "Trades", "Win Rate %", "Avg Return %",
                                   "Avg Win %", "Avg Loss %", "Best Day %", "Worst Day %", "Max Drawdown %"]]

    # Monthly
    monthly = []
    for ym, g in tl.groupby("YM"):
        p = port[port.YM == ym]
        s = trade_stats(g)
        s.update({"Month": ym,
                  "Best Day %": round(p.port_ret.max(), 3) if len(p) else None,
                  "Worst Day %": round(p.port_ret.min(), 3) if len(p) else None})
        monthly.append(s)
    monthly = pd.DataFrame(monthly)[["Month", "Trades", "Win Rate %", "Avg Return %",
                                     "Avg Win %", "Avg Loss %", "Best Day %", "Worst Day %"]]

    # Overall performance summary
    w = tl[tl["Return %"] > 0]; l = tl[tl["Return %"] < 0]
    best_stock = tl.loc[tl["Return %"].idxmax()]; worst_stock = tl.loc[tl["Return %"].idxmin()]
    best_day = port.loc[port.port_ret.idxmax()]; worst_day = port.loc[port.port_ret.idxmin()]
    perf = pd.DataFrame([
        ("Coverage", f"{tl.Date.min()} .. {tl.Date.max()}"),
        ("Trading days", tl.Date.nunique()),
        ("Total trades", len(tl)),
        ("Profitable trades", int((tl['Return %'] > 0).sum())),
        ("Losing trades", int((tl['Return %'] < 0).sum())),
        ("Win rate %", round((tl['Return %'] > 0).mean() * 100, 1)),
        ("Avg return / trade %", round(tl['Return %'].mean(), 3)),
        ("Avg win %", round(w['Return %'].mean(), 3)),
        ("Avg loss %", round(l['Return %'].mean(), 3)),
        ("Avg MFE %", round(tl['Max Favorable Move %'].mean(), 3)),
        ("Avg MAE %", round(tl['Max Adverse Move %'].mean(), 3)),
        ("Best day (portfolio) %", f"{round(best_day.port_ret,3)} on {best_day.Date}"),
        ("Worst day (portfolio) %", f"{round(worst_day.port_ret,3)} on {worst_day.Date}"),
        ("Best stock-trade %", f"{best_stock['Return %']} {best_stock['Stock']} {best_stock['Date']}"),
        ("Worst stock-trade %", f"{worst_stock['Return %']} {worst_stock['Stock']} {worst_stock['Date']}"),
        ("Portfolio max drawdown %", round(port['drawdown%'].min(), 2)),
        ("Portfolio total return %", round((port.equity.iloc[-1] - 1) * 100, 1)),
    ], columns=["Metric", "Value"])

    # Return distribution (per-trade)
    bins = [-100, -7, -5, -3, -1, 0, 1, 3, 5, 7, 100]
    labels = ["<-7", "-7..-5", "-5..-3", "-3..-1", "-1..0", "0..1", "1..3", "3..5", "5..7", ">7"]
    tl["_bucket"] = pd.cut(tl["Return %"], bins, labels=labels)
    dist = tl.groupby("_bucket", observed=True).size().reset_index(name="Trades")
    dist["% of trades"] = round(dist["Trades"] / len(tl) * 100, 1)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    log_cols = ["Date", "Rank", "Stock", "Signal Score", "Entry Time", "Entry Price",
                "EOD Price", "Return %", "Day High", "Day Low", "Max Favorable Move %",
                "Max Adverse Move %", "Win/Loss", "Notes"]
    with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
        perf.to_excel(xl, "1_Performance_Summary", index=False)
        yearly.to_excel(xl, "2_Yearly", index=False)
        monthly.to_excel(xl, "3_Monthly", index=False)
        dist.to_excel(xl, "4_Return_Distribution", index=False)
        port[["Date", "n_stocks", "port_ret", "equity", "drawdown%"]].rename(
            columns={"port_ret": "Top5_Portfolio_Return%"}).to_excel(xl, "5_Portfolio_Daily", index=False)
        tl[log_cols].to_excel(xl, "6_Trade_Log", index=False)

    pd.set_option("display.width", 220)
    print("\n=== PERFORMANCE SUMMARY ===")
    print(perf.to_string(index=False))
    print("\n=== YEARLY ===")
    print(yearly.to_string(index=False))
    print(f"\n[*] wrote {OUT}")
    if DESK.exists():
        import shutil; shutil.copy(OUT, DESK / OUT.name); print(f"[*] copied to {DESK/OUT.name}")


if __name__ == "__main__":
    main()
