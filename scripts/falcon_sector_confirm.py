"""
Sector-confirmed Falcon entry.
At the 9:15 open: a sector is "up" if its stocks open green on average; a stock is
"up" if it opens above its prior close. From the EOD Falcon Top-20 watchlist, enter
ONLY the picks where BOTH the stock is up AND its sector is up. Enter at the open,
exit at the close. Standard performance-format output.
Output: outputs/Falcon_Sector_Confirmed.xlsx
"""
import sqlite3, bisect
from pathlib import Path
import numpy as np
import pandas as pd
from falcon_signal_replay import load_patterns, rank_for_date

ROOT = Path(__file__).resolve().parent.parent
SLIM = ROOT / "data" / "db" / "kanida_universe.db"
OUT = ROOT / "outputs" / "Falcon_Sector_Confirmed_Top5.xlsx"
DESK = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"
POOL = 20
PICK = 5                 # concentrate to top-5 by Falcon rank among confirmed picks
START = "2023-01-01"


def main():
    con = sqlite3.connect(str(SLIM))
    patterns = load_patterns(con)

    # sector map
    sectors = {}
    try:
        for s, sec in con.execute("SELECT symbol, sector FROM falcon_sectors"):
            if sec:
                sectors[s] = sec
    except Exception:
        pass
    if not sectors:                       # fallback: sector from stored signals
        for s, sec in con.execute("SELECT DISTINCT symbol, sector FROM falcon_signals_live WHERE sector IS NOT NULL"):
            sectors[s] = sec
    print(f"[*] sector map: {len(sectors)} symbols", flush=True)

    cal = [r[0] for r in con.execute("SELECT DISTINCT trade_date FROM ohlc_daily WHERE trade_date>=? ORDER BY 1",
                                     (START,))]
    calset = set(cal)
    def next_td(d):
        i = bisect.bisect_right(cal, d)
        return cal[i] if i < len(cal) else None

    feat_days = [r[0] for r in con.execute(
        "SELECT DISTINCT trade_date FROM falcon_features WHERE trade_date>=? ORDER BY 1", (START,))]

    rows = []            # exploded per-stock
    daily = []           # per-day portfolio
    for k, sd in enumerate(feat_days):
        ed = next_td(sd)
        if ed is None or ed not in calset:
            continue
        rk = rank_for_date(con, patterns, sd, min_fires=10)
        if not rk:
            continue
        top20 = {c["symbol"]: c for c in rk[:POOL]}

        # opens on entry day, closes on signal day (= prior trading day of ed)
        opens = {r[0]: (r[1], r[2]) for r in con.execute(
            "SELECT symbol, open, close FROM ohlc_daily WHERE trade_date=?", (ed,))}   # (open, close) on ed
        pclose = {r[0]: r[1] for r in con.execute(
            "SELECT symbol, close FROM ohlc_daily WHERE trade_date=?", (sd,))}          # prior close

        # universe-wide opening gaps -> sector breadth
        sec_gaps = {}
        for sym, sec in sectors.items():
            o = opens.get(sym); pc = pclose.get(sym)
            if not o or pc is None or pc <= 0 or o[0] is None or o[0] <= 0:
                continue
            g = o[0] / pc - 1
            sec_gaps.setdefault(sec, []).append(g)
        sector_up = {sec: (np.mean(v) > 0) for sec, v in sec_gaps.items() if v}
        n_sec_up = sum(1 for up in sector_up.values() if up)

        # qualify the Top-20: stock up AND sector up
        legs = []
        for sym, info in top20.items():
            o = opens.get(sym); pc = pclose.get(sym); sec = sectors.get(sym)
            if not o or pc is None or pc <= 0 or o[0] is None or o[0] <= 0:
                continue
            op, cl = o
            stock_up = (op / pc - 1) > 0
            if stock_up and sector_up.get(sec, False):
                ret = (cl / op - 1) * 100
                legs.append({"date": ed, "stock": sym, "sector": sec, "rank": info["rank"],
                             "gap_pct": round((op / pc - 1) * 100, 2),
                             "entry_open": round(op, 2), "exit_close": round(cl, 2),
                             "return_pct": round(ret, 3), "result": "WIN" if ret > 0 else "LOSS"})
        legs.sort(key=lambda l: l["rank"])      # best Falcon rank first
        legs = legs[:PICK]                       # concentrate to top-5 of the confirmed names
        if not legs:
            daily.append({"date": ed, "n_stocks": 0, "stocks": "", "sectors_up": n_sec_up,
                          "portfolio_return_pct": None, "result": "NO_TRADE"})
            continue
        port = float(np.mean([l["return_pct"] for l in legs]))
        rows.extend(legs)
        daily.append({"date": ed, "n_stocks": len(legs),
                      "stocks": ", ".join(l["stock"] for l in legs), "sectors_up": n_sec_up,
                      "portfolio_return_pct": round(port, 3),
                      "result": "WIN" if port > 0 else "LOSS"})
        if (k + 1) % 200 == 0:
            print(f"  [{k+1}/{len(feat_days)}] {ed}", flush=True)
    con.close()

    tl = pd.DataFrame(rows)
    dl = pd.DataFrame(daily)
    traded = dl[dl.n_stocks > 0].copy()
    traded["dt"] = pd.to_datetime(traded["date"]); traded["Year"] = traded.dt.dt.year
    traded["YM"] = traded.dt.dt.strftime("%Y-%m")
    pr = traded["portfolio_return_pct"]

    eq = (1 + pr / 100).cumprod(); dd = (eq / eq.cummax() - 1) * 100
    w = pr[pr > 0]; l = pr[pr < 0]
    perf = pd.DataFrame([
        ("Coverage", f"{traded.date.min()} .. {traded.date.max()}"),
        ("Total signal days", len(dl)),
        ("Days traded (>=1 qualifier)", len(traded)),
        ("Days NO trade (0 qualifiers)", int((dl.n_stocks == 0).sum())),
        ("Days with >=5 stocks", int((traded.n_stocks >= 5).sum())),
        ("Days with <5 stocks", int((traded.n_stocks < 5).sum())),
        ("Avg stocks / traded day", round(traded.n_stocks.mean(), 2)),
        ("Total stock-trades", len(tl)),
        ("Win rate (days positive) %", round((pr > 0).mean() * 100, 1)),
        ("Avg return / day %", round(pr.mean(), 3)),
        ("Avg WIN day %", round(w.mean(), 3)),
        ("Avg LOSS day %", round(l.mean(), 3)),
        ("Best day %", f"{round(pr.max(),3)} on {traded.loc[pr.idxmax(),'date']}"),
        ("Worst day %", f"{round(pr.min(),3)} on {traded.loc[pr.idxmin(),'date']}"),
        ("Max drawdown %", round(dd.min(), 2)),
        ("Per-trade win rate %", round((tl.return_pct > 0).mean() * 100, 1)),
    ], columns=["Metric", "Value"])

    def agg(g):
        p = g.portfolio_return_pct; w = p[p > 0]; l = p[p < 0]
        return pd.Series({"Days": len(g), "Win%": round((p > 0).mean()*100, 1),
                          "Avg/day%": round(p.mean(), 3), "AvgWin%": round(w.mean(), 3) if len(w) else None,
                          "AvgLoss%": round(l.mean(), 3) if len(l) else None,
                          "Avg_stocks": round(g.n_stocks.mean(), 1),
                          "Sum%": round(p.sum(), 1)})
    yearly = traded.groupby("Year").apply(agg, include_groups=False).reset_index()
    monthly = traded.groupby("YM").apply(agg, include_groups=False).reset_index()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
        perf.to_excel(xl, "1_Performance_Summary", index=False)
        yearly.to_excel(xl, "2_Yearly", index=False)
        monthly.to_excel(xl, "3_Monthly", index=False)
        dl.to_excel(xl, "4_Daily_Trade_Log", index=False)
        tl.to_excel(xl, "5_Trade_Journal_Exploded", index=False)

    pd.set_option("display.width", 200)
    print("\n=== SECTOR-CONFIRMED FALCON ENTRY ===")
    print(perf.to_string(index=False))
    print("\n=== YEARLY ===")
    print(yearly.to_string(index=False))
    print(f"\n[*] wrote {OUT}")
    if DESK.exists():
        import shutil; shutil.copy(OUT, DESK / OUT.name); print(f"[*] copied to {DESK/OUT.name}")


if __name__ == "__main__":
    main()
