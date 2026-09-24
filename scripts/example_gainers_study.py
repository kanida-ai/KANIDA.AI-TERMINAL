"""July-8 GAINERS impulse + order-flow study. Does the winning-long share a fingerprint,
and — critically — does the order book distinguish a REAL gainer from a NAUKRI-style
fade-trap? Also: how much of the gain was available AFTER 10:15 (tradeable) vs already done."""
import sqlite3, json
from pathlib import Path
import numpy as np
ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
DAY = "2026-07-08"


def main():
    con = sqlite3.connect(str(DB), timeout=90); con.execute("PRAGMA query_only=1")
    # day return (open->close) + turnover for all July-8 symbols
    rows = con.execute(
        "SELECT symbol, "
        " MAX(CASE WHEN substr(bar_time,12,8)='09:15:00' THEN open END) o, "
        " MAX(CASE WHEN substr(bar_time,12,5)='10:15' THEN close END) p1015, "
        " MAX(CASE WHEN substr(bar_time,12,8)='15:29:00' THEN close END) c, "
        " SUM(volume*close) turnover "
        "FROM ohlc_1min WHERE bar_time BETWEEN ? AND ? GROUP BY symbol",
        (DAY + " 09:15:00", DAY + " 15:29:00")).fetchall()
    df = [(s, o, p, c, t) for s, o, p, c, t in rows if o and c and p and o > 0 and t and t > 5e7]  # >Rs5Cr turnover
    dr = sorted(df, key=lambda x: x[3] / x[1] - 1, reverse=True)
    gainers = dr[:15]; losers = dr[-15:]

    # book buy% per symbol (full-day + first-hour) from order flow
    def bookpct(sym, upto=None):
        q = "SELECT total_buy_qty,total_sell_qty FROM mkt_orderflow_1min WHERE symbol=? AND segment='CASH' AND bar_time BETWEEN ? AND ?"
        end = DAY + " " + (upto or "15:30")
        r = con.execute(q, (sym, DAY + " 09:15", end)).fetchall()
        b = sum(x[0] for x in r if x[0]); s = sum(x[1] for x in r if x[1])
        return b / (b + s) * 100 if (b + s) else np.nan

    print(f"TOP 15 GAINERS — July 8 (liquid, >Rs5Cr turnover)\n")
    print(f"{'symbol':<12}{'day%':>7}{'by10:15%':>10}{'aft10:15%':>11}{'%gain early':>13}{'book buy%':>11}{'1h buy%':>9}")
    gb = []
    for s, o, p, c, t in gainers:
        dayr = (c / o - 1) * 100; early = (p / o - 1) * 100; fwd = (c / p - 1) * 100
        pctearly = early / dayr * 100 if dayr else 0
        bk = bookpct(s); bk1 = bookpct(s, "10:15"); gb.append(bk)
        print(f"{s:<12}{dayr:>6.2f}%{early:>9.2f}%{fwd:>10.2f}%{pctearly:>11.0f}%{bk:>10.1f}%{bk1:>8.1f}%")

    lb = [bookpct(s) for s, *_ in losers]
    print(f"\nORDER-BOOK BUY% — the discriminator test:")
    print(f"  top-15 GAINERS avg book buy%: {np.nanmean(gb):.1f}%   (median {np.nanmedian(gb):.1f}%)")
    print(f"  bot-15 LOSERS  avg book buy%: {np.nanmean(lb):.1f}%")
    print(f"  NAUKRI (fade-trap): 41.9%   JIOFIN (loser): 41.3%")

    # tradeability: fraction of gainers whose gain mostly came AFTER 10:15
    aft = [( (c/p-1) > (p/o-1) ) for s,o,p,c,t in gainers]
    print(f"\n  gainers whose move was mostly AFTER 10:15 (tradeable from a 1h signal): {sum(aft)}/15")
    con.close()


if __name__ == "__main__":
    main()
