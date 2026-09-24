"""Exploratory data analysis on ohlc_futures_1min (real NFO stock-futures 1-min + OI).
Goal: find what edge the FUTURES data supports that equity can't give us — OI positioning,
free shorting, intraday structure — and propose a strategy.

Window is ~2 months (Apr29-Jul3), front (July) contract. July is the nearest-expiry for the
whole window, but it is the FAR month early on (thin) and becomes the liquid near-month from
~June; so we lean on liquidity filters and cross-sectional (210-stock) power, and flag the
short history. All returns are on the FUTURES contract price. net-of-cost not applied here
(EDA of raw edges); costs discussed in the proposal.
"""
import sqlite3
from pathlib import Path
import numpy as np
import pandas as pd

DB = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine") / "universe_engine" / "data" / "db" / "kanida_universe.db"
FRONT = "2026-07-28"


def build_panel():
    con = sqlite3.connect(str(DB))
    df = pd.read_sql_query(
        "SELECT symbol, bar_time, open, high, low, close, volume, oi FROM ohlc_futures_1min "
        "WHERE expiry=? ", con, params=(FRONT,))
    con.close()
    df["date"] = df.bar_time.str[:10]; df["hm"] = df.bar_time.str[11:16]
    df = df.sort_values(["symbol", "bar_time"])
    g = df.groupby(["symbol", "date"], sort=False)
    p = g.agg(opn=("open", "first"), cls=("close", "last"), hi=("high", "max"),
              lo=("low", "min"), vol=("volume", "sum"), oi_last=("oi", "last"),
              oi_first=("oi", "first"), nbar=("close", "size")).reset_index()
    c0929 = df[df.hm == "09:29"].groupby(["symbol", "date"]).close.last().rename("c0929")
    p = p.merge(c0929, on=["symbol", "date"], how="left")
    p = p[p.nbar >= 300]                         # full sessions only
    p["turnover"] = p.vol * p.cls                # rupee turnover proxy
    p["day_ret"] = p.cls / p.opn - 1             # open->close (futures)
    p["first15"] = p.c0929 / p.opn - 1
    p["rest"] = p.cls / p.c0929 - 1
    p["intraday_oi"] = p.oi_last / p.oi_first - 1
    p = p.sort_values(["symbol", "date"])
    p["prev_cls"] = p.groupby("symbol").cls.shift(1)
    p["prev_oi"] = p.groupby("symbol").oi_last.shift(1)
    p["gap"] = p.opn / p.prev_cls - 1
    p["oi_chg"] = p.oi_last / p.prev_oi - 1       # day-over-day OI change
    p["ret_co"] = p.cls / p.prev_cls - 1          # close-to-close
    p["next_day_ret"] = p.groupby("symbol").day_ret.shift(-1)   # next session open->close
    p["next_co"] = p.groupby("symbol").ret_co.shift(-1)
    return p


def qstat(x):
    x = np.asarray(x, float); x = x[np.isfinite(x)]
    if len(x) < 5: return (np.nan, np.nan, 0)
    return (x.mean() * 100, x.mean() / (x.std(ddof=1) / np.sqrt(len(x))), len(x))


def main():
    p = build_panel()
    dates = sorted(p.date.unique())
    print(f"PANEL: {len(p):,} stock-days | {p.symbol.nunique()} stocks | {dates[0]}..{dates[-1]} ({len(dates)} days)\n")

    # ---- liquidity ramp (July contract becomes liquid near expiry) ----
    print("Liquidity by week (median stock turnover, Cr) — shows the front-month ramp:")
    p["wk"] = pd.to_datetime(p.date).dt.isocalendar().week
    for wk, gg in p.groupby("wk"):
        print(f"  wk{wk}: median turnover {gg.turnover.median()/1e7:6.1f} Cr | stock-days {len(gg)}")
    # focus universe = liquid stock-days (turnover above cross-sectional median that day)
    p["liq"] = p.groupby("date").turnover.transform(lambda s: s >= s.median())
    L = p[p.liq].copy()
    print(f"\nLiquid subset (>= daily median turnover): {len(L):,} stock-days\n")

    # ---- 1) OI-PRICE quadrants -> next-day return (the futures-specific signal) ----
    print("=== 1) OI-PRICE FRAMEWORK: today's (price dir x OI dir) -> NEXT-day open->close ===")
    def quad(r):
        if r.ret_co > 0 and r.oi_chg > 0: return "long_buildup"
        if r.ret_co < 0 and r.oi_chg > 0: return "short_buildup"
        if r.ret_co > 0 and r.oi_chg < 0: return "short_cover"
        if r.ret_co < 0 and r.oi_chg < 0: return "long_unwind"
        return "flat"
    L["quad"] = L.apply(quad, axis=1)
    print(f"{'quadrant':<16}{'n':>7}{'next-day mean%':>16}{'t-stat':>9}")
    for q in ["long_buildup", "short_buildup", "short_cover", "long_unwind"]:
        m, t, n = qstat(L[L.quad == q].next_day_ret)
        print(f"{q:<16}{n:>7}{m:>16.3f}{t:>9.2f}")

    # ---- 2) cross-sectional OI-change signal ----
    print("\n=== 2) CROSS-SECTIONAL daily OI%-change quintile -> next-day open->close ===")
    L["oi_q"] = L.groupby("date").oi_chg.transform(lambda s: pd.qcut(s.rank(method="first"), 5, labels=False) if s.notna().sum() >= 10 else np.nan)
    print(f"{'OI-chg quintile':<16}{'n':>7}{'next-day mean%':>16}{'t-stat':>9}")
    for qi in range(5):
        m, t, n = qstat(L[L.oi_q == qi].next_day_ret)
        print(f"{['Q1 low','Q2','Q3','Q4','Q5 high'][qi]:<16}{n:>7}{m:>16.3f}{t:>9.2f}")

    # ---- 3) cross-sectional daily momentum/reversal ----
    print("\n=== 3) CROSS-SECTIONAL prior-day return quintile -> next-day open->close (mom vs reversal) ===")
    L["ret_q"] = L.groupby("date").ret_co.transform(lambda s: pd.qcut(s.rank(method="first"), 5, labels=False) if s.notna().sum() >= 10 else np.nan)
    print(f"{'prior-ret quintile':<18}{'n':>7}{'next-day mean%':>16}{'t-stat':>9}")
    for qi in range(5):
        m, t, n = qstat(L[L.ret_q == qi].next_day_ret)
        print(f"{['Q1 losers','Q2','Q3','Q4','Q5 winners'][qi]:<18}{n:>7}{m:>16.3f}{t:>9.2f}")

    # ---- 4) opening-range: first-15min -> rest-of-day (intraday continuation?) ----
    print("\n=== 4) OPENING RANGE (intraday): first-15min sign -> rest-of-day (09:30->15:29) ===")
    for lab, sub in [("first15 > +0.5%", L[L.first15 > 0.005]), ("first15 < -0.5%", L[L.first15 < -0.005]),
                     ("|first15| <= 0.5%", L[L.first15.abs() <= 0.005])]:
        m, t, n = qstat(sub.rest)
        print(f"  {lab:<18} n={n:>6}  rest-of-day mean {m:+.3f}%  t={t:.2f}")

    # ---- 5) overnight gap -> intraday (fade or follow?) ----
    print("\n=== 5) OVERNIGHT GAP -> intraday open->close (fade or follow?) ===")
    for lab, sub in [("gap > +1%", L[L.gap > 0.01]), ("gap < -1%", L[L.gap < -0.01]),
                     ("|gap| <= 1%", L[L.gap.abs() <= 0.01])]:
        m, t, n = qstat(sub.day_ret)
        print(f"  {lab:<14} n={n:>6}  intraday mean {m:+.3f}%  t={t:.2f}")

    # ---- 6) long/short base rates ----
    print("\n=== 6) BASE RATES (liquid subset) ===")
    print(f"  intraday open->close: mean {L.day_ret.mean()*100:+.3f}% | %up {(L.day_ret>0).mean()*100:.1f}% | std {L.day_ret.std()*100:.2f}%")
    print(f"  overnight gap:        mean {L.gap.mean()*100:+.3f}% | %up {(L.gap>0).mean()*100:.1f}%")
    print(f"  close-to-close:       mean {L.ret_co.mean()*100:+.3f}% | %up {(L.ret_co>0).mean()*100:.1f}%")


if __name__ == "__main__":
    main()
