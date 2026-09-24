"""Layer 1 of the opening-window model: does the 9:15-9:18 window carry market direction,
and does a weak/strong open CONTINUE or REVERSE? Cash 1-min, F&O universe, 2 years.

Per stock-morning: open=09:15 open, early return to 09:18, opening volume (9:15-9:18).
Per morning (breadth): % stocks down vs open at 09:18, and the DOWN-side share of opening
volume. Outcomes: market avg return 09:18->09:30, ->10:00, ->15:29 (equal-weight F&O).
Then: does opening breadth/vol-side predict the rest, and is a strong-down open continuation
or a sweep-reversal?
"""
import sqlite3
from pathlib import Path
import numpy as np
import pandas as pd

DB = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine") / "universe_engine" / "data" / "db" / "kanida_universe.db"
MINS = ("09:15", "09:16", "09:17", "09:18", "09:30", "10:00", "15:29")


def load():
    con = sqlite3.connect(str(DB))
    fo = [r[0] for r in con.execute("SELECT symbol FROM fo_stock_master").fetchall()]
    idx = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}
    fo = [s for s in fo if s not in idx]
    ph = ",".join("?" * len(fo)); mph = ",".join("?" * len(MINS))
    q = (f"SELECT symbol, substr(bar_time,1,10) d, substr(bar_time,12,5) hm, open, close, volume "
         f"FROM ohlc_1min WHERE symbol IN ({ph}) AND substr(bar_time,12,5) IN ({mph})")
    df = pd.read_sql_query(q, con, params=fo + list(MINS))
    con.close()
    return df


def main():
    df = load()
    print(f"loaded {len(df):,} rows | {df.symbol.nunique()} F&O stocks | {df.d.nunique()} mornings")
    op = df[df.hm == "09:15"].set_index(["symbol", "d"]).open.rename("open")
    def cl(m): return df[df.hm == m].set_index(["symbol", "d"]).close.rename(m)
    vol = df[df.hm.isin(["09:15", "09:16", "09:17", "09:18"])].groupby(["symbol", "d"]).volume.sum().rename("vol0918")
    p = pd.concat([op, cl("09:18"), cl("09:30"), cl("10:00"), cl("15:29"), vol], axis=1).dropna(subset=["open", "09:18"])
    p["early"] = p["09:18"] / p.open - 1                     # 09:15 open -> 09:18
    p["r_930"] = p["09:30"] / p["09:18"] - 1                 # 09:18 -> 09:30
    p["r_1000"] = p["10:00"] / p["09:18"] - 1
    p["r_close"] = p["15:29"] / p["09:18"] - 1
    p = p.reset_index()

    # ---- breadth per morning ----
    def agg(g):
        down = g.early < 0
        return pd.Series(dict(
            n=len(g), pct_down=down.mean() * 100,
            down_vol_share=g.loc[down, "vol0918"].sum() / g.vol0918.sum() * 100 if g.vol0918.sum() else np.nan,
            m_930=g.r_930.mean() * 100, m_1000=g.r_1000.mean() * 100, m_close=g.r_close.mean() * 100,
            early_mean=g.early.mean() * 100))
    B = p.groupby("d").apply(agg).reset_index()
    B = B[B.n >= 50]
    print(f"\nmornings with >=50 stocks: {len(B)}")
    print(f"opening breadth (% stocks down vs open at 09:18): mean {B.pct_down.mean():.1f}%  "
          f"[p10 {B.pct_down.quantile(.1):.0f}  p50 {B.pct_down.median():.0f}  p90 {B.pct_down.quantile(.9):.0f}]")

    # ---- does opening breadth predict the rest of the morning / day? ----
    print("\n=== Layer-1: opening breadth (09:18) -> market rest-of-day (equal-weight F&O) ===")
    B["breadth_q"] = pd.qcut(B.pct_down, 5, labels=["Q1 strong-up", "Q2", "Q3", "Q4", "Q5 strong-down"])
    print(f"{'opening breadth bucket':<20}{'n':>5}{'->09:30%':>10}{'->10:00%':>10}{'->close%':>10}{'earlyMove%':>11}")
    for q, g in B.groupby("breadth_q", observed=True):
        print(f"{str(q):<20}{len(g):>5}{g.m_930.mean():>10.3f}{g.m_1000.mean():>10.3f}{g.m_close.mean():>10.3f}{g.early_mean.mean():>11.3f}")

    # ---- CONTINUATION vs REVERSAL on strong-down opens ----
    print("\n=== Continuation vs Reversal: strongest-DOWN opens (top decile % down) ===")
    thr = B.pct_down.quantile(0.9)
    wk = B[B.pct_down >= thr]
    print(f"  {len(wk)} weak-open mornings (>= {thr:.0f}% stocks down). Market 09:18->:")
    print(f"    ->09:30 mean {wk.m_930.mean():+.3f}% (%green {(wk.m_930>0).mean()*100:.0f}%) | "
          f"->10:00 {wk.m_1000.mean():+.3f}% | ->close {wk.m_close.mean():+.3f}% (%green {(wk.m_close>0).mean()*100:.0f}%)")
    st = B[B.pct_down <= B.pct_down.quantile(0.1)]
    print(f"  {len(st)} strong-open mornings (<= {B.pct_down.quantile(0.1):.0f}% down). Market 09:18->close "
          f"{st.m_close.mean():+.3f}% (%green {(st.m_close>0).mean()*100:.0f}%)")

    # ---- does DOWN-VOLUME share add info beyond breadth? ----
    print("\n=== Down-side VOLUME share (is selling volume-backed?) -> rest-of-day ===")
    B["dv_q"] = pd.qcut(B.down_vol_share, 4, labels=["low downvol", "Q2", "Q3", "high downvol"])
    for q, g in B.groupby("dv_q", observed=True):
        print(f"  {str(q):<14} n={len(g):>4}  09:18->close mean {g.m_close.mean():+.3f}%  (%green {(g.m_close>0).mean()*100:.0f}%)")


if __name__ == "__main__":
    main()
