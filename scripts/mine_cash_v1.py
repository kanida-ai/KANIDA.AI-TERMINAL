"""Behavioral alpha mining v1 — CASH universe, 2.5 yr, HINDSIGHT-FREE.
Decision time t = 10:15 (first 60 min observed). Features use ONLY [09:15, t]; outcomes
use ONLY (t, close]. So every 'signal' is tradeable in real time. Control = the full
universe base rate. Tests the core hypothesis: does early impulse / liquidity-sweep /
relative-volume behavior in the first hour PREDICT the forward move, above base rate?
"""
import sqlite3, pickle
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
CACHE = ROOT / "docs" / "ops" / "_cash_intraday_v1.pkl"
T = "10:15"


def extract():
    con = sqlite3.connect(str(DB), timeout=120); con.execute("PRAGMA query_only=1")
    q = f"""
    SELECT symbol, substr(bar_time,1,10) d,
      MAX(CASE WHEN substr(bar_time,12,8)='09:15:00' THEN open END) o915,
      MAX(CASE WHEN substr(bar_time,12,5)='{T}' THEN close END) pt,
      MAX(CASE WHEN substr(bar_time,12,8)='15:29:00' THEN close END) eod,
      MAX(CASE WHEN substr(bar_time,12,8)='15:28:00' THEN close END) eod2,
      MAX(CASE WHEN substr(bar_time,12,5)<='{T}' THEN high END) ehi,
      MIN(CASE WHEN substr(bar_time,12,5)<='{T}' THEN low  END) elo,
      SUM(CASE WHEN substr(bar_time,12,5)<='{T}' THEN volume ELSE 0 END) vol_early,
      SUM(CASE WHEN substr(bar_time,12,5)>'{T}'  THEN volume ELSE 0 END) vol_rest,
      MAX(CASE WHEN substr(bar_time,12,5)>'{T}'  THEN high END) hi_after,
      MIN(CASE WHEN substr(bar_time,12,5)>'{T}'  THEN low  END) lo_after
    FROM ohlc_1min GROUP BY symbol, substr(bar_time,1,10)"""
    df = pd.read_sql_query(q, con); con.close()
    df["eod"] = df["eod"].fillna(df["eod2"])
    df = df.dropna(subset=["o915", "pt", "eod", "hi_after"])
    df = df[(df.o915 > 0) & (df.pt > 0)]
    pickle.dump(df, open(CACHE, "wb"))
    return df


def main():
    if CACHE.exists():
        df = pickle.load(open(CACHE, "rb"))
    else:
        print("[*] extracting 2.5yr cash per-stock-day (one big scan)...", flush=True)
        df = extract()
    print(f"[*] {len(df):,} stock-days | {df.symbol.nunique()} symbols | {df.d.min()}..{df.d.max()}\n")

    df["early_ret"] = df.pt / df.o915 - 1
    df["early_range"] = (df.ehi - df.elo) / df.o915
    df["early_dip"] = df.elo / df.o915 - 1                       # lowest point of first hour
    df["fwd_ret"] = df.eod / df.pt - 1                           # t -> close
    df["fwd_mfe"] = df.hi_after / df.pt - 1                      # forward max-favourable
    df["fwd_mae"] = df.lo_after / df.pt - 1
    # relative volume: early volume vs the stock's OWN median early volume (no look-ahead within-day)
    med = df.groupby("symbol")["vol_early"].transform("median")
    df["relvol"] = df.vol_early / med.replace(0, np.nan)
    df = df.dropna(subset=["relvol", "fwd_ret"])

    N = len(df)
    def rate(mask, col="fwd_mfe", th=0.02):
        return (df.loc[mask, col] >= th).mean() * 100
    print("CONTROL (base rate, ALL stock-days):")
    print(f"  mean fwd_ret {df.fwd_ret.mean()*100:+.3f}% | P(fwd_MFE>=+1%) {rate(df.index==df.index,'fwd_mfe',0.01):.1f}% | "
          f"P(fwd_MFE>=+2%) {rate(df.index==df.index,'fwd_mfe',0.02):.1f}% | P(fwd_ret>0) {(df.fwd_ret>0).mean()*100:.1f}%\n")

    def buckets(col, edges, labels):
        print(f"By {col} (early, first 60min):")
        print(f"  {'bucket':<16}{'n':>8}{'meanFwdRet':>12}{'P(MFE>=1%)':>12}{'P(MFE>=2%)':>12}{'P(fwd>0)':>10}")
        for lab, lo, hi in zip(labels, edges[:-1], edges[1:]):
            m = (df[col] > lo) & (df[col] <= hi)
            if m.sum() < 50: continue
            print(f"  {lab:<16}{m.sum():>8}{df.loc[m,'fwd_ret'].mean()*100:>11.3f}%{rate(m,'fwd_mfe',0.01):>11.1f}%"
                  f"{rate(m,'fwd_mfe',0.02):>11.1f}%{(df.loc[m,'fwd_ret']>0).mean()*100:>9.1f}%")
        print()
    buckets("early_ret", [-9, -0.02, -0.005, 0.005, 0.02, 9], ["< -2%", "-2..-0.5%", "-0.5..+0.5%", "+0.5..+2%", "> +2%"])
    buckets("relvol", [0, 0.7, 1.3, 2.5, 5, 999], ["<0.7x", "0.7-1.3x", "1.3-2.5x", "2.5-5x", ">5x"])

    # 2D: the user's core hypothesis — high early impulse AND high relvol
    print("2D — high early impulse (>+1%) x relative volume:")
    print(f"  {'relvol bucket':<16}{'n':>8}{'meanFwdRet':>12}{'P(MFE>=2%)':>12}")
    up = df[df.early_ret > 0.01]
    for lab, lo, hi in [("<1.3x", 0, 1.3), ("1.3-2.5x", 1.3, 2.5), (">2.5x", 2.5, 999)]:
        m = (up.relvol > lo) & (up.relvol <= hi)
        if m.sum() < 30: continue
        print(f"  {lab:<16}{m.sum():>8}{up.loc[m,'fwd_ret'].mean()*100:>11.3f}%{(up.loc[m,'fwd_mfe']>=0.02).mean()*100:>11.1f}%")

    # liquidity-sweep-and-recover proxy: dipped >=0.5% below open in first hour, but back above open by t
    print("\nLIQUIDITY-SWEEP proxy — dipped >=0.5% below 09:15 open, recovered above open by 10:15:")
    swept = (df.early_dip <= -0.005) & (df.early_ret > 0)
    nosweep_up = (df.early_dip > -0.005) & (df.early_ret > 0)
    print(f"  {'group':<22}{'n':>8}{'meanFwdRet':>12}{'P(MFE>=2%)':>12}{'P(fwd>0)':>10}")
    for lab, m in [("swept+recovered", swept), ("clean up (no sweep)", nosweep_up)]:
        print(f"  {lab:<22}{m.sum():>8}{df.loc[m,'fwd_ret'].mean()*100:>11.3f}%{(df.loc[m,'fwd_mfe']>=0.02).mean()*100:>11.1f}%{(df.loc[m,'fwd_ret']>0).mean()*100:>9.1f}%")


if __name__ == "__main__":
    main()
