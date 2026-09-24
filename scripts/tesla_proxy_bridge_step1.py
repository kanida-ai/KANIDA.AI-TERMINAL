"""Step 1 of the Tesla->2.8yr bridge: on the 3 order-flow days, compute PURE-OHLCV proxies
and measure how well each tracks the REAL order-flow feature it must replace. Everything is
in the Tesla scores CSV (raw OHLCV + real OF features + forward short returns), so no join.

Tests: (a) proxy<->real correlation per feature; (b) can an OHLCV-only short-lean predict the
forward 15m short move (dir_15m_bps) as well as the real short_drive?  = is the bridge viable?
"""
from pathlib import Path
import numpy as np, pandas as pd
TES = Path(r"C:\Users\SPS\Documents\Kanida Ai\outputs\falcon_tesla")
CSV = TES / "walk_forward_all_cash_scores.csv"
USE = ["instrument", "day", "bar_time", "high", "low", "close", "volume",
       "close_pos", "candle_aggr", "range_bps", "volume_ratio", "vwap_gap_bps", "close_atp_gap_bps",
       "tick_buy%", "net_aggression", "atp_torque", "short_drive", "long_drive",
       "bid_absorption", "ask_absorption", "falcon_phase", "dir_15m_bps", "win_15m"]


def main():
    df = pd.read_csv(CSV, usecols=USE)
    df["bar_time"] = pd.to_datetime(df["bar_time"])
    df = df.sort_values(["instrument", "day", "bar_time"])
    print(f"[*] {len(df):,} rows | {df.instrument.nunique()} instruments | days {sorted(df.day.unique())}")

    # ---- pure-OHLCV proxies ----
    df["px_closeloc"] = ((df.close - df.low) / (df.high - df.low).replace(0, np.nan))       # 0=low,1=high -> aggression
    typ = (df.high + df.low + df.close) / 3.0
    g = df.groupby(["instrument", "day"], sort=False)
    cvol = g["volume"].cumsum(); ctv = g.apply(lambda x: (typ.loc[x.index] * x["volume"]).cumsum()).reset_index(level=[0,1], drop=True)
    df["px_vwap"] = ctv / cvol.replace(0, np.nan)
    df["px_vwapdev_bps"] = (df.close - df.px_vwap) / df.close * 1e4                          # proxy for close_atp_gap / atp_torque
    df["px_absorption"] = df.volume_ratio / (df.range_bps.abs() / 100 + 0.1)                 # hi vol, small range
    df["px_shortlean"] = (-df.px_closeloc.fillna(0.5) + 0.5) * df.volume_ratio.clip(0, 8)    # sell-side pressure proxy

    print("\nPROXY <-> REAL correlation (Pearson, on the 3 overlapping days):")
    pairs = [("px_closeloc", "tick_buy%", "close-location vs tick buy%"),
             ("px_closeloc", "net_aggression", "close-location vs net_aggression"),
             ("candle_aggr", "net_aggression", "candle_aggr(OHLCV) vs net_aggression"),
             ("px_vwapdev_bps", "close_atp_gap_bps", "OHLCV vwap-dev vs close-ATP gap"),
             ("px_vwapdev_bps", "atp_torque", "OHLCV vwap-dev vs atp_torque"),
             ("px_absorption", "bid_absorption", "vol/range vs bid_absorption"),
             ("px_absorption", "ask_absorption", "vol/range vs ask_absorption"),
             ("volume_ratio", "short_drive", "volume_ratio vs short_drive"),
             ("px_shortlean", "short_drive", "OHLCV short-lean vs short_drive")]
    for a, b, lab in pairs:
        d = df[[a, b]].replace([np.inf, -np.inf], np.nan).dropna()
        r = d[a].corr(d[b]) if len(d) > 100 else np.nan
        tag = "  STRONG" if abs(r) >= 0.6 else ("  moderate" if abs(r) >= 0.35 else "  weak")
        print(f"  {lab:<44} r = {r:+.2f}{tag}  (n={len(d):,})")

    # ---- does an OHLCV-only short-lean predict the forward short move as well as real short_drive? ----
    print("\nPREDICTIVE TEST — forward 15m SHORT return (dir_15m_bps>0 = short wins), by signal quintile:")
    valid = df[df.dir_15m_bps.notna()].copy()
    base_wr = (valid.dir_15m_bps > 0).mean() * 100
    base_ret = valid.dir_15m_bps.mean()
    print(f"  base: short-win {base_wr:.1f}% | avg dir {base_ret:+.1f} bps  (n={len(valid):,})")
    for sig, lab in [("short_drive", "REAL short_drive (top quintile)"),
                     ("px_shortlean", "PROXY OHLCV short-lean (top quintile)"),
                     ("px_vwapdev_bps", "PROXY vwap-dev (bottom quintile=below vwap)")]:
        s = valid[sig].replace([np.inf, -np.inf], np.nan)
        if sig == "px_vwapdev_bps":
            m = s <= s.quantile(0.20)
        else:
            m = s >= s.quantile(0.80)
        sub = valid[m]
        print(f"  {lab:<42} short-win {(sub.dir_15m_bps>0).mean()*100:5.1f}% | avg dir {sub.dir_15m_bps.mean():+6.1f} bps | n={len(sub):,}")


if __name__ == "__main__":
    main()
