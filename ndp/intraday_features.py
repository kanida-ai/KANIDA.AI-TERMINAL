# -*- coding: utf-8 -*-
"""Previous-day intraday structure features (from 5-min bars, 2024-05+). Computed as-of close of day T
(all information available at T's close) to predict T+1's open->close. Leak-free. These capture the
intraday PATH that daily OHLC cannot see: VWAP position, last-hour drift, close location, breadth, am/pm."""
import os, sqlite3
import numpy as np, pandas as pd
SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "universe_engine", "data", "db", "kanida_universe.db")


def _px_at(g, hm):
    """Close of the bar at/just before time hm (e.g. '10:00')."""
    sub = g[g.hm <= hm]
    return sub.close.iloc[-1] if len(sub) else np.nan


def build(sym):
    con = sqlite3.connect("file:" + SRC.replace("\\", "/") + "?mode=ro", uri=True)
    # 1-min gives full 2024-05+ coverage (ohlc_5min is sparse); scalars are timeframe-agnostic
    b = pd.read_sql_query("SELECT bar_time,open,high,low,close,volume FROM ohlc_1min WHERE symbol=? ORDER BY bar_time",
                          con, params=[sym]); con.close()
    if b.empty:
        return pd.DataFrame(columns=["date"])
    b["date"] = b.bar_time.str[:10]; b["hm"] = b.bar_time.str[11:16]
    b = b[(b.hm >= "09:15") & (b.hm <= "15:30")]
    rows = []
    for d, g in b.groupby("date"):
        g = g.sort_values("bar_time")
        if len(g) < 60: continue
        o, c = g.open.iloc[0], g.close.iloc[-1]; hi, lo = g.high.max(), g.low.min()
        tp = (g.high + g.low + g.close) / 3
        vwap = (tp * g.volume).sum() / max(g.volume.sum(), 1)
        px1000, px1215, px1430 = _px_at(g, "10:00"), _px_at(g, "12:15"), _px_at(g, "14:30")
        r15 = g.set_index("hm").close.iloc[::15]                       # 15-min grid for breadth
        up = (r15.diff() > 0).sum(); dn = (r15.diff() < 0).sum(); nb = max(len(r15) - 1, 1)
        am_vol = g[g.hm < "12:15"].volume.sum(); pm_vol = g[g.hm >= "12:15"].volume.sum()
        rows.append(dict(
            date=d,
            id_vwap_dev=(c / vwap - 1) * 100,                          # close vs session VWAP
            id_close_pos=(c - lo) / (hi - lo + 1e-9),                  # where it closed in the day's range 0..1
            id_range=(hi - lo) / c * 100,                              # intraday range (vol regime)
            id_morning_ret=(px1000 / o - 1) * 100,                     # first ~45 min drive
            id_afternoon_ret=(c / px1215 - 1) * 100 if px1215 else np.nan,
            id_lasthour_ret=(c / px1430 - 1) * 100 if px1430 else np.nan,  # smart-money close drift
            id_breadth=(up - dn) / max(nb, 1),                         # up vs down 5-min bars
            id_above_vwap_frac=(g.close > vwap).mean(),                # % of day above VWAP
            id_ampm_vol=am_vol / (pm_vol + 1e-9),                      # morning vs afternoon volume
        ))
    return pd.DataFrame(rows)


if __name__ == "__main__":
    df = build("ICICIBANK")
    print(f"intraday features: {len(df)} sessions  {df.date.min()} -> {df.date.max()}")
    print(df.describe().round(3).to_string())
