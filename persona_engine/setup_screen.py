"""
Setup screener (operator 2026-06-23): test many morning setups/filters and rank by
precision@5 toward the targets. Each setup = a per-day ranking rule over morning
(9:15-10:00) features; pick top-5; measure how many hit the move (10:00->15:15).

Setups cover: momentum, REVERSAL, FLAT/coiled, HIGH-VOLUME, HIGH-VOLATILITY,
ACCUMULATION (volume + no price move), breakout/near-high, VWAP, market-activity.
Targets reported: directional and magnitude, at +/-1% and +/-2%.
"""
from __future__ import annotations
import numpy as np, pandas as pd
from persona_engine import db, universe
from persona_engine.intraday_eod import morning_features

NPICK = 5


def prep(con, fo):
    mf = morning_features(con, fo).dropna(subset=["m_ret", "post_move"])
    # add a couple of daily-context cols
    d = pd.read_sql_query("SELECT symbol,trade_date,atr_20_pct,vol_ratio_20d,rs_index_20d "
                          "FROM persona_signal_features WHERE symbol IN (%s)" % ",".join("?"*len(fo)),
                          con, params=fo).sort_values(["symbol", "trade_date"])
    d["date"] = d.groupby("symbol")["trade_date"].shift(-1)
    mf = mf.merge(d[["symbol", "date", "atr_20_pct", "vol_ratio_20d", "rs_index_20d"]],
                  on=["symbol", "date"], how="left")
    # market activity context per day
    mf["mkt_disp"] = mf.groupby("date")["m_ret"].transform("std")
    return mf


def screen(mf, name, score_col, target, direction, ascending=False):
    """target: 'mag' => |move|>=X ; 'long' => move>=X ; 'short' => move<=-X.
    Pick top-5 by score (desc unless ascending). Return (p@5@1%, p@5@2%)."""
    res1, res2 = [], []
    for dt, g in mf.groupby("date"):
        g = g.dropna(subset=[score_col])
        if len(g) < 30:
            continue
        picks = g.sort_values(score_col, ascending=ascending).head(NPICK)
        m = picks["post_move"]
        if target == "mag":
            res1.append((m.abs() >= 1).sum()); res2.append((m.abs() >= 2).sum())
        elif target == "long":
            res1.append((m >= 1).sum()); res2.append((m >= 2).sum())
        else:
            res1.append((m <= -1).sum()); res2.append((m <= -2).sum())
    return round(np.mean(res1), 2), round(np.mean(res2), 2)


def run(con, fo):
    mf = prep(con, fo)
    mf["abs_mret"] = mf["m_ret"].abs()
    mf["accum"] = mf["m_vol"].rank(pct=True) - mf["m_range"].rank(pct=True)  # high vol, low range
    print(f"rows={len(mf)} dates={mf['date'].nunique()}\n")
    print(f"{'SETUP':38s} {'target':5s}  p@5(1%)  p@5(2%)")
    setups = [
        # magnitude (will it MOVE at all, either way)
        ("rank morning volatility (mag)", "m_volat", "mag", "", False),
        ("rank morning volume (mag)", "m_vol", "mag", "", False),
        ("rank recent ATR (mag)", "atr_20_pct", "mag", "", False),
        ("rank market activity disp (mag)", "mkt_disp", "mag", "", False),
        ("rank |morning move| (mag)", "abs_mret", "mag", "", False),
        ("FLAT: low morning range (mag)", "m_range", "mag", "", True),
        ("ACCUMULATION vol+flat (mag)", "accum", "mag", "", False),
        # directional LONG
        ("MOMENTUM long: morning gainers", "m_ret", "long", "L", False),
        ("REVERSAL long: morning losers", "m_ret", "long", "L", True),
        ("near morning high (long)", "m_loc", "long", "L", False),
        ("above VWAP (long)", "m_vwap_dev", "long", "L", False),
        ("high vol + up (long)", "m_vol", "long", "L", False),
        # directional SHORT
        ("MOMENTUM short: morning losers", "m_ret", "short", "S", True),
        ("REVERSAL short: morning gainers", "m_ret", "short", "S", False),
        ("below VWAP (short)", "m_vwap_dev", "short", "S", True),
    ]
    rows = []
    for name, col, tgt, _d, asc in setups:
        p1, p2 = screen(mf, name, col, tgt, _d, ascending=asc)
        rows.append((name, tgt, p1, p2))
        print(f"{name:38s} {tgt:5s}   {p1:>5}    {p2:>5}", flush=True)
    print("\nBenchmark: full ML model magnitude p@5(1%)=2.8 ; directional p@5(1%)=1.3")
    best = max(rows, key=lambda r: r[2])
    print(f"BEST setup by p@5(1%): {best[0]} ({best[1]}) -> {best[2]}/5 @1%, {best[3]}/5 @2%")


if __name__ == "__main__":
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-23")
    run(con, fo)
    con.close()
    print("SCREEN_DONE")
