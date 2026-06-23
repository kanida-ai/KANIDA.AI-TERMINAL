"""
First-hour BLOCK engine (operator spec 2026-06-23).

Builds per (symbol, trade_date) features from the first-hour 15-min blocks:
  b1 = 09:15-09:30, b2 = 09:30-09:45, b3 = 09:45-10:00 (+ b4 10:00-10:15 reserved)
Per block: return, range, volume, vwap-dev, close-location, up-bar share, max excursion.
Cross-block structure: momentum acceleration, volume expansion/contraction, compression,
ignition, breakout vs opening range, consolidation, trend-consistency/reversal.
Plus prior-day daily context + market/sector regime.

Outcomes (10:00 entry -> measured to 15:15): up_touch%, dn_touch%, ret_close%.

Persisted to table `block_features` so the split-experiments + walk-forward run fast.
Build once:  python -m persona_engine.block_engine build
"""
from __future__ import annotations
import numpy as np, pandas as pd
from persona_engine import db, universe
from persona_engine.model import ALL_FEATURES

BLOCKS = {"b1": ("09:15", "09:29"), "b2": ("09:30", "09:44"), "b3": ("09:45", "09:59")}


def _load_firsthour(con, fo):
    rows = con.execute(
        "SELECT symbol, bar_time, open, high, low, close, volume FROM ohlc_1min "
        "WHERE substr(bar_time,12,5) >= '09:15' AND substr(bar_time,12,5) <= '10:00' "
        "AND symbol IN (%s)" % ",".join("?"*len(fo)), list(fo)).fetchall()
    m = pd.DataFrame(rows, columns=["symbol", "bar_time", "o", "h", "l", "c", "v"])
    m["date"] = m["bar_time"].str[:10]; m["tod"] = m["bar_time"].str[11:16]
    m["ret"] = m.groupby(["symbol", "date"])["c"].pct_change()
    return m


def _block_feats(m):
    out = {}
    for bn, (lo, hi) in BLOCKS.items():
        b = m[(m["tod"] >= lo) & (m["tod"] <= hi)]
        g = b.groupby(["symbol", "date"])
        o = g["o"].first(); c = g["c"].last(); h = g["h"].max(); l = g["l"].min()
        vwap = (b.assign(cv=b["c"]*b["v"]).groupby(["symbol", "date"])["cv"].sum() /
                g["v"].sum().replace(0, np.nan))
        f = pd.DataFrame({
            f"{bn}_ret": (c/o-1)*100,
            f"{bn}_range": (h-l)/o*100,
            f"{bn}_vol": g["v"].sum(),
            f"{bn}_vwapdev": (c/vwap-1)*100,
            f"{bn}_loc": (c-l)/(h-l).replace(0, np.nan),
            f"{bn}_upbars": g["ret"].apply(lambda s: (s > 0).mean()),
            f"{bn}_hi": h, f"{bn}_lo": l, f"{bn}_c": c,
        })
        out[bn] = f
    df = out["b1"].join(out["b2"]).join(out["b3"])
    # cross-block structure
    df["accel"] = df["b3_ret"] - df["b1_ret"]                       # momentum ignition
    df["vol_expand"] = df["b3_vol"] / df["b1_vol"].replace(0, np.nan)
    df["compression"] = df["b3_range"] / df["b1_range"].replace(0, np.nan)  # <1 = tightening
    df["cum_ret"] = (df["b3_c"]/df.get("b1_c") - 1)*100 if "b1_c" in df else np.nan
    df["px_1000"] = df["b3_c"]
    df["orb_hi"] = df[["b1_hi", "b2_hi"]].max(axis=1); df["orb_lo"] = df[["b1_lo", "b2_lo"]].min(axis=1)
    df["breakout_up"] = (df["b3_c"] > df["orb_hi"]).astype(int)
    df["breakout_dn"] = (df["b3_c"] < df["orb_lo"]).astype(int)
    df["trend_consist"] = np.sign(df["b1_ret"]).fillna(0) + np.sign(df["b2_ret"]).fillna(0) + np.sign(df["b3_ret"]).fillna(0)
    df["reversal"] = ((np.sign(df["b1_ret"]) != np.sign(df["b3_ret"])) & df["b3_ret"].notna()).astype(int)
    df["cum_vol"] = df["b1_vol"]+df["b2_vol"]+df["b3_vol"]
    df["morning_vol"] = m.groupby(["symbol", "date"])["ret"].std().mul(100)
    return df.reset_index()


def _outcomes(con, fo):
    rows = con.execute(
        "SELECT symbol, substr(bar_time,1,10) d, "
        "MIN(CASE WHEN substr(bar_time,12,5)='10:00' THEN open END) entry, "
        "MAX(high) hi, MIN(low) lo, MAX(CASE WHEN substr(bar_time,12,5)='15:15' THEN close END) c "
        "FROM ohlc_1min WHERE substr(bar_time,12,5)>='10:00' AND substr(bar_time,12,5)<='15:15' "
        "AND symbol IN (%s) GROUP BY symbol,d" % ",".join("?"*len(fo)), list(fo)).fetchall()
    t = pd.DataFrame(rows, columns=["symbol", "date", "entry", "hi", "lo", "c1515"]).dropna(subset=["entry"])
    t["up_touch"] = (t["hi"]/t["entry"]-1)*100
    t["dn_touch"] = (t["lo"]/t["entry"]-1)*100
    t["ret_close"] = (t["c1515"]/t["entry"]-1)*100
    return t[["symbol", "date", "up_touch", "dn_touch", "ret_close"]]


def build(con, fo):
    m = _load_firsthour(con, fo)
    bf = _block_feats(m)
    sect = {r["symbol"]: (r["sector"] or "UNK") for r in con.execute("SELECT symbol,sector FROM falcon_sectors")}
    bf["sector"] = bf["symbol"].map(sect).fillna("UNK")
    g = bf.groupby("date")["b3_ret"]
    bf["mkt_ret"] = g.transform("mean"); bf["mkt_breadth"] = g.transform(lambda s: (s > 0).mean())
    bf["mkt_disp"] = g.transform("std"); bf["rel_mkt"] = bf["b3_ret"] - bf["mkt_ret"]
    bf["rel_sec"] = bf["b3_ret"] - bf.groupby(["date", "sector"])["b3_ret"].transform("mean")
    daily = pd.read_sql_query("SELECT * FROM persona_signal_features WHERE symbol IN (%s)"
                              % ",".join("?"*len(fo)), con, params=fo).sort_values(["symbol", "trade_date"])
    daily["date"] = daily.groupby("symbol")["trade_date"].shift(-1)
    dcols = [f for f in ALL_FEATURES if f in daily.columns]
    bf = bf.merge(daily[["symbol", "date"]+dcols], on=["symbol", "date"], how="left")
    out = _outcomes(con, fo)
    bf = bf.merge(out, on=["symbol", "date"], how="inner").dropna(subset=["up_touch", "px_1000"])
    # persist
    drop = [c for c in ["b1_hi", "b1_lo", "b1_c", "b2_hi", "b2_lo", "b2_c", "b3_hi", "b3_lo", "b3_c",
                        "orb_hi", "orb_lo"] if c in bf.columns]
    bf = bf.drop(columns=drop)
    bf.to_sql("block_features", con, if_exists="replace", index=False)
    print(f"block_features built: {len(bf)} rows, {bf['date'].nunique()} dates "
          f"({bf['date'].min()}..{bf['date'].max()}), {bf.shape[1]} cols")
    return bf


if __name__ == "__main__":
    import sys
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-23")
    build(con, fo)
    con.close()
    print("BLOCKBUILD_DONE")
