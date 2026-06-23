"""
Next-morning OPENING features — the Stage-2 confirmation signal (user direction
2026-06-22): an EOD shortlist is confirmed/narrowed the next morning using the
opening gap + early-session (9:15→9:45) momentum, which carry far more directional
information than anything available at the prior EOD.

Sources:
  • overnight GAP — from daily open vs prev close (full history 2016–2026)
  • early-session 1-min momentum + early volume — from ohlc_1min (2024-05→2026-05)

Lookahead discipline: these values are all known by ~09:45 on the OUTCOME day, so
they are legitimately usable to choose the final Top-10 *for that same day's*
forward return — provided we measure the capturable window (09:45→close), not the
full 09:15→close (we report both, and never count the 09:15–09:45 window as skill).

Built into table ``persona_open_features`` keyed by (symbol, trade_date) where
trade_date is the OUTCOME day (the morning we observe).
"""
from __future__ import annotations

import sqlite3
from typing import List, Optional

import numpy as np
import pandas as pd

MINUTES = ("09:15:00", "09:30:00", "09:45:00", "10:00:00")

SCHEMA = """
CREATE TABLE IF NOT EXISTS persona_open_features (
    symbol         TEXT NOT NULL,
    trade_date     TEXT NOT NULL,      -- the OUTCOME morning
    prev_close     REAL,
    day_open       REAL,
    day_close      REAL,
    gap_pct        REAL,               -- open/prev_close-1 (full history)
    ret_o_0930     REAL,               -- 09:15->09:30 (intraday, 2024+)
    ret_o_0945     REAL,               -- 09:15->09:45
    ret_o_1000     REAL,               -- 09:15->10:00
    evol_0945      REAL,               -- first-30min volume / 20d avg daily vol
    ret_0945_close REAL,               -- 09:45->close  (capturable target)
    oc_full        REAL,               -- 09:15->close  (spec metric)
    has_intraday   INTEGER,
    PRIMARY KEY (symbol, trade_date)
)
"""


def _daily(con, symbols, start):
    df = pd.read_sql_query(
        "SELECT symbol, trade_date, open, close, volume FROM ohlc_daily "
        "WHERE quality_flag!='rejected'" + (f" AND trade_date>='{start}'" if start else ""),
        con)
    df = df[df["symbol"].isin(symbols)].sort_values(["symbol", "trade_date"])
    g = df.groupby("symbol", group_keys=False)
    df["prev_close"] = g["close"].shift(1)
    df["gap_pct"] = (df["open"] / df["prev_close"] - 1) * 100
    df["oc_full"] = (df["close"] / df["open"] - 1) * 100
    df["vol20"] = g["volume"].transform(lambda s: s.rolling(20).mean())
    return df


def _intraday_minutes(con, symbols, start):
    qs = ",".join("?" * len(symbols))
    rows = con.execute(
        f"SELECT symbol, bar_time, open, volume FROM ohlc_1min "
        f"WHERE substr(bar_time,12,8) IN {MINUTES} "
        f"AND bar_time>='{start} 00:00:00' AND symbol IN ({qs})",
        list(symbols)).fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["symbol", "bar_time", "open", "volume"])
    df["trade_date"] = df["bar_time"].str[:10]
    df["tod"] = df["bar_time"].str[11:19]
    px = df.pivot_table(index=["symbol", "trade_date"], columns="tod",
                        values="open", aggfunc="first")
    vol = df[df["tod"].isin(["09:15:00", "09:30:00", "09:45:00"])].groupby(
        ["symbol", "trade_date"])["volume"].sum().rename("evol_30m")
    out = px.join(vol)
    out = out.reset_index().rename(columns={
        "09:15:00": "px0915", "09:30:00": "px0930",
        "09:45:00": "px0945", "10:00:00": "px1000"})
    return out


def build_open_features(con: sqlite3.Connection, symbols: List[str],
                        start: str = "2021-01-01",
                        intraday_start: str = "2024-05-13", write: bool = True) -> pd.DataFrame:
    con.execute(SCHEMA)
    daily = _daily(con, symbols, start)
    intra = _intraday_minutes(con, symbols, intraday_start)

    df = daily.merge(intra, on=["symbol", "trade_date"], how="left")
    has_i = df.get("px0915")
    if has_i is None:
        df["px0915"] = np.nan
        for c in ("px0930", "px0945", "px1000", "evol_30m"):
            df[c] = np.nan
    # intraday returns vs 09:15 open (fallback to day_open if px0915 missing)
    base = df["px0915"].fillna(df["open"])
    df["ret_o_0930"] = (df["px0930"] / base - 1) * 100
    df["ret_o_0945"] = (df["px0945"] / base - 1) * 100
    df["ret_o_1000"] = (df["px1000"] / base - 1) * 100
    df["ret_0945_close"] = (df["close"] / df["px0945"] - 1) * 100
    df["evol_0945"] = df["evol_30m"] / df["vol20"].replace(0, np.nan)
    df["has_intraday"] = df["px0945"].notna().astype(int)

    cols = ["symbol", "trade_date", "prev_close", "open", "close", "gap_pct",
            "ret_o_0930", "ret_o_0945", "ret_o_1000", "evol_0945",
            "ret_0945_close", "oc_full", "has_intraday"]
    out = df[cols].rename(columns={"open": "day_open", "close": "day_close"})
    out = out.replace([np.inf, -np.inf], np.nan)
    if write:
        _write(con, out)
    return out


def _write(con, out):
    rows = [tuple(None if pd.isna(v) else v for v in r)
            for r in out.itertuples(index=False, name=None)]
    con.executemany(
        "INSERT OR REPLACE INTO persona_open_features "
        "(symbol,trade_date,prev_close,day_open,day_close,gap_pct,ret_o_0930,"
        "ret_o_0945,ret_o_1000,evol_0945,ret_0945_close,oc_full,has_intraday) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.commit()


if __name__ == "__main__":
    from persona_engine import db, universe
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-22")
    out = build_open_features(con, fo, start="2021-01-01")
    n_i = int(out["has_intraday"].sum())
    print(f"open features: {len(out)} rows, {n_i} with intraday "
          f"({out['trade_date'].min()}..{out['trade_date'].max()})")
    con.close()
