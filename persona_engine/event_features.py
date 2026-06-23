"""
Build the NEW non-price event/conviction features into one table the engine joins:
``persona_event_features`` keyed by (symbol, trade_date).

  • earn_today   : results scheduled on this trading day (the morning)
  • earn_next1   : results scheduled on the NEXT trading day — known at EOD today,
                   so usable to seed tomorrow's shortlist (no lookahead)
  • earn_recent2 : results in the last 2 trading days (post-results drift/vol)
  • deliv_pct    : delivery % (conviction) for this day
  • deliv_z20    : delivery % z-scored vs trailing 20d
  • accum        : deliv_z20 signed by the day's return (accumulation proxy)
"""
from __future__ import annotations

import sqlite3
from typing import List

import numpy as np
import pandas as pd

SCHEMA = """
CREATE TABLE IF NOT EXISTS persona_event_features (
    symbol      TEXT NOT NULL,
    trade_date  TEXT NOT NULL,
    earn_today  INTEGER DEFAULT 0,
    earn_next1  INTEGER DEFAULT 0,
    earn_recent2 INTEGER DEFAULT 0,
    deliv_pct   REAL,
    deliv_z20   REAL,
    accum       REAL,
    PRIMARY KEY (symbol, trade_date)
)"""


def build(con: sqlite3.Connection, start: str = "2022-01-01") -> int:
    con.execute(SCHEMA)
    cal = [r[0] for r in con.execute(
        "SELECT DISTINCT trade_date FROM ohlc_daily WHERE trade_date>=? ORDER BY trade_date",
        (start,))]
    pos = {d: i for i, d in enumerate(cal)}

    # earnings -> map each result_date to the trading day on/after it
    earn = pd.read_sql_query("SELECT symbol, result_date FROM corp_earnings_dates", con)
    arr = np.array(cal)
    def to_trading(d):
        i = np.searchsorted(arr, d)
        return cal[i] if i < len(cal) else None
    earn["tday"] = earn["result_date"].map(to_trading)
    earn = earn.dropna(subset=["tday"])
    earn_today = set(zip(earn["symbol"], earn["tday"]))

    # build per (symbol, trade_date) earnings flags
    # earn_next1 on day D = earnings on cal[pos[D]+1]; earn_recent2 = earnings on D-1 or D-2
    sig = pd.read_sql_query(
        "SELECT symbol, trade_date, sig_ret_pct FROM persona_signal_features WHERE trade_date>=?",
        con, params=[start])
    deliv = pd.read_sql_query(
        "SELECT symbol, trade_date, deliv_pct FROM delivery_daily WHERE trade_date>=?",
        con, params=[start])

    df = sig.merge(deliv, on=["symbol", "trade_date"], how="left")
    df = df[df["trade_date"].isin(pos)]
    # delivery rolling z
    df = df.sort_values(["symbol", "trade_date"])
    g = df.groupby("symbol", group_keys=False)
    mu = g["deliv_pct"].transform(lambda s: s.rolling(20, min_periods=5).mean())
    sd = g["deliv_pct"].transform(lambda s: s.rolling(20, min_periods=5).std())
    df["deliv_z20"] = (df["deliv_pct"] - mu) / sd.replace(0, np.nan)
    df["accum"] = df["deliv_z20"] * np.sign(df["sig_ret_pct"].fillna(0))

    # earnings flags (vectorised via mapping)
    idx = df["trade_date"].map(pos)
    next_day = idx.add(1).map(lambda i: cal[i] if 0 <= i < len(cal) else None)
    prev1 = idx.sub(1).map(lambda i: cal[i] if 0 <= i < len(cal) else None)
    prev2 = idx.sub(2).map(lambda i: cal[i] if 0 <= i < len(cal) else None)
    sym = df["symbol"].values
    df["earn_today"] = [(s, d) in earn_today for s, d in zip(sym, df["trade_date"])]
    df["earn_next1"] = [(s, d) in earn_today if d else False for s, d in zip(sym, next_day)]
    df["earn_recent2"] = [((s, a) in earn_today) or ((s, b) in earn_today)
                          for s, a, b in zip(sym, prev1, prev2)]

    out = df[["symbol", "trade_date", "earn_today", "earn_next1", "earn_recent2",
              "deliv_pct", "deliv_z20", "accum"]].copy()
    for c in ("earn_today", "earn_next1", "earn_recent2"):
        out[c] = out[c].astype(int)
    out = out.replace([np.inf, -np.inf], np.nan)
    rows = [tuple(None if (isinstance(v, float) and pd.isna(v)) else v for v in r)
            for r in out.itertuples(index=False, name=None)]
    con.executemany(
        "INSERT OR REPLACE INTO persona_event_features"
        "(symbol,trade_date,earn_today,earn_next1,earn_recent2,deliv_pct,deliv_z20,accum) "
        "VALUES (?,?,?,?,?,?,?,?)", rows)
    con.commit()
    return len(rows)


if __name__ == "__main__":
    from persona_engine import db
    con = db.connect()
    n = build(con, start="2022-01-01")
    print("persona_event_features rows:", n)
    print("  w/ earnings_next1:", con.execute("SELECT SUM(earn_next1) FROM persona_event_features").fetchone()[0])
    print("  w/ delivery:", con.execute("SELECT COUNT(*) FROM persona_event_features WHERE deliv_pct IS NOT NULL").fetchone()[0])
    con.close()
