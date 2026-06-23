"""
Forward-return / actual-outcome computation (MEASURE stage).

These are the ONLY place future data is touched, and only ever to score a
prediction whose date has already passed. Two horizons:

  • F&O next-day  : full next-session return, open(T+1) -> close(T+1)  [spec §2.3]
  • Long-Term     : close(T) -> close(T+h), h in {20, 40} trading days [spec §3.3]

"Actual Top 10" for a date T = the 10 largest / smallest forward returns across the
relevant universe on T (ranked over the universe that was eligible at T).
"""
from __future__ import annotations

import sqlite3
from typing import List, Optional

import numpy as np
import pandas as pd

from persona_engine.features import INDEX_SYMBOL, load_ohlc


def forward_returns(
    con: sqlite3.Connection,
    symbols: Optional[List[str]] = None,
    start: Optional[str] = None,
) -> pd.DataFrame:
    """Return per (symbol, trade_date): next-day O->C and 20d/40d close returns.

    The values are attached to the *prediction* date T (the day the signal is
    generated). fwd_nd is realised on T+1; ret_20/ret_40 over the following
    20/40 sessions.
    """
    df = load_ohlc(con, symbols=symbols)
    df = df[df["symbol"] != INDEX_SYMBOL].copy()
    df = df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
    g = df.groupby("symbol", group_keys=False)

    nxt_open = g["open"].shift(-1)
    nxt_close = g["close"].shift(-1)
    df["fwd_nd"] = (nxt_close - nxt_open) / nxt_open * 100.0
    df["fwd_nd_date"] = g["trade_date"].shift(-1)

    c = df["close"]
    df["ret_20"] = (g["close"].shift(-20) / c - 1) * 100.0
    df["ret_40"] = (g["close"].shift(-40) / c - 1) * 100.0
    df["d20_date"] = g["trade_date"].shift(-20)
    df["d40_date"] = g["trade_date"].shift(-40)

    out = df[["symbol", "trade_date", "close", "fwd_nd", "fwd_nd_date",
              "ret_20", "ret_40", "d20_date", "d40_date"]]
    if start:
        out = out[out["trade_date"] >= start]
    return out.reset_index(drop=True)


def actual_top10(
    fwd: pd.DataFrame, date: str, ret_col: str, universe: List[str], k: int = 10
):
    """Return (top_gainers_df, top_losers_df) for a single date.

    Ranked across `universe` symbols that have a non-null forward return on `date`.
    """
    sub = fwd[(fwd["trade_date"] == date) & (fwd["symbol"].isin(universe))]
    sub = sub.dropna(subset=[ret_col])
    if sub.empty:
        return sub, sub
    gain = sub.sort_values(ret_col, ascending=False).head(k).reset_index(drop=True)
    lose = sub.sort_values(ret_col, ascending=True).head(k).reset_index(drop=True)
    return gain, lose


def rank_maps(fwd_date: pd.DataFrame, ret_col: str):
    """Given a single-date frame, return {symbol: gainer_rank}, {symbol: loser_rank}
    (1 = best gainer / worst loser)."""
    s = fwd_date.dropna(subset=[ret_col])
    gain_rank = (
        s.sort_values(ret_col, ascending=False).reset_index(drop=True)
        .assign(r=lambda d: d.index + 1).set_index("symbol")["r"].to_dict()
    )
    lose_rank = (
        s.sort_values(ret_col, ascending=True).reset_index(drop=True)
        .assign(r=lambda d: d.index + 1).set_index("symbol")["r"].to_dict()
    )
    return gain_rank, lose_rank
