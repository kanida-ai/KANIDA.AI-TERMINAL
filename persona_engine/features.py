"""
Stock-agent feature computation (spec §1.2, §2.4, §2.5, §3.4).

Everything here is strictly point-in-time: a feature row dated T uses only OHLCV
on or before T (no lookahead — constitutional rule P1). Forward returns/outcomes
live in ``outcomes.py`` and are only ever joined at MEASURE time.

We compute our own vectorised feature set from ``ohlc_daily`` rather than depend on
``falcon_features`` coverage, so the personas work across the full date range
(incl. thin 2021) and we never touch the Falcon tables.
"""
from __future__ import annotations

import sqlite3
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

INDEX_SYMBOL = "NIFTY50"


# ── loading ────────────────────────────────────────────────────────────────────

def load_sectors(con: sqlite3.Connection) -> Dict[str, str]:
    return {
        r["symbol"]: (r["sector"] or "Unknown")
        for r in con.execute("SELECT symbol, sector FROM falcon_sectors")
    }


def load_ohlc(
    con: sqlite3.Connection,
    symbols: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> pd.DataFrame:
    sql = (
        "SELECT symbol, trade_date, open, high, low, close, volume "
        "FROM ohlc_daily WHERE quality_flag != 'rejected'"
    )
    params: list = []
    if symbols is not None:
        qs = ",".join("?" * len(symbols))
        sql += f" AND symbol IN ({qs})"
        params += list(symbols)
    if start:
        sql += " AND trade_date >= ?"
        params.append(start)
    if end:
        sql += " AND trade_date <= ?"
        params.append(end)
    df = pd.read_sql_query(sql, con, params=params)
    df = df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
    return df


def load_index(con: sqlite3.Connection, start: Optional[str] = None) -> pd.Series:
    sql = "SELECT trade_date, close FROM ohlc_daily WHERE symbol = ?"
    params = [INDEX_SYMBOL]
    if start:
        sql += " AND trade_date >= ?"
        params.append(start)
    sql += " ORDER BY trade_date"
    idx = pd.read_sql_query(sql, con, params=params)
    return idx.set_index("trade_date")["close"]


# ── vectorised feature math (per symbol) ────────────────────────────────────────

def _rsi(close: pd.Series, n: int = 14) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0).rolling(n).mean()
    dn = (-delta.clip(upper=0)).rolling(n).mean()
    rs = up / dn.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def _atr_pct(g: pd.DataFrame, n: int = 20) -> pd.Series:
    pc = g["close"].shift(1)
    tr = pd.concat(
        [(g["high"] - g["low"]),
         (g["high"] - pc).abs(),
         (g["low"] - pc).abs()], axis=1
    ).max(axis=1)
    return tr.rolling(n).mean() / g["close"] * 100


def _per_symbol(g: pd.DataFrame) -> pd.DataFrame:
    c, o, h, l, v = g["close"], g["open"], g["high"], g["low"], g["volume"]
    out = pd.DataFrame(index=g.index)
    out["trade_date"] = g["trade_date"]
    out["symbol"] = g["symbol"]

    ret1 = c.pct_change()
    out["sig_ret_pct"] = ret1 * 100
    out["two_day_ret_pct"] = (c / c.shift(2) - 1) * 100

    vol20 = v.rolling(20).mean()
    out["vol_ratio_20d"] = v / vol20.replace(0, np.nan)
    out["vol_5d_vs_20d"] = v.rolling(5).mean() / vol20.replace(0, np.nan)

    out["gap_pct"] = (o / c.shift(1) - 1) * 100
    rng = (h - l).replace(0, np.nan)
    out["close_loc"] = (c - l) / rng
    out["rsi_14"] = _rsi(c, 14)
    out["roc_5"] = (c / c.shift(5) - 1) * 100
    out["roc_20"] = (c / c.shift(20) - 1) * 100
    out["prior_4w_ret"] = out["roc_20"]

    hh20 = h.rolling(20).max()
    out["dist_high_20"] = (c / hh20 - 1) * 100
    sma20 = c.rolling(20).mean()
    sma50 = c.rolling(50).mean()
    sma200 = c.rolling(200).mean()
    out["dist_sma_20"] = (c / sma20 - 1) * 100
    out["atr_20_pct"] = _atr_pct(g, 20)

    # distribution days: down day on above-average volume, count over last 5
    distrib = ((ret1 < 0) & (v > vol20)).astype(float)
    out["n_distrib_5d"] = distrib.rolling(5).sum()

    # consolidation: streak of recent days with tight daily range (<3% of close)
    tight = ((h - l) / c < 0.03).astype(int)
    # run length of trailing tight days
    grp = (tight == 0).cumsum()
    out["consol_days"] = tight.groupby(grp).cumsum()

    # OBV accumulation slope (normalised by 20d avg volume)
    obv = (np.sign(ret1).fillna(0) * v).cumsum()
    out["obv_slope_20d"] = (obv - obv.shift(20)) / (vol20.replace(0, np.nan) * 20)

    # multi-timeframe trend alignment 0..4
    trend = (
        (c > sma20).astype(int)
        + (c > sma50).astype(int)
        + (c > sma200).astype(int)
        + (out["roc_5"] > 0).astype(int)
    )
    out["trend_5_20_50_200"] = trend.astype(float)

    # breakout quality: new 20d high today -> volume strength, else 0
    new_high = (c >= hh20)
    out["breakout_quality"] = np.where(
        new_high, out["vol_ratio_20d"].fillna(0), 0.0
    )

    out["avg_lift"] = np.nan  # optional, joined from falcon engine if available
    return out


def compute_features(
    df: pd.DataFrame, index_close: pd.Series, sectors: Dict[str, str]
) -> pd.DataFrame:
    """Compute the full persona feature frame for all symbols in ``df``."""
    if df.empty:
        return df
    feats = df.groupby("symbol", group_keys=False).apply(_per_symbol)
    feats["sector"] = feats["symbol"].map(sectors).fillna("Unknown")

    # ── relative strength vs index ──
    idx = index_close.copy()
    idx_roc5 = (idx / idx.shift(5) - 1) * 100
    idx_roc20 = (idx / idx.shift(20) - 1) * 100
    idx_roc60 = (idx / idx.shift(60) - 1) * 100
    feats["rs_index_5d"] = feats["roc_5"] - feats["trade_date"].map(idx_roc5)
    feats["rs_index_20d"] = feats["roc_20"] - feats["trade_date"].map(idx_roc20)
    # roc_60 not stored per-row; recompute via map from a close-based proxy:
    # approximate 60d stock roc using merge below
    # (computed in _per_symbol-free path to keep memory low)
    # rs_index_60d handled after roc_60 merge:
    feats["rs_index_60d"] = np.nan

    # ── sector aggregates per day (rs_sector + sector_rank) ──
    # use 5d roc as the sector comparison horizon (spec §2.4 sector_rank by 5d ret)
    sub = feats[["trade_date", "sector", "symbol", "roc_5"]].dropna(subset=["roc_5"])
    sec_mean = (
        sub.groupby(["trade_date", "sector"])["roc_5"].transform("mean")
    )
    feats.loc[sub.index, "rs_sector_5d"] = sub["roc_5"] - sec_mean
    feats.loc[sub.index, "sector_rank_5d"] = (
        sub.groupby(["trade_date", "sector"])["roc_5"].rank(pct=True)
    )
    return feats


def _roc60(df: pd.DataFrame) -> pd.Series:
    return df.groupby("symbol")["close"].transform(lambda s: (s / s.shift(60) - 1) * 100)


FEATURE_COLS = [
    "sig_ret_pct", "two_day_ret_pct", "vol_ratio_20d", "vol_5d_vs_20d",
    "rs_index_5d", "rs_index_20d", "rs_sector_5d", "sector_rank_5d",
    "gap_pct", "close_loc", "rsi_14", "roc_5", "roc_20", "dist_high_20",
    "dist_sma_20", "atr_20_pct", "n_distrib_5d", "consol_days",
    "obv_slope_20d", "rs_index_60d", "trend_5_20_50_200", "prior_4w_ret",
    "breakout_quality", "avg_lift",
]


def build_features(
    con: sqlite3.Connection,
    start: str = "2020-06-01",
    end: Optional[str] = None,
    symbols: Optional[List[str]] = None,
    write: bool = True,
) -> pd.DataFrame:
    """Compute and (optionally) persist persona features for a date range."""
    sectors = load_sectors(con)
    # load a warm-up buffer before `start` so rolling windows are valid at `start`
    df = load_ohlc(con, symbols=symbols, start=None, end=end)
    df = df[df["symbol"] != INDEX_SYMBOL]
    index_close = load_index(con)

    feats = compute_features(df, index_close, sectors)
    # add roc_60-derived rs_index_60d
    roc60 = _roc60(df)
    idx_roc60 = (index_close / index_close.shift(60) - 1) * 100
    feats["rs_index_60d"] = roc60.values - feats["trade_date"].map(idx_roc60).values

    feats = feats[feats["trade_date"] >= start].copy()
    if end:
        feats = feats[feats["trade_date"] <= end]

    if write:
        _write_features(con, feats)
    return feats


def _write_features(con: sqlite3.Connection, feats: pd.DataFrame) -> int:
    cols = ["symbol", "trade_date", "sector"] + FEATURE_COLS
    sub = feats.reindex(columns=cols)
    sub = sub.replace([np.inf, -np.inf], np.nan)
    rows = [tuple(None if pd.isna(x) else float(x) if isinstance(x, (int, float, np.floating)) and c not in ("symbol", "trade_date", "sector") else x
                  for c, x in zip(cols, r))
            for r in sub.itertuples(index=False, name=None)]
    placeholders = ",".join("?" * len(cols))
    con.executemany(
        f"INSERT OR REPLACE INTO persona_signal_features ({','.join(cols)}) "
        f"VALUES ({placeholders})",
        rows,
    )
    con.commit()
    return len(rows)


if __name__ == "__main__":
    from persona_engine import db
    con = db.connect()
    feats = build_features(con, start="2021-12-01", end="2022-02-28", write=True)
    print("rows:", len(feats))
    print(feats[["symbol", "trade_date"] + FEATURE_COLS].tail(3).to_string())
    con.close()
