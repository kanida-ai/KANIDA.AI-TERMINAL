"""Falcon Tesla — LIVE order-flow-native intraday SHORT signal engine.

Ports the operator's batch reference to real-time inference:

    C:\\Users\\SPS\\Documents\\Kanida Ai\\outputs\\falcon_tesla\\falcons_tesla_short_engine_v2.py
        (base_gate, add_phase_transition_features, add_stock_personality,
         add_train_personality_quality, grade → A++/A+++, select_trade_lifecycle)
    + falcons_tesla_walk_forward_pnl.py (score_all_cash / add_sector_context)
    + falcons_tesla_probe.py            (add_microstructure_features — vendored in
                                         tesla_features.py)

DATA SOURCE (live): the order-flow poller writes minute bars to
`mkt_orderflow_1min` in the universe DB (default
<repo>/universe_engine/data/db/kanida_universe.db), populated minute-by-minute
during market hours. NIFTYFUT context = the same table (symbol='NIFTY',
segment='FUT'). Sector = mkt_reference. This module opens the DB READ-ONLY and
NEVER writes to it.

KEY CHANGE vs the batch: the batch fixes TRAIN_DAYS=['2026-07-08','2026-07-09'].
For LIVE, stock personality + train-quality use a ROLLING window of the last K
COMPLETED trading days present in the poll (config `personality_window_days`,
default 5), resolved fresh each call — NOT hardcoded dates. Fed the 07-10 poll
with K=2 the window resolves to [07-08, 07-09] → identical to the batch (proven
by the parity test).

This module is DECISION-ONLY: it returns A++/A+++ SHORT signals. It never places
or exits an order — the AutoTrade session does that through the hardened
execution paths.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from . import tesla_features as tf

try:
    from pandas.errors import PerformanceWarning
    warnings.simplefilter("ignore", PerformanceWarning)
except Exception:  # pragma: no cover
    pass

IST = timezone(timedelta(hours=5, minutes=30))
log = logging.getLogger("autotrade.tesla")

# Repo root = <root>/backend/autotrade/strategies/tesla_short_engine.py → parents[3]
_REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DB_PATH = _REPO_ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"

# Minimum minute-bars a CASH symbol must have on a day to be a candidate — matches
# the batch get_all_cash_candidates (>= 300 rows on at least one window day).
MIN_CANDIDATE_ROWS = 300


# ── Signal shape ─────────────────────────────────────────────────────────────

@dataclass
class TeslaSignal:
    instrument: str          # cash symbol (== NSE tradingsymbol for equity)
    day: str                 # "YYYY-MM-DD"
    time: str                # "HH:MM" IST
    bar_time: str            # ISO minute bar
    grade: str               # "A++" | "A+++"
    setup: str               # v2_setup
    entry_ref_price: float   # bar close (the batch short entry ref)
    short_drive: float
    sector: Optional[str] = None
    # v3 DiD seat-rank key (v3_rank_score) when the DiD layer is ON; None for the
    # v2 path (rotation then ranks by short_drive exactly as before — byte-safe).
    rank_score: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "instrument": self.instrument, "day": self.day, "time": self.time,
            "bar_time": self.bar_time, "grade": self.grade, "setup": self.setup,
            "entry_ref_price": self.entry_ref_price,
            "short_drive": self.short_drive, "sector": self.sector,
            "rank_score": self.rank_score,
        }


@dataclass
class TeslaSignalResult:
    as_of: Optional[str]                 # the inference minute (ISO) or None=latest
    infer_day: str
    train_days: List[str]
    signals: List[TeslaSignal] = field(default_factory=list)
    n_candidates: int = 0
    note: str = ""


# ── DB access (read-only) ────────────────────────────────────────────────────

def connect_db_readonly(db_path: Optional[Path] = None) -> sqlite3.Connection:
    p = Path(db_path) if db_path else DEFAULT_DB_PATH
    uri = f"file:{p.as_posix()}?mode=ro"
    con = sqlite3.connect(uri, uri=True, timeout=90)
    con.execute("PRAGMA query_only=ON")
    return con


def available_trading_days(con: sqlite3.Connection) -> List[str]:
    """Distinct days present in mkt_orderflow_1min (CASH), ascending."""
    rows = con.execute(
        "SELECT DISTINCT substr(bar_time,1,10) AS d FROM mkt_orderflow_1min "
        "WHERE segment='CASH' ORDER BY d"
    ).fetchall()
    return [r[0] for r in rows]


def resolve_window(con: sqlite3.Connection, infer_day: str,
                   personality_window_days: int) -> List[str]:
    """The K COMPLETED trading days strictly BEFORE infer_day that exist in the
    poll (rolling personality window). Fewer than K available → use what exists."""
    days = available_trading_days(con)
    prior = [d for d in days if d < infer_day]
    if personality_window_days > 0:
        prior = prior[-personality_window_days:]
    return prior


# ── Cash-universe scoring (replicates walk_forward score_all_cash + sector) ──

# SQLite bound-param safety margin (3.32+ allows 32766; chunk well under it so
# the batched IN(...) reads work on any bundled sqlite).
_SQL_IN_CHUNK = 900


def _cash_row_counts(con: sqlite3.Connection,
                     days: Sequence[str]) -> Dict[str, Dict[str, int]]:
    """{symbol: {day: n_bars}} for CASH minute-bars on `days`, via ONE GROUP BY
    scan instead of a per-(symbol,day) COUNT (removes the N+1). Byte-identical to
    the per-symbol COUNT(*) over `bar_time>=day AND bar_time<day+1`: the day range
    is [min(days), max(days)+1) and non-listed intermediate days are dropped by
    `dayset`, so each returned count == the per-day COUNT."""
    days = list(days)
    if not days:
        return {}
    lo = min(days)
    hi = (pd.Timestamp(max(days)) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    dayset = set(days)
    out: Dict[str, Dict[str, int]] = {}
    for sym, d, n in con.execute(
            "SELECT symbol, substr(bar_time,1,10) AS d, COUNT(*) AS c "
            "FROM mkt_orderflow_1min WHERE segment='CASH' "
            "AND bar_time>=? AND bar_time<? "
            "GROUP BY symbol, substr(bar_time,1,10)", (lo, hi)):
        if d in dayset:
            out.setdefault(sym, {})[d] = n
    return out


def _get_cash_candidates(con: sqlite3.Connection, days: Sequence[str]) -> pd.DataFrame:
    ref = pd.read_sql_query(
        "SELECT symbol, segment, instrument_key, sector, company, lot_size "
        "FROM mkt_reference WHERE segment='CASH' ORDER BY symbol", con)
    days = list(days)
    counts_by_sym = _cash_row_counts(con, days)
    rows = []
    for row in ref.itertuples(index=False):
        counts = {day: counts_by_sym.get(row.symbol, {}).get(day, 0)
                  for day in days}
        if counts and max(counts.values()) >= MIN_CANDIDATE_ROWS:
            rec = row._asdict()
            rec["sector"] = rec["sector"] or "UNKNOWN"
            rows.append(rec)
    return pd.DataFrame(rows)


def _pull_cash_symbol(con: sqlite3.Connection, symbol: str,
                      days: Sequence[str]) -> pd.DataFrame:
    frames = []
    for day in days:
        nxt = (pd.Timestamp(day) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        q = pd.read_sql_query(
            """
            SELECT of.symbol, of.segment, of.instrument_key, of.bar_time,
              of.open, of.high, of.low, of.close, of.volume, of.atp, of.oi,
              of.total_buy_qty, of.total_sell_qty,
              of.b1p, of.b1q, of.b2p, of.b2q, of.b3p, of.b3q, of.b4p, of.b4q, of.b5p, of.b5q,
              of.a1p, of.a1q, of.a2p, of.a2q, of.a3p, of.a3q, of.a4p, of.a4q, of.a5p, of.a5q,
              of.last_qty, of.b1o, of.b2o, of.b3o, of.b4o, of.b5o,
              of.a1o, of.a2o, of.a3o, of.a4o, of.a5o,
              tr.n_ticks AS tick_n, tr.buy_vol AS tick_buy_vol,
              tr.sell_vol AS tick_sell_vol, tr.buy_vol_pct AS "tick_buy%",
              tr.avg_tick_vol AS tick_avg_size, tr.max_tick_vol AS tick_max_burst,
              tr.max_trade_qty AS tick_max_order
            FROM mkt_orderflow_1min of
            LEFT JOIN mkt_trades_1min tr
              ON tr.instrument_key = of.instrument_key AND tr.bar_time = of.bar_time
            WHERE of.symbol=? AND of.segment='CASH' AND of.bar_time>=? AND of.bar_time<?
            ORDER BY of.bar_time
            """, con, params=(symbol, day, nxt))
        if not q.empty:
            frames.append(q)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df["bar_time"] = pd.to_datetime(df["bar_time"])
    df["instrument"] = df["symbol"]
    df["root_symbol"] = df["symbol"]
    df["time"] = df["bar_time"].dt.strftime("%H:%M")
    df["day"] = df["bar_time"].dt.strftime("%Y-%m-%d")
    total_book = df["total_buy_qty"] + df["total_sell_qty"]
    df["buy_imb%"] = (df["total_buy_qty"] / total_book.replace(0, np.nan)) * 100.0
    bid_cols = ["b1q", "b2q", "b3q", "b4q", "b5q"]
    ask_cols = ["a1q", "a2q", "a3q", "a4q", "a5q"]
    top_bid = df[bid_cols].sum(axis=1, min_count=1)
    top_ask = df[ask_cols].sum(axis=1, min_count=1)
    df["depth_bid%"] = (top_bid / (top_bid + top_ask).replace(0, np.nan)) * 100.0
    df["avg_bid_ord"] = df["b1q"] / df["b1o"].replace(0, np.nan)
    df["avg_ask_ord"] = df["a1q"] / df["a1o"].replace(0, np.nan)
    df["block_x"] = df["tick_max_order"] / df["tick_avg_size"].replace(0, np.nan)
    return df


_CASH_SELECT = """
    SELECT of.symbol, of.segment, of.instrument_key, of.bar_time,
      of.open, of.high, of.low, of.close, of.volume, of.atp, of.oi,
      of.total_buy_qty, of.total_sell_qty,
      of.b1p, of.b1q, of.b2p, of.b2q, of.b3p, of.b3q, of.b4p, of.b4q, of.b5p, of.b5q,
      of.a1p, of.a1q, of.a2p, of.a2q, of.a3p, of.a3q, of.a4p, of.a4q, of.a5p, of.a5q,
      of.last_qty, of.b1o, of.b2o, of.b3o, of.b4o, of.b5o,
      of.a1o, of.a2o, of.a3o, of.a4o, of.a5o,
      tr.n_ticks AS tick_n, tr.buy_vol AS tick_buy_vol,
      tr.sell_vol AS tick_sell_vol, tr.buy_vol_pct AS "tick_buy%",
      tr.avg_tick_vol AS tick_avg_size, tr.max_tick_vol AS tick_max_burst,
      tr.max_trade_qty AS tick_max_order
    FROM mkt_orderflow_1min of
    LEFT JOIN mkt_trades_1min tr
      ON tr.instrument_key = of.instrument_key AND tr.bar_time = of.bar_time
"""


def _postprocess_cash_raw(df: pd.DataFrame) -> pd.DataFrame:
    """The per-row derived columns _pull_cash_symbol adds after the SQL read
    (all VECTORISED / row-local, so identical whether applied per-symbol or to a
    multi-symbol frame)."""
    df["bar_time"] = pd.to_datetime(df["bar_time"])
    df["instrument"] = df["symbol"]
    df["root_symbol"] = df["symbol"]
    df["time"] = df["bar_time"].dt.strftime("%H:%M")
    df["day"] = df["bar_time"].dt.strftime("%Y-%m-%d")
    total_book = df["total_buy_qty"] + df["total_sell_qty"]
    df["buy_imb%"] = (df["total_buy_qty"] / total_book.replace(0, np.nan)) * 100.0
    bid_cols = ["b1q", "b2q", "b3q", "b4q", "b5q"]
    ask_cols = ["a1q", "a2q", "a3q", "a4q", "a5q"]
    top_bid = df[bid_cols].sum(axis=1, min_count=1)
    top_ask = df[ask_cols].sum(axis=1, min_count=1)
    df["depth_bid%"] = (top_bid / (top_bid + top_ask).replace(0, np.nan)) * 100.0
    df["avg_bid_ord"] = df["b1q"] / df["b1o"].replace(0, np.nan)
    df["avg_ask_ord"] = df["a1q"] / df["a1o"].replace(0, np.nan)
    df["block_x"] = df["tick_max_order"] / df["tick_avg_size"].replace(0, np.nan)
    return df


def _pull_cash_symbols_batch(con: sqlite3.Connection, symbols: Sequence[str],
                             days: Sequence[str]) -> pd.DataFrame:
    """BATCHED equivalent of looping _pull_cash_symbol over `symbols`: ONE query
    per ~900-symbol chunk (`WHERE of.symbol IN (...)`) over the [min(days),
    max(days)+1) bar_time range, with the SAME LEFT JOIN to mkt_trades_1min, then
    the identical per-row post-processing. Rows outside `days` (non-listed
    intermediate days inside the range) are dropped so the row set matches the
    per-symbol per-day pulls exactly. Per-(instrument,day) independence of every
    downstream feature makes this byte-identical to the per-symbol path."""
    syms = list(dict.fromkeys(symbols))
    days = list(days)
    if not syms or not days:
        return pd.DataFrame()
    lo = min(days)
    hi = (pd.Timestamp(max(days)) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    dayset = set(days)
    frames = []
    for i in range(0, len(syms), _SQL_IN_CHUNK):
        chunk = syms[i:i + _SQL_IN_CHUNK]
        marks = ",".join("?" * len(chunk))
        q = pd.read_sql_query(
            _CASH_SELECT +
            f"WHERE of.symbol IN ({marks}) AND of.segment='CASH' "
            "AND of.bar_time>=? AND of.bar_time<? "
            "ORDER BY of.symbol, of.bar_time",
            con, params=(*chunk, lo, hi))
        if not q.empty:
            frames.append(q)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    # Keep only the requested days (per-symbol path never saw other days).
    keep = df["bar_time"].astype(str).str.slice(0, 10).isin(dayset)
    df = df[keep].reset_index(drop=True)
    if df.empty:
        return pd.DataFrame()
    return _postprocess_cash_raw(df)


def _add_forward_15m(scored: pd.DataFrame) -> pd.DataFrame:
    """fwd_15m_bps per (instrument, day) — the only forward horizon the v2
    personality-quality gate needs. For the LIVE infer day these are NaN toward
    the tail (no future bars yet); quality only reads them on the PAST train days
    where the forward bars exist (faithful to the batch)."""
    out = scored.sort_values(["instrument", "day", "bar_time"]).copy()
    grouped = out.groupby(["instrument", "day"], sort=False)
    future_close = grouped["close"].shift(-15)
    out["fwd_15m_bps"] = (future_close / out["close"] - 1.0) * 10000.0
    return out


def score_universe_from_db(con: sqlite3.Connection,
                           days: Sequence[str]) -> pd.DataFrame:
    """Build the scored CASH frame for `days` — matches
    walk_forward_pnl.score_all_cash + add_sector_context (fut confirmation = 0 for
    cash). Returns a frame with all columns the v2 grader needs."""
    candidates = _get_cash_candidates(con, days)
    if candidates.empty:
        return pd.DataFrame()
    scored_parts = []
    for symbol in candidates["symbol"].drop_duplicates():
        raw = _pull_cash_symbol(con, symbol, days)
        if raw.empty:
            continue
        for _, group in raw.groupby(["instrument", "day"], sort=False):
            scored_parts.append(tf.add_microstructure_features(group))
    if not scored_parts:
        return pd.DataFrame()
    scored = pd.concat(scored_parts, ignore_index=True)
    # Cash gets NO futures confirmation (fut_* = 0) — walk_forward does the same.
    scored["short_drive"] = tf.clamp01(scored["short_drive_core"])
    scored["long_drive"] = tf.clamp01(scored["long_drive_core"])
    scored["bid_absorption"] = tf.clamp01(scored["bid_absorption_core"])
    scored["ask_absorption"] = tf.clamp01(scored["ask_absorption_core"])
    scored["falcon_tesla_score"] = (scored["long_drive"] - scored["short_drive"]) * 100.0
    scored["absorption_bias"] = (scored["bid_absorption"] - scored["ask_absorption"]) * 100.0
    scored = tf.assign_phase(scored)
    scored = _add_forward_15m(scored)

    # Sector + derived columns (matches add_sector_context).
    sect = candidates[["symbol", "sector", "company"]].drop_duplicates()
    scored = scored.merge(sect, left_on="instrument", right_on="symbol",
                          how="left", suffixes=("", "_ref"))
    scored["sector"] = scored["sector"].fillna("UNKNOWN")
    scored["bar_time"] = pd.to_datetime(scored["bar_time"])
    scored["close_atp_gap_bps"] = ((scored["close"] - scored["atp"]) /
                                   scored["close"].replace(0, np.nan)) * 10000.0
    scored["tick_sell%"] = 100.0 - scored["tick_buy%"]
    return scored


def _sector_map(con: sqlite3.Connection,
                symbols: Sequence[str]) -> pd.DataFrame:
    """sector/company for `symbols` from mkt_reference (CASH), NULL sector →
    'UNKNOWN' — matches _get_cash_candidates' sector handling. Returns a frame
    with columns [symbol, sector, company]."""
    ref = pd.read_sql_query(
        "SELECT symbol, sector, company FROM mkt_reference WHERE segment='CASH'",
        con)
    ref = ref[ref["symbol"].isin(list(symbols))].drop_duplicates("symbol").copy()
    ref["sector"] = ref["sector"].fillna("UNKNOWN")
    return ref[["symbol", "sector", "company"]]


def score_universe_for_symbols(con: sqlite3.Connection,
                               days: Sequence[str],
                               symbols: Sequence[str],
                               vectorized: bool = False) -> pd.DataFrame:
    """Score an EXPLICIT symbol list over `days` — the same per-(instrument,day)
    feature+phase+forward+sector pipeline as score_universe_from_db, but for a
    caller-supplied symbol set (skips the >=300-row candidate discovery, which
    the caller has already done for the WINDOW). Used by the once-per-minute
    fast path so the infer day is scored for the SAME union candidate set the
    full recompute uses (train ∪ infer candidates). Per-(instrument,day)
    independence makes this byte-identical to score_universe_from_db for the
    same rows (proven by the synthetic parity test)."""
    syms = list(dict.fromkeys(symbols))  # de-dup, preserve order
    if not syms:
        return pd.DataFrame()
    # ONE batched read for the whole symbol set (removes the per-symbol N+1) —
    # then the SAME per-(instrument,day) feature pipeline. groupby(sort=False) on
    # the symbol,bar_time-ordered frame yields the groups in the identical order
    # the per-symbol loop did, so the result is byte-identical.
    raw = _pull_cash_symbols_batch(con, syms, days)
    return _finalize_scored_from_raw(con, raw, syms, vectorized=vectorized)


def _finalize_scored_from_raw(con: sqlite3.Connection, raw: pd.DataFrame,
                              syms: Sequence[str],
                              vectorized: bool = False) -> pd.DataFrame:
    """The scoring pipeline AFTER the raw poll read: microstructure + drive/
    absorption + phase + fwd + sector, for an already-fetched post-processed raw
    frame. Split out of score_universe_for_symbols so the INCREMENTAL read path
    (which assembles `raw` from a per-process bar cache instead of a full DB read)
    runs the SAME scoring and is therefore byte-identical.

    Row order is irrelevant here: the vectorised microstructure sorts by
    [instrument, day, bar_time] internally; the loop path groups the same way; and
    every downstream op (assign_phase / _add_forward_15m sort / the sector merge)
    is per-(instrument,day) or row-local. So a `raw` reassembled from cached +
    freshly-read bars yields the identical scored frame as a full re-read of the
    same rows.

    DEFAULT (vectorized=False) → the committed per-(instrument,day) microstructure
    loop (the exact oracle score_universe_from_db uses). vectorized=True → the
    multi-group VECTORISED microstructure (byte-identical to the loop; proven)."""
    if raw is None or raw.empty:
        return pd.DataFrame()
    if vectorized:
        scored = tf.add_microstructure_features_vectorized(raw)
        if scored.empty:
            return pd.DataFrame()
    else:
        scored_parts = [tf.add_microstructure_features(group)
                        for _, group in raw.groupby(["instrument", "day"],
                                                     sort=False)]
        if not scored_parts:
            return pd.DataFrame()
        scored = pd.concat(scored_parts, ignore_index=True)
    # DE-FRAGMENTED assembly (round-2 opt 2): add the six drive/absorption columns
    # in ONE block-consolidated assign() instead of six single-column inserts (the
    # source of the pandas "highly fragmented DataFrame" PerformanceWarning). The
    # arithmetic is unchanged, so the values are bit-identical to the per-column
    # assigns (proven by the all-columns scored-frame parity test).
    sd = tf.clamp01(scored["short_drive_core"])
    ld = tf.clamp01(scored["long_drive_core"])
    ba = tf.clamp01(scored["bid_absorption_core"])
    aa = tf.clamp01(scored["ask_absorption_core"])
    scored = scored.assign(
        short_drive=sd, long_drive=ld,
        bid_absorption=ba, ask_absorption=aa,
        falcon_tesla_score=(ld - sd) * 100.0,
        absorption_bias=(ba - aa) * 100.0)
    scored = tf.assign_phase(scored)
    scored = _add_forward_15m(scored)
    sect = _sector_map(con, syms)
    scored = scored.merge(sect, left_on="instrument", right_on="symbol",
                          how="left", suffixes=("", "_ref"))
    bt = pd.to_datetime(scored["bar_time"])
    gap = ((scored["close"] - scored["atp"]) /
           scored["close"].replace(0, np.nan)) * 10000.0
    scored = scored.assign(
        sector=scored["sector"].fillna("UNKNOWN"),
        bar_time=bt,
        close_atp_gap_bps=gap,
        **{"tick_sell%": 100.0 - scored["tick_buy%"]})
    return scored


def candidate_symbols(con: sqlite3.Connection,
                      days: Sequence[str]) -> List[str]:
    """Symbols that qualify as CASH candidates over `days` (>=300 rows on at
    least one day) — the symbol list of _get_cash_candidates."""
    cands = _get_cash_candidates(con, days)
    if cands.empty:
        return []
    return list(cands["symbol"].drop_duplicates())


# ── INCREMENTAL infer-day poll read (round-2 warm-tick optimization) ──────────
#
# The per-minute recompute re-read the WHOLE infer day (~9.6s on 498 symbols) and
# re-scanned the day for the candidate row-counts (~1.7s) every tick — even though
# only the newest minute(s) change. The poller (scripts/mkt_poller.py) writes each
# 1-min bar EXACTLY ONCE, AFTER the minute closes, via INSERT OR IGNORE — it never
# rewrites an existing (instrument_key, bar_time) row and bars arrive in monotonic
# minute order. So the finalized bars are IMMUTABLE: a per-process cache of them is
# provably identical to a fresh full read, and each tick only needs to fetch bars
# with bar_time > cached_max. This is BYTE-SAFE (not an approximation): the
# reassembled raw frame == a full re-read of the same rows, so the scored/graded
# signals are byte-identical (proven by the full-day incremental parity test). The
# caches live in the (per-worker) process the offload pool reuses — same lifetime
# model as _TRAIN_CACHE; each worker warms once per day then reads incrementally.


def _read_cash_raw_day(con: sqlite3.Connection, symbols: Sequence[str], day: str,
                       bar_time_gt: Optional[str] = None,
                       bar_time_le: Optional[str] = None) -> pd.DataFrame:
    """RAW (text bar_time, PRE-postprocess) CASH rows for `symbols` on `day`,
    batched over ~900-symbol IN() chunks with the SAME LEFT JOIN to
    mkt_trades_1min as _pull_cash_symbols_batch. `bar_time_gt` (exclusive) and/or
    `bar_time_le` (inclusive) narrow the read to an incremental window; both None
    → the whole day. bar_time is stored as fixed-format "YYYY-MM-DD HH:MM" text so
    lexicographic string comparison == chronological (index idx_mktof_sym_dt makes
    the per-symbol bar_time range seek instant). Returns rows for `day` only."""
    syms = list(dict.fromkeys(symbols))
    if not syms:
        return pd.DataFrame()
    hi = (pd.Timestamp(day) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    conds = ["of.segment='CASH'"]
    tail_params: List[Any] = []
    if bar_time_gt is not None:
        conds.append("of.bar_time>?")
        tail_params.append(bar_time_gt)
    else:
        conds.append("of.bar_time>=?")
        tail_params.append(day)
    conds.append("of.bar_time<?")
    tail_params.append(hi)
    if bar_time_le is not None:
        conds.append("of.bar_time<=?")
        tail_params.append(bar_time_le)
    frames = []
    for i in range(0, len(syms), _SQL_IN_CHUNK):
        chunk = syms[i:i + _SQL_IN_CHUNK]
        marks = ",".join("?" * len(chunk))
        where = (f"WHERE of.symbol IN ({marks}) AND " + " AND ".join(conds) +
                 " ORDER BY of.symbol, of.bar_time")
        q = pd.read_sql_query(_CASH_SELECT + where, con,
                              params=(*chunk, *tail_params))
        if not q.empty:
            frames.append(q)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    keep = df["bar_time"].astype(str).str.slice(0, 10) == day
    return df[keep].reset_index(drop=True)


@dataclass
class _InferRawCache:
    db_key: str
    infer_day: str
    covered: set                 # symbols whose infer-day rows are in `proc`
    proc: pd.DataFrame           # POST-PROCESSED raw frame for `covered`
    max_bar_time: str            # max bar_time TEXT currently cached ("" if none)
    built_at: str


_INFER_RAW_CACHE: Dict[str, _InferRawCache] = {}
_INFER_RAW_LOCK = threading.RLock()
_infer_raw_build_count = 0       # full (cold / day-roll) reads
_infer_raw_incr_count = 0        # incremental (tail) reads


@dataclass
class _InferCountCache:
    db_key: str
    infer_day: str
    ref_cash: tuple              # mkt_reference CASH symbols (invariant intraday)
    counts: Dict[str, int]       # {symbol: infer-day row count so far}
    scan_max: str                # max bar_time counted ("" if none)


_INFER_COUNT_CACHE: Dict[str, _InferCountCache] = {}
_INFER_COUNT_LOCK = threading.RLock()


def reset_infer_caches() -> None:
    """Drop the incremental infer-day raw + count caches (tests / forced refresh)."""
    global _infer_raw_build_count, _infer_raw_incr_count
    with _INFER_RAW_LOCK:
        _INFER_RAW_CACHE.clear()
        _infer_raw_build_count = 0
        _infer_raw_incr_count = 0
    with _INFER_COUNT_LOCK:
        _INFER_COUNT_CACHE.clear()


def _incremental_infer_candidates(con: sqlite3.Connection, db_key: str,
                                  infer_day: str) -> List[str]:
    """The infer-day CASH candidate symbols (>=300 rows), maintained
    INCREMENTALLY: the per-symbol row counts are accumulated by scanning only the
    NEW bars (bar_time > scan_max) each tick, so the full-day GROUP BY runs once
    (cold) then only the tail is counted. Byte-identical to candidate_symbols(con,
    [infer_day]): the summed increments equal the full COUNT(*) per symbol, and the
    result is the mkt_reference-CASH symbols with count>=300 (same set)."""
    key = f"{db_key}|{infer_day}"
    hi = (pd.Timestamp(infer_day) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    with _INFER_COUNT_LOCK:
        entry = _INFER_COUNT_CACHE.get(key)
        if entry is None or entry.infer_day != infer_day:
            ref = pd.read_sql_query(
                "SELECT symbol FROM mkt_reference WHERE segment='CASH' "
                "ORDER BY symbol", con)
            ref_cash = tuple(dict.fromkeys(ref["symbol"].tolist()))
            entry = _InferCountCache(db_key=db_key, infer_day=infer_day,
                                     ref_cash=ref_cash, counts={}, scan_max="")
            _INFER_COUNT_CACHE.clear()
            _INFER_COUNT_CACHE[key] = entry
        gt = entry.scan_max if entry.scan_max else infer_day
        op = ">" if entry.scan_max else ">="
        new_max = None
        for sym, cnt, mx in con.execute(
                "SELECT symbol, COUNT(*) AS c, MAX(bar_time) AS m "
                "FROM mkt_orderflow_1min WHERE segment='CASH' "
                f"AND bar_time{op}? AND bar_time<? GROUP BY symbol",
                (gt, hi)):
            entry.counts[sym] = entry.counts.get(sym, 0) + int(cnt)
            if mx is not None and (new_max is None or mx > new_max):
                new_max = mx
        if new_max is not None:
            entry.scan_max = new_max
        refset = set(entry.ref_cash)
        return [s for s in entry.ref_cash
                if s in refset and entry.counts.get(s, 0) >= MIN_CANDIDATE_ROWS]


def _incremental_infer_raw(con: sqlite3.Connection, db_key: str, infer_day: str,
                           all_syms: Sequence[str]) -> pd.DataFrame:
    """Return the POST-PROCESSED raw infer-day frame for `all_syms`, reading from
    the poll DB INCREMENTALLY. On a cold cache (or day-roll) it does a full read;
    thereafter each tick reads only the new minute(s) (bar_time > cached_max) for
    the covered symbols plus the full history of any newly-added symbols. Because
    stored bars are immutable + monotonic, the reassembled frame is byte-identical
    to _pull_cash_symbols_batch(con, all_syms, [infer_day])."""
    global _infer_raw_build_count, _infer_raw_incr_count
    key = f"{db_key}|{infer_day}"
    want = set(all_syms)
    with _INFER_RAW_LOCK:
        entry = _INFER_RAW_CACHE.get(key)
        # Full (cold) read on: no cache, a day-roll, OR — defensively — if the
        # wanted set is NOT a superset of what's cached (all_syms grows monotonically
        # within a day, so a SHRINK "can't happen"; if it ever did, a stale superset
        # frame would break parity, so we rebuild instead).
        if (entry is None or entry.infer_day != infer_day
                or not want.issuperset(entry.covered)):
            raw = _read_cash_raw_day(con, sorted(want), infer_day)
            # Capture the max bar_time as TEXT (DB format "YYYY-MM-DD HH:MM")
            # BEFORE _postprocess_cash_raw mutates bar_time to datetime in place —
            # this text feeds the next tick's `bar_time > ?` incremental read.
            mx = str(raw["bar_time"].max()) if not raw.empty else ""
            proc = (_postprocess_cash_raw(raw) if not raw.empty
                    else pd.DataFrame())
            entry = _InferRawCache(db_key=db_key, infer_day=infer_day,
                                   covered=set(want), proc=proc, max_bar_time=mx,
                                   built_at=datetime.now(IST).isoformat())
            _INFER_RAW_CACHE.clear()
            _INFER_RAW_CACHE[key] = entry
            _infer_raw_build_count += 1
            return entry.proc
        new_syms = want - entry.covered
        parts: List[pd.DataFrame] = []
        if not entry.proc.empty:
            parts.append(entry.proc)
        new_max = entry.max_bar_time
        # (a) HISTORY (bar_time <= cached_max) for newly-added symbols only.
        if new_syms and entry.max_bar_time:
            hist = _read_cash_raw_day(con, sorted(new_syms), infer_day,
                                      bar_time_le=entry.max_bar_time)
            if not hist.empty:
                parts.append(_postprocess_cash_raw(hist))
        elif new_syms:  # cache had no bars yet — new syms get the full day below
            pass
        # (b) TAIL (bar_time > cached_max) for ALL wanted symbols (covered + new).
        gt = entry.max_bar_time if entry.max_bar_time else None
        tail = _read_cash_raw_day(con, sorted(want), infer_day, bar_time_gt=gt)
        if not tail.empty:
            tmax = str(tail["bar_time"].max())   # TEXT max BEFORE postprocess
            parts.append(_postprocess_cash_raw(tail))
            if not new_max or tmax > new_max:
                new_max = tmax
        proc = (pd.concat(parts, ignore_index=True) if len(parts) > 1
                else (parts[0] if parts else pd.DataFrame()))
        entry.proc = proc
        entry.covered = set(want)
        entry.max_bar_time = new_max or ""
        _infer_raw_incr_count += 1
        return entry.proc


# ── NIFTYFUT context (verbatim logic from short_engine_v2.load_nifty_context) ─

def load_nifty_context(con: sqlite3.Connection, days: Sequence[str]) -> pd.DataFrame:
    lo = min(days)
    hi = (pd.Timestamp(max(days)) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    q = pd.read_sql_query(
        """
        SELECT of.* FROM mkt_orderflow_1min of
        WHERE of.symbol='NIFTY' AND of.segment='FUT'
          AND of.bar_time>=? AND of.bar_time<? ORDER BY of.bar_time
        """, con, params=(lo, hi))
    if q.empty:
        return pd.DataFrame(columns=[
            "day", "time", "nifty_close", "nifty_move_open_pct", "nifty_ret_bps",
            "nifty_atp_torque", "nifty_short_pressure", "nifty_long_pressure",
            "nifty_net_aggression"])
    q["bar_time"] = pd.to_datetime(q["bar_time"])
    q["instrument"] = "NIFTYFUT"
    q["root_symbol"] = "NIFTY"
    q["time"] = q["bar_time"].dt.strftime("%H:%M")
    q["day"] = q["bar_time"].dt.strftime("%Y-%m-%d")
    total_book = q["total_buy_qty"] + q["total_sell_qty"]
    q["buy_imb%"] = (q["total_buy_qty"] / total_book.replace(0, np.nan)) * 100.0
    bid_cols = ["b1q", "b2q", "b3q", "b4q", "b5q"]
    ask_cols = ["a1q", "a2q", "a3q", "a4q", "a5q"]
    top_bid = q[bid_cols].sum(axis=1, min_count=1)
    top_ask = q[ask_cols].sum(axis=1, min_count=1)
    q["depth_bid%"] = (top_bid / (top_bid + top_ask).replace(0, np.nan)) * 100.0
    parts = []
    for _, group in q.groupby("day", sort=False):
        parts.append(tf.add_microstructure_features(group))
    scored = pd.concat(parts, ignore_index=True)
    ctx = scored[[
        "day", "time", "close", "move_from_first_close%", "ret_bps",
        "atp_torque", "short_drive_core", "long_drive_core", "net_aggression",
    ]].rename(columns={
        "close": "nifty_close",
        "move_from_first_close%": "nifty_move_open_pct",
        "ret_bps": "nifty_ret_bps",
        "atp_torque": "nifty_atp_torque",
        "short_drive_core": "nifty_short_pressure",
        "long_drive_core": "nifty_long_pressure",
        "net_aggression": "nifty_net_aggression",
    })
    return ctx


# ── v2 grading pipeline (ported; TRAIN_DAYS → passed-in rolling window) ───────

def add_market_context(df: pd.DataFrame, nifty_ctx: pd.DataFrame) -> pd.DataFrame:
    all_ctx = (
        df.groupby(["day", "time"])
        .agg(
            market_median_ret_bps=("ret_bps", "median"),
            market_down_breadth=("ret_bps", lambda s: float((s < 0).mean() * 100.0)),
            market_median_move_open_pct=("move_from_first_close%", "median"),
            market_short_pressure=("short_drive", "median"),
            market_long_pressure=("long_drive", "median"),
        ).reset_index())
    sector_ctx = (
        df.groupby(["sector", "day", "time"])
        .agg(
            sector_median_ret_bps=("ret_bps", "median"),
            sector_down_breadth=("ret_bps", lambda s: float((s < 0).mean() * 100.0)),
            sector_move_open_pct=("move_from_first_close%", "median"),
            sector_short_pressure=("short_drive", "median"),
            sector_long_pressure=("long_drive", "median"),
        ).reset_index())
    out = df.merge(all_ctx, on=["day", "time"], how="left")
    out = out.merge(sector_ctx, on=["sector", "day", "time"], how="left")
    if nifty_ctx is not None and not nifty_ctx.empty:
        out = out.merge(nifty_ctx, on=["day", "time"], how="left")
    else:  # no nifty data → columns present but NaN (gate will not fire)
        for c in ["nifty_close", "nifty_move_open_pct", "nifty_ret_bps",
                  "nifty_atp_torque", "nifty_short_pressure",
                  "nifty_long_pressure", "nifty_net_aggression"]:
            out[c] = np.nan
    return out


def add_phase_transition_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.sort_values(["instrument", "day", "bar_time"]).copy()
    short_phase = out["falcon_phase"].isin(["SHORT_DRIVE", "ATP_ASK_REVERSAL_DISTRIBUTION"])
    out["is_short_phase"] = short_phase.astype(int)
    out["is_relief_phase"] = (
        out["long_drive"].gt(out["short_drive"])
        | out["falcon_phase"].isin(["NEUTRAL", "LONG_DRIVE", "ATP_BID_REVERSAL_ABSORPTION"])
    ).astype(int)

    def per_group(g: pd.DataFrame) -> pd.DataFrame:
        g = g.copy()
        g["prev_phase"] = g["falcon_phase"].shift()
        g["prev_low_10"] = g["close"].shift().rolling(10, min_periods=3).min()
        g["prev_high_10"] = g["close"].shift().rolling(10, min_periods=3).max()
        g["prev_short_count_20"] = g["is_short_phase"].shift().rolling(20, min_periods=1).sum()
        g["prev_relief_count_8"] = g["is_relief_phase"].shift().rolling(8, min_periods=1).sum()
        g["new_breakdown"] = g["close"].le(g["prev_low_10"] * 0.9995)
        g["first_short_after_pause"] = g["is_short_phase"].eq(1) & ~g["prev_phase"].isin(
            ["SHORT_DRIVE", "ATP_ASK_REVERSAL_DISTRIBUTION"])
        g["reload_after_relief"] = (
            g["falcon_phase"].eq("SHORT_DRIVE")
            & g["prev_short_count_20"].ge(1)
            & g["prev_relief_count_8"].ge(2)
            & g["new_breakdown"])
        g["ask_distribution_turn"] = (
            g["falcon_phase"].eq("ATP_ASK_REVERSAL_DISTRIBUTION")
            & g["atp_ask_reversal_pressure"].ge(0.16)
            & g["prev_relief_count_8"].ge(1))
        g["phase_transition_ok"] = (
            (g["first_short_after_pause"] & g["new_breakdown"])
            | g["reload_after_relief"]
            | g["ask_distribution_turn"])
        return g

    parts = []
    for (instrument, day), group in out.groupby(["instrument", "day"], sort=False):
        part = per_group(group)
        part["instrument"] = instrument
        part["day"] = day
        parts.append(part)
    return pd.concat(parts, ignore_index=True)


def add_phase_transition_features_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    """Vectorised, multi-group equivalent of add_phase_transition_features.

    Byte-identical to the per-(instrument,day) loop version (proven by the
    equivalence test) but computes every group at once with grouped shift/rolling
    ops instead of a 498-iteration Python loop. Stateless."""
    out = df.sort_values(["instrument", "day", "bar_time"]).reset_index(drop=True).copy()
    gkeys = ["instrument", "day"]
    short_phase = out["falcon_phase"].isin(["SHORT_DRIVE", "ATP_ASK_REVERSAL_DISTRIBUTION"])
    out["is_short_phase"] = short_phase.astype(int)
    out["is_relief_phase"] = (
        out["long_drive"].gt(out["short_drive"])
        | out["falcon_phase"].isin(["NEUTRAL", "LONG_DRIVE", "ATP_BID_REVERSAL_ABSORPTION"])
    ).astype(int)

    def _grp(col):
        return out.groupby(gkeys, sort=False)[col]

    def _align(series):
        s = series.copy()
        s.index = s.index.get_level_values(-1)
        return s.sort_index()

    out["prev_phase"] = _grp("falcon_phase").shift()
    out["_close_s1"] = _grp("close").shift()
    out["prev_low_10"] = _align(out.groupby(gkeys, sort=False)["_close_s1"].rolling(10, min_periods=3).min())
    out["prev_high_10"] = _align(out.groupby(gkeys, sort=False)["_close_s1"].rolling(10, min_periods=3).max())
    out["_iss_s1"] = _grp("is_short_phase").shift()
    out["prev_short_count_20"] = _align(out.groupby(gkeys, sort=False)["_iss_s1"].rolling(20, min_periods=1).sum())
    out["_isr_s1"] = _grp("is_relief_phase").shift()
    out["prev_relief_count_8"] = _align(out.groupby(gkeys, sort=False)["_isr_s1"].rolling(8, min_periods=1).sum())
    out = out.drop(columns=["_close_s1", "_iss_s1", "_isr_s1"])

    out["new_breakdown"] = out["close"].le(out["prev_low_10"] * 0.9995)
    out["first_short_after_pause"] = out["is_short_phase"].eq(1) & ~out["prev_phase"].isin(
        ["SHORT_DRIVE", "ATP_ASK_REVERSAL_DISTRIBUTION"])
    out["reload_after_relief"] = (
        out["falcon_phase"].eq("SHORT_DRIVE")
        & out["prev_short_count_20"].ge(1)
        & out["prev_relief_count_8"].ge(2)
        & out["new_breakdown"])
    out["ask_distribution_turn"] = (
        out["falcon_phase"].eq("ATP_ASK_REVERSAL_DISTRIBUTION")
        & out["atp_ask_reversal_pressure"].ge(0.16)
        & out["prev_relief_count_8"].ge(1))
    out["phase_transition_ok"] = (
        (out["first_short_after_pause"] & out["new_breakdown"])
        | out["reload_after_relief"]
        | out["ask_distribution_turn"])
    return out


def add_stock_personality(df: pd.DataFrame, train_days: Sequence[str]) -> pd.DataFrame:
    train = df[df["day"].isin(list(train_days))].copy()
    profile = (
        train.groupby("instrument")
        .agg(
            train_vol_p80=("volume_ratio", lambda s: float(s.quantile(0.80))),
            train_vol_p90=("volume_ratio", lambda s: float(s.quantile(0.90))),
            train_gap_p35=("close_atp_gap_bps", lambda s: float(s.quantile(0.35))),
            train_short_p70=("short_drive", lambda s: float(s.quantile(0.70))),
        ).reset_index())
    out = df.merge(profile, on="instrument", how="left")
    out["personality_vol_gate"] = out["train_vol_p80"].clip(lower=1.5, upper=6.0).fillna(3.0)
    out["personality_gap_gate"] = np.minimum(out["train_gap_p35"].fillna(-15.0), -15.0)
    out["personality_short_gate"] = np.maximum(out["train_short_p70"].fillna(0.60), 0.60)
    return out


def base_gate(df: pd.DataFrame) -> pd.Series:
    market_sell = (
        (df["market_median_move_open_pct"] <= -0.20)
        & (df["market_down_breadth"] >= 50)
        & (df["market_short_pressure"] >= df["market_long_pressure"])
        & (df["nifty_move_open_pct"] <= -0.20)
        & (df["nifty_short_pressure"] >= df["nifty_long_pressure"]))
    sector_sell = (
        (df["sector_move_open_pct"] <= -0.20)
        & (df["sector_down_breadth"] >= 55)
        & (df["sector_short_pressure"] >= df["sector_long_pressure"]))
    stock_short = (
        df["falcon_phase"].isin(["SHORT_DRIVE", "ATP_ASK_REVERSAL_DISTRIBUTION"])
        & (df["short_drive"] >= df["personality_short_gate"])
        & (df["long_drive"] <= 0.20)
        & (df["bid_absorption"] <= 0.30)
        & (df["atp_torque"] <= -0.70)
        & (df["net_aggression"] <= -0.35)
        & (df["close_atp_gap_bps"] <= df["personality_gap_gate"])
        & (df["volume_ratio"] >= df["personality_vol_gate"])
        & (df["phase_transition_ok"]))
    tick_ok = df["tick_buy%"].isna() | (df["tick_buy%"] <= 35)
    return market_sell & sector_sell & stock_short & tick_ok


def add_train_personality_quality(df: pd.DataFrame,
                                  train_days: Sequence[str]) -> pd.DataFrame:
    out = df.copy()
    out["base_gate"] = base_gate(out)
    train = out[out["day"].isin(list(train_days)) & out["base_gate"]
                & out["fwd_15m_bps"].notna()].copy()
    train["short_dir_15m_bps"] = -train["fwd_15m_bps"]
    quality = (
        train.groupby("instrument")
        .agg(
            train_gate_n=("instrument", "size"),
            train_gate_wr15=("short_dir_15m_bps", lambda s: float((s > 0).mean() * 100.0)),
            train_gate_avg15=("short_dir_15m_bps", "mean"),
        ).reset_index())
    out = out.merge(quality, on="instrument", how="left")
    out["personality_quality_ok"] = (
        out["train_gate_n"].fillna(0).ge(1)
        & out["train_gate_wr15"].fillna(0).ge(55)
        & out["train_gate_avg15"].fillna(-999).gt(0))
    return out


def grade(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    gate = out["base_gate"] & out["personality_quality_ok"]
    strong_market = (
        (out["market_median_move_open_pct"] <= -0.50)
        & (out["market_down_breadth"] >= 60)
        & (out["nifty_move_open_pct"] <= -0.50)
        & (out["sector_down_breadth"] >= 70))
    strong_stock = (
        (out["short_drive"] >= 0.68)
        & (out["long_drive"] <= 0.14)
        & (out["volume_ratio"] >= np.maximum(out["train_vol_p90"].fillna(4.0), 4.0))
        & (out["close_atp_gap_bps"] <= -30))
    out["v2_grade"] = "IGNORE"
    out.loc[out["falcon_phase"].isin(["SHORT_DRIVE", "ATP_ASK_REVERSAL_DISTRIBUTION"]),
            "v2_grade"] = "WATCH"
    out.loc[gate, "v2_grade"] = "A++"
    out.loc[gate & strong_market & strong_stock, "v2_grade"] = "A+++"
    out["v2_setup"] = np.select(
        [out["falcon_phase"].eq("SHORT_DRIVE"),
         out["falcon_phase"].eq("ATP_ASK_REVERSAL_DISTRIBUTION")],
        ["SHORT_RELOAD_OR_BREAKDOWN", "ASK_DISTRIBUTION_TURN"], default="NONE")
    return out


def select_trade_lifecycle(df: pd.DataFrame, cooldown_minutes: int = 30,
                           grades: Sequence[str] = ("A++", "A+++"),
                           grade_col: str = "v2_grade") -> pd.DataFrame:
    # grade_col defaults to "v2_grade" (byte-identical to the deployed v2 path);
    # the v3 DiD layer passes grade_col="v3_grade" to select on the v3 regrade.
    candidates = df[df[grade_col].isin(list(grades))].sort_values(
        ["instrument", "day", "bar_time"]).copy()
    rows = []
    for _, group in candidates.groupby(["instrument", "day"], sort=False):
        last_entry = None
        for idx, row in group.iterrows():
            if last_entry is None or (row["bar_time"] - last_entry).total_seconds() >= cooldown_minutes * 60:
                rows.append(idx)
                last_entry = row["bar_time"]
    return candidates.loc[rows].copy()


def grade_scored_frame(scored: pd.DataFrame, nifty_ctx: pd.DataFrame,
                       train_days: Sequence[str],
                       did_layer_enabled: bool = False) -> pd.DataFrame:
    """Run the full v2 grade over an already-scored cash frame (+ nifty ctx).
    Personality/quality use train_days only; grade applies to all rows.
    Returns the graded frame (adds v2_grade / v2_setup / base_gate / gates).

    did_layer_enabled=False (DEFAULT) → returns EXACTLY the v2 graded frame, so
    the deployed Tesla path is byte-identical. did_layer_enabled=True → additively
    appends the v3 DiD block + v3 regrade (v3_grade / v3_setup / v3_did_gate /
    v3_rank_score / did_* columns) WITHOUT mutating any v2 column. The pre5_*
    rolling means inside add_did_features are computed here, on the FULL intraday
    minute series (the frame still carries every minute per instrument — the
    latest-minute collapse happens later in compute_live_signals)."""
    df = add_market_context(scored, nifty_ctx)
    df = add_phase_transition_features(df)
    df = add_stock_personality(df, train_days)
    df = add_train_personality_quality(df, train_days)
    df = grade(df)
    if did_layer_enabled:
        from . import tesla_did as _did
        df = _did.apply_did_layer(df)
    return df


# ── Infer-only grading (per-minute fast path) ────────────────────────────────
#
# grade_scored_frame grades the FULL train+infer frame every minute, but only the
# infer day's rows are ever selected. Everything the grader needs for an infer row
# is either (a) an infer-day cross-section (market/sector context per (day,time),
# phase-transition per (instrument,day)) — computable from the infer frame ALONE —
# or (b) a TRAIN-derived per-instrument STAT (the personality profile + the
# train-gate quality). The two train stats change once per DAY, so we compute them
# ONCE per day-roll (from the train frame, exactly as grade_scored_frame would),
# cache them, and merge them onto the infer frame. Result: byte-identical to
# grade_scored_frame(train+infer)[day==infer] — proven by the infer-only parity
# test — while grading only the infer day's rows.

_PROFILE_COLS = ["train_vol_p80", "train_vol_p90", "train_gap_p35", "train_short_p70"]
_QUALITY_COLS = ["train_gate_n", "train_gate_wr15", "train_gate_avg15"]


def compute_train_stats(train_scored: pd.DataFrame, train_nifty: pd.DataFrame,
                        train_days: Sequence[str]) -> tuple:
    """Compute the two TRAIN-derived per-instrument stat tables the grader needs:
      * profile  — [instrument, train_vol_p80/p90, train_gap_p35, train_short_p70]
                   (add_stock_personality's raw groupby profile), and
      * quality  — [instrument, train_gate_n, train_gate_wr15, train_gate_avg15]
                   (add_train_personality_quality's train-gate stats).
    These are exactly what grade_scored_frame(train+infer) derives from the TRAIN
    rows; because add_market_context is a per-(day,time) cross-section, evaluating
    the pipeline on the train frame ALONE yields the identical train base_gate →
    identical quality (and the profile quantiles are per-instrument, train-only by
    construction). Empty train → empty tables (infer merges → NaN → the same
    fillna defaults the full path uses)."""
    if train_scored is None or train_scored.empty or not list(train_days):
        return (pd.DataFrame(columns=["instrument"] + _PROFILE_COLS),
                pd.DataFrame(columns=["instrument"] + _QUALITY_COLS))
    df = add_market_context(train_scored, train_nifty)
    df = add_phase_transition_features(df)
    df = add_stock_personality(df, train_days)
    df = add_train_personality_quality(df, train_days)
    profile = (df[["instrument"] + _PROFILE_COLS]
               .drop_duplicates("instrument").reset_index(drop=True))
    quality = (df.loc[df["train_gate_n"].notna(), ["instrument"] + _QUALITY_COLS]
               .drop_duplicates("instrument").reset_index(drop=True))
    return profile, quality


def _apply_cached_personality(df: pd.DataFrame, profile: pd.DataFrame) -> pd.DataFrame:
    """Merge the cached train profile + derive the personality gates — the non-
    train-groupby tail of add_stock_personality (byte-identical for infer rows)."""
    for c in _PROFILE_COLS:      # drop any stale columns before the merge
        if c in df.columns:
            df = df.drop(columns=[c])
    out = df.merge(profile, on="instrument", how="left")
    out["personality_vol_gate"] = out["train_vol_p80"].clip(lower=1.5, upper=6.0).fillna(3.0)
    out["personality_gap_gate"] = np.minimum(out["train_gap_p35"].fillna(-15.0), -15.0)
    out["personality_short_gate"] = np.maximum(out["train_short_p70"].fillna(0.60), 0.60)
    return out


def _apply_cached_quality(df: pd.DataFrame, quality: pd.DataFrame) -> pd.DataFrame:
    """Set base_gate on the infer frame + merge the cached train quality → the
    personality_quality_ok flag — the non-train tail of
    add_train_personality_quality (byte-identical for infer rows)."""
    out = df.copy()
    out["base_gate"] = base_gate(out)
    for c in _QUALITY_COLS:
        if c in out.columns:
            out = out.drop(columns=[c])
    out = out.merge(quality, on="instrument", how="left")
    out["personality_quality_ok"] = (
        out["train_gate_n"].fillna(0).ge(1)
        & out["train_gate_wr15"].fillna(0).ge(55)
        & out["train_gate_avg15"].fillna(-999).gt(0))
    return out


def grade_infer_only(infer_scored: pd.DataFrame, infer_nifty: pd.DataFrame,
                     train_days: Sequence[str],
                     profile: pd.DataFrame, quality: pd.DataFrame,
                     did_layer_enabled: bool = False,
                     vectorized: bool = False) -> pd.DataFrame:
    """Grade ONLY the infer-day scored frame using the cached train stats. Returns
    the same graded columns (v2_grade/v2_setup/base_gate/… + DiD when enabled) as
    grade_scored_frame would for the infer rows — byte-identical (proven).

    vectorized=False (DEFAULT) → the committed per-group phase-transition loop +
    the per-group DiD pre-mean (the exact oracle grade_scored_frame uses).
    vectorized=True → the multi-group VECTORISED phase-transition + fast DiD
    pre-mean (byte-identical to the loop; proven by the full-day parity test)."""
    df = add_market_context(infer_scored, infer_nifty)
    df = (add_phase_transition_features_vectorized(df) if vectorized
          else add_phase_transition_features(df))
    df = _apply_cached_personality(df, profile)
    df = _apply_cached_quality(df, quality)
    df = grade(df)
    if did_layer_enabled:
        from . import tesla_did as _did
        df = _did.apply_did_layer(df, fast=vectorized)
    return df


# ── Public live-inference entry point ────────────────────────────────────────

def _min_grades(min_grade: str) -> List[str]:
    if str(min_grade).upper() in ("A+++", "APLUS3", "STRONG"):
        return ["A+++"]
    return ["A++", "A+++"]


def _signal_from_row(r: "pd.Series", did_layer_enabled: bool) -> "TeslaSignal":
    """Build a TeslaSignal from a selected graded row. v2 (default) reads
    v2_grade/v2_setup and leaves rank_score=None (rotation ranks by short_drive,
    byte-identical to today). v3 reads v3_grade/v3_setup and carries v3_rank_score
    so rotation ranks seats by the DiD rank score."""
    grade_col = "v3_grade" if did_layer_enabled else "v2_grade"
    setup_col = "v3_setup" if did_layer_enabled else "v2_setup"
    rank_score = None
    if did_layer_enabled:
        rs = r.get("v3_rank_score")
        rank_score = None if pd.isna(rs) else float(rs)
    return TeslaSignal(
        instrument=str(r["instrument"]), day=str(r["day"]),
        time=str(r["time"]),
        bar_time=pd.to_datetime(r["bar_time"]).strftime("%Y-%m-%d %H:%M:%S"),
        grade=str(r[grade_col]), setup=str(r[setup_col]),
        entry_ref_price=float(r["close"]),
        short_drive=float(r["short_drive"]),
        sector=(None if pd.isna(r.get("sector")) else str(r.get("sector"))),
        rank_score=rank_score)


def compute_live_signals(
    *,
    as_of: Optional[str] = None,
    infer_day: Optional[str] = None,
    db_path: Optional[Path] = None,
    personality_window_days: int = 5,
    min_grade: str = "A++",
    cooldown_minutes: int = 30,
    latest_only: bool = True,
    did_layer_enabled: bool = False,
    con: Optional[sqlite3.Connection] = None,
) -> TeslaSignalResult:
    """Compute the CURRENT A++/A+++ SHORT signals from the live poll.

    did_layer_enabled : False (DEFAULT) → the deployed v2 path (byte-identical).
                 True → apply the v3 DiD layer: regrade to A++/A+++ via the
                 stricter v3_did_gate and rank seats by v3_rank_score. Exits/trail
                 are unaffected (v3 is entry-selection only).
    as_of      : ISO "YYYY-MM-DD HH:MM" — evaluate signals up to and including
                 this minute (bars strictly after it are ignored, so no
                 look-ahead). None → use the latest bar present for infer_day.
    infer_day  : the trading day being traded. None → the latest day in the poll.
    personality_window_days : rolling K completed prior days for personality/quality.
    min_grade  : "A++" (A++ and A+++) | "A+++" (only the strongest).
    cooldown_minutes : per-instrument minimum spacing between selected entries.
    latest_only: True → return only signals at the as_of minute (what a live tick
                 acts on); False → all selected signals for the day (backtest view).

    Returns a TeslaSignalResult; NEVER raises on empty data (returns empty list).
    """
    owns_con = con is None
    if owns_con:
        con = connect_db_readonly(db_path)
    try:
        days_all = available_trading_days(con)
        if not days_all:
            return TeslaSignalResult(as_of=as_of, infer_day=infer_day or "",
                                     train_days=[], note="no poll data")
        if infer_day is None:
            if as_of:
                infer_day = str(as_of)[:10]
            else:
                infer_day = days_all[-1]
        train_days = resolve_window(con, infer_day, personality_window_days)
        window_days = train_days + [infer_day]
        window_days = [d for d in window_days if d in set(days_all)]
        if infer_day not in window_days:
            return TeslaSignalResult(as_of=as_of, infer_day=infer_day,
                                     train_days=train_days,
                                     note=f"infer_day {infer_day} has no poll data")

        scored = score_universe_from_db(con, window_days)
        if scored.empty:
            return TeslaSignalResult(as_of=as_of, infer_day=infer_day,
                                     train_days=train_days,
                                     note="no scored candidates")
        n_candidates = int(scored["instrument"].nunique())
        nifty_ctx = load_nifty_context(con, window_days)
        graded = grade_scored_frame(scored, nifty_ctx, train_days,
                                    did_layer_enabled=did_layer_enabled)

        grade_col = "v3_grade" if did_layer_enabled else "v2_grade"
        selected = select_trade_lifecycle(
            graded, cooldown_minutes=cooldown_minutes,
            grades=_min_grades(min_grade), grade_col=grade_col)
        # Restrict to the inference day.
        selected = selected[selected["day"] == infer_day].copy()
        if selected.empty:
            return TeslaSignalResult(as_of=as_of, infer_day=infer_day,
                                     train_days=train_days, n_candidates=n_candidates,
                                     note="no A++/A+++ signals for infer_day")

        # As-of cutoff (no look-ahead): keep bars <= as_of.
        cutoff = pd.to_datetime(as_of) if as_of is not None else None
        if cutoff is not None:
            selected = selected[selected["bar_time"] <= cutoff].copy()

        # latest_only → what a live tick acts on: the signals AT the current
        # minute only. as_of set → exactly that minute; else → the newest bar
        # that produced a signal today.
        if latest_only and not selected.empty:
            target_bar = cutoff if cutoff is not None else selected["bar_time"].max()
            selected = selected[selected["bar_time"] == target_bar].copy()

        sigs: List[TeslaSignal] = []
        for _, r in selected.sort_values(["bar_time", "instrument"]).iterrows():
            sigs.append(_signal_from_row(r, did_layer_enabled))
        return TeslaSignalResult(as_of=as_of, infer_day=infer_day,
                                 train_days=train_days, signals=sigs,
                                 n_candidates=n_candidates)
    finally:
        if owns_con:
            con.close()


# ── OPTIMIZED once-per-minute recompute (train-day cache + latest-minute) ─────
#
# compute_live_signals rescored the FULL window (train + infer days) on every
# call — ~120s. But the ROLLING personality/train-quality inputs derive ONLY
# from the COMPLETED train days: they change once per DAY, not per minute. This
# path caches the scored TRAIN frame (+ train NIFTY context) per (db, train_days)
# and rebuilds it ONLY on a day-roll; per minute it rescored just the INFER day
# (for the SAME union candidate set the full recompute uses) and feeds the exact
# same grade pipeline. Result: byte-identical signals to compute_live_signals for
# a given minute, in a few seconds instead of ~120s (proven by the parity test).


@dataclass
class _TrainDayCache:
    db_key: str
    train_days: tuple
    train_scored: pd.DataFrame
    train_nifty: pd.DataFrame
    covered: set          # symbols whose TRAIN-day rows are in train_scored
    built_at: str
    # Cached per-day-roll invariants (opt 2 + 3): the TRAIN candidate symbol list
    # (candidate_symbols(train) is invariant intraday) + the two train-derived
    # per-instrument stat tables the infer-only grader merges.
    train_candidates: tuple = ()
    train_profile: pd.DataFrame = field(default_factory=pd.DataFrame)
    train_quality: pd.DataFrame = field(default_factory=pd.DataFrame)


_TRAIN_CACHE: Dict[str, _TrainDayCache] = {}
_TRAIN_CACHE_LOCK = threading.RLock()
# Diagnostics — full REBUILDs (day-roll) vs incremental EXTENDs (new symbol).
_train_cache_build_count = 0
_train_cache_extend_count = 0


def reset_train_cache() -> None:
    """Drop the train-day cache AND the incremental infer-day caches (tests / a
    forced refresh) — resetting the train window also invalidates the infer state
    it pairs with, so a test that resets between recomputes gets a clean slate."""
    global _train_cache_build_count, _train_cache_extend_count
    with _TRAIN_CACHE_LOCK:
        _TRAIN_CACHE.clear()
        _train_cache_build_count = 0
        _train_cache_extend_count = 0
    reset_infer_caches()


def _train_cache_key(db_key: str, train_days: Sequence[str]) -> str:
    return f"{db_key}|{','.join(train_days)}"


def _build_train_cache(con: sqlite3.Connection, db_key: str,
                       train_days: Sequence[str],
                       train_candidates: Sequence[str],
                       symbols: Sequence[str],
                       vectorized: bool = False) -> _TrainDayCache:
    """FULL build of the train-day scored frame + NIFTY context + the cached
    train-derived stat tables (a day-roll). Scores the union `symbols` over the
    completed train days ONCE, then derives the personality profile + train-gate
    quality from that frame."""
    global _train_cache_build_count
    tdays = tuple(train_days)
    if tdays:
        train_scored = score_universe_for_symbols(con, list(tdays), symbols,
                                                  vectorized=vectorized)
        train_nifty = load_nifty_context(con, list(tdays))
    else:  # first trading day in the poll — no completed prior days
        train_scored = pd.DataFrame()
        train_nifty = pd.DataFrame()
    profile, quality = compute_train_stats(train_scored, train_nifty, tdays)
    entry = _TrainDayCache(
        db_key=db_key, train_days=tdays, train_scored=train_scored,
        train_nifty=train_nifty, covered=set(symbols),
        built_at=datetime.now(IST).isoformat(),
        train_candidates=tuple(train_candidates),
        train_profile=profile, train_quality=quality)
    _train_cache_build_count += 1
    return entry


def _get_train_cache(con: sqlite3.Connection, db_key: str,
                     train_days: Sequence[str],
                     infer_candidates: Sequence[str],
                     vectorized: bool = False) -> _TrainDayCache:
    """Return the train-day cache for (db_key, train_days), REBUILDING only on a
    day-roll (train_days change) and INCREMENTALLY extending it if the infer-day
    candidate set has grown to include symbols not yet covered on the train days
    (keeps parity with the full recompute's union candidate set without a full
    rebuild). candidate_symbols(train) is discovered ONCE on build and cached
    (invariant intraday); the union that must be scored on the train days is
    train_candidates ∪ infer_candidates. When that union grows, train_scored is
    extended AND the train stats are recomputed (train market/sector medians shift
    when a symbol is added), so parity with the full recompute holds."""
    global _train_cache_extend_count
    key = _train_cache_key(db_key, train_days)
    infer_set = set(infer_candidates)
    with _TRAIN_CACHE_LOCK:
        entry = _TRAIN_CACHE.get(key)
        if entry is None or entry.train_days != tuple(train_days):
            train_cands = (candidate_symbols(con, train_days)
                           if tuple(train_days) else [])
            union = sorted(set(train_cands) | infer_set)
            entry = _build_train_cache(con, db_key, train_days,
                                       train_cands, union,
                                       vectorized=vectorized)
            _TRAIN_CACHE.clear()  # only ever ONE train-day window is live
            _TRAIN_CACHE[key] = entry
            return entry
        need = set(entry.train_candidates) | infer_set
        missing = need - entry.covered
        if missing and tuple(train_days):
            extra = score_universe_for_symbols(con, list(train_days),
                                               sorted(missing),
                                               vectorized=vectorized)
            if not extra.empty:
                entry.train_scored = (
                    extra if entry.train_scored.empty
                    else pd.concat([entry.train_scored, extra],
                                   ignore_index=True))
            entry.covered |= missing
            # Train medians (add_market_context) shift when the candidate set
            # grows → recompute the cached stats over the extended train frame.
            entry.train_profile, entry.train_quality = compute_train_stats(
                entry.train_scored, entry.train_nifty, tuple(train_days))
            _train_cache_extend_count += 1
        elif missing:
            entry.covered |= missing
        return entry


def compute_live_signals_fast(
    *,
    as_of: Optional[str] = None,
    infer_day: Optional[str] = None,
    db_path: Optional[Path] = None,
    personality_window_days: int = 5,
    min_grade: str = "A++",
    cooldown_minutes: int = 30,
    latest_only: bool = True,
    did_layer_enabled: bool = False,
    vectorized: bool = False,
    incremental: bool = False,
    con: Optional[sqlite3.Connection] = None,
) -> TeslaSignalResult:
    """Optimized equivalent of compute_live_signals: caches the scored TRAIN
    frame per day-roll and rescored only the INFER day per call. Produces the
    SAME A++/A+++ signals for a given minute (parity test), far faster. Same
    signature/semantics as compute_live_signals (incl. did_layer_enabled); NEVER
    raises on empty data. The DiD layer is a post-grade step on the same scored
    frame, so the train-day cache is unaffected (it caches the v2 scored inputs,
    not the grade).

    vectorized=False (DEFAULT) → the committed per-group microstructure/phase-
    transition loop (the reviewed ~46s path). vectorized=True → the multi-group
    VECTORISED feature pipeline (byte-identical to the loop; proven by the full-day
    all-columns minute-by-minute parity test) — the sub-10s once-per-minute path.
    OFF by default: the committed loop stays the default until the parity proof is
    reviewed and the operator sets tesla_vectorized_features=true.

    incremental=False (DEFAULT) → the infer day + its candidate row-counts are
    fully re-read from the poll DB each call. incremental=True (round-2 opt) → the
    finalized infer-day bars + candidate counts are cached in-process and only the
    NEW minute(s) are read (bar_time > cached_max); the SAME vectorised scoring
    then runs on the reassembled frame. Byte-identical (the poller's bars are
    immutable — INSERT OR IGNORE, append-only; proven by the incremental parity
    test), and it drops the ~11s infer-day DB read/scan to <0.5s. Gated OFF until
    the operator flips tesla_incremental_read=true."""
    owns_con = con is None
    if owns_con:
        con = connect_db_readonly(db_path)
    db_key = Path(db_path).as_posix() if db_path else DEFAULT_DB_PATH.as_posix()
    try:
        days_all = available_trading_days(con)
        if not days_all:
            return TeslaSignalResult(as_of=as_of, infer_day=infer_day or "",
                                     train_days=[], note="no poll data")
        if infer_day is None:
            infer_day = str(as_of)[:10] if as_of else days_all[-1]
        train_days = resolve_window(con, infer_day, personality_window_days)
        if infer_day not in set(days_all):
            return TeslaSignalResult(as_of=as_of, infer_day=infer_day,
                                     train_days=train_days,
                                     note=f"infer_day {infer_day} has no poll data")

        # Union candidate set = train candidates ∪ infer candidates == the window
        # candidate set _get_cash_candidates(train+infer) would produce. The train
        # candidates + train-derived stats are cached per day-roll; only the infer
        # candidates are rediscovered each minute (one batched GROUP BY).
        infer_cands = (_incremental_infer_candidates(con, db_key, infer_day)
                       if incremental
                       else candidate_symbols(con, [infer_day]))
        cache = _get_train_cache(con, db_key, train_days, infer_cands,
                                 vectorized=vectorized)
        all_syms = sorted(set(cache.train_candidates) | set(infer_cands))
        if not all_syms:
            return TeslaSignalResult(as_of=as_of, infer_day=infer_day,
                                     train_days=train_days,
                                     note="no scored candidates")

        # Rescore ONLY the infer day (the train frame is cached). n_candidates is
        # the union instrument set with data on ANY window day — matching the full
        # recompute's score_universe_from_db(window).nunique(). incremental=True
        # reassembles the infer-day raw frame from the in-process bar cache (only
        # new minutes read) and runs the SAME _finalize_scored_from_raw — the
        # reassembled rows are byte-identical to a full re-read.
        if incremental:
            infer_raw = _incremental_infer_raw(con, db_key, infer_day, all_syms)
            infer_scored = _finalize_scored_from_raw(con, infer_raw, all_syms,
                                                     vectorized=vectorized)
        else:
            infer_scored = score_universe_for_symbols(con, [infer_day], all_syms,
                                                      vectorized=vectorized)
        inst = set()
        if cache.train_scored is not None and not cache.train_scored.empty:
            inst |= set(cache.train_scored["instrument"].unique())
        if not infer_scored.empty:
            inst |= set(infer_scored["instrument"].unique())
        n_candidates = len(inst)
        if infer_scored.empty:
            note = ("no A++/A+++ signals for infer_day" if n_candidates
                    else "no scored candidates")
            return TeslaSignalResult(as_of=as_of, infer_day=infer_day,
                                     train_days=train_days,
                                     n_candidates=n_candidates, note=note)

        # Grade the infer day ONLY, using the cached train stats — byte-identical
        # to grade_scored_frame(train+infer)[day==infer].
        infer_nifty = load_nifty_context(con, [infer_day])
        graded = grade_infer_only(
            infer_scored, infer_nifty, train_days,
            cache.train_profile, cache.train_quality,
            did_layer_enabled=did_layer_enabled, vectorized=vectorized)
        grade_col = "v3_grade" if did_layer_enabled else "v2_grade"
        selected = select_trade_lifecycle(
            graded, cooldown_minutes=cooldown_minutes,
            grades=_min_grades(min_grade), grade_col=grade_col)
        selected = selected[selected["day"] == infer_day].copy()
        if selected.empty:
            return TeslaSignalResult(as_of=as_of, infer_day=infer_day,
                                     train_days=train_days, n_candidates=n_candidates,
                                     note="no A++/A+++ signals for infer_day")
        cutoff = pd.to_datetime(as_of) if as_of is not None else None
        if cutoff is not None:
            selected = selected[selected["bar_time"] <= cutoff].copy()
        if latest_only and not selected.empty:
            target_bar = cutoff if cutoff is not None else selected["bar_time"].max()
            selected = selected[selected["bar_time"] == target_bar].copy()

        sigs: List[TeslaSignal] = []
        for _, r in selected.sort_values(["bar_time", "instrument"]).iterrows():
            sigs.append(_signal_from_row(r, did_layer_enabled))
        return TeslaSignalResult(as_of=as_of, infer_day=infer_day,
                                 train_days=train_days, signals=sigs,
                                 n_candidates=n_candidates)
    finally:
        if owns_con:
            con.close()
