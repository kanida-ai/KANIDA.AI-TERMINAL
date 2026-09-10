"""
Point-in-time market data. THE enforcement point for "no look-ahead".

Every read goes through a `PriceFrames` built with a hard `as_of`. The frame
physically does not contain a bar after its `as_of`, so a look-ahead bug is a
`KeyError`, not a silently optimistic number. A discovery-phase caller is handed a
frame sealed at `discovery_end`; it cannot see the validation window, let alone the
book window, even if it tries.

KNOWN LIMITATION — THE UNIVERSE IS TODAY'S INDEX MEMBERSHIP (measured on the universe
the engine actually queries, not on a superset of it):

`load_universe` runs `in_nifty500 = 1 AND is_active = 1`, which returns **501 symbols,
498 with bars**. Two distinct problems follow, and they are not the same problem:

  1. **Membership look-ahead.** 182 of the 498 have no bar at the discovery window's
     start and 84 first trade after it *closes*. They are in a 2016 backtest for one
     reason: they are in the Nifty 500 today.
  2. **Survivorship.** `is_active = 1` *causes* the perfect survival it would be easy to
     mistake for luck: **0 of the 498 stop trading before the last session.** Names
     delisted or merged away during the history are excluded by construction.

Both inflate ABSOLUTE returns. The engine's response is not to pretend otherwise: it
treats `edge_vs_baseline` — signal minus the SAME universe over the SAME window under the
SAME exits — as the load-bearing statistic, because the bias is common to both terms and
largely cancels there, and it publishes absolute expectancy only alongside this caveat.
An adversarial audit measured the residual: splitting discovery by each symbol's eventual
2016→2026 appreciation (unknowable at t) moves the edge between 1.15 and 2.24 while the
absolute expectancy moves far more — so the mitigation holds for the edge and does NOT
rescue the absolute number. The real fix is a point-in-time index-membership history,
which the warehouse does not have.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date
from functools import cached_property
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd

from .config import EngineConfig

#: Attached to every provenance the engine emits. States the filter that actually runs.
SURVIVORSHIP_NOTE = (
    "Universe = today's Nifty 500 membership (in_nifty500=1 AND is_active=1): 498 names "
    "with bars, of which 0 stop trading before the last session and 182 were not listed "
    "at the discovery window's start. Both membership look-ahead and survivorship inflate "
    "ABSOLUTE returns; edge_vs_baseline compares against the same biased universe and is "
    "the statistic to read."
)


class LookAheadError(RuntimeError):
    """A caller asked for data after its `as_of`. This is always a bug."""


def _connect(db_path: str) -> sqlite3.Connection:
    p = Path(db_path)
    if not p.exists():
        raise FileNotFoundError(f"price warehouse not found: {db_path}")
    return sqlite3.connect(f"file:{p.as_posix()}?mode=ro", uri=True)


@dataclass(frozen=True)
class Universe:
    """The symbol set + how it was resolved. Serialised into provenance."""
    universe_id: str
    symbols: tuple[str, ...]
    note: str = SURVIVORSHIP_NOTE


def load_universe(cfg: EngineConfig) -> Universe:
    """Master membership only — the *tradeable* filter is point-in-time, see `liquid_mask`."""
    with _connect(cfg.price_db) as con:
        rows = con.execute(
            "SELECT symbol FROM instrument_labels "
            "WHERE in_nifty500=1 AND instrument_type='STOCK' AND is_active=1 "
            "ORDER BY symbol"
        ).fetchall()
    return Universe(cfg.universe_id, tuple(r[0] for r in rows))


class PriceFrames:
    """
    Wide (date × symbol) OHLCV frames, hard-sealed at `as_of`.

    Construct via `PriceFrames.load(cfg, as_of=...)`. Nothing else in the engine
    opens the price warehouse.
    """

    def __init__(
        self,
        *,
        as_of: date,
        opens: pd.DataFrame,
        highs: pd.DataFrame,
        lows: pd.DataFrame,
        closes: pd.DataFrame,
        volumes: pd.DataFrame,
        index_close: pd.Series,
        universe: Universe,
        data_source: str,
        cfg: EngineConfig,
        requested_as_of: Optional[date] = None,
    ) -> None:
        if not closes.empty and closes.index[-1].date() > as_of:
            raise LookAheadError("frame contains a bar after as_of — seal broken at construction")
        self.as_of = as_of
        #: What the caller asked for, before it was clamped to the last real session.
        #: `staleness_sessions` is the gap, and it is published as a fact.
        self.requested_as_of = requested_as_of or as_of
        self.o, self.h, self.l, self.c, self.v = opens, highs, lows, closes, volumes
        self.index_close = index_close
        self.universe = universe
        self.data_source = data_source
        self.cfg = cfg

    # ── construction ────────────────────────────────────────────────────────

    @classmethod
    def load(
        cls,
        cfg: EngineConfig,
        *,
        as_of: date,
        start: Optional[date] = None,
        universe: Optional[Universe] = None,
    ) -> "PriceFrames":
        uni = universe or load_universe(cfg)
        start = start or cfg.history_start
        with _connect(cfg.price_db) as con:
            ph = ",".join("?" * len(uni.symbols))
            q = (
                "SELECT symbol, substr(bar_time,1,10) AS d, open, high, low, close, volume "
                "FROM ohlc_daily WHERE bar_time >= ? AND bar_time < ? "
                f"AND symbol IN ({ph})"
            )
            # `< as_of + 1 day` as a string bound: bar_time is 'YYYY-MM-DD hh:mm:ss'.
            raw = pd.read_sql_query(
                q, con,
                params=[start.isoformat(), (as_of.isoformat() + "z"), *uni.symbols],
            )
            idx = pd.read_sql_query(
                "SELECT substr(bar_time,1,10) AS d, close FROM ohlc_daily "
                "WHERE symbol = ? AND bar_time >= ? AND bar_time < ? ORDER BY d",
                con, params=[cfg.index_symbol, start.isoformat(), as_of.isoformat() + "z"],
            )
        if raw.empty:
            raise RuntimeError("no price data in the requested window")

        # The warehouse's PK is (symbol, bar_time); a date can appear twice with
        # different time parts. Keep the LAST row for a date — never merge them.
        raw = raw.drop_duplicates(subset=["symbol", "d"], keep="last")
        raw["d"] = pd.to_datetime(raw["d"])

        def wide(col: str) -> pd.DataFrame:
            return raw.pivot(index="d", columns="symbol", values=col).sort_index()

        closes = wide("close")
        idx = idx.drop_duplicates(subset=["d"], keep="last")
        idx["d"] = pd.to_datetime(idx["d"])
        index_close = idx.set_index("d")["close"].reindex(closes.index).ffill()

        # The effective as_of is the last session the warehouse actually has. Claiming
        # a window that runs to today when the data stops five weeks ago would make
        # every provenance block on this run subtly false.
        effective = min(as_of, closes.index[-1].date())

        return cls(
            as_of=effective, requested_as_of=as_of,
            opens=wide("open"), highs=wide("high"), lows=wide("low"),
            closes=closes, volumes=wide("volume"),
            index_close=index_close, universe=uni, data_source=cfg.data_source, cfg=cfg,
        )

    def sealed_at(self, as_of: date) -> "PriceFrames":
        """
        A strictly narrower view. Used to hand the discovery phase a frame that
        cannot see the validation or book windows.
        """
        if as_of > self.as_of:
            raise LookAheadError(f"cannot re-seal forward: {as_of} > {self.as_of}")
        keep = self.c.index <= pd.Timestamp(as_of)
        return PriceFrames(
            as_of=as_of,
            opens=self.o[keep], highs=self.h[keep], lows=self.l[keep],
            closes=self.c[keep], volumes=self.v[keep],
            index_close=self.index_close[keep],
            universe=self.universe, data_source=self.data_source, cfg=self.cfg,
            requested_as_of=as_of,
        )

    # ── derived, all strictly backward-looking ──────────────────────────────

    @property
    def dates(self) -> pd.DatetimeIndex:
        return self.c.index

    @property
    def staleness_sessions(self) -> int:
        """Calendar days between the last real session and what the caller asked for."""
        return max(0, (self.requested_as_of - self.as_of).days)

    def session_slice(self, start: date, end: date) -> pd.DatetimeIndex:
        if end > self.as_of:
            raise LookAheadError(f"window end {end} is after the frame's as_of {self.as_of}")
        d = self.c.index
        return d[(d >= pd.Timestamp(start)) & (d <= pd.Timestamp(end))]

    @cached_property
    def turnover(self) -> pd.DataFrame:
        """Close × volume. A proxy for traded value; the warehouse has no notional."""
        return self.c * self.v

    @cached_property
    def liquid_mask(self) -> pd.DataFrame:
        """
        Tradeable-at-t mask. Uses only bars ≤ t: a rolling median of traded value.

        The price floor is DISABLED by default (`min_price_inr = 0`) and the reason is
        worth keeping: prices here are back-adjusted, so whether a 2016 bar clears a ₹20
        floor depends on splits that happen *after* 2016 — look-ahead, and measurably so
        (it removed 1.8% of liquid cells, led by names that later multiplied 3–35×, i.e.
        it was quietly deleting future winners from the discovery window). The turnover
        filter is point-in-time and does the work on its own. The parameter survives for
        whoever adds an unadjusted price series.
        """
        cfg = self.cfg
        med = self.turnover.rolling(cfg.turnover_lookback, min_periods=cfg.turnover_lookback).median()
        mask = med >= cfg.min_median_turnover_inr
        if cfg.min_price_inr > 0:
            mask = mask & (self.c >= cfg.min_price_inr)
        return mask

    @cached_property
    def returns(self) -> pd.DataFrame:
        return self.c.pct_change()

    def rolling_max(self, n: int, *, exclude_today: bool = True) -> pd.DataFrame:
        src = self.c.shift(1) if exclude_today else self.c
        return src.rolling(n, min_periods=n).max()

    def rolling_min(self, n: int, *, exclude_today: bool = True) -> pd.DataFrame:
        src = self.c.shift(1) if exclude_today else self.c
        return src.rolling(n, min_periods=n).min()

    def sma(self, n: int) -> pd.DataFrame:
        return self.c.rolling(n, min_periods=n).mean()

    def vol_ratio(self, n: int = 20) -> pd.DataFrame:
        """Today's volume vs its own trailing median. Never uses tomorrow."""
        return self.v / self.v.rolling(n, min_periods=n).median()

    @cached_property
    def atr_pct(self) -> pd.DataFrame:
        """20-session ATR as a % of close. Wilder's TR, strictly trailing."""
        pc = self.c.shift(1)
        tr = pd.concat(
            [(self.h - self.l), (self.h - pc).abs(), (self.l - pc).abs()]
        ).groupby(level=0).max()
        return tr.rolling(20, min_periods=20).mean() / self.c * 100.0

    # ── market-state series (for the observer) ──────────────────────────────

    @cached_property
    def breadth_above_50dma(self) -> pd.Series:
        return (self.c > self.sma(50)).where(self.c.notna()).mean(axis=1)

    @cached_property
    def breadth_above_200dma(self) -> pd.Series:
        return (self.c > self.sma(200)).where(self.c.notna()).mean(axis=1)

    @cached_property
    def index_realised_vol_20d(self) -> pd.Series:
        return self.index_close.pct_change().rolling(20, min_periods=20).std() * np.sqrt(252)

    @cached_property
    def index_above_200dma(self) -> pd.Series:
        return self.index_close > self.index_close.rolling(200, min_periods=200).mean()

    @cached_property
    def median_vol_ratio(self) -> pd.Series:
        """Cross-sectional median of each name's volume-vs-median. Dry-up detector."""
        return self.vol_ratio(20).median(axis=1)


def expanding_percentile(series: pd.Series, *, min_periods: int = 250) -> pd.Series:
    """
    Where does today sit in its own history — using only sessions STRICTLY BEFORE it?

    A full-sample `rank(pct=True)` would be look-ahead: the classic silent one, because
    it produces a perfectly plausible number. This is the point-in-time version and the
    observer uses nothing else.

    Today is excluded from its own reference set, so the reading answers "how unusual is
    this against what I had already seen?". A reading with fewer than `min_periods` prior
    observations is NaN rather than a guess.
    """
    vals = series.to_numpy(dtype=float)
    out = np.full(len(vals), np.nan)
    seen: list[float] = []
    for i, x in enumerate(vals):
        if len(seen) >= min_periods and np.isfinite(x):
            arr = np.asarray(seen)
            out[i] = float((arr <= x).mean())
        if np.isfinite(x):
            seen.append(x)
    return pd.Series(out, index=series.index)


def sessions_between(dates: pd.DatetimeIndex, start: date, end: date) -> Iterable[pd.Timestamp]:
    return (d for d in dates if pd.Timestamp(start) <= d <= pd.Timestamp(end))
