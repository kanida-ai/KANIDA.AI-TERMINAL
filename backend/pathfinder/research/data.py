"""
Point-in-time market data for the research engine.

THE SEAL. A `MarketData` is built for one `as_of` and physically holds no bar after it.
Backward-looking columns (returns, moving averages, volume baselines) are causal and are
computed once on the full history; FORWARD-looking columns (the outcomes a base rate is
measured on) are minted only inside the seal:

    f{h}   = close[t+h] / open[t+1] - 1     entry = NEXT OPEN after the signal close,
                                            exit  = close of the h-th session after it
    r5cc   = close[t+5] / close[t] - 1      the week-later move from the signal close — the
                                            statistic the volume-anomaly prototype measures

A forward value exists for a row only when its resolving bar is on or before `as_of`
(`available_from = entry_idx + h <= today_idx`). Otherwise it is NaN, and NaN is not a number:
every base rate below is computed over `.notna()` rows, so an unresolved occurrence can never
leak into a statistic. `test_pathfinder_s1.py` proves seal-invariance: the frame sealed at D
equals the frame built from history truncated at D.

The sealed frame PHYSICALLY holds no post-seal price (S1 audit P7): the helper columns the
forward outcomes are minted from (`next_open`, `_d{h}`, `_c{h}`, `_badfwd{h}`) live only on
the unsealed history kept for re-sealing and are dropped from every sealed frame.

Glitch bars (S1 audit, data quality): the warehouse carries six listing-day / corporate-action
bars where the close-to-close ratio is > 4x or < 0.25x (DELHIVERY +9,200% on its listing day,
COHANCE +112,000%, ...). Those are not prices. A session whose ratio is outside [0.25, 4] is a
HOLE: its return is NaN, and any forward outcome whose window straddles it is NaN. Before this
guard the surge template counted them as +6% "jumps" and a dip's five-day mean was +14% off one
831x "return".

Corporate-action days (S1 second audit A4): the warehouse is split/bonus-adjusted but NOT
demerger-adjusted (CGPOWER −71.7% on 2016-03-15, TATACHEM −56.2%, ABFRL −55.9%, ADANIENT
−41.9%) and carries unadjusted split-like prints (JBCHEPHARM −49% on 2023-09-18, SPLPETRO
−50%). A split / bonus / demerger / rights ex-date from the `corp_actions` table (`_ca`), or
any single-day |move| beyond `corp_action_ret_guard_pct` (`_ca_suspect`, a SUSPECTED
corporate action — the table only starts in 2020), is treated exactly like a glitch bar: the
return is NaN, so it can be neither a dip / surge case nor the day's subject, and every
forward window straddling it is NaN.

Synthetic opens (A7): a bar whose open equals its close to the tick (`_synth_open`) is a
placeholder open, not a tradeable price (1.5% of bars; 4.2% in 2013). No forward outcome is
minted from an entry at such an open: `f{h}` for the row before it is NaN.

Universe = today's Nifty-500 membership (`in_nifty500=1 AND is_active=1`). That is a
survivorship-biased universe for ABSOLUTE returns (see engine/market.py for the measured
extent); it is stated on every provenance as `universe` and in `data_disclosures`.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .config import ResearchConfig

HORIZONS = (1, 3, 5)
MOVE_WINDOW = 5
#: A one-session close-to-close ratio outside this band is a data hole, not a price move.
GLITCH_RATIO = (0.25, 4.0)
#: Corporate-action types whose ex-date changes the price basis. Dividends, AGMs, buybacks do not.
CORP_ACTION_TYPES = ("split", "bonus", "demerger", "rights")
#: Columns that carry post-seal information on the unsealed history. Never present on a sealed frame.
POST_SEAL_COLUMNS = ("next_open", "_next_synth") + tuple(f"_d{h}" for h in HORIZONS) \
    + tuple(f"_c{h}" for h in HORIZONS) + tuple(f"_badfwd{h}" for h in HORIZONS)


@dataclass(frozen=True)
class DataExclusions:
    """What the data rules removed, for the edition record and the provenance disclosure."""
    corp_action_bars: int
    suspected_corp_action_bars: int
    glitch_bars: int
    synthetic_open_bars: int
    corp_actions_loaded: int

    def as_dict(self) -> dict[str, int]:
        return {"corp_action_bars": self.corp_action_bars,
                "suspected_corp_action_bars": self.suspected_corp_action_bars,
                "glitch_bars": self.glitch_bars, "synthetic_open_bars": self.synthetic_open_bars,
                "corp_actions_loaded": self.corp_actions_loaded}


class LookAheadError(RuntimeError):
    """A caller asked for data after its `as_of`. Always a bug."""


def _connect(db_path: str) -> sqlite3.Connection:
    p = Path(db_path)
    if not p.exists():
        raise FileNotFoundError(f"price warehouse not found: {db_path} (set KANIDA_DB)")
    return sqlite3.connect(f"file:{p.as_posix()}?mode=ro", uri=True)


def load_corp_actions(con: sqlite3.Connection) -> Optional[pd.DataFrame]:
    """The warehouse's `corp_actions` table (symbol, ex_date, action_type), or None if absent."""
    has = con.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'corp_actions'").fetchone()
    if not has:
        return None
    return pd.read_sql_query("SELECT symbol, substr(ex_date,1,10) AS ex_date, action_type FROM corp_actions", con)


def _causal_columns(df: pd.DataFrame, cfg: ResearchConfig,
                    corp_actions: Optional[pd.DataFrame] = None) -> tuple[pd.DataFrame, DataExclusions]:
    """Backward-looking derived columns. Safe to compute on the full history."""
    df = df.sort_values(["symbol", "d"], kind="mergesort").reset_index(drop=True)
    # A zero or negative price is a data hole, not a price (MAZDOCK carries eight 0.0 bars).
    # NaN it so no return, ratio or base rate can be minted from it.
    for c in ("open", "close"):
        df.loc[~(df[c] > 0), c] = np.nan
    df.loc[~(df["volume"] >= 0), "volume"] = np.nan
    g = df.groupby("symbol", sort=False)
    df["ret"] = g["close"].pct_change(fill_method=None)
    # Glitch bars: a >4x or <0.25x close-to-close ratio is a hole. The return is NaN and the
    # bar is remembered (`_bad`) so forward windows straddling it can be blanked in `sealed()`.
    ratio = 1.0 + df["ret"]
    glitch = ratio.notna() & ((ratio > GLITCH_RATIO[1]) | (ratio < GLITCH_RATIO[0]))
    # Corporate-action days (A4): the ex-date from the NSE table, and any |move| beyond the
    # guard as a suspected one. Same treatment as a glitch bar.
    ca = pd.Series(False, index=df.index)
    n_ca_loaded = 0
    if cfg.exclude_corp_actions and corp_actions is not None and len(corp_actions):
        cax = corp_actions[corp_actions["action_type"].isin(CORP_ACTION_TYPES)]
        n_ca_loaded = int(len(cax))
        keys = set(zip(cax["symbol"].astype(str), cax["ex_date"].astype(str).str.slice(0, 10)))
        if keys:
            ca = pd.Series([(s, d) in keys for s, d in zip(df["symbol"], df["d"])], index=df.index)
    suspect = pd.Series(False, index=df.index)
    if cfg.exclude_corp_actions:
        suspect = df["ret"].notna() & (df["ret"].abs() > cfg.corp_action_ret_guard_pct / 100.0) & ~ca & ~glitch
    bad = glitch | ca | suspect
    df.loc[bad, "ret"] = np.nan
    df["_bad"] = bad.astype(int)
    df["_ca"] = ca.astype(int)
    df["_ca_suspect"] = suspect.astype(int)
    # Synthetic opens (A7): open == close to the tick is a placeholder, not an entry price.
    synth = df["open"].notna() & df["close"].notna() & (df["open"] == df["close"]) if cfg.exclude_synthetic_opens \
        else pd.Series(False, index=df.index)
    df["_synth_open"] = synth.astype(int)
    excl = DataExclusions(corp_action_bars=int(ca.sum()), suspected_corp_action_bars=int(suspect.sum()),
                          glitch_bars=int(glitch.sum()), synthetic_open_bars=int(synth.sum()),
                          corp_actions_loaded=n_ca_loaded)
    g = df.groupby("symbol", sort=False)
    df["oc"] = df["close"] / df["open"] - 1.0                 # open -> close, same session
    df.loc[synth, "oc"] = np.nan
    df["ma20"] = g["close"].transform(lambda s: s.rolling(20).mean())
    df["vol20"] = g["volume"].transform(lambda s: s.rolling(20).mean().shift(1))
    df["r15"] = df["close"] / g["close"].shift(15) - 1.0
    df.loc[g["_bad"].transform(lambda s: s.rolling(15, min_periods=1).sum()) > 0, "r15"] = np.nan
    # Dates of the bars a forward column would resolve on. Used by `sealed()` to blank
    # any outcome whose resolving bar lies past the seal. POST-SEAL information: these
    # columns exist only on the unsealed history and are dropped by `sealed()`.
    df["next_open"] = g["open"].shift(-1)
    df["_next_synth"] = g["_synth_open"].shift(-1).fillna(0).astype(int)   # the entry open is synthetic
    for h in HORIZONS:
        df[f"_d{h}"] = g["d"].shift(-h)
        df[f"_c{h}"] = g["close"].shift(-h)
        fwd = sum(g["_bad"].shift(-k).fillna(0) for k in range(1, h + 1))
        df[f"_badfwd{h}"] = fwd.astype(int)                   # a glitch / corp-action bar inside (t, t+h]
    return df, excl


@dataclass
class MarketData:
    """A sealed, long-format daily frame plus the two index series. Never mutated."""

    df: pd.DataFrame
    index_close: pd.Series          # NIFTY 50 close by date (str)
    vix_close: pd.Series            # INDIA VIX close by date (str)
    as_of: str                      # last session on or before the requested date
    requested_as_of: str
    cfg: ResearchConfig
    _full: Optional[pd.DataFrame] = None     # unsealed history, kept for re-sealing only
    exclusions: Optional[DataExclusions] = None
    #: Last bar the unsealed history held when this frame was built — the data the engine
    #: COULD see at generation time (A1: an edition is backfilled when this lies past it).
    data_through: Optional[str] = None

    # ── construction ────────────────────────────────────────────────────────

    @classmethod
    def load(cls, cfg: ResearchConfig, *, as_of: Optional[str] = None) -> "MarketData":
        """Read the warehouse once, then seal at `as_of` (default: the last bar)."""
        with _connect(cfg.price_db) as con:
            raw = pd.read_sql_query(
                "SELECT o.symbol, substr(o.bar_time,1,10) AS d, o.open, o.close, o.volume, "
                "l.sector, l.in_nifty50 FROM ohlc_daily o JOIN instrument_labels l "
                "ON o.symbol = l.symbol WHERE l.in_nifty500 = 1 AND l.is_active = 1 "
                "AND o.bar_time >= ? ORDER BY o.symbol, o.bar_time", con, params=[cfg.history_start])
            idx = pd.read_sql_query(
                "SELECT substr(bar_time,1,10) AS d, close FROM ohlc_daily WHERE symbol = ? "
                "AND bar_time >= ? ORDER BY bar_time", con, params=[cfg.index_symbol, cfg.history_start])
            vix = pd.read_sql_query(
                "SELECT substr(bar_time,1,10) AS d, close FROM ohlc_daily WHERE symbol = ? "
                "AND bar_time >= ? ORDER BY bar_time", con, params=[cfg.vix_symbol, cfg.history_start])
            ca = load_corp_actions(con)
        if raw.empty:
            raise RuntimeError("no price data for the Nifty-500 universe")
        # The warehouse PK is (symbol, bar_time); a date can appear twice with different
        # time parts (2013-2018). Keep the LAST row by bar_time — the query is ORDERED so
        # "last" is deterministic — and never merge them.
        raw = raw.drop_duplicates(subset=["symbol", "d"], keep="last")
        idx = idx.drop_duplicates("d", keep="last").set_index("d")["close"].astype(float)
        vix = vix.drop_duplicates("d", keep="last").set_index("d")["close"].astype(float)
        return cls.from_frame(raw, idx, vix, cfg, as_of=as_of, corp_actions=ca)

    @classmethod
    def from_frame(cls, raw: pd.DataFrame, index_close: pd.Series, vix_close: pd.Series,
                   cfg: ResearchConfig, *, as_of: Optional[str] = None,
                   corp_actions: Optional[pd.DataFrame] = None) -> "MarketData":
        """
        Build from an in-memory long frame (tests). Columns: symbol,d,open,close,volume,sector,in_nifty50.
        `corp_actions` (optional): symbol, ex_date, action_type — as the warehouse table.
        """
        raw = raw.copy()
        raw["d"] = raw["d"].astype(str).str.slice(0, 10)
        for c in ("open", "close", "volume"):
            raw[c] = raw[c].astype(float)
        if "in_nifty50" not in raw:
            raw["in_nifty50"] = 0
        full, excl = _causal_columns(raw, cfg, corp_actions)
        last = str(full["d"].max())
        md = cls(df=full, index_close=index_close, vix_close=vix_close, as_of=last,
                 requested_as_of=last, cfg=cfg, _full=full, exclusions=excl, data_through=last)
        return md.sealed(as_of or last)

    def sealed(self, as_of: str) -> "MarketData":
        """A frame that cannot see a bar after `as_of`. Re-sealing can only narrow."""
        as_of = str(as_of)[:10]
        if self._full is None:
            if as_of > self.as_of:
                raise LookAheadError(f"cannot widen a sealed frame from {self.as_of} to {as_of}")
            raise LookAheadError("this frame has no unsealed history to re-seal from")
        base = self._full
        df = base[base["d"] <= as_of].copy()
        if df.empty:
            raise LookAheadError(f"no session on or before {as_of}")
        actual = str(df["d"].max())
        # Forward outcomes exist only when the resolving bar is inside the seal, the window
        # holds no glitch / corporate-action bar, and the entry open is a real price (A7).
        for h in HORIZONS:
            ok = df[f"_d{h}"].notna() & (df[f"_d{h}"] <= actual) & (df[f"_badfwd{h}"] == 0)
            df[f"f{h}"] = np.where(ok & (df["_next_synth"] == 0), df[f"_c{h}"] / df["next_open"] - 1.0, np.nan)
            # The close-to-close move over h sessions (the anomaly's statistic; A8: one per horizon
            # so the grader can read the rule's own horizon, never a fixed window).
            df[f"r{h}cc"] = np.where(ok, df[f"_c{h}"] / df["close"] - 1.0, np.nan)
        # THE SEAL, physically: no post-seal price survives on the frame handed out.
        df = df.drop(columns=list(POST_SEAL_COLUMNS))
        return MarketData(
            df=df, index_close=self.index_close[self.index_close.index <= actual],
            vix_close=self.vix_close[self.vix_close.index <= actual],
            as_of=actual, requested_as_of=as_of, cfg=self.cfg, _full=base,
            exclusions=self.exclusions, data_through=self.data_through)

    # ── calendar ────────────────────────────────────────────────────────────

    @property
    def sessions(self) -> list[str]:
        return sorted(self.df["d"].unique().tolist())

    @property
    def today(self) -> str:
        return self.as_of

    @property
    def prev_session(self) -> Optional[str]:
        s = self.sessions
        return s[-2] if len(s) >= 2 else None

    def session_index(self, d: str) -> int:
        s = self.sessions
        try:
            return s.index(d)
        except ValueError:
            raise KeyError(f"{d} is not a session in this frame") from None

    def session_after(self, d: str, n: int) -> Optional[str]:
        """The n-th session after `d`, or None if it lies past the seal."""
        s = self.sessions
        i = self.session_index(d) + n
        return s[i] if 0 <= i < len(s) else None

    @property
    def first_session(self) -> str:
        return self.sessions[0]

    # ── slices ──────────────────────────────────────────────────────────────

    def today_rows(self) -> pd.DataFrame:
        return self.df[self.df["d"] == self.as_of]

    def rows_on(self, d: str) -> pd.DataFrame:
        return self.df[self.df["d"] == d]

    def symbol(self, sym: str) -> pd.DataFrame:
        return self.df[self.df["symbol"] == sym]

    def sectors(self) -> pd.Series:
        """symbol -> sector (as labelled today)."""
        return self.df.drop_duplicates("symbol").set_index("symbol")["sector"]

    def close_wide(self, symbols: Optional[list[str]] = None) -> pd.DataFrame:
        sub = self.df if symbols is None else self.df[self.df["symbol"].isin(symbols)]
        return sub.pivot_table(index="d", columns="symbol", values="close")

    @property
    def n_symbols_today(self) -> int:
        return int(self.today_rows()["symbol"].nunique())
