from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
from typing import Iterable

import numpy as np
import pandas as pd

from .models import EvaluationPlan, Policy


HORIZONS = (1, 3, 5, 10)


@dataclass
class RealizedOutcome:
    available: bool
    due_date: str | None
    gross_return: float | None
    net_return: float | None
    benchmark_return: float | None
    scored_return: float | None
    grade: str


class MarketData:
    """Read-only market-data adapter and deterministic feature store."""

    def __init__(self, db_path: str | Path, policy: Policy, through: str | None = None):
        self.db_path = Path(db_path).resolve()
        if not self.db_path.is_file():
            raise FileNotFoundError(f"Market database not found: {self.db_path}")
        self.policy = policy
        self.bars = self._load(through)
        if self.bars.empty:
            raise ValueError("No active Nifty 500 daily bars were available")
        self._prepare()

    def _load(self, through: str | None) -> pd.DataFrame:
        upper = through or "9999-12-31"
        query = """
            SELECT o.symbol,
                   substr(o.bar_time, 1, 10) AS session,
                   o.open, o.high, o.low, o.close, o.volume,
                   COALESCE(l.sector, 'Unclassified') AS sector,
                   COALESCE(l.company_name, l.company, o.symbol) AS company,
                   l.in_nifty50
              FROM instrument_labels l
              JOIN ohlc_daily o ON o.symbol = l.symbol
             WHERE l.in_nifty500 = 1
               AND l.is_active = 1
               AND o.bar_time >= ?
               AND o.bar_time < ?
               AND o.open > 0 AND o.close > 0 AND o.volume >= 0
             ORDER BY o.symbol, o.bar_time
        """
        with sqlite3.connect(f"file:{self.db_path.as_posix()}?mode=ro", uri=True) as connection:
            frame = pd.read_sql_query(query, connection, params=(self.policy.history_start, upper + "T23:59:59"))
        frame["date"] = pd.to_datetime(frame.pop("session"), errors="raise").dt.normalize()
        numeric = ["open", "high", "low", "close", "volume", "in_nifty50"]
        frame[numeric] = frame[numeric].apply(pd.to_numeric, errors="coerce")
        frame = frame.dropna(subset=["symbol", "date", "open", "close"]).sort_values(["symbol", "date"])
        return frame.reset_index(drop=True)

    def _prepare(self) -> None:
        frame = self.bars
        grouped = frame.groupby("symbol", sort=False, observed=True)
        frame["ret_1"] = grouped["close"].pct_change(fill_method=None)
        frame["past_5"] = frame["close"] / grouped["close"].shift(5) - 1.0
        frame["past_20"] = frame["close"] / grouped["close"].shift(20) - 1.0
        frame["volume_median_20"] = grouped["volume"].transform(
            lambda values: values.shift(1).rolling(20, min_periods=15).median()
        )
        frame["volume_ratio"] = frame["volume"] / frame["volume_median_20"].replace(0, np.nan)
        mean_60 = grouped["ret_1"].transform(lambda values: values.shift(1).rolling(60, min_periods=40).mean())
        std_60 = grouped["ret_1"].transform(lambda values: values.shift(1).rolling(60, min_periods=40).std(ddof=1))
        frame["return_z_60"] = (frame["ret_1"] - mean_60) / std_60.replace(0, np.nan)
        frame["liquidity_20"] = grouped.apply(
            lambda group: (group["close"] * group["volume"]).shift(1).rolling(20, min_periods=15).median(),
            include_groups=False,
        ).reset_index(level=0, drop=True)

        self.sessions = pd.Index(sorted(frame["date"].unique()))
        self._session_position = {pd.Timestamp(day): index for index, day in enumerate(self.sessions)}
        for horizon in HORIZONS:
            next_open = grouped["open"].shift(-1)
            exit_close = grouped["close"].shift(-horizon)
            frame[f"fwd_{horizon}"] = exit_close / next_open - 1.0
            date_map = {
                day: (self.sessions[index + horizon] if index + horizon < len(self.sessions) else pd.NaT)
                for index, day in enumerate(self.sessions)
            }
            frame[f"outcome_date_{horizon}"] = frame["date"].map(date_map)

        market = frame.groupby("date", observed=True).agg(
            ew_return=("ret_1", "mean"),
            breadth=("ret_1", lambda values: float((values > 0).mean())),
            dispersion=("ret_1", "std"),
            coverage=("symbol", "nunique"),
        )
        nifty50 = frame[frame["in_nifty50"] == 1].groupby("date", observed=True)["ret_1"].mean()
        market["nifty50_return"] = nifty50
        for horizon in HORIZONS:
            market[f"fwd_{horizon}"] = frame.groupby("date", observed=True)[f"fwd_{horizon}"].mean()
            market[f"outcome_date_{horizon}"] = [
                self.sessions[self._session_position[day] + horizon]
                if self._session_position[day] + horizon < len(self.sessions) else pd.NaT
                for day in market.index
            ]
        market["trend_20"] = (1.0 + market["ew_return"].fillna(0)).rolling(20, min_periods=15).apply(np.prod, raw=True) - 1.0
        market["volatility_20"] = market["ew_return"].rolling(20, min_periods=15).std(ddof=1) * np.sqrt(252)
        market["breadth_percentile"] = market["breadth"].expanding(60).rank(pct=True)
        market["dispersion_percentile"] = market["dispersion"].expanding(60).rank(pct=True)
        market["regime"] = [self._regime(trend, vol) for trend, vol in zip(market["trend_20"], market["volatility_20"])]
        self.market = market

        sector = frame.groupby(["date", "sector"], observed=True).agg(
            sector_return=("ret_1", "mean"),
            member_count=("symbol", "nunique"),
            liquidity=("liquidity_20", "sum"),
        )
        for horizon in HORIZONS:
            sector[f"fwd_{horizon}"] = frame.groupby(["date", "sector"], observed=True)[f"fwd_{horizon}"].mean()
        sector = sector.reset_index().sort_values(["sector", "date"])
        by_sector = sector.groupby("sector", sort=False, observed=True)
        sector["past_5"] = by_sector["sector_return"].transform(lambda values: values.shift(1).rolling(5, min_periods=5).sum())
        sector["past_20"] = by_sector["sector_return"].transform(lambda values: values.shift(1).rolling(20, min_periods=15).sum())
        sector["daily_rank"] = sector.groupby("date", observed=True)["sector_return"].rank(method="min", ascending=False)
        sector["past_20_rank"] = sector.groupby("date", observed=True)["past_20"].rank(method="average", ascending=False, pct=True)
        for horizon in HORIZONS:
            market_forward = market[f"fwd_{horizon}"]
            sector[f"relative_fwd_{horizon}"] = sector[f"fwd_{horizon}"] - sector["date"].map(market_forward)
            sector[f"outcome_date_{horizon}"] = sector["date"].map(market[f"outcome_date_{horizon}"])
        sector["regime"] = sector["date"].map(market["regime"])
        self.sectors = sector

    @staticmethod
    def _regime(trend: float, volatility: float) -> str:
        if pd.isna(trend) or pd.isna(volatility):
            return "forming"
        trend_label = "uptrend" if trend > 0.02 else "downtrend" if trend < -0.02 else "range"
        vol_label = "high_vol" if volatility > 0.20 else "normal_vol"
        return f"{trend_label}/{vol_label}"

    @property
    def latest_date(self) -> str:
        return self.sessions[-1].strftime("%Y-%m-%d")

    def resolve_date(self, value: str | None) -> pd.Timestamp:
        if value is None or value == "latest":
            return pd.Timestamp(self.sessions[-1])
        requested = pd.Timestamp(value).normalize()
        eligible = self.sessions[self.sessions <= requested]
        if eligible.empty:
            raise ValueError(f"No session on or before {value}")
        return pd.Timestamp(eligible[-1])

    def session_offset(self, day: str | pd.Timestamp, offset: int) -> pd.Timestamp | None:
        resolved = self.resolve_date(str(day)[:10])
        target = self._session_position[resolved] + offset
        if target < 0 or target >= len(self.sessions):
            return None
        return pd.Timestamp(self.sessions[target])

    def sessions_between(self, start: str, end: str) -> list[pd.Timestamp]:
        start_day, end_day = pd.Timestamp(start), self.resolve_date(end)
        return [pd.Timestamp(day) for day in self.sessions if start_day <= day <= end_day]

    def regime(self, as_of: str | pd.Timestamp) -> str:
        return str(self.market.loc[self.resolve_date(str(as_of)[:10]), "regime"])

    def current_bars(self, as_of: str | pd.Timestamp) -> pd.DataFrame:
        day = self.resolve_date(str(as_of)[:10])
        return self.bars[self.bars["date"] == day].copy()

    def public_universe(self, as_of: str | pd.Timestamp) -> dict[str, object]:
        day = self.resolve_date(str(as_of)[:10])
        current = self.current_bars(day)
        expected = int(self.bars.groupby("date")["symbol"].nunique().rolling(20, min_periods=1).median().loc[day])
        actual = int(current["symbol"].nunique())
        return {
            "source": str(self.db_path),
            "latest_session": day.strftime("%Y-%m-%d"),
            "rows_loaded": int(len(self.bars)),
            "symbols_scanned": actual,
            "expected_recent_coverage": expected,
            "coverage_ratio": round(actual / expected, 4) if expected else 0.0,
            "stale_or_partial": actual < expected * 0.9,
        }

    def liquid_symbols(self, as_of: str | pd.Timestamp, sector: str | None = None, limit: int = 10) -> list[str]:
        current = self.current_bars(as_of)
        if sector is not None:
            current = current[current["sector"] == sector]
        return current.sort_values("liquidity_20", ascending=False)["symbol"].dropna().head(limit).tolist()

    def nifty50_symbols(self, as_of: str | pd.Timestamp) -> list[str]:
        current = self.current_bars(as_of)
        return current[current["in_nifty50"] == 1]["symbol"].tolist()

    def _basket_return(self, symbols: Iterable[str], signal_day: pd.Timestamp, horizon: int) -> float | None:
        subset = self.bars[(self.bars["date"] == signal_day) & self.bars["symbol"].isin(list(symbols))]
        values = subset[f"fwd_{horizon}"].dropna()
        return float(values.mean()) if len(values) else None

    def realize(self, plan: EvaluationPlan, signal_date: str) -> RealizedOutcome:
        signal_day = self.resolve_date(signal_date)
        due = self.session_offset(signal_day, plan.horizon_sessions)
        if due is None:
            return RealizedOutcome(False, None, None, None, None, None, "PENDING")
        gross = self._basket_return(plan.symbols, signal_day, plan.horizon_sessions)
        if gross is None:
            return RealizedOutcome(False, due.strftime("%Y-%m-%d"), None, None, None, None, "PENDING")
        benchmark = self._basket_return(plan.benchmark_symbols, signal_day, plan.horizon_sessions) if plan.benchmark_symbols else None
        net = gross - self.policy.cost_rate
        direction_multiplier = -1.0 if plan.direction == "SHORT" else 1.0
        if plan.kind == "relative":
            scored = direction_multiplier * (gross - (benchmark or 0.0)) - self.policy.cost_rate
        elif plan.kind == "absolute_move":
            scored = abs(gross)
        else:
            scored = direction_multiplier * gross - self.policy.cost_rate
        hurdle = plan.hurdle
        if plan.kind == "absolute_move":
            grade = "RIGHT" if scored > hurdle else "INCONCLUSIVE" if scored > hurdle * 0.75 else "WRONG"
        elif plan.kind == "no_trade":
            grade = "RIGHT" if scored < -hurdle else "WRONG" if scored > hurdle else "INCONCLUSIVE"
        else:
            grade = "RIGHT" if scored > hurdle else "WRONG" if scored < -hurdle else "INCONCLUSIVE"
        return RealizedOutcome(True, due.strftime("%Y-%m-%d"), gross, net, benchmark, scored, grade)
