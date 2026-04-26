from __future__ import annotations

from collections import defaultdict
from bisect import bisect_right
from statistics import median
from typing import Any

from engine.readers.sqlite_reader import ReadOnlySQLite


def pct(new: float | None, old: float | None) -> float | None:
    if new is None or old in (None, 0):
        return None
    return (new - old) / old


def bucket_return(value: float | None, prefix: str) -> str:
    if value is None:
        return f"{prefix}_unknown"
    if value >= 0.10:
        return f"{prefix}_strong_up"
    if value >= 0.03:
        return f"{prefix}_up"
    if value <= -0.10:
        return f"{prefix}_strong_down"
    if value <= -0.03:
        return f"{prefix}_down"
    return f"{prefix}_sideways"


def bucket_day(value: float | None) -> str:
    if value is None:
        return "day_unknown"
    if value >= 0.025:
        return "day_expansion_up"
    if value >= 0.005:
        return "day_up"
    if value <= -0.025:
        return "day_expansion_down"
    if value <= -0.005:
        return "day_down"
    return "day_flat"


def continuation_state(ret20: float | None, ret1: float | None) -> str:
    if ret20 is None or ret1 is None:
        return "flow_unknown"
    if ret20 > 0.03 and ret1 > 0.005:
        return "uptrend_continuation"
    if ret20 > 0.03 and ret1 < -0.005:
        return "uptrend_pullback"
    if ret20 < -0.03 and ret1 < -0.005:
        return "downtrend_continuation"
    if ret20 < -0.03 and ret1 > 0.005:
        return "downtrend_reversal_attempt"
    return "range_move"


def compression_state(range20: float | None, ret1_abs: float | None) -> str:
    if range20 is None:
        return "vol_unknown"
    if range20 <= 0.08:
        base = "compressed"
    elif range20 >= 0.22:
        base = "expanded"
    else:
        base = "normal_vol"
    if ret1_abs is not None and ret1_abs >= 0.025:
        return f"{base}_break"
    return base


class TrendFeatureEngine:
    """Builds stock trend/context states from OHLC, without writing them anywhere."""

    def __init__(self, db: ReadOnlySQLite, markets: list[str]):
        self.db = db
        self.markets = markets
        self.features: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.market_context: dict[tuple[str, str], dict[str, Any]] = {}
        self.sector_context: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.sector_by_stock: dict[tuple[str, str], str] = {}
        self.existing_trend_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.dates_by_stock: dict[tuple[str, str], list[str]] = {}

    def build(self) -> "TrendFeatureEngine":
        self._load_sector_mapping()
        self._load_existing_trend_states()
        rows = self._load_ohlc()
        by_stock: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_stock[(row["market"], row["ticker"])].append(row)

        all_by_date: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        sector_by_date: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)

        for key, stock_rows in by_stock.items():
            stock_rows.sort(key=lambda r: r["trade_date"])
            market, ticker = key
            self.dates_by_stock[(market, ticker)] = [r["trade_date"] for r in stock_rows]
            closes = [r["close"] for r in stock_rows]
            highs = [r["high"] for r in stock_rows]
            lows = [r["low"] for r in stock_rows]
            volumes = [r["volume"] for r in stock_rows]
            for i, row in enumerate(stock_rows):
                close = row["close"]
                ret1 = pct(close, closes[i - 1]) if i >= 1 else None
                ret5 = pct(close, closes[i - 5]) if i >= 5 else None
                ret20 = pct(close, closes[i - 20]) if i >= 20 else None
                ret60 = pct(close, closes[i - 60]) if i >= 60 else None
                hi20 = max(highs[max(0, i - 19) : i + 1]) if i >= 5 else None
                lo20 = min(lows[max(0, i - 19) : i + 1]) if i >= 5 else None
                range20 = ((hi20 - lo20) / close) if hi20 and lo20 and close else None
                avg_vol20 = _avg(volumes[max(0, i - 19) : i + 1])
                avg_vol60 = _avg(volumes[max(0, i - 59) : i + 1])
                vol_ratio = (avg_vol20 / avg_vol60) if avg_vol20 and avg_vol60 else None
                existing = self._existing_trend_state(market, ticker, row["trade_date"])
                trend20 = bucket_return(ret20, "ret20")
                trend60 = bucket_return(ret60, "ret60")
                day = bucket_day(ret1)
                flow = continuation_state(ret20, ret1)
                vol_state = compression_state(range20, abs(ret1) if ret1 is not None else None)
                weekly_proxy = trend60.replace("ret60", "weekly_proxy")
                composite = "|".join([trend20, flow, vol_state])
                feat = {
                    "market": market,
                    "ticker": ticker,
                    "date": row["trade_date"],
                    "close": close,
                    "ret1": ret1,
                    "ret5": ret5,
                    "ret20": ret20,
                    "ret60": ret60,
                    "range20": range20,
                    "volume_ratio20_60": vol_ratio,
                    "trend20_state": trend20,
                    "trend60_state": trend60,
                    "day_state": day,
                    "flow_state": flow,
                    "compression_state": vol_state,
                    "weekly_proxy_state": weekly_proxy,
                    "existing_trend_state": existing.get("trend_state", "trend_unknown"),
                    "existing_trend_strength": existing.get("trend_strength"),
                    "composite_trend_state": composite,
                    "sector": self.sector_by_stock.get((market, ticker), "UNKNOWN"),
                }
                self.features[(market, ticker, row["trade_date"])] = feat
                all_by_date[(market, row["trade_date"])].append(feat)
                sector_by_date[(market, feat["sector"], row["trade_date"])].append(feat)

        self._build_market_context(all_by_date)
        self._build_sector_context(sector_by_date)
        return self

    def get(self, market: str, ticker: str, date: str) -> dict[str, Any]:
        feat = self.features.get((market, ticker, date))
        if feat is None:
            fallback_date = self._latest_date_on_or_before(market, ticker, date)
            if fallback_date:
                feat = self.features.get((market, ticker, fallback_date))
        if feat is None:
            return {
                "composite_trend_state": "trend_missing",
                "weekly_proxy_state": "weekly_proxy_unknown",
                "sector": self.sector_by_stock.get((market, ticker), "UNKNOWN"),
                "market_context_state": "market_unknown",
                "sector_context_state": "sector_unknown",
                "volatility_regime": "vol_unknown",
            }
        market_ctx = self.market_context.get((market, date), {})
        sector_ctx = self.sector_context.get((market, feat["sector"], date), {})
        return {
            **feat,
            "market_context_state": market_ctx.get("market_context_state", "market_unknown"),
            "volatility_regime": market_ctx.get("volatility_regime", "vol_unknown"),
            "sector_context_state": sector_ctx.get("sector_context_state", "sector_unknown"),
        }

    def _latest_date_on_or_before(self, market: str, ticker: str, date: str) -> str | None:
        dates = self.dates_by_stock.get((market, ticker), [])
        if not dates:
            return None
        idx = bisect_right(dates, date) - 1
        if idx < 0:
            return None
        return dates[idx]

    def _load_ohlc(self) -> list[dict[str, Any]]:
        placeholders = ",".join("?" for _ in self.markets)
        return self.db.query(
            f"""
            select ticker, market, trade_date, open, high, low, close, volume
            from ohlc_daily
            where market in ({placeholders})
            order by market, ticker, trade_date
            """,
            self.markets,
        )

    def _load_sector_mapping(self) -> None:
        if not self.db.has_table("sector_mapping"):
            return
        placeholders = ",".join("?" for _ in self.markets)
        rows = self.db.query(
            f"select ticker, market, coalesce(sector, 'UNKNOWN') as sector from sector_mapping where market in ({placeholders})",
            self.markets,
        )
        self.sector_by_stock = {
            (r["market"], r["ticker"]): r["sector"] or "UNKNOWN" for r in rows
        }

    def _load_existing_trend_states(self) -> None:
        if not self.db.has_table("stock_trend_state"):
            return
        placeholders = ",".join("?" for _ in self.markets)
        rows = self.db.query(
            f"""
            select ticker, market, trade_date, trend_state, trend_strength
            from stock_trend_state
            where market in ({placeholders})
            """,
            self.markets,
        )
        self.existing_trend_by_key = {
            (r["market"], r["ticker"], r["trade_date"]): {
                "trend_state": r["trend_state"],
                "trend_strength": r["trend_strength"],
            }
            for r in rows
        }

    def _existing_trend_state(self, market: str, ticker: str, date: str) -> dict[str, Any]:
        return self.existing_trend_by_key.get((market, ticker, date), {})

    def _build_market_context(self, grouped: dict[tuple[str, str], list[dict[str, Any]]]) -> None:
        for (market, date), rows in grouped.items():
            ret20s = [r["ret20"] for r in rows if r.get("ret20") is not None]
            ranges = [r["range20"] for r in rows if r.get("range20") is not None]
            if not ret20s:
                state = "market_unknown"
            else:
                breadth = sum(1 for x in ret20s if x > 0.03) / len(ret20s)
                if breadth >= 0.62:
                    state = "market_broad_up"
                elif breadth <= 0.38:
                    state = "market_broad_down"
                else:
                    state = "market_mixed"
            med_range = median(ranges) if ranges else None
            if med_range is None:
                vol = "vol_unknown"
            elif med_range >= 0.22:
                vol = "high_vol_regime"
            elif med_range <= 0.10:
                vol = "low_vol_regime"
            else:
                vol = "normal_vol_regime"
            self.market_context[(market, date)] = {
                "market_context_state": state,
                "volatility_regime": vol,
            }

    def _build_sector_context(
        self, grouped: dict[tuple[str, str, str], list[dict[str, Any]]]
    ) -> None:
        for (market, sector, date), rows in grouped.items():
            ret20s = [r["ret20"] for r in rows if r.get("ret20") is not None]
            if len(ret20s) < 3:
                state = "sector_unknown"
            else:
                med = median(ret20s)
                if med >= 0.04:
                    state = "sector_strong"
                elif med <= -0.04:
                    state = "sector_weak"
                else:
                    state = "sector_neutral"
            self.sector_context[(market, sector, date)] = {
                "sector_context_state": state
            }


def _avg(values: list[float | None]) -> float | None:
    nums = [v for v in values if v is not None]
    if not nums:
        return None
    return sum(nums) / len(nums)
