from __future__ import annotations

from collections import deque
from statistics import median
from typing import Any


def pct(new: float | None, old: float | None) -> float | None:
    if new is None or old in (None, 0):
        return None
    return (new - old) / old


def build_behavior_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Derive pre-move behavior features from OHLCV only."""
    rows = sorted(rows, key=lambda r: r["trade_date"])
    out: list[dict[str, Any]] = []
    closes: list[float] = []
    highs: list[float] = []
    lows: list[float] = []
    opens: list[float] = []
    vols: list[float] = []

    for i, row in enumerate(rows):
        open_ = float(row["open"] or 0)
        high = float(row["high"] or 0)
        low = float(row["low"] or 0)
        close = float(row["close"] or 0)
        vol = float(row["volume"] or 0)
        opens.append(open_)
        highs.append(high)
        lows.append(low)
        closes.append(close)
        vols.append(vol)

        if i < 60 or close <= 0:
            continue

        ret1 = pct(close, closes[i - 1])
        ret3 = pct(close, closes[i - 3]) if i >= 3 else None
        ret5 = pct(close, closes[i - 5]) if i >= 5 else None
        ret10 = pct(close, closes[i - 10]) if i >= 10 else None
        ret20 = pct(close, closes[i - 20]) if i >= 20 else None
        ret60 = pct(close, closes[i - 60]) if i >= 60 else None

        range_ = max(high - low, 0.0)
        body = abs(close - open_)
        upper_wick = high - max(open_, close)
        lower_wick = min(open_, close) - low
        close_location = ((close - low) / range_) if range_ > 0 else 0.5
        body_ratio = body / range_ if range_ > 0 else 0.0

        sma20 = avg(closes[i - 19 : i + 1])
        sma50 = avg(closes[i - 49 : i + 1])
        sma200 = avg(closes[i - 199 : i + 1]) if i >= 199 else None
        sma20_prev = avg(closes[i - 24 : i - 4]) if i >= 24 else None
        sma50_prev = avg(closes[i - 59 : i - 9]) if i >= 59 else None
        vol20 = avg(vols[i - 19 : i + 1])
        vol50 = avg(vols[i - 49 : i + 1])
        atr14 = avg(true_ranges(opens, highs, lows, closes, i, 14))
        atr50 = avg(true_ranges(opens, highs, lows, closes, i, 50))
        hi20 = max(highs[i - 19 : i + 1])
        lo20 = min(lows[i - 19 : i + 1])
        hi60 = max(highs[i - 59 : i + 1])
        lo60 = min(lows[i - 59 : i + 1])
        range20 = (hi20 - lo20) / close if close else None
        range60 = (hi60 - lo60) / close if close else None
        prev_high20 = max(highs[i - 20 : i]) if i >= 20 else None
        prev_low20 = min(lows[i - 20 : i]) if i >= 20 else None
        gap = pct(open_, closes[i - 1]) if i >= 1 else None

        feat = {
            "market": row["market"],
            "ticker": row["ticker"],
            "trade_date": row["trade_date"],
            "close": close,
            "ret1": ret1,
            "ret3": ret3,
            "ret5": ret5,
            "ret10": ret10,
            "ret20": ret20,
            "ret60": ret60,
            "trend_20": bucket_return(ret20),
            "trend_60": bucket_return(ret60),
            "current_move": bucket_day(ret1),
            "flow": flow_state(ret20, ret1),
            "candle": candle_state(close, open_, close_location, body_ratio, upper_wick, lower_wick, range_),
            "volume": volume_state(vol20, vol50),
            "volatility": volatility_state(atr14, atr50, range20),
            "range_state": range_state(range20, range60),
            "ma_position": ma_position(close, sma20, sma50, sma200),
            "ma_slope": ma_slope(sma20, sma20_prev, sma50, sma50_prev),
            "sr_state": support_resistance_state(close, prev_high20, prev_low20),
            "gap_state": gap_state(gap, close, open_),
            "breakout_state": breakout_state(close, prev_high20, prev_low20, ret1),
        }
        feat["behavior_atoms"] = behavior_atoms(feat)
        feat["coarse_behavior"] = "|".join(
            [feat["trend_20"], feat["flow"], feat["volatility"], feat["ma_position"], feat["breakout_state"]]
        )
        out.append(feat)
    return out


def behavior_atoms(feat: dict[str, Any]) -> list[str]:
    keys = [
        "trend_20",
        "trend_60",
        "current_move",
        "flow",
        "candle",
        "volume",
        "volatility",
        "range_state",
        "ma_position",
        "ma_slope",
        "sr_state",
        "gap_state",
        "breakout_state",
    ]
    return [f"{k}:{feat[k]}" for k in keys if feat.get(k)]


def forward_outcomes(rows: list[dict[str, Any]], windows: list[int]) -> dict[tuple[str, int], dict[str, float]]:
    rows = sorted(rows, key=lambda r: r["trade_date"])
    out: dict[tuple[str, int], dict[str, float]] = {}
    closes = [float(r["close"] or 0) for r in rows]
    highs = [float(r["high"] or 0) for r in rows]
    lows = [float(r["low"] or 0) for r in rows]
    for i, row in enumerate(rows):
        close = closes[i]
        if close <= 0:
            continue
        for window in windows:
            end = min(len(rows), i + window + 1)
            if end <= i + 1:
                continue
            f_closes = closes[i + 1 : end]
            f_highs = highs[i + 1 : end]
            f_lows = lows[i + 1 : end]
            if not f_closes:
                continue
            out[(row["trade_date"], window)] = {
                "forward_return": (f_closes[-1] - close) / close,
                "max_forward_return": (max(f_highs) - close) / close,
                "min_forward_return": (min(f_lows) - close) / close,
            }
    return out


def avg(values: list[float | None]) -> float | None:
    nums = [v for v in values if v is not None]
    if not nums:
        return None
    return sum(nums) / len(nums)


def true_ranges(
    opens: list[float], highs: list[float], lows: list[float], closes: list[float], idx: int, lookback: int
) -> list[float]:
    start = max(1, idx - lookback + 1)
    vals = []
    for j in range(start, idx + 1):
        vals.append(max(highs[j] - lows[j], abs(highs[j] - closes[j - 1]), abs(lows[j] - closes[j - 1])))
    return vals


def bucket_return(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value >= 0.10:
        return "strong_up"
    if value >= 0.03:
        return "up"
    if value <= -0.10:
        return "strong_down"
    if value <= -0.03:
        return "down"
    return "sideways"


def bucket_day(value: float | None) -> str:
    if value is None:
        return "day_unknown"
    if value >= 0.025:
        return "expansion_up"
    if value >= 0.005:
        return "up"
    if value <= -0.025:
        return "expansion_down"
    if value <= -0.005:
        return "down"
    return "flat"


def flow_state(ret20: float | None, ret1: float | None) -> str:
    if ret20 is None or ret1 is None:
        return "unknown"
    if ret20 >= 0.03 and ret1 >= 0.005:
        return "up_continuation"
    if ret20 >= 0.03 and ret1 <= -0.005:
        return "up_pullback"
    if ret20 <= -0.03 and ret1 <= -0.005:
        return "down_continuation"
    if ret20 <= -0.03 and ret1 >= 0.005:
        return "down_reversal_attempt"
    return "range_move"


def candle_state(
    close: float, open_: float, close_location: float, body_ratio: float, upper_wick: float, lower_wick: float, range_: float
) -> str:
    direction = "green" if close >= open_ else "red"
    if close_location >= 0.75 and body_ratio >= 0.55:
        return f"{direction}_close_near_high"
    if close_location <= 0.25 and body_ratio >= 0.55:
        return f"{direction}_close_near_low"
    if range_ > 0 and lower_wick / range_ >= 0.45:
        return "lower_wick_rejection"
    if range_ > 0 and upper_wick / range_ >= 0.45:
        return "upper_wick_rejection"
    if body_ratio <= 0.25:
        return "small_body"
    return f"{direction}_balanced"


def volume_state(vol20: float | None, vol50: float | None) -> str:
    if not vol20 or not vol50:
        return "volume_unknown"
    ratio = vol20 / vol50
    if ratio >= 1.4:
        return "volume_expanding"
    if ratio >= 1.1:
        return "volume_rising"
    if ratio <= 0.75:
        return "volume_dryup"
    return "volume_normal"


def volatility_state(atr14: float | None, atr50: float | None, range20: float | None) -> str:
    if not atr14 or not atr50 or range20 is None:
        return "vol_unknown"
    ratio = atr14 / atr50
    if range20 <= 0.08 or ratio <= 0.75:
        return "compressed"
    if range20 >= 0.22 or ratio >= 1.25:
        return "expanded"
    return "normal_vol"


def range_state(range20: float | None, range60: float | None) -> str:
    if range20 is None or range60 in (None, 0):
        return "range_unknown"
    ratio = range20 / range60
    if ratio <= 0.55:
        return "range_contracting"
    if ratio >= 1.15:
        return "range_expanding"
    return "range_normal"


def ma_position(close: float, sma20: float | None, sma50: float | None, sma200: float | None) -> str:
    if not sma20 or not sma50:
        return "ma_unknown"
    if close > sma20 > sma50 and (not sma200 or sma50 > sma200):
        return "above_stacked_ma"
    if close > sma20 and close > sma50:
        return "above_key_ma"
    if close < sma20 < sma50:
        return "below_stacked_ma"
    if close < sma20 and close < sma50:
        return "below_key_ma"
    return "ma_mixed"


def ma_slope(sma20: float | None, sma20_prev: float | None, sma50: float | None, sma50_prev: float | None) -> str:
    s20 = pct(sma20, sma20_prev)
    s50 = pct(sma50, sma50_prev)
    if s20 is None or s50 is None:
        return "slope_unknown"
    if s20 > 0.01 and s50 > 0:
        return "ma_slope_up"
    if s20 < -0.01 and s50 < 0:
        return "ma_slope_down"
    if s20 > 0.01 and s50 <= 0:
        return "short_slope_turning_up"
    if s20 < -0.01 and s50 >= 0:
        return "short_slope_turning_down"
    return "ma_slope_flat"


def support_resistance_state(close: float, prev_high20: float | None, prev_low20: float | None) -> str:
    if not prev_high20 or not prev_low20 or close <= 0:
        return "sr_unknown"
    dist_high = (prev_high20 - close) / close
    dist_low = (close - prev_low20) / close
    if dist_high <= 0.015:
        return "near_resistance"
    if dist_low <= 0.015:
        return "near_support"
    if dist_high < dist_low:
        return "upper_range"
    return "lower_range"


def gap_state(gap: float | None, close: float, open_: float) -> str:
    if gap is None:
        return "gap_unknown"
    if gap >= 0.02:
        return "gap_up_hold" if close >= open_ else "gap_up_fade"
    if gap <= -0.02:
        return "gap_down_recover" if close >= open_ else "gap_down_continue"
    return "no_gap"


def breakout_state(close: float, prev_high20: float | None, prev_low20: float | None, ret1: float | None) -> str:
    if not prev_high20 or not prev_low20:
        return "breakout_unknown"
    if close > prev_high20:
        return "breakout"
    if close < prev_low20:
        return "breakdown"
    if ret1 is not None and ret1 > 0.02 and close > (prev_high20 + prev_low20) / 2:
        return "range_expansion_up"
    if ret1 is not None and ret1 < -0.02 and close < (prev_high20 + prev_low20) / 2:
        return "range_expansion_down"
    return "inside_range"
