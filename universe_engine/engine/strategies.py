"""
Universe-engine strategies — 107 long-only patterns.
Ported from custom_agent.py (bullish set + helpers). No bearish, no neutral.
Each strategy: (df, i) -> bool. df has columns Open, High, Low, Close, Volume.
"""
from __future__ import annotations
import math


def _body(df, i):
    return abs(df["Close"].iloc[i] - df["Open"].iloc[i])

def _range_(df, i):
    return df["High"].iloc[i] - df["Low"].iloc[i]

def _is_bull_candle(df, i):
    return df["Close"].iloc[i] > df["Open"].iloc[i]

def _is_bear_candle(df, i):
    return df["Close"].iloc[i] < df["Open"].iloc[i]

def _vol_avg(df, i, n=20):
    start = max(0, i - n)
    return df["Volume"].iloc[start:i].mean() if i > 0 else df["Volume"].iloc[i]

def _sma(df, col, i, n):
    start = max(0, i - n + 1)
    return df[col].iloc[start:i+1].mean()

def _ema(series, span):
    return series.ewm(span=span, adjust=False).mean()

def _atr(df, i, n=14):
    start = max(1, i - n + 1)
    trs = []
    for j in range(start, i + 1):
        tr = max(
            df["High"].iloc[j] - df["Low"].iloc[j],
            abs(df["High"].iloc[j] - df["Close"].iloc[j-1]),
            abs(df["Low"].iloc[j] - df["Close"].iloc[j-1]),
        )
        trs.append(tr)
    return sum(trs) / len(trs) if trs else 0


# ── 107 long-only strategies ──────────────────────────────────────

def _bull_engulfing(df, i):
    if i < 1: return False
    return (_is_bear_candle(df, i-1) and _is_bull_candle(df, i)
            and df["Open"].iloc[i] < df["Close"].iloc[i-1]
            and df["Close"].iloc[i] > df["Open"].iloc[i-1]
            and _body(df, i) > _body(df, i-1))

def _morning_star(df, i):
    if i < 2: return False
    return (_is_bear_candle(df, i-2)
            and _body(df, i-1) < _body(df, i-2) * 0.5
            and _is_bull_candle(df, i)
            and df["Close"].iloc[i] > (df["Open"].iloc[i-2] + df["Close"].iloc[i-2]) / 2)

def _hammer(df, i):
    if i < 1: return False
    body = _body(df, i)
    rng = _range_(df, i)
    if rng == 0: return False
    lower_wick = df["Open"].iloc[i] - df["Low"].iloc[i] if _is_bull_candle(df, i) else df["Close"].iloc[i] - df["Low"].iloc[i]
    return lower_wick >= 2 * body and body / rng < 0.35 and df["Close"].iloc[i] > df["Close"].iloc[i-1]

def _flag_and_pole(df, i):
    if i < 10: return False
    pole_gain = (df["Close"].iloc[i-5] - df["Close"].iloc[i-10]) / df["Close"].iloc[i-10]
    if pole_gain < 0.05: return False
    consol_hi = df["High"].iloc[i-5:i].max()
    consol_lo = df["Low"].iloc[i-5:i].min()
    consol_range = (consol_hi - consol_lo) / consol_hi
    return consol_range < 0.04 and df["Close"].iloc[i] > consol_hi

def _range_breakout_bull(df, i):
    if i < 20: return False
    rng_hi = df["High"].iloc[i-20:i].max()
    rng_lo = df["Low"].iloc[i-20:i].min()
    rng_size = (rng_hi - rng_lo) / rng_lo
    if rng_size > 0.25: return False
    return df["Close"].iloc[i] > rng_hi * 1.005 and df["Volume"].iloc[i] > _vol_avg(df, i, 10) * 1.3

def _volume_surge_bull(df, i):
    if i < 20: return False
    avg_vol = _vol_avg(df, i, 20)
    return (df["Volume"].iloc[i] > avg_vol * 2.0
            and _is_bull_candle(df, i)
            and df["Close"].iloc[i] > df["Close"].iloc[i-1] * 1.01)

def _cup_and_handle(df, i):
    if i < 30: return False
    left_peak = df["High"].iloc[i-30:i-15].max()
    cup_bot = df["Low"].iloc[i-15:i-5].min()
    right_peak = df["High"].iloc[i-5:i].max()
    depth = (left_peak - cup_bot) / left_peak
    return (0.10 < depth < 0.35
            and right_peak >= left_peak * 0.97
            and df["Close"].iloc[i] > right_peak * 0.99)

def _golden_cross(df, i):
    if i < 50: return False
    sma20 = _sma(df, "Close", i, 20)
    sma50 = _sma(df, "Close", i, 50)
    sma20_prev = _sma(df, "Close", i-1, 20)
    sma50_prev = _sma(df, "Close", i-1, 50)
    return sma20_prev <= sma50_prev and sma20 > sma50

def _ema_breakout(df, i):
    if i < 20: return False
    ema20 = _ema(df["Close"].iloc[:i+1], 20).iloc[-1]
    ema_prev = _ema(df["Close"].iloc[:i], 20).iloc[-1] if i > 0 else ema20
    return (df["Close"].iloc[i] > ema20
            and df["Close"].iloc[i-1] < ema_prev
            and _is_bull_candle(df, i))

def _higher_highs_higher_lows(df, i):
    if i < 4: return False
    hh = all(df["High"].iloc[j] > df["High"].iloc[j-1] for j in [i-1, i-2])
    hl = all(df["Low"].iloc[j] > df["Low"].iloc[j-1] for j in [i-1, i-2])
    return hh and hl and df["Close"].iloc[i] > df["Close"].iloc[i-1]

def _inside_bar_breakout_bull(df, i):
    if i < 2: return False
    mother = i - 2
    inside = i - 1
    is_inside = (df["High"].iloc[inside] < df["High"].iloc[mother]
                 and df["Low"].iloc[inside] > df["Low"].iloc[mother])
    return is_inside and df["Close"].iloc[i] > df["High"].iloc[mother]

def _vcp_pattern(df, i):
    if i < 20: return False
    highs = [df["High"].iloc[i-20], df["High"].iloc[i-12], df["High"].iloc[i-6], df["High"].iloc[i-2]]
    lows  = [df["Low"].iloc[i-20],  df["Low"].iloc[i-12],  df["Low"].iloc[i-6],  df["Low"].iloc[i-2]]
    contracting = all(highs[j] - lows[j] < highs[j-1] - lows[j-1] for j in range(1, 4))
    return contracting and df["Close"].iloc[i] > df["High"].iloc[i-2]

def _demand_zone_bounce(df, i):
    if i < 10: return False
    recent_lo = df["Low"].iloc[i-5:i].min()
    prior_support = df["Low"].iloc[i-10:i-5].min()
    zone_width = abs(recent_lo - prior_support) / prior_support
    return (zone_width < 0.02
            and df["Low"].iloc[i] <= recent_lo * 1.01
            and df["Close"].iloc[i] > df["Open"].iloc[i]
            and df["Close"].iloc[i] > df["Close"].iloc[i-1])

def _three_white_soldiers(df, i):
    if i < 3: return False
    return all(
        _is_bull_candle(df, j) and df["Close"].iloc[j] > df["Close"].iloc[j-1]
        and df["Open"].iloc[j] > df["Open"].iloc[j-1]
        for j in [i, i-1, i-2]
    )

def _pullback_to_sma20_bull(df, i):
    if i < 21: return False
    sma20 = _sma(df, "Close", i-1, 20)
    return (df["Low"].iloc[i] <= sma20 * 1.01
            and df["Close"].iloc[i] > sma20
            and _is_bull_candle(df, i))

def _fib_r2_r3(df, i):
    if i < 20: return False
    swing_lo = df["Low"].iloc[i-20:i-5].min()
    swing_hi = df["High"].iloc[i-20:i-5].max()
    diff = swing_hi - swing_lo
    r2 = swing_hi + diff * 0.618
    r3 = swing_hi + diff * 1.0
    return df["Low"].iloc[i] <= r2 * 1.005 and df["Close"].iloc[i] > r2 and df["High"].iloc[i] < r3

def _breakaway_gap_bull(df, i):
    if i < 5: return False
    return (df["Open"].iloc[i] > df["High"].iloc[i-1] * 1.005
            and _is_bull_candle(df, i)
            and df["Volume"].iloc[i] > _vol_avg(df, i, 10) * 1.5)

def _tightening_closes_bull(df, i):
    if i < 5: return False
    closes = [df["Close"].iloc[i-j] for j in range(5)]
    diffs  = [abs(closes[j] - closes[j+1]) / closes[j+1] for j in range(4)]
    return all(d < 0.008 for d in diffs) and df["Close"].iloc[i] > df["Close"].iloc[i-5]

def _rsi_oversold_reversal(df, i):
    if i < 15: return False
    deltas = df["Close"].diff().iloc[max(0,i-14):i+1]
    gains  = deltas.clip(lower=0).mean()
    losses = (-deltas.clip(upper=0)).mean()
    if losses == 0: return False
    rs  = gains / losses
    rsi = 100 - 100 / (1 + rs)
    prev_deltas = df["Close"].diff().iloc[max(0,i-15):i]
    pg = prev_deltas.clip(lower=0).mean()
    pl = (-prev_deltas.clip(upper=0)).mean()
    if pl == 0: return False
    pr = pg / pl
    prev_rsi = 100 - 100 / (1 + pr)
    return prev_rsi < 30 and rsi > 30

def _sma50_support_hold(df, i):
    if i < 51: return False
    sma50 = _sma(df, "Close", i, 50)
    return (df["Low"].iloc[i] > sma50 * 0.99
            and df["Close"].iloc[i] > sma50
            and df["Close"].iloc[i] > df["Close"].iloc[i-1])

def _vol_dry_up_before_breakout(df, i):
    if i < 10: return False
    prev_low_vol = df["Volume"].iloc[i-5:i-1].min()
    avg_vol = _vol_avg(df, i, 20)
    return (prev_low_vol < avg_vol * 0.6
            and df["Volume"].iloc[i] > avg_vol * 1.5
            and _is_bull_candle(df, i))

def _ascending_triangle_breakout(df, i):
    if i < 15: return False
    highs = df["High"].iloc[i-15:i]
    resistance = highs.max()
    lows = df["Low"].iloc[i-15:i]
    lows_trend = lows.iloc[-1] > lows.iloc[0]
    return (lows_trend
            and df["Close"].iloc[i] > resistance
            and df["Volume"].iloc[i] > _vol_avg(df, i, 15) * 1.2)

def _double_bottom(df, i):
    if i < 20: return False
    lo1_idx = df["Low"].iloc[i-20:i-10].idxmin() if hasattr(df["Low"].iloc[i-20:i-10], 'idxmin') else i-15
    lo2_idx = df["Low"].iloc[i-10:i].idxmin() if hasattr(df["Low"].iloc[i-10:i], 'idxmin') else i-5
    try:
        lo1 = df["Low"].iloc[i-20:i-10].min()
        lo2 = df["Low"].iloc[i-10:i].min()
    except Exception:
        return False
    neck = df["High"].iloc[i-15:i-5].max()
    return (abs(lo1 - lo2) / lo1 < 0.03
            and df["Close"].iloc[i] > neck)

def _bullish_harami(df, i):
    if i < 1: return False
    return (_is_bear_candle(df, i-1)
            and _is_bull_candle(df, i)
            and df["Open"].iloc[i] > df["Close"].iloc[i-1]
            and df["Close"].iloc[i] < df["Open"].iloc[i-1]
            and _body(df, i) < _body(df, i-1) * 0.6)

def _bullish_marubozu(df, i):
    body = _body(df, i)
    rng = _range_(df, i)
    if rng == 0: return False
    return (_is_bull_candle(df, i)
            and body / rng > 0.85
            and df["Volume"].iloc[i] > _vol_avg(df, i, 10) * 1.2)

def _sma_stack_bull(df, i):
    if i < 50: return False
    sma10 = _sma(df, "Close", i, 10)
    sma20 = _sma(df, "Close", i, 20)
    sma50 = _sma(df, "Close", i, 50)
    return df["Close"].iloc[i] > sma10 > sma20 > sma50

def _pivot_breakout_bull(df, i):
    if i < 3: return False
    pivot = (df["High"].iloc[i-1] + df["Low"].iloc[i-1] + df["Close"].iloc[i-1]) / 3
    r1 = 2 * pivot - df["Low"].iloc[i-1]
    return df["Close"].iloc[i] > r1 and _is_bull_candle(df, i)

def _consecutive_bull_bars(df, i):
    if i < 4: return False
    return all(_is_bull_candle(df, j) for j in [i, i-1, i-2])

def _low_vol_tight_range_bull(df, i):
    if i < 10: return False
    ranges = [(df["High"].iloc[i-j] - df["Low"].iloc[i-j]) / df["Close"].iloc[i-j] for j in range(1, 6)]
    avg_vol = _vol_avg(df, i, 20)
    return (all(r < 0.015 for r in ranges)
            and df["Volume"].iloc[i-1] < avg_vol * 0.7
            and _is_bull_candle(df, i))

def _open_above_prev_high_bull(df, i):
    if i < 1: return False
    return (df["Open"].iloc[i] > df["High"].iloc[i-1]
            and df["Close"].iloc[i] > df["Open"].iloc[i])

def _week_high_breakout(df, i):
    if i < 10: return False
    week_hi = df["High"].iloc[i-5:i].max()
    return df["Close"].iloc[i] > week_hi and df["Volume"].iloc[i] > _vol_avg(df, i, 10)

def _close_near_high_bull(df, i):
    rng = _range_(df, i)
    if rng == 0: return False
    return (df["Close"].iloc[i] - df["Low"].iloc[i]) / rng > 0.75 and _is_bull_candle(df, i)

def _higher_low_after_retest(df, i):
    if i < 5: return False
    return (df["Low"].iloc[i] > df["Low"].iloc[i-3]
            and df["Low"].iloc[i-3] < df["Low"].iloc[i-5]
            and _is_bull_candle(df, i))

def _strong_close_midpoint_cross(df, i):
    if i < 1: return False
    mid_prev = (df["High"].iloc[i-1] + df["Low"].iloc[i-1]) / 2
    return df["Close"].iloc[i] > mid_prev * 1.005 and _is_bull_candle(df, i)

def _accumulation_breakout(df, i):
    if i < 30: return False
    prev_hi = df["High"].iloc[i-30:i-5].max()
    vol_in_zone = df["Volume"].iloc[i-30:i-5].mean()
    return (df["Close"].iloc[i] > prev_hi
            and df["Volume"].iloc[i] > vol_in_zone * 1.8)

def _doji_to_bull(df, i):
    if i < 2: return False
    doji_body = _body(df, i-1)
    doji_rng  = _range_(df, i-1)
    is_doji   = doji_rng > 0 and doji_body / doji_rng < 0.15
    return is_doji and _is_bull_candle(df, i) and df["Close"].iloc[i] > df["High"].iloc[i-1]

def _spring_pattern(df, i):
    if i < 10: return False
    support = df["Low"].iloc[i-10:i-1].min()
    return (df["Low"].iloc[i] < support
            and df["Close"].iloc[i] > support
            and df["Volume"].iloc[i] > _vol_avg(df, i, 10) * 1.3)

def _thrust_bar_bull(df, i):
    if i < 5: return False
    avg_rng = sum(_range_(df, j) for j in range(i-5, i)) / 5
    return (_range_(df, i) > avg_rng * 1.5
            and _is_bull_candle(df, i)
            and df["Close"].iloc[i] > df["High"].iloc[i-1])

def _ema_ribbon_bull(df, i):
    if i < 30: return False
    e10 = _ema(df["Close"].iloc[:i+1], 10).iloc[-1]
    e20 = _ema(df["Close"].iloc[:i+1], 20).iloc[-1]
    e30 = _ema(df["Close"].iloc[:i+1], 30).iloc[-1]
    return df["Close"].iloc[i] > e10 > e20 > e30

def _closing_range_expansion_bull(df, i):
    if i < 3: return False
    prev_rng = max(_range_(df, i-1), 0.0001)
    return _range_(df, i) > prev_rng * 1.4 and _is_bull_candle(df, i)

def _reclaim_prev_close_bull(df, i):
    if i < 3: return False
    return (df["Open"].iloc[i] < df["Close"].iloc[i-2]
            and df["Close"].iloc[i] > df["Close"].iloc[i-2])

def _sector_relative_strength(df, i):
    if i < 5: return False
    stock_ret = (df["Close"].iloc[i] - df["Close"].iloc[i-5]) / df["Close"].iloc[i-5]
    return stock_ret > 0.03

def _52w_high_breakout(df, i):
    n = min(i, 252)
    if n < 50: return False
    hi_52w = df["High"].iloc[i-n:i].max()
    return (df["Close"].iloc[i] >= hi_52w * 0.98
            and df["Volume"].iloc[i] > _vol_avg(df, i, 20) * 1.2)

def _multi_bar_base_breakout(df, i):
    if i < 15: return False
    base_hi = df["High"].iloc[i-10:i].max()
    base_lo = df["Low"].iloc[i-10:i].min()
    base_tightness = (base_hi - base_lo) / base_lo
    return base_tightness < 0.06 and df["Close"].iloc[i] > base_hi

def _sma200_reclaim_bull(df, i):
    if i < 201: return False
    sma200 = _sma(df, "Close", i, 200)
    sma200_prev = _sma(df, "Close", i-1, 200)
    return df["Close"].iloc[i-1] < sma200_prev and df["Close"].iloc[i] > sma200

def _high_tight_flag(df, i):
    if i < 10: return False
    pole = (df["High"].iloc[i-8] - df["Low"].iloc[i-10]) / df["Low"].iloc[i-10]
    flag_lo = df["Low"].iloc[i-4:i].min()
    flag_hi = df["High"].iloc[i-4:i].max()
    flag_depth = (flag_hi - flag_lo) / flag_hi
    return pole > 0.10 and flag_depth < 0.05 and df["Close"].iloc[i] > flag_hi

def _reversal_vol_spike_bull(df, i):
    if i < 5: return False
    avg_vol = _vol_avg(df, i, 20)
    return (df["Volume"].iloc[i] > avg_vol * 2.5
            and df["Low"].iloc[i] < df["Low"].iloc[i-1]
            and df["Close"].iloc[i] > df["Open"].iloc[i]
            and df["Close"].iloc[i] > df["Close"].iloc[i-1])

def _wick_rejection_low_bull(df, i):
    rng = _range_(df, i)
    if rng == 0: return False
    lower_wick = min(df["Open"].iloc[i], df["Close"].iloc[i]) - df["Low"].iloc[i]
    return (lower_wick / rng > 0.55
            and _is_bull_candle(df, i)
            and df["Close"].iloc[i] > df["Close"].iloc[i-1])

def _above_key_round_number(df, i):
    price = df["Close"].iloc[i]
    magnitude = 10 ** int(math.log10(price))
    nearest_round = round(price / magnitude) * magnitude
    return (abs(price - nearest_round) / nearest_round < 0.015
            and df["Close"].iloc[i] > df["Open"].iloc[i])

def _bull_continuation_after_gap(df, i):
    if i < 3: return False
    gap = df["Open"].iloc[i-1] > df["High"].iloc[i-2]
    return gap and df["Close"].iloc[i] > df["High"].iloc[i-1]

def _strong_open_strong_close(df, i):
    if i < 1: return False
    open_vs_prev = df["Open"].iloc[i] > df["Close"].iloc[i-1]
    close_near_hi = (df["Close"].iloc[i] - df["Low"].iloc[i]) / max(_range_(df, i), 0.001) > 0.7
    return open_vs_prev and close_near_hi and _is_bull_candle(df, i)

def _increasing_vol_on_up_days(df, i):
    if i < 5: return False
    up_vols   = [df["Volume"].iloc[j] for j in range(i-4, i+1) if _is_bull_candle(df, j)]
    down_vols = [df["Volume"].iloc[j] for j in range(i-4, i+1) if _is_bear_candle(df, j)]
    if not up_vols or not down_vols: return False
    return sum(up_vols) / len(up_vols) > sum(down_vols) / len(down_vols) * 1.3

def _bull_power_candle(df, i):
    if i < 10: return False
    avg_body = sum(_body(df, j) for j in range(i-10, i)) / 10
    return _body(df, i) > avg_body * 2.0 and _is_bull_candle(df, i)

def _price_compression_bull(df, i):
    if i < 10: return False
    recent_rng  = (df["High"].iloc[i-3:i].max() - df["Low"].iloc[i-3:i].min()) / df["Close"].iloc[i-3]
    broader_rng = (df["High"].iloc[i-10:i-3].max() - df["Low"].iloc[i-10:i-3].min()) / df["Close"].iloc[i-10]
    return recent_rng < broader_rng * 0.5 and _is_bull_candle(df, i)

def _new_month_high_bull(df, i):
    if i < 22: return False
    month_hi = df["High"].iloc[i-22:i].max()
    return df["Close"].iloc[i] >= month_hi and _is_bull_candle(df, i)

def _low_wick_candle_series(df, i):
    if i < 3: return False
    return all(
        (min(df["Open"].iloc[j], df["Close"].iloc[j]) - df["Low"].iloc[j]) / max(_range_(df, j), 0.001) < 0.15
        for j in [i, i-1, i-2]
    )

def _trend_day_bull(df, i):
    if i < 1: return False
    open_p = df["Open"].iloc[i]
    close_p = df["Close"].iloc[i]
    if _range_(df, i) == 0: return False
    return ((close_p - open_p) / _range_(df, i) > 0.7
            and _is_bull_candle(df, i)
            and df["Volume"].iloc[i] > _vol_avg(df, i, 10))

def _pullback_to_ema50_bull(df, i):
    if i < 51: return False
    ema50 = _ema(df["Close"].iloc[:i+1], 50).iloc[-1]
    return (df["Low"].iloc[i] <= ema50 * 1.01
            and df["Close"].iloc[i] > ema50
            and _is_bull_candle(df, i))

def _three_bar_reversal_bull(df, i):
    if i < 3: return False
    return (_is_bear_candle(df, i-2)
            and _range_(df, i-1) < _range_(df, i-2) * 0.6
            and _is_bull_candle(df, i)
            and df["Close"].iloc[i] > df["Open"].iloc[i-2])

def _new_52w_high_momentum(df, i):
    n = min(i, 252)
    if n < 20: return False
    past_hi = df["High"].iloc[i-n:i-1].max()
    return (df["High"].iloc[i] > past_hi
            and df["Volume"].iloc[i] > _vol_avg(df, i, 20) * 1.3
            and _is_bull_candle(df, i))

def _consolidation_tight_bull(df, i):
    if i < 8: return False
    highs = df["High"].iloc[i-7:i]
    lows  = df["Low"].iloc[i-7:i]
    rng   = (highs.max() - lows.min()) / lows.min()
    return rng < 0.04 and df["Close"].iloc[i] > highs.max() * 0.99

def _rising_lows_bull(df, i):
    if i < 6: return False
    lows = [df["Low"].iloc[i-j] for j in range(5)]
    return all(lows[j] > lows[j+1] for j in range(4))

def _candle_size_expansion_bull(df, i):
    if i < 5: return False
    avg = sum(_range_(df, j) for j in range(i-5, i)) / 5
    return _range_(df, i) > avg * 1.6 and _is_bull_candle(df, i)

def _price_action_momentum_bull(df, i):
    if i < 5: return False
    ret5 = (df["Close"].iloc[i] - df["Close"].iloc[i-5]) / df["Close"].iloc[i-5]
    return ret5 > 0.04 and _is_bull_candle(df, i)

def _vol_confirmed_breakout(df, i):
    if i < 20: return False
    prev_hi = df["High"].iloc[i-20:i].max()
    return (df["Close"].iloc[i] > prev_hi
            and df["Volume"].iloc[i] > _vol_avg(df, i, 20) * 2.0)

def _morning_doji_star(df, i):
    if i < 2: return False
    big_bear   = _is_bear_candle(df, i-2) and _body(df, i-2) > _range_(df, i-2) * 0.6
    small_body = _body(df, i-1) / max(_range_(df, i-1), 0.001) < 0.2
    big_bull   = _is_bull_candle(df, i) and _body(df, i) > _range_(df, i) * 0.6
    return big_bear and small_body and big_bull

def _gap_fill_and_continue_bull(df, i):
    if i < 5: return False
    gap_open = df["Open"].iloc[i-3] < df["Close"].iloc[i-4]
    fill_gap  = df["Close"].iloc[i-1] >= df["Close"].iloc[i-4]
    return gap_open and fill_gap and _is_bull_candle(df, i)

def _wide_range_bar_bull(df, i):
    if i < 10: return False
    avg = sum(_range_(df, j) for j in range(i-10, i)) / 10
    return _range_(df, i) > avg * 2.0 and _is_bull_candle(df, i)

def _price_near_vwap_bounce(df, i):
    if i < 20: return False
    c = df["Close"].iloc[i-20:i]; v = df["Volume"].iloc[i-20:i]
    vwap = (c * v).sum() / v.sum() if v.sum() > 0 else df["Close"].iloc[i]
    return (df["Low"].iloc[i] <= vwap * 1.005
            and df["Close"].iloc[i] > vwap
            and _is_bull_candle(df, i))

def _higher_open_higher_close(df, i):
    if i < 1: return False
    return (df["Open"].iloc[i] > df["Open"].iloc[i-1]
            and df["Close"].iloc[i] > df["Close"].iloc[i-1])

def _macd_cross_bull(df, i):
    if i < 30: return False
    ema12 = _ema(df["Close"].iloc[:i+1], 12).iloc[-1]
    ema26 = _ema(df["Close"].iloc[:i+1], 26).iloc[-1]
    ema12p = _ema(df["Close"].iloc[:i], 12).iloc[-1]
    ema26p = _ema(df["Close"].iloc[:i], 26).iloc[-1]
    return (ema12p - ema26p) < 0 and (ema12 - ema26) > 0

def _price_action_acceleration_bull(df, i):
    if i < 10: return False
    ret_early = (df["Close"].iloc[i-5] - df["Close"].iloc[i-10]) / df["Close"].iloc[i-10]
    ret_late  = (df["Close"].iloc[i] - df["Close"].iloc[i-5]) / df["Close"].iloc[i-5]
    return ret_late > ret_early and ret_late > 0.02

def _exhaustion_bottom_bull(df, i):
    if i < 5: return False
    dropped = (df["Close"].iloc[i-5] - df["Close"].iloc[i-1]) / df["Close"].iloc[i-5] > 0.07
    reversal = df["Close"].iloc[i] > df["Close"].iloc[i-1] * 1.02
    return dropped and reversal and df["Volume"].iloc[i] > _vol_avg(df, i, 10) * 1.5

def _trend_reversal_candle_bull(df, i):
    if i < 3: return False
    prior_down = df["Close"].iloc[i-3] > df["Close"].iloc[i-2] > df["Close"].iloc[i-1]
    strong_bull = _is_bull_candle(df, i) and df["Close"].iloc[i] > df["Close"].iloc[i-2]
    return prior_down and strong_bull

def _support_cluster_bull(df, i):
    if i < 30: return False
    sma20 = _sma(df, "Close", i, 20)
    sma50 = _sma(df, "Close", i, 50)
    cluster = abs(sma20 - sma50) / sma50 < 0.02
    return cluster and df["Close"].iloc[i] > sma20 and _is_bull_candle(df, i)

def _high_volume_reversal_bull(df, i):
    if i < 5: return False
    avg_vol = _vol_avg(df, i, 20)
    prev_bear = _is_bear_candle(df, i-1)
    return (prev_bear
            and df["Volume"].iloc[i-1] > avg_vol * 2.5
            and _is_bull_candle(df, i)
            and df["Close"].iloc[i] > df["High"].iloc[i-1])

def _small_body_before_big_bull(df, i):
    if i < 2: return False
    small_prev = _body(df, i-1) < _body(df, i-2) * 0.4
    big_now    = _body(df, i) > _body(df, i-2) * 1.3
    return small_prev and big_now and _is_bull_candle(df, i)

def _ema_slope_positive(df, i):
    if i < 21: return False
    e20_now  = _ema(df["Close"].iloc[:i+1], 20).iloc[-1]
    e20_prev = _ema(df["Close"].iloc[:i],   20).iloc[-1]
    return e20_now > e20_prev and df["Close"].iloc[i] > e20_now

def _high_price_range_in_trend(df, i):
    if i < 5: return False
    all_up = all(df["Close"].iloc[j] > df["Close"].iloc[j-1] for j in range(i-3, i+1))
    return all_up and df["Volume"].iloc[i] > _vol_avg(df, i, 10)

def _retest_breakout_level_bull(df, i):
    if i < 15: return False
    breakout_level = df["High"].iloc[i-15:i-5].max()
    return (df["Low"].iloc[i] <= breakout_level * 1.01
            and df["Close"].iloc[i] > breakout_level
            and _is_bull_candle(df, i))

def _close_above_open_range_bull(df, i):
    if i < 1: return False
    or_hi = max(df["Open"].iloc[i], df["High"].iloc[i-1])
    return df["Close"].iloc[i] > or_hi and _is_bull_candle(df, i)

def _vol_spike_pullback_buy(df, i):
    if i < 10: return False
    avg_vol = _vol_avg(df, i, 20)
    prev_spike = df["Volume"].iloc[i-3] > avg_vol * 2.0 and _is_bull_candle(df, i-3)
    pullback = df["Close"].iloc[i-1] < df["Close"].iloc[i-3]
    return prev_spike and pullback and _is_bull_candle(df, i)

def _strong_first_bar_continuation(df, i):
    if i < 2: return False
    first_strong = _is_bull_candle(df, i-1) and _body(df, i-1) > _range_(df, i-1) * 0.7
    return first_strong and _is_bull_candle(df, i) and df["Open"].iloc[i] >= df["Close"].iloc[i-1]

def _narrow_spread_accumulation(df, i):
    if i < 10: return False
    spreads = [_range_(df, j) / df["Close"].iloc[j] for j in range(i-9, i)]
    return (max(spreads) < 0.02
            and df["Volume"].iloc[i-1] < _vol_avg(df, i, 20) * 0.8
            and _is_bull_candle(df, i))

def _close_upper_quartile_series(df, i):
    if i < 3: return False
    def upper_q(j):
        rng = _range_(df, j)
        if rng == 0: return False
        return (df["Close"].iloc[j] - df["Low"].iloc[j]) / rng > 0.75
    return all(upper_q(j) for j in [i, i-1, i-2])

def _momentum_persistence_bull(df, i):
    if i < 10: return False
    up_bars = sum(1 for j in range(i-10, i+1) if _is_bull_candle(df, j))
    return up_bars >= 7 and _is_bull_candle(df, i)

def _gap_and_go_bull(df, i):
    if i < 2: return False
    gap = df["Open"].iloc[i] > df["High"].iloc[i-1]
    go  = df["Close"].iloc[i] > df["Open"].iloc[i] and df["Close"].iloc[i] > df["High"].iloc[i-1]
    return gap and go

def _sma20_slope_bull(df, i):
    if i < 22: return False
    s_now  = _sma(df, "Close", i, 20)
    s_prev = _sma(df, "Close", i-2, 20)
    return s_now > s_prev and df["Close"].iloc[i] > s_now

def _green_close_after_red_open(df, i):
    if i < 1: return False
    return (df["Open"].iloc[i] < df["Close"].iloc[i-1]
            and df["Close"].iloc[i] > df["Close"].iloc[i-1])

def _volume_trend_bull(df, i):
    if i < 10: return False
    vols = [df["Volume"].iloc[i-j] for j in range(10)]
    up_days   = [j for j in range(10) if _is_bull_candle(df, i-j)]
    down_days = [j for j in range(10) if _is_bear_candle(df, i-j)]
    if not up_days or not down_days: return False
    avg_up   = sum(df["Volume"].iloc[i-j] for j in up_days) / len(up_days)
    avg_down = sum(df["Volume"].iloc[i-j] for j in down_days) / len(down_days)
    return avg_up > avg_down * 1.2

def _body_to_range_bull(df, i):
    rng = _range_(df, i)
    if rng == 0: return False
    return _body(df, i) / rng > 0.65 and _is_bull_candle(df, i)

def _swing_low_higher_bull(df, i):
    if i < 10: return False
    local_lo = df["Low"].iloc[i-10:i].min()
    prev_lo  = df["Low"].iloc[i-20:i-10].min() if i >= 20 else df["Low"].iloc[:i-10].min()
    return local_lo > prev_lo and _is_bull_candle(df, i)

def _power_hour_close_bull(df, i):
    if i < 1: return False
    return (df["Close"].iloc[i] == df["High"].iloc[i]
            or (df["High"].iloc[i] - df["Close"].iloc[i]) / max(_range_(df, i), 0.001) < 0.05)

def _anchor_vwap_above(df, i):
    if i < 20: return False
    c = df["Close"].iloc[i-20:i+1]; v = df["Volume"].iloc[i-20:i+1]
    vwap = (c * v).sum() / v.sum() if v.sum() > 0 else df["Close"].iloc[i]
    return df["Close"].iloc[i] > vwap and _is_bull_candle(df, i)

def _expansion_day_follow_through(df, i):
    if i < 2: return False
    big_day = _range_(df, i-1) > sum(_range_(df, j) for j in range(i-6, i-1)) / 5 * 1.5
    return big_day and _is_bull_candle(df, i-1) and df["Open"].iloc[i] > df["Close"].iloc[i-1] * 0.99

def _wicks_shorter_bull(df, i):
    if i < 5: return False
    def upper_wick(j):
        return df["High"].iloc[j] - max(df["Open"].iloc[j], df["Close"].iloc[j])
    recent_wicks = [upper_wick(j) for j in range(i-4, i+1)]
    return recent_wicks[-1] < recent_wicks[0] and _is_bull_candle(df, i)

def _trend_line_support_bounce(df, i):
    if i < 10: return False
    lo_series = [df["Low"].iloc[i-j] for j in range(10)]
    rising    = lo_series[0] > lo_series[-1]
    return rising and df["Close"].iloc[i] > df["Close"].iloc[i-1] * 1.005

def _above_prev_day_midpoint(df, i):
    if i < 1: return False
    mid = (df["High"].iloc[i-1] + df["Low"].iloc[i-1]) / 2
    return df["Close"].iloc[i] > mid and df["Open"].iloc[i] > mid

def _decreasing_sell_pressure(df, i):
    if i < 10: return False
    bear_bodies = [_body(df, j) for j in range(i-9, i+1) if _is_bear_candle(df, j)]
    if len(bear_bodies) < 3: return True
    return bear_bodies[-1] < bear_bodies[0] and _is_bull_candle(df, i)

def _volume_price_confirmation_bull(df, i):
    if i < 3: return False
    price_up = df["Close"].iloc[i] > df["Close"].iloc[i-1]
    vol_up   = df["Volume"].iloc[i] > df["Volume"].iloc[i-1]
    return price_up and vol_up and _is_bull_candle(df, i)

def _rising_sma20_price_above(df, i):
    if i < 22: return False
    sma_now  = _sma(df, "Close", i, 20)
    sma_prev = _sma(df, "Close", i-3, 20)
    return sma_now > sma_prev and df["Close"].iloc[i] > sma_now

def _low_atr_coil_bull(df, i):
    if i < 20: return False
    atr_now  = _atr(df, i, 10)
    atr_prev = _atr(df, i-10, 10)
    return atr_now < atr_prev * 0.7 and _is_bull_candle(df, i)

def _follow_through_day_bull(df, i):
    if i < 5: return False
    big_bar  = _is_bull_candle(df, i-4) and _range_(df, i-4) > sum(_range_(df,j) for j in range(i-8,i-4))/4
    pullback = df["Close"].iloc[i-1] < df["Close"].iloc[i-4]
    follow   = _is_bull_candle(df, i) and df["Close"].iloc[i] > df["High"].iloc[i-4]
    return big_bar and pullback and follow

def _open_equals_low_bull(df, i):
    if i < 1: return False
    return (abs(df["Open"].iloc[i] - df["Low"].iloc[i]) / max(df["Close"].iloc[i], 0.001) < 0.002
            and _is_bull_candle(df, i))

def _price_density_above_sma(df, i):
    if i < 21: return False
    sma20 = _sma(df, "Close", i, 20)
    pct_above = sum(1 for j in range(i-20, i+1) if df["Close"].iloc[j] > sma20) / 21
    return pct_above > 0.75 and df["Close"].iloc[i] > sma20

def _thrust_from_consolidation_bull(df, i):
    if i < 10: return False
    base_range = (df["High"].iloc[i-8:i-2].max() - df["Low"].iloc[i-8:i-2].min()) / df["Close"].iloc[i-8]
    return (base_range < 0.05
            and df["Close"].iloc[i] > df["High"].iloc[i-2]
            and _range_(df, i) > _range_(df, i-2) * 1.5)

def _close_vs_prev_range_bull(df, i):
    if i < 1: return False
    prev_rng = _range_(df, i-1)
    if prev_rng == 0: return False
    return df["Close"].iloc[i] > df["High"].iloc[i-1] + prev_rng * 0.1

def _bull_outside_day(df, i):
    if i < 1: return False
    return (df["High"].iloc[i] > df["High"].iloc[i-1]
            and df["Low"].iloc[i] < df["Low"].iloc[i-1]
            and _is_bull_candle(df, i))

def _bear_gap_fill_continuation_bull(df, i):
    if i < 5: return False
    bear_gap = df["Open"].iloc[i-4] < df["Close"].iloc[i-5]
    filled   = df["Close"].iloc[i] >= df["Close"].iloc[i-5]
    return bear_gap and filled and _is_bull_candle(df, i)

# ── Strategy registry ──────────────────────────────────────────────

BULLISH_STRATEGIES = [
    ("Bull engulfing",           _bull_engulfing),
    ("Morning star",             _morning_star),
    ("Hammer",                   _hammer),
    ("Flag & pole",              _flag_and_pole),
    ("Range breakout",           _range_breakout_bull),
    ("Volume surge",             _volume_surge_bull),
    ("Cup & handle",             _cup_and_handle),
    ("Golden cross",             _golden_cross),
    ("EMA breakout",             _ema_breakout),
    ("Higher highs / lows",      _higher_highs_higher_lows),
    ("Inside bar breakout",      _inside_bar_breakout_bull),
    ("VCP pattern",              _vcp_pattern),
    ("Demand zone bounce",       _demand_zone_bounce),
    ("Three white soldiers",     _three_white_soldiers),
    ("SMA20 pullback",           _pullback_to_sma20_bull),
    ("Fibonacci R2→R3",          _fib_r2_r3),
    ("Breakaway gap",            _breakaway_gap_bull),
    ("Tightening closes",        _tightening_closes_bull),
    ("RSI oversold reversal",    _rsi_oversold_reversal),
    ("SMA50 support hold",       _sma50_support_hold),
    ("Vol dry-up breakout",      _vol_dry_up_before_breakout),
    ("Ascending triangle",       _ascending_triangle_breakout),
    ("Double bottom",            _double_bottom),
    ("Bullish harami",           _bullish_harami),
    ("Bullish marubozu",         _bullish_marubozu),
    ("SMA stack bull",           _sma_stack_bull),
    ("Pivot breakout",           _pivot_breakout_bull),
    ("Consecutive bull bars",    _consecutive_bull_bars),
    ("Low-vol tight range",      _low_vol_tight_range_bull),
    ("Open above prev high",     _open_above_prev_high_bull),
    ("Week high breakout",       _week_high_breakout),
    ("Close near high",          _close_near_high_bull),
    ("Higher low retest",        _higher_low_after_retest),
    ("Midpoint cross bull",      _strong_close_midpoint_cross),
    ("Accumulation breakout",    _accumulation_breakout),
    ("Doji to bull",             _doji_to_bull),
    ("Spring pattern",           _spring_pattern),
    ("Thrust bar bull",          _thrust_bar_bull),
    ("EMA ribbon bull",          _ema_ribbon_bull),
    ("Range expansion bull",     _closing_range_expansion_bull),
    ("Reclaim prev close",       _reclaim_prev_close_bull),
    ("Sector RS bull",           _sector_relative_strength),
    ("52W high breakout",        _52w_high_breakout),
    ("Multi-bar base break",     _multi_bar_base_breakout),
    ("SMA200 reclaim",           _sma200_reclaim_bull),
    ("High tight flag",          _high_tight_flag),
    ("Reversal vol spike bull",  _reversal_vol_spike_bull),
    ("Wick rejection low",       _wick_rejection_low_bull),
    ("Bull continuation gap",    _bull_continuation_after_gap),
    ("Strong open close bull",   _strong_open_strong_close),
    ("Up-day vol increase",      _increasing_vol_on_up_days),
    ("Bull power candle",        _bull_power_candle),
    ("Price compression bull",   _price_compression_bull),
    ("New month high",           _new_month_high_bull),
    ("Low wick series",          _low_wick_candle_series),
    ("Trend day bull",           _trend_day_bull),
    ("EMA50 pullback",           _pullback_to_ema50_bull),
    ("Three-bar reversal bull",  _three_bar_reversal_bull),
    ("52W high momentum",        _new_52w_high_momentum),
    ("Tight consolidation bull", _consolidation_tight_bull),
    ("Rising lows",              _rising_lows_bull),
    ("Candle expansion bull",    _candle_size_expansion_bull),
    ("PA momentum bull",         _price_action_momentum_bull),
    ("Vol confirmed break",      _vol_confirmed_breakout),
    ("Morning doji star",        _morning_doji_star),
    ("Gap fill continue bull",   _gap_fill_and_continue_bull),
    ("Wide range bar bull",      _wide_range_bar_bull),
    ("VWAP bounce",              _price_near_vwap_bounce),
    ("Higher open/close",        _higher_open_higher_close),
    ("MACD cross bull",          _macd_cross_bull),
    ("PA acceleration bull",     _price_action_acceleration_bull),
    ("Exhaustion bottom",        _exhaustion_bottom_bull),
    ("Trend reversal bull",      _trend_reversal_candle_bull),
    ("Support cluster bull",     _support_cluster_bull),
    ("High vol reversal bull",   _high_volume_reversal_bull),
    ("Small body big bull",      _small_body_before_big_bull),
    ("EMA slope positive",       _ema_slope_positive),
    ("High price in trend",      _high_price_range_in_trend),
    ("Retest break level",       _retest_breakout_level_bull),
    ("Close above OR",           _close_above_open_range_bull),
    ("Vol spike pullback buy",   _vol_spike_pullback_buy),
    ("First bar continuation",   _strong_first_bar_continuation),
    ("Narrow spread accum",      _narrow_spread_accumulation),
    ("Upper quartile series",    _close_upper_quartile_series),
    ("Momentum persistence",     _momentum_persistence_bull),
    ("Gap and go bull",          _gap_and_go_bull),
    ("SMA20 slope bull",         _sma20_slope_bull),
    ("Green close red open",     _green_close_after_red_open),
    ("Volume trend bull",        _volume_trend_bull),
    ("Body to range bull",       _body_to_range_bull),
    ("Swing low higher",         _swing_low_higher_bull),
    ("Power hour close",         _power_hour_close_bull),
    ("Above VWAP anchor",        _anchor_vwap_above),
    ("Expansion follow-thru",    _expansion_day_follow_through),
    ("Wicks shortening bull",    _wicks_shorter_bull),
    ("Trend line bounce",        _trend_line_support_bounce),
    ("Above prev mid bull",      _above_prev_day_midpoint),
    ("Sell pressure decrease",   _decreasing_sell_pressure),
    ("Vol price confirm bull",   _volume_price_confirmation_bull),
    ("Rising SMA20 above",       _rising_sma20_price_above),
    ("Low ATR coil bull",        _low_atr_coil_bull),
    ("Follow through day",       _follow_through_day_bull),
    ("Open equals low",          _open_equals_low_bull),
    ("Price density above SMA",  _price_density_above_sma),
    ("Thrust consolidation",     _thrust_from_consolidation_bull),
    ("Close vs prev range",      _close_vs_prev_range_bull),
    ("Bull outside day",         _bull_outside_day),
    ("Bear gap fill bull",       _bear_gap_fill_continuation_bull),
]


ALL_LONG_STRATEGIES = BULLISH_STRATEGIES
STRATEGY_COUNT = len(ALL_LONG_STRATEGIES)
