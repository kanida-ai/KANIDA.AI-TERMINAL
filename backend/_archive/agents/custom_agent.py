"""
KANIDA — CUSTOM AGENT (Build Your Own)
=======================================
Universal strategy fingerprint engine.

Architecture:
  Bot 1 — FingerprintBuilder   : Pre-computes per-stock strategy win rates
  Bot 2 — LiveScorer           : Checks which strategies are firing today
  Bot 3 — HistoricalOutcomes   : Forward outcome stats for firing strategies
  Bot 4 — MarketRegimeFilter   : NIFTY/SPY regime + sector context
  Bot 5 — RiskCalculator       : Capital, position size (half-Kelly), targets
  Bot 6 — NarrativeWriter      : Plain-English output (SEBI-compliant)
  Bot 7 — PaperTradeLogger     : Forward ledger (when enabled)

Usage:
  python custom_agent.py --ticker HDFCBANK --market NSE --bias bullish
  python custom_agent.py --ticker RELIANCE --market NSE --bias bullish --timeframe 1D
  python custom_agent.py --ticker AAPL --market US --bias bearish --capital 25000
  python custom_agent.py --ticker TCS --market NSE --bias neutral --backtest 3y
  python custom_agent.py --ticker NVDA --market US --bias bullish --paper-trade
"""

import warnings; warnings.filterwarnings("ignore")
import argparse
import json
import math
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

try:
    import yfinance as yf
    YF_AVAILABLE = True
except ImportError:
    YF_AVAILABLE = False

try:
    import pandas as pd
    import numpy as np
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


# ══════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════

TODAY            = datetime.today()
STRATEGY_THRESHOLD = 0.55   # minimum historical win rate for a strategy to qualify per stock
MIN_INSTANCES    = 5         # minimum times a pattern must appear to be counted
FORWARD_DAYS     = 15        # trading days for forward outcome measurement
REGIME_TICKER_NSE = "^NSEI"
REGIME_TICKER_US  = "SPY"
W = 70                       # output width

BACKTEST_YEARS = {
    "1y":  1,
    "3y":  3,
    "5y":  5,
    "10y": 10,
}

TIMEFRAME_INTERVAL = {
    "30m": "30m",
    "1H":  "1h",
    "4H":  "1h",   # yfinance has no 4h — we resample 1h → 4h
    "1D":  "1d",
    "1W":  "1wk",
    "1M":  "1mo",
}

TIMEFRAME_BARS = {
    "30m": 50,
    "1H":  50,
    "4H":  50,
    "1D":  60,
    "1W":  52,
    "1M":  24,
}


# ══════════════════════════════════════════════════════════════════
# STOCK UNIVERSES
# ══════════════════════════════════════════════════════════════════

NSE_FNO = [
    "AARTIIND","ABB","ABBOTINDIA","ABCAPITAL","ABFRL","ACC","ADANIENT",
    "ADANIPORTS","ALKEM","AMBUJACEM","APOLLOHOSP","APOLLOTYRE","ASHOKLEY",
    "ASIANPAINT","ASTRAL","AUBANK","AUROPHARMA","AXISBANK",
    "BAJAJ-AUTO","BAJAJFINSV","BAJFINANCE","BALKRISIND","BANDHANBNK",
    "BANKBARODA","BATAINDIA","BEL","BERGEPAINT","BHARATFORG","BHARTIARTL",
    "BHEL","BIOCON","BPCL","BRITANNIA","CANBK","CANFINHOME",
    "CHOLAFIN","CIPLA","COALINDIA","COFORGE","COLPAL","CONCOR",
    "CROMPTON","DABUR","DEEPAKNTR","DIVISLAB","DIXON","DLF","DRREDDY",
    "EICHERMOT","ESCORTS","EXIDEIND","FEDERALBNK","GAIL","GLENMARK",
    "GODREJCP","GODREJPROP","GRANULES","GRASIM","GUJGASLTD","HAL",
    "HAVELLS","HCLTECH","HDFCAMC","HDFCBANK","HDFCLIFE","HEROMOTOCO",
    "HINDALCO","HINDPETRO","HINDUNILVR","ICICIBANK","ICICIGI","ICICIPRULI",
    "IDEA","IDFCFIRSTB","IEX","IGL","INDHOTEL","INDUSINDBK","INFY",
    "IOC","IRCTC","IRFC","ITC","JINDALSTEL","JSWSTEEL","JUBLFOOD",
    "KOTAKBANK","LAURUSLABS","LICHSGFIN","LICI","LT","LTIM","LTTS",
    "LUPIN","M&M","MANAPPURAM","MARICO","MARUTI","MCX","MPHASIS",
    "MRF","MUTHOOTFIN","NAUKRI","NESTLEIND","NMDC","NTPC","OFSS",
    "ONGC","PAGEIND","PERSISTENT","PETRONET","PFC","PIDILITIND","PNB",
    "POLYCAB","POWERGRID","PVRINOX","RECLTD","RELIANCE","SAIL",
    "SBICARD","SBILIFE","SBIN","SHREECEM","SIEMENS","SRF","SUNPHARMA",
    "TATACHEM","TATACONSUM","TATAMOTORS","TATAPOWER","TATASTEEL",
    "TCS","TECHM","TITAN","TORNTPHARM","TORNTPOWER","TRENT",
    "TVSMOTOR","UBL","ULTRACEMCO","UPL","VEDL","VOLTAS","WIPRO",
    "ZOMATO","ZYDUSLIFE","MCDOWELL-N",
]

US_OPTIONS = [
    "AAPL","MSFT","AMZN","GOOGL","META","NVDA","TSLA","AMD",
    "NFLX","CRM","ORCL","ADBE","INTC","QCOM","TXN","MU",
    "AVGO","AMAT","LRCX","KLAC","MRVL","PANW","CRWD","ZS",
    "SNOW","PLTR","COIN","UBER","LYFT","DASH","ABNB","SHOP",
    "PYPL","V","MA","AXP","GS","MS","JPM","BAC","WFC","C",
    "BLK","SCHW","JNJ","PFE","ABBV","BMY","LLY","UNH",
    "XOM","CVX","COP","SLB","OXY","BA","RTX","LMT","GE","CAT",
    "WMT","TGT","COST","HD","NKE","SBUX","MCD","DIS","T","VZ",
]

SECTOR_MAP_NSE = {
    "HDFCBANK":"Banks","ICICIBANK":"Banks","SBIN":"Banks","AXISBANK":"Banks",
    "KOTAKBANK":"Banks","BAJFINANCE":"NBFC","INDUSINDBK":"Banks","IDFCFIRSTB":"Banks",
    "TCS":"IT","INFY":"IT","WIPRO":"IT","HCLTECH":"IT","TECHM":"IT",
    "MPHASIS":"IT","PERSISTENT":"IT","LTIM":"IT","LTTS":"IT","COFORGE":"IT",
    "SUNPHARMA":"Pharma","CIPLA":"Pharma","DRREDDY":"Pharma","LUPIN":"Pharma",
    "AUROPHARMA":"Pharma","DIVISLAB":"Pharma","BIOCON":"Pharma",
    "MARUTI":"Auto","TATAMOTORS":"Auto","M&M":"Auto","HEROMOTOCO":"Auto",
    "BAJAJ-AUTO":"Auto","EICHERMOT":"Auto","TVSMOTOR":"Auto","ESCORTS":"Auto",
    "RELIANCE":"Energy","BPCL":"Energy","IOC":"Energy","HINDPETRO":"Energy",
    "ONGC":"Energy","GAIL":"Energy","PETRONET":"Energy",
    "TATASTEEL":"Metals","JSWSTEEL":"Metals","HINDALCO":"Metals",
    "VEDL":"Metals","NMDC":"Metals","SAIL":"Metals","JINDALSTEL":"Metals",
    "HINDUNILVR":"FMCG","ITC":"FMCG","BRITANNIA":"FMCG","NESTLEIND":"FMCG",
    "MARICO":"FMCG","DABUR":"FMCG","COLPAL":"FMCG","GODREJCP":"FMCG",
    "LT":"Capital Goods","HAL":"Capital Goods","SIEMENS":"Capital Goods",
    "HAVELLS":"Capital Goods","POLYCAB":"Capital Goods","ABB":"Capital Goods",
    "ASIANPAINT":"Paints","BERGEPAINT":"Paints","PIDILITIND":"Chemicals",
    "DEEPAKNTR":"Chemicals","TITAN":"Consumer","TRENT":"Retail",
    "DMART":"Retail","IRCTC":"Travel","INDHOTEL":"Hotels",
    "BHARTIARTL":"Telecom","NTPC":"Power","POWERGRID":"Power",
    "TATAPOWER":"Power","TORNTPOWER":"Power","JSWENERGY":"Power",
    "ZOMATO":"Internet","NAUKRI":"Internet","MCDOWELL-N":"Consumer",
}


# ══════════════════════════════════════════════════════════════════
# STRATEGY DEFINITIONS  — 300 total (100 per bias)
# Each strategy is a function: (df, idx) -> bool
# df has columns: Open, High, Low, Close, Volume
# idx is the current bar index
# ══════════════════════════════════════════════════════════════════

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


# ── BULLISH STRATEGIES (100) ──────────────────────────────────────

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


# ── BEARISH STRATEGIES (100) ──────────────────────────────────────

def _bear_engulfing(df, i):
    if i < 1: return False
    return (_is_bull_candle(df, i-1) and _is_bear_candle(df, i)
            and df["Open"].iloc[i] > df["Close"].iloc[i-1]
            and df["Close"].iloc[i] < df["Open"].iloc[i-1]
            and _body(df, i) > _body(df, i-1))

def _evening_star(df, i):
    if i < 2: return False
    return (_is_bull_candle(df, i-2)
            and _body(df, i-1) < _body(df, i-2) * 0.5
            and _is_bear_candle(df, i)
            and df["Close"].iloc[i] < (df["Open"].iloc[i-2] + df["Close"].iloc[i-2]) / 2)

def _shooting_star(df, i):
    if i < 1: return False
    upper_wick = df["High"].iloc[i] - max(df["Open"].iloc[i], df["Close"].iloc[i])
    body = _body(df, i)
    rng  = _range_(df, i)
    return (upper_wick >= 2 * body
            and body / max(rng, 0.001) < 0.35
            and df["Close"].iloc[i] < df["Close"].iloc[i-1])

def _head_and_shoulders(df, i):
    if i < 20: return False
    ls  = df["High"].iloc[i-18:i-12].max()
    hd  = df["High"].iloc[i-12:i-6].max()
    rs  = df["High"].iloc[i-6:i].max()
    neck = df["Low"].iloc[i-12:i].mean()
    return (hd > ls and hd > rs
            and abs(ls - rs) / ls < 0.05
            and df["Close"].iloc[i] < neck)

def _range_breakdown_bear(df, i):
    if i < 20: return False
    rng_lo = df["Low"].iloc[i-20:i].min()
    rng_hi = df["High"].iloc[i-20:i].max()
    rng_size = (rng_hi - rng_lo) / rng_lo
    if rng_size > 0.25: return False
    return df["Close"].iloc[i] < rng_lo * 0.995 and df["Volume"].iloc[i] > _vol_avg(df, i, 10) * 1.3

def _volume_surge_bear(df, i):
    if i < 20: return False
    avg_vol = _vol_avg(df, i, 20)
    return (df["Volume"].iloc[i] > avg_vol * 2.0
            and _is_bear_candle(df, i)
            and df["Close"].iloc[i] < df["Close"].iloc[i-1] * 0.99)

def _death_cross(df, i):
    if i < 50: return False
    sma20 = _sma(df, "Close", i, 20)
    sma50 = _sma(df, "Close", i, 50)
    sma20p = _sma(df, "Close", i-1, 20)
    sma50p = _sma(df, "Close", i-1, 50)
    return sma20p >= sma50p and sma20 < sma50

def _supply_zone_tap(df, i):
    if i < 10: return False
    prior_hi = df["High"].iloc[i-10:i-2].max()
    return (df["High"].iloc[i] >= prior_hi * 0.99
            and df["Close"].iloc[i] < prior_hi
            and _is_bear_candle(df, i))

def _lower_highs_lower_lows(df, i):
    if i < 4: return False
    lh = all(df["High"].iloc[j] < df["High"].iloc[j-1] for j in [i-1, i-2])
    ll = all(df["Low"].iloc[j] < df["Low"].iloc[j-1] for j in [i-1, i-2])
    return lh and ll and df["Close"].iloc[i] < df["Close"].iloc[i-1]

def _dark_cloud_cover(df, i):
    if i < 1: return False
    mid = (df["Open"].iloc[i-1] + df["Close"].iloc[i-1]) / 2
    return (_is_bull_candle(df, i-1)
            and df["Open"].iloc[i] > df["High"].iloc[i-1]
            and df["Close"].iloc[i] < mid
            and _is_bear_candle(df, i))

def _fib_s2_s3(df, i):
    if i < 20: return False
    swing_hi = df["High"].iloc[i-20:i-5].max()
    swing_lo = df["Low"].iloc[i-20:i-5].min()
    diff = swing_hi - swing_lo
    s2 = swing_lo - diff * 0.618
    s3 = swing_lo - diff * 1.0
    return df["High"].iloc[i] >= s2 * 0.995 and df["Close"].iloc[i] < s2 and df["Low"].iloc[i] > s3

def _three_black_crows(df, i):
    if i < 3: return False
    return all(
        _is_bear_candle(df, j) and df["Close"].iloc[j] < df["Close"].iloc[j-1]
        and df["Open"].iloc[j] < df["Open"].iloc[j-1]
        for j in [i, i-1, i-2]
    )

def _sma20_resistance_hold(df, i):
    if i < 21: return False
    sma20 = _sma(df, "Close", i, 20)
    return (df["High"].iloc[i] >= sma20 * 0.99
            and df["Close"].iloc[i] < sma20
            and _is_bear_candle(df, i))

def _ema_breakdown_bear(df, i):
    if i < 20: return False
    ema20 = _ema(df["Close"].iloc[:i+1], 20).iloc[-1]
    ema_prev = _ema(df["Close"].iloc[:i], 20).iloc[-1]
    return (df["Close"].iloc[i] < ema20
            and df["Close"].iloc[i-1] > ema_prev
            and _is_bear_candle(df, i))

def _inside_bar_breakdown_bear(df, i):
    if i < 2: return False
    mother = i - 2
    inside = i - 1
    is_inside = (df["High"].iloc[inside] < df["High"].iloc[mother]
                 and df["Low"].iloc[inside] > df["Low"].iloc[mother])
    return is_inside and df["Close"].iloc[i] < df["Low"].iloc[mother]

def _bearish_harami(df, i):
    if i < 1: return False
    return (_is_bull_candle(df, i-1)
            and _is_bear_candle(df, i)
            and df["Open"].iloc[i] < df["Close"].iloc[i-1]
            and df["Close"].iloc[i] > df["Open"].iloc[i-1]
            and _body(df, i) < _body(df, i-1) * 0.6)

def _bearish_marubozu(df, i):
    body = _body(df, i)
    rng  = _range_(df, i)
    if rng == 0: return False
    return (_is_bear_candle(df, i)
            and body / rng > 0.85
            and df["Volume"].iloc[i] > _vol_avg(df, i, 10) * 1.2)

def _sma_stack_bear(df, i):
    if i < 50: return False
    sma10 = _sma(df, "Close", i, 10)
    sma20 = _sma(df, "Close", i, 20)
    sma50 = _sma(df, "Close", i, 50)
    return df["Close"].iloc[i] < sma10 < sma20 < sma50

def _rsi_overbought_reversal(df, i):
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
    return prev_rsi > 70 and rsi < 70

def _reversal_vol_spike_bear(df, i):
    if i < 5: return False
    avg_vol = _vol_avg(df, i, 20)
    return (df["Volume"].iloc[i] > avg_vol * 2.5
            and df["High"].iloc[i] > df["High"].iloc[i-1]
            and _is_bear_candle(df, i)
            and df["Close"].iloc[i] < df["Close"].iloc[i-1])

def _close_near_low_bear(df, i):
    rng = _range_(df, i)
    if rng == 0: return False
    return (df["High"].iloc[i] - df["Close"].iloc[i]) / rng > 0.75 and _is_bear_candle(df, i)

def _breakaway_gap_bear(df, i):
    if i < 5: return False
    return (df["Open"].iloc[i] < df["Low"].iloc[i-1] * 0.995
            and _is_bear_candle(df, i)
            and df["Volume"].iloc[i] > _vol_avg(df, i, 10) * 1.5)

def _distribution_topping(df, i):
    if i < 10: return False
    near_hi = df["High"].iloc[i-10:i].max()
    below   = df["Close"].iloc[i] < near_hi * 0.97
    vol_clim = df["Volume"].iloc[i-3:i].mean() > _vol_avg(df, i, 20) * 1.5
    return below and vol_clim and _is_bear_candle(df, i)

def _double_top(df, i):
    if i < 20: return False
    hi1 = df["High"].iloc[i-20:i-10].max()
    hi2 = df["High"].iloc[i-10:i].max()
    neck = df["Low"].iloc[i-15:i-5].min()
    return (abs(hi1 - hi2) / hi1 < 0.02
            and df["Close"].iloc[i] < neck)

def _pivot_breakdown_bear(df, i):
    if i < 3: return False
    pivot = (df["High"].iloc[i-1] + df["Low"].iloc[i-1] + df["Close"].iloc[i-1]) / 3
    s1 = 2 * pivot - df["High"].iloc[i-1]
    return df["Close"].iloc[i] < s1 and _is_bear_candle(df, i)

def _consecutive_bear_bars(df, i):
    if i < 4: return False
    return all(_is_bear_candle(df, j) for j in [i, i-1, i-2])

def _gap_down_continuation_bear(df, i):
    if i < 2: return False
    gap  = df["Open"].iloc[i] < df["Low"].iloc[i-1]
    cont = df["Close"].iloc[i] < df["Open"].iloc[i]
    return gap and cont

def _wick_rejection_high_bear(df, i):
    rng = _range_(df, i)
    if rng == 0: return False
    upper_wick = df["High"].iloc[i] - max(df["Open"].iloc[i], df["Close"].iloc[i])
    return (upper_wick / rng > 0.55
            and _is_bear_candle(df, i)
            and df["Close"].iloc[i] < df["Close"].iloc[i-1])

def _macd_cross_bear(df, i):
    if i < 30: return False
    ema12 = _ema(df["Close"].iloc[:i+1], 12).iloc[-1]
    ema26 = _ema(df["Close"].iloc[:i+1], 26).iloc[-1]
    ema12p = _ema(df["Close"].iloc[:i], 12).iloc[-1]
    ema26p = _ema(df["Close"].iloc[:i], 26).iloc[-1]
    return (ema12p - ema26p) > 0 and (ema12 - ema26) < 0

def _exhaustion_top_bear(df, i):
    if i < 5: return False
    rallied = (df["Close"].iloc[i-1] - df["Close"].iloc[i-5]) / df["Close"].iloc[i-5] > 0.07
    reversal = df["Close"].iloc[i] < df["Close"].iloc[i-1] * 0.98
    return rallied and reversal and df["Volume"].iloc[i] > _vol_avg(df, i, 10) * 1.5

def _high_volume_reversal_bear(df, i):
    if i < 5: return False
    avg_vol = _vol_avg(df, i, 20)
    prev_bull = _is_bull_candle(df, i-1)
    return (prev_bull
            and df["Volume"].iloc[i-1] > avg_vol * 2.5
            and _is_bear_candle(df, i)
            and df["Close"].iloc[i] < df["Low"].iloc[i-1])

def _below_sma50_bear(df, i):
    if i < 51: return False
    sma50 = _sma(df, "Close", i, 50)
    return df["Close"].iloc[i] < sma50 and _is_bear_candle(df, i)

def _sma200_break_bear(df, i):
    if i < 201: return False
    sma200 = _sma(df, "Close", i, 200)
    sma200_prev = _sma(df, "Close", i-1, 200)
    return df["Close"].iloc[i-1] > sma200_prev and df["Close"].iloc[i] < sma200

def _trend_reversal_candle_bear(df, i):
    if i < 3: return False
    prior_up   = df["Close"].iloc[i-3] < df["Close"].iloc[i-2] < df["Close"].iloc[i-1]
    strong_bear = _is_bear_candle(df, i) and df["Close"].iloc[i] < df["Close"].iloc[i-2]
    return prior_up and strong_bear

def _ema_slope_negative(df, i):
    if i < 21: return False
    e20_now  = _ema(df["Close"].iloc[:i+1], 20).iloc[-1]
    e20_prev = _ema(df["Close"].iloc[:i],   20).iloc[-1]
    return e20_now < e20_prev and df["Close"].iloc[i] < e20_now

def _momentum_persistence_bear(df, i):
    if i < 10: return False
    dn_bars = sum(1 for j in range(i-10, i+1) if _is_bear_candle(df, j))
    return dn_bars >= 7 and _is_bear_candle(df, i)

def _vol_dry_up_before_breakdown(df, i):
    if i < 10: return False
    prev_low_vol = df["Volume"].iloc[i-5:i-1].min()
    avg_vol = _vol_avg(df, i, 20)
    return (prev_low_vol < avg_vol * 0.6
            and df["Volume"].iloc[i] > avg_vol * 1.5
            and _is_bear_candle(df, i))

def _wide_range_bar_bear(df, i):
    if i < 10: return False
    avg = sum(_range_(df, j) for j in range(i-10, i)) / 10
    return _range_(df, i) > avg * 2.0 and _is_bear_candle(df, i)

def _lower_open_lower_close(df, i):
    if i < 1: return False
    return (df["Open"].iloc[i] < df["Open"].iloc[i-1]
            and df["Close"].iloc[i] < df["Close"].iloc[i-1])

def _price_action_deceleration_bear(df, i):
    if i < 10: return False
    ret_early = (df["Close"].iloc[i-5] - df["Close"].iloc[i-10]) / df["Close"].iloc[i-10]
    ret_late  = (df["Close"].iloc[i] - df["Close"].iloc[i-5]) / df["Close"].iloc[i-5]
    return ret_late < ret_early and ret_late < -0.02

def _descending_triangle_breakdown(df, i):
    if i < 15: return False
    lows  = df["Low"].iloc[i-15:i]
    support = lows.min()
    highs = df["High"].iloc[i-15:i]
    highs_trend = highs.iloc[-1] < highs.iloc[0]
    return (highs_trend
            and df["Close"].iloc[i] < support
            and df["Volume"].iloc[i] > _vol_avg(df, i, 15) * 1.2)

def _failed_breakout_bear(df, i):
    if i < 5: return False
    prev_hi = df["High"].iloc[i-5:i-1].max()
    breakout = df["High"].iloc[i-1] > prev_hi
    fail = df["Close"].iloc[i] < prev_hi
    return breakout and fail and _is_bear_candle(df, i)

def _vol_spike_rally_sell(df, i):
    if i < 10: return False
    avg_vol = _vol_avg(df, i, 20)
    prev_spike = df["Volume"].iloc[i-3] > avg_vol * 2.0 and _is_bear_candle(df, i-3)
    pullback   = df["Close"].iloc[i-1] > df["Close"].iloc[i-3]
    return prev_spike and pullback and _is_bear_candle(df, i)

def _open_below_prev_low_bear(df, i):
    if i < 1: return False
    return (df["Open"].iloc[i] < df["Low"].iloc[i-1]
            and df["Close"].iloc[i] < df["Open"].iloc[i])

def _candle_size_expansion_bear(df, i):
    if i < 5: return False
    avg = sum(_range_(df, j) for j in range(i-5, i)) / 5
    return _range_(df, i) > avg * 1.6 and _is_bear_candle(df, i)

def _price_action_momentum_bear(df, i):
    if i < 5: return False
    ret5 = (df["Close"].iloc[i] - df["Close"].iloc[i-5]) / df["Close"].iloc[i-5]
    return ret5 < -0.04 and _is_bear_candle(df, i)

def _52w_low_breakdown(df, i):
    n = min(i, 252)
    if n < 50: return False
    lo_52w = df["Low"].iloc[i-n:i].min()
    return (df["Close"].iloc[i] <= lo_52w * 1.02
            and df["Volume"].iloc[i] > _vol_avg(df, i, 20) * 1.2)

def _bear_flag_breakdown(df, i):
    if i < 10: return False
    pole_drop = (df["Close"].iloc[i-10] - df["Close"].iloc[i-5]) / df["Close"].iloc[i-10]
    if pole_drop < 0.05: return False
    consol_hi = df["High"].iloc[i-5:i].max()
    consol_lo = df["Low"].iloc[i-5:i].min()
    consol_range = (consol_hi - consol_lo) / consol_hi
    return consol_range < 0.04 and df["Close"].iloc[i] < consol_lo

def _supply_zone_retest_fail(df, i):
    if i < 15: return False
    old_hi = df["High"].iloc[i-15:i-5].max()
    return (df["High"].iloc[i-2:i].max() >= old_hi * 0.98
            and df["Close"].iloc[i] < old_hi * 0.97
            and _is_bear_candle(df, i))

def _volume_selling_pressure(df, i):
    if i < 10: return False
    dn_vols = [df["Volume"].iloc[j] for j in range(i-9, i+1) if _is_bear_candle(df, j)]
    up_vols  = [df["Volume"].iloc[j] for j in range(i-9, i+1) if _is_bull_candle(df, j)]
    if not dn_vols or not up_vols: return False
    return sum(dn_vols)/len(dn_vols) > sum(up_vols)/len(up_vols) * 1.3

def _close_lower_quartile_series(df, i):
    if i < 3: return False
    def lower_q(j):
        rng = _range_(df, j)
        if rng == 0: return False
        return (df["High"].iloc[j] - df["Close"].iloc[j]) / rng > 0.75
    return all(lower_q(j) for j in [i, i-1, i-2])

def _sma20_slope_bear(df, i):
    if i < 22: return False
    s_now  = _sma(df, "Close", i, 20)
    s_prev = _sma(df, "Close", i-2, 20)
    return s_now < s_prev and df["Close"].iloc[i] < s_now

def _rising_wedge_breakdown(df, i):
    if i < 15: return False
    highs = df["High"].iloc[i-15:i]
    lows  = df["Low"].iloc[i-15:i]
    h_slope = (highs.iloc[-1] - highs.iloc[0]) / 15
    l_slope = (lows.iloc[-1] - lows.iloc[0]) / 15
    return (l_slope > 0 and h_slope > 0
            and l_slope > h_slope
            and df["Close"].iloc[i] < lows.iloc[-1])

def _atr_expansion_bear(df, i):
    if i < 20: return False
    atr_now  = _atr(df, i, 5)
    atr_prev = _atr(df, i-5, 10)
    return atr_now > atr_prev * 1.5 and _is_bear_candle(df, i)

def _multi_bar_resistance_hold(df, i):
    if i < 10: return False
    res = df["High"].iloc[i-10:i-2].max()
    touches = sum(1 for j in range(i-10, i) if df["High"].iloc[j] >= res * 0.99)
    return (touches >= 2
            and df["Close"].iloc[i] < res * 0.98
            and _is_bear_candle(df, i))

def _close_below_open_range_bear(df, i):
    if i < 1: return False
    or_lo = min(df["Open"].iloc[i], df["Low"].iloc[i-1])
    return df["Close"].iloc[i] < or_lo and _is_bear_candle(df, i)

def _bear_outside_day(df, i):
    if i < 1: return False
    return (df["High"].iloc[i] > df["High"].iloc[i-1]
            and df["Low"].iloc[i] < df["Low"].iloc[i-1]
            and _is_bear_candle(df, i))

def _decreasing_buy_pressure(df, i):
    if i < 10: return False
    bull_bodies = [_body(df, j) for j in range(i-9, i+1) if _is_bull_candle(df, j)]
    if len(bull_bodies) < 3: return True
    return bull_bodies[-1] < bull_bodies[0] and _is_bear_candle(df, i)

def _volume_price_confirmation_bear(df, i):
    if i < 3: return False
    price_dn = df["Close"].iloc[i] < df["Close"].iloc[i-1]
    vol_up   = df["Volume"].iloc[i] > df["Volume"].iloc[i-1]
    return price_dn and vol_up and _is_bear_candle(df, i)

def _rising_channel_breakdown(df, i):
    if i < 20: return False
    sma20 = _sma(df, "Close", i, 20)
    below = df["Close"].iloc[i] < sma20 * 0.97
    downtrend_sma = _sma(df, "Close", i-3, 20)
    return below and sma20 < downtrend_sma

def _open_gap_fail_bear(df, i):
    if i < 2: return False
    gap_up = df["Open"].iloc[i-1] > df["High"].iloc[i-2]
    fail   = df["Close"].iloc[i-1] < df["Open"].iloc[i-1]
    cont   = _is_bear_candle(df, i) and df["Close"].iloc[i] < df["Low"].iloc[i-1]
    return gap_up and fail and cont

def _increasing_bear_bodies(df, i):
    if i < 5: return False
    bodies = [_body(df, j) for j in range(i-4, i+1) if _is_bear_candle(df, j)]
    if len(bodies) < 3: return False
    return bodies[-1] > bodies[0]

def _price_below_vwap_bear(df, i):
    if i < 20: return False
    c = df["Close"].iloc[i-20:i+1]; v = df["Volume"].iloc[i-20:i+1]
    vwap = (c * v).sum() / v.sum() if v.sum() > 0 else df["Close"].iloc[i]
    return df["Close"].iloc[i] < vwap and _is_bear_candle(df, i)

def _trendline_break_bear(df, i):
    if i < 10: return False
    hi_series = [df["High"].iloc[i-j] for j in range(10)]
    declining = hi_series[0] < hi_series[-1]
    return declining and df["Close"].iloc[i] < df["Close"].iloc[i-1] * 0.995

def _below_prev_day_midpoint(df, i):
    if i < 1: return False
    mid = (df["High"].iloc[i-1] + df["Low"].iloc[i-1]) / 2
    return df["Close"].iloc[i] < mid and df["Open"].iloc[i] < mid

def _momentum_bear(df, i):
    if i < 5: return False
    return all(_is_bear_candle(df, j) for j in [i, i-1]) and df["Close"].iloc[i] < df["Low"].iloc[i-3]

def _sma50_resistance_bear(df, i):
    if i < 51: return False
    sma50 = _sma(df, "Close", i, 50)
    return (df["High"].iloc[i] >= sma50 * 0.99
            and df["Close"].iloc[i] < sma50
            and _is_bear_candle(df, i))

def _price_action_collapse_bear(df, i):
    if i < 3: return False
    return (df["Close"].iloc[i] < df["Low"].iloc[i-2]
            and _is_bear_candle(df, i)
            and df["Volume"].iloc[i] > _vol_avg(df, i, 10) * 1.4)

def _narrow_spread_distribution(df, i):
    if i < 10: return False
    spreads = [_range_(df, j) / df["Close"].iloc[j] for j in range(i-9, i)]
    return (max(spreads) < 0.02
            and df["Volume"].iloc[i-1] < _vol_avg(df, i, 20) * 0.8
            and _is_bear_candle(df, i))

def _low_atr_coil_bear(df, i):
    if i < 20: return False
    atr_now  = _atr(df, i, 10)
    atr_prev = _atr(df, i-10, 10)
    return atr_now < atr_prev * 0.7 and _is_bear_candle(df, i)

def _thrust_bar_bear(df, i):
    if i < 5: return False
    avg_rng = sum(_range_(df, j) for j in range(i-5, i)) / 5
    return (_range_(df, i) > avg_rng * 1.5
            and _is_bear_candle(df, i)
            and df["Close"].iloc[i] < df["Low"].iloc[i-1])

def _close_lower_half_bear(df, i):
    if i < 1: return False
    mid = (df["High"].iloc[i] + df["Low"].iloc[i]) / 2
    return df["Close"].iloc[i] < mid and _is_bear_candle(df, i)

def _strong_close_low_bear(df, i):
    rng = _range_(df, i)
    if rng == 0: return False
    return (df["High"].iloc[i] - df["Close"].iloc[i]) / rng < 0.1 and _is_bear_candle(df, i)

def _down_day_after_up_vol(df, i):
    if i < 2: return False
    return (df["Volume"].iloc[i-1] > _vol_avg(df, i, 20) * 1.5
            and _is_bull_candle(df, i-1)
            and _is_bear_candle(df, i)
            and df["Close"].iloc[i] < df["Low"].iloc[i-1])

def _swing_high_lower_bear(df, i):
    if i < 10: return False
    local_hi = df["High"].iloc[i-10:i].max()
    prev_hi  = df["High"].iloc[i-20:i-10].max() if i >= 20 else df["High"].iloc[:i-10].max()
    return local_hi < prev_hi and _is_bear_candle(df, i)

def _eod_selling_bear(df, i):
    rng = _range_(df, i)
    if rng == 0: return False
    return ((df["High"].iloc[i] - df["Close"].iloc[i]) / rng > 0.6
            and df["Volume"].iloc[i] > _vol_avg(df, i, 10))

def _cascade_bear(df, i):
    if i < 3: return False
    return all(_is_bear_candle(df, j) and df["Close"].iloc[j] < df["Low"].iloc[j-1]
               for j in [i, i-1])

def _volatility_expansion_bear(df, i):
    if i < 10: return False
    atr5  = _atr(df, i, 5)
    atr20 = _atr(df, i, 20)
    return atr5 > atr20 * 1.4 and _is_bear_candle(df, i)

def _break_of_structure_bear(df, i):
    if i < 10: return False
    swing_lo = df["Low"].iloc[i-10:i-2].min()
    return (df["Close"].iloc[i] < swing_lo
            and _is_bear_candle(df, i))

def _open_lower_close_lower_series(df, i):
    if i < 3: return False
    return all(df["Open"].iloc[j] < df["Open"].iloc[j-1]
               and df["Close"].iloc[j] < df["Close"].iloc[j-1]
               for j in [i, i-1])

def _rejection_at_resistance_bear(df, i):
    if i < 20: return False
    res = df["High"].iloc[i-20:i-3].max()
    return (df["High"].iloc[i-1] >= res * 0.99
            and df["Close"].iloc[i] < res * 0.97
            and df["Close"].iloc[i] < df["Open"].iloc[i-1])

def _downward_channel_continue(df, i):
    if i < 10: return False
    sma10 = _sma(df, "Close", i, 10)
    sma10p = _sma(df, "Close", i-3, 10)
    return sma10 < sma10p and df["Close"].iloc[i] < sma10

def _open_equal_high_bear(df, i):
    rng = _range_(df, i)
    if rng == 0: return False
    return (abs(df["Open"].iloc[i] - df["High"].iloc[i]) / max(df["Close"].iloc[i], 0.001) < 0.002
            and _is_bear_candle(df, i))

def _price_density_below_sma(df, i):
    if i < 21: return False
    sma20 = _sma(df, "Close", i, 20)
    pct_below = sum(1 for j in range(i-20, i+1) if df["Close"].iloc[j] < sma20) / 21
    return pct_below > 0.75 and df["Close"].iloc[i] < sma20

def _bear_gap_no_fill(df, i):
    if i < 3: return False
    gap_dn = df["Open"].iloc[i-1] < df["Close"].iloc[i-2]
    no_fill = df["High"].iloc[i] < df["Close"].iloc[i-2]
    return gap_dn and no_fill and _is_bear_candle(df, i)

def _follow_through_day_bear(df, i):
    if i < 5: return False
    big_bar  = _is_bear_candle(df, i-4) and _range_(df, i-4) > sum(_range_(df,j) for j in range(i-8,i-4))/4
    pullback = df["Close"].iloc[i-1] > df["Close"].iloc[i-4]
    follow   = _is_bear_candle(df, i) and df["Close"].iloc[i] < df["Low"].iloc[i-4]
    return big_bar and pullback and follow


# ── NEUTRAL / RANGE STRATEGIES (100) ─────────────────────────────

def _bollinger_squeeze(df, i):
    if i < 21: return False
    closes = df["Close"].iloc[i-20:i+1]
    sma = closes.mean()
    std = closes.std()
    bb_width = 4 * std / sma
    prev_closes = df["Close"].iloc[i-25:i-5]
    prev_std = prev_closes.std()
    prev_width = 4 * prev_std / prev_closes.mean() if prev_closes.mean() > 0 else 1
    return bb_width < prev_width * 0.7

def _tight_consolidation_neutral(df, i):
    if i < 10: return False
    rng = (df["High"].iloc[i-10:i].max() - df["Low"].iloc[i-10:i].min()) / df["Close"].iloc[i-10]
    return rng < 0.04

def _mean_reversion_oversold(df, i):
    if i < 21: return False
    sma20 = _sma(df, "Close", i, 20)
    dev = (df["Close"].iloc[i] - sma20) / sma20
    return dev < -0.05

def _mean_reversion_overbought(df, i):
    if i < 21: return False
    sma20 = _sma(df, "Close", i, 20)
    dev = (df["Close"].iloc[i] - sma20) / sma20
    return dev > 0.05

def _low_vol_squeeze_neutral(df, i):
    if i < 20: return False
    avg_vol = _vol_avg(df, i, 20)
    recent_vol = df["Volume"].iloc[i-5:i].mean()
    return recent_vol < avg_vol * 0.5

def _range_bound_neutral(df, i):
    if i < 20: return False
    hi = df["High"].iloc[i-20:i].max()
    lo = df["Low"].iloc[i-20:i].min()
    rng_pct = (hi - lo) / lo
    return rng_pct < 0.08

def _coil_spring_neutral(df, i):
    if i < 15: return False
    ranges = [_range_(df, j) for j in range(i-14, i+1)]
    return ranges[-1] < min(ranges[:-1]) and df["Volume"].iloc[i] < _vol_avg(df, i, 20) * 0.6

def _iv_contraction(df, i):
    if i < 10: return False
    ranges = [_range_(df, j) / df["Close"].iloc[j] for j in range(i-9, i+1)]
    return all(ranges[j] <= ranges[j-1] for j in range(1, len(ranges)))

def _price_in_midrange(df, i):
    if i < 20: return False
    hi = df["High"].iloc[i-20:i].max()
    lo = df["Low"].iloc[i-20:i].min()
    mid = (hi + lo) / 2
    return abs(df["Close"].iloc[i] - mid) / mid < 0.02

def _sideways_sma(df, i):
    if i < 22: return False
    sma_vals = [_sma(df, "Close", i-j, 20) for j in range(5)]
    max_dev = max(abs(sma_vals[j] - sma_vals[j+1]) / sma_vals[j+1] for j in range(4))
    return max_dev < 0.01

def _doji_series_neutral(df, i):
    if i < 3: return False
    def is_doji(j):
        rng = _range_(df, j)
        return rng > 0 and _body(df, j) / rng < 0.15
    return sum(1 for j in [i, i-1, i-2] if is_doji(j)) >= 2

def _vol_avg_flat_neutral(df, i):
    if i < 30: return False
    v1 = df["Volume"].iloc[i-30:i-15].mean()
    v2 = df["Volume"].iloc[i-15:i].mean()
    return abs(v1 - v2) / v1 < 0.15

def _price_oscillation_neutral(df, i):
    if i < 10: return False
    signs = [1 if df["Close"].iloc[j] > df["Close"].iloc[j-1] else -1 for j in range(i-8, i+1)]
    flips = sum(1 for j in range(1, len(signs)) if signs[j] != signs[j-1])
    return flips >= 5

def _atr_stable_neutral(df, i):
    if i < 20: return False
    atr1 = _atr(df, i, 5)
    atr2 = _atr(df, i-10, 5)
    return abs(atr1 - atr2) / max(atr2, 0.001) < 0.2

def _flat_base_neutral(df, i):
    if i < 15: return False
    rng = (df["High"].iloc[i-15:i].max() - df["Low"].iloc[i-15:i].min()) / df["Close"].iloc[i-15]
    return rng < 0.06

def _ema_convergence_neutral(df, i):
    if i < 50: return False
    e20 = _ema(df["Close"].iloc[:i+1], 20).iloc[-1]
    e50 = _ema(df["Close"].iloc[:i+1], 50).iloc[-1]
    return abs(e20 - e50) / e50 < 0.01

def _price_near_sma20_neutral(df, i):
    if i < 21: return False
    sma20 = _sma(df, "Close", i, 20)
    return abs(df["Close"].iloc[i] - sma20) / sma20 < 0.01

def _histogram_near_zero_neutral(df, i):
    if i < 30: return False
    ema12 = _ema(df["Close"].iloc[:i+1], 12).iloc[-1]
    ema26 = _ema(df["Close"].iloc[:i+1], 26).iloc[-1]
    hist = abs(ema12 - ema26) / df["Close"].iloc[i]
    return hist < 0.005

def _small_candles_cluster_neutral(df, i):
    if i < 5: return False
    bodies = [_body(df, j) / df["Close"].iloc[j] for j in range(i-4, i+1)]
    return all(b < 0.008 for b in bodies)

def _no_trend_neutral(df, i):
    if i < 20: return False
    ret20 = abs(df["Close"].iloc[i] - df["Close"].iloc[i-20]) / df["Close"].iloc[i-20]
    return ret20 < 0.03

def _compression_before_move(df, i):
    if i < 15: return False
    recent = (df["High"].iloc[i-5:i].max() - df["Low"].iloc[i-5:i].min()) / df["Close"].iloc[i-5]
    prior  = (df["High"].iloc[i-15:i-5].max() - df["Low"].iloc[i-15:i-5].min()) / df["Close"].iloc[i-15]
    return recent < prior * 0.5

def _equal_highs_neutral(df, i):
    if i < 10: return False
    highs = df["High"].iloc[i-10:i]
    return (highs.max() - highs.min()) / highs.min() < 0.015

def _equal_lows_neutral(df, i):
    if i < 10: return False
    lows = df["Low"].iloc[i-10:i]
    return (lows.max() - lows.min()) / lows.min() < 0.015

def _balanced_vol_neutral(df, i):
    if i < 10: return False
    up_v  = [df["Volume"].iloc[j] for j in range(i-9, i+1) if _is_bull_candle(df, j)]
    dn_v  = [df["Volume"].iloc[j] for j in range(i-9, i+1) if _is_bear_candle(df, j)]
    if not up_v or not dn_v: return False
    ratio = sum(up_v)/len(up_v) / (sum(dn_v)/len(dn_v))
    return 0.75 < ratio < 1.33

def _bb_middle_band_walk(df, i):
    if i < 21: return False
    closes = df["Close"].iloc[i-20:i+1]
    sma = closes.mean()
    std = closes.std()
    upper = sma + 2*std
    lower = sma - 2*std
    return lower < df["Close"].iloc[i] < upper and abs(df["Close"].iloc[i] - sma) / sma < 0.02

def _inside_week_neutral(df, i):
    if i < 2: return False
    return (df["High"].iloc[i] < df["High"].iloc[i-1]
            and df["Low"].iloc[i] > df["Low"].iloc[i-1])

def _narrow_daily_range_neutral(df, i):
    rng = _range_(df, i)
    return rng / df["Close"].iloc[i] < 0.01

def _sma_cross_flat_neutral(df, i):
    if i < 21: return False
    s10 = _sma(df, "Close", i, 10)
    s20 = _sma(df, "Close", i, 20)
    return abs(s10 - s20) / s20 < 0.005

def _price_not_trending_neutral(df, i):
    if i < 30: return False
    highs = [df["High"].iloc[i-j*5] for j in range(6)]
    lows  = [df["Low"].iloc[i-j*5] for j in range(6)]
    h_range = (max(highs) - min(highs)) / min(highs)
    l_range = (max(lows) - min(lows)) / min(lows)
    return h_range < 0.05 and l_range < 0.05

def _midpoint_bounce_neutral(df, i):
    if i < 20: return False
    hi = df["High"].iloc[i-20:i].max()
    lo = df["Low"].iloc[i-20:i].min()
    mid = (hi + lo) / 2
    return (df["Low"].iloc[i] <= mid * 1.01
            and df["High"].iloc[i] >= mid * 0.99)

def _volume_below_avg_neutral(df, i):
    if i < 20: return False
    return df["Volume"].iloc[i] < _vol_avg(df, i, 20) * 0.7

def _low_beta_day_neutral(df, i):
    rng = _range_(df, i)
    return rng / df["Close"].iloc[i] < 0.008

def _sma_cluster_neutral(df, i):
    if i < 50: return False
    s20 = _sma(df, "Close", i, 20)
    s50 = _sma(df, "Close", i, 50)
    return abs(s20 - s50) / s50 < 0.015 and abs(df["Close"].iloc[i] - s20) / s20 < 0.02

def _bb_width_low_neutral(df, i):
    if i < 21: return False
    closes = df["Close"].iloc[i-20:i+1]
    std = closes.std()
    mean = closes.mean()
    bb_width = 2 * std / mean
    return bb_width < 0.02

def _high_low_symmetry_neutral(df, i):
    if i < 1: return False
    mid = (df["High"].iloc[i] + df["Low"].iloc[i]) / 2
    return abs(df["Open"].iloc[i] - mid) / mid < 0.01

def _vol_mean_reversion_neutral(df, i):
    if i < 20: return False
    avg_vol = _vol_avg(df, i, 20)
    recent_vol = df["Volume"].iloc[i]
    return 0.6 < recent_vol / avg_vol < 1.4

def _choppy_action_neutral(df, i):
    if i < 5: return False
    net_move = abs(df["Close"].iloc[i] - df["Close"].iloc[i-5]) / df["Close"].iloc[i-5]
    total_move = sum(_range_(df, j) for j in range(i-4, i+1)) / df["Close"].iloc[i-5]
    if total_move == 0: return False
    efficiency = net_move / total_move
    return efficiency < 0.3

def _open_near_prev_close_neutral(df, i):
    if i < 1: return False
    return abs(df["Open"].iloc[i] - df["Close"].iloc[i-1]) / df["Close"].iloc[i-1] < 0.005

def _price_hugging_sma_neutral(df, i):
    if i < 21: return False
    sma20 = _sma(df, "Close", i, 20)
    count = sum(1 for j in range(i-5, i+1)
                if abs(df["Close"].iloc[j] - sma20) / sma20 < 0.015)
    return count >= 4

def _sma_flat_neutral(df, i):
    if i < 22: return False
    vals = [_sma(df, "Close", i-j, 20) for j in range(6)]
    return max(vals) / min(vals) - 1 < 0.01

def _indecision_cluster_neutral(df, i):
    if i < 5: return False
    def is_doji(j):
        rng = _range_(df, j)
        return rng > 0 and _body(df, j) / rng < 0.2
    return sum(1 for j in range(i-4, i+1) if is_doji(j)) >= 3

def _bb_band_hug_neutral(df, i):
    if i < 21: return False
    closes = df["Close"].iloc[i-20:i+1]
    sma = closes.mean()
    std = closes.std()
    upper = sma + 2*std
    lower = sma - 2*std
    near_upper = abs(df["Close"].iloc[i] - upper) / upper < 0.01
    near_lower = abs(df["Close"].iloc[i] - lower) / lower < 0.01
    return near_upper or near_lower

def _range_equilibrium_neutral(df, i):
    if i < 20: return False
    hi = df["High"].iloc[i-20:i].max()
    lo = df["Low"].iloc[i-20:i].min()
    mid = (hi + lo) / 2
    return abs(df["Close"].iloc[i] - mid) / mid < 0.015

def _decreasing_atr_neutral(df, i):
    if i < 20: return False
    a5  = _atr(df, i, 5)
    a10 = _atr(df, i, 10)
    a20 = _atr(df, i, 20)
    return a5 < a10 < a20

def _price_at_midpoint_consolidation(df, i):
    if i < 10: return False
    hi = df["High"].iloc[i-10:i].max()
    lo = df["Low"].iloc[i-10:i].min()
    mid = (hi + lo) / 2
    return abs(df["Close"].iloc[i] - mid) / mid < 0.025

def _no_follow_through_neutral(df, i):
    if i < 3: return False
    big_move = abs(df["Close"].iloc[i-2] - df["Close"].iloc[i-3]) / df["Close"].iloc[i-3] > 0.015
    follow = abs(df["Close"].iloc[i] - df["Close"].iloc[i-1]) / df["Close"].iloc[i-1] < 0.005
    return big_move and follow

def _balanced_open_close_neutral(df, i):
    if i < 5: return False
    diffs = [abs(df["Close"].iloc[j] - df["Open"].iloc[j]) / df["Open"].iloc[j] for j in range(i-4, i+1)]
    return all(d < 0.008 for d in diffs)

def _micro_range_neutral(df, i):
    if i < 3: return False
    hi3 = df["High"].iloc[i-3:i+1].max()
    lo3 = df["Low"].iloc[i-3:i+1].min()
    return (hi3 - lo3) / lo3 < 0.025

def _consolidation_above_support(df, i):
    if i < 20: return False
    support = df["Low"].iloc[i-20:i-5].min()
    consol_lo = df["Low"].iloc[i-5:i].min()
    return consol_lo > support * 1.01

def _vol_neutral_zone(df, i):
    if i < 20: return False
    avg_vol = _vol_avg(df, i, 20)
    return 0.7 < df["Volume"].iloc[i] / avg_vol < 1.3

def _lateral_drift_neutral(df, i):
    if i < 10: return False
    returns = [(df["Close"].iloc[j] - df["Close"].iloc[j-1]) / df["Close"].iloc[j-1] for j in range(i-9, i+1)]
    return all(abs(r) < 0.008 for r in returns)

def _price_stalling_neutral(df, i):
    if i < 5: return False
    recent_gain = (df["Close"].iloc[i] - df["Close"].iloc[i-5]) / df["Close"].iloc[i-5]
    return abs(recent_gain) < 0.01

def _body_decreasing_neutral(df, i):
    if i < 5: return False
    bodies = [_body(df, j) for j in range(i-4, i+1)]
    return bodies[-1] < bodies[0] * 0.5

def _consolidation_below_resistance(df, i):
    if i < 20: return False
    resistance = df["High"].iloc[i-20:i-5].max()
    consol_hi  = df["High"].iloc[i-5:i].max()
    return consol_hi < resistance * 0.99

def _symmetric_triangle_neutral(df, i):
    if i < 15: return False
    highs = df["High"].iloc[i-15:i]
    lows  = df["Low"].iloc[i-15:i]
    h_slope = (highs.iloc[-1] - highs.iloc[0]) / 15
    l_slope = (lows.iloc[-1] - lows.iloc[0]) / 15
    return h_slope < 0 and l_slope > 0

def _price_oscillating_around_sma(df, i):
    if i < 21: return False
    sma20 = _sma(df, "Close", i, 20)
    crosses = sum(
        1 for j in range(i-9, i)
        if (df["Close"].iloc[j] > sma20) != (df["Close"].iloc[j+1] > sma20)
    )
    return crosses >= 3

def _low_momentum_day(df, i):
    if i < 5: return False
    vol_avg = _vol_avg(df, i, 20)
    return df["Volume"].iloc[i] < vol_avg * 0.5 and _range_(df, i) / df["Close"].iloc[i] < 0.01

def _high_wick_low_wick_balance(df, i):
    rng = _range_(df, i)
    if rng == 0: return False
    upper_wick = df["High"].iloc[i] - max(df["Open"].iloc[i], df["Close"].iloc[i])
    lower_wick = min(df["Open"].iloc[i], df["Close"].iloc[i]) - df["Low"].iloc[i]
    return abs(upper_wick - lower_wick) / rng < 0.2

def _price_returning_to_base(df, i):
    if i < 15: return False
    base_hi = df["High"].iloc[i-15:i-5].max()
    base_lo = df["Low"].iloc[i-15:i-5].min()
    base_mid = (base_hi + base_lo) / 2
    return abs(df["Close"].iloc[i] - base_mid) / base_mid < 0.02

def _steady_vol_neutral(df, i):
    if i < 10: return False
    vols = [df["Volume"].iloc[j] for j in range(i-9, i+1)]
    coeff_var = (max(vols) - min(vols)) / (sum(vols)/len(vols))
    return coeff_var < 0.3

def _price_at_weekly_midpoint(df, i):
    if i < 5: return False
    w_hi = df["High"].iloc[i-5:i].max()
    w_lo = df["Low"].iloc[i-5:i].min()
    w_mid = (w_hi + w_lo) / 2
    return abs(df["Close"].iloc[i] - w_mid) / w_mid < 0.015

def _consolidation_volume_dry(df, i):
    if i < 10: return False
    avg_vol = _vol_avg(df, i, 20)
    consol_vol = df["Volume"].iloc[i-5:i].mean()
    return consol_vol < avg_vol * 0.6

def _no_breakout_attempt_neutral(df, i):
    if i < 10: return False
    recent_hi = df["High"].iloc[i-10:i].max()
    return df["High"].iloc[i] < recent_hi * 0.99

def _tight_range_low_vol_neutral(df, i):
    if i < 5: return False
    rng_pct = _range_(df, i) / df["Close"].iloc[i]
    vol_ratio = df["Volume"].iloc[i] / _vol_avg(df, i, 20)
    return rng_pct < 0.012 and vol_ratio < 0.75

def _mean_reversion_setup_neutral(df, i):
    if i < 21: return False
    sma20 = _sma(df, "Close", i, 20)
    dev = (df["Close"].iloc[i] - sma20) / sma20
    return abs(dev) > 0.04

def _price_rejecting_extremes(df, i):
    if i < 5: return False
    period_hi = df["High"].iloc[i-5:i].max()
    period_lo = df["Low"].iloc[i-5:i].min()
    c = df["Close"].iloc[i]
    return (c > period_lo * 1.01) and (c < period_hi * 0.99)

def _overlapping_candles_neutral(df, i):
    if i < 3: return False
    return all(
        df["Low"].iloc[j] < df["High"].iloc[j-1] and df["High"].iloc[j] > df["Low"].iloc[j-1]
        for j in [i, i-1]
    )

def _inside_consolidation_band(df, i):
    if i < 20: return False
    hi20 = df["High"].iloc[i-20:i].max()
    lo20 = df["Low"].iloc[i-20:i].min()
    band_mid = (hi20 + lo20) / 2
    band_quarter = (hi20 - lo20) * 0.25
    return abs(df["Close"].iloc[i] - band_mid) < band_quarter

def _rsi_midzone_neutral(df, i):
    if i < 15: return False
    deltas = df["Close"].diff().iloc[max(0,i-14):i+1]
    gains  = deltas.clip(lower=0).mean()
    losses = (-deltas.clip(upper=0)).mean()
    if losses == 0: return False
    rsi = 100 - 100 / (1 + gains/losses)
    return 40 < rsi < 60

def _volume_compression_neutral(df, i):
    if i < 20: return False
    avg = _vol_avg(df, i, 20)
    recent = df["Volume"].iloc[i-3:i].mean()
    return recent < avg * 0.55

def _price_action_pause_neutral(df, i):
    if i < 10: return False
    trend_10 = (df["Close"].iloc[i-10] - df["Close"].iloc[i-5]) / df["Close"].iloc[i-10]
    recent   = (df["Close"].iloc[i-5] - df["Close"].iloc[i]) / df["Close"].iloc[i-5]
    return abs(trend_10) > 0.03 and abs(recent) < 0.01

def _closing_in_midrange_neutral(df, i):
    rng = _range_(df, i)
    if rng == 0: return False
    pos = (df["Close"].iloc[i] - df["Low"].iloc[i]) / rng
    return 0.35 < pos < 0.65

def _equilibrium_zone_neutral(df, i):
    if i < 30: return False
    hi30 = df["High"].iloc[i-30:i].max()
    lo30 = df["Low"].iloc[i-30:i].min()
    eq_lo = lo30 + (hi30 - lo30) * 0.4
    eq_hi = lo30 + (hi30 - lo30) * 0.6
    return eq_lo <= df["Close"].iloc[i] <= eq_hi

def _small_range_day_series_neutral(df, i):
    if i < 3: return False
    return all(_range_(df, j) / df["Close"].iloc[j] < 0.01 for j in [i, i-1, i-2])

def _trend_exhaustion_neutral(df, i):
    if i < 15: return False
    strong_trend = abs((df["Close"].iloc[i-15] - df["Close"].iloc[i-5]) / df["Close"].iloc[i-15]) > 0.07
    now_flat     = abs((df["Close"].iloc[i-5] - df["Close"].iloc[i]) / df["Close"].iloc[i-5]) < 0.02
    return strong_trend and now_flat

def _low_atr_environment_neutral(df, i):
    if i < 20: return False
    return _atr(df, i, 14) / df["Close"].iloc[i] < 0.015

def _price_stalemate_neutral(df, i):
    if i < 5: return False
    return all(abs(df["Close"].iloc[j] - df["Close"].iloc[j-1]) / df["Close"].iloc[j-1] < 0.006
               for j in range(i-4, i+1))

def _vol_stalemate_neutral(df, i):
    if i < 10: return False
    vols = [df["Volume"].iloc[j] for j in range(i-9, i+1)]
    avg  = sum(vols) / len(vols)
    return all(0.7 * avg < v < 1.3 * avg for v in vols)

def _price_around_ema_neutral(df, i):
    if i < 21: return False
    ema20 = _ema(df["Close"].iloc[:i+1], 20).iloc[-1]
    return abs(df["Close"].iloc[i] - ema20) / ema20 < 0.012

def _inside_day_neutral(df, i):
    if i < 1: return False
    return (df["High"].iloc[i] < df["High"].iloc[i-1]
            and df["Low"].iloc[i] > df["Low"].iloc[i-1])

def _price_band_walk_neutral(df, i):
    if i < 20: return False
    hi = df["High"].iloc[i-20:i].max()
    lo = df["Low"].iloc[i-20:i].min()
    band_width = hi - lo
    lower_zone = lo + band_width * 0.2
    upper_zone = lo + band_width * 0.8
    return lower_zone < df["Close"].iloc[i] < upper_zone

def _consolidation_contraction_neutral(df, i):
    if i < 10: return False
    early_rng = max(_range_(df, i-9), _range_(df, i-8))
    late_rng  = max(_range_(df, i-1), _range_(df, i))
    return late_rng < early_rng * 0.6

def _price_discovery_neutral(df, i):
    if i < 10: return False
    prices_in_zone = sum(1 for j in range(i-10, i+1)
                         if abs(df["Close"].iloc[j] - df["Close"].iloc[i]) / df["Close"].iloc[i] < 0.015)
    return prices_in_zone >= 7

def _bb_squeeze_coil_neutral(df, i):
    if i < 21: return False
    closes = df["Close"].iloc[i-20:i+1]
    sma = closes.mean()
    std = closes.std()
    bb_width_pct = 4 * std / sma
    return bb_width_pct < 0.04


# ── STRATEGY REGISTRIES ───────────────────────────────────────────

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

BEARISH_STRATEGIES = [
    ("Bear engulfing",           _bear_engulfing),
    ("Evening star",             _evening_star),
    ("Shooting star",            _shooting_star),
    ("Head & shoulders",         _head_and_shoulders),
    ("Range breakdown",          _range_breakdown_bear),
    ("Volume surge bear",        _volume_surge_bear),
    ("Death cross",              _death_cross),
    ("Supply zone tap",          _supply_zone_tap),
    ("Lower highs / lows",       _lower_highs_lower_lows),
    ("Dark cloud cover",         _dark_cloud_cover),
    ("Fibonacci S2→S3",          _fib_s2_s3),
    ("Three black crows",        _three_black_crows),
    ("SMA20 resistance",         _sma20_resistance_hold),
    ("EMA breakdown",            _ema_breakdown_bear),
    ("Inside bar breakdown",     _inside_bar_breakdown_bear),
    ("Bearish harami",           _bearish_harami),
    ("Bearish marubozu",         _bearish_marubozu),
    ("SMA stack bear",           _sma_stack_bear),
    ("RSI overbought reversal",  _rsi_overbought_reversal),
    ("Reversal vol spike bear",  _reversal_vol_spike_bear),
    ("Close near low",           _close_near_low_bear),
    ("Breakaway gap down",       _breakaway_gap_bear),
    ("Distribution top",         _distribution_topping),
    ("Double top",               _double_top),
    ("Pivot breakdown",          _pivot_breakdown_bear),
    ("Consecutive bear bars",    _consecutive_bear_bars),
    ("Gap down continue",        _gap_down_continuation_bear),
    ("Wick rejection high",      _wick_rejection_high_bear),
    ("MACD cross bear",          _macd_cross_bear),
    ("Exhaustion top",           _exhaustion_top_bear),
    ("High vol reversal bear",   _high_volume_reversal_bear),
    ("Below SMA50",              _below_sma50_bear),
    ("SMA200 break",             _sma200_break_bear),
    ("Trend reversal bear",      _trend_reversal_candle_bear),
    ("EMA slope negative",       _ema_slope_negative),
    ("Momentum persistence bear",_momentum_persistence_bear),
    ("Vol dry-up breakdown",     _vol_dry_up_before_breakdown),
    ("Wide range bar bear",      _wide_range_bar_bear),
    ("Lower open/close",         _lower_open_lower_close),
    ("PA deceleration bear",     _price_action_deceleration_bear),
    ("Descending triangle",      _descending_triangle_breakdown),
    ("Failed breakout",          _failed_breakout_bear),
    ("Vol spike rally sell",     _vol_spike_rally_sell),
    ("Open below prev low",      _open_below_prev_low_bear),
    ("Candle expansion bear",    _candle_size_expansion_bear),
    ("PA momentum bear",         _price_action_momentum_bear),
    ("52W low breakdown",        _52w_low_breakdown),
    ("Bear flag breakdown",      _bear_flag_breakdown),
    ("Supply zone retest fail",  _supply_zone_retest_fail),
    ("Volume selling pressure",  _volume_selling_pressure),
    ("Lower quartile series",    _close_lower_quartile_series),
    ("SMA20 slope bear",         _sma20_slope_bear),
    ("Rising wedge breakdown",   _rising_wedge_breakdown),
    ("ATR expansion bear",       _atr_expansion_bear),
    ("Resistance cluster hold",  _multi_bar_resistance_hold),
    ("Close below OR",           _close_below_open_range_bear),
    ("Bear outside day",         _bear_outside_day),
    ("Buy pressure decrease",    _decreasing_buy_pressure),
    ("Vol price confirm bear",   _volume_price_confirmation_bear),
    ("Rising channel break",     _rising_channel_breakdown),
    ("Gap open fail bear",       _open_gap_fail_bear),
    ("Increasing bear bodies",   _increasing_bear_bodies),
    ("Price below VWAP",         _price_below_vwap_bear),
    ("Trendline break bear",     _trendline_break_bear),
    ("Below prev mid bear",      _below_prev_day_midpoint),
    ("Momentum bear",            _momentum_bear),
    ("SMA50 resistance",         _sma50_resistance_bear),
    ("PA collapse bear",         _price_action_collapse_bear),
    ("Narrow spread distrib",    _narrow_spread_distribution),
    ("Low ATR coil bear",        _low_atr_coil_bear),
    ("Thrust bar bear",          _thrust_bar_bear),
    ("Close lower half",         _close_lower_half_bear),
    ("Strong close low",         _strong_close_low_bear),
    ("Down day after up vol",    _down_day_after_up_vol),
    ("Swing high lower",         _swing_high_lower_bear),
    ("EOD selling",              _eod_selling_bear),
    ("Cascade bear",             _cascade_bear),
    ("Volatility expansion",     _volatility_expansion_bear),
    ("Break of structure",       _break_of_structure_bear),
    ("Open low close low series",_open_lower_close_lower_series),
    ("Rejection at resistance",  _rejection_at_resistance_bear),
    ("Downward channel",         _downward_channel_continue),
    ("Open equals high",         _open_equal_high_bear),
    ("Price density below SMA",  _price_density_below_sma),
    ("Bear gap no fill",         _bear_gap_no_fill),
    ("Follow through day bear",  _follow_through_day_bear),
    ("Bear RS sector",           lambda df, i: not _sector_relative_strength(df, i) if i >= 5 else False),
    ("SMA stack bear 2",         lambda df, i: _sma_stack_bear(df, i)),
    ("MACD negative hist",       lambda df, i: (
        i >= 30 and
        _ema(df["Close"].iloc[:i+1], 12).iloc[-1] - _ema(df["Close"].iloc[:i+1], 26).iloc[-1] < 0
    )),
    ("Consecutive new lows",     lambda df, i: (
        i >= 4 and
        all(df["Low"].iloc[j] < df["Low"].iloc[j-1] for j in [i, i-1, i-2])
    )),
    ("Price below ema ribbon",   lambda df, i: (
        i >= 30 and
        df["Close"].iloc[i] <
        _ema(df["Close"].iloc[:i+1], 10).iloc[-1] and
        _ema(df["Close"].iloc[:i+1], 10).iloc[-1] <
        _ema(df["Close"].iloc[:i+1], 20).iloc[-1] and
        _ema(df["Close"].iloc[:i+1], 20).iloc[-1] <
        _ema(df["Close"].iloc[:i+1], 30).iloc[-1]
    )),
    ("Negative PA acceleration", lambda df, i: (
        i >= 10 and
        (df["Close"].iloc[i] - df["Close"].iloc[i-5]) / df["Close"].iloc[i-5] <
        (df["Close"].iloc[i-5] - df["Close"].iloc[i-10]) / df["Close"].iloc[i-10] and
        (df["Close"].iloc[i] - df["Close"].iloc[i-5]) / df["Close"].iloc[i-5] < -0.015
    )),
    ("Reversal after spike bear",lambda df, i: (
        i >= 5 and
        _vol_avg(df, i, 20) > 0 and
        df["Volume"].iloc[i-1] > _vol_avg(df, i, 20) * 2.0 and
        _is_bull_candle(df, i-1) and
        _is_bear_candle(df, i)
    )),
    ("Wicks shortening bear",    lambda df, i: (
        i >= 5 and
        (df["Low"].iloc[i] - df["High"].iloc[i] + max(df["Open"].iloc[i], df["Close"].iloc[i])) <
        (df["Low"].iloc[i-2] - df["High"].iloc[i-2] + max(df["Open"].iloc[i-2], df["Close"].iloc[i-2]))
        and _is_bear_candle(df, i)
    )),
    ("Close lowest in 5 bars",   lambda df, i: (
        i >= 5 and
        df["Close"].iloc[i] == min(df["Close"].iloc[i-5:i+1].tolist())
    )),
    ("Gap cluster breakdown",    lambda df, i: (
        i >= 5 and
        df["Open"].iloc[i] < df["Close"].iloc[i-1] and
        df["Close"].iloc[i] < df["Open"].iloc[i] and
        df["Volume"].iloc[i] > _vol_avg(df, i, 10) * 1.2
    )),
    ("High open weak close",     lambda df, i: (
        i >= 1 and
        df["Open"].iloc[i] > df["High"].iloc[i-1] * 0.99 and
        df["Close"].iloc[i] < df["Low"].iloc[i-1]
    )),
]

NEUTRAL_STRATEGIES = [
    ("Bollinger squeeze",        _bollinger_squeeze),
    ("Tight consolidation",      _tight_consolidation_neutral),
    ("Mean rev oversold",        _mean_reversion_oversold),
    ("Mean rev overbought",      _mean_reversion_overbought),
    ("Low vol squeeze",          _low_vol_squeeze_neutral),
    ("Range bound",              _range_bound_neutral),
    ("Coil spring",              _coil_spring_neutral),
    ("IV contraction",           _iv_contraction),
    ("Price in midrange",        _price_in_midrange),
    ("Sideways SMA",             _sideways_sma),
    ("Doji series",              _doji_series_neutral),
    ("Vol avg flat",             _vol_avg_flat_neutral),
    ("Price oscillation",        _price_oscillation_neutral),
    ("ATR stable",               _atr_stable_neutral),
    ("Flat base",                _flat_base_neutral),
    ("EMA convergence",          _ema_convergence_neutral),
    ("Price near SMA20",         _price_near_sma20_neutral),
    ("MACD histogram near zero", _histogram_near_zero_neutral),
    ("Small candles cluster",    _small_candles_cluster_neutral),
    ("No trend",                 _no_trend_neutral),
    ("Compression before move",  _compression_before_move),
    ("Equal highs",              _equal_highs_neutral),
    ("Equal lows",               _equal_lows_neutral),
    ("Balanced volume",          _balanced_vol_neutral),
    ("BB middle band walk",      _bb_middle_band_walk),
    ("Inside week",              _inside_week_neutral),
    ("Narrow daily range",       _narrow_daily_range_neutral),
    ("SMA cross flat",           _sma_cross_flat_neutral),
    ("Price not trending",       _price_not_trending_neutral),
    ("Midpoint bounce",          _midpoint_bounce_neutral),
    ("Volume below avg",         _volume_below_avg_neutral),
    ("Low beta day",             _low_beta_day_neutral),
    ("SMA cluster",              _sma_cluster_neutral),
    ("BB width low",             _bb_width_low_neutral),
    ("High/low symmetry",        _high_low_symmetry_neutral),
    ("Vol mean reversion",       _vol_mean_reversion_neutral),
    ("Choppy action",            _choppy_action_neutral),
    ("Open near prev close",     _open_near_prev_close_neutral),
    ("Price hugging SMA",        _price_hugging_sma_neutral),
    ("SMA flat",                 _sma_flat_neutral),
    ("Indecision cluster",       _indecision_cluster_neutral),
    ("BB band hug",              _bb_band_hug_neutral),
    ("Range equilibrium",        _range_equilibrium_neutral),
    ("Decreasing ATR",           _decreasing_atr_neutral),
    ("Price at midpoint consol", _price_at_midpoint_consolidation),
    ("No follow-through",        _no_follow_through_neutral),
    ("Balanced open/close",      _balanced_open_close_neutral),
    ("Micro range",              _micro_range_neutral),
    ("Consolidation above supp", _consolidation_above_support),
    ("Vol neutral zone",         _vol_neutral_zone),
    ("Lateral drift",            _lateral_drift_neutral),
    ("Price stalling",           _price_stalling_neutral),
    ("Body decreasing",          _body_decreasing_neutral),
    ("Consol below resistance",  _consolidation_below_resistance),
    ("Symmetric triangle",       _symmetric_triangle_neutral),
    ("Price oscillates SMA",     _price_oscillating_around_sma),
    ("Low momentum day",         _low_momentum_day),
    ("Wick balance",             _high_wick_low_wick_balance),
    ("Returning to base",        _price_returning_to_base),
    ("Steady volume",            _steady_vol_neutral),
    ("Price at weekly mid",      _price_at_weekly_midpoint),
    ("Consolidation vol dry",    _consolidation_volume_dry),
    ("No breakout attempt",      _no_breakout_attempt_neutral),
    ("Tight range low vol",      _tight_range_low_vol_neutral),
    ("Mean rev setup",           _mean_reversion_setup_neutral),
    ("Price rejecting extremes", _price_rejecting_extremes),
    ("Overlapping candles",      _overlapping_candles_neutral),
    ("Inside consolidation band",_inside_consolidation_band),
    ("RSI midzone",              _rsi_midzone_neutral),
    ("Volume compression",       _volume_compression_neutral),
    ("PA pause",                 _price_action_pause_neutral),
    ("Closing in midrange",      _closing_in_midrange_neutral),
    ("Equilibrium zone",         _equilibrium_zone_neutral),
    ("Small range day series",   _small_range_day_series_neutral),
    ("Trend exhaustion",         _trend_exhaustion_neutral),
    ("Low ATR environment",      _low_atr_environment_neutral),
    ("Price stalemate",          _price_stalemate_neutral),
    ("Vol stalemate",            _vol_stalemate_neutral),
    ("Price around EMA",         _price_around_ema_neutral),
    ("Inside day",               _inside_day_neutral),
    ("Price band walk",          _price_band_walk_neutral),
    ("Contraction consolidation",_consolidation_contraction_neutral),
    ("Price discovery",          _price_discovery_neutral),
    ("BB squeeze coil",          _bb_squeeze_coil_neutral),
    ("Resting in zone",          lambda df, i: _range_equilibrium_neutral(df, i) and _vol_mean_reversion_neutral(df, i)),
    ("Compression and wait",     lambda df, i: _compression_before_move(df, i) and _volume_below_avg_neutral(df, i)),
    ("Double inside day",        lambda df, i: i >= 2 and _inside_day_neutral(df, i) and _inside_day_neutral(df, i-1)),
    ("Flat vol flat price",      lambda df, i: _lateral_drift_neutral(df, i) and _vol_stalemate_neutral(df, i)),
    ("NR4 bar",                  lambda df, i: (
        i >= 4 and
        _range_(df, i) == min(_range_(df, j) for j in range(i-3, i+1))
    )),
    ("Inside day low vol",       lambda df, i: _inside_day_neutral(df, i) and _volume_below_avg_neutral(df, i)),
    ("Two-bar reversal neutral", lambda df, i: (
        i >= 2 and
        abs(df["Close"].iloc[i] - df["Open"].iloc[i-1]) / df["Open"].iloc[i-1] < 0.005
    )),
    ("Doji after trend",         lambda df, i: (
        i >= 5 and
        abs((df["Close"].iloc[i-5] - df["Close"].iloc[i-1]) / df["Close"].iloc[i-5]) > 0.04 and
        _body(df, i) / max(_range_(df, i), 0.001) < 0.15
    )),
    ("Consolidation mid-trend",  lambda df, i: (
        i >= 15 and
        abs((df["Close"].iloc[i] - df["Close"].iloc[i-5]) / df["Close"].iloc[i-5]) < 0.015 and
        abs((df["Close"].iloc[i-10] - df["Close"].iloc[i-15]) / df["Close"].iloc[i-15]) > 0.04
    )),
    ("Price platform neutral",   lambda df, i: (
        i >= 8 and
        (df["High"].iloc[i-8:i].max() - df["Low"].iloc[i-8:i].min()) / df["Close"].iloc[i-8] < 0.035
    )),
    ("Spine bar neutral",        lambda df, i: (
        i >= 1 and
        _range_(df, i) / df["Close"].iloc[i] < 0.005
    )),
    ("Vol below 20d avg x2",     lambda df, i: (
        i >= 20 and df["Volume"].iloc[i] < _vol_avg(df, i, 20) * 0.5
    )),
]

STRATEGY_MAP = {
    "bullish":  BULLISH_STRATEGIES,
    "bearish":  BEARISH_STRATEGIES,
    "neutral":  NEUTRAL_STRATEGIES,
}


# ══════════════════════════════════════════════════════════════════
# DATA FETCHING
# ══════════════════════════════════════════════════════════════════

def _sym(ticker: str, market: str) -> str:
    if market.upper() == "NSE" and not ticker.endswith(".NS"):
        return ticker + ".NS"
    return ticker.upper()


def fetch_data(ticker: str, market: str, timeframe: str, years: int) -> Optional["pd.DataFrame"]:
    if not PANDAS_AVAILABLE or not YF_AVAILABLE:
        raise ImportError("pip install yfinance pandas numpy")

    sym = _sym(ticker, market)
    end = datetime.today()
    start = end - timedelta(days=years * 365 + 90)

    interval = TIMEFRAME_INTERVAL.get(timeframe, "1d")

    # yfinance limits intraday history
    if interval in ("30m", "1h"):
        start = end - timedelta(days=59)

    try:
        raw = yf.download(sym, start=start, end=end, interval=interval,
                          auto_adjust=True, progress=False, multi_level_index=False)
    except Exception as e:
        return None

    if raw is None or len(raw) < 20:
        return None

    raw = raw.reset_index()
    raw.columns = [str(c).strip().title() for c in raw.columns]

    col_map = {}
    for want in ["Open", "High", "Low", "Close", "Volume"]:
        for c in raw.columns:
            if c.lower() == want.lower():
                col_map[c] = want
    raw = raw.rename(columns=col_map)

    date_col = "Datetime" if "Datetime" in raw.columns else "Date"
    keep = [date_col, "Open", "High", "Low", "Close", "Volume"]
    keep = [c for c in keep if c in raw.columns]
    raw = raw[keep].dropna().reset_index(drop=True)
    raw = raw.rename(columns={date_col: "Date"})

    # Resample 1H → 4H
    if timeframe == "4H":
        raw["Date"] = pd.to_datetime(raw["Date"])
        raw = raw.set_index("Date")
        raw = raw.resample("4h").agg({
            "Open": "first", "High": "max",
            "Low": "min", "Close": "last", "Volume": "sum"
        }).dropna().reset_index()

    raw["Date"] = pd.to_datetime(raw["Date"])
    raw = raw.sort_values("Date").reset_index(drop=True)
    return raw


def fetch_live_price(ticker: str, market: str) -> Optional[float]:
    if not YF_AVAILABLE:
        return None
    try:
        t = yf.Ticker(_sym(ticker, market))
        p = t.fast_info.get("last_price") or t.fast_info.get("lastPrice")
        if p:
            return float(p)
        info = t.info or {}
        return float(info.get("currentPrice") or info.get("regularMarketPrice") or 0) or None
    except Exception:
        return None


def fetch_regime(market: str) -> Tuple[str, float]:
    try:
        ticker = REGIME_TICKER_NSE if market.upper() == "NSE" else REGIME_TICKER_US
        raw = yf.download(ticker, period="6mo", interval="1d",
                          auto_adjust=True, progress=False, multi_level_index=False)
        if raw is None or len(raw) < 50:
            return "UNKNOWN", 50.0
        closes = raw["Close"].values
        sma50  = closes[-50:].mean()
        sma200 = closes[-200:].mean() if len(closes) >= 200 else closes.mean()
        price  = closes[-1]
        ret20  = (price - closes[-20]) / closes[-20] * 100 if len(closes) >= 20 else 0
        score = 50.0
        if price > sma50:  score += 15
        else:              score -= 15
        if price > sma200: score += 10
        else:              score -= 10
        if ret20 > 3:      score += 10
        elif ret20 < -3:   score -= 10
        score = max(0, min(100, score))
        regime = "BULL" if score >= 60 else "BEAR" if score <= 40 else "NEUTRAL"
        return regime, score
    except Exception:
        return "UNKNOWN", 50.0


# ══════════════════════════════════════════════════════════════════
# BOT 1 — FINGERPRINT BUILDER
# ══════════════════════════════════════════════════════════════════

@dataclass
class StrategyRecord:
    name:           str
    appearances:    int   = 0
    wins:           int   = 0
    win_rate:       float = 0.0
    avg_forward:    float = 0.0
    qualifies:      bool  = False   # win_rate >= STRATEGY_THRESHOLD and appearances >= MIN_INSTANCES


def build_fingerprint(df: "pd.DataFrame", bias: str, forward_days: int = FORWARD_DAYS) -> List[StrategyRecord]:
    """
    Bot 1 — For each strategy, scan all historical bars and compute:
      - how many times the pattern appeared
      - how often the forward outcome was positive (win = forward close > entry)
      - avg forward return
    Then mark which strategies qualify (win_rate >= threshold AND appearances >= min)
    """
    strategies = STRATEGY_MAP.get(bias, BULLISH_STRATEGIES)
    records = []
    n = len(df)

    for name, fn in strategies:
        appearances = 0
        wins = 0
        fwd_returns = []

        for i in range(1, n - forward_days - 1):
            try:
                fired = fn(df, i)
            except Exception:
                fired = False

            if not fired:
                continue

            appearances += 1
            entry_price = df["Close"].iloc[i]
            future_price = df["Close"].iloc[i + forward_days]

            if bias == "bullish":
                fwd_ret = (future_price - entry_price) / entry_price * 100
                win = fwd_ret > 0
            elif bias == "bearish":
                fwd_ret = (entry_price - future_price) / entry_price * 100
                win = fwd_ret > 0
            else:  # neutral — win = price stays within ±3% range
                fwd_ret = abs((future_price - entry_price) / entry_price * 100)
                win = fwd_ret < 3.0
                fwd_ret = -fwd_ret if (future_price - entry_price) / entry_price < 0 else fwd_ret

            if win:
                wins += 1
            fwd_returns.append(fwd_ret)

        win_rate  = wins / appearances if appearances > 0 else 0.0
        avg_fwd   = sum(fwd_returns) / len(fwd_returns) if fwd_returns else 0.0
        qualifies = (win_rate >= STRATEGY_THRESHOLD and appearances >= MIN_INSTANCES)

        records.append(StrategyRecord(
            name=name,
            appearances=appearances,
            wins=wins,
            win_rate=win_rate,
            avg_forward=avg_fwd,
            qualifies=qualifies,
        ))

    records.sort(key=lambda r: r.win_rate, reverse=True)
    return records


# ══════════════════════════════════════════════════════════════════
# BOT 2 — LIVE SCORER
# ══════════════════════════════════════════════════════════════════

@dataclass
class LiveScore:
    qualified_total:    int   = 0   # strategies that passed fingerprint threshold
    firing_count:       int   = 0   # of those, how many are active right now
    score_pct:          float = 0.0 # firing / qualified * 100
    firing_strategies:  List[StrategyRecord] = field(default_factory=list)
    top_strategy:       Optional[StrategyRecord] = None
    score_label:        str   = ""  # STRONG / DEVELOPING / NO SIGNAL


def score_live(df: "pd.DataFrame", fingerprint: List[StrategyRecord],
               bias: str) -> LiveScore:
    """
    Bot 2 — At the latest bar, check which qualified strategies are firing.
    Score = firing / qualified (stock-specific denominator).
    """
    strategies   = STRATEGY_MAP.get(bias, BULLISH_STRATEGIES)
    strat_fn_map = {name: fn for name, fn in strategies}

    qualified    = [r for r in fingerprint if r.qualifies]
    qualified_total = len(qualified)
    if qualified_total == 0:
        return LiveScore(score_label="NO SIGNAL")

    i   = len(df) - 1
    firing = []

    for record in qualified:
        fn = strat_fn_map.get(record.name)
        if fn is None:
            continue
        try:
            if fn(df, i):
                firing.append(record)
        except Exception:
            pass

    score_pct = len(firing) / qualified_total * 100
    label = ("STRONG" if score_pct >= 60
             else "DEVELOPING" if score_pct >= 35
             else "NO SIGNAL")

    top = max(firing, key=lambda r: r.win_rate) if firing else None

    return LiveScore(
        qualified_total=qualified_total,
        firing_count=len(firing),
        score_pct=score_pct,
        firing_strategies=firing,
        top_strategy=top,
        score_label=label,
    )


# ══════════════════════════════════════════════════════════════════
# BOT 3 — HISTORICAL OUTCOMES
# ══════════════════════════════════════════════════════════════════

@dataclass
class OutcomeStats:
    strategy_name:  str   = ""
    appearances:    int   = 0
    win_rate:       float = 0.0
    avg_fwd:        float = 0.0
    best_case:      float = 0.0
    worst_case:     float = 0.0
    median_fwd:     float = 0.0


def compute_outcomes(df: "pd.DataFrame", firing: List[StrategyRecord],
                     bias: str, forward_days: int = FORWARD_DAYS) -> List[OutcomeStats]:
    """
    Bot 3 — For each firing strategy, collect all historical forward returns
    and compute distribution stats.
    """
    strategies = STRATEGY_MAP.get(bias, BULLISH_STRATEGIES)
    strat_fn_map = {name: fn for name, fn in strategies}
    n = len(df)
    results = []

    for record in firing[:5]:  # top 5 firing strategies
        fn = strat_fn_map.get(record.name)
        if fn is None:
            continue

        fwd_rets = []
        for i in range(1, n - forward_days - 1):
            try:
                if not fn(df, i):
                    continue
            except Exception:
                continue

            entry = df["Close"].iloc[i]
            fut   = df["Close"].iloc[i + forward_days]

            if bias == "bullish":
                fwd_ret = (fut - entry) / entry * 100
            elif bias == "bearish":
                fwd_ret = (entry - fut) / entry * 100
            else:
                fwd_ret = (fut - entry) / entry * 100

            fwd_rets.append(fwd_ret)

        if not fwd_rets:
            continue

        fwd_rets_sorted = sorted(fwd_rets)
        mid = len(fwd_rets_sorted) // 2
        median = fwd_rets_sorted[mid]

        results.append(OutcomeStats(
            strategy_name=record.name,
            appearances=len(fwd_rets),
            win_rate=record.win_rate,
            avg_fwd=sum(fwd_rets) / len(fwd_rets),
            best_case=max(fwd_rets),
            worst_case=min(fwd_rets),
            median_fwd=median,
        ))

    return results


# ══════════════════════════════════════════════════════════════════
# BOT 4 — MARKET REGIME FILTER
# ══════════════════════════════════════════════════════════════════

@dataclass
class RegimeContext:
    regime:         str   = "UNKNOWN"
    regime_score:   float = 50.0
    bias_aligned:   bool  = True
    sector:         str   = ""
    alignment_note: str   = ""


def check_regime(market: str, bias: str, ticker: str) -> RegimeContext:
    regime, score = fetch_regime(market)
    sector = SECTOR_MAP_NSE.get(ticker.upper().replace(".NS", ""), "")

    aligned = True
    note    = ""

    if bias == "bullish" and regime == "BEAR":
        aligned = False
        note = f"Market is in a BEAR regime (score {score:.0f}/100). Bullish setups carry higher failure risk. Consider waiting for regime improvement."
    elif bias == "bearish" and regime == "BULL":
        aligned = False
        note = f"Market is in a BULL regime (score {score:.0f}/100). Bearish setups carry higher failure risk against the trend."
    elif regime == "NEUTRAL":
        note = f"Market regime is neutral (score {score:.0f}/100). No strong tailwind or headwind."
    else:
        note = f"Market regime ({regime}, score {score:.0f}/100) is aligned with your {bias} bias."

    return RegimeContext(
        regime=regime,
        regime_score=score,
        bias_aligned=aligned,
        sector=sector,
        alignment_note=note,
    )


# ══════════════════════════════════════════════════════════════════
# BOT 5 — RISK CALCULATOR
# ══════════════════════════════════════════════════════════════════

@dataclass
class RiskOutput:
    live_price:     float = 0.0
    capital:        float = 0.0
    max_risk_pct:   float = 2.0
    max_risk_amt:   float = 0.0
    kelly_fraction: float = 0.0
    position_size:  float = 0.0   # capital to deploy
    shares:         int   = 0
    stop_loss:      float = 0.0
    stop_pct:       float = 0.0
    target_1:       float = 0.0
    target_2:       float = 0.0
    risk_reward:    float = 0.0
    currency:       str   = "₹"


def calculate_risk(live_price: float, capital: float, max_risk_pct: float,
                   score: LiveScore, market: str, df: "pd.DataFrame",
                   bias: str = "bullish") -> RiskOutput:
    cur = "₹" if market.upper() == "NSE" else "$"

    if live_price <= 0:
        return RiskOutput(currency=cur)

    # ATR-based stop distance
    atr = _atr(df, len(df)-1, 14) if len(df) > 14 else live_price * 0.02
    stop_pct = min(max(atr / live_price * 100 * 1.5, 1.5), 8.0)

    # Direction-aware stop and targets
    if bias == "bearish":
        stop_loss = live_price * (1 + stop_pct / 100)   # stop ABOVE entry for shorts
        target_1  = live_price * (1 - stop_pct * 1.5 / 100)  # targets BELOW entry
        target_2  = live_price * (1 - stop_pct * 3.0 / 100)
    else:
        stop_loss = live_price * (1 - stop_pct / 100)   # stop BELOW entry for longs
        target_1  = live_price * (1 + stop_pct * 1.5 / 100)
        target_2  = live_price * (1 + stop_pct * 3.0 / 100)

    # Kelly sizing
    win_rate = score.top_strategy.win_rate if score.top_strategy else 0.5
    avg_win  = abs(score.top_strategy.avg_forward / 100) if score.top_strategy else 0.05
    avg_loss = stop_pct / 100

    if avg_loss > 0:
        kelly = (win_rate - (1 - win_rate) * (avg_loss / max(avg_win, 0.001)))
    else:
        kelly = 0.0

    half_kelly    = max(kelly * 0.5, 0.02)
    position_size = capital * half_kelly
    position_size = min(position_size, capital * max_risk_pct / stop_pct * 100 / 100)

    shares = int(position_size / live_price)
    t1_pct = stop_pct * 1.5
    rr     = t1_pct / stop_pct if stop_pct > 0 else 0

    return RiskOutput(
        live_price=live_price,
        capital=capital,
        max_risk_pct=max_risk_pct,
        max_risk_amt=capital * max_risk_pct / 100,
        kelly_fraction=half_kelly,
        position_size=position_size,
        shares=shares,
        stop_loss=stop_loss,
        stop_pct=stop_pct,
        target_1=target_1,
        target_2=target_2,
        risk_reward=rr,
        currency=cur,
    )


# ══════════════════════════════════════════════════════════════════
# BOT 6 — NARRATIVE WRITER
# ══════════════════════════════════════════════════════════════════

def write_narrative(ticker: str, market: str, bias: str, timeframe: str,
                    fingerprint: List[StrategyRecord],
                    live_score: LiveScore,
                    outcomes: List[OutcomeStats],
                    regime: RegimeContext,
                    risk: RiskOutput) -> str:

    qualified = [r for r in fingerprint if r.qualifies]
    q_count   = len(qualified)
    cur       = risk.currency
    score_str = f"{live_score.firing_count}/{q_count}"

    lines = []
    lines.append("=" * W)
    lines.append(f"  KANIDA.AI — CUSTOM AGENT  ·  {ticker.upper()}  ·  {bias.upper()}  ·  {timeframe}")
    lines.append(f"  {TODAY.strftime('%d %b %Y %H:%M')}  ·  Market: {market.upper()}")
    lines.append("=" * W)

    # ── Fingerprint summary
    lines.append(f"\n  STRATEGY FINGERPRINT — {ticker.upper()}")
    lines.append(f"  {'─' * (W-2)}")
    lines.append(f"  Of 100 {bias} strategies tested against {ticker.upper()}'s full price history:")
    lines.append(f"  {q_count} qualified (win rate ≥ {int(STRATEGY_THRESHOLD*100)}% with ≥ {MIN_INSTANCES} historical appearances).")
    if q_count == 0:
        lines.append(f"  No strategies have a statistically reliable edge on {ticker.upper()} for {bias} setups.")
        lines.append(f"  This does not mean the stock won't move — it means history cannot validate a pattern.")
        return "\n".join(lines)

    top5 = [r for r in qualified[:5]]
    lines.append(f"\n  Top {len(top5)} historically validated {bias} strategies for {ticker.upper()}:")
    for r in top5:
        bar = "█" * int(r.win_rate * 20) + "░" * (20 - int(r.win_rate * 20))
        lines.append(f"    {r.name:<30} [{bar}] {r.win_rate*100:.0f}%  ({r.appearances} historical setups)")

    # ── Live score
    lines.append(f"\n  LIVE SIGNAL SCORE — {TODAY.strftime('%d %b %Y')}")
    lines.append(f"  {'─' * (W-2)}")
    lines.append(f"  {score_str} of {ticker.upper()}'s qualified {bias} strategies are firing right now.")

    label_map = {
        "STRONG":     f"  Signal strength: STRONG — {live_score.firing_count} of {q_count} validated setups simultaneously active.",
        "DEVELOPING": f"  Signal strength: DEVELOPING — some setups aligning but not yet a full-stack signal.",
        "NO SIGNAL":  f"  Signal strength: NO SIGNAL — fewer than 35% of validated setups are active today.",
    }
    lines.append(label_map.get(live_score.score_label, ""))

    if live_score.firing_strategies:
        lines.append(f"\n  Currently active strategies:")
        for r in live_score.firing_strategies[:6]:
            lines.append(f"    • {r.name:<32} {r.win_rate*100:.0f}% historical win rate  ({r.appearances} instances)")

    # ── Historical outcomes
    if outcomes:
        lines.append(f"\n  HISTORICAL OUTCOME DISTRIBUTION — top firing strategies")
        lines.append(f"  {'─' * (W-2)}")
        lines.append(f"  Forward window: {FORWARD_DAYS} trading days from signal")
        lines.append("")
        for o in outcomes:
            lines.append(f"  {o.strategy_name}")
            lines.append(f"    Win rate: {o.win_rate*100:.0f}%  ·  Median: {o.median_fwd:+.1f}%  ·  "
                         f"Avg: {o.avg_fwd:+.1f}%  ·  Best: {o.best_case:+.1f}%  ·  Worst: {o.worst_case:+.1f}%")
            lines.append(f"    Historical appearances: {o.appearances}")

    # ── Regime
    lines.append(f"\n  MARKET REGIME")
    lines.append(f"  {'─' * (W-2)}")
    lines.append(f"  {regime.alignment_note}")
    if regime.sector:
        lines.append(f"  Sector: {regime.sector}")

    # ── Risk
    if risk.live_price > 0:
        is_short   = (bias == "bearish")
        stop_dir   = f"+{risk.stop_pct:.1f}%" if is_short else f"−{risk.stop_pct:.1f}%"
        t1_dir     = f"−{risk.stop_pct*1.5:.1f}%" if is_short else f"+{risk.stop_pct*1.5:.1f}%"
        t2_dir     = f"−{risk.stop_pct*3.0:.1f}%" if is_short else f"+{risk.stop_pct*3.0:.1f}%"
        trade_type = "Short (sell/put)" if is_short else "Long (buy/call)"
        lines.append(f"\n  RISK & POSITION  ·  {trade_type}")
        lines.append(f"  {'─' * (W-2)}")
        lines.append(f"  Live price:     {cur}{risk.live_price:,.2f}")
        lines.append(f"  Capital:        {cur}{risk.capital:,.0f}   ·   Max risk: {cur}{risk.max_risk_amt:,.0f} ({risk.max_risk_pct:.0f}%)")
        lines.append(f"  Position size:  {cur}{risk.position_size:,.0f}  ({risk.kelly_fraction*100:.1f}% half-Kelly)   ·   {risk.shares} shares/units")
        lines.append(f"  Stop-loss:      {cur}{risk.stop_loss:,.2f}  ({stop_dir})")
        lines.append(f"  Target 1:       {cur}{risk.target_1:,.2f}  ({t1_dir})")
        lines.append(f"  Target 2:       {cur}{risk.target_2:,.2f}  ({t2_dir})")
        lines.append(f"  Risk/Reward:    {risk.risk_reward:.1f}:1")

    # ── SEBI note
    lines.append(f"\n  {'─' * (W-2)}")
    lines.append(f"  Statistical analysis only. No buy/sell recommendation.")
    lines.append(f"  Past pattern outcomes do not guarantee future results.")
    lines.append(f"  Consult a SEBI-registered advisor before trading.")
    lines.append("=" * W)

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════
# BOT 7 — PAPER TRADE LOGGER
# ══════════════════════════════════════════════════════════════════

import json
import os

PAPER_LEDGER_FILE = "paper_ledger.json"


def log_paper_trade(ticker: str, market: str, bias: str,
                    risk: RiskOutput, live_score: LiveScore,
                    agent_name: str = "Custom Agent") -> None:
    """
    Bot 7 — When paper trading is enabled, log the signal to a local JSON ledger.
    Each entry: ticker, date, entry price, stop, T1, T2, strategy score, agent.
    Outcomes are updated externally or on next run.
    """
    entry = {
        "agent":         agent_name,
        "ticker":        ticker.upper(),
        "market":        market.upper(),
        "bias":          bias,
        "date":          TODAY.strftime("%Y-%m-%d %H:%M"),
        "entry_price":   round(risk.live_price, 2),
        "stop_loss":     round(risk.stop_loss, 2),
        "stop_pct":      round(risk.stop_pct, 2),
        "target_1":      round(risk.target_1, 2),
        "target_2":      round(risk.target_2, 2),
        "position_size": round(risk.position_size, 2),
        "shares":        risk.shares,
        "signal_score":  f"{live_score.firing_count}/{live_score.qualified_total}",
        "score_label":   live_score.score_label,
        "top_strategy":  live_score.top_strategy.name if live_score.top_strategy else "",
        "status":        "OPEN",
        "outcome_pct":   None,
        "exit_date":     None,
        "exit_price":    None,
    }

    ledger = []
    if os.path.exists(PAPER_LEDGER_FILE):
        try:
            with open(PAPER_LEDGER_FILE, "r") as f:
                ledger = json.load(f)
        except Exception:
            ledger = []

    ledger.append(entry)

    with open(PAPER_LEDGER_FILE, "w") as f:
        json.dump(ledger, f, indent=2)

    print(f"\n  ✅  Paper trade logged → {PAPER_LEDGER_FILE}")
    print(f"  Entry: {risk.currency}{risk.live_price:,.2f}  ·  Stop: {risk.currency}{risk.stop_loss:,.2f}  ·  T1: {risk.currency}{risk.target_1:,.2f}")


def print_paper_ledger() -> None:
    if not os.path.exists(PAPER_LEDGER_FILE):
        print("  No paper trades logged yet.")
        return
    with open(PAPER_LEDGER_FILE, "r") as f:
        ledger = json.load(f)
    if not ledger:
        print("  Paper ledger is empty.")
        return

    print(f"\n  PAPER TRADE LEDGER  ({len(ledger)} entries)")
    print(f"  {'─'*80}")
    fmt = "  {:<12} {:<8} {:<10} {:<10} {:<10} {:<8} {:<12} {:<10}"
    print(fmt.format("Ticker", "Bias", "Entry", "Stop", "T1", "Score", "Status", "Date"))
    print(f"  {'─'*80}")
    for e in ledger[-20:]:
        print(fmt.format(
            e["ticker"], e["bias"][:4],
            str(e["entry_price"]), str(e["stop_loss"]), str(e["target_1"]),
            e["signal_score"], e["status"], e["date"][:10]
        ))
    print()


# ══════════════════════════════════════════════════════════════════
# MAIN RUNNER — run() and run_silent()
# ══════════════════════════════════════════════════════════════════

def run(ticker: str, market: str = "NSE", bias: str = "bullish",
        timeframe: str = "1D", backtest: str = "5y",
        capital: float = 500_000, max_risk_pct: float = 2.0,
        paper_trade: bool = False, agent_name: str = "Custom Agent") -> None:

    years = BACKTEST_YEARS.get(backtest, 5)
    cur   = "₹" if market.upper() == "NSE" else "$"

    print(f"\n  Loading {ticker.upper()} ({market.upper()}, {timeframe}, {backtest} backtest)…")

    df = fetch_data(ticker, market, timeframe, years)
    if df is None or len(df) < 30:
        print(f"  ❌  Could not fetch data for {ticker}. Check ticker and market.")
        return

    print(f"  Data loaded: {len(df)} bars  ·  Building fingerprint…")

    # Bot 1
    fingerprint = build_fingerprint(df, bias)
    qualified   = [r for r in fingerprint if r.qualifies]
    print(f"  Fingerprint complete: {len(qualified)} of 100 {bias} strategies qualified for {ticker.upper()}")

    # Bot 2
    live_score = score_live(df, fingerprint, bias)

    # Bot 3
    outcomes = compute_outcomes(df, live_score.firing_strategies, bias) if live_score.firing_strategies else []

    # Bot 4
    regime = check_regime(market, bias, ticker)

    # Bot 5
    live_price = fetch_live_price(ticker, market) or float(df["Close"].iloc[-1])
    risk = calculate_risk(live_price, capital, max_risk_pct, live_score, market, df, bias)

    # Bot 6
    narrative = write_narrative(ticker, market, bias, timeframe,
                                fingerprint, live_score, outcomes, regime, risk)
    print(narrative)

    # Bot 7 — legacy local JSON ledger (kept for backward compat)
    if paper_trade and live_score.score_label in ("STRONG", "DEVELOPING"):
        log_paper_trade(ticker, market, bias, risk, live_score, agent_name)

    # Bot 7b — NEW: emit signal_events + roster-gated paper_trades into kanida_signals.db
    # This is what lets the learning pipeline see live activity and the retrospective
    # layer measure action vs no-action. Runs regardless of STRONG/DEVELOPING label —
    # the signal_roster.status='active' gate decides whether a paper_trades row is written.
    if live_score.firing_strategies:
        try:
            from signals.live_emit import emit_live_signals
            last_bar = df["Date"].iloc[-1] if "Date" in df.columns else df.index[-1]
            signal_date = last_bar.strftime("%Y-%m-%d") if hasattr(last_bar, "strftime") else str(last_bar)[:10]
            emit_counts = emit_live_signals(
                ticker=ticker.upper(),
                market=market.upper(),
                timeframe=timeframe,
                bias=bias,
                firing_strategy_names=[s.name for s in live_score.firing_strategies],
                signal_date=signal_date,
                entry_price=live_price,
            )
            print(
                f"  signals.db → events +{emit_counts['events_inserted']} "
                f"(dup {emit_counts['events_already_existed']}) · "
                f"paper_trades logged {emit_counts['paper_trades_logged']} / "
                f"gated {emit_counts['paper_trades_gated_out']}"
            )
        except Exception as e:
            print(f"  ⚠  signals.db emit failed: {e}")


def run_silent(ticker: str, market: str = "NSE", bias: str = "bullish",
               timeframe: str = "1D", backtest: str = "5y",
               capital: float = 500_000, max_risk_pct: float = 2.0) -> dict:
    """Batch-friendly — returns dict, no stdout."""
    years = BACKTEST_YEARS.get(backtest, 5)
    try:
        df = fetch_data(ticker, market, timeframe, years)
        if df is None or len(df) < 30:
            return {"ticker": ticker, "market": market, "error": "no data",
                    "qualified": False, "score_pct": 0, "firing": 0, "qualified_total": 0}

        fingerprint  = build_fingerprint(df, bias)
        live_score   = score_live(df, fingerprint, bias)
        regime       = check_regime(market, bias, ticker)
        live_price   = fetch_live_price(ticker, market) or float(df["Close"].iloc[-1])
        risk         = calculate_risk(live_price, capital, max_risk_pct, live_score, market, df, bias)
        qualified    = [r for r in fingerprint if r.qualifies]

        # Emit signal_events + roster-gated paper_trades (silent-safe; swallow errors).
        emit_counts = None
        if live_score.firing_strategies:
            try:
                from signals.live_emit import emit_live_signals
                last_bar = df["Date"].iloc[-1] if "Date" in df.columns else df.index[-1]
                signal_date = last_bar.strftime("%Y-%m-%d") if hasattr(last_bar, "strftime") else str(last_bar)[:10]
                emit_counts = emit_live_signals(
                    ticker=ticker.upper(),
                    market=market.upper(),
                    timeframe=timeframe,
                    bias=bias,
                    firing_strategy_names=[s.name for s in live_score.firing_strategies],
                    signal_date=signal_date,
                    entry_price=live_price,
                )
            except Exception:
                emit_counts = None

        return {
            "ticker":            ticker,
            "market":            market,
            "bias":              bias,
            "timeframe":         timeframe,
            "qualified_total":   len(qualified),
            "firing_count":      live_score.firing_count,
            "score_pct":         round(live_score.score_pct, 1),
            "score_label":       live_score.score_label,
            "top_strategy":      live_score.top_strategy.name if live_score.top_strategy else "",
            "top_win_rate":      round(live_score.top_strategy.win_rate * 100, 1) if live_score.top_strategy else 0,
            "regime":            regime.regime,
            "regime_score":      round(regime.regime_score, 1),
            "bias_aligned":      regime.bias_aligned,
            "sector":            regime.sector,
            "live_price":        round(live_price, 2),
            "stop_loss":         round(risk.stop_loss, 2),
            "stop_pct":          round(risk.stop_pct, 2),
            "target_1":          round(risk.target_1, 2),
            "target_2":          round(risk.target_2, 2),
            "kelly_fraction":    round(risk.kelly_fraction * 100, 1),
            "position_size":     round(risk.position_size, 0),
            "qualified":         live_score.score_pct >= 35,
            "emit_counts":       emit_counts,
            "error":             None,
        }
    except Exception as e:
        return {"ticker": ticker, "market": market, "error": str(e),
                "qualified": False, "score_pct": 0, "firing": 0, "qualified_total": 0}


# ══════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(
        description="KANIDA Custom Agent — Universal Strategy Fingerprint Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--ticker",       required=True, help="Stock ticker, e.g. HDFCBANK or NVDA")
    ap.add_argument("--market",       default="NSE", choices=["NSE", "US"], help="NSE or US")
    ap.add_argument("--bias",         default="bullish", choices=["bullish", "bearish", "neutral"])
    ap.add_argument("--timeframe",    default="1D",
                    choices=["30m", "1H", "4H", "1D", "1W", "1M"])
    ap.add_argument("--backtest",     default="5y", choices=["1y", "3y", "5y", "10y"])
    ap.add_argument("--capital",      type=float, default=500_000, help="Trading capital")
    ap.add_argument("--max-risk",     type=float, default=2.0, help="Max risk per trade (%)")
    ap.add_argument("--paper-trade",  action="store_true", help="Log signal to paper ledger")
    ap.add_argument("--ledger",       action="store_true", help="Print paper trade ledger")
    ap.add_argument("--agent-name",   default="Custom Agent", help="Name for this agent config")
    args = ap.parse_args()

    if not PANDAS_AVAILABLE:
        print("❌  pip install pandas numpy yfinance")
        sys.exit(1)

    if args.ledger:
        print_paper_ledger()
        return

    run(
        ticker=args.ticker,
        market=args.market,
        bias=args.bias,
        timeframe=args.timeframe,
        backtest=args.backtest,
        capital=args.capital,
        max_risk_pct=args.max_risk,
        paper_trade=args.paper_trade,
        agent_name=args.agent_name,
    )


if __name__ == "__main__":
    main()
