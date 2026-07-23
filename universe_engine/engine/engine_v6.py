"""
Engine V6 — Confluence-stacked deterministic signal engine.

OHLC-only. No OI, no intraday. Designed to survive OOS testing across 2+ years
on Nifty 500.

Fires when ≥1 base bullish pattern fires AND ≥2 confluence filters pass.
Tested across 4 hold periods (5d, 10d, 20d, 30d).

BASE PATTERNS (each returns bool):
  NHB              new high breakout (close > 20d high, upper-third close)
  HLBO             higher-low breakout (3 HLs + close > prior swing high)
  VCP              volatility contraction (ATR20 declining 5d + price near top)
  VOL_THRUST       3-day volume > 1.5× 20d avg + close upper half
  REPRICING_60D    close > 60-day high, gap up from prior bar's high

CONFLUENCES (each returns bool):
  TREND_50         close > 50d SMA AND 50d SMA rising
  TREND_200        close > 200d SMA
  RSI_HEALTHY      RSI(14) in [50, 75]
  NEAR_FIB_38      within 2% of 38.2% retracement of recent 60d range
  NEAR_PIVOT_R1    within 1.5% of weekly Pivot R1
  VOL_RISING       5d avg vol > 20d avg vol
"""
from __future__ import annotations
import sqlite3, statistics
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ─────────────────────────────────────────────────────────────────────────────
# Indicator helpers
# ─────────────────────────────────────────────────────────────────────────────

def _sma(values: List[float], n: int) -> Optional[float]:
    if len(values) < n: return None
    return sum(values[-n:]) / n


def _rsi(closes: List[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1: return None
    gains, losses = [], []
    for i in range(-period, 0):
        diff = closes[i] - closes[i - 1]
        if diff >= 0: gains.append(diff); losses.append(0)
        else: gains.append(0); losses.append(-diff)
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0: return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _atr(bars: List[Dict], n: int = 20) -> Optional[float]:
    if len(bars) < n + 1: return None
    trs = []
    for i in range(-n, 0):
        h, l, pc = bars[i]["high"], bars[i]["low"], bars[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / n


# ─────────────────────────────────────────────────────────────────────────────
# Base patterns (each: (bars, idx) -> bool)
# ─────────────────────────────────────────────────────────────────────────────

def pat_nhb(bars: List[Dict], idx: int) -> bool:
    """New High Breakout: close > max(close, last 20 days) + close in upper-third."""
    if idx < 20: return False
    today = bars[idx]
    win = bars[idx - 20: idx]
    if not win: return False
    prior_max_close = max(b["close"] for b in win)
    if today["close"] <= prior_max_close: return False
    if today["high"] <= today["low"]: return False
    cul = (today["close"] - today["low"]) / (today["high"] - today["low"])
    return cul >= 0.66


def pat_hlbo(bars: List[Dict], idx: int) -> bool:
    """Higher-low breakout: 3 consecutive higher daily lows preceded the breakout day,
       and today's close exceeds the highest high of those 3 days."""
    if idx < 4: return False
    l1, l2, l3 = bars[idx - 3]["low"], bars[idx - 2]["low"], bars[idx - 1]["low"]
    if not (l1 < l2 < l3): return False
    h_max = max(bars[idx - 3]["high"], bars[idx - 2]["high"], bars[idx - 1]["high"])
    return bars[idx]["close"] > h_max


def pat_vcp(bars: List[Dict], idx: int) -> bool:
    """Volatility contraction pattern: ATR(20) declining for 5 days AND price within
       3% of recent 20d high."""
    if idx < 30: return False
    atrs = []
    for j in range(idx - 4, idx + 1):
        a = _atr(bars[: j + 1], 20)
        if a is None: return False
        atrs.append(a)
    # Strictly declining (allow equal in middle)
    if not all(atrs[i] >= atrs[i + 1] for i in range(len(atrs) - 1)): return False
    high_20 = max(b["high"] for b in bars[idx - 19: idx + 1])
    if high_20 <= 0: return False
    return (high_20 - bars[idx]["close"]) / high_20 <= 0.03


def pat_vol_thrust(bars: List[Dict], idx: int) -> bool:
    """3-day volume > 1.5× 20-day baseline AND close in upper half."""
    if idx < 23: return False
    last3 = sum(b["volume"] for b in bars[idx - 2: idx + 1]) / 3
    base20 = sum(b["volume"] for b in bars[idx - 22: idx - 2]) / 20
    if base20 <= 0: return False
    today = bars[idx]
    if today["high"] <= today["low"]: return False
    cul = (today["close"] - today["low"]) / (today["high"] - today["low"])
    return last3 / base20 >= 1.5 and cul >= 0.5


def pat_repricing_60d(bars: List[Dict], idx: int) -> bool:
    """Close > 60d high (excluding today), gap up from prior bar."""
    if idx < 60: return False
    win = bars[idx - 60: idx]
    prior_high = max(b["high"] for b in win)
    if bars[idx]["close"] <= prior_high: return False
    return bars[idx]["open"] > bars[idx - 1]["high"]


BASE_PATTERNS = {
    "NHB":           pat_nhb,
    "HLBO":          pat_hlbo,
    "VCP":           pat_vcp,
    "VOL_THRUST":    pat_vol_thrust,
    "REPRICING_60D": pat_repricing_60d,
}


# ─────────────────────────────────────────────────────────────────────────────
# Confluence filters
# ─────────────────────────────────────────────────────────────────────────────

def conf_trend_50(bars: List[Dict], idx: int) -> bool:
    """Close > 50d SMA AND 50d SMA rising vs 5 days ago."""
    if idx < 55: return False
    closes = [b["close"] for b in bars[: idx + 1]]
    sma_now = _sma(closes, 50)
    sma_prev = _sma(closes[:-5], 50)
    if sma_now is None or sma_prev is None: return False
    return bars[idx]["close"] > sma_now and sma_now > sma_prev


def conf_trend_200(bars: List[Dict], idx: int) -> bool:
    if idx < 200: return False
    closes = [b["close"] for b in bars[: idx + 1]]
    sma200 = _sma(closes, 200)
    return sma200 is not None and bars[idx]["close"] > sma200


def conf_rsi_healthy(bars: List[Dict], idx: int) -> bool:
    if idx < 15: return False
    closes = [b["close"] for b in bars[: idx + 1]]
    rsi = _rsi(closes, 14)
    return rsi is not None and 50 <= rsi <= 75


def conf_near_fib_38(bars: List[Dict], idx: int) -> bool:
    """Within 2% of 38.2% retracement of recent 60-day range.
       38.2% retracement (from low going up to high) = low + 0.382*(high-low)."""
    if idx < 60: return False
    win = bars[idx - 60: idx + 1]
    hi = max(b["high"] for b in win)
    lo = min(b["low"]  for b in win)
    if hi <= lo: return False
    fib_38 = lo + 0.382 * (hi - lo)
    fib_50 = lo + 0.50  * (hi - lo)
    fib_61 = lo + 0.618 * (hi - lo)
    cl = bars[idx]["close"]
    # Close within 2% of any fib level
    for lvl in (fib_38, fib_50, fib_61):
        if abs(cl - lvl) / lvl <= 0.02:
            return True
    return False


def conf_near_pivot_r1(bars: List[Dict], idx: int) -> bool:
    """Within 1.5% of weekly Pivot R1 (using last completed week)."""
    if idx < 6: return False
    last_week = bars[idx - 5: idx]
    h = max(b["high"] for b in last_week)
    l = min(b["low"]  for b in last_week)
    c = last_week[-1]["close"]
    pivot = (h + l + c) / 3
    r1 = 2 * pivot - l
    r2 = pivot + (h - l)
    cl = bars[idx]["close"]
    return abs(cl - r1) / r1 <= 0.015 or abs(cl - r2) / r2 <= 0.015


def conf_vol_rising(bars: List[Dict], idx: int) -> bool:
    if idx < 25: return False
    last5 = sum(b["volume"] for b in bars[idx - 4: idx + 1]) / 5
    prior20 = sum(b["volume"] for b in bars[idx - 24: idx - 4]) / 20
    return prior20 > 0 and last5 > prior20


CONFLUENCES = {
    "TREND_50":       conf_trend_50,
    "TREND_200":      conf_trend_200,
    "RSI_HEALTHY":    conf_rsi_healthy,
    "NEAR_FIB":       conf_near_fib_38,
    "NEAR_PIVOT_R1":  conf_near_pivot_r1,
    "VOL_RISING":     conf_vol_rising,
}


# ─────────────────────────────────────────────────────────────────────────────
# Per-symbol scanner
# ─────────────────────────────────────────────────────────────────────────────

HOLD_PERIODS = (5, 10, 20, 30)
MIN_CONFLUENCES = 2


def _daily_bars(con, symbol, from_date, to_date):
    rows = con.execute("""SELECT trade_date, open, high, low, close, volume
                           FROM ohlc_daily
                           WHERE symbol=? AND trade_date>=? AND trade_date<=?
                           ORDER BY trade_date""",
                       (symbol, from_date, to_date)).fetchall()
    return [{"trade_date": r[0], "open": r[1], "high": r[2], "low": r[3],
              "close": r[4], "volume": r[5]} for r in rows]


def _scan_one_symbol(args) -> List[Dict]:
    symbol, db_path, eval_start, eval_end = args
    con = sqlite3.connect(db_path, timeout=60.0)
    load_from = (date.fromisoformat(eval_start) - timedelta(days=320)).isoformat()
    load_to   = (date.fromisoformat(eval_end)   + timedelta(days=45)).isoformat()
    bars = _daily_bars(con, symbol, load_from, load_to)
    con.close()
    if len(bars) < 220: return []

    bar_idx_by_date = {b["trade_date"]: i for i, b in enumerate(bars)}
    out = []
    for d, idx in bar_idx_by_date.items():
        if d < eval_start or d > eval_end: continue
        if idx + max(HOLD_PERIODS) >= len(bars): continue

        # Evaluate base patterns
        bases_fired = [name for name, fn in BASE_PATTERNS.items() if fn(bars, idx)]
        if not bases_fired: continue

        # Evaluate confluences
        confs_fired = [name for name, fn in CONFLUENCES.items() if fn(bars, idx)]
        if len(confs_fired) < MIN_CONFLUENCES: continue

        # Outcomes for each hold period
        cl_today = bars[idx]["close"]
        op_next = bars[idx + 1]["open"] if idx + 1 < len(bars) else None
        if op_next is None or op_next <= 0: continue

        outcomes = {}
        for n in HOLD_PERIODS:
            if idx + n >= len(bars): continue
            window = bars[idx + 1: idx + 1 + n]
            cl_exit = window[-1]["close"]
            mfe = max(b["high"] for b in window) / op_next - 1
            mae = min(b["low"]  for b in window) / op_next - 1
            ret = cl_exit / op_next - 1
            outcomes[f"ret_{n}d"]  = round(ret * 100, 3)
            outcomes[f"mfe_{n}d"]  = round(mfe * 100, 3)
            outcomes[f"mae_{n}d"]  = round(mae * 100, 3)

        out.append({
            "symbol":      symbol,
            "signal_date": d,
            "entry_date":  bars[idx + 1]["trade_date"],
            "entry_price": round(op_next, 4),
            "bases":       bases_fired,
            "confluences": confs_fired,
            "n_bases":     len(bases_fired),
            "n_confs":     len(confs_fired),
            **outcomes,
        })
    return out


def scan_universe(db_path: Path, symbols: List[str],
                    eval_start: str, eval_end: str,
                    n_workers: int = 16) -> List[Dict]:
    n_workers = max(10, min(48, n_workers))
    args_list = [(s, str(db_path), eval_start, eval_end) for s in symbols]
    out: List[Dict] = []
    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        futs = {ex.submit(_scan_one_symbol, a): a[0] for a in args_list}
        done = 0
        for f in as_completed(futs):
            sym = futs[f]
            try:
                rs = f.result()
            except Exception as e:
                print(f"  [{sym}] ERROR: {e}", flush=True); rs = []
            out.extend(rs); done += 1
            if done % 50 == 0:
                print(f"  [{done}/{len(args_list)}] symbols, signals={len(out)}", flush=True)
    return out
