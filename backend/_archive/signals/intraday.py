"""
KANIDA Signals — Intraday 5-min Refresh Core
=============================================
Lightweight per-stock refresh designed to run every 5 minutes during
market hours across all covered tickers.

Design
------
The expensive work in custom_agent.run_silent() is build_fingerprint()
— it scans 5 years of bars for every one of ~100 strategies to compute
qualifies/win_rate. That result is stable across an intraday session
(historical data doesn't change mid-day), so we cache it per
(ticker, market, timeframe, bias) per trading day.

Intraday refresh path per stock:
  1. Fetch latest daily bar (yfinance 1d for today — it updates live)
  2. Read historical OHLC from ohlc_daily, splice today's live bar as the
     final row → build a fresh `df`.
  3. Look up cached fingerprint; if missing or stale, build it once.
  4. score_live(df, fingerprint, bias) — cheap (only checks last bar).
  5. emit_live_signals() — UNIQUE on (signal_date, strategy_name) means
     duplicates from the same trading day are silently skipped, so we
     only record NEW firings on the latest bar.

Performance
-----------
build_fingerprint: ~40s/stock (done once per day per bias per tf)
score_live + emit: <0.2s/stock
Full intraday cycle across 213 NSE stocks at 20 workers: < 10s after
fingerprint cache is warm.
"""

from __future__ import annotations

import sys
import os
import threading
from datetime import date, datetime, timezone
from typing import Optional, Tuple

from .db import get_conn
from .live_emit import emit_live_signals


# ──────────────────────────────────────────────────────────────────
# FINGERPRINT CACHE — in-process, keyed by (ticker, market, tf, bias, date)
# ──────────────────────────────────────────────────────────────────

_FP_CACHE: dict[Tuple[str, str, str, str, str], list] = {}
_FP_LOCK  = threading.Lock()


def _fp_key(ticker: str, market: str, timeframe: str, bias: str) -> tuple:
    return (ticker.upper(), market.upper(), timeframe, bias, date.today().isoformat())


def _get_or_build_fingerprint(ticker: str, market: str, timeframe: str,
                              bias: str, df) -> list:
    key = _fp_key(ticker, market, timeframe, bias)
    with _FP_LOCK:
        fp = _FP_CACHE.get(key)
    if fp is not None:
        return fp

    # Lazy import — keep this module import-light
    from agents.custom_agent import build_fingerprint
    fp = build_fingerprint(df, bias)
    with _FP_LOCK:
        _FP_CACHE[key] = fp
    return fp


def clear_fingerprint_cache() -> int:
    """Drop the cache (e.g. at day rollover). Returns count cleared."""
    with _FP_LOCK:
        n = len(_FP_CACHE)
        _FP_CACHE.clear()
    return n


# ──────────────────────────────────────────────────────────────────
# OHLC FROM DB + LIVE SNAPSHOT SPLICE
# ──────────────────────────────────────────────────────────────────

def _load_ohlc_df_from_db(ticker: str, market: str, timeframe: str,
                          years: int = 5):
    """Read historical OHLC from ohlc_daily into a DataFrame shaped like
    what custom_agent expects (columns: Date, Open, High, Low, Close, Volume)."""
    import pandas as pd

    start = (datetime.today().replace(year=datetime.today().year - years)
             ).strftime("%Y-%m-%d")
    conn = get_conn()
    try:
        rows = conn.execute(
            """SELECT trade_date, open, high, low, close, volume
               FROM ohlc_daily
               WHERE ticker=? AND market=? AND trade_date >= ?
               ORDER BY trade_date ASC""",
            (ticker, market, start),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return None

    df = pd.DataFrame(rows, columns=["Date", "Open", "High", "Low", "Close", "Volume"])
    df["Date"] = pd.to_datetime(df["Date"])

    # Weekly resample if requested
    if timeframe == "1W":
        df = df.set_index("Date").resample("W-FRI").agg({
            "Open": "first", "High": "max", "Low": "min",
            "Close": "last", "Volume": "sum",
        }).dropna().reset_index()

    return df


def _splice_live_bar(df, live_price: float, today: Optional[str] = None):
    """Replace the last bar's close with live_price, or append a new bar
    if today's bar isn't in df yet. Keeps the df shape identical."""
    import pandas as pd

    today_dt = pd.to_datetime(today or date.today().isoformat())

    if len(df) and df["Date"].iloc[-1].date() == today_dt.date():
        # Today's bar exists — update close + high/low envelope
        df.loc[df.index[-1], "Close"] = live_price
        df.loc[df.index[-1], "High"]  = max(df["High"].iloc[-1], live_price)
        df.loc[df.index[-1], "Low"]   = min(df["Low"].iloc[-1], live_price)
    else:
        # Append a new bar for today — partial, but score_live only
        # checks the last bar for firing
        df.loc[len(df)] = {
            "Date":   today_dt,
            "Open":   live_price,
            "High":   live_price,
            "Low":    live_price,
            "Close":  live_price,
            "Volume": 0,
        }
    return df


# ──────────────────────────────────────────────────────────────────
# PER-STOCK INTRADAY REFRESH
# ──────────────────────────────────────────────────────────────────

def refresh_stock_intraday(
    ticker: str,
    market: str,
    timeframe: str = "1D",
    biases: tuple = ("bullish", "bearish", "neutral"),
) -> dict:
    """One 5-min refresh cycle for one stock. Safe — swallows errors."""
    from agents.custom_agent import score_live, fetch_live_price

    result = {
        "ticker":              ticker,
        "events_inserted":     0,
        "paper_trades_logged": 0,
        "paper_trades_gated":  0,
        "error":               None,
    }

    try:
        df = _load_ohlc_df_from_db(ticker, market, timeframe)
        if df is None or len(df) < 60:
            result["error"] = "insufficient_ohlc"
            return result

        live_price = fetch_live_price(ticker, market)
        if live_price is None:
            # Fall back to last close — still lets us detect bars already closed
            live_price = float(df["Close"].iloc[-1])
        df = _splice_live_bar(df, float(live_price))

        signal_date = df["Date"].iloc[-1].strftime("%Y-%m-%d")

        for bias in biases:
            try:
                fp = _get_or_build_fingerprint(ticker, market, timeframe, bias, df)
                ls = score_live(df, fp, bias)
                if not ls.firing_strategies:
                    continue
                ec = emit_live_signals(
                    ticker=ticker.upper(),
                    market=market.upper(),
                    timeframe=timeframe,
                    bias=bias,
                    firing_strategy_names=[s.name for s in ls.firing_strategies],
                    signal_date=signal_date,
                    entry_price=float(live_price),
                )
                result["events_inserted"]     += ec["events_inserted"]
                result["paper_trades_logged"] += ec["paper_trades_logged"]
                result["paper_trades_gated"]  += ec["paper_trades_gated_out"]
            except Exception as e:
                result["error"] = f"{bias}:{e}"

    except Exception as e:
        result["error"] = f"outer:{e}"

    return result


# ──────────────────────────────────────────────────────────────────
# MARKET HOURS GATE
# ──────────────────────────────────────────────────────────────────

def is_market_open(market: str, now: Optional[datetime] = None) -> bool:
    """Simple market-hours gate.
    NSE: 09:15-15:30 IST (Mon-Fri)
    US : 09:30-16:00 ET  (Mon-Fri)
    Holidays are not checked here — the runner will simply emit no new
    signals because price won't move."""
    if now is None:
        now = datetime.now(timezone.utc)
    if now.weekday() >= 5:
        return False

    m = market.upper()
    if m == "NSE":
        # IST = UTC + 5:30
        mins = (now.hour * 60 + now.minute + 330) % (24 * 60)
        return (9 * 60 + 15) <= mins <= (15 * 60 + 30)
    if m == "US":
        # ET ≈ UTC - 4 (EDT) or UTC - 5 (EST). Use -4 as a safe approximation
        # during trading months; winter may admit a 1-hr early/late window,
        # which is fine for a 5-min cadence loop.
        mins = (now.hour * 60 + now.minute - 4 * 60) % (24 * 60)
        return (9 * 60 + 30) <= mins <= (16 * 60 + 0)
    return False
