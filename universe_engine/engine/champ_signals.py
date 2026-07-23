"""
Signal library for the championship harness.

Each signal is a *ranking function*: given a date and the eligible universe,
return a dict {symbol: score}. Higher score = stronger pick. The backtest
takes the top-N by score at window start.

MVP signal: Momentum + Relative Strength (M_RS).
  Score = 0.6 * 12-1m return + 0.4 * 20d_RS_rank
  - 12-1m return: total return from t-252 to t-21 trading days (skipping last
    month to avoid short-term reversal). Standard cross-sectional momentum
    factor.
  - 20d RS rank: rank of the symbol's last-20-day return vs the universe.
  Weight blend mirrors O'Neil/Jegadeesh-Titman literature: bulk-momentum +
  recent-leadership.

Future signal families (stubbed but not fully implemented in this MVP):
  - CAN SLIM-style breakout (needs earnings data)
  - VCP (volatility contraction pattern)
  - Engine overlap >= 0.90
  - Engine multi_pattern_count >= 5
  - IAS-as-negative-filter
  - Composite (z-score blend)
"""
from __future__ import annotations
import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, List, Optional


@dataclass
class SignalContext:
    """Bars-as-of-date keyed by symbol — passed to signals so they don't need DB access."""
    bars: Dict[str, List[dict]]   # symbol -> [{trade_date, open, high, low, close, volume}, ...] sorted asc
    on_date: str                  # ISO date string; signal must use bars STRICTLY BEFORE this date


def _bars_strictly_before(bars: List[dict], cutoff_date: str) -> List[dict]:
    """Slice bars list to those strictly before cutoff_date. Bars are sorted asc."""
    out = []
    for b in bars:
        if b["trade_date"] < cutoff_date:
            out.append(b)
        else:
            break
    return out


def momentum_rs_score(ctx: SignalContext, universe: List[str]) -> Dict[str, float]:
    """
    M_RS signal — 60/40 blend of 12-1m total return and 20d RS rank.
    Returns {symbol: score}. Symbols with insufficient history are excluded.
    """
    rets_12_1m: Dict[str, float] = {}
    rets_20d:   Dict[str, float] = {}

    for sym in universe:
        bars = ctx.bars.get(sym, [])
        bars = _bars_strictly_before(bars, ctx.on_date)
        if len(bars) < 260:    # need 252+21 bars
            continue
        last = bars[-1]["close"]
        # 12-1m: t-252 to t-21
        p_252 = bars[-252]["close"]
        p_21  = bars[-21]["close"]
        if p_252 > 0 and p_21 > 0:
            rets_12_1m[sym] = (p_21 / p_252) - 1.0
        # 20d
        p_20 = bars[-20]["close"]
        if p_20 > 0:
            rets_20d[sym] = (last / p_20) - 1.0

    common = set(rets_12_1m) & set(rets_20d)
    if not common:
        return {}

    # Cross-sectional rank for both
    def rank_pct(d: Dict[str, float]) -> Dict[str, float]:
        items = sorted(d.items(), key=lambda x: x[1])
        n = len(items)
        return {sym: (i + 1) / n for i, (sym, _) in enumerate(items)}

    r1 = rank_pct({s: rets_12_1m[s] for s in common})
    r2 = rank_pct({s: rets_20d[s] for s in common})
    return {s: 0.6 * r1[s] + 0.4 * r2[s] for s in common}


# ── Bar loader (used by the orchestrator) ────────────────────────────────────

def load_universe_bars(con: sqlite3.Connection,
                        symbols: List[str],
                        from_date: Optional[str] = None,
                        to_date: Optional[str] = None) -> Dict[str, List[dict]]:
    """Load all daily bars for the given symbols into memory, optionally limited
    to [from_date, to_date]. Returns {symbol: [bar_dict, ...]} sorted asc."""
    where = ["symbol IN ({})".format(",".join("?" * len(symbols)))]
    params: list = list(symbols)
    if from_date:
        where.append("trade_date >= ?"); params.append(from_date)
    if to_date:
        where.append("trade_date <= ?"); params.append(to_date)
    sql = f"""
        SELECT symbol, trade_date, open, high, low, close, volume
        FROM ohlc_daily
        WHERE {' AND '.join(where)}
        ORDER BY symbol, trade_date
    """
    out: Dict[str, List[dict]] = {s: [] for s in symbols}
    for sym, td, o, h, l, c, v in con.execute(sql, params):
        out[sym].append({"trade_date": td, "open": o, "high": h,
                          "low": l, "close": c, "volume": v})
    return out


# ── Registry ─────────────────────────────────────────────────────────────────

SIGNAL_REGISTRY = {
    "momentum_rs": momentum_rs_score,
}


def get_signal(name: str):
    if name not in SIGNAL_REGISTRY:
        raise KeyError(f"Unknown signal '{name}'. Available: {list(SIGNAL_REGISTRY)}")
    return SIGNAL_REGISTRY[name]
