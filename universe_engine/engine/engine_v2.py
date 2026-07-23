"""
Engine V2 — multi-pattern OR detector with universal pivot gate.

Built on the 6-winner audit (V1 missed Bandhan; close-upper-third is not universal).

UNIVERSAL CONTEXT GATE (every signal must pass):
  - close within 3% of 20-day high (proves we're at a pivot, not a random bounce)
  - in_nifty200 = 1 (top-200 by mcap, liquidity proxy)
  - no corp-action ±5 days (handled by UniverseFilter)

THEN any one of 5 patterns fires:

  A — Bandhan smart-money (late-day absorption)
        range compression  + vol dry-up + late-vol surge + close >= 60% + OI 5d >= +40%
        with >= 4 price-up + OI-up days in the buildup
  B — Compression + OI buildup (Nestle/Hindunilvr-style)
        >= 4 sub-3% range days  AND  OI 5d >= +40% with >=4 price-up+OI-up days
        (NO close-gate — compression+OI is the evidence)
  C — Heavy dry-up stealth (LTTS-style)
        >= 4 of last 7 days vol <= 80% of baseline  AND  close >= 60%
  D — Strong-close imbalance (ABB/TRENT-style)
        close >= 70% AND (late-vol surge OR OI 5d >= +40% OR >=3 sub-3% range days)
  E — Relative strength leadership (Aarti/sector momentum)
        >= 3 sub-2.5% range days in last 7  AND  RS rank in top 20% by 20d return
        (NO close-gate initially)

Per-pattern hit rate measured separately to validate each cluster has edge.
"""
from __future__ import annotations
import sqlite3, statistics
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from engine.engine_v1 import (
    _daily_bars, _intraday_late_volume, _intraday_first15_volume,
    _agg_oi_series, _simulate_entries,
    LATE_WINDOW_START, LATE_WINDOW_END, FORWARD_DAYS,
)


# ─────────────────────────────────────────────────────────────────────────────
# V2 thresholds
# ─────────────────────────────────────────────────────────────────────────────

# Universal gate
NEAR_HIGH_PCT     = 0.03          # close must be within 3% of 20d high
HIGH_LOOKBACK     = 20

# Range compression
SUB_3_RANGE_PCT   = 3.0
SUB_2_5_RANGE_PCT = 2.5
RANGE_LOOKBACK    = 7

# Dry-up
DRYUP_75_RATIO    = 0.75
DRYUP_80_RATIO    = 0.80
DRYUP_LOOKBACK    = 7
VOL_BASELINE      = 30

# Late-day vol
LATE_RATIO        = 1.4
LATE_LOOKBACK     = 10

# Close location
CLOSE_HIGH_THRESH = 0.70
CLOSE_MID_THRESH  = 0.60

# OI gate (stricter than V1)
OI_5D_GROWTH      = 0.40          # +40% (Codex's nuance)
OI_PUP_OIUP_DAYS  = 4              # require >= 4 days of price-up AND OI-up

# RS gate
RS_LOOKBACK       = 20
RS_TOP_PCT        = 0.20          # top 20% by 20d return


# ─────────────────────────────────────────────────────────────────────────────
# Feature computations
# ─────────────────────────────────────────────────────────────────────────────

def _close_location(bar: Dict) -> float:
    if bar["high"] <= bar["low"]: return 0.5
    return (bar["close"] - bar["low"]) / (bar["high"] - bar["low"])


def _near_20d_high(bars: List[Dict], idx: int) -> Optional[float]:
    """Returns (close / max(high, last 20)) - 1. Negative = below high."""
    if idx < HIGH_LOOKBACK: return None
    window = bars[idx - HIGH_LOOKBACK + 1: idx + 1]
    high20 = max(b["high"] for b in window)
    if high20 <= 0: return None
    return (bars[idx]["close"] / high20) - 1.0


def _range_metrics(bars: List[Dict], idx: int) -> Dict:
    """Counts of sub-2.5% and sub-3% range days in last RANGE_LOOKBACK days incl idx."""
    if idx < RANGE_LOOKBACK - 1:
        return {"n_sub_2_5": 0, "n_sub_3": 0, "avg_range": 0}
    rngs = []
    for b in bars[idx - RANGE_LOOKBACK + 1: idx + 1]:
        if b["close"] > 0:
            rngs.append((b["high"] - b["low"]) / b["close"] * 100)
    return {
        "n_sub_2_5": sum(1 for r in rngs if r <= SUB_2_5_RANGE_PCT),
        "n_sub_3":   sum(1 for r in rngs if r <= SUB_3_RANGE_PCT),
        "avg_range": statistics.mean(rngs) if rngs else 0,
    }


def _dryup_counts(bars: List[Dict], idx: int) -> Dict:
    """Counts of last 7 days with vol <= 75% / <= 80% of 30d baseline."""
    if idx < VOL_BASELINE + DRYUP_LOOKBACK:
        return {"n_75": 0, "n_80": 0, "mean_ratio": 0}
    base = bars[idx - VOL_BASELINE - DRYUP_LOOKBACK + 1: idx - DRYUP_LOOKBACK + 1]
    recent = bars[idx - DRYUP_LOOKBACK + 1: idx + 1]
    base_vol = statistics.mean([b["volume"] for b in base if b["volume"] > 0]) if base else 0
    if base_vol <= 0:
        return {"n_75": 0, "n_80": 0, "mean_ratio": 0}
    n_75 = sum(1 for b in recent if b["volume"] <= 0.75 * base_vol)
    n_80 = sum(1 for b in recent if b["volume"] <= 0.80 * base_vol)
    mean_ratio = statistics.mean([b["volume"] for b in recent if b["volume"] > 0]) / base_vol
    return {"n_75": n_75, "n_80": n_80, "mean_ratio": mean_ratio}


def _late_vol_ratio(late_by_day: Dict[str, int], today: str, days_sorted: List[str]) -> float:
    if today not in days_sorted: return 0
    i = days_sorted.index(today)
    if i < LATE_LOOKBACK: return 0
    today_v = late_by_day.get(today, 0)
    prior = [late_by_day.get(d, 0) for d in days_sorted[i - LATE_LOOKBACK: i]]
    prior = [v for v in prior if v > 0]
    if not prior: return 0
    base = statistics.mean(prior)
    return today_v / base if base > 0 else 0


def _oi_signal(oi_series: Dict[str, int], bars: List[Dict],
                bar_idx_by_date: Dict[str, int], today: str) -> Optional[Dict]:
    """Returns dict with growth_5d, n_pup_oiup_days, OR None if insufficient OI data."""
    if today not in oi_series: return None
    days = sorted(oi_series.keys())
    if today not in days: return None
    i = days.index(today)
    if i < 5: return None
    days_window = days[i - 5: i + 1]
    oi_now = oi_series[days_window[-1]]
    oi_then = oi_series[days_window[0]]
    if oi_then <= 0: return None
    growth = (oi_now / oi_then) - 1.0

    # Count price-up + OI-up days in the 5-day window
    pup_oiup = 0
    for j in range(1, len(days_window)):
        d_prev, d_curr = days_window[j-1], days_window[j]
        if d_prev not in bar_idx_by_date or d_curr not in bar_idx_by_date:
            continue
        p_prev = bars[bar_idx_by_date[d_prev]]["close"]
        p_curr = bars[bar_idx_by_date[d_curr]]["close"]
        oi_prev = oi_series[d_prev]
        oi_curr = oi_series[d_curr]
        if p_curr > p_prev and oi_curr > oi_prev:
            pup_oiup += 1

    return {"growth_5d": growth, "n_pup_oiup_days": pup_oiup}


def _rs_rank(symbol: str, on_date: str, all_returns: Dict[str, float]) -> Optional[float]:
    """Returns rank percentile of this symbol's 20d return vs all_returns."""
    if symbol not in all_returns: return None
    vals = sorted(all_returns.values())
    n = len(vals)
    if n == 0: return None
    my_v = all_returns[symbol]
    rank = sum(1 for v in vals if v <= my_v)
    return rank / n


# ─────────────────────────────────────────────────────────────────────────────
# Pattern evaluators (each returns bool fired)
# ─────────────────────────────────────────────────────────────────────────────

def _pattern_A(rng, dry, late_r, close_loc, oi) -> bool:
    """Bandhan smart-money — late-day absorption."""
    if oi is None: return False
    return (rng["n_sub_2_5"] >= 3 and dry["n_75"] >= 3 and late_r >= LATE_RATIO
             and close_loc >= CLOSE_MID_THRESH
             and oi["growth_5d"] >= OI_5D_GROWTH and oi["n_pup_oiup_days"] >= OI_PUP_OIUP_DAYS)


def _pattern_B(rng, oi) -> bool:
    """Compression + OI buildup. NO close gate."""
    if oi is None: return False
    return (rng["n_sub_3"] >= 4
             and oi["growth_5d"] >= OI_5D_GROWTH and oi["n_pup_oiup_days"] >= OI_PUP_OIUP_DAYS)


def _pattern_C(dry, close_loc) -> bool:
    """Heavy dry-up stealth."""
    return dry["n_80"] >= 4 and close_loc >= CLOSE_MID_THRESH


def _pattern_D(close_loc, late_r, oi, rng) -> bool:
    """Strong-close imbalance."""
    if close_loc < CLOSE_HIGH_THRESH: return False
    cond_a = late_r >= LATE_RATIO
    cond_b = (oi is not None and oi["growth_5d"] >= OI_5D_GROWTH
                and oi["n_pup_oiup_days"] >= OI_PUP_OIUP_DAYS)
    cond_c = rng["n_sub_3"] >= 3
    return cond_a or cond_b or cond_c


def _pattern_E(rng, rs_rank) -> bool:
    """RS leadership. NO close gate initially."""
    if rs_rank is None: return False
    return rng["n_sub_2_5"] >= 3 and rs_rank >= (1.0 - RS_TOP_PCT)


# ─────────────────────────────────────────────────────────────────────────────
# Per-symbol scanner (worker)
# ─────────────────────────────────────────────────────────────────────────────

def _scan_one_symbol(args) -> List[Dict]:
    symbol, db_path, eval_start, eval_end, all_returns_by_date = args
    con = sqlite3.connect(db_path, timeout=60.0)

    load_from = (date.fromisoformat(eval_start) - timedelta(days=80)).isoformat()
    load_to   = (date.fromisoformat(eval_end)   + timedelta(days=15)).isoformat()
    bars = _daily_bars(con, symbol, load_from, load_to)
    if len(bars) < 50:
        con.close(); return []

    bar_idx_by_date = {b["trade_date"]: i for i, b in enumerate(bars)}
    late_vol_by_day = _intraday_late_volume(con, symbol, load_from, load_to)
    first15_by_day  = _intraday_first15_volume(con, symbol, load_from, load_to)
    oi_series       = _agg_oi_series(con, symbol, load_from, load_to)
    days_sorted     = sorted(late_vol_by_day.keys())
    con.close()

    out = []
    for d, idx in bar_idx_by_date.items():
        if d < eval_start or d > eval_end: continue
        if idx + FORWARD_DAYS >= len(bars): continue

        # Universal gate: within 3% of 20d high
        near_high = _near_20d_high(bars, idx)
        if near_high is None or near_high < -NEAR_HIGH_PCT:
            continue

        bar = bars[idx]
        rng = _range_metrics(bars, idx)
        dry = _dryup_counts(bars, idx)
        late_r = _late_vol_ratio(late_vol_by_day, d, days_sorted)
        close_loc = _close_location(bar)
        oi = _oi_signal(oi_series, bars, bar_idx_by_date, d)

        rs_rank = None
        if all_returns_by_date and d in all_returns_by_date:
            rs_rank = _rs_rank(symbol, d, all_returns_by_date[d])

        # Evaluate patterns
        fired = {
            "A": _pattern_A(rng, dry, late_r, close_loc, oi),
            "B": _pattern_B(rng, oi),
            "C": _pattern_C(dry, close_loc),
            "D": _pattern_D(close_loc, late_r, oi, rng),
            "E": _pattern_E(rng, rs_rank),
        }
        if not any(fired.values()):
            continue

        rec = {
            "symbol":      symbol,
            "signal_date": d,
            "near_high":   round(near_high, 4),
            "n_sub_2_5":   rng["n_sub_2_5"],
            "n_sub_3":     rng["n_sub_3"],
            "avg_range":   round(rng["avg_range"], 3),
            "dryup_n75":   dry["n_75"],
            "dryup_n80":   dry["n_80"],
            "mean_v_ratio": round(dry["mean_ratio"], 3),
            "late_ratio":  round(late_r, 3),
            "close_loc":   round(close_loc, 3),
            "oi_growth_5d":  round(oi["growth_5d"], 3) if oi else None,
            "oi_pup_oiup":   oi["n_pup_oiup_days"] if oi else None,
            "rs_rank":     round(rs_rank, 3) if rs_rank is not None else None,
            "patterns":    [k for k, v in fired.items() if v],
            "has_oi":      oi is not None,
        }
        rec.update(_simulate_entries(bars, idx, late_vol_by_day, first15_by_day, days_sorted))
        out.append(rec)

    return out


# ─────────────────────────────────────────────────────────────────────────────
# Universe-wide RS computation (precompute once for all symbols × dates)
# ─────────────────────────────────────────────────────────────────────────────

def precompute_rs(con: sqlite3.Connection, symbols: List[str],
                   eval_start: str, eval_end: str) -> Dict[str, Dict[str, float]]:
    """For each date in [eval_start, eval_end], compute each symbol's 20d return.
    Returns {date: {symbol: 20d_return}}."""
    load_from = (date.fromisoformat(eval_start) - timedelta(days=40)).isoformat()
    load_to   = eval_end
    rows = con.execute(f"""
        SELECT symbol, trade_date, close FROM ohlc_daily
        WHERE symbol IN ({",".join("?"*len(symbols))})
          AND trade_date >= ? AND trade_date <= ?
        ORDER BY symbol, trade_date
    """, list(symbols) + [load_from, load_to]).fetchall()

    # Group by symbol
    by_sym: Dict[str, List[Tuple[str, float]]] = {}
    for sym, td, cl in rows:
        by_sym.setdefault(sym, []).append((td, cl))

    # For each (sym, date_idx >= 20), compute 20d return; bucket by date
    by_date: Dict[str, Dict[str, float]] = {}
    for sym, series in by_sym.items():
        for i in range(RS_LOOKBACK, len(series)):
            td_now, cl_now = series[i]
            td_then, cl_then = series[i - RS_LOOKBACK]
            if cl_then <= 0: continue
            ret = (cl_now / cl_then) - 1.0
            by_date.setdefault(td_now, {})[sym] = ret
    return by_date


# ─────────────────────────────────────────────────────────────────────────────
# Top-level driver
# ─────────────────────────────────────────────────────────────────────────────

def scan_universe_v2(db_path: Path, symbols: List[str],
                       eval_start: str, eval_end: str,
                       n_workers: int = 16) -> List[Dict]:
    n_workers = max(10, min(48, n_workers))
    con = sqlite3.connect(db_path, timeout=60.0)
    print("[v2] Precomputing 20d RS rankings ...", flush=True)
    rs_by_date = precompute_rs(con, symbols, eval_start, eval_end)
    con.close()
    print(f"[v2] RS dates: {len(rs_by_date)}", flush=True)

    args_list = [(s, str(db_path), eval_start, eval_end, rs_by_date) for s in symbols]
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
            if done % 25 == 0:
                print(f"  [{done}/{len(args_list)}] symbols scanned, signals={len(out)}", flush=True)
    return out
