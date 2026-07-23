"""
Engine V3 — V2 with empirical fixes from the V2 scorecard.

V3 changes (vs V2):
  1. DROP Pattern E (RS leadership) — 47.8% WR alone, no edge.
  2. RETIRE Pattern A (Bandhan full-stack) — 0 standalone fires, too restrictive.
  3. ADD fakeout suppressor for Pattern D: reject D signals where prior 2 days
     cumulative close-to-close return > +5% (prevents chasing extended moves
     like POLYCAB-Mar-5 where the stock was already extended into the close).
  4. RANK every fired signal by composite score; output top-5 per day only.
     Composite score: 2.0*close_loc + 1.0*(pattern_count/3) + 0.5*min(OI_growth,2)
                       + 0.3*(n_sub_3/7).
  5. DEFAULT to Method B (two-stage with 9:30 vol confirm) for primary outcomes;
     Method A still reported for comparison.

Universal pivot gate (close within 3% of 20d high) and pattern definitions for
B, C, D unchanged from V2.
"""
from __future__ import annotations
import sqlite3, statistics
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from engine.engine_v1 import (
    _daily_bars, _intraday_late_volume, _intraday_first15_volume,
    _agg_oi_series, _simulate_entries, FORWARD_DAYS,
)
from engine.engine_v2 import (
    _close_location, _near_20d_high, _range_metrics, _dryup_counts,
    _late_vol_ratio, _oi_signal,
    NEAR_HIGH_PCT, LATE_RATIO,
    CLOSE_HIGH_THRESH, CLOSE_MID_THRESH,
    OI_5D_GROWTH, OI_PUP_OIUP_DAYS,
    _pattern_B, _pattern_C,
)


# ─────────────────────────────────────────────────────────────────────────────
# V3 thresholds
# ─────────────────────────────────────────────────────────────────────────────

FAKEOUT_PRIOR_2D_PCT = 0.05      # reject D if prior 2 days cumulative > +5%
TOP_N_PER_DAY        = 5         # concentrate to top-5 ranked signals per day


# ─────────────────────────────────────────────────────────────────────────────
# Pattern D with fakeout suppressor
# ─────────────────────────────────────────────────────────────────────────────

def _pattern_D_v3(close_loc, late_r, oi, rng, prior_2d_ret) -> bool:
    """V2 pattern D + fakeout suppressor."""
    if close_loc < CLOSE_HIGH_THRESH: return False
    if prior_2d_ret is not None and prior_2d_ret > FAKEOUT_PRIOR_2D_PCT:
        return False    # already extended; risk of chasing
    cond_a = late_r >= LATE_RATIO
    cond_b = (oi is not None and oi["growth_5d"] >= OI_5D_GROWTH
                and oi["n_pup_oiup_days"] >= OI_PUP_OIUP_DAYS)
    cond_c = rng["n_sub_3"] >= 3
    return cond_a or cond_b or cond_c


def _prior_2d_return(bars: List[Dict], idx: int) -> Optional[float]:
    if idx < 2: return None
    p_now = bars[idx]["close"]
    p_2 = bars[idx - 2]["close"]
    if p_2 <= 0: return None
    return (p_now / p_2) - 1.0


# ─────────────────────────────────────────────────────────────────────────────
# Composite ranking score
# ─────────────────────────────────────────────────────────────────────────────

def _composite_score(close_loc, n_patterns, oi_growth, n_sub_3) -> float:
    oi_term = 0.5 * min(oi_growth, 2.0) if oi_growth is not None else 0.0
    return (2.0 * close_loc
             + 1.0 * (n_patterns / 3.0)
             + oi_term
             + 0.3 * (n_sub_3 / 7.0))


# ─────────────────────────────────────────────────────────────────────────────
# Per-symbol scanner (worker)
# ─────────────────────────────────────────────────────────────────────────────

def _scan_one_symbol_v3(args) -> List[Dict]:
    symbol, db_path, eval_start, eval_end = args
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

        # Universal gate
        near_high = _near_20d_high(bars, idx)
        if near_high is None or near_high < -NEAR_HIGH_PCT:
            continue

        bar = bars[idx]
        rng = _range_metrics(bars, idx)
        dry = _dryup_counts(bars, idx)
        late_r = _late_vol_ratio(late_vol_by_day, d, days_sorted)
        close_loc = _close_location(bar)
        oi = _oi_signal(oi_series, bars, bar_idx_by_date, d)
        prior_2d = _prior_2d_return(bars, idx)

        # V3 patterns: B, C, D only (E, A retired)
        fired = {
            "B": _pattern_B(rng, oi),
            "C": _pattern_C(dry, close_loc),
            "D": _pattern_D_v3(close_loc, late_r, oi, rng, prior_2d),
        }
        patterns_hit = [k for k, v in fired.items() if v]
        if not patterns_hit:
            continue

        oi_growth = oi["growth_5d"] if oi else None
        score = _composite_score(close_loc, len(patterns_hit), oi_growth, rng["n_sub_3"])

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
            "oi_growth_5d": round(oi_growth, 3) if oi_growth is not None else None,
            "oi_pup_oiup":  oi["n_pup_oiup_days"] if oi else None,
            "prior_2d_ret": round(prior_2d, 4) if prior_2d is not None else None,
            "patterns":    patterns_hit,
            "n_patterns":  len(patterns_hit),
            "score":       round(score, 4),
            "has_oi":      oi is not None,
        }
        rec.update(_simulate_entries(bars, idx, late_vol_by_day, first15_by_day, days_sorted))
        out.append(rec)

    return out


# ─────────────────────────────────────────────────────────────────────────────
# Top-N per day reduction
# ─────────────────────────────────────────────────────────────────────────────

def reduce_to_top_n_per_day(signals: List[Dict], top_n: int = TOP_N_PER_DAY) -> List[Dict]:
    by_day: Dict[str, List[Dict]] = defaultdict(list)
    for s in signals:
        by_day[s["signal_date"]].append(s)
    out = []
    for d, group in by_day.items():
        group_sorted = sorted(group, key=lambda x: x["score"], reverse=True)
        out.extend(group_sorted[:top_n])
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Top-level driver
# ─────────────────────────────────────────────────────────────────────────────

def scan_universe_v3(db_path: Path, symbols: List[str],
                       eval_start: str, eval_end: str,
                       n_workers: int = 16,
                       top_n_per_day: int = TOP_N_PER_DAY) -> Tuple[List[Dict], List[Dict]]:
    """Returns (all_signals, top_n_signals)."""
    n_workers = max(10, min(48, n_workers))
    args_list = [(s, str(db_path), eval_start, eval_end) for s in symbols]
    out: List[Dict] = []
    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        futs = {ex.submit(_scan_one_symbol_v3, a): a[0] for a in args_list}
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
    top_n = reduce_to_top_n_per_day(out, top_n_per_day)
    return out, top_n
