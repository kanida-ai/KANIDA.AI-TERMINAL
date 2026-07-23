"""
Engine V1 — VCP-style breakout setup detector + entry simulator.

Setup criteria (all must fire on signal day, in EOD scan):
  S1. Range contraction:    >= 3 of last 7 days had range% <= 2.5%
  S2. Volume dry-up:        avg vol last 7 days <= 0.75 * prior-30-day baseline
  S3. Late-day vol surge:   13:45-15:30 vol today >= 1.4 * prior 10-day late-vol mean
  S4. OI buildup:           5-day total-OI change >= +15% AND price stable-or-up over that window
                            (only applied to F&O symbols where OI data exists)

Two entry methods simulated side-by-side:
  EM-A. Blind MOO: enter at next-day OPEN price
  EM-B. Two-stage buy-stop:
        - place stop at prior-day high * 1.003
        - by 09:30 cumulative volume must be >= 1.5 * median(prior-10d first-15-min volume)
        - if not confirmed, cancel; if confirmed, fill at the stop level (or open if higher)

Outcomes measured for every fired signal:
  - ret_1d:  close[t+1] / entry - 1
  - ret_5d:  close[t+5] / entry - 1
  - mfe_5d:  max(high[t+1..t+5]) / entry - 1
  - mae_5d:  min(low[t+1..t+5])  / entry - 1
  - hit_5pc, hit_10pc, hit_15pc: did mfe_5d cross those?
  - failure: did mae_5d hit -7% (initial stop)?
"""
from __future__ import annotations
import sqlite3, statistics
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# Default thresholds (V1) — all hardcoded; tune in V2.
RANGE_NARROW_PCT      = 2.5      # %
RANGE_NARROW_MIN_DAYS = 3        # of last 7
RANGE_LOOKBACK        = 7
VOL_DRYUP_RATIO       = 0.75
VOL_DRYUP_LOOKBACK    = 7
VOL_BASELINE_LOOKBACK = 30
LATE_VOL_RATIO        = 1.4
LATE_VOL_LOOKBACK     = 10
OI_BUILDUP_PCT        = 0.15     # 5-day OI change >= +15%
OI_LOOKBACK_DAYS      = 5
OI_PRICE_STABLE_PCT   = -0.03    # price drop allowed up to -3% during OI buildup

INITIAL_STOP_PCT      = 0.07
LATE_WINDOW_START     = "13:45"
LATE_WINDOW_END       = "15:30"
FORWARD_DAYS          = 5

CONFIRM_FIRST15_MULT  = 1.5      # entry-method-B confirmation multiple


# ─────────────────────────────────────────────────────────────────────────────
# Bar loader helpers
# ─────────────────────────────────────────────────────────────────────────────

def _daily_bars(con: sqlite3.Connection, symbol: str,
                 from_date: str, to_date: str) -> List[Dict]:
    rows = con.execute("""
        SELECT trade_date, open, high, low, close, volume
        FROM ohlc_daily WHERE symbol=? AND trade_date>=? AND trade_date<=?
        ORDER BY trade_date
    """, (symbol, from_date, to_date)).fetchall()
    return [{"trade_date": r[0], "open": r[1], "high": r[2], "low": r[3],
             "close": r[4], "volume": r[5]} for r in rows]


def _intraday_late_volume(con: sqlite3.Connection, symbol: str,
                            from_date: str, to_date: str) -> Dict[str, int]:
    """Per-day total volume between 13:45 and 15:30."""
    rows = con.execute(f"""
        SELECT date(bar_time), SUM(volume) FROM ohlc_1min
        WHERE symbol=? AND date(bar_time) BETWEEN ? AND ?
          AND time(bar_time) >= '{LATE_WINDOW_START}'
          AND time(bar_time) <= '{LATE_WINDOW_END}'
        GROUP BY date(bar_time)
    """, (symbol, from_date, to_date)).fetchall()
    return {r[0]: r[1] or 0 for r in rows}


def _intraday_first15_volume(con: sqlite3.Connection, symbol: str,
                               from_date: str, to_date: str) -> Dict[str, int]:
    """Per-day total volume between 09:15 and 09:30."""
    rows = con.execute("""
        SELECT date(bar_time), SUM(volume) FROM ohlc_1min
        WHERE symbol=? AND date(bar_time) BETWEEN ? AND ?
          AND time(bar_time) >= '09:15' AND time(bar_time) < '09:30'
        GROUP BY date(bar_time)
    """, (symbol, from_date, to_date)).fetchall()
    return {r[0]: r[1] or 0 for r in rows}


def _agg_oi_series(con: sqlite3.Connection, symbol: str,
                     from_date: str, to_date: str) -> Dict[str, int]:
    rows = con.execute("""
        SELECT trade_date, total_oi FROM aggregate_oi_daily
        WHERE symbol=? AND trade_date BETWEEN ? AND ?
        ORDER BY trade_date
    """, (symbol, from_date, to_date)).fetchall()
    return {r[0]: r[1] or 0 for r in rows}


# ─────────────────────────────────────────────────────────────────────────────
# Component evaluators
# ─────────────────────────────────────────────────────────────────────────────

def _range_contracted(bars: List[Dict], idx: int) -> bool:
    """idx = index of signal day in bars (last 7 days = bars[idx-6..idx])."""
    if idx < RANGE_LOOKBACK - 1: return False
    n_narrow = 0
    for b in bars[idx - RANGE_LOOKBACK + 1: idx + 1]:
        if b["close"] <= 0: continue
        rng_pct = (b["high"] - b["low"]) / b["close"] * 100
        if rng_pct <= RANGE_NARROW_PCT:
            n_narrow += 1
    return n_narrow >= RANGE_NARROW_MIN_DAYS


def _volume_dryup(bars: List[Dict], idx: int) -> bool:
    if idx < VOL_BASELINE_LOOKBACK + VOL_DRYUP_LOOKBACK: return False
    base = bars[idx - VOL_BASELINE_LOOKBACK - VOL_DRYUP_LOOKBACK + 1: idx - VOL_DRYUP_LOOKBACK + 1]
    recent = bars[idx - VOL_DRYUP_LOOKBACK + 1: idx + 1]
    base_vol = statistics.mean([b["volume"] for b in base if b["volume"] > 0]) if base else 0
    recent_vol = statistics.mean([b["volume"] for b in recent if b["volume"] > 0]) if recent else 0
    if base_vol <= 0: return False
    return recent_vol / base_vol <= VOL_DRYUP_RATIO


def _late_vol_surge(late_vol_by_day: Dict[str, int], today: str, days_sorted: List[str]) -> bool:
    if today not in days_sorted: return False
    i = days_sorted.index(today)
    if i < LATE_VOL_LOOKBACK: return False
    today_late = late_vol_by_day.get(today, 0)
    prior = [late_vol_by_day.get(d, 0) for d in days_sorted[i - LATE_VOL_LOOKBACK: i]]
    prior = [v for v in prior if v > 0]
    if not prior: return False
    base = statistics.mean(prior)
    return today_late / base >= LATE_VOL_RATIO if base > 0 else False


def _oi_buildup(oi_series: Dict[str, int], bars: List[Dict],
                  bar_idx_by_date: Dict[str, int], today: str) -> Optional[bool]:
    """Returns True/False if OI data sufficient, else None (skip OI gate)."""
    if today not in oi_series:
        return None
    days = sorted(oi_series.keys())
    if today not in days:
        return None
    i = days.index(today)
    if i < OI_LOOKBACK_DAYS: return None
    prior_date = days[i - OI_LOOKBACK_DAYS]
    oi_now  = oi_series[today]
    oi_then = oi_series[prior_date]
    if oi_then <= 0: return None
    oi_chg = (oi_now / oi_then) - 1.0
    if oi_chg < OI_BUILDUP_PCT: return False
    # price stable-or-up over same window
    if today not in bar_idx_by_date or prior_date not in bar_idx_by_date:
        return None
    p_now = bars[bar_idx_by_date[today]]["close"]
    p_then = bars[bar_idx_by_date[prior_date]]["close"]
    if p_then <= 0: return None
    p_chg = (p_now / p_then) - 1.0
    return p_chg >= OI_PRICE_STABLE_PCT


# ─────────────────────────────────────────────────────────────────────────────
# Per-symbol scanner (worker)
# ─────────────────────────────────────────────────────────────────────────────

def _scan_one_symbol(args) -> List[Dict]:
    symbol, db_path, eval_start, eval_end = args
    con = sqlite3.connect(db_path, timeout=60.0)

    # Load 60 days extra before eval_start for baselines, and forward to end+15 for outcomes
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
        if idx + FORWARD_DAYS >= len(bars): continue   # need forward window

        s1 = _range_contracted(bars, idx)
        s2 = _volume_dryup(bars, idx)
        s3 = _late_vol_surge(late_vol_by_day, d, days_sorted)
        s4 = _oi_buildup(oi_series, bars, bar_idx_by_date, d)
        # For non-F&O symbols (s4 == None), require s1+s2+s3 only.
        all_fire = s1 and s2 and s3 and (s4 is True or s4 is None)
        if not all_fire:
            continue

        rec = {
            "symbol": symbol, "signal_date": d,
            "s1_range_contracted": s1,
            "s2_vol_dryup":         s2,
            "s3_late_vol_surge":    s3,
            "s4_oi_buildup":        s4,
            "has_oi": s4 is not None,
        }
        rec.update(_simulate_entries(bars, idx, late_vol_by_day, first15_by_day, days_sorted))
        out.append(rec)

    return out


def _simulate_entries(bars: List[Dict], idx: int,
                       late_vol_by_day: Dict[str, int],
                       first15_by_day: Dict[str, int],
                       days_sorted: List[str]) -> Dict:
    """Simulate both entry methods + 1d/5d outcomes."""
    today      = bars[idx]
    next_bar   = bars[idx + 1] if idx + 1 < len(bars) else None
    fwd_bars   = bars[idx + 1: idx + 1 + FORWARD_DAYS]
    out: Dict = {"em_a_filled": False, "em_b_filled": False,
                 "em_a_entry": None,  "em_b_entry": None,
                 "em_b_cancel_reason": None,
                 "ret_1d_a": None, "ret_5d_a": None,
                 "ret_1d_b": None, "ret_5d_b": None,
                 "mfe_5d_a": None, "mae_5d_a": None,
                 "mfe_5d_b": None, "mae_5d_b": None,
                 "hit_5pc_a": False, "hit_10pc_a": False, "hit_15pc_a": False,
                 "hit_5pc_b": False, "hit_10pc_b": False, "hit_15pc_b": False}
    if not next_bar or len(fwd_bars) == 0:
        return out

    # Method A — blind MOO
    em_a_entry = next_bar["open"]
    out["em_a_filled"] = True
    out["em_a_entry"]  = em_a_entry
    if em_a_entry > 0:
        out["ret_1d_a"] = (next_bar["close"] / em_a_entry) - 1
        if len(fwd_bars) >= FORWARD_DAYS:
            out["ret_5d_a"] = (fwd_bars[-1]["close"] / em_a_entry) - 1
            out["mfe_5d_a"] = max(b["high"] for b in fwd_bars) / em_a_entry - 1
            out["mae_5d_a"] = min(b["low"]  for b in fwd_bars) / em_a_entry - 1
            out["hit_5pc_a"]  = out["mfe_5d_a"] >= 0.05
            out["hit_10pc_a"] = out["mfe_5d_a"] >= 0.10
            out["hit_15pc_a"] = out["mfe_5d_a"] >= 0.15

    # Method B — two-stage buy-stop
    buy_stop = today["high"] * 1.003
    # Confirmation by 09:30: cumulative first-15m vol >= 1.5x median(prior-10d first-15m)
    next_d = next_bar["trade_date"]
    today_d_idx = days_sorted.index(today["trade_date"]) if today["trade_date"] in days_sorted else -1
    prior_first15 = []
    if today_d_idx >= 10:
        prior_first15 = [first15_by_day.get(d, 0) for d in days_sorted[today_d_idx - 10: today_d_idx]]
        prior_first15 = [v for v in prior_first15 if v > 0]
    f15_today_next = first15_by_day.get(next_d, 0)
    if not prior_first15:
        out["em_b_cancel_reason"] = "no_baseline_first15"
        return out
    base_first15 = statistics.median(prior_first15)
    if f15_today_next < CONFIRM_FIRST15_MULT * base_first15:
        out["em_b_cancel_reason"] = "first15_volume_below_threshold"
        return out
    # Volume confirmed — fill at buy_stop or open (whichever higher)
    em_b_entry = max(buy_stop, next_bar["open"])
    if next_bar["high"] < buy_stop:
        # Stop never triggered intraday on next day
        out["em_b_cancel_reason"] = "stop_not_triggered"
        return out

    out["em_b_filled"] = True
    out["em_b_entry"]  = em_b_entry
    if em_b_entry > 0:
        out["ret_1d_b"] = (next_bar["close"] / em_b_entry) - 1
        if len(fwd_bars) >= FORWARD_DAYS:
            out["ret_5d_b"] = (fwd_bars[-1]["close"] / em_b_entry) - 1
            out["mfe_5d_b"] = max(b["high"] for b in fwd_bars) / em_b_entry - 1
            out["mae_5d_b"] = min(b["low"]  for b in fwd_bars) / em_b_entry - 1
            out["hit_5pc_b"]  = out["mfe_5d_b"] >= 0.05
            out["hit_10pc_b"] = out["mfe_5d_b"] >= 0.10
            out["hit_15pc_b"] = out["mfe_5d_b"] >= 0.15
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Top-level driver
# ─────────────────────────────────────────────────────────────────────────────

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
            if done % 25 == 0:
                print(f"  [{done}/{len(args_list)}] symbols scanned, signals so far={len(out)}", flush=True)
    return out
