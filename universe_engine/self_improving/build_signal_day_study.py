#!/usr/bin/env python3
"""build_signal_day_study.py — S2B: per-signal-day efficacy study.

A NEW study that is DIFFERENT from build_baseline.py.

  build_baseline.py  = managed-portfolio sim (fixed ₹5L/yr, cash-constrained,
                       skip_already_held). One wallet per year; a held symbol is
                       NOT re-entered while open. → falcon_baseline_trades.

  THIS SCRIPT        = per-signal-day efficacy. Take the engine's top-10 picks on
                       EVERY signal day and simulate EACH pick INDEPENDENTLY with
                       REPLICATED capital (₹50k per pick), NO skip_already_held,
                       NO cash constraint. The same stock can appear on many
                       signal days → many independent rows (duplicates expected).
                       It answers "if a customer acts on a pick on ANY given day,
                       what happens to that stock + its outcome", and tracks what
                       happens AFTER our exit (D+8 → D+60). → falcon_signal_day_study.

────────────────────────────────────────────────────────────────────────────
SELECTION (same as Falcon Top 10, MINUS skip/cash):
  For each year Y (2021→latest) we run the IDENTICAL eligibility + signal
  machinery the parity Falcon Top 10 uses:
    elig = eligible_patterns_for_year(all_pats, Y)            (core:169)
    sigs = compute_year_signals(..., min_fires=run_cfg.min_fires)  (core:181)
    if sort_key == "avg_lift": s["score"] = s["avg_lift"]     (sim:462-464)
    sigs_by_sd = group sigs by signal_date                    (sim:465-467)
  Then, PER signal_date, we sort the day's signals by score (= avg_lift) desc and
  take the TOP-N (default 10). Every one of those picks becomes ONE independent
  trade — we do NOT skip held symbols and do NOT apply any cash constraint. This
  is the same cohort the portfolio sim draws from; we simply act on all top-10
  every day instead of fitting them into a ₹5L wallet.

EXIT MECHANICS (reused — proven identical to parity, not re-implemented):
  The portfolio engine's per-position exit block lives in
  persona_engine_core.simulate_year (core:384-436): per trading day, mark
  high_water = close/entry_px-1; init_stop_lvl = entry_px*(1+init_stop);
  if trail armed (high_water>=trail_trigger) trail_stop = max(entry_px,
  min low over last trail_lookback bars), stop_lvl = max(init,trail); priority
  SL→TARGET→TIME; gap-down honored via exit_px_raw = min(stop_lvl, day_open);
  exit_px = exit_px_raw*(1-SLIP); gross = shares*(exit_px-entry_px);
  fees = actual_deployed*FEE; net = gross-fees. Entry (core:480-523):
  ep = open*(1+SLIP) (entry_multiplier=1 for the daily persona), shares =
  floor(₹50k/ep), actual_deployed = shares*ep.
  We re-run THAT EXACT arithmetic, per pick, in `simulate_independent_pick`
  below — copied line-for-line from core:384-436 with the multi-position cash/
  held-symbol bookkeeping stripped out (a single position never competes for
  cash and is never skipped). Every constant (SLIP, FEE) and every comparison is
  imported from / mirrors persona_engine_core; NO exit rule is invented.

  PARITY CROSS-CHECK proves the replication is faithful: for any (signal_date,
  symbol) that ALSO exists in falcon_baseline_trades (persona='falcon_top10'),
  we assert entry_price / exit_price / exit_reason / net_ret_pct match EXACTLY.
  Mismatches = a mechanics-divergence bug → must be 0.

POST-EXIT "what happened next" (the key addition), D+8..D+60:
  Measured as close/entry_px-1 (consistent with the hold-journey base = the
  entry bar's unslipped open). For target offset k (8,10,15,20,30,45,60) we take
  the bar at entry_bar_idx + (k-1) if it EXISTS; otherwise NULL (the future bar
  has not arrived yet — NEVER imputed). post_hold_high_ret = max close_ret over
  the AVAILABLE bars in D+8..D+60; post_hold_peak_day = that D. early_exit_flag =
  1 if post_hold_high_ret > 15% (the stock kept running after we exited).
  kept_running_d30 = 1 if d30_close_ret > net_ret_pct (NULL if either is NULL).

CLI:
  python build_signal_day_study.py --rnd-db <path>             # apply (rebuild)
  python build_signal_day_study.py --rnd-db <path> --dry-run   # compute, no write
  python build_signal_day_study.py --rnd-db <path> --years 2021,2022
  python build_signal_day_study.py --rnd-db <path> --top-n 10 --out out

Constraints honored: additive, idempotent (DELETE persona then INSERT in one
transaction), RND-only writes, no PROD / shared-engine-code mutation (INV2).
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

# Windows consoles default to cp1252, which can't encode the box-drawing chars
# in the summary print. Force utf-8 so output never crashes the run.
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# ── Import the PRODUCTION engine (reuse only — never modified). ──────────────
# Mirror backend/main.py's import root exactly (see build_baseline.py header).
_HERE = Path(__file__).resolve().parent
_REPO_ROOT = _HERE.parent.parent
_BACKEND_ROOT = _REPO_ROOT / "backend"
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

from power_user.services.persona_simulator import (  # noqa: E402
    PERSONA_CONFIGS,
    _resolve_rnd_db_path,
    PROD_DB,
)
from power_user.services.persona_engine_core import (  # noqa: E402
    build_sector_map,
    compute_year_signals,
    eligible_patterns_for_year,
    load_all_bars,
    load_full_patterns,
    load_panel,
    simulate_year,
    trading_days,
    SLIP,        # = 5 bps / 1e4  (entry + exit slippage)  — reused, not redefined
    FEE,         # = 30 bps / 1e4 (round-trip fee on actual_deployed) — reused
)
import math  # noqa: E402  (engine uses math.floor for integer shares)

PERSONA_SLUG = "falcon-top-10"
STUDY_PERSONA_TAG = "falcon_top10_daily"   # this study's persona value
BASELINE_PERSONA_TAG = "falcon_top10"      # the portfolio baseline (for parity x-check)

# Table-8 Falcon Top 10 thresholds (identical to build_baseline.py).
BIG_WINNER_PEAK_PCT = 12.0
BIG_LOSER_TROUGH_PCT = -7.0
BIG_LOSER_INIT_STOP = "INIT_STOP"
PEAK_SUSTAINED_FRAC = 0.9

# Post-exit "what happened next" — D offsets relative to the entry bar.
POST_OFFSETS = [8, 10, 15, 20, 30, 45, 60]
EARLY_EXIT_THRESHOLD_PCT = 15.0   # post_hold_high_ret > 15% => stock kept running


# ════════════════════════════════════════════════════════════════════════════
# 1. SHARED RUN CONTEXT (identical loaders to the parity Falcon Top 10 run)
# ════════════════════════════════════════════════════════════════════════════

def _build_run_context(rnd_db: str, prod_db: str) -> Dict[str, Any]:
    cfg_dict = PERSONA_CONFIGS[PERSONA_SLUG]
    run_cfg = cfg_dict["run_cfg"]
    sim_start = cfg_dict["sim_start"]
    sim_end = cfg_dict["sim_end"]
    cash_start = cfg_dict["cash_per_year"]

    all_pats = load_full_patterns(rnd_db)                 # core:61 (RND patterns)
    X, sym, dt = load_panel(prod_db, sim_start, sim_end)  # core:86 (PROD features)
    bars = load_all_bars(prod_db, "2020-12-01", sim_end)  # core:109 (pad for trail)
    # SECTOR SOURCE = RND DB (review fix): falcon_sectors read from RND, not PROD.
    # PROD's falcon_sectors is missing GUJGASLTD/LTIM/ZOMATO (-> null sector ->
    # null move_type); the RND copy holds the backfilled mappings
    # (fix_sector_backfill.py). OHLC/features/bars above still come from PROD.
    sector_map = build_sector_map(rnd_db)                 # core:140 (sectors from RND)
    all_td = trading_days(prod_db, sim_start, sim_end)    # core:127

    years_arr = np.array([int(d[:4]) for d in dt], dtype=np.int32)
    years_in_window = sorted({int(d[:4]) for d in all_td})

    return {
        "cfg_dict": cfg_dict, "run_cfg": run_cfg,
        "sim_start": sim_start, "sim_end": sim_end, "cash_start": cash_start,
        "all_pats": all_pats, "X": X, "sym": sym, "dt": dt, "bars": bars,
        "sector_map": sector_map, "all_td": all_td,
        "years_arr": years_arr, "years_in_window": years_in_window,
    }


def _year_signals_by_signal_date(ctx: Dict[str, Any], Y: int) -> Dict[str, List[Dict]]:
    """Reproduce the parity Falcon Top 10 signal generation for year Y, returning
    {signal_date -> [signal dicts]} with score == avg_lift (the Falcon ranker).
    Same as persona_simulator.py lines 451-467."""
    run_cfg = ctx["run_cfg"]
    all_pats = ctx["all_pats"]
    X, sym, dt = ctx["X"], ctx["sym"], ctx["dt"]
    years_arr = ctx["years_arr"]

    elig = eligible_patterns_for_year(all_pats, Y)               # core:169
    year_mask = years_arr == Y
    sigs = compute_year_signals(                                  # core:181
        X, sym, dt, year_mask, elig, min_fires=run_cfg.min_fires,
    )
    if run_cfg.sort_key == "avg_lift":                           # sim:462-464
        for s in sigs:
            s["score"] = s["avg_lift"]
    sigs_by_sd: Dict[str, List[Dict]] = defaultdict(list)        # sim:465-467
    for s in sigs:
        sigs_by_sd[s["signal_date"]].append(s)
    return sigs_by_sd


# ════════════════════════════════════════════════════════════════════════════
# 2. INDEPENDENT PER-PICK SIMULATION
#    Exit mechanics copied LINE-FOR-LINE from persona_engine_core.simulate_year
#    (core:384-436) with multi-position cash/held bookkeeping removed. A single
#    position never competes for cash and is never skipped → no cash gate, no
#    held_syms check. Everything else (entry px, integer shares, high_water,
#    init/trail stop, gap-down, priority SL→TARGET→TIME, slippage, fees) is the
#    SAME arithmetic. Parity is proven by the cross-check against the baseline.
# ════════════════════════════════════════════════════════════════════════════

def simulate_independent_pick(
    sig: Dict[str, Any],
    bars_sym: List[Dict],
    sd_idx: int,
    run_cfg,
) -> Optional[Dict[str, Any]]:
    """Simulate ONE pick independently. Returns a trade dict shaped like
    simulate_year's closed-trade dict, or None if the pick is unenterable.

    sd_idx == the index into bars_sym of the FIRST bar with date > signal_date
    (the next-open entry bar) — identical to simulate_year's `_bars_start_idx`
    (core:354).
    """
    # ── ENTRY (mirror core:483-523) ──
    if sd_idx >= len(bars_sym):
        return None
    ep_raw = bars_sym[sd_idx]["open"]
    if ep_raw <= 0:
        return None
    # entry_multiplier = 1.0 for the daily persona (no intraday filter). core:474,491
    ep = ep_raw * 1.0 * (1 + SLIP)
    allocated = run_cfg.fixed_per_trade                    # ₹50,000 for Falcon Top 10
    shares = math.floor(allocated / ep)                    # integer shares, core:495
    if shares < 1:
        return None                                        # qty<1 (price > allocated)
    actual_deployed = shares * ep                          # core:500
    entry_bar_idx = sd_idx
    # time_exit_idx: clamp to last available bar (core:517-518). split_15_mode is
    # never set for the daily persona, so the natural-hold branch always applies.
    time_exit_idx = min(entry_bar_idx + run_cfg.hold_days - 1, len(bars_sym) - 1)
    time_exit_date = bars_sym[time_exit_idx]["date"]

    high_water = 0.0
    # ── PER-DAY EXIT WALK (mirror core:384-439) ──
    # CRITICAL parity detail: in simulate_year a position is ADDED to open_pos in
    # step 2 of the entry day (core:441-525), AFTER step 1's exit checks already
    # ran for that day (core:384-439). So the ENTRY-DAY bar is NEVER evaluated for
    # an exit — the first exit check is the NEXT trading day (bar_idx =
    # entry_bar_idx + 1). We therefore start the walk at entry_bar_idx + 1.
    # Iterating the symbol's own bars in order is equivalent to simulate_year
    # iterating td_list and skipping days where `d not in idx_map` (core:391-393):
    # both visit exactly this symbol's bars after entry, in date order.
    last_idx = len(bars_sym) - 1
    for bar_idx in range(entry_bar_idx + 1, last_idx + 1):
        day_bar = bars_sym[bar_idx]
        cur_ret = day_bar["close"] / ep - 1               # core:396
        if cur_ret > high_water:
            high_water = cur_ret

        init_stop_lvl = ep * (1 + run_cfg.init_stop)      # core:400
        stop_lvl = init_stop_lvl
        armed = False
        if run_cfg.trail_trigger is not None:             # core:403-408
            armed = high_water >= run_cfg.trail_trigger
            if armed:
                window = bars_sym[max(0, bar_idx - run_cfg.trail_lookback + 1): bar_idx + 1]
                trail_stop = max(ep, min(b["low"] for b in window))
                stop_lvl = max(init_stop_lvl, trail_stop)
        target_lvl = (ep * (1 + run_cfg.target)           # core:409-410
                      if run_cfg.target is not None else None)

        exit_reason = None
        exit_px_raw = None
        if day_bar["low"] <= stop_lvl:                    # core:415-417
            exit_reason = "TRAIL_GIVEBACK" if armed else "INIT_STOP"
            exit_px_raw = min(stop_lvl, day_bar["open"])  # gap-down honored
        elif target_lvl is not None and day_bar["high"] >= target_lvl:  # core:418-420
            exit_reason = "TARGET"
            exit_px_raw = max(target_lvl, day_bar["open"])
        elif day_bar["date"] == time_exit_date:           # core:421-423
            exit_reason = "TIME_STOP"
            exit_px_raw = day_bar["close"]

        if exit_reason:
            exit_px = exit_px_raw * (1 - SLIP)            # core:426
            gross_pnl = shares * (exit_px - ep)           # core:427
            fees = actual_deployed * FEE                  # core:428
            net_pnl = gross_pnl - fees                    # core:429
            trading_hold = bar_idx - entry_bar_idx + 1    # core:432
            return {
                "symbol": sig["symbol"],
                "signal_date": sig["signal_date"],
                "entry_date": bars_sym[entry_bar_idx]["date"],
                "exit_date": day_bar["date"],
                "score": sig["score"], "n_fires": sig["n_fires"],
                "entry_px": ep, "entry_bar_idx": entry_bar_idx,
                "shares": shares, "actual_deployed": actual_deployed,
                "exit_px": exit_px, "gross_pnl": gross_pnl, "fees": fees,
                "net_pnl": net_pnl, "exit_reason": exit_reason,
                "trading_hold_days": trading_hold, "high_water": high_water,
            }

    # No exit triggered within available bars — the position is still open at the
    # end of the loaded bar series (e.g. a recent pick whose 7-day hold hasn't
    # fully elapsed in data). Treat as OPEN_AT_END (MTM at last bar), mirroring
    # simulate_year's year-end MTM (core:548-574) but at the data edge.
    bar = bars_sym[last_idx]
    mtm_px = bar["close"] * (1 - SLIP)                    # core:563
    gross_pnl = shares * (mtm_px - ep)
    trading_hold = last_idx - entry_bar_idx + 1
    return {
        "symbol": sig["symbol"], "signal_date": sig["signal_date"],
        "entry_date": bars_sym[entry_bar_idx]["date"],
        "exit_date": None,                                # not actually closed
        "score": sig["score"], "n_fires": sig["n_fires"],
        "entry_px": ep, "entry_bar_idx": entry_bar_idx,
        "shares": shares, "actual_deployed": actual_deployed,
        "exit_px": None, "gross_pnl": gross_pnl, "fees": None,
        "net_pnl": None, "exit_reason": "OPEN_AT_BACKTEST_END_MTM",
        "trading_hold_days": trading_hold, "high_water": high_water,
        "_is_open": True,
    }


# ════════════════════════════════════════════════════════════════════════════
# 3. INTRA-HOLD JOURNEY (identical definitions to build_baseline.compute_journey)
# ════════════════════════════════════════════════════════════════════════════

def compute_journey(trade: Dict[str, Any], bars_sym: List[Dict], hold_days: int) -> Dict[str, Any]:
    eidx = trade["entry_bar_idx"]
    out: Dict[str, Any] = {}
    if eidx is None or eidx >= len(bars_sym):
        return out
    entry_open = bars_sym[eidx]["open"]
    if not entry_open or entry_open <= 0:
        return out

    peak_ret = peak_day = trough_ret = trough_day = None
    last_close_ret = None
    n_days = min(hold_days, len(bars_sym) - eidx)
    for d in range(1, n_days + 1):
        bar = bars_sym[eidx + (d - 1)]
        o_ret = (bar["open"] / entry_open - 1.0) * 100.0
        h_ret = (bar["high"] / entry_open - 1.0) * 100.0
        l_ret = (bar["low"] / entry_open - 1.0) * 100.0
        c_ret = (bar["close"] / entry_open - 1.0) * 100.0
        out[f"d{d}_open_ret"] = o_ret
        out[f"d{d}_high_ret"] = h_ret
        out[f"d{d}_low_ret"] = l_ret
        out[f"d{d}_close_ret"] = c_ret
        if peak_ret is None or h_ret > peak_ret:
            peak_ret, peak_day = h_ret, d
        if trough_ret is None or l_ret < trough_ret:
            trough_ret, trough_day = l_ret, d
        last_close_ret = c_ret

    out["peak_ret_during_hold"] = peak_ret
    out["peak_day_during_hold"] = peak_day
    out["trough_ret_during_hold"] = trough_ret
    out["trough_day_during_hold"] = trough_day
    if peak_day is not None and trough_day is not None:
        out["peak_before_trough"] = 1 if peak_day < trough_day else 0
        out["trough_before_peak"] = 1 if trough_day < peak_day else 0
    else:
        out["peak_before_trough"] = None
        out["trough_before_peak"] = None
    if peak_ret is not None and last_close_ret is not None:
        if peak_ret <= 0:
            out["peak_sustained"] = 1 if last_close_ret >= peak_ret else 0
        else:
            out["peak_sustained"] = 1 if last_close_ret >= PEAK_SUSTAINED_FRAC * peak_ret else 0
    else:
        out["peak_sustained"] = None

    exit_reason = trade.get("exit_reason")
    big_winner = (peak_ret is not None and peak_ret > BIG_WINNER_PEAK_PCT)
    big_loser_trough = (trough_ret is not None and trough_ret < BIG_LOSER_TROUGH_PCT)
    big_loser = big_loser_trough or (exit_reason == BIG_LOSER_INIT_STOP)
    out["big_winner_flag"] = 1 if big_winner else 0
    out["big_loser_flag"] = 1 if big_loser else 0
    return out


# ════════════════════════════════════════════════════════════════════════════
# 4. POST-EXIT "WHAT HAPPENED NEXT" (D+8..D+60) — NULL when bar not arrived
# ════════════════════════════════════════════════════════════════════════════

def compute_post_exit(
    trade: Dict[str, Any],
    bars_sym: List[Dict],
    net_ret_pct: Optional[float],
) -> Dict[str, Any]:
    """All returns are close/entry_px-1 (in %), base = entry bar's RAW open
    (same base as the intra-hold journey). A D+k value is NULL when that future
    bar does not exist (never imputed)."""
    out: Dict[str, Any] = {f"d{k}_close_ret": None for k in POST_OFFSETS}
    out["post_hold_high_ret"] = None
    out["post_hold_peak_day"] = None
    out["early_exit_flag"] = None
    out["kept_running_d30"] = None

    eidx = trade["entry_bar_idx"]
    if eidx is None or eidx >= len(bars_sym):
        return out
    entry_open = bars_sym[eidx]["open"]
    if not entry_open or entry_open <= 0:
        return out

    # Individual D+k close returns (NULL where the bar hasn't arrived).
    for k in POST_OFFSETS:
        j = eidx + (k - 1)
        if j < len(bars_sym):
            out[f"d{k}_close_ret"] = (bars_sym[j]["close"] / entry_open - 1.0) * 100.0

    # post_hold_high_ret = max close_ret over EVERY available bar in D+8..D+60
    # (not just the 7 sampled offsets — the full window, so a peak between the
    # sampled days isn't missed). post_hold_peak_day = the D at which it occurred.
    hi_ret = None
    hi_day = None
    for k in range(8, 61):
        j = eidx + (k - 1)
        if j >= len(bars_sym):
            break
        c_ret = (bars_sym[j]["close"] / entry_open - 1.0) * 100.0
        if hi_ret is None or c_ret > hi_ret:
            hi_ret, hi_day = c_ret, k
    out["post_hold_high_ret"] = hi_ret
    out["post_hold_peak_day"] = hi_day
    if hi_ret is not None:
        out["early_exit_flag"] = 1 if hi_ret > EARLY_EXIT_THRESHOLD_PCT else 0

    d30 = out["d30_close_ret"]
    if d30 is not None and net_ret_pct is not None:
        out["kept_running_d30"] = 1 if d30 > net_ret_pct else 0

    return out


# ════════════════════════════════════════════════════════════════════════════
# 5. PRIOR APPEARANCES (dup context) — calendar-30d count of prior picks
# ════════════════════════════════════════════════════════════════════════════

def build_prior_appearances_index(
    picks: List[Tuple[str, str]],
) -> Dict[Tuple[str, str], int]:
    """picks = list of (signal_date, symbol) for EVERY top-10 pick across all
    years, in any order. Returns {(signal_date, symbol) -> # times this symbol
    was a pick in the prior 30 CALENDAR days (strictly before this signal_date)}.

    Duplicates of the same (signal_date, symbol) — which cannot happen here since
    a symbol appears at most once per signal_date in a cohort — would each see
    the same prior count. Counting is by symbol over the rolling 30-cal-day
    window ending the day before signal_date.
    """
    by_symbol: Dict[str, List[str]] = defaultdict(list)
    for sd, symbol in picks:
        by_symbol[symbol].append(sd)
    for symbol in by_symbol:
        by_symbol[symbol].sort()

    out: Dict[Tuple[str, str], int] = {}
    for sd, symbol in picks:
        d_this = datetime.fromisoformat(sd).date()
        lo = d_this - timedelta(days=30)
        cnt = 0
        for prior_sd in by_symbol[symbol]:
            d_prior = datetime.fromisoformat(prior_sd).date()
            if lo <= d_prior < d_this:
                cnt += 1
        out[(sd, symbol)] = cnt
    return out


# ════════════════════════════════════════════════════════════════════════════
# 6. ROW BUILDER
# ════════════════════════════════════════════════════════════════════════════

def build_row(
    trade: Dict[str, Any],
    rank: int,
    sector_map: Dict[str, str],
    bars_sym: List[Dict],
    hold_days: int,
    prior_30d: int,
) -> Dict[str, Any]:
    sym = trade["symbol"]
    entry_px = trade["entry_px"]
    n_fires = int(trade.get("n_fires", 0))
    avg_lift = float(trade.get("score", 0.0))          # Falcon Top 10 score == avg_lift
    sum_lift = avg_lift * n_fires if n_fires else None
    actual_deployed = trade.get("actual_deployed")
    net_pnl = trade.get("net_pnl")
    net_ret_pct = (
        (net_pnl / actual_deployed * 100.0)
        if (net_pnl is not None and actual_deployed) else None
    )

    journey = compute_journey(trade, bars_sym, hold_days)
    post = compute_post_exit(trade, bars_sym, net_ret_pct)

    row: Dict[str, Any] = {
        "persona": STUDY_PERSONA_TAG,
        "signal_date": trade["signal_date"],
        "entry_date": trade["entry_date"],
        "exit_date": trade.get("exit_date"),
        "symbol": sym,
        "sector": sector_map.get(sym),
        "engine_rank": rank,
        "avg_lift": avg_lift,
        "n_fires": n_fires,
        "sum_lift": sum_lift,
        "entry_price": entry_px,
        "exit_price": trade.get("exit_px"),
        "exit_reason": trade.get("exit_reason"),
        "hold_days_trading": int(trade.get("trading_hold_days", 0)),
        "shares": int(trade["shares"]),
        "net_pnl": net_pnl,
        "net_ret_pct": net_ret_pct,
        "prior_appearances_30d": prior_30d,
    }
    for d in range(1, 8):
        for f in ("open", "high", "low", "close"):
            row[f"d{d}_{f}_ret"] = journey.get(f"d{d}_{f}_ret")
    for k in ("peak_ret_during_hold", "peak_day_during_hold",
              "trough_ret_during_hold", "trough_day_during_hold",
              "peak_before_trough", "trough_before_peak", "peak_sustained",
              "big_winner_flag", "big_loser_flag"):
        row[k] = journey.get(k)
    for k in (["post_hold_high_ret", "post_hold_peak_day", "early_exit_flag",
               "kept_running_d30"] + [f"d{o}_close_ret" for o in POST_OFFSETS]):
        row[k] = post.get(k)
    return row


# INSERT column order (every column except id + created_at, which DB defaults).
_COLS = [
    "persona", "signal_date", "entry_date", "exit_date", "symbol", "sector",
    "engine_rank", "avg_lift", "n_fires", "sum_lift",
    "entry_price", "exit_price", "exit_reason", "hold_days_trading", "shares",
    "net_pnl", "net_ret_pct",
    "d1_open_ret", "d1_high_ret", "d1_low_ret", "d1_close_ret",
    "d2_open_ret", "d2_high_ret", "d2_low_ret", "d2_close_ret",
    "d3_open_ret", "d3_high_ret", "d3_low_ret", "d3_close_ret",
    "d4_open_ret", "d4_high_ret", "d4_low_ret", "d4_close_ret",
    "d5_open_ret", "d5_high_ret", "d5_low_ret", "d5_close_ret",
    "d6_open_ret", "d6_high_ret", "d6_low_ret", "d6_close_ret",
    "d7_open_ret", "d7_high_ret", "d7_low_ret", "d7_close_ret",
    "peak_ret_during_hold", "peak_day_during_hold",
    "trough_ret_during_hold", "trough_day_during_hold",
    "peak_before_trough", "trough_before_peak", "peak_sustained",
    "big_winner_flag", "big_loser_flag",
    "d8_close_ret", "d10_close_ret", "d15_close_ret", "d20_close_ret",
    "d30_close_ret", "d45_close_ret", "d60_close_ret",
    "post_hold_high_ret", "post_hold_peak_day", "early_exit_flag",
    "kept_running_d30",
    "prior_appearances_30d",
]


# ════════════════════════════════════════════════════════════════════════════
# 7. PARITY CROSS-CHECK against falcon_baseline_trades
# ════════════════════════════════════════════════════════════════════════════

def _load_baseline_trades(rnd_db: str) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """Map (signal_date, symbol) -> baseline closed-trade execution fields, for
    persona='falcon_top10'. Only CLOSED trades (exit_price NOT NULL) are
    comparable (open-at-end baseline rows store NULL exec). If the baseline table
    doesn't exist yet, return {} (cross-check becomes a no-op with a note)."""
    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        has = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='falcon_baseline_trades'"
        ).fetchone()
        if not has:
            return {}
        rows = con.execute(
            "SELECT signal_date, symbol, entry_price, exit_price, exit_reason, "
            "       net_ret_pct, hold_days_trading "
            "  FROM falcon_baseline_trades "
            " WHERE persona = ? AND exit_price IS NOT NULL",
            (BASELINE_PERSONA_TAG,),
        ).fetchall()
    finally:
        con.close()
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for sd, sym, ep, xp, xr, nr, hd in rows:
        out[(sd, sym)] = {
            "entry_price": ep, "exit_price": xp,
            "exit_reason": xr, "net_ret_pct": nr, "hold_days_trading": hd,
        }
    return out


def _approx(a: Optional[float], b: Optional[float], tol: float = 1e-6) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(a - b) <= tol * max(1.0, abs(a), abs(b))


def parity_cross_check(
    study_rows: List[Dict[str, Any]],
    baseline: Dict[Tuple[str, str], Dict[str, Any]],
    hold: int = 7,
) -> Tuple[int, List[str], List[str]]:
    """For every (signal_date, symbol) shared with the baseline, assert the
    study's CLOSED-trade execution matches the baseline. Returns
    (n_matched, data_edge_msgs, true_mismatch_msgs).

    Mechanics are identical, so any difference can ONLY come from the baseline
    having been built against an OLDER OHLC window: a recent baseline trade whose
    7-day hold ran past the data edge was TIME_STOP-truncated early
    (hold_days_trading < hold). The study, run later with more bars, extends that
    same hold → same entry, later exit. We classify those as DATA_EDGE (expected,
    not a bug). A TRUE mismatch is a difference on a fully-resolved baseline trade
    (full hold, or a stop/trail exit) — that MUST be 0 (real mechanics divergence).

    Only closed study rows are compared, once per (signal_date, symbol)."""
    matched = 0
    data_edge: List[str] = []
    mismatches: List[str] = []
    seen: set = set()
    for r in study_rows:
        if r.get("exit_price") is None:
            continue  # open-at-end study row — no closed baseline counterpart
        key = (r["signal_date"], r["symbol"])
        if key not in baseline or key in seen:
            continue
        seen.add(key)
        b = baseline[key]
        ok = (
            _approx(r["entry_price"], b["entry_price"])
            and _approx(r["exit_price"], b["exit_price"])
            and (r["exit_reason"] == b["exit_reason"])
            and _approx(r["net_ret_pct"], b["net_ret_pct"])
        )
        if ok:
            matched += 1
            continue
        # Differs — is it explained by the baseline being truncated at the data
        # edge? Entry must still match exactly (entry mechanics never differ), and
        # the baseline trade must be a short TIME_STOP (hold cut below `hold`).
        b_truncated = (
            b.get("exit_reason") == "TIME_STOP"
            and (b.get("hold_days_trading") or hold) < hold
        )
        msg = (
            f"{key}: study(ep={r['entry_price']}, xp={r['exit_price']}, "
            f"xr={r['exit_reason']}, nr={r['net_ret_pct']}) vs "
            f"baseline(ep={b['entry_price']}, xp={b['exit_price']}, "
            f"xr={b['exit_reason']}, nr={b['net_ret_pct']}, "
            f"hold={b.get('hold_days_trading')})"
        )
        if _approx(r["entry_price"], b["entry_price"]) and b_truncated:
            data_edge.append(msg)   # expected: study has fresher OHLC
        else:
            mismatches.append(msg)  # real mechanics divergence
    return matched, data_edge, mismatches


# ════════════════════════════════════════════════════════════════════════════
# 8. EFFICACY STATS (per-year + overall)
# ════════════════════════════════════════════════════════════════════════════

def _median(vals: List[float]) -> Optional[float]:
    v = sorted(vals)
    n = len(v)
    if n == 0:
        return None
    if n % 2:
        return v[n // 2]
    return (v[n // 2 - 1] + v[n // 2]) / 2.0


def _rate(num: int, den: int) -> Optional[float]:
    return (num / den * 100.0) if den else None


def compute_efficacy(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Headline every-day efficacy on CLOSED study rows (net_ret_pct not None)."""
    closed = [r for r in rows if r.get("net_ret_pct") is not None]
    n = len(closed)
    if n == 0:
        return {"n_picks": 0}
    rets = [r["net_ret_pct"] for r in closed]
    n_win = sum(1 for x in rets if x > 0)
    n_bw = sum(1 for r in closed if r.get("big_winner_flag") == 1)
    n_bl = sum(1 for r in closed if r.get("big_loser_flag") == 1)
    phh = [r["post_hold_high_ret"] for r in closed if r.get("post_hold_high_ret") is not None]
    n_ee_den = sum(1 for r in closed if r.get("early_exit_flag") is not None)
    n_ee = sum(1 for r in closed if r.get("early_exit_flag") == 1)
    return {
        "n_picks": n,
        "win_rate": _rate(n_win, n),
        "avg_ret": sum(rets) / n,
        "median_ret": _median(rets),
        "big_winner_rate": _rate(n_bw, n),
        "big_loser_rate": _rate(n_bl, n),
        "avg_post_hold_high_ret": (sum(phh) / len(phh)) if phh else None,
        "early_exit_rate": _rate(n_ee, n_ee_den) if n_ee_den else None,
    }


# ════════════════════════════════════════════════════════════════════════════
# 9. EXCEL / CSV OUTPUT
# ════════════════════════════════════════════════════════════════════════════

def write_excel(
    out_dir: Path,
    rows: List[Dict[str, Any]],
    per_year: List[Dict[str, Any]],
    overall: Dict[str, Any],
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    xlsx_path = out_dir / "falcon_signal_day_study.xlsx"
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font
    except Exception:
        return _write_csv_fallback(out_dir, rows, per_year, overall)

    wb = Workbook()

    # Sheet 1 — All Signal-Day Trades (every pick).
    ws1 = wb.active
    ws1.title = "All Signal-Day Trades"
    ws1.append(_COLS)
    for c in ws1[1]:
        c.font = Font(bold=True)
    for r in rows:
        ws1.append([r.get(col) for col in _COLS])

    # Sheet 2 — Per-Year Efficacy.
    ws2 = wb.create_sheet("Per-Year Efficacy")
    py_cols = ["year", "n_picks", "win_rate", "avg_ret", "median_ret",
               "big_winner_rate", "big_loser_rate", "avg_post_hold_high_ret",
               "early_exit_rate"]
    ws2.append(py_cols)
    for c in ws2[1]:
        c.font = Font(bold=True)
    for yr in per_year:
        ws2.append([yr.get(col) for col in py_cols])

    # Sheet 3 — Overall headline efficacy.
    ws3 = wb.create_sheet("Overall")
    ws3.append(["metric", "value"])
    for c in ws3[1]:
        c.font = Font(bold=True)
    for k in ["n_picks", "win_rate", "avg_ret", "median_ret", "big_winner_rate",
              "big_loser_rate", "avg_post_hold_high_ret", "early_exit_rate"]:
        ws3.append([k, overall.get(k)])

    wb.save(xlsx_path)
    return xlsx_path


def _write_csv_fallback(
    out_dir: Path,
    rows: List[Dict[str, Any]],
    per_year: List[Dict[str, Any]],
    overall: Dict[str, Any],
) -> Path:
    import csv
    trades_path = out_dir / "falcon_signal_day_study_trades.csv"
    with open(trades_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(_COLS)
        for r in rows:
            w.writerow([r.get(col) for col in _COLS])
    py_path = out_dir / "falcon_signal_day_study_per_year.csv"
    py_cols = ["year", "n_picks", "win_rate", "avg_ret", "median_ret",
               "big_winner_rate", "big_loser_rate", "avg_post_hold_high_ret",
               "early_exit_rate"]
    with open(py_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(py_cols)
        for yr in per_year:
            w.writerow([yr.get(col) for col in py_cols])
    overall_path = out_dir / "falcon_signal_day_study_overall.csv"
    with open(overall_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["metric", "value"])
        for k in ["n_picks", "win_rate", "avg_ret", "median_ret",
                  "big_winner_rate", "big_loser_rate", "avg_post_hold_high_ret",
                  "early_exit_rate"]:
            w.writerow([k, overall.get(k)])
    print(f"[signal_day_study] openpyxl missing — wrote CSV fallback: "
          f"{trades_path.name}, {py_path.name}, {overall_path.name}")
    return trades_path


# ════════════════════════════════════════════════════════════════════════════
# 10. SCHEMA APPLY (CREATE IF NOT EXISTS, before any write)
# ════════════════════════════════════════════════════════════════════════════

def apply_schema(rnd_db: str) -> None:
    sql_path = _HERE / "schema_signal_day_study.sql"
    ddl = sql_path.read_text(encoding="utf-8")
    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        con.executescript(ddl)
        con.commit()
    finally:
        con.close()


# ════════════════════════════════════════════════════════════════════════════
# 11. WRITE (idempotent, single transaction, RND-only)
# ════════════════════════════════════════════════════════════════════════════

def _write(rnd_db: str, rows: List[Dict[str, Any]]) -> None:
    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        con.execute("PRAGMA foreign_keys=ON")
        con.execute("BEGIN")
        con.execute(
            "DELETE FROM falcon_signal_day_study WHERE persona = ?",
            (STUDY_PERSONA_TAG,),
        )
        placeholders = ", ".join("?" for _ in _COLS)
        sql = (f"INSERT INTO falcon_signal_day_study ({', '.join(_COLS)}) "
               f"VALUES ({placeholders})")
        con.executemany(sql, [[r.get(c) for c in _COLS] for r in rows])
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


# ════════════════════════════════════════════════════════════════════════════
# 12. ORCHESTRATION
# ════════════════════════════════════════════════════════════════════════════

def run(
    rnd_db: str,
    prod_db: str,
    years: Optional[List[int]],
    top_n: int,
    dry_run: bool,
    out_dir: Path,
) -> int:
    print(f"[signal_day_study] RND DB: {rnd_db}")
    print(f"[signal_day_study] PROD DB: {prod_db}")
    print(f"[signal_day_study] persona: {STUDY_PERSONA_TAG} (top-{top_n}/signal_date, "
          f"NO skip_already_held, NO cash constraint)")
    print(f"[signal_day_study] mode: {'DRY-RUN (no writes)' if dry_run else 'APPLY'}")

    ctx = _build_run_context(rnd_db, prod_db)
    run_cfg = ctx["run_cfg"]
    hold_days = run_cfg.hold_days
    bars = ctx["bars"]
    sector_map = ctx["sector_map"]
    X = ctx["X"]
    print(f"[signal_day_study] loaded {len(ctx['all_pats'])} patterns, "
          f"{X.shape[0]} panel rows, {len(bars)} symbols, "
          f"{len(ctx['all_td'])} trading days. "
          f"fixed_per_trade=₹{run_cfg.fixed_per_trade:,.0f}")

    candidate_years = ctx["years_in_window"]
    if years:
        candidate_years = [y for y in candidate_years if y in set(years)]
    print(f"[signal_day_study] years to run: {candidate_years}")

    # ── PASS 1: build every top-N pick (trade) across all years ──
    # Each entry: (year, rank, trade_dict). bars_sym is bars[symbol].
    all_trades: List[Tuple[int, int, Dict[str, Any]]] = []
    all_picks_keys: List[Tuple[str, str]] = []   # (signal_date, symbol) for prior-30d
    for Y in candidate_years:
        sigs_by_sd = _year_signals_by_signal_date(ctx, Y)
        # Restrict each year's picks to entries whose entry_date falls in the
        # persona's year window (matches simulate_year's year_td gating). We let
        # the entry bar fall naturally (next open after signal_date); a signal at
        # year-end whose entry is in the next year is still attributed to its
        # signal_date's year here — same cohort the engine produced it in.
        n_picks_year = 0
        for sd, group in sigs_by_sd.items():
            # Top-N per signal_date by score (= avg_lift) desc — SAME selection
            # as Falcon Top 10, just without skip/cash. Stable sort preserves
            # the engine's tie order.
            ranked = sorted(group, key=lambda s: s["score"], reverse=True)[:top_n]
            for pos, sig in enumerate(ranked, start=1):
                sym = sig["symbol"]
                bs = bars.get(sym)
                if not bs:
                    continue
                # next-open entry bar index (core:354).
                sd_idx = next((i for i, b in enumerate(bs) if b["date"] > sd), None)
                if sd_idx is None:
                    continue
                trade = simulate_independent_pick(sig, bs, sd_idx, run_cfg)
                if trade is None:
                    continue
                all_trades.append((Y, pos, trade))
                all_picks_keys.append((sd, sym))
                n_picks_year += 1
        print(f"[signal_day_study]   {Y}: {n_picks_year} independent picks")

    # ── prior_appearances_30d index over ALL picks (cross-year safe) ──
    prior_idx = build_prior_appearances_index(all_picks_keys)

    # ── PASS 2: build rows ──
    rows: List[Dict[str, Any]] = []
    rows_by_year: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for (Y, rank, trade) in all_trades:
        sym = trade["symbol"]
        bs = bars[sym]
        prior_30d = prior_idx.get((trade["signal_date"], sym), 0)
        row = build_row(trade, rank, sector_map, bs, hold_days, prior_30d)
        rows.append(row)
        rows_by_year[Y].append(row)

    # ── Efficacy stats ──
    per_year: List[Dict[str, Any]] = []
    for Y in sorted(rows_by_year):
        st = compute_efficacy(rows_by_year[Y])
        st["year"] = Y
        per_year.append(st)
    overall = compute_efficacy(rows)

    # ── Print per-year efficacy ──
    print("\n[signal_day_study] ── PER-YEAR EFFICACY (every-day, independent) ──")
    hdr = (f"  {'Year':>5} {'Picks':>6} {'Win%':>7} {'AvgRet':>8} {'MedRet':>8} "
           f"{'BigW%':>7} {'BigL%':>7} {'PostHi':>8} {'EarlyX%':>8}")
    print(hdr)
    for st in per_year:
        def f(x, p="{:.2f}"):
            return p.format(x) if x is not None else "—"
        print(f"  {st['year']:>5} {st['n_picks']:>6} {f(st['win_rate']):>7} "
              f"{f(st['avg_ret']):>8} {f(st['median_ret']):>8} "
              f"{f(st['big_winner_rate']):>7} {f(st['big_loser_rate']):>7} "
              f"{f(st['avg_post_hold_high_ret']):>8} {f(st['early_exit_rate']):>8}")

    print("\n[signal_day_study] ── OVERALL (headline every-day efficacy) ──")
    for k in ["n_picks", "win_rate", "avg_ret", "median_ret", "big_winner_rate",
              "big_loser_rate", "avg_post_hold_high_ret", "early_exit_rate"]:
        v = overall.get(k)
        print(f"    {k:>24}: {v if v is not None else '—'}")

    # ── PARITY CROSS-CHECK ──
    baseline = _load_baseline_trades(rnd_db)
    if not baseline:
        print("\n[signal_day_study] PARITY CROSS-CHECK: falcon_baseline_trades "
              "absent or empty for persona 'falcon_top10' — SKIPPED (run "
              "build_baseline.py first to enable this check).")
    else:
        matched, data_edge, mismatches = parity_cross_check(rows, baseline)
        print(f"\n[signal_day_study] PARITY CROSS-CHECK vs falcon_baseline_trades "
              f"('{BASELINE_PERSONA_TAG}'):")
        print(f"    shared closed (signal_date, symbol) matched EXACTLY: {matched}")
        print(f"    data-edge (baseline TIME_STOP truncated at old data edge; "
              f"study extends with fresher OHLC — EXPECTED): {len(data_edge)}")
        for m in data_edge[:20]:
            print(f"      ~ {m}")
        if mismatches:
            print(f"    *** TRUE MISMATCHES: {len(mismatches)} (MUST be 0 — mechanics "
                  f"divergence bug) ***")
            for m in mismatches[:20]:
                print(f"      ! {m}")
            if len(mismatches) > 20:
                print(f"      ... and {len(mismatches) - 20} more")
        else:
            print("    true mismatches: 0  → independent mechanics are identical "
                  "to the portfolio engine on all fully-resolved shared trades.")

    n_open = sum(1 for r in rows if r.get("exit_price") is None)
    print(f"\n[signal_day_study] total rows: {len(rows)} "
          f"({len(rows) - n_open} closed, {n_open} open-at-data-edge)")

    if dry_run:
        print("[signal_day_study] DRY-RUN — nothing written (no DB, no Excel).")
        return 0

    # ── APPLY: schema + write + Excel ──
    apply_schema(rnd_db)
    _write(rnd_db, rows)
    xlsx = write_excel(out_dir, rows, per_year, overall)
    print(f"[signal_day_study] APPLY complete — wrote {len(rows)} rows to "
          f"falcon_signal_day_study (persona='{STUDY_PERSONA_TAG}').")
    print(f"[signal_day_study] workbook: {xlsx}")
    return 0


# ════════════════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════════════════

def _parse_years(s: Optional[str]) -> Optional[List[int]]:
    if not s:
        return None
    out: List[int] = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part and ".." not in part:
            a, b = part.split("-", 1)
            out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    return out


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Per-signal-day efficacy study: top-N picks every signal day, "
                    "each simulated independently (replicated ₹50k, no skip, no "
                    "cash constraint), plus post-exit D+8..D+60 tracking."
    )
    p.add_argument("--rnd-db", default=None, required=False,
                   help="Path to the RND research DB (default: persona resolver).")
    p.add_argument("--prod-db", default=None,
                   help="Path to the PROD DB for OHLC/features (default: config.POWER_DB_PATH).")
    p.add_argument("--years", default=None,
                   help="Comma/range list, e.g. '2021,2022' or '2021-2023'. "
                        "Default: all years in the persona window (2021..current).")
    p.add_argument("--top-n", type=int, default=10,
                   help="Picks per signal_date (default: 10 = Falcon Top 10).")
    p.add_argument("--dry-run", action="store_true",
                   help="Compute + print per-year efficacy + parity cross-check; "
                        "write NOTHING (no DB, no Excel).")
    p.add_argument("--out", default=str(_HERE / "out"),
                   help="Output directory for the workbook (default: ./out).")
    args = p.parse_args(argv)

    rnd_db = args.rnd_db or _resolve_rnd_db_path()
    prod_db = args.prod_db or PROD_DB

    return run(
        rnd_db=rnd_db,
        prod_db=prod_db,
        years=_parse_years(args.years),
        top_n=args.top_n,
        dry_run=args.dry_run,
        out_dir=Path(args.out),
    )


if __name__ == "__main__":
    sys.exit(main())
