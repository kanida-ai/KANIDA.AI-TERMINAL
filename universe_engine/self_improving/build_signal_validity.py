#!/usr/bin/env python3
"""build_signal_validity.py — S4: Signal validity (freshness + decay) + intraday
detection comparison for the Falcon Top 10 signal universe.

This is Step 4 of the Self-Improving Engine (spec build-sequence step 4 — "Signal
validity and intraday detection. Signal freshness analysis. Where 1-min data
exists, run intraday detection comparison."). It populates the
``falcon_signal_validity`` table (RND DB) and the signal-timing columns on
``falcon_pattern_taxonomy`` (RND DB), and writes ``falcon_signal_validity_report.xlsx``.

NOTHING is executed at build time (no Python in env). Validated by reading; run
later with ``--dry-run`` first.

────────────────────────────────────────────────────────────────────────────
SIGNAL UNIVERSE (identical to the signal-day study / Falcon Top 10):
  The engine's top-10/day picks. We reuse build_signal_day_study's EXACT
  selection machinery — _build_run_context + _year_signals_by_signal_date —
  which reproduces the parity Falcon Top 10 signal generation (eligible patterns
  for the year, compute_year_signals at min_fires=10, score == avg_lift, grouped
  by signal_date) and then takes the top-N per signal_date by avg_lift desc. No
  skip_already_held, no cash constraint (this is a per-signal study, like S2B).

────────────────────────────────────────────────────────────────────────────
PART A — SIGNAL FRESHNESS + DECAY (ALL YEARS, daily-derivable). The gate deliverable.

  For each top-N signal we compute, from PROD daily OHLC (fresh):

  • next_day_open_price = the D+1 open (the bar we would enter into).
  • entry_price_delta_pct = (D+1 open / signal-date close − 1) × 100 — the gap
    we enter into vs where the signal fired.

  • valid_at_next_open (the VALIDITY RULE — documented):
      The D+1 open is still ACTIONABLE iff it has NOT already gapped past the
      profit target NOR below the initial stop, measured from the SAME base the
      trade rules use (the D+1 entry price itself is the reference for the ±band;
      but "already moved past the band at the open" is judged vs the SIGNAL-DATE
      CLOSE — the price the signal was generated at, i.e. the move that happened
      overnight before we could act). Concretely:
        gap = (D+1 open / signal_date_close − 1)
        valid_at_next_open = 1  iff  init_stop < gap < target
                                      (default −7% < gap < +12%)
      • gap >= +12% → the move we were trying to capture has ALREADY happened
        overnight; entering now buys the top → STALE (not actionable).
      • gap <= −7% → the setup has already broken down past our stop overnight →
        STALE (not actionable).
      This is the band the locked trade rules (init_stop=−0.07, target=+0.12 via
      trail_trigger) define, applied to the overnight gap. Rule is documented in
      S4-build-log.md.

  • DECAY — re-simulate the SAME trade for entry at D+1, D+2, D+3 opens:
      We REUSE the engine's exit mechanics (simulate_independent_pick from
      build_signal_day_study, which is copied line-for-line from
      persona_engine_core.simulate_year:384-436) by passing a SHIFTED entry-bar
      index. The standard entry is the first bar with date > signal_date (sd_idx,
      = the D+1 next-open entry, core:354). D+2 = sd_idx+1, D+3 = sd_idx+2. Each
      re-sim runs the IDENTICAL 7-day / −7% / +12% / 10d-Donchian rules — only the
      entry bar moves. We store the three net_ret_pct (entry_d1/_d2/_d3 outcomes).
      NO exit rule is re-implemented; we shift the entry into the proven engine.

  • n_days_signal_remained_valid = how many of {D+1, D+2, D+3} entries are still
    positive-EV, i.e. their re-simulated net_ret_pct > 0 (the documented validity
    rule for the decay leg). 0..3.

  • eod_signal_outcome = the STANDARD D+1-entry net_ret_pct (the baseline the
    intraday early-entry is compared against in Part B). Same number S2B/baseline
    store for this (signal_date, symbol).

  Per-pattern aggregates → falcon_pattern_taxonomy (signal-timing cols ONLY):
    signal_validity_next_day_pct = % of the pattern's signals with
        valid_at_next_open == 1 (i.e. D+1 entry still actionable).
    avg_signal_decay_days        = the entry-day (1/2/3) at which the pattern's
        AVERAGE net_ret_pct PEAKS across D+1/D+2/D+3 (where the edge is best /
        how fast it decays). 1.0 = best to enter D+1 (freshest); 3.0 = edge
        actually peaks later.
    signal_best_entry_window     = 'D+1' / 'D+2' / 'D+3' — the same peak as a
        label.
  HFCL: only set these for patterns with n >= 18 signals (resolved D+1 trades);
  else NULL. A pattern's "signals" = its resolved (closed) D+1 trades, joined via
  the existing falcon_pattern_contributions table (persona=falcon_top10).

  falcon_signal_validity rows are populated with the daily-derivable fields for
  EVERY top-N signal (one row per (signal_date, symbol)).

────────────────────────────────────────────────────────────────────────────
PART B — INTRADAY DETECTION (ONLY where ohlc_1min exists: 2024-05-13 → 2026-05-11,
499 symbols). A price-based intraday-FRESHNESS pass.

  For signals whose ENTRY date (= D+1) falls in the 1-min window we query
  ohlc_1min in the RND DB PER (symbol, entry_date) ONLY (never scan the whole
  87.8M table). bar_time format = "%Y-%m-%d %H:%M:%S"; columns symbol, bar_time,
  open, high, low, close, volume; PK (symbol, bar_time).

  From that day's minute bars we read the price at 09:15 (session open), 10:00,
  11:00, 13:00 and compute:
    • valid_at_next_10am / _11am / _1pm = is the signal still ACTIONABLE at that
      intraday checkpoint? Same band rule as valid_at_next_open but the "current
      price" is the checkpoint price and the base is the signal-date close:
        gap_t = (price_t / signal_date_close − 1); valid = init_stop < gap_t < target.
    • intraday_entry_price = the first available price in the first 15 minutes
      (09:15..09:29), else the 09:15 open (the price an intraday-early entrant
      would actually get).
    • intraday_early_entry_outcome = the trade's net return measured the SAME way
      the engine measures it, but ENTERED at intraday_entry_price instead of the
      D+1 open. We REUSE the engine exit walk: we run simulate_independent_pick on
      a SYNTHETIC bar list whose entry bar's `open` is overwritten with
      intraday_entry_price (all later bars unchanged), so the identical
      −7%/+12%/10d/7-day exit logic plays out off the earlier entry. (Entry-day
      bar is never evaluated for exit in the engine — core:441-525 — so replacing
      only its open is faithful: exits still start D+2.)
    • eod_signal_outcome = the standard D+1-open net_ret_pct (Part A).
    • detected_intraday = 1 if the signal was actionable intraday (valid at the
      first available checkpoint among 09:15→13:00) AND a 1-min entry price exists.
    • intraday_false_positive = the signal LOOKED actionable intraday but the
      realized outcome (eod_signal_outcome, the D+1 standard trade) was a LOSS.
      false_positive_reason names which check passed vs the loss.

  Signals BEFORE 2024-05-13 / symbols WITHOUT 1-min → ALL intraday fields NULL
  (never fabricated). The run prints how many signals fell IN vs OUT of the
  window.

  Per-pattern (1-min subset) → taxonomy:
    intraday_detectable          = 1 if the pattern has >= 18 in-window signals
                                   and any were detected intraday (else NULL <18).
    intraday_detection_accuracy  = % of in-window detections whose D+1 outcome
                                   was a win (detected_intraday==1 AND outcome>0).
    intraday_false_positive_rate = % of in-window detections that were false
                                   positives. (n >= 18 in-window else NULL.)

  DEFERRED (documented, not built here): the DEEPER per-pattern intraday RULE
  detection — re-evaluating each pattern's mined rule on intraday-computed
  features every 15 min (the spec's "live detection scan", false-positive 30-min
  hold, sector/volume cross-checks). That needs an intraday FEATURE pipeline
  (the 38 FEATURE_COLS recomputed on partial-day 1-min bars) which does not exist
  yet. THIS pass is a price-based intraday-freshness comparison only.

────────────────────────────────────────────────────────────────────────────
PART C — Report falcon_signal_validity_report.xlsx
  Sheet "Per-Pattern Validity"   : per pattern — signal_validity_next_day_pct,
      avg_signal_decay_days, signal_best_entry_window,
      intraday_detection_accuracy / intraday_false_positive_rate (where available).
  Sheet "Decay Summary"          : avg net_ret by entry D+1/D+2/D+3, overall +
      per-year.
  Sheet "Intraday Coverage"      : n signals in / out of the 1-min window + the
      intraday early-entry-vs-next-open comparison (avg delta, win-rate, FP rate).

────────────────────────────────────────────────────────────────────────────
CONSTRAINTS honored: REUSE the engine exit mechanics for the alternate-entry
re-sims (simulate_independent_pick — copied line-for-line from
persona_engine_core.simulate_year; we never re-implement an exit rule, we only
move the entry bar). HFCL n>=18 for any per-pattern taxonomy flag. Additive /
idempotent (DELETE this persona's falcon_signal_validity rows then INSERT;
taxonomy UPDATE only the 6 signal-timing cols — quality_flag etc. untouched).
RND-only writes (falcon_signal_validity + taxonomy signal-timing cols). No PROD /
shared-engine-code mutation (INV2). OHLC/features read from PROD (fresh); 1-min +
sectors read from RND.

CLI:
  python build_signal_validity.py --rnd-db <path> [--prod-db <path>]
      [--years 2021,2022] [--top-n 10] [--dry-run] [--out out/v4]
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Windows consoles default to cp1252, which can't encode box-drawing chars in the
# summary print. Force utf-8 so output never crashes the run.
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# ── Import the PRODUCTION engine + the S2B study helpers (reuse only). ────────
# Mirror backend/main.py's import root exactly (see build_signal_day_study.py).
_HERE = Path(__file__).resolve().parent
_REPO_ROOT = _HERE.parent.parent
_BACKEND_ROOT = _REPO_ROOT / "backend"
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))
# The self_improving dir itself (to import build_signal_day_study as a module).
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from power_user.services.persona_simulator import (  # noqa: E402
    _resolve_rnd_db_path,
    PROD_DB,
)

# REUSE the signal-day study's selection + the engine exit mechanics. These are
# the SAME functions the parity Falcon Top 10 path uses (build_signal_day_study
# imports them straight from persona_engine_core); importing here keeps ONE copy
# of the selection + exit logic, never a re-implementation.
import build_signal_day_study as sds  # noqa: E402
# Concretely used:
#   sds._build_run_context           — identical loaders to the parity run
#   sds._year_signals_by_signal_date — parity Falcon Top 10 signal generation
#   sds.simulate_independent_pick    — engine exit walk (core:384-436 copy)

PERSONA_TAG = "falcon_top10_signal_validity"   # this study's persona value
CONTRIB_PERSONA_TAG = "falcon_top10"           # join key into pattern_contributions

# 1-min coverage window (STEP0_AUDIT.md §C). Signals with entry_date outside this
# get ALL intraday fields NULL (never fabricated). Re-confirmed at runtime against
# the actual MIN/MAX(bar_time) in ohlc_1min (the constants are a documented
# fallback / sanity bound; the live MIN/MAX is authoritative).
ONE_MIN_START = "2024-05-13"
ONE_MIN_END = "2026-05-11"

# Intraday checkpoints (IST clock times in the bar_time string).
CK_OPEN = "09:15:00"
CK_10AM = "10:00:00"
CK_11AM = "11:00:00"
CK_1PM = "13:00:00"
FIRST_15_END = "09:30:00"   # first-15-min window is [09:15, 09:30)

HFCL_MIN_N = 18             # per spec Constitutional Rules #3 & #10

# Taxonomy columns this step OWNS (writes). Everything else untouched (additive).
_TAXONOMY_WRITE_COLS = [
    "signal_validity_next_day_pct",
    "avg_signal_decay_days",
    "signal_best_entry_window",
    "intraday_detectable",
    "intraday_detection_accuracy",
    "intraday_false_positive_rate",
]


def _ist_today() -> str:
    """Today in IST (Asia/Kolkata). Per the always-use-IST rule: compute IST
    explicitly, never from the server clock / log timestamps."""
    ist = timezone(timedelta(hours=5, minutes=30))
    return datetime.now(ist).strftime("%Y-%m-%d")


# ════════════════════════════════════════════════════════════════════════════
# PART A — SIGNAL FRESHNESS + DECAY (daily-derivable, all years)
# ════════════════════════════════════════════════════════════════════════════

def _net_ret_from_trade(trade: Optional[Dict[str, Any]]) -> Optional[float]:
    """net_ret_pct from a simulate_independent_pick trade dict (closed only)."""
    if trade is None:
        return None
    net_pnl = trade.get("net_pnl")
    actual = trade.get("actual_deployed")
    if net_pnl is None or not actual:
        return None
    return net_pnl / actual * 100.0


def _resim_entry_at(
    sig: Dict[str, Any], bars_sym: List[Dict], entry_idx: int, run_cfg
) -> Optional[float]:
    """Re-simulate the SAME trade entered at bars_sym[entry_idx]'s open, REUSING
    the engine exit mechanics (sds.simulate_independent_pick). Returns net_ret_pct
    (closed) or None (unenterable / still open at data edge). entry_idx == sd_idx
    is the D+1 standard entry; sd_idx+1 = D+2; sd_idx+2 = D+3."""
    if entry_idx is None or entry_idx >= len(bars_sym):
        return None
    trade = sds.simulate_independent_pick(sig, bars_sym, entry_idx, run_cfg)
    if trade is None or trade.get("_is_open"):
        return None
    return _net_ret_from_trade(trade)


def _valid_band(gap_frac: float, run_cfg) -> bool:
    """The documented VALIDITY RULE band: a move is still actionable iff the gap
    (fraction, vs signal-date close) has NOT crossed the locked trade band —
    init_stop < gap < target. target is run_cfg.target if set else
    run_cfg.trail_trigger (Falcon Top 10 arms the trail at +12% — that is the
    profit objective)."""
    lo = run_cfg.init_stop                                   # -0.07
    hi = run_cfg.target if run_cfg.target is not None else run_cfg.trail_trigger
    if hi is None:
        hi = 0.12                                            # documented fallback
    return lo < gap_frac < hi


def compute_part_a_row(
    sig: Dict[str, Any],
    bars_sym: List[Dict],
    sd_idx: int,
    sd_close: Optional[float],
    rank: int,
    run_cfg,
) -> Dict[str, Any]:
    """Build the daily-derivable falcon_signal_validity fields + the decay legs
    for one top-N signal. sd_idx = first bar with date > signal_date (D+1 entry,
    core:354). sd_close = the signal-date close (the price the signal fired at)."""
    next_open = bars_sym[sd_idx]["open"] if sd_idx < len(bars_sym) else None

    # entry_price_delta_pct + valid_at_next_open vs the SIGNAL-DATE close.
    entry_delta = None
    valid_open = None
    if next_open is not None and sd_close and sd_close > 0:
        gap = next_open / sd_close - 1.0
        entry_delta = gap * 100.0
        valid_open = 1 if _valid_band(gap, run_cfg) else 0

    # Decay: D+1 / D+2 / D+3 re-sims (REUSE engine exit; only entry bar shifts).
    ret_d1 = _resim_entry_at(sig, bars_sym, sd_idx, run_cfg)
    ret_d2 = _resim_entry_at(sig, bars_sym, sd_idx + 1, run_cfg)
    ret_d3 = _resim_entry_at(sig, bars_sym, sd_idx + 2, run_cfg)

    # n_days_signal_remained_valid = # of {D+1,D+2,D+3} entries still positive-EV.
    n_valid = sum(1 for r in (ret_d1, ret_d2, ret_d3) if r is not None and r > 0)

    return {
        "persona": PERSONA_TAG,
        "signal_date": sig["signal_date"],
        "symbol": sig["symbol"],
        "engine_rank": rank,
        "avg_lift": float(sig.get("avg_lift", sig.get("score", 0.0))),
        "valid_at_next_open": valid_open,
        "n_days_signal_remained_valid": n_valid,
        "next_day_open_price": next_open,
        "entry_price_delta_pct": entry_delta,
        "eod_signal_outcome": ret_d1,           # standard D+1-entry net_ret_pct
        # decay legs (kept on the in-memory row for the report + taxonomy; the DB
        # schema stores eod_signal_outcome; _ret_d2/_ret_d3 are report-only).
        "_ret_d1": ret_d1,
        "_ret_d2": ret_d2,
        "_ret_d3": ret_d3,
        "_sd_idx": sd_idx,
    }


# ════════════════════════════════════════════════════════════════════════════
# PART B — INTRADAY DETECTION (1-min window only; per-(symbol,date) query)
# ════════════════════════════════════════════════════════════════════════════

def _one_min_window(rnd_db: str) -> Tuple[Optional[str], Optional[str], bool]:
    """Authoritative 1-min coverage from the RND DB: (min_date, max_date, exists).
    Reads MIN/MAX(date(bar_time)) once. If ohlc_1min is absent → (None,None,False)
    and Part B is skipped entirely (every intraday field stays NULL)."""
    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        has = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='ohlc_1min'"
        ).fetchone()
        if not has:
            return None, None, False
        row = con.execute(
            "SELECT MIN(substr(bar_time,1,10)), MAX(substr(bar_time,1,10)) FROM ohlc_1min"
        ).fetchone()
    finally:
        con.close()
    if not row or row[0] is None:
        return None, None, True   # table exists but empty
    return row[0], row[1], True


def _load_minute_bars_for(
    rnd_db: str, symbol: str, day: str
) -> List[Tuple[str, float, float, float, float]]:
    """All 1-min bars for (symbol, day) ONLY — a single indexed point query per
    (symbol, date) (never a full-table scan). Returns [(hhmmss, o,h,l,c)] sorted
    by time. bar_time = "%Y-%m-%d %H:%M:%S"; we range-scan that one day via the
    PK (symbol, bar_time) prefix."""
    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        rows = con.execute(
            "SELECT bar_time, open, high, low, close FROM ohlc_1min "
            "WHERE symbol = ? AND bar_time >= ? AND bar_time < ? "
            "ORDER BY bar_time",
            (symbol, f"{day} 00:00:00", f"{day} 23:59:59"),
        ).fetchall()
    finally:
        con.close()
    out = []
    for bt, o, h, l, c in rows:
        # bt = "YYYY-MM-DD HH:MM:SS" → take the HH:MM:SS clock part.
        out.append((bt[11:19], o, h, l, c))
    return out


def _price_at_or_after(
    minute_bars: List[Tuple[str, float, float, float, float]], hhmmss: str
) -> Optional[float]:
    """The OPEN of the first bar at or after hhmmss (the price you'd act at that
    checkpoint). None if no bar at/after that time exists for the day."""
    for (t, o, _h, _l, _c) in minute_bars:
        if t >= hhmmss:
            return o
    return None


def _first_15_entry_price(
    minute_bars: List[Tuple[str, float, float, float, float]]
) -> Tuple[Optional[float], Optional[str]]:
    """Intraday early-entry price = open of the FIRST bar in [09:15, 09:30); else
    the 09:15 open; else None. Returns (price, detection_time_hhmmss)."""
    for (t, o, _h, _l, _c) in minute_bars:
        if CK_OPEN <= t < FIRST_15_END:
            return o, t
    # fall back to the session open at/after 09:15
    for (t, o, _h, _l, _c) in minute_bars:
        if t >= CK_OPEN:
            return o, t
    return None, None


def compute_part_b_row(
    rnd_db: str,
    sig: Dict[str, Any],
    bars_sym: List[Dict],
    sd_idx: int,
    sd_close: Optional[float],
    eod_outcome: Optional[float],
    run_cfg,
    win_lo: str,
    win_hi: str,
) -> Dict[str, Any]:
    """Intraday fields for one signal. ALL NULL when the entry date is outside the
    1-min window OR the symbol has no minute bars that day (never fabricated).
    Returns a dict of the intraday columns + an `_in_window` marker."""
    null_row = {
        "valid_at_next_10am": None, "valid_at_next_11am": None,
        "valid_at_next_1pm": None, "detected_intraday": None,
        "intraday_detection_time": None, "intraday_entry_price": None,
        "intraday_early_entry_outcome": None, "intraday_false_positive": None,
        "false_positive_reason": None, "_in_window": 0,
    }
    if sd_idx >= len(bars_sym):
        return null_row
    entry_date = bars_sym[sd_idx]["date"]      # = D+1 (the bar we enter into)
    if not (win_lo <= entry_date <= win_hi):
        return null_row                         # before 1-min start / after end

    minute_bars = _load_minute_bars_for(rnd_db, sig["symbol"], entry_date)
    if not minute_bars:
        return null_row                         # symbol has no 1-min bars that day

    # In-window AND has data → compute the intraday-freshness fields.
    def _valid_at(hhmmss: str) -> Optional[int]:
        px = _price_at_or_after(minute_bars, hhmmss)
        if px is None or not sd_close or sd_close <= 0:
            return None
        gap = px / sd_close - 1.0
        return 1 if _valid_band(gap, run_cfg) else 0

    v10 = _valid_at(CK_10AM)
    v11 = _valid_at(CK_11AM)
    v1p = _valid_at(CK_1PM)

    intr_px, det_time = _first_15_entry_price(minute_bars)

    # intraday early-entry outcome: REUSE the engine exit walk on a synthetic bar
    # list whose ENTRY bar open = intr_px (all other bars unchanged). The engine
    # never evaluates the entry-day bar for an exit (core:441-525), so replacing
    # only its open is faithful — the −7%/+12%/10d/7-day exits still start D+2.
    intr_outcome = None
    if intr_px is not None and intr_px > 0:
        synth = list(bars_sym)
        eb = dict(bars_sym[sd_idx])
        eb["open"] = intr_px
        synth[sd_idx] = eb
        intr_outcome = _resim_entry_at(sig, synth, sd_idx, run_cfg)

    # detected_intraday: actionable at the first checkpoint we can read (open→1pm)
    # AND a 1-min entry price exists.
    first_valid = None
    for v in (_valid_at(CK_OPEN), v10, v11, v1p):
        if v is not None:
            first_valid = v
            break
    detected = 1 if (first_valid == 1 and intr_px is not None) else 0

    # false positive: looked actionable intraday but the realized (D+1 standard)
    # outcome was a LOSS.
    fp = None
    fp_reason = None
    if detected == 1 and eod_outcome is not None:
        if eod_outcome <= 0:
            fp = 1
            fp_reason = (f"actionable intraday (valid@open/10/11/1pm="
                         f"{first_valid}/{v10}/{v11}/{v1p}) but D+1 outcome "
                         f"{eod_outcome:.2f}% <= 0")
        else:
            fp = 0
            fp_reason = None

    return {
        "valid_at_next_10am": v10, "valid_at_next_11am": v11,
        "valid_at_next_1pm": v1p, "detected_intraday": detected,
        "intraday_detection_time": det_time, "intraday_entry_price": intr_px,
        "intraday_early_entry_outcome": intr_outcome,
        "intraday_false_positive": fp, "false_positive_reason": fp_reason,
        "_in_window": 1,
    }


# ════════════════════════════════════════════════════════════════════════════
# PER-PATTERN AGGREGATION → taxonomy signal-timing cols (HFCL n>=18)
# ════════════════════════════════════════════════════════════════════════════

def _load_signal_to_patterns(rnd_db: str) -> Dict[Tuple[str, str], List[int]]:
    """{(signal_date, symbol) -> [pattern_id, ...]} from the EXISTING
    falcon_pattern_contributions table (persona=falcon_top10), restricted to
    RESOLVED occurrences (parent trade closed). Reused, not re-derived: Step 2
    already wrote which patterns fired for each Falcon Top 10 pick. Empty {} if
    the table is absent (per-pattern taxonomy update becomes a no-op, documented)."""
    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        present = {r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('falcon_pattern_contributions','falcon_baseline_trades')"
        ).fetchall()}
        # Need BOTH tables for the resolved-occurrence join; absent → no-op {}.
        if not {"falcon_pattern_contributions", "falcon_baseline_trades"} <= present:
            return {}
        rows = con.execute(
            """
            SELECT c.signal_date, c.symbol, c.pattern_id
              FROM falcon_pattern_contributions c
              JOIN falcon_baseline_trades bt ON bt.id = c.trade_id
             WHERE c.persona = ?
               AND bt.net_ret_pct IS NOT NULL
            """,
            (CONTRIB_PERSONA_TAG,),
        ).fetchall()
    finally:
        con.close()
    out: Dict[Tuple[str, str], List[int]] = defaultdict(list)
    for sd, sym, pid in rows:
        out[(sd, sym)].append(int(pid))
    return out


def _mean(vals: List[float]) -> Optional[float]:
    return (sum(vals) / len(vals)) if vals else None


def _rate(num: int, den: int) -> Optional[float]:
    return (num / den * 100.0) if den else None


def compute_pattern_taxonomy(
    rows: List[Dict[str, Any]],
    sig_to_pats: Dict[Tuple[str, str], List[int]],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Aggregate the per-signal rows to per-pattern taxonomy values. A pattern's
    "signals" = the top-N signals it contributed to (via pattern_contributions),
    counting only signals with a resolved D+1 outcome (_ret_d1 not None) for the
    decay/peak math; signal_validity_next_day_pct uses valid_at_next_open.

    HFCL: signal_validity_next_day_pct / avg_signal_decay_days /
    signal_best_entry_window only set when n >= 18 of THAT pattern's signals
    (else NULL). intraday_* only set when the pattern has >= 18 IN-WINDOW signals
    (else NULL)."""
    # index rows by (signal_date, symbol)
    by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in rows:
        by_key[(r["signal_date"], r["symbol"])] = r

    # invert: pattern_id -> list of its signal rows
    by_pat: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for key, pids in sig_to_pats.items():
        r = by_key.get(key)
        if r is None:
            continue
        for pid in pids:
            by_pat[pid].append(r)

    tax_rows: List[Dict[str, Any]] = []
    n_validity_set = n_intraday_set = 0
    for pid, sig_rows in sorted(by_pat.items()):
        rec: Dict[str, Any] = {"pattern_id": pid,
                               "signal_validity_next_day_pct": None,
                               "avg_signal_decay_days": None,
                               "signal_best_entry_window": None,
                               "intraday_detectable": None,
                               "intraday_detection_accuracy": None,
                               "intraday_false_positive_rate": None,
                               "_n_signals": len(sig_rows)}

        # ── Validity + decay (all-years; HFCL on signal count) ──
        if len(sig_rows) >= HFCL_MIN_N:
            valid_vals = [r["valid_at_next_open"] for r in sig_rows
                          if r["valid_at_next_open"] is not None]
            if valid_vals:
                rec["signal_validity_next_day_pct"] = _rate(sum(valid_vals), len(valid_vals))
            d1 = [r["_ret_d1"] for r in sig_rows if r["_ret_d1"] is not None]
            d2 = [r["_ret_d2"] for r in sig_rows if r["_ret_d2"] is not None]
            d3 = [r["_ret_d3"] for r in sig_rows if r["_ret_d3"] is not None]
            means = {1: _mean(d1), 2: _mean(d2), 3: _mean(d3)}
            avail = {d: m for d, m in means.items() if m is not None}
            if avail:
                best_day = max(avail, key=lambda d: avail[d])
                rec["avg_signal_decay_days"] = float(best_day)
                rec["signal_best_entry_window"] = f"D+{best_day}"
            if rec["signal_validity_next_day_pct"] is not None:
                n_validity_set += 1

        # ── Intraday (in-window subset; HFCL on in-window signal count) ──
        inw = [r for r in sig_rows if r.get("_intraday", {}).get("_in_window") == 1]
        if len(inw) >= HFCL_MIN_N:
            det = [r for r in inw if r["_intraday"].get("detected_intraday") == 1]
            n_det = len(det)
            if n_det:
                n_acc = sum(1 for r in det
                            if r["eod_signal_outcome"] is not None
                            and r["eod_signal_outcome"] > 0)
                n_fp = sum(1 for r in det if r["_intraday"].get("intraday_false_positive") == 1)
                rec["intraday_detection_accuracy"] = _rate(n_acc, n_det)
                rec["intraday_false_positive_rate"] = _rate(n_fp, n_det)
                rec["intraday_detectable"] = 1
            else:
                rec["intraday_detectable"] = 0
            n_intraday_set += 1

        tax_rows.append(rec)

    stats = {
        "n_patterns": len(tax_rows),
        "n_validity_set": n_validity_set,
        "n_intraday_set": n_intraday_set,
    }
    return tax_rows, stats


# ════════════════════════════════════════════════════════════════════════════
# DECAY / INTRADAY SUMMARIES (overall + per-year) for the report
# ════════════════════════════════════════════════════════════════════════════

def compute_decay_summary(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """avg net_ret by entry D+1/D+2/D+3 — overall + per-year. year = signal_date
    year. Each leg averaged over its available (closed) re-sims."""
    buckets: Dict[Any, Dict[str, List[float]]] = defaultdict(
        lambda: {"d1": [], "d2": [], "d3": [], "valid": []}
    )
    for r in rows:
        yr = r["signal_date"][:4]
        for k, leg in (("d1", "_ret_d1"), ("d2", "_ret_d2"), ("d3", "_ret_d3")):
            v = r.get(leg)
            if v is not None:
                buckets[yr][k].append(v)
                buckets["ALL"][k].append(v)
        if r["valid_at_next_open"] is not None:
            buckets[yr]["valid"].append(r["valid_at_next_open"])
            buckets["ALL"]["valid"].append(r["valid_at_next_open"])

    out: List[Dict[str, Any]] = []
    for key in sorted(buckets, key=lambda k: (k == "ALL", k)):
        b = buckets[key]
        out.append({
            "scope": "Overall" if key == "ALL" else key,
            "n_d1": len(b["d1"]), "avg_ret_d1": _mean(b["d1"]),
            "n_d2": len(b["d2"]), "avg_ret_d2": _mean(b["d2"]),
            "n_d3": len(b["d3"]), "avg_ret_d3": _mean(b["d3"]),
            "valid_at_open_pct": (_rate(sum(b["valid"]), len(b["valid"]))
                                  if b["valid"] else None),
        })
    return out


def compute_intraday_coverage(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """n signals in / out of the 1-min window + intraday early-entry vs next-open
    comparison (avg delta, win rates, FP rate)."""
    in_win = [r for r in rows if r["_intraday"].get("_in_window") == 1]
    out_win = [r for r in rows if r["_intraday"].get("_in_window") != 1]
    det = [r for r in in_win if r["_intraday"].get("detected_intraday") == 1]

    deltas = []
    intr_wins = eod_wins = 0
    cmp_n = 0
    for r in in_win:
        ie = r["_intraday"].get("intraday_early_entry_outcome")
        eo = r["eod_signal_outcome"]
        if ie is not None and eo is not None:
            deltas.append(ie - eo)
            cmp_n += 1
            if ie > 0:
                intr_wins += 1
            if eo > 0:
                eod_wins += 1
    n_fp = sum(1 for r in det if r["_intraday"].get("intraday_false_positive") == 1)
    n_acc = sum(1 for r in det if r["eod_signal_outcome"] is not None
                and r["eod_signal_outcome"] > 0)
    return {
        "n_total": len(rows),
        "n_in_window": len(in_win),
        "n_out_window": len(out_win),
        "n_detected_intraday": len(det),
        "n_compared": cmp_n,
        "avg_intraday_minus_eod_delta": _mean(deltas),
        "intraday_early_win_rate": _rate(intr_wins, cmp_n) if cmp_n else None,
        "eod_entry_win_rate": _rate(eod_wins, cmp_n) if cmp_n else None,
        "intraday_detection_accuracy": _rate(n_acc, len(det)) if det else None,
        "intraday_false_positive_rate": _rate(n_fp, len(det)) if det else None,
    }


# ════════════════════════════════════════════════════════════════════════════
# SCHEMA APPLY (CREATE IF NOT EXISTS + guarded taxonomy ALTERs)
# ════════════════════════════════════════════════════════════════════════════

def _existing_columns(con: sqlite3.Connection, table: str) -> List[str]:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]


def apply_schema(rnd_db: str) -> None:
    """Ensure falcon_signal_validity exists + the taxonomy signal-timing columns
    exist — both idempotently, additive only.

    falcon_signal_validity is created from schema_self_improving.sql (the canonical
    Step-1 DDL) via CREATE TABLE IF NOT EXISTS. The taxonomy signal-timing columns
    come from taxonomy_columns.sql; we apply ONLY the 6 we own, PRAGMA-guarded
    (skip ones already present). Never drops/renames/retypes (INV2)."""
    # 1. falcon_signal_validity (from the canonical schema file).
    self_sql = (_HERE / "schema_self_improving.sql").read_text(encoding="utf-8")
    # Extract just the CREATE TABLE ... falcon_signal_validity (...) statement so
    # we don't recreate the other 9 tables here (they may differ across runs);
    # CREATE TABLE IF NOT EXISTS makes any of them a safe no-op anyway, so we run
    # the whole script — it's idempotent and additive.
    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        con.executescript(self_sql)
        # 2. Guarded taxonomy ALTERs for the 6 signal-timing columns.
        existing = set(_existing_columns(con, "falcon_pattern_taxonomy")) \
            if con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name='falcon_pattern_taxonomy'").fetchone() else None
        if existing is not None:
            alters = {
                "signal_validity_next_day_pct":
                    "ALTER TABLE falcon_pattern_taxonomy ADD COLUMN signal_validity_next_day_pct REAL",
                "avg_signal_decay_days":
                    "ALTER TABLE falcon_pattern_taxonomy ADD COLUMN avg_signal_decay_days REAL",
                "signal_best_entry_window":
                    "ALTER TABLE falcon_pattern_taxonomy ADD COLUMN signal_best_entry_window TEXT",
                "intraday_detectable":
                    "ALTER TABLE falcon_pattern_taxonomy ADD COLUMN intraday_detectable INTEGER",
                "intraday_detection_accuracy":
                    "ALTER TABLE falcon_pattern_taxonomy ADD COLUMN intraday_detection_accuracy REAL",
                "intraday_false_positive_rate":
                    "ALTER TABLE falcon_pattern_taxonomy ADD COLUMN intraday_false_positive_rate REAL",
            }
            for col, stmt in alters.items():
                if col in existing:
                    continue
                try:
                    con.execute(stmt)
                except sqlite3.OperationalError as e:
                    if "duplicate column" in str(e).lower():
                        continue
                    raise
        con.commit()
    finally:
        con.close()


# ════════════════════════════════════════════════════════════════════════════
# WRITE (idempotent, single transaction, RND-only)
# ════════════════════════════════════════════════════════════════════════════

_SV_COLS = [
    "signal_date", "symbol", "persona", "engine_rank", "avg_lift",
    "valid_at_next_open", "valid_at_next_10am", "valid_at_next_11am",
    "valid_at_next_1pm", "n_days_signal_remained_valid",
    "detected_intraday", "intraday_detection_time", "intraday_entry_price",
    "next_day_open_price", "entry_price_delta_pct",
    "intraday_early_entry_outcome", "eod_signal_outcome",
    "intraday_false_positive", "false_positive_reason",
]


def _sv_value(r: Dict[str, Any], col: str) -> Any:
    """Resolve a falcon_signal_validity column value from a combined row (Part A
    fields at top level; Part B fields under r['_intraday'])."""
    if col in ("valid_at_next_10am", "valid_at_next_11am", "valid_at_next_1pm",
               "detected_intraday", "intraday_detection_time",
               "intraday_entry_price", "intraday_early_entry_outcome",
               "intraday_false_positive", "false_positive_reason"):
        return r["_intraday"].get(col)
    return r.get(col)


def _write(
    rnd_db: str,
    rows: List[Dict[str, Any]],
    tax_rows: List[Dict[str, Any]],
) -> Tuple[int, int, int]:
    """Apply both writes in ONE transaction. Returns
    (n_validity_inserted, n_taxonomy_updated, n_taxonomy_missing).

    Idempotent: DELETE this persona's falcon_signal_validity rows, re-INSERT;
    taxonomy UPDATE only the 6 signal-timing columns (quality_flag etc. untouched)."""
    con = sqlite3.connect(rnd_db, timeout=120.0)
    n_sv = n_tax = n_missing = 0
    try:
        con.execute("BEGIN")

        # ── falcon_signal_validity (DELETE-then-INSERT for this persona) ──
        con.execute("DELETE FROM falcon_signal_validity WHERE persona = ?",
                    (PERSONA_TAG,))
        placeholders = ", ".join("?" for _ in _SV_COLS)
        sql = (f"INSERT INTO falcon_signal_validity ({', '.join(_SV_COLS)}) "
               f"VALUES ({placeholders})")
        con.executemany(sql, [[_sv_value(r, c) for c in _SV_COLS] for r in rows])
        n_sv = len(rows)

        # ── taxonomy UPDATE — only existing signal-timing cols ──
        tax_cols_present = set(_existing_columns(con, "falcon_pattern_taxonomy")) \
            if con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name='falcon_pattern_taxonomy'").fetchone() else set()
        write_cols = [c for c in _TAXONOMY_WRITE_COLS if c in tax_cols_present]
        if "pattern_id" in tax_cols_present and write_cols:
            set_sql = ", ".join(f"{c} = ?" for c in write_cols)
            upd = f"UPDATE falcon_pattern_taxonomy SET {set_sql} WHERE pattern_id = ?"
            for rec in tax_rows:
                vals = [rec.get(c) for c in write_cols] + [rec["pattern_id"]]
                cur = con.execute(upd, vals)
                if cur.rowcount and cur.rowcount > 0:
                    n_tax += 1
                else:
                    n_missing += 1
        else:
            n_missing = len(tax_rows)

        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
    return n_sv, n_tax, n_missing


# ════════════════════════════════════════════════════════════════════════════
# EXCEL / CSV OUTPUT
# ════════════════════════════════════════════════════════════════════════════

def _round(v: Optional[float], nd: int = 4) -> Any:
    return round(v, nd) if isinstance(v, (int, float)) else v


def write_report(
    out_dir: Path,
    tax_rows: List[Dict[str, Any]],
    decay_summary: List[Dict[str, Any]],
    coverage: Dict[str, Any],
) -> Tuple[Path, str]:
    """falcon_signal_validity_report.xlsx (openpyxl) or .csv fallback."""
    out_dir.mkdir(parents=True, exist_ok=True)

    pv_headers = [
        "pattern_id", "n_signals", "signal_validity_next_day_pct",
        "avg_signal_decay_days", "signal_best_entry_window",
        "intraday_detectable", "intraday_detection_accuracy",
        "intraday_false_positive_rate",
    ]
    pv_data = [[
        r["pattern_id"], r["_n_signals"],
        _round(r["signal_validity_next_day_pct"]),
        _round(r["avg_signal_decay_days"]),
        r["signal_best_entry_window"],
        r["intraday_detectable"],
        _round(r["intraday_detection_accuracy"]),
        _round(r["intraday_false_positive_rate"]),
    ] for r in tax_rows]

    ds_headers = ["scope", "n_d1", "avg_ret_d1", "n_d2", "avg_ret_d2",
                  "n_d3", "avg_ret_d3", "valid_at_open_pct"]
    ds_data = [[
        r["scope"], r["n_d1"], _round(r["avg_ret_d1"]),
        r["n_d2"], _round(r["avg_ret_d2"]),
        r["n_d3"], _round(r["avg_ret_d3"]),
        _round(r["valid_at_open_pct"]),
    ] for r in decay_summary]

    ic_pairs = [
        ("1-min window start", coverage.get("_win_lo")),
        ("1-min window end", coverage.get("_win_hi")),
        ("total signals", coverage["n_total"]),
        ("signals IN 1-min window", coverage["n_in_window"]),
        ("signals OUT of 1-min window", coverage["n_out_window"]),
        ("detected intraday", coverage["n_detected_intraday"]),
        ("compared (intraday vs next-open)", coverage["n_compared"]),
        ("avg (intraday_early − next_open) ret %",
         _round(coverage["avg_intraday_minus_eod_delta"])),
        ("intraday-early-entry win rate %",
         _round(coverage["intraday_early_win_rate"])),
        ("next-open-entry win rate %", _round(coverage["eod_entry_win_rate"])),
        ("intraday detection accuracy %",
         _round(coverage["intraday_detection_accuracy"])),
        ("intraday false-positive rate %",
         _round(coverage["intraday_false_positive_rate"])),
    ]

    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font
    except Exception:
        return _write_csv_fallback(out_dir, pv_headers, pv_data,
                                   ds_headers, ds_data, ic_pairs)

    wb = Workbook()
    ws1 = wb.active
    ws1.title = "Per-Pattern Validity"
    ws1.append(pv_headers)
    for c in ws1[1]:
        c.font = Font(bold=True)
    for d in pv_data:
        ws1.append(d)

    ws2 = wb.create_sheet("Decay Summary")
    ws2.append(ds_headers)
    for c in ws2[1]:
        c.font = Font(bold=True)
    for d in ds_data:
        ws2.append(d)

    ws3 = wb.create_sheet("Intraday Coverage")
    ws3.append(["metric", "value"])
    for c in ws3[1]:
        c.font = Font(bold=True)
    for k, v in ic_pairs:
        ws3.append([k, v])

    path = out_dir / "falcon_signal_validity_report.xlsx"
    wb.save(str(path))
    return path, "xlsx"


def _write_csv_fallback(out_dir, pv_h, pv_d, ds_h, ds_d, ic_pairs) -> Tuple[Path, str]:
    import csv
    p1 = out_dir / "falcon_signal_validity_per_pattern.csv"
    with open(p1, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(pv_h); w.writerows(pv_d)
    p2 = out_dir / "falcon_signal_validity_decay.csv"
    with open(p2, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(ds_h); w.writerows(ds_d)
    p3 = out_dir / "falcon_signal_validity_intraday_coverage.csv"
    with open(p3, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["metric", "value"]); w.writerows(ic_pairs)
    print(f"[signal_validity] openpyxl missing — wrote CSV fallback: "
          f"{p1.name}, {p2.name}, {p3.name}")
    return p1, "csv"


# ════════════════════════════════════════════════════════════════════════════
# ORCHESTRATION
# ════════════════════════════════════════════════════════════════════════════

def run(
    rnd_db: str,
    prod_db: str,
    years: Optional[List[int]],
    top_n: int,
    dry_run: bool,
    out_dir: Path,
) -> int:
    print(f"[signal_validity] RND DB:  {rnd_db}")
    print(f"[signal_validity] PROD DB: {prod_db}  (read-only: OHLC/features)")
    print(f"[signal_validity] persona: {PERSONA_TAG} (top-{top_n}/signal_date)")
    print(f"[signal_validity] mode: {'DRY-RUN (no writes)' if dry_run else 'APPLY'}  "
          f"IST date={_ist_today()}")

    # Shared run context (same loaders as the parity Falcon Top 10 run).
    ctx = sds._build_run_context(rnd_db, prod_db)
    run_cfg = ctx["run_cfg"]
    bars = ctx["bars"]
    X = ctx["X"]
    print(f"[signal_validity] loaded {len(ctx['all_pats'])} patterns, "
          f"{X.shape[0]} panel rows, {len(bars)} symbols, "
          f"{len(ctx['all_td'])} trading days. "
          f"band=({run_cfg.init_stop:+.0%}, "
          f"{(run_cfg.target if run_cfg.target is not None else run_cfg.trail_trigger):+.0%}) "
          f"fixed_per_trade=Rs{run_cfg.fixed_per_trade:,.0f}")

    candidate_years = ctx["years_in_window"]
    if years:
        candidate_years = [y for y in candidate_years if y in set(years)]
    print(f"[signal_validity] years to run: {candidate_years}")

    # 1-min coverage window (authoritative from RND).
    win_lo, win_hi, one_min_exists = _one_min_window(rnd_db)
    if not one_min_exists:
        print("[signal_validity] ohlc_1min ABSENT in RND DB — Part B skipped "
              "(all intraday fields NULL).")
        win_lo = win_lo or ONE_MIN_START
        win_hi = win_hi or ONE_MIN_END
    else:
        print(f"[signal_validity] ohlc_1min window: {win_lo} -> {win_hi}")

    # Per-symbol signal-date close lookup (for the gap/validity base): the bar
    # whose date == signal_date. Build once from PROD bars.
    # ── PASS 1: build every top-N signal row (Part A + Part B) ──
    rows: List[Dict[str, Any]] = []
    for Y in candidate_years:
        sigs_by_sd = sds._year_signals_by_signal_date(ctx, Y)
        n_year = 0
        for sd, group in sigs_by_sd.items():
            ranked = sorted(group, key=lambda s: s["score"], reverse=True)[:top_n]
            for pos, sig in enumerate(ranked, start=1):
                sym = sig["symbol"]
                bs = bars.get(sym)
                if not bs:
                    continue
                # signal-date close (the price the signal fired at).
                sd_close = next((b["close"] for b in bs if b["date"] == sd), None)
                # next-open entry bar index (core:354).
                sd_idx = next((i for i, b in enumerate(bs) if b["date"] > sd), None)
                if sd_idx is None:
                    continue
                part_a = compute_part_a_row(sig, bs, sd_idx, sd_close, pos, run_cfg)
                if one_min_exists:
                    part_b = compute_part_b_row(
                        rnd_db, sig, bs, sd_idx, sd_close,
                        part_a["eod_signal_outcome"], run_cfg, win_lo, win_hi)
                else:
                    # ohlc_1min absent → all intraday fields NULL (never fabricated).
                    part_b = {
                        "valid_at_next_10am": None, "valid_at_next_11am": None,
                        "valid_at_next_1pm": None, "detected_intraday": None,
                        "intraday_detection_time": None, "intraday_entry_price": None,
                        "intraday_early_entry_outcome": None,
                        "intraday_false_positive": None, "false_positive_reason": None,
                        "_in_window": 0,
                    }
                part_a["_intraday"] = part_b
                rows.append(part_a)
                n_year += 1
        print(f"[signal_validity]   {Y}: {n_year} signals")

    # ── Per-pattern aggregation (taxonomy signal-timing) ──
    sig_to_pats = _load_signal_to_patterns(rnd_db)
    if not sig_to_pats:
        print("[signal_validity] falcon_pattern_contributions absent/empty "
              "(persona 'falcon_top10') — per-pattern taxonomy update will be a "
              "NO-OP (run build_baseline.py first).")
    tax_rows, tax_stats = compute_pattern_taxonomy(rows, sig_to_pats)

    # ── Summaries for the report ──
    decay_summary = compute_decay_summary(rows)
    coverage = compute_intraday_coverage(rows)
    coverage["_win_lo"] = win_lo
    coverage["_win_hi"] = win_hi

    # ── Print summary ──
    n_valid_open = sum(1 for r in rows if r["valid_at_next_open"] == 1)
    n_valid_den = sum(1 for r in rows if r["valid_at_next_open"] is not None)
    print(f"\n[signal_validity] ── PART A — freshness + decay ──")
    print(f"  total signals: {len(rows)}")
    print(f"  valid_at_next_open: {n_valid_open}/{n_valid_den} "
          f"({_rate(n_valid_open, n_valid_den):.1f}%)" if n_valid_den else
          "  valid_at_next_open: n/a")
    print(f"  ── DECAY (avg net_ret by entry day) ──")
    print(f"  {'scope':>8} {'n_d1':>6} {'D+1':>8} {'n_d2':>6} {'D+2':>8} "
          f"{'n_d3':>6} {'D+3':>8} {'valid@open%':>11}")
    for r in decay_summary:
        def f(x):
            return f"{x:.2f}" if x is not None else "-"
        print(f"  {r['scope']:>8} {r['n_d1']:>6} {f(r['avg_ret_d1']):>8} "
              f"{r['n_d2']:>6} {f(r['avg_ret_d2']):>8} {r['n_d3']:>6} "
              f"{f(r['avg_ret_d3']):>8} {f(r['valid_at_open_pct']):>11}")

    print(f"\n[signal_validity] ── PART B — intraday (1-min) coverage ──")
    print(f"  in 1-min window:  {coverage['n_in_window']}")
    print(f"  out of window:    {coverage['n_out_window']}")
    print(f"  detected intraday:{coverage['n_detected_intraday']}")
    print(f"  compared (intraday vs next-open): {coverage['n_compared']}")
    print(f"  avg (intraday_early - next_open) ret %: "
          f"{_round(coverage['avg_intraday_minus_eod_delta'])}")
    print(f"  intraday-early win%: {_round(coverage['intraday_early_win_rate'])}  "
          f"next-open win%: {_round(coverage['eod_entry_win_rate'])}")
    print(f"  intraday detection accuracy %: "
          f"{_round(coverage['intraday_detection_accuracy'])}  "
          f"FP rate %: {_round(coverage['intraday_false_positive_rate'])}")

    print(f"\n[signal_validity] ── PER-PATTERN TAXONOMY (HFCL n>={HFCL_MIN_N}) ──")
    print(f"  patterns touched: {tax_stats['n_patterns']}")
    print(f"  validity/decay set (n>=18 signals): {tax_stats['n_validity_set']}")
    print(f"  intraday set (n>=18 in-window):     {tax_stats['n_intraday_set']}")

    if dry_run:
        print(f"\n[signal_validity] DRY-RUN — would write {len(rows)} "
              f"falcon_signal_validity rows + {tax_stats['n_patterns']} taxonomy "
              f"UPDATEs + the report. Nothing written.")
        return 0

    # ── APPLY: schema + write + Excel ──
    apply_schema(rnd_db)
    n_sv, n_tax, n_missing = _write(rnd_db, rows, tax_rows)
    path, mode = write_report(out_dir, tax_rows, decay_summary, coverage)
    print(f"\n[signal_validity] APPLY complete:")
    print(f"  falcon_signal_validity: {n_sv} rows (persona='{PERSONA_TAG}').")
    print(f"  taxonomy: {n_tax} rows updated; {n_missing} pattern_ids absent.")
    print(f"  report -> {path}  ({mode})")
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
        description="S4 — signal validity (freshness + decay) + intraday detection "
                    "comparison for the Falcon Top 10 signal universe."
    )
    p.add_argument("--rnd-db", default=None,
                   help="RND research DB (falcon_signal_validity + taxonomy + "
                        "ohlc_1min + pattern_contributions). All writes go here. "
                        "Default: persona resolver.")
    p.add_argument("--prod-db", default=None,
                   help="PROD DB for OHLC/features (default: config.POWER_DB_PATH). "
                        "READ-ONLY.")
    p.add_argument("--years", default=None,
                   help="Comma/range list, e.g. '2021,2022' or '2024-2026'. "
                        "Default: all persona-window years.")
    p.add_argument("--top-n", type=int, default=10,
                   help="Signals per signal_date (default: 10 = Falcon Top 10).")
    p.add_argument("--dry-run", action="store_true",
                   help="Compute + print summaries; write NOTHING.")
    p.add_argument("--out", default=str(_HERE / "out" / "v4"),
                   help="Output dir for the workbook (default: ./out/v4).")
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
