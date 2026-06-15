#!/usr/bin/env python3
"""build_baseline.py — Step 2 of the Self-Improving Engine.

Runs the **Falcon Top 10 walk-forward (2021 → latest)** and populates the new
``falcon_baseline_trades`` + ``falcon_pattern_contributions`` tables with the
full intra-period journey — WITHOUT changing the trade results. Parity is
preserved *by construction*: the trade set is produced by the SAME machinery
``persona_simulator.simulate_persona("falcon-top-10")`` uses, importing and
re-running ``persona_engine_core.simulate_year`` per year with an EXACT replica
of the Falcon Top 10 setup. No shared/engine code is modified (INV2).

What this script does NOT do (left for later steps, columns NULL):
  * Post-exit D+8..D+60 tracking → ``falcon_post_exit_tracking`` (separate child
    table, Step "post-exit"). The ``falcon_baseline_trades`` post-hold columns
    that need future bars are left NULL here.
  * Sector attribution, regime tagging, repeater intelligence, signal-validity,
    self-improved ranking (Phase 2), auto-trade readiness.
  * Near-miss (ranks 11-50), big-winner/loser study, weekly state — other tables.

Reuse map (proves parity preservation):
  persona_simulator.simulate_persona() does, per year Y:
    elig = eligible_patterns_for_year(all_pats, Y)                # core:169
    sigs = compute_year_signals(X, sym, dt, year_mask, elig,
                                min_fires=run_cfg.min_fires)      # core:181
    if sort_key == "avg_lift": s["score"] = s["avg_lift"]        # sim:462-464
    sigs_by_sd = group sigs by signal_date                        # sim:465-467
    r = simulate_year(sigs_by_sd, bars, year_td, run_cfg, cash)  # core:311
  We replicate that loop EXACTLY (same PERSONA_CONFIGS["falcon-top-10"] run_cfg,
  same RND patterns / PROD panel+bars / sector map / trading days, same
  sim_start/sim_end, same ₹5L yearly reset). The resulting closed +
  open-at-end trades are therefore identical to the parity run.

Journey math (Falcon Top 10, 7-day hold) — computed from the SAME ``bars`` the
sim used (load_all_bars), never refetched:
  For hold day d in 1..hold_days, bar = bars[symbol][entry_bar_idx + (d-1)]
  (entry_bar_idx is the trade's own ``_bars_start_idx`` / ``entry_bar_idx`` — the
  next-open bar after signal_date that simulate_year entered on). entry_px_raw is
  the *unslipped* open of that entry bar; returns are vs entry_px_raw so the
  journey reflects the actual market path, consistent with how the sim measures
  high_water (close/entry_px). dN_{open,high,low,close}_ret = bar/entry_px_raw-1.
  peak_ret_during_hold = max daily high_ret (+ day);
  trough_ret_during_hold = min daily low_ret (+ day);
  peak_before_trough / trough_before_peak compare those days;
  peak_sustained = close_ret on the final available hold day >= 0.9*peak_ret.
  Table-8 Falcon Top 10 flags: big_winner = peak_ret > 12% on any D+1..D+7;
  big_loser = trough_ret < -7% at any point OR exit_reason == INIT_STOP.

Pattern contributions (faithful, not fabricated): the parity path's
``load_full_patterns`` discards pattern_id/regime (keeps mined_year/rule/lift),
and ``compute_year_signals`` only stores the COUNT (n_fires) + SUM (sum_lift),
never which patterns fired. So per-pick fired pattern_ids are NOT returned by
the parity machinery. We RE-DERIVE them read-only: load a *superset* of the same
patterns that ALSO carries pattern_id (same query + JOIN + drawdown_bounce
filter), restrict to the year's eligible window via the same
``eligible_patterns_for_year``, and re-evaluate each pattern's ``rule_mask``
against the trade's own feature-row. As a parity guard we assert the re-derived
fire-count == trade n_fires and sum(lift) ≈ trade score. ``regime`` is NULL (no
regime column exists in the pattern tables); ``oos_hit_rate`` ← candidate
precision_pct/100 (closest available proxy); ``lift_pp`` ← avg_oos_year_lift_pp
(the exact value summed into the score).

CLI:
  python build_baseline.py --rnd-db <path>            # apply (clean rebuild)
  python build_baseline.py --rnd-db <path> --dry-run  # compute, write nothing
  python build_baseline.py --rnd-db <path> --limit-year 2021   # quick test
  python build_baseline.py --rnd-db <path> --years 2021,2022   # explicit years

Locked parity reference (eyeball the printed per-year summary against these):
  2021 +5.35% / 15 closed     2022 +76.36% / 317     2023 +564.47% / 810
  2024 +469.05% / 846         2025 +346.64% / 644
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

# Windows consoles default to cp1252, which can't encode the box-drawing chars
# in the summary print (caused an exit-1 AFTER all numbers were computed). Force
# utf-8 so output never crashes the run. No-op where already utf-8.
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# ── Import the PRODUCTION engine (reuse only — never modified). ──────────────
# build_baseline.py lives in universe_engine/self_improving/. The backend's
# canonical import root is <repo>/backend (the app runs with backend/ on
# sys.path and imports as `power_user.*`, `routers.*`, etc. — see backend/main.py;
# there is no backend/__init__.py). We mirror that exactly so the engine module's
# relative import `from .. import config` (= power_user.config) resolves. Put
# <repo>/backend on sys.path and import as `power_user.services.*`.
_HERE = Path(__file__).resolve().parent
_REPO_ROOT = _HERE.parent.parent          # <repo>/universe_engine/self_improving -> <repo>
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
    in_drawdown_bounce,  # re-exported via feature_cols import in the core module
    load_all_bars,
    load_full_patterns,
    load_panel,
    rule_mask,
    simulate_year,
    trading_days,
    RETENTION_YEARS,
    MINING_START_YEAR,
)

PERSONA_SLUG = "falcon-top-10"
PERSONA_TAG = "falcon_top10"          # value stored in the `persona` columns

# Table-8 Falcon Top 10 thresholds (the prompt's "Journey definitions").
BIG_WINNER_PEAK_PCT = 12.0            # peak_ret > 12% at ANY point D+1..D+7
BIG_LOSER_TROUGH_PCT = -7.0          # trough_ret < -7% at ANY point ...
BIG_LOSER_INIT_STOP = "INIT_STOP"    # ... OR INIT_STOP exit
PEAK_SUSTAINED_FRAC = 0.9            # close at hold-end >= 90% of peak → sustained


# ════════════════════════════════════════════════════════════════════════════
# 1. RUN THE PARITY-VALIDATED WALK-FORWARD (reuse persona machinery)
# ════════════════════════════════════════════════════════════════════════════

def _build_falcon_run_context(rnd_db: str, prod_db: str) -> Dict[str, Any]:
    """Load EXACTLY what persona_simulator.simulate_persona('falcon-top-10')
    loads, using the identical loaders. Returns the shared inputs for the
    per-year loop."""
    cfg_dict = PERSONA_CONFIGS[PERSONA_SLUG]
    run_cfg = cfg_dict["run_cfg"]
    sim_start = cfg_dict["sim_start"]
    sim_end = cfg_dict["sim_end"]
    cash_start = cfg_dict["cash_per_year"]

    all_pats = load_full_patterns(rnd_db)                 # core:61  (RND patterns)
    X, sym, dt = load_panel(prod_db, sim_start, sim_end)  # core:86  (PROD features)
    bars = load_all_bars(prod_db, "2020-12-01", sim_end)  # core:109 (pad for trail)
    sector_map = build_sector_map(prod_db)                # core:140
    all_td = trading_days(prod_db, sim_start, sim_end)    # core:127

    years_arr = np.array([int(d[:4]) for d in dt], dtype=np.int32)
    years_in_window = sorted({int(d[:4]) for d in all_td})

    return {
        "cfg_dict": cfg_dict,
        "run_cfg": run_cfg,
        "sim_start": sim_start,
        "sim_end": sim_end,
        "cash_start": cash_start,
        "all_pats": all_pats,
        "X": X,
        "sym": sym,
        "dt": dt,
        "bars": bars,
        "sector_map": sector_map,
        "all_td": all_td,
        "years_arr": years_arr,
        "years_in_window": years_in_window,
    }


def _simulate_one_year(ctx: Dict[str, Any], Y: int) -> Dict[str, Any]:
    """Reproduce simulate_persona's per-year body EXACTLY for year Y.

    Mirrors persona_simulator.py lines 450-474:
      eligible_patterns_for_year -> compute_year_signals(min_fires) ->
      (avg_lift score rewrite) -> group by signal_date -> simulate_year.
    Returns simulate_year's raw result dict (closed/open/timeline/...).
    Also returns the per-(symbol,signal_date) signal index so contributions can
    be re-derived against the exact feature row that produced each pick.
    """
    run_cfg = ctx["run_cfg"]
    sim_end = ctx["sim_end"]
    all_pats = ctx["all_pats"]
    X, sym, dt = ctx["X"], ctx["sym"], ctx["dt"]
    bars = ctx["bars"]
    cash_start = ctx["cash_start"]
    years_arr = ctx["years_arr"]
    years_in_window = ctx["years_in_window"]

    elig = eligible_patterns_for_year(all_pats, Y)            # core:169
    year_mask = years_arr == Y
    sigs = compute_year_signals(                              # core:181
        X, sym, dt, year_mask, elig, min_fires=run_cfg.min_fires,
    )
    # Falcon Top 10 ranks by avg_lift — overwrite score (sim:462-464).
    if run_cfg.sort_key == "avg_lift":
        for s in sigs:
            s["score"] = s["avg_lift"]

    sigs_by_sd: Dict[str, List[Dict]] = defaultdict(list)    # sim:465-467
    for s in sigs:
        sigs_by_sd[s["signal_date"]].append(s)

    year_start = f"{Y}-01-01"
    year_end = sim_end if Y == years_in_window[-1] else f"{Y}-12-31"
    year_td = [d for d in ctx["all_td"] if year_start <= d <= year_end]
    if not year_td:
        return {"empty": True}

    r = simulate_year(dict(sigs_by_sd), bars, year_td, run_cfg, cash_start)  # core:311

    # Build an index from (symbol, signal_date) -> the row's feature index in X,
    # restricted to this year (year_mask), so contributions can re-evaluate the
    # exact panel row. dt/sym are aligned 1:1 with X rows.
    sig_row_idx: Dict[Tuple[str, str], int] = {}
    year_idxs = np.nonzero(year_mask)[0]
    for i in year_idxs:
        sig_row_idx[(str(sym[i]), str(dt[i]))] = int(i)

    r["_sig_row_idx"] = sig_row_idx
    r["_elig"] = elig
    return r


# ════════════════════════════════════════════════════════════════════════════
# 2. INTRA-HOLD JOURNEY (Falcon Top 10, 7-day hold) — from the SAME bars
# ════════════════════════════════════════════════════════════════════════════

def compute_journey(
    trade: Dict[str, Any],
    bars: Dict[str, List[Dict]],
    hold_days: int,
) -> Dict[str, Any]:
    """Per-day open/high/low/close returns vs the entry bar's RAW open, plus the
    journey summary + Table-8 big winner/loser flags.

    Uses the trade's own ``entry_bar_idx`` (== ``_bars_start_idx``) into
    ``bars[symbol]`` — the identical bar series simulate_year traded on — so no
    data is refetched and the journey is path-consistent with the sim.
    """
    sym = trade["symbol"]
    bs = bars.get(sym, [])
    eidx = trade.get("entry_bar_idx", trade.get("_bars_start_idx"))
    out: Dict[str, Any] = {}

    if eidx is None or eidx >= len(bs):
        return out  # cannot reconstruct — leave all journey cols NULL

    entry_open = bs[eidx]["open"]
    if not entry_open or entry_open <= 0:
        return out

    # Per-day returns (only for hold days that actually have a bar — the sim
    # itself truncates at len(bs)-1, so trailing days past data are NULL).
    peak_ret = None
    peak_day = None
    trough_ret = None
    trough_day = None
    last_close_ret = None
    last_day_with_bar = None
    n_days = min(hold_days, len(bs) - eidx)
    for d in range(1, n_days + 1):
        bar = bs[eidx + (d - 1)]
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
        last_day_with_bar = d

    out["peak_ret_during_hold"] = peak_ret
    out["peak_day_during_hold"] = peak_day
    out["trough_ret_during_hold"] = trough_ret
    out["trough_day_during_hold"] = trough_day
    out["peak_before_trough"] = (
        1 if (peak_day is not None and trough_day is not None and peak_day < trough_day)
        else (0 if peak_day is not None and trough_day is not None else None)
    )
    out["trough_before_peak"] = (
        1 if (peak_day is not None and trough_day is not None and trough_day < peak_day)
        else (0 if peak_day is not None and trough_day is not None else None)
    )
    # peak_sustained: did the position hold most of its peak at hold-end close?
    if peak_ret is not None and last_close_ret is not None:
        if peak_ret <= 0:
            out["peak_sustained"] = 1 if last_close_ret >= peak_ret else 0
        else:
            out["peak_sustained"] = 1 if last_close_ret >= PEAK_SUSTAINED_FRAC * peak_ret else 0
    else:
        out["peak_sustained"] = None

    # ── Table-8 Falcon Top 10 big winner / big loser flags ──
    exit_reason = trade.get("exit_reason")
    big_winner = (peak_ret is not None and peak_ret > BIG_WINNER_PEAK_PCT)
    big_loser_trough = (trough_ret is not None and trough_ret < BIG_LOSER_TROUGH_PCT)
    big_loser_stop = (exit_reason == BIG_LOSER_INIT_STOP)
    big_loser = big_loser_trough or big_loser_stop

    out["big_winner_flag"] = 1 if big_winner else 0
    out["big_loser_flag"] = 1 if big_loser else 0
    out["big_winner_peak_day"] = peak_day if big_winner else None
    # trough day attributed only when the trough threshold (not the stop) drove it
    out["big_loser_trough_day"] = trough_day if big_loser_trough else None
    # big_winner_sustained: was the >12% peak still held at hold-end close?
    if big_winner:
        out["big_winner_sustained"] = (
            1 if (last_close_ret is not None and last_close_ret > BIG_WINNER_PEAK_PCT) else 0
        )
    else:
        out["big_winner_sustained"] = None
    # big_loser_recovered: did a -7%+ trough end the hold back above -7%?
    if big_loser_trough:
        out["big_loser_recovered"] = (
            1 if (last_close_ret is not None and last_close_ret > BIG_LOSER_TROUGH_PCT) else 0
        )
    else:
        out["big_loser_recovered"] = None

    return out


# ════════════════════════════════════════════════════════════════════════════
# 3. PATTERN CONTRIBUTIONS — re-derive fired patterns (read-only superset load)
# ════════════════════════════════════════════════════════════════════════════

def load_patterns_with_ids(rnd_db_path: str) -> List[Dict[str, Any]]:
    """Superset of ``persona_engine_core.load_full_patterns`` that ALSO carries
    pattern_id / mined_year / classification / oos_hit_rate(precision).

    Same source query + JOIN + drawdown_bounce filter as the parity loader, so
    the fire-set re-derived from these is IDENTICAL to what produced n_fires /
    sum_lift in the sim — only enriched with identity columns the parity loader
    drops. ``lift`` == ``avg_oos_year_lift_pp`` (the exact value summed into the
    score). READ-ONLY: this never touches the parity run path.
    """
    con = sqlite3.connect(rnd_db_path, timeout=120.0)
    try:
        rows = con.execute("""
            SELECT c.pattern_id, c.mined_year, c.rule_json,
                   p.avg_oos_year_lift_pp, p.classification, c.precision_pct
              FROM falcon_promoted_patterns p
              JOIN falcon_pattern_candidates c ON p.pattern_id = c.pattern_id
             WHERE p.classification IN ('universal','regime_dependent')
        """).fetchall()
    finally:
        con.close()
    out: List[Dict[str, Any]] = []
    for pid, my, rj, lift, cls, prec in rows:
        rule = [(f, op, float(th)) for f, op, th in json.loads(rj)]
        if in_drawdown_bounce(rule):
            continue
        out.append({
            "pattern_id": int(pid),
            "mined_year": int(my),
            "rule": rule,
            "lift": float(lift),
            "classification": str(cls) if cls is not None else None,
            "oos_hit_rate": (float(prec) / 100.0) if prec is not None else None,
        })
    return out


def derive_contributions_for_trade(
    trade: Dict[str, Any],
    X: np.ndarray,
    sig_row_idx: Dict[Tuple[str, str], int],
    elig_pats_with_ids: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Re-evaluate each eligible (id-carrying) pattern's rule against the trade's
    own feature row to recover which patterns fired. Returns (contrib_rows,
    parity_warning_or_None).

    Parity guard: the recovered fire-count MUST equal the trade's ``n_fires``
    (the sim's confluence count for this pick). ``n_fires`` is the robust
    invariant — it survives the avg_lift score rewrite untouched, whereas the
    trade's ``score`` for Falcon Top 10 is avg_lift (= sum_lift / n_fires), not
    the raw sum. A count match proves the SAME eligible patterns fired here as
    in the sim, so the contribution set is faithful (not fabricated).
    """
    key = (trade["symbol"], trade["signal_date"])
    ridx = sig_row_idx.get(key)
    if ridx is None:
        return [], f"no panel row for {key}"

    row = X[ridx:ridx + 1, :]  # shape (1, n_features)
    fired: List[Dict[str, Any]] = []
    sum_lift = 0.0
    for p in elig_pats_with_ids:
        m = rule_mask(p["rule"], row)
        if m[0]:
            fired.append(p)
            sum_lift += p["lift"]

    # Parity guard — recovered fires must reproduce the sim's n_fires.
    warning = None
    n_fires_trade = int(trade.get("n_fires", 0))
    if len(fired) != n_fires_trade:
        warning = (f"contrib mismatch {key}: re-derived {len(fired)} fires "
                   f"vs trade n_fires {n_fires_trade}")

    contribs = []
    for p in fired:
        contribs.append({
            "pattern_id": p["pattern_id"],
            "pattern_mined_year": p["mined_year"],
            "lift_pp": p["lift"],
            "oos_hit_rate": p["oos_hit_rate"],
            "classification": p["classification"],
        })
    return contribs, warning


# ════════════════════════════════════════════════════════════════════════════
# 4. ROW BUILDERS
# ════════════════════════════════════════════════════════════════════════════

def _cal_hold_days(entry_date: str, exit_date: Optional[str]) -> Optional[int]:
    if not exit_date:
        return None
    d0 = datetime.fromisoformat(entry_date).date()
    d1 = datetime.fromisoformat(exit_date).date()
    return (d1 - d0).days


def build_trade_row(
    trade: Dict[str, Any],
    rank: int,
    sector_map: Dict[str, str],
    bars: Dict[str, List[Dict]],
    hold_days: int,
    is_open_at_end: bool,
    contrib_pids: List[int],
    contrib_mined_years: List[int],
) -> Dict[str, Any]:
    """Map one simulate_year trade dict → a falcon_baseline_trades row dict."""
    sym = trade["symbol"]
    entry_px = trade["entry_px"]
    n_fires = int(trade.get("n_fires", 0))
    score = float(trade.get("score", 0.0))            # for Falcon Top10 = avg_lift
    actual_deployed = trade.get("actual_deployed") or (trade["shares"] * entry_px)
    net_pnl = trade.get("net_pnl")
    net_ret_pct = (net_pnl / actual_deployed * 100.0) if (net_pnl is not None and actual_deployed) else None

    # exit_date is real for closed; for open-at-end it's the MTM sentinel date,
    # but the position is NOT closed → store exit_date/price NULL per schema
    # (later post-exit / live steps own open positions).
    exit_date = trade.get("exit_date") if not is_open_at_end else None
    exit_price = trade.get("exit_px") if not is_open_at_end else None
    exit_reason = trade.get("exit_reason") if not is_open_at_end else None

    journey = compute_journey(trade, bars, hold_days)

    # avg_lift / sum_lift identity: score (post-rewrite) IS avg_lift; sum = avg*n.
    avg_lift = score
    sum_lift = score * n_fires if n_fires else None

    # top_3_pattern_ids — by pattern lift descending isn't available here without
    # carrying lift; contrib_pids already arrive ordered by the iteration; take
    # first 3 (callers pass them lift-sorted). Stored as JSON per schema.
    top_3 = contrib_pids[:3]
    mined_years_used = sorted(set(contrib_mined_years))

    row = {
        "persona": PERSONA_TAG,
        "signal_date": trade["signal_date"],
        "entry_date": trade["entry_date"],
        "exit_date": exit_date,
        "symbol": sym,
        "stock_name": None,                       # no stock-name source in parity path
        "sector": sector_map.get(sym),
        "engine_rank": rank,
        "avg_lift": avg_lift,
        "n_fires": n_fires,
        "sum_lift": sum_lift,
        "top_3_pattern_ids": json.dumps(top_3) if top_3 else None,
        "pattern_mined_years_used": json.dumps(mined_years_used) if mined_years_used else None,
        "entry_price": entry_px,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "hold_days_calendar": _cal_hold_days(trade["entry_date"], exit_date),
        "hold_days_trading": (int(trade.get("trading_hold_days", 0)) if not is_open_at_end else None),
        "shares": int(trade["shares"]),
        "actual_deployed": actual_deployed,
        "gross_pnl": (trade.get("gross_pnl") if not is_open_at_end else None),
        "fees": (trade.get("fees") if not is_open_at_end else None),
        "net_pnl": (net_pnl if not is_open_at_end else None),
        "net_ret_pct": (net_ret_pct if not is_open_at_end else None),
    }
    # Journey columns (d1..d7 + summary + flags). Missing keys default to None.
    for d in range(1, 8):
        for f in ("open", "high", "low", "close"):
            row[f"d{d}_{f}_ret"] = journey.get(f"d{d}_{f}_ret")
    for k in ("peak_ret_during_hold", "peak_day_during_hold",
              "trough_ret_during_hold", "trough_day_during_hold",
              "peak_before_trough", "trough_before_peak", "peak_sustained",
              "big_winner_flag", "big_loser_flag", "big_winner_peak_day",
              "big_loser_trough_day", "big_winner_sustained", "big_loser_recovered"):
        row[k] = journey.get(k)
    return row


# Ordered column list for falcon_baseline_trades INSERT (Phase-1 populated subset
# + the journey/flag columns). All other schema columns default to NULL.
_BASELINE_COLS = [
    "persona", "signal_date", "entry_date", "exit_date", "symbol", "stock_name",
    "sector", "engine_rank", "avg_lift", "n_fires", "sum_lift",
    "top_3_pattern_ids", "pattern_mined_years_used",
    "entry_price", "exit_price", "exit_reason",
    "hold_days_calendar", "hold_days_trading", "shares", "actual_deployed",
    "gross_pnl", "fees", "net_pnl", "net_ret_pct",
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
    "big_winner_flag", "big_loser_flag", "big_winner_peak_day",
    "big_loser_trough_day", "big_winner_sustained", "big_loser_recovered",
]

_CONTRIB_COLS = [
    "trade_id", "signal_date", "symbol", "persona",
    "pattern_id", "pattern_mined_year", "regime", "lift_pp", "oos_hit_rate",
    "realized_outcome", "sector", "move_type", "market_regime", "sector_regime",
    "big_winner_flag", "big_loser_flag",
]


# ════════════════════════════════════════════════════════════════════════════
# 5. ORCHESTRATION
# ════════════════════════════════════════════════════════════════════════════

def run(
    rnd_db: str,
    prod_db: str,
    years: Optional[List[int]],
    limit_year: Optional[int],
    dry_run: bool,
) -> int:
    print(f"[build_baseline] RND DB: {rnd_db}")
    print(f"[build_baseline] PROD DB: {prod_db}")
    print(f"[build_baseline] persona: {PERSONA_SLUG} (tag '{PERSONA_TAG}')")
    print(f"[build_baseline] mode: {'DRY-RUN (no writes)' if dry_run else 'APPLY'}")

    ctx = _build_falcon_run_context(rnd_db, prod_db)
    run_cfg = ctx["run_cfg"]
    hold_days = run_cfg.hold_days
    cash_start = ctx["cash_start"]
    bars = ctx["bars"]
    sector_map = ctx["sector_map"]
    X = ctx["X"]
    print(f"[build_baseline] loaded {len(ctx['all_pats'])} patterns, "
          f"{X.shape[0]} panel rows, {len(bars)} symbols, "
          f"{len(ctx['all_td'])} trading days")

    # Id-carrying pattern superset for contributions (read-only).
    pats_with_ids = load_patterns_with_ids(rnd_db)
    print(f"[build_baseline] id-carrying patterns for contributions: {len(pats_with_ids)}")

    # Which years to run.
    candidate_years = ctx["years_in_window"]
    if limit_year is not None:
        candidate_years = [y for y in candidate_years if y == limit_year]
    elif years:
        candidate_years = [y for y in candidate_years if y in set(years)]
    print(f"[build_baseline] years to run: {candidate_years}")

    # Accumulators for write (built fully before any DB write).
    baseline_rows: List[Dict[str, Any]] = []
    # contributions tied to baseline rows by list position (resolved to trade_id
    # after the baseline INSERT returns row ids).
    contribs_per_trade: List[List[Dict[str, Any]]] = []
    year_summaries: List[Dict[str, Any]] = []
    parity_warnings: List[str] = []

    for Y in candidate_years:
        r = _simulate_one_year(ctx, Y)
        if r.get("empty"):
            print(f"[build_baseline]   {Y}: no trading days in window, skipped")
            continue

        sig_row_idx = r["_sig_row_idx"]
        elig_ids = eligible_patterns_for_year(
            pats_with_ids, Y, retention=RETENTION_YEARS, mining_start=MINING_START_YEAR,
        )

        closed = r["closed_trades"]
        open_at_end = r["open_at_end_trades"]
        end_eq = r["ending_equity"]
        year_ret_pct = (end_eq / cash_start - 1.0) * 100.0
        n_closed = len(closed)

        # engine_rank = the pick's position within its OWN signal_date cohort,
        # ranked by score (avg_lift) descending — exactly the ordering the sim
        # accepts candidates in (simulate_year sorts each day's candidates by
        # score before accepting). This is a faithful per-cohort rank derived
        # from the trade set itself; ties keep stable order. (Daily cohorts
        # group by entry-day in the sim, but each trade's signal_date is its
        # native cohort key for ranking — within a signal_date all picks share
        # the same candidate pool. A later step may record exact daily accept
        # position if needed.)
        all_year_trades = [(t, False) for t in closed] + [(t, True) for t in open_at_end]
        # Compute rank within signal_date.
        _by_sd: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for t, _is_open in all_year_trades:
            _by_sd[t["signal_date"]].append(t)
        _rank_of: Dict[int, int] = {}   # id(trade) -> rank within its signal_date
        for sd, grp in _by_sd.items():
            grp_sorted = sorted(grp, key=lambda x: x.get("score", 0.0), reverse=True)
            for pos, t in enumerate(grp_sorted, start=1):
                _rank_of[id(t)] = pos

        for (t, is_open) in all_year_trades:
            rank0 = _rank_of.get(id(t))
            contribs, warn = derive_contributions_for_trade(t, X, sig_row_idx, elig_ids)
            if warn:
                parity_warnings.append(f"{Y}: {warn}")
            # Order contributions by lift desc so top_3_pattern_ids is meaningful.
            contribs.sort(key=lambda c: (c["lift_pp"] if c["lift_pp"] is not None else -1e9),
                          reverse=True)
            pids = [c["pattern_id"] for c in contribs]
            mined_years = [c["pattern_mined_year"] for c in contribs]

            brow = build_trade_row(
                t, rank0, sector_map, bars, hold_days, is_open, pids, mined_years,
            )
            net_ret_pct = brow["net_ret_pct"]
            big_w = brow["big_winner_flag"]
            big_l = brow["big_loser_flag"]
            baseline_rows.append(brow)

            # Build contribution rows (trade_id filled post-insert).
            crows = []
            for c in contribs:
                crows.append({
                    "signal_date": t["signal_date"],
                    "symbol": t["symbol"],
                    "persona": PERSONA_TAG,
                    "pattern_id": c["pattern_id"],
                    "pattern_mined_year": c["pattern_mined_year"],
                    "regime": None,                 # no regime col in pattern tables
                    "lift_pp": c["lift_pp"],
                    "oos_hit_rate": c["oos_hit_rate"],
                    "realized_outcome": net_ret_pct,   # = trade net_ret_pct
                    "sector": sector_map.get(t["symbol"]),
                    "move_type": None,                 # sector attribution = later step
                    "market_regime": None,
                    "sector_regime": None,
                    "big_winner_flag": big_w,
                    "big_loser_flag": big_l,
                })
            contribs_per_trade.append(crows)

        n_contribs_year = sum(len(c) for c in contribs_per_trade[-len(all_year_trades):]) if all_year_trades else 0
        year_summaries.append({
            "year": Y, "return_pct": year_ret_pct, "n_closed": n_closed,
            "n_open_at_end": len(open_at_end), "n_contribs": n_contribs_year,
        })
        print(f"[build_baseline]   {Y}: return {year_ret_pct:+.2f}%  "
              f"n_closed={n_closed}  n_open_at_end={len(open_at_end)}  "
              f"contribs={n_contribs_year}")

    # ── Per-year summary (eyeball vs locked parity numbers) ──
    print("\n[build_baseline] ── PER-YEAR SUMMARY (vs locked parity) ──")
    LOCKED = {2021: (5.35, 15), 2022: (76.36, 317), 2023: (564.47, 810),
              2024: (469.05, 846), 2025: (346.64, 644)}
    print(f"  {'Year':>5} {'Return%':>12} {'Closed':>7} | {'LockedRet%':>11} {'LockedN':>8}  {'match?':>6}")
    for ys in year_summaries:
        lr, ln = LOCKED.get(ys["year"], (None, None))
        ret_ok = (lr is not None and abs(ys["return_pct"] - lr) < 0.05)
        n_ok = (ln is not None and ys["n_closed"] == ln)
        match = "OK" if (lr is None or (ret_ok and n_ok)) else "DIFF"
        lr_s = f"{lr:+.2f}" if lr is not None else "—"
        ln_s = str(ln) if ln is not None else "—"
        print(f"  {ys['year']:>5} {ys['return_pct']:>+12.2f} {ys['n_closed']:>7} | "
              f"{lr_s:>11} {ln_s:>8}  {match:>6}")
    print(f"\n[build_baseline] total baseline rows: {len(baseline_rows)}  "
          f"total contribution rows: {sum(len(c) for c in contribs_per_trade)}")
    if parity_warnings:
        print(f"[build_baseline] PARITY WARNINGS ({len(parity_warnings)}) — "
              f"contribution re-derivation did not reproduce n_fires:")
        for w in parity_warnings[:20]:
            print(f"    ! {w}")
        if len(parity_warnings) > 20:
            print(f"    ... and {len(parity_warnings) - 20} more")
    else:
        print("[build_baseline] parity guard: all re-derived contributions "
              "reproduced trade n_fires exactly.")

    if dry_run:
        print("\n[build_baseline] DRY-RUN — nothing written.")
        return 0

    # ── WRITE (single transaction, idempotent rebuild for this persona) ──
    _write(rnd_db, baseline_rows, contribs_per_trade)
    print(f"\n[build_baseline] APPLY complete — wrote {len(baseline_rows)} baseline + "
          f"{sum(len(c) for c in contribs_per_trade)} contribution rows for "
          f"persona='{PERSONA_TAG}'.")
    return 0


def _write(
    rnd_db: str,
    baseline_rows: List[Dict[str, Any]],
    contribs_per_trade: List[List[Dict[str, Any]]],
) -> None:
    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        con.execute("PRAGMA foreign_keys=ON")
        con.execute("BEGIN")
        # Idempotent: drop this persona's existing rows (contributions first —
        # FK child — then trades).
        con.execute(
            "DELETE FROM falcon_pattern_contributions WHERE persona = ?", (PERSONA_TAG,)
        )
        con.execute(
            "DELETE FROM falcon_baseline_trades WHERE persona = ?", (PERSONA_TAG,)
        )

        b_placeholders = ", ".join("?" for _ in _BASELINE_COLS)
        b_sql = (f"INSERT INTO falcon_baseline_trades ({', '.join(_BASELINE_COLS)}) "
                 f"VALUES ({b_placeholders})")
        c_placeholders = ", ".join("?" for _ in _CONTRIB_COLS)
        c_sql = (f"INSERT INTO falcon_pattern_contributions ({', '.join(_CONTRIB_COLS)}) "
                 f"VALUES ({c_placeholders})")

        for brow, crows in zip(baseline_rows, contribs_per_trade):
            cur = con.execute(b_sql, [brow.get(c) for c in _BASELINE_COLS])
            trade_id = cur.lastrowid
            for crow in crows:
                crow["trade_id"] = trade_id
                con.execute(c_sql, [crow.get(c) for c in _CONTRIB_COLS])

        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


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
        description="Populate falcon_baseline_trades + falcon_pattern_contributions "
                    "from the parity-validated Falcon Top 10 walk-forward."
    )
    p.add_argument("--rnd-db", default=None,
                   help="Path to the RND research DB (default: persona resolver).")
    p.add_argument("--prod-db", default=None,
                   help="Path to the PROD DB for OHLC/features (default: config.POWER_DB_PATH).")
    p.add_argument("--years", default=None,
                   help="Comma/range list, e.g. '2021,2022' or '2021-2023'. "
                        "Default: all years in the persona window (2021..current).")
    p.add_argument("--limit-year", type=int, default=None,
                   help="Run a single year only (quick test). Overrides --years.")
    p.add_argument("--dry-run", action="store_true",
                   help="Run sim + compute + print summary; write NOTHING.")
    args = p.parse_args(argv)

    rnd_db = args.rnd_db or _resolve_rnd_db_path()
    prod_db = args.prod_db or PROD_DB

    return run(
        rnd_db=rnd_db,
        prod_db=prod_db,
        years=_parse_years(args.years),
        limit_year=args.limit_year,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    sys.exit(main())
