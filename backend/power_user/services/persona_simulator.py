"""DRAFT — Persona simulator (single source of truth for backtest data).

Replaces the 5 desktop Excel files. Production code calls
`simulate_persona(persona_id)` to get fresh backtest results — no Excel
upload, no manual refresh.

CONFIG-DRIVEN: each persona is one entry in PERSONA_CONFIGS. To change a
persona's behaviour (top_n, SL, target, hold, etc.) — edit the dict
entry and restart the backend. No code changes elsewhere.

NUMBERS COME OUT IDENTICAL to the desktop Excel files. The audit Excels
remain on operator's Desktop as the canonical artefact for offline review,
but production no longer depends on them.

USAGE:
    from .persona_simulator import simulate_persona
    result = simulate_persona("daily-trader")
    # result["yearly"]   → list of {year, return_pct, max_dd, wr, n_closed}
    # result["monthly"]  → list of {year, month, end_equity, return_pct}
    # result["trades"]   → list of trade dicts (entry/exit/pnl/etc.)
    # result["summary"]  → aggregate {avg_yearly_return, median, worst, best, ...}
    # result["open_positions"] → currently-open positions in the simulator
    # result["last_updated"] → ISO timestamp of when this run completed
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np

from .. import config
from .persona_engine_core import (
    PersonaRunConfig,
    build_sector_map,
    compute_year_signals,
    eligible_patterns_for_year,
    entry_filter_daily,
    entry_filter_tuesday,
    first_td_of_month_set,
    load_all_bars,
    load_full_patterns,
    load_panel,
    simulate_year,
    trading_days,
)

log = logging.getLogger("kanida.power_user.persona_simulator")
IST = timezone(timedelta(hours=5, minutes=30))


# ════════════════════════════════════════════════════════════════════════
# DB PATH RESOLUTION
# ════════════════════════════════════════════════════════════════════════
# RND DB has full pattern catalog. PROD DB has fresh OHLC + features and is
# what the live engine uses. Both paths come from the same _REPO_ROOT used
# by config.POWER_DB_PATH so we stay consistent with the rest of the backend.

def _resolve_rnd_db_path() -> str:
    """RND DB path. Prefer POWER_RND_DB_PATH (env-overridable): in the cloud the
    universe_engine R&D DB is absent, so this resolves to the local serving DB
    (carries pattern catalog + OHLC + features). Falls back to the R&D path
    (laptop) then a dev-adjacent copy. (#3 serve-time dep.)"""
    envp = getattr(config, "POWER_RND_DB_PATH", None)
    if envp and Path(envp).exists():
        return str(envp)
    p = Path(config.POWER_DB_PATH).parent.parent.parent / "universe_engine" / "data" / "db" / "kanida_universe.db"
    if p.exists():
        return str(p)
    # Fallback (development): some installs have RND alongside PROD
    alt = Path(config.POWER_DB_PATH).parent / "kanida_universe_rnd.db"
    if alt.exists():
        return str(alt)
    raise FileNotFoundError(f"RND DB not found (POWER_RND_DB_PATH={envp}, tried {p})")


def _resolve_intraday_db_path() -> str:
    """Intraday mining DB — holds intraday_dataset_v2 used by P2's 9:30 filter."""
    p = Path(config.POWER_DB_PATH).parent.parent.parent / "universe_engine" / "data" / "db" / "intraday_mining.db"
    if not p.exists():
        raise FileNotFoundError(f"Intraday DB not found at {p}")
    return str(p)


PROD_DB = config.POWER_DB_PATH      # uses existing resolution from config.py


# P2 intraday filter thresholds — match sim_p2_v3_fixed35k.py exactly.
P2_RET_THRESHOLD_PCT = 0.0          # first_15_ret > 0 (strict)
P2_VOL_THRESHOLD_PCT = 5.0          # first_15_vol > 5% of yesterday's full-day volume


def _load_intraday_lookup() -> Dict[tuple, tuple]:
    """Load `intraday_dataset_v2` → {(symbol, entry_date): (first_15_ret_pct,
    first_15_vol_pct_of_yest)} for P2's 9:30 walk-down filter."""
    import sqlite3
    path = _resolve_intraday_db_path()
    out: Dict[tuple, tuple] = {}
    con = sqlite3.connect(path, timeout=60.0)
    try:
        for sym, ed, ret15, vol15 in con.execute("""
            SELECT symbol, entry_date,
                   today_first_15_ret_pct,
                   today_first_15_vol_pct_of_yest
              FROM intraday_dataset_v2
        """):
            out[(sym, ed)] = (ret15, vol15)
    finally:
        con.close()
    return out


def _make_p2_intraday_filter(lookup: Dict[tuple, tuple]) -> Any:
    """Build a closure that the engine_core's simulate_year calls per
    candidate. Returns either None (rejected) or the entry multiplier
    (1 + first_15_ret/100). Math + thresholds match sim_p2_v3_fixed35k.py."""
    import math as _math

    def _filter(c: Dict[str, Any], entry_date: str) -> Optional[float]:
        key = (c["symbol"], entry_date)
        if key not in lookup:
            return None                           # no intraday data → reject
        ret15, vol15 = lookup[key]
        # Reject if either field is missing or NaN
        if ret15 is None or vol15 is None:
            return None
        try:
            if _math.isnan(float(ret15)) or _math.isnan(float(vol15)):
                return None
        except (TypeError, ValueError):
            return None
        if float(ret15) <= P2_RET_THRESHOLD_PCT:    # 9:30 ret must be POSITIVE
            return None
        if float(vol15) <= P2_VOL_THRESHOLD_PCT:    # vol must be > 5% of yest
            return None
        # Pass — engine should compute entry at open × (1 + first_15_ret/100)
        return 1.0 + float(ret15) / 100.0
    return _filter


# ════════════════════════════════════════════════════════════════════════
# PERSONA CONFIGS — single source of truth
#
# To change a persona's locked rules: edit this dict, restart backend.
# Numbers in production refresh immediately on next API call.
# ════════════════════════════════════════════════════════════════════════

# 2026-05-27: Dynamic sim_end so every backend restart advances the backtest
# horizon to "today" (IST). Previously sim_end was a hardcoded date that froze
# behind real time — the Falcon Top 10 detail page showed 2026 YTD = +91.34%
# (sim through May 19) while the listing card showed +100.5% (live data through
# today via portfolio_engine.run_eod_for_date). Same engine, same data, two
# different cutoff dates → user-visible mismatch.
#
# Now: at module import time, compute _SIM_END_TODAY = today's IST date. Daily
# backend restart at 03:00 IST advances this to the new day. The persona cache
# (24h TTL) naturally rebuilds with the new horizon during the restart-window
# warm-up. Both the simulator's yearly aggregate and the live portfolio data
# now end on the same date.
#
# Personas with intraday filter (patient-trader) keep their data-availability-
# bounded sim_end since intraday data may lag (re-evaluated below).
_SIM_END_TODAY = datetime.now(IST).date().isoformat()

PERSONA_CONFIGS: Dict[str, Dict[str, Any]] = {

    "daily-trader": {
        "name": "The Daily Trader",
        "tagline": "Active trader who doesn't miss a single day's opportunity.",
        "risk_tier": "MEDIUM",
        "sim_start": "2021-01-01",
        "sim_end": "2026-05-14",            # extend as live data accumulates
        "yearly_reset": True,
        "cash_per_year": 5_00_000.0,
        "run_cfg": PersonaRunConfig(
            top_n=14,
            top_n_candidates=100,
            fixed_per_trade=35_000.0,
            hold_days=7,
            init_stop=-0.07,
            target=None,                    # use trail, no fixed target
            trail_trigger=0.12,
            trail_lookback=10,
            entry_filter=entry_filter_daily,
            # P1 reference (sim_p1_v3_fixed35k.py) groups candidates by
            # signal_date before accepting — keeps parity test at zero drift.
            group_by_signal_date=True,
        ),
        "source_sim_script": "sim_p1_v3_fixed35k.py",
    },

    "patient-trader": {
        "name": "The Patient Trader",
        "tagline": "Same engine as Daily Trader, but waits 15 minutes for the morning to confirm.",
        "risk_tier": "LOW-MEDIUM",
        "sim_start": "2024-05-13",          # capped by intraday data availability
        "sim_end": "2026-05-14",
        "yearly_reset": True,
        "cash_per_year": 5_00_000.0,
        "run_cfg": PersonaRunConfig(
            top_n=14,
            top_n_candidates=100,
            fixed_per_trade=35_000.0,
            hold_days=7,
            init_stop=-0.07,
            target=None,
            trail_trigger=0.12,
            trail_lookback=10,
            entry_filter=entry_filter_daily,
            # P2 reference (sim_p2_v3_fixed35k.py) does NOT group — it walks
            # the flat top-100 by score and accepts the first 14 that pass
            # the 9:30 intraday filter. Leave group_by_signal_date at False.
            # intraday_filter wired separately by simulate_persona() since it
            # needs the intraday_dataset_v2 lookup table loaded.
        ),
        "source_sim_script": "sim_p2_v3_fixed35k.py",
        "_uses_intraday_filter": True,
    },

    "weekly-trader": {
        "name": "The Weekly Trader",
        "tagline": "One day a week — Tuesday. Top 10 high-conviction picks. Hold to next Monday close.",
        "risk_tier": "MEDIUM-HIGH",
        "sim_start": "2021-01-01",
        "sim_end": "2026-05-14",
        "yearly_reset": True,
        "cash_per_year": 5_00_000.0,
        "run_cfg": PersonaRunConfig(
            top_n=10,
            top_n_candidates=100,
            fixed_per_trade=50_000.0,
            hold_days=5,                    # Tue → Wed → Thu → Fri → Mon close
            init_stop=-0.07,
            target=0.12,                    # fixed target, NO trail
            trail_trigger=None,
            entry_filter=entry_filter_tuesday,
        ),
        "source_sim_script": "sim_p3_v3_locked.py",
    },

    "monthly-trader": {
        # 2026-05-17 — switched to Alt G ("two-batch monthly cadence").
        # Previously: single entry on the 1st TD of each month, hold ~22 days.
        # Now:        batch 1 enters TD 0 / exits TD 14, batch 2 enters TD 15 /
        #             exits month-end. Capital recycles inside the month so it
        #             never sits idle for >1 trading day.
        # Requires `split_15_mode=True` on the run_cfg (engine v3.1 feature).
        "name": "The Monthly Trader",
        "tagline": "Twice a month — top 14 on day 1 and mid-month. Capital never sits idle.",
        "risk_tier": "LOWEST",
        "sim_start": "2021-01-01",
        "sim_end": "2026-05-14",
        "yearly_reset": True,
        "cash_per_year": 5_00_000.0,
        "run_cfg": PersonaRunConfig(
            top_n=14,
            top_n_candidates=100,
            fixed_per_trade=35_714.0,       # = 5L / 14
            hold_days=15,                   # batch-1 max hold; batch-2 forced to month-end
            init_stop=-0.10,                # -10% SL (unchanged from legacy P4)
            target=0.30,                    # +30% target (unchanged)
            trail_trigger=None,
            entry_filter=entry_filter_daily,    # ignored when split_15_mode=True
            split_15_mode=True,                 # ← Alt G two-batch entry/exit logic
        ),
        "source_sim_script": "sim_p4_alts_fullyear.py",
        # `_first_td_of_month_only` flag removed — split_15_mode handles both
        # batches internally; the wiring layer no longer needs to build
        # extra_entry_set for this persona.
    },

    "btst-trader": {
        "name": "The BTST Trader",
        "tagline": "Highest absolute returns. Buy at 9:15. Sell at next-day close. Repeat every day.",
        "risk_tier": "HIGHEST",
        "warning": "Lost 54% in 2021's regime shift. Not for risk-averse traders.",
        "sim_start": "2021-01-01",
        "sim_end": "2026-05-14",
        "yearly_reset": True,
        "cash_per_year": 5_00_000.0,
        "run_cfg": PersonaRunConfig(
            top_n=15,
            top_n_candidates=100,
            fixed_per_trade=33_333.0,       # = 5L / 15
            hold_days=2,                    # T0 entry + T1 close exit
            init_stop=-0.05,                # tighter SL
            target=0.07,                    # +7% target, 1:1.4 RR
            trail_trigger=None,
            entry_filter=entry_filter_daily,
        ),
        "source_sim_script": "sim_p5_v3_locked.py",
    },

    "falcon-top-10": {
        # 2026-05-21 — sixth persona, "Falcon Top 10".
        # Three knobs differ structurally from the default P3/P4/P5 simulator
        # branch (the engine's group_by_signal_date=False default):
        #   • sort_key="avg_lift"        → rank by quality-per-fire, not sum_lift
        #   • min_fires=10               → raises the confluence gate from 2 → 10
        #   • group_by_signal_date=True  → matches reference falcon_corrections.py:
        #       group raw cands by signal_date, take top_n per group, union,
        #       resort by score. NO daily accepted-count cap — relies on cash
        #       (₹50K × 10 = ₹5L). Without this flag the engine inherits the
        #       flat-top-100 branch with a hard top_n=10/day ceiling, which
        #       diverges from the reference in dense-signal years (2022-2025).
        #
        # Trade size = ₹50,000 (= ₹5L / top_n=10), not P1's ₹35,000.
        # SL / trail / hold identical to P1: −7% init stop, +12% trail trigger,
        # 10-day Donchian low as trail floor, max 7-TD hold.
        "name":          "Falcon Top 10",
        "tagline":       "Engine's 10 highest-quality daily picks. Full ₹5L deployed, 7-day hold.",
        "risk_tier":     "MEDIUM",
        "sim_start":     "2021-01-01",
        # 2026-05-27: dynamic — advances daily via the 03:00 IST backend restart.
        # Persona cache (24h TTL) rebuilds during the warm-up window after each
        # restart, so the simulator's 2026 yearly aggregate now ends on the
        # same date as the live portfolio_engine output → both views agree.
        "sim_end":       _SIM_END_TODAY,
        "yearly_reset":  True,
        "cash_per_year": 5_00_000.0,
        "run_cfg": PersonaRunConfig(
            top_n=10,
            top_n_candidates=100,
            fixed_per_trade=50_000.0,       # = 5L / 10
            hold_days=7,
            init_stop=-0.07,
            target=None,                    # use trail, no fixed target
            trail_trigger=0.12,
            trail_lookback=10,
            entry_filter=entry_filter_daily,
            sort_key="avg_lift",            # rank by sum_lift / n_fires
            min_fires=10,                   # high-confluence gate
            group_by_signal_date=True,      # matches reference candidate policy
        ),
        "source_sim_script": "falcon_top10_5yr_walkforward_CORRECTED.xlsx",
    },
}


# ════════════════════════════════════════════════════════════════════════
# IN-MEMORY CACHE (keep one full sim result per persona, TTL'd)
# ════════════════════════════════════════════════════════════════════════
# Cost per persona run: ~30-90 sec (P5 BTST is heaviest, ~85 sec).
# Cache TTL: 1 hour. Force-refresh via `simulate_persona(slug, force=True)`.

_CACHE: Dict[str, Dict[str, Any]] = {}
# 2026-05-26: bumped from 1h to 24h. Persona output only changes when
# PERSONA_CONFIGS is edited (rare — operator action). Backend auto-restarts
# daily at 03:00 IST via Task Scheduler; warm_cache.bat absorbs the 149s
# rebuild during that restart window. With 24h TTL, the persona sim NEVER
# rebuilds during user-facing hours → no more 2-minute hangs on /power/today
# when the previous 1h TTL expired mid-day.
_CACHE_TTL_SECONDS = 86400

# ── SINGLE-FLIGHT (2026-07-16, DoS hardening) ────────────────────────────
# The persona GET endpoints are intentionally PUBLIC (user-facing: Co-Trading
# and /power/portfolios/{slug} call them with no auth), so they cannot be
# token-gated without breaking those pages. The abuse vector is therefore a
# CACHE STAMPEDE: on a cold/expired cache, N concurrent requests each launched
# their own 30-90s sim (P5 BTST ~85s), so a handful of parallel requests could
# pin the CPU indefinitely.
#
# Fix: one lock per slug. The first caller runs the sim; concurrent callers
# block on the lock and then re-check the cache, so they return the first
# caller's result instead of recomputing. Bounded work = 1 sim per slug per
# TTL, regardless of request volume. Purely additive: same result, same cache
# semantics, no signature change. `force=True` (admin-gated) still recomputes,
# but is serialised per slug so it can't be used to stack parallel sims either.
_LOCKS: Dict[str, threading.Lock] = {}
_LOCKS_GUARD = threading.Lock()


def _slug_lock(slug: str) -> threading.Lock:
    """Get-or-create the per-slug sim lock (guarded, so the dict itself is safe)."""
    with _LOCKS_GUARD:
        lock = _LOCKS.get(slug)
        if lock is None:
            lock = threading.Lock()
            _LOCKS[slug] = lock
        return lock


def _cache_valid(slug: str) -> bool:
    entry = _CACHE.get(slug)
    if entry is None:
        return False
    age = time.time() - entry["_cached_at"]
    return age < _CACHE_TTL_SECONDS


# ════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ════════════════════════════════════════════════════════════════════════

def simulate_persona(persona_slug: str, force: bool = False) -> Dict[str, Any]:
    """Run the backtest for one persona (single-flighted). Returns the result dict.

    Public entry point. Behaviour is unchanged for callers; this only collapses
    CONCURRENT cold-cache calls for the same slug into ONE sim run (see the
    single-flight note above) so the public persona endpoints can't be used to
    stack unbounded 30-90s simulations. Result shape/caching are identical.
    """
    if persona_slug not in PERSONA_CONFIGS:
        raise ValueError(f"Unknown persona: {persona_slug}. "
                          f"Valid: {list(PERSONA_CONFIGS.keys())}")

    # Fast path: warm cache, no lock contention at all (the common case).
    if not force and _cache_valid(persona_slug):
        log.info("persona_simulator: cache hit for %s", persona_slug)
        return _CACHE[persona_slug]["result"]

    with _slug_lock(persona_slug):
        # Re-check under the lock: while we waited, the caller that held the
        # lock may have just populated the cache → reuse it, don't recompute.
        if not force and _cache_valid(persona_slug):
            log.info("persona_simulator: cache hit after single-flight wait for %s",
                     persona_slug)
            return _CACHE[persona_slug]["result"]
        return _simulate_persona_uncached(persona_slug, force=force)


def _simulate_persona_uncached(persona_slug: str, force: bool = False) -> Dict[str, Any]:
    """Run the backtest for one persona. Returns the full result dict.

    INTERNAL — call `simulate_persona()` instead (it single-flights this).

    Cached for `_CACHE_TTL_SECONDS` (1 hour) per persona. Pass `force=True`
    to bypass cache (e.g., after a config change).

    Output dict shape:
      {
        "persona_id": str,
        "name": str,
        "tagline": str,
        "risk_tier": str,
        "config": {locked rules — what UI displays in the "Locked rules" box},
        "yearly": [{year, return_pct, max_dd, wr, n_closed, n_open, end_equity}],
        "monthly": [{year, month, end_equity, return_pct}],
        "trades": [{entry_date, exit_date, symbol, ...full audit trade row}],
        "summary": {avg_yearly_return, median, best, worst, positive_years,
                    avg_max_dd, avg_wr, total_pnl},
        "open_positions": [{symbol, entry_date, entry_px, current_mtm_px, unrealized_pnl}],
        "reconciliation": [{year, start_capital, closed_net_pnl, open_mtm,
                            computed, reported, gap}],
        "last_updated": ISO timestamp (IST),
        "source_sim_script": "sim_p1_v3_fixed35k.py" (for traceability),
      }
    """
    if persona_slug not in PERSONA_CONFIGS:
        raise ValueError(f"Unknown persona: {persona_slug}. "
                          f"Valid: {list(PERSONA_CONFIGS.keys())}")

    if not force and _cache_valid(persona_slug):
        log.info("persona_simulator: cache hit for %s", persona_slug)
        return _CACHE[persona_slug]["result"]

    log.info("persona_simulator: running fresh sim for %s", persona_slug)
    t0 = time.time()

    cfg_dict = PERSONA_CONFIGS[persona_slug]
    run_cfg = cfg_dict["run_cfg"]
    sim_start = cfg_dict["sim_start"]
    sim_end = cfg_dict["sim_end"]
    cash_start = cfg_dict["cash_per_year"]

    # Load data — RND for patterns, PROD for OHLC + features
    rnd_db = _resolve_rnd_db_path()
    all_pats = load_full_patterns(rnd_db)
    log.info("  patterns loaded: %d", len(all_pats))

    X, sym, dt = load_panel(PROD_DB, sim_start, sim_end)
    log.info("  panel: %d rows", X.shape[0])

    bars = load_all_bars(PROD_DB, "2020-12-01", sim_end)  # pad for trail lookback
    sector_map = build_sector_map(PROD_DB)
    all_td = trading_days(PROD_DB, sim_start, sim_end)
    log.info("  bars: %d symbols, %d trading days", len(bars), len(all_td))

    # Pre-compute first-td-of-month set for P4
    first_td_set = first_td_of_month_set(all_td)
    if cfg_dict.get("_first_td_of_month_only"):
        run_cfg.extra_entry_set = first_td_set

    # Wire intraday filter for P2 (operator-spec'd open item, 2026-05-16):
    # Load intraday_dataset_v2 → build (symbol, entry_date) lookup → attach
    # closure that returns the entry multiplier (1 + first_15_ret/100) or None.
    if cfg_dict.get("_uses_intraday_filter"):
        try:
            intra_lookup = _load_intraday_lookup()
            run_cfg.intraday_filter = _make_p2_intraday_filter(intra_lookup)
            log.info("  P2 intraday filter wired (%d (symbol, date) rows)", len(intra_lookup))
        except FileNotFoundError as e:
            log.warning("P2 intraday filter NOT wired (%s) — results will mismatch the Excel parity test.", e)

    years_arr = np.array([int(d[:4]) for d in dt], dtype=np.int32)
    years_in_window = sorted({int(d[:4]) for d in all_td})

    # ── Year-by-year sim (yearly reset) ───────────────────────────────────
    from collections import defaultdict
    yearly_results = []
    all_trades = []
    monthly_equity_data = []
    # Captures the LATEST year's raw simulator output (timeline + raw trade
    # dicts). Used downstream by `materialize_persona_live()` to populate
    # the live-virtual-portfolio SQLite tables — single source of truth so
    # the grid card / equity curve / open positions don't drift from the
    # backtest. Reset on every year; final value = last year in window.
    current_year_data: Optional[Dict[str, Any]] = None
    for Y in years_in_window:
        elig = eligible_patterns_for_year(all_pats, Y)
        year_mask = years_arr == Y
        # 2026-05-21 — per-persona min_fires (Falcon Top 10 uses 10; P1-P5 use 2).
        sigs = compute_year_signals(
            X, sym, dt, year_mask, elig,
            min_fires=run_cfg.min_fires,
        )
        # Persona-configurable ranker. Default "score" (sum_lift, P1-P5).
        # "avg_lift" → rewrite each signal's score so simulate_year's sort uses
        # quality-per-fire instead of raw confluence sum. simulate_year itself
        # stays untouched — it always reads s["score"].
        if run_cfg.sort_key == "avg_lift":
            for s in sigs:
                s["score"] = s["avg_lift"]
        sigs_by_sd = defaultdict(list)
        for s in sigs:
            sigs_by_sd[s["signal_date"]].append(s)
        year_start = f"{Y}-01-01"
        year_end = sim_end if Y == years_in_window[-1] else f"{Y}-12-31"
        year_td = [d for d in all_td if year_start <= d <= year_end]
        if not year_td:
            continue

        r = simulate_year(dict(sigs_by_sd), bars, year_td, run_cfg, cash_start)

        # Build year summary
        end_eq = r["ending_equity"]
        year_ret_pct = (end_eq / cash_start - 1) * 100
        n_closed = len(r["closed_trades"])
        n_open = len(r["open_at_end_trades"])
        n_wins = sum(1 for t in r["closed_trades"] if t["net_pnl"] > 0)
        wr = n_wins / n_closed * 100 if n_closed else 0
        min_dd = min((t["drawdown_pct"] for t in r["timeline"]), default=0)
        closed_pnl_sum = sum(t["net_pnl"] for t in r["closed_trades"])
        open_mtm_sum = sum(t["net_pnl"] for t in r["open_at_end_trades"])
        yearly_results.append({
            "year": Y,
            "start_capital": cash_start,
            "end_equity": end_eq,
            "pnl": end_eq - cash_start,
            "return_pct": year_ret_pct,
            "max_dd_pct": min_dd,
            "win_rate_pct": wr,
            "n_closed": n_closed,
            "n_open_at_year_end": n_open,
            "closed_net_pnl_sum": closed_pnl_sum,
            "open_mtm_net_pnl_sum": open_mtm_sum,
        })

        # Build monthly snapshot
        import pandas as pd
        df_tl = pd.DataFrame(r["timeline"])
        df_tl["month"] = df_tl["date"].str[:7]
        prev_eq = cash_start
        winning_months = 0   # additive: count of this year's months with return_pct > 0
        for mo, grp in df_tl.groupby("month"):
            eq_series = grp["equity"].tolist()
            end_eq_m = eq_series[-1]
            month_ret = (end_eq_m / prev_eq - 1) * 100
            # max_dd_pct — worst peak-to-trough inside the month, computed from
            # the intra-month equity series (most-negative %). Includes the
            # carry-in level (prev_eq) as the opening peak so a month that only
            # falls is correctly negative. None if the series is empty.
            max_dd_pct = _intramonth_max_dd(eq_series, prev_eq)
            monthly_equity_data.append({
                "year": Y, "month": mo,
                "end_equity": end_eq_m,
                "return_pct": month_ret,
                "max_dd_pct": max_dd_pct,
            })
            if month_ret > 0:
                winning_months += 1
            prev_eq = end_eq_m

        # additive: attach winning_months to THIS year's already-appended row.
        yearly_results[-1]["winning_months"] = winning_months

        # Convert trade rows to API-clean format
        for t in r["closed_trades"]:
            all_trades.append(_trade_to_api_row(t, Y, sector_map))
        for t in r["open_at_end_trades"]:
            all_trades.append(_trade_to_api_row(t, Y, sector_map))

        # Capture this year's raw data — overwritten each iteration so the
        # final value (after the loop) is the latest year's state.
        current_year_data = {
            "year":               Y,
            "cash_start":         cash_start,
            "timeline":           r["timeline"],
            "closed_trades":      r["closed_trades"],
            "open_at_end_trades": r["open_at_end_trades"],
            "sector_map":         sector_map,
        }

    # ── Aggregate summary ─────────────────────────────────────────────────
    rets = [r["return_pct"] for r in yearly_results]
    dds = [r["max_dd_pct"] for r in yearly_results]
    wrs = [r["win_rate_pct"] for r in yearly_results]
    pnls = [r["pnl"] for r in yearly_results]
    summary = {
        "avg_yearly_return_pct": float(np.mean(rets)) if rets else 0,
        "median_yearly_return_pct": float(np.median(rets)) if rets else 0,
        "best_year_pct": max(rets) if rets else 0,
        "worst_year_pct": min(rets) if rets else 0,
        "positive_years": sum(1 for r in rets if r > 0),
        "total_years": len(rets),
        "avg_max_drawdown_pct": float(np.mean(dds)) if dds else 0,
        "worst_drawdown_pct": min(dds) if dds else 0,
        "avg_win_rate_pct": float(np.mean(wrs)) if wrs else 0,
        "total_pnl_rs": sum(pnls),
        "total_trades": sum(r["n_closed"] for r in yearly_results),
    }

    # ── Reconciliation table (for QA / display) ───────────────────────────
    reconciliation = []
    for r in yearly_results:
        computed = r["start_capital"] + r["closed_net_pnl_sum"] + r["open_mtm_net_pnl_sum"]
        gap = r["end_equity"] - computed
        reconciliation.append({
            "year": r["year"],
            "start_capital": r["start_capital"],
            "closed_net_pnl": r["closed_net_pnl_sum"],
            "open_mtm_gross": r["open_mtm_net_pnl_sum"],
            "computed_end_equity": computed,
            "reported_end_equity": r["end_equity"],
            "gap": gap,
        })

    # ── Locked rules box (display-only) ───────────────────────────────────
    locked_rules = {
        "entry_time": "9:15 IST (open)" if cfg_dict["run_cfg"].entry_filter == entry_filter_daily else "9:15 IST (varies by persona)",
        "trade_size_rs": run_cfg.fixed_per_trade,
        "top_n": run_cfg.top_n,
        "hold_days_max": run_cfg.hold_days,
        "stop_loss_pct": run_cfg.init_stop * 100,
        "target_pct": run_cfg.target * 100 if run_cfg.target else None,
        "trail_trigger_pct": run_cfg.trail_trigger * 100 if run_cfg.trail_trigger else None,
        "capital_model": "Cash only — no borrowed money or leverage. Integer shares only. Idle cash tracked.",
    }

    result = {
        "persona_id": persona_slug,
        "name": cfg_dict["name"],
        "tagline": cfg_dict["tagline"],
        "risk_tier": cfg_dict["risk_tier"],
        "warning": cfg_dict.get("warning"),
        "sim_start": sim_start,
        "sim_end": sim_end,
        "config": locked_rules,
        "yearly": yearly_results,
        "monthly": monthly_equity_data,
        "trades": all_trades,
        "summary": summary,
        "reconciliation": reconciliation,
        "open_positions": [],   # populated by live-data layer in future; empty for static backtest
        "last_updated": datetime.now(IST).isoformat(),
        "source_sim_script": cfg_dict["source_sim_script"],
        "_elapsed_seconds": round(time.time() - t0, 1),
        # Underscore-prefixed: NOT part of the public API contract. Internal
        # use only — consumed by materialize_persona_live() to populate the
        # live-virtual-portfolio tables. Routers may strip this before
        # serializing.
        "_current_year": current_year_data,
    }

    # Cache + return
    _CACHE[persona_slug] = {"_cached_at": time.time(), "result": result}
    log.info("persona_simulator: %s sim complete in %.0fs",
             persona_slug, result["_elapsed_seconds"])
    return result


def _intramonth_max_dd(eq_series: List[float], carry_in_eq: float) -> Optional[float]:
    """Max drawdown WITHIN a month, as the most-negative peak-to-trough percent.

    Walks the intra-month equity series tracking the running peak and the worst
    (equity / peak - 1). The carry-in equity (the month's opening level) seeds
    the first peak so a month that only declines is still correctly negative.
    Returns 0.0 when the month never dipped below its running peak, or None when
    the series is empty / unusable.
    """
    if not eq_series:
        return None
    peak = carry_in_eq if (carry_in_eq and carry_in_eq > 0) else eq_series[0]
    if not peak or peak <= 0:
        return None
    worst = 0.0
    for eq in eq_series:
        if eq > peak:
            peak = eq
        if peak > 0:
            dd = (eq / peak - 1.0) * 100.0
            if dd < worst:
                worst = dd
    return round(worst, 2)


def _trade_to_api_row(t: Dict, year: int, sector_map: Dict) -> Dict[str, Any]:
    """Convert raw simulator trade dict → clean API row."""
    import pandas as pd
    actual_deployed = t["actual_deployed"]
    gross_ret_pct = (t["gross_pnl"] / actual_deployed * 100) if actual_deployed else 0
    net_ret_pct = (t["net_pnl"] / actual_deployed * 100) if actual_deployed else 0
    cal_hold = (pd.to_datetime(t["exit_date"]) - pd.to_datetime(t["entry_date"])).days
    return {
        "year": year,
        "entry_date": t["entry_date"],
        "exit_date": t["exit_date"],
        "calendar_hold_days": cal_hold,
        "trading_hold_days": t.get("trading_hold_days", 0),
        "symbol": t["symbol"],
        "sector": sector_map.get(t["symbol"], "?"),
        "allocated_capital_rs": t["allocated"],
        "shares": t["shares"],
        "entry_price": round(t["entry_px"], 2),
        "actual_deployed_rs": round(t["actual_deployed"], 2),
        "unused_cash_rs": round(t["unused_cash"], 2),
        "exit_price": round(t["exit_px"], 2),
        "gross_pnl_rs": round(t["gross_pnl"], 2),
        "fees_rs": round(t["fees"], 2),
        "net_pnl_rs": round(t["net_pnl"], 2),
        "gross_return_pct": round(gross_ret_pct, 2),
        "net_return_pct": round(net_ret_pct, 2),
        "exit_reason": t["exit_reason"],
        "score": round(t["score"], 1),
    }


def list_personas() -> List[Dict[str, str]]:
    """Lightweight metadata for the persona-selector UI (no full sim needed)."""
    return [
        {
            "id": slug,
            "name": cfg["name"],
            "tagline": cfg["tagline"],
            "risk_tier": cfg["risk_tier"],
            "warning": cfg.get("warning"),
        }
        for slug, cfg in PERSONA_CONFIGS.items()
    ]


def invalidate_cache(slug: Optional[str] = None) -> None:
    """Clear cache for one persona (or all). Call after editing PERSONA_CONFIGS."""
    if slug is None:
        _CACHE.clear()
    elif slug in _CACHE:
        del _CACHE[slug]


# ════════════════════════════════════════════════════════════════════════
# LIVE-TABLE MATERIALIZER (2026-05-21)
# ════════════════════════════════════════════════════════════════════════
# Bridges the persona simulator (audit-correct source of truth) into the
# legacy live-virtual-portfolio SQLite tables (portfolio_positions +
# portfolio_equity_history) that the grid card / equity curve / open
# positions UIs read from.
#
# Why it exists:
#   The legacy `portfolio_engine.backfill()` dispatches on PortfolioParams
#   (entry_cadence enum, etc.) and uses the global `compute_top_n` with
#   default min_fires=2 + sum_lift scoring. That works for P1, P3, P5 but
#   silently produces the WRONG strategy for personas that depend on the
#   new PersonaRunConfig knobs:
#     • monthly-trader Alt G   → needs split_15_mode (two-batch monthly)
#     • falcon-top-10          → needs avg_lift sort + min_fires=10
#                                + group_by_signal_date
#
#   Rather than retrofit the legacy dispatcher with parallel logic, this
#   materializer takes the persona simulator's already-verified output
#   for the latest calendar year and rewrites the live tables from it.
#
# Idempotent: re-running on the same persona wipes + reseeds its rows.
# Other personas in the same tables are untouched.

# Map simulator's exit-reason strings → portfolio_positions enum values.
_EXIT_REASON_MAP = {
    "INIT_STOP":      "SL",
    "TRAIL_GIVEBACK": "TRAIL",
    "TARGET":         "TARGET",
    "TIME_STOP":      "TIME",
}


def materialize_persona_live(
    con: "sqlite3.Connection",
    slug: str,
) -> Dict[str, int]:
    """Rewrite portfolio_positions + portfolio_equity_history for ONE persona,
    using the persona simulator's latest-year output as the source of truth.

    Returns: {"positions_inserted": int, "equity_rows_inserted": int}
    Raises if the persona has no `_current_year` data (shouldn't happen for
    any sim window that includes >=1 year).
    """
    import sqlite3 as _sqlite3
    # 1. Pull persona-simulator output (uses 1-hr cache)
    result = simulate_persona(slug)
    cy = result.get("_current_year")
    if not cy:
        raise RuntimeError(
            f"persona '{slug}' has no _current_year data — simulator output may "
            "be stale (pre-2026-05-21 schema). Run with force=True to refresh."
        )

    # 2. Look up portfolio_id from the legacy definitions table
    row = con.execute(
        "SELECT id FROM portfolio_definitions WHERE slug = ?", (slug,)
    ).fetchone()
    if not row:
        raise RuntimeError(
            f"no portfolio_definitions row for slug='{slug}'. Run "
            "seed_portfolio_definitions() first."
        )
    pid = row["id"] if isinstance(row, _sqlite3.Row) else row[0]

    # 3. Wipe existing rows for this persona (idempotent re-runs)
    n_pos_deleted = con.execute(
        "DELETE FROM portfolio_positions WHERE portfolio_id = ?", (pid,)
    ).rowcount
    n_eq_deleted  = con.execute(
        "DELETE FROM portfolio_equity_history WHERE portfolio_id = ?", (pid,)
    ).rowcount
    con.execute(
        "DELETE FROM portfolio_event_log WHERE portfolio_id = ?", (pid,)
    )
    log.info("materialize %s: deleted %d positions + %d equity rows",
              slug, n_pos_deleted, n_eq_deleted)

    # 4. Look up run_cfg for SL/target level derivation
    cfg = PERSONA_CONFIGS[slug]
    run_cfg = cfg["run_cfg"]
    sector_map: Dict[str, str] = cy.get("sector_map", {})

    # 5. INSERT every trade (closed + open-at-year-end) into portfolio_positions.
    # `open_at_end_trades` carries a SYNTHETIC exit_date / exit_reason set by
    # `simulate_year` for MTM accounting — those positions are still OPEN for
    # live-table purposes. Detect via the sentinel exit_reason and clear the
    # exit fields so the UI's "open positions" query (exit_date IS NULL) hits.
    OPEN_AT_END_SENTINEL = "OPEN_AT_BACKTEST_END_MTM"
    n_pos = 0
    for t in cy["closed_trades"] + cy["open_at_end_trades"]:
        exit_reason_raw = t.get("exit_reason", "")
        is_open_at_end  = exit_reason_raw == OPEN_AT_END_SENTINEL
        is_closed       = (t.get("exit_date") is not None) and (not is_open_at_end)
        sl_level        = t["entry_px"] * (1 + run_cfg.init_stop)
        target_lvl      = (t["entry_px"] * (1 + run_cfg.target)
                            if run_cfg.target is not None else None)
        pnl_rs          = t.get("net_pnl") if is_closed else None
        deployed        = t.get("actual_deployed") or (t["shares"] * t["entry_px"])
        pnl_pct         = ((pnl_rs / deployed * 100) if (is_closed and deployed) else None)
        exit_reason     = (_EXIT_REASON_MAP.get(exit_reason_raw, "MANUAL")
                            if is_closed else None)
        con.execute("""
            INSERT INTO portfolio_positions (
                portfolio_id, signal_date, entry_date, symbol, sector,
                rank_on_entry, score_on_entry, story_on_entry,
                entry_price, qty, capital_committed,
                sl_level, target_level, trail_high_water, trail_active,
                exit_date, exit_price, exit_reason, pnl_rs, pnl_pct, holding_days
            ) VALUES (?, ?, ?, ?, ?,  ?, ?, ?,  ?, ?, ?,
                      ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?)
        """, (
            pid,
            t.get("signal_date") or t["entry_date"],
            t["entry_date"],
            t["symbol"],
            sector_map.get(t["symbol"]),
            0,                                           # rank_on_entry: not tracked by simulator
            float(t.get("score", 0)),
            None,                                        # story_on_entry: build on read
            float(t["entry_px"]),
            int(t["shares"]),
            float(deployed),
            float(sl_level),
            float(target_lvl) if target_lvl is not None else None,
            None,                                        # trail_high_water: end-state only
            0,                                           # trail_active: end-state only
            t.get("exit_date") if is_closed else None,
            (float(t.get("exit_px")) if is_closed and t.get("exit_px") is not None else None),
            exit_reason,
            (float(pnl_rs) if pnl_rs is not None else None),
            (float(pnl_pct) if pnl_pct is not None else None),
            int(t.get("trading_hold_days", 0)) if is_closed else None,
        ))
        n_pos += 1

    # 6. INSERT daily equity_history from the simulator's timeline.
    # Compute running peak + drawdown + daily P&L deltas (the simulator gives
    # drawdown_pct directly but recomputed for schema fields).
    n_eq = 0
    cash_start = float(cy["cash_start"])
    peak_eq    = cash_start
    prev_eq    = cash_start
    cum_ret    = 0.0
    for tl in cy["timeline"]:
        eq           = float(tl["equity"])
        peak_eq      = max(peak_eq, eq)
        daily_pnl_rs = eq - prev_eq
        daily_pnl_pct = (daily_pnl_rs / prev_eq * 100.0) if prev_eq else 0.0
        cum_ret      = (eq / cash_start - 1.0) * 100.0 if cash_start else 0.0
        max_dd_pct   = (eq - peak_eq) / peak_eq * 100.0 if peak_eq else 0.0
        con.execute("""
            INSERT INTO portfolio_equity_history (
                portfolio_id, trade_date, cash_balance, deployed_capital,
                mtm_unrealized, total_equity, daily_pnl_rs, daily_pnl_pct,
                cumulative_return_pct, max_drawdown_pct, peak_equity,
                n_open_positions, n_closed_today, n_opened_today
            ) VALUES (?, ?, ?, ?,  ?, ?, ?, ?,  ?, ?, ?,  ?, ?, ?)
        """, (
            pid,
            tl["date"],
            float(tl.get("cash", 0)),
            float(tl.get("deployed", 0)),
            float(tl.get("unrealized", 0)),
            eq,
            daily_pnl_rs,
            daily_pnl_pct,
            cum_ret,
            max_dd_pct,
            peak_eq,
            int(tl.get("n_open", 0)),
            0,                                           # n_closed_today: not tracked in timeline
            0,                                           # n_opened_today: not tracked in timeline
        ))
        prev_eq = eq
        n_eq += 1

    con.commit()
    log.info("materialize %s: inserted %d positions + %d equity rows", slug, n_pos, n_eq)
    return {"positions_inserted": n_pos, "equity_rows_inserted": n_eq}
