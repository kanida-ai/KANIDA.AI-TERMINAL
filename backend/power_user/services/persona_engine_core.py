"""DRAFT — Persona V3 audit-ready engine core.

Extracted from the 5 worktree simulator scripts (sim_p1_v3_fixed35k.py,
sim_p2_v3_fixed35k.py, sim_p3_v3_locked.py, sim_p4_v3_locked.py,
sim_p5_v3_locked.py) into a single reusable module. Every persona shares
this engine; persona-specific behaviour is parameterised via the config
dict in `persona_simulator.py`.

WHY THIS MODULE EXISTS:
  - Before: 5 separate sim scripts, each with ~80% duplicated code, each
    producing a static Excel file that production read manually.
  - After: one engine, 5 config dicts, single source of truth, accessible
    via API. Config changes = edit one line + restart backend.

AUDIT-READY GUARANTEES (enforced by every function here):
  1. Integer shares only (math.floor) — no fractional Indian-cash-equity trades
  2. No leverage — actual exposure (shares * entry_px) <= allocated capital
  3. Idle cash from share-rounding stays in cash pool, not redeployed
  4. Gap-down stops honoured (exit at min(stop_level, day_open))
  5. n_fires >= 2 production gate (must be confirmed by >=2 separate patterns)
  6. drawdown_bounce pattern family excluded (per V7.1 default)
  7. Walk-forward: pattern can only fire for rows where year > mined_year
  8. P&L reconciles year-by-year to Re 0 gap
"""
from __future__ import annotations

import json
import logging
import math
import os
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import numpy as np

from .feature_cols import FEATURE_COLS, in_drawdown_bounce

log = logging.getLogger("kanida.power_user.persona_engine_core")


# ════════════════════════════════════════════════════════════════════════
# CONSTANTS — match the desktop simulator scripts exactly
# ════════════════════════════════════════════════════════════════════════

SLIP_BPS = 5.0          # 5 bps slippage on entry, 5 bps on exit
COST_BPS = 30.0         # 30 bps round-trip brokerage on actual_deployed
SLIP = SLIP_BPS / 1e4   # = 0.0005
FEE = COST_BPS / 1e4    # = 0.0030

MIN_FIRES_DEFAULT = 2   # production gate: ≥2 confluent patterns to be a signal
RETENTION_YEARS = 4     # rolling pattern window (mining_window_years in prod)
MINING_START_YEAR = 2020  # V2 expanding-then-rolling: pattern pool starts here

FEATURE_IDX = {c: i for i, c in enumerate(FEATURE_COLS)}


# ════════════════════════════════════════════════════════════════════════
# DATA LOADERS — pure functions, take a sqlite connection
# ════════════════════════════════════════════════════════════════════════

# ── Sim pattern-set source (R&D-DB-split, Leg 2) — DEFAULT-OFF ────────────
# The sim needs the UNFILTERED promoted-pattern catalog (~1826 R&D rows). PROD's
# published tables are mining-window-filtered by publish_patterns.py (~834 rows),
# so the sim cannot repoint at PROD. Cloud-migration §4.2: the single replica
# won't carry the ~38 GB R&D DB, so this read must be able to use a small
# published artifact (scripts/publish_full_patterns.py → table
# `falcon_sim_patterns`).
#
#   env FALCON_SIM_PATTERNS_ARTIFACT = path to the artifact DB.
#
# UNSET (default) → reads R&D exactly as today (byte-identical). SET + usable →
# reads the artifact, which materialises the identical JOIN rows in the identical
# order (a `seq` column preserves R&D's natural row order, because the downstream
# float accumulation in compute_year_signals is order-sensitive). SET but
# missing/malformed → WARN and FALL BACK to R&D (safe: R&D is the source of
# truth; we never silently simulate on wrong data). The Python post-processing
# below is shared by both paths, so parity is exact by construction.
_SIM_PATTERNS_ARTIFACT_ENV = "FALCON_SIM_PATTERNS_ARTIFACT"
_SIM_PATTERNS_TABLE = "falcon_sim_patterns"


def _patterns_from_rows(
    rows: List[Tuple[Any, Any, Any]],
) -> List[Dict[str, Any]]:
    """Shared transform: (mined_year, rule_json, avg_oos_year_lift_pp) rows →
    the persona pattern dicts, applying the drawdown_bounce Python filter.

    Used identically for the R&D read and the artifact read so the two paths
    can only differ in the raw rows they feed in (and those are equal by
    construction of the publisher)."""
    out: List[Dict[str, Any]] = []
    for my, rj, lift in rows:
        rule = [(f, op, float(th)) for f, op, th in json.loads(rj)]
        if in_drawdown_bounce(rule):
            continue
        out.append({"mined_year": int(my), "rule": rule, "lift": float(lift)})
    return out


def _read_sim_patterns_artifact(
    path: str,
) -> Optional[List[Tuple[Any, Any, Any]]]:
    """Return the raw (mined_year, rule_json, avg_oos_year_lift_pp) rows from the
    published artifact, ordered by the stored `seq` (which preserves R&D's
    natural JOIN order). Returns None on missing/malformed artifact so the
    caller can fall back to R&D. Never raises."""
    try:
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        con = sqlite3.connect(
            f"file:{Path(path).as_posix()}?mode=ro", uri=True, timeout=30.0
        )
        try:
            names = {
                r[0] for r in con.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            if _SIM_PATTERNS_TABLE not in names:
                raise ValueError(f"artifact missing table {_SIM_PATTERNS_TABLE!r}")
            return con.execute(
                f"SELECT mined_year, rule_json, avg_oos_year_lift_pp "
                f"FROM {_SIM_PATTERNS_TABLE} ORDER BY seq"
            ).fetchall()
        finally:
            con.close()
    except Exception as e:  # noqa: BLE001 — any failure ⇒ safe R&D fallback
        log.warning(
            "%s set to %r but unusable (%r) — falling back to R&D DB for the sim "
            "pattern set (behaviour unchanged, cloud-migration goal NOT met until "
            "fixed)", _SIM_PATTERNS_ARTIFACT_ENV, path, e,
        )
        return None


def load_full_patterns(rnd_db_path: str) -> List[Dict[str, Any]]:
    """Load the unfiltered promoted-pattern catalog (~1,384 after the
    drawdown_bounce filter) for the persona / co-trade simulators.

    Prefers the published `falcon_sim_patterns` artifact when
    FALCON_SIM_PATTERNS_ARTIFACT is set + usable; otherwise reads the R&D DB
    exactly as before (default, byte-identical). Returns list of dicts with
    {mined_year, rule, lift}.
    """
    art_path = (os.environ.get(_SIM_PATTERNS_ARTIFACT_ENV) or "").strip()
    if art_path:
        rows = _read_sim_patterns_artifact(art_path)
        if rows is not None:
            return _patterns_from_rows(rows)
        # else: warning already logged; fall through to the R&D read below.

    con = sqlite3.connect(rnd_db_path, timeout=120.0)
    try:
        rows = con.execute("""
            SELECT c.mined_year, c.rule_json, p.avg_oos_year_lift_pp
              FROM falcon_promoted_patterns p
              JOIN falcon_pattern_candidates c ON p.pattern_id = c.pattern_id
             WHERE p.classification IN ('universal','regime_dependent')
        """).fetchall()
    finally:
        con.close()
    return _patterns_from_rows(rows)


def load_panel(prod_db_path: str, start: str, end: str):
    """Load falcon_features over [start, end]. Returns (X, symbols, dates)."""
    feat_select = ", ".join(FEATURE_COLS)
    con = sqlite3.connect(prod_db_path, timeout=120.0)
    try:
        rows = con.execute(f"""
            SELECT symbol, trade_date, {feat_select}
              FROM falcon_features WHERE trade_date BETWEEN ? AND ?
             ORDER BY trade_date, symbol
        """, (start, end)).fetchall()
    finally:
        con.close()
    n = len(rows)
    X = np.full((n, len(FEATURE_COLS)), np.nan, dtype=np.float64)
    sym = np.empty(n, dtype=object)
    dt = np.empty(n, dtype=object)
    for i, r in enumerate(rows):
        sym[i] = r[0]
        dt[i] = r[1]
        X[i] = [v if v is not None else np.nan for v in r[2:]]
    return X, sym, dt


def load_all_bars(prod_db_path: str, start: str, end: str) -> Dict[str, List[Dict]]:
    """Load OHLC bars indexed by symbol. Each value is a list of bar dicts
    sorted by date."""
    con = sqlite3.connect(prod_db_path, timeout=120.0)
    try:
        rows = con.execute("""
            SELECT symbol, trade_date, open, high, low, close
              FROM ohlc_daily WHERE trade_date BETWEEN ? AND ?
        """, (start, end)).fetchall()
    finally:
        con.close()
    bars = defaultdict(list)
    for r in rows:
        bars[r[0]].append({"date": r[1], "open": r[2], "high": r[3],
                            "low": r[4], "close": r[5]})
    return {s: sorted(bs, key=lambda b: b["date"]) for s, bs in bars.items()}


def trading_days(prod_db_path: str, start: str, end: str) -> List[str]:
    """Distinct trade_dates in OHLC table, sorted ascending."""
    con = sqlite3.connect(prod_db_path, timeout=120.0)
    try:
        rows = con.execute("""
            SELECT DISTINCT trade_date FROM ohlc_daily
             WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date
        """, (start, end)).fetchall()
    finally:
        con.close()
    return [r[0] for r in rows]


def build_sector_map(prod_db_path: str) -> Dict[str, str]:
    """Symbol → sector mapping from falcon_sectors."""
    con = sqlite3.connect(prod_db_path, timeout=30.0)
    try:
        rows = con.execute("SELECT symbol, sector FROM falcon_sectors").fetchall()
    finally:
        con.close()
    return {s: sec for s, sec in rows}


# ════════════════════════════════════════════════════════════════════════
# SIGNAL GENERATION — walk-forward eligible patterns + n_fires >= 2 gate
# ════════════════════════════════════════════════════════════════════════

def rule_mask(rule: List[Tuple[str, str, float]], X: np.ndarray) -> np.ndarray:
    """Vectorised rule evaluation. Returns boolean mask of rows where rule fires."""
    m = np.ones(X.shape[0], dtype=bool)
    for f, op, th in rule:
        idx = FEATURE_IDX.get(f)
        if idx is None:
            return np.zeros(X.shape[0], dtype=bool)
        col = X[:, idx]
        if op == "<=":
            m &= (col <= th) & ~np.isnan(col)
        else:
            m &= (col > th) & ~np.isnan(col)
    return m


def eligible_patterns_for_year(
    all_pats: List[Dict[str, Any]],
    Y: int,
    retention: int = RETENTION_YEARS,
    mining_start: int = MINING_START_YEAR,
) -> List[Dict[str, Any]]:
    """V2 expanding-then-rolling: eligible window = [max(mining_start, Y-retention), Y-1]."""
    lo = max(mining_start, Y - retention)
    hi = Y - 1
    return [p for p in all_pats if lo <= p["mined_year"] <= hi]


def compute_year_signals(
    X: np.ndarray,
    sym: np.ndarray,
    dt: np.ndarray,
    year_mask: np.ndarray,
    pats: List[Dict[str, Any]],
    min_fires: int = MIN_FIRES_DEFAULT,
) -> List[Dict[str, Any]]:
    """Walk-forward signal generation for a single year.

    Returns rows where n_fires >= min_fires (production gate).
    Each row: {symbol, signal_date, n_fires, score=sum(oos_lift_pp), avg_lift}.

    2026-05-21 — added `avg_lift = sum_lift / n_fires` so personas can sort by
    quality-per-fire (Falcon Top 10) instead of raw confluence sum. `score` is
    still sum_lift for backward compatibility with P1-P5; personas that want
    avg_lift-based ranking set `sort_key='avg_lift'` on their PersonaRunConfig
    and the simulator layer rewrites `score` to `avg_lift` before grouping.
    """
    Xy = X[year_mask]
    n = year_mask.sum()
    n_fires = np.zeros(n, dtype=np.int32)
    sum_lift = np.zeros(n, dtype=np.float64)
    for p in pats:
        m = rule_mask(p["rule"], Xy)
        n_fires += m.astype(np.int32)
        sum_lift += m.astype(np.float64) * p["lift"]
    sub_sym = sym[year_mask]
    sub_dt = dt[year_mask]
    out = []
    for i in range(n):
        if n_fires[i] >= min_fires:
            nf = int(n_fires[i])
            sl = float(sum_lift[i])
            out.append({
                "symbol": str(sub_sym[i]),
                "signal_date": str(sub_dt[i]),
                "n_fires": nf,
                "score": sl,                                # sum_lift (legacy ranker)
                "avg_lift": (sl / nf) if nf else 0.0,       # new: quality-per-fire
            })
    return out


# ════════════════════════════════════════════════════════════════════════
# ENTRY-DAY FILTERS — persona-specific gates
# ════════════════════════════════════════════════════════════════════════

def entry_filter_daily(d: str) -> bool:
    """Daily personas (P1, P2, P5): every trading day is an entry day."""
    return True


def entry_filter_tuesday(d: str) -> bool:
    """P3 Weekly: only Tuesday."""
    return date.fromisoformat(d).weekday() == 1


def first_td_of_month_set(td_list: List[str]) -> Set[str]:
    """P4 Monthly: precompute set of first-trading-day-of-month dates."""
    out = set()
    seen = set()
    for d in td_list:
        mo = d[:7]
        if mo not in seen:
            out.add(d)
            seen.add(mo)
    return out


# ════════════════════════════════════════════════════════════════════════
# CORE SIMULATOR — runs ONE year for ONE persona
# ════════════════════════════════════════════════════════════════════════

@dataclass
class PersonaRunConfig:
    """All persona-specific behaviour collapsed to a config object."""
    top_n: int                         # how many positions to enter per cycle
    top_n_candidates: int = 100        # candidate pool size (engine top-100)
    fixed_per_trade: float = 35_000.0  # rupees per trade (V3 Indian-cash-equity)
    hold_days: int = 7                 # max trading sessions to hold
    init_stop: float = -0.07           # initial stop loss (% as fraction)
    target: Optional[float] = None     # fixed target % (P3, P4, P5); None = use trail
    trail_trigger: Optional[float] = None    # arm trail at this gain (P1, P2)
    trail_lookback: int = 10           # 10-day Donchian for trail
    entry_filter: Callable[[str], bool] = entry_filter_daily
    # Intraday filter (P2 only): given a candidate dict + the entry date, returns
    # EITHER None (reject, candidate failed the 9:30 15-min check) OR a float
    # entry multiplier — typically (1 + first_15_ret/100) — so the entry price
    # becomes `open × multiplier`. Reference: sim_p2_v3_fixed35k.py.
    intraday_filter: Optional[Callable[[Dict[str, Any], str], Optional[float]]] = None
    extra_entry_set: Optional[Set[str]] = None  # P4 only: first-td-of-month set
    # When True (P1 + P2 references), per-day candidates are first grouped by
    # signal_date, each group truncated to `top_n` by score, then re-sorted as
    # a single pool before acceptance. When False (P3/P4/P5 references), the
    # full pool is sorted once by score and capped at `top_n_candidates`. The
    # difference only matters on days where the same `entry_date` accepts
    # candidates from multiple `signal_date`s — rare but causes a few-trade
    # drift if the wrong strategy is used. Matches sim_p1_v3_fixed35k.py.
    group_by_signal_date: bool = False
    # P4 Alt G — two-batch monthly cadence ("Capital never sits idle"). When
    # True the engine IGNORES cfg.entry_filter AND cfg.extra_entry_set; it
    # builds its own per-month schedule:
    #   • Batch 1: enters on TD 0 (1st trading day of the month) → exits on TD 14
    #   • Batch 2: enters on TD 15 (16th trading day of the month) → exits on
    #                the month's final TD
    # SL, target, and the intraday filter still apply normally on top of these
    # fixed exit dates (whichever comes first wins). Months with fewer than 16
    # TDs run batch 1 only; months with <15 TDs fall back to natural hold_days
    # for batch 1 (rare — partial months at sim_start / sim_end boundaries).
    # Reference: sim_p4_alts_fullyear.py.
    split_15_mode: bool = False
    # Falcon Top 10 (2026-05-21) — picks how candidates rank inside a day:
    #   • "score"     (default) → sum_lift, the legacy confluence ranker used by
    #                              P1-P5 (heavier pattern stack = higher).
    #   • "avg_lift"  → sum_lift / n_fires, the quality-per-fire ranker. Pairs
    #                   with min_fires=10 to surface high-precision setups
    #                   that don't necessarily have the most pattern firings.
    # Simulator wiring lives in persona_simulator.py: after `compute_year_signals`
    # builds the year's signal list, the persona layer overwrites each row's
    # `score` field with `avg_lift` so simulate_year's existing sort logic works
    # unchanged. simulate_year itself never reads sort_key directly.
    sort_key: str = "score"
    # Production gate for n_fires (confluent patterns required per signal). P1-P5
    # all use the engine default of 2. Falcon Top 10 raises this to 10 — the
    # critical knob that takes worst-year from -23% (min_fires=2) to +5.35%
    # (min_fires=10). Plumbed through to compute_year_signals on each year.
    min_fires: int = MIN_FIRES_DEFAULT


def simulate_year(
    sigs_by_signal_date: Dict[str, List[Dict]],
    bars: Dict[str, List[Dict]],
    td_list: List[str],
    cfg: PersonaRunConfig,
    cash_start: float = 5_00_000.0,
) -> Dict[str, Any]:
    """Run one persona for one calendar year. Returns dict with closed trades,
    open trades at end, skipped trades, timeline, equity, etc.

    THIS IS THE V3 AUDIT-READY ENGINE — identical to what the 5 sim scripts run.
    All Indian-cash-equity rules enforced (integer shares, no leverage, idle cash).
    """
    sym_to_dates_idx = {s: {b["date"]: i for i, b in enumerate(bs)}
                          for s, bs in bars.items()}

    # ── split_15_mode (P4 Alt G) — pre-compute the two-batch schedule ──
    # Outside split mode all four maps stay None and the original
    # entry-filter + hold_days logic runs unchanged.
    batch1_entries:  Optional[Set[str]]      = None
    batch2_entries:  Optional[Set[str]]      = None
    batch1_exit_map: Optional[Dict[str, str]] = None
    monthend_map:    Optional[Dict[str, str]] = None
    valid_entries:   Optional[Set[str]]      = None
    if cfg.split_15_mode:
        _by_month: Dict[str, List[str]] = defaultdict(list)
        for _d in td_list:
            _by_month[_d[:7]].append(_d)
        batch1_entries  = {tds[0]  for tds in _by_month.values()}
        batch2_entries  = {tds[15] for tds in _by_month.values() if len(tds) >= 16}
        # Batch-1 exit = TD 14 of the same month (15 trading days inclusive).
        batch1_exit_map = {tds[0]: tds[14] for tds in _by_month.values() if len(tds) >= 15}
        monthend_map    = {mo: tds[-1] for mo, tds in _by_month.items()}
        valid_entries   = batch1_entries | batch2_entries

    # Pre-compute entry candidates (signal_date -> entry_date map)
    by_entry: Dict[str, List[Dict]] = defaultdict(list)
    for sd, group in sigs_by_signal_date.items():
        for s in group:
            sym = s["symbol"]
            if sym not in bars:
                continue
            bs = bars[sym]
            sd_idx = next((i for i, b in enumerate(bs) if b["date"] > sd), None)
            if sd_idx is None:
                continue
            ed = bs[sd_idx]["date"]
            # Apply entry-day filter / split-batch schedule.
            if cfg.split_15_mode:
                # P4 Alt G: only batch1 (1st TD) or batch2 (16th TD) of month
                # are valid entry dates. entry_filter / extra_entry_set ignored.
                if valid_entries is None or ed not in valid_entries:
                    continue
            else:
                if not cfg.entry_filter(ed):
                    continue
                # Apply first-td-of-month set (P4 legacy mode)
                if cfg.extra_entry_set is not None and ed not in cfg.extra_entry_set:
                    continue
            by_entry[ed].append({
                "symbol": sym, "signal_date": sd, "entry_date": ed,
                "score": s["score"], "n_fires": s["n_fires"],
                "_bars_start_idx": sd_idx,
            })

    cash = cash_start
    open_pos: List[Dict] = []
    closed: List[Dict] = []
    skipped: List[Dict] = []
    timeline: List[Dict] = []
    peak_eq = cash_start
    max_concurrent = 0

    for d in td_list:
        # ── 1. Check exits on open positions ──────────────────────────────
        new_open = []
        for p in open_pos:
            sym = p["symbol"]
            bs = bars[sym]
            idx_map = sym_to_dates_idx[sym]
            if d not in idx_map:
                new_open.append(p)
                continue
            bar_idx = idx_map[d]
            day_bar = bs[bar_idx]
            cur_ret = day_bar["close"] / p["entry_px"] - 1
            if cur_ret > p["high_water"]:
                p["high_water"] = cur_ret

            init_stop_lvl = p["entry_px"] * (1 + cfg.init_stop)
            stop_lvl = init_stop_lvl
            armed = False
            if cfg.trail_trigger is not None:
                armed = p["high_water"] >= cfg.trail_trigger
                if armed:
                    window = bs[max(0, bar_idx - cfg.trail_lookback + 1) : bar_idx + 1]
                    trail_stop = max(p["entry_px"], min(b["low"] for b in window))
                    stop_lvl = max(init_stop_lvl, trail_stop)
            target_lvl = (p["entry_px"] * (1 + cfg.target)
                           if cfg.target is not None else None)

            exit_reason = None
            exit_px_raw = None
            # Priority: SL first (conservative), then target, then time exit
            if day_bar["low"] <= stop_lvl:
                exit_reason = "TRAIL_GIVEBACK" if armed else "INIT_STOP"
                exit_px_raw = min(stop_lvl, day_bar["open"])
            elif target_lvl is not None and day_bar["high"] >= target_lvl:
                exit_reason = "TARGET"
                exit_px_raw = max(target_lvl, day_bar["open"])
            elif d == p["time_exit_date"]:
                exit_reason = "TIME_STOP"
                exit_px_raw = day_bar["close"]

            if exit_reason:
                exit_px = exit_px_raw * (1 - SLIP)
                gross_pnl = p["shares"] * (exit_px - p["entry_px"])
                fees = p["actual_deployed"] * FEE
                net_pnl = gross_pnl - fees
                exit_proceeds = p["shares"] * exit_px - fees
                cash += exit_proceeds
                trading_hold = bar_idx - p["entry_bar_idx"] + 1
                closed.append({**p, "exit_date": d, "exit_px": exit_px,
                                "gross_pnl": gross_pnl, "fees": fees, "net_pnl": net_pnl,
                                "exit_reason": exit_reason,
                                "trading_hold_days": trading_hold})
            else:
                new_open.append(p)
        open_pos = new_open

        # ── 2. New entries (only on filter-matching days) ─────────────────
        held_syms = {p["symbol"] for p in open_pos}
        raw_cands = by_entry.get(d, [])
        if cfg.group_by_signal_date:
            # P1/P2 reference behaviour: top-N per signal_date, union, resort.
            by_sd_today: Dict[str, List[Dict]] = defaultdict(list)
            for c in raw_cands:
                by_sd_today[c["signal_date"]].append(c)
            cands_today = []
            for sd, grp in by_sd_today.items():
                grp.sort(key=lambda x: x["score"], reverse=True)
                cands_today.extend(grp[:cfg.top_n])
            cands_today.sort(key=lambda c: c["score"], reverse=True)
        else:
            # P3/P4/P5 reference behaviour: flat sort, cap by top_n_candidates.
            cands_today = sorted(raw_cands,
                                  key=lambda c: c["score"], reverse=True
                                  )[:cfg.top_n_candidates]

        # P1/P2 references (group_by_signal_date=True) intentionally allow
        # MORE than top_n entries on a multi-signal_date day. Their cap is
        # implicit (cash exhaustion + per-sd grouping ceiling = top_n * n_sds).
        # P3/P4/P5 references use a strict top_n cap on accepted count.
        accepted_count_cap = None if cfg.group_by_signal_date else cfg.top_n
        accepted = []
        for c in cands_today:
            if accepted_count_cap is not None and len(accepted) >= accepted_count_cap:
                break
            if c["symbol"] in held_syms:
                continue
            # Intraday filter (P2 only) — returns None to reject or a float
            # entry multiplier (e.g. 1.005 for a +0.5% first-15-min return).
            # Daily personas leave this None → multiplier defaults to 1.0.
            entry_multiplier: float = 1.0
            if cfg.intraday_filter is not None:
                fil = cfg.intraday_filter(c, d)
                if fil is None:
                    continue
                entry_multiplier = float(fil)
            sym = c["symbol"]
            bs = bars[sym]
            bsi = c["_bars_start_idx"]
            if bsi >= len(bs):
                continue
            ep_raw = bs[bsi]["open"]
            if ep_raw <= 0:
                continue
            # Entry price = open × multiplier × (1 + slip). For P2 this is
            # 9:15 open × (1 + first_15_ret/100) × slip = the 9:30 close
            # implied by the intraday data.
            ep = ep_raw * entry_multiplier * (1 + SLIP)

            # *** V3 AUDIT-READY: INTEGER SHARES, NO LEVERAGE ***
            allocated = cfg.fixed_per_trade
            shares = math.floor(allocated / ep)
            if shares < 1:
                skipped.append({**c, "entry_px": ep, "allocated": allocated,
                                 "skip_reason": "qty<1 (stock price > allocated)"})
                continue
            actual_deployed = shares * ep
            unused_cash = allocated - actual_deployed
            if cash < actual_deployed:
                skipped.append({**c, "entry_px": ep, "allocated": allocated,
                                 "skip_reason": "insufficient cash"})
                continue
            cash -= actual_deployed
            if cfg.split_15_mode:
                # Batch 1 (entry = TD 0 of month) → exit on TD 14 of same month.
                # Batch 2 (entry = TD 15 of month) → exit on month's last TD.
                # Fallback to natural hold_days when the month is too short.
                fallback_date = bs[min(bsi + cfg.hold_days - 1, len(bs) - 1)]["date"]
                if batch1_entries is not None and d in batch1_entries:
                    time_exit_date = (batch1_exit_map or {}).get(d, fallback_date)
                else:  # batch 2
                    time_exit_date = (monthend_map or {}).get(d[:7], fallback_date)
            else:
                time_exit_idx = min(bsi + cfg.hold_days - 1, len(bs) - 1)
                time_exit_date = bs[time_exit_idx]["date"]
            accepted.append({**c, "entry_px": ep, "entry_bar_idx": bsi,
                              "shares": shares, "allocated": allocated,
                              "actual_deployed": actual_deployed,
                              "unused_cash": unused_cash, "high_water": 0.0,
                              "time_exit_date": time_exit_date})
            held_syms.add(c["symbol"])
        open_pos.extend(accepted)

        # ── 3. End-of-day equity + timeline ────────────────────────────────
        unrealized = 0.0
        for p in open_pos:
            sym = p["symbol"]
            bs = bars[sym]
            if d in sym_to_dates_idx[sym]:
                bar = bs[sym_to_dates_idx[sym][d]]
                mtm_px = bar["close"] * (1 - SLIP)
                unrealized += p["shares"] * (mtm_px - p["entry_px"])
        deployed = sum(p["actual_deployed"] for p in open_pos)
        equity = cash + deployed + unrealized
        peak_eq = max(peak_eq, equity)
        max_concurrent = max(max_concurrent, len(open_pos))
        dd_pct = (equity - peak_eq) / peak_eq * 100 if peak_eq > 0 else 0
        timeline.append({
            "date": d, "equity": equity, "cash": cash,
            "deployed": deployed, "unrealized": unrealized,
            "drawdown_pct": dd_pct, "n_open": len(open_pos),
        })

    # ── 4. MTM open positions at year-end (reconciliation requires GROSS) ─
    open_at_end = []
    if td_list:
        last_d = td_list[-1]
        for p in open_pos:
            sym = p["symbol"]
            bs = bars[sym]
            idx_map = sym_to_dates_idx[sym]
            if last_d in idx_map:
                bar_idx = idx_map[last_d]
            else:
                cand = [(i, b) for i, b in enumerate(bs) if b["date"] <= last_d]
                bar_idx, _ = cand[-1] if cand else (None, None)
            if bar_idx is None:
                continue
            bar = bs[bar_idx]
            mtm_px = bar["close"] * (1 - SLIP)
            gross_pnl = p["shares"] * (mtm_px - p["entry_px"])
            estimated_close_fees = p["actual_deployed"] * FEE
            # For reconciliation: net_pnl = gross_pnl (fees deferred — position not yet closed)
            net_pnl = gross_pnl
            trading_hold = bar_idx - p["entry_bar_idx"] + 1
            open_at_end.append({**p, "exit_date": last_d, "exit_px": mtm_px,
                "gross_pnl": gross_pnl, "fees": 0.0,
                "estimated_close_fees": estimated_close_fees,
                "net_pnl": net_pnl,
                "exit_reason": "OPEN_AT_BACKTEST_END_MTM",
                "trading_hold_days": trading_hold})

    return {
        "closed_trades": closed,
        "open_at_end_trades": open_at_end,
        "skipped_trades": skipped,
        "timeline": timeline,
        "ending_equity": timeline[-1]["equity"] if timeline else cash_start,
        "ending_cash": cash,
        "peak_eq": peak_eq,
        "max_concurrent": max_concurrent,
    }
