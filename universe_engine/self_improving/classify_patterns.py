#!/usr/bin/env python3
"""classify_patterns.py — Step 3 of the Self-Improving Engine.

**Pattern Suitability Classification.** Reads the resolved Falcon Top 10 trades
produced by Step 2 (``falcon_baseline_trades`` + ``falcon_pattern_contributions``
in the RND DB), classifies every pattern that contributed to those trades, writes
the classification into the ``falcon_pattern_taxonomy`` new columns (RND DB only),
and produces ``falcon_pattern_health_report.xlsx``.

NOTHING is executed here at build time (no Python in env). The script is written
to be validated by reading + run later with ``--dry-run`` first.

THE HFCL RULE (statistical discipline — enforced everywhere):
  Never conclude anything from a single example. A pattern only earns a *flag*
  (big_loser_risk, persona-suitability, a confident quality_flag, a non-trivial
  maturity) when it has **n >= 18 resolved occurrences** AND the expected-value
  check across ALL of them is satisfied. Below 18 resolved trades the pattern is
  marked INSUFFICIENT_DATA / pattern_maturity='insufficient_data' and every
  boolean flag is left 0/NULL. This mirrors Phase-2 Constitutional Rules #3 & #10.

WHAT "RESOLVED" MEANS HERE:
  ``falcon_pattern_contributions.realized_outcome`` == the contributing trade's
  ``net_ret_pct``. Step-2 sets ``net_ret_pct`` (and hence realized_outcome) to
  NULL for *open-at-end* trades and a real number for *closed* trades. So a
  "resolved occurrence" of a pattern = a contribution row whose realized_outcome
  IS NOT NULL (equivalently, its parent baseline trade is closed). All per-pattern
  aggregates below are computed over resolved occurrences ONLY.

PARTS:
  A. Sector attribution on falcon_baseline_trades — fill the Step-2 NULL columns
     ``sector_ret_same_period``, ``stock_vs_sector``, ``move_type``,
     ``sector_tailwind`` for each baseline trade, using an equal-weight chained
     sector index (ported verbatim from sim_sweep.build_sector_indices) over the
     trade's own entry->exit window. (UPDATE in place — additive, idempotent.)
  B. Per-pattern classification -> falcon_pattern_taxonomy (RND). Aggregate each
     pattern across its resolved occurrences (contributions JOIN baseline_trades)
     and fill the Step-1 taxonomy columns that are derivable now. Deferred columns
     are documented + left NULL (never fabricated).
  C. Excel: falcon_pattern_health_report.xlsx (one row per pattern). openpyxl if
     available, else a .csv fallback (+ a printed note).

REUSE (read-only; no shared/engine code modified — INV2):
  - persona_simulator.PROD_DB — the same PROD OHLC/falcon_sectors source Step 2 /
    the persona sim use (imported via the backend import root, like build_baseline).
  - sim_sweep.build_sector_indices — its exact equal-weight chained-index math is
    re-implemented here (sim_sweep lives in an engine worktree NOT on this branch;
    copying its ~20-line pure function is safer than importing across worktrees
    and keeps Step 3 self-contained. The math is identical — see _build_sector_indices).

WRITES: RND DB ONLY. Never PROD. Single transaction, rebuildable.

CLI:
  python classify_patterns.py --rnd-db <path> [--prod-db <path>] [--dry-run] [--out <dir>]
    --rnd-db   REQUIRED. Research DB holding baseline_trades / contributions /
               falcon_pattern_taxonomy. All writes go here.
    --prod-db  OHLC + falcon_sectors source for the sector index (default: the
               persona resolver's PROD_DB). READ-ONLY.
    --dry-run  Compute Part A + Part B + the report contents and print a full
               bucket summary; write NOTHING (no DB UPDATE, no Excel file).
    --out      Output dir for the Excel report (default:
               universe_engine/self_improving/out/).
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Windows consoles default to cp1252; force utf-8 so the summary print never
# crashes the run after all numbers are computed (same fix as build_baseline).
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# ── Import the PRODUCTION engine loaders (reuse only — never modified). ───────
# Mirror backend's import root exactly (see build_baseline.py rationale): put
# <repo>/backend on sys.path and import as power_user.services.*.
_HERE = Path(__file__).resolve().parent
_REPO_ROOT = _HERE.parent.parent              # <repo>/universe_engine/self_improving -> <repo>
_BACKEND_ROOT = _REPO_ROOT / "backend"
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

from power_user.services.persona_simulator import (  # noqa: E402
    PROD_DB,
)

PERSONA_TAG = "falcon_top10"          # the only persona with trades in Step 2

# ── Classification version. Bump on each schema/logic change of this step. ───
CLASSIFICATION_VERSION = 1

# ════════════════════════════════════════════════════════════════════════════
# THRESHOLDS — all documented in S3-build-log.md. Single source of truth here.
# ════════════════════════════════════════════════════════════════════════════

# HFCL minimum resolved occurrences before ANY flag may be set (spec Phase-2,
# Constitutional Rules #3 & #10).
HFCL_MIN_N = 18

# pattern_maturity cutoffs (n = resolved occurrences). Chosen to align the lowest
# bound with the HFCL gate (18) so "insufficient_data" == "below HFCL".
#   n < 18            -> insufficient_data   (HFCL: cannot conclude anything)
#   18 <= n < 50      -> emerging
#   50 <= n < 150     -> established
#   n >= 150          -> stable
MATURITY_EMERGING_MIN = 18
MATURITY_ESTABLISHED_MIN = 50
MATURITY_STABLE_MIN = 150

# quality_flag thresholds (spec "Phase 2 Update 1" sector-quality override):
#   STOCK_SPECIFIC_ALPHA: win_rate in SECTOR_HEADWIND >= 50% AND n_headwind >= 10
#   SECTOR_FOLLOWER:      win_rate tailwind >= 60% AND win_rate headwind < 40%
QUALITY_HEADWIND_WR_MIN = 50.0       # percent
QUALITY_HEADWIND_N_MIN = 10          # spec uses n_headwind >= 10 (a sub-gate < HFCL_MIN_N)
QUALITY_TAILWIND_WR_MIN = 60.0       # percent
QUALITY_TAILWIND_HEADWIND_WR_MAX = 40.0  # percent

# big_loser_risk (HFCL-disciplined): n >= 18 AND big-loser-heavy AND EV not positive.
BIG_LOSER_RATE_MIN = 0.30            # >= 30% of resolved occurrences are big losers
BIG_LOSER_EV_MAX = 0.0               # AND expected value (mean net_ret_pct) <= 0

# Journey peak buckets (Falcon Top 10, 7-day hold).
EARLY_PEAK_MAX_DAY = 2               # peak_day <= 2  -> early
LATE_PEAK_MIN_DAY = 5                # peak_day >= 5  -> late

# Sector index minimum peers (Part A): below this, the equal-weight index is too
# thin to be a trustworthy benchmark -> leave the trade's sector fields NULL.
SECTOR_MIN_PEERS = 3

# 60-day window for n_resolved_trades_60d (calendar days back from max signal_date).
RECENT_WINDOW_DAYS = 60


def _ist_today() -> str:
    """Today's date in IST (Asia/Kolkata) as YYYY-MM-DD. Per the always-use-IST
    rule: compute IST explicitly, never from the server clock / log timestamps."""
    ist = timezone(timedelta(hours=5, minutes=30))
    return datetime.now(ist).strftime("%Y-%m-%d")


# ════════════════════════════════════════════════════════════════════════════
# PART A — SECTOR ATTRIBUTION ON falcon_baseline_trades
# ════════════════════════════════════════════════════════════════════════════

def _build_sector_indices(
    prod_db: str, start: str, end: str
) -> Tuple[Dict[str, Dict[str, float]], Dict[str, str], Dict[str, int]]:
    """Equal-weight, daily-rebalanced chained index per sector — ported VERBATIM
    from sim_sweep.build_sector_indices (engine worktree). Base 100.0 on the
    first day a sector has any member return; on days with no member return the
    level carries forward.

    Returns:
      indices    : {sector: {trade_date: index_level}}
      sym_to_sec : {symbol: sector}
      peer_count : {sector: number of distinct member symbols seen in OHLC window}
                   (used to drop too-thin sectors — sim_sweep has no such guard;
                   we add it per the Step-3 instruction "if a trade's sector has
                   too few peers for an index, leave its sector fields NULL").
    """
    con = sqlite3.connect(prod_db, timeout=120.0)
    try:
        rows = con.execute("SELECT symbol, sector FROM falcon_sectors").fetchall()
        sym_to_sec = {s: sec for s, sec in rows if sec is not None}
        sectors = sorted(set(sym_to_sec.values()))
        ohlc = con.execute(
            "SELECT trade_date, symbol, close FROM ohlc_daily "
            "WHERE trade_date BETWEEN ? AND ?",
            (start, end),
        ).fetchall()
    finally:
        con.close()

    by_date: Dict[str, Dict[str, float]] = defaultdict(dict)
    for d, sym, cl in ohlc:
        if sym in sym_to_sec and cl is not None:
            by_date[d][sym] = cl

    sorted_dates = sorted(by_date.keys())
    indices: Dict[str, Dict[str, float]] = {sec: {} for sec in sectors}
    peer_count: Dict[str, int] = {}
    for sec in sectors:
        members = [s for s, x in sym_to_sec.items() if x == sec]
        seen_members = set()
        member_prev: Dict[str, float] = {}
        idx_value = 100.0
        for d in sorted_dates:
            day = by_date[d]
            rets = []
            for m in members:
                if m in day and m in member_prev and member_prev[m] > 0:
                    rets.append(day[m] / member_prev[m] - 1.0)
                if m in day:
                    member_prev[m] = day[m]
                    seen_members.add(m)
            if rets:
                idx_value *= (1.0 + sum(rets) / len(rets))
            indices[sec][d] = idx_value
        peer_count[sec] = len(seen_members)
    return indices, sym_to_sec, peer_count


def _classify_move_type(stock_ret: float, sector_ret: float) -> str:
    """Sector attribution bucket per the spec's move_type vocabulary
    (STOCK_LED / SECTOR_DRIVEN / SECTOR_HEADWIND / SECTOR_DRAGGED / STOCK_WEAKNESS).

    Reasoning (selection-vs-sector separation, spec Constitutional Rule #12):
      stock up,  sector up,  stock beat sector  -> STOCK_LED      (own alpha, tailwind)
      stock up,  sector up,  stock lagged sector-> SECTOR_DRIVEN  (rode the sector)
      stock up,  sector down (headwind)         -> SECTOR_HEADWIND(won vs a falling sector = stock-specific alpha)
      stock down, sector down                   -> SECTOR_DRAGGED (sector pulled it down)
      stock down, sector up                     -> STOCK_WEAKNESS (fell while sector rose)
    """
    stock_up = stock_ret > 0
    sector_up = sector_ret > 0
    if stock_up and sector_up:
        return "STOCK_LED" if stock_ret >= sector_ret else "SECTOR_DRIVEN"
    if stock_up and not sector_up:
        return "SECTOR_HEADWIND"
    if (not stock_up) and (not sector_up):
        return "SECTOR_DRAGGED"
    # stock down, sector up
    return "STOCK_WEAKNESS"


def _load_baseline_trades_for_attribution(rnd_db: str) -> List[Dict[str, Any]]:
    """Load the fields Part A needs from falcon_baseline_trades (this persona).
    entry_date / exit_date frame the sector-index window; net_ret_pct is the
    stock leg. Open-at-end trades (exit_date NULL OR net_ret_pct NULL) cannot be
    attributed over a closed window -> they are returned but get NULL sector
    fields (documented)."""
    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        rows = con.execute(
            "SELECT id, symbol, sector, entry_date, exit_date, net_ret_pct "
            "FROM falcon_baseline_trades WHERE persona = ?",
            (PERSONA_TAG,),
        ).fetchall()
    finally:
        con.close()
    out = []
    for tid, sym, sec, ed, xd, nret in rows:
        out.append({
            "id": int(tid), "symbol": sym, "sector": sec,
            "entry_date": ed, "exit_date": xd, "net_ret_pct": nret,
        })
    return out


def _sector_ret_over_window(
    indices: Dict[str, Dict[str, float]],
    sector: str,
    entry_date: str,
    exit_date: str,
) -> Optional[float]:
    """Sector index return (%) over [entry_date, exit_date], measured the SAME way
    the stock leg is (entry->exit), using the index level on each date. Index
    levels carry forward, so any calendar date inside the OHLC window resolves to
    a level. Returns None if either endpoint level is missing (date outside the
    index's covered range)."""
    sec_idx = indices.get(sector)
    if not sec_idx:
        return None
    lvl0 = sec_idx.get(entry_date)
    lvl1 = sec_idx.get(exit_date)
    if lvl0 is None or lvl1 is None or lvl0 <= 0:
        return None
    return (lvl1 / lvl0 - 1.0) * 100.0


def compute_part_a(
    rnd_db: str, prod_db: str
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Compute per-trade sector attribution. Returns (updates, stats) where
    `updates` is a list of dicts {id, sector_ret_same_period, stock_vs_sector,
    move_type, sector_tailwind} ready to UPDATE, and `stats` counts coverage."""
    trades = _load_baseline_trades_for_attribution(rnd_db)
    if not trades:
        return [], {"n_trades": 0, "n_attributed": 0, "n_null_open": 0,
                    "n_null_thin_sector": 0, "n_null_no_sector": 0,
                    "n_null_window": 0}

    # Index window spans all trade entry/exit dates.
    all_dates = []
    for t in trades:
        if t["entry_date"]:
            all_dates.append(t["entry_date"])
        if t["exit_date"]:
            all_dates.append(t["exit_date"])
    start = min(all_dates)
    end = max(all_dates)

    indices, sym_to_sec, peer_count = _build_sector_indices(prod_db, start, end)

    updates: List[Dict[str, Any]] = []
    stats = {"n_trades": len(trades), "n_attributed": 0, "n_null_open": 0,
             "n_null_thin_sector": 0, "n_null_no_sector": 0, "n_null_window": 0}

    for t in trades:
        sector = t["sector"]
        # Open-at-end (no closed window / no realized stock return) -> NULL.
        if not t["exit_date"] or t["net_ret_pct"] is None:
            stats["n_null_open"] += 1
            continue
        if not sector:
            stats["n_null_no_sector"] += 1
            continue
        if peer_count.get(sector, 0) < SECTOR_MIN_PEERS:
            stats["n_null_thin_sector"] += 1
            continue
        sec_ret = _sector_ret_over_window(
            indices, sector, t["entry_date"], t["exit_date"]
        )
        if sec_ret is None:
            stats["n_null_window"] += 1
            continue
        stock_ret = float(t["net_ret_pct"])
        updates.append({
            "id": t["id"],
            "sector_ret_same_period": sec_ret,
            "stock_vs_sector": stock_ret - sec_ret,
            "move_type": _classify_move_type(stock_ret, sec_ret),
            "sector_tailwind": 1 if sec_ret > 0 else 0,
        })
        stats["n_attributed"] += 1
    return updates, stats


# ════════════════════════════════════════════════════════════════════════════
# PART B — PER-PATTERN CLASSIFICATION -> falcon_pattern_taxonomy
# ════════════════════════════════════════════════════════════════════════════

def _taxonomy_columns(rnd_db: str) -> Optional[set]:
    """Return the set of column names on falcon_pattern_taxonomy in the RND DB,
    or None if the table doesn't exist (caller decides what to do)."""
    con = sqlite3.connect(rnd_db, timeout=30.0)
    try:
        try:
            rows = con.execute("PRAGMA table_info(falcon_pattern_taxonomy)").fetchall()
        except sqlite3.OperationalError:
            return None
    finally:
        con.close()
    if not rows:
        return None
    return {r[1] for r in rows}


def _load_pattern_occurrences(
    rnd_db: str, attribution: Dict[int, Dict[str, Any]]
) -> Dict[int, List[Dict[str, Any]]]:
    """For each pattern_id (persona=falcon_top10), gather its RESOLVED occurrences
    by joining contributions -> baseline_trades. Resolved = parent trade closed
    (net_ret_pct NOT NULL). Pulls everything the per-pattern aggregates need.

    IMPORTANT — Part A → Part B ordering: the sector split win-rates depend on
    Part A's move_type / sector_tailwind. In a fresh run those columns are still
    NULL in the DB (Part A is only WRITTEN at the end / not at all in --dry-run),
    so we do NOT read them from bt; instead we take them from the IN-MEMORY
    ``attribution`` map ({trade_id: {move_type, sector_tailwind}}) that compute_part_a
    just produced. This makes Part B correct on the first run and identical in
    dry-run vs apply. peak_day / peak_sustained / flags come straight from bt
    (Step-2-populated). We filter bt.net_ret_pct IS NOT NULL so only resolved
    occurrences count (the HFCL denominator)."""
    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        rows = con.execute(
            """
            SELECT c.pattern_id, c.pattern_mined_year, c.trade_id,
                   bt.net_ret_pct, bt.signal_date,
                   bt.peak_day_during_hold, bt.peak_sustained,
                   bt.big_winner_flag, bt.big_loser_flag
              FROM falcon_pattern_contributions c
              JOIN falcon_baseline_trades bt ON bt.id = c.trade_id
             WHERE c.persona = ?
               AND bt.net_ret_pct IS NOT NULL
            """,
            (PERSONA_TAG,),
        ).fetchall()
    finally:
        con.close()

    by_pat: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for (pid, my, tid, nret, sd, pday, psust, bw, bl) in rows:
        attr = attribution.get(int(tid), {})  # {} -> sector fields NULL (unattributed)
        by_pat[int(pid)].append({
            "mined_year": int(my) if my is not None else None,
            "net_ret_pct": float(nret),
            "signal_date": sd,
            "peak_day": (int(pday) if pday is not None else None),
            "peak_sustained": (int(psust) if psust is not None else None),
            "big_winner": (int(bw) if bw is not None else 0),
            "big_loser": (int(bl) if bl is not None else 0),
            "move_type": attr.get("move_type"),
            "sector_tailwind": attr.get("sector_tailwind"),  # None when unattributed
        })
    return by_pat


def _rate(numer: int, denom: int) -> Optional[float]:
    return (numer / denom) if denom else None


def _mean(vals: List[float]) -> Optional[float]:
    return (sum(vals) / len(vals)) if vals else None


def _maturity(n: int) -> str:
    if n < MATURITY_EMERGING_MIN:
        return "insufficient_data"
    if n < MATURITY_ESTABLISHED_MIN:
        return "emerging"
    if n < MATURITY_STABLE_MIN:
        return "established"
    return "stable"


def classify_one_pattern(
    pid: int, occ: List[Dict[str, Any]], max_signal_date: Optional[str]
) -> Dict[str, Any]:
    """Aggregate one pattern's resolved occurrences into taxonomy column values.
    HFCL is enforced inside: flags only set when n >= HFCL_MIN_N (+ EV check)."""
    n = len(occ)
    rets = [o["net_ret_pct"] for o in occ]
    n_wins = sum(1 for r in rets if r > 0)
    win_rate = _rate(n_wins, n)                       # fraction 0..1
    ev = _mean(rets)                                  # mean net_ret_pct (%)

    # 60-day recency (calendar) from the LATEST signal_date in resolved data.
    n_60d = 0
    if max_signal_date:
        try:
            cutoff = (datetime.fromisoformat(max_signal_date).date()
                      - timedelta(days=RECENT_WINDOW_DAYS))
            for o in occ:
                if o["signal_date"] and datetime.fromisoformat(o["signal_date"]).date() >= cutoff:
                    n_60d += 1
        except ValueError:
            n_60d = 0

    # Big winner / loser rates by persona (JSON). Only falcon_top10 has data.
    n_bw = sum(o["big_winner"] for o in occ)
    n_bl = sum(o["big_loser"] for o in occ)
    bw_rate = _rate(n_bw, n)
    bl_rate = _rate(n_bl, n)
    big_winner_rate_by_persona = json.dumps({PERSONA_TAG: bw_rate})
    big_loser_rate_by_persona = json.dumps({PERSONA_TAG: bl_rate})

    # big_loser_risk (HFCL): n>=18 AND big-loser-heavy AND EV not positive.
    if n >= HFCL_MIN_N and bl_rate is not None and ev is not None:
        big_loser_risk = 1 if (bl_rate >= BIG_LOSER_RATE_MIN and ev <= BIG_LOSER_EV_MAX) else 0
    else:
        big_loser_risk = None   # below HFCL: cannot conclude -> NULL (not 0-as-fact)

    # Journey: peak day stats (only over occurrences that have a peak_day).
    peak_days = [o["peak_day"] for o in occ if o["peak_day"] is not None]
    typical_peak_day = _mean([float(d) for d in peak_days]) if peak_days else None
    early_peak_rate = (_rate(sum(1 for d in peak_days if d <= EARLY_PEAK_MAX_DAY), len(peak_days))
                       if peak_days else None)
    late_peak_rate = (_rate(sum(1 for d in peak_days if d >= LATE_PEAK_MIN_DAY), len(peak_days))
                      if peak_days else None)
    sustained = [o["peak_sustained"] for o in occ if o["peak_sustained"] is not None]
    peak_sustained_rate = (_rate(sum(sustained), len(sustained)) if sustained else None)

    # Sector-split win rates (from Part A's per-trade sector_tailwind).
    #   tailwind = sector index rose over the hold (sector_tailwind == 1)
    #   headwind = sector index fell over the hold (sector_tailwind == 0)
    #   neutral  = NO attribution available (sector_tailwind is None: open trade,
    #              no sector, thin-sector index, or date outside index window).
    #              There is no "flat sector" bucket — the index return is rarely
    #              exactly 0 — so 'neutral' here honestly means 'unattributed',
    #              documented in the build log. (>=0 would fold flat into tailwind;
    #              we keep the binary up/down split the spec's quality test needs.)
    # The spec's "sector-headwind win-rate" for STOCK_SPECIFIC_ALPHA is exactly
    # wr over the headwind subset = win rate when the pattern fired into a falling
    # sector (a stock that wins there is showing stock-specific alpha).
    tail = [o for o in occ if o["sector_tailwind"] == 1]
    head = [o for o in occ if o["sector_tailwind"] == 0]
    neut = [o for o in occ if o["sector_tailwind"] is None]   # unattributed
    wr_tail = _pct_win(tail)
    wr_head = _pct_win(head)
    wr_neut = _pct_win(neut)
    n_headwind = len(head)               # spec gate: n_headwind >= 10
    wr_headwind_pct = wr_head            # already a percent or None

    # quality_flag (spec Phase-2 Update-1 thresholds), HFCL-gated.
    quality_flag = _quality_flag(n, wr_headwind_pct, n_headwind, wr_tail)

    maturity = _maturity(n)

    # Persona suitability — only assess where >= HFCL_MIN_N resolved trades exist
    # for THAT persona. Only falcon_top10 has trades -> swing_suitable reflects
    # falcon_top10 (a 7-day EOD swing; the Falcon Top-10 IS the swing persona's
    # data here). All OTHER personas have NO trades -> NULL (never fabricated).
    swing_suitable = None
    if n >= HFCL_MIN_N and ev is not None:
        swing_suitable = 1 if ev > 0 else 0
    # No data for these personas -> NULL (documented rationale; not 0-as-fact).
    positional_suitable = None
    longterm_suitable = None
    btst_suitable = None
    intraday_suitable = None
    short_suitable = None
    index_suitable = None
    fo_only = None

    return {
        "pattern_id": pid,
        "n_resolved_trades_total": n,
        "n_resolved_trades_60d": n_60d,
        "_win_rate": (win_rate * 100.0) if win_rate is not None else None,  # report uses %
        "_ev": ev,
        "typical_peak_day": typical_peak_day,
        "peak_sustained_rate": peak_sustained_rate,
        "early_peak_rate": early_peak_rate,
        "late_peak_rate": late_peak_rate,
        "win_rate_sector_tailwind": wr_tail,
        "win_rate_sector_headwind": wr_head,
        "win_rate_sector_neutral": wr_neut,
        "big_winner_rate_by_persona": big_winner_rate_by_persona,
        "big_loser_rate_by_persona": big_loser_rate_by_persona,
        "big_loser_risk": big_loser_risk,
        "quality_flag": quality_flag,
        "pattern_maturity": maturity,
        "swing_suitable": swing_suitable,
        "positional_suitable": positional_suitable,
        "longterm_suitable": longterm_suitable,
        "btst_suitable": btst_suitable,
        "intraday_suitable": intraday_suitable,
        "short_suitable": short_suitable,
        "index_suitable": index_suitable,
        "fo_only": fo_only,
        # report extras (not taxonomy cols)
        "_n_big_winners": n_bw,
        "_n_big_losers": n_bl,
        "_bw_rate": bw_rate,
        "_bl_rate": bl_rate,
        "_mined_years": sorted({o["mined_year"] for o in occ if o["mined_year"] is not None}),
    }


def _pct_win(occ: List[Dict[str, Any]]) -> Optional[float]:
    """Win rate (PERCENT) over a subset of occurrences, or None if empty."""
    if not occ:
        return None
    wins = sum(1 for o in occ if o["net_ret_pct"] > 0)
    return wins / len(occ) * 100.0


def _quality_flag(
    n: int,
    wr_headwind_pct: Optional[float],
    n_headwind: int,
    wr_tail_pct: Optional[float],
) -> str:
    """quality_flag per spec Phase-2 Update 1, HFCL-gated. Returns one of:
    'INSUFFICIENT_DATA' / 'STOCK_SPECIFIC_ALPHA' / 'SECTOR_FOLLOWER' / '' (->NULL).

    - INSUFFICIENT_DATA    : n < HFCL_MIN_N (18) — cannot conclude anything.
    - STOCK_SPECIFIC_ALPHA : sector-headwind win-rate >= 50% AND n_headwind >= 10
                             (the pattern wins even when its sector is falling).
    - SECTOR_FOLLOWER      : tailwind win-rate >= 60% AND headwind win-rate < 40%
                             (rides the sector; weak when the sector turns).
    - '' (writer maps to NULL): n >= 18 but neither sector signature is earned.
      The spec lists REGIME_SPECIFIC as the next fallback, but market/sector
      regime tags do NOT exist on these trades (Step-2 left them NULL; see the
      build log "Deferred"). We never INVENT a regime, so REGIME_SPECIFIC is not
      emitted here. The honest value is NULL = 'classified, no special flag'.
    """
    if n < HFCL_MIN_N:
        return "INSUFFICIENT_DATA"
    if (wr_headwind_pct is not None and wr_headwind_pct >= QUALITY_HEADWIND_WR_MIN
            and n_headwind >= QUALITY_HEADWIND_N_MIN):
        return "STOCK_SPECIFIC_ALPHA"
    if (wr_tail_pct is not None and wr_tail_pct >= QUALITY_TAILWIND_WR_MIN
            and (wr_headwind_pct is None or wr_headwind_pct < QUALITY_TAILWIND_HEADWIND_WR_MAX)):
        return "SECTOR_FOLLOWER"
    # n>=18 but no clean sector signature, and no regime tags to test
    # REGIME_SPECIFIC -> honest NULL (classified, no special flag). Returned as a
    # sentinel the writer maps to NULL.
    return ""   # writer maps '' -> NULL


# Taxonomy columns this step OWNS (writes). Everything else on the table is left
# untouched (additive). Deferred columns are intentionally NOT in this list.
_TAXONOMY_WRITE_COLS = [
    "n_resolved_trades_total", "n_resolved_trades_60d",
    "typical_peak_day", "peak_sustained_rate", "early_peak_rate", "late_peak_rate",
    "win_rate_sector_tailwind", "win_rate_sector_headwind", "win_rate_sector_neutral",
    "big_winner_rate_by_persona", "big_loser_rate_by_persona", "big_loser_risk",
    "quality_flag", "pattern_maturity",
    "swing_suitable", "positional_suitable", "longterm_suitable", "btst_suitable",
    "intraday_suitable", "short_suitable", "index_suitable", "fo_only",
    "last_classification_date", "classification_version",
]


def compute_part_b(
    rnd_db: str, attribution: Dict[int, Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Classify every pattern present in resolved contributions. Returns
    (pattern_rows, bucket_stats). ``attribution`` is Part A's in-memory
    {trade_id: {move_type, sector_tailwind}} map (so the sector splits use the
    freshly-computed attribution, not stale DB columns)."""
    by_pat = _load_pattern_occurrences(rnd_db, attribution)
    if not by_pat:
        return [], {"n_patterns": 0}

    # Global max signal_date across all resolved occurrences (for the 60d window).
    max_sd = None
    for occ in by_pat.values():
        for o in occ:
            if o["signal_date"] and (max_sd is None or o["signal_date"] > max_sd):
                max_sd = o["signal_date"]

    today = _ist_today()
    rows: List[Dict[str, Any]] = []
    maturity_buckets: Dict[str, int] = defaultdict(int)
    quality_buckets: Dict[str, int] = defaultdict(int)
    n_big_loser_risk = 0
    n_swing_suitable = 0

    for pid, occ in sorted(by_pat.items()):
        rec = classify_one_pattern(pid, occ, max_sd)
        rec["last_classification_date"] = today
        rec["classification_version"] = CLASSIFICATION_VERSION
        rows.append(rec)
        maturity_buckets[rec["pattern_maturity"]] += 1
        q = rec["quality_flag"] if rec["quality_flag"] else "(none/null)"
        quality_buckets[q] += 1
        if rec["big_loser_risk"] == 1:
            n_big_loser_risk += 1
        if rec["swing_suitable"] == 1:
            n_swing_suitable += 1

    stats = {
        "n_patterns": len(rows),
        "maturity_buckets": dict(maturity_buckets),
        "quality_buckets": dict(quality_buckets),
        "n_big_loser_risk": n_big_loser_risk,
        "n_swing_suitable": n_swing_suitable,
        "max_signal_date": max_sd,
    }
    return rows, stats


# ════════════════════════════════════════════════════════════════════════════
# PART C — EXCEL (openpyxl, .csv fallback)
# ════════════════════════════════════════════════════════════════════════════

_REPORT_HEADERS = [
    "pattern_id", "mined_years", "quality_flag", "pattern_maturity",
    "n_resolved", "n_resolved_60d", "win_rate_pct", "expected_value_pct",
    "big_winner_rate", "big_loser_rate", "big_loser_risk",
    "typical_peak_day", "peak_sustained_rate", "early_peak_rate", "late_peak_rate",
    "win_rate_sector_tailwind", "win_rate_sector_headwind", "win_rate_sector_neutral",
    "swing_suitable",
]


def _report_row(rec: Dict[str, Any]) -> List[Any]:
    return [
        rec["pattern_id"],
        ",".join(str(y) for y in rec["_mined_years"]) if rec["_mined_years"] else "",
        rec["quality_flag"] or "",
        rec["pattern_maturity"],
        rec["n_resolved_trades_total"],
        rec["n_resolved_trades_60d"],
        _round(rec["_win_rate"]),
        _round(rec["_ev"]),
        _round(rec["_bw_rate"]),
        _round(rec["_bl_rate"]),
        rec["big_loser_risk"],
        _round(rec["typical_peak_day"]),
        _round(rec["peak_sustained_rate"]),
        _round(rec["early_peak_rate"]),
        _round(rec["late_peak_rate"]),
        _round(rec["win_rate_sector_tailwind"]),
        _round(rec["win_rate_sector_headwind"]),
        _round(rec["win_rate_sector_neutral"]),
        rec["swing_suitable"],
    ]


def _round(v: Optional[float], nd: int = 4) -> Any:
    return round(v, nd) if isinstance(v, (int, float)) else v


def write_report(rows: List[Dict[str, Any]], out_dir: Path) -> Tuple[Path, str]:
    """Write falcon_pattern_health_report.xlsx (or .csv fallback). Returns
    (path, mode) where mode is 'xlsx' or 'csv'."""
    out_dir.mkdir(parents=True, exist_ok=True)
    data_rows = [_report_row(r) for r in rows]
    try:
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "pattern_health"
        ws.append(_REPORT_HEADERS)
        for dr in data_rows:
            ws.append(dr)
        path = out_dir / "falcon_pattern_health_report.xlsx"
        wb.save(str(path))
        return path, "xlsx"
    except ImportError:
        import csv
        path = out_dir / "falcon_pattern_health_report.csv"
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(_REPORT_HEADERS)
            w.writerows(data_rows)
        return path, "csv"


# ════════════════════════════════════════════════════════════════════════════
# WRITE (single transaction, RND ONLY)
# ════════════════════════════════════════════════════════════════════════════

def _write(
    rnd_db: str,
    part_a_updates: List[Dict[str, Any]],
    pattern_rows: List[Dict[str, Any]],
    tax_cols: set,
) -> Tuple[int, int, int]:
    """Apply Part A (baseline UPDATE) + Part B (taxonomy UPDATE) in ONE
    transaction. Returns (n_baseline_updated, n_taxonomy_updated, n_taxonomy_missing).
    Idempotent: every run recomputes + overwrites these columns in place."""
    con = sqlite3.connect(rnd_db, timeout=120.0)
    n_base = n_tax = n_missing = 0
    try:
        con.execute("BEGIN")

        # ── Part A: UPDATE falcon_baseline_trades (additive columns) ──
        for u in part_a_updates:
            con.execute(
                "UPDATE falcon_baseline_trades SET "
                "  sector_ret_same_period = ?, stock_vs_sector = ?, "
                "  move_type = ?, sector_tailwind = ? "
                "WHERE id = ?",
                (u["sector_ret_same_period"], u["stock_vs_sector"],
                 u["move_type"], u["sector_tailwind"], u["id"]),
            )
            n_base += 1

        # ── Part B: UPDATE falcon_pattern_taxonomy by pattern_id ──
        # Only write columns that ACTUALLY exist on the table (guards a RND DB
        # where the Step-1 ALTERs weren't applied). '' quality_flag -> NULL.
        write_cols = [c for c in _TAXONOMY_WRITE_COLS if c in tax_cols]
        if "pattern_id" not in tax_cols or not write_cols:
            # No addressable key, or none of our target columns exist on the
            # table -> skip Part B entirely (Part A still commits). Documented.
            con.commit()
            return n_base, 0, len(pattern_rows)
        set_sql = ", ".join(f"{c} = ?" for c in write_cols)
        upd = f"UPDATE falcon_pattern_taxonomy SET {set_sql} WHERE pattern_id = ?"
        for rec in pattern_rows:
            vals = []
            for c in write_cols:
                v = rec.get(c)
                if c == "quality_flag" and v == "":
                    v = None
                vals.append(v)
            vals.append(rec["pattern_id"])
            cur = con.execute(upd, vals)
            if cur.rowcount and cur.rowcount > 0:
                n_tax += 1
            else:
                n_missing += 1   # pattern_id not present in taxonomy table

        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
    return n_base, n_tax, n_missing


# ════════════════════════════════════════════════════════════════════════════
# ORCHESTRATION
# ════════════════════════════════════════════════════════════════════════════

def run(rnd_db: str, prod_db: str, out_dir: Path, dry_run: bool) -> int:
    print(f"[classify_patterns] RND DB:  {rnd_db}")
    print(f"[classify_patterns] PROD DB: {prod_db}  (read-only: OHLC + falcon_sectors)")
    print(f"[classify_patterns] mode: {'DRY-RUN (no writes)' if dry_run else 'APPLY'}")
    print(f"[classify_patterns] classification_version={CLASSIFICATION_VERSION}  "
          f"IST date={_ist_today()}")

    # ── PART A ──
    part_a_updates, a_stats = compute_part_a(rnd_db, prod_db)
    print("\n[classify_patterns] ── PART A — sector attribution (baseline_trades) ──")
    print(f"  trades total:        {a_stats['n_trades']}")
    print(f"  attributed (set):    {a_stats['n_attributed']}")
    print(f"  NULL (open-at-end):  {a_stats['n_null_open']}")
    print(f"  NULL (no sector):    {a_stats['n_null_no_sector']}")
    print(f"  NULL (thin sector <{SECTOR_MIN_PEERS} peers): {a_stats['n_null_thin_sector']}")
    print(f"  NULL (date outside index window): {a_stats['n_null_window']}")

    # In-memory attribution map for Part B (avoids the stale-DB ordering trap:
    # Part A's columns are not yet written when Part B aggregates).
    attribution = {
        u["id"]: {"move_type": u["move_type"], "sector_tailwind": u["sector_tailwind"]}
        for u in part_a_updates
    }

    # ── PART B ──
    tax_cols = _taxonomy_columns(rnd_db)
    if tax_cols is None:
        print("\n[classify_patterns] WARNING: falcon_pattern_taxonomy not found in "
              "RND DB. Part B taxonomy UPDATE will be SKIPPED (Part A + report "
              "still run). Apply Step-1 schema first.")
        tax_cols = set()
    else:
        missing_write = [c for c in _TAXONOMY_WRITE_COLS if c not in tax_cols]
        if missing_write:
            print(f"\n[classify_patterns] NOTE: {len(missing_write)} target taxonomy "
                  f"columns absent (Step-1 ALTERs not fully applied?): {missing_write}")

    pattern_rows, b_stats = compute_part_b(rnd_db, attribution)
    print("\n[classify_patterns] ── PART B — per-pattern classification ──")
    print(f"  patterns classified: {b_stats['n_patterns']}")
    if b_stats["n_patterns"]:
        print(f"  max resolved signal_date: {b_stats['max_signal_date']}")
        print(f"  maturity buckets: {b_stats['maturity_buckets']}")
        print(f"  quality_flag buckets: {b_stats['quality_buckets']}")
        print(f"  big_loser_risk=1 patterns: {b_stats['n_big_loser_risk']}")
        print(f"  swing_suitable=1 patterns: {b_stats['n_swing_suitable']}")
    print(f"  HFCL gate: n>={HFCL_MIN_N} required for any flag; "
          f"maturity cutoffs {MATURITY_EMERGING_MIN}/{MATURITY_ESTABLISHED_MIN}/"
          f"{MATURITY_STABLE_MIN}")

    if dry_run:
        # Still compute the report contents so the audit agent can see them, but
        # write NOTHING (no DB, no file).
        print(f"\n[classify_patterns] DRY-RUN — would write {len(part_a_updates)} "
              f"baseline UPDATEs + {len(pattern_rows)} taxonomy UPDATEs + the "
              f"health report ({len(pattern_rows)} rows). Nothing written.")
        # Print first few report rows for eyeballing.
        print("\n[classify_patterns] report preview (first 8 patterns):")
        print("  " + " | ".join(_REPORT_HEADERS))
        for rec in pattern_rows[:8]:
            print("  " + " | ".join(str(x) for x in _report_row(rec)))
        return 0

    # ── WRITE (RND only) ──
    n_base, n_tax, n_missing = _write(rnd_db, part_a_updates, pattern_rows, tax_cols)
    path, mode = write_report(pattern_rows, out_dir)
    print(f"\n[classify_patterns] APPLY complete:")
    print(f"  Part A: {n_base} baseline_trades rows updated (sector attribution).")
    print(f"  Part B: {n_tax} taxonomy rows updated; "
          f"{n_missing} pattern_ids absent from taxonomy (no row to update).")
    print(f"  Part C: report written -> {path}  ({mode})")
    if n_missing:
        print(f"  NOTE: {n_missing} classified patterns had no falcon_pattern_taxonomy "
              f"row (pattern present in contributions but not taxonomy). Their "
              f"classification is in the report but not the taxonomy table.")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Step 3 — classify Falcon Top 10 patterns by suitability "
                    "(sector attribution + per-pattern taxonomy + health report)."
    )
    p.add_argument("--rnd-db", required=True,
                   help="REQUIRED. RND research DB (baseline_trades / contributions "
                        "/ falcon_pattern_taxonomy). All writes go here.")
    p.add_argument("--prod-db", default=None,
                   help="OHLC + falcon_sectors source for the sector index "
                        "(default: persona PROD_DB). READ-ONLY.")
    p.add_argument("--out", default=None,
                   help="Output dir for falcon_pattern_health_report.xlsx "
                        "(default: universe_engine/self_improving/out/).")
    p.add_argument("--dry-run", action="store_true",
                   help="Compute + print summary; write nothing (no DB, no file).")
    args = p.parse_args(argv)

    prod_db = args.prod_db or PROD_DB
    out_dir = Path(args.out) if args.out else (_HERE / "out")

    return run(rnd_db=args.rnd_db, prod_db=prod_db, out_dir=out_dir,
               dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
