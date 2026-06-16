#!/usr/bin/env python3
"""build_bigwinner_study.py — Step 5 of the Self-Improving Engine.

**Big Winner / Big Loser Study (per pattern, per persona).** Pure AGGREGATION over
already-resolved data — NO re-simulation. For each (pattern_id, persona) the script
rolls up every RESOLVED occurrence of the pattern (a contribution row whose parent
``falcon_baseline_trades`` row is closed) into the spec's
``falcon_big_winner_loser_study`` columns, applies the HFCL statistical discipline,
writes the study table (RND DB only), and emits ``falcon_big_winner_loser_study.xlsx``
including the spec-named HFCL / CARTRADE / IFCI deep-dive tabs.

NOTHING is executed here at build time (no Python in env). Written to be validated by
reading and run later with ``--dry-run`` first.

────────────────────────────────────────────────────────────────────────────
WHAT "BIG WINNER / BIG LOSER" MEANS (Table 8 — Falcon Top 10, 7-day hold):
  Big winner = peak_ret > 12% at ANY point D+1..D+7   (peak_ret_during_hold)
  Big loser  = trough_ret < −7% at ANY point          (trough_ret_during_hold)
               OR exit_reason = INIT_STOP
  These flags are persona-specific (Constitutional Rule #20). Falcon Top 10 is the
  ONLY persona with resolved trades in Step 2, so it is the only persona that gets
  rows. Other personas → no rows (documented). The flags are already computed and
  stored on each trade at Step-2 build time (build_baseline.classify_journey →
  big_winner_flag / big_loser_flag); we reuse them as the SOURCE OF TRUTH rather
  than re-deriving thresholds here, so this study can never drift from the per-trade
  classification. (We also recompute the descriptive winner/loser sub-stats from
  the journey columns of the SAME flagged occurrences.)

WHAT "RESOLVED OCCURRENCE" MEANS (identical to classify_patterns.py):
  A row of ``falcon_pattern_contributions`` (persona='falcon_top10') whose parent
  ``falcon_baseline_trades`` row is CLOSED — i.e. bt.net_ret_pct IS NOT NULL.
  Open-at-end trades have no realized outcome and are excluded from every
  aggregate (so n_total_occurrences is the HFCL denominator). The SAME pattern can
  contribute to many trades on many signal dates → many occurrences (this is what
  the HFCL rule counts: "Setup X appeared 47 times…").

────────────────────────────────────────────────────────────────────────────
PER (pattern_id, persona) STUDY COLUMNS + EXACT FORMULAS
  Let OCC = resolved occurrences of the pattern; W = OCC with big_winner_flag=1;
  L = OCC with big_loser_flag=1; N = OCC neither winner nor loser (neutral).

  n_total_occurrences          = len(OCC)
  n_big_winners                = len(W)
  n_big_losers                 = len(L)
  n_neutral                    = len(OCC) − len(W) − len(L)
                                 (an occurrence CAN be both a big winner AND a big
                                  loser — peaked >12% AND troughed <−7% on its path;
                                  such a row counts in BOTH W and L and is NOT
                                  neutral. So n_neutral is the count of occurrences
                                  with NEITHER flag, not len(OCC)−W−L when overlaps
                                  exist. Implemented as a direct NEITHER count;
                                  n_neutral = len(OCC) − len(W ∪ L). Documented.)
  big_winner_rate              = n_big_winners / n_total_occurrences
  big_loser_rate               = n_big_losers  / n_total_occurrences

  WINNERS (over W only):
  avg_peak_ret_when_winner     = mean(peak_ret_during_hold for w in W)
  median_peak_ret_when_winner  = median(peak_ret_during_hold for w in W)
  avg_peak_day_when_winner     = mean(peak_day_during_hold  for w in W, day not NULL)
  peak_sustained_rate          = mean(peak_sustained==1 for w in W, sustained not NULL)
  sector_tailwind_rate_winners = mean(sector_tailwind==1 for w in W, tailwind not NULL)
  stock_led_rate_winners       = mean(move_type=='STOCK_LED' for w in W, move_type not NULL)

  LOSERS (over L only):
  avg_trough_ret_when_loser    = mean(trough_ret_during_hold for l in L)
  avg_trough_day_when_loser    = mean(trough_day_during_hold for l in L, day not NULL)
  recovery_rate_after_trough   = fraction of L that RECOVERED from the trough by our
                                 exit. DEFINITION (documented in build log): an
                                 occurrence "recovered" if its realized exit return
                                 ended STRICTLY ABOVE its trough — net_ret_pct >
                                 trough_ret_during_hold. (Rationale: every closed
                                 trade exits at or after its trough, so net_ret_pct >=
                                 trough almost always; ">" measures any bounce off the
                                 low. We DO NOT use ">=0" because a loser that exits at
                                 −5% after troughing −9% genuinely recovered ground —
                                 the spec's two suggested definitions are both offered;
                                 we pick "ended > trough" and state it. NULL inputs
                                 excluded.)
  stop_hit_rate                = fraction of L with exit_reason=='INIT_STOP'
                                 (denominator = len(L); the −7% stop is the loser's
                                 realized stop-out, distinct from a path-only trough)
  sector_headwind_rate_losers  = mean(move_type=='SECTOR_HEADWIND' for l in L, not NULL)
  stock_weakness_rate_losers   = mean(move_type=='STOCK_WEAKNESS'  for l in L, not NULL)

  EXPECTED VALUE (over ALL OCC — the HFCL "expected value across all occurrences"):
  expected_value_at_hold_end   = mean(net_ret_pct for o in OCC)
                                 = realized EV at OUR exit (what the trade actually made)
  expected_value_at_peak       = mean(peak_ret_during_hold for o in OCC)
                                 = OPPORTUNITY EV (best the path offered intra-hold;
                                   an upper bound a perfect-exit would have captured).
                                 The gap (peak EV − hold-end EV) = give-back the exit
                                 rule left on the table; surfaced for exit-rule review.

  pattern_maturity (reuse classify_patterns cutoffs, n = n_total_occurrences):
    n < 18            -> insufficient_data   (== below HFCL gate)
    18 <= n < 50      -> emerging
    50 <= n < 150     -> established
    n >= 150          -> stable

  HFCL FLAGS (spec Statistical Discipline + Constitutional Rules #3, #10):
    big_winner_candidate = 1  IFF n_total_occurrences >= 18
                               AND expected_value_at_hold_end > 0
                               (positive EV across ALL occurrences — NOT just winners)
    big_loser_risk       = 1  IFF n_total_occurrences >= 18
                               AND big_loser_rate >= 0.30
                               AND expected_value_at_hold_end <= 0
    n < 18  -> BOTH flags NULL (cannot conclude anything — never 0-as-fact).
    When n >= 18 but the candidate / risk condition is not met -> that flag = 0
    (a real "has data, not flagged" outcome, distinct from NULL).

  week_ending = MAX(signal_date) across all resolved occurrences in the data
    (single backtest snapshot; the genuine weekly refresh is Step 6 / Update 5).
    One value for the whole run → the UNIQUE(pattern_id, persona, week_ending) key
    makes a re-run with the SAME data idempotent (DELETE-by-persona then INSERT).

────────────────────────────────────────────────────────────────────────────
DEEP DIVES (spec-named examples: HFCL, CARTRADE, IFCI) — the HFCL RULE made concrete
  The spec's whole point: "Setup X appeared 47 times, 12 big winners, EV +8.3%" — and
  NEVER "HFCL ran because of pattern X". So the deep-dive tabs are SYMBOL-level: for
  each named SYMBOL we list EVERY resolved occurrence (one row per signal day) from
  ``falcon_baseline_trades`` — signal_date, entry_price, net_ret_pct, peak_ret,
  trough_ret, big_winner/loser flags, move_type, exit_reason — PLUS an aggregate
  footer line: "appeared N times, X big winners (Y%), Z big losers, EV(hold-end) =
  +A%, EV(peak) = +B%". If a symbol has FEW or ZERO occurrences we say so honestly
  (that is itself the HFCL lesson — you cannot conclude from one example). These tabs
  read the managed-portfolio baseline trades; an OPTIONAL second block per symbol
  reads the per-signal-day study (falcon_signal_day_study) when that table exists,
  because the unconstrained per-day study has MORE occurrences per symbol (no
  skip_already_held / cash cap) — strictly additive context, clearly labelled.

CONSTRAINTS honored: additive, idempotent (DELETE this persona's study rows then
INSERT in ONE transaction), RND-DB-only writes, NO PROD / shared-engine-code mutation
(INV2). HFCL everywhere — no candidate/risk flag without n>=18 + EV check. Per-persona
thresholds — only Falcon Top 10 has data; others get no rows (noted in the report).

CLI:
  python build_bigwinner_study.py --rnd-db <path> [--dry-run] [--out <dir>]
    --rnd-db   REQUIRED. Research DB with falcon_baseline_trades /
               falcon_pattern_contributions / falcon_big_winner_loser_study. All
               writes go here. (Falls back to the persona resolver if omitted.)
    --dry-run  Compute everything + print a full summary; write NOTHING (no DB row,
               no Excel file).
    --out      Output dir for the Excel report (default:
               universe_engine/self_improving/out; the orchestrator passes out/v5).
"""
from __future__ import annotations

import argparse
import sqlite3
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Windows consoles default to cp1252; force utf-8 so the summary print never
# crashes the run after all numbers are computed (same fix as the sibling steps).
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# ── Import the PRODUCTION engine resolver (reuse only — never modified). ──────
# Mirror backend's import root exactly (see classify_patterns.py / build_baseline.py).
_HERE = Path(__file__).resolve().parent
_REPO_ROOT = _HERE.parent.parent              # <repo>/universe_engine/self_improving -> <repo>
_BACKEND_ROOT = _REPO_ROOT / "backend"
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

# _resolve_rnd_db_path lets --rnd-db be optional (same default the other steps use).
try:
    from power_user.services.persona_simulator import (  # noqa: E402
        _resolve_rnd_db_path,
    )
except Exception:  # pragma: no cover — import only needed when --rnd-db omitted
    _resolve_rnd_db_path = None  # type: ignore

PERSONA_TAG = "falcon_top10"          # the only persona with resolved trades (Step 2)

# Deep-dive symbols the spec names explicitly. Demonstrate the HFCL rule (never
# conclude from one example) on each — list every occurrence + the aggregate line.
DEEP_DIVE_SYMBOLS = ["HFCL", "CARTRADE", "IFCI"]

# ════════════════════════════════════════════════════════════════════════════
# THRESHOLDS — single source of truth; mirrored from classify_patterns.py so the
# study and the taxonomy classification can never diverge. All in S5-build-log.md.
# ════════════════════════════════════════════════════════════════════════════

# HFCL minimum resolved occurrences before ANY candidate/risk flag may be set
# (spec Statistical Discipline + Constitutional Rules #3 & #10).
HFCL_MIN_N = 18

# pattern_maturity cutoffs (n = resolved occurrences) — identical to classify_patterns.
MATURITY_EMERGING_MIN = 18
MATURITY_ESTABLISHED_MIN = 50
MATURITY_STABLE_MIN = 150

# big_winner_candidate: n>=18 AND positive EV across ALL occurrences.
BIG_WINNER_EV_MIN = 0.0               # expected_value_at_hold_end > 0

# big_loser_risk: n>=18 AND big-loser-heavy AND EV not positive.
BIG_LOSER_RATE_MIN = 0.30             # big_loser_rate >= 30%
BIG_LOSER_EV_MAX = 0.0                # AND expected_value_at_hold_end <= 0


def _ist_today() -> str:
    """Today's date in IST (Asia/Kolkata) as YYYY-MM-DD. Per the always-use-IST
    rule: compute IST explicitly, never from the server clock / log timestamps."""
    ist = timezone(timedelta(hours=5, minutes=30))
    return datetime.now(ist).strftime("%Y-%m-%d")


# ════════════════════════════════════════════════════════════════════════════
# SMALL STATS HELPERS (NULL-safe)
# ════════════════════════════════════════════════════════════════════════════

def _mean(vals: List[float]) -> Optional[float]:
    return (sum(vals) / len(vals)) if vals else None


def _median(vals: List[float]) -> Optional[float]:
    return statistics.median(vals) if vals else None


def _rate(numer: int, denom: int) -> Optional[float]:
    return (numer / denom) if denom else None


def _maturity(n: int) -> str:
    if n < MATURITY_EMERGING_MIN:
        return "insufficient_data"
    if n < MATURITY_ESTABLISHED_MIN:
        return "emerging"
    if n < MATURITY_STABLE_MIN:
        return "established"
    return "stable"


def _round(v: Optional[float], nd: int = 4) -> Any:
    return round(v, nd) if isinstance(v, (int, float)) and not isinstance(v, bool) else v


# ════════════════════════════════════════════════════════════════════════════
# LOAD — resolved occurrences per pattern (contributions JOIN baseline_trades)
# ════════════════════════════════════════════════════════════════════════════

def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    return con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _load_pattern_occurrences(rnd_db: str) -> Tuple[Dict[int, List[Dict[str, Any]]], Optional[str]]:
    """For each pattern_id (persona=falcon_top10) gather RESOLVED occurrences by
    joining contributions -> baseline_trades (bt.net_ret_pct NOT NULL).

    EVERYTHING the study needs is taken from the JOINed ``bt`` row, NOT from the
    contribution row. Rationale: build_baseline writes
    falcon_pattern_contributions.move_type = NULL (sector attribution is a later
    step that only fills falcon_baseline_trades.move_type via classify_patterns
    Part A). So move_type / sector_tailwind / peak / trough / peak_sustained /
    exit_reason / net_ret_pct all come from bt — the single, post-classify source
    of truth. big_winner_flag / big_loser_flag are identical on both tables; we
    read them from bt too for one consistent source.

    Returns (by_pattern, max_signal_date). max_signal_date = MAX(bt.signal_date)
    over ALL resolved occurrences → the study's week_ending (single snapshot)."""
    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        rows = con.execute(
            """
            SELECT c.pattern_id,
                   bt.signal_date,
                   bt.net_ret_pct,
                   bt.peak_ret_during_hold,
                   bt.peak_day_during_hold,
                   bt.trough_ret_during_hold,
                   bt.trough_day_during_hold,
                   bt.peak_sustained,
                   bt.big_winner_flag,
                   bt.big_loser_flag,
                   bt.sector_tailwind,
                   bt.move_type,
                   bt.exit_reason
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
    max_sd: Optional[str] = None
    for (pid, sd, nret, pret, pday, tret, tday, psust, bw, bl, stail, mtype, xreason) in rows:
        if sd and (max_sd is None or sd > max_sd):
            max_sd = sd
        by_pat[int(pid)].append({
            "signal_date": sd,
            "net_ret_pct": float(nret),                                   # always present (filter)
            "peak_ret": (float(pret) if pret is not None else None),
            "peak_day": (int(pday) if pday is not None else None),
            "trough_ret": (float(tret) if tret is not None else None),
            "trough_day": (int(tday) if tday is not None else None),
            "peak_sustained": (int(psust) if psust is not None else None),
            "big_winner": (int(bw) if bw is not None else 0),
            "big_loser": (int(bl) if bl is not None else 0),
            "sector_tailwind": (int(stail) if stail is not None else None),
            "move_type": mtype,                                           # may be NULL (unattributed)
            "exit_reason": xreason,
        })
    return by_pat, max_sd


# ════════════════════════════════════════════════════════════════════════════
# AGGREGATE — one pattern's resolved occurrences -> study row
# ════════════════════════════════════════════════════════════════════════════

def study_one_pattern(pid: int, occ: List[Dict[str, Any]], week_ending: str) -> Dict[str, Any]:
    """Aggregate one pattern's resolved occurrences into the
    falcon_big_winner_loser_study columns. HFCL enforced inside (flags only when
    n >= HFCL_MIN_N + EV check)."""
    n = len(occ)

    winners = [o for o in occ if o["big_winner"] == 1]
    losers = [o for o in occ if o["big_loser"] == 1]
    # neutral = NEITHER flag. An occurrence can be BOTH (spiked >12% then crashed
    # <−7% on the same path), so neutral is a direct "neither" count — not n−W−L.
    n_neither = sum(1 for o in occ if o["big_winner"] != 1 and o["big_loser"] != 1)

    n_bw = len(winners)
    n_bl = len(losers)
    n_neutral = n_neither

    big_winner_rate = _rate(n_bw, n)
    big_loser_rate = _rate(n_bl, n)

    # ── WINNERS sub-stats (over W only; each NULL-safe over its available subset) ──
    w_peaks = [o["peak_ret"] for o in winners if o["peak_ret"] is not None]
    avg_peak_ret_when_winner = _mean(w_peaks)
    median_peak_ret_when_winner = _median(w_peaks)
    w_peak_days = [float(o["peak_day"]) for o in winners if o["peak_day"] is not None]
    avg_peak_day_when_winner = _mean(w_peak_days)
    w_sust = [o["peak_sustained"] for o in winners if o["peak_sustained"] is not None]
    peak_sustained_rate = _rate(sum(w_sust), len(w_sust)) if w_sust else None
    w_tail = [o["sector_tailwind"] for o in winners if o["sector_tailwind"] is not None]
    sector_tailwind_rate_winners = _rate(sum(w_tail), len(w_tail)) if w_tail else None
    w_mt = [o["move_type"] for o in winners if o["move_type"] is not None]
    stock_led_rate_winners = (
        _rate(sum(1 for m in w_mt if m == "STOCK_LED"), len(w_mt)) if w_mt else None
    )

    # ── LOSERS sub-stats (over L only) ──
    l_troughs = [o["trough_ret"] for o in losers if o["trough_ret"] is not None]
    avg_trough_ret_when_loser = _mean(l_troughs)
    l_trough_days = [float(o["trough_day"]) for o in losers if o["trough_day"] is not None]
    avg_trough_day_when_loser = _mean(l_trough_days)
    # recovery_rate_after_trough: among losers, fraction whose realized exit ended
    # STRICTLY ABOVE the trough (net_ret_pct > trough_ret). NULL inputs excluded.
    l_recov_pairs = [
        (o["net_ret_pct"], o["trough_ret"]) for o in losers if o["trough_ret"] is not None
    ]
    recovery_rate_after_trough = (
        _rate(sum(1 for nr, tr in l_recov_pairs if nr > tr), len(l_recov_pairs))
        if l_recov_pairs else None
    )
    # stop_hit_rate: fraction of losers stopped out at the −7% init stop.
    stop_hit_rate = (
        _rate(sum(1 for o in losers if o["exit_reason"] == "INIT_STOP"), n_bl)
        if n_bl else None
    )
    l_mt = [o["move_type"] for o in losers if o["move_type"] is not None]
    sector_headwind_rate_losers = (
        _rate(sum(1 for m in l_mt if m == "SECTOR_HEADWIND"), len(l_mt)) if l_mt else None
    )
    stock_weakness_rate_losers = (
        _rate(sum(1 for m in l_mt if m == "STOCK_WEAKNESS"), len(l_mt)) if l_mt else None
    )

    # ── EXPECTED VALUE over ALL occurrences ──
    ev_hold_end = _mean([o["net_ret_pct"] for o in occ])
    peak_vals = [o["peak_ret"] for o in occ if o["peak_ret"] is not None]
    ev_peak = _mean(peak_vals)

    maturity = _maturity(n)

    # ── HFCL flags ──
    if n >= HFCL_MIN_N and ev_hold_end is not None:
        big_winner_candidate = 1 if ev_hold_end > BIG_WINNER_EV_MIN else 0
        if big_loser_rate is not None:
            big_loser_risk = 1 if (
                big_loser_rate >= BIG_LOSER_RATE_MIN and ev_hold_end <= BIG_LOSER_EV_MAX
            ) else 0
        else:
            big_loser_risk = 0
    else:
        big_winner_candidate = None    # below HFCL: cannot conclude -> NULL
        big_loser_risk = None

    return {
        "pattern_id": pid,
        "persona": PERSONA_TAG,
        "week_ending": week_ending,
        "n_total_occurrences": n,
        "n_big_winners": n_bw,
        "n_big_losers": n_bl,
        "n_neutral": n_neutral,
        "big_winner_rate": big_winner_rate,
        "avg_peak_ret_when_winner": avg_peak_ret_when_winner,
        "median_peak_ret_when_winner": median_peak_ret_when_winner,
        "avg_peak_day_when_winner": avg_peak_day_when_winner,
        "peak_sustained_rate": peak_sustained_rate,
        "sector_tailwind_rate_winners": sector_tailwind_rate_winners,
        "stock_led_rate_winners": stock_led_rate_winners,
        "big_loser_rate": big_loser_rate,
        "avg_trough_ret_when_loser": avg_trough_ret_when_loser,
        "avg_trough_day_when_loser": avg_trough_day_when_loser,
        "recovery_rate_after_trough": recovery_rate_after_trough,
        "stop_hit_rate": stop_hit_rate,
        "sector_headwind_rate_losers": sector_headwind_rate_losers,
        "stock_weakness_rate_losers": stock_weakness_rate_losers,
        "expected_value_at_hold_end": ev_hold_end,
        "expected_value_at_peak": ev_peak,
        "big_loser_risk": big_loser_risk,
        "big_winner_candidate": big_winner_candidate,
        "pattern_maturity": maturity,
    }


# Column order — EXACTLY the falcon_big_winner_loser_study schema (minus id, which
# AUTOINCREMENTs). Used for both the INSERT and the Excel "Falcon Top 10" tab.
_STUDY_COLS = [
    "pattern_id", "persona", "week_ending",
    "n_total_occurrences", "n_big_winners", "n_big_losers", "n_neutral",
    "big_winner_rate",
    "avg_peak_ret_when_winner", "median_peak_ret_when_winner",
    "avg_peak_day_when_winner", "peak_sustained_rate",
    "sector_tailwind_rate_winners", "stock_led_rate_winners",
    "big_loser_rate", "avg_trough_ret_when_loser", "avg_trough_day_when_loser",
    "recovery_rate_after_trough", "stop_hit_rate",
    "sector_headwind_rate_losers", "stock_weakness_rate_losers",
    "expected_value_at_hold_end", "expected_value_at_peak",
    "big_loser_risk", "big_winner_candidate", "pattern_maturity",
]


def compute_study(rnd_db: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Aggregate every pattern present in resolved contributions. Returns
    (study_rows, stats)."""
    by_pat, max_sd = _load_pattern_occurrences(rnd_db)
    if not by_pat:
        return [], {"n_patterns": 0, "week_ending": None}

    week_ending = max_sd
    rows: List[Dict[str, Any]] = []
    maturity_buckets: Dict[str, int] = defaultdict(int)
    n_candidate = n_risk = n_above_hfcl = 0
    for pid, occ in sorted(by_pat.items()):
        rec = study_one_pattern(pid, occ, week_ending)
        rows.append(rec)
        maturity_buckets[rec["pattern_maturity"]] += 1
        if rec["n_total_occurrences"] >= HFCL_MIN_N:
            n_above_hfcl += 1
        if rec["big_winner_candidate"] == 1:
            n_candidate += 1
        if rec["big_loser_risk"] == 1:
            n_risk += 1

    stats = {
        "n_patterns": len(rows),
        "week_ending": week_ending,
        "maturity_buckets": dict(maturity_buckets),
        "n_above_hfcl": n_above_hfcl,
        "n_big_winner_candidate": n_candidate,
        "n_big_loser_risk": n_risk,
    }
    return rows, stats


# ════════════════════════════════════════════════════════════════════════════
# DEEP DIVES — per-symbol occurrence ledger (the HFCL rule, made concrete)
# ════════════════════════════════════════════════════════════════════════════

def _load_symbol_occurrences_baseline(rnd_db: str, symbol: str) -> List[Dict[str, Any]]:
    """Every RESOLVED occurrence of a SYMBOL in the managed-portfolio baseline
    (falcon_baseline_trades, persona=falcon_top10, closed). One row per signal day."""
    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        rows = con.execute(
            """
            SELECT signal_date, entry_date, exit_date, entry_price, exit_price,
                   net_ret_pct, peak_ret_during_hold, peak_day_during_hold,
                   trough_ret_during_hold, trough_day_during_hold,
                   big_winner_flag, big_loser_flag, move_type, exit_reason
              FROM falcon_baseline_trades
             WHERE persona = ? AND symbol = ? AND net_ret_pct IS NOT NULL
             ORDER BY signal_date
            """,
            (PERSONA_TAG, symbol),
        ).fetchall()
    finally:
        con.close()
    out = []
    for r in rows:
        out.append({
            "signal_date": r[0], "entry_date": r[1], "exit_date": r[2],
            "entry_price": r[3], "exit_price": r[4], "net_ret_pct": r[5],
            "peak_ret": r[6], "peak_day": r[7], "trough_ret": r[8], "trough_day": r[9],
            "big_winner": r[10], "big_loser": r[11], "move_type": r[12], "exit_reason": r[13],
        })
    return out


def _load_symbol_occurrences_sds(rnd_db: str, symbol: str) -> Optional[List[Dict[str, Any]]]:
    """OPTIONAL additive context: every occurrence of a SYMBOL in the per-signal-day
    study (falcon_signal_day_study — unconstrained, no skip/cash). Returns None if
    that table does not exist on this DB (Step 2B not yet run)."""
    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        if not _table_exists(con, "falcon_signal_day_study"):
            return None
        rows = con.execute(
            """
            SELECT signal_date, entry_date, exit_date, entry_price, exit_price,
                   net_ret_pct, peak_ret_during_hold, peak_day_during_hold,
                   trough_ret_during_hold, trough_day_during_hold,
                   big_winner_flag, big_loser_flag, exit_reason
              FROM falcon_signal_day_study
             WHERE symbol = ? AND net_ret_pct IS NOT NULL
             ORDER BY signal_date
            """,
            (symbol,),
        ).fetchall()
    finally:
        con.close()
    out = []
    for r in rows:
        out.append({
            "signal_date": r[0], "entry_date": r[1], "exit_date": r[2],
            "entry_price": r[3], "exit_price": r[4], "net_ret_pct": r[5],
            "peak_ret": r[6], "peak_day": r[7], "trough_ret": r[8], "trough_day": r[9],
            "big_winner": r[10], "big_loser": r[11], "exit_reason": r[12],
        })
    return out


def _symbol_aggregate_line(occ: List[Dict[str, Any]]) -> str:
    """Build the HFCL-rule aggregate sentence for a symbol's occurrence list:
    'appeared N times, X big winners (Y%), Z big losers, EV(hold-end)=+A%,
    EV(peak)=+B%'. Honest about thin n (the whole point of the HFCL rule)."""
    n = len(occ)
    if n == 0:
        return ("appeared 0 times in resolved Falcon Top 10 trades — NO data. "
                "Per the HFCL rule, NOTHING can be concluded for this symbol.")
    n_bw = sum(1 for o in occ if o["big_winner"] == 1)
    n_bl = sum(1 for o in occ if o["big_loser"] == 1)
    ev_he = _mean([o["net_ret_pct"] for o in occ if o["net_ret_pct"] is not None])
    ev_pk = _mean([o["peak_ret"] for o in occ if o["peak_ret"] is not None])
    bw_pct = (n_bw / n * 100.0) if n else 0.0
    bl_pct = (n_bl / n * 100.0) if n else 0.0
    ev_he_str = f"{ev_he:+.2f}%" if ev_he is not None else "NA"
    line = (f"appeared {n} times, {n_bw} big winners ({bw_pct:.1f}%), "
            f"{n_bl} big losers ({bl_pct:.1f}%), EV(hold-end)={ev_he_str}")
    if ev_pk is not None:
        line += f", EV(peak)={ev_pk:+.2f}%"
    if n < HFCL_MIN_N:
        line += (f"  [HFCL: n<{HFCL_MIN_N} — INSUFFICIENT to flag a candidate; "
                 f"do NOT conclude from this alone]")
    else:
        line += f"  [HFCL: n>={HFCL_MIN_N} — eligible to flag IF EV>0]"
    return line


def build_deep_dives(rnd_db: str) -> Dict[str, Dict[str, Any]]:
    """For each spec-named symbol, collect its baseline occurrence ledger + the
    optional per-signal-day ledger + the HFCL aggregate line(s)."""
    out: Dict[str, Dict[str, Any]] = {}
    for sym in DEEP_DIVE_SYMBOLS:
        base_occ = _load_symbol_occurrences_baseline(rnd_db, sym)
        sds_occ = _load_symbol_occurrences_sds(rnd_db, sym)
        out[sym] = {
            "baseline": base_occ,
            "baseline_agg": _symbol_aggregate_line(base_occ),
            "sds": sds_occ,  # None if table absent
            "sds_agg": (_symbol_aggregate_line(sds_occ) if sds_occ is not None else None),
        }
    return out


# ════════════════════════════════════════════════════════════════════════════
# WRITE (single transaction, RND ONLY, idempotent)
# ════════════════════════════════════════════════════════════════════════════

def _study_value(rec: Dict[str, Any], col: str) -> Any:
    """Coerce one study value for SQLite. Booleans -> 0/1/NULL; everything else
    passes through (Python None -> SQL NULL)."""
    v = rec.get(col)
    return v


def _write(rnd_db: str, study_rows: List[Dict[str, Any]]) -> int:
    """Idempotent write of the study table for this persona, in ONE transaction:
    DELETE WHERE persona=falcon_top10, then INSERT all rows. RND-only. The
    UNIQUE(pattern_id, persona, week_ending) key + single week_ending makes
    re-running on the same data a clean rebuild. Returns rows inserted."""
    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        con.execute("BEGIN")
        if not _table_exists(con, "falcon_big_winner_loser_study"):
            con.rollback()
            raise RuntimeError(
                "falcon_big_winner_loser_study not found in RND DB. Apply Step-1 "
                "schema (schema_self_improving.sql) first.")
        con.execute("DELETE FROM falcon_big_winner_loser_study WHERE persona = ?",
                    (PERSONA_TAG,))
        placeholders = ", ".join("?" for _ in _STUDY_COLS)
        sql = (f"INSERT INTO falcon_big_winner_loser_study ({', '.join(_STUDY_COLS)}) "
               f"VALUES ({placeholders})")
        con.executemany(
            sql, [[_study_value(r, c) for c in _STUDY_COLS] for r in study_rows]
        )
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
    return len(study_rows)


# ════════════════════════════════════════════════════════════════════════════
# EXCEL / CSV OUTPUT
# ════════════════════════════════════════════════════════════════════════════

# Per-persona thresholds noted in the "Falcon Top 10" tab header (Table 8).
_FT10_THRESHOLD_NOTE = (
    "Falcon Top 10 (7-day hold) — Big winner: peak_ret > 12% at ANY point D+1..D+7. "
    "Big loser: trough_ret < −7% at ANY point, OR exit_reason = INIT_STOP. "
    "Measurement: daily high/low during hold. HFCL: big_winner_candidate / "
    "big_loser_risk only set when n_total_occurrences >= 18 AND the EV check passes; "
    "below 18 both are NULL. Thresholds are persona-specific (Constitutional Rule #20)."
)

_HFCL_SUMMARY_NOTE = (
    "HFCL RULE (Statistical Discipline). Never conclude a pattern causes big winners "
    "from one example. For EVERY pattern we compute, across ALL resolved occurrences: "
    "n_total_occurrences, % big winners, % big losers, % neutral, and expected value "
    "(mean net_ret_pct). A pattern is flagged big_winner_candidate=1 ONLY IF n>=18 AND "
    "expected_value_at_hold_end > 0 (positive EV across ALL occurrences, not just the "
    "winners). big_loser_risk=1 ONLY IF n>=18 AND big_loser_rate>=0.30 AND EV<=0. n<18 "
    "-> both NULL. The system answers 'Setup X appeared 47 times, 12 big winners, EV "
    "+8.3%' — never 'HFCL ran because of pattern X'. expected_value_at_hold_end = EV at "
    "OUR exit; expected_value_at_peak = the opportunity EV (best the path offered)."
)

_OTHER_PERSONAS_NOTE = (
    "Only the 'falcon_top10' persona has resolved trades in this build (Step 2 ran "
    "Falcon Top 10 first). All other personas (BTST, Intraday, Weekly Swing, "
    "Positional, Long-Term, Futures L/S, Index Options) have NO resolved trades yet, "
    "so they produce NO study rows and NO tabs. Their per-persona Big-Winner/Big-Loser "
    "thresholds (Table 8) differ and must NEVER be mixed with Falcon Top 10's "
    "(Constitutional Rule #20). Tabs appear here as personas are activated."
)

_FT10_HEADERS = list(_STUDY_COLS)

_DEEP_HEADERS = [
    "source", "signal_date", "entry_date", "exit_date", "entry_price", "exit_price",
    "net_ret_pct", "peak_ret_during_hold", "peak_day", "trough_ret_during_hold",
    "trough_day", "big_winner", "big_loser", "move_type", "exit_reason",
]


def _ft10_row(rec: Dict[str, Any]) -> List[Any]:
    return [_round(rec.get(c)) if c not in (
        "pattern_id", "persona", "week_ending", "n_total_occurrences",
        "n_big_winners", "n_big_losers", "n_neutral", "big_loser_risk",
        "big_winner_candidate", "pattern_maturity") else rec.get(c)
        for c in _STUDY_COLS]


def _deep_row(source: str, o: Dict[str, Any]) -> List[Any]:
    return [
        source, o.get("signal_date"), o.get("entry_date"), o.get("exit_date"),
        _round(o.get("entry_price")), _round(o.get("exit_price")),
        _round(o.get("net_ret_pct")), _round(o.get("peak_ret")), o.get("peak_day"),
        _round(o.get("trough_ret")), o.get("trough_day"),
        o.get("big_winner"), o.get("big_loser"),
        o.get("move_type", ""), o.get("exit_reason"),
    ]


def write_report(
    out_dir: Path,
    study_rows: List[Dict[str, Any]],
    deep: Dict[str, Dict[str, Any]],
) -> Tuple[Path, str]:
    """falcon_big_winner_loser_study.xlsx (openpyxl) or .csv fallback.

    Tabs:
      - "Falcon Top 10"  : one row per pattern (the study columns) + threshold +
                           HFCL header notes.
      - "<SYMBOL> deep dive" (HFCL / CARTRADE / IFCI): every occurrence + the
                           aggregate HFCL line(s).
      - "HFCL Summary"   : the n>=18 + EV rule + per-persona note.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    ft10_data = [_ft10_row(r) for r in study_rows]

    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font
    except Exception:
        return _write_csv_fallback(out_dir, study_rows, deep)

    wb = Workbook()

    # ── Tab 1: Falcon Top 10 ──
    ws = wb.active
    ws.title = "Falcon Top 10"
    ws.append([_FT10_THRESHOLD_NOTE])
    ws.append([_HFCL_SUMMARY_NOTE])
    ws.append([])
    ws.append(_FT10_HEADERS)
    hdr_row_idx = ws.max_row
    for c in ws[hdr_row_idx]:
        c.font = Font(bold=True)
    for d in ft10_data:
        ws.append(d)

    # ── Deep-dive tabs (HFCL / CARTRADE / IFCI) ──
    for sym in DEEP_DIVE_SYMBOLS:
        info = deep.get(sym, {})
        ws_s = wb.create_sheet(f"{sym} deep dive"[:31])  # Excel tab name <= 31 chars
        ws_s.append([f"{sym} — every resolved occurrence (HFCL rule: never conclude "
                     f"from one example)"])
        ws_s.append([f"BASELINE (managed-portfolio, skip_already_held, cash-capped): "
                     f"{info.get('baseline_agg', 'n/a')}"])
        sds_agg = info.get("sds_agg")
        if sds_agg is not None:
            ws_s.append([f"PER-SIGNAL-DAY study (unconstrained, additive context): "
                         f"{sds_agg}"])
        else:
            ws_s.append(["PER-SIGNAL-DAY study: table falcon_signal_day_study not "
                         "present on this DB — baseline ledger only."])
        ws_s.append([])
        ws_s.append(_DEEP_HEADERS)
        for c in ws_s[ws_s.max_row]:
            c.font = Font(bold=True)
        for o in info.get("baseline", []) or []:
            ws_s.append(_deep_row("baseline", o))
        for o in (info.get("sds") or []):
            ws_s.append(_deep_row("signal_day", o))

    # ── HFCL Summary tab ──
    ws_h = wb.create_sheet("HFCL Summary")
    ws_h.append(["HFCL Rule"]); ws_h["A1"].font = Font(bold=True)
    ws_h.append([_HFCL_SUMMARY_NOTE])
    ws_h.append([])
    ws_h.append(["Per-persona coverage"]); ws_h[ws_h.max_row][0].font = Font(bold=True)
    ws_h.append([_OTHER_PERSONAS_NOTE])

    path = out_dir / "falcon_big_winner_loser_study.xlsx"
    wb.save(str(path))
    return path, "xlsx"


def _write_csv_fallback(
    out_dir: Path, study_rows: List[Dict[str, Any]], deep: Dict[str, Dict[str, Any]]
) -> Tuple[Path, str]:
    import csv
    p1 = out_dir / "falcon_big_winner_loser_study.csv"
    with open(p1, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([_FT10_THRESHOLD_NOTE])
        w.writerow([_HFCL_SUMMARY_NOTE])
        w.writerow([])
        w.writerow(_FT10_HEADERS)
        for r in study_rows:
            w.writerow(_ft10_row(r))
    for sym in DEEP_DIVE_SYMBOLS:
        info = deep.get(sym, {})
        ps = out_dir / f"falcon_big_winner_loser_deepdive_{sym}.csv"
        with open(ps, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([f"{sym} deep dive"])
            w.writerow([f"BASELINE: {info.get('baseline_agg', 'n/a')}"])
            if info.get("sds_agg") is not None:
                w.writerow([f"PER-SIGNAL-DAY: {info['sds_agg']}"])
            w.writerow([])
            w.writerow(_DEEP_HEADERS)
            for o in info.get("baseline", []) or []:
                w.writerow(_deep_row("baseline", o))
            for o in (info.get("sds") or []):
                w.writerow(_deep_row("signal_day", o))
    print(f"[bigwinner_study] openpyxl missing — wrote CSV fallback: {p1.name} (+ "
          f"per-symbol deep-dive CSVs)")
    return p1, "csv"


# ════════════════════════════════════════════════════════════════════════════
# ORCHESTRATION
# ════════════════════════════════════════════════════════════════════════════

def run(rnd_db: str, out_dir: Path, dry_run: bool) -> int:
    print(f"[bigwinner_study] RND DB:  {rnd_db}")
    print(f"[bigwinner_study] persona: {PERSONA_TAG} (only persona with resolved trades)")
    print(f"[bigwinner_study] mode: {'DRY-RUN (no writes)' if dry_run else 'APPLY'}  "
          f"IST date={_ist_today()}")

    study_rows, stats = compute_study(rnd_db)
    print("\n[bigwinner_study] ── per-(pattern,persona) study ──")
    print(f"  patterns studied:      {stats['n_patterns']}")
    if stats["n_patterns"]:
        print(f"  week_ending (max sd):  {stats['week_ending']}")
        print(f"  maturity buckets:      {stats['maturity_buckets']}")
        print(f"  patterns n>={HFCL_MIN_N}:        {stats['n_above_hfcl']}")
        print(f"  big_winner_candidate=1: {stats['n_big_winner_candidate']}")
        print(f"  big_loser_risk=1:       {stats['n_big_loser_risk']}")
    print(f"  HFCL gate: n>={HFCL_MIN_N} + EV check for any flag; maturity cutoffs "
          f"{MATURITY_EMERGING_MIN}/{MATURITY_ESTABLISHED_MIN}/{MATURITY_STABLE_MIN}")

    # Deep dives (HFCL / CARTRADE / IFCI).
    deep = build_deep_dives(rnd_db)
    print("\n[bigwinner_study] ── deep dives (HFCL rule made concrete) ──")
    for sym in DEEP_DIVE_SYMBOLS:
        info = deep[sym]
        n_base = len(info["baseline"])
        n_sds = (len(info["sds"]) if info["sds"] is not None else None)
        print(f"  {sym}: baseline {info['baseline_agg']}")
        if n_sds is not None:
            print(f"       signal_day occurrences: {n_sds}")
        else:
            print(f"       signal_day study: table absent (baseline only)")

    if dry_run:
        print(f"\n[bigwinner_study] DRY-RUN — would DELETE persona='{PERSONA_TAG}' "
              f"study rows then INSERT {len(study_rows)} rows + write the report "
              f"({len(study_rows)} patterns + {len(DEEP_DIVE_SYMBOLS)} deep-dive tabs). "
              f"Nothing written.")
        print("\n[bigwinner_study] study preview (first 8 patterns):")
        print("  " + " | ".join(_FT10_HEADERS))
        for rec in study_rows[:8]:
            print("  " + " | ".join(str(x) for x in _ft10_row(rec)))
        return 0

    if not study_rows:
        print("\n[bigwinner_study] No resolved occurrences found — nothing to write. "
              "(Run Step 2 build_baseline first.) No DB change, no report.")
        return 0

    n_ins = _write(rnd_db, study_rows)
    path, mode = write_report(out_dir, study_rows, deep)
    print(f"\n[bigwinner_study] APPLY complete:")
    print(f"  study: {n_ins} rows inserted into falcon_big_winner_loser_study "
          f"(persona='{PERSONA_TAG}', week_ending={stats['week_ending']}).")
    print(f"  report: -> {path}  ({mode})")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Step 5 — Big Winner / Big Loser study per (pattern, persona). "
                    "Aggregation over resolved trades; HFCL-disciplined; writes "
                    "falcon_big_winner_loser_study + the Excel report."
    )
    p.add_argument("--rnd-db", default=None,
                   help="RND research DB (baseline_trades / contributions / "
                        "big_winner_loser_study). All writes go here. Defaults to "
                        "the persona resolver if omitted.")
    p.add_argument("--out", default=None,
                   help="Output dir for falcon_big_winner_loser_study.xlsx "
                        "(default: universe_engine/self_improving/out; the "
                        "orchestrator passes out/v5).")
    p.add_argument("--dry-run", action="store_true",
                   help="Compute + print summary; write nothing (no DB row, no file).")
    args = p.parse_args(argv)

    rnd_db = args.rnd_db
    if not rnd_db:
        if _resolve_rnd_db_path is None:
            print("ERROR: --rnd-db not given and the persona resolver could not be "
                  "imported. Pass --rnd-db explicitly.", file=sys.stderr)
            return 2
        rnd_db = _resolve_rnd_db_path()

    out_dir = Path(args.out) if args.out else (_HERE / "out")
    return run(rnd_db=rnd_db, out_dir=out_dir, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
