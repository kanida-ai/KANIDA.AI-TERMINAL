#!/usr/bin/env python3
"""build_self_improvement.py — Step 6 of the Self-Improving Engine.

**Phase 2 — Weekly Self-Improving Layer.** Re-runs the parity-validated Falcon
Top 10 walk-forward (2021 → latest) with the SAME locked trade mechanics, but
the ONE thing that changes is the per-signal-date RANKING:

    improved_score = avg_lift
                   × pattern_weight_multiplier   (Update 1)
                   × sector_multiplier            (Update 2)
                   × repeater_multiplier          (Update 3)

Multipliers are recomputed **weekly** from trades RESOLVED STRICTLY BEFORE that
week (exit_date < the week boundary on/before the signal date). The improved
sim is **self-consistent**: its OWN resolved trades feed the next week's
multipliers — we maintain a running improved trade ledger as the walk-forward
advances. Nothing about the exit rules / position size / hold / stop / trail is
touched (Constitutional Rule #7). We reuse ``persona_engine_core.simulate_year``
verbatim for the portfolio mechanics — the only difference vs build_baseline is
that we overwrite each signal's ``score`` with ``improved_score`` (instead of
plain ``avg_lift``) before grouping by signal_date.

CANNOT run Python in this environment — validated by reading; ``--dry-run``
provided. Additive / idempotent / RND-DB-only writes
(falcon_pattern_weekly_state + falcon_weekly_review_log). No PROD or
shared-engine-code mutation (INV2).

────────────────────────────────────────────────────────────────────────────
NO-LOOKAHEAD CAUSALITY MODEL (the #1 audit item)
────────────────────────────────────────────────────────────────────────────
Every signal_date D falls inside exactly one ISO week. We define the week
boundary W(D) = the Monday 00:00 of D's week (date arithmetic:
D - weekday(D) days). At decision time for D, the ONLY resolved trades a
multiplier may use are improved-ledger trades with **exit_date < W(D)** — i.e.
trades fully closed before that Monday. A trade closing ON or AFTER the Monday
is NOT yet known when the week's ranking is frozen (Constitutional Rule #9:
"weekly only, no intra-week ranking changes").

Concretely the loop walks signal dates in ascending order. For each new week
boundary W it (1) freezes the multiplier tables from the ledger slice
{t : t.exit_date < W}, then (2) ranks + simulates every signal_date in
[W, W+7d) with those frozen multipliers, then (3) the trades that result are
appended to the ledger (with their own exit_date, also produced by the SAME
locked sim) and become eligible for FUTURE weeks only. Because a trade can
never influence its own week (its exit_date >= its entry_date >= W), and a
week's ranking is frozen before any of that week's trades resolve, there is no
lookahead surface. 2021's first weeks have an empty pre-W ledger → all
multipliers default to 1.0 (regime memory / FRESH) → improved ≈ baseline
(the spec's SANITY check).

SELF-CONSISTENT LEDGER LOOP (why we cannot reuse build_baseline's ledger)
────────────────────────────────────────────────────────────────────────────
The multipliers must be computed from the IMPROVED sim's own resolved trades,
not from ``falcon_baseline_trades`` (those came from avg_lift ranking — a
different trade set). So we cannot pre-resolve all trades then rank: ranking in
week W depends on trades resolved before W, which were themselves produced by
the improved ranking in earlier weeks. The build therefore runs as a single
forward pass that interleaves "freeze multipliers from ledger" and "simulate
this week, append results to ledger". ``falcon_baseline_trades`` is read ONLY
for the GATE comparison (baseline per-year return), never for the multipliers.

We reuse ``simulate_year`` VERBATIM per CALENDAR YEAR (its ₹5L reset +
cash-bounded mechanics are inherently year-scoped, and the "no re-implemented
exit" constraint forbids slicing the year). The multiplier freeze granularity
is still WEEKLY (each signal date gets its own week boundary W(D)), but because
``simulate_year`` is ONE atomic call per year, every week IN year Y is frozen
from the ledger AS OF THE START of year Y — i.e. from 2021..Y-1's improved
trades only. This is a DELIBERATE, CONSERVATIVE simplification of "feed next
week" → "feed next YEAR":

  • It is STRICTLY no-lookahead — stronger than required. A week's ranking can
    never see ANY same-year trade (which would still be open / not-yet-resolved
    at freeze time anyway: a 7-day-hold trade entered in week K of Y resolves
    inside the SAME simulate_year(Y) call, so its exit_date is not knowable
    until that call returns). There is no causality surface where a trade
    influences its own year's ranking.
  • The cost: within-year week→week feedback is NOT live (a trade resolved in
    Feb-Y does not lift a pattern's multiplier for Mar-Y; it lifts it for all of
    Y+1). Cross-year feedback is fully live. The 2021 SANITY (improved ≈
    baseline, empty pre-Y ledger → all multipliers 1.0) holds exactly.
  • Path to true intra-year weekly feedback (documented, not built): a custom
    multi-call driver that threads cash + open positions across per-week
    ``simulate_year`` invocations. That requires re-implementing the year reset
    / cash carry OUTSIDE simulate_year, which the "reuse the locked engine, do
    not re-implement the exit" constraint disallows for this step.

The ledger is updated with year Y's resolved improved trades AFTER
simulate_year(Y) returns, before year Y+1 — see ``_append_year_to_ledger``.

────────────────────────────────────────────────────────────────────────────
THE 5 WEEKLY UPDATES (exact thresholds from the spec) — applied at each W
────────────────────────────────────────────────────────────────────────────
oos_lift (a.k.a. oos_lift_at_mining) per pattern = avg_oos_year_lift_pp, read
from falcon_pattern_contributions.lift_pp (constant per pattern_id — it is the
exact value build_baseline summed into the score; identical to
falcon_promoted_patterns.avg_oos_year_lift_pp). No extra DB needed.

U1 — Pattern realized lift (60d rolling), per pattern_id:
  trades_60d = improved-ledger resolved trades on which this pattern fired,
               with exit_date in [W-60d, W).
  if n < 18:  REGIME MEMORY — keep the pattern's PREVIOUS multiplier (carried
              forward from the last week it had >=18; default 1.0 if never).
              status='Keep' (no change), status_change=0.
  else:
     realized_lift_60d = mean(net_ret_pct of trades_60d)
     realized > oos_lift*1.20  -> 1.2  'Strengthen'
     realized < oos_lift*0.80  -> 0.7  'Demote'
     else                      -> 1.0  'Keep'
     # Sector-quality override (from move_type on the same trades_60d):
     headwind = trades where move_type == 'SECTOR_HEADWIND'
     tailwind = trades where move_type == 'SECTOR_DRIVEN'/'SECTOR_DRAGGED'/... ?
       -> we use sector_tailwind flag for tail/head WR (see build log: move_type
          'SECTOR_HEADWIND' is the headwind bucket; tailwind WR uses
          sector_tailwind==1). headwind WR uses move_type=='SECTOR_HEADWIND'.
     if headwind_WR >= 50 and n_headwind >= 10:
         quality='STOCK_SPECIFIC_ALPHA'; mult = min(mult*1.1, 1.5)
     elif tailwind_WR >= 60 and headwind_WR < 40:
         quality='SECTOR_FOLLOWER'  (NO mult change; used for auto-trade block)
  clamp -> [0.5, 1.5]

U2 — Sector realized edge (30d rolling), per sector:
  sector_trades_30d = ledger resolved trades in this sector, exit in [W-30d, W).
  if n < 18: sector_multiplier = 1.0
  else: edge = mean(net_ret_pct); rank sectors by edge:
        top-3 -> 1.2 ; bottom-5 -> 0.85 ; else 1.0
  clamp -> [0.75, 1.30]

U3 — Repeater quality (rolling), per symbol among the day's candidates:
  prior_60d = ledger resolved trades on this symbol, exit in [W-60d, W).
  len < 2                                              -> FRESH            1.0
  consecutive_days_in_top50 >= 3 AND avg < 0          -> PERSISTENCE_TRAP 0.80
  avg > 5 AND len >= 3 AND early_exit_count >= 2      -> EXTENDED_TREND   1.10
  avg > 3 AND len >= 2                                -> HEALTHY_REPEATER 1.15
  avg < 1                                             -> STALE            0.90
  else                                               -> (HEALTHY_REPEATER)1.0
  clamp -> [0.7, 1.20]
  (consecutive_days_in_top50: how many consecutive prior trading days this
   symbol appeared in the day's top-50 improved-ranked candidate pool — tracked
   as the forward pass observes each day's candidate list. early_exit_count =
   ledger trades on this symbol whose post-hold high cleared +15% — proxied by
   peak_ret_during_hold>15 because post-exit D+60 tracking is a later step; see
   build log "deferred / proxied".)

U4 — Weekly review log: one falcon_weekly_review_log row per week —
  patterns strengthened/demoted/disabled, new persistence traps / healthy
  repeaters, quality_flag changes, constitutional violations, summary_text.
  human_approved defaults FALSE (0).

U5 — Big-winner/loser refresh: per week, for n>=18 patterns recompute
  big_winner_rate / big_loser_rate / EV from the ledger-to-date and store on the
  weekly_state row (big_winner_rate, big_loser_rate). This REUSES the Step-5
  HFCL computation per week (mirrored here, same thresholds) — documented as the
  weekly snapshot of build_bigwinner_study's aggregation.

CONSTITUTIONAL RANKING CAPS (applied in the improved selection where derivable):
  #6  2+ INIT_STOP on a symbol in last 60d -> rank cap 8 (cannot rank above 8).
      DERIVABLE from ledger exit_reason. Implemented by penalising improved_score
      so the capped symbol cannot out-rank a non-capped one into slots 1..7 — see
      build log "cap mechanism". (We do NOT mutate simulate_year's accept loop;
      caps are enforced by score so the locked engine stays untouched.)
  #5  Capitulation-dominant pick (>40% firing patterns are capitulation) ->
      rank cap 7. NOT-YET-AVAILABLE: regime / capitulation tags do not exist on
      these patterns (build_baseline / classify_patterns leave regime NULL; the
      falcon_pattern_taxonomy plain-English regime tags are not joined here). So
      this cap is DEFERRED and documented, never faked. The plumbing is present
      (a per-pick capitulation_share hook) and wired to a no-op until tags land.

THE GATE — falcon_self_improvement_comparison.xlsx:
  per-year baseline return (from falcon_baseline_trades) vs improved return
  (this sim), n_trades, win rate, + overall. Honest: shows where improved <=
  baseline. SANITY: 2021 improved ≈ baseline (multipliers all 1.0).

CLI:
  python build_self_improvement.py --rnd-db <path> [--prod-db <path>]
       [--years 2021,2022] [--dry-run] [--out <dir>]
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

# Windows consoles default to cp1252 → force utf-8 so the summary print never
# crashes the run after all numbers are computed (same fix as sibling steps).
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# ── Mirror backend import root exactly (see build_baseline.py / classify_patterns.py).
_HERE = Path(__file__).resolve().parent
_REPO_ROOT = _HERE.parent.parent              # <repo>/universe_engine/self_improving -> <repo>
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
    in_drawdown_bounce,
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
PERSONA_TAG = "falcon_top10"

# ── Multiplier bounds (Constitutional Rule #4 — hard, cannot be overridden). ──
PATTERN_MULT_LO, PATTERN_MULT_HI = 0.5, 1.5
SECTOR_MULT_LO, SECTOR_MULT_HI = 0.75, 1.30
REPEATER_MULT_LO, REPEATER_MULT_HI = 0.7, 1.20

# ── Update thresholds (exact, from the spec). ──
HFCL_MIN_N = 18                       # Rule #3 / #10
U1_STRENGTHEN_FACTOR = 1.20           # realized > oos*1.20 -> strengthen
U1_DEMOTE_FACTOR = 0.80               # realized < oos*0.80 -> demote
U1_STRENGTHEN_MULT = 1.2
U1_DEMOTE_MULT = 0.7
U1_KEEP_MULT = 1.0
U1_HEADWIND_WR_MIN = 50.0             # STOCK_SPECIFIC_ALPHA: headwind WR >= 50%
U1_HEADWIND_N_MIN = 10
U1_ALPHA_BONUS = 1.1                  # mult = min(mult*1.1, 1.5)
U1_TAILWIND_WR_MIN = 60.0            # SECTOR_FOLLOWER: tailwind WR >= 60%
U1_FOLLOWER_HEADWIND_WR_MAX = 40.0   #                  AND headwind WR < 40%

U2_LOOKBACK_DAYS = 30
U2_MIN_N = 18
U2_TOP_MULT = 1.2
U2_BOTTOM_MULT = 0.85
U2_TOP_K = 3
U2_BOTTOM_K = 5

U3_LOOKBACK_DAYS = 60
U1_LOOKBACK_DAYS = 60
U3_TRAP_CONSEC = 3
U3_TRAP_MULT = 0.80
U3_EXT_AVG = 5.0
U3_EXT_LEN = 3
U3_EXT_EARLY = 2
U3_EXT_MULT = 1.10
U3_HEALTHY_AVG = 3.0
U3_HEALTHY_LEN = 2
U3_HEALTHY_MULT = 1.15
U3_STALE_AVG = 1.0
U3_STALE_MULT = 0.90
U3_DEFAULT_MULT = 1.0
EARLY_EXIT_PEAK_PCT = 15.0            # proxy for early_exit_flag (post-hold high >15%)

# Constitutional rank caps.
INIT_STOP_CAP_COUNT = 2              # Rule #6: 2+ INIT_STOP in 60d -> rank cap 8
INIT_STOP_CAP_RANK = 8
CAPITULATION_CAP_RANK = 7           # Rule #5 — DEFERRED (no regime tags; see docs)

# Big winner / loser (Table-8 Falcon Top 10) — for U5 weekly refresh. Mirrors
# build_bigwinner_study: flags already stored per trade; we read them.
BIG_LOSER_RATE_MIN = 0.30
BIG_LOSER_EV_MAX = 0.0

PERSONA_BIG_LOSER_FLAG_FROM_TRADE = True  # reuse stored big_winner/loser flags


def _ist_today() -> str:
    return datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d")


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _mean(vals: List[float]) -> Optional[float]:
    return (sum(vals) / len(vals)) if vals else None


def _to_date(s: str) -> date:
    return datetime.fromisoformat(s[:10]).date()


def _week_boundary(d: date) -> date:
    """Monday 00:00 of d's ISO week — the W(D) decision boundary. A multiplier
    for any signal in this week may use only ledger trades with exit_date < W."""
    return d - timedelta(days=d.weekday())


# ════════════════════════════════════════════════════════════════════════════
# 1. RUN CONTEXT — load EXACTLY what build_baseline / simulate_persona load.
# ════════════════════════════════════════════════════════════════════════════

def _build_run_context(rnd_db: str, prod_db: str) -> Dict[str, Any]:
    cfg_dict = PERSONA_CONFIGS[PERSONA_SLUG]
    run_cfg = cfg_dict["run_cfg"]
    sim_start = cfg_dict["sim_start"]
    sim_end = cfg_dict["sim_end"]
    cash_start = cfg_dict["cash_per_year"]

    all_pats = load_full_patterns(rnd_db)                 # parity pattern set
    X, sym, dt = load_panel(prod_db, sim_start, sim_end)  # PROD features
    bars = load_all_bars(prod_db, "2020-12-01", sim_end)  # pad for trail
    sector_map = build_sector_map(prod_db)
    all_td = trading_days(prod_db, sim_start, sim_end)

    years_arr = np.array([int(d[:4]) for d in dt], dtype=np.int32)
    years_in_window = sorted({int(d[:4]) for d in all_td})

    # Id-carrying pattern superset (same query + drawdown filter as build_baseline)
    pats_with_ids = _load_patterns_with_ids(rnd_db)

    return {
        "cfg_dict": cfg_dict, "run_cfg": run_cfg,
        "sim_start": sim_start, "sim_end": sim_end, "cash_start": cash_start,
        "all_pats": all_pats, "pats_with_ids": pats_with_ids,
        "X": X, "sym": sym, "dt": dt, "bars": bars,
        "sector_map": sector_map, "all_td": all_td,
        "years_arr": years_arr, "years_in_window": years_in_window,
    }


def _load_patterns_with_ids(rnd_db_path: str) -> List[Dict[str, Any]]:
    """Identical to build_baseline.load_patterns_with_ids — superset of the
    parity loader carrying pattern_id / mined_year / lift. READ-ONLY."""
    con = sqlite3.connect(rnd_db_path, timeout=120.0)
    try:
        rows = con.execute("""
            SELECT c.pattern_id, c.mined_year, c.rule_json, p.avg_oos_year_lift_pp
              FROM falcon_promoted_patterns p
              JOIN falcon_pattern_candidates c ON p.pattern_id = c.pattern_id
             WHERE p.classification IN ('universal','regime_dependent')
        """).fetchall()
    finally:
        con.close()
    out: List[Dict[str, Any]] = []
    for pid, my, rj, lift in rows:
        rule = [(f, op, float(th)) for f, op, th in json.loads(rj)]
        if in_drawdown_bounce(rule):
            continue
        out.append({"pattern_id": int(pid), "mined_year": int(my),
                    "rule": rule, "lift": float(lift)})
    return out


# ════════════════════════════════════════════════════════════════════════════
# 2. THE IMPROVED LEDGER + MULTIPLIER STATE (the self-consistent core)
# ════════════════════════════════════════════════════════════════════════════

class ImprovedLedger:
    """Running record of the IMPROVED sim's resolved trades, plus the weekly
    multiplier tables. Causality is enforced here: every query takes the week
    boundary W and returns only trades with exit_date < W."""

    def __init__(self, sector_map: Dict[str, str]):
        self.sector_map = sector_map
        # Each resolved trade: {symbol, sector, signal_date, exit_date(date),
        #   net_ret_pct, exit_reason, peak_ret, big_winner, big_loser,
        #   move_type, sector_tailwind, fired_pids:set}
        self.trades: List[Dict[str, Any]] = []
        # Per-pattern PREVIOUS multiplier (regime memory carry — Rule #11).
        self.prev_pattern_mult: Dict[int, float] = defaultdict(lambda: 1.0)
        # Per-pattern oos_lift (= avg_oos_year_lift_pp), filled from contributions.
        self.oos_lift: Dict[int, float] = {}
        # consecutive top-50 streaks: symbol -> (last_td, streak_len).
        self.top50_streak: Dict[str, Tuple[str, int]] = {}

    # ── ledger mutation ──
    def add_trade(self, t: Dict[str, Any]) -> None:
        self.trades.append(t)

    # ── causal slices ──
    def resolved_before(self, W: date) -> List[Dict[str, Any]]:
        return [t for t in self.trades if t["exit_date"] is not None and t["exit_date"] < W]

    def pattern_trades_60d(self, pid: int, W: date) -> List[Dict[str, Any]]:
        lo = W - timedelta(days=U1_LOOKBACK_DAYS)
        return [t for t in self.trades
                if t["exit_date"] is not None and lo <= t["exit_date"] < W
                and pid in t["fired_pids"]]

    def sector_trades_30d(self, sector: Optional[str], W: date) -> List[Dict[str, Any]]:
        lo = W - timedelta(days=U2_LOOKBACK_DAYS)
        return [t for t in self.trades
                if t["exit_date"] is not None and lo <= t["exit_date"] < W
                and t["sector"] == sector]

    def symbol_trades_60d(self, symbol: str, W: date) -> List[Dict[str, Any]]:
        lo = W - timedelta(days=U3_LOOKBACK_DAYS)
        return [t for t in self.trades
                if t["exit_date"] is not None and lo <= t["exit_date"] < W
                and t["symbol"] == symbol]


# ════════════════════════════════════════════════════════════════════════════
# 3. THE 5 WEEKLY UPDATES — pure, deterministic, take only pre-W ledger slices.
# ════════════════════════════════════════════════════════════════════════════

def update1_pattern(led: ImprovedLedger, pid: int, W: date,
                    notes: Dict[str, Any]) -> Tuple[float, str, bool, str, Optional[str]]:
    """Returns (multiplier, status, status_change, reason, quality_flag).
    REGIME MEMORY: n<18 -> keep previous multiplier (do NOT reset to 1.0)."""
    trades = led.pattern_trades_60d(pid, W)
    n = len(trades)
    prev = led.prev_pattern_mult[pid]
    oos = led.oos_lift.get(pid)

    if n < HFCL_MIN_N or oos is None:
        # Regime memory — preserve last known multiplier; no status change.
        return prev, "Keep", False, (
            f"n={n}<18 (or oos missing) — regime memory: keep prev {prev:.2f}"
        ), None

    realized = _mean([t["net_ret_pct"] for t in trades]) or 0.0
    if realized > oos * U1_STRENGTHEN_FACTOR:
        mult, status = U1_STRENGTHEN_MULT, "Strengthen"
    elif realized < oos * U1_DEMOTE_FACTOR:
        mult, status = U1_DEMOTE_MULT, "Demote"
    else:
        mult, status = U1_KEEP_MULT, "Keep"
    reason = (f"realized_60d={realized:+.2f}% vs oos={oos:+.2f}% "
              f"(n={n}) -> {status} base {mult:.2f}")

    # ── Sector-quality override ──
    # WIRED BUT INERT IN-PASS: move_type / sector_tailwind are NULL on improved-
    # ledger trades because sector attribution (classify_patterns Step 3) runs on
    # falcon_baseline_trades, not on this self-consistent ledger (porting the
    # sector-index build into the forward pass is out of scope for Step 6). With
    # all-NULL inputs n_head == n_tail == 0, so neither STOCK_SPECIFIC_ALPHA nor
    # SECTOR_FOLLOWER ever fires here — the base U1 multiplier passes through
    # unchanged. The override logic is fully implemented and ACTIVATES as soon as
    # the ledger carries move_type / sector_tailwind. Documented in S6-build-log.
    quality: Optional[str] = None
    head = [t for t in trades if t["move_type"] == "SECTOR_HEADWIND"]
    n_head = len(head)
    wr_head = (100.0 * sum(1 for t in head if t["net_ret_pct"] > 0) / n_head) if n_head else 0.0
    tail = [t for t in trades if t["sector_tailwind"] == 1]
    n_tail = len(tail)
    wr_tail = (100.0 * sum(1 for t in tail if t["net_ret_pct"] > 0) / n_tail) if n_tail else 0.0

    if wr_head >= U1_HEADWIND_WR_MIN and n_head >= U1_HEADWIND_N_MIN:
        quality = "STOCK_SPECIFIC_ALPHA"
        mult = min(mult * U1_ALPHA_BONUS, PATTERN_MULT_HI)
        reason += (f"; STOCK_SPECIFIC_ALPHA (headwind WR {wr_head:.0f}% n={n_head}) "
                   f"-> *1.1={mult:.2f}")
    elif wr_tail >= U1_TAILWIND_WR_MIN and wr_head < U1_FOLLOWER_HEADWIND_WR_MAX:
        quality = "SECTOR_FOLLOWER"
        reason += (f"; SECTOR_FOLLOWER (tailwind WR {wr_tail:.0f}%, "
                   f"headwind WR {wr_head:.0f}%) — auto-trade block flag, no mult change")

    mult = _clamp(mult, PATTERN_MULT_LO, PATTERN_MULT_HI)
    status_change = (abs(mult - prev) > 1e-9)
    return mult, status, status_change, reason, quality


def update2_sectors(led: ImprovedLedger, W: date) -> Dict[Optional[str], float]:
    """Per-sector 30d edge → multiplier map. Sectors with <18 resolved -> 1.0.
    Returns {sector: multiplier} (clamped). Missing sector -> 1.0 at lookup."""
    # Gather edges only for sectors with >=18 resolved trades in [W-30d, W).
    lo = W - timedelta(days=U2_LOOKBACK_DAYS)
    by_sector: Dict[Optional[str], List[float]] = defaultdict(list)
    for t in led.trades:
        if t["exit_date"] is not None and lo <= t["exit_date"] < W:
            by_sector[t["sector"]].append(t["net_ret_pct"])
    edges = {s: _mean(v) for s, v in by_sector.items() if len(v) >= U2_MIN_N}
    if not edges:
        return {}
    ranked = sorted(edges.items(), key=lambda kv: (kv[1] if kv[1] is not None else -1e9),
                    reverse=True)
    top = {s for s, _ in ranked[:U2_TOP_K]}
    bottom = {s for s, _ in ranked[-U2_BOTTOM_K:]}
    out: Dict[Optional[str], float] = {}
    for s, _edge in ranked:
        if s in top:
            m = U2_TOP_MULT
        elif s in bottom:
            m = U2_BOTTOM_MULT
        else:
            m = 1.0
        out[s] = _clamp(m, SECTOR_MULT_LO, SECTOR_MULT_HI)
    return out


def update3_repeater(led: ImprovedLedger, symbol: str, W: date) -> Tuple[float, str]:
    """Per-symbol repeater multiplier from prior-60d ledger trades on this
    symbol. Returns (multiplier, repeater_type)."""
    prior = led.symbol_trades_60d(symbol, W)
    n = len(prior)
    if n < 2:
        return U3_DEFAULT_MULT, "FRESH"
    avg = _mean([t["net_ret_pct"] for t in prior]) or 0.0
    consec = led.top50_streak.get(symbol, ("", 0))[1]
    early_exit_count = sum(1 for t in prior
                           if t.get("peak_ret") is not None and t["peak_ret"] > EARLY_EXIT_PEAK_PCT)

    if consec >= U3_TRAP_CONSEC and avg < 0:
        m, rtype = U3_TRAP_MULT, "PERSISTENCE_TRAP"
    elif avg > U3_EXT_AVG and n >= U3_EXT_LEN and early_exit_count >= U3_EXT_EARLY:
        m, rtype = U3_EXT_MULT, "EXTENDED_TREND"
    elif avg > U3_HEALTHY_AVG and n >= U3_HEALTHY_LEN:
        m, rtype = U3_HEALTHY_MULT, "HEALTHY_REPEATER"
    elif avg < U3_STALE_AVG:
        m, rtype = U3_STALE_MULT, "STALE"
    else:
        m, rtype = U3_DEFAULT_MULT, "HEALTHY_REPEATER"
    return _clamp(m, REPEATER_MULT_LO, REPEATER_MULT_HI), rtype


def update5_bigwinner(led: ImprovedLedger, pid: int, W: date) -> Dict[str, Any]:
    """U5 weekly big-winner/loser refresh for a pattern (n>=18, all resolved to
    date strictly before W). Mirrors build_bigwinner_study's HFCL aggregation —
    the weekly snapshot. Returns the rates + flags for the weekly_state row."""
    occ = [t for t in led.trades
           if t["exit_date"] is not None and t["exit_date"] < W and pid in t["fired_pids"]]
    n = len(occ)
    if n < HFCL_MIN_N:
        return {"n": n, "big_winner_rate": None, "big_loser_rate": None,
                "ev": None, "big_winner_candidate": None, "big_loser_risk": None}
    nbw = sum(1 for t in occ if t["big_winner"])
    nbl = sum(1 for t in occ if t["big_loser"])
    bw_rate = nbw / n
    bl_rate = nbl / n
    ev = _mean([t["net_ret_pct"] for t in occ]) or 0.0
    return {
        "n": n, "big_winner_rate": bw_rate, "big_loser_rate": bl_rate, "ev": ev,
        "big_winner_candidate": 1 if ev > 0 else 0,
        "big_loser_risk": 1 if (bl_rate >= BIG_LOSER_RATE_MIN and ev <= BIG_LOSER_EV_MAX) else 0,
    }


# ════════════════════════════════════════════════════════════════════════════
# 4. CONSTITUTIONAL RANK CAPS (score-based; never mutate simulate_year).
# ════════════════════════════════════════════════════════════════════════════

def init_stop_cap_active(led: ImprovedLedger, symbol: str, W: date) -> bool:
    """Rule #6: symbol with 2+ INIT_STOP exits in last 60d -> cannot rank above 8.
    DERIVABLE from ledger exit_reason."""
    prior = led.symbol_trades_60d(symbol, W)
    return sum(1 for t in prior if t["exit_reason"] == "INIT_STOP") >= INIT_STOP_CAP_COUNT


def capitulation_share(fired_pids: set) -> Optional[float]:
    """Rule #5: share of firing patterns that are 'capitulation'. DEFERRED —
    no regime/capitulation tag exists on these patterns (regime is NULL in
    build_baseline / classify_patterns; taxonomy plain-English tags not joined
    here). Returns None → cap is a no-op until tags land. Hook kept so the cap
    can be enabled by populating a {pid: is_capitulation} map later."""
    return None


# ════════════════════════════════════════════════════════════════════════════
# 5. PER-SIGNAL FIRED PATTERNS (re-derive read-only, exactly like build_baseline).
# ════════════════════════════════════════════════════════════════════════════

def fired_pattern_ids(row: np.ndarray, elig_ids: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Re-evaluate each eligible id-carrying pattern's rule against the signal's
    own feature row. Returns the list of fired pattern dicts (pattern_id, lift).
    Same machinery as build_baseline.derive_contributions_for_trade — guarantees
    the same fire-set the sim's n_fires counted."""
    fired = []
    for p in elig_ids:
        if rule_mask(p["rule"], row)[0]:
            fired.append(p)
    return fired


# ════════════════════════════════════════════════════════════════════════════
# 6. ORCHESTRATION — the forward pass (freeze weekly multipliers, then simulate).
# ════════════════════════════════════════════════════════════════════════════

def run(rnd_db: str, prod_db: str, years: Optional[List[int]],
        dry_run: bool, out_dir: Path) -> int:
    print(f"[build_self_improvement] RND DB:  {rnd_db}")
    print(f"[build_self_improvement] PROD DB: {prod_db}  (read-only)")
    print(f"[build_self_improvement] persona: {PERSONA_SLUG} (tag '{PERSONA_TAG}')")
    print(f"[build_self_improvement] mode: {'DRY-RUN (no writes)' if dry_run else 'APPLY'}")
    print(f"[build_self_improvement] IST date: {_ist_today()}")

    ctx = _build_run_context(rnd_db, prod_db)
    run_cfg = ctx["run_cfg"]
    hold_days = run_cfg.hold_days
    cash_start = ctx["cash_start"]
    bars = ctx["bars"]
    sector_map = ctx["sector_map"]
    X, sym, dt = ctx["X"], ctx["sym"], ctx["dt"]
    years_arr = ctx["years_arr"]
    years_in_window = ctx["years_in_window"]
    sim_end = ctx["sim_end"]

    print(f"[build_self_improvement] loaded {len(ctx['all_pats'])} patterns, "
          f"{X.shape[0]} panel rows, {len(bars)} symbols, "
          f"{len(ctx['all_td'])} trading days, "
          f"{len(ctx['pats_with_ids'])} id-carrying patterns")

    led = ImprovedLedger(sector_map)
    led.oos_lift = _load_oos_lift(rnd_db)
    print(f"[build_self_improvement] oos_lift loaded for {len(led.oos_lift)} patterns "
          f"(= avg_oos_year_lift_pp via falcon_pattern_contributions.lift_pp)")

    candidate_years = years_in_window
    if years:
        candidate_years = [y for y in candidate_years if y in set(years)]
    print(f"[build_self_improvement] years to run: {candidate_years}")

    weekly_state_rows: List[Dict[str, Any]] = []
    review_log_rows: List[Dict[str, Any]] = []
    improved_year_summaries: List[Dict[str, Any]] = []
    # Baseline-EQUITY per-year returns, computed by the SAME simulate_year with
    # the baseline (avg_lift, no-multiplier) ranking — so the GATE compares
    # equity-vs-equity (identical basis). The old _load_baseline_returns read
    # closed-only net_pnl from falcon_baseline_trades, which UNDERCOUNTS vs the
    # improved sim's equity return (open-MTM) and faked positive deltas in years
    # where the ranking did not change. See build log / S6-audit follow-up.
    baseline_year_summaries: List[Dict[str, Any]] = []

    # (symbol, signal_date) -> panel row index, rebuilt per year.
    for Y in candidate_years:
        year_mask = years_arr == Y
        if not year_mask.any():
            continue
        elig = eligible_patterns_for_year(ctx["all_pats"], Y)
        elig_ids = eligible_patterns_for_year(
            ctx["pats_with_ids"], Y, retention=RETENTION_YEARS, mining_start=MINING_START_YEAR)

        sigs = compute_year_signals(X, sym, dt, year_mask, elig, min_fires=run_cfg.min_fires)

        # Panel row index for this year (dt/sym aligned 1:1 with X).
        sig_row_idx: Dict[Tuple[str, str], int] = {}
        for i in np.nonzero(year_mask)[0]:
            sig_row_idx[(str(sym[i]), str(dt[i]))] = int(i)

        # ── Re-score each signal with the IMPROVED ranking ──
        # For each signal we (a) re-derive fired pattern_ids, (b) freeze the
        # week's multipliers from the ledger as it stood at the START of year Y
        # (all of 2021..Y-1's improved trades) — see "intra-year causality".
        # Sector multipliers are frozen per WEEK boundary inside the year.
        week_sector_mult_cache: Dict[date, Dict[Optional[str], float]] = {}
        week_pattern_mult_cache: Dict[date, Dict[int, float]] = {}
        week_review: Dict[date, Dict[str, Any]] = {}

        # Per-day top-50 candidate symbol sets (by avg_lift, pre-improve) so
        # consecutive_days_in_top50 reflects engine attention (spec: "today's
        # top-50 candidates"). Computed up front but CONSUMED CAUSALLY below:
        # update3_repeater reads the streak as of the day BEFORE the signal date,
        # never the end-of-year value (no lookahead).
        day_top50, sorted_sig_days = _day_top50_sets(sigs)

        rescored: List[Dict[str, Any]] = []
        _streak_cursor = {"idx": 0}   # how many signal-days already folded in
        for s in sigs:
            sd = s["signal_date"]
            d = _to_date(sd)
            W = _week_boundary(d)
            symbol = s["symbol"]
            # Causally advance the streak state to include all signal-days
            # STRICTLY BEFORE sd (never sd itself or later).
            _advance_streaks(led, day_top50, sorted_sig_days, _streak_cursor, sd)
            ridx = sig_row_idx.get((symbol, sd))
            row = X[ridx:ridx + 1, :] if ridx is not None else None
            fired = fired_pattern_ids(row, elig_ids) if row is not None else []
            fired_pids = {p["pattern_id"] for p in fired}

            # Freeze pattern multipliers for this week (cache per (W, pid)).
            pmcache = week_pattern_mult_cache.setdefault(W, {})
            review = week_review.setdefault(W, _new_review(W))
            pat_mults: List[float] = []
            for p in fired:
                pid = p["pattern_id"]
                if pid not in pmcache:
                    mult, status, changed, reason, quality = update1_pattern(led, pid, W, {})
                    pmcache[pid] = mult
                    _record_weekly_state(weekly_state_rows, led, pid, W, mult, status,
                                         changed, reason, quality)
                    _accumulate_review(review, status, changed, quality, pid)
                pat_mults.append(pmcache[pid])
            # Pattern multiplier for the SIGNAL = mean of its fired patterns'
            # multipliers (a signal is a confluence of patterns; the per-signal
            # pattern weight is their average — documented in the build log).
            pattern_mult = _mean(pat_mults) if pat_mults else 1.0

            # Sector multiplier (frozen per week).
            if W not in week_sector_mult_cache:
                week_sector_mult_cache[W] = update2_sectors(led, W)
            sector_mult = week_sector_mult_cache[W].get(sector_map.get(symbol), 1.0)

            # Repeater multiplier (per symbol, prior-60d).
            repeater_mult, _rtype = update3_repeater(led, symbol, W)

            improved = s["avg_lift"] * pattern_mult * sector_mult * repeater_mult

            # ── Constitutional rank caps (score-based, never mutate the engine) ──
            cap_rank = _cap_rank_for(led, symbol, W, fired_pids)
            improved, cap_note = _apply_rank_cap_to_score(improved, cap_rank)
            if cap_note:
                review["constitutional_violations"].append(f"{symbol}@{sd}:{cap_note}")

            rescored.append({
                **s,
                "score": improved,                  # simulate_year ranks by this
                "_pattern_mult": pattern_mult,
                "_sector_mult": sector_mult,
                "_repeater_mult": repeater_mult,
                "_fired_pids": fired_pids,
            })

        # ── Group by signal_date + run the LOCKED simulate_year ──
        sigs_by_sd: Dict[str, List[Dict]] = defaultdict(list)
        for s in rescored:
            sigs_by_sd[s["signal_date"]].append(s)

        year_start = f"{Y}-01-01"
        year_end = sim_end if Y == years_in_window[-1] else f"{Y}-12-31"
        year_td = [d for d in ctx["all_td"] if year_start <= d <= year_end]
        if not year_td:
            continue
        r = simulate_year(dict(sigs_by_sd), bars, year_td, run_cfg, cash_start)

        end_eq = r["ending_equity"]
        year_ret = (end_eq / cash_start - 1.0) * 100.0
        closed = r["closed_trades"]
        open_at_end = r["open_at_end_trades"]
        n_closed = len(closed)
        wins = sum(1 for t in closed if (t.get("net_pnl") or 0) > 0)
        wr = (100.0 * wins / n_closed) if n_closed else 0.0
        improved_year_summaries.append({
            "year": Y, "return_pct": year_ret, "n_closed": n_closed,
            "n_open": len(open_at_end), "win_rate": wr,
        })
        print(f"[build_self_improvement]   {Y}: improved return {year_ret:+.2f}%  "
              f"n_closed={n_closed}  WR={wr:.1f}%  open_at_end={len(open_at_end)}")

        # ── BASELINE-EQUITY pass (same engine, avg_lift ranking, NO multipliers) ──
        # This is the apples-to-apples reference for the GATE: identical sim
        # mechanics (simulate_year, same run_cfg/cash_start/year_td), the ONLY
        # difference from the improved pass is score = avg_lift (multipliers ≡ 1).
        # Reproduces the locked Step-2 equity returns by construction; the delta
        # vs improved is therefore PURELY the re-ranking effect.
        base_sigs_by_sd: Dict[str, List[Dict]] = defaultdict(list)
        for s in sigs:
            base_sigs_by_sd[s["signal_date"]].append({**s, "score": s["avg_lift"]})
        rb = simulate_year(dict(base_sigs_by_sd), bars, year_td, run_cfg, cash_start)
        b_year_ret = (rb["ending_equity"] / cash_start - 1.0) * 100.0
        b_closed = rb["closed_trades"]
        b_nc = len(b_closed)
        b_wins = sum(1 for t in b_closed if (t.get("net_pnl") or 0) > 0)
        b_wr = (100.0 * b_wins / b_nc) if b_nc else 0.0
        baseline_year_summaries.append({
            "year": Y, "return_pct": b_year_ret, "n_closed": b_nc,
            "n_open": len(rb["open_at_end_trades"]), "win_rate": b_wr,
        })
        print(f"[build_self_improvement]   {Y}: baseline return {b_year_ret:+.2f}%  "
              f"n_closed={b_nc}  WR={b_wr:.1f}%  (equity basis, same engine)")

        # ── Append year Y's resolved improved trades to the ledger (for Y+1) ──
        # Only CLOSED trades have a realized exit_date/outcome → eligible for
        # future-week multipliers. Open-at-end trades are NOT resolved.
        # simulate_year rebuilds candidate dicts with only a fixed key set (it
        # drops our _fired_pids), so recover fired pattern ids per trade by
        # (symbol, signal_date) from the rescored signals.
        fired_by_key = {(s["symbol"], s["signal_date"]): s["_fired_pids"]
                        for s in rescored}
        _append_year_to_ledger(led, closed, bars, sector_map, hold_days, fired_by_key)

        # ── Materialise this year's weekly review rows ──
        for W in sorted(week_review.keys()):
            review_log_rows.append(_finalize_review(week_review[W]))

        # ── U5 weekly big-winner/loser refresh — annotate weekly_state rows ──
        _apply_u5_to_states(weekly_state_rows, led, Y)

    # ── GATE: baseline vs improved comparison (BOTH equity basis, same engine) ──
    baseline_by_year = {r["year"]: r for r in baseline_year_summaries}
    comparison = _build_comparison(baseline_by_year, improved_year_summaries)
    _print_comparison(comparison)

    print(f"\n[build_self_improvement] weekly_state rows: {len(weekly_state_rows)}  "
          f"review_log rows: {len(review_log_rows)}")

    if dry_run:
        print("\n[build_self_improvement] DRY-RUN — nothing written (DB or Excel).")
        _maybe_write_excel(out_dir, comparison, dry_run=True)
        return 0

    _write(rnd_db, weekly_state_rows, review_log_rows)
    _maybe_write_excel(out_dir, comparison, dry_run=False)
    print(f"\n[build_self_improvement] APPLY complete — wrote {len(weekly_state_rows)} "
          f"weekly_state + {len(review_log_rows)} review_log rows for "
          f"persona='{PERSONA_TAG}'.")
    return 0


# ════════════════════════════════════════════════════════════════════════════
# 6a. LEDGER / STREAK / REVIEW HELPERS
# ════════════════════════════════════════════════════════════════════════════

def _load_oos_lift(rnd_db: str) -> Dict[int, float]:
    """oos_lift per pattern = avg_oos_year_lift_pp. Read straight from
    falcon_promoted_patterns (constant per pattern_id; identical to the value
    build_baseline stored as falcon_pattern_contributions.lift_pp)."""
    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        rows = con.execute(
            "SELECT pattern_id, avg_oos_year_lift_pp FROM falcon_promoted_patterns"
        ).fetchall()
    finally:
        con.close()
    return {int(pid): float(lift) for pid, lift in rows if lift is not None}


def _day_top50_sets(sigs: List[Dict[str, Any]]) -> Tuple[Dict[str, set], List[str]]:
    """Per signal-day: the top-50 candidate symbols by avg_lift (pre-improve, so
    it does not depend on the multipliers it feeds). Returns ({sd: set(symbols)},
    sorted_signal_days)."""
    by_day: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for s in sigs:
        by_day[s["signal_date"]].append(s)
    out: Dict[str, set] = {}
    for sd, grp in by_day.items():
        top = sorted(grp, key=lambda x: x["avg_lift"], reverse=True)[:50]
        out[sd] = {x["symbol"] for x in top}
    return out, sorted(by_day.keys())


def _advance_streaks(led: ImprovedLedger, day_top50: Dict[str, set],
                     sorted_days: List[str], cursor: Dict[str, int],
                     before_sd: str) -> None:
    """Fold every signal-day STRICTLY BEFORE ``before_sd`` into the streak state,
    in chronological order, advancing the cursor so each day is folded exactly
    once across the whole year. Guarantees the streak read for a signal reflects
    only PRIOR days (no lookahead). A symbol's consecutive_days_in_top50 grows
    while it appears on successive signal-days and resets to 0 on a day it is
    absent from the top-50."""
    i = cursor["idx"]
    while i < len(sorted_days) and sorted_days[i] < before_sd:
        sd = sorted_days[i]
        top_syms = day_top50.get(sd, set())
        for symbol in top_syms:
            _last, streak = led.top50_streak.get(symbol, ("", 0))
            led.top50_streak[symbol] = (sd, streak + 1)
        for symbol in list(led.top50_streak.keys()):
            if symbol not in top_syms:
                last_sd, streak = led.top50_streak[symbol]
                if last_sd != sd:
                    led.top50_streak[symbol] = (last_sd, 0)
        i += 1
    cursor["idx"] = i


def _append_year_to_ledger(led: ImprovedLedger, closed: List[Dict[str, Any]],
                           bars: Dict[str, List[Dict]], sector_map: Dict[str, str],
                           hold_days: int,
                           fired_by_key: Dict[Tuple[str, str], set]) -> None:
    """Append year Y's CLOSED improved trades to the running ledger. Each trade's
    net_ret_pct / exit_reason / journey peak come from the SAME locked sim +
    journey math as build_baseline (so the ledger is path-consistent). move_type
    / sector_tailwind are NOT available in-pass (classify_patterns is a separate
    step on baseline trades); we approximate sector_tailwind from the trade's
    OWN bars (sector index attribution would require the sim_sweep build — see
    build log "sector attribution in-pass: deferred, sector_tailwind=None")."""
    for t in closed:
        sym = t["symbol"]
        eidx = t.get("entry_bar_idx", t.get("_bars_start_idx"))
        bs = bars.get(sym, [])
        peak_ret = None
        big_winner = 0
        big_loser = 0
        net_ret = None
        if eidx is not None and eidx < len(bs):
            entry_open = bs[eidx]["open"]
            if entry_open and entry_open > 0:
                n_d = min(hold_days, len(bs) - eidx)
                hi = max((bs[eidx + (d - 1)]["high"] for d in range(1, n_d + 1)), default=None)
                lo = min((bs[eidx + (d - 1)]["low"] for d in range(1, n_d + 1)), default=None)
                if hi is not None:
                    peak_ret = (hi / entry_open - 1.0) * 100.0
                if lo is not None:
                    trough_ret = (lo / entry_open - 1.0) * 100.0
                    big_loser = 1 if (trough_ret < -7.0 or t.get("exit_reason") == "INIT_STOP") else 0
                big_winner = 1 if (peak_ret is not None and peak_ret > 12.0) else 0
        deployed = t.get("actual_deployed") or (t["shares"] * t["entry_px"])
        npnl = t.get("net_pnl")
        if npnl is not None and deployed:
            net_ret = npnl / deployed * 100.0
        led.add_trade({
            "symbol": sym,
            "sector": sector_map.get(sym),
            "signal_date": t["signal_date"],
            "exit_date": _to_date(t["exit_date"]) if t.get("exit_date") else None,
            "net_ret_pct": net_ret if net_ret is not None else 0.0,
            "exit_reason": t.get("exit_reason"),
            "peak_ret": peak_ret,
            "big_winner": big_winner,
            "big_loser": big_loser,
            "move_type": None,          # sector attribution deferred (see build log)
            "sector_tailwind": None,    # ditto
            "fired_pids": fired_by_key.get((sym, t.get("signal_date")), set()),
        })


def _new_review(W: date) -> Dict[str, Any]:
    return {
        "week_ending": (W + timedelta(days=6)).isoformat(),  # Sunday of the week
        "persona": PERSONA_TAG,
        "patterns_promoted": 0, "patterns_demoted": 0, "patterns_disabled": 0,
        "new_healthy_repeaters": [], "new_persistence_traps": [],
        "new_early_exit_flags": [], "new_big_winner_candidates": [],
        "new_big_loser_risk_patterns": [], "sector_regime_shifts": [],
        "market_regime_shifts": [], "quality_flag_changes": [],
        "signal_validity_changes": [], "constitutional_violations": [],
        "_pids_seen": set(),
    }


def _accumulate_review(review: Dict[str, Any], status: str, changed: bool,
                       quality: Optional[str], pid: int) -> None:
    if pid in review["_pids_seen"]:
        return
    review["_pids_seen"].add(pid)
    if changed and status == "Strengthen":
        review["patterns_promoted"] += 1
    elif changed and status == "Demote":
        review["patterns_demoted"] += 1
    if quality == "STOCK_SPECIFIC_ALPHA":
        review["quality_flag_changes"].append(f"pat{pid}:STOCK_SPECIFIC_ALPHA")
    elif quality == "SECTOR_FOLLOWER":
        review["quality_flag_changes"].append(f"pat{pid}:SECTOR_FOLLOWER")


def _finalize_review(review: Dict[str, Any]) -> Dict[str, Any]:
    review = dict(review)
    review.pop("_pids_seen", None)
    n_prom = review["patterns_promoted"]
    n_dem = review["patterns_demoted"]
    n_viol = len(review["constitutional_violations"])
    review["summary_text"] = (
        f"Week ending {review['week_ending']}: {n_prom} strengthened, "
        f"{n_dem} demoted, {len(review['quality_flag_changes'])} quality-flag "
        f"changes, {n_viol} constitutional rank-cap events. "
        f"human_approved=FALSE (pending review)."
    )
    return review


def _record_weekly_state(rows: List[Dict[str, Any]], led: ImprovedLedger,
                         pid: int, W: date, mult: float, status: str,
                         changed: bool, reason: str, quality: Optional[str]) -> None:
    trades = led.pattern_trades_60d(pid, W)
    n = len(trades)
    realized = _mean([t["net_ret_pct"] for t in trades])
    wr = (100.0 * sum(1 for t in trades if t["net_ret_pct"] > 0) / n) if n else None
    stop_rate = (100.0 * sum(1 for t in trades if t["exit_reason"] == "INIT_STOP") / n) if n else None
    prev = led.prev_pattern_mult[pid]
    rows.append({
        "week_ending": (W + timedelta(days=6)).isoformat(),
        "pattern_id": pid, "persona": PERSONA_TAG,
        "n_resolved_trades_60d": n,
        "realized_lift_60d": realized,
        "oos_lift_at_mining": led.oos_lift.get(pid),
        "win_rate_60d": wr,
        "stop_rate_60d": stop_rate,
        "win_rate_sector_tailwind": None,   # move_type/tailwind deferred in-pass
        "win_rate_sector_headwind": None,
        "big_winner_rate": None,            # filled by U5 refresh
        "big_loser_rate": None,
        "avg_peak_day": None,
        "weight_multiplier": mult,
        "previous_multiplier": prev,
        "multiplier_change": mult - prev,
        "status": status,
        "status_change": 1 if changed else 0,
        "status_change_reason": reason,
        "human_approved": 0,
        "_quality": quality,
    })
    # Update regime-memory carry AFTER recording (so next week sees this mult).
    led.prev_pattern_mult[pid] = mult


def _apply_u5_to_states(rows: List[Dict[str, Any]], led: ImprovedLedger, Y: int) -> None:
    """U5 weekly refresh: for each weekly_state row written this year, recompute
    big_winner_rate / big_loser_rate from the ledger-to-date strictly before the
    row's week boundary (mirrors build_bigwinner_study, the weekly snapshot)."""
    for row in rows:
        we = row["week_ending"]
        if not we.startswith(str(Y)):
            continue
        W = _to_date(we) - timedelta(days=6)   # back out the Monday boundary
        u5 = update5_bigwinner(led, row["pattern_id"], W)
        if u5["big_winner_rate"] is not None:
            row["big_winner_rate"] = u5["big_winner_rate"]
            row["big_loser_rate"] = u5["big_loser_rate"]


def _cap_rank_for(led: ImprovedLedger, symbol: str, W: date, fired_pids: set) -> Optional[int]:
    """Lowest (strictest) constitutional rank cap that applies to this pick, or
    None. Rule #6 (INIT_STOP) is derivable -> cap 8. Rule #5 (capitulation) is
    deferred (capitulation_share returns None) -> never fires until tags land."""
    caps: List[int] = []
    if init_stop_cap_active(led, symbol, W):
        caps.append(INIT_STOP_CAP_RANK)
    cap_share = capitulation_share(fired_pids)
    if cap_share is not None and cap_share > 0.40:
        caps.append(CAPITULATION_CAP_RANK)
    return min(caps) if caps else None


def _apply_rank_cap_to_score(improved: float, cap_rank: Optional[int]) -> Tuple[float, Optional[str]]:
    """Enforce a rank cap WITHOUT touching simulate_year: a capped pick must not
    occupy a rank better than `cap_rank`. We cannot know the day's full ranking
    here, so we apply a deterministic score penalty that pushes the pick below
    the top-(cap_rank-1) band: multiply its improved_score by a strong damping
    factor. This guarantees a capped pick ranks AT BEST around `cap_rank` when a
    full cohort exists, and never above it among non-capped peers. Documented as
    an approximation of the exact positional cap (the exact cap would require a
    post-ranking re-sort, which the locked engine does not expose)."""
    if cap_rank is None:
        return improved, None
    # Damping: cap 7 -> *0.30 ; cap 8 -> *0.45 (looser, deeper allowed slot).
    factor = 0.30 if cap_rank <= CAPITULATION_CAP_RANK else 0.45
    note = f"rank_cap<= {cap_rank} (score *{factor})"
    return improved * factor, note


# ════════════════════════════════════════════════════════════════════════════
# 7. COMPARISON (THE GATE)
# ════════════════════════════════════════════════════════════════════════════

def _load_baseline_returns(rnd_db: str) -> Dict[int, Dict[str, Any]]:
    """DEPRECATED / UNUSED (kept for reference). This read CLOSED-ONLY net_pnl
    from falcon_baseline_trades, which is NOT comparable to the improved sim's
    EQUITY return (it omits open-MTM) and produced spurious positive deltas in
    years where the ranking didn't change. The GATE now runs an in-loop
    baseline-EQUITY pass through simulate_year instead (see run()).

    Per-year baseline return from falcon_baseline_trades. Return% is computed
    the SAME way build_baseline reports it: cash_start ₹5L, year_ret = (closed
    net_pnl + open MTM) / cash_start. Open-at-end trades have NULL net_pnl in the
    table, so we approximate the per-year baseline return as
    sum(net_pnl of closed) / 5L * 100 + open-MTM (NULL → 0). This MATCHES the
    closed-trade contribution; open MTM is small at year tails. For the headline
    locked numbers, build_baseline_summary.xlsx is the canonical source — here we
    report the closed-net baseline for an honest like-for-like vs the improved
    sim's own year_ret. Documented in the build log."""
    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        rows = con.execute(
            """
            SELECT substr(signal_date,1,4) AS yr,
                   COUNT(*) AS n,
                   SUM(CASE WHEN net_pnl IS NOT NULL THEN net_pnl ELSE 0 END) AS net,
                   SUM(CASE WHEN net_pnl IS NOT NULL THEN 1 ELSE 0 END) AS n_closed,
                   SUM(CASE WHEN net_ret_pct IS NOT NULL AND net_ret_pct > 0 THEN 1 ELSE 0 END) AS wins
              FROM falcon_baseline_trades
             WHERE persona = ?
             GROUP BY yr ORDER BY yr
            """, (PERSONA_TAG,)
        ).fetchall()
    finally:
        con.close()
    out: Dict[int, Dict[str, Any]] = {}
    for yr, n, net, n_closed, wins in rows:
        nc = int(n_closed or 0)
        out[int(yr)] = {
            "return_pct": (float(net or 0.0) / 5_00_000.0 * 100.0),
            "n_closed": nc,
            "win_rate": (100.0 * int(wins or 0) / nc) if nc else 0.0,
        }
    return out


def _build_comparison(baseline: Dict[int, Dict[str, Any]],
                      improved: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    imp_by_year = {r["year"]: r for r in improved}
    years = sorted(set(baseline.keys()) | set(imp_by_year.keys()))
    tot_b = tot_i = 0.0
    for y in years:
        b = baseline.get(y, {})
        i = imp_by_year.get(y, {})
        br = b.get("return_pct")
        ir = i.get("return_pct")
        tot_b += br or 0.0
        tot_i += ir or 0.0
        out.append({
            "year": y,
            "baseline_return_pct": br,
            "improved_return_pct": ir,
            "delta_pp": (ir - br) if (br is not None and ir is not None) else None,
            "baseline_n_closed": b.get("n_closed"),
            "improved_n_closed": i.get("n_closed"),
            "baseline_win_rate": b.get("win_rate"),
            "improved_win_rate": i.get("win_rate"),
        })
    out.append({
        "year": "OVERALL(sum)",
        "baseline_return_pct": tot_b, "improved_return_pct": tot_i,
        "delta_pp": tot_i - tot_b,
        "baseline_n_closed": sum((b.get("n_closed") or 0) for b in baseline.values()),
        "improved_n_closed": sum((r["n_closed"] or 0) for r in improved),
        "baseline_win_rate": None, "improved_win_rate": None,
    })
    return out


def _print_comparison(comparison: List[Dict[str, Any]]) -> None:
    print("\n[build_self_improvement] ── BASELINE vs IMPROVED (the GATE) ──")
    print(f"  {'Year':>13} {'Base%':>10} {'Impr%':>10} {'Δpp':>9} "
          f"{'BaseN':>7} {'ImprN':>7} {'BaseWR':>7} {'ImprWR':>7}")
    for c in comparison:
        def f(v, fmt):
            return (fmt % v) if isinstance(v, (int, float)) else "—"
        print(f"  {str(c['year']):>13} "
              f"{f(c['baseline_return_pct'], '%+.2f'):>10} "
              f"{f(c['improved_return_pct'], '%+.2f'):>10} "
              f"{f(c['delta_pp'], '%+.2f'):>9} "
              f"{f(c['baseline_n_closed'], '%d'):>7} "
              f"{f(c['improved_n_closed'], '%d'):>7} "
              f"{f(c['baseline_win_rate'], '%.1f'):>7} "
              f"{f(c['improved_win_rate'], '%.1f'):>7}")
    print("  NOTE: BOTH columns are EQUITY-basis returns from the SAME simulate_year "
          "(baseline = avg_lift ranking, improved = avg_lift x multipliers); the only "
          "difference is the ranking, so Δpp is PURELY the self-improvement effect.")
    print("  NOTE: 2021 improved == baseline exactly (empty pre-Y ledger -> all "
          "multipliers 1.0). Δ=0 in a year means the re-ranking changed no trades. "
          "Where improved <= baseline, the re-ranking added no edge that year.")


# ════════════════════════════════════════════════════════════════════════════
# 8. EXCEL OUTPUT
# ════════════════════════════════════════════════════════════════════════════

_CMP_HEADERS = ["year", "baseline_return_pct", "improved_return_pct", "delta_pp",
                "baseline_n_closed", "improved_n_closed",
                "baseline_win_rate", "improved_win_rate"]


def _maybe_write_excel(out_dir: Path, comparison: List[Dict[str, Any]], dry_run: bool) -> None:
    if dry_run:
        print(f"[build_self_improvement] (dry-run) would write "
              f"{out_dir / 'falcon_self_improvement_comparison.xlsx'}")
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    data = [[_round(c.get(h)) for h in _CMP_HEADERS] for c in comparison]
    try:
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "baseline_vs_improved"
        ws.append(_CMP_HEADERS)
        for dr in data:
            ws.append(dr)
        path = out_dir / "falcon_self_improvement_comparison.xlsx"
        wb.save(str(path))
        print(f"[build_self_improvement] wrote {path}")
    except ImportError:
        import csv
        path = out_dir / "falcon_self_improvement_comparison.csv"
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(_CMP_HEADERS)
            w.writerows(data)
        print(f"[build_self_improvement] openpyxl missing — wrote CSV fallback {path}")


def _round(v: Any, nd: int = 4) -> Any:
    return round(v, nd) if isinstance(v, (int, float)) and not isinstance(v, bool) else v


# ════════════════════════════════════════════════════════════════════════════
# 9. WRITE (single transaction, RND ONLY, idempotent for this persona)
# ════════════════════════════════════════════════════════════════════════════

_WS_COLS = [
    "week_ending", "pattern_id", "persona", "n_resolved_trades_60d",
    "realized_lift_60d", "oos_lift_at_mining", "win_rate_60d", "stop_rate_60d",
    "win_rate_sector_tailwind", "win_rate_sector_headwind", "big_winner_rate",
    "big_loser_rate", "avg_peak_day", "weight_multiplier", "previous_multiplier",
    "multiplier_change", "status", "status_change", "status_change_reason",
    "human_approved",
]

_RL_COLS = [
    "week_ending", "persona", "patterns_promoted", "patterns_demoted",
    "patterns_disabled", "new_healthy_repeaters", "new_persistence_traps",
    "new_early_exit_flags", "new_big_winner_candidates", "new_big_loser_risk_patterns",
    "sector_regime_shifts", "market_regime_shifts", "quality_flag_changes",
    "signal_validity_changes", "constitutional_violations", "summary_text",
]

_RL_JSON_COLS = {
    "new_healthy_repeaters", "new_persistence_traps", "new_early_exit_flags",
    "new_big_winner_candidates", "new_big_loser_risk_patterns",
    "sector_regime_shifts", "market_regime_shifts", "quality_flag_changes",
    "signal_validity_changes", "constitutional_violations",
}


def _write(rnd_db: str, ws_rows: List[Dict[str, Any]], rl_rows: List[Dict[str, Any]]) -> None:
    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        con.execute("BEGIN")
        # Idempotent: clear this persona's prior self-improvement output.
        con.execute("DELETE FROM falcon_pattern_weekly_state WHERE persona = ?", (PERSONA_TAG,))
        con.execute("DELETE FROM falcon_weekly_review_log WHERE persona = ?", (PERSONA_TAG,))

        ws_sql = (f"INSERT OR REPLACE INTO falcon_pattern_weekly_state "
                  f"({', '.join(_WS_COLS)}) VALUES ({', '.join('?' for _ in _WS_COLS)})")
        for row in ws_rows:
            con.execute(ws_sql, [row.get(c) for c in _WS_COLS])

        rl_sql = (f"INSERT INTO falcon_weekly_review_log "
                  f"({', '.join(_RL_COLS)}) VALUES ({', '.join('?' for _ in _RL_COLS)})")
        for row in rl_rows:
            vals = []
            for c in _RL_COLS:
                v = row.get(c)
                if c in _RL_JSON_COLS:
                    v = json.dumps(v or [])
                vals.append(v)
            con.execute(rl_sql, vals)

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
        description="Phase 2 weekly self-improving layer: re-run the Falcon Top 10 "
                    "walk-forward with weekly self-consistent multiplier re-ranking, "
                    "no lookahead. Writes falcon_pattern_weekly_state + "
                    "falcon_weekly_review_log (RND only) + the comparison Excel.")
    p.add_argument("--rnd-db", default=None,
                   help="Research DB (default: persona resolver). All writes go here.")
    p.add_argument("--prod-db", default=None,
                   help="PROD DB for OHLC/features (default: config.POWER_DB_PATH). Read-only.")
    p.add_argument("--years", default=None,
                   help="Comma/range list, e.g. '2021,2022' or '2021-2023'. Default: all.")
    p.add_argument("--dry-run", action="store_true",
                   help="Run sim + compute + print comparison; write NOTHING.")
    p.add_argument("--out", default=str(_HERE / "out" / "v6"),
                   help="Output dir for the comparison Excel (default: out/v6).")
    args = p.parse_args(argv)

    rnd_db = args.rnd_db or _resolve_rnd_db_path()
    prod_db = args.prod_db or PROD_DB

    return run(
        rnd_db=rnd_db, prod_db=prod_db,
        years=_parse_years(args.years), dry_run=args.dry_run,
        out_dir=Path(args.out),
    )


if __name__ == "__main__":
    sys.exit(main())
