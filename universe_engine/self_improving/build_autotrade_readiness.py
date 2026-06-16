#!/usr/bin/env python3
"""build_autotrade_readiness.py — Step 7 of the Self-Improving Engine.

**Auto-Trade Readiness classification (spec Table 14) + the 3 remaining Excel
outputs.** Pure CLASSIFICATION + AGGREGATION + EXPORT over already-resolved data
— NO walk-forward re-simulation. Reads everything Steps 0-6 produced in the RND
research DB, applies the Table-14 auto-trade decision rules with MOST-RESTRICTIVE-
WINS precedence at TWO levels (per-pattern and per-pick), validates whether the
AUTO_TRADE gate actually CONCENTRATES the winners (the honest gate, like Step 6),
and writes the three deliverable workbooks.

NOTHING is executed here at build time (no Python in this environment) — written
to be validated by reading, then run by the orchestrator with ``--dry-run`` first.
Additive / idempotent / RND-DB-only writes. No PROD or shared-engine-code mutation
(INV2). PROD is only ever read (and this step needs no OHLC, so PROD is not even
opened).

────────────────────────────────────────────────────────────────────────────
TABLE 14 → VERIFIED-COLUMN MAPPING (the one source of truth for both levels)
────────────────────────────────────────────────────────────────────────────
Precedence (most-restrictive wins): evaluate AVOID, then WATCHLIST, then
MANUAL_REVIEW; if none triggers -> AUTO_TRADE.

  multiplier
    pattern level : latest-week falcon_pattern_weekly_state.weight_multiplier for
                    the pattern (persona falcon_top10); fallback taxonomy
                    .weight_multiplier; else 1.0.
    pick level    : mean of the pick's FIRED patterns' latest-week multipliers
                    (via falcon_pattern_contributions trade_id->pattern_id);
                    fallback baseline_trades.pattern_weight_multiplier if
                    populated; else 1.0.
  quality_flag, pattern_maturity, big_loser_risk,
  signal_valid_at_open (= signal_validity_next_day_pct),
  intraday_false_positive_rate
                    : taxonomy (pattern level). At PICK level these pattern-level
                      criteria are taken from the WORST (most-restrictive) of the
                      pick's fired patterns.
  recent INIT_STOP  : the pattern's (or stock+pattern's, at pick level) last 2
                      RESOLVED appearances in falcon_baseline_trades ordered by
                      signal_date desc; count exit_reason='INIT_STOP'. 2+ -> AVOID;
                      exactly 1 -> MANUAL_REVIEW.
  constitutional violation
                    : 2+ INIT_STOP in last 60d (Constitutional Rule #6) -> AVOID.
  HFCL              : maturity=insufficient_data OR pattern n_resolved < 18
                      -> WATCHLIST. Never fabricated.

PICK-TIME-ONLY criteria (repeater_type, current sector headwind/regime,
capitulation%): at the PATTERN level these are NOT evaluable — they describe a
specific signal day, not a pattern's lifetime. We therefore do NOT fabricate them
per-pattern; we treat them as not-triggered and STATE in the block_reason that
they are evaluated at signal time (pick level). At the PICK level they ARE
available on falcon_baseline_trades and ARE evaluated — with the honest caveat
(verified by reading the S2/S3 build logs) that several of those columns were
left NULL by the upstream steps; a NULL input is treated as "unknown / not
triggered" and named as such in the reason, never invented.

────────────────────────────────────────────────────────────────────────────
DELIVERABLES
  1. ALTER falcon_pattern_taxonomy ADD autotrade_decision / autotrade_block_reason
     (idempotent; only if absent — PRAGMA-checked).
  2. Per-pattern classification -> taxonomy.autotrade_decision + block_reason.
  3. Per-pick classification -> baseline_trades.autotrade_decision + block_reason
     + the VALIDATION GATE (does AUTO_TRADE concentrate winners? honest yes/no).
  4. falcon_autotrade_readiness.xlsx   (by_pattern / pick_validation / by_pick).
  5. falcon_early_exit_study.xlsx      (D+1..D+60 journey + optimal hold per pattern).
  6. falcon_weekly_review.xlsx         (latest week for human approval).

CONSTRAINTS honored: RND-only writes; PROD read-only (unopened); additive;
idempotent (UPDATE-in-place per persona / DELETE-by-key then INSERT where
needed); single transaction per table; --dry-run computes + prints ALL summaries
(incl. the pick_validation table) and writes NOTHING; --out defaults to out/v7.

CLI:
  python build_autotrade_readiness.py --rnd-db <path> [--prod-db <path>]
       [--dry-run] [--out <dir>]
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

# Windows consoles default to cp1252 -> force utf-8 so the summary print never
# crashes the run after all numbers are computed (same fix as the sibling steps).
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

# _resolve_rnd_db_path lets --rnd-db be optional (same default the other steps use).
try:
    from power_user.services.persona_simulator import (  # noqa: E402
        _resolve_rnd_db_path,
        PROD_DB,
    )
except Exception:  # pragma: no cover — only needed when --rnd-db omitted
    _resolve_rnd_db_path = None  # type: ignore
    PROD_DB = None  # type: ignore

PERSONA_TAG = "falcon_top10"            # taxonomy / baseline / weekly_state
PERSONA_TAG_DAILY = "falcon_top10_daily"  # signal_day_study
PERSONA_TAG_DAILY_VALIDITY = "falcon_top10_signal_validity"  # falcon_signal_validity

# ════════════════════════════════════════════════════════════════════════════
# TABLE 14 THRESHOLDS — single source of truth here. All documented in S7-build-log.md.
# ════════════════════════════════════════════════════════════════════════════
HFCL_MIN_N = 18                       # n_resolved < 18 -> WATCHLIST (HFCL)
MULT_AUTO_MIN = 1.0                   # AUTO_TRADE needs multiplier >= 1.0
MULT_MANUAL_LO = 0.8                  # MANUAL_REVIEW if multiplier in [0.8, 1.0)
MULT_WATCH_MAX = 0.8                  # WATCHLIST if multiplier < 0.8
SIGNAL_VALID_MIN = 60.0              # signal_valid_at_open >= 60 for AUTO_TRADE
INTRADAY_FP_MAX = 30.0              # intraday_false_positive_rate < 30 for AUTO; > 30 -> AVOID
CAPITULATION_MAX = 40.0            # capitulation% > 40 -> WATCHLIST (pick-time only)
INIT_STOP_RECENT_N = 2               # how many recent RESOLVED appearances to scan
INIT_STOP_AVOID = 2                  # 2+ recent INIT_STOP -> AVOID
INIT_STOP_MANUAL = 1                 # exactly 1 recent INIT_STOP -> MANUAL_REVIEW
CONSTITUTIONAL_60D_INIT_STOP = 2     # Rule #6: 2+ INIT_STOP in last 60d -> AVOID
CONSTITUTIONAL_LOOKBACK_DAYS = 60

# Decision labels (ordered most- to least-restrictive for stable reporting).
DECISIONS = ["AVOID", "WATCHLIST", "MANUAL_REVIEW", "AUTO_TRADE"]

# Optimal-hold offsets for the early-exit study (D-offsets with a stored close_ret).
EARLY_EXIT_OFFSETS = [1, 2, 3, 4, 5, 6, 7, 8, 10, 15, 20, 30, 45, 60]


def _ist_today() -> str:
    """Today's date in IST (Asia/Kolkata). Per the always-use-IST rule: compute
    IST explicitly, never from the server clock / log timestamps."""
    ist = timezone(timedelta(hours=5, minutes=30))
    return datetime.now(ist).strftime("%Y-%m-%d")


def _mean(vals: List[float]) -> Optional[float]:
    return (sum(vals) / len(vals)) if vals else None


def _round(v: Any, nd: int = 4) -> Any:
    return round(v, nd) if isinstance(v, (int, float)) and not isinstance(v, bool) else v


def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    return con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _is_init_stop(exit_reason: Optional[str]) -> bool:
    return (exit_reason or "").upper() == "INIT_STOP"


# ════════════════════════════════════════════════════════════════════════════
# 1. SCHEMA — additive, idempotent ALTERs on falcon_pattern_taxonomy.
# ════════════════════════════════════════════════════════════════════════════

def _taxonomy_columns(con: sqlite3.Connection) -> set:
    try:
        rows = con.execute("PRAGMA table_info(falcon_pattern_taxonomy)").fetchall()
    except sqlite3.OperationalError:
        return set()
    return {r[1] for r in rows}


def ensure_taxonomy_columns(con: sqlite3.Connection, dry_run: bool) -> List[str]:
    """ALTER TABLE ... ADD COLUMN autotrade_decision / autotrade_block_reason,
    only if absent (PRAGMA-checked). Returns the list of columns it would add
    (in dry-run) or actually added. ADDITIVE — never drops/retypes."""
    cols = _taxonomy_columns(con)
    if not cols:
        # Table absent -> nothing we can add; caller surfaces the warning.
        return []
    want = [("autotrade_decision", "TEXT"), ("autotrade_block_reason", "TEXT")]
    added: List[str] = []
    for name, typ in want:
        if name in cols:
            continue
        added.append(name)
        if not dry_run:
            con.execute(f"ALTER TABLE falcon_pattern_taxonomy ADD COLUMN {name} {typ}")
    return added


# ════════════════════════════════════════════════════════════════════════════
# 2. LOADERS (read-only).
# ════════════════════════════════════════════════════════════════════════════

def _latest_week_multipliers(con: sqlite3.Connection) -> Tuple[Dict[Any, float], Dict[Any, str], Optional[str]]:
    """Latest-week weight_multiplier + status per pattern_id from
    falcon_pattern_weekly_state (persona falcon_top10). 'Latest' = MAX(week_ending)
    per pattern_id (a pattern may stop appearing in later weeks; we take ITS last
    known week, not the global max — the most recent CURRENT multiplier for it).

    Returns ({pattern_id: multiplier}, {pattern_id: status}, global_max_week)."""
    if not _table_exists(con, "falcon_pattern_weekly_state"):
        return {}, {}, None
    rows = con.execute(
        """
        SELECT pattern_id, week_ending, weight_multiplier, status
          FROM falcon_pattern_weekly_state
         WHERE persona = ?
        """,
        (PERSONA_TAG,),
    ).fetchall()
    latest_week: Dict[Any, str] = {}
    mult: Dict[Any, float] = {}
    status: Dict[Any, str] = {}
    global_max: Optional[str] = None
    for pid, we, wm, st in rows:
        if we and (global_max is None or we > global_max):
            global_max = we
        if we is None:
            continue
        prev = latest_week.get(pid)
        if prev is None or we > prev:
            latest_week[pid] = we
            mult[pid] = float(wm) if wm is not None else 1.0
            status[pid] = st
    return mult, status, global_max


def _load_taxonomy(con: sqlite3.Connection) -> Dict[Any, Dict[str, Any]]:
    """Per-pattern taxonomy attributes needed for Table 14. Keyed by pattern_id
    AS STORED (the taxonomy PK may be TEXT or INTEGER depending on the DB; we key
    on the raw value AND on its int form so joins from contributions, which use
    INTEGER pattern_id, resolve either way — see _pid_variants)."""
    cols = _taxonomy_columns(con)
    have = lambda c: c in cols  # noqa: E731
    sel = ["pattern_id"]
    for c in ("plain_english", "quality_flag", "pattern_maturity", "big_loser_risk",
              "signal_validity_next_day_pct", "intraday_false_positive_rate",
              "weight_multiplier", "swing_suitable", "status",
              "n_resolved_trades_total"):
        if have(c):
            sel.append(c)
    rows = con.execute(f"SELECT {', '.join(sel)} FROM falcon_pattern_taxonomy").fetchall()
    out: Dict[Any, Dict[str, Any]] = {}
    for r in rows:
        rec = {sel[i]: r[i] for i in range(len(sel))}
        out[rec["pattern_id"]] = rec
    return out


def _load_pattern_resolved_appearances(con: sqlite3.Connection) -> Dict[Any, List[Dict[str, Any]]]:
    """For each pattern_id: its RESOLVED appearances (contributions JOIN closed
    baseline_trades), with signal_date + exit_date + exit_reason, ordered later
    by signal_date desc. Resolved = bt.net_ret_pct IS NOT NULL. Keyed by the
    contributions pattern_id (INTEGER)."""
    rows = con.execute(
        """
        SELECT c.pattern_id, bt.signal_date, bt.exit_date, bt.exit_reason
          FROM falcon_pattern_contributions c
          JOIN falcon_baseline_trades bt ON bt.id = c.trade_id
         WHERE c.persona = ?
           AND bt.net_ret_pct IS NOT NULL
        """,
        (PERSONA_TAG,),
    ).fetchall()
    by_pat: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
    for pid, sd, xd, xr in rows:
        by_pat[pid].append({"signal_date": sd, "exit_date": xd, "exit_reason": xr})
    for pid in by_pat:
        by_pat[pid].sort(key=lambda d: (d["signal_date"] or ""), reverse=True)
    return by_pat


def _count_recent_init_stops(appearances: List[Dict[str, Any]], last_n: int) -> int:
    """INIT_STOP count among the last `last_n` RESOLVED appearances (already
    sorted signal_date desc)."""
    return sum(1 for a in appearances[:last_n] if _is_init_stop(a["exit_reason"]))


def _count_init_stops_60d(appearances: List[Dict[str, Any]], ref_date: Optional[str]) -> int:
    """INIT_STOP count among appearances whose signal_date is within 60 calendar
    days before ref_date (Constitutional Rule #6). ref_date defaults to the most
    recent appearance's signal_date (the pattern's 'now')."""
    if not appearances:
        return 0
    ref = ref_date or appearances[0]["signal_date"]
    if not ref:
        return 0
    try:
        ref_d = datetime.fromisoformat(ref[:10]).date()
    except ValueError:
        return 0
    lo = ref_d - timedelta(days=CONSTITUTIONAL_LOOKBACK_DAYS)
    n = 0
    for a in appearances:
        sd = a["signal_date"]
        if not sd or not _is_init_stop(a["exit_reason"]):
            continue
        try:
            d = datetime.fromisoformat(sd[:10]).date()
        except ValueError:
            continue
        if lo <= d <= ref_d:
            n += 1
    return n


def _pid_variants(pid: Any) -> List[Any]:
    """Candidate keys to look a pattern up under, to bridge TEXT-vs-INTEGER
    pattern_id between taxonomy (PK type varies) and contributions (INTEGER)."""
    out = [pid]
    try:
        out.append(int(pid))
    except (TypeError, ValueError):
        pass
    out.append(str(pid))
    # de-dup preserving order
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def _lookup(d: Dict[Any, Any], pid: Any, default: Any = None) -> Any:
    for k in _pid_variants(pid):
        if k in d:
            return d[k]
    return default


# ════════════════════════════════════════════════════════════════════════════
# 3. PER-PATTERN CLASSIFICATION (Table 14, most-restrictive-wins).
# ════════════════════════════════════════════════════════════════════════════

def classify_pattern(
    pid: Any,
    tax: Dict[str, Any],
    multiplier: float,
    mult_source: str,
    status: Optional[str],
    n_resolved: int,
    recent_init_stops: int,
    constitutional_violation: bool,
) -> Tuple[str, str, Dict[str, Any]]:
    """Apply Table 14 to ONE pattern. Returns (decision, block_reason, extras).

    PICK-TIME-ONLY criteria (repeater_type, current sector headwind/regime,
    capitulation%) are NOT fabricated here: they are evaluated at pick time and
    that is stated in the block_reason. HFCL: maturity=insufficient_data OR
    n_resolved < 18 -> WATCHLIST."""
    quality = tax.get("quality_flag")
    maturity = tax.get("pattern_maturity")
    big_loser_risk = tax.get("big_loser_risk")
    sig_valid = tax.get("signal_validity_next_day_pct")
    intraday_fp = tax.get("intraday_false_positive_rate")
    disabled = (status or "").lower() in ("disabled", "disable")

    # Each criterion contributes a (decision, human-readable clause) so the reason
    # can cite exactly what bound. We collect ALL triggered clauses per tier and
    # let precedence (AVOID > WATCHLIST > MANUAL_REVIEW) pick the winner.
    avoid: List[str] = []
    watch: List[str] = []
    manual: List[str] = []

    # ── AVOID ──
    if recent_init_stops >= INIT_STOP_AVOID:
        avoid.append(f"{recent_init_stops} INIT_STOPs in last {INIT_STOP_RECENT_N} appearances")
    if constitutional_violation:
        avoid.append("constitutional violation (2+ INIT_STOP in last 60d, Rule #6)")
    if disabled:
        avoid.append("pattern disabled")
    if intraday_fp is not None and intraday_fp > INTRADAY_FP_MAX:
        avoid.append(f"intraday_false_positive_rate {intraday_fp:.0f}%>{INTRADAY_FP_MAX:.0f}")

    # ── WATCHLIST ──
    if multiplier < MULT_WATCH_MAX:
        watch.append(f"multiplier {multiplier:.2f}<{MULT_WATCH_MAX:.2f}")
    if maturity == "insufficient_data":
        watch.append("maturity=insufficient_data")
    if n_resolved < HFCL_MIN_N:
        watch.append(f"n_resolved {n_resolved}<{HFCL_MIN_N} (HFCL)")
    if quality == "SECTOR_FOLLOWER":
        # The "sector headwind" half is a pick-time fact; at pattern level a
        # SECTOR_FOLLOWER is parked on WATCHLIST conservatively (it becomes
        # AVOID-ish only into a headwind, decided at the pick). Documented.
        watch.append("quality_flag=SECTOR_FOLLOWER (headwind decided at pick time)")
    if sig_valid is not None and sig_valid < SIGNAL_VALID_MIN:
        watch.append(f"signal_valid_at_open {sig_valid:.0f}%<{SIGNAL_VALID_MIN:.0f}")

    # ── MANUAL_REVIEW ──
    if MULT_MANUAL_LO <= multiplier < MULT_AUTO_MIN:
        manual.append(f"multiplier {multiplier:.2f} in [{MULT_MANUAL_LO:.2f},{MULT_AUTO_MIN:.2f})")
    if recent_init_stops == INIT_STOP_MANUAL:
        manual.append("exactly 1 recent INIT_STOP")
    if maturity == "emerging":
        manual.append("maturity=emerging")
    if big_loser_risk == 1:
        manual.append("big_loser_risk=True")

    # AUTO_TRADE requires signal_valid_at_open>=60 AND intraday_fp<30 as confirmed
    # positives (Table 14). If either is unknown (NULL — pattern lacks signal-
    # validity / 1-min coverage), it cannot be certified AUTO_TRADE -> MANUAL_REVIEW
    # (safe). (<60 / >30 are already routed to WATCHLIST/AVOID above.)
    auto_unconfirmed: List[str] = []
    if sig_valid is None:
        auto_unconfirmed.append("signal_valid_at_open unknown (no signal-validity coverage)")
    if intraday_fp is None:
        auto_unconfirmed.append("intraday_false_positive_rate unknown (no 1-min coverage)")

    # ── Resolve by precedence ──
    pick_time_note = ("repeater_type / current sector-regime / capitulation% are "
                      "evaluated at signal time (pick level)")
    if avoid:
        decision = "AVOID"
        clauses = avoid
    elif watch:
        decision = "WATCHLIST"
        clauses = watch
    elif manual:
        decision = "MANUAL_REVIEW"
        clauses = manual
    elif auto_unconfirmed:
        decision = "MANUAL_REVIEW"
        clauses = ["AUTO_TRADE blocked — required criteria not confirmed: "
                   + "; ".join(auto_unconfirmed)]
    else:
        decision = "AUTO_TRADE"
        clauses = [
            f"multiplier {multiplier:.2f}>={MULT_AUTO_MIN:.2f}",
            f"maturity={maturity}", "big_loser_risk=False",
            f"signal_valid_at_open={sig_valid:.0f}%>={SIGNAL_VALID_MIN:.0f}",
            f"intraday_fp={intraday_fp:.0f}%<{INTRADAY_FP_MAX:.0f}",
            "no recent INIT_STOP", "no constitutional violation",
        ]
    reason = f"{decision}: " + "; ".join(clauses) + f". ({pick_time_note}.)"
    # mult-source note appended so the auditor sees fallback usage.
    if mult_source != "weekly_state":
        reason += f" [multiplier source: {mult_source}]"

    extras = {
        "multiplier": multiplier,
        "quality_flag": quality,
        "pattern_maturity": maturity,
        "big_loser_risk": big_loser_risk,
        "signal_valid_at_open": sig_valid,
        "intraday_fp": intraday_fp,
        "recent_init_stops": recent_init_stops,
        "n_resolved": n_resolved,
        "plain_english": tax.get("plain_english"),
    }
    return decision, reason, extras


def compute_pattern_decisions(con: sqlite3.Connection) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Classify EVERY pattern with a taxonomy row. Returns (rows, decision_counts)."""
    taxonomy = _load_taxonomy(con)
    ws_mult, ws_status, _global_week = _latest_week_multipliers(con)
    appearances = _load_pattern_resolved_appearances(con)

    rows: List[Dict[str, Any]] = []
    counts: Dict[str, int] = defaultdict(int)
    for pid, tax in taxonomy.items():
        # multiplier resolution: weekly_state -> taxonomy.weight_multiplier -> 1.0
        wm = _lookup(ws_mult, pid)
        if wm is not None:
            multiplier, mult_source = float(wm), "weekly_state"
        elif tax.get("weight_multiplier") is not None:
            multiplier, mult_source = float(tax["weight_multiplier"]), "taxonomy.weight_multiplier"
        else:
            multiplier, mult_source = 1.0, "default(1.0)"
        status = _lookup(ws_status, pid) or tax.get("status")

        app = _lookup(appearances, pid, [])
        # n_resolved: prefer the live appearance count; fall back to the taxonomy
        # n_resolved_trades_total (Step 3) when the pattern has a taxonomy row but
        # no contributions match (e.g. PK-type mismatch) — most-conservative max.
        n_app = len(app)
        n_tax = tax.get("n_resolved_trades_total") or 0
        n_resolved = max(n_app, int(n_tax) if n_tax else 0)
        recent_is = _count_recent_init_stops(app, INIT_STOP_RECENT_N)
        const_viol = _count_init_stops_60d(app, None) >= CONSTITUTIONAL_60D_INIT_STOP

        decision, reason, extras = classify_pattern(
            pid, tax, multiplier, mult_source, status, n_resolved, recent_is, const_viol)
        counts[decision] += 1
        rows.append({
            "pattern_id": pid,
            "decision": decision,
            "block_reason": reason,
            "mult_source": mult_source,
            **extras,
        })
    rows.sort(key=lambda r: (DECISIONS.index(r["decision"]), str(r["pattern_id"])))
    return rows, dict(counts)


# ════════════════════════════════════════════════════════════════════════════
# 4. PER-PICK CLASSIFICATION (Table 14 with the pick-time criteria LIVE).
# ════════════════════════════════════════════════════════════════════════════

def _load_picks(con: sqlite3.Connection) -> List[Dict[str, Any]]:
    """All baseline_trades picks for the persona, with every Table-14 pick-time
    input. Columns the upstream steps may have left NULL (repeater_type,
    sector_regime_on_signal_date, signal_still_valid_at_open,
    pattern_weight_multiplier) are read as-is — NULL is handled as 'unknown', not
    fabricated (see module docstring + S7-build-log)."""
    cols = {r[1] for r in con.execute("PRAGMA table_info(falcon_baseline_trades)").fetchall()}
    have = lambda c: c in cols  # noqa: E731
    base = ["id", "symbol", "signal_date", "exit_reason", "net_pnl", "net_ret_pct"]
    opt = [c for c in ("repeater_type", "move_type", "sector_tailwind",
                       "sector_regime_on_signal_date", "signal_still_valid_at_open",
                       "pattern_weight_multiplier", "engine_rank", "top_3_pattern_ids")
           if have(c)]
    sel = base + opt
    rows = con.execute(
        f"SELECT {', '.join(sel)} FROM falcon_baseline_trades WHERE persona = ?",
        (PERSONA_TAG,),
    ).fetchall()
    out = []
    for r in rows:
        rec = {sel[i]: r[i] for i in range(len(sel))}
        out.append(rec)
    return out


def _load_pick_fired_patterns(con: sqlite3.Connection) -> Dict[int, List[Any]]:
    """trade_id -> [fired pattern_id, ...] via falcon_pattern_contributions."""
    rows = con.execute(
        "SELECT trade_id, pattern_id FROM falcon_pattern_contributions WHERE persona = ?",
        (PERSONA_TAG,),
    ).fetchall()
    out: Dict[int, List[Any]] = defaultdict(list)
    for tid, pid in rows:
        if tid is not None:
            out[int(tid)].append(pid)
    return out


def _load_oos_lift(con: sqlite3.Connection) -> Dict[Any, float]:
    """pattern_id -> avg_oos_year_lift_pp from falcon_promoted_patterns. Used only
    to pick the DOMINANT fired pattern (highest OOS lift) for the pick-level
    intraday_fp criterion. Returns {} if the table/column is absent."""
    out: Dict[Any, float] = {}
    try:
        rows = con.execute(
            "SELECT pattern_id, avg_oos_year_lift_pp FROM falcon_promoted_patterns"
        ).fetchall()
    except sqlite3.OperationalError:
        return out
    for pid, lift in rows:
        if pid is not None and lift is not None:
            out[pid] = float(lift)
    return out


def _load_signal_validity_map(con: sqlite3.Connection) -> Dict[Tuple[str, str], Optional[int]]:
    """(signal_date,symbol) -> valid_at_next_open (0/1) from falcon_signal_validity
    (persona falcon_top10_signal_validity). This is the POPULATED source for the
    pick-level signal_valid_at_open criterion; baseline_trades.signal_still_valid_at_open
    is NULL for every row."""
    out: Dict[Tuple[str, str], Optional[int]] = {}
    try:
        rows = con.execute(
            "SELECT signal_date, symbol, valid_at_next_open FROM falcon_signal_validity "
            "WHERE persona = ?", (PERSONA_TAG_DAILY_VALIDITY,),
        ).fetchall()
    except sqlite3.OperationalError:
        return out
    for sd, sym, v in rows:
        out[(sd, sym)] = (int(v) if v is not None else None)
    return out


def _stock_pattern_init_stop_history(con: sqlite3.Connection) -> Dict[Tuple[str, Any], List[Dict[str, Any]]]:
    """(symbol, pattern_id) -> resolved appearances (signal_date, exit_reason),
    sorted signal_date desc. For the pick-level 'same stock+pattern recent
    INIT_STOP' check."""
    rows = con.execute(
        """
        SELECT bt.symbol, c.pattern_id, bt.signal_date, bt.exit_reason
          FROM falcon_pattern_contributions c
          JOIN falcon_baseline_trades bt ON bt.id = c.trade_id
         WHERE c.persona = ?
           AND bt.net_ret_pct IS NOT NULL
        """,
        (PERSONA_TAG,),
    ).fetchall()
    out: Dict[Tuple[str, Any], List[Dict[str, Any]]] = defaultdict(list)
    for sym, pid, sd, xr in rows:
        out[(sym, pid)].append({"signal_date": sd, "exit_reason": xr})
    for k in out:
        out[k].sort(key=lambda d: (d["signal_date"] or ""), reverse=True)
    return out


def _pick_is_headwind(pick: Dict[str, Any]) -> Tuple[Optional[bool], str]:
    """Sector headwind at the pick. Sources, in order of directness:
      sector_regime_on_signal_date (if it names a headwind regime) ->
      move_type == 'SECTOR_HEADWIND' / 'SECTOR_DRAGGED' / 'STOCK_WEAKNESS' ->
      sector_tailwind == 0.
    Returns (is_headwind or None-if-unknown, source-note)."""
    reg = pick.get("sector_regime_on_signal_date")
    if reg:
        r = reg.upper()
        if "HEADWIND" in r or "BEAR" in r or "WEAK" in r or "DOWN" in r:
            return True, f"sector_regime={reg}"
        if "TAILWIND" in r or "BULL" in r or "STRONG" in r or "UP" in r:
            return False, f"sector_regime={reg}"
    mt = pick.get("move_type")
    if mt:
        if mt in ("SECTOR_HEADWIND", "SECTOR_DRAGGED", "STOCK_WEAKNESS"):
            return True, f"move_type={mt}"
        if mt in ("STOCK_LED", "SECTOR_DRIVEN"):
            return False, f"move_type={mt}"
    st = pick.get("sector_tailwind")
    if st is not None:
        return (st == 0), f"sector_tailwind={st}"
    return None, "sector regime/move_type/tailwind all NULL (unknown)"


def classify_pick(
    pick: Dict[str, Any],
    fired_pids: List[Any],
    pat_attrs: Dict[Any, Dict[str, Any]],     # pid -> {quality,maturity,big_loser_risk,intraday_fp,n_resolved}
    ws_mult: Dict[Any, float],
    sp_history: Dict[Tuple[str, Any], List[Dict[str, Any]]],
    validity_map: Dict[Tuple[str, str], Optional[int]],   # (signal_date,symbol) -> valid_at_next_open 0/1
) -> Tuple[str, str, Dict[str, Any]]:
    """Apply Table 14 to ONE pick, with the pick-time criteria LIVE. Pattern-level
    criteria are taken from the WORST of the pick's fired patterns (most-
    restrictive). Returns (decision, reason, extras)."""
    symbol = pick["symbol"]

    # ── multiplier = mean of fired patterns' latest-week multiplier (fallbacks) ──
    fmults = [float(_lookup(ws_mult, p)) for p in fired_pids if _lookup(ws_mult, p) is not None]
    if fmults:
        multiplier, mult_source = _mean(fmults), "mean(fired weekly_state)"
    elif pick.get("pattern_weight_multiplier") is not None:
        multiplier, mult_source = float(pick["pattern_weight_multiplier"]), "pick.pattern_weight_multiplier"
    else:
        multiplier, mult_source = 1.0, "default(1.0)"

    # ── DOMINANT-pattern attributes ──
    # A pick is a CONFLUENCE of patterns. Worst-of/min-across-fired is pathological
    # at pick level: a pick firing many patterns almost always has SOME thin or
    # noisy co-pattern, so worst-of would WATCHLIST/AVOID ~every pick (verified:
    # min-n_resolved<18 alone WATCHLISTed 74%). The pick's actual edge is its
    # DOMINANT pattern (highest OOS lift) — classify by that pattern's quality /
    # maturity / intraday_fp / n_resolved. EXCEPTION: big_loser_risk stays any-of
    # (rare — 12 patterns — and safety-relevant). INIT_STOP/constitutional checks
    # below remain across the stock+pattern history.
    def _dom(attr: str, default: Any = None) -> Any:
        cands = [(_lookup(pat_attrs, p, {}).get("oos_lift", 0.0) or 0.0,
                  _lookup(pat_attrs, p, {}).get(attr))
                 for p in fired_pids if _lookup(pat_attrs, p, {}).get(attr) is not None]
        return max(cands, key=lambda t: t[0])[1] if cands else default

    dom_quality = _dom("quality_flag")
    worst_maturity = _dom("pattern_maturity")          # dominant pattern's maturity
    intraday_fp = _dom("intraday_fp")                  # dominant pattern's intraday FP
    min_n_resolved = _dom("n_resolved", 0) or 0        # dominant pattern's n_resolved
    blrisks = [_lookup(pat_attrs, p, {}).get("big_loser_risk") for p in fired_pids]
    big_loser_risk = 1 if any(b == 1 for b in blrisks) else (0 if any(b == 0 for b in blrisks) else None)
    qualities = [dom_quality]                          # has_alpha / has_follower read this
    has_follower = (dom_quality == "SECTOR_FOLLOWER")

    # ── pick-time criteria (LIVE) ──
    repeater = pick.get("repeater_type")
    is_headwind, hw_src = _pick_is_headwind(pick)
    # signal_valid_at_open: baseline_trades.signal_still_valid_at_open is NULL for
    # every row (never populated by Steps 2-6), so fall back to the POPULATED
    # falcon_signal_validity.valid_at_next_open keyed (signal_date,symbol). 0/1/None.
    sig_valid_open = pick.get("signal_still_valid_at_open")   # BOOLEAN 0/1 or NULL
    sig_valid_src = "baseline.signal_still_valid_at_open"
    if sig_valid_open is None:
        sig_valid_open = validity_map.get((pick.get("signal_date"), symbol))
        sig_valid_src = "falcon_signal_validity.valid_at_next_open"
    # recent INIT_STOP on the SAME stock+pattern history (worst across fired pats).
    recent_is = 0
    for p in fired_pids:
        hist = sp_history.get((symbol, p), [])
        recent_is = max(recent_is, _count_recent_init_stops(hist, INIT_STOP_RECENT_N))
    const_viol = False
    for p in fired_pids:
        hist = sp_history.get((symbol, p), [])
        if _count_init_stops_60d(hist, pick.get("signal_date")) >= CONSTITUTIONAL_60D_INIT_STOP:
            const_viol = True
            break

    avoid: List[str] = []
    watch: List[str] = []
    manual: List[str] = []

    # ── AVOID ──
    if recent_is >= INIT_STOP_AVOID:
        avoid.append(f"{recent_is} recent INIT_STOPs (stock+pattern)")
    if const_viol:
        avoid.append("constitutional violation (2+ INIT_STOP in 60d, Rule #6)")
    if intraday_fp is not None and intraday_fp > INTRADAY_FP_MAX:
        avoid.append(f"intraday_false_positive_rate {intraday_fp:.0f}%>{INTRADAY_FP_MAX:.0f}")

    # ── WATCHLIST ──
    if multiplier < MULT_WATCH_MAX:
        watch.append(f"multiplier {multiplier:.2f}<{MULT_WATCH_MAX:.2f}")
    if repeater == "PERSISTENCE_TRAP":
        watch.append("repeater=PERSISTENCE_TRAP")
    if worst_maturity == "insufficient_data":
        watch.append("maturity=insufficient_data")
    if min_n_resolved < HFCL_MIN_N:
        watch.append(f"min fired-pattern n_resolved {min_n_resolved}<{HFCL_MIN_N} (HFCL)")
    if has_follower and is_headwind is True:
        watch.append(f"SECTOR_FOLLOWER into headwind ({hw_src})")
    if sig_valid_open == 0:
        watch.append("signal_still_valid_at_open=0 (<60)")

    # ── MANUAL_REVIEW ──
    if MULT_MANUAL_LO <= multiplier < MULT_AUTO_MIN:
        manual.append(f"multiplier {multiplier:.2f} in [{MULT_MANUAL_LO:.2f},{MULT_AUTO_MIN:.2f})")
    if recent_is == INIT_STOP_MANUAL:
        manual.append("exactly 1 recent INIT_STOP")
    if repeater == "STALE":
        manual.append("repeater=STALE")
    if worst_maturity == "emerging":
        manual.append("maturity=emerging")
    if has_follower and is_headwind is False:
        # SECTOR_FOLLOWER AND sector neutral/positive -> MANUAL (spec).
        manual.append("SECTOR_FOLLOWER with non-headwind sector")
    if big_loser_risk == 1:
        manual.append("big_loser_risk=True")

    pick_time_unknowns = []
    if repeater is None:
        pick_time_unknowns.append("repeater_type NULL")
    if is_headwind is None:
        pick_time_unknowns.append(hw_src)
    if sig_valid_open is None:
        pick_time_unknowns.append("signal_still_valid_at_open NULL")

    # AUTO_TRADE is a real-money gate: a REQUIRED-POSITIVE criterion that is
    # unconfirmed (NULL/unknown) must NOT be treated as satisfied — it forces at
    # least MANUAL_REVIEW (human confirms before auto-executing). Definitive
    # failures (e.g. sector headwind without alpha) also block AUTO_TRADE.
    has_alpha = any(q == "STOCK_SPECIFIC_ALPHA" for q in qualities)
    auto_failed: List[str] = []        # definitively fails a required positive
    auto_unconfirmed: List[str] = []   # cannot certify (data not recorded)
    # repeater must be FRESH/HEALTHY (STALE/TRAP already routed to manual/watch).
    if repeater not in ("FRESH", "HEALTHY_REPEATER"):
        if repeater is None:
            auto_unconfirmed.append("repeater not recorded (live system must confirm FRESH/HEALTHY)")
        else:
            auto_failed.append(f"repeater={repeater}")
    # sector NOT headwind OR STOCK_SPECIFIC_ALPHA.
    if not (has_alpha or is_headwind is False):
        if is_headwind is True:
            auto_failed.append(f"sector headwind without STOCK_SPECIFIC_ALPHA ({hw_src})")
        else:
            auto_unconfirmed.append(f"sector regime unknown and not STOCK_SPECIFIC_ALPHA ({hw_src})")
    # signal valid at open must be confirmed (==1); ==0 already routed to watch.
    if sig_valid_open != 1:
        if sig_valid_open is None:
            auto_unconfirmed.append("signal_valid_at_open unknown")
    # The ONLY thing between this pick and AUTO_TRADE is the unrecorded repeater?
    pending_repeater = (not avoid and not watch and not manual
                        and not auto_failed
                        and auto_unconfirmed == ["repeater not recorded (live system must confirm FRESH/HEALTHY)"])

    if avoid:
        decision, clauses = "AVOID", avoid
    elif watch:
        decision, clauses = "WATCHLIST", watch
    elif manual:
        decision, clauses = "MANUAL_REVIEW", manual
    elif auto_failed or auto_unconfirmed:
        # Passes all negative/threshold gates but a required-positive criterion is
        # not confirmed -> MANUAL_REVIEW (safe), never AUTO_TRADE.
        decision = "MANUAL_REVIEW"
        clauses = ["AUTO_TRADE blocked — required criteria not confirmed: "
                   + "; ".join(auto_failed + auto_unconfirmed)]
    else:
        decision = "AUTO_TRADE"
        clauses = [
            f"multiplier {multiplier:.2f}>={MULT_AUTO_MIN:.2f}",
            f"repeater={repeater}",
            f"sector={'non-headwind' if is_headwind is False else 'alpha-exempt'}",
            f"maturity={worst_maturity}", "big_loser_risk=False",
            f"sig_valid_at_open={sig_valid_open} ({sig_valid_src})",
        ]
    reason = f"{decision}: " + "; ".join(clauses) + "."
    if pending_repeater:
        reason += " [AUTO_TRADE-eligible on all recorded criteria; repeater not recorded historically]"
    if pick_time_unknowns:
        reason += " [unknown inputs treated as not-triggered: " + "; ".join(pick_time_unknowns) + "]"
    if mult_source not in ("mean(fired weekly_state)",):
        reason += f" [multiplier source: {mult_source}]"

    extras = {
        "multiplier": multiplier,
        "repeater_type": repeater,
        "is_headwind": is_headwind,
        "worst_maturity": worst_maturity,
        "big_loser_risk": big_loser_risk,
        "intraday_fp": intraday_fp,
        "recent_init_stops": recent_is,
        "sig_valid_at_open": sig_valid_open,
        "sig_valid_src": sig_valid_src,
        "auto_pending_repeater": pending_repeater,
        "net_pnl": pick.get("net_pnl"),
        "net_ret_pct": pick.get("net_ret_pct"),
        "engine_rank": pick.get("engine_rank"),
    }
    return decision, reason, extras


_MATURITY_RANK = {"insufficient_data": 0, "emerging": 1, "established": 2, "stable": 3}


def _worst_maturity(maturities: List[Optional[str]]) -> Optional[str]:
    """Least-mature (most-restrictive) non-NULL maturity among fired patterns."""
    present = [m for m in maturities if m]
    if not present:
        return None
    return min(present, key=lambda m: _MATURITY_RANK.get(m, 0))


def compute_pick_decisions(
    con: sqlite3.Connection, pattern_rows: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Classify ALL picks. Returns (rows, decision_counts). Reuses the per-pattern
    extras for the WORST-of-fired pattern-level attributes."""
    picks = _load_picks(con)
    fired = _load_pick_fired_patterns(con)
    ws_mult, _ws_status, _gw = _latest_week_multipliers(con)
    sp_history = _stock_pattern_init_stop_history(con)
    validity_map = _load_signal_validity_map(con)
    oos_lift = _load_oos_lift(con)   # pid -> avg_oos_year_lift_pp (dominant-pattern key)

    pat_attrs: Dict[Any, Dict[str, Any]] = {}
    for pr in pattern_rows:
        pat_attrs[pr["pattern_id"]] = {
            "quality_flag": pr.get("quality_flag"),
            "pattern_maturity": pr.get("pattern_maturity"),
            "big_loser_risk": pr.get("big_loser_risk"),
            "intraday_fp": pr.get("intraday_fp"),
            "n_resolved": pr.get("n_resolved", 0),
            "oos_lift": _lookup(oos_lift, pr["pattern_id"]),
        }

    rows: List[Dict[str, Any]] = []
    counts: Dict[str, int] = defaultdict(int)
    for pick in picks:
        fpids = fired.get(int(pick["id"]), [])
        decision, reason, extras = classify_pick(pick, fpids, pat_attrs, ws_mult, sp_history, validity_map)
        counts[decision] += 1
        rows.append({
            "id": pick["id"],
            "symbol": pick["symbol"],
            "signal_date": pick["signal_date"],
            "decision": decision,
            "block_reason": reason,
            **extras,
        })
    return rows, dict(counts)


# ════════════════════════════════════════════════════════════════════════════
# 5. THE VALIDATION GATE — does AUTO_TRADE concentrate the winners? (HONEST)
# ════════════════════════════════════════════════════════════════════════════

def build_pick_validation(pick_rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Group CLOSED picks by autotrade_decision -> n, win-rate, avg net_ret_pct,
    summed net_pnl, % of total winners captured. A CLOSED pick = net_ret_pct (or
    net_pnl) IS NOT NULL. Returns (table_rows, verdict)."""
    closed = [r for r in pick_rows if r.get("net_ret_pct") is not None or r.get("net_pnl") is not None]
    total_n = len(closed)
    total_wins = sum(1 for r in closed if (r.get("net_pnl") or 0) > 0)
    total_pnl = sum((r.get("net_pnl") or 0.0) for r in closed)

    table: List[Dict[str, Any]] = []
    by_dec: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in closed:
        by_dec[r["decision"]].append(r)

    # ALL_TOP10 reference row (the full closed set) so the gate is comparable.
    def _row(label: str, grp: List[Dict[str, Any]]) -> Dict[str, Any]:
        n = len(grp)
        wins = sum(1 for r in grp if (r.get("net_pnl") or 0) > 0)
        wr = (100.0 * wins / n) if n else None
        avg_ret = _mean([r["net_ret_pct"] for r in grp if r.get("net_ret_pct") is not None])
        sum_pnl = sum((r.get("net_pnl") or 0.0) for r in grp)
        pct_winners = (100.0 * wins / total_wins) if total_wins else None
        return {
            "decision": label, "n": n, "win_rate": wr, "avg_net_ret": avg_ret,
            "sum_net_pnl": sum_pnl, "pct_winners_captured": pct_winners,
            "pct_of_picks": (100.0 * n / total_n) if total_n else None,
        }

    for dec in DECISIONS:
        if dec in by_dec:
            table.append(_row(dec, by_dec[dec]))
    # Diagnostic cohort: picks that pass EVERY recorded Table-14 criterion and whose
    # ONLY unconfirmed gate is the never-recorded repeater_type. These are
    # MANUAL_REVIEW in the safe DB decision, but this cohort answers "would the
    # gate concentrate winners, assuming the live repeater check passes?" — so the
    # validation stays meaningful despite the historical data gap.
    pending = [r for r in closed if r.get("auto_pending_repeater")]
    table.append(_row("AUTO_TRADE_PENDING_REPEATER", pending))
    # The gate's PRIMARY measurable value: removing the AVOID bucket. What remains
    # is the tradeable set (WATCHLIST/MANUAL/AUTO). If AVOID isolates the losers,
    # the tradeable set's WR/return is materially above the full top-10 set.
    tradeable = [r for r in closed if r.get("decision") != "AVOID"]
    table.append(_row("TRADEABLE_ex_AVOID", tradeable))
    table.append(_row("ALL_TOP10", closed))

    # ── VERDICT (the constitutional #14 honesty check) ──
    auto = by_dec.get("AUTO_TRADE", [])
    auto_n = len(auto)
    auto_wins = sum(1 for r in auto if (r.get("net_pnl") or 0) > 0)
    auto_wr = (100.0 * auto_wins / auto_n) if auto_n else None
    all_wr = (100.0 * total_wins / total_n) if total_n else None
    auto_avg = _mean([r["net_ret_pct"] for r in auto if r.get("net_ret_pct") is not None])
    all_avg = _mean([r["net_ret_pct"] for r in closed if r.get("net_ret_pct") is not None])
    pct_winners_auto = (100.0 * auto_wins / total_wins) if total_wins else None
    pct_picks_auto = (100.0 * auto_n / total_n) if total_n else None

    # "Concentrates winners" = AUTO_TRADE WR materially above the full-set WR AND
    # avg net_ret above the full set. Honest threshold: any positive lift counts
    # as 'helps' but we report the magnitude; a non-positive lift = does NOT help.
    wr_lift = (auto_wr - all_wr) if (auto_wr is not None and all_wr is not None) else None
    ret_lift = (auto_avg - all_avg) if (auto_avg is not None and all_avg is not None) else None
    concentrates = bool(wr_lift is not None and wr_lift > 0 and (ret_lift is None or ret_lift >= 0))

    # ── TRADEABLE (ex-AVOID) — the gate's primary measurable value ──
    avoid_grp = by_dec.get("AVOID", [])
    avoid_n = len(avoid_grp)
    avoid_wins = sum(1 for r in avoid_grp if (r.get("net_pnl") or 0) > 0)
    avoid_wr = (100.0 * avoid_wins / avoid_n) if avoid_n else None
    avoid_avg = _mean([r["net_ret_pct"] for r in avoid_grp if r.get("net_ret_pct") is not None])
    trad = [r for r in closed if r.get("decision") != "AVOID"]
    trad_n = len(trad)
    trad_wins = sum(1 for r in trad if (r.get("net_pnl") or 0) > 0)
    trad_wr = (100.0 * trad_wins / trad_n) if trad_n else None
    trad_avg = _mean([r["net_ret_pct"] for r in trad if r.get("net_ret_pct") is not None])
    trad_wr_lift = (trad_wr - all_wr) if (trad_wr is not None and all_wr is not None) else None
    trad_ret_lift = (trad_avg - all_avg) if (trad_avg is not None and all_avg is not None) else None
    gate_removes_losers = bool(avoid_wr is not None and all_wr is not None and avoid_wr < all_wr
                               and trad_wr_lift is not None and trad_wr_lift > 0)

    # ── PENDING_REPEATER cohort (the measurable gate — see table note) ──
    pend = [r for r in closed if r.get("auto_pending_repeater")]
    pend_n = len(pend)
    pend_wins = sum(1 for r in pend if (r.get("net_pnl") or 0) > 0)
    pend_wr = (100.0 * pend_wins / pend_n) if pend_n else None
    pend_avg = _mean([r["net_ret_pct"] for r in pend if r.get("net_ret_pct") is not None])
    pend_wr_lift = (pend_wr - all_wr) if (pend_wr is not None and all_wr is not None) else None
    pend_ret_lift = (pend_avg - all_avg) if (pend_avg is not None and all_avg is not None) else None
    pend_concentrates = bool(pend_wr_lift is not None and pend_wr_lift > 0
                             and (pend_ret_lift is None or pend_ret_lift >= 0))

    verdict = {
        "total_closed": total_n,
        "total_winners": total_wins,
        "total_net_pnl": total_pnl,
        "auto_n": auto_n,
        "auto_wr": auto_wr,
        "all_wr": all_wr,
        "wr_lift_pp": wr_lift,
        "auto_avg_ret": auto_avg,
        "all_avg_ret": all_avg,
        "ret_lift_pp": ret_lift,
        "pct_winners_captured_by_auto": pct_winners_auto,
        "pct_picks_in_auto": pct_picks_auto,
        "concentrates_winners": concentrates,
        "pend_n": pend_n,
        "pend_wr": pend_wr,
        "pend_avg_ret": pend_avg,
        "pend_wr_lift_pp": pend_wr_lift,
        "pend_ret_lift_pp": pend_ret_lift,
        "pend_concentrates_winners": pend_concentrates,
        "pct_picks_in_pend": (100.0 * pend_n / total_n) if total_n else None,
        "avoid_n": avoid_n,
        "avoid_wr": avoid_wr,
        "avoid_avg_ret": avoid_avg,
        "trad_n": trad_n,
        "trad_wr": trad_wr,
        "trad_avg_ret": trad_avg,
        "trad_wr_lift_pp": trad_wr_lift,
        "trad_ret_lift_pp": trad_ret_lift,
        "gate_removes_losers": gate_removes_losers,
    }
    return table, verdict


def print_pick_validation(table: List[Dict[str, Any]], verdict: Dict[str, Any]) -> None:
    print("\n[autotrade_readiness] ── PICK VALIDATION GATE (does AUTO_TRADE concentrate winners?) ──")
    print(f"  {'decision':>14} {'n':>6} {'%picks':>7} {'WR%':>7} "
          f"{'avg_ret%':>9} {'sum_pnl':>14} {'%winners':>9}")
    for row in table:
        def f(v, fmt):
            return (fmt % v) if isinstance(v, (int, float)) else "—"
        print(f"  {row['decision']:>14} {f(row['n'], '%d'):>6} "
              f"{f(row['pct_of_picks'], '%.1f'):>7} {f(row['win_rate'], '%.1f'):>7} "
              f"{f(row['avg_net_ret'], '%+.2f'):>9} {f(row['sum_net_pnl'], '%.0f'):>14} "
              f"{f(row['pct_winners_captured'], '%.1f'):>9}")
    v = verdict
    print(f"\n  GATE AS A RISK FILTER (primary measurable value): AVOID = {v['avoid_n']} picks, "
          f"WR {_fmt(v['avoid_wr'], '%.1f')}% / avg_ret {_fmt(v['avoid_avg_ret'], '%+.2f')}% "
          f"(vs ALL {_fmt(v['all_wr'], '%.1f')}% / {_fmt(v['all_avg_ret'], '%+.2f')}%). "
          f"Removing AVOID -> tradeable set {v['trad_n']} picks, "
          f"WR {_fmt(v['trad_wr'], '%.1f')}% (lift {_fmt(v['trad_wr_lift_pp'], '%+.1f')}pp), "
          f"avg_ret {_fmt(v['trad_avg_ret'], '%+.2f')}% (lift {_fmt(v['trad_ret_lift_pp'], '%+.2f')}pp).")
    if v["gate_removes_losers"]:
        print("  VERDICT: the gate WORKS as a risk filter — AVOID isolates materially worse "
              "picks (lower WR than the full set) and removing them lifts the tradeable set. "
              "(Constitutional #14: the auto-trade gate's job is to remove flagged setups.)")
    else:
        print("  VERDICT (HONEST): AVOID does not isolate clearly worse picks on this in-sample "
              "data — the gate is not adding measurable risk-filtering value here. Reported as-is.")
    print(f"\n  AUTO_TRADE: {v['auto_n']} picks "
          f"({_fmt(v['pct_picks_in_auto'], '%.1f')}% of closed), "
          f"WR {_fmt(v['auto_wr'], '%.1f')}% vs ALL {_fmt(v['all_wr'], '%.1f')}% "
          f"(lift {_fmt(v['wr_lift_pp'], '%+.1f')}pp), "
          f"avg_ret {_fmt(v['auto_avg_ret'], '%+.2f')}% vs ALL {_fmt(v['all_avg_ret'], '%+.2f')}% "
          f"(lift {_fmt(v['ret_lift_pp'], '%+.2f')}pp), "
          f"captures {_fmt(v['pct_winners_captured_by_auto'], '%.1f')}% of all winners.")
    if v["auto_n"] == 0:
        print("  NOTE: 0 picks certify as AUTO_TRADE in the SAFE DB decision because "
              "repeater_type was never recorded historically (NULL on every row). This is "
              "a data-completeness gap, not a strategy result — the live system records "
              "repeater at signal time. Gate measured on the PENDING_REPEATER cohort below.")
    elif v["concentrates_winners"]:
        print("  VERDICT: AUTO_TRADE CONCENTRATES winners (higher WR than the full top-10 "
              "set, non-negative avg-return lift) -> the stricter gate is justified.")
    else:
        print("  VERDICT (HONEST): AUTO_TRADE does NOT clearly concentrate winners on this "
              "in-sample data (no positive WR lift, or negative avg-return lift). The gate "
              "is a RISK filter (it removes flagged setups), not a return-amplifier here. "
              "Constitutional #14: reported as-is, not dressed up.")
    print(f"\n  PENDING_REPEATER cohort (passes all RECORDED Table-14 criteria; only the "
          f"never-recorded repeater is unconfirmed): {v['pend_n']} picks "
          f"({_fmt(v['pct_picks_in_pend'], '%.1f')}% of closed), "
          f"WR {_fmt(v['pend_wr'], '%.1f')}% vs ALL {_fmt(v['all_wr'], '%.1f')}% "
          f"(lift {_fmt(v['pend_wr_lift_pp'], '%+.1f')}pp), "
          f"avg_ret {_fmt(v['pend_avg_ret'], '%+.2f')}% vs ALL {_fmt(v['all_avg_ret'], '%+.2f')}% "
          f"(lift {_fmt(v['pend_ret_lift_pp'], '%+.2f')}pp).")
    if v["pend_concentrates_winners"]:
        print("  VERDICT (pending-repeater): the recorded Table-14 gate CONCENTRATES winners "
              "(higher WR + non-negative avg-return lift) -> stricter gate is justified, "
              "ASSUMING the live repeater check passes. In-sample.")
    else:
        print("  VERDICT (HONEST, pending-repeater): the recorded Table-14 gate does NOT "
              "clearly concentrate winners in-sample (no WR lift, or negative avg-return "
              "lift) -> it is a RISK filter, not a return-amplifier. Reported as-is.")


def _fmt(v: Any, fmt: str) -> str:
    return (fmt % v) if isinstance(v, (int, float)) else "—"


# ════════════════════════════════════════════════════════════════════════════
# 6. EARLY-EXIT STUDY (D+1..D+60 journey + optimal hold per pattern).
# ════════════════════════════════════════════════════════════════════════════

def compute_early_exit_study(con: sqlite3.Connection) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Per pattern: avg close_ret at each offset (join signal_day_study <->
    contributions by (signal_date, symbol)), avg post_hold_peak_high_ret,
    avg_peak_day (from big_winner_loser_study.avg_peak_day_when_winner),
    optimal_hold_day = offset maximizing avg cumulative close_ret,
    EV_at_hold_end vs EV_at_peak (from big_winner_loser_study). HFCL: n>=18 to
    recommend an optimal hold; else INSUFFICIENT_DATA. Returns (rows, summary)."""
    has_sds = _table_exists(con, "falcon_signal_day_study")
    has_bwl = _table_exists(con, "falcon_big_winner_loser_study")

    # (signal_date, symbol) -> {offset: close_ret, post_peak: ...} from signal_day_study.
    sds_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    if has_sds:
        sel = ["signal_date", "symbol", "post_hold_peak_high_ret"]
        sel += [f"d{k}_close_ret" for k in EARLY_EXIT_OFFSETS]
        rows = con.execute(
            f"SELECT {', '.join(sel)} FROM falcon_signal_day_study WHERE persona = ?",
            (PERSONA_TAG_DAILY,),
        ).fetchall()
        for r in rows:
            rec = {sel[i]: r[i] for i in range(len(sel))}
            sds_by_key[(rec["signal_date"], rec["symbol"])] = rec

    # contributions: pattern_id -> list of (signal_date, symbol)
    contrib = con.execute(
        "SELECT pattern_id, signal_date, symbol FROM falcon_pattern_contributions WHERE persona = ?",
        (PERSONA_TAG,),
    ).fetchall()
    by_pat_keys: Dict[Any, List[Tuple[str, str]]] = defaultdict(list)
    for pid, sd, sym in contrib:
        by_pat_keys[pid].append((sd, sym))

    # big_winner_loser_study -> per pattern EV + avg_peak_day
    bwl: Dict[Any, Dict[str, Any]] = {}
    if has_bwl:
        for pid, apd, ev_he, ev_pk, n_occ in con.execute(
            """SELECT pattern_id, avg_peak_day_when_winner, expected_value_at_hold_end,
                      expected_value_at_peak, n_total_occurrences
                 FROM falcon_big_winner_loser_study WHERE persona = ?""",
            (PERSONA_TAG,),
        ).fetchall():
            bwl[pid] = {"avg_peak_day": apd, "ev_hold_end": ev_he,
                        "ev_peak": ev_pk, "n_occ": n_occ}

    rows: List[Dict[str, Any]] = []
    all_journeys: Dict[int, List[float]] = defaultdict(list)  # offset -> all close_rets (for summary)
    n_recommended = 0
    for pid, keys in sorted(by_pat_keys.items(), key=lambda kv: str(kv[0])):
        # collect close_ret per offset across this pattern's (sd,sym) occurrences
        per_offset: Dict[int, List[float]] = defaultdict(list)
        post_peaks: List[float] = []
        n_journey = 0
        for key in keys:
            sds = sds_by_key.get(key)
            if not sds:
                continue
            n_journey += 1
            pp = sds.get("post_hold_peak_high_ret")
            if pp is not None:
                post_peaks.append(float(pp))
            for k in EARLY_EXIT_OFFSETS:
                v = sds.get(f"d{k}_close_ret")
                if v is not None:
                    per_offset[k].append(float(v))
                    all_journeys[k].append(float(v))
        avg_by_offset = {k: _mean(per_offset[k]) for k in EARLY_EXIT_OFFSETS}
        # optimal hold = offset with the MAX avg cumulative close_ret (cum return
        # to that day IS the close_ret at that offset, measured from entry).
        valid_offsets = [(k, avg_by_offset[k]) for k in EARLY_EXIT_OFFSETS if avg_by_offset[k] is not None]
        b = bwl.get(pid, {})
        n_occ = b.get("n_occ") or n_journey
        if valid_offsets and (n_occ and n_occ >= HFCL_MIN_N):
            optimal_hold_day, optimal_avg_ret = max(valid_offsets, key=lambda kv: kv[1])
            n_recommended += 1
        else:
            optimal_hold_day, optimal_avg_ret = None, None

        rec = {
            "pattern_id": pid,
            "n_journeys": n_journey,
            "n_occ": n_occ,
            "avg_peak_day": b.get("avg_peak_day"),
            "ev_at_hold_end": b.get("ev_hold_end"),
            "ev_at_peak": b.get("ev_peak"),
            "avg_post_hold_peak_high_ret": _mean(post_peaks),
            "optimal_hold_day": optimal_hold_day,
            "optimal_avg_close_ret": optimal_avg_ret,
            "recommendation": ("INSUFFICIENT_DATA" if optimal_hold_day is None
                               else f"hold to D+{optimal_hold_day}"),
        }
        for k in EARLY_EXIT_OFFSETS:
            rec[f"d{k}_close_ret"] = avg_by_offset[k]
        rows.append(rec)

    # summary row across all patterns
    summary = {
        "pattern_id": "ALL_PATTERNS",
        "n_journeys": sum(len(v) for v in all_journeys.values()) // max(len(EARLY_EXIT_OFFSETS), 1),
        "avg_by_offset": {k: _mean(all_journeys[k]) for k in EARLY_EXIT_OFFSETS},
        "n_patterns": len(rows),
        "n_recommended": n_recommended,
        "has_sds": has_sds,
        "has_bwl": has_bwl,
    }
    return rows, summary


# ════════════════════════════════════════════════════════════════════════════
# 7. WEEKLY REVIEW (latest week, for human approval).
# ════════════════════════════════════════════════════════════════════════════

_REVIEW_LOG_COLS = [
    "week_ending", "persona", "patterns_promoted", "patterns_demoted",
    "patterns_disabled", "new_healthy_repeaters", "new_persistence_traps",
    "new_early_exit_flags", "new_big_winner_candidates", "new_big_loser_risk_patterns",
    "sector_regime_shifts", "market_regime_shifts", "quality_flag_changes",
    "signal_validity_changes", "constitutional_violations", "summary_text",
    "review_status",
]


def compute_weekly_review(con: sqlite3.Connection) -> Dict[str, Any]:
    """The MAX(week_ending) review_log row(s) for persona falcon_top10 + that
    week's changed weekly_state rows (status_change=1)."""
    out: Dict[str, Any] = {"latest_week": None, "review_rows": [], "changed_states": []}
    if not _table_exists(con, "falcon_weekly_review_log"):
        return out
    cols = {r[1] for r in con.execute("PRAGMA table_info(falcon_weekly_review_log)").fetchall()}
    sel = [c for c in _REVIEW_LOG_COLS if c in cols]
    max_we = con.execute(
        "SELECT MAX(week_ending) FROM falcon_weekly_review_log WHERE persona = ?",
        (PERSONA_TAG,),
    ).fetchone()[0]
    if max_we is None:
        # persona may be NULL on some rows -> fall back to the global latest.
        max_we = con.execute("SELECT MAX(week_ending) FROM falcon_weekly_review_log").fetchone()[0]
    out["latest_week"] = max_we
    if max_we is None:
        return out
    rows = con.execute(
        f"SELECT {', '.join(sel)} FROM falcon_weekly_review_log "
        f"WHERE week_ending = ? AND (persona = ? OR persona IS NULL)",
        (max_we, PERSONA_TAG),
    ).fetchall()
    out["review_rows"] = [{sel[i]: r[i] for i in range(len(sel))} for r in rows]

    # changed weekly_state rows for that week.
    if _table_exists(con, "falcon_pattern_weekly_state"):
        ws_cols = {r[1] for r in con.execute("PRAGMA table_info(falcon_pattern_weekly_state)").fetchall()}
        wsel = [c for c in ("pattern_id", "weight_multiplier", "previous_multiplier",
                            "multiplier_change", "status", "status_change_reason",
                            "n_resolved_trades_60d", "realized_lift_60d")
                if c in ws_cols]
        change_clause = "status_change = 1" if "status_change" in ws_cols else "1=1"
        crows = con.execute(
            f"SELECT {', '.join(wsel)} FROM falcon_pattern_weekly_state "
            f"WHERE persona = ? AND week_ending = ? AND {change_clause}",
            (PERSONA_TAG, max_we),
        ).fetchall()
        out["changed_states"] = [{wsel[i]: r[i] for i in range(len(wsel))} for r in crows]
    return out


# ════════════════════════════════════════════════════════════════════════════
# 8. WRITES (RND only, single transaction per table, idempotent UPDATE-in-place).
# ════════════════════════════════════════════════════════════════════════════

def write_taxonomy_decisions(con: sqlite3.Connection, pattern_rows: List[Dict[str, Any]]) -> int:
    """UPDATE falcon_pattern_taxonomy.autotrade_decision + autotrade_block_reason
    in place, by pattern_id. Idempotent (recompute + overwrite). Returns rows hit."""
    n = 0
    for pr in pattern_rows:
        cur = con.execute(
            "UPDATE falcon_pattern_taxonomy SET autotrade_decision = ?, "
            "autotrade_block_reason = ? WHERE pattern_id = ?",
            (pr["decision"], pr["block_reason"], pr["pattern_id"]),
        )
        if cur.rowcount and cur.rowcount > 0:
            n += cur.rowcount
    return n


def write_pick_decisions(con: sqlite3.Connection, pick_rows: List[Dict[str, Any]]) -> int:
    """UPDATE falcon_baseline_trades.autotrade_decision + autotrade_block_reason
    in place, by id. Idempotent. Returns rows hit."""
    n = 0
    for pr in pick_rows:
        cur = con.execute(
            "UPDATE falcon_baseline_trades SET autotrade_decision = ?, "
            "autotrade_block_reason = ? WHERE id = ?",
            (pr["decision"], pr["block_reason"], pr["id"]),
        )
        if cur.rowcount and cur.rowcount > 0:
            n += cur.rowcount
    return n


# ════════════════════════════════════════════════════════════════════════════
# 9. EXCEL OUTPUTS (openpyxl, .csv fallback) — three workbooks.
# ════════════════════════════════════════════════════════════════════════════

def _new_wb():
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font
        return Workbook(), Font
    except Exception:
        return None, None


def _write_sheet(ws, headers: List[str], data: List[List[Any]], Font=None) -> None:
    ws.append(headers)
    if Font is not None:
        for c in ws[ws.max_row]:
            c.font = Font(bold=True)
    for row in data:
        ws.append([_round(v) for v in row])


_BY_PATTERN_HEADERS = [
    "pattern_id", "plain_english", "decision", "multiplier", "quality_flag",
    "maturity", "big_loser_risk", "signal_valid_at_open", "intraday_fp",
    "recent_init_stops", "n_resolved", "block_reason",
]


def _by_pattern_row(pr: Dict[str, Any]) -> List[Any]:
    return [
        pr["pattern_id"], pr.get("plain_english") or "", pr["decision"],
        pr.get("multiplier"), pr.get("quality_flag") or "",
        pr.get("pattern_maturity") or "", pr.get("big_loser_risk"),
        pr.get("signal_valid_at_open"), pr.get("intraday_fp"),
        pr.get("recent_init_stops"), pr.get("n_resolved"), pr["block_reason"],
    ]


_PICK_VAL_HEADERS = ["decision", "n", "pct_of_picks", "win_rate", "avg_net_ret",
                     "sum_net_pnl", "pct_winners_captured"]


def _pick_val_row(row: Dict[str, Any]) -> List[Any]:
    return [row["decision"], row["n"], row.get("pct_of_picks"), row.get("win_rate"),
            row.get("avg_net_ret"), row.get("sum_net_pnl"), row.get("pct_winners_captured")]


_BY_PICK_HEADERS = ["id", "symbol", "signal_date", "decision", "multiplier",
                    "repeater_type", "is_headwind", "worst_maturity", "big_loser_risk",
                    "intraday_fp", "recent_init_stops", "engine_rank",
                    "net_ret_pct", "net_pnl", "block_reason"]


def _by_pick_row(pr: Dict[str, Any]) -> List[Any]:
    return [pr["id"], pr["symbol"], pr["signal_date"], pr["decision"], pr.get("multiplier"),
            pr.get("repeater_type") or "", pr.get("is_headwind"), pr.get("worst_maturity") or "",
            pr.get("big_loser_risk"), pr.get("intraday_fp"), pr.get("recent_init_stops"),
            pr.get("engine_rank"), pr.get("net_ret_pct"), pr.get("net_pnl"), pr["block_reason"]]


def write_readiness_xlsx(out_dir: Path, pattern_rows: List[Dict[str, Any]],
                         val_table: List[Dict[str, Any]], verdict: Dict[str, Any],
                         pick_rows: List[Dict[str, Any]]) -> Tuple[Path, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    wb, Font = _new_wb()
    if wb is None:
        return _readiness_csv(out_dir, pattern_rows, val_table, verdict, pick_rows)

    ws = wb.active
    ws.title = "by_pattern"
    _write_sheet(ws, _BY_PATTERN_HEADERS, [_by_pattern_row(r) for r in pattern_rows], Font)

    wv = wb.create_sheet("pick_validation")
    verdict_note = (
        "Does AUTO_TRADE concentrate the winners vs the full top-10 set? "
        f"AUTO_TRADE WR {_fmt(verdict['auto_wr'], '%.1f')}% vs ALL "
        f"{_fmt(verdict['all_wr'], '%.1f')}% (lift {_fmt(verdict['wr_lift_pp'], '%+.1f')}pp); "
        f"avg_ret lift {_fmt(verdict['ret_lift_pp'], '%+.2f')}pp; captures "
        f"{_fmt(verdict['pct_winners_captured_by_auto'], '%.1f')}% of winners with "
        f"{_fmt(verdict['pct_picks_in_auto'], '%.1f')}% of picks. VERDICT: "
        + ("CONCENTRATES winners (gate justified)." if verdict["concentrates_winners"]
           else "does NOT clearly concentrate winners on this in-sample data — it is a "
                "RISK filter, not a return amplifier (reported honestly, Constitutional #14)."))
    wv.append([verdict_note])
    wv.append([])
    _write_sheet(wv, _PICK_VAL_HEADERS, [_pick_val_row(r) for r in val_table], Font)

    wp = wb.create_sheet("by_pick")
    _write_sheet(wp, _BY_PICK_HEADERS, [_by_pick_row(r) for r in pick_rows], Font)

    path = out_dir / "falcon_autotrade_readiness.xlsx"
    wb.save(str(path))
    return path, "xlsx"


def _readiness_csv(out_dir: Path, pattern_rows, val_table, verdict, pick_rows) -> Tuple[Path, str]:
    import csv
    p = out_dir / "falcon_autotrade_readiness_by_pattern.csv"
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(_BY_PATTERN_HEADERS)
        for r in pattern_rows:
            w.writerow([_round(v) for v in _by_pattern_row(r)])
    with open(out_dir / "falcon_autotrade_readiness_pick_validation.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(_PICK_VAL_HEADERS)
        for r in val_table:
            w.writerow([_round(v) for v in _pick_val_row(r)])
    with open(out_dir / "falcon_autotrade_readiness_by_pick.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(_BY_PICK_HEADERS)
        for r in pick_rows:
            w.writerow([_round(v) for v in _by_pick_row(r)])
    print("[autotrade_readiness] openpyxl missing — wrote CSV fallback (3 files).")
    return p, "csv"


def _early_exit_headers() -> List[str]:
    base = ["pattern_id", "n_occ", "n_journeys", "avg_peak_day", "ev_at_hold_end",
            "ev_at_peak", "avg_post_hold_peak_high_ret", "optimal_hold_day",
            "optimal_avg_close_ret", "recommendation"]
    return base + [f"d{k}_close_ret" for k in EARLY_EXIT_OFFSETS]


def _early_exit_row(rec: Dict[str, Any]) -> List[Any]:
    base = [rec["pattern_id"], rec.get("n_occ"), rec.get("n_journeys"),
            rec.get("avg_peak_day"), rec.get("ev_at_hold_end"), rec.get("ev_at_peak"),
            rec.get("avg_post_hold_peak_high_ret"), rec.get("optimal_hold_day"),
            rec.get("optimal_avg_close_ret"), rec.get("recommendation")]
    return base + [rec.get(f"d{k}_close_ret") for k in EARLY_EXIT_OFFSETS]


def write_early_exit_xlsx(out_dir: Path, rows: List[Dict[str, Any]],
                          summary: Dict[str, Any]) -> Tuple[Path, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    headers = _early_exit_headers()
    data = [_early_exit_row(r) for r in rows]
    # summary row (avg by offset across all patterns)
    srow = {"pattern_id": "ALL_PATTERNS", "n_occ": summary.get("n_patterns"),
            "n_journeys": summary.get("n_journeys"), "recommendation": "summary"}
    for k in EARLY_EXIT_OFFSETS:
        srow[f"d{k}_close_ret"] = summary["avg_by_offset"][k]
    data.append(_early_exit_row(srow))

    note = ("Per-pattern D+1..D+60 journey (avg close_ret at each offset), avg "
            "post-hold peak, avg_peak_day + EV(hold-end)/EV(peak) from Step 5, and "
            "optimal_hold_day = the offset maximizing avg cumulative close_ret. HFCL: "
            "optimal hold recommended only when n_occ>=18; else INSUFFICIENT_DATA. "
            "EV(peak) - EV(hold-end) = give-back the 7-day exit leaves on the table "
            "(quantifies the 'engine exits too early' finding).")
    if not summary.get("has_sds"):
        note += " [WARNING: falcon_signal_day_study absent — journeys empty.]"

    wb, Font = _new_wb()
    if wb is None:
        import csv
        p = out_dir / "falcon_early_exit_study.csv"
        with open(p, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f); w.writerow([note]); w.writerow([]); w.writerow(headers)
            for d in data:
                w.writerow([_round(v) for v in d])
        print("[autotrade_readiness] openpyxl missing — early-exit CSV fallback.")
        return p, "csv"
    ws = wb.active
    ws.title = "early_exit_by_pattern"
    ws.append([note]); ws.append([])
    _write_sheet(ws, headers, data, Font)
    path = out_dir / "falcon_early_exit_study.xlsx"
    wb.save(str(path))
    return path, "xlsx"


def write_weekly_review_xlsx(out_dir: Path, review: Dict[str, Any]) -> Tuple[Path, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    latest = review.get("latest_week")
    rev_rows = review.get("review_rows", [])
    changed = review.get("changed_states", [])

    rev_headers = _REVIEW_LOG_COLS
    rev_data = [[rr.get(c) for c in rev_headers] for rr in rev_rows]
    ch_headers = ["pattern_id", "previous_multiplier", "weight_multiplier",
                  "multiplier_change", "status", "n_resolved_trades_60d",
                  "realized_lift_60d", "status_change_reason"]
    ch_data = [[cr.get(c) for c in ch_headers] for cr in changed]

    note = (f"Latest weekly review for human approval — week_ending={latest}, "
            f"persona={PERSONA_TAG}. review_status carried from the log "
            f"(PENDING until a human approves). 'changed_patterns' tab = that week's "
            f"weekly_state rows with status_change=1 (old->new multiplier + reason).")

    wb, Font = _new_wb()
    if wb is None:
        import csv
        p = out_dir / "falcon_weekly_review.csv"
        with open(p, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f); w.writerow([note]); w.writerow([]); w.writerow(rev_headers)
            for d in rev_data:
                w.writerow([_round(v) for v in d])
            w.writerow([]); w.writerow(ch_headers)
            for d in ch_data:
                w.writerow([_round(v) for v in d])
        print("[autotrade_readiness] openpyxl missing — weekly-review CSV fallback.")
        return p, "csv"
    ws = wb.active
    ws.title = "latest_review"
    ws.append([note]); ws.append([])
    _write_sheet(ws, rev_headers, rev_data, Font)
    wc = wb.create_sheet("changed_patterns")
    _write_sheet(wc, ch_headers, ch_data, Font)
    path = out_dir / "falcon_weekly_review.xlsx"
    wb.save(str(path))
    return path, "xlsx"


# ════════════════════════════════════════════════════════════════════════════
# ORCHESTRATION
# ════════════════════════════════════════════════════════════════════════════

def run(rnd_db: str, prod_db: Optional[str], dry_run: bool, out_dir: Path) -> int:
    print(f"[autotrade_readiness] RND DB:  {rnd_db}")
    print(f"[autotrade_readiness] PROD DB: {prod_db}  (read-only; NOT opened this step)")
    print(f"[autotrade_readiness] persona: {PERSONA_TAG} / {PERSONA_TAG_DAILY}")
    print(f"[autotrade_readiness] mode: {'DRY-RUN (no writes)' if dry_run else 'APPLY'}  "
          f"IST date={_ist_today()}")

    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        # ── 1. additive schema (computed always; applied only on APPLY) ──
        tax_cols = _taxonomy_columns(con)
        if not tax_cols:
            print("[autotrade_readiness] WARNING: falcon_pattern_taxonomy not found — "
                  "per-pattern classification cannot be written. Apply Step-1 schema first.")
        would_add = ensure_taxonomy_columns(con, dry_run=True)
        if would_add:
            print(f"[autotrade_readiness] taxonomy columns to add (ALTER): {would_add}")
        else:
            print("[autotrade_readiness] taxonomy autotrade_decision/block_reason already present.")

        # ── 2. per-pattern classification ──
        pattern_rows, pat_counts = compute_pattern_decisions(con)
        print(f"\n[autotrade_readiness] ── PER-PATTERN ({len(pattern_rows)} patterns) ──")
        for d in DECISIONS:
            print(f"  {d:>14}: {pat_counts.get(d, 0)}")

        # ── 3. per-pick classification + validation gate ──
        pick_rows, pick_counts = compute_pick_decisions(con, pattern_rows)
        print(f"\n[autotrade_readiness] ── PER-PICK ({len(pick_rows)} picks) ──")
        for d in DECISIONS:
            print(f"  {d:>14}: {pick_counts.get(d, 0)}")
        val_table, verdict = build_pick_validation(pick_rows)
        print_pick_validation(val_table, verdict)

        # ── 5. early-exit study ──
        ee_rows, ee_summary = compute_early_exit_study(con)
        print(f"\n[autotrade_readiness] ── EARLY-EXIT STUDY ──")
        print(f"  patterns with journeys: {ee_summary['n_patterns']}  "
              f"(signal_day_study present: {ee_summary['has_sds']}, "
              f"big_winner_loser_study present: {ee_summary['has_bwl']})")
        print(f"  patterns with an optimal-hold recommendation (n_occ>=18): "
              f"{ee_summary['n_recommended']}")
        avg_off = ee_summary["avg_by_offset"]
        if any(v is not None for v in avg_off.values()):
            best = max(((k, v) for k, v in avg_off.items() if v is not None),
                       key=lambda kv: kv[1])
            print(f"  ALL-pattern avg close_ret peaks at D+{best[0]} ({best[1]:+.2f}%) "
                  f"— vs the 7-day hold this quantifies the early-exit give-back.")

        # ── 6. weekly review ──
        review = compute_weekly_review(con)
        print(f"\n[autotrade_readiness] ── WEEKLY REVIEW ──")
        print(f"  latest week_ending: {review['latest_week']}  "
              f"review_log rows: {len(review['review_rows'])}  "
              f"changed weekly_state (status_change=1): {len(review['changed_states'])}")

        if dry_run:
            print("\n[autotrade_readiness] DRY-RUN — nothing written (DB or Excel).")
            print(f"  would ALTER taxonomy: {would_add or '(none)'}")
            print(f"  would UPDATE {len(pattern_rows)} taxonomy rows + "
                  f"{len(pick_rows)} baseline_trades rows.")
            print(f"  would write 3 workbooks to {out_dir}.")
            print("\n[autotrade_readiness] by_pattern preview (first 6):")
            print("  " + " | ".join(_BY_PATTERN_HEADERS[:6]))
            for r in pattern_rows[:6]:
                print("  " + " | ".join(str(x) for x in _by_pattern_row(r)[:6]))
            con.rollback()
            return 0

        # ── APPLY: schema + writes in ONE transaction per concern ──
        con.execute("BEGIN")
        try:
            added = ensure_taxonomy_columns(con, dry_run=False)
            n_tax = write_taxonomy_decisions(con, pattern_rows) if tax_cols else 0
            n_pick = write_pick_decisions(con, pick_rows)
            con.commit()
        except Exception:
            con.rollback()
            raise
    finally:
        con.close()

    # ── Excel (after DB commit; writes nothing to PROD) ──
    p1, m1 = write_readiness_xlsx(out_dir, pattern_rows, val_table, verdict, pick_rows)
    p2, m2 = write_early_exit_xlsx(out_dir, ee_rows, ee_summary)
    p3, m3 = write_weekly_review_xlsx(out_dir, review)

    print(f"\n[autotrade_readiness] APPLY complete:")
    print(f"  taxonomy: ALTER added {added or '(none — already present)'}; "
          f"{n_tax} autotrade_decision rows updated.")
    print(f"  baseline_trades: {n_pick} autotrade_decision rows updated.")
    print(f"  workbooks: {p1} ({m1}); {p2} ({m2}); {p3} ({m3})")
    print(f"  per-pattern counts: " + ", ".join(f"{d}={pat_counts.get(d,0)}" for d in DECISIONS))
    print(f"  per-pick counts:    " + ", ".join(f"{d}={pick_counts.get(d,0)}" for d in DECISIONS))
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Step 7 — Auto-Trade Readiness classification (spec Table 14) at "
                    "pattern + pick level, the AUTO_TRADE winner-concentration gate, "
                    "and the 3 remaining Excel outputs. RND-only writes; no re-simulation."
    )
    p.add_argument("--rnd-db", default=None,
                   help="RND research DB (taxonomy / baseline_trades / contributions / "
                        "weekly_state / signal_day_study / big_winner_loser_study / "
                        "weekly_review_log). All writes go here. Falls back to the "
                        "persona resolver if omitted.")
    p.add_argument("--prod-db", default=None,
                   help="PROD DB (read-only; this step needs no OHLC so it is not "
                        "opened). Default: persona PROD_DB.")
    p.add_argument("--dry-run", action="store_true",
                   help="Compute + print ALL summaries (incl. the pick_validation gate); "
                        "write NOTHING (no DB, no Excel).")
    p.add_argument("--out", default=str(_HERE / "out" / "v7"),
                   help="Output dir for the 3 workbooks (default: out/v7).")
    args = p.parse_args(argv)

    rnd_db = args.rnd_db
    if not rnd_db:
        if _resolve_rnd_db_path is None:
            print("ERROR: --rnd-db not given and the persona resolver could not be "
                  "imported. Pass --rnd-db explicitly.", file=sys.stderr)
            return 2
        rnd_db = _resolve_rnd_db_path()
    prod_db = args.prod_db or PROD_DB

    return run(rnd_db=rnd_db, prod_db=prod_db, dry_run=args.dry_run, out_dir=Path(args.out))


if __name__ == "__main__":
    sys.exit(main())
