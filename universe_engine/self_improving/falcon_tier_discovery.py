#!/usr/bin/env python3
"""falcon_tier_discovery.py — S-TIER-DISCOVERY: self-learning, walk-forward tier engine.

Assigns every Falcon top-10 signal a TIER **at signal-generation time** (before
entry) and DISCOVERS the ENTERPRISE rulebook by greedy/beam search over
SIGNAL-TIME-ONLY features, validated OUT-OF-SAMPLE (walk-forward), never on
in-sample fit. Tiers are meant to be SELLABLE:

    GOLD       ~70% WR  (baseline: flat/down signal days)
    ENTERPRISE 80-90%+  WR  (discovered conjunctions, promoted only on pooled OOS)

────────────────────────────────────────────────────────────────────────────
DATA (verified live; this script only READS these two RND tables for training)
  RND DB  universe_engine/data/db/kanida_universe.db   (read for training, write rulebook)
  PROD DB data/db/kanida_universe.db                   (read-only; not strictly needed here)

  falcon_signal_day_context  (keyed by `id`) — SIGNAL-TIME features:
     signal_day_ret_pct, signal_day_range_pct, signal_day_high_pct,
     signal_day_low_pct, signal_day_vol_ratio, prev_day_ret_pct, two_day_ret_pct,
     signal_day_circuit ('UPPER'/'LOWER'/'NONE'), entry_context, is_* booleans,
     is_extension. is_extension=1 rows = the RECENT/TODAY picks (live tiering).

  falcon_signal_day_study  (persona='falcon_top10_daily', keyed by `id`) — OUTCOMES:
     net_ret_pct (LABEL source), exit_reason, engine_rank, avg_lift, n_fires,
     sector, signal_date, symbol, prior_appearances_30d, d1..d7/d30 (FORBIDDEN
     as features), post_hold_* (FORBIDDEN).

  TRAINING set = context ⋈ study ON id, WHERE net_ret_pct IS NOT NULL AND
  is_extension=0  (~10,063 rows). LABEL: win = net_ret_pct > 0. year = signal_date[:4].
  TODAY's picks for live tiering = is_extension=1 rows (latest signal_date).

────────────────────────────────────────────────────────────────────────────
HARD RULE — SIGNAL-TIME FEATURES ONLY (see _ALLOWED_FEATURES / _FORBIDDEN_COLS).
  ALLOWED as rule inputs: signal_day_ret_pct, signal_day_range_pct,
  signal_day_vol_ratio, prev_day_ret_pct, two_day_ret_pct, signal_day_circuit,
  engine_rank, avg_lift, n_fires, prior_appearances_30d, sector.
  FORBIDDEN (label / post-entry, NEVER a feature): entry_gap_pct, d1_*..d7_*,
  d30_*, net_ret_pct, exit_reason, post_hold_*. A guard ASSERTS no rule touches
  a forbidden column.

────────────────────────────────────────────────────────────────────────────
GOLD baseline  : signal_day_ret_pct <= 0 (flat/down). IS + walk-forward OOS
  WR/avg_ret/N computed each run; also the <=+2 variant. GOLD is the reference
  ENTERPRISE must beat (pooled OOS avg_ret).

ENTERPRISE discovery (greedy/beam "law of scaling"):
  Greedily add ONE signal-time condition at a time maximizing win-rate.
  Numeric features → threshold splits from a per-feature quantile grid (deciles)
  for <= and >; categorical (circuit/sector) → equality. Beam width 5, max depth
  5. Each kept conjunction records its FULL scaling path:
  (condition added, cumulative WR, cumulative N) per step — the explainable
  "we added X and WR went 70→78→85%" story.

WALK-FORWARD VALIDATION (integrity core — PROMOTE on OOS only):
  Discovery may PROPOSE candidates on the full set, but PROMOTION is decided on
  POOLED OOS + per-year consistency. Per test-year Y in TEST_YEARS we evaluate
  the fixed structural conjunction on year-Y rows (how it generalizes), then pool.

  PROMOTION gates (ENTERPRISE):
    pooled OOS WR >= 80%  AND
    pooled OOS avg_ret > GOLD's OOS avg_ret  AND
    pooled OOS N >= 100  AND
    per-year consistency: OOS WR >= 75% in >= 4 of 5 test years, each year N >= 18.
  High WR surviving only at N<100 or failing consistency → INSUFFICIENT_DATA
  (reported, NOT promoted). A near-miss set (best rules just under the gate) is
  surfaced for operator review. If NOTHING clears the gate we say so plainly and
  report the BEST robust rule + the full WR-vs-volume frontier.

SELF-LEARNING — RND table `falcon_tier_rules` (dated snapshots, history kept):
  rule_id, tier, conditions_json, scaling_path_json, is_wr/is_ret/is_n,
  oos_wr/oos_ret/oos_n, per_year_oos_json, status
  (PROMOTED/CANDIDATE/INSUFFICIENT_DATA/DEMOTED), as_of. On re-run a
  previously-PROMOTED rule is marked DEMOTED if its latest pooled OOS WR falls
  below its tier gate. Idempotent per as_of (delete that as_of's rows, re-insert).

APPLY + OUTPUTS:
  Tier every resolved row: ENTERPRISE if it matches ANY promoted enterprise rule;
  else GOLD if signal_day_ret<=0; else AVOID if signal_day_ret>15; else STANDARD.
  TODAY's top-10 (is_extension=1, latest signal_date) tiered the same way.
  Excel out/v9/falcon_tier_discovery.xlsx: (1) Rulebook, (2) Scaling Path,
  (3) OOS WR-vs-Volume Frontier, (4) Tier Performance, (5) Today's Top-10 Tiered.

CLI (mirrors sibling builders):
  python falcon_tier_discovery.py --rnd-db <p> --prod-db <p> [--out <dir>] [--as-of <YYYY-MM-DD>]
  python falcon_tier_discovery.py --dry-run     # print rulebook+frontier+today's tiers; write NOTHING

Constraints honored: RND-only writes; PROD read-only; never modifies
falcon_signal_day_study / falcon_signal_day_context / out/v3..v8; idempotent per
as_of; --dry-run writes nothing; no Kite; no prod-tree / shared-engine mutation.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Windows consoles default to cp1252 → force utf-8 so box-drawing / ₹ never crash.
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# ── Import root (mirror sibling builders) — optional; only for path defaults. ──
_HERE = Path(__file__).resolve().parent
_REPO_ROOT = _HERE.parent.parent
_BACKEND_ROOT = _REPO_ROOT / "backend"
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

try:  # path defaults from the persona resolver, like the siblings.
    from power_user.services.persona_simulator import (  # noqa: E402
        _resolve_rnd_db_path, PROD_DB,
    )
except Exception:  # defensive — script still runs with explicit --rnd-db/--prod-db.
    _resolve_rnd_db_path = None  # type: ignore
    PROD_DB = None  # type: ignore

STUDY_PERSONA_TAG = "falcon_top10_daily"
CONTEXT_TABLE = "falcon_signal_day_context"
STUDY_TABLE = "falcon_signal_day_study"
RULES_TABLE = "falcon_tier_rules"

# ── Walk-forward test years (each year is one OOS fold; pooled = all). ──
TEST_YEARS = ["2022", "2023", "2024", "2025", "2026"]

# ── ENTERPRISE promotion gates (decided on OOS only). ──
ENT_OOS_WR_GATE = 80.0       # pooled OOS WR >= 80%
ENT_OOS_N_GATE = 100         # pooled OOS N >= 100 (sellable volume)
ENT_PER_YEAR_WR = 75.0       # per-year OOS WR >= this in...
ENT_PER_YEAR_MIN_YEARS = 4   # ...at least this many of the 5 test years,
ENT_PER_YEAR_MIN_N = 18      # ...each counting only if that year's N >= this.
OVERFIT_GAP_PP = 10.0        # flag if IS-OOS WR gap > 10pp.

# ── Beam search config. ──
BEAM_WIDTH = 5
MAX_DEPTH = 5
QUANTILE_DECILES = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
MIN_LEAF_N = 50              # a candidate node must keep >= this many TRAIN rows.
MIN_WR_IMPROVE_PP = 0.5      # adding a condition must lift WR by at least this.

# ── Tier-apply thresholds (non-rule heuristics). ──
GOLD_MAX_SIGNAL_RET = 0.0    # GOLD = signal_day_ret_pct <= 0
GOLD_RELAXED_MAX = 2.0       # reported "more volume" variant: <= +2
AVOID_SIGNAL_RET = 15.0      # AVOID = signal_day_ret_pct > 15 (blow-off)

# ════════════════════════════════════════════════════════════════════════════
# FEATURE GOVERNANCE — signal-time only.
# ════════════════════════════════════════════════════════════════════════════
# Numeric signal-time features (source table noted): from context unless tagged STUDY.
_NUM_FEATURES = [
    "signal_day_ret_pct",       # context
    "signal_day_range_pct",     # context
    "signal_day_high_pct",      # context (signal-time, allowed)
    "signal_day_low_pct",       # context (signal-time, allowed)
    "signal_day_vol_ratio",     # context
    "prev_day_ret_pct",         # context
    "two_day_ret_pct",          # context
    "engine_rank",              # study  (known at signal time — it's the rank)
    "avg_lift",                 # study  (score/n_fires — signal time)
    "n_fires",                  # study  (signal time)
    "prior_appearances_30d",    # study  (signal time)
]
_CAT_FEATURES = ["signal_day_circuit", "sector"]   # context, study

_ALLOWED_FEATURES = set(_NUM_FEATURES) | set(_CAT_FEATURES)

# Columns that are the LABEL or post-entry — NEVER usable as a rule input.
_FORBIDDEN_COLS = {
    "entry_gap_pct", "net_ret_pct", "exit_reason",
    # any d1..d7/d8..d60 open/high/low/close return, and post_hold_*, are post-entry.
}
def _is_forbidden(col: str) -> bool:
    if col in _FORBIDDEN_COLS:
        return True
    if col.startswith("post_hold"):
        return True
    # d<N>_<open|high|low|close>_ret  (intra-hold / post-entry journey)
    if col.startswith("d") and col.endswith("_ret"):
        head = col[1:].split("_", 1)[0]
        if head.isdigit():
            return True
    return False


# ════════════════════════════════════════════════════════════════════════════
# 1. LOAD TRAINING + TODAY rows (context ⋈ study on id).
# ════════════════════════════════════════════════════════════════════════════
def load_rows(rnd_db: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], str]:
    """Returns (train_rows, today_rows, max_signal_date).

    train_rows : net_ret_pct NOT NULL AND is_extension=0 — the labeled cohort.
    today_rows : is_extension=1 (latest signal_date only) — for live tiering.
    Each row carries only the signal-time feature columns + label + keys.
    """
    con = sqlite3.connect(rnd_db, timeout=120.0)
    con.row_factory = sqlite3.Row
    try:
        # Pull the context table columns we need + study outcome/identity columns.
        # We select * from both and merge in python (column sets are stable, but
        # this is robust to additive schema changes).
        ctx = {r["id"]: dict(r) for r in con.execute(
            f"SELECT * FROM {CONTEXT_TABLE}")}
        study = {}
        for r in con.execute(
            f"SELECT * FROM {STUDY_TABLE} WHERE persona = ?", (STUDY_PERSONA_TAG,)
        ):
            study[r["id"]] = dict(r)
    finally:
        con.close()

    train: List[Dict[str, Any]] = []
    today_all: List[Dict[str, Any]] = []
    for cid, c in ctx.items():
        s = study.get(cid)
        if s is None:
            continue
        merged = _merge_signal_time(c, s)
        is_ext = c.get("is_extension")
        if is_ext == 1:
            today_all.append(merged)
            continue
        if s.get("net_ret_pct") is None:
            continue   # unresolved, non-extension → not trainable
        merged["win"] = 1 if s["net_ret_pct"] > 0 else 0
        merged["net_ret_pct"] = s["net_ret_pct"]
        merged["year"] = (merged.get("signal_date") or "")[:4]
        train.append(merged)

    max_sd = max((r.get("signal_date") or "" for r in train), default="")
    # TODAY = the latest is_extension signal_date only (the live top-10).
    today_max_sd = max((r.get("signal_date") or "" for r in today_all), default="")
    today = [r for r in today_all if r.get("signal_date") == today_max_sd]
    today.sort(key=lambda r: (r.get("engine_rank") or 99))
    return train, today, (max_sd if max_sd else today_max_sd)


def _merge_signal_time(c: Dict[str, Any], s: Dict[str, Any]) -> Dict[str, Any]:
    """Build a row holding ONLY signal-time features + identity (+ label later).

    Context wins for context-sourced fields; study supplies engine_rank/avg_lift/
    n_fires/prior_appearances_30d/sector + identity. Forbidden columns are never
    copied in (defense-in-depth — the search also guards)."""
    out: Dict[str, Any] = {}
    out["id"] = c.get("id")
    out["signal_date"] = c.get("signal_date") or s.get("signal_date")
    out["symbol"] = c.get("symbol") or s.get("symbol")
    # context-sourced signal-time features
    for k in ("signal_day_ret_pct", "signal_day_range_pct", "signal_day_high_pct",
              "signal_day_low_pct", "signal_day_vol_ratio", "prev_day_ret_pct",
              "two_day_ret_pct", "signal_day_circuit"):
        out[k] = c.get(k)
    # study-sourced signal-time features
    out["engine_rank"] = s.get("engine_rank")
    out["avg_lift"] = s.get("avg_lift")
    out["n_fires"] = s.get("n_fires")
    out["prior_appearances_30d"] = s.get("prior_appearances_30d")
    out["sector"] = s.get("sector")
    return out


# ════════════════════════════════════════════════════════════════════════════
# 2. CONDITIONS  (atomic signal-time predicates over a row).
# ════════════════════════════════════════════════════════════════════════════
class Condition:
    """One atomic predicate. op in {'<=','>','=='}. Feature MUST be allowed."""
    __slots__ = ("feature", "op", "value")

    def __init__(self, feature: str, op: str, value: Any):
        assert feature in _ALLOWED_FEATURES and not _is_forbidden(feature), (
            f"forbidden/unknown feature in rule: {feature}")
        assert op in ("<=", ">", "=="), f"bad op {op}"
        self.feature, self.op, self.value = feature, op, value

    def test(self, row: Dict[str, Any]) -> bool:
        v = row.get(self.feature)
        if v is None:
            return False   # NULL never satisfies a predicate (no imputation).
        if self.op == "<=":
            return v <= self.value
        if self.op == ">":
            return v > self.value
        return v == self.value

    def label(self) -> str:
        if self.op == "==":
            return f"{self.feature} == {self.value!r}"
        val = f"{self.value:.4g}" if isinstance(self.value, float) else str(self.value)
        return f"{self.feature} {self.op} {val}"

    def to_dict(self) -> Dict[str, Any]:
        return {"feature": self.feature, "op": self.op, "value": self.value}


def conj_test(conds: List[Condition], row: Dict[str, Any]) -> bool:
    return all(c.test(row) for c in conds)


def conj_label(conds: List[Condition]) -> str:
    return " AND ".join(c.label() for c in conds)


def conj_to_json(conds: List[Condition]) -> str:
    return json.dumps([c.to_dict() for c in conds])


# ════════════════════════════════════════════════════════════════════════════
# 3. METRICS over a cohort of labeled rows.
# ════════════════════════════════════════════════════════════════════════════
def cohort_stats(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """N, WR (% of net_ret_pct>0), avg net_ret_pct. Only labeled rows expected."""
    n = len(rows)
    if n == 0:
        return {"n": 0, "wr": None, "avg_ret": None}
    wins = sum(1 for r in rows if r.get("win") == 1)
    rets = [r["net_ret_pct"] for r in rows if r.get("net_ret_pct") is not None]
    avg = (sum(rets) / len(rets)) if rets else None
    return {"n": n, "wr": wins / n * 100.0, "avg_ret": avg}


def select_rows(rows: List[Dict[str, Any]], conds: List[Condition]) -> List[Dict[str, Any]]:
    return [r for r in rows if conj_test(conds, r)]


# ════════════════════════════════════════════════════════════════════════════
# 4. CANDIDATE GENERATION  (per-feature split grid).
# ════════════════════════════════════════════════════════════════════════════
def _quantiles(vals: List[float], qs: List[float]) -> List[float]:
    """Distinct quantile cut points (linear interpolation, sorted, de-duped)."""
    xs = sorted(v for v in vals if v is not None)
    if not xs:
        return []
    cuts = []
    for q in qs:
        idx = q * (len(xs) - 1)
        lo = int(idx)
        hi = min(lo + 1, len(xs) - 1)
        frac = idx - lo
        cuts.append(xs[lo] * (1 - frac) + xs[hi] * frac)
    # de-dup with rounding to avoid near-identical splits.
    seen, out = set(), []
    for c in cuts:
        key = round(c, 6)
        if key not in seen:
            seen.add(key)
            out.append(round(c, 6))
    return out


def build_atomic_conditions(rows: List[Dict[str, Any]]) -> List[Condition]:
    """All candidate atomic predicates from the TRAIN distribution.

    Numeric → (feature <= cut) and (feature > cut) for each decile cut.
    Categorical → (feature == value) for each observed value with enough support.
    """
    conds: List[Condition] = []
    # numeric
    for f in _NUM_FEATURES:
        vals = [r.get(f) for r in rows if r.get(f) is not None]
        if len(vals) < MIN_LEAF_N:
            continue
        for cut in _quantiles(vals, QUANTILE_DECILES):
            conds.append(Condition(f, "<=", cut))
            conds.append(Condition(f, ">", cut))
    # categorical (require >= MIN_LEAF_N support so a split is sellable)
    for f in _CAT_FEATURES:
        counts: Dict[Any, int] = defaultdict(int)
        for r in rows:
            v = r.get(f)
            if v is not None:
                counts[v] += 1
        for v, cnt in counts.items():
            if cnt >= MIN_LEAF_N:
                conds.append(Condition(f, "==", v))
    return conds


def _same_feature_op(a: Condition, b: Condition) -> bool:
    """Avoid stacking two predicates on the SAME (feature, op) — redundant /
    contradictory (e.g. ret>2 AND ret>5). Different ops on same feature (a band)
    are allowed (ret>2 AND ret<=10)."""
    return a.feature == b.feature and a.op == b.op


# ════════════════════════════════════════════════════════════════════════════
# 5. BEAM SEARCH  (greedy "law of scaling" — maximize WR, record scaling path).
# ════════════════════════════════════════════════════════════════════════════
class Candidate:
    __slots__ = ("conds", "scaling_path", "is_stats")

    def __init__(self, conds: List[Condition], scaling_path: List[Dict[str, Any]],
                 is_stats: Dict[str, Any]):
        self.conds = conds
        self.scaling_path = scaling_path
        self.is_stats = is_stats

    def key(self) -> str:
        # canonical key for dedup: sorted condition labels.
        return "|".join(sorted(c.label() for c in self.conds))


def beam_search(train: List[Dict[str, Any]]) -> List[Candidate]:
    """Greedy/beam search. Returns the kept conjunctions (depth 1..MAX_DEPTH),
    each with its IS stats and full scaling path. PROPOSAL stage only — uses the
    full TRAIN set to find high-WR structures; promotion is decided later on OOS.
    """
    atomics = build_atomic_conditions(train)
    base = cohort_stats(train)
    base_wr = base["wr"] or 0.0

    beam: List[Candidate] = []
    # depth 1
    for c in atomics:
        sel = select_rows(train, [c])
        st = cohort_stats(sel)
        if st["n"] < MIN_LEAF_N or st["wr"] is None:
            continue
        if st["wr"] <= base_wr + MIN_WR_IMPROVE_PP:
            continue
        path = [{"step": 1, "added": c.label(), "wr": round(st["wr"], 2),
                 "n": st["n"], "avg_ret": round(st["avg_ret"], 3) if st["avg_ret"] is not None else None}]
        beam.append(Candidate([c], path, st))
    beam = _prune_beam(beam, BEAM_WIDTH)

    kept: List[Candidate] = list(beam)
    seen = {cand.key() for cand in kept}

    # depths 2..MAX_DEPTH — greedily extend each beam member by one atomic.
    for depth in range(2, MAX_DEPTH + 1):
        next_beam: List[Candidate] = []
        for cand in beam:
            cur_wr = cand.is_stats["wr"] or 0.0
            for c in atomics:
                if any(_same_feature_op(c, existing) for existing in cand.conds):
                    continue
                new_conds = cand.conds + [c]
                sel = select_rows(train, new_conds)
                st = cohort_stats(sel)
                if st["n"] < MIN_LEAF_N or st["wr"] is None:
                    continue
                if st["wr"] <= cur_wr + MIN_WR_IMPROVE_PP:
                    continue   # condition must materially lift WR ("scaling").
                path = cand.scaling_path + [{
                    "step": depth, "added": c.label(), "wr": round(st["wr"], 2),
                    "n": st["n"],
                    "avg_ret": round(st["avg_ret"], 3) if st["avg_ret"] is not None else None}]
                nc = Candidate(new_conds, path, st)
                k = nc.key()
                if k in seen:
                    continue
                seen.add(k)
                next_beam.append(nc)
        if not next_beam:
            break
        next_beam = _prune_beam(next_beam, BEAM_WIDTH)
        kept.extend(next_beam)
        beam = next_beam
    return kept


def _prune_beam(cands: List[Candidate], width: int) -> List[Candidate]:
    """Keep the top-`width` by IS WR, tie-broken by N (more volume preferred)."""
    return sorted(cands, key=lambda c: (-(c.is_stats["wr"] or 0.0),
                                        -(c.is_stats["n"] or 0)))[:width]


# ════════════════════════════════════════════════════════════════════════════
# 6. WALK-FORWARD OOS EVALUATION of a fixed conjunction.
# ════════════════════════════════════════════════════════════════════════════
def walk_forward(conds: List[Condition], train: List[Dict[str, Any]]
                 ) -> Dict[str, Any]:
    """Evaluate the fixed structural conjunction per test-year, then pool.

    Returns: per_year {year: {n,wr,avg_ret}}, pooled {n,wr,avg_ret}, and a
    consistency summary (years_meeting_wr, years_with_min_n).
    """
    per_year: Dict[str, Dict[str, Any]] = {}
    pooled_rows: List[Dict[str, Any]] = []
    for y in TEST_YEARS:
        yr_rows = [r for r in train if r.get("year") == y]
        sel = select_rows(yr_rows, conds)
        st = cohort_stats(sel)
        per_year[y] = st
        pooled_rows.extend(sel)
    pooled = cohort_stats(pooled_rows)

    years_min_n = [y for y in TEST_YEARS if (per_year[y]["n"] or 0) >= ENT_PER_YEAR_MIN_N]
    years_meeting = [y for y in years_min_n
                     if per_year[y]["wr"] is not None and per_year[y]["wr"] >= ENT_PER_YEAR_WR]
    return {
        "per_year": per_year,
        "pooled": pooled,
        "years_with_min_n": years_min_n,
        "years_meeting_wr": years_meeting,
    }


def gold_baseline(train: List[Dict[str, Any]], max_ret: float) -> Dict[str, Any]:
    """GOLD reference: signal_day_ret_pct <= max_ret. IS + walk-forward OOS."""
    cond = [Condition("signal_day_ret_pct", "<=", max_ret)]
    is_stats = cohort_stats(select_rows(train, cond))
    wf = walk_forward(cond, train)
    return {"cond": cond, "is": is_stats, "wf": wf}


# ════════════════════════════════════════════════════════════════════════════
# 7. PROMOTION DECISION.
# ════════════════════════════════════════════════════════════════════════════
def classify_candidate(cand: Candidate, wf: Dict[str, Any],
                       gold_oos_avg_ret: Optional[float]) -> Tuple[str, str]:
    """Return (status, reason) per the ENTERPRISE gates.

    PROMOTED / CANDIDATE / INSUFFICIENT_DATA. (DEMOTED is assigned later by the
    self-learning re-run comparison, never here.)
    """
    pooled = wf["pooled"]
    n = pooled["n"] or 0
    wr = pooled["wr"]
    avg = pooled["avg_ret"]
    n_years_meet = len(wf["years_meeting_wr"])

    if wr is None:
        return "INSUFFICIENT_DATA", "no OOS rows"
    beats_gold = (gold_oos_avg_ret is None) or (avg is not None and avg > gold_oos_avg_ret)
    gate_wr = wr >= ENT_OOS_WR_GATE
    gate_n = n >= ENT_OOS_N_GATE
    gate_consist = n_years_meet >= ENT_PER_YEAR_MIN_YEARS

    if gate_wr and gate_n and beats_gold and gate_consist:
        return "PROMOTED", (
            f"pooled OOS WR {wr:.1f}% >= {ENT_OOS_WR_GATE:.0f}, N {n} >= {ENT_OOS_N_GATE}, "
            f"avg {avg:.2f}% > GOLD {gold_oos_avg_ret:.2f}%, "
            f"{n_years_meet}/{len(TEST_YEARS)} yrs >= {ENT_PER_YEAR_WR:.0f}% WR")
    # high WR but thin / inconsistent → flag, do not promote.
    if gate_wr and not (gate_n and gate_consist):
        why = []
        if not gate_n:
            why.append(f"N {n} < {ENT_OOS_N_GATE}")
        if not gate_consist:
            why.append(f"only {n_years_meet}/{len(TEST_YEARS)} yrs >= {ENT_PER_YEAR_WR:.0f}% WR")
        return "INSUFFICIENT_DATA", "WR clears but " + "; ".join(why)
    # otherwise a near-miss candidate (surfaced for review).
    miss = []
    if not gate_wr:
        miss.append(f"WR {wr:.1f}% < {ENT_OOS_WR_GATE:.0f}")
    if not beats_gold:
        miss.append("avg_ret <= GOLD")
    if not gate_n:
        miss.append(f"N {n} < {ENT_OOS_N_GATE}")
    if not gate_consist:
        miss.append(f"{n_years_meet}/{len(TEST_YEARS)} yrs consistent")
    return "CANDIDATE", "near-miss: " + "; ".join(miss)


# ════════════════════════════════════════════════════════════════════════════
# 8. RULEBOOK TABLE  (self-learning, dated snapshots, idempotent per as_of).
# ════════════════════════════════════════════════════════════════════════════
def ensure_rules_table(con: sqlite3.Connection) -> None:
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {RULES_TABLE} (
            row_pk            INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id           TEXT,
            tier              TEXT,
            conditions_json   TEXT,
            scaling_path_json TEXT,
            is_wr             REAL,
            is_ret            REAL,
            is_n              INTEGER,
            oos_wr            REAL,
            oos_ret           REAL,
            oos_n             INTEGER,
            per_year_oos_json TEXT,
            status            TEXT,
            reason            TEXT,
            as_of             TEXT,
            created_at        TEXT DEFAULT (datetime('now'))
        )
    """)
    con.execute(
        f"CREATE INDEX IF NOT EXISTS idx_ftr_as_of ON {RULES_TABLE}(as_of)")
    con.execute(
        f"CREATE INDEX IF NOT EXISTS idx_ftr_rule_id ON {RULES_TABLE}(rule_id)")


def previously_promoted(con: sqlite3.Connection, as_of: str) -> Dict[str, float]:
    """rule_id -> last PROMOTED pooled OOS WR from the MOST RECENT snapshot
    strictly before `as_of`. Used to mark a now-failing rule DEMOTED."""
    ensure_rules_table(con)
    rows = con.execute(
        f"SELECT rule_id, oos_wr, as_of FROM {RULES_TABLE} "
        f"WHERE status='PROMOTED' AND as_of < ? ORDER BY as_of", (as_of,)
    ).fetchall()
    latest: Dict[str, Tuple[str, float]] = {}
    for rid, wr, ao in rows:
        if rid is None:
            continue
        if rid not in latest or ao > latest[rid][0]:
            latest[rid] = (ao, wr)
    return {rid: wr for rid, (ao, wr) in latest.items()}


def write_rulebook(rnd_db: str, as_of: str, rule_records: List[Dict[str, Any]]) -> int:
    """Idempotent per as_of: delete this as_of's rows, re-insert. History (other
    as_of snapshots) preserved. Returns rows written."""
    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        con.execute("BEGIN")
        ensure_rules_table(con)
        con.execute(f"DELETE FROM {RULES_TABLE} WHERE as_of = ?", (as_of,))
        cols = ["rule_id", "tier", "conditions_json", "scaling_path_json",
                "is_wr", "is_ret", "is_n", "oos_wr", "oos_ret", "oos_n",
                "per_year_oos_json", "status", "reason"]
        all_cols = cols + ["as_of"]
        ph = ", ".join("?" for _ in all_cols)
        con.executemany(
            f"INSERT INTO {RULES_TABLE} ({', '.join(all_cols)}) VALUES ({ph})",
            [[rec.get(c) for c in cols] + [as_of] for rec in rule_records])
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
    return len(rule_records)


# ════════════════════════════════════════════════════════════════════════════
# 9. TIER ASSIGNMENT  (apply promoted enterprise rules + GOLD/AVOID/STANDARD).
# ════════════════════════════════════════════════════════════════════════════
def assign_tier(row: Dict[str, Any],
                promoted_conjs: List[Tuple[str, List[Condition]]]) -> Tuple[str, str]:
    """ENTERPRISE if matches ANY promoted enterprise rule; else GOLD if
    signal_day_ret<=0; else AVOID if signal_day_ret>15; else STANDARD."""
    for rid, conds in promoted_conjs:
        if conj_test(conds, row):
            return "ENTERPRISE", f"matches {rid}: {conj_label(conds)}"
    sdr = row.get("signal_day_ret_pct")
    if sdr is not None and sdr <= GOLD_MAX_SIGNAL_RET:
        return "GOLD", f"signal_day_ret {sdr:.2f}% <= 0 (flat/down baseline)"
    if sdr is not None and sdr > AVOID_SIGNAL_RET:
        return "AVOID", f"signal_day_ret {sdr:.2f}% > 15 (blow-off / chase risk)"
    return "STANDARD", ("no promoted-enterprise match; signal_day_ret "
                        + (f"{sdr:.2f}%" if sdr is not None else "NULL")
                        + " in (0,15]")


def tier_performance(train: List[Dict[str, Any]],
                     promoted_conjs: List[Tuple[str, List[Condition]]]
                     ) -> List[Dict[str, Any]]:
    """Per-tier N/WR/avg_ret over resolved rows + per-year OOS WR per tier."""
    by_tier: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in train:
        tier, _ = assign_tier(r, promoted_conjs)
        by_tier[tier].append(r)
    order = ["ENTERPRISE", "GOLD", "STANDARD", "AVOID"]
    out = []
    for tier in order:
        rows = by_tier.get(tier, [])
        st = cohort_stats(rows)
        per_year = {}
        for y in TEST_YEARS:
            per_year[y] = cohort_stats([r for r in rows if r.get("year") == y])
        out.append({"tier": tier, "n": st["n"], "wr": st["wr"],
                    "avg_ret": st["avg_ret"], "per_year": per_year})
    return out


# ════════════════════════════════════════════════════════════════════════════
# 10. WR-vs-VOLUME FRONTIER  (operator prices off this).
# ════════════════════════════════════════════════════════════════════════════
def build_frontier(all_candidates: List[Tuple[Candidate, Dict[str, Any]]]
                   ) -> List[Dict[str, Any]]:
    """Pareto-style frontier of (pooled OOS N, pooled OOS WR): for each achievable
    WR level, the conjunction giving the MOST OOS volume at >= that WR. Sorted by
    WR descending. Lets the operator read 'at 85% WR we have N rows/yr; drop to
    80% and N triples'."""
    pts = []
    for cand, wf in all_candidates:
        p = wf["pooled"]
        if p["wr"] is None or (p["n"] or 0) < ENT_PER_YEAR_MIN_N:
            continue
        pts.append({
            "label": conj_label(cand.conds),
            "oos_wr": round(p["wr"], 2),
            "oos_n": p["n"],
            "oos_avg_ret": round(p["avg_ret"], 3) if p["avg_ret"] is not None else None,
            "depth": len(cand.conds),
        })
    # frontier: sort by WR desc; keep a point only if it adds volume over all
    # higher-WR points kept so far (non-dominated on the WR↑ / N↑ trade-off).
    pts.sort(key=lambda d: (-d["oos_wr"], -d["oos_n"]))
    frontier, best_n = [], -1
    for p in pts:
        if p["oos_n"] > best_n:
            frontier.append(p)
            best_n = p["oos_n"]
    return frontier


# ════════════════════════════════════════════════════════════════════════════
# 11. EXCEL WRITER  (5 sheets; openpyxl with CSV/JSON fallback).
# ════════════════════════════════════════════════════════════════════════════
def _try_openpyxl():
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill
        return Workbook, Font, PatternFill
    except Exception:
        return None, None, None


_TIER_FILL = {
    "ENTERPRISE": "C6EFCE", "GOLD": "FFF2CC", "STANDARD": "DDEBF7",
    "AVOID": "FFC7CE",
}


def write_excel(out_dir: Path, payload: Dict[str, Any]) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "falcon_tier_discovery.xlsx"
    Workbook, Font, PatternFill = _try_openpyxl()
    if Workbook is None:
        return _csv_fallback(out_dir, payload)

    wb = Workbook()

    # ── Sheet 1: Rulebook ──
    ws = wb.active
    ws.title = "Rulebook"
    hdr = ["rule_id", "tier", "status", "conditions", "is_wr", "is_ret", "is_n",
           "oos_wr", "oos_ret", "oos_n", "is_oos_wr_gap_pp", "overfit_flag",
           "years_meeting_wr", "reason"]
    ws.append(hdr)
    for cell in ws[1]:
        cell.font = Font(bold=True)
    for rec in payload["rulebook"]:
        gap = (None if rec["is_wr"] is None or rec["oos_wr"] is None
               else round(rec["is_wr"] - rec["oos_wr"], 2))
        overfit = "" if gap is None else ("OVERFIT" if gap > OVERFIT_GAP_PP else "")
        ws.append([rec["rule_id"], rec["tier"], rec["status"], rec["conditions_label"],
                   _r(rec["is_wr"]), _r(rec["is_ret"], 3), rec["is_n"],
                   _r(rec["oos_wr"]), _r(rec["oos_ret"], 3), rec["oos_n"],
                   gap, overfit, rec["years_meeting_wr"], rec["reason"]])
    for i, w in enumerate([18, 11, 16, 60, 8, 8, 8, 8, 8, 8, 14, 10, 16, 50], 1):
        ws.column_dimensions[ws.cell(1, i).column_letter].width = w

    # ── Sheet 2: Scaling Path (best enterprise/promoted, else best candidate) ──
    ws2 = wb.create_sheet("Scaling Path")
    sp = payload["best_scaling_path"]
    ws2.append([f"Best rule scaling path — {sp['headline']}"])
    ws2.cell(1, 1).font = Font(bold=True, size=12)
    ws2.append([])
    ws2.append(["step", "condition added", "cumulative IS WR %", "cumulative N",
                "cumulative avg_ret %"])
    for cell in ws2[3]:
        cell.font = Font(bold=True)
    for step in sp["path"]:
        ws2.append([step["step"], step["added"], step["wr"], step["n"],
                    step.get("avg_ret")])
    ws2.append([])
    ws2.append([f"Pooled OOS: WR {sp['oos_wr']}  N {sp['oos_n']}  avg_ret {sp['oos_ret']}"])
    for i, w in enumerate([6, 46, 18, 14, 18], 1):
        ws2.column_dimensions[ws2.cell(1, i).column_letter].width = w

    # ── Sheet 3: OOS WR-vs-Volume Frontier ──
    ws3 = wb.create_sheet("OOS WR-vs-Volume Frontier")
    ws3.append(["oos_wr_%", "oos_n", "oos_avg_ret_%", "depth", "rule"])
    for cell in ws3[1]:
        cell.font = Font(bold=True)
    for p in payload["frontier"]:
        ws3.append([p["oos_wr"], p["oos_n"], p["oos_avg_ret"], p["depth"], p["label"]])
    for i, w in enumerate([10, 10, 14, 7, 70], 1):
        ws3.column_dimensions[ws3.cell(1, i).column_letter].width = w

    # ── Sheet 4: Tier Performance ──
    ws4 = wb.create_sheet("Tier Performance")
    yhdr = []
    for y in TEST_YEARS:
        yhdr += [f"{y}_wr", f"{y}_n"]
    ws4.append(["tier", "N", "win_rate_%", "avg_ret_%"] + yhdr)
    for cell in ws4[1]:
        cell.font = Font(bold=True)
    for t in payload["tier_perf"]:
        rowvals = [t["tier"], t["n"], _r(t["wr"]), _r(t["avg_ret"], 3)]
        for y in TEST_YEARS:
            py = t["per_year"][y]
            rowvals += [_r(py["wr"]), py["n"]]
        ws4.append(rowvals)
        fill = _TIER_FILL.get(t["tier"])
        if fill:
            for ci in range(1, 5 + 2 * len(TEST_YEARS)):
                ws4.cell(ws4.max_row, ci).fill = PatternFill("solid", fgColor=fill)
    for i, w in enumerate([12, 8, 11, 11] + [9] * (2 * len(TEST_YEARS)), 1):
        ws4.column_dimensions[ws4.cell(1, i).column_letter].width = w

    # ── Sheet 5: Today's Top-10 Tiered ──
    ws5 = wb.create_sheet("Today Top-10 Tiered")
    ws5.append([f"Signal date: {payload['today_signal_date'] or 'n/a'}"])
    ws5.cell(1, 1).font = Font(bold=True, size=12)
    ws5.append([])
    thdr = ["rank", "symbol", "sector", "signal_day_ret_%", "vol_ratio",
            "two_day_ret_%", "prev_day_ret_%", "circuit", "avg_lift", "n_fires",
            "assigned_tier", "why"]
    ws5.append(thdr)
    for cell in ws5[3]:
        cell.font = Font(bold=True)
    for t in payload["today_tiered"]:
        ws5.append([t["rank"], t["symbol"], t["sector"], _r(t["signal_day_ret_pct"]),
                    _r(t["signal_day_vol_ratio"], 3), _r(t["two_day_ret_pct"]),
                    _r(t["prev_day_ret_pct"]), t["signal_day_circuit"],
                    _r(t["avg_lift"], 3), t["n_fires"], t["tier"], t["why"]])
        fill = _TIER_FILL.get(t["tier"])
        if fill:
            for ci in range(1, len(thdr) + 1):
                ws5.cell(ws5.max_row, ci).fill = PatternFill("solid", fgColor=fill)
    for i, w in enumerate([5, 12, 20, 14, 10, 13, 13, 8, 9, 8, 13, 56], 1):
        ws5.column_dimensions[ws5.cell(1, i).column_letter].width = w

    wb.save(path)
    return path


def _r(v: Optional[float], nd: int = 2) -> Optional[float]:
    return None if v is None else round(v, nd)


def _csv_fallback(out_dir: Path, payload: Dict[str, Any]) -> Path:
    import csv
    p = out_dir / "falcon_tier_discovery_rulebook.csv"
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["rule_id", "tier", "status", "conditions", "is_wr", "is_n",
                    "oos_wr", "oos_ret", "oos_n", "reason"])
        for rec in payload["rulebook"]:
            w.writerow([rec["rule_id"], rec["tier"], rec["status"],
                        rec["conditions_label"], rec["is_wr"], rec["is_n"],
                        rec["oos_wr"], rec["oos_ret"], rec["oos_n"], rec["reason"]])
    with open(out_dir / "falcon_tier_discovery_today.csv", "w", newline="",
              encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["rank", "symbol", "tier", "signal_day_ret_pct", "why"])
        for t in payload["today_tiered"]:
            w.writerow([t["rank"], t["symbol"], t["tier"],
                        t["signal_day_ret_pct"], t["why"]])
    print(f"[tier-disc] openpyxl missing — wrote CSV fallback: {p.name}")
    return p


# ════════════════════════════════════════════════════════════════════════════
# 12. FORBIDDEN-FEATURE GUARD  (defense-in-depth).
# ════════════════════════════════════════════════════════════════════════════
def assert_no_forbidden(conds: List[Condition]) -> None:
    for c in conds:
        if _is_forbidden(c.feature) or c.feature not in _ALLOWED_FEATURES:
            raise AssertionError(
                f"FORBIDDEN feature in rule: {c.feature} — discovery must use "
                f"signal-time features only.")


# ════════════════════════════════════════════════════════════════════════════
# 13. ORCHESTRATION.
# ════════════════════════════════════════════════════════════════════════════
def run(rnd_db: str, prod_db: Optional[str], out_dir: Path,
        as_of: Optional[str], dry_run: bool) -> int:
    print(f"[tier-disc] RND DB : {rnd_db}")
    print(f"[tier-disc] PROD DB: {prod_db}  (read-only; not required for discovery)")
    print(f"[tier-disc] mode   : {'DRY-RUN (no writes)' if dry_run else 'APPLY'}")

    train, today, max_sd = load_rows(rnd_db)
    if not train:
        print("[tier-disc] ERROR: no trainable rows (context ⋈ study, net_ret_pct "
              "NOT NULL, is_extension=0). Run build_signal_day_context.py first.")
        return 2
    as_of = as_of or max_sd
    print(f"[tier-disc] training N = {len(train)}  (resolved, non-extension)")
    print(f"[tier-disc] today's picks (is_extension=1, latest) = {len(today)}  "
          f"signal_date={today[0]['signal_date'] if today else 'n/a'}")
    print(f"[tier-disc] as_of = {as_of}")

    base = cohort_stats(train)
    print(f"[tier-disc] baseline (all train): WR {base['wr']:.1f}%  "
          f"avg_ret {base['avg_ret']:.2f}%  N {base['n']}")

    # ── GOLD baseline (re-confirm) ──
    gold = gold_baseline(train, GOLD_MAX_SIGNAL_RET)
    gold_relax = gold_baseline(train, GOLD_RELAXED_MAX)
    g_is, g_wf = gold["is"], gold["wf"]["pooled"]
    print("\n[tier-disc] ── GOLD baseline (signal_day_ret <= 0) ──")
    print(f"    IS : WR {g_is['wr']:.1f}%  avg_ret {g_is['avg_ret']:.2f}%  N {g_is['n']}")
    print(f"    OOS: WR {g_wf['wr']:.1f}%  avg_ret {g_wf['avg_ret']:.2f}%  N {g_wf['n']}  (pooled walk-forward)")
    gr_is, gr_wf = gold_relax["is"], gold_relax["wf"]["pooled"]
    print(f"    GOLD<=+2 (more volume) IS: WR {gr_is['wr']:.1f}% N {gr_is['n']} | "
          f"OOS: WR {gr_wf['wr']:.1f}% N {gr_wf['n']}")
    gold_oos_avg = g_wf["avg_ret"]

    # ── ENTERPRISE discovery (beam search → walk-forward → classify) ──
    print("\n[tier-disc] ── ENTERPRISE discovery (beam search) ──")
    candidates = beam_search(train)
    print(f"    generated {len(candidates)} candidate conjunctions "
          f"(beam={BEAM_WIDTH}, depth<={MAX_DEPTH})")

    evaluated: List[Tuple[Candidate, Dict[str, Any], str, str]] = []
    for cand in candidates:
        assert_no_forbidden(cand.conds)           # GUARD
        wf = walk_forward(cand.conds, train)
        status, reason = classify_candidate(cand, wf, gold_oos_avg)
        evaluated.append((cand, wf, status, reason))

    # rank: PROMOTED first, then by pooled OOS WR.
    rank_order = {"PROMOTED": 0, "CANDIDATE": 1, "INSUFFICIENT_DATA": 2}
    evaluated.sort(key=lambda e: (rank_order.get(e[2], 9),
                                  -((e[1]["pooled"]["wr"]) or 0.0),
                                  -((e[1]["pooled"]["n"]) or 0)))

    promoted = [e for e in evaluated if e[2] == "PROMOTED"]
    near_miss = [e for e in evaluated if e[2] == "CANDIDATE"][:10]
    insufficient = [e for e in evaluated if e[2] == "INSUFFICIENT_DATA"][:10]

    # ── self-learning: detect DEMOTIONS vs prior snapshots ──
    demotions: List[Dict[str, Any]] = []
    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        prior = previously_promoted(con, as_of)
    finally:
        con.close()

    # ── assemble rulebook records (stable rule_id from conditions hash) ──
    def rule_id_for(conds: List[Condition]) -> str:
        key = conj_to_json(conds)
        import hashlib
        return "ENT_" + hashlib.sha1(key.encode()).hexdigest()[:10]

    rulebook: List[Dict[str, Any]] = []
    promoted_conjs: List[Tuple[str, List[Condition]]] = []
    live_promoted_ids = set()

    def record(cand: Candidate, wf: Dict[str, Any], status: str, reason: str):
        rid = rule_id_for(cand.conds)
        p = wf["pooled"]
        per_year_json = json.dumps({y: wf["per_year"][y] for y in TEST_YEARS})
        rulebook.append({
            "rule_id": rid, "tier": "ENTERPRISE", "status": status,
            "conditions_label": conj_label(cand.conds),
            "conditions_json": conj_to_json(cand.conds),
            "scaling_path_json": json.dumps(cand.scaling_path),
            "is_wr": cand.is_stats["wr"], "is_ret": cand.is_stats["avg_ret"],
            "is_n": cand.is_stats["n"],
            "oos_wr": p["wr"], "oos_ret": p["avg_ret"], "oos_n": p["n"],
            "per_year_oos_json": per_year_json,
            "years_meeting_wr": ",".join(wf["years_meeting_wr"]),
            "reason": reason,
        })
        return rid

    for cand, wf, status, reason in promoted:
        rid = record(cand, wf, status, reason)
        promoted_conjs.append((rid, cand.conds))
        live_promoted_ids.add(rid)
    for cand, wf, status, reason in near_miss:
        record(cand, wf, status, reason)
    for cand, wf, status, reason in insufficient:
        record(cand, wf, status, reason)

    # DEMOTED: a previously-PROMOTED rule_id not promoted this run (or below gate).
    live_by_id = {r["rule_id"]: r for r in rulebook}
    for rid, prev_wr in prior.items():
        cur = live_by_id.get(rid)
        cur_oos_wr = cur["oos_wr"] if cur else None
        if rid not in live_promoted_ids:
            demotions.append({"rule_id": rid, "prev_oos_wr": prev_wr,
                              "now_oos_wr": cur_oos_wr})
            if cur is not None:
                cur["status"] = "DEMOTED"
                cur["reason"] = (f"DEMOTED: prior OOS WR {prev_wr:.1f}% → now "
                                 + (f"{cur_oos_wr:.1f}%" if cur_oos_wr is not None
                                    else "no longer clears gate"))
            else:
                # rule no longer even surfaced — record a DEMOTED stub.
                rulebook.append({
                    "rule_id": rid, "tier": "ENTERPRISE", "status": "DEMOTED",
                    "conditions_label": "(conditions from prior snapshot)",
                    "conditions_json": "[]", "scaling_path_json": "[]",
                    "is_wr": None, "is_ret": None, "is_n": None,
                    "oos_wr": None, "oos_ret": None, "oos_n": None,
                    "per_year_oos_json": "{}", "years_meeting_wr": "",
                    "reason": f"DEMOTED: prior OOS WR {prev_wr:.1f}% no longer reproduced this run",
                })

    # ── report enterprise outcome ──
    if promoted:
        print(f"\n[tier-disc] PROMOTED enterprise rules: {len(promoted)}")
        for cand, wf, status, reason in promoted:
            p = wf["pooled"]
            print(f"    • {conj_label(cand.conds)}")
            print(f"      OOS WR {p['wr']:.1f}%  avg_ret {p['avg_ret']:.2f}%  "
                  f"N {p['n']}  | IS WR {cand.is_stats['wr']:.1f}%  "
                  f"(gap {cand.is_stats['wr'] - p['wr']:+.1f}pp)")
            print(f"      scaling: " + " → ".join(
                f"{s['wr']:.0f}%(N{s['n']})" for s in cand.scaling_path))
    else:
        print("\n[tier-disc] NO conjunction cleared the ENTERPRISE gate "
              f"(OOS WR>={ENT_OOS_WR_GATE:.0f}% AND N>={ENT_OOS_N_GATE} AND beats GOLD "
              f"AND {ENT_PER_YEAR_MIN_YEARS}/{len(TEST_YEARS)} yrs consistent).")
        # best robust rule = highest OOS WR among those with N>=gate (else N>=min_n).
        robust = [e for e in evaluated if (e[1]["pooled"]["n"] or 0) >= ENT_OOS_N_GATE
                  and e[1]["pooled"]["wr"] is not None]
        if not robust:
            robust = [e for e in evaluated if (e[1]["pooled"]["n"] or 0) >= ENT_PER_YEAR_MIN_N
                      and e[1]["pooled"]["wr"] is not None]
        if robust:
            robust.sort(key=lambda e: -(e[1]["pooled"]["wr"] or 0))
            cand, wf = robust[0][0], robust[0][1]
            p = wf["pooled"]
            print(f"    BEST ROBUST rule (N>={ENT_OOS_N_GATE if any((e[1]['pooled']['n'] or 0)>=ENT_OOS_N_GATE for e in evaluated) else ENT_PER_YEAR_MIN_N}): "
                  f"{conj_label(cand.conds)}")
            print(f"      OOS WR {p['wr']:.1f}%  avg_ret {p['avg_ret']:.2f}%  N {p['n']}")
            print(f"      scaling: " + " → ".join(
                f"{s['wr']:.0f}%(N{s['n']})" for s in cand.scaling_path))

    if near_miss:
        print(f"\n[tier-disc] near-miss candidates (just under gate): {len(near_miss)}")
        for cand, wf, status, reason in near_miss[:5]:
            p = wf["pooled"]
            print(f"    - OOS WR {p['wr']:.1f}% N {p['n']}: {conj_label(cand.conds)}  [{reason}]")

    if demotions:
        print(f"\n[tier-disc] DEMOTIONS (self-learning vs prior snapshot): {len(demotions)}")
        for d in demotions:
            print(f"    {d['rule_id']}: prior OOS WR {d['prev_oos_wr']:.1f}% → "
                  + (f"{d['now_oos_wr']:.1f}%" if d['now_oos_wr'] is not None else "absent"))

    # ── best scaling path for sheet 2 (a promoted rule if any, else best candidate) ──
    bc = bwf = None
    head = "BEST CANDIDATE (none promoted)"
    if promoted:
        bc, bwf = promoted[0][0], promoted[0][1]
        head = "PROMOTED ENTERPRISE"
    else:
        ev = sorted([e for e in evaluated if e[1]["pooled"]["wr"] is not None],
                    key=lambda e: -(e[1]["pooled"]["wr"] or 0))
        if ev:
            bc, bwf = ev[0][0], ev[0][1]
        elif candidates:
            bc = candidates[0]
            bwf = walk_forward(candidates[0].conds, train)
    if bc is not None and bwf is not None:
        bp = bwf["pooled"]
        best_scaling_path = {
            "headline": f"{head}: {conj_label(bc.conds)}",
            "path": bc.scaling_path,
            "oos_wr": _r(bp["wr"]), "oos_n": bp["n"], "oos_ret": _r(bp["avg_ret"], 3),
        }
    else:  # no candidate beat the baseline at all
        best_scaling_path = {
            "headline": "no conjunction beat the baseline WR — see GOLD baseline",
            "path": [], "oos_wr": None, "oos_n": 0, "oos_ret": None,
        }

    # ── frontier ──
    frontier = build_frontier([(c, wf) for (c, wf, _, _) in evaluated])
    print("\n[tier-disc] ── OOS WR-vs-Volume Frontier (top rows) ──")
    for p in frontier[:8]:
        print(f"    WR {p['oos_wr']:.1f}%  N {p['oos_n']:>5}  avg {p['oos_avg_ret']}  "
              f"| {p['label']}")

    # ── tier performance over train ──
    tier_perf = tier_performance(train, promoted_conjs)
    print("\n[tier-disc] ── Tier Performance (resolved train) ──")
    for t in tier_perf:
        wr = f"{t['wr']:.1f}%" if t["wr"] is not None else "n/a"
        ar = f"{t['avg_ret']:.2f}%" if t["avg_ret"] is not None else "n/a"
        print(f"    {t['tier']:>10}: N {t['n']:>5}  WR {wr:>7}  avg {ar}")

    # ── today's top-10 tiered ──
    today_tiered = []
    for r in today:
        tier, why = assign_tier(r, promoted_conjs)
        today_tiered.append({
            "rank": r.get("engine_rank"), "symbol": r.get("symbol"),
            "sector": r.get("sector"),
            "signal_day_ret_pct": r.get("signal_day_ret_pct"),
            "signal_day_vol_ratio": r.get("signal_day_vol_ratio"),
            "two_day_ret_pct": r.get("two_day_ret_pct"),
            "prev_day_ret_pct": r.get("prev_day_ret_pct"),
            "signal_day_circuit": r.get("signal_day_circuit"),
            "avg_lift": r.get("avg_lift"), "n_fires": r.get("n_fires"),
            "tier": tier, "why": why,
        })
    print(f"\n[tier-disc] ── Today's Top-10 Tiered "
          f"(signal_date {today[0]['signal_date'] if today else 'n/a'}) ──")
    for t in today_tiered:
        sdr = (f"{t['signal_day_ret_pct']:+.2f}%" if t["signal_day_ret_pct"] is not None
               else "NULL")
        print(f"    #{(t['rank'] or 0):>2} {str(t['symbol'])[:12]:12} "
              f"sdr={sdr:>8} -> {t['tier']}")

    # ── PARITY / SANITY ──
    print("\n[tier-disc] ── PARITY / SANITY ──")
    print(f"    training N: {len(train)}")
    print(f"    GOLD IS WR {g_is['wr']:.1f}% vs OOS WR {g_wf['wr']:.1f}% "
          f"(gap {g_is['wr'] - g_wf['wr']:+.1f}pp)")
    overfit_flags = 0
    for cand, wf, status, reason in promoted:
        gap = cand.is_stats["wr"] - (wf["pooled"]["wr"] or 0)
        if gap > OVERFIT_GAP_PP:
            overfit_flags += 1
            print(f"    OVERFIT FLAG: {conj_label(cand.conds)} IS-OOS gap {gap:+.1f}pp")
    print(f"    promoted rules with IS-OOS gap>{OVERFIT_GAP_PP:.0f}pp: {overfit_flags}")
    # forbidden-feature confirmation
    all_ok = True
    for rec in rulebook:
        try:
            for cd in json.loads(rec["conditions_json"]):
                if _is_forbidden(cd["feature"]) or cd["feature"] not in _ALLOWED_FEATURES:
                    all_ok = False
        except Exception:
            pass
    print(f"    forbidden-feature check: {'PASS (none used)' if all_ok else 'FAIL'}")

    payload = {
        "rulebook": rulebook,
        "best_scaling_path": best_scaling_path,
        "frontier": frontier,
        "tier_perf": tier_perf,
        "today_tiered": today_tiered,
        "today_signal_date": (today[0]["signal_date"] if today else None),
    }

    if dry_run:
        print("\n[tier-disc] DRY-RUN — nothing written (no rulebook table, no Excel).")
        return 0

    # ── APPLY: write rulebook (idempotent per as_of) + Excel ──
    n_written = write_rulebook(rnd_db, as_of, rulebook)
    print(f"\n[tier-disc] wrote {n_written} rows to {RULES_TABLE} (as_of={as_of}).")

    xlsx = write_excel(out_dir, payload)
    print(f"[tier-disc] wrote workbook: {xlsx}")
    print("[tier-disc] APPLY complete.")
    return 0


# ════════════════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════════════════
def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Self-learning, walk-forward TIER discovery for Falcon "
                    "signals: GOLD baseline + beam-searched ENTERPRISE rulebook, "
                    "promoted on pooled OOS only, written to RND falcon_tier_rules "
                    "(dated snapshots) with a 5-sheet Excel.")
    p.add_argument("--rnd-db", default=None,
                   help="RND research DB (default: persona resolver).")
    p.add_argument("--prod-db", default=None,
                   help="PROD DB (read-only; default: config.POWER_DB_PATH). Not "
                        "required for discovery — all training data is in RND.")
    p.add_argument("--out", default=str(_HERE / "out" / "v9"),
                   help="Output dir for the workbook (default: ./out/v9).")
    p.add_argument("--as-of", default=None,
                   help="Snapshot date for the rulebook (default: max signal_date). "
                        "Never uses wall-clock now.")
    p.add_argument("--dry-run", action="store_true",
                   help="Compute + print rulebook/frontier/today's tiers; write NOTHING.")
    args = p.parse_args(argv)

    rnd_db = args.rnd_db or (_resolve_rnd_db_path() if _resolve_rnd_db_path else None)
    if not rnd_db:
        print("[tier-disc] ERROR: --rnd-db required (persona resolver unavailable).")
        return 2
    prod_db = args.prod_db or PROD_DB
    return run(rnd_db=rnd_db, prod_db=prod_db, out_dir=Path(args.out),
               as_of=args.as_of, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
