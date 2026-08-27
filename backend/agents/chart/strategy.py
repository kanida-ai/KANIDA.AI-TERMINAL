"""
Chart Agent · Strategy-Replay ETV (v3 §8.2) — the correctness keystone.

v3 §0/§8: two outcome families, NEVER conflated.
  * Pattern-forward outcome (evidence.pattern_evidence, §8.1) — did the CHART contain predictive
    information? (fixed hold-to-close). Always kept for research; never overwritten here.
  * Strategy outcome (THIS module, §8.2) — did our EXECUTION POLICY monetize it? The decision gate
    (§9) reads THESE stats, because we must judge the same strategy the agent will actually trade.

GOVERNANCE / HONESTY (v3 §8.2 consequence, §9, §14):
  The exit policy is GOVERNED and PRE-DECLARED (frozen in a rulebook version). It is NOT fit on the
  data used to decide — doing so would re-introduce the horizon/exit cherry-picking §9 bans. The
  defaults below are the frozen policy `STRATEGY_VERSION`; changing them is a NEW rulebook version,
  calibrated only by walk-forward + OOS in Loop 4 — never live, never on the deciding sample.

Point-in-time: replay is only valid on RESOLVED occurrences (entry_idx + H - 1 <= as_of_idx); the
callers filter to those. Entry is the NEXT open. Costs applied on every simulated trade.
"""
from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Optional
import numpy as np
import pandas as pd

COST = 0.003          # 30 bps round-trip — same as the pattern-forward family (§3)
STRATEGY_VERSION = "S-horiz-v1"   # frozen governed policy id, stamped on every decision


# --------------------------------------------------------------------- the governed policy
@dataclass(frozen=True)
class StrategyPolicy:
    """Frozen, governed exit policy `S` (v3 §8.2). Pre-declared — NOT fit on the deciding data.

    Fields:
      max_hold        H bars; if nothing else triggers, exit at close[entry+H-1] (HORIZON).
      trail_pct       trailing stop off the running peak high (default 8%). low<=peak*(1-trail)=>TRAIL.
      target_pct      optional fixed profit target off entry. high>=entry*(1+target)=>TARGET. None=off.
      stop_pct        optional HARD intrabar stop off entry. low<=entry*(1-stop)=>STOP. None=off
                      (the DEFAULT relies on the structural close-based invalidation instead).
      buffer          structural-invalidation buffer (matches detector PARAMS buffer=0.002):
                      close < level*(1-buffer) => INVALIDATION (the pattern's thesis is broken).
    """
    version: str = STRATEGY_VERSION
    max_hold: int = 10          # H = T+10, matches the detector's tracking + resolved-as-of window
    trail_pct: float = 0.08     # ~8% trail (MEMORY: intraday trailing is how MIS risk is managed)
    target_pct: Optional[float] = None
    stop_pct: Optional[float] = None
    buffer: float = 0.002

    def to_dict(self) -> dict:
        return asdict(self)


DEFAULT_POLICY = StrategyPolicy()


# --------------------------------------------------------------------- single-occurrence replay
def _get(occ, key, default=None):
    if isinstance(occ, dict):
        return occ.get(key, default)
    return getattr(occ, key, default)


def _replay_core(o, h, l, c, n, entry_idx: int, level: float, P: StrategyPolicy) -> Optional[dict]:
    """Replay policy P from entry_idx over up to H bars. Returns the strategy outcome or None if the
    occurrence is not fully resolved (entry_idx + H - 1 > last bar) — replay must never peek forward.
    Check order follows v3 §8.2: STOP -> TARGET -> TRAIL -> INVALIDATION -> HORIZON."""
    H = P.max_hold
    last = entry_idx + H - 1
    if entry_idx < 0 or last >= n or o[entry_idx] <= 0:
        return None
    entry_px = float(o[entry_idx])
    stop_px = entry_px * (1 - P.stop_pct) if P.stop_pct else None
    tgt_px = entry_px * (1 + P.target_pct) if P.target_pct else None
    inval_px = level * (1 - P.buffer)          # structural stop = thesis broken (close-based)

    peak = -np.inf
    exit_px = None
    exit_reason = None
    exit_d = last
    dd = 0.0                                    # worst close-to-peak drawdown over the hold
    peak_close = entry_px
    for d in range(entry_idx, last + 1):
        # drawdown texture on the close path
        peak_close = max(peak_close, float(c[d]))
        dd = min(dd, float(c[d]) / peak_close - 1)
        # 1) hard intrabar STOP (optional)
        if stop_px is not None and l[d] <= stop_px:
            exit_px, exit_reason, exit_d = stop_px, "STOP", d; break
        # 2) fixed TARGET (optional)
        if tgt_px is not None and h[d] >= tgt_px:
            exit_px, exit_reason, exit_d = tgt_px, "TARGET", d; break
        # 3) TRAIL off the running peak high (update peak with this bar, then test this bar's low)
        peak = max(peak, float(h[d]))
        trail_px = peak * (1 - P.trail_pct)
        if l[d] <= trail_px:
            exit_px, exit_reason, exit_d = trail_px, "TRAIL", d; break
        # 4) structural INVALIDATION (close below the reclaimed level)
        if c[d] < inval_px:
            exit_px, exit_reason, exit_d = float(c[d]), "INVALIDATION", d; break
    if exit_reason is None:                     # 5) nothing triggered -> HORIZON at H
        exit_px, exit_reason, exit_d = float(c[last]), "HORIZON", last

    seg_hi = float(h[entry_idx:exit_d + 1].max())
    seg_lo = float(l[entry_idx:exit_d + 1].min())
    return {
        "entry_px": round(entry_px, 4),
        "exit_px": round(float(exit_px), 4),
        "exit_reason": exit_reason,
        "strategy_return": float(exit_px) / entry_px - 1 - COST,   # net of cost
        "holding_period": int(exit_d - entry_idx + 1),
        "strat_mfe": seg_hi / entry_px - 1,
        "strat_mae": seg_lo / entry_px - 1,
        "strat_drawdown": float(dd),
    }


def replay_one(df: pd.DataFrame, occurrence, policy: StrategyPolicy = DEFAULT_POLICY) -> Optional[dict]:
    """Public single-occurrence replay. ``occurrence`` may be a dict, a PatternOccurrence, or the
    detector's _Event — anything exposing ``entry_idx`` and ``level``. Only ever call on RESOLVED
    occurrences (entry_idx + policy.max_hold - 1 <= as_of); returns None if not resolvable."""
    o, h, l, c = (df[k].values for k in ["open", "high", "low", "close"])
    entry_idx = int(_get(occurrence, "entry_idx", -1))
    level = _get(occurrence, "level", None)
    if level is None:
        return None
    return _replay_core(o, h, l, c, len(c), entry_idx, float(level), policy)


# --------------------------------------------------------------------- aggregated strategy evidence
def _ci_low(returns: np.ndarray) -> Optional[float]:
    """Lower 95% CI of Strategy-ETV (normal-approx: mean - 1.96*SE). Bootstrap CI is SPEC (§7.3)."""
    if returns.size < 2:
        return None
    se = float(returns.std(ddof=1)) / np.sqrt(returns.size)
    return (float(returns.mean()) - 1.96 * se) * 100


def strategy_baseline_etv(df: pd.DataFrame, policy: StrategyPolicy, as_of_idx: Optional[int] = None) -> Optional[float]:
    """Strategy-baseline: enter EVERY day's next open and exit under the SAME policy, so the edge
    isolates what the PATTERN adds over the exit machinery alone (v3 §9 G2). A random entry has NO
    pattern thesis/level to break, so the structural INVALIDATION is DISABLED for the baseline
    (level := -inf, so ``close < level*(1-buffer)`` never fires); the baseline monetizes on the
    level-agnostic exits only (trail / target / horizon). This corrects the earlier handicap where
    level:=entry_open made the baseline invalidate ~75% of the time (47% on the entry day),
    understating it ~0.44pp and inflating ``edge`` fed to G2 (auditor Finding 1). Point-in-time:
    only bars <= as_of are used."""
    o, h, l, c = (df[k].values for k in ["open", "high", "low", "close"])
    n = len(c) if as_of_idx is None else min(len(c), int(as_of_idx) + 1)
    H = policy.max_hold
    rets = []
    for i in range(1, n - H + 1):
        r = _replay_core(o, h, l, c, n, i, -np.inf, policy)   # no level -> structural invalidation off
        if r is not None:
            rets.append(r["strategy_return"])
    if not rets:
        return None
    return float(np.mean(rets)) * 100


def strategy_evidence(df: pd.DataFrame, occurrences, policy: StrategyPolicy = DEFAULT_POLICY,
                      as_of_idx: Optional[int] = None) -> Optional[dict]:
    """Aggregate the strategy outcome over RESOLVED occurrences (v3 §8.2). Returns Strategy-ETV,
    win rate, avg win/loss, payoff, avg strat MFE/MAE, avg drawdown, failure rate, N, CI_low, exit
    breakdown, and a recency split (for G6). Only occurrences with entry+H-1 <= as_of are used."""
    o, h, l, c = (df[k].values for k in ["open", "high", "low", "close"])
    n = len(c)
    H = policy.max_hold
    dates = df.index
    recs = []
    for occ in occurrences:
        e = int(_get(occ, "entry_idx", -1))
        if as_of_idx is not None and e + H - 1 > int(as_of_idx):   # resolved-only (leak-proof)
            continue
        r = replay_one(df, occ, policy)
        if r is None:
            continue
        r["_signal_idx"] = int(_get(occ, "signal_idx", e - 1))
        recs.append(r)
    if not recs:
        return None
    ret = np.array([r["strategy_return"] for r in recs], float)
    wins = ret[ret > 0]; losses = ret[ret <= 0]
    mfe = np.array([r["strat_mfe"] for r in recs], float)
    mae = np.array([r["strat_mae"] for r in recs], float)
    dd = np.array([r["strat_drawdown"] for r in recs], float)
    exits: dict = {}
    for r in recs:
        exits[r["exit_reason"]] = exits.get(r["exit_reason"], 0) + 1

    # recency split for G6 — recent 365d of signal dates vs prior (point-in-time as-of)
    end = dates[int(as_of_idx)] if as_of_idx is not None else dates[-1]
    cutoff = end - pd.Timedelta(days=365)
    sig_dates = pd.to_datetime([dates[r["_signal_idx"]] for r in recs])
    rmask = np.asarray(sig_dates >= cutoff)
    recent_r, prior_r = ret[rmask], ret[~rmask]
    recency = {
        "recent_n": int(rmask.sum()), "prior_n": int((~rmask).sum()),
        "recent_etv": round(float(recent_r.mean()) * 100, 3) if recent_r.size else None,
        "prior_etv": round(float(prior_r.mean()) * 100, 3) if prior_r.size else None,
    }

    return {
        "version": policy.version,
        "policy": policy.to_dict(),
        "n": len(recs),
        "etv": round(float(ret.mean()) * 100, 3),
        "win": round(float((ret > 0).mean()) * 100, 1),
        "avg_win": round(float(wins.mean()) * 100, 2) if wins.size else 0.0,
        "avg_loss": round(float(losses.mean()) * 100, 2) if losses.size else 0.0,
        "payoff": round(float(wins.mean() / -losses.mean()), 2) if (wins.size and losses.size and losses.mean() < 0) else None,
        "mfe": round(float(mfe.mean()) * 100, 2),
        "mae": round(float(mae.mean()) * 100, 2),
        "drawdown": round(float(dd.mean()) * 100, 2),
        "failure_rate": round(float((ret <= 0).mean()) * 100, 1),
        "ci_low": _ci_low(ret),
        "exits": exits,
        "recency": recency,
        "avg_holding": round(float(np.mean([r["holding_period"] for r in recs])), 1),
    }
