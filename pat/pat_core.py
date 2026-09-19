"""IMMUTABLE SCORER for the autonomous pattern agent.

The agent rewrites its own configuration, its own pattern logic, and its own
search strategy. It may not rewrite this file, and it halts if this file changes.

WHY THE EXCEPTION EXISTS
------------------------
An agent told to reach a target has two routes: find a real edge, or weaken the
test. The second is far easier. Every guard below was added because its absence
produced a large and completely false result:

  coverage cap      a rule set firing on 90% of sessions is not selection, it
                    is being long every day. That alone produced -0.25%/day and
                    looked like a directional finding.
  skipped-days      what the strategy DID NOT trade. If the days it sat out
                    averaged +5%, the rules are excluding the right tail, and
                    no amount of tuning inside them fixes that.
  forward protocol  fit on years before, test on the year after, never revisit.
  shuffle control   the same procedure on shuffled outcomes.
  point-in-time     thresholds from expanding own-history, lagged one day.

If the agent could edit these it would remove the coverage cap within an hour,
report a permanent long as a discovery, and be worth nothing.
"""
from __future__ import annotations

import hashlib
import inspect
import sys

import numpy as np
import pandas as pd

SEAL_FROM = "2026-01-01"   # 2026 is sealed: the agent never sees it
COST = 0.0011
SLIP = 0.0005
MAX_COVERAGE = 0.40      # above this it is not a pattern, it is a market view
MIN_SIGNALS = 15


def fingerprint() -> str:
    src = inspect.getsource(sys.modules[__name__]).split("ANCHOR")[0]
    return hashlib.sha256(src.encode()).hexdigest()[:16]


def wilson(k: int, n: int, z: float = 1.96) -> float:
    if n == 0:
        return 0.0
    p = k / n
    return max(0.0, (p + z*z/(2*n) - z*np.sqrt(p*(1-p)/n + z*z/(4*n*n)))
               / (1 + z*z/n))


def base_rates(fw: pd.DataFrame, sel: np.ndarray) -> dict:
    ok = sel & fw["r1"].notna().to_numpy()
    b = {}
    for k in (1, 2, 3):
        r = fw.loc[ok, f"r{k}"]
        b[f"hit{k}"] = float((r > 0).mean()) if len(r) else 0.0
        b[f"avg{k}"] = float(r.mean()) if len(r) else 0.0
    b["n"] = int(ok.sum())
    return b


def score_mask(mask, fw, base, min_n) -> dict | None:
    m = mask & fw["r1"].notna().to_numpy()
    n = int(m.sum())
    if n < min_n:
        return None
    o = {"n": n}
    for k in (1, 2, 3):
        r = fw.loc[m, f"r{k}"].to_numpy()
        w = int((r > 0).sum())
        o[f"hit{k}"] = w / n
        o[f"avg{k}"] = float(r.mean())
        o[f"mfe{k}"] = float(fw.loc[m, f"mfe{k}"].mean())
        o[f"mae{k}"] = float(fw.loc[m, f"mae{k}"].mean())
        o[f"conf{k}"] = wilson(w, n)
        o[f"strength{k}"] = o[f"conf{k}"] - base[f"hit{k}"]
        o[f"edge{k}"] = o[f"avg{k}"] - base[f"avg{k}"]
    d = fw.loc[m, "date"]
    half = d.median()
    a = m & (fw["date"] <= half).to_numpy()
    b = m & (fw["date"] > half).to_numpy()
    if a.sum() > 4 and b.sum() > 4:
        e1 = fw.loc[a, "r1"].mean() - base["avg1"]
        e2 = fw.loc[b, "r1"].mean() - base["avg1"]
        o["consistency"] = float(np.sign(e1) == np.sign(e2))
        o["recency"] = float(e2)
    else:
        o["consistency"], o["recency"] = 0.0, 0.0
    return o


def evaluate_year(sel: np.ndarray, te: np.ndarray, fw: pd.DataFrame,
                  side: str, rng) -> dict:
    """One test year, judged with every guard applied. Never modified."""
    te_ok = te & fw["r1"].notna().to_numpy()
    s = sel & te_ok
    n = int(s.sum())
    cover = n / max(int(te_ok.sum()), 1)
    out = {"signals": n, "coverage": cover}
    if n < MIN_SIGNALS:
        out.update(valid=False, why="too few signals")
        return out
    if cover > MAX_COVERAGE:
        out.update(valid=False, why=f"coverage {cover:.0%} > {MAX_COVERAGE:.0%}")
        return out
    sgn = 1.0 if side == "long" else -1.0
    r1 = fw.loc[s, "r1"].to_numpy() * sgn
    r2 = fw.loc[s, "r2"].to_numpy() * sgn
    r3 = fw.loc[s, "r3"].to_numpy() * sgn
    base = base_rates(fw, te)
    skipped = fw.loc[te_ok & ~sel, "r1"]
    idx = np.where(te_ok)[0]
    sh = [float(np.mean(fw["r1"].to_numpy()[rng.choice(idx, n, replace=False)] * sgn))
          for _ in range(200)]
    # PROFIT, per horizon. This is what the agent now optimises: money per
    # signal and its risk-adjusted version, not merely a hit rate. A pattern
    # that wins 70% of the time in 0.2% increments and loses 2% when it is
    # wrong is not a good pattern, and only the return distribution says so.
    for h, rr in ((1, r1), (2, r2), (3, r3)):
        sd = float(np.std(rr))
        out[f"avg{h}"] = float(np.mean(rr))
        out[f"std{h}"] = sd
        out[f"sharpe{h}"] = (float(np.mean(rr)) / sd * np.sqrt(len(rr))
                             if sd > 1e-9 else 0.0)
        out[f"hitrate{h}"] = float((rr > 0).mean())
        out[f"total{h}"] = float(np.sum(rr))
    out.update(valid=True, why="ok",
               hit1=float((r1 > 0).mean()),
               base_avg1=base["avg1"] * sgn, base_avg3=base["avg3"] * sgn,
               shuffled=float(np.mean(sh)),
               edge1=float(r1.mean()) - np.mean(sh),
               skipped_avg1=float(skipped.mean()) if len(skipped) else np.nan)
    return out


def fitness(years: list[dict], horizon: int = 1) -> tuple[float, str]:
    """PROFIT, risk-adjusted, gated on beating chance.

    The metric is the average per-signal Sharpe at the chosen horizon: mean
    return divided by its own dispersion, scaled by the number of signals. That
    rewards making money reliably rather than being right often -- a rule that
    wins 70% of the time in small increments and loses badly when wrong scores
    poorly here, correctly.

    The gates stay in front of it. A configuration must be valid in most years
    and beat its shuffled control in most years BEFORE any profit counts. A
    single spectacular year cannot carry three bad ones, because the mean is
    taken over years that individually qualified.
    """
    if not years:
        return -9.0, "no valid years"
    valid = [y for y in years if y.get("valid")]
    if len(valid) < max(2, len(years) - 1):
        # A GRADED penalty, not a flat one. The constraint is unchanged --
        # anything invalid still scores negative and can never be adopted -- but
        # a configuration covering 45% of sessions now scores better than one
        # covering 90%, which gives the search a direction to walk in. A flat
        # -9 everywhere left the agent guessing blindly.
        cov = np.mean([y.get("coverage", 1.0) for y in years]) if years else 1.0
        excess = max(cov - MAX_COVERAGE, 0.0)
        return (-9.0 + len(valid) - excess * 4.0,
                f"only {len(valid)}/{len(years)} valid, mean cover {cov:.0%}")
    h = int(horizon)
    edges = np.array([y["edge1"] for y in valid])
    won = int((edges > 0).sum())
    if won < np.ceil(len(valid) * 0.6):
        return -5.0 + won, f"beat shuffle in only {won}/{len(valid)} years"
    sh = np.array([y.get(f"sharpe{h}", 0.0) for y in valid])
    av = np.array([y.get(f"avg{h}", 0.0) for y in valid])
    if (av > 0).sum() < np.ceil(len(valid) * 0.6):
        return -1.0 + float(av.mean()), \
            f"profitable in only {int((av > 0).sum())}/{len(valid)} years"
    return float(sh.mean()), (f"{h}d Sharpe {sh.mean():.2f}, "
                              f"avg {av.mean():+.3f}%/signal, "
                              f"beat shuffle {won}/{len(valid)}")


# ANCHOR
CORE = fingerprint()
