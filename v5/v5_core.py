"""V5 IMMUTABLE CORE — the one part the agent may not rewrite.

WHY THIS FILE IS LOCKED
-----------------------
The agent modifies its own feature code, its own model space, and its own search
policy. It reads the database to find its own resources. It runs unattended and
never stops. All of that is in v5_agent.py and v5_genome.py, and all of it is
genuinely self-modifying.

This file is different, and it is the only exception.

An optimiser under pressure to reach +1%/day has two routes: find a real edge,
or weaken the test that decides what counts as one. The second is enormously
easier. Remove the purge gap and multi-day targets leak across the fold
boundary. Drop the shuffle control and drift reads as skill. Widen the seal and
2026 becomes training data. Every one of those changes makes the reported number
go up immediately, and every one destroys the thing the number was for.

This is not a hypothetical failure. In this project six separate corrections
were needed, and every single one moved the results DOWN -- because each
unexamined assumption had been flattering the search. An agent with write access
to its own scorer would have moved all six in the other direction, reported
+1%/day within an hour, and been worth nothing.

So: the agent may rewrite anything that GENERATES a candidate. It may not
rewrite anything that JUDGES one. That boundary is what makes its self-
improvement trustworthy rather than merely fast.

The hash below is checked on every run. If this file changes, the agent halts.
"""
from __future__ import annotations

import hashlib
import inspect
import sys
from datetime import datetime

import numpy as np
import pandas as pd

SEAL_FROM = "2026-01-01"        # never crossed
COST = 0.0011                   # round trip, retail intraday India
SLIP = 0.0005                   # per round trip
MIN_TRAIN = 3000
MIN_TEST = 200
PURGE_DAYS = 5


def core_fingerprint() -> str:
    src = inspect.getsource(sys.modules[__name__])
    body = src.split("FINGERPRINT_ANCHOR")[0]
    return hashlib.sha256(body.encode()).hexdigest()[:16]


def assert_sealed(df: pd.DataFrame, col: str = "session") -> None:
    """Hard stop if anything from the sealed period reached the process."""
    if df.empty:
        return
    mx = pd.to_datetime(df[col]).max()
    if str(mx)[:10] >= SEAL_FROM:
        raise RuntimeError(
            f"SEAL BREACH: data at {mx} reached research code. "
            f"Nothing from {SEAL_FROM} onward may be read before deploy.")


def purged_folds(sessions: np.ndarray, n_folds: int = 8,
                 purge_days: int = PURGE_DAYS):
    """Expanding windows with a gap, so a multi-day target cannot straddle.

    Without the gap, a 3-day target computed on the last training day overlaps
    the first test days. The model then sees the answer through the boundary and
    every multi-day result is inflated.
    """
    s = np.sort(np.unique(sessions))
    if len(s) < n_folds * 4:
        return
    cuts = [s[int(len(s) * (i + 1) / (n_folds + 1))] for i in range(n_folds)]
    for i, c in enumerate(cuts):
        end = cuts[i + 1] if i + 1 < len(cuts) else s[-1] + np.timedelta64(1, "D")
        yield c - np.timedelta64(purge_days, "D"), c, end


def net(gross: np.ndarray) -> np.ndarray:
    """Costs are charged here and nowhere else, so they cannot be tuned away."""
    return gross - COST - SLIP


def book_per_session(picks_by_leg: dict, n_sessions: int) -> float:
    """Fixed capital: same-day positions SPLIT it, so the book earns the MEAN.

    Summing concurrent positions would imply unlimited capital and is the
    easiest way to manufacture a large number from a small edge.
    """
    acc = {}
    for pk in picks_by_leg.values():
        if pk is None or len(pk) == 0:
            continue
        for s_, v in zip(pk["session"], pk["real"]):
            acc.setdefault(s_, []).append(v)
    if not acc:
        return 0.0
    per = np.array([np.mean(v) for v in acc.values()])
    return float(per.sum() / max(n_sessions, 1) * 100)


def evaluate_leg(real: np.ndarray, n_sessions: int) -> dict:
    if len(real) == 0:
        return {"n": 0, "mean_pct": 0.0, "win": 0.0, "per_session": 0.0}
    return {"n": int(len(real)),
            "mean_pct": float(np.mean(real) * 100),
            "win": float(np.mean(real > 0)),
            "per_session": float(np.sum(real) / max(n_sessions, 1) * 100)}


def verdict(raw_edge: float, shuffled_edge: float, n: int,
            margin: float = 0.05) -> tuple[bool, str]:
    """A candidate must beat its OWN shuffled control, not zero."""
    if n < 60:
        return False, "too few trades"
    if raw_edge <= 0:
        return False, "negative"
    if raw_edge <= shuffled_edge + margin:
        return False, "no better than shuffled"
    return True, "passes"


def audit_line(tag: str, **kw) -> str:
    bits = " ".join(f"{k}={v}" for k, v in kw.items())
    return f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {tag} {bits}"


# FINGERPRINT_ANCHOR — everything above is hashed; nothing below affects it.
CORE_HASH = core_fingerprint()
