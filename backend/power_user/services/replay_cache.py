"""Replay cache — featured pre-compute + on-the-fly arbitrary + 24h TTL.

Three paths:
  1. Featured replays (Apr-15-2026, Nov-04-2024, Dec-15-2025) — pre-computed
     once, stored with is_featured=1, never expire. Sub-200ms read.
  2. Arbitrary historical date — compute on-the-fly from features+patterns+
     OHLC, cache in DB for 24h. ~1.5s cold, <200ms warm.
  3. Random replay — uniform pick over trading days in last 2yr, then same
     path as arbitrary.

All routers consume the same `build_replay_payload` output shape:
    {
      replay_date, entry_date, is_featured, title?, hook?,
      aggregate: { n_picks, horizons: { d1: {wr, hit_5pct, avg_ret}, ... } },
      picks: [Pick, Pick, ...]   ← each is a v1 Pick payload
    }

The picks list goes through build_pick_payload → validate_pick_payload, so
schema drift can't enter the cache.
"""
from __future__ import annotations

import json
import logging
import random
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ..config import FEATURED_REPLAYS
from .explainer import (
    aggregate_outcomes,
    build_pick_payload,
    compute_top_n,
    get_outcomes,
    load_patterns,
    validate_pick_payload,
)

log = logging.getLogger("kanida.power_user.replay_cache")
IST = timezone(timedelta(hours=5, minutes=30))

DEFAULT_TOP_N = 50           # featured/random replays show 50 picks each
ARBITRARY_TTL_HR = 24        # cache an arbitrary-date payload for 24h
RANDOM_LOOKBACK_DAYS = 730   # 2-year window for 🎲 Random Replay


# ──────────────────────────────────────────────────────────────────────────
# Build a single replay payload (the canonical shape)
# ──────────────────────────────────────────────────────────────────────────

def build_replay_payload(con: sqlite3.Connection,
                          replay_date: str,
                          top_n: int = DEFAULT_TOP_N,
                          is_featured: bool = False,
                          title: Optional[str] = None,
                          hook: Optional[str] = None,
                          ) -> Dict[str, Any]:
    """Compute the full replay payload from scratch (no cache lookup).

    Used internally by:
      - precompute_featured() — runs once per featured date at boot
      - get_or_compute() on cache miss for arbitrary dates
    """
    entry_row = con.execute(
        "SELECT MIN(trade_date) FROM ohlc_daily WHERE trade_date > ?",
        (replay_date,),
    ).fetchone()
    entry_date = entry_row[0] if entry_row else None
    if entry_date is None:
        return {
            "replay_date":  replay_date,
            "entry_date":   None,
            "is_featured":  is_featured,
            "title":        title,
            "hook":         hook,
            "aggregate":    {"n_picks": 0, "horizons": {}},
            "picks":        [],
            "error":        "NO_NEXT_TRADING_DAY",
        }

    patterns = load_patterns(con)
    raw_picks = compute_top_n(con, replay_date, top_n=top_n, patterns=patterns)

    picks: List[Dict[str, Any]] = []
    picks_with_outcomes: List[Dict[str, Any]] = []
    for rank, p in enumerate(raw_picks, start=1):
        out = get_outcomes(con, p["symbol"], entry_date)
        payload = build_pick_payload(
            rank=rank, pick=p, outcomes=out or None,
            signal_date=replay_date, entry_date=entry_date,
        )
        # build_pick_payload already self-validates, but assert again as a
        # belt-and-suspenders chokepoint for the cache layer.
        validate_pick_payload(payload)
        picks.append(payload)
        picks_with_outcomes.append(payload)

    aggregate = aggregate_outcomes(picks_with_outcomes)

    return {
        "replay_date":  replay_date,
        "entry_date":   entry_date,
        "is_featured":  is_featured,
        "title":        title,
        "hook":         hook,
        "aggregate":    aggregate,
        "picks":        picks,
        "computed_at":  datetime.now(IST).isoformat(),
    }


# ──────────────────────────────────────────────────────────────────────────
# Featured replays — precomputed once, immutable
# ──────────────────────────────────────────────────────────────────────────

def precompute_featured(con: sqlite3.Connection, force: bool = False) -> Dict[str, Any]:
    """Compute every featured replay and store in falcon_replay_cache.

    Idempotent: if `force=False` and a payload already exists for a featured
    date, skip it. Use `force=True` to recompute (e.g. after a code change
    in the explainer).

    Called from main.py lifespan at backend boot.
    """
    summary: Dict[str, Any] = {"computed": [], "skipped": [], "errors": []}
    now_iso = datetime.now(IST).isoformat()

    for entry in FEATURED_REPLAYS:
        date  = entry["date"]
        title = entry.get("title")
        hook  = entry.get("hook")

        if not force:
            existing = con.execute(
                "SELECT 1 FROM falcon_replay_cache WHERE replay_date = ? AND is_featured = 1",
                (date,),
            ).fetchone()
            if existing:
                summary["skipped"].append(date)
                continue

        try:
            payload = build_replay_payload(con, date,
                                            is_featured=True,
                                            title=title, hook=hook)
            if payload.get("error"):
                summary["errors"].append({"date": date, "reason": payload["error"]})
                continue
            con.execute(
                "INSERT OR REPLACE INTO falcon_replay_cache "
                "(replay_date, payload_json, is_featured, title, computed_at, expires_at) "
                "VALUES (?, ?, 1, ?, ?, NULL)",
                (date, json.dumps(payload, default=str), title, now_iso),
            )
            con.commit()
            summary["computed"].append({
                "date":     date,
                "title":    title,
                "n_picks":  payload["aggregate"]["n_picks"],
            })
            log.info("replay_cache: featured %s pre-computed (n=%d)",
                     date, payload["aggregate"]["n_picks"])
        except Exception as e:
            log.exception("replay_cache: precompute %s failed", date)
            summary["errors"].append({"date": date, "reason": str(e)[:200]})

    return summary


def list_featured(con: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Return the lightweight list of featured replays (headline-only) — used
    by the landing page to render the 3 cards above the fold. Sub-50ms."""
    rows = con.execute("""
        SELECT replay_date, title, payload_json
          FROM falcon_replay_cache
         WHERE is_featured = 1
         ORDER BY replay_date DESC
    """).fetchall()
    out: List[Dict[str, Any]] = []
    for replay_date, title, payload_json in rows:
        try:
            payload = json.loads(payload_json)
        except (TypeError, ValueError):
            continue
        agg = payload.get("aggregate") or {}
        horizons = agg.get("horizons") or {}
        d5  = horizons.get("d5")  or {}
        d15 = horizons.get("d15") or {}
        out.append({
            "replay_date":  replay_date,
            "title":        title,
            "hook":         payload.get("hook"),
            "n_picks":      agg.get("n_picks", 0),
            "wr_d5":        d5.get("wr"),
            "wr_d15":       d15.get("wr"),
            "avg_d15":      d15.get("avg_ret"),
            "hit_5_d15":    d15.get("hit_5pct"),
        })
    return out


# ──────────────────────────────────────────────────────────────────────────
# Arbitrary date — featured short-circuit, then cache, then on-the-fly
# ──────────────────────────────────────────────────────────────────────────

def get_or_compute(con: sqlite3.Connection, replay_date: str) -> Dict[str, Any]:
    """Three-tier lookup:
      1. Featured cache (is_featured=1) — never expires
      2. Arbitrary cache (is_featured=0) — 24h TTL
      3. Fresh compute → cache → return
    Returns the canonical replay payload."""
    row = con.execute("""
        SELECT payload_json, is_featured, expires_at
          FROM falcon_replay_cache WHERE replay_date = ?
    """, (replay_date,)).fetchone()

    if row:
        payload_json, is_featured, expires_at = row
        if is_featured:
            return json.loads(payload_json)
        # Arbitrary cache hit — check TTL
        if expires_at:
            try:
                exp_dt = datetime.fromisoformat(expires_at)
                if datetime.now(IST) < exp_dt:
                    return json.loads(payload_json)
            except ValueError:
                pass    # malformed expires_at → recompute

    # Cache miss or expired → recompute
    payload = build_replay_payload(con, replay_date)
    if payload.get("error"):
        return payload

    now    = datetime.now(IST)
    exp    = (now + timedelta(hours=ARBITRARY_TTL_HR)).isoformat()
    con.execute(
        "INSERT OR REPLACE INTO falcon_replay_cache "
        "(replay_date, payload_json, is_featured, computed_at, expires_at) "
        "VALUES (?, ?, 0, ?, ?)",
        (replay_date, json.dumps(payload, default=str), now.isoformat(), exp),
    )
    con.commit()
    return payload


# ──────────────────────────────────────────────────────────────────────────
# Random replay — uniform pick over last 2yr trading days
# ──────────────────────────────────────────────────────────────────────────

def random_replay_date(con: sqlite3.Connection,
                        lookback_days: int = RANDOM_LOOKBACK_DAYS) -> Optional[str]:
    """Uniformly pick a trading day from the last `lookback_days`.

    Excludes the most recent 30 trading days so D+15 outcomes exist.
    Returns None if not enough history (shouldn't happen in prod)."""
    rows = con.execute(
        """SELECT DISTINCT trade_date FROM ohlc_daily
            WHERE trade_date >= date('now', ?)
            ORDER BY trade_date DESC""",
        (f"-{lookback_days} days",),
    ).fetchall()
    if len(rows) < 40:
        return None
    # Drop the most recent 30 trading days — D+15 outcomes need 30+ future bars
    candidate_dates = [r[0] for r in rows[30:]]
    if not candidate_dates:
        return None
    return random.choice(candidate_dates)


def random_replay(con: sqlite3.Connection) -> Dict[str, Any]:
    """Pick a random date and return its replay payload (with caching)."""
    date = random_replay_date(con)
    if date is None:
        return {"error": "INSUFFICIENT_HISTORY"}
    return get_or_compute(con, date)
