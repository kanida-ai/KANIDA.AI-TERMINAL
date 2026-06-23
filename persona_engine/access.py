"""
Read-only access API for the persona engine.

This is the clean surface the Power-User portal can later call to surface the two
new trading styles. It is strictly READ-ONLY (opens the RND DB in ro mode) and does
NOT build any UI. No write paths, no engine triggers.
"""
from __future__ import annotations

import json
from typing import List, Optional

from persona_engine import db


def _rows(sql: str, params=()) -> List[dict]:
    con = db.connect(read_only=True)
    try:
        return [dict(r) for r in con.execute(sql, params).fetchall()]
    finally:
        con.close()


# ── F&O ─────────────────────────────────────────────────────────────────────---

def fo_latest_date() -> Optional[str]:
    r = _rows("SELECT MAX(prediction_date) d FROM fo_daily_predictions")
    return r[0]["d"] if r else None


def fo_predictions(date: Optional[str] = None) -> dict:
    """Long/Short Top-10 for a date (defaults to latest)."""
    date = date or fo_latest_date()
    if not date:
        return {"date": None, "long": [], "short": []}
    rows = _rows(
        "SELECT direction,rank,symbol,sector,long_score,short_score,top_features "
        "FROM fo_daily_predictions WHERE prediction_date=? ORDER BY direction,rank",
        (date,))
    out = {"date": date, "long": [], "short": []}
    for r in rows:
        r["top_features"] = json.loads(r["top_features"]) if r["top_features"] else {}
        (out["long"] if r["direction"] == "LONG" else out["short"]).append(r)
    return out


def fo_daily_review(date: Optional[str] = None) -> Optional[dict]:
    date = date or fo_latest_date()
    r = _rows("SELECT * FROM fo_daily_review WHERE prediction_date=?", (date,))
    if not r:
        return None
    d = r[0]
    d["review_json"] = json.loads(d["review_json"]) if d["review_json"] else {}
    return d


def fo_performance() -> dict:
    """Overall + by-year Long/Short Top-10 hit rates from stored outcomes."""
    rows = _rows(
        "SELECT substr(prediction_date,1,4) y, direction, "
        "AVG(hit)*10.0 avg_hits, COUNT(*) n "
        "FROM fo_prediction_outcomes GROUP BY y, direction ORDER BY y, direction")
    return {"by_year_direction": rows}


# ── Long-Term ───────────────────────────────────────────────────────────────---

def lt_latest_date() -> Optional[str]:
    r = _rows("SELECT MAX(prediction_date) d FROM lt_daily_predictions")
    return r[0]["d"] if r else None


def lt_predictions(date: Optional[str] = None) -> dict:
    date = date or lt_latest_date()
    if not date:
        return {"date": None, "picks": []}
    rows = _rows(
        "SELECT rank,symbol,sector,lt_score,top_features FROM lt_daily_predictions "
        "WHERE prediction_date=? ORDER BY rank", (date,))
    for r in rows:
        r["top_features"] = json.loads(r["top_features"]) if r["top_features"] else {}
    return {"date": date, "picks": rows}


def lt_performance() -> dict:
    rows = _rows(
        "SELECT substr(prediction_date,1,4) y, "
        "AVG(in_top10_4wk)*10.0 hit4_of10, AVG(in_top10_8wk)*10.0 hit8_of10, "
        "AVG(ret_4wk) avg_ret_4wk, AVG(ret_8wk) avg_ret_8wk, COUNT(*) n "
        "FROM lt_prediction_outcomes GROUP BY y ORDER BY y")
    return {"by_year": rows}


# ── learning queue ──────────────────────────────────────────────────────────---

def active_rules(persona: Optional[str] = None) -> List[dict]:
    sql = "SELECT * FROM learning_proposals WHERE status='ACTIVE'"
    params = ()
    if persona:
        sql += " AND persona=?"
        params = (persona,)
    sql += " ORDER BY hit_rate DESC"
    return _rows(sql, params)


def pending_proposals(persona: Optional[str] = None) -> List[dict]:
    sql = "SELECT * FROM learning_proposals WHERE human_approved=0"
    params = ()
    if persona:
        sql += " AND persona=?"
        params = (persona,)
    return _rows(sql + " ORDER BY week_ending DESC, hit_rate DESC", params)
