"""/api/falcon/patterns/* — promoted pattern browser.

Reads from falcon_promoted_patterns × falcon_pattern_candidates.
"""
from typing import List, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from ..db import falcon_conn

router = APIRouter()


class PromotedPattern(BaseModel):
    pattern_id:           int
    classification:       str
    mined_year:           str
    scope:                str
    outcome_target:       str
    n_obs:                int
    precision_pct:        float
    base_rate_pct:        float
    is_lift_pp:           float
    avg_oos_year_lift_pp: float
    n_years_passed:       int
    avg_cross_sector_lift_pp: float
    rule_text:            str


@router.get("/falcon/patterns", response_model=List[PromotedPattern])
def list_patterns(
    limit: int = Query(100, ge=1, le=1000),
    classification: Optional[str] = Query(None),
    target: Optional[str] = Query(None),
    min_oos_lift: float = Query(0.0),
    sort: str = Query("oos_lift", pattern="^(oos_lift|is_lift|n_obs)$"),
):
    """Browse promoted patterns. Defaults to top-100 by avg OOS year lift."""
    where = ["1=1"]
    args: list = []
    if classification:
        where.append("p.classification = ?"); args.append(classification)
    if target:
        where.append("c.outcome_target = ?"); args.append(target)
    where.append("p.avg_oos_year_lift_pp >= ?"); args.append(min_oos_lift)

    sort_col = {
        "oos_lift": "p.avg_oos_year_lift_pp DESC",
        "is_lift":  "c.lift_pct DESC",
        "n_obs":    "c.n_obs DESC",
    }[sort]

    args.append(limit)
    with falcon_conn() as con:
        rows = con.execute(f"""
            SELECT c.pattern_id, p.classification, c.mined_year, c.scope,
                   c.outcome_target, c.n_obs, c.precision_pct, c.base_rate_pct,
                   c.lift_pct AS is_lift_pp,
                   p.avg_oos_year_lift_pp, p.n_years_passed,
                   p.avg_cross_sector_lift_pp, c.rule_text
            FROM falcon_promoted_patterns p
            INNER JOIN falcon_pattern_candidates c ON p.pattern_id = c.pattern_id
            WHERE {' AND '.join(where)}
            ORDER BY {sort_col} LIMIT ?""", args).fetchall()
    return [PromotedPattern(**dict(r)) for r in rows]


@router.get("/falcon/patterns/stats")
def pattern_stats():
    """Counts by classification and outcome target."""
    with falcon_conn() as con:
        by_class = con.execute("""
            SELECT classification, COUNT(*) AS n
            FROM falcon_promoted_patterns GROUP BY classification
        """).fetchall()
        by_target = con.execute("""
            SELECT c.outcome_target, COUNT(*) AS n
            FROM falcon_promoted_patterns p
            INNER JOIN falcon_pattern_candidates c ON p.pattern_id = c.pattern_id
            GROUP BY c.outcome_target
        """).fetchall()
        by_year = con.execute("""
            SELECT c.mined_year, COUNT(*) AS n
            FROM falcon_promoted_patterns p
            INNER JOIN falcon_pattern_candidates c ON p.pattern_id = c.pattern_id
            GROUP BY c.mined_year
        """).fetchall()
        total = con.execute("SELECT COUNT(*) FROM falcon_promoted_patterns").fetchone()[0]
    return {
        "total_promoted": total,
        "by_classification": [dict(r) for r in by_class],
        "by_target":         [dict(r) for r in by_target],
        "by_mined_year":     [dict(r) for r in by_year],
    }
