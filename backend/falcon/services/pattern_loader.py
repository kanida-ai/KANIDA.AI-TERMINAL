"""Loads promoted patterns from DB. Lightweight — no sklearn or numpy here.

Returns rules in a form the runtime signal scorer can apply directly.
"""
import json
from typing import Dict, List, Optional, Set, Tuple

from ..config import DROPPED_FAMILIES
from ..db import falcon_conn


def classify_families(rule: List[Tuple[str, str, float]]) -> Set[str]:
    """Tag a rule with its feature families. Mirrors the offline classifier
    so V7.1 can drop drawdown_bounce consistently."""
    fams: Set[str] = set()
    for f, op, th in rule:
        if f == "weekly_close_loc"  and op == ">":     fams.add("weekly_close")
        if f == "atr_20_pct"        and op == ">":     fams.add("high_atr")
        if f in ("dist_high_252", "dist_high_120") and op == "<=" and th < -10:
            fams.add("drawdown_bounce")
        if f == "weekly_range_pct"  and op == ">":     fams.add("weekly_range")
    return fams


def load_promoted_patterns(
    exclude_families: Optional[List[str]] = None,
    include_classifications: Optional[List[str]] = None,
) -> List[Dict]:
    """Load promoted patterns from DB, optionally filtered.

    Defaults: exclude drawdown_bounce (V7.1), include universal+regime_dependent.
    """
    if exclude_families is None:
        exclude_families = list(DROPPED_FAMILIES)
    if include_classifications is None:
        include_classifications = ["universal", "regime_dependent"]
    excl = set(exclude_families)
    cls_filter = "(" + ",".join("?" * len(include_classifications)) + ")"

    with falcon_conn() as con:
        rows = con.execute(f"""
            SELECT c.pattern_id, c.mined_year, c.outcome_target, c.rule_json,
                   p.classification, p.avg_oos_year_lift_pp
            FROM falcon_promoted_patterns p
            INNER JOIN falcon_pattern_candidates c ON p.pattern_id = c.pattern_id
            WHERE p.classification IN {cls_filter}
            ORDER BY p.avg_oos_year_lift_pp DESC
        """, include_classifications).fetchall()

    out = []
    for r in rows:
        rule = [(f, op, th) for f, op, th in json.loads(r["rule_json"])]
        fams = classify_families(rule)
        if excl & fams:
            continue
        out.append({
            "pattern_id":     r["pattern_id"],
            "mined_year":     r["mined_year"],
            "target":         r["outcome_target"],
            "rule":           rule,
            "classification": r["classification"],
            "oos_lift":       r["avg_oos_year_lift_pp"],
            "families":       list(fams),
        })
    return out
