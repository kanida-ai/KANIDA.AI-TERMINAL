"""
Chart Agent · MARKET STORY (breadth lines for the left column).

Best-effort, REAL, computed straight from the day's scan setups — never fabricated. Two families:

  BREADTH  (always computable from the scan itself): total setups, breakout/approaching/retest/failed
           counts, bullish-vs-bearish split, and the per-pattern tally. These need nothing but the
           setups list.

  SECTOR   (computable ONLY if a symbol->sector source exists): concentration of today's setups by
           sector. We use data.sector_map() (the REAL instrument_labels.sector table in kanida.db).
           If NO sector source is available the sector lines are returned as ``available: false`` with
           an honest note — we do NOT invent sector membership.

Symbols with no sector mapping are counted under ``unmapped`` so the concentration percentages are
honest about coverage (the local source maps ~500 of ~1560 symbols).
"""
from __future__ import annotations
from collections import Counter
from typing import Optional

from . import data


def _pct(n: int, total: int) -> float:
    return round(100.0 * n / total, 1) if total else 0.0


def market_story(setups: list, as_of_date: Optional[str] = None) -> dict:
    """Breadth + (best-effort) sector concentration for a scan's setups list. Guarded."""
    setups = setups or []
    total = len(setups)
    stages = Counter((s.get("stage") or "?") for s in setups)
    dirs = Counter((s.get("direction") or "long") for s in setups)
    by_pattern = Counter((s.get("pattern") or "?") for s in setups)

    lines = [f"{total} chart setups on {as_of_date or 'the day'}."]
    if total:
        lines.append(f"{stages.get('BREAKOUT', 0)} breakouts, {stages.get('APPROACHING', 0)} approaching, "
                     f"{stages.get('RETEST', 0)} retests, {stages.get('FAILED', 0)} failed.")
        lines.append(f"{dirs.get('long', 0)} bullish vs {dirs.get('short', 0)} bearish.")
        top = by_pattern.most_common(3)
        if top:
            lines.append("Most common: " + ", ".join(
                f"{p.replace('_', ' ')} ({c})" for p, c in top))

    breadth = {
        "total": total,
        "by_stage": dict(stages),
        "bullish": dirs.get("long", 0),
        "bearish": dirs.get("short", 0),
        "by_pattern": dict(by_pattern),
        "lines": lines,
    }

    # --- SECTOR concentration (only if a real source exists) -----------------------------------
    smap = {}
    try:
        smap = data.sector_map()
    except Exception:  # noqa: BLE001
        smap = {}
    if not smap:
        sector = {"available": False, "lines": [],
                  "note": ("no symbol->sector source on the active data source — sector concentration "
                           "unavailable (not fabricated).")}
    else:
        sec_counts = Counter()
        unmapped = 0
        for s in setups:
            sec = smap.get((s.get("symbol") or "").upper())
            if sec:
                sec_counts[sec] += 1
            else:
                unmapped += 1
        mapped = total - unmapped
        sec_lines = []
        for sec, c in sec_counts.most_common(5):
            sec_lines.append(f"{sec} {_pct(c, mapped)}% ({c})")
        sector = {
            "available": True,
            "mapped": mapped, "unmapped": unmapped,
            "source": "instrument_labels.sector",
            "by_sector": dict(sec_counts.most_common()),
            "lines": ([f"Sector mix of {mapped} mapped setups: " + "; ".join(sec_lines)]
                      if sec_lines else []),
            "note": (f"{unmapped}/{total} setups have no sector mapping on the active source — "
                     f"percentages are over the {mapped} mapped setups only." if unmapped else None),
        }

    return {"as_of_date": as_of_date, "breadth": breadth, "sector": sector}
