"""
Chart Agent · Pattern: Triangle (ascending / descending / symmetrical), daily.  [SPEC]

SKELETON — registers so the multi-pattern structure is real and extensible, but returns no
occurrences yet. Implementing it requires only this module (detector + signature); the shared
matcher, evidence, gates, tracker and storyline are inherited (v3 §17).

v3 process to build (do NOT fabricate before it exists):
  §5.1 swing pivots (highs AND lows, strict local extremum, confirmed +L bars later)
  §5.2 fit the two converging trendlines (a resistance line + a support line) with a governed
       slope model; a Triangle needs a [SPEC] slope_max/convergence test (§5.2 note) — the
       horizontal detector deliberately clusters only FLAT tops, so sloped lines are a different
       detector, exactly as the playbook states.
  §5.4 stage rules: forming (inside the apex), breakout (close beyond a line on volume), retest.
  §6   signature: pattern=triangle, sub-type, slope pair, apex distance, touches, volume bucket.

Until then detect() honestly returns [].
"""
from __future__ import annotations
from typing import Optional
from .base import PatternDetector, PatternOccurrence
from . import registry


class TriangleDetector(PatternDetector):
    pattern_id = "triangle"
    name = "Triangle (asc/desc/symmetrical)"
    status = "spec"

    def detect(self, df, as_of_idx: Optional[int] = None) -> list[PatternOccurrence]:
        # TODO(v3 §5.1-§5.4): swing pivots -> converging trendline fit (governed slope model)
        #                      -> forming/breakout/retest stages. Point-in-time; entry = next open.
        return []


registry.register(TriangleDetector())
