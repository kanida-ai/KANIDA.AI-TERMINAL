"""
Chart Agent · Pattern: Channel (parallel trend channel, up/down/flat), daily.  [SPEC]

SKELETON — registers so the multi-pattern structure is real and extensible, but returns no
occurrences yet. Implementing it requires only this module (detector + signature); the shared
matcher, evidence, gates, tracker and storyline are inherited (v3 §17).

v3 process to build (do NOT fabricate before it exists):
  §5.1 swing pivots (highs AND lows).
  §5.2 fit two PARALLEL trendlines (equal slope) enclosing price; governed slope model + a
       [SPEC] slope_max guard. A channel is the sloped generalisation of the flat horizontal
       level, so it explicitly needs the sloped-line machinery the horizontal detector omits.
  §5.4 stage rules: riding the channel, breakout beyond a boundary on volume, retest of the band.
  §6   signature: pattern=channel, direction/slope, width, position-in-band, touches, volume bucket.

Until then detect() honestly returns [].
"""
from __future__ import annotations
from typing import Optional
from .base import PatternDetector, PatternOccurrence
from . import registry


class ChannelDetector(PatternDetector):
    pattern_id = "channel"
    name = "Channel (parallel trend channel)"
    status = "spec"

    def detect(self, df, as_of_idx: Optional[int] = None) -> list[PatternOccurrence]:
        # TODO(v3 §5.1-§5.4): swing pivots -> two parallel trendline fit (governed slope model)
        #                      -> ride/breakout/retest stages. Point-in-time; entry = next open.
        return []


registry.register(ChannelDetector())
