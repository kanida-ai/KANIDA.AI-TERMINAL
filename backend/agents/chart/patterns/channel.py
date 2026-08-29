"""Chart Agent · Pattern: Channel (parallel same-sign slopes, non-converging), up or down. EITHER.
Breakout beyond a boundary sets direction+level. Geometry engine: patterns/_geometry.py.

REPLACES the old [SPEC] skeleton — now a real detector built on the shared sloped-geometry engine."""
from __future__ import annotations
from ._geometry import SlopedDetector
from . import registry


class ChannelDetector(SlopedDetector):
    pattern_id = "channel"
    name = "Channel (parallel trend channel, direction from the break)"
    breakout_side = "either"
    direction = None


registry.register(ChannelDetector())
