"""Chart Agent · Pattern: Falling Wedge (both lines down, converging). BULLISH.
Breakout above the falling resistance -> long. Geometry engine: patterns/_geometry.py."""
from __future__ import annotations
from ._geometry import SlopedDetector
from . import registry


class FallingWedgeDetector(SlopedDetector):
    pattern_id = "falling_wedge"
    name = "Falling Wedge (both lines down, converging)"
    breakout_side = "upper"
    direction = "long"


registry.register(FallingWedgeDetector())
