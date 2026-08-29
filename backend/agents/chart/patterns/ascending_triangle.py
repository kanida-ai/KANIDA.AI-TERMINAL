"""Chart Agent · Pattern: Ascending Triangle (flat top + rising bottom, converging). BULLISH.
Breakout above the flat resistance -> long. Geometry engine: patterns/_geometry.py."""
from __future__ import annotations
from ._geometry import SlopedDetector
from . import registry


class AscendingTriangleDetector(SlopedDetector):
    pattern_id = "ascending_triangle"
    name = "Ascending Triangle (flat top, rising bottom)"
    breakout_side = "upper"
    direction = "long"


registry.register(AscendingTriangleDetector())
