"""Chart Agent · Pattern: Symmetrical Triangle (falling top + rising bottom, converging). EITHER.
Whichever line breaks first sets direction+level. Geometry engine: patterns/_geometry.py."""
from __future__ import annotations
from ._geometry import SlopedDetector
from . import registry


class SymmetricalTriangleDetector(SlopedDetector):
    pattern_id = "symmetrical_triangle"
    name = "Symmetrical Triangle (converging, direction from the break)"
    breakout_side = "either"
    direction = None


registry.register(SymmetricalTriangleDetector())
