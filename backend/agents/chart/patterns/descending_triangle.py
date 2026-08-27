"""Chart Agent · Pattern: Descending Triangle (falling top + flat bottom, converging). BEARISH.
Breakdown below the flat support -> short. Geometry engine: patterns/_geometry.py."""
from __future__ import annotations
from ._geometry import SlopedDetector
from . import registry


class DescendingTriangleDetector(SlopedDetector):
    pattern_id = "descending_triangle"
    name = "Descending Triangle (falling top, flat bottom)"
    breakout_side = "lower"
    direction = "short"


registry.register(DescendingTriangleDetector())
