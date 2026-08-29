"""Chart Agent · Pattern: Rising Wedge (both lines up, converging). BEARISH.
Breakdown below the rising support -> short. Geometry engine: patterns/_geometry.py."""
from __future__ import annotations
from ._geometry import SlopedDetector
from . import registry


class RisingWedgeDetector(SlopedDetector):
    pattern_id = "rising_wedge"
    name = "Rising Wedge (both lines up, converging)"
    breakout_side = "lower"
    direction = "short"


registry.register(RisingWedgeDetector())
