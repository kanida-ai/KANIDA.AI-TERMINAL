"""Chart Agent · Pattern: Rectangle (flat top + flat bottom, parallel, non-converging). EITHER.
Range breakout — whichever boundary breaks first sets direction+level. Engine: patterns/_geometry.py."""
from __future__ import annotations
from ._geometry import SlopedDetector
from . import registry


class RectangleDetector(SlopedDetector):
    pattern_id = "rectangle"
    name = "Rectangle (parallel flat range, direction from the break)"
    breakout_side = "either"
    direction = None


registry.register(RectangleDetector())
