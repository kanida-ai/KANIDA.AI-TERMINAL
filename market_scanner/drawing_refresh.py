"""Refresh cached drawing payloads only; never detect patterns or alter their identity.

The scanner stores both original detection_bars and the final served bar window.
Their difference is an exact coordinate offset. NaN padding is internal only and
cannot become a drawable price or a served candle.
"""
from __future__ import annotations

import copy
import math

from . import pattern_lines

DRAWING_VERSION = 'geometry-guides-3'
UNAVAILABLE = 'Marker only: exact source-window coordinates for this drawing are unavailable.'
OUTSIDE = 'Marker only: required pattern anchors are outside the cached candle window.'


def _integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _indices(geometry):
    array_keys = {key for key, _, _ in pattern_lines.ANCHORS} | {'troughs', 'lead_in_highs', 'island_bars'}
    scalar_keys = {key for key, _, _ in pattern_lines.POINTS}
    for key in array_keys:
        values = geometry.get(key)
        if isinstance(values, (list, tuple)):
            for value in values:
                if _integer(value):
                    yield value
    for key in scalar_keys:
        if _integer(geometry.get(key)):
            yield geometry[key]
    for point in (geometry.get('points') or {}).values():
        if isinstance(point, dict) and _integer(point.get('index')):
            yield point['index']


def refresh_cell(cell):
    """Return a response copy with fresh non-legacy overlays; cached storage is untouched."""
    bars = cell.get('bars') or []
    original_n = cell.get('detection_bars')
    offset = original_n - len(bars) if _integer(original_n) and original_n >= len(bars) else None
    by_end = {bar.get('end'): i for i, bar in enumerate(bars)}
    missing = dict(open=math.nan, high=math.nan, low=math.nan, close=math.nan, gap=True)
    padded = [missing] * offset + bars if offset is not None else []
    matches = []
    for match in cell.get('matches') or []:
        if match.get('family') == 'chart' and str(match.get('variant', '')).startswith('legacy_'):
            matches.append(match)  # original ten overlays, including every byte of line metadata
            continue
        item = copy.deepcopy(match)
        item['drawing_version'] = DRAWING_VERSION
        matches.append(item)
        n_at_detection = (match.get('input') or {}).get('bars')
        if offset is None or (n_at_detection is not None and n_at_detection != original_n):
            item.update(lines=[], geometry_note=UNAVAILABLE)
            continue
        first, signal = match.get('start_index'), match.get('end_index')
        detected = by_end.get(match.get('detected_at_bar_end'))
        if not all(_integer(v) for v in (first, signal, detected)) or not (0 <= first <= detected <= signal < len(bars)):
            item.update(lines=[], geometry_note=UNAVAILABLE)
            continue
        geometry = copy.deepcopy(match.get('geometry') or {})
        if any(i < offset or i >= original_n or i > signal + offset for i in _indices(geometry)):
            item.update(lines=[], geometry_note=OUTSIDE)
            continue
        # Cached raw geometry is the setup anchor. Its original confirmed overlay
        # retains the exact published trigger; recover that scalar, never fit one.
        if match.get('detector_state') == 'confirmed':
            trigger = next((line for line in match.get('lines', []) if line.get('label') == 'Trigger level'), None)
            prices = [p.get('value') for p in (trigger or {}).get('points', [])]
            if prices and all(isinstance(v, (float, int)) and math.isfinite(v) and v == prices[0] for v in prices):
                if match.get('family') == 'price_action':
                    geometry.setdefault('confirmation_level', prices[0])
                elif match.get('family') == 'harmonic':
                    geometry.setdefault('confirmation', {}).setdefault('level', prices[0])
                else:
                    geometry.setdefault('confirm_level', prices[0])
        event = dict(score=match.get('fit_score', match.get('score', 0)), geometry=geometry,
                     detected_index=detected + offset, signal_index=signal + offset)
        lines, note = pattern_lines.build(match, event, padded,
            formation_start=first + offset, signal_index=signal + offset,
            tracking=match.get('tracking'))
        if any(p['index'] < offset or p['index'] > signal + offset for line in lines for p in line['points']):
            item.update(lines=[], geometry_note=OUTSIDE)
            continue
        item.update(lines=pattern_lines.rebase(lines, offset), geometry_note=note,
                    geometry_source_offset=offset, drawing_source='cached_frozen_geometry_and_served_candles')
    return dict(cell, matches=matches)
