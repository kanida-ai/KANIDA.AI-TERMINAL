"""Drawable overlays for the research detector set.

The app draws a pattern from the match's ``lines`` — the shape the legacy ten
chart detectors have always produced (``market_scanner/detectors.py``:
``segment(label, indices, values, role)``) and that
``kanida-app/src/patternGeometry.ts`` + ``PatternCanvas.tsx`` consume::

    {"label": "Resistance", "role": "boundary", "points": [{"index": 12, "value": 1477.5}, ...]}

Roles the canvas understands:

``boundary``  a level or trendline, drawn green and listed in the legend readout
              (``boundaryValuesAt``).  Two of them labelled *Resistance* and
              *Support* also produce the shaded envelope.
``curve``     a fitted curve, drawn blue.
``anchors``   the pivots the detector actually used; drawn as circles.
``shape``     a polyline through the formation.
``label``     a single point that becomes a text label on the chart.
``candle_range`` a rectangle around named candles: first point is (start, low),
              second point is (end, high); same-index points cover one candle.

This module translates each research family's own ``geometry`` into that shape.
It **invents no structure**: every price it draws is either one the detector
published, or a high/low of a bar the detector named (documented per rule
below), or the live layer's own tracking level — which is labelled as such.

Contract this module keeps
--------------------------
* ``build`` always returns ``(lines, note)``.  ``note`` is non-empty **exactly
  when** ``lines`` is empty, so a pattern with no drawable structure says
  "marker only" instead of rendering a silent blank.
* Every point index is an integer inside the served window, and every value is
  finite.  Points are dropped only together with their whole line, never
  silently thinned, so a two-point boundary can never become a one-point stub.
* ``CH01``-``CH10`` (the legacy adapter) publish no geometry at all — the
  research event carries only ``pattern_start``.  Their overlays are recovered
  by re-running the *same* legacy replay the adapter itself runs and taking that
  match's own ``lines``; the recovered match is accepted only when its pattern
  and score match the event, otherwise a note is returned.  Nothing is guessed.
"""
from __future__ import annotations

import math

#: Y value for an anchor index the detector named without publishing a price.
#: 'high'/'low' take that bar's extreme, None takes its close.  This is a
#: drawing choice, recorded here so it is never mistaken for detector output.
ANCHOR_HIGH, ANCHOR_LOW, ANCHOR_CLOSE = 'high', 'low', 'close'

# --------------------------------------------------------------- generic tables
#: geometry key -> legend label, for a published horizontal price level.
LEVELS = (
    ('resistance', 'Resistance'),
    ('support', 'Support'),
    ('neckline', 'Neckline'),
    ('rim_level', 'Rim resistance'),
    ('base_boundary', 'Base boundary'),
    ('base_high', 'Base high'),
    ('breakout_level', 'Breakout level'),
    ('retracement_level', 'Retracement level'),
    ('projection_target', 'Projection target'),
    ('island_extreme_edge', 'Island edge'),
    ('pair_high', 'Pair high'),
    ('pair_low', 'Pair low'),
    ('mother_high', 'Mother bar high'),
    ('mother_low', 'Mother bar low'),
    ('nr_high', 'Narrow range high'),
    ('nr_low', 'Narrow range low'),
    ('prior_high', 'Prior candle high'),
    ('prior_low', 'Prior candle low'),
    ('breach_extreme', 'Breach extreme'),
    ('handle_failure_level', 'Handle failure level'),
)

#: geometry key -> legend label, for a published fitted line ``[j0, y0, slope]``.
SLOPED = (
    ('support_line', 'Support'),
    ('upper_line', 'Resistance'),
    ('lower_line', 'Support'),
    ('upper_right_line', 'Resistance'),
    ('lower_right_line', 'Support'),
    ('trendline', 'Trendline'),
)

#: geometry key -> (label, which extreme of the named bar to draw at).
ANCHORS = (
    ('resistance_touches', 'Resistance tests', ANCHOR_HIGH),
    ('support_touches', 'Support tests', ANCHOR_LOW),
    ('support_troughs', 'Support troughs', ANCHOR_LOW),
    ('upper_anchors', 'Upper pivots', ANCHOR_HIGH),
    ('lower_anchors', 'Lower pivots', ANCHOR_LOW),
    ('highs', 'Upper pivots', ANCHOR_HIGH),
    ('lows', 'Lower pivots', ANCHOR_LOW),
    ('pullback_highs', 'Pullback highs', ANCHOR_HIGH),
    ('pullback_lows', 'Pullback lows', ANCHOR_LOW),
    ('lead_in_lows', 'Lead-in lows', ANCHOR_LOW),
    ('intervening_extremes', 'Intervening extremes', ANCHOR_CLOSE),
    ('anchors', 'Turning points', ANCHOR_CLOSE),
)

#: geometry key -> (label, extreme) for a single named bar shown as a text label.
POINTS = (
    ('pole_start', 'Pole start', ANCHOR_CLOSE),
    ('pole_top', 'Pole top', ANCHOR_CLOSE),
    ('leg_start', 'Leg start', ANCHOR_CLOSE),
    ('leg_end', 'Leg end', ANCHOR_CLOSE),
    ('correction_end', 'Correction end', ANCHOR_CLOSE),
    ('decline_start', 'Move start', ANCHOR_CLOSE),
    ('extreme', 'Extreme', ANCHOR_CLOSE),
    ('bowl_extreme', 'Bowl extreme', ANCHOR_CLOSE),
    ('dome_extreme', 'Dome extreme', ANCHOR_CLOSE),
    ('bump_peak', 'Bump peak', ANCHOR_HIGH),
    ('left_rim', 'Left rim', ANCHOR_HIGH),
    ('right_rim', 'Right rim', ANCHOR_HIGH),
    ('handle_extreme', 'Handle', ANCHOR_CLOSE),
    ('intervening_extreme', 'Intervening extreme', ANCHOR_CLOSE),
    ('mother_index', 'Mother bar', ANCHOR_CLOSE),
    ('setup_index', 'Setup bar', ANCHOR_CLOSE),
    ('first_gap_bar', 'First gap', ANCHOR_CLOSE),
    ('second_gap_bar', 'Second gap', ANCHOR_CLOSE),
    ('breach_index', 'False break', ANCHOR_CLOSE),
    ('failure_index', 'Failure close', ANCHOR_CLOSE),
    ('mother_candidate_index', 'Mother bar', ANCHOR_CLOSE),
)

#: ``troughs``/``trough_levels`` come as a pair: indices with their own prices.
LEVELLED_ANCHORS = (('troughs', 'trough_levels', 'Turning points'),)

#: The declared plan per family, for the coverage test and ``/api/state``.
FAMILY_PLAN = {
    'chart': 'published boundaries, fitted lines, pivot anchors, trigger and failure levels',
    'harmonic': 'XABCD legs with point and ratio labels, PRZ band, trigger and failure levels',
    'price_action': 'published level pair (or the marked candle range) plus the confirmation level',
    'candlestick': 'marked candle range plus the trigger level and the tracked failure level',
}
LEGACY_PLAN = 'legacy replay overlay recovered from the original detector'


def plan_for(spec):
    """The declared drawing recipe for one registered cell. Never empty."""
    if spec['family'] == 'chart' and str(spec['variant']).startswith('legacy_'):
        return LEGACY_PLAN
    return FAMILY_PLAN[spec['family']]


# ------------------------------------------------------------------- primitives
def _finite(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _index(value, n):
    """A geometry index as an int inside the window, or None."""
    try:
        i = int(round(float(value)))
    except (TypeError, ValueError):
        return None
    return i if 0 <= i < n else None


def _point(index, value):
    return {'index': int(index), 'value': round(float(value), 6)}


def _bar_value(bars, i, extreme):
    bar = bars[i]
    return bar['high'] if extreme == ANCHOR_HIGH else bar['low'] if extreme == ANCHOR_LOW else bar['close']


def _level(label, value, first, last, role='boundary'):
    value = _finite(value)
    if value is None or last <= first:
        return None
    return {'label': label, 'role': role,
            'points': [_point(first, value), _point(last, value)]}


def _sloped(label, line, first, last, bars):
    """A published ``[j0, y0, slope]`` fit drawn across the formation."""
    try:
        j0, y0, slope = (float(x) for x in line[:3])
    except (TypeError, ValueError, IndexError):
        return None
    if not all(math.isfinite(x) for x in (j0, y0, slope)) or last <= first:
        return None
    a, b = y0 + slope * (first - j0), y0 + slope * (last - j0)
    if not (math.isfinite(a) and math.isfinite(b)):
        return None
    return {'label': label, 'role': 'boundary',
            'points': [_point(first, a), _point(last, b)]}


def _anchors(label, indices, bars, extreme, levels=None):
    n = len(bars)
    points = []
    for position, raw in enumerate(indices or ()):
        i = _index(raw, n)
        if i is None:
            return None                      # a named bar outside the window: draw nothing
        value = None
        if levels is not None and position < len(levels):
            value = _finite(levels[position])
        if value is None:
            value = _finite(_bar_value(bars, i, extreme))
        if value is None:
            return None
        points.append(_point(i, value))
    return {'label': label, 'role': 'anchors', 'points': points} if points else None


def _shape(label, indices, bars, extreme=ANCHOR_CLOSE):
    n = len(bars)
    points = []
    for raw in indices:
        i = _index(raw, n)
        if i is None:
            return None
        value = _finite(_bar_value(bars, i, extreme))
        if value is None:
            return None
        points.append(_point(i, value))
    return {'label': label, 'role': 'shape', 'points': points} if len(points) > 1 else None


def _text(label, index, bars, extreme=ANCHOR_CLOSE, value=None):
    i = _index(index, len(bars))
    if i is None:
        return None
    value = _finite(value if value is not None else _bar_value(bars, i, extreme))
    if value is None:
        return None
    return {'label': label, 'role': 'label', 'points': [_point(i, value)]}


def _add(lines, line):
    if line and line['points']:
        lines.append(line)


# ------------------------------------------------------------------- per family
def _tracking_lines(tracking, first, last):
    """The live layer's own tracking level, labelled as the live rule when it is.

    The detector-published boundary is already drawn from ``geometry``; this adds
    the level the *lifecycle* actually tested, so the chart shows what
    invalidated (or would invalidate) the setup."""
    if not isinstance(tracking, dict):
        return []
    level = _finite(tracking.get('failure_level'))
    if level is None:
        return []
    label = ('Failure level' if tracking.get('failure_level_source') == 'detector'
             else 'Failure level (live rule)')
    line = _level(label, level, first, last)
    return [line] if line else []


def _named_shape(label, nodes, bars):
    """Join only detector-named extrema; this is a structure guide, not a fitted curve."""
    points = []
    for index, extreme, value in nodes:
        i = _index(index, len(bars))
        if i is None:
            return None
        y = _finite(value if value is not None else _bar_value(bars, i, extreme))
        if y is None:
            return None
        points.append(_point(i, y))
    if len(points) < 2:
        return None
    return dict(label=label, role='shape', points=points, source='named_extrema_structure_guide')


def _range(label, first, last, low, high):
    low, high = _finite(low), _finite(high)
    if low is None or high is None or low > high or last < first:
        return None
    return dict(label=label, role='candle_range', points=[_point(first, low), _point(last, high)])


def _chart_lines(geometry, bars, first, last, spec=None):
    spec = spec or {}
    pid, side = spec.get('pattern_id'), spec.get('side', 'long')
    geometry = dict(geometry)  # mirrored interpretation must never mutate the frozen event
    # These keys are in the detector's mirrored coordinate system; its prices
    # and fitted lines are already real prices, but its pivot index arrays are not renamed.
    if side == 'short' and pid in ('CH14', 'CH28'):
        for a, b in [('upper_anchors', 'lower_anchors'), ('pullback_highs', 'pullback_lows')]:
            if a in geometry and b in geometry:
                geometry[a], geometry[b] = geometry[b], geometry[a]
    if pid == 'CH27' and side == 'long' and 'lead_in_lows' in geometry:
        geometry['lead_in_highs'] = geometry.pop('lead_in_lows')
    lines = []
    for key, label in SLOPED:
        start = first
        # A fitted line's j0 may be its mean x, not the first supporting pivot.
        # Its left domain therefore comes from the named fit anchors, never from
        # the entire formation (which can start much earlier on the other side).
        anchors_key = {'support_line': 'support_troughs',
                       'upper_line': 'upper_anchors',
                       'lower_line': 'lower_anchors',
                       'trendline': 'lead_in_highs' if side == 'long' and pid == 'CH27' else 'lead_in_lows'}.get(key)
        domain = geometry.get(anchors_key) if anchors_key else None
        if domain:
            start = max(first, min(domain))
        elif pid == 'CH14' and key in ('upper_line', 'lower_line'):
            start = geometry.get('pole_top', first)
        if pid == 'CH23' and key in ('upper_right_line', 'lower_right_line'):
            pivots = geometry.get('highs' if key == 'upper_right_line' else 'lows') or []
            start = pivots[1] if len(pivots) >= 2 else last
        if key in geometry:
            _add(lines, _sloped(label, geometry[key], start, last, bars))
    for key, label in LEVELS:
        if key in geometry:
            if pid in ('CH17', 'CH18') and key == 'resistance':
                label = 'Neckline'
            elif pid == 'CH20' and key == 'rim_level':
                label = 'Rim support'
            _add(lines, _level(label, geometry[key], first, last))
    for key, levels_key, label in LEVELLED_ANCHORS:
        if key in geometry:
            _add(lines, _anchors(label, geometry.get(key), bars, ANCHOR_CLOSE,
                                 geometry.get(levels_key)))
    for key, label, extreme in ANCHORS:
        if key in geometry:
            if key == 'anchors' and pid in ('CH22', 'CH23'):
                continue  # upper/lower arrays already supply the actual pivot prices
            if key == 'intervening_extremes' and pid in ('CH17', 'CH18'):
                extreme = ANCHOR_LOW if side == 'short' else ANCHOR_HIGH
            _add(lines, _anchors(label, geometry.get(key), bars, extreme))
    if geometry.get('lead_in_highs'):
        _add(lines, _anchors('Lead-in highs', geometry['lead_in_highs'], bars, ANCHOR_HIGH))
    high, low = (ANCHOR_LOW, ANCHOR_HIGH) if side == 'short' else (ANCHOR_HIGH, ANCHOR_LOW)
    # Horizontal/diagonal formations: connect only the actual alternating touches.
    pairs = [('resistance_touches', ANCHOR_HIGH), ('support_troughs', ANCHOR_LOW)] if pid == 'CH11' else \
            [('resistance_touches', ANCHOR_HIGH), ('support_touches', ANCHOR_LOW)] if pid == 'CH13' else \
            [('upper_anchors', ANCHOR_HIGH), ('lower_anchors', ANCHOR_LOW)] if pid in ('CH14', 'CH22') else \
            [('pullback_highs', ANCHOR_HIGH), ('pullback_lows', ANCHOR_LOW)] if pid == 'CH28' else []
    nodes = sorted([(i, extreme, None) for key, extreme in pairs for i in geometry.get(key, [])])
    if nodes:
        _add(lines, _named_shape('Pivot structure guide', nodes, bars))
    if pid == 'CH14' and all(k in geometry for k in ('pole_start', 'pole_top')):
        _add(lines, _named_shape('Pole', [(geometry['pole_start'], low, None), (geometry['pole_top'], high, None)], bars))
    if pid in ('CH15', 'CH16', 'CH17', 'CH18'):
        turns = geometry.get('troughs') or []
        levels = geometry.get('trough_levels') or []
        middle = geometry.get('intervening_extremes') or ([geometry['intervening_extreme']] if 'intervening_extreme' in geometry else [])
        nodes = []
        for j, i in enumerate(turns):
            nodes.append((i, low, levels[j] if j < len(levels) else None))
            if j < len(middle):
                nodes.append((middle[j], high, None))
        _add(lines, _named_shape('Double-turn structure guide' if len(turns) == 2 else 'Triple-turn structure guide', nodes, bars))
    if pid in ('CH19', 'CH20') and all(k in geometry for k in ('left_rim', 'bowl_extreme')):
        _add(lines, _named_shape('Rounding structure guide (not a fitted curve)',
             [(geometry['left_rim'], high, geometry.get('rim_level')), (geometry['bowl_extreme'], low, None), (last, ANCHOR_CLOSE, None)], bars))
    if pid == 'CH21' and all(k in geometry for k in ('left_rim', 'dome_extreme', 'right_rim')):
        _add(lines, _named_shape('Inverted cup structure guide (not a fitted curve)',
             [(geometry['left_rim'], ANCHOR_LOW, None), (geometry['dome_extreme'], ANCHOR_HIGH, None), (geometry['right_rim'], ANCHOR_LOW, None)], bars))
        if 'handle_extreme' in geometry:
            _add(lines, _named_shape('Handle structure guide', [(geometry['right_rim'], ANCHOR_LOW, None),
                (geometry['handle_extreme'], ANCHOR_HIGH, None), (last, ANCHOR_CLOSE, None)], bars))
    if pid == 'CH23':
        for key, label, extreme in [('highs', 'Diamond upper structure guide', ANCHOR_HIGH), ('lows', 'Diamond lower structure guide', ANCHOR_LOW)]:
            _add(lines, _named_shape(label, [(i, extreme, None) for i in geometry.get(key, [])], bars))
    if pid == 'CH24' and all(k in geometry for k in ('first_gap_bar', 'second_gap_bar')):
        a, b = _index(geometry['first_gap_bar'], len(bars)), _index(geometry['second_gap_bar'], len(bars))
        if a is not None and b is not None and 0 < a < b <= last:
            island = bars[a:b]
            _add(lines, _range('Island candles', a, b - 1, min(x['low'] for x in island), max(x['high'] for x in island)))
            for i, label in [(a, 'First price gap'), (b, 'Second price gap')]:
                before, after = bars[i - 1], bars[i]
                lo_gap, hi_gap = (before['high'], after['low']) if after['low'] > before['high'] else (after['high'], before['low'])
                if hi_gap > lo_gap:
                    _add(lines, _range(label, i - 1, i, lo_gap, hi_gap))
    if pid == 'CH25' and all(k in geometry for k in ('decline_start', 'extreme')):
        _add(lines, _named_shape('Reversal structure guide', [(geometry['decline_start'], high, None), (geometry['extreme'], low, None), (last, ANCHOR_CLOSE, None)], bars))
    if pid == 'CH26' and all(k in geometry for k in ('leg_start', 'leg_end', 'correction_end')):
        _add(lines, _named_shape('Measured leg', [(geometry['leg_start'], low, None), (geometry['leg_end'], high, None), (geometry['correction_end'], low, None)], bars))
    if pid == 'CH27' and 'bump_peak' in geometry:
        key, lead, peak = ('lead_in_highs', ANCHOR_HIGH, ANCHOR_LOW) if side == 'long' else ('lead_in_lows', ANCHOR_LOW, ANCHOR_HIGH)
        _add(lines, _named_shape('Bump structure guide', [(i, lead, None) for i in geometry.get(key, [])] + [(geometry['bump_peak'], peak, None)], bars))
    for key, label, extreme in POINTS:
        if key in geometry:
            if key in ('pole_start', 'leg_start', 'correction_end') and pid in ('CH14', 'CH26'):
                extreme = low
            elif key in ('pole_top', 'leg_end') and pid in ('CH14', 'CH26'):
                extreme = high
                if key == 'pole_top':
                    label = 'Pole low' if side == 'short' else 'Pole high'
            elif key == 'intervening_extreme' and pid in ('CH15', 'CH16'):
                extreme = high
            elif key == 'left_rim' and pid in ('CH19', 'CH20'):
                extreme = high
            elif key == 'bowl_extreme' and pid in ('CH19', 'CH20'):
                extreme = low
                label = 'Dome extreme' if pid == 'CH20' else 'Bowl extreme'
            elif pid == 'CH21':
                extreme = ANCHOR_LOW if key in ('left_rim', 'right_rim') else ANCHOR_HIGH
            elif key == 'decline_start' and pid == 'CH25':
                extreme = high
            elif key == 'extreme' and pid == 'CH25':
                extreme = low
            elif key == 'bump_peak' and pid == 'CH27':
                extreme = ANCHOR_LOW if side == 'long' else ANCHOR_HIGH
                label = 'Bump trough' if side == 'long' else 'Bump peak'
            _add(lines, _text(label, geometry[key], bars, extreme))
    if 'failure_level' in geometry:
        # The detector publishes the raw structural level; its close-based
        # failure rule also applies the frozen ATR buffer. Do not call it a stop.
        _add(lines, _level('Failure reference', geometry['failure_level'], first, last))
    if 'confirm_level' in geometry:
        _add(lines, _level('Trigger level', geometry['confirm_level'], first, last))
    return lines


def _harmonic_lines(geometry, bars, first, last):
    points = geometry.get('points')
    if not isinstance(points, dict):
        return []
    n = len(bars)
    ordered = []
    for label, node in points.items():
        if not isinstance(node, dict):
            continue
        i = _index(node.get('index'), n)
        price = _finite(node.get('price'))
        if i is None or price is None:
            return []                       # a leg we cannot place: draw nothing, say so
        ordered.append((i, str(label), price))
    ordered.sort()
    if len(ordered) < 3:
        return []
    lines = [{'label': 'XABCD legs', 'role': 'shape',
              'points': [_point(i, price) for i, _label, price in ordered]}]
    _add(lines, {'label': 'Harmonic points', 'role': 'anchors',
                 'points': [_point(i, price) for i, _label, price in ordered]})
    for i, label, price in ordered:
        _add(lines, _text(label, i, bars, value=price))
    # Ratio labels sit at the midpoint of the leg they measure (the numerator).
    position = {label: (i, price) for i, label, price in ordered}
    for name, value in (geometry.get('ratios') or {}).items():
        ratio = _finite(value)
        numerator = str(name).split('/')[0].replace('time_', '')
        if ratio is None or len(numerator) != 2:
            continue
        a, b = position.get(numerator[0]), position.get(numerator[1])
        if not a or not b:
            continue
        _add(lines, _text(f'{name} {ratio:.2f}', (a[0] + b[0]) // 2, bars,
                          value=(a[1] + b[1]) / 2))
    band = (geometry.get('prz') or {}).get('price_band')
    if isinstance(band, (list, tuple)) and len(band) == 2:
        _add(lines, _level('PRZ low', min(band), first, last))
        _add(lines, _level('PRZ high', max(band), first, last))
    confirmation = geometry.get('confirmation')
    if isinstance(confirmation, dict):
        _add(lines, _level('Trigger level', confirmation.get('level'), first, last))
        _add(lines, _level('Failure level', confirmation.get('failure_level'), first, last))
    return lines


def _price_action_lines(geometry, bars, first, last, marked, spec=None):
    lines = []
    spec = spec or {}
    pid, side = spec.get('pattern_id'), spec.get('side')
    window = bars[marked[0]:marked[1] + 1]
    if window:
        label = 'Range comparison candles' if pid == 'PA05' else 'Pattern candles'
        _add(lines, _range(label, marked[0], marked[1],
                          min(b['low'] for b in window), max(b['high'] for b in window)))
        recognition = {'PA01': 'Tweezers recognized', 'PA02': 'Inside range recognized',
                       'PA03': 'Outside bar', 'PA04': 'Pin bar', 'PA05': 'Narrowest bar',
                       'PA06': 'Failed break recognized', 'PA07': 'Window recognized',
                       'PA08': 'Reversal close'}.get(pid, 'Pattern recognized')
        _add(lines, _text(recognition, marked[1], bars))
        if last > marked[1]:
            _add(lines, _text('Confirmation', last, bars))
    # These pairs are named by the frozen event. Verify the actual gap and its
    # published size before drawing it; an opening gap is not a full-range void.
    if pid in ('PA07', 'PA08') and marked[1] == marked[0] + 1:
        prior, current = bars[marked[0]], bars[marked[1]]
        if pid == 'PA07':
            lo, hi = (prior['high'], current['low']) if side == 'long' else (current['high'], prior['low'])
            size, label = geometry.get('window_size'), 'Full-range window'
        else:
            lo, hi = (current['open'], prior['low']) if side == 'long' else (prior['high'], current['open'])
            size, label = geometry.get('opening_gap'), 'Opening gap (filled intrabar)'
        size = _finite(size)
        if size is not None and hi > lo and math.isclose(hi - lo, size, rel_tol=1e-7, abs_tol=1e-6):
            _add(lines, _range(label, marked[0], marked[1], lo, hi))
            _add(lines, _level(label + ' lower edge', lo, marked[0], marked[1]))
            _add(lines, _level(label + ' upper edge', hi, marked[0], marked[1]))
            if pid == 'PA08':
                _add(lines, _text('Gapped open', marked[1], bars, value=current['open']))
    published = False
    for key, label in LEVELS:
        if key in geometry:
            line = _level(label, geometry[key], first, last)
            if line:
                lines.append(line)
                published = True
    if not published:
        # PA03/PA04/PA07 publish no level pair. The marked candles are the shape,
        # so their own range is drawn -- taken from the bars the detector named
        # (`marked`), never from the bars the line happens to be anchored across.
        window = bars[marked[0]:marked[1] + 1]
        if window:
            _add(lines, _level('Marked candle high', max(b['high'] for b in window), first, last))
            _add(lines, _level('Marked candle low', min(b['low'] for b in window), first, last))
    for key, label, extreme in POINTS:
        if key in geometry:
            _add(lines, _text(label, geometry[key], bars, extreme))
    if 'confirmation_level' in geometry:
        _add(lines, _level('Trigger level', geometry['confirmation_level'], first, last))
    return lines


def _candlestick_lines(geometry, bars, first, last, marked, spec=None):
    lines = []
    _add(lines, _level('Pattern high', geometry.get('high'), first, last))
    _add(lines, _level('Pattern low', geometry.get('low'), first, last))
    if not lines:
        window = bars[marked[0]:marked[1] + 1]
        if window:
            _add(lines, _level('Pattern high', max(b['high'] for b in window), first, last))
            _add(lines, _level('Pattern low', min(b['low'] for b in window), first, last))
    window = bars[marked[0]:marked[1] + 1]
    if window:
        # TA-Lib defines these shapes with an additional preceding candle.
        # It is part of the frozen recognition span, not an extra crow/hammer.
        context = (spec or {}).get('pattern_id') in {
            'CDL3BLACKCROWS', 'CDLHAMMER', 'CDLHANGINGMAN', 'CDLINVERTEDHAMMER', 'CDLSHOOTINGSTAR'}
        _add(lines, _range('Pattern + preceding context' if context else 'Pattern candles', marked[0], marked[1],
                          geometry.get('low', min(b['low'] for b in window)),
                          geometry.get('high', max(b['high'] for b in window))))
        if context:
            _add(lines, _text('Defining prior candle', marked[0], bars))
        # These TA-Lib patterns intrinsically confirm on their third candle;
        # a separate later confirmation bar would misstate the frozen event.
        intrinsic = (spec or {}).get('pattern_id') in {'CDL3INSIDE', 'CDL3OUTSIDE'}
        recognition_label = 'Recognized + confirmed' if intrinsic else 'Pattern recognized'
        _add(lines, _text(recognition_label, marked[1], bars))
        if last > marked[1]:
            _add(lines, _text('Confirmation', last, bars))
        if (spec or {}).get('pattern_id') in {'CDLHIKKAKE', 'CDLHIKKAKEMOD'}:
            # TA-Lib core 0.6.4 confirms beyond patternIdx-1: candle two
            # for Hikkake, candle three for modified Hikkake. This threshold
            # is the named inside bar's extreme, not the whole pattern range.
            inside_index = marked[1] - 1
            if marked[0] <= inside_index < marked[1]:
                extreme = 'high' if (spec or {}).get('side') == 'long' else 'low'
                _add(lines, _level('Inside bar trigger', bars[inside_index][extreme], inside_index, last))
    for key, label, extreme in POINTS:
        if key in geometry:
            _add(lines, _text(label, geometry[key], bars, extreme))
    if 'confirm_level' in geometry:
        _add(lines, _level('Trigger level', geometry['confirm_level'], first, last))
    return lines


# ----------------------------------------------------------------- legacy recovery
class LegacyOverlays:
    """Recovers CH01-CH10 overlays by re-running the adapter's own replay.

    ``pattern_research/legacy.py`` builds a ``HistoricalReplay`` and keeps only
    the episode events; the original match — with the ``lines`` the app has
    always drawn — is discarded.  This rebuilds the *same* replay over the *same
    bars* with the *same* mask and takes the match back, so nothing about the
    overlay is reimplemented.  Built lazily, once per (symbol, timeframe), and
    only when a CH01-CH10 detection is actually surfaced.
    """

    def __init__(self, bars):
        self.bars = bars
        self._replay = None
        self._mask = None

    def _ensure(self):
        if self._replay is None:
            from .pattern_research.legacy import IDS
            from .historical import HistoricalReplay
            from .replay_filter import masks
            replay = HistoricalReplay(self.bars, 260, list(IDS))
            self._replay = replay
            self._mask = masks(replay.h, replay.l, replay.c, replay.v, replay.tr,
                               replay.tops, replay.bottoms, 260)
        return self._replay, self._mask

    def lines(self, pattern_id, signal_index, score):
        """The legacy match's own lines, rebased to this window, or None.

        Accepted only when the recovered match is the same pattern with the same
        score as the event; a mismatch means the replay and the adapter disagree
        and the caller must say "no overlay" rather than draw something else."""
        from .pattern_research.legacy import IDS
        name = next((n for n, pid in IDS.items() if pid == pattern_id), None)
        if name is None or not 0 <= signal_index < len(self.bars):
            return None
        replay, mask = self._ensure()
        found = replay.at(signal_index, int(mask[signal_index]))
        match = next((m for m in found if m['pattern'] == name), None)
        if match is None or abs(float(match['score']) - float(score)) > 1e-9:
            return None
        offset = int(match['window_offset'])
        out = []
        for line in match.get('lines') or ():
            points = []
            for point in line['points']:
                i = _index(point['index'] + offset, len(self.bars))
                value = _finite(point['value'])
                if i is None or value is None:
                    return None
                points.append(_point(i, value))
            if points:
                out.append({'label': line['label'], 'role': line['role'], 'points': points})
        return out or None


# ------------------------------------------------------------------------ build
NOTE_NO_GEOMETRY = 'Marker only: this detector publishes no drawable structure for this event.'
NOTE_LEGACY = ('Marker only: the CH01-CH10 replay did not reproduce this event’s match, '
               'so its original overlay could not be recovered.')
NOTE_OUT_OF_WINDOW = 'Marker only: the pattern’s structure lies outside the candles served here.'


def build(spec, event, bars, *, formation_start, signal_index, tracking=None, legacy=None):
    """``(lines, note)`` for one detection.

    ``note`` is non-empty exactly when ``lines`` is empty.  Indices are absolute
    positions in ``bars``; the caller rebases them if it trims the window.
    """
    n = len(bars)
    if not n:
        return [], NOTE_OUT_OF_WINDOW
    first, last = _index(formation_start, n), _index(signal_index, n)
    if first is None or last is None or last < first:
        return [], NOTE_OUT_OF_WINDOW
    recognized = _index(event.get('detected_index', signal_index), n)
    recognized = min(last, max(first, recognized)) if recognized is not None else last
    marked = (first, recognized)            # confirmation bars do not become pattern candles
    if last == first:
        # Levels may reach one bar back for visibility; they never consume a
        # post-signal candle. The marked rectangle keeps the actual one-bar span.
        first = max(0, first - 1)
    if last == first and spec['family'] not in ('candlestick', 'price_action'):
        return [], NOTE_OUT_OF_WINDOW       # a one-candle window cannot carry a line
    geometry = event.get('geometry') if isinstance(event.get('geometry'), dict) else {}
    family = spec['family']

    if family == 'chart' and str(spec['variant']).startswith('legacy_'):
        recovered = legacy.lines(spec['pattern_id'], int(signal_index), event['score']) if legacy else None
        if not recovered:
            return [], NOTE_LEGACY
        lines = list(recovered)
    elif family == 'chart':
        lines = _chart_lines(geometry, bars, first, last, spec)
    elif family == 'harmonic':
        lines = _harmonic_lines(geometry, bars, first, last)
    elif family == 'price_action':
        lines = _price_action_lines(geometry, bars, first, last, marked, spec)
    else:
        lines = _candlestick_lines(geometry, bars, first, last, marked, spec)

    lines.extend(_tracking_lines(tracking, first, last))
    lines = [l for l in lines if l['points'] and all(
        0 <= p['index'] < n and math.isfinite(p['value']) for p in l['points'])]
    if not lines:
        return [], NOTE_NO_GEOMETRY
    return lines, ''


def rebase(lines, offset):
    """Shift every point index by ``-offset`` after the served window is trimmed."""
    for line in lines or ():
        for point in line['points']:
            point['index'] -= offset
    return lines


def oldest_index(lines, default):
    """The earliest bar any overlay refers to, so the served window can cover it."""
    indices = [p['index'] for line in lines or () for p in line['points']]
    return min(indices + [default]) if indices else default
