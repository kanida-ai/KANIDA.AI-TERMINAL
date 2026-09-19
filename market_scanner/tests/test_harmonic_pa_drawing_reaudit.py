"""OHLC/ratio semantics for every frozen harmonic and price-action variant.

These checks validate these drawings, not statistical detector validity.
"""
import json
import math
from pathlib import Path

import pytest

from market_scanner import pattern_lines

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = [e for e in json.loads((ROOT / 'docs/pattern_research/drawing_audit_examples.json').read_text(encoding='utf-8'))['examples']
            if e['spec']['family'] in ('harmonic', 'price_action')]


def inspect_example(e):
    spec, ev, bars = e['spec'], e['event'], e['bars']
    g, offset = ev['geometry'], e['window_offset']
    first, detected, signal = e['formation_start'], ev['detected_index'] - offset, e['signal_index']
    pid, bull = spec['pattern_id'], spec['side'] == 'long'
    padded = [dict(open=math.nan, high=math.nan, low=math.nan, close=math.nan)] * offset + bars
    generated, note = pattern_lines.build(spec, ev, padded,
        formation_start=first + offset, signal_index=signal + offset)
    lines = {l['label']: l for l in pattern_lines.rebase(generated, offset)}
    checks = {}
    def check(name, ok):
        checks[name] = bool(ok)
    def near(a, b):
        return math.isclose(a, b, rel_tol=1e-6, abs_tol=1e-6)
    def values(label, expected):
        return label in lines and all(near(p['value'], expected) for p in lines[label]['points'])
    check('chronological_lifecycle', 0 <= first <= detected <= signal < len(bars))
    check('drawable_finite_bounded', not note and all(0 <= p['index'] <= signal and math.isfinite(p['value']) for l in lines.values() for p in l['points']))
    if spec['family'] == 'harmonic':
        points = sorted(g['points'].items(), key=lambda x: x[1]['index'])
        point_map = dict(points)
        check('pivots_are_named_ohlc_extrema', all(near(p['price'], bars[p['index'] - offset][p['kind']]) for _, p in points))
        check('causal_pivot_availability', all(p['available_index'] <= ev['detected_index'] for _, p in points))
        check('alternating_pivots_and_side', all(a['kind'] != b['kind'] for (_, a), (_, b) in zip(points, points[1:])) and (points[-1][1]['kind'] == ('low' if bull else 'high')))
        check('exact_pivot_labels', all(lines[name]['points'] == [{'index': p['index'] - offset, 'value': p['price']}] for name, p in points))
        for name, value in g['ratios'].items():
            key = name.removeprefix('time_')
            numerator, denominator = key.split('/')
            def leg(pair):
                a, b = [point_map[x] for x in pair]
                return (b['index'] - a['index']) if name.startswith('time_') else abs(b['price'] - a['price'])
            expected = leg(numerator) / leg(denominator)
            # The detector's terminal retracement is directional, not a length.
            if not name.startswith('time_') and numerator == points[1][0] + points[-1][0]:
                expected = (1 if points[1][1]['kind'] == 'high' else -1) * (points[1][1]['price'] - points[-1][1]['price']) / leg(denominator)
            check('ratio_' + name, near(value, expected))
        check('exact_projected_prz', values('PRZ low', min(g['prz']['price_band'])) and values('PRZ high', max(g['prz']['price_band'])))
        check('published_trigger', values('Trigger level', g['confirmation']['level']))
        check('published_failure', values('Failure level', g['confirmation']['failure_level']))
        check('failure_outside_terminal_extreme', g['confirmation']['failure_level'] < points[-1][1]['price'] if bull else g['confirmation']['failure_level'] > points[-1][1]['price'])
    else:
        window = bars[first:detected + 1]
        box = lines['Range comparison candles' if pid == 'PA05' else 'Pattern candles']['points']
        check('recognition_box_exact_ohlc_and_timing', box == [{'index': first, 'value': min(b['low'] for b in window)}, {'index': detected, 'value': max(b['high'] for b in window)}])
        check('confirmation_not_in_shape', lines.get('Confirmation', {}).get('points', [{}])[0].get('index') == signal if signal > detected else 'Confirmation' not in lines)
        current, prior = bars[detected], bars[detected - 1]
        if pid == 'PA01':
            extreme = 'low' if bull else 'high'
            check('tweezer_equal_extremes', near(abs(current[extreme] - prior[extreme]), g['extreme_diff']) and g['extreme_diff'] <= g['tolerance'])
        if pid in ('PA02', 'PA06'):
            mother = bars[g['mother_index'] - offset]
            check('mother_levels_exact', near(mother['high'], g['mother_high']) and near(mother['low'], g['mother_low']))
            end = detected if pid == 'PA02' else g['last_inside_index'] - offset
            check('inside_bars_within_mother', all(b['high'] <= mother['high'] and b['low'] >= mother['low'] for b in bars[first + 1:end + 1]))
        if pid == 'PA03':
            check('outside_both_prior_extremes', current['high'] > prior['high'] and current['low'] < prior['low'])
            check('outside_close_quarter', (current['close'] - current['low']) / (current['high'] - current['low']) >= .75 if bull else (current['close'] - current['low']) / (current['high'] - current['low']) <= .25)
        if pid == 'PA04':
            body = abs(current['close'] - current['open'])
            upper, lower = current['high'] - max(current['open'], current['close']), min(current['open'], current['close']) - current['low']
            dom, opp = (lower, upper) if bull else (upper, lower)
            span = current['high'] - current['low']
            check('pin_shadow_proportions', dom >= 2 * body and dom >= .60 * span and opp <= .15 * span)
            check('pin_published_fractions', near(dom / span, g['dominant_shadow_frac']) and near(opp / span, g['opposite_shadow_frac']))
            if spec['variant'].endswith('_context'):
                check('published_context_direction', g['prior_trend'] == ('down' if bull else 'up'))
        if pid == 'PA05':
            check('narrowest_of_comparison_window', current['high'] - current['low'] < min(b['high'] - b['low'] for b in window[:-1]))
            check('nr_levels_exact', near(current['high'], g['nr_high']) and near(current['low'], g['nr_low']))
        if pid == 'PA06':
            breach = bars[g['breach_index'] - offset:g['failure_index'] - offset + 1]
            check('false_break_extreme', near(g['breach_extreme'], min(b['low'] for b in breach) if bull else max(b['high'] for b in breach)))
            check('failure_close_back_inside', g['mother_low'] < current['close'] < g['mother_high'])
        if pid in ('PA07', 'PA08'):
            if pid == 'PA07':
                lo, hi = (prior['high'], current['low']) if bull else (current['high'], prior['low'])
                key, label = 'window_size', 'Full-range window'
            else:
                lo, hi = (current['open'], prior['low']) if bull else (prior['high'], current['open'])
                key, label = 'opening_gap', 'Opening gap (filled intrabar)'
                check('reversal_fills_opening_gap', current['close'] > hi if bull else current['close'] < lo)
            check('gap_is_actual_positive_ohlc_distance', hi > lo and near(hi - lo, g[key]))
            check('gap_box_exact', lines[label]['points'] == [{'index': first, 'value': lo}, {'index': detected, 'value': hi}])
        if 'confirmation_level' in g:
            check('published_trigger', values('Trigger level', g['confirmation_level']))
            check('signal_close_crosses_trigger', bars[signal]['close'] > g['confirmation_level'] if bull else bars[signal]['close'] < g['confirmation_level'])
    return checks


@pytest.mark.parametrize('example', EXAMPLES, ids=lambda e: '-'.join(e['spec'][k] for k in ('pattern_id', 'variant', 'side')))
def test_frozen_drawing_semantics(example):
    checks = inspect_example(example)
    assert all(checks.values()), [name for name, passed in checks.items() if not passed]


def test_all_expected_variants_are_checked():
    assert len(EXAMPLES) == 52
    assert sum(e['spec']['family'] == 'harmonic' for e in EXAMPLES) == 26
