"""Semantic drawing checks: actual extrema, causal marks, mirrored geometry and cached refresh."""
import copy

import pytest

from market_scanner import pattern_lines as drawing
from market_scanner.drawing_refresh import refresh_cell


def bars(n=40):
    return [dict(time=f't{i}', end=f'e{i}', open=100 + i, high=110 + i, low=90 + i, close=102 + i, volume=1000) for i in range(n)]


def render(pid, geometry, side='long', detected=25, signal=30, family='chart', first=5):
    spec = dict(pattern_id=pid, side=side, variant='canonical', family=family)
    event = dict(geometry=geometry, detected_index=detected, signal_index=signal, score=1)
    lines, note = drawing.build(spec, event, bars(), formation_start=first, signal_index=signal)
    return {line['label']: line for line in lines}, note


@pytest.mark.parametrize('pid,side,expected', [('CH15', 'short', [150, 105, 151]), ('CH16', 'long', [95, 125, 96])])
def test_double_turns_use_published_extrema_and_real_neckline_pivot(pid, side, expected):
    levels = expected[::2]
    lines, _ = render(pid, dict(troughs=[10, 20], trough_levels=levels, intervening_extreme=15, neckline=120), side)
    assert [p['value'] for p in lines['Double-turn structure guide']['points']] == expected


@pytest.mark.parametrize('pid,side,middle', [('CH17', 'short', [102, 112]), ('CH18', 'long', [122, 132])])
def test_triple_turns_do_not_mark_closes_as_intervening_extremes(pid, side, middle):
    lines, _ = render(pid, dict(troughs=[5, 15, 25], trough_levels=[100, 101, 100], intervening_extremes=[12, 22], resistance=120), side)
    assert [p['value'] for p in lines['Intervening extremes']['points']] == middle
    assert 'Neckline' in lines and 'Resistance' not in lines


def test_bear_pennant_swaps_mirrored_pivot_roles_and_does_not_project_over_pole():
    geo = dict(pole_start=5, pole_top=12, upper_anchors=[16, 22], lower_anchors=[18, 24], upper_line=[16, 125, -.1], lower_line=[18, 110, .1])
    before = copy.deepcopy(geo)
    lines, _ = render('CH14', geo, 'short')
    assert geo == before
    assert [p['value'] for p in lines['Pole']['points']] == [115, 102]
    assert lines['Resistance']['points'][0]['index'] == 18
    assert [p['value'] for p in lines['Upper pivots']['points']] == [128, 134]
    assert [p['value'] for p in lines['Lower pivots']['points']] == [106, 112]


def test_rounding_top_is_not_a_low_based_bowl_or_an_invented_fit():
    lines, _ = render('CH20', dict(left_rim=5, bowl_extreme=15, rim_level=95), 'short')
    guide = lines['Rounding structure guide (not a fitted curve)']
    assert [p['value'] for p in guide['points']] == [95, 125, 132]
    assert guide['role'] == 'shape' and 'Rim support' in lines
    assert lines['Dome extreme']['points'][0]['value'] == 125


def test_inverted_cup_uses_low_rims_high_dome_and_high_handle():
    lines, _ = render('CH21', dict(left_rim=5, dome_extreme=12, right_rim=20, handle_extreme=25), 'short')
    assert [p['value'] for p in lines['Inverted cup structure guide (not a fitted curve)']['points']] == [95, 122, 110]
    assert [p['value'] for p in lines['Handle structure guide']['points']] == [110, 135, 132]


def test_diamond_draws_both_halves_and_limits_right_fit_to_right_half():
    lines, _ = render('CH23', dict(highs=[5, 15, 25], lows=[8, 18, 28], anchors=[5, 8, 15, 18, 25, 28], upper_right_line=[15, 130, -.2], lower_right_line=[18, 100, .2]))
    assert [p['index'] for p in lines['Diamond upper structure guide']['points']] == [5, 15, 25]
    assert [p['index'] for p in lines['Diamond lower structure guide']['points']] == [8, 18, 28]
    assert lines['Resistance']['points'][0]['index'] == 15
    assert lines['Support']['points'][0]['index'] == 18
    assert 'Turning points' not in lines  # previously generic close anchors contradicted true pivots


@pytest.mark.parametrize('side,expected', [('long', [95, 125, 115]), ('short', [115, 105, 135])])
def test_measured_move_joins_correct_named_high_low_extremes(side, expected):
    lines, _ = render('CH26', dict(leg_start=5, leg_end=15, correction_end=25), side)
    assert [p['value'] for p in lines['Measured leg']['points']] == expected


def test_mirrored_bump_and_contractions_use_real_extremes():
    lines, _ = render('CH27', dict(lead_in_lows=[5, 10], bump_peak=20), 'long')
    assert [p['value'] for p in lines['Bump structure guide']['points']] == [115, 120, 110]
    lines, _ = render('CH28', dict(pullback_highs=[5, 15, 25], pullback_lows=[8, 18, 28]), 'short')
    assert [p['value'] for p in lines['Pullback highs']['points']] == [118, 128, 138]


def test_confirmed_candle_range_stops_at_recognition_and_has_no_diagonal():
    lines, _ = render('CDLENGULFING', dict(low=91, high=125, confirm_level=126), detected=6, signal=10, family='candlestick', first=5)
    assert lines['Pattern candles']['points'] == [{'index': 5, 'value': 91.}, {'index': 6, 'value': 125.}]
    assert not any(line['role'] == 'shape' for line in lines.values())
    assert lines['Confirmation']['points'][0]['index'] == 10
    assert lines['Pattern recognized']['points'][0]['index'] == 6


def test_triangle_fit_starts_at_supporting_trough_not_earlier_resistance():
    lines, _ = render('CH11', dict(resistance=160, resistance_touches=[5, 25],
                                  support_troughs=[15, 24], support_line=[19.5, 120, 2]))
    assert lines['Support']['points'] == [{'index': 15, 'value': 111.}, {'index': 30, 'value': 141.}]
    assert lines['Resistance']['points'][0]['index'] == 5


def test_three_crows_marks_three_crows_and_explicit_defining_prior_candle():
    lines, _ = render('CDL3BLACKCROWS', dict(low=91, high=125), detected=8,
                      signal=10, family='candlestick', first=5)
    assert [p['index'] for p in lines['Pattern + preceding context']['points']] == [5, 8]
    assert lines['Defining prior candle']['points'][0]['index'] == 5
    assert lines['Pattern recognized']['points'][0]['index'] == 8
    assert lines['Confirmation']['points'][0]['index'] == 10


@pytest.mark.parametrize('pid', ['CH25', 'CH26', 'CH27', 'CH28'])
def test_expanded_chart_publishes_exact_failure_reference_without_inventing_stop(pid):
    lines, _ = render(pid, {'failure_level': 98.25}, first=5)
    assert [p['value'] for p in lines['Failure reference']['points']] == [98.25, 98.25]
    lines, _ = render(pid, {})
    assert 'Failure reference' not in lines


def test_mirrored_structural_labels_name_lows_as_lows():
    lines, _ = render('CH14', dict(pole_start=5, pole_top=12), 'short')
    assert lines['Pole low']['points'][0]['value'] == 102
    assert 'Pole top' not in lines
    lines, _ = render('CH27', dict(lead_in_lows=[5, 10], bump_peak=20), 'long')
    assert lines['Bump trough']['points'][0]['value'] == 110
    assert 'Bump peak' not in lines


def test_single_candle_never_uses_next_candle_price_and_supports_one_bar_window():
    spec = dict(pattern_id='CDLDOJI', side='long', family='candlestick', variant='break')
    b = bars(1)
    lines, note = drawing.build(spec, dict(geometry={}, detected_index=0), b, formation_start=0, signal_index=0)
    box = next(line for line in lines if line['role'] == 'candle_range')
    assert not note and box['points'] == [{'index': 0, 'value': 90.}, {'index': 0, 'value': 110.}]


@pytest.mark.parametrize('pid', ['PA01', 'PA02', 'PA03', 'PA04', 'PA05', 'PA06', 'PA07', 'PA08'])
def test_price_action_marks_only_recognition_candles(pid):
    lines, _ = render(pid, {}, detected=7, signal=10, family='price_action', first=5)
    box = next(line for line in lines.values() if line['role'] == 'candle_range')
    assert [p['index'] for p in box['points']] == [5, 7]
    assert lines['Confirmation']['points'][0]['index'] == 10


@pytest.mark.parametrize('pid,side', [('PA07', 'long'), ('PA07', 'short'), ('PA08', 'long'), ('PA08', 'short')])
def test_price_action_gap_uses_verified_prices_and_distinguishes_opening_gap(pid, side):
    b = bars(4)
    b[1].update(open=98, high=100, low=95, close=97)
    b[2].update(open=93 if side == 'long' else 102, high=105 if side == 'long' else 93,
                low=102 if side == 'long' else 88, close=103 if side == 'long' else 90)
    if pid == 'PA07':
        b[2]['open'] = b[2]['close']
    else:
        b[2].update(high=103, low=92, close=99 if side == 'long' else 96)
    geo = {'window_size' if pid == 'PA07' else 'opening_gap': 2}
    spec = dict(pattern_id=pid, family='price_action', variant='test', side=side)
    lines, _ = drawing.build(spec, dict(geometry=geo, detected_index=2), b, formation_start=1, signal_index=3)
    label = 'Full-range window' if pid == 'PA07' else 'Opening gap (filled intrabar)'
    box = next(l for l in lines if l['label'] == label)
    assert box['points'][1]['value'] - box['points'][0]['value'] == 2
    geo[next(iter(geo))] = 999  # mismatched frozen metadata must not invent a gap
    lines, _ = drawing.build(spec, dict(geometry=geo, detected_index=2), b, formation_start=1, signal_index=3)
    assert not any(l['label'] == label for l in lines)


def cached_cell():
    b = bars(40)[20:]
    match = dict(pattern_id='CDLENGULFING', family='candlestick', variant='canonical', side='long',
        start_index=5, end_index=10, detected_at_bar_end='e26', detector_state='confirmed',
        input={'bars': 40}, geometry={'high': 138, 'low': 112},
        lines=[{'label': 'Trigger level', 'role': 'boundary', 'points': [{'index': 5, 'value': 140.}, {'index': 10, 'value': 140.}]}],
        detection_id='unchanged', score=100)
    return dict(bars=b, detection_bars=40, matches=[match])


def test_cached_refresh_uses_exact_offset_preserves_inputs_and_trigger():
    cell = cached_cell(); original = copy.deepcopy(cell)
    out = refresh_cell(cell); match = out['matches'][0]
    assert cell == original and out['bars'] == original['bars']
    assert match['detection_id'] == 'unchanged' and match['geometry_source_offset'] == 20
    labels = {line['label']: line for line in match['lines']}
    assert labels['Pattern candles']['points'][1]['index'] == 6
    assert labels['Trigger level']['points'][0]['value'] == 140
    assert all(0 <= p['index'] <= 10 for l in match['lines'] for p in l['points'])


def test_cached_refresh_is_fail_closed_and_preserves_legacy_exactly():
    cell = cached_cell()
    legacy = dict(cell['matches'][0], pattern_id='CH01', family='chart', variant='legacy_1.0.1')
    cell['matches'].append(legacy)
    cell['detection_bars'] = None
    out = refresh_cell(cell)
    assert out['matches'][0]['lines'] == [] and out['matches'][0]['geometry_note']
    assert out['matches'][1] == legacy
    cell = cached_cell(); cell['matches'][0]['geometry']['mother_index'] = 10
    assert refresh_cell(cell)['matches'][0]['lines'] == []


@pytest.mark.parametrize('family,pid,geometry', [
    ('price_action', 'PA01', {'pair_low': 112, 'pair_high': 138}),
    ('harmonic', 'HA01', {'points': {'A': {'index': 25, 'price': 115},
                                  'B': {'index': 26, 'price': 130},
                                  'C': {'index': 27, 'price': 118}}}),
])
def test_cached_refresh_restores_confirmed_trigger_in_each_family_schema(family, pid, geometry):
    cell = cached_cell()
    cell['matches'][0].update(family=family, pattern_id=pid, geometry=geometry)
    refreshed = refresh_cell(cell)['matches'][0]
    trigger = next(line for line in refreshed['lines'] if line['label'] == 'Trigger level')
    assert [p['value'] for p in trigger['points']] == [140, 140]
