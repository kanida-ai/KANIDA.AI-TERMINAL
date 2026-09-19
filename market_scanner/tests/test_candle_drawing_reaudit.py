"""Intrinsic candle confirmations must not suggest a later confirming bar."""
import json
from pathlib import Path

from market_scanner import pattern_lines


def test_all_frozen_intrinsic_variants_mark_recognition_and_confirmation_together():
    path = Path(__file__).resolve().parents[2] / 'docs/pattern_research/drawing_audit_examples.json'
    examples = json.loads(path.read_text(encoding='utf-8'))['examples']
    intrinsic = [x for x in examples if x['spec']['pattern_id'] in {'CDL3INSIDE', 'CDL3OUTSIDE'}]
    assert len(intrinsic) == 8
    for x in intrinsic:
        event = dict(x['event'])
        event['detected_index'] -= x['window_offset']
        # These two patterns only publish high/low geometry, so no internal
        # geometry index needs rebasing for this stored-window rendering check.
        assert event['detected_index'] == x['signal_index']
        lines, note = pattern_lines.build(x['spec'], event, x['bars'],
            formation_start=x['formation_start'], signal_index=x['signal_index'])
        assert not note
        labels = {line['label']:line for line in lines}
        assert 'Pattern recognized' not in labels and 'Confirmation' not in labels
        marker = labels['Recognized + confirmed']['points'][0]
        assert marker['index'] == x['signal_index']
        assert marker['value'] == x['bars'][x['signal_index']]['close']


def test_hikkake_threshold_uses_second_or_third_inside_candle_for_both_sides():
    path = Path(__file__).resolve().parents[2] / 'docs/pattern_research/drawing_audit_examples.json'
    examples = json.loads(path.read_text(encoding='utf-8'))['examples']
    samples = [x for x in examples if x['spec']['pattern_id'] in {'CDLHIKKAKE', 'CDLHIKKAKEMOD'}]
    assert {(x['spec']['pattern_id'],x['spec']['side']) for x in samples} == {
        ('CDLHIKKAKE','long'),('CDLHIKKAKE','short'),('CDLHIKKAKEMOD','long'),('CDLHIKKAKEMOD','short')}
    for x in samples:
        event = dict(x['event'])
        event['detected_index'] -= x['window_offset']
        event['geometry'] = dict(event['geometry'])
        if 'setup_index' in event['geometry']:
            event['geometry']['setup_index'] -= x['window_offset']
        recognition = event['detected_index']
        inside = recognition-1
        expected_inside = x['formation_start']+(2 if x['spec']['pattern_id']=='CDLHIKKAKEMOD' else 1)
        assert inside == expected_inside
        key = 'high' if x['spec']['side']=='long' else 'low'
        level = x['bars'][inside][key]
        lines, note = pattern_lines.build(x['spec'],event,x['bars'],formation_start=x['formation_start'],signal_index=x['signal_index'])
        trigger = next(line for line in lines if line['label']=='Inside bar trigger')
        assert not note
        assert trigger['points'] == [{'index':inside,'value':round(level,6)}, {'index':x['signal_index'],'value':round(level,6)}]
        if event['state']=='confirmed':
            close=x['bars'][x['signal_index']]['close']
            assert close>level if x['spec']['side']=='long' else close<level
