"""Source geometry checks for all thirteen registered original-chart directions."""
import json
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'docs/pattern_research'
data = json.loads((OUT / 'drawing_audit_examples.json').read_text(encoding='utf-8'))
examples = []
for e in data['examples']:
    if not (e['spec']['family'] == 'chart' and e['spec']['variant'].startswith('legacy_')):
        continue
    bars, lines, pid = e['bars'], e['lines'], e['spec']['pattern_id']
    checks = {
        'finite_causal_coordinates': all(0 <= p['index'] < len(bars) and np.isfinite(p['value']) for l in lines for p in l['points']),
        'published_anchors_match_extremes': all(abs(p['value'] - bars[p['index']]['low' if 'lower' in l['label'].lower() else 'high']) < 1e-5 for l in lines if l['role'] == 'anchors' for p in l['points']),
    }
    observations = []
    if pid == 'CH01':
        curve = next(l for l in lines if l['label'] == 'Cup')
        a, b = curve['points'][0]['index'], curve['points'][-1]['index']
        coeff = np.polyfit(np.linspace(-1, 1, b-a+1), [q['close'] for q in bars[a:b+1]], 2)
        checks['saved_curve_matches_detector_close_fit'] = max(abs(p['value']-np.polyval(coeff, 2*(p['index']-a)/(b-a)-1)) for p in curve['points']) < 1e-5
        handle = next(l for l in lines if l['label'] == 'Handle')['points']
        checks['handle_uses_rim_high_handle_low_signal_close'] = all(abs(p['value']-bars[p['index']][k]) < 1e-5 for p,k in zip(handle, ['high','low','close']))
        observations.append('Removed browser replacement of the saved detector quadratic fit; source fit and handle anchors retained.')
    if pid == 'CH03':
        pole = next(l for l in lines if l['label'] == 'Pole')['points']
        checks['pole_matches_published_close_impulse'] = all(abs(p['value']-bars[p['index']]['close']) < 1e-5 for p in pole)
    if pid in ('CH09','CH10'):
        shape = next(l for l in lines if l['label'] == 'Pattern')['points']
        keys = ['high','low','high','low','high'] if pid == 'CH09' else ['low','high','low','high','low']
        checks['shoulders_and_neck_pivots_match_extremes'] = all(abs(p['value']-bars[p['index']][k]) < 1e-5 for p,k in zip(shape,keys))
        checks['head_beyond_both_shoulders'] = shape[2]['value'] > max(shape[0]['value'],shape[4]['value']) if pid == 'CH09' else shape[2]['value'] < min(shape[0]['value'],shape[4]['value'])
    if pid in ('CH02','CH03'):
        observations.append('Removed extra browser-inferred swing points; these were not published by the detector.')
    examples.append({
        'event_identity':e['event_identity'], 'event_sha256':e['event_sha256'],
        'pattern_id':pid, 'variant':e['spec']['variant'], 'side':e['spec']['side'],
        'state':e['event']['state'], 'symbol':e['symbol'], 'timeframe':e['timeframe'],
        'visual_review':{'reviewed':False, 'gallery_page':1 if len(examples)<9 else 2, 'post_fix_recheck':False},
        'semantic_checks':{k:bool(v) for k,v in checks.items()}, 'observations':observations,
        'status':'inspected_source_geometry_consistent',
        'limitations':'Exact frozen example, not every historical occurrence or timeframe; no trading-performance certification.'
    })
assert len(examples) == 13
assert all(all(e['semantic_checks'].values()) for e in examples)
(OUT/'LEGACY_DRAWING_REAUDIT.json').write_text(json.dumps({'run':data['run'], 'scope':'All 13 registered direction variants of CH01–CH10', 'examples':examples}, indent=2), encoding='utf-8')
print('13 legacy variants passed source geometry checks; post-fix visual review is separate.')
