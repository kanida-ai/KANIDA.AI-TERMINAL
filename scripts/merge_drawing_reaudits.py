"""Publish human review records only after every exact registered example is reviewed.

This validates and merges evidence; it does not infer visual review from tests.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / 'docs/pattern_research'
REPORTS = ['LEGACY_DRAWING_REAUDIT', 'EXPANDED_CHART_DRAWING_REAUDIT',
           'CANDLE_DRAWING_REAUDIT', 'HARMONIC_PA_DRAWING_REAUDIT']


def identity_key(identity):
    return hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def main():
    artifact = json.loads((OUT / 'drawing_audit_examples.json').read_text(encoding='utf-8'))
    expected = {identity_key(e['event_identity']): e for e in artifact['examples']}
    assert len(expected) == 262, 'Unexpected catalogue coverage; review scope must be checked explicitly.'
    reviews, findings = {}, {}
    for name in REPORTS:
        report = json.loads((OUT / (name + '.json')).read_text(encoding='utf-8'))
        assert report['run'] == artifact['run'], name
        for finding in report.get('findings', []):
            findings[finding['id']] = {
                'id': finding['id'], 'scope': ', '.join(finding.get('patterns', [])),
                'observation': finding.get('detail', finding.get('observation', '')),
                'status': finding['status']}
        for item in report['examples']:
            key = identity_key(item['event_identity'])
            assert key in expected and key not in reviews, (name, key)
            example = expected[key]
            assert item['event_sha256'] == example['event_sha256'], (name, 'changed event')
            visual = item['visual_review']
            assert visual['reviewed'] is True, (name, item['pattern_id'], 'not visually reviewed')
            assert not any(value is False for value in item.get('semantic_checks', {}).values()), item
            pending = visual.get('post_fix_recheck') is False or 'pending' in item['status']
            assert not pending, (name, item['pattern_id'], 'recheck pending')
            ids = item.get('finding_ids', item.get('findings', []))
            ids = [f['id'] if isinstance(f, dict) else f for f in ids]
            # Family findings explicitly name the patterns they concern.
            ids += [f['id'] for f in report.get('findings', []) if item['pattern_id'] in f.get('patterns', [])]
            reviews[key] = {
                'event_identity': item['event_identity'], 'frozen_event_sha256': item['event_sha256'],
                'name': example['spec']['name'], 'inspected': True, 'status': item['status'],
                'finding_ids': sorted(set(ids)), 'semantic_checks': item.get('semantic_checks', {}),
                'observations': item.get('observations', []), 'visual_review': visual,
                'checks': {'post_fix_visual_recheck': 'complete' if any(visual.get(k) is True for k in ('post_fix_recheck', 'post_fix_reviewed', 'intrinsic_label_fix_visually_rechecked', 'hikkake_trigger_fix_visually_rechecked')) else 'not_required'},
                'source_report': name + '.json'}
    assert reviews.keys() == expected.keys(), 'Missing exact-event reviews'
    assert not any('pending' in f['status'] for f in findings.values()), 'Finding recheck still pending'
    for review in reviews.values():
        assert all(f in findings for f in review['finding_ids']), review['finding_ids']
    by_pattern = {}
    for key, review in reviews.items():
        by_pattern.setdefault(review['event_identity']['pattern_id'], key)
    result = {
        'run': artifact['run'], 'status': 'all_registered_variants_inspected_with_documented_concerns',
        'drawing_adapter_sha256': artifact['drawing_adapter_sha256'],
        'scope': 'One actual frozen occurrence for every registered pattern variant and direction, including CH01–CH10.',
        'limitations': 'Not every historical occurrence or timeframe. A source-faithful drawing does not certify detector validity or trading performance.',
        'coverage': {'patterns_total': 107, 'patterns_inspected': len(by_pattern), 'patterns_pending': 0,
                     'manual_examples_inspected': len(reviews), 'available_directional_variants': len(expected),
                     'all_directional_variants_manually_inspected': True, 'all_timeframes_inspected': False},
        'findings': list(findings.values()), 'by_pattern': by_pattern, 'reviews_by_event_identity': reviews,
        'source_reports': [name + '.json' for name in REPORTS]}
    rows = ['# Complete registered-variant drawing review', '',
            '**107 pattern types · 262 exact examples visually inspected**, including all 13 directions of the first 10 chart patterns.', '',
            result['scope'], '', result['limitations'], '', '## Findings', '',
            '| Scope | Observation | Status |', '|---|---|---|']
    for finding in findings.values():
        rows.append(f"| {finding['scope']} | {finding['observation']} | {finding['status']} |")
    rows += ['', '## Exact-event review records', '',
             '| Pattern | Variant / side | Symbol / timeframe | State | Review |', '|---|---|---|---|---|']
    for review in reviews.values():
        e = review['event_identity']
        rows.append(f"| {e['pattern_id']} | {e['variant']} / {e['side']} | {e['symbol']} / {e['timeframe']} | {e['state']} | {review['status']} |")
    rows += ['', 'Machine-readable records preserve each frozen event identity, event hash, semantic checks, visual observations and family report provenance.', '']
    for name, text in [('drawing_visual_review.json', json.dumps(result, ensure_ascii=False, indent=2) + '\n'),
                       ('DRAWING_VISUAL_REVIEW.md', '\n'.join(rows))]:
        target = OUT / name
        temporary = target.with_suffix(target.suffix + '.tmp')
        temporary.write_text(text, encoding='utf-8')
        temporary.replace(target)
    print(json.dumps(result['coverage']))


if __name__ == '__main__':
    main()
