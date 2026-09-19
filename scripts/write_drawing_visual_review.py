"""Record the lead reviewer's reported visual inspection, separately from examples.

This records review evidence; it does not perform visual review. Additional
inspection must be explicitly reported before adding pattern IDs to reviewed.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'docs/pattern_research'


def write_report(review):
    counts = review['coverage']
    md = ['# Pattern drawing visual review', '', f"Status: **{review['status']}**.", '',
          f"Visually inspected: **{counts['patterns_inspected']}/{counts['patterns_total']} pattern IDs**, using one real representative example per pattern. **{counts['patterns_pending']} remain pending.**", '',
          review['scope'], '', review['representative_binding'], '', review['limitations'], '',
          'Inspection results below record the lead reviewer’s reported observations. They do not certify detector validity or imply that every observed issue is fixed.', '',
          '## Findings and follow-up', '', '| Finding | Scope | Observation | Status |', '|---|---|---|---|']
    for finding in review['findings']:
        md.append(f"| {finding['id']} | {finding['scope']} | {finding['observation']} | {finding['status']} |")
    md += ['', '## Fix recheck scope', '', review['recheck_scope'], '',
           '| Finding | Exact representative patterns rechecked | Observation |', '|---|---|---|']
    for finding in review['findings']:
        if finding.get('recheck'):
            md.append(f"| {finding['id']} | {', '.join(finding['recheck']['pattern_ids'])} | {finding['recheck']['observation']} |")
    md += ['', 'Reported automated validation: 89 backend tests and 14 history tests passed; all 61 candle spans checked; 13 legacy directional overlays unchanged. These checks do not expand the manual recheck scope.', '']
    md += ['', '## Per-pattern representative review', '',
           'The machine-readable manifest retains the complete frozen event identity and a SHA-256 key for each representative. These review records are separate from regenerated drawing examples.', '',
           '| Pattern | Representative | Variant / side / state | Review status | Follow-up |', '|---|---|---|---|---|']
    for pid, identity_key in sorted(review['by_pattern'].items()):
        item=review['reviews_by_event_identity'][identity_key]
        event=item['event_identity']
        md.append(f"| {pid} {item['name']} | {event['symbol']} {event['timeframe']} {event['signal_at']} | {event['variant']} / {event['side']} / {event['state']} | {item['status']} | {', '.join(item['finding_ids']) or 'None reported' if item['inspected'] else 'Inspection pending'} |")
    for name,content in [('DRAWING_VISUAL_REVIEW.md','\n'.join(md)+'\n'),
                         ('drawing_visual_review.json',json.dumps(review,ensure_ascii=False,indent=2)+'\n')]:
        target=OUT/name
        temporary=target.with_suffix(target.suffix+'.tmp')
        temporary.write_text(content,encoding='utf-8')
        temporary.replace(target)


def main():
    path=OUT/'drawing_audit_examples.json'
    examples=json.loads(path.read_text(encoding='utf-8'))
    representatives={}
    for item in sorted(examples['examples'],key=lambda x:tuple(x['spec'][k] for k in ('pattern_id','variant','side'))):
        representatives.setdefault(item['spec']['pattern_id'],item)
    previous_path=OUT/'drawing_visual_review.json'
    if previous_path.exists():
        previous=json.loads(previous_path.read_text(encoding='utf-8'))
        if previous.get('source_reports'):
            raise RuntimeError('This representative-only writer is superseded. Use merge_drawing_reaudits.py to preserve all 262 exact-event reviews.')
        for pid,item in representatives.items():
            previous_key=previous['by_pattern'].get(pid)
            if previous_key is None or previous['reviews_by_event_identity'][previous_key]['event_identity'] != item['event_identity']:
                raise ValueError(f'Representative changed for {pid}; obtain an explicit new visual review before updating its record.')
    # Lead reviewer explicitly completed the remaining 52 candle representatives
    # after the first 55-pattern inspection. This is a report, not an automated
    # assumption that generated examples have been visually checked.
    reviewed=set(representatives)
    findings=[
        {'id':'CH11_SUPPORT_EXTENT','scope':'CH11','observation':'Support line was extrapolated before its first published pivot. The corrected line starts at its first support trough.','status':'resolved_with_sample_recheck',
         'recheck':{'pattern_ids':['CH11'],'observation':'First support starts at the trough; corrected extent visually confirmed.'}},
        {'id':'HARMONIC_RATIO_CLUTTER','scope':'HA01–HA10','observation':'Ratio labels crowded the chart. The ratio readout now sits outside the plot.','status':'resolved_with_sample_recheck',
         'recheck':{'pattern_ids':['HA01','HA02','HA03'],'observation':'XABCD point labels clear and ratios outside the plot in these three representatives.'}},
        {'id':'CANDLE_LABEL_OVERLAP','scope':'Candlestick drawings','observation':'Pattern labels overlapped. Sample recheck confirms separated recognized/confirmed labels.','status':'resolved_with_sample_recheck',
         'recheck':{'pattern_ids':['CDL2CROWS','CDL3BLACKCROWS','CDL3INSIDE'],'observation':'Recognized and confirmed labels do not overlap in these three representatives.'}},
        {'id':'PA_FORMATION_MARKING','scope':'PA01–PA08','observation':'Two generic levels did not adequately identify the price-action formation. Marked ranges and gap labels now identify the formation.','status':'resolved_with_sample_recheck',
         'recheck':{'pattern_ids':[f'PA{i:02}' for i in range(1,9)],'observation':'All eight representative patterns rechecked; range marks and gap labels convey the formation.'}},
        {'id':'ROUNDING_GUIDES_LIMIT','scope':'CH19–CH21','observation':'Frozen events publish rim/extreme anchors, not fitted curves. Straight anchor guides must be explicit and must not imply a fitted rounding curve.','status':'documented_representation_limit'},
        {'id':'CH17_DETECTOR_QUALITY','scope':'CH17 AADHARHFC 4H 2025-03-21','observation':'The triple-top example has highs near 440/449/449 but very unequal intervening valleys near 424/345. The structural mapping matches frozen evidence; validity as a useful triple-top remains a detector-quality concern.','status':'unresolved_detector_quality_concern'},
        {'id':'CANDLE_LIFECYCLE_CONTEXT','scope':'Candlestick drawings','observation':'All 61 candle representatives were inspected. The final dynamic range/context legend was rechecked on CDL3BLACKCROWS: the amber box is identified as pattern plus preceding context, the defining-prior-candle leader points to the context bar, and Recognized is separate.','status':'resolved_with_sample_recheck',
         'recheck':{'pattern_ids':['CDL3BLACKCROWS'],'observation':'Final context legend visible: amber box describes pattern plus preceding context; defining-prior-candle leader identifies the context bar; Recognized label remains separate.'}},
    ]
    next(f for f in findings if f['id']=='ROUNDING_GUIDES_LIMIT')['recheck']={'pattern_ids':['CH19','CH20','CH21'],
        'observation':'Explicit straight-guide caveat visible for all three representatives; representation limitation remains documented.'}
    rechecked={pid for f in findings for pid in f.get('recheck',{}).get('pattern_ids',[])}
    review={'schema_version':1,'status':'all_representatives_reviewed_with_findings','run':examples['run'],
        'review_basis':'Lead reviewer reported visual inspection of the first sorted real example per pattern. This manifest records that report.',
        'scope':'One frozen real example for each of the 107 pattern IDs: 28 chart patterns, 10 harmonic patterns, 8 price-action patterns and 61 candlestick patterns. The selected example retains its registered variant, side, state and timeframe. This is not manual inspection of all 262 directional variants or every timeframe.',
        'representative_binding':'Each review applies only to the exact event_identity recorded here, selected as the first sorted example for that pattern in the reviewed artifact. Changing the representative or its identity does not transfer this visual review. Later drawing changes require the recorded recheck.',
        'limitations':'Visual drawing inspection assesses whether the overlay represents the published event and remains legible. Structural checks and detector pattern validity are separate. No trading-performance or detector-validity certification is made.',
        'recheck_scope':'After the fixes, the lead reviewer rechecked CH11; CH19–CH21; HA01–HA03; PA01–PA08; and CDL2CROWS, CDL3BLACKCROWS, CDL3INSIDE. These 18 exact representatives cover the recorded fixes and straight-guide caveat. The final dynamic range/context legend was additionally rechecked on CDL3BLACKCROWS. This is not a new visual inspection of all 107 representatives, all 262 variants, or every timeframe.',
        'gallery_recheck_reported':'Lead reviewer confirmed the gallery displays the 107 representative-review count and the unresolved CH17 concern.',
        'automated_validation_reported':{'source':'lead_reviewer_report','backend_tests_passed':89,'history_tests_passed':14,
            'candlestick_span_cases':61,'unchanged_legacy_directional_overlays':13},
        'coverage':{'patterns_total':len(representatives),'patterns_inspected':len(reviewed),'patterns_pending':len(representatives)-len(reviewed),
            'manual_examples_inspected':len(reviewed),'available_directional_variants':len(examples['examples']),
            'representatives_with_fix_or_caveat_recheck':len(rechecked),
            'all_directional_variants_manually_inspected':False,'all_timeframes_manually_inspected':False},
        'example_artifact_at_manifest_creation':{'path':path.relative_to(ROOT).as_posix(),'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
            'note':'This identifies the artifact at manifest creation; it does not assert that post-inspection overlay changes were visually rechecked.'},
        'findings':findings,'by_pattern':{},'reviews_by_event_identity':{}}
    for pid,item in sorted(representatives.items()):
        identity=item['event_identity']
        identity_key=hashlib.sha256(json.dumps(identity,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        inspected=pid in reviewed
        ids=[]
        if pid=='CH11': ids.append('CH11_SUPPORT_EXTENT')
        if pid.startswith('HA'): ids.append('HARMONIC_RATIO_CLUTTER')
        if pid.startswith('PA'): ids.append('PA_FORMATION_MARKING')
        if pid.startswith('CDL') and inspected: ids.extend(['CANDLE_LABEL_OVERLAP','CANDLE_LIFECYCLE_CONTEXT'])
        if pid in ('CH19','CH20','CH21'): ids.append('ROUNDING_GUIDES_LIMIT')
        if pid=='CH17': ids.append('CH17_DETECTOR_QUALITY')
        status='pending'
        if inspected:
            status='inspected_no_specific_issue_reported' if not ids else 'inspected_follow_up_recorded'
        review['by_pattern'][pid]=identity_key
        review['reviews_by_event_identity'][identity_key]={'pattern_id':pid,'name':item['spec']['name'],
            'event_identity':identity,'frozen_event_sha256':item['event_sha256'],'inspected':inspected,'status':status,
            'finding_ids':ids,'detector_validity':'not_certified',
            'rechecked_findings':[f['id'] for f in findings if pid in f.get('recheck',{}).get('pattern_ids',[])],
            'checks':{'representative_visually_inspected':inspected,
                'post_fix_visual_recheck':'sample_rechecked' if pid in rechecked else 'not_in_post_fix_recheck_sample' if ids else 'not_requested',
                'frozen_event_mapping': 'matches_frozen_geometry_with_detector_quality_concern' if pid=='CH17' else 'no_individual_semantic_claim',
                'automated_event_and_window_checks':item['validation']}}
    write_report(review)
    print(json.dumps(review['coverage'],indent=2))


if __name__=='__main__':
    main()
