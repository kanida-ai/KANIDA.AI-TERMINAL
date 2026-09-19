"""Record exhaustive candle drawing review plus OHLC/definition checks.

The visual-page flags are explicit human-review bookkeeping, never inferred from
nonempty lines. This script does not run a candlestick detector or backtest.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from market_scanner.pattern_research.candlesticks import TABLE

OUT=ROOT/'docs/pattern_research'
CONTEXT={'CDL3BLACKCROWS','CDLHAMMER','CDLHANGINGMAN','CDLINVERTEDHAMMER','CDLSHOOTINGSTAR'}
INTRINSIC={'CDL3INSIDE','CDL3OUTSIDE'}


def near(a,b):
    return abs(float(a)-float(b))<1e-5


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reviewed-through-page',type=int,default=0)
    parser.add_argument('--intrinsic-rechecked',action='store_true')
    parser.add_argument('--hikkake-rechecked',action='store_true')
    args=parser.parse_args()
    source=OUT/'drawing_audit_examples.json'
    all_examples=json.loads(source.read_text(encoding='utf-8'))
    samples=sorted([x for x in all_examples['examples'] if x['spec']['family']=='candlestick'],
        key=lambda x:tuple(x['spec'][k] for k in ('pattern_id','variant','side')))
    table={r[0]:r for r in TABLE}
    reviews=[]
    for number,sample in enumerate(samples,1):
        spec,event,bars=sample['spec'],sample['event'],sample['bars']
        pid=spec['pattern_id']; row=table[pid]
        first=sample['formation_start']; recognized=event['detected_index']-sample['window_offset']; signal=sample['signal_index']
        marked=bars[first:recognized+1]
        geometry=event['geometry']; labels={x['label']:x for x in sample['lines']}
        range_lines=[x for x in sample['lines'] if x['role']=='candle_range']
        expected_label='Pattern + preceding context' if pid in CONTEXT else 'Pattern candles'
        expected_recognition='Recognized + confirmed' if pid in INTRINSIC else 'Pattern recognized'
        checks={
            'span_matches_pinned_definition':len(marked)==row[2],
            'definition_high_matches_marked_wicks':near(geometry['high'],max(b['high'] for b in marked)),
            'definition_low_matches_marked_wicks':near(geometry['low'],min(b['low'] for b in marked)),
            'ohlc_bodies_inside_wicks':all(b['low']<=min(b['open'],b['close'])<=max(b['open'],b['close'])<=b['high'] for b in bars),
            'one_unambiguous_marked_range':len(range_lines)==1,
            'context_named_explicitly':bool(range_lines) and range_lines[0]['label']==expected_label,
            'range_ends_at_recognition_excludes_confirmation':bool(range_lines) and [p['index'] for p in range_lines[0]['points']]==[first,recognized],
            'range_prices_are_marked_wick_extremes':bool(range_lines) and near(range_lines[0]['points'][0]['value'],geometry['low']) and near(range_lines[0]['points'][1]['value'],geometry['high']),
            'high_level_matches_published_high':all(near(p['value'],geometry['high']) for p in labels['Pattern high']['points']),
            'low_level_matches_published_low':all(near(p['value'],geometry['low']) for p in labels['Pattern low']['points']),
            'recognition_label_has_correct_lifecycle_meaning':expected_recognition in labels,
            'recognition_marker_at_correct_candle_close':expected_recognition in labels and labels[expected_recognition]['points']==[{'index':recognized,'value':round(bars[recognized]['close'],6)}],
            'points_do_not_reach_future_bars':all(0<=p['index']<=signal for line in sample['lines'] for p in line['points']),
            'signal_at_causal_window_end':signal==len(bars)-1,
            'raw_event_identity_matches_presented_signal':sample['event_identity']['signal_at']==bars[signal]['time'],
        }
        if pid in CONTEXT:
            checks['defining_prior_candle_marker_correct']=labels['Defining prior candle']['points']==[{'index':first,'value':round(bars[first]['close'],6)}]
        if pid in {'CDLHIKKAKE','CDLHIKKAKEMOD'}:
            inside=recognized-1
            level=bars[inside]['high' if spec['side']=='long' else 'low']
            checks['hikkake_inside_bar_trigger_matches_pinned_definition']=labels.get('Inside bar trigger',{}).get('points')==[
                {'index':inside,'value':round(level,6)},{'index':signal,'value':round(level,6)}]
        if signal>recognized:
            checks['confirmation_marker_at_signal_close']=labels.get('Confirmation',{}).get('points')==[{'index':signal,'value':round(bars[signal]['close'],6)}]
            checks['confirmation_within_three_bars']=signal-recognized<=3
            if 'confirm_level' in geometry:
                checks['trigger_level_matches_published_threshold']=all(near(p['value'],geometry['confirm_level']) for p in labels['Trigger level']['points'])
                checks['confirming_close_beyond_threshold']=bars[signal]['close']>geometry['confirm_level'] if spec['side']=='long' else bars[signal]['close']<geometry['confirm_level']
                expected=geometry['high']+.12*geometry['setup_atr'] if spec['side']=='long' else geometry['low']-.12*geometry['setup_atr']
                checks['threshold_uses_frozen_setup_atr']=near(geometry['confirm_level'],expected)
            elif pid in {'CDLHIKKAKE','CDLHIKKAKEMOD'}:
                inside=bars[recognized-1]
                checks['hikkake_closes_beyond_inside_bar']=bars[signal]['close']>inside['high'] if spec['side']=='long' else bars[signal]['close']<inside['low']
                checks['hikkake_library_confirmation_recorded']=abs(event['library_value'])==200
        elif pid in INTRINSIC:
            checks['intrinsic_confirmation_at_recognition']=event['state']=='confirmed' and event['confirmed_index']==event['detected_index']
            checks['no_invented_later_confirmation']='Confirmation' not in labels
        else:
            checks['setup_has_no_confirmation_label']='Confirmation' not in labels and event['state']=='setup'
        if 'color' in geometry:
            actual=(bars[recognized]['close']>bars[recognized]['open'])-(bars[recognized]['close']<bars[recognized]['open'])
            checks['frozen_color_matches_ohlc']=actual==geometry['color']
        if spec['variant'] in {'break_up','break_down'} and event['state']=='setup':
            checks['unconfirmed_break_is_neutral']=event['direction']=='neutral'
        if spec['variant']=='canonical_context':
            expected_trend=('down' if spec['side']=='long' else 'up') if row[4]=='reversal' else ('up' if spec['side']=='long' else 'down')
            checks['stored_context_direction_matches_definition']=geometry.get('prior_trend')==expected_trend
        page=(number-1)//9+1; grid_row=(number-1)%9//3+1
        visual=page<=args.reviewed_through_page
        failed=[name for name,passed in checks.items() if not passed]
        identity_key=hashlib.sha256(json.dumps(sample['event_identity'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
        body_wicks=[]
        for i,b in enumerate(marked,first):
            body_wicks.append({'index':i,'time':b['time'],'open':b['open'],'high':b['high'],'low':b['low'],'close':b['close'],
                'body':round(abs(b['close']-b['open']),6),'upper_wick':round(b['high']-max(b['open'],b['close']),6),
                'lower_wick':round(min(b['open'],b['close'])-b['low'],6),
                'role':'defining_prior_context' if pid in CONTEXT and i==first else 'pattern_candle',
                'display_color':'green' if b['close']>=b['open'] else 'red'})
        reviews.append({'event_identity_key':identity_key,'event_identity':sample['event_identity'],'frozen_event_sha256':sample['event_sha256'],'event_sha256':sample['event_sha256'],
            'pattern_id':pid,'name':spec['name'],'variant':spec['variant'],'side':spec['side'],'definition':spec['definition'],
            'status':'pending_visual_review' if not visual else 'reviewed_with_failed_checks' if failed else 'drawing_matches_frozen_definition',
            'visual_review':{'reviewed':visual,'gallery_page':page,'gallery_row':grid_row,'ordinal':number,
                'method':'Actual KANIDA PatternCanvas via CUA screenshots; every card inspected for box span, candle bodies/wicks, labels and recognition/confirmation separation.',
                'intrinsic_label_fix_visually_rechecked':args.intrinsic_rechecked if pid in INTRINSIC else None},
            'semantic_checks':checks,'failed_checks':failed,'marked_candle_measurements':body_wicks,
            'findings':['INTRINSIC_CONFIRMATION_LABEL'] if pid in INTRINSIC else ['HIKKAKE_TRIGGER_EXPLANATION'] if pid in {'CDLHIKKAKE','CDLHIKKAKEMOD'} else [],
            'observations':['Visually compared the marked range to the actual candle bodies/wicks and the recognition/confirmation locations.' if visual else 'Visual review pending.',
                f'Pinned recognition span contains {len(marked)} candle(s), with {1 if pid in CONTEXT else 0} explicitly defining prior context candle(s).',
                'Stored prices and dates were checked independently against OHLC; detector validity remains unclassified.'],
            'recognition_time':bars[recognized]['time'],'confirmation_time':bars[signal]['time'] if event['state']=='confirmed' else None,
            'representation_notes':['The amber range encloses the marked candle highs/lows; it is not a fitted shape.','Candle body=open/close; wick=high/low; green includes equality.']+
                (['The preceding defining context candle is explicitly included and labeled.'] if pid in CONTEXT else [])+
                (['Recognition and confirmation occur on the same third candle.'] if pid in INTRINSIC else []),
            'detector_validity':'not_certified','source':sample['source']})
    completed=sum(x['visual_review']['reviewed'] for x in reviews)
    issues=[{'id':'INTRINSIC_CONFIRMATION_LABEL','patterns':sorted(INTRINSIC),'affected_variants':8,
        'finding':'Intrinsic patterns previously showed only Recognized, omitting their simultaneous confirmation meaning.',
        'detail':'All eight CDL3INSIDE/CDL3OUTSIDE variant/side examples intrinsically confirm on the same third candle. A single combined recognition/confirmation label preserves that meaning without adding another candle.',
        'fix':'One Recognized + confirmed marker on the third candle; no invented later confirmation bar.',
        'status':'fixed_and_all_eight_rechecked' if args.intrinsic_rechecked else 'fixed_in_code_visual_recheck_pending'}]
    issues.append({'id':'HIKKAKE_TRIGGER_EXPLANATION','patterns':['CDLHIKKAKE','CDLHIKKAKEMOD'],'affected_variants':4,
        'detail':'Hikkake confirms across the second candle extreme; modified Hikkake across the third. Both use the bar immediately before recognition, not the entire pattern high/low. Earlier drawings omitted this distinct trigger and could make a valid library confirmation look inconsistent with the range.',
        'fix':'Inside bar trigger uses the actual named bar high for long and low for short, beginning at that inside bar and ending at signal.',
        'status':'fixed_and_all_four_rechecked' if args.hikkake_rechecked else 'fixed_in_code_visual_recheck_pending',
        'primary_sources':['https://raw.githubusercontent.com/TA-Lib/ta-lib/v0.6.4/src/ta_func/ta_CDLHIKKAKE.c',
            'https://raw.githubusercontent.com/TA-Lib/ta-lib/v0.6.4/src/ta_func/ta_CDLHIKKAKEMOD.c'],
        'verified_installed_version':{'python_wrapper':'0.6.8','core':'0.6.4'}})
    for review in reviews:
        if review['pattern_id'] in {'CDLHIKKAKE','CDLHIKKAKEMOD'}:
            review['visual_review']['hikkake_trigger_fix_visually_rechecked']=args.hikkake_rechecked
        if review['visual_review']['ordinal'] in {117,119} and review['visual_review']['reviewed']:
            review['findings'].append('SMALL_GRID_LABEL_ON_LEVEL')
            review['status']='drawing_matches_with_documented_legibility_limit'
            review['observations'].append('In the small grid, the Recognized text intersects the horizontal high/trigger lines. It remains interpretable on inspection, but legibility is weaker than a clear label background; prices and marker position are correct.')
    issues.append({'id':'SMALL_GRID_LABEL_ON_LEVEL','patterns':['CDLMORNINGDOJISTAR','CDLMORNINGSTAR'],
        'affected_ordinals':[117,119],'severity':'low','status':'documented_legibility_limit',
        'detail':'On the two canonical long AADHARHFC 1H examples dated 2025-07-15, the small-grid Recognized text crosses a horizontal high/trigger line. The marker remains interpretable and its price/date mapping is correct. This is a readability limitation, not a geometry defect.',
        'fix':'No further noncritical renderer change during this full audit; retained as an explicit limitation.'})
    result={'schema_version':1,'status':'complete_with_explicit_limits' if completed==len(reviews) and not any(x['failed_checks'] for x in reviews) and args.intrinsic_rechecked and args.hikkake_rechecked else 'in_progress',
        'scope':'All 165 frozen candlestick variant/side examples across 61 pattern IDs, one stored event and one observed timeframe per variant. Every example receives a fresh visual review; the earlier 61-representative review is not treated as coverage.',
        'limitations':'Drawing accuracy is correspondence to the stored event, its pinned recognition span and OHLC. This review does not certify every detector classification, trend quality, profitability, every historical occurrence, or every timeframe. Canonical TA-Lib recognitions do not require trend context; canonical_context variants do.',
        'run':all_examples['run'],'source_run':all_examples['run'],'source_examples_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
        'drawing_adapter_sha256':hashlib.sha256((ROOT/'market_scanner/pattern_lines.py').read_bytes()).hexdigest(),
        'definition_source':'market_scanner/pattern_research/candlesticks.py TABLE, _definition, detect and _hikkake_confirm; pinned TA-Lib 0.6.8; no detector/backtest rerun.',
        'candle_renderer_source':'kanida-app/src/PatternCanvas.tsx: candleLayer draws high-low wicks, open-close bodies and green when close>=open.',
        'coverage':{'variants':len(reviews),'pattern_ids':len({x['pattern_id'] for x in reviews}),'visually_reviewed':completed,
            'semantic_checks_pass':sum(not x['failed_checks'] for x in reviews),'intrinsic_variants_rechecked':8 if args.intrinsic_rechecked else 0,
            'hikkake_variants_rechecked':4 if args.hikkake_rechecked else 0},
        'findings':issues,'examples':reviews}
    target=OUT/'CANDLE_DRAWING_REAUDIT.json'
    target.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    md=['# Complete candlestick drawing re-audit','',f"Status: **{result['status']}**. Visual coverage **{completed}/165 variants**; semantic checks pass **{result['coverage']['semantic_checks_pass']}/165**.",'',result['scope'],'',result['limitations'],'','## Findings','']
    for issue in issues:
        md.extend([f"### {issue['id']}",'',issue['detail']+' '+issue['fix']+' Status: '+issue['status']+'.',''])
    md.extend(['Hikkake definitions verified against the installed TA-Lib core 0.6.4: [Hikkake source](https://raw.githubusercontent.com/TA-Lib/ta-lib/v0.6.4/src/ta_func/ta_CDLHIKKAKE.c), [modified Hikkake source](https://raw.githubusercontent.com/TA-Lib/ta-lib/v0.6.4/src/ta_func/ta_CDLHIKKAKEMOD.c).','','## Exact event review ledger','','| # | Pattern | Variant / side | Event | Visual page / row | Semantic checks |','|---:|---|---|---|---|---|'])
    for x in reviews:
        e=x['event_identity'];v=x['visual_review']
        md.append(f"| {v['ordinal']} | {x['pattern_id']} | {x['variant']} / {x['side']} | {e['symbol']} {e['timeframe']} {e['signal_at']} | {str(v['gallery_page'])+' / '+str(v['gallery_row']) if v['reviewed'] else 'pending'} | {', '.join(x['failed_checks']) if x['failed_checks'] else 'pass'} |")
    (OUT/'CANDLE_DRAWING_REAUDIT.md').write_text('\n'.join(md)+'\n',encoding='utf-8')
    print(json.dumps(result['coverage'],indent=2))
    print(json.dumps([{'ordinal':x['visual_review']['ordinal'],'pattern':x['pattern_id'],'failures':x['failed_checks']} for x in reviews if x['failed_checks']],indent=2))


if __name__=='__main__': main()
