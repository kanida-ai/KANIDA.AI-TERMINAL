"""Freeze real drawing examples without rerunning research detectors/backtests.

Reads the committed research cell index, stock events and frozen OHLCV history.
Selects one representative per registered variant/side, preferring confirmed
events and longer formations within each selected stock. Legacy overlays alone
use the unchanged legacy replay to recover its original drawing (cached per TF).
"""
from __future__ import annotations

import argparse
import copy
import gzip
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
RUN = '4b33a5249562631524d6'
OUT = ROOT / 'docs/pattern_research'
RESEARCH = ROOT / 'market_scanner/output/expanded_research'


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def read_gzip(path):
    raw = gzip.decompress(Path(path).read_bytes())
    return json.loads(raw), hashlib.sha256(raw).hexdigest()


def key(spec):
    return tuple(spec[k] for k in ('pattern_id', 'variant', 'side'))


def start_of(event, time_index):
    return int(event.get('formation_start_index', time_index.get(event.get('pattern_start'), -1)))


def availability_indices(value):
    """Published pivot availability excludes projections such as apex_index."""
    if not isinstance(value, dict):
        return []
    result = []
    for name, child in value.items():
        if name.endswith('_available') or name == 'available_index':
            result.extend(child if isinstance(child, list) else [child])
        elif isinstance(child, dict):
            result.extend(availability_indices(child))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--render-only', action='store_true', help='Rebuild overlays from already selected frozen events.')
    args = parser.parse_args()
    manifest = read_json(RESEARCH / RUN / 'manifest.json')
    specs = read_json(OUT / 'IMPLEMENTED_CATALOGUE.json')['specifications']
    specs_by_key = {key(s): s for s in specs}
    con = sqlite3.connect(f'file:{(RESEARCH / "research.sqlite3").as_posix()}?mode=ro', uri=True)
    con.row_factory = sqlite3.Row
    selected = {}
    missing = []
    loaded = []
    if args.render_only:
        previous = read_json(OUT / 'drawing_audit_examples.json')
        selected = {key(x['spec']): x for x in previous['examples']}
        missing = previous['missing']
        loaded = previous['stocks_read']
    else:
        unresolved = set(specs_by_key)
        while unresolved:
            wanted = sorted(unresolved)[0]
            row = con.execute('SELECT symbol,artifact FROM cells WHERE run=? AND pattern_id=? AND variant=? AND side=? AND occurrences>0 LIMIT 1', (RUN, *wanted)).fetchone()
            if row is None:
                missing.append({'spec': specs_by_key[wanted], 'status': 'no_frozen_occurrences', 'reason': 'No committed cell in this frozen run has occurrences for this exact variant and side.'})
                unresolved.remove(wanted)
                continue
            symbol = row['symbol']
            stock_path = RESEARCH / row['artifact']
            stock, stock_hash = read_gzip(stock_path)
            metadata = json.loads(con.execute('SELECT metadata FROM stocks WHERE run=? AND symbol=?', (RUN,symbol)).fetchone()[0])
            assert stock_hash == metadata['artifact_sha256'], symbol
            history_path = Path(manifest['histories'][symbol])
            history, history_hash = read_gzip(history_path)
            assert history_hash == manifest['history_hashes'][symbol] == stock['history_sha256'], symbol
            assert stock['run'] == RUN and stock['symbol'] == symbol
            time_indices = {tf: {b['time']: i for i,b in enumerate(frame[0])} for tf,frame in history.items()}
            candidates = {}
            for cell in stock['cells']:
                identity = key(cell)
                if identity not in unresolved:
                    continue
                tf = cell['timeframe']
                for event in cell['occurrence_events']:
                    signal = int(event['signal_index'])
                    start = start_of(event, time_indices[tf])
                    if not 0 <= start <= signal < len(history[tf][0]):
                        continue
                    rank = (event['state'] == 'confirmed', min(signal-start, 500), signal, tf)
                    if identity not in candidates or rank > candidates[identity][0]:
                        candidates[identity] = (rank, tf, event, start)
            for identity, (_, tf, event, start) in candidates.items():
                selected[identity] = {'spec': specs_by_key[identity], 'symbol': symbol, 'timeframe': tf, 'event': event,
                    'source': {'run': RUN, 'stock_artifact': stock_path.relative_to(ROOT).as_posix(), 'stock_sha256': stock_hash,
                               'history_artifact': history_path.relative_to(ROOT).as_posix(), 'history_sha256': history_hash},
                    'formation_start_absolute': start, 'signal_index_absolute': int(event['signal_index'])}
            unresolved -= candidates.keys()
            loaded.append(symbol)
            print(f'{symbol}: selected {len(selected)}/{len(specs)}; missing {len(missing)}; unresolved {len(unresolved)}', flush=True)
            if wanted not in candidates:
                raise RuntimeError(f'Indexed occurrence absent from verified stock artifact: {wanted} {symbol}')
    con.close()
    # Save selection before any slow legacy drawing recovery; it is independently reviewable.
    selection_fields = ('spec','symbol','timeframe','event','source','formation_start_absolute','signal_index_absolute')
    selection = {'run': RUN, 'stocks_read': loaded,
        'examples': [{k:x[k] for k in selection_fields} for x in selected.values()], 'missing': missing}
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / 'drawing_audit_selection.json').write_text(json.dumps(selection, ensure_ascii=False, indent=2), encoding='utf-8')
    from market_scanner import pattern_lines
    histories = {}
    legacy = {}
    examples = []
    for identity, sample in sorted(selected.items()):
        symbol, tf = sample['symbol'], sample['timeframe']
        if symbol not in histories:
            histories[symbol] = read_gzip(ROOT / sample['source']['history_artifact'])[0]
        bars = histories[symbol][tf][0]
        signal, start = sample['signal_index_absolute'], sample['formation_start_absolute']
        event, spec = sample['event'], sample['spec']
        original = None
        if str(spec['variant']).startswith('legacy_'):
            if (symbol,tf) not in legacy:
                legacy[symbol,tf] = pattern_lines.LegacyOverlays(bars)
            original = legacy[symbol,tf]
        lines, note = pattern_lines.build(spec, event, bars[:signal+1], formation_start=start, signal_index=signal, legacy=original)
        absolute_lines = copy.deepcopy(lines)
        earliest = min([start] + [p['index'] for line in lines for p in line['points']])
        offset = max(0, earliest - 15)
        pattern_lines.rebase(lines, offset)
        window = bars[offset:signal+1]
        detected = int(event.get('detected_index', signal))
        validation = {
            'timestamp_matches_formation': bars[start]['time'] == event['pattern_start'],
            'event_index_order': start <= detected <= signal,
            'confirmed_index_matches_signal': event.get('confirmed_index', signal) == signal,
            'bars_end_at_signal': window[-1]['time'] == bars[signal]['time'],
            'all_points_finite_and_in_causal_window': all(0 <= p['index'] < len(window) and math.isfinite(p['value']) for line in lines for p in line['points']),
            'geometry_note_contract': bool(note) == (not bool(lines)),
            'published_pivots_available_by_signal': all(isinstance(i,(int,float)) and 0 <= i <= signal for i in availability_indices(event.get('geometry',{}))),
        }
        if original:
            validation['legacy_overlay_exactly_preserved'] = absolute_lines == original.lines(spec['pattern_id'], signal, event['score'])
        if spec['family'] == 'harmonic':
            points = event.get('geometry',{}).get('points',{})
            validation['harmonic_points_match_named_candle_extremes'] = all(
                0 <= int(p['index']) <= signal and abs(float(p['price'])-bars[int(p['index'])][p['kind']]) < 1e-5
                for p in points.values())
        identity_fields = {'run':RUN, 'symbol':symbol, 'timeframe':tf, 'pattern_id':spec['pattern_id'],
            'variant':spec['variant'], 'side':spec['side'], 'state':event['state'], 'episode':event['episode'],
            'formation_at':bars[start]['time'], 'detected_at':bars[detected]['time'], 'signal_at':bars[signal]['time']}
        event_hash = hashlib.sha256(json.dumps(event,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        sample.update(bars=window, window_offset=offset, formation_start=start-offset, signal_index=signal-offset,
            signal_at=bars[signal]['time'], detected_at=bars[detected]['time'], signal_close_at=bars[signal].get('end'),
            lines=lines, geometry_note=note, validation=validation,
            event_identity=identity_fields, event_sha256=event_hash,
            drawing_source='unchanged_legacy_replay' if original else 'frozen_event_geometry_and_named_candles',
            semantic_review='pending_visual_review')
        examples.append(sample)
        if original:
            print(f'Legacy recovered {identity}: {len(lines)} lines', flush=True)
    present_ids = {x['spec']['pattern_id'] for x in examples}
    total_ids = {s['pattern_id'] for s in specs}
    summary = {'catalogue_entries': len(total_ids), 'directional_variants': len(specs), 'patterns_with_examples': len(present_ids),
        'variants_with_examples': len(examples), 'variants_without_frozen_occurrences': len(missing),
        'patterns_without_frozen_occurrences': sorted(total_ids-present_ids),
        'drawable_examples': sum(bool(x['lines']) for x in examples),
        'validation_failures': [{'key':list(key(x['spec'])),'checks':[k for k,v in x['validation'].items() if not v]} for x in examples if not all(x['validation'].values())],
        'states': {s: sum(x['event']['state']==s for x in examples) for s in ('setup','confirmed')}}
    result = {'run': RUN, 'drawing_adapter_sha256':hashlib.sha256((ROOT/'market_scanner/pattern_lines.py').read_bytes()).hexdigest(),
        'method': 'Representative real frozen event per registered variant and side; confirmed then longest useful formation within the first indexed stock containing that variant. Frozen event indices remain absolute; line/bar indices are rebased by window_offset. No new research detection or backtest is run. Legacy replay only recovers its original overlays.',
        'causality': 'Every displayed candle and every overlay point ends at or before the event signal. Original frozen geometry is preserved unchanged. This checks presentation causality, not a new detector prefix-stability proof.',
        'coverage': summary, 'stocks_read': loaded, 'examples': examples, 'missing': missing}
    (OUT / 'drawing_audit_examples.json').write_text(json.dumps(result, ensure_ascii=False, separators=(',',':')), encoding='utf-8')
    (OUT / 'drawing_audit_coverage.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    md = ['# Frozen drawing example coverage', '', f'Frozen run: `{RUN}`.', '', f"Actual examples: **{len(present_ids)}/{len(total_ids)} pattern IDs**, **{len(examples)}/{len(specs)} directional variants**.", '', result['method'], '', result['causality'], '', 'These are review examples, not an assertion that every drawing is visually correct. Visual review remains explicit.', '', '| Pattern | Variant | Side | Example | State | Lines | Checks |', '|---|---|---|---|---|---:|---|']
    for sample in examples:
        spec=sample['spec']
        md.append(f"| {spec['pattern_id']} {spec['name']} | {spec['variant']} | {spec['side']} | {sample['symbol']} {sample['timeframe']} {sample['signal_at']} | {sample['event']['state']} | {len(sample['lines'])} | {'pass' if all(sample['validation'].values()) else 'FAIL'} |")
    md += ['', '## No frozen occurrence', '', '| Pattern | Variant | Side |', '|---|---|---|']
    md += [f"| {x['spec']['pattern_id']} {x['spec']['name']} | {x['spec']['variant']} | {x['spec']['side']} |" for x in missing]
    (OUT / 'DRAWING_AUDIT_COVERAGE.md').write_text('\n'.join(md)+'\n', encoding='utf-8')
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == '__main__':
    main()
