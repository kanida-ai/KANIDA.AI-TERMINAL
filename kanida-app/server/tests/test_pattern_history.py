"""Acceptance tests for the isolated descriptive-history pilot and frozen replay."""
import gzip
import hashlib
import json
import sqlite3
from datetime import datetime, timedelta

import pytest

from kanida_pilot.errors import PilotError
from kanida_pilot.pattern_history import PatternHistoryService, build_cache, build_cell, movement, _cells, _pilot_cells
from kanida_pilot.research_store import ResearchStore


def bars(n=24):
    start = datetime(2024, 1, 1, 9, 15)
    return [dict(time=(start + timedelta(days=i)).isoformat(' '),
                 end=(start + timedelta(days=i, hours=6, minutes=15)).isoformat(' '),
                 open=100., high=110. + i, low=95. - i / 2, close=101. + i,
                 volume=1000, gap=False) for i in range(n)]


SPEC = dict(pattern_id='CH16', variant='canonical', side='long', states=['setup', 'confirmed'],
            definition_version='1.0.0', definition='Frozen double-bottom definition', name='Double Bottom')


def event(signal, state='setup'):
    return dict(signal_index=signal, episode=signal - 1, state=state,
                formation_start_index=max(0, signal - 2), detected_index=signal,
                geometry={'troughs': [max(0, signal - 2), signal - 1], 'neckline': 108.}, score=.8)


def test_next_open_gross_horizon_and_excursion_timing():
    b = bars()
    b[1].update(open=200, high=210, low=180, close=205)
    b[2].update(open=205, high=230, low=190, close=220)
    b[3].update(open=220, high=225, low=170, close=210)
    result = movement(b, 0, 3)
    assert result['reference_price'] == 200
    assert result['gross_return_pct'] == 5  # no 0.40% deduction, no signal-close entry
    assert result['max_up_pct'] == 15 and result['max_down_pct'] == 15
    assert result['max_up_bar'] == 2 and result['max_down_bar'] == 3
    assert movement(b, 0, 1)['gross_return_pct'] == 2.5


def test_pending_and_quality_exclusions_are_horizon_specific():
    b = bars(8)
    b[4]['gap'] = True
    assert movement(b, 0, 3)['status'] == 'measured'
    assert movement(b, 0, 5)['reason'] == 'quality_gap_in_window'
    assert movement(b, 6, 3)['reason'] == 'horizon_incomplete'
    assert movement(b, 7, 1)['reason'] == 'no_reference_candle'
    assert movement(b, 4, 1)['reason'] == 'signal_quality'
    assert movement(b, 3, 1)['reason'] == 'reference_quality'
    assert movement(b, 0, 5)['gross_return_pct'] is None


def test_no_future_leakage_into_shorter_movement():
    b = bars()
    first = movement(b, 0, 3)
    b[6].update(low=1, high=10000, gap=True)
    assert movement(b, 0, 3) == first


def test_states_overlap_and_baseline_same_exact_window():
    b = bars()
    summary, occurrences = build_cell(b, [event(2), event(3), event(3, 'confirmed')], 'TEST', SPEC, 'setup', 'r')
    assert summary['occurrence_count'] == 2  # overlapping occurrences retained, states not pooled
    assert len(occurrences) == 2
    assert summary['horizons'][0]['baseline']['total'] == 2
    assert summary['horizons'][0]['baseline']['mean_gross_return_pct'] == summary['horizons'][0]['observed']['mean_gross_return_pct']
    confirmed, _ = build_cell(b, [event(2), event(3, 'confirmed')], 'TEST', SPEC, 'confirmed', 'r')
    assert confirmed['occurrence_count'] == 1


def test_no_occurrences_are_distinct_from_missing_stock():
    summary, _ = build_cell(bars(), [], 'TEST', SPEC, 'setup', 'r')
    assert summary['occurrence_count'] == 0 and summary['last_seen'] is None
    assert summary['horizons'][0]['observed']['up_pct'] is None


def test_denominators_separate_flat_pending_and_invalid_prices():
    b = bars(8)
    b[1].update(open=100., high=100., low=100., close=100.)
    b[4]['low'] = float('nan')
    summary, _ = build_cell(b, [event(0), event(3), event(7)], 'TEST', SPEC, 'setup', 'r')
    stats = summary['horizons'][0]['observed']
    assert (stats['n'], stats['unchanged_n'], stats['pending'], stats['quality_excluded']) == (1, 1, 1, 1)
    assert stats['up_pct'] == 0 and stats['median_bars_to_max_up'] == 0
    json.dumps(summary, allow_nan=False)


@pytest.fixture
def frozen(tmp_path):
    root = tmp_path / 'research'
    run = root / 'frozen-run'
    (run / 'stocks').mkdir(parents=True)
    (run / 'history').mkdir()
    b = bars()
    name = hashlib.sha256(b'TEST').hexdigest() + '.json.gz'
    history = gzip.compress(json.dumps({'1D': [b, {}]}, separators=(',', ':')).encode())
    (run / 'history' / name).write_bytes(history)
    cells = [dict(SPEC, timeframe='1D', occurrence_events=[event(i) for i in range(2, 24)] + [event(5, 'confirmed')])]
    artifact = gzip.compress(json.dumps(dict(symbol='TEST', run='frozen-run', cells=cells), separators=(',', ':')).encode())
    (run / 'stocks' / name).write_bytes(artifact)
    manifest = dict(id='frozen-run', source_run='clean-source', source_market_latest=b[-1]['end'],
                    symbols=['TEST'], specifications=[SPEC], history_hashes={'TEST': hashlib.sha256(gzip.decompress(history)).hexdigest()},
                    code_hashes={'detector.py': 'frozen-code'}, dependencies={'talib': 'test'})
    (run / 'manifest.json').write_text(json.dumps(manifest))
    with sqlite3.connect(root / 'research.sqlite3') as con:
        con.execute('create table runs(id text,status text)')
        con.execute("insert into runs values ('frozen-run','complete')")
        con.execute('create table stocks(run text,symbol text,status text,metadata text)')
        con.execute('insert into stocks values (?,?,?,?)', ('frozen-run', 'TEST', 'complete', json.dumps(dict(
            artifact_sha256=hashlib.sha256(gzip.decompress(artifact)).hexdigest(), history_sha256=hashlib.sha256(gzip.decompress(history)).hexdigest()))))
    catalogue = tmp_path / 'catalogue.json'
    catalogue.write_text(json.dumps(dict(specifications=[SPEC])))
    release = tmp_path / 'release.json'
    store = ResearchStore(root, 'frozen-run', catalogue, release)
    cache = tmp_path / 'cache.sqlite3'
    yield store, cache, b, release, run
    store.close()


def test_cache_roundtrip_recent_completion_and_frozen_replay(frozen):
    store, cache, b, _, _ = frozen
    result = build_cache(store, cache)
    assert result['cached_symbols'] == 1
    service = PatternHistoryService(store, cache)
    assert service.catalogue()['review_required'] is True
    stocks = service.stocks('CH16', 'canonical', 'setup')
    assert stocks['stocks'][0]['occurrence_count'] == 22
    history = service.history('TEST', 'CH16', 'canonical', 'setup', 5)
    assert history['recent_completed'][0]['signal_index'] == 18
    assert len(history['recent_completed']) == 5
    assert len(history['recent_pending']) == 5
    assert history['recent_occurrences'][0]['signal_index'] == 23
    chosen = history['recent_completed'][0]
    replay = service.replay('TEST', 'CH16', 'canonical', 'setup', chosen['id'])
    assert replay['geometry'] == event(18)['geometry']
    actual = next(row for row in replay['bars'] if row['index'] == 19)
    assert actual['close'] == b[19]['close'] and replay['reference_index'] == 19
    assert replay['geometry_source'] == 'frozen_detector_event'
    with pytest.raises(PilotError, match='does not belong'):
        service.replay('TEST', 'CH16', 'canonical', 'confirmed', chosen['id'])
    with pytest.raises(PilotError, match='not yet'):
        service.history('MISSING', 'CH16', 'canonical', 'setup')
    assert build_cache(store, cache)['cached_symbols'] == 1  # resumable, no duplicate occurrences


def test_source_tampering_fails_before_cache_publication(frozen):
    store, cache, _, _, run = frozen
    path = next((run / 'history').iterdir())
    path.write_bytes(gzip.compress(gzip.decompress(path.read_bytes()) + b' '))
    with pytest.raises(ValueError, match='checksum'):
        build_cache(store, cache)
    with sqlite3.connect(cache) as con:
        assert con.execute('select count(*) from histories').fetchone()[0] == 0


def test_builder_keeps_source_files_unchanged_and_refuses_source_output(frozen):
    store, cache, _, _, run = frozen
    files = [p for p in store.directory.rglob('*') if p.is_file()]
    before = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
    build_cache(store, cache)
    assert {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in files} == before
    with pytest.raises(ValueError, match='outside'):
        build_cache(store, run / 'forbidden.sqlite3')
    assert not (run / 'forbidden.sqlite3').exists()


def test_publication_and_detector_identity_fail_closed(frozen):
    store, cache, _, release, _ = frozen
    build_cache(store, cache)
    service = PatternHistoryService(store, cache)
    release.write_text(json.dumps({'runs': {'frozen-run': 'withheld_source_quality_review'}}))
    with pytest.raises(PilotError, match='withheld'):
        service.catalogue()
    release.write_text(json.dumps({'runs': {'frozen-run': 'released'}}))
    assert service.catalogue()['review_required'] is False
    store.catalogue.spec('CH16', 'canonical', 'long')['definition'] = 'different detector'
    with pytest.raises(PilotError, match='definition differs'):
        service.catalogue()


def test_manifest_change_invalidates_serving(frozen):
    store, cache, _, _, run = frozen
    build_cache(store, cache)
    path = run / 'manifest.json'
    path.write_text(path.read_text() + '\n')
    with pytest.raises(PilotError, match='identity'):
        PatternHistoryService(store, cache).catalogue()


def test_selective_extraction_matches_reference_parser(tmp_path):
    other = dict(SPEC, pattern_id='OTHER', timeframe='1D', occurrence_events=[], unused='x' * 1100000)
    intraday = dict(SPEC, timeframe='1H', occurrence_events=[event(1)], unused='y' * 1100000)
    desired = dict(SPEC, timeframe='1D', occurrence_events=[event(3)],
                   unused='An escaped marker: {"pattern_id":"CH16",')
    path = tmp_path / 'stock.json.gz'
    path.write_bytes(gzip.compress(json.dumps({'cells': [other, intraday, desired]}, separators=(',', ':')).encode()))
    expected = [c for c in _cells(path) if c['pattern_id'] == 'CH16' and c['timeframe'] == '1D']
    assert list(_pilot_cells(path)) == expected
