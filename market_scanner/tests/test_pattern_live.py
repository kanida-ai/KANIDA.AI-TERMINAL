"""Live research-detector adapter: parity, lifecycle, warm-up gate, legacy safety.

What these tests defend
-----------------------
1. **Parity** -- the live adapter must surface *the research module's own
   events*, unchanged.  It may not invent, drop or re-time one.
2. **Timestamp identity** -- a detection's id must not move when the loader
   fetches a different amount of history.  This is the property that makes a
   live detection joinable to research evidence at all.
3. **Lifecycle** -- forming -> confirmed -> invalidated -> expired, with the
   fallback failure boundary labelled as a live-layer rule, never as the
   detector's own.
4. **Warm-up gate** -- the research set does not run without a passing,
   identity-matched measurement.  No blanket window is accepted.
5. **The legacy switch is off by default and changes nothing.**
"""
from __future__ import annotations

import json
import math
import os
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pytest

from market_scanner import detectors, pattern_live
from market_scanner.data import load_config, market15_path

ROOT = Path(__file__).resolve().parents[2]


# ------------------------------------------------------------------- fixtures
def synthetic(n=420, seed=7, start=date(2023, 1, 2)):
    """Deterministic daily candles on consecutive weekdays.

    A random walk with enough structure that every research family finds
    something; the point is reproducibility, not realism.
    """
    rng = np.random.default_rng(seed)
    close = 100 * np.exp(np.cumsum(rng.normal(0, 0.012, n)))
    wobble = 1 + 0.004 * np.sin(np.arange(n) / 5.0)
    bars, day = [], start
    for i in range(n):
        while day.weekday() >= 5:
            day += timedelta(days=1)
        c = float(close[i] * wobble[i])
        o = float(close[i - 1] if i else close[0])
        hi = max(o, c) * (1 + abs(float(rng.normal(0, 0.004))))
        lo = min(o, c) * (1 - abs(float(rng.normal(0, 0.004))))
        bars.append({'time': f'{day.isoformat()} 09:15:00', 'end': f'{day.isoformat()} 15:30:00',
                     'open': round(o, 2), 'high': round(hi, 2), 'low': round(lo, 2),
                     'close': round(c, 2), 'volume': float(10_000 + i), 'gap': False})
        day += timedelta(days=1)
    return bars


def research_events(bars, timeframe='1D'):
    """Every event the research modules emit, keyed the way research keys them."""
    modules, _specs, _by_key, _family = pattern_live.research_registry()
    found = {}
    for module in modules:
        for key, events in module.detect(bars, timeframe).items():
            if events:
                found[key] = events
    return found


@pytest.fixture(scope='module')
def bars():
    return synthetic()


@pytest.fixture(scope='module')
def detections(bars):
    return pattern_live.detect_live(bars, '1D', symbol='TEST', grace=None)


# --------------------------------------------------------------------- parity
def test_live_surfaces_exactly_the_research_events(bars, detections):
    """Same symbol, same window => the same events, re-keyed to timestamps."""
    events = research_events(bars)
    tail_start = len(bars) - pattern_live.LIVE_TAIL

    expected = set()
    for key, rows in events.items():
        episodes = {}
        for event in rows:
            episodes.setdefault(event['episode'], []).append(event)
        for group in episodes.values():
            group.sort(key=lambda e: (e['signal_index'], pattern_live.STATE_ORDER[e['state']]))
            anchor = next((e for e in group if e['state'] == 'setup'), group[0])
            confirm = next((e for e in group if e['state'] == 'confirmed'), None)
            detected = int(anchor.get('detected_index', anchor['signal_index']))
            newest = max(detected, int(confirm['signal_index']) if confirm else -1)
            if newest < tail_start:
                continue
            # The CH01-CH10 adapter reports its formation start as a timestamp
            # (`pattern_start`) and never as an index; both must resolve the same.
            start = anchor.get('formation_start_index')
            first = bars[int(start)]['time'] if start is not None else anchor['pattern_start']
            expected.add((key[0], key[1], key[2], first, bars[detected]['end']))

    got = {(d['pattern_id'], d['variant'], d['side'], d['formation_start'], d['detected_at_bar_end'])
           for d in detections}
    assert got == expected, (
        f'live/research parity broken: missing {sorted(expected - got)[:5]}, '
        f'invented {sorted(got - expected)[:5]}')
    assert expected, 'fixture produced no events in the live tail; the test would prove nothing'


def test_confirmation_time_comes_from_the_detector(bars, detections):
    events = research_events(bars)
    for detection in detections:
        key = (detection['pattern_id'], detection['variant'], detection['side'])
        def first_bar(event):
            start = event.get('formation_start_index')
            return bars[int(start)]['time'] if start is not None else event['pattern_start']

        confirmed = [e for e in events[key]
                     if e['state'] == 'confirmed' and first_bar(e) == detection['formation_start']]
        if detection['confirmed_at_bar_end'] is None:
            assert not confirmed
        else:
            assert bars[int(confirmed[0]['signal_index'])]['end'] == detection['confirmed_at_bar_end']


def test_no_detection_claims_a_bar_that_is_not_in_the_window(bars, detections):
    """An incomplete or absent candle can never become a signal."""
    ends = {b['end'] for b in bars}
    starts = {b['time'] for b in bars}
    for d in detections:
        assert d['formation_start'] in starts
        assert d['detected_at_bar_end'] in ends
        assert d['signal_at_bar_end'] in ends
        assert d['candle_end'] == bars[-1]['end']
        assert d['confirmed_at_bar_end'] in ends or d['confirmed_at_bar_end'] is None


# ----------------------------------------------------------- timestamp identity
def test_detection_id_survives_a_different_fetch_depth():
    """Keys are timestamps + spec identity, never bar indices (contract 4)."""
    full = synthetic(420)
    short = full[60:]
    short[0] = dict(short[0], gap=False)
    a = pattern_live.detect_live(full, '1D', symbol='TEST', grace=None)
    b = pattern_live.detect_live(short, '1D', symbol='TEST', grace=None)
    keyed_a = {d['detection_id']: d for d in a}
    keyed_b = {d['detection_id']: d for d in b}
    shared = set(keyed_a) & set(keyed_b)
    assert shared, 'the two windows shared no detection at all'
    for detection_id in shared:
        for field in ('formation_start', 'detected_at_bar_end', 'signal_at_bar_end',
                      'confirmed_at_bar_end', 'state', 'episode_id', 'pattern_id',
                      'variant', 'side'):
            assert keyed_a[detection_id][field] == keyed_b[detection_id][field], field


def test_episode_id_is_stable_across_states():
    bars = synthetic(300, seed=11)
    grouped = {}
    for d in pattern_live.detect_live(bars, '1D', symbol='TEST', grace=None):
        grouped.setdefault(d['episode_id'], set()).add(
            (d['pattern_id'], d['variant'], d['side'], d['formation_start']))
    assert all(len(v) == 1 for v in grouped.values())


# ------------------------------------------------------------------ lifecycle
def _bar(day, o, h, l, c, gap=False):
    return {'time': f'2024-01-{day:02d} 09:15:00', 'end': f'2024-01-{day:02d} 15:30:00',
            'open': o, 'high': h, 'low': l, 'close': c, 'volume': 1000.0, 'gap': gap}


def _event(state, signal, detected, start, geometry=None, direction='bullish'):
    event = {'signal_index': signal, 'episode': start, 'state': state, 'atr': 1.0, 'score': 0.9,
             'direction': direction, 'pattern_start': f'2024-01-{start + 1:02d} 09:15:00',
             'formation_start_index': start, 'detected_index': detected}
    if geometry is not None:
        event['geometry'] = geometry
    if state == 'confirmed':
        event['confirmed_index'] = signal
    return event


def test_setup_without_confirmation_is_forming_then_expires():
    bars = [_bar(i + 1, 100, 101, 99, 100) for i in range(7)]
    setup = _event('setup', 2, 2, 0)
    state, reason, _at, detail = pattern_live.resolve_state(bars[:4], setup, None, 0, 2,
                                                           'candlestick', True)
    assert (state, reason) == ('forming', 'awaiting_confirmation')
    assert detail['expiry_bars'] == pattern_live.FAMILY_EXPIRY['candlestick']
    # The last bar that may still confirm is detected + expiry (index 5 here), so the
    # setup is only expired once a bar after it has closed.
    state, _r, _at, _d = pattern_live.resolve_state(bars[:6], setup, None, 0, 2, 'candlestick', True)
    assert state == 'forming'
    state, reason, _at, _detail = pattern_live.resolve_state(bars, setup, None, 0, 2,
                                                            'candlestick', True)
    assert (state, reason) == ('expired', 'tracking_window_elapsed')


def test_close_through_the_failure_level_invalidates():
    bars = [_bar(i + 1, 100, 101, 99, 100) for i in range(4)]
    bars[3] = _bar(4, 100, 100, 90, 90)          # closes well under the formation low
    setup = _event('setup', 2, 2, 0)
    state, reason, at, detail = pattern_live.resolve_state(bars, setup, None, 0, 2,
                                                          'candlestick', True)
    assert (state, reason) == ('invalidated', 'closed_through_failure_level')
    assert at == bars[3]['end']
    assert detail['failure_level_source'] == 'live_structure'


def test_detector_published_failure_level_wins_over_the_live_fallback():
    bars = [_bar(i + 1, 100, 101, 99, 100) for i in range(4)]
    bars[3] = _bar(4, 100, 100, 97, 97.5)
    setup = _event('setup', 2, 2, 0, geometry={'failure_level': 98.0, 'expiry_bars': 7})
    state, reason, _at, detail = pattern_live.resolve_state(bars, setup, None, 0, 2, 'chart', True)
    assert (state, reason) == ('invalidated', 'closed_through_failure_level')
    assert detail == {**detail, 'failure_level': 98.0, 'failure_level_source': 'detector',
                      'expiry_bars': 7}


def test_confirmed_stays_confirmed_inside_its_window(monkeypatch):
    bars = [_bar(i + 1, 100, 101, 99, 100) for i in range(4)]
    confirm = _event('confirmed', 3, 2, 0)
    state, reason, at, _detail = pattern_live.resolve_state(bars, None, confirm, 0, 2,
                                                            'candlestick', True)
    assert (state, reason) == ('confirmed', 'detector_confirmation')
    assert at == bars[3]['end']


def test_a_quality_gap_expires_the_setup_it_cannot_span():
    bars = [_bar(i + 1, 100, 101, 99, 100) for i in range(4)]
    bars[3] = _bar(4, 100, 101, 99, 100, gap=True)
    setup = _event('setup', 2, 2, 0)
    state, reason, at, _detail = pattern_live.resolve_state(bars, setup, None, 0, 2,
                                                            'candlestick', True)
    assert (state, reason) == ('expired', 'segment_break')
    assert at == bars[3]['end']


def test_harmonic_expiry_comes_from_the_detector_geometry():
    bars = [_bar(i + 1, 100, 101, 99, 100) for i in range(8)]
    setup = _event('setup', 2, 2, 0,
                   geometry={'confirmation': {'window': 10, 'level': 120.0, 'failure_level': 50.0}})
    state, _reason, _at, detail = pattern_live.resolve_state(bars, setup, None, 0, 2,
                                                             'harmonic', True)
    assert detail['expiry_bars'] == 10 and detail['failure_level'] == 50.0
    assert state == 'forming'


def test_every_lifecycle_state_is_reachable_on_the_fixture(bars, detections):
    observed = {d['state'] for d in detections}
    assert {'forming', 'confirmed'} <= observed
    assert observed <= {'forming', 'confirmed', 'invalidated', 'expired'}
    assert all(d['live'] == (d['state'] in ('forming', 'confirmed')) for d in detections)


def test_terminal_grace_keeps_only_recent_transitions(bars):
    graced = pattern_live.detect_live(bars, '1D', symbol='TEST')
    for d in graced:
        assert d['live'] or d['bars_since_state'] <= pattern_live.TERMINAL_GRACE
    assert len(graced) < len(pattern_live.detect_live(bars, '1D', symbol='TEST', grace=None))


# ------------------------------------------------------------------- identity
def test_detector_spec_hash_is_stable_and_covers_the_detector_files():
    first = pattern_live.detector_spec_hash()
    assert first == pattern_live.detector_spec_hash()
    assert len(first) == 64
    for name in pattern_live.DETECTOR_FILES:
        assert (pattern_live.ROOT / name).exists(), name


def test_evidence_is_never_claimed_when_the_identity_differs(monkeypatch):
    monkeypatch.setitem(pattern_live._RESEARCH, 'fake-run',
                        {'run': 'fake-run', 'status': 'known', 'detector_spec_hash': 'different'})
    monkeypatch.setenv('SCANNER_RESEARCH_RUN', 'fake-run')
    result = pattern_live.evidence_identity()
    assert result['status'] == 'detector_mismatch'
    assert 'no compatible historical evidence' in result['note']


def test_missing_research_run_is_unavailable_not_a_name_match(monkeypatch):
    monkeypatch.setenv('SCANNER_RESEARCH_RUN', 'no-such-run-id')
    pattern_live._RESEARCH.pop('no-such-run-id', None)
    assert pattern_live.research_identity()['status'] == 'missing'
    assert pattern_live.evidence_identity()['status'] == 'unavailable'


# ------------------------------------------------------------------ warm-up gate
def _report(**over):
    table = {f'{family}|{tf}': {'family': family, 'timeframe': tf, 'required_bars': bars}
             for tf, rows in (('1D', {'chart': 260, 'candlestick': 100}),
                              ('1W', {'chart': 260, 'candlestick': 100}))
             for family, bars in rows.items()}
    base = {'version': 1, 'status': 'pass', 'live_rules_version': pattern_live.LIVE_RULES_VERSION,
            'detector_spec_hash': pattern_live.detector_spec_hash(), 'table': table,
            'required_bars_by_timeframe': {'1D': 260, '1W': 260}}
    base.update(over)
    return base


@pytest.fixture(autouse=True)
def _reset_warmup_cache():
    pattern_live._WARMUP = None
    yield
    pattern_live._WARMUP = None


def test_missing_warmup_report_refuses_the_research_set(tmp_path):
    with pytest.raises(pattern_live.WarmupError, match='No warm-up measurement'):
        pattern_live.warmup(tmp_path / 'absent.json')


def test_failed_warmup_report_refuses_the_research_set(tmp_path):
    path = tmp_path / 'w.json'
    path.write_text(json.dumps(_report(status='incomplete', failed_cells=['chart|1H'])))
    with pytest.raises(pattern_live.WarmupError, match='did not pass'):
        pattern_live.warmup(path)


def test_warmup_measured_against_other_detector_code_is_rejected(tmp_path):
    path = tmp_path / 'w.json'
    path.write_text(json.dumps(_report(detector_spec_hash='stale')))
    with pytest.raises(pattern_live.WarmupError, match='different detector spec hash'):
        pattern_live.warmup(path)


def test_warmup_measured_under_other_live_rules_is_rejected(tmp_path):
    path = tmp_path / 'w.json'
    path.write_text(json.dumps(_report(live_rules_version='0.0.1')))
    with pytest.raises(pattern_live.WarmupError, match='live rules'):
        pattern_live.warmup(path)


def test_required_bars_has_no_blanket_default(tmp_path):
    path = tmp_path / 'w.json'
    path.write_text(json.dumps(_report()))
    assert pattern_live.required_bars('1D', path) == 260
    with pytest.raises(pattern_live.WarmupError, match='No measured warm-up'):
        pattern_live.required_bars('1H', path)
    with pytest.raises(pattern_live.WarmupError, match='No measured warm-up'):
        pattern_live.family_bars('4H', path)


def test_a_family_short_of_its_warm_up_is_not_run(tmp_path):
    """A short window must drop the family, never run it and label it anyway."""
    path = tmp_path / 'w.json'
    path.write_text(json.dumps(_report()))
    assert pattern_live.minimum_bars('1W', path) == 100
    assert pattern_live.families_for('1W', 150, path) == (['candlestick'], ['chart'])
    assert pattern_live.families_for('1W', 300, path) == (['candlestick', 'chart'], [])
    assert pattern_live.families_for('1W', 50, path) == ([], ['candlestick', 'chart'])


def test_detect_live_honours_the_family_restriction(bars):
    only = pattern_live.detect_live(bars, '1D', symbol='TEST', grace=None,
                                    families=['candlestick'])
    assert only and {d['family'] for d in only} == {'candlestick'}
    assert pattern_live.detect_live(bars, '1D', symbol='TEST', families=[]) == []


def test_shipped_warmup_report_matches_this_build():
    """The measurement in the repository must belong to this detector code."""
    if not pattern_live.WARMUP_REPORT.exists():
        pytest.skip('warm-up has not been measured on this machine')
    report = pattern_live.warmup(pattern_live.WARMUP_REPORT)
    assert report['status'] == 'pass'
    assert not report.get('coverage_gaps')
    for timeframe, value in report['required_bars_by_timeframe'].items():
        assert value and value >= 60, timeframe


# ----------------------------------------------------------------- the switch
def test_pattern_set_defaults_to_legacy(monkeypatch):
    monkeypatch.delenv('SCANNER_PATTERN_SET', raising=False)
    assert pattern_live.pattern_set() == pattern_live.LEGACY
    assert pattern_live.pattern_set({}) == pattern_live.LEGACY


def test_pattern_set_env_wins_and_rejects_nonsense(monkeypatch):
    monkeypatch.setenv('SCANNER_PATTERN_SET', 'research')
    assert pattern_live.pattern_set({'pattern_set': 'legacy'}) == pattern_live.RESEARCH
    monkeypatch.setenv('SCANNER_PATTERN_SET', 'everything')
    with pytest.raises(ValueError, match='SCANNER_PATTERN_SET'):
        pattern_live.pattern_set()


def test_legacy_scan_is_unchanged(monkeypatch, tmp_path):
    """With the switch off the scanner must produce the legacy matches exactly."""
    from market_scanner import engine

    monkeypatch.delenv('SCANNER_PATTERN_SET', raising=False)
    monkeypatch.delenv('SCANNER_CANDLE_SOURCE', raising=False)
    config = load_config()
    scanner = engine.Scanner.__new__(engine.Scanner)
    scanner.config = config
    scanner.pattern_set = pattern_live.LEGACY
    scanner.candle_source = 'legacy'
    scanner.cells = {}
    scanner.calendar = None
    scanner.market15_snapshot = None
    scanner.research = {}

    candles = synthetic(300)
    monkeypatch.setattr(engine.Scanner, 'load_candles',
                        lambda self, symbol, timeframes, cutoff: (None, [], None))
    monkeypatch.setattr(engine, 'aggregate',
                        lambda rows, tf, cal, cutoff, limit: (candles[-limit:], {'invalid_rows': 0}))

    stock = {'symbol': 'TEST', 'company': 'Test', 'sector': 'Unclassified'}
    cells = scanner.scan_stock(stock, ['1D'], datetime(2026, 9, 15, 15, 30), True)
    (_symbol, _tf, cell), = cells

    expected = detectors.detect(candles[-config['history_bars']:], config['enabled_patterns'])
    for match in expected:
        match.update({'id': 'TEST:1D:' + match['pattern'], 'symbol': 'TEST',
                      'company': 'Test', 'sector': 'Unclassified', 'timeframe': '1D'})
    assert cell['matches'] == expected
    assert cell['bar_count'] == config['history_bars']
    assert 'detection_bars' not in cell
    assert scanner.history_bars('1D') == config['history_bars']
    assert scanner.history_window(['1H', '1D']) is None


def test_research_scan_cell_reports_what_ran(monkeypatch):
    """A research cell must say how much history it detected on and which
    families were runnable, and its match indices must land on the bars it keeps."""
    from market_scanner import engine

    if not pattern_live.WARMUP_REPORT.exists():
        pytest.skip('warm-up has not been measured on this machine')
    config = load_config()
    scanner = engine.Scanner.__new__(engine.Scanner)
    scanner.config = config
    scanner.pattern_set = pattern_live.RESEARCH
    scanner.candle_source = 'legacy'
    scanner.cells = {}
    scanner.calendar = None
    scanner.market15_snapshot = 'snap-test'
    scanner.research = {'spec_hash': pattern_live.detector_spec_hash(),
                        'evidence': pattern_live.evidence_identity(),
                        'catalogue': [],
                        'required_bars': {tf: pattern_live.required_bars(tf)
                                          for tf in config['timeframes']}}

    candles = synthetic(400)
    monkeypatch.setattr(engine.Scanner, 'load_candles',
                        lambda self, symbol, timeframes, cutoff: (None, [], None))
    monkeypatch.setattr(engine, 'aggregate',
                        lambda rows, tf, cal, cutoff, limit: (candles[-limit:], {'invalid_rows': 0}))

    stock = {'symbol': 'TEST', 'company': 'Test', 'sector': 'Unclassified'}
    (_s, _tf, cell), = scanner.scan_stock(stock, ['1D'], datetime(2026, 9, 15, 15, 30), True)
    assert cell['status'] == 'scanned'
    assert cell['detection_bars'] == pattern_live.required_bars('1D')
    assert cell['families_scanned'] and not cell['families_insufficient_history']
    assert cell['matches']
    for match in cell['matches']:
        assert 0 <= match['start_index'] <= match['end_index'] < len(cell['bars'])
        assert cell['bars'][match['start_index']]['time'] == match['formation_start']
        assert match['input']['snapshot_id'] == 'snap-test'
        assert match['evidence_status'] in ('identity_match', 'detector_mismatch', 'unavailable')
        _check_lines(match, cell['bars'])
    # The snapshot commit serialises the whole cell with allow_nan=False.
    assert json.loads(engine.dumps(cell))['symbol'] == 'TEST'


def test_research_scan_drops_families_it_cannot_warm_up(monkeypatch):
    from market_scanner import engine

    if not pattern_live.WARMUP_REPORT.exists():
        pytest.skip('warm-up has not been measured on this machine')
    config = load_config()
    need = pattern_live.family_bars('1D')
    short = min(need.values()) + 1
    assert short < max(need.values()), 'families must differ for this test to mean anything'
    scanner = engine.Scanner.__new__(engine.Scanner)
    scanner.config = config
    scanner.pattern_set = pattern_live.RESEARCH
    scanner.candle_source = 'legacy'
    scanner.cells = {}
    scanner.calendar = None
    scanner.market15_snapshot = None
    scanner.research = {'spec_hash': pattern_live.detector_spec_hash(),
                        'evidence': pattern_live.evidence_identity(), 'catalogue': [],
                        'required_bars': {tf: pattern_live.required_bars(tf)
                                          for tf in config['timeframes']}}
    candles = synthetic(400)[:short]
    monkeypatch.setattr(engine.Scanner, 'load_candles',
                        lambda self, symbol, timeframes, cutoff: (None, [], None))
    monkeypatch.setattr(engine, 'aggregate',
                        lambda rows, tf, cal, cutoff, limit: (candles, {'invalid_rows': 0}))
    stock = {'symbol': 'TEST', 'company': 'Test', 'sector': 'Unclassified'}
    (_s, _tf, cell), = scanner.scan_stock(stock, ['1D'], datetime(2026, 9, 15, 15, 30), True)
    assert cell['status'] == 'scanned'
    assert cell['families_insufficient_history'], 'a short window must name what it could not run'
    assert set(cell['families_scanned']) & {'candlestick', 'price_action'}
    assert {m['family'] for m in cell['matches']} <= set(cell['families_scanned'])


def test_research_loader_window_comes_from_the_measurement():
    from market_scanner import engine

    scanner = engine.Scanner.__new__(engine.Scanner)
    scanner.pattern_set = pattern_live.RESEARCH
    scanner.research = {'required_bars': {'1H': 300, '4H': 260, '1D': 200, '1W': 130}}
    window = scanner.history_window(['1H', '4H', '1D', '1W'])
    assert window['sessions'] == max(-(-300 // 6), -(-260 // 2)) + 10
    assert window['daily_bars'] == max(200, 130 * 5) + 10
    assert scanner.history_bars('4H') == 260


# --------------------------------------------------- real data (CAS + parity)
_M15 = market15_path()
needs_market15 = pytest.mark.skipif(not _M15.exists(), reason='db/market15.db is not present')


@pytest.fixture(scope='module')
def cas_bars():
    """1H candles for a CAS (F&O) symbol straight off the live path."""
    from market_data.calendar import REGIME_CAS, RegimeBook, SessionRegime
    from market_data.live.calendar_ext import live_calendar
    from market_scanner.data import (aggregate_market15, load_market15, market15_regimes,
                                     open_market15)

    cutoff = datetime(2026, 9, 15, 15, 30)
    with open_market15(_M15) as store:
        regimes = market15_regimes(store)
        symbol = next((s for s in ('RELIANCE', 'TITAN', 'INFY') if s in regimes), None)
        if symbol is None:
            pytest.skip('no CAS symbol in the store')
        rows = load_market15(store, symbol, True, cutoff, sessions=120)
    book = RegimeBook([SessionRegime(symbol, regimes[symbol], REGIME_CAS, 'stored')])
    calendar = live_calendar().for_symbol(symbol, book)
    bars, _quality = aggregate_market15(rows, '1H', calendar, None, cutoff, 400)
    return symbol, regimes[symbol], bars


@needs_market15
def test_cas_session_end_is_respected_end_to_end(cas_bars):
    """Contract 2A: from 2026-08-03 a CAS symbol's last 1H bucket ends 15:15."""
    symbol, cas_from, bars = cas_bars
    assert bars, 'no candles for the CAS symbol'
    after = [b for b in bars if date.fromisoformat(b['end'][:10]) >= cas_from]
    assert after, 'window holds no post-CAS candles'
    assert all(b['end'][11:16] <= '15:15' for b in after), 'a candle ran past the CAS session end'
    detections = pattern_live.detect_live(bars, '1H', symbol=symbol, grace=None)
    ends = {b['end'] for b in bars}
    assert all(d['signal_at_bar_end'] in ends for d in detections)
    assert all(d['candle_end'] == bars[-1]['end'] for d in detections)


@needs_market15
def test_real_data_parity_live_vs_research(cas_bars):
    symbol, _cas_from, bars = cas_bars
    events = research_events(bars, '1H')
    detections = pattern_live.detect_live(bars, '1H', symbol=symbol, grace=None)
    tail_start = len(bars) - pattern_live.LIVE_TAIL
    expected = set()
    for key, rows in events.items():
        for event in rows:
            detected = int(event.get('detected_index', event['signal_index']))
            if max(detected, int(event['signal_index'])) < tail_start:
                continue
            start = event.get('formation_start_index')
            start = int(start) if start is not None else None
            first = bars[start]['time'] if start is not None else event['pattern_start']
            expected.add((key[0], key[1], key[2], first))
    got = {(d['pattern_id'], d['variant'], d['side'], d['formation_start']) for d in detections}
    assert expected, 'no research events in the compared tail'
    assert got == expected


@needs_market15
def test_research_run_identity_matches_the_running_code():
    identity = pattern_live.research_identity()
    if identity['status'] != 'known':
        pytest.skip('research run manifest is not on this machine')
    assert identity['detector_spec_hash'] == pattern_live.detector_spec_hash(), (
        'live detector code differs from the research run it claims evidence from')


# ------------------------------------------------------------------- overlays
def _check_lines(detection, bars):
    """Every registered variant draws something, or says why it does not."""
    lines, note = detection['lines'], detection['geometry_note']
    assert bool(lines) != bool(note), (
        f'{detection["pattern_id"]}/{detection["variant"]}: lines and note must be '
        f'exclusive, got {len(lines)} lines and note {note!r}')
    assert detection['drawing_plan'], 'every cell declares a drawing plan'
    for line in lines:
        assert line['role'] in ('boundary', 'curve', 'anchors', 'shape', 'label', 'candle_range'), line['role']
        assert isinstance(line['label'], str) and line['label']
        assert line['points'], f'{line["label"]} has no points'
        for point in line['points']:
            assert isinstance(point['index'], int), point
            assert 0 <= point['index'] < len(bars), (
                f'{detection["pattern_id"]} {line["label"]} point {point["index"]} '
                f'is outside the served window of {len(bars)} candles')
            assert math.isfinite(point['value']) and point['value'] > 0, point
        if line['role'] == 'boundary':
            assert len(line['points']) >= 2, f'{line["label"]} is a boundary with one point'


def test_every_detection_is_drawable_or_says_why(bars, detections):
    assert detections
    for detection in detections:
        _check_lines(detection, bars)


def test_declared_drawing_plan_covers_every_registered_cell():
    """No registered variant may fall through to an undeclared recipe."""
    from market_scanner import pattern_lines
    _modules, specs, _by_key, _family = pattern_live.research_registry()
    plans = {(s['pattern_id'], s['variant']): pattern_lines.plan_for(s) for s in specs}
    assert len(plans) == 205, len(plans)
    assert all(plans.values()), [k for k, v in plans.items() if not v]
    legacy = {k for k, v in plans.items() if v == pattern_lines.LEGACY_PLAN}
    assert {k[0] for k in legacy} == {f'CH{n:02d}' for n in range(1, 11)}


def test_lines_and_note_are_mutually_exclusive_on_empty_geometry(bars):
    from market_scanner import pattern_lines
    _modules, _specs, by_key, _f = pattern_live.research_registry()
    spec = by_key[('CH25', 'v_bottom', 'long')]
    # A chart detection draws only what its detector published. With no geometry
    # there is nothing to draw, and the caller must be told so explicitly.
    for geometry in (None, {}):
        lines, note = pattern_lines.build(spec, {'score': 1.0, 'geometry': geometry}, bars,
                                          formation_start=10, signal_index=20)
        assert (lines, note) == ([], pattern_lines.NOTE_NO_GEOMETRY)
    # ...unless the lifecycle has a level to show, which is drawn and labelled.
    lines, note = pattern_lines.build(spec, {'score': 1.0, 'geometry': {}}, bars,
                                      formation_start=10, signal_index=20,
                                      tracking={'failure_level': 95.0,
                                                'failure_level_source': 'live_structure'})
    assert not note and [l['label'] for l in lines] == ['Failure level (live rule)']


def test_out_of_window_formation_is_a_note_not_a_bad_line(bars):
    from market_scanner import pattern_lines
    _modules, _specs, by_key, _f = pattern_live.research_registry()
    spec = by_key[('CH25', 'v_bottom', 'long')]
    lines, note = pattern_lines.build(spec, {'score': 1.0, 'geometry': {'support': 100.0}}, bars,
                                      formation_start=-5, signal_index=20)
    assert (lines, note) == ([], pattern_lines.NOTE_OUT_OF_WINDOW)


def test_harmonic_draws_its_legs_and_ratio_labels():
    from market_scanner import pattern_lines
    _modules, _specs, by_key, _f = pattern_live.research_registry()
    spec = by_key[('HA01', 'canonical', 'long')]
    window = synthetic(60, seed=5)
    geometry = {'points': {'A': {'index': 5, 'price': 100.0, 'kind': 'high'},
                           'B': {'index': 15, 'price': 90.0, 'kind': 'low'},
                           'C': {'index': 22, 'price': 96.0, 'kind': 'high'},
                           'D': {'index': 30, 'price': 86.0, 'kind': 'low'}},
                'ratios': {'BC/AB': 0.6, 'CD/AB': 1.0},
                'prz': {'ratio': 'CD/AB', 'price_band': [85.0, 87.0]},
                'confirmation': {'window': 10, 'level': 97.0, 'failure_level': 85.5}}
    lines, note = pattern_lines.build(spec, {'score': 1.0, 'geometry': geometry}, window,
                                      formation_start=5, signal_index=33)
    assert not note
    labels = {l['label']: l for l in lines}
    assert labels['XABCD legs']['role'] == 'shape'
    assert [p['index'] for p in labels['XABCD legs']['points']] == [5, 15, 22, 30]
    assert [p['value'] for p in labels['XABCD legs']['points']] == [100.0, 90.0, 96.0, 86.0]
    assert {'A', 'B', 'C', 'D'} <= set(labels)
    assert any(l.startswith('BC/AB') for l in labels), labels.keys()
    assert labels['PRZ low']['points'][0]['value'] == 85.0
    assert labels['Trigger level']['points'][0]['value'] == 97.0
    _check_lines({'lines': lines, 'geometry_note': note, 'pattern_id': 'HA01',
                  'variant': 'canonical', 'drawing_plan': 'x'}, window)


def test_chart_boundaries_use_the_published_level_and_fit():
    from market_scanner import pattern_lines
    _modules, _specs, by_key, _f = pattern_live.research_registry()
    spec = by_key[('CH13', 'upside', 'long')]
    window = synthetic(80, seed=6)
    geometry = {'resistance': 120.0, 'support': 100.0,
                'resistance_touches': [10, 20], 'support_touches': [14, 24],
                'confirm_level': 121.0}
    lines, note = pattern_lines.build(spec, {'score': 1.0, 'geometry': geometry}, window,
                                      formation_start=8, signal_index=30)
    assert not note
    labels = {l['label']: l for l in lines}
    assert [p['value'] for p in labels['Resistance']['points']] == [120.0, 120.0]
    assert [p['index'] for p in labels['Resistance']['points']] == [8, 30]
    assert labels['Resistance tests']['role'] == 'anchors'
    assert [p['index'] for p in labels['Resistance tests']['points']] == [10, 20]
    assert [p['value'] for p in labels['Resistance tests']['points']] == [
        window[10]['high'], window[20]['high']]
    assert labels['Trigger level']['points'][0]['value'] == 121.0
    # Resistance + Support is what makes the app draw the shaded envelope.
    assert {'Resistance', 'Support'} <= set(labels)


def test_sloped_fit_is_evaluated_from_the_detectors_own_line():
    from market_scanner import pattern_lines
    _modules, _specs, by_key, _f = pattern_live.research_registry()
    spec = by_key[('CH22', 'upside', 'long')]
    window = synthetic(80, seed=7)
    geometry = {'upper_line': [20.0, 110.0, 0.5], 'lower_line': [20.0, 100.0, 0.25]}
    lines, _note = pattern_lines.build(spec, {'score': 1.0, 'geometry': geometry}, window,
                                       formation_start=10, signal_index=40)
    labels = {l['label']: l for l in lines}
    assert labels['Resistance']['points'] == [{'index': 10, 'value': 105.0},
                                              {'index': 40, 'value': 120.0}]
    assert labels['Support']['points'] == [{'index': 10, 'value': 97.5},
                                           {'index': 40, 'value': 105.0}]


def test_candlestick_draws_the_marked_range_and_its_levels():
    from market_scanner import pattern_lines
    _modules, _specs, by_key, _f = pattern_live.research_registry()
    spec = by_key[('CDLENGULFING', 'canonical', 'long')]
    window = synthetic(40, seed=8)
    geometry = {'high': 105.0, 'low': 99.0, 'confirm_level': 105.6, 'color': 'white'}
    lines, note = pattern_lines.build(
        spec, {'score': 1.0, 'geometry': geometry}, window, formation_start=20, signal_index=21,
        tracking={'failure_level': 98.4, 'failure_level_source': 'live_structure'})
    assert not note
    labels = {l['label']: l for l in lines}
    assert labels['Pattern high']['points'][0]['value'] == 105.0
    assert labels['Pattern low']['points'][0]['value'] == 99.0
    assert labels['Trigger level']['points'][0]['value'] == 105.6
    # The live fallback must be visibly labelled as the live rule, not the detector's.
    assert labels['Failure level (live rule)']['points'][0]['value'] == 98.4
    assert 'Failure level' not in labels


def test_detector_published_failure_level_is_not_labelled_as_the_live_rule():
    from market_scanner import pattern_lines
    _modules, _specs, by_key, _f = pattern_live.research_registry()
    spec = by_key[('CH26', 'bull', 'long')]
    window = synthetic(60, seed=9)
    lines, _note = pattern_lines.build(
        spec, {'score': 1.0, 'geometry': {'breakout_level': 110.0}}, window,
        formation_start=10, signal_index=30,
        tracking={'failure_level': 95.0, 'failure_level_source': 'detector'})
    labels = {l['label'] for l in lines}
    assert 'Failure level' in labels and 'Failure level (live rule)' not in labels


def test_price_action_without_a_published_pair_draws_the_marked_candles():
    from market_scanner import pattern_lines
    _modules, _specs, by_key, _f = pattern_live.research_registry()
    spec = by_key[('PA03', 'bullish_close', 'long')]
    window = synthetic(40, seed=10)
    lines, note = pattern_lines.build(
        spec, {'score': 1.0, 'geometry': {'confirmation_level': 111.0}}, window,
        formation_start=25, signal_index=25)
    assert not note
    labels = {l['label']: l for l in lines}
    assert labels['Marked candle high']['points'][0]['value'] == max(
        b['high'] for b in window[25:27])
    assert labels['Trigger level']['points'][0]['value'] == 111.0


def test_price_action_uses_the_published_mother_bar_range():
    from market_scanner import pattern_lines
    _modules, _specs, by_key, _f = pattern_live.research_registry()
    spec = by_key[('PA02', 'first_break_up', 'long')]
    window = synthetic(40, seed=11)
    geometry = {'mother_high': 104.0, 'mother_low': 100.0, 'mother_index': 20,
                'confirmation_level': 104.5, 'inside_count': 1}
    lines, _note = pattern_lines.build(spec, {'score': 1.0, 'geometry': geometry}, window,
                                       formation_start=20, signal_index=22)
    labels = {l['label']: l for l in lines}
    assert labels['Mother bar high']['points'][0]['value'] == 104.0
    assert labels['Mother bar low']['points'][0]['value'] == 100.0
    assert labels['Mother bar']['role'] == 'label'
    assert labels['Mother bar']['points'][0]['index'] == 20


@needs_market15
def test_legacy_chart_overlays_are_recovered_from_the_original_replay(cas_bars):
    """CH01-CH10 publish no geometry; their overlay comes back from the replay."""
    from market_scanner import pattern_lines
    symbol, _cas, window = cas_bars
    found = pattern_live.detect_live(window, '1H', symbol=symbol, grace=None)
    legacy = [d for d in found if d['variant'].startswith('legacy_')]
    if not legacy:
        pytest.skip('no CH01-CH10 detection in this window')
    drawn = [d for d in legacy if d['lines']]
    assert drawn, 'every legacy detection fell back to a note'
    for detection in legacy:
        _check_lines(detection, window)
        assert detection['drawing_plan'] == pattern_lines.LEGACY_PLAN
        if not detection['lines']:
            assert detection['geometry_note'] == pattern_lines.NOTE_LEGACY
    for detection in drawn:
        roles = {l['role'] for l in detection['lines']}
        assert roles & {'boundary', 'shape', 'curve'}, roles


@needs_market15
def test_overlay_points_survive_the_served_window_trim(cas_bars):
    """After trimming, every overlay point must still index a served candle."""
    from market_scanner import engine
    symbol, _cas, window = cas_bars
    scanner = engine.Scanner.__new__(engine.Scanner)
    scanner.config = load_config()
    found = pattern_live.detect_live(window, '1H', symbol=symbol, grace=None)
    trimmed, matches = scanner.trim_window([dict(b) for b in window], found)
    assert matches
    for match in matches:
        _check_lines(match, trimmed)
        assert 0 <= match['start_index'] <= match['end_index'] < len(trimmed)
        assert trimmed[match['start_index']]['time'] == match['formation_start']


@needs_market15
def test_every_observed_variant_on_real_data_is_drawable_or_noted(cas_bars):
    symbol, _cas, window = cas_bars
    seen = {}
    for detection in pattern_live.detect_live(window, '1H', symbol=symbol, grace=None):
        _check_lines(detection, window)
        seen.setdefault((detection['pattern_id'], detection['variant']),
                        bool(detection['lines']))
    assert len(seen) >= 10, seen
    assert sum(seen.values()) >= len(seen) * 0.8, (
        f'only {sum(seen.values())}/{len(seen)} observed variants produced overlays')


# ------------------------------------------------------------------ retention
def _ledger(tmp_path, rows):
    import sqlite3
    con = sqlite3.connect(tmp_path / 'ledger.sqlite3')
    pattern_live.ensure_schema(con)
    for state, last_seen, suffix in rows:
        con.execute('INSERT INTO detections VALUES(' + ','.join('?' * 24) + ')',
                    (f'id{suffix}', f'ep{suffix}', 'AAA', '1D', 'CDLDOJI', 'break_up', 'long',
                     'candlestick', 'hash', pattern_live.LIVE_RULES_VERSION, state, 'r', last_seen,
                     '2026-01-01 09:15:00', '2026-01-01 15:30:00', '2026-01-01 15:30:00', None,
                     'bullish', 1.0, 1.0, last_seen, last_seen, last_seen, '{}'))
    con.commit()
    return con


def test_retention_prunes_terminated_rows_but_never_the_live_book(tmp_path):
    now = datetime(2026, 9, 16, 16, 0)
    old, recent = '2026-01-01 15:30:00', '2026-09-15 15:30:00'
    con = _ledger(tmp_path, [('expired', old, 1), ('invalidated', old, 2),
                             ('expired', recent, 3), ('forming', old, 4),
                             ('confirmed', old, 5)])
    result = pattern_live.prune(con, now, {'detection_retention_days': 90,
                                           'detection_max_rows': 0})
    assert result['by_age'] == 2 and result['by_cap'] == 0
    kept = {r[0] for r in con.execute('SELECT detection_id FROM detections')}
    assert kept == {'id3', 'id4', 'id5'}, 'a live detection was pruned by age'
    assert result['remaining'] == 3
    con.close()


def test_retention_row_cap_drops_the_oldest_terminated_first(tmp_path):
    con = _ledger(tmp_path, [('expired', f'2026-09-{day:02d} 15:30:00', day)
                             for day in range(1, 6)] + [('forming', '2026-01-01 15:30:00', 9)])
    result = pattern_live.prune(con, datetime(2026, 9, 16), {'detection_retention_days': 0,
                                                             'detection_max_rows': 2})
    assert result['by_age'] == 0 and result['by_cap'] == 3
    kept = {r[0] for r in con.execute('SELECT detection_id FROM detections')}
    assert kept == {'id4', 'id5', 'id9'}
    con.close()


def test_retention_can_be_disabled_and_is_configurable(tmp_path, monkeypatch):
    con = _ledger(tmp_path, [('expired', '2020-01-01 15:30:00', 1)])
    assert pattern_live.prune(con, datetime(2026, 9, 16),
                              {'detection_retention_days': 0,
                               'detection_max_rows': 0})['remaining'] == 1
    monkeypatch.setenv('SCANNER_DETECTION_RETENTION_DAYS', '30')
    monkeypatch.setenv('SCANNER_DETECTION_MAX_ROWS', '0')
    assert pattern_live.retention({'detection_retention_days': 90}) == (30, 0)
    assert pattern_live.prune(con, datetime(2026, 9, 16))['remaining'] == 0
    con.close()


def test_retention_rejects_nonsense(monkeypatch):
    monkeypatch.setenv('SCANNER_DETECTION_RETENTION_DAYS', 'forever')
    with pytest.raises(ValueError, match='whole number'):
        pattern_live.retention()
    monkeypatch.setenv('SCANNER_DETECTION_RETENTION_DAYS', '-3')
    with pytest.raises(ValueError, match='negative'):
        pattern_live.retention()


def test_default_retention_is_bounded():
    assert pattern_live.RETENTION_DAYS > 0 and pattern_live.RETENTION_MAX_ROWS > 0


# --------------------------------------------------------------------- server
class _FakeRequest:
    """Minimal socket stand-in so the real handler can be driven in-process."""

    def __init__(self, path):
        import io
        self.sent = io.BytesIO()
        self._read = io.BytesIO(f'GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n'.encode())

    def makefile(self, mode, *a, **kw):
        return self._read if 'r' in mode else self.sent

    def sendall(self, data):
        self.sent.write(data)


def _get(scanner, path):
    from market_scanner.server import make_handler
    from types import SimpleNamespace
    handler = make_handler(SimpleNamespace(scanner=scanner))
    request = _FakeRequest(path)
    handler(request, ('127.0.0.1', 0), None)
    raw = request.sent.getvalue()
    head, _, body = raw.partition(b'\r\n\r\n')
    return int(head.split()[1]), json.loads(body or b'null')


class _StubScanner:
    pattern_set = pattern_live.RESEARCH
    config = {'port': 8765}

    def __init__(self, matches):
        self._matches = matches
        self.stocks = []

    def summaries(self):
        return [dict(m, current=True) for m in self._matches]


def test_matches_endpoint_exposes_pattern_variant_side_and_state(bars):
    rows = pattern_live.detect_live(bars, '1D', symbol='RELIANCE', company='R',
                                    sector='Energy', grace=None)
    rows = [{k: v for k, v in r.items() if k not in ('geometry', 'input', 'evidence', 'lines')}
            for r in rows]
    scanner = _StubScanner(rows)

    status, payload = _get(scanner, '/api/matches?min_trades=0&limit=5000')
    assert status == 200
    assert payload['pattern_set'] == 'research'
    assert payload['total'] == len(rows)
    first = payload['matches'][0]
    for field in ('pattern_id', 'variant', 'side', 'state', 'detector_state', 'family',
                  'evidence_status', 'live', 'formation_start', 'confirmed_at_bar_end'):
        assert field in first, field

    target = rows[0]
    status, filtered = _get(
        scanner, f'/api/matches?min_trades=0&pattern_id={target["pattern_id"]}'
                 f'&variant={target["variant"]}&side={target["side"]}')
    assert status == 200
    assert filtered['matches']
    assert all(m['pattern_id'] == target['pattern_id'] and m['variant'] == target['variant']
               and m['side'] == target['side'] for m in filtered['matches'])

    status, live_only = _get(scanner, '/api/matches?min_trades=0&live=true')
    assert all(m['live'] for m in live_only['matches'])
    status, forming = _get(scanner, '/api/matches?min_trades=0&state=forming')
    assert all(m['state'] == 'forming' for m in forming['matches'])


def test_chart_endpoint_serves_the_overlay_the_app_draws(bars):
    """/api/chart is what PatternCanvas fetches; `lines` must arrive with it."""
    rows = pattern_live.detect_live(bars, '1D', symbol='RELIANCE', grace=None)
    cell = {'symbol': 'RELIANCE', 'timeframe': '1D', 'status': 'scanned',
            'bars': bars, 'matches': rows, 'last_candle': bars[-1]['end']}

    class _ChartScanner(_StubScanner):
        def chart(self, symbol, timeframe):
            return {**cell, 'stock': {'symbol': symbol}, 'current': True}

    status, payload = _get(_ChartScanner(rows), '/api/chart?symbol=RELIANCE&timeframe=1D')
    assert status == 200
    assert len(payload['bars']) == len(bars)
    drawn = [m for m in payload['matches'] if m['lines']]
    assert drawn, 'no match arrived with an overlay'
    for match in payload['matches']:
        _check_lines(match, payload['bars'])


def test_summaries_stay_light_but_keep_the_note_and_the_plan(bars):
    """The list view carries why a pattern is drawable, not the drawing itself."""
    import threading

    from market_scanner import engine
    rows = pattern_live.detect_live(bars, '1D', symbol='RELIANCE', grace=None)
    scanner = engine.Scanner.__new__(engine.Scanner)
    scanner.lock = threading.RLock()
    scanner.config = load_config()
    scanner.cells = {('RELIANCE', '1D'): {'matches': rows}}
    scanner.expected_map = lambda: ({'1D': {'latest_expected': bars[-1]['end']}},) * 2
    scanner.expected_close = lambda tf, symbol, expected=None, cas=None: bars[-1]['end']
    summaries = scanner.summaries()
    assert summaries
    for summary in summaries:
        assert 'lines' not in summary and 'geometry' not in summary
        assert 'evidence' not in summary and 'input' not in summary
        assert 'geometry_note' in summary and 'drawing_plan' in summary
        assert summary['current'] is True


def test_matches_endpoint_never_attaches_legacy_evidence_to_a_research_match(bars):
    rows = [{k: v for k, v in r.items() if k not in ('geometry', 'input', 'evidence', 'lines')}
            for r in pattern_live.detect_live(bars, '1D', symbol='RELIANCE', grace=None)]
    _status, payload = _get(_StubScanner(rows), '/api/matches?min_trades=0')
    assert all(m['history'] == [] for m in payload['matches'])


def test_matches_limit_is_bounded_and_reports_the_true_total(bars):
    rows = [{k: v for k, v in r.items() if k not in ('geometry', 'input', 'evidence', 'lines')}
            for r in pattern_live.detect_live(bars, '1D', symbol='RELIANCE', grace=None)]
    _status, payload = _get(_StubScanner(rows), '/api/matches?min_trades=0&limit=3')
    assert payload['returned'] == 3 and payload['total'] == len(rows)
    assert len(payload['matches']) == 3


# ---------------------------------------------------------------- persistence
def test_persist_upserts_and_keeps_first_seen(bars, tmp_path):
    import sqlite3

    con = sqlite3.connect(tmp_path / 'cache.sqlite3')
    pattern_live.ensure_schema(con)
    records = pattern_live.detect_live(bars, '1D', symbol='TEST', grace=None)[:20]
    assert records
    pattern_live.persist(con, records, '2026-09-16 10:00:00')
    moved = [dict(r, state='expired', state_reason='tracking_window_elapsed') for r in records]
    pattern_live.persist(con, moved, '2026-09-16 11:00:00')
    rows = list(con.execute('SELECT first_seen, last_seen, state FROM detections'))
    assert len(rows) == len(records)
    assert {r[0] for r in rows} == {'2026-09-16 10:00:00'}
    assert {r[1] for r in rows} == {'2026-09-16 11:00:00'}
    assert {r[2] for r in rows} == {'expired'}
    assert pattern_live.live_counts(con) == {'expired': len(records)}
    con.close()
