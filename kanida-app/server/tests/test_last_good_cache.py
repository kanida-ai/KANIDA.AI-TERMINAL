"""BACKLOG item 2a: the app always shows the last scan, and the trade gates still fail closed.

The scanner takes minutes to load ~90,000 setups at startup and can be restarted mid-day. These tests pin
the deal that makes that survivable:

* a DISPLAY read (`cache=True`) replays the last successful body, stamped with when it was taken;
* a TRADE read (the default, `cache=False`) still gets a 503 from the very same warm cache;
* the replay outlives a pilot restart, because it is a file;
* the cache is bounded and never invents or mixes data.
"""
import time
import pytest

from types import SimpleNamespace

import kanida_pilot.evidence as evidence_module
from kanida_pilot.errors import PilotError
from kanida_pilot.evidence import (CACHEABLE, Evidence, RECONNECT_GRACE_SECONDS, STARTING_TEXT,
                                   UNREACHABLE_TEXT, require_current_evidence)
from kanida_pilot.lastgood import LastGood

STATE = {'universe': 500, 'source_latest': '2026-09-17 15:30:00', 'source_stale': False,
         'server_time': '2026-09-17 15:31:02', 'pattern_set': 'legacy',
         'database': r'C:\scanner\market15.db',
         'data_status': {'version': 1, 'as_of': '2026-09-17 15:30:00',
                         'source': {'kind': 'market15', 'label': 'Live 15-minute store', 'live': True}}}
MATCHES = [{'id': 'AAAA:1D:cup_handle', 'symbol': 'AAAA', 'pattern': 'cup_handle', 'history': [{'side': 'long'}]}]


class Response:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            error = RuntimeError(f'HTTP {self.status_code}')
            error.response = self
            raise error

    def json(self):
        return self.payload


class Scanner:
    """A scanner that can be up, refusing connections, or still starting up."""

    def __init__(self):
        self.mode = 'up'
        self.calls = []

    def get(self, url, params=None):
        self.calls.append((url, dict(params or {})))
        if self.mode == 'refused':
            raise ConnectionRefusedError('[WinError 10061] No connection could be made')
        if self.mode == 'starting':
            return Response({'error': 'still loading', 'starting': True, 'loaded_pct': 41}, 503)
        path = url.rsplit('8765', 1)[-1]
        return Response(dict(STATE) if path == '/api/state' else list(MATCHES))


def build(tmp_path, name='last_good.sqlite3', **caps):
    scanner = Scanner()
    settings = SimpleNamespace(research_directory='', research_url='http://127.0.0.1:8765',
                               last_good_path=str(tmp_path / name))
    evidence = Evidence(settings, scanner, last_good=LastGood(str(tmp_path / name), **caps) if caps else None)
    return evidence, scanner


def warm(tmp_path, name='last_good.sqlite3'):
    """An Evidence whose display reads have succeeded once, then a scanner that stops answering."""
    evidence, scanner = build(tmp_path, name)
    assert evidence.get('/api/state', {}, cache=True)['universe'] == 500
    assert evidence.get('/api/matches', {'min_trades': '0'}, cache=True) == MATCHES
    evidence.cache.clear()  # the 30 s in-process cache must not be what answers below
    scanner.mode = 'refused'
    return evidence, scanner


# --- 1. the display keeps working while the scanner is down --------------------------------------------------
def test_a_display_read_serves_the_last_scan_with_its_own_timestamp(tmp_path):
    evidence, _ = warm(tmp_path)
    state = evidence.get('/api/state', {}, cache=True)
    assert state['served_from_cache'] is True
    assert state['upstream_error'] == UNREACHABLE_TEXT
    assert state['reconnecting'] is True and state['upstream_down_seconds'] >= 0
    assert state['cached_at'] and len(state['cached_at']) == 19  # naive IST, like every other stamp in the app
    # the data's OWN as-of time, not the moment it was cached
    assert state['cached_as_of'] == '2026-09-17 15:31:02'
    # and nothing inside the body was invented or changed
    assert {k: v for k, v in state.items() if k in STATE} == {k: v for k, v in STATE.items() if k != 'database'}


def test_a_scanner_that_is_still_loading_says_so_rather_than_reading_as_a_failure(tmp_path):
    evidence, scanner = warm(tmp_path)
    scanner.mode = 'starting'
    assert evidence.get('/api/state', {}, cache=True)['upstream_error'] == STARTING_TEXT


def test_a_list_body_is_replayed_untouched(tmp_path):
    """The legacy /api/matches shape is a list; provenance has nowhere to live, so nothing is bolted on."""
    evidence, _ = warm(tmp_path)
    assert evidence.get('/api/matches', {'min_trades': '0'}, cache=True) == MATCHES


def test_display_matches_still_render_while_the_scanner_is_down(tmp_path):
    evidence, _ = warm(tmp_path)
    assert evidence.display_matches({'min_trades': '0'}) == MATCHES


def test_the_reconnecting_grace_period_ends_and_the_outage_is_reported_as_lasting(tmp_path):
    evidence, _ = warm(tmp_path)
    evidence.get('/api/state', {}, cache=True)
    evidence.outage_since = time.time() - (RECONNECT_GRACE_SECONDS + 60)
    evidence.cache.clear()
    state = evidence.get('/api/state', {}, cache=True)
    assert state['reconnecting'] is False and state['upstream_down_seconds'] > RECONNECT_GRACE_SECONDS


def test_the_outage_clock_resets_the_moment_the_scanner_answers(tmp_path):
    evidence, scanner = warm(tmp_path)
    evidence.get('/api/state', {}, cache=True)
    assert evidence.outage_since is not None
    scanner.mode = 'up'
    evidence.cache.clear()
    assert evidence.get('/api/state', {}, cache=True).get('served_from_cache') is None
    assert evidence.outage_since is None and evidence.down_seconds() == 0


# --- 2. it survives a pilot restart --------------------------------------------------------------------------
def test_the_last_scan_survives_a_pilot_restart(tmp_path):
    first, _ = build(tmp_path)
    assert first.get('/api/state', {}, cache=True)['universe'] == 500
    # a brand new process: new Evidence, new in-memory cache, same file on disk, scanner down from the start
    second, scanner = build(tmp_path)
    scanner.mode = 'refused'
    state = second.get('/api/state', {}, cache=True)
    assert state['served_from_cache'] is True and state['universe'] == 500


def test_nothing_cached_at_all_is_still_an_honest_failure(tmp_path):
    """First ever run: there is no last scan to show, so the app is told so instead of being shown a fiction."""
    evidence, scanner = build(tmp_path, 'empty.sqlite3')
    scanner.mode = 'refused'
    with pytest.raises(PilotError) as error:
        evidence.get('/api/state', {}, cache=True)
    assert (error.value.status, error.value.code) == (503, 'RESEARCH_UNAVAILABLE')


# --- 3. the trade gates still fail closed --------------------------------------------------------------------
def test_a_trade_read_is_refused_from_the_very_same_warm_cache(tmp_path):
    evidence, _ = warm(tmp_path)
    assert evidence.get('/api/state', {}, cache=True)['served_from_cache'] is True   # display: served
    evidence.cache.clear()
    for path, params in (('/api/state', {}), ('/api/matches', {'min_trades': '0'}), ('/api/chart', {'symbol': 'AAAA'})):
        with pytest.raises(PilotError) as error:
            evidence.get(path, params)                                               # trade: refused
        assert (error.value.status, error.value.code) == (503, 'RESEARCH_UNAVAILABLE'), path


def test_the_setup_lookup_and_exit_plan_still_fail_closed(tmp_path):
    evidence, _ = warm(tmp_path)
    for call in (lambda: evidence.matches(), lambda: evidence.match('AAAA:1D:cup_handle'),
                 lambda: evidence.exit_plan('AAAA:1D:cup_handle', 'long')):
        with pytest.raises(PilotError) as error:
            call()
        assert error.value.status == 503


def test_a_saved_plan_cannot_be_simulated_or_sent_live_on_cached_data(tmp_path):
    """require_current_evidence reads /api/state WITHOUT the cache flag, so a cached scan can never
    satisfy the DATA_STALE check - the plan is refused for as long as the scanner is unreachable."""
    evidence, _ = warm(tmp_path)
    payload = dict(symbol='AAAA', pattern_name='Cup & Handle', timeframe='1D', side='long', quantity=1,
                   notional=100, reserved_cost=.4, planned_risk=3, snapshot_price=100, stop_pct=2, reward=2,
                   exit_mode='suggested', evidence_applies=True, match_id='AAAA:1D:cup_handle')
    with pytest.raises(PilotError) as error:
        require_current_evidence(evidence, payload, 3)
    assert error.value.status >= 409 and error.value.code != 'OK'


def test_the_in_process_cache_never_leaks_a_replay_into_a_fail_closed_read(tmp_path):
    """A display read parks the replayed body in the 30 s in-process cache. The trade path must not read it."""
    evidence, _ = warm(tmp_path)
    assert evidence.get('/api/state', {}, cache=True)['served_from_cache'] is True
    with pytest.raises(PilotError):
        evidence.get('/api/state', {})


def test_position_sizing_is_never_replayed(tmp_path):
    """/api/backtests/capital turns a cell into a share count. A size computed on a stale snapshot must
    never be shown as current, so it is in PATHS but deliberately not in CACHEABLE."""
    assert '/api/backtests/capital' not in CACHEABLE
    assert '/api/backtests/capital' in evidence_module.PATHS
    evidence, scanner = build(tmp_path, 'capital.sqlite3')
    params = {'symbol': 'AAAA', 'timeframe': '1D', 'pattern': 'cup_handle', 'side': 'long'}
    evidence.get('/api/backtests/capital', params, cache=True)
    evidence.cache.clear()
    scanner.mode = 'refused'
    with pytest.raises(PilotError) as error:
        evidence.get('/api/backtests/capital', params, cache=True)
    assert error.value.code == 'RESEARCH_UNAVAILABLE'


# --- 4. it stays small ---------------------------------------------------------------------------------------
def test_the_cache_is_bounded_and_evicts_the_oldest_first(tmp_path):
    store = LastGood(str(tmp_path / 'bounded.sqlite3'), max_entries=3, max_total_bytes=10_000)
    for index in range(6):
        assert store.put('/api/chart', f'key{index}', {'rows': [index]}) is True
        time.sleep(0.01)
    stats = store.stats()
    assert stats['entries'] == 3 and stats['bytes'] < 10_000
    assert store.get('key0') is None and store.get('key5') is not None


def test_a_body_too_large_to_be_worth_keeping_is_not_kept(tmp_path):
    store = LastGood(str(tmp_path / 'big.sqlite3'), max_entry_bytes=200)
    assert store.put('/api/chart', 'big', {'rows': ['x' * 500]}) is False
    assert store.get('big') is None


def test_a_broken_cache_file_never_takes_a_live_read_down(tmp_path):
    broken = tmp_path / 'broken.sqlite3'
    broken.write_bytes(b'this is not a database')
    store = LastGood(str(broken))
    store.put('/api/state', 'k', {'a': 1})          # swallowed
    assert store.get('k') in (None, ({'a': 1}, store.get('k')[1] if store.get('k') else 0))
    settings = SimpleNamespace(research_directory='', research_url='http://127.0.0.1:8765', last_good_path=str(broken))
    evidence = Evidence(settings, Scanner())
    assert evidence.get('/api/state', {}, cache=True)['universe'] == 500
