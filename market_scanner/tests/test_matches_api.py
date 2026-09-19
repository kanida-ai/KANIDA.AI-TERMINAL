"""`GET /api/matches` in both pattern sets: the legacy-history screen must never run silently.

What these tests defend
-----------------------
1. **Research mode serves detections.** A research match carries NO legacy history by design, so the
   legacy-history screen is False for every row. It used to run anyway - on a bare request the only
   reason it ran at all was ``performance.DEFAULT_MIN_TRADES`` - and deleted 100% of the book. A bare
   request now returns detections.
2. **The screen is reported, never dropped on the floor.** Every research body carries ``history_screen``
   saying the screen did not apply, why, and where the real evidence is.
3. **An explicit screen is refused, not faked.** ``performance=``/``return_band=``/``min_trades>0`` in
   research mode is a 400 carrying the reason; ``min_trades=0`` is the explicit "any sample size" and is
   not a screen, so it is served.
4. **Legacy mode is unchanged**, down to the exact bytes: still a bare JSON list, still screened by
   ``DEFAULT_MIN_TRADES`` on a bare request, and with no ``history_screen`` anywhere in the body.
"""
from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest

from market_scanner import pattern_live, performance, server


def stats(n):
    return {'n': n, 'win_rate': 70.0, 'expectancy_pct': 1.5, 'expectancy_ci95': [0.4, 2.6],
            'max_adverse_pct': -1.0, 'rule': {'hold': 5}}


def legacy_match(symbol, n):
    """A legacy match AFTER backtest_store.match_history has attached its history."""
    return {'id': f'{symbol}-1D-channel', 'symbol': symbol, 'company': symbol + ' Ltd.', 'pattern': 'channel',
            'pattern_name': 'Channel', 'timeframe': '1D', 'state': 'confirmed', 'current': True,
            'score': 1.0, 'sector': 'IT', 'universes': ['nifty500'],
            'history': [{'side': 'long', 'run': 'r1', 'status': 'ok',
                         'reference': stats(n), 'test': stats(n)}]}


def research_match(symbol):
    return {'id': f'{symbol}-1D-CDLBELTHOLD', 'detection_id': symbol + 'det', 'symbol': symbol,
            'company': symbol + ' Ltd.', 'pattern': 'CDLBELTHOLD', 'pattern_name': 'Belt hold',
            'pattern_id': 'CDLBELTHOLD', 'variant': 'canonical', 'side': 'short', 'family': 'candlestick',
            'timeframe': '1D', 'state': 'forming', 'detector_state': 'setup', 'evidence_status': 'identity_match',
            'current': True, 'live': True, 'score': 1.0, 'sector': 'IT', 'universes': ['nifty500']}


class FakeScanner:
    def __init__(self, pattern_set, rows):
        self.pattern_set, self._rows, self.config = pattern_set, rows, {'port': 0}

    def summaries(self):
        return [json.loads(json.dumps(m)) for m in self._rows]


class FakeBoot:
    def __init__(self, scanner):
        self.scanner = scanner

    def status(self):  # pragma: no cover - only reached when scanner is None
        return {'error': 'starting'}


@contextmanager
def running(scanner, monkeypatch):
    """The real handler over a fake scanner; only the two stores it would hit on disk are stubbed."""
    monkeypatch.setattr(server.catalog, 'selected_symbols', lambda filters: None)
    monkeypatch.setattr(server.catalog, 'annotate', lambda rows: rows)
    # The legacy join is the identity here: the fixtures already carry the history it would attach.
    monkeypatch.setattr(server.backtest_store, 'match_history', lambda rows: rows)
    httpd = ThreadingHTTPServer(('127.0.0.1', 0), server.make_handler(FakeBoot(scanner)))
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{httpd.server_address[1]}'
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


def fetch(base, query=''):
    with urlopen(base + '/api/matches' + query, timeout=10) as response:
        return response.status, response.read()


def fetch_error(base, query):
    with pytest.raises(HTTPError) as caught:
        fetch(base, query)
    return caught.value.code, json.loads(caught.value.read())


RESEARCH_ROWS = [research_match('AAAA'), research_match('BBBB'), research_match('CCCC')]
LEGACY_ROWS = [legacy_match('AAAA', 12), legacy_match('BBBB', 2)]


# --- research -------------------------------------------------------------------------------------------

def test_research_bare_request_serves_detections_and_reports_the_screen(monkeypatch):
    """The regression: a bare /api/matches used to answer {"matches": [], "total": 0}."""
    with running(FakeScanner(pattern_live.RESEARCH, RESEARCH_ROWS), monkeypatch) as base:
        status, raw = fetch(base)
    body = json.loads(raw)
    assert status == 200
    assert body['total'] == 3 and body['returned'] == 3 and len(body['matches']) == 3
    assert body['pattern_set'] == pattern_live.RESEARCH
    screen = body['history_screen']
    assert screen['applied'] is False
    assert screen['reason'] == performance.NO_LEGACY_HISTORY == 'research_matches_have_no_legacy_history'
    # The screen that a bare request silently applied before is named, not hidden.
    assert screen['skipped'] == {'mode': 'reference', 'performance': None, 'return_band': None,
                                 'min_trades': performance.DEFAULT_MIN_TRADES}
    assert screen['default_min_trades'] == performance.DEFAULT_MIN_TRADES
    assert 'evidence=true' in screen['evidence'] and 'research_cell_status' in screen['evidence']
    assert 'legacy backtest history' in screen['message']
    # No detection is given a legacy history to be screened on.
    assert all(m['history'] == [] for m in body['matches'])


def test_research_min_trades_zero_is_not_a_screen(monkeypatch):
    """`min_trades=0` is the explicit "any sample size" - the absence of a screen, so it is served."""
    with running(FakeScanner(pattern_live.RESEARCH, RESEARCH_ROWS), monkeypatch) as base:
        status, raw = fetch(base, '?min_trades=0')
    body = json.loads(raw)
    assert status == 200 and body['total'] == 3
    assert body['history_screen']['applied'] is False
    assert body['history_screen']['skipped'] is None


@pytest.mark.parametrize('query', ['?min_trades=20', '?min_trades=5', '?performance=positive',
                                   '?return_band=1_2', '?performance=strong&min_trades=0'])
def test_research_refuses_an_explicit_legacy_history_screen(query, monkeypatch):
    """Refused (400), never answered with an unscreened set that merely looks screened."""
    with running(FakeScanner(pattern_live.RESEARCH, RESEARCH_ROWS), monkeypatch) as base:
        code, body = fetch_error(base, query)
    assert code == 400
    assert body['error'] == performance.NO_LEGACY_HISTORY_REFUSAL
    assert 'legacy backtest history' in body['error'] and 'min_trades=0' in body['error']


def test_research_still_validates_screen_arguments(monkeypatch):
    """A malformed screen is still a 400 about the argument, before the refusal is reached."""
    with running(FakeScanner(pattern_live.RESEARCH, RESEARCH_ROWS), monkeypatch) as base:
        code, body = fetch_error(base, '?return_band=nonsense')
    assert code == 400 and body['error'] == 'Unknown return range'


def test_research_other_filters_still_apply(monkeypatch):
    """Skipping the HISTORY screen does not skip the detection filters."""
    with running(FakeScanner(pattern_live.RESEARCH, RESEARCH_ROWS), monkeypatch) as base:
        body = json.loads(fetch(base, '?symbol=AAAA')[1])
    assert [m['symbol'] for m in body['matches']] == ['AAAA']
    assert body['total'] == 1 and body['history_screen']['applied'] is False


# --- legacy: unchanged ----------------------------------------------------------------------------------

def expected_legacy(rows, minimum):
    return json.dumps([m for m in rows if m['history'][0]['reference']['n'] >= minimum],
                      allow_nan=False).encode()


def test_legacy_bare_request_is_byte_for_byte_the_old_answer(monkeypatch):
    """Still a bare JSON list, still screened by DEFAULT_MIN_TRADES, with no new key anywhere."""
    with running(FakeScanner(pattern_live.LEGACY, LEGACY_ROWS), monkeypatch) as base:
        status, raw = fetch(base)
    assert status == 200
    assert raw == expected_legacy(LEGACY_ROWS, performance.DEFAULT_MIN_TRADES)
    assert b'history_screen' not in raw
    body = json.loads(raw)
    assert isinstance(body, list) and [m['symbol'] for m in body] == ['AAAA']


def test_legacy_explicit_screens_still_run(monkeypatch):
    with running(FakeScanner(pattern_live.LEGACY, LEGACY_ROWS), monkeypatch) as base:
        assert fetch(base, '?min_trades=0')[1] == expected_legacy(LEGACY_ROWS, 0)
        assert fetch(base, '?min_trades=20')[1] == expected_legacy(LEGACY_ROWS, 20)
        band = json.loads(fetch(base, '?return_band=1_2')[1])
        assert [m['symbol'] for m in band] == ['AAAA']
        profile = json.loads(fetch(base, '?performance=high_wr')[1])
        assert [m['symbol'] for m in profile] == ['AAAA']
        # `strong` forces mode=test and a minimum of 20, which nothing here meets.
        assert json.loads(fetch(base, '?performance=strong')[1]) == []


def test_legacy_screen_argument_errors_are_unchanged(monkeypatch):
    with running(FakeScanner(pattern_live.LEGACY, LEGACY_ROWS), monkeypatch) as base:
        assert fetch_error(base, '?min_trades=x') == (400, {'error': 'Minimum trades must be a whole number'})
        assert fetch_error(base, '?performance=nope')[1] == {'error': 'Unknown historical basis or performance filter'}


# --- the helper's own contract --------------------------------------------------------------------------

@pytest.mark.parametrize('filters,expected', [
    ({}, False),
    ({'min_trades': ''}, False),
    ({'min_trades': '0'}, False),
    ({'min_trades': '1'}, True),
    ({'min_trades': str(performance.DEFAULT_MIN_TRADES)}, True),
    ({'performance': 'positive'}, True),
    ({'return_band': '1_2'}, True),
    ({'mode': 'test'}, False),
])
def test_history_screen_requested(filters, expected):
    """The server default is not a request; an explicitly typed sample size is."""
    performance.screen_args(filters)
    assert performance.history_screen_requested(filters) is expected
