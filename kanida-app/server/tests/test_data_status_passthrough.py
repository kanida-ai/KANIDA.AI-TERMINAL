"""`/api/state.data_status` must reach the app unchanged through the pilot proxy.

The pilot does not re-shape research responses; it strips exactly one field
(`database`, an absolute path on the research box) and caches the rest for 30 s.
This pins that: a new additive field on `/api/state` is passed through whole,
and the one field that is deliberately removed still is.  Without this the
Data status popover would silently show nothing after a scanner upgrade.
"""
from types import SimpleNamespace

from kanida_pilot.evidence import Evidence

STATE = {
    'universe': 500, 'source_latest': '2026-09-16 14:00:00', 'source_stale': False,
    'candle_source': 'market15',
    'database': r'C:\Users\SPS\Documents\Kanida_Falcon\db\market15.db',
    'data_status': {
        'version': 1, 'as_of': '2026-09-16 14:20:00',
        'provider': {'id': 'kite', 'label': 'Zerodha Kite', 'delay_seconds': 0, 'note': None},
        'source': {'kind': 'market15', 'label': 'Live 15-minute store', 'live': True,
                   'store': 'market15.db', 'note': None},
        'latest_bar': {'start': '2026-09-16 14:00:00', 'end': '2026-09-16 14:15:00',
                       'by_timeframe': {'1H': '2026-09-16 14:15:00'}, 'note': None},
        'last_run': {'run_id': 'run_1', 'finished_at': '2026-09-16 14:20:00', 'symbols': 500,
                     'bars_written': 500, 'errors': 0, 'age_seconds': 0, 'note': None},
        'next_refresh': {'at': '2026-09-16 14:30:00', 'in_seconds': 600, 'note': 'x'},
        'session': {'state': 'open', 'close': '2026-09-16 15:30:00'},
        'stalled': {'value': False, 'after_minutes': 20, 'note': None},
        'stale': {'value': False, 'note': None},
    },
}


class Response:
    def __init__(self, payload):
        self.payload = payload
        self.status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class Http:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, params=None):
        self.calls.append((url, params))
        return Response(self.payload)


def evidence(payload=None):
    http = Http(payload if payload is not None else dict(STATE))
    settings = SimpleNamespace(research_directory='', research_url='http://127.0.0.1:8765')
    return Evidence(settings, http), http


def test_data_status_reaches_the_app_untouched():
    ev, _ = evidence()
    state = ev.get('/api/state')
    assert state['data_status'] == STATE['data_status']
    # the existing fields the app already relies on keep their shape
    assert state['source_latest'] == '2026-09-16 14:00:00'
    assert state['source_stale'] is False
    assert state['candle_source'] == 'market15'


def test_the_research_box_path_is_still_the_only_field_removed():
    ev, _ = evidence()
    state = ev.get('/api/state')
    assert 'database' not in state
    assert set(STATE) - set(state) == {'database'}


def test_a_scanner_without_data_status_is_not_broken_by_the_proxy():
    payload = {k: v for k, v in STATE.items() if k != 'data_status'}
    ev, _ = evidence(payload)
    assert 'data_status' not in ev.get('/api/state')
