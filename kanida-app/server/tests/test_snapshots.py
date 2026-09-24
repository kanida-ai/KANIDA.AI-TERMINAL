"""IMMUTABLE READING SNAPSHOTS — the 21 Sep 2026 history-repaint can never return.

Live, at 10:15 on 21 Sep 2026, the at-the-money strike moved 23,400 -> 23,350 and every earlier reading was
re-described on the new contracts: 10:00 had said "Put activity building", history then said "Call activity".
These tests drive the REAL engine (server/engine/run_engine.cjs, the browser's own TypeScript) through a fake store
whose spot walks 23,300 -> 23,400 -> 23,500 (and down, and back), and prove:

  * each reading is anchored on ITS OWN spot, ATM and contract set;
  * a stored reading never changes when later readings arrive, when the worker runs again, or after a restart;
  * persistence is built from the stored snapshots, matched by STRIKE across an ATM move;
  * a reading with no spot is recorded as unreconstructable, not guessed.
"""
import json
import shutil
import sqlite3

import pytest

from kanida_pilot.snapshots import Engine, SnapshotStore, SnapshotWorker, events_from_snapshots

pytestmark = pytest.mark.skipif(shutil.which('node') is None, reason='the engine is the TypeScript run by node')

STEP = 100


class FakeStore:
    """A derivatives reader over a scripted session. `spots[i]` is spot at reading i (None = not captured)."""

    def __init__(self, marks, spots, build=None, kinds=None):
        self.marks, self.spots = marks, spots
        # OI builds on the calls at 23,400-23,500 every reading; puts barely move
        self.build = build or (lambda strike, kind, i: (20000 * i if kind == 'CE' and strike in (23400, 23500) else 0))
        self.db = sqlite3.connect(':memory:', check_same_thread=False)
        self.db.execute('create table underlying_snapshots(underlying text, captured_at text, spot real, mark_kind text)')
        self.db.execute('create table metrics(captured_at text, scope text)')
        kinds = kinds or ['bar_close'] * len(marks)
        for m, s, k in zip(marks, spots, kinds):
            self.db.execute('insert into underlying_snapshots values(?,?,?,?)', ('NIFTY', m, s, k))
            self.db.execute("insert into metrics values(?, 'underlying')", (m,))

    def _connect(self):
        return self.db

    def _atm(self, spot):
        return round(spot / STEP) * STEP

    def oi_grid(self, underlying, expiry='', at=''):
        i = self.marks.index(at)
        spot = self.spots[i]
        if spot is None:
            return {'rows': [], 'note': f'no spot was captured for {underlying} at this reading', 'expiry': '2026-09-22'}
        if i < 1:
            return {'rows': [], 'note': 'Not enough readings captured yet', 'expiry': '2026-09-22'}
        atm = self._atm(spot)
        rows = []
        for kind, row, sign in (('CE', 'calls', 1), ('PE', 'puts', -1)):
            for n in range(5):
                k = atm + sign * n * STEP
                pts = [{'at': m, 'oi': 100000 + self.build(k, kind, j), 'delta_oi': self.build(k, kind, j),
                        'price': max(1.0, 100 - 5 * j if kind == 'CE' and k in (23400, 23500) else 50)}
                       for j, m in enumerate(self.marks[:i + 1])]
                rows.append({'slot': f'{kind}{n}', 'option_type': kind, 'row': row, 'label': f'{k} {kind}',
                             'present': True, 'strike': k, 'instrument_token': k * (1 if kind == 'CE' else 2),
                             'tradingsymbol': f'NIFTY{k}{kind}', 'points': pts, 'marks': i + 1,
                             'marks_with_delta': i + 1, 'marks_with_price': i + 1})
        return {'rows': rows, 'marks': self.marks[:i + 1], 'atm_strike': atm, 'spot': spot,
                'expiry': '2026-09-22', 'underlying': underlying}

    def oi_by_strike(self, underlying, expiry='', at=''):
        spot = self.spots[self.marks.index(at)]
        if spot is None:
            return {'rows': [], 'spot': None}
        atm = self._atm(spot)
        rows = [{'strike': atm + d * STEP, 'ce_oi': 100000 + 5000 * d, 'pe_oi': 100000 - 5000 * d}
                for d in range(-6, 7)]
        return {'rows': rows, 'spot': spot, 'expiry': '2026-09-22', 'as_of': at, 'max_pain_strike': atm,
                'total_ce_oi': sum(r['ce_oi'] for r in rows), 'total_pe_oi': sum(r['pe_oi'] for r in rows)}

    def _series(self, field, value):
        return {'points': [{'at': m, field: value} for m in self.marks]}

    def pcr_series(self, underlying, expiry=''):
        return self._series('pcr_oi', 1.1)

    def maxpain_series(self, underlying, expiry=''):
        return self._series('max_pain_strike', 23400)

    def iv_series(self, underlying, expiry=''):
        return self._series('iv_pct', 13.0)


MARKS = ['2026-09-21 09:30:00', '2026-09-21 09:45:00', '2026-09-21 10:00:00', '2026-09-21 10:15:00']


@pytest.fixture(scope='module')
def engine():
    e = Engine()
    yield e
    e.close()


def _run(tmp_path, spots, engine, marks=MARKS, name='intel.db'):
    store = SnapshotStore(tmp_path / name)
    worker = SnapshotWorker(FakeStore(marks, spots), store, engine)
    worker.process_session('2026-09-21')
    return store, worker


def _snaps(store):
    return {r['reading_at'][11:16]: r for r in store.session_rows('NIFTY', '2026-09-21')}


def test_each_reading_is_anchored_on_its_own_atm_and_contracts(tmp_path, engine):
    store, _ = _run(tmp_path, [23300, 23310, 23400, 23500], engine)
    s = _snaps(store)
    assert s['09:30']['atm_strike'] == 23300
    assert s['09:45']['atm_strike'] == 23300
    assert s['10:00']['atm_strike'] == 23400
    assert s['10:15']['atm_strike'] == 23500
    for at, atm in (('09:45', 23300), ('10:00', 23400), ('10:15', 23500)):
        strikes = sorted({c['strike'] for c in json.loads(s[at]['contracts'])})
        assert strikes == [atm + d * STEP for d in range(-4, 5)], f'{at} must hold ITS OWN ten contracts'
        assert all(c['instrument_token'] for c in json.loads(s[at]['contracts'])), 'the exact contracts are kept'
    assert s['09:30']['status'] == 'ok' and json.loads(s['09:30']['chained'])['state'] == 'opening'


def test_a_stored_reading_never_changes_as_the_session_grows(tmp_path, engine):
    marks3 = MARKS[:3]
    early, _ = _run(tmp_path, [23300, 23310, 23400], engine, marks3, 'early.db')
    before = {k: dict(v) for k, v in _snaps(early).items()}
    # the SAME store, after a later reading at a higher ATM, and after the worker runs again (a restart)
    worker = SnapshotWorker(FakeStore(MARKS, [23300, 23310, 23400, 23500]), early, engine)
    worker.process_session('2026-09-21')
    worker.process_session('2026-09-21')
    after = _snaps(early)
    for at in ('09:30', '09:45', '10:00'):
        for field in ('atm_strike', 'contracts', 'reading', 'chained', 'id', 'created_at'):
            assert after[at][field] == before[at][field], f'{at}.{field} was rewritten'
    assert after['10:15']['atm_strike'] == 23500
    assert len(early.session_rows('NIFTY', '2026-09-21')) == 4, 'no duplicates from re-running'


def test_insert_is_the_only_write_and_it_never_overwrites(tmp_path, engine):
    store, _ = _run(tmp_path, [23300, 23310, 23400, 23500], engine)
    original = _snaps(store)['10:00']
    forged = {k: original[k] for k in original.keys() if k != 'id'}
    forged['atm_strike'] = 99999
    forged['chained'] = '{"state":"forged"}'
    store.insert(forged)
    assert _snaps(store)['10:00']['atm_strike'] == 23400
    assert 'forged' not in _snaps(store)['10:00']['chained']


def test_persistence_is_built_from_stored_snapshots_by_strike(tmp_path, engine):
    # the build stays at 23,400-23,500 CE while ATM moves 23,300 -> 23,400 -> 23,500: the same strikes, so the same
    # run — it may hold or change shape, but it is never re-announced as new
    store, _ = _run(tmp_path, [23300, 23310, 23400, 23500], engine)
    s = {k: json.loads(v['chained']) for k, v in _snaps(store).items()}
    assert s['09:45']['state'] == 'building'
    assert s['10:00']['state'] != 'appeared', 'the same strikes after an ATM move are not new activity'
    assert s['10:00']['persistence_since'] == MARKS[1]
    assert s['10:15']['persistence_since'] == MARKS[1]
    assert s['10:15']['persistence_minutes'] == 30
    assert _snaps(store)['10:15']['previous_snapshot_id'] == _snaps(store)['10:00']['id'], 'chained to the stored one'


@pytest.mark.parametrize('spots', [
    [23500, 23510, 23400, 23300],      # down several strikes
    [23300, 23310, 23500, 23300],      # up, then back to the earlier ATM
])
def test_atm_moving_down_and_back_keeps_every_reading_on_its_own_contracts(tmp_path, engine, spots):
    store, _ = _run(tmp_path, spots, engine)
    s = _snaps(store)
    for i, at in enumerate(('09:45', '10:00', '10:15'), start=1):
        atm = round(spots[i] / STEP) * STEP
        assert s[at]['atm_strike'] == atm
        assert sorted({c['strike'] for c in json.loads(s[at]['contracts'])}) == [atm + d * STEP for d in range(-4, 5)]


def test_a_reading_with_no_spot_is_unreconstructable_not_guessed(tmp_path, engine):
    store, _ = _run(tmp_path, [23300, 23310, None, 23400], engine)
    gap = _snaps(store)['10:00']
    assert gap['status'] == 'unreconstructable'
    assert 'no spot' in gap['reason']
    assert gap['chained'] is None and gap['contracts'] is None
    # and the next reading chains to the last reading that WAS anchored
    assert _snaps(store)['10:15']['previous_snapshot_id'] == _snaps(store)['09:45']['id']


def test_the_list_is_built_from_the_same_snapshots(tmp_path, engine):
    store, worker = _run(tmp_path, [23300, 23310, 23400, 23500], engine)
    rows = events_from_snapshots(store, '2026-09-21', MARKS[-1], worker.version()[0], expected=1)
    assert rows and rows[0]['headline'] == json.loads(_snaps(store)['10:15']['chained'])['plain_language_read']
    assert rows[0]['first_at'] == MARKS[1]
    assert events_from_snapshots(store, '2026-09-21', MARKS[-1], worker.version()[0], expected=5) is None, \
        'an incomplete reading never replaces the list'


def test_every_snapshot_carries_its_engine_and_rules_version(tmp_path, engine):
    store, worker = _run(tmp_path, [23300, 23310, 23400, 23500], engine)
    ev, rv = worker.version()
    assert ev.startswith('pane/') and rv == 'signal/2'
    for r in store.session_rows('NIFTY', '2026-09-21'):
        assert r['engine_version'] == ev and r['rules_version'] == rv
        if r['status'] == 'ok':
            assert json.loads(r['chained'])['engine_version'] == ev


def test_a_post_close_capture_is_not_snapshotted(tmp_path, engine):
    """21 Sep 15:46: the 15:45 post-close capture was snapshotted as if it were a trading reading and every row came
    out unusable. It is a settlement read after the session, with no fifteen-minute interval behind it."""
    marks = MARKS + ['2026-09-21 10:30:00']
    store = SnapshotStore(tmp_path / 'pc.db')
    worker = SnapshotWorker(FakeStore(marks, [23300, 23310, 23400, 23500, 23500],
                                      kinds=['bar_close'] * 4 + ['post_close']), store, engine)
    assert '2026-09-21 10:30:00' not in worker.readings('2026-09-21')
    worker.process_session('2026-09-21')
    assert '10:30' not in _snaps(store)
