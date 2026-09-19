"""`/api/state.data_status` -- the object the app's Data status popover reads.

Every value here must be traceable to something the live ingest loop actually
writes (`meta['live.*']`, `ingest_runs`) or to the scanner's own calendar.  The
tests pin three things that are easy to get quietly wrong:

* the stored timestamps are **UTC** (`market_data.store.utcnow`) and everything
  published is **IST**, so the conversion happens exactly once;
* a 15-minute delayed vendor is *delayed by design*, not stalled or stale, so
  its `delay_seconds` is reported and folded into the next expected refresh;
* "stalled" is only asserted while the market is open -- an idle loop at
  22:00 IST is the loop working correctly.
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from unittest import mock
from datetime import date, datetime, timedelta
from pathlib import Path

from market_scanner import data_status as ds
from market_scanner.data import Calendar, load_config

CONFIG = load_config()
#: 2026-09-15 is a Tuesday; 2026-09-14 is an NSE holiday in the same config,
#: and 2026-09-19 is a Saturday.  All three are used below.
SESSION_DAY = date(2026, 9, 15)


def utc(text):
    """A stored stamp in the loop's own format (naive UTC, seconds)."""
    return datetime.fromisoformat(text).isoformat(timespec='seconds')


class FakeScanner:
    """The three attributes `data_status()` reads off a Scanner."""

    def __init__(self, source='market15', market15=None, timeframes=None):
        self.calendar = Calendar(CONFIG)
        self.candle_source = source
        self.market15 = Path(market15) if market15 else None
        self.config = {'timeframes': list(CONFIG['timeframes'])}
        self.metadata = {'timeframes': {tf: {'as_of': f'2026-09-15 1{i}:15:00'}
                                        for i, tf in enumerate(timeframes or ['1H'])}}


def make_store(path, *, latest_bar='2026-09-15 14:15:00', cycle=None, run=None,
               quarantine=None):
    """A minimal stand-in for `db/market15.db`: just the two things we read."""
    con = sqlite3.connect(path)
    con.executescript(
        'CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);'
        'CREATE TABLE ingest_runs (run_id TEXT PRIMARY KEY, started_at TEXT NOT NULL,'
        ' finished_at TEXT, provider TEXT, plan TEXT, requests INTEGER, rows INTEGER,'
        ' errors INTEGER, status TEXT);')
    if latest_bar:
        start = datetime.fromisoformat(latest_bar)
        con.execute("INSERT INTO meta VALUES ('live.latest_bar_start',?)", (latest_bar,))
        con.execute("INSERT INTO meta VALUES ('live.latest_bar_end',?)",
                    ((start + timedelta(minutes=15)).isoformat(sep=' '),))
    if cycle:
        con.execute("INSERT INTO meta VALUES ('live.last_cycle',?)", (json.dumps(cycle),))
        con.execute("INSERT INTO meta VALUES ('live.last_cycle_finished_at',?)",
                    (cycle.get('finished_at'),))
    if run:
        con.execute('INSERT INTO ingest_runs (run_id,started_at,finished_at,provider,requests,'
                    'rows,errors,status) VALUES (?,?,?,?,?,?,?,?)',
                    (run['run_id'], run['started_at'], run.get('finished_at'), run.get('provider'),
                     run.get('requests', 0), run.get('rows', 0), run.get('errors', 0),
                     run.get('status', 'ok')))
    if quarantine is not None:
        con.execute(
            'CREATE TABLE quarantine (symbol TEXT PRIMARY KEY, reason TEXT NOT NULL,'
            ' detail TEXT, status TEXT NOT NULL DEFAULT "quarantined",'
            ' first_seen TEXT NOT NULL, last_checked TEXT NOT NULL,'
            ' checks INTEGER NOT NULL DEFAULT 1, last_error TEXT, released_at TEXT,'
            ' provider TEXT, run_id TEXT)')
        for row in quarantine:
            con.execute(
                'INSERT INTO quarantine (symbol,reason,detail,status,first_seen,'
                'last_checked,checks) VALUES (?,?,?,?,?,?,?)',
                (row['symbol'], row.get('reason', 'not_in_provider_instrument_list'),
                 row.get('detail', ''), row.get('status', 'quarantined'),
                 row.get('first_seen', '2026-09-16 05:00:00'),
                 row.get('last_checked', '2026-09-16 05:00:00'),
                 row.get('checks', 1)))
    con.commit()
    con.close()


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.calendar = Calendar(CONFIG)

    def state(self, when):
        return ds.session_status(self.calendar, when)

    def test_four_states_in_ist(self):
        self.assertEqual(self.state(datetime(2026, 9, 15, 8, 0))['state'], 'pre_open')
        self.assertEqual(self.state(datetime(2026, 9, 15, 9, 15))['state'], 'open')
        self.assertEqual(self.state(datetime(2026, 9, 15, 13, 0))['state'], 'open')
        self.assertEqual(self.state(datetime(2026, 9, 15, 15, 30))['state'], 'open')
        self.assertEqual(self.state(datetime(2026, 9, 15, 15, 31))['state'], 'closed')

    def test_holiday_and_weekend_are_distinguished_by_reason(self):
        holiday = self.state(datetime(2026, 9, 14, 11, 0))
        self.assertEqual(holiday['state'], 'holiday')
        self.assertEqual(holiday['reason'], 'exchange_holiday')
        self.assertEqual(holiday['next_open'], '2026-09-15 09:15:00')
        weekend = self.state(datetime(2026, 9, 19, 11, 0))
        self.assertEqual((weekend['state'], weekend['reason']), ('holiday', 'weekend'))
        self.assertEqual(weekend['next_open'], '2026-09-21 09:15:00')

    def test_next_open_after_the_close_is_the_next_session(self):
        self.assertEqual(self.state(datetime(2026, 9, 15, 16, 0))['next_open'],
                         '2026-09-16 09:15:00')

    def test_next_bar_end_walks_the_fifteen_minute_grid(self):
        self.assertEqual(ds.next_bar_end(self.calendar, datetime(2026, 9, 15, 9, 20)),
                         datetime(2026, 9, 15, 9, 30))
        self.assertEqual(ds.next_bar_end(self.calendar, datetime(2026, 9, 15, 9, 30)),
                         datetime(2026, 9, 15, 9, 45))
        # the last bar of the day, then the first of the next session
        self.assertEqual(ds.next_bar_end(self.calendar, datetime(2026, 9, 15, 15, 20)),
                         datetime(2026, 9, 15, 15, 30))
        self.assertEqual(ds.next_bar_end(self.calendar, datetime(2026, 9, 15, 16, 0)),
                         datetime(2026, 9, 16, 9, 30))
        # an NSE holiday is skipped, never filled
        self.assertEqual(ds.next_bar_end(self.calendar, datetime(2026, 9, 14, 11, 0)),
                         datetime(2026, 9, 15, 9, 30))


class ProviderTests(unittest.TestCase):
    def test_declared_delays(self):
        self.assertEqual(ds.provider_delay_seconds('kite'), 0)
        self.assertEqual(ds.provider_delay_seconds('fake'), 0)
        self.assertIsNone(ds.provider_delay_seconds('someone_else'))

    def test_vendor15_delay_follows_its_env(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop('VENDOR15_DELAY_SECONDS', None)
            self.assertEqual(ds.provider_delay_seconds('vendor15'), 900)
            os.environ['VENDOR15_DELAY_SECONDS'] = '300'
            self.assertEqual(ds.provider_delay_seconds('vendor15'), 300)
            os.environ['VENDOR15_DELAY_SECONDS'] = 'nonsense'
            self.assertEqual(ds.provider_delay_seconds('vendor15'), 900)


class StatusTests(unittest.TestCase):
    def setUp(self):
        ds.clear_cache()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name) / 'market15.db'
        env = mock.patch.dict(os.environ, {'MARKET_DATA_PROVIDER': 'kite'}, clear=False)
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop('SCANNER_INGEST_STALL_MINUTES', None)

    def healthy(self, finished='2026-09-15 08:50:00', errors=0):
        """A store whose loop wrote the 14:00-14:15 bar. `finished` is UTC."""
        make_store(self.db, latest_bar='2026-09-15 14:00:00',
                   cycle={'run_id': 'run_1', 'symbols': 500, 'rows': 500, 'errors': errors,
                          'requests': 500, 'seconds': 172.4, 'daily_rows': 0,
                          'started_at': utc('2026-09-15 08:47:00'), 'finished_at': utc(finished)},
                   run={'run_id': 'run_1', 'started_at': utc('2026-09-15 08:47:00'),
                        'finished_at': utc(finished), 'provider': 'kite', 'requests': 500,
                        'rows': 500, 'errors': errors, 'status': 'ok' if not errors else 'partial'})
        return FakeScanner(market15=self.db)

    # -- live ---------------------------------------------------------------
    def test_live_status_reports_source_bar_and_last_run(self):
        out = ds.data_status(self.healthy(), '2026-09-15 14:00:00', False,
                             now=datetime(2026, 9, 15, 14, 20))
        self.assertEqual(out['source'], {'kind': 'market15', 'label': 'Live 15-minute store',
                                         'live': True, 'store': 'market15.db', 'note': None})
        self.assertEqual(out['provider']['id'], 'kite')
        self.assertEqual(out['provider']['delay_seconds'], 0)
        self.assertEqual(out['latest_bar']['start'], '2026-09-15 14:00:00')
        self.assertEqual(out['latest_bar']['end'], '2026-09-15 14:15:00')
        self.assertEqual(out['session']['state'], 'open')
        run = out['last_run']
        self.assertEqual(run['run_id'], 'run_1')
        # stored UTC 08:50 -> IST 14:20, so the cycle finished right now
        self.assertEqual(run['finished_at'], '2026-09-15 14:20:00')
        self.assertEqual(run['started_at'], '2026-09-15 14:17:00')
        self.assertEqual((run['symbols'], run['bars_written'], run['errors']), (500, 500, 0))
        self.assertEqual(run['age_seconds'], 0)
        self.assertIs(out['stalled']['value'], False)
        self.assertIs(out['stale']['value'], False)

    def test_next_refresh_is_the_next_bar_close(self):
        out = ds.data_status(self.healthy(), '', False, now=datetime(2026, 9, 15, 14, 20))
        self.assertEqual(out['next_refresh']['at'], '2026-09-15 14:30:00')
        self.assertEqual(out['next_refresh']['in_seconds'], 600)

    def test_vendor_delay_is_added_to_the_next_refresh_not_called_stale(self):
        with mock.patch.dict(os.environ, {'MARKET_DATA_PROVIDER': 'vendor15',
                                                   'VENDOR15_DELAY_SECONDS': '900'}):
            out = ds.data_status(self.healthy(), '', False, now=datetime(2026, 9, 15, 14, 20))
        self.assertEqual(out['provider']['delay_seconds'], 900)
        self.assertEqual(out['next_refresh']['at'], '2026-09-15 14:45:00')
        self.assertIn('by design', out['next_refresh']['note'])
        self.assertIs(out['stalled']['value'], False)

    def test_per_timeframe_newest_bar_comes_from_the_last_scan(self):
        scanner = self.healthy()
        scanner.metadata = {'timeframes': {'1H': {'as_of': '2026-09-15 14:15:00'},
                                           '1D': {'as_of': '2026-09-12 15:30:00'}}}
        out = ds.data_status(scanner, '', False, now=datetime(2026, 9, 15, 14, 20))
        self.assertEqual(out['latest_bar']['by_timeframe'],
                         {'1H': '2026-09-15 14:15:00', '4H': None,
                          '1D': '2026-09-12 15:30:00', '1W': None})

    # -- stalled ------------------------------------------------------------
    def test_stalled_when_the_market_is_open_and_no_cycle_finished(self):
        scanner = self.healthy(finished='2026-09-15 07:30:00')   # IST 13:00
        out = ds.data_status(scanner, '', False, now=datetime(2026, 9, 15, 14, 20))
        self.assertIs(out['stalled']['value'], True)
        self.assertEqual(out['stalled']['after_minutes'], 20)
        self.assertIn('80 minutes ago', out['stalled']['note'])

    def test_not_stalled_outside_market_hours(self):
        scanner = self.healthy(finished='2026-09-15 07:30:00')
        out = ds.data_status(scanner, '', False, now=datetime(2026, 9, 15, 22, 0))
        self.assertIs(out['stalled']['value'], False)
        self.assertIn('only expected to run while the market is open', out['stalled']['note'])

    def test_errors_in_the_last_cycle_are_surfaced_without_claiming_stalled(self):
        out = ds.data_status(self.healthy(errors=3), '', False,
                             now=datetime(2026, 9, 15, 14, 20))
        self.assertIs(out['stalled']['value'], False)
        self.assertEqual(out['last_run']['errors'], 3)
        self.assertIn('completed with errors', out['stalled']['note'])

    def test_a_cycle_in_flight_is_not_stalled(self):
        """The commonest false alarm: a run that started 20 s ago has no finish yet."""
        make_store(self.db, latest_bar='2026-09-15 14:00:00',
                   run={'run_id': 'run_2', 'started_at': utc('2026-09-15 08:49:40'),
                        'finished_at': None, 'provider': 'kite', 'status': 'running'})
        out = ds.data_status(FakeScanner(market15=self.db), '', False,
                             now=datetime(2026, 9, 15, 14, 20))
        self.assertIs(out['last_run']['running'], True)
        self.assertIsNone(out['last_run']['age_seconds'])
        self.assertIs(out['stalled']['value'], False)
        self.assertIn('A cycle is running now', out['stalled']['note'])

    def test_a_run_stuck_in_running_for_hours_is_stalled(self):
        make_store(self.db, latest_bar='2026-09-15 14:00:00',
                   run={'run_id': 'run_2', 'started_at': utc('2026-09-15 04:00:00'),
                        'finished_at': None, 'provider': 'kite', 'status': 'running'})
        out = ds.data_status(FakeScanner(market15=self.db), '', False,
                             now=datetime(2026, 9, 15, 14, 20))
        self.assertIs(out['stalled']['value'], True)
        self.assertIn('has ever recorded a finish', out['stalled']['note'])

    def test_a_running_cycle_carries_the_previous_finish(self):
        """Stalled must be judged on the last cycle that FINISHED, not the one in flight."""
        scanner = self.healthy(finished='2026-09-15 08:50:00')
        con = sqlite3.connect(self.db)
        con.execute("INSERT INTO ingest_runs (run_id,started_at,finished_at,provider,status) "
                    "VALUES ('run_2',?,NULL,'kite','running')", (utc('2026-09-15 08:52:00'),))
        con.commit(); con.close()
        ds.clear_cache()
        out = ds.data_status(scanner, '', False, now=datetime(2026, 9, 15, 14, 25))
        self.assertEqual(out['last_run']['run_id'], 'run_2')
        self.assertIs(out['last_run']['running'], True)
        self.assertEqual(out['last_run']['last_success_at'], '2026-09-15 14:20:00')
        self.assertEqual(out['last_run']['last_success_age_seconds'], 300)
        self.assertIs(out['stalled']['value'], False)

    def test_store_with_no_ingest_run_says_so_rather_than_guessing(self):
        make_store(self.db, latest_bar='2026-09-15 14:00:00')
        out = ds.data_status(FakeScanner(market15=self.db), '', False,
                             now=datetime(2026, 9, 15, 14, 20))
        self.assertIsNone(out['last_run'])
        self.assertIn('has not completed a cycle', out['last_run_note'])
        self.assertIs(out['stalled']['value'], True)

    def test_missing_store_degrades_to_a_note(self):
        out = ds.data_status(FakeScanner(market15=Path(self.tmp.name) / 'absent.db'),
                             '2026-09-11 15:30:00', True, now=datetime(2026, 9, 15, 14, 20))
        self.assertIsNone(out['last_run'])
        self.assertIn('could not be opened', out['last_run_note'])
        # the newest bar we do know about still comes through, flagged stale
        self.assertEqual(out['latest_bar']['start'], '2026-09-11 15:30:00')
        self.assertIs(out['stale']['value'], True)

    # -- legacy -------------------------------------------------------------
    def test_legacy_source_is_labelled_stored_not_live(self):
        out = ds.data_status(FakeScanner(source='legacy'), '2026-07-31', False,
                             now=datetime(2026, 9, 15, 14, 20))
        self.assertEqual(out['source']['kind'], 'legacy')
        self.assertIs(out['source']['live'], False)
        self.assertEqual(out['latest_bar']['start'], '2026-07-31')
        self.assertIsNone(out['latest_bar']['end'])
        self.assertIsNone(out['last_run'])
        self.assertIsNone(out['next_refresh']['at'])
        self.assertIs(out['stalled']['value'], False)
        self.assertIn('Only the live 15-minute store', out['stalled']['note'])


class EngineHookTests(unittest.TestCase):
    """`Scanner.data_status` is what `/api/state` actually calls."""

    def test_it_returns_the_object(self):
        from market_scanner.engine import Scanner
        out = Scanner.data_status(FakeScanner(source='legacy'), '2026-07-31', False)
        self.assertEqual(out['source']['kind'], 'legacy')
        self.assertEqual(out['version'], 1)

    def test_a_failure_degrades_to_a_note_instead_of_a_500(self):
        from market_scanner.engine import Scanner
        broken = FakeScanner(source='legacy')
        del broken.calendar
        with self.assertLogs('kanida', level='ERROR'):
            out = Scanner.data_status(broken, '', False)
        self.assertIn('error', out)
        self.assertIn('could not be read', out['note'])


class CacheTests(unittest.TestCase):
    def test_the_store_read_is_memoised(self):
        ds.clear_cache()
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / 'market15.db'
            make_store(db, latest_bar='2026-09-15 14:00:00')
            first = ds.read_market15(db)
            db.unlink()
            self.assertIs(ds.read_market15(db), first)      # served from the cache
            ds.clear_cache()
            self.assertIsNotNone(ds.read_market15(db)['error'])


class QuarantineTests(unittest.TestCase):
    """A skipped symbol must be reported, not just quietly absent.

    Skipping the six names Kite cannot serve is what takes `last_run.errors`
    from 6 to 0.  If that were all we did, the panel would go from a warning
    about the loop to a clean bill of health while six symbols silently stopped
    advancing.  `data_status.quarantine` is the thing that stops the fix from
    becoming a lie.
    """

    def setUp(self):
        ds.clear_cache()
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / 'market15.db'

    def tearDown(self):
        self.tmp.cleanup()
        ds.clear_cache()

    def _status(self, **kw):
        make_store(self.db, **kw)
        return ds.data_status(FakeScanner(market15=self.db),
                              now=datetime(2026, 9, 15, 14, 30))

    def test_quarantined_symbols_are_named_with_their_reason(self):
        out = self._status(quarantine=[
            {'symbol': 'LTIM', 'detail': "LTIM is not in the provider's instrument list"},
            {'symbol': 'HEG'}])
        block = out['quarantine']
        self.assertEqual(block['count'], 2)
        self.assertEqual([s['symbol'] for s in block['symbols']], ['HEG', 'LTIM'])
        self.assertIn('HEG, LTIM', block['note'])
        self.assertIn('data is unavailable', block['note'])
        self.assertIn('re-checked once a day', block['note'])
        self.assertTrue(all(s['first_seen'] and s['last_checked']
                            for s in block['symbols']))

    def test_a_released_symbol_is_not_reported(self):
        out = self._status(quarantine=[{'symbol': 'LTIM', 'status': 'released'}])
        self.assertEqual(out['quarantine']['count'], 0)
        self.assertIn('serving every symbol', out['quarantine']['note'])

    def test_no_quarantine_table_is_reported_as_unknown_not_as_zero(self):
        out = self._status()
        self.assertIsNone(out['quarantine']['count'])
        self.assertIn('no quarantine table', out['quarantine']['note'])

    def test_the_block_says_why_the_error_count_is_clean(self):
        out = self._status(quarantine=[{'symbol': 'LTIM'}],
                           run={'run_id': 'r1', 'started_at': utc('2026-09-15 08:50:00'),
                                'finished_at': utc('2026-09-15 08:55:00'), 'errors': 0})
        self.assertEqual(out['last_run']['errors'], 0)
        self.assertIn('do not appear in `last_run.errors`', out['quarantine']['note'])

    def test_the_legacy_source_has_no_quarantine(self):
        out = ds.data_status(FakeScanner(source='legacy'), '2026-07-31', False)
        self.assertEqual(out['quarantine'], {
            'count': 0, 'symbols': [],
            'note': 'Only the live 15-minute store has a quarantine.'})


class PatternsTests(unittest.TestCase):
    """Prices move every 15 minutes; patterns only when a candle completes.

    `patterns` must say which candle each timeframe actually scanned, when the
    next one closes, and tell "waiting for the next candle" (normal) apart from
    "a completed candle was never scanned" (behind).
    """

    def setUp(self):
        ds.clear_cache()
        os.environ.pop('SCANNER_INGEST_STALL_MINUTES', None)

    def scanner(self, scanned, source='market15', progress=None):
        s = FakeScanner(source=source)
        s.config['settlement_delay_seconds'] = 120
        s.metadata = {'timeframes': {tf: {'as_of': v, 'finished_at': '2026-09-15 08:06:38.962849'}
                                     for tf, v in scanned.items()}}
        if progress is not None:
            s.progress = progress
        return s

    PREV = {'1H': '2026-09-11 15:30:00', '4H': '2026-09-11 15:30:00',
            '1D': '2026-09-11 15:30:00', '1W': '2026-09-11 15:30:00'}

    def block(self, scanned, now, **kw):
        return ds.patterns_block(self.scanner(scanned, **kw), now, True)

    def test_before_the_first_hourly_candle_patterns_are_current_and_next_is_10_15(self):
        # 2026-09-15 10:01: the 14th is a holiday, so the newest completed 1H candle is 11 Sep 15:30.
        out = self.block(self.PREV, datetime(2026, 9, 15, 10, 1))
        self.assertEqual(out['latest'], '2026-09-11 15:30:00')
        self.assertIs(out['up_to_date'], True)
        self.assertEqual((out['updating'], out['behind']), ([], []))
        self.assertEqual(out['next_update'], '2026-09-15 10:15:00')
        self.assertEqual(out['by_timeframe']['4H']['next_close'], '2026-09-15 13:15:00')

    def test_a_candle_that_just_closed_is_updating_not_behind(self):
        out = self.block(self.PREV, datetime(2026, 9, 15, 10, 25))
        self.assertEqual(out['updating'], ['1H'])
        self.assertEqual(out['behind'], [])
        self.assertIs(out['up_to_date'], False)
        self.assertEqual(out['by_timeframe']['1H']['expected'], '2026-09-15 10:15:00')
        self.assertEqual(out['by_timeframe']['1H']['overdue_seconds'], 600)
        self.assertEqual(out['next_update'], '2026-09-15 11:15:00')

    def test_the_settlement_delay_is_respected(self):
        out = self.block(self.PREV, datetime(2026, 9, 15, 10, 16))
        self.assertIs(out['up_to_date'], True)

    def test_an_unscanned_candle_past_the_stall_limit_is_behind(self):
        out = self.block(self.PREV, datetime(2026, 9, 15, 10, 40))
        self.assertEqual(out['behind'], ['1H'])
        self.assertIn('1H', out['note'])
        self.assertIs(out['by_timeframe']['1H']['behind'], True)

    def test_scanned_candle_is_the_latest(self):
        scanned = dict(self.PREV, **{'1H': '2026-09-15 10:15:00'})
        out = self.block(scanned, datetime(2026, 9, 15, 10, 40))
        self.assertEqual(out['latest'], '2026-09-15 10:15:00')
        self.assertIs(out['up_to_date'], True)

    def test_after_the_close_the_daily_scan_is_expected(self):
        scanned = {'1H': '2026-09-15 15:15:00', '4H': '2026-09-15 13:15:00',
                   '1D': '2026-09-11 15:30:00', '1W': '2026-09-11 15:30:00'}
        out = self.block(scanned, datetime(2026, 9, 15, 22, 0))
        self.assertEqual(sorted(out['behind']), ['1D', '1H', '4H'])
        done = {'1H': '2026-09-15 15:30:00', '4H': '2026-09-15 15:30:00',
                '1D': '2026-09-15 15:30:00', '1W': '2026-09-11 15:30:00'}
        out = self.block(done, datetime(2026, 9, 15, 22, 0))
        self.assertIs(out['up_to_date'], True)
        self.assertEqual(out['next_update'], '2026-09-16 10:15:00')

    def test_a_timeframe_never_scanned_is_reported_not_guessed(self):
        out = self.block({'1H': '2026-09-11 15:30:00'}, datetime(2026, 9, 15, 10, 1))
        self.assertIsNone(out['by_timeframe']['4H']['scanned'])
        self.assertIn('4H', out['behind'])

    def test_the_legacy_source_expects_no_update(self):
        out = ds.patterns_block(self.scanner(self.PREV, source='legacy'),
                                datetime(2026, 9, 15, 10, 40), False)
        self.assertIsNone(out['next_update'])
        self.assertIsNone(out['up_to_date'])
        self.assertEqual(out['latest'], '2026-09-11 15:30:00')

    def test_data_status_carries_the_block_and_a_failure_is_a_note(self):
        out = ds.data_status(self.scanner(self.PREV, source='legacy'), '2026-07-31', False,
                             now=datetime(2026, 9, 15, 10, 1))
        self.assertEqual(out['patterns']['latest'], '2026-09-11 15:30:00')
        with mock.patch.object(ds, 'patterns_block', side_effect=RuntimeError('boom')):
            out = ds.data_status(self.scanner(self.PREV, source='legacy'), '', False,
                                 now=datetime(2026, 9, 15, 10, 1))
        self.assertEqual(out['patterns']['error'], 'boom')
        self.assertIn('could not be read', out['patterns']['note'])


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
