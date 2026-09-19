import copy
import json
import sqlite3
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import numpy as np

from market_scanner.data import Calendar, ClosingConnection, aggregate, connect_source, load_config
from market_scanner.detectors import Detector, detect, pivots


def candles(values, volume=None):
    return [dict(time=f'2026-01-{i%28+1:02d} 09:15:00', end=f'2026-01-{i%28+1:02d} 15:30:00',
                 open=float(c)-.05,high=float(c)+.15,low=float(c)-.15,close=float(c),
                 volume=float(volume[i]) if volume is not None else 1000.,gap=False) for i,c in enumerate(values)]


def linear_nodes(nodes, length):
    return np.interp(np.arange(length),[p[0] for p in nodes],[p[1] for p in nodes])


def boundary_fixture(kind):
    x=np.arange(160)
    settings={'symmetrical_triangle':(.0,10-.045*x),'falling_wedge':(-.09*x,10-.045*x),
              'rising_wedge':(.09*x,10-.045*x),'descending_triangle':(-.045*x,10-.045*x),
              'channel':(.04*x,np.full(160,5.))}
    center,amplitude=settings[kind]
    return candles(100+center+amplitude*np.sin(x*2*np.pi/16))


class CandleTests(unittest.TestCase):
    def setUp(self): self.calendar=Calendar(load_config())

    def rows(self, day='2026-07-30',count=75):
        start=datetime.fromisoformat(day+' 09:15:00')
        return [((start+timedelta(minutes=5*i)).isoformat(sep=' '),100+i,102+i,99+i,101+i,10) for i in range(count)]

    def test_market_aligned_four_hour_and_closing_stub(self):
        bars,_=aggregate(self.rows(),'4H',self.calendar,datetime(2026,7,30,16))
        self.assertEqual([b['end'][11:] for b in bars],['13:15:00','15:30:00'])
        self.assertEqual([b['volume'] for b in bars],[480,270])
        self.assertEqual(bars[0]['open'],100)
        self.assertEqual(bars[0]['close'],148)
        self.assertEqual(bars[0]['high'],149)

    def test_incomplete_candle_never_appears(self):
        bars,_=aggregate(self.rows(),'4H',self.calendar,datetime(2026,7,30,13,14,59))
        self.assertEqual(bars,[])
        bars,_=aggregate(self.rows(),'4H',self.calendar,datetime(2026,7,30,13,15))
        self.assertEqual(len(bars),1)

    def test_missing_internal_source_bar_rejected(self):
        rows=self.rows(); rows.pop(18)
        bars,q=aggregate(rows,'4H',self.calendar,datetime(2026,7,30,16))
        self.assertEqual(len(bars),1)
        self.assertEqual(bars[0]['time'][11:],'13:15:00')
        self.assertEqual(q['incomplete_buckets'],1)

    def test_hourly_never_crosses_session(self):
        next_day=[(r[0],*(v+75 for v in r[1:5]),r[5]) for r in self.rows('2026-07-31')]
        bars,_=aggregate(self.rows()+next_day,'1H',self.calendar,datetime(2026,8,1))
        self.assertEqual(len(bars),14)
        self.assertEqual(bars[6]['volume'],30)
        self.assertFalse(any(b['gap'] for b in bars))

    def test_weekly_waits_for_friday_and_checks_missing_day(self):
        rows=[(f'2026-07-{day} 00:00:00',100,102,99,101,1000) for day in (27,28,29,30,31)]
        self.assertEqual(aggregate(rows,'1W',self.calendar,datetime(2026,7,31,15,29))[0],[])
        self.assertEqual(len(aggregate(rows,'1W',self.calendar,datetime(2026,7,31,15,30))[0]),1)
        self.assertEqual(aggregate(rows[:-1],'1W',self.calendar,datetime(2026,8,1))[0],[])

    def test_friday_holiday_week_finishes_thursday(self):
        rows=[(f'2026-06-{day} 00:00:00',100,102,99,101,1000) for day in (22,23,24,25)]
        bars,_=aggregate(rows,'1W',self.calendar,datetime(2026,6,25,15,30))
        self.assertEqual(len(bars),1)
        self.assertEqual(bars[0]['end'],'2026-06-25 15:30:00')

    def test_first_observed_week_needs_known_calendar_for_earlier_days(self):
        dates=['2013-01-01','2013-01-02','2013-01-03','2013-01-04','2013-01-07','2013-01-08','2013-01-09','2013-01-10','2013-01-11']
        calendar=Calendar(load_config(),dates)
        rows=[(day+' 00:00:00',100,102,99,101,1000) for day in dates]
        bars,quality=aggregate(rows,'1W',calendar,datetime(2013,1,12))
        self.assertEqual(len(bars),1)
        self.assertEqual(bars[0]['time'],'2013-01-07 09:15:00')
        self.assertEqual(quality['incomplete_buckets'],1)

    def test_holiday_and_special_sunday_schedule(self):
        self.assertEqual(self.calendar.next_close('1H',datetime(2026,9,11,16)),'2026-09-15 10:15:00')
        self.assertIsNotNone(self.calendar.session(date(2026,2,1)))

    def test_invalid_ohlc_excluded(self):
        rows=self.rows(); rows[0]=(rows[0][0],100,95,99,101,10)
        bars,q=aggregate(rows,'4H',self.calendar,datetime(2026,7,30,16))
        self.assertEqual(q['invalid_rows'],1)
        self.assertEqual(len(bars),1)

    def test_source_connection_is_read_only(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'source.db'
            with sqlite3.connect(path,factory=ClosingConnection) as con: con.execute('CREATE TABLE sample (value INTEGER)')
            with connect_source(path) as con:
                with self.assertRaises(sqlite3.OperationalError): con.execute('INSERT INTO sample VALUES(1)')


class DetectorTests(unittest.TestCase):
    def test_unconfirmed_pivots_are_not_used(self):
        self.assertEqual(pivots(np.array([1,2,3,8,3,2,1])).tolist(),[3])
        self.assertEqual(pivots(np.array([1,2,3,8,3,2])).tolist(),[])

    def test_flat_prices_do_not_qualify(self):
        self.assertEqual(detect(candles(np.full(160,100.))),[])
        self.assertEqual(detect(candles(np.linspace(60,140,160))),[])

    def test_line_pattern_positive_fixtures(self):
        for name in ('symmetrical_triangle','falling_wedge','rising_wedge','descending_triangle','channel'):
            with self.subTest(pattern=name):
                self.assertIn(name,[m['pattern'] for m in detect(boundary_fixture(name))])

    def test_gaps_inside_a_pattern_reject_it(self):
        bars=boundary_fixture('channel')
        self.assertTrue(detect(bars))
        bars[-5]['gap']=True
        self.assertEqual(detect(bars),[])

    def test_head_shoulders_and_inverse(self):
        values=linear_nodes([(0,70),(25,104),(36,94),(51,116),(67,95),(81,103),(87,98),(91,97)],92)
        for inverse in (False,True):
            key='inverse_head_shoulders' if inverse else 'head_shoulders'
            bars=candles(200-values if inverse else values)
            self.assertIn(key,[m['pattern'] for m in detect(bars)])

    def test_cup_and_handle_rounded_positive_and_v_negative(self):
        prefix=np.linspace(80,110,25)
        cup=90+20*np.linspace(-1,1,61)**2
        handle=np.array([108,107,106,105,104,105,106,107,108,109])
        volumes=np.r_[np.full(86,1000),np.full(10,600)]
        bars=candles(np.r_[prefix,cup,handle],volumes)
        # Distinct rim highs preserve a confirmed pivot at the transition.
        bars[24]['high']+=.1
        self.assertIn('cup_handle',[m['pattern'] for m in detect(bars)])
        v=90+20*abs(np.linspace(-1,1,61))
        sharp=candles(np.r_[prefix,v,handle],volumes);sharp[24]['high']+=.1
        self.assertNotIn('cup_handle',[m['pattern'] for m in detect(sharp)])

    def test_horizontal_breakout_requires_fresh_cross_and_volume(self):
        x=np.arange(90)
        values=100+4*np.sin(x*2*np.pi/16)
        values[-3:]=[102,103,104.8]
        bars=candles(values);bars[-1]['volume']=2000
        self.assertIn('horizontal_breakout',[m['pattern'] for m in detect(bars)])
        bars[-1]['volume']=800
        self.assertNotIn('horizontal_breakout',[m['pattern'] for m in detect(bars)])

    def test_flag_requires_pole_and_volume_contraction(self):
        prefix=np.linspace(80,82,40)
        pole=np.linspace(82,104,10)
        flag=104-np.arange(15)*.22+np.sin(np.arange(15)*2*np.pi/5)*.8
        values=np.r_[prefix,pole,flag,103]
        volumes=np.r_[np.full(40,1000),np.full(10,2500),np.full(15,700),1800]
        bars=candles(values,volumes)
        self.assertIn('flag_pole',[m['pattern'] for m in detect(bars)])
        bearish=candles(200-values,volumes)
        self.assertIn('bearish',[m['direction'] for m in detect(bearish) if m['pattern']=='flag_pole'])

    def test_enabled_detectors_are_respected(self):
        bars=boundary_fixture('channel')
        self.assertEqual(detect(bars,['cup_handle']),[])
        self.assertEqual(detect(bars,[]),[])


class SnapshotTests(unittest.TestCase):
    def test_snapshot_replacement_and_independent_timeframes(self):
        from market_scanner.engine import Scanner
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            source=root/'source.db'
            with sqlite3.connect(source,factory=ClosingConnection) as con:
                con.executescript('''CREATE TABLE instrument_labels (symbol TEXT,company TEXT,company_name TEXT,sector TEXT,instrument_type TEXT,exchange TEXT,is_active INTEGER);
                    INSERT INTO instrument_labels VALUES ('TEST','Test Stock',NULL,'Test','EQ','NSE',1);
                    CREATE TABLE ohlc_daily (symbol TEXT,bar_time TEXT,open REAL,high REAL,low REAL,close REAL,volume REAL);
                    CREATE TABLE ohlc_5min (symbol TEXT,bar_time TEXT,open REAL,high REAL,low REAL,close REAL,volume REAL);''')
            config=load_config();config['database']=str(source)
            with patch('market_scanner.engine.ROOT',root):
                scanner=Scanner(config)
                def fake(stock,tfs,cutoff,force):
                    return [('TEST',tf,dict(symbol='TEST',timeframe=tf,status='scanned',bar_count=50,last_candle='2026-07-31 15:30:00',signature='test',bars=[],matches=[{'pattern':'channel'}] if fake.has_match else [])) for tf in tfs]
                fake.has_match=True
                with patch.object(scanner,'scan_stock',side_effect=fake):
                    scanner.run_scan(['1H','4H'])
                    self.assertTrue(scanner.cells[('TEST','1H')]['matches'])
                    fake.has_match=False
                    scanner.run_scan(['4H'])
                self.assertEqual(scanner.cells[('TEST','4H')]['matches'],[])
                self.assertTrue(scanner.cells[('TEST','1H')]['matches'])
                restored=Scanner(config)
                self.assertEqual(restored.cells[('TEST','4H')]['matches'],[])
                self.assertTrue(restored.cells[('TEST','1H')]['matches'])

    def test_duplicate_scan_is_not_started(self):
        from market_scanner.engine import Scanner
        import threading
        scanner=Scanner.__new__(Scanner)
        scanner.lock=threading.RLock()
        scanner.progress={'running':True}
        self.assertFalse(scanner.request_scan())


if __name__=='__main__': unittest.main()
