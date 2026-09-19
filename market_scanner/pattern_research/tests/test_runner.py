"""Boundary and persistence checks independent of detector implementations."""
import gzip
import json
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

from market_scanner.pattern_research import runner,store


class RunnerChecks(unittest.TestCase):
    def setUp(self):
        self.spec=dict(pattern_id='PA02',variant='up',side='long',states=['setup'],definition_version='1')
        self.bars=[dict(end='2020-01-01 10:15:00')]
        self.event=dict(signal_index=0,episode=0,state='setup',atr=1.,score=1.,direction='bullish',pattern_start='2020-01-01 09:15:00')

    def test_valid_event_and_empty_registered_result(self):
        runner.validate_events({runner.key(self.spec):[self.event]},[self.spec],self.bars)
        runner.validate_events({},[self.spec],self.bars)

    def test_unregistered_is_not_no_occurrences(self):
        with self.assertRaisesRegex(ValueError,'Unregistered'):
            runner.validate_events({('PA03','up','long'):[self.event]},[self.spec],self.bars)

    def test_duplicate_future_nan_and_unknown_state_rejected(self):
        for events in ([self.event,self.event],[dict(self.event,formation_start_index=1)],
                       [dict(self.event,atr=float('nan'))],[dict(self.event,state='confirmed')]):
            with self.subTest(events=events),self.assertRaises(ValueError):
                runner.validate_events({runner.key(self.spec):events},[self.spec],self.bars)

    def test_artifact_and_single_writer_summary(self):
        stats=dict(n=1,expectancy_pct=-.4)
        evaluation=dict(reference=dict(stats=stats,trades=[{'entry':100}]),walkforward=dict(stats=dict(n=0),trades=[]),folds=[])
        cell=dict(self.spec,timeframe='1H',status='small_sample',occurrences=1,occurrence_events=[self.event],evaluation=evaluation)
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            con=store.connect(root)
            path,digest=store.write_stock_artifact('r','A',dict(run='r',symbol='A',cells=[cell]),root)
            store.commit_stock(con,'r','A',path,digest,root)
            row=con.execute('SELECT reference_n,reference_mean,summary FROM cells').fetchone()
            self.assertEqual(row[:2],(1,-.4))
            self.assertNotIn('occurrence_events',json.loads(row[2]))
            self.assertEqual(len(json.loads(gzip.decompress(Path(path).read_bytes()))['cells']),1)
            # Resuming a stock replaces the same identity instead of duplicating it.
            store.commit_stock(con,'r','A',path,digest,root)
            self.assertEqual(con.execute('SELECT count(*) FROM cells').fetchone()[0],1)
            with self.assertRaisesRegex(ValueError,'digest'):
                store.commit_stock(con,'r','A',path,'wrong',root)
            con.close()

    def test_completed_artifacts_are_verified_before_resume(self):
        stats=dict(n=0)
        cell=dict(self.spec,timeframe='1H',status='no_occurrences',occurrences=0,
                  evaluation=dict(reference=dict(stats=stats),walkforward=dict(stats=stats),folds=[]))
        manifest=dict(id='r',timeframes=['1H'],specifications=[self.spec],history_hashes={'A':'history'})
        with tempfile.TemporaryDirectory() as temporary:
            con=store.connect(temporary)
            path,digest=store.write_stock_artifact('r','A',dict(symbol='A',run='r',history_sha256='history',cells=[cell]),temporary)
            store.commit_stock(con,'r','A',path,digest,temporary)
            metadata=json.loads(con.execute('SELECT metadata FROM stocks').fetchone()[0])
            self.assertTrue(store.verify_stock(con,manifest,'A',metadata,temporary))
            con.execute('DELETE FROM cells');con.commit()
            with self.assertRaisesRegex(ValueError,'coverage'):
                store.verify_stock(con,manifest,'A',metadata,temporary)
            Path(path).write_bytes(gzip.compress(b'{}'))
            with self.assertRaisesRegex(ValueError,'digest'):
                store.verify_stock(con,manifest,'A',metadata,temporary)
            Path(path).unlink()
            with self.assertRaises(FileNotFoundError):
                store.verify_stock(con,manifest,'A',metadata,temporary)
            con.close()

    def test_snapshot_rejects_wrong_location_or_changed_code(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'manifest.json'
            package=Path(temporary)/'code'/'market_scanner'
            package.mkdir(parents=True)
            with self.assertRaisesRegex(ValueError,'frozen code directory'):
                runner.verify_snapshot(path,{})
            (package/'x.py').write_text('changed')
            with patch.object(runner,'PACKAGE',package),self.assertRaisesRegex(ValueError,'integrity'):
                runner.verify_snapshot(path,{'code_hashes':{'x.py':'incorrect'}})


if __name__=='__main__':
    unittest.main()
