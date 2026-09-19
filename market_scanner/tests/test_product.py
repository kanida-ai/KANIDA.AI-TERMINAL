import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from market_scanner import product


def match():
    return dict(id='TEST:1H:cup_handle',symbol='TEST',pattern='cup_handle',
        pattern_name='Cup & Handle',timeframe='1H',direction='bullish',price=100,current=False,
        candle_end='2026-07-31 15:30:00',history=[dict(side='long',reference={'n':5})])


class ProductTests(unittest.TestCase):
    def data(self, **kwargs):
        return dict(match_id=match()['id'],side='long',request_id='request-1',**kwargs)

    def test_whole_share_cost_and_risk_limits(self):
        p=product.build_plan(self.data(),[match()])
        self.assertEqual(p['quantity'],99)
        self.assertLessEqual(p['notional']+p['reserved_cost'],p['allocation'])
        self.assertLessEqual(p['planned_risk'],p['account']*p['risk_pct']/100)
        p=product.build_plan(self.data(allocation=100000,risk_pct=.1),[match()])
        self.assertEqual(p['quantity'],34)

    def test_no_execution_or_stale_fill(self):
        p=product.build_plan(self.data(),[match()])
        self.assertEqual(p['status'],'draft')
        self.assertEqual(p['execution'],'not_connected')
        self.assertIn('Fresh completed-candle prices are required',p['blockers'])
        self.assertNotIn('entry_price',p)

    def test_invalid_parameters(self):
        for values in [{'risk_pct':float('nan')},{'allocation':100001},{'stop_pct':0},{'account':-1},{'reward':6}]:
            with self.subTest(values=values),self.assertRaises(ValueError):
                product.build_plan(self.data(**values),[match()])
        with self.assertRaises(ValueError):product.build_plan(self.data(),[])
        with self.assertRaises(ValueError):product.build_plan({**self.data(),'side':'short'},[match()])

    def test_cannot_afford_one_share(self):
        with self.assertRaises(ValueError):product.build_plan(self.data(),[{**match(),'price':1000000}])

    def test_plan_persistence_idempotence_and_actions(self):
        with tempfile.TemporaryDirectory() as directory,patch.object(product,'DB',Path(directory)/'product.db'),patch.object(product,'read_engine',return_value=[match()]):
            product.initialize()
            first=product.save_plan(self.data())
            retry=product.save_plan(self.data())
            self.assertEqual(first['id'],retry['id'])
            self.assertEqual(len(product.list_product()['plans']),1)
            paused=product.change_plan({'id':first['id'],'action':'pause'})
            self.assertEqual(paused['status'],'paused')
            resumed=product.change_plan({'id':first['id'],'action':'resume'})
            self.assertEqual(resumed['status'],'draft')
            self.assertEqual(resumed['execution'],'not_connected')
            product.change_plan({'id':first['id'],'action':'archive'})
            with self.assertRaises(ValueError):product.change_plan({'id':first['id'],'action':'resume'})
            self.assertEqual(len(product.list_product()['events']),4)

    def test_watch_persistence_and_idempotence(self):
        with tempfile.TemporaryDirectory() as directory,patch.object(product,'DB',Path(directory)/'product.db'),patch.object(product,'read_engine',return_value=[match()]):
            product.initialize()
            data={'match_id':match()['id'],'action':'add'}
            product.watch_action(data)
            product.watch_action(data)
            state=product.list_product()
            self.assertEqual(len(state['watchlist']),1)
            self.assertEqual(state['watchlist'][0]['candle_end'],match()['candle_end'])
            self.assertEqual(len(state['events']),1)
            self.assertEqual(state['plans'],[])
            product.watch_action({**data,'action':'remove'})
            self.assertEqual(product.list_product()['watchlist'],[])
            with self.assertRaises(ValueError):product.watch_action({**data,'action':'execute'})
            with self.assertRaises(ValueError):product.watch_action({**data,'match_id':'missing'})


if __name__=='__main__': unittest.main()
