import copy
import unittest
from market_scanner.backtest import Rule,simulate,statistics,mine_cell,rules_config,episodes
from market_scanner.historical import HistoricalReplay
from market_scanner.replay_filter import masks
import numpy as np


def bars(n=100,price=100):
    return [{'time':f'2020-{1+i//28:02d}-{i%28+1:02d} 09:15:00','end':f'2020-{1+i//28:02d}-{i%28+1:02d} 15:30:00',
             'open':price,'high':price+1,'low':price-1,'close':price,'volume':100.,'gap':False} for i in range(n)]


def event(i=40,state='setup'):
    return {'signal_index':i,'state':state,'atr':2.,'episode':i,'score':80,'direction':'bullish','pattern_start':'2020-01-01'}


class TradeTests(unittest.TestCase):
    def setUp(self):self.config=rules_config()

    def test_next_open_costs_and_time_exit(self):
        b=bars();b[40]['close']=50;b[41]['open']=100;b[42].update(high=110,close=108,low=95)
        ts,_=simulate([event()],b,'long',Rule('setup',2),40,100,self.config)
        t=ts[0];self.assertEqual(t['entry_index'],41);self.assertEqual(t['exit_index'],42)
        self.assertAlmostEqual(t['net_return_pct'],7.6);self.assertEqual(t['mfe_pct'],10);self.assertEqual(t['mae_pct'],5)

    def test_same_bar_stop_first_and_bounded_excursion(self):
        b=bars();b[41].update(high=106,low=95)
        ts,_=simulate([event()],b,'long',Rule('setup',3,1,2),40,100,self.config)
        t=ts[0];self.assertEqual(t['exit'],98);self.assertTrue(t['same_bar_stop_target'])
        self.assertEqual(t['mfe_pct'],0);self.assertEqual(t['mfe_upper_pct'],4);self.assertEqual(t['mae_pct'],2)

    def test_gap_stop_fill_and_short_sign(self):
        b=bars();b[42].update(open=90,high=92,low=89,close=91)
        ts,_=simulate([event()],b,'long',Rule('setup',3,1,2),40,100,self.config)
        self.assertEqual(ts[0]['exit'],90);self.assertEqual(ts[0]['exit_reason'],'stop_gap')
        b[41].update(high=101,low=95,close=96)
        ts,_=simulate([event()],b,'short',Rule('setup',3,1,2),40,100,self.config)
        self.assertAlmostEqual(ts[0]['net_return_pct'],3.6)

    def test_no_incomplete_horizon_or_gap_survivor_bias(self):
        b=bars();b[99].update(high=200)
        ts,x=simulate([event(98)],b,'long',Rule('setup',3,1,2),40,100,self.config)
        self.assertEqual(ts,[]);self.assertEqual(x['boundary'],1)
        b[42]['gap']=True
        ts,x=simulate([event()],b,'long',Rule('setup',3),40,100,self.config)
        self.assertEqual(ts,[]);self.assertEqual(x['gap'],1)

    def test_nonoverlapping(self):
        ts,x=simulate([event(40),event(41),event(43)],bars(),'long',Rule('setup',3),40,100,self.config)
        self.assertEqual([t['entry_index'] for t in ts],[41,44]);self.assertEqual(x['overlap'],1)

    def test_expectancy_is_mean_net_return(self):
        ts,_=simulate([event(40),event(45)],bars(),'long',Rule('setup',2),40,100,self.config)
        ts[0]['net_return_pct']=3.;ts[1]['net_return_pct']=-1.
        s=statistics(ts);self.assertEqual(s['win_rate'],50);self.assertEqual(s['expectancy_pct'],1)
        self.assertLess(s['win_rate_ci95'][0],50);self.assertGreater(s['win_rate_ci95'][1],50)

    def test_test_prices_do_not_change_selected_rule(self):
        b=bars(500);ev=[event(i) for i in range(40,498,4)]
        for i in range(500):b[i].update(open=100+i,high=102+i,low=99.5+i,close=101.5+i)
        cfg={**self.config,'minimum_training_trades':3,'minimum_validation_trades':2}
        a=mine_cell(ev,b,'1H','long',cfg)
        self.assertIsNotNone(a['rule'])
        changed=copy.deepcopy(b)
        for v in changed[400:]:v.update(open=50,high=1000,low=1,close=5)
        z=mine_cell(ev,changed,'1H','long',cfg)
        self.assertEqual(a['rule'],z['rule']);self.assertEqual(a['shortlist'],z['shortlist'])
        for t in a['trades']:
            span=a['date_spans'][t['split']]
            self.assertGreaterEqual(t['entry_index'],span['start_index'])
            self.assertLess(t['exit_index'],span['end_index_exclusive'])

    def test_episode_rearm_and_state_transition(self):
        class Replay:
            bars=bars(60);tr=np.ones(60);c=np.ones(60)*100
            def at(self,i,mask):
                if i not in (40,41,42,43,47):return []
                return [{'pattern':'cup_handle','direction':'bullish','state':'confirmed' if i==43 else 'setup','score':80,'pattern_start':'2020'}]
        g,n=episodes(Replay());e=g['cup_handle','long']
        self.assertEqual([v['signal_index'] for v in e],[40,43,47]);self.assertEqual(e[0]['episode'],e[1]['episode'])

    def test_conservative_filter_matches_full_replay(self):
        rng=np.random.default_rng(528);b=bars(400);close=100+np.cumsum(rng.normal(0,1,400))
        for i,row in enumerate(b):row.update(open=close[i],high=close[i]+1,low=close[i]-1,close=close[i],volume=float(rng.integers(50,250)))
        r=HistoricalReplay(b);m=masks(r.h,r.l,r.c,r.v,r.tr,r.tops,r.bottoms)
        for i in range(39,len(b)):self.assertEqual(r.at(i),r.at(i,int(m[i])))

    def test_filter_preserves_all_scanner_positive_and_negative_fixtures(self):
        from unittest.mock import patch
        from market_scanner.tests import test_scanner
        original=test_scanner.detect
        def checked(b,enabled=None):
            expected=original(b,enabled)
            r=HistoricalReplay(b,enabled=enabled)
            m=masks(r.h,r.l,r.c,r.v,r.tr,r.tops,r.bottoms)
            actual=r.at(len(b)-1,int(m[-1])) if b else []
            actual=[{k:v for k,v in a.items() if k not in ('signal_index','history_start_index','window_offset')} for a in actual]
            self.assertEqual(expected,actual)
            return expected
        suite=unittest.defaultTestLoader.loadTestsFromTestCase(test_scanner.DetectorTests)
        result=unittest.TestResult()
        with patch.object(test_scanner,'detect',side_effect=checked):suite.run(result)
        self.assertEqual(result.errors,[]);self.assertEqual(result.failures,[])

    def test_zero_history_and_rare_pattern_do_not_get_estimates(self):
        empty=mine_cell([],[],'1W','long',self.config)
        self.assertEqual(empty['status'],'no_occurrences');self.assertIsNone(empty['next_expectancy_pct'])
        rare=mine_cell([event()],bars(),'1D','long',self.config)
        self.assertEqual(rare['status'],'no_validated_rule');self.assertEqual(rare['reference']['n'],1)
        self.assertIsNone(rare['historical_win_probability_pct'])


if __name__=='__main__':unittest.main()
