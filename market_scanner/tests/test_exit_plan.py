import copy
import unittest
from market_scanner.exit_plan import describe, assess
from market_scanner.product import build_plan


def fixture():
    bars=[dict(time=f'2026-07-{i%28+1:02d} 09:15:00',end=f'2026-07-{i%28+1:02d} 15:30:00',open=100,high=100.5,low=99.5,close=100) for i in range(50)]
    match=dict(id='TEST:1H:channel',symbol='TEST',pattern='channel',pattern_name='Channel',timeframe='1H',direction='bullish',price=100,current=False,candle_end=bars[-1]['end'],history=[dict(side='long',run='frozen',reference={'n':40})])
    found=dict(pattern='channel',start_index=10,lines=[dict(label='Support',role='boundary',points=[dict(index=10,value=99.5),dict(index=49,value=99.5)])])
    chart=dict(bars=bars,matches=[found])
    stats=dict(n=30,expectancy_pct=.9,expectancy_ci95=[.8,1.0],selection_score=.8,win_rate=100)
    study=dict(symbol='TEST',pattern='channel',timeframe='1H',side='long',run_id='frozen',reference={'n':40},
        rule=dict(trigger='confirmed',stop_atr=2,target_r=3,hold=12),candidates_tested=56,
        splits={k:stats.copy() for k in ('train','validation','test')},
        trades=[dict(split='test',entry_index=i,net_return_pct=.8 if i%2 else 1.0) for i in range(30)])
    return match,chart,study


class ExitPlanTests(unittest.TestCase):
    def test_exact_selected_rule_and_its_own_metrics(self):
        m,c,s=fixture();p=describe(m,'long',c,s)
        self.assertEqual(p['status'],'supported')
        self.assertEqual((p['reward'],p['rule']['hold'],p['rule']['trigger']),(3,12,'confirmed'))
        self.assertEqual((p['stop'],p['target']),(98,106))
        self.assertEqual(p['rule_metrics']['n'],30)
        self.assertFalse(p['execution_ready'])

    def test_baseline_does_not_validate_benchmark(self):
        m,c,s=fixture();s.update(rule=None,splits={},trades=[])
        s['reference'].update(win_rate=100,expectancy_pct=10)
        p=describe(m,'long',c,s)
        self.assertEqual(p['status'],'limited')
        self.assertEqual(p['reward'],2)
        self.assertEqual(p['stop_pct'],1)  # Geometry/noise, not a universal 2.5%.
        self.assertIsNone(p['rule_metrics'])
        self.assertFalse(p['evidence_applies'])

    def test_no_history_is_default_and_failed_test_is_not_replaced(self):
        m,c,s=fixture();p=describe(m,'long',c,{})
        self.assertEqual(p['status'],'default')
        s['splits']['test'].update(expectancy_pct=-1,expectancy_ci95=[-2,-.5])
        s['trades']=[dict(split='test',entry_index=i,net_return_pct=-1) for i in range(30)]
        self.assertEqual(describe(m,'long',c,s)['status'],'failed')

    def test_small_unstable_or_time_only_rule_cannot_claim_support(self):
        _,_,s=fixture()
        small=copy.deepcopy(s);small['trades']=small['trades'][:5];small['splits']['test']['n']=5
        unstable=copy.deepcopy(s)
        for t in unstable['trades'][:15]:t['net_return_pct']=-.1
        timed=copy.deepcopy(s);timed['rule'].update(stop_atr=0,target_r=0)
        for value in (small,unstable,timed):self.assertEqual(assess(value)['status'],'limited')

    def test_wider_geometry_cannot_borrow_tested_atr_evidence(self):
        m,c,s=fixture();c['matches'][0]['lines'][0]['points'][-1]['value']=95
        p=describe(m,'long',c,s)
        self.assertEqual(p['status'],'limited');self.assertEqual(p['reward'],2)
        self.assertIsNone(p['rule_metrics'])

    def test_wrong_identity_snapshot_and_short_orientation(self):
        m,c,s=fixture();wrong=copy.deepcopy(s);wrong['side']='short'
        with self.assertRaises(ValueError):describe(m,'long',c,wrong)
        with self.assertRaises(ValueError):describe({**m,'candle_end':'old'},'long',c,s)
        s['side']='short';m['direction']='bearish';m['history'][0]['side']='short'
        p=describe(m,'short',c,s)
        self.assertGreater(p['stop'],p['price']);self.assertLess(p['target'],p['price'])

    def test_saving_binds_rules_and_custom_edits_remove_evidence(self):
        m,c,s=fixture();p=describe(m,'long',c,s)
        data=dict(match_id=m['id'],side='long',exit_mode='suggested',suggestion_id=p['id'])
        saved=build_plan(data,[m],p)
        self.assertEqual((saved['reward'],saved['hold']),(3,12))
        self.assertTrue(saved['evidence_applies'])
        self.assertEqual(saved['exit_rule']['stop_atr'],2)
        custom=build_plan({**data,'exit_mode':'custom','reward':1.5,'stop_pct':3,'hold':4},[m],p)
        self.assertFalse(custom['evidence_applies']);self.assertIsNone(custom['exit_evidence'])
        self.assertEqual(custom['hold'],4)
        with self.assertRaises(ValueError):build_plan({**data,'suggestion_id':'forged'},[m],p)
        with self.assertRaises(ValueError):build_plan(data,[m])


if __name__=='__main__':unittest.main()
