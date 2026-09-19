"""Exit-plan evidence must belong to the exact rule that is traded (UX item 1.1) and the
snapshot-mode study refusal is intentional (UX item 4.4)."""
import pytest
from sqlalchemy import select
from kanida_pilot.config import Settings
from kanida_pilot.db import plans,now
from kanida_pilot.errors import PilotError
from kanida_pilot.evidence import Evidence,tradability,HISTORY_LABEL
from test_pilot import app,signup  # noqa: F401  (shared fixture)

MATCH=dict(id='m1',symbol='TEST',pattern='cup_handle',pattern_name='Cup & Handle',timeframe='1D',direction='bullish',
           candle_end='e59',price=100,history=[dict(side='long',run='r1')])

def chart():
 bars=[dict(time=f't{i}',end=f'e{i}',open=100,high=100.5,low=99.5,close=100) for i in range(60)]
 # Handle low 99 and ATR ~1 give a structural distance of 1.25 (1.25% stop).
 found=dict(pattern='cup_handle',start_index=0,lines=[dict(label='Handle',role='shape',points=[dict(index=55,value=99)])])
 return dict(bars=bars,matches=[found])

def cell(rule=None,n=24):
 values=[1.5 if i%2 else .1 for i in range(n)]
 return dict(symbol='TEST',pattern='cup_handle',timeframe='1D',side='long',run_id='r1',rule=rule,
  rule_description='confirmed; next open; frozen rule',candidates_tested=1,reference=dict(n=8),
  assumptions=dict(minimum_test_trades_for_estimate=20,round_trip_fee_bps=30,round_trip_slippage_bps=10),
  splits=dict(train=dict(n=30,selection_score=.5),validation=dict(n=25,selection_score=.4),
              test=dict(n=n,expectancy_pct=sum(values)/n,win_rate=50,expectancy_ci95=[.1,1.2])),
  trades=[dict(split='test',entry_index=i,net_return_pct=v) for i,v in enumerate(values)])

def exit_plan(study):
 ev=Evidence(Settings(),None)
 ev.match=lambda identity:MATCH
 ev.get=lambda path,params=None:study if path=='/api/backtests/cell' else chart()
 return ev.exit_plan('m1','long')

def test_frozen_supported_rule_is_tradable_evidence():
 value=exit_plan(cell(dict(trigger='confirmed',hold=10,stop_atr=2,target_r=2)))
 assert value['status']=='supported' and value['evidence_applies']
 assert value['tradable_evidence'] is True and value['evidence_basis']=='exact_rule_later_test'
 assert value['rule_metrics']['n']==24 and 'Exact tested rule' in value['tradable_reason']
 assert value['history_label']==HISTORY_LABEL

def test_structural_benchmark_is_not_tradable_evidence():
 value=exit_plan(cell(None))
 # Baseline trades exist but no frozen rule: the scanner labels this 'limited'; the rule is the 1:2 structural benchmark.
 assert value['status']=='limited' and value['rule']['kind']=='structure' and value['reward']==2
 assert value['tradable_evidence'] is False and value['evidence_basis']=='illustrative_benchmark'
 assert value['tradable_reason'].startswith('Illustrative benchmark')

def test_tested_stop_inside_structure_cannot_lend_evidence():
 value=exit_plan(cell(dict(trigger='confirmed',hold=10,stop_atr=1,target_r=2)))
 assert value['status']=='limited' and value['tradable_evidence'] is False

def test_small_sample_and_mismatched_rule_are_rejected():
 rule=dict(kind='atr',trigger='confirmed',hold=10,stop_atr=2,target_r=2)
 # A6: tradability now also needs the side, the scanner screen and the pattern direction (fail closed if absent).
 base=dict(status='supported',evidence_applies=True,usable=True,rule=rule,side='long',screen='review',
  evidence=dict(selected_rule={k:v for k,v in rule.items() if k!='kind'},minimum_later_trades=20),rule_metrics=dict(n=12,expectancy_pct=.5))
 ok,reason=tradability(base,'bullish');assert not ok and reason.startswith('Only 12')
 ok,_=tradability(dict(base,rule_metrics=dict(n=24,expectancy_pct=.5)),'bullish');assert ok
 ok,_=tradability(dict(base,rule=dict(rule,hold=6),rule_metrics=dict(n=24,expectancy_pct=.5)),'bullish');assert not ok
 ok,_=tradability(dict(base,rule_metrics=dict(n=24,expectancy_pct=-.1)),'bullish');assert not ok
 ok,_=tradability(dict(base,status='failed',rule_metrics=dict(n=24,expectancy_pct=.5)),'bullish');assert not ok

def untested(data):
 mode=data.get('exit_mode','suggested')
 return dict(id=data['match_id'],symbol='TEST',pattern='cup_handle',pattern_name='Cup & Handle',timeframe='1D',side='long',snapshot_price=100,
  notional=10000,reserved_cost=40,planned_risk=290,quantity=100,stop_pct=2.5,reward=2,hold=10,history={},status='draft',exit_mode=mode,
  exit_evidence=dict(tradable_evidence=False,tradable_reason='Illustrative benchmark only: fixture.') if mode=='suggested' else None,blockers=[])

def test_plan_save_rejects_rule_without_tradable_evidence(app,monkeypatch):
 c=signup(app);monkeypatch.setattr(app.state.evidence,'prepare',untested)
 r=c.post('/api/product/plans',json=dict(match_id='t',side='long',request_id='untested-0001',exit_mode='suggested'))
 assert r.status_code==409 and r.json()['code']=='EXIT_EVIDENCE' and 'Illustrative' in r.json()['error']
 r=c.post('/api/product/plans',json=dict(match_id='t',side='long',request_id='custom-0001',exit_mode='custom'))
 assert r.status_code==409 and 'Custom exits' in r.json()['error']
 assert not c.get('/api/product').json()['plans']

def test_illustrative_plan_is_saved_but_cannot_simulate(app,monkeypatch):
 c=signup(app);monkeypatch.setattr(app.state.evidence,'prepare',untested)
 r=c.post('/api/product/plans',json=dict(match_id='t',side='long',request_id='illustrative-0001',exit_mode='suggested',illustrative=True))
 assert r.status_code==200,r.text;p=r.json()
 assert p['status']=='illustrative' and p['illustrative'] is True and p['tradable_evidence'] is False
 body=dict(plan_id=p['id'],request_id='simulate-illus-01',acknowledge_synthetic=True)
 assert c.post('/api/trading/simulate',json=body).json()['code']=='PLAN_REQUIRED'
 assert c.post('/api/product/plan-action',json=dict(id=p['id'],action='pause')).json()['status']=='paused'
 assert c.post('/api/product/plan-action',json=dict(id=p['id'],action='resume')).json()['status']=='illustrative'
 assert c.post('/api/trading/simulate',json=body).status_code==409

def test_tradable_plan_saves_as_draft(app):
 c=signup(app)
 p=c.post('/api/product/plans',json=dict(match_id='t',side='long',request_id='tradable-0001')).json()
 assert p['status']=='draft' and p['tradable_evidence'] is True and p['illustrative'] is False

def test_legacy_plans_are_annotated_readable_but_not_tradable(app):
 # Was test_legacy_plans_are_annotated_not_blocked, which asserted a no-evidence legacy plan reads as 'draft'
 # (audit A1 bypass). It now reads as 'illustrative'; the stored row and payload are never rewritten.
 c=signup(app);uid=c.get('/api/auth/me').json()['user']['id']
 old=dict(id='legacy1',symbol='OLD',pattern_name='Cup & Handle',timeframe='1D',side='long',quantity=1,notional=100,reserved_cost=.4,
  planned_risk=3,snapshot_price=100,stop_pct=2,reward=2,exit_mode='suggested',evidence_applies=False)
 with app.state.db.tx() as conn:
  conn.execute(plans.insert().values(id='legacy1',user_id=uid,request_id='legacy-request',payload_hash='x',payload=old,status='draft',created=now(),updated=now()))
 saved=c.get('/api/product').json()['plans'][0]
 assert saved['status']=='illustrative' and saved['evidence_check']=='legacy' and saved['tradable_evidence'] is False
 with app.state.db.tx() as conn:assert conn.execute(select(plans.c.payload)).scalar()==old and conn.execute(select(plans.c.status)).scalar()=='draft'

def test_snapshot_mode_refuses_studies_by_design(tmp_path):
 ev=Evidence(Settings(research_directory=str(tmp_path)),None)
 with pytest.raises(PilotError) as error:ev.study('/api/replay',{})
 assert error.value.code=='STUDY_WORKER' and error.value.status==503
