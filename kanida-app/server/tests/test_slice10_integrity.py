"""Slice 10 - integrity (GTM audit 2026-09-25): save barrier, quote validity, cost vs market, calendar, alert freshness.

Uses the isolated temporary-DB / fake-market fixtures of test_strategy_builder. No network, no broker.
"""
from datetime import date,datetime,timedelta
import pytest
from test_strategy_builder import live,pilot,strategy,EXP  # noqa: F401 - fixtures
from kanida_pilot.strategy_builder import alerts as AL
from kanida_pilot.strategy_builder import exchange as XC
from kanida_pilot.strategy_builder import execution as EX
from kanida_pilot.strategy_builder import quotes as Q
from kanida_pilot.strategy_builder import service as S


# --- P01: actions name the draft they act on ------------------------------------------------------------------
def test_snapshot_refuses_a_draft_other_than_the_one_on_screen_and_is_idempotent(pilot):
 _a,owner,_o=pilot
 s=strategy(owner);sid=s['id'];v=s['draft']['version'];h=s['draft']['checksum']
 body=dict(s['draft']['body']);body['legs']=[{**l,'price_basis':'manual','price':120.0} if i==0 else l for i,l in enumerate(body['legs'])]
 saved=owner.post(f'/api/sb/strategies/{sid}/draft',json={'version':v,'body':body}).json()
 stale=owner.post(f'/api/sb/strategies/{sid}/snapshots',json={'expected_version':v,'input_hash':h})
 assert stale.status_code==409 and stale.json()['code']=='DRAFT_CHANGED'
 wrong_hash=owner.post(f'/api/sb/strategies/{sid}/snapshots',json={'expected_version':saved['draft']['version'],'input_hash':h})
 assert wrong_hash.status_code==409
 g={'expected_version':saved['draft']['version'],'input_hash':saved['draft']['checksum'],'request_id':'v2'}
 one=owner.post(f'/api/sb/strategies/{sid}/snapshots',json=g).json()
 assert one['checksum']==saved['draft']['checksum'] and one['body']['legs'][0]['price']==120.0
 again=owner.post(f'/api/sb/strategies/{sid}/snapshots',json=g).json()
 assert again['id']==one['id'] and len(owner.get(f'/api/sb/strategies/{sid}').json()['snapshots'])==1


def test_duplicate_and_preview_refuse_a_stale_draft_version(live):
 _app,owner,_o,_fake=live
 s=strategy(owner);sid=s['id']
 owner.post(f'/api/sb/strategies/{sid}/draft',json={'version':s['draft']['version'],'body':s['draft']['body']})
 assert owner.post(f'/api/sb/strategies/{sid}/duplicate',json={'expected_version':s['draft']['version']}).status_code==409
 assert owner.post(f'/api/sb/strategies/{sid}/preview',json={'expected_version':s['draft']['version']}).status_code==409
 assert owner.post(f'/api/sb/strategies/{sid}/preview',json={'expected_version':s['draft']['version']+1}).status_code==200


# --- P02: one quote-validity contract ---------------------------------------------------------------------------
NOW=datetime(2026,9,28,10,0,0)
@pytest.mark.parametrize('row,ok,reason',[
 ({'bid':100,'ask':101,'quote_at':'2026-09-28 09:59:55'},True,None),
 ({'bid':100,'ask':100,'quote_at':'2026-09-28 09:59:55'},True,None),              # locked is allowed
 ({'bid':160,'ask':150,'quote_at':'2026-09-28 09:59:55'},False,'CROSSED'),
 ({'bid':0,'ask':5,'quote_at':'2026-09-28 09:59:55'},False,'ZERO'),
 ({'bid':None,'ask':5,'quote_at':'2026-09-28 09:59:55'},False,'NO_BID_ASK'),
 ({'bid':100,'ask':101,'quote_at':'2026-09-28 09:58:00'},False,'STALE'),          # 120 s old option, whatever spot says
 ({'bid':100,'ask':101,'quote_at':'2026-09-28 10:01:00'},False,'FUTURE_TIME'),
 ({'bid':100,'ask':101,'quote_at':None},False,'NO_TIME'),
])
def test_quote_validity_is_deterministic(row,ok,reason):
 v=Q.leg(row,NOW,'order')
 assert v['ok'] is ok and (reason in v['reasons'] if reason else not v['reasons'])


def test_an_old_option_quote_blocks_review_even_when_spot_is_fresh(live,monkeypatch):
 _app,owner,_o,fake=live
 s=strategy(owner)
 real=fake.chain
 def chain(u,e):
  ch=real(u,e)
  for r in ch['rows']:
   for k in ('CE','PE'):r[k]['quote_at']=(EX.now_ist()-timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')
  return ch
 monkeypatch.setattr(fake,'chain',chain)
 p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 c={x['key']:x for x in p['checks']}
 assert c['quotes_fresh']['status']=='pass' and c['quotes_valid']['status']=='block' and not p['can_submit']


def test_paper_broker_never_fills_on_a_crossed_quote(live):
 app,owner,_o,fake=live
 s=strategy(owner)
 p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 buy=next(o for o in p['orders'] if o['side']=='B')
 d=owner.post(f"/api/sb/strategies/{s['id']}/deployments",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'x1','confirm':True}).json()
 assert d['status'] in ('active','working','partially_filled')
 fake.books[buy['symbol']]=(buy['limit']+5,buy['limit']-1)     # crossed: ask below the limit but bid above the ask
 ex=app.state.strategy_builder_execution
 s2=strategy(owner)
 p2=owner.post(f"/api/sb/strategies/{s2['id']}/preview",json={}).json()
 assert not p2['can_submit']


# --- P04: cost basis vs market valuation -------------------------------------------------------------------------
@pytest.mark.parametrize('template',['long_call','bull_put_spread','call_ratio_spread','iron_condor'])
def test_entry_price_moves_pnl_not_greeks(live,template):
 app,owner,_o,_fake=live
 s=strategy(owner,template);body=s['draft']['body'];market=app.state.strategy_builder_market
 a=S.analysis(market,body)
 changed={**body,'legs':[{**l,'price_basis':'manual','price':round((a['legs'][i]['entry'] or 0)+7.0,2)} for i,l in enumerate(body['legs'])]}
 b=S.analysis(market,changed)
 for k in ('delta','gamma','theta','vega'):assert a['greeks'][k]==b['greeks'][k]
 assert a['pop']['sigma']==b['pop']['sigma']       # POP itself moves with the breakeven; its volatility does not
 units=sum(r['units'] for r in a['legs'])
 assert b['scenario_pnl']['value']==pytest.approx(a['scenario_pnl']['value']-7.0*units,abs=1.0)


def test_an_unsolvable_manual_entry_keeps_valid_market_greeks(live):
 app,owner,_o,_fake=live
 s=strategy(owner,'long_call');body=s['draft']['body'];market=app.state.strategy_builder_market
 absurd={**body,'legs':[{**l,'price_basis':'manual','price':0.05} for l in body['legs']]}     # below intrinsic value
 a=S.analysis(market,absurd)
 assert a['greeks']['status']=='available' and a['legs'][0]['iv_source'].startswith('market_')


def test_reference_volatility_is_the_chain_atm_or_labelled_a_proxy(live):
 app,owner,_o,_fake=live
 s=strategy(owner,'bull_call_spread');a=S.analysis(app.state.strategy_builder_market,s['draft']['body'])
 assert a['pop']['sigma_basis'] in ('chain_atm_iv','nearest_leg_iv_proxy') and 'atm_iv_to_expiry'!=a['sd']['basis']


# --- P09: exchange calendar ----------------------------------------------------------------------------------------
def test_session_gate_knows_nse_holidays_and_refuses_unknown_years():
 assert XC.session(datetime(2026,10,2,10,0))['status']=='holiday'                 # Gandhi Jayanti (NSE FO list)
 assert XC.session(datetime(2026,9,28,10,0))['open'] is True
 assert XC.session(datetime(2026,9,28,15,30))['open'] is False
 assert XC.session(datetime(2026,9,27,10,0))['status']=='closed'                  # Sunday
 assert XC.session(datetime(2027,1,4,10,0))['status']=='HOLIDAY_LIST_MISSING'
 assert XC.add_trading_days(date(2026,10,1),1)==date(2026,10,5)                   # skips the Friday holiday and the weekend


def test_exchange_time_ignores_the_host_timezone(monkeypatch):
 monkeypatch.setenv('TZ','UTC')
 import time as _t
 if hasattr(_t,'tzset'):_t.tzset()
 from zoneinfo import ZoneInfo
 assert abs((XC.now_ist()-datetime.now(ZoneInfo('Asia/Kolkata')).replace(tzinfo=None)).total_seconds())<5


def test_calendar_route_serves_the_versioned_list(pilot):
 _a,owner,_o=pilot
 c=owner.get('/api/sb/calendar').json()
 assert c['version'] and 2026 in c['years'] and any(h['date']=='2026-10-02' for h in c['holidays'])


# --- P10: alert freshness and recovery ---------------------------------------------------------------------------
def test_stale_to_fresh_with_the_condition_held_does_not_fire_twice(live,monkeypatch):
 app,owner,_o,fake=live
 al=app.state.strategy_builder_alerts;al.stop()
 monkeypatch.setattr(AL,'market_open',lambda at=None:True)
 s=strategy(owner)
 owner.post(f"/api/sb/strategies/{s['id']}/alerts",json={'type':'price_cross','params':{'level':22990,'direction':'above'}})   # spot 23000
 al.cycle()
 real=fake._now
 monkeypatch.setattr(fake,'_now',lambda:(EX.now_ist()-timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S'))
 al.cycle();al.cycle()
 rule=owner.get('/api/sb/alerts').json()['rules'][0]
 assert rule['state']=='data_unavailable' and 'min old' in (rule['suppressed'] or '') and rule['last_value']==23000.0
 monkeypatch.setattr(fake,'_now',real)
 al.cycle();al.cycle()
 kinds=[e['kind'] for e in owner.get('/api/sb/alerts').json()['events']]
 assert kinds.count('triggered')==1 and 'recovered' in kinds
 assert owner.get('/api/sb/alerts').json()['rules'][0]['state']=='triggered'


def test_a_rule_armed_before_going_stale_fires_on_a_fresh_crossing(live,monkeypatch):
 app,owner,_o,fake=live
 al=app.state.strategy_builder_alerts;al.stop()
 monkeypatch.setattr(AL,'market_open',lambda at=None:True)
 s=strategy(owner)
 monkeypatch.setattr(fake,'_now',lambda:(EX.now_ist()-timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S'))
 owner.post(f"/api/sb/strategies/{s['id']}/alerts",json={'type':'price_cross','params':{'level':22990,'direction':'above'}})
 al.cycle()
 assert [e['kind'] for e in owner.get('/api/sb/alerts').json()['events']]==['data_unavailable']
 monkeypatch.undo();monkeypatch.setattr(AL,'market_open',lambda at=None:True)
 al.cycle()
 assert 'triggered' in [e['kind'] for e in owner.get('/api/sb/alerts').json()['events']]


# --- P03: discovery results bound to their request --------------------------------------------------------------
def test_use_as_draft_builds_the_server_held_candidate_not_the_current_form(pilot):
 _a,owner,other=pilot
 r=owner.post('/api/sb/discover',json={'underlying':'NIFTY','expiry':EXP,'view':'up','target':23300,'lots':2}).json()
 assert r['result_id'] and r['request_hash'] and r['candidates']
 c=r['candidates'][0]
 assert c['candidate_id'].startswith(r['result_id']) and c['funds']['status']=='unknown' and c['price_basis']==['ltp']   # slice 13: the basis actually used per leg (P06)
 s=owner.post('/api/sb/discover/use',json={'candidate_id':c['candidate_id']}).json()
 b=s['draft']['body']
 assert b['underlying']=='NIFTY' and b['expiry']==EXP and b['template']==c['template']
 assert [(l['side'],l['type'],l['strike'],l['lots']) for l in b['legs']]==[(l['side'],l['type'],l['strike'],l['lots']) for l in c['legs']]
 assert all(l['price_basis']=='exec' for l in b['legs']) and b['scenario']=={'spot':23300.0} and 'LTP' in s['thesis']
 assert other.post('/api/sb/discover/use',json={'candidate_id':c['candidate_id']}).status_code==409     # never another user's result
 assert owner.post('/api/sb/discover/use',json={'candidate_id':'nope.0'}).status_code==409


def test_budget_is_labelled_a_max_loss_budget_not_capital(pilot):
 _a,owner,_o=pilot
 r=owner.post('/api/sb/discover',json={'underlying':'NIFTY','expiry':EXP,'view':'up','target':23300,'budget':1}).json()
 assert not r['candidates'] and 'max-loss budget' in r['binding']['label']


# --- P06: an unhealthy batch is quarantined, and its failure is concise --------------------------------------------
def test_a_batch_with_failures_is_quarantined_from_the_evidence_board(pilot,monkeypatch):
 from test_strategy_builder import daily_series
 from kanida_pilot.strategy_builder import experiments as XP
 app,_owner,_x=pilot
 lab=app.state.strategy_builder_lab
 nifty,vix=daily_series(start='2019-01-01',n=1400)
 monkeypatch.setattr(lab,'series_for',lambda u:(nifty,vix))
 monkeypatch.setitem(XP.GRIDS,'tiny',{'name':'tiny','underlying':'NIFTY','templates':['bull_call_spread'],'weekdays':[2],
  'dte':[[1,7]],'from':'2019-03-01','to':'2024-06-30','split':'2022-01-03','slippage_pct':0.5,'why':'test'})
 out=XP.run_batch(lab,'u-q','tiny',workers=1)
 assert out['healthy'] and out['quarantine'] is None
 before=lab.evidence_board('u-q')['families'].get('NIFTY',{}).get('tests',0)
 assert before>0
 with lab.lock:
  lab.c.execute("update lab_batches set status='completed_with_failures',failed=1 where id=?",(out['id'],))
  for i in range(3000):
   lab.c.execute('insert into lab_runs values(?,?,?,?,?,?,?,?,?,?,?)',(f'f{i}','u-q',None,'backtest','{"batch": "%s"}'%out['id'],'failed',1.0,None,
    "Traceback ...\nAttributeError: 'NoneType' object has no attribute 'lock'",0.0,0.0))
  lab.c.commit()
 fam=lab.evidence_board('u-q')['families']['NIFTY']
 # quant audit F1: quarantined runs keep counting in m (they just cannot be survivors) - m never shrinks
 assert fam['tests']>=before and fam['survivors']==0 and fam['quarantined_tests']>=before
 b=XP.batch(lab,'u-q',out['id'])
 assert not b['healthy'] and 'can be a survivor' in b['quarantine'] and 'still counts as a test' in b['quarantine']
 assert b['failure_summary']==[{'reason':"AttributeError: 'NoneType' object has no attribute 'lock'",'count':3000}]
 assert len(b['failed_runs'])<=5 and all('error' not in f for f in b['failed_runs'])


# --- P07: a mark that fell back to the last trade is never called a liquidation value ------------------------------
def test_deployment_mark_names_its_real_basis(live):
 app,owner,_o,fake=live
 s=strategy(owner)
 p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 d=owner.post(f"/api/sb/strategies/{s['id']}/deployments",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'m1','confirm':True}).json()
 got=app.state.strategy_builder_execution.deployment(d['owner'] if 'owner' in d else d['user_id'],d['id'])
 assert got['mark_basis'].startswith('liquidation') and all(x['mark_basis'] in ('bid','ask') for x in got['positions'] if x['units'])
 for x in p['orders']:fake.books[x['symbol']]=(160.0,150.0)          # crossed books everywhere: fall back to LTP, and say so
 got=app.state.strategy_builder_execution.deployment(d['user_id'],d['id'])
 assert 'indicative' in got['mark_basis'] and all(x['mark_basis']=='ltp' for x in got['positions'] if x['units'])


# --- audit follow-ups (review + quant audit of slice 10) ---------------------------------------------------------
def test_unhealthy_batch_planned_rules_stay_in_the_family_count():
 from kanida_pilot.strategy_builder import evidence as EVB
 import json as _j
 from test_strategy_builder import fake_run,edge
 rid,spec,res,at=fake_run('w','bull_call_spread',4,edge(32))
 e=EVB.entry(rid,_j.loads(spec),_j.loads(res),0.0)
 alone=EVB.board_entries([e])
 assert alone['families']['NIFTY']['tests']==1
 with_missing=EVB.board_entries([e],{'NIFTY':199})               # 199 planned rules of an unhealthy batch produced nothing
 assert with_missing['families']['NIFTY']['tests']==200
 p=with_missing['rules'][0]['p']
 assert p is not None
 # BH with m=200: rank 1 passes only at p <= 0.10/200. The status must follow the 200-test threshold, not the 1-test one.
 assert (with_missing['rules'][0]['status']=='tested_significant')==(p<=EVB.FDR_Q/200 and alone['rules'][0]['status']=='tested_significant')
 q=EVB.board_entries([{**e,'quarantined':True}])
 assert q['rules'][0]['status']!='tested_significant' and q['families']['NIFTY']['tests']==1


def test_a_stuck_running_batch_is_swept_to_abandoned(pilot):
 from kanida_pilot.strategy_builder import experiments as XP
 app,_o,_x=pilot
 lab=app.state.strategy_builder_lab
 with lab.lock:
  lab.c.executescript(XP.SCHEMA)
  lab.c.execute("insert into lab_batches values('Bold','u-s','g','g','{\"underlying\": \"NIFTY\"}','h',10,0,0,'running',0,0,null,null)");lab.c.commit()
 assert XP.sweep_stale(lab)==1
 assert XP.batches(lab,'u-s')[0]['status']=='abandoned'
 # it produced NO result, so nothing was seen and nothing is added to m (a partly-run batch does count - see above)
 assert lab.evidence_board('u-s')['families'].get('NIFTY',{}).get('tests',0)==0


def test_repeated_snapshot_request_is_one_revision_even_via_the_store(pilot):
 app,owner,_o=pilot
 s=strategy(owner);st=app.state.strategy_builder_store;uid=st.c.execute('select user_id from strategies where id=?',(s['id'],)).fetchone()[0]
 a=st.snapshot(uid,s['id'],'',s['draft']['body'],None,{},'v1');b=st.snapshot(uid,s['id'],'',s['draft']['body'],None,{},'v1')
 assert a['id']==b['id'] and b.get('repeated') and len(st.revisions(uid,s['id']))==1


def test_paper_start_refuses_a_stale_draft_version(pilot):
 _a,owner,_o=pilot
 s=strategy(owner)
 owner.post(f"/api/sb/strategies/{s['id']}/draft",json={'version':s['draft']['version'],'body':s['draft']['body']})
 r=owner.post(f"/api/sb/strategies/{s['id']}/paper",json={'confirm':True,'expected_version':s['draft']['version']})
 assert r.status_code==409 and r.json()['code']=='DRAFT_CHANGED'


def test_calendar_coverage_warns_before_the_list_runs_out():
 assert XC.coverage(datetime(2026,9,26,10,0))['warning'] is None
 w=XC.coverage(datetime(2026,12,1,10,0))
 assert w['covers_until']=='2026-12-31' and 'ends 31 Dec 2026' in w['warning']


def test_kite_quote_without_a_timestamp_is_not_given_the_host_clock():
 from kanida_pilot.strategy_builder.kite_market import KiteMarket
 k=KiteMarket.__new__(KiteMarket)
 k.quotes=lambda keys:{keys[0]:{'last_price':23000.0}}
 assert k.reading('NIFTY') is None


def test_time_value_is_a_market_quantity_not_the_cost_basis(live):
 app,owner,_o,_fake=live
 s=strategy(owner,'long_call');body=s['draft']['body'];market=app.state.strategy_builder_market
 a=S.analysis(market,body);b=S.analysis(market,{**body,'legs':[{**l,'price_basis':'manual','price':0.05} for l in body['legs']]})
 assert a['legs'][0]['time_value']==b['legs'][0]['time_value']
