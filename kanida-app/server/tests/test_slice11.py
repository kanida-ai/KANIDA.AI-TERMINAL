"""Slice 11 - everything the GTM audit left after slice 10 (research/paper; live stays gated). Isolated fixtures only."""
import json,time
import pytest
from test_strategy_builder import live,pilot,strategy,EXP,daily_series  # noqa: F401 - fixtures
from kanida_pilot.strategy_builder import lab as LB


# --- 11.1 P22 durable Lab jobs --------------------------------------------------------------------------------------
def _wait(owner,rid,n=150):
 for _ in range(n):
  got=owner.get(f'/api/sb/lab/runs/{rid}').json()
  if got['status'] not in ('queued','running'):return got
  time.sleep(0.1)
 return got


def test_a_run_left_running_by_a_restart_is_recovered_as_failed(pilot):
 app,owner,_o=pilot
 lab=app.state.strategy_builder_lab
 with lab.lock:
  lab.c.execute("insert into lab_runs values('stuck','u1',null,'backtest','{}','running',0.4,null,null,0,null)");lab.c.commit()
 again=LB.Lab(lab.store,lab.market,lab.daily.kanida_db,lab.derivatives_db)
 r=again.run('u1','stuck')
 assert r['status']=='failed' and 'restarted' in r['error']


def test_runs_queue_are_bounded_per_user_cancel_and_carry_a_manifest(pilot,monkeypatch):
 app,owner,_o=pilot
 nifty,vix=daily_series(start='2023-01-02',n=700)
 lab=app.state.strategy_builder_lab
 monkeypatch.setattr(lab.daily,'series',lambda sym:nifty if sym=='NIFTY 50' else vix)
 gate=__import__('threading').Event()
 real=LB.backtest
 def slow(*a,**k):gate.wait(5);return real(*a,**k)
 monkeypatch.setattr(LB,'backtest',slow)
 body={'template':'bull_call_spread','param':4,'from':'2023-01-02','to':'2025-09-01'}
 ids=[owner.post('/api/sb/lab/backtests',json=body).json()['id'] for _ in range(3)]
 busy=owner.post('/api/sb/lab/backtests',json=body)
 assert busy.status_code==429 and busy.json()['code']=='LAB_BUSY'
 third=owner.get(f'/api/sb/lab/runs/{ids[2]}').json()
 assert third['status']=='queued' and third['queue_ahead']>=0            # 2 workers: the third waits
 c=owner.post(f'/api/sb/lab/runs/{ids[2]}/cancel',json={}).json()
 assert c['status']=='cancelled'
 gate.set()
 done=_wait(owner,ids[0])
 assert done['status']=='completed',done.get('error')
 m=done['result']['manifest']
 assert m['request_hash'] and m['model']=='lab-bsm-vix-v1' and m['fees'] and m['underlying_days'][2]>0 and m['underlying_hash'] and m['lot_source'] and m['computed_at'].endswith('IST')
 assert _wait(owner,ids[2])['status']=='cancelled'                       # a cancelled queued run never starts


# --- 11.2 P23 replay coverage ---------------------------------------------------------------------------------------
def test_replay_refuses_a_partial_basket_and_a_thin_overlap():
 legs=[{'symbol':'A','side':'B','units':65,'label':'Buy A'},{'symbol':'B','side':'S','units':-65,'label':'Sell B'}]
 gone=LB.replay(legs,{'A':[('t1',1),('t2',2)],'B':[]})
 assert gone['status']=='incomplete' and not gone['points'] and 'Sell B' in gone['reason'] and 'last' not in gone
 thin=LB.replay(legs,{'A':[(f't{i}',100+i) for i in range(10)],'B':[('t0',50),('t9',40)]})
 assert thin['status']=='insufficient_coverage' and thin['coverage_detail']['common_bars']==2 and 'best' not in thin
 ok=LB.replay(legs,{'A':[('t1',100),('t2',110),('t3',120)],'B':[('t1',50),('t3',40)]})
 assert ok['status']=='ok' and ok['coverage_detail']['share']==pytest.approx(2/3,abs=1e-3)


def test_replay_route_reports_requested_vs_effective(pilot):
 _a,owner,_o=pilot
 s=strategy(owner)
 r=owner.post(f"/api/sb/strategies/{s['id']}/replay",json={'interval':'5minute','days':2})
 assert r.status_code==200
 j=r.json()
 assert j['request']['requested']['interval']=='5minute' and j['request']['effective']['interval']=='15minute'
 assert any('15-minute' in c for c in j['request']['changed'])


# --- 11.3 P18 records ------------------------------------------------------------------------------------------------
def test_snapshot_rename_and_notes_never_change_its_checksum(pilot):
 _a,owner,other=pilot
 s=strategy(owner)
 snap=owner.post(f"/api/sb/strategies/{s['id']}/snapshots",json={}).json()
 r=owner.post(f"/api/sb/revisions/{snap['id']}",json={'name':'Before RBI','notes':'Thesis: range until policy.'}).json()
 assert r['name']=='Before RBI' and r['notes'].startswith('Thesis') and r['checksum']==snap['checksum'] and r['intact']
 assert other.post(f"/api/sb/revisions/{snap['id']}",json={'name':'x'}).status_code==404
 assert owner.post(f"/api/sb/revisions/{snap['id']}",json={'name':'  '}).status_code==400
 listed=owner.get(f"/api/sb/strategies/{s['id']}").json()['snapshots'][0]
 assert listed['name']=='Before RBI' and listed['notes'].startswith('Thesis')


def test_library_sorts_and_badges_expired_and_empty(pilot):
 _a,owner,_o=pilot
 strategy(owner)
 owner.post('/api/sb/strategies',json={'body':{'underlying':'NIFTY','expiry':'2020-01-30','legs':[]},'name':'Aardvark old'})
 rows=owner.get('/api/sb/strategies?sort=name').json()['strategies']
 assert rows[0]['name']=='Aardvark old' and set(rows[0]['badges'])=={'expired','empty'}
 assert rows[0]['deployments']=={'open':0,'attention':0,'closed':0}
 assert owner.get('/api/sb/strategies?sort=bogus').status_code==400


# --- 11.4 P20 deployment monitor ------------------------------------------------------------------------------------
def test_deployment_keeps_last_known_value_and_shows_residual_exposure(live,monkeypatch):
 app,owner,_o,fake=live
 s=strategy(owner)
 p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 sell=next(o for o in p['orders'] if o['side']=='S')
 d=owner.post(f"/api/sb/strategies/{s['id']}/deployments",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'dm1','confirm':True}).json()
 ex=app.state.strategy_builder_execution
 got=ex.deployment(d['user_id'],d['id'])
 assert got['net'] is not None and got['last_known'] is None
 assert not got['exposure_mismatch']
 monkeypatch.setattr(fake,'chain',lambda u,e:(_ for _ in ()).throw(RuntimeError('feed down')))
 down=ex.deployment(d['user_id'],d['id'])
 assert down['net'] is None and down['last_known']['net']==got['net'] and down['last_known']['age_seconds']>=0


def test_a_hedge_filled_without_its_short_shows_the_residual(live):
 app,owner,_o,fake=live
 s=strategy(owner)
 p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 sell=next(o for o in p['orders'] if o['side']=='S')
 fake.books[sell['symbol']]=(sell['limit']-5,sell['limit']-4)          # the bid never reaches the sell limit: it rests
 d=owner.post(f"/api/sb/strategies/{s['id']}/deployments",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'dm2','confirm':True}).json()
 got=app.state.strategy_builder_execution.deployment(d['user_id'],d['id'])
 short=next(e for e in got['exposure'] if e['leg_id']==sell['leg_id'])
 assert got['exposure_mismatch'] and short['held']==0 and short['planned']<0 and short['residual']==-short['planned']


# --- 11.5 P21 alert settings ------------------------------------------------------------------------------------------
def test_alert_settings_are_editable_under_the_version_guard(pilot):
 _a,owner,_o=pilot
 s=strategy(owner)
 r=owner.post(f"/api/sb/strategies/{s['id']}/alerts",json={'type':'price_cross','params':{'level':23100,'direction':'above'}}).json()
 u=owner.post(f"/api/sb/alerts/{r['id']}",json={'version':r['version'],'cooldown':1800,'session':'always','channels':['in_app','browser','sms']}).json()
 assert u['cooldown']==1800 and u['session']=='always' and u['channels']==['in_app','browser'] and u['version']==r['version']+1
 assert owner.post(f"/api/sb/alerts/{r['id']}",json={'version':r['version'],'cooldown':60}).status_code==409
 assert owner.post(f"/api/sb/alerts/{r['id']}",json={'version':u['version'],'session':'weekends'}).status_code==400
 assert 'suppressed' in u


# --- 11.6 P15 (non-live): an unknown outcome is reconciled by key, never re-sent ---------------------------------------
from test_strategy_builder import bridged  # noqa: E402,F401


def test_accepted_then_lost_response_is_reconciled_by_lookup_never_a_second_post(bridged):
 _a,owner,_f,engine=bridged
 engine.drop_response=True
 s=strategy(owner,'iron_condor',param=4);p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 r=owner.post(f"/api/sb/strategies/{s['id']}/autotrade",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'k-unk-0001'}).json()
 assert r['intent_id']=='i1' and r['state'] in ('accepted','dry_run_complete')     # found by its key right away
 assert len([c for c in engine.calls if c[0]=='POST'])==1


def test_an_unanswered_handoff_stays_unknown_until_found_or_released_by_the_user(bridged,monkeypatch):
 _a,owner,_f,engine=bridged
 engine.silent=True                                                     # connected, never answers (POST and the lookup)
 s=strategy(owner,'iron_condor',param=4);p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 r=owner.post(f"/api/sb/strategies/{s['id']}/autotrade",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'k-unk-0002'}).json()
 assert r['state']=='unknown' and 'nothing will be re-sent' in r['reason']
 p2=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 blocked=owner.post(f"/api/sb/strategies/{s['id']}/autotrade",json={'preview_id':p2['id'],'preview_hash':p2['hash'],'idempotency_key':'k-unk-0003'})
 assert blocked.status_code==409 and blocked.json()['code']=='HANDOFF_UNRESOLVED'
 engine.silent=False                                                    # the engine is back and has no record of it
 import kanida_pilot.strategy_builder.autotrade_bridge as AB
 real=AB.time.time
 monkeypatch.setattr(AB.time,'time',lambda:real()+3600)
 engine.silent=True                                                      # the lookup itself gets no answer: stays unknown
 got=owner.get(f"/api/sb/autotrade/routes/{r['id']}").json()
 assert got['state']=='unknown'
 assert owner.post(f"/api/sb/autotrade/routes/{r['id']}/release",json={}).status_code==400
 rel=owner.post(f"/api/sb/autotrade/routes/{r['id']}/release",json={'confirm':True}).json()
 assert rel['state']=='released_by_user' and 'never confirmed' in rel['reason']
 engine.silent=False
 p3=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 again=owner.post(f"/api/sb/strategies/{s['id']}/autotrade",json={'preview_id':p3['id'],'preview_hash':p3['hash'],'idempotency_key':'k-unk-0004'})
 assert again.status_code==200                                           # the block lifts only by the user's explicit release
 assert len([c for c in engine.calls if c[0]=='POST'])==2


# --- 11.14 P24 release evidence --------------------------------------------------------------------------------------
def test_ops_metrics_time_routes_count_the_funnel_and_store_no_identity(pilot):
 app,owner,other=pilot
 s=strategy(owner)
 owner.post('/api/sb/analyze',json={'body':s['draft']['body']})
 owner.post(f"/api/sb/strategies/{s['id']}/snapshots",json={})
 m=owner.get('/api/sb/ops/metrics').json()
 steps={f['step']:f for f in m['funnel']}
 assert steps['start']['events']>=1 and steps['analyze']['events']>=1 and steps['snapshot']['events']==1 and steps['start']['sessions']==1
 r=next(x for x in m['latency']['routes'] if x['route']=='POST /api/sb/strategies/{sid}/snapshots')
 assert r['n']==1 and r['p50_ms'] is not None and r['errors_5xx']==0
 assert other.get('/api/sb/ops/metrics').status_code==403
 st=app.state.strategy_builder_store
 raw=' '.join(str(tuple(x)) for x in st.c.execute('select * from sb_events').fetchall())
 assert s['id'] not in raw and 'example.invalid' not in raw and 'kanida_session' not in raw


# --- audit follow-ups (dev-reviewer + dev-quant-auditor, slice 11) --------------------------------------------------
def test_a_cancelled_run_can_never_become_completed(pilot):
 app,_o,_x=pilot
 lab=app.state.strategy_builder_lab
 with lab.lock:
  lab.c.execute("insert into lab_runs values('r1','u1',null,'backtest','{}','cancelled',0.5,null,'Cancelled by you.',0,1)");lab.c.commit()
 assert not lab._move('r1','running','completed',progress=1.0)
 assert not lab._move('r1','queued','running')
 assert lab.run('u1','r1')['status']=='cancelled'


def test_a_cli_lab_never_recovers_the_servers_live_runs(pilot):
 app,owner,_x=pilot
 lab=app.state.strategy_builder_lab
 with lab.lock:
  lab.c.execute("insert into lab_runs values('live1','u1',null,'backtest','{}','running',0.3,null,null,0,null)")
  lab.c.execute("insert into lab_run_owner values('live1',?)",(lab.boot,));lab.c.commit()
 LB.Lab(lab.store,lab.market,lab.daily.kanida_db,lab.derivatives_db,recover=False)
 assert lab.run('u1','live1')['status']=='running'
 LB.Lab(lab.store,lab.market,lab.daily.kanida_db,lab.derivatives_db)       # a NEW server process: the old boot's run is dead
 assert lab.run('u1','live1')['status']=='failed'


def test_a_stock_without_a_lot_size_never_borrows_niftys(pilot,monkeypatch):
 app,_o,_x=pilot
 lab=app.state.strategy_builder_lab
 monkeypatch.setattr(lab.market,'expiries',lambda u:(_ for _ in ()).throw(RuntimeError('down')))
 with pytest.raises(LB.LabError):lab.lot_size('RELIANCE')
 assert lab.lot_size('NIFTY',with_source=True)==(65,'fallback_nifty_65')


def test_replay_coverage_counts_against_the_exchange_session_not_the_best_leg():
 legs=[{'symbol':'A','side':'B','units':65,'label':'A'},{'symbol':'B','side':'S','units':-65,'label':'B'}]
 thin={'A':[(f'2026-09-24 09:{15+i}:00',100+i) for i in range(10)],'B':[(f'2026-09-24 09:{15+i}:00',50) for i in range(10)]}
 assert LB.replay(legs,thin)['status']=='ok'                                  # both legs thin, 100% of each other
 r=LB.replay(legs,thin,expected=125)
 assert r['status']=='insufficient_coverage' and r['coverage_detail']['denominator']=='expected_session_bars'
 from datetime import datetime
 assert LB.expected_bars('15minute',datetime(2026,9,28,0,0),datetime(2026,9,29,15,40))==50          # two full sessions
 assert LB.expected_bars('15minute',datetime(2026,10,2,0,0),datetime(2026,10,2,15,40))==0           # NSE holiday


def test_alert_update_on_a_stale_version_is_a_conflict_not_a_silent_loss(pilot):
 _a,owner,_o=pilot
 s=strategy(owner)
 r=owner.post(f"/api/sb/strategies/{s['id']}/alerts",json={'type':'price_cross','params':{'level':23100,'direction':'above'}}).json()
 from kanida_pilot.strategy_builder import alerts as ALM
 al=_a.state.strategy_builder_alerts;uid=al.c.execute('select user_id from alert_rules where id=?',(r['id'],)).fetchone()[0]
 real=al.rule
 al.rule=lambda u,rid:{**real(u,rid),'version':r['version']}                       # the row changed between read and write
 al.c.execute('update alert_rules set version=version+1 where id=?',(r['id'],));al.c.commit()
 with pytest.raises(ALM.AlertError) as e:al.update(uid,r['id'],{'version':r['version'],'cooldown':600})
 assert e.value.code=='VERSION_CONFLICT'


def test_exposure_is_computed_from_orders_even_when_marks_fail(live,monkeypatch):
 app,owner,_o,fake=live
 s=strategy(owner)
 p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 for o in p['orders']:
  if o['side']=='S':fake.books[o['symbol']]=(o['limit']-5,o['limit']-4)
 d=owner.post(f"/api/sb/strategies/{s['id']}/deployments",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'ex1','confirm':True}).json()
 monkeypatch.setattr(fake,'chain',lambda u,e:(_ for _ in ()).throw(RuntimeError('feed down')))
 got=app.state.strategy_builder_execution.deployment(d['user_id'],d['id'])
 assert got['exposure_mismatch'] and any(e['residual'] for e in got['exposure'])


def test_a_sending_handoff_left_by_a_crash_becomes_unknown_on_restart(bridged):
 app,owner,_f,engine=bridged
 st=app.state.strategy_builder_store
 st.c.execute("insert into autotrade_routes values('rx','u1','s1','p1','k1','dry_run',null,'sending',null,null,0,0)");st.c.commit()
 from kanida_pilot.strategy_builder.autotrade_bridge import AutotradeRoutes
 AutotradeRoutes(st,engine and app.state.strategy_builder_autotrade.bridge)
 assert st.c.execute("select state from autotrade_routes where id='rx'").fetchone()[0]=='unknown'


def test_an_authoritative_no_record_from_autotrade_resolves_unknown_to_not_received(bridged):
 _a,owner,_f,engine=bridged
 engine.silent=True
 s=strategy(owner,'iron_condor',param=4);p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 r=owner.post(f"/api/sb/strategies/{s['id']}/autotrade",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'k-nr-1'}).json()
 assert r['state']=='unknown'
 engine.silent=False                                                      # exact lookup: 404 INTENT_NOT_FOUND
 got=owner.get(f"/api/sb/autotrade/routes/{r['id']}").json()
 assert got['state']=='not_received' and 'exact lookup' in got['reason']
 assert [c for c in engine.calls if c[0]=='GET' and c[1].endswith('/by-key')] and len([c for c in engine.calls if c[0]=='POST'])==1
