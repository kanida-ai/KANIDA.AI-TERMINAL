"""Slice 12 - trustworthy evidence, faster building, closing partials, multi-expiry. Isolated fixtures only."""
import json
import pytest
from test_strategy_builder import live,pilot,strategy,EXP,daily_series,fake_run,edge  # noqa: F401
from kanida_pilot.strategy_builder import lab as LB
from kanida_pilot.strategy_builder import evidence as EVB


# --- A4 realistic stock slippage -----------------------------------------------------------------------------------------
def test_stock_fills_pay_at_least_the_measured_half_spread_bucketed_by_pct_of_spot():
 # buckets by premium as % of spot (point-in-time; a later split cannot move a premium between buckets - quant C4)
 assert LB.half_spread(1.0,1000)==pytest.approx(0.1429) and LB.half_spread(5.0,1000)==pytest.approx(0.0966)
 assert LB.half_spread(20.0,1000)==pytest.approx(0.0217) and LB.half_spread(50.0,1000)==pytest.approx(0.0448)
 assert LB.half_spread(0.1,100)==LB.half_spread(1.0,1000)                                        # same ratio, same bucket
 assert LB._fill(5.0,'B',True,0.01,stock=True,spot=1000)==pytest.approx(5.0+5.0*0.0966,abs=0.01)
 assert LB._fill(3.0,'B',True,0.01,stock=False)==pytest.approx(3.05,abs=0.01)                    # index: spec slippage (min 0.05)
 assert LB._fill(50,'S',True,0.10,stock=True,spot=1000)==pytest.approx(45.0)                     # a larger spec is never lowered


# --- A5 bad-price guard ----------------------------------------------------------------------------------------------------
def _s(days,closes):return {'days':days,'open':list(closes),'high':list(closes),'low':list(closes),'close':list(closes),'sources':{}}
def test_guard_records_breaks_without_truncating_and_a_futures_confirmed_crash_is_kept():
 days=['2022-10-24','2022-10-25','2022-10-26','2022-10-27','2022-10-28'];nm=_s(days,[15.6,15.7,15.75,29.25,29.4])
 g=LB.guard_series(nm)
 assert g['days']==days and g['breaks'][0]['day']=='2022-10-27' and 'no futures record' in g['breaks'][0]['reason']   # never truncated
 stray=_s(['2016-03-01','2016-03-02','2022-05-24','2022-05-25'],[100,101,500,505])
 assert 'gap' in LB.guard_series(stray)['breaks'][0]['reason']
 class Fut:                                    # the stock's futures fell as much: a genuine crash stays in the sample
  def futures_move(self,sym,d0,d1):return -0.50
 crash=_s(['2020-03-13','2020-03-16'],[100,50])
 assert LB.guard_series(crash,Fut(),'YESBANK')['breaks']==[]
 assert LB.guard_series(_s(['2024-01-01','2024-01-02'],[100,103]))['guard'] is None


def test_a_trade_crossing_a_data_break_is_excluded_but_earlier_ones_are_untouched():
 from test_strategy_builder import lab_spec
 nifty,vix=daily_series(start='2021-01-04',n=200)
 bad=dict(nifty);bad['breaks']=[{'day':nifty['days'][120],'reason':'test'}]
 spec=lab_spec(template='bull_call_spread',param=4,weekday=2,dte_min=1,dte_max=10,**{'from':nifty['days'][0],'to':nifty['days'][-1],'split':nifty['days'][100]})
 base,_=LB.simulate(spec,nifty,vix,65);cut,sk=LB.simulate(spec,bad,vix,65)
 assert sk.get('data_break',0)>0
 early=[t for t in base if t['expiry']<nifty['days'][120]]
 assert [t['entry'] for t in cut if t['expiry']<nifty['days'][120]]==[t['entry'] for t in early]


# --- A7 width as % of spot; never too close to expiry ----------------------------------------------------------------------
def test_stock_strike_step_is_one_percent_of_spot():
 assert LB.stock_step(1575.0)==15.75 and LB.stock_step(15.75)==0.1575


# --- A6 evidence versioning --------------------------------------------------------------------------------------------------
def test_evidence_entries_carry_their_version_and_model():
 rid,spec,res,at=fake_run('v1run','bull_call_spread',4,edge(32))
 e=EVB.entry(rid,json.loads(spec),{**json.loads(res),'model':'lab-bsm-vix-v1'},0.0)
 assert e['ev']==EVB.EVIDENCE_VERSION and e['model']=='lab-bsm-vix-v1'


def test_a_stock_run_of_an_older_lab_model_is_quarantined_but_still_counted(pilot):
 app,_o,_x=pilot
 lab=app.state.strategy_builder_lab
 rid,spec,res,at=fake_run('old1','bull_call_spread',4,edge(32))
 sp={**json.loads(spec),'underlying':'RELIANCE'};r={**json.loads(res),'model':'lab-bsm-vix-v1'}
 with lab.lock:
  lab.c.execute('insert into lab_runs values(?,?,?,?,?,?,?,?,?,?,?)',('old1','u9',None,'backtest',json.dumps(sp),'completed',1.0,json.dumps(r),None,0.0,0.0));lab.c.commit()
 fam=lab.evidence_board('u9')['families']['STOCKS']
 assert fam['survivors']==0 and fam['tests']>=1 and fam['quarantined_tests']>=1
 assert LB.model_for('RELIANCE')=='lab-bsm-vix-v2-stock' and LB.model_for('NIFTY')=='lab-bsm-vix-v1'


# --- A9 point-in-time leakage (property test) --------------------------------------------------------------------------------
@pytest.mark.parametrize('seed',range(12))
def test_no_decision_ever_depends_on_data_after_it(seed):
 import random
 from test_strategy_builder import lab_spec
 rnd=random.Random(seed)
 nifty,vix=daily_series(start='2021-01-04',n=500)
 tpl=rnd.choice(['bull_call_spread','iron_condor','bear_put_spread','short_strangle','long_straddle'])
 spec=lab_spec(template=tpl,param=None,weekday=rnd.choice([0,1,2,3,4]),dte_min=rnd.choice([1,3]),dte_max=rnd.choice([7,14]))
 spec={**spec,'param':LB.BY_KEY[tpl]['param']['default'] if LB.BY_KEY[tpl]['param'] else None}
 base,_=LB.simulate(spec,nifty,vix,65)
 if not base:pytest.skip('no trade for this rule')
 k=rnd.randrange(len(base));t=base[k];cut=t['entry']
 def perturb(s):
  out={**s}
  for key in ('open','high','low','close'):
   if key in s:out[key]=[v*(1+rnd.uniform(-.3,.3)) if d>cut else v for d,v in zip(s['days'],s[key])]
  return out
 again,_=LB.simulate(spec,perturb(nifty),perturb(vix),65)
 same=[x for x in again if x['entry']==cut]
 assert same,'the trade entered on the cut day disappeared after changing only LATER data'
 a=same[0]
 assert (a['decision'],a['expiry'],[(l['side'],l['type'],l['strike'],l['entry']) for l in a['legs']])==(t['decision'],t['expiry'],[(l['side'],l['type'],l['strike'],l['entry']) for l in t['legs']])


# --- A8 evidence attached to a strategy -----------------------------------------------------------------------------------
def test_strategy_evidence_names_the_structure_and_version(pilot):
 _a,owner,_o=pilot
 s=strategy(owner,'bull_call_spread')
 e=owner.get(f"/api/sb/strategies/{s['id']}/evidence").json()
 assert e['structure']=='Bull Call Spread' and e['status'] in ('model_only','insufficient','tested_significant','tested_not_significant')
 assert e['evidence_version']==EVB.EVIDENCE_VERSION
 legs=s['draft']['body']['legs'];odd={**s['draft']['body'],'legs':legs+[{**legs[0],'id':'L9','strike':legs[0]['strike']+400}]}
 owner.post(f"/api/sb/strategies/{s['id']}/draft",json={'version':s['draft']['version'],'body':odd})
 assert owner.get(f"/api/sb/strategies/{s['id']}/evidence").json()['status']=='none'


# --- B5 paper exits --------------------------------------------------------------------------------------------------------
def _deploy(owner,fake,template='bull_call_spread',key='b5'):
 s=strategy(owner,template)
 p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 d=owner.post(f"/api/sb/strategies/{s['id']}/deployments",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':key,'confirm':True}).json()
 return s,d


def test_an_exit_rule_closes_the_paper_position_through_the_normal_review(live):
 app,owner,_o,fake=live
 s,d=_deploy(owner,fake)
 assert owner.post(f"/api/sb/deployments/{d['id']}/exit-rules",json={'stop_pct':500}).status_code==400
 r=owner.post(f"/api/sb/deployments/{d['id']}/exit-rules",json={'stop_pct':1}).json()
 assert r['exit_rules']['stop_pct']==1
 ex=app.state.strategy_builder_execution
 note=ex.check_exit_rules(d['user_id'],d['id'])                      # paying the spread already loses more than 1% of max loss
 assert note and 'stop rule met' in note and 'close orders placed' in note
 got=ex.deployment(d['user_id'],d['id'])
 assert got['status'] in ('closing','closed') and any(i['kind']=='close' for i in got['intents'])


def test_an_expired_paper_position_settles_at_intrinsic(live,monkeypatch):
 app,owner,_o,fake=live
 s,d=_deploy(owner,fake,key='b5x')
 ex=app.state.strategy_builder_execution
 from kanida_pilot.strategy_builder import execution as EXM
 import datetime as _dt
 monkeypatch.setattr(EXM,'now_ist',lambda:_dt.datetime.strptime(EXP,'%Y-%m-%d')+_dt.timedelta(days=1,hours=10))
 monkeypatch.setattr(ex,'_expiry_spot',lambda u,e:(f'{e} 15:29:00',23300.0))
 got=ex.deployment(d['user_id'],d['id'])
 assert got['status']=='closed'
 settle=[f for f in got['fills'] if f['basis']=='expiry_settlement']
 assert settle and all(abs(f['price']-max(0.0,23300.0-next(l for l in got['leg_meta'] if l['id']==f['leg_id'])['strike']))<1e-9 for f in settle)
 assert all(not p['units'] for p in got['positions'])


# --- D multi-expiry -------------------------------------------------------------------------------------------------------
def test_a_calendar_spread_is_valued_at_the_near_expiry_with_the_far_leg_by_model():
 from datetime import datetime
 from kanida_pilot.strategy_builder import analytics as A
 from test_strategy_builder import leg,SPOT
 at=datetime(2026,9,23,15,30)
 near=leg('CE',23000,'S',120,expiry=EXP,iv=0.12);far=leg('CE',23000,'B',260,expiry='2026-10-27',id='F',iv=0.13)
 a=A.analyze([near,far],SPOT,at)
 assert a['max_loss']['basis']=='model_at_near_expiry' and not a['max_loss'].get('unlimited') and not a['max_profit'].get('unlimited')
 assert a['max_loss']['value']<0<a['max_profit']['value']
 # the most it can lose is about the net debit paid (the far call keeps time value): never worse than the debit
 debit=65*(260-120)
 assert a['max_loss']['value']>=-debit-1
 best=max(a['curve'],key=lambda p:p['expiry'] or -1e18)
 assert abs(best['s']-23000)<=23000*0.01                              # a calendar peaks at the shared strike
 assert len(a['breakevens']['value'])==2 and a['pop']['status']=='available'
 g=a['greeks']
 assert g['status']=='available' and abs(g['delta'])<65*0.3            # roughly delta-neutral at the strike
 assert any('Multi-expiry' in w for w in a['warnings'])


def test_a_diagonal_with_an_uncovered_far_short_call_is_unlimited():
 from datetime import datetime
 from kanida_pilot.strategy_builder import analytics as A
 from test_strategy_builder import leg,SPOT
 a=A.analyze([leg('CE',23000,'B',150,expiry=EXP,iv=0.12),leg('CE',23100,'S',200,expiry='2026-10-27',id='F',lots=2,iv=0.13)],SPOT,datetime(2026,9,23,15,30))
 assert a['max_loss'].get('unlimited') is True


def test_calendar_and_diagonal_templates_resolve_across_two_expiries_and_are_recognised():
 import tempfile,os
 from test_strategy_builder import store as mkstore
 from kanida_pilot.strategy_builder.market import Market
 from kanida_pilot.strategy_builder.templates import resolve,recognise,TEMPLATES
 d=tempfile.mkdtemp();m=Market(mkstore(os.path.join(d,'d.db')));ch=m.chain('NIFTY',EXP)
 far={**ch,'expiry':'2026-10-27','rows':[{**r} for r in ch['rows']]}
 for t in [x for x in TEMPLATES if x.get('multi_expiry')]:
  for v in ((t['param'] or {}).get('variants') or [None]):
   got=resolve(t['key'],ch,v,1,far)
   assert sorted({l['expiry'] for l in got['legs']})==[EXP,'2026-10-27']
   assert recognise(got['legs'])['key']==t['key'],(t['key'],v)
 m.close()


def test_a_calendar_template_without_a_later_expiry_says_so(pilot):
 _a,owner,_o=pilot
 r=owner.post('/api/sb/templates/resolve',json={'template':'long_call_calendar','underlying':'NIFTY','expiry':EXP})
 assert r.status_code==400 and r.json()['code']=='NO_FAR_EXPIRY'
 assert owner.post('/api/sb/lab/backtests',json={'template':'long_call_calendar'}).status_code==400


def test_roll_out_closes_the_near_legs_and_opens_the_same_strikes_later():
 from kanida_pilot.strategy_builder import adjust as ADJ
 legs=[{'id':'a','type':'CE','side':'B','strike':23000.0,'lots':1,'expiry':EXP,'include':True},{'id':'b','type':'CE','side':'S','strike':23200.0,'lots':1,'expiry':EXP,'include':True}]
 new,note=ADJ.apply('roll_out',legs,23000,[22900.0,23000.0,23200.0],to_expiry='2026-10-27')
 assert [l['expiry'] for l in new]==['2026-10-27']*2 and 'reopen' in note
 orders=ADJ.delta_orders(legs,new)
 assert sorted((o['expiry'],o['side'],o['strike']) for o in orders)==sorted([(EXP,'S',23000.0),(EXP,'B',23200.0),('2026-10-27','B',23000.0),('2026-10-27','S',23200.0)])
 with pytest.raises(ADJ.NotApplicable):ADJ.apply('roll_out',legs,23000,[],to_expiry=None)


def test_roll_out_without_a_later_expiry_is_explained_and_never_lab_tested(pilot):
 _a,owner,_o=pilot
 s=strategy(owner)
 c=owner.post(f"/api/sb/strategies/{s['id']}/adjust/candidates",json={}).json()
 ro=next(x for x in c['candidates'] if x['rule']=='roll_out')
 assert not ro['available'] and 'no later expiry' in ro['reason']
 assert owner.post('/api/sb/lab/backtests',json={'template':'iron_condor','adjust':{'rule':'roll_out'}}).status_code==400


# --- C two-user isolation: every /api/sb route with an id refuses another user's resource ----------------------------------
def test_no_strategy_builder_route_lets_another_user_touch_an_owners_resource(live):
 app,owner,other,fake=live
 s=strategy(owner)
 snap=owner.post(f"/api/sb/strategies/{s['id']}/snapshots",json={}).json()
 p=owner.post(f"/api/sb/strategies/{s['id']}/preview",json={}).json()
 d=owner.post(f"/api/sb/strategies/{s['id']}/deployments",json={'preview_id':p['id'],'preview_hash':p['hash'],'idempotency_key':'iso','confirm':True}).json()
 rule=owner.post(f"/api/sb/strategies/{s['id']}/alerts",json={'type':'price_cross','params':{'level':23100,'direction':'above'}}).json()
 ids={'sid':s['id'],'rid':snap['id'],'did':d['id'],'run':'nope'}
 alert_rid=rule['id']
 checked=[]
 def walk(rs):
  for r in rs:
   if getattr(r,'original_router',None) is not None:yield from walk(r.original_router.routes)
   else:yield r
 seen=set()
 for r in walk(app.router.routes):
  path=getattr(r,'path','') or '';methods=getattr(r,'methods',set()) or set()
  if not path.startswith('/api/sb/') or '{' not in path:continue
  for m in sorted(methods-{'HEAD','OPTIONS'}):
   if (m,path) in seen:continue
   seen.add((m,path))
   url=path.replace('{sid}',ids['sid']).replace('{did}',ids['did']).replace('{bid}','nope')
   if '/alerts/{rid}' in path:url=url.replace('{rid}',alert_rid)
   elif '/lab/runs/{rid}' in path:url=url.replace('{rid}',ids['run'])
   elif '/autotrade/routes/{rid}' in path:url=url.replace('{rid}','nope')
   else:url=url.replace('{rid}',ids['rid'])
   body={'version':1,'expected_version':1,'confirm':True,'body':s['draft']['body'],'revision_id':ids['rid'],'name':'x','notes':'x','target_pct':10}
   resp=other.request(m,url,json=body) if m!='GET' else other.get(url)
   checked.append((m,path,resp.status_code))
   assert resp.status_code in (400,403,404,409,422,503),(m,path,resp.status_code,resp.text[:200])
   if resp.status_code<300:raise AssertionError((m,path,'leaked'))
 assert len(checked)>=30,len(checked)            # unique (method, route) pairs
 # and the owner's resources are unchanged by all of that
 assert owner.get(f"/api/sb/strategies/{s['id']}").json()['draft']['version']==s['draft']['version']
 assert owner.get(f"/api/sb/revisions/{snap['id']}").json()['name']==snap['name']


def test_backup_restore_drill_round_trips_every_table(pilot,tmp_path):
 import importlib.util,pathlib
 app,owner,_o=pilot
 strategy(owner);st=app.state.strategy_builder_store
 spec=importlib.util.spec_from_file_location('sb_backup',str(pathlib.Path(__file__).resolve().parents[2]/'scripts'/'sb_backup.py'))
 B=importlib.util.module_from_spec(spec);spec.loader.exec_module(B)
 db=st.c.execute('pragma database_list').fetchone()[2]
 d=B.drill(db,tmp_path/'bk')
 assert d['integrity']=='ok' and d['match'] and d['counts']['strategies']>=1
 with pytest.raises(SystemExit):B.restore(d['backup'],d['restored'])        # never overwrites


# --- A2/A3 verified calendar + point-in-time membership (synthetic bhavcopy) --------------------------------------------
def _bhav(path,rows,ok_days):
 import sqlite3
 c=sqlite3.connect(path)
 c.execute('create table fo_daily(trade_date text,symbol text,instrument text,expiry text,strike real,option_type text,open real,high real,low real,close real,settle real,contracts integer,oi integer,oi_change integer,lot_size integer,source_format text)')
 c.execute('create table fetch_log(trade_date text primary key,url text,status text,http_status integer,rows integer,fetched_at text,note text)')
 for d in ok_days:c.execute("insert into fetch_log values(?,?,?,?,?,?,?)",(d,'x','ok',200,1,'t',None))
 for d,sym,ins,e in rows:c.execute('insert into fo_daily(trade_date,symbol,instrument,expiry,strike,option_type) values(?,?,?,?,?,?)',(d,sym,ins,e,100.0,'CE'))
 c.commit();c.close()


def test_the_lab_takes_expiries_from_the_listing_and_trades_a_stock_only_while_it_had_options(tmp_path):
 from kanida_pilot.strategy_builder.fo_calendar import FoCalendar
 from test_strategy_builder import lab_spec
 nifty,vix=daily_series(start='2021-01-04',n=120)
 days=nifty['days']
 # the index lists ONLY an odd Wednesday expiry 5 days out from every day (a derived Thursday rule would never find it)
 from datetime import date,timedelta
 def wed_after(d):
  x=date.fromisoformat(d)+timedelta(days=1)
  while x.weekday()!=2:x+=timedelta(days=1)
  return x.isoformat()
 rows=[(d,'NIFTY','OPTIDX',wed_after(d)) for d in days]
 # a stock with options only from the 60th day onwards
 rows+=[(d,'NEWCO','OPTSTK',wed_after(d)) for d in days[60:]]
 p=str(tmp_path/'b.db');_bhav(p,rows,days)
 cal=FoCalendar(p)
 spec=lab_spec(template='bull_call_spread',param=4,weekday=0,dte_min=1,dte_max=10,**{'from':days[0],'to':days[-1],'split':days[60]})
 tr,sk=LB.simulate(spec,nifty,vix,65,cal=cal.lookup('NIFTY'))
 assert tr and all(t['expiry_source']=='verified' and date.fromisoformat(t['expiry']).weekday()==2 for t in tr)
 st={**spec,'underlying':'NEWCO','exit_dte':2,'slippage':0.01}
 tr2,sk2=LB.simulate(st,nifty,vix,100,cal=cal.lookup('NEWCO'))
 assert sk2.get('not_in_fo',0)>0 and all(t['entry']>=days[60] for t in tr2)


def test_banknifty_needs_the_verified_calendar():
 from test_strategy_builder import lab_spec
 nifty,vix=daily_series(start='2021-01-04',n=80)
 with pytest.raises(LB.LabError):LB.simulate(lab_spec(underlying='BANKNIFTY'),nifty,vix,35)


# --- slice-12 audit follow-ups ---------------------------------------------------------------------------------------------
def test_expiry_settlement_is_per_leg_and_needs_a_closing_reading(live,monkeypatch):
 app,owner,_o,fake=live
 s,d=_deploy(owner,fake,key='pl1')
 ex=app.state.strategy_builder_execution
 from kanida_pilot.strategy_builder import execution as EXM
 import datetime as _dt
 # make ONE leg a later-expiry leg (a calendar's far leg): only the near leg may settle
 far=d['revision_id'];rb=app.state.strategy_builder_store
 row=rb.c.execute('select body from revisions where id=?',(far,)).fetchone()[0];body=json.loads(row)
 body['legs'][1]['expiry']='2026-10-27';rb.c.execute('update revisions set body=? where id=?',(json.dumps(body),far));rb.c.commit()
 monkeypatch.setattr(EXM,'now_ist',lambda:_dt.datetime.strptime(EXP,'%Y-%m-%d')+_dt.timedelta(days=1,hours=10))
 monkeypatch.setattr(ex,'_expiry_spot',lambda u,e:(f'{e} 15:29:00',23300.0) if e==EXP else None)
 got=ex.deployment(d['user_id'],d['id'])
 settled={f['leg_id'] for f in got['fills'] if f['basis']=='expiry_settlement'}
 assert settled=={body['legs'][0]['id']} and got['status']!='closed'
 again=ex.deployment(d['user_id'],d['id'])                             # a second read never settles twice
 assert len([f for f in again['fills'] if f['basis']=='expiry_settlement'])==1


def test_expiry_spot_must_be_from_the_closing_window(live):
 app,_o,_x,_f=live
 ex=app.state.strategy_builder_execution
 assert ex._expiry_spot('NIFTY','2020-01-30') is None                  # the stored reading is not from that day's close


def test_roll_out_refuses_a_calendar_and_exit_rules_refuse_what_can_never_fire(live):
 from kanida_pilot.strategy_builder import adjust as ADJ
 cal=[{'id':'n','type':'CE','side':'S','strike':23000.0,'lots':1,'expiry':EXP,'include':True},{'id':'f','type':'CE','side':'B','strike':23000.0,'lots':1,'expiry':'2026-10-27','include':True}]
 with pytest.raises(ADJ.NotApplicable):ADJ.apply('roll_out',cal,23000,[23000.0],to_expiry='2026-11-24')
 app,owner,_o,fake=live
 s,d=_deploy(owner,fake,template='long_call',key='er2')
 r=owner.post(f"/api/sb/deployments/{d['id']}/exit-rules",json={'target_pct':50})
 assert r.status_code==409 and r.json()['code']=='TARGET_UNDEFINED'


def test_a_diagonal_max_loss_includes_the_far_spot_limit():
 from datetime import datetime
 from kanida_pilot.strategy_builder import analytics as A
 from test_strategy_builder import leg,SPOT
 legs=[leg('CE',23000,'S',150,expiry=EXP,iv=0.12),leg('CE',30000,'B',60,expiry='2027-09-28',id='F',iv=0.25)]
 a=A.analyze(legs,SPOT,datetime(2026,9,23,15,30))
 import math
 tau=(A.expiry_moment('2027-09-28')-A.expiry_moment(EXP)).total_seconds()/A.SECONDS_PER_YEAR
 limit=65*(150-60)+65*(-(30000*math.exp(-A.RATE*tau))+23000)          # S->inf: -(S-23000) + (S - 30000 e^-rt)
 assert a['max_loss']['value']<=limit+1


def test_a_point_in_time_rule_can_test_a_stock_that_left_fno(pilot,tmp_path,monkeypatch):
 p=str(tmp_path/'b.db');days=['2021-01-04','2021-01-05']
 _bhav(p,[(d,'LEAVERCO','OPTSTK','2021-01-28') for d in days],days)
 import sqlite3;c=sqlite3.connect(p);c.execute("update fo_daily set lot_size=1200");c.commit();c.close()
 monkeypatch.setenv('PILOT_FO_BHAVCOPY_DB',p)
 app,_o,_x=pilot
 lab=app.state.strategy_builder_lab
 from kanida_pilot.strategy_builder.fo_calendar import FoCalendar
 lab.cal=FoCalendar(p)
 with pytest.raises(LB.LabError):lab.validate({'underlying':'LEAVERCO','template':'long_call'})
 sp=lab.validate({'underlying':'LEAVERCO','template':'long_call','pit':True,'exit_dte':2,'dte_min':5,'dte_max':30})
 assert sp['pit'] and lab.lot_size('LEAVERCO',with_source=True)[0]==1200


def test_a_renamed_stock_keeps_its_archive_history_under_the_old_name(tmp_path):
 from kanida_pilot.strategy_builder.fo_calendar import FoCalendar
 p=str(tmp_path/'b.db');_bhav(p,[('2022-01-03','MCDOWELL-N','OPTSTK','2022-01-27'),('2023-06-01','UNITDSPR','OPTSTK','2023-06-29')],['2022-01-03','2023-06-01'])
 ch=tmp_path/'ch.csv';ch.write_text('SM_NAME,SM_KEY_SYMBOL,SM_NEW_SYMBOL,SM_APPLICABLE_FROM\nUnited Spirits,MCDOWELL-N,UNITDSPR,01-Jun-2023\n')
 cal=FoCalendar(p,changes=str(ch))
 assert set(cal.listed('UNITDSPR'))=={'2022-01-03','2023-06-01'} and 'UNITDSPR' in cal.ever_members('2022-01-01','2023-12-31')
 assert set(FoCalendar(p,changes=str(tmp_path/'none.csv')).listed('UNITDSPR'))=={'2023-06-01'}   # without the list: fewer days, never invented


@pytest.mark.parametrize('seed',range(8))
def test_no_stock_decision_depends_on_later_data_through_the_whole_pipeline(seed):
 import random
 from test_strategy_builder import lab_spec
 rnd=random.Random(100+seed)
 nifty,vix=daily_series(start='2021-01-04',n=420)
 stock={**nifty,'close':[c*1.7 for c in nifty['close']],'open':[o*1.7 for o in nifty['open']],'high':[h*1.7 for h in nifty['high']],'low':[l*1.7 for l in nifty['low']]}
 def pipe(s):
  g=LB.guard_series(s);return g,LB.scaled_vol(g,nifty,vix)
 spec={**lab_spec(template=rnd.choice(['bull_call_spread','iron_condor','bear_put_spread']),weekday=rnd.choice([1,2,3]),dte_min=5,dte_max=30,
  **{'from':nifty['days'][40],'to':nifty['days'][-1],'split':nifty['days'][250]}),'underlying':'TESTCO','exit_dte':2,'slippage':0.01,
  'target_pct':rnd.choice([None,50]),'stop_pct':rnd.choice([None,60])}
 spec['param']=LB.BY_KEY[spec['template']]['param']['default'] if LB.BY_KEY[spec['template']]['param'] else None
 s0,v0=pipe(stock);base,_=LB.simulate(spec,s0,v0,100)
 if not base:pytest.skip('no trade')
 cut=base[len(base)//2]['exit']
 later={**stock}
 for k in ('open','high','low','close'):later[k]=[x*(1+rnd.uniform(-.6,.6)) if d>cut else x for d,x in zip(stock['days'],stock[k])]   # includes >40% "breaks"
 s1,v1=pipe(later);again,_=LB.simulate(spec,s1,v1,100)
 key=lambda t:(t['decision'],t['entry'],t['exit'],t['net'],tuple((l['strike'],l['entry']) for l in t['legs']))
 assert [key(t) for t in again if t['exit']<=cut]==[key(t) for t in base if t['exit']<=cut]
