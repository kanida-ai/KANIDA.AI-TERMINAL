"""Quant audit A1/A2/A6/A7: the tradable-evidence gate holds at simulate and live submission, not only at save."""
from datetime import date
import pytest
from sqlalchemy import select
import kanida_pilot.evidence as evidence_module
import kanida_pilot.live as live_module
from kanida_pilot.config import Settings
from kanida_pilot.db import plans,orders,users,wallets,now,row,ident
from kanida_pilot.errors import PilotError
from kanida_pilot.evidence import Evidence,tradability
from test_pilot import app,signup,plan,FIXTURE_RULE  # noqa: F401  (shared fixture)
from test_exit_evidence import MATCH,chart,cell

TODAY=date(2026,9,13)
LEGACY=dict(symbol='OLD',pattern_name='Cup & Handle',timeframe='1D',side='long',quantity=1,notional=100,reserved_cost=.4,
 planned_risk=3,snapshot_price=100,stop_pct=2,reward=2,exit_mode='suggested',evidence_applies=False)

def uid_of(c):return c.get('/api/auth/me').json()['user']['id']
def user_of(app,c):
 uid=uid_of(c)  # HTTP call first: never while holding the SQLite write lock
 with app.state.db.tx() as conn:return row(conn,select(users).where(users.c.id==uid))
def simulate(c,plan_id,request='gate-simulation-001'):
 return c.post('/api/trading/simulate',json=dict(plan_id=plan_id,request_id=request,scenario='target',acknowledge_synthetic=True))
def insert_plan(app,c,payload,status='draft'):
 identity=ident();uid=uid_of(c)
 with app.state.db.tx() as conn:
  conn.execute(plans.insert().values(id=identity,user_id=uid,request_id='req-'+identity,payload_hash='x',payload=dict(payload,id=identity),status=status,created=now(),updated=now()))
 return identity
def arm_live(app,monkeypatch):
 # Test-only unlock so the refusals behind the release lock are reachable. The broker must never be called.
 monkeypatch.setattr(live_module,'LIVE_RELEASE_APPROVED',True);s=app.state.live.settings;s.live_enabled=True;s.live_activation='fixture-only'
 calls=[]
 def broker(*args):calls.append(args);raise AssertionError('broker must not be reached')
 monkeypatch.setattr(app.state.live.kite,'call',broker)
 return calls
def refused_live(app,c,plan_id,request='gate-live-000001'):
 user=user_of(app,c)
 verified=dict(user_id=user['id'],side='long',holding='overnight',instrument=dict(instrument_type='EQ',exchange='NSE',tradingsymbol='TEST',lot_size=1,tick_size=.05),
  quote=dict(last_price=100,received_at=now()),max_notional=10000,market_open=True,protective_exit_ready=True,margin_verified=True,cost_reserve_paise=100,capital_limit_paise=1000000)
 with pytest.raises(PilotError) as error:app.state.live.submit(user,dict(plan_id=plan_id,quantity=10,limit_price=100,request_id=request),verified)
 with app.state.db.tx() as conn:assert not conn.execute(select(orders.c.id).where(orders.c.mode=='live')).first()
 return error.value
def refetch(monkeypatch,app,**change):
 ev=app.state.evidence;original=ev.exit_plan
 monkeypatch.setattr(ev,'exit_plan',lambda identity,side,run=None,snapshot=None:dict(original(identity,side),**change))

# A1 -------------------------------------------------------------------------------------------------
def test_legacy_draft_without_evidence_cannot_simulate_or_go_live(app,monkeypatch):
 c=signup(app);identity=insert_plan(app,c,LEGACY)
 r=simulate(c,identity)
 assert r.status_code==409 and r.json()['code']=='EXIT_EVIDENCE' and 'prepare it again' in r.json()['error'].lower()
 calls=arm_live(app,monkeypatch);error=refused_live(app,c,identity)
 assert (error.status,error.code)==(409,'EXIT_EVIDENCE') and calls==[]
 forged=insert_plan(app,c,dict(LEGACY,tradable_evidence=False,tradable_reason='Illustrative benchmark only: fixture.'))
 assert simulate(c,forged,'gate-simulation-002').json()['code']=='EXIT_EVIDENCE'
 # Still readable and archivable; nothing reserved, no order recorded.
 assert any(p['id']==identity and p['status']=='illustrative' for p in c.get('/api/product').json()['plans'])
 assert c.post('/api/product/plan-action',json=dict(id=identity,action='archive')).json()['status']=='archived'
 trading=c.get('/api/trading').json();assert trading['wallet']['reserved_paise']==0 and not trading['orders']

def test_resume_of_non_tradable_plan_is_illustrative(app):
 c=signup(app);identity=insert_plan(app,c,LEGACY)
 act=lambda i,a:c.post('/api/product/plan-action',json=dict(id=i,action=a)).json()
 assert act(identity,'pause')['status']=='paused' and act(identity,'resume')['status']=='illustrative'
 with app.state.db.tx() as conn:assert conn.execute(select(plans.c.status).where(plans.c.id==identity)).scalar()=='illustrative'
 # A legacy plan on the frozen tested rule may resume to draft, but submission still re-verifies it,
 # and this one never recorded its rule and candle, so it cannot be verified.
 tested=insert_plan(app,c,dict(LEGACY,evidence_applies=True))
 assert act(tested,'pause')['status']=='paused' and act(tested,'resume')['status']=='draft'
 assert simulate(c,tested).json()['code']=='EXIT_EVIDENCE_CHANGED'

# A2: re-fetch ---------------------------------------------------------------------------------------
def test_saved_tradable_plan_refused_when_refetched_evidence_fails(app,monkeypatch):
 c=signup(app);p=plan(c)
 refetch(monkeypatch,app,tradable_evidence=False,tradable_reason='Only 12 later-test trades for this exact rule; 20 are required.')
 r=simulate(c,p['id']);assert r.status_code==409 and r.json()['code']=='EXIT_EVIDENCE_CHANGED' and 'Only 12' in r.json()['error']
 calls=arm_live(app,monkeypatch);error=refused_live(app,c,p['id'])
 assert (error.status,error.code)==(409,'EXIT_EVIDENCE_CHANGED') and calls==[]
 assert c.get('/api/trading').json()['wallet']['reserved_paise']==0

@pytest.mark.parametrize('change,allowed',[
 (dict(rule=dict(FIXTURE_RULE,hold=6)),False),(dict(rule=dict(FIXTURE_RULE,stop_atr=1.5)),False),
 (dict(rule=dict(FIXTURE_RULE,target_r=3)),False),(dict(rule=dict(FIXTURE_RULE,trigger='setup')),False),
 (dict(snapshot='2026-08-03'),False),
 (dict(run='r2'),True)])  # a new research run is accepted only because the exact saved rule still passes
def test_refetch_is_compared_with_the_saved_rule(app,monkeypatch,change,allowed):
 c=signup(app);p=plan(c);refetch(monkeypatch,app,**change)
 r=simulate(c,p['id'])
 if allowed:assert r.status_code==200,r.text;assert r.json()['payload']['evidence_verified']['run_changed'] is True
 else:assert r.status_code==409 and r.json()['code']=='EXIT_EVIDENCE_CHANGED',r.text

def test_setup_missing_from_research_is_refused(app,monkeypatch):
 c=signup(app);p=plan(c)
 def gone(*args,**kwargs):raise PilotError(409,'SETUP_CHANGED','This setup is no longer in the current research snapshot.')
 monkeypatch.setattr(app.state.evidence,'exit_plan',gone)
 r=simulate(c,p['id']);assert r.json()['code']=='EXIT_EVIDENCE_CHANGED' and 'no longer' in r.json()['error']

# A2: stale data -------------------------------------------------------------------------------------
def stored_data(monkeypatch,app,latest='2026-07-31',flag=False):
 monkeypatch.setattr(evidence_module,'market_today',lambda:TODAY)
 monkeypatch.setattr(app.state.evidence,'get',lambda path,params=None:{'source_stale':flag,'source_latest':latest})

def test_stale_data_is_refused_at_simulate_and_live(app,monkeypatch):
 c=signup(app);p=plan(c);stored_data(monkeypatch,app)
 r=simulate(c,p['id']);body=r.json()
 assert r.status_code==409 and body['code']=='DATA_STALE'
 assert 'ends 31 Jul 2026, 44 days ago' in body['error'] and 'no older than 3 days' in body['error']
 calls=arm_live(app,monkeypatch);error=refused_live(app,c,p['id'])
 assert (error.status,error.code)==(409,'DATA_STALE') and calls==[]
 assert c.get('/api/trading').json()['wallet']['reserved_paise']==0

@pytest.mark.parametrize('latest,flag,limit,code',[
 ('2026-09-10',False,3,None),('2026-09-09',False,3,'DATA_STALE'),('2026-09-12 15:30',True,3,'DATA_STALE'),
 ('',False,3,'DATA_STALE'),('2026-07-31',False,60,None)])
def test_data_age_limit_is_enforced_by_server_setting(app,monkeypatch,latest,flag,limit,code):
 c=signup(app);p=plan(c);stored_data(monkeypatch,app,latest,flag);app.state.simulation.settings.max_data_age_days=limit
 r=simulate(c,p['id'])
 assert (r.status_code==200 if code is None else r.json()['code']==code),r.text

def test_max_data_age_setting_loads_from_environment(monkeypatch):
 import kanida_pilot.config as config
 from cryptography.fernet import Fernet
 monkeypatch.setattr(config,'load_dotenv',lambda *args,**kwargs:None)
 for key,value in dict(PILOT_ENVIRONMENT='local',PILOT_ORIGIN='http://127.0.0.1:8082',PILOT_ENCRYPTION_KEY=Fernet.generate_key().decode(),
                       PILOT_DATABASE_URL='sqlite://',PILOT_MAX_DATA_AGE_DAYS='5').items():monkeypatch.setenv(key,value)
 assert Settings.load().max_data_age_days==5 and Settings().max_data_age_days==3
 monkeypatch.setenv('PILOT_MAX_DATA_AGE_DAYS','soon')
 with pytest.raises(ValueError):Settings.load()

# A6 -------------------------------------------------------------------------------------------------
def described(direction,side='long'):
 ev=Evidence(Settings(),None)
 ev.match=lambda identity:dict(MATCH,direction=direction,history=[dict(side=side,run='r1')])
 ev.get=lambda path,params=None:dict(cell(dict(trigger='confirmed',hold=10,stop_atr=2,target_r=2)),side=side) if path=='/api/backtests/cell' else chart()
 return ev.exit_plan('m1',side)

@pytest.mark.parametrize('direction,side',[('neutral','long'),('neutral','short'),('bearish','long')])
def test_neutral_or_mismatched_direction_is_not_tradable(direction,side):
 value=described(direction,side)
 assert value['evidence_applies'] is True and value['tradable_evidence'] is False and value['evidence_basis']=='illustrative_benchmark'
 if direction=='neutral':assert value['screen']=='watch' and value['tradable_reason'].startswith('Neutral pattern')

def test_non_review_screen_is_not_tradable():
 rule=dict(kind='atr',trigger='confirmed',hold=10,stop_atr=2,target_r=2)
 base=dict(status='supported',evidence_applies=True,usable=True,rule=rule,side='long',screen='review',
  evidence=dict(selected_rule={k:v for k,v in rule.items() if k!='kind'},minimum_later_trades=20),rule_metrics=dict(n=24,expectancy_pct=.5))
 assert tradability(base,'bullish')[0] is True
 for screen in ('watch','pass',None):
  ok,reason=tradability(dict(base,screen=screen),'bullish');assert not ok and 'For review' in reason
 assert tradability(base)[0] is False  # unknown pattern direction fails closed

# A7 -------------------------------------------------------------------------------------------------
def ledger(app,c,*outcomes):
 uid=uid_of(c)
 with app.state.db.tx() as conn:
  for i,value in enumerate(outcomes):
   conn.execute(orders.insert().values(id=ident(),user_id=uid,plan_id=None,request_id=f'ledger-fixture-{i:04d}',request_hash='x',mode='simulation',symbol='TEST',
    status='closed',product='CNC',payload=dict(scenario='fixture'),filled_qty=1,average_paise=0,realized_paise=value,created=now(),updated=now()))
  conn.execute(wallets.update().where(wallets.c.user_id==uid).values(realized_paise=sum(outcomes),cash_paise=10000000+sum(outcomes)))

def test_loss_limit_counts_losses_and_ignores_gains(app):
 c=signup(app);p=plan(c)
 ledger(app,c,600000,-150000,-100000)  # +6% chosen-scenario gains, -2.5% losses: net positive, losses beyond 2%
 r=simulate(c,p['id']);assert r.status_code==409 and r.json()['code']=='LOSS_LIMIT' and 'do not offset' in r.json()['error']
 w=c.get('/api/trading').json()['wallet']
 assert (w['realized_paise'],w['realized_loss_paise'],w['loss_limit_paise'],w['loss_limit_reached'])==(350000,250000,200000,True)
 assert w['available_capital_paise']==w['initial_paise'] and w['scenario_outcome_total_paise']==350000

def test_losses_up_to_the_limit_still_allow_entries(app):
 c=signup(app);p=plan(c);ledger(app,c,-200000)  # exactly 2% is not beyond the limit
 r=simulate(c,p['id']);assert r.status_code==200,r.text
 assert r.json()['payload']['evidence_verified']['basis']=='exact_rule_later_test'
