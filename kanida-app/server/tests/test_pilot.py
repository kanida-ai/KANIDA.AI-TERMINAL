import hashlib,hmac,json
from contextlib import contextmanager
from pathlib import Path
import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient
from sqlalchemy import select
from kanida_pilot.config import Settings
from kanida_pilot.app import create_app
from kanida_pilot.db import users,oauth,orders,subscriptions,now,row
from kanida_pilot.auth import digest
from kanida_pilot.kite import next_expiry
from kanida_pilot.errors import PilotError
from kanida_pilot.evidence import market_today

FIXTURE_RULE=dict(kind='atr',trigger='confirmed',hold=10,stop_atr=2,target_r=2)
class Evidence:
 def study(self,path,params=None,body=None):return {'path':path,'params':params,'body':body}
 # Fresh stored data by default (server now refuses stale data); stale refusal is tested in test_evidence_gate.py.
 def get(self,path,params=None,cache=False):return {'source_stale':False,'source_latest':market_today().isoformat()}
 # The DISPLAY match set (chart workspace, legend, watchlist). The real Evidence passes the legacy list
 # through untouched and unwraps research detections; the fixture mirrors the legacy half.
 def display_matches(self,params=None):return []
 def display_match(self,identity):return self.match(identity)
 def match(self,identity):return dict(id=identity,symbol='TEST',pattern='cup_handle',pattern_name='Cup & Handle',timeframe='1D',direction='bullish',candle_end='2026-07-31')
 # Server-side re-fetch used at simulate/live submission; mirrors the suggestion embedded by prepare().
 def exit_plan(self,identity,side,run=None,snapshot=None):
  return dict(match_id=identity,side=side,run='r1',snapshot='2026-07-31',status='supported',screen='review',usable=True,rule=dict(FIXTURE_RULE),
   tradable_evidence=True,tradable_reason='Fixture exact tested rule.',evidence_basis='exact_rule_later_test')
 def prepare(self,data):
  return dict(self.match(data['match_id']),match_id=data['match_id'],side='long',snapshot_price=100,snapshot_end='2026-07-31',notional=data.get('notional',10000),reserved_cost=40,planned_risk=290,
   quantity=100,stop_pct=2.5,reward=2,hold=10,history={},created='2026-09-12',status='draft',
   exit_mode='suggested',exit_rule=dict(FIXTURE_RULE),exit_evidence=self.exit_plan(data['match_id'],'long'))

def pilot_settings(tmp_path,**extra):
 """Base settings for a throwaway pilot. The expanded pattern research is pointed at an empty directory so the
 shared fixtures never read the real (multi-gigabyte, still-running) research tree or write the real index;
 tests that need it build their own fixture research databases (see test_research_cards.py)."""
 return Settings(**{**dict(origin='http://testserver',origins=['http://testserver'],
  database_url='sqlite:///'+str(tmp_path/'test.sqlite'),encryption_key=Fernet.generate_key().decode(),
  web_directory=str(tmp_path),owner_email='owner@example.invalid',
  pattern_research_directory=str(tmp_path/'no-research'),pattern_catalogue_path=str(tmp_path/'no-catalogue.json'),
  pattern_index_path=str(tmp_path/'research_index.sqlite3'),evidence_release_path=str(tmp_path/'evidence_release.json'),
  # No live detection ledger by default: the real one belongs to the scanner and must never be read by a test.
  # Tests that need one build their own (see test_live_detections.py).
  pattern_detection_db=str(tmp_path/'no-scan-cache.sqlite3'),pattern_detection_cache_seconds=0,
  # Same rule for the F&O store: the real db/derivatives.db belongs to the capture worker and is never read
  # by a test. test_derivatives.py builds its own (docs/DERIVATIVES_SPEC.md §2).
  derivatives_database=str(tmp_path/'no-derivatives.db')),
  **extra})

@pytest.fixture
def app(tmp_path):
 value=create_app(pilot_settings(tmp_path),evidence=Evidence());yield value;value.state.db.close()

def signup(app,email='owner@example.invalid',role='owner'):
 client=TestClient(app);invite=app.state.auth.invite(email,role)
 result=client.post('/api/auth/register',json={'email':email,'password':'fixture-only-password-2026','name':'Test account','invite':invite,'policy_version':'private-pilot-v1'})
 assert result.status_code==200,result.text
 client.headers.update({'X-Kanida-CSRF':result.json()['csrf'],'Origin':'http://testserver'})
 r=client.post('/api/account/onboarding',json={'acknowledge_pilot':True,'policy_version':'private-pilot-v1','timeframes':['1D']})
 assert r.status_code==200,r.text
 return client

def plan(client,request='fixture-plan-0001',**extra):
 response=client.post('/api/product/plans',json={'match_id':'test','request_id':request,**extra})
 assert response.status_code==200,response.text
 return response.json()


def test_study_routes_bind_owner_and_require_csrf(app):
 c=signup(app)
 owner=c.get('/api/studies').json()['params']['owner']
 assert owner and owner!='other-owner'
 r=c.post('/api/studies',json={'owner':'other-owner','symbols':['TEST']})
 assert r.status_code==200 and r.json()['body']['owner']==owner
 assert c.get('/api/studies/example/chart?owner=other-owner&id=wrong&trade=1').json()['params']=={'owner':owner,'id':'example','trade':'1'}
 c.headers.pop('X-Kanida-CSRF')
 assert c.post('/api/studies',json={}).status_code==403
 assert c.post('/api/studies/example/cancel',json={}).status_code==403


def test_simulation_requires_subscription_entitlement(app):
 c=signup(app,'research@example.invalid','member')
 assert c.get('/api/studies').status_code==402
 assert c.get('/api/replay').status_code==402


def test_plan_retains_owner_scoped_study_without_changing_exits(app,monkeypatch):
 c=signup(app);owner=c.get('/api/studies').json()['params']['owner'];calls=[]
 def study(path,params=None,body=None):
  calls.append(params)
  return dict(status='complete',settings={'capital':10000},result=dict(run='frozen',engine_version='2',validation='Fixed-rule historical backtest',
   trades=[dict(id='1',symbol='TEST',pattern='cup_handle',timeframe='1D',side='long',rule={'hold':20})]))
 monkeypatch.setattr(app.state.evidence,'study',study)
 value=plan(c,side='long',study_context={'id':'saved-study','trade_id':'1','owner':'spoof'})
 assert calls==[{'owner':owner,'id':'saved-study','result':'true'}]
 assert value['research_context']['rule']=={'hold':20}
 assert value['hold']==10 and value['status']=='draft'
 assert c.post('/api/product/plans',json=dict(match_id='test',side='short',request_id='wrong-direction-study',study_context={'id':'saved-study','trade_id':'1'})).status_code==409

def test_private_routes_and_invitation(app):
 c=TestClient(app)
 for endpoint in ('/api/state','/api/matches','/api/product','/api/account','/api/trading','/api/studies','/api/studies/example','/api/studies/example/chart','/api/replay','/api/replay/chart'):
  assert c.get(endpoint).status_code==401
 assert c.get('/api/pilot/config').json()['invitation_required']
 r=c.post('/api/auth/register',json={'email':'bad@example.invalid','password':'fixture-password-1234','invite':'bad','policy_version':'private-pilot-v1'})
 assert r.status_code==403

def test_csrf_and_revocation(app):
 c=signup(app);assert c.get('/api/auth/me').json()['user']['onboarded']
 c.headers['X-Kanida-CSRF']='wrong'
 assert c.post('/api/product/watch',json={'action':'add','match_id':'test'}).status_code==403
 c.headers['X-Kanida-CSRF']=c.get('/api/auth/me').json()['csrf']
 assert c.post('/api/auth/logout',json={'all_devices':True}).status_code==200
 assert c.get('/api/product').status_code==401

def test_origin_and_json_required(app):
 c=TestClient(app)
 assert c.post('/api/auth/login',json={},headers={'Origin':'https://attacker.invalid'}).status_code==403
 assert c.post('/api/auth/login',data='email=a').status_code==415

def test_account_isolation_and_idempotency(app):
 a=signup(app);b=signup(app,'second@example.invalid');p=plan(a)
 assert not b.get('/api/product').json()['plans']
 assert b.post('/api/product/plan-action',json={'id':p['id'],'action':'archive'}).status_code==404
 assert plan(a)['id']==p['id']
 assert a.post('/api/product/plans',json={'match_id':'other','request_id':'fixture-plan-0001'}).status_code==409

def test_member_entitlement_gates_research(app):
 c=signup(app,'member@example.invalid','member')
 assert c.get('/api/state').status_code==402
 assert c.get('/api/account').status_code==200
 assert c.get('/api/admin/readiness').status_code==403

def test_native_bearer_session(app):
 signup(app)
 c=TestClient(app);r=c.post('/api/auth/login',json={'email':'owner@example.invalid','password':'fixture-only-password-2026'},headers={'X-Kanida-Client':'native'})
 assert r.status_code==200;assert 'set-cookie' not in r.headers
 c.headers['Authorization']='Bearer '+r.json()['access_token']
 assert c.get('/api/product').status_code==200
 assert c.post('/api/auth/logout',json={}).status_code==200
 assert c.get('/api/product').status_code==401

def test_live_orders_are_locked_before_provider_call(app):
 c=signup(app)
 assert c.post('/api/trading/live',json={}).status_code==423
 assert c.get('/health').json()['live_enabled'] is False

@pytest.mark.parametrize('case,expected',[('target','closed'),('stop','closed'),('gap','closed'),('partial','closed'),('rejected','rejected')])
def test_simulated_lifecycle_and_ledger(app,case,expected):
 c=signup(app);p=plan(c);start=c.get('/api/trading').json()['wallet']['cash_paise']
 body={'plan_id':p['id'],'request_id':'simulation-test-001','scenario':case,'acknowledge_synthetic':True}
 r=c.post('/api/trading/simulate',json=body);assert r.status_code==200,r.text;order=r.json()
 assert c.post('/api/trading/simulate',json=body).json()['id']==order['id']
 for _ in range(10):
  result=c.post('/api/trading/advance',json={'id':order['id']});assert result.status_code==200,result.text
 final=c.get('/api/trading').json();last=final['orders'][0]
 assert last['status']==expected;assert final['wallet']['reserved_paise']==0
 assert final['wallet']['cash_paise']==start+last['realized_paise']
 assert last['payload']['source']=='synthetic_workflow_fixture'
 if case in ('stop','gap'):assert last['realized_paise']<0
 if case=='rejected':assert last['realized_paise']==0

def test_chosen_scenario_outcomes_are_separate_from_capital(app):
 from kanida_pilot.db import wallets
 c=signup(app);p=plan(c);uid=c.get('/api/auth/me').json()['user']['id']
 w=c.get('/api/trading').json()['wallet'];initial=w['initial_paise']
 assert (w['available_capital_paise'],w['scenario_outcome_total_paise'],w['scenario_outcome_count'])==(initial,0,0) and 'synthetic' in w['scenario_outcome_note'].lower()
 order=c.post('/api/trading/simulate',json={'plan_id':p['id'],'request_id':'simulation-test-001','scenario':'target','acknowledge_synthetic':True}).json()
 assert c.get('/api/trading').json()['wallet']['available_capital_paise']==initial-1004000
 for _ in range(10):c.post('/api/trading/advance',json={'id':order['id']})
 final=c.get('/api/trading').json();w=final['wallet'];last=final['orders'][0]
 assert last['status']=='closed' and last['realized_paise']>0 and last['outcome_kind']=='chosen_scenario_synthetic'
 assert w['available_capital_paise']==initial and w['reserved_paise']==0
 assert w['scenario_outcome_total_paise']==last['realized_paise']==w['realized_paise'] and w['scenario_outcome_count']==1
 assert w['cash_paise']==initial+last['realized_paise']
 with app.state.db.tx() as conn:conn.execute(wallets.update().where(wallets.c.user_id==uid).values(cash_paise=initial*10))
 big=plan(c,'fixture-plan-0002',notional=initial//100+1)
 r=c.post('/api/trading/simulate',json={'plan_id':big['id'],'request_id':'simulation-test-002','scenario':'target','acknowledge_synthetic':True})
 assert r.status_code==409 and r.json()['code']=='FUNDS'

def test_pause_does_not_block_exits(app):
 c=signup(app);p=plan(c)
 order=c.post('/api/trading/simulate',json={'plan_id':p['id'],'request_id':'simulation-test-001','scenario':'stop','acknowledge_synthetic':True}).json()
 c.post('/api/trading/advance',json={'id':order['id']})
 c.post('/api/trading/pause',json={'paused':True})
 assert c.post('/api/trading/simulate',json={'plan_id':p['id'],'request_id':'simulation-test-002','acknowledge_synthetic':True}).status_code==409
 assert c.post('/api/trading/exit',json={'id':order['id']}).status_code==200
 assert c.get('/api/trading').json()['wallet']['reserved_paise']==0

def test_cancel_releases_reservation(app):
 c=signup(app);p=plan(c)
 order=c.post('/api/trading/simulate',json={'plan_id':p['id'],'request_id':'simulation-test-001','acknowledge_synthetic':True}).json()
 assert c.post('/api/trading/cancel',json={'id':order['id']}).status_code==200
 assert c.get('/api/trading').json()['wallet']['reserved_paise']==0
 assert c.post('/api/trading/cancel',json={'id':order['id']}).status_code==409

def test_google_state_binding_and_single_use(app):
 auth=app.state.auth;auth.settings.google_client_id='test';auth.settings.google_client_secret='fixture'
 url=auth.start_google('browser-a')
 from urllib.parse import urlparse,parse_qs
 state=parse_qs(urlparse(url).query)['state'][0]
 with pytest.raises(PilotError):auth.consume_flow(state,'browser-b','google')
 assert auth.consume_flow(state,'browser-a','google')['provider']=='google'
 with pytest.raises(PilotError):auth.consume_flow(state,'browser-a','google')

def test_webhook_forgery(app):
 billing=app.state.billing;s=billing.settings
 s.razorpay_key_id='rzp_test_fixture';s.razorpay_key_secret='fixture';s.razorpay_webhook_secret='fixture-webhook';s.razorpay_plan_id='plan_fixture'
 c=TestClient(app);raw=b'{"event":"subscription.activated","payload":{}}'
 assert c.post('/api/billing/webhook',content=raw,headers={'X-Razorpay-Signature':'fake'}).status_code==400
 signature=hmac.new(s.razorpay_webhook_secret.encode(),raw,hashlib.sha256).hexdigest()
 assert c.post('/api/billing/webhook',content=raw,headers={'X-Razorpay-Signature':signature}).status_code==200
 assert c.post('/api/billing/webhook',content=raw,headers={'X-Razorpay-Signature':signature}).json()['duplicate']

def test_kite_expiry_before_and_after_six():
 from datetime import datetime,timezone,timedelta
 ist=timezone(timedelta(hours=5,minutes=30))
 before=datetime(2026,9,14,5,59,tzinfo=ist);after=datetime(2026,9,14,6,0,tzinfo=ist)
 assert datetime.fromtimestamp(next_expiry(before.timestamp()),ist).day==14
 assert datetime.fromtimestamp(next_expiry(after.timestamp()),ist).day==15

def test_device_proof_is_required_and_single_use(app):
 import base64
 browser=signup(app);device=TestClient(app);verifier='p'*64
 challenge=base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip('=')
 code=device.post('/api/auth/device/start',json={'challenge':challenge}).json()['code']
 assert device.post('/api/auth/device/complete',json={'code':code,'verifier':verifier}).json()['pending']
 assert browser.post('/api/auth/device/approve',json={'code':code,'confirm_device':True}).status_code==200
 assert device.post('/api/auth/device/complete',json={'code':code,'verifier':'x'*64}).status_code==400
 result=device.post('/api/auth/device/complete',json={'code':code,'verifier':verifier});assert result.status_code==200
 assert device.post('/api/auth/device/complete',json={'code':code,'verifier':verifier}).status_code==400
 device.headers['Authorization']='Bearer '+result.json()['access_token']
 assert device.get('/api/product').status_code==200

def test_recovery_revokes_sessions_and_cannot_replay(app):
 from urllib.parse import urlparse,parse_qs
 owner=signup(app);member=signup(app,'recover@example.invalid','member');public=TestClient(app)
 result=owner.post('/api/admin/recovery',json={'email':'recover@example.invalid'});assert result.status_code==200,result.text
 code=parse_qs(urlparse(result.json()['url']).query)['code'][0]
 assert member.post('/api/admin/recovery',json={'email':'owner@example.invalid'}).status_code==403
 body={'code':code,'password':'new-fixture-password-98765'}
 assert public.post('/api/auth/recover',json=body).status_code==200
 assert member.get('/api/account').status_code==401
 assert public.post('/api/auth/recover',json=body).status_code==400
 assert public.post('/api/auth/login',json={'email':'recover@example.invalid','password':body['password']}).status_code==200

def test_owner_included_access_is_explicit_and_preserved(app):
 owner=signup(app);member=signup(app,'included@example.invalid','member')
 assert member.get('/api/state').status_code==402
 assert owner.post('/api/admin/access',json={'email':'included@example.invalid'}).status_code==200
 assert member.get('/api/state').status_code==200
 member.post('/api/account/onboarding',json={'acknowledge_pilot':True,'policy_version':'private-pilot-v1','timeframes':['4H']})
 assert member.get('/api/billing/status').json()['access_reason']=='Included pilot access'

def test_billing_requires_paid_period_and_matching_plan(app):
 c=signup(app,'paid@example.invalid','member');billing=app.state.billing;billing.settings.razorpay_plan_id='plan_fixture'
 uid=c.get('/api/auth/me').json()['user']['id']
 with app.state.db.tx() as conn:conn.execute(subscriptions.insert().values(id='sub-local',user_id=uid,provider_id='sub_provider',status='created',created=now(),updated=now(),last_event=0,cancel_at_end=False))
 value={'id':'sub_provider','plan_id':'plan_fixture','status':'active','current_end':now()+3600,'paid_count':0}
 billing.apply(value);assert not c.get('/api/billing/status').json()['access']
 value['paid_count']=1;billing.apply(value);assert c.get('/api/billing/status').json()['access']
 value['status']='halted';billing.apply(value);assert not c.get('/api/billing/status').json()['access']
 value['plan_id']='plan_wrong'
 with pytest.raises(PilotError):billing.apply(value)

def test_instrument_routing_and_fresh_quote_gate():
 from kanida_pilot.live import product_for,validate_intent
 cash={'instrument_type':'EQ','exchange':'NSE','lot_size':1,'tick_size':.05}
 future={'instrument_type':'FUT','exchange':'NFO','lot_size':25,'tick_size':.05}
 assert product_for(cash,'long','overnight')=='CNC'
 assert product_for(cash,'short','intraday')=='MIS'
 assert product_for(future,'short','overnight')=='NRML'
 with pytest.raises(PilotError):product_for(cash,'short','overnight')
 quote={'last_price':100,'received_at':now()}
 assert validate_intent(cash,quote,10,100.05,'long','overnight',10000)=='CNC'
 with pytest.raises(PilotError):validate_intent(future,quote,26,100,'long','overnight',10000)
 with pytest.raises(PilotError):validate_intent(cash,quote,10,100.03,'long','overnight',10000)
 with pytest.raises(PilotError):validate_intent(cash,dict(quote,received_at=now()-11),10,100,'long','overnight',10000)

def test_live_release_cannot_be_enabled_by_environment(app):
 c=signup(app);app.state.live.settings.live_enabled=True;app.state.live.settings.live_activation='not-a-release'
 assert c.post('/api/trading/live',json={}).status_code==423

def test_ambiguous_live_submission_is_durable_and_not_retried(app,monkeypatch):
 import kanida_pilot.live as module
 c=signup(app);p=plan(c);uid=c.get('/api/auth/me').json()['user']['id']
 with app.state.db.tx() as conn:user=row(conn,select(users).where(users.c.id==uid))
 service=app.state.live;monkeypatch.setattr(module,'LIVE_RELEASE_APPROVED',True);service.settings.live_enabled=True;service.settings.live_activation='fixture-only'
 calls=[]
 def broker(*args):calls.append(args);raise TimeoutError('fixture')
 monkeypatch.setattr(service.kite,'call',broker)
 verified=dict(user_id=uid,side='long',holding='overnight',instrument=dict(instrument_type='EQ',exchange='NSE',tradingsymbol='TEST',lot_size=1,tick_size=.05),
  quote=dict(last_price=100,received_at=now()),max_notional=10000,market_open=True,protective_exit_ready=True,margin_verified=True,cost_reserve_paise=100,capital_limit_paise=1000000)
 body=dict(plan_id=p['id'],quantity=10,limit_price=100,request_id='live-fixture-0001')
 with pytest.raises(PilotError):service.submit(user,body,verified)
 result=service.submit(user,body,verified);assert result['status']=='confirmation_required';assert len(calls)==1

def test_concurrent_simulation_requests_reserve_once(app):
 from concurrent.futures import ThreadPoolExecutor
 c=signup(app);p=plan(c);uid=c.get('/api/auth/me').json()['user']['id']
 with app.state.db.tx() as conn:user=row(conn,select(users).where(users.c.id==uid))
 body=dict(plan_id=p['id'],request_id='concurrent-0001',scenario='partial',acknowledge_synthetic=True)
 with ThreadPoolExecutor(max_workers=4) as pool:results=list(pool.map(lambda _:app.state.simulation.submit(user,body),range(4)))
 assert len({r['id'] for r in results})==1
 assert c.get('/api/trading').json()['wallet']['reserved_paise']==1004000

def test_uncertain_billing_creation_recovers_without_duplicate(app,monkeypatch):
 c=signup(app,'checkout@example.invalid','member');billing=app.state.billing;s=billing.settings
 s.razorpay_key_id='rzp_test_fixture';s.razorpay_key_secret='fixture';s.razorpay_webhook_secret='fixture';s.razorpay_plan_id='plan_fixture'
 calls=[]
 def uncertain(method,path,body=None):
  calls.append((method,path));raise PilotError(502,'UNCERTAIN','Fixture timeout')
 monkeypatch.setattr(billing,'call',uncertain)
 assert c.post('/api/billing/checkout',json={'acknowledge_test':True}).status_code==502
 assert c.post('/api/billing/checkout',json={'acknowledge_test':True}).status_code==409
 assert len(calls)==1
 with app.state.db.tx() as conn:sub=row(conn,select(subscriptions))
 value=dict(id='sub_recovered',plan_id='plan_fixture',status='active',paid_count=1,current_end=now()+3600,short_url='https://rzp.io/i/fixture',notes={'kanida_reference':sub['id']})
 def recovered(method,path,body=None):return {'items':[value]} if path.startswith('subscriptions?') else value
 monkeypatch.setattr(billing,'call',recovered)
 assert c.post('/api/billing/refresh',json={}).json()['access']
 assert c.post('/api/billing/checkout',json={'acknowledge_test':True}).json()['id']==sub['id']

def test_kite_token_encryption_binding_and_ownership(app,monkeypatch):
 import httpx
 from urllib.parse import urlparse,parse_qs
 from kanida_pilot.db import brokers
 c=signup(app);d=signup(app,'second-broker@example.invalid');kite=app.state.kite
 kite.settings.kite_api_key='fixture_key';kite.settings.kite_api_secret='fixture_secret'
 requests=[]
 def response(request):
  requests.append(request)
  return httpx.Response(200,json={'status':'success','data':{'user_id':'FIXTURE123','access_token':'fixture-provider-token'}})
 kite.http=httpx.Client(transport=httpx.MockTransport(response))
 value=c.post('/api/broker/kite/start',json={});assert value.status_code==200
 state=parse_qs(parse_qs(urlparse(value.json()['url']).query)['redirect_params'][0])['state'][0]
 assert d.get('/api/broker/kite/callback',params={'state':state,'request_token':'fixture'},follow_redirects=False).headers['location'].endswith('LOGIN_EXPIRED')
 assert c.get('/api/broker/kite/callback',params={'state':state,'request_token':'fixture'},follow_redirects=False).headers['location']=='/account?broker=connected'
 with app.state.db.tx() as conn:stored=row(conn,select(brokers))
 assert stored['encrypted_token']!='fixture-provider-token'
 assert kite.cipher.decrypt(stored['encrypted_token'].encode()).decode()=='fixture-provider-token'
 assert 'fixture-provider-token' not in c.get('/api/account').text
 value=d.post('/api/broker/kite/start',json={});state=parse_qs(parse_qs(urlparse(value.json()['url']).query)['redirect_params'][0])['state'][0]
 assert d.get('/api/broker/kite/callback',params={'state':state,'request_token':'fixture2'},follow_redirects=False).headers['location'].endswith('BROKER_OWNER')
 assert c.post('/api/broker/disconnect',json={}).status_code==200
 assert requests[-1].method=='DELETE' and requests[-1].url.params['access_token']=='fixture-provider-token'
 kite.http.close()

def test_google_verified_claims_require_matching_nonce_and_invite(app,monkeypatch):
 import httpx
 from urllib.parse import urlparse,parse_qs
 from google.oauth2 import id_token
 auth=app.state.auth;auth.settings.google_client_id='fixture-id';auth.settings.google_client_secret='fixture-secret'
 auth.http=httpx.Client(transport=httpx.MockTransport(lambda r:httpx.Response(200,json={'id_token':'fixture-token'})))
 invite=auth.invite('google@example.invalid');url=auth.start_google('fixture-browser',invite)
 state=parse_qs(urlparse(url).query)['state'][0]
 with app.state.db.tx() as conn:flow=row(conn,select(oauth).where(oauth.c.hash==digest(state)))
 monkeypatch.setattr(id_token,'verify_oauth2_token',lambda *a,**kw:dict(nonce=flow['nonce'],email_verified=True,email='google@example.invalid',sub='fixture-sub',name='Google fixture'))
 user=auth.google_callback(state,'fixture-browser','fixture-code');assert user['email']=='google@example.invalid';assert user['role']=='member';assert user['password_hash'] is None
 with pytest.raises(PilotError):auth.google_callback(state,'fixture-browser','fixture-code')
 url=auth.start_google('fixture-browser');state=parse_qs(urlparse(url).query)['state'][0]
 with pytest.raises(PilotError):auth.google_callback(state,'fixture-browser','fixture-code')
 auth.http.close()

def test_older_billing_fetch_cannot_overwrite_newer_state(app):
 c=signup(app,'race@example.invalid','member');billing=app.state.billing;billing.settings.razorpay_plan_id='plan_fixture'
 uid=c.get('/api/auth/me').json()['user']['id']
 with app.state.db.tx() as conn:conn.execute(subscriptions.insert().values(id='race-sub',user_id=uid,provider_id='sub_race',status='created',created=now(),updated=now(),last_event=0,cancel_at_end=False))
 value=dict(id='sub_race',plan_id='plan_fixture',paid_count=1,current_end=now()+3600,status='halted',_kanida_request_stamp=200)
 billing.apply(value)
 billing.apply(dict(value,status='active',_kanida_request_stamp=100))
 assert not c.get('/api/billing/status').json()['access']
