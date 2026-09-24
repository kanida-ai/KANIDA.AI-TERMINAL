"""/api/screener/* mounted on a real pilot app: sign-in, owner scoping, immutable defaults, alerts, CSRF."""
import time
import pytest
from sqlalchemy import select
from kanida_pilot.app import create_app
from kanida_pilot.db import users
from kanida_pilot.screener import mount
from screener_fixture import build,ladder
from test_pilot import Evidence,pilot_settings,signup

SESSION='2026-09-21';EXP='2026-09-29'
TIMES=['09:30','09:45','10:00','10:15','10:30','10:45']
N=len(TIMES)
CALL_OI={'strikes':{'kind':'atm','below':2,'above':2},'conditions':[{'metric':'oi','side':'CE','state':'up_cont',
 'window':{'kind':'minutes','value':30}}]}


def grant(app,email):
 with app.state.db.tx() as c:
  user=c.execute(select(users).where(users.c.email==email)).mappings().first()
  prefs=dict(user['preferences'] or {});prefs['pilot_access_until']=time.time()+86400
  c.execute(users.update().where(users.c.email==email).values(preferences=prefs))


@pytest.fixture
def pilot(tmp_path):
 times=TIMES
 oid={(24500,'CE'):{'oid':[0,1e5,2e5,3e5,4e5,5e5]}}
 store=build(str(tmp_path/'derivatives.db'),SESSION,times,{('NIFTY',EXP):{'spot':[24500.0]*N}},
  ladder('NIFTY',EXP,[24300,24400,24500,24600,24700],N,oid))
 settings=pilot_settings(tmp_path,derivatives_database=store)
 app=create_app(settings,evidence=Evidence())
 screener=mount(app,settings,str(tmp_path/'screener.db'))
 assert screener is not None
 owner=signup(app)
 other=signup(app,'member@example.invalid','member');grant(app,'member@example.invalid')
 yield app,owner,other
 app.state.db.close();screener.db.close()


def test_routes_sit_ahead_of_the_catch_alls(pilot):
 app,owner,_=pilot
 r=owner.get('/api/screener/vocabulary')
 assert r.status_code==200 and {m['key'] for m in r.json()['metrics']}>={'oi','iv','pcr','maxpain','premium','delta','gamma'}


def test_sign_in_is_required(pilot):
 app,_o,_m=pilot
 from fastapi.testclient import TestClient
 assert TestClient(app).get('/api/screener/scanners').status_code==401


def test_defaults_are_listed_and_cannot_be_changed(pilot):
 _app,owner,_=pilot
 body=owner.get('/api/screener/scanners').json()
 defaults=[s for s in body['scanners'] if s['is_default']]
 assert len(defaults)>=10 and body['status']['available']
 sid=defaults[0]['id']
 assert owner.post(f'/api/screener/scanners/{sid}',json={'name':'x'}).status_code==403
 assert owner.post(f'/api/screener/scanners/{sid}/delete',json={}).status_code==403
 copy=owner.post(f'/api/screener/scanners/{sid}/duplicate',json={}).json()
 assert copy['mine'] and copy['definition']==defaults[0]['definition'] and copy['name'].endswith('(copy)')


def test_saved_scanners_are_private_to_their_owner(pilot):
 _app,owner,other=pilot
 made=owner.post('/api/screener/scanners',json={'name':'  My   call OI  ','definition':CALL_OI,'source':'visual'}).json()
 assert made['name']=='My call OI' and made['grain']=='contract'
 sid=made['id']
 assert sid in {s['id'] for s in owner.get('/api/screener/scanners').json()['scanners']}
 assert sid not in {s['id'] for s in other.get('/api/screener/scanners').json()['scanners']}
 for call in (lambda c:c.get(f'/api/screener/scanners/{sid}/results'),
   lambda c:c.post(f'/api/screener/scanners/{sid}',json={'name':'mine now'}),
   lambda c:c.post(f'/api/screener/scanners/{sid}/delete',json={}),
   lambda c:c.post(f'/api/screener/scanners/{sid}/duplicate',json={})):
  assert call(other).status_code==404
 renamed=owner.post(f'/api/screener/scanners/{sid}',json={'name':'Renamed'}).json()
 assert renamed['name']=='Renamed'
 assert owner.post(f'/api/screener/scanners/{sid}/delete',json={}).json()['ok']
 assert owner.get(f'/api/screener/scanners/{sid}/results').status_code==404


def test_results_views(pilot):
 _app,owner,_=pilot
 sid=owner.post('/api/screener/scanners',json={'name':'c','definition':CALL_OI}).json()['id']
 r=owner.get(f'/api/screener/scanners/{sid}/results?view=all').json()
 assert r['counts']['all']==1 and r['matches'][0]['title']=='NIFTY · 24,500 CE' and r['matches'][0]['active']
 assert owner.get(f'/api/screener/scanners/{sid}/results?view=ended').json()['matches']==[]
 assert owner.get(f'/api/screener/scanners/{sid}/results?view=nope').status_code==400


def test_invalid_definitions_are_refused_with_the_reason(pilot):
 _app,owner,_=pilot
 r=owner.post('/api/screener/run',json={'definition':{'conditions':[{'metric':'oi','state':'unusual'}]}})
 assert r.status_code==400 and 'OI cannot be' in r.json()['error']


def test_parse_and_describe_produce_the_same_definition(pilot):
 _app,owner,_=pilot
 p=owner.post('/api/screener/parse',json={'text':'NIFTY call OI increasing continuously for 30 min, atm ±2'}).json()
 d=owner.post('/api/screener/describe',json={'definition':p['definition']}).json()
 assert d['definition']==p['definition'] and d['reads_as']==p['reads_as']
 assert p['definition']['universe']=={'kind':'symbols','symbols':['NIFTY']}


def test_csrf_is_enforced_on_writes(pilot):
 _app,owner,_=pilot
 csrf=owner.headers.pop('X-Kanida-CSRF')
 try:assert owner.post('/api/screener/scanners',json={'name':'x','definition':CALL_OI}).status_code==403
 finally:owner.headers['X-Kanida-CSRF']=csrf


def test_alerts_fire_once_per_transition_and_only_after_subscribing(pilot):
 app,owner,other=pilot
 sid=owner.post('/api/screener/scanners',json={'name':'alerts','definition':CALL_OI}).json()['id']
 sub=owner.post(f'/api/screener/scanners/{sid}/notify',json={'new':True,'ended':True,'changed':True}).json()
 assert sub['notify']=={'new':True,'ended':True,'changed':True}
 screener=app.state.screener
 # the match began at 10:00 - BEFORE the subscription (made at 10:45) - so it is not replayed as an alert
 screener.results(CALL_OI,cache=False)
 assert owner.get('/api/screener/alerts').json()['alerts']==[]
 # a later transition counts: rewind the subscription to 09:30 and evaluate again, twice
 screener.db._x('update subscriptions set alerts_from=? where scanner_id=?',(f'{SESSION} 09:30:00',sid))
 screener.results(CALL_OI,cache=False);screener.results(CALL_OI,cache=False)
 got=owner.get('/api/screener/alerts').json()
 assert [a['kind'] for a in got['alerts']]==['new'] and got['unread']==1
 assert 'New match in alerts at 10:00' in got['alerts'][0]['text']
 assert other.get('/api/screener/alerts').json()['alerts']==[]
 assert owner.post('/api/screener/alerts/read',json={}).json()['unread']==0
