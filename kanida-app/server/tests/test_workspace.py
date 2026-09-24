"""The workspace: the validated definition, templates, autosave with a version check, owner scoping, the AI
summary (deterministic, source-scoped, scanner-aware and profile-scoped), Greeks and volume."""
import json,sqlite3,time
import pytest
from sqlalchemy import select
from kanida_pilot.app import create_app
from kanida_pilot.db import users
from kanida_pilot.screener import mount as mount_screener
from kanida_pilot.workspace import mount as mount_workspace
from kanida_pilot.workspace.model import WorkspaceError,normalize
from kanida_pilot.workspace.registry import WIDGETS
from kanida_pilot.workspace.templates import TEMPLATES
from screener_fixture import build,ladder
from test_pilot import Evidence,pilot_settings,signup

SESSION='2026-09-21';EXP='2026-09-29'
TIMES=['09:30','09:45','10:00','10:15','10:30','10:45']
N=len(TIMES)


# --- the definition --------------------------------------------------------------------------------------------
def test_every_template_is_a_valid_definition():
 assert {t[0] for t in TEMPLATES}>={'options-trader','index-derivatives','intraday-scanner','volatility-watch','blank'}
 for _k,name,_d,raw in TEMPLATES:
  d=normalize({**raw,'name':name})
  assert [w['position'] for w in d['widgets']]==list(range(len(d['widgets'])))
  # rows are composed by the layout engine (scripts/check-workbench-layout.cjs), so a template carries only
  # order and size class — and never more than the engine can put on two screen-sharing rows at 1440px
  assert len(d['widgets'])<=8 and d['layout']=={'columns':'auto'},name


def test_layout_choice_is_validated():
 assert normalize({'widgets':[],'layout':{'columns':3}})['layout']=={'columns':3}
 with pytest.raises(WorkspaceError):normalize({'widgets':[],'layout':{'columns':9}})


def test_normalize_keeps_only_real_settings_and_orders_by_position():
 d=normalize({'widgets':[
  {'widget_type':'pcr','position':2,'settings':{'view':'chart','made_up':'x'}},
  {'widget_type':'option_chain','position':0,'expiry':'2026-09-29','follow_workspace':False,'instrument':'banknifty'},
  {'widget_type':'screener_results','position':1,'scanner_id':'call-oi-building','instrument':'NIFTY'}]})
 kinds=[w['widget_type'] for w in d['widgets']]
 assert kinds==['option_chain','screener_results','pcr']
 chain,res,pcr=d['widgets']
 assert chain['instrument']=='BANKNIFTY' and chain['follow_workspace'] is False and chain['expiry']=='2026-09-29'
 assert res['instrument'] is None and res['follow_workspace'] is False and res['scanner_id']=='call-oi-building'
 assert pcr['settings']=={'view':'chart'} and pcr['size']==WIDGETS['pcr']['size']


@pytest.mark.parametrize('raw,needle',[
 ({'widgets':[{'widget_type':'nope'}]},'no widget called'),
 ({'widgets':[{'widget_type':'pcr','size':'XL'}]},'size must be'),
 ({'widgets':[{'widget_type':'pcr','follow_workspace':False}]},'needs its own instrument'),
 ({'widgets':[{'widget_type':'pcr','settings':{'view':'sideways'}}]},'not a choice'),
 ({'widgets':[{'widget_type':'pcr'}]*25},'at most 24'),
])
def test_normalize_refuses_with_a_sentence(raw,needle):
 with pytest.raises(WorkspaceError) as e:normalize(raw)
 assert needle in str(e.value)


def test_focus_carries_the_scanner_and_match():
 d=normalize({'selected':{'underlying':'nifty','focus':{'side':'CE','strikes':[24500,'x'],'from':'10:15',
  'scanner_id':'call-oi-building','match_key':'NIFTY|2026-09-29|1001','because':['a']}},'widgets':[]})
 f=d['selected']['focus']
 assert d['selected']['underlying']=='NIFTY' and f['strikes']==[24500.0] and f['scanner_id']=='call-oi-building'


# --- a pilot with both mounts ---------------------------------------------------------------------------------
SNAP_SCHEMA='''create table reading_snapshots(id integer primary key, session text, reading_at text, underlying text,
 expiry text, spot real, atm_strike real, status text, reason text, contracts text, reading text, chained text,
 previous_snapshot_id integer, engine_version text, rules_version text, created_at text)'''


def snapshots(path,rows):
 c=sqlite3.connect(path);c.execute(SNAP_SCHEMA)
 for i,(t,obj) in enumerate(rows):
  c.execute('insert into reading_snapshots(session,reading_at,underlying,expiry,status,chained,engine_version,created_at)'
   ' values(?,?,?,?,?,?,?,?)',(SESSION,f'{SESSION} {t}:00','NIFTY',EXP,'ok',json.dumps(obj),'pane/test',''))
 c.commit();c.close()


def state(t,prev,call='building',put='quiet',**extra):
 return {'timestamp':f'{SESSION} {t}:00','previous_timestamp':f'{SESSION} {prev}:00' if prev else None,'call_state':call,
  'put_state':put,'side':'CE','location':'above ATM','strike_range':[24500,24600],'leading_strike':24500,'breadth':'clustered',
  'plain_language_headline':'Call positions building above ATM','state_change':'Building','call_oi_change':2e5,
  'put_oi_change':0,'pcr_current':0.9,'pcr_previous':0.92,'max_pain_current':24500,'max_pain_previous':24500,'iv_change':0.4,
  'key_strikes':[{'strike':24500,'side':'CE','lead':True,'note':'Highest activity'}],'conflicting_evidence':[],'tags':[],
  'engine_version':'pane/test',**extra}


def grant(app,email):
 with app.state.db.tx() as c:
  u=c.execute(select(users).where(users.c.email==email)).mappings().first()
  p=dict(u['preferences'] or {});p['pilot_access_until']=time.time()+86400
  c.execute(users.update().where(users.c.email==email).values(preferences=p))


@pytest.fixture
def pilot(tmp_path):
 oid={(24500,'CE'):{'oid':[0,1e5,2e5,3e5,4e5,5e5]}}
 store=build(str(tmp_path/'derivatives.db'),SESSION,TIMES,{('NIFTY',EXP):{'spot':[24500.0]*N,
  'ce_volume':[1000,1500,None,2600,3000,3100],'pe_volume':[800,900,None,1200,1700,1800]}},
  ladder('NIFTY',EXP,[24300,24400,24500,24600,24700],N,oid))
 intel=str(tmp_path/'intelligence.db')
 snapshots(intel,[(t,state(t,TIMES[i-1] if i else None)) for i,t in enumerate(TIMES)])
 settings=pilot_settings(tmp_path,derivatives_database=store,intelligence_database=str(tmp_path/'pilot-intel.db'),snapshots='off')
 app=create_app(settings,evidence=Evidence())
 sc=mount_screener(app,settings,str(tmp_path/'screener.db'))
 import os
 os.environ['PILOT_WORKSPACE_INTELLIGENCE']=intel
 try:wsstore=mount_workspace(app,settings,str(tmp_path/'workspace.db'))
 finally:os.environ.pop('PILOT_WORKSPACE_INTELLIGENCE',None)
 assert sc and wsstore
 owner=signup(app);other=signup(app,'member@example.invalid','member');grant(app,'member@example.invalid')
 yield app,owner,other
 app.state.db.close();sc.db.close();wsstore.close()


def test_first_visit_gets_a_working_workspace(pilot):
 _a,owner,_m=pilot
 got=owner.get('/api/workspace/workspaces').json()['workspaces']
 assert len(got)==1 and got[0]['name']=='My workspace'
 assert {w['widget_type'] for w in got[0]['widgets']}>={'screener_results','option_chain','signal','ai_summary'}
 assert owner.get('/api/workspace/workspaces').json()['workspaces'][0]['id']==got[0]['id']   # not re-created


def test_save_is_versioned_and_private(pilot):
 _a,owner,other=pilot
 ws=owner.post('/api/workspace/workspaces',json={'template':'blank','name':'NIFTY setup'}).json()
 assert ws['name']=='NIFTY setup' and ws['widgets']==[] and ws['version']==1
 d={'name':'NIFTY setup','selected':{'underlying':'NIFTY'},'widgets':[{'widget_type':'pcr'},{'widget_type':'pcr','follow_workspace':False,'instrument':'BANKNIFTY'}]}
 saved=owner.post(f"/api/workspace/workspaces/{ws['id']}",json={'definition':{**d,'layout':{'columns':3}},'version':1}).json()
 assert saved['version']==2 and [w['instrument'] for w in saved['widgets']]==[None,'BANKNIFTY']
 assert saved['layout']=={'columns':3}
 d={**d,'layout':{'columns':3}}
 stale=owner.post(f"/api/workspace/workspaces/{ws['id']}",json={'definition':{**d,'name':'x'},'version':1})
 assert stale.status_code==409 and stale.json()['current']['version']==2
 for call in (lambda c:c.get(f"/api/workspace/workspaces/{ws['id']}"),
   lambda c:c.post(f"/api/workspace/workspaces/{ws['id']}",json={'definition':d,'version':2}),
   lambda c:c.post(f"/api/workspace/workspaces/{ws['id']}/delete",json={})):
  assert call(other).status_code==404
 copy=owner.post(f"/api/workspace/workspaces/{ws['id']}/duplicate",json={}).json()
 assert copy['name']=='NIFTY setup (copy)' and len(copy['widgets'])==2 and copy['id']!=ws['id']
 assert copy['layout']=={'columns':3}
 left=owner.post(f"/api/workspace/workspaces/{ws['id']}/delete",json={}).json()['workspaces']
 assert ws['id'] not in [x['id'] for x in left]


def test_bad_definition_is_refused(pilot):
 _a,owner,_m=pilot
 ws=owner.get('/api/workspace/workspaces').json()['workspaces'][0]
 r=owner.post(f"/api/workspace/workspaces/{ws['id']}",json={'definition':{'widgets':[{'widget_type':'nope'}]},'version':ws['version']})
 assert r.status_code==400 and 'no widget called' in r.json()['error']


def test_summary_reads_only_connected_sources(pilot):
 _a,owner,_m=pilot
 lean=owner.post('/api/workspace/summary',json={'underlying':'NIFTY','sources':[],'scope':'connected'}).json()
 assert lean['available'] and lean['as_of']==f'{SESSION} 10:45:00'
 texts=[l['text'] for v in lean['sections'].values() for l in v]
 assert any('Call positions building above ATM' in t for t in texts)
 assert not any(t.startswith('PCR ') for t in texts) and 'PCR' in lean['not_read']
 assert any('Call OI rose at each of the last 6 readings' in l['text'] for l in lean['sections']['persistent'])
 full=owner.post('/api/workspace/summary',json={'underlying':'NIFTY','sources':['pcr','iv'],'scope':'connected'}).json()
 assert any(l['text']=='PCR 0.92 → 0.90.' and l['source']=='PCR' for l in full['sections']['changed'])
 for banned in ('buy','sell','bullish','bearish'):
  assert banned not in json.dumps(full['sections']).lower()


def test_summary_and_signal_follow_the_users_scanner_and_ignore_anyone_elses(pilot):
 _a,owner,other=pilot
 mine=owner.post('/api/screener/scanners',json={'name':'My call OI','definition':{'strikes':{'kind':'atm','below':2,'above':2},
  'conditions':[{'metric':'oi','side':'CE','state':'up_cont','window':{'kind':'minutes','value':30}}]}}).json()
 res=owner.get(f"/api/screener/scanners/{mine['id']}/results?view=all").json()
 m=res['matches'][0]
 focus={'scanner_id':mine['id'],'match_key':m['key'],'source':'My call OI','from':m['episode_started']}
 s=owner.post('/api/workspace/summary',json={'underlying':'NIFTY','sources':[],'focus':focus}).json()
 lines=[l['text'] for l in s['sections']['scanner']]
 assert lines[0].startswith('My call OI (your scanner): Call OI increasing continuously')
 assert any(t.startswith('Still matching') or t.startswith('New match') for t in lines)
 assert any('the 15-min signal agrees — call OI rose' in t for t in lines)
 strip=owner.get(f"/api/workspace/scanner-context?underlying=NIFTY&scanner_id={mine['id']}&match_key={m['key']}").json()
 assert strip['available'] and strip['context']['agreement'][0]['agrees'] is True
 # another user cannot read this user's scanner through the workspace
 theirs=other.post('/api/workspace/summary',json={'underlying':'NIFTY','sources':[],'focus':focus}).json()
 assert theirs['sections']['scanner']==[] and theirs['scanner'] is None
 assert other.get(f"/api/workspace/scanner-context?underlying=NIFTY&scanner_id={mine['id']}").json()['available'] is False


def test_key_strikes_greeks_volume(pilot):
 _a,owner,_m=pilot
 ks=owner.get('/api/workspace/key-strikes?underlying=NIFTY').json()
 assert ks['available'] and ks['key_strikes'][0]['strike']==24500
 g=owner.get('/api/workspace/greeks?underlying=NIFTY&atm=2').json()
 assert g['available'] and g['computed'] and g['atm_strike']==24500 and len(g['rows'])==5
 v=owner.get('/api/workspace/volume?underlying=NIFTY').json()
 pts=v['points']
 # the day's volume up to the first reading, then per interval; a reading with no value is a gap, and the
 # difference across it is not drawn as one interval's volume
 assert pts[0]=={'at':'09:30','ce':1000,'pe':800,'opening':True}
 assert pts[1]=={'at':'09:45','ce':500,'pe':100}
 assert pts[2]['gap'] and pts[3]['gap'] and pts[4]=={'at':'10:30','ce':400,'pe':500}
 assert owner.get('/api/workspace/greeks?underlying=bad%20name').status_code==400
