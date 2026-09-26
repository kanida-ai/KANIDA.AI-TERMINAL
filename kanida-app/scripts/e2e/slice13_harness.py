"""Slice 13 browser harness (fresh GTM audit P01-P04, P07). Isolated temp databases, a two-expiry stored reading, a
fixture-only owner account; never touches the pilot's data or Kite. Port 8093.
  E2E_OUT=/tmp/e2e .pilot-venv/bin/python scripts/e2e/slice13_harness.py &
  E2E_OUT=/tmp/e2e <python with playwright> scripts/e2e/slice13_browser.py      (build dist-pilot first)"""
import json,os,sys,tempfile,pathlib
APP=pathlib.Path(__file__).resolve().parents[2]
sys.path[:0]=[str(APP/'server'),str(APP/'server'/'tests')]
os.environ['PILOT_SB_ALERTS']='off'
import uvicorn
from fastapi.testclient import TestClient
from kanida_pilot.app import create_app
from kanida_pilot.strategy_builder import mount
from test_pilot import Evidence,pilot_settings
from test_gtm_fresh import two_expiry_store
d=pathlib.Path(tempfile.mkdtemp());OUT=pathlib.Path(os.getenv('E2E_OUT',str(d)));OUT.mkdir(parents=True,exist_ok=True);ORIGIN='http://127.0.0.1:8093'
db=two_expiry_store(str(d/'derivatives.db'))
settings=pilot_settings(d,derivatives_database=db,snapshots='off',origin=ORIGIN,origins=[ORIGIN],
 web_directory=str(APP/'dist-pilot'))
app=create_app(settings,evidence=Evidence());mount(app,settings,str(d/'sb.db'),live=False)
c=TestClient(app,base_url=ORIGIN);invite=app.state.auth.invite('owner@example.invalid','owner')
r=c.post('/api/auth/register',json={'email':'owner@example.invalid','password':'fixture-only-password-2026','name':'Test account','invite':invite,'policy_version':'private-pilot-v1'})
assert r.status_code==200,r.text
c.headers.update({'X-Kanida-CSRF':r.json()['csrf'],'Origin':ORIGIN})
assert c.post('/api/account/onboarding',json={'acknowledge_pilot':True,'policy_version':'private-pilot-v1','timeframes':['1D']}).status_code==200
json.dump([{'name':k,'value':v,'domain':'127.0.0.1','path':'/'} for k,v in c.cookies.items()],open(str(OUT/'cookies.json'),'w'))
EXP='2026-09-29';FAR='2026-10-06'
cal=c.post('/api/sb/strategies',json={'name':'Slice13 calendar','body':{'underlying':'NIFTY','expiry':EXP,'legs':[
 {'id':'near','type':'CE','side':'S','strike':23000,'expiry':EXP},{'id':'far','type':'CE','side':'B','strike':23000,'expiry':FAR}]}}).json()
cal2=c.post('/api/sb/strategies',json={'name':'Slice13 calendar (phone)','body':{'underlying':'NIFTY','expiry':EXP,'legs':[
 {'id':'near','type':'CE','side':'S','strike':23000,'expiry':EXP},{'id':'far','type':'CE','side':'B','strike':23000,'expiry':FAR}]}}).json()
miss=c.post('/api/sb/strategies',json={'name':'Slice13 missing hedge','body':{'underlying':'NIFTY','expiry':EXP,'legs':[
 {'id':'a','type':'CE','side':'S','strike':23000},{'id':'b','type':'CE','side':'B','strike':30000}]}}).json()
vert=c.post('/api/sb/strategies',json={'name':'Slice13 vertical','body':{'underlying':'NIFTY','expiry':EXP,'legs':[
 {'id':'a','type':'CE','side':'B','strike':23000},{'id':'b','type':'CE','side':'S','strike':23200}]}}).json()
run=c.post(f"/api/sb/strategies/{vert['id']}/paper",json={'confirm':True,'expected_version':vert['draft']['version']}).json()
disc=c.post('/api/sb/discover',json={'underlying':'NIFTY','expiry':EXP,'view':'up','target':23300,'max_loss':6000,'lots':1}).json()
fromd=c.post('/api/sb/discover/use',json={'candidate_id':disc['candidates'][0]['candidate_id']}).json() if disc.get('candidates') else {'id':None}
json.dump({'paper_run':run.get('id'),'from_discover':fromd['id'],'calendar':cal['id'],'calendar_phone':cal2['id'],'missing':miss['id'],'vertical':vert['id']},open(str(OUT/'ids.json'),'w'))
print('ready',flush=True)
uvicorn.run(app,host='127.0.0.1',port=8093,log_level='warning')
