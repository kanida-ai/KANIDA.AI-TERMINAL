"""Member access and live-detection identity must also hold on the new history routes."""
from fastapi.testclient import TestClient
from kanida_pilot.app import create_app
from test_pilot import Evidence, pilot_settings, signup


class HistoryStub:
 def catalogue(self):return {'patterns': []}
 def stocks(self,*args,**kwargs):return {'stocks': []}
 def history(self,*args,**kwargs):return {'selection':list(args),'horizon':kwargs['horizon']}
 def replay(self,*args):return {'selection':list(args)}


def make_app(tmp_path):
 app=create_app(pilot_settings(tmp_path),evidence=Evidence())
 app.state.pattern_history=HistoryStub()
 return app


def test_history_routes_require_member_and_forward_exact_selection(tmp_path):
 app=make_app(tmp_path)
 try:
  anonymous=TestClient(app)
  for path in ('catalogue','stocks','history?symbol=TEST','replay?symbol=TEST&occurrence_id=a'):
   assert anonymous.get('/api/pattern-history/'+path).status_code==401
  client=signup(app)
  assert client.get('/api/pattern-history/catalogue').json()=={'patterns':[]}
  data=client.get('/api/pattern-history/history?symbol=TEST&pattern_id=CDLENGULFING&variant=canonical_context&state=setup&horizon=10')
  assert data.status_code==200,data.text
  assert data.json()=={'selection':['TEST','CDLENGULFING','canonical_context','setup'],'horizon':10}
  assert client.get('/api/pattern-history/replay?symbol=TEST&occurrence_id=a').json()['selection']==['TEST','CH16','canonical','confirmed','a']
 finally:app.state.db.close()


def test_live_history_cannot_borrow_another_detector_or_state(tmp_path,monkeypatch):
 app=make_app(tmp_path)
 try:
  cfg=dict(pattern_id='CH16',variant='canonical',side='long',timeframe='1D',research_run=pilot_settings(tmp_path).pattern_research_run)
  monkeypatch.setattr(app.state.strategies,'research_strategy',lambda key:({'source_config':cfg},{}))
  card={'live_evidence':{'status':'identity_match'},'detection':{'state':'forming'}}
  monkeypatch.setattr(app.state.strategies,'card',lambda *args:card)
  client=signup(app)
  query='symbol=TEST&pattern_id=CH16&variant=canonical&state=setup&strategy_key=pilot&detection_id=abcd'
  assert client.get('/api/pattern-history/history?'+query).status_code==200
  assert client.get('/api/pattern-history/history?'+query.replace('state=setup','state=confirmed')).status_code==409
  card['live_evidence']={'status':'detector_mismatch','note':'Different detector'}
  for endpoint in ('history','replay'):
   result=client.get('/api/pattern-history/'+endpoint+'?'+query+'&occurrence_id=a')
   assert result.status_code==409 and result.json()['code']=='HISTORY_IDENTITY_MISMATCH'
  assert client.get('/api/pattern-history/history?'+query.replace('&strategy_key=pilot','')).status_code==400
  card['live_evidence']={'status':'identity_match'}
  cfg['variant']='other'
  assert client.get('/api/pattern-history/history?'+query).status_code==409
 finally:app.state.db.close()
