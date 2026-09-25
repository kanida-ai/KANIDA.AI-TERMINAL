"""/api/sb/* — the Strategy Builder. Member-only and owner-scoped; GET + POST only (the pilot's CORS allows those two).

  GET  /api/sb/underlyings                      index underlyings the store has readings for
  GET  /api/sb/expiries?underlying=             listed expiries at the newest reading (exchange-local dates)
  GET  /api/sb/chain?underlying=&expiry=        one chain at the newest reading (LTP basis, flags, computed IV)
  GET  /api/sb/templates                        recipes + the 'later' list
  POST /api/sb/templates/resolve                {template, underlying, expiry, param?, lots?} -> concrete legs
  POST /api/sb/analyze                          {body} -> the Analyze panel, stamped with input_hash
  POST /api/sb/discover                         {underlying, expiry, view, target|low+high, max_loss?, budget?, hedged_only?}
  GET  /api/sb/strategies                       the caller's library
  POST /api/sb/strategies                       {name?, body?, thesis?}                create
  GET  /api/sb/strategies/{id}                  strategy + draft + snapshots + activity + paper runs
  POST /api/sb/strategies/{id}/draft            {version, body}   autosave; 409 + the stored copy on a stale version
  POST /api/sb/strategies/{id}/meta             {name?, thesis?, tags?}
  POST /api/sb/strategies/{id}/snapshots        {name?}           immutable revision of the current draft
  POST /api/sb/strategies/{id}/restore          {revision_id, version}  a NEW draft version with that snapshot's legs
  POST /api/sb/strategies/{id}/duplicate        {revision_id?}    research-only copy (no paper runs)
  POST /api/sb/strategies/{id}/archive          {archived}
  GET  /api/sb/revisions/{rid}
  POST /api/sb/strategies/{id}/paper            {confirm:true}    snapshot + simulated fills; never a broker order
  GET  /api/sb/paper                            the caller's paper runs, marked at the newest reading
  GET  /api/sb/paper/{run}
  POST /api/sb/paper/{run}/close                {confirm:true}
"""
from __future__ import annotations
import re
from fastapi import APIRouter,Body,Request
from fastapi.responses import JSONResponse
from ..errors import PilotError
from ..screener.routes import _identity
from . import discover as D
from . import service as S
from .market import MarketUnavailable
from .store import Conflict
from .templates import ResolveError,public,recognise,resolve
from .execution import ExecError
from .alerts import AlertError,TYPES as ALERT_TYPES

SYMBOL=re.compile(r'^[A-Z0-9&-]{1,20}$');DATE=re.compile(r'^\d{4}-\d{2}-\d{2}$')
NAME_MAX=80


def _symbol(v):
 s=str(v or '').strip().upper()
 if not SYMBOL.match(s):raise PilotError(400,'FIELD_INVALID','underlying is required.')
 return s
def _date(v):
 s=str(v or '').strip()
 if not DATE.match(s):raise PilotError(400,'FIELD_INVALID','expiry must be YYYY-MM-DD.')
 return s
def _name(v,fallback):
 t=' '.join(str(v or '').split())[:NAME_MAX]
 return t or fallback


def build_router(app,market,store,execution=None,alerts=None):
 r=APIRouter()
 def me(request):return _identity(app,request)
 def own(user,sid):
  s=store.get(user['id'],sid)
  if not s:raise PilotError(404,'STRATEGY_NOT_FOUND','There is no such strategy.')
  return s
 def body_of(raw):
  try:return S.normalize_body(raw)
  except S.Invalid as e:raise PilotError(400,e.code,e.message)
 def guard(fn,*a,**k):
  try:return fn(*a,**k)
  except MarketUnavailable as e:raise PilotError(503,'MARKET_UNAVAILABLE',str(e))

 @r.get('/api/sb/underlyings')
 def underlyings(request:Request):
  me(request);return {'underlyings':guard(market.underlyings)}

 @r.get('/api/sb/expiries')
 def expiries(request:Request,underlying:str=''):
  me(request);return guard(market.expiries,_symbol(underlying))

 @r.get('/api/sb/chain')
 def chain(request:Request,underlying:str='',expiry:str=''):
  me(request);got=guard(market.chain,_symbol(underlying),_date(expiry))
  if not got:raise PilotError(404,'NO_READING','The option store has no reading for this underlying.')
  return got

 @r.get('/api/sb/templates')
 def templates(request:Request):
  me(request);return public()

 @r.post('/api/sb/templates/resolve')
 def template_resolve(request:Request,data:dict=Body(default={})):
  me(request)
  got=guard(market.chain,_symbol(data.get('underlying')),_date(data.get('expiry')))
  if not got:raise PilotError(404,'NO_READING','The option store has no reading for this underlying.')
  try:lots=max(1,min(500,int(data.get('lots') or 1)))
  except (TypeError,ValueError):raise PilotError(400,'FIELD_INVALID','lots must be a whole number.')
  try:return {**resolve(str(data.get('template') or ''),got,data.get('param'),lots),'as_of':got['as_of']}
  except ResolveError as e:raise PilotError(400,e.code,e.message)

 @r.post('/api/sb/analyze')
 def analyze(request:Request,data:dict=Body(default={})):
  me(request);return guard(S.analysis,market,body_of(data.get('body')))

 @r.post('/api/sb/discover')
 def discover(request:Request,data:dict=Body(default={})):
  me(request)
  got=guard(market.chain,_symbol(data.get('underlying')),_date(data.get('expiry')))
  if not got:raise PilotError(404,'NO_READING','The option store has no reading for this underlying.')
  try:return D.run(got,data)
  except D.DiscoverError as e:raise PilotError(400,'DISCOVER_INVALID',e.message)

 # --- library ---------------------------------------------------------------------------------------------------
 @r.get('/api/sb/strategies')
 def strategies(request:Request):
  user=me(request);rows=store.list(user['id'])
  for row in rows:
   s=store.get(user['id'],row['id']);legs=(s['draft'] or {}).get('body',{}).get('legs') or []
   row['structure']=recognise(legs)['name'] if legs else 'Empty'
  return {'strategies':rows}

 @r.post('/api/sb/strategies')
 def create(request:Request,data:dict=Body(default={})):
  user=me(request);body=body_of(data.get('body') or {})
  name=_name(data.get('name'),f"Untitled {body['underlying'] or 'NIFTY'} strategy")
  try:return store.create(user['id'],name,body,thesis=str(data.get('thesis') or '')[:500])
  except ValueError as e:raise PilotError(409,'STRATEGY_LIMIT',str(e))

 @r.get('/api/sb/strategies/{sid}')
 def detail(request:Request,sid:str):
  user=me(request);s=own(user,sid)
  return {**s,'snapshots':store.revisions(user['id'],sid),'activity':store.activity(user['id'],sid),
   'paper':[_paper_row(p) for p in store.papers(user['id'],sid)],
   'deployments':[_dep_row(d) for d in execution.deployments(user['id'],sid)] if execution else [],
   'alerts':alerts.rules(user['id'],sid) if alerts else []}

 @r.post('/api/sb/strategies/{sid}/draft')
 def save_draft(request:Request,sid:str,data:dict=Body(default={})):
  user=me(request);own(user,sid);body=body_of(data.get('body'))
  try:version=int(data.get('version'))
  except (TypeError,ValueError):raise PilotError(400,'FIELD_INVALID','version is required.')
  try:return store.save_draft(user['id'],sid,version,body)
  except Conflict as c:
   return JSONResponse({'error':'This strategy was changed in another window. The newer copy was loaded.','code':'VERSION_CONFLICT',
    'current':c.current},status_code=409)

 @r.post('/api/sb/strategies/{sid}/meta')
 def meta(request:Request,sid:str,data:dict=Body(default={})):
  user=me(request);own(user,sid)
  tags=data.get('tags')
  if tags is not None:tags=[_name(t,'')[:24] for t in (tags if isinstance(tags,list) else [])][:10]
  return store.update_meta(user['id'],sid,_name(data.get('name'),'') or None if 'name' in data else None,
   str(data.get('thesis'))[:500] if data.get('thesis') is not None else None,tags)

 @r.post('/api/sb/strategies/{sid}/snapshots')
 def snapshot(request:Request,sid:str,data:dict=Body(default={})):
  user=me(request);s=own(user,sid);body=s['draft']['body']
  if not body['legs']:raise PilotError(400,'EMPTY_STRATEGY','Add at least one leg before saving a snapshot.')
  a=guard(S.analysis,market,body,table=False)
  a.pop('curve',None)
  return store.snapshot(user['id'],sid,_name(data.get('name'),''),body,a.get('as_of'),a)

 @r.get('/api/sb/revisions/{rid}')
 def revision(request:Request,rid:str):
  user=me(request);rev=store.revision(user['id'],rid)
  if not rev:raise PilotError(404,'SNAPSHOT_NOT_FOUND','There is no such snapshot.')
  return rev

 @r.post('/api/sb/strategies/{sid}/restore')
 def restore(request:Request,sid:str,data:dict=Body(default={})):
  user=me(request);own(user,sid);rev=store.revision(user['id'],str(data.get('revision_id') or ''))
  if not rev or rev['strategy_id']!=sid:raise PilotError(404,'SNAPSHOT_NOT_FOUND','There is no such snapshot.')
  try:return store.save_draft(user['id'],sid,int(data.get('version')),rev['body'])
  except (TypeError,ValueError):raise PilotError(400,'FIELD_INVALID','version is required.')
  except Conflict as c:
   return JSONResponse({'error':'This strategy was changed in another window.','code':'VERSION_CONFLICT','current':c.current},status_code=409)

 @r.post('/api/sb/strategies/{sid}/duplicate')
 def duplicate(request:Request,sid:str,data:dict=Body(default={})):
  user=me(request);s=own(user,sid);body=s['draft']['body']
  rid=data.get('revision_id')
  if rid:
   rev=store.revision(user['id'],str(rid))
   if not rev or rev['strategy_id']!=sid:raise PilotError(404,'SNAPSHOT_NOT_FOUND','There is no such snapshot.')
   body=rev['body']
  try:return store.create(user['id'],_name(s['name']+' (copy)',s['name']),body,thesis=s['thesis'],source=sid)
  except ValueError as e:raise PilotError(409,'STRATEGY_LIMIT',str(e))

 @r.post('/api/sb/strategies/{sid}/archive')
 def archive(request:Request,sid:str,data:dict=Body(default={})):
  user=me(request);own(user,sid);return store.archive(user['id'],sid,data.get('archived',True) is not False)

 # --- paper -----------------------------------------------------------------------------------------------------
 @r.post('/api/sb/strategies/{sid}/paper')
 def paper_start(request:Request,sid:str,data:dict=Body(default={})):
  user=me(request);s=own(user,sid)
  if data.get('confirm') is not True:raise PilotError(400,'CONFIRM_REQUIRED','Confirm the simulated fills to start a paper run.')
  body=s['draft']['body']
  chain,legs,problems=guard(S.hydrate,market,body)
  if not chain or not legs:raise PilotError(400,'EMPTY_STRATEGY','Add at least one priced leg before paper trading.')
  if problems:raise PilotError(400,'UNRESOLVED_CONTRACT',problems[0])
  try:fills=S.paper_fills(chain,legs)
  except S.Invalid as e:raise PilotError(400,e.code,e.message)
  a=S.analysis(market,body,table=False);a.pop('curve',None)
  rev=store.snapshot(user['id'],sid,_name(data.get('name'),'Paper entry'),body,chain['as_of'],a)
  run=store.paper_open(user['id'],sid,rev['id'],S.POLICY,chain['as_of'],fills)
  return S.paper_view(market,store,user['id'],run['id'])

 @r.get('/api/sb/paper')
 def paper_list(request:Request):
  user=me(request)
  names={x['id']:x['name'] for x in store.list(user['id'])}
  out=[]
  for p in store.papers(user['id']):
   v=guard(S.paper_view,market,store,user['id'],p['id'])
   if v:v['strategy_name']=names.get(v['strategy_id']);out.append(v)
  return {'runs':out}

 @r.get('/api/sb/paper/{run}')
 def paper_get(request:Request,run:str):
  user=me(request);v=guard(S.paper_view,market,store,user['id'],run)
  if not v:raise PilotError(404,'PAPER_NOT_FOUND','There is no such paper run.')
  return v

 @r.post('/api/sb/paper/{run}/close')
 def paper_close(request:Request,run:str,data:dict=Body(default={})):
  user=me(request)
  if data.get('confirm') is not True:raise PilotError(400,'CONFIRM_REQUIRED','Confirm the simulated exit to close this paper run.')
  p=store.paper(user['id'],run)
  if not p:raise PilotError(404,'PAPER_NOT_FOUND','There is no such paper run.')
  if p['status']!='open':raise PilotError(409,'PAPER_CLOSED','This paper run is already closed.')
  rev=store.revision(user['id'],p['revision_id'])
  chain,legs,problems=guard(S.hydrate,market,rev['body'])
  if problems or not chain:raise PilotError(400,'UNRESOLVED_CONTRACT',(problems or ['No reading.'])[0])
  try:fills=S.paper_fills(chain,legs,opening=False)
  except S.Invalid as e:raise PilotError(400,e.code,e.message)
  store.paper_close(user['id'],run,fills,chain['as_of'])
  return S.paper_view(market,store,user['id'],run)

 # --- slice 4: order review, intents, paper deployments ---------------------------------------------------------------
 def ex(fn,*a,**k):
  if not execution:raise PilotError(503,'EXECUTION_UNAVAILABLE','Order review is not available on this server.')
  try:return guard(fn,*a,**k)
  except ExecError as e:
   if e.extra:return JSONResponse({'error':e.message,'code':e.code,**e.extra},status_code=e.status)
   raise PilotError(e.status,e.code,e.message)

 @r.get('/api/sb/status')
 def status(request:Request):
  me(request);st=market.status() if hasattr(market,'status') else {'live':False,'source':'stored'}
  from .execution import LIVE_CAPABILITY,market_open,now_ist
  return {**st,'market_open':market_open(),'now_ist':now_ist().strftime('%Y-%m-%d %H:%M:%S'),'live_orders':LIVE_CAPABILITY,
   'paper_capital':execution.paper_capital(me(request)['id']) if execution else None}

 @r.post('/api/sb/strategies/{sid}/preview')
 def preview(request:Request,sid:str,data:dict=Body(default={})):
  user=me(request);s=own(user,sid)
  return ex(execution.preview,user['id'],s,{'product':data.get('product'),'price_policy':data.get('price_policy'),'limits':data.get('limits') or {}})

 @r.post('/api/sb/strategies/{sid}/deployments')
 def deploy(request:Request,sid:str,data:dict=Body(default={})):
  user=me(request);s=own(user,sid)
  if data.get('confirm') is not True:raise PilotError(400,'CONFIRM_REQUIRED','Confirm the exact order plan to place paper orders.')
  return ex(execution.confirm,user['id'],s,str(data.get('preview_id') or ''),str(data.get('preview_hash') or ''),
   str(data.get('idempotency_key') or ''),data.get('ack_unlimited') is True)

 @r.get('/api/sb/deployments')
 def deployments(request:Request):
  user=me(request)
  names={x['id']:x['name'] for x in store.list(user['id'])}
  out=[]
  for d in (execution.deployments(user['id']) if execution else []):
   d['strategy_name']=names.get(d['strategy_id']);d.pop('revision_body',None);out.append(d)
  return {'deployments':out,'paper_capital':execution.paper_capital(user['id']) if execution else None}

 @r.get('/api/sb/deployments/{did}')
 def deployment(request:Request,did:str):
  user=me(request);d=ex(execution.deployment,user['id'],did)
  if not d:raise PilotError(404,'DEPLOYMENT_NOT_FOUND','There is no such deployment.')
  d.pop('revision_body',None);return d

 @r.post('/api/sb/deployments/{did}/cancel')
 def cancel(request:Request,did:str,data:dict=Body(default={})):
  user=me(request);d=ex(execution.cancel,user['id'],did);d.pop('revision_body',None);return d

 @r.post('/api/sb/deployments/{did}/close-preview')
 def close_preview(request:Request,did:str,data:dict=Body(default={})):
  user=me(request);d=execution.deployment(user['id'],did) if execution else None
  if not d:raise PilotError(404,'DEPLOYMENT_NOT_FOUND','There is no such deployment.')
  s=own(user,d['strategy_id'])
  return ex(execution.preview,user['id'],s,{'product':d['product'],'price_policy':data.get('price_policy')},kind='close',deployment=d)

 @r.post('/api/sb/deployments/{did}/close')
 def close(request:Request,did:str,data:dict=Body(default={})):
  user=me(request);d=execution.deployment(user['id'],did,mark=False) if execution else None
  if not d:raise PilotError(404,'DEPLOYMENT_NOT_FOUND','There is no such deployment.')
  if data.get('confirm') is not True:raise PilotError(400,'CONFIRM_REQUIRED','Confirm the exact close orders.')
  s=own(user,d['strategy_id']);out=ex(execution.confirm,user['id'],s,str(data.get('preview_id') or ''),str(data.get('preview_hash') or ''),
   str(data.get('idempotency_key') or ''));out.pop('revision_body',None);return out

 # --- slice 5: alerts (notify only) --------------------------------------------------------------------------------------
 def al(fn,*a,**k):
  if not alerts:raise PilotError(503,'ALERTS_UNAVAILABLE','Alerts are not available on this server.')
  try:return guard(fn,*a,**k)
  except AlertError as e:raise PilotError(e.status,e.code,e.message)

 @r.get('/api/sb/alerts')
 def alerts_all(request:Request):
  user=me(request)
  names={x['id']:x['name'] for x in store.list(user['id'])}
  rules=al(alerts.rules,user['id']);events=al(alerts.events,user['id'],None,100)
  for x in rules+events:x['strategy_name']=names.get(x['strategy_id'])
  return {'rules':rules,'events':events,'unacked':alerts.unacked(user['id']),
   'types':[{'key':k,**v,'scopes':list(v['scopes'])} for k,v in ALERT_TYPES.items()],'boundary':'Alerts notify only. They never place, modify or cancel an order.'}

 @r.get('/api/sb/alerts/unacked')
 def alerts_unacked(request:Request):
  user=me(request);return {'unacked':alerts.unacked(user['id']) if alerts else 0,'latest':(alerts.events(user['id'],None,5,unacked=True) if alerts else [])}

 @r.get('/api/sb/strategies/{sid}/alerts')
 def alerts_for(request:Request,sid:str):
  user=me(request);own(user,sid)
  return {'rules':al(alerts.rules,user['id'],sid),'events':al(alerts.events,user['id'],sid,50)}

 @r.post('/api/sb/strategies/{sid}/alerts')
 def alert_create(request:Request,sid:str,data:dict=Body(default={})):
  user=me(request);own(user,sid);dep=None
  if data.get('deployment_id'):
   dep=execution.deployment(user['id'],str(data['deployment_id']),mark=False) if execution else None
   if not dep or dep['strategy_id']!=sid:raise PilotError(404,'DEPLOYMENT_NOT_FOUND','There is no such deployment for this strategy.')
  rule=al(alerts.create,user['id'],sid,data,dep)
  return {**rule,'now':al(alerts.evaluate_rule,user['id'],rule,None,False)}

 @r.post('/api/sb/alerts/{rid}')
 def alert_update(request:Request,rid:str,data:dict=Body(default={})):
  user=me(request);return al(alerts.update,user['id'],rid,data)

 @r.post('/api/sb/alerts/{rid}/delete')
 def alert_delete(request:Request,rid:str,data:dict=Body(default={})):
  user=me(request);return al(alerts.delete,user['id'],rid)

 @r.post('/api/sb/alerts/{rid}/check')
 def alert_check(request:Request,rid:str,data:dict=Body(default={})):
  user=me(request);rule=al(alerts.rule,user['id'],rid)
  if not rule:raise PilotError(404,'ALERT_NOT_FOUND','There is no such alert.')
  return al(alerts.evaluate_rule,user['id'],rule,None,False)

 @r.post('/api/sb/alert-events/ack')
 def alert_ack(request:Request,data:dict=Body(default={})):
  user=me(request);return al(alerts.ack,user['id'],str(data['event_id']) if data.get('event_id') else None)

 return r


def _paper_row(p):
 return {k:p[k] for k in ('id','status','revision_id','opened_reading','closed_reading','created_at','closed_at')}


def _dep_row(d):
 return {k:d.get(k) for k in ('id','status','mode','product','opened_at','closed_at','net','realised','unrealised','fees','marked_at')}
