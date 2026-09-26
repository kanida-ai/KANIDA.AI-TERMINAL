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
from datetime import date
from . import discover as D
from . import analytics as A
from . import service as S
from .market import MarketUnavailable
from .store import Conflict
from .templates import ResolveError,public,recognise,resolve
from .execution import ExecError
from .alerts import AlertError,TYPES as ALERT_TYPES
from .lab import LabError

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


def build_router(app,market,store,execution=None,alerts=None,lab=None,autotrade=None):
 r=APIRouter()
 discover_results=D.Results()
 def me(request):return _identity(app,request)
 def own(user,sid):
  s=store.get(user['id'],sid)
  if not s:raise PilotError(404,'STRATEGY_NOT_FOUND','There is no such strategy.')
  return s
 def body_of(raw):
  try:return S.normalize_body(raw)
  except S.Invalid as e:raise PilotError(400,e.code,e.message)
 def ticks(body):
  try:S.check_ticks(market,body)
  except S.Invalid as e:raise PilotError(400,e.code,e.message)
 def guard(fn,*a,**k):
  try:return fn(*a,**k)
  except MarketUnavailable as e:raise PilotError(503,'MARKET_UNAVAILABLE',str(e))

 @r.get('/api/sb/ops/metrics')
 def ops_metrics(request:Request,days:int=7):
  user=me(request)
  if user.get('role')!='owner':raise PilotError(403,'OWNER_ONLY','Release metrics are visible to the owner only.')
  ops=getattr(app.state,'strategy_builder_ops',None)
  if not ops:raise PilotError(503,'OPS_UNAVAILABLE','Metrics are not available on this server.')
  return ops.metrics(max(1,min(90,days)))

 @r.get('/api/sb/calendar')
 def exchange_calendar(request:Request):
  me(request);from . import exchange as XC
  return {**XC.public(),'now':XC.session()}

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
  return _with_greeks(got)

 def _with_greeks(ch):
  """Per-strike model Greeks (per unit) from each option's own solved IV at the reading - the chain's Greeks view."""
  t=max(float(ch.get('days_to_expiry') or 0),0.0)/365.0;spot=ch.get('spot')
  if not spot or t<=0:return {**ch,'greeks_basis':'unavailable at expiry'}
  rows=[]
  for r in ch['rows']:
   nr=dict(r)
   for k in ('CE','PE'):
    x=r.get(k)
    if x and x.get('iv'):
     g=A.bs_greeks(spot,r['strike'],t,x['iv']/100.0,k)
     nr[k]={**x,'greeks':{'delta':round(g['delta'],3),'gamma':round(g['gamma'],5),'theta':round(g['theta'],2),'vega':round(g['vega'],2)}}
   rows.append(nr)
  return {**ch,'rows':rows,'greeks_basis':'model BSM per unit, each option at its own IV, at the reading'}

 @r.get('/api/sb/spreads')
 def spreads(request:Request,underlying:str='',expiry:str='',type:str='CE',side:str='debit',width:int=2,lots:int=1):
  me(request)
  from . import spreads as SP
  got=guard(market.chain,_symbol(underlying),_date(expiry))
  if not got:raise PilotError(404,'NO_READING','The option store has no reading for this underlying.')
  try:return SP.build(got,type,side,int(width),int(lots))
  except SP.SpreadsError as e:raise PilotError(400,'FIELD_INVALID',e.message)

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
  key=str(data.get('template') or '');far=None
  from .templates import BY_KEY as _TB
  if (_TB.get(key) or {}).get('multi_expiry'):
   fe=data.get('far_expiry')
   if fe and str(fe)<=got['expiry']:raise PilotError(400,'FIELD_INVALID','far_expiry must be later than the near expiry.')
   if not fe:
    exps=guard(market.expiries,got['underlying'])['expiries']
    from datetime import date as _d
    later=[x['expiry'] for x in exps if (_d.fromisoformat(x['expiry'])-_d.fromisoformat(got['expiry'])).days>=6]
    fe=later[0] if later else None
   far=guard(market.chain,got['underlying'],_date(fe)) if fe else None
  try:return {**resolve(key,got,data.get('param'),lots,far),'as_of':got['as_of']}
  except ResolveError as e:raise PilotError(400,e.code,e.message)

 @r.post('/api/sb/analyze')
 def analyze(request:Request,data:dict=Body(default={})):
  me(request);body=body_of(data.get('body'));ticks(body);return guard(S.analysis,market,body)

 @r.post('/api/sb/discover')
 def discover(request:Request,data:dict=Body(default={})):
  me(request)
  got=guard(market.chain,_symbol(data.get('underlying')),_date(data.get('expiry')))
  if not got:raise PilotError(404,'NO_READING','The option store has no reading for this underlying.')
  try:out=D.run(got,data)
  except D.DiscoverError as e:raise PilotError(400,'DISCOVER_INVALID',e.message)
  if lab and (got['underlying']=='NIFTY' or got['underlying'] in lab.stocks()):
   from . import evidence as EV
   b=lab.evidence_board(me(request)['id'])
   for c in out['candidates']:
    ev=EV.for_candidate(b,c['template'],c['param'],EV.next_session_dte(got['expiry'],got['as_of']),got['underlying'])   # counted as the Lab counts: from the next session
    if ev:c['evidence']=ev
   out['candidates']=EV.rank(out['candidates'])
   fam=b['families'].get(EV.family_of(got['underlying']),{'tests':0,'survivors':0})
   out['evidence']={'tests':fam['tests'],'survivors':fam['survivors'],'fdr_q':b['fdr_q'],'min_oos':b['min_oos']}
   out['basis']=('Tier 1: "Tested ✓" defined-risk rules (surviving Benjamini-Hochberg FDR 10% across every distinct rule you tested) whose out-of-sample '
    '95% low return per ₹100 of maximum model loss is above zero, by that low. Tier 2: everything else by expiry P&L at your view divided by capital '
    'at risk (model, at this reading). Prices are last traded (LTP); the draft reprices at buy-at-ask / sell-at-bid where quotes exist. Not a forecast.')
  return discover_results.put(me(request)['id'],{**data,'underlying':got['underlying'],'expiry':got['expiry']},out)

 @r.post('/api/sb/discover/use')
 def discover_use(request:Request,data:dict=Body(default={})):
  """Create a draft from a server-held candidate: its own terms, never the current form (GTM audit P03)."""
  user=me(request)
  try:body,c,thesis=discover_results.draft_body(user['id'],data.get('candidate_id'))
  except D.DiscoverError as e:raise PilotError(409,'CANDIDATE_GONE',e.message)
  from .format_util import dm
  nb=body_of(body)
  try:s=store.create(user['id'],_name(f"{body['underlying']} {c['name']} {dm(body['expiry'])}",c['name']),nb,thesis=thesis)
  except ValueError as e:raise PilotError(409,'STRATEGY_LIMIT',str(e))
  return {**s,'candidate_id':c['candidate_id']}

 @r.get('/api/sb/strategies/{sid}/evidence')
 def strategy_evidence(request:Request,sid:str):
  """The Lab evidence behind THIS strategy's structure (GTM/A8): the same matching rules as Discover, plus the
  evidence version and the deciding run's date, so a stale or mismatched result is visible where the strategy is built."""
  user=me(request);s=own(user,sid);body=s['draft']['body'];legs=[l for l in body.get('legs',[]) if l.get('include',True)]
  from . import evidence as EV
  if not legs:return {'status':'none','label':'No legs yet'}
  st=recognise([{**l,'lot_size':1} for l in legs])
  if not st.get('exact') or not st.get('key'):return {'status':'none','label':'Custom structure - no Lab rule describes it','structure':st.get('name')}
  u=body.get('underlying') or 'NIFTY'
  if not lab or not (u=='NIFTY' or u in lab.stocks()):return {'status':'none','label':f'The Lab does not test {u} yet','structure':st['name']}
  try:got=market.reading(u);as_of=got[0] if got else None
  except Exception:as_of=None  # noqa: BLE001
  dte=EV.next_session_dte(body['expiry'],as_of) if as_of and body.get('expiry') else None
  param=body.get('param') if body.get('template')==st['key'] else None
  b=lab.evidence_board(user['id'])
  ev=EV.for_candidate(b,st['key'],param,dte,u)
  if not ev:return {'status':'model_only','label':'Not tested in your Lab yet','structure':st['name'],'template':st['key'],'param':param,'evidence_version':EV.EVIDENCE_VERSION}
  run=lab.run(user['id'],ev['run_id'],full=False) if ev.get('run_id') else None
  return {**ev,'structure':st['name'],'template':st['key'],'param':param,'dte':dte,'evidence_version':EV.EVIDENCE_VERSION,
   'deciding_run_at':run['created_at'] if run else None}

 @r.get('/api/sb/lab/universe')
 def lab_universe(request:Request):
  me(request)
  return {'underlyings':(['NIFTY']+sorted(lab.stocks())) if lab else [],'lot_note':'Each test trades ONE set of the structure at the current lot size; results scale linearly with lots.'}

 @r.get('/api/sb/lab/batches')
 def lab_batches(request:Request):
  if not lab:return {'batches':[]}
  from . import experiments as XP
  return {'batches':XP.batches(lab,me(request)['id']),'grids':{k:{kk:v[kk] for kk in ('name','underlying','why')} for k,v in XP.GRIDS.items()}}

 @r.get('/api/sb/lab/batches/{bid}')
 def lab_batch(request:Request,bid:str):
  from . import experiments as XP
  b=XP.batch(lab,me(request)['id'],bid) if lab else None
  if not b:raise PilotError(404,'BATCH_NOT_FOUND','There is no such experiment batch.')
  return b

 @r.get('/api/sb/lab/evidence')
 def evidence_board(request:Request):
  if not lab:return {'rules':[],'tests':0,'survivors':0}
  b=lab.evidence_board(me(request)['id']);b.pop('by_run',None);return b

 # --- library ---------------------------------------------------------------------------------------------------
 @r.get('/api/sb/strategies')
 def strategies(request:Request,sort:str='updated'):
  user=me(request);rows=store.list(user['id'])
  from .exchange import now_ist
  today=now_ist().strftime('%Y-%m-%d');deps=execution.summaries(user['id']) if execution else {}
  for row in rows:
   s=store.get(user['id'],row['id']);legs=(s['draft'] or {}).get('body',{}).get('legs') or []
   row['structure']=recognise(legs)['name'] if legs else 'Empty'
   # badges (GTM P18): what state the saved terms are in, and whether paper money is riding on them
   row['badges']=(['expired'] if row.get('expiry') and row['expiry']<today else [])+(['empty'] if not legs else [])
   row['deployments']=deps.get(row['id'],{'open':0,'attention':0,'closed':0})
  key={'name':lambda r:(r['name'] or '').lower(),'expiry':lambda r:r.get('expiry') or '9999','created':lambda r:-(r.get('created_at') or 0),
   'updated':lambda r:-(r.get('updated_at') or 0)}.get(sort)
  if not key:raise PilotError(400,'FIELD_INVALID','sort must be one of updated, created, name, expiry.')
  rows.sort(key=key)
  return {'strategies':rows,'sort':sort}

 @r.post('/api/sb/strategies')
 def create(request:Request,data:dict=Body(default={})):
  user=me(request);body=body_of(data.get('body') or {});ticks(body)
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
  user=me(request);own(user,sid);body=body_of(data.get('body'));ticks(body)
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

 def expect_draft(s,data):
  """GTM audit P01: an action taken on a draft names the version (and optionally the body checksum) the user was
  looking at. If the server holds anything else, the action is refused instead of acting on the wrong body."""
  ev=data.get('expected_version')
  if ev is None:return
  try:ev=int(ev)
  except (TypeError,ValueError):raise PilotError(400,'FIELD_INVALID','expected_version must be a whole number.')
  h=data.get('input_hash')
  if s['draft']['version']!=ev or (h and h!=s['draft']['checksum']):
   raise PilotError(409,'DRAFT_CHANGED','The saved draft is not the version on your screen. Save your latest edits and try again.')

 @r.post('/api/sb/strategies/{sid}/snapshots')
 def snapshot(request:Request,sid:str,data:dict=Body(default={})):
  user=me(request);s=own(user,sid);body=s['draft']['body']
  req=str(data.get('request_id') or '')[:64] or None
  if req:
   done=store.snapshot_for_request(user['id'],sid,req)
   if done:return {**done,'repeated':True}
  expect_draft(s,data)
  if not body['legs']:raise PilotError(400,'EMPTY_STRATEGY','Add at least one leg before saving a snapshot.')
  a=guard(S.analysis,market,body,table=False)
  a.pop('curve',None)
  snap=store.snapshot(user['id'],sid,_name(data.get('name'),''),body,a.get('as_of'),a,req)
  return snap if snap.get('repeated') else {**snap,'draft_version':s['draft']['version']}

 @r.post('/api/sb/revisions/{rid}')
 def revision_meta(request:Request,rid:str,data:dict=Body(default={})):
  user=me(request)
  name=_name(data.get('name'),'') if 'name' in data else None
  if name is not None and not name:raise PilotError(400,'FIELD_INVALID','A snapshot name cannot be empty.')
  notes=str(data.get('notes'))[:2000] if data.get('notes') is not None else None
  rev=store.update_revision(user['id'],rid,name,notes)
  if not rev:raise PilotError(404,'SNAPSHOT_NOT_FOUND','There is no such snapshot.')
  return rev

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
  if not rid:expect_draft(s,data)
  if rid:
   rev=store.revision(user['id'],str(rid))
   if not rev or rev['strategy_id']!=sid:raise PilotError(404,'SNAPSHOT_NOT_FOUND','There is no such snapshot.')
   body=rev['body']
  try:return store.create(user['id'],_name(s['name']+' (copy)',s['name']),body,thesis=s['thesis'],source=sid)
  except ValueError as e:raise PilotError(409,'STRATEGY_LIMIT',str(e))

 @r.post('/api/sb/strategies/{sid}/archive')
 def archive(request:Request,sid:str,data:dict=Body(default={})):
  user=me(request);own(user,sid);return store.archive(user['id'],sid,data.get('archived',True) is not False)

 # --- slice 14: research consent + the user's own decision history ----------------------------------------------
 @r.get('/api/sb/consent')
 def consent_get(request:Request):
  user=me(request);return {'consent':store.consent(user['id']),'policy_version':store.CONSENT_POLICY,
   'explain':'If you agree, your strategies, paper trades and adjustments are used - without your name or account - to measure which adjustments help or hurt across many traders. You can change this at any time; it applies to new records.'}

 @r.post('/api/sb/consent')
 def consent_set(request:Request,data:dict=Body(default={})):
  user=me(request)
  if not isinstance(data.get('consent'),bool):raise PilotError(400,'FIELD_INVALID','consent must be true or false.')
  return store.set_consent(user['id'],data['consent'])

 @r.get('/api/sb/decisions')
 def decisions(request:Request,strategy_id:str|None=None,limit:int=200):
  user=me(request)
  if strategy_id:own(user,strategy_id)
  return {'decisions':store.decisions(user['id'],strategy_id,max(1,min(1000,limit)))}

 # --- paper -----------------------------------------------------------------------------------------------------
 @r.post('/api/sb/strategies/{sid}/paper')
 def paper_start(request:Request,sid:str,data:dict=Body(default={})):
  user=me(request);s=own(user,sid);expect_draft(s,data)
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

 # --- slice 7: hand a reviewed plan to engine autotrade (dry run by default; live only when autotrade's gates pass) ---
 def _live(user_id):
  from .execution import LIVE_CAPABILITY
  if not autotrade:return {**LIVE_CAPABILITY,'configured':False,'live_allowed':False,'gates':[]}
  cap=autotrade.bridge.capability(user_id)
  return {**cap,'enabled':bool(cap['configured'] and cap['reachable'])}

 @r.get('/api/sb/autotrade/capability')
 def at_capability(request:Request):
  return _live(me(request)['id'])

 @r.post('/api/sb/strategies/{sid}/autotrade')
 def at_route(request:Request,sid:str,data:dict=Body(default={})):
  if not autotrade:raise PilotError(503,'AUTOTRADE_UNAVAILABLE','AutoTrade hand-off is not available on this server.')
  user=me(request);s=own(user,sid)
  mode='live' if data.get('mode')=='live' else 'dry_run'
  if mode=='live' and data.get('confirm_live') is not True:
   raise PilotError(400,'LIVE_CONFIRM_REQUIRED','A live request needs the explicit real-orders confirmation.')
  return ex(autotrade.route,user['id'],s,str(data.get('preview_id') or ''),str(data.get('preview_hash') or ''),
   str(data.get('idempotency_key') or ''),mode)

 @r.get('/api/sb/autotrade/routes')
 def at_routes(request:Request,strategy_id:str|None=None):
  if not autotrade:return {'routes':[]}
  return {'routes':autotrade.list(me(request)['id'],strategy_id)}

 @r.get('/api/sb/autotrade/routes/{rid}')
 def at_get(request:Request,rid:str):
  d=autotrade.get(me(request)['id'],rid) if autotrade else None
  if not d:raise PilotError(404,'ROUTE_NOT_FOUND','There is no such AutoTrade hand-off.')
  return d

 @r.post('/api/sb/autotrade/routes/{rid}/release')
 def at_release(request:Request,rid:str,data:dict=Body(default={})):
  d=ex(autotrade.release,me(request)['id'],rid,data.get('confirm') is True) if autotrade else None
  if not d:raise PilotError(404,'ROUTE_NOT_FOUND','There is no such AutoTrade hand-off.')
  return d

 @r.post('/api/sb/autotrade/routes/{rid}/cancel')
 def at_cancel(request:Request,rid:str):
  d=ex(autotrade.cancel,me(request)['id'],rid) if autotrade else None
  if not d:raise PilotError(404,'ROUTE_NOT_FOUND','There is no such AutoTrade hand-off.')
  return d

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
  from . import exchange as XC
  return {**st,'market_open':market_open(),'session':XC.session(),'calendar':XC.coverage(),'now_ist':now_ist().strftime('%Y-%m-%d %H:%M:%S'),'live_orders':_live(me(request)['id']),
   'paper_capital':execution.paper_capital(me(request)['id']) if execution else None}

 @r.post('/api/sb/strategies/{sid}/preview')
 def preview(request:Request,sid:str,data:dict=Body(default={})):
  user=me(request);s=own(user,sid);expect_draft(s,data)
  out=ex(execution.preview,user['id'],s,{'product':data.get('product'),'price_policy':data.get('price_policy'),'limits':data.get('limits') or {}})
  if isinstance(out,dict):out['live']=_live(user['id'])
  return out

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

 @r.post('/api/sb/deployments/{did}/exit-rules')
 def deployment_exit_rules(request:Request,did:str,data:dict=Body(default={})):
  user=me(request)
  def num(k):
   v=data.get(k)
   if v in (None,''):return None
   try:return float(v)
   except (TypeError,ValueError):raise PilotError(400,'FIELD_INVALID',f'{k} must be a number.')
  return ex(execution.set_exit_rules,user['id'],did,num('target_pct'),num('stop_pct'))

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

 # --- slice 8: the adjustment assistant (K12) - candidates, apply as a new version, delta orders for a deployment ------
 def _adjust_base(user,s,did):
  from . import assistant as AS
  if did:
   d=execution.deployment(user['id'],did) if execution else None
   if not d or d['strategy_id']!=s['id']:raise PilotError(404,'DEPLOYMENT_NOT_FOUND','There is no such deployment for this strategy.')
   try:return AS.position_body(d),True,d
   except AS.AssistError as e:raise PilotError(e.status,e.code,e.message)
  return s['draft']['body'],False,None

 def _candidates(user,s,did):
  from . import assistant as AS
  body,held,d=_adjust_base(user,s,did)
  ev=(lambda t,p,rule,k:lab.adjust_evidence_for(user['id'],t,p,rule,k)) if lab else None
  with store.lock:
   if d:before=bool(store.c.execute("select 1 from deployment_revisions where deployment_id=? and cause='adjust'",(d['id'],)).fetchone())
   else:before=bool(store.c.execute("select 1 from activity where strategy_id=? and kind='adjust'",(s['id'],)).fetchone())
  try:out=guard(AS.candidates,market,body,ev,held,before)
  except AS.AssistError as e:raise PilotError(e.status,e.code,e.message)
  out['deployment_id']=did or None;out['draft_version']=s['draft']['version']
  return out

 @r.post('/api/sb/strategies/{sid}/adjust/candidates')
 def adjust_candidates(request:Request,sid:str,data:dict=Body(default={})):
  user=me(request);s=own(user,sid)
  out=_candidates(user,s,str(data.get('deployment_id') or '') or None)
  for c in out['candidates']:c.pop('body',None)          # the server recomputes on apply; the client never sends legs
  return out

 @r.post('/api/sb/strategies/{sid}/adjust/apply')
 def adjust_apply(request:Request,sid:str,data:dict=Body(default={})):
  user=me(request);s=own(user,sid);did=str(data.get('deployment_id') or '') or None
  try:version=int(data.get('version'))
  except (TypeError,ValueError):raise PilotError(400,'FIELD_INVALID','version is required.')
  rule=str(data.get('rule') or '');k=data.get('k')
  out=_candidates(user,s,did)
  c=next((c for c in out['candidates'] if c['rule']==rule and (c.get('k') or None)==(int(k) if k not in (None,'') else None)),None)
  if not c:raise PilotError(404,'ADJUSTMENT_NOT_FOUND','There is no such adjustment for this strategy now.')
  if not c.get('available'):raise PilotError(409,'ADJUSTMENT_NOT_APPLICABLE',c.get('reason') or 'Not applicable now.')
  body=body_of(c['body'])
  try:st=store.save_draft(user['id'],sid,version,body)
  except Conflict as cf:
   return JSONResponse({'error':'This strategy was changed in another window. Review the adjustment again.','code':'VERSION_CONFLICT','current':cf.current},status_code=409)
  store._log(user['id'],sid,'adjust',f"{c['name']}: {c['note']}"+(f" (paper deployment {did[:6]} - review the delta orders)" if did else ''))
  c.pop('body',None);c.pop('overlay',None)
  return {'strategy':st,'adjustment':c,'deployment_id':did}

 @r.post('/api/sb/deployments/{did}/adjust-preview')
 def adjust_preview(request:Request,did:str,data:dict=Body(default={})):
  user=me(request);d=execution.deployment(user['id'],did) if execution else None
  if not d:raise PilotError(404,'DEPLOYMENT_NOT_FOUND','There is no such deployment.')
  s=own(user,d['strategy_id'])
  out=ex(execution.preview,user['id'],s,{'product':d['product'],'price_policy':data.get('price_policy'),'limits':data.get('limits') or {}},kind='adjust',deployment=d)
  if isinstance(out,dict):out['live']=_live(user['id'])
  return out

 @r.post('/api/sb/deployments/{did}/adjust')
 def adjust_confirm(request:Request,did:str,data:dict=Body(default={})):
  user=me(request);d=execution.deployment(user['id'],did,mark=False) if execution else None
  if not d:raise PilotError(404,'DEPLOYMENT_NOT_FOUND','There is no such deployment.')
  if data.get('confirm') is not True:raise PilotError(400,'CONFIRM_REQUIRED','Confirm the exact adjustment orders.')
  s=own(user,d['strategy_id']);out=ex(execution.confirm,user['id'],s,str(data.get('preview_id') or ''),str(data.get('preview_hash') or ''),
   str(data.get('idempotency_key') or ''),bool(data.get('ack_unlimited')));out.pop('revision_body',None);return out

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

 # --- slice 6: the Lab -------------------------------------------------------------------------------------------------
 def lb(fn,*a,**k):
  if not lab:raise PilotError(503,'LAB_UNAVAILABLE','The Lab is not available on this server.')
  try:return guard(fn,*a,**k)
  except LabError as e:raise PilotError(e.status,e.code,e.message)

 @r.post('/api/sb/lab/backtests')
 def lab_backtest(request:Request,data:dict=Body(default={})):
  user=me(request);sid=data.get('strategy_id')
  if sid:own(user,str(sid))
  return lb(lab.start_backtest,user['id'],data,str(sid) if sid else None)

 @r.get('/api/sb/lab/runs')
 def lab_runs(request:Request,strategy_id:str=''):
  user=me(request);runs=lb(lab.runs,user['id'],strategy_id or None)
  st=lab.evidence_board(user['id'])['by_run']
  for x in runs:
   e=st.get(x['id'])
   if e:x['evidence']={'status':e['status'],'p':e['p'],'tests':e['tests'],'n_oos':e['n_oos'],'low':e['low'],'runs_of_rule':e['runs'],'decides':e['deciding_run']==x['id']}
  return {'runs':runs}

 @r.post('/api/sb/lab/runs/{rid}/cancel')
 def lab_cancel(request:Request,rid:str):
  user=me(request);return lb(lab.cancel,user['id'],rid)

 @r.get('/api/sb/lab/runs/{rid}')
 def lab_run(request:Request,rid:str):
  user=me(request);run=lb(lab.run,user['id'],rid)
  if not run:raise PilotError(404,'RUN_NOT_FOUND','There is no such Lab run.')
  return run

 @r.post('/api/sb/strategies/{sid}/replay')
 def lab_replay(request:Request,sid:str,data:dict=Body(default={})):
  user=me(request);s=own(user,sid)
  interval=data.get('interval') if data.get('interval') in ('5minute','15minute','60minute','day') else '15minute'
  try:days=max(1,min(60,int(data.get('days') or 10)))
  except (TypeError,ValueError):raise PilotError(400,'FIELD_INVALID','days must be a whole number.')
  return lb(lab.replay_strategy,user['id'],s,interval,days)

 return r


def _paper_row(p):
 return {k:p[k] for k in ('id','status','revision_id','opened_reading','closed_reading','created_at','closed_at')}


def _dep_row(d):
 return {k:d.get(k) for k in ('id','status','mode','product','opened_at','closed_at','net','realised','unrealised','fees','marked_at')}
