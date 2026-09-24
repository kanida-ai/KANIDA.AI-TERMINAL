"""/api/screener/* — the screener's API. Member-only, owner-scoped, GET + POST (the pilot's CORS allows only those).

  GET  /api/screener/vocabulary            the closed vocabulary the builder draws
  GET  /api/screener/status                the newest reading the store holds
  GET  /api/screener/scanners              KANIDA defaults + the caller's own scanners, with alert settings
  POST /api/screener/scanners              create {name, definition, source, nl_text}
  POST /api/screener/scanners/{id}         update {name?, definition?, nl_text?}      (own scanners only)
  POST /api/screener/scanners/{id}/duplicate                                         (defaults or own)
  POST /api/screener/scanners/{id}/delete                                            (own scanners only)
  POST /api/screener/scanners/{id}/notify  {new, ended, changed}
  GET  /api/screener/scanners/{id}/results ?view=active|ended|all&limit=
  POST /api/screener/describe              {definition} -> normalized definition + "Reads as"
  POST /api/screener/parse                 {text} -> definition + what was understood / assumed / unmapped
  POST /api/screener/run                   {definition, view?, limit?} -> results for an UNSAVED draft
  GET  /api/screener/alerts                the caller's notifications
  POST /api/screener/alerts/read           {ids?}  (no ids = all)

A scanner that is not the caller's and is not a default answers 404, never 403: another user's scanner does
not exist as far as this caller can tell.
"""
from __future__ import annotations
import hmac
from fastapi import APIRouter,Body,Request
from ..errors import PilotError
from . import definition as D,vocab as V

COOKIE='kanida_session'
NAME_MAX=60
VIEWS=('active','ended','all')


def _identity(app,request):
 """app.py's identity() + member(), re-stated here because those are closures inside create_app. Same rules:
 bearer token (native) or the session cookie (web), CSRF on every non-GET web request, onboarding and access."""
 auth,billing=app.state.auth,app.state.billing
 header=request.headers.get('authorization','');native=header.startswith('Bearer ')
 raw=header[7:] if native else request.cookies.get(COOKIE,'')
 result=auth.resolve(raw,'native' if native else 'web')
 if not result:raise PilotError(401,'SIGN_IN_REQUIRED','Sign in to your private pilot account.')
 user,session=result
 if request.method!='GET' and not native:
  if not hmac.compare_digest(request.headers.get('x-kanida-csrf',''),session['csrf']):
   raise PilotError(403,'CSRF','Refresh the page before continuing.')
 if not user['onboarded']:raise PilotError(403,'ONBOARDING_REQUIRED','Complete your pilot setup first.')
 if not billing.status(user)['access']:raise PilotError(402,'ACCESS_REQUIRED','Your pilot subscription needs attention.')
 return user


def _name(value,fallback='My scanner'):
 text=' '.join(str(value or '').split())[:NAME_MAX]
 return text or fallback


def _definition(raw):
 try:return D.normalize(raw)
 except (D.DefinitionError,TypeError,ValueError) as error:raise PilotError(400,'SCANNER_INVALID',str(error))


def _view(payload,view,limit):
 if view not in VIEWS:raise PilotError(400,'FIELD_INVALID','view must be active, ended or all.')
 matches=payload.get('matches') or []
 if view=='active':matches=[m for m in matches if m.get('active')]
 elif view=='ended':matches=[m for m in matches if not m.get('active')]
 return {**payload,'view':view,'matches':matches[:limit],'truncated':len(matches)>limit,'shown':min(limit,len(matches))}


def _limit(value):
 try:n=int(value or 100)
 except (TypeError,ValueError):raise PilotError(400,'FIELD_INVALID','limit must be a whole number.')
 if not 1<=n<=500:raise PilotError(400,'FIELD_INVALID','limit must be 1-500.')
 return n


def build_router(app,screener):
 r=APIRouter()
 def me(request):return _identity(app,request)

 def visible(user,scanner_id):
  sc=screener.db.scanner(scanner_id)
  if not sc or not (sc['is_default'] or sc['owner_id']==user['id']):
   raise PilotError(404,'SCANNER_NOT_FOUND','There is no such scanner.')
  return sc

 def owned(user,scanner_id):
  sc=visible(user,scanner_id)
  if sc['is_default']:raise PilotError(403,'SCANNER_DEFAULT','KANIDA scanners cannot be changed. Duplicate it to make your own.')
  return sc

 def public(sc,sub=None,counts=None):
  d=sc['definition']
  return {'id':sc['id'],'name':sc['name'],'description':sc.get('description'),'is_default':sc['is_default'],
   'mine':not sc['is_default'],'source':sc['source'],'nl_text':sc.get('nl_text'),'definition':d,
   'reads_as':D.reads_as(d),'grain':D.grain(d),'updated':sc['updated'],
   'notify':{'new':bool(sub and sub['notify_new']),'ended':bool(sub and sub['notify_ended']),
    'changed':bool(sub and sub['notify_changed'])},'counts':counts}

 def latest_through():
  st=screener.status()
  return st.get('as_of') if st.get('available') else None

 @r.get('/api/screener/vocabulary')
 def vocabulary(request:Request):
  me(request);return {**V.catalogue(),'underlyings':screener.underlyings()}

 @r.get('/api/screener/status')
 def status(request:Request):
  me(request);return screener.status()

 @r.get('/api/screener/scanners')
 def scanners(request:Request):
  user=me(request)
  subs=screener.db.subscriptions_for(user['id'])
  st=screener.status()
  out=[]
  for sc in screener.db.scanners_for(user['id']):
   counts=None
   if st.get('available'):
    hit=screener.db.result(sc['digest'],st['session'])
    if hit:counts={**(hit['payload'].get('counts') or {}),'through':hit['through'],'current':hit['through']==st['as_of']}
   out.append(public(sc,subs.get(sc['id']),counts))
  return {'scanners':out,'status':st,'unread':screener.db.unread_count(user['id'])}

 @r.post('/api/screener/scanners')
 def create(request:Request,data:dict=Body(...)):
  user=me(request)
  d=_definition(data.get('definition'))
  source=data.get('source') if data.get('source') in ('visual','nl') else 'visual'
  mine=[s for s in screener.db.scanners_for(user['id']) if not s['is_default']]
  if len(mine)>=50:raise PilotError(409,'SCANNER_LIMIT','You can keep up to 50 scanners. Delete one to save another.')
  sc=screener.db.create(user['id'],_name(data.get('name')),d,D.digest(d),source,
   nl_text=(str(data.get('nl_text'))[:600] if data.get('nl_text') else None))
  return public(sc)

 @r.post('/api/screener/scanners/{scanner_id}')
 def update(request:Request,scanner_id:str,data:dict=Body(...)):
  user=me(request);sc=owned(user,scanner_id)
  fields={}
  if 'name' in data:fields['name']=_name(data.get('name'),sc['name'])
  if 'definition' in data:
   d=_definition(data.get('definition'));fields['definition']=d;fields['digest']=D.digest(d)
  if 'nl_text' in data:fields['nl_text']=str(data.get('nl_text') or '')[:600] or None
  if not fields:raise PilotError(400,'FIELD_INVALID','Nothing to change.')
  sc=screener.db.update(scanner_id,**fields)
  sub=screener.db.subscription(user['id'],scanner_id)
  if sub and 'digest' in fields:
   # a changed definition is a different scanner: its alerts start from now, never from its new history
   sub=screener.db.subscribe(user['id'],scanner_id,sub['notify_new'],sub['notify_ended'],sub['notify_changed'],latest_through())
  return public(sc,sub)

 @r.post('/api/screener/scanners/{scanner_id}/duplicate')
 def duplicate(request:Request,scanner_id:str,data:dict=Body(default={})):
  user=me(request);sc=visible(user,scanner_id)
  name=_name(data.get('name'),f"{sc['name']} (copy)"[:NAME_MAX])
  copy=screener.db.create(user['id'],name,sc['definition'],sc['digest'],'visual' if sc['source']=='default' else sc['source'],
   nl_text=sc.get('nl_text'))
  return public(copy)

 @r.post('/api/screener/scanners/{scanner_id}/delete')
 def delete(request:Request,scanner_id:str,data:dict=Body(default={})):
  user=me(request);owned(user,scanner_id)
  screener.db.delete(scanner_id)
  return {'ok':True,'id':scanner_id}

 @r.post('/api/screener/scanners/{scanner_id}/notify')
 def notify(request:Request,scanner_id:str,data:dict=Body(...)):
  user=me(request);sc=visible(user,scanner_id)
  sub=screener.db.subscribe(user['id'],scanner_id,bool(data.get('new')),bool(data.get('ended')),bool(data.get('changed')),
   latest_through())
  return public(sc,sub)

 @r.get('/api/screener/scanners/{scanner_id}/results')
 def results(request:Request,scanner_id:str,view:str='active',limit:str='100'):
  user=me(request);sc=visible(user,scanner_id)
  payload=screener.results(sc['definition'])
  return {**_view(payload,view,_limit(limit)),'scanner':public(sc,screener.db.subscription(user['id'],scanner_id))}

 @r.post('/api/screener/describe')
 def describe(request:Request,data:dict=Body(...)):
  me(request)
  d=_definition(data.get('definition'))
  return {'definition':d,'reads_as':D.reads_as(d),'grain':D.grain(d),'computed':D.uses_computed(d)}

 @r.post('/api/screener/parse')
 def parse(request:Request,data:dict=Body(...)):
  me(request)
  text=str(data.get('text') or '')
  if not text.strip():raise PilotError(400,'FIELD_INVALID','Describe the market behaviour you want to find.')
  try:return screener.parse(text)
  except D.DefinitionError as error:raise PilotError(400,'SCANNER_UNDERSTOOD_NOTHING',str(error))

 @r.post('/api/screener/run')
 def run(request:Request,data:dict=Body(...)):
  me(request)
  d=_definition(data.get('definition'))
  payload=screener.results(d)
  return _view(payload,str(data.get('view') or 'active'),_limit(data.get('limit')))

 @r.get('/api/screener/alerts')
 def alerts(request:Request,unread:str=''):
  user=me(request)
  return {'alerts':screener.db.alerts_for(user['id'],unread_only=unread in ('1','true')),
   'unread':screener.db.unread_count(user['id'])}

 @r.post('/api/screener/alerts/read')
 def alerts_read(request:Request,data:dict=Body(default={})):
  user=me(request)
  ids=[int(x) for x in (data.get('ids') or []) if str(x).isdigit()][:200]
  screener.db.mark_read(user['id'],ids or None)
  return {'ok':True,'unread':screener.db.unread_count(user['id'])}

 return r
