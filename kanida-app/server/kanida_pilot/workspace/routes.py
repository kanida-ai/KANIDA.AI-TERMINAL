"""/api/workspace/* — saved workspaces, templates, the widget registry, and the three new reads (summary, Greeks,
volume). Member-only and owner-scoped; GET + POST only (the pilot's CORS allows those two).

  GET  /api/workspace/registry
  GET  /api/workspace/templates
  GET  /api/workspace/workspaces                     the caller's workspaces (created from a template on first visit)
  POST /api/workspace/workspaces                     {template?, name?}          create
  GET  /api/workspace/workspaces/{id}
  POST /api/workspace/workspaces/{id}                {definition, version}       save (autosave; 409 on a stale version)
  POST /api/workspace/workspaces/{id}/duplicate
  POST /api/workspace/workspaces/{id}/delete
  POST /api/workspace/summary                        {underlying, sources[], scope, focus}
  GET  /api/workspace/greeks?underlying=&expiry=&atm=
  GET  /api/workspace/volume?underlying=&expiry=
"""
from __future__ import annotations
import copy,os,re
from fastapi import APIRouter,Body,Request
from fastapi.responses import JSONResponse
from ..errors import PilotError
from ..screener.routes import _identity
from . import alignment,extras,summary
from .model import WorkspaceError,normalize
from .registry import catalogue
from .store import Conflict
from .templates import TEMPLATES

SYMBOL=re.compile(r'^[A-Z0-9&-]{1,20}$')
DATE=re.compile(r'^\d{4}-\d{2}-\d{2}$')
FIRST_TEMPLATE='options-trader'


def _symbol(v):
 s=str(v or '').strip().upper()
 if not SYMBOL.match(s):raise PilotError(400,'FIELD_INVALID','underlying is required.')
 return s


def build_router(app,store,settings):
 r=APIRouter()
 # the snapshots are read READ-ONLY (mode=ro); a dev server may point this at the real file while the pilot's own
 # snapshot store stays on a throwaway one
 intel=os.getenv('PILOT_WORKSPACE_INTELLIGENCE') or settings.intelligence_database
 def me(request):return _identity(app,request)
 def template(key):
  hit=next((t for t in TEMPLATES if t[0]==key),None)
  if not hit:raise PilotError(404,'TEMPLATE_NOT_FOUND','There is no such template.')
  return hit
 def definition_of(key,name=None):
  slug,title,_d,raw=template(key)
  d=normalize({**copy.deepcopy(raw),'name':name or title})
  return d,slug
 def own(user,ws_id):
  ws=store.get(user['id'],ws_id)
  if not ws:raise PilotError(404,'WORKSPACE_NOT_FOUND','There is no such workspace.')
  return ws

 @r.get('/api/workspace/registry')
 def registry(request:Request):
  me(request);return catalogue()

 @r.get('/api/workspace/templates')
 def templates(request:Request):
  me(request)
  return {'templates':[{'key':k,'name':n,'description':d,'widgets':[w['widget_type'] for w in raw['widgets']]}
   for k,n,d,raw in TEMPLATES]}

 @r.get('/api/workspace/workspaces')
 def workspaces(request:Request):
  user=me(request)
  rows=store.list(user['id'])
  if not rows:
   d,slug=definition_of(FIRST_TEMPLATE,'My workspace')
   rows=[store.create(user['id'],d,slug)]
  return {'workspaces':rows}

 @r.post('/api/workspace/workspaces')
 def create(request:Request,data:dict=Body(default={})):
  user=me(request)
  if len(store.list(user['id']))>=20:raise PilotError(409,'WORKSPACE_LIMIT','You can keep up to 20 workspaces.')
  d,slug=definition_of(str(data.get('template') or 'blank'),' '.join(str(data.get('name') or '').split())[:60] or None)
  return store.create(user['id'],d,slug)

 @r.get('/api/workspace/workspaces/{ws_id}')
 def get(request:Request,ws_id:str):
  user=me(request);return own(user,ws_id)

 @r.post('/api/workspace/workspaces/{ws_id}')
 def save(request:Request,ws_id:str,data:dict=Body(...)):
  user=me(request);own(user,ws_id)
  try:d=normalize(data.get('definition'))
  except WorkspaceError as error:raise PilotError(400,'WORKSPACE_INVALID',str(error))
  try:saved=store.save(user['id'],ws_id,d,data.get('version'))
  except Conflict as c:
   return JSONResponse({'error':'This workspace changed in another window. The newer copy was loaded.',
    'code':'WORKSPACE_STALE','current':c.current},status_code=409)
  return saved

 @r.post('/api/workspace/workspaces/{ws_id}/duplicate')
 def duplicate(request:Request,ws_id:str,data:dict=Body(default={})):
  user=me(request);ws=own(user,ws_id)
  d=normalize({'name':f"{ws['name']} (copy)"[:60],'selected':ws['selected'],'widgets':ws['widgets'],'layout':ws.get('layout')})
  return store.create(user['id'],d,ws.get('template'))

 @r.post('/api/workspace/workspaces/{ws_id}/delete')
 def delete(request:Request,ws_id:str,data:dict=Body(default={})):
  user=me(request);own(user,ws_id)
  store.delete(user['id'],ws_id)
  return {'ok':True,'workspaces':store.list(user['id'])}

 def scanner_block(user,underlying,focus):
  if not focus or not focus.get('scanner_id'):return None
  snap=summary.latest_snapshot(intel,underlying)
  return alignment.context(getattr(app.state,'screener',None),user,focus,underlying,snap)

 @r.post('/api/workspace/summary')
 def ai_summary(request:Request,data:dict=Body(...)):
  user=me(request)
  sources=[str(s) for s in (data.get('sources') or [])][:40]
  scope='all' if data.get('scope')=='all' else 'connected'
  focus=data.get('focus') if isinstance(data.get('focus'),dict) else None
  underlying=_symbol(data.get('underlying'))
  return summary.build(intel,settings.derivatives_database,underlying,
   sources,scope,focus,scanner=scanner_block(user,underlying,focus))

 @r.get('/api/workspace/scanner-context')
 def scanner_context(request:Request,underlying:str='',scanner_id:str='',match_key:str=''):
  """The same scanner block the summary opens with — for the 15-min signal widget, so both say the same thing."""
  user=me(request)
  block=scanner_block(user,_symbol(underlying),{'scanner_id':scanner_id[:32],'match_key':match_key[:80]})
  return {'available':bool(block),'context':block}

 @r.get('/api/workspace/key-strikes')
 def key_strikes(request:Request,underlying:str=''):
  """The 15-min signal engine's key strikes at its latest reading of this instrument (read-only snapshots)."""
  me(request)
  name=_symbol(underlying)
  snaps=summary.session_snapshots(intel,name)
  if not snaps:return {'available':False,'underlying':name,'text':f'No 15-min reading of {name} has been captured yet.'}
  at,expiry,cur=snaps[-1]
  return {'available':True,'underlying':name,'expiry':expiry,'as_of':at,'headline':cur.get('plain_language_headline'),
   'key_strikes':cur.get('key_strikes') or [],'location':cur.get('location'),'leading_strike':cur.get('leading_strike')}

 @r.get('/api/workspace/greeks')
 def greeks(request:Request,underlying:str='',expiry:str='',atm:int=3):
  me(request)
  if expiry and not DATE.match(expiry):raise PilotError(400,'FIELD_INVALID','expiry must be YYYY-MM-DD.')
  return extras.greeks(getattr(app.state,'screener',None),_symbol(underlying),expiry or None,max(1,min(int(atm),10)))

 @r.get('/api/workspace/volume')
 def volume(request:Request,underlying:str='',expiry:str=''):
  me(request)
  if expiry and not DATE.match(expiry):raise PilotError(400,'FIELD_INVALID','expiry must be YYYY-MM-DD.')
  return extras.volume(settings.derivatives_database,_symbol(underlying),expiry or None)

 return r
