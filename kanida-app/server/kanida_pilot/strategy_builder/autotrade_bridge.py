"""Slice 7 — the live bridge: hand a reviewed order plan to engine/backend/autotrade as a strategy-intent basket.

This app never talks to a broker. It sends the EXACT reviewed plan (the preview, hash-checked and unexpired) to
autotrade's /api/autotrade/intents, and autotrade decides everything else:
  * DRY RUN by default; a live request is refused there unless every gate passes (master switch, options certified,
    strategy intents live, broker certified for baskets, an unexpired OPERATOR ARM for this user + account, market
    open, and a verified basket margin).
  * Defined-risk only, hedges (BUY groups) fully filled before any SELL group is sent.
This app holds only the operator service token (PILOT_AUTOTRADE_TOKEN). It never holds the arm token, so it can
never arm itself. The token is read from the environment and never logged or returned.
"""
from __future__ import annotations
import json,os,time,uuid,logging
from typing import Any,Dict,Optional

import httpx

log=logging.getLogger('strategy_builder.autotrade')
SOURCE='kanida-app-strategy-builder'
TIMEOUT=5.0

SCHEMA='''
create table if not exists autotrade_routes(
 id text primary key, user_id text not null, strategy_id text not null, preview_id text not null, idem_key text not null,
 mode text not null, intent_id text, state text not null, reason text, intent text, created_at real not null, updated_at real not null,
 unique(user_id, idem_key));
create index if not exists ix_atr_user on autotrade_routes(user_id, strategy_id, created_at);
'''
TERMINAL={'completed','dry_run_complete','blocked','failed','cancelled','attention_required','refused','released_by_user'}
# GTM audit P15: a transport failure AFTER the request left says nothing about whether autotrade accepted it. Those
# errors put the hand-off in 'unknown'; it is reconciled by looking up its idempotency key - never by re-sending.
UNKNOWN_CODES={'AUTOTRADE_NO_RESPONSE'}   # 5xx from the engine also counts: it answered after receiving it


class BridgeError(Exception):
 def __init__(self,status,code,message):super().__init__(message);self.status=status;self.code=code;self.message=message


class Bridge:
 """HTTP client for autotrade's strategy-intent intake. Unconfigured -> every call says so; nothing is faked."""
 def __init__(self,url:Optional[str]=None,token:Optional[str]=None,account:Optional[str]=None,client=None):
  self.url=(url if url is not None else os.environ.get('PILOT_AUTOTRADE_URL','')).rstrip('/')
  self._token=token if token is not None else os.environ.get('PILOT_AUTOTRADE_TOKEN','')
  self.account=account if account is not None else os.environ.get('PILOT_AUTOTRADE_ACCOUNT','')
  self._client=client

 @property
 def configured(self)->bool:return bool(self.url and self._token)

 def engine_user(self,user_id:str)->str:return f'pilot:{user_id}'

 def _call(self,method:str,path:str,**kw)->Dict[str,Any]:
  if not self.configured:raise BridgeError(503,'AUTOTRADE_NOT_CONFIGURED','AutoTrade is not connected to this app (PILOT_AUTOTRADE_URL / PILOT_AUTOTRADE_TOKEN).')
  c=self._client or httpx
  try:
   r=c.request(method,f'{self.url}/api/autotrade/intents{path}',headers={'X-Operator-Token':self._token},timeout=TIMEOUT,**kw)
  except (ConnectionError,httpx.ConnectError,httpx.ConnectTimeout) as e:   # never connected: nothing was sent
   raise BridgeError(503,'AUTOTRADE_UNREACHABLE',f'AutoTrade could not be reached ({type(e).__name__}); nothing was sent.')
  except Exception as e:  # noqa: BLE001 - connected, but no answer: it may have been accepted (GTM audit P15)
   raise BridgeError(504,'AUTOTRADE_NO_RESPONSE',f'AutoTrade did not answer ({type(e).__name__}).')
  try:data=r.json()
  except ValueError:data={}
  if r.status_code>=400:
   msg=data.get('error') or data.get('detail') or f'AutoTrade refused ({r.status_code})'
   raise BridgeError(r.status_code if r.status_code<500 else 502,data.get('code') or 'AUTOTRADE_REFUSED',str(msg))
  return data

 def capability(self,user_id:str)->Dict[str,Any]:
  base={'configured':self.configured,'engine_user':self.engine_user(user_id),'broker_account_id':self.account or None}
  if not self.configured:
   return {**base,'reachable':False,'live_allowed':False,'gates':[],'arm':None,
    'reason':'AutoTrade is not connected to this app. Paper orders still work.'}
  try:
   d=self._call('GET','/capability',params={'user_id':self.engine_user(user_id),'broker_account_id':self.account or None})
  except BridgeError as e:
   return {**base,'reachable':False,'live_allowed':False,'gates':[],'arm':None,'reason':e.message}
  return {**base,'reachable':True,'live_allowed':bool(d.get('live_allowed')),'gates':d.get('gates') or [],'arm':d.get('arm'),
   'accepts':d.get('accepts'),'reason':'Every gate passes - live orders are possible.' if d.get('live_allowed') else
   'Dry run only until every gate below passes. Live needs the operator to arm this account.'}

 def submit(self,basket:Dict[str,Any])->Dict[str,Any]:return self._call('POST','',json=basket)
 def find(self,idempotency_key:str)->Optional[Dict[str,Any]]:
  """The intent autotrade holds for this key, or None if it has none. Read-only: never re-sends anything."""
  d=self._call('GET','',params={'source':SOURCE,'limit':200})
  return next((i for i in (d.get('intents') or []) if i.get('idempotency_key')==idempotency_key),None)
 def get(self,iid:str)->Dict[str,Any]:return self._call('GET',f'/{iid}')
 def cancel(self,iid:str)->Dict[str,Any]:return self._call('POST',f'/{iid}/cancel',json={})


class AutotradeRoutes:
 """Records every hand-off to autotrade (one per idempotency key) and mirrors autotrade's state back."""
 def __init__(self,store,bridge:Bridge):
  self.store=store;self.bridge=bridge;self.c=store.c;self.lock=store.lock
  with self.lock:
   self.c.executescript(SCHEMA)
   # a hand-off left 'sending' by a crash or restart has an unknown outcome - it is reconciled, never left blocking
   self.c.execute("update autotrade_routes set state='unknown',reason='Interrupted before an answer arrived (restart). Reconciling by its key; nothing will be re-sent.',updated_at=? where state='sending'",(time.time(),))
   self.c.commit()

 def basket(self,user_id:str,preview:Dict[str,Any],body:Dict[str,Any],mode:str,idem:str)->Dict[str,Any]:
  orders=json.loads(preview['orders'])
  return {'source':SOURCE,'idempotency_key':f'{user_id}:{idem}'[:128],'mode':mode,'broker':'zerodha',
   'user_id':self.bridge.engine_user(user_id),'broker_account_id':self.bridge.account or None,
   'reference':{'strategy_id':preview['strategy_id'],'preview_id':preview['id'],'preview_hash':preview['hash']},
   'legs':[{'tradingsymbol':o['symbol'],'exchange':'NFO','underlying':body['underlying'],'expiry':body['expiry'],
    'strike':o['strike'],'option_type':o['type'],'side':'BUY' if o['side']=='B' else 'SELL','quantity':int(o['qty']),
    'lot_size':int(o['lot_size']),'limit_price':float(o['limit']),'product':preview['product'],'group':int(o['group'])} for o in orders]}

 def route(self,user_id:str,strategy:Dict[str,Any],preview_id:str,preview_hash:str,idem:str,mode:str)->Dict[str,Any]:
  from .execution import ExecError
  if mode not in ('dry_run','live'):raise ExecError(400,'FIELD_INVALID','mode must be dry_run or live.')
  if not idem or len(idem)>80:raise ExecError(400,'IDEMPOTENCY_KEY_REQUIRED','An idempotency key is required.')
  with self.lock:
   prior=self.c.execute('select * from autotrade_routes where user_id=? and idem_key=?',(user_id,idem)).fetchone()
   if prior:
    if prior['preview_id']!=preview_id or prior['mode']!=mode:raise ExecError(409,'IDEMPOTENCY_CONFLICT','This request key was already used for a different hand-off.')
    return self.get(user_id,prior['id'])
  with self.lock:
   p=self.c.execute('select * from previews where id=? and user_id=?',(preview_id,user_id)).fetchone()
  if not p or p['strategy_id']!=strategy['id']:raise ExecError(404,'PREVIEW_NOT_FOUND','There is no such order preview.')
  if p['kind']!='open':raise ExecError(409,'NOT_SUPPORTED','Only opening orders can be handed to AutoTrade in this slice.')
  if p['hash']!=preview_hash:raise ExecError(409,'PREVIEW_CHANGED','The order plan changed. Review it again.')
  if time.time()>p['expires_at']:raise ExecError(409,'PREVIEW_EXPIRED','This preview has expired - quotes move. Refresh it and review again.')
  if strategy['draft']['version']!=p['draft_version']:raise ExecError(409,'PREVIEW_CHANGED','The strategy was edited after this preview. Review it again.')
  checks=json.loads(p['checks'])
  if any(c['status']=='block' for c in checks):raise ExecError(409,'CHECKS_FAILED','A pre-trade check blocks these orders.')
  orders=json.loads(p['orders'])
  uncovered=[k for k in ('CE','PE') if sum(o['qty'] for o in orders if o['type']==k and o['side']=='S')>sum(o['qty'] for o in orders if o['type']==k and o['side']=='B')]
  if uncovered or any(c['key']=='risk' and c['status']!='pass' for c in checks):
   raise ExecError(409,'UNDEFINED_RISK','AutoTrade accepts defined-risk structures only: every short '+('/'.join(uncovered) or 'leg')+' needs a covering long. Add the hedge legs first.')
  rid=uuid.uuid4().hex[:16];t=time.time()
  basket=self.basket(user_id,dict(p),json.loads(p['body']),mode,idem)
  with self.lock:
   # the "nothing unresolved" check and the insert are one step under the lock (quant audit F4): two concurrent
   # hand-offs of the same strategy can never both pass
   pending=self.c.execute("select id from autotrade_routes where user_id=? and strategy_id=? and state in ('sending','unknown')",(user_id,strategy['id'])).fetchone();still=None
   if pending:
    try:self.get(user_id,pending['id'])                                  # reconcile it now (it may resolve)
    except Exception:pass  # noqa: BLE001
    still=self.c.execute("select id from autotrade_routes where id=? and state in ('sending','unknown')",(pending['id'],)).fetchone()
   if pending and still:
    raise ExecError(409,'HANDOFF_UNRESOLVED','An earlier hand-off of this strategy has an unknown outcome. It is being reconciled with AutoTrade - no new orders until it resolves.')
   self.c.execute('insert into autotrade_routes values(?,?,?,?,?,?,?,?,?,?,?,?)',(rid,user_id,strategy['id'],preview_id,idem,mode,None,'sending',None,None,t,t))
   self.store._log(user_id,strategy['id'],'autotrade_route',f"Order plan handed to AutoTrade ({'LIVE request' if mode=='live' else 'dry run'})")
   self.c.commit()
  try:
   res=self.bridge.submit(basket)
   if not isinstance(res,dict):raise ValueError('AutoTrade answered with an unexpected body')
  except (ValueError,TypeError,AttributeError) as e:    # it answered, but not in a form we can trust: outcome unknown
   self._set(rid,state='unknown',reason=f'AutoTrade answered unexpectedly ({type(e).__name__}). Reconciling by its key; nothing will be re-sent.')
   return self.get(user_id,rid)
  except BridgeError as e:
   if e.code in UNKNOWN_CODES or (e.status>=500 and e.code not in ('AUTOTRADE_UNREACHABLE','AUTOTRADE_NOT_CONFIGURED')):
    self._set(rid,state='unknown',reason=f'{e.code}: {e.message} The outcome is unknown - reconciling by its key; nothing will be re-sent.')
   else:
    self._set(rid,state='refused',reason=f'{e.code}: {e.message}')
   return self.get(user_id,rid)
  it=res.get('intent') if isinstance(res.get('intent'),dict) else {}
  if not it.get('id'):
   self._set(rid,state='unknown',reason='AutoTrade answered without an intent id. Reconciling by its key; nothing will be re-sent.')
   return self.get(user_id,rid)
  self._set(rid,intent_id=it.get('id'),state=it.get('state') or 'accepted',reason=it.get('reason'),intent=json.dumps(it))
  return self.get(user_id,rid)

 def _set(self,rid:str,**f)->None:
  cols=', '.join(f'{k}=?' for k in f)
  with self.lock:
   self.c.execute(f'update autotrade_routes set {cols}, updated_at=? where id=?',(*f.values(),time.time(),rid));self.c.commit()

 def _row(self,r)->Dict[str,Any]:
  d=dict(r);d['intent']=json.loads(d['intent']) if d.get('intent') else None;return d

 def get(self,user_id:str,rid:str,refresh:bool=True)->Optional[Dict[str,Any]]:
  with self.lock:r=self.c.execute('select * from autotrade_routes where id=? and user_id=?',(rid,user_id)).fetchone()
  if not r:return None
  d=self._row(r)
  if d['state']=='sending' and time.time()-d['created_at']>TIMEOUT*3:     # the process died mid-call: outcome unknown
   self._set(rid,state='unknown',reason='The hand-off was interrupted before an answer arrived. Reconciling by its key; nothing will be re-sent.');d['state']='unknown'
  if refresh and d['state']=='unknown':
   try:
    # found by its key = adopt it. NOT found is never taken as "not received": the engine list is bounded, so absence
    # proves nothing (quant audit F4). It stays unknown until found, or until the user explicitly releases it.
    it=self.bridge.find(f"{user_id}:{d['idem_key']}"[:128])
    if it:self._set(rid,intent_id=it.get('id'),state=it.get('state') or 'accepted',reason=it.get('reason'),intent=json.dumps(it))
    with self.lock:d=self._row(self.c.execute('select * from autotrade_routes where id=?',(rid,)).fetchone())
   except BridgeError as e:
    d['refresh_error']=e.message
   return d
  if refresh and d['intent_id'] and d['state'] not in TERMINAL:
   try:
    it=self.bridge.get(d['intent_id']).get('intent') or {}
    self._set(rid,state=it.get('state') or d['state'],reason=it.get('reason'),intent=json.dumps(it))
    with self.lock:d=self._row(self.c.execute('select * from autotrade_routes where id=?',(rid,)).fetchone())
   except BridgeError as e:
    d['refresh_error']=e.message
  return d

 def release(self,user_id:str,rid:str,confirm:bool)->Optional[Dict[str,Any]]:
  """The user states they checked AutoTrade and this unknown hand-off did not create orders. Recorded as their
  decision (never inferred by this app); it lifts the block on new hand-offs of the strategy."""
  from .execution import ExecError
  d=self.get(user_id,rid,refresh=True)
  if not d:return None
  if d['state']!='unknown':raise ExecError(409,'NOT_UNKNOWN','Only a hand-off with an unknown outcome can be released.')
  if confirm is not True:raise ExecError(400,'CONFIRM_REQUIRED','Confirm that you checked AutoTrade and no orders exist for this hand-off.')
  self._set(rid,state='released_by_user',reason='Released by the user after checking AutoTrade; this app never confirmed the outcome itself.')
  with self.lock:
   self.store._log(user_id,d['strategy_id'],'autotrade_release','Unknown AutoTrade hand-off released by the user after checking AutoTrade')
   self.c.commit()
  return self.get(user_id,rid,refresh=False)

 def list(self,user_id:str,strategy_id:Optional[str]=None)->list:
  q,p='select id from autotrade_routes where user_id=?',[user_id]
  if strategy_id:q+=' and strategy_id=?';p.append(strategy_id)
  with self.lock:ids=[r[0] for r in self.c.execute(q+' order by created_at desc limit 50',p).fetchall()]
  return [self.get(user_id,i) for i in ids]

 def cancel(self,user_id:str,rid:str)->Optional[Dict[str,Any]]:
  d=self.get(user_id,rid,refresh=False)
  if not d:return None
  if d['intent_id'] and d['state'] not in TERMINAL:
   try:
    it=self.bridge.cancel(d['intent_id']).get('intent') or {}
    self._set(rid,state=it.get('state') or d['state'],reason=it.get('reason'),intent=json.dumps(it))
   except BridgeError as e:
    from .execution import ExecError
    raise ExecError(e.status,e.code,e.message)
  return self.get(user_id,rid,refresh=False)
