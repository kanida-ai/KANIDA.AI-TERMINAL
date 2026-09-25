"""Slice 4 — order review, idempotent intents and a PAPER broker that fills against live quotes.

Blueprint B §11 / K11 inside blueprint A's execution boundary:
  * An ExecutionPreview is the exact plan: every order's contract, side, quantity, limit, product, sequence group and
    slice, the quotes it was built from, the checks it passed, the exchange margin (Kite basket margin, read-only) and
    estimated charges. It carries a content hash and EXPIRES (30 s). Anything that changes the plan needs a new one.
  * Confirming creates a deployment and one OrderIntent per slice, persisted BEFORE dispatch, under an idempotency
    key: the same key returns the same deployment; the same key with a different preview is refused.
  * Dispatch goes to the PAPER broker only. A limit BUY fills at the ask when ask <= limit; a limit SELL fills at the
    bid when bid >= limit; otherwise the order rests and a worker re-checks live quotes. Hedges (buys) are group 1;
    sells are dispatched only after group 1 is fully filled, so a partial entry is never net-short naked.
  * LIVE routing is not wired here. It goes through engine/backend/autotrade (paper-default, per-broker certified,
    operator-armed) and this module reports it as a disabled capability with that reason.
Nothing in this file calls a broker's order API. The only network reads are the Kite quote/margin reads in kite_market.
"""
from __future__ import annotations
import hashlib,json,threading,time,uuid,logging
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo
from . import analytics as A
from . import charges as CH
from . import service as S
from .templates import recognise

log=logging.getLogger('strategy_builder.execution')
IST=ZoneInfo('Asia/Kolkata')
PREVIEW_TTL=30
QUOTE_MAX_AGE=15
SPREAD_WARN=0.05
PAPER_CAPITAL=1_000_000.0
# NSE quantity-freeze limits per order (units). CONFIGURED, not fetched - verify against the current NSE circular.
FREEZE_UNITS={'NIFTY':1800,'BANKNIFTY':900,'FINNIFTY':1800,'MIDCPNIFTY':2800,'NIFTYNXT50':600}
FREEZE_SOURCE='Configured NSE freeze limits (verify against the current NSE circular)'
LIVE_CAPABILITY={'enabled':False,'reason':'Live orders route through backend/autotrade - paper by default, certified per broker and armed by the operator. Not enabled in the pilot.'}

SCHEMA='''
create table if not exists previews(
 id text primary key, user_id text not null, strategy_id text not null, draft_version integer not null, kind text not null,
 deployment_id text, product text not null, body text not null, orders text not null, checks text not null, margin text, hash text not null,
 created_at real not null, expires_at real not null);
create table if not exists deployments(
 id text primary key, user_id text not null, strategy_id text not null, revision_id text not null, mode text not null,
 status text not null, product text not null, preview_id text not null, idem_key text not null, margin text,
 opened_at real not null, closed_at real, unique(user_id, idem_key));
create index if not exists ix_dep_user on deployments(user_id, status);
create table if not exists intents(
 id text primary key, deployment_id text not null, preview_id text not null, kind text not null, grp integer not null, seq integer not null,
 leg_id text not null, symbol text not null, token integer, side text not null, qty integer not null, limit_price real not null,
 state text not null, filled_qty integer not null default 0, avg_price real, fees real not null default 0, reason text,
 created_at real not null, updated_at real not null);
create index if not exists ix_int_dep on intents(deployment_id, state);
create table if not exists dfills(
 id text primary key, intent_id text not null, deployment_id text not null, leg_id text not null, side text not null, qty integer not null,
 price real not null, fees real not null, basis text not null, quote_ts text, created_at real not null);
'''
OPEN_STATES=('created','acknowledged','partially_filled')


class ExecError(Exception):
 def __init__(self,status,code,message,extra=None):super().__init__(message);self.status=status;self.code=code;self.message=message;self.extra=extra or {}


def uid():return uuid.uuid4().hex[:16]
def now_ist():return datetime.now(IST).replace(tzinfo=None)
def tick_round(x,tick,up=False):
 tick=tick or 0.05;n=x/tick
 import math
 return round((math.ceil(n-1e-9) if up else math.floor(n+1e-9))*tick,2)


def market_open(at=None):
 at=at or now_ist()
 return at.weekday()<5 and (at.hour,at.minute)>=(9,15) and (at.hour,at.minute)<(15,30)


class Execution:
 def __init__(self,store,market):
  self.store=store;self.market=market;self.c=store.c;self.lock=store.lock
  with self.lock:self.c.executescript(SCHEMA);self.c.commit()
  self._stop=threading.Event();self._worker=None

 # --- preview ------------------------------------------------------------------------------------------------------
 def preview(self,user_id,strategy,options,kind='open',deployment=None):
  status=self.market.status() if hasattr(self.market,'status') else {'live':False,'reason':'No live market.'}
  if not status['live']:
   raise ExecError(409,'MARKET_DATA_NOT_LIVE','Paper orders fill against live quotes, and live data is unavailable: '+(status.get('reason') or ''))
  body=strategy['draft']['body']
  if kind=='close':body=self._close_body(deployment)
  chain,legs,problems=S.hydrate(self.market,body)
  if problems:raise ExecError(400,'UNRESOLVED_CONTRACT',problems[0])
  legs=[l for l in legs if l.get('include',True)]
  if not legs:raise ExecError(400,'EMPTY_STRATEGY','There are no included legs to order.')
  product=options.get('product') if options.get('product') in ('NRML','MIS') else 'NRML'
  policy=options.get('price_policy') if options.get('price_policy') in ('marketable','mid') else 'marketable'
  overrides=options.get('limits') or {}
  checks=[];tick=chain.get('tick_size') or 0.05
  def check(key,label,state,detail=''):checks.append({'key':key,'label':label,'status':state,'detail':detail})
  at=now_ist();quote_at=A.parse_ist(chain['as_of'])
  check('market_open','Market is open (09:15-15:30 IST, weekdays)','pass' if market_open(at) else 'block',
   f"Now {at.strftime('%a %H:%M:%S')} IST")
  age=(at-quote_at).total_seconds() if quote_at else None
  check('quotes_fresh',f'Quotes are fresh (at most {QUOTE_MAX_AGE}s old)','pass' if age is not None and age<=QUOTE_MAX_AGE else 'block',
   f'Quote time {chain["as_of"]} IST' + (f' ({age:.0f}s old)' if age is not None else ''))
  orders=[];no_quote=[];wide=[]
  freeze=FREEZE_UNITS.get(body['underlying'])
  hedged=any(l['side']=='B' for l in legs) and any(l['side']=='S' for l in legs)
  for l in legs:
   bid,ask=l.get('bid'),l.get('ask')
   if not bid or not ask:no_quote.append(l);continue
   mid=(bid+ask)/2;spread=(ask-bid)/mid if mid else 1
   if spread>SPREAD_WARN:wide.append(f"{int(l['strike'])} {l['type']} {spread*100:.1f}%")
   if policy=='marketable':limit=tick_round(ask,tick,up=True) if l['side']=='B' else tick_round(bid,tick)   # still crosses; always on the tick
   else:limit=tick_round(mid,tick,up=l['side']=='B')
   if l['id'] in overrides:
    try:o=float(overrides[l['id']])
    except (TypeError,ValueError):raise ExecError(400,'FIELD_INVALID','A limit price must be a number.')
    if not (bid*0.8<=o<=ask*1.2):raise ExecError(400,'LIMIT_OUT_OF_BAND',f"The limit for {int(l['strike'])} {l['type']} must be within 20% of the quote ({bid}-{ask}).")
    limit=tick_round(o,tick,up=l['side']=='B')
   units=int(l['lots'])*int(l['lot_size'])
   slices=[];left=units;lot=int(l['lot_size'])
   cap=(freeze//lot)*lot if freeze else units
   while left>0:q=min(left,cap or left);slices.append(q);left-=q
   orders.append({'leg_id':l['id'],'symbol':l['symbol'],'token':l['token'],'type':l['type'],'strike':l['strike'],'side':l['side'],'qty':units,
    'lots':int(l['lots']),'lot_size':lot,'limit':limit,'bid':bid,'ask':ask,'group':1 if (l['side']=='B' or not hedged) else 2,'slices':slices,
    'charges':round(CH.leg_charges(l['side'],limit,units)['total'],2)})
  check('quotes_present','Every leg has a live bid and ask','pass' if not no_quote else 'block',
   ', '.join(f"{int(l['strike'])} {l['type']}" for l in no_quote) or 'All legs quoted')
  check('spread','Bid-ask spreads are reasonable (<=5%)','warn' if wide else 'pass','Wide: '+', '.join(wide) if wide else 'All within 5%')
  check('freeze','Orders respect the exchange freeze quantity','pass' if freeze else 'warn',
   (f'{FREEZE_SOURCE}: {freeze} units per order; larger legs are sliced' if freeze else 'No configured freeze limit for this underlying'))
  st=recognise(legs);unlimited=False;margin=None
  if kind=='close':
   st={'name':'Close: '+recognise(deployment['revision_body'].get('legs',[]))['name']}
   check('risk','Closing orders reduce exposure','pass','Buys back shorts first, then sells longs')
  else:
   a=A.analyze(legs,chain['spot'],A.parse_ist(chain['as_of']),grid_points=3)
   unlimited=bool((a.get('max_loss') or {}).get('unlimited'))
   check('risk','Maximum loss is defined' if not unlimited else 'Unlimited-loss structure','pass' if not unlimited else 'warn',
    'Needs explicit acknowledgement' if unlimited else f"Structural max loss {-(a.get('max_loss') or {}).get('value',0):,.0f}")
  try:
   margin=self.market.basket_margin([{'symbol':o['symbol'],'side':o['side'],'qty':o['qty'],'price':o['limit'],'product':product} for o in orders]) if (orders and kind=='open') else None
  except Exception as e:  # noqa: BLE001 - margin unavailable is a stated state, never a zero
   check('margin','Exchange margin','warn',f'Margin unavailable ({type(e).__name__})')
  if margin:
   free=self.paper_capital(user_id)['available']
   need=margin['final'] or 0
   if kind=='open':check('margin','Paper capital covers the exchange margin','pass' if need<=free else 'block',f'Needs {need:,.0f}; available {free:,.0f}')
  orders.sort(key=lambda o:(o['group'],o['strike']))
  canonical={'strategy':strategy['id'],'version':strategy['draft']['version'],'kind':kind,'deployment':deployment['id'] if deployment else None,
   'product':product,'orders':[{k:o[k] for k in ('leg_id','symbol','side','qty','limit','group','slices')} for o in orders]}
  h=hashlib.sha256(json.dumps(canonical,sort_keys=True).encode()).hexdigest()
  pid=uid();t=time.time()
  blocked=[c for c in checks if c['status']=='block']
  out={'id':pid,'hash':h,'kind':kind,'strategy_id':strategy['id'],'deployment_id':deployment['id'] if deployment else None,'product':product,
   'price_policy':policy,'orders':orders,'checks':checks,'can_submit':not blocked and bool(orders),'requires_ack':unlimited,
   'margin':margin,'charges':round(sum(o['charges'] for o in orders),2),'structure':st['name'],'as_of':chain['as_of'],'spot':chain['spot'],
   'expires_at':t+PREVIEW_TTL,'ttl':PREVIEW_TTL,'mode':'paper','live':LIVE_CAPABILITY,
   'sequence':'Buys (hedges) first; sells only after every buy has filled.' if hedged else 'All legs together.',
   'net_premium':round(sum((-1 if o['side']=='B' else 1)*o['limit']*o['qty'] for o in orders),2)}
  with self.lock:
   self.c.execute('insert into previews values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(pid,user_id,strategy['id'],strategy['draft']['version'],kind,
    out['deployment_id'],product,json.dumps(body),json.dumps(orders),json.dumps(checks),json.dumps(margin),h,t,t+PREVIEW_TTL));self.c.commit()
  return out

 # --- confirm ------------------------------------------------------------------------------------------------------
 def confirm(self,user_id,strategy,preview_id,preview_hash,idem_key,ack_unlimited=False):
  if not idem_key or len(idem_key)>80:raise ExecError(400,'IDEMPOTENCY_KEY_REQUIRED','An idempotency key is required.')
  with self.lock:
   existing=self.c.execute('select * from deployments where user_id=? and idem_key=?',(user_id,idem_key)).fetchone()
   if existing:
    if existing['preview_id']!=preview_id:raise ExecError(409,'IDEMPOTENCY_CONFLICT','This request key was already used for a different order plan.')
    return self.deployment(user_id,existing['id'])
   close_existing=self.c.execute("select d.id from intents i join deployments d on d.id=i.deployment_id where i.preview_id=? and d.user_id=?",(preview_id,user_id)).fetchone()
   if close_existing:return self.deployment(user_id,close_existing['id'])
   p=self.c.execute('select * from previews where id=? and user_id=?',(preview_id,user_id)).fetchone()
  if not p:raise ExecError(404,'PREVIEW_NOT_FOUND','There is no such order preview.')
  if p['hash']!=preview_hash:raise ExecError(409,'PREVIEW_CHANGED','The order plan changed. Review it again.')
  if time.time()>p['expires_at']:raise ExecError(409,'PREVIEW_EXPIRED','This preview has expired - quotes move. Refresh it and review again.')
  if p['kind']=='open' and strategy['draft']['version']!=p['draft_version']:raise ExecError(409,'PREVIEW_CHANGED','The strategy was edited after this preview. Review it again.')
  checks=json.loads(p['checks'])
  if any(c['status']=='block' for c in checks):raise ExecError(409,'CHECKS_FAILED','A pre-trade check blocks these orders.')
  if any(c['key']=='risk' and c['status']=='warn' for c in checks) and not ack_unlimited:
   raise ExecError(400,'ACK_REQUIRED','This structure has unlimited loss. Acknowledge it to continue.')
  orders=json.loads(p['orders']);t=time.time()
  if p['kind']=='open':
   body=json.loads(p['body'])
   rev=self.store.snapshot(user_id,strategy['id'],'Paper deployment entry',body,None,{})
   did=uid()
   with self.lock:
    self.c.execute('insert into deployments values(?,?,?,?,?,?,?,?,?,?,?,?)',(did,user_id,strategy['id'],rev['id'],'paper','submitting',
     p['product'],preview_id,idem_key,p['margin'],t,None))
    self._intents(did,preview_id,'open',orders,t)
    self.store._log(user_id,strategy['id'],'paper_deploy',f'Paper deployment: {sum(len(o["slices"]) for o in orders)} orders created')
    self.c.commit()
  else:
   did=p['deployment_id']
   with self.lock:
    d=self.c.execute('select * from deployments where id=? and user_id=?',(did,user_id)).fetchone()
    if not d or d['status'] not in ('active','attention_required'):raise ExecError(409,'NOT_ACTIVE','Only an active deployment can be closed.')
    self.c.execute("update deployments set status='closing' where id=?",(did,))
    self._intents(did,preview_id,'close',orders,t)
    self.store._log(user_id,strategy['id'],'paper_close',f'Close orders created for paper deployment {did[:6]}')
    self.c.commit()
  self.dispatch(did)
  return self.deployment(user_id,did)

 def _intents(self,did,pid,kind,orders,t):
  seq=0
  for o in orders:
   for q in o['slices']:
    seq+=1
    self.c.execute('insert into intents values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(uid(),did,pid,kind,o['group'],seq,o['leg_id'],o['symbol'],o['token'],
     o['side'],q,o['limit'],'created',0,None,0.0,None,t,t))

 # --- the paper broker -----------------------------------------------------------------------------------------------
 def dispatch(self,did):
  """Acknowledge the next group's intents and try to fill resting intents against live quotes.

  A group is released ONLY when every intent of the group before it is FILLED. A cancelled, rejected or partly
  filled earlier group stops the sequence for good - a hedge that did not complete never lets the short go out.
  Every state change is guarded by the state it expects, so a concurrent cancel is never overwritten.
  """
  for kind in ('open','close'):
   with self.lock:
    groups=[r[0] for r in self.c.execute('select distinct grp from intents where deployment_id=? and kind=? order by grp',(did,kind)).fetchall()]
   for g in groups:
    with self.lock:
     self.c.execute("update intents set state='acknowledged',updated_at=? where deployment_id=? and kind=? and grp=? and state='created'",(time.time(),did,kind,g))
     self.c.commit()
     grp=[dict(r) for r in self.c.execute('select * from intents where deployment_id=? and kind=? and grp=? order by seq',(did,kind,g)).fetchall()]
    self._fill(did,grp)
    with self.lock:
     states=[r[0] for r in self.c.execute('select state from intents where deployment_id=? and kind=? and grp=?',(did,kind,g)).fetchall()]
    if not all(st=='filled' for st in states):break     # the next group waits - or, if this one was cancelled, never goes
  self._roll_up(did)

 def _fill(self,did,intents):
  live=[r for r in intents if r['state'] in ('acknowledged','partially_filled')]
  if not live:return
  if not market_open():return
  try:q=self.market.live_market.quotes([f"NFO:{r['symbol']}" for r in live])
  except Exception as e:  # noqa: BLE001 - no quote means no fill, never a fill at zero
   log.warning('paper fill: quotes unavailable (%s)',type(e).__name__);return
  for r in live:
   d=(q.get(f"NFO:{r['symbol']}") or {}).get('depth') or {}
   bid=(d.get('buy') or [{}])[0].get('price') or None;ask=(d.get('sell') or [{}])[0].get('price') or None
   ts=str((q.get(f"NFO:{r['symbol']}") or {}).get('timestamp') or '')[:19]
   price=None
   if r['side']=='B' and ask and ask<=r['limit_price']:price=ask
   if r['side']=='S' and bid and bid>=r['limit_price']:price=bid
   if price is None:continue
   qty=r['qty']-r['filled_qty'];fees=CH.leg_charges(r['side'],price,qty)['total'];t=time.time()
   with self.lock:
    done=self.c.execute("update intents set state='filled',filled_qty=qty,avg_price=?,fees=fees+?,updated_at=? where id=? and state in ('acknowledged','partially_filled')",
     (price,round(fees,2),t,r['id'])).rowcount
    if done:self.c.execute('insert into dfills values(?,?,?,?,?,?,?,?,?,?,?)',(uid(),r['id'],did,r['leg_id'],r['side'],qty,price,round(fees,2),'paper_quote_cross',ts,t))
    self.c.commit()

 def _roll_up(self,did):
  with self.lock:
   d=self.c.execute('select status from deployments where id=?',(did,)).fetchone()
   rows=self.c.execute('select kind,state from intents where deployment_id=?',(did,)).fetchall()
   if not d:return
   status=d['status']
   opens=[r['state'] for r in rows if r['kind']=='open'];closes=[r['state'] for r in rows if r['kind']=='close']
   if closes and all(s=='filled' for s in closes):status='closed'
   elif closes and any(s in OPEN_STATES for s in closes):status='closing'
   elif closes:status='attention_required'     # a close stopped part-way: a residual position remains
   elif opens and all(s=='filled' for s in opens):status='active'
   elif any(s=='filled' for s in opens):status='partially_filled' if any(s in OPEN_STATES for s in opens) else 'attention_required'
   elif any(s in OPEN_STATES for s in opens):status='working'
   elif opens and all(s=='cancelled' for s in opens):status='cancelled'
   self.c.execute('update deployments set status=?,closed_at=? where id=?',(status,time.time() if status in ('closed','cancelled') else None,did))
   self.c.commit()

 def cancel(self,user_id,did):
  with self.lock:
   d=self.c.execute('select * from deployments where id=? and user_id=?',(did,user_id)).fetchone()
   if not d:raise ExecError(404,'DEPLOYMENT_NOT_FOUND','There is no such deployment.')
   n=self.c.execute("update intents set state='cancelled',reason='cancelled by user',updated_at=? where deployment_id=? and state in ('created','acknowledged','partially_filled')",(time.time(),did)).rowcount
   self.store._log(user_id,d['strategy_id'],'paper_cancel',f'Cancelled {n} resting paper orders')
   self.c.commit()
  self._roll_up(did)
  return self.deployment(user_id,did)

 def _close_body(self,deployment):
  """The legs that flatten what this deployment actually holds (filled quantities, not the plan)."""
  pos={}
  for f in deployment['fills']:
   p=pos.setdefault(f['leg_id'],0);pos[f['leg_id']]=p+(f['qty'] if f['side']=='B' else -f['qty'])
  body=dict(deployment['revision_body']);legs=[]
  for l in body['legs']:
   u=pos.get(l['id'],0)
   if not u:continue
   if not deployment.get('lot_size'):raise ExecError(409,'MARKET_DATA_NOT_LIVE','The lot size could not be read from live data.')
   lots=abs(u)//deployment['lot_size']
   legs.append({**l,'side':'S' if u>0 else 'B','lots':lots,'price_basis':'exec','price':None,'include':True})
  if not legs:raise ExecError(409,'NOTHING_TO_CLOSE','This deployment holds no filled position.')
  return {**body,'legs':legs}

 # --- reads ------------------------------------------------------------------------------------------------------------
 def deployment(self,user_id,did,mark=True):
  with self.lock:
   d=self.c.execute('select * from deployments where id=? and user_id=?',(did,user_id)).fetchone()
   if not d:return None
   intents=[dict(r) for r in self.c.execute('select * from intents where deployment_id=? order by kind,grp,seq',(did,)).fetchall()]
   fills=[dict(r) for r in self.c.execute('select * from dfills where deployment_id=? order by created_at',(did,)).fetchall()]
  out=dict(d);out['margin']=json.loads(out['margin'] or 'null');out['intents']=intents;out['fills']=fills
  rev=self.store.revision(user_id,d['revision_id']);out['revision_body']=rev['body'] if rev else {}
  out['revision']={'id':rev['id'],'n':rev['n'],'name':rev['name']} if rev else None
  out['lot_size']=None;out['live']=LIVE_CAPABILITY
  # positions from fills, marked at live quotes
  pos={}
  for f in fills:
   p=pos.setdefault(f['leg_id'],{'units':0,'cost':0.0,'realised':0.0,'fees':0.0})
   signed=f['qty'] if f['side']=='B' else -f['qty'];p['fees']+=f['fees']
   if p['units']==0 or (p['units']>0)==(signed>0):p['cost']+=signed*f['price'];p['units']+=signed
   else:
    avg=p['cost']/p['units'];closed=min(abs(signed),abs(p['units']))
    p['realised']+=closed*((f['price']-avg) if p['units']>0 else (avg-f['price']))
    p['units']+=signed;p['cost']=avg*p['units']
  marks={};chain=None
  if mark and out['revision_body'].get('underlying'):
   try:
    chain=self.market.chain(out['revision_body']['underlying'],out['revision_body']['expiry'])
    out['lot_size']=chain['lot_size'] if chain else None
    rows={(r['strike']):r for r in (chain or {}).get('rows',[])}
    for l in out['revision_body']['legs']:
     q=(rows.get(l['strike']) or {}).get(l['type']) or {}
     marks[l['id']]={'bid':q.get('bid'),'ask':q.get('ask'),'ltp':q.get('ltp')}
   except Exception:  # noqa: BLE001
    chain=None
  legs=[];unreal=0.0;realised=0.0;fees=0.0;missing=False
  meta={l['id']:l for l in out['revision_body'].get('legs',[])}
  for lid,p in pos.items():
   m=marks.get(lid,{});realised+=p['realised'];fees+=p['fees']
   liq=(m.get('bid') if p['units']>0 else m.get('ask')) or m.get('ltp')
   u=None
   if p['units']:
    if liq is None:missing=True
    else:u=p['units']*liq-p['cost'];unreal+=u
   l=meta.get(lid,{})
   legs.append({'leg_id':lid,'label':f"{int(l.get('strike',0))} {l.get('type','')}",'units':p['units'],'avg':round(p['cost']/p['units'],2) if p['units'] else None,
    'mark':liq,'unrealised':round(u,2) if u is not None else None,'realised':round(p['realised'],2),'fees':round(p['fees'],2)})
  out['positions']=legs;out['realised']=round(realised,2);out['fees']=round(fees,2)
  out['unrealised']=None if missing else round(unreal,2)
  out['net']=None if missing else round(realised+unreal-fees,2)
  out['marked_at']=chain['as_of'] if chain else None
  out['mark_basis']='liquidation: longs at the bid, shorts at the ask'
  return out

 def deployments(self,user_id,strategy_id=None):
  with self.lock:
   q='select id from deployments where user_id=?'+(' and strategy_id=?' if strategy_id else '')+' order by opened_at desc'
   ids=[r[0] for r in self.c.execute(q,(user_id,strategy_id) if strategy_id else (user_id,)).fetchall()]
  return [self.deployment(user_id,i) for i in ids]

 def paper_capital(self,user_id):
  with self.lock:
   rows=self.c.execute("select margin from deployments where user_id=? and status in ('submitting','working','partially_filled','active','closing','attention_required')",(user_id,)).fetchall()
  used=sum((json.loads(r['margin'] or 'null') or {}).get('final') or 0 for r in rows)
  return {'capital':PAPER_CAPITAL,'blocked':round(used,2),'available':round(PAPER_CAPITAL-used,2)}

 # --- the worker: rests orders until live quotes cross them --------------------------------------------------------
 def start(self):
  if self._worker:return
  def run():
   while not self._stop.wait(5):
    try:
     with self.lock:
      ids=[r[0] for r in self.c.execute("select distinct deployment_id from intents where state in ('acknowledged','partially_filled','created')").fetchall()]
     for did in ids:self.dispatch(did)
    except Exception:log.exception('paper worker cycle failed; it retries next cycle')
  self._worker=threading.Thread(target=run,daemon=True,name='sb-paper-broker');self._worker.start()

 def stop(self):
  self._stop.set()
