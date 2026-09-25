"""Slice 5 — strategy alerts. NOTIFY ONLY: nothing here places, modifies or cancels an order (blueprint B K13).

A rule watches ONE strategy's research draft or ONE paper deployment's actual position. It is evaluated on live
quotes by a worker. Semantics, chosen so a rule never storms:
  * it TRIGGERS on the transition into its condition, once;
  * it RE-ARMS only after the condition has cleared by a hysteresis margin (so a price sitting on the line does not
    fire again and again), and never within its cooldown;
  * a time rule fires once and then EXPIRES;
  * if its inputs are unavailable (market data not live, a leg without a quote) it goes DATA_UNAVAILABLE and records
    that once - it never fires on a missing or zero value - and records RECOVERED when inputs return;
  * outside its session window (market hours by default) it is not evaluated.
Every evaluation stores the value and time it saw; every state change is an event the user can acknowledge.

Rule types
  price_cross     underlying crosses a level            params {level, direction: above|below}
  breakeven_near  spot within X points of a breakeven    params {points}
  pnl             deployment net P&L past a threshold    params {amount, direction: loss|profit}   (deployment scope)
  delta           |net delta| at or above a limit        params {units}
  expiry_time     a reminder on a date/time (IST)        params {at: 'YYYY-MM-DD HH:MM'}
  short_itm       a short leg goes in the money          params {}
"""
from __future__ import annotations
import json,threading,time,uuid,logging
from datetime import datetime
from . import analytics as A
from .execution import market_open,now_ist

log=logging.getLogger('strategy_builder.alerts')
TYPES={
 'price_cross':{'label':'Underlying crosses a level','scopes':('strategy','deployment')},
 'breakeven_near':{'label':'Spot near a breakeven','scopes':('strategy','deployment')},
 'pnl':{'label':'Position P&L threshold','scopes':('deployment',)},
 'delta':{'label':'Net delta limit','scopes':('strategy','deployment')},
 'expiry_time':{'label':'Reminder at a time','scopes':('strategy','deployment')},
 'short_itm':{'label':'A short leg goes in the money','scopes':('strategy','deployment')},
}
STATES=('armed','triggered','paused','data_unavailable','expired')
MAX_RULES_PER_USER=100
SCHEMA='''
create table if not exists alert_rules(
 id text primary key, user_id text not null, strategy_id text not null, deployment_id text, type text not null, params text not null,
 session text not null, cooldown integer not null, channels text not null, state text not null, version integer not null,
 last_eval_at text, last_value real, last_triggered_at real, note text, created_at real not null, updated_at real not null, deleted integer not null default 0);
create index if not exists ix_rule_user on alert_rules(user_id, deleted);
create table if not exists alert_events(
 id text primary key, rule_id text not null, user_id text not null, strategy_id text not null, deployment_id text, kind text not null,
 value real, message text not null, occurred_at text not null, created_at real not null, acked_at real);
create index if not exists ix_evt_user on alert_events(user_id, acked_at, created_at);
'''


class AlertError(Exception):
 def __init__(self,status,code,message):super().__init__(message);self.status=status;self.code=code;self.message=message


def uid():return uuid.uuid4().hex[:16]


def validate(rtype,params,scope):
 if rtype not in TYPES:raise AlertError(400,'ALERT_TYPE','Unknown alert type.')
 if scope not in TYPES[rtype]['scopes']:raise AlertError(400,'ALERT_SCOPE',f"{TYPES[rtype]['label']} needs a paper deployment - a research draft has no actual position.")
 p=params if isinstance(params,dict) else {}
 def num(k,positive=True):
  try:v=float(p.get(k))
  except (TypeError,ValueError):raise AlertError(400,'FIELD_INVALID',f'{k} must be a number.')
  if positive and v<=0:raise AlertError(400,'FIELD_INVALID',f'{k} must be positive.')
  return v
 if rtype=='price_cross':
  d=p.get('direction')
  if d not in ('above','below'):raise AlertError(400,'FIELD_INVALID','direction must be above or below.')
  return {'level':num('level'),'direction':d}
 if rtype=='breakeven_near':return {'points':num('points')}
 if rtype=='pnl':
  d=p.get('direction')
  if d not in ('loss','profit'):raise AlertError(400,'FIELD_INVALID','direction must be loss or profit.')
  return {'amount':num('amount'),'direction':d}
 if rtype=='delta':return {'units':num('units')}
 if rtype=='expiry_time':
  at=A.parse_ist(p.get('at'))
  if not at:raise AlertError(400,'FIELD_INVALID','at must be YYYY-MM-DD HH:MM (IST).')
  return {'at':at.strftime('%Y-%m-%d %H:%M')}
 return {}


def describe(rtype,p):
 if rtype=='price_cross':return f"Underlying {'rises above' if p['direction']=='above' else 'falls below'} {p['level']:,.2f}"
 if rtype=='breakeven_near':return f"Spot within {p['points']:,.0f} points of a breakeven"
 if rtype=='pnl':return f"Net P&L {'loses' if p['direction']=='loss' else 'makes'} ₹{p['amount']:,.0f} or more"
 if rtype=='delta':return f"|Net delta| reaches {p['units']:,.0f} units"
 if rtype=='expiry_time':return f"Reminder at {p['at']} IST"
 return 'A short leg goes in the money'


# --- pure evaluation ------------------------------------------------------------------------------------------------
def evaluate(rtype,p,ctx,state):
 """(available, value, condition, rearm_ok, message). `ctx` carries live inputs; `state` is the rule's current state.

 condition   the alert condition is true now
 rearm_ok    the condition has cleared by the hysteresis margin (a triggered rule may re-arm)
 """
 now=ctx.get('now')
 if rtype=='expiry_time':
  due=A.parse_ist(p['at'])<=now
  return True,None,due,False,f"Reminder: {p['at']} IST"
 spot=ctx.get('spot')
 if rtype=='price_cross':
  if spot is None:return False,None,False,False,'No live spot'
  lvl=p['level'];pad=lvl*0.001
  cond=spot>=lvl if p['direction']=='above' else spot<=lvl
  clear=spot<lvl-pad if p['direction']=='above' else spot>lvl+pad
  return True,spot,cond,clear,f"Spot {spot:,.2f} {'is above' if p['direction']=='above' else 'is below'} {lvl:,.2f}"
 if rtype=='breakeven_near':
  bes=ctx.get('breakevens')
  if spot is None or not bes:return False,None,False,False,'No live spot or breakevens'
  near=min(bes,key=lambda b:abs(b-spot));dist=abs(near-spot)
  return True,dist,dist<=p['points'],dist>p['points']*1.5,f"Spot {spot:,.2f} is {dist:,.0f} points from the breakeven {near:,.0f}"
 if rtype=='pnl':
  pnl=ctx.get('net')
  if pnl is None:return False,None,False,False,'P&L unavailable (a leg has no mark)'
  a=p['amount']
  if p['direction']=='loss':cond=pnl<=-a;clear=pnl>-a*0.9
  else:cond=pnl>=a;clear=pnl<a*0.9
  return True,pnl,cond,clear,f"Net P&L {pnl:+,.0f} (threshold {'-' if p['direction']=='loss' else '+'}₹{a:,.0f})"
 if rtype=='delta':
  d=ctx.get('delta')
  if d is None:return False,None,False,False,'Delta unavailable'
  return True,d,abs(d)>=p['units'],abs(d)<p['units']*0.8,f"Net delta {d:+,.1f} units (limit {p['units']:,.0f})"
 if rtype=='short_itm':
  shorts=ctx.get('shorts')
  if spot is None or shorts is None:return False,None,False,False,'No live spot'
  itm=[s for s in shorts if (s['type']=='CE' and spot>s['strike']) or (s['type']=='PE' and spot<s['strike'])]
  return True,len(itm),bool(itm),not itm,('In the money: '+', '.join(f"{int(s['strike'])} {s['type']}" for s in itm)) if itm else 'No short leg is in the money'
 return False,None,False,False,'Unknown rule'


def step(state,available,cond,rearm_ok,last_trig,cooldown,now_ts,rtype):
 """Next state and whether to emit an event: (state, event_kind or None)."""
 if state in ('paused','expired'):return state,None
 if not available:return ('data_unavailable','data_unavailable') if state!='data_unavailable' else (state,None)
 if state=='data_unavailable':state='armed';recovered=True
 else:recovered=False
 if state=='armed' and cond:
  if last_trig and now_ts-last_trig<cooldown:return state,('recovered' if recovered else None)
  return ('expired' if rtype=='expiry_time' else 'triggered'),'triggered'
 if state=='triggered' and rearm_ok and not cond:return 'armed','rearmed'
 return state,('recovered' if recovered else None)


class Alerts:
 def __init__(self,store,market,execution):
  self.store=store;self.market=market;self.execution=execution;self.c=store.c;self.lock=store.lock
  with self.lock:self.c.executescript(SCHEMA);self.c.commit()
  self._stop=threading.Event();self._worker=None

 # --- CRUD ---------------------------------------------------------------------------------------------------------
 def _row(self,r):
  if not r:return None
  d=dict(r);d['params']=json.loads(d['params']);d['channels']=json.loads(d['channels']);d.pop('deleted',None)
  d['label']=TYPES[d['type']]['label'];d['description']=describe(d['type'],d['params']);d['scope']='deployment' if d['deployment_id'] else 'strategy'
  return d

 def create(self,user_id,strategy_id,data,deployment=None):
  rtype=str(data.get('type') or '');scope='deployment' if deployment else 'strategy'
  params=validate(rtype,data.get('params'),scope)
  session=data.get('session') if data.get('session') in ('market','always') else 'market'
  if rtype=='expiry_time':session='always'
  try:cooldown=max(60,min(86400,int(data.get('cooldown') or 900)))
  except (TypeError,ValueError):raise AlertError(400,'FIELD_INVALID','cooldown must be a number of seconds.')
  channels=[c for c in (data.get('channels') or ['in_app']) if c in ('in_app','browser')] or ['in_app']
  with self.lock:
   n=self.c.execute('select count(*) from alert_rules where user_id=? and deleted=0',(user_id,)).fetchone()[0]
   if n>=MAX_RULES_PER_USER:raise AlertError(409,'ALERT_LIMIT',f'You can keep up to {MAX_RULES_PER_USER} alert rules.')
   rid=uid();t=time.time()
   self.c.execute('insert into alert_rules values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(rid,user_id,strategy_id,deployment['id'] if deployment else None,rtype,
    json.dumps(params),session,cooldown,json.dumps(channels),'armed',1,None,None,None,str(data.get('note') or '')[:140],t,t,0))
   self.store._log(user_id,strategy_id,'alert_created',f'Alert created: {describe(rtype,params)}')
   self.c.commit()
  return self.rule(user_id,rid)

 def rule(self,user_id,rid):
  with self.lock:r=self.c.execute('select * from alert_rules where id=? and user_id=? and deleted=0',(rid,user_id)).fetchone()
  return self._row(r)

 def rules(self,user_id,strategy_id=None):
  q='select * from alert_rules where user_id=? and deleted=0'+(' and strategy_id=?' if strategy_id else '')+' order by created_at desc'
  with self.lock:rows=self.c.execute(q,(user_id,strategy_id) if strategy_id else (user_id,)).fetchall()
  return [self._row(r) for r in rows]

 def update(self,user_id,rid,data):
  r=self.rule(user_id,rid)
  if not r:raise AlertError(404,'ALERT_NOT_FOUND','There is no such alert.')
  try:version=int(data.get('version'))
  except (TypeError,ValueError):raise AlertError(400,'FIELD_INVALID','version is required.')
  if version!=r['version']:raise AlertError(409,'VERSION_CONFLICT','This alert changed elsewhere. Reload it.')
  params=validate(r['type'],data['params'],r['scope']) if 'params' in data else r['params']
  state=r['state']
  if data.get('action')=='pause':state='paused'
  elif data.get('action')=='resume':state='armed' if r['state'] in ('paused','triggered','data_unavailable') else r['state']
  elif 'params' in data and r['state']!='paused':state='armed'      # an edited rule starts fresh
  with self.lock:
   self.c.execute('update alert_rules set params=?,state=?,version=version+1,updated_at=? where id=?',(json.dumps(params),state,time.time(),rid))
   self.c.commit()
  return self.rule(user_id,rid)

 def delete(self,user_id,rid):
  r=self.rule(user_id,rid)
  if not r:raise AlertError(404,'ALERT_NOT_FOUND','There is no such alert.')
  with self.lock:self.c.execute('update alert_rules set deleted=1,updated_at=? where id=?',(time.time(),rid));self.c.commit()
  return {'ok':True}

 def events(self,user_id,strategy_id=None,limit=100,unacked=False):
  q='select * from alert_events where user_id=?'+(' and strategy_id=?' if strategy_id else '')+(' and acked_at is null' if unacked else '')+' order by created_at desc limit ?'
  args=[user_id]+([strategy_id] if strategy_id else [])+[limit]
  with self.lock:rows=self.c.execute(q,args).fetchall()
  return [dict(r) for r in rows]

 def unacked(self,user_id):
  with self.lock:return self.c.execute("select count(*) from alert_events where user_id=? and acked_at is null and kind in ('triggered','data_unavailable')",(user_id,)).fetchone()[0]

 def ack(self,user_id,eid=None):
  with self.lock:
   if eid:n=self.c.execute('update alert_events set acked_at=? where id=? and user_id=? and acked_at is null',(time.time(),eid,user_id)).rowcount
   else:n=self.c.execute('update alert_events set acked_at=? where user_id=? and acked_at is null',(time.time(),user_id)).rowcount
   self.c.commit()
  return {'acknowledged':n}

 # --- evaluation -------------------------------------------------------------------------------------------------
 def context(self,user_id,rule,cache):
  """Live inputs for a rule: spot, breakevens, delta, shorts, and (deployment scope) net P&L. Missing = None."""
  now=now_ist();ctx={'now':now}
  status=self.market.status() if hasattr(self.market,'status') else {'live':False}
  if not status.get('live'):return ctx
  if rule['deployment_id']:
   key=('d',rule['deployment_id'])
   if key not in cache:
    d=self.execution.deployment(user_id,rule['deployment_id'])
    held=[]
    if d:
     body=d['revision_body'];meta={l['id']:l for l in body.get('legs',[])}
     for p in d['positions']:
      if p['units']:
       l=meta.get(p['leg_id'],{})
       held.append({**l,'side':'B' if p['units']>0 else 'S','lots':abs(p['units'])//(d.get('lot_size') or 1),'price_basis':'manual','price':p['avg'],'include':True})
    cache[key]=(d,held)
   d,held=cache[key]
   if not d:return ctx
   ctx['net']=d['net']
   body={**d['revision_body'],'legs':held}
  else:
   s=self.store.get(user_id,rule['strategy_id'])
   if not s:return ctx
   body=s['draft']['body']
  if not body.get('legs'):
   try:
    got=self.market.reading(body['underlying']);ctx['spot']=got[1] if got else None
   except Exception:pass  # noqa: BLE001
   return ctx
  from . import service as S
  key=('a',json.dumps(body,sort_keys=True))
  if key not in cache:
   try:cache[key]=S.analysis(self.market,S.normalize_body(body),table=False)
   except Exception:cache[key]=None  # noqa: BLE001
  a=cache[key]
  if not a or a.get('status') not in ('ok','partial'):return ctx
  ctx['spot']=a.get('spot')
  if (a.get('breakevens') or {}).get('status')=='available':ctx['breakevens']=a['breakevens']['value']
  if (a.get('greeks') or {}).get('status')=='available':ctx['delta']=a['greeks']['delta']
  ctx['shorts']=[{'type':l['type'],'strike':l['strike']} for l in body['legs'] if l.get('side')=='S' and l.get('include',True)]
  return ctx

 def evaluate_rule(self,user_id,rule,cache=None,record=True):
  cache=cache if cache is not None else {}
  if rule['session']=='market' and not market_open():return {'evaluated':False,'reason':'Outside market hours'}
  ctx=self.context(user_id,rule,cache)
  available,value,cond,rearm_ok,message=evaluate(rule['type'],rule['params'],ctx,rule['state'])
  out={'evaluated':True,'available':available,'value':value,'condition':cond,'message':message,'at':ctx['now'].strftime('%Y-%m-%d %H:%M:%S')}
  if not record:return out
  new,event=step(rule['state'],available,cond,rearm_ok,rule['last_triggered_at'],rule['cooldown'],time.time(),rule['type'])
  t=time.time()
  with self.lock:
   cur=self.c.execute('select state,version from alert_rules where id=? and deleted=0',(rule['id'],)).fetchone()
   if not cur or cur['version']!=rule['version']:return {**out,'skipped':'changed during evaluation'}   # edited meanwhile
   self.c.execute('update alert_rules set state=?,last_eval_at=?,last_value=?,last_triggered_at=? where id=?',(new,out['at'],value,
    t if event=='triggered' else rule['last_triggered_at'],rule['id']))
   if event:
    text={'triggered':f"{describe(rule['type'],rule['params'])} - {message}",'rearmed':f"Re-armed: {message}",
     'data_unavailable':f"Not evaluated: {message}. The alert will not fire on missing data.",'recovered':'Live inputs are back; the alert is armed.'}[event]
    self.c.execute('insert into alert_events values(?,?,?,?,?,?,?,?,?,?,?)',(uid(),rule['id'],user_id,rule['strategy_id'],rule['deployment_id'],event,value,text,out['at'],t,
     t if event in ('rearmed','recovered') else None))
   self.c.commit()
  return {**out,'state':new,'event':event}

 def cycle(self):
  with self.lock:rows=self.c.execute("select * from alert_rules where deleted=0 and state not in ('paused','expired')").fetchall()
  cache={}
  for r in rows:
   try:self.evaluate_rule(r['user_id'],self._row(r),cache)
   except Exception:log.exception('alert rule %s failed to evaluate',r['id'])

 def start(self,every=15):
  if self._worker:return
  def run():
   while not self._stop.wait(every):
    try:self.cycle()
    except Exception:log.exception('alert cycle failed; retrying next cycle')
  self._worker=threading.Thread(target=run,daemon=True,name='sb-alerts');self._worker.start()

 def stop(self):self._stop.set()
