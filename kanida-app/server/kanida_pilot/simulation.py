from __future__ import annotations
from decimal import Decimal,ROUND_HALF_UP
from sqlalchemy import select,func
from .db import orders,plans,wallets,record,ident,now,row
from .product import valid_request,body_hash
from .evidence import require_current_evidence,max_data_age
from .errors import PilotError

def paise(value):return int((Decimal(str(value))*100).quantize(Decimal('1'),rounding=ROUND_HALF_UP))
CASES={'target':[0,.35,-.15,.8,1.5,2.2],'stop':[0,.25,-.3,-1.1],
 'gap':[0,.4,-1.8],'partial':[0,.2,.3,.8,2.2],'rejected':[]}
ACTIVE=('queued','partial','open')
OUTCOME_NOTE='Outcomes of price scenarios you chose in Review test order. Synthetic, after assumed costs; not market, historical or broker results, and not added to capital.'
def available_capital(wallet):return wallet['initial_paise']-wallet['reserved_paise']
LOSS_LIMIT_FRACTION=.02
def loss_limit(wallet):return int(wallet['initial_paise']*LOSS_LIMIT_FRACTION)
def realized_losses(c,user_id):
 """Cumulative realized simulated LOSSES only (sum of negative outcomes, as a positive number).
 Gains from chosen synthetic 'target' scenarios must never offset or postpone the loss limit."""
 value=c.execute(select(func.coalesce(func.sum(orders.c.realized_paise),0)).where(orders.c.user_id==user_id,orders.c.mode=='simulation',orders.c.realized_paise<0)).scalar()
 return -int(value or 0)

class Simulation:
 """Deterministic workflow fixtures, isolated from both research and broker data.

 Paths are explicit test scenarios, never historical probabilities or forecasts.
 Each transition and its cash ledger mutation share one database transaction.
 """
 def __init__(self,db,evidence=None,settings=None):self.db=db;self.evidence=evidence;self.settings=settings
 def verify(self,payload):
  if self.evidence is None:raise PilotError(503,'EVIDENCE_UNAVAILABLE','Plan evidence cannot be re-verified right now, so new simulated entries are refused.')
  return require_current_evidence(self.evidence,payload,max_data_age(self.settings))
 def submit(self,user,data):
  request=valid_request(data);hashed=body_hash(data);case=data.get('scenario','target')
  if case not in CASES:raise PilotError(400,'SIMULATION_CASE','Choose a supported simulation scenario.')
  if data.get('acknowledge_synthetic') is not True:raise PilotError(400,'SIMULATION_CONSENT','Confirm that this simulation uses synthetic prices.')
  # Evidence and data age are re-verified server-side (network fetch) before the ledger transaction.
  # Retries of an existing request skip it and return the recorded order. Plan payloads are immutable;
  # the payload is compared again under the wallet lock so the verified plan is the one reserved.
  with self.db.tx() as c:
   prior=row(c,select(orders.c.id).where(orders.c.user_id==user['id'],orders.c.request_id==request))
   candidate=None if prior else row(c,select(plans).where(plans.c.id==data.get('plan_id'),plans.c.user_id==user['id']))
  checked=None
  if candidate and candidate['status']=='draft':checked=dict(payload=candidate['payload'],evidence=self.verify(candidate['payload']))
  with self.db.tx() as c:
   wallet=row(c,select(wallets).where(wallets.c.user_id==user['id']).with_for_update())
   old=row(c,select(orders).where(orders.c.user_id==user['id'],orders.c.request_id==request))
   if old:
    if old['request_hash']!=hashed:raise PilotError(409,'REQUEST_REUSED','This request identifier was used for another instruction.')
    return old
   if wallet['paused']:raise PilotError(409,'PAUSED','New simulated entries are paused. Existing positions can still exit.')
   plan=row(c,select(plans).where(plans.c.id==data.get('plan_id'),plans.c.user_id==user['id']))
   if not plan or plan['status']!='draft':raise PilotError(409,'PLAN_REQUIRED','Choose an active draft plan.')
   if checked is None or plan['payload']!=checked['payload']:raise PilotError(409,'PLAN_CHANGED','This plan changed while its evidence was being checked. Review it and retry.')
   other=row(c,select(orders).where(orders.c.plan_id==plan['id'],orders.c.user_id==user['id'],orders.c.status.in_(ACTIVE)))
   if other:raise PilotError(409,'ALREADY_ACTIVE','This plan already has an active simulated order.')
   p=plan['payload'];notional=paise(p['notional']);cost=paise(p['reserved_cost']);reserve=notional+cost
   # Capital accounting excludes chosen-scenario outcomes: a user-picked synthetic path must never grow spendable capital.
   available=available_capital(wallet)
   if reserve>available:raise PilotError(409,'FUNDS','This simulation exceeds your available virtual cash.')
   initial=wallet['initial_paise']
   if realized_losses(c,user['id'])>loss_limit(wallet):raise PilotError(409,'LOSS_LIMIT','Realized simulated losses exceed 2% of the virtual account, so new entries are paused. Gains from chosen scenarios do not offset this limit.')
   if paise(p['planned_risk'])>initial*LOSS_LIMIT_FRACTION:raise PilotError(409,'RISK_LIMIT','Planned risk exceeds 2% of the virtual account.')
   identity=ident();payload=dict(plan=p,scenario=case,step=-1,next_at=now()+3,reserve_paise=reserve,notional_paise=notional,cost_paise=cost,evidence_verified=checked['evidence'],
    mark_paise=paise(p['snapshot_price']),mfe_pct=0,mae_pct=0,source='synthetic_workflow_fixture',holding_policy='Delivery' if p['side']=='long' else 'Same-session short simulation',
    model='Unleveraged virtual collateral; 0.40% research cost assumption, not broker charges.')
   value=dict(id=identity,user_id=user['id'],plan_id=plan['id'],request_id=request,request_hash=hashed,mode='simulation',symbol=p['symbol'],status='queued',
    product='CNC' if p['side']=='long' else 'MIS',payload=payload,filled_qty=0,average_paise=0,realized_paise=0,created=now(),updated=now())
   c.execute(orders.insert().values(**value));c.execute(wallets.update().where(wallets.c.user_id==user['id']).values(reserved_paise=wallet['reserved_paise']+reserve))
   record(c,user['id'],'simulation','Simulated order queued',p['symbol']+' · synthetic '+case+' scenario. No broker order.',identity)
  return value
 def advance(self,identity,user_id=None,force=False):
  with self.db.tx() as c:
   seed=row(c,select(orders).where(orders.c.id==identity))
   if not seed or user_id and seed['user_id']!=user_id:raise PilotError(404,'ORDER_NOT_FOUND','Order not found.')
   wallet=row(c,select(wallets).where(wallets.c.user_id==seed['user_id']).with_for_update())
   item=row(c,select(orders).where(orders.c.id==identity).with_for_update())
   if not item or user_id and item['user_id']!=user_id:raise PilotError(404,'ORDER_NOT_FOUND','Order not found.')
   if item['mode']!='simulation' or item['status'] not in ACTIVE:return item
   p=dict(item['payload']);plan=p['plan']
   if not force and p['next_at']>now():return item
   status=item['status'];qty=item['filled_qty'];avg=item['average_paise'];realized=0
   if p['scenario']=='rejected':status='rejected';p['reason']='Synthetic broker rejection used to test recovery.'
   elif p['step']==-1:
    if wallet['paused']:return item
    p['step']=0;qty=max(1,plan['quantity']//2) if p['scenario']=='partial' and plan['quantity']>1 else plan['quantity'];status='partial' if qty<plan['quantity'] else 'open';avg=paise(plan['snapshot_price'])
   elif status=='partial':qty=plan['quantity'];status='open'
   else:
    p['step']+=1;path=CASES[p['scenario']];r=path[min(p['step'],len(path)-1)];sign=1 if plan['side']=='long' else -1
    move=r*plan['stop_pct'];mark=plan['snapshot_price']*(1+sign*move/100);p['mark_paise']=paise(max(.01,mark))
    p['mfe_pct']=max(p['mfe_pct'],move);p['mae_pct']=max(p['mae_pct'],-move)
    if r<=-1:status='closed';p['reason']='Gap beyond stop' if p['scenario']=='gap' else 'Stop reached'
    elif r>=plan['reward']:status='closed';p['reason']='Target reached'
    elif p['step']>=len(path)-1:status='closed';p['reason']='Synthetic holding limit reached'
    if status=='closed':realized=(p['mark_paise']-avg)*qty*sign-p['cost_paise']
   p['next_at']=now()+5
   if status in ('closed','rejected'):
    c.execute(wallets.update().where(wallets.c.user_id==item['user_id']).values(reserved_paise=max(0,wallet['reserved_paise']-p['reserve_paise']),
     cash_paise=wallet['cash_paise']+realized,realized_paise=wallet['realized_paise']+realized))
   c.execute(orders.update().where(orders.c.id==identity).values(payload=p,status=status,filled_qty=qty,average_paise=avg,realized_paise=realized,updated=now()))
   if status!=item['status']:record(c,item['user_id'],'simulation','Simulated order '+status,item['symbol']+' · '+p.get('reason','Synthetic workflow update.'),identity)
   return dict(item,payload=p,status=status,filled_qty=qty,average_paise=avg,realized_paise=realized,updated=now())
 def tick(self):
  with self.db.tx() as c:ids=list(c.execute(select(orders.c.id).where(orders.c.mode=='simulation',orders.c.status.in_(ACTIVE))).scalars())
  for identity in ids:self.advance(identity)
 def cancel(self,user,identity):
  with self.db.tx() as c:
   wallet=row(c,select(wallets).where(wallets.c.user_id==user['id']).with_for_update())
   item=row(c,select(orders).where(orders.c.id==identity,orders.c.user_id==user['id']).with_for_update())
   if not item or item['mode']!='simulation':raise PilotError(404,'ORDER_NOT_FOUND','Simulated order not found.')
   if item['status']!='queued':raise PilotError(409,'ORDER_STATE','Only an unfilled queued order can be cancelled. Use Exit for an open position.')
   c.execute(wallets.update().where(wallets.c.user_id==user['id']).values(reserved_paise=max(0,wallet['reserved_paise']-item['payload']['reserve_paise'])))
   c.execute(orders.update().where(orders.c.id==identity).values(status='cancelled',updated=now()))
   record(c,user['id'],'simulation','Simulated order cancelled',item['symbol']+' · virtual cash released.',identity)
  return {'ok':True}
 def exit(self,user,identity):
  with self.db.tx() as c:
   wallet=row(c,select(wallets).where(wallets.c.user_id==user['id']).with_for_update())
   item=row(c,select(orders).where(orders.c.id==identity,orders.c.user_id==user['id']).with_for_update())
   if not item or item['mode']!='simulation':raise PilotError(404,'ORDER_NOT_FOUND','Simulated position not found.')
   if item['status'] not in ('open','partial'):raise PilotError(409,'ORDER_STATE','This simulated position is not open.')
   p=dict(item['payload']);sign=1 if p['plan']['side']=='long' else -1
   cost=round(p['cost_paise']*item['filled_qty']/p['plan']['quantity']);pnl=(p['mark_paise']-item['average_paise'])*item['filled_qty']*sign-cost
   p['reason']='Closed by you in simulation'
   c.execute(wallets.update().where(wallets.c.user_id==user['id']).values(reserved_paise=max(0,wallet['reserved_paise']-p['reserve_paise']),cash_paise=wallet['cash_paise']+pnl,realized_paise=wallet['realized_paise']+pnl))
   c.execute(orders.update().where(orders.c.id==identity).values(payload=p,status='closed',realized_paise=pnl,updated=now()))
   record(c,user['id'],'simulation','Simulated position closed',item['symbol']+' · virtual outcome recorded.',identity)
  return {'ok':True}
 def pause(self,user,paused):
  with self.db.tx() as c:
   c.execute(wallets.update().where(wallets.c.user_id==user['id']).values(paused=bool(paused)))
   record(c,user['id'],'risk','New entries '+('paused' if paused else 'resumed'),'Open simulated positions continue to their exits.')
  return {'ok':True}
 def state(self,user):
  with self.db.tx() as c:
   wallet=row(c,select(wallets).where(wallets.c.user_id==user['id']))
   values=[dict(r) for r in c.execute(select(orders).where(orders.c.user_id==user['id']).order_by(orders.c.created.desc()).limit(150)).mappings()]
   count=c.execute(select(func.count()).select_from(orders).where(orders.c.user_id==user['id'],orders.c.mode=='simulation',orders.c.status=='closed')).scalar() or 0
   losses=realized_losses(c,user['id'])
  # cash_paise/realized_paise keep their ledger meaning; these derived fields separate capital from chosen-scenario outcomes.
  if wallet:wallet=dict(wallet,available_capital_paise=available_capital(wallet),scenario_outcome_total_paise=wallet['realized_paise'],scenario_outcome_count=count,scenario_outcome_note=OUTCOME_NOTE,
   realized_loss_paise=losses,loss_limit_paise=loss_limit(wallet),loss_limit_reached=losses>loss_limit(wallet))
  for o in values:
   if o['mode']=='simulation':o['outcome_kind']='chosen_scenario_synthetic'
  return dict(wallet=wallet,orders=values,live_enabled=False,simulation_source='Synthetic prices; not Kite data and not measured investment performance.')
