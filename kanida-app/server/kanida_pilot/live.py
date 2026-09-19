"""Staged Kite execution adapter. This release cannot be armed by configuration.

The entry/protective-exit supervisor and broker operating approval must be
completed before release. Provider acceptance is not a fill. Ambiguous requests
are reconciled by a durable tag and never automatically resubmitted.
"""
from datetime import datetime
from decimal import Decimal
from sqlalchemy import select
from .db import orders,plans,users,record,row,ident,now
from .product import valid_request,body_hash
from .simulation import paise
from .evidence import require_current_evidence,max_data_age
from .kite import IST
from .errors import PilotError
LIVE_RELEASE_APPROVED=False
OPEN=('submitting','confirmation_required','accepted','partial','open','cancel_pending','filled')

def product_for(instrument,side,holding):
 if side not in ('long','short') or holding not in ('intraday','overnight'):raise PilotError(400,'ORDER_POLICY','Choose a supported side and holding policy.')
 kind=instrument.get('instrument_type');exchange=instrument.get('exchange')
 if kind=='EQ' and exchange in ('NSE','BSE'):
  if side=='short' and holding=='overnight':raise PilotError(400,'CASH_SHORT','Cash equity cannot be shorted overnight. An F&O-eligible stock is not itself a futures contract.')
  return 'MIS' if holding=='intraday' else 'CNC'
 if kind in ('FUT','CE','PE') and exchange=='NFO':return 'MIS' if holding=='intraday' else 'NRML'
 raise PilotError(400,'INSTRUMENT','This instrument is not supported by the pilot execution policy.')

def validate_intent(instrument,quote,quantity,price,side,holding,max_notional):
 product=product_for(instrument,side,holding)
 if type(quantity)!=int or quantity<=0 or quantity%int(instrument.get('lot_size') or 1):raise PilotError(400,'QUANTITY','Quantity must be a positive whole number of exchange lots.')
 tick=Decimal(str(instrument.get('tick_size') or 0));limit=Decimal(str(price))
 if not limit.is_finite() or limit<=0 or tick<=0 or limit%tick:raise PilotError(400,'PRICE_TICK','Use a positive limit price on the instrument’s tick size.')
 if now()-int(quote.get('received_at',0))>10 or int(quote.get('received_at',0))>now()+2:raise PilotError(409,'STALE_QUOTE','A fresh broker quote is required.')
 last=Decimal(str(quote.get('last_price',0)))
 if last<=0 or abs(limit-last)/last>Decimal('.005'):raise PilotError(409,'PRICE_MOVED','The limit is more than 0.5% from the verified broker quote. Review the order again.')
 if limit*quantity>Decimal(str(max_notional)):raise PilotError(409,'CAPITAL_LIMIT','This order exceeds the approved maximum notional.')
 return product

class Live:
 def __init__(self,db,kite,settings,evidence=None):self.db=db;self.kite=kite;self.settings=settings;self.evidence=evidence
 def gate(self):
  if not LIVE_RELEASE_APPROVED or not self.settings.live_enabled or not self.settings.live_activation:
   raise PilotError(423,'LIVE_LOCKED','Live trading is disabled in this release. Broker approval, fresh data, protective exits and the operating review must be completed before activation.')
 def submit(self,user,data,verified=None):
  self.gate()
  # Only a server supervisor may supply verified context, never request JSON.
  if not verified or verified.get('user_id')!=user['id']:raise PilotError(423,'EXECUTION_CONTEXT','An approved server execution context is required.')
  request=valid_request(data);hashed=body_hash(data)
  quantity=data.get('quantity');price=data.get('limit_price');side=verified['side'];instrument=verified['instrument']
  product=validate_intent(instrument,verified['quote'],quantity,price,side,verified['holding'],verified['max_notional'])
  if not verified.get('market_open') or not verified.get('protective_exit_ready') or not verified.get('margin_verified'):raise PilotError(409,'EXECUTION_PREFLIGHT','Market, protective exit and margin checks must all pass.')
  if product=='MIS' and datetime.now(IST).strftime('%H:%M')>='15:00':raise PilotError(409,'INTRADAY_CUTOFF','The pilot does not open intraday positions after 15:00 IST.')
  # Stricter refusal only: the draft must still have current exact-rule evidence on fresh stored data
  # (EXIT_EVIDENCE / DATA_STALE / EXIT_EVIDENCE_CHANGED), re-fetched by the server before any intent is recorded.
  with self.db.tx() as c:
   prior=row(c,select(orders.c.id).where(orders.c.user_id==user['id'],orders.c.request_id==request))
   candidate=None if prior else row(c,select(plans).where(plans.c.id==data.get('plan_id'),plans.c.user_id==user['id'],plans.c.status=='draft'))
  checked=None
  if candidate:
   if self.evidence is None:raise PilotError(423,'EVIDENCE_UNAVAILABLE','Plan evidence cannot be re-verified, so live submission is refused.')
   require_current_evidence(self.evidence,candidate['payload'],max_data_age(self.settings));checked=candidate['payload']
  with self.db.tx() as c:
   c.execute(select(users.c.id).where(users.c.id==user['id']).with_for_update()).first()
   old=row(c,select(orders).where(orders.c.user_id==user['id'],orders.c.request_id==request))
   if old:
    if old['request_hash']!=hashed:raise PilotError(409,'REQUEST_REUSED','This instruction identifier belongs to another order.')
    return old
   plan=row(c,select(plans).where(plans.c.id==data.get('plan_id'),plans.c.user_id==user['id'],plans.c.status=='draft'))
   if not plan:raise PilotError(409,'PLAN_REQUIRED','Review an owned active draft first.')
   if checked is None or plan['payload']!=checked:raise PilotError(409,'PLAN_CHANGED','This plan changed while its evidence was being checked. Review it again.')
   pending=[o for o in c.execute(select(orders).where(orders.c.user_id==user['id'],orders.c.mode=='live')).mappings() if o['status'] in OPEN or o['payload'].get('reserved_paise',0)>0]
   if any(o['plan_id']==plan['id'] for o in pending):raise PilotError(409,'ALREADY_ACTIVE','This plan already has an unresolved live order.')
   reserved=sum(o['payload'].get('reserved_paise',0) for o in pending)
   amount=paise(quantity*float(price))+int(verified['cost_reserve_paise'])
   if reserved+amount>int(verified['capital_limit_paise']):raise PilotError(409,'CAPITAL_LIMIT','Outstanding commitments exceed the approved capital.')
   identity=ident();tag='KN'+identity[:18]
   payload={'exchange':instrument['exchange'],'tradingsymbol':instrument['tradingsymbol'],'transaction_type':'BUY' if side=='long' else 'SELL','quantity':quantity,'price':float(price),'product':product,'order_type':'LIMIT','validity':'DAY','tag':tag}
   value=dict(id=identity,user_id=user['id'],plan_id=plan['id'],request_id=request,request_hash=hashed,mode='live',symbol=instrument['tradingsymbol'],status='submitting',product=product,
    payload=dict(instruction=payload,quantity=quantity,reserved_paise=amount,side=side),created=now(),updated=now(),filled_qty=0,average_paise=0,realized_paise=0)
   c.execute(orders.insert().values(**value));record(c,user['id'],'broker','Live order intent recorded','Awaiting provider confirmation.',identity)
  try:provider_id=str(self.kite.call(user,'POST','orders/regular',payload)['order_id'])
  except Exception:
   with self.db.tx() as c:c.execute(orders.update().where(orders.c.id==identity).values(status='confirmation_required',updated=now()))
   raise PilotError(502,'ORDER_UNCONFIRMED','The broker response is uncertain. Reconcile this instruction; do not submit another order.') from None
  with self.db.tx() as c:c.execute(orders.update().where(orders.c.id==identity).values(broker_id=provider_id,status='accepted',updated=now()))
  return dict(value,broker_id=provider_id,status='accepted')
 def reconcile(self,user):
  book=self.kite.call(user,'GET','orders')
  with self.db.tx() as c:
   values=c.execute(select(orders).where(orders.c.user_id==user['id'],orders.c.mode=='live').with_for_update()).mappings().all()
   for value in values:
    instruction=value['payload']['instruction']
    found=[o for o in book if (value['broker_id'] and str(o.get('order_id'))==value['broker_id']) or o.get('tag')==instruction['tag']]
    if len(found)!=1:continue
    broker=found[0]
    if any(str(broker.get(k))!=str(instruction[k]) for k in ('tradingsymbol','exchange','transaction_type','product','quantity')):continue
    filled=int(broker.get('filled_quantity') or 0)
    if not value['filled_qty']<=filled<=instruction['quantity']:continue
    status={'COMPLETE':'filled','CANCELLED':'cancelled','REJECTED':'rejected','OPEN':'open','TRIGGER PENDING':'open'}.get(broker.get('status'),'accepted')
    if filled and status in ('accepted','open'):status='partial'
    payload=dict(value['payload'],broker_status=broker.get('status'),reason=str(broker.get('status_message') or '')[:300])
    # Cancelled partial fills retain their commitment until a position supervisor reconciles exits.
    if status in ('cancelled','rejected') and filled==0:payload['reserved_paise']=0
    c.execute(orders.update().where(orders.c.id==value['id']).values(status=status,broker_id=str(broker['order_id']),filled_qty=filled,average_paise=paise(broker.get('average_price') or 0),payload=payload,updated=now()))
    if status!=value['status']:record(c,user['id'],'broker','Broker order '+status,value['symbol']+' · reconciled with Kite.',value['id'])
  return {'ok':True}
 def cancel(self,user,identity):
  self.gate()
  with self.db.tx() as c:
   value=row(c,select(orders).where(orders.c.id==identity,orders.c.user_id==user['id'],orders.c.mode=='live').with_for_update())
   if not value or not value['broker_id']:raise PilotError(409,'RECONCILE_FIRST','Reconcile the owned order before cancellation.')
   if value['status'] not in ('accepted','open','partial'):raise PilotError(409,'ORDER_STATE','This order is not cancellable.')
   c.execute(orders.update().where(orders.c.id==identity).values(status='cancel_pending',updated=now()))
  try:self.kite.call(user,'DELETE','orders/regular/'+value['broker_id'])
  finally:self.reconcile(user)
  return {'ok':True}
