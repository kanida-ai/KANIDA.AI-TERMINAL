from __future__ import annotations
import hashlib,hmac,json,threading,time
from urllib.parse import urlencode
from sqlalchemy import select
from .db import subscriptions,webhooks,users,record,ident,now,row
from .errors import PilotError

# Strictly increasing request stamps. Windows time.time_ns() can return the same value for requests a few milliseconds apart, and apply() drops a stamp that is not newer than the stored one.
_stamp_lock=threading.Lock();_last_stamp=0
def next_stamp():
 global _last_stamp
 with _stamp_lock:
  _last_stamp=max(time.time_ns(),_last_stamp+1);return _last_stamp

class Billing:
 def __init__(self,db,settings,http):self.db=db;self.settings=settings;self.http=http
 def require(self):
  if not self.settings.billing_ready:raise PilotError(503,'BILLING_SETUP','Razorpay test checkout is awaiting owner setup. No payment has been taken.')
 def call(self,method,path,body=None):
  self.require()
  request_stamp=next_stamp()
  try:
   response=self.http.request(method,'https://api.razorpay.com/v1/'+path,auth=(self.settings.razorpay_key_id,self.settings.razorpay_key_secret),json=body)
   response.raise_for_status();value=response.json()
   if isinstance(value,dict):value['_kanida_request_stamp']=request_stamp
   return value
  except Exception:raise PilotError(502,'RAZORPAY_UNAVAILABLE','Razorpay could not confirm this request. Check the billing status before trying again.') from None
 def latest(self,c,user):return row(c,select(subscriptions).where(subscriptions.c.user_id==user['id']).order_by(subscriptions.c.created.desc()))
 def status(self,user):
  with self.db.tx() as c:sub=self.latest(c,user)
  grant=(user.get('preferences') or {}).get('pilot_access_until',0)>now()
  entitled=user['role']=='owner' or grant or bool(sub and sub['status'] in ('active','cancelled') and (sub['current_end'] or 0)>now())
  return dict(provider='razorpay',mode='test',configured=self.settings.billing_ready,access=entitled,
   access_reason='Owner pilot access' if user['role']=='owner' else 'Included pilot access' if grant else 'Test subscription' if entitled else 'Subscription required',
   subscription={k:sub[k] for k in ('id','status','current_end','cancel_at_end','updated')} if sub else None)
 def create(self,user):
  self.require()
  with self.db.tx() as c:
   c.execute(select(users.c.id).where(users.c.id==user['id']).with_for_update()).first()
   existing=self.latest(c,user)
   if existing and existing['status'] not in ('completed','expired','cancelled','failed'):
    if existing['provider_id'] and existing['checkout_url']:return dict(id=existing['id'],url=existing['checkout_url'],status=existing['status'])
    raise PilotError(409,'BILLING_PENDING','A checkout request is being resolved. Refresh status; another subscription will not be created.')
   identity=ident()
   c.execute(subscriptions.insert().values(id=identity,user_id=user['id'],status='creating',created=now(),updated=now(),last_event=0,cancel_at_end=False))
  try:
   value=self.call('POST','subscriptions',dict(plan_id=self.settings.razorpay_plan_id,total_count=12,quantity=1,customer_notify=0,notes={'kanida_reference':identity}))
   if not isinstance(value.get('id'),str) or not value.get('short_url','').startswith('https://rzp.io/'):
    raise PilotError(502,'CHECKOUT_RESPONSE','The checkout response could not be verified.')
  except PilotError:
   with self.db.tx() as c:c.execute(subscriptions.update().where(subscriptions.c.id==identity).values(status='confirmation_required',updated=now()))
   raise
  with self.db.tx() as c:
   c.execute(subscriptions.update().where(subscriptions.c.id==identity).values(provider_id=value['id'],plan_id=value.get('plan_id'),checkout_url=value['short_url'],status=value['status'],updated=now()))
   record(c,user['id'],'billing','Test checkout prepared','Razorpay will collect test payment details. Access is confirmed by the server.',identity)
  return dict(id=identity,url=value['short_url'],status=value['status'])
 def synchronize(self,user):
  with self.db.tx() as c:sub=self.latest(c,user)
  if sub and not sub['provider_id'] and sub['status'] in ('creating','confirmation_required'):
   found=[]
   for skip in range(0,1000,100):
    page=self.call('GET','subscriptions?'+urlencode({'plan_id':self.settings.razorpay_plan_id,'from':sub['created']-60,'count':100,'skip':skip}))
    items=page.get('items',[])
    found.extend(v for v in items if (v.get('notes') or {}).get('kanida_reference')==sub['id'] and v.get('plan_id')==self.settings.razorpay_plan_id)
    if len(items)<100:break
   if len(found)==1:
    value=found[0]
    if not str(value.get('short_url','')).startswith('https://rzp.io/'):raise PilotError(502,'CHECKOUT_RESPONSE','The recovered checkout URL could not be verified.')
    with self.db.tx() as c:
     current=row(c,select(subscriptions).where(subscriptions.c.id==sub['id']).with_for_update())
     if not current['provider_id']:
      c.execute(subscriptions.update().where(subscriptions.c.id==sub['id']).values(provider_id=value['id'],checkout_url=value['short_url'],plan_id=value['plan_id'],updated=now()))
      record(c,user['id'],'billing','Checkout recovered','The uncertain request was matched to its unique provider reference.',sub['id'])
    sub=dict(sub,provider_id=value['id'])
   else:raise PilotError(409,'BILLING_PENDING','The previous checkout could not yet be matched uniquely. The owner must review it before another subscription is created.')
  if sub and sub['provider_id']:
   value=self.call('GET','subscriptions/'+sub['provider_id']);self.apply(value)
  return self.status(user)
 def apply(self,value):
  if value.get('plan_id')!=self.settings.razorpay_plan_id:raise PilotError(400,'PLAN_MISMATCH','The subscription does not belong to this pilot plan.')
  with self.db.tx() as c:
   sub=row(c,select(subscriptions).where(subscriptions.c.provider_id==value.get('id')).with_for_update())
   if not sub:return
   request_stamp=value.get('_kanida_request_stamp') or next_stamp()
   if request_stamp<=sub['last_event']:return
   status=value.get('status','unknown');end=value.get('current_end')
   if status not in ('created','authenticated','active','pending','halted','cancelled','completed','expired'):status='unknown'
   # An authorization payment alone does not grant a paid service period.
   paid=int(value.get('paid_count') or 0)>0
   if not paid:end=None
   changed=status!=sub['status'] or end!=sub['current_end']
   c.execute(subscriptions.update().where(subscriptions.c.id==sub['id']).values(status=status,current_end=end,last_event=request_stamp,updated=now()))
   if changed:record(c,sub['user_id'],'billing','Subscription '+status,'Razorpay test subscription status verified.',sub['id'])
 def cancel(self,user):
  with self.db.tx() as c:sub=self.latest(c,user)
  if not sub or not sub['provider_id']:raise PilotError(404,'NO_SUBSCRIPTION','There is no subscription to cancel.')
  self.call('POST','subscriptions/'+sub['provider_id']+'/cancel',{'cancel_at_cycle_end':1})
  self.apply(self.call('GET','subscriptions/'+sub['provider_id']))
  with self.db.tx() as c:
   c.execute(subscriptions.update().where(subscriptions.c.id==sub['id']).values(cancel_at_end=True,updated=now()))
   record(c,user['id'],'billing','Cancellation requested','Test subscription cancellation requested at the end of the current cycle.',sub['id'])
  return self.status(user)
 def webhook(self,raw,signature):
  self.require()
  expected=hmac.new(self.settings.razorpay_webhook_secret.encode(),raw,hashlib.sha256).hexdigest()
  if not signature or not hmac.compare_digest(expected,signature):raise PilotError(400,'WEBHOOK_SIGNATURE','Invalid webhook signature.')
  stamp=hashlib.sha256(raw).hexdigest();identity='razorpay:'+stamp
  with self.db.tx() as c:
   if row(c,select(webhooks).where(webhooks.c.id==identity)):return {'ok':True,'duplicate':True}
  try:payload=json.loads(raw);provider_id=payload.get('payload',{}).get('subscription',{}).get('entity',{}).get('id')
  except (ValueError,AttributeError):raise PilotError(400,'WEBHOOK_BODY','Invalid webhook body.')
  if provider_id:
   with self.db.tx() as c:known=row(c,select(subscriptions).where(subscriptions.c.provider_id==provider_id))
   if known:
    # Fetch canonical status rather than applying potentially delayed, reordered events.
    self.apply(self.call('GET','subscriptions/'+provider_id))
  with self.db.tx() as c:
   if not row(c,select(webhooks).where(webhooks.c.id==identity)):
    c.execute(webhooks.insert().values(id=identity,provider='razorpay',created=now(),digest=stamp))
  return {'ok':True}
