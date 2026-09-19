from __future__ import annotations
import hashlib,hmac
from datetime import datetime,timedelta,timezone
from urllib.parse import urlencode
from cryptography.fernet import Fernet
from sqlalchemy import select
from .auth import digest,token
from .db import brokers,oauth,record,now,row
from .errors import PilotError
IST=timezone(timedelta(hours=5,minutes=30))

def next_expiry(stamp=None):
 current=datetime.fromtimestamp(stamp or now(),IST);six=current.replace(hour=6,minute=0,second=0,microsecond=0)
 if current>=six:six+=timedelta(days=1)
 return int(six.timestamp())

class Kite:
 def __init__(self,db,settings,http,auth):self.db=db;self.settings=settings;self.http=http;self.auth=auth;self.cipher=Fernet(settings.encryption_key.encode())
 def start(self,user,binding):
  if not self.settings.kite_ready:raise PilotError(503,'KITE_SETUP','Kite Connect is awaiting owner setup.')
  state=token()
  with self.db.tx() as c:c.execute(oauth.insert().values(hash=digest(state),provider='kite',user_id=user['id'],binding=digest(binding),expires=now()+600,used=False))
  return 'https://kite.zerodha.com/connect/login?'+urlencode({'v':3,'api_key':self.settings.kite_api_key,'redirect_params':urlencode({'state':state})})
 def callback(self,state,binding,request_token):
  flow=self.auth.consume_flow(state,binding,'kite')
  checksum=hashlib.sha256((self.settings.kite_api_key+request_token+self.settings.kite_api_secret).encode()).hexdigest()
  try:
   response=self.http.post('https://api.kite.trade/session/token',data={'api_key':self.settings.kite_api_key,'request_token':request_token,'checksum':checksum},headers={'X-Kite-Version':'3'})
   response.raise_for_status();value=response.json()['data'];access=value['access_token'];account=str(value['user_id'])
   if not account or not access:raise ValueError('missing account')
  except Exception:raise PilotError(401,'KITE_LOGIN_FAILED','Kite could not complete the connection. Please sign in again.') from None
  with self.db.tx() as c:
   other=row(c,select(brokers).where(brokers.c.account_hash==digest(account)))
   if other and other['user_id']!=flow['user_id']:raise PilotError(409,'BROKER_OWNER','This broker account is already connected to another pilot account.')
   fields=dict(broker='kite',account_hash=digest(account),masked_id='•••'+account[-3:],encrypted_token=self.cipher.encrypt(access.encode()).decode(),expires=next_expiry(),status='connected',updated=now())
   if row(c,select(brokers).where(brokers.c.user_id==flow['user_id'])):c.execute(brokers.update().where(brokers.c.user_id==flow['user_id']).values(**fields))
   else:c.execute(brokers.insert().values(user_id=flow['user_id'],**fields))
   record(c,flow['user_id'],'broker','Kite connected','Broker authorization is connected. Live execution remains independently gated.')
  return flow['user_id']
 def status(self,user):
  with self.db.tx() as c:value=row(c,select(brokers).where(brokers.c.user_id==user['id']))
  status=value['status'] if value else 'not_connected'
  if value and value['expires'] and value['expires']<=now() and status=='connected':status='reconnect_required'
  return dict(broker='kite',configured=self.settings.kite_ready,status=status,account=value['masked_id'] if value else None,expires=value['expires'] if value else None,live_enabled=False)
 def call(self,user,method,path,data=None):
  with self.db.tx() as c:value=row(c,select(brokers).where(brokers.c.user_id==user['id']))
  if not value or value['status']!='connected' or (value['expires'] or 0)<=now():raise PilotError(401,'BROKER_RECONNECT','Reconnect Kite before continuing.')
  access=self.cipher.decrypt(value['encrypted_token'].encode()).decode()
  try:
   params={'api_key':self.settings.kite_api_key,'access_token':access} if method=='DELETE' and path=='session/token' else None
   response=self.http.request(method,'https://api.kite.trade/'+path,data=data,params=params,headers={'X-Kite-Version':'3','Authorization':'token '+self.settings.kite_api_key+':'+access})
   if response.status_code in (401,403):
    with self.db.tx() as c:c.execute(brokers.update().where(brokers.c.user_id==user['id']).values(status='reconnect_required',encrypted_token=None,updated=now()))
    raise PilotError(401,'BROKER_RECONNECT','Kite authorization expired. Reconnect your account.')
   response.raise_for_status();return response.json()['data']
  except PilotError:raise
  except Exception:raise PilotError(502,'BROKER_UNCONFIRMED','Kite has not confirmed the request. Reconcile its status before retrying.') from None
 def disconnect(self,user):
  # Revoke the provider token before local deletion; failure remains visible for retry.
  status=self.status(user)
  if status['status']=='connected':self.call(user,'DELETE','session/token')
  with self.db.tx() as c:
   c.execute(brokers.update().where(brokers.c.user_id==user['id']).values(status='disconnected',encrypted_token=None,expires=now(),updated=now()))
   record(c,user['id'],'broker','Kite disconnected','Stored broker authorization removed. Existing broker positions are not closed.')
  return self.status(user)
 def reconcile(self,user):
  result={key:self.call(user,'GET',path) for key,path in [('orders','orders'),('positions','portfolio/positions'),('holdings','portfolio/holdings')]}
  return result
 def verify_postback(self,payload):
  value=str(payload.get('order_id',''))+str(payload.get('order_timestamp',''))+self.settings.kite_api_secret
  expected=hashlib.sha256(value.encode()).hexdigest()
  if not self.settings.kite_api_secret or not hmac.compare_digest(expected,str(payload.get('checksum',''))):raise PilotError(400,'KITE_CHECKSUM','Invalid broker update checksum.')
