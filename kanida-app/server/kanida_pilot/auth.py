from __future__ import annotations
import base64, hashlib, hmac, re, secrets
from threading import BoundedSemaphore
from urllib.parse import urlencode
from argon2 import PasswordHasher
from argon2.exceptions import VerificationError,InvalidHashError
from cryptography.fernet import Fernet
from sqlalchemy import select, and_
from .db import users,invites,sessions,oauth,rate_limits,wallets,record,ident,now,row
from .errors import PilotError

class BoundedHasher:
 def __init__(self):self.hasher=PasswordHasher(time_cost=3,memory_cost=65536,parallelism=2);self.capacity=BoundedSemaphore(2)
 def hash(self,value):
  with self.capacity:return self.hasher.hash(value)
 def verify(self,hashed,value):
  with self.capacity:return self.hasher.verify(hashed,value)
 def check_needs_rehash(self,hashed):return self.hasher.check_needs_rehash(hashed)
ph=BoundedHasher()
DUMMY=ph.hash('not-a-real-account-password')
def digest(value:str):return hashlib.sha256(value.encode()).hexdigest()
def token():return secrets.token_urlsafe(32)
def email_value(value):
 value=str(value).strip().lower()
 if len(value)>254 or not re.fullmatch(r'[^\s@]+@[^\s@]+\.[^\s@]+',value):raise PilotError(400,'EMAIL','Enter a valid email address.')
 return value
def password_value(value):
 if not isinstance(value,str) or not 12<=len(value)<=256:raise PilotError(400,'PASSWORD','Use a password between 12 and 256 characters.')
 return value
def public_user(user):return {k:user[k] for k in ('id','email','name','role','onboarded','preferences','created')}

class Auth:
 def __init__(self,db,settings,http):self.db=db;self.settings=settings;self.http=http;self.cipher=Fernet(settings.encryption_key.encode())
 def limit(self,key,maximum=12,period=900):
  key=digest(key);stamp=now()
  with self.db.tx() as c:
   item=row(c,select(rate_limits).where(rate_limits.c.key==key).with_for_update())
   if not item:c.execute(rate_limits.insert().values(key=key,window=stamp,count=1))
   elif stamp-item['window']>=period:c.execute(rate_limits.update().where(rate_limits.c.key==key).values(window=stamp,count=1))
   elif item['count']>=maximum:raise PilotError(429,'SLOW_DOWN','Too many attempts. Please try again later.')
   else:c.execute(rate_limits.update().where(rate_limits.c.key==key).values(count=item['count']+1))
 def invite(self,email,role='member',hours=72):
  email=email_value(email)
  if role not in ('owner','member'):raise PilotError(400,'ROLE','Unknown account role.')
  raw=token()
  with self.db.tx() as c:
   c.execute(invites.insert().values(hash=digest(raw),email=email,role=role,expires=now()+hours*3600))
  return raw
 def consume_invite(self,c,raw,email):
  invite=row(c,select(invites).where(invites.c.hash==digest(raw)).with_for_update())
  if not invite or invite['used'] or invite['expires']<=now() or not hmac.compare_digest(invite['email'],email):
   raise PilotError(403,'INVITE_REQUIRED','Use a valid private-pilot invitation for this email.')
  c.execute(invites.update().where(invites.c.hash==invite['hash']).values(used=now()))
  return invite['role']
 def create_user(self,c,email,name,role,password_hash=None,sub=None):
  user=dict(id=ident(),email=email,name=name[:80] or email.split('@')[0],role=role,password_hash=password_hash,google_sub=sub,
   active=True,onboarded=False,policy_version=None,preferences={},created=now())
  c.execute(users.insert().values(**user))
  c.execute(wallets.insert().values(user_id=user['id'],initial_paise=10000000,cash_paise=10000000,reserved_paise=0,realized_paise=0,paused=False))
  record(c,user['id'],'account','Account created','Private pilot invitation accepted.')
  return user
 def register(self,email,password,name,invite,policy):
  email=email_value(email);password_value(password)
  if policy!=self.settings.policy_version:raise PilotError(400,'CONSENT','Review and accept the current private pilot terms.')
  hashed=ph.hash(password)
  with self.db.tx() as c:
   if row(c,select(users).where(users.c.email==email)):raise PilotError(409,'ACCOUNT_EXISTS','An account already exists. Sign in instead.')
   role=self.consume_invite(c,invite,email)
   user=self.create_user(c,email,str(name),role,hashed)
   c.execute(users.update().where(users.c.id==user['id']).values(policy_version=policy))
   return user
 def login(self,email,password):
  email=email_value(email)
  if not isinstance(password,str) or len(password)>256:raise PilotError(401,'SIGN_IN_FAILED','Email or password is incorrect.')
  with self.db.tx() as c:user=row(c,select(users).where(users.c.email==email))
  try:valid=ph.verify((user or {}).get('password_hash') or DUMMY,password)
  except (VerificationError,InvalidHashError):valid=False
  if not valid or not user or not user['active'] or not user['password_hash']:raise PilotError(401,'SIGN_IN_FAILED','Email or password is incorrect.')
  if ph.check_needs_rehash(user['password_hash']):
   with self.db.tx() as c:c.execute(users.update().where(users.c.id==user['id']).values(password_hash=ph.hash(password)))
  return user
 def issue(self,user,kind='web',label=''):
  if kind not in ('web','native'):raise PilotError(400,'SESSION_KIND','Unknown session type.')
  raw=token();csrf=token()
  with self.db.tx() as c:
   c.execute(sessions.insert().values(hash=digest(raw),user_id=user['id'],csrf=csrf,kind=kind,created=now(),expires=now()+7*86400,revoked=False,label=label[:120]))
   record(c,user['id'],'session','Signed in',f'A {kind} session was opened.')
  return raw,csrf
 def resolve(self,raw,kind=None):
  if not raw or len(raw)>256:return None
  with self.db.tx() as c:
   session=row(c,select(sessions).where(sessions.c.hash==digest(raw)))
   if not session or session['revoked'] or session['expires']<=now() or kind and session['kind']!=kind:return None
   user=row(c,select(users).where(users.c.id==session['user_id']))
  return (user,session) if user and user['active'] else None
 def revoke(self,user,raw=None,all_devices=False):
  with self.db.tx() as c:
   query=sessions.update().where(sessions.c.user_id==user['id'])
   if not all_devices:query=query.where(sessions.c.hash==digest(raw or ''))
   c.execute(query.values(revoked=True));record(c,user['id'],'session','Signed out','All sessions revoked.' if all_devices else 'This session was revoked.')
 def start_google(self,binding,invite=''):
  if not self.settings.google_ready:raise PilotError(503,'GOOGLE_NOT_CONFIGURED','Google sign-in is awaiting pilot setup. You can use your invitation and password.')
  state=token();nonce=token();verifier=token()
  with self.db.tx() as c:
   c.execute(oauth.insert().values(hash=digest(state),provider='google',binding=digest(binding),nonce=nonce,verifier=self.cipher.encrypt(verifier.encode()).decode(),
    expires=now()+600,used=False,return_path=invite if len(invite)<=100 else ''))
  challenge=base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip('=')
  return 'https://accounts.google.com/o/oauth2/v2/auth?'+urlencode(dict(client_id=self.settings.google_client_id,response_type='code',scope='openid email profile',
   redirect_uri=self.settings.origin+'/api/auth/google/callback',state=state,nonce=nonce,code_challenge=challenge,code_challenge_method='S256'))
 def consume_flow(self,state,binding,provider):
  with self.db.tx() as c:
   flow=row(c,select(oauth).where(oauth.c.hash==digest(state)).with_for_update())
   if not flow or flow['used'] or flow['expires']<=now() or flow['provider']!=provider or not hmac.compare_digest(flow['binding'],digest(binding)):
    raise PilotError(400,'LOGIN_EXPIRED','This connection request expired. Start again from KANIDA.')
   c.execute(oauth.update().where(oauth.c.hash==flow['hash']).values(used=True))
   return flow
 def google_callback(self,state,binding,code):
  flow=self.consume_flow(state,binding,'google')
  try:
   response=self.http.post('https://oauth2.googleapis.com/token',data=dict(code=code,client_id=self.settings.google_client_id,client_secret=self.settings.google_client_secret,
    redirect_uri=self.settings.origin+'/api/auth/google/callback',grant_type='authorization_code',code_verifier=self.cipher.decrypt(flow['verifier'].encode()).decode()))
   response.raise_for_status()
   from google.oauth2 import id_token
   from google.auth.transport.requests import Request
   claims=id_token.verify_oauth2_token(response.json()['id_token'],Request(),self.settings.google_client_id)
   if claims.get('nonce')!=flow['nonce'] or claims.get('email_verified') is not True:raise ValueError('claims')
   email=email_value(claims['email']);sub=claims['sub']
  except Exception:raise PilotError(401,'GOOGLE_FAILED','Google sign-in could not be verified. Please start again.') from None
  with self.db.tx() as c:
   user=row(c,select(users).where(users.c.google_sub==sub))
   if not user:
    existing=row(c,select(users).where(users.c.email==email))
    if existing:
     # A verified Google identity may attach only to the same invited email.
     if existing['google_sub'] and existing['google_sub']!=sub:raise PilotError(403,'IDENTITY_MISMATCH','Sign in using the account originally invited.')
     c.execute(users.update().where(users.c.id==existing['id']).values(google_sub=sub));user=existing
    else:
     role=self.consume_invite(c,flow['return_path'] or '',email)
     user=self.create_user(c,email,claims.get('name',''),role,sub=sub)
   if not user['active']:raise PilotError(403,'ACCOUNT_DISABLED','This pilot account has been disabled.')
   return user
