from __future__ import annotations
import hmac,json,logging,threading
from contextlib import asynccontextmanager
from pathlib import Path
import httpx
from fastapi import FastAPI,Request,Body
from fastapi.responses import JSONResponse,RedirectResponse,FileResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from .config import Settings
from .db import Database,users,sessions,brokers,orders,oauth,record,now,row
from .auth import Auth,public_user,token,digest
from .billing import Billing
from .kite import Kite
from .evidence import Evidence,PATHS
from .product import Product
from .simulation import Simulation
from .live import Live
from .strategies import Strategies
from .snapshots import SnapshotStore,SnapshotWorker,events_from_snapshots,rows_for_api
from .signal_noise import SignalNoise,markdown as sn_markdown
from .research_store import ResearchStore
from .research_index import ResearchIndex
from .pattern_history import PatternHistoryService
from .detections import LiveDetections,SCOPES as DETECTION_SCOPES,ROW_LIMIT_MAX as DETECTION_LIMIT_MAX
from .derivatives import (Derivatives,OPTION_TYPES as DERIVATIVE_TYPES,ROW_LIMIT_MAX as DERIVATIVE_LIMIT_MAX,
 SERIES_LIMIT_MAX as DERIVATIVE_POINTS_MAX,CHART_INTERVALS as DERIVATIVE_INTERVALS,
 DEFAULT_CHART_INTERVAL as DERIVATIVE_INTERVAL_DEFAULT,SCREENER_LIMIT_MAX as DERIVATIVE_SCREENER_LIMIT_MAX,
 clean_expiry,clean_symbol)
from .errors import PilotError

COOKIE='kanida_session';BINDING='kanida_flow'

def create_app(settings=None,http=None,evidence=None):
 settings=settings or Settings.load();db=Database(settings.database_url);db.migrate()
 http=http or httpx.Client(timeout=18,follow_redirects=False)
 auth=Auth(db,settings,http);billing=Billing(db,settings,http);kite=Kite(db,settings,http,auth)
 evidence=evidence or Evidence(settings,http);product=Product(db,evidence);simulation=Simulation(db,evidence,settings)
 live_service=Live(db,kite,settings,evidence)
 # Researched pattern catalogue + its precomputed Discover index. Both are read-only on market_scanner/ and a
 # missing or unreadable research tree degrades to "evidence loading" instead of breaking the server.
 store=index=None
 try:
  store=ResearchStore(settings.pattern_research_directory,settings.pattern_research_run,settings.pattern_catalogue_path,settings.evidence_release_path)
  if store.available():index=ResearchIndex(store,settings.pattern_index_path,settings.pattern_index_refresh_seconds,
   settings.pattern_labels_database)
  else:store=None
 except Exception:
  logging.getLogger('pilot').warning('Pattern research is unavailable; Discover will serve the stored scan only.');store=index=None
 # Live detections of those same researched patterns, read-only from the scanner's detection ledger
 # (docs/LIVE_DETECTION.md §5A). Absent, or holding no `detections` table because the scanner is running the
 # legacy pattern set, simply means live_detection stays False and Discover serves the researched history.
 detections=None
 try:
  detections=LiveDetections(settings.pattern_detection_db,settings.pattern_detection_cache_seconds,
   labels=(index.labels if index else None))
 except Exception:
  logging.getLogger('pilot').warning('The live detection ledger is unavailable; Discover will serve the researched history only.');detections=None
 registry=Strategies(db,evidence,settings,store,index,detections);registry.seed()
 pattern_history=PatternHistoryService(store,settings.pattern_history_path) if store else None
 # Derivative tab (docs/DERIVATIVES_SPEC.md §4). READ-ONLY on db/derivatives.db; the capture and metrics
 # workers own every write. Constructing it never touches the file, so a machine without one still boots.
 derivatives=Derivatives(settings.derivatives_database)
 # IMMUTABLE READING SNAPSHOTS: written once per reading by a worker with its OWN reader, served by the routes below.
 snapshot_store=SnapshotStore(settings.intelligence_database)
 snapshot_worker=SnapshotWorker(Derivatives(settings.derivatives_database),snapshot_store)
 # SIGNAL-TO-NOISE AUDIT on those snapshots: claims recorded once, outcomes appended, verdicts with reasons.
 signal_noise=SignalNoise(settings.intelligence_database)
 sn_reports=Path(settings.intelligence_database).parent/'sn_reports'
 def audit(session):
  engine_version,rules_version=snapshot_worker.version()
  if not signal_noise.index_names:
   try:signal_noise.index_names=set(derivatives.status().get('index_underlyings') or [])
   except Exception:pass  # noqa: BLE001 - instrument type is a label, never a reason to stop the audit
  signal_noise.record(session,engine_version,rules_version);signal_noise.evaluate(session,engine_version)
  # the report is a VIEW over the immutable records, rewritten as outcomes accumulate; the EOD one once closed
  sn_reports.mkdir(parents=True,exist_ok=True)
  (sn_reports/f'SN_{session}.md').write_text(sn_markdown(signal_noise.report(session,engine_version)),encoding='utf-8')
 @asynccontextmanager
 async def lifespan(app):
  stop=threading.Event()
  def tick():
   while not stop.wait(2):
    try:simulation.tick()
    except Exception:logging.getLogger('pilot').error('Simulation worker transition failed; it will reconcile on the next cycle.')
  worker=threading.Thread(target=tick,daemon=True,name='pilot-simulation');worker.start()
  snaps=None
  if str(settings.snapshots).lower()!='off':
   snaps=threading.Thread(target=snapshot_worker.run,args=(stop,),kwargs={'after':audit},daemon=True,name='pilot-snapshots');snaps.start()
  yield
  stop.set();worker.join(timeout=5)
  if snaps:snaps.join(timeout=5)
  snapshot_worker.engine.close();http.close();db.close()
 app=FastAPI(title='KANIDA Private Pilot',docs_url=None,redoc_url=None,openapi_url=None,lifespan=lifespan)
 app.state.db=db;app.state.auth=auth;app.state.billing=billing;app.state.kite=kite;app.state.simulation=simulation;app.state.evidence=evidence;app.state.live=live_service;app.state.strategies=registry;app.state.derivatives=derivatives;app.state.pattern_history=pattern_history
 app.add_middleware(CORSMiddleware,allow_origins=settings.origins or [settings.origin],allow_credentials=True,allow_methods=['GET','POST'],allow_headers=['Content-Type','Authorization','X-Kanida-CSRF','X-Kanida-Client'])

 @app.exception_handler(PilotError)
 async def known_error(_,error):return JSONResponse({'error':error.message,'code':error.code},status_code=error.status)
 @app.exception_handler(IntegrityError)
 async def conflict(_,error):return JSONResponse({'error':'This operation conflicts with an existing record. Refresh before retrying.','code':'CONFLICT'},status_code=409)
 @app.exception_handler(Exception)
 async def unknown(_,error):
  logging.getLogger('pilot').error('Request failed: %s',type(error).__name__)
  return JSONResponse({'error':'KANIDA could not complete this request. Your saved state is preserved.','code':'INTERNAL'},status_code=500)

 @app.middleware('http')
 async def protect(request,call_next):
  path=request.url.path
  if request.method in ('POST','PUT','PATCH','DELETE'):
   max_size=1048576 if path.endswith('/webhook') or path.endswith('/postback') else 65536
   body=await request.body()
   if len(body)>max_size:return JSONResponse({'error':'Request is too large.'},status_code=413)
   if not (path.endswith('/webhook') or path.endswith('/postback')):
    origin=request.headers.get('origin')
    if origin and origin not in settings.origins:return JSONResponse({'error':'Request origin is not permitted.'},status_code=403)
    if request.headers.get('content-type','').split(';')[0]!='application/json':return JSONResponse({'error':'JSON request required.'},status_code=415)
  response=await call_next(request)
  response.headers['X-Content-Type-Options']='nosniff';response.headers['Referrer-Policy']='no-referrer';response.headers['X-Frame-Options']='DENY'
  response.headers['Cache-Control']='no-store';response.headers['Permissions-Policy']='camera=(), microphone=(), geolocation=()'
  response.headers['Content-Security-Policy']="default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'"
  if settings.secure:response.headers['Strict-Transport-Security']='max-age=31536000'
  return response

 def identity(request,required=True):
  header=request.headers.get('authorization','');native=header.startswith('Bearer ')
  raw=header[7:] if native else request.cookies.get(COOKIE,'')
  result=auth.resolve(raw,'native' if native else 'web')
  if not result:
   if required:raise PilotError(401,'SIGN_IN_REQUIRED','Sign in to your private pilot account.')
   return None
  user,session=result
  if request.method!='GET' and not native:
   csrf=request.headers.get('x-kanida-csrf','')
   if not hmac.compare_digest(csrf,session['csrf']):raise PilotError(403,'CSRF','Refresh the page before continuing.')
  request.state.raw_session=raw;request.state.session=session
  return user
 def member(request):
  user=identity(request)
  if not user['onboarded']:raise PilotError(403,'ONBOARDING_REQUIRED','Complete your pilot setup first.')
  if not billing.status(user)['access']:raise PilotError(402,'ACCESS_REQUIRED','Your pilot subscription needs attention.')
  return user
 def owner(request):
  user=identity(request)
  if user['role']!='owner':raise PilotError(403,'OWNER_REQUIRED','This action is available to the pilot owner.')
  return user
 def session_response(user,request):
  native=request.headers.get('x-kanida-client')=='native';raw,csrf=auth.issue(user,'native' if native else 'web',request.headers.get('user-agent',''))
  result={'user':public_user(user),'csrf':csrf}
  if native:result['access_token']=raw
  response=JSONResponse(result)
  if not native:response.set_cookie(COOKIE,raw,max_age=7*86400,httponly=True,secure=settings.secure,samesite='lax',path='/')
  return response

 @app.get('/health')
 def health():
  with db.tx() as c:c.execute(select(users.c.id).limit(1)).first()
  return {'ok':True,'app':'KANIDA Private Pilot','live_enabled':False}
 @app.get('/api/pilot/config')
 def config():return {'pilot':True,'google':settings.google_ready,'billing':settings.billing_ready,'kite':settings.kite_ready,'live_enabled':False,
  'policy_version':settings.policy_version,'origin':settings.origin,'mobile_origin':settings.mobile_origin,'billing_mode':'test',
  'registration_mode':settings.registration_mode,'invitation_required':settings.invitation_required}
 @app.get('/api/auth/me')
 def me(request:Request):
  user=identity(request,False)
  return {'user':public_user(user) if user else None,'csrf':request.state.session['csrf'] if user else None,'billing':billing.status(user) if user else None}
 @app.post('/api/auth/register')
 def register(request:Request,data:dict=Body(...)):
  auth.limit('register:'+str(request.client.host),8)
  user=auth.register(data.get('email'),data.get('password'),data.get('name',''),data.get('invite',''),data.get('policy_version'))
  return session_response(user,request)
 @app.post('/api/auth/login')
 def login(request:Request,data:dict=Body(...)):
  auth.limit('login:'+str(request.client.host),40);auth.limit('email:'+str(data.get('email','')).lower(),12)
  return session_response(auth.login(data.get('email'),data.get('password')),request)
 @app.post('/api/auth/logout')
 def logout(request:Request,data:dict=Body(default={})):
  user=identity(request);auth.revoke(user,request.state.raw_session,bool(data.get('all_devices')))
  response=JSONResponse({'ok':True});response.delete_cookie(COOKIE,path='/');return response
 @app.post('/api/auth/password')
 def password(request:Request,data:dict=Body(...)):
  user=identity(request);auth.limit('password:'+user['id'],5)
  auth.login(user['email'],data.get('current_password'))
  from .auth import password_value,ph
  new_hash=ph.hash(password_value(data.get('password')))
  with db.tx() as c:
   c.execute(users.update().where(users.c.id==user['id']).values(password_hash=new_hash));c.execute(sessions.update().where(sessions.c.user_id==user['id']).values(revoked=True))
   record(c,user['id'],'account','Password changed','All prior sessions were revoked.')
  return session_response(user,request)
 @app.post('/api/admin/recovery')
 def recovery_link(request:Request,data:dict=Body(...)):
  acting=owner(request)
  from .auth import email_value
  email=email_value(data.get('email'));code=token()
  with db.tx() as c:
   target=row(c,select(users).where(users.c.email==email,users.c.active==True))
   if not target:raise PilotError(404,'ACCOUNT_NOT_FOUND','There is no active pilot account for this email.')
   c.execute(oauth.update().where(oauth.c.provider=='recovery',oauth.c.user_id==target['id']).values(used=True))
   c.execute(oauth.insert().values(hash=digest(code),provider='recovery',user_id=target['id'],binding='',expires=now()+1800,used=False))
   record(c,acting['id'],'account','Recovery link created','A private single-use recovery link was generated. No email was sent.')
  return {'url':settings.origin+'/recover?code='+code,'expires_in_minutes':30}
 @app.post('/api/auth/recover')
 def recover(request:Request,data:dict=Body(...)):
  auth.limit('recover:'+str(request.client.host),10)
  from .auth import password_value,ph
  hashed=ph.hash(password_value(data.get('password')))
  with db.tx() as c:
   flow=row(c,select(oauth).where(oauth.c.hash==digest(str(data.get('code','')))).with_for_update())
   if not flow or flow['provider']!='recovery' or flow['used'] or flow['expires']<=now():raise PilotError(400,'RECOVERY_EXPIRED','This recovery link expired or has already been used.')
   target=row(c,select(users).where(users.c.id==flow['user_id'],users.c.active==True))
   if not target:raise PilotError(400,'RECOVERY_EXPIRED','This recovery link is no longer available.')
   c.execute(oauth.update().where(oauth.c.hash==flow['hash']).values(used=True))
   c.execute(users.update().where(users.c.id==target['id']).values(password_hash=hashed))
   c.execute(sessions.update().where(sessions.c.user_id==target['id']).values(revoked=True))
   record(c,target['id'],'account','Account recovered','Password changed and all sessions revoked.')
  return {'ok':True}
 @app.post('/api/auth/device/start')
 def device_start(request:Request,data:dict=Body(...)):
  import re
  auth.limit('device:'+str(request.client.host),12)
  challenge=data.get('challenge','')
  if not isinstance(challenge,str) or not re.fullmatch(r'[A-Za-z0-9_-]{43}',challenge):raise PilotError(400,'DEVICE_CHALLENGE','Invalid device connection request.')
  code=token()
  with db.tx() as c:c.execute(oauth.insert().values(hash=digest(code),provider='device',binding=challenge,expires=now()+600,used=False))
  return {'code':code,'url':settings.mobile_origin+'/device?code='+code,'expires_in':600}
 @app.post('/api/auth/device/approve')
 def device_approve(request:Request,data:dict=Body(...)):
  user=identity(request)
  if data.get('confirm_device') is not True:raise PilotError(400,'DEVICE_CONSENT','Confirm that you initiated this device connection.')
  with db.tx() as c:
   flow=row(c,select(oauth).where(oauth.c.hash==digest(str(data.get('code','')))).with_for_update())
   if not flow or flow['provider']!='device' or flow['used'] or flow['expires']<=now():raise PilotError(400,'DEVICE_EXPIRED','This device request expired. Start again in the app.')
   if flow['user_id'] and flow['user_id']!=user['id']:raise PilotError(409,'DEVICE_APPROVED','This request already has an account.')
   c.execute(oauth.update().where(oauth.c.hash==flow['hash']).values(user_id=user['id']))
   record(c,user['id'],'session','Mobile device approved','The requesting app must prove possession of its private verification code.')
  return {'ok':True}
 @app.post('/api/auth/device/complete')
 def device_complete(request:Request,data:dict=Body(...)):
  import base64,hashlib
  auth.limit('device-complete:'+str(request.client.host),60)
  verifier=str(data.get('verifier',''))
  if not 43<=len(verifier)<=128:raise PilotError(400,'DEVICE_PROOF','Invalid device verification.')
  challenge=base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip('=')
  with db.tx() as c:
   flow=row(c,select(oauth).where(oauth.c.hash==digest(str(data.get('code','')))).with_for_update())
   if not flow or flow['provider']!='device' or flow['used'] or flow['expires']<=now() or not hmac.compare_digest(flow['binding'],challenge):raise PilotError(400,'DEVICE_EXPIRED','The device request expired or could not be verified.')
   if not flow['user_id']:return {'pending':True}
   user=row(c,select(users).where(users.c.id==flow['user_id']))
   if not user or not user['active']:raise PilotError(403,'ACCOUNT_DISABLED','This account is disabled.')
   c.execute(oauth.update().where(oauth.c.hash==flow['hash']).values(used=True))
  raw,csrf=auth.issue(user,'native','Paired mobile app')
  return {'user':public_user(user),'access_token':raw,'csrf':csrf}
 @app.get('/api/auth/google/start')
 def google_start(request:Request,invite:str=''):
  auth.limit('google:'+str(request.client.host),20)
  binding=token();url=auth.start_google(binding,invite)
  response=RedirectResponse(url,status_code=303);response.set_cookie(BINDING,binding,max_age=600,httponly=True,secure=settings.secure,samesite='lax');return response
 @app.get('/api/auth/google/callback')
 def google_callback(request:Request,state:str='',code:str='',error:str=''):
  try:
   if error:raise PilotError(401,'GOOGLE_CANCELLED','Google sign-in was cancelled.')
   user=auth.google_callback(state,request.cookies.get(BINDING,''),code);raw,_=auth.issue(user)
   response=RedirectResponse('/onboarding' if not user['onboarded'] else '/',status_code=303)
   response.set_cookie(COOKIE,raw,max_age=7*86400,httponly=True,secure=settings.secure,samesite='lax')
  except PilotError as e:response=RedirectResponse('/signin?error='+e.code,status_code=303)
  response.delete_cookie(BINDING);return response

 @app.post('/api/account/onboarding')
 def onboarding(request:Request,data:dict=Body(...)):
  user=identity(request)
  if data.get('policy_version')!=settings.policy_version or data.get('acknowledge_pilot') is not True:raise PilotError(400,'CONSENT','Review and accept the private pilot conditions.')
  name=str(data.get('name',user['name'])).strip()
  if not name or len(name)>80:raise PilotError(400,'NAME','Enter your name, up to 80 characters.')
  frames=data.get('timeframes',['1H','4H','1D','1W'])
  if not isinstance(frames,list) or not frames or any(tf not in ('1H','4H','1D','1W') for tf in frames):raise PilotError(400,'TIMEFRAMES','Choose at least one supported timeframe.')
  preferences=dict(user.get('preferences') or {},timeframes=list(dict.fromkeys(frames)),mode='research',live_consent=False)
  with db.tx() as c:
   c.execute(users.update().where(users.c.id==user['id']).values(name=name,onboarded=True,preferences=preferences,policy_version=settings.policy_version))
   record(c,user['id'],'account','Pilot setup complete','Research and simulated workflow selected. Live execution is disabled.')
  return {'ok':True}
 @app.get('/api/account')
 def account(request:Request):
  user=identity(request)
  with db.tx() as c:device_count=len(c.execute(select(sessions.c.hash).where(sessions.c.user_id==user['id'],sessions.c.revoked==False,sessions.c.expires>now())).all())
  return {'user':public_user(user),'billing':billing.status(user),'broker':kite.status(user),'devices':device_count,'live_enabled':False}
 @app.get('/api/admin/readiness')
 def readiness(request:Request):
  owner(request)
  return {'google':settings.google_ready,'razorpay':settings.billing_ready,'kite':settings.kite_ready,'origin':settings.origin,'live_enabled':False,
   'callbacks':{'google':settings.origin+'/api/auth/google/callback','razorpay':settings.origin+'/api/billing/webhook','kite':settings.origin+'/api/broker/kite/callback','kite_postback':settings.origin+'/api/broker/kite/postback'},
   'required':{'google':['Google web OAuth client ID','Google client secret'],'razorpay':['Test key ID','Test key secret','Webhook secret','Test subscription plan ID']}}
 @app.post('/api/admin/invites')
 def invite(request:Request,data:dict=Body(...)):
  user=owner(request);raw=auth.invite(data.get('email'),'member')
  with db.tx() as c:record(c,user['id'],'account','Pilot invitation created','A single-use invitation was generated. No email was sent.')
  return {'url':settings.origin+'/signup?invite='+raw,'expires_in_hours':72}
 @app.post('/api/admin/access')
 def pilot_access(request:Request,data:dict=Body(...)):
  acting=owner(request)
  from .auth import email_value
  email=email_value(data.get('email'))
  with db.tx() as c:
   target=row(c,select(users).where(users.c.email==email).with_for_update())
   if not target:raise PilotError(404,'ACCOUNT_NOT_FOUND','Ask the invited person to create their account first.')
   prefs=dict(target['preferences'] or {},pilot_access_until=now()+14*86400)
   c.execute(users.update().where(users.c.id==target['id']).values(preferences=prefs))
   record(c,target['id'],'billing','Included pilot access granted','The owner granted 14 days of access. No payment was taken.')
   record(c,acting['id'],'account','Pilot access granted','14 days of included access granted to an invited account.',target['id'])
  return {'ok':True}

 @app.get('/api/billing/status')
 def billing_status(request:Request):return billing.status(identity(request))
 @app.post('/api/billing/checkout')
 def checkout(request:Request,data:dict=Body(default={})):
  user=identity(request)
  if data.get('acknowledge_test') is not True:raise PilotError(400,'BILLING_CONSENT','Confirm that this is a test subscription.')
  return billing.create(user)
 @app.post('/api/billing/refresh')
 def billing_refresh(request:Request):return billing.synchronize(identity(request))
 @app.post('/api/billing/cancel')
 def billing_cancel(request:Request):return billing.cancel(identity(request))
 @app.post('/api/billing/webhook')
 async def billing_webhook(request:Request):return billing.webhook(await request.body(),request.headers.get('x-razorpay-signature',''))

 @app.get('/api/broker/status')
 def broker_status(request:Request):return kite.status(identity(request))
 @app.post('/api/broker/kite/start')
 def kite_start(request:Request):
  user=member(request);binding=token();url=kite.start(user,binding)
  response=JSONResponse({'url':url});response.set_cookie(BINDING,binding,max_age=600,httponly=True,secure=settings.secure,samesite='lax');return response
 @app.get('/api/broker/kite/callback')
 def kite_callback(request:Request,state:str='',request_token:str='',status:str=''):
  try:
   if status and status!='success':raise PilotError(400,'KITE_CANCELLED','Kite connection was cancelled.')
   kite.callback(state,request.cookies.get(BINDING,''),request_token);url='/account?broker=connected'
  except PilotError as e:url='/account?error='+e.code
  response=RedirectResponse(url,status_code=303);response.delete_cookie(BINDING);return response
 @app.post('/api/broker/disconnect')
 def broker_disconnect(request:Request):return kite.disconnect(identity(request))
 @app.post('/api/broker/reconcile')
 def broker_reconcile(request:Request):return kite.reconcile(member(request))
 @app.post('/api/broker/kite/postback')
 async def postback(request:Request):
  value=await request.json();kite.verify_postback(value)
  # Updates are signals to reconcile; never let a callback invent an owned order.
  with db.tx() as c:
   item=row(c,select(orders).where(orders.c.broker_id==str(value.get('order_id'))))
   if item:record(c,item['user_id'],'broker','Broker update received','Order reconciliation requested.',item['id'])
  if item:
   with db.tx() as c:user=row(c,select(users).where(users.c.id==item['user_id']))
   live_service.reconcile(user)
  return {'ok':True}

 @app.get('/api/product')
 def product_state(request:Request):return product.state(member(request))
 @app.get('/api/product/exit-plan')
 def exit_plan(request:Request,match_id:str,side:str,run:str|None=None,snapshot:str|None=None):
  member(request);return evidence.exit_plan(match_id,side,run,snapshot)
 @app.post('/api/product/plans')
 def save_plan(request:Request,data:dict=Body(...)):return product.save(member(request),data)
 @app.post('/api/product/watch')
 def watch(request:Request,data:dict=Body(...)):return product.watch(member(request),data)
 @app.post('/api/product/plan-action')
 def plan_action(request:Request,data:dict=Body(...)):return product.change(member(request),data)
 @app.get('/api/trading')
 def trading(request:Request):return simulation.state(member(request))
 @app.post('/api/trading/simulate')
 def simulate(request:Request,data:dict=Body(...)):return simulation.submit(member(request),data)
 @app.post('/api/trading/advance')
 def advance(request:Request,data:dict=Body(...)):return simulation.advance(data.get('id'),member(request)['id'],True)
 @app.post('/api/trading/cancel')
 def cancel(request:Request,data:dict=Body(...)):return simulation.cancel(member(request),data.get('id'))
 @app.post('/api/trading/exit')
 def exit_position(request:Request,data:dict=Body(...)):return simulation.exit(member(request),data.get('id'))
 @app.post('/api/trading/pause')
 def pause(request:Request,data:dict=Body(...)):return simulation.pause(member(request),data.get('paused',True))
 @app.post('/api/trading/live')
 def live(request:Request,data:dict=Body(...)):
  return live_service.submit(member(request),data)
 @app.post('/api/trading/live/reconcile')
 def live_reconcile(request:Request):return live_service.reconcile(member(request))
 @app.post('/api/trading/live/cancel')
 def live_cancel(request:Request,data:dict=Body(...)):return live_service.cancel(member(request),data.get('id'))

 @app.get('/api/lab/{action}')
 def lab_read(request:Request,action:str):
  user=member(request)
  if action not in ('capabilities','jobs','job','chart','strategies','portfolios'):raise PilotError(404,'NOT_FOUND','Research endpoint not found.')
  return evidence.study('/api/lab/'+action,{**dict(request.query_params),'owner':user['id']})
 @app.post('/api/lab/{action}')
 def lab_write(request:Request,action:str,data:dict=Body(...)):
  user=member(request)
  if action=='kite-holdings':
   holdings=kite.call(user,'GET','portfolio/holdings')
   combined={}
   for h in holdings:
    quantity=int(h.get('quantity',0))+int(h.get('t1_quantity',0));symbol=h.get('tradingsymbol','')
    if quantity<=0 or not symbol:continue
    prior=combined.get(symbol,{'quantity':0,'cost':0});prior['quantity']+=quantity;prior['cost']+=quantity*float(h.get('average_price',0));combined[symbol]=prior
   values=[dict(symbol=k,quantity=v['quantity'],average_price=v['cost']/v['quantity']) for k,v in combined.items()]
   return evidence.study('/api/lab/analyse',body={'owner':user['id'],'holdings':values,'source':'Kite holdings; valued at stored historical prices'})
  if action not in ('interpret','research','cancel','save','analyse','prepare','portfolio-action'):raise PilotError(404,'NOT_FOUND','Research endpoint not found.')
  return evidence.study('/api/lab/'+action,body={**data,'owner':user['id']})

 @app.get('/api/studies')
 def study_list(request:Request):return evidence.study('/api/studies',{'owner':member(request)['id']})
 @app.post('/api/studies')
 def study_start(request:Request,data:dict=Body(...)):return evidence.study('/api/studies',body={'owner':member(request)['id'],'settings':data})
 @app.get('/api/studies/{study_id}')
 def study_result(request:Request,study_id:str):return evidence.study('/api/studies/job',{'owner':member(request)['id'],'id':study_id,'result':'true'})
 @app.post('/api/studies/{study_id}/cancel')
 def study_cancel(request:Request,study_id:str,data:dict=Body(...)):return evidence.study('/api/studies/cancel',body={'owner':member(request)['id'],'id':study_id})
 @app.get('/api/studies/{study_id}/chart')
 def study_chart(request:Request,study_id:str):return evidence.study('/api/studies/chart',{**dict(request.query_params),'owner':member(request)['id'],'id':study_id})
 @app.get('/api/replay')
 def historical_replay(request:Request):
  member(request);return evidence.study('/api/replay',dict(request.query_params))
 @app.get('/api/replay/chart')
 def historical_replay_chart(request:Request):
  member(request);return evidence.study('/api/replay/chart',dict(request.query_params))

 # Strategy registry, Discover catalog and results (docs/FALCON_DISCOVER_SPEC.md §5, §6). Declared before the research catch-all.
 @app.get('/api/strategies/catalog')
 def strategy_catalog(request:Request,universe:str='nifty500'):
  member(request);return registry.catalog(universe)
 @app.get('/api/strategies/research')
 def strategy_research_state(request:Request):
  member(request);state=registry.research_state()
  if state is None:raise PilotError(503,'RESEARCH_UNAVAILABLE','Pattern research is not available on this server.')
  return state
 @app.get('/api/strategies/{key}/results')
 def strategy_results(request:Request,key:str,universe:str='nifty500',limit:int=0,offset:int=0,q:str=''):
  member(request)
  if not 0<=limit<=2000 or not 0<=offset<=1000000:raise PilotError(400,'FIELD_INVALID','limit must be 0-2000 and offset 0-1,000,000.')
  return registry.results(key,universe,limit=limit or None,offset=offset,search=(q.strip()[:40] or None))
 @app.get('/api/strategies/{key}/detections')
 def strategy_detections(request:Request,key:str,scope:str='live',limit:int=0,offset:int=0):
  """Live detections of one researched strategy, from the scanner's detection ledger.

  Always bounded: `limit` is capped server-side and `total` is the real count before the cut, so the page
  can never issue an unbounded fetch and can never under-report what exists.
  """
  member(request)
  if scope not in DETECTION_SCOPES:raise PilotError(400,'FIELD_INVALID','scope must be one of: '+', '.join(DETECTION_SCOPES)+'.')
  if not 0<=limit<=DETECTION_LIMIT_MAX or not 0<=offset<=1000000:
   raise PilotError(400,'FIELD_INVALID',f'limit must be 0-{DETECTION_LIMIT_MAX} and offset 0-1,000,000.')
  return registry.detections(key,scope=scope,limit=limit or None,offset=offset)
 def history_service(request):
  member(request)
  if not app.state.pattern_history:raise PilotError(503,'RESEARCH_UNAVAILABLE','Pattern history is unavailable on this server.')
  return app.state.pattern_history

 def history_selection(symbol,pattern_id,variant,state,strategy_key,detection_id):
  # A live selection cannot attach another variant, state or detector's frozen history.
  if not strategy_key and detection_id:raise PilotError(400,'FIELD_INVALID','A live detection needs its strategy key.')
  if strategy_key:
   item,_=registry.research_strategy(strategy_key);cfg=item['source_config'] or {}
   if (cfg.get('pattern_id'),cfg.get('variant'),cfg.get('side'),cfg.get('timeframe'),cfg.get('research_run'))!=(pattern_id,variant,'long','1D',settings.pattern_research_run):
    raise PilotError(409,'HISTORY_IDENTITY_MISMATCH','This selection does not match the daily pattern history pilot.')
  if detection_id:
   if len(detection_id)>64 or not detection_id.isalnum():raise PilotError(400,'FIELD_INVALID','Invalid detection identifier.')
   card=registry.card(strategy_key,symbol,detection_id)
   if (card.get('live_evidence') or {}).get('status')!='identity_match':
    raise PilotError(409,'HISTORY_IDENTITY_MISMATCH',(card.get('live_evidence') or {}).get('note') or 'No compatible historical evidence for this detection.')
   detection=card.get('detection') or {};detected_state=detection.get('state')
   expected='setup' if detected_state in ('setup','forming') else 'confirmed' if detected_state=='confirmed' else None
   if state!=expected:raise PilotError(409,'HISTORY_STATE_MISMATCH','Choose history for the same forming or confirmed state as this detection.')

 @app.get('/api/pattern-history/catalogue')
 def pattern_history_catalogue(request:Request):return history_service(request).catalogue()

 @app.get('/api/pattern-drawings/audit')
 def pattern_drawing_audit(request:Request):
  member(request)
  path=Path(settings.pattern_catalogue_path).parent/'drawing_audit_examples.json'
  if not path.is_file():raise PilotError(503,'DRAWING_AUDIT_UNAVAILABLE','The drawing review examples have not been prepared.')
  return FileResponse(path,media_type='application/json',headers={'Cache-Control':'no-store'})

 @app.get('/api/pattern-drawings/review')
 def pattern_drawing_review(request:Request):
  member(request)
  path=Path(settings.pattern_catalogue_path).parent/'drawing_visual_review.json'
  if not path.is_file():raise PilotError(503,'DRAWING_REVIEW_UNAVAILABLE','The visual review record is not available.')
  return FileResponse(path,media_type='application/json',headers={'Cache-Control':'no-store'})

 @app.get('/api/pattern-history/stocks')
 def pattern_history_stocks(request:Request,pattern_id:str='CH16',variant:str='canonical',state:str='confirmed',q:str='',limit:int=100,offset:int=0):
  return history_service(request).stocks(pattern_id,variant,state,search=q,limit=limit,offset=offset)

 @app.get('/api/pattern-history/history')
 def pattern_history_evidence(request:Request,symbol:str,pattern_id:str='CH16',variant:str='canonical',state:str='confirmed',strategy_key:str='',detection_id:str='',horizon:int=5):
  service=history_service(request);history_selection(symbol,pattern_id,variant,state,strategy_key,detection_id)
  return service.history(symbol,pattern_id,variant,state,horizon=horizon)

 @app.get('/api/pattern-history/replay')
 def pattern_history_replay(request:Request,symbol:str,occurrence_id:str,pattern_id:str='CH16',variant:str='canonical',state:str='confirmed',strategy_key:str='',detection_id:str=''):
  service=history_service(request);history_selection(symbol,pattern_id,variant,state,strategy_key,detection_id)
  return service.replay(symbol,pattern_id,variant,state,occurrence_id)

 @app.get('/api/strategies/{key}/card')
 def strategy_card(request:Request,key:str,symbol:str='',detection_id:str=''):
  """The trader evidence card: served from precomputed research rows, never computed on the click.

  `detection_id` opens the card from a LIVE detection; the evidence is then served only when that
  detection's own identity matches the research run (pattern id, variant, side, timeframe, spec hash).
  """
  member(request)
  if detection_id and (len(detection_id)>64 or not detection_id.isalnum()):
   raise PilotError(400,'FIELD_INVALID','detection_id must be up to 64 letters or digits.')
  return registry.card(key,symbol,detection_id or None)
 @app.post('/api/admin/strategies/refresh-index')
 def admin_refresh_index(request:Request):
  owner(request)
  if not registry.index:raise PilotError(503,'RESEARCH_UNAVAILABLE','Pattern research is not available on this server.')
  started=registry.index.ensure()
  return dict(started=bool(started),state=registry.index.state())
 @app.get('/api/admin/strategies')
 def admin_strategies(request:Request):
  owner(request);return registry.admin()
 @app.post('/api/admin/strategies')
 def admin_strategy_create(request:Request,data:dict=Body(...)):return registry.create(owner(request),data)
 @app.patch('/api/admin/strategies/{key}')
 def admin_strategy_update(request:Request,key:str,data:dict=Body(...)):return registry.update(owner(request),key,data)
 @app.post('/api/admin/strategies/{key}/preview')
 def admin_strategy_preview(request:Request,key:str,data:dict=Body(default={})):
  owner(request);return registry.preview(key,str(data.get('universe') or request.query_params.get('universe') or 'nifty500'))
 @app.get('/api/admin/strategy-sources')
 def admin_strategy_sources(request:Request):
  """What the add-strategy form may offer, from the registry itself (never from the scanner's current detector
  set). Both sources are listed whatever the scanner is running, because POST /api/admin/strategies accepts both."""
  owner(request);return registry.admin_sources()
 @app.get('/api/admin/blocks')
 def admin_blocks(request:Request):
  owner(request);return registry.admin_blocks()
 @app.post('/api/admin/blocks')
 def admin_block_create(request:Request,data:dict=Body(...)):return registry.create_block(owner(request),data)
 @app.patch('/api/admin/blocks/{key}')
 def admin_block_update(request:Request,key:str,data:dict=Body(...)):return registry.update_block(owner(request),key,data)

 # Derivative tab (docs/DERIVATIVES_SPEC.md §4). Declared before the research catch-all. Every one of these is a
 # READ of db/derivatives.db: display-only, nothing here feeds a trading gate (§5).
 def _derivative_query(underlying='',expiry='',option_type='',max_dte=None,min_premium_cr=None,limit=0,points=0):
  name=clean_symbol(underlying)
  if underlying and not name:raise PilotError(400,'FIELD_INVALID','underlying must be a traded symbol.')
  date=clean_expiry(expiry)
  if expiry and not date:raise PilotError(400,'FIELD_INVALID','expiry must be a date as YYYY-MM-DD.')
  kind=(option_type or '').strip().upper()
  if kind and kind not in DERIVATIVE_TYPES:raise PilotError(400,'FIELD_INVALID','option_type must be CE or PE.')
  if max_dte is not None and not 0<=max_dte<=400:raise PilotError(400,'FIELD_INVALID','max_dte must be 0-400 days.')
  if min_premium_cr is not None and not 0<=min_premium_cr<=100000:raise PilotError(400,'FIELD_INVALID','min_premium_cr must be 0-100000.')
  if not 0<=limit<=DERIVATIVE_LIMIT_MAX:raise PilotError(400,'FIELD_INVALID',f'limit must be 0-{DERIVATIVE_LIMIT_MAX}.')
  if not 0<=points<=DERIVATIVE_POINTS_MAX:raise PilotError(400,'FIELD_INVALID',f'points must be 0-{DERIVATIVE_POINTS_MAX}.')
  return name,date,kind
 @app.get('/api/derivatives/status')
 def derivative_status(request:Request):
  member(request);return derivatives.status()
 @app.get('/api/derivatives/capture')
 def derivative_capture(request:Request,at:str=''):
  """DERIVATIVE CAPTURE HEALTH, on its own route and separate from the app's cash-feed status.

  What was MEASURED at a 15-min reading, as one of missing_capture / partial_capture / complete /
  no_eligible_rows / filtered_out / failed, with the required-field coverage behind it and the newest
  attempted, available and complete readings. This exists because the global status chip describes prices
  and patterns - the cash feed - and went on saying "healthy" straight through an F&O capture outage that
  had left every contract row of the newest reading with no premium and no spot. Read-only (section 5).
  """
  member(request)
  try:return derivatives.capture_health(at[:32])
  except Exception as error:  # noqa: BLE001 - a store that cannot be read is a STATE, not a 500
   logging.getLogger('pilot').warning('derivatives: capture health failed (%s)',error)
   return {**derivatives.envelope(),'state':'failed','healthy':False,
    'state_text':'The F&O store could not be read for this request. No rows are shown, which is not the '
     'same as no rows existing.'}
 @app.get('/api/derivatives/filters')
 def derivative_filters(request:Request):
  member(request);return derivatives.filters()
 @app.get('/api/derivatives/unusual')
 def derivative_unusual(request:Request,underlying:str='',expiry:str='',watchlist:str='',option_type:str='',max_dte:int=-1,min_premium_cr:float=0.0,limit:int=0):
  """The §3.2-§3.4 screen, per underlying, expandable to strikes. The §3 floors always apply."""
  member(request)
  dte=None if max_dte<0 else max_dte
  name,date,kind=_derivative_query(underlying,expiry,option_type,dte,min_premium_cr,limit)
  # A premium floor the READER raised cannot be honoured at a 15-min reading where no traded average price was
  # captured: there is no premium in it to measure against. That is a refusal the caller sees, exactly as the
  # screener's own refused filters are - dropping it quietly would serve a wider list than was asked for.
  try:
   return derivatives.unusual(name,date,watchlist=watchlist[:20],max_dte=dte,option_type=kind,
    min_premium_cr=min_premium_cr or None,limit=limit or None)
  except ValueError as error:raise PilotError(400,'FILTER_REFUSED',str(error))
 # `at` is the 15-minute reading the TAB is on - the one its screener resolved. Omitted, each of these reads
 # the newest reading the underlying has, exactly as it did before. It is passed because the newest reading of
 # a session rebuilt from candles carries no spot, and a chain with no spot has nothing to sit around: that is
 # how the chain came to open at 21,350 against a spot of 23,302.
 @app.get('/api/derivatives/chain')
 def derivative_chain(request:Request,underlying:str='',expiry:str='',option_type:str='',at:str=''):
  member(request)
  name,date,kind=_derivative_query(underlying,expiry,option_type)
  if not name:raise PilotError(400,'FIELD_INVALID','underlying is required for the option chain.')
  return derivatives.chain(name,date,kind,at=at[:32])
 @app.get('/api/derivatives/oi-by-strike')
 def derivative_oi_by_strike(request:Request,underlying:str='',expiry:str='',at:str=''):
  member(request)
  name,date,_=_derivative_query(underlying,expiry)
  if not name:raise PilotError(400,'FIELD_INVALID','underlying is required for OI by strike.')
  return derivatives.oi_by_strike(name,date,at=at[:32])
 @app.get('/api/derivatives/oi-grid')
 def derivative_oi_grid(request:Request,underlying:str='',expiry:str='',at:str=''):
  """The owner's ΔOI block: ten small series — ATM CE and the four strikes above, ATM PE and the four below.

  ΔOI is open interest added or removed since the previous close, at each 15-minute mark of the session. Read
  only; nothing here is a forecast and nothing here feeds a trading gate (§5).
  """
  member(request)
  name,date,_=_derivative_query(underlying,expiry)
  if not name:raise PilotError(400,'FIELD_INVALID','underlying is required for the ΔOI strike grid.')
  return derivatives.oi_grid(name,date,at=at[:32])
 @app.get('/api/derivatives/events')
 def derivative_events(request:Request,at:str='',limit:int=0):
  """What is happening across the book at one 15-min reading — one row per instrument.

  Computed ONCE per reading and served to every reader, rather than a grid request per instrument per
  browser. It adds no analytic: the walk is the tab's own (rule signal/2), and every line it returns is an
  observation of what the numbers did between two named readings. Nothing here is a forecast (§5).
  """
  member(request);_derivative_query(limit=limit)
  body=derivatives.events(at=at[:32],limit=limit or None)
  # ONE ENGINE FOR THE LIST AND THE PANE: once every instrument's immutable snapshot for this reading exists, the
  # list is built from them. Until then (seconds after the metrics land) the pass above stands.
  try:
   reading=str(body.get('as_of') or '')
   expected=int(body.get('instruments') or len(body.get('rows') or []))
   if reading and expected:
    rows=events_from_snapshots(snapshot_store,reading[:10],reading,snapshot_worker.version()[0],expected)
    if rows is not None:
     body=dict(body);body['rows']=rows[:limit] if limit else rows
     body['measured']=sum(1 for r in rows if r.get('live'));body['source']='snapshots'
  except Exception as error:  # noqa: BLE001 - the list must still answer from its own pass
   logging.getLogger('pilot').warning('derivatives: events from snapshots failed (%s)',error)
  return body
 @app.get('/api/derivatives/snapshots')
 def derivative_snapshots(request:Request,underlying:str='',session:str='',upto:str=''):
  """THE SESSION AS KANIDA SAW IT: one immutable snapshot per reading for one underlying, oldest first. Each was
  written once, from the grid anchored at its own reading, by the engine version it names; none is ever
  re-derived from a later reading's contracts. Readings that could not be anchored say so."""
  member(request)
  name=underlying.strip().upper()[:40]
  day=(session or snapshot_worker.newest_session() or '')[:10]
  engine_version=snapshot_worker.version()[0]
  rows=snapshot_store.session_rows(name,day,engine_version,upto[:32] or None) if name and day else []
  return {'underlying':name,'session':day,'engine_version':engine_version,'snapshots':rows_for_api(rows)}
 @app.get('/api/derivatives/signal-noise')
 def derivative_signal_noise(request:Request,session:str=''):
  """THE SIGNAL-TO-NOISE AUDIT for a session: what KANIDA said, what held, what was noise and why. Internal."""
  member(request)
  day=(session or snapshot_worker.newest_session() or '')[:10]
  report=signal_noise.report(day,snapshot_worker.version()[0]) if day else {}
  return {**report,'markdown':sn_markdown(report) if report else ''}
 @app.get('/api/derivatives/indices')
 def derivative_indices(request:Request,points:int=0):
  member(request);_derivative_query(points=points)
  return derivatives.indices(points=points or None)
 @app.get('/api/derivatives/futures')
 def derivative_futures(request:Request,underlying:str='',watchlist:str='',limit:int=0):
  member(request)
  name,_date,_kind=_derivative_query(underlying,limit=limit)
  return derivatives.futures(name,watchlist=watchlist[:20],limit=limit or None)
 @app.get('/api/derivatives/futures-chart')
 def derivative_futures_chart(request:Request,underlying:str='',interval:str=DERIVATIVE_INTERVAL_DEFAULT):
  """The Derivative tab's price chart: the FRONT futures contract's own candles.

  `interval` is 15-minute by default, daily as the alternative; anything else is a 400. The response states how
  many sessions it is actually returning and which contract they belong to, so a short series after the front
  contract rolls reads as a short series rather than a broken chart. Display only - nothing here is a forecast
  and nothing here feeds a trading gate (§5).
  """
  member(request)
  name,_date,_kind=_derivative_query(underlying)
  if not name:raise PilotError(400,'FIELD_INVALID','underlying is required for the futures chart.')
  span=(interval or '').strip().lower()
  if span not in DERIVATIVE_INTERVALS:
   raise PilotError(400,'FIELD_INVALID',f"interval must be one of {', '.join(DERIVATIVE_INTERVALS)}.")
  body=derivatives.futures_chart(name,span)
  # A store that is readable but lists no futures contract for this underlying is a bad request, not an empty
  # card: there is no contract to name, so there is nothing honest to draw. A store that is not there at all
  # still answers 200 with `available: False`, exactly like every other card on the tab.
  if body.get('unknown_underlying'):
   raise PilotError(400,'FIELD_INVALID',f'{name} has no futures contract in the F&O store.')
  return body
 @app.get('/api/derivatives/series')
 def derivative_series(request:Request,underlying:str='',instrument_token:int=0,points:int=0):
  """The linked panel's own series: a contract's 15-minute price + OI, or the underlying's spot + total OI."""
  member(request)
  name,_date,_kind=_derivative_query(underlying,points=points)
  if instrument_token and not 0<instrument_token<10**12:raise PilotError(400,'FIELD_INVALID','instrument_token is not a valid token.')
  return derivatives.series(name,instrument_token or None,points=points or None)

 # --- the session series: PCR, max pain, implied volatility, the screener, futures build-up ------------------
 # Each of these reads PRECOMPUTED rows off the metrics table's own primary key, so a whole session of one
 # series is one index seek. Display only: nothing here forecasts, nothing here feeds a trading gate (§5).
 @app.get('/api/derivatives/pcr-series')
 def derivative_pcr_series(request:Request,underlying:str='',expiry:str=''):
  """§3.5 through the session: OI PCR and volume PCR at every 15-min reading, oldest first, plus a direction.

  A chain too thin to carry a meaningful ratio has that reading WITHHELD with a named reason rather than
  printed - the floors and the reasons travel with the response.
  """
  member(request)
  name,date,_kind=_derivative_query(underlying,expiry)
  if not name:raise PilotError(400,'FIELD_INVALID','underlying is required for the PCR series.')
  return derivatives.pcr_series(name,date)
 @app.get('/api/derivatives/maxpain-series')
 def derivative_maxpain_series(request:Request,underlying:str='',expiry:str=''):
  """§3.6 through the session: the max-pain strike, the spot beside it, the distance and the OI it rests on."""
  member(request)
  name,date,_kind=_derivative_query(underlying,expiry)
  if not name:raise PilotError(400,'FIELD_INVALID','underlying is required for the max pain series.')
  return derivatives.maxpain_series(name,date)
 @app.get('/api/derivatives/iv-series')
 def derivative_iv_series(request:Request,underlying:str='',expiry:str='',strike:float=0.0,option_type:str=''):
  """Implied volatility through the session. THE ONE COMPUTED NUMBER ON THIS TAB.

  Kite does not supply implied volatility, so this is solved from each option's own last traded price with a
  Black-Scholes model. The response says so in `computed`, `model`, `risk_free_rate`, `risk_free_rate_source`
  and `computed_text`, and every reading the maths cannot be trusted on is null WITH the reason. Nothing
  computed here is written to the store.
  """
  member(request)
  name,date,kind=_derivative_query(underlying,expiry,option_type)
  if not name:raise PilotError(400,'FIELD_INVALID','underlying is required for the implied volatility series.')
  if strike and not 0<strike<10**9:raise PilotError(400,'FIELD_INVALID','strike is not a listed strike.')
  if bool(strike)!=bool(kind):
   raise PilotError(400,'FIELD_INVALID','strike and option_type go together: give both for one strike, or '
    'neither for the at-the-money reading.')
  return derivatives.iv_series(name,date,strike or None,kind)
 @app.get('/api/derivatives/futures-buildup')
 def derivative_futures_buildup(request:Request,underlying:str=''):
  """§3.7 through the session: the front futures contract's OI against its own average, and its basis."""
  member(request)
  name,_date,_kind=_derivative_query(underlying)
  if not name:raise PilotError(400,'FIELD_INVALID','underlying is required for the futures build-up series.')
  return derivatives.futures_buildup(name)
 @app.get('/api/derivatives/screener')
 def derivative_screener(request:Request):
  """§3.2-§3.4 as a real screen, with every filter optional and combinable.

  The query string is read WHOLE rather than through named parameters, so a filter this screener does not
  offer is a 400 that names it - never a parameter FastAPI drops on the floor while the panel goes on showing
  it as active. `applied` is built from what the query actually did; `available` from the columns this store
  actually carries.
  """
  member(request)
  asked={k:v for k,v in request.query_params.items() if v not in (None,'')}
  limit=asked.pop('limit',None)
  if limit is not None:
   try:limit=int(limit)
   except (TypeError,ValueError):raise PilotError(400,'FIELD_INVALID','limit must be a whole number.')
   if not 1<=limit<=DERIVATIVE_SCREENER_LIMIT_MAX:
    raise PilotError(400,'FIELD_INVALID',f'limit must be 1-{DERIVATIVE_SCREENER_LIMIT_MAX}.')
  try:return derivatives.screener(asked,limit=limit)
  except ValueError as error:raise PilotError(400,'FILTER_REFUSED',str(error))

 @app.get('/api/matches')
 def matches(request:Request):
  """Matches for the DISPLAY surfaces (chart workspace, legend, watchlist).

  On the legacy pattern set this is exactly the passthrough it has always been. On the research set the
  scanner's paginated object is unwrapped into the live book's best-fitting slice, with every row's `history`
  still empty - so the workspace draws today's detections and no evidence or trade gate can read a number
  off them. The trade path resolves against `Evidence.matches()`, which stays legacy-only.
  """
  member(request);return evidence.display_matches(dict(request.query_params))
 @app.get('/api/{path:path}')
 def research(request:Request,path:str):
  member(request);full='/api/'+path
  if full not in PATHS:raise PilotError(404,'NOT_FOUND','Endpoint not found.')
  # Every route reachable from HERE is a read the app renders, so it opts into the last-good cache: while the
  # scanner restarts the app keeps drawing the last scan, stamped with when it was taken. Nothing that decides
  # a trade comes through this route - the plan, exit-plan and submission paths call Evidence directly, without
  # `cache`, and still fail closed. `/api/backtests/capital` is in PATHS but not CACHEABLE (sizing is never replayed).
  return evidence.get(full,dict(request.query_params),cache=True)
 @app.get('/{path:path}')
 def web(path:str):
  root=Path(settings.web_directory).resolve();file=(root/path).resolve()
  if not file.is_relative_to(root):raise PilotError(404,'NOT_FOUND','Page not found.')
  if not file.is_file():file=root/'index.html'
  if not file.is_file():raise PilotError(503,'WEB_BUILD','The pilot web build is not available yet.')
  return FileResponse(file)
 return app
