"""DEV ONLY — the Options Screener on its own port (default 8092), isolated from the running pilot.

    scripts/start-screener-dev.ps1          (sets PYTHONPATH and runs this)

What it shares with the pilot on :8082, and what it does not:
  * db/derivatives.db   READ-ONLY, the same store the Derivative tab reads.
  * everything writable is under var/dev-screener/: its own pilot database (throwaway dev accounts, never the real
    users), its own screener store, its own last-good cache, research index and intelligence file. The snapshot
    worker is OFF, so var/intelligence.db is never opened for writing.
  * its own web build: dist-screener/.
  * THE COOKIE. Browsers scope cookies by host, not port, so a dev sign-in on 127.0.0.1:8092 would overwrite the
    `kanida_session` cookie the pilot on 127.0.0.1:8082 uses. This server renames it to `kanida_dev_session` on
    the way in and out, and ignores the pilot's own cookie entirely.
  * `/__dev/session?as=owner|member` signs the browser in as one of two throwaway dev accounts WITHOUT a password,
    so the screen can be tested screen by screen. That route exists only in this file — `screener.mount`, which
    is what the real pilot gets, has no such route.
"""
from __future__ import annotations
import os,sys,time
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
DEV=ROOT/'var'/'dev-screener'
DEV.mkdir(parents=True,exist_ok=True)
PORT=int(os.getenv('SCREENER_DEV_PORT','8092'))
DEFAULTS={
 'PILOT_PORT':str(PORT),'PILOT_ORIGIN':f'http://127.0.0.1:{PORT}','PILOT_ORIGINS':f'http://localhost:{PORT}',
 'PILOT_ENVIRONMENT':'local','PILOT_SNAPSHOTS':'off',
 'PILOT_DATABASE_URL':'sqlite:///'+str(DEV/'pilot.sqlite3'),
 'PILOT_INTELLIGENCE_DATABASE':str(DEV/'intelligence.db'),
 'PILOT_LAST_GOOD_PATH':str(DEV/'last_good.sqlite3'),
 'PILOT_PATTERN_INDEX_PATH':str(DEV/'research_index.sqlite3'),
 'PILOT_PATTERN_HISTORY_PATH':str(DEV/'pattern_history.sqlite3'),
 'PILOT_PATTERN_RESEARCH_DIRECTORY':str(DEV/'no-research'),
 'PILOT_PATTERN_DETECTION_DB':str(DEV/'no-scan-cache.sqlite3'),
 # SCREENER_DEV_WEB serves a different build from the same dev backend (e.g. a preview of an after-close change)
 'PILOT_WEB_DIRECTORY':os.getenv('SCREENER_DEV_WEB') or str(ROOT/'dist-screener'),
 'PILOT_SCREENER_DATABASE':str(DEV/'screener.db'),
 'PILOT_WORKSPACE_DATABASE':str(DEV/'workspace.db'),
 # the workspace's summary / key strikes read the REAL snapshots, opened mode=ro (never the pilot's writer)
 'PILOT_WORKSPACE_INTELLIGENCE':str(ROOT/'var'/'intelligence.db'),
}
for k,v in DEFAULTS.items():os.environ[k]=v   # forced: nothing from .env.pilot may point this server at the pilot's files

sys.path.insert(0,str(ROOT/'server'))
import uvicorn  # noqa: E402
from fastapi import Request  # noqa: E402
from fastapi.responses import RedirectResponse  # noqa: E402
from sqlalchemy import select  # noqa: E402
from kanida_pilot.app import create_app  # noqa: E402
from kanida_pilot.config import Settings  # noqa: E402
from kanida_pilot.db import users  # noqa: E402
from kanida_pilot.screener import mount  # noqa: E402
from kanida_pilot.workspace import mount as mount_workspace  # noqa: E402

REAL,DEVNAME=b'kanida_session',b'kanida_dev_session'
ACCOUNTS={'owner':('dev-owner@kanida.invalid','Dev owner','owner'),'member':('dev-member@kanida.invalid','Dev member','member')}


class CookieIsolation:
 """Present `kanida_dev_session` to the app as `kanida_session`, drop the pilot's real cookie, and rename the
 cookie the app sets on the way out."""
 def __init__(self,app):self.app=app
 async def __call__(self,scope,receive,send):
  if scope['type']!='http':return await self.app(scope,receive,send)
  headers=[]
  for k,v in scope.get('headers',[]):
   if k==b'cookie':
    parts=[p.strip() for p in v.split(b';') if p.strip()]
    parts=[p for p in parts if not p.startswith(REAL+b'=')]
    parts=[REAL+p[len(DEVNAME):] if p.startswith(DEVNAME+b'=') else p for p in parts]
    v=b'; '.join(parts)
   headers.append((k,v))
  scope=dict(scope,headers=headers)
  async def rename(message):
   if message['type']=='http.response.start':
    message['headers']=[(k,DEVNAME+v[len(REAL):] if k==b'set-cookie' and v.startswith(REAL+b'=') else v)
     for k,v in message.get('headers',[])]
   await send(message)
  return await self.app(scope,receive,rename)


def build():
 settings=Settings.load()
 app=create_app(settings)
 screener=mount(app,settings)
 if mount_workspace(app,settings) is None:raise SystemExit('The workspace failed to mount; see the log above.')
 if screener is None:raise SystemExit('The screener failed to mount; see the log above.')
 auth,db=app.state.auth,app.state.db

 def account(kind):
  email,name,role=ACCOUNTS[kind]
  with db.tx() as c:
   user=c.execute(select(users).where(users.c.email==email)).mappings().first()
   if not user:
    made=auth.create_user(c,email,name,role)
    c.execute(users.update().where(users.c.id==made['id']).values(onboarded=True,policy_version=settings.policy_version,
     preferences={'pilot_access_until':int(time.time())+30*86400}))
   user=c.execute(select(users).where(users.c.email==email)).mappings().first()
  return dict(user)

 @app.get('/__dev/session')
 def dev_session(request:Request):
  kind=request.query_params.get('as','owner')
  if kind not in ACCOUNTS:kind='owner'
  raw,_csrf=auth.issue(account(kind),'web','screener-dev')
  response=RedirectResponse(request.query_params.get('to','/screener') if str(request.query_params.get('to','')).startswith('/') else '/screener',status_code=303)
  response.set_cookie('kanida_session',raw,max_age=86400,httponly=True,samesite='lax',path='/')
  return response
 # ahead of the catch-alls, like the screener's own routes
 app.router.routes.insert(0,app.router.routes.pop())
 return CookieIsolation(app)


if __name__=='__main__':
 uvicorn.run(build(),host='127.0.0.1',port=PORT,access_log=False)
