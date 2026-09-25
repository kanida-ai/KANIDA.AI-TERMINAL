"""Release evidence for the strategy product (GTM audit P24): request latency, failures and a privacy-safe funnel.

  * Latency: every /api/sb/* request is timed by route TEMPLATE (never by id-bearing path); p50/p95/max and error
    rates over the last RING samples per route, in memory.
  * Funnel: start -> analyze -> save -> snapshot -> review -> deploy (+ discover, lab_run, autotrade), persisted. The
    only identifier is a salted hash of the session cookie (pseudonymous, not anonymous) - no user id, e-mail, strategy content, price or token.
  * Owner-only read: GET /api/sb/ops/metrics.
"""
from __future__ import annotations
import hashlib,os,threading,time
from collections import defaultdict,deque

RING=500
FUNNEL={('POST','/api/sb/strategies'):'start',('POST','/api/sb/analyze'):'analyze',('POST','/api/sb/strategies/{sid}/draft'):'save',
 ('POST','/api/sb/strategies/{sid}/snapshots'):'snapshot',('POST','/api/sb/strategies/{sid}/preview'):'review',
 ('POST','/api/sb/strategies/{sid}/deployments'):'deploy',('POST','/api/sb/discover'):'discover',('POST','/api/sb/discover/use'):'discover_use',
 ('POST','/api/sb/lab/backtests'):'lab_run',('POST','/api/sb/strategies/{sid}/autotrade'):'autotrade',('POST','/api/sb/strategies/{sid}/paper'):'paper'}
ORDER=['start','analyze','save','snapshot','review','deploy']
SCHEMA='''
create table if not exists sb_events(id integer primary key autoincrement, at real not null, kind text not null, actor text not null, ok integer not null);
create index if not exists ix_sb_events on sb_events(kind, at);
create table if not exists sb_meta(k text primary key, v text not null);
'''


def pct(xs,q):
 if not xs:return None
 s=sorted(xs);i=min(len(s)-1,max(0,int(round(q*(len(s)-1)))))
 return round(s[i],1)


class Ops:
 def __init__(self,store):
  self.c=store.c;self.lock=store.lock;self._lat=defaultdict(lambda:deque(maxlen=RING));self._err=defaultdict(int);self._n=defaultdict(int);self._mlock=threading.Lock()
  with self.lock:
   self.c.executescript(SCHEMA)
   r=self.c.execute("select v from sb_meta where k='salt'").fetchone()
   if not r:self.c.execute("insert into sb_meta values('salt',?)",(os.urandom(16).hex(),))
   self.c.commit()
   self.salt=self.c.execute("select v from sb_meta where k='salt'").fetchone()[0]

 def actor(self,session_cookie):
  return hashlib.sha256((self.salt+(session_cookie or 'anon')).encode()).hexdigest()[:12]

 def observe(self,method,template,status,ms,session_cookie):
  key=f'{method} {template}'
  with self._mlock:
   self._lat[key].append(ms);self._n[key]+=1
   if status>=500:self._err[key]+=1
  kind=FUNNEL.get((method,template))
  if kind:
   with self.lock:
    t=time.time()
    self.c.execute('insert into sb_events(at,kind,actor,ok) values(?,?,?,?)',(t,kind,self.actor(session_cookie),int(status<400)))
    if int(t)%97==0:self.c.execute('delete from sb_events where at<?',(t-90*86400,))   # keep 90 days
    self.c.commit()

 def metrics(self,days=7):
  with self._mlock:
   routes=[{'route':k,'n':self._n[k],'p50_ms':pct(list(v),.5),'p95_ms':pct(list(v),.95),'max_ms':round(max(v),1) if v else None,
    'errors_5xx':self._err[k]} for k,v in sorted(self._lat.items())]
  since=time.time()-days*86400
  with self.lock:
   rows=self.c.execute('select kind,count(*) n,count(distinct actor) actors,sum(1-ok) failed from sb_events where at>=? group by kind',(since,)).fetchall()
  ev={r['kind']:{'events':r['n'],'sessions':r['actors'],'failed':r['failed']} for r in rows}
  return {'window_days':days,'latency':{'since':'process start','samples_per_route':RING,'routes':routes},
   'funnel':[{'step':k,**ev.get(k,{'events':0,'sessions':0,'failed':0})} for k in ORDER],
   'other':{k:v for k,v in ev.items() if k not in ORDER},
   'privacy':'Pseudonymous: sessions are salted hashes of the session cookie (the salt is stored with the data, so this is not anonymisation). No user id, e-mail, strategy content, price or P&L is stored. Steps count events and sessions per step; they are not a per-user conversion path.'}


def install(app,ops,prefix='/api/sb/'):
 """Time every strategy-builder request by its route template (added once, at mount)."""
 @app.middleware('http')
 async def _sb_timing(request,call_next):
  if not request.url.path.startswith(prefix):return await call_next(request)
  t=time.perf_counter();status=500
  try:
   resp=await call_next(request);status=resp.status_code;return resp
  finally:
   route=request.scope.get('route');tpl=getattr(route,'path',None) or 'unmatched'
   try:ops.observe(request.method,tpl,status,(time.perf_counter()-t)*1000,request.cookies.get('kanida_session'))
   except Exception:pass  # noqa: BLE001 - measurement never breaks a request
