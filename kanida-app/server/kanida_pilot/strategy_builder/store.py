"""var/strategy_builder.db — strategies, autosaved drafts, immutable snapshots, paper runs and their fills. Owner-scoped.

Object language (blueprint B §2):
  strategy   the durable container (name, thesis, tags)
  draft      the one editable research state; every save carries the version it was based on, and a save based on
             an older version (a second tab) is refused with the stored copy - never a silent overwrite
  snapshot   a named, IMMUTABLE revision of the draft, with a checksum; restoring one makes a new draft version
  paper run  a simulated execution ledger against ONE snapshot; fills are append-only
Research / Paper / Archived are DERIVED views over these rows, not a status column that could lie.
"""
from __future__ import annotations
import hashlib,json,sqlite3,threading,time,uuid
from pathlib import Path

SCHEMA='''
create table if not exists strategies(
 id text primary key, user_id text not null, name text not null, thesis text not null default '', tags text not null default '[]',
 underlying text, source_strategy_id text, created_at real not null, updated_at real not null, archived_at real);
create index if not exists ix_sb_user on strategies(user_id, archived_at, updated_at);
create table if not exists drafts(
 strategy_id text primary key, version integer not null, body text not null, updated_at real not null);
create table if not exists revisions(
 id text primary key, strategy_id text not null, n integer not null, name text not null, body text not null,
 checksum text not null, reading_at text, analysis text, created_at real not null, unique(strategy_id, n));
create table if not exists paper_runs(
 id text primary key, user_id text not null, strategy_id text not null, revision_id text not null, status text not null,
 policy text not null, opened_reading text not null, closed_reading text, created_at real not null, closed_at real);
create index if not exists ix_sb_paper on paper_runs(user_id, status);
create table if not exists paper_fills(
 id text primary key, run_id text not null, seq integer not null, leg_id text not null, action text not null, side text not null,
 units integer not null, price real not null, basis text not null, fees real not null, reading_at text not null, created_at real not null);
create table if not exists activity(
 id text primary key, user_id text not null, strategy_id text not null, kind text not null, detail text not null, created_at real not null);
create index if not exists ix_sb_activity on activity(strategy_id, created_at);
-- slice 14: the immutable decision log (the moat's data clock). Every strategy decision and outcome, append-only: no
-- UPDATE or DELETE is possible (triggers abort), and each row chains the previous row's hash so tampering is detectable.
-- research_consent is copied from the user at write time; pooled research may only ever read rows where it is 1.
create table if not exists decision_log(
 seq integer primary key autoincrement, id text not null unique, user_id text not null, strategy_id text, kind text not null,
 action_at real not null, market_sample_at text, mode text, payload text not null, research_consent integer not null default 0,
 prev_hash text not null, hash text not null);
create index if not exists ix_sb_decisions on decision_log(user_id, action_at);
create table if not exists research_consent(user_id text primary key, consent integer not null, policy_version text not null, updated_at real not null);
create table if not exists research_consent_history(user_id text not null, consent integer not null, policy_version text not null, at real not null);
create trigger if not exists decision_log_no_update before update on decision_log begin select raise(abort,'decision_log is append-only'); end;
create trigger if not exists decision_log_no_delete before delete on decision_log begin select raise(abort,'decision_log is append-only'); end;
create table if not exists snapshot_requests(
 user_id text not null, strategy_id text not null, request_id text not null, revision_id text not null, created_at real not null,
 primary key(user_id, strategy_id, request_id));
'''
MAX_STRATEGIES=200


class Conflict(Exception):
 def __init__(self,current):super().__init__('stale version');self.current=current


def checksum(body):return hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def uid():return uuid.uuid4().hex[:16]


class Store:
 def __init__(self,path):
  Path(path).parent.mkdir(parents=True,exist_ok=True)
  self.lock=threading.RLock()
  self.c=sqlite3.connect(str(path),check_same_thread=False,timeout=15);self.c.row_factory=sqlite3.Row
  with self.lock:
   self.c.execute('pragma journal_mode=wal');self.c.executescript(SCHEMA)
   if 'notes' not in {r[1] for r in self.c.execute('pragma table_info(revisions)').fetchall()}:
    self.c.execute("alter table revisions add column notes text not null default ''")
   self.c.commit()

 def close(self):
  for fn in getattr(self,'closers',[]):
   try:fn()
   except Exception:pass  # noqa: BLE001
  self.c.close()

 def _log(self,user_id,sid,kind,detail):
  self.c.execute('insert into activity values(?,?,?,?,?,?)',(uid(),user_id,sid,kind,detail,time.time()))

 # --- the immutable decision log (slice 14) ----------------------------------------------------------------------
 DECISIONS=('strategy_created','snapshot','paper_open','paper_close','deployment_open','fill','adjustment_chosen',
            'deployment_close','exit_rule_fired','alert_triggered','settlement')

 def decide(self,user_id,kind,payload,strategy_id=None,market_sample_at=None,mode=None,consent=False,commit=True):
  """Append one decision or outcome. Never raises into the caller's flow: a log failure is logged, the action stands."""
  if kind not in self.DECISIONS:raise ValueError(f'unknown decision kind {kind}')
  try:
   with self.lock:
    prev=self.c.execute('select hash from decision_log order by seq desc limit 1').fetchone()
    prev=prev[0] if prev else '0'*64;did=uid();at=time.time()
    body=json.dumps(payload,sort_keys=True,separators=(',',':'),default=str)
    h=hashlib.sha256('|'.join([prev,did,user_id,strategy_id or '',kind,f'{at:.6f}',market_sample_at or '',mode or '',body,str(int(bool(consent)))]).encode()).hexdigest()
    self.c.execute('insert into decision_log(id,user_id,strategy_id,kind,action_at,market_sample_at,mode,payload,research_consent,prev_hash,hash) values(?,?,?,?,?,?,?,?,?,?,?)',
     (did,user_id,strategy_id,kind,at,market_sample_at,mode,body,int(bool(consent)),prev,h))
    if commit:self.c.commit()
   return did
  except Exception:
   import logging;logging.getLogger('strategy_builder.decisions').exception('decision log write failed (%s)',kind);return None

 CONSENT_POLICY='research-consent-v1'

 def consent(self,user_id)->bool:
  """Opt-in only: pooled research may use a user's decisions only after they said yes. Default False."""
  with self.lock:r=self.c.execute('select consent from research_consent where user_id=?',(user_id,)).fetchone()
  return bool(r and r[0])

 def set_consent(self,user_id,on):
  with self.lock:
   t=time.time()
   self.c.execute('insert or replace into research_consent values(?,?,?,?)',(user_id,int(bool(on)),self.CONSENT_POLICY,t))
   self.c.execute('insert into research_consent_history values(?,?,?,?)',(user_id,int(bool(on)),self.CONSENT_POLICY,t))
   self.c.commit()
  return {'consent':bool(on),'policy_version':self.CONSENT_POLICY,'updated_at':t}

 def decisions(self,user_id,strategy_id=None,limit=500):
  with self.lock:
   q='select seq,id,strategy_id,kind,action_at,market_sample_at,mode,payload,research_consent from decision_log where user_id=?'+(' and strategy_id=?' if strategy_id else '')+' order by seq desc limit ?'
   rows=self.c.execute(q,(user_id,strategy_id,limit) if strategy_id else (user_id,limit)).fetchall()
  return [{**dict(r),'payload':json.loads(r['payload'])} for r in rows]

 def verify_decisions(self):
  """(ok, rows checked, first bad seq): recompute the hash chain end to end."""
  with self.lock:rows=self.c.execute('select * from decision_log order by seq').fetchall()
  prev='0'*64
  for r in rows:
   h=hashlib.sha256('|'.join([prev,r['id'],r['user_id'],r['strategy_id'] or '',r['kind'],f"{r['action_at']:.6f}",r['market_sample_at'] or '',r['mode'] or '',r['payload'],str(r['research_consent'])]).encode()).hexdigest()
   if r['prev_hash']!=prev or r['hash']!=h:return False,len(rows),r['seq']
   prev=h
  return True,len(rows),None

 # --- strategies ------------------------------------------------------------------------------------------------
 def _strategy(self,r):
  if not r:return None
  d=dict(r);d['tags']=json.loads(d['tags']);return d

 def list(self,user_id):
  with self.lock:
   rows=self.c.execute('select s.*,d.version draft_version,d.body draft_body,(select count(*) from revisions v where v.strategy_id=s.id) snapshots,'
    "(select count(*) from paper_runs p where p.strategy_id=s.id and p.status='open') paper_open,"
    '(select count(*) from paper_runs p where p.strategy_id=s.id) paper_total from strategies s left join drafts d on d.strategy_id=s.id '
    'where s.user_id=? order by s.updated_at desc',(user_id,)).fetchall()
  out=[]
  for r in rows:
   d=self._strategy(r);body=json.loads(d.pop('draft_body') or '{}')
   d['legs']=len(body.get('legs') or []);d['expiry']=body.get('expiry');d['structure']=body.get('structure')
   out.append(d)
  return out

 def get(self,user_id,sid):
  with self.lock:
   s=self._strategy(self.c.execute('select * from strategies where id=? and user_id=?',(sid,user_id)).fetchone())
   if not s:return None
   d=self.c.execute('select version,body,updated_at from drafts where strategy_id=?',(sid,)).fetchone()
  if d:
   b=json.loads(d['body'])
   s['draft']={'version':d['version'],'body':b,'updated_at':d['updated_at'],'checksum':checksum(b)}
  else:s['draft']=None
  return s

 def create(self,user_id,name,body,thesis='',source=None):
  with self.lock:
   n=self.c.execute('select count(*) from strategies where user_id=? and archived_at is null',(user_id,)).fetchone()[0]
   if n>=MAX_STRATEGIES:raise ValueError(f'You can keep up to {MAX_STRATEGIES} active strategies. Archive some first.')
   sid=uid();t=time.time()
   self.c.execute('insert into strategies(id,user_id,name,thesis,tags,underlying,source_strategy_id,created_at,updated_at) values(?,?,?,?,?,?,?,?,?)',
    (sid,user_id,name,thesis,'[]',body.get('underlying'),source,t,t))
   self.c.execute('insert into drafts values(?,?,?,?)',(sid,1,json.dumps(body),t))
   self._log(user_id,sid,'created',f'Created "{name}"'+(' as a copy' if source else ''))
   self.decide(user_id,'strategy_created',{'name':name,'body':body,'source_strategy_id':source,'thesis':thesis},strategy_id=sid,
    mode='research',consent=self.consent(user_id),commit=False)
   self.c.commit()
  return self.get(user_id,sid)

 def save_draft(self,user_id,sid,version,body):
  with self.lock:
   s=self.get(user_id,sid)
   if not s:return None
   if s['draft']['version']!=version:raise Conflict(s)
   t=time.time()
   self.c.execute('update drafts set version=version+1,body=?,updated_at=? where strategy_id=?',(json.dumps(body),t,sid))
   self.c.execute('update strategies set updated_at=?,underlying=? where id=?',(t,body.get('underlying'),sid))
   self.c.commit()
  return self.get(user_id,sid)

 def update_meta(self,user_id,sid,name=None,thesis=None,tags=None):
  with self.lock:
   s=self.get(user_id,sid)
   if not s:return None
   self.c.execute('update strategies set name=?,thesis=?,tags=?,updated_at=? where id=?',
    (name if name is not None else s['name'],thesis if thesis is not None else s['thesis'],
     json.dumps(tags if tags is not None else s['tags']),time.time(),sid))
   if name is not None and name!=s['name']:self._log(user_id,sid,'renamed',f'Renamed to "{name}"')
   self.c.commit()
  return self.get(user_id,sid)

 def archive(self,user_id,sid,on=True):
  with self.lock:
   s=self.get(user_id,sid)
   if not s:return None
   self.c.execute('update strategies set archived_at=? where id=?',(time.time() if on else None,sid))
   self._log(user_id,sid,'archived' if on else 'restored','Archived' if on else 'Restored from archive')
   self.c.commit()
  return self.get(user_id,sid)

 # --- snapshots (immutable revisions) ---------------------------------------------------------------------------
 def snapshot_for_request(self,user_id,sid,request_id):
  """The revision an earlier identical snapshot request created (a repeated click never makes a second one)."""
  with self.lock:
   r=self.c.execute('select revision_id from snapshot_requests where user_id=? and strategy_id=? and request_id=?',(user_id,sid,request_id)).fetchone()
  return self.revision(user_id,r[0]) if r else None

 def snapshot(self,user_id,sid,name,body,reading_at,analysis,request_id=None):
  with self.lock:
   s=self.get(user_id,sid)
   if not s:return None
   if request_id:     # re-checked under the lock: two concurrent identical requests create ONE revision (review F3)
    done=self.c.execute('select revision_id from snapshot_requests where user_id=? and strategy_id=? and request_id=?',(user_id,sid,request_id)).fetchone()
    if done:rid=done[0];done=True
   else:done=None
  if done:return {**self.revision(user_id,rid),'repeated':True}
  with self.lock:
   n=(self.c.execute('select max(n) from revisions where strategy_id=?',(sid,)).fetchone()[0] or 0)+1
   rid=uid()
   self.c.execute('insert into revisions(id,strategy_id,n,name,body,checksum,reading_at,analysis,created_at) values(?,?,?,?,?,?,?,?,?)',(rid,sid,n,name or f'Snapshot {n}',json.dumps(body),checksum(body),
    reading_at,json.dumps(analysis),time.time()))
   self._log(user_id,sid,'snapshot',f'Saved snapshot {n}: {name or "Snapshot "+str(n)}')
   self.decide(user_id,'snapshot',{'revision_id':rid,'n':n,'name':name,'body':body,'analysis':_summary(analysis or {})},strategy_id=sid,
    market_sample_at=reading_at,mode='research',consent=self.consent(user_id),commit=False)
   if request_id:self.c.execute('insert or ignore into snapshot_requests values(?,?,?,?,?)',(user_id,sid,request_id,rid,time.time()))
   self.c.commit()
  return self.revision(user_id,rid)

 def revisions(self,user_id,sid):
  with self.lock:
   if not self.get(user_id,sid):return None
   rows=self.c.execute('select id,n,name,notes,checksum,reading_at,created_at,analysis from revisions where strategy_id=? order by n desc',(sid,)).fetchall()
  return [{**{k:r[k] for k in ('id','n','name','notes','checksum','reading_at','created_at')},'summary':_summary(json.loads(r['analysis'] or '{}'))} for r in rows]

 def update_revision(self,user_id,rid,name=None,notes=None):
  """Rename a snapshot or edit its notes. Its body, analysis and checksum are immutable and never touched (GTM P18)."""
  with self.lock:
   r=self.c.execute('select v.id,v.strategy_id,v.n,v.name from revisions v join strategies s on s.id=v.strategy_id where v.id=? and s.user_id=?',(rid,user_id)).fetchone()
   if not r:return None
   if name is not None:self.c.execute('update revisions set name=? where id=?',(name,rid))
   if notes is not None:self.c.execute('update revisions set notes=? where id=?',(notes,rid))
   what=[f'renamed to "{name}"'] if name is not None and name!=r['name'] else []
   if notes is not None:what.append('notes edited')
   if what:self._log(user_id,r['strategy_id'],'snapshot_meta',f"Snapshot {r['n']}: "+', '.join(what))
   self.c.commit()
  return self.revision(user_id,rid)

 def revision(self,user_id,rid):
  with self.lock:
   r=self.c.execute('select v.* from revisions v join strategies s on s.id=v.strategy_id where v.id=? and s.user_id=?',(rid,user_id)).fetchone()
  if not r:return None
  d=dict(r);d['body']=json.loads(d['body']);d['analysis']=json.loads(d['analysis'] or '{}')
  d['intact']=checksum(d['body'])==d['checksum']
  return d

 def activity(self,user_id,sid,limit=100):
  with self.lock:
   rows=self.c.execute('select kind,detail,created_at from activity where strategy_id=? and user_id=? order by created_at desc limit ?',(sid,user_id,limit)).fetchall()
  return [dict(r) for r in rows]

 # --- paper -----------------------------------------------------------------------------------------------------
 def paper_open(self,user_id,sid,rid,policy,reading_at,fills):
  with self.lock:
   run=uid();t=time.time()
   self.c.execute('insert into paper_runs values(?,?,?,?,?,?,?,?,?,?)',(run,user_id,sid,rid,'open',json.dumps(policy),reading_at,None,t,None))
   for i,f in enumerate(fills):
    self.c.execute('insert into paper_fills values(?,?,?,?,?,?,?,?,?,?,?,?)',(uid(),run,i,f['leg_id'],'open',f['side'],f['units'],f['price'],
     f['basis'],f['fees'],reading_at,t))
   self._log(user_id,sid,'paper_open',f'Started a paper run on snapshot at reading {reading_at}')
   self.decide(user_id,'paper_open',{'run_id':run,'revision_id':rid,'fills':fills,'policy':policy},strategy_id=sid,market_sample_at=reading_at,
    mode='practice_stored',consent=self.consent(user_id),commit=False)
   self.c.commit()
  return self.paper(user_id,run)

 def paper_close(self,user_id,run,fills,reading_at):
  with self.lock:
   r=self.paper(user_id,run)
   if not r or r['status']!='open':return r
   seq=len(r['fills']);t=time.time()
   for i,f in enumerate(fills):
    self.c.execute('insert into paper_fills values(?,?,?,?,?,?,?,?,?,?,?,?)',(uid(),run,seq+i,f['leg_id'],'close',f['side'],f['units'],f['price'],
     f['basis'],f['fees'],reading_at,t))
   self.c.execute("update paper_runs set status='closed',closed_reading=?,closed_at=? where id=?",(reading_at,t,run))
   self._log(user_id,r['strategy_id'],'paper_close',f'Closed a paper run at reading {reading_at}')
   self.decide(user_id,'paper_close',{'run_id':run,'fills':fills},strategy_id=r['strategy_id'],market_sample_at=reading_at,
    mode='practice_stored',consent=self.consent(user_id),commit=False)
   self.c.commit()
  return self.paper(user_id,run)

 def paper(self,user_id,run):
  with self.lock:
   r=self.c.execute('select * from paper_runs where id=? and user_id=?',(run,user_id)).fetchone()
   if not r:return None
   fills=self.c.execute('select leg_id,action,side,units,price,basis,fees,reading_at,seq from paper_fills where run_id=? order by seq',(run,)).fetchall()
  d=dict(r);d['policy']=json.loads(d['policy']);d['fills']=[dict(f) for f in fills];return d

 def papers(self,user_id,sid=None):
  with self.lock:
   q='select id from paper_runs where user_id=?'+(' and strategy_id=?' if sid else '')+' order by created_at desc'
   ids=[r[0] for r in self.c.execute(q,(user_id,sid) if sid else (user_id,)).fetchall()]
  return [self.paper(user_id,i) for i in ids]


def _summary(a):
 pick=lambda k:(a.get(k) or {}).get('value') if (a.get(k) or {}).get('status')=='available' else None
 return {'max_profit':pick('max_profit'),'max_loss':pick('max_loss'),'unlimited_loss':bool((a.get('max_loss') or {}).get('unlimited')),
  'breakevens':pick('breakevens'),'premium':pick('premium')}
