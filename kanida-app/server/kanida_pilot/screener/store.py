"""var/screener.db — the screener's OWN store. db/derivatives.db and var/intelligence.db are never written.

  scanners       saved definitions. owner_id NULL + is_default=1 = a KANIDA default, visible to everyone and
                 editable by no one (a user duplicates it to change it). Every other row belongs to exactly one
                 user and is returned to that user only.
  subscriptions  per user, per scanner: which transitions notify, and `alerts_from` — the reading from which
                 transitions count, so turning alerts on at 11:00 never replays the morning's transitions.
  results        the latest evaluation of a definition (by digest) for a session. Two scanners that mean the
                 same thing share one evaluation.
  alerts         one row per (user, scanner, instrument, reading, kind). The UNIQUE key is the dedupe: the same
                 transition can never notify twice, and "still matching" is never a transition.
  iv_cache       solved IV / delta / gamma per (contract, reading). A captured reading never changes.
"""
from __future__ import annotations
import json,sqlite3,threading,time,uuid
from pathlib import Path

SCHEMA='''
create table if not exists scanners(
 id text primary key, owner_id text, name text not null, definition text not null, digest text not null,
 source text not null, nl_text text, is_default integer not null default 0, position integer not null default 0,
 description text, created real not null, updated real not null, deleted integer not null default 0);
create index if not exists ix_scanners_owner on scanners(owner_id, deleted);
create table if not exists subscriptions(
 user_id text not null, scanner_id text not null, notify_new integer not null default 0,
 notify_ended integer not null default 0, notify_changed integer not null default 0, alerts_from text,
 updated real not null, primary key(user_id, scanner_id));
create table if not exists results(
 digest text not null, session text not null, through text not null, payload text not null,
 computed_at real not null, primary key(digest, session));
create table if not exists alerts(
 id integer primary key, user_id text not null, scanner_id text not null, session text not null,
 reading_at text not null, entity_key text not null, kind text not null, title text not null, text text not null,
 created real not null, read integer not null default 0,
 unique(user_id, scanner_id, session, entity_key, reading_at, kind));
create index if not exists ix_alerts_user on alerts(user_id, read, created);
create table if not exists iv_cache(
 token integer not null, at text not null, iv_pct real, reason text, delta real, gamma real,
 primary key(token, at)) without rowid;
create index if not exists ix_iv_at on iv_cache(at);
'''


class ScreenerStore:
 def __init__(self,path):
  self.path=str(path)
  Path(self.path).parent.mkdir(parents=True,exist_ok=True)
  self.lock=threading.RLock()
  self.c=sqlite3.connect(self.path,check_same_thread=False,timeout=15)
  self.c.row_factory=sqlite3.Row
  with self.lock:
   self.c.execute('pragma journal_mode=wal')
   self.c.executescript(SCHEMA)
   self.c.commit()

 def close(self):self.c.close()

 def _q(self,sql,args=()):
  with self.lock:return [dict(r) for r in self.c.execute(sql,args).fetchall()]

 def _x(self,sql,args=()):
  with self.lock:
   cur=self.c.execute(sql,args);self.c.commit();return cur

 # --- scanners ------------------------------------------------------------------------------------------
 @staticmethod
 def _scanner(r):
  if not r:return None
  r=dict(r);r['definition']=json.loads(r['definition']);r['is_default']=bool(r['is_default'])
  return r

 def scanner(self,scanner_id):
  rows=self._q('select * from scanners where id=? and deleted=0',(scanner_id,))
  return self._scanner(rows[0]) if rows else None

 def scanners_for(self,user_id):
  rows=self._q('select * from scanners where deleted=0 and (is_default=1 or owner_id=?) '
   'order by is_default desc, position, updated desc',(user_id,))
  return [self._scanner(r) for r in rows]

 def all_scanners(self):
  return [self._scanner(r) for r in self._q('select * from scanners where deleted=0')]

 def create(self,owner_id,name,definition,digest,source,nl_text=None,is_default=False,position=0,description=None,scanner_id=None):
  now=time.time();sid=scanner_id or uuid.uuid4().hex[:16]
  self._x('insert into scanners(id,owner_id,name,definition,digest,source,nl_text,is_default,position,description,created,updated)'
   ' values(?,?,?,?,?,?,?,?,?,?,?,?)',(sid,owner_id,name,json.dumps(definition,sort_keys=True),digest,source,nl_text,
   int(is_default),position,description,now,now))
  return self.scanner(sid)

 def update(self,scanner_id,**fields):
  sets,args=[],[]
  for k,v in fields.items():
   if k=='definition':v=json.dumps(v,sort_keys=True)
   sets.append(f'{k}=?');args.append(v)
  sets.append('updated=?');args.append(time.time())
  self._x(f'update scanners set {",".join(sets)} where id=?',(*args,scanner_id))
  return self.scanner(scanner_id)

 def delete(self,scanner_id):
  self._x('update scanners set deleted=1, updated=? where id=?',(time.time(),scanner_id))
  self._x('delete from subscriptions where scanner_id=?',(scanner_id,))

 def upsert_default(self,slug,name,definition,digest,position,description):
  existing=self._q('select id from scanners where id=?',(slug,))
  if existing:
   self._x('update scanners set name=?,definition=?,digest=?,position=?,description=?,deleted=0 where id=?',
    (name,json.dumps(definition,sort_keys=True),digest,position,description,slug))
  else:
   self.create(None,name,definition,digest,'default',is_default=True,position=position,description=description,scanner_id=slug)

 # --- subscriptions -------------------------------------------------------------------------------------
 def subscription(self,user_id,scanner_id):
  rows=self._q('select * from subscriptions where user_id=? and scanner_id=?',(user_id,scanner_id))
  return rows[0] if rows else None

 def subscriptions_for(self,user_id):
  return {r['scanner_id']:r for r in self._q('select * from subscriptions where user_id=?',(user_id,))}

 def all_subscriptions(self):
  return self._q('select * from subscriptions where notify_new=1 or notify_ended=1 or notify_changed=1')

 def subscribe(self,user_id,scanner_id,new,ended,changed,alerts_from):
  self._x('insert into subscriptions(user_id,scanner_id,notify_new,notify_ended,notify_changed,alerts_from,updated)'
   ' values(?,?,?,?,?,?,?) on conflict(user_id,scanner_id) do update set notify_new=excluded.notify_new,'
   ' notify_ended=excluded.notify_ended, notify_changed=excluded.notify_changed, alerts_from=excluded.alerts_from,'
   ' updated=excluded.updated',(user_id,scanner_id,int(new),int(ended),int(changed),alerts_from,time.time()))
  return self.subscription(user_id,scanner_id)

 # --- results -------------------------------------------------------------------------------------------
 def result(self,digest,session):
  rows=self._q('select * from results where digest=? and session=?',(digest,session))
  if not rows:return None
  r=rows[0];r['payload']=json.loads(r['payload']);return r

 def put_result(self,digest,session,through,payload):
  self._x('insert into results(digest,session,through,payload,computed_at) values(?,?,?,?,?) on conflict(digest,session)'
   ' do update set through=excluded.through,payload=excluded.payload,computed_at=excluded.computed_at',
   (digest,session,through,json.dumps(payload),time.time()))

 # --- alerts --------------------------------------------------------------------------------------------
 def add_alerts(self,rows):
  """rows: (user_id, scanner_id, session, reading_at, entity_key, kind, title, text). Duplicates are ignored."""
  if not rows:return 0
  now=time.time()
  with self.lock:
   before=self.c.total_changes
   self.c.executemany('insert or ignore into alerts(user_id,scanner_id,session,reading_at,entity_key,kind,title,text,created)'
    ' values(?,?,?,?,?,?,?,?,?)',[(*r,now) for r in rows])
   self.c.commit()
   return self.c.total_changes-before

 def alerts_for(self,user_id,limit=50,unread_only=False):
  sql='select a.*, s.name as scanner_name from alerts a left join scanners s on s.id=a.scanner_id where a.user_id=?'
  if unread_only:sql+=' and a.read=0'
  return self._q(sql+' order by a.created desc, a.id desc limit ?',(user_id,limit))

 def unread_count(self,user_id):
  return self._q('select count(*) n from alerts where user_id=? and read=0',(user_id,))[0]['n']

 def mark_read(self,user_id,ids=None):
  if ids:
   marks=','.join('?'*len(ids))
   self._x(f'update alerts set read=1 where user_id=? and id in ({marks})',(user_id,*ids))
  else:self._x('update alerts set read=1 where user_id=?',(user_id,))

 # --- IV cache ------------------------------------------------------------------------------------------
 def iv_rows(self,at):
  with self.lock:return self.c.execute('select token,iv_pct,reason,delta,gamma from iv_cache where at=?',(at,)).fetchall()

 def put_iv(self,rows):
  if not rows:return
  with self.lock:
   self.c.executemany('insert or replace into iv_cache(token,at,iv_pct,reason,delta,gamma) values(?,?,?,?,?,?)',rows)
   self.c.commit()
