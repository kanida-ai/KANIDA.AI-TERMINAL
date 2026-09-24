"""var/workspace.db — saved workspaces, one row each, owner-scoped.

`version` makes autosave safe: a save carries the version it was based on, and a save based on an older version
than the one stored (a second tab, a second device) is refused with the stored copy instead of silently
overwriting it. Scanners are NOT stored here: widgets reference them by id, and they stay in var/screener.db.
"""
from __future__ import annotations
import json,sqlite3,threading,time,uuid
from pathlib import Path

SCHEMA='''
create table if not exists workspaces(
 id text primary key, user_id text not null, name text not null, position integer not null default 0,
 template text, selected text not null, widgets text not null, version integer not null default 1,
 created_at real not null, updated_at real not null, deleted integer not null default 0);
create index if not exists ix_ws_user on workspaces(user_id, deleted, position);
'''


class Conflict(Exception):
 def __init__(self,current):super().__init__('stale version');self.current=current


class WorkspaceStore:
 def __init__(self,path):
  Path(path).parent.mkdir(parents=True,exist_ok=True)
  self.lock=threading.RLock()
  self.c=sqlite3.connect(str(path),check_same_thread=False,timeout=15)
  self.c.row_factory=sqlite3.Row
  with self.lock:
   self.c.execute('pragma journal_mode=wal');self.c.executescript(SCHEMA)
   # added after the first release: a store created before it gains the column, empty = the default layout
   if 'layout' not in {r[1] for r in self.c.execute('pragma table_info(workspaces)')}:
    self.c.execute("alter table workspaces add column layout text not null default '{}'")
   self.c.commit()

 def close(self):self.c.close()

 @staticmethod
 def _row(r):
  if not r:return None
  d=dict(r);d['selected']=json.loads(d['selected']);d['widgets']=json.loads(d['widgets']);d.pop('deleted',None)
  d['layout']={'columns':'auto',**json.loads(d.get('layout') or '{}')}
  return d

 def get(self,user_id,ws_id):
  with self.lock:
   r=self.c.execute('select * from workspaces where id=? and user_id=? and deleted=0',(ws_id,user_id)).fetchone()
  return self._row(r)

 def list(self,user_id):
  with self.lock:
   rows=self.c.execute('select * from workspaces where user_id=? and deleted=0 order by position, created_at',(user_id,)).fetchall()
  return [self._row(r) for r in rows]

 def create(self,user_id,definition,template=None):
  now=time.time();wid=uuid.uuid4().hex[:16]
  with self.lock:
   pos=self.c.execute('select coalesce(max(position),-1)+1 from workspaces where user_id=? and deleted=0',(user_id,)).fetchone()[0]
   self.c.execute('insert into workspaces(id,user_id,name,position,template,selected,widgets,layout,version,created_at,updated_at)'
    ' values(?,?,?,?,?,?,?,?,1,?,?)',(wid,user_id,definition['name'],pos,template,json.dumps(definition['selected']),
    json.dumps(definition['widgets']),json.dumps(definition.get('layout') or {}),now,now))
   self.c.commit()
  return self.get(user_id,wid)

 def save(self,user_id,ws_id,definition,base_version):
  with self.lock:
   cur=self.get(user_id,ws_id)
   if cur is None:return None
   if base_version is not None and int(base_version)!=cur['version']:raise Conflict(cur)
   self.c.execute('update workspaces set name=?,selected=?,widgets=?,layout=?,version=version+1,updated_at=? where id=? and user_id=?',
    (definition['name'],json.dumps(definition['selected']),json.dumps(definition['widgets']),
     json.dumps(definition.get('layout') or {}),time.time(),ws_id,user_id))
   self.c.commit()
  return self.get(user_id,ws_id)

 def delete(self,user_id,ws_id):
  with self.lock:
   n=self.c.execute('update workspaces set deleted=1,updated_at=? where id=? and user_id=? and deleted=0',
    (time.time(),ws_id,user_id)).rowcount
   self.c.commit()
  return n>0
