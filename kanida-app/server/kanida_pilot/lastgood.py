"""Last-good cache for the DISPLAY reads the app needs to render (BACKLOG item 2a).

The scanner (`market_scanner`, 127.0.0.1:8765) takes minutes to load ~90,000 setups at startup and
can be restarted mid-day. While it is unreachable the app used to show a full-page error. This store
keeps the last SUCCESSFUL body of each display read so the app can keep showing the last scan, with
the time it was taken, instead of an error page.

Three rules this file exists to hold:

1. **Display only.** Nothing that decides a trade is written here. `evidence.CACHEABLE` is the whole
   list of paths that may be stored, and every read has to ask for the fallback explicitly
   (`Evidence.get(..., cache=True)`). The trade gates call the same `get()` without that flag and
   therefore still fail closed when the scanner is down.
2. **Bounded.** One SQLite file with a row cap, a per-entry byte cap and a total byte cap. The oldest
   entries are evicted first, so a long-running pilot cannot grow this without limit.
3. **Never fatal.** A broken or unwritable cache file must never take a live read down with it, so
   every operation swallows its own errors and simply behaves as an empty cache.

It survives a pilot restart because it is a file (`var/last_good.sqlite3`), not process memory.
"""
from __future__ import annotations
import json,sqlite3,threading,time
from pathlib import Path

#: At most this many distinct (path, query) responses are kept.
MAX_ENTRIES=200
#: A single response larger than this is not worth keeping (and is never the app's render path).
MAX_ENTRY_BYTES=6*1024*1024
#: Hard ceiling on the whole file's stored bodies.
MAX_TOTAL_BYTES=24*1024*1024

SCHEMA='''CREATE TABLE IF NOT EXISTS last_good(
 key TEXT PRIMARY KEY, path TEXT NOT NULL, body TEXT NOT NULL, bytes INTEGER NOT NULL, stored REAL NOT NULL);
CREATE INDEX IF NOT EXISTS idx_last_good_stored ON last_good(stored);'''

class LastGood:
 """The last successful body per (path, query) key. `path=''` keeps it in memory only (tests)."""
 def __init__(self,path='',max_entries=MAX_ENTRIES,max_entry_bytes=MAX_ENTRY_BYTES,max_total_bytes=MAX_TOTAL_BYTES):
  self.path=str(path or '');self.lock=threading.Lock()
  self.max_entries=int(max_entries);self.max_entry_bytes=int(max_entry_bytes);self.max_total_bytes=int(max_total_bytes)
  self.memory={}
  if self.path:
   try:
    Path(self.path).parent.mkdir(parents=True,exist_ok=True)
    with self.connect() as con:con.executescript(SCHEMA)
   except Exception:self.path=''  # an unwritable file degrades to memory, never to a crash

 def connect(self):
  con=sqlite3.connect(self.path,timeout=10,check_same_thread=False)
  con.execute('PRAGMA journal_mode=WAL');con.execute('PRAGMA busy_timeout=10000')
  return con

 def put(self,path,key,value):
  """Store one successful body. Returns True when it was kept."""
  try:body=json.dumps(value,allow_nan=False)
  except (TypeError,ValueError):return False
  size=len(body.encode('utf-8'))
  if size>self.max_entry_bytes:return False
  # Sub-second, so several entries written inside the same second still evict in the order they arrived.
  stored=time.time()
  with self.lock:
   if not self.path:
    self.memory[key]=(path,body,size,stored);self.trim_memory();return True
   try:
    with self.connect() as con:
     con.execute('INSERT INTO last_good(key,path,body,bytes,stored) VALUES(?,?,?,?,?) '
      'ON CONFLICT(key) DO UPDATE SET path=excluded.path,body=excluded.body,bytes=excluded.bytes,stored=excluded.stored',
      (key,path,body,size,stored))
     self.trim(con)
     con.commit()
    return True
   except Exception:return False

 def get(self,key):
  """`(value, stored_epoch)` for the last good body, or None."""
  with self.lock:
   if not self.path:
    hit=self.memory.get(key)
    if not hit:return None
    try:return json.loads(hit[1]),hit[3]
    except ValueError:return None
   try:
    with self.connect() as con:
     found=con.execute('SELECT body,stored FROM last_good WHERE key=?',(key,)).fetchone()
   except Exception:return None
  if not found:return None
  try:return json.loads(found[0]),float(found[1])
  except (ValueError,TypeError):return None

 def trim(self,con):
  """Oldest-first eviction down to both caps. Called inside the writer's transaction."""
  con.execute('DELETE FROM last_good WHERE key IN (SELECT key FROM last_good ORDER BY stored DESC, rowid DESC LIMIT -1 OFFSET ?)',
   (self.max_entries,))
  total=con.execute('SELECT COALESCE(SUM(bytes),0) FROM last_good').fetchone()[0] or 0
  while total>self.max_total_bytes:
   oldest=con.execute('SELECT key,bytes FROM last_good ORDER BY stored,rowid LIMIT 1').fetchone()
   if not oldest:break
   con.execute('DELETE FROM last_good WHERE key=?',(oldest[0],));total-=oldest[1]

 def trim_memory(self):
  while len(self.memory)>self.max_entries or sum(v[2] for v in self.memory.values())>self.max_total_bytes:
   oldest=min(self.memory,key=lambda k:(self.memory[k][3],k))
   del self.memory[oldest]
   if not self.memory:break

 def stats(self):
  """`{entries, bytes, oldest, newest}` - what the operator needs to see how big this got."""
  with self.lock:
   if not self.path:
    return dict(entries=len(self.memory),bytes=sum(v[2] for v in self.memory.values()),
     oldest=min((v[3] for v in self.memory.values()),default=None),newest=max((v[3] for v in self.memory.values()),default=None),
     path=None,max_entries=self.max_entries,max_bytes=self.max_total_bytes)
   try:
    with self.connect() as con:
     row=con.execute('SELECT COUNT(*),COALESCE(SUM(bytes),0),MIN(stored),MAX(stored) FROM last_good').fetchone()
   except Exception:row=(0,0,None,None)
  return dict(entries=row[0],bytes=row[1],oldest=row[2],newest=row[3],path=self.path,
   max_entries=self.max_entries,max_bytes=self.max_total_bytes)
