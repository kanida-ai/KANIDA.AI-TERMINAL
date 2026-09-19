"""Live detections of the 107 researched patterns, read from the scanner's detection ledger.

Why the ledger and not `/api/matches`
-------------------------------------
The Discover catalog serves **all 1,048** research strategies in one response, and a full research pass
writes ~89,000 detections (~50,000 of them live) across ~500 symbols (docs/LIVE_DETECTION.md §7). The
scanner's research `/api/matches` is paginated by `limit` alone (default 500, max 5,000) with **no offset
or cursor**, so neither a per-strategy fan-out (1,048 HTTP calls) nor a whole-book fetch (10+ truncated
pages that can never be stitched) can produce an honest count. The ledger (`detections`, §5A) holds exactly
the same records with `pattern_id`, `variant`, `side`, `timeframe`, `state`, `detected_at` and
`detector_spec_hash` as first-class columns, so one indexed GROUP BY answers every header count and one
indexed SELECT answers a card's list.

Boundary: **read-only**, `mode=ro` + `PRAGMA query_only`, exactly like ResearchStore. This module never
writes to `market_scanner/`, never runs a detector and never invents a detection. Overlays (`lines`) are
deliberately NOT in the ledger payload (`pattern_live.LEDGER_OMIT`); the chart keeps getting them from the
scanner's own `/api/chart`, so geometry has one source and it is the detector's.

Degradation: a missing file, a missing `detections` table (the scanner is running the legacy set, or its
cache was pointed elsewhere) or a locked database all report `available=False`. The caller then serves
`live_detection: false` and the researched-history numbers exactly as before - never a zero that reads
like "we looked and found nothing".
"""
from __future__ import annotations
import json,logging,sqlite3,threading,time
from datetime import datetime,timedelta,timezone
from pathlib import Path

LOG=logging.getLogger('pilot.detections')
IST=timezone(timedelta(hours=5,minutes=30))
#: `pattern_live.resolve_state` lifecycle. `forming`/`confirmed` are the live book; the other two are
#: terminated and are kept only for TERMINAL_GRACE bars plus the ledger's retention window.
LIVE_STATES=('forming','confirmed')
STATES=('forming','confirmed','invalidated','expired')
#: Display order in a list: the live book first, newest first inside each state.
STATE_RANK={'confirmed':0,'forming':1,'invalidated':2,'expired':3}
STATE_LABEL={'forming':'Forming','confirmed':'Confirmed','invalidated':'Invalidated','expired':'Expired'}
#: `live` (alias `active`) is the DEFAULT: every setup still forming or confirmed, whenever it was first detected.
#: A wedge that formed on Monday and is still valid is exactly what a trader needs; `today` narrows to the
#: setups first detected on the latest session and `week` to the last 7 days.
SCOPES=('live','active','today','week')
DEFAULT_SCOPE='live'
WEEK_DAYS=7
DEFAULT_CACHE_SECONDS=20
DEFAULT_ROW_LIMIT=100
ROW_LIMIT_MAX=500
#: Counting every row of an 89k-row ledger four ways is one full scan; it is cached, and this caps how
#: long a single aggregate may take before the caller gives up rather than holding a request open.
QUERY_TIMEOUT=8.0
TIMEFRAMES=('1D','1H','4H','1W')


def strategy_key(pattern_id,variant,side,timeframe):
 """Same key the registry and the research index use (research_store.strategy_key)."""
 return f'{pattern_id}-{variant}-{side}-{timeframe}'.lower()

def today_ist():return datetime.now(IST).date().isoformat()
def days_before(day,n):
 try:return (datetime.strptime(day,'%Y-%m-%d')-timedelta(days=int(n))).date().isoformat()
 except (TypeError,ValueError):return day

def _day(value):
 """The calendar day of a bar-end/scan timestamp ('2026-09-16 15:15' -> '2026-09-16')."""
 text=str(value or '')
 return text[:10] if len(text)>=10 else None


# --- research-run identity ---------------------------------------------------------------------------------
# The frozen research run's detector identity, computed by the SAME function the scanner uses
# (market_scanner/pattern_live.research_identity), so the two hashes are comparable by construction rather
# than by coincidence. Imported lazily and guarded: a research tree this machine cannot read must make the
# app say "no compatible evidence", never crash and never fall back to a pattern-name match.
_IDENTITY={}
_IDENTITY_LOCK=threading.Lock()
#: Tests substitute this; production leaves it None and the real scanner function is used.
identity_source=None

def research_identity(run):
 """`{'run','status','detector_spec_hash'?}` for a frozen research run. `status` is never invented."""
 run=str(run or '')
 if not run:return {'run':run,'status':'unavailable','note':'No research run is registered for this strategy.'}
 with _IDENTITY_LOCK:
  cached=_IDENTITY.get(run)
 if cached is not None:return cached
 if identity_source is not None:
  result=identity_source(run)
 else:
  try:
   from market_scanner import pattern_live  # noqa: PLC0415 - lazy: the pilot must boot without it
   result=pattern_live.research_identity(run)
  except Exception as error:  # unreadable research tree => unavailable, never a name match
   LOG.warning('live detections: the research run identity could not be computed: %s',error)
   result={'run':run,'status':'unavailable','note':f'{type(error).__name__}: {error}'}
 result=dict(result or {},run=run)
 with _IDENTITY_LOCK:_IDENTITY[run]=result
 return result

def evidence_identity(run,detector_spec_hash):
 """Whether ONE live detection may be joined to the research run at all (LIVE_DETECTION.md §6).

 `identity_match` only means the identity PERMITS a research card; whether a card exists for that exact
 (symbol, timeframe, pattern, variant, side) is a separate lookup that this never answers.
 """
 research=research_identity(run)
 expected=research.get('detector_spec_hash')
 if research.get('status')!='known' or not expected:
  return {'status':'unavailable','note':research.get('note') or 'The research run is not readable here; no compatible historical evidence.',
   'research_run':research.get('run'),'research_detector_spec_hash':expected,'detector_spec_hash':detector_spec_hash}
 if not detector_spec_hash:
  return {'status':'unavailable','note':'This detection records no detector identity; no compatible historical evidence.',
   'research_run':research.get('run'),'research_detector_spec_hash':expected,'detector_spec_hash':None}
 if str(detector_spec_hash)!=str(expected):
  return {'status':'detector_mismatch','note':'The live detector identity differs from the research run; no compatible historical evidence.',
   'research_run':research.get('run'),'research_detector_spec_hash':expected,'detector_spec_hash':detector_spec_hash}
 return {'status':'identity_match','note':'Live detector code, specifications and dependencies are byte-identical to the research run.',
  'research_run':research.get('run'),'research_detector_spec_hash':expected,'detector_spec_hash':detector_spec_hash}


class LiveDetections:
 """Read-only view of `detections` in the scanner's scan cache."""

 def __init__(self,path,cache_seconds=DEFAULT_CACHE_SECONDS,labels=None):
  self.path=Path(path) if path else None
  self.cache_seconds=max(0,int(cache_seconds or 0))
  self._labels=labels  # callable -> {symbol: {company, sector}}; the index's own catalogue reader
  self._local=threading.local()
  self._cache=None;self._cache_at=0.0;self._cache_lock=threading.Lock();self._refreshing=False
  # One aggregate at a time. Without this the first page load - catalog plus one results request per scanner
  # card - would each start its own full scan of the same ~130 MB ledger concurrently, and the page would wait
  # on all of them. The winner builds; the others wait and then find the finished aggregate in the cache.
  self._build_lock=threading.Lock()

 # --- freshness stamp -------------------------------------------------
 def _stamp(self):
  """(size, mtime) of the ledger file AND its write-ahead log.

  A committed scan pass writes the WAL first, so the main file's own mtime can sit still across a pass; both
  are stamped. The aggregate below is a full table scan of a ~130 MB ledger, and it is only worth redoing
  when one of these actually moves - which is once every scan pass (2-10 minutes, LIVE_DETECTION.md §7).
  """
  out=[]
  for path in (self.path,Path(str(self.path)+'-wal')) if self.path else ():
   try:
    stat=path.stat();out.append((stat.st_size,stat.st_mtime_ns))
   except OSError:out.append(None)
  return tuple(out)

 # --- connection ------------------------------------------------------
 def _connect(self):
  connection=getattr(self._local,'connection',None)
  if connection is not None:return connection
  if not self.path or not self.path.is_file():return None
  try:
   connection=sqlite3.connect(f'file:{self.path.as_posix()}?mode=ro',uri=True,timeout=QUERY_TIMEOUT,check_same_thread=False)
   connection.row_factory=sqlite3.Row
   connection.execute('PRAGMA query_only=1')
   connection.execute('select 1 from detections limit 1')  # legacy pattern set has no ledger at all
  except sqlite3.Error as error:
   LOG.info('live detections: the scan cache has no readable detection ledger (%s)',error)
   try:
    if connection is not None:connection.close()
   except sqlite3.Error:pass
   return None
  self._local.connection=connection
  return connection
 def _forget(self):
  connection=getattr(self._local,'connection',None)
  if connection is not None:
   try:connection.close()
   except sqlite3.Error:pass
   self._local.connection=None
 def available(self):return self._connect() is not None
 def close(self):self._forget()

 def labels(self):
  if not self._labels:return {}
  try:return self._labels() or {}
  except Exception:return {}

 # --- aggregate -------------------------------------------------------
 def snapshot(self,today=None,force=False):
  """Per-strategy live counts plus the ledger's own freshness. Cached; a request never waits twice.

  Every number is a COUNT over ledger rows. `today`/`week` count the detections whose signal bar closed
  on (or within 7 days of) the IST trading day; `live` counts the rows standing in `forming`/`confirmed`
  right now, however long they have been standing. They are different questions and are never merged.
  """
  day=today or today_ist()
  now=time.monotonic()
  stamp=self._stamp()
  with self._cache_lock:
   cached=self._cache
   if not force and cached is not None and cached['day']==day:
    # Unchanged ledger, same trading day: the previous aggregate is still exactly right.
    if cached['stamp']==stamp:return cached
    # The ledger moved. Serve the previous aggregate (it carries its own `scanned_at`, so it is never
    # presented as newer than it is) and rebuild off the request path, so no page waits on a full scan.
    if now-self._cache_at>=self.cache_seconds and not self._refreshing:
     self._refreshing=True
     threading.Thread(target=self._refresh,args=(day,),name='live-detections',daemon=True).start()
    return cached
  with self._build_lock:
   # Another caller may have finished the very same build while this one waited for the lock.
   with self._cache_lock:
    cached=self._cache
    if not force and cached is not None and cached['day']==day and cached['stamp']==stamp:return cached
   built=dict(self._snapshot(day),stamp=stamp)
   with self._cache_lock:
    self._cache=built;self._cache_at=time.monotonic()
   return built

 def _refresh(self,day):
  try:
   with self._build_lock:  # never scan beside a foreground build
    stamp=self._stamp();built=dict(self._snapshot(day),stamp=stamp)
    with self._cache_lock:self._cache=built;self._cache_at=time.monotonic()
  except Exception as error:  # a failed refresh keeps the previous aggregate serving
   LOG.warning('live detections: background refresh failed: %s',error)
  finally:
   with self._cache_lock:self._refreshing=False

 def _snapshot(self,day):
  empty=dict(day=day,stamp=None,available=False,counts={},timeframes={},states={},rows=0,live=0,symbols=0,
   as_of=None,scanned_at=None,detector_spec_hash=None,mixed_detector_identity=False,error=None)
  connection=self._connect()
  if connection is None:return empty
  try:
   # Grouped by DETECTION DAY too, so "spotted today" can be measured against the latest SESSION the ledger
   # holds for each timeframe rather than the calendar: after midnight IST, on a weekend or a holiday, the
   # calendar day has no candle at all and every card would read zero although nothing changed.
   rows=connection.execute(
    'select pattern_id,variant,side,timeframe,state,substr(detected_at,1,10) detected_day,count(*) n,'
    ' count(distinct symbol) symbols,max(last_seen) last_seen,max(as_of) as_of,max(detected_at) last_detected '
    'from detections group by pattern_id,variant,side,timeframe,state,detected_day').fetchall()
   hashes=[r[0] for r in connection.execute('select distinct detector_spec_hash from detections limit 4')]
   totals={s:n for s,n in connection.execute('select state,count(*) from detections group by state')}
   # Whatever lifecycle states the ledger ACTUALLY holds, read from the scanner's own index rather than
   # assumed here. `rows()` needs them to reach that index (see the comment there), and a state the scanner
   # adds later must widen the list rather than silently drop its detections.
   present=sorted(totals)
   symbols=int(connection.execute('select count(distinct symbol) from detections').fetchone()[0] or 0)
  except sqlite3.Error as error:
   LOG.warning('live detections: the ledger could not be aggregated: %s',error)
   self._forget()
   return dict(empty,error=f'{type(error).__name__}: {error}')
  counts={};frames={}
  # The latest session per timeframe = the day of the newest candle the scanner detected on for it.
  session={}
  for r in rows:
   d=str(r['as_of'] or '')[:10]
   if d and d>session.get(r['timeframe'],''):session[r['timeframe']]=d
  for r in rows:
   key=strategy_key(r['pattern_id'],r['variant'],r['side'],r['timeframe'])
   bucket=counts.get(key)
   if bucket is None:
    bucket=counts[key]=dict(strategy_key=key,pattern_id=r['pattern_id'],variant=r['variant'],side=r['side'],
     timeframe=r['timeframe'],detections=0,today=0,week=0,live=0,symbols=0,last_seen=None,as_of=None,last_detected=None,
     **{s:0 for s in STATES})
   n=int(r['n'] or 0);bucket['detections']+=n
   latest=session.get(r['timeframe']) or day
   if r['detected_day']==latest:bucket['today']+=n
   if r['detected_day'] and r['detected_day']>=days_before(latest,WEEK_DAYS-1):bucket['week']+=n
   if r['state'] in STATES:bucket[r['state']]+=n
   if r['state'] in LIVE_STATES:bucket['live']+=n
   # `symbols` is a per-(state) distinct count, so summing it would double-count a symbol that holds two
   # states of the same pattern. It is reported as the largest single-state count: a floor, never a total.
   bucket['symbols']=max(bucket['symbols'],int(r['symbols'] or 0))
   for field in ('last_seen','as_of','last_detected'):
    if r[field] and (bucket[field] is None or r[field]>bucket[field]):bucket[field]=r[field]
   frame=frames.setdefault(r['timeframe'],dict(timeframe=r['timeframe'],detections=0,live=0,last_seen=None,as_of=None,
    session=session.get(r['timeframe'])))
   frame['detections']+=n
   if r['state'] in LIVE_STATES:frame['live']+=n
   for field in ('last_seen','as_of'):
    if r[field] and (frame[field] is None or r[field]>frame[field]):frame[field]=r[field]
  return dict(day=day,stamp=None,available=True,counts=counts,timeframes=frames,present_states=present,
   states={s:int(totals.get(s,0)) for s in STATES},
   rows=sum(int(v) for v in totals.values()),live=sum(int(totals.get(s,0)) for s in LIVE_STATES),
   symbols=symbols,
   as_of=max([f['as_of'] for f in frames.values() if f['as_of']],default=None),
   scanned_at=max([f['last_seen'] for f in frames.values() if f['last_seen']],default=None),
   detector_spec_hash=hashes[0] if len(hashes)==1 else None,
   mixed_detector_identity=len(hashes)>1,error=None)

 def counts(self,today=None):return self.snapshot(today)['counts']

 def state(self,today=None):
  """The serving state of live detection itself, for the catalog header. Never claims what it cannot read."""
  snap=self.snapshot(today)
  return dict(available=bool(snap['available']),day=snap['day'],as_of=snap['as_of'],scanned_at=snap['scanned_at'],
   detections=snap['rows'],live=snap['live'],symbols=snap['symbols'],states=snap['states'],
   timeframes=snap['timeframes'],detector_spec_hash=snap['detector_spec_hash'],
   mixed_detector_identity=bool(snap['mixed_detector_identity']),
   ledger=str(self.path) if self.path else None,error=snap['error'])

 # --- one strategy's list ---------------------------------------------
 def rows(self,pattern_id,variant,side,timeframe,scope='today',limit=DEFAULT_ROW_LIMIT,offset=0,today=None,symbol=None):
  """`(rows, total)` for one strategy. Always bounded: `limit` is capped at ROW_LIMIT_MAX."""
  connection=self._connect()
  if connection is None:return [],0
  scope='live' if scope=='active' else scope if scope in SCOPES else DEFAULT_SCOPE
  day=today or today_ist()
  snapshot=self.snapshot(day)
  # The scanner's own index is `detections(state, timeframe, pattern_id)`. Without a predicate on `state` --
  # its LEADING column -- every list would be a full scan of a ~130 MB ledger. Constraining `state` to the
  # states the ledger actually holds (read from that same index in `_snapshot`) turns the scan into one seek
  # per state and changes no result: it is the set of values the column already has. A state the scanner adds
  # later appears in `present_states` on the next aggregate, so nothing is ever silently excluded.
  present=list(snapshot.get('present_states') or STATES)
  where=['state in ('+','.join('?'*len(present))+')','timeframe=?','pattern_id=?','variant=?','side=?']
  args=list(present)+[timeframe,pattern_id,variant,side]
  if scope=='live':where.append("state in ('forming','confirmed')")
  latest=((snapshot.get('timeframes') or {}).get(timeframe) or {}).get('session') or day
  if scope=='today':where.append('substr(detected_at,1,10)=?');args.append(latest)
  elif scope=='week':where.append('substr(detected_at,1,10)>=?');args.append(days_before(latest,WEEK_DAYS-1))
  if symbol:where.append('symbol=?');args.append(str(symbol).upper())
  clause=' and '.join(where)
  limit=max(1,min(int(limit or DEFAULT_ROW_LIMIT),ROW_LIMIT_MAX));offset=max(0,int(offset or 0))
  try:
   total=int(connection.execute(f'select count(*) from detections where {clause}',args).fetchone()[0] or 0)
   found=connection.execute(
    f'select * from detections where {clause} '
    # Newest detection first, so an older-but-active setup reads as older; the lifecycle breaks ties.
    "order by detected_at desc,case state when 'confirmed' then 0 when 'forming' then 1 when 'invalidated' then 2 else 3 end,"
    ' fit_score desc,symbol limit ? offset ?',args+[limit,offset]).fetchall()
  except sqlite3.Error as error:
   LOG.warning('live detections: the ledger could not be listed: %s',error)
   self._forget()
   return [],0
  frames=(snapshot.get('timeframes') or {}).get(timeframe) or {}
  newest=frames.get('last_seen')
  labels=self.labels()
  return [self._row(r,labels.get(r['symbol']) or {},newest,frames.get('session')) for r in found],total

 @staticmethod
 def _payload(raw):
  try:
   value=json.loads(raw) if raw else {}
  except (TypeError,ValueError):return {}
  return value if isinstance(value,dict) else {}

 def _row(self,r,label,newest_seen,session=None):
  """One ledger row as the app sees it. Nothing is derived that the scanner did not record."""
  payload=self._payload(r['payload'])
  geometry_note=payload.get('geometry_note') or ''
  return dict(
   detection_id=r['detection_id'],episode_id=r['episode_id'],
   symbol=r['symbol'],company=label.get('company') or r['symbol'],sector=label.get('sector'),
   timeframe=r['timeframe'],pattern_id=r['pattern_id'],variant=r['variant'],side=r['side'],family=r['family'],
   pattern_name=payload.get('pattern_name') or r['pattern_id'],
   state=r['state'],state_label=STATE_LABEL.get(r['state'],r['state']),state_reason=r['state_reason'],
   state_at=r['state_at'],detector_state=payload.get('detector_state'),
   live=r['state'] in LIVE_STATES,bars_since_state=payload.get('bars_since_state'),
   # Completed candles since the detector's signal bar, as of the pass that last saw it - the row's age.
   bars_since_signal=payload.get('bars_since_signal'),
   direction=r['direction'],fit_score=r['fit_score'],score=payload.get('score'),
   price=payload.get('price'),atr=r['atr'],
   formation_start=r['formation_start'],detected_at=r['detected_at'],signal_at=r['signal_at'],
   confirmed_at=r['confirmed_at'],as_of=r['as_of'],first_seen=r['first_seen'],last_seen=r['last_seen'],
   detected_day=_day(r['detected_at']),
   # First found on the newest session the ledger holds for this timeframe - the row's small "new" marker.
   # Measured against the SESSION, not the calendar day, so it does not flip off overnight or at a weekend.
   new=bool(session) and _day(r['detected_at'])==session,
   # "Still in the newest committed pass for this timeframe." A row that is not current is shown with its
   # own last_seen rather than hidden: the scanner may simply not have rescanned that timeframe yet.
   current=bool(newest_seen) and r['last_seen']==newest_seen,
   detector_spec_hash=r['detector_spec_hash'],live_rules_version=r['live_rules_version'],
   # `lines` are not in the ledger by design; the chart fetches them from the scanner's /api/chart.
   # A non-empty note means the detector published nothing drawable - the card says so instead of
   # rendering a silent blank (pattern_lines.build's lines/note exclusivity).
   geometry_note=geometry_note,drawable=not geometry_note,
   quality_tags=payload.get('quality_tags') or [],
   # The scanner's own match id for this cell. NOT unique across episodes - `detection_id` is.
   match_id=f"{r['symbol']}:{r['timeframe']}:{r['pattern_id']}:{r['variant']}:{r['side']}")

 def detection(self,detection_id):
  """One ledger row by its detection id, or None."""
  connection=self._connect()
  if connection is None or not detection_id:return None
  try:
   row=connection.execute('select * from detections where detection_id=?',(str(detection_id),)).fetchone()
  except sqlite3.Error as error:
   LOG.warning('live detections: the ledger could not be read: %s',error)
   self._forget();return None
  if not row:return None
  frames=self.snapshot()['timeframes'].get(row['timeframe']) or {}
  labels=self.labels()
  return self._row(row,labels.get(row['symbol']) or {},frames.get('last_seen'),frames.get('session'))
