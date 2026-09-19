"""Precomputed Discover index over the pattern-research artifacts.

Why: the catalog serves 262 direction/variant combinations x 4 timeframes ~= 1,048 strategies, over ~519k
research cells and a multi-gigabyte outcome database. Scanning those on a page request is not an option, so
this module builds a compact SQLite index (`var/research_index.sqlite3`) of per-strategy counts and per-symbol
rows, and the request path only ever reads it.

Refresh is by schedule or on research/outcome-run change, always on a background thread; a request never
triggers or waits for a build. While a build runs the previous index keeps serving.

Every stored number is copied from a research column. Nothing is modelled or back-filled here: the ranking key
`edge_low_pct` is the engine's own `baseline_diff_ci95_low` (the 95% low of this pattern's mean net return
MINUS entering the same stock on any eligible bar, same window, horizon and cost model). Where the engine
stores no value the column stays NULL and the row sorts last - it never receives a substitute.

Read-only on `market_scanner/` (ResearchStore enforces `mode=ro`). Nothing here recomputes research.
"""
from __future__ import annotations
import logging,os,re,socket,sqlite3,threading,time
from datetime import datetime,timezone
from pathlib import Path
from .cards import TIER_LIMITED,TIER_RESULT,WALKFORWARD_MIN,evidence_state,tier_for
from .research_store import TIMEFRAMES,strategy_key

LOG=logging.getLogger('pilot.research_index')

SCHEMA="""
CREATE TABLE IF NOT EXISTS index_meta(key TEXT PRIMARY KEY,value TEXT);
CREATE TABLE IF NOT EXISTS strategy_stats(
 strategy_key TEXT PRIMARY KEY,pattern_id TEXT,variant TEXT,side TEXT,timeframe TEXT,family TEXT,state TEXT,
 cells INTEGER DEFAULT 0,cells_with_evidence INTEGER DEFAULT 0,cells_tested INTEGER DEFAULT 0,
 cells_no_occurrences INTEGER DEFAULT 0,cells_loading INTEGER DEFAULT 0,listed INTEGER DEFAULT 0,
 tier_result INTEGER DEFAULT 0,tier_limited INTEGER DEFAULT 0,tier_history INTEGER DEFAULT 0,
 occurrences INTEGER DEFAULT 0,
 found INTEGER DEFAULT 0,found_week INTEGER DEFAULT 0,best_edge_low_pct REAL,best_edge_symbol TEXT,
 beats_baseline INTEGER DEFAULT 0,last_seen TEXT);
CREATE TABLE IF NOT EXISTS strategy_rows(
 strategy_key TEXT NOT NULL,symbol TEXT NOT NULL,company TEXT,research_status TEXT,selection_status TEXT,
 evidence_state TEXT,has_evidence INTEGER DEFAULT 0,evidence_tier INTEGER DEFAULT 4,
 occurrences INTEGER,oos_n INTEGER,oos_expectancy_pct REAL,oos_win_rate_pct REAL,
 edge_mean_pct REAL,edge_low_pct REAL,edge_high_pct REAL,beats_baseline INTEGER,
 baseline_mean_net_return_pct REAL,p_target_first REAL,p_stop_first REAL,selected_horizon INTEGER,
 peak_net_return_pct REAL,q_value REAL,p_value REAL,sector TEXT,market_cap_tier TEXT,last_seen TEXT,
 found INTEGER DEFAULT 0,
 PRIMARY KEY(strategy_key,symbol));
CREATE INDEX IF NOT EXISTS strategy_rows_rank ON strategy_rows(strategy_key,evidence_tier,edge_low_pct DESC);
"""
# Company and sector for the researched universe, read-only from the shared instrument catalogue. The stored-scan
# path reads the same table (market_scanner/data.py `universe`), so research rows read like stored-scan rows.
LABEL_SQL=("select symbol,COALESCE(company,company_name,symbol) as company,COALESCE(sector,'Unclassified') as sector "
 "from instrument_labels where exchange='NSE' and instrument_type in ('STOCK','EQ') and is_active=1")
# Bump whenever the index columns change. The index is a derived cache, never a source of truth, so a version
# change simply drops and rebuilds it - `CREATE TABLE IF NOT EXISTS` alone would leave an older file short of the
# new columns and every read would fail on a missing key.
SCHEMA_VERSION='5'
DATE=re.compile(r'"entry_date"\s*:\s*"(\d{4}-\d{2}-\d{2})"')
BUILD_LOCK=threading.Lock()  # in-process only; the cross-process lease below is what actually protects the file
# Several processes can share one index file (the pilot on 8082 and the UI-QA instance on 8083 both default to
# var/research_index.sqlite3). A per-process lock cannot stop two of them rebuilding into the same file and
# deleting each other's rows, so the build takes a LEASE recorded inside the index itself. A lease older than
# LEASE_SECONDS is assumed dead (the process was killed mid-build) and is stolen.
LEASE_SECONDS=1800
DEFAULT_REFRESH_SECONDS=1800
BATCH=20000
ROW_FIELDS=('strategy_key','symbol','company','research_status','selection_status','evidence_state','has_evidence',
 'evidence_tier',
 'occurrences','oos_n','oos_expectancy_pct','oos_win_rate_pct','edge_mean_pct','edge_low_pct','edge_high_pct',
 'beats_baseline','baseline_mean_net_return_pct','p_target_first','p_stop_first','selected_horizon',
 'peak_net_return_pct','q_value','p_value','sector','market_cap_tier','last_seen','found')
STAT_FIELDS=('strategy_key','pattern_id','variant','side','timeframe','family','state','cells','cells_with_evidence',
 'cells_tested','cells_no_occurrences','cells_loading','listed','tier_result','tier_limited','tier_history',
 'occurrences','found','found_week','best_edge_low_pct',
 'best_edge_symbol','beats_baseline','last_seen')

def _days(a,b):
 try:return abs((datetime.strptime(a,'%Y-%m-%d')-datetime.strptime(b,'%Y-%m-%d')).days)
 except (TypeError,ValueError):return 10**6
def _last_date(raw):
 """Newest occurrence date, read straight out of the stored JSON text (cheaper than parsing 500k blobs)."""
 if not raw:return None
 found=DATE.findall(raw if isinstance(raw,str) else str(raw))
 return max(found) if found else None
def _parse_latest(value):
 """"1D=2026-09-15 1H=2026-09-15" -> {'1D':'2026-09-15','1H':'2026-09-15'}. Unreadable entries are dropped."""
 out={}
 for part in str(value or '').split():
  timeframe,_,day=part.partition('=')
  if timeframe in TIMEFRAMES and re.fullmatch(r'\d{4}-\d{2}-\d{2}',day):out[timeframe]=day
 return out
def _blank_row(key,symbol,label=None):
 return dict.fromkeys(ROW_FIELDS)|dict(strategy_key=key,symbol=symbol,occurrences=0,found=0,has_evidence=0,evidence_tier=4,
  company=(label or {}).get('company') or symbol,sector=(label or {}).get('sector'))

class ResearchIndex:
 def __init__(self,store,path,refresh_seconds=DEFAULT_REFRESH_SECONDS,labels_database=None):
  self.store=store;self.path=Path(path);self.refresh_seconds=int(refresh_seconds)
  self.labels_database=Path(labels_database) if labels_database else None
  self._local=threading.local();self._building=False;self._last_error=None
  self.path.parent.mkdir(parents=True,exist_ok=True)
  connection=self._write()
  try:
   with connection:
    version=None
    try:
     row=connection.execute("select value from index_meta where key='schema_version'").fetchone()
     version=row['value'] if row else None
    except sqlite3.Error:version=None
    if version!=SCHEMA_VERSION:
     # An index built by an older version is discarded wholesale rather than patched: it is a derived cache.
     connection.execute('drop table if exists strategy_stats');connection.execute('drop table if exists strategy_rows')
     connection.execute('drop table if exists index_meta')
    connection.executescript(SCHEMA)
    connection.execute('insert into index_meta values(?,?) on conflict(key) do update set value=excluded.value',
     ('schema_version',SCHEMA_VERSION))
  finally:connection.close()

 # --- connections -------------------------------------------------------
 def _write(self):
  connection=sqlite3.connect(self.path,timeout=30.0);connection.row_factory=sqlite3.Row
  connection.execute('PRAGMA journal_mode=WAL');connection.execute('PRAGMA synchronous=NORMAL')
  return connection
 def read(self):
  connection=getattr(self._local,'read',None)
  if connection is None:
   connection=sqlite3.connect(f'file:{self.path.as_posix()}?mode=ro',uri=True,timeout=5.0,check_same_thread=False)
   connection.row_factory=sqlite3.Row;self._local.read=connection
  return connection
 def _meta(self):
  try:return {r['key']:r['value'] for r in self.read().execute('select key,value from index_meta')}
  except sqlite3.Error:return {}
 def _populated(self):
  """Does the index actually hold rows? Metadata alone is not proof: a migration drops the tables and leaves a
  schema-only file behind, which must always be treated as needing a build."""
  try:return int(self.read().execute('select count(*) from strategy_stats').fetchone()[0])>0
  except sqlite3.Error:return False

 # --- cross-process build lease ----------------------------------------
 @staticmethod
 def _owner():return f'{socket.gethostname()}:{os.getpid()}'
 def _take_lease(self,force=False):
  """Atomically claim the right to build. Returns True only for the one caller that wins."""
  connection=self._write();connection.isolation_level=None
  try:
   connection.execute('BEGIN IMMEDIATE')
   row=connection.execute("select value from index_meta where key='build_lease'").fetchone()
   now=time.time()
   if row and not force:
    try:owner,started=str(row['value']).rsplit('@',1);started=float(started)
    except ValueError:owner,started='unknown',0.0
    if now-started<LEASE_SECONDS:
     connection.execute('ROLLBACK')
     LOG.info('research index: a build is already running elsewhere (%s); skipping',owner)
     return False
    LOG.warning('research index: stealing a build lease abandoned by %s',owner)
   connection.execute('insert into index_meta values(?,?) on conflict(key) do update set value=excluded.value',
    ('build_lease',f'{self._owner()}@{now:.0f}'))
   connection.execute('COMMIT')
   return True
  except sqlite3.Error as error:
   LOG.warning('research index: could not take the build lease: %s',error)
   try:connection.execute('ROLLBACK')
   except sqlite3.Error:pass
   return False
  finally:connection.close()
 def _release_lease(self):
  try:
   connection=self._write()
   try:
    with connection:connection.execute("delete from index_meta where key='build_lease'")
   finally:connection.close()
  except sqlite3.Error:pass
 def labels(self):
  """symbol -> {company, sector} from the shared instrument catalogue, read-only. Missing or unreadable labels
  are not an error: the row falls back to its symbol, exactly as it did before."""
  if not self.labels_database or not self.labels_database.is_file():return {}
  try:
   connection=sqlite3.connect(f'file:{self.labels_database.as_posix()}?mode=ro',uri=True,timeout=30.0)
   connection.row_factory=sqlite3.Row;connection.execute('PRAGMA query_only=1')
   try:return {r['symbol']:dict(company=r['company'],sector=r['sector']) for r in connection.execute(LABEL_SQL)}
   finally:connection.close()
  except sqlite3.Error:return {}

 # --- state / refresh ---------------------------------------------------
 def state(self):
  meta=self._meta();outcome=self.store.outcome_run() or {};built=meta.get('built_at');age=None
  if built:
   try:age=int(time.time()-datetime.fromisoformat(built).timestamp())
   except ValueError:age=None
  current=outcome.get('id');status=outcome.get('status');populated=self._populated()
  # An index with no rows is ALWAYS stale, whatever its metadata says. A schema migration drops the tables and
  # leaves a file holding only `schema_version`; without this the staleness comparison could be satisfied by
  # leftover metadata and nothing would ever rebuild.
  # A finishing outcome run keeps the SAME id and only changes status, so the status must be part of staleness:
  # otherwise a completed run would wait for the refresh timer before its last symbols appeared.
  stale=(not populated or not meta or meta.get('research_run')!=self.store.research_run
   or meta.get('outcome_run')!=(current or '') or meta.get('outcome_status')!=(status or '')
   or (age is not None and age>self.refresh_seconds))
  lease=meta.get('build_lease')
  return dict(built_at=built,age_seconds=age,building=self._building,stale=bool(stale),error=self._last_error,
   populated=populated,build_lease=lease,
   research_run=meta.get('research_run'),outcome_run=meta.get('outcome_run') or None,
   indexed_outcome_status=meta.get('outcome_status') or None,
   current_outcome_run=current,current_outcome_status=status,
   source_run=meta.get('source_run') or None,snapshot_id=meta.get('snapshot_id') or None,
   engine_version=meta.get('engine_version') or None,
   strategies=int(meta.get('strategies') or 0),rows=int(meta.get('rows') or 0),
   symbols_research=int(meta.get('symbols_research') or 0),symbols_with_evidence=int(meta.get('symbols_with_evidence') or 0),
   evidence_coverage_pct=float(meta.get('evidence_coverage_pct') or 0.0),
   build_seconds=float(meta.get('build_seconds') or 0.0),latest_seen=meta.get('latest_seen') or None,
   # The research run's own data end, per timeframe. A card needs ONE of these (its own timeframe) so it can
   # name the evidence date beside the live detection time instead of falling back to "unknown age".
   latest_by_timeframe=_parse_latest(meta.get('latest_seen')),
   publication=self.store.publication())
 def ensure(self):
  """Kick a background rebuild when the index is stale. Never blocks and never runs two builds at once."""
  if self._building:return False
  if not self.store.available():
   LOG.warning('research index: the research tree is unavailable, so no build was started');return False
  try:
   if not self.state()['stale']:return False
  except Exception as error:
   LOG.warning('research index: could not read the index state (%s); building anyway',error)
  threading.Thread(target=self.build,name='research-index',daemon=True).start();return True

 # --- build -------------------------------------------------------------
 def build(self,force=False):
  """Rebuild the index. Safe to call from anywhere: the in-process lock stops two threads, and the lease in the
  index file stops two PROCESSES sharing one file from deleting each other's rows."""
  if not BUILD_LOCK.acquire(blocking=False):
   LOG.info('research index: this process is already building; skipping');return False
  if not self._take_lease(force):BUILD_LOCK.release();return False
  started=time.time();self._building=True;self._last_error=None;ok=False
  LOG.info('research index: build started (%s)',self.path)
  try:
   self._build();ok=True
   LOG.info('research index: build finished in %.0fs',time.time()-started)
  except Exception as error:  # a failed build must never take the serving index or the server down
   self._last_error=f'{type(error).__name__}: {error}'
   LOG.error('research index: build FAILED after %.0fs: %s',time.time()-started,self._last_error)
  finally:
   self._release_lease();self._building=False;BUILD_LOCK.release()
  if ok:
   try:
    connection=self._write()
    with connection:connection.execute(
     'insert into index_meta values(?,?) on conflict(key) do update set value=excluded.value',
     ('build_seconds',f'{time.time()-started:.1f}'))
    connection.close();self._local.read=None
   except sqlite3.Error:pass
  return ok

 def _build(self):
  store=self.store;run=store.research_run;outcome=store.outcome_run() or {};outcome_run=outcome.get('id')
  specs={(s['pattern_id'],s['variant'],s['side']):s for s in store.catalogue.specs}
  stats={}
  for spec in store.catalogue.specs:
   for tf in TIMEFRAMES:
    key=strategy_key(spec['pattern_id'],spec['variant'],spec['side'],tf)
    stats[key]=dict(dict.fromkeys(STAT_FIELDS,0),strategy_key=key,pattern_id=spec['pattern_id'],variant=spec['variant'],
     side=spec['side'],timeframe=tf,family=spec['family'],state=spec['state'],best_edge_low_pct=None,
     best_edge_symbol=None,last_seen=None)
  rows={};symbols=set();labels=self.labels()
  # 1) every attempted research cell: status + occurrences. Cells that never occurred are counted, not stored.
  cursor=store.connect('research').execute(
   'select symbol,timeframe,pattern_id,variant,side,status,occurrences from cells where run=?',(run,))
  while True:
   batch=cursor.fetchmany(BATCH)
   if not batch:break
   for r in batch:
    if (r['pattern_id'],r['variant'],r['side']) not in specs or r['timeframe'] not in TIMEFRAMES:continue
    key=strategy_key(r['pattern_id'],r['variant'],r['side'],r['timeframe']);bucket=stats.get(key)
    if bucket is None:continue
    symbols.add(r['symbol']);bucket['cells']+=1;bucket['occurrences']+=int(r['occurrences'] or 0)
    if r['status']=='no_occurrences':bucket['cells_no_occurrences']+=1;continue
    row=_blank_row(key,r['symbol'],labels.get(r['symbol']))
    row.update(research_status=r['status'],occurrences=int(r['occurrences'] or 0))
    rows[(key,r['symbol'])]=row
  # 2) the outcome evidence, where the engine has reached. `summary` is deliberately NOT selected (it is large
  #    and only the card needs it); every value below is an engine column copied as-is.
  evidence_symbols=set()
  if outcome_run:
   cursor=store.connect('outcomes').execute(
    'select symbol,timeframe,pattern_id,variant,side,state,selection_status,occurrences,oos_n,oos_expectancy_pct,'
    'oos_win_rate_pct,baseline_diff_mean_net_return_pct,baseline_diff_ci95_low,baseline_diff_ci95_high,'
    'baseline_beats_unconditional,baseline_mean_net_return_pct,p_target_first,p_stop_first,selected_horizon,'
    'oos_q_value,oos_p_value,sector,market_cap_tier,last_occurrences,return_peak_net_return_pct '
    'from cell_outcomes where run=?',(outcome_run,))
   while True:
    batch=cursor.fetchmany(BATCH)
    if not batch:break
    for r in batch:
     spec=specs.get((r['pattern_id'],r['variant'],r['side']))
     if not spec or r['timeframe'] not in TIMEFRAMES or r['state']!=spec['state']:continue
     key=strategy_key(r['pattern_id'],r['variant'],r['side'],r['timeframe']);bucket=stats.get(key)
     if bucket is None:continue
     evidence_symbols.add(r['symbol'])
     row=rows.get((key,r['symbol']))
     if row is None:row=rows[(key,r['symbol'])]=_blank_row(key,r['symbol'],labels.get(r['symbol']))
     row.update(has_evidence=1,selection_status=r['selection_status'],oos_n=r['oos_n'],oos_expectancy_pct=r['oos_expectancy_pct'],
      oos_win_rate_pct=r['oos_win_rate_pct'],edge_mean_pct=r['baseline_diff_mean_net_return_pct'],
      edge_low_pct=r['baseline_diff_ci95_low'],edge_high_pct=r['baseline_diff_ci95_high'],
      beats_baseline=r['baseline_beats_unconditional'],baseline_mean_net_return_pct=r['baseline_mean_net_return_pct'],
      p_target_first=r['p_target_first'],p_stop_first=r['p_stop_first'],selected_horizon=r['selected_horizon'],
      q_value=r['oos_q_value'],p_value=r['oos_p_value'],market_cap_tier=r['market_cap_tier'],
      # The shared instrument catalogue wins; the engine's own label is only a fallback.
      sector=row['sector'] or r['sector'],
      peak_net_return_pct=r['return_peak_net_return_pct'],last_seen=_last_date(r['last_occurrences']),
      occurrences=int(r['occurrences'] or row['occurrences'] or 0))
     bucket['cells_with_evidence']+=1
     if r['selection_status']=='tested':bucket['cells_tested']+=1
     if r['baseline_beats_unconditional']:bucket['beats_baseline']+=1
  # 3) derive the evidence state, recency and the per-strategy aggregates.
  latest={tf:None for tf in TIMEFRAMES}
  for row in rows.values():
   tf=stats[row['strategy_key']]['timeframe']
   if row['last_seen'] and (latest[tf] is None or row['last_seen']>latest[tf]):latest[tf]=row['last_seen']
  for (key,symbol),row in rows.items():
   bucket=stats[key];tf=bucket['timeframe']
   row['evidence_state']=evidence_state(row['research_status'],row['selection_status'],row['oos_n'],
    row['selection_status'] is not None)
   # Rank on how usable the evidence is, not on whether an outcome row exists: most cells are measured but
   # carry no accepted walk-forward selection, and those must not head the list.
   row['evidence_tier']=tier=tier_for(row['evidence_state'])
   if tier==TIER_RESULT:bucket['tier_result']+=1
   elif tier==TIER_LIMITED:bucket['tier_limited']+=1
   else:bucket['tier_history']+=1
   if row['evidence_state']=='loading':bucket['cells_loading']+=1
   if row['last_seen']:
    top=latest[tf];row['found']=1 if top and row['last_seen']==top else 0
    if top and _days(row['last_seen'],top)<=7:bucket['found_week']+=1
    if not bucket['last_seen'] or row['last_seen']>bucket['last_seen']:bucket['last_seen']=row['last_seen']
   bucket['found']+=row['found'];bucket['listed']+=1  # exactly the rows this strategy lists
   # Rank only on cells whose walk-forward sample meets the contract minimum; a thin cell never tops the list.
   low=row['edge_low_pct'] if (row['oos_n'] or 0)>=WALKFORWARD_MIN else None
   if low is not None and (bucket['best_edge_low_pct'] is None or low>bucket['best_edge_low_pct']):
    bucket['best_edge_low_pct']=low;bucket['best_edge_symbol']=symbol
  # 4) swap in one transaction. Refuse to replace a populated index with an empty one: the research databases are
  #    being written concurrently, so a transient read failure must leave the previous index serving rather than
  #    blank the page with numbers that look like "we looked and found nothing".
  if not rows and int(self._meta().get('rows') or 0)>0:
   raise RuntimeError('refusing to replace the serving index with an empty build (the research read returned no cells)')
  connection=self._write()
  try:
   with connection:
    connection.executescript(SCHEMA)
    connection.execute('delete from strategy_stats');connection.execute('delete from strategy_rows')
    connection.executemany('insert into strategy_stats('+','.join(STAT_FIELDS)+') values('+
     ','.join(':'+f for f in STAT_FIELDS)+')',list(stats.values()))
    connection.executemany('insert into strategy_rows('+','.join(ROW_FIELDS)+') values('+
     ','.join(':'+f for f in ROW_FIELDS)+')',list(rows.values()))
    meta=dict(research_run=run,outcome_run=outcome_run or '',source_run=outcome.get('source_run') or '',
     snapshot_id=outcome.get('snapshot_id') or '',engine_version=outcome.get('engine_version') or '',
     outcome_status=outcome.get('status') or '',built_at=datetime.now(timezone.utc).isoformat(),
     strategies=len(stats),rows=len(rows),symbols_research=len(symbols),symbols_with_evidence=len(evidence_symbols),
     evidence_coverage_pct=f'{(100.0*len(evidence_symbols)/len(symbols)) if symbols else 0.0:.1f}',
     latest_seen=' '.join(f'{tf}={latest[tf]}' for tf in TIMEFRAMES if latest[tf]))
    connection.executemany('insert into index_meta values(?,?) on conflict(key) do update set value=excluded.value',
     [(k,str(v)) for k,v in meta.items()])
  finally:connection.close()
  self._local.read=None  # reopen so this thread's reader sees the swapped content

 # --- serving -----------------------------------------------------------
 def stats(self,keys=None):
  """All 1,048 rows, or a named subset. One indexed read; the catalog never touches the research databases."""
  query='select * from strategy_stats';args=()
  if keys is not None:
   keys=list(keys)
   if not keys:return {}
   query+=' where strategy_key in ('+','.join('?'*len(keys))+')';args=tuple(keys)
  try:return {r['strategy_key']:dict(r) for r in self.read().execute(query,args)}
  except sqlite3.Error:return {}  # a missing index yields no stats; callers say "evidence loading", never zero
 def rows(self,key,limit=None,offset=0,search=None):
  # Rows that HAVE evidence rank above rows the outcome run has not reached; the rest of the order is unchanged.
  # Primary key is the evidence tier (usable evidence first). Inside a tier the edge ordering is unchanged:
  # edge 95% low, then the out-of-sample mean, then the walk-forward sample size, then the symbol.
  query=('select * from strategy_rows where strategy_key=?'+(' and (symbol like ? or upper(company) like ?)' if search else '')+
   ' order by evidence_tier,edge_low_pct is null,edge_low_pct desc,'
   ' oos_expectancy_pct is null,oos_expectancy_pct desc,oos_n desc,occurrences desc,symbol')
  args=[key]+([f'%{search.upper()}%']*2 if search else [])
  if limit:query+=' limit ? offset ?';args+=[int(limit),int(offset)]
  try:return [dict(r) for r in self.read().execute(query,args)]
  except sqlite3.Error:return []
 def row_count(self,key,search=None):
  try:
   return int(self.read().execute('select count(*) from strategy_rows where strategy_key=?'+
    (' and (symbol like ? or upper(company) like ?)' if search else ''),
    [key]+([f'%{search.upper()}%']*2 if search else [])).fetchone()[0])
  except sqlite3.Error:return 0
 def coverage(self,key):
  """Per-strategy evidence coverage for the card: how many of this strategy's stocks already have an outcome row."""
  try:
   r=self.read().execute('select sum(has_evidence) ready,count(*) total,'
    ' sum(evidence_tier=0) tier_result,sum(evidence_tier=1) tier_limited,sum(evidence_tier>1) tier_history'
    ' from strategy_rows where strategy_key=?',(key,)).fetchone()
   return dict(ready=int(r['ready'] or 0),total=int(r['total'] or 0),tier_result=int(r['tier_result'] or 0),
    tier_limited=int(r['tier_limited'] or 0),tier_history=int(r['tier_history'] or 0))
  except sqlite3.Error:return dict(ready=0,total=0,tier_result=0,tier_limited=0,tier_history=0)


# --- command line -----------------------------------------------------------
# `python -m kanida_pilot.research_index --build` rebuilds the index without the app, using the same settings the
# server uses. Run it from the `server` directory (or with `server` on PYTHONPATH). Useful when the app is stuck
# behind a stale or half-built index, and for a scheduled refresh.
def from_settings(settings=None):
 """Build the store + index pair the server uses, straight from Settings."""
 from .config import Settings
 from .research_store import ResearchStore
 settings=settings or Settings.load()
 store=ResearchStore(settings.pattern_research_directory,settings.pattern_research_run,
  settings.pattern_catalogue_path,settings.evidence_release_path)
 if not store.available():
  raise SystemExit(f'Pattern research is not available at {settings.pattern_research_directory} '
   f'(catalogue: {settings.pattern_catalogue_path}).')
 return store,ResearchIndex(store,settings.pattern_index_path,settings.pattern_index_refresh_seconds,
  settings.pattern_labels_database)

def main(argv=None):
 import argparse,json
 parser=argparse.ArgumentParser(prog='python -m kanida_pilot.research_index',
  description='Build or inspect the precomputed Discover research index.')
 parser.add_argument('--build',action='store_true',help='rebuild the index now (blocks until finished)')
 parser.add_argument('--force',action='store_true',help='with --build, steal a build lease held by another process')
 parser.add_argument('--state',action='store_true',help='print the index state as JSON and exit')
 parser.add_argument('--quiet',action='store_true',help='log warnings and errors only')
 args=parser.parse_args(argv)
 logging.basicConfig(level=logging.WARNING if args.quiet else logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
 store,index=from_settings()
 if args.state or not args.build:
  print(json.dumps(index.state(),indent=1,default=str))
  if not args.build:return 0
 started=time.time()
 ok=index.build(force=args.force)
 state=index.state()
 if not ok:
  print(f'Build did not run: {state.get("error") or "another process holds the build lease (use --force)"}')
  return 1
 print(f'Built in {time.time()-started:.0f}s: {state["strategies"]:,} strategies, {state["rows"]:,} rows, '
  f'evidence for {state["symbols_with_evidence"]:,} of {state["symbols_research"]:,} stocks '
  f'({state["evidence_coverage_pct"]:.1f}%).')
 return 0

if __name__=='__main__':
 raise SystemExit(main())
