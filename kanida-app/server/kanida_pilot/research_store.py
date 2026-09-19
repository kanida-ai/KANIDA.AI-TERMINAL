"""Read-only access to the expanded pattern-research artifacts (docs/pattern_research/*).

Boundary: `market_scanner/` is owned by the research workers. This module NEVER writes there and never
computes research. It opens `research.sqlite3` (table `cells`) and `outcomes.sqlite3` (tables
`cell_outcomes`, `bucket_outcomes`, engine v1.2.0) with `mode=ro` and `query_only`, and reads the frozen
`docs/pattern_research/IMPLEMENTED_CATALOGUE.json`.

Publication gate (EVIDENCE_SERVING_CONTRACT.md §9): eligibility is NEVER inferred from coverage or from
`tested_positive`. A research run is publishable only when an operator has recorded an explicit decision in
`var/evidence_release.json`. With no decision the run is `unreviewed` and every card carries the contract's
"Historical data requires review" label; with `withheld_source_quality_review` it is withheld. Either way the
existing trading gates are untouched: research cards are evidence, never a tradable-evidence verdict.
"""
from __future__ import annotations
import json,sqlite3,threading
from pathlib import Path
from .errors import PilotError

TIMEFRAMES=('1D','1H','4H','1W')
FAMILIES=('chart','candlestick','price_action','harmonic')
STATES=('confirmed','setup')  # preference order when a spec supports both
# Display grids per EVIDENCE_SERVING_CONTRACT / outcome engine `display_grid.declared`.
DISPLAY_GRID={'1H':(1,2,4,8,12,24),'4H':(1,2,3,5,8,10),'1D':(1,2,3,5,10),'1W':(1,2,4,8,12)}
BAR_WORD={'1H':'1H candles','4H':'4H candles','1D':'daily candles','1W':'weekly candles'}
COST_PCT=0.40
# Publication decisions an operator may record. Only `released` clears the review label.
PUBLICATION_STATUSES=('released','unreviewed','withheld_source_quality_review')
# Known from EVIDENCE_SERVING_CONTRACT.md §9; a file decision overrides these defaults.
KNOWN_PUBLICATION={'8ae6ddc251e80668239e':'withheld_source_quality_review'}

def _spec_key(pattern_id,variant,side):return f'{pattern_id}:{variant}:{side}'
def strategy_key(pattern_id,variant,side,timeframe):return f'{pattern_id}-{variant}-{side}-{timeframe}'.lower()

class Catalogue:
 """The frozen 107-entry / 262-combination catalogue. Names and descriptions come from here, never invented."""
 def __init__(self,path):
  self.path=Path(path);self.specs=[];self.by_key={};self.digest=None;self.error=None
  try:
   data=json.loads(self.path.read_text(encoding='utf-8'))
  except (OSError,ValueError) as error:
   self.error=f'{type(error).__name__}: {error}';return
  self.run=data.get('run');self.entries=data.get('catalogue_entries');self.combinations=data.get('directional_variant_combinations')
  for spec in data.get('specifications') or []:
   item=dict(pattern_id=spec['pattern_id'],variant=spec['variant'],side=spec['side'],name=spec.get('name') or spec['pattern_id'],
    family=spec.get('family') or 'chart',definition_version=spec.get('definition_version'),states=tuple(spec.get('states') or ()),
    interpretation=spec.get('interpretation'),lookback=spec.get('lookback'),definition=spec.get('definition') or '')
   item['state']=next((s for s in STATES if s in item['states']),item['states'][0] if item['states'] else 'confirmed')
   self.specs.append(item);self.by_key[_spec_key(*(item[k] for k in ('pattern_id','variant','side')))]=item
 def spec(self,pattern_id,variant,side):return self.by_key.get(_spec_key(pattern_id,variant,side))
 def spec_ids(self,pattern_id):
  """The specs registered under one catalogue pattern id (empty when it is not a researched pattern)."""
  return [s for s in self.specs if s['pattern_id']==pattern_id]
 def by_family(self,family):return [s for s in self.specs if s['family']==family]

def _connect(path):
 """Read-only, query-only connection. The research writer may hold the WAL; readers never block it."""
 connection=sqlite3.connect(f'file:{Path(path).as_posix()}?mode=ro',uri=True,timeout=5.0,check_same_thread=False)
 connection.row_factory=sqlite3.Row
 connection.execute('PRAGMA query_only=1')
 return connection

class ResearchStore:
 """Opens the two research databases lazily and per thread; a missing or busy database degrades, never crashes."""
 def __init__(self,directory,research_run,catalogue_path,release_path=None):
  self.directory=Path(directory);self.research_run=research_run
  self.catalogue=Catalogue(catalogue_path)
  self.release_path=Path(release_path) if release_path else None
  self._local=threading.local()
 @property
 def research_db(self):return self.directory/'research.sqlite3'
 @property
 def outcomes_db(self):return self.directory/'outcomes.sqlite3'
 def available(self):return self.research_db.is_file() and bool(self.catalogue.specs)

 def connect(self,which):
  cache=getattr(self._local,'connections',None)
  if cache is None:cache=self._local.connections={}
  if which not in cache:
   path=self.research_db if which=='research' else self.outcomes_db
   if not path.is_file():raise PilotError(503,'RESEARCH_UNAVAILABLE','Pattern research is not available on this server.')
   try:cache[which]=_connect(path)
   except sqlite3.Error as error:raise PilotError(503,'RESEARCH_UNAVAILABLE',f'Pattern research could not be opened: {error}') from None
  return cache[which]
 def close(self):
  for connection in (getattr(self._local,'connections',None) or {}).values():
   try:connection.close()
   except sqlite3.Error:pass
  self._local.connections={}

 def outcome_run(self):
  """The newest outcome run for our research run: a completed run wins over a running one, newest first."""
  try:rows=self.connect('outcomes').execute(
    'select id,research_run,source_run,snapshot_id,engine_version,status,created_at from outcome_runs where research_run=? order by created_at desc',
    (self.research_run,)).fetchall()
  except (PilotError,sqlite3.Error):return None
  if not rows:return None
  chosen=next((r for r in rows if r['status']=='complete'),rows[0])
  return dict(chosen)

 def publication(self,run=None):
  """§9: an explicit operator decision, or `unreviewed`. Never inferred from the data."""
  run=run or self.research_run
  decisions={}
  if self.release_path and self.release_path.is_file():
   try:
    data=json.loads(self.release_path.read_text(encoding='utf-8'))
    decisions={str(k):v for k,v in (data.get('runs') or {}).items()}
   except (OSError,ValueError):decisions={}
  entry=decisions.get(run) or {}
  status=entry.get('publication_status') if isinstance(entry,dict) else entry
  if status not in PUBLICATION_STATUSES:status=KNOWN_PUBLICATION.get(run,'unreviewed')
  source=entry.get('source_quality_status') if isinstance(entry,dict) else None
  return dict(run=run,publication_status=status,
   source_quality_status=source or ('resolved' if status=='released' else 'unresolved'),
   decided_by=(entry.get('decided_by') if isinstance(entry,dict) else None),
   decided_at=(entry.get('decided_at') if isinstance(entry,dict) else None),
   released=status=='released')

 # --- single-row reads: the card is served from precomputed rows, never recomputed ---
 def cell_outcome(self,run,symbol,timeframe,pattern_id,variant,side,state):
  row=self.connect('outcomes').execute(
   'select * from cell_outcomes where run=? and symbol=? and timeframe=? and pattern_id=? and variant=? and side=? and state=?',
   (run,symbol,timeframe,pattern_id,variant,side,state)).fetchone()
  return dict(row) if row else None
 def buckets(self,run,symbol,timeframe,pattern_id,variant,side,state):
  rows=self.connect('outcomes').execute(
   'select dimension,bucket,n,sample_label,status,win_rate_pct,mean_net_return_pct,median_net_return_pct,median_mfe_pct,median_mae_pct,'
   'baseline_scope,baseline_n,baseline_mean_net_return_pct,diff_mean_net_return_pct,p_value,q_value,discovery_q10,horizon '
   'from bucket_outcomes where run=? and symbol=? and timeframe=? and pattern_id=? and variant=? and side=? and state=? '
   'order by dimension,bucket',(run,symbol,timeframe,pattern_id,variant,side,state)).fetchall()
  return [dict(r) for r in rows]
 def studied_symbols(self,symbols,timeframe,pattern_id,variant,side):
  """Which of `symbols` this run actually STUDIED for one cell identity.

  The live scanner's universe is wider than the research run's, so a detection can be perfectly valid on a
  stock the run never covered. That is "never studied", not "not enough history". Read from `cells`, which is
  the source of truth: it holds every attempted cell INCLUDING the ones that had no occurrences, which the
  Discover index deliberately does not store as rows. An unreadable research database yields an empty set, so
  every row reads as unstudied rather than silently claiming evidence that cannot be shown.
  """
  symbols=[s for s in dict.fromkeys(symbols or []) if s]
  if not symbols:return set()
  try:
   rows=self.connect('research').execute(
    'select symbol from cells where run=? and timeframe=? and pattern_id=? and variant=? and side=? and symbol in ('
    +','.join('?'*len(symbols))+')',[self.research_run,timeframe,pattern_id,variant,side]+symbols).fetchall()
  except (PilotError,sqlite3.Error):return set()
  return {r['symbol'] for r in rows}
 def research_cell(self,symbol,timeframe,pattern_id,variant,side):
  row=self.connect('research').execute(
   'select status,occurrences,reference_n,wf_n,reference_mean,wf_mean from cells where run=? and symbol=? and timeframe=? and pattern_id=? and variant=? and side=?',
   (self.research_run,symbol,timeframe,pattern_id,variant,side)).fetchone()
  return dict(row) if row else None
 def outcome_symbols(self,run):
  try:rows=self.connect('outcomes').execute('select symbol,status from outcome_symbols where run=?',(run,)).fetchall()
  except (PilotError,sqlite3.Error):return {}
  return {r['symbol']:r['status'] for r in rows}
