"""The Derivative tab's reader: `db/derivatives.db`, READ-ONLY (docs/DERIVATIVES_SPEC.md §2-§4).

Boundary
--------
This module **never writes, never captures and never computes a signal**. The capture worker (D1) writes
`contracts`/`snapshots`/`candles_15m`/`underlying_snapshots`; the metrics worker (D2) writes `metrics` - the
§3 signals. Everything here is a SELECT plus presentation (rounding, ₹ crore, a sort, a group-by for the §3
roll-up). The connection is opened `mode=ro` with `PRAGMA query_only`, exactly like `detections.py`.

Where the numbers come from
---------------------------
`market_data/derivatives/metrics.py` (D2) owns the definitions. When that module is importable and exposes a
reader for a card (`METRIC_READERS`), it is called and its rows are served as-is. Until it lands, the same
rows are read from the `metrics` table D2 writes, which is the shape §2/§3 fixes. Which of the two answered
is stated in every response as `source`, so nothing here can quietly look computed when it was read, or the
other way round.

Because the exact column NAMES in `metrics` are D2's to choose, the reader is column-driven: `FIELDS` maps
each §3 signal to the names it may carry, and a signal whose column is absent is served as `null` -> the card
prints "no baseline" or a dash. A missing column can therefore never turn into a zero, and `missing` on every
response names what could not be found.

Honesty rules enforced here (§3.2, §5)
--------------------------------------
* A volume-versus-average ratio is served ONLY with at least `MIN_BASELINE_SESSIONS` sessions behind it.
  Fewer (or an unknown count) -> `volume_ratio: null`, `volume_baseline: "none"`. Never a ratio.
* The liquidity floors of §3 are applied to every "unusual" list and are returned with it, so the UI can
  state the floors in force on the card itself.
* Nothing is predicted. Every field describes a state that has already been captured, with its `as_of`.

Degradation: a missing file, a missing table or a locked database all report `available: False` with
`EMPTY_TEXT` - "No F&O data captured yet - capture starts at the next 15-min reading" - never an error page.
"""
from __future__ import annotations
import logging,sqlite3,threading
from datetime import date,datetime,timedelta,timezone
from pathlib import Path

LOG=logging.getLogger('pilot.derivatives')
IST=timezone(timedelta(hours=5,minutes=30))
QUERY_TIMEOUT=8.0

#: §3 liquidity floors. Constants in code, shown in the UI (the response carries them).
FLOOR_PREMIUM_CR=2.0
FLOOR_OI_LOTS=1
FLOOR_LAST_PRICE=1.0
FLOORS={'premium_cr':FLOOR_PREMIUM_CR,'oi_lots':FLOOR_OI_LOTS,'last_price':FLOOR_LAST_PRICE}
FLOORS_TEXT=(f'Liquidity floors in force: premium traded ≥ ₹{FLOOR_PREMIUM_CR:g} cr, '
 f'OI ≥ {FLOOR_OI_LOTS} lot, last price ≥ ₹{FLOOR_LAST_PRICE:g}.')
#: §3.2: fewer than 3 sessions of history is "no baseline", never a ratio.
MIN_BASELINE_SESSIONS=3
#: The one empty-state sentence for the whole tab. Not an error - capture simply has not run yet.
EMPTY_TEXT='No F&O data captured yet — capture starts at the next 15-min reading'
#: §4 card 4 names these three exactly.
INDEX_UNDERLYINGS=('NIFTY','BANKNIFTY','FINNIFTY')
#: Watch-list groups offered by the filter panel. "all" is every captured underlying.
WATCHLISTS=(('all','All underlyings'),('indices','Indices'))
OPTION_TYPES=('CE','PE')
BUILDUP_LABELS={'long_buildup':'Long build-up','short_buildup':'Short build-up',
 'short_covering':'Short covering','long_unwinding':'Long unwinding'}
#: Rupees per crore. The only unit conversion this module performs (§3.4 asks for ₹ crore).
CRORE=1e7
ROW_LIMIT_DEFAULT=60
ROW_LIMIT_MAX=500
SERIES_LIMIT_MAX=400

# --- the futures price chart (between the unusual screener and the ΔOI grid) -----------------------------------
#: The two cadences the chart offers. 15 minutes is the default; daily is the alternative offered at the top.
INTERVAL_15M='15m'
INTERVAL_1D='1d'
CHART_INTERVALS=(INTERVAL_15M,INTERVAL_1D)
DEFAULT_CHART_INTERVAL=INTERVAL_15M
#: (table, time column) per interval. `candles_day` holds the VENDOR'S OWN daily bar for the same contract,
#: written by `market_data.derivatives.backfill.run_daily_backfill`. It is deliberately not `daily_rollups`,
#: which is the capture worker's roll-up of its own 15-minute bars and is rebuilt from them.
CHART_TABLES={INTERVAL_15M:('candles_15m','bar_start'),INTERVAL_1D:('candles_day','session_date')}
INTERVAL_LABELS={INTERVAL_15M:'15 minutes',INTERVAL_1D:'Daily'}
#: One response is capped here. A futures contract lives about three months - roughly 1,560 fifteen-minute bars
#: and about 60 sessions - so this ceiling never cuts into a contract's own history.
CHART_MAX_CANDLES=2000
#: Fewer distinct trading sessions than this and the series is too short to read as a trend. It says so in plain
#: words rather than drawing a stub that looks broken: when the front contract rolls, the new one starts with
#: almost no history of its own, and that is a fact about the contract, not a fault in the data.
SHORT_HISTORY_SESSIONS=10
#: What the chart is, stated on itself. It is one contract's own candles - never a stitched continuous series
#: and never the underlying index.
CHART_TEXT=("This is the front futures contract's own price history. It is not a stitched continuous series "
 'and it is not the underlying index, so it begins on the day the exchange listed the contract.')
#: The gap rule, stated where the reader can see it. A reading the exchange HAD, that this contract has no bar
#: for, keeps its slot with empty prices - so the chart cannot close the hole up and draw two readings side by
#: side that were never side by side.
GAPS_TEXT=('A 15-min reading or a session this contract has no bar for keeps its place on the chart with no '
 'candle drawn. The hole is never closed up and no bar is carried forward into it.')
#: What the store can and cannot testify to, said plainly rather than implied. The session grid is the union of
#: the readings every contract in the same table holds: a reading exists there only because something traded at
#: it. A session missing from the WHOLE store therefore cannot be told apart from a day the exchange was shut,
#: and is not reported as a gap.
GRID_SOURCE_TEXT=('Which readings the exchange had is taken from the store itself \u2014 the readings every other '
 'contract holds. A session missing from every contract reads as no session at all, not as a gap.')

# --- the ΔOI strike grid (the owner's 2 × 5 block) ------------------------------------------------------------
#: Four strikes either side of the money: ATM CE and ATM+1..+4 CE on the calls row, ATM PE and ATM−1..−4 PE on the
#: puts row. Exactly ten slots, always in this order, so a strike that is not listed reads as a named gap rather
#: than shifting the grid.
GRID_WIDTH=4
GRID_SLOTS=2*(GRID_WIDTH+1)
#: "the latest mark versus four marks ago" — one hour of 15-minute marks.
DIRECTION_LOOKBACK_MARKS=4
#: |change| under this fraction of the contract's OWN largest |ΔOI| today is flat, not a direction.
FLAT_FRACTION=0.05
#: The same 5% shape, applied to the contract's OWN premium: |price change over the window| under this fraction
#: of its largest |price move from the day's first reading| today is flat, not a direction. Kept as its own
#: constant (rather than reusing FLAT_FRACTION) so the check script can read BOTH sides and compare them.
PRICE_FLAT_FRACTION=0.05
#: The block-level read: both rows building, and neither side's total |ΔOI change| over the window more than
#: roughly a third larger than the other's. Above this ratio the two sides are not "similar" and nothing is said.
BLOCK_BALANCE_RATIO=1.33
#: The capture worker's own name for a 15-minute bar-close mark (market_data/derivatives/config.MARK_BAR_CLOSE).
#: Applied only when the store actually carries the column, so a store written before it existed still reads.
MARK_BAR_CLOSE='bar_close'
#: The grid's own empty sentence. Not an error: two marks simply have not been captured yet.
NOT_ENOUGH_MARKS='Not enough readings captured yet — the first line appears after two 15-min readings'
#: The three definitions the block states on itself. Stated, never re-invented per widget.
DELTA_OI_TEXT=('ΔOI is open interest added or removed since the previous close. Every line starts at 0 at the '
 'first 15-min reading of the day.')
ATM_TEXT=('ATM is the listed strike nearest spot in the front expiry. ATM+n is n strikes above spot, ATM−n is n '
 'strikes below.')
DIRECTION_TEXT=(f'Direction is the latest ΔOI against the reading {DIRECTION_LOOKBACK_MARKS} back (one hour). Flat '
 f'when the change is under {FLAT_FRACTION:.0%} of that contract\'s own largest ΔOI today. Fewer than two readings '
 'is "no baseline" and carries no direction at all.')
#: What a slot's direction may say. "no baseline" is a state, not a fourth direction.
GRID_DIRECTIONS=('building','flat','unwinding','no baseline')
#: What the contract's OWN premium may say over the same window. Same shape, same "no baseline" state.
GRID_PRICE_DIRECTIONS=('up','down','flat','no baseline')

# --- price and OI read together (the owner's two lines per tile) ----------------------------------------------
#: (option type, price direction, OI direction) -> (what is happening, what it means). Keyed as one string so
#: the check script can read this table out of the file and compare it with read_api.py and with logic.ts.
#: Nothing here says what happens next: each row names who appears to be doing what, right now.
#:
#: COMPLETE by construction: every one of the nine (price x OI) combinations is listed for each option type,
#: so there is no fall-through case for a reading to be collapsed into. The rule the check script enforces
#: over the whole table: when OI has a direction the sentence says something happened to open interest, and
#: when OI is flat the sentence says open interest barely moved. The chip and the sentence can then never
#: disagree. The four directional rows per type are the owner's own wording; the five rows that involve a
#: flat axis are shared, because a call and a put read the same way when one of the two numbers is still.
FLOW_LABELS={
 'CE|down|building':('Call writing increasing','Sellers are building resistance'),
 'CE|up|unwinding':('Call short covering','Call sellers are exiting'),
 'CE|up|building':('Call buying increasing','Traders are buying upside'),
 'CE|down|unwinding':('Call buyers exiting','Call buyers are closing out'),
 'CE|flat|building':('New positions added','Premium barely moved'),
 'CE|flat|unwinding':('Positions closing out','Premium barely moved'),
 'CE|up|flat':('Premium rose','Open interest barely moved'),
 'CE|down|flat':('Premium fell','Open interest barely moved'),
 'CE|flat|flat':('Very little change','Positioning is unchanged'),
 'PE|down|building':('Put writing increasing','Sellers are building support'),
 'PE|up|unwinding':('Put short covering','Put sellers are exiting'),
 'PE|up|building':('Put buying increasing','Traders are buying downside protection'),
 'PE|down|unwinding':('Put buyers exiting','Put buyers are closing out'),
 'PE|flat|building':('New positions added','Premium barely moved'),
 'PE|flat|unwinding':('Positions closing out','Premium barely moved'),
 'PE|up|flat':('Premium rose','Open interest barely moved'),
 'PE|down|flat':('Premium fell','Open interest barely moved'),
 'PE|flat|flat':('Very little change','Positioning is unchanged'),
}
#: ONLY when BOTH are flat. A tile whose OI moved never says this: the sentence under a tile must never
#: contradict the chip above it, and "positioning is unchanged" beside "↓ UNWINDING" is a false statement.
FLOW_FLAT_WHAT='Very little change'
FLOW_FLAT_MEANING='Positioning is unchanged'
#: Fewer than two readings carrying a price, or carrying a ΔOI. Never guessed at.
FLOW_NOT_ENOUGH='Not enough readings yet'
#: Every sentence a tile is allowed to show as "what is happening". Nothing else reaches the reader, whichever
#: module computed it - a delegate that drifts is recomputed here rather than served.
FLOW_WORDINGS=frozenset([w for w,_ in FLOW_LABELS.values()]+[FLOW_NOT_ENOUGH])
#: The one line under the whole 2 x 5 block, printed only when the test above passes. Nothing otherwise.
BLOCK_BOTH_BUILDING='Both sides building similarly — no clear directional edge'
#: The second line every tile now draws, stated on the block rather than implied.
PRICE_TEXT=('The dashed line on each tile is that contract\'s own last traded price at the same 15-min readings. '
 'It has its own scale — a rupee premium and a count of contracts share no units.')
#: The one sentence under the block that keeps the two-line reading honest. It is a reading of two numbers.
FLOW_TEXT=('Every opened contract has a buyer and a seller. These two lines read which side was paying up at each '
 '15-min reading — they do not say what happens next.')
#: What the block-level line is, and the tolerance it uses, stated where the reader can see it.
BLOCK_TEXT=(f'The line under the block appears only when the calls and the puts are both building over the same '
 f'window and neither side\'s total ΔOI change is more than {BLOCK_BALANCE_RATIO:g}x the other\'s. Otherwise '
 'nothing is said, because there is nothing to say.')
#: Underlyings and expiries arrive from the URL; they are matched against these before touching SQL.
SYMBOL_MAX=40
EXPIRY_LEN=10


def today_ist():return datetime.now(IST).date()

def clean_symbol(value):
 """An underlying as the catalogue stores it, or '' - never a fragment of SQL."""
 text=str(value or '').strip().upper()
 if not text or len(text)>SYMBOL_MAX:return ''
 return text if all(c.isalnum() or c in '&-_.' for c in text) else ''

def clean_expiry(value):
 """'YYYY-MM-DD' or ''."""
 text=str(value or '').strip()[:EXPIRY_LEN]
 try:datetime.strptime(text,'%Y-%m-%d')
 except (TypeError,ValueError):return ''
 return text

def days_to_expiry(expiry,today=None):
 """Whole days from today (IST) to the expiry date. None when the date is unreadable."""
 text=clean_expiry(expiry)
 if not text:return None
 try:return (date.fromisoformat(text)-(today or today_ist())).days
 except ValueError:return None

def _num(value):
 try:
  out=float(value)
 except (TypeError,ValueError):return None
 return out if out==out and out not in (float('inf'),float('-inf')) else None

def _int(value):
 out=_num(value)
 return None if out is None else int(out)

def _round(value,places=2):
 out=_num(value)
 return None if out is None else round(out,places)


# --- the §3 signal -> column-name map -----------------------------------------------------------------------
# D2 owns the names; these are the ones §2/§3 imply plus the obvious spellings. First hit wins. A signal with
# no column present is served as None, which the cards render as "no baseline" or a dash.
FIELDS={
 'last_price':('last_price','ltp','close'),
 'oi':('oi','open_interest'),
 'oi_lots':('oi_lots','oi_in_lots'),
 'volume':('volume','day_volume','cumulative_volume'),
 'average_price':('average_price','vwap','avg_price'),
 'premium_inr':('premium_inr','premium','premium_traded','premium_rupees','premium_value','premium_rs'),
 'premium_cr':('premium_cr','premium_crore','premium_traded_cr'),
 'price_change_15m_pct':('price_change_15m_pct','price_chg_15m_pct'),  # never price_change_15m: that column is the rupee move, not a percent
 'oi_change_15m':('oi_change_15m','oi_chg_15m'),
 'oi_change_15m_pct':('oi_change_15m_pct','oi_chg_15m_pct','oi_change_pct_15m'),
 'buildup_15m':('buildup_15m','build_up_15m','oi_buildup_15m'),
 'price_change_day_pct':('price_change_day_pct','price_chg_day_pct'),  # never price_change_day: rupee move
 'oi_change_day':('oi_change_day','oi_chg_day','oi_change_dod'),
 'oi_change_day_pct':('oi_change_day_pct','oi_chg_day_pct','oi_change_dod_pct','oi_change_pct_day'),
 'buildup_day':('buildup_day','build_up_day','oi_buildup_day','buildup_dod'),
 'volume_ratio':('volume_ratio','volume_vs_median','volume_vs_average','volume_tod_ratio','vol_tod_ratio'),
 'volume_baseline_sessions':('volume_baseline_sessions','baseline_sessions','volume_baseline_n','baseline_n','vol_tod_sessions'),
 'volume_to_oi':('volume_to_oi','volume_oi_ratio','vol_oi_ratio'),
 'previous_oi':('previous_oi','prev_day_oi','prev_close_oi','vol_oi_prev_oi'),
 'days_to_expiry':('days_to_expiry','dte'),
 'basis':('basis','futures_basis'),
 'oi_vs_20d_avg':('oi_vs_20d_avg','oi_vs_20day_avg','oi_share_20d_avg','fut_oi_vs_avg'),
 'spot':('spot','underlying_price'),
}
#: Columns the cards read off `contracts`; these are D1's and are fixed by §2.
CONTRACT_FIELDS=('tradingsymbol','underlying','instrument_type','strike','expiry','lot_size')


class _Columns:
 """Which §3 signals this `metrics` table actually carries."""
 def __init__(self,present):
  self.present=set(present or ())
  self.map={}
  for field,names in FIELDS.items():
   for name in names:
    if name in self.present:self.map[field]=name;break
  self.missing=sorted(f for f in FIELDS if f not in self.map)
 def select(self,alias='m'):
  return [f'{alias}."{col}" as "{field}"' for field,col in self.map.items()]
 def has(self,field):return field in self.map


class Derivatives:
 """Read-only view of `db/derivatives.db` for the Derivative tab."""

 def __init__(self,path,metrics_module=None,read_module=None):
  self.path=Path(path) if path else None
  self._local=threading.local()
  self._metrics=metrics_module  # tests inject; production resolves lazily
  self._metrics_tried=metrics_module is not None
  self._read=read_module        # market_data.derivatives.read_api, same deal
  self._read_tried=read_module is not None
  self._lock=threading.Lock()

 # --- connection ------------------------------------------------------
 def _connect(self):
  connection=getattr(self._local,'connection',None)
  if connection is not None:return connection
  if not self.path or not self.path.is_file():return None
  try:
   connection=sqlite3.connect(f'file:{self.path.as_posix()}?mode=ro',uri=True,timeout=QUERY_TIMEOUT,check_same_thread=False)
   connection.row_factory=sqlite3.Row
   connection.execute('PRAGMA query_only=1')
   connection.execute('select 1 from contracts limit 1')
  except sqlite3.Error as error:
   LOG.info('derivatives: no readable F&O store yet (%s)',error)
   try:
    if connection is not None:connection.close()
   except sqlite3.Error:pass
   return None
  self._local.connection=connection
  return connection
 def close(self):
  connection=getattr(self._local,'connection',None)
  if connection is not None:
   try:connection.close()
   except sqlite3.Error:pass
   self._local.connection=None
 def available(self):return self._connect() is not None

 def _tables(self):
  connection=self._connect()
  if connection is None:return set()
  try:return {r[0] for r in connection.execute("select name from sqlite_master where type='table'")}
  except sqlite3.Error:return set()
 def _columns(self,table):
  connection=self._connect()
  if connection is None:return _Columns(())
  try:return _Columns([r[1] for r in connection.execute(f'PRAGMA table_info("{table}")')])
  except sqlite3.Error:return _Columns(())
 def _column_names(self,table):
  """The raw column names of a table, for the optional columns the store may or may not carry."""
  connection=self._connect()
  if connection is None:return set()
  try:return {r[1] for r in connection.execute(f'PRAGMA table_info("{table}")')}
  except sqlite3.Error:return set()
 def _rows(self,sql,params=()):
  connection=self._connect()
  if connection is None:return []
  try:return [dict(r) for r in connection.execute(sql,params)]
  except sqlite3.Error as error:
   LOG.info('derivatives: query failed (%s)',error);return []
 def _one(self,sql,params=()):
  rows=self._rows(sql,params)
  return rows[0] if rows else None
 def _max(self,table,column='captured_at',where='',params=()):
  if table not in self._tables():return None
  row=self._one(f'select max("{column}") as v from "{table}"'+(f' where {where}' if where else ''),params)
  return (row or {}).get('v')

 # --- D2's reader module ----------------------------------------------
 def metrics_module(self):
  """`market_data.derivatives.metrics` when it is importable, else None. Resolved once."""
  if not self._metrics_tried:
   with self._lock:
    if not self._metrics_tried:
     self._metrics_tried=True
     try:
      from market_data.derivatives import metrics  # noqa: PLC0415 - lazy: the pilot must boot without it
      self._metrics=metrics
     except Exception:  # not built yet, or not on this machine's path
      LOG.info('derivatives: metrics module not importable; serving the stored `metrics` rows.')
      self._metrics=None
  return self._metrics
 def read_module(self):
  """`market_data.derivatives.read_api` when it is importable, else None. Resolved once.

  The pilot runs with PYTHONPATH=server, so on most machines this is None and the ΔOI grid is read from the
  store below instead. Which of the two answered is stated in the response as `source`.
  """
  if not self._read_tried:
   with self._lock:
    if not self._read_tried:
     self._read_tried=True
     try:
      from market_data.derivatives import read_api  # noqa: PLC0415 - lazy: the pilot must boot without it
      self._read=read_api
     except Exception:
      LOG.info('derivatives: read_api not importable; reading the ΔOI grid from the store.')
      self._read=None
  return self._read

 def _delegate(self,name,**kwargs):
  """Call D2's reader for a card. Returns (rows, True) when it answered, ([], False) otherwise.

  A reader that is absent, or whose signature does not accept these keywords, is not an error: the stored
  rows are served instead and `source` says so.
  """
  module=self.metrics_module()
  reader=getattr(module,name,None) if module else None
  if reader is None:return [],False
  try:
   # The reader opens the store itself, so it must be told WHICH store: without this it resolves its own
   # default path and would answer from the production file while this reader is pointed at another one.
   try:rows=reader(db_path=self.path,**kwargs)
   except TypeError:rows=reader(**kwargs)
  except TypeError as error:
   LOG.info('derivatives: %s(%s) not called - %s',name,','.join(kwargs),error);return [],False
  except Exception as error:
   LOG.warning('derivatives: %s failed (%s); serving the stored rows.',name,error);return [],False
  return [dict(r) for r in (rows or [])],True

 # --- shared envelope --------------------------------------------------
 def envelope(self,as_of=None,source='store',missing=(),**extra):
  """Every card carries its own as-of, the floors in force and, when empty, the reason in plain words."""
  ready=self.available()
  body={'available':ready,'captured':bool(as_of),'as_of':as_of,'floors':dict(FLOORS),'floors_text':FLOORS_TEXT,
   'source':source if ready else 'none','missing':list(missing),'empty_text':EMPTY_TEXT,
   'empty_reason':None if as_of else EMPTY_TEXT,'baseline_sessions_required':MIN_BASELINE_SESSIONS}
  body.update(extra)
  return body

 def status(self):
  """Is there anything captured at all, and when. The tab's own header line reads this."""
  tables=self._tables()
  as_of=self._max('metrics') or self._max('snapshots') or self._max('underlying_snapshots')
  columns=self._columns('metrics')
  sessions=None
  if 'candles_15m' in tables:
   row=self._one('select count(distinct substr(bar_start,1,10)) as n from candles_15m')
   sessions=_int((row or {}).get('n'))
  counts={}
  if 'contracts' in tables:
   for row in self._rows('select instrument_type as t, count(*) as n from contracts group by instrument_type'):
    counts[str(row.get('t') or '?')]=_int(row.get('n')) or 0
  return self.envelope(as_of=as_of,source='metrics_module' if self.metrics_module() else 'store',
   missing=columns.missing,tables=sorted(tables),contracts=counts,backfill_sessions=sessions,
   metrics_ready=bool(columns.map),index_underlyings=list(INDEX_UNDERLYINGS))

 # --- row assembly -----------------------------------------------------
 def _metric_rows(self,where='',params=(),limit=None,types=None,order=''):
  """Latest-mark `metrics` rows joined to their contract. Column-driven; absent signals come back None."""
  tables=self._tables()
  if 'metrics' not in tables or 'contracts' not in tables:return [],None,_Columns(())
  columns=self._columns('metrics')
  if not columns.map:return [],None,columns
  as_of=self._max('metrics')
  if not as_of:return [],None,columns
  clauses=['m.captured_at=?']
  args=[as_of]
  if types:
   clauses.append('c.instrument_type in (%s)'%','.join('?' for _ in types));args.extend(types)
  if where:clauses.append(where);args.extend(params)
  select=','.join([f'c."{f}" as "{f}"' for f in CONTRACT_FIELDS]+['m.captured_at as captured_at',
   'm.instrument_token as instrument_token']+columns.select())
  sql=(f'select {select} from metrics m join contracts c on c.instrument_token=m.instrument_token '
   f'where {" and ".join(clauses)} {order}')
  if limit:sql+=f' limit {int(limit)}'
  return [self._shape(r) for r in self._rows(sql,tuple(args))],as_of,columns

 def _shape(self,row):
  """One contract row in the tab's own shape. Presentation only - no signal is computed here."""
  premium_cr=_num(row.get('premium_cr'))
  if premium_cr is None:
   premium=_num(row.get('premium_inr'))
   premium_cr=premium/CRORE if premium is not None else None
  sessions=_int(row.get('volume_baseline_sessions'))
  ratio=_num(row.get('volume_ratio'))
  # §3.2, enforced on the serving side too: a ratio without a stated baseline of >= 3 sessions is not served.
  enough=sessions is not None and sessions>=MIN_BASELINE_SESSIONS
  dte=_int(row.get('days_to_expiry'))
  if dte is None:dte=days_to_expiry(row.get('expiry'))
  return {
   'instrument_token':_int(row.get('instrument_token')),'tradingsymbol':row.get('tradingsymbol') or '',
   'underlying':row.get('underlying') or '','instrument_type':row.get('instrument_type') or '',
   'strike':_num(row.get('strike')),'expiry':row.get('expiry') or '','lot_size':_int(row.get('lot_size')),
   'days_to_expiry':dte,'captured_at':row.get('captured_at'),
   'last_price':_round(row.get('last_price')),'oi':_int(row.get('oi')),'oi_lots':_int(row.get('oi_lots')),
   'volume':_int(row.get('volume')),'average_price':_round(row.get('average_price')),
   'premium_cr':_round(premium_cr),
   'price_change_15m_pct':_round(row.get('price_change_15m_pct')),
   'oi_change_15m':_int(row.get('oi_change_15m')),'oi_change_15m_pct':_round(row.get('oi_change_15m_pct')),
   'buildup_15m':row.get('buildup_15m') or None,
   'price_change_day_pct':_round(row.get('price_change_day_pct')),
   'oi_change_day':_int(row.get('oi_change_day')),'oi_change_day_pct':_round(row.get('oi_change_day_pct')),
   'buildup_day':row.get('buildup_day') or None,
   'volume_ratio':_round(ratio) if enough else None,
   'volume_baseline_sessions':sessions,'volume_baseline':'ok' if enough else 'none',
   'volume_to_oi':_round(row.get('volume_to_oi')),'previous_oi':_int(row.get('previous_oi')),
   'basis':_round(row.get('basis')),'oi_vs_20d_avg':_round(row.get('oi_vs_20d_avg')),
   # The underlying's price at this mark, for the table's Spot column. `spot` is already in FIELDS (so a store
   # without the column already names it in `missing`); this only passes the captured value through. Absent ⇒
   # None ⇒ a dash, never the chain's spot borrowed from another mark.
   'spot':_round(row.get('spot')),
  }

 def _passes(self,row):
  """§3 liquidity floors. A row missing one of the three numbers does not pass - absence is not evidence."""
  premium,price=row.get('premium_cr'),row.get('last_price')
  lots=row.get('oi_lots')
  if lots is None:
   oi,lot=row.get('oi'),row.get('lot_size')
   lots=(oi/lot) if oi is not None and lot else None
  return (premium is not None and premium>=FLOOR_PREMIUM_CR and price is not None and price>=FLOOR_LAST_PRICE
   and lots is not None and lots>=FLOOR_OI_LOTS)

 # --- filters ----------------------------------------------------------
 def filters(self):
  """Everything the Customize panel offers: underlyings, expiries, watch-lists, the floors, the as-of."""
  tables=self._tables()
  if 'contracts' not in tables:return self.envelope(underlyings=[],expiries=[],watchlists=[dict(key=k,label=l) for k,l in WATCHLISTS],option_types=list(OPTION_TYPES))
  as_of=self._max('metrics') or self._max('snapshots')
  today=today_ist()
  underlyings=[]
  for row in self._rows("select underlying,"
    " sum(case when instrument_type in ('CE','PE') then 1 else 0 end) as options,"
    " sum(case when instrument_type='FUT' then 1 else 0 end) as futures,"
    ' max(lot_size) as lot_size from contracts group by underlying order by underlying'):
   name=row.get('underlying') or ''
   if not name:continue
   underlyings.append({'underlying':name,'options':_int(row.get('options')) or 0,'futures':_int(row.get('futures')) or 0,
    'lot_size':_int(row.get('lot_size')),'is_index':name.upper() in INDEX_UNDERLYINGS})
  expiries=[]
  for row in self._rows('select underlying,expiry,count(*) as contracts from contracts'
    ' where expiry>=? group by underlying,expiry order by underlying,expiry',(today.isoformat(),)):
   expiry=clean_expiry(row.get('expiry'))
   if not expiry:continue
   expiries.append({'underlying':row.get('underlying') or '','expiry':expiry,
    'days_to_expiry':days_to_expiry(expiry,today),'contracts':_int(row.get('contracts')) or 0})
  return self.envelope(as_of=as_of,missing=self._columns('metrics').missing,underlyings=underlyings,expiries=expiries,
   watchlists=[{'key':k,'label':l} for k,l in WATCHLISTS],option_types=list(OPTION_TYPES),
   index_underlyings=list(INDEX_UNDERLYINGS))

 # --- card 1: unusual activity ----------------------------------------
 def unusual(self,underlying='',expiry='',watchlist='',max_dte=None,option_type='',min_premium_cr=None,limit=None):
  """§3.2-§3.4 screen, rolled up per underlying and expandable to its strikes (§4 card 1).

  The floors of §3 are ALWAYS applied; `min_premium_cr` can only raise the premium floor, never lower it.
  """
  rows,delegated=self._delegate('unusual_activity',underlying=underlying or None,expiry=expiry or None,
   max_days_to_expiry=max_dte,option_type=option_type or None,min_premium_cr=min_premium_cr,limit=limit)
  columns=self._columns('metrics')
  if delegated:
   shaped=[self._shape(r) for r in rows];as_of=shaped[0]['captured_at'] if shaped else self._max('metrics')
  else:
   clauses,params=[],[]
   if underlying:clauses.append('c.underlying=?');params.append(underlying)
   if expiry:clauses.append('c.expiry=?');params.append(expiry)
   if option_type:clauses.append('c.instrument_type=?');params.append(option_type)
   shaped,as_of,columns=self._metric_rows(' and '.join(clauses),tuple(params),types=OPTION_TYPES)
  floor=max(FLOOR_PREMIUM_CR,_num(min_premium_cr) or 0.0)
  names=self._watchlist(watchlist)
  kept=[]
  for row in shaped:
   if not self._passes(row):continue
   if (row.get('premium_cr') or 0)<floor:continue
   if names is not None and row['underlying'].upper() not in names:continue
   if max_dte is not None and (row.get('days_to_expiry') is None or row['days_to_expiry']>max_dte):continue
   kept.append(row)
  groups={}
  for row in kept:
   group=groups.setdefault(row['underlying'],{'underlying':row['underlying'],'premium_cr':0.0,'strikes':[],
    'expiries':set(),'calls':0,'puts':0,'oi_change_day':0,'has_oi_change':False})
   group['premium_cr']+=row.get('premium_cr') or 0.0
   group['strikes'].append(row)
   if row.get('expiry'):group['expiries'].add(row['expiry'])
   if row['instrument_type']=='CE':group['calls']+=1
   elif row['instrument_type']=='PE':group['puts']+=1
   if row.get('oi_change_day') is not None:group['oi_change_day']+=row['oi_change_day'];group['has_oi_change']=True
  out=[]
  for group in groups.values():
   strikes=sorted(group['strikes'],key=lambda r:-(r.get('premium_cr') or 0))
   out.append({'underlying':group['underlying'],'premium_cr':round(group['premium_cr'],2),
    'strike_count':len(strikes),'calls':group['calls'],'puts':group['puts'],
    'expiries':sorted(group['expiries']),
    'days_to_expiry':min([s['days_to_expiry'] for s in strikes if s['days_to_expiry'] is not None],default=None),
    'oi_change_day':group['oi_change_day'] if group['has_oi_change'] else None,
    'strikes':strikes[:20]})
  out.sort(key=lambda g:-g['premium_cr'])
  cut=max(1,min(int(limit or ROW_LIMIT_DEFAULT),ROW_LIMIT_MAX))
  return self.envelope(as_of=as_of,source='metrics_module' if delegated else 'store',missing=columns.missing,
   rows=out[:cut],total=len(out),floor_premium_cr=floor,
   empty_note=None if not as_of else ('No contract clears the liquidity floors at this 15-min reading.' if not out else None))

 def _watchlist(self,key):
  key=str(key or '').strip().lower()
  if key=='indices':return {n.upper() for n in INDEX_UNDERLYINGS}
  return None

 # --- card 2: option chain ---------------------------------------------
 def chain(self,underlying,expiry='',option_type=''):
  """§4 card 2: one row per strike with CE and PE beside each other, at the latest mark."""
  if not underlying:return self.envelope(rows=[],underlying='',expiry='')
  expiry=expiry or self._front_expiry(underlying)
  rows,delegated=self._delegate('option_chain',underlying=underlying,expiry=expiry or None)
  columns=self._columns('metrics')
  if delegated:
   shaped=[self._shape(r) for r in rows];as_of=shaped[0]['captured_at'] if shaped else self._max('metrics')
  else:
   clauses,params=['c.underlying=?'],[underlying]
   if expiry:clauses.append('c.expiry=?');params.append(expiry)
   shaped,as_of,columns=self._metric_rows(' and '.join(clauses),tuple(params),types=OPTION_TYPES,order='order by c.strike')
  strikes={}
  for row in shaped:
   if option_type and row['instrument_type']!=option_type:continue
   strike=row.get('strike')
   if strike is None:continue
   slot=strikes.setdefault(strike,{'strike':strike,'ce':None,'pe':None})
   slot['ce' if row['instrument_type']=='CE' else 'pe']=row
  spot=self._spot(underlying)
  out=sorted(strikes.values(),key=lambda r:r['strike'])
  return self.envelope(as_of=as_of,source='metrics_module' if delegated else 'store',missing=columns.missing,
   rows=out,underlying=underlying,expiry=expiry,spot=spot.get('spot'),
   days_to_expiry=days_to_expiry(expiry),total=len(out))

 def _front_expiry(self,underlying):
  row=self._one('select min(expiry) as e from contracts where underlying=? and expiry>=?',
   (underlying,today_ist().isoformat()))
  return clean_expiry((row or {}).get('e'))

 def _spot(self,underlying):
  """Latest `underlying_snapshots` row: spot, futures price, PCR, max pain - all D1/D2's numbers."""
  if 'underlying_snapshots' not in self._tables():return {}
  row=self._one('select * from underlying_snapshots where underlying=? order by captured_at desc limit 1',(underlying,))
  if not row:return {}
  return {'captured_at':row.get('captured_at'),'spot':_round(row.get('spot')),'fut_price':_round(row.get('fut_price')),
   'total_ce_oi':_int(row.get('total_ce_oi')),'total_pe_oi':_int(row.get('total_pe_oi')),
   'total_ce_volume':_int(row.get('total_ce_volume')),'total_pe_volume':_int(row.get('total_pe_volume')),
   'pcr_oi':_round(row.get('pcr_oi')),'pcr_volume':_round(row.get('pcr_volume')),
   'max_pain_strike':_num(row.get('max_pain_strike'))}

 # --- card 3: OI by strike ---------------------------------------------
 def oi_by_strike(self,underlying,expiry=''):
  """§4 card 3: CE vs PE OI per strike, with max pain and spot marked. §3.6's total OI travels with it."""
  if not underlying:return self.envelope(rows=[],underlying='',expiry='')
  chain=self.chain(underlying,expiry)
  series=[]
  total_ce=total_pe=0
  for row in chain.get('rows') or []:
   ce,pe=row.get('ce') or {},row.get('pe') or {}
   total_ce+=ce.get('oi') or 0;total_pe+=pe.get('oi') or 0
   series.append({'strike':row['strike'],'ce_oi':ce.get('oi'),'pe_oi':pe.get('oi'),
    'ce_oi_change_day':ce.get('oi_change_day'),'pe_oi_change_day':pe.get('oi_change_day'),
    'ce_buildup_day':ce.get('buildup_day'),'pe_buildup_day':pe.get('buildup_day')})
  spot=self._spot(underlying)
  max_pain=spot.get('max_pain_strike')
  distance=None
  if max_pain is not None and spot.get('spot') is not None:distance=round(spot['spot']-max_pain,2)
  return self.envelope(as_of=chain.get('as_of'),source=chain.get('source','store'),missing=chain.get('missing',[]),
   rows=series,underlying=underlying,expiry=chain.get('expiry'),spot=spot.get('spot'),
   max_pain_strike=max_pain,max_pain_distance=distance,
   total_ce_oi=total_ce or None,total_pe_oi=total_pe or None,
   days_to_expiry=chain.get('days_to_expiry'))

 # --- card 4: index dashboard ------------------------------------------
 def indices(self,names=None,points=None):
  """§4 card 4: NIFTY / BANKNIFTY / FINNIFTY - PCR, max pain, and OI through the day.

  The through-the-day series is `underlying_snapshots` read back in capture order; nothing is interpolated.
  """
  wanted=[clean_symbol(n) for n in (names or INDEX_UNDERLYINGS)]
  wanted=[n for n in wanted if n]
  if 'underlying_snapshots' not in self._tables():
   return self.envelope(rows=[{'underlying':n,'captured':False} for n in wanted])
  cut=max(1,min(int(points or 60),SERIES_LIMIT_MAX))
  as_of=self._max('underlying_snapshots')
  day=str(as_of or '')[:10]
  out=[]
  for name in wanted:
   latest=self._spot(name)
   series=[]
   for row in self._rows('select captured_at,spot,pcr_oi,pcr_volume,total_ce_oi,total_pe_oi,max_pain_strike'
     ' from underlying_snapshots where underlying=? and substr(captured_at,1,10)=?'
     ' order by captured_at desc limit ?',(name,day,cut)):
    series.append({'captured_at':row.get('captured_at'),'spot':_round(row.get('spot')),
     'pcr_oi':_round(row.get('pcr_oi')),'pcr_volume':_round(row.get('pcr_volume')),
     'total_ce_oi':_int(row.get('total_ce_oi')),'total_pe_oi':_int(row.get('total_pe_oi')),
     'max_pain_strike':_num(row.get('max_pain_strike'))})
   series.reverse()
   distance=None
   if latest.get('max_pain_strike') is not None and latest.get('spot') is not None:
    distance=round(latest['spot']-latest['max_pain_strike'],2)
   expiry=self._front_expiry(name)
   out.append({'underlying':name,'captured':bool(latest),'captured_at':latest.get('captured_at'),
    'spot':latest.get('spot'),'pcr_oi':latest.get('pcr_oi'),'pcr_volume':latest.get('pcr_volume'),
    'max_pain_strike':latest.get('max_pain_strike'),'max_pain_distance':distance,
    'total_ce_oi':latest.get('total_ce_oi'),'total_pe_oi':latest.get('total_pe_oi'),
    'expiry':expiry,'days_to_expiry':days_to_expiry(expiry),'series':series})
  return self.envelope(as_of=as_of,rows=out,missing=self._columns('metrics').missing)

 # --- card 5: futures build-up -----------------------------------------
 def futures(self,underlying='',watchlist='',limit=None):
  """§3.7 / §4 card 5: the front futures contract per underlying with its build-up label, OI share and basis."""
  rows,delegated=self._delegate('futures_buildup',underlying=underlying or None,limit=limit)
  columns=self._columns('metrics')
  if delegated:
   shaped=[self._shape(r) for r in rows];as_of=shaped[0]['captured_at'] if shaped else self._max('metrics')
  else:
   clauses,params=[],[]
   if underlying:clauses.append('c.underlying=?');params.append(underlying)
   shaped,as_of,columns=self._metric_rows(' and '.join(clauses),tuple(params),types=('FUT',),order='order by c.expiry')
  names=self._watchlist(watchlist)
  front,out={},[]
  for row in shaped:
   if names is not None and row['underlying'].upper() not in names:continue
   current=front.get(row['underlying'])
   if current is None or (row.get('expiry') or '9999')<(current.get('expiry') or '9999'):front[row['underlying']]=row
  for row in front.values():
   basis=row.get('basis')
   if basis is None:
    spot=self._spot(row['underlying']).get('spot')
    # §3.7 defines basis as futures − spot. Served only when BOTH numbers were captured; never half of one.
    if spot is not None and row.get('last_price') is not None:basis=round(row['last_price']-spot,2)
   out.append({**row,'basis':basis})
  out.sort(key=lambda r:-(r.get('premium_cr') or 0) if r.get('premium_cr') is not None else 0)
  cut=max(1,min(int(limit or ROW_LIMIT_DEFAULT),ROW_LIMIT_MAX))
  return self.envelope(as_of=as_of,source='metrics_module' if delegated else 'store',missing=columns.missing,
   rows=out[:cut],total=len(out))

 # --- the linked chart --------------------------------------------------
 def series(self,underlying='',instrument_token=None,points=None):
  """The linked panel: one contract's 15-minute price + OI, or the underlying's spot + total OI.

  `candles_15m` (D1's backfill and its top-up) for a contract; `underlying_snapshots` for an underlying.
  Nothing is resampled, gap-filled or extended - the points are the captured marks, newest last.
  """
  cut=max(1,min(int(points or 120),SERIES_LIMIT_MAX))
  tables=self._tables()
  if instrument_token is not None and 'candles_15m' in tables:
   contract=self._one('select tradingsymbol,underlying,instrument_type,strike,expiry,lot_size from contracts'
    ' where instrument_token=?',(instrument_token,)) or {}
   rows=self._rows('select bar_start,open,high,low,close,volume,oi from candles_15m where instrument_token=?'
    ' order by bar_start desc limit ?',(instrument_token,cut))
   rows.reverse()
   points_out=[{'t':r.get('bar_start'),'price':_round(r.get('close')),'oi':_int(r.get('oi')),
    'volume':_int(r.get('volume'))} for r in rows]
   as_of=points_out[-1]['t'] if points_out else self._max('candles_15m','bar_start')
   return self.envelope(as_of=as_of,points=points_out,kind='contract',
    tradingsymbol=contract.get('tradingsymbol') or '',underlying=contract.get('underlying') or underlying,
    instrument_token=instrument_token,expiry=contract.get('expiry') or '',
    strike=_num(contract.get('strike')),instrument_type=contract.get('instrument_type') or '',
    price_label='Last price',oi_label='Open interest')
  if not underlying or 'underlying_snapshots' not in tables:
   return self.envelope(points=[],kind='none',underlying=underlying)
  rows=self._rows('select captured_at,spot,total_ce_oi,total_pe_oi,pcr_oi from underlying_snapshots'
   ' where underlying=? order by captured_at desc limit ?',(underlying,cut))
  rows.reverse()
  points_out=[]
  for row in rows:
   ce,pe=_int(row.get('total_ce_oi')),_int(row.get('total_pe_oi'))
   total=None if ce is None and pe is None else (ce or 0)+(pe or 0)
   points_out.append({'t':row.get('captured_at'),'price':_round(row.get('spot')),'oi':total,
    'ce_oi':ce,'pe_oi':pe,'pcr_oi':_round(row.get('pcr_oi'))})
  as_of=points_out[-1]['t'] if points_out else self._max('underlying_snapshots')
  return self.envelope(as_of=as_of,points=points_out,kind='underlying',underlying=underlying,
   price_label='Spot',oi_label='Total option OI (CE + PE)')

 # --- the futures price chart -------------------------------------------
 @staticmethod
 def clean_interval(value):
  """'15m' / '1d' as the chart names them, or '' - never a fragment of anything."""
  text=str(value or '').strip().lower()
  return text if text in CHART_INTERVALS else ''

 @staticmethod
 def interval_note(interval,symbol,sessions,candles,available):
  """The one plain sentence that describes an interval for THIS contract. Describes, never predicts."""
  sym=symbol or 'this contract'
  plural='' if sessions==1 else 's'
  if interval==INTERVAL_1D:
   if not available:
    return (f'No daily candles are stored for {sym} yet — daily history is fetched contract by contract, and '
     'this one has not been fetched.')
   return (f'One bar per trading session of {sym} itself — the contract, not the index, and not a stitched '
    f'continuous series. {sessions} trading session{plural} stored. A futures contract is listed for roughly three '
    'months, so its own daily history is short by nature, and it starts again from almost nothing each time '
    'the front contract rolls.')
  if not available:
   return f'No 15-minute candles are stored for {sym} yet — they arrive with the next 15-min reading.'
  return (f'15-minute candles of {sym} itself — the contract, not the index. {candles} stored bars across '
   f'{sessions} trading session{plural}. A reading with no bar keeps its place and draws nothing.')

 @staticmethod
 def short_history_text(symbol,interval,sessions,expiry,dte):
  """Why this series is short, in the owner's register. A short series is a fact, not an error."""
  sym=symbol or 'this contract'
  span=INTERVAL_LABELS.get(interval,interval).lower()
  plural='' if sessions==1 else 's'
  tail=''
  if expiry:
   away='' if dte is None else f' ({dte} day{"" if dte==1 else "s"} away)'
   tail=(f' {sym} expires on {expiry}{away}, and the front contract after it starts again with almost no '
    'history of its own.')
  return (f'Short series: {sessions} trading session{plural} of {span} candles for {sym}. That is too little '
   'history to read as a trend, so it is drawn as it is rather than padded out.'+tail)

 def _front_future(self,underlying,today=None):
  """The front futures contract of `underlying`: the nearest expiry not yet past.

  Falls back to the LATEST expiry stored when every futures contract we hold has expired - an expired contract
  disappears from the vendor but stays in this store, and serving the newest one we know is honest as long as
  its expiry travels with it, which it always does. None means this underlying has no future here at all.
  """
  if 'contracts' not in self._tables():return None
  day=(today or today_ist()).isoformat()
  columns='instrument_token,tradingsymbol,underlying,expiry'
  row=self._one(f"select {columns} from contracts where underlying=? and instrument_type='FUT'"
   " and expiry>=? order by expiry limit 1",(underlying,day))
  if row is None:
   row=self._one(f"select {columns} from contracts where underlying=? and instrument_type='FUT'"
    " order by expiry desc limit 1",(underlying,))
  return row

 def _chart_counts(self,token):
  """Rows and distinct trading days this contract actually has, per interval.

  This is what lets the page disable an interval control instead of offering a dead one.
  """
  tables=self._tables()
  out={}
  for key in CHART_INTERVALS:
   table,column=CHART_TABLES[key]
   if table not in tables:
    out[key]={'candles':0,'sessions':0,'available':False};continue
   row=self._one(f'select count(*) as n,count(distinct substr("{column}",1,10)) as d from "{table}"'
    ' where instrument_token=?',(token,)) or {}
   count=_int(row.get('n')) or 0
   out[key]={'candles':count,'sessions':_int(row.get('d')) or 0,'available':count>0}
  return out

 def _chart_from_store(self,underlying,interval):
  """One contract's own candles at one cadence, oldest first, read straight from the §2 store.

  Same shape and same definitions as `market_data.derivatives.read_api.futures_chart_series`; that module
  answers instead whenever it is importable. Nothing here resamples, interpolates or extends - the rows are
  the bars the store holds, and a missing session stays missing.
  """
  base={'underlying':underlying,'interval':interval,'contract':None,'candles':[],'sessions':0,
   'session':None,'as_of':None,'max_candles':CHART_MAX_CANDLES,
   'intervals':{k:{'candles':0,'sessions':0,'available':False} for k in CHART_INTERVALS}}
  contract=self._front_future(underlying)
  if contract is None:return base
  token=_int(contract.get('instrument_token'))
  base['contract']={'tradingsymbol':contract.get('tradingsymbol') or '','instrument_token':token,
   'expiry':clean_expiry(contract.get('expiry')) or None}
  if token is None:return base
  base['intervals']=self._chart_counts(token)
  if not (base['intervals'].get(interval) or {}).get('available'):return base
  table,column=CHART_TABLES[interval]
  rows=self._rows(f'select "{column}" as at,open,high,low,close,volume,oi from "{table}"'
   f' where instrument_token=? order by "{column}" desc limit ?',(token,CHART_MAX_CANDLES))
  rows.reverse()
  stored=[{'at':r.get('at'),'open':_round(r.get('open')),'high':_round(r.get('high')),
   'low':_round(r.get('low')),'close':_round(r.get('close')),'volume':_int(r.get('volume')),
   # unknown stays unknown: a session the vendor sent no open interest for is not a session with none
   'oi':_int(r.get('oi'))} for r in rows if r.get('at')]
  if not stored:return base
  grid=self._session_grid(table,column,stored[0]['at'],stored[-1]['at'])
  base['candles']=self.with_gaps(stored,grid)
  # `sessions` counts the distinct trading days the series SPANS, and a day counts only when a real bar landed
  # on it. A day made entirely of empty slots is a day this contract has no data for.
  days=sorted({str(c['at'])[:10] for c in stored})
  base['sessions'],base['session']=len(days),(days[-1] if days else None)
  base['as_of']=stored[-1]['at']
  return base

 def _session_grid(self,table,column,lo,hi):
  """Every reading the STORE ITSELF knows the exchange had between `lo` and `hi`.

  The union of the readings every contract in that table holds: a 15-min bar start exists there only because
  something traded at it, and a session date only because something traded that day. Same definition, same
  SQL, as `market_data.derivatives.read_api.session_grid`.

  The boundary, stated because it matters: a session missing from the WHOLE store cannot be told apart from a
  day the exchange was shut, so it is not reported as a gap. Only a reading the store holds for some other
  contract, and not for this one, is a gap.
  """
  rows=self._rows(f'select distinct "{column}" as at from "{table}" where "{column}">=? and "{column}"<=?'
   f' order by "{column}"',(lo,hi))
  return [r.get('at') for r in rows if r.get('at')]

 @staticmethod
 def with_gaps(stored,grid):
  """The series on the exchange's own grid: a reading with no bar keeps its slot, drawing nothing.

  Leaving a missing bar OUT would make its neighbours adjacent, and the chart would then claim continuous
  trading across a period that had none - an untrue picture that nothing downstream could detect, because the
  absence would be invisible by construction. The slot stays and its prices are null.

  A volume of 0 and an `oi` of null are different statements from \"no bar at all\": a bar that genuinely traded
  nothing is a real bar with real prices and is never turned into a gap.
  """
  have={c['at']:c for c in stored}
  out=[]
  for at in grid:
   row=have.get(at)
   if row is None:
    out.append({'at':at,'open':None,'high':None,'low':None,'close':None,'volume':None,'oi':None,'gap':True})
   else:out.append({**row,'gap':False})
  return out

 def _chart_delegate(self,underlying,interval):
  """D2's own reader, when `market_data.derivatives.read_api` is on this machine's path."""
  module=self.read_module()
  reader=getattr(module,'futures_chart_series',None) if module else None
  connection=self._connect()
  if reader is None or connection is None:return None
  try:
   raw=reader(connection,underlying,interval=interval,max_candles=CHART_MAX_CANDLES)
  except TypeError as error:
   LOG.info('derivatives: futures_chart_series not called - %s',error);return None
  except Exception as error:
   LOG.warning('derivatives: futures_chart_series failed (%s); reading the chart from the store.',error)
   return None
  # A reader whose shape this module does not recognise does not get to put a chart on screen: the store
  # answers instead, and `source` says which of the two did. The two paths can then never be out of step.
  if not isinstance(raw,dict) or not isinstance(raw.get('candles'),list):return None
  intervals=raw.get('intervals')
  if not isinstance(intervals,dict) or any(k not in intervals for k in CHART_INTERVALS):return None
  contract=raw.get('contract')
  if contract is not None and not isinstance(contract,dict):return None
  return raw

 def futures_chart(self,underlying,interval=DEFAULT_CHART_INTERVAL):
  """The futures price chart: the FRONT contract's own candles, 15-minute by default, daily as the alternative.

  Display only. It states how many sessions it is actually returning and which contract they belong to, so a
  short series reads as a short series - which is exactly what happens the day the front contract rolls, since
  the new front contract has almost no history of its own. Nothing here is a forecast (§5).
  """
  interval=self.clean_interval(interval) or DEFAULT_CHART_INTERVAL
  notes={'chart_text':CHART_TEXT,'gaps_text':GAPS_TEXT,'grid_source_text':GRID_SOURCE_TEXT,
   'short_history_sessions':SHORT_HISTORY_SESSIONS,'max_candles':CHART_MAX_CANDLES,
   'default_interval':DEFAULT_CHART_INTERVAL}
  if not underlying:
   return self.envelope(interval=interval,contract=None,candles=[],sessions=0,session=None,bars=0,gaps=0,
    short_history=False,short_history_text=None,intervals=[],note=None,unknown_underlying=False,
    underlying='',**notes)
  raw=self._chart_delegate(underlying,interval)
  source='metrics_module' if raw is not None else 'store'
  if raw is None:raw=self._chart_from_store(underlying,interval)
  contract=raw.get('contract') or None
  symbol=(contract or {}).get('tradingsymbol') or ''
  expiry=clean_expiry((contract or {}).get('expiry')) or ''
  dte=days_to_expiry(expiry) if expiry else None
  counts=raw.get('intervals') or {}
  offered=[]
  for key in CHART_INTERVALS:
   row=counts.get(key) or {}
   sessions=_int(row.get('sessions')) or 0
   count=_int(row.get('candles')) or 0
   available=bool(row.get('available')) and count>0
   offered.append({'interval':key,'label':INTERVAL_LABELS[key],'available':available,
    'candles':count,'sessions':sessions,'selected':key==interval,
    'note':self.interval_note(key,symbol,sessions,count,available)})
  candles=[]
  for row in (raw.get('candles') or []):
   if not row.get('at'):continue
   close=_round(row.get('close'))
   # A slot with no close is not a candle: whichever path produced it, it is a gap, and it is flagged as one
   # here rather than trusted - so a delegate and the store fallback can never disagree about what a hole is.
   gap=close is None
   candles.append({'at':row.get('at'),'open':_round(row.get('open')),'high':_round(row.get('high')),
    'low':_round(row.get('low')),'close':close,'volume':(None if gap else _int(row.get('volume'))),
    'oi':(None if gap else _int(row.get('oi'))),'gap':gap})
  drawn=[c for c in candles if not c['gap']]
  # `sessions` is the number of distinct TRADING DAYS the candles span - days, not readings, at either
  # interval - and only a day a real bar landed on is counted. `bars` is the stored-bar count; the two are
  # different numbers at the 15-minute interval and are never conflated.
  days=sorted({str(c['at'])[:10] for c in drawn})
  sessions=len(days)
  short=bool(drawn) and sessions<SHORT_HISTORY_SESSIONS
  served=next((o for o in offered if o['interval']==interval),None)
  # An empty card must say WHY it is empty. The tab's one empty sentence is about the 15-min capture, and on a
  # store that HAS been captured it would be a false reason for a chart whose daily history simply has not been
  # fetched - so the interval's own note is the reason instead. A store that is not readable at all keeps the
  # tab's sentence, because then the sentence is true.
  reason=None
  if not drawn:reason=(served or {}).get('note') if self.available() else EMPTY_TEXT
  return self.envelope(as_of=(drawn[-1]['at'] if drawn else None),source=source,missing=[],
   empty_reason=reason,
   underlying=underlying,interval=interval,bars=len(drawn),gaps=len(candles)-len(drawn),
   contract=({'tradingsymbol':symbol,'instrument_token':_int((contract or {}).get('instrument_token')),
    'expiry':expiry or None,'days_to_expiry':dte} if contract else None),
   candles=candles,sessions=sessions,session=(days[-1] if days else None),
   short_history=short,
   short_history_text=(self.short_history_text(symbol,interval,sessions,expiry,dte) if short else None),
   intervals=offered,note=(served or {}).get('note'),
   unknown_underlying=contract is None and self.available(),**notes)

 # --- the owner's ΔOI strike grid (2 × 5) --------------------------------
 @staticmethod
 def grid_label(option_type,offset):
  """"ATM CE", "ATM+3 CE", "ATM−2 PE" — the owner's own wording for a slot."""
  if not offset:return f'ATM {option_type}'
  return f'ATM{"+" if offset>0 else "−"}{abs(int(offset))} {option_type}'

 @staticmethod
 def grid_direction(points):
  """building / unwinding / flat / "no baseline", from the very points the line is drawn from.

  The rule, stated once: the latest ΔOI against the mark DIRECTION_LOOKBACK_MARKS back (one hour). Flat when
  |change| is under FLAT_FRACTION of that contract's OWN largest |ΔOI| today. Fewer than two marks carrying a
  ΔOI is "no baseline" — a state, never a fourth direction and never a chip.
  """
  usable=[p for p in points or () if p.get('delta_oi') is not None]
  if len(usable)<2:return 'no baseline',{'points_with_delta':len(usable)}
  index=max(0,len(usable)-1-DIRECTION_LOOKBACK_MARKS)
  latest,reference=usable[-1],usable[index]
  change=latest['delta_oi']-reference['delta_oi']
  scale=max(abs(p['delta_oi']) for p in usable)
  threshold=FLAT_FRACTION*scale
  label='flat' if (scale==0 or abs(change)<threshold) else ('building' if change>0 else 'unwinding')
  return label,{'from':reference['at'],'to':latest['at'],'change':_round(change),
   'flat_threshold':_round(threshold),'marks_back':len(usable)-1-index}

 @staticmethod
 def grid_price_direction(points):
  """up / down / flat / "no baseline" for the contract's OWN premium, from the very points the tile draws.

  The same shape as grid_direction, read on price: the latest price against the reading
  DIRECTION_LOOKBACK_MARKS back (one hour); flat when |change| is under PRICE_FLAT_FRACTION of that contract's
  own largest |price move from the day's first reading| today. Fewer than two readings carrying a price is
  "no baseline" — never a direction, never a guess.
  """
  usable=[p for p in points or () if _num(p.get('price')) is not None]
  if len(usable)<2:return 'no baseline',{'points_with_price':len(usable)}
  index=max(0,len(usable)-1-DIRECTION_LOOKBACK_MARKS)
  latest,reference=_num(usable[-1]['price']),_num(usable[index]['price'])
  first=_num(usable[0]['price'])
  change=latest-reference
  scale=max(abs(_num(p['price'])-first) for p in usable)
  threshold=PRICE_FLAT_FRACTION*scale
  label='flat' if (scale==0 or abs(change)<threshold) else ('up' if change>0 else 'down')
  return label,{'from':usable[index].get('at'),'to':usable[-1].get('at'),'price_change':_round(change),
   'price_change_pct':None if not reference else _round(change/reference*100),
   'price_flat_threshold':_round(threshold,4),'readings_back':len(usable)-1-index}

 @classmethod
 def grid_flow(cls,option_type,points):
  """Price and OI over the SAME window, read together: what is happening, and what it means.

  Identical mechanics for a call and a put, read on the option's own premium; the wording differs because
  writing a call and writing a put sit on opposite sides of the strike. Nothing here is a forecast and nothing
  here is a recommendation: each label names who appears to be doing what, at the readings on the tile.
  """
  oi_direction,oi_detail=cls.grid_direction(points)
  price_direction,price_detail=cls.grid_price_direction(points)
  kind=str(option_type or '').upper()
  if price_direction=='no baseline' or oi_direction=='no baseline':
   what,meaning=FLOW_NOT_ENOUGH,None
  else:
   # A straight lookup: FLOW_LABELS holds all nine combinations, so a flat axis has its OWN row and is never
   # collapsed into another one. An option type the table does not hold falls to "not enough", never to a call.
   what,meaning=FLOW_LABELS.get(f'{kind}|{price_direction}|{oi_direction}',(FLOW_NOT_ENOUGH,None))
  detail={'from':price_detail.get('from') or oi_detail.get('from'),
   'to':price_detail.get('to') or oi_detail.get('to'),
   'price_change':price_detail.get('price_change'),'price_change_pct':price_detail.get('price_change_pct'),
   'oi_change':oi_detail.get('change'),'price_flat_threshold':price_detail.get('price_flat_threshold'),
   'oi_flat_threshold':oi_detail.get('flat_threshold'),
   'readings_back':oi_detail.get('marks_back',price_detail.get('readings_back',DIRECTION_LOOKBACK_MARKS))}
  return {'price_direction':price_direction,'oi_direction':oi_direction,'what_label':what,'meaning':meaning,
   'detail':detail}

 @staticmethod
 def grid_block_read(rows):
  """The one line under the whole block, or '' — aggregated from the ten slots already on screen.

  No second query and no strike the grid is not showing: this sums the SAME window change each tile already
  carries. Both sides building, and neither side's total more than BLOCK_BALANCE_RATIO x the other's, is the
  only thing it will say. Anything else prints nothing rather than forcing a summary.
  """
  totals={'calls':0.0,'puts':0.0};counted={'calls':0,'puts':0}
  for slot in rows or ():
   change=_num(((slot or {}).get('flow') or {}).get('detail',{}).get('oi_change'))
   if change is None:continue
   side='puts' if (slot.get('row')=='puts') else 'calls'
   totals[side]+=change;counted[side]+=1
  if not counted['calls'] or not counted['puts']:return ''
  calls,puts=totals['calls'],totals['puts']
  if calls<=0 or puts<=0:return ''
  high,low=max(calls,puts),min(calls,puts)
  if low<=0 or high>BLOCK_BALANCE_RATIO*low:return ''
  return BLOCK_BOTH_BUILDING

 def _grid_session(self):
  """The day the grid describes: the newest captured day in the store."""
  newest=self._max('snapshots') or self._max('candles_15m','bar_start') or self._max('underlying_snapshots')
  return str(newest or '')[:10] or None

 def _grid_previous_close_oi(self,tokens,session):
  """Each contract's OI at the last bar of the last session BEFORE `session`, from `candles_15m`.

  ΔOI is measured against the previous CLOSE, never against the day's first mark. A contract the backfill has
  never covered simply has no entry here and its ΔOI stays None — "no baseline", never a zero.
  """
  if not tokens or 'candles_15m' not in self._tables():return {}
  marks=','.join('?' for _ in tokens)
  sql=(f'with prior as (select instrument_token,bar_start,oi,substr(bar_start,1,10) as d from candles_15m'
   f' where substr(bar_start,1,10)<? and instrument_token in ({marks})),'
   ' last_day as (select instrument_token,max(d) as d from prior group by instrument_token),'
   ' last_bar as (select p.instrument_token as instrument_token,max(p.bar_start) as bar_start from prior p'
   '  join last_day l on l.instrument_token=p.instrument_token and l.d=p.d group by p.instrument_token)'
   ' select c.instrument_token as instrument_token,c.oi as oi from candles_15m c join last_bar b'
   '  on b.instrument_token=c.instrument_token and b.bar_start=c.bar_start')
  out={}
  for row in self._rows(sql,(session,*tokens)):
   value=_num(row.get('oi'))
   if value is not None:out[_int(row.get('instrument_token'))]=value
  return out

 def _grid_points(self,tokens,session,mark):
  """{'source':…,'rows':[…]} — every 15-minute mark of `session` up to `mark`, in capture order.

  `snapshots` is the capture worker's own table and is preferred; a store that only holds the backfill is read
  from `candles_15m` instead, and the response says which. A mark with no row is simply absent — nothing here
  interpolates, carries forward or back-fills, so a gap in the capture stays a gap in the line.
  """
  tables=self._tables()
  marks=','.join('?' for _ in tokens)
  if 'snapshots' in tables:
   kind=' and mark_kind=?' if 'mark_kind' in self._column_names('snapshots') else ''
   args=[*tokens,session]+([MARK_BAR_CLOSE] if kind else [])+[mark]
   rows=self._rows(f'select instrument_token,captured_at,oi,last_price as price from snapshots'
    f' where instrument_token in ({marks})'
    f' and substr(captured_at,1,10)=?{kind} and captured_at<=? order by captured_at',tuple(args))
   if rows:return {'source':'snapshots','rows':rows}
  if 'candles_15m' in tables:
   rows=self._rows(f'select instrument_token,bar_start as captured_at,oi,close as price from candles_15m'
    f' where instrument_token in ({marks}) and substr(bar_start,1,10)=? and bar_start<=? order by bar_start',
    (*tokens,session,mark))
   if rows:return {'source':'candles_15m','rows':rows}
  return {'source':'none','rows':[]}

 def _grid_from_store(self,underlying,expiry=''):
  """The ten at-the-money contracts and their ΔOI series, read straight from the §2 store.

  Same shape and same definitions as `market_data.derivatives.read_api.strike_oi_series`; that module answers
  instead whenever it is importable. Nothing is computed here beyond the one subtraction the definition IS
  (captured OI − captured previous-close OI) and the direction rule stated above.
  """
  base={'underlying':underlying,'expiry':expiry or None,'session':None,'as_of':None,'spot':None,
   'spot_symbol':None,'atm_strike':None,'atm_basis':None,'marks':[],'contracts':[],'points_source':'none',
   'note':None}
  session=self._grid_session()
  if session is None:
   base['note']='the store has no captured readings yet';return base
  base['session']=session
  # `spot_symbol` is an optional column, so the row is read whole and the field simply read off it.
  row=self._one('select * from underlying_snapshots where underlying=? and substr(captured_at,1,10)=?'
   ' and spot is not null order by captured_at desc limit 1',(underlying,session)) \
   if 'underlying_snapshots' in self._tables() else None
  mark,spot=(row or {}).get('captured_at'),_num((row or {}).get('spot'))
  if row is None or spot is None:
   # Everything in this block hangs off the ATM, and the ATM hangs off the spot.
   base['note']=(f'no spot was captured for {underlying} at any 15-min reading of {session} — the strike at the money '
    'cannot be identified, so no line is drawn')
   return base
  base['as_of'],base['spot'],base['spot_symbol']=mark,_round(spot),row.get('spot_symbol')
  expiry=clean_expiry(expiry) or self._front_expiry(underlying)
  base['expiry']=expiry or None
  if not expiry:
   base['note']=f'no option expiry is listed for {underlying}';return base
  listed=[r for r in self._rows('select instrument_token,tradingsymbol,strike,instrument_type from contracts'
   " where underlying=? and expiry=? and instrument_type in ('CE','PE')",(underlying,expiry))
   if _num(r.get('strike')) is not None]
  if not listed:
   base['note']=f'no option contracts are listed for {underlying} {expiry}';return base
  ladder=sorted({_num(r.get('strike')) for r in listed})
  atm=min(ladder,key=lambda s:(abs(s-spot),-s))
  base['atm_strike'],base['atm_basis']=atm,{'mark':mark,'spot':_round(spot),'rule':'listed strike nearest spot'}
  at=ladder.index(atm)
  wanted=[(atm,'CE',0),(atm,'PE',0)]
  for step in range(1,GRID_WIDTH+1):
   if at+step<len(ladder):wanted.append((ladder[at+step],'CE',step))
   if at-step>=0:wanted.append((ladder[at-step],'PE',-step))
  by_key={(_num(r.get('strike')),r.get('instrument_type')):r for r in listed}
  chosen=[(by_key[(s,t)],off) for s,t,off in wanted if (s,t) in by_key]
  tokens=[t for t in (_int(r.get('instrument_token')) for r,_ in chosen) if t is not None]
  if not tokens:
   base['note']=f'no instrument token is stored for the strikes at the money in {underlying} {expiry}';return base
  previous=self._grid_previous_close_oi(tokens,session)
  captured=self._grid_points(tokens,session,mark)
  base['points_source']=captured['source']
  series={t:[] for t in tokens}
  for point in captured['rows']:
   token=_int(point.get('instrument_token'))
   if token not in series:continue
   oi=_num(point.get('oi'))
   close=previous.get(token)
   series[token].append({'at':point.get('captured_at'),'oi':_int(oi),
    # a gap stays a gap, and a missing previous close stays missing — never measured off the day's first mark
    'delta_oi':None if (oi is None or close is None) else _int(oi-close),
    # the contract's OWN last traded price at this reading. Absent stays absent: never carried forward.
    'price':_round(point.get('price'))})
  contracts=[]
  for contract,offset in chosen:
   token=_int(contract.get('instrument_token'))
   points=series.get(token) or []
   direction,detail=self.grid_direction(points)
   kind=contract.get('instrument_type') or ''
   contracts.append({'tradingsymbol':contract.get('tradingsymbol') or '','instrument_token':token,
    'strike':_num(contract.get('strike')),'option_type':kind,
    'atm_offset':offset,'previous_close_oi':_int(previous.get(token)),'points':points,
    'direction':direction,'direction_detail':detail,'flow':self.grid_flow(kind,points)})
  base['contracts']=contracts
  base['marks']=sorted({p['at'] for c in contracts for p in c['points'] if p.get('at')})
  return base

 def _grid_delegate(self,underlying,expiry=''):
  """D2's own reader, when `market_data.derivatives.read_api` is on this machine's path."""
  module=self.read_module()
  reader=getattr(module,'strike_oi_series',None) if module else None
  connection=self._connect()
  if reader is None or connection is None:return None
  try:
   raw=reader(connection,underlying,expiry=clean_expiry(expiry) or None,width=GRID_WIDTH)
  except TypeError as error:
   LOG.info('derivatives: strike_oi_series not called - %s',error);return None
  except Exception as error:
   LOG.warning('derivatives: strike_oi_series failed (%s); reading the ΔOI grid from the store.',error);return None
  if not isinstance(raw,dict):return None
  out=dict(raw)
  out.setdefault('points_source','read_api')
  return out

 def oi_grid(self,underlying,expiry=''):
  """The owner's ΔOI block: ten small series, five calls at and above the money, five puts at and below it.

  ΔOI is open interest added or removed since the previous close, at each 15-minute mark of the newest captured
  session. Ten slots are ALWAYS returned in the same order, so a strike the exchange does not list reads as a
  named gap instead of shifting the grid under the reader.
  """
  notes={'delta_oi_text':DELTA_OI_TEXT,'atm_text':ATM_TEXT,'direction_text':DIRECTION_TEXT,
   'not_enough_marks':NOT_ENOUGH_MARKS,'direction_lookback_marks':DIRECTION_LOOKBACK_MARKS,
   'flat_fraction':FLAT_FRACTION,'grid_width':GRID_WIDTH,'grid_slots':GRID_SLOTS,
   'price_text':PRICE_TEXT,'flow_text':FLOW_TEXT,'block_text':BLOCK_TEXT,
   'price_flat_fraction':PRICE_FLAT_FRACTION,'block_balance_ratio':BLOCK_BALANCE_RATIO}
  if not underlying:
   return self.envelope(rows=[],underlying='',expiry='',session=None,spot=None,spot_symbol=None,
    atm_strike=None,atm_basis=None,marks=[],total=0,days_to_expiry=None,points_source='none',
    empty_note=None,empty_detail=None,block_read='',**notes)
  raw=self._grid_delegate(underlying,expiry)
  source='metrics_module' if raw is not None else 'store'
  if raw is None:raw=self._grid_from_store(underlying,clean_expiry(expiry))
  found={}
  for contract in raw.get('contracts') or []:
   kind=str(contract.get('option_type') or '').upper()
   offset=_int(contract.get('atm_offset'))
   if kind in OPTION_TYPES and offset is not None:found[(kind,offset)]=contract
  expected=[('CE',n) for n in range(GRID_WIDTH+1)]+[('PE',0)]+[('PE',-n) for n in range(1,GRID_WIDTH+1)]
  expiry_out=clean_expiry(raw.get('expiry')) or ''
  rows,present=[],0
  for kind,offset in expected:
   contract=found.get((kind,offset))
   label=self.grid_label(kind,offset)
   slot={'slot':f'{kind}{offset:+d}','option_type':kind,'atm_offset':offset,'label':label,
    'row':'calls' if kind=='CE' else 'puts','present':contract is not None,'tradingsymbol':None,
    'instrument_token':None,'strike':None,'previous_close_oi':None,'points':[],'direction':'no baseline',
    'direction_detail':{'points_with_delta':0},'marks':0,'marks_with_delta':0,'latest_delta_oi':None,
    'peak_abs_delta_oi':None,'marks_with_price':0,'latest_price':None,
    'flow':{'price_direction':'no baseline','oi_direction':'no baseline','what_label':FLOW_NOT_ENOUGH,
     'meaning':None,'detail':{'points_with_delta':0,'points_with_price':0}},
    'missing_text':None if contract is not None else
     f'{label} is not a listed strike in {underlying}{" "+expiry_out if expiry_out else ""}.'}
   if contract is not None:
    present+=1
    points=[{'at':p.get('at'),'oi':_int(p.get('oi')),'delta_oi':_int(p.get('delta_oi')),
     'price':_round(p.get('price'))} for p in contract.get('points') or []]
    deltas=[p['delta_oi'] for p in points if p['delta_oi'] is not None]
    prices=[p['price'] for p in points if p['price'] is not None]
    direction=contract.get('direction')
    detail=contract.get('direction_detail') or {}
    if direction not in GRID_DIRECTIONS:direction,detail=self.grid_direction(points)
    flow=contract.get('flow')
    # A reader that predates the two-line tile (or serves a flow this module does not recognise) does not get to
    # put an unknown sentence on screen: the reading is recomputed from the very points the tile draws. The
    # WORDING is checked too, not only the shape - a delegate cannot smuggle a label this table does not hold.
    if not isinstance(flow,dict) or flow.get('price_direction') not in GRID_PRICE_DIRECTIONS \
       or flow.get('oi_direction') not in GRID_DIRECTIONS or flow.get('what_label') not in FLOW_WORDINGS:
     flow=self.grid_flow(kind,points)
    slot.update({'tradingsymbol':contract.get('tradingsymbol') or '',
     'instrument_token':_int(contract.get('instrument_token')),'strike':_num(contract.get('strike')),
     'previous_close_oi':_int(contract.get('previous_close_oi')),'points':points,'direction':direction,
     'direction_detail':detail,'marks':len(points),'marks_with_delta':len(deltas),
     'latest_delta_oi':deltas[-1] if deltas else None,
     'peak_abs_delta_oi':max((abs(d) for d in deltas),default=None),
     'marks_with_price':len(prices),'latest_price':prices[-1] if prices else None,'flow':flow})
   rows.append(slot)
  drawable=max((s['marks_with_delta'] for s in rows),default=0)
  # Two marks is the whole rule: one point is a dot, not a line, so the grid says the ONE sentence the owner
  # asked for rather than drawing one - and the reader's own explanation travels beside it as `empty_detail`,
  # so "why" is never lost and "not enough marks yet" is never dressed up as an error.
  thin=drawable<2
  note=NOT_ENOUGH_MARKS if thin else None
  detail=raw.get('note') or None
  if thin:rows=[]
  block=self.grid_block_read(rows)
  return self.envelope(as_of=raw.get('as_of'),source=source,missing=[],rows=rows,underlying=underlying,
   empty_detail=detail,block_read=block,
   expiry=expiry_out,session=raw.get('session'),spot=_round(raw.get('spot')),spot_symbol=raw.get('spot_symbol'),
   atm_strike=_num(raw.get('atm_strike')),atm_basis=raw.get('atm_basis'),marks=list(raw.get('marks') or []),
   total=present,days_to_expiry=days_to_expiry(expiry_out) if expiry_out else None,
   points_source=raw.get('points_source') or 'none',empty_note=note,**notes)
