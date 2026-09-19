"""The Derivative tab's session series: PCR, max pain, implied volatility, the screener, futures build-up.

These five routes are the ones the owner asked for by name - "OI, IV, Max pain, PCR and corresponding delta,
all of it available for both stocks and indexes". They read the D2 worker's `metrics` table, whose real shape
is keyed (scope, metric_key, captured_at); `test_derivatives.py` builds the older shape, so the store here is
the CURRENT one, copied from `db/derivatives.db` rather than imported, so a drift in the capture worker's
schema shows up as a failing test instead of as an empty card.

What is asserted, and why each one is here:

 * an INDEX and a THIN SINGLE STOCK both answer - the owner's ask is explicitly both;
 * a reading the store holds for some other underlying, and not for this one, keeps its slot with null values
   and `gap: true`: the hole is never closed up and nothing is carried forward into it;
 * a missing expiry is an empty series, not a 500 and not another expiry's numbers;
 * one reading is "no baseline" and carries no direction at all - never a direction from a single point;
 * a chain under the contract floor, or with one empty leg, has its figure WITHHELD with a named reason -
   never a put-call ratio resting on three contracts;
 * every implied-volatility rejection reason is reachable and every blank carries its reason;
 * IMPLIED VOLATILITY IS LABELLED COMPUTED on every response, with the model and the rate's provenance;
 * a filter the screener cannot honour is REFUSED with a 400 that names it - never accepted, shown as active,
   and then silently dropped, which is the bug that cost a day;
 * nothing any of them says is a forecast (§5).
"""
import math
import sqlite3
import pytest
from kanida_pilot import derivatives as D
from kanida_pilot import implied_vol as IV
from test_pilot import Evidence,pilot_settings,signup  # noqa: F401
from test_derivatives import client_for,build_store as build_legacy_store  # noqa: F401

SESSION='2026-09-18'
#: Twelve readings of the session. The index has all twelve; the stock stops after the fourth, exactly as the
#: real store does on a day the capture narrowed part-way through.
MARKS=[f'{SESSION} {h:02d}:{m:02d}:00' for h in (9,10,11) for m in (30,45,0,15)][:12]
MARKS=[f'{SESSION} 09:30:00',f'{SESSION} 09:45:00',f'{SESSION} 10:00:00',f'{SESSION} 10:15:00',
 f'{SESSION} 10:30:00',f'{SESSION} 10:45:00',f'{SESSION} 11:00:00',f'{SESSION} 11:15:00',
 f'{SESSION} 11:30:00',f'{SESSION} 11:45:00',f'{SESSION} 12:00:00',f'{SESSION} 12:15:00']
STOCK_MARKS=MARKS[:4]
EXPIRY='2026-09-29'
OTHER_EXPIRY='2026-10-27'

# The CURRENT §2 store, copied not imported. Only the columns these routes read.
SCHEMA='''
CREATE TABLE contracts(instrument_token INTEGER PRIMARY KEY, tradingsymbol TEXT NOT NULL, underlying TEXT
 NOT NULL, instrument_type TEXT NOT NULL, strike REAL NOT NULL DEFAULT 0, expiry TEXT NOT NULL,
 lot_size INTEGER NOT NULL, first_seen TEXT, last_seen TEXT, in_scope INTEGER DEFAULT 1);
CREATE TABLE snapshots(instrument_token INTEGER NOT NULL, captured_at TEXT NOT NULL,
 mark_kind TEXT NOT NULL DEFAULT 'bar_close', last_price REAL, average_price REAL, volume INTEGER, oi INTEGER,
 last_trade_time TEXT, exchange_time TEXT, source TEXT, vendor_id TEXT, fetched_at TEXT, snapshot_id TEXT,
 PRIMARY KEY (instrument_token, captured_at)) WITHOUT ROWID;
CREATE TABLE candles_15m(instrument_token INTEGER NOT NULL, bar_start TEXT NOT NULL, open REAL, high REAL,
 low REAL, close REAL, volume INTEGER, oi INTEGER, PRIMARY KEY (instrument_token, bar_start)) WITHOUT ROWID;
CREATE TABLE underlying_snapshots(underlying TEXT NOT NULL, captured_at TEXT NOT NULL, spot REAL,
 spot_symbol TEXT, fut_price REAL, total_ce_oi INTEGER, total_pe_oi INTEGER, total_ce_volume INTEGER,
 total_pe_volume INTEGER, pcr_oi REAL, pcr_volume REAL, max_pain_strike REAL,
 PRIMARY KEY (underlying, captured_at)) WITHOUT ROWID;
CREATE TABLE metrics(scope TEXT NOT NULL, metric_key TEXT NOT NULL, captured_at TEXT NOT NULL,
 instrument_token INTEGER, tradingsymbol TEXT, underlying TEXT NOT NULL, instrument_type TEXT, strike REAL,
 expiry TEXT, lot_size INTEGER, days_to_expiry INTEGER, last_price REAL, average_price REAL, volume REAL,
 oi REAL, spot REAL, price_change_pct_15m REAL, oi_change_15m REAL, oi_change_pct_15m REAL, buildup_15m TEXT,
 price_change_pct_day REAL, oi_change_day REAL, oi_change_pct_day REAL, buildup_day TEXT, vol_tod_ratio REAL,
 vol_tod_sessions INTEGER, vol_tod_status TEXT, vol_oi_ratio REAL, vol_oi_status TEXT, premium_rs REAL,
 premium_cr REAL, premium_status TEXT, pcr_oi REAL, pcr_volume REAL, pcr_trend TEXT, total_ce_oi REAL,
 total_pe_oi REAL, total_ce_volume REAL, total_pe_volume REAL, max_pain_strike REAL, max_pain_distance REAL,
 max_pain_total_oi REAL, max_pain_status TEXT, fut_oi_avg REAL, fut_oi_vs_avg REAL,
 fut_oi_vs_avg_status TEXT, basis REAL, basis_pct REAL, basis_status TEXT, contracts INTEGER,
 unusual INTEGER NOT NULL DEFAULT 0, unusual_reasons TEXT,
 PRIMARY KEY (scope, metric_key, captured_at));
CREATE INDEX idx_metrics_und_at    ON metrics (underlying, captured_at);
CREATE INDEX idx_metrics_expiry    ON metrics (underlying, expiry, captured_at);
CREATE INDEX idx_metrics_unusual   ON metrics (captured_at, unusual);
CREATE INDEX ix_contracts_underlying ON contracts(underlying, expiry, instrument_type);
CREATE INDEX ix_contracts_symbol     ON contracts(tradingsymbol);
CREATE INDEX ix_snapshots_time     ON snapshots(captured_at);
CREATE INDEX ix_underlying_time    ON underlying_snapshots(captured_at);
CREATE INDEX ix_candles_time       ON candles_15m(bar_start);
'''
#: The real store's own indexes, copied above with the tables, because the query PLANS this file pins are a
#: property of the schema AND its indexes. Without them the planner picks a scan on a twelve-row table and the
#: test would pass or fail for reasons that have nothing to do with the code.

#: (token, tradingsymbol, underlying, type, strike, lot). Two indexes, one liquid stock, one chain so thin the
#: chain floor withholds its figures, and one chain with an empty put leg.
CONTRACTS=[
 (101,'NIFTY26SEP23500CE','NIFTY','CE',23500.0,75),
 (102,'NIFTY26SEP23500PE','NIFTY','PE',23500.0,75),
 (103,'NIFTY26SEP23600CE','NIFTY','CE',23600.0,75),
 (104,'NIFTY26SEP23400PE','NIFTY','PE',23400.0,75),
 (105,'NIFTY26SEPFUT','NIFTY','FUT',0.0,75),
 (201,'RELIANCE26SEP1240CE','RELIANCE','CE',1240.0,500),
 (202,'RELIANCE26SEP1240PE','RELIANCE','PE',1240.0,500),
 (203,'RELIANCE26SEPFUT','RELIANCE','FUT',0.0,500),
 (301,'THINCO26SEP100CE','THINCO','CE',100.0,1000),
 (302,'THINCO26SEP100PE','THINCO','PE',100.0,1000),
 (401,'ONELEG26SEP500CE','ONELEG','CE',500.0,600),
 (402,'ONELEG26SEP500PE','ONELEG','PE',500.0,600),
]


def _chain(connection,underlying,marks,*,contracts,ce_oi,pe_oi,max_pain,spot,expiry=EXPIRY,status='ok'):
 """One underlying's per-expiry metric row at each of `marks`, and its `underlying_snapshots` row beside it."""
 rows=[]
 for index,mark in enumerate(marks):
  ce=ce_oi+index*1000
  # A leg that starts empty STAYS empty, so the one-side-empty rule is exercised at every reading rather
  # than only at the first one.
  pe=(pe_oi+index*2500) if pe_oi else 0
  here=spot+index*2.0
  rows.append(('underlying',f'{underlying}|{expiry}',mark,None,None,underlying,None,None,expiry,None,10,
   None,None,None,None,here,
   (pe/ce if ce else None),(pe/ce*0.8 if ce else None),'rising',
   ce,pe,ce*2.0,pe*2.0,
   max_pain+index*50.0,(max_pain+index*50.0)-here,(ce+pe),status,contracts))
 connection.executemany('insert into metrics(scope,metric_key,captured_at,instrument_token,tradingsymbol,'
  'underlying,instrument_type,strike,expiry,lot_size,days_to_expiry,last_price,average_price,volume,oi,spot,'
  'pcr_oi,pcr_volume,pcr_trend,total_ce_oi,total_pe_oi,total_ce_volume,total_pe_volume,max_pain_strike,'
  'max_pain_distance,max_pain_total_oi,max_pain_status,contracts)'
  ' values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',rows)
 connection.executemany('insert into underlying_snapshots(underlying,captured_at,spot,spot_symbol,fut_price,'
  'total_ce_oi,total_pe_oi,pcr_oi,pcr_volume,max_pain_strike) values(?,?,?,?,?,?,?,?,?,?)',
  [(underlying,mark,spot+i*2.0,underlying,spot+i*2.0+3.0,ce_oi,pe_oi,None,None,None)
   for i,mark in enumerate(marks)])


def _option(connection,token,symbol,underlying,kind,strike,lot,marks,prices,spots,*,expiry=EXPIRY,
  dte=10,last_trade=None,premium_cr=12.0,volume_ratio=8.0,sessions=10):
 """One option contract's per-reading metric rows, and the snapshot rows the staleness gate reads."""
 rows=[]
 for mark,price,spot in zip(marks,prices,spots):
  rows.append(('contract',symbol,mark,token,symbol,underlying,kind,strike,expiry,lot,dte,price,price,
   500000.0,900000.0,spot,premium_cr,'ok',volume_ratio,sessions,'ok',3.2,'ok',0.8,1.4,'Long build-up',
   'Short build-up',1))
 connection.executemany('insert into metrics(scope,metric_key,captured_at,instrument_token,tradingsymbol,'
  'underlying,instrument_type,strike,expiry,lot_size,days_to_expiry,last_price,average_price,volume,oi,spot,'
  'premium_cr,premium_status,vol_tod_ratio,vol_tod_sessions,vol_tod_status,vol_oi_ratio,vol_oi_status,'
  'oi_change_pct_15m,oi_change_pct_day,buildup_15m,buildup_day,unusual)'
  ' values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',rows)
 connection.executemany('insert into snapshots(instrument_token,captured_at,last_price,oi,last_trade_time,'
  "vendor_id,fetched_at,snapshot_id) values(?,?,?,?,?,'kite','x','y')",
  [(token,mark,price,900000,(last_trade or mark)) for mark,price in zip(marks,prices)])


def build_store(path):
 """A store in the CURRENT §2 shape: a full-session index, a stock that stops after four readings, a chain
 under the contract floor, and a chain with an empty put leg."""
 connection=sqlite3.connect(path)
 connection.executescript(SCHEMA)
 connection.executemany('insert into contracts(instrument_token,tradingsymbol,underlying,instrument_type,'
  'strike,expiry,lot_size,first_seen,last_seen) values(?,?,?,?,?,?,?,?,?)',
  [(*c[:4],c[4],EXPIRY,c[5],SESSION,SESSION) for c in CONTRACTS])
 # A second expiry exists for NIFTY so "the front expiry" is a real choice rather than the only one.
 connection.execute('insert into contracts(instrument_token,tradingsymbol,underlying,instrument_type,strike,'
  "expiry,lot_size,first_seen,last_seen) values(?,?,?,?,?,?,?,?,?)",
  (111,'NIFTY26OCT23500CE','NIFTY','CE',23500.0,OTHER_EXPIRY,75,SESSION,SESSION))
 # The index: every reading of the session, a PCR that rises and a max-pain strike that shifts up.
 _chain(connection,'NIFTY',MARKS,contracts=192,ce_oi=5_000_000,pe_oi=4_200_000,max_pain=23500.0,spot=23480.0)
 # The stock: only the first four readings. The other eight are holes the store knows the session had.
 _chain(connection,'RELIANCE',STOCK_MARKS,contracts=86,ce_oi=900_000,pe_oi=480_000,max_pain=1290.0,spot=1244.0)
 # Under the chain floor: four listed contracts. Every figure is withheld, never printed.
 _chain(connection,'THINCO',MARKS,contracts=4,ce_oi=12_000,pe_oi=9_000,max_pain=100.0,spot=99.0)
 # One side with no open interest at all: the ratio is withheld rather than served as a zero or an infinity.
 _chain(connection,'ONELEG',MARKS,contracts=40,ce_oi=800_000,pe_oi=0,max_pain=500.0,spot=498.0)
 # The at-the-money pair of the index, priced so both legs solve, with the premium easing through the day.
 spots=[23480.0+i*2.0 for i in range(len(MARKS))]
 _option(connection,101,'NIFTY26SEP23500CE','NIFTY','CE',23500.0,75,MARKS,
  [240.0-i*6.0 for i in range(len(MARKS))],spots)
 _option(connection,102,'NIFTY26SEP23500PE','NIFTY','PE',23500.0,75,MARKS,
  [225.0-i*5.0 for i in range(len(MARKS))],spots)
 # A call whose last trade is two hours before the reading: the price is not the reading's price.
 _option(connection,103,'NIFTY26SEP23600CE','NIFTY','CE',23600.0,75,MARKS,
  [180.0]*len(MARKS),spots,last_trade=f'{SESSION} 07:15:00')
 # A put priced UNDER its own intrinsic value: the model has no root, so the reading is null with the reason.
 _option(connection,104,'NIFTY26SEP23400PE','NIFTY','PE',23400.0,75,MARKS,[1.0]*len(MARKS),
  [23000.0]*len(MARKS))
 # The stock's own at-the-money pair, on its four readings only.
 stock_spots=[1244.0+i*2.0 for i in range(len(STOCK_MARKS))]
 _option(connection,201,'RELIANCE26SEP1240CE','RELIANCE','CE',1240.0,500,STOCK_MARKS,
  [34.0,33.0,32.0,31.0],stock_spots)
 _option(connection,202,'RELIANCE26SEP1240PE','RELIANCE','PE',1240.0,500,STOCK_MARKS,
  [29.0,28.5,28.0,27.5],stock_spots)
 # The futures contracts, with OI against their own average and a basis that widens.
 for token,symbol,underlying,marks,base,spot0,lot in ((105,'NIFTY26SEPFUT','NIFTY',MARKS,23500.0,23480.0,75),
   (203,'RELIANCE26SEPFUT','RELIANCE',STOCK_MARKS,1247.0,1244.0,500)):
  rows=[]
  for index,mark in enumerate(marks):
   spot=spot0+index*2.0
   price=base+index*2.4
   rows.append(('contract',symbol,mark,token,symbol,underlying,'FUT',0.0,EXPIRY,lot,10,price,price,
    1_000_000.0,9_000_000.0+index*40_000,spot,240.0,'ok',
    9_300_000.0,(9_000_000.0+index*40_000)/9_300_000.0,'ok',price-spot,(price-spot)/spot*100.0,'ok',
    0.4,-0.6,'Long build-up','Short build-up'))
  connection.executemany('insert into metrics(scope,metric_key,captured_at,instrument_token,tradingsymbol,'
   'underlying,instrument_type,strike,expiry,lot_size,days_to_expiry,last_price,average_price,volume,oi,spot,'
   'premium_cr,premium_status,fut_oi_avg,fut_oi_vs_avg,fut_oi_vs_avg_status,basis,basis_pct,basis_status,'
   'oi_change_pct_15m,oi_change_pct_day,buildup_15m,buildup_day)'
   ' values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',rows)
 connection.commit();connection.close()


@pytest.fixture
def store(tmp_path):
 path=tmp_path/'derivatives.db';build_store(path);return str(path)

@pytest.fixture
def reader(store):
 value=D.Derivatives(store);yield value;value.close()


# --- the shape the UI worker builds to -------------------------------------------------------------------
def test_pcr_series_serves_an_index_oldest_first(reader):
 """The contract: points oldest first, each carrying the four numbers, plus a direction over the same window."""
 body=reader.pcr_series('NIFTY')
 assert body['available'] and body['underlying']=='NIFTY' and body['expiry']==EXPIRY
 assert [p['at'] for p in body['points']]==MARKS
 for point in body['points']:
  assert set(('at','pcr_oi','pcr_volume','total_ce_oi','total_pe_oi')) <= set(point)
  assert point['pcr_oi'] is not None and point['withheld'] is None
 assert body['direction'] in D.DIRECTION_WORDS['pcr'].values()
 assert body['direction_detail']['readings_back']==D.DIRECTION_LOOKBACK_MARKS
 # The series was built rising, and a rise of that size is not inside the flat band.
 assert body['direction']=='rising'
 assert body['session']==SESSION and body['as_of']==MARKS[-1]

def test_pcr_series_serves_a_thin_single_stock_and_keeps_its_holes(reader):
 """The owner's ask is BOTH. A reading the store had, and this stock has no row for, keeps its slot."""
 body=reader.pcr_series('RELIANCE')
 assert [p['at'] for p in body['points']]==MARKS,'the series sits on the store\'s own reading grid'
 filled=[p for p in body['points'] if not p['gap']]
 holes=[p for p in body['points'] if p['gap']]
 assert len(filled)==len(STOCK_MARKS) and len(holes)==len(MARKS)-len(STOCK_MARKS)
 for point in holes:
  assert point['pcr_oi'] is None and point['pcr_volume'] is None
  assert point['withheld']=='no_reading'
 assert body['withheld_reasons']['no_reading']==len(holes)
 # Nothing is carried forward: the last real reading's number never appears in a hole.
 assert {p['pcr_oi'] for p in holes}=={None}

def test_a_gap_is_never_interpolated_or_carried_forward(reader):
 """The rule stated as a test: the values in a hole are null, and the hole keeps its place in the order."""
 body=reader.maxpain_series('RELIANCE')
 ats=[p['at'] for p in body['points']]
 assert ats==sorted(ats)==MARKS
 for point in body['points'][len(STOCK_MARKS):]:
  assert point['gap'] and point['max_pain_strike'] is None and point['spot'] is None
  assert point['distance'] is None and point['total_oi'] is None

def test_maxpain_series_direction_is_the_owners_three_words(reader):
 """"Shifting Up · Stable · Shifting Down" - served as DATA; the UI renders his wording."""
 body=reader.maxpain_series('NIFTY')
 assert body['direction']=='shifting_up'
 assert body['direction_label']=='Shifting Up'
 assert set(D.DIRECTION_WORDS['max_pain'].values())=={'shifting_up','shifting_down','stable','no baseline'}
 assert body['direction_labels']['shifting_down']=='Shifting Down'
 assert body['direction_labels']['stable']=='Stable'
 for point in body['points'][:len(MARKS)]:
  assert set(('at','max_pain_strike','spot','distance','total_oi')) <= set(point)
 # The distance convention is stated, and the numbers obey it.
 first=body['points'][0]
 assert first['distance']==pytest.approx(first['max_pain_strike']-first['spot'],abs=1e-6)
 assert 'MINUS spot' in body['distance_definition']

def test_a_missing_expiry_is_an_empty_series_not_an_error_and_not_another_expirys_numbers(reader):
 """An expiry this store has no chain for answers 200 with no points - never the front expiry's figures."""
 for route in (reader.pcr_series,reader.maxpain_series):
  body=route('NIFTY','2030-01-31')
  assert body['available'] and body['expiry']=='2030-01-31'
  assert body['readings_with_value']==0
  assert body['points']==[],'an expiry with no chain serves nothing, never another chain'
  assert body['direction']=='no baseline'
 # And the front expiry's own numbers are real, so the emptiness above is about the expiry and not the store.
 assert reader.pcr_series('NIFTY',EXPIRY)['readings_with_value']==len(MARKS)

def test_one_reading_is_no_baseline_and_carries_no_direction(tmp_path):
 """A single point is a dot, not a line. It never becomes a direction."""
 path=tmp_path/'one.db'
 connection=sqlite3.connect(path);connection.executescript(SCHEMA)
 connection.execute('insert into contracts(instrument_token,tradingsymbol,underlying,instrument_type,strike,'
  'expiry,lot_size,first_seen,last_seen) values(?,?,?,?,?,?,?,?,?)',
  (901,'ONCE26SEP100CE','ONCE','CE',100.0,EXPIRY,1000,SESSION,SESSION))
 _chain(connection,'ONCE',MARKS[:1],contracts=40,ce_oi=500_000,pe_oi=400_000,max_pain=100.0,spot=99.0)
 connection.commit();connection.close()
 reader=D.Derivatives(str(path))
 try:
  for route in (reader.pcr_series,reader.maxpain_series):
   body=route('ONCE')
   assert body['readings_with_value']==1
   assert body['direction']=='no baseline'
   assert body['direction_detail']['readings_with_value']==1
   assert 'from' not in body['direction_detail'],'a single reading names no window'
 finally:reader.close()


# --- the chain floors: withheld, never a figure resting on three contracts --------------------------------
def test_a_chain_under_the_contract_floor_is_withheld_with_a_reason(reader):
 """§5: a number too thin to mean anything is withheld and SAYS SO - it is never printed and then qualified."""
 body=reader.pcr_series('THINCO')
 assert body['readings_with_value']==0
 assert body['withheld_reasons']=={'thin_chain':len(MARKS)}
 assert all(p['pcr_oi'] is None and p['withheld']=='thin_chain' for p in body['points'])
 assert body['chain_floors']['min_contracts']==D.CHAIN_MIN_CONTRACTS
 assert str(D.CHAIN_MIN_CONTRACTS) in body['chain_floors_text']
 assert body['reason_text']['thin_chain']
 pain=reader.maxpain_series('THINCO')
 assert all(p['max_pain_strike'] is None for p in pain['points'])

def test_a_chain_with_one_empty_leg_is_withheld(reader):
 """A ratio against zero open interest is not a ratio. It is withheld with its own reason."""
 body=reader.pcr_series('ONELEG')
 assert body['withheld_reasons']=={'one_side_empty':len(MARKS)}
 assert all(p['pcr_oi'] is None for p in body['points'])


# --- implied volatility: the one computed number ----------------------------------------------------------
def test_iv_is_labelled_computed_with_the_model_and_the_rate_provenance(reader):
 """It must be unmistakable that this is a model output and not something the exchange reported."""
 body=reader.iv_series('NIFTY')
 assert body['computed'] is True
 assert body['exchange_reported'] is False
 assert body['model']==IV.MODEL and 'Black-Scholes' in body['model']
 assert body['method']==IV.METHOD
 assert body['risk_free_rate']==IV.RISK_FREE_RATE
 source=body['risk_free_rate_source']
 assert source['live_feed'] is False
 assert 'implied_vol.py' in source['where']
 assert source['text'] and 'not a rate this server fetched' in source['text']
 assert 'COMPUTED here, not reported by the exchange' in body['computed_text']
 assert body['assumptions']['dividend'] and body['assumptions']['day_count'] and body['assumptions']['style']
 assert body['day_count']=='ACT/365'

def test_iv_serves_an_atm_reading_for_the_underlying_and_a_series_per_strike(reader):
 """"IV" day to day means the at-the-money reading; a strike is served too when one is asked for."""
 body=reader.iv_series('NIFTY')
 assert body['atm_strike']==23500.0
 assert body['atm']['ce']['tradingsymbol']=='NIFTY26SEP23500CE'
 assert body['atm']['pe']['tradingsymbol']=='NIFTY26SEP23500PE'
 assert [p['at'] for p in body['points']]==MARKS
 solved=[p for p in body['points'] if p['iv'] is not None]
 assert len(solved)==len(MARKS)
 for point in solved:
  assert point['basis']=='call and put'
  assert 0.0<point['iv']<3.0,'an annualised volatility, as a fraction'
  assert point['iv_pct']==pytest.approx(point['iv']*100.0,abs=1e-3),'the two are rounded apart'
 assert body['atm']['basis_rule']
 leg=reader.iv_series('NIFTY',strike=23500.0,option_type='CE')['strike']
 assert leg['present'] and leg['tradingsymbol']=='NIFTY26SEP23500CE'
 assert leg['readings_with_value']==len(MARKS)

def test_iv_direction_is_the_owners_three_words(reader):
 """"Expanding · Stable · Cooling"."""
 assert set(D.DIRECTION_WORDS['iv'].values())=={'expanding','cooling','stable','no baseline'}
 body=reader.iv_series('NIFTY')
 assert body['direction'] in ('expanding','cooling','stable','no baseline')
 assert body['direction_labels']['expanding']=='Expanding'
 assert body['direction_labels']['cooling']=='Cooling'

def test_iv_works_on_a_thin_single_stock_and_marks_its_holes(reader):
 """The owner's ask again: both. The stock's eight missing readings are holes, not zeros."""
 body=reader.iv_series('RELIANCE')
 assert body['atm_strike']==1240.0
 assert [p['at'] for p in body['points']]==MARKS
 solved=[p for p in body['points'] if p['iv'] is not None]
 assert len(solved)==len(STOCK_MARKS)
 holes=[p for p in body['points'] if p['gap']]
 assert len(holes)==len(MARKS)-len(STOCK_MARKS)
 assert all(p['reason']=='no_reading' for p in holes)
 assert body['rejections']['no_reading']==2*len(holes),'one per leg'

def test_a_stale_last_trade_is_null_and_says_which(reader):
 """A price that is not the reading's price never becomes a volatility, however well it would solve."""
 body=reader.iv_series('NIFTY',strike=23600.0,option_type='CE')
 leg=body['strike']
 assert leg['present'] and leg['readings_with_value']==0
 assert leg['rejections']=={'stale_last_trade':len(MARKS)}
 for point in leg['points']:
  assert point['iv'] is None and point['reason']=='stale_last_trade'
  assert point['reason_text']==IV.REASONS['stale_last_trade']
  assert point['staleness']=='stale'

def test_a_price_below_intrinsic_is_null_and_says_which(reader):
 """A deep in-the-money put priced at ₹1: the model has no root, so nothing is reported."""
 leg=reader.iv_series('NIFTY',strike=23400.0,option_type='PE')['strike']
 assert leg['readings_with_value']==0
 assert set(leg['rejections'])=={'price_below_intrinsic'}
 assert all(p['reason_text']==IV.REASONS['price_below_intrinsic'] for p in leg['points'])

def test_an_unlisted_strike_says_so_rather_than_answering_with_another_one(reader):
 body=reader.iv_series('NIFTY',strike=99999.0,option_type='CE')
 assert body['strike']['present'] is False
 assert body['strike']['points']==[]
 assert 'not a listed strike' in body['strike']['missing_text']

@pytest.mark.parametrize('reason',sorted(IV.REASONS))
def test_every_iv_rejection_reason_is_reachable_and_has_a_sentence(reason):
 """Every named reason can actually happen, and every one of them prints something a reader can act on."""
 assert IV.REASONS[reason].strip()
 spot,strike,years=23480.0,23500.0,11/365
 floor,_ceiling=IV.bounds(spot,strike,years,IV.RISK_FREE_RATE,'CE')
 cases={
  'expiry_today':dict(price=240.0,spot=spot,strike=strike,years=0.0,option_type='CE',days_to_expiry=0),
  'no_time_value':dict(price=floor,spot=spot,strike=strike,years=years,option_type='CE'),
  'price_below_intrinsic':dict(price=1.0,spot=spot,strike=23000.0,years=years,option_type='CE'),
  'price_above_upper_bound':dict(price=spot+1.0,spot=spot,strike=strike,years=years,option_type='CE'),
  'stale_last_trade':dict(price=240.0,spot=spot,strike=strike,years=years,option_type='CE',
   seconds_since_last_trade=IV.STALE_SECONDS+1),
  'outside_bracket':dict(price=spot*0.999,spot=spot,strike=40000.0,years=years,option_type='CE'),
  'missing_price':dict(price=None,spot=spot,strike=strike,years=years,option_type='CE'),
  'missing_spot':dict(price=240.0,spot=None,strike=strike,years=years,option_type='CE'),
  'missing_strike':dict(price=240.0,spot=spot,strike=None,years=years,option_type='CE'),
  'missing_expiry':dict(price=240.0,spot=spot,strike=strike,years=None,option_type='CE'),
  'non_positive_price':dict(price=0.0,spot=spot,strike=strike,years=years,option_type='CE'),
  'unknown_option_type':dict(price=240.0,spot=spot,strike=strike,years=years,option_type='FUT'),
 }
 if reason=='no_reading':
  # Not a failure of the maths: the reader raises it for a reading the store has no row for, and
  # test_iv_works_on_a_thin_single_stock_and_marks_its_holes exercises it end to end.
  return
 if reason=='no_convergence':
  # Defence in depth: MAX_ITERATIONS is far above what the tolerance needs, so it is reached only if the
  # loop is capped. Cap it and the solver reports a blank rather than a half-solved number.
  cap=IV.MAX_ITERATIONS
  try:
   IV.MAX_ITERATIONS=2
   assert IV.solve(240.0,spot,strike,years,'CE')['reason']=='no_convergence'
  finally:IV.MAX_ITERATIONS=cap
  return
 out=IV.solve(**cases[reason])
 assert out['iv'] is None and out['reason']==reason,(reason,out)

def test_the_solver_returns_the_volatility_it_was_priced_at(reader):
 """The one correctness check that matters: price at a known volatility, solve it back."""
 for kind in ('CE','PE'):
  for sigma in (0.08,0.145,0.32,0.90):
   for years in (1/365,11/365,90/365):
    price=IV.price_bs(23480.0,23500.0,years,IV.RISK_FREE_RATE,sigma,kind)
    solved=IV.solve(price,23480.0,23500.0,years,kind)
    assert solved['iv']==pytest.approx(sigma,abs=1e-5),(kind,sigma,years,solved)

def test_a_null_iv_always_carries_a_reason_and_a_solved_one_never_does(reader):
 """The invariant: no blank without a reason, no number with one."""
 for body in (reader.iv_series('NIFTY'),reader.iv_series('RELIANCE'),
   reader.iv_series('NIFTY',strike=23400.0,option_type='PE')):
  legs=[body['points']]
  if body.get('strike') and body['strike'].get('points'):legs.append(body['strike']['points'])
  for points in legs:
   for point in points:
    assert (point['iv'] is None)==(point['reason'] is not None),point


# --- the screener: applied or refused, never silently dropped ---------------------------------------------
def test_the_screener_reports_exactly_what_it_applied(reader):
 body=reader.screener({'option_type':'CE','max_dte':30})
 keys={row['key'] for row in body['applied']}
 assert {'at','min_premium_cr','min_last_price','min_oi_lots','option_type','max_dte'} <= keys
 for row in body['applied']:assert row['text'] and 'always' in row
 # The §3 floors are ALWAYS on and are marked as such.
 floors={row['key'] for row in body['applied'] if row['always']}
 assert {'min_premium_cr','min_last_price','min_oi_lots'} <= floors
 assert body['scanned'] is not None and body['total']==len(body['rows'])
 assert all(row['instrument_type']=='CE' for row in body['rows'])

def test_min_premium_cr_can_raise_the_floor_but_never_lower_it(reader):
 body=reader.screener({'min_premium_cr':0.1})
 applied={row['key']:row['value'] for row in body['applied']}
 assert applied['min_premium_cr']==D.FLOOR_PREMIUM_CR
 raised=reader.screener({'min_premium_cr':500.0})
 assert {row['key']:row['value'] for row in raised['applied']}['min_premium_cr']==500.0
 assert raised['rows']==[]

@pytest.mark.parametrize('bad,fragment',[
 ({'nonsense':'1'},'not a filter'),
 ({'at':'2026-01-01 09:30:00'},'not a 15-min reading'),
 ({'moneyness':'sideways'},'must be one of'),
 ({'underlying_kind':'commodity'},'must be one of'),
 ({'option_type':'XX'},'must be one of'),
 ({'buildup':'Bull'},'must be one of'),
 ({'buildup_window':'weekly'},'must be one of'),
 ({'min_dte':999},'between'),
 ({'max_oi_change_day_pct':'abc'},'between'),
 ({'underlying':'; drop table metrics'},'traded symbol'),
 ({'expiry':'not-a-date'},'YYYY-MM-DD'),
])
def test_a_filter_the_screener_cannot_honour_is_refused_not_ignored(reader,bad,fragment):
 """The bug that cost a day: a filter shown as active while it silently deleted every row. Never again."""
 with pytest.raises(ValueError) as error:reader.screener(bad)
 assert fragment in str(error.value)

def test_a_filter_whose_column_the_store_lacks_is_refused_and_marked_not_ready(tmp_path):
 """A store without the volume baseline cannot honour the volume-ratio filter, and says so both ways."""
 path=tmp_path/'bare.db'
 connection=sqlite3.connect(path)
 connection.executescript(SCHEMA.replace(' vol_tod_ratio REAL,\n vol_tod_sessions INTEGER,',' '))
 connection.execute('insert into contracts(instrument_token,tradingsymbol,underlying,instrument_type,strike,'
  'expiry,lot_size,first_seen,last_seen) values(?,?,?,?,?,?,?,?,?)',
  (101,'NIFTY26SEP23500CE','NIFTY','CE',23500.0,EXPIRY,75,SESSION,SESSION))
 connection.commit();connection.close()
 reader=D.Derivatives(str(path))
 try:
  ready={row['key']:row for row in reader.screener_available()}
  assert ready['min_volume_ratio']['ready'] is False
  assert ready['min_volume_ratio']['missing_columns']
  with pytest.raises(ValueError) as error:reader.screener({'min_volume_ratio':3})
  assert 'cannot be applied' in str(error.value)
 finally:reader.close()

def test_the_screener_lists_what_can_be_filtered_on(reader):
 available={row['key']:row for row in reader.screener_available()}
 for key in ('min_premium_cr','min_volume_ratio','min_volume_to_oi','min_oi_change_15m_pct',
   'min_oi_change_day_pct','buildup','min_dte','max_dte','option_type','moneyness','underlying_kind'):
  assert key in available and available[key]['ready'] is True,key
  assert available[key]['text']

def test_the_screener_says_how_wide_the_reading_it_read_was(reader):
 """A reading that covered one underlying must not read as a market with one name in it."""
 body=reader.screener({})
 assert body['coverage']['underlyings']>=1
 assert body['readings'] and body['readings'][0]['at']==body['as_of']
 assert body['scanned']==body['coverage']['rows']

def test_the_screener_never_serves_a_ratio_without_its_baseline(tmp_path):
 """§3.2 on the serving side: fewer than 3 sessions is "no baseline", never a number."""
 path=tmp_path/'thin-baseline.db'
 connection=sqlite3.connect(path);connection.executescript(SCHEMA)
 connection.execute('insert into contracts(instrument_token,tradingsymbol,underlying,instrument_type,strike,'
  'expiry,lot_size,first_seen,last_seen) values(?,?,?,?,?,?,?,?,?)',
  (101,'NIFTY26SEP23500CE','NIFTY','CE',23500.0,EXPIRY,75,SESSION,SESSION))
 _option(connection,101,'NIFTY26SEP23500CE','NIFTY','CE',23500.0,75,MARKS[:1],[240.0],[23480.0],
  volume_ratio=9.9,sessions=2)
 connection.commit();connection.close()
 reader=D.Derivatives(str(path))
 try:
  row=reader.screener({})['rows'][0]
  assert row['volume_ratio'] is None and row['volume_baseline']=='none'
  assert row['volume_baseline_sessions']==2
  # And the filter cannot smuggle it back in.
  assert reader.screener({'min_volume_ratio':1.0})['rows']==[]
 finally:reader.close()

def test_moneyness_and_underlying_kind_are_computed_from_the_same_reading(reader):
 body=reader.screener({'underlying_kind':'index'})
 assert body['rows'] and all(row['underlying_kind']=='index' for row in body['rows'])
 assert all(row['underlying'] in D.INDEX_KINDS for row in body['rows'])
 stock=reader.screener({'underlying_kind':'stock'})
 assert all(row['underlying'] not in D.INDEX_KINDS for row in stock['rows'])
 atm=reader.screener({'moneyness':'atm'})
 for row in atm['rows']:
  assert abs(row['strike']-row['spot'])<=D.MONEYNESS_BAND*row['spot']

def test_the_buildup_filter_never_mixes_its_two_windows(reader):
 """§3.1: the 15-minute and the day-on-day label are different statements and are never conflated."""
 fifteen=reader.screener({'buildup':'Long build-up','buildup_window':'15m'})
 assert fifteen['buildup_window']=='15m'
 assert all(row['buildup_15m']=='Long build-up' for row in fifteen['rows'])
 day=reader.screener({'buildup':'Long build-up','buildup_window':'day'})
 assert day['rows']==[],'the fixture writes "Short build-up" on the day window'
 kept=reader.screener({'buildup':'Short build-up','buildup_window':'day'})
 assert kept['rows'] and all(row['buildup_day']=='Short build-up' for row in kept['rows'])
 for row in kept['rows']:assert row['buildup_window']=='day' and row['buildup']==row['buildup_day']


# --- futures build-up -------------------------------------------------------------------------------------
def test_futures_buildup_serves_oi_against_its_own_average_and_the_basis(reader):
 body=reader.futures_buildup('NIFTY')
 assert body['contract']['tradingsymbol']=='NIFTY26SEPFUT'
 assert [p['at'] for p in body['points']]==MARKS
 for point in body['points']:
  assert point['oi_vs_avg'] is not None and point['basis'] is not None and point['basis_pct'] is not None
 assert body['oi_direction'] in D.DIRECTION_WORDS['oi'].values()
 assert body['basis_direction'] in D.DIRECTION_WORDS['basis'].values()
 assert body['oi_direction']=='building' and body['basis_direction']=='widening'
 assert body['oi_direction_detail']['readings_back']==D.DIRECTION_LOOKBACK_MARKS

def test_futures_buildup_on_a_thin_stock_keeps_its_holes(reader):
 body=reader.futures_buildup('RELIANCE')
 assert len([p for p in body['points'] if not p['gap']])==len(STOCK_MARKS)
 for point in body['points'][len(STOCK_MARKS):]:
  assert point['gap'] and point['oi_vs_avg'] is None and point['basis'] is None

def test_an_underlying_with_no_future_says_so_rather_than_drawing_nothing(reader):
 body=reader.futures_buildup('THINCO')
 assert body['points']==[]
 assert 'no futures contract' in body['empty_note']


# --- degradation and honesty ------------------------------------------------------------------------------
def test_every_new_route_answers_on_a_store_that_does_not_exist(tmp_path):
 """No store at all: 200-shaped bodies with `available: False`. Never an exception, never an error page."""
 reader=D.Derivatives(str(tmp_path/'missing.db'))
 try:
  for body in (reader.pcr_series('NIFTY'),reader.maxpain_series('NIFTY'),reader.iv_series('NIFTY'),
    reader.futures_buildup('NIFTY'),reader.screener({})):
   assert body['available'] is False
   assert body['empty_reason']==D.EMPTY_TEXT
 finally:reader.close()

def test_every_new_route_answers_on_the_older_store_shape(tmp_path):
 """A store written before `scope` existed still answers - from `underlying_snapshots` - and says which."""
 path=tmp_path/'legacy.db';build_legacy_store(path)
 reader=D.Derivatives(str(path))
 try:
  assert reader._has_scope() is False
  pcr=reader.pcr_series('NIFTY')
  assert pcr['available'] and pcr['series_source']=='underlying_snapshots'
  assert pcr['readings_with_value']>=1
  pain=reader.maxpain_series('NIFTY')
  assert pain['readings_with_value']>=1
  assert pcr['expiry'] is None and 'all expiries' in pcr['expiry_basis']
  for body in (reader.iv_series('NIFTY'),reader.futures_buildup('NIFTY'),reader.screener({})):
   assert body['available'] is True,'`available` stays a boolean on every route'
 finally:reader.close()

BANNED=('will','expect','expected','forecast','predict','prediction','likely','should rise','should fall',
 'target price','support level','resistance level','breakout','overbought','oversold','bullish','bearish',
 'buy signal','sell signal','uptrend','downtrend','rally','reversal','momentum')

def _strings(value):
 if isinstance(value,str):yield value
 elif isinstance(value,dict):
  for item in value.values():yield from _strings(item)
 elif isinstance(value,(list,tuple)):
  for item in value:yield from _strings(item)

def test_no_route_says_anything_about_what_happens_next(reader):
 """§5. Every string these five routes can put on screen, against the check script's own forbidden list."""
 bodies=[reader.pcr_series('NIFTY'),reader.pcr_series('RELIANCE'),reader.maxpain_series('NIFTY'),
  reader.maxpain_series('THINCO'),reader.iv_series('NIFTY'),reader.iv_series('NIFTY',strike=23500.0,
  option_type='CE'),reader.futures_buildup('NIFTY'),reader.screener({}),
  reader.screener({'option_type':'PE'})]
 said=[]
 for body in bodies:
  for text in _strings(body):
   lowered=text.lower()
   said+= [f'{word}: {text[:90]}' for word in BANNED if f' {word} ' in f' {lowered} '
    or lowered.startswith(f'{word} ') or lowered.endswith(f' {word}')]
 assert said==[],'the tab never says what happens next: '+' | '.join(sorted(set(said))[:5])

def test_nothing_computed_is_written_back_to_the_store(reader,store):
 """The store is opened read-only, and the implied volatility never reaches a column beside a reported one."""
 reader.iv_series('NIFTY')
 connection=sqlite3.connect(f'file:{store}?mode=ro',uri=True)
 try:
  columns={row[1] for row in connection.execute('PRAGMA table_info("metrics")')}
  assert not [c for c in columns if 'iv' in c.split('_') or 'implied' in c]
 finally:connection.close()


# --- the HTTP surface the UI worker calls -----------------------------------------------------------------
def test_the_routes_are_mounted_and_shaped(tmp_path,store):
 app,client=client_for(tmp_path,derivatives_database=store)
 try:
  pcr=client.get('/api/derivatives/pcr-series?underlying=NIFTY').json()
  assert pcr['points'] and pcr['direction']=='rising'
  pain=client.get(f'/api/derivatives/maxpain-series?underlying=RELIANCE&expiry={EXPIRY}').json()
  assert pain['underlying']=='RELIANCE' and pain['expiry']==EXPIRY
  iv=client.get('/api/derivatives/iv-series?underlying=NIFTY').json()
  assert iv['computed'] is True and iv['atm']['ce']['tradingsymbol']
  strike=client.get('/api/derivatives/iv-series?underlying=NIFTY&strike=23500&option_type=CE').json()
  assert strike['strike']['present'] is True
  futures=client.get('/api/derivatives/futures-buildup?underlying=NIFTY').json()
  assert futures['contract']['tradingsymbol']=='NIFTY26SEPFUT'
  screen=client.get('/api/derivatives/screener?option_type=CE&limit=5').json()
  assert screen['applied'] and screen['available']
  assert len(screen['rows'])<=5
 finally:app.state.db.close()

def test_the_http_screener_refuses_a_filter_it_does_not_offer(tmp_path,store):
 """A parameter FastAPI would otherwise drop on the floor is a 400 that names it."""
 app,client=client_for(tmp_path,derivatives_database=store)
 try:
  response=client.get('/api/derivatives/screener?min_sample_size=30')
  assert response.status_code==400
  body=response.json()
  assert body['code']=='FILTER_REFUSED' and 'min_sample_size' in body['error']
  assert client.get('/api/derivatives/screener?moneyness=sideways').status_code==400
  assert client.get('/api/derivatives/screener?limit=99999').status_code==400
 finally:app.state.db.close()

def test_the_http_routes_refuse_a_half_given_strike(tmp_path,store):
 """A strike with no option type (or the other way round) is a refusal, not a silently ignored parameter."""
 app,client=client_for(tmp_path,derivatives_database=store)
 try:
  assert client.get('/api/derivatives/iv-series?underlying=NIFTY&strike=23500').status_code==400
  assert client.get('/api/derivatives/iv-series?underlying=NIFTY&option_type=CE').status_code==400
  assert client.get('/api/derivatives/pcr-series').status_code==400
  assert client.get('/api/derivatives/maxpain-series?underlying=NIFTY&expiry=nope').status_code==400
 finally:app.state.db.close()


# --- the measured requirement: these routes read precomputed rows through an index, never a scan -----------
# "Light speed" is a measured requirement, not an adjective, so it is pinned here. Timings belong in the
# benchmark, not in a test suite that runs on whatever machine is free; what a test CAN hold is the shape of
# the query, which is what the timings actually come from. Each of these was a measured regression once:
#   * the expiry list as `select distinct expiry from metrics where scope='underlying' and underlying=?`
#     read every per-underlying row in the store - 23 ms a call, growing every session;
#   * the screener's reading list as `count(distinct underlying) ... group by captured_at` over `metrics`
#     read all 265,552 contract rows through two temporary B-trees - 612 ms a call;
#   * the tab header's session count as `count(distinct substr(bar_start,1,10))` read all 2,675,883 candle
#     rows - 416 ms a call.
def _plans(reader,sql,params=()):
 connection=reader._connect()
 return [row[3] for row in connection.execute('explain query plan '+sql,params)]

def test_the_series_read_the_metrics_primary_key_not_a_scan(reader):
 """A whole session of one series is ONE index seek of a few dozen rows, whatever the store's size."""
 plans=_plans(reader,"select captured_at,pcr_oi from metrics where scope='underlying' and metric_key=?"
  ' order by captured_at',('NIFTY|'+EXPIRY,))
 assert any('SEARCH' in p and 'metric_key=?' in p for p in plans),plans
 assert not any(p.startswith('SCAN metrics') for p in plans),plans

def test_the_expiry_list_never_reads_every_per_underlying_row(reader):
 """The candidates come from `contracts` through its own index, then one primary-key seek each."""
 plans=_plans(reader,"select distinct expiry from contracts where underlying=?"
  " and instrument_type in ('CE','PE') order by expiry limit ?",('NIFTY',24))
 assert any('SEARCH contracts' in p for p in plans),plans
 seek=_plans(reader,"select 1 as ok from metrics where scope='underlying' and metric_key=? limit 1",
  ('NIFTY|'+EXPIRY,))
 assert any('metric_key=?' in p for p in seek),seek
 # The October chain is LISTED but has no captured metric row, so it is not offered as a series.
 assert reader._underlying_expiries('NIFTY')==[EXPIRY]

def test_the_screener_reading_list_never_groups_the_whole_metrics_table(reader):
 """Coverage comes from `underlying_snapshots`, which is one small row per underlying per reading."""
 plans=_plans(reader,'select captured_at,count(*) as underlyings from underlying_snapshots'
  ' group by captured_at order by captured_at desc limit ?',(40,))
 assert not any('metrics' in p for p in plans),plans
 readings=reader.screener_readings()
 assert readings and readings[0]['at']==MARKS[-1]
 assert readings[0]['underlyings']==3,'NIFTY, THINCO and ONELEG wrote a row there; the stock had stopped'
 # And the ROW count is taken for the one reading served, through a covering index.
 plans=_plans(reader,'select count(*) as n from metrics where captured_at=?',(MARKS[-1],))
 assert any('SEARCH' in p for p in plans),plans

def test_the_tab_header_counts_its_sessions_by_walking_the_index(reader):
 """Same answer as count(distinct substr(...)), without reading every candle row."""
 body=reader.status()
 connection=reader._connect()
 naive=connection.execute('select count(distinct substr(bar_start,1,10)) from candles_15m').fetchone()[0]
 assert body['backfill_sessions']==naive
