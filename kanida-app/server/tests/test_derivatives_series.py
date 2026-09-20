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
import pathlib
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
 body=reader.screener({'option_type':'CE','max_dte':30,'group':'contract'})
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
  row=reader.screener({'group':'contract'})['rows'][0]
  assert row['volume_ratio'] is None and row['volume_baseline']=='none'
  assert row['volume_baseline_sessions']==2
  # And the filter cannot smuggle it back in - in either view.
  assert reader.screener({'min_volume_ratio':1.0,'group':'contract'})['rows']==[]
  assert reader.screener({'min_volume_ratio':1.0})['rows']==[]
 finally:reader.close()

def test_moneyness_and_underlying_kind_are_computed_from_the_same_reading(reader):
 body=reader.screener({'underlying_kind':'index','group':'contract'})
 assert body['rows'] and all(row['underlying_kind']=='index' for row in body['rows'])
 assert all(row['underlying'] in D.INDEX_KINDS for row in body['rows'])
 stock=reader.screener({'underlying_kind':'stock','group':'contract'})
 assert all(row['underlying'] not in D.INDEX_KINDS for row in stock['rows'])
 atm=reader.screener({'moneyness':'atm','group':'contract'})
 for row in atm['rows']:
  assert abs(row['strike']-row['spot'])<=D.MONEYNESS_BAND*row['spot']
 # the row-level filters narrow the UNDERLYING view too - it is built from the rows that survived them
 grouped=reader.screener({'underlying_kind':'index'})
 assert grouped['rows'] and all(row['underlying'] in D.INDEX_KINDS for row in grouped['rows'])

def test_the_buildup_filter_never_mixes_its_two_windows(reader):
 """§3.1: the 15-minute and the day-on-day label are different statements and are never conflated."""
 fifteen=reader.screener({'group':'contract','buildup':'Long build-up','buildup_window':'15m'})
 assert fifteen['buildup_window']=='15m'
 assert all(row['buildup_15m']=='Long build-up' for row in fifteen['rows'])
 day=reader.screener({'group':'contract','buildup':'Long build-up','buildup_window':'day'})
 assert day['rows']==[],'the fixture writes "Short build-up" on the day window'
 kept=reader.screener({'group':'contract','buildup':'Short build-up','buildup_window':'day'})
 assert kept['rows'] and all(row['buildup_day']=='Short build-up' for row in kept['rows'])
 for row in kept['rows']:assert row['buildup_window']=='day' and row['buildup']==row['buildup_day']

# --- P06: what "unusual" counts, and the order it drives ---------------------------------------------------
#
# The screener drew "Unusual · 3 conditions · 80 contracts" on NIFTY at the 11:30 reading of 18 Sep 2026. It
# had tallied the store's complete reason SENTENCE, and a sentence carries its own multiple - "volume 206.0x
# its own time-of-day median" and "volume 781.9x its own time-of-day median" are ONE RULE at two contracts.
# 80 flagged contracts wrote 124 different sentences, and the badge clamped 124 to three.
#
# These build one small store whose contracts trip the SAME rule at many different multiples, which is the
# exact shape that produced the bug.
#: The two sentences the store writes, at whatever multiple the contract reported.
def _vol_tod(value):return f'volume {value:.1f}x its own time-of-day median'
def _day_vol(value):return f"day volume {value:.1f}x yesterday's OI"


def _unusual_store(path,rows):
 """A store whose `metrics` table carries the two baseline columns the real one has.

 `rows` are (symbol, underlying, type, strike, premium_cr, vol_tod_ratio, vol_oi_ratio, reasons).
 """
 schema=SCHEMA.replace('vol_tod_sessions INTEGER','vol_tod_median REAL, vol_tod_sessions INTEGER')\
  .replace('vol_oi_ratio REAL','vol_oi_ratio REAL, vol_oi_prev_oi REAL')
 connection=sqlite3.connect(path);connection.executescript(schema)
 for token,(symbol,underlying,kind,strike,premium,tod,voi,reasons) in enumerate(rows,start=900):
  connection.execute('insert into contracts(instrument_token,tradingsymbol,underlying,instrument_type,strike,'
   'expiry,lot_size,first_seen,last_seen) values(?,?,?,?,?,?,?,?,?)',
   (token,symbol,underlying,kind,strike,EXPIRY,75,SESSION,SESSION))
  connection.execute('insert into metrics(scope,metric_key,captured_at,instrument_token,tradingsymbol,'
   'underlying,instrument_type,strike,expiry,lot_size,days_to_expiry,last_price,average_price,volume,oi,spot,'
   'premium_cr,premium_status,vol_tod_ratio,vol_tod_median,vol_tod_sessions,vol_tod_status,vol_oi_ratio,'
   'vol_oi_prev_oi,vol_oi_status,buildup_day,unusual,unusual_reasons)'
   " values('contract',?,?,?,?,?,?,?,?,?,10,240.0,240.0,500000.0,900000.0,23480.0,?,'ok',?,1200.0,10,'ok',?,"
   "4000.0,'ok','Long build-up',?,?)",
   (symbol,MARKS[0],token,symbol,underlying,kind,strike,EXPIRY,75,premium,tod,voi,
    1 if reasons else 0,','.join(reasons)))
  connection.execute('insert into snapshots(instrument_token,captured_at,last_price,oi,last_trade_time,'
   "vendor_id,fetched_at,snapshot_id) values(?,?,240.0,900000,?,'kite','x','y')",(token,MARKS[0],MARKS[0]))
 connection.execute('insert into underlying_snapshots(underlying,captured_at,spot,spot_symbol,fut_price,'
  'total_ce_oi,total_pe_oi,pcr_oi,pcr_volume,max_pain_strike) values(?,?,?,?,?,?,?,?,?,?)',
  ('NIFTY',MARKS[0],23480.0,'NIFTY',23483.0,100,100,None,None,None))
 connection.commit();connection.close()
 return D.Derivatives(str(path))


def test_numeric_variants_of_one_rule_are_one_condition(tmp_path):
 """THE AUDIT'S CASE: many multiples of the same two rules are TWO conditions, never many."""
 rows=[('NIFTY26SEP23500CE','NIFTY','CE',23500.0,40.0,206.0,28.3,[_vol_tod(206.0),_day_vol(28.3)]),
  ('NIFTY26SEP23600CE','NIFTY','CE',23600.0,30.0,781.9,23.0,[_vol_tod(781.9),_day_vol(23.0)]),
  ('NIFTY26SEP23400PE','NIFTY','PE',23400.0,20.0,73.1,35.9,[_vol_tod(73.1),_day_vol(35.9)]),
  ('NIFTY26SEP23300PE','NIFTY','PE',23300.0,10.0,34.3,None,[_vol_tod(34.3)])]
 reader=_unusual_store(tmp_path/'unusual.db',rows)
 try:
  group=reader.screener({})['rows'][0]
  # SEVEN distinct reason SENTENCES across four contracts...
  assert len({reason for _,_,_,_,_,_,_,reasons in rows for reason in reasons})==7
  # ...and TWO rules. This number could never be three, because there is no third rule to trip.
  assert group['unusual_rule_count']==2
  assert group['unusual_rule_count']<3
  assert [rule['rule_id'] for rule in group['unusual_rules']]==[D.RULE_VOL_TOD,D.RULE_DAY_VOL_VS_PREV_OI]
  # THREE DIFFERENT NUMBERS, kept apart: rules, contracts, firings.
  assert group['unusual']==4,'contracts flagged'
  assert group['unusual_observations']==7,'times a rule fired'
  assert [rule['contracts'] for rule in group['unusual_rules']]==[4,3]
  assert [rule['observations'] for rule in group['unusual_rules']]==[4,3]
  # the old sentence tally is gone from the row entirely
  assert 'unusual_reasons' not in group
 finally:reader.close()


def test_a_trigger_carries_its_rule_its_comparison_and_its_baseline(tmp_path):
 """Not prose: rule id, version, value, comparator, threshold, baseline and sample count."""
 reader=_unusual_store(tmp_path/'trigger.db',
  [('NIFTY26SEP23500CE','NIFTY','CE',23500.0,40.0,206.0,28.3,[_vol_tod(206.0),_day_vol(28.3)])])
 try:
  row=reader.screener({'group':'contract'})['rows'][0]
  tod,voi=row['unusual_triggers']
  assert tod=={'rule_id':D.RULE_VOL_TOD,'rule_version':D.UNUSUAL_RULES_VERSION,'value':206.0,
   'comparator':'>=','threshold':D.UNUSUAL_VOL_TOD_RATIO,'baseline':1200.0,'sample_count':10,'unit':'x'}
  assert voi=={'rule_id':D.RULE_DAY_VOL_VS_PREV_OI,'rule_version':D.UNUSUAL_RULES_VERSION,'value':28.3,
   'comparator':'>','threshold':D.VOL_OI_SPIKE_RATIO,'baseline':4000.0,'sample_count':1,'unit':'x'}
  # a sentence a RULE wrote is not repeated on the trigger: it is that rule at that value, and both are here
  assert 'text' not in tod and 'text' not in voi
  # the store's own words are kept beside the structure, not replaced by it
  assert row['unusual_reasons']==f'{_vol_tod(206.0)},{_day_vol(28.3)}'
  # and the same numbers reach the instrument row, as a MAXIMUM with the contract named - never a mean
  group=reader.screener({})['rows'][0]
  peak=group['unusual_rules'][0]
  assert peak['value_max']==206.0 and peak['value_max_symbol']=='NIFTY26SEP23500CE'
  assert peak['baseline_at_max']==1200.0 and peak['sample_count_at_max']==10
  # THE EVIDENCE ITSELF travels with the row, so the drawer cannot show a different reading
  assert [c['tradingsymbol'] for c in group['unusual_contracts']]==['NIFTY26SEP23500CE']
  assert group['unusual_contracts'][0]['triggers']==[tod,voi]
 finally:reader.close()


def test_a_condition_this_build_does_not_name_stays_itself(tmp_path):
 """A sentence no rule claims is counted on its own - never folded into a rule that did not fire."""
 reader=_unusual_store(tmp_path/'odd.db',
  [('NIFTY26SEP23500CE','NIFTY','CE',23500.0,40.0,206.0,None,[_vol_tod(206.0),'something new'])])
 try:
  group=reader.screener({})['rows'][0]
  assert [rule['rule_id'] for rule in group['unusual_rules']]==[D.RULE_VOL_TOD,D.RULE_UNCLASSIFIED]
  # a TALLY carries the rule's id and its numbers; the words for every rule are served once per response
  odd=group['unusual_rules'][1]
  assert set(odd)=={'rule_id','rule_version','contracts','observations','value_max','value_max_symbol',
   'baseline_at_max','sample_count_at_max'}
  assert odd['contracts']==1 and odd['value_max'] is None
  assert [rule['rule_id'] for rule in reader.screener({})['unusual_rules']]==[
   D.RULE_VOL_TOD,D.RULE_DAY_VOL_VS_PREV_OI]
  assert group['unusual_contracts'][0]['triggers'][1]['text']=='something new'
  assert group['unusual_contracts'][0]['triggers'][1]['value'] is None,'no number is invented for it'
 finally:reader.close()


def test_the_screener_order_is_deterministic_and_says_what_it_is(tmp_path):
 """Most distinct conditions, then most contracts flagged, then premium, then the NAME."""
 rows=[
  # two conditions, one contract, small premium
  ('AAA26SEP100CE','AAA','CE',100.0,5.0,206.0,28.3,[_vol_tod(206.0),_day_vol(28.3)]),
  # one condition, one contract, huge premium - and it must NOT come first
  ('BIG26SEP100CE','BIG','CE',100.0,900.0,206.0,None,[_vol_tod(206.0)]),
  # a perfect tie with CCC on all three counts: only the name separates them
  ('DDD26SEP100CE','DDD','CE',100.0,50.0,206.0,None,[_vol_tod(206.0)]),
  ('CCC26SEP100CE','CCC','CE',100.0,50.0,206.0,None,[_vol_tod(206.0)]),
 ]
 reader=_unusual_store(tmp_path/'order.db',rows)
 try:
  body=reader.screener({})
  assert [row['underlying'] for row in body['rows']]==['AAA','BIG','CCC','DDD']
  # the tie is broken by the name, so the SAME query gives the SAME order every time
  assert [row['underlying'] for row in reader.screener({})['rows']]==['AAA','BIG','CCC','DDD']
  # AND THE RESPONSE SAYS WHAT THE ORDER IS. The page used to print "Busiest by premium" over this list.
  ranking=body['ranking']
  assert ranking['view']=='underlying' and ranking['label']==D.SCREENER_RANK_LABEL
  assert [key['field'] for key in ranking['keys']]==['unusual_rule_count','unusual','premium_cr','underlying']
  assert [key['direction'] for key in ranking['keys']]==['desc','desc','desc','asc']
  assert all(key['text'] for key in ranking['keys']),'every key says what it is, in words'
  # the contract list is a different order and says so rather than borrowing this one
  contracts=reader.screener({'group':'contract'})
  assert contracts['ranking']['label']==D.CONTRACT_RANK_LABEL
  assert [key['field'] for key in contracts['ranking']['keys']]==['premium_cr']
  assert [row['underlying'] for row in contracts['rows']][0]=='BIG','largest premium traded first'
  # the closed list of rules travels with the rows, so the page never has to guess at it
  assert [rule['rule_id'] for rule in body['unusual_rules']]==[D.RULE_VOL_TOD,D.RULE_DAY_VOL_VS_PREV_OI]
  assert body['unusual_rules_version']==D.UNUSUAL_RULES_VERSION
 finally:reader.close()


def test_a_store_without_the_baseline_columns_still_answers(tmp_path):
 """A trigger keeps the value its own sentence states when the store no longer carries the column."""
 path=tmp_path/'old.db'
 connection=sqlite3.connect(path);connection.executescript(SCHEMA)   # the older shape: no baseline columns
 connection.execute('insert into contracts(instrument_token,tradingsymbol,underlying,instrument_type,strike,'
  'expiry,lot_size,first_seen,last_seen) values(?,?,?,?,?,?,?,?,?)',
  (101,'NIFTY26SEP23500CE','NIFTY','CE',23500.0,EXPIRY,75,SESSION,SESSION))
 connection.execute('insert into metrics(scope,metric_key,captured_at,instrument_token,tradingsymbol,'
  'underlying,instrument_type,strike,expiry,lot_size,days_to_expiry,last_price,average_price,volume,oi,spot,'
  'premium_cr,premium_status,vol_tod_ratio,vol_tod_sessions,vol_tod_status,unusual,unusual_reasons)'
  " values('contract',?,?,101,?,'NIFTY','CE',23500.0,?,75,10,240.0,240.0,500000.0,900000.0,23480.0,40.0,"
  "'ok',206.0,10,'ok',1,?)",
  ('NIFTY26SEP23500CE',MARKS[0],'NIFTY26SEP23500CE',EXPIRY,_vol_tod(206.0)))
 connection.execute('insert into snapshots(instrument_token,captured_at,last_price,oi,last_trade_time,'
  "vendor_id,fetched_at,snapshot_id) values(101,?,240.0,900000,?,'kite','x','y')",(MARKS[0],MARKS[0]))
 connection.commit();connection.close()
 reader=D.Derivatives(str(path))
 try:
  row=reader.screener({'group':'contract'})['rows'][0]
  trigger=row['unusual_triggers'][0]
  assert trigger['rule_id']==D.RULE_VOL_TOD and trigger['value']==206.0
  assert trigger['baseline'] is None,'a baseline the store does not hold is missing, never a nought'
  assert reader.screener({})['rows'][0]['unusual_rule_count']==1
 finally:reader.close()


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


# ---------------------------------------------------------------------------------------------------------------
# WHICH 15-MIN READING THE SCREENER OPENS ON, AND WHICH FLOORS IT CAN APPLY THERE
#
# When the capture dies part-way through a session the rest of it is rebuilt from 15-minute candles, and a
# candle carries no traded-price average - so `premium_cr` is NULL for every contract in those readings.
#
# That USED to mean the readings were unusable: the premium floor was applied anyway, it removed every row, and
# the tab fell back to the last live reading. On the real store of 18 Sep 2026 that left the whole tab sitting
# on 11:30 while the 15:30 reading held 10,510 contracts with a real last price, a real volume and a real open
# interest. One derived field that could not be measured was hiding four hours of fields that could.
#
# Now the floors DEGRADE: a floor is applied where its number was captured and is NOT applied where it was not,
# the rows are kept, and the response says which floors were in force and why the others were not. What is
# never done is substitute a number for the missing one - `average_price_est` is in the store and is not read.
# ---------------------------------------------------------------------------------------------------------------
REBUILT_MARKS=[f'{SESSION} {t}:00' for t in ('09:30','09:45','10:00','10:15','10:30','10:45')]


def _rebuilt_store(path):
 """Three captured readings, then three rebuilt from candles: priced, but with no premium at all."""
 connection=sqlite3.connect(path);connection.executescript(SCHEMA)
 connection.executemany('insert into contracts(instrument_token,tradingsymbol,underlying,instrument_type,'
  'strike,expiry,lot_size,first_seen,last_seen) values(?,?,?,?,?,?,?,?,?)',
  [(101,'NIFTY26SEP23500CE','NIFTY','CE',23500.0,EXPIRY,75,SESSION,SESSION),
   (201,'RELIANCE26SEP1240CE','RELIANCE','CE',1240.0,EXPIRY,500,SESSION,SESSION)])
 live,rebuilt=REBUILT_MARKS[:3],REBUILT_MARKS[3:]
 _option(connection,101,'NIFTY26SEP23500CE','NIFTY','CE',23500.0,75,live,[240.0]*3,[23480.0]*3)
 _option(connection,201,'RELIANCE26SEP1240CE','RELIANCE','CE',1240.0,500,live,[34.0]*3,[1244.0]*3)
 # The rebuilt readings: a close, an open interest, everything a candle carries - and premium_cr NULL,
 # because a candle has no traded-price average to build one from.
 for token,symbol,underlying,kind,strike,lot,price in ((101,'NIFTY26SEP23500CE','NIFTY','CE',23500.0,75,236.0),
   (201,'RELIANCE26SEP1240CE','RELIANCE','CE',1240.0,500,33.0)):
  connection.executemany('insert into metrics(scope,metric_key,captured_at,instrument_token,tradingsymbol,'
   'underlying,instrument_type,strike,expiry,lot_size,days_to_expiry,last_price,volume,oi,spot,premium_cr)'
   ' values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
   [('contract',symbol,mark,token,symbol,underlying,kind,strike,EXPIRY,lot,10,price,500000.0,900000.0,
     23480.0,None) for mark in rebuilt])
 # Every reading covered both underlyings: the capture reached them, it just could not price them.
 connection.executemany('insert into underlying_snapshots(underlying,captured_at,spot) values(?,?,?)',
  [(name,mark,100.0) for name in ('NIFTY','RELIANCE') for mark in REBUILT_MARKS])
 connection.commit();connection.close()


def test_a_rebuilt_reading_keeps_its_rows_under_the_floors_it_can_apply(tmp_path):
 """THE OWNER'S COMPLAINT, as a test. The newest reading carries no premium and IS still usable: it is
 gated on the two floors that were captured, the rows survive, and the card says which floors those were."""
 path=tmp_path/'rebuilt-afternoon.db'
 _rebuilt_store(path)
 reader=D.Derivatives(str(path))
 try:
  body=reader.screener({})
  # the NEWEST reading, not an older one: nothing is four hours behind any more
  assert body['as_of']==REBUILT_MARKS[-1]
  assert body['newest_at']==REBUILT_MARKS[-1]
  assert body['reading_is_newest'] is True
  assert body['reading_chosen'] is False
  assert body['reading_skipped']==0
  # and it is a reading with real rows on it, across more than one underlying
  assert body['rows'] and len({row['underlying'] for row in body['rows']})==2
  # THE DEGRADED FLOOR SET IS ON THE CARD. Two floors, named; the third named as not applied, with why.
  assert body['floors_degraded'] is True
  assert body['floors_applied']==['oi','last_price']
  assert body['floors_unmeasured']==['premium_cr']
  assert 'premium traded' not in body['floors_text']
  assert 'OI \u2265 1 lot' in body['floors_text'] and 'last price' in body['floors_text']
  assert any('premium floor could not be applied' in line for line in body['floors_unmeasured_text'])
  # the §3 constants themselves are unchanged - the definition did not move, only what could be applied
  assert body['floors']=={'premium_cr':2.0,'oi_lots':1,'last_price':1.0}
  # AND THE SORT IS ONE THE READING CAN SUPPORT. Ordering by a column that is null on every row is not an
  # ordering; the response says what it actually ranked by.
  assert body['ranking']['ranked_by']=='volume'
  assert body['ranking']['degraded'] is True
  assert all(key['field']!='premium_cr' for key in body['ranking']['keys'])
  # every reading is still listed, so nothing was taken away from the reader
  assert {row['at'] for row in body['readings']}=={*REBUILT_MARKS}
 finally:reader.close()


def test_a_premium_filter_asked_for_at_a_reading_with_no_premium_is_refused_not_ignored(tmp_path):
 """A floor the reader RAISED cannot be honoured where premium was never captured. Dropping it silently
 would serve a wider list than was asked for, under the caption of a narrower one."""
 path=tmp_path/'rebuilt-afternoon.db'
 _rebuilt_store(path)
 reader=D.Derivatives(str(path))
 try:
  try:
   reader.screener({'at':REBUILT_MARKS[-1],'min_premium_cr':20})
   assert False,'a filter that cannot be applied must be refused'
  except ValueError as error:
   assert 'no traded average price was captured' in str(error)
  # at a reading that DID capture premium the same filter is honoured exactly as before
  body=reader.screener({'at':REBUILT_MARKS[0],'min_premium_cr':20})
  assert body['as_of']==REBUILT_MARKS[0]
  assert any(rule['key']=='min_premium_cr' and rule['value']==20 for rule in body['applied'])
 finally:reader.close()


def test_a_reading_the_reader_chose_is_served_exactly_as_asked(tmp_path):
 """The default narrows the CHOICE; it never overrides one."""
 path=tmp_path/'rebuilt-afternoon.db'
 _rebuilt_store(path)
 reader=D.Derivatives(str(path))
 try:
  body=reader.screener({'at':REBUILT_MARKS[0]})
  assert body['as_of']==REBUILT_MARKS[0]
  assert body['reading_chosen'] is True and body['reading_is_newest'] is False
  assert body['reading_skipped']==0
  # a live reading measured all three, so nothing about it is degraded
  assert body['floors_degraded'] is False
  assert body['floors_applied']==['premium_cr','oi','last_price']
  assert body['ranking']['ranked_by']=='premium_cr'
 finally:reader.close()


def test_the_usable_reading_rests_on_the_floors_and_never_on_an_estimated_price(tmp_path):
 """A number that decides what the reader can see has to be one the exchange reported. `average_price_est`
 exists in the real store and is deliberately not read anywhere in this module."""
 source=(D.__file__ if hasattr(D,'__file__') else '')
 assert source
 with open(source,encoding='utf-8') as handle:text=handle.read()
 assert 'average_price_est' not in text
 # and the probe applies the SAME floors the screener applies at that same reading, through one builder
 path=tmp_path/'rebuilt-afternoon.db'
 _rebuilt_store(path)
 reader=D.Derivatives(str(path))
 try:
  assert reader._clears_floors(REBUILT_MARKS[0]) is True
  assert reader._clears_floors(REBUILT_MARKS[-1]) is True,'the two measurable floors are enough'
  assert reader._floors_in_force(REBUILT_MARKS[0])['applied']==('premium_cr','oi','last_price')
  assert reader._floors_in_force(REBUILT_MARKS[-1])['applied']==('oi','last_price')
  assert reader._floors_in_force(REBUILT_MARKS[-1])['unmeasured']==('premium_cr',)
  at,skipped=reader._usable_reading(reader.screener_readings())
  assert (at,skipped)==(REBUILT_MARKS[-1],0)
 finally:reader.close()


def test_a_store_where_no_reading_clears_the_floors_still_serves_its_newest(tmp_path):
 """When EVERY reading is empty the honest answer is the newest one, empty - not an older one that is just
 as empty under an older as-of. The floors that COULD be applied here reject everything on their own: the
 last price is under ₹1 and the open interest is under one lot."""
 path=tmp_path/'nothing-clears.db'
 connection=sqlite3.connect(path);connection.executescript(SCHEMA)
 connection.execute('insert into contracts(instrument_token,tradingsymbol,underlying,instrument_type,strike,'
  'expiry,lot_size,first_seen,last_seen) values(?,?,?,?,?,?,?,?,?)',
  (101,'NIFTY26SEP23500CE','NIFTY','CE',23500.0,EXPIRY,75,SESSION,SESSION))
 connection.executemany('insert into metrics(scope,metric_key,captured_at,instrument_token,tradingsymbol,'
  'underlying,instrument_type,strike,expiry,lot_size,days_to_expiry,last_price,volume,oi,spot,premium_cr)'
  ' values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
  [('contract','NIFTY26SEP23500CE',mark,101,'NIFTY26SEP23500CE','NIFTY','CE',23500.0,EXPIRY,75,10,0.5,
    500000.0,10.0,23480.0,None) for mark in REBUILT_MARKS])
 connection.executemany('insert into underlying_snapshots(underlying,captured_at,spot) values(?,?,?)',
  [('NIFTY',mark,100.0) for mark in REBUILT_MARKS])
 connection.commit();connection.close()
 reader=D.Derivatives(str(path))
 try:
  body=reader.screener({})
  assert body['as_of']==REBUILT_MARKS[-1] and body['rows']==[]
  assert body['reading_is_newest'] is True and body['reading_skipped']==0
  # AND THE SENTENCE IS STILL THE CAPTURE STATE'S. Premium was never measured here, so this is not a reading
  # of a quiet market however empty the list is.
  assert body['empty_state']=='partial_capture'
  note=body['empty_note'] or ''
  assert 'not a quiet market' in note
  assert 'cleared the floors' not in note and 'clears the floors' not in note
  assert body['capture']['healthy'] is False
  assert body['capture']['missing_fields']==['premium_cr']
 finally:reader.close()


# ---------------------------------------------------------------------------------------------------------------
# NOTHING CHANGES AT A READING WHERE PREMIUM WAS MEASURED
#
# The degradation above is the whole risk of this change: a screen that quietly stops applying a floor at a
# reading that CAN support it would show contracts no reader asked to see. This is the guard, run against the
# real store when it is there - the 11:30 reading of 18 Sep 2026, the last live one before the outage.
# ---------------------------------------------------------------------------------------------------------------
REAL_STORE=pathlib.Path(__file__).resolve().parents[3]/'db'/'derivatives.db'
LIVE_READING='2026-09-18 11:30:00'


@pytest.mark.skipif(not REAL_STORE.is_file(),reason='the production F&O store is not on this machine')
def test_the_last_live_reading_of_18_sep_still_applies_all_three_floors():
 """27,239 contract rows, every floor measurable, all three applied, ranked by premium - unchanged."""
 reader=D.Derivatives(str(REAL_STORE))
 try:
  if not reader.available() or not reader._reading_rows(LIVE_READING):
   pytest.skip('this store does not hold the 18 Sep 2026 session')
  force=reader._floors_in_force(LIVE_READING)
  assert force['applied']==('premium_cr','oi','last_price')
  assert force['unmeasured']==() and force['absent']==()
  body=reader.screener({'at':LIVE_READING,'group':'contract'})
  assert body['floors_degraded'] is False
  assert body['floors_text']==D.FLOORS_TEXT,'the three-floor sentence, word for word'
  assert body['ranking']['ranked_by']=='premium_cr'
  assert all(row['premium_cr']>=2.0 for row in body['rows']),'the premium floor is still in force'
  # the floors-only count is the one the screener reports as `cleared`; it must not have moved
  assert body['capture']['cleared']==reader._cleared_count(LIVE_READING)
 finally:reader.close()


# ---------------------------------------------------------------------------------------------------------------
# ONE ROW PER UNDERLYING
#
# The screener listed CONTRACTS, largest premium first. At the 11:30 reading of 18 Sep 2026, 85 underlyings had
# a contract over the liquidity floors - and NIFTY alone had 104 of the 491 contracts. A hundred-row list was
# therefore a hundred rows of NIFTY, and the reader's conclusion was the obvious one: no stock is active. That
# is what the owner meant by "why other stocks are not populating".
#
# So the default view is one row per underlying, and the contract list is the drill-down. What these tests hold
# is the part that can quietly go wrong: an aggregate that is not honest. Sums of rupees and contracts are fine.
# A MEAN OF RATIOS IS NOT A RATIO, and none is served.
# ---------------------------------------------------------------------------------------------------------------

def test_the_screener_opens_on_one_row_per_underlying(reader):
 # The stock in this store stops after its fourth reading, so this asks for one that BOTH names are at -
 # which is the whole point of the view: more than one underlying in it.
 body=reader.screener({'at':STOCK_MARKS[0]})
 assert body['view']=='underlying' and list(body['views'])==['underlying','contract']
 names=[row['underlying'] for row in body['rows']]
 assert names==sorted(set(names),key=names.index),'one row per underlying, never two'
 assert len(names)==body['groups_total']
 # the counts add up to the contract view, exactly
 contracts=reader.screener({'group':'contract','at':STOCK_MARKS[0]},limit=500)
 assert body['contracts_total']==len(contracts['rows'])
 assert sum(row['contracts'] for row in body['rows'])==body['contracts_total']
 by_name={}
 for row in contracts['rows']:by_name.setdefault(row['underlying'],[]).append(row)
 assert {row['underlying'] for row in body['rows']}==set(by_name)
 for row in body['rows']:
  mine=by_name[row['underlying']]
  assert row['contracts']==len(mine)
  assert row['calls']==sum(1 for r in mine if r['instrument_type']=='CE')
  assert row['puts']==sum(1 for r in mine if r['instrument_type']=='PE')
  # the SUMS: rupees and contracts, which add
  assert row['premium_cr']==pytest.approx(sum(r['premium_cr'] or 0 for r in mine),abs=0.01)
  assert row['volume']==sum(r['volume'] or 0 for r in mine)
  assert row['oi']==sum(r['oi'] or 0 for r in mine)
  # Spot is NOT an aggregate, and it is not a guess either: it is served only when the rows AGREE on it.
  # Where they do not, the row carries None and says so, rather than printing one of two numbers.
  spots={r['spot'] for r in mine if r['spot'] is not None}
  if len(spots)==1:assert row['spot']==next(iter(spots)) and row['spot_disagrees'] is False
  elif not spots:assert row['spot'] is None
  else:assert row['spot'] is None and row['spot_disagrees'] is True
 # the busiest name no longer owns the whole list: every name that cleared is reachable
 assert len(names)>1,'the fixture must have more than one underlying over the floors'
 # and the list is sorted by the same measure the contract list is
 premiums=[row['premium_cr'] for row in body['rows']]
 assert premiums==sorted(premiums,reverse=True)


def test_no_underlying_row_carries_an_invented_aggregate(reader):
 """A mean of ratios is not a ratio. §3.2 arrives as the LARGEST reading with its contract named, §3.3 as a
 count over the tab's own existing threshold, and §3.1 as counts - never as an underlying-level label."""
 body=reader.screener({'at':STOCK_MARKS[0]})
 contracts=reader.screener({'group':'contract','at':STOCK_MARKS[0]},limit=500)
 by_name={}
 for row in contracts['rows']:by_name.setdefault(row['underlying'],[]).append(row)
 for row in body['rows']:
  mine=by_name[row['underlying']]
  # NOT served at all: a per-contract signal has no underlying-level value
  for banned in ('volume_ratio','volume_to_oi','buildup_day','buildup_15m','buildup','moneyness','strike',
    'last_price','instrument_type','tradingsymbol'):
   assert banned not in row,f'{banned} must not appear on an underlying row'
  # §3.2: the maximum, and the contract it belongs to
  ratios=[r['volume_ratio'] for r in mine if r['volume_ratio'] is not None]
  assert row['volume_ratio_max']==(max(ratios) if ratios else None)
  assert row['volume_baseline_contracts']==len(ratios)
  if ratios:
   owner=max(mine,key=lambda r:(r['volume_ratio'] is not None,r['volume_ratio'] or 0))
   assert row['volume_ratio_max_symbol']==owner['tradingsymbol']
   # and it is emphatically NOT the mean, whenever the two differ
   mean=sum(ratios)/len(ratios)
   if max(ratios)!=mean:assert row['volume_ratio_max']!=pytest.approx(mean)
  else:
   assert row['volume_ratio_max_symbol'] is None
  # §3.3: a COUNT over the threshold the tab already uses, not a mean
  assert row['volume_to_oi_over_1']==sum(1 for r in mine if (r['volume_to_oi'] or 0)>1)
  # §3.1: counts per label, and no label of its own
  counts={}
  for r in mine:
   if r['buildup_day']:counts[r['buildup_day']]=counts.get(r['buildup_day'],0)+1
  assert row['buildup_counts']==counts
  # the busiest contract is a real row, not a summary of one
  if mine:
   top=max(mine,key=lambda r:r['premium_cr'] or 0)
   assert row['top']['tradingsymbol']==top['tradingsymbol']
   assert row['top']['premium_cr']==top['premium_cr']


def test_the_view_is_a_filter_the_server_reports_and_refuses(reader):
 body=reader.screener({'group':'contract'})
 applied={row['key']:row['value'] for row in body['applied']}
 assert applied['group']=='contract','the view is reported like every other thing the query did'
 assert reader.screener({})['view']=='underlying'
 with pytest.raises(ValueError):reader.screener({'group':'sideways'})
 # and it is offered, so a caller can discover it rather than guess
 offered={row['key'] for row in reader.screener_available()}
 assert 'group' in offered


def test_the_underlying_view_is_built_from_every_row_that_cleared_not_from_the_page(reader):
 """The aggregate must describe the MARKET, not the first page of it. A sum of the first N contracts would
 be a number about this list rather than about the reading."""
 full=reader.screener({})
 capped=reader.screener({},limit=1)
 assert capped['limit']==1 and len(capped['rows'])==1
 # the totals are unchanged by the cut, and the one row served is identical to the same row in the full list
 assert capped['groups_total']==full['groups_total']
 assert capped['contracts_total']==full['contracts_total']
 assert capped['rows'][0]==full['rows'][0]


# ---------------------------------------------------------------------------------------------------------------
# THE OPTION CHAIN OPENS AT THE MONEY
#
# It did not. It took the newest reading; a session rebuilt from 15-minute candles carries NO SPOT at all; so
# the chain had no anchor and opened at its lowest strike - 21,350 against a spot of 23,302 on the real store,
# nearly two thousand points away, every visible contract far out of the money and priced at a rupee.
#
# The reading the TAB is on is now passed to it, which is where the spot comes from.
# ---------------------------------------------------------------------------------------------------------------

def test_the_chain_reads_the_reading_it_is_asked_for(reader):
 first,last=MARKS[0],MARKS[-1]
 early=reader.chain('NIFTY',at=first)
 late=reader.chain('NIFTY',at=last)
 assert early['as_of']==first and late['as_of']==last
 assert early['spot'] is not None,'a captured reading has a spot for the chain to sit around'
 # the ladder itself is untouched by the choice: nothing is dropped and nothing is reordered
 assert [row['strike'] for row in early['rows']]==sorted(row['strike'] for row in early['rows'])
 assert {row['strike'] for row in early['rows']}=={row['strike'] for row in late['rows']}
 # omitted, it behaves exactly as it always did: the newest reading this underlying has
 assert reader.chain('NIFTY')['as_of']==reader.chain('NIFTY',at=last)['as_of']
 # and OI by strike rides on it, so it lands on the same reading
 assert reader.oi_by_strike('NIFTY',at=first)['as_of']==first
 assert reader.oi_by_strike('NIFTY',at=first)['spot']==early['spot']


def test_a_chain_at_a_reading_with_no_spot_says_so_rather_than_guessing(tmp_path):
 """A rebuilt reading carries no spot. The chain must come back with spot None - never a spot borrowed from
 another reading, which would put the at-the-money marker on the wrong strike."""
 path=tmp_path/'no-spot.db'
 connection=sqlite3.connect(path);connection.executescript(SCHEMA)
 connection.execute('insert into contracts(instrument_token,tradingsymbol,underlying,instrument_type,strike,'
  'expiry,lot_size,first_seen,last_seen) values(?,?,?,?,?,?,?,?,?)',
  (101,'NIFTY26SEP23500CE','NIFTY','CE',23500.0,EXPIRY,75,SESSION,SESSION))
 live,rebuilt=MARKS[0],MARKS[1]
 _option(connection,101,'NIFTY26SEP23500CE','NIFTY','CE',23500.0,75,[live],[240.0],[23480.0])
 connection.execute('insert into metrics(scope,metric_key,captured_at,instrument_token,tradingsymbol,'
  'underlying,instrument_type,strike,expiry,lot_size,days_to_expiry,last_price,volume,oi,spot,premium_cr)'
  ' values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
  ('contract','NIFTY26SEP23500CE',rebuilt,101,'NIFTY26SEP23500CE','NIFTY','CE',23500.0,EXPIRY,75,10,236.0,
   500000.0,900000.0,None,None))
 connection.executemany('insert into underlying_snapshots(underlying,captured_at,spot) values(?,?,?)',
  [('NIFTY',live,23480.0),('NIFTY',rebuilt,None)])
 connection.commit();connection.close()
 view=D.Derivatives(str(path))
 try:
  assert view.chain('NIFTY',at=live)['spot']==23480.0
  assert view.chain('NIFTY',at=rebuilt)['spot'] is None,'no spot is None, never another reading\'s spot'
 finally:view.close()


# --- Δ SINCE THE PREVIOUS SESSION'S CLOSE ------------------------------------------------------------------
# The owner could read that PCR was "flat" and max pain "stable" — and could not read that PCR had moved 0.03
# and the strike 50 points. The word was never the problem; the missing number was. So every reading of PCR,
# max pain and implied volatility now carries a Δ on ΔOI's own definition: this reading minus the same figure
# at the LAST 15-min reading of the session before.
#
# The rule these tests exist to hold: NO PREVIOUS CLOSE MEANS NO Δ. Null with a reason — never a 0, never
# "unchanged". A 0 in a Δ column is a claim that the figure did not move, and an absent baseline is not a claim.
PRIOR_SESSION='2026-09-17'
PRIOR_MARK=f'{PRIOR_SESSION} 15:45:00'


def _prior_close(path,underlying='NIFTY',*,pcr_oi=1.1576,pcr_volume=1.0061,max_pain=23300.0,
  contracts=192,ce_oi=5_000_000,pe_oi=4_200_000,spot=23470.0,status='ok',expiry=EXPIRY):
 """The previous session's CLOSING reading for one chain, written into an existing test store."""
 connection=sqlite3.connect(path)
 connection.execute('insert into metrics(scope,metric_key,captured_at,underlying,expiry,days_to_expiry,spot,'
  'pcr_oi,pcr_volume,total_ce_oi,total_pe_oi,max_pain_strike,max_pain_distance,max_pain_total_oi,'
  'max_pain_status,contracts) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
  ('underlying',f'{underlying}|{expiry}',PRIOR_MARK,underlying,expiry,11,spot,pcr_oi,pcr_volume,ce_oi,pe_oi,
   max_pain,max_pain-spot,ce_oi+pe_oi,status,contracts))
 connection.commit();connection.close()


def test_every_reading_of_all_three_carries_a_delta_field(reader):
 """A Δ per point for PCR, max pain and implied volatility — present on EVERY reading, gap or not."""
 pcr=reader.pcr_series('NIFTY')
 for point in pcr['points']:
  assert 'delta_pcr_oi' in point and 'delta_pcr_volume' in point
  assert 'delta_pcr_oi_reason' in point and 'delta_pcr_volume_reason' in point
 pain=reader.maxpain_series('NIFTY')
 for point in pain['points']:
  assert 'delta_max_pain_strike' in point and 'delta_max_pain_strike_reason' in point
 iv=reader.iv_series('NIFTY')
 for point in iv['points']:
  assert 'delta_iv' in point and 'delta_iv_pct' in point and 'delta_iv_pct_reason' in point
 for leg in ('ce','pe'):
  for point in ((iv['atm'] or {}).get(leg) or {}).get('points') or []:
   assert 'delta_iv_pct' in point and 'delta_iv_pct_reason' in point


def test_no_previous_close_is_null_with_a_reason_and_never_a_zero(reader):
 """This store holds ONE session. Nothing before it, so nothing has a Δ — and nothing says it did not move."""
 for body,key in ((reader.pcr_series('NIFTY'),'delta_pcr_oi'),
   (reader.maxpain_series('NIFTY'),'delta_max_pain_strike'),
   (reader.iv_series('NIFTY'),'delta_iv_pct')):
  assert body['readings_with_delta']==0
  assert body['previous_close_reason'],'an absent baseline must always say so'
  assert 'unchanged' not in body['previous_close_reason'].lower()
  for point in body['points']:
   assert point[key] is None,'a missing baseline is never a zero'
   assert point.get(key+'_reason')=='no_baseline'


def test_pcr_delta_is_a_ratio_change_against_the_previous_close(store):
 """PCR's Δ is a change in the RATIO — never a percentage of a ratio — and it is the plain subtraction."""
 _prior_close(store)
 reader=D.Derivatives(store)
 try:
  body=reader.pcr_series('NIFTY')
  assert body['previous_close_at']==PRIOR_MARK
  assert body['previous_close_pcr_oi']==1.1576 and body['previous_close_pcr_volume']==1.0061
  assert body['previous_close_reason'] is None
  assert body['delta_unit']=='ratio'
  assert body['readings_with_delta']==len(MARKS)
  for point in body['points']:
   assert point['delta_pcr_oi']==pytest.approx(point['pcr_oi']-1.1576,abs=1e-4)
   assert point['delta_pcr_oi_reason'] is None
  assert body['latest_delta_pcr_oi']==pytest.approx(body['latest_pcr_oi']-1.1576,abs=1e-4)
  assert body['latest_delta_pcr_oi_at']==MARKS[-1]
  # the WORD the owner chose is still there; the number stands beside it, it does not replace it
  assert body['direction'] in D.DIRECTION_WORDS['pcr'].values()
 finally:
  reader.close()


def test_max_pain_delta_is_in_strike_points_and_never_a_percentage(store):
 """Max pain is a STRIKE. Its Δ is in points, it moves in strike steps, and the unit is stated."""
 _prior_close(store)
 reader=D.Derivatives(store)
 try:
  body=reader.maxpain_series('NIFTY')
  assert body['previous_close_max_pain_strike']==23300.0
  assert body['delta_unit']=='strike points'
  assert 'percentage' in body['delta_unit_text'].lower(),'the rule against a percentage is stated on the wire'
  # the store's chain steps the strike 50 points per reading from 23,500
  assert body['points'][0]['delta_max_pain_strike']==pytest.approx(200.0)
  assert body['latest_delta_max_pain_strike']==pytest.approx(body['latest_max_pain_strike']-23300.0)
  # every Δ is a whole number of strike steps, never a fraction of a percent
  for point in body['points']:
   assert point['delta_max_pain_strike'] is None or float(point['delta_max_pain_strike']).is_integer()
  assert body['direction'] in D.DIRECTION_WORDS['max_pain'].values()
 finally:
  reader.close()


def test_a_previous_close_the_floors_would_refuse_is_not_a_baseline(store):
 """A closing reading too thin to carry a ratio does not get promoted into a baseline for a whole session."""
 _prior_close(store,contracts=3)
 reader=D.Derivatives(store)
 try:
  body=reader.pcr_series('NIFTY')
  assert body['previous_close_pcr_oi'] is None
  assert body['previous_close_withheld']=='thin_chain'
  assert body['previous_close_reason']==D.WITHHELD_REASONS['thin_chain']
  assert body['readings_with_delta']==0
  assert all(p['delta_pcr_oi'] is None for p in body['points'])
 finally:
  reader.close()


def test_a_previous_close_the_worker_marked_unusable_is_not_a_max_pain_baseline(store):
 """`max_pain_status` is honoured on the BASELINE exactly as it is on a reading of the session itself."""
 _prior_close(store,status='stale')
 reader=D.Derivatives(store)
 try:
  body=reader.maxpain_series('NIFTY')
  assert body['previous_close_max_pain_strike'] is None
  assert body['previous_close_withheld']=='store_status'
  assert all(p['delta_max_pain_strike'] is None for p in body['points'])
 finally:
  reader.close()


def test_a_gap_reading_has_no_delta_even_when_a_baseline_exists(store):
 """Gaps stay gaps. RELIANCE stops after four readings; the eight holes get a reason, not a number."""
 _prior_close(store,underlying='RELIANCE',pcr_oi=0.5,pcr_volume=0.4,max_pain=1280.0,contracts=86,
  ce_oi=900_000,pe_oi=480_000,spot=1240.0)
 reader=D.Derivatives(store)
 try:
  body=reader.pcr_series('RELIANCE')
  assert body['previous_close_pcr_oi']==0.5
  filled=[p for p in body['points'] if not p['gap']]
  holes=[p for p in body['points'] if p['gap']]
  assert len(filled)==len(STOCK_MARKS) and holes
  assert all(p['delta_pcr_oi'] is not None for p in filled)
  for point in holes:
   assert point['delta_pcr_oi'] is None and point['delta_pcr_oi_reason']=='no_value'
 finally:
  reader.close()


def test_iv_delta_is_in_volatility_points_never_a_percentage_of_a_percentage(store):
 """The previous session's closing option price is RE-SOLVED with the same model, and Δ is in vol points."""
 connection=sqlite3.connect(store)
 for token,symbol,kind,strike,price in ((101,'NIFTY26SEP23500CE','CE',23500.0,250.0),
   (102,'NIFTY26SEP23500PE','PE',23500.0,235.0)):
  connection.execute('insert into metrics(scope,metric_key,captured_at,instrument_token,tradingsymbol,'
   'underlying,instrument_type,strike,expiry,lot_size,days_to_expiry,last_price,average_price,volume,oi,spot)'
   ' values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
   ('contract',symbol,PRIOR_MARK,token,symbol,'NIFTY',kind,strike,EXPIRY,75,11,price,price,500000.0,900000.0,
    23470.0))
  connection.execute('insert into snapshots(instrument_token,captured_at,last_price,oi,last_trade_time,'
   "vendor_id,fetched_at,snapshot_id) values(?,?,?,?,?,'kite','x','y')",
   (token,PRIOR_MARK,price,900000,PRIOR_MARK))
 connection.commit();connection.close()
 reader=D.Derivatives(store)
 try:
  body=reader.iv_series('NIFTY')
  assert body['delta_unit']=='volatility points'
  assert body['previous_close_iv_pct'] is not None and body['previous_close_basis']=='call and put'
  assert body['readings_with_delta']==len(MARKS)
  base=body['previous_close_iv_pct']
  for point in body['points']:
   assert point['delta_iv_pct']==pytest.approx(point['iv_pct']-base,abs=1e-3)
  # the Δ is a DIFFERENCE of two percentages, not a percentage change of one
  latest=body['latest_iv_pct']
  assert body['latest_delta_iv_pct']==pytest.approx(latest-base,abs=1e-3)
  assert abs(body['latest_delta_iv_pct']-(latest-base)/base*100.0)>1e-3
  # and each leg carries its OWN baseline rather than borrowing the at-the-money one
  for leg in ('ce','pe'):
   side=(body['atm'] or {})[leg]
   assert side['previous_close_iv_pct'] is not None
   assert side['readings_with_delta']==len(MARKS)
   assert side['delta_unit']=='volatility points'
 finally:
  reader.close()


def test_no_delta_wording_offers_a_percentage_of_a_percentage(reader):
 """§5, stated as a test: the served Δ wording never offers a percentage for a strike or for a volatility."""
 assert D.MAX_PAIN_DELTA_UNIT=='strike points'
 assert D.IV_DELTA_UNIT=='volatility points'
 assert D.PCR_DELTA_UNIT=='ratio'
 assert 'never shown as a percentage' in D.MAX_PAIN_DELTA_TEXT
 assert 'never shown as a percentage change of a percentage' in D.IV_DELTA_TEXT
 # "mark" is the store's word, not the reader's: nothing served here may say it.
 for text in (D.SERIES_DELTA_TEXT,D.PCR_DELTA_TEXT,D.MAX_PAIN_DELTA_TEXT,D.IV_DELTA_TEXT,
   *D.SERIES_DELTA_REASONS.values()):
  assert 'mark' not in text.lower()
 for text in D.SERIES_DELTA_REASONS.values():
  assert 'unchanged' not in text.lower()
