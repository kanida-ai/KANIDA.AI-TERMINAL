"""The Derivative tab's read-only server (docs/DERIVATIVES_SPEC.md §2-§5).

The real `db/derivatives.db` is written by the capture and metrics workers, so these tests build their own
store with the §2 schema and assert what the tab is allowed to say:

 * with no store at all, every card answers 200 with `available: False` and the ONE empty sentence - never a
   500, never an error page;
 * the §3 liquidity floors are applied to the unusual list and are returned with it, so the card can state
   them; `min_premium_cr` can raise the floor but never lower it;
 * §3.2 is enforced on the serving side: a volume ratio with fewer than 3 baseline sessions is served as
   `null` with `volume_baseline: "none"` - never a number;
 * a signal column D2 has not written yet comes back `null` and is named in `missing` - never a zero;
 * the option chain, OI-by-strike, index and futures cards each carry their own `as_of`;
 * the store is opened read-only: the pilot cannot write to it.
"""
import re
import sqlite3
from pathlib import Path
import pytest
from kanida_pilot import derivatives as D
from kanida_pilot.app import create_app
from test_pilot import Evidence,pilot_settings,signup  # noqa: F401

MARK='2026-09-18 14:45'
EARLIER='2026-09-18 14:30'
EXPIRY='2026-09-25'
FAR='2026-10-30'

# Exactly the §2 store. Copied, not imported, so a drift in the capture worker's schema shows up here.
SCHEMA='''
CREATE TABLE contracts(instrument_token INTEGER PRIMARY KEY, tradingsymbol TEXT, underlying TEXT,
 instrument_type TEXT, strike REAL, expiry TEXT, lot_size INTEGER, first_seen TEXT, last_seen TEXT);
CREATE TABLE snapshots(instrument_token INTEGER, captured_at TEXT, last_price REAL, average_price REAL,
 volume INTEGER, oi INTEGER, buy_quantity INTEGER, sell_quantity INTEGER, bid REAL, ask REAL, source TEXT);
CREATE TABLE candles_15m(instrument_token INTEGER, bar_start TEXT, open REAL, high REAL, low REAL,
 close REAL, volume INTEGER, oi INTEGER);
CREATE TABLE underlying_snapshots(underlying TEXT, captured_at TEXT, spot REAL, fut_price REAL,
 total_ce_oi INTEGER, total_pe_oi INTEGER, total_ce_volume INTEGER, total_pe_volume INTEGER,
 pcr_oi REAL, pcr_volume REAL, max_pain_strike REAL);
CREATE TABLE metrics(instrument_token INTEGER, captured_at TEXT, last_price REAL, oi INTEGER,
 volume INTEGER, average_price REAL, premium_inr REAL, price_change_15m_pct REAL, oi_change_15m INTEGER,
 buildup_15m TEXT, price_change_day_pct REAL, oi_change_day INTEGER, oi_change_day_pct REAL,
 buildup_day TEXT, volume_ratio REAL, volume_baseline_sessions INTEGER, volume_to_oi REAL,
 previous_oi INTEGER, basis REAL, oi_vs_20d_avg REAL);
'''

CR=D.CRORE


def build_store(path,*,metrics=True):
 """A small but complete store: one index, one stock, CE/PE strikes, a future, and two capture marks."""
 connection=sqlite3.connect(path)
 connection.executescript(SCHEMA)
 contracts=[
  # token, tradingsymbol, underlying, type, strike, expiry, lot
  (101,'NIFTY25SEP25000CE','NIFTY','CE',25000.0,EXPIRY,75),
  (102,'NIFTY25SEP25000PE','NIFTY','PE',25000.0,EXPIRY,75),
  (103,'NIFTY25SEP25200CE','NIFTY','CE',25200.0,EXPIRY,75),
  (104,'NIFTY25OCT25000CE','NIFTY','CE',25000.0,FAR,75),
  (201,'RELIANCE25SEP1400CE','RELIANCE','CE',1400.0,EXPIRY,500),
  (202,'RELIANCE25SEP1400PE','RELIANCE','PE',1400.0,EXPIRY,500),
  (301,'NIFTY25SEPFUT','NIFTY','FUT',None,EXPIRY,75),
  (302,'RELIANCE25SEPFUT','RELIANCE','FUT',None,EXPIRY,500),
 ]
 connection.executemany('insert into contracts(instrument_token,tradingsymbol,underlying,instrument_type,'
  'strike,expiry,lot_size,first_seen,last_seen) values(?,?,?,?,?,?,?,?,?)',
  [(*c,EARLIER,MARK) for c in contracts])
 if metrics:
  # token, price, oi, volume, avg, premium ₹, buildup day, oi chg day, vol ratio, baseline sessions, vol/oi
  rows=[
   # clears every floor, 10 sessions of baseline
   (101,142.5,3_750_000,2_100_000,140.0,48*CR,'long_buildup',420_000,2.4,10,1.2),
   (102,88.0,2_400_000,900_000,86.0,12*CR,'short_buildup',180_000,0.9,10,0.6),
   # under the ₹2 cr premium floor -> never in the unusual list
   (103,3.5,900_000,120_000,3.4,0.4*CR,'long_unwinding',-40_000,1.4,10,0.3),
   # far expiry, clears the floors: only the days-to-expiry filter may drop it
   (104,210.0,1_100_000,600_000,205.0,9*CR,'long_buildup',60_000,1.1,8,0.4),
   # a ratio with only 2 sessions behind it: §3.2 says "no baseline", never a number
   (201,26.0,1_500_000,700_000,25.5,7*CR,'short_covering',-90_000,3.9,2,0.5),
   # last price below ₹1 -> floor
   (202,0.8,800_000,200_000,0.75,5*CR,'long_unwinding',-10_000,1.0,10,0.2),
   (301,25150.0,12_000_000,3_000_000,25140.0,300*CR,'long_buildup',900_000,1.3,10,0.25),
   (302,1412.0,5_000_000,1_200_000,1410.0,80*CR,'short_buildup',300_000,1.1,10,0.3),
  ]
  for mark in (EARLIER,MARK):
   connection.executemany('insert into metrics(instrument_token,captured_at,last_price,oi,volume,average_price,'
    'premium_inr,buildup_day,oi_change_day,volume_ratio,volume_baseline_sessions,volume_to_oi,'
    'price_change_day_pct,buildup_15m) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
    [(r[0],mark,r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],r[10],1.5,'long_buildup') for r in rows])
 connection.executemany('insert into underlying_snapshots(underlying,captured_at,spot,fut_price,total_ce_oi,'
  'total_pe_oi,total_ce_volume,total_pe_volume,pcr_oi,pcr_volume,max_pain_strike) values(?,?,?,?,?,?,?,?,?,?,?)',
  [('NIFTY',EARLIER,25100.0,25140.0,5_000_000,4_200_000,900_000,700_000,0.84,0.78,25000.0),
   ('NIFTY',MARK,25120.0,25150.0,5_200_000,4_600_000,1_000_000,820_000,0.88,0.82,25000.0),
   ('BANKNIFTY',MARK,54200.0,54280.0,3_100_000,2_700_000,600_000,520_000,0.87,0.87,54000.0),
   ('RELIANCE',MARK,1408.0,1412.0,900_000,700_000,200_000,150_000,0.78,0.75,1400.0)])
 connection.executemany('insert into candles_15m(instrument_token,bar_start,open,high,low,close,volume,oi)'
  ' values(?,?,?,?,?,?,?,?)',
  [(101,EARLIER,138.0,144.0,137.0,140.0,1_800_000,3_600_000),
   (101,MARK,140.0,145.0,139.5,142.5,2_100_000,3_750_000)])
 connection.commit();connection.close()


@pytest.fixture
def store(tmp_path):
 path=tmp_path/'derivatives.db';build_store(path);return str(path)

def client_for(tmp_path,**extra):
 app=create_app(pilot_settings(tmp_path,**extra),evidence=Evidence())
 try:yield_client=signup(app)
 except Exception:app.state.db.close();raise
 return app,yield_client


# --- nothing captured yet ------------------------------------------------------------------------------
def test_every_card_is_empty_not_broken_before_capture(tmp_path):
 """No store at all: 200, `available: False`, the one empty sentence. Never an error page."""
 app,client=client_for(tmp_path)
 try:
  for path in ('/api/derivatives/status','/api/derivatives/filters','/api/derivatives/unusual',
    '/api/derivatives/indices','/api/derivatives/futures','/api/derivatives/series?underlying=NIFTY',
    '/api/derivatives/oi-grid?underlying=NIFTY'):
   result=client.get(path)
   assert result.status_code==200,(path,result.text)
   body=result.json()
   assert body['available'] is False and body['captured'] is False,path
   assert body['empty_reason']==D.EMPTY_TEXT,path
   assert body['floors']=={'premium_cr':2.0,'oi_lots':1,'last_price':1.0},path
   assert body['as_of'] is None,path
 finally:app.state.db.close()

def test_chain_needs_an_underlying(tmp_path):
 app,client=client_for(tmp_path)
 try:
  assert client.get('/api/derivatives/chain').status_code==400
  assert client.get('/api/derivatives/chain?underlying=NIFTY%27;drop').status_code==400
  assert client.get('/api/derivatives/unusual?expiry=not-a-date').status_code==400
  assert client.get('/api/derivatives/unusual?option_type=XX').status_code==400
  assert client.get('/api/derivatives/futures?limit=99999').status_code==400
 finally:app.state.db.close()


# --- the floors and the baseline rule -------------------------------------------------------------------
def test_unusual_applies_the_liquidity_floors_and_states_them(tmp_path,store):
 app,client=client_for(tmp_path,derivatives_database=store)
 try:
  body=client.get('/api/derivatives/unusual').json()
  assert body['available'] and body['as_of']==MARK
  assert body['floors_text'].startswith('Liquidity floors in force')
  assert body['floor_premium_cr']==2.0
  symbols={s['tradingsymbol'] for row in body['rows'] for s in row['strikes']}
  assert 'NIFTY25SEP25000CE' in symbols
  assert 'NIFTY25SEP25200CE' not in symbols,'premium below ₹2 cr must not appear'
  assert 'RELIANCE25SEP1400PE' not in symbols,'last price below ₹1 must not appear'
  # rolled up per underlying, richest first (§3 roll-up)
  assert [r['underlying'] for r in body['rows']][0]=='NIFTY'
  nifty=body['rows'][0]
  assert nifty['strike_count']==3 and nifty['calls']==2 and nifty['puts']==1
  assert nifty['premium_cr']==pytest.approx(69.0)
 finally:app.state.db.close()

def test_min_premium_raises_the_floor_and_can_never_lower_it(tmp_path,store):
 app,client=client_for(tmp_path,derivatives_database=store)
 try:
  raised=client.get('/api/derivatives/unusual?min_premium_cr=20').json()
  assert raised['floor_premium_cr']==20.0
  assert {s['tradingsymbol'] for r in raised['rows'] for s in r['strikes']}=={'NIFTY25SEP25000CE'}
  lowered=client.get('/api/derivatives/unusual?min_premium_cr=0.1').json()
  assert lowered['floor_premium_cr']==2.0,'the §3 floor is a floor, not a default'
 finally:app.state.db.close()

def test_a_ratio_without_three_sessions_is_no_baseline_never_a_number(tmp_path,store):
 app,client=client_for(tmp_path,derivatives_database=store)
 try:
  body=client.get('/api/derivatives/chain?underlying=RELIANCE').json()
  call=[r for r in body['rows'] if r['strike']==1400.0][0]['ce']
  assert call['volume_baseline_sessions']==2
  assert call['volume_baseline']=='none'
  assert call['volume_ratio'] is None,'2 sessions of history may never be served as a ratio'
  nifty=client.get('/api/derivatives/chain?underlying=NIFTY').json()
  ce=[r for r in nifty['rows'] if r['strike']==25000.0][0]['ce']
  assert ce['volume_baseline']=='ok' and ce['volume_ratio']==2.4
 finally:app.state.db.close()

def test_a_signal_column_d2_has_not_written_is_null_and_named(tmp_path,store):
 """`oi_change_15m_pct` is not in this store: it must come back null and be listed in `missing`."""
 app,client=client_for(tmp_path,derivatives_database=store)
 try:
  body=client.get('/api/derivatives/chain?underlying=NIFTY').json()
  ce=[r for r in body['rows'] if r['strike']==25000.0][0]['ce']
  assert ce['oi_change_15m_pct'] is None and ce['oi_change_15m'] is None
  assert 'oi_change_15m_pct' in body['missing'] and 'premium_cr' in body['missing']
  assert ce['premium_cr']==48.0,'₹ premium is converted to crore, not invented'
 finally:app.state.db.close()


# --- the cards ------------------------------------------------------------------------------------------
def test_chain_and_expiry_default_to_the_front_expiry(tmp_path,store):
 app,client=client_for(tmp_path,derivatives_database=store)
 try:
  body=client.get('/api/derivatives/chain?underlying=NIFTY').json()
  assert body['expiry']==EXPIRY and body['spot']==25120.0
  assert [r['strike'] for r in body['rows']]==[25000.0,25200.0],'the far expiry is a different chain'
  assert body['rows'][0]['ce']['tradingsymbol']=='NIFTY25SEP25000CE'
  assert body['rows'][0]['pe']['tradingsymbol']=='NIFTY25SEP25000PE'
  assert body['rows'][1]['pe'] is None
  far=client.get(f'/api/derivatives/chain?underlying=NIFTY&expiry={FAR}').json()
  assert [r['strike'] for r in far['rows']]==[25000.0]
 finally:app.state.db.close()

def test_oi_by_strike_carries_max_pain_spot_and_the_total_it_came_from(tmp_path,store):
 app,client=client_for(tmp_path,derivatives_database=store)
 try:
  body=client.get('/api/derivatives/oi-by-strike?underlying=NIFTY').json()
  assert body['max_pain_strike']==25000.0 and body['spot']==25120.0
  # Distance is the max-pain STRIKE MINUS spot: 25000 - 25120 = -120, so a positive distance means the strike
  # sits above spot. That is the convention `metrics.max_pain_distance` is written with and the one
  # /api/derivatives/maxpain-series serves, and this card used to return the opposite sign - two contradictory
  # readings of one number on one tab. The definition now travels with the response.
  assert body['max_pain_distance']==-120.0
  assert 'MINUS spot' in body['max_pain_distance_definition']
  assert body['total_ce_oi']==3_750_000+900_000 and body['total_pe_oi']==2_400_000
  assert body['as_of']==MARK
 finally:app.state.db.close()

def test_index_card_reads_pcr_and_the_day_series(tmp_path,store):
 app,client=client_for(tmp_path,derivatives_database=store)
 try:
  body=client.get('/api/derivatives/indices').json()
  by_name={r['underlying']:r for r in body['rows']}
  assert set(by_name)=={'NIFTY','BANKNIFTY','FINNIFTY'}
  assert by_name['NIFTY']['pcr_oi']==0.88 and by_name['NIFTY']['max_pain_strike']==25000.0
  assert [p['captured_at'] for p in by_name['NIFTY']['series']]==[EARLIER,MARK],'oldest first, nothing filled in'
  assert by_name['FINNIFTY']['captured'] is False,'an index with no capture says so; it shows no numbers'
  assert by_name['FINNIFTY']['pcr_oi'] is None
 finally:app.state.db.close()

def test_futures_card_is_the_front_contract_with_its_basis(tmp_path,store):
 app,client=client_for(tmp_path,derivatives_database=store)
 try:
  body=client.get('/api/derivatives/futures').json()
  by_name={r['underlying']:r for r in body['rows']}
  assert set(by_name)=={'NIFTY','RELIANCE'}
  assert by_name['NIFTY']['expiry']==EXPIRY and by_name['NIFTY']['buildup_day']=='long_buildup'
  assert by_name['NIFTY']['basis']==pytest.approx(25150.0-25120.0)
  assert by_name['RELIANCE']['basis']==pytest.approx(1412.0-1408.0)
  indices=client.get('/api/derivatives/futures?watchlist=indices').json()
  assert {r['underlying'] for r in indices['rows']}=={'NIFTY'}
 finally:app.state.db.close()

def test_series_serves_a_contract_and_an_underlying(tmp_path,store):
 app,client=client_for(tmp_path,derivatives_database=store)
 try:
  contract=client.get('/api/derivatives/series?instrument_token=101').json()
  assert contract['kind']=='contract' and contract['tradingsymbol']=='NIFTY25SEP25000CE'
  assert [p['t'] for p in contract['points']]==[EARLIER,MARK]
  assert contract['points'][-1]['price']==142.5 and contract['points'][-1]['oi']==3_750_000
  underlying=client.get('/api/derivatives/series?underlying=NIFTY').json()
  assert underlying['kind']=='underlying' and underlying['price_label']=='Spot'
  assert underlying['points'][-1]['price']==25120.0
  assert underlying['points'][-1]['oi']==5_200_000+4_600_000
 finally:app.state.db.close()

def test_days_to_expiry_filter(tmp_path,store):
 app,client=client_for(tmp_path,derivatives_database=store)
 try:
  wide=client.get('/api/derivatives/unusual?max_dte=400').json()
  assert {s['expiry'] for r in wide['rows'] for s in r['strikes']}=={EXPIRY,FAR}
  # the filter is inclusive of rows whose expiry date could not be read: those carry days_to_expiry=None
  # and are dropped rather than guessed.
  for row in wide['rows']:
   for strike in row['strikes']:assert strike['days_to_expiry'] is not None
 finally:app.state.db.close()


# --- the boundary ---------------------------------------------------------------------------------------
def test_the_store_is_opened_read_only(store):
 reader=D.Derivatives(store)
 assert reader.available()
 connection=reader._connect()
 with pytest.raises(sqlite3.OperationalError):
  connection.execute("insert into contracts(instrument_token,underlying) values(999,'X')")
 reader.close()

def test_a_store_without_metrics_yet_still_lists_contracts(tmp_path):
 """D1 captures before D2 computes: the filters card works, the screens say there is no metric yet."""
 path=tmp_path/'derivatives.db';build_store(path,metrics=False)
 app,client=client_for(tmp_path,derivatives_database=str(path))
 try:
  filters=client.get('/api/derivatives/filters').json()
  assert filters['available'] is True
  assert {u['underlying'] for u in filters['underlyings']}=={'NIFTY','RELIANCE'}
  assert any(u['is_index'] for u in filters['underlyings'])
  unusual=client.get('/api/derivatives/unusual').json()
  assert unusual['rows']==[] and unusual['available'] is True
 finally:app.state.db.close()

def test_metrics_module_is_used_when_it_exposes_a_reader(tmp_path,store):
 """D2's own reader wins, and the response says which source answered."""
 class Module:
  @staticmethod
  def futures_buildup(**_kwargs):
   return [{'instrument_token':301,'tradingsymbol':'NIFTY25SEPFUT','underlying':'NIFTY','instrument_type':'FUT',
    'expiry':EXPIRY,'lot_size':75,'captured_at':MARK,'last_price':25150.0,'oi':12_000_000,'basis':30.0,
    'buildup_day':'long_buildup','oi_vs_20d_avg':1.3,'premium_cr':300.0}]
 reader=D.Derivatives(store,metrics_module=Module)
 body=reader.futures()
 assert body['source']=='metrics_module'
 assert [r['underlying'] for r in body['rows']]==['NIFTY']
 assert body['rows'][0]['basis']==30.0
 reader.close()

def test_a_reader_that_fails_falls_back_to_the_store(tmp_path,store):
 class Module:
  @staticmethod
  def futures_buildup(**_kwargs):raise RuntimeError('not ready')
 reader=D.Derivatives(store,metrics_module=Module)
 body=reader.futures()
 assert body['source']=='store' and {r['underlying'] for r in body['rows']}=={'NIFTY','RELIANCE'}
 reader.close()


# --- the owner's ΔOI strike grid -------------------------------------------------------------------------
# A second store, richer than the one above: a full strike ladder around the money, the PREVIOUS session's
# closing OI in `candles_15m`, and seven 15-minute marks in `snapshots`. Everything the grid says has to come
# out of these rows - nothing here may be smoothed, carried forward or invented.
GRID_DAY='2026-09-18'
PRIOR_CLOSE='2026-09-17 15:15'
GRID_MARKS=[f'{GRID_DAY} {t}' for t in ('09:30','09:45','10:00','10:15','10:30','10:45','11:00')]
GRID_SPOT=25120.0
GRID_ATM=25100.0
GRID_SCHEMA='''
CREATE TABLE contracts(instrument_token INTEGER PRIMARY KEY, tradingsymbol TEXT, underlying TEXT,
 instrument_type TEXT, strike REAL, expiry TEXT, lot_size INTEGER, first_seen TEXT, last_seen TEXT,
 in_scope INTEGER DEFAULT 1);
CREATE TABLE snapshots(instrument_token INTEGER, captured_at TEXT, mark_kind TEXT DEFAULT 'bar_close',
 last_price REAL, average_price REAL, volume INTEGER, oi INTEGER, source TEXT);
CREATE TABLE candles_15m(instrument_token INTEGER, bar_start TEXT, open REAL, high REAL, low REAL,
 close REAL, volume INTEGER, oi INTEGER);
CREATE TABLE underlying_snapshots(underlying TEXT, captured_at TEXT, mark_kind TEXT DEFAULT 'bar_close',
 spot REAL, spot_symbol TEXT, fut_price REAL, total_ce_oi INTEGER, total_pe_oi INTEGER,
 pcr_oi REAL, pcr_volume REAL, max_pain_strike REAL);
CREATE TABLE metrics(instrument_token INTEGER, captured_at TEXT, last_price REAL, oi INTEGER);
'''
#: strike -> (CE token, PE token). 25100 is the strike nearest the 25,120 spot, so it is the ATM.
GRID_TOKENS={24700:(4701,4702),24800:(4801,4802),24900:(4901,4902),25000:(5001,5002),25100:(5101,5102),
 25200:(5201,5202),25300:(5301,5302),25400:(5401,5402),25500:(5501,5502),25600:(5601,5602)}
#: The ATM+4 call is deliberately NOT listed, so the grid has to keep its slot and say so.
GRID_UNLISTED=(5501,)
#: token -> previous-session closing OI. 25000 PE (5002) is deliberately absent from this map: it is captured at
#: every mark but was never backfilled, so it has no previous close - and therefore no ΔOI, ever.
GRID_PREVIOUS={5101:1_000_000,5102:1_000_000,5201:800_000,5301:600_000,5401:500_000,
 4901:400_000,4902:400_000,4801:350_000,4802:350_000,4701:300_000,4702:300_000}
#: token -> the ΔOI (against that previous close) at each of the seven marks. None marks a mark that was not
#: captured at all for that contract; the series simply has no point there.
GRID_DELTAS={
 5101:[0,50_000,100_000,150_000,200_000,250_000,300_000],          # ATM CE: building
 5102:[0,-40_000,-80_000,-120_000,-160_000,-200_000,-240_000],     # ATM PE: unwinding
 5201:[0,100_000,200_000,200_000,201_000,202_000,203_000],         # ATM+1 CE: flat inside the 5% band
 5301:[0],                                                          # ATM+2 CE: one mark only -> no baseline
 5401:[0,20_000,None,None,60_000,70_000,80_000],                   # ATM+3 CE: a gap in the capture
 4901:[0,30_000,60_000,90_000,120_000,150_000,180_000],            # ATM−2 PE
 4902:[0,30_000,60_000,90_000,120_000,150_000,180_000],
 4801:[0,10_000,20_000,30_000,40_000,50_000,60_000],
 4802:[0,10_000,20_000,30_000,40_000,50_000,60_000],
 4701:[0,5_000,10_000,15_000,20_000,25_000,30_000],
 4702:[0,5_000,10_000,15_000,20_000,25_000,30_000],
}
#: token -> that contract's OWN last traded price at each of the seven marks. A None here is a mark that WAS
#: captured but carried no price: the price line must show a gap there and the ΔOI line must not.
#: The prices are chosen so that between them the ten slots exercise a call row, a put row, a flat reading and
#: both no-baseline cases of the price+OI table.
GRID_PRICES={
 5101:[100.0,110.0,120.0,130.0,140.0,150.0,160.0],   # ATM CE: price up, OI building  -> call buying
 5102:[80.0,88.0,96.0,104.0,112.0,120.0,128.0],      # ATM PE: price up, OI unwinding -> put short covering
 5201:[50.0,51.0,52.0,53.0,54.0,55.0,56.0],          # ATM+1 CE: OI is flat, so the reading is "very little"
 5301:[40.0],                                         # ATM+2 CE: one mark only
 5401:[30.0,33.0,None,None,None,40.0,42.0],          # ATM+3 CE: a capture gap AND a mark with no price
 4901:[70.0,70.0,70.0,70.0,70.0,70.0,70.0],
 4902:[200.0,190.0,180.0,170.0,160.0,150.0,140.0],   # ATM−2 PE: price down, OI building -> put writing
 4801:[60.0,66.0,72.0,78.0,84.0,90.0,96.0],
 4802:[60.0,66.0,72.0,78.0,84.0,90.0,96.0],          # ATM−3 PE: price up, OI building   -> put buying
 4701:[25.0,25.0,25.0,25.0,25.0,25.0,25.0],
 4702:[25.0,25.0,25.0,25.0,25.0,25.0,25.0],          # ATM−4 PE: price flat             -> very little change
}


def build_grid_store(path):
 connection=sqlite3.connect(path)
 connection.executescript(GRID_SCHEMA)
 rows=[]
 for value,(ce,pe) in GRID_TOKENS.items():
  for token,kind in ((ce,'CE'),(pe,'PE')):
   if token in GRID_UNLISTED:continue
   rows.append((token,f'NIFTY25SEP{value:g}{kind}','NIFTY',kind,float(value),EXPIRY,75,PRIOR_CLOSE,GRID_MARKS[-1],1))
 connection.executemany('insert into contracts(instrument_token,tradingsymbol,underlying,instrument_type,strike,'
  'expiry,lot_size,first_seen,last_seen,in_scope) values(?,?,?,?,?,?,?,?,?,?)',rows)
 # the previous session's LAST bar: this, and only this, is what ΔOI is measured against
 connection.executemany('insert into candles_15m(instrument_token,bar_start,close,volume,oi) values(?,?,?,?,?)',
  [(token,PRIOR_CLOSE,100.0,1000,oi) for token,oi in GRID_PREVIOUS.items()])
 # and an earlier bar of the same prior session, so "the LAST bar of the LAST day" is actually being picked
 connection.executemany('insert into candles_15m(instrument_token,bar_start,close,volume,oi) values(?,?,?,?,?)',
  [(token,'2026-09-17 09:30',100.0,1000,oi+999_999) for token,oi in GRID_PREVIOUS.items()])
 snaps=[]
 for token,deltas in GRID_DELTAS.items():
  close=GRID_PREVIOUS.get(token)
  prices=GRID_PRICES.get(token) or []
  for i,(mark,delta) in enumerate(zip(GRID_MARKS,deltas)):
   if delta is None:continue  # a mark that was never captured for this contract
   price=prices[i] if i<len(prices) else None
   snaps.append((token,mark,'bar_close',price,1000,(close or 0)+delta))
 # 25000 PE is captured at every mark but was never backfilled, so it has no previous close
 snaps.extend((5002,mark,'bar_close',12.0+i,1000,700_000+i*10_000) for i,mark in enumerate(GRID_MARKS))
 connection.executemany('insert into snapshots(instrument_token,captured_at,mark_kind,last_price,volume,oi)'
  ' values(?,?,?,?,?,?)',snaps)
 connection.executemany('insert into underlying_snapshots(underlying,captured_at,mark_kind,spot,spot_symbol)'
  ' values(?,?,?,?,?)',[('NIFTY',mark,'bar_close',GRID_SPOT,'NSE:NIFTY 50') for mark in GRID_MARKS])
 connection.commit();connection.close()


@pytest.fixture
def grid_store(tmp_path):
 path=tmp_path/'grid.db';build_grid_store(path);return str(path)


def test_oi_grid_needs_an_underlying(tmp_path):
 app,client=client_for(tmp_path)
 try:
  assert client.get('/api/derivatives/oi-grid').status_code==400
  assert client.get('/api/derivatives/oi-grid?underlying=N%27;drop').status_code==400
  body=client.get('/api/derivatives/oi-grid?underlying=NIFTY').json()
  assert body['available'] is False and body['captured'] is False
  assert body['empty_reason']==D.EMPTY_TEXT and body['rows']==[]
 finally:app.state.db.close()


def test_oi_grid_is_always_the_same_ten_slots_in_the_owners_order(tmp_path,grid_store):
 """ATM CE then +1..+4 CE, ATM PE then −1..−4 PE. Ten slots, always, in that order."""
 app,client=client_for(tmp_path,derivatives_database=grid_store)
 try:
  body=client.get('/api/derivatives/oi-grid?underlying=NIFTY').json()
  assert body['available'] and body['captured'] and body['as_of']==GRID_MARKS[-1]
  assert len(body['rows'])==D.GRID_SLOTS==10
  assert [(r['option_type'],r['atm_offset']) for r in body['rows']]==[
   ('CE',0),('CE',1),('CE',2),('CE',3),('CE',4),('PE',0),('PE',-1),('PE',-2),('PE',-3),('PE',-4)]
  assert [r['label'] for r in body['rows']]==['ATM CE','ATM+1 CE','ATM+2 CE','ATM+3 CE','ATM+4 CE',
   'ATM PE','ATM−1 PE','ATM−2 PE','ATM−3 PE','ATM−4 PE']
  assert [r['row'] for r in body['rows']]==['calls']*5+['puts']*5
  # the ATM is the listed strike nearest the captured spot, and the block says which spot it used
  assert body['atm_strike']==GRID_ATM and body['spot']==GRID_SPOT
  assert body['atm_basis']['mark']==GRID_MARKS[-1] and body['atm_basis']['rule']=='listed strike nearest spot'
  assert [r['strike'] for r in body['rows']]==[25100.0,25200.0,25300.0,25400.0,None,
   25100.0,25000.0,24900.0,24800.0,24700.0]
 finally:app.state.db.close()


def test_delta_oi_is_measured_against_the_previous_close(tmp_path,grid_store):
 """ΔOI = captured OI − the previous session's CLOSING OI. The line starts at 0 at the day's first mark."""
 app,client=client_for(tmp_path,derivatives_database=grid_store)
 try:
  body=client.get('/api/derivatives/oi-grid?underlying=NIFTY').json()
  atm=body['rows'][0]
  assert atm['tradingsymbol']=='NIFTY25SEP25100CE'
  assert atm['previous_close_oi']==1_000_000
  assert [p['delta_oi'] for p in atm['points']]==GRID_DELTAS[5101]
  assert atm['points'][0]['delta_oi']==0,'every line starts at 0 at the first mark of the day'
  assert [p['oi'] for p in atm['points']]==[1_000_000+d for d in GRID_DELTAS[5101]]
  assert [p['at'] for p in atm['points']]==GRID_MARKS
  # a contract the backfill never covered has NO previous close, so it has no ΔOI at all - never a zero line
  put=next(r for r in body['rows'] if r['strike']==25000.0)
  assert put['previous_close_oi'] is None
  assert [p['delta_oi'] for p in put['points']]==[None]*len(GRID_MARKS)
  assert put['direction']=='no baseline' and put['latest_delta_oi'] is None
 finally:app.state.db.close()


def test_direction_is_the_hour_lookback_with_a_five_percent_flat_band(tmp_path,grid_store):
 app,client=client_for(tmp_path,derivatives_database=grid_store)
 try:
  body=client.get('/api/derivatives/oi-grid?underlying=NIFTY').json()
  found={r['slot']:r for r in body['rows']}
  assert found['CE+0']['direction']=='building'
  assert found['PE+0']['direction']=='unwinding'
  assert found['CE+1']['direction']=='flat','a change inside 5% of the contract own peak is flat'
  assert found['CE+2']['direction']=='no baseline','one mark is not a baseline'
  assert found['CE+0']['direction_detail']['marks_back']==D.DIRECTION_LOOKBACK_MARKS
  assert found['CE+0']['direction_detail']['change']==200_000
  assert body['direction_lookback_marks']==4 and body['flat_fraction']==0.05
 finally:app.state.db.close()


def test_the_direction_rule_itself(tmp_path):
 """The rule stated once, tested on its own: an hour back, a 5% flat band, and two marks before any of it."""
 reader=D.Derivatives(None)
 mark=lambda i,v:{'at':f'{GRID_DAY} 10:{i:02d}','delta_oi':v}
 assert reader.grid_direction([])[0]=='no baseline'
 assert reader.grid_direction([mark(0,0)])[0]=='no baseline'
 assert reader.grid_direction([mark(0,None),mark(1,5)])[0]=='no baseline','a None is not a mark'
 assert reader.grid_direction([mark(0,0),mark(1,100)])[0]=='building'
 assert reader.grid_direction([mark(0,0),mark(1,-100)])[0]=='unwinding'
 assert reader.grid_direction([mark(0,0),mark(1,0)])[0]=='flat','no movement at all is flat, never a direction'
 # the flat band is 5% of the contract's OWN largest |ΔOI|, which here is the later of the two points
 assert reader.grid_direction([mark(0,1000),mark(1,1049)])[0]=='flat','49 is inside 5% of 1049'
 assert reader.grid_direction([mark(0,1000),mark(1,1060)])[0]=='building','60 is outside 5% of 1060'
 # with more than four marks the reference is exactly four back, not the first point
 far=[mark(i,v) for i,v in enumerate([0,1000,2000,3000,4000,4100])]
 label,detail=reader.grid_direction(far)
 assert label=='building' and detail['marks_back']==D.DIRECTION_LOOKBACK_MARKS
 assert detail['change']==4100-1000


def test_a_gap_in_the_capture_stays_a_gap(tmp_path,grid_store):
 """Two marks were never captured for ATM+3 CE. They are absent - nothing is interpolated or carried forward."""
 app,client=client_for(tmp_path,derivatives_database=grid_store)
 try:
  body=client.get('/api/derivatives/oi-grid?underlying=NIFTY').json()
  gapped=next(r for r in body['rows'] if r['slot']=='CE+3')
  assert [p['at'] for p in gapped['points']]==[GRID_MARKS[i] for i in (0,1,4,5,6)]
  assert len(gapped['points'])==5<len(GRID_MARKS)
  assert all(p['delta_oi'] is not None for p in gapped['points'])
  # the block's own mark list is the union of what was captured, so the gap is visible against it
  assert body['marks']==GRID_MARKS
 finally:app.state.db.close()


def test_a_strike_that_is_not_listed_keeps_its_slot_and_says_so(tmp_path,grid_store):
 app,client=client_for(tmp_path,derivatives_database=grid_store)
 try:
  body=client.get('/api/derivatives/oi-grid?underlying=NIFTY').json()
  absent=next(r for r in body['rows'] if r['slot']=='CE+4')
  assert absent['present'] is False and absent['strike'] is None and absent['points']==[]
  assert absent['direction']=='no baseline'
  assert 'ATM+4 CE' in absent['missing_text'] and 'not a listed strike' in absent['missing_text']
  assert body['total']==9,'nine of the ten slots are listed in this expiry'
 finally:app.state.db.close()


def test_the_grid_says_not_enough_marks_before_two(tmp_path,store):
 """The plain store has no snapshots and no previous close: the block says so, and it is not an error."""
 app,client=client_for(tmp_path,derivatives_database=store)
 try:
  result=client.get('/api/derivatives/oi-grid?underlying=NIFTY')
  assert result.status_code==200
  body=result.json()
  assert body['available'] is True and body['rows']==[]
  assert body['empty_note']==D.NOT_ENOUGH_MARKS==body['not_enough_marks']
  # the reason travels beside the owner's sentence rather than replacing it, and it is never an error
  assert body['empty_detail'] and 'snapshot' in body['empty_detail'].lower()
 finally:app.state.db.close()


def test_the_grid_states_its_definitions_and_never_predicts(tmp_path,grid_store):
 app,client=client_for(tmp_path,derivatives_database=grid_store)
 try:
  body=client.get('/api/derivatives/oi-grid?underlying=NIFTY').json()
  assert 'since the previous close' in body['delta_oi_text']
  assert 'nearest spot' in body['atm_text']
  assert 'no baseline' in body['direction_text']
  assert body['floors']=={'premium_cr':2.0,'oi_lots':1,'last_price':1.0}
  assert body['expiry']==EXPIRY and body['session']==GRID_DAY
  assert body['points_source'] in ('snapshots','candles_15m','read_api')
  banned=('bullish','bearish','will ','expect','forecast','predict','likely','target')
  text=' '.join(str(v) for v in body.values() if isinstance(v,str)).lower()
  for word in banned:assert word not in text,word
  for row in body['rows']:
   assert row['direction'] in D.GRID_DIRECTIONS
 finally:app.state.db.close()


# --- the second line: the contract's own price ---------------------------------------------------------------
def test_every_point_carries_the_contracts_own_price(tmp_path,grid_store):
 """A point is {at, oi, delta_oi, price}. `price` is `snapshots.last_price` - nothing derived, nothing borrowed."""
 app,client=client_for(tmp_path,derivatives_database=grid_store)
 try:
  body=client.get('/api/derivatives/oi-grid?underlying=NIFTY').json()
  atm=body['rows'][0]
  assert [p['price'] for p in atm['points']]==GRID_PRICES[5101]
  assert sorted(atm['points'][0])==['at','delta_oi','oi','price']
  assert atm['latest_price']==GRID_PRICES[5101][-1] and atm['marks_with_price']==7
  # the put row reads its OWN premium, never the call's
  put=next(r for r in body['rows'] if r['slot']=='PE-2')
  assert [p['price'] for p in put['points']]==GRID_PRICES[4902]
 finally:app.state.db.close()


def test_a_mark_with_no_price_is_a_gap_on_the_price_line_only(tmp_path,grid_store):
 """ATM+3 CE was captured at 10:30 with no last price. The price is None there; the OI beside it is not."""
 app,client=client_for(tmp_path,derivatives_database=grid_store)
 try:
  body=client.get('/api/derivatives/oi-grid?underlying=NIFTY').json()
  gapped=next(r for r in body['rows'] if r['slot']=='CE+3')
  # five captured marks: 09:30, 09:45, 10:30, 10:45, 11:00 - 10:30 carries no price
  assert [p['price'] for p in gapped['points']]==[30.0,33.0,None,40.0,42.0]
  assert all(p['delta_oi'] is not None for p in gapped['points']),'the ΔOI line is untouched by a missing price'
  assert gapped['marks']==5 and gapped['marks_with_price']==4
  assert gapped['latest_price']==42.0
  # nothing is interpolated or carried forward: the gap is exactly where the store has no number
  assert gapped['points'][2]['price'] is None
 finally:app.state.db.close()


def test_a_contract_with_no_price_at_all_has_no_reading(tmp_path,grid_store):
 """Strip every last price from one contract: the price line vanishes and the reading says so - never a guess."""
 connection=sqlite3.connect(grid_store)
 connection.execute('update snapshots set last_price=null where instrument_token=?',(5101,))
 connection.commit();connection.close()
 app,client=client_for(tmp_path,derivatives_database=grid_store)
 try:
  body=client.get('/api/derivatives/oi-grid?underlying=NIFTY').json()
  atm=body['rows'][0]
  assert all(p['price'] is None for p in atm['points'])
  assert atm['marks_with_price']==0 and atm['latest_price'] is None
  assert atm['direction']=='building','the ΔOI reading is unaffected by a missing price'
  assert atm['flow']['price_direction']=='no baseline'
  assert atm['flow']['what_label']==D.FLOW_NOT_ENOUGH and atm['flow']['meaning'] is None
 finally:app.state.db.close()


# --- price and ΔOI read together -------------------------------------------------------------------------------
def test_the_price_and_oi_reading_on_the_served_grid(tmp_path,grid_store):
 app,client=client_for(tmp_path,derivatives_database=grid_store)
 try:
  body=client.get('/api/derivatives/oi-grid?underlying=NIFTY').json()
  found={r['slot']:r for r in body['rows']}
  assert found['CE+0']['flow']['price_direction']=='up'
  assert found['CE+0']['flow']['oi_direction']=='building'
  assert found['CE+0']['flow']['what_label']=='Call buying increasing'
  assert found['CE+0']['flow']['meaning']=='Traders are buying upside'
  assert found['PE+0']['flow']['what_label']=='Put short covering'
  assert found['PE-2']['flow']['what_label']=='Put writing increasing'
  assert found['PE-3']['flow']['what_label']=='Put buying increasing'
  # a flat premium over an hour in which open interest plainly moved is NOT "nothing happened"
  assert found['PE-4']['flow']['price_direction']=='flat'
  assert found['PE-4']['flow']['oi_direction']=='building'
  assert found['PE-4']['flow']['what_label']=='New positions added'
  assert found['PE-4']['flow']['meaning']=='Premium barely moved'
  # and the other way round: the premium moved, the book did not
  assert found['CE+1']['flow']['price_direction']=='up'
  assert found['CE+1']['flow']['oi_direction']=='flat'
  assert found['CE+1']['flow']['what_label']=='Premium rose'
  assert found['CE+1']['flow']['meaning']=='Open interest barely moved'
  assert found['CE+2']['flow']['what_label']==D.FLOW_NOT_ENOUGH,'one reading is not a baseline'
  assert found['PE-1']['flow']['what_label']==D.FLOW_NOT_ENOUGH,'no previous close, so no ΔOI baseline'
  assert found['CE+4']['flow']['what_label']==D.FLOW_NOT_ENOUGH,'an unlisted strike reads as no baseline'
  # the workings travel with it, over the SAME window the direction chip uses
  detail=found['CE+0']['flow']['detail']
  assert detail['readings_back']==D.DIRECTION_LOOKBACK_MARKS
  assert detail['from']==GRID_MARKS[2] and detail['to']==GRID_MARKS[-1]
  assert detail['price_change']==40.0 and detail['oi_change']==200_000
  assert detail['price_change_pct']==pytest.approx(40/120*100,rel=1e-3)
 finally:app.state.db.close()


#: the six series the table's nine combinations are built from
FLOW_OI={'building':[0,20_000,40_000,60_000,80_000,100_000],
 'unwinding':[0,-20_000,-40_000,-60_000,-80_000,-100_000],'flat':[0,0,0,0,0,0]}
FLOW_PRICE={'up':[100.0,104.0,109.0,115.0,122.0,130.0],'down':[130.0,122.0,115.0,109.0,104.0,100.0],
 'flat':[100.0]*6}
#: every one of the nine (price x OI) combinations, per option type. There is no fall-through case: a flat
#: axis has its OWN wording, because collapsing it into the both-flat row put a tile at war with its own chip.
FLOW_TABLE={
 ('CE','down','building'):('Call writing increasing','Sellers are building resistance'),
 ('CE','up','unwinding'):('Call short covering','Call sellers are exiting'),
 ('CE','up','building'):('Call buying increasing','Traders are buying upside'),
 ('CE','down','unwinding'):('Call buyers exiting','Call buyers are closing out'),
 ('CE','flat','building'):('New positions added','Premium barely moved'),
 ('CE','flat','unwinding'):('Positions closing out','Premium barely moved'),
 ('CE','up','flat'):('Premium rose','Open interest barely moved'),
 ('CE','down','flat'):('Premium fell','Open interest barely moved'),
 ('CE','flat','flat'):('Very little change','Positioning is unchanged'),
 ('PE','down','building'):('Put writing increasing','Sellers are building support'),
 ('PE','up','unwinding'):('Put short covering','Put sellers are exiting'),
 ('PE','up','building'):('Put buying increasing','Traders are buying downside protection'),
 ('PE','down','unwinding'):('Put buyers exiting','Put buyers are closing out'),
 ('PE','flat','building'):('New positions added','Premium barely moved'),
 ('PE','flat','unwinding'):('Positions closing out','Premium barely moved'),
 ('PE','up','flat'):('Premium rose','Open interest barely moved'),
 ('PE','down','flat'):('Premium fell','Open interest barely moved'),
 ('PE','flat','flat'):('Very little change','Positioning is unchanged'),
}


def flow_points(deltas,prices):
 return [{'at':f'{GRID_DAY} 10:{i:02d}','oi':1_000_000+d,'delta_oi':d,'price':p}
  for i,(d,p) in enumerate(zip(deltas,prices))]


@pytest.mark.parametrize('key',sorted(FLOW_TABLE))
def test_every_row_of_the_price_and_oi_table(key):
 """All eighteen rows on the rule itself. Identical mechanics; only the wording differs by side."""
 reader=D.Derivatives(None)
 kind,price_way,oi_way=key
 flow=reader.grid_flow(kind,flow_points(FLOW_OI[oi_way],FLOW_PRICE[price_way]))
 assert flow['price_direction']==price_way and flow['oi_direction']==oi_way,key
 assert (flow['what_label'],flow['meaning'])==FLOW_TABLE[key],key


def test_the_table_the_server_serves_is_that_table(tmp_path):
 assert dict(D.FLOW_LABELS)=={f'{k[0]}|{k[1]}|{k[2]}':v for k,v in FLOW_TABLE.items()}
 assert len(D.FLOW_LABELS)==18,'nine price x OI combinations for calls and nine for puts'
 for kind in ('CE','PE'):
  for price_way in ('up','down','flat'):
   for oi_way in ('building','unwinding','flat'):
    assert f'{kind}|{price_way}|{oi_way}' in D.FLOW_LABELS


def test_the_sentence_never_contradicts_the_chip(tmp_path):
 """The rule as a property over the whole table, not a list of cases.

 The bug this exists for: a tile read "↓ UNWINDING" over "Positioning is unchanged" while 3.7 million
 contracts closed in that hour. When OI has a direction the sentence must say something happened to open
 interest; when OI is flat it must say open interest barely moved.
 """
 moved=re.compile(r'\b(writing|covering|buying|exiting|added|closing out)\b',re.I)
 still=re.compile(r'\b(open interest barely moved|positioning is unchanged)\b',re.I)
 reader=D.Derivatives(None)
 for key in FLOW_TABLE:
  kind,price_way,oi_way=key
  what,meaning=FLOW_TABLE[key]
  sentence=f'{what}. {meaning}'
  if oi_way=='flat':
   assert still.search(sentence),key
   assert not moved.search(what),key
  else:
   assert moved.search(what),key
   assert not still.search(sentence),key
   assert (what,meaning)!=(D.FLOW_FLAT_WHAT,D.FLOW_FLAT_MEANING),key
  # and the same on the runtime path, from real points
  flow=reader.grid_flow(kind,flow_points(FLOW_OI[oi_way],FLOW_PRICE[price_way]))
  assert (flow['what_label'],flow['meaning'])==(what,meaning),key


def test_the_bug_the_owner_saw_on_the_atm_put(tmp_path):
 """Unwinding open interest beside a barely-moved premium is never "positioning is unchanged"."""
 reader=D.Derivatives(None)
 # the premium jumped early and has barely moved since, so it is flat against its OWN largest move
 barely=[120.0,140.0,140.0,140.0,140.0,140.05]
 flow=reader.grid_flow('PE',flow_points(FLOW_OI['unwinding'],barely))
 assert flow['price_direction']=='flat' and flow['oi_direction']=='unwinding'
 assert flow['what_label']=='Positions closing out'
 assert flow['meaning']=='Premium barely moved'
 assert flow['what_label']!=D.FLOW_FLAT_WHAT
 assert 'unchanged' not in f"{flow['what_label']} {flow['meaning']}" 


def test_no_baseline_on_either_side_is_never_guessed(tmp_path):
 reader=D.Derivatives(None)
 one=[{'at':f'{GRID_DAY} 10:00','oi':1,'delta_oi':0,'price':100.0}]
 for kind in ('CE','PE'):
  assert reader.grid_flow(kind,[])['what_label']==D.FLOW_NOT_ENOUGH
  assert reader.grid_flow(kind,one)['what_label']==D.FLOW_NOT_ENOUGH
  assert reader.grid_flow(kind,one)['meaning'] is None
  # a ΔOI baseline with no price baseline is still "not enough"
  priced=[{'at':f'{GRID_DAY} 10:{i:02d}','oi':1,'delta_oi':i*1000,'price':None} for i in range(6)]
  assert reader.grid_flow(kind,priced)['price_direction']=='no baseline'
  assert reader.grid_flow(kind,priced)['what_label']==D.FLOW_NOT_ENOUGH
  # and a price baseline with no ΔOI baseline, the other way round
  unpriced=[{'at':f'{GRID_DAY} 10:{i:02d}','oi':1,'delta_oi':None,'price':100.0+i} for i in range(6)]
  assert reader.grid_flow(kind,unpriced)['oi_direction']=='no baseline'
  assert reader.grid_flow(kind,unpriced)['what_label']==D.FLOW_NOT_ENOUGH
 # an instrument type the table does not know is never quietly read as a call
 both=[{'at':f'{GRID_DAY} 10:{i:02d}','oi':1,'delta_oi':i*1000,'price':100.0+i} for i in range(6)]
 assert reader.grid_flow('FUT',both)['what_label']==D.FLOW_NOT_ENOUGH


def test_the_price_flat_band_is_five_percent_of_the_contracts_own_move(tmp_path):
 """The same 5% shape as ΔOI, on the contract's own premium, measured from its first priced reading."""
 reader=D.Derivatives(None)
 def at(prices):
  return [{'at':f'{GRID_DAY} 10:{i:02d}','oi':1,'delta_oi':i,'price':p} for i,p in enumerate(prices)]
 assert D.PRICE_FLAT_FRACTION==D.FLAT_FRACTION==0.05
 # first 100, peak 200 -> the band is ₹5; +4 over the hour is inside it, +6 is outside
 assert reader.grid_price_direction(at([100,200,200,200,200,204]))[0]=='flat'
 assert reader.grid_price_direction(at([100,200,200,200,200,206]))[0]=='up'
 assert reader.grid_price_direction(at([100,200,200,200,200,194]))[0]=='down'
 assert reader.grid_price_direction(at([120,120]))[0]=='flat','no move at all is flat, never a direction'
 label,detail=reader.grid_price_direction(at([100,120,140,160,180,200]))
 assert label=='up' and detail['readings_back']==D.DIRECTION_LOOKBACK_MARKS
 assert detail['price_change']==80 and detail['from']==f'{GRID_DAY} 10:01'
 assert detail['price_flat_threshold']==pytest.approx(D.PRICE_FLAT_FRACTION*100)
 # a mark with no price is skipped by the rule, exactly as it is skipped by the line
 assert reader.grid_price_direction(at([100,110,None,None,160]))[0]=='up'
 assert reader.grid_price_direction(at([100]))[0]=='no baseline'
 assert reader.grid_price_direction([])[0]=='no baseline'


# --- the one line under the whole block --------------------------------------------------------------------------
def test_the_block_line_only_speaks_when_both_sides_build_similarly(tmp_path):
 reader=D.Derivatives(None)
 def side(row,scale):
  return {'row':row,'flow':{'detail':{'oi_change':scale}}}
 assert D.BLOCK_BALANCE_RATIO==1.33
 assert reader.grid_block_read([side('calls',100_000),side('puts',100_000)])==D.BLOCK_BOTH_BUILDING
 assert reader.grid_block_read([side('calls',100_000),side('puts',130_000)])==D.BLOCK_BOTH_BUILDING
 assert reader.grid_block_read([side('calls',100_000),side('puts',140_000)])=='','a third larger says nothing'
 assert reader.grid_block_read([side('calls',-100_000),side('puts',-100_000)])=='','both unwinding'
 assert reader.grid_block_read([side('calls',100_000),side('puts',-100_000)])=='','one each way'
 assert reader.grid_block_read([side('calls',100_000)])=='','calls alone is not both sides'
 assert reader.grid_block_read([side('puts',100_000)])==''
 assert reader.grid_block_read([])=='' and reader.grid_block_read(None)==''
 # a slot with no baseline contributes nothing rather than a zero
 assert reader.grid_block_read([side('calls',100_000),{'row':'puts','flow':{'detail':{}}}])==''


def test_the_block_line_is_served_with_the_grid(tmp_path,grid_store):
 """It aggregates the ten slots already on the block; it never reads a strike the grid is not showing."""
 app,client=client_for(tmp_path,derivatives_database=grid_store)
 try:
  body=client.get('/api/derivatives/oi-grid?underlying=NIFTY').json()
  # in this fixture the calls build and the puts are mixed, so the block has nothing to say
  assert body['block_read']==''
  assert body['block_balance_ratio']==D.BLOCK_BALANCE_RATIO
  assert body['price_flat_fraction']==D.PRICE_FLAT_FRACTION
 finally:app.state.db.close()


def test_the_block_line_appears_when_both_sides_really_do_build(tmp_path,grid_store):
 """Turn the ATM put from unwinding to building, sized so the two sides are within a third of each other."""
 connection=sqlite3.connect(grid_store)
 # calls contribute +200,000 (ATM) +3,000 (ATM+1) +80,000 (ATM+3) = 283,000 over the hour; the other three
 # puts contribute 180,000, so an ATM put of +100,000 puts the two sides inside BLOCK_BALANCE_RATIO.
 for i,mark in enumerate(GRID_MARKS):
  connection.execute('update snapshots set oi=? where instrument_token=? and captured_at=?',
   (1_000_000+i*25_000,5102,mark))
 connection.commit();connection.close()
 app,client=client_for(tmp_path,derivatives_database=grid_store)
 try:
  body=client.get('/api/derivatives/oi-grid?underlying=NIFTY').json()
  found={r['slot']:r for r in body['rows']}
  assert found['CE+0']['direction']=='building' and found['PE+0']['direction']=='building'
  assert body['block_read']==D.BLOCK_BOTH_BUILDING
  assert 'mark' not in body['block_read'].lower()
  for word in ('bullish','bearish','will ','forecast','predict','target'):
   assert word not in body['block_read'].lower()
 finally:app.state.db.close()


def test_the_block_states_the_price_line_and_stays_honest(tmp_path,grid_store):
 app,client=client_for(tmp_path,derivatives_database=grid_store)
 try:
  body=client.get('/api/derivatives/oi-grid?underlying=NIFTY').json()
  assert 'own scale' in body['price_text']
  assert 'buyer and a seller' in body['flow_text']
  assert 'do not say what happens next' in body['flow_text']
  assert 'both building' in body['block_text'] and '1.33' in body['block_text']
  # §5 over every sentence this block can now put on screen
  banned=('bullish','bearish','will ','expect','forecast','predict','likely','target','buy signal','sell signal')
  sentences=[str(v) for v in body.values() if isinstance(v,str)]
  for row in body['rows']:
   sentences+= [str(row['flow']['what_label']),str(row['flow']['meaning'] or '')]
  for text in sentences:
   for word in banned:assert word not in text.lower(),(word,text)
  # and the word "mark" never reaches the reader
  for text in sentences:assert 'mark' not in text.lower(),text
 finally:app.state.db.close()


def test_the_read_module_answers_when_it_is_importable(tmp_path,grid_store):
 """`market_data.derivatives.read_api` wins when it is on the path, and the response says which source answered."""
 class Module:
  @staticmethod
  def strike_oi_series(_connection,underlying,**_kwargs):
   return {'underlying':underlying,'expiry':EXPIRY,'session':GRID_DAY,'as_of':GRID_MARKS[-1],
    'spot':GRID_SPOT,'atm_strike':GRID_ATM,'atm_basis':{'mark':GRID_MARKS[-1],'spot':GRID_SPOT,
    'rule':'listed strike nearest spot'},'marks':GRID_MARKS[:2],
    'contracts':[{'tradingsymbol':'NIFTY25SEP25100CE','instrument_token':5101,'strike':GRID_ATM,
     'option_type':'CE','atm_offset':0,'previous_close_oi':1_000_000,'direction':'building',
     'direction_detail':{'marks_back':1},
     'points':[{'at':GRID_MARKS[0],'oi':1_000_000,'delta_oi':0,'price':100.0},
      {'at':GRID_MARKS[1],'oi':1_200_000,'delta_oi':200_000,'price':130.0}]}]}
 reader=D.Derivatives(grid_store,read_module=Module)
 body=reader.oi_grid('NIFTY')
 assert body['source']=='metrics_module'
 assert body['total']==1 and len(body['rows'])==10
 assert body['rows'][0]['direction']=='building' and body['rows'][0]['marks_with_delta']==2
 assert body['rows'][1]['present'] is False
 # the reader sent no `flow`, so it was recomputed from the very points it DID send - never left blank
 assert body['rows'][0]['marks_with_price']==2 and body['rows'][0]['latest_price']==130.0
 assert body['rows'][0]['flow']['what_label']=='Call buying increasing'
 reader.close()


def test_a_read_module_that_fails_falls_back_to_the_store(tmp_path,grid_store):
 class Module:
  @staticmethod
  def strike_oi_series(*_args,**_kwargs):raise RuntimeError('not ready')
 reader=D.Derivatives(grid_store,read_module=Module)
 body=reader.oi_grid('NIFTY')
 assert body['source']=='store' and body['total']==9
 assert body['points_source']=='snapshots','the store fallback reads the capture worker own marks'
 assert body['rows'][0]['tradingsymbol']=='NIFTY25SEP25100CE'
 # the store fallback reaches exactly the same verdicts as the reader it stands in for
 found={r['slot']:r['direction'] for r in body['rows']}
 assert found['CE+0']=='building' and found['PE+0']=='unwinding' and found['CE+1']=='flat'
 assert found['CE+2']=='no baseline' and found['PE-1']=='no baseline'
 assert [p['delta_oi'] for p in body['rows'][0]['points']]==GRID_DELTAS[5101]
 reader.close()


# --- the futures price chart (GET /api/derivatives/futures-chart) --------------------------------------------
# What is protected here: the chart is the FRONT contract's OWN history at one cadence; it states how many
# sessions it is really returning and whose they are; a gap stays a gap; an unknown open interest stays null;
# and no sentence it prints says what happens next (§5).

CHART_SCHEMA=SCHEMA+('CREATE TABLE candles_day(instrument_token INTEGER, session_date TEXT, open REAL,'
 ' high REAL, low REAL, close REAL, volume INTEGER, oi INTEGER);')

def _chart_dates():
 """Expiries and sessions built off the SAME clock the reader uses, so the front contract and the
 days-to-expiry never depend on the calendar day the suite happens to run on."""
 from datetime import timedelta
 today=D.today_ist()
 days=[]
 day=today-timedelta(days=1)
 while len(days)<13:
  if day.weekday()<5:days.append(day)
  day-=timedelta(days=1)
 days.reverse()
 return today,today+timedelta(days=7),today+timedelta(days=45),days

TODAY_IST,FRONT_EXPIRY,NEXT_EXPIRY,CHART_DAYS=_chart_dates()
#: The session and the 15-min reading left out of the FRONT contract on purpose. Both are held by another
#: contract in the same store, so the store knows the exchange had them - which is what makes them gaps rather
#: than days the exchange was shut.
CHART_GAP=CHART_DAYS[6]
CHART_SESSIONS=[d for d in CHART_DAYS if d!=CHART_GAP]
CHART_BARS=('09:15:00','09:30:00','09:45:00')
CHART_GAP_BAR=CHART_SESSIONS[-2].isoformat()+' 09:30:00'
FRONT_TOKEN=401
NEXT_TOKEN=402
RELIANCE_FUT_TOKEN=403


def build_chart_store(path):
 """A NIFTY front future with 15-minute AND daily candles, a later NIFTY future, and a RELIANCE future with
 15-minute candles only."""
 connection=sqlite3.connect(path)
 connection.executescript(CHART_SCHEMA)
 connection.executemany('insert into contracts(instrument_token,tradingsymbol,underlying,instrument_type,'
  'strike,expiry,lot_size,first_seen,last_seen) values(?,?,?,?,?,?,?,?,?)',[
   (FRONT_TOKEN,'NIFTYFRONTFUT','NIFTY','FUT',0.0,FRONT_EXPIRY.isoformat(),75,EARLIER,MARK),
   (NEXT_TOKEN,'NIFTYNEXTFUT','NIFTY','FUT',0.0,NEXT_EXPIRY.isoformat(),75,EARLIER,MARK),
   (RELIANCE_FUT_TOKEN,'RELIANCEFRONTFUT','RELIANCE','FUT',0.0,FRONT_EXPIRY.isoformat(),500,EARLIER,MARK),
   (501,'NIFTYFRONT25000CE','NIFTY','CE',25000.0,FRONT_EXPIRY.isoformat(),75,EARLIER,MARK)])
 daily=[]
 for i,day in enumerate(CHART_SESSIONS):
  base=25000.0+i*10
  # the newest session carries NO open interest: unknown must stay null, never a zero
  oi=None if i==len(CHART_SESSIONS)-1 else 12_000_000+i*50_000
  daily.append((FRONT_TOKEN,day.isoformat(),base,base+40,base-30,base+15,3_000_000+i,oi))
 # the next contract trades EVERY session, so the store can testify that CHART_GAP was a trading day
 for i,day in enumerate(CHART_DAYS):
  daily.append((NEXT_TOKEN,day.isoformat(),24900.0,24950.0,24880.0,24910.0,120_000,400_000+i))
 connection.executemany('insert into candles_day(instrument_token,session_date,open,high,low,close,volume,oi)'
  ' values(?,?,?,?,?,?,?,?)',daily)
 bars=[]
 for token in (FRONT_TOKEN,RELIANCE_FUT_TOKEN):
  for day in CHART_SESSIONS[-3:]:
   for hhmm in CHART_BARS:
    at=day.isoformat()+' '+hhmm
    # the front contract is missing ONE reading the RELIANCE future has: a real gap, not a closed exchange
    if token==FRONT_TOKEN and at==CHART_GAP_BAR:continue
    bars.append((token,at,100.0,101.0,99.0,100.5,1000,2000))
 connection.executemany('insert into candles_15m(instrument_token,bar_start,open,high,low,close,volume,oi)'
  ' values(?,?,?,?,?,?,?,?)',bars)
 connection.commit();connection.close()


@pytest.fixture
def chart_store(tmp_path):
 path=tmp_path/'chart.db';build_chart_store(path);return str(path)

def offered_note(body,interval):
 return [i for i in body['intervals'] if i['interval']==interval][0]['note']


def test_the_chart_defaults_to_fifteen_minutes_and_names_its_contract(tmp_path,chart_store):
 app,client=client_for(tmp_path,derivatives_database=chart_store)
 try:
  body=client.get('/api/derivatives/futures-chart?underlying=NIFTY').json()
  assert body['available'] and body['interval']=='15m','15 minutes is the default'
  assert body['contract']['tradingsymbol']=='NIFTYFRONTFUT'
  assert body['contract']['instrument_token']==FRONT_TOKEN
  assert body['contract']['expiry']==FRONT_EXPIRY.isoformat()
  assert body['contract']['days_to_expiry']==7
  assert len(body['candles'])==9,'nine readings on the exchange grid'
  assert body['bars']==8 and body['gaps']==1,'eight stored bars and one empty slot'
  assert body['sessions']==3,'`sessions` counts trading DAYS, not readings'
  assert body['session']==CHART_SESSIONS[-1].isoformat()
  assert body['as_of']==body['candles'][-1]['at']
  ats=[c['at'] for c in body['candles']]
  assert ats==sorted(ats),'oldest first'
  assert set(body['candles'][0])=={'at','open','high','low','close','volume','oi','gap'}
  for key in ('available','captured','as_of','floors','floors_text','source','missing','empty_text',
    'empty_reason','baseline_sessions_required'):
   assert key in body,key
 finally:app.state.db.close()


def test_the_daily_interval_serves_the_same_contracts_own_sessions(tmp_path,chart_store):
 app,client=client_for(tmp_path,derivatives_database=chart_store)
 try:
  body=client.get('/api/derivatives/futures-chart?underlying=NIFTY&interval=1d').json()
  assert body['interval']=='1d'
  assert body['contract']['tradingsymbol']=='NIFTYFRONTFUT','daily is the SAME contract, not the index'
  assert body['sessions']==len(CHART_SESSIONS)==12 and body['bars']==12
  drawn=[c for c in body['candles'] if not c['gap']]
  assert [c['at'] for c in drawn]==[d.isoformat() for d in CHART_SESSIONS]
  assert body['session']==CHART_SESSIONS[-1].isoformat()
  assert drawn[-1]['oi'] is None,'unknown open interest is null, never 0'
  assert drawn[0]['oi']==12_000_000
  # a bar that traded is never turned into a gap, whatever its numbers say
  assert drawn[-1]['gap'] is False and drawn[-1]['close'] is not None
 finally:app.state.db.close()


def test_a_missing_session_keeps_its_slot_so_the_chart_cannot_close_it_up(tmp_path,chart_store):
 """The binding rule: an absent bar arrives WITH its timestamp and null prices, never omitted.

 Omitting it would make its neighbours adjacent on a chart drawn by index, and the picture would then claim
 continuous trading across a period that had none.
 """
 app,client=client_for(tmp_path,derivatives_database=chart_store)
 try:
  body=client.get('/api/derivatives/futures-chart?underlying=NIFTY&interval=1d').json()
  ats=[c['at'] for c in body['candles']]
  assert len(ats)==len(set(ats))==13,'twelve sessions this contract has, plus the one it does not'
  assert ats==sorted(ats)
  hole=[c for c in body['candles'] if c['at']==CHART_GAP.isoformat()]
  assert len(hole)==1,'the missing session is PRESENT as a slot'
  assert hole[0]=={'at':CHART_GAP.isoformat(),'open':None,'high':None,'low':None,'close':None,
   'volume':None,'oi':None,'gap':True}
  # its neighbours are still its neighbours - the hole is not closed up
  assert ats[ats.index(CHART_GAP.isoformat())-1]==CHART_DAYS[5].isoformat()
  assert ats[ats.index(CHART_GAP.isoformat())+1]==CHART_DAYS[7].isoformat()
  assert body['gaps']==1 and body['bars']==12
  assert body['sessions']==12,'a day made only of an empty slot is not a session this contract has'
 finally:app.state.db.close()


def test_a_missing_15_minute_reading_keeps_its_slot_too(tmp_path,chart_store):
 app,client=client_for(tmp_path,derivatives_database=chart_store)
 try:
  body=client.get('/api/derivatives/futures-chart?underlying=NIFTY&interval=15m').json()
  ats=[c['at'] for c in body['candles']]
  assert CHART_GAP_BAR in ats,'the reading the exchange had keeps its place'
  hole=[c for c in body['candles'] if c['at']==CHART_GAP_BAR][0]
  assert hole['gap'] is True and hole['close'] is None and hole['volume'] is None
  assert body['bars']==8 and body['gaps']==1
  # the reading either side of it is intact
  assert body['candles'][ats.index(CHART_GAP_BAR)-1]['close']==100.5
  assert body['candles'][ats.index(CHART_GAP_BAR)+1]['close']==100.5
 finally:app.state.db.close()


def test_a_day_the_exchange_was_shut_is_not_a_gap(tmp_path,chart_store):
 """Weekends and holidays are absent from the series and are NOT padded with empty slots.

 The grid is the store's own: a session date exists there only because some contract traded that day.
 """
 from datetime import timedelta
 app,client=client_for(tmp_path,derivatives_database=chart_store)
 try:
  body=client.get('/api/derivatives/futures-chart?underlying=NIFTY&interval=1d').json()
  ats={c['at'] for c in body['candles']}
  span=(CHART_DAYS[0],CHART_DAYS[-1])
  day=span[0]
  shut=[]
  while day<=span[1]:
   if day.weekday()>=5:shut.append(day.isoformat())
   day+=timedelta(days=1)
  assert shut,'the span covers at least one weekend'
  assert not (ats & set(shut)),'a closed exchange is no session, not an empty slot'
 finally:app.state.db.close()


def test_a_bad_interval_is_a_400(tmp_path,chart_store):
 app,client=client_for(tmp_path,derivatives_database=chart_store)
 try:
  for bad in ('1h','5m','day','15minute','nonsense','1d;drop','0'):
   assert client.get('/api/derivatives/futures-chart?underlying=NIFTY&interval='+bad).status_code==400,bad
 finally:app.state.db.close()


def test_a_missing_or_unknown_underlying_is_a_400(tmp_path,chart_store):
 app,client=client_for(tmp_path,derivatives_database=chart_store)
 try:
  assert client.get('/api/derivatives/futures-chart').status_code==400
  assert client.get('/api/derivatives/futures-chart?underlying=NIFTY%27;drop').status_code==400
  # a clean symbol this store lists no futures contract for: there is no contract to name and nothing to draw
  assert client.get('/api/derivatives/futures-chart?underlying=NOSUCH').status_code==400
  assert client.get('/api/derivatives/futures-chart?underlying=BANKNIFTY').status_code==400
 finally:app.state.db.close()


def test_a_contract_with_no_daily_history_offers_no_daily_interval(tmp_path,chart_store):
 """RELIANCE's future has 15-minute bars and no daily bars: the control is disabled, not offered dead."""
 app,client=client_for(tmp_path,derivatives_database=chart_store)
 try:
  body=client.get('/api/derivatives/futures-chart?underlying=RELIANCE&interval=1d').json()
  assert body['contract']['tradingsymbol']=='RELIANCEFRONTFUT'
  assert body['candles']==[] and body['sessions']==0 and body['session'] is None
  assert body['bars']==0 and body['gaps']==0
  # the empty card names the REAL reason, not the tab's 15-min capture sentence
  assert body['as_of'] is None
  assert body['empty_reason']==offered_note(body,'1d')
  assert body['empty_reason']!=D.EMPTY_TEXT
  offered={i['interval']:i for i in body['intervals']}
  assert offered['1d']['available'] is False and offered['1d']['candles']==0
  assert offered['15m']['available'] is True and offered['15m']['sessions']==3
  assert 'No daily candles are stored' in offered['1d']['note']
  assert body['short_history'] is False,'no series at all is not a short series'
 finally:app.state.db.close()


def test_a_store_written_before_daily_history_existed_still_answers(tmp_path,store):
 """The §2 store with no `candles_day` table at all: 200, and the daily control simply unavailable."""
 app,client=client_for(tmp_path,derivatives_database=store)
 try:
  body=client.get('/api/derivatives/futures-chart?underlying=NIFTY&interval=1d').json()
  assert body['available'] and body['candles']==[]
  # the contract is still named - only the bars are missing - and neither control is offered as live
  assert body['contract']['tradingsymbol']=='NIFTY25SEPFUT'
  assert {i['interval']:i['available'] for i in body['intervals']}=={'15m':False,'1d':False}
  assert body['empty_reason']==offered_note(body,'1d') and body['short_history'] is False
 finally:app.state.db.close()


def test_the_short_history_flag_fires_on_a_real_short_series(tmp_path):
 """The roll consequence, made visible: a front contract with three sessions is flagged and explained."""
 path=tmp_path/'short.db'
 connection=sqlite3.connect(path)
 connection.executescript(CHART_SCHEMA)
 connection.execute('insert into contracts(instrument_token,tradingsymbol,underlying,instrument_type,strike,'
  'expiry,lot_size,first_seen,last_seen) values(?,?,?,?,?,?,?,?,?)',
  (NEXT_TOKEN,'NIFTYNEXTFUT','NIFTY','FUT',0.0,NEXT_EXPIRY.isoformat(),75,EARLIER,MARK))
 connection.executemany('insert into candles_day(instrument_token,session_date,open,high,low,close,volume,oi)'
  ' values(?,?,?,?,?,?,?,?)',
  [(NEXT_TOKEN,d.isoformat(),100.0,101.0,99.0,100.5,1000,5000) for d in CHART_DAYS[-3:]])
 connection.commit();connection.close()
 reader=D.Derivatives(str(path))
 try:
  body=reader.futures_chart('NIFTY','1d')
  assert body['sessions']==3 and body['short_history'] is True
  assert body['sessions']<body['short_history_sessions']
  assert 'NIFTYNEXTFUT' in body['short_history_text']
  assert '3 trading sessions' in body['short_history_text']
  assert body['contract']['days_to_expiry']==45
  assert {i['interval']:i['available'] for i in body['intervals']}=={'15m':False,'1d':True}
 finally:reader.close()


def test_a_long_enough_series_is_not_flagged_short(tmp_path,chart_store):
 reader=D.Derivatives(chart_store)
 try:
  body=reader.futures_chart('NIFTY','1d')
  assert body['sessions']==12 and body['sessions']>=body['short_history_sessions']
  assert body['short_history'] is False and body['short_history_text'] is None
 finally:reader.close()


def test_every_interval_carries_its_own_note_and_the_served_one_is_repeated(tmp_path,chart_store):
 app,client=client_for(tmp_path,derivatives_database=chart_store)
 try:
  body=client.get('/api/derivatives/futures-chart?underlying=NIFTY&interval=1d').json()
  assert [i['interval'] for i in body['intervals']]==list(D.CHART_INTERVALS)
  for offered in body['intervals']:
   assert offered['note'] and offered['label']
   assert offered['selected'] is (offered['interval']=='1d')
  assert body['note']==[i for i in body['intervals'] if i['interval']=='1d'][0]['note']
  assert 'NIFTYFRONTFUT' in body['note'] and 'not the index' in body['note']
  assert body['chart_text'].startswith('This is the front futures')
 finally:app.state.db.close()


def test_the_chart_never_says_what_happens_next(tmp_path,chart_store):
 """§5 over every sentence this route can produce."""
 banned=re.compile(r'\b(will|expect|forecast|predict|likely|should rise|should fall|target|'
  r'support level|resistance level|bullish|bearish|buy signal|sell signal)\b',re.I)
 reader=D.Derivatives(chart_store)
 try:
  sentences=[D.CHART_TEXT,D.GAPS_TEXT,D.GRID_SOURCE_TEXT]
  for interval in D.CHART_INTERVALS:
   for available in (True,False):
    sentences.append(reader.interval_note(interval,'NIFTYFRONTFUT',12,300,available))
   sentences.append(reader.short_history_text('NIFTYFRONTFUT',interval,2,FRONT_EXPIRY.isoformat(),7))
  for body in (reader.futures_chart('NIFTY','15m'),reader.futures_chart('NIFTY','1d')):
   sentences+=[body['note'],body['chart_text'],body['gaps_text'],body['grid_source_text'],
    body['short_history_text'] or '']
   sentences+=[i['note'] for i in body['intervals']]
  for text in sentences:
   assert text is not None
   assert not banned.search(str(text)),'a chart sentence must not predict: '+str(text)
 finally:reader.close()


def test_the_chart_is_empty_not_broken_before_any_capture(tmp_path):
 app,client=client_for(tmp_path)
 try:
  result=client.get('/api/derivatives/futures-chart?underlying=NIFTY')
  assert result.status_code==200,'no store at all is an empty card, never a 400'
  body=result.json()
  assert body['available'] is False and body['captured'] is False
  assert body['empty_reason']==D.EMPTY_TEXT and body['candles']==[] and body['contract'] is None
  assert body['sessions']==0 and body['short_history'] is False
  assert {i['interval']:i['available'] for i in body['intervals']}=={'15m':False,'1d':False}
 finally:app.state.db.close()


def test_the_read_module_answers_the_chart_when_it_is_importable(tmp_path,chart_store):
 """`market_data.derivatives.read_api.futures_chart_series` wins, and `source` says which answered."""
 class Module:
  @staticmethod
  def futures_chart_series(_connection,underlying,interval='15m',**_kwargs):
   return {'underlying':underlying,'interval':interval,
    'contract':{'tradingsymbol':'NIFTYFRONTFUT','instrument_token':FRONT_TOKEN,
     'expiry':FRONT_EXPIRY.isoformat()},
    'candles':[{'at':CHART_DAYS[-2].isoformat(),'open':None,'high':None,'low':None,'close':None,
      'volume':None,'oi':None},
     {'at':CHART_DAYS[-1].isoformat(),'open':1.0,'high':2.0,'low':0.5,'close':1.5,
      'volume':10,'oi':None}],
    'sessions':1,'session':CHART_DAYS[-1].isoformat(),'as_of':CHART_DAYS[-1].isoformat(),
    'intervals':{'15m':{'candles':9,'sessions':3,'available':True},
     '1d':{'candles':1,'sessions':1,'available':True}}}
 reader=D.Derivatives(chart_store,read_module=Module)
 try:
  body=reader.futures_chart('NIFTY','1d')
  assert body['source']=='metrics_module'
  assert body['sessions']==1 and body['short_history'] is True
  assert body['bars']==1 and body['gaps']==1
  # a slot the delegate sent with no close is a gap, and is flagged as one HERE rather than taken on trust
  assert body['candles'][0]['gap'] is True and body['candles'][1]['gap'] is False
  assert body['candles'][1]['oi'] is None,'a delegate may not turn an unknown into a zero'
  assert body['as_of']==CHART_DAYS[-1].isoformat(),'as-of is the newest real bar, never a trailing hole'
  assert body['contract']['days_to_expiry']==7,'days-to-expiry is computed here, not taken on trust'
 finally:reader.close()


def test_a_read_module_that_fails_or_drifts_falls_back_to_the_store(tmp_path,chart_store):
 class Broken:
  @staticmethod
  def futures_chart_series(*_args,**_kwargs):raise RuntimeError('not ready')
 class Drifted:
  @staticmethod
  def futures_chart_series(*_args,**_kwargs):return {'candles':[],'intervals':{'15m':{}}}
 for module in (Broken,Drifted):
  reader=D.Derivatives(chart_store,read_module=module)
  try:
   body=reader.futures_chart('NIFTY','1d')
   assert body['source']=='store',module.__name__
   assert body['sessions']==12 and body['contract']['tradingsymbol']=='NIFTYFRONTFUT'
  finally:reader.close()


def test_the_two_paths_agree_candle_for_candle(tmp_path,chart_store):
 """The store fallback and the real `read_api` must not drift: same contract, same bars, same nulls."""
 import sys
 root=str(Path(__file__).resolve().parents[3])
 if root not in sys.path:sys.path.insert(0,root)
 read_api=pytest.importorskip('market_data.derivatives.read_api')
 reader=D.Derivatives(chart_store,read_module=read_api)
 try:
  for interval in D.CHART_INTERVALS:
   delegated=reader.futures_chart('NIFTY',interval)
   assert delegated['source']=='metrics_module',interval
   plain=D.Derivatives(chart_store)
   try:direct=plain.futures_chart('NIFTY',interval)
   finally:plain.close()
   for key in ('candles','contract','sessions','session','bars','gaps','intervals','short_history',
     'short_history_text','note','as_of'):
    assert delegated[key]==direct[key],(interval,key)
 finally:reader.close()


def test_the_pilot_cannot_write_to_the_chart_store(tmp_path,chart_store):
 reader=D.Derivatives(chart_store)
 try:
  reader.futures_chart('NIFTY','1d')
  with pytest.raises(sqlite3.OperationalError):
   reader._connect().execute('delete from candles_day')
 finally:reader.close()
