"""P01 and P02: one coherent context, and truthful empty states for a capture outage.

Both of these are regressions of a REAL recorded case, not invented ones. On 18 September 2026 the F&O
capture died at 11:30 IST and the rest of the session was rebuilt from 15-minute candles. A candle carries no
traded-price average and no underlying price, so from 11:45 onward every contract row had a null premium and
a null spot. Two defects came out of that one outage, and each of them passed 371 tests:

P01 — THE SCREEN COMBINED READINGS. The max-pain headline printed a strike of 23,350 (the 15:45 reading), a
     spot of 23,302 (the 11:30 reading) and "2 below" (the distance of the 11:30 PAIR: 23,300 against
     23,302). All three numbers were captured. All three were correct. The sentence they made was false,
     because no reading of that session ever held 23,350 two points below spot.

     Two more in the same family: days-to-expiry was computed both from the captured date and from today, so
     one 22 Sep option read as 4 days on one panel and 2 on another; and the distance had two opposite
     conventions, `indices()` serving spot − strike while the session panel served strike − spot.

P02 — THE SCREEN CALLED AN OUTAGE A QUIET MARKET. The 15:45 reading held 10,552 contract rows, zero with a
     spot and zero with a premium, and the screen said "No contract cleared the liquidity floors". Nothing
     was measured. A trader reads that wording as "the market is quiet", which is the opposite of the truth.

What these tests hold, in one line each: a figure is served at the reading it was observed at; a relationship
between two figures exists only when both were observed together; a day count is counted from the session on
screen; and an empty list says what actually happened, in one of five separate states.
"""
import sqlite3
import pytest
from kanida_pilot import derivatives as D
from test_pilot import Evidence,pilot_settings,signup  # noqa: F401
from test_derivatives import client_for  # noqa: F401
from test_derivatives_series import SCHEMA

# --- THE RECORDED CASE, to the number ---------------------------------------------------------------------
SESSION='2026-09-18'
EXPIRY='2026-09-22'
#: The last reading the capture actually reached, and the newest reading the store holds. Everything between
#: them was rebuilt from candles.
LIVE_MARK=f'{SESSION} 11:30:00'
REBUILT_MARKS=[f'{SESSION} {t}' for t in ('11:45:00','12:00:00','15:45:00')]
NEWEST_MARK=REBUILT_MARKS[-1]
MARKS=[f'{SESSION} 09:30:00',f'{SESSION} 10:30:00',LIVE_MARK]+REBUILT_MARKS
#: The audit's own figures for NIFTY.
LIVE_STRIKE,LIVE_SPOT,LIVE_DISTANCE=23300.0,23302.0,-2.0
NEWEST_STRIKE=23350.0


def _store(path,*,rebuilt=True,spot_in_rebuilt=False,premium_in_rebuilt=False):
 """The 18 Sep 2026 store: three live readings, then three rebuilt ones with nothing measured in them."""
 connection=sqlite3.connect(path)
 connection.executescript(SCHEMA)
 connection.executemany('insert into contracts(instrument_token,tradingsymbol,underlying,instrument_type,'
  'strike,expiry,lot_size,first_seen,last_seen) values(?,?,?,?,?,?,?,?,?)',
  [(101,'NIFTY26SEP23300CE','NIFTY','CE',23300.0,EXPIRY,75,SESSION,SESSION),
   (102,'NIFTY26SEP23300PE','NIFTY','PE',23300.0,EXPIRY,75,SESSION,SESSION),
   (103,'NIFTY26SEP23400CE','NIFTY','CE',23400.0,EXPIRY,75,SESSION,SESSION),
   (104,'NIFTY26SEP23400PE','NIFTY','PE',23400.0,EXPIRY,75,SESSION,SESSION),
   (105,'NIFTY26SEP23500CE','NIFTY','CE',23500.0,EXPIRY,75,SESSION,SESSION),
   (106,'NIFTY26SEP23500PE','NIFTY','PE',23500.0,EXPIRY,75,SESSION,SESSION)])
 live=[m for m in MARKS if m<=LIVE_MARK]
 # --- the per-underlying chain rows: the max-pain strike, the spot beside it and their distance ----------
 chain=[]
 for index,mark in enumerate(live):
  chain.append(('underlying',f'NIFTY|{EXPIRY}',mark,'NIFTY',EXPIRY,4,LIVE_SPOT,1.05,1.02,
   5_000_000.0,5_250_000.0,LIVE_STRIKE,LIVE_STRIKE-LIVE_SPOT,10_250_000.0,'ok',192))
 if rebuilt:
  for mark in REBUILT_MARKS:
   # A REBUILT READING CARRIES A STRIKE AND NO SPOT. Max pain rests on open interest, which a candle DOES
   # carry; spot does not come from a candle at all. That asymmetry is the whole of the P01 defect.
   here=LIVE_SPOT if spot_in_rebuilt else None
   chain.append(('underlying',f'NIFTY|{EXPIRY}',mark,'NIFTY',EXPIRY,4,here,1.07,1.01,
    4_900_000.0,5_240_000.0,NEWEST_STRIKE,None,10_140_000.0,'ok',192))
 connection.executemany('insert into metrics(scope,metric_key,captured_at,underlying,expiry,days_to_expiry,'
  'spot,pcr_oi,pcr_volume,total_ce_oi,total_pe_oi,max_pain_strike,max_pain_distance,max_pain_total_oi,'
  'max_pain_status,contracts) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',chain)
 # --- the contract rows the screener and the floors read ------------------------------------------------
 rows=[]
 for token,symbol,kind,strike in ((101,'NIFTY26SEP23300CE','CE',23300.0),
   (102,'NIFTY26SEP23300PE','PE',23300.0),(103,'NIFTY26SEP23400CE','CE',23400.0),
   (104,'NIFTY26SEP23400PE','PE',23400.0),(105,'NIFTY26SEP23500CE','CE',23500.0),
   (106,'NIFTY26SEP23500PE','PE',23500.0)):
  for mark in live:
   rows.append(('contract',symbol,mark,token,symbol,'NIFTY',kind,strike,EXPIRY,75,4,180.0,178.0,
    500_000.0,900_000.0,LIVE_SPOT,14.5))
  if rebuilt:
   for mark in REBUILT_MARKS:
    rows.append(('contract',symbol,mark,token,symbol,'NIFTY',kind,strike,EXPIRY,75,4,176.0,None,
     500_000.0,900_000.0,(LIVE_SPOT if spot_in_rebuilt else None),
     (14.5 if premium_in_rebuilt else None)))
 connection.executemany('insert into metrics(scope,metric_key,captured_at,instrument_token,tradingsymbol,'
  'underlying,instrument_type,strike,expiry,lot_size,days_to_expiry,last_price,average_price,volume,oi,spot,'
  'premium_cr) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',rows)
 # Every reading REACHED the underlying — the capture was there, it just could not measure anything.
 connection.executemany('insert into underlying_snapshots(underlying,captured_at,spot,max_pain_strike)'
  ' values(?,?,?,?)',
  [('NIFTY',mark,LIVE_SPOT,LIVE_STRIKE) for mark in live]
  +([('NIFTY',mark,(LIVE_SPOT if spot_in_rebuilt else None),NEWEST_STRIKE) for mark in REBUILT_MARKS]
    if rebuilt else []))
 connection.commit();connection.close()


@pytest.fixture
def outage(tmp_path):
 """The recorded case: live to 11:30, rebuilt to 15:45, nothing measured after the outage."""
 path=tmp_path/'outage.db';_store(path);return str(path)

@pytest.fixture
def healthy(tmp_path):
 """The same store with no outage at all: every reading complete."""
 path=tmp_path/'healthy.db';_store(path,rebuilt=False);return str(path)

@pytest.fixture
def reader(outage):
 value=D.Derivatives(outage);yield value;value.close()


# ============================================================================================================
# P01 — ONE COHERENT INSTRUMENT, EXPIRY AND TIME CONTEXT
# ============================================================================================================

def test_the_recorded_max_pain_case_never_borrows_a_spot_from_another_reading(reader):
 """THE AUDIT'S OWN EXAMPLE. 23,350 from 15:45, 23,302 from 11:30, and NO distance between them."""
 body=reader.maxpain_series('NIFTY')
 # each figure is served at the reading it was observed at, and says which one that was
 assert body['latest_max_pain_strike']==NEWEST_STRIKE
 assert body['latest_max_pain_strike_at']==NEWEST_MARK
 assert body['latest_spot']==LIVE_SPOT
 assert body['latest_spot_at']==LIVE_MARK
 # THE DEFECT: the headline used to carry the 11:30 pair's distance beside the 15:45 strike.
 assert body['latest_distance'] is None,'a distance is never carried across two readings'
 assert body['latest_distance_at'] is None
 assert body['latest_distance_withheld']=='no_spot_at_reading'
 assert 'carries no spot' in body['latest_distance_reason']
 # and the number it used to borrow is still in the series, at its own reading, where it is true
 at_live=next(p for p in body['points'] if p['at']==LIVE_MARK)
 assert at_live['distance']==pytest.approx(LIVE_DISTANCE)


def test_the_latest_complete_reading_is_offered_beside_the_headline_never_inside_it(reader):
 """A coherent pair IS available — at 11:30 — so it is named, with its own time, rather than blended in."""
 body=reader.maxpain_series('NIFTY')
 pair=body['latest_complete_pair']
 assert pair['at']==LIVE_MARK
 assert pair['max_pain_strike']==LIVE_STRIKE and pair['spot']==LIVE_SPOT
 assert pair['distance']==pytest.approx(LIVE_DISTANCE)
 # the pair's strike is the OLDER one: offering it must never rewrite the headline figure
 assert body['latest_max_pain_strike']==NEWEST_STRIKE!=pair['max_pain_strike']
 assert 'never mixed into' in body['latest_complete_pair_text']


def test_a_session_with_no_spot_at_all_says_so_rather_than_naming_a_reading(tmp_path):
 """No spot anywhere is a different reason from "no spot at THIS reading", and reads differently."""
 path=tmp_path/'no-spot.db'
 connection=sqlite3.connect(path);connection.executescript(SCHEMA)
 connection.executemany('insert into contracts(instrument_token,tradingsymbol,underlying,instrument_type,'
  'strike,expiry,lot_size,first_seen,last_seen) values(?,?,?,?,?,?,?,?,?)',
  [(101,'NIFTY26SEP23300CE','NIFTY','CE',23300.0,EXPIRY,75,SESSION,SESSION)])
 connection.executemany('insert into metrics(scope,metric_key,captured_at,underlying,expiry,days_to_expiry,'
  'spot,total_ce_oi,total_pe_oi,max_pain_strike,max_pain_status,contracts)'
  ' values(?,?,?,?,?,?,?,?,?,?,?,?)',
  [('underlying',f'NIFTY|{EXPIRY}',mark,'NIFTY',EXPIRY,4,None,5e6,5e6,NEWEST_STRIKE,'ok',192)
   for mark in MARKS])
 connection.commit();connection.close()
 value=D.Derivatives(str(path))
 try:
  body=value.maxpain_series('NIFTY')
  assert body['latest_spot'] is None and body['latest_distance'] is None
  assert body['latest_distance_withheld']=='no_spot'
  assert body['latest_complete_pair'] is None
 finally:value.close()


def test_a_complete_session_still_serves_a_distance_at_the_headline_reading(healthy):
 """The fix must not withhold a distance that IS coherent: same reading, both figures, a real number."""
 value=D.Derivatives(healthy)
 try:
  body=value.maxpain_series('NIFTY')
  assert body['latest_max_pain_strike_at']==body['latest_spot_at']==body['latest_distance_at']==LIVE_MARK
  assert body['latest_distance']==pytest.approx(LIVE_DISTANCE)
  assert body['latest_distance_withheld'] is None and body['latest_distance_reason'] is None
 finally:value.close()


@pytest.mark.parametrize('strike,spot,expected',[
 (23350.0,23302.0,48.0),   # the strike ABOVE spot is POSITIVE
 (23300.0,23302.0,-2.0),   # the strike BELOW spot is NEGATIVE — the recorded case's own sign
 (23300.0,23300.0,0.0),    # at spot
])
def test_distance_is_strike_minus_spot_on_every_route_and_both_signs_are_tested(tmp_path,strike,spot,expected):
 """ONE CONVENTION. `indices()` used to serve spot − strike: the opposite sign of the other two routes."""
 path=tmp_path/f'sign-{strike}-{spot}.db'
 connection=sqlite3.connect(path);connection.executescript(SCHEMA)
 connection.executemany('insert into contracts(instrument_token,tradingsymbol,underlying,instrument_type,'
  'strike,expiry,lot_size,first_seen,last_seen) values(?,?,?,?,?,?,?,?,?)',
  [(101,'NIFTY26SEP23300CE','NIFTY','CE',23300.0,EXPIRY,75,SESSION,SESSION),
   (102,'NIFTY26SEP23300PE','NIFTY','PE',23300.0,EXPIRY,75,SESSION,SESSION)])
 connection.execute('insert into metrics(scope,metric_key,captured_at,underlying,expiry,days_to_expiry,spot,'
  'total_ce_oi,total_pe_oi,max_pain_strike,max_pain_total_oi,max_pain_status,contracts)'
  ' values(?,?,?,?,?,?,?,?,?,?,?,?,?)',
  ('underlying',f'NIFTY|{EXPIRY}',LIVE_MARK,'NIFTY',EXPIRY,4,spot,5e6,5e6,strike,1e7,'ok',192))
 connection.executemany('insert into metrics(scope,metric_key,captured_at,instrument_token,tradingsymbol,'
  'underlying,instrument_type,strike,expiry,lot_size,days_to_expiry,last_price,average_price,volume,oi,spot,'
  'premium_cr) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
  [('contract',s,LIVE_MARK,t,s,'NIFTY',k,23300.0,EXPIRY,75,4,180.0,178.0,5e5,9e5,spot,14.5)
   for t,s,k in ((101,'NIFTY26SEP23300CE','CE'),(102,'NIFTY26SEP23300PE','PE'))])
 connection.execute('insert into underlying_snapshots(underlying,captured_at,spot,max_pain_strike)'
  ' values(?,?,?,?)',('NIFTY',LIVE_MARK,spot,strike))
 connection.commit();connection.close()
 value=D.Derivatives(str(path))
 try:
  series=value.maxpain_series('NIFTY')
  grid=value.oi_by_strike('NIFTY')
  index=next(r for r in value.indices(names=['NIFTY'])['rows'] if r['underlying']=='NIFTY')
  assert series['latest_distance']==pytest.approx(expected)
  assert grid['max_pain_distance']==pytest.approx(expected)
  assert index['max_pain_distance']==pytest.approx(expected),'indices used to serve the opposite sign'
  # and the convention is stated on every one of them, so a reader never has to infer the sign
  for body in (series,grid):
   assert 'MINUS spot' in (body.get('distance_definition') or body.get('max_pain_distance_definition'))
 finally:value.close()


def test_the_index_list_withholds_a_distance_it_cannot_measure(tmp_path):
 """A missing spot on the index list is an unavailable distance with a reason — never a zero."""
 path=tmp_path/'index-no-spot.db'
 connection=sqlite3.connect(path);connection.executescript(SCHEMA)
 connection.execute('insert into contracts(instrument_token,tradingsymbol,underlying,instrument_type,strike,'
  'expiry,lot_size,first_seen,last_seen) values(?,?,?,?,?,?,?,?,?)',
  (101,'NIFTY26SEP23300CE','NIFTY','CE',23300.0,EXPIRY,75,SESSION,SESSION))
 connection.execute('insert into underlying_snapshots(underlying,captured_at,spot,max_pain_strike)'
  ' values(?,?,?,?)',('NIFTY',NEWEST_MARK,None,NEWEST_STRIKE))
 connection.commit();connection.close()
 value=D.Derivatives(str(path))
 try:
  row=next(r for r in value.indices(names=['NIFTY'])['rows'] if r['underlying']=='NIFTY')
  assert row['max_pain_strike']==NEWEST_STRIKE
  assert row['max_pain_distance'] is None,'never a zero for an unmeasurable distance'
  assert row['max_pain_distance_withheld']=='no_spot_at_reading'
  assert row['max_pain_distance_reason']
 finally:value.close()


# --- days to expiry: ONE convention, counted from the session ---------------------------------------------

def test_days_to_expiry_is_counted_from_the_session_and_never_from_today(reader):
 """THE AUDIT'S SECOND CASE: the same 22 Sep option read as 4 days and as 2 on one page."""
 for body in (reader.maxpain_series('NIFTY'),reader.pcr_series('NIFTY'),reader.iv_series('NIFTY')):
  assert body['session']==SESSION
  assert body['days_to_expiry']==4,'22 Sep is four days after the 18 Sep session, whatever today is'
  assert 'never counted from today' in body['days_to_expiry_basis']
 # the chain and the OI-by-strike card agree, because they count from the same session
 assert reader.chain('NIFTY',EXPIRY)['days_to_expiry']==4
 assert reader.oi_by_strike('NIFTY',EXPIRY)['days_to_expiry']==4


@pytest.mark.parametrize('session,expiry,expected',[
 ('2026-09-18','2026-09-22',4),     # the recorded case
 ('2026-09-22','2026-09-22',0),     # EXPIRY DAY is zero, not one and not "today"
 ('2026-09-21','2026-09-22',1),     # the day before
 ('2026-09-23','2026-09-22',-1),    # a session AFTER expiry is negative, never clamped to zero
 ('2026-12-31','2027-01-29',29),    # across a year boundary
 ('2028-02-28','2028-03-01',2),     # across a leap day
])
def test_the_day_convention_holds_across_dates_and_boundaries(session,expiry,expected):
 assert D.session_days_to_expiry(expiry,session)==expected


def test_an_unreadable_date_is_no_day_count_at_all_rather_than_a_guess():
 for bad in ('','not-a-date','2026-13-01',None):
  assert D.session_days_to_expiry(bad,SESSION) is None
 # an unreadable SESSION falls back to today rather than to a wrong date, and says nothing false
 assert D.session_days_to_expiry(EXPIRY,'nonsense')==D.days_to_expiry(EXPIRY)


def test_the_expiry_picker_is_the_one_place_counted_from_today_and_says_so(reader):
 """`filters()` lists expiries still to come. That question IS about today, and the basis is stated."""
 body=reader.filters()
 assert 'counted from today' in body['expiry_days_basis']
 assert 'SESSION on screen' in body['expiry_days_basis']


# --- the context object itself -----------------------------------------------------------------------------

ROUTES=('maxpain_series','pcr_series','iv_series','chain','oi_by_strike','oi_grid','futures_buildup')

def test_every_dependent_route_carries_the_one_context(reader):
 """Instrument, expiry, session, as-of, timezone and live-versus-historical, on every card."""
 for name in ROUTES:
  body=getattr(reader,name)('NIFTY')
  context=body.get('context')
  assert context is not None,f'{name} carries no context'
  for key in ('underlying','expiry','session','at','timezone','mode','days_to_expiry','is_newest','source'):
   assert key in context,f'{name}.context is missing {key}'
  assert context['underlying']=='NIFTY',name
  assert context['timezone']=='Asia/Kolkata',name
  assert context['mode'] in D.ANALYSIS_MODES,name
  assert 'never carried forward' in context['context_text']


def test_a_context_names_the_instrument_that_was_asked_for_and_no_other(reader):
 """Rapid symbol changes cannot mix panels: every response says which symbol it is answering for, so a
 response that arrives after the reader moved on is identifiable as the wrong one rather than blended in."""
 for symbol in ('NIFTY','RELIANCE','NIFTY'):
  body=reader.maxpain_series(symbol)
  assert body['context']['underlying']==symbol
  assert body['underlying']==symbol


def test_an_expiry_that_was_asked_for_travels_on_the_context(reader):
 body=reader.maxpain_series('NIFTY',EXPIRY)
 assert body['context']['expiry']==EXPIRY and body['expiry']==EXPIRY


def test_a_closed_session_is_historical_and_todays_session_is_live(tmp_path):
 """`mode` is a fact about the boundary, not a setting."""
 today=D.today_ist().isoformat()
 assert D.AnalysisContext(session=SESSION).mode=='historical'
 assert D.AnalysisContext(session=today).mode=='live'
 assert D.AnalysisContext().mode=='live','with no session resolved yet, the boundary is the live edge'
 # and the session is read off a reading stamp when only that is given
 assert D.AnalysisContext(at=f'{SESSION} 15:45:00').session==SESSION


def test_the_context_says_when_the_boundary_is_not_the_newest_reading(reader):
 """The actual observation time is visible wherever it differs from the boundary the reader chose."""
 body=reader.screener({'at':LIVE_MARK})
 assert body['context']['at']==LIVE_MARK
 assert body['context']['newest_at']==NEWEST_MARK
 assert body['context']['is_newest'] is False


# ============================================================================================================
# P02 — TRUTHFUL EMPTY STATES FOR A CAPTURE OUTAGE
# ============================================================================================================

def test_the_recorded_outage_is_a_partial_capture_and_never_a_quiet_market(reader):
 """THE AUDIT'S THIRD CASE, and the owner's complaint on top of it.

 Rows are there. The premium in them was never measured - so the premium floor is not applied, the rows are
 KEPT, and the reading still reports itself as a partial capture rather than as a reading of a quiet market.
 Both halves matter: hiding the rows was the bug, and calling the reading healthy would be the next one."""
 body=reader.screener({'at':NEWEST_MARK})
 # THE ROWS SURVIVE. This is the fix: 10,510 contracts with a real last price and a real open interest were
 # being deleted from the screen by a floor that rested on a number nobody had.
 assert body['rows'],'a reading with prices and open interest is not an empty reading'
 assert body['empty_note'] is None and body['empty_state'] is None
 # ...AND THE READING IS STILL NOT HEALTHY, and still says why in its own words.
 capture=body['capture']
 assert capture['state']=='partial_capture' and capture['healthy'] is False
 note=capture['state_text']
 assert 'not captured' in note
 assert 'not a quiet market' in note
 # the old wording is GONE: it described a market, and no market was described
 assert 'cleared the floors' not in note and 'clears the floors' not in note
 assert 'premium_cr' in capture['missing_fields']
 # the counts are MEASURED, not asserted: rows present, premium and spot absent
 assert capture['rows']>0
 assert capture['coverage']['premium_cr']['present']==0
 assert capture['coverage']['spot']['present']==0
 assert capture['coverage']['oi']['present']==capture['rows'],'open interest DID survive the outage'
 # AND THE DEGRADED FLOOR SET IS ON THE CARD, in the response the page renders from.
 assert body['floors_degraded'] is True
 assert body['floors_unmeasured']==['premium_cr']
 assert 'premium_cr' not in body['floors_applied']
 assert 'premium traded' not in body['floors_text']
 assert any('premium floor could not be applied' in line for line in body['floors_unmeasured_text'])
 assert capture['floors_unmeasured']==['premium_cr'],'the chip and the list read the same answer'
 # the sort says what it is: premium cannot order a reading that has none
 assert body['ranking']['ranked_by']=='volume'


def test_a_live_reading_of_the_same_store_is_untouched_by_the_degradation(reader):
 """THE GUARD. The reading before the outage measured all three floors, so all three are applied, the
 sentence is the three-floor one word for word, and the sort is still premium traded."""
 body=reader.screener({'at':LIVE_MARK})
 assert body['floors_degraded'] is False
 assert body['floors_applied']==['premium_cr','oi','last_price']
 assert body['floors_unmeasured']==[] and body['floors_unmeasured_text']==[]
 assert body['floors_text']==D.FLOORS_TEXT
 assert body['ranking']['ranked_by']=='premium_cr'
 assert body['capture']['state']=='complete' and body['capture']['healthy'] is True


def test_a_complete_reading_with_nothing_eligible_still_says_no_matches(tmp_path):
 """The opposite failure would be as bad: a real reading of a real market must still read as one."""
 path=tmp_path/'thin.db'
 # every required field captured; the premium is simply under the floor
 _store(path,rebuilt=False)
 connection=sqlite3.connect(path)
 connection.execute("update metrics set premium_cr=0.4 where scope='contract'")
 connection.commit();connection.close()
 value=D.Derivatives(str(path))
 try:
  body=value.screener({'at':LIVE_MARK})
  assert body['rows']==[]
  assert body['empty_state']=='no_eligible_rows'
  assert 'no contract cleared them' in (body['empty_note'] or '')
  assert 'not a gap in it' in (body['empty_note'] or '')
  assert body['capture']['healthy'] is True,'a real reading is a healthy reading, empty or not'
  assert body['capture']['missing_fields']==[]
 finally:value.close()


def test_filters_that_exclude_everything_are_their_own_state(healthy):
 """The reader's own choice is not the market's, and the two never share a sentence."""
 value=D.Derivatives(healthy)
 try:
  body=value.screener({'at':LIVE_MARK,'underlying':'RELIANCE'})
  assert body['rows']==[]
  assert body['empty_state']=='filtered_out'
  assert 'the filters in force excluded' in (body['empty_note'] or '')
  assert body['capture']['cleared']>0,'contracts DID clear the floors at this reading'
  assert body['capture']['matched']==0
  assert body['capture']['healthy'] is True
 finally:value.close()


def test_a_reading_with_no_rows_at_all_is_missing_capture(tmp_path):
 path=tmp_path/'gap.db';_store(path,rebuilt=False)
 value=D.Derivatives(str(path))
 try:
  health=value.capture_health(f'{SESSION} 14:00:00')
  assert health['state']=='missing_capture'
  assert health['rows']==0 and health['healthy'] is False
  assert 'No contract row was captured' in health['state_text']
 finally:value.close()


def test_a_store_that_cannot_be_read_is_a_failure_state_and_not_an_empty_market(tmp_path):
 """A request that failed is not a market that was quiet, and it never wears a healthy badge."""
 value=D.Derivatives(str(tmp_path/'does-not-exist.db'))
 try:
  health=value.capture_health(NEWEST_MARK)
  assert health['state']=='failed' and health['healthy'] is False
  assert 'could not be read' in health['state_text']
  assert 'not the same as no rows existing' in health['state_text']
 finally:value.close()


def test_the_capture_route_answers_and_never_500s_on_a_missing_store(tmp_path):
 app,client=client_for(tmp_path)
 try:
  result=client.get('/api/derivatives/capture')
  assert result.status_code==200,'a store that is not there is a STATE, not a server error'
  assert result.json()['state'] in D.CAPTURE_STATES
 finally:app.state.db.close()


def test_the_capture_route_reports_the_recorded_outage_over_http(tmp_path,outage):
 app,client=client_for(tmp_path,derivatives_database=outage)
 try:
  body=client.get(f'/api/derivatives/capture?at={NEWEST_MARK}').json()
  assert body['state']=='partial_capture' and body['healthy'] is False
  assert body['latest_complete_at']==LIVE_MARK
  assert body['latest_attempted_at']==NEWEST_MARK
  assert set(body['states'])==set(D.CAPTURE_STATES)
 finally:app.state.db.close()


def test_the_latest_complete_reading_is_offered_and_never_silently_substituted(reader):
 """The screener stays where the reader put it; the complete reading is NAMED beside it."""
 body=reader.screener({'at':NEWEST_MARK})
 assert body['as_of']==NEWEST_MARK,'the chosen reading is served exactly as asked'
 assert body['reading_chosen'] is True
 assert body['capture']['latest_complete_at']==LIVE_MARK
 assert body['capture']['latest_complete_is_here'] is False


def test_partial_capture_never_receives_a_healthy_badge_on_any_surface(reader):
 """One rule, checked on every surface that carries capture health."""
 for body in (reader.status(),reader.screener({'at':NEWEST_MARK}),reader.unusual()):
  capture=body.get('capture')
  if capture is None:continue
  if capture['state']=='partial_capture':assert capture['healthy'] is False
  assert capture['healthy']==(capture['state'] in D.CAPTURE_HEALTHY)


def test_the_tab_status_reports_derivative_coverage_separately_from_the_app_feed(reader):
 """The global chip describes the cash feed. This one describes F&O capture, and they can disagree."""
 body=reader.status()
 assert body['capture'] is not None
 assert body['capture']['state']=='partial_capture'
 assert body['capture']['healthy'] is False
 assert 'says nothing at all about F&O' in D.CAPTURE_TEXT or 'F&O' in body['capture']['capture_text']


def test_partial_records_stay_visible_where_the_fields_they_do_have_support_the_analysis(reader):
 """A partial reading is not a deleted reading. Open interest survived the outage, so PCR and the max-pain
 strike — both of which rest on open interest alone — are still served at the rebuilt readings."""
 pcr=reader.pcr_series('NIFTY')
 pain=reader.maxpain_series('NIFTY')
 assert [p['at'] for p in pcr['points']][-1]==NEWEST_MARK
 newest=next(p for p in pain['points'] if p['at']==NEWEST_MARK)
 assert newest['max_pain_strike']==NEWEST_STRIKE,'a strike that WAS measured is still shown'
 assert newest['spot'] is None,'and the one that was not stays empty'
 assert newest['distance'] is None


def test_capture_recovers_when_a_later_reading_is_complete_again(tmp_path):
 """Failure, retry and recovery: the state follows the store rather than sticking."""
 path=tmp_path/'recovery.db';_store(path)
 value=D.Derivatives(str(path))
 try:
  assert value.capture_health(NEWEST_MARK,cleared=0)['state']=='partial_capture'
  # the capture comes back at the next reading
  recovered=f'{SESSION} 15:50:00'
  connection=sqlite3.connect(path)
  connection.executemany('insert into metrics(scope,metric_key,captured_at,instrument_token,tradingsymbol,'
   'underlying,instrument_type,strike,expiry,lot_size,days_to_expiry,last_price,average_price,volume,oi,'
   'spot,premium_cr) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
   [('contract',s,recovered,t,s,'NIFTY',k,23300.0,EXPIRY,75,4,180.0,178.0,5e5,9e5,LIVE_SPOT,14.5)
    for t,s,k in ((101,'NIFTY26SEP23300CE','CE'),(102,'NIFTY26SEP23300PE','PE'))])
  connection.execute('insert into underlying_snapshots(underlying,captured_at,spot) values(?,?,?)',
   ('NIFTY',recovered,LIVE_SPOT))
  connection.commit();connection.close()
  value.close()
  value=D.Derivatives(str(path))
  health=value.capture_health(recovered,cleared=2,matched=2)
  assert health['state']=='complete' and health['healthy'] is True
  assert health['missing_fields']==[]
  assert health['latest_complete_at']==recovered
 finally:value.close()


def test_every_capture_state_has_its_own_sentence_and_none_of_them_says_quiet():
 """Five separate states, five separate sentences, and no two that can be read as the same fact."""
 texts=[D.CAPTURE_STATE_TEXT[state] for state in D.CAPTURE_STATES]
 assert len(set(texts))==len(texts),'two states sharing a sentence is two states the reader cannot tell apart'
 for text in texts:
  assert text and text[0].isupper()
  assert 'quiet' not in text.lower() or 'not a quiet market' in text


def test_nothing_the_reader_sees_calls_a_reading_a_mark(reader):
 """The reader's word for a 15-minute capture is "15-min reading". "Mark" is the worker's own word and
 never reaches the screen."""
 bodies=[reader.status(),reader.screener({}),reader.maxpain_series('NIFTY'),reader.capture_health(LIVE_MARK)]
 seen=[]
 def walk(value):
  if isinstance(value,dict):
   for key,item in value.items():
    # field NAMES are the wire contract, not copy; only the sentences are read
    if isinstance(item,str) and (key.endswith('text') or key.endswith('_reason') or key in
      ('state_text','empty_note','reading_rule','definition','distance_definition','capture_text')):
     seen.append(item)
    else:walk(item)
  elif isinstance(value,list):
   for item in value:walk(item)
 for body in bodies:walk(body)
 assert seen
 for text in seen:
  words=text.lower().replace('.',' ').replace(',',' ').split()
  assert 'mark' not in words and 'marks' not in words,text


# --- the boundary reaches the blocks, not just the cards that already took one ----------------------------

def test_the_delta_oi_grid_stops_at_the_tab_s_own_reading(tmp_path):
 """A block that ran on to a later reading than the chain beside it is one page describing two moments.

 On the recorded session those two moments are four hours apart: the screener resolved 11:30 and the grid
 resolved its own newest captured session, which ran to 15:45.
 """
 path=tmp_path/'grid.db'
 connection=sqlite3.connect(path);connection.executescript(SCHEMA)
 connection.executemany('insert into contracts(instrument_token,tradingsymbol,underlying,instrument_type,'
  'strike,expiry,lot_size,first_seen,last_seen) values(?,?,?,?,?,?,?,?,?)',
  [(101,'NIFTY26SEP23300CE','NIFTY','CE',23300.0,EXPIRY,75,SESSION,SESSION),
   (102,'NIFTY26SEP23300PE','NIFTY','PE',23300.0,EXPIRY,75,SESSION,SESSION)])
 every=[f'{SESSION} 09:30:00',LIVE_MARK,NEWEST_MARK]
 connection.executemany('insert into underlying_snapshots(underlying,captured_at,spot) values(?,?,?)',
  [('NIFTY',mark,LIVE_SPOT) for mark in every])
 connection.executemany('insert into snapshots(instrument_token,captured_at,last_price,oi,vendor_id,'
  "fetched_at,snapshot_id) values(?,?,?,?,'kite','x','y')",
  [(token,mark,180.0,900000) for token in (101,102) for mark in every])
 connection.commit();connection.close()
 value=D.Derivatives(str(path))
 try:
  wide=value.oi_grid('NIFTY',EXPIRY)
  assert wide['marks'][-1]==NEWEST_MARK,'with no boundary it still resolves the newest, exactly as before'
  bounded=value.oi_grid('NIFTY',EXPIRY,at=LIVE_MARK)
  assert bounded['marks'][-1]==LIVE_MARK,'with the boundary it stops there'
  assert NEWEST_MARK not in bounded['marks']
  assert bounded['session']==SESSION
  assert bounded['context']['at']==bounded['as_of']
 finally:value.close()


def test_the_grid_route_takes_the_boundary_over_http(tmp_path,outage):
 app,client=client_for(tmp_path,derivatives_database=outage)
 try:
  body=client.get(f'/api/derivatives/oi-grid?underlying=NIFTY&expiry={EXPIRY}&at={LIVE_MARK}').json()
  assert body['context']['at'] is None or body['context']['at']<=LIVE_MARK
  for stamp in body['marks'] or []:
   assert stamp<=LIVE_MARK,'no reading past the boundary the rest of the page is on'
 finally:app.state.db.close()


def test_the_boundary_is_read_in_ist_and_not_in_the_server_s_own_timezone(monkeypatch):
 """TIMEZONE. The session on screen is an IST trading day. A server in Pacific time is hours behind it, and
 that is exactly how the recorded case produced two different day counts for one option: one panel counted
 from the captured IST date and the other from `date.today()` wherever the process happened to be."""
 assert D.ANALYSIS_TZ=='Asia/Kolkata'
 # IST is UTC+5:30, so late evening in the Americas is already the NEXT trading day in India
 from datetime import date as _date
 monkeypatch.setattr(D,'today_ist',lambda:_date(2026,9,20))
 assert D.AnalysisContext(session='2026-09-20').mode=='live'
 assert D.AnalysisContext(session=SESSION).mode=='historical'
 # the day count of the recorded case does NOT move with the clock: it is 4 from the 18 Sep session, always
 assert D.session_days_to_expiry(EXPIRY,SESSION)==4
 monkeypatch.setattr(D,'today_ist',lambda:_date(2026,9,21))
 assert D.session_days_to_expiry(EXPIRY,SESSION)==4
 # and the today-based helper DOES move, which is why it is not used beside a captured figure
 assert D.days_to_expiry(EXPIRY)==1


def test_no_reading_named_means_the_newest_reading_not_no_reading(reader):
 """Found live 21 Sep 2026 09:36 IST: the app-wide chip calls the capture route with no `at`, and the route
 answered `missing_capture` — "F&O not captured" — through a complete capture. Unnamed means the newest."""
 unnamed=reader.capture_health()
 newest=unnamed['latest_attempted_at']
 assert newest,'the fixture store must hold a reading for this to mean anything'
 named=reader.capture_health(newest)
 assert unnamed['at']==newest,'an unnamed request is answered about the newest attempted reading'
 assert unnamed['state']==named['state'] and unnamed['rows']==named['rows']
 assert not (unnamed['state']=='missing_capture' and named['rows']>0),\
  'a reading holding rows may never be reported as not captured'
