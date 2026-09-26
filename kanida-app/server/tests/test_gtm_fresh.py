"""Slice 13 - the fresh GTM audit (research/gtm-audit-fresh-2026-09-25-2355) as regressions.

Probes 1-4 are the audit's own assertions, unchanged. Probes 5 and 6 in the audit encoded the OLD adapter shapes
(assistant._profile on expiry-stripped legs; a bare analyze() without the chain reference). Those adapters are gone, so
here the same requirements are asserted through the real code paths: assistant.candidates vs the builder's analysis,
and discover.run / spreads.build vs the builder's analysis. Isolated temporary fixtures only.
"""
import math,sqlite3
from datetime import datetime
import pytest
from kanida_pilot.strategy_builder import service as S, analytics as A, assistant as AS, discover as D, spreads as SP
from kanida_pilot.strategy_builder.market import Market
from kanida_pilot import implied_vol as IV
from test_strategy_builder import store, EXP, SPOT, LOT, SIGMA, STRIKES, AT

FAR='2026-10-06'


def two_expiry_store(path):
 """The standard fixture plus a later expiry priced by BSM at the same volatility (a calendar's far leg)."""
 store(path)
 c=sqlite3.connect(path);t=A.years_between(datetime(2026,9,23,15,30),FAR);token=5000
 for k in STRIKES:
  for kind in ('CE','PE'):
   token+=1;px=round(IV.price_bs(SPOT,k,t,IV.RISK_FREE_RATE,SIGMA,kind),2)
   c.execute('insert into contracts values(?,?,?,?,?,?,?,?)',(token,f'NIFTY26OCT{k}{kind}','NIFTY',kind,k,FAR,LOT,0.05))
   c.execute('insert into snapshots values(?,?,?,?,?,?,?,?)',(token,AT,max(px,0.05),None,None,1e5,1e6,'2026-09-23 15:29:50'))
 c.commit();c.close();return path


# --- audit probes 1-4, verbatim requirements ---------------------------------------------------------------------------
def test_crossed_book_cannot_be_execution_price():
 price, basis = S.leg_price({'price_basis': 'exec', 'side': 'B'}, {'bid': 10, 'ask': 5, 'ltp': 7})
 assert basis != 'exec', (price, basis)
 assert (price, basis) == (7, 'ltp')                                   # the labelled indicative fallback
 assert S.exec_price({'bid': 0, 'ask': 5, 'ltp': 4}, 'S')[1:] == ('ltp', 'ZERO')


def test_fractional_lots_are_rejected():
 with pytest.raises(S.Invalid):
  S.normalize_body({'underlying': 'NIFTY', 'expiry': EXP,
                    'legs': [{'type': 'CE', 'side': 'B', 'strike': 23000, 'lots': 1.9}]})


def test_nonfinite_scenario_is_rejected():
 with pytest.raises(S.Invalid):
  S.normalize_body({'scenario': {'spot': float('nan')}})


@pytest.mark.parametrize('bad', [
 {'legs': [{'type': 'CE', 'side': 'B', 'strike': float('inf')}]},
 {'legs': [{'type': 'CE', 'side': 'B', 'strike': 23000, 'lots': 0}]},
 {'legs': [{'type': 'CE', 'side': 'B', 'strike': 23000, 'lots': -1}]},
 {'legs': [{'type': 'CE', 'side': 'B', 'strike': 23000, 'lots': True}]},
 {'legs': [{'type': 'CE', 'side': 'B', 'strike': 23000, 'price_basis': 'manual', 'price': float('nan')}]},
 {'expiry': '2026-02-31'},
 {'legs': [{'type': 'CE', 'side': 'B', 'strike': 23000, 'expiry': '2026-13-01'}]},
 {'legs': [{'type': 'CE', 'side': 'B', 'strike': 23000 + 50 * i} for i in range(9)]},
 {'scenario': {'spot': float('inf')}},
 {'scenario': {'iv_shift': 80}},
 {'scenario': {'iv_shift': float('nan')}},
])
def test_invalid_inputs_are_400s_never_clamped_or_truncated(bad):
 with pytest.raises(S.Invalid):
  S.normalize_body({'underlying': 'NIFTY', 'expiry': EXP, **bad})


def test_whole_numbers_given_as_text_or_float_are_accepted():
 b = S.normalize_body({'underlying': 'NIFTY', 'expiry': EXP, 'legs': [{'type': 'CE', 'side': 'B', 'strike': '23000', 'lots': 2.0}]})
 assert b['legs'][0]['lots'] == 2 and b['legs'][0]['strike'] == 23000.0


def test_unresolved_leg_does_not_leave_available_strategy_risk(tmp_path):
 m = Market(store(str(tmp_path / 'market.db')))
 try:
  body = S.normalize_body({'underlying': 'NIFTY', 'expiry': EXP, 'legs': [
   {'id': 'a', 'type': 'CE', 'side': 'B', 'strike': 23000},
   {'id': 'b', 'type': 'CE', 'side': 'S', 'strike': 30000}]})
  result = S.analysis(m, body)
  assert result.get('max_loss', {}).get('status') != 'available', result.get('max_loss')
  # fail closed everywhere, the missing contract named, the structure the user ASKED for (not "Long Call")
  assert result['status'] == 'incomplete' and result['curve'] == [] and result['table'] == []
  assert all(result[k]['status'] == 'unavailable' for k in S.POSITION_KEYS)
  assert result['unresolved'][0]['contract_id'] == f'NFO|NIFTY|{EXP}|30000|CE'
  assert result['structure']['key'] == 'bull_call_spread'
  # excluding the missing leg is the deliberate way to explore what is left
  body['legs'][1]['include'] = False
  assert S.analysis(m, body)['max_loss']['status'] == 'available'
 finally:
  m.close()


# --- probe 5: Adjust values a calendar exactly as the builder does ------------------------------------------------------
def test_calendar_adjustment_profile_agrees_with_builder_horizon(tmp_path):
 m = Market(two_expiry_store(str(tmp_path / 'market.db')))
 try:
  body = S.normalize_body({'underlying': 'NIFTY', 'expiry': EXP, 'legs': [
   {'id': 'near', 'type': 'CE', 'side': 'S', 'strike': 23000, 'expiry': EXP},
   {'id': 'far', 'type': 'CE', 'side': 'B', 'strike': 23000, 'expiry': FAR}]})
  builder = S.analysis(m, body, table=False)
  assert builder['status'] == 'ok' and builder['max_profit']['basis'] == 'model_at_near_expiry'
  got = AS.candidates(m, body)
  cur = got['current']
  assert cur['best'] == pytest.approx(builder['max_profit']['value'], abs=0.01)
  assert cur['worst'] == pytest.approx(builder['max_loss']['value'], abs=0.01)
  assert cur['breakevens'] == builder['breakevens']['value'] and len(cur['breakevens']) == 2
  assert got['horizon']['kind'] == 'model_near_expiry' and got['horizon']['expiry'] == EXP
  # the overlay's current curve is the builder's near-expiry curve, not a flat intrinsic loss
  top = max(p['current'] for p in next(c for c in got['candidates'] if c.get('available'))['overlay'])
  assert top > 0
  # close-all leaves only realised P&L: flat, and equal to today's modelled exit value less charges
  close = next(c for c in got['candidates'] if c['rule'] == 'close_all')
  assert close['available'] and close['after']['best'] == pytest.approx(close['after']['worst'], abs=0.01)
 finally:
  m.close()


def test_same_expiry_adjustment_is_exact_and_matches_the_builder(tmp_path):
 m = Market(store(str(tmp_path / 'market.db')))
 try:
  body = S.normalize_body({'underlying': 'NIFTY', 'expiry': EXP, 'legs': [
   {'id': 'a', 'type': 'CE', 'side': 'B', 'strike': 23000}, {'id': 'b', 'type': 'CE', 'side': 'S', 'strike': 23200}]})
  builder = S.analysis(m, body, table=False);got = AS.candidates(m, body)
  assert got['horizon']['kind'] == 'expiry_exact'
  assert got['current']['worst'] == builder['max_loss']['value'] and got['current']['best'] == builder['max_profit']['value']
  assert got['current']['breakevens'] == builder['breakevens']['value']
 finally:
  m.close()


# --- probe 6: one probability reference across Discover, spreads and the builder ----------------------------------------
def test_discovery_and_builder_share_probability_reference(tmp_path):
 m = Market(store(str(tmp_path / 'market.db')))
 try:
  chain = m.chain('NIFTY', EXP)
  chain['atm_iv'] = 10.0                   # the chain reference differs from the nearest leg's IV, as observed in the UI
  class Frozen:
   def chain(self, underlying, expiry): return chain
  res = D.run(chain, {'view': 'up', 'target': 23300, 'max_loss': 50000, 'lots': 1})
  card = next(c for c in res['candidates'] if c['template'] == 'bull_call_spread')
  body = S.normalize_body({'underlying': 'NIFTY', 'expiry': EXP, 'legs': [
   {k: l[k] for k in ('id', 'type', 'side', 'strike', 'lots', 'expiry')} | {'price_basis': 'exec'} for l in card['legs']]})
  builder = S.analysis(Frozen(), body)
  assert card['pop'] == builder['pop']['value'], (card['pop'], builder['pop'])
  assert card['reference']['source'] == builder['pop']['sigma_basis'] == 'chain_atm_iv'
  # the spread picker uses the same reference
  ks = sorted(l['strike'] for l in card['legs'])
  sp = SP.build(chain, 'CE', 'debit', int(round((ks[1] - ks[0]) / chain['strike_step'])))
  row = next(r for r in sp['rows'] if r['strikes'] == [l['strike'] for l in sorted(card['legs'], key=lambda l: l['strike'])])
  assert row['pop'] == builder['pop']['value']
  assert row['points'] == row['strikes'][1] - row['strikes'][0] and row['executable'] is False   # stored LTP: indicative
 finally:
  m.close()


# --- P01: canonical contract identity ------------------------------------------------------------------------------------
def test_equal_strike_and_type_in_two_expiries_are_two_contracts(tmp_path):
 m = Market(two_expiry_store(str(tmp_path / 'market.db')))
 try:
  body = S.normalize_body({'underlying': 'NIFTY', 'expiry': EXP, 'legs': [
   {'id': 'near', 'type': 'CE', 'side': 'S', 'strike': 23000, 'expiry': EXP},
   {'id': 'far', 'type': 'CE', 'side': 'B', 'strike': 23000, 'expiry': FAR}]})
  q = {x['id']: x for x in S.analysis(m, body)['legs_quotes']}
  assert q['near']['contract_id'] != q['far']['contract_id']
  assert q['near']['symbol'].startswith('NIFTY26SEP') and q['far']['symbol'].startswith('NIFTY26OCT')
  assert q['far']['ltp'] > q['near']['ltp']                              # the far contract's own price, never the near one's
 finally:
  m.close()
