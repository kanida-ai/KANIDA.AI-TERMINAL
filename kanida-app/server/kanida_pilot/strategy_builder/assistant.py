"""Slice 8 — K12 Adjustment assistant: candidate adjustments for a strategy (a draft, or what a paper deployment holds).

For each rule in adjust.RULES it computes, from the market reading now:
  * the delta orders (only what changes), priced where they would execute (buy at the ask, sell at the bid; LTP
    fallback labelled), with estimated charges;
  * the exact expiry P&L of the position AFTER the adjustment: the current legs from THEIR entry prices plus the delta
    orders at today's prices, net of the orders' charges - overlaid on the current position's curve;
  * worst case, breakevens, the model delta change and the exchange-margin change (when a broker margin is readable);
  * the Lab evidence for exactly that rule on exactly that structure, or 'Model only'.
It never orders. Choosing a candidate writes a new draft version; a deployment then reviews the delta orders.
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional

from . import adjust as ADJ
from . import analytics as A
from . import charges as CH
from . import service as S
from .templates import recognise

CANDIDATES = [('roll_tested_short', 1), ('roll_tested_short', 2), ('add_hedge_wing', 2), ('close_tested_side', None),
              ('reduce_half', None), ('close_all', None), ('roll_out', None)]


class AssistError(Exception):
 def __init__(self, status, code, message):
  super().__init__(message);self.status = status;self.code = code;self.message = message


def _intr(t, k, x):
 return max(x - k, 0.0) if t == 'CE' else max(k - x, 0.0)


def _pnl(x, legs, lot):
 """Expiry P&L of legs [{type, strike, units(+/-lots), price}] at spot x, in rupees."""
 return sum(l['units'] * lot * (_intr(l['type'], l['strike'], x) - l['price']) for l in legs)


def _profile(legs, lot, fees):
 """Exact worst case and breakevens of a piecewise-linear expiry P&L (kinks at strikes)."""
 ks = sorted({float(l['strike']) for l in legs})
 if not ks:
  return {'worst': round(-fees, 2), 'unlimited_loss': False, 'breakevens': [], 'best': round(-fees, 2)}
 pts = [0.0] + ks + [ks[-1] * 2]
 vals = [_pnl(x, legs, lot) - fees for x in pts]
 slope_up = sum(l['units'] for l in legs if l['type'] == 'CE')          # lots of net calls beyond the top strike
 bes = [pts[-1]] if vals[-1] == 0 else []
 for (x0, v0), (x1, v1) in zip(zip(pts, vals), zip(pts[1:], vals[1:])):
  if v0 == 0:
   bes.append(x0)
  elif (v0 < 0) != (v1 < 0):
   bes.append(x0 + (x1 - x0) * (-v0) / (v1 - v0))
 return {'worst': None if slope_up < 0 else round(min(vals), 2), 'unlimited_loss': slope_up < 0,
         'best': None if slope_up > 0 else round(max(vals), 2), 'breakevens': sorted({round(b, 2) for b in bes})}


def _exec_price(row, side):
 bid, ask, ltp = row.get('bid'), row.get('ask'), row.get('ltp')
 if bid and ask:
  return (ask if side == 'B' else bid), 'exec'
 return ltp, 'ltp'


def _value(x):
 return x.get('value') if isinstance(x, dict) and x.get('status') == 'available' else None


def _conditioned(ev, t, chain, adjusted_before):
 """Lab evidence applies only when today looks like the Lab's triggered trades: the tested short within the run's
 trigger, days to expiry inside the run's range, and no earlier adjustment (the Lab allows one per trade)."""
 if not ev:
  return None
 why = []
 if t is None or t[1] * 100 > ev['trigger_pct']:
  why.append(f"the tested short is {'not' if t is None else f'{t[1]*100:.2f}%'} from the money, the Lab triggered at {ev['trigger_pct']:g}%")
 dte = chain.get('days_to_expiry')
 if dte is None or dte > ev['dte'][1]:
  why.append(f"{dte} days to expiry, the Lab entered at {ev['dte'][0]}-{ev['dte'][1]}")
 if adjusted_before:
  why.append('this position was already adjusted (the Lab tests one adjustment per trade)')
 if why:
  return {'status': 'model_only', 'label': 'Model only - Lab conditions differ', 'note': 'Lab run exists but ' + '; '.join(why) + '.', 'lab': ev}
 return ev


def position_body(deployment) -> Dict[str, Any]:
 """The body a paper deployment actually HOLDS now: filled units per leg, in whole lots, with average entry prices."""
 lot = deployment.get('lot_size')
 if not lot:
  raise AssistError(409, 'MARKET_DATA_NOT_LIVE', 'The lot size could not be read, so the held position cannot be sized.')
 meta = {l['id']: l for l in (deployment.get('leg_meta') or deployment['revision_body'].get('legs', []))}
 legs = []
 for p in deployment['positions']:
  if not p['units']:
   continue
  if p['units'] % lot:
   raise AssistError(409, 'PART_LOT_POSITION', 'The deployment holds a part-lot position; adjust it manually.')
  m = meta.get(p['leg_id'])
  if not m:
   raise AssistError(409, 'UNKNOWN_LEG', 'A held leg is missing from the deployment revisions.')
  legs.append({**m, 'side': 'B' if p['units'] > 0 else 'S', 'lots': abs(p['units']) // lot, 'price_basis': 'manual', 'entry_from_fills': True,
               'price': p['avg'], 'include': True})
 if not legs:
  raise AssistError(409, 'NOTHING_HELD', 'This deployment holds no open position.')
 return {**deployment['revision_body'], 'legs': legs, 'scenario': {}}


def candidates(market, body: Dict[str, Any], evidence=None, held=False, adjusted_before=False) -> Dict[str, Any]:
 """evidence(template, param, rule, k) -> dict|None. held=True when `body` is a deployment's position (entry prices
 are the fills); otherwise each leg's own price basis is its entry."""
 chain, legs, problems = S.hydrate(market, body)
 if not chain:
  raise AssistError(409, 'NO_MARKET', (problems or ['Choose an underlying and an expiry.'])[0])
 if problems:
  raise AssistError(409, 'UNRESOLVED_CONTRACT', problems[0])
 legs = [l for l in legs if l.get('include', True)]
 if not legs:
  raise AssistError(400, 'EMPTY_STRATEGY', 'There are no legs to adjust.')
 spot, lot = chain['spot'], chain['lot_size']
 rows = {r['strike']: r for r in chain['rows']}
 grid = sorted(r['strike'] for r in chain['rows'])
 # the next listed expiry after the position's, for "roll out" (its own chain prices the new legs)
 cur_exp = max(l['expiry'] for l in legs)
 try:
  nxt = next((x['expiry'] for x in market.expiries(body['underlying'])['expiries'] if x['expiry'] > cur_exp), None)
 except Exception:  # noqa: BLE001
  nxt = None
 chains_rows = {chain['expiry']: rows}
 def rows_for(e):
  e = e or chain['expiry']
  if e not in chains_rows:
   ch = market.chain(body['underlying'], e)
   chains_rows[e] = {r['strike']: r for r in (ch or {}).get('rows', [])}
  return chains_rows[e]
 cur = [{'type': l['type'], 'strike': l['strike'], 'units': (1 if l['side'] == 'B' else -1) * int(l['lots']), 'price': l['price']} for l in legs]
 if any(c['price'] is None for c in cur):
  raise AssistError(409, 'NO_PRICE', 'A leg has no price in the reading, so its entry cannot be valued.')
 cur_prof = _profile(cur, lot, 0.0)
 a_cur = S.analysis(market, body, table=False)
 t = ADJ.tested(legs, spot)
 st = recognise(legs)
 # evidence belongs to the structure the legs ARE; a stale template tag on an edited draft never earns it
 template = st['key'] if st.get('exact') else None
 param = body.get('param') if (template and body.get('template') == template) else None
 xs = sorted({round(p['s'], 2) for p in (a_cur.get('curve') or [])} | {float(l['strike']) for l in legs})
 out = []
 for rule, k in CANDIDATES:
  meta = ADJ.RULES[rule]
  cand = {'rule': rule, 'k': k, 'name': meta['name'] + (f' +{k}' if k else ''), 'explain': meta['explain'].format(k=k or '', to=nxt or 'the next expiry')}
  try:
   new_legs, note = ADJ.apply(rule, legs, spot, grid, k or 1, to_expiry=nxt)
  except ADJ.NotApplicable as e:
   out.append({**cand, 'available': False, 'reason': str(e)});continue
  orders = ADJ.delta_orders(legs, new_legs)
  priced, basis, fees, cash, missing = [], set(), 0.0, 0.0, []
  for o in orders:
   row = (rows_for(o.get('expiry')).get(o['strike']) or {}).get(o['type'])
   if not row:
    missing.append(f"{o['strike']:g} {o['type']}");continue
   px, b = _exec_price(row, o['side'])
   if px is None:
    missing.append(f"{o['strike']:g} {o['type']}");continue
   qty = o['lots'] * lot;basis.add(b)
   f = CH.leg_charges(o['side'], px, qty)['total'];fees += f
   cash += (-1 if o['side'] == 'B' else 1) * px * qty
   priced.append({**o, 'symbol': row['symbol'], 'qty': qty, 'price': px, 'basis': b, 'charges': round(f, 2)})
  if missing:
   out.append({**cand, 'available': False, 'reason': 'No executable price for ' + ', '.join(missing)});continue
  after = cur + [{'type': o['type'], 'strike': o['strike'], 'units': (1 if o['side'] == 'B' else -1) * o['lots'], 'price': o['price']} for o in priced]
  prof = _profile(after, lot, fees)
  new_body = {**body, 'legs': [{k2: v for k2, v in l.items() if k2 in ('id', 'type', 'side', 'strike', 'lots', 'expiry', 'price_basis', 'price', 'include')}
                               | ({'price_basis': 'exec', 'price': None} if not held else {}) for l in new_legs],
              'template': None, 'param': None, 'scenario': {}}
  if new_legs:   # the strategy's expiry is its nearest leg's (a roll-out moves it; review H1/H2)
   new_body['expiry'] = min(l.get('expiry') or body['expiry'] for l in new_legs)
  if held:        # the draft that records a deployment's adjusted position uses live prices for its analysis
   new_body['legs'] = [{**l, 'price_basis': 'exec', 'price': None} for l in new_body['legs']]
  a_new = S.analysis(market, new_body, table=False) if new_legs else None
  d_cur = (a_cur.get('greeks') or {}).get('delta');d_new = ((a_new or {}).get('greeks') or {}).get('delta') if a_new else 0.0
  m_cur, m_new = _value(a_cur.get('margin')), (_value(a_new.get('margin')) if a_new else 0.0)
  ev = _conditioned(evidence(template, param, rule, k), t, chain, adjusted_before) if (evidence and template) else None
  out.append({**cand, 'available': True, 'note': note, 'orders': priced, 'price_basis': sorted(basis),
   'cash': round(cash, 2), 'charges': round(fees, 2),
   'after': {'worst': prof['worst'], 'unlimited_loss': prof['unlimited_loss'], 'best': prof['best'], 'breakevens': prof['breakevens']},
   'delta': {'current': d_cur, 'after': d_new, 'change': round(d_new - d_cur, 2) if d_cur is not None and d_new is not None else None},
   'margin': {'current': m_cur, 'after': m_new, 'change': round(m_new - m_cur, 2) if m_cur is not None and m_new is not None else None},
   'overlay': [{'s': x, 'current': round(_pnl(x, cur, lot), 2), 'after': round(_pnl(x, after, lot) - fees, 2)} for x in xs],
   'structure_after': recognise(new_legs)['name'] if new_legs else 'Flat',
   'body': new_body,
   'evidence': ev or {'status': 'model_only', 'label': 'Model only - no Lab run of this adjustment rule on this structure'}})
 unavailable = [{'rule': r, 'name': n, 'available': False, 'reason': why} for r, (n, why) in ADJ.UNAVAILABLE.items()]
 return {'as_of': chain['as_of'], 'spot': spot, 'lot_size': lot, 'quality': chain['quality'], 'structure': st['name'],
  'template': template, 'param': param, 'held': held,
  'tested': None if not t else {'leg_id': t[0].get('id'), 'label': f"{t[0]['strike']:g} {t[0]['type']} short", 'distance_pct': round(t[1] * 100, 2)},
  'current': {'worst': cur_prof['worst'], 'unlimited_loss': cur_prof['unlimited_loss'], 'breakevens': cur_prof['breakevens'],
              'delta': (a_cur.get('greeks') or {}).get('delta'), 'margin': _value(a_cur.get('margin'))},
  'entry_basis': 'deployment fills (average price)' if held else 'each leg\'s own price basis at this reading',
  'candidates': out + unavailable,
  'notes': ['P&L after an adjustment = the current legs from their entry prices + the delta orders at today\'s executable prices, net of estimated charges on the delta orders.',
            'Expiry P&L; before expiry the model value differs. Delta is the model delta in rupees per 1-point move.',
            'Nothing is ordered here. Choosing an adjustment writes a new version; a paper deployment then reviews only the delta orders.']}
