"""Slice 8 — the adjustment rules catalogue (blueprint A §14 / K12), shared by the live assistant and the Lab.

Pure functions over a leg list ([{id?, type CE|PE, side B|S, strike, lots, ...}]), the underlying spot and the strike
grid that is actually tradeable. Nothing here prices, orders or reads a market; the assistant and the Lab do that, so
the rule a user applies today is byte-for-byte the rule the Lab backtested.

"Tested" is deterministic: among the SHORT legs, the one whose strike is closest to (or furthest past) the money -
distance = (strike - spot) / spot for a short call, (spot - strike) / spot for a short put; the smallest distance wins
(negative = in the money). A structure with no short leg has no tested side.
"""
from __future__ import annotations
import uuid
from typing import Any, Dict, List, Optional, Tuple

RULES = {
 'roll_tested_short': {'name': 'Roll the tested short away', 'needs_short': True, 'k': True,
  'explain': 'Move the tested short {k} strike(s) further out of the money; a long wing it would cross moves with it.'},
 'add_hedge_wing': {'name': 'Add a hedge wing', 'needs_short': True, 'k': True,
  'explain': 'Buy a long {k} strike(s) beyond each uncovered short, so the loss becomes capped (strangle -> condor, straddle -> iron butterfly).'},
 'close_tested_side': {'name': 'Close the tested side', 'needs_short': True, 'k': False,
  'explain': 'Close every leg of the tested option type (the short and its wing together); the other side stays open.'},
 'reduce_half': {'name': 'Reduce lots by half', 'needs_short': False, 'k': False,
  'explain': 'Halve every leg, keeping the structure; half the risk, half the reward.'},
 'close_all': {'name': 'Close all', 'needs_short': False, 'k': False,
  'explain': 'Close every leg and take the current result.'},
 'roll_out': {'name': 'Roll out to the next expiry', 'needs_short': False, 'k': False, 'lab': False,
  'explain': 'Close every leg and reopen the same strikes in the next expiry ({to}) - more time, at the cost of paying the spread twice.'},
}
UNAVAILABLE = {
 'convert_to_butterfly': ('Convert to a butterfly', 'Covered by "Add a hedge wing" for a short straddle (it becomes an iron butterfly); ratio butterflies are not in this release.'),
}


class NotApplicable(Exception):
 pass


def _units(l) -> int:
 return int(l['lots']) * (1 if l['side'] == 'B' else -1)


def distance(l, spot: float) -> float:
 """How far a SHORT leg is from the money, as a fraction of spot (negative = in the money)."""
 return ((l['strike'] - spot) if l['type'] == 'CE' else (spot - l['strike'])) / spot


def tested(legs: List[Dict[str, Any]], spot: float) -> Optional[Tuple[Dict[str, Any], float]]:
 shorts = [l for l in legs if l['side'] == 'S' and l.get('include', True)]
 if not shorts:
  return None
 best = min(shorts, key=lambda l: (distance(l, spot), l['type'], l['strike']))
 return best, distance(best, spot)


def _shift(strike: float, steps: int, grid: List[float]) -> float:
 """The strike `steps` grid positions away (positive = up). Raises when the grid runs out - never invents a strike."""
 g = sorted(set(grid))
 if strike not in g:
  raise NotApplicable(f'{strike:g} is not in the tradeable strike list')
 i = g.index(strike) + steps
 if not 0 <= i < len(g):
  raise NotApplicable('the strike list does not extend far enough')
 return g[i]


def _new_id() -> str:
 return 'A' + uuid.uuid4().hex[:6]


def apply(rule: str, legs: List[Dict[str, Any]], spot: float, grid: List[float], k: int = 1,
          tested_key: Optional[Tuple[str, float]] = None, to_expiry: Optional[str] = None) -> Tuple[List[Dict[str, Any]], str]:
 """The adjusted leg list and a one-line description. Unchanged legs are returned as the SAME dicts (ids kept);
 a changed contract is a new leg with a new id. Raises NotApplicable with the reason.

 tested_key=(type, strike) pins the tested short decided earlier (the Lab reads the trigger at a close and acts at
 the next open - the decision must not be re-made with the open's spot)."""
 if rule in UNAVAILABLE:
  raise NotApplicable(UNAVAILABLE[rule][1])
 if rule not in RULES:
  raise NotApplicable('unknown rule')
 act = [l for l in legs if l.get('include', True)]
 if not act:
  raise NotApplicable('there are no open legs')
 if RULES[rule]['needs_short'] and not any(l['side'] == 'S' for l in act):
  raise NotApplicable('there is no short leg to adjust')
 k = int(k or 1)
 if rule == 'close_all':
  return [], 'Close every leg'
 if rule == 'roll_out':
  if len({l.get('expiry') for l in act}) > 1:
   raise NotApplicable('the position spans expiries (a calendar or diagonal) - rolling every leg to one expiry would collapse it; roll legs individually')
  if not to_expiry:
   raise NotApplicable('no later expiry is listed to roll into')
  if any(l.get('expiry') == to_expiry for l in act):
   raise NotApplicable('the position is already in that expiry')
  return [{**l, 'id': _new_id(), 'expiry': to_expiry} for l in act], f'Close every leg and reopen the same strikes in {to_expiry}'
 if rule == 'reduce_half':
  if any(int(l['lots']) < 2 or int(l['lots']) % 2 for l in act):
   raise NotApplicable('every leg needs an even number of lots (2 or more) to halve without changing the structure')
  return [{**l, 'lots': int(l['lots']) // 2} for l in act], 'Halve every leg'
 t = None
 if tested_key is not None:
  t = next((l for l in act if l['side'] == 'S' and (l['type'], float(l['strike'])) == (tested_key[0], float(tested_key[1]))), None)
  if t is None:
   raise NotApplicable('the tested short is no longer held')
 else:
  t, _d = tested(act, spot)
 if rule == 'close_tested_side':
  rest = [l for l in act if l['type'] != t['type']]
  return rest, f"Close the {'call' if t['type'] == 'CE' else 'put'} side ({len(act) - len(rest)} leg(s))"
 if rule == 'roll_tested_short':
  away = k if t['type'] == 'CE' else -k
  new_k = _shift(t['strike'], away, grid)
  out, moved = [], []
  for l in act:
   if l is t:
    out.append({**l, 'id': _new_id(), 'strike': new_k});continue
   # only a wing BETWEEN the old and the new short strike is crossed; a long on the inner side stays put
   crossed = l['type'] == t['type'] and l['side'] == 'B' and ((t['strike'] < l['strike'] <= new_k) if t['type'] == 'CE' else (new_k <= l['strike'] < t['strike']))
   if crossed:
    nk = _shift(l['strike'], away, grid)
    out.append({**l, 'id': _new_id(), 'strike': nk});moved.append(f"{l['strike']:g}->{nk:g}")
   else:
    out.append(l)
  if len({(l['type'], l['strike'], l['side']) for l in out}) < len(out) or len({(l['type'], l['strike']) for l in out}) < len(out):
   raise NotApplicable('the roll would land on a strike another leg already uses')
  note = f"Roll the short {t['strike']:g} {t['type']} to {new_k:g}" + (f"; wing {', '.join(moved)}" if moved else '')
  return out, note
 if rule == 'add_hedge_wing':
  out = list(act);added = []
  for ot in ('CE', 'PE'):
   short_lots = -sum(_units(l) for l in act if l['type'] == ot and l['side'] == 'S')
   long_lots = sum(_units(l) for l in act if l['type'] == ot and l['side'] == 'B')
   gap = short_lots - long_lots
   if gap <= 0:
    continue
   shorts = [l['strike'] for l in act if l['type'] == ot and l['side'] == 'S']
   edge = max(shorts) if ot == 'CE' else min(shorts)
   wk = _shift(edge, k if ot == 'CE' else -k, grid)
   if any(l['type'] == ot and l['strike'] == wk for l in act):
    raise NotApplicable(f'a leg already uses the {wk:g} {ot}')
   out.append({'id': _new_id(), 'type': ot, 'side': 'B', 'strike': wk, 'lots': gap,
               'expiry': act[0].get('expiry'), 'price_basis': 'exec', 'price': None, 'include': True})
   added.append(f'{wk:g} {ot} x{gap}')
  if not added:
   raise NotApplicable('every short is already covered by a long wing')
  return out, 'Buy ' + ' and '.join(added)
 raise NotApplicable('unknown rule')


def delta_orders(current: List[Dict[str, Any]], target: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
 """Orders (in lots) that turn `current` into `target`, per contract (type, strike). Buys first."""
 # a contract is (type, strike, expiry): with multi-expiry the same strike in two expiries is two contracts (slice 12)
 def key(l):return (l['type'], float(l['strike']), l.get('expiry') or '')
 def book(legs):
  b = {}
  for l in legs:
   if l.get('include', True):
    b[key(l)] = b.get(key(l), 0) + _units(l)
  return b
 cur, tgt = book(current), book(target)
 ids = {key(l): l['id'] for l in list(target) + list(current) if l.get('id')}
 # a contract already held keeps the id its fills are booked under (so one contract is never split across two ids);
 # a new contract takes the target's id
 out = []
 for key in sorted(set(cur) | set(tgt), key=lambda k: (k[0], k[1], k[2])):
  d = tgt.get(key, 0) - cur.get(key, 0)
  if d:
   out.append({'id': ids.get(key) or _new_id(), 'type': key[0], 'strike': key[1], 'expiry': key[2] or None, 'side': 'B' if d > 0 else 'S', 'lots': abs(d),
               'effect': 'close' if abs(tgt.get(key, 0)) < abs(cur.get(key, 0)) and (tgt.get(key, 0) * cur.get(key, 0) >= 0) else 'open'})
 return sorted(out, key=lambda o: (o['side'] != 'B', o['type'], o['strike']))
