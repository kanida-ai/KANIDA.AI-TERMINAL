"""Spreads mode (Robinhood's pre-paired spread chain, adapted): every vertical spread of one kind and width in an expiry,
one row per anchor strike, priced where it would execute and with the risk numbers on the row.

Kinds (type + side of the NET premium):
  CE debit  = bull call spread : buy K,        sell K + width
  CE credit = bear call spread : sell K,       buy K + width
  PE debit  = bear put spread  : buy K + width, sell K
  PE credit = bull put spread  : sell K + width, buy K
Prices: buy at the ask and sell at the bid when the reading has a quote; otherwise the last traded price, labelled.
Every number is the model at this reading (POP lognormal at the chain's ATM IV); nothing is a forecast.
Choosing a row creates an ordinary strategy (the same draft and revisions as building it by hand).
"""
from __future__ import annotations
from typing import Any, Dict, List

from . import analytics as A
from . import service as S
from . import charges as CH

KINDS = {('CE', 'debit'): ('bull_call_spread', 'Bull Call Spread'), ('CE', 'credit'): ('bear_call_spread', 'Bear Call Spread'),
         ('PE', 'debit'): ('bear_put_spread', 'Bear Put Spread'), ('PE', 'credit'): ('bull_put_spread', 'Bull Put Spread')}
MAX_ROWS = 16      # the strikes nearest the money; the table opens where decisions are made


class SpreadsError(Exception):
    def __init__(self, message):
        super().__init__(message);self.message = message


def _px(q, side):
    px, basis, _ = S.exec_price(q, side)     # one quote policy: a crossed/zero book is never 'exec' (audit P05)
    return px, basis


def build(chain: Dict[str, Any], kind: str, side: str, width: int, lots: int = 1) -> Dict[str, Any]:
    kind, side = kind.upper(), side.lower()
    if (kind, side) not in KINDS:
        raise SpreadsError('type must be CE or PE and side debit or credit')
    if not 1 <= width <= 20:
        raise SpreadsError('width must be 1-20 strikes')
    if not 1 <= lots <= 500:
        raise SpreadsError('lots must be 1-500')
    key, name = KINDS[(kind, side)]
    spot, lot = chain['spot'], chain['lot_size']
    reading = A.parse_ist(chain['as_of'])
    ks = sorted(r['strike'] for r in chain['rows'] if r.get(kind) and r[kind].get('ltp') is not None)
    rows = {r['strike']: r for r in chain['rows']}
    atm = min(ks, key=lambda k: abs(k - spot)) if ks else None
    out = [];inconsistent = 0
    for i, k1 in enumerate(ks):
        if i + width >= len(ks):
            break
        k2 = ks[i + width]
        lo, hi = rows[k1][kind], rows[k2][kind]
        # which strike is bought: calls debit/puts credit buy the LOWER strike
        buy_low = (kind == 'CE' and side == 'debit') or (kind == 'PE' and side == 'credit')
        legs_spec = [('B', k1, lo), ('S', k2, hi)] if buy_low else [('S', k1, lo), ('B', k2, hi)]
        legs, basis = [], set()
        for j, (sd, k, q) in enumerate(legs_spec):
            p, b = _px(q, sd)
            if p is None:
                break
            basis.add(b)
            legs.append({'id': f'L{j + 1}', 'type': kind, 'side': sd, 'strike': k, 'lots': lots, 'lot_size': lot, 'expiry': chain['expiry'],
                         'price': p, 'price_basis': 'exec', 'ltp': q.get('ltp'), 'bid': q.get('bid'), 'ask': q.get('ask'), 'include': True,
                         'token': q.get('token'), 'symbol': q.get('symbol')})
        if len(legs) != 2:
            continue
        net = sum((1 if l['side'] == 'S' else -1) * l['price'] for l in legs)          # + credit, - debit, per unit
        if (side == 'debit' and net >= 0) or (side == 'credit' and net <= 0):
            continue                                                                  # an inverted quote: not a real spread
        if abs(net) >= (k2 - k1):
            inconsistent += 1                                                         # a premium at/over the width is impossible:
            continue                                                                  # stale last-trade prices, never shown as a spread
        _j, a = S.analyze_on_chain(chain, [{**l, 'price_basis': 'manual'} for l in legs], grid_points=3)   # same reference as the builder (P06)
        if a.get('status') != 'ok':
            continue
        units = lots * lot
        fees = sum(CH.leg_charges(l['side'], l['price'], units)['total'] for l in legs)
        ml, mp = a['max_loss'], a['max_profit']
        risk = -ml['value'] if ml.get('value') is not None else None
        g = a.get('greeks') or {}
        out.append({'anchor': k1, 'strikes': [k1, k2], 'points': k2 - k1, 'executable': basis == {'exec'}, 'distance_pct': round((k1 / spot - 1) * 100, 2), 'atm': k1 == atm,
                    'net': round(net * units, 2), 'net_per_unit': round(net, 2), 'direction': 'credit' if net > 0 else 'debit',
                    'max_profit': mp.get('value'), 'max_loss': ml.get('value'), 'breakevens': (a['breakevens'] or {}).get('value'),
                    'pop': (a['pop'] or {}).get('value'), 'return_on_risk': round((mp['value'] / risk) * 100, 1) if (risk and mp.get('value') is not None) else None,
                    'delta': g.get('delta'), 'theta': g.get('theta'), 'charges': round(fees, 2), 'price_basis': sorted(basis),
                    'legs': [{k: l[k] for k in ('type', 'side', 'strike', 'lots', 'expiry', 'price')} for l in legs]})
    # nearest the money first, then keep a symmetric window around it
    out.sort(key=lambda r: abs(r['distance_pct']))
    out = sorted(out[:MAX_ROWS], key=lambda r: r['anchor'])
    return {'template': key, 'name': name, 'type': kind, 'side': side, 'width': width, 'lots': lots, 'expiry': chain['expiry'],
            'as_of': chain['as_of'], 'spot': spot, 'lot_size': lot, 'rows': out, 'excluded_inconsistent': inconsistent,
            'basis': 'Buy at the ask, sell at the bid where quoted (else last traded, labelled). Max P/L at expiry, gross of charges; POP is a lognormal model at the ATM IV. Not a forecast.'}
