"""F&O OPTIONS charges — an ESTIMATE for one set of order legs, labelled so wherever it is shown.

engine/backend/autotrade/charges.py is equity-only (its docstring flags F&O as a follow-up), so the builder carries
its own small table. They are NOT fetched from anywhere and are not a contract note.

Per order leg (premium turnover = price x units):
  brokerage   flat per executed order (default Rs 20; brokers differ)
  STT         on the SELL side, at the rate IN FORCE ON THE TRADE DATE (data/stt_schedule.json, slice 14 / E07)
  exchange    NSE options transaction charge on premium turnover
  SEBI        Rs 10 per crore of turnover
  stamp duty  0.003% of premium on the BUY side
  GST         18% on (brokerage + exchange + SEBI)

STT is effective-dated (point in time): every rate in the schedule carries its effective date and its primary source
(NSE circulars / Finance Bill memorandum). A date before the schedule's coverage is refused with STTScheduleGap, never
guessed. Exercise STT is charged on the base in force on the expiry date: settlement price x quantity before
1 Sep 2019, intrinsic value x quantity from then. The other components are the current (Sep 2026) values for every
date - a stated limitation, see the schedule's `known_gaps`.
"""
from __future__ import annotations
import json
from datetime import date,datetime
from pathlib import Path

_SCHEDULE_FILE=Path(__file__).with_name('data')/'stt_schedule.json'
_SCHEDULE=json.loads(_SCHEDULE_FILE.read_text())
STT_SCHEDULE_VERSION=_SCHEDULE['version']
STT_COVERAGE_FROM=date.fromisoformat(_SCHEDULE['coverage_from'])
#: Bumped whenever any rate or rule changes: every stored net result names the version it was charged under, and a run
#: under another version is superseded evidence (lab.evidence_board quarantines it).
VERSION=f'fo-options-2026-09-estimate-v2+{STT_SCHEDULE_VERSION}'
RATES={'brokerage_per_order':20.0,'exchange':0.0003503,'sebi':10/1e7,'stamp_buy':0.00003,'gst':0.18}


class STTScheduleGap(ValueError):
 """The trade date is outside the verified STT schedule, or the base the law used then was not supplied."""


def _as_date(trade_date):
 if trade_date is None:
  from .exchange import now_ist
  return now_ist().date()
 if isinstance(trade_date,datetime):return trade_date.date()
 if isinstance(trade_date,date):return trade_date
 return date.fromisoformat(str(trade_date).strip()[:10])


def _entry(kind,trade_date):
 d=_as_date(trade_date)
 if d<STT_COVERAGE_FROM:
  raise STTScheduleGap(f'No verified STT rate before {STT_COVERAGE_FROM.isoformat()} (asked for {d.isoformat()}).')
 hist=_SCHEDULE[kind]['history']
 best=None
 for e in hist:
  if date.fromisoformat(e['effective_from'])<=d:best=e
 if best is None:raise STTScheduleGap(f'No {kind} STT rate in force on {d.isoformat()}.')
 return best


def stt_rates(trade_date=None):
 """The three STT rates in force on `trade_date` (default: today IST), each with effective date and source."""
 out={}
 for kind in ('option_sale','option_exercise','futures_sale'):
  e=_entry(kind,trade_date)
  src=_SCHEDULE['sources'].get(e['source'],{})
  out[kind]={'rate':e['rate'],'effective_from':e['effective_from'],'base':e.get('base') or _SCHEDULE[kind].get('base'),
   'source':src.get('url'),'source_title':src.get('title')}
 return {'date':_as_date(trade_date).isoformat(),'schedule':STT_SCHEDULE_VERSION,**out}


def option_sale_stt_rate(trade_date=None):
 return _entry('option_sale',trade_date)['rate']


def exercise_stt(settlement_value,trade_date=None,notional_value=None):
 """STT on the exercise of an in-the-money LONG option, effective-dated on the expiry (`trade_date`).

 settlement_value: intrinsic value x quantity (the base from 1 Sep 2019). notional_value: underlying settlement price x
 quantity - the base before 1 Sep 2019; required for such dates, else STTScheduleGap (the base is never substituted).
 """
 if not settlement_value or settlement_value<=0:return 0.0
 e=_entry('option_exercise',trade_date)
 if e.get('base')=='settlement_price':
  if notional_value is None:
   raise STTScheduleGap(f"Exercise STT on {_as_date(trade_date).isoformat()} was charged on the settlement price (notional); pass notional_value.")
  return abs(notional_value)*e['rate']
 return abs(settlement_value)*e['rate']


def futures_stt(turnover,trade_date=None):
 """STT on a futures SALE at the rate in force on `trade_date`."""
 return abs(turnover)*_entry('futures_sale',trade_date)['rate']


def leg_charges(side,price,units,trade_date=None,rates=None):
 """Charges for one option order leg on `trade_date` (default today IST). `rates` overrides the non-STT constants
 (and STT itself when it carries 'stt_sell'); a dict passed positionally in the old 4th slot is still honoured."""
 if isinstance(trade_date,dict) and rates is None:rates,trade_date=trade_date,None
 rates=RATES if rates is None else rates
 turnover=abs(price*units)
 brokerage=rates['brokerage_per_order'] if turnover>0 else 0.0
 if side=='S':
  stt=turnover*(rates['stt_sell'] if 'stt_sell' in rates else option_sale_stt_rate(trade_date))
 else:stt=0.0
 exch=turnover*rates['exchange'];sebi=turnover*rates['sebi']
 stamp=turnover*rates['stamp_buy'] if side=='B' else 0.0
 gst=(brokerage+exch+sebi)*rates['gst']
 return {'brokerage':brokerage,'stt':stt,'exchange':exch,'sebi':sebi,'stamp':stamp,'gst':gst,
  'total':brokerage+stt+exch+sebi+stamp+gst}


def estimate(legs,rates=None,trade_date=None):
 """Entry charges for `legs` ({side, price, lots, lot_size}), itemised. Exit charges are not included."""
 parts={'brokerage':0.0,'stt':0.0,'exchange':0.0,'sebi':0.0,'stamp':0.0,'gst':0.0,'total':0.0}
 for l in legs:
  u=int(l['lots'])*int(l['lot_size'])
  for k,v in leg_charges(l['side'],float(l['price'] or 0),u,trade_date=trade_date,rates=rates).items():parts[k]+=v
 d=_as_date(trade_date)
 return {**{k:round(v,2) for k,v in parts.items()},'version':VERSION,'scope':'entry orders only',
  'stt_rate_sell':option_sale_stt_rate(d),'stt_as_of':d.isoformat(),
  'note':'Estimate from published rates (STT at the rate in force on the trade date) and a flat Rs 20 brokerage per order; your broker contract note is the truth.'}
