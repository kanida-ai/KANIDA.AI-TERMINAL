"""F&O OPTIONS charges — an ESTIMATE for one set of order legs, labelled so wherever it is shown.

engine/backend/autotrade/charges.py is equity-only (its docstring flags F&O as a follow-up), so the builder carries
its own small table. Rates are one versioned constants block; a rate change is a one-line edit plus a new version.
They are published NSE / SEBI / statutory rates for index and stock OPTIONS as understood in Sep 2026 and a flat
broker brokerage. They are NOT fetched from anywhere and are not a contract note.

Per order leg (premium turnover = price x units):
  brokerage   flat per executed order (default Rs 20; brokers differ)
  STT         0.1% of premium on the SELL side
  exchange    NSE options transaction charge on premium turnover
  SEBI        Rs 10 per crore of turnover
  stamp duty  0.003% of premium on the BUY side
  GST         18% on (brokerage + exchange + SEBI)
"""
from __future__ import annotations

VERSION='fo-options-2026-09-estimate'
RATES={'brokerage_per_order':20.0,'stt_sell':0.001,'exchange':0.0003503,'sebi':10/1e7,'stamp_buy':0.00003,'gst':0.18}


def leg_charges(side,price,units,rates=RATES):
 turnover=abs(price*units)
 brokerage=rates['brokerage_per_order'] if turnover>0 else 0.0
 stt=turnover*rates['stt_sell'] if side=='S' else 0.0
 exch=turnover*rates['exchange'];sebi=turnover*rates['sebi']
 stamp=turnover*rates['stamp_buy'] if side=='B' else 0.0
 gst=(brokerage+exch+sebi)*rates['gst']
 return {'brokerage':brokerage,'stt':stt,'exchange':exch,'sebi':sebi,'stamp':stamp,'gst':gst,
  'total':brokerage+stt+exch+sebi+stamp+gst}


def estimate(legs,rates=RATES):
 """Entry charges for `legs` ({side, price, lots, lot_size}), itemised. Exit charges are not included."""
 parts={'brokerage':0.0,'stt':0.0,'exchange':0.0,'sebi':0.0,'stamp':0.0,'gst':0.0,'total':0.0}
 for l in legs:
  u=int(l['lots'])*int(l['lot_size'])
  for k,v in leg_charges(l['side'],float(l['price'] or 0),u,rates).items():parts[k]+=v
 return {**{k:round(v,2) for k,v in parts.items()},'version':VERSION,'scope':'entry orders only',
  'note':'Estimate from published rates and a flat Rs 20 brokerage per order; your broker contract note is the truth.'}
