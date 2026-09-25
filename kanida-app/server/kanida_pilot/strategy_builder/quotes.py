"""ONE quote-validity contract for analysis, order review, paper fills and alerts (GTM audit P02).

A source being "live" says nothing about a sample being fresh. Every executable or alerting decision checks the
sample itself:
  * crossed (bid > ask), zero or missing bid/ask         -> not executable
  * the option's OWN quote time (never the spot's time)   -> stale beyond the purpose's limit
  * a timestamp in the future beyond a small clock skew  -> invalid (a clock or feed problem, not a fresh quote)
Research views may still show such values, but only marked indicative.
"""
from __future__ import annotations
from .analytics import parse_ist

POLICY='quote-validity-v1'
CLOCK_SKEW=5            # seconds a quote may appear to be ahead of our clock
MAX_AGE={'order':30,    # an option quote used to build or fill an order
 'spot_order':15,       # the underlying reading behind an order preview
 'alert':120}           # the reading an alert evaluates


def age(at_text,now):
 """Seconds between the sample time and now (negative = the sample claims a future time), or None."""
 t=parse_ist(at_text)
 return None if t is None else (now-t).total_seconds()


def sample(at_text,now,purpose):
 """{ok, reason, age}. reason: NO_TIME | FUTURE_TIME | STALE | None."""
 a=age(at_text,now)
 if a is None:return {'ok':False,'reason':'NO_TIME','age':None}
 if a<-CLOCK_SKEW:return {'ok':False,'reason':'FUTURE_TIME','age':round(a,1)}
 if a>MAX_AGE[purpose]:return {'ok':False,'reason':'STALE','age':round(a,1)}
 return {'ok':True,'reason':None,'age':round(max(a,0.0),1)}


def book(bid,ask):
 """{ok, reason}. reason: NO_BID_ASK | ZERO | CROSSED | None. bid == ask (locked) is allowed."""
 if bid is None or ask is None:return {'ok':False,'reason':'NO_BID_ASK'}
 if bid<=0 or ask<=0:return {'ok':False,'reason':'ZERO'}
 if bid>ask:return {'ok':False,'reason':'CROSSED'}
 return {'ok':True,'reason':None}


def leg(row,now,purpose='order'):
 """Executable validity of one option quote: {ok, reasons[], age}. `row` carries bid, ask and quote_at."""
 b=book(row.get('bid'),row.get('ask'));s=sample(row.get('quote_at'),now,purpose)
 reasons=[r for r in (b['reason'],s['reason']) if r]
 return {'ok':not reasons,'reasons':reasons,'age':s['age']}


TEXT={'NO_BID_ASK':'no live bid and ask','ZERO':'a zero bid or ask','CROSSED':'a crossed book (bid above ask)',
 'NO_TIME':'no quote time','FUTURE_TIME':'a quote time in the future','STALE':'a stale quote'}


def describe(reasons):return ', '.join(TEXT.get(r,r) for r in reasons)
