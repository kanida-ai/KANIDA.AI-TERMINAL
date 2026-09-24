"""The two figures the workspace needed that no existing route served: Greeks by strike, and volume per interval.

GREEKS are the screener's own: implied volatility from `implied_vol.solve` (exactly as the IV route calls it) and
delta / gamma from the same Black-Scholes-Merton model at the solved volatility, cached in var/screener.db. They
are COMPUTED, never exchange figures, and every blank carries the solver's reason. On an expiry's own expiry day
IV cannot be solved, so the nearest expiry rolls to the next one and says so (owner decision Q6).

VOLUME is the stored cumulative call / put volume of the book at each 15-min reading (metrics, underlying scope),
differenced reading to reading. A reading with no value leaves a gap; the difference across a gap is not drawn as
one interval's volume.
"""
from __future__ import annotations
import sqlite3
from pathlib import Path
from ..implied_vol import REASONS


def _ro(path):return sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=10)


def greeks(screener,underlying,expiry=None,atm=3):
 if screener is None:return {'available':False,'text':'The screener engine is not running on this server.'}
 s=screener.sessions.get()
 if s is None or not s.n:return {'available':False,'text':'No F&O readings are stored yet.'}
 ex=s.expiries(underlying)
 if not ex:return {'available':False,'text':f'{underlying} has no options in the capture scope.'}
 note=None
 if expiry and expiry in ex:chosen=expiry
 else:
  chosen=ex[0]
  if chosen<=s.session and len(ex)>1:
   chosen=ex[1];note=(f'{underlying} options expire today, and implied volatility cannot be solved on expiry day, '
    f'so this reads the next expiry ({chosen}).')
 book=s.books[(underlying,chosen)]
 i=s.n-1
 band=s.band(book,i,{'kind':'atm','below':atm,'above':atm})
 screener.greeks.ensure(s,{i:set(band)})
 rows={}
 for t,off in band.items():
  c=s.contracts[t]
  r=screener.greeks.value(s.session,t,i) or (None,None,None,None)
  row=rows.setdefault(c.strike,{'strike':c.strike,'offset':off,'CE':None,'PE':None})
  row[c.type]={'iv_pct':r[0],'delta':r[2],'gamma':r[3],'price':None if c.price[i]!=c.price[i] else c.price[i],
   'reason':REASONS.get(r[1]) if r[1] else None}
 atm_strike=s.atm(book,i)
 return {'available':True,'underlying':underlying,'expiry':chosen,'as_of':s.readings[i],'atm_strike':atm_strike,
  'spot':s.spot_at(book,i),'rows':sorted(rows.values(),key=lambda r:r['strike']),'note':note,'computed':True,
  'computed_text':'IV, delta and gamma are COMPUTED with a Black-Scholes-Merton model from each option\'s own '
   'traded price — they are not exchange figures.'}


def volume(derivatives_path,underlying,expiry=None):
 if not Path(derivatives_path).is_file():return {'available':False,'text':'No F&O store is attached to this server.'}
 c=_ro(derivatives_path)
 try:
  cols={r[1] for r in c.execute('pragma table_info(metrics)')}
  if not {'total_ce_volume','total_pe_volume'}<=cols:
   return {'available':False,'underlying':underlying,'text':'This store does not carry book volume yet.'}
  head=c.execute("select max(captured_at) from metrics where scope='underlying' and underlying=?",(underlying,)).fetchone()
  if not head or not head[0]:return {'available':False,'text':f'Nothing has been captured for {underlying} yet.'}
  session=head[0][:10]
  if not expiry:
   row=c.execute("select min(expiry) from metrics where scope='underlying' and underlying=? and captured_at>=?",
    (underlying,f'{session} ')).fetchone()
   expiry=row[0] if row else None
  rows=c.execute("select captured_at,total_ce_volume,total_pe_volume from metrics where scope='underlying' and metric_key=?"
   " and captured_at>=? and captured_at<=? order by captured_at",(f'{underlying}|{expiry}',f'{session} 09:00',f'{session} 15:30:59')).fetchall()
 finally:c.close()
 points=[];prev=None
 for at,ce,pe in rows:
  if prev and ce is not None and pe is not None and prev[1] is not None and prev[2] is not None:
   points.append({'at':at[11:16],'ce':max(0.0,ce-prev[1]),'pe':max(0.0,pe-prev[2])})
  elif prev is None:
   points.append({'at':at[11:16],'ce':ce,'pe':pe,'opening':True})
  else:points.append({'at':at[11:16],'ce':None,'pe':None,'gap':True})
  prev=(at,ce,pe)
 last=rows[-1] if rows else None
 return {'available':bool(points),'underlying':underlying,'expiry':expiry,'session':session,
  'as_of':last[0] if last else None,'points':points,
  'day_ce':last[1] if last else None,'day_pe':last[2] if last else None,
  'text':None if points else f'No book volume captured for {underlying} this session.',
  'definition':'Contracts traded across every strike of this expiry in each 15-min interval (the change in the '
   'day\'s cumulative volume between two readings). The first bar is the day\'s volume up to the first reading.'}
