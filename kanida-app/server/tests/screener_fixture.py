"""A tiny db/derivatives.db for the screener tests — the columns the screener reads, and nothing it doesn't.

The real store belongs to the capture workers and is never read by a test. Each test builds the readings it
needs: a session, a list of reading times, books (underlying + expiry) and contracts, each with a per-reading
series where None means "no row at this reading".
"""
from __future__ import annotations
import sqlite3

METRICS='''create table metrics(scope text not null, metric_key text not null, captured_at text not null,
 instrument_token integer, tradingsymbol text, underlying text not null, instrument_type text, strike real,
 expiry text, lot_size integer, days_to_expiry integer, last_price real, oi real, oi_change_day real, volume real,
 spot real, unusual integer not null default 0, floors_passed integer, vol_tod_ratio real, pcr_oi real,
 max_pain_strike real, total_ce_oi real, total_pe_oi real, total_ce_volume real, total_pe_volume real,
 primary key(scope, metric_key, captured_at))'''
SNAPSHOTS='''create table snapshots(instrument_token integer not null, captured_at text not null, last_trade_time text,
 primary key(instrument_token, captured_at))'''


def build(path,session,times,books,contracts):
 """books: {(underlying, expiry): {'spot': [...], 'pcr': [...], 'maxpain': [...], 'dte': int}}
 contracts: [{'token', 'underlying', 'expiry', 'type', 'strike', 'price': [...], 'oid': [...], 'oi': [...],
  'unusual': [...], 'floors': [...], 'vtr': [...], 'dte': int}] — every series is one value per reading."""
 c=sqlite3.connect(path)
 c.execute(METRICS);c.execute(SNAPSHOTS)
 for i,t in enumerate(times):
  at=f'{session} {t}:00'
  for (und,exp),b in books.items():
   spot=b['spot'][i]
   if spot is None and b.get('skip_missing',True):continue
   c.execute('insert into metrics(scope,metric_key,captured_at,underlying,expiry,days_to_expiry,spot,pcr_oi,max_pain_strike,'
    'total_ce_volume,total_pe_volume) values(?,?,?,?,?,?,?,?,?,?,?)',('underlying',f'{und}|{exp}',at,und,exp,b.get('dte',7),spot,
    (b.get('pcr') or [None]*len(times))[i],(b.get('maxpain') or [None]*len(times))[i],
    (b.get('ce_volume') or [None]*len(times))[i],(b.get('pe_volume') or [None]*len(times))[i]))
  for k in contracts:
   price=k['price'][i]
   if price is None and (k.get('oid') or [None]*len(times))[i] is None:continue
   spot=books[(k['underlying'],k['expiry'])]['spot'][i]
   symbol=f"{k['underlying']}{k['expiry']}{int(k['strike'])}{k['type']}"
   oid=(k.get('oid') or [None]*len(times))[i]
   c.execute('insert into metrics(scope,metric_key,captured_at,instrument_token,tradingsymbol,underlying,instrument_type,'
    'strike,expiry,lot_size,days_to_expiry,last_price,oi,oi_change_day,spot,unusual,floors_passed,vol_tod_ratio)'
    ' values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
    ('contract',symbol,at,k['token'],symbol,k['underlying'],k['type'],k['strike'],k['expiry'],50,k.get('dte',7),price,
     (k.get('oi') or [None]*len(times))[i] if k.get('oi') else (None if oid is None else 1_000_000+oid),oid,spot,
     (k.get('unusual') or [0]*len(times))[i],(k.get('floors') or [1]*len(times))[i],(k.get('vtr') or [None]*len(times))[i]))
   c.execute('insert into snapshots values(?,?,?)',(k['token'],at,at))
 c.commit();c.close()
 return path


def ladder(underlying,expiry,strikes,n,series=None,types=('CE','PE'),start=1000,dte=7):
 """One contract per (strike, type), flat by default (premium 100, ΔOI 0 at every reading); override any series
 per (strike, type): series={(24500, 'CE'): {'oid': [...], 'price': [...]}}."""
 out=[];token=start
 for s in strikes:
  for t in types:
   token+=1
   k={'token':token,'underlying':underlying,'expiry':expiry,'type':t,'strike':float(s),'dte':dte,
    'price':[100.0]*n,'oid':[0.0]*n}
   k.update((series or {}).get((s,t),{}))
   out.append(k)
 return out
