"""Exchange time and the NSE F&O session calendar - ONE place for "is the market open" (GTM audit P09).

  * Time is exchange time: Asia/Kolkata, whatever the host's timezone. Nothing here reads host-local now().
  * Holidays come from a versioned file (data/nse_fo_holidays.json, NSE holiday master, segment FO). A year the file
    does not cover is NOT assumed open: the gate reports HOLIDAY_LIST_MISSING and executable actions stay blocked.
  * Special sessions (Diwali Muhurat) are announced separately; they are treated as closed, never guessed open.
"""
from __future__ import annotations
import json
from datetime import date,datetime,timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

IST=ZoneInfo('Asia/Kolkata')
OPEN=(9,15);CLOSE=(15,30)
_FILE=Path(__file__).with_name('data')/'nse_fo_holidays.json'
_CAL=None


def now_ist():
 """Exchange-local wall time as a naive datetime (the convention every stored IST text in this package uses)."""
 return datetime.now(IST).replace(tzinfo=None)


def calendar():
 global _CAL
 if _CAL is None:
  try:raw=json.loads(_FILE.read_text())
  except (OSError,ValueError):raw={'version':None,'years':[],'holidays':[]}
  _CAL={'version':raw.get('version'),'source':raw.get('source'),'years':set(raw.get('years') or []),
   'holidays':{h['date']:h['description'] for h in raw.get('holidays') or []},'note':raw.get('note')}
 return _CAL


def day_status(d):
 """('trading'|'weekend'|'holiday'|'unknown', description)."""
 d=d.date() if isinstance(d,datetime) else d
 cal=calendar()
 if d.weekday()>=5:return 'weekend',d.strftime('%A')
 if d.year not in cal['years']:return 'unknown',f'No NSE holiday list is loaded for {d.year}'
 h=cal['holidays'].get(d.isoformat())
 return ('holiday',h) if h else ('trading',None)


def session(at=None):
 """The session gate: {open, status, reason, calendar_version}. `at` is naive IST (default: now)."""
 at=at or now_ist()
 st,desc=day_status(at)
 base={'calendar_version':calendar()['version'],'at':at.strftime('%Y-%m-%d %H:%M:%S')}
 if st=='weekend':return {**base,'open':False,'status':'closed','reason':f'Weekend ({desc})'}
 if st=='holiday':return {**base,'open':False,'status':'holiday','reason':f'NSE holiday: {desc}'}
 if st=='unknown':return {**base,'open':False,'status':'HOLIDAY_LIST_MISSING','reason':desc+' - executable actions stay blocked.'}
 hm=(at.hour,at.minute)
 if hm<OPEN:return {**base,'open':False,'status':'pre_open','reason':'Before 09:15 IST'}
 if hm>=CLOSE:return {**base,'open':False,'status':'closed','reason':'After 15:30 IST'}
 return {**base,'open':True,'status':'open','reason':'Regular session 09:15-15:30 IST'}


def market_open(at=None):return session(at)['open']


def is_trading_day(d):return day_status(d)[0]=='trading'


def add_trading_days(d,n):
 """Step n trading days from d (n may be negative). Unknown-year days count as trading (research date stepping only)."""
 d=d.date() if isinstance(d,datetime) else d
 step=1 if n>=0 else -1;left=abs(n)
 while left:
  d+=timedelta(days=step)
  if day_status(d)[0] in ('trading','unknown'):left-=1
 return d


def coverage(at=None,warn_days=45):
 """How long the loaded holiday list lasts: {covers_until, days_left, warning}. After it, executable actions block."""
 at=at or now_ist();years=calendar()['years']
 if not years:return {'covers_until':None,'days_left':0,'warning':'No NSE holiday list is loaded - order review and alerts stay blocked.'}
 end=date(max(years),12,31);left=(end-at.date()).days
 return {'covers_until':end.isoformat(),'days_left':left,
  'warning':None if left>warn_days else (f'The NSE holiday list ends {end:%d %b %Y} ({max(left,0)} days). Load the next year\'s list, or order review and alerts will block from then.')}


def public():
 cal=calendar()
 return {'version':cal['version'],'source':cal['source'],'years':sorted(cal['years']),
  'holidays':[{'date':k,'description':v} for k,v in sorted(cal['holidays'].items())],'note':cal['note'],
  'session':{'open':'09:15','close':'15:30','timezone':'Asia/Kolkata'}}
