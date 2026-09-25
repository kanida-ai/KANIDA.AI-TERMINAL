"""Verified F&O calendar and point-in-time membership from NSE's own daily bhavcopy archive (slice 12, A2/A3).

db/fo_bhavcopy.db is loaded by market_data/bhavcopy (official NSE F&O bhavcopies, 2016 onwards). From it, for one
underlying, this module answers two point-in-time questions for any trading day D:
  * which option expiries were LISTED on D (the Lab picks its expiry from these - no derived calendar rule), and
  * whether the underlying had listed options on D at all (a stock is only tradable on days it was in F&O).
A day the archive does not cover (outside its fetched range, or a day that failed to load) is reported as 'unknown'
and the caller falls back to its derived calendar, counted and labelled - never silently treated as verified.
READ-ONLY.
"""
from __future__ import annotations
import os,sqlite3,threading,time
from pathlib import Path

DEFAULT=Path(__file__).resolve().parents[4]/'db'/'fo_bhavcopy.db'
# NSE's official symbol-change list (old symbol, new symbol, date). kanida.db keeps a renamed stock's whole history under
# its NEW symbol while the bhavcopy keeps the OLD name until the change (quant audit C2: MCDOWELL-N -> UNITDSPR). Optional:
# without it, a renamed stock's pre-change F&O history is simply not found (stated in the run), never invented.
SYMBOL_CHANGES=Path(__file__).with_name('data')/'nse_symbol_changes.csv'


class FoCalendar:
 def __init__(self,path=None,changes=None):
  self.path=str(path or os.getenv('PILOT_FO_BHAVCOPY_DB') or DEFAULT);self.lock=threading.Lock();self._mem={};self._cov=None
  self.old_names={}                     # new symbol -> [(old symbol, change date)]
  cp=Path(changes) if changes else SYMBOL_CHANGES
  try:
   import csv
   for row in csv.reader(open(cp,encoding='utf-8',errors='replace')):
    if len(row)<4 or not row[1].strip() or row[1].strip().upper()=='SM_KEY_SYMBOL':continue
    old,new,when=row[1].strip().upper(),row[2].strip().upper(),row[3].strip()
    try:from datetime import datetime as _dt;when=_dt.strptime(when,'%d-%b-%Y').date().isoformat()
    except ValueError:continue
    self.old_names.setdefault(new,[]).append((old,when))
  except OSError:pass
  self.changes_loaded=bool(self.old_names)

 def names(self,symbol:str):
  """(symbol, [(old name, used before date)]) - every archive name this stock traded under."""
  return symbol,self.old_names.get(symbol,[])

 def _conn(self):
  return sqlite3.connect(f'file:{self.path}?mode=ro',uri=True,timeout=20)

 def available(self)->bool:
  try:
   c=self._conn();ok=c.execute("select 1 from fetch_log where status='ok' limit 1").fetchone();c.close();return bool(ok)
  except sqlite3.Error:return False

 def covered_days(self):
  """The set of trading days whose bhavcopy loaded OK (cached 10 minutes)."""
  if self._cov and time.time()-self._cov[0]<600:return self._cov[1]
  try:
   c=self._conn();days={r[0] for r in c.execute("select trade_date from fetch_log where status='ok'")};c.close()
  except sqlite3.Error:days=set()
  self._cov=(time.time(),days);return days

 def listed(self,symbol:str):
  """{trade_date: sorted expiries with listed options} for one underlying (cached 10 minutes)."""
  hit=self._mem.get(symbol)
  if hit and time.time()-hit[0]<600:return hit[1]
  out={}
  try:
   c=self._conn()
   for d,e in c.execute("select distinct trade_date,expiry from fo_daily where symbol=? and instrument in ('OPTIDX','OPTSTK') order by trade_date,expiry",(symbol,)):
    out.setdefault(d,[]).append(e)
   for old,until in self.old_names.get(symbol,[]):            # history under an earlier name, only before the change
    for d,e in c.execute("select distinct trade_date,expiry from fo_daily where symbol=? and instrument in ('OPTIDX','OPTSTK') and trade_date<? order by trade_date,expiry",(old,until)):
     if e not in out.setdefault(d,[]):out[d].append(e)
   for d in out:out[d].sort()
   c.close()
  except sqlite3.Error:out={}
  with self.lock:self._mem[symbol]=(time.time(),out)
  return out

 def lookup(self,symbol:str):
  """A picklable {'listed':..., 'covered':...} bundle for a simulation (or a batch worker)."""
  # one consistent snapshot: listing and coverage read together, so a day being loaded mid-fetch is never "covered with
  # no listing" (a false not_in_fo - quant audit)
  self._mem.pop(symbol,None);self._cov=None
  covered=self.covered_days()          # FIRST: a day's rows and its 'ok' log commit together, so every day covered now
  listed=self.listed(symbol)           # is fully present in the listing read after it
  return {'listed':listed,'covered':covered,'source':'NSE F&O bhavcopy (db/fo_bhavcopy.db)','changes_loaded':self.changes_loaded}

 def lot_size(self,symbol:str):
  """The most recent lot size NSE recorded for this stock's options (UDiFF files carry it; older files do not)."""
  try:
   c=self._conn();r=c.execute("select lot_size,trade_date from fo_daily where symbol=? and instrument in ('OPTSTK','OPTIDX') and lot_size is not null order by trade_date desc limit 1",(symbol,)).fetchone();c.close()
  except sqlite3.Error:r=None
  return (int(r[0]),r[1]) if r else None

 def futures_move(self,symbol:str,d0:str,d1:str):
  """The settle-to-settle move of the SAME futures contract (nearest expiry listed on both days) from d0 to d1, or
  None. Used to tell a genuine market move from a bad row in adjusted price data."""
  try:
   c=self._conn()
   r=c.execute("""select a.settle,b.settle from fo_daily a join fo_daily b on b.symbol=a.symbol and b.expiry=a.expiry and b.instrument=a.instrument
     where a.symbol=? and a.instrument in ('FUTSTK','FUTIDX') and a.trade_date=? and b.trade_date=? and a.settle>0 and b.settle>0 order by a.expiry limit 1""",(symbol,d0,d1)).fetchone()
   c.close()
  except sqlite3.Error:r=None
  return (r[1]/r[0]-1) if r else None

 def ever_members(self,start:str,end:str):
  """Every stock that had listed options on ANY day in [start, end] - the point-in-time universe (includes names that
  later LEFT F&O, which today's list misses)."""
  try:
   c=self._conn();out={r[0] for r in c.execute("select distinct symbol from fo_daily where instrument='OPTSTK' and trade_date between ? and ?",(start,end))};c.close()
  except sqlite3.Error:out=set()
  # an old name maps to the stock's CURRENT symbol (the one kanida.db stores its history under)
  cur={old:new for new,olds in self.old_names.items() for old,_ in olds}
  return {cur.get(x,x) for x in out}
