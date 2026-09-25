"""Slice 6 — the Lab: REPLAY of exact contracts and a MODEL-PRICED RULE BACKTEST. (Quant rules: ~/.claude/rules/quant.md.)

Two different questions, never mixed (blueprint B §12):
  replay     How did THESE contracts behave? Real traded prices only - Kite historical candles for listed contracts,
             else the captured 15-minute candles in db/derivatives.db. A bar missing for any leg is SKIPPED and counted;
             nothing is forward-filled.
  backtest   Would this RULE have made money? No NSE option history deeper than a few weeks exists on this machine, so
             option prices are MODELLED: Black-Scholes on the NIFTY 50 daily series with India VIX as the volatility.
             Every result says 'Model-priced - not traded prices' and can reach at most a 'Model-tested' badge.

Point in time (the law):
  * the decision is made at a day's CLOSE with that day's close and VIX; entry is the NEXT trading day's OPEN;
  * exits are checked at each close and filled at the next open; an expiry settles at its close, intrinsic only;
  * strikes and the expiry are chosen from what was known at entry (spot at the open, VIX of the decision day);
  * a trade counts only once it has CLOSED within the data (available_from <= last day); open trades are excluded
    and counted; one position at a time.
Costs: every fill pays the F&O charges estimate plus slippage (a fraction of the model price, never below a tick,
against you). Ranking statistic: expectancy per trade and per Rs 100 of capital at risk, with a bootstrap 95% CI -
never win rate. Discovery / out-of-sample split by date; the badge reads the OUT-OF-SAMPLE lower bound only.

Scope v1: NIFTY only (its weekly expiry calendar is derived below and stated; BANKNIFTY/FINNIFTY weeklies were
discontinued in Nov 2024 and need their own calendars). Rupee amounts use today's lot size, stated on the result.
"""
from __future__ import annotations
import json,math,random,sqlite3,threading,time,uuid,logging
from datetime import date,datetime,timedelta
from . import analytics as A
from . import charges as CH
from .templates import BY_KEY
from . import adjust as ADJ
from .. import implied_vol as IV

log=logging.getLogger('strategy_builder.lab')
MODEL='lab-bsm-vix-v1'
STEP={'NIFTY':50.0}
INDEX_SYMBOL={'NIFTY':'NIFTY 50'}
KITE_INDEX_TOKEN={'NIFTY 50':256265,'INDIA VIX':264969}
INDICES={'NIFTY','BANKNIFTY','FINNIFTY','MIDCPNIFTY','NIFTYNXT50'}
STOCK_EXIT_DTE=2          # stock options are PHYSICALLY settled: every stock rule exits at the open of the day before expiry or earlier
STOCK_MIN_SLIP=0.01       # stock option spreads are wider than NIFTY's: at least 1% of the model price per fill
STOCK_FROM='2016-01-01'
MONTHLY_TUESDAY_FROM=date(2025,9,1)   # NSE equity-derivative expiries moved to Tuesday (monthly: last Tuesday) from Sep 2025
TUESDAY_FROM=date(2025,9,1)       # NSE moved NIFTY weekly expiry from Thursday to Tuesday (derived calendar, stated)
WEEKLY_FROM=date(2019,2,11)       # NIFTY weekly options began 11 Feb 2019; before that only the monthly (last Thursday) existed
SHORT_SESSION_BARS=300            # a day with fewer 1-minute NIFTY bars than this is a special/short session (Muhurat etc.)
BOOT=2000;RANDOM_REPS=40
UNBOUNDED_PROFIT={'long_call','long_put','long_straddle','long_strangle'}   # no capped max profit to take a % of
SCHEMA='''
create table if not exists lab_runs(
 id text primary key, user_id text not null, strategy_id text, kind text not null, spec text not null, status text not null,
 progress real not null default 0, result text, error text, created_at real not null, finished_at real);
create index if not exists ix_lab_user on lab_runs(user_id, created_at);
create table if not exists lab_evidence(run_id text primary key, entry text not null);
create table if not exists lab_daily(symbol text not null, day text not null, open real, high real, low real, close real, source text not null,
 primary key(symbol, day));
'''


class LabError(Exception):
 def __init__(self,status,code,message):super().__init__(message);self.status=status;self.code=code;self.message=message


# --- data --------------------------------------------------------------------------------------------------------------
class Daily:
 """NIFTY 50 and INDIA VIX daily bars: db/kanida.db (read-only) + a Kite top-up for the days after it, cached locally."""
 def __init__(self,kanida_db,store,market):
  self.kanida_db=kanida_db;self.store=store;self.market=market;self._mem={}
 def _kanida(self,symbol):
  try:
   c=sqlite3.connect(f'file:{self.kanida_db}?mode=ro',uri=True,timeout=20)
   rows=c.execute('select substr(bar_time,1,10),open,high,low,close from ohlc_daily where symbol=? order by bar_time',(symbol,)).fetchall();c.close()
  except sqlite3.Error:rows=[]
  cutoff=_last_complete_day()
  return [r for r in rows if r[0]<=cutoff]        # never a partial bar for a session still running (IST)
 def special_sessions(self):
  """Days that are not normal sessions: weekend dates, and days with a short 1-minute record (Muhurat, special
  Saturdays, the Feb-2021 outage). Excluded from decisions, entries, marks and exits."""
  if 'special' in self._mem:return self._mem['special'][1]
  out=set()
  try:
   c=sqlite3.connect(f'file:{self.kanida_db}?mode=ro',uri=True,timeout=20)
   out={r[0] for r in c.execute("select substr(bar_time,1,10) d,count(*) n from ohlc_1min where symbol='NIFTY 50' group by d having n<?",(SHORT_SESSION_BARS,))}
   c.close()
  except sqlite3.Error:pass
  self._mem['special']=(time.time(),out)
  return out
 def _topup(self,symbol,after):
  live=getattr(self.market,'live_market',None)
  if not live or symbol not in KITE_INDEX_TOKEN or self.store is None:return []     # stocks: kanida.db only (it is corporate-action adjusted; Kite bars may not be)
  ok,_=live.available()
  if not ok:return []
  today=datetime.fromisoformat(_now_ist().strftime('%Y-%m-%d'))
  start=(datetime.strptime(after,'%Y-%m-%d')+timedelta(days=1)).strftime('%Y-%m-%d')
  if start>=today.strftime('%Y-%m-%d'):return []
  try:
   r=live._get(f"/instruments/historical/{KITE_INDEX_TOKEN[symbol]}/day",**{'from':start+' 00:00:00','to':(today-timedelta(days=1)).strftime('%Y-%m-%d')+' 23:59:59'})
   bars=[(b[0][:10],b[1],b[2],b[3],b[4]) for b in r.json()['data']['candles']]
  except Exception as e:  # noqa: BLE001 - a missing top-up is stated as coverage, never invented
   log.warning('lab: Kite top-up for %s failed (%s)',symbol,type(e).__name__);return []
  with self.store.lock:
   for b in bars:self.store.c.execute('insert or replace into lab_daily values(?,?,?,?,?,?,?)',(symbol,)+b+('kite_historical',))
   self.store.c.commit()
  return bars
 def series(self,symbol):
  if symbol in self._mem and time.time()-self._mem[symbol][0]<3600:return self._mem[symbol][1]
  base=self._kanida(symbol)
  last=base[-1][0] if base else '2012-12-31'
  cached=[]
  if self.store is not None:                                   # a batch worker has no store: kanida.db only
   with self.store.lock:
    cached=self.store.c.execute('select day,open,high,low,close from lab_daily where symbol=? and day>? order by day',(symbol,last)).fetchall()
  cached=[tuple(r) for r in cached]
  newest=cached[-1][0] if cached else last
  extra=self._topup(symbol,newest)
  rows=[(d,o,h,l,c,'kanida.db') for d,o,h,l,c in base]+[r+('kite_historical',) for r in cached]+[r+('kite_historical',) for r in extra if r[0]>newest]
  out={'days':[r[0] for r in rows],'open':[r[1] for r in rows],'close':[r[4] for r in rows],'high':[r[2] for r in rows],'low':[r[3] for r in rows],
   'sources':{'kanida.db':(base[0][0],base[-1][0],len(base)) if base else None,
    'kite_historical':((cached+extra)[0][0],(cached+extra)[-1][0],len(cached)+len([r for r in extra if r[0]>newest])) if (cached or extra) else None}}
  self._mem[symbol]=(time.time(),out)
  return out


def _now_ist():
 from zoneinfo import ZoneInfo
 return datetime.now(ZoneInfo('Asia/Kolkata')).replace(tzinfo=None)


def _last_complete_day():
 """Today counts only after the close (15:45 IST buffer); before that the newest complete session is yesterday."""
 now=_now_ist()
 return now.strftime('%Y-%m-%d') if (now.hour,now.minute)>=(15,45) else (now-timedelta(days=1)).strftime('%Y-%m-%d')


def clean_series(series,special):
 """Drop special sessions and weekend dates from a daily series (keeps the lists aligned)."""
 keep=[i for i,d in enumerate(series['days']) if d not in special and date.fromisoformat(d).weekday()<5]
 return {**series,**{k:[series[k][i] for i in keep] for k in ('days','open','high','low','close')},'excluded_special':len(series['days'])-len(keep)}


def weekly_expiries(days):
 """NIFTY expiries over the trading days given: MONTHLY only (last Thursday) before weekly options began on
 2019-02-11; weekly Thursday from then; weekly Tuesday from 2025-09-01. An expiry on a non-trading day moves to the
 previous trading day. DERIVED, not read from an exchange archive - stated on every result."""
 have=set(days);out=[]
 d0=date.fromisoformat(days[0]);d1=date.fromisoformat(days[-1])+timedelta(days=21)
 d=d0
 while d<=d1:
  wd=1 if d>=TUESDAY_FROM else 3
  last_thursday=d.weekday()==3 and (d+timedelta(days=7)).month!=d.month
  if d.weekday()==wd and (d>=WEEKLY_FROM or last_thursday):
   e=d
   if e.isoformat()<=days[-1]:
    while e.isoformat() not in have and e>d0:e-=timedelta(days=1)
   out.append(e.isoformat())
  d+=timedelta(days=1)
 return sorted(set(out))


def monthly_expiries(days):
 """Stock option expiries: the LAST Thursday of each month until 2025-08-31, the last Tuesday from 2025-09-01; an expiry on
 a non-trading day moves to the previous trading day. DERIVED from the stated rule, not read from an exchange archive."""
 have=set(days);out=[]
 d0=date.fromisoformat(days[0]).replace(day=1);end=date.fromisoformat(days[-1])+timedelta(days=45)
 m=d0
 while m<=end:
  nxt=(m.replace(day=28)+timedelta(days=4)).replace(day=1)
  last=nxt-timedelta(days=1)
  wd=1 if last>=MONTHLY_TUESDAY_FROM else 3
  x=last
  while x.weekday()!=wd:x-=timedelta(days=1)
  k=x
  while k.isoformat() not in have and k>date.fromisoformat(days[0]) and k<=date.fromisoformat(days[-1]):k-=timedelta(days=1)
  out.append(k.isoformat() if k<=date.fromisoformat(days[-1]) else x.isoformat())
  m=nxt
 return sorted(set(out))


def stock_step(spot):
 """A strike interval of about 1% of spot on a 1/2.5/5 ladder - an APPROXIMATION of NSE's price-banded intervals (stated)."""
 for s_ in (1,2.5,5,10,20,50,100,250,500,1000):
  if s_>=spot*0.01:return float(s_)
 return 1000.0


def scaled_vol(u,nifty,vix,window=20):
 """Point-in-time volatility for a stock: its own trailing realised vol (20 closes up to and including the day) scaled
 by the index's implied/realised ratio that day (India VIX / NIFTY realised), clipped. A MODEL, stated on every result."""
 def rv(days,closes):
  out={};lr=[None]+[math.log(closes[i]/closes[i-1]) if closes[i-1] and closes[i] else None for i in range(1,len(closes))]
  for i in range(window,len(closes)):
   w=[x for x in lr[i-window+1:i+1] if x is not None]
   if len(w)>=window-2:
    m=sum(w)/len(w);out[days[i]]=math.sqrt(sum((x-m)**2 for x in w)/(len(w)-1))*math.sqrt(252)*100
  return out
 ru=rv(u['days'],u['close']);rn=rv(nifty['days'],nifty['close']);vx=dict(zip(vix['days'],vix['close']))
 days=[];vals=[]
 for d in u['days']:
  if d in ru and d in rn and d in vx and rn[d]>0:
   ratio=min(2.5,max(0.6,vx[d]/rn[d]))
   days.append(d);vals.append(round(min(150.0,max(8.0,ru[d]*ratio)),3))
 return {'days':days,'open':vals,'close':vals,'high':vals,'low':vals,'sources':{'model':'stock 20-day realised vol x (India VIX / NIFTY 20-day realised vol), clipped 0.6-2.5x, 8-150%'}}


# --- the rule backtest -------------------------------------------------------------------------------------------------
def _years(entry_day,expiry,at_open=True):
 start=datetime.fromisoformat(entry_day)+(timedelta(hours=9,minutes=15) if at_open else timedelta(hours=15,minutes=30))
 return max(0.0,(A.expiry_moment(expiry)-start).total_seconds()/A.SECONDS_PER_YEAR)


def _price(s,k,t,sigma,kind):
 return A.bs_price(s,k,t,sigma,kind) if t>0 else A.intrinsic(kind,k,s)


def _strikes(tpl,param,spot,sigma,t,step):
 atm=round(spot/step)*step;out=[]
 for spec in tpl['legs']:
  rule=spec['strike'];off=(rule['steps']+rule['per']*(param or 0))*step
  if rule['ref']=='atm':k=atm+off
  else:k=round(spot*math.exp(rule['sd']*sigma*math.sqrt(max(t,1/365)))/step)*step+off
  out.append({'side':spec['side'],'type':spec['type'],'strike':k,'mult':int(spec.get('mult',1))})
 return out


def _fill(model,side,opening,slip):
 px=max(0.05,model);move=max(0.05,px*slip)
 buying=(side=='B') if opening else (side=='S')
 return round(px+move if buying else max(0.05,px-move),2)


def _adjusted_walk(spec,legs,nifty,vmap,days,n,e,x,sig,lot_size,slip,step):
 """Walk one trade with ONE adjustment allowed (spec['adjust'] = {rule, k, trigger_pct}).

 Point in time: the trigger is read at a close (the tested short's distance to the money, as % of that close); the
 adjustment is filled at the NEXT open at model prices with slippage and charges on every order. Exits are exactly
 the baseline's (expiry settle, or exit_dte at the next open) - targets/stops are not allowed with an adjustment - so
 every adjusted trade has a baseline twin with the same entry and exit days (a paired comparison)."""
 adj=spec['adjust'];trigger=adj['trigger_pct']/100.0
 pos={(l['type'],l['strike']):l['units'] for l in legs}               # signed units per contract
 cash=-sum(l['units']*l['entry'] for l in legs);fees=0.0;info=None
 def generic():
  return [{'id':f"{t}{k:g}",'type':t,'side':'B' if u>0 else 'S','strike':k,'lots':abs(u)//lot_size,'include':True} for (t,k),u in pos.items() if u]
 j=e
 while True:
  cd=days[j] if j<n else None
  if cd is None:return None
  if cd>=x:
   s=nifty['close'][j]
   gross=cash+sum(u*A.intrinsic(t,k,s) for (t,k),u in pos.items())
   fees+=sum(0.00125*A.intrinsic(t,k,s)*u for (t,k),u in pos.items() if u>0 and A.intrinsic(t,k,s)>0)
   return {'exit':cd,'reason':'expiry','gross':gross,'fees':fees,'adjustment':info}
  cs=nifty['close'][j];cv=vmap.get(cd,sig)/100.0
  dte=(date.fromisoformat(x)-date.fromisoformat(cd)).days
  if spec.get('exit_dte') is not None and dte<=spec['exit_dte']:
   if j+1>=n:return None
   no=nifty['open'][j+1];tn=_years(days[j+1],x)
   for (t,k),u in pos.items():
    if not u:continue
    side='S' if u>0 else 'B';px=_fill(_price(no,k,tn,cv,t),'B' if u>0 else 'S',False,slip)
    cash+=u*px;fees+=CH.leg_charges(side,px,abs(u))['total']
   return {'exit':days[j+1],'reason':'time','gross':cash,'fees':fees,'adjustment':info}
  g=generic();tst=ADJ.tested(g,cs) if g else None
  if info is None and tst and tst[1]<=trigger and j+1<n and days[j+1]<=x:
   no=nifty['open'][j+1];tn=_years(days[j+1],x)
   grid=sorted({step*i for i in range(int(no*0.85/step),int(no*1.15/step)+2)}|{k for (_t,k) in pos})   # a realistic weekly strike range
   try:
    new,note=ADJ.apply(adj['rule'],g,no,grid,adj.get('k') or 1,tested_key=(tst[0]['type'],tst[0]['strike']))   # decided at the close
   except ADJ.NotApplicable as ex:
    info={'day':days[j+1],'trigger_day':cd,'applied':False,'reason':str(ex)}
   else:
    orders=ADJ.delta_orders(g,new);done=[]
    for o in orders:
     u=o['lots']*lot_size*(1 if o['side']=='B' else -1)
     model=_price(no,o['strike'],tn,cv,o['type']);px=max(0.05,model);mv=max(0.05,px*slip)
     px=round(px+mv if o['side']=='B' else max(0.05,px-mv),2)
     cash-=u*px;fees+=CH.leg_charges(o['side'],px,abs(u))['total']
     pos[(o['type'],o['strike'])]=pos.get((o['type'],o['strike']),0)+u
     done.append({'side':o['side'],'type':o['type'],'strike':o['strike'],'lots':o['lots'],'price':px})
    info={'day':days[j+1],'trigger_day':cd,'applied':True,'note':note,'distance_pct':round(tst[1]*100,2),'orders':done}
  j+=1


def simulate(spec,nifty,vix,lot_size,entry_days=None,rng=None):
 """Run the rule once. entry_days: a set of decision days to use instead of the schedule (the random control)."""
 tpl=BY_KEY[spec['template']];param=spec.get('param');slip=spec['slippage']
 stock=spec.get('underlying','NIFTY') not in INDICES
 step=None if stock else STEP['NIFTY']
 days=nifty['days'];vmap=dict(zip(vix['days'],vix['close']))
 expiries=monthly_expiries(days) if stock else weekly_expiries(days);last=days[-1]
 pos_of={d:i for i,d in enumerate(days)}
 lo=spec['from'];hi=spec['to']
 trades=[];skipped={'no_expiry':0,'no_vix':0,'open_at_end':0};i=0;n=len(days)
 def decision_ok(d):
  if entry_days is not None:return d in entry_days
  wd=spec['weekday'];return wd=='daily' or date.fromisoformat(d).weekday()==int(wd)
 while i<n-1:
  d=days[i]
  if d<lo or d>hi or not decision_ok(d):i+=1;continue
  sig=vmap.get(d)
  if not sig:skipped['no_vix']+=1;i+=1;continue
  e=i+1;ed=days[e];spot=nifty['open'][e]
  cands=[x for x in expiries if x>=ed and spec['dte_min']<=(date.fromisoformat(x)-date.fromisoformat(ed)).days<=spec['dte_max']]
  if not cands:skipped['no_expiry']+=1;i+=1;continue
  x=cands[0];t=_years(ed,x);sigma=sig/100.0
  legs=_strikes(tpl,param,spot,sigma,t,step or stock_step(spot))
  if len({(l['type'],l['strike'],l['side']) for l in legs})<len(legs):skipped['no_expiry']+=1;i+=1;continue
  for l in legs:
   l['units']=(1 if l['side']=='B' else -1)*lot_size*l.get('mult',1)
   l['entry']=_fill(_price(spot,l['strike'],t,sigma,l['type']),l['side'],True,slip)
  fees=sum(CH.leg_charges(l['side'],l['entry'],abs(l['units']))['total'] for l in legs)
  prof=A.expiry_profile([{**l,'lots':l.get('mult',1),'lot_size':lot_size,'price':l['entry']} for l in legs])
  max_loss=None if prof['unlimited_loss'] else -prof['max_loss'];max_profit=None if prof['unlimited_profit'] else prof['max_profit']
  if spec.get('adjust'):
   w=_adjusted_walk(spec,legs,nifty,vmap,days,n,e,x,sig,lot_size,slip,step)
   if w is None:skipped['open_at_end']+=1;break
   fees+=w['fees'];net=w['gross']-fees
   trades.append({'decision':d,'entry':ed,'exit':w['exit'],'expiry':x,'reason':w['reason'],'spot_entry':spot,'vix':sig,
    'legs':[{'side':l['side'],'type':l['type'],'strike':l['strike'],'entry':l['entry']} for l in legs],
    'gross':round(w['gross'],2),'fees':round(fees,2),'net':round(net,2),'capital_at_risk':round(max_loss,2) if max_loss else None,
    'hold_days':(date.fromisoformat(w['exit'])-date.fromisoformat(ed)).days,'adjustment':w['adjustment']})
   i=days.index(w['exit']) if w['exit'] in days else i+1
   continue
  # walk forward: check at each close, exit next open; settle at expiry close
  xi=pos_of.get(x)
  def sess_exit(jj):
   # stocks (physical settlement): exit at the open of the session BEFORE expiry, counted in trading sessions so a
   # weekend or holiday can never push the exit onto the expiry day itself
   left=(xi-jj) if xi is not None else (date.fromisoformat(x)-date.fromisoformat(days[jj])).days
   return left<=spec['exit_dte']
  j=e;exit_day=None;exit_px=None;reason=None
  while True:
   cd=days[j] if j<n else None
   if cd is None:break
   if cd>=x:                                       # expiry session: settle at its close, intrinsic
    s=nifty['close'][j];exit_day=cd;reason='expiry';exit_px=[A.intrinsic(l['type'],l['strike'],s) for l in legs];break
   cs=nifty['close'][j];cv=vmap.get(cd,sig)/100.0;tt=_years(cd,x,at_open=False)
   mark=sum(l['units']*(_price(cs,l['strike'],tt,cv,l['type'])-l['entry']) for l in legs)
   dte=(date.fromisoformat(x)-date.fromisoformat(cd)).days
   hit=None
   if spec.get('target_pct') and max_profit and mark>=max_profit*spec['target_pct']/100:hit='target'
   elif spec.get('stop_pct') and max_loss and mark<=-max_loss*spec['stop_pct']/100:hit='stop'
   elif spec.get('exit_dte') is not None and (sess_exit(j) if stock else dte<=spec['exit_dte']):hit='time'
   if hit:
    if j+1>=n:break                                # the next open is not in the data yet - trade still open
    nd=days[j+1];no=nifty['open'][j+1];tn=_years(nd,x)
    exit_day=nd;reason=hit;exit_px=[_fill(_price(no,l['strike'],tn,cv,l['type']),l['side'],False,slip) for l in legs];break
   j+=1
  if exit_day is None:skipped['open_at_end']+=1;break
  gross=sum(l['units']*(p-l['entry']) for l,p in zip(legs,exit_px))
  if reason!='expiry':fees+=sum(CH.leg_charges('S' if l['side']=='B' else 'B',p,abs(l['units']))['total'] for l,p in zip(legs,exit_px))
  else:fees+=sum(0.00125*p*abs(l['units']) for l,p in zip(legs,exit_px) if l['side']=='B' and p>0)   # STT on exercise of ITM longs
  net=gross-fees
  trades.append({'decision':d,'entry':ed,'exit':exit_day,'expiry':x,'reason':reason,'spot_entry':spot,'vix':sig,
   'legs':[{'side':l['side'],'type':l['type'],'strike':l['strike'],'entry':l['entry'],'exit':round(p,2)} for l,p in zip(legs,exit_px)],
   'gross':round(gross,2),'fees':round(fees,2),'net':round(net,2),'capital_at_risk':round(max_loss,2) if max_loss else None,
   'hold_days':(date.fromisoformat(exit_day)-date.fromisoformat(ed)).days})
  i=days.index(exit_day) if exit_day in days else j     # one position at a time: the next decision is on/after the exit
 return trades,skipped


def _stats(trades,rng):
 n=len(trades)
 if not n:return {'n':0}
 nets=[t['net'] for t in trades];mean=sum(nets)/n
 boot=sorted(sum(rng.choice(nets) for _ in range(n))/n for _ in range(BOOT))
 lo,hi=boot[int(0.025*BOOT)],boot[int(0.975*BOOT)-1]
 caps=[t for t in trades if t['capital_at_risk']]
 per100=(sum(t['net'] for t in caps)/sum(t['capital_at_risk'] for t in caps)*100) if caps else None
 eq=0;peak=0;dd=0
 for x in nets:eq+=x;peak=max(peak,eq);dd=min(dd,eq-peak)
 wins=[x for x in nets if x>0];losses=[x for x in nets if x<=0]
 return {'n':n,'expectancy':round(mean,2),'ci95':[round(lo,2),round(hi,2)],'per_100_capital':round(per100,3) if per100 is not None else None,
  'total':round(sum(nets),2),'max_drawdown':round(dd,2),'worst':round(min(nets),2),'best':round(max(nets),2),
  'win_rate':round(len(wins)/n*100,1),'avg_win':round(sum(wins)/len(wins),2) if wins else None,'avg_loss':round(sum(losses)/len(losses),2) if losses else None,
  'avg_hold_days':round(sum(t['hold_days'] for t in trades)/n,2)}


def _diff_stats(diffs,rng):
 n=len(diffs)
 if not n:return {'n':0}
 mean=sum(diffs)/n
 boot=sorted(sum(rng.choice(diffs) for _ in range(n))/n for _ in range(BOOT))
 return {'n':n,'mean':round(mean,2),'ci95':[round(boot[int(0.025*BOOT)],2),round(boot[int(0.975*BOOT)-1],2)],
  'helped':sum(1 for x in diffs if x>0),'hurt':sum(1 for x in diffs if x<0)}


def _adjustment_block(spec,trades,nifty,vix,lot_size,split,rng):
 """Paired: each adjusted trade vs its baseline twin (same rule without the adjustment, same entry and exit days).
 The improvement per TRIGGERED trade decides; untriggered trades are identical by construction (difference 0)."""
 base,_=simulate({**spec,'adjust':None},nifty,vix,lot_size)
 twin={b['entry']:b for b in base}
 pairs=[(t,twin[t['entry']]) for t in trades if t['entry'] in twin and twin[t['entry']]['exit']==t['exit']]
 unpaired=len(trades)-len(pairs)
 trig=[(t,b) for t,b in pairs if (t.get('adjustment') or {}).get('applied')]
 diff=lambda ps:[round(t['net']-b['net'],2) for t,b in ps]
 d_disc=[p for p in trig if p[0]['entry']<split];d_oos=[p for p in trig if p[0]['entry']>=split]
 out={'rule':spec['adjust'],'pairs':len(pairs),'unpaired':unpaired,'oos_diffs':diff(d_oos),
  'triggered':{'all':len(trig),'discovery':len(d_disc),'oos':len(d_oos)},
  'not_applicable':sum(1 for t,_b in pairs if (t.get('adjustment') or {}).get('applied') is False),
  'improvement_per_triggered_trade':{'all':_diff_stats(diff(trig),rng),'discovery':_diff_stats(diff(d_disc),rng),'oos':_diff_stats(diff(d_oos),rng)},
  'baseline':{'oos':_stats([b for t,b in pairs if t['entry']>=split],rng)}}
 o=out['improvement_per_triggered_trade']['oos']
 out['badge']=adjust_badge(diff(d_oos),1,rng)
 return out


def adjust_badge(oos_diffs,m,rng):
 """Badge from the out-of-sample paired improvement, Bonferroni-corrected for m runs tried (alpha = 5%/m)."""
 n=len(oos_diffs);caveat=' (model prices, one IV - no skew)'
 if n<30:return {'status':'insufficient','label':f"Adjustment model-tested - too few out-of-sample triggers (n={n}, need 30)",'tests':m}
 a=0.05/max(1,m)
 boot=sorted(sum(rng.choice(oos_diffs) for _ in range(n))/n for _ in range(BOOT))
 lo=boot[max(0,int(a/2*BOOT))];hi=boot[min(BOOT-1,int((1-a/2)*BOOT))]
 tag=f' - corrected for {m} runs tried' if m>1 else ''
 if lo>0:return {'status':'adjust_helped','label':f"Adjustment model-tested - improved out-of-sample results (low +₹{lo:,.0f} per triggered trade{tag}){caveat}",'ci':[round(lo,2),round(hi,2)],'tests':m}
 if hi<0:return {'status':'adjust_hurt','label':f"Adjustment model-tested - made out-of-sample results worse (high ₹{hi:,.0f} per triggered trade{tag}){caveat}",'ci':[round(lo,2),round(hi,2)],'tests':m}
 return {'status':'adjust_not_significant','label':f'Adjustment model-tested - no significant out-of-sample difference{tag}{caveat}','ci':[round(lo,2),round(hi,2)],'tests':m}


def backtest(spec,nifty,vix,lot_size,progress=lambda p:None):
 rng=random.Random(20260925)
 trades,skipped=simulate(spec,nifty,vix,lot_size);progress(0.4)
 split=spec['split']
 straddle=[t for t in trades if t['entry']<split<=t['exit']]          # uses out-of-sample prices: in neither set
 trades=[t for t in trades if t not in straddle];skipped['straddled_split']=len(straddle)
 disc=[t for t in trades if t['entry']<split];oos=[t for t in trades if t['entry']>=split]
 stats={'all':_stats(trades,rng),'discovery':_stats(disc,rng),'oos':_stats(oos,rng)};progress(0.55)
 # random-entry control: the same rule on randomly chosen decision days, same count, many repetitions
 pool=[d for d in nifty['days'] if spec['from']<=d<=spec['to']];means=[];oos_means=[]
 k=max(1,len(trades))
 for r in range(RANDOM_REPS):
  pick=set(rng.sample(pool,min(len(pool),k*3)))
  tr,_=simulate(spec,nifty,vix,lot_size,entry_days=pick)
  tr=[t for t in tr if not (t['entry']<split<=t['exit'])]
  sub=rng.sample(tr,min(k,len(tr))) if tr else []                  # a random subset across the WHOLE period
  if sub:means.append(sum(t['net'] for t in sub)/len(sub))
  oos_tr=[t for t in tr if t['entry']>=split]
  if oos_tr:
   osub=rng.sample(oos_tr,min(len(oos_tr),max(1,len(oos))))
   oos_means.append(sum(t['net'] for t in osub)/len(osub))
  progress(0.55+0.4*(r+1)/RANDOM_REPS)
 control={'reps':len(means),'mean_expectancy':round(sum(means)/len(means),2) if means else None,
  'actual_percentile':round(sum(1 for m in means if m<(stats['all'].get('expectancy') or 0))/len(means)*100,1) if means else None,
  'oos_mean_expectancy':round(sum(oos_means)/len(oos_means),2) if oos_means else None,
  'oos_actual_percentile':round(sum(1 for m in oos_means if m<(stats['oos'].get('expectancy') or 0))/len(oos_means)*100,1) if oos_means else None}
 adjustment=_adjustment_block(spec,trades,nifty,vix,lot_size,split,rng) if spec.get('adjust') else None
 if adjustment:        # capital at risk changes mid-trade when an adjustment applies; a per-₹100 figure would mislead
  for v in stats.values():
   if v.get('n'):v['per_100_capital']=None
 o=stats['oos']
 if o.get('n',0)<30:badge={'status':'insufficient','label':f"Model-tested - insufficient out-of-sample trades (n={o.get('n',0)}, need 30)"}
 elif o['ci95'][0]>0:badge={'status':'model_positive','label':f"Model-tested - out-of-sample expectancy positive (95% low ₹{o['ci95'][0]:,.0f}/trade)"}
 else:badge={'status':'model_not_significant','label':'Model-tested - out-of-sample expectancy not significantly positive'}
 eq=[];c=0
 for t in trades:c+=t['net'];eq.append({'day':t['exit'],'equity':round(c,2),'split':'oos' if t['entry']>=split else 'discovery'})
 return {'kind':'backtest','model':MODEL,'badge':badge,'stats':stats,'control':control,'adjustment':adjustment,'equity':eq,'trades':trades,'skipped':skipped,
  'lot_size':lot_size,'spec':spec,
  'provenance':{'price_source':'model','label':'Model-priced - not traded prices',
   'underlying':nifty['sources'],
   'volatility':({'series':'STOCK MODEL: its 20-day realised vol x (India VIX / NIFTY 20-day realised vol) that day, clipped - weaker than an implied-vol history','sources':vix['sources']}
    if spec.get('underlying','NIFTY') not in INDICES else {'series':'INDIA VIX close (decision day for entry, each close for marks)',**{k:v for k,v in vix['sources'].items()}}),
   'expiry_calendar':('Derived stock expiries: last Thursday monthly until 2025-08-31, last Tuesday from 2025-09-01; holidays move to the previous trading day'
    if spec.get('underlying','NIFTY') not in INDICES else 'Derived NIFTY expiries: monthly (last Thursday) only before 2019-02-11, weekly Thursday from then, weekly Tuesday from 2025-09-01; holidays move to the previous trading day'),
   'stock_caveats':(['Physically settled: every trade exits at the open of the trading session before expiry (counted in sessions) - never held into delivery',
    "Universe = today's F&O list (survivorship: stocks that left F&O are absent; a stock may not have had options in the early years)",
    'Strike interval approximated at ~1% of spot on a 1/2.5/5 ladder; today\'s lot size for every year',
    'Prices from kanida.db (corporate-action adjusted); no Kite top-up for stocks'] if spec.get('underlying','NIFTY') not in INDICES else None),
   'special_sessions_excluded':nifty.get('excluded_special',0),
   'assumptions':['Black-Scholes, European, no dividend, 6.5% constant rate; one IV (VIX) for every strike - no skew, so wings are mispriced',
    f"Slippage {spec['slippage']*100:.1f}% of the model price per fill (min 0.05), against you; F&O charges on every fill; STT on exercise of ITM longs at expiry",
    'Decision at the close, entry at the next open; exits checked at closes and filled at the next open; one position at a time',
    f'Rupee amounts use today\'s lot size ({lot_size}) for every year',
    'Closed trades only; trades still open at the end of the data are excluded and counted; a trade open across the out-of-sample boundary is in neither set',
    'Special sessions (Muhurat, special Saturdays, short days) and weekend dates are excluded from decisions, entries, marks and exits',
    'Today\'s STT rates are applied to every year (older years slightly pessimistic)']}}


# --- replay of exact contracts ----------------------------------------------------------------------------------------
def replay(legs,candles,entry_at=None):
 """legs: [{id,symbol,side,units,label}]; candles: {symbol: [(ts, close), ...]}. P&L path from the first common bar."""
 series={l['symbol']:dict(candles.get(l['symbol']) or []) for l in legs}
 stamps=sorted(set().union(*[set(s) for s in series.values()])) if series else []
 if entry_at:stamps=[t for t in stamps if t>=entry_at]
 full=[t for t in stamps if all(t in s for s in series.values())];skipped=len(stamps)-len(full)
 if not full:return {'kind':'replay','points':[],'skipped_bars':skipped,'coverage':{k:len(v) for k,v in series.items()}}
 t0=full[0];entry={l['symbol']:series[l['symbol']][t0] for l in legs}
 pts=[{'t':t,'pnl':round(sum(l['units']*(series[l['symbol']][t]-entry[l['symbol']]) for l in legs),2)} for t in full]
 vals=[p['pnl'] for p in pts]
 return {'kind':'replay','entry_at':t0,'entry_prices':entry,'points':pts,'skipped_bars':skipped,'last':vals[-1],'best':max(vals),'worst':min(vals),
  'coverage':{k:len(v) for k,v in series.items()},'note':'Real traded prices, gross of charges and slippage. Missing bars are skipped, never filled.'}


# --- runs (jobs) ------------------------------------------------------------------------------------------------------
class Lab:
 def __init__(self,store,market,kanida_db,derivatives_db):
  self.store=store;self.market=market;self.derivatives_db=derivatives_db;self.c=store.c;self.lock=store.lock
  with self.lock:self.c.executescript(SCHEMA);self.c.commit()
  self.daily=Daily(kanida_db,store,market)

 def _save(self,rid,**kw):
  with self.lock:
   sets=','.join(f'{k}=?' for k in kw)
   self.c.execute(f'update lab_runs set {sets} where id=?',(*[json.dumps(v) if k=='result' else v for k,v in kw.items()],rid));self.c.commit()

 def validate(self,raw):
  tpl=BY_KEY.get(str(raw.get('template') or ''))
  if not tpl:raise LabError(400,'TEMPLATE_NOT_FOUND','Choose a template to test.')
  u=str(raw.get('underlying') or 'NIFTY').upper()
  stock=u!='NIFTY'
  if stock and (u in INDICES or u not in self.stocks()):
   raise LabError(400,'UNSUPPORTED_UNDERLYING','The Lab backtests NIFTY and F&O stocks in this release. BANKNIFTY/FINNIFTY wait for a verified expiry calendar.')
  p=tpl['param'];param=raw.get('param',p['default'] if p else None)
  if p and param not in p['variants']:raise LabError(400,'FIELD_INVALID',f"{p['label']} must be one of {p['variants']}.")
  wd=raw.get('weekday',2)
  if wd!='daily':
   try:wd=int(wd)
   except (TypeError,ValueError):raise LabError(400,'FIELD_INVALID','weekday must be 0-4 or daily.')
   if not 0<=wd<=4:raise LabError(400,'FIELD_INVALID','weekday must be 0-4 (Mon-Fri) or daily.')
  def num(k,default,lo,hi,allow_none=False):
   v=raw.get(k,default)
   if v in (None,'') and allow_none:return None
   try:v=float(v)
   except (TypeError,ValueError):raise LabError(400,'FIELD_INVALID',f'{k} must be a number.')
   if not lo<=v<=hi:raise LabError(400,'FIELD_INVALID',f'{k} must be between {lo} and {hi}.')
   return v
  f=str(raw.get('from') or ('2016-01-01'));t=str(raw.get('to') or date.today().isoformat())
  for x in (f,t):
   try:date.fromisoformat(x)
   except ValueError:raise LabError(400,'FIELD_INVALID','from/to must be YYYY-MM-DD.')
  if f>=t:raise LabError(400,'FIELD_INVALID','from must be before to.')
  split=str(raw.get('split') or '')
  if split:
   try:date.fromisoformat(split)
   except ValueError:raise LabError(400,'FIELD_INVALID','split must be YYYY-MM-DD.')
   if not f<split<t:raise LabError(400,'FIELD_INVALID','split must fall inside the period.')
  else:split=(date.fromisoformat(f)+(date.fromisoformat(t)-date.fromisoformat(f))/2).isoformat()
  dmin=int(num('dte_min',1,0,60));dmax=int(num('dte_max',7,0,60))
  if raw.get('stop_pct') not in (None,'') and tpl['risk']!='defined':
   raise LabError(400,'STOP_UNDEFINED',f"{tpl['name']} has no defined maximum loss, so a stop at a % of it cannot be applied. Test it without a stop or choose a defined-risk structure.")
  if raw.get('target_pct') not in (None,'') and tpl['key'] in UNBOUNDED_PROFIT:
   raise LabError(400,'TARGET_UNDEFINED',f"{tpl['name']} has no capped maximum profit, so a target at a % of it cannot be applied.")
  if dmin>dmax:raise LabError(400,'FIELD_INVALID','dte_min must not exceed dte_max.')
  exit_dte=(int(num('exit_dte',None,0,30,True)) if raw.get('exit_dte') not in (None,'') else None)
  slip=max(0.005,num('slippage_pct',0.5,0.5,10)/100)
  if stock:
   if exit_dte is None:exit_dte=STOCK_EXIT_DTE
   if exit_dte<STOCK_EXIT_DTE:raise LabError(400,'PHYSICAL_SETTLEMENT',f'Stock options are physically settled: exit at least {STOCK_EXIT_DTE} days before expiry.')
   if dmin<=exit_dte:raise LabError(400,'FIELD_INVALID',f'Min days to expiry must be above the exit ({exit_dte}).')
   if slip<STOCK_MIN_SLIP:slip=STOCK_MIN_SLIP
   if raw.get('adjust') not in (None,'',{}):raise LabError(400,'NOT_SUPPORTED','Adjustment backtests are NIFTY-only in this release.')
  adj=None;ra=raw.get('adjust')
  if ra not in (None,'',{}):
   if not isinstance(ra,dict) or ra.get('rule') not in ADJ.RULES:
    raise LabError(400,'FIELD_INVALID','adjust.rule must be one of: '+', '.join(ADJ.RULES)+'.')
   if raw.get('target_pct') not in (None,'') or raw.get('stop_pct') not in (None,''):
    raise LabError(400,'ADJUST_WITH_EXITS','An adjustment is tested with hold-to-expiry or a time exit only, so each trade has an exact baseline twin. Remove the target/stop.')
   if ADJ.RULES[ra['rule']]['needs_short'] and not any(l['side']=='S' for l in tpl['legs']):
    raise LabError(400,'ADJUST_NOT_APPLICABLE',f"{tpl['name']} has no short leg, so '{ADJ.RULES[ra['rule']]['name']}' can never apply.")
   try:trig=float(ra.get('trigger_pct',0.5))
   except (TypeError,ValueError):raise LabError(400,'FIELD_INVALID','adjust.trigger_pct must be a number.')
   if not -5<=trig<=10:raise LabError(400,'FIELD_INVALID','adjust.trigger_pct must be between -5 and 10 (% of spot).')
   kk=None
   if ADJ.RULES[ra['rule']]['k']:
    try:kk=int(ra.get('k',1))
    except (TypeError,ValueError):raise LabError(400,'FIELD_INVALID','adjust.k must be a whole number.')
    if not 1<=kk<=10:raise LabError(400,'FIELD_INVALID','adjust.k must be 1-10 strikes.')
   adj={'rule':ra['rule'],'k':kk,'trigger_pct':trig}
  return {'adjust':adj,'underlying':u,'template':tpl['key'],'param':param,'weekday':wd,'dte_min':dmin,'dte_max':dmax,
   'target_pct':num('target_pct',None,1,100,True),'stop_pct':num('stop_pct',None,1,100,True),'exit_dte':exit_dte,
   'slippage':slip,'from':f,'to':t,'split':split}

 def lot_size(self,underlying='NIFTY'):
  try:
   ex=self.market.expiries(underlying)['expiries']
   return int(ex[0]['lot_size']) if ex else 65
  except Exception:return 65  # noqa: BLE001

 def start_backtest(self,user_id,raw,strategy_id=None):
  spec=self.validate(raw);rid=uuid.uuid4().hex[:16]
  with self.lock:
   self.c.execute('insert into lab_runs values(?,?,?,?,?,?,?,?,?,?,?)',(rid,user_id,strategy_id,'backtest',json.dumps(spec),'running',0,None,None,time.time(),None));self.c.commit()
  def work():
   try:
    nifty,vix=self.series_for(spec.get('underlying','NIFTY'))
    res=backtest(spec,nifty,vix,self.lot_size(spec.get('underlying','NIFTY')),progress=lambda p:self._save(rid,progress=round(p,2)))
    self._save(rid,status='completed',progress=1.0,result=res,finished_at=time.time())
   except Exception as e:  # noqa: BLE001 - a failed run is a state with its reason, never a partial 'result'
    log.exception('lab run failed');self._save(rid,status='failed',error=getattr(e,'message',None) or type(e).__name__,finished_at=time.time())
  threading.Thread(target=work,daemon=True,name=f'lab-{rid}').start()
  return self.run(user_id,rid)

 def series_for(self,underlying):
  """(underlying daily series, volatility series in VIX points) - both special-session cleaned, point in time."""
  special=self.daily.special_sessions()
  nifty=clean_series(self.daily.series('NIFTY 50'),special);vix=clean_series(self.daily.series('INDIA VIX'),special)
  if not nifty['days']:raise LabError(503,'NO_HISTORY','No NIFTY 50 daily history is readable on this machine.')
  if underlying=='NIFTY':return nifty,vix
  s=clean_series(self.daily.series(underlying),special)
  if len(s['days'])<60:raise LabError(503,'NO_HISTORY',f'Not enough daily history for {underlying} in kanida.db.')
  return s,scaled_vol(s,nifty,vix)

 def stocks(self):
  """F&O stocks: TODAY's list from the option store (survivorship: past members that left are not in it - stated)."""
  if getattr(self,'_stocks',None) is None:
   try:
    c=sqlite3.connect(f'file:{self.derivatives_db}?mode=ro',uri=True,timeout=10)
    self._stocks={r[0] for r in c.execute('select distinct underlying from contracts')}-INDICES;c.close()
   except sqlite3.Error:self._stocks=set()
  return self._stocks

 def run(self,user_id,rid,full=True):
  with self.lock:r=self.c.execute('select * from lab_runs where id=? and user_id=?',(rid,user_id)).fetchone()
  if not r:return None
  d=dict(r);d['spec']=json.loads(d['spec']);res=json.loads(d['result']) if d['result'] else None
  if res and not full:res={k:res[k] for k in ('badge','stats','control','kind') if k in res}
  d['result']=res;return d

 def runs(self,user_id,strategy_id=None,limit=30):
  q='select id from lab_runs where user_id=?'+(' and strategy_id=?' if strategy_id else '')+' order by created_at desc limit ?'
  with self.lock:ids=[x[0] for x in self.c.execute(q,(user_id,strategy_id,limit) if strategy_id else (user_id,limit)).fetchall()]
  return [self.run(user_id,i,full=False) for i in ids]

 def evidence_for(self,user_id,template,param):
  """The newest completed backtest of this template/param for the user, summarised for Discover."""
  with self.lock:
   rows=self.c.execute("select id,spec,result from lab_runs where user_id=? and kind='backtest' and status='completed' order by created_at desc limit 50",(user_id,)).fetchall()
  same=[(r,json.loads(r['spec'])) for r in rows]
  # the same rule Discover shows: this template and width, held to expiry (no stop/target/time exit), on NIFTY
  same=[(r,s) for r,s in same if s['template']==template and s.get('param')==param and s.get('underlying')=='NIFTY'
   and s.get('target_pct') is None and s.get('stop_pct') is None and s.get('exit_dte') is None and not s.get('adjust')]
  if not same:return None
  r,s=same[0];res=json.loads(r['result']);o=res['stats']['oos']
  return {'run_id':r['id'],'status':res['badge']['status'],'label':res['badge']['label'],'n_oos':o.get('n',0),'oos_ci95':o.get('ci95'),
   'period':[s['from'],s['to']],'schedule':{'weekday':s['weekday'],'dte':[s['dte_min'],s['dte_max']]},'runs_tried':len(same),
   'note':f"Held to expiry; decisions on {'every day' if s['weekday']=='daily' else ['Mon','Tue','Wed','Thu','Fri'][int(s['weekday'])]}, {s['dte_min']}-{s['dte_max']} days to expiry; {len(same)} run(s) of this rule tried"}

 def evidence_board(self,user_id):
  """Every completed plain backtest of this user, BH-corrected per family. Per-run evidence entries are computed once
  and persisted (lab_evidence); only runs without one are read in full."""
  from . import evidence as EV
  with self.lock:
   rows=self.c.execute("""select r.id,r.spec,r.created_at,e.entry from lab_runs r left join lab_evidence e on e.run_id=r.id
     where r.user_id=? and r.kind='backtest' and r.status='completed' order by r.created_at""",(user_id,)).fetchall()
  entries=[];new=[]
  for r in rows:
   if r['entry']:
    e=json.loads(r['entry'])
    if not e.get('adjust'):entries.append(e)
    continue
   s=json.loads(r['spec'])
   if s.get('adjust'):
    new.append((r['id'],json.dumps({'adjust':True})));continue
   with self.lock:res=self.c.execute('select result from lab_runs where id=?',(r['id'],)).fetchone()['result']
   res=json.loads(res) if res else None
   if not res or res.get('kind')!='backtest':continue
   e=EV.entry(r['id'],s,res,r['created_at']);entries.append(e);new.append((r['id'],json.dumps(e)))
  if new:
   with self.lock:
    self.c.executemany('insert or replace into lab_evidence values(?,?)',new);self.c.commit()
  return EV.board_entries(entries)

 def adjust_evidence_for(self,user_id,template,param,rule,k):
  """The newest completed Lab run of EXACTLY this adjustment (rule and k) on EXACTLY this structure (template and
  width), held to expiry on NIFTY. Different triggers are different runs; all of them are counted."""
  with self.lock:
   rows=self.c.execute("select id,spec,result from lab_runs where user_id=? and kind='backtest' and status='completed' order by created_at desc limit 100",(user_id,)).fetchall()
  specs=[(r,json.loads(r['spec'])) for r in rows]
  # every adjustment run on this structure counts as a test (any rule, k, trigger or schedule) - the multiple-testing base
  family=[(r,s) for r,s in specs if s.get('adjust') and s['template']==template and s.get('param')==param and s.get('underlying')=='NIFTY']
  same=[(r,s) for r,s in family if s['adjust']['rule']==rule and (s['adjust'].get('k') or None)==(k or None) and s.get('exit_dte') is None]
  if not same:return None
  r,s=same[0];res=json.loads(r['result']);a=res.get('adjustment') or {}
  m=len(family);b=adjust_badge(a.get('oos_diffs') or [],m,random.Random(20260925))
  return {'run_id':r['id'],'status':b['status'],'label':b['label'],'n_oos_triggered':len(a.get('oos_diffs') or []),'ci_corrected':b.get('ci'),
   'trigger_pct':s['adjust']['trigger_pct'],'dte':[s['dte_min'],s['dte_max']],'weekday':s['weekday'],'runs_tried':m,'period':[s['from'],s['to']],
   'note':f"Lab: triggered when the tested short was within {s['adjust']['trigger_pct']:g}% of spot at a close ({s['dte_min']}-{s['dte_max']} days to expiry at entry), applied at the next open, once per trade; {m} adjustment run(s) on this structure counted as tests"}

 # --- replay -----------------------------------------------------------------------------------------------------
 def replay_strategy(self,user_id,strategy,interval='15minute',days=10):
  body=strategy['draft']['body']
  from . import service as S
  chain,legs,problems=S.hydrate(self.market,body)
  legs=[l for l in legs if l.get('include',True)]
  if not legs:raise LabError(400,'EMPTY_STRATEGY','Add at least one listed leg to replay.')
  candles={};source=None
  live=getattr(self.market,'live_market',None)
  if live and live.available()[0]:
   to=datetime.now();fr=to-timedelta(days=days)
   try:
    for l in legs:
     r=live._get(f"/instruments/historical/{l['token']}/{interval}",**{'from':fr.strftime('%Y-%m-%d %H:%M:%S'),'to':to.strftime('%Y-%m-%d %H:%M:%S')})
     candles[l['symbol']]=[(b[0][:19].replace('T',' '),b[4]) for b in r.json()['data']['candles']]
    source='kite_historical'
   except Exception as e:  # noqa: BLE001
    log.warning('replay: Kite historical failed (%s); using captured candles',type(e).__name__);candles={}
  if not candles:
   c=sqlite3.connect(f'file:{self.derivatives_db}?mode=ro',uri=True,timeout=10)
   for l in legs:
    rows=c.execute('select k.bar_start,k.close from candles_15m k join contracts t on t.instrument_token=k.instrument_token where t.tradingsymbol=? order by k.bar_start',(l['symbol'],)).fetchall()
    candles[l['symbol']]=[(r[0][:19],r[1]) for r in rows]
   c.close();source='captured_candles_15m'
  res=replay([{'symbol':l['symbol'],'side':l['side'],'units':A.units(l),'label':f"{'Buy' if l['side']=='B' else 'Sell'} {int(l['strike'])} {l['type']}"} for l in legs],candles)
  res['source']=source;res['legs']=[{'symbol':l['symbol'],'label':f"{'Buy' if l['side']=='B' else 'Sell'} {l['lots']}× {int(l['strike'])} {l['type']}"} for l in legs]
  res['interval']=interval if source=='kite_historical' else '15minute';res['problems']=problems
  return res
