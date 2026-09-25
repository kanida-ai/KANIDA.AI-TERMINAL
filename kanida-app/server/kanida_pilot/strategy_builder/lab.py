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
from .. import implied_vol as IV

log=logging.getLogger('strategy_builder.lab')
MODEL='lab-bsm-vix-v1'
STEP={'NIFTY':50.0}
INDEX_SYMBOL={'NIFTY':'NIFTY 50'}
KITE_INDEX_TOKEN={'NIFTY 50':256265,'INDIA VIX':264969}
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
  if not live:return []
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
  out.append({'side':spec['side'],'type':spec['type'],'strike':k})
 return out


def _fill(model,side,opening,slip):
 px=max(0.05,model);move=max(0.05,px*slip)
 buying=(side=='B') if opening else (side=='S')
 return round(px+move if buying else max(0.05,px-move),2)


def simulate(spec,nifty,vix,lot_size,entry_days=None,rng=None):
 """Run the rule once. entry_days: a set of decision days to use instead of the schedule (the random control)."""
 tpl=BY_KEY[spec['template']];param=spec.get('param');step=STEP['NIFTY'];slip=spec['slippage']
 days=nifty['days'];vmap=dict(zip(vix['days'],vix['close']))
 expiries=weekly_expiries(days);last=days[-1]
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
  legs=_strikes(tpl,param,spot,sigma,t,step)
  if len({(l['type'],l['strike'],l['side']) for l in legs})<len(legs):skipped['no_expiry']+=1;i+=1;continue
  for l in legs:
   l['units']=(1 if l['side']=='B' else -1)*lot_size
   l['entry']=_fill(_price(spot,l['strike'],t,sigma,l['type']),l['side'],True,slip)
  fees=sum(CH.leg_charges(l['side'],l['entry'],lot_size)['total'] for l in legs)
  prof=A.expiry_profile([{**l,'lots':1,'lot_size':lot_size,'price':l['entry']} for l in legs])
  max_loss=None if prof['unlimited_loss'] else -prof['max_loss'];max_profit=None if prof['unlimited_profit'] else prof['max_profit']
  # walk forward: check at each close, exit next open; settle at expiry close
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
   elif spec.get('exit_dte') is not None and dte<=spec['exit_dte']:hit='time'
   if hit:
    if j+1>=n:break                                # the next open is not in the data yet - trade still open
    nd=days[j+1];no=nifty['open'][j+1];tn=_years(nd,x)
    exit_day=nd;reason=hit;exit_px=[_fill(_price(no,l['strike'],tn,cv,l['type']),l['side'],False,slip) for l in legs];break
   j+=1
  if exit_day is None:skipped['open_at_end']+=1;break
  gross=sum(l['units']*(p-l['entry']) for l,p in zip(legs,exit_px))
  if reason!='expiry':fees+=sum(CH.leg_charges('S' if l['side']=='B' else 'B',p,lot_size)['total'] for l,p in zip(legs,exit_px))
  else:fees+=sum(0.00125*p*lot_size for l,p in zip(legs,exit_px) if l['side']=='B' and p>0)   # STT on exercise of ITM longs
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
 o=stats['oos']
 if o.get('n',0)<30:badge={'status':'insufficient','label':f"Model-tested - insufficient out-of-sample trades (n={o.get('n',0)}, need 30)"}
 elif o['ci95'][0]>0:badge={'status':'model_positive','label':f"Model-tested - out-of-sample expectancy positive (95% low ₹{o['ci95'][0]:,.0f}/trade)"}
 else:badge={'status':'model_not_significant','label':'Model-tested - out-of-sample expectancy not significantly positive'}
 eq=[];c=0
 for t in trades:c+=t['net'];eq.append({'day':t['exit'],'equity':round(c,2),'split':'oos' if t['entry']>=split else 'discovery'})
 return {'kind':'backtest','model':MODEL,'badge':badge,'stats':stats,'control':control,'equity':eq,'trades':trades,'skipped':skipped,
  'lot_size':lot_size,'spec':spec,
  'provenance':{'price_source':'model','label':'Model-priced - not traded prices',
   'underlying':nifty['sources'],'volatility':{'series':'INDIA VIX close (decision day for entry, each close for marks)',**{k:v for k,v in vix['sources'].items()}},
   'expiry_calendar':'Derived NIFTY expiries: monthly (last Thursday) only before 2019-02-11, weekly Thursday from then, weekly Tuesday from 2025-09-01; holidays move to the previous trading day',
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
  if u!='NIFTY':raise LabError(400,'UNSUPPORTED_UNDERLYING','The Lab backtests NIFTY only in this release (its weekly expiry calendar is the one derived and stated).')
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
  f=str(raw.get('from') or '2016-01-01');t=str(raw.get('to') or date.today().isoformat())
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
  return {'underlying':u,'template':tpl['key'],'param':param,'weekday':wd,'dte_min':dmin,'dte_max':dmax,
   'target_pct':num('target_pct',None,1,100,True),'stop_pct':num('stop_pct',None,1,100,True),'exit_dte':(int(num('exit_dte',None,0,30,True)) if raw.get('exit_dte') not in (None,'') else None),
   'slippage':max(0.005,num('slippage_pct',0.5,0.5,10)/100),'from':f,'to':t,'split':split}

 def lot_size(self):
  try:
   ex=self.market.expiries('NIFTY')['expiries']
   return int(ex[0]['lot_size']) if ex else 65
  except Exception:return 65  # noqa: BLE001

 def start_backtest(self,user_id,raw,strategy_id=None):
  spec=self.validate(raw);rid=uuid.uuid4().hex[:16]
  with self.lock:
   self.c.execute('insert into lab_runs values(?,?,?,?,?,?,?,?,?,?,?)',(rid,user_id,strategy_id,'backtest',json.dumps(spec),'running',0,None,None,time.time(),None));self.c.commit()
  def work():
   try:
    special=self.daily.special_sessions()
    nifty=clean_series(self.daily.series('NIFTY 50'),special);vix=clean_series(self.daily.series('INDIA VIX'),special)
    if not nifty['days']:raise LabError(503,'NO_HISTORY','No NIFTY 50 daily history is readable on this machine.')
    res=backtest(spec,nifty,vix,self.lot_size(),progress=lambda p:self._save(rid,progress=round(p,2)))
    self._save(rid,status='completed',progress=1.0,result=res,finished_at=time.time())
   except Exception as e:  # noqa: BLE001 - a failed run is a state with its reason, never a partial 'result'
    log.exception('lab run failed');self._save(rid,status='failed',error=getattr(e,'message',None) or type(e).__name__,finished_at=time.time())
  threading.Thread(target=work,daemon=True,name=f'lab-{rid}').start()
  return self.run(user_id,rid)

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
   and s.get('target_pct') is None and s.get('stop_pct') is None and s.get('exit_dte') is None]
  if not same:return None
  r,s=same[0];res=json.loads(r['result']);o=res['stats']['oos']
  return {'run_id':r['id'],'status':res['badge']['status'],'label':res['badge']['label'],'n_oos':o.get('n',0),'oos_ci95':o.get('ci95'),
   'period':[s['from'],s['to']],'schedule':{'weekday':s['weekday'],'dte':[s['dte_min'],s['dte_max']]},'runs_tried':len(same),
   'note':f"Held to expiry; decisions on {'every day' if s['weekday']=='daily' else ['Mon','Tue','Wed','Thu','Fri'][int(s['weekday'])]}, {s['dte_min']}-{s['dte_max']} days to expiry; {len(same)} run(s) of this rule tried"}

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
