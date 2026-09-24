"""What a state word MEANS, read off the 15-minute readings — with the tab's own rules, not new thresholds.

THE THREE DIRECTION RULES ARE PORTS, NOT INVENTIONS. Each is a line-for-line port of a function the Derivative
tab already draws its chips with; `scripts/check-screener.cjs` runs the TypeScript originals on the same
fixtures as `tests/test_screener_states.py` and refuses a difference.

  grid_oi     logic.ts gridDirection       ΔOI (OI against the previous close). Flat inside 5% of the
                                           contract's own largest |ΔOI| today (GRID_FLAT_FRACTION).
  grid_price  logic.ts gridPriceDirection  the contract's own premium. Flat inside 5% of its own largest
                                           move from the day's first priced reading (GRID_PRICE_FLAT_FRACTION).
  session     logic.ts sessionDirection    PCR, max pain, IV, spot, futures OI. Flat inside 5% of the
                                           series' own span (SESSION_FLAT_FRACTION).

Every rule reads CAPTURED values only: a reading with no value is skipped, never filled (summary.ts RULE 2 —
an absence of measurement neither extends nor ends anything). "`lookback` back" is counted in captured values.

HOW THE WORDS COMPOSE THOSE RULES (the window is `k` 15-min intervals):
  Increasing / Decreasing / Stable  the rule itself with lookback = k, instead of the tab's fixed 4.
  ... continuously                  every one of the k intervals, each read by the same rule with lookback
                                    1, has the same non-flat direction, and so does the window as a whole.
  Expanding / Contracting rapidly   continuously, AND the latest interval's move is more than PACE_UP (1.25x)
                                    the one before it — summary.ts "strengthened", the block's own pace band.
  Reversing higher / lower          the window that ended one reading earlier read the other way, and the
                                    latest interval clears the flat band in the new direction.
A window needs every reading it names: "45 min" at 09:45 is not enough readings, never a guess.
"""
from __future__ import annotations

GRID_FLAT_FRACTION=0.05
GRID_PRICE_FLAT_FRACTION=0.05
SESSION_FLAT_FRACTION=0.05
PACE_UP=1.25
PACE_DOWN=0.75
#: logic.ts GRID_DIRECTION_LOOKBACK / SESSION_LOOKBACK: the tab's own chips read over four readings.
TAB_LOOKBACK=4


def _real(values):
 return [float(v) for v in values if v is not None]


def grid_oi(values,lookback=TAB_LOOKBACK,flat=GRID_FLAT_FRACTION):
 """logic.ts gridDirection. 'up' | 'flat' | 'down', or None under two captured values."""
 usable=_real(values)
 if len(usable)<2:return None
 index=max(0,len(usable)-1-lookback)
 change=usable[-1]-usable[index]
 scale=max(abs(v) for v in usable)
 if scale==0 or abs(change)<flat*scale:return 'flat'
 return 'up' if change>0 else 'down'


def grid_price(values,lookback=TAB_LOOKBACK,flat=GRID_PRICE_FLAT_FRACTION):
 """logic.ts gridPriceDirection: the band scales with the contract's own largest move from its first price."""
 usable=_real(values)
 if len(usable)<2:return None
 index=max(0,len(usable)-1-lookback)
 change=usable[-1]-usable[index]
 first=usable[0]
 scale=max(abs(v-first) for v in usable)
 if scale==0 or abs(change)<flat*scale:return 'flat'
 return 'up' if change>0 else 'down'


def session(values,lookback=TAB_LOOKBACK,flat=SESSION_FLAT_FRACTION):
 """logic.ts sessionDirection, with keys normalised to up / flat / down."""
 real=_real(values)
 if len(real)<2:return None
 last=real[-1]
 earlier=real[max(0,len(real)-1-lookback)]
 change=last-earlier
 span=max(max(abs(v-earlier) for v in real),abs(change)) or abs(last) or 1
 if abs(change)<=span*flat:return 'flat'
 return 'up' if change>0 else 'down'


RULES={'grid_oi':grid_oi,'grid_price':grid_price,'session':session}


def direction(family,values,lookback):
 return RULES[family](values,lookback)


def step_directions(family,values,count):
 """The direction of each of the last `count` intervals, each read by the same rule with lookback 1 over the
 values captured up to that interval's end — exactly what the chip would have said at that reading."""
 real=_real(values)
 out=[]
 for end in range(len(real)-count,len(real)):
  out.append(direction(family,real[:end+1],1) if end>=1 else None)
 return out


def pace(values,at=None):
 """|latest interval move| / |the interval before it|, when both moved the same way; else None.

 `at` (the reading index of each captured value) makes it refuse across a gap: a move that spans a missing
 reading covers 30 minutes, and comparing it with a 15-minute move would call it "speeding up" when nothing did."""
 real=_real(values)
 if len(real)<3:return None
 if at is not None and (at[-1]-at[-2]!=1 or at[-2]-at[-3]!=1):return None
 last,prev=real[-1]-real[-2],real[-2]-real[-3]
 if prev==0 or last==0 or (last>0)!=(prev>0):return None
 return abs(last)/abs(prev)


def pace_word(ratio):
 """summary.ts PACE_UP / PACE_DOWN: 'strengthening' above 1.25x, 'weakening' under 0.75x, else None."""
 if ratio is None:return None
 if ratio>PACE_UP:return 'strengthening'
 if ratio<PACE_DOWN:return 'weakening'
 return None


def read(family,values,k,state,at=None):
 """Whether `state` holds at the newest captured value, over a window of `k` intervals.

 Returns (holds, info). `holds` is None when there are not enough captured readings for the window —
 "not enough readings yet", which is neither a match nor a miss. `info` carries the numbers the
 explanation sentence is written from: the value at the window's start and end, the change, and the
 per-interval directions.
 """
 real=_real(values)
 n=len(real)
 k=max(1,int(k))
 need=k+1
 if state in ('up_rapid','down_rapid'):need=max(k,2)+1
 if state in ('rev_up','rev_down'):need=k+2
 info={'k':k,'captured':n,'need':need}
 if n<need:return None,info
 d=direction(family,real,k)
 steps=step_directions(family,real,k)
 info.update(direction=d,start=real[-1-k],end=real[-1],change=real[-1]-real[-1-k],step_directions=steps,
  pace=pace(real,at))
 want='up' if state.startswith('up') or state=='rev_up' else 'down'
 if state in ('up','down'):return d==want,info
 if state=='stable':return d=='flat',info
 if state in ('up_cont','down_cont'):return d==want and all(s==want for s in steps),info
 if state in ('up_rapid','down_rapid'):
  run=step_directions(family,real,max(k,2))
  ratio=info['pace']
  return (d==want and all(s==want for s in run) and ratio is not None and ratio>PACE_UP),info
 if state in ('rev_up','rev_down'):
  before=direction(family,real[:-1],k)
  other='down' if want=='up' else 'up'
  info['before']=before
  return (before==other and steps[-1]==want),info
 raise ValueError(f'state {state!r} is not a directional state')


#: logic.ts FLOW_LABELS, the four cells that name a participant. (option type, price dir, OI dir) -> behaviour.
FLOW={('CE','down','up'):'writing',('CE','up','up'):'buying',('CE','up','down'):'short_covering',
 ('CE','down','down'):'buyers_exiting',('PE','down','up'):'writing',('PE','up','up'):'buying',
 ('PE','up','down'):'short_covering',('PE','down','down'):'buyers_exiting'}
#: The tab's own sentence for each, word for word FLOW_LABELS.
FLOW_TEXT={('CE','writing'):'Call writing increasing',('CE','buying'):'Call buying increasing',
 ('CE','short_covering'):'Call short covering',('CE','buyers_exiting'):'Call buyers exiting',
 ('PE','writing'):'Put writing increasing',('PE','buying'):'Put buying increasing',
 ('PE','short_covering'):'Put short covering',('PE','buyers_exiting'):'Put buyers exiting'}


def flow(option_type,prices,ois,k):
 """logic.ts gridFlow over a window of k intervals: (behaviour | 'none', price dir, OI dir), or None when
 either line has too few captured values for the window."""
 p,o=_real(prices),_real(ois)
 if len(p)<k+1 or len(o)<k+1:return None
 pd,od=grid_price(p,k),grid_oi(o,k)
 if pd is None or od is None:return None
 return FLOW.get((option_type,pd,od),'none'),pd,od


def quiet_steps(prices,ois,k):
 """summary.ts behaviour 'quiet' (FLOW_LABELS flat|flat) at each of the last k intervals."""
 p,o=_real(prices),_real(ois)
 if len(p)<k+1 or len(o)<k+1:return None
 for back in range(k):
  pe,oe=len(p)-back,len(o)-back
  if grid_price(p[:pe],1)!='flat' or grid_oi(o[:oe],1)!='flat':return False
 return True


def breadth_word(strikes,ladder):
 """signal.ts breadthOf: isolated / clustered / dispersed / none."""
 n=len(strikes)
 if n<=0:return 'none'
 if n==1:return 'isolated'
 idx=sorted(ladder.index(s) for s in strikes if s in ladder)
 contiguous=bool(idx) and idx[-1]-idx[0]==len(idx)-1
 return 'clustered' if contiguous else 'dispersed'


class Series:
 """One captured series with prefix statistics, so each rule above answers at ANY reading in O(1).

 Same arithmetic as the reference functions, reorganised: a prefix max/min stands in for re-scanning the
 values up to each reading. `tests/test_screener_states.py` checks this class against the reference functions
 on every reading of random and real series; the reference functions are the ones checked against logic.ts.
 """
 __slots__=('at','v','pos','pmax','pmin','pabs','pdev')
 def __init__(self,values):
  self.at=[];self.v=[]
  for i,x in enumerate(values):
   if x is None or x!=x:continue
   self.at.append(i);self.v.append(float(x))
  self.pos={i:j for j,i in enumerate(self.at)}
  self.pmax,self.pmin,self.pabs,self.pdev=[],[],[],[]
  hi=lo=None;ab=dev=0.0
  for x in self.v:
   hi=x if hi is None or x>hi else hi;lo=x if lo is None or x<lo else lo
   ab=max(ab,abs(x));dev=max(dev,abs(x-self.v[0]))
   self.pmax.append(hi);self.pmin.append(lo);self.pabs.append(ab);self.pdev.append(dev)

 def index(self,i):
  """The captured-value index of reading i, or None when that reading carried no value."""
  return self.pos.get(i)

 def direction(self,family,j,lookback):
  if j is None or j<1:return None
  v=self.v
  e=max(0,j-lookback)
  change=v[j]-v[e]
  if family=='grid_oi':
   scale=self.pabs[j]
   if scale==0 or abs(change)<GRID_FLAT_FRACTION*scale:return 'flat'
  elif family=='grid_price':
   scale=self.pdev[j]
   if scale==0 or abs(change)<GRID_PRICE_FLAT_FRACTION*scale:return 'flat'
  else:
   earlier=v[e]
   span=max(self.pmax[j]-earlier,earlier-self.pmin[j],abs(change)) or abs(v[j]) or 1
   if abs(change)<=span*SESSION_FLAT_FRACTION:return 'flat'
  return 'up' if change>0 else 'down'

 def steps(self,family,j,count):
  return [self.direction(family,end,1) if end>=1 else None for end in range(j-count+1,j+1)]

 def pace(self,j):
  if j is None or j<2:return None
  if self.at[j]-self.at[j-1]!=1 or self.at[j-1]-self.at[j-2]!=1:return None  # never across a gap
  v=self.v
  last,prev=v[j]-v[j-1],v[j-1]-v[j-2]
  if prev==0 or last==0 or (last>0)!=(prev>0):return None
  return abs(last)/abs(prev)

 def read(self,family,j,k,state):
  """`read()` above, at captured index j. None = not enough captured readings for the window."""
  k=max(1,int(k))
  n=0 if j is None else j+1
  need=k+1
  if state in ('up_rapid','down_rapid'):need=max(k,2)+1
  if state in ('rev_up','rev_down'):need=k+2
  info={'k':k,'captured':n,'need':need}
  if n<need:return None,info
  d=self.direction(family,j,k)
  steps=self.steps(family,j,k)
  info.update(direction=d,start=self.v[j-k],end=self.v[j],change=self.v[j]-self.v[j-k],step_directions=steps,
   pace=self.pace(j),start_at=self.at[j-k],end_at=self.at[j])
  want='up' if state.startswith('up') or state=='rev_up' else 'down'
  if state in ('up','down'):return d==want,info
  if state=='stable':return d=='flat',info
  if state in ('up_cont','down_cont'):return d==want and all(s==want for s in steps),info
  if state in ('up_rapid','down_rapid'):
   run=self.steps(family,j,max(k,2))
   ratio=info['pace']
   return (d==want and all(s==want for s in run) and ratio is not None and ratio>PACE_UP),info
  if state in ('rev_up','rev_down'):
   before=self.direction(family,j-1,k)
   info['before']=before
   return (before==('down' if want=='up' else 'up') and steps[-1]==want),info
  raise ValueError(f'state {state!r} is not a directional state')
