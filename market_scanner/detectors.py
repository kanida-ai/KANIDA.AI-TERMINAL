"""Deterministic geometry detectors. Scores express fit, never win probability.

All detectors use the same closed-bar snapshot, run independently, and inspect
only confirmed pivots (three later CLOSED bars). Rules are documented in RULES.md.
"""
from __future__ import annotations
import numpy as np

PATTERNS = [
    ('cup_handle', 'Cup & Handle', 'Rounded cup, aligned rims, a shallow handle, and a prior advance.'),
    ('horizontal_breakout', 'Horizontal Breakout', 'Repeated resistance tests followed by a fresh close above resistance with volume expansion.'),
    ('flag_pole', 'Flag & Pole', 'An impulsive pole followed by a compact, counter-trend parallel consolidation.'),
    ('symmetrical_triangle', 'Symmetrical Triangle', 'Falling resistance and rising support converge, with repeated touches on both sides.'),
    ('falling_wedge', 'Falling Wedge', 'Both boundaries fall; resistance falls faster, compressing the range.'),
    ('rising_wedge', 'Rising Wedge', 'Both boundaries rise; support rises faster, compressing the range.'),
    ('channel', 'Channel', 'Two parallel boundaries contain price, with repeated alternating reactions.'),
    ('descending_triangle', 'Descending Triangle', 'Falling resistance converges toward a horizontal support level.'),
    ('head_shoulders', 'Head & Shoulders', 'A higher head between two balanced shoulders, two neckline pivots, and a prior advance.'),
    ('inverse_head_shoulders', 'Inverse Head & Shoulders', 'A lower head between two balanced shoulders, two neckline pivots, and a prior decline.'),
]
NAMES = {p[0]:p[1] for p in PATTERNS}


def pivots(values, high=True, radius=3):
    found=[]
    for i in range(radius, len(values)-radius):
        window = values[i-radius:i+radius+1]
        if (values[i] == (max(window) if high else min(window))
                and np.count_nonzero(window == values[i]) == 1):
            found.append(i)
    return np.array(found, dtype=int)


def fit_line(x, y):
    slope, intercept = np.polyfit(x, y, 1)
    return float(slope), float(intercept), float(np.sqrt(np.mean((y - (slope*x+intercept))**2)))


def segment(label, indices, values, role='boundary'):
    return {'label':label, 'role':role, 'points':[{'index':int(i),'value':round(float(v),6)} for i,v in zip(indices,values)]}


class Detector:
    def __init__(self, bars):
        self.bars=bars
        self.n=len(bars)
        self.o,self.h,self.l,self.c,self.v = (np.array([b[k] for b in bars],dtype=float) for k in ('open','high','low','close','volume'))
        tr=np.maximum(self.h-self.l,np.maximum(abs(self.h-np.roll(self.c,1)),abs(self.l-np.roll(self.c,1))))
        self.atr=max(float(np.mean(tr[-21:-1])), float(self.c[-1])*.001)
        self.tops=pivots(self.h)
        self.bottoms=pivots(self.l,False)
        # Normalize volume for the same market-aligned slot (closing stub is shorter).
        slot=bars[-1]['time'][11:16]
        comparable=[i for i,b in enumerate(bars[:-1]) if b['time'][11:16]==slot]
        base=float(np.median(self.v[comparable[-20:]])) if comparable else 0
        self.volume_ratio=float(self.v[-1]/base) if base>0 else 0

    def result(self,key,start,quality,direction,state,lines,evidence):
        if any(b['gap'] for b in self.bars[start+1:]) or np.count_nonzero(self.v[start:]>0)/len(self.v[start:]) < .95:
            return None
        score=min(99., max(0., float(quality)))
        if score < 72:
            return None
        return {'pattern':key, 'pattern_name':NAMES[key], 'start_index':int(start),
                'end_index':self.n-1, 'bars_in_pattern':self.n-int(start), 'score':round(score,1),
                'direction':direction, 'state':state, 'lines':lines, 'evidence':evidence,
                'volume_ratio':round(self.volume_ratio,2), 'price':float(self.c[-1]),
                'candle_end':self.bars[-1]['end'], 'pattern_start':self.bars[start]['time']}

    def breakout_state(self, line, direction, start):
        sign=1 if direction=='bullish' else -1
        a,b=line
        distance=sign*(self.c[-1]-(a*(self.n-1)+b))
        if distance>self.atr*.12:
            # Exactly a fresh cross in the last three closed candles; no old breakouts.
            candidates=[]
            for j in range(max(start+1,self.n-3),self.n):
                if sign*(self.c[j]-(a*j+b))>self.atr*.12 and sign*(self.c[j-1]-(a*(j-1)+b))<=self.atr*.12:
                    candidates.append(j)
            if not candidates or distance>2.5*self.atr:
                return None
            j=candidates[-1]
            slot=self.bars[j]['time'][11:16]
            comparable=[k for k in range(j) if self.bars[k]['time'][11:16]==slot][-20:]
            base=np.median(self.v[comparable]) if comparable else 0
            ratio=self.v[j]/base if base>0 else 0
            if ratio<1.2 or any(sign*(self.c[k]-(a*k+b))<=0 for k in range(j,self.n)):
                return None
            return 'confirmed'
        if distance < -2.5*self.atr:
            return None
        # An already-broken pattern that has failed back inside is not a fresh setup.
        if any(sign*(self.c[j]-(a*j+b))>self.atr*.4 for j in range(max(start,self.n-5),self.n-1)):
            return None
        return 'setup'

    def horizontal(self):
        found=[]
        for width in (24,36,48,64,90,120):
            start=self.n-width-3
            if start<0: continue
            peaks=self.tops[(self.tops>=start)&(self.tops<self.n-3)]
            if len(peaks)<3: continue
            level=float(np.max(self.h[start:self.n-3]))
            touches=peaks[abs(self.h[peaks]-level)<=.55*self.atr]
            if len(touches)<3 or touches[-1]-touches[0]<width*.4: continue
            if np.ptp(self.l[start:self.n-3])<self.atr*1.2: continue
            state=self.breakout_state((0,level),'bullish',start)
            if state!='confirmed': continue
            if np.any(self.c[start:self.n-3]>level+.15*self.atr): continue
            q=79+min(len(touches)-3,3)*3+min(self.volume_ratio,3)*2
            found.append(self.result('horizontal_breakout',start,q,'bullish',state,
                [segment('Resistance',[start,self.n-1],[level,level]),segment('Resistance tests',touches,self.h[touches],'anchors')],
                [f'{len(touches)} separated resistance tests', 'Fresh close above resistance', 'Breakout volume ≥ 1.2× comparable candles', 'Close within 2.5 ATR of resistance']))
        return found

    def boundaries(self):
        found=[]
        for width in (28,40,56,72,96,128):
            for end_gap in (0,2):
                start=self.n-width-end_gap
                end=self.n-1-end_gap
                if start<0: continue
                hi=self.tops[(self.tops>=start)&(self.tops<=end)]
                lo=self.bottoms[(self.bottoms>=start)&(self.bottoms<=end)]
                if len(hi)<3 or len(lo)<3: continue
                if min(np.ptp(hi),np.ptp(lo))<width*.48: continue
                ah,bh,eh=fit_line(hi,self.h[hi]); al,bl,el=fit_line(lo,self.l[lo])
                if max(eh,el)>.65*self.atr: continue
                # At least five alternations rule out clusters on a single boundary.
                types=[t for _,t in sorted([(i,1) for i in hi]+[(i,-1) for i in lo])]
                if sum(a!=b for a,b in zip(types,types[1:]))<5: continue
                x=np.arange(start,end+1)
                upper=ah*x+bh; lower=al*x+bl
                w0=ah*start+bh-(al*start+bl)
                w1=ah*end+bh-(al*end+bl)
                if w0<2.5*self.atr or w1<.8*self.atr or min(upper-lower)<=0: continue
                containment=float(np.mean((self.h[x]<=upper+.7*self.atr)&(self.l[x]>=lower-.7*self.atr)))
                if containment<.9: continue
                if not (al*(self.n-1)+bl-.7*self.atr<=self.c[-1]<=ah*(self.n-1)+bh+.7*self.atr) and end_gap==0: continue
                norm=self.atr/width
                contraction=w1/w0
                types=[]
                if ah<-.6*norm and al>.6*norm and .2<contraction<.8:
                    types.append(('symmetrical_triangle','neutral'))
                if ah<al<-.2*norm and .2<contraction<.8:
                    types.append(('falling_wedge','bullish'))
                if al>ah>.2*norm and .2<contraction<.8:
                    types.append(('rising_wedge','bearish'))
                if ah<-.9*norm and abs(al)<.45*norm and .2<contraction<.8:
                    types.append(('descending_triangle','bearish'))
                if abs(ah-al)*width<=min(.9*self.atr,w0*.18) and .8<contraction<1.2:
                    types.append(('channel','bullish' if ah>.25*norm else 'bearish' if ah<-.25*norm else 'neutral'))
                for key,direction in types:
                    if key!='channel':
                        apex=(bl-bh)/(ah-al)
                        if not end<apex<end+width*1.5: continue
                    upper_now=ah*(self.n-1)+bh; lower_now=al*(self.n-1)+bl
                    if self.c[-1]>upper_now+.12*self.atr:
                        if direction=='bearish': continue
                        state=self.breakout_state((ah,bh),'bullish',start); actual='bullish'
                    elif self.c[-1]<lower_now-.12*self.atr:
                        if direction=='bullish': continue
                        state=self.breakout_state((al,bl),'bearish',start); actual='bearish'
                    else:
                        state='setup'; actual=direction
                    if not state: continue
                    if key=='channel' and state=='confirmed': continue
                    quality=76+8*containment+5*(1-(eh+el)/(1.3*self.atr))
                    lines=[segment('Resistance',[start,self.n-1],[ah*start+bh,upper_now]),segment('Support',[start,self.n-1],[al*start+bl,lower_now]),
                           segment('Upper pivots',hi,self.h[hi],'anchors'),segment('Lower pivots',lo,self.l[lo],'anchors')]
                    found.append(self.result(key,start,quality,actual,state,lines,[f'{len(hi)} upper / {len(lo)} lower pivots',f'{containment:.0%} price containment',
                        f'Boundary fit error ≤ {max(eh,el)/self.atr:.2f} ATR', 'Parallel boundaries' if key=='channel' else f'Range contracted {(1-contraction):.0%}']))
        return found

    def cups(self):
        found=[]
        for right in self.tops[(self.tops>=self.n-26)&(self.tops<=self.n-5)]:
            for left in self.tops[(self.tops>=max(15,right-150))&(self.tops<=right-24)]:
                width=right-left
                if self.n-1-right>width*.45: continue
                bottom=left+int(np.argmin(self.l[left:right+1]))
                if not .25<(bottom-left)/width<.75: continue
                rim=(self.h[left]+self.h[right])/2
                depth=rim-self.l[bottom]
                if not max(3*self.atr,.04*rim)<depth<.4*rim: continue
                alignment=abs(self.h[left]-self.h[right])/depth
                if alignment>.22: continue
                if self.h[left]-self.c[max(0,left-20)]<.5*depth: continue
                y=self.c[left:right+1]
                x=np.linspace(-1,1,len(y))
                coeff=np.polyfit(x,y,2)
                fitted=np.polyval(coeff,x)
                denom=np.sum((y-y.mean())**2)
                r2=1-np.sum((y-fitted)**2)/denom if denom else 0
                if coeff[0]<=0 or r2<.78: continue
                # A broad floor and curved fit must outperform a sharp V-shaped fit.
                floor=np.mean(y<self.l[bottom]+.28*depth)
                vx=np.column_stack((np.abs(np.arange(len(y))-(bottom-left)),np.ones(len(y))))
                vfit=vx@np.linalg.lstsq(vx,y,rcond=None)[0]
                if not .18<floor<.60 or np.mean((y-fitted)**2)>np.mean((y-vfit)**2)*.9: continue
                handle_bottom=right+1+int(np.argmin(self.l[right+1:]))
                handle_depth=self.h[right]-self.l[handle_bottom]
                if not .07*depth<handle_depth<.4*depth or handle_bottom>=self.n-1: continue
                if np.mean(self.v[right+1:self.n-1])>np.mean(self.v[left:right+1])*1.05: continue
                resistance=max(self.h[left],self.h[right])
                state=self.breakout_state((0,resistance),'bullish',left)
                if not state or self.c[-1]<self.h[right]-.45*depth: continue
                curve_indices=np.linspace(left,right,25).astype(int)
                curve_values=np.polyval(coeff,2*(curve_indices-left)/width-1)
                found.append(self.result('cup_handle',left,78+r2*10+(1-alignment)*4,'bullish',state,
                    [segment('Cup',curve_indices,curve_values,'curve'),segment('Handle',[right,handle_bottom,self.n-1],[self.h[right],self.l[handle_bottom],self.c[-1]],'shape'),
                     segment('Rim resistance',[left,self.n-1],[resistance,resistance])],
                    [f'Rounded bowl fit {r2:.0%}',f'Rim difference {alignment:.0%} of depth',f'Handle retracement {handle_depth/depth:.0%}', 'Prior advance and contracting handle volume']))
        return found

    def shoulders(self, inverse=False):
        found=[]
        sign=-1 if inverse else 1
        high=-self.l if inverse else self.h
        low=-self.h if inverse else self.l
        peaks=self.bottoms if inverse else self.tops
        peaks=peaks[peaks>=max(15,self.n-140)]
        key='inverse_head_shoulders' if inverse else 'head_shoulders'
        for j in range(1,len(peaks)-1):
            left,head,right=map(int,peaks[j-1:j+2])
            width=right-left
            if width<16 or self.n-1-right>max(6,width*.4): continue
            t1=left+int(np.argmin(low[left:head+1])); t2=head+int(np.argmin(low[head:right+1]))
            if not left<t1<head<t2<right: continue
            depth=high[head]-(low[t1]+low[t2])/2
            if depth<4*self.atr: continue
            prominence=high[head]-max(high[left],high[right])
            if not .18*depth<prominence<.65*depth: continue
            if abs(high[left]-high[right])>.23*depth: continue
            if not .45<(head-left)/(right-head)<2.2: continue
            if abs(low[t1]-low[t2])>.3*depth: continue
            if high[left]-sign*self.c[max(0,left-20)]<.5*depth: continue
            a=(low[t2]-low[t1])/(t2-t1); b=low[t1]-a*t1
            direction='bullish' if inverse else 'bearish'
            state=self.breakout_state((sign*a,sign*b),direction,left)
            if not state: continue
            # Reject a price that has invalidated the right shoulder.
            if sign*self.c[-1]>high[right]+.3*self.atr: continue
            lines=[segment('Pattern',[left,t1,head,t2,right],[sign*high[left],sign*low[t1],sign*high[head],sign*low[t2],sign*high[right]],'shape'),
                   segment('Neckline',[left,self.n-1],[sign*(a*left+b),sign*(a*(self.n-1)+b)]),
                   segment('Left shoulder',[left],[sign*high[left]],'label'),segment('Head',[head],[sign*high[head]],'label'),segment('Right shoulder',[right],[sign*high[right]],'label')]
            q=83+6*(1-abs(high[left]-high[right])/depth)
            found.append(self.result(key,left,q,direction,state,lines,[f'Head prominence {prominence/depth:.0%} of depth','Balanced shoulder height and duration', 'Two confirmed neckline pivots','Prior decline' if inverse else 'Prior advance']))
        return found

    def flags(self):
        found=[]
        for length in (10,14,18,24):
            start=self.n-length-1
            if start<22: continue
            for sign in (1,-1):
                pole_start=start-12+int(np.argmin(sign*self.c[start-12:start-5]))
                pole=sign*(self.c[start]-self.c[pole_start])
                path=np.sum(abs(np.diff(self.c[pole_start:start+1])))
                if pole<5*self.atr or pole/max(path,1e-9)<.72: continue
                pole_volume=np.mean(self.v[pole_start:start+1])
                before=np.mean(self.v[max(0,pole_start-15):pole_start])
                if pole_volume<before*1.2: continue
                x=np.arange(start,self.n-1)
                a,b,err=fit_line(x,self.c[x])
                if sign*a>0 or abs(a)*length>pole*.5 or err>.8*self.atr: continue
                upper=float(np.max(self.h[x]-a*x)); lower=float(np.min(self.l[x]-a*x))
                width=upper-lower
                if width>pole*.5 or width<self.atr: continue
                if np.mean(self.v[x])>pole_volume*.85: continue
                retracement=(self.c[start]-np.min(self.l[x]))/pole if sign==1 else (np.max(self.h[x])-self.c[start])/pole
                if not .08<retracement<.5: continue
                direction='bullish' if sign==1 else 'bearish'
                state=self.breakout_state((a,upper if sign==1 else lower),direction,start)
                if not state: continue
                if not a*(self.n-1)+lower-self.atr*(.4 if sign==1 else 2.5) <= self.c[-1] <= a*(self.n-1)+upper+self.atr*(2.5 if sign==1 else .4): continue
                lines=[segment('Pole',[pole_start,start],[self.c[pole_start],self.c[start]],'shape'),
                       segment('Flag resistance',[start,self.n-1],[a*start+upper,a*(self.n-1)+upper]),
                       segment('Flag support',[start,self.n-1],[a*start+lower,a*(self.n-1)+lower])]
                found.append(self.result('flag_pole',pole_start,82+min(pole/self.atr,10)*.7,direction,state,lines,
                    [f'Impulse {pole/self.atr:.1f} ATR',f'Pole efficiency {pole/path:.0%}',f'Retracement {retracement:.0%}', 'Counter-trend flag with lower volume']))
        return found


def detect(bars, enabled=None):
    if len(bars)<40: return []
    enabled=set(NAMES if enabled is None else enabled)
    d=Detector(bars)
    candidates=[]
    # Independent families, never short-circuit when one pattern qualifies.
    if 'horizontal_breakout' in enabled: candidates.extend(d.horizontal())
    if enabled & {'symmetrical_triangle','falling_wedge','rising_wedge','channel','descending_triangle'}: candidates.extend(d.boundaries())
    if 'cup_handle' in enabled: candidates.extend(d.cups())
    if 'head_shoulders' in enabled: candidates.extend(d.shoulders())
    if 'inverse_head_shoulders' in enabled: candidates.extend(d.shoulders(True))
    if 'flag_pole' in enabled: candidates.extend(d.flags())
    best={}
    for c in candidates:
        if c and c['pattern'] in enabled and (c['pattern'] not in best or c['score']>best[c['pattern']]['score']):
            best[c['pattern']]=c
    return sorted(best.values(),key=lambda c:-c['score'])
