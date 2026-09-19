"""Conservative compiled rejection gates. Only the original Detector qualifies.

This filter skips families whose necessary geometry cannot pass. Tolerances are
deliberately wider than the detector's. No fitted thresholds or future pivots.
"""
import numpy as np
from numba import njit


@njit(cache=True)
def fit(x,y):
    xm=x.mean(); ym=y.mean()
    a=((x-xm)*(y-ym)).sum()/((x-xm)**2).sum()
    b=ym-a*xm
    return a,b,np.sqrt(((y-a*x-b)**2).mean())


@njit(cache=True)
def masks(h,l,c,v,tr,tops,bottoms,lookback=260):
    out=np.zeros(len(c),np.uint8)
    negative_h=-h; negative_l=-l
    eps=1e-7
    for t in range(39,len(c)):
        begin=max(0,t-lookback+1)
        hp=tops[(tops>=begin+3)&(tops<=t-3)]
        lp=bottoms[(bottoms>=begin+3)&(bottoms<=t-3)]
        atr=max(tr[t-20:t].mean(),c[t]*.001)
        # Boundary family: linear fits and necessary shape constraints.
        for width in (28,40,56,72,96,128):
            for eg in (0,2):
                start=t+1-width-eg; end=t-eg
                if start<begin:continue
                hi=hp[(hp>=start)&(hp<=end)]; lo=lp[(lp>=start)&(lp<=end)]
                if len(hi)<3 or len(lo)<3:continue
                if min(hi[-1]-hi[0],lo[-1]-lo[0])<width*.48:continue
                ah,bh,eh=fit(hi.astype(np.float64)-start,h[hi])
                al,bl,el=fit(lo.astype(np.float64)-start,l[lo])
                if max(eh,el)>(.65+eps)*atr:continue
                w0=bh-bl; w1=(ah-al)*(end-start)+bh-bl
                if w0<(2.5-eps)*atr or w1<(.8-eps)*atr:continue
                norm=atr/width; ratio=w1/w0
                triangle=(ah<(-.6+eps)*norm and al>(.6-eps)*norm)
                falling=(ah<al+eps*norm and al<(-.2+eps)*norm)
                rising=(al>ah-eps*norm and ah>(.2-eps)*norm)
                descending=(ah<(-.9+eps)*norm and abs(al)<(.45+eps)*norm)
                converge=(.2-eps<ratio<.8+eps) and (triangle or falling or rising or descending)
                channel=abs(ah-al)*width<=min(.9*atr,w0*.18)+eps*atr and .8-eps<ratio<1.2+eps
                if not (converge or channel):continue
                contained=0
                for j in range(start,end+1):
                    if h[j]<=ah*(j-start)+bh+(.7+eps)*atr and l[j]>=al*(j-start)+bl-(.7+eps)*atr:contained+=1
                if contained/(end-start+1)<.9-eps:continue
                if eg==0 and not al*(t-start)+bl-(.7+eps)*atr<=c[t]<=ah*(t-start)+bh+(.7+eps)*atr:continue
                out[t]|=1
        # Cup: necessary rim, floor location, depth, handle and prior trend.
        for right in hp[(hp>=t+1-26)&(hp<=t+1-5)]:
            for left in hp[(hp>=max(begin+15,right-150))&(hp<=right-24)]:
                width=right-left
                if t-right>width*.45:continue
                bottom=left+np.argmin(l[left:right+1])
                if not .25-eps<(bottom-left)/width<.75+eps:continue
                rim=(h[left]+h[right])/2; depth=rim-l[bottom]
                if not max((3-eps)*atr,(.04-eps)*rim)<depth<(.4+eps)*rim:continue
                if abs(h[left]-h[right])>(.22+eps)*depth:continue
                if h[left]-c[max(begin,left-20)]<(.5-eps)*depth:continue
                hb=right+1+np.argmin(l[right+1:t+1]); hd=h[right]-l[hb]
                if not (.07-eps)*depth<hd<(.4+eps)*depth or hb>=t:continue
                if v[right+1:t].mean()>v[left:right+1].mean()*(1.05+eps):continue
                if c[t]<h[right]-(.45+eps)*depth:continue
                if abs(c[t]-max(h[left],h[right]))>(2.5+eps)*atr:continue
                out[t]|=2
        # Shoulder families.
        for inverse in (0,1):
            sign=1 if inverse==0 else -1
            high=h if inverse==0 else negative_l
            low=l if inverse==0 else negative_h
            peaks=hp if inverse==0 else lp
            peaks=peaks[peaks>=max(begin+15,t+1-140)]
            for j in range(1,len(peaks)-1):
                left=peaks[j-1]; head=peaks[j]; right=peaks[j+1]; width=right-left
                if width<16 or t-right>max(6,width*.4):continue
                t1=left+np.argmin(low[left:head+1]); t2=head+np.argmin(low[head:right+1])
                if not left<t1<head<t2<right:continue
                depth=high[head]-(low[t1]+low[t2])/2
                if depth<(4-eps)*atr:continue
                prominence=high[head]-max(high[left],high[right])
                if not (.18-eps)*depth<prominence<(.65+eps)*depth:continue
                if abs(high[left]-high[right])>(.23+eps)*depth:continue
                if not .45-eps<(head-left)/(right-head)<2.2+eps:continue
                if abs(low[t1]-low[t2])>(.3+eps)*depth:continue
                if high[left]-sign*c[max(begin,left-20)]<(.5-eps)*depth:continue
                a=(low[t2]-low[t1])/(t2-t1)
                line=low[t1]+a*(t-t1)
                if abs(sign*c[t]-line)>(2.5+eps)*atr:continue
                if sign*c[t]>high[right]+(.3+eps)*atr:continue
                out[t]|=4 if inverse==0 else 8
        # Flag: necessary pole, regression, volume and retracement.
        for length in (10,14,18,24):
            start=t-length
            if start<begin+22:continue
            for sign in (1,-1):
                ps=start-12+np.argmin(sign*c[start-12:start-5])
                pole=sign*(c[start]-c[ps]); path=np.abs(np.diff(c[ps:start+1])).sum()
                if pole<(5-eps)*atr or pole/max(path,1e-9)<.72-eps:continue
                pv=v[ps:start+1].mean()
                if pv<v[max(begin,ps-15):ps].mean()*(1.2-eps):continue
                x=np.arange(length).astype(np.float64)
                a,b,err=fit(x,c[start:t])
                if sign*a>eps*atr or abs(a)*length>pole*(.5+eps) or err>(.8+eps)*atr:continue
                upper=np.max(h[start:t]-a*x); lower=np.min(l[start:t]-a*x)
                w=upper-lower
                if w>pole*(.5+eps) or w<(1-eps)*atr:continue
                if v[start:t].mean()>pv*(.85+eps):continue
                retr=(c[start]-np.min(l[start:t]))/pole if sign==1 else (np.max(h[start:t])-c[start])/pole
                if not .08-eps<retr<.5+eps:continue
                line=a*length+(upper if sign==1 else lower)
                if abs(c[t]-line)>(2.5+eps)*atr:continue
                out[t]|=16
        for width in (24,36,48,64,90,120):
            start=t+1-width-3
            if start<begin:continue
            peaks=hp[(hp>=start)&(hp<t-2)]
            if len(peaks)<3:continue
            level=h[start:t-2].max()
            if not (.12-eps)*atr<c[t]-level<(2.5+eps)*atr:continue
            touches=peaks[np.abs(h[peaks]-level)<=(.55+eps)*atr]
            if len(touches)<3 or touches[-1]-touches[0]<width*.4:continue
            if l[start:t-2].max()-l[start:t-2].min()<(1.2-eps)*atr:continue
            out[t]|=32
    return out
