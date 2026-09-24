# -*- coding: utf-8 -*-
"""FULL technical-indicator library — 120+ named indicators, each with EXPLICIT parameters and a
buy(+1)/sell(-1)/neutral(0) signal rule. Built for the playbook so every indicator is visible: its
params, what it fires each day, and its next-day open->close performance. No shortcuts."""
import numpy as np, pandas as pd


def ema(s, n): return pd.Series(s).ewm(span=n, adjust=False).mean().values
def sma(s, n): return pd.Series(s).rolling(n).mean().values
def wma(s, n):
    w = np.arange(1, n+1); return pd.Series(s).rolling(n).apply(lambda x: np.dot(x, w)/w.sum(), raw=True).values
def rma(s, n): return pd.Series(s).ewm(alpha=1/n, adjust=False).mean().values
def std(s, n): return pd.Series(s).rolling(n).std().values
def hhv(s, n): return pd.Series(s).rolling(n).max().values
def llv(s, n): return pd.Series(s).rolling(n).min().values
def rsi(c, n):
    d = np.diff(c, prepend=c[0]); return 100 - 100/(1 + rma(np.clip(d, 0, None), n)/(rma(-np.clip(d, None, 0), n)+1e-9))
def tr(h, l, c):
    pc = np.roll(c, 1); pc[0] = c[0]; return np.maximum(h-l, np.maximum(np.abs(h-pc), np.abs(l-pc)))
def atr(h, l, c, n): return rma(tr(h, l, c), n)
def hma(s, n):
    return wma(2*wma(s, n//2) - wma(s, n), int(np.sqrt(n)))
def cross(fast, slow):
    s = np.zeros(len(fast)); pf, ps = np.roll(fast, 1), np.roll(slow, 1)
    s[(fast > slow) & (pf <= ps)] = 1; s[(fast < slow) & (pf >= ps)] = -1; return s
def state(cond_up, cond_dn):
    s = np.zeros(len(cond_up)); s[cond_up] = 1; s[cond_dn] = -1; return s


def build_all(D):
    """Return list of (name, params, signal_array). ~120+ indicators."""
    o, h, l, c, v = D["o"], D["h"], D["l"], D["c"], D["v"]
    n = len(c); tp = (h+l+c)/3; pc = np.roll(c, 1); pc[0] = c[0]
    R = []
    def add(name, params, sig): R.append((name, params, np.nan_to_num(sig, nan=0.0)))

    # ================= TREND / MOVING AVERAGES =================
    for p in (10, 20, 50, 100, 200): add(f"price_vs_SMA", f"n={p}", state(c > sma(c, p), c < sma(c, p)))
    for p in (9, 21, 34, 50): add(f"price_vs_EMA", f"n={p}", state(c > ema(c, p), c < ema(c, p)))
    add("price_vs_WMA", "n=20", state(c > wma(c, 20), c < wma(c, 20)))
    add("price_vs_HMA", "n=20", state(c > hma(c, 20), c < hma(c, 20)))
    for (f_, s_) in [(5, 20), (9, 21), (12, 26), (5, 50), (3, 18), (7, 34), (8, 21), (10, 30)]:
        add("EMA_cross", f"{f_}x{s_}", cross(ema(c, f_), ema(c, s_)))
    for (f_, s_) in [(10, 20), (20, 50), (50, 100), (50, 200)]:
        add("SMA_cross", f"{f_}x{s_}", cross(sma(c, f_), sma(c, s_)))
    add("golden_death_cross", "50x200", cross(sma(c, 50), sma(c, 200)))
    for (f_, s_, g_) in [(12, 26, 9), (8, 21, 5), (5, 34, 5), (10, 30, 9)]:
        macd = ema(c, f_) - ema(c, s_); add("MACD", f"{f_},{s_},{g_}", cross(macd, ema(macd, g_)))
    macd = ema(c, 12) - ema(c, 26); add("MACD_hist", "12,26,9", state((macd-ema(macd, 9)) > 0, (macd-ema(macd, 9)) < 0))
    for p in (9, 15, 18):
        t3 = ema(ema(ema(c, p), p), p); add("TRIX", f"n={p}", state(pd.Series(t3).pct_change().values > 0, pd.Series(t3).pct_change().values < 0))
    add("DPO", "n=20", state((c - np.roll(sma(c, 20), 11)) > 0, (c - np.roll(sma(c, 20), 11)) < 0))
    ten = (hhv(h, 9)+llv(l, 9))/2; kij = (hhv(h, 26)+llv(l, 26))/2
    add("Ichimoku_TK_cross", "9,26", cross(ten, kij))
    spanA = np.roll((ten+kij)/2, 26); spanB = np.roll((hhv(h, 52)+llv(l, 52))/2, 26)
    add("Ichimoku_cloud", "9,26,52", state((c > spanA) & (c > spanB), (c < spanA) & (c < spanB)))
    for (p, m) in [(10, 3.0), (7, 2.0), (14, 3.0)]:
        a = atr(h, l, c, p); hl2 = (h+l)/2; st = np.zeros(n); dirn = np.ones(n)
        ub = hl2+m*a; lb = hl2-m*a
        for i in range(1, n):
            if np.isnan(ub[i]): st[i] = c[i]; continue
            dirn[i] = 1 if c[i] > st[i-1] else (-1 if c[i] < st[i-1] else dirn[i-1]); st[i] = lb[i] if dirn[i] == 1 else ub[i]
        add("SuperTrend", f"{p},{m}", state(dirn > 0, dirn < 0))
    # PSAR
    st = np.zeros(n); bull = True; af = 0.02; ep = h[0]; sar = l[0]
    for i in range(1, n):
        sar = sar+af*(ep-sar)
        if bull:
            if l[i] < sar: bull = False; sar = ep; ep = l[i]; af = 0.02
            elif h[i] > ep: ep = h[i]; af = min(af+0.02, 0.2)
        else:
            if h[i] > sar: bull = True; sar = ep; ep = h[i]; af = 0.02
            elif l[i] < ep: ep = l[i]; af = min(af+0.02, 0.2)
        st[i] = 1 if bull else -1
    add("Parabolic_SAR", "0.02,0.2", st)
    for p in (14, 21):
        up = np.diff(h, prepend=h[0]); dn = -np.diff(l, prepend=l[0])
        pdm = np.where((up > dn) & (up > 0), up, 0.0); mdm = np.where((dn > up) & (dn > 0), dn, 0.0)
        a = atr(h, l, c, p)+1e-9; pdi = 100*rma(pdm, p)/a; mdi = 100*rma(mdm, p)/a; adx = rma(100*np.abs(pdi-mdi)/(pdi+mdi+1e-9), p)
        add("ADX_DI", f"n={p}", state((pdi > mdi) & (adx > 20), (mdi > pdi) & (adx > 20)))
    for p in (14, 25):
        au = pd.Series(h).rolling(p).apply(lambda x: (p-1-np.argmax(x))/p*100, raw=True).values
        ad = pd.Series(l).rolling(p).apply(lambda x: (p-1-np.argmin(x))/p*100, raw=True).values
        add("Aroon", f"n={p}", state(au > ad, au < ad))
    vp = np.abs(h-np.roll(l, 1)); vm = np.abs(l-np.roll(h, 1))
    vi = (pd.Series(vp).rolling(14).sum().values-pd.Series(vm).rolling(14).sum().values)/(pd.Series(tr(h, l, c)).rolling(14).sum().values+1e-9)
    add("Vortex", "n=14", state(vi > 0, vi < 0))
    add("Alligator", "5,13", state(ema(c, 5) > ema(c, 13), ema(c, 5) < ema(c, 13)))
    add("GMMA", "short_vs_long", state((ema(c, 3)+ema(c, 5)+ema(c, 8))/3 > (ema(c, 30)+ema(c, 50)+ema(c, 60))/3,
                                        (ema(c, 3)+ema(c, 5)+ema(c, 8))/3 < (ema(c, 30)+ema(c, 50)+ema(c, 60))/3))
    sm = sma(c, 20); add("LinReg_slope", "n=20", state((sm/np.roll(sm, 5)-1) > 0, (sm/np.roll(sm, 5)-1) < 0))

    # ================= OSCILLATORS (mean-reversion: oversold=buy) =================
    for p, os_, ob_ in [(14, 30, 70), (7, 30, 70), (21, 30, 70), (9, 25, 75)]:
        r = rsi(c, p); add("RSI", f"n={p},os={os_}", state(r < os_, r > ob_))
    r2 = rsi(c, 2); add("RSI_Connors", "n=2,os=10", state(r2 < 10, r2 > 90))
    for k_, os_ in [(14, 20), (5, 20), (21, 15)]:
        ll = llv(l, k_); hh = hhv(h, k_); kk = (c-ll)/(hh-ll+1e-9)*100
        add("Stochastic", f"k={k_},os={os_}", state(kk < os_, kk > 100-os_))
    r = rsi(c, 14); sr = (r-llv(r, 14))/(hhv(r, 14)-llv(r, 14)+1e-9)*100; add("StochRSI", "14", state(sr < 20, sr > 80))
    for p in (14, 21):
        hh = hhv(h, p); ll = llv(l, p); wr = -100*(hh-c)/(hh-ll+1e-9); add("Williams_R", f"n={p}", state(wr < -80, wr > -20))
    for p, t_ in [(14, 100), (20, 150), (14, 200)]:
        ma = sma(tp, p); md = pd.Series(np.abs(tp-ma)).rolling(p).mean().values; cc = (tp-ma)/(0.015*md+1e-9)
        add("CCI", f"n={p},t={t_}", state(cc < -t_, cc > t_))
    d14 = np.diff(c, prepend=c[0])
    cmo = (pd.Series(np.clip(d14, 0, None)).rolling(14).sum().values - pd.Series(-np.clip(d14, None, 0)).rolling(14).sum().values)/(pd.Series(np.abs(d14)).rolling(14).sum().values+1e-9)*100
    add("CMO", "n=14", state(cmo < -50, cmo > 50))
    for p in (10, 20):
        roc = pd.Series(c).pct_change(p).values*100; add("ROC", f"n={p}", state(roc > 0, roc < 0))
    add("Momentum", "n=10", state((c-np.roll(c, 10)) > 0, (c-np.roll(c, 10)) < 0))
    bp = c-np.minimum(l, pc); avg = lambda p: pd.Series(bp).rolling(p).sum().values/(pd.Series(tr(h, l, c)).rolling(p).sum().values+1e-9)
    ult = 100*(4*avg(7)+2*avg(14)+avg(28))/7; add("Ultimate_Osc", "7,14,28", state(ult < 30, ult > 70))
    pc1 = np.diff(c, prepend=c[0]); tsi = 100*ema(ema(pc1, 25), 13)/(ema(ema(np.abs(pc1), 25), 13)+1e-9); add("TSI", "25,13", state(tsi > 0, tsi < 0))
    ll14 = llv(l, 14); hh14 = hhv(h, 14); x = np.clip(2*((c-ll14)/(hh14-ll14+1e-9)-0.5), -0.999, 0.999)
    fish = ema(0.5*np.log((1+x)/(1-x)), 5); add("Fisher", "n=14", state(fish < -1, fish > 1))
    med = (h+l)/2; ao = sma(med, 5)-sma(med, 34); add("Awesome_Osc", "5,34", state(ao > 0, ao < 0))
    add("Accelerator_Osc", "5,34", state((ao-sma(ao, 5)) > 0, (ao-sma(ao, 5)) < 0))
    cop = wma(pd.Series(c).pct_change(14).values*100 + pd.Series(c).pct_change(11).values*100, 10); add("Coppock", "14,11,10", state(cop > 0, cop < 0))
    # Connors RSI
    streak = np.zeros(n)
    for i in range(1, n): streak[i] = (streak[i-1]+1 if streak[i-1] > 0 else 1) if c[i] > c[i-1] else ((streak[i-1]-1 if streak[i-1] < 0 else -1) if c[i] < c[i-1] else 0)
    crsi = (rsi(c, 3)+rsi(streak, 2)+pd.Series(c).pct_change().rolling(100).apply(lambda w: (w[:-1] < w[-1]).mean()*100, raw=True).values)/3
    add("Connors_RSI", "3,2,100", state(crsi < 20, crsi > 80))
    add("Elder_Ray", "13", state((h-ema(c, 13)) > 0, (l-ema(c, 13)) < 0))
    add("Balance_of_Power", "", state(((c-o)/(h-l+1e-9)) > 0.3, ((c-o)/(h-l+1e-9)) < -0.3))

    # ================= BANDS / VOLATILITY =================
    for p, k_ in [(20, 2.0), (20, 2.5), (14, 2.0), (30, 2.0)]:
        m = sma(c, p); sd = std(c, p); add("Bollinger", f"n={p},k={k_}", state(c < m-k_*sd, c > m+k_*sd))
    m = sma(c, 20); sd = std(c, 20); pctb = (c-(m-2*sd))/((m+2*sd)-(m-2*sd)+1e-9)
    add("Bollinger_pctB", "20,2", state(pctb < 0.05, pctb > 0.95))
    bw = (4*sd)/(m+1e-9); add("Bollinger_bandwidth", "20,2", state(bw < pd.Series(bw).rolling(50).quantile(0.2).values, bw > pd.Series(bw).rolling(50).quantile(0.8).values))
    add("Bollinger_walk", "20,2", state((c > m+2*sd) & (np.roll(c, 1) > np.roll(m+2*sd, 1)), (c < m-2*sd) & (np.roll(c, 1) < np.roll(m-2*sd, 1))))
    for p, an, k_ in [(20, 10, 2.0), (20, 10, 2.5), (20, 20, 2.0)]:
        e = ema(c, p); a = atr(h, l, c, an); add("Keltner", f"ma={p},atr={an},k={k_}", state(c < e-k_*a, c > e+k_*a))
    for p in (20, 55):
        add("Donchian_break", f"n={p}", state(c > np.roll(hhv(h, p), 1), c < np.roll(llv(l, p), 1)))
    sm5 = sma(c, 5); a14 = atr(h, l, c, 14); add("STARC", "5,14,2", state(c < sm5-2*a14, c > sm5+2*a14))
    acc = 4*(h-l)/(h+l+1e-9); abu = sma(c, 20)*(1+acc); abl = sma(c, 20)*(1-acc); add("Acceleration_bands", "20", state(c < abl, c > abu))
    a14 = atr(h, l, c, 14); add("Chandelier", "22,3", state((c-(hhv(h, 22)-3*a14)) < 0, (c-(hhv(h, 22)-3*a14)) > 3*a14))
    add("ATR_trailing_stop", "14,3", state(c < hhv(c, 10)-3*a14, c > llv(c, 10)+3*a14))
    dd = 1-c/hhv(c, 14); ulcer = np.sqrt(pd.Series(dd**2).rolling(14).mean().values)*100; add("Ulcer_Index", "14", state(ulcer > pd.Series(ulcer).rolling(50).quantile(0.8).values, ulcer < pd.Series(ulcer).rolling(50).quantile(0.2).values))
    atrp = atr(h, l, c, 14)/c*100; add("ATR_pct", "14", state(atrp > pd.Series(atrp).rolling(50).quantile(0.8).values, atrp < pd.Series(atrp).rolling(50).quantile(0.2).values))
    hv = pd.Series(c).pct_change().rolling(20).std().values*100; add("Hist_Volatility", "20", state(hv < pd.Series(hv).rolling(50).quantile(0.2).values, hv > pd.Series(hv).rolling(50).quantile(0.8).values))
    add("Mass_Index", "9,25", state(pd.Series(ema(h-l, 9)/(ema(ema(h-l, 9), 9)+1e-9)).rolling(25).sum().values > 27, pd.Series(ema(h-l, 9)/(ema(ema(h-l, 9), 9)+1e-9)).rolling(25).sum().values < 26.5))
    ci = 100*np.log10(pd.Series(tr(h, l, c)).rolling(14).sum().values/(hhv(h, 14)-llv(l, 14)+1e-9))/np.log10(14); add("Choppiness", "14", state(ci > 61.8, ci < 38.2))

    # ================= VOLUME =================
    sgn = np.sign(np.diff(c, prepend=c[0])); obv = np.cumsum(sgn*v)
    for p in (5, 10, 20): add("OBV_slope", f"n={p}", state((obv-np.roll(obv, p)) > 0, (obv-np.roll(obv, p)) < 0))
    mfm = ((c-l)-(h-c))/(h-l+1e-9); mfv = mfm*v
    for p, t_ in [(20, 0.05), (20, 0.1)]:
        cmf = pd.Series(mfv).rolling(p).sum().values/(pd.Series(v).rolling(p).sum().values+1e-9); add("CMF", f"n={p},t={t_}", state(cmf > t_, cmf < -t_))
    ad = np.cumsum(mfv); add("AccDist_slope", "10", state((ad-np.roll(ad, 10)) > 0, (ad-np.roll(ad, 10)) < 0))
    add("Chaikin_Osc", "3,10", state((ema(ad, 3)-ema(ad, 10)) > 0, (ema(ad, 3)-ema(ad, 10)) < 0))
    mf = tp*v; pos = np.where(tp > np.roll(tp, 1), mf, 0.0); neg = np.where(tp < np.roll(tp, 1), mf, 0.0)
    for p, os_ in [(14, 20), (14, 25)]:
        mfi = 100-100/(1+pd.Series(pos).rolling(p).sum().values/(pd.Series(neg).rolling(p).sum().values+1e-9)); add("MFI", f"n={p},os={os_}", state(mfi < os_, mfi > 100-os_))
    fi = ema(np.diff(c, prepend=c[0])*v, 13); add("Force_Index", "13", state(fi > 0, fi < 0))
    eom = pd.Series(((h+l)/2-(np.roll(h, 1)+np.roll(l, 1))/2)*(h-l)/(v+1e-9)).rolling(14).mean().values; add("Ease_of_Movement", "14", state(eom > 0, eom < 0))
    add("Volume_ROC", "10", state(pd.Series(v).pct_change(10).values > 0.5, pd.Series(v).pct_change(10).values < -0.5))
    vo = (sma(v, 5)-sma(v, 20))/(sma(v, 20)+1e-9)*100; add("Volume_Osc", "5,20", state(vo > 20, vo < -20))
    nvi = np.ones(n); pvi = np.ones(n)
    for i in range(1, n):
        r_ = c[i]/c[i-1] if c[i-1] else 1
        nvi[i] = nvi[i-1]*r_ if v[i] < v[i-1] else nvi[i-1]; pvi[i] = pvi[i-1]*r_ if v[i] > v[i-1] else pvi[i-1]
    add("NVI", "", state(nvi > ema(nvi, 255), nvi < ema(nvi, 255)))
    add("PVI", "", state(pvi > ema(pvi, 255), pvi < ema(pvi, 255)))
    pvt = np.cumsum(pd.Series(c).pct_change().fillna(0).values*v); add("PVT_slope", "10", state((pvt-np.roll(pvt, 10)) > 0, (pvt-np.roll(pvt, 10)) < 0))
    add("Volume_spike", "20,2", state((v > 2*sma(v, 20)) & (c > o), (v > 2*sma(v, 20)) & (c < o)))

    # ================= STRUCTURE / PIVOTS / FIB =================
    P = (np.roll(h, 1)+np.roll(l, 1)+np.roll(c, 1))/3; rng1 = np.roll(h, 1)-np.roll(l, 1)
    add("Pivot_classic", "P", state(c < 2*P-np.roll(h, 1), c > 2*P-np.roll(l, 1)))            # below S1 / above R1
    add("Pivot_R2_S2", "", state(l <= P-rng1*0.618, h >= P+rng1*0.618))
    add("Pivot_R3_S3", "", state(l <= P-rng1*1.0, h >= P+rng1*1.0))
    add("Camarilla", "", state(l <= np.roll(c, 1)-rng1*1.1/6, h >= np.roll(c, 1)+rng1*1.1/6))
    cpr_w = np.abs(P-(np.roll(h, 1)+np.roll(l, 1))/2)*2/c*100; add("CPR_width", "", state(cpr_w < pd.Series(cpr_w).rolling(50).quantile(0.2).values, cpr_w > pd.Series(cpr_w).rolling(50).quantile(0.8).values))
    for p in (20, 60):
        add("NDay_breakout", f"n={p}", state(c >= hhv(h, p), c <= llv(l, p)))
    for lvl in (0.382, 0.5, 0.618):
        hh = hhv(h, 60); ll = llv(l, 60); fib = hh-(hh-ll)*lvl; add("Fib_retrace", f"{lvl}", state((np.roll(c, 1) > fib) & (c <= fib), (np.roll(c, 1) < fib) & (c >= fib)))
    add("Darvas_box", "20", state(c > np.roll(hhv(h, 20), 1), c < np.roll(llv(l, 20), 1)))
    pos20 = (c-llv(l, 20))/(hhv(h, 20)-llv(l, 20)+1e-9); add("Range_position", "20", state(pos20 < 0.1, pos20 > 0.9))
    # TD setup
    td = np.zeros(n)
    for i in range(4, n): td[i] = (td[i-1]+1 if td[i-1] > 0 else 1) if c[i] > c[i-4] else ((td[i-1]-1 if td[i-1] < 0 else -1) if c[i] < c[i-4] else 0)
    add("TD_setup", "9", state(td <= -9, td >= 9))

    # ================= PRICE ACTION =================
    body = (c-o); rng = (h-l)+1e-9
    add("Candle_body", "", state(body/rng > 0.6, body/rng < -0.6))
    add("Bullish_bearish_engulf", "", state((c > np.roll(h, 1)) & (o < np.roll(l, 1)), (c < np.roll(l, 1)) & (o > np.roll(h, 1))))
    add("Hammer_shooting", "", state(((np.minimum(o, c)-l)/rng > 0.6) & (c > o), ((h-np.maximum(o, c))/rng > 0.6) & (c < o)))
    add("Inside_bar", "", state((h < np.roll(h, 1)) & (l > np.roll(l, 1)) & (c > o), (h < np.roll(h, 1)) & (l > np.roll(l, 1)) & (c < o)))
    add("Outside_bar", "", state((h > np.roll(h, 1)) & (l < np.roll(l, 1)) & (c > o), (h > np.roll(h, 1)) & (l < np.roll(l, 1)) & (c < o)))
    gap = (o-pc)/pc*100; add("Gap", "", state(gap < -1.0, gap > 1.0))
    add("HigherHigh_LowerLow", "", state((h > np.roll(h, 1)) & (l > np.roll(l, 1)), (h < np.roll(h, 1)) & (l < np.roll(l, 1))))
    cloc = (c-l)/rng; add("Close_location", "", state(cloc < 0.2, cloc > 0.8))
    add("ThreeBar_reversal", "", state((c > np.roll(h, 1)) & (np.roll(c, 1) < np.roll(c, 2)), (c < np.roll(l, 1)) & (np.roll(c, 1) > np.roll(c, 2))))
    return R
