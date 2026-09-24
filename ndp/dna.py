# -*- coding: utf-8 -*-
"""Indicator-DNA engine — per-indicator, per-stock PARAMETRIC optimization (the user's framework).
Every indicator family has a parameter grid; the engine discovers each stock's own best settings, retires
indicators with no edge, then combines survivors. Leak-free EXPANDING WEEKLY walk-forward. Outcome = next
day OPEN->CLOSE realized return. Objective = EXPECTED RETURN first, WIN RATE as a qualifying gate:
  Buy: E[oc]>=+0.5% & WR(oc>=+0.5%)>=70%   StrongBuy: E>=+1.0% & WR(>=+1.0%)>=75%
  Sell: E[-oc]>=+0.5% & WR(oc<=-0.5%)>=70%  StrongSell: E<=-1.0% & WR(oc<=-1.0%)>=75%
Signals precomputed once per (family,params) over full history (causal), so the walk-forward is fast.
"""
import os, sys, sqlite3, itertools, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAILY = os.path.join(ROOT, "data", "db", "kanida_universe.db")
MIN_SIG = 12          # min signals in a training window to consider a config
MIN_TRAIN_WK = 8      # weeks of history before the first live prediction
CATS = [("Buy", +1, 0.5, 0.70), ("StrongBuy", +1, 1.0, 0.75),
        ("Sell", -1, 0.5, 0.70), ("StrongSell", -1, 1.0, 0.75)]

# ------------------------------------------------------------------ helpers (all causal)
def ema(s, n): return pd.Series(s).ewm(span=n, adjust=False).mean().values
def sma(s, n): return pd.Series(s).rolling(n).mean().values
def rma(s, n): return pd.Series(s).ewm(alpha=1/n, adjust=False).mean().values
def rsi(c, n):
    d = np.diff(c, prepend=c[0]); up = rma(np.clip(d, 0, None), n); dn = rma(-np.clip(d, None, 0), n)
    return 100 - 100/(1 + up/(dn + 1e-9))
def true_range(h, l, c):
    pc = np.roll(c, 1); pc[0] = c[0]
    return np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc)))
def atr(h, l, c, n): return rma(true_range(h, l, c), n)


# ------------------------------------------------------------------ indicator families
# each family: name -> (param_grid, signal_fn(D, p) -> np.array of {+1,-1,0})
# D = dict of o,h,l,c,v arrays. +1 = long trigger, -1 = short trigger, 0 = none.
def _cross(fast, slow):
    up = (fast > slow) & (np.roll(fast, 1) <= np.roll(slow, 1))
    dn = (fast < slow) & (np.roll(fast, 1) >= np.roll(slow, 1))
    s = np.zeros(len(fast)); s[up] = 1; s[dn] = -1; return s

def build_families(intraday=False):
    F = {}
    D_KEYS = ID_COLS if intraday else []
    F["ema_cross"] = ([dict(f=f, s=s) for f in (3, 5, 7, 9, 12) for s in (18, 20, 26, 34, 50) if f < s],
                      lambda D, p: _cross(ema(D["c"], p["f"]), ema(D["c"], p["s"])))
    F["sma_cross"] = ([dict(f=f, s=s) for f in (5, 10, 20) for s in (20, 50, 100, 200) if f < s],
                      lambda D, p: _cross(sma(D["c"], p["f"]), sma(D["c"], p["s"])))
    F["price_vs_sma"] = ([dict(n=n) for n in (10, 20, 50, 100, 200)],
                         lambda D, p: np.where(D["c"] > sma(D["c"], p["n"]), 1, -1))
    F["price_vs_ema"] = ([dict(n=n) for n in (9, 21, 34, 50)],
                         lambda D, p: np.where(D["c"] > ema(D["c"], p["n"]), 1, -1))
    def ma_touch(D, p):                                  # bounce off MA (mean-reversion long / rejection short)
        m = sma(D["c"], p["n"]); s = np.zeros(len(m))
        s[(D["l"] <= m) & (D["c"] > m)] = 1; s[(D["h"] >= m) & (D["c"] < m)] = -1; return s
    F["ma_touch"] = ([dict(n=n) for n in (7, 9, 10, 15, 20, 21, 50)], ma_touch)
    def macd_sig(D, p):
        macd = ema(D["c"], p["f"]) - ema(D["c"], p["s"]); sigl = ema(macd, p["g"])
        return _cross(macd, sigl)
    F["macd"] = ([dict(f=f, s=s, g=9) for (f, s) in [(12, 26), (8, 21), (5, 34), (10, 30)]] +
                 [dict(f=12, s=26, g=g) for g in (5, 7, 12)], macd_sig)
    def rsi_mr(D, p):
        r = rsi(D["c"], p["n"]); s = np.zeros(len(r)); s[r < p["os"]] = 1; s[r > p["ob"]] = -1; return s
    F["rsi_mr"] = ([dict(n=n, os=o, ob=100-o) for n in (5, 7, 9, 14, 21) for o in (20, 25, 30, 35)], rsi_mr)
    def stoch(D, p):
        ll = pd.Series(D["l"]).rolling(p["k"]).min().values; hh = pd.Series(D["h"]).rolling(p["k"]).max().values
        k = (D["c"] - ll)/(hh - ll + 1e-9)*100; s = np.zeros(len(k)); s[k < p["os"]] = 1; s[k > 100-p["os"]] = -1; return s
    F["stoch"] = ([dict(k=k, os=o) for k in (9, 14, 21) for o in (15, 20, 25)], stoch)
    def cci(D, p):
        tp = (D["h"]+D["l"]+D["c"])/3; ma = sma(tp, p["n"]); md = pd.Series(np.abs(tp-ma)).rolling(p["n"]).mean().values
        c = (tp-ma)/(0.015*md+1e-9); s = np.zeros(len(c)); s[c < -p["t"]] = 1; s[c > p["t"]] = -1; return s
    F["cci"] = ([dict(n=n, t=t) for n in (14, 20) for t in (100, 150, 200)], cci)
    def williams(D, p):
        hh = pd.Series(D["h"]).rolling(p["n"]).max().values; ll = pd.Series(D["l"]).rolling(p["n"]).min().values
        wr = -100*(hh-D["c"])/(hh-ll+1e-9); s = np.zeros(len(wr)); s[wr < -p["t"]] = 1; s[wr > -(100-p["t"])] = -1; return s
    F["williams"] = ([dict(n=n, t=t) for n in (10, 14, 21) for t in (80, 85, 90)], williams)
    def bollinger(D, p):                                  # rich: separate ma len, std ZONES, breach/re-entry
        m = sma(D["c"], p["ma"]); sd = pd.Series(D["c"]).rolling(p["ma"]).std().values
        stretch = (D["c"] - m) / (sd + 1e-9); z = p["z"]; s = np.zeros(len(m))
        if p["mode"] == "breach":
            s[stretch <= -z] = 1; s[stretch >= z] = -1
        else:                                             # re-entry: yesterday beyond zone, today back inside
            pv = np.roll(stretch, 1); s[(pv <= -z) & (stretch > -z)] = 1; s[(pv >= z) & (stretch < z)] = -1
        s[np.isnan(stretch)] = 0; return s
    F["bollinger"] = ([dict(ma=ma, z=z, mode=md) for ma in (14, 20, 30) for z in (1.5, 2.0, 2.5, 3.0)
                       for md in ("breach", "reentry")], bollinger)
    def donch_break(D, p):
        hh = pd.Series(D["h"]).rolling(p["n"]).max().shift(1).values; ll = pd.Series(D["l"]).rolling(p["n"]).min().shift(1).values
        s = np.zeros(len(hh)); s[D["c"] > hh] = 1; s[D["c"] < ll] = -1; return s
    F["donchian_break"] = ([dict(n=n) for n in (10, 20, 33, 55)], donch_break)
    def adx_di(D, p):
        n = p["n"]; up = np.diff(D["h"], prepend=D["h"][0]); dn = -np.diff(D["l"], prepend=D["l"][0])
        pdm = np.where((up > dn) & (up > 0), up, 0.0); mdm = np.where((dn > up) & (dn > 0), dn, 0.0)
        a = atr(D["h"], D["l"], D["c"], n) + 1e-9; pdi = 100*rma(pdm, n)/a; mdi = 100*rma(mdm, n)/a
        adx = rma(100*np.abs(pdi-mdi)/(pdi+mdi+1e-9), n)
        s = np.zeros(len(a)); s[(pdi > mdi) & (adx > p["t"])] = 1; s[(mdi > pdi) & (adx > p["t"])] = -1; return s
    F["adx_di"] = ([dict(n=n, t=t) for n in (14, 21) for t in (20, 25, 30)], adx_di)
    def supertrend(D, p):
        n, m = p["n"], p["m"]; a = atr(D["h"], D["l"], D["c"], n); hl2 = (D["h"]+D["l"])/2
        ub = hl2 + m*a; lb = hl2 - m*a; st = np.zeros(len(a)); dirn = np.ones(len(a)); c = D["c"]
        for i in range(1, len(a)):
            if np.isnan(ub[i]): st[i] = c[i]; continue
            dirn[i] = 1 if c[i] > st[i-1] else (-1 if c[i] < st[i-1] else dirn[i-1])
            st[i] = lb[i] if dirn[i] == 1 else ub[i]
        return _cross(c, st)
    F["supertrend"] = ([dict(n=n, m=m) for n in (7, 10, 14) for m in (2.0, 3.0)], supertrend)
    def aroon(D, p):
        n = p["n"]; au = pd.Series(D["h"]).rolling(n).apply(lambda x: (n-1-np.argmax(x))/n*100, raw=True).values
        ad = pd.Series(D["l"]).rolling(n).apply(lambda x: (n-1-np.argmin(x))/n*100, raw=True).values
        return np.where(au > ad, 1, -1)
    F["aroon"] = ([dict(n=n) for n in (14, 25)], aroon)
    def roc_mom(D, p):
        r = pd.Series(D["c"]).pct_change(p["n"]).values*100; s = np.zeros(len(r))
        if p["mode"] == "mom": s[r > p["t"]] = 1; s[r < -p["t"]] = -1
        else: s[r < -p["t"]] = 1; s[r > p["t"]] = -1                     # reversion
        return s
    F["roc"] = ([dict(n=n, t=t, mode=md) for n in (5, 10, 20) for t in (2, 4) for md in ("mom", "rev")], roc_mom)
    def mfi(D, p):
        tp = (D["h"]+D["l"]+D["c"])/3; mf = tp*D["v"]; pos = np.where(tp > np.roll(tp, 1), mf, 0.0); neg = np.where(tp < np.roll(tp, 1), mf, 0.0)
        pr = pd.Series(pos).rolling(p["n"]).sum().values; nr = pd.Series(neg).rolling(p["n"]).sum().values
        m = 100 - 100/(1 + pr/(nr+1e-9)); s = np.zeros(len(m)); s[m < p["os"]] = 1; s[m > 100-p["os"]] = -1; return s
    F["mfi"] = ([dict(n=n, os=o) for n in (10, 14) for o in (20, 25)], mfi)
    def cmf(D, p):
        mfm = ((D["c"]-D["l"])-(D["h"]-D["c"]))/(D["h"]-D["l"]+1e-9); mfv = mfm*D["v"]
        c = pd.Series(mfv).rolling(p["n"]).sum().values/(pd.Series(D["v"]).rolling(p["n"]).sum().values+1e-9)
        s = np.zeros(len(c)); s[c > p["t"]] = 1; s[c < -p["t"]] = -1; return s
    F["cmf"] = ([dict(n=n, t=t) for n in (20,) for t in (0.05, 0.1, 0.15)], cmf)
    def obv_slope(D, p):
        sgn = np.sign(np.diff(D["c"], prepend=D["c"][0])); obv = np.cumsum(sgn*D["v"])
        sl = obv - np.roll(obv, p["n"]); return np.where(sl > 0, 1, -1)
    F["obv"] = ([dict(n=n) for n in (5, 10, 20)], obv_slope)
    def vortex(D, p):
        n = p["n"]; tr = true_range(D["h"], D["l"], D["c"]); pc = np.roll(D["c"], 1)
        vp = np.abs(D["h"]-np.roll(D["l"], 1)); vm = np.abs(D["l"]-np.roll(D["h"], 1))
        vi = (pd.Series(vp).rolling(n).sum().values - pd.Series(vm).rolling(n).sum().values)/(pd.Series(tr).rolling(n).sum().values+1e-9)
        return np.where(vi > 0, 1, -1)
    F["vortex"] = ([dict(n=n) for n in (14, 21)], vortex)
    def psar(D, p):
        h, l, c = D["h"], D["l"], D["c"]; n = len(c); s = np.zeros(n); af0, afmax = p["step"], p["max"]
        bull = True; af = af0; ep = h[0]; sar = l[0]
        for i in range(1, n):
            sar = sar + af*(ep - sar)
            if bull:
                if l[i] < sar: bull = False; sar = ep; ep = l[i]; af = af0
                elif h[i] > ep: ep = h[i]; af = min(af+af0, afmax)
            else:
                if h[i] > sar: bull = True; sar = ep; ep = h[i]; af = af0
                elif l[i] < ep: ep = l[i]; af = min(af+af0, afmax)
            s[i] = 1 if bull else -1
        return s
    F["psar"] = ([dict(step=st, max=0.2) for st in (0.02, 0.03)], psar)
    def pivot_rev(D, p):                                  # prior-day pivots: bounce at S-levels / reject at R-levels
        ph, pl, pc = np.roll(D["h"], 1), np.roll(D["l"], 1), np.roll(D["c"], 1)
        P = (ph+pl+pc)/3; rng = ph-pl
        lvl = {"1": (P+rng*0.382, P-rng*0.382), "2": (P+rng*0.618, P-rng*0.618), "3": (P+rng*1.0, P-rng*1.0)}
        R, S = lvl[p["lvl"]]; s = np.zeros(len(P)); s[D["l"] <= S] = 1; s[D["h"] >= R] = -1; return s
    F["pivot_rev"] = ([dict(lvl=lv) for lv in ("1", "2", "3")], pivot_rev)
    def gap_react(D, p):
        g = (D["o"]-np.roll(D["c"], 1))/np.roll(D["c"], 1)*100; s = np.zeros(len(g))
        if p["mode"] == "fade": s[g < -p["t"]] = 1; s[g > p["t"]] = -1
        else: s[g > p["t"]] = 1; s[g < -p["t"]] = -1
        return s
    F["gap"] = ([dict(t=t, mode=md) for t in (0.5, 1.0) for md in ("fade", "cont")], gap_react)
    def nday_pos(D, p):                                   # position in N-day range: reversion from extremes
        hh = pd.Series(D["h"]).rolling(p["n"]).max().values; ll = pd.Series(D["l"]).rolling(p["n"]).min().values
        pos = (D["c"]-ll)/(hh-ll+1e-9); s = np.zeros(len(pos)); s[pos < 0.1] = 1; s[pos > 0.9] = -1; return s
    F["nday_range"] = ([dict(n=n) for n in (20, 40, 60)], nday_pos)
    def trix_sig(D, p):
        t = ema(ema(ema(D["c"], p["n"]), p["n"]), p["n"]); tr = pd.Series(t).pct_change().values*100
        return np.where(tr > 0, 1, -1)
    F["trix"] = ([dict(n=n) for n in (9, 15, 18)], trix_sig)
    def keltner(D, p):                                    # rich: SEPARATE ma & atr lengths, ATR ZONES, breach/re-entry
        m = ema(D["c"], p["ma"]); a = atr(D["h"], D["l"], D["c"], p["atr"])
        stretch = (D["c"] - m) / (a + 1e-9); z = p["z"]; s = np.zeros(len(m))
        if p["mode"] == "breach":                         # close z-ATRs or more beyond the mean
            s[stretch <= -z] = 1; s[stretch >= z] = -1
        else:                                             # re-entry: was beyond z-ATR, now snapped back inside
            pv = np.roll(stretch, 1); s[(pv <= -z) & (stretch > -z)] = 1; s[(pv >= z) & (stretch < z)] = -1
        s[np.isnan(stretch)] = 0; return s
    F["keltner"] = ([dict(ma=ma, atr=at, z=z, mode=md) for ma in (10, 20, 34) for at in (10, 14, 20)
                     for z in (1.5, 2.0, 2.5, 3.0) for md in ("breach", "reentry")], keltner)
    def awesome(D, p):
        med = (D["h"]+D["l"])/2; ao = sma(med, p["f"]) - sma(med, p["s"]); return _cross(ao, np.zeros_like(ao))
    F["awesome"] = ([dict(f=f, s=s) for (f, s) in [(5, 34), (5, 20), (8, 21)]], awesome)

    # ---- previous-day INTRADAY-structure families (only fire where 15m/5m data exists: 2024-05+) ----
    if "id_vwap_dev" in D_KEYS:
        def _thr(col, t, mode):                          # generic threshold family on an intraday scalar
            def fn(D, p):
                x = D[col]; s = np.zeros(len(x))
                if p["mode"] == "trend": s[x > p["t"]] = 1; s[x < -p["t"]] = -1
                else: s[x > p["t"]] = -1; s[x < -p["t"]] = 1     # reversion (strong close -> fade next day)
                s[np.isnan(x)] = 0; return s
            return fn
        F["id_vwap"] = ([dict(col="id_vwap_dev", t=t, mode=md) for t in (0.1, 0.3) for md in ("trend", "rev")], _thr("id_vwap_dev", 0, 0))
        F["id_lasthour"] = ([dict(col="id_lasthour_ret", t=t, mode=md) for t in (0.1, 0.25) for md in ("trend", "rev")], _thr("id_lasthour_ret", 0, 0))
        F["id_morning"] = ([dict(col="id_morning_ret", t=t, mode=md) for t in (0.3, 0.6) for md in ("trend", "rev")], _thr("id_morning_ret", 0, 0))
        F["id_afternoon"] = ([dict(col="id_afternoon_ret", t=t, mode=md) for t in (0.2, 0.4) for md in ("trend", "rev")], _thr("id_afternoon_ret", 0, 0))
        def id_closepos(D, p):                            # closed weak (bottom of range) -> bounce; strong -> fade
            x = D["id_close_pos"]; s = np.zeros(len(x)); s[x < p["lo"]] = 1; s[x > 1-p["lo"]] = -1; s[np.isnan(x)] = 0; return s
        F["id_closepos"] = ([dict(lo=lo) for lo in (0.15, 0.25, 0.35)], id_closepos)
        def id_breadth(D, p):
            x = D["id_breadth"]; s = np.zeros(len(x))
            if p["mode"] == "trend": s[x > p["t"]] = 1; s[x < -p["t"]] = -1
            else: s[x > p["t"]] = -1; s[x < -p["t"]] = 1
            s[np.isnan(x)] = 0; return s
        F["id_breadth"] = ([dict(t=t, mode=md) for t in (0.1, 0.2) for md in ("trend", "rev")], id_breadth)
        def id_vwapfrac(D, p):
            x = D["id_above_vwap_frac"]; s = np.zeros(len(x)); s[x > p["hi"]] = 1 if p["mode"] == "trend" else -1
            s[x < 1-p["hi"]] = -1 if p["mode"] == "trend" else 1; s[np.isnan(x)] = 0; return s
        F["id_vwapfrac"] = ([dict(hi=h, mode=md) for h in (0.65, 0.75) for md in ("trend", "rev")], id_vwapfrac)
    return F


# ------------------------------------------------------------------ data + precompute
ID_COLS = ["id_vwap_dev", "id_close_pos", "id_range", "id_morning_ret", "id_afternoon_ret",
           "id_lasthour_ret", "id_breadth", "id_above_vwap_frac", "id_ampm_vol"]


def load(sym, with_intraday=False):
    con = sqlite3.connect("file:" + DAILY.replace("\\", "/") + "?mode=ro", uri=True)
    g = pd.read_sql_query("SELECT trade_date,open,high,low,close,volume FROM ohlc_daily WHERE symbol=? "
                          "AND trade_date>='2016-01-01' ORDER BY trade_date", con, params=[sym]); con.close()
    D = dict(o=g.open.values.astype(float), h=g.high.values.astype(float), l=g.low.values.astype(float),
             c=g.close.values.astype(float), v=g.volume.values.astype(float))
    o1 = np.roll(D["o"], -1); c1 = np.roll(D["c"], -1)          # next-day open->close
    oc = (c1 - o1) / o1 * 100.0; oc[-1] = np.nan
    dates = g.trade_date.values
    wk = pd.to_datetime(g.trade_date).dt.strftime("%G%V").values
    if with_intraday:
        from ndp import intraday_features as IDF
        idf = IDF.build(sym).set_index("date")
        for col in ID_COLS:
            m = idf[col].reindex(g.trade_date.values).values if col in idf.columns else np.full(len(g), np.nan)
            D[col] = m.astype(float)
        D["_has_intraday"] = True
    return D, oc, dates, wk


def precompute(D, F):
    """All (family,param) -> signal series, once. Returns list of (family, pstr, params, sig)."""
    sigs = []
    for fam, (grid, fn) in F.items():
        for p in grid:
            s = fn(D, p).astype(float)
            pstr = ",".join(f"{k}={v}" for k, v in p.items())
            sigs.append((fam, pstr, p, s))
    return sigs


# ------------------------------------------------------------------ walk-forward (expanding, weekly)
def _cumstats(sig, oc):
    """Cumulative long/short counts, return-sums and hit-counts so any train-prefix metric is O(1)."""
    valid = ~np.isnan(oc); L = (sig == 1) & valid; S = (sig == -1) & valid
    d = dict(
        Ln=np.cumsum(L), Lr=np.cumsum(np.where(L, oc, 0.0)),
        Lh05=np.cumsum(L & (oc >= 0.5)), Lh10=np.cumsum(L & (oc >= 1.0)),
        Sn=np.cumsum(S), Sr=np.cumsum(np.where(S, -oc, 0.0)),
        Sh05=np.cumsum(S & (oc <= -0.5)), Sh10=np.cumsum(S & (oc <= -1.0)))
    return d


def walk_forward(sym, with_intraday=False, start=None):
    D, oc, dates, wk = load(sym, with_intraday=with_intraday)
    F = build_families(intraday=with_intraday); sigs = precompute(D, F)
    cum = {i: _cumstats(s, oc) for i, (_, _, _, s) in enumerate(sigs)}
    fam_cfgs = {}
    for i, (fam, pstr, p, s) in enumerate(sigs): fam_cfgs.setdefault(fam, []).append(i)
    weeks = sorted(set(wk)); widx = {w: np.where(wk == w)[0] for w in weeks}
    recs = []                                        # OOS records: family, cat, date, ret_dir, hit, oc, params
    for wi in range(MIN_TRAIN_WK, len(weeks)):
        tw = weeks[wi]; test_i = widx[tw]; a = test_i[0]
        if a == 0: continue
        if start is not None and dates[a] < start: continue     # restrict evaluation era (fair intraday compare)
        pe = a - 1                                    # prefix end (train = [0, a))
        for cat, direction, bar, gate in CATS:
            side = "L" if direction == 1 else "S"
            hk = f"{side}h{'05' if bar == 0.5 else '10'}"
            for fam, cfg_ids in fam_cfgs.items():
                best = None
                for i in cfg_ids:
                    c = cum[i]; n = c[f"{side}n"][pe]
                    if n < MIN_SIG: continue
                    E = c[f"{side}r"][pe] / n; WR = c[hk][pe] / n
                    ok = WR >= gate
                    score = (1, E) if ok else (0, E)          # WR-gate first, then max expected return
                    if best is None or score > best[0]: best = (score, i, E, WR, n)
                if best is None: continue
                i = best[1]; s = sigs[i][3]; pstr = sigs[i][1]
                for j in test_i:
                    if s[j] != direction or np.isnan(oc[j]): continue
                    rd = oc[j] if direction == 1 else -oc[j]
                    recs.append((fam, cat, dates[j], round(rd, 3), int(rd >= bar), round(oc[j], 3), pstr))
    R = pd.DataFrame(recs, columns=["family", "cat", "date", "ret_dir", "hit", "oc", "params"])
    return R, D, oc, dates, wk


def base_rates(oc):
    v = oc[~np.isnan(oc)]
    return {"Buy": (v >= 0.5).mean(), "StrongBuy": (v >= 1.0).mean(),
            "Sell": (v <= -0.5).mean(), "StrongSell": (v <= -1.0).mean()}


GATE = {"Buy": (0.5, 0.70), "StrongBuy": (1.0, 0.75), "Sell": (0.5, 0.70), "StrongSell": (1.0, 0.75)}


def evaluate(R, base):
    """Per (family,cat) OOS aggregate + qualification against the expected-return-first + WR gate."""
    rows = []
    for (fam, cat), d in R.groupby(["family", "cat"]):
        n = len(d)
        if n < 20: continue
        E = d.ret_dir.mean(); WR = d.hit.mean(); bar, gate = GATE[cat]
        qualified = (E >= bar) and (WR >= gate)
        rows.append(dict(family=fam, cat=cat, n=n, E_ret=round(E, 3), WR=round(WR*100, 1),
                         base=round(base[cat]*100, 1), bar=bar, wr_gate=int(gate*100),
                         edge_vs_base=round((WR - base[cat])*100, 1),
                         qualified="YES" if qualified else "no",
                         common_params=d.params.mode().iloc[0] if len(d.params.mode()) else ""))
    return pd.DataFrame(rows).sort_values(["cat", "E_ret"], ascending=[True, False])


def combine(R, sc, base):
    """Phase B: among SURVIVING families per category (positive edge vs base), require K to agree on a day."""
    out = []
    for cat, (bar, gate) in GATE.items():
        surv = sc[(sc.cat == cat) & (sc.edge_vs_base > 0)].family.tolist()
        if len(surv) < 2: continue
        d = R[(R.cat == cat) & (R.family.isin(surv))]
        by_day = d.groupby("date").agg(k=("family", "nunique"), oc=("oc", "first")).reset_index()
        for K in (2, 3):
            sel = by_day[by_day.k >= K]
            if len(sel) < 20: continue
            rd = sel.oc if cat in ("Buy", "StrongBuy") else -sel.oc
            E = rd.mean(); WR = (rd >= bar).mean()
            out.append(dict(cat=cat, K=K, n=len(sel), E_ret=round(E, 3), WR=round(WR*100, 1),
                            base=round(base[cat]*100, 1), qualified="YES" if (E >= bar and WR >= gate) else "no"))
    return pd.DataFrame(out)


def main(sym="ICICIBANK", with_intraday=False, start=None):
    tag = "DAILY+INTRADAY" if with_intraday else "DAILY-ONLY"
    print("="*84); print(f"INDICATOR-DNA ENGINE — {sym}  [{tag}]  (open->close; expected-return-first + WR gate)")
    if start: print(f"evaluation era restricted to >= {start}")
    print("="*84)
    R, D, oc, dates, wk = walk_forward(sym, with_intraday=with_intraday, start=start)
    base = base_rates(oc if start is None else oc[dates >= start])   # base on the evaluation era
    print(f"OOS records {len(R):,}  ·  period {R.date.min()} -> {R.date.max()}  ·  families {R.family.nunique()}")
    print(f"base rates (open->close): Buy>=.5 {base['Buy']*100:.1f}%  SB>=1 {base['StrongBuy']*100:.1f}%  "
          f"Sell<=-.5 {base['Sell']*100:.1f}%  SS<=-1 {base['StrongSell']*100:.1f}%")
    sc = evaluate(R, base)
    print("\n--- per-indicator OOS performance, best config per stock (top 6 by E[ret] per category) ---")
    for cat, _, _, _ in CATS:
        q = sc[sc.cat == cat].head(6)
        print(f"\n{cat}  [gate: E>={GATE[cat][0]}%  WR>={int(GATE[cat][1]*100)}%   base {base[cat]*100:.1f}%]")
        if q.empty: print("   (no family had >=20 OOS signals)"); continue
        print(f"   {'family':<16}{'n':>5}{'E_ret':>7}{'WR':>7}{'edge':>7}  qual  common_params")
        for _, r in q.iterrows():
            print(f"   {r.family:<16}{r.n:>5}{r.E_ret:>+7.2f}{r.WR:>6.1f}%{r.edge_vs_base:>+6.1f}  {r.qualified:<4}  {r.common_params}")
    # retirement
    surv = sc[sc.edge_vs_base > 0].family.unique(); allfam = R.family.unique()
    retired = [f for f in allfam if f not in surv]
    print(f"\n--- indicator retirement (no positive edge vs base in ANY category) ---")
    print(f"   surviving: {len(surv)}  ·  retired: {len(retired)} -> {', '.join(retired) if retired else '(none)'}")
    cb = combine(R, sc, base)
    print("\n--- Phase B: combinations of surviving indicators (K agree) ---")
    print(cb.to_string(index=False) if not cb.empty else "   (insufficient survivors to combine)")
    nqual = int((sc.qualified == "YES").sum()) + (int((cb.qualified == "YES").sum()) if not cb.empty else 0)
    verdict = "QUALIFYING PATTERN(S) FOUND" if nqual > 0 else "NO PATTERN CLEARS THE GATE (honest null)"
    print("\n" + "="*84); print(f"VERDICT: {verdict}   (individual qualified={int((sc.qualified=='YES').sum())})"); print("="*84)
    xls = os.path.join(os.path.expanduser("~"), "Downloads", f"DNA_{sym}{'_intraday' if with_intraday else ''}.xlsx")
    with pd.ExcelWriter(xls, engine="openpyxl") as w:
        sc.to_excel(w, "per_indicator", index=False)
        (cb if not cb.empty else pd.DataFrame([{"note": "no combos"}])).to_excel(w, "combinations", index=False)
        R.to_excel(w, "oos_signals", index=False)
    print(f"saved -> {xls}")


if __name__ == "__main__":
    import sys as _s
    args = [a for a in _s.argv[1:] if not a.startswith("--")]
    sym = args[0].upper() if args else "ICICIBANK"
    intr = "--intraday" in _s.argv
    st = "2024-05-13" if (intr or "--recent" in _s.argv) else None
    main(sym, with_intraday=intr, start=st)
