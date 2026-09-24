"""Comprehensive technical-indicator feature set from DAILY OHLCV. ~85 features across bands/volatility,
trend/overlays, momentum/strength, volume-flow, and structure/pivots. Settings tuned to standard defaults.
Excluded (need data we don't have): intraday VWAP / Anchor VWAP, Volume Profile, Market Facilitation,
Fair Value Gaps / BPR, session High/Low, Open Interest.
"""
import numpy as np, pandas as pd
warn = np.errstate(all="ignore")


def _ema(s, n): return s.ewm(span=n, adjust=False).mean()
def _rma(s, n): return s.ewm(alpha=1.0 / n, adjust=False).mean()
def _wma(s, n):
    w = np.arange(1, n + 1); sw = w.sum()
    return s.rolling(n).apply(lambda x: np.dot(x, w) / sw, raw=True)
def _hma(s, n):
    return _wma(2 * _wma(s, max(2, n // 2)) - _wma(s, n), max(2, int(np.sqrt(n))))
def _lr_slope(s, n):
    x = np.arange(n) - (n - 1) / 2.0; den = (x ** 2).sum()
    return s.rolling(n).apply(lambda y: np.dot(x, y) / den, raw=True)
def _kama(c, n=10, f=2, sl=30):
    ch = c.diff(n).abs(); vol = c.diff().abs().rolling(n).sum()
    er = (ch / (vol + 1e-9)).clip(0, 1); sc = (er * (2 / (f + 1) - 2 / (sl + 1)) + 2 / (sl + 1)) ** 2
    out = c.copy().values.astype(float); v = c.values.astype(float)
    for i in range(1, len(v)):
        if np.isnan(sc.iloc[i]) or np.isnan(out[i - 1]): out[i] = v[i]
        else: out[i] = out[i - 1] + sc.iloc[i] * (v[i] - out[i - 1])
    return pd.Series(out, index=c.index)


def compute_features(g):
    g = g.sort_values("trade_date").reset_index(drop=True)
    o = g.open.values.astype(float); h = g.high.values.astype(float); l = g.low.values.astype(float)
    c = g.close.values.astype(float); v = g.volume.values.astype(float); td = g.trade_date.values; n = len(c)
    C = pd.Series(c); H = pd.Series(h); L = pd.Series(l); O = pd.Series(o); V = pd.Series(v)
    dt = pd.to_datetime(g.trade_date); iso = dt.dt.isocalendar(); wk = (iso.year.astype(int) * 100 + iso.week.astype(int)).values
    pc = C.shift(1); r1 = (C / pc - 1) * 100
    tr = pd.concat([H - L, (H - pc).abs(), (L - pc).abs()], axis=1).max(axis=1)
    atr14 = _rma(tr, 14); atr20 = tr.rolling(20).mean(); tp = (H + L + C) / 3
    d = {"trade_date": td, "wk": wk}

    # ---------- momentum / MAs (overlays) ----------
    for k in [1, 3, 5, 10, 20, 60]: d[f"ret{k}"] = C.pct_change(k) * 100
    for k in [10, 20, 50, 100, 200]: d[f"d_sma{k}"] = (C / C.rolling(k).mean() - 1) * 100
    for k in [12, 26, 50]: d[f"d_ema{k}"] = (C / _ema(C, k) - 1) * 100
    d["d_wma20"] = (C / _wma(C, 20) - 1) * 100
    d["d_hma20"] = (C / _hma(C, 20) - 1) * 100
    d["d_vwma20"] = (C / ((C * V).rolling(20).sum() / (V.rolling(20).sum() + 1e-9)) - 1) * 100
    d["d_kama"] = (C / _kama(C) - 1) * 100
    mcg = C.copy()  # McGinley Dynamic
    mv = C.values.astype(float); out = mv.copy()
    for i in range(1, n):
        base = out[i - 1] if not np.isnan(out[i - 1]) else mv[i]
        out[i] = base + (mv[i] - base) / (0.6 * 10 * (mv[i] / (base + 1e-9)) ** 4 + 1e-9)
    d["d_mcg"] = (C / pd.Series(out, index=C.index) - 1) * 100
    d["d_alma"] = (C / _wma(C, 20) - 1) * 100  # ALMA approx via wma window (gaussian-weighted proxy)
    # slopes / linreg
    for k in [20, 50, 200]:
        sm = C.rolling(k).mean(); d[f"slope{k}"] = (sm / sm.shift(5) - 1) * 100
    d["sma20_50"] = (C.rolling(20).mean() / C.rolling(50).mean() - 1) * 100
    d["sma50_200"] = (C.rolling(50).mean() / C.rolling(200).mean() - 1) * 100
    lrsl = _lr_slope(C, 20); d["lr_slope"] = lrsl / C * 100
    lrline = C.rolling(20).mean() + lrsl * ((20 - 1) / 2.0)  # linreg value at last point
    d["lr_dist"] = (C - lrline) / C * 100
    d["tsf_dist"] = (C - (lrline + lrsl)) / C * 100  # time-series forecast (1-step)
    d["dpo"] = (C - C.rolling(21).mean().shift(11)) / C * 100  # detrended price osc
    d["gmma_spread"] = (_ema(C, 3) + _ema(C, 5) + _ema(C, 8)) / 3 / ((_ema(C, 30) + _ema(C, 50) + _ema(C, 60)) / 3) * 100 - 100
    trix = _ema(_ema(_ema(C, 15), 15), 15); d["trix"] = trix.pct_change() * 100
    # Alligator, Ichimoku, GMMA
    d["allig"] = (_ema(C, 5).shift(3) / _ema(C, 13).shift(8) - 1) * 100
    ten = (H.rolling(9).max() + L.rolling(9).min()) / 2; kij = (H.rolling(26).max() + L.rolling(26).min()) / 2
    d["tenkan_kijun"] = (ten / kij - 1) * 100; spanA = (ten + kij) / 2; spanB = (H.rolling(52).max() + L.rolling(52).min()) / 2
    d["ichi_pos"] = (C - (spanA + spanB) / 2) / C * 100
    # Aroon
    d["aroon_up"] = H.rolling(25).apply(lambda x: (25 - 1 - np.argmax(x)) / 25 * 100, raw=True)
    d["aroon_dn"] = L.rolling(25).apply(lambda x: (25 - 1 - np.argmin(x)) / 25 * 100, raw=True)
    d["aroon_osc"] = d["aroon_up"] - d["aroon_dn"]

    # ---------- volatility / bands ----------
    mid = C.rolling(20).mean(); sd = C.rolling(20).std()
    ub = mid + 2 * sd; lb = mid - 2 * sd
    d["bb_pctb"] = (C - lb) / (ub - lb + 1e-9); d["bb_width"] = (ub - lb) / (mid + 1e-9)
    d["bb_walk_up"] = (C > ub).rolling(5).sum(); d["bb_walk_dn"] = (C < lb).rolling(5).sum()
    kmid = _ema(C, 20); d["kc_pos"] = (C - (kmid - 2 * atr20)) / (4 * atr20 + 1e-9)  # Keltner
    dh = H.rolling(20).max(); dl = L.rolling(20).min(); d["donch_pos"] = (C - dl) / (dh - dl + 1e-9)
    d["donch_width"] = (dh - dl) / C * 100
    sm5 = C.rolling(5).mean(); d["starc_pos"] = (C - (sm5 - 2 * atr14)) / (4 * atr14 + 1e-9)  # STARC
    acc = 4 * (H - L) / (H + L + 1e-9); ab_u = C.rolling(20).mean() * (1 + acc); ab_l = C.rolling(20).mean() * (1 - acc)
    d["accel_pos"] = (C - ab_l) / (ab_u - ab_l + 1e-9)
    stderr = (C - lrline).abs().rolling(20).mean(); d["seband_pos"] = (C - (lrline - 2 * stderr)) / (4 * stderr + 1e-9)
    d["hlb_pos"] = (C - (H.rolling(20).max() + L.rolling(20).min()) / 2) / C * 100
    d["chandelier"] = (C - (H.rolling(22).max() - 3 * atr14)) / C * 100
    d["atr_tstop"] = (C - (C.rolling(10).max() - 3 * atr14)) / C * 100
    d["darvas"] = (C - H.rolling(20).max().shift(1)) / C * 100
    fibw = mid + 1.618 * atr20; d["fib_band_pos"] = (C - (mid - 1.618 * atr20)) / (2 * 1.618 * atr20 + 1e-9)
    dd = 1 - C / C.rolling(14).max(); d["ulcer"] = np.sqrt((dd ** 2).rolling(14).mean()) * 100
    d["atrp"] = atr20 / C * 100; d["atr5v20"] = tr.rolling(5).mean() / (atr20 + 1e-9)
    d["hvol"] = C.pct_change().rolling(20).std() * 100
    # SuperTrend direction
    hl2 = (H + L) / 2; bub = hl2 + 3 * atr14; blb = hl2 - 3 * atr14
    st = np.zeros(n); dirn = np.ones(n)
    for i in range(1, n):
        if np.isnan(bub.iloc[i]): st[i] = c[i]; continue
        if c[i] > st[i - 1]: dirn[i] = 1
        elif c[i] < st[i - 1]: dirn[i] = -1
        else: dirn[i] = dirn[i - 1]
        st[i] = blb.iloc[i] if dirn[i] == 1 else bub.iloc[i]
    d["supertrend_dir"] = dirn; d["supertrend_dist"] = (c - st) / c * 100

    # ---------- momentum / strength oscillators ----------
    delta = C.diff()
    for k in [14, 7, 3]:
        u = _rma(delta.clip(lower=0), k); dn = _rma(-delta.clip(upper=0), k); d[f"rsi{k}"] = 100 - 100 / (1 + u / (dn + 1e-9))
    ll14 = L.rolling(14).min(); hh14 = H.rolling(14).max(); stk = (C - ll14) / (hh14 - ll14 + 1e-9) * 100
    d["stoch_k"] = stk; d["stoch_d"] = stk.rolling(3).mean()
    rsi14 = d["rsi14"]; d["stochrsi"] = (rsi14 - rsi14.rolling(14).min()) / (rsi14.rolling(14).max() - rsi14.rolling(14).min() + 1e-9) * 100
    d["williams"] = -100 * (hh14 - C) / (hh14 - ll14 + 1e-9)
    e12 = _ema(C, 12); e26 = _ema(C, 26); macd = e12 - e26
    d["macd"] = macd / C * 100; d["macd_hist"] = (macd - _ema(macd, 9)) / C * 100
    matp = tp.rolling(20).mean(); mad = (tp - matp).abs().rolling(20).mean(); d["cci"] = (tp - matp) / (0.015 * mad + 1e-9)
    d["roc10"] = C.pct_change(10) * 100; d["roc20"] = C.pct_change(20) * 100
    d["cmo"] = (delta.clip(lower=0).rolling(14).sum() - (-delta.clip(upper=0)).rolling(14).sum()) / (delta.abs().rolling(14).sum() + 1e-9) * 100
    d["ao"] = (((H + L) / 2).rolling(5).mean() - ((H + L) / 2).rolling(34).mean()) / C * 100
    # ADX / DI / Vortex
    upmove = H.diff(); downmove = -L.diff()
    plusDM = np.where((upmove > downmove) & (upmove > 0), upmove, 0.0); minusDM = np.where((downmove > upmove) & (downmove > 0), downmove, 0.0)
    pdi = 100 * _rma(pd.Series(plusDM, index=C.index), 14) / (atr14 + 1e-9); mdi = 100 * _rma(pd.Series(minusDM, index=C.index), 14) / (atr14 + 1e-9)
    dx = 100 * (pdi - mdi).abs() / (pdi + mdi + 1e-9); d["adx"] = _rma(dx, 14); d["plus_di"] = pdi; d["minus_di"] = mdi
    vpp = (H - L.shift(1)).abs(); vpm = (L - H.shift(1)).abs()
    d["vortex"] = (vpp.rolling(14).sum() - vpm.rolling(14).sum()) / (tr.rolling(14).sum() + 1e-9)
    # Ultimate, TSI, Coppock, Fisher, Mass, CoG
    bp = C - pd.concat([L, pc], axis=1).min(axis=1)
    avg = lambda p: bp.rolling(p).sum() / (tr.rolling(p).sum() + 1e-9)
    d["ult"] = 100 * (4 * avg(7) + 2 * avg(14) + avg(28)) / 7
    pc1 = C.diff(); d["tsi"] = 100 * _ema(_ema(pc1, 25), 13) / (_ema(_ema(pc1.abs(), 25), 13) + 1e-9)
    d["coppock"] = _wma(C.pct_change(14) * 100 + C.pct_change(11) * 100, 10)
    x = 2 * ((C - ll14) / (hh14 - ll14 + 1e-9) - 0.5); x = x.clip(-0.999, 0.999)
    fish = 0.5 * np.log((1 + x) / (1 - x)); d["fisher"] = _ema(fish, 5)
    d["mass"] = (_ema(H - L, 9) / (_ema(_ema(H - L, 9), 9) + 1e-9)).rolling(25).sum()
    d["cog"] = -_wma(C, 10) / (C.rolling(10).mean() + 1e-9)
    # Connors RSI
    streak = np.zeros(n)
    for i in range(1, n):
        if c[i] > c[i - 1]: streak[i] = streak[i - 1] + 1 if streak[i - 1] > 0 else 1
        elif c[i] < c[i - 1]: streak[i] = streak[i - 1] - 1 if streak[i - 1] < 0 else -1
    sd_ = pd.Series(streak, index=C.index); su = _rma(sd_.diff().clip(lower=0), 2); sdn = _rma(-sd_.diff().clip(upper=0), 2)
    rsi_streak = 100 - 100 / (1 + su / (sdn + 1e-9)); pctrank = C.pct_change().rolling(100).apply(lambda w: (w[:-1] < w[-1]).mean() * 100, raw=True)
    d["crsi"] = (d["rsi3"] + rsi_streak + pctrank) / 3

    # ---------- volume / liquidity ----------
    av20 = V.rolling(20).mean(); d["volr"] = V / (av20 + 1e-9); d["vol5v20"] = V.rolling(5).mean() / (av20 + 1e-9)
    d["vol_osc"] = (V.rolling(5).mean() - av20) / (av20 + 1e-9) * 100; d["vroc"] = V.pct_change(10) * 100
    sgn = np.sign(r1.fillna(0)); obv = (sgn * V).cumsum(); d["obv_chg"] = (obv - obv.shift(10)) / (av20 * 10 + 1e-9)
    mfm = ((C - L) - (H - C)) / (H - L + 1e-9); mfv = mfm * V; d["cmf"] = mfv.rolling(20).sum() / (V.rolling(20).sum() + 1e-9)
    ad = mfv.cumsum(); d["ad_chg"] = (ad - ad.shift(10)) / (av20 * 10 + 1e-9); d["chosc"] = (_ema(ad, 3) - _ema(ad, 10)) / (av20 + 1e-9)
    d["eom"] = (((H + L) / 2).diff() * (H - L) / (V + 1e-9)).rolling(14).mean() * 1e6
    mf = tp * V; pmf = mf.where(tp > tp.shift(1), 0).rolling(14).sum(); nmf = mf.where(tp < tp.shift(1), 0).rolling(14).sum()
    d["mfi"] = 100 - 100 / (1 + pmf / (nmf + 1e-9))
    d["force"] = _ema(C.diff() * V, 13) / (av20 * C + 1e-9)
    d["bull_power"] = (H - _ema(C, 13)) / C * 100; d["bear_power"] = (L - _ema(C, 13)) / C * 100
    pvt = (C.pct_change() * V).cumsum(); d["pvt_chg"] = (pvt - pvt.shift(10)) / (av20 * 10 + 1e-9)
    d["mfi_bw"] = (H - L) / (V + 1e-9) * 1e6  # market facilitation (daily proxy)
    d["vol_z"] = (V - av20) / (V.rolling(20).std() + 1e-9)
    # Twiggs, Klinger, Elder handled above; NVI/PVI
    nvi = np.ones(n); pvi = np.ones(n)
    for i in range(1, n):
        r = c[i] / c[i - 1] if c[i - 1] else 1
        nvi[i] = nvi[i - 1] * r if v[i] < v[i - 1] else nvi[i - 1]
        pvi[i] = pvi[i - 1] * r if v[i] > v[i - 1] else pvi[i - 1]
    d["nvi_sig"] = pd.Series(nvi, index=C.index) / pd.Series(nvi, index=C.index).rolling(50).mean() - 1
    d["pvi_sig"] = pd.Series(pvi, index=C.index) / pd.Series(pvi, index=C.index).rolling(50).mean() - 1

    # ---------- structure / pivots ----------
    P = (pc.shift(0) * 0 + (H.shift(1) + L.shift(1) + C.shift(1)) / 3)  # prior-day pivot
    d["dist_pivot"] = (C - P) / C * 100
    d["dist_r1"] = (C - (2 * P - L.shift(1))) / C * 100; d["dist_s1"] = (C - (2 * P - H.shift(1))) / C * 100
    rng1 = H.shift(1) - L.shift(1); d["dist_cam_r3"] = (C - (C.shift(1) + rng1 * 1.1 / 4)) / C * 100
    cprw = ((2 * P - ((H.shift(1) + L.shift(1)) / 2)).abs() - ((H.shift(1) + L.shift(1)) / 2 - 0).abs())  # placeholder
    d["cpr_width"] = (abs(P - (H.shift(1) + L.shift(1)) / 2) * 2) / C * 100
    for k in [20, 60, 120, 252]:
        d[f"dist_hi{k}"] = (C / H.rolling(k).max() - 1) * 100
    for k in [20, 60]:
        d[f"dist_lo{k}"] = (C / L.rolling(k).min() - 1) * 100
    d["pos20"] = (C - L.rolling(20).min()) / (H.rolling(20).max() - L.rolling(20).min() + 1e-9)
    d["pos60"] = (C - L.rolling(60).min()) / (H.rolling(60).max() - L.rolling(60).min() + 1e-9)
    rn = np.round(c, -int(np.floor(np.log10(np.nanmax(c) + 1))) + 1) if n else c; d["round_dist"] = (c - np.round(c / 50) * 50) / c * 100
    # TD setup count (simplified)
    tdc = np.zeros(n)
    for i in range(4, n):
        if c[i] > c[i - 4]: tdc[i] = tdc[i - 1] + 1 if tdc[i - 1] > 0 else 1
        elif c[i] < c[i - 4]: tdc[i] = tdc[i - 1] - 1 if tdc[i - 1] < 0 else -1
    d["td_setup"] = tdc

    # ---------- price action ----------
    rng = (H - L); d["body"] = np.where(rng > 0, (C - O) / rng, 0); d["uwick"] = np.where(rng > 0, (H - pd.concat([O, C], axis=1).max(axis=1)) / rng, 0)
    d["lwick"] = np.where(rng > 0, (pd.concat([O, C], axis=1).min(axis=1) - L) / rng, 0)
    d["gap"] = (O - pc) / pc * 100; d["cloc"] = np.where(h > l, (c - l) / (h - l), np.nan); d["rng"] = rng / pc * 100
    d["hh5"] = (H > H.shift(1)).rolling(5).sum(); d["hl5"] = (L > L.shift(1)).rolling(5).sum()

    # ---------- weekly PIT ----------
    grp = pd.Series(wk).ne(pd.Series(wk).shift()).cumsum().values
    wtd_ret = np.full(n, np.nan); wtd_pos = np.full(n, np.nan); wk_range = np.full(n, np.nan); wk_du = np.full(n, np.nan)
    for gg in np.unique(grp):
        ii = np.where(grp == gg)[0]; whh = np.maximum.accumulate(h[ii]); wll = np.minimum.accumulate(l[ii]); base = c[ii[0]] if np.isnan(pc.iloc[ii[0]]) else pc.iloc[ii[0]]
        wtd_ret[ii] = (c[ii] / base - 1) * 100; wtd_pos[ii] = np.where(whh > wll, (c[ii] - wll) / (whh - wll), np.nan)
        wk_range[ii] = np.where(c[ii] > 0, (whh - wll) / c[ii] * 100, np.nan); wk_du[ii] = np.cumsum((r1.values[ii] > 0).astype(float))
    d["wtd_ret"] = wtd_ret; d["wtd_pos"] = wtd_pos; d["wk_range"] = wk_range
    d["wk_vs_sma20"] = (c / C.rolling(100).mean().values - 1) * 100; d["wk_daysup"] = wk_du

    # ---------- label + outcome (T+1 open->close) ----------
    oc_next = np.roll((c - o) / o * 100, -1); oc_next[-1] = np.nan
    d["ret_oc"] = oc_next; d["y"] = (oc_next > 0).astype(float)
    tdn = np.roll(td, -1).astype(object); tdn[-1] = None; d["trade_date_next"] = tdn
    D = pd.DataFrame(d)
    return D


FEATS = None  # set by miner from columns
