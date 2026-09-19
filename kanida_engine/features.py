"""
KANIDA unified engine — FEATURE FRAME BUILDER.
Runs the atoms over a stock's history to produce the point-in-time MINING FRAME:
  micro EOD footprints (from 1-min atoms) + macro daily technicals + WTD/MTD (to-date) + T-N lookbacks.
Every column is as-of-that-day (leak-free). This frame is what the miner discovers combinations over,
and the SAME atoms produce the live to-time vector for intraday serving (parity).

build(days_bars, ...) works on in-memory data (used by a synthetic wiring test now, and by the
DB reader post-fetch). No look-ahead: atoms only see bars up to 'now'; macro uses shift(1) where a
prior-close reference is needed.
"""
import numpy as np, pandas as pd
from atoms import StockState


def _daily_from_minute(days_bars):
    """Aggregate 1-min bars into a daily OHLCV frame + per-day micro EOD snapshot (via atoms)."""
    st = StockState(); rows = []
    for day in sorted(days_bars):
        bars = days_bars[day]
        if not bars:
            continue
        st.reset_day()
        o = bars[0][0]; hi = -1e18; lo = 1e18; c = bars[-1][3]; vol = 0.0
        for (bo, bh, bl, bc, bv) in bars:
            st.update(bo, bh, bl, bc, bv)
            hi = max(hi, bh); lo = min(lo, bl); vol += bv
        snap = st.eod_snapshot()
        rows.append({"day": pd.Timestamp(day), "o": o, "h": hi, "l": lo, "c": c, "v": vol, **snap})
    return pd.DataFrame(rows).set_index("day").sort_index()


def _macro(d, mkt=None):
    """Daily technicals + WTD/MTD (to-date) macro features — all point-in-time (shift where needed)."""
    O, H, L, C, V = d["o"], d["h"], d["l"], d["c"], d["v"]
    pc = C.shift(1); tr = np.maximum(H - L, np.maximum((H - pc).abs(), (L - pc).abs()))
    F = pd.DataFrame(index=d.index)
    F["m_roc_5"] = (C / C.shift(5) - 1) * 100
    F["m_roc_20"] = (C / C.shift(20) - 1) * 100
    F["m_atr20_pct"] = tr.rolling(20).mean() / C * 100
    F["m_dist_high_20"] = (C / H.rolling(20).max() - 1) * 100
    F["m_dist_high_60"] = (C / H.rolling(60).max() - 1) * 100
    F["m_dist_sma_20"] = (C / C.rolling(20).mean() - 1) * 100
    sma20 = C.rolling(20).mean(); F["m_slope_sma20"] = (sma20 / sma20.shift(5) - 1) * 100
    dl = C.diff(); F["m_rsi14"] = 100 - 100 / (1 + dl.clip(lower=0).rolling(14).mean() / (-dl.clip(upper=0)).rolling(14).mean())
    F["m_vol_vs_20"] = V / V.rolling(20).mean()
    # money flow (institutional accumulation/distribution)
    mfm = ((C - L) - (H - C)) / (H - L).replace(0, np.nan)
    F["m_cmf20"] = (mfm * V).rolling(20).sum() / V.rolling(20).sum()
    typ = (H + L + C) / 3.0; rmf = typ * V
    pos = rmf.where(typ > typ.shift(1), 0.0); neg = rmf.where(typ < typ.shift(1), 0.0)
    F["m_mfi14"] = 100 - 100 / (1 + pos.rolling(14).sum() / neg.rolling(14).sum().replace(0, np.nan))
    obv = (np.sign(C.diff()).fillna(0) * V).cumsum()
    F["m_obv_slope20"] = (obv - obv.shift(20)) / V.rolling(20).mean().replace(0, np.nan)
    # WTD / MTD to-date (point-in-time running from the period's first close)
    wk = C.index.to_period("W"); mo = C.index.to_period("M")
    wk_open = C.groupby(wk).transform("first"); mo_open = C.groupby(mo).transform("first")
    F["m_wtd_ret"] = (C / wk_open - 1) * 100
    F["m_mtd_ret"] = (C / mo_open - 1) * 100
    F["m_wtd_range"] = (H.groupby(wk).cummax() - L.groupby(wk).cummin()) / C * 100
    if mkt is not None:
        mk = mkt.reindex(C.index).ffill()
        F["m_rs_20d"] = ((C / C.shift(20) - 1) - (mk / mk.shift(20) - 1)) * 100
    return F


def _event(index):
    """Leak-free calendar EVENT features (known at signal-time from the date alone): monthly-expiry
    proximity (last Thursday), day-of-week, month-phase. Added to the mining frame so the miner can
    discover genuinely event-driven, more-consistent patterns (M12b)."""
    idx = pd.DatetimeIndex(index); n = len(idx)
    ev = pd.DataFrame(index=idx)
    ev["ev_dow"] = idx.dayofweek.values.astype(float)                       # 0=Mon .. 4=Fri
    is_exp_day = np.zeros(n, bool); is_exp_wk = np.zeros(n, bool)
    dte = np.full(n, 31.0); is_me = np.zeros(n, bool); is_ms = np.zeros(n, bool)
    dfp = pd.DataFrame({"pos": np.arange(n), "d": idx, "p": idx.to_period("M")})
    for _, g in dfp.groupby("p", sort=False):
        thu = g[g.d.dt.dayofweek == 3]
        ed = thu.d.max() if len(thu) else g.d.max()                         # monthly F&O expiry
        pos = g.pos.values
        is_exp_day[g.pos[g.d == ed].values] = True
        is_exp_wk[g.pos[(g.d >= ed - pd.Timedelta(days=4)) & (g.d <= ed)].values] = True
        dte[pos] = np.clip([(ed - pd.Timestamp(x)).days for x in g.d.values], 0, 31)
        is_me[pos[-3:]] = True; is_ms[pos[:3]] = True
    ev["ev_is_expiry_day"] = is_exp_day.astype(float)
    ev["ev_is_expiry_wk"] = is_exp_wk.astype(float)
    ev["ev_days_to_expiry"] = dte
    ev["ev_is_month_end"] = is_me.astype(float)
    ev["ev_is_month_start"] = is_ms.astype(float)
    return ev


def build(days_bars, market_close=None, lookback_N=5):
    """Return the daily MINING FRAME: micro EOD footprints + macro/WTD/MTD + T-1..T-N micro lookbacks
    + calendar EVENT features. Every row is as-of that day's close (EOD). Labels added by the miner."""
    d = _daily_from_minute(days_bars)
    if len(d) < 30:
        return pd.DataFrame()
    macro = _macro(d, market_close)
    micro_cols = ["eod_ret", "eod_range_pct", "eod_n_hivol", "eod_n_iceberg", "eod_n_absorb",
                  "eod_buy_sell_imbalance", "eod_max_vol_z", "eod_atp_crossings",
                  "eod_cvd_norm", "eod_vol_compression", "eod_deepest_pullback", "eod_dist_poc"]
    micro = d[micro_cols].copy()
    frame = pd.concat([macro, micro], axis=1)
    # T-N lookbacks (parameter, not hardcoded): the miner discovers which depths matter per stock
    for n in range(1, lookback_N + 1):
        for col in micro_cols:
            frame[f"{col}_T{n}"] = d[col].shift(n)
    frame = pd.concat([frame, _event(frame.index)], axis=1)          # calendar event features (M12b)
    frame["_o"] = d["o"]; frame["_h"] = d["h"]; frame["_l"] = d["l"]; frame["_c"] = d["c"]
    frame["year"] = frame.index.year
    return frame


# ---- outcome targets (config the miner mines against) ----
DAILY_TARGETS = [("up_1pct_1d", "up", 1.0, 1), ("up_2pct_2d", "up", 2.0, 2), ("up_5pct_5d", "up", 5.0, 5),
                 ("dn_1pct_1d", "dn", 1.0, 1), ("dn_2pct_2d", "dn", 2.0, 2), ("dn_5pct_5d", "dn", 5.0, 5)]
# intraday forward windows (minutes) — enables "1% up within 45 min / from 09:15" style outcomes
INTRADAY_TARGETS = [("up_1pct_45m", "up", 1.0, 45), ("up_2pct_120m", "up", 2.0, 120),
                    ("dn_1pct_45m", "dn", 1.0, 45), ("dn_2pct_120m", "dn", 2.0, 120)]


def label_daily(frame, direction, pct, w):
    C, H, L = frame["_c"], frame["_h"], frame["_l"]
    fh = H.shift(-1).rolling(w).max().shift(-(w - 1)) if w > 1 else H.shift(-1)
    fl = L.shift(-1).rolling(w).min().shift(-(w - 1)) if w > 1 else L.shift(-1)
    return ((fh / C - 1) * 100 >= pct).astype(float) if direction == "up" else ((fl / C - 1) * 100 <= -pct).astype(float)


def label_intraday(fwd_hl, entry, direction, pct, n_min):
    """fwd_hl = next-n_min list of (high, low) 1-min bars. 1 if target touched intraday within n_min.
    Also usable to compute minute MFE/MAE to the target (max fav / max adverse before it hits)."""
    tgt = entry * (1 + pct / 100) if direction == "up" else entry * (1 - pct / 100)
    for (h, l) in fwd_hl[:n_min]:
        if direction == "up" and h >= tgt: return 1.0
        if direction == "dn" and l <= tgt: return 1.0
    return 0.0


def build_from_db(symbol, kdb=r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db", lookback_N=5):
    """Post-fetch path: read a symbol's 1-min from kanida.db and build the frame. (Run AFTER the fetch.)"""
    import sqlite3
    con = sqlite3.connect(kdb, timeout=120)
    df = pd.read_sql("SELECT bar_time,open,high,low,close,volume FROM ohlc_1min WHERE symbol=? ORDER BY bar_time",
                     con, params=[symbol])
    mk = pd.read_sql("SELECT bar_time,close FROM ohlc_daily WHERE symbol='NIFTY 50' ORDER BY bar_time", con)
    con.close()
    df["day"] = df["bar_time"].str[:10]
    days_bars = {day: list(zip(g["open"], g["high"], g["low"], g["close"], g["volume"]))
                 for day, g in df.groupby("day", sort=True)}
    mk["date"] = pd.to_datetime(mk["bar_time"]); market = mk.set_index("date")["close"]
    return build(days_bars, market, lookback_N)


import os
CACHE_DIR = r"C:\Users\SPS\Documents\Kanida_Falcon\db\frame_cache"


def load_frame(symbol, lookback_N=5):
    """Frame CACHE: build the daily frame from 1-min ONCE, then reuse instantly. All heavy work
    (reading ~1M 1-min bars + atoms) happens once per stock; subsequent calls load a small pickle."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    safe = "".join(ch if ch.isalnum() else "_" for ch in symbol)
    path = os.path.join(CACHE_DIR, f"{safe}_L{lookback_N}.pkl")
    if os.path.exists(path):
        try:
            return pd.read_pickle(path)
        except Exception:
            pass
    frame = build_from_db(symbol, lookback_N=lookback_N)
    if not frame.empty:
        try:
            frame.to_pickle(path)
        except Exception:
            pass
    return frame
