"""
FALCON HULK V1 DATA SPINE  --  builder
=====================================================================
Isolated, point-in-time, leak-free feature+label foundation for
RELIANCE + ADANIENT.

HARD RULES (enforced in this file):
  * 2026 IS SEALED. No bar with date >= 2026-01-01 is ever read. The
    seal is applied at the SQL WHERE clause of every source read.
  * Strict point-in-time. Every feature at time T uses ONLY bars
    with timestamp <= T. Every forward label uses only bars AFTER
    the reference date T. No full-week / future stamping.
  * Isolation. Writes ONLY to data/db/falcon_hulk.db. Source is
    opened mode=ro. No falcon_* / portfolio_* table is touched.
  * Window 2018-01-01 .. 2025-12-31. Symbols RELIANCE, ADANIENT.
  * Gaps (RELIANCE 1min Mar-2020 & Sep-2022) are NEVER bridged. The
    trading calendar is derived from the 1-min data, so missing
    sessions simply do not exist; multi-day labels that span a
    calendar gap > 4 days get gap_flag=1.

ALL TIMES ARE IST (Asia/Kolkata). Source market data is already IST;
values are stored verbatim.
"""
import os
import sqlite3
from collections import deque, OrderedDict
from datetime import date, datetime

# ---------------------------------------------------------------- paths
BASE = r"C:/Users/SPS/Desktop/Kanida.ai Terminal Quant Intelligence Engine"
SRC = os.path.join(BASE, "universe_engine", "data", "db", "kanida_universe.db")
DST = os.path.join(BASE, "data", "db", "falcon_hulk.db")

SYMBOLS = ["RELIANCE", "ADANIENT"]
START = "2018-01-01"           # first session date emitted
END = "2025-12-31"            # last session date emitted
SEAL = "2026-01-01"           # NEVER read a bar with date >= this
DAILY_WARMUP = "2016-01-01"    # daily history read from here for rolling warmup

COMPLETE_MIN_BARS = 370        # complete_flag=1 iff n_bars >= this
GAP_CAL_DAYS = 4               # calendar-day gap threshold for gap_flag

# intraday grid: every 5 min from 09:20 to 14:30 inclusive
def _grid_minutes():
    out = []
    h, m = 9, 20
    while (h, m) <= (14, 30):
        out.append(f"{h:02d}:{m:02d}")
        m += 5
        if m >= 60:
            m -= 60
            h += 1
    return out
GRID = _grid_minutes()

HORIZONS = [1, 2, 3, 5, 10]


# ---------------------------------------------------------------- io
def src_conn():
    return sqlite3.connect(f"file:{SRC}?mode=ro", uri=True)


def load_1min(con, sym):
    """All 1-min bars for sym, 2018-01-01 .. < SEAL. Seal enforced in SQL."""
    cur = con.execute(
        "SELECT bar_time, open, high, low, close, volume "
        "FROM mkt_ohlc_1min WHERE symbol=? AND bar_time>=? AND bar_time<? "
        "ORDER BY bar_time",
        (sym, START + " 00:00:00", SEAL + " 00:00:00"),
    )
    return cur.fetchall()


def load_daily(con, sym):
    """Daily bars from 2016 warmup .. < SEAL. Seal enforced in SQL."""
    cur = con.execute(
        "SELECT trade_date, open, high, low, close, volume "
        "FROM ohlc_daily WHERE symbol=? AND trade_date>=? AND trade_date<? "
        "ORDER BY trade_date",
        (sym, DAILY_WARMUP, SEAL),
    )
    return cur.fetchall()


# ---------------------------------------------------------------- helpers
def d(s):
    return date(int(s[0:4]), int(s[5:7]), int(s[8:10]))


def group_sessions(bars1m):
    """bars1m rows -> OrderedDict date-> list of (hhmm, o,h,l,c,v) sorted."""
    sess = OrderedDict()
    for bt, o, h, l, c, v in bars1m:
        day = bt[0:10]
        hhmm = bt[11:16]
        sess.setdefault(day, []).append((hhmm, o, h, l, c, v))
    return sess


def session_ohlc(sess):
    """date-> (o,h,l,c,v, n_bars, first_hhmm, last_hhmm) aggregated from 1min."""
    out = OrderedDict()
    for day, rows in sess.items():
        o = rows[0][1]
        c = rows[-1][4]
        hi = max(r[2] for r in rows)
        lo = min(r[3] for r in rows)
        vol = sum(r[5] for r in rows)
        out[day] = (o, hi, lo, c, vol, len(rows), rows[0][0], rows[-1][0])
    return out


# ---------------------------------------------------------------- schema
def create_schema(w):
    w.executescript(
        """
    DROP TABLE IF EXISTS hulk_sessions;
    CREATE TABLE hulk_sessions(
        symbol TEXT, trade_date TEXT, n_bars INTEGER,
        first_bar TEXT, last_bar TEXT, complete_flag INTEGER,
        PRIMARY KEY(symbol, trade_date));

    DROP TABLE IF EXISTS hulk_labels_multiday;
    CREATE TABLE hulk_labels_multiday(
        symbol TEXT, direction TEXT, entry_kind TEXT,
        entry_date TEXT, entry_price REAL,
        ret_t1 REAL, ret_t2 REAL, ret_t3 REAL, ret_t5 REAL, ret_t10 REAL,
        mfe_t5 REAL, mae_t5 REAL,
        days_to_p05 INTEGER, days_to_p1 INTEGER,
        peak_ret_t10 REAL, giveback_from_peak_t10 REAL,
        gap_flag INTEGER, label_end_date TEXT,
        PRIMARY KEY(symbol, direction, entry_kind, entry_date));

    DROP TABLE IF EXISTS hulk_labels_intraday;
    CREATE TABLE hulk_labels_intraday(
        symbol TEXT, direction TEXT, entry_date TEXT, entry_minute TEXT,
        entry_price REAL, ret_to_close REAL,
        mfe_intraday REAL, mae_intraday REAL,
        min_to_p05 INTEGER, min_to_p1 INTEGER,
        peak_ret REAL, giveback REAL,
        PRIMARY KEY(symbol, direction, entry_date, entry_minute));

    DROP TABLE IF EXISTS hulk_daily_features;
    CREATE TABLE hulk_daily_features(
        symbol TEXT, trade_date TEXT,
        roc_1 REAL, roc_5 REAL, roc_10 REAL, roc_20 REAL, roc_60 REAL,
        dist_sma_20 REAL, dist_sma_50 REAL, dist_sma_200 REAL,
        slope_sma_20 REAL, rsi_14 REAL, atr_20_pct REAL,
        range_pct REAL, gap_pct REAL, close_loc REAL,
        dist_high_20 REAL, dist_high_60 REAL, dist_high_120 REAL, dist_high_252 REAL,
        vol_vs_20d REAL,
        n_higher_highs_5 INTEGER, n_higher_lows_5 INTEGER,
        PRIMARY KEY(symbol, trade_date));

    DROP TABLE IF EXISTS hulk_intraday_features;
    CREATE TABLE hulk_intraday_features(
        symbol TEXT, bar_time TEXT,
        min_since_open INTEGER, ret_since_open REAL,
        opening_range_pos REAL, vwap_dev REAL,
        cum_vol_vs_20d_avg REAL,
        rolling_5min_ret REAL, rolling_15min_ret REAL,
        PRIMARY KEY(symbol, bar_time));
    """
    )


# ---------------------------------------------------------------- table 1
def build_sessions(w, sym, sohlc):
    rows = []
    for day, (o, hi, lo, c, vol, nb, fb, lb) in sohlc.items():
        if day < START or day > END:
            continue
        rows.append((sym, day, nb, fb + ":00", lb + ":00",
                     1 if nb >= COMPLETE_MIN_BARS else 0))
    w.executemany(
        "INSERT INTO hulk_sessions VALUES(?,?,?,?,?,?)", rows)
    return len(rows)


# ---------------------------------------------------------------- table 2
def build_multiday(w, sym, sohlc):
    days = list(sohlc.keys())           # ordered calendar (2018.. < 2026)
    dts = [d(x) for x in days]
    O = [sohlc[x][0] for x in days]
    H = [sohlc[x][1] for x in days]
    L = [sohlc[x][2] for x in days]
    C = [sohlc[x][3] for x in days]
    n = len(days)
    out = []
    for i, day in enumerate(days):
        if day < START or day > END:
            continue
        # gap flag & label_end_date over T..T+10 (calendar span of available part)
        last_idx = min(i + 10, n - 1)
        max_gap = 0
        for k in range(i, last_idx):
            g = (dts[k + 1] - dts[k]).days
            if g > max_gap:
                max_gap = g
        gap_flag = 1 if max_gap > GAP_CAL_DAYS else 0
        label_end_date = days[i + 10] if i + 10 < n else None

        for direction in ("long", "short"):
            sgn = 1.0 if direction == "long" else -1.0
            for entry_kind in ("close", "nextopen"):
                if entry_kind == "close":
                    entry = C[i]
                else:
                    if i + 1 >= n:
                        continue
                    entry = O[i + 1]
                if entry is None or entry <= 0:
                    continue

                rets = {}
                for hh in HORIZONS:
                    j = i + hh
                    rets[hh] = sgn * (C[j] / entry - 1.0) if j < n else None

                # MFE/MAE over T+1..T+5 (favorable / adverse, direction-adjusted)
                mfe = mae = None
                for k in range(i + 1, min(i + 5, n - 1) + 1):
                    if direction == "long":
                        fav = H[k] / entry - 1.0
                        adv = L[k] / entry - 1.0
                    else:
                        fav = -(L[k] / entry - 1.0)
                        adv = -(H[k] / entry - 1.0)
                    mfe = fav if mfe is None else max(mfe, fav)
                    mae = adv if mae is None else min(mae, adv)

                # days-to-target & peak over T+1..T+10 (favorable via extremes)
                d05 = d1 = None
                peak = None
                for off in range(1, 11):
                    k = i + off
                    if k >= n:
                        break
                    fav = (H[k] / entry - 1.0) if direction == "long" \
                        else -(L[k] / entry - 1.0)
                    peak = fav if peak is None else max(peak, fav)
                    if d05 is None and fav >= 0.005:
                        d05 = off
                    if d1 is None and fav >= 0.01:
                        d1 = off
                giveback = None
                if peak is not None and rets[10] is not None:
                    giveback = peak - rets[10]

                out.append((sym, direction, entry_kind, day, entry,
                            rets[1], rets[2], rets[3], rets[5], rets[10],
                            mfe, mae, d05, d1, peak, giveback,
                            gap_flag, label_end_date))
    w.executemany(
        "INSERT INTO hulk_labels_multiday VALUES("
        "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", out)
    return len(out)


# ---------------------------------------------------------------- table 4
def _sma(C, i, n):
    if i - n + 1 < 0:
        return None
    return sum(C[i - n + 1:i + 1]) / n


def build_daily_features(w, sym, sohlc, daily_rows):
    """Unified daily series = ohlc_daily (2016..2025) with any 1min-only
    session dates (special sessions) appended, sorted. Rolling features use
    this whole series (all bars <= T). Rows emitted only for dates that are
    in the 1min session calendar within [START, END]."""
    series = {}   # date -> (o,h,l,c,v)
    for td, o, h, l, c, v in daily_rows:
        series[td] = (o, h, l, c, v)
    calendar = set(sohlc.keys())
    for day in sohlc:
        if day not in series:
            o, hi, lo, c, vol, *_ = sohlc[day]
            series[day] = (o, hi, lo, c, vol)
    days = sorted(series.keys())
    O = [series[x][0] for x in days]
    H = [series[x][1] for x in days]
    L = [series[x][2] for x in days]
    C = [series[x][3] for x in days]
    V = [series[x][4] for x in days]
    N = len(days)

    # Wilder RSI-14 over the series
    rsi = [None] * N
    period = 14
    if N > period:
        gains = losses = 0.0
        for k in range(1, period + 1):
            ch = C[k] - C[k - 1]
            gains += max(ch, 0.0)
            losses += max(-ch, 0.0)
        ag = gains / period
        al = losses / period
        rsi[period] = 100.0 if al == 0 else 100.0 - 100.0 / (1 + ag / al)
        for k in range(period + 1, N):
            ch = C[k] - C[k - 1]
            g = max(ch, 0.0)
            ls = max(-ch, 0.0)
            ag = (ag * (period - 1) + g) / period
            al = (al * (period - 1) + ls) / period
            rsi[k] = 100.0 if al == 0 else 100.0 - 100.0 / (1 + ag / al)

    # ATR-20 (simple mean of TR)
    TR = [None] * N
    for k in range(1, N):
        TR[k] = max(H[k] - L[k], abs(H[k] - C[k - 1]), abs(L[k] - C[k - 1]))

    out = []
    for i, day in enumerate(days):
        if day not in calendar or day < START or day > END:
            continue
        c = C[i]

        def roc(nn):
            return (c / C[i - nn] - 1.0) if i - nn >= 0 else None
        s20 = _sma(C, i, 20)
        s50 = _sma(C, i, 50)
        s200 = _sma(C, i, 200)
        s20_5 = _sma(C, i - 5, 20) if i - 5 >= 0 else None
        slope = ((s20 - s20_5) / s20_5) if (s20 is not None
                                            and s20_5 not in (None, 0)) else None
        atr = None
        if i >= 20 and all(TR[k] is not None for k in range(i - 19, i + 1)):
            atr = (sum(TR[i - 19:i + 1]) / 20) / c
        rng = (H[i] - L[i]) / c if c else None
        gap = (O[i] / C[i - 1] - 1.0) if i >= 1 and C[i - 1] else None
        cloc = ((c - L[i]) / (H[i] - L[i])) if H[i] > L[i] else 0.5

        def disthigh(nn):
            if i - nn + 1 < 0:
                return None
            return c / max(H[i - nn + 1:i + 1]) - 1.0
        vol20 = None
        if i >= 20:
            av = sum(V[i - 20:i]) / 20  # prior 20, excluding today
            vol20 = V[i] / av if av else None
        nhh = sum(1 for k in range(i - 4, i + 1)
                  if k >= 1 and H[k] > H[k - 1]) if i >= 5 else None
        nhl = sum(1 for k in range(i - 4, i + 1)
                  if k >= 1 and L[k] > L[k - 1]) if i >= 5 else None

        out.append((
            sym, day, roc(1), roc(5), roc(10), roc(20), roc(60),
            (c / s20 - 1.0) if s20 else None,
            (c / s50 - 1.0) if s50 else None,
            (c / s200 - 1.0) if s200 else None,
            slope, rsi[i], atr, rng, gap, cloc,
            disthigh(20), disthigh(60), disthigh(120), disthigh(252),
            vol20, nhh, nhl))
    w.executemany(
        "INSERT INTO hulk_daily_features VALUES("
        "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", out)
    return len(out)


# ---------------------------------------------------------------- tables 3 & 5
def _tmin(hhmm):
    return int(hhmm[0:2]) * 60 + int(hhmm[3:5])


def build_intraday(w, sym, sess, sohlc):
    """Both intraday labels (3) and intraday features (5). Grid = GRID
    minutes on COMPLETE sessions only. Point-in-time throughout."""
    lab_out = []
    feat_out = []
    hist = deque(maxlen=20)   # prior complete sessions' {minute-of-day: cumvol}
    days = list(sess.keys())
    for day in days:
        if day < START or day > END:
            continue
        nb = sohlc[day][5]
        if nb < COMPLETE_MIN_BARS:      # skip incomplete / Muhurat sessions
            continue
        rows = sess[day]                # (hhmm,o,h,l,c,v) sorted
        by = {r[0]: r for r in rows}
        if "09:15" not in by or "15:29" not in by:
            continue
        open0915 = by["09:15"][1]

        # cumulative vwap num/den & cumvol, indexed by minute-of-day
        cumv = {}
        cum_pv = 0.0
        cum_v = 0.0
        for hhmm, o, h, l, c, v in rows:
            tp = (h + l + c) / 3.0
            cum_pv += tp * v
            cum_v += v
            cumv[hhmm] = (cum_v, cum_pv)
        # opening range 09:15..09:30 inclusive
        or_hi = or_lo = None
        for hhmm, o, h, l, c, v in rows:
            if hhmm <= "09:30":
                or_hi = h if or_hi is None else max(or_hi, h)
                or_lo = l if or_lo is None else min(or_lo, l)
            else:
                break
        close1529 = by["15:29"][4]

        for g in GRID:
            if g not in by:
                continue
            gt = _tmin(g)
            _, go, gh, gl, gc, gv = by[g]

            # ---------- FEATURES (bars <= g) ----------
            cum_v_g, cum_pv_g = cumv[g]
            vwap = (cum_pv_g / cum_v_g) if cum_v_g else None
            vwap_dev = ((gc - vwap) / vwap) if vwap else None
            or_pos = None
            if gt >= _tmin("09:30") and or_hi is not None and or_hi > or_lo:
                or_pos = (gc - or_lo) / (or_hi - or_lo)
            # cum_vol_vs_20d: this session's cumvol@g / avg prior-20 cumvol@g
            prior = [hm[g][0] for hm in hist if g in hm]
            cvr = (cum_v_g / (sum(prior) / len(prior))) if prior else None
            # rolling returns using minute-of-day offsets
            def mn(off):
                key = f"{(gt - off)//60:02d}:{(gt - off)%60:02d}"
                return by[key][4] if key in by else None
            c5 = mn(5)
            c15 = mn(15)
            r5 = (gc / c5 - 1.0) if c5 else None
            r15 = (gc / c15 - 1.0) if c15 else None
            feat_out.append((
                sym, day + " " + g + ":00",
                gt - _tmin("09:15"),
                (gc / open0915 - 1.0) if open0915 else None,
                or_pos, vwap_dev, cvr, r5, r15))

            # ---------- LABELS (bars strictly after g -> 15:29) ----------
            entry = gc
            if entry <= 0:
                continue
            fwd = [r for r in rows if r[0] > g]   # forward path
            for direction in ("long", "short"):
                sgn = 1.0 if direction == "long" else -1.0
                ret_close = sgn * (close1529 / entry - 1.0)
                mfe = mae = None
                peak = None
                m05 = m1 = None
                for hhmm, o, h, l, c, v in fwd:
                    if direction == "long":
                        fav = h / entry - 1.0
                        adv = l / entry - 1.0
                    else:
                        fav = -(l / entry - 1.0)
                        adv = -(h / entry - 1.0)
                    mfe = fav if mfe is None else max(mfe, fav)
                    mae = adv if mae is None else min(mae, adv)
                    peak = fav if peak is None else max(peak, fav)
                    if m05 is None and fav >= 0.005:
                        m05 = _tmin(hhmm) - gt
                    if m1 is None and fav >= 0.01:
                        m1 = _tmin(hhmm) - gt
                giveback = (peak - ret_close) if peak is not None else None
                lab_out.append((
                    sym, direction, day, g, entry, ret_close,
                    mfe, mae, m05, m1, peak, giveback))

        hist.append(cumv)   # this complete session becomes prior for next

    w.executemany(
        "INSERT INTO hulk_labels_intraday VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        lab_out)
    w.executemany(
        "INSERT INTO hulk_intraday_features VALUES(?,?,?,?,?,?,?,?,?)",
        feat_out)
    return len(lab_out), len(feat_out)


# ---------------------------------------------------------------- main
def main():
    print("HULK build start", datetime.now())
    print("SRC:", SRC)
    print("DST:", DST)
    assert os.path.exists(SRC), "source db missing"
    w = sqlite3.connect(DST)
    create_schema(w)
    sc = src_conn()
    counts = {}
    for sym in SYMBOLS:
        print("\n=== %s ===" % sym)
        bars = load_1min(sc, sym)
        drows = load_daily(sc, sym)
        # seal assertion at load level
        assert bars[-1][0] < SEAL + " 00:00:00", "SEAL VIOLATION 1min"
        assert drows[-1][0] < SEAL, "SEAL VIOLATION daily"
        print("  1min bars:", len(bars), "daily:", len(drows))
        sess = group_sessions(bars)
        sohlc = session_ohlc(sess)
        counts[(sym, "sessions")] = build_sessions(w, sym, sohlc)
        print("  sessions:", counts[(sym, "sessions")])
        counts[(sym, "multiday")] = build_multiday(w, sym, sohlc)
        print("  multiday labels:", counts[(sym, "multiday")])
        counts[(sym, "daily_feat")] = build_daily_features(w, sym, sohlc, drows)
        print("  daily features:", counts[(sym, "daily_feat")])
        li, fi = build_intraday(w, sym, sess, sohlc)
        counts[(sym, "intraday_lab")] = li
        counts[(sym, "intraday_feat")] = fi
        print("  intraday labels:", li, " intraday features:", fi)
        w.commit()
    sc.close()
    # indexes for downstream miners
    w.executescript(
        "CREATE INDEX IF NOT EXISTS ix_md_sym_dt ON hulk_labels_multiday(symbol, entry_date);"
        "CREATE INDEX IF NOT EXISTS ix_df_sym_dt ON hulk_daily_features(symbol, trade_date);"
        "CREATE INDEX IF NOT EXISTS ix_il_sym_dt ON hulk_labels_intraday(symbol, entry_date);"
        "CREATE INDEX IF NOT EXISTS ix_if_sym_bt ON hulk_intraday_features(symbol, bar_time);"
    )
    w.commit()
    w.close()
    print("\nDONE", datetime.now())
    print(counts)


if __name__ == "__main__":
    main()
