"""
Falcon Engine — feature extractor.

For every (symbol, trade_date), compute ~50 numeric features describing the
market state on that day. These are the inputs the miner uses to discover
combinations that precede big-move outcomes.

Feature buckets:
  Price action (daily):     range%, close_loc, gap%, body/wick proportions
  Trend distance:           dist from 20/50/200 SMA, slope of 20/50 SMA
  Momentum:                 RSI(14), ROC over 5/20/60 days
  Volume:                   today vs 20d, 5d vs 20d, sub-75% count
  Range/volatility:         ATR(20)%, ATR ratio 5/20, sub-2.5/sub-3 range counts
  Higher-low / higher-high: counts in last 5 days
  Distance from N-day high: 10/20/60/120/252
  Weekly features:          weekly close vs 20-week SMA, weekly breakout, weekly range, weekly close_loc
  Sector relative:          stock 20d/60d return vs sector equal-weight index
  Market relative:          stock 20d/60d return vs Nifty 50 (proxied by top-10 large-cap mean)

All features are floats (or ints for counts). NULLs allowed where insufficient
history.
"""
from __future__ import annotations
import sqlite3, statistics
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional


SCHEMA = """
CREATE TABLE IF NOT EXISTS falcon_features (
    symbol TEXT NOT NULL, trade_date TEXT NOT NULL,
    -- Price action
    range_pct REAL, close_loc REAL, gap_pct REAL,
    body_pct REAL, upper_wick_pct REAL, lower_wick_pct REAL,
    -- Trend
    dist_sma_20 REAL, dist_sma_50 REAL, dist_sma_200 REAL,
    slope_sma_20 REAL, slope_sma_50 REAL,
    -- Momentum
    rsi_14 REAL, roc_5 REAL, roc_20 REAL, roc_60 REAL,
    -- Volume
    vol_vs_20d REAL, vol_5d_vs_20d REAL,
    n_sub_75v_7d INTEGER, n_sub_75v_20d INTEGER,
    -- Range / volatility
    atr_20_pct REAL, atr_5_vs_20 REAL,
    n_sub_2_5_range_7d INTEGER, n_sub_3_range_7d INTEGER,
    -- Patterns
    n_higher_lows_5d INTEGER, n_higher_highs_5d INTEGER,
    -- Distance from N-day high (negative = below high)
    dist_high_10 REAL, dist_high_20 REAL, dist_high_60 REAL,
    dist_high_120 REAL, dist_high_252 REAL,
    -- Weekly
    weekly_close_vs_sma20 REAL, weekly_breakout_20w INTEGER,
    weekly_range_pct REAL, weekly_close_loc REAL,
    -- Relative strength
    rs_sector_20d REAL, rs_sector_60d REAL,
    rs_market_20d REAL, rs_market_60d REAL,
    PRIMARY KEY (symbol, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_falcon_features_date ON falcon_features(trade_date);
"""


def ensure_schema(con: sqlite3.Connection):
    con.executescript(SCHEMA)
    con.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Math helpers
# ─────────────────────────────────────────────────────────────────────────────

def _sma(vals: List[float], n: int, end_idx: int) -> Optional[float]:
    """SMA of last n values ending at end_idx (inclusive)."""
    if end_idx + 1 < n: return None
    return sum(vals[end_idx - n + 1: end_idx + 1]) / n


def _rsi(closes: List[float], end_idx: int, period: int = 14) -> Optional[float]:
    if end_idx < period: return None
    gains, losses = 0.0, 0.0
    for i in range(end_idx - period + 1, end_idx + 1):
        d = closes[i] - closes[i - 1]
        if d >= 0: gains += d
        else: losses -= d
    avg_g = gains / period
    avg_l = losses / period
    if avg_l == 0: return 100.0
    rs = avg_g / avg_l
    return 100 - (100 / (1 + rs))


def _atr(bars: List[Dict], end_idx: int, n: int = 20) -> Optional[float]:
    if end_idx < n: return None
    trs = []
    for i in range(end_idx - n + 1, end_idx + 1):
        h, l, pc = bars[i]["high"], bars[i]["low"], bars[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / n


# ─────────────────────────────────────────────────────────────────────────────
# Per-symbol feature extraction
# ─────────────────────────────────────────────────────────────────────────────

def _features_for_symbol(symbol: str,
                           daily_bars: List[Dict],
                           weekly_bars: List[Dict],
                           sector_index: Dict[str, float],
                           market_index: Dict[str, float],
                           start_date: str, end_date: str) -> List[tuple]:
    """Compute features per day in [start_date, end_date]. Returns list of tuples
    matching falcon_features schema for executemany."""
    if len(daily_bars) < 252:
        return []

    closes = [b["close"] for b in daily_bars]
    highs  = [b["high"]  for b in daily_bars]
    lows   = [b["low"]   for b in daily_bars]
    opens  = [b["open"]  for b in daily_bars]
    vols   = [b["volume"] for b in daily_bars]

    # Index weekly bars by week_start
    weekly_close_by_date = {b["week_start"]: b["close"] for b in weekly_bars}
    weekly_dates_sorted = sorted(weekly_close_by_date)

    out = []
    for idx, bar in enumerate(daily_bars):
        d = bar["trade_date"]
        if d < start_date or d > end_date: continue
        if idx < 252: continue   # need 252 days for trailing-year high

        # ── Price action ─────────────────────────────────────────────
        range_pct = (bar["high"] - bar["low"]) / bar["close"] * 100 if bar["close"] > 0 else 0
        close_loc = (bar["close"] - bar["low"]) / (bar["high"] - bar["low"]) if bar["high"] > bar["low"] else 0.5
        gap_pct = (bar["open"] / closes[idx - 1] - 1) * 100 if closes[idx - 1] > 0 else 0
        body = abs(bar["close"] - bar["open"])
        rng = bar["high"] - bar["low"]
        body_pct = body / rng if rng > 0 else 0
        upper_wick_pct = (bar["high"] - max(bar["open"], bar["close"])) / rng if rng > 0 else 0
        lower_wick_pct = (min(bar["open"], bar["close"]) - bar["low"]) / rng if rng > 0 else 0

        # ── Trend ─────────────────────────────────────────────────────
        sma20 = _sma(closes, 20, idx)
        sma50 = _sma(closes, 50, idx)
        sma200 = _sma(closes, 200, idx)
        sma20_prev5 = _sma(closes, 20, idx - 5) if idx >= 25 else None
        sma50_prev5 = _sma(closes, 50, idx - 5) if idx >= 55 else None
        dist_sma_20 = (bar["close"] / sma20 - 1) * 100 if sma20 else None
        dist_sma_50 = (bar["close"] / sma50 - 1) * 100 if sma50 else None
        dist_sma_200 = (bar["close"] / sma200 - 1) * 100 if sma200 else None
        slope_sma_20 = (sma20 / sma20_prev5 - 1) * 100 if (sma20 and sma20_prev5) else None
        slope_sma_50 = (sma50 / sma50_prev5 - 1) * 100 if (sma50 and sma50_prev5) else None

        # ── Momentum ──────────────────────────────────────────────────
        rsi_14 = _rsi(closes, idx, 14)
        roc_5  = (bar["close"] / closes[idx - 5]  - 1) * 100 if idx >= 5  and closes[idx - 5]  > 0 else None
        roc_20 = (bar["close"] / closes[idx - 20] - 1) * 100 if idx >= 20 and closes[idx - 20] > 0 else None
        roc_60 = (bar["close"] / closes[idx - 60] - 1) * 100 if idx >= 60 and closes[idx - 60] > 0 else None

        # ── Volume ────────────────────────────────────────────────────
        vol_avg_20 = sum(vols[idx - 20: idx]) / 20 if idx >= 20 else None
        vol_avg_5  = sum(vols[idx - 5: idx])  / 5  if idx >= 5  else None
        vol_vs_20d = vols[idx] / vol_avg_20 if vol_avg_20 else None
        vol_5d_vs_20d = vol_avg_5 / vol_avg_20 if (vol_avg_5 and vol_avg_20) else None
        n_sub_75v_7d  = sum(1 for v in vols[idx - 6:idx + 1] if vol_avg_20 and v <= 0.75 * vol_avg_20) if vol_avg_20 else 0
        n_sub_75v_20d = sum(1 for v in vols[idx - 19:idx + 1] if vol_avg_20 and v <= 0.75 * vol_avg_20) if vol_avg_20 else 0

        # ── Range / volatility ───────────────────────────────────────
        atr_20 = _atr(daily_bars, idx, 20)
        atr_5  = _atr(daily_bars, idx, 5)
        atr_20_pct = atr_20 / bar["close"] * 100 if (atr_20 and bar["close"] > 0) else None
        atr_5_vs_20 = atr_5 / atr_20 if (atr_5 and atr_20) else None
        ranges = [(daily_bars[i]["high"] - daily_bars[i]["low"]) / daily_bars[i]["close"] * 100
                    if daily_bars[i]["close"] > 0 else 0
                    for i in range(idx - 6, idx + 1)]
        n_sub_2_5_range_7d = sum(1 for r in ranges if r <= 2.5)
        n_sub_3_range_7d   = sum(1 for r in ranges if r <= 3.0)

        # ── Higher-lows / higher-highs ───────────────────────────────
        n_hl5 = sum(1 for i in range(idx - 4, idx + 1) if i > 0 and lows[i]  > lows[i - 1])
        n_hh5 = sum(1 for i in range(idx - 4, idx + 1) if i > 0 and highs[i] > highs[i - 1])

        # ── Distance from N-day high ────────────────────────────────
        dh10  = (bar["close"] / max(highs[idx - 9: idx + 1])  - 1) * 100 if idx >= 9   else None
        dh20  = (bar["close"] / max(highs[idx - 19: idx + 1]) - 1) * 100 if idx >= 19  else None
        dh60  = (bar["close"] / max(highs[idx - 59: idx + 1]) - 1) * 100 if idx >= 59  else None
        dh120 = (bar["close"] / max(highs[idx - 119: idx + 1])- 1) * 100 if idx >= 119 else None
        dh252 = (bar["close"] / max(highs[idx - 251: idx + 1])- 1) * 100 if idx >= 251 else None

        # ── Weekly features ─────────────────────────────────────────
        # Find the week containing this date (use last weekly bar with week_start <= d)
        weekly_close_vs_sma20w = None
        weekly_breakout_20w = None
        weekly_range_pct = None
        weekly_close_loc = None
        applicable_weeks = [wd for wd in weekly_dates_sorted if wd <= d]
        if len(applicable_weeks) >= 21:
            current_w_idx = len(applicable_weeks) - 1
            current_wd = applicable_weeks[current_w_idx]
            current_wbar = next((b for b in weekly_bars if b["week_start"] == current_wd), None)
            if current_wbar:
                w_closes = [next(b["close"] for b in weekly_bars if b["week_start"] == w)
                              for w in applicable_weeks[-21:]]
                w_sma_20 = sum(w_closes[:-1]) / 20 if len(w_closes) >= 21 else None
                if w_sma_20 and w_sma_20 > 0:
                    weekly_close_vs_sma20w = (current_wbar["close"] / w_sma_20 - 1) * 100
                # Breakout: week's close exceeds prior 20-week high
                w_highs_prior = [next(b["high"] for b in weekly_bars if b["week_start"] == w)
                                    for w in applicable_weeks[-21:-1]]
                if w_highs_prior:
                    weekly_breakout_20w = 1 if current_wbar["close"] > max(w_highs_prior) else 0
                if current_wbar["close"] > 0:
                    weekly_range_pct = (current_wbar["high"] - current_wbar["low"]) / current_wbar["close"] * 100
                if current_wbar["high"] > current_wbar["low"]:
                    weekly_close_loc = (current_wbar["close"] - current_wbar["low"]) / (current_wbar["high"] - current_wbar["low"])

        # ── Relative strength ──────────────────────────────────────
        # Sector relative: stock 20d/60d return - sector 20d/60d return
        s_idx_today = sector_index.get(d)
        s_idx_20    = sector_index.get(daily_bars[idx - 20]["trade_date"]) if idx >= 20 else None
        s_idx_60    = sector_index.get(daily_bars[idx - 60]["trade_date"]) if idx >= 60 else None
        rs_sector_20d = None
        rs_sector_60d = None
        if roc_20 is not None and s_idx_today and s_idx_20 and s_idx_20 > 0:
            sec_ret_20 = (s_idx_today / s_idx_20 - 1) * 100
            rs_sector_20d = roc_20 - sec_ret_20
        if roc_60 is not None and s_idx_today and s_idx_60 and s_idx_60 > 0:
            sec_ret_60 = (s_idx_today / s_idx_60 - 1) * 100
            rs_sector_60d = roc_60 - sec_ret_60

        # Market relative
        m_idx_today = market_index.get(d)
        m_idx_20    = market_index.get(daily_bars[idx - 20]["trade_date"]) if idx >= 20 else None
        m_idx_60    = market_index.get(daily_bars[idx - 60]["trade_date"]) if idx >= 60 else None
        rs_market_20d = None
        rs_market_60d = None
        if roc_20 is not None and m_idx_today and m_idx_20 and m_idx_20 > 0:
            mk_ret_20 = (m_idx_today / m_idx_20 - 1) * 100
            rs_market_20d = roc_20 - mk_ret_20
        if roc_60 is not None and m_idx_today and m_idx_60 and m_idx_60 > 0:
            mk_ret_60 = (m_idx_today / m_idx_60 - 1) * 100
            rs_market_60d = roc_60 - mk_ret_60

        out.append((
            symbol, d,
            round(range_pct, 4), round(close_loc, 4), round(gap_pct, 4),
            round(body_pct, 4), round(upper_wick_pct, 4), round(lower_wick_pct, 4),
            _r(dist_sma_20), _r(dist_sma_50), _r(dist_sma_200),
            _r(slope_sma_20), _r(slope_sma_50),
            _r(rsi_14), _r(roc_5), _r(roc_20), _r(roc_60),
            _r(vol_vs_20d), _r(vol_5d_vs_20d),
            n_sub_75v_7d, n_sub_75v_20d,
            _r(atr_20_pct), _r(atr_5_vs_20),
            n_sub_2_5_range_7d, n_sub_3_range_7d,
            n_hl5, n_hh5,
            _r(dh10), _r(dh20), _r(dh60), _r(dh120), _r(dh252),
            _r(weekly_close_vs_sma20w), weekly_breakout_20w,
            _r(weekly_range_pct), _r(weekly_close_loc),
            _r(rs_sector_20d), _r(rs_sector_60d),
            _r(rs_market_20d), _r(rs_market_60d),
        ))

    return out


def _r(x):
    return None if x is None else round(x, 4)


# ─────────────────────────────────────────────────────────────────────────────
# Sector / market index pre-compute
# ─────────────────────────────────────────────────────────────────────────────

def build_sector_indices(con: sqlite3.Connection,
                           start_date: str, end_date: str) -> Dict[str, Dict[str, float]]:
    """Equal-weight daily price index per sector. Returns {sector: {date: index_value}}."""
    sectors_rows = con.execute("SELECT symbol, sector FROM falcon_sectors").fetchall()
    sym_to_sec = {s: sec for s, sec in sectors_rows}
    sectors = sorted(set(sym_to_sec.values()))

    indices: Dict[str, Dict[str, float]] = {sec: {} for sec in sectors}

    rows = con.execute("""
        SELECT trade_date, symbol, close FROM ohlc_daily
        WHERE trade_date BETWEEN ? AND ?
    """, (start_date, end_date)).fetchall()

    by_date: Dict[str, Dict[str, float]] = defaultdict(dict)
    for d, sym, cl in rows:
        if sym in sym_to_sec:
            by_date[d][sym] = cl

    # For each sector, compute equal-weight cumulative index (base 100)
    for sector in sectors:
        members = [s for s, sec in sym_to_sec.items() if sec == sector]
        # Compute daily mean of (today/yesterday - 1) across members
        member_prev: Dict[str, float] = {}
        idx_value = 100.0
        sorted_dates = sorted(by_date.keys())
        for d in sorted_dates:
            day = by_date[d]
            rets = []
            for m in members:
                if m in day and m in member_prev and member_prev[m] > 0:
                    rets.append(day[m] / member_prev[m] - 1)
                if m in day:
                    member_prev[m] = day[m]
            if rets:
                idx_value *= (1 + sum(rets) / len(rets))
            indices[sector][d] = idx_value
    return indices


def build_market_index(con: sqlite3.Connection,
                         start_date: str, end_date: str,
                         top_n: int = 50) -> Dict[str, float]:
    """Proxy for Nifty 50: equal-weight cumulative index of top-N most liquid stocks
    (by 60-day avg traded value as of start_date)."""
    rows = con.execute("""
        SELECT symbol, AVG(close * volume) AS av
        FROM ohlc_daily
        WHERE trade_date >= date(?, '-90 days') AND trade_date <= ?
        GROUP BY symbol
        ORDER BY av DESC
        LIMIT ?
    """, (start_date, start_date, top_n)).fetchall()
    members = [r[0] for r in rows]

    daily_rows = con.execute(f"""
        SELECT trade_date, symbol, close FROM ohlc_daily
        WHERE trade_date BETWEEN ? AND ?
          AND symbol IN ({','.join('?'*len(members))})
    """, (start_date, end_date, *members)).fetchall()

    by_date: Dict[str, Dict[str, float]] = defaultdict(dict)
    for d, sym, cl in daily_rows:
        by_date[d][sym] = cl

    member_prev: Dict[str, float] = {}
    idx = {}
    val = 100.0
    for d in sorted(by_date.keys()):
        day = by_date[d]
        rets = []
        for m in members:
            if m in day and m in member_prev and member_prev[m] > 0:
                rets.append(day[m] / member_prev[m] - 1)
            if m in day:
                member_prev[m] = day[m]
        if rets:
            val *= (1 + sum(rets) / len(rets))
        idx[d] = val
    return idx


# ─────────────────────────────────────────────────────────────────────────────
# Multi-worker driver
# ─────────────────────────────────────────────────────────────────────────────

def _worker(args) -> int:
    symbol, db_path, start_date, end_date, sector_idx, market_idx = args
    con = sqlite3.connect(db_path, timeout=60.0)
    daily = con.execute("""
        SELECT trade_date, open, high, low, close, volume FROM ohlc_daily
        WHERE symbol = ? ORDER BY trade_date
    """, (symbol,)).fetchall()
    weekly = con.execute("""
        SELECT week_start, open, high, low, close, volume FROM ohlc_weekly
        WHERE symbol = ? ORDER BY week_start
    """, (symbol,)).fetchall()
    con.close()

    daily_bars = [{"trade_date": r[0], "open": r[1], "high": r[2], "low": r[3],
                    "close": r[4], "volume": r[5]} for r in daily]
    weekly_bars = [{"week_start": r[0], "open": r[1], "high": r[2], "low": r[3],
                     "close": r[4], "volume": r[5]} for r in weekly]

    rows = _features_for_symbol(symbol, daily_bars, weekly_bars,
                                  sector_idx, market_idx, start_date, end_date)
    if not rows: return 0

    con = sqlite3.connect(db_path, timeout=60.0)
    con.executemany("""
        INSERT OR REPLACE INTO falcon_features VALUES
        (?,?, ?,?,?, ?,?,?, ?,?,?, ?,?, ?,?,?,?, ?,?, ?,?, ?,?, ?,?, ?,?, ?,?,?,?,?, ?,?, ?,?, ?,?, ?,?)
    """, rows)
    con.commit()
    con.close()
    return len(rows)


def extract_universe_features(db_path: Path, symbols: List[str],
                                start_date: str, end_date: str,
                                n_workers: int = 16) -> Dict:
    n_workers = max(10, min(48, n_workers))
    con = sqlite3.connect(db_path, timeout=60.0)
    ensure_schema(con)

    print(f"[features] Pre-computing sector indices...")
    sector_idx = build_sector_indices(con, start_date, end_date)
    print(f"[features] {len(sector_idx)} sectors")

    print(f"[features] Pre-computing market index (top-50 proxy)...")
    market_idx = build_market_index(con, start_date, end_date, top_n=50)
    print(f"[features] Market index dates: {len(market_idx)}")
    con.close()

    # Map sector to its index for each symbol's worker
    print(f"[features] Building sector lookup per symbol...")
    con = sqlite3.connect(db_path, timeout=60.0)
    sym_sector = {s: sec for s, sec in
                    con.execute("SELECT symbol, sector FROM falcon_sectors")}
    con.close()

    args_list = []
    for sym in symbols:
        sec = sym_sector.get(sym)
        sec_idx = sector_idx.get(sec, {}) if sec else {}
        args_list.append((sym, str(db_path), start_date, end_date, sec_idx, market_idx))

    print(f"[features] Launching {n_workers} workers for {len(args_list)} symbols ...")
    summary = {"done": 0, "rows_total": 0}
    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        futs = {ex.submit(_worker, a): a[0] for a in args_list}
        for f in as_completed(futs):
            sym = futs[f]
            try:
                rows = f.result()
            except Exception as e:
                print(f"  [{sym}] ERROR: {e}", flush=True); rows = 0
            summary["done"] += 1
            summary["rows_total"] += rows
            if summary["done"] % 50 == 0:
                print(f"  [{summary['done']}/{len(args_list)}] rows so far={summary['rows_total']:,}",
                       flush=True)
    return summary
