"""
Multi-timeframe IAS (Institutional Accumulation Score) — adapted from Kanida Intraday Lab.

WINDOW IS FIXED: last ~104 minutes of trading day (13:45 → 15:29 IST).
GRANULARITY VARIES: 1m / 5m / 15m / 30m bars of that same window.

Tests at which resolution the IAS-style signal shows the strongest causal evidence
of next-day return. Uses the 5 statistical tests from causation_engine.py:
  1. Welch's t-test on base rates
  2. Dose-response across IAS quintiles (Cochran-Armitage trend test)
  3. Multi-day forward returns D+1..D+5 with Bonferroni correction
  4. Granger causality F-test
  5. (OI interaction — skipped if OI table absent)
"""
from __future__ import annotations
import math, sqlite3, statistics
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, List, Dict, Tuple

# ─────────────────────────────────────────────────────────────────────
# Window + scaling
# ─────────────────────────────────────────────────────────────────────

WINDOW_START = "13:45"
WINDOW_END   = "15:29"
FORWARD_DAYS = 5
WIN_CLOSE_MIN = 1.0           # next-day close-to-close >= 1% = "up day"
IAS_SIGNAL_THRESHOLD = 5.5    # IAS >= this = signal day

# Min bars required in window per TF (rough thresholds; trade-off resolution vs reliability)
MIN_BARS_PER_TF = {
    "1min":  30,
    "5min":  10,
    "15min": 4,
    "30min": 2,
}


# ─────────────────────────────────────────────────────────────────────
# Stats helpers (no scipy)
# ─────────────────────────────────────────────────────────────────────

def _norm_cdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))

def welch_ttest(a: List[float], b: List[float]) -> Dict:
    if len(a) < 2 or len(b) < 2:
        return {"t":0,"p":1,"d":0,"n_a":len(a),"n_b":len(b),"mean_a":0,"mean_b":0}
    na, nb = len(a), len(b)
    ma, mb = statistics.mean(a), statistics.mean(b)
    va, vb = statistics.variance(a), statistics.variance(b)
    se = math.sqrt(va/na + vb/nb)
    if se == 0:
        return {"t":0,"p":1,"d":0,"n_a":na,"n_b":nb,"mean_a":ma,"mean_b":mb}
    t_stat = (ma - mb) / se
    # Approximate two-tailed p via normal (df typically large)
    p_val = 2 * (1 - _norm_cdf(abs(t_stat)))
    pooled = math.sqrt(((na-1)*va + (nb-1)*vb) / (na+nb-2))
    d = (ma - mb)/pooled if pooled > 0 else 0
    return {"t":round(t_stat,4),"p":round(p_val,6),"d":round(d,4),
            "n_a":na,"n_b":nb,"mean_a":round(ma,4),"mean_b":round(mb,4)}


def chi2_trend(hits: List[int], totals: List[int]) -> Dict:
    """Cochran-Armitage trend test for monotone dose-response."""
    n_groups = len(hits)
    if n_groups < 2 or sum(totals) == 0:
        return {"chi2":0, "p":1}
    total_hits = sum(hits); total_n = sum(totals)
    p_overall = total_hits / total_n
    if p_overall in (0, 1): return {"chi2":0, "p":1}
    scores = list(range(1, n_groups+1))
    mean_s = sum(scores[i]*totals[i] for i in range(n_groups)) / total_n
    num = sum(scores[i]*(hits[i] - totals[i]*p_overall) for i in range(n_groups))
    den = p_overall*(1-p_overall) * sum(totals[i]*(scores[i]-mean_s)**2 for i in range(n_groups))
    if den <= 0: return {"chi2":0, "p":1}
    z = num / math.sqrt(den)
    chi2 = z*z
    # one-df chi2 p-value via normal cdf approximation: p = 2*(1-norm_cdf(|z|))
    p = 2 * (1 - _norm_cdf(abs(z)))
    return {"chi2": round(chi2,4), "p": round(p,6), "z": round(z,4)}


# ─────────────────────────────────────────────────────────────────────
# Bar loaders
# ─────────────────────────────────────────────────────────────────────

def load_bars(con: sqlite3.Connection, symbol: str, tf: str) -> List[Dict]:
    table = f"ohlc_{tf}"
    rows = con.execute(f"""
        SELECT bar_time, open, high, low, close, volume
        FROM {table} WHERE symbol = ? ORDER BY bar_time
    """, (symbol,)).fetchall()
    return [{"bar_time": r[0], "trade_date": r[0][:10], "hm": r[0][11:16],
             "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5]}
            for r in rows]


def get_universe(con: sqlite3.Connection, index_col: str = "in_nifty200") -> List[str]:
    rows = con.execute(f"""
        SELECT symbol FROM universe_master
        WHERE is_active = 1 AND {index_col} = 1 ORDER BY symbol
    """).fetchall()
    return [r[0] for r in rows]


# ─────────────────────────────────────────────────────────────────────
# IAS computation (TF-agnostic; bars-in-window scales naturally)
# ─────────────────────────────────────────────────────────────────────

def vwap(bars: List[Dict]) -> float:
    tv = sum((b["high"]+b["low"]+b["close"])/3 * b["volume"] for b in bars)
    v  = sum(b["volume"] for b in bars)
    return tv / v if v else 0.0


def compute_ias(window_bars: List[Dict], day_bars: List[Dict],
                avg_20d_vol: float, prev_close: float, tf: str) -> float:
    """Compute IAS for one day's accumulation window.
    Window-bar count varies by TF (~104 at 1m, ~21 at 5m, ~7 at 15m, ~3 at 30m).
    Components scale relative to bars-in-window.
    """
    n_bars = len(window_bars)
    if n_bars < MIN_BARS_PER_TF.get(tf, 30):
        return 0.0
    if not day_bars:
        return 0.0

    all_vols = [b["volume"] for b in day_bars if b["volume"] > 0]
    avg_bar_vol = statistics.mean(all_vols) if all_vols else 1

    pb_vol = [b["volume"] for b in window_bars]
    pb_cls = [b["close"]  for b in window_bars]
    pb_hi  = [b["high"]   for b in window_bars]
    pb_lo  = [b["low"]    for b in window_bars]
    pb_op  = [b["open"]   for b in window_bars]

    # 1. Volume surge: last 1/3 of window vs day avg
    last_third = max(1, n_bars // 3)
    surge_vol = statistics.mean(pb_vol[-last_third:])
    vol_surge = surge_vol / avg_bar_vol if avg_bar_vol else 0

    # 2. Volume acceleration: 2nd half vs 1st half
    half = n_bars // 2
    vol_accel = (sum(pb_vol[half:]) / max(sum(pb_vol[:half]), 1)) if half > 0 else 1

    # 3. Price compression: range / close
    w_high, w_low, w_cls = max(pb_hi), min(pb_lo), pb_cls[-1]
    price_compress = (w_high - w_low) / w_cls * 100 if w_cls else 99

    # 4. Body compression: avg body / avg range
    bodies = [abs(b["close"]-b["open"]) for b in window_bars]
    ranges = [b["high"]-b["low"] for b in window_bars if b["high"] > b["low"]]
    body_ratio = (statistics.mean(bodies) / statistics.mean(ranges)
                  if ranges and statistics.mean(ranges) > 0 else 1.0)

    # 5. VWAP proximity
    day_vwap = vwap(day_bars)
    vwap_devs = [abs(b["close"]-day_vwap)/day_vwap*100 for b in window_bars if day_vwap > 0]
    vwap_prox = statistics.mean(vwap_devs) if vwap_devs else 99

    # 6. VWAP support
    above_vwap = sum(1 for b in window_bars if b["close"] >= day_vwap*0.998)
    vwap_support_r = above_vwap / n_bars

    # 7. Late-day vol share
    day_vol = sum(b["volume"] for b in day_bars)
    late_vol_pct = sum(pb_vol) / day_vol * 100 if day_vol else 0

    # 8. Absorption: vol_surge / |price move during window|
    price_move = abs(w_cls - pb_op[0]) / prev_close * 100 if prev_close else 99
    absorption = vol_surge / max(price_move, 0.01)

    # 9. Higher-low count
    hl_count = sum(1 for i in range(1, n_bars) if window_bars[i]["low"] > window_bars[i-1]["low"])
    hl_ratio = hl_count / max(n_bars - 1, 1)

    def clamp(x): return max(0.0, min(1.0, x))

    s = {
        "vol_surge":    clamp(vol_surge / 4.0),
        "vol_accel":    clamp((vol_accel - 1.0) / 2.0),
        "compress":     clamp(1 - price_compress / 3.0),
        "body":         clamp(1 - body_ratio),
        "vwap_prox":    clamp(1 - vwap_prox / 1.5),
        "vwap_support": vwap_support_r,
        "late_vol":     clamp(late_vol_pct / 40.0),
        "absorption":   clamp(absorption / 8.0),
        "hl":           hl_ratio,
    }
    w = {"vol_surge":2.5,"vol_accel":1.5,"compress":1.2,"body":0.8,
         "vwap_prox":0.8,"vwap_support":0.7,"late_vol":1.0,
         "absorption":1.0,"hl":0.5}
    return sum(s[k]*w[k] for k in w) / sum(w.values()) * 10


# ─────────────────────────────────────────────────────────────────────
# Per-ticker panel builder (worker)
# ─────────────────────────────────────────────────────────────────────

def _build_ticker_panel(args):
    symbol, tf, db_path = args
    con = sqlite3.connect(db_path, timeout=60)
    bars = load_bars(con, symbol, tf)
    con.close()
    if len(bars) < 50:
        return symbol, []

    # Group bars by day
    by_day: Dict[str, List[Dict]] = defaultdict(list)
    for b in bars:
        by_day[b["trade_date"]].append(b)
    sorted_days = sorted(by_day.keys())

    # Daily total volume for 20d rolling avg
    daily_vol = {d: sum(b["volume"] for b in by_day[d]) for d in sorted_days}

    panel = []
    for i in range(1, len(sorted_days)):
        d_str = sorted_days[i]
        prev_str = sorted_days[i-1]
        day_bars = by_day[d_str]
        prev_bars = by_day[prev_str]
        prev_close = prev_bars[-1]["close"] if prev_bars else 0
        if not prev_close or prev_close <= 0:
            continue

        # Window slice: 13:45-15:29 of TODAY
        window_bars = [b for b in day_bars
                       if WINDOW_START <= b["hm"] <= WINDOW_END]

        # 20d avg vol
        prev21 = sorted_days[max(0, i-21):i]
        avg20 = statistics.mean([daily_vol[d] for d in prev21]) if prev21 else 1

        ias = compute_ias(window_bars, day_bars, avg20, prev_close, tf)
        close_today = day_bars[-1]["close"]

        # Forward returns D+1..D+FORWARD_DAYS
        fwd = {}
        for fk in range(1, FORWARD_DAYS+1):
            if i + fk < len(sorted_days):
                fday = sorted_days[i+fk]
                fbars = by_day[fday]
                fclose = fbars[-1]["close"] if fbars else None
                fwd[f"fwd_d{fk}"] = round((fclose/close_today - 1)*100, 4) if fclose else None
            else:
                fwd[f"fwd_d{fk}"] = None

        panel.append({
            "ticker": symbol, "date": d_str, "tf": tf,
            "ias": round(ias, 3),
            "signal": 1 if ias >= IAS_SIGNAL_THRESHOLD else 0,
            "close": close_today,
            "vol_vs_20d": round(sum(b["volume"] for b in day_bars) / max(avg20,1), 3),
            "n_window_bars": len(window_bars),
            **fwd,
        })

    return symbol, panel


# ─────────────────────────────────────────────────────────────────────
# Causation tests (panel-level)
# ─────────────────────────────────────────────────────────────────────

def test1_base_rates(panel: List[Dict]) -> Dict:
    sig   = [r["fwd_d1"] for r in panel if r["signal"]==1 and r["fwd_d1"] is not None]
    nosig = [r["fwd_d1"] for r in panel if r["signal"]==0 and r["fwd_d1"] is not None]
    def hit(rs): return sum(1 for r in rs if r >= WIN_CLOSE_MIN)/len(rs) if rs else 0
    stat = welch_ttest(sig, nosig)
    return {
        "n_signal":   len(sig),  "n_nosignal": len(nosig),
        "hit_signal": round(hit(sig), 4),
        "hit_nosignal": round(hit(nosig), 4),
        "lift":       round(hit(sig)/hit(nosig), 3) if hit(nosig) > 0 else 0,
        "mean_signal":   round(statistics.mean(sig), 4) if sig else 0,
        "mean_nosignal": round(statistics.mean(nosig), 4) if nosig else 0,
        **stat,
    }


def test2_dose_response(panel: List[Dict], n_buckets: int = 5) -> List[Dict]:
    valid = [(r["ias"], r["fwd_d1"]) for r in panel if r["fwd_d1"] is not None]
    if len(valid) < n_buckets * 5:
        return []
    valid.sort(key=lambda x: x[0])
    bs = len(valid) // n_buckets
    out = []
    for b in range(n_buckets):
        lo, hi = b*bs, (b+1)*bs if b < n_buckets-1 else len(valid)
        grp = valid[lo:hi]
        ias_vals = [x[0] for x in grp]
        rets = [x[1] for x in grp]
        hits = sum(1 for r in rets if r >= WIN_CLOSE_MIN)
        out.append({
            "quintile": b+1, "n": len(grp),
            "ias_lo": round(min(ias_vals), 2), "ias_hi": round(max(ias_vals), 2),
            "hit_rate": round(hits/len(grp), 4),
            "mean_fwd_d1": round(statistics.mean(rets), 4),
            "hits": hits,
        })
    trend = chi2_trend([r["hits"] for r in out], [r["n"] for r in out])
    for r in out:
        r["trend_chi2"] = trend["chi2"]
        r["trend_p"]    = trend["p"]
    return out


def test3_multiday(panel: List[Dict]) -> List[Dict]:
    out = []
    for fk in range(1, FORWARD_DAYS+1):
        key = f"fwd_d{fk}"
        sig   = [r[key] for r in panel if r["signal"]==1 and r[key] is not None]
        nosig = [r[key] for r in panel if r["signal"]==0 and r[key] is not None]
        stat = welch_ttest(sig, nosig)
        out.append({
            "day": fk, "n_signal": len(sig), "n_nosignal": len(nosig),
            "mean_signal":   round(statistics.mean(sig), 4) if sig else 0,
            "mean_nosignal": round(statistics.mean(nosig), 4) if nosig else 0,
            "t": stat["t"], "p": stat["p"],
            "p_bonferroni": round(min(1.0, stat["p"] * FORWARD_DAYS), 6),
            "cohens_d": stat["d"],
        })
    return out


def test4_granger(panel: List[Dict]) -> Dict:
    """Granger causality test (pooled OLS): does IAS predict fwd_d1 beyond past returns?"""
    try:
        import numpy as np
    except ImportError:
        return {"error": "numpy required for Granger"}

    by_ticker = defaultdict(list)
    for r in panel: by_ticker[r["ticker"]].append(r)
    for t in by_ticker: by_ticker[t].sort(key=lambda x: x["date"])

    X_full, X_restr, y = [], [], []
    for t, rows in by_ticker.items():
        for i in range(2, len(rows)):
            r0 = rows[i]
            if r0["fwd_d1"] is None: continue
            l1 = rows[i-1].get("fwd_d1"); l2 = rows[i-2].get("fwd_d1")
            if l1 is None or l2 is None: continue
            ias = r0["ias"]
            X_full.append([1.0, l1, l2, ias])
            X_restr.append([1.0, l1, l2])
            y.append(r0["fwd_d1"])

    if len(y) < 30:
        return {"n": len(y), "error": "insufficient data"}

    X_full = np.array(X_full);  X_restr = np.array(X_restr); y = np.array(y)
    # OLS via lstsq
    b_full,  *_ = np.linalg.lstsq(X_full,  y, rcond=None)
    b_restr, *_ = np.linalg.lstsq(X_restr, y, rcond=None)
    rss_full  = np.sum((y - X_full  @ b_full)**2)
    rss_restr = np.sum((y - X_restr @ b_restr)**2)
    n = len(y); p_full = X_full.shape[1]; p_restr = X_restr.shape[1]
    df1, df2 = p_full - p_restr, n - p_full
    if df2 <= 0 or rss_full <= 0:
        return {"n": n, "error": "degenerate"}
    F = ((rss_restr - rss_full)/df1) / (rss_full/df2)
    # F-distribution p-value via approximation: convert to chi2 / df1 then use chi2 p-value
    # Use scipy if available, else rough approximation
    try:
        from scipy.stats import f as f_dist
        p = float(1 - f_dist.cdf(F, df1, df2))
    except ImportError:
        # Rough: use chi2(df1) approximation for large df2
        p = float(1 - _norm_cdf(math.sqrt(2*F*df1) - math.sqrt(2*df1 - 1))) if F > 0 else 1.0
    return {"n": n, "F": round(F, 4), "df1": df1, "df2": df2,
            "p": round(p, 6), "ias_coef": round(float(b_full[3]), 5)}


# ─────────────────────────────────────────────────────────────────────
# Top-level driver
# ─────────────────────────────────────────────────────────────────────

def run_multi_tf(db_path: Path, tfs: List[str], n_workers: int = 16) -> Dict:
    n_workers = max(10, min(48, n_workers))
    con = sqlite3.connect(db_path)
    universe = get_universe(con)
    con.close()
    print(f"[ias-multi] universe: {len(universe)} symbols", flush=True)
    print(f"[ias-multi] window:   {WINDOW_START} → {WINDOW_END} (fixed)", flush=True)
    print(f"[ias-multi] TFs:      {tfs}", flush=True)
    print(f"[ias-multi] workers:  {n_workers}", flush=True)

    results = {}
    for tf in tfs:
        # Quick presence check
        con = sqlite3.connect(db_path)
        cnt = con.execute(f"SELECT COUNT(*) FROM ohlc_{tf}").fetchone()[0]
        con.close()
        if cnt == 0:
            print(f"\n[ias-multi] === {tf}: NO DATA, skipping ===", flush=True)
            continue

        print(f"\n[ias-multi] === {tf}  ({cnt:,} bars in DB) ===", flush=True)
        args_list = [(s, tf, str(db_path)) for s in universe]
        panel: List[Dict] = []
        with ProcessPoolExecutor(max_workers=n_workers) as ex:
            futs = {ex.submit(_build_ticker_panel, a): a[0] for a in args_list}
            done = 0
            for f in as_completed(futs):
                try:
                    sym, p = f.result()
                except Exception as e:
                    print(f"  [{futs[f]}] ERROR: {e}", flush=True); continue
                panel.extend(p); done += 1
                if done % 30 == 0:
                    print(f"  [{tf}] {done}/{len(args_list)} symbols  panel={len(panel):,}", flush=True)
        print(f"  [{tf}] panel built: {len(panel):,} (ticker, day) rows", flush=True)

        # Run causation tests
        t1 = test1_base_rates(panel)
        t2 = test2_dose_response(panel)
        t3 = test3_multiday(panel)
        t4 = test4_granger(panel)
        results[tf] = {
            "panel_size": len(panel),
            "test1_base_rates": t1,
            "test2_dose_response": t2,
            "test3_multiday": t3,
            "test4_granger": t4,
        }
        print(f"  [{tf}] Test 1: hit_rate signal={t1.get('hit_signal',0)*100:.1f}% vs nosig={t1.get('hit_nosignal',0)*100:.1f}% "
              f"lift={t1.get('lift',0):.2f} p={t1.get('p','?')}", flush=True)
        if t2:
            mono_p = t2[0].get("trend_p", 1)
            print(f"  [{tf}] Test 2: dose-response trend p={mono_p}", flush=True)
        sig_days_t3 = sum(1 for r in t3 if r.get("p_bonferroni", 1) < 0.05)
        print(f"  [{tf}] Test 3: {sig_days_t3}/{FORWARD_DAYS} forward days Bonferroni-significant", flush=True)
        print(f"  [{tf}] Test 4: Granger F={t4.get('F','?')} p={t4.get('p','?')} (n={t4.get('n','?')})", flush=True)
    return results
