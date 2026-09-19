"""
KANIDA Algorithm 1 — Agent 1: THE CHART AGENT  (first end-to-end slice)
=======================================================================
An autonomous research agent whose specialty is CHART PATTERNS. It runs the full
Algorithm-1 loop on a pattern trigger:

    IDENTIFY  -> detect the pattern point-in-time (no look-ahead)
    DRAW      -> render the pattern on a chart card (like the moneycontrol SUPER PRO cards)
    EXPERIMENT-> backtest the pattern PER STOCK: forward-return distribution, EV/ETV,
                 win rate, PF, MAE/MFE, regime splits (Market Worlds), recency, OOS,
                 vs the stock's own baseline, after costs  (the KANIDA difference:
                 moneycontrol only draws+narrates; we prove whether it is PROFITABLE)
    PUBLISH   -> a plain-language finding + evidence card + status
                 (NEW / CONFIRMED / WATCH / DECAYING / FAILED), posted under the
                 agent's influencer profile for its subscribers.

This slice implements ONE pattern — "Horizontal Trendline · Breakout-Retest + Volume"
(the ⑦ card) — on ONE stock (RELIANCE, daily). It is built as a clean seam so more
patterns and the 1-min tier drop in later.

Guardrails honored: point-in-time detection, costs, discovery-vs-OOS split, no tiny-
sample promotion, parameter-neighbourhood stability, baseline comparison. Never
fabricates a passing result.

Run:  python scripts/chart_agent.py --symbol RELIANCE
"""
from __future__ import annotations
import os, json, argparse, sqlite3, math
from dataclasses import dataclass, field, asdict
from typing import Optional
import numpy as np, pandas as pd

DB = os.path.join(os.path.dirname(__file__), "..", "db", "kanida.db")
OUT = os.path.join(os.path.dirname(__file__), "..", "reports", "chart_agent")
NIFTY = "NIFTY 50"
COST = 0.003          # 30 bps round-trip
HORIZONS = [1, 3, 5]  # trading-day holds measured from entry (next open)

# Detector parameters — one place so single-stock and universe runs stay identical.
PARAMS = dict(L=5, level_window=120, min_touches=2, tol=0.01, buffer=0.002,
              vol_mult=1.3, retest_vol_mult=1.0, vol_win=20, retest_max=15, retest_tol=0.012)

# A liquid, full-history basket for pooled evidence (skipped gracefully if a name is absent).
BASKET = ["RELIANCE", "INFY", "SBIN", "TCS", "HDFCBANK", "ICICIBANK", "AXISBANK", "KOTAKBANK",
          "LT", "ITC", "HINDUNILVR", "BHARTIARTL", "BAJFINANCE", "MARUTI", "ASIANPAINT",
          "SUNPHARMA", "TITAN", "ULTRACEMCO", "WIPRO", "HCLTECH", "TECHM", "POWERGRID", "NTPC",
          "ONGC", "COALINDIA", "TATASTEEL", "JSWSTEEL", "HINDALCO", "GRASIM", "CIPLA", "DRREDDY",
          "NESTLEIND", "BRITANNIA", "EICHERMOT", "HEROMOTOCO", "M&M", "TATAMOTORS", "ADANIPORTS",
          "BPCL", "IOC", "TATACONSUM", "PIDILITIND", "DABUR", "GAIL", "VEDL", "SAIL"]

# The agent's persistent identity (the influencer layer from the Algorithm-1 doc).
AGENT = {
    "id": "chart-agent-01",
    "name": "ChartSense",
    "specialty": "chart patterns — breakouts, trendlines, triangles",
    "voice": "evidence-first technical analyst; shows the receipts, admits the failures",
    "tagline": "I don't just draw patterns. I prove whether they pay.",
}


# ----------------------------------------------------------------------------- DATA
def load_daily(symbol: str) -> pd.DataFrame:
    con = sqlite3.connect(f"file:{os.path.abspath(DB)}?mode=ro", uri=True)
    q = ("SELECT substr(bar_time,1,10) date, open, high, low, close, volume "
         "FROM ohlc_daily WHERE symbol=? ORDER BY bar_time")
    df = pd.read_sql_query(q, con, params=(symbol,))
    nf = pd.read_sql_query(
        "SELECT substr(bar_time,1,10) date, close FROM ohlc_daily WHERE symbol=? ORDER BY bar_time",
        con, params=(NIFTY,))
    con.close()
    df["date"] = pd.to_datetime(df["date"]); df = df.set_index("date")
    nf["date"] = pd.to_datetime(nf["date"]); nf = nf.set_index("date")["close"]
    df["nifty"] = nf.reindex(df.index).ffill()
    return df.dropna(subset=["open", "high", "low", "close"])


def market_worlds(df: pd.DataFrame) -> dict:
    """5 regime masks aligned to df.index — the Algorithm-1 Market Worlds (robustness)."""
    nf = df["nifty"]; idx = df.index
    ma200 = nf.rolling(200).mean()
    rv = nf.pct_change().rolling(20).std() * np.sqrt(252)
    above = (nf > ma200).values
    hv = (rv > rv.quantile(0.75)).values
    unseen = np.asarray((idx >= "2018-01-01") & (idx < "2020-01-01"))
    lowliq = (df["volume"] < df["volume"].quantile(0.25)).values
    return {"Normal": above & ~hv & ~unseen, "High-Vol": hv, "Bear": ~above,
            "Low-Liquidity": lowliq, "Unseen 18-19": unseen}


# ------------------------------------------------------------------------- DETECTOR
@dataclass
class PatternEvent:
    signal_idx: int          # day the pattern confirms (decision timestamp)
    entry_idx: int           # next open (trade entry)
    breakout_idx: int
    retest_idx: int
    level: float             # the horizontal resistance R
    win_start: int           # window for drawing
    touches: list = field(default_factory=list)   # pivot-high indices that define the level
    pattern: str = "Horizontal Trendline"
    tag: str = "Breakout-Retest + Volume"
    direction: str = "long"
    timeframe: str = "1D"


def _pivot_highs(high: np.ndarray, L: int) -> np.ndarray:
    """Boolean array; pivot high at i confirmed using +/-L bars (known only at i+L)."""
    n = len(high); piv = np.zeros(n, bool)
    for i in range(L, n - L):
        if high[i] == high[i - L:i + L + 1].max():
            piv[i] = True
    return piv


def detect_horizontal_breakout_retest(
    df: pd.DataFrame, L=5, level_window=120, min_touches=2, tol=0.01, buffer=0.002,
    vol_mult=1.3, retest_vol_mult=1.0, vol_win=20, retest_max=15, retest_tol=0.012,
) -> list[PatternEvent]:
    """Point-in-time detector for the ⑦ pattern: a horizontal resistance touched >=2x,
    a VOLUME breakout above it, then a pullback that RETESTS the reclaimed level and
    reverses up (confirmation = signal day). Breakout requires strong volume; the retest
    reversal requires at-or-above-average volume. Entry = next open."""
    o, h, l, c, v = (df[k].values for k in ["open", "high", "low", "close", "volume"])
    n = len(c)
    piv = _pivot_highs(h, L)
    avgvol = pd.Series(v).rolling(vol_win).mean().values
    events: list[PatternEvent] = []
    b = level_window
    while b < n:
        # confirmed pivot highs inside the lookback window (known strictly before b)
        pv = [i for i in range(max(0, b - level_window), b - L) if piv[i]]
        if len(pv) < min_touches or not np.isfinite(avgvol[b]):
            b += 1; continue
        prices = np.array([h[i] for i in pv])
        # find the horizontal level (>=min_touches pivots within tol) that b is breaking
        R = None; touch_idx = []
        for cand in np.sort(prices)[::-1]:
            m = np.abs(prices - cand) <= tol * cand
            if m.sum() < min_touches:
                continue
            lvl = float(prices[m].mean())
            if not (c[b - 1] <= lvl * (1 + buffer) < c[b]):     # today is the first close above
                continue
            ti = [pv[k] for k in range(len(pv)) if m[k]]
            # clean FLAT TOP: no daily close above the level between the first touch and the breakout
            if c[min(ti):b].max() > lvl * (1 + buffer):
                continue
            R = lvl; touch_idx = ti; break
        if R is None or v[b] <= vol_mult * avgvol[b]:           # breakout needs volume
            b += 1; continue
        # look for a retest+confirmation within retest_max days after the breakout
        fired = None
        for d in range(b + 1, min(b + 1 + retest_max, n - 1)):
            touched = l[d] <= R * (1 + retest_tol)              # pulled back to the level
            holds = c[d] >= R * (1 - retest_tol)
            confirm = c[d] > c[d - 1] and c[d] > R and np.isfinite(avgvol[d]) and v[d] > retest_vol_mult * avgvol[d]
            if touched and holds and confirm:
                fired = d; break
        if fired is not None and fired + 1 < n:
            wstart = max(0, min([b] + touch_idx) - 10)          # window starts where the level formed
            events.append(PatternEvent(signal_idx=fired, entry_idx=fired + 1, breakout_idx=b,
                                       retest_idx=fired, level=float(R), touches=touch_idx,
                                       win_start=wstart))
            b = fired + 2                                       # dedup: resume past the event
        else:
            b += 1
    return events


# ----------------------------------------------------------------------- EXPERIMENT
def _fwd(o, c, e: int, hor: int, n: int) -> Optional[float]:
    if e + hor - 1 >= n or o[e] <= 0: return None
    return c[e + hor - 1] / o[e] - 1 - COST


def _mae_mfe(o, hi, lo, e: int, hor: int, n: int):
    if e + hor - 1 >= n: return None, None
    seg_hi = hi[e:e + hor].max(); seg_lo = lo[e:e + hor].min()
    return (seg_lo / o[e] - 1), (seg_hi / o[e] - 1)            # MAE (worst), MFE (best)


def _card(rets: np.ndarray) -> Optional[dict]:
    r = rets[np.isfinite(rets)]
    if len(r) == 0: return None
    wins, loss = r[r > 0], r[r <= 0]
    return {
        "n": int(len(r)), "win": round(float((r > 0).mean()) * 100, 1),
        "ev": round(float(r.mean()) * 100, 3), "med": round(float(np.median(r)) * 100, 3),
        "avg_win": round(float(wins.mean()) * 100, 2) if len(wins) else 0.0,
        "avg_loss": round(float(loss.mean()) * 100, 2) if len(loss) else 0.0,
        "pf": round(float(wins.sum() / -loss.sum()), 2) if loss.sum() < 0 else None,
    }


def run_experiment(df: pd.DataFrame, events: list[PatternEvent], detector_kwargs: dict) -> dict:
    o, hi, lo, c = (df[k].values for k in ["open", "high", "low", "close"])
    n = len(c); worlds = market_worlds(df)
    ev_entries = np.array([e.entry_idx for e in events], int)
    sig_idx = np.array([e.signal_idx for e in events], int)
    dates = df.index

    def rets_for(entries, hor):
        return np.array([_fwd(o, c, int(e), hor, n) for e in entries], float)

    all_entries = np.arange(1, n)   # baseline: enter every day's next open
    horizons = {}
    for hor in HORIZONS:
        ev_r = rets_for(ev_entries, hor)
        base_r = rets_for(all_entries, hor)
        card = _card(ev_r) or {}
        base_mean = float(np.nanmean(base_r)) * 100
        card["baseline_ev"] = round(base_mean, 3)
        card["edge"] = round((card.get("ev", 0) - base_mean), 3) if card else None
        horizons[f"T+{hor}"] = card

    # headline horizon = T+3
    H = 3
    ev_r3 = rets_for(ev_entries, H)
    # MAE / MFE (risk texture) on T+3
    maes, mfes = [], []
    for e in ev_entries:
        a, f = _mae_mfe(o, hi, lo, int(e), H, n)
        if a is not None: maes.append(a); mfes.append(f)
    mae = round(float(np.mean(maes)) * 100, 2) if maes else None
    mfe = round(float(np.mean(mfes)) * 100, 2) if mfes else None

    # regime splits on T+3
    regime = {}
    for w, mask in worlds.items():
        sel = mask[sig_idx]
        rr = ev_r3[sel]
        cd = _card(rr)
        if cd: regime[w] = {"n": cd["n"], "win": cd["win"], "ev": cd["ev"]}

    # recency: last 365d of events vs before  (T+3)
    cutoff = dates.max() - pd.Timedelta(days=365)
    recent_mask = np.asarray(dates[sig_idx] >= cutoff)
    rec = _card(ev_r3[recent_mask]); old = _card(ev_r3[~recent_mask])

    # OOS: discovery (<=2021) vs validation (>=2022)  (T+3)
    oos_mask = np.asarray(dates[sig_idx] >= "2022-01-01")
    disc = _card(ev_r3[~oos_mask]); val = _card(ev_r3[oos_mask])

    # parameter-neighbourhood stability: re-run at vol_mult +/- and report T+3 EV spread
    stab = []
    for vm in (detector_kwargs.get("vol_mult", 1.5) * f for f in (0.85, 1.0, 1.15)):
        kw = {**detector_kwargs, "vol_mult": vm}
        evs = detect_horizontal_breakout_retest(df, **kw)
        rr = rets_for(np.array([e.entry_idx for e in evs], int), H)
        cd = _card(rr)
        if cd: stab.append(cd["ev"])
    stability = {"ev_T3_across_vol_mult": [round(x, 3) for x in stab],
                 "spread": round(max(stab) - min(stab), 3) if len(stab) > 1 else None}

    return {"horizons": horizons, "mae_T3": mae, "mfe_T3": mfe, "regime": regime,
            "recency": {"recent_365d": rec, "prior": old},
            "oos": {"discovery_<=2021": disc, "validation_>=2022": val},
            "stability": stability}


# ------------------------------------------------------------------ POOLED (universe)
def collect(df: pd.DataFrame, events: list[PatternEvent]) -> tuple[list, dict]:
    """Per-event records + this stock's pooled baseline returns, for universe aggregation."""
    o, hi, lo, c = (df[k].values for k in ["open", "high", "low", "close"])
    n = len(c); worlds = market_worlds(df); dates = df.index
    recs = []
    for e in events:
        rec = {"date": dates[e.signal_idx], "world": None}
        for hor in HORIZONS:
            rec[hor] = _fwd(o, c, e.entry_idx, hor, n)
        a, f = _mae_mfe(o, hi, lo, e.entry_idx, 3, n); rec["mae"], rec["mfe"] = a, f
        for w, m in worlds.items():
            if m[e.signal_idx]: rec["world"] = w; break
        recs.append(rec)
    base = {hor: np.array([_fwd(o, c, i, hor, n) for i in range(1, n)], float) for hor in HORIZONS}
    return recs, base


def build_pooled(recs: list, base_list: list) -> dict:
    base_pooled = {hor: np.concatenate([b[hor] for b in base_list]) for hor in HORIZONS}
    horizons = {}
    for hor in HORIZONS:
        r = np.array([rc[hor] for rc in recs], float)
        card = _card(r) or {}
        bmean = float(np.nanmean(base_pooled[hor])) * 100
        card["baseline_ev"] = round(bmean, 3)
        card["edge"] = round(card.get("ev", 0) - bmean, 3) if card else None
        horizons[f"T+{hor}"] = card
    r3 = np.array([rc[3] for rc in recs], float)
    maes = [rc["mae"] for rc in recs if rc["mae"] is not None]
    mfes = [rc["mfe"] for rc in recs if rc["mfe"] is not None]
    dates = pd.to_datetime([rc["date"] for rc in recs])
    regime = {}
    for w in ["Normal", "High-Vol", "Bear", "Low-Liquidity", "Unseen 18-19"]:
        sel = np.array([rc["world"] == w for rc in recs])
        cd = _card(r3[sel])
        if cd: regime[w] = {"n": cd["n"], "win": cd["win"], "ev": cd["ev"]}
    cutoff = dates.max() - pd.Timedelta(days=365)
    rmask = np.asarray(dates >= cutoff)
    omask = np.asarray(dates >= "2022-01-01")
    return {"horizons": horizons,
            "mae_T3": round(float(np.mean(maes)) * 100, 2) if maes else None,
            "mfe_T3": round(float(np.mean(mfes)) * 100, 2) if mfes else None,
            "regime": regime,
            "recency": {"recent_365d": _card(r3[rmask]), "prior": _card(r3[~rmask])},
            "oos": {"discovery_<=2021": _card(r3[~omask]), "validation_>=2022": _card(r3[omask])},
            "stability": {"ev_T3_across_vol_mult": [], "spread": None}}


def run_universe(symbols: list[str]) -> dict:
    all_recs, base_list, per_stock = [], [], {}
    for s in symbols:
        try:
            df = load_daily(s)
        except Exception:
            continue
        if len(df) < 400:
            continue
        evs = detect_horizontal_breakout_retest(df, **PARAMS)
        if not evs:
            continue
        recs, base = collect(df, evs)
        all_recs += recs; base_list.append(base)
        per_stock[s] = {"n": len(evs), "ev_T3": (_card(np.array([r[3] for r in recs], float)) or {}).get("ev")}
    if not all_recs:
        print("no occurrences across the basket."); return {}
    exp = build_pooled(all_recs, base_list)
    status, reason = classify(exp)
    finding = finding_text(f"{len(per_stock)}-stock basket", exp, status, reason)
    t3 = exp["horizons"]["T+3"]
    print("=" * 92)
    print(f"  {AGENT['name']} — POOLED EXPERIMENT: Horizontal Trendline · Breakout-Retest + Volume · 1D")
    print("=" * 92)
    print(f"  basket: {len(per_stock)} stocks · {t3['n']} pooled occurrences  ->  [{status}]")
    for k in ("T+1", "T+3", "T+5"):
        c = exp["horizons"][k]
        print(f"    {k:4} n={c.get('n',0):4}  win {c.get('win',0):5}%  EV {c.get('ev',0):+.2f}%  "
              f"edge {c.get('edge',0):+.2f}%  PF {c.get('pf')}")
    print(f"    MFE/MAE (T+3): {exp['mfe_T3']:+.2f}% / {exp['mae_T3']:+.2f}%")
    print(f"    regimes: " + " | ".join(f"{w}:{d['ev']:+.2f}%(n{d['n']})" for w, d in exp["regime"].items()))
    r, o_ = exp["recency"]["recent_365d"], exp["recency"]["prior"]
    print(f"    recency T+3 EV: recent {r['ev'] if r else 'NA'} vs prior {o_['ev'] if o_ else 'NA'}")
    d, val = exp["oos"]["discovery_<=2021"], exp["oos"]["validation_>=2022"]
    print(f"    OOS T+3 EV: discovery {d['ev'] if d else 'NA'} vs validation {val['ev'] if val else 'NA'}")
    top = sorted((p for p in per_stock.items() if p[1]["ev_T3"] is not None), key=lambda x: -x[1]["ev_T3"])[:6]
    print("    best stocks (T+3 EV): " + " | ".join(f"{s}:{v['ev_T3']:+.2f}%(n{v['n']})" for s, v in top))
    print("-" * 92)
    print("  FINDING:", finding)
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, "POOLED_horizontal_breakout_retest.json"), "w") as f:
        json.dump({"agent": AGENT, "universe": list(per_stock), "evidence": exp,
                   "status": status, "reason": reason, "finding": finding,
                   "per_stock": per_stock, "params": PARAMS}, f, indent=2)
    return exp


def pattern_evidence(df: pd.DataFrame, events: list[PatternEvent], max_h: int = 10) -> Optional[dict]:
    """The 'what happens next' evidence: forward cumulative-return PATHS T+0..T+max_h for every
    occurrence, per-horizon win%/median/quartiles, favorable (MFE) & adverse (MAE) excursions,
    up/down split and magnitudes, and target hit-rates. This is what the trader visualises."""
    o, hi, lo, c = (df[k].values for k in ["open", "high", "low", "close"])
    n = len(c)
    paths, mfe, mae = [], {h: [] for h in range(1, max_h + 1)}, {h: [] for h in range(1, max_h + 1)}
    for e in events:
        s = e.entry_idx
        if s + max_h - 1 >= n or o[s] <= 0:
            continue
        path = [0.0] + [c[s + d - 1] / o[s] - 1 - COST for d in range(1, max_h + 1)]
        paths.append(path)
        for h in range(1, max_h + 1):
            mfe[h].append(hi[s:s + h].max() / o[s] - 1)
            mae[h].append(lo[s:s + h].min() / o[s] - 1)
    if not paths:
        return None
    P = np.array(paths)
    horizons = {}
    for h in range(1, max_h + 1):
        col = P[:, h]
        horizons[h] = {"win": round(float((col > 0).mean()) * 100, 1),
                       "mean": round(float(col.mean()) * 100, 2),
                       "median": round(float(np.median(col)) * 100, 2),
                       "p25": round(float(np.percentile(col, 25)) * 100, 2),
                       "p75": round(float(np.percentile(col, 75)) * 100, 2),
                       "mfe": round(float(np.mean(mfe[h])) * 100, 2),
                       "mae": round(float(np.mean(mae[h])) * 100, 2)}
    ref = P[:, max_h]; ups = ref[ref > 0]; downs = ref[ref <= 0]
    mfeR, maeR = np.array(mfe[max_h]), np.array(mae[max_h])
    summary = {"n": len(P), "ref_h": max_h,
               "pct_up": round(float((ref > 0).mean()) * 100, 1),
               "pct_down": round(float((ref <= 0).mean()) * 100, 1),
               "avg_up": round(float(ups.mean()) * 100, 2) if len(ups) else 0.0,
               "avg_down": round(float(downs.mean()) * 100, 2) if len(downs) else 0.0,
               "hit_up2": round(float((mfeR >= 0.02).mean()) * 100, 1),
               "hit_up5": round(float((mfeR >= 0.05).mean()) * 100, 1),
               "hit_dn2": round(float((maeR <= -0.02).mean()) * 100, 1),
               "hit_dn5": round(float((maeR <= -0.05).mean()) * 100, 1)}
    return {"paths": P.tolist(), "horizons": horizons, "summary": summary, "ref_h": max_h}


def classify(exp: dict) -> tuple[str, str]:
    """Status lifecycle + one-line reason — honest, sample- and edge-aware."""
    t3 = exp["horizons"].get("T+3", {})
    n = t3.get("n", 0); ev = t3.get("ev", 0.0); edge = t3.get("edge", 0.0)
    rec = exp["recency"]["recent_365d"]; old = exp["recency"]["prior"]
    if n < 20:
        return "WATCH", f"only {n} historical occurrences — insufficient evidence to trust yet."
    if ev is None or ev <= 0:
        return "FAILED", f"negative expectancy after costs (EV {ev:+.2f}% over T+3)."
    if edge is not None and edge <= 0:
        return "WATCH", f"profitable (+{ev:.2f}% T+3) but no edge over simply holding the stock ({edge:+.2f}%)."
    if rec and old and rec["ev"] < 0.5 * old["ev"]:
        return "DECAYING", f"edge fading — recent EV {rec['ev']:+.2f}% vs {old['ev']:+.2f}% historically."
    return "CONFIRMED", f"positive expectancy (+{ev:.2f}% T+3, +{edge:.2f}% edge) across {n} occurrences."


# --------------------------------------------------------------------------- PUBLISH
def finding_text(symbol: str, exp: dict, status: str, reason: str) -> str:
    t1, t3, t5 = (exp["horizons"].get(k, {}) for k in ("T+1", "T+3", "T+5"))
    return (
        f"{symbol} · Horizontal Trendline breakout with retest + volume. "
        f"When this exact setup fired before: {t3.get('n',0)} times, "
        f"{t3.get('win',0)}% closed higher by T+3 for {t3.get('ev',0):+.2f}% avg after costs "
        f"({t3.get('edge',0):+.2f}% vs the stock's own baseline). "
        f"T+1 {t1.get('ev',0):+.2f}% · T+5 {t5.get('ev',0):+.2f}%. "
        f"Typical heat before it works: MFE {exp['mfe_T3']:+.2f}% / MAE {exp['mae_T3']:+.2f}%. "
        f"[{status}] {reason}"
    )


# ---------------------------------------------------------------------------- RENDER
PALETTE = dict(GREEN="#00c98a", RED="#ff4d6d", INK="#0b0b14", GRID="#202036",
               TXT="#f4f4fc", GOLD="#ffd166", BLUE="#4da3ff")
STATUS_COLOR = {"CONFIRMED": "#00c98a", "WATCH": "#ffd166", "DECAYING": "#ff9f43",
                "FAILED": "#ff4d6d", "NEW": "#4da3ff", "FORMING": "#4da3ff", "WORKED": "#00c98a"}


def _candles(ax, w, x, P):
    o, h, l, c = (w[k].values for k in ["open", "high", "low", "close"])
    up = c >= o
    from matplotlib.patches import Rectangle
    for i in range(len(w)):
        col = P["GREEN"] if up[i] else P["RED"]
        ax.plot([x[i], x[i]], [l[i], h[i]], color=col, lw=0.9, zorder=2)
        ax.add_patch(Rectangle((x[i] - 0.32, min(o[i], c[i])), 0.64, max(abs(c[i] - o[i]), 1e-6),
                               facecolor=col, edgecolor=col, zorder=3))
    return up


def render_chart(df: pd.DataFrame, a: int, z: int, symbol: str, status: str, caption: str,
                 chip: str, view_label: str, path: str, event: Optional[PatternEvent] = None):
    """One reference-style card. Draws candles + volume; if `event` is given, overlays the
    resistance ONLY across its active span (first touch -> just past the retest), the touch
    points, and the breakout/retest markers anchored to the level. Year-stamped x-axis."""
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    P = PALETTE; a = max(0, a); z = min(z, len(df) - 1)
    w = df.iloc[a:z + 1]; x = np.arange(len(w)); h, l, v = w["high"].values, w["low"].values, w["volume"].values

    fig, (axp, axv) = plt.subplots(2, 1, figsize=(12, 6.6), gridspec_kw={"height_ratios": [3.2, 1]},
                                   sharex=True, facecolor=P["INK"])
    for ax in (axp, axv):
        ax.set_facecolor(P["INK"])
        for s in ax.spines.values(): s.set_color(P["GRID"])
        ax.tick_params(colors="#8888a8", labelsize=8); ax.grid(True, color=P["GRID"], lw=0.5, alpha=0.5)
    up = _candles(axp, w, x, P)
    axv.bar(x, v, color=[P["GREEN"] if u else P["RED"] for u in up], alpha=0.6, width=0.7)
    axv.set_ylabel("vol", color="#8888a8", fontsize=8)

    if event is not None:
        lvl = event.level
        touch_rel = [t - a for t in event.touches if a <= t <= z]
        # resistance line spans only where it is active: first touch -> a little past the retest
        lo = min(touch_rel) if touch_rel else max(0, event.breakout_idx - a)
        hi = min(len(w) - 1, (event.retest_idx - a) + 12)
        lo = max(0, lo)
        if hi > lo:
            axp.hlines(lvl, lo, hi, color=P["GOLD"], ls="--", lw=1.5, zorder=1)
            axp.text(hi, lvl, "  resistance", color=P["GOLD"], va="center", fontsize=8)
        for t in touch_rel:
            axp.scatter([t], [h[t]], marker="o", s=30, facecolor="none", edgecolor=P["GOLD"], lw=1.2, zorder=4)
        bi, ri = event.breakout_idx - a, event.retest_idx - a
        if 0 <= bi < len(w): axp.scatter([bi], [lvl * 0.994], marker="^", s=130, color=P["GREEN"], zorder=6, label="breakout")
        if 0 <= ri < len(w): axp.scatter([ri], [lvl * 0.994], marker="o", s=95, color=P["BLUE"], zorder=6, label="retest")
        axp.legend(loc="upper left", fontsize=8, facecolor=P["INK"], edgecolor=P["GRID"], labelcolor=P["TXT"])

    scol = STATUS_COLOR.get(status, P["TXT"])
    axp.text(0.008, 1.065, chip, transform=axp.transAxes, fontsize=11.5, fontweight="bold", color=P["TXT"],
             va="bottom", bbox=dict(boxstyle="round,pad=0.5", facecolor="#151528", edgecolor=P["GRID"]))
    axp.text(0.008, 1.012, f"{symbol} · {view_label}", transform=axp.transAxes, fontsize=9,
             color="#8888a8", va="bottom")
    axp.text(0.992, 1.065, f"[{status}]", transform=axp.transAxes, fontsize=11.5, fontweight="bold",
             color=scol, va="bottom", ha="right")
    fig.text(0.99, 0.965, f"KANIDA · {AGENT['name']}", color="#8888a8", fontsize=8, ha="right")
    fig.text(0.012, 0.02, caption, color="#d6d6ea", fontsize=8.4, ha="left")
    step = max(1, len(w) // 9)
    labels = [w.index[i].strftime("%d-%b-%y") if i % step == 0 else "" for i in range(len(w))]
    axv.set_xticks(x); axv.set_xticklabels(labels, rotation=0, fontsize=7.5)
    plt.subplots_adjust(left=0.06, right=0.965, top=0.88, bottom=0.13, hspace=0.08)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fig.savefig(path, dpi=130, facecolor=P["INK"]); plt.close(fig)


def _insights_stats(ev):
    """Derive the tiles, the winning/losing cohort paths, the best window, and the bullets."""
    P = np.array(ev["paths"]) * 100; H = ev["ref_h"]; s = ev["summary"]; hz = ev["horizons"]
    ref = P[:, H]; wm = ref > 0; lm = ~wm
    win_path = P[wm].mean(axis=0) if wm.any() else None      # avg path of occurrences that ENDED up
    lose_path = P[lm].mean(axis=0) if lm.any() else None     # avg path of occurrences that ENDED down
    best = None                                              # contiguous horizons with strongest mean
    for wdt in (3, 4, 5):
        for a in range(1, H - wdt + 2):
            avg = float(np.mean([hz[h]["mean"] for h in range(a, a + wdt)]))
            if best is None or avg > best[0]:
                best = (avg, a, a + wdt - 1)
    bw = (best[1], best[2])
    bullets = []
    bullets.append("Pattern often works, but follow-through is not immediate."
                   if hz[1]["mean"] < hz[bw[1]]["mean"] else
                   "Reacts fast — most of the move lands in the first day or two.")
    bullets.append(f"Best holding window: T+{bw[0]} to T+{bw[1]} after entry.")
    bullets.append("If it fails, weakness usually shows up early."
                   if (lose_path is not None and lose_path[1] < -0.2) else
                   "Failures tend to bleed slowly rather than break down at once.")
    return {"P": P, "H": H, "s": s, "hz": hz, "win_path": win_path, "lose_path": lose_path,
            "bw": bw, "bullets": bullets}


def render_evidence(symbol: str, ev: dict, status: str, finding: str, path: str, chip: str):
    """Insights card: 4 headline tiles + Typical Winning/Losing outcome paths + plain-English takeaways."""
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import FancyBboxPatch
    d = _insights_stats(ev)
    H, s, hz = d["H"], d["s"], d["hz"]; win_path, lose_path, bw = d["win_path"], d["lose_path"], d["bw"]
    days = np.arange(d["P"].shape[1])
    Pc = PALETTE; INK, GRID, TXT, GREEN, RED, GOLD = (Pc[k] for k in ["INK", "GRID", "TXT", "GREEN", "RED", "GOLD"])
    scol = STATUS_COLOR.get(status, TXT)

    fig = plt.figure(figsize=(12.4, 7.0), facecolor=INK)
    fig.text(0.04, 0.94, chip, color=TXT, fontsize=15.5, fontweight="bold", ha="left")
    fig.text(0.04, 0.90, f"{symbol} · simple setup view", color="#8888a8", fontsize=9.5, ha="left")
    fig.text(0.965, 0.94, f"[{status}]", color=scol, fontsize=14, fontweight="bold", ha="right")

    # four headline tiles
    def tile(x, label, num, sub, c):
        fig.patches.append(FancyBboxPatch((x, 0.70), 0.207, 0.155, transform=fig.transFigure,
                           boxstyle="round,pad=0.006,rounding_size=0.02", facecolor="#141426",
                           edgecolor=GRID, lw=1.2, zorder=0))
        fig.text(x + 0.016, 0.822, label, color="#8888a8", fontsize=9.5, ha="left", va="center")
        fig.text(x + 0.016, 0.772, num, color=c, fontsize=21, fontweight="bold", ha="left", va="center")
        fig.text(x + 0.016, 0.723, sub, color="#8888a8", fontsize=8, ha="left", va="center")
    xs = [0.04, 0.278, 0.516, 0.754]
    tile(xs[0], "Win Rate", f"{s['pct_up']:.1f}%", "by T+10", GREEN if s["pct_up"] >= 50 else RED)
    tile(xs[1], "Avg Return", f"{hz[H]['mean']:+.2f}%", "by T+10", GREEN if hz[H]["mean"] >= 0 else RED)
    tile(xs[2], "Best Window", f"T+{bw[0]} to T+{bw[1]}", "strongest follow-through", GOLD)
    tile(xs[3], "Risk", f"{hz[H]['mae']:.2f}%", "avg adverse move", RED)

    # Typical Outcome Paths — winners vs losers (honest cohorts)
    ax = fig.add_axes([0.055, 0.27, 0.905, 0.36]); ax.set_facecolor(INK)
    for sp in ax.spines.values(): sp.set_color(GRID)
    ax.tick_params(colors="#8888a8", labelsize=8); ax.grid(True, color=GRID, lw=0.5, alpha=0.3)
    ax.axhline(0, color="#8888a8", lw=0.9, ls="--")
    if win_path is not None:
        ax.plot(days, win_path, color=GREEN, lw=2.2, marker="o", ms=4.5, label="Typical Winning Path", zorder=4)
        for x in range(1, H + 1):
            ax.annotate(f"{win_path[x]:+.2f}", (x, win_path[x]), color=GREEN, fontsize=6.8, ha="center", va="bottom", xytext=(0, 4), textcoords="offset points")
    if lose_path is not None:
        ax.plot(days, lose_path, color=RED, lw=2.2, marker="o", ms=4.5, label="Typical Losing Path", zorder=4)
        for x in range(1, H + 1):
            ax.annotate(f"{lose_path[x]:+.2f}", (x, lose_path[x]), color=RED, fontsize=6.8, ha="center", va="top", xytext=(0, -5), textcoords="offset points")
    ax.set_title("Typical Outcome Paths", color=TXT, fontsize=11.5, fontweight="bold", loc="left", pad=6)
    ax.set_xticks(days); ax.set_xticklabels([("T+" + str(x)) if x > 0 else "entry" for x in days], fontsize=7.5)
    ax.set_ylabel("cumulative return %", color="#8888a8", fontsize=9)
    ax.legend(loc="upper left", fontsize=8.5, facecolor=INK, edgecolor=GRID, labelcolor=TXT)

    # plain-English takeaways
    fig.text(0.045, 0.185, "What it means", color=GOLD, fontsize=10.5, fontweight="bold", ha="left")
    for i, b in enumerate(d["bullets"]):
        fig.text(0.055, 0.135 - i * 0.045, "•  " + b, color="#d6d6ea", fontsize=10, ha="left", va="center")
    note = f"Based on {s['n']} occurrences — " + ("small sample, treat as an early signal." if s["n"] < 20 else "solid sample.")
    fig.text(0.965, 0.03, f"{note}   ·   KANIDA · {AGENT['name']}", color="#8888a8", fontsize=8, ha="right")

    os.makedirs(os.path.dirname(path), exist_ok=True)
    fig.savefig(path, dpi=130, facecolor=INK); plt.close(fig)


def render_history_map(df: pd.DataFrame, events: list[PatternEvent], symbol: str, status: str,
                       path: str, max_h: int = 10):
    """Every historical occurrence marked on the price line — GREEN where it worked, RED where it
    failed — so the trader sees the pattern's full track record on the actual chart."""
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter
    from matplotlib.lines import Line2D
    o, c = df["open"].values, df["close"].values; n = len(c); idx = df.index; x = np.arange(n)
    Pc = PALETTE; INK, GRID, TXT, GREEN, RED, GOLD = (Pc[k] for k in ["INK", "GRID", "TXT", "GREEN", "RED", "GOLD"])
    scol = STATUS_COLOR.get(status, TXT)

    fig = plt.figure(figsize=(13, 6.3), facecolor=INK)
    ax = fig.add_axes([0.06, 0.13, 0.90, 0.70]); ax.set_facecolor(INK)
    for sp in ax.spines.values(): sp.set_color(GRID)
    ax.tick_params(colors="#8888a8", labelsize=8); ax.grid(True, color=GRID, lw=0.5, alpha=0.3)
    ax.plot(x, c, color="#6a6a86", lw=1.0, zorder=1)

    wins = losses = 0
    for e in events:
        s = e.entry_idx
        if s + max_h - 1 >= n:
            continue
        r = (c[s + max_h - 1] / o[s] - 1 - COST) * 100
        bi = e.breakout_idx
        if r > 0:
            wins += 1
            ax.scatter(bi, c[bi], marker="^", s=120, color=GREEN, edgecolor="white", lw=0.5, zorder=5)
            ax.annotate(f"+{r:.1f}%", (bi, c[bi]), color=GREEN, fontsize=7, ha="center", va="bottom",
                        xytext=(0, 8), textcoords="offset points")
        else:
            losses += 1
            ax.scatter(bi, c[bi], marker="v", s=120, color=RED, edgecolor="white", lw=0.5, zorder=5)
            ax.annotate(f"{r:.1f}%", (bi, c[bi]), color=RED, fontsize=7, ha="center", va="top",
                        xytext=(0, -10), textcoords="offset points")

    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0f}"))
    step = max(1, n // 9); ticks = list(range(0, n, step))
    ax.set_xticks(ticks); ax.set_xticklabels([idx[t].strftime("%Y") for t in ticks])
    ax.set_ylabel("price (log)", color="#8888a8", fontsize=9)
    leg = [Line2D([0], [0], marker="^", color="none", markerfacecolor=GREEN, markersize=11, label="worked — closed higher by T+10"),
           Line2D([0], [0], marker="v", color="none", markerfacecolor=RED, markersize=11, label="failed — closed lower")]
    ax.legend(handles=leg, loc="upper left", fontsize=8.5, facecolor=INK, edgecolor=GRID, labelcolor=TXT)

    fig.text(0.06, 0.93, f"{symbol} · Horizontal Trendline · Breakout-Retest + Volume", color=TXT, fontsize=14, fontweight="bold")
    fig.text(0.06, 0.895, f"Every time this pattern formed — green worked, red failed   "
             f"({wins} worked / {losses} failed)", color=GOLD, fontsize=10.5, fontweight="bold")
    fig.text(0.965, 0.93, f"[{status}]", color=scol, fontsize=13, fontweight="bold", ha="right")
    fig.text(0.965, 0.03, f"KANIDA · {AGENT['name']}", color="#8888a8", fontsize=8, ha="right")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fig.savefig(path, dpi=130, facecolor=INK); plt.close(fig)


# ------------------------------------------------------------------------------ MAIN
def run(symbol: str = "RELIANCE") -> dict:
    df = load_daily(symbol)
    kw = dict(PARAMS)
    events = detect_horizontal_breakout_retest(df, **kw)
    if not events:
        print(f"[{symbol}] no pattern occurrences found."); return {}
    exp = run_experiment(df, events, kw)
    status, reason = classify(exp)
    headline = finding_text(symbol, exp, status, reason)

    os.makedirs(OUT, exist_ok=True)
    ev = events[-1]; n = len(df)
    sig_date = df.index[ev.signal_idx].strftime("%d-%b-%Y")
    # (1) pattern card — neighbourhood of the most recent detected occurrence
    img = os.path.join(OUT, f"{symbol}_horizontal_breakout_retest.png")
    render_chart(df, ev.win_start, min(ev.retest_idx + 25, n - 1), symbol, status, headline,
                 chip=f"{ev.pattern}  |  {ev.tag}", view_label=f"pattern @ {sig_date}", path=img, event=ev)
    # (2) latest-bars view — lines up with a live TradingView chart
    img_latest = os.path.join(OUT, f"{symbol}_latest.png")
    last_close = float(df["close"].iloc[-1]); last_date = df.index[-1].strftime("%d-%b-%Y")
    ev_in = ev if ev.retest_idx >= n - 180 else None
    latest_caption = (f"{symbol} · latest {min(180, n)} sessions · close Rs {last_close:.1f} on {last_date}. "
                      f"Most recent detected pattern: {ev.pattern} ({ev.tag}) on {sig_date} -> [{status}]. "
                      f"NOTE: data ends {last_date} (engine snapshot) — a live feed may be a few sessions ahead.")
    render_chart(df, n - min(180, n), n - 1, symbol, status, latest_caption,
                 chip=f"{symbol} · latest price action", view_label="1D · last ~180 sessions",
                 path=img_latest, event=ev_in)
    # (3) historical-evidence card — the "what happened next" view (T+0..T+10)
    # (0) history map — every occurrence on the price chart, green=worked / red=failed
    img_map = os.path.join(OUT, f"{symbol}_history_map.png")
    render_history_map(df, events, symbol, status, img_map)
    evid = pattern_evidence(df, events, max_h=10)
    img_evidence = os.path.join(OUT, f"{symbol}_evidence.png")
    img_story = None
    if evid:
        render_evidence(symbol, evid, status, headline, img_evidence,
                        chip=f"{ev.pattern} · {ev.tag}")
        # (4) combined STORY card — pattern chart on top, insights below (the trader's single view)
        try:
            from PIL import Image
            top, bot = Image.open(img).convert("RGB"), Image.open(img_evidence).convert("RGB")
            W = max(top.width, bot.width)
            rz = lambda im: im if im.width == W else im.resize((W, round(im.height * W / im.width)))
            top, bot = rz(top), rz(bot)
            story = Image.new("RGB", (W, top.height + bot.height), (11, 11, 20))
            story.paste(top, (0, 0)); story.paste(bot, (0, top.height))
            img_story = os.path.join(OUT, f"{symbol}_story.png"); story.save(img_story)
        except Exception as e:
            print(f"  (story card skipped: {e})")

    card = {
        "agent": AGENT, "symbol": symbol, "pattern": "Horizontal Trendline",
        "tag": "Breakout-Retest + Volume", "timeframe": "1D", "direction": "long",
        "occurrences": len(events),
        "data_range": [df.index.min().strftime("%Y-%m-%d"), df.index.max().strftime("%Y-%m-%d")],
        "last_signal_date": df.index[events[-1].signal_idx].strftime("%Y-%m-%d"),
        "last_level": round(events[-1].level, 2),
        "evidence": exp, "status": status, "reason": reason, "finding": headline,
        "detector_params": kw, "cost_bps": COST * 10000,
        "evidence_curve": evid,
        "image": os.path.abspath(img), "image_latest": os.path.abspath(img_latest),
        "image_evidence": os.path.abspath(img_evidence) if evid else None,
        "image_story": os.path.abspath(img_story) if img_story else None,
        "image_history_map": os.path.abspath(img_map),
    }
    with open(os.path.join(OUT, f"{symbol}_horizontal_breakout_retest.json"), "w") as f:
        json.dump(card, f, indent=2)

    # console publish
    print("=" * 92)
    print(f"  {AGENT['name']} ({AGENT['id']}) — {AGENT['tagline']}")
    print("=" * 92)
    print(f"  {symbol} · Horizontal Trendline · Breakout-Retest + Volume · 1D   ->  [{status}]")
    print(f"  occurrences: {len(events)}  ({card['data_range'][0]} -> {card['data_range'][1]})")
    for k in ("T+1", "T+3", "T+5"):
        c = exp["horizons"][k]
        print(f"    {k:4} n={c.get('n',0):4}  win {c.get('win',0):5}%  EV {c.get('ev',0):+.2f}%  "
              f"edge {c.get('edge',0):+.2f}%  PF {c.get('pf')}")
    print(f"    MFE/MAE (T+3): {exp['mfe_T3']:+.2f}% / {exp['mae_T3']:+.2f}%")
    print(f"    regimes: " + " | ".join(f"{w}:{d['ev']:+.2f}%(n{d['n']})" for w, d in exp["regime"].items()))
    r, o_ = exp["recency"]["recent_365d"], exp["recency"]["prior"]
    print(f"    recency T+3 EV: recent {r['ev'] if r else 'NA'} vs prior {o_['ev'] if o_ else 'NA'}")
    d, val = exp["oos"]["discovery_<=2021"], exp["oos"]["validation_>=2022"]
    print(f"    OOS T+3 EV: discovery {d['ev'] if d else 'NA'} vs validation {val['ev'] if val else 'NA'}")
    print(f"    stability (T+3 EV across vol_mult): {exp['stability']['ev_T3_across_vol_mult']}")
    print("-" * 92)
    print("  FINDING:", headline)
    print(f"  pattern card  -> {img}")
    print(f"  latest view   -> {img_latest}")
    if evid: print(f"  evidence card -> {img_evidence}")
    return card


def build_story4(symbol: str) -> Optional[str]:
    """The 4-section story, all four charts drawn identically:
       1) current forming setup   2) evidence + data
       3) a historical WIN example   4) a historical FAIL example."""
    df = load_daily(symbol); n = len(df)
    events = detect_horizontal_breakout_retest(df, **PARAMS)
    if len(events) < 3:
        print(f"{symbol}: not enough occurrences for a 4-section story."); return None
    o, c = df["open"].values, df["close"].values
    t10 = lambda e: (c[e.entry_idx + 9] / o[e.entry_idx] - 1 - COST) * 100
    resolved = [e for e in events if e.entry_idx + 9 < n]
    win_ev, fail_ev, current = max(resolved, key=t10), min(resolved, key=t10), events[-1]
    exp = run_experiment(df, events, dict(PARAMS)); status, _ = classify(exp)
    evid = pattern_evidence(df, events, 10)
    chip = f"{current.pattern} · {current.tag}"
    os.makedirs(OUT, exist_ok=True)

    def date(e): return df.index[e.signal_idx].strftime("%d-%b-%Y")
    cur_pending = current.entry_idx + 9 >= n
    p1 = os.path.join(OUT, f"{symbol}_s1_forming.png")
    render_chart(df, current.win_start, min(current.retest_idx + 25, n - 1), symbol,
                 "FORMING" if cur_pending else status,
                 caption=f"SECTION 1 — {symbol}: this setup completed on {date(current)}"
                         + (" — outcome still open (forming)." if cur_pending else "."),
                 chip=chip, view_label=f"FORMING NOW · signal {date(current)}", path=p1, event=current)

    p2 = os.path.join(OUT, f"{symbol}_s2_evidence.png")
    render_evidence(symbol, evid, status, "", p2, chip=chip)

    wr, fr = t10(win_ev), t10(fail_ev)
    p3 = os.path.join(OUT, f"{symbol}_s3_worked.png")
    render_chart(df, win_ev.win_start, min(win_ev.entry_idx + 13, n - 1), symbol, "WORKED",
                 caption=f"SECTION 3 — a time it WORKED · {date(win_ev)}: broke out, retested, then ran +{wr:.1f}% by T+10.",
                 chip=chip, view_label=f"WORKED · {date(win_ev)} → +{wr:.1f}% by T+10", path=p3, event=win_ev)

    p4 = os.path.join(OUT, f"{symbol}_s4_failed.png")
    render_chart(df, fail_ev.win_start, min(fail_ev.entry_idx + 13, n - 1), symbol, "FAILED",
                 caption=f"SECTION 4 — a time it FAILED · {date(fail_ev)}: same setup, but it rolled over for {fr:.1f}% by T+10.",
                 chip=chip, view_label=f"FAILED · {date(fail_ev)} → {fr:.1f}% by T+10", path=p4, event=fail_ev)

    try:
        from PIL import Image
        imgs = [Image.open(p).convert("RGB") for p in [p1, p2, p3, p4]]
        W = max(i.width for i in imgs)
        imgs = [i if i.width == W else i.resize((W, round(i.height * W / i.width))) for i in imgs]
        story = Image.new("RGB", (W, sum(i.height for i in imgs)), (11, 11, 20))
        y = 0
        for i in imgs:
            story.paste(i, (0, y)); y += i.height
        out = os.path.join(OUT, f"{symbol}_story4.png"); story.save(out)
        print(f"  4-section story -> {out}")
        return out
    except Exception as e:
        print(f"  (stack skipped: {e})"); return None


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="RELIANCE")
    ap.add_argument("--universe", action="store_true", help="run the pooled basket experiment")
    ap.add_argument("--story4", action="store_true", help="build the 4-section story card")
    args = ap.parse_args()
    if args.universe:
        run_universe(BASKET)
    elif args.story4:
        build_story4(args.symbol)
    else:
        run(args.symbol)
