"""
Central ORDER ROUTING + trade simulation — THE single source of truth for which product each trade uses.
Enforces real Zerodha rules so we never model an un-executable trade:

  Equity Cash · CNC : LONG only  · overnight OK (unlimited hold) · 1x
  Equity Cash · MIS : LONG+SHORT · intraday only (square off ~15:20 same day) · 5x
  Derivatives · NRML: LONG+SHORT · overnight OK (F&O stocks ONLY) · 5x

Routing:
  long  1-day      -> MIS  (intraday)
  long  multi-day  -> CNC  (delivery, held overnight)
  short 1-day      -> MIS  (intraday)
  short multi-day  -> NRML if the stock is F&O (held overnight)
                      ELSE "daily MIS re-short": we can't hold a cash short overnight, so we re-enter an
                      intraday MIS short each day across the signal window. Captures intraday down-moves,
                      stays flat overnight (misses overnight gaps). This rescues short edges on non-F&O names.
"""
import numpy as np
LEV = {"MIS": 5.0, "CNC": 1.0, "NRML": 5.0}
COST = {"MIS": 0.08, "CNC": 0.15, "NRML": 0.05}


def route(direction, w, is_fno):
    """Return (product, mode). mode: 'single' = one held trade; 'daily' = re-enter intraday MIS each day."""
    long = (direction == "up")
    if long:
        return ("MIS", "single") if w == 1 else ("CNC", "single")
    if w == 1:
        return ("MIS", "single")
    return ("NRML", "single") if is_fno else ("MIS", "daily")


def label(product, mode):
    """Display bucket. Daily-reshort short campaigns are shown as 'MIS-Daily' so their multi-day span
    isn't confused with true single-session MIS (whose hold is ~1 day)."""
    return "MIS-Daily" if mode == "daily" else product


def sim_roc(O, H, L, C, i, direction, pct, w, product, mode):
    """Per-signal trade outcome from index i. Returns (net_roc_pct, hold_days, exit_idx) or None.
    net_roc is after costs, times leverage — the expectancy-native per-trade result."""
    n = len(O)
    if i + 1 >= n:
        return None
    long = (direction == "up")
    if mode == "daily":
        # non-F&O multi-day short = intraday MIS short re-entered each day across the window
        gsum = 0.0; days = 0; last = i
        for x in range(i + 1, min(i + 1 + w, n)):
            o, c = O[x], C[x]
            if not (np.isfinite(o) and o > 0):
                break
            gsum += (o - c) / o * 100.0          # intraday short gross (profit when close < open)
            days += 1; last = x
        if days == 0:
            return None
        net = (gsum - days * COST["MIS"]) * LEV["MIS"]    # one MIS round-trip cost per day
        return net, days, last
    # 'single': enter next open, exit on target touch within w (daily H/L) else time-exit at the w-th close
    e = O[i + 1]
    if not (np.isfinite(e) and e > 0):
        return None
    tgt = e * (1 + pct / 100) if long else e * (1 - pct / 100)
    ex = None; off = 1
    for k, x in enumerate(range(i + 1, min(i + 1 + w, n)), 1):
        off = k
        if long and H[x] >= tgt: ex = tgt; break
        if (not long) and L[x] <= tgt: ex = tgt; break
    if ex is None:
        ex = C[i + off]
    gross = (ex / e - 1) * 100 if long else (1 - ex / e) * 100
    return (gross - COST[product]) * LEV[product], off, i + off


def net_roc_series(frame, direction, pct, w, is_fno):
    """Per-day (net_roc[], hold[], product) for one target, using the legal F&O-aware router."""
    O = frame["_o"].values; H = frame["_h"].values; L = frame["_l"].values; C = frame["_c"].values
    n = len(O); net = np.full(n, np.nan); hold = np.full(n, np.nan)
    product, mode = route(direction, w, is_fno)
    for i in range(n - 1):
        r = sim_roc(O, H, L, C, i, direction, pct, w, product, mode)
        if r is not None:
            net[i] = r[0]; hold[i] = r[1]
    return net, hold, product
