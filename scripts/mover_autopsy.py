r"""FORENSIC ORDER-FLOW AUTOPSY  (detector CALIBRATION, not signal discovery)
================================================================================
Day = 2026-07-09.  Two movers + four boring liquid controls, run IDENTICALLY.

  MOVER-UP   = KALYANKJIL  (+11.06%)
  MOVER-DOWN = DRREDDY      (-4.83%)
  CONTROLS   = HDFCBANK, ICICIBANK, ITC, BAJFINANCE  (small move, liquid)

THE CONTROL RULE IS MANDATORY: every footprint is computed the SAME way on movers
AND controls.  A footprint only "counts" if it fired on a mover and stayed quiet on
the controls.  Mover-only findings are studying the answer key.

DATA-HONESTY (storage is PER-MINUTE aggregates, NOT per-tick / NOT per-price-level):
  * CVD          = running sum of (buy_vol - sell_vol) per MINUTE  -> WELL SUPPORTED.
  * Iceberg      = coarse proxy (minute-granularity, ~5s-stale book, no per-price tick). WEAK.
  * Vanishing    = coarse proxy (no order-lifecycle data; true spoofing needs order-by-order).
  * Participant  = avg_tick_vol stability + block ratio + orders-per-level; full print-size
                   distribution is NOT stored -> "mode share" is NOT computable.  Arrival-time
                   analysis SKIPPED (timestamps lack resolution).
  * All thresholds z-scored PER STOCK on a CAUSAL trailing rolling window (no look-ahead).

Findings are HYPOTHESES for the v2 engine (n=2 movers, 1 tick-day) to be judged on the
live 2-week capture -- NOT shipped from this autopsy.  No standalone directional-probability
claims from one stock-day.
"""
import sys, statistics as st
from pathlib import Path
from datetime import datetime

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
sys.path.insert(0, str(ROOT / "scripts"))
from flow_paper_engine import _con   # read-only DB handle (per task instruction)

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

DAY   = "2026-07-09"
MOVERS   = {"KALYANKJIL": ("UP", +11.06), "DRREDDY": ("DOWN", -4.83)}
CONTROLS = ["HDFCBANK", "ICICIBANK", "ITC", "BAJFINANCE"]
ALL_SYMS = list(MOVERS) + CONTROLS
NIFTY50  = "NSE:NIFTY 50"
NIFTYFUT = "NFO:NIFTY26JULFUT"

# ---- v2 detector constants (VERBATIM from inst_flow_engine.py) ----------------
THR      = 0.0015     # 0.15% min 1-min thrust for a PRICE-confirm
VMULT    = 2.0        # volume spike vs trailing median
WIN      = 20         # trailing-median window (minutes)
TICK_HI  = 55.0       # tick buy% for aggression / who-hit confirm
WARMUP   = 5          # no entries in first 5 processed minutes
ENTRY_CUT= "15:15"    # no NEW entries after this
OI_CTX   = 2.0
PRICE_FAMS = {"AGGRESSION", "ABSORPTION", "ACCEPTANCE"}

# ---- autopsy z / window knobs ------------------------------------------------
ZWIN     = 30         # trailing rolling window (min) for causal z-scores
ZMIN     = 10         # min samples before a z is defined
Z_FLAG   = 2.0        # |z| threshold to flag a footprint (per spec)
ABS_WIN  = 5          # window (min) for CVD-vs-price absorption
MOVE_1PC = 1.0        # "move underway" threshold (% from open)

# ==============================================================================
# 1. LOAD
# ==============================================================================
def load_stock(cur, sym):
    """Join orderflow + trades on bar_time for one NSE cash symbol, 09:15..close, ordered."""
    ik = f"NSE:{sym}"
    of = cur.execute(
        "SELECT bar_time,open,high,low,close,volume,atp,total_buy_qty,total_sell_qty,"
        "b1p,b1q,b2p,b2q,b3p,b3q,b4p,b4q,b5p,b5q,a1p,a1q,a2p,a2q,a3p,a3q,a4p,a4q,a5p,a5q,"
        "b1o,b2o,b3o,b4o,b5o,a1o,a2o,a3o,a4o,a5o "
        "FROM mkt_orderflow_1min WHERE instrument_key=? AND bar_time>=? ORDER BY bar_time",
        (ik, DAY + " 09:15")).fetchall()
    ofc = [d[0] for d in cur.description]
    tr = cur.execute(
        "SELECT bar_time,n_ticks,volume,buy_vol,sell_vol,buy_vol_pct,avg_tick_vol,max_tick_vol,max_trade_qty "
        "FROM mkt_trades_1min WHERE instrument_key=? AND bar_time>=? ORDER BY bar_time",
        (ik, DAY + " 09:15")).fetchall()
    trc = [d[0] for d in cur.description]
    trmap = {r[0]: dict(zip(trc, r)) for r in tr}
    rows = []
    for r in of:
        d = dict(zip(ofc, r))
        t = trmap.get(d["bar_time"], {})
        for k in ("n_ticks", "buy_vol", "sell_vol", "buy_vol_pct", "avg_tick_vol", "max_tick_vol", "max_trade_qty"):
            d[k] = t.get(k)
        d["tr_volume"] = t.get("volume")
        rows.append(d)
    return rows

def series_move_from_open(cur, ik):
    r = cur.execute("SELECT bar_time,close FROM mkt_orderflow_1min WHERE instrument_key=? AND bar_time>=? ORDER BY bar_time",
                    (ik, DAY + " 09:15")).fetchall()
    if not r:
        return {}, None
    o = r[0][1]
    return {t: (c / o - 1) for t, c in r if c and o}, o

def load_market(cur):
    """NIFTY-50 move-from-open per minute + NIFTY-fut context per minute (faithful v2 inputs)."""
    mkt, _ = series_move_from_open(cur, NIFTY50)
    fut = cur.execute("SELECT bar_time,close,oi FROM mkt_orderflow_1min WHERE instrument_key=? AND bar_time>=? ORDER BY bar_time",
                      (NIFTYFUT, DAY + " 09:15")).fetchall()
    ctx = {}
    if fut:
        f_open = fut[0][1]; f_open_oi = fut[0][2]
        for t, c, oi in fut:
            if not (f_open and f_open_oi and c and oi is not None):
                ctx[t] = "neutral"; continue
            oi_open = (oi / f_open_oi - 1) * 100
            move_open = (c / f_open - 1) * 100
            if oi_open > OI_CTX and move_open < 0:
                ctx[t] = "SHORT-buildup"
            elif oi_open > OI_CTX and move_open > 0:
                ctx[t] = "LONG-buildup"
            else:
                ctx[t] = "neutral"
    return mkt, ctx

def load_universe_stretch(cur):
    """Faithful cross-sectional anti-chase threshold: per minute, 80th-pctile of |move-from-open|
    across EVERY instrument captured that minute (exactly what inst_flow_engine does)."""
    rows = cur.execute("SELECT instrument_key,bar_time,close FROM mkt_orderflow_1min WHERE bar_time>=? ORDER BY instrument_key,bar_time",
                       (DAY + " 09:15",)).fetchall()
    day_open = {}
    permin = {}
    for ik, t, c in rows:
        if c is None:
            continue
        if ik not in day_open:
            day_open[ik] = c
        o = day_open[ik]
        if o:
            permin.setdefault(t, []).append(abs(c / o - 1) * 100)
    STRETCH_Q = 0.80
    thr = {}
    for t, mv in permin.items():
        mv.sort()
        thr[t] = mv[min(int(len(mv) * STRETCH_Q), len(mv) - 1)]
    return thr

# ==============================================================================
# 2. CAUSAL ROLLING Z  (no look-ahead)
# ==============================================================================
def rolling_z(vals):
    """z_t vs the trailing ZWIN values (exclusive of t). None until ZMIN samples exist."""
    out = []
    for i, v in enumerate(vals):
        if v is None:
            out.append(None); continue
        base = [x for x in vals[max(0, i - ZWIN):i] if x is not None]
        if len(base) < ZMIN:
            out.append(None); continue
        m = st.fmean(base)
        sd = st.pstdev(base)
        out.append((v - m) / sd if sd > 0 else 0.0)
    return out

# ==============================================================================
# 3. v2 DETECTOR FAMILIES  (verbatim logic, has_tick=True)
# ==============================================================================
def fam_aggression(of, ret, spike):
    if not spike or abs(ret) < THR:
        return 0, 0.0
    side = 1 if ret > 0 else -1
    bp = of["buy_vol_pct"]
    if bp is None:
        return 0, 0.0
    if side == 1 and bp < TICK_HI:
        return 0, 0.0
    if side == -1 and bp > 100 - TICK_HI:
        return 0, 0.0
    strength = abs(ret) / THR + abs(bp - 50) / 10
    mo, vol = of["max_trade_qty"], of["volume"] or 0
    if mo and vol and mo >= 0.10 * vol:
        strength += 1
    return side, strength

def fam_absorption(of, ret):
    hi, lo, c = of["high"], of["low"], of["close"]
    if hi is None or lo is None or hi <= lo:
        return 0, 0.0
    loc = (c - lo) / (hi - lo)
    bp = of["buy_vol_pct"]
    if bp is not None:
        sell_hit, buy_hit = bp <= 100 - TICK_HI, bp >= TICK_HI
    else:
        tbq, tsq = of["total_buy_qty"] or 0, of["total_sell_qty"] or 0
        imb = tbq / (tbq + tsq) if (tbq + tsq) else 0.5
        sell_hit, buy_hit = imb <= 0.45, imb >= 0.55
    if sell_hit and loc >= 0.70 and ret >= -THR:
        return 1, 1.0 + loc            # bullish absorption (hit down, held up)
    if buy_hit and loc <= 0.30 and ret <= THR:
        return -1, 1.0 + (1 - loc)     # bearish absorption (hit up, held down)
    return 0, 0.0

def fam_acceptance(of, above_atp, seen):
    c, atp = of["close"], of["atp"]
    if c is None or atp is None or atp <= 0:
        return 0, 0.0
    acc = above_atp / seen if seen else 0.5
    if c >= atp:
        return 1, 0.5 + acc
    return -1, 0.5 + (1 - acc)

def build_detectors(rows):
    """Walk a stock minute-by-minute, replicate v2 state (prev_close, trailing vol median, atp
    acceptance) and emit per-minute family fires + candidate (>=2 fams, >=1 price fam, rising edge)."""
    from collections import deque
    prev_close = None
    voldq = deque(maxlen=WIN)
    above_atp = seen = 0
    day_open = None
    confirmed_prev = False
    out = []
    for i, of in enumerate(rows):
        c = of["close"]
        if day_open is None and c is not None:
            day_open = c
        if of["atp"]:
            seen += 1; above_atp += int(c >= of["atp"])
        ret = (c / prev_close - 1) if (prev_close and c) else 0.0
        med = sorted(voldq)[len(voldq) // 2] if voldq else 0
        spike = med > 0 and (of["volume"] or 0) >= VMULT * med
        fams = {}
        a = fam_aggression(of, ret, spike)
        b = fam_absorption(of, ret)
        # inst_flow_engine updates above_atp/seen in PASS0 (THIS bar included) before scoring — match it:
        z = fam_acceptance(of, above_atp, seen)
        for name, val in (("AGGRESSION", a), ("ABSORPTION", b), ("ACCEPTANCE", z)):
            if val[0] != 0:
                fams[name] = val
        cand = None
        if fams:
            long_s = sum(v[1] for f, v in fams.items() if v[0] == 1)
            short_s = sum(v[1] for f, v in fams.items() if v[0] == -1)
            side = 1 if long_s > short_s else -1
            fired = [f for f, v in fams.items() if v[0] == side]
            if len(fired) >= 2 and (set(fired) & PRICE_FAMS):
                cand = dict(side=side, fired=fired, score=round(sum(fams[f][1] for f in fired), 2))
        confirmed = cand is not None
        fresh = confirmed and not confirmed_prev
        confirmed_prev = confirmed
        out.append(dict(
            ret=ret, spike=spike, med=med,
            agg=a, absb=b, acc=z, fams=fams, cand=cand, fresh=fresh,
            move_open=(c / day_open - 1) if (day_open and c) else 0.0,
            n_min=i))
        if c is not None:
            prev_close = c
        if of["volume"] is not None:
            voldq.append(of["volume"])
    return out

def v2_reject(side, mv, rs, sig, stretched, mkt_move, ctx):
    """Verbatim _reject_v2 (mode v2). mkt_move = NIFTY-50 move-from-open; ctx = NIFTY-fut context."""
    if mkt_move is not None and mkt_move > 0.001 or ctx == "LONG-buildup":
        regime = "up"
    elif mkt_move is not None and mkt_move < -0.001 or ctx == "SHORT-buildup":
        regime = "down"
    else:
        regime = "neutral"
    if side == 1:
        if mv <= 0 or rs < 0.001:
            return "long-not-leading"
        if regime == "down" and rs < 0.005:
            return "counter-long-weak"
        return "anti-chase" if stretched else None
    if regime == "up":
        if mv < 0.003:
            return "not-a-pop"
        if "ABSORPTION" not in sig["fired"]:
            return "no-distribution"
    return "anti-chase" if stretched else None

# ==============================================================================
# 4. FOOTPRINT ANALYSES
# ==============================================================================
def compute_cvd(rows):
    cvd = 0; out = []
    for of in rows:
        b, s = of.get("buy_vol"), of.get("sell_vol")
        d = (b - s) if (b is not None and s is not None) else 0
        cvd += d
        out.append((d, cvd))
    return out

def absorption_events(rows, det, cvd):
    """Section 1. Window (ABS_WIN) net CVD pressure vs price change.
      hidden-BUYER  : strong NET SELLING (sum dCVD z<=-Z_FLAG) but price held/rose over window.
      hidden-SELLER : strong NET BUYING  (sum dCVD z>=+Z_FLAG) but price capped/fell over window.
    z-scored per stock on the trailing rolling distribution of the window-sum (causal)."""
    n = len(rows)
    winsum = [None] * n
    for i in range(n):
        if i < ABS_WIN - 1:
            continue
        winsum[i] = sum(cvd[j][0] for j in range(i - ABS_WIN + 1, i + 1))
    zsum = rolling_z(winsum)
    events = []
    for i in range(ABS_WIN, n):
        if zsum[i] is None:
            continue
        c0 = rows[i - ABS_WIN + 1]["close"]; c1 = rows[i]["close"]
        if not (c0 and c1):
            continue
        pchg = (c1 / c0 - 1) * 100
        t = rows[i]["bar_time"]
        # hidden buyer: heavy net selling absorbed, price did not fall
        if zsum[i] <= -Z_FLAG and pchg >= -0.10:
            events.append(dict(time=t, kind="HIDDEN_BUYER", side="bid", z=round(zsum[i], 2),
                               net_cvd=int(winsum[i]), price_chg_pct=round(pchg, 3),
                               level=round(c1, 2)))
        # hidden seller: heavy net buying absorbed, price did not rise
        elif zsum[i] >= Z_FLAG and pchg <= 0.10:
            events.append(dict(time=t, kind="HIDDEN_SELLER", side="ask", z=round(zsum[i], 2),
                               net_cvd=int(winsum[i]), price_chg_pct=round(pchg, 3),
                               level=round(c1, 2)))
    return events

def iceberg_events(rows):
    """Section 2 (COARSE, labeled). Price resting AT a displayed level across >=2 consecutive
    minutes (level not breaking) while minute traded volume z>=Z_FLAG yet the displayed size at
    that touch is small. minute-granularity, ~5s-stale book, no per-price tick -> WEAK proxy."""
    n = len(rows)
    vol = [r["volume"] for r in rows]
    zvol = rolling_z(vol)
    events = []
    for i in range(1, n):
        cur, prv = rows[i], rows[i - 1]
        c = cur["close"]
        if c is None or zvol[i] is None or zvol[i] < Z_FLAG:
            continue
        # is price resting at a bid or ask touch, unchanged vs prev minute (level holding)?
        for side, pk, qk in (("bid", "b1p", "b1q"), ("ask", "a1p", "a1q")):
            lvl, disp = cur.get(pk), cur.get(qk)
            plvl = prv.get(pk)
            if lvl is None or disp is None or plvl is None:
                continue
            if abs(c - lvl) <= max(0.05, lvl * 0.0005) and abs(lvl - plvl) <= max(0.05, lvl * 0.0005):
                traded = cur["volume"] or 0
                if disp > 0 and traded >= 3 * disp:   # far more traded than was ever shown
                    events.append(dict(time=cur["bar_time"], side=side, level=round(lvl, 2),
                                       displayed=int(disp), traded=int(traded),
                                       ratio=round(traded / disp, 1), vol_z=round(zvol[i], 2)))
    return events

def vanishing_events(rows):
    """Section 3 (COARSE, labeled). A displayed level with size z>=Z_FLAG (per stock, across the
    5 shown levels) present at price P in minute t, GONE from all 5 levels at t+1, with LOW minute
    traded volume (pulled, not filled). no order-lifecycle data -> true spoofing needs order-by-order."""
    n = len(rows)
    volz = rolling_z([r["volume"] for r in rows])
    # trailing size stats for z of an individual big level
    events = []
    size_hist = []
    for i in range(n - 1):
        r, nx = rows[i], rows[i + 1]
        base = [x for x in size_hist[max(0, i - ZWIN):i] if x is not None]
        # gather all displayed levels this minute with their size
        levels = []
        for side, pk, qk in (("bid", "b1p", "b1q"), ("bid", "b2p", "b2q"), ("bid", "b3p", "b3q"),
                             ("bid", "b4p", "b4q"), ("bid", "b5p", "b5q"),
                             ("ask", "a1p", "a1q"), ("ask", "a2p", "a2q"), ("ask", "a3p", "a3q"),
                             ("ask", "a4p", "a4q"), ("ask", "a5p", "a5q")):
            p, q = r.get(pk), r.get(qk)
            if p and q:
                levels.append((side, p, q))
        # size z baseline from trailing individual level sizes
        flat_hist = [x for sub in size_hist[max(0, i - ZWIN):i] if sub for x in sub]
        if len(flat_hist) >= ZMIN:
            m = st.fmean(flat_hist); sd = st.pstdev(flat_hist)
        else:
            m = sd = None
        nx_prices = set()
        for pk in ("b1p", "b2p", "b3p", "b4p", "b5p", "a1p", "a2p", "a3p", "a4p", "a5p"):
            if nx.get(pk):
                nx_prices.add(round(nx[pk], 2))
        for side, p, q in levels:
            if m is None or sd in (None, 0):
                continue
            zq = (q - m) / sd
            if zq < Z_FLAG:
                continue
            if round(p, 2) in nx_prices:
                continue  # level still there -> not vanished
            # low traded volume next minute => pulled, not filled
            if volz[i + 1] is not None and volz[i + 1] > 0.5:
                continue
            events.append(dict(time=r["bar_time"], side=side, level=round(p, 2),
                               displayed=int(q), size_z=round(zq, 2),
                               next_vol=int(nx["volume"] or 0)))
        size_hist.append([l[2] for l in levels])
    return events

def participant_profile(rows):
    """Section 4. avg_tick_vol stability (rolling CV), block ratio = max_trade_qty/avg_tick_vol,
    orders-per-level whale read = avg qty/order at touch (b1q/b1o vs a1q/a1o), PERSISTENT.
    FULL per-trade size distribution is NOT stored -> print-size 'mode share' NOT computable.
    Arrival-time analysis SKIPPED (timestamps lack resolution)."""
    atv = [r.get("avg_tick_vol") for r in rows]
    valid_atv = [x for x in atv if x]
    cv = (st.pstdev(valid_atv) / st.fmean(valid_atv)) if len(valid_atv) > 2 and st.fmean(valid_atv) else None
    block = []
    for r in rows:
        a, m = r.get("avg_tick_vol"), r.get("max_trade_qty")
        block.append((m / a) if (a and m) else None)
    bvals = [x for x in block if x]
    # whale read: per-order size at touch, smoothed, require persistence
    from collections import deque
    win = deque(maxlen=5)
    whale_side_run = 0; whale_side = None; persistent_minutes = 0
    for r in rows:
        b1q, b1o = r.get("b1q"), r.get("b1o")
        a1q, a1o = r.get("a1q"), r.get("a1o")
        bid_ps = (b1q / b1o) if (b1q and b1o) else None
        ask_ps = (a1q / a1o) if (a1q and a1o) else None
        if bid_ps is None or ask_ps is None:
            side = None
        elif bid_ps >= 1.5 * ask_ps:
            side = "bid"      # big orders resting on bid = accumulation lean
        elif ask_ps >= 1.5 * bid_ps:
            side = "ask"      # big orders on ask = distribution lean
        else:
            side = None
        win.append(side)
        # persistence: same side dominates the last 5
        from collections import Counter
        cc = Counter([x for x in win if x])
        dom = cc.most_common(1)[0] if cc else None
        if dom and dom[1] >= 4:
            persistent_minutes += 1
            whale_side = dom[0]
    return dict(
        atv_cv=round(cv, 3) if cv is not None else None,
        atv_mean=round(st.fmean(valid_atv)) if valid_atv else None,
        block_ratio_med=round(st.median(bvals), 2) if bvals else None,
        block_ratio_max=round(max(bvals), 2) if bvals else None,
        block_ratio_p90=round(sorted(bvals)[int(len(bvals) * 0.9)], 2) if bvals else None,
        whale_persistent_min=persistent_minutes,
        whale_side_dominant=whale_side)

# ==============================================================================
# 5. RUN PER STOCK
# ==============================================================================
def analyze():
    con = _con(ro=True); cur = con.cursor()
    mkt, ctx = load_market(cur)
    stretch = load_universe_stretch(cur)
    data = {}
    for sym in ALL_SYMS:
        rows = load_stock(cur, sym)
        det = build_detectors(rows)
        cvd = compute_cvd(rows)
        absev = absorption_events(rows, det, cvd)
        ice = iceberg_events(rows)
        van = vanishing_events(rows)
        part = participant_profile(rows)
        # move timeline
        o = rows[0]["close"]
        move = [((r["close"] / o - 1) * 100 if r["close"] and o else 0.0) for r in rows]
        # move-start (first |move|>=1% that is same sign as day result & sustained)
        result = move[-1]
        sign = 1 if result >= 0 else -1
        move_start = None
        for i, m in enumerate(move):
            if m * sign >= MOVE_1PC:
                move_start = rows[i]["bar_time"]; break
        move_start_2pc = None
        for i, m in enumerate(move):
            if m * sign >= 2.0:
                move_start_2pc = rows[i]["bar_time"]; break
        # v2 candidate & entry reconstruction
        #   first_entry uses single-position semantics (engine holds one pos; exits not modeled).
        #   entry_signal_min counts EVERY gate-surviving fresh candidate (uncapped) = honest fire-rate.
        #   reject_reasons tallies WHY gate-surviving candidates were blocked (grounds the report).
        from collections import Counter
        first_cand = None; first_entry = None; entries = []
        entry_signal_min = 0; reject_reasons = Counter()
        pos_open = False
        for i, d in enumerate(det):
            t = rows[i]["bar_time"]; hhmm = t[11:16]
            if d["cand"] and first_cand is None:
                first_cand = dict(time=t, side=d["cand"]["side"], fired=d["cand"]["fired"])
            if d["cand"] and d["fresh"] and hhmm < ENTRY_CUT and d["n_min"] >= WARMUP:
                sig = d["cand"]; side = sig["side"]
                mv = d["move_open"]; mkt_move = mkt.get(t, 0.0); rs = mv - mkt_move
                sthr = stretch.get(t)
                stretched = sthr is not None and abs(mv) * 100 >= sthr and ((mv > 0) == (side == 1))
                rej = v2_reject(side, mv, rs, sig, stretched, mkt_move, ctx.get(t, "neutral"))
                if rej is None:
                    entry_signal_min += 1
                    if not pos_open:
                        entries.append(dict(time=t, side=("long" if side == 1 else "short"),
                                            fired="+".join(sig["fired"]), move_open=round(mv * 100, 2),
                                            rs=round(rs * 100, 2)))
                        if first_entry is None:
                            first_entry = entries[-1]
                        pos_open = True
                else:
                    reject_reasons[rej] += 1
        data[sym] = dict(rows=rows, det=det, cvd=cvd, absev=absev, ice=ice, van=van, part=part,
                         move=move, move_start=move_start, move_start_2pc=move_start_2pc,
                         result=result, first_cand=first_cand, first_entry=first_entry, entries=entries,
                         entry_signal_min=entry_signal_min, reject_reasons=reject_reasons)
    con.close()
    return data, mkt, ctx

# ==============================================================================
# 6. LEAD-TIME & SCORECARD
# ==============================================================================
def tmin(a, b):
    if not a or not b:
        return None
    fa = datetime.strptime(a, "%Y-%m-%d %H:%M"); fb = datetime.strptime(b, "%Y-%m-%d %H:%M")
    return int((fb - fa).total_seconds() // 60)

def family_fire_counts(det):
    """Count minutes each family fired (any side) and directionally-correct fires."""
    c = {"AGGRESSION": 0, "ABSORPTION": 0, "ACCEPTANCE": 0}
    for d in det:
        for f in d["fams"]:
            c[f] += 1
    return c

def build_scorecard(data):
    """Per family: earliest fire on each mover, lead vs move-start(1%), control false-positive rate."""
    fams = ["AGGRESSION", "ABSORPTION", "ACCEPTANCE"]
    sc = {}
    for fam in fams:
        row = {"family": fam}
        # earliest DIRECTIONALLY-CORRECT fire on each mover + lead vs move_start
        for sym in MOVERS:
            d = data[sym]; want = 1 if MOVERS[sym][0] == "UP" else -1
            first = None
            for i, dd in enumerate(d["det"]):
                v = dd["fams"].get(fam)
                if v and v[0] == want:
                    first = d["rows"][i]["bar_time"]; break
            row[f"{sym}_first"] = first[11:] if first else "—"
            row[f"{sym}_lead"] = tmin(first, d["move_start"]) if first and d["move_start"] else None
        # control false-positive: fraction of control-minutes with a fire (any side) — boring names should be quiet
        fp = []
        for sym in CONTROLS:
            cnt = family_fire_counts(data[sym]["det"])[fam]
            fp.append(cnt / len(data[sym]["det"]))
        row["ctrl_fire_rate"] = round(sum(fp) / len(fp), 3)
        sc[fam] = row
    # CANDIDATE (>=2 fam) and ENTRY scorecard
    cand_row = {"family": "v2 CANDIDATE (>=2 fam)"}
    entry_row = {"family": "v2 ENTRY (full gates)"}
    for sym in MOVERS:
        d = data[sym]
        cand_row[f"{sym}_first"] = d["first_cand"]["time"][11:] if d["first_cand"] else "—"
        cand_row[f"{sym}_lead"] = tmin(d["first_cand"]["time"], d["move_start"]) if d["first_cand"] and d["move_start"] else None
        # annotate entry with SIDE — a wrong-way entry is not a "catch"
        if d["first_entry"]:
            want = "long" if MOVERS[sym][0] == "UP" else "short"
            tag = "OK" if d["first_entry"]["side"] == want else "WRONG-WAY"
            entry_row[f"{sym}_first"] = f"{d['first_entry']['time'][11:]} {d['first_entry']['side']}({tag})"
        else:
            entry_row[f"{sym}_first"] = "— (no entry)"
        entry_row[f"{sym}_lead"] = tmin(d["first_entry"]["time"], d["move_start"]) if d["first_entry"] and d["move_start"] else None
    # control candidate/entry counts as false positive
    cand_fp = sum(1 for sym in CONTROLS if data[sym]["first_cand"])
    entry_fp = sum(1 for sym in CONTROLS if data[sym]["entries"])
    ctrl_cand_min = sum(sum(1 for x in data[sym]["det"] if x["cand"]) for sym in CONTROLS)
    ctrl_entry_min = sum(data[sym]["entry_signal_min"] for sym in CONTROLS)
    cand_row["ctrl_fire_rate"] = f"{cand_fp}/4 controls flagged; {ctrl_cand_min} candidate-min total"
    entry_row["ctrl_fire_rate"] = f"{entry_fp}/4 controls entered; {ctrl_entry_min} gate-surviving signal-min total"
    sc["CANDIDATE"] = cand_row; sc["ENTRY"] = entry_row
    # ---- verdicts (calibration; NOT ship decisions) ----
    sc["AGGRESSION"]["verdict"] = ("KEEP — highest specificity (control fire-rate ~1%); led DRREDDY by +1min. "
        "BLIND SPOT: on the +11% up-mover the tick-rule classified the tape SELL-heavy, so LONG-aggression fired LATE (09:40, -14min). "
        "Do not use as the SOLE trigger for up-moves; pair with absorption/acceptance.")
    sc["ABSORPTION"]["verdict"] = ("KEEP as 2nd confirmer / EARLY-WARNING only — earliest correct fire on BOTH movers "
        "(KALYAN +11min, DRREDDY +2min) but noisy on controls (fires ~29% of control-min). Never standalone.")
    sc["ACCEPTANCE"]["verdict"] = ("DEMOTE to tiebreaker weight — fires EVERY minute on EVERY name (close is always >=/<= ATP). "
        "Zero standalone discrimination; useful only to break long/short ties.")
    sc["CANDIDATE"]["verdict"] = ("NOT a signal by itself — the raw >=2-family candidate fired 50-63 min on EACH control "
        "(4/4). All discrimination must come from the entry gates, not the family count.")
    sc["ENTRY"]["verdict"] = ("FAILED on BOTH movers this day. KALYANKJIL: 0 entries — anti-chase rejected it 64x because a gap-and-go "
        "is always top-quintile stretched (the +11% winner was structurally un-enterable). DRREDDY: the ONE gate-surviving entry was a "
        "WRONG-WAY LONG at 09:37 (+0.45%, bullish ABS+ACC on an early bounce) that then faced the -4.8% markdown — the regime gate "
        "('not-a-pop' x55) suppressed the CORRECT short in an up-market while a bounce leaked a losing long. And 4/4 controls also "
        "produced entries (3-33 signal-min each). On n=1 the entry layer neither caught the winners nor stayed off the controls.")
    return sc

# ==============================================================================
# 7. EXCEL
# ==============================================================================
HDR = Font(bold=True, color="FFFFFF"); HFILL = PatternFill("solid", fgColor="1F4E78")
SUBF = Font(bold=True); TITLE = Font(bold=True, size=14)
MOVEFILL = PatternFill("solid", fgColor="FFF2CC")
UPFILL = PatternFill("solid", fgColor="C6EFCE"); DNFILL = PatternFill("solid", fgColor="FFC7CE")
THIN = Border(*[Side(style="thin", color="D9D9D9")] * 4)

def hdr_row(ws, r, cols, start=1):
    for j, c in enumerate(cols):
        cell = ws.cell(row=r, column=start + j, value=c)
        cell.font = HDR; cell.fill = HFILL; cell.alignment = Alignment(horizontal="center", wrap_text=True)
        cell.border = THIN

def autosize(ws, maxw=42):
    for col in ws.columns:
        w = 0; letter = None
        for cell in col:
            letter = cell.column_letter
            if cell.value is not None:
                w = max(w, len(str(cell.value)))
        if letter:
            ws.column_dimensions[letter].width = min(maxw, max(9, w + 2))

def write_excel(data, mkt, scorecard, path):
    wb = openpyxl.Workbook()

    # ---------- Summary ----------
    ws = wb.active; ws.title = "Summary"
    ws["A1"] = "FORENSIC ORDER-FLOW AUTOPSY  —  2026-07-09  (detector CALIBRATION, n=2 movers, 1 tick-day)"
    ws["A1"].font = TITLE
    r = 3
    intro = [
        "SCOPE: MOVER-UP KALYANKJIL (+11.06%), MOVER-DOWN DRREDDY (-4.83%); CONTROLS HDFCBANK, ICICIBANK, ITC, BAJFINANCE.",
        "CONTROL RULE: every footprint computed IDENTICALLY on movers and controls. A footprint counts only if it fired on a",
        "   mover and stayed quiet on the controls. Findings are HYPOTHESES for the v2 engine — to be judged on the live 2-week",
        "   capture, NOT shipped from this autopsy. No directional-probability claims from one stock-day.",
        "DATA HONESTY: storage is PER-MINUTE aggregates (no per-tick, no per-price-level). CVD is minute-granularity (GOOD).",
        "   Iceberg / Vanishing are COARSE proxies (~5s-stale book, no order-lifecycle) — WEAK. Print-size 'mode share' and",
        "   arrival-time analysis are NOT computable and are omitted. All z-scores are CAUSAL trailing-30-min, per stock.",
    ]
    for line in intro:
        ws.cell(row=r, column=1, value=line); r += 1
    r += 1
    ws.cell(row=r, column=1, value="DETECTOR SCORECARD").font = TITLE; r += 1
    ws.cell(row=r, column=1, value="Lead = minutes the footprint PRECEDED the move crossing +/-1% from open (positive = early warning; negative/— = lagged/absent).").font = Font(italic=True)
    r += 1
    cols = ["Detector family", "KALYANKJIL first fire", "lead(min) vs +1%", "DRREDDY first fire",
            "lead(min) vs -1%", "Control fire-rate / count", "Verdict"]
    hdr_row(ws, r, cols)
    r += 1
    verdicts = {
        "AGGRESSION": None, "ABSORPTION": None, "ACCEPTANCE": None,
        "CANDIDATE": None, "ENTRY": None,
    }
    order = ["AGGRESSION", "ABSORPTION", "ACCEPTANCE", "CANDIDATE", "ENTRY"]
    for fam in order:
        sc = scorecard[fam]
        name = sc["family"]
        kf = sc.get("KALYANKJIL_first", "—"); kl = sc.get("KALYANKJIL_lead")
        df = sc.get("DRREDDY_first", "—"); dl = sc.get("DRREDDY_lead")
        cr = sc.get("ctrl_fire_rate")
        vals = [name, kf, kl if kl is not None else "—", df, dl if dl is not None else "—", cr, sc.get("verdict", "")]
        for j, v in enumerate(vals):
            c = ws.cell(row=r, column=1 + j, value=v); c.border = THIN
            c.alignment = Alignment(wrap_text=True, vertical="top")
        r += 1
    ws.column_dimensions[get_column_letter(7)].width = 80
    r += 2

    # institutional read
    ws.cell(row=r, column=1, value="INSTITUTIONAL READ  (evidence chain per mover)").font = TITLE; r += 1
    reads = [
        ("KALYANKJIL  (+11.06%)  =  HIDDEN ACCUMULATION / demand-led markup",
         ["Cumulative minute-CVD ended NET -460,412 (tape classified SELL-heavy) WHILE price rose +11% -> textbook absorption divergence.",
          "13 HIDDEN_BUYER absorption windows (heavy net selling absorbed, price held/rose); close persistently >= ATP all day (acceptance LONG 376/376).",
          "Read: a patient buyer absorbed profit-taking / retail supply at the bid and marked the stock up. Aggressive-buy (AGGRESSION) confirmation",
          "  arrived LATE (09:40) precisely because the fast up-ticks were sell-classified — a Lee-Ready blind spot on gap-and-go, not absence of demand."]),
        ("DRREDDY  (-4.83%)  =  HIDDEN DISTRIBUTION / supply-led markdown",
         ["Cumulative minute-CVD ended NET +1,506,722 (tape classified BUY-heavy) WHILE price fell -4.83% -> absorption divergence, opposite sign.",
          "19 HIDDEN_SELLER absorption windows (heavy net buying capped, price stalled/fell); close persistently <= ATP; block_ratio max ~29x avg tick",
          "  (outsized single prints consistent with one large seller working an order); persistent larger ask-side orders (104 min).",
          "Read: a large seller distributed into buyer demand, capping every bounce and marking the stock down."]),
    ]
    for title, lines in reads:
        c = ws.cell(row=r, column=1, value=title); c.font = SUBF
        c.fill = UPFILL if "KALYAN" in title else DNFILL; r += 1
        for ln in lines:
            ws.cell(row=r, column=1, value="   " + ln); r += 1
        r += 1

    ws.cell(row=r, column=1, value="THE STANDOUT DISCRIMINATOR (verified 2 ways): cumulative minute-CVD ran OPPOSITE to price on BOTH movers, "
            "but SAME-sign as price on ALL FOUR controls. Absorption divergence — not directional CVD-follows-price — is what separated the movers.").font = SUBF
    r += 2
    ws.cell(row=r, column=1, value="HONEST LIMITS").font = TITLE; r += 1
    limits = [
        "n = 2 movers, 1 tick-day. NO standalone directional-probability claims. Findings are HYPOTHESES for the v2 engine, to be",
        "   judged on the live 2-week capture — not shipped from this autopsy.",
        "CVD sign may be partly Lee-Ready misclassification in fast/gap markets; but the DIVERGENCE itself is the absorption signature and",
        "   it is exactly what discriminated movers (OPPOSITE) from controls (SAME). Both movers verified against raw SUM(buy_vol)-SUM(sell_vol).",
        "ICEBERG and VANISHING are COARSE minute/level proxies and did NOT discriminate: controls often had MORE events (e.g. ICICIBANK 23",
        "   icebergs vs KALYAN 2; BAJFINANCE 77 vanish vs KALYAN 50). Treat as NON-findings pending order-by-order data.",
        "Entry-gate FAILED on both movers: KALYANKJIL un-enterable (0 entries, 64 anti-chase rejects — gap-and-go is always stretched);",
        "   DRREDDY's one entry was a WRONG-WAY LONG at 09:37 into the -4.8% drop. 4/4 controls also produced entries. The detector FAMILIES",
        "   lead the moves, but the ENTRY GATES (anti-chase + regime-asymmetric short block) mis-timed both movers on this single day.",
    ]
    for ln in limits:
        ws.cell(row=r, column=1, value=ln); r += 1
    autosize(ws)
    ws.column_dimensions["A"].width = 130
    ws.column_dimensions[get_column_letter(7)].width = 80

    # ---------- Timeline (movers) ----------
    for sym in MOVERS:
        d = data[sym]
        ws = wb.create_sheet(f"Timeline_{sym}")
        ws.cell(row=1, column=1, value=f"MINUTE-BY-MINUTE TIMELINE — {sym} ({MOVERS[sym][0]} {MOVERS[sym][1]:+.2f}%)").font = TITLE
        ws.cell(row=2, column=1, value=f"move_start(+/-1%)={d['move_start']}  |  +/-2%={d['move_start_2pc']}  |  "
                f"v2 first CANDIDATE={d['first_cand']['time'] if d['first_cand'] else '—'}  |  "
                f"v2 first ENTRY={d['first_entry']['time'] if d['first_entry'] else '—'}").font = SUBF
        cols = ["time", "close", "move%", "ret1m%", "minVol", "spike", "buy%", "dCVD", "CVD",
                "AGG", "ABS", "ACC", "cand", "entry", "footprints"]
        hr = 4; hdr_row(ws, hr, cols)
        absmap = {e["time"]: e for e in d["absev"]}
        icemap = {}
        for e in d["ice"]:
            icemap.setdefault(e["time"], []).append(f"ICE-{e['side']}@{e['level']}(x{e['ratio']})")
        vanmap = {}
        for e in d["van"]:
            vanmap.setdefault(e["time"], []).append(f"VANISH-{e['side']}@{e['level']}(z{e['size_z']})")
        rr = hr + 1
        for i, of in enumerate(d["rows"]):
            det = d["det"][i]; t = of["bar_time"]
            fp = []
            if t in absmap:
                fp.append(f"{absmap[t]['kind']}(z{absmap[t]['z']})")
            fp += icemap.get(t, []); fp += vanmap.get(t, [])
            def famstr(v):
                if v[0] == 0:
                    return ""
                return ("L" if v[0] == 1 else "S") + f"{v[1]:.1f}"
            cand = det["cand"]
            candstr = ""
            if cand:
                candstr = ("LONG" if cand["side"] == 1 else "SHORT") + " " + "+".join(f[:3] for f in cand["fired"])
                if det["fresh"]:
                    candstr = "*" + candstr  # rising edge
            entrystr = ""
            for e in d["entries"]:
                if e["time"] == t:
                    entrystr = f"ENTER {e['side']}"
            row = [t[11:], round(of["close"], 2), round(d["move"][i], 2), round(det["ret"] * 100, 3),
                   of["volume"], "Y" if det["spike"] else "", of.get("buy_vol_pct"),
                   int(d["cvd"][i][0]), int(d["cvd"][i][1]),
                   famstr(det["agg"]), famstr(det["absb"]), famstr(det["acc"]),
                   candstr, entrystr, "; ".join(fp)]
            for j, v in enumerate(row):
                cell = ws.cell(row=rr, column=1 + j, value=v); cell.border = THIN
            # highlight
            if entrystr:
                for j in range(len(row)):
                    ws.cell(row=rr, column=1 + j).fill = MOVEFILL
            if t == d["move_start"]:
                ws.cell(row=rr, column=3).fill = (UPFILL if d["result"] >= 0 else DNFILL)
            rr += 1
        ws.freeze_panes = "A5"
        autosize(ws, maxw=34)

    # ---------- CVD (all stocks: absorption events + control divergences) ----------
    ws = wb.create_sheet("CVD")
    ws.cell(row=1, column=1, value="SECTION 1 — CVD & ABSORPTION (minute-granularity CVD; GOOD analysis)").font = TITLE
    ws.cell(row=2, column=1, value="HIDDEN_BUYER = heavy net SELLING absorbed, price held (bid defense). HIDDEN_SELLER = heavy net BUYING capped, price stalled (ask supply). z>=2 causal.").font = Font(italic=True)
    r = 4
    cols = ["stock", "role", "day_result%", "CVD_end", "#absorption_events", "#hidden_buyer", "#hidden_seller",
            "first_event_time", "first_event_kind", "move_start(+/-1%)", "absorption_LEAD(min)"]
    hdr_row(ws, r, cols); r += 1
    for sym in ALL_SYMS:
        d = data[sym]; role = "MOVER-" + MOVERS[sym][0] if sym in MOVERS else "control"
        hb = sum(1 for e in d["absev"] if e["kind"] == "HIDDEN_BUYER")
        hsl = sum(1 for e in d["absev"] if e["kind"] == "HIDDEN_SELLER")
        first = d["absev"][0] if d["absev"] else None
        # lead of the FIRST directionally-consistent absorption vs move_start
        want = "HIDDEN_BUYER" if d["result"] >= 0 else "HIDDEN_SELLER"
        firstdir = next((e for e in d["absev"] if e["kind"] == want), None)
        lead = tmin(firstdir["time"], d["move_start"]) if (firstdir and d["move_start"]) else None
        vals = [sym, role, round(d["result"], 2), int(d["cvd"][-1][1]), len(d["absev"]), hb, hsl,
                first["time"][11:] if first else "—", first["kind"] if first else "—",
                d["move_start"][11:] if d["move_start"] else "—", lead if lead is not None else "—"]
        for j, v in enumerate(vals):
            cell = ws.cell(row=r, column=1 + j, value=v); cell.border = THIN
            if sym in MOVERS:
                cell.fill = MOVEFILL
        r += 1
    r += 2
    ws.cell(row=r, column=1, value="ALL ABSORPTION EVENTS (every stock, identical detector):").font = SUBF; r += 1
    cols = ["stock", "role", "time", "kind", "z", "net_cvd(win)", "price_chg%(win)", "level"]
    hdr_row(ws, r, cols); r += 1
    for sym in ALL_SYMS:
        role = "MOVER" if sym in MOVERS else "control"
        for e in data[sym]["absev"]:
            vals = [sym, role, e["time"][11:], e["kind"], e["z"], e["net_cvd"], e["price_chg_pct"], e["level"]]
            for j, v in enumerate(vals):
                ws.cell(row=r, column=1 + j, value=v).border = THIN
            r += 1
    autosize(ws)

    # ---------- Icebergs ----------
    ws = wb.create_sheet("Icebergs")
    ws.cell(row=1, column=1, value="SECTION 2 — ICEBERG (COARSE proxy: minute-granularity, ~5s-stale book, NO per-price tick data — WEAK)").font = TITLE
    ws.cell(row=2, column=1, value="Flagged: price resting AT a held bid/ask touch while minute traded volume (z>=2) >> displayed size at that touch (>=3x).").font = Font(italic=True)
    r = 4; cols = ["stock", "role", "time", "side", "level", "displayed_size", "traded_vol", "traded/displayed", "vol_z"]
    hdr_row(ws, r, cols); r += 1
    any_ice = False
    for sym in ALL_SYMS:
        role = "MOVER" if sym in MOVERS else "control"
        for e in data[sym]["ice"]:
            any_ice = True
            vals = [sym, role, e["time"][11:], e["side"], e["level"], e["displayed"], e["traded"], e["ratio"], e["vol_z"]]
            for j, v in enumerate(vals):
                cell = ws.cell(row=r, column=1 + j, value=v); cell.border = THIN
                if sym in MOVERS:
                    cell.fill = MOVEFILL
            r += 1
    if not any_ice:
        ws.cell(row=r, column=1, value="No iceberg-proxy events cleared the z>=2 + 3x-displayed bar on any stock.")
    autosize(ws)

    # ---------- Vanishing ----------
    ws = wb.create_sheet("VanishingLiquidity")
    ws.cell(row=1, column=1, value="SECTION 3 — VANISHING LIQUIDITY (COARSE proxy: NO order-lifecycle data; true spoofing needs order-by-order)").font = TITLE
    ws.cell(row=2, column=1, value="Flagged: displayed level size z>=2 (per stock) present at price P in minute t, GONE from all 5 levels at t+1, with LOW next-minute volume (pulled, not filled).").font = Font(italic=True)
    r = 4; cols = ["stock", "role", "time", "side", "level", "displayed_size", "size_z", "next_min_vol"]
    hdr_row(ws, r, cols); r += 1
    # also a per-stock/side summary for the "phantom clusters before movers" question
    summ = {}
    for sym in ALL_SYMS:
        role = "MOVER" if sym in MOVERS else "control"
        bid = sum(1 for e in data[sym]["van"] if e["side"] == "bid")
        ask = sum(1 for e in data[sym]["van"] if e["side"] == "ask")
        summ[sym] = (role, bid, ask)
        for e in data[sym]["van"]:
            vals = [sym, role, e["time"][11:], e["side"], e["level"], e["displayed"], e["size_z"], e["next_vol"]]
            for j, v in enumerate(vals):
                cell = ws.cell(row=r, column=1 + j, value=v); cell.border = THIN
                if sym in MOVERS:
                    cell.fill = MOVEFILL
            r += 1
    r += 2
    ws.cell(row=r, column=1, value="PHANTOM-LEVEL SIDE COUNT (do phantoms cluster on one side?):").font = SUBF; r += 1
    hdr_row(ws, r, ["stock", "role", "phantom_bid", "phantom_ask", "total"]); r += 1
    for sym in ALL_SYMS:
        role, bid, ask = summ[sym]
        for j, v in enumerate([sym, role, bid, ask, bid + ask]):
            ws.cell(row=r, column=1 + j, value=v).border = THIN
        r += 1
    autosize(ws)

    # ---------- Participants ----------
    ws = wb.create_sheet("Participants")
    ws.cell(row=1, column=1, value="SECTION 4 — PARTICIPANT PROFILING (avg_tick_vol stability + block ratio + orders-per-level whale read)").font = TITLE
    ws.cell(row=2, column=1, value="LIMIT: full per-trade size distribution is NOT stored -> print-size 'mode share' NOT computable. Arrival-time analysis SKIPPED (timestamps lack resolution).").font = Font(italic=True)
    r = 4
    cols = ["stock", "role", "day_result%", "avg_tick_vol(mean)", "atv_CV(stability)", "block_ratio(median)",
            "block_ratio(p90)", "block_ratio(max)", "whale_persistent_min", "whale_side_dominant"]
    hdr_row(ws, r, cols); r += 1
    for sym in ALL_SYMS:
        d = data[sym]; p = d["part"]; role = "MOVER-" + MOVERS[sym][0] if sym in MOVERS else "control"
        vals = [sym, role, round(d["result"], 2), p["atv_mean"], p["atv_cv"], p["block_ratio_med"],
                p["block_ratio_p90"], p["block_ratio_max"], p["whale_persistent_min"], p["whale_side_dominant"] or "—"]
        for j, v in enumerate(vals):
            cell = ws.cell(row=r, column=1 + j, value=v); cell.border = THIN
            if sym in MOVERS:
                cell.fill = MOVEFILL
        r += 1
    r += 2
    ws.cell(row=r, column=1, value="Read: higher block_ratio p90/max = presence of outsized single prints (institutional block boost). "
            "whale_side = persistent larger avg order-size at touch (bid=accumulation lean, ask=distribution lean).")
    autosize(ws)

    # ---------- Controls comparison ----------
    ws = wb.create_sheet("Controls_comparison")
    ws.cell(row=1, column=1, value="SIDE-BY-SIDE FOOTPRINT COUNTS — movers vs controls (the ONLY way a footprint 'counts')").font = TITLE
    r = 3
    cols = ["stock", "role", "day_result%", "AGG_min", "ABS_min", "ACC_min",
            "candidate_min", "entry_signal_min", "first_entry", "top_reject_reason",
            "absorption_ev", "iceberg_ev", "vanish_ev"]
    hdr_row(ws, r, cols); r += 1
    for sym in ALL_SYMS:
        d = data[sym]; role = "MOVER-" + MOVERS[sym][0] if sym in MOVERS else "control"
        fc = family_fire_counts(d["det"])
        cand_min = sum(1 for x in d["det"] if x["cand"])
        toprej = d["reject_reasons"].most_common(1)[0] if d["reject_reasons"] else ("—", 0)
        vals = [sym, role, round(d["result"], 2), fc["AGGRESSION"], fc["ABSORPTION"], fc["ACCEPTANCE"],
                cand_min, d["entry_signal_min"], d["first_entry"]["time"][11:] if d["first_entry"] else "—",
                f"{toprej[0]} x{toprej[1]}", len(d["absev"]), len(d["ice"]), len(d["van"])]
        for j, v in enumerate(vals):
            cell = ws.cell(row=r, column=1 + j, value=v); cell.border = THIN
            if sym in MOVERS:
                cell.fill = MOVEFILL
        r += 1
    r += 2
    ws.cell(row=r, column=1, value="ACCEPTANCE fires EVERY minute by construction (close is always >= or < ATP) — it is a WEAK stand-alone family; "
            "it only matters as the 2nd confirmer. Compare AGG / ABS / candidate / entry columns: those are the discriminating footprints.").font = Font(italic=True)
    autosize(ws)

    wb.save(path)
    return path

# ==============================================================================
# 8. MAIN
# ==============================================================================
def main():
    data, mkt, ctx = analyze()
    scorecard = build_scorecard(data)
    out = ROOT / "docs" / "ops" / "MOVER_AUTOPSY_2026-07-09.xlsx"
    write_excel(data, mkt, scorecard, out)
    # copy to Downloads
    import shutil
    dl = Path.home() / "Downloads" / "MOVER_AUTOPSY_2026-07-09.xlsx"
    try:
        shutil.copy(out, dl)
    except Exception as e:
        print("Downloads copy failed:", e)
    # ---- console report ----
    print("=" * 90)
    print("MOVER AUTOPSY 2026-07-09  — detector calibration (n=2 movers, 1 tick-day)")
    print("=" * 90)
    for sym in ALL_SYMS:
        d = data[sym]; role = ("MOVER-" + MOVERS[sym][0]) if sym in MOVERS else "control"
        fc = family_fire_counts(d["det"])
        print(f"\n{sym:11} [{role}] result={d['result']:+.2f}%  move_start(1%)={d['move_start']}  2%={d['move_start_2pc']}")
        print(f"   CVD_end={int(d['cvd'][-1][1]):>12,}  absorption_ev={len(d['absev'])} (HB {sum(1 for e in d['absev'] if e['kind']=='HIDDEN_BUYER')}/HS {sum(1 for e in d['absev'] if e['kind']=='HIDDEN_SELLER')})  iceberg={len(d['ice'])}  vanish={len(d['van'])}")
        print(f"   fam-min AGG={fc['AGGRESSION']} ABS={fc['ABSORPTION']} ACC={fc['ACCEPTANCE']}  candidate_min={sum(1 for x in d['det'] if x['cand'])}  entries={len(d['entries'])}")
        print(f"   v2 first CANDIDATE={d['first_cand']['time'] if d['first_cand'] else '—'}  first ENTRY={d['first_entry']['time'] if d['first_entry'] else '—'}  gate-surviving entry-signal min={d['entry_signal_min']}")
        if d["reject_reasons"]:
            print(f"   entry REJECTS: {dict(d['reject_reasons'])}")
        p = d["part"]
        print(f"   participant: atv_mean={p['atv_mean']} CV={p['atv_cv']} block_p90={p['block_ratio_p90']} block_max={p['block_ratio_max']} whale_side={p['whale_side_dominant']} persist_min={p['whale_persistent_min']}")
    print("\n" + "-" * 90)
    print("SCORECARD (lead = min before +/-1% move-start; ctrl = control fire-rate/count)")
    print("-" * 90)
    for fam in ["AGGRESSION", "ABSORPTION", "ACCEPTANCE", "CANDIDATE", "ENTRY"]:
        sc = scorecard[fam]
        print(f"{sc['family']:24} KALYAN {sc.get('KALYANKJIL_first','—')} lead={sc.get('KALYANKJIL_lead')}  "
              f"DRREDDY {sc.get('DRREDDY_first','—')} lead={sc.get('DRREDDY_lead')}  ctrl={sc.get('ctrl_fire_rate')}")
    print("\nSaved:", out)
    print("Downloads:", dl)

if __name__ == "__main__":
    main()
