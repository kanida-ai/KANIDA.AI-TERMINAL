"""LIVE virtual-capital PAPER-TRADING engine on order-flow "lift" signals.

Consumes the live 1-min capture (mkt_orderflow_1min = book+depth+whale/crowd, and
mkt_trades_1min = tick buy/sell split) minute-by-minute and paper-trades:

  BUYER LIFT  -> go LONG  Rs 5,00,000 notional, then trail
  SELLER LIFT -> go SHORT Rs 5,00,000 notional, then trail

Trail config (user knobs):
  hard-stop        -3%   (active before the trail arms)
  trail arm        +3%   (favorable excursion that turns the trail on)
  initial floor    +2%   (locked profit the moment it arms)
  give-back        1.25% (trail = peak_favorable - 1.25%, never below the floor)

Capital UNCONSTRAINED: every valid signal is taken at Rs 5L. One position per symbol at
a time; after a symbol's trade closes it may RE-ENTER on a fresh signal the same day.
Intraday only -> any open position is squared off at 15:29.

Signal (per instrument, per minute):
  base trigger  : 1-min price thrust >= +THR (long) / <= -THR (short) on a >= VMULT x
                  volume spike vs the trailing WIN-minute median.
  book confirm  : buy_imb (total pending) buy-lean for longs / sell-lean for shorts,
                  OR near-touch top-5 depth-qty lean the same way.
  ENRICHMENT (recorded, not required -> so we can measure what actually matters):
    whale/crowd : avg qty-per-order at the pressing touch (b1q/b1o vs a1q/a1o). One big
                  order lifting = whale (institutional); many small = crowd (retail).
    tick        : buy_vol_pct + largest single print (block) from the websocket ticks.
  A 'conviction' score (0-3) counts how many of {book, whale, tick} agree with direction.

Modes:
  --live            run the current live session minute-by-minute (self-gates 09:15-15:30 IST)
  --replay YYYY-MM-DD   re-run a stored session as if live (calibration / reference day)
  --report YYYY-MM-DD   (re)build the Excel journal + metrics for a date
All trades persist to flow_paper_trades; open positions to flow_paper_positions.
"""
import os, sys, sqlite3, time, argparse, shutil
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
sys.path.insert(0, str(ROOT / "scripts")); sys.path.insert(0, str(ROOT / "universe_engine"))
from mkt_poller import DB, IST, phase

# ---- knobs -------------------------------------------------------------------
CAP        = 500_000        # Rs per signal (notional)
HARD_STOP  = 0.025          # -2.5% pre-arm (was -3%)
ARM        = 0.013          # +1.3% arms the trail (was +3% — too high intraday, winners evaporated)
FLOOR      = 0.009          # +0.9% locked floor once armed (was +2%)
GIVEBACK   = 0.006          # 0.6% give-back from peak (was 1.25%)
THR        = 0.0015         # 0.15% min 1-min thrust to call a lift
VMULT      = 2.0            # volume >= 2x trailing median = spike
WIN        = 20            # trailing-median window (minutes)
IMB_HI     = 0.52           # book buy-lean threshold (sell-lean = 1-IMB_HI)
DEPTH_HI   = 0.55           # near-touch depth buy-lean threshold
TICK_HI    = 55.0           # tick buy_vol_pct threshold
WHALE_MULT = 1.5            # pressing-side avg order >= 1.5x other side = whale
MIN_PRICE  = 20.0           # skip sub-Rs20 names
CONSEC     = 2              # require N consecutive same-direction lift bars before entering
FLIP_ATP   = False          # lever #1: trade WITH ATP location (flip counter-location lifts)
ENTRY_CUT  = "15:15"        # no NEW entries after this (still trail existing)
EOD_MIN    = "15:29"        # force square-off
WARMUP     = 5              # suppress signals in the first N minutes (opening noise)
NIFTY_KEY  = "NSE:NIFTY 50" # regime anchor for the v2 direction gate
EMA_WIN    = 15             # NIFTY EMA window for the regime read

PAPER_SCHEMA = """
CREATE TABLE IF NOT EXISTS flow_paper_trades (
    trade_date TEXT, variant TEXT, symbol TEXT, segment TEXT, instrument_key TEXT, side TEXT,
    entry_min TEXT, entry_px REAL, qty INTEGER, notional REAL,
    exit_min TEXT, exit_px REAL, exit_reason TEXT, armed INTEGER,
    pnl_rs REAL, pnl_pct REAL, mfe_pct REAL, mae_pct REAL, hold_min INTEGER,
    ret_signal REAL, buy_imb REAL, depth_bid REAL, whale_side TEXT, ord_ratio REAL,
    tick_buy_pct REAL, block_qty INTEGER, conviction INTEGER, net_pnl_rs REAL, atp REAL, flipped INTEGER,
    PRIMARY KEY (trade_date, variant, instrument_key, entry_min)
);
CREATE INDEX IF NOT EXISTS idx_ptr_date ON flow_paper_trades(trade_date, variant);
CREATE TABLE IF NOT EXISTS flow_paper_positions (
    trade_date TEXT, variant TEXT, symbol TEXT, segment TEXT, instrument_key TEXT, side TEXT, entry_min TEXT,
    entry_px REAL, qty INTEGER, peak_fav REAL, cur_px REAL, upnl_rs REAL, armed INTEGER,
    PRIMARY KEY (trade_date, variant, instrument_key)
);
"""

OF_KEEP = ("open", "high", "low", "close", "volume", "atp", "total_buy_qty", "total_sell_qty",
           "b1q", "b2q", "b3q", "b4q", "b5q", "a1q", "a2q", "a3q", "a4q", "a5q",
           "b1o", "a1o", "oi")


def _con(ro=False):
    if ro:
        c = sqlite3.connect("file:" + str(DB).replace("\\", "/") + "?mode=ro", uri=True, timeout=30)
    else:
        c = sqlite3.connect(str(DB), timeout=60); c.execute("PRAGMA busy_timeout=60000")
    return c


def cost(seg, entry, exit, qty):
    """Standard Zerodha intraday charges (round-trip) -> Rs. Equity MIS vs futures."""
    tb, ts = entry * qty, exit * qty
    brok = min(20, 0.0003 * tb) + min(20, 0.0003 * ts)
    if seg in ("FUT", "INDEX"):
        stt = 0.0002 * ts; txn = 0.0000173 * (tb + ts); stamp = 0.00002 * tb
    else:
        stt = 0.00025 * ts; txn = 0.0000297 * (tb + ts); stamp = 0.00003 * tb
    sebi = 0.000001 * (tb + ts); gst = 0.18 * (brok + txn + sebi)
    return brok + stt + txn + stamp + sebi + gst


class Sym:
    __slots__ = ("prev_close", "vol", "seg", "pos", "streak_dir", "streak_n")
    def __init__(self, seg):
        self.prev_close = None; self.vol = deque(maxlen=WIN); self.seg = seg; self.pos = None
        self.streak_dir = None; self.streak_n = 0   # consecutive same-direction lift bars


class Engine:
    def __init__(self, trade_date, verbose=True, consec=CONSEC, flip_atp=FLIP_ATP,
                 regime_gate=False, variant="v1"):
        self.date = trade_date; self.syms = {}; self.trades = []; self.verbose = verbose
        self.n_min = 0; self.consec = consec; self.flip_atp = flip_atp
        self.regime_gate = regime_gate; self.variant = variant
        self.nifty_ema = None; self.nifty_n = 0; self.breadth_dir = None; self.regime_dir = None

    def _update_regime(self, of_rows):
        """v2 direction brain: NIFTY vs its EMA AND market breadth must agree -> regime_dir."""
        nrow = of_rows.get(NIFTY_KEY); nc = nrow["close"] if nrow else None
        ema_dir = None
        if nc is not None:
            k = 2.0 / (EMA_WIN + 1)
            self.nifty_ema = nc if self.nifty_ema is None else self.nifty_ema * (1 - k) + nc * k
            self.nifty_n += 1
            if self.nifty_n >= 5:
                ema_dir = "long" if nc > self.nifty_ema else "short" if nc < self.nifty_ema else None
        up = tot = 0
        for of in of_rows.values():
            if of["segment"] == "INDEX":
                continue
            o, c = of.get("open"), of.get("close")
            if o is None or c is None:
                continue
            tot += 1
            if c > o:
                up += 1
        if tot:
            self.breadth_dir = "long" if (up / tot) >= 0.5 else "short"
        self.regime_dir = ema_dir if (ema_dir and ema_dir == self.breadth_dir) else None

    # ---- signal ----
    def _signal(self, s, of, tick):
        c = of["close"]
        if c is None or c < MIN_PRICE or s.prev_close is None or not s.vol:
            return None
        ret = c / s.prev_close - 1
        med = sorted(s.vol)[len(s.vol) // 2]
        vol = of["volume"] or 0
        spike = med > 0 and vol >= VMULT * med
        if not spike or abs(ret) < THR:
            return None
        tbq, tsq = of["total_buy_qty"] or 0, of["total_sell_qty"] or 0
        imb = tbq / (tbq + tsq) if (tbq + tsq) else 0.5
        bd = sum(of.get(f"b{i}q") or 0 for i in range(1, 6))
        ad = sum(of.get(f"a{i}q") or 0 for i in range(1, 6))
        depth = bd / (bd + ad) if (bd + ad) else 0.5
        side = "long" if ret >= THR else "short"
        book_ok = (imb >= IMB_HI or depth >= DEPTH_HI) if side == "long" else (imb <= 1 - IMB_HI or depth <= 1 - DEPTH_HI)
        if not book_ok:
            return None
        # lever #1: ATP location flip — keep the trigger (count unchanged), correct the SIDE.
        # buy-lift BELOW ATP = trapped buyers -> short; sell-lift ABOVE ATP = trapped sellers -> long.
        atp = of.get("atp"); flipped = 0
        if self.flip_atp and atp and c:
            if side == "long" and c < atp:
                side = "short"; flipped = 1
            elif side == "short" and c > atp:
                side = "long"; flipped = 1
        # lever (v2): regime-gate direction flip — trade WITH the confirmed market regime
        if self.regime_gate and self.regime_dir and side != self.regime_dir:
            side = self.regime_dir; flipped = 1
        # enrichment (whale/crowd + tick) — recorded, feeds conviction; uses the FINAL side
        b1q, b1o, a1q, a1o = of.get("b1q"), of.get("b1o"), of.get("a1q"), of.get("a1o")
        avg_bid = (b1q / b1o) if (b1q and b1o) else None
        avg_ask = (a1q / a1o) if (a1q and a1o) else None
        whale_side, ratio = None, None
        if avg_bid and avg_ask:
            if avg_bid >= WHALE_MULT * avg_ask:
                whale_side, ratio = "bid", avg_bid / avg_ask
            elif avg_ask >= WHALE_MULT * avg_bid:
                whale_side, ratio = "ask", avg_ask / avg_bid
        tbp = tick.get("buy_vol_pct") if tick else None
        block = tick.get("max_trade_qty") if tick else None
        conv = 1  # book already agreed
        if whale_side == ("bid" if side == "long" else "ask"):
            conv += 1
        if tbp is not None and ((side == "long" and tbp >= TICK_HI) or (side == "short" and tbp <= 100 - TICK_HI)):
            conv += 1
        return dict(side=side, ret=ret, imb=imb, depth=depth, whale_side=whale_side,
                    ratio=ratio, tick_buy_pct=tbp, block=block, conv=conv, atp=atp, flipped=flipped)

    # ---- position mgmt ----
    def _manage(self, s, of, minute):
        p = s.pos; hi, lo, o = of["high"], of["low"], of["open"]
        entry, peak, side = p["entry_px"], p["peak_fav"], p["side"]
        armed = peak >= ARM
        if side == "long":
            stop = entry * (1 + max(FLOOR, peak - GIVEBACK)) if armed else entry * (1 - HARD_STOP)
            if o is not None and o <= stop:
                return self._close(s, o, minute, "gap_stop", armed)
            if lo is not None and lo <= stop:
                return self._close(s, stop, minute, "trail_stop" if armed else "hard_stop", armed)
        else:
            stop = entry * (1 - max(FLOOR, peak - GIVEBACK)) if armed else entry * (1 + HARD_STOP)
            if o is not None and o >= stop:
                return self._close(s, o, minute, "gap_stop", armed)
            if hi is not None and hi >= stop:
                return self._close(s, stop, minute, "trail_stop" if armed else "hard_stop", armed)
        # still open -> update excursions with this bar
        if side == "long":
            fav = (hi / entry - 1) if hi else 0; adv = (1 - lo / entry) if lo else 0
        else:
            fav = (1 - lo / entry) if lo else 0; adv = (hi / entry - 1) if hi else 0
        p["peak_fav"] = max(p["peak_fav"], fav); p["max_adv"] = max(p["max_adv"], adv)
        p["cur_px"] = of["close"]
        return None

    def _open(self, s, sym, key, of, minute, sig):
        px = of["close"]; qty = int(CAP // px)
        if qty < 1:
            return
        s.pos = dict(symbol=sym, key=key, seg=s.seg, side=sig["side"], entry_min=minute,
                     entry_px=px, qty=qty, peak_fav=0.0, max_adv=0.0, cur_px=px, sig=sig)

    def _close(self, s, exit_px, minute, reason, armed):
        p = s.pos
        entry, qty, side = p["entry_px"], p["qty"], p["side"]
        pnl = qty * (exit_px - entry) if side == "long" else qty * (entry - exit_px)
        pct = (exit_px / entry - 1) if side == "long" else (entry / exit_px - 1)
        net = pnl - cost(s.seg, entry, exit_px, qty)
        hold = self._minutes_between(p["entry_min"], minute)
        sig = p["sig"]
        self.trades.append(dict(
            trade_date=self.date, variant=self.variant, symbol=p["symbol"], segment=s.seg, instrument_key=p["key"],
            side=side, entry_min=p["entry_min"], entry_px=round(entry, 2), qty=qty,
            notional=round(entry * qty, 0), exit_min=minute, exit_px=round(exit_px, 2),
            exit_reason=reason, armed=int(armed), pnl_rs=round(pnl, 0), pnl_pct=round(pct * 100, 3),
            mfe_pct=round(p["peak_fav"] * 100, 3), mae_pct=round(p["max_adv"] * 100, 3), hold_min=hold,
            ret_signal=round(sig["ret"] * 100, 3), buy_imb=round(sig["imb"], 3), depth_bid=round(sig["depth"], 3),
            whale_side=sig["whale_side"], ord_ratio=round(sig["ratio"], 2) if sig["ratio"] else None,
            tick_buy_pct=sig["tick_buy_pct"], block_qty=sig["block"], conviction=sig["conv"],
            net_pnl_rs=round(net, 0), atp=round(sig["atp"], 2) if sig.get("atp") else None,
            flipped=sig.get("flipped", 0)))
        s.pos = None
        return self.trades[-1]

    @staticmethod
    def _minutes_between(a, b):
        fa = datetime.strptime(a, "%Y-%m-%d %H:%M"); fb = datetime.strptime(b, "%Y-%m-%d %H:%M")
        return int((fb - fa).total_seconds() // 60)

    # ---- per-minute step ----
    def on_minute(self, minute, of_rows, tick_rows):
        hhmm = minute[11:16]; is_eod = hhmm >= EOD_MIN
        if self.regime_gate:
            self._update_regime(of_rows)
        opened = closed = 0
        for key, of in of_rows.items():
            sym, seg = of["symbol"], of["segment"]
            if seg == "INDEX" and key.startswith("NSE:"):   # index SPOT not traded; index FUT (seg FUT) is
                continue
            s = self.syms.get(key)
            if s is None:
                s = self.syms[key] = Sym(seg)
            tick = tick_rows.get(key)
            # 1) evaluate the lift EVERY bar so the consecutive-streak stays accurate
            sig = self._signal(s, of, tick) if of["close"] is not None else None
            if sig is not None:
                if sig["side"] == s.streak_dir:
                    s.streak_n += 1
                else:
                    s.streak_dir = sig["side"]; s.streak_n = 1
            else:
                s.streak_dir = None; s.streak_n = 0
            # 2) manage existing position on this bar
            if s.pos is not None:
                r = self._manage(s, of, minute)
                if r is None and is_eod:
                    r = self._close(s, of["close"], minute, "EOD", s.pos["peak_fav"] >= ARM)
                if r is not None:
                    closed += 1
            # 3) enter only on CONSEC consecutive same-direction lifts, when flat & before cutoff
            if (s.pos is None and hhmm < ENTRY_CUT and self.n_min >= WARMUP
                    and sig is not None and s.streak_n >= self.consec):
                self._open(s, sym, key, of, minute, sig); opened += 1
            # 4) update rolling state
            if of["close"] is not None:
                s.prev_close = of["close"]
            if of["volume"] is not None:
                s.vol.append(of["volume"])
        self.n_min += 1
        if self.verbose and (opened or closed):
            live = sum(1 for s in self.syms.values() if s.pos)
            print(f"  [{minute[11:16]}] +{opened} entries  -{closed} exits  | open={live}  trades={len(self.trades)}", flush=True)
        return opened, closed

    def open_upnl(self):
        return sum((p["cur_px"] - p["entry_px"]) * p["qty"] if p["side"] == "long"
                   else (p["entry_px"] - p["cur_px"]) * p["qty"]
                   for p in (s.pos for s in self.syms.values()) if p)

    def persist(self):
        con = _con(); con.executescript(PAPER_SCHEMA)
        if self.trades:
            cols = list(self.trades[0].keys())
            con.executemany(f"INSERT OR REPLACE INTO flow_paper_trades ({','.join(cols)}) VALUES ({','.join('?'*len(cols))})",
                            [tuple(t[c] for c in cols) for t in self.trades])
        con.execute("DELETE FROM flow_paper_positions WHERE trade_date=? AND variant=?", (self.date, self.variant))
        for s in self.syms.values():
            p = s.pos
            if p:
                up = ((p["cur_px"] - p["entry_px"]) * p["qty"]) if p["side"] == "long" else ((p["entry_px"] - p["cur_px"]) * p["qty"])
                con.execute("INSERT OR REPLACE INTO flow_paper_positions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            (self.date, self.variant, p["symbol"], s.seg, p["key"], p["side"], p["entry_min"], p["entry_px"],
                             p["qty"], round(p["peak_fav"] * 100, 2), p["cur_px"], round(up, 0), int(p["peak_fav"] >= ARM)))
        con.commit(); con.close()


# ---- data access -------------------------------------------------------------
def load_minute(con, minute):
    of = {}
    cols = "symbol,segment,instrument_key," + ",".join(OF_KEEP)
    for r in con.execute(f"SELECT {cols} FROM mkt_orderflow_1min WHERE bar_time=?", (minute,)):
        d = dict(symbol=r[0], segment=r[1])
        for i, c in enumerate(OF_KEEP):
            d[c] = r[3 + i]
        of[r[2]] = d
    tick = {}
    try:
        for r in con.execute("SELECT instrument_key,buy_vol_pct,max_trade_qty,volume,n_ticks "
                             "FROM mkt_trades_1min WHERE bar_time=?", (minute,)):
            tick[r[0]] = dict(buy_vol_pct=r[1], max_trade_qty=r[2], volume=r[3], n_ticks=r[4])
    except sqlite3.OperationalError:
        pass
    return of, tick


def session_minutes(con, date, upto=None):
    q = "SELECT DISTINCT bar_time FROM mkt_orderflow_1min WHERE substr(bar_time,1,10)=? AND bar_time>=? "
    args = [date, f"{date} 09:15"]
    if upto:
        q += "AND bar_time<=? "; args.append(upto)
    q += "ORDER BY bar_time"
    return [r[0] for r in con.execute(q, args)]


def wipe(date, variant=None):
    con = _con(); con.executescript(PAPER_SCHEMA)
    if variant:
        con.execute("DELETE FROM flow_paper_trades WHERE trade_date=? AND variant=?", (date, variant))
        con.execute("DELETE FROM flow_paper_positions WHERE trade_date=? AND variant=?", (date, variant))
    else:
        con.execute("DELETE FROM flow_paper_trades WHERE trade_date=?", (date,))
        con.execute("DELETE FROM flow_paper_positions WHERE trade_date=?", (date,))
    con.commit(); con.close()


# ---- runners -----------------------------------------------------------------
def _load_day(con, date):
    """One pass over the stored day -> [(minute, of_rows, tick_rows), ...] grouped by minute."""
    cols = "bar_time,symbol,segment,instrument_key," + ",".join(OF_KEEP)
    tickmap = {}
    try:
        for r in con.execute("SELECT bar_time,instrument_key,buy_vol_pct,max_trade_qty FROM mkt_trades_1min "
                             "WHERE substr(bar_time,1,10)=?", (date,)):
            tickmap.setdefault(r[0], {})[r[1]] = dict(buy_vol_pct=r[2], max_trade_qty=r[3])
    except sqlite3.OperationalError:
        pass
    out = []; cur = None; of_rows = {}
    for r in con.execute(f"SELECT {cols} FROM mkt_orderflow_1min WHERE substr(bar_time,1,10)=? AND bar_time>=? ORDER BY bar_time",
                         (date, f"{date} 09:15")):
        m = r[0]
        if cur is not None and m != cur:
            out.append((cur, of_rows, tickmap.get(cur, {}))); of_rows = {}
        cur = m
        d = dict(symbol=r[1], segment=r[2])
        for i, c in enumerate(OF_KEEP):
            d[c] = r[4 + i]
        of_rows[r[3]] = d
    if of_rows:
        out.append((cur, of_rows, tickmap.get(cur, {})))
    return out


def run_replay(date):
    print(f"[paper] REPLAY {date} (calibration / reference)", flush=True)
    wipe(date)
    con = _con(ro=True); minutes = _load_day(con, date); con.close()
    eng = Engine(date)
    for m, of, tk in minutes:
        eng.on_minute(m, of, tk)
    eng.persist()
    print(f"[paper] REPLAY done: {len(eng.trades)} trades, {sum(1 for s in eng.syms.values() if s.pos)} still open (EOD forced)", flush=True)
    report(date)


def _metrics(trades):
    import statistics as st
    if not trades:
        return dict(n=0)
    n = len(trades); wins = [t for t in trades if t["pnl_rs"] > 0]
    gp = sum(t["pnl_rs"] for t in wins); gl = -sum(t["pnl_rs"] for t in trades if t["pnl_rs"] <= 0)
    from collections import Counter
    rc = Counter(t["exit_reason"] for t in trades)
    return dict(n=n, wr=len(wins) / n * 100, avg_ret=st.mean(t["pnl_pct"] for t in trades),
                avg_mfe=st.mean(t["mfe_pct"] for t in trades), avg_mae=st.mean(t["mae_pct"] for t in trades),
                pf=(gp / gl if gl else float("inf")), gross=sum(t["pnl_rs"] for t in trades),
                net=sum(t["net_pnl_rs"] for t in trades), armed_pct=sum(t["armed"] for t in trades) / n * 100,
                eod_pct=rc.get("EOD", 0) / n * 100)


def run_abc(date, consecs=(1, 2, 3)):
    """A/B/C: same day, same trail config, entry gate = 1 vs 2 vs 3 consecutive lifts."""
    print(f"[paper] A/B/C replay {date} | consec = {consecs} | trail: hardstop -{HARD_STOP:.0%} arm +{ARM:.0%} "
          f"floor +{FLOOR:.0%} give {GIVEBACK:.2%}", flush=True)
    con = _con(ro=True); minutes = _load_day(con, date); con.close()
    variants = {}
    for k in consecs:
        eng = Engine(date, verbose=False, consec=k)
        for m, of, tk in minutes:
            eng.on_minute(m, of, tk)
        variants[k] = eng.trades

    def line(lbl, m):
        if not m.get("n"):
            return f"  {lbl:10s}  n=0"
        return (f"  {lbl:10s}  n={m['n']:>4d}  WR={m['wr']:>5.1f}%  avgRet={m['avg_ret']:>+7.3f}%  "
                f"MFE={m['avg_mfe']:>4.2f}%  PF={m['pf']:>5.2f}  armed={m['armed_pct']:>4.0f}%  "
                f"EOD={m['eod_pct']:>3.0f}%  net=Rs{m['net']:>12,.0f}")
    for k in consecs:
        tr = variants[k]
        print(f"\n===== CONSEC = {k} ({'single lift' if k==1 else str(k)+' consecutive lifts'}) =====", flush=True)
        print(line("ALL", _metrics(tr)))
        print(line("LONG", _metrics([t for t in tr if t["side"] == "long"])))
        print(line("SHORT", _metrics([t for t in tr if t["side"] == "short"])))
        print(line("CASH", _metrics([t for t in tr if t["segment"] == "CASH"])))
        print(line("FUT", _metrics([t for t in tr if t["segment"] == "FUT"])))
    _abc_excel(date, variants, consecs)


def run_flip_ab(date, consec=2):
    """Lever #1 A/B: consec-gated, ATP-flip OFF vs ON (same trigger set -> same count)."""
    print(f"[paper] FLIP A/B {date} | consec={consec} | OFF vs ON (ATP location flip)", flush=True)
    con = _con(ro=True); minutes = _load_day(con, date); con.close()
    res = {}
    for flip in (False, True):
        eng = Engine(date, verbose=False, consec=consec, flip_atp=flip)
        for m, of, tk in minutes:
            eng.on_minute(m, of, tk)
        res[flip] = eng.trades

    def line(lbl, m):
        if not m.get("n"):
            return f"  {lbl:10s}  n=0"
        return (f"  {lbl:10s}  n={m['n']:>4d}  WR={m['wr']:>5.1f}%  avgRet={m['avg_ret']:>+7.3f}%  "
                f"PF={m['pf']:>5.2f}  net=Rs{m['net']:>12,.0f}")
    for flip in (False, True):
        tr = res[flip]; nfl = sum(t["flipped"] for t in tr)
        print(f"\n===== FLIP {'ON' if flip else 'OFF'} ({nfl} of {len(tr)} sides flipped) =====", flush=True)
        print(line("ALL", _metrics(tr)))
        print(line("LONG", _metrics([t for t in tr if t["side"] == "long"])))
        print(line("SHORT", _metrics([t for t in tr if t["side"] == "short"])))
        print(line("CASH", _metrics([t for t in tr if t["segment"] == "CASH"])))
        print(line("FUT", _metrics([t for t in tr if t["segment"] == "FUT"])))


def _abc_excel(date, variants, consecs):
    try:
        import pandas as pd
    except Exception:
        return
    out = ROOT / "docs" / "ops" / f"PAPER_FLOW_ABC_{date}.xlsx"
    rows = []
    for k in consecs:
        tr = variants[k]
        for lbl, sub in [("ALL", tr), ("LONG", [t for t in tr if t["side"] == "long"]),
                         ("SHORT", [t for t in tr if t["side"] == "short"]),
                         ("CASH", [t for t in tr if t["segment"] == "CASH"]),
                         ("FUT", [t for t in tr if t["segment"] == "FUT"])]:
            m = _metrics(sub)
            if m.get("n"):
                rows.append(dict(consec=k, bucket=lbl, n=m["n"], WR_pct=round(m["wr"], 1),
                                 avg_ret_pct=round(m["avg_ret"], 3), avg_mfe_pct=round(m["avg_mfe"], 2),
                                 PF=round(m["pf"], 2), armed_pct=round(m["armed_pct"], 0),
                                 eod_pct=round(m["eod_pct"], 0), gross_rs=round(m["gross"]), net_rs=round(m["net"])))
            else:
                rows.append(dict(consec=k, bucket=lbl, n=0))
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        pd.DataFrame(rows).to_excel(w, sheet_name="Compare", index=False)
        for k in consecs:
            if variants[k]:
                pd.DataFrame(variants[k]).to_excel(w, sheet_name=f"Trades_consec{k}", index=False)
    try:
        shutil.copy(out, Path.home() / "Downloads" / out.name)
    except Exception:
        pass
    print(f"\n[paper] A/B/C journal -> {out}  (copy in Downloads)", flush=True)


def run_live_multi():
    """Run v1 (your config) and v2 (config + regime direction gate) as PARALLEL live streams."""
    date = datetime.now(IST).strftime("%Y-%m-%d")
    cfgs = [dict(variant="v1", regime_gate=False), dict(variant="v2", regime_gate=True)]
    print(f"[paper] LIVE MULTI {date} | v1=base  v2=+regime-gate | phase={phase()} | "
          f"{datetime.now(IST):%H:%M:%S IST}", flush=True)
    while phase() == "pre-open":
        print("  pre-open — waiting for 09:15...", flush=True); time.sleep(20)
    if phase() != "open":
        print(f"[paper] phase={phase()} — no live session now.", flush=True); return
    engs = []
    for c in cfgs:
        wipe(date, c["variant"])
        engs.append(Engine(date, verbose=False, consec=CONSEC, regime_gate=c["regime_gate"], variant=c["variant"]))
    last = f"{date} 09:14"
    while True:
        p = phase(); cur_min = datetime.now(IST).strftime("%Y-%m-%d %H:%M")
        con = _con(ro=True)
        mins = [m for m in session_minutes(con, date) if m > last and m < cur_min]
        data = [(m,) + load_minute(con, m) for m in mins]
        con.close()
        for m, of, tk in data:
            for e in engs:
                e.on_minute(m, of, tk)
            last = m
        if data:
            for e in engs:
                e.persist()
            parts = " | ".join(f"{e.variant}: {len(e.trades)}tr net Rs{sum(t['net_pnl_rs'] for t in e.trades):,.0f} "
                               f"+open Rs{e.open_upnl():,.0f}" for e in engs)
            print(f"    [{last[11:16]}] {parts}", flush=True)
        if p in ("closed", "weekend"):
            for e in engs:
                e.persist()
            print("[paper] session closed — finalizing both variants", flush=True)
            break
        time.sleep(12)
    for c in cfgs:
        report(date, c["variant"])


def run_live():
    date = datetime.now(IST).strftime("%Y-%m-%d")
    print(f"[paper] LIVE session {date} | phase={phase()} | now={datetime.now(IST):%H:%M:%S IST}", flush=True)
    while phase() == "pre-open":
        print("  pre-open — waiting for 09:15...", flush=True); time.sleep(20)
    if phase() not in ("open",):
        print(f"[paper] phase={phase()} — no live session now. Use --replay for a past day.", flush=True); return
    wipe(date)
    eng = Engine(date); last = f"{date} 09:14"
    while True:
        p = phase()
        cur_min = datetime.now(IST).strftime("%Y-%m-%d %H:%M")
        con = _con(ro=True)
        # process every completed minute we haven't seen yet (strictly before the current wall minute)
        mins = [m for m in session_minutes(con, date) if m > last and m < cur_min]
        for m in mins:
            of, tick = load_minute(con, m); eng.on_minute(m, of, tick); last = m
        con.close()
        if mins:
            eng.persist()
            print(f"    live P&L: net Rs {sum(t['net_pnl_rs'] for t in eng.trades):,.0f} closed + "
                  f"Rs {eng.open_upnl():,.0f} open  ({len(eng.trades)} trades)", flush=True)
        if p in ("closed", "weekend"):
            print("[paper] session closed — finalizing", flush=True)
            eng.persist(); break
        time.sleep(12)
    report(date)


# ---- reporting ---------------------------------------------------------------
def _fmt(trades):
    import statistics as st
    if not trades:
        return "no trades"
    n = len(trades); wins = [t for t in trades if t["pnl_rs"] > 0]
    wr = len(wins) / n * 100
    gross = sum(t["pnl_rs"] for t in trades); net = sum(t["net_pnl_rs"] for t in trades)
    avg_ret = st.mean(t["pnl_pct"] for t in trades); avg_mfe = st.mean(t["mfe_pct"] for t in trades)
    avg_mae = st.mean(t["mae_pct"] for t in trades)
    gp = sum(t["pnl_rs"] for t in wins); gl = -sum(t["pnl_rs"] for t in trades if t["pnl_rs"] <= 0)
    pf = gp / gl if gl else float("inf")
    return (f"n={n} WR={wr:.1f}% avgRet={avg_ret:+.3f}% avgMFE={avg_mfe:.2f}% avgMAE={avg_mae:.2f}% "
            f"PF={pf:.2f} gross=Rs{gross:,.0f} net=Rs{net:,.0f}")


def report(date, variant=None):
    con = _con(ro=True)
    cols = [x[1] for x in con.execute("PRAGMA table_info(flow_paper_trades)")]
    q = "SELECT * FROM flow_paper_trades WHERE trade_date=?"; args = [date]
    if variant:
        q += " AND variant=?"; args.append(variant)
    rows = [dict(zip(cols, r)) for r in con.execute(q + " ORDER BY entry_min", args)]
    con.close()
    print(f"\n===== PAPER FLOW REPORT {date}{(' ['+variant+']') if variant else ''} =====", flush=True)
    print("ALL     :", _fmt(rows))
    for side in ("long", "short"):
        print(f"{side.upper():8s}:", _fmt([t for t in rows if t["side"] == side]))
    for seg in ("CASH", "FUT"):
        print(f"{seg:8s}:", _fmt([t for t in rows if t["segment"] == seg]))
    from collections import Counter
    rc = Counter(t["exit_reason"] for t in rows)
    print("exits   :", dict(rc))
    # conviction buckets (does whale/tick confirmation help?)
    for cv in (1, 2, 3):
        print(f"conv={cv}  :", _fmt([t for t in rows if t["conviction"] == cv]))
    if rows:
        _excel(date, rows, variant)


def _excel(date, rows, variant=None):
    try:
        import pandas as pd
    except Exception:
        print("[paper] pandas missing — skip Excel", flush=True); return
    out = ROOT / "docs" / "ops" / f"PAPER_FLOW_{date}{('_'+variant) if variant else ''}.xlsx"
    df = pd.DataFrame(rows)
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="Trades", index=False)
        def agg(g):
            wins = (g["pnl_rs"] > 0).sum()
            return pd.Series(dict(n=len(g), WR_pct=round(wins / len(g) * 100, 1),
                                  avg_ret_pct=round(g["pnl_pct"].mean(), 3), avg_mfe_pct=round(g["mfe_pct"].mean(), 2),
                                  avg_mae_pct=round(g["mae_pct"].mean(), 2), gross_rs=round(g["pnl_rs"].sum()),
                                  net_rs=round(g["net_pnl_rs"].sum())))
        summ = pd.concat([df.assign(grp="ALL").groupby("grp").apply(agg),
                          df.groupby("side").apply(agg), df.groupby("segment").apply(agg),
                          df.groupby("conviction").apply(agg)])
        summ.to_excel(w, sheet_name="Summary")
        df.groupby("symbol").apply(agg).sort_values("net_rs", ascending=False).to_excel(w, sheet_name="By_Symbol")
        df["hour"] = df["entry_min"].str[11:13]
        df.groupby("hour").apply(agg).to_excel(w, sheet_name="By_Hour")
    try:
        dl = Path.home() / "Downloads" / out.name; shutil.copy(out, dl)
        print(f"[paper] journal -> {out}  (copy: {dl})", flush=True)
    except Exception:
        print(f"[paper] journal -> {out}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--livemulti", action="store_true")
    ap.add_argument("--replay"); ap.add_argument("--report"); ap.add_argument("--abc")
    ap.add_argument("--flipab")
    a = ap.parse_args()
    if a.livemulti:
        run_live_multi()
    elif a.flipab:
        run_flip_ab(a.flipab)
    elif a.abc:
        run_abc(a.abc)
    elif a.replay:
        run_replay(a.replay)
    elif a.report:
        report(a.report)
    else:
        run_live()


if __name__ == "__main__":
    main()
