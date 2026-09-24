"""EXPERIMENT — STACKED v2: compose the three ALREADY-VALIDATED levers into ONE engine.

Does NOT edit flow_paper_engine.py. A single Engine subclass applies, together:

  L1  DIRECTION / REGIME FLIP  (from exp_direction_gate.py, winning variant b3):
        regime = NIFTY-50 vs EMA15 CONFIRMED-BY-breadth (ema_and_breadth). When the
        raw order-flow lift side disagrees with the confirmed market regime, FLIP the
        side to the regime side (keeps the trade -> count preserved). Micro-trend and
        the single-EMA / anchor variants are discarded.

  L2  ATR NORMALIZATION        (from exp_atr_norm.py, setting S1 = k1 m10 p10 q4):
        per-stock 1-min ATR% scales the entry thrust (max(THR, k*ATR%)) and the
        stop/arm/giveback frozen at entry (stop=clamp(m*ATR%,1.5,4%), arm=p*ATR%,
        give=q*ATR%, floor proportional = 2/3 of arm). ~1.3% arm for an avg-vol stock.

  L3  SMART-ZONE ENTRY         (from exp_smartzone_exits.py, ENTRY part only):
        on a fresh signal, register a PENDING entry and wait up to 2 bars for a shallow
        pullback toward micro-support (long: max(4-bar swing low, close*(1-0.30%));
        short: mirror). Fill there if tagged; else MARKET-fill at close after 2 bars
        (count preserved). The scratch exit is deliberately EXCLUDED (it hurt WR).

Toggles (use_dir, use_atr, use_entry) let us run baseline, each lever alone, the full
stack, and the three leave-one-out ablations on the same signal stream.

Run:  C:/Users/SPS/anaconda3/python.exe exp_v2_stacked.py [YYYY-MM-DD]
"""
import sys
from collections import deque

ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, ROOT + r"\scripts")
sys.path.insert(0, ROOT + r"\universe_engine")

import flow_paper_engine as fpe
from flow_paper_engine import Engine, _load_day, _metrics, _con

# ---- L1 regime knobs (context only; trigger/consec untouched) ----
NIFTY_KEY = "NSE:NIFTY 50"
EMA_WIN   = 15
# ---- L2 ATR setting S1 ----
ATR_K, ATR_M, ATR_P, ATR_Q, ATR_WIN = 1.0, 10, 10, 4.0, 14
# ---- L3 smart-zone entry knobs ----
WAIT_BARS  = 2
TARGET_DIP = 0.003
SWING_LOOK = 4


class V2Engine(Engine):
    """One engine that composes L1 direction-flip + L2 ATR trail + L3 smart-zone entry.

    With all three toggles False it is byte-identical to the base Engine.
    """

    def __init__(self, trade_date, use_dir=False, use_atr=False, use_entry=False, consec=2):
        super().__init__(trade_date, verbose=False, consec=consec, flip_atp=False)
        self.use_dir, self.use_atr, self.use_entry = use_dir, use_atr, use_entry
        self.k, self.m, self.p, self.q = ATR_K, ATR_M, ATR_P, ATR_Q
        self.atr_win = ATR_WIN
        # L2 state
        self.atr = {}            # id(Sym) -> dict(prevc, trs deque, atrpct)
        self.entry_atrs = []
        # L1 state
        self.nifty_ema = None; self.nifty_anchor = None; self.nifty_n = 0
        self.regime_dir = None; self.breadth = None; self.breadth_dir = None
        self.n_flip = 0
        self.reg_minutes = {"long": 0, "short": 0, "none": 0}
        # L3 state
        self.pending = {}        # key -> pending-entry dict
        self.hist = {}           # key -> deque[(low,high)]
        self.n_pull_fill = 0; self.n_market_fill = 0

    # ================= L1: market regime (recomputed once per minute) =================
    def _update_regime(self, of_rows):
        nrow = of_rows.get(NIFTY_KEY)
        nc = nrow["close"] if nrow else None
        ema_dir = None
        if nc is not None:
            k = 2.0 / (EMA_WIN + 1)
            self.nifty_ema = nc if self.nifty_ema is None else self.nifty_ema * (1 - k) + nc * k
            if self.nifty_anchor is None:
                self.nifty_anchor = nc
            self.nifty_n += 1
            if self.nifty_n >= 5:
                ema_dir = "long" if nc > self.nifty_ema else "short" if nc < self.nifty_ema else None
        up = tot = 0
        for _k, of in of_rows.items():
            if of["segment"] == "INDEX":
                continue
            o, c = of.get("open"), of.get("close")
            if o is None or c is None:
                continue
            tot += 1
            if c > o:
                up += 1
        if tot:
            self.breadth = up / tot
            self.breadth_dir = "long" if self.breadth >= 0.5 else "short"
        # b3: EMA direction only when breadth AGREES, else no regime opinion
        self.regime_dir = ema_dir if (ema_dir and ema_dir == self.breadth_dir) else None

    # ================= L2: per-stock ATR% =================
    def _atr_state(self, s):
        st = self.atr.get(id(s))
        if st is None:
            st = self.atr[id(s)] = dict(prevc=None, trs=deque(maxlen=self.atr_win), atrpct=None)
        return st

    def _update_atr(self, s, of):
        st = self._atr_state(s)
        c, h, l = of["close"], of["high"], of["low"]
        if c is None or h is None or l is None:
            return st
        if st["prevc"] is not None:
            tr = max(h - l, abs(h - st["prevc"]), abs(l - st["prevc"]))
            st["trs"].append(tr)
        st["prevc"] = c
        if st["trs"] and c:
            st["atrpct"] = (sum(st["trs"]) / len(st["trs"])) / c
        return st

    # ================= signal = base trigger + L2 thrust + L1 flip =================
    def _signal(self, s, of, tick):
        st = self._update_atr(s, of)            # keep ATR live EVERY bar
        atrpct = st["atrpct"]
        thr_eff = max(fpe.THR, self.k * atrpct) if (self.use_atr and atrpct) else fpe.THR

        c = of["close"]
        if c is None or c < fpe.MIN_PRICE or s.prev_close is None or not s.vol:
            return None
        ret = c / s.prev_close - 1
        med = sorted(s.vol)[len(s.vol) // 2]
        vol = of["volume"] or 0
        spike = med > 0 and vol >= fpe.VMULT * med
        if not spike or abs(ret) < thr_eff:
            return None
        tbq, tsq = of["total_buy_qty"] or 0, of["total_sell_qty"] or 0
        imb = tbq / (tbq + tsq) if (tbq + tsq) else 0.5
        bd = sum(of.get(f"b{i}q") or 0 for i in range(1, 6))
        ad = sum(of.get(f"a{i}q") or 0 for i in range(1, 6))
        depth = bd / (bd + ad) if (bd + ad) else 0.5
        side = "long" if ret >= thr_eff else "short"
        book_ok = (imb >= fpe.IMB_HI or depth >= fpe.DEPTH_HI) if side == "long" \
            else (imb <= 1 - fpe.IMB_HI or depth <= 1 - fpe.DEPTH_HI)
        if not book_ok:
            return None
        atp = of.get("atp"); flipped = 0
        # enrichment (conviction recorded on the RAW side, as in both source levers)
        b1q, b1o, a1q, a1o = of.get("b1q"), of.get("b1o"), of.get("a1q"), of.get("a1o")
        avg_bid = (b1q / b1o) if (b1q and b1o) else None
        avg_ask = (a1q / a1o) if (a1q and a1o) else None
        whale_side, ratio = None, None
        if avg_bid and avg_ask:
            if avg_bid >= fpe.WHALE_MULT * avg_ask:
                whale_side, ratio = "bid", avg_bid / avg_ask
            elif avg_ask >= fpe.WHALE_MULT * avg_bid:
                whale_side, ratio = "ask", avg_ask / avg_bid
        tbp = tick.get("buy_vol_pct") if tick else None
        block = tick.get("max_trade_qty") if tick else None
        conv = 1
        if whale_side == ("bid" if side == "long" else "ask"):
            conv += 1
        if tbp is not None and ((side == "long" and tbp >= fpe.TICK_HI) or (side == "short" and tbp <= 100 - fpe.TICK_HI)):
            conv += 1
        # ---- L1 DIRECTION FLIP: correct the side to the confirmed regime, keep the trade ----
        if self.use_dir and self.regime_dir and self.regime_dir != side:
            side = self.regime_dir; flipped = 1; self.n_flip += 1
        return dict(side=side, ret=ret, imb=imb, depth=depth, whale_side=whale_side,
                    ratio=ratio, tick_buy_pct=tbp, block=block, conv=conv, atp=atp, flipped=flipped)

    # ================= L2: freeze levels at entry (both market & pullback fills) =================
    def _set_pos(self, s, sym, key, minute, px, sig):
        st = self.atr.get(id(s)); atrpct = st["atrpct"] if st else None
        if self.use_atr and atrpct:
            hs = min(0.04, max(0.015, self.m * atrpct))
            arm = self.p * atrpct
            gb = self.q * atrpct
            floor = arm * (fpe.FLOOR / fpe.ARM)     # proportional floor (2/3 of arm)
        else:
            hs, arm, gb, floor = fpe.HARD_STOP, fpe.ARM, fpe.GIVEBACK, fpe.FLOOR
        self.entry_atrs.append(atrpct)
        s.pos = dict(symbol=sym, key=key, seg=s.seg, side=sig["side"], entry_min=minute,
                     entry_px=px, qty=int(fpe.CAP // px), peak_fav=0.0, max_adv=0.0, cur_px=px,
                     sig=sig, hs=hs, arm=arm, gb=gb, floor=floor, atrpct=atrpct)

    def _open(self, s, sym, key, of, minute, sig):
        px = of["close"]
        if px is None or int(fpe.CAP // px) < 1:
            return
        self._set_pos(s, sym, key, minute, px, sig)

    def _open_at(self, s, sym, key, minute, entry_px, sig):
        if entry_px is None or entry_px < fpe.MIN_PRICE or int(fpe.CAP // entry_px) < 1:
            return
        self._set_pos(s, sym, key, minute, entry_px, sig)

    # ================= L2: manage using per-position stored levels =================
    def _manage(self, s, of, minute):
        p = s.pos; hi, lo, o = of["high"], of["low"], of["open"]
        entry, peak, side = p["entry_px"], p["peak_fav"], p["side"]
        hs, arm, fl, gb = p["hs"], p["arm"], p["floor"], p["gb"]
        armed = peak >= arm
        if side == "long":
            stop = entry * (1 + max(fl, peak - gb)) if armed else entry * (1 - hs)
            if o is not None and o <= stop:
                return self._close(s, o, minute, "gap_stop", armed)
            if lo is not None and lo <= stop:
                return self._close(s, stop, minute, "trail_stop" if armed else "hard_stop", armed)
        else:
            stop = entry * (1 - max(fl, peak - gb)) if armed else entry * (1 + hs)
            if o is not None and o >= stop:
                return self._close(s, o, minute, "gap_stop", armed)
            if hi is not None and hi >= stop:
                return self._close(s, stop, minute, "trail_stop" if armed else "hard_stop", armed)
        if side == "long":
            fav = (hi / entry - 1) if hi else 0; adv = (1 - lo / entry) if lo else 0
        else:
            fav = (1 - lo / entry) if lo else 0; adv = (hi / entry - 1) if hi else 0
        p["peak_fav"] = max(p["peak_fav"], fav); p["max_adv"] = max(p["max_adv"], adv)
        p["cur_px"] = of["close"]
        return None

    # ================= per-minute step: composes L1 (regime) + L2 (arm at EOD) + L3 (pending) =================
    def _target(self, side, of, key):
        c = of["close"]
        lows = [of["low"]] + [b[0] for b in self.hist.get(key, [])]
        highs = [of["high"]] + [b[1] for b in self.hist.get(key, [])]
        lows = [x for x in lows if x is not None]
        highs = [x for x in highs if x is not None]
        if side == "long":
            swing = min(lows) if lows else c
            return max(swing, c * (1 - TARGET_DIP))
        swing = max(highs) if highs else c
        return min(swing, c * (1 + TARGET_DIP))

    def on_minute(self, minute, of_rows, tick_rows):
        if self.use_dir:
            self._update_regime(of_rows)
            self.reg_minutes[self.regime_dir or "none"] += 1
        hhmm = minute[11:16]; is_eod = hhmm >= fpe.EOD_MIN
        opened = closed = 0
        for key, of in of_rows.items():
            sym, seg = of["symbol"], of["segment"]
            if seg == "INDEX" and key.startswith("NSE:"):
                continue
            s = self.syms.get(key)
            if s is None:
                s = self.syms[key] = fpe.Sym(seg)
            tick = tick_rows.get(key)
            sig = self._signal(s, of, tick) if of["close"] is not None else None
            if sig is not None:
                if sig["side"] == s.streak_dir:
                    s.streak_n += 1
                else:
                    s.streak_dir = sig["side"]; s.streak_n = 1
            else:
                s.streak_dir = None; s.streak_n = 0
            # manage existing position (EOD armed-flag uses per-position arm)
            if s.pos is not None:
                r = self._manage(s, of, minute)
                if r is None and is_eod:
                    r = self._close(s, of["close"], minute, "EOD", s.pos["peak_fav"] >= s.pos["arm"])
                if r is not None:
                    closed += 1
            # L3: resolve a pending smart-entry (pullback fill, else market after WAIT_BARS)
            if self.use_entry and s.pos is None and key in self.pending:
                pend = self.pending[key]; side = pend["side"]; tgt = pend["target"]
                o, hi, lo, c = of["open"], of["high"], of["low"], of["close"]
                fill_px = None
                if side == "long":
                    if lo is not None and lo <= tgt:
                        fill_px = o if (o is not None and o <= tgt) else tgt
                else:
                    if hi is not None and hi >= tgt:
                        fill_px = o if (o is not None and o >= tgt) else tgt
                if fill_px is not None:
                    self._open_at(s, sym, key, minute, fill_px, pend["sig"]); self.n_pull_fill += 1
                    del self.pending[key]
                else:
                    pend["left"] -= 1
                    if pend["left"] <= 0 or is_eod:
                        self._open_at(s, sym, key, minute, c, pend["sig"]); self.n_market_fill += 1
                        del self.pending[key]
                if s.pos is not None:
                    opened += 1
            # fresh entry (or register a pending smart-entry)
            if (s.pos is None and hhmm < fpe.ENTRY_CUT and self.n_min >= fpe.WARMUP
                    and sig is not None and s.streak_n >= self.consec
                    and (not self.use_entry or key not in self.pending)):
                if self.use_entry:
                    self.pending[key] = dict(side=sig["side"], sig=sig,
                                             target=self._target(sig["side"], of, key), left=WAIT_BARS)
                else:
                    self._open(s, sym, key, of, minute, sig); opened += 1
            # rolling state
            if of["close"] is not None:
                s.prev_close = of["close"]
            if of["volume"] is not None:
                s.vol.append(of["volume"])
            if self.use_entry:
                self.hist.setdefault(key, deque(maxlen=SWING_LOOK)).append((of["low"], of["high"]))
        self.n_min += 1
        return opened, closed


# ---------------------------------------------------------------------------
def build(minutes, date, **kw):
    eng = V2Engine(date, **kw)
    for m, of, tk in minutes:
        eng.on_minute(m, of, tk)
    return eng


def _row(lbl, tr):
    m = _metrics(tr)
    if not m.get("n"):
        return f"    {lbl:6s} n=0"
    return (f"    {lbl:6s} n={m['n']:>4d}  WR={m['wr']:>5.1f}%  avgRet={m['avg_ret']:>+7.3f}%  "
            f"MFE={m['avg_mfe']:>4.2f}%  MAE={m['avg_mae']:>4.2f}%  armed={m['armed_pct']:>4.1f}%  "
            f"PF={m['pf']:>5.2f}  net=Rs{m['net']:>12,.0f}")


def report(tag, eng):
    tr = eng.trades
    print(f"\n----- {tag}  (flips={eng.n_flip}  pull_fill={eng.n_pull_fill}  mkt_fill={eng.n_market_fill}) -----")
    print(_row("ALL", tr))
    print(_row("LONG", [t for t in tr if t["side"] == "long"]))
    print(_row("SHORT", [t for t in tr if t["side"] == "short"]))
    return _metrics(tr)


def main():
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-07-08"
    con = _con(ro=True); minutes = _load_day(con, date); con.close()
    print(f"[v2] STACKED experiment -- {date} | {len(minutes)} minutes")
    print(f"[v2] L1 regime=NIFTY EMA{EMA_WIN}&breadth  |  L2 ATR S1(k{ATR_K} m{ATR_M} p{ATR_P} q{ATR_Q})  |  "
          f"L3 smart-entry(wait{WAIT_BARS} dip{TARGET_DIP:.2%})")

    variants = [
        ("baseline (no levers)",        dict(use_dir=False, use_atr=False, use_entry=False)),
        ("L1 direction ALONE",          dict(use_dir=True,  use_atr=False, use_entry=False)),
        ("L2 ATR ALONE",                dict(use_dir=False, use_atr=True,  use_entry=False)),
        ("L3 entry ALONE",              dict(use_dir=False, use_atr=False, use_entry=True)),
        ("STACKED v2 (all three)",      dict(use_dir=True,  use_atr=True,  use_entry=True)),
        ("v2 MINUS direction",          dict(use_dir=False, use_atr=True,  use_entry=True)),
        ("v2 MINUS ATR",                dict(use_dir=True,  use_atr=False, use_entry=True)),
        ("v2 MINUS entry",              dict(use_dir=True,  use_atr=True,  use_entry=False)),
    ]
    engs = {}
    for tag, kw in variants:
        eng = build(minutes, date, **kw)
        engs[tag] = eng
        report(tag, eng)

    # regime read across the session (context for the honest verdict)
    rm = engs["STACKED v2 (all three)"].reg_minutes
    tot = sum(rm.values()) or 1
    print(f"\n[v2] regime minutes:  long={rm['long']} ({rm['long']/tot:.0%})  "
          f"short={rm['short']} ({rm['short']/tot:.0%})  none/undecided={rm['none']} ({rm['none']/tot:.0%})")

    # compact comparison table (ALL trades)
    base = _metrics(engs["baseline (no levers)"].trades)
    print("\n\n================ COMPARISON TABLE (ALL trades vs baseline) ================")
    hdr = f"{'variant':28s} {'n':>4s} {'dN':>4s} {'WR%':>6s} {'dWR':>6s} {'armed%':>7s} {'PF':>5s} {'net Rs':>13s} {'dnet Rs':>13s} {'flips':>6s}"
    print(hdr); print("-" * len(hdr))
    for tag, _ in variants:
        eng = engs[tag]; m = _metrics(eng.trades)
        dn = m["n"] - base["n"]; dwr = m["wr"] - base["wr"]; dnet = m["net"] - base["net"]
        keep = "" if m["n"] >= base["n"] - 5 else "  <-count cut"
        print(f"{tag:28s} {m['n']:>4d} {dn:>+4d} {m['wr']:>6.1f} {dwr:>+6.1f} {m['armed_pct']:>7.1f} "
              f"{m['pf']:>5.2f} {m['net']:>13,.0f} {dnet:>+13,.0f} {eng.n_flip:>6d}{keep}")

    # short-side detail for the stack vs baseline (the ceiling question)
    print("\n----- SHORT-side detail (stack pushes toward the short ceiling) -----")
    for tag in ("baseline (no levers)", "L1 direction ALONE", "STACKED v2 (all three)"):
        sh = [t for t in engs[tag].trades if t["side"] == "short"]
        m = _metrics(sh)
        print(f"  {tag:28s} SHORT n={m.get('n',0):>4d}  WR={m.get('wr',0):>5.1f}%  net=Rs{m.get('net',0):>12,.0f}")


if __name__ == "__main__":
    main()
