"""EXPERIMENT: Smart-zone entry + exit upgrades on the flow paper engine.

Lever = better FILLS + banking winners + scratching losers, WITHOUT cutting count.

Two independent parts (each toggleable; tested alone + combined):
  1. SMART-ZONE ENTRY  (smart_entry=True)
       On a fresh signal the base engine enters at the 2nd-consec-lift bar CLOSE
       (often the local extreme = chasing). Instead we register a PENDING entry and
       wait up to WAIT_BARS(2) bars for a shallow pullback toward a micro-support:
         long  -> target = max(recent 4-bar swing low , close*(1-TARGET_DIP))
         short -> target = min(recent 4-bar swing high, close*(1+TARGET_DIP))
       If the bar trades to the target within the window -> fill there (better price).
       If no pullback after WAIT_BARS -> MARKET fill at that bar's close (count preserved).
  2. EXIT UPGRADES      (scratch=True / partial=True), override of _manage/_close:
       (a) SCRATCH  : pre-arm, if red beyond SCRATCH_THR(0.6%) AND the bar continues
                      against us (lower-low for a long / higher-high for a short) and the
                      hard-stop was NOT already hit this bar -> exit at the bar close near
                      the small loss instead of riding to -3%.
       (b) PARTIAL  : bank HALF at +PARTIAL_ARM(1.5%) favourable, trail the remainder.
                      Banked P&L is folded back into the SAME trade so the count is
                      unchanged (count-preserving by construction).

A/B/C/D on the reference day: baseline / entry-only / exit-only / both.
Prints _metrics for ALL / long / short (+avg_mae) and the exit-reason mix per variant.

Run:  C:/Users/SPS/anaconda3/python.exe exp_smartzone_exits.py [YYYY-MM-DD]
"""
import sys
from collections import deque, Counter

from flow_paper_engine import (Engine, _load_day, _metrics, _con, cost,
                               CAP, ARM, HARD_STOP, FLOOR, GIVEBACK,
                               ENTRY_CUT, EOD_MIN, WARMUP, MIN_PRICE)

# ---- lever knobs -------------------------------------------------------------
WAIT_BARS   = 2       # bars to wait for a pullback before market-filling
TARGET_DIP  = 0.003   # shallow pullback depth cap (0.30%) below/above signal close
SWING_LOOK  = 4       # bars of micro swing high/low to anchor the pullback target
SCRATCH_THR = 0.006   # 0.60% adverse -> eligible to scratch out
PARTIAL_ARM = 0.015   # +1.50% favourable -> bank half


class SmartEngine(Engine):
    """Subclass: pending-entry state machine + scratch/partial exit upgrades.

    Toggles: smart_entry, scratch, partial. With all False it is byte-identical to Engine.
    """

    def __init__(self, trade_date, verbose=False, consec=2,
                 smart_entry=False, scratch=False, partial=False):
        super().__init__(trade_date, verbose=verbose, consec=consec)
        self.smart_entry = smart_entry
        self.scratch = scratch
        self.partial = partial
        self.pending = {}                 # key -> pending-entry dict
        self.hist = {}                    # key -> deque[(low,high)] recent bars
        self.n_market_fill = 0            # smart-entry filled at market (no pullback)
        self.n_pull_fill = 0             # smart-entry filled on a pullback

    # ---------- entry (base close-fill, but seed the exit-upgrade bookkeeping) ----------
    def _open(self, s, sym, key, of, minute, sig):
        super()._open(s, sym, key, of, minute, sig)
        if s.pos is not None:
            s.pos.update(prev_low=of["low"], prev_high=of["high"],
                         partialed=False, banked_rs=0.0, banked_cost=0.0, half_qty=0)

    # ---------- entry helper (explicit price / minute) ----------
    def _open_at(self, s, sym, key, of, minute, entry_px, sig):
        if entry_px is None or entry_px < MIN_PRICE:
            return
        qty = int(CAP // entry_px)
        if qty < 1:
            return
        s.pos = dict(symbol=sym, key=key, seg=s.seg, side=sig["side"], entry_min=minute,
                     entry_px=entry_px, qty=qty, peak_fav=0.0, max_adv=0.0, cur_px=entry_px,
                     sig=sig, prev_low=of["low"], prev_high=of["high"],
                     partialed=False, banked_rs=0.0, banked_cost=0.0, half_qty=0)

    def _target(self, side, of, key):
        """Shallow-pullback support/resistance target for a fresh signal on this bar."""
        c = of["close"]
        lows = [of["low"]] + [b[0] for b in self.hist.get(key, [])]
        highs = [of["high"]] + [b[1] for b in self.hist.get(key, [])]
        lows = [x for x in lows if x is not None]
        highs = [x for x in highs if x is not None]
        if side == "long":
            swing = min(lows) if lows else c
            return max(swing, c * (1 - TARGET_DIP))       # shallow dip, not below swing
        swing = max(highs) if highs else c
        return min(swing, c * (1 + TARGET_DIP))

    # ---------- exit upgrades ----------
    def _manage(self, s, of, minute):
        p = s.pos
        hi, lo, o, c = of["high"], of["low"], of["open"], of["close"]
        entry, peak, side = p["entry_px"], p["peak_fav"], p["side"]
        armed = peak >= ARM

        # (b) PARTIAL: bank half the first time we tag +PARTIAL_ARM favourable (pre-final)
        if self.partial and not p["partialed"] and p["qty"] >= 2:
            if side == "long" and hi is not None and hi >= entry * (1 + PARTIAL_ARM):
                bpx = entry * (1 + PARTIAL_ARM); half = p["qty"] // 2
                p["banked_rs"] += half * (bpx - entry)
                p["banked_cost"] += cost(s.seg, entry, bpx, half)
                p["half_qty"] = half; p["qty"] -= half; p["partialed"] = True
            elif side == "short" and lo is not None and lo <= entry * (1 - PARTIAL_ARM):
                bpx = entry * (1 - PARTIAL_ARM); half = p["qty"] // 2
                p["banked_rs"] += half * (entry - bpx)
                p["banked_cost"] += cost(s.seg, entry, bpx, half)
                p["half_qty"] = half; p["qty"] -= half; p["partialed"] = True

        # (a) SCRATCH: pre-arm, red beyond threshold, bar continues against, hard-stop NOT hit
        if self.scratch and not armed:
            if side == "long":
                hs = entry * (1 - HARD_STOP)
                cur = (c / entry - 1) if c else 0
                lower_low = (p["prev_low"] is not None and lo is not None and lo < p["prev_low"])
                if cur <= -SCRATCH_THR and lower_low and (lo is None or lo > hs) and (o is None or o > hs):
                    p["prev_low"], p["prev_high"] = lo, hi
                    return self._close(s, c, minute, "scratch", False)
            else:
                hs = entry * (1 + HARD_STOP)
                cur = (entry / c - 1) if c else 0
                higher_high = (p["prev_high"] is not None and hi is not None and hi > p["prev_high"])
                if cur <= -SCRATCH_THR and higher_high and (hi is None or hi < hs) and (o is None or o < hs):
                    p["prev_low"], p["prev_high"] = lo, hi
                    return self._close(s, c, minute, "scratch", False)

        r = super()._manage(s, of, minute)
        if s.pos is not None:                 # still open -> roll prev-bar extremes
            p["prev_low"], p["prev_high"] = lo, hi
        return r

    def _close(self, s, exit_px, minute, reason, armed):
        p = s.pos
        banked_rs, banked_cost, half = p.get("banked_rs", 0.0), p.get("banked_cost", 0.0), p.get("half_qty", 0)
        entry = p["entry_px"]
        t = super()._close(s, exit_px, minute, reason, armed)   # remainder-qty trade
        if banked_rs:                                            # fold booked half back in (count stays 1)
            rem_qty = t["qty"]
            t["pnl_rs"] = round(t["pnl_rs"] + banked_rs, 0)
            t["net_pnl_rs"] = round(t["net_pnl_rs"] + banked_rs - banked_cost, 0)
            orig_qty = rem_qty + half
            t["pnl_pct"] = round(t["pnl_rs"] / (entry * orig_qty) * 100, 3)
            t["qty"] = orig_qty
            t["exit_reason"] = "partial+" + reason
        return t

    # ---------- per-minute step (base copy + pending-entry machine) ----------
    def on_minute(self, minute, of_rows, tick_rows):
        hhmm = minute[11:16]; is_eod = hhmm >= EOD_MIN
        opened = closed = 0
        for key, of in of_rows.items():
            sym, seg = of["symbol"], of["segment"]
            if seg == "INDEX" and key.startswith("NSE:"):
                continue
            s = self.syms.get(key)
            if s is None:
                s = self.syms[key] = SymState(seg)
            tick = tick_rows.get(key)
            sig = self._signal(s, of, tick) if of["close"] is not None else None
            if sig is not None:
                if sig["side"] == s.streak_dir:
                    s.streak_n += 1
                else:
                    s.streak_dir = sig["side"]; s.streak_n = 1
            else:
                s.streak_dir = None; s.streak_n = 0

            # 2) manage existing position
            if s.pos is not None:
                r = self._manage(s, of, minute)
                if r is None and is_eod:
                    r = self._close(s, of["close"], minute, "EOD", s.pos["peak_fav"] >= ARM)
                if r is not None:
                    closed += 1

            # 2b) resolve a PENDING smart-entry (fill on pullback, else market after WAIT_BARS)
            if self.smart_entry and s.pos is None and key in self.pending:
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
                    self._open_at(s, sym, key, of, minute, fill_px, pend["sig"]); self.n_pull_fill += 1
                    del self.pending[key]
                else:
                    pend["left"] -= 1
                    if pend["left"] <= 0 or is_eod:
                        self._open_at(s, sym, key, of, minute, c, pend["sig"]); self.n_market_fill += 1
                        del self.pending[key]
                if s.pos is not None:
                    opened += 1

            # 3) fresh entry (or register a pending smart-entry)
            if (s.pos is None and hhmm < ENTRY_CUT and self.n_min >= WARMUP
                    and sig is not None and s.streak_n >= self.consec and key not in self.pending):
                if self.smart_entry:
                    self.pending[key] = dict(side=sig["side"], sig=sig,
                                             target=self._target(sig["side"], of, key), left=WAIT_BARS)
                else:
                    self._open(s, sym, key, of, minute, sig); opened += 1

            # 4) rolling state
            if of["close"] is not None:
                s.prev_close = of["close"]
            if of["volume"] is not None:
                s.vol.append(of["volume"])
            self.hist.setdefault(key, deque(maxlen=SWING_LOOK)).append((of["low"], of["high"]))
        self.n_min += 1
        return opened, closed


class SymState:
    """Identical to flow_paper_engine.Sym (seg set at construction from the row)."""
    __slots__ = ("prev_close", "vol", "seg", "pos", "streak_dir", "streak_n")
    def __init__(self, seg):
        from flow_paper_engine import WIN
        self.prev_close = None; self.vol = deque(maxlen=WIN); self.seg = seg
        self.pos = None; self.streak_dir = None; self.streak_n = 0


def run(date):
    con = _con(ro=True); minutes = _load_day(con, date); con.close()

    def build(**kw):
        eng = SmartEngine(date, **kw)
        for m, of, tk in minutes:
            eng.on_minute(m, of, tk)
        return eng

    variants = {
        "BASELINE":     dict(smart_entry=False, scratch=False, partial=False),
        "ENTRY-only":   dict(smart_entry=True,  scratch=False, partial=False),
        "EXIT-only":    dict(smart_entry=False, scratch=True,  partial=False),
        "BOTH":         dict(smart_entry=True,  scratch=True,  partial=False),
        "BOTH+partial": dict(smart_entry=True,  scratch=True,  partial=True),
    }
    engs = {name: build(**kw) for name, kw in variants.items()}

    def row(lbl, tr):
        m = _metrics(tr)
        if not m.get("n"):
            return f"    {lbl:6s} n=0"
        losers = [t for t in tr if t["pnl_rs"] <= 0]
        avg_loss = (sum(t["pnl_pct"] for t in losers) / len(losers)) if losers else 0.0
        return (f"    {lbl:6s} n={m['n']:>4d}  WR={m['wr']:>5.1f}%  avgRet={m['avg_ret']:>+7.3f}%  "
                f"avgLoss={avg_loss:>+7.3f}%  MAE={m['avg_mae']:>5.3f}%  PF={m['pf']:>5.2f}  "
                f"net=Rs{m['net']:>12,.0f}")

    base_m = _metrics(engs["BASELINE"].trades)
    print(f"\n================ SMART-ZONE / EXIT-UPGRADE A/B  |  {date} ================")
    for name, eng in engs.items():
        tr = eng.trades
        m = _metrics(tr)
        d_n = m["n"] - base_m["n"]; d_wr = m["wr"] - base_m["wr"]
        d_net = m["net"] - base_m["net"]
        print(f"\n----- {name}  (dN={d_n:+d}  dWR={d_wr:+.1f}pp  dNet=Rs{d_net:+,.0f}) -----")
        print(row("ALL", tr))
        print(row("LONG", [t for t in tr if t["side"] == "long"]))
        print(row("SHORT", [t for t in tr if t["side"] == "short"]))
        rc = Counter(t["exit_reason"] for t in tr)
        print("    exits:", dict(rc))
        if eng.smart_entry:
            print(f"    smart-entry fills: pullback={eng.n_pull_fill}  market={eng.n_market_fill}")

    # avg realised entry improvement (entry vs baseline same signal-set is hard to pair; report gross)
    print("\n(baseline n reproduced exactly = calibration OK)" if base_m["n"] == 392
          else f"\n(baseline n={base_m['n']})")


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "2026-07-08")
