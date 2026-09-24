"""EXPERIMENT — ATR NORMALIZATION lever for flow_paper_engine.

Fixed 0.15% thrust and flat -3%/+3%/1.25% trail are noise in a high-beta stock and
huge in a sleepy one. This lever computes a per-stock 1-min ATR% (rolling true range /
close over ~14 bars) and SCALES the entry thrust + the stop/arm/giveback per stock:

  entry thrust  : max(THR, k * ATR%)              (a lift must clear the stock's own noise)
  hard-stop     : clamp(m * ATR%, 1.5%, 4%)       (per-stock, replaces flat 3%)
  arm level     : p * ATR%                         (replaces flat 3%)
  giveback      : q * ATR%                         (replaces flat 1.25%)
  floor         : arm * (FLOOR/ARM)                (kept PROPORTIONAL to arm = 2/3 of arm,
                                                    so a scaled arm below 2% can't lock a
                                                    floor above the peak -> phantom fills)

ATR% here is the mean 1-min TRUE RANGE as a fraction of price, i.e. per-BAR volatility.
So the multipliers are large: with ATR%~0.3%, m=10 -> 3% stop (matches the flat default
for an average-vol stock), tighter for calm names, wider for wild ones. That is the point.

Nothing in flow_paper_engine.py is edited — we subclass Engine and override _signal,
_open, _manage, on_minute (only the EOD armed-flag line changes there).

Run:  python exp_atr_norm.py            # 2026-07-08, baseline + settings grid
"""
import sys, argparse
from collections import deque, Counter

ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, ROOT + r"\scripts")
sys.path.insert(0, ROOT + r"\universe_engine")

import flow_paper_engine as fpe
from flow_paper_engine import Engine, _load_day, _metrics, _con


class ATREngine(Engine):
    """Engine with per-stock ATR%-scaled entry thrust and stop/arm/giveback."""

    def __init__(self, trade_date, k, m, p, q, atr_win=14, consec=2):
        super().__init__(trade_date, verbose=False, consec=consec)
        self.k, self.m, self.p, self.q = k, m, p, q
        self.atr_win = atr_win
        self.atr = {}          # id(Sym) -> dict(prevc, trs deque, atrpct)
        self.entry_atrs = []   # ATR% observed at each entry (diagnostic)

    # ---- per-stock ATR% state (updated EVERY bar, before any early return) ----
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

    # ---- signal: mirror base, ATR-scaled thrust gate ----
    def _signal(self, s, of, tick):
        st = self._update_atr(s, of)          # keep ATR live every bar
        atrpct = st["atrpct"]
        thr_eff = max(fpe.THR, self.k * atrpct) if atrpct else fpe.THR

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
        if self.flip_atp and atp and c:
            if side == "long" and c < atp:
                side = "short"; flipped = 1
            elif side == "short" and c > atp:
                side = "long"; flipped = 1
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
        return dict(side=side, ret=ret, imb=imb, depth=depth, whale_side=whale_side,
                    ratio=ratio, tick_buy_pct=tbp, block=block, conv=conv, atp=atp, flipped=flipped)

    # ---- open: freeze ATR-scaled levels at entry ----
    def _open(self, s, sym, key, of, minute, sig):
        px = of["close"]; qty = int(fpe.CAP // px)
        if qty < 1:
            return
        st = self.atr.get(id(s)); atrpct = st["atrpct"] if st else None
        if atrpct:
            hs = min(0.04, max(0.015, self.m * atrpct))
            arm = self.p * atrpct
            gb = self.q * atrpct
        else:
            hs, arm, gb = fpe.HARD_STOP, fpe.ARM, fpe.GIVEBACK
        floor = arm * (fpe.FLOOR / fpe.ARM)   # proportional floor (2/3 of arm)
        self.entry_atrs.append(atrpct)
        s.pos = dict(symbol=sym, key=key, seg=s.seg, side=sig["side"], entry_min=minute,
                     entry_px=px, qty=qty, peak_fav=0.0, max_adv=0.0, cur_px=px, sig=sig,
                     hs=hs, arm=arm, gb=gb, floor=floor, atrpct=atrpct)

    # ---- manage: mirror base, per-stock stored levels ----
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

    # ---- per-minute step: mirror base; only EOD armed-flag uses per-stock arm ----
    def on_minute(self, minute, of_rows, tick_rows):
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
            if s.pos is not None:
                r = self._manage(s, of, minute)
                if r is None and is_eod:
                    r = self._close(s, of["close"], minute, "EOD", s.pos["peak_fav"] >= s.pos["arm"])
                if r is not None:
                    closed += 1
            if (s.pos is None and hhmm < fpe.ENTRY_CUT and self.n_min >= fpe.WARMUP
                    and sig is not None and s.streak_n >= self.consec):
                self._open(s, sym, key, of, minute, sig); opened += 1
            if of["close"] is not None:
                s.prev_close = of["close"]
            if of["volume"] is not None:
                s.vol.append(of["volume"])
        self.n_min += 1
        return opened, closed


# ---------------------------------------------------------------------------
def run_variant(minutes, engine):
    for m, of, tk in minutes:
        engine.on_minute(m, of, tk)
    return engine.trades


def pctl(vals, q):
    vals = sorted(v for v in vals if v is not None)
    if not vals:
        return None
    i = min(len(vals) - 1, int(q * len(vals)))
    return vals[i]


def fmt_metrics(lbl, m):
    if not m.get("n"):
        return f"  {lbl:8s}  n=0"
    return (f"  {lbl:8s}  n={m['n']:>4d}  WR={m['wr']:>5.1f}%  avgRet={m['avg_ret']:>+7.3f}%  "
            f"MFE={m['avg_mfe']:>4.2f}%  MAE={m['avg_mae']:>4.2f}%  PF={m['pf']:>5.2f}  "
            f"armed={m['armed_pct']:>4.1f}%  EOD={m['eod_pct']:>4.1f}%  net=Rs{m['net']:>11,.0f}")


def exit_mix(trades):
    c = Counter(t["exit_reason"] for t in trades)
    tot = len(trades) or 1
    order = ["EOD", "hard_stop", "trail_stop", "gap_stop"]
    parts = [f"{k}={c.get(k,0)} ({c.get(k,0)/tot*100:.0f}%)" for k in order if c.get(k, 0)]
    for k in c:
        if k not in order:
            parts.append(f"{k}={c[k]} ({c[k]/tot*100:.0f}%)")
    return "  ".join(parts)


def report_variant(title, trades, extra_atrs=None):
    print(f"\n===== {title} =====", flush=True)
    print(fmt_metrics("ALL", _metrics(trades)))
    print(fmt_metrics("LONG", _metrics([t for t in trades if t["side"] == "long"])))
    print(fmt_metrics("SHORT", _metrics([t for t in trades if t["side"] == "short"])))
    print("  exits :", exit_mix(trades))
    if extra_atrs:
        a = [x for x in extra_atrs if x is not None]
        if a:
            print(f"  ATR%@entry: n={len(a)}  p10={pctl(a,.1)*100:.3f}%  med={pctl(a,.5)*100:.3f}%  "
                  f"p90={pctl(a,.9)*100:.3f}%")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="2026-07-08")
    ap.add_argument("--consec", type=int, default=2)
    a = ap.parse_args()
    date = a.date

    con = _con(ro=True); minutes = _load_day(con, date); con.close()
    print(f"[exp-atr] {date}  consec={a.consec}  bars={len(minutes)}", flush=True)
    print(f"[exp-atr] base trail: hardstop -{fpe.HARD_STOP:.0%} arm +{fpe.ARM:.0%} "
          f"floor +{fpe.FLOOR:.0%} give {fpe.GIVEBACK:.2%} thr {fpe.THR:.2%}", flush=True)

    # ---- baseline (unmodified Engine) ----
    base = Engine(date, verbose=False, consec=a.consec)
    base_tr = run_variant(minutes, base)
    report_variant("BASELINE (flat trail)", base_tr)

    # ---- ATR-scaled settings grid ----
    # (k, m, p, q): thrust=max(THR,k*ATR%), stop=clamp(m*ATR%,1.5%,4%), arm=p*ATR%, give=q*ATR%
    settings = [
        ("S1 balanced",  dict(k=1.0,  m=10, p=10, q=4.0)),   # ~reproduces flat for avg-vol
        ("S2 easy-arm",  dict(k=0.75, m=8,  p=8,  q=3.5)),   # tighter stop+arm -> more trailing
        ("S3 strict-in", dict(k=1.25, m=12, p=9,  q=4.0)),   # stricter entry, wider stop
        ("S4 loose-in",  dict(k=0.5,  m=8,  p=7,  q=3.0)),   # keep count high, easy arm
        ("S5 tight-arm", dict(k=1.0,  m=9,  p=6,  q=2.5)),   # very easy arm, short give
    ]
    results = {"BASELINE": base_tr}
    for name, cfg in settings:
        eng = ATREngine(date, consec=a.consec, **cfg)
        tr = run_variant(minutes, eng)
        results[name] = tr
        report_variant(f"{name}  (k={cfg['k']} m={cfg['m']} p={cfg['p']} q={cfg['q']})",
                       tr, extra_atrs=eng.entry_atrs)

    # ---- honesty control: FLAT small levels (no ATR scaling) ----------------
    # Isolates how much of the gain is per-stock ATR ADAPTIVITY vs simply using a
    # smaller arm/stop than the flat 3%. Uses the S1 median-equivalent absolutes.
    _sv = (fpe.HARD_STOP, fpe.ARM, fpe.FLOOR, fpe.GIVEBACK)
    fpe.HARD_STOP, fpe.ARM, fpe.FLOOR, fpe.GIVEBACK = 0.015, 0.013, 0.0087, 0.0052
    fs = Engine(date, verbose=False, consec=a.consec)
    fs_tr = run_variant(minutes, fs)
    fpe.HARD_STOP, fpe.ARM, fpe.FLOOR, fpe.GIVEBACK = _sv   # restore
    results["FLAT-small"] = fs_tr
    report_variant("FLAT-small control (arm1.3/stop1.5/floor0.87/give0.52, NO ATR)", fs_tr)

    # ---- compact comparison table vs baseline ----
    print("\n\n===== COMPARISON (ALL trades) vs BASELINE =====", flush=True)
    bm = _metrics(base_tr)
    hdr = f"  {'variant':14s} {'n':>4s} {'dN':>5s} {'WR%':>6s} {'dWR':>6s} {'PF':>5s} {'armed%':>7s} {'EOD%':>6s} {'net Rs':>12s}"
    print(hdr); print("  " + "-" * (len(hdr) - 2))
    for name in results:
        m = _metrics(results[name])
        if not m.get("n"):
            continue
        dn = m["n"] - bm["n"]; dwr = m["wr"] - bm["wr"]
        print(f"  {name:14s} {m['n']:>4d} {dn:>+5d} {m['wr']:>6.1f} {dwr:>+6.1f} "
              f"{m['pf']:>5.2f} {m['armed_pct']:>7.1f} {m['eod_pct']:>6.1f} {m['net']:>12,.0f}")


if __name__ == "__main__":
    main()
