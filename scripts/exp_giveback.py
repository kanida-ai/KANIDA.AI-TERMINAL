"""A/B the give-back knob: current tight flat 0.6% vs flat-looser vs ATR-scaled per-stock.
Only the give-back changes; hard-stop/arm/floor stay at the live v1 config. Both days.

give-back governs the trail AFTER arming: stop = entry*(1 +/- max(FLOOR, peak - giveback)).
Tighter give-back books earlier (clips winners); looser lets them run but returns more on reversal.
ATR-scaled: giveback = q * ATR%(at entry), clamped -> high-vol names get a wider trail.

No invented numbers — every row simulated from the 1-min data.
"""
import sys
from collections import deque
from pathlib import Path

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
sys.path.insert(0, str(ROOT / "scripts"))
import flow_paper_engine as F
from flow_paper_engine import Engine, HARD_STOP, ARM, FLOOR, CAP

DAYS = ["2026-07-08", "2026-07-09"]


class GBEngine(Engine):
    def __init__(self, *a, gb_mode="flat", gb_flat=0.006, gb_q=5.0, gb_clamp=(0.004, 0.03), atr_win=14, **kw):
        super().__init__(*a, **kw)
        self.gb_mode = gb_mode; self.gb_flat = gb_flat; self.gb_q = gb_q
        self.gb_clamp = gb_clamp; self.atr_win = atr_win
        self.atr = {}; self._pc = {}

    def _atr_pct(self, key):
        d = self.atr.get(key)
        return (sum(d) / len(d)) if d else None

    def on_minute(self, minute, of_rows, tick_rows):
        r = super().on_minute(minute, of_rows, tick_rows)      # entry uses ATR through the PRIOR bar
        for key, of in of_rows.items():                        # then fold this bar into ATR
            h, l, c = of.get("high"), of.get("low"), of.get("close")
            if h is None or l is None or c is None:
                continue
            pc = self._pc.get(key)
            tr = (h - l) if pc is None else max(h - l, abs(h - pc), abs(l - pc))
            self.atr.setdefault(key, deque(maxlen=self.atr_win)).append(tr / c if c else 0)
            self._pc[key] = c
        return r

    def _open(self, s, sym, key, of, minute, sig):
        super()._open(s, sym, key, of, minute, sig)
        if s.pos is not None:
            if self.gb_mode == "atr":
                a = self._atr_pct(key)
                gb = self.gb_q * a if a else self.gb_flat
                lo, hi = self.gb_clamp; gb = min(max(gb, lo), hi)
            else:
                gb = self.gb_flat
            s.pos["giveback"] = gb

    def _manage(self, s, of, minute):
        p = s.pos; gb = p.get("giveback", self.gb_flat)
        hi, lo, o = of["high"], of["low"], of["open"]
        entry, peak, side = p["entry_px"], p["peak_fav"], p["side"]
        armed = peak >= ARM
        if side == "long":
            stop = entry * (1 + max(FLOOR, peak - gb)) if armed else entry * (1 - HARD_STOP)
            if o is not None and o <= stop:
                return self._close(s, o, minute, "gap_stop", armed)
            if lo is not None and lo <= stop:
                return self._close(s, stop, minute, "trail_stop" if armed else "hard_stop", armed)
        else:
            stop = entry * (1 - max(FLOOR, peak - gb)) if armed else entry * (1 + HARD_STOP)
            if o is not None and o >= stop:
                return self._close(s, o, minute, "gap_stop", armed)
            if hi is not None and hi >= stop:
                return self._close(s, stop, minute, "trail_stop" if armed else "hard_stop", armed)
        if side == "long":
            fav = (hi / entry - 1) if hi else 0; adv = (1 - lo / entry) if lo else 0
        else:
            fav = (1 - lo / entry) if lo else 0; adv = (hi / entry - 1) if hi else 0
        p["peak_fav"] = max(p["peak_fav"], fav); p["max_adv"] = max(p["max_adv"], adv); p["cur_px"] = of["close"]
        return None


def run_variant(days_data, **cfg):
    trades = []
    for d, mins in days_data:
        e = GBEngine(d, verbose=False, consec=2, variant="gb", **cfg)
        for m, of, tk in mins:
            e.on_minute(m, of, tk)
        for t in e.trades:
            t = dict(t); t["day"] = d; trades.append(t)
    return trades


def summ(trades):
    from collections import Counter
    if not trades:
        return "n=0"
    n = len(trades); w = sum(1 for t in trades if t["pnl_rs"] > 0)
    net = sum(t["net_pnl_rs"] for t in trades); gross = sum(t["pnl_rs"] for t in trades)
    avgret = sum(t["pnl_pct"] for t in trades) / n
    rc = Counter(t["exit_reason"] for t in trades)
    tr = rc.get("trail_stop", 0) / n * 100; eod = rc.get("EOD", 0) / n * 100
    return (f"n={n:4d} WR={w/n*100:4.1f}% avgRet={avgret:+.3f}% gross=Rs{gross:>10,.0f} "
            f"net=Rs{net:>10,.0f} trail={tr:3.0f}% EOD={eod:3.0f}%")


def main():
    con = F._con(ro=True)
    days_data = [(d, F._load_day(con, d)) for d in DAYS]
    con.close()
    variants = [
        ("FLAT 0.6% (current)", dict(gb_mode="flat", gb_flat=0.006)),
        ("FLAT 1.0%", dict(gb_mode="flat", gb_flat=0.010)),
        ("FLAT 1.5%", dict(gb_mode="flat", gb_flat=0.015)),
        ("FLAT 2.0%", dict(gb_mode="flat", gb_flat=0.020)),
        ("ATR x5  (~0.6% median)", dict(gb_mode="atr", gb_q=5.0)),
        ("ATR x8  (~1.0% median)", dict(gb_mode="atr", gb_q=8.0)),
        ("ATR x12 (~1.5% median)", dict(gb_mode="atr", gb_q=12.0)),
    ]
    print(f"give-back A/B | days={DAYS} | consec=2, hard-2.5/arm+1.3/floor+0.9 fixed\n")
    for lbl, cfg in variants:
        tr = run_variant(days_data, **cfg)
        d1 = [t for t in tr if t["day"] == DAYS[0]]; d2 = [t for t in tr if t["day"] == DAYS[1]]
        print(f"{lbl:24s} POOLED {summ(tr)}")
        print(f"{'':24s}  07-08 {summ(d1)}")
        print(f"{'':24s}  07-09 {summ(d2)}")
        print()


if __name__ == "__main__":
    main()
