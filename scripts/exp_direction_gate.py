"""EXPERIMENT — Lever: DIRECTION / REGIME GATE ("both sides" brain).

Baseline flow engine takes the trend-following side of every order-flow lift
(buy-lift -> long, sell-lift -> short). On a DOWN/reversal day the longs are
mostly wrong-side (13% WR) while shorts win (77% WR). This lever consults a
TREND/REGIME context and, when the raw lift side disagrees, FLIPS the side to
the trend side (keeps the trade -> count preserved) instead of deleting it.

Two context signals, maintained live minute-by-minute:
  1. per-stock MICRO-TREND : rolling window of recent 1-min closes per instrument;
     linear-regression slope over the window (small dead-band to ignore noise).
  2. MARKET REGIME         : NIFTY 50 index vs its own EMA (primary, stable read)
     plus breadth = fraction of instruments with an up minute-candle (context).

Variants A/B'd vs baseline on the reference day:
  (a) micro_flip   : flip to the micro-trend side when it disagrees      (keeps count)
  (b) regime_flip  : flip to the market-regime (NIFTY) side              (keeps count)
  (c) both_flip    : flip only when micro-trend AND regime AGREE against  (keeps count)
  (d) micro_confirm/regime_confirm : DROP disagreements (cuts count — reference only)

Does NOT edit the engine. Subclasses Engine and overrides _signal / on_minute.
Trail config untouched. Usage:  python exp_direction_gate.py [YYYY-MM-DD]
"""
import sys
from collections import deque
from pathlib import Path

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "universe_engine"))

from flow_paper_engine import Engine, _load_day, _metrics, _con  # noqa: E402

NIFTY_KEY = "NSE:NIFTY 50"

# ---- gate tuning (context only — trigger/trail/consec all untouched) ----
MT_WIN  = 15      # micro-trend rolling window (minutes)
MT_MIN  = 8       # need >= this many closes before a micro-trend direction is valid
MT_BAND = 0.0005  # net move over the window must exceed 0.05% to call a direction
EMA_WIN = 15      # NIFTY EMA window for the regime read


class GatedEngine(Engine):
    """Engine + a direction/regime gate applied AFTER the raw signal is formed."""

    def __init__(self, trade_date, gate="base", consec=2, regime_mode="ema", **kw):
        super().__init__(trade_date, verbose=False, consec=consec, flip_atp=False, **kw)
        self.gate = gate
        self.regime_mode = regime_mode   # how regime_dir is derived: ema | anchor | ema_and_breadth
        self.hist = {}            # id(Sym) -> deque of recent closes (micro-trend)
        self.nifty_ema = None
        self.nifty_anchor = None  # NIFTY session-open reference (day anchor)
        self.nifty_n = 0
        self.regime_dir = None    # "long"/"short"/None for the CURRENT minute
        self.breadth = None
        self.breadth_dir = None
        self.n_flip = 0           # sides actually flipped
        self.n_dropped = 0        # signals dropped by a confirm-only gate
        self.reg_minutes = {"long": 0, "short": 0, "none": 0}

    # ---- market regime, recomputed once per minute BEFORE the per-symbol loop ----
    def on_minute(self, minute, of_rows, tick_rows):
        self._update_regime(of_rows)
        self.reg_minutes[self.regime_dir or "none"] += 1
        return super().on_minute(minute, of_rows, tick_rows)

    def _update_regime(self, of_rows):
        # NIFTY 50 vs its own EMA -> stable index direction
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
        # breadth = fraction of tradable instruments with an up minute-candle
        up = tot = 0
        for k2, of in of_rows.items():
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
        # anchor dir: NIFTY vs session open (stable, persistent on a trend day)
        anchor_dir = None
        if nc is not None and self.nifty_anchor:
            anchor_dir = "long" if nc > self.nifty_anchor else "short" if nc < self.nifty_anchor else None
        # select regime_dir per mode
        if self.regime_mode == "anchor":
            self.regime_dir = anchor_dir
        elif self.regime_mode == "ema_and_breadth":
            self.regime_dir = ema_dir if (ema_dir and ema_dir == self.breadth_dir) else None
        else:  # "ema"
            self.regime_dir = ema_dir

    # ---- micro-trend from this instrument's own rolling closes ----
    def _micro_dir(self, sid):
        h = self.hist.get(sid)
        if not h or len(h) < MT_MIN:
            return None
        xs = list(h); n = len(xs)
        mx = (n - 1) / 2.0; my = sum(xs) / n
        num = sum((i - mx) * (xs[i] - my) for i in range(n))
        den = sum((i - mx) ** 2 for i in range(n))
        if den == 0 or my == 0:
            return None
        slope = num / den
        net = slope * (n - 1) / my          # net fractional move across the window
        if net > MT_BAND:
            return "long"
        if net < -MT_BAND:
            return "short"
        return None

    # ---- signal with the direction gate bolted on ----
    def _signal(self, s, of, tick):
        sig = super()._signal(s, of, tick)
        # maintain the per-stock micro-trend history EVERY bar (even when no signal)
        c = of.get("close")
        if c is not None:
            self.hist.setdefault(id(s), deque(maxlen=MT_WIN)).append(c)
        if sig is None or self.gate == "base":
            return sig
        raw = sig["side"]
        micro = self._micro_dir(id(s))
        regime = self.regime_dir
        new, flipped = raw, 0
        if self.gate == "micro_flip":
            if micro and micro != raw:
                new, flipped = micro, 1
        elif self.gate == "regime_flip":
            if regime and regime != raw:
                new, flipped = regime, 1
        elif self.gate == "both_flip":
            if micro and regime and micro == regime and micro != raw:
                new, flipped = micro, 1
        elif self.gate == "micro_confirm":
            if micro and micro != raw:
                self.n_dropped += 1
                return None
        elif self.gate == "regime_confirm":
            if regime and regime != raw:
                self.n_dropped += 1
                return None
        if flipped:
            sig["side"] = new
            sig["flipped"] = 1
            self.n_flip += 1
        return sig


def run(minutes, gate, consec=2, regime_mode="ema"):
    eng = GatedEngine("2026-07-08", gate=gate, consec=consec, regime_mode=regime_mode)
    for m, of, tk in minutes:
        eng.on_minute(m, of, tk)
    return eng


def _row(lbl, tr):
    m = _metrics(tr)
    if not m.get("n"):
        return f"  {lbl:14s} n=   0"
    return (f"  {lbl:14s} n={m['n']:>4d}  WR={m['wr']:>5.1f}%  avgRet={m['avg_ret']:>+7.3f}%  "
            f"PF={m['pf']:>5.2f}  net=Rs{m['net']:>12,.0f}")


def report(tag, eng, base_all):
    tr = eng.trades
    lg = [t for t in tr if t["side"] == "long"]
    sh = [t for t in tr if t["side"] == "short"]
    m = _metrics(tr)
    hdr = f"===== {tag} ====="
    extra = f"flips={eng.n_flip}" if eng.n_flip else ""
    if eng.n_dropped:
        extra += f"  dropped={eng.n_dropped}"
    print(f"\n{hdr}   {extra}")
    print(_row("ALL", tr))
    print(_row("LONG", lg))
    print(_row("SHORT", sh))
    # deltas vs baseline ALL
    if base_all is not None and m.get("n"):
        b = base_all
        dwr = m["wr"] - b["wr"]
        dn = m["n"] - b["n"]
        dnet = m["net"] - b["net"]
        print(f"  {'d vs base':14s} dWR={dwr:>+5.1f}pp  dn={dn:>+4d}  dnet=Rs{dnet:>+12,.0f}")
    return m


def main():
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-07-08"
    con = _con(ro=True)
    minutes = _load_day(con, date)
    con.close()
    print(f"[exp] DIRECTION/REGIME GATE -- {date} | {len(minutes)} minutes")
    print(f"[exp] trail FIXED: hardstop -3% arm +3% floor +2% give 1.25% | consec=2 | trigger untouched")
    print(f"[exp] micro-trend: slope over {MT_WIN}-min (min {MT_MIN}, band {MT_BAND:.2%}) | "
          f"regime: NIFTY 50 vs EMA{EMA_WIN}")

    # baseline
    base = run(minutes, "base")
    bm = _metrics(base.trades)
    print(f"\n[exp] regime read across session: "
          f"{base_reg(base)}")
    report("BASELINE (no gate)", base, None)
    base_all = bm

    # (gate, label, regime_mode)
    order = [
        ("micro_flip",      "(a) micro_flip",                   "ema"),
        ("regime_flip",     "(b) regime_flip [ema]",            "ema"),
        ("both_flip",       "(c) both_flip [micro&ema]",        "ema"),
        ("micro_confirm",   "(d) micro_confirm [drops]",        "ema"),
        ("regime_confirm",  "(d) regime_confirm [ema,drops]",   "ema"),
        # supplementary: stronger / more persistent regime reads (keep count)
        ("regime_flip",     "(b2) regime_flip [anchor]",        "anchor"),
        ("regime_flip",     "(b3) regime_flip [ema&breadth]",   "ema_and_breadth"),
        ("regime_confirm",  "(d2) regime_confirm [anchor,drops]", "anchor"),
    ]
    results = {"base": base_all}
    labels = {"base": "baseline"}
    for g, tag, rmode in order:
        eng = run(minutes, g, regime_mode=rmode)
        key = f"{g}|{rmode}"
        results[key] = report(tag, eng, base_all)
        labels[key] = tag
    # compact comparison table
    print("\n\n================ COMPARISON TABLE (vs baseline; consec=2, trail fixed) ================")
    print(f"{'variant':34s} {'n':>5s} {'WR%':>6s} {'dWR':>6s} {'net Rs':>13s} {'dnet Rs':>13s}")
    def line(name, key):
        m = results[key]
        if not m.get("n"):
            print(f"{name:34s}   n=0"); return
        dwr = m["wr"] - base_all["wr"]; dnet = m["net"] - base_all["net"]
        keep = "" if m["n"] >= base_all["n"] - 5 else "  <-count cut"
        print(f"{name:34s} {m['n']:>5d} {m['wr']:>6.1f} {dwr:>+6.1f} {m['net']:>13,.0f} {dnet:>+13,.0f}{keep}")
    line("baseline", "base")
    for g, tag, rmode in order:
        line(tag, f"{g}|{rmode}")


def base_reg(eng):
    rm = eng.reg_minutes
    tot = sum(rm.values()) or 1
    return (f"NIFTY-regime minutes  long={rm['long']} ({rm['long']/tot:.0%})  "
            f"short={rm['short']} ({rm['short']/tot:.0%})  none={rm['none']}")


if __name__ == "__main__":
    main()
