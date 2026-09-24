"""EXPERIMENT — LEVER: ORDER-FLOW STRUCTURE (cumulative signed delta + absorption + whale).

Upgrades the entry from a single-minute book snapshot to persistent, STRUCTURED flow:
  1. CUMULATIVE SIGNED TICK DELTA per instrument over the last N minutes
       (buy_vol - sell_vol, taken straight from mkt_trades_1min which already stores
        buy_vol / sell_vol). Require the delta to AGREE with the trade side and be RISING
        to confirm; FLIP the side when it strongly disagrees.
  2. ABSORPTION — heavy same-side aggression (buy_vol_pct extreme) but price NOT advancing
        over the last few bars => a passive player is absorbing => the move fails => skip.
  3. WHALE — big resting order on the opposite touch (base sig whale_side against the trade)
        => skip; large block print (tick max_trade_qty) recorded as positive confirmation.

Subclasses Engine and overrides _signal only (per the brief). Maintains its own per-symbol
rolling delta/price history. Two run modes off the SAME subclass:
  - gate OFF  -> returns the base sig untouched (== baseline) but RECORDS flow features
                 at every entry, so we can run the delta-AGREEMENT diagnostic.
  - gate ON   -> applies confirm / flip / skip.

Validation day: 2026-07-09 (live, partial — the tick table is EMPTY for 2026-07-08).

Run:  python exp_flow_structure.py [YYYY-MM-DD]
"""
import sys, sqlite3
from collections import deque, defaultdict
from flow_paper_engine import Engine, _load_day, _metrics, _con, OF_KEEP, WHALE_MULT

# ---- flow-structure knobs ----------------------------------------------------
FLOW_WIN     = 5      # rolling window (minutes) for cumulative signed delta / price
STRONG_DIS   = 0.40   # |delta_ratio| this large AND opposite side => FLIP
CONF_MIN     = 0.05   # need at least this much same-side delta_ratio to confirm
ABS_BVP_HI   = 75.0   # buy_vol_pct this extreme (same side) = heavy aggression
BLOCK_MULT   = 4.0    # block print >= this * avg tick vol = institutional block


def _rich_ticks(con, date):
    """Like flow_paper_engine._load_day's tickmap but keeps buy_vol/sell_vol/volume/bvp/block."""
    tm = {}
    for r in con.execute(
        "SELECT bar_time,instrument_key,buy_vol,sell_vol,volume,buy_vol_pct,max_trade_qty,n_ticks "
        "FROM mkt_trades_1min WHERE substr(bar_time,1,10)=?", (date,)):
        tm.setdefault(r[0], {})[r[1]] = dict(buy_vol=r[2], sell_vol=r[3], volume=r[4],
                                             buy_vol_pct=r[5], max_trade_qty=r[6], n_ticks=r[7])
    return tm


def load_day_rich(con, date):
    """One pass -> [(minute, of_rows, rich_tick_rows), ...] (tick dict carries buy_vol/sell_vol)."""
    cols = "bar_time,symbol,segment,instrument_key," + ",".join(OF_KEEP)
    tickmap = _rich_ticks(con, date)
    out = []; cur = None; of_rows = {}
    for r in con.execute(
        f"SELECT {cols} FROM mkt_orderflow_1min WHERE substr(bar_time,1,10)=? AND bar_time>=? ORDER BY bar_time",
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


class FlowEngine(Engine):
    """Order-flow-structure lever on top of the base lift engine."""
    def __init__(self, date, gate=False, **kw):
        super().__init__(date, **kw)
        self.gate = gate
        self._hist = defaultdict(lambda: deque(maxlen=FLOW_WIN))  # id(s) -> deque of bar dicts
        self._flow_at_entry = {}   # (key, minute) -> flow diagnostic captured at entry bar
        self._last_flow = {}       # id(s) -> flow dict computed this bar
        self._n_flip = 0; self._n_skip = 0

    # ---- flow feature computation (uses the rolling window incl. current bar) ----
    def _flow(self, s, of, tick, side):
        c = of.get("close")
        h = self._hist[id(s)]
        # current-bar signed delta from tick (guard the futures buy_vol_pct==0 artifact)
        bv = sv = None
        if tick:
            bv, sv = tick.get("buy_vol"), tick.get("sell_vol")
            bvp = tick.get("buy_vol_pct")
            # futures / unclassified ticks come through as bvp==0 with all volume as sell -> unreliable
            if bvp == 0.0 and (sv or 0) > 0 and (bv or 0) == 0:
                bv = sv = None
        h.append(dict(bv=bv, sv=sv, bvp=(tick.get("buy_vol_pct") if tick else None), px=c))
        # cumulative signed delta over window (only bars with usable tick)
        win_b = sum(x["bv"] for x in h if x["bv"] is not None)
        win_s = sum(x["sv"] for x in h if x["sv"] is not None)
        tot = win_b + win_s
        have = sum(1 for x in h if x["bv"] is not None)
        cum_delta = win_b - win_s
        delta_ratio = (cum_delta / tot) if tot else 0.0
        # rising: this minute's own delta agrees with side
        cur_delta = ((bv - sv) if (bv is not None and sv is not None) else 0)
        rising = (cur_delta > 0) if side == "long" else (cur_delta < 0)
        # price advance across the window in the favourable direction
        pxs = [x["px"] for x in h if x["px"] is not None]
        if len(pxs) >= 2 and pxs[0]:
            radv = pxs[-1] / pxs[0] - 1
            price_adv = radv if side == "long" else -radv
        else:
            price_adv = 0.0
        # agreement of cumulative delta with the side
        agree = (delta_ratio > 0) if side == "long" else (delta_ratio < 0)
        # absorption: extreme same-side aggression yet price NOT going the side's way
        bvp = tick.get("buy_vol_pct") if tick else None
        same_side_extreme = bvp is not None and (
            (side == "long" and bvp >= ABS_BVP_HI) or (side == "short" and bvp <= 100 - ABS_BVP_HI))
        absorption = bool(same_side_extreme and price_adv <= 0)
        # block print (positive whale confirmation)
        block = tick.get("max_trade_qty") if tick else None
        avgv = (tick.get("volume") / tick.get("n_ticks")) if (tick and tick.get("n_ticks")) else None
        block_conf = bool(block and avgv and block >= BLOCK_MULT * avgv)
        return dict(have=have, cum_delta=cum_delta, delta_ratio=delta_ratio, agree=bool(agree),
                    rising=bool(rising), price_adv=price_adv, absorption=absorption,
                    block_conf=block_conf, has_tick=bool(tick and have > 0))

    def _signal(self, s, of, tick):
        sig = super()._signal(s, of, tick)
        if sig is None:
            # still advance the rolling window on non-signal bars so cum-delta stays continuous
            self._flow(s, of, tick, "long")
            return None
        side = sig["side"]
        f = self._flow(s, of, tick, side)
        self._last_flow[id(s)] = f
        # whale RESTING against the trade (base sig whale_side on the opposite touch)
        opp_touch = "ask" if side == "long" else "bid"
        whale_against = (sig.get("whale_side") == opp_touch)
        f["whale_against"] = bool(whale_against)
        sig["_flow"] = f
        if not self.gate:
            return sig  # baseline behaviour; features recorded via _open
        # ---- GATE: confirm / flip / skip ----
        if not f["has_tick"]:
            return sig  # no tick evidence -> fall back to base behaviour (don't punish missing data)
        # 1) strong disagreement of cumulative delta -> FLIP the side
        if (not f["agree"]) and abs(f["delta_ratio"]) >= STRONG_DIS:
            sig["side"] = "short" if side == "long" else "long"
            sig["flipped"] = 1; self._n_flip += 1
            return sig
        # 2) absorption OR whale resting against -> the move is failing -> SKIP
        if f["absorption"] or whale_against:
            self._n_skip += 1
            return None
        # 3) require confirmation: cumulative delta agrees (non-trivially) AND is rising
        if not (f["agree"] and abs(f["delta_ratio"]) >= CONF_MIN and f["rising"]):
            self._n_skip += 1
            return None
        return sig

    def _open(self, s, sym, key, of, minute, sig):
        super()._open(s, sym, key, of, minute, sig)
        if s.pos is not None:
            self._flow_at_entry[(key, minute)] = self._last_flow.get(id(s), {})


def _run(date, gate):
    con = _con(ro=True); minutes = load_day_rich(con, date); con.close()
    eng = FlowEngine(date, gate=gate, verbose=False, consec=2)
    for m, of, tk in minutes:
        eng.on_minute(m, of, tk)
    return eng


def _line(lbl, m):
    if not m.get("n"):
        return f"  {lbl:8s} n=0"
    return (f"  {lbl:8s} n={m['n']:>3d}  WR={m['wr']:>5.1f}%  avgRet={m['avg_ret']:>+7.3f}%  "
            f"MFE={m['avg_mfe']:>4.2f}%  MAE={m['avg_mae']:>5.2f}%  PF={m['pf']:>5.2f}  net=Rs{m['net']:>10,.0f}")


def _block(title, trades):
    print(f"\n{title}")
    print(_line("ALL", _metrics(trades)))
    print(_line("LONG", _metrics([t for t in trades if t["side"] == "long"])))
    print(_line("SHORT", _metrics([t for t in trades if t["side"] == "short"])))


def _diagnostic(eng, consec_lbl):
    """Among BASELINE trades: split by whether cumulative tick-delta AGREED with the side."""
    trades = eng.trades
    fe = eng._flow_at_entry
    agreed, disagr, notick = [], [], []
    absorb, whale_ag, blk = [], [], []
    for t in trades:
        f = fe.get((t["instrument_key"], t["entry_min"]), {})
        if not f or not f.get("has_tick"):
            notick.append(t); continue
        (agreed if f["agree"] else disagr).append(t)
        if f.get("absorption"): absorb.append(t)
        if f.get("whale_against"): whale_ag.append(t)
        if f.get("block_conf"): blk.append(t)

    def one(lbl, sub):
        m = _metrics(sub)
        if not m.get("n"):
            return f"    {lbl:22s} n=0"
        return (f"    {lbl:22s} n={m['n']:>3d}  WR={m['wr']:>5.1f}%  avgRet={m['avg_ret']:>+7.3f}%  "
                f"MFE={m['avg_mfe']:>4.2f}%  PF={m['pf']:>5.2f}")
    print(f"\n----- DELTA-AGREEMENT DIAGNOSTIC on BASELINE trades ({consec_lbl}) -----")
    print(f"    (does cumulative tick-delta separate winners from losers, BEFORE any gate?)")
    print(one("delta AGREED w/ side", agreed))
    print(one("delta DISAGREED", disagr))
    print(one("no usable tick", notick))
    print(one("ABSORPTION flagged", absorb))
    print(one("WHALE resting against", whale_ag))
    print(one("BLOCK-print confirmed", blk))
    return agreed, disagr


def main():
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-07-09"
    con = _con(ro=True)
    nt = con.execute("SELECT COUNT(*),COUNT(DISTINCT instrument_key),COUNT(DISTINCT bar_time),"
                     "MIN(bar_time),MAX(bar_time) FROM mkt_trades_1min WHERE substr(bar_time,1,10)=?",
                     (date,)).fetchone()
    con.close()
    print("=" * 88)
    print(f"ORDER-FLOW STRUCTURE lever — validation {date}")
    print(f"tick sample: rows={nt[0]:,}  instruments={nt[1]}  minutes={nt[2]}  span={nt[3]}..{nt[4]}")
    print(f"knobs: FLOW_WIN={FLOW_WIN}  STRONG_DIS={STRONG_DIS}  CONF_MIN={CONF_MIN}  "
          f"ABS_BVP_HI={ABS_BVP_HI}  BLOCK_MULT={BLOCK_MULT}  (base consec=2)")
    print("=" * 88)

    base = _run(date, gate=False)
    flow = _run(date, gate=True)

    _block("===== A: BASELINE (consec=2) =====", base.trades)
    _block(f"===== B: +FLOW-STRUCTURE (flips={flow._n_flip}, skips={flow._n_skip}) =====", flow.trades)

    # deltas
    ba, fa = _metrics(base.trades), _metrics(flow.trades)
    if ba.get("n") and fa.get("n"):
        print(f"\n  A/B delta:  n {ba['n']}->{fa['n']}   WR {ba['wr']:.1f}%->{fa['wr']:.1f}%   "
              f"avgRet {ba['avg_ret']:+.3f}%->{fa['avg_ret']:+.3f}%   net Rs{ba['net']:,.0f}->Rs{fa['net']:,.0f}")

    # diagnostic — run on the richer consec=1 baseline too (consec=2 is tiny)
    _diagnostic(base, "consec=2")
    b1 = _run_consec(date, 1)
    _diagnostic(b1, "consec=1 (larger sample)")


def _run_consec(date, k):
    con = _con(ro=True); minutes = load_day_rich(con, date); con.close()
    eng = FlowEngine(date, gate=False, verbose=False, consec=k)
    for m, of, tk in minutes:
        eng.on_minute(m, of, tk)
    return eng


if __name__ == "__main__":
    main()
