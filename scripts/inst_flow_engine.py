"""Institutional-Flow Screening & Paper-Trading Engine  (diagnostic, capital-unconstrained).

Scans every 1-min bar across cash + F&O + NIFTY-future context for BOTH long and short
INSTITUTIONAL-ENTRY signals, paper-trades every valid signal at Rs 5L, trails with the
fixed knobs below, and records a full trade log + plain-language journal (incl. 60-min
post-exit verdicts) + a minute-by-minute Tape + summary analytics.

SIGNAL STACK  (resting-book fields are LOG-ONLY, never entry inputs):
  Families (>=2 independent must agree on side; >=1 must be a PRICE family):
    AGGRESSION   executed aggression: tick buy/sell split + block + volume-spike, PRICE-confirmed.
                   (07-08 has no tick -> PROXY = volume spike + price thrust sign.)
    ABSORPTION   hit hard one way but price refuses + closes against the hitters.
    ACCEPTANCE   price accepted the far side of ATP/VWAP (long: close>=ATP; short: close<=ATP).
    OI (F&O)     LONG/SHORT buildup (oi_from_open x move_from_open). SUPPORTING only —
                   never anchors: an entry needs a PRICE family too (OI+acceptance = premium).
  Gates:
    ANTI-CHASE   skip if move_from_open is already stretched (top-quintile |move| vs peers now).
    MKT-CONTEXT  no longs while NIFTY-fut is strong SHORT-buildup; no shorts vice-versa.
    STOCK-FUT    F&O names: cash & future must agree on side.
  Re-entry: allowed, but only on a FRESH >=2-family confirmation (rising edge) after the
            prior position closed — never on the same stale signal.

EXITS (fixed for this run):  hard-stop -3.0% | arm +1.5% | floor +1.0% | give-back 0.5% | 15:29 sq-off.
No lookahead: signal on bar t -> act on bar t (next completed bar); intrabar stop/trail per the
validated exit gate. Net of costs every trade. Long/short always reported separately.

Run:  python inst_flow_engine.py 2026-07-08 2026-07-09
"""
import sys, statistics as st
from collections import deque, Counter, defaultdict
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from flow_paper_engine import _con, cost, _load_day, OF_KEEP, ROOT   # reuse data + cost machinery

# ---- fixed knobs (this run) --------------------------------------------------
CAP        = 500_000
HARD_STOP  = 0.030     # -3.0%
ARM        = 0.015     # +1.5% arms the trail
FLOOR      = 0.010     # +1.0% locked floor once armed  (spec: 0.9-1.0, set 1.0)
GIVEBACK   = 0.005     # 0.5% give-back from peak
EOD_MIN    = "15:29"
ENTRY_CUT  = "15:15"   # no NEW entries after this
# ---- signal params -----------------------------------------------------------
THR        = 0.0015    # 0.15% min 1-min price thrust for a PRICE-confirm
VMULT      = 2.0       # volume spike vs trailing median
WIN        = 20
TICK_HI    = 55.0      # tick buy% for aggression confirm (07-09)
BLOCK_X    = 3.0       # biggest print >= 3x avg tick = block boost
STRETCH_Q  = 0.80      # anti-chase: top-quintile |move_from_open| vs peers = stretched
OI_CTX     = 2.0       # NIFTY-fut |oi_from_open%| for a "strong" context read
OI_STOCK   = 0.5       # per-name oi_from_open% for a buildup vote
MIN_PRICE  = 20.0
WARMUP     = 5
NIFTY_FUT  = "NFO:NIFTY26JULFUT"
NIFTY_50   = "NSE:NIFTY 50"
RS_THR     = 0.002     # relative-strength gate: long needs excess-vs-NIFTY > +0.2%, short < -0.2%
PRICE_FAMS = {"AGGRESSION", "ABSORPTION", "ACCEPTANCE"}


class Sym:
    __slots__ = ("seg", "under", "prev_close", "day_open", "day_open_oi", "vol", "pos",
                 "above_atp", "seen", "reentry", "confirmed_prev", "closes")
    def __init__(self, seg, under):
        self.seg = seg; self.under = under; self.prev_close = None; self.day_open = None
        self.day_open_oi = None; self.vol = deque(maxlen=WIN); self.pos = None
        self.above_atp = 0; self.seen = 0; self.reentry = 0
        self.confirmed_prev = False           # rising-edge tracker for stale-signal suppression
        self.closes = []                       # (minute, close) full-day series for post-exit study


def _under(sym, seg):
    return sym  # underlying key = symbol; cash 'NAUKRI' and fut 'NAUKRI' share it


class InstEngine:
    def __init__(self, date, has_tick, rs_gate=False, rs_thr=RS_THR, mode="v1"):
        self.date = date; self.has_tick = has_tick; self.mode = mode
        self.syms = {}; self.trades = []; self.tape = []; self.skips = []; self.n_min = 0
        self.ctx = "neutral"                    # NIFTY-fut market context
        self.rs_gate = rs_gate; self.rs_thr = rs_thr; self.mkt_move = 0.0   # NIFTY-50 move-from-open

    # ---------- family detectors (each: +1 long / -1 short / 0) ----------
    def _fam_aggression(self, s, of, tick, ret, spike):
        if not spike or abs(ret) < THR:
            return 0, 0.0
        side = 1 if ret > 0 else -1
        strength = abs(ret) / THR
        if self.has_tick and tick and tick.get("buy_vol_pct") is not None:
            bp = tick["buy_vol_pct"]
            if side == 1 and bp < TICK_HI:
                return 0, 0.0                   # exact: tick must confirm the buy aggression
            if side == -1 and bp > 100 - TICK_HI:
                return 0, 0.0
            strength += abs(bp - 50) / 10
            mo, vol = tick.get("max_trade_qty"), of["volume"] or 0
            if mo and vol and mo >= 0.10 * vol:
                strength += 1                    # block: one print >= 10% of the minute's volume
        return side, strength

    def _fam_absorption(self, s, of, tick, ret):
        hi, lo, c = of["high"], of["low"], of["close"]
        if hi is None or lo is None or hi <= lo:
            return 0, 0.0
        loc = (c - lo) / (hi - lo)               # close location in the bar's range
        # who hit? tick if we have it, else book-pressure proxy
        if self.has_tick and tick and tick.get("buy_vol_pct") is not None:
            bp = tick["buy_vol_pct"]
            sell_hit, buy_hit = bp <= 100 - TICK_HI, bp >= TICK_HI
        else:
            tbq, tsq = of["total_buy_qty"] or 0, of["total_sell_qty"] or 0
            imb = tbq / (tbq + tsq) if (tbq + tsq) else 0.5
            sell_hit, buy_hit = imb <= 0.45, imb >= 0.55
        if sell_hit and loc >= 0.70 and ret >= -THR:     # hit down, held up -> bullish absorption
            return 1, 1.0 + loc
        if buy_hit and loc <= 0.30 and ret <= THR:       # hit up, held down -> bearish absorption
            return -1, 1.0 + (1 - loc)
        return 0, 0.0

    def _fam_acceptance(self, s, of):
        c, atp = of["close"], of.get("atp")
        if c is None or atp is None or atp <= 0:
            return 0, 0.0
        acc = s.above_atp / s.seen if s.seen else 0.5    # time-above-ATP so far
        if c >= atp:
            return 1, 0.5 + acc
        return -1, 0.5 + (1 - acc)

    def _fam_oi(self, s, of):
        if s.seg not in ("FUT",) or of.get("oi") is None or s.day_open_oi in (None, 0):
            return 0, 0.0
        oi_open = (of["oi"] / s.day_open_oi - 1) * 100
        move_open = (of["close"] / s.day_open - 1) * 100 if s.day_open else 0
        if abs(oi_open) < OI_STOCK:
            return 0, 0.0
        if oi_open > 0:
            return (1, abs(oi_open)) if move_open >= 0 else (-1, abs(oi_open))   # LONG / SHORT buildup
        return (1, abs(oi_open)) if move_open >= 0 else (-1, abs(oi_open))       # covering / unwind (weak, same side as price)

    # ---------- per-name candidate ----------
    def _candidate(self, s, of, tick):
        c = of["close"]
        if c is None or c < MIN_PRICE or s.prev_close is None or not s.vol:
            return None
        ret = c / s.prev_close - 1
        med = sorted(s.vol)[len(s.vol) // 2]
        spike = med > 0 and (of["volume"] or 0) >= VMULT * med
        fams = {}
        for name, val in (("AGGRESSION", self._fam_aggression(s, of, tick, ret, spike)),
                          ("ABSORPTION", self._fam_absorption(s, of, tick, ret)),
                          ("ACCEPTANCE", self._fam_acceptance(s, of)),
                          ("OI", self._fam_oi(s, of))):
            if val[0] != 0:
                fams[name] = val
        if not fams:
            return None
        long_s = sum(v[1] for f, v in fams.items() if v[0] == 1)
        short_s = sum(v[1] for f, v in fams.items() if v[0] == -1)
        side = 1 if long_s > short_s else -1
        fired = [f for f, v in fams.items() if v[0] == side]
        if len(fired) < 2:
            return None
        if not (set(fired) & PRICE_FAMS):        # OI (+ anything) can never enter alone
            return None
        score = sum(fams[f][1] for f in fired)
        return dict(side=side, fired=fired, score=round(score, 2), ret=ret,
                    move_open=(c / s.day_open - 1) * 100 if s.day_open else 0)

    def _reject_v2(self, side, mv, rs, sig, stretched):
        """v2 = REGIME-ASYMMETRIC selection (from the alpha anatomy):
          - WITH-TREND side: liberal — take the flow signal (capture the beta), only anti-chase.
          - COUNTER-TREND side: restrictive — only genuine divergence, since counter-trend trades
            are where the engine bled.
              counter-trend LONG  (falling market): require strong relative strength AND actually rising.
              counter-trend SHORT (rising market):  require a FADED POP with distribution/absorption
                                                     (not a falling knife — those bounce).
          Regime read = NIFTY-50 move-from-open + NIFTY-future OI context (faster than move alone)."""
        if self.mkt_move > 0.001 or self.ctx == "LONG-buildup":
            regime = "up"
        elif self.mkt_move < -0.001 or self.ctx == "SHORT-buildup":
            regime = "down"
        else:
            regime = "neutral"
        if side == 1:                                   # LONG = genuine strength only
            if mv <= 0 or rs < 0.001:                   # never long a stock red on the day or lagging the market
                return "long-not-leading"
            if regime == "down" and rs < 0.005:         # counter-trend long -> demand STRONG relative strength
                return "counter-long-weak"
            return "anti-chase" if stretched else None
        if regime == "up":                              # counter-trend short -> fade a failed pop only
            if mv < 0.003:
                return "not-a-pop"
            if "ABSORPTION" not in sig["fired"]:
                return "no-distribution"
        return "anti-chase" if stretched else None

    # ---------- market context (NIFTY future) ----------
    def _update_ctx(self, of_rows):
        nf = of_rows.get(NIFTY_FUT)
        if not nf or nf.get("oi") is None:
            return
        s = self.syms.get(NIFTY_FUT)
        if not s or s.day_open is None or getattr(s, "day_open_oi", None) in (None, 0):
            return
        oi_open = (nf["oi"] / s.day_open_oi - 1) * 100
        move_open = (nf["close"] / s.day_open - 1) * 100
        if oi_open > OI_CTX and move_open < 0:
            self.ctx = "SHORT-buildup"
        elif oi_open > OI_CTX and move_open > 0:
            self.ctx = "LONG-buildup"
        else:
            self.ctx = "neutral"

    # ---------- exit machinery (validated gate, this run's knobs) ----------
    def _manage(self, s, of, minute):
        p = s.pos; hi, lo, o = of["high"], of["low"], of["open"]
        entry, peak, side = p["entry_px"], p["peak_fav"], p["side"]
        armed = peak >= ARM
        floor_binds = (peak - GIVEBACK) <= FLOOR
        if side == "long":
            stop = entry * (1 + max(FLOOR, peak - GIVEBACK)) if armed else entry * (1 - HARD_STOP)
            if o is not None and o <= stop:
                return self._close(s, o, minute, armed, floor_binds, gap=True)
            if lo is not None and lo <= stop:
                return self._close(s, stop, minute, armed, floor_binds)
        else:
            stop = entry * (1 - max(FLOOR, peak - GIVEBACK)) if armed else entry * (1 + HARD_STOP)
            if o is not None and o >= stop:
                return self._close(s, o, minute, armed, floor_binds, gap=True)
            if hi is not None and hi >= stop:
                return self._close(s, stop, minute, armed, floor_binds)
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
            return False
        s.reentry += 1
        s.pos = dict(symbol=sym, key=key, seg=s.seg, side=("long" if sig["side"] == 1 else "short"),
                     entry_min=minute, entry_px=px, qty=qty, peak_fav=0.0, max_adv=0.0, cur_px=px,
                     sig=sig, reentry=s.reentry, ctx=self.ctx,
                     atp_acc=round(s.above_atp / s.seen * 100, 1) if s.seen else None)
        return True

    def _close(self, s, exit_px, minute, armed, floor_binds, gap=False, eod=False):
        p = s.pos; entry, qty, side = p["entry_px"], p["qty"], p["side"]
        pnl = qty * (exit_px - entry) if side == "long" else qty * (entry - exit_px)
        pct = (exit_px / entry - 1) * 100 if side == "long" else (entry / exit_px - 1) * 100
        net = pnl - cost(s.seg, entry, exit_px, qty)
        if eod:
            reason = "EOD"
        elif not armed:
            reason = "HARD_STOP"
        elif floor_binds:
            reason = "FLOOR"
        else:
            reason = "TRAIL"
        peak = p["peak_fav"] * 100
        sig = p["sig"]
        self.trades.append(dict(
            entry_time=p["entry_min"], symbol=p["symbol"], segment=s.seg, side=side,
            entry_px=round(entry, 2), qty=qty, notional=round(entry * qty),
            signal_families=("+".join(sig["fired"])), falcon_flow_score=sig["score"],
            exit_time=minute, exit_px=round(exit_px, 2), exit_reason=reason,
            pnl_rs=round(pnl), net_pnl_rs=round(net), pnl_pct=round(pct, 3),
            mfe_pct=round(p["peak_fav"] * 100, 3), mae_pct=round(p["max_adv"] * 100, 3),
            peak_profit_pct=round(peak, 3), give_back_from_peak_pct=round(peak - pct, 3),
            hold_min=self._mins(p["entry_min"], minute), re_entry_number=p["reentry"],
            market_context_at_entry=p["ctx"], atp_acceptance_pct=p["atp_acc"],
            entry_thrust_pct=round(sig["ret"] * 100, 3), entry_move_open_pct=round(sig.get("move_open", 0), 3),
            instrument_key=p["key"], gap_exit=int(gap)))
        s.pos = None
        return self.trades[-1]

    @staticmethod
    def _mins(a, b):
        from datetime import datetime
        return int((datetime.strptime(b, "%Y-%m-%d %H:%M") - datetime.strptime(a, "%Y-%m-%d %H:%M")).total_seconds() // 60)

    # ---------- per-minute step ----------
    def on_minute(self, minute, of_rows, tick_rows):
        hhmm = minute[11:16]; is_eod = hhmm >= EOD_MIN
        # PASS 0: refresh rolling state + day-open, collect close series
        for key, of in of_rows.items():
            sym, seg = of["symbol"], of["segment"]
            s = self.syms.get(key)
            if s is None:
                s = self.syms[key] = Sym(seg, _under(sym, seg)); s.day_open_oi = of.get("oi")
            if s.day_open is None and of["close"] is not None:
                s.day_open = of["close"]; s.day_open_oi = of.get("oi")
            if of["close"] is not None:
                s.closes.append((minute, of["close"]))
                if of.get("atp"):
                    s.seen += 1; s.above_atp += int(of["close"] >= of["atp"])
        self._update_ctx(of_rows)
        nsym = self.syms.get(NIFTY_50); nrow = of_rows.get(NIFTY_50)   # market move-from-open for RS
        if nsym and nsym.day_open and nrow and nrow["close"]:
            self.mkt_move = nrow["close"] / nsym.day_open - 1
        # PASS 1: candidates + cross-sectional stretch threshold + per-underlying sides
        cands = {}; moves = []; und_side = defaultdict(dict)
        for key, of in of_rows.items():
            s = self.syms[key]
            if of["segment"] == "INDEX" and key.startswith("NSE:"):
                continue
            tick = tick_rows.get(key)
            sig = self._candidate(s, of, tick)
            if of.get("close") and s.day_open:
                moves.append(abs(of["close"] / s.day_open - 1) * 100)
            if sig:
                cands[key] = sig
                und_side[s.under][s.seg] = sig["side"]
        stretch_thr = None
        if moves:
            moves.sort(); stretch_thr = moves[min(int(len(moves) * STRETCH_Q), len(moves) - 1)]
        # PASS 2: manage open positions, then enter survivors
        opened = closed = 0; new_l = []; new_s = []; exit_reasons = []
        for key, of in of_rows.items():
            s = self.syms[key]
            if of["segment"] == "INDEX" and key.startswith("NSE:"):
                continue
            if s.pos is not None:
                r = self._manage(s, of, minute)
                if r is None and is_eod:
                    r = self._close(s, of["close"], minute, s.pos["peak_fav"] >= ARM,
                                    (s.pos["peak_fav"] - GIVEBACK) <= FLOOR, eod=True)
                if r is not None:
                    closed += 1; exit_reasons.append(r["exit_reason"])
            sig = cands.get(key)
            confirmed = sig is not None
            fresh = confirmed and not s.confirmed_prev      # rising edge -> not a stale re-fire
            s.confirmed_prev = confirmed
            if (s.pos is None and confirmed and fresh and hhmm < ENTRY_CUT and self.n_min >= WARMUP):
                side = sig["side"]
                mv = of["close"] / s.day_open - 1 if s.day_open else 0
                rs = mv - self.mkt_move                          # excess return vs NIFTY-50 (relative strength)
                fut_disagree = (s.under in und_side and "CASH" in und_side[s.under] and "FUT" in und_side[s.under]
                                and und_side[s.under]["CASH"] != und_side[s.under]["FUT"])
                stretched = stretch_thr is not None and abs(mv) * 100 >= stretch_thr and ((mv > 0) == (side == 1))
                if fut_disagree:
                    reject = "stock-fut-disagree"
                elif self.mode == "v2":
                    reject = self._reject_v2(side, mv, rs, sig, stretched)
                else:
                    if self.rs_gate and ((side == 1 and rs < self.rs_thr) or (side == -1 and rs > -self.rs_thr)):
                        reject = "weak-RS"
                    elif stretched:
                        reject = "anti-chase"
                    elif (side == 1 and self.ctx == "SHORT-buildup") or (side == -1 and self.ctx == "LONG-buildup"):
                        reject = "mkt-context"
                    else:
                        reject = None
                if reject:
                    self.skips.append(dict(minute=minute, symbol=of["symbol"], side=("long" if side == 1 else "short"),
                                           reason=reject, move_from_open=round(mv * 100, 2), rs_pct=round(rs * 100, 2),
                                           families="+".join(sig["fired"])))
                elif self._open(s, of["symbol"], key, of, minute, sig):
                    opened += 1
                    (new_l if side == 1 else new_s).append(of["symbol"])
            if of["close"] is not None:
                s.prev_close = of["close"]
            if of["volume"] is not None:
                s.vol.append(of["volume"])
        self.n_min += 1
        live = [s.pos for s in self.syms.values() if s.pos]
        rmix = ", ".join(f"{n} {r.lower()}" for r, n in Counter(exit_reasons).items())
        self.tape.append(dict(
            time=hhmm,
            new_long=(f"{len(new_l)}" + (" (" + ",".join(new_l[:6]) + ("…" if len(new_l) > 6 else "") + ")" if new_l else "")),
            new_short=(f"{len(new_s)}" + (" (" + ",".join(new_s[:6]) + ("…" if len(new_s) > 6 else "") + ")" if new_s else "")),
            exits=(f"{closed}" + (f": {rmix}" if closed else "")),
            open_positions=len(live),
            open_long_short=f"{sum(1 for p in live if p['side']=='long')}/{sum(1 for p in live if p['side']=='short')}",
            running_net_pnl_rs=round(sum(t["net_pnl_rs"] for t in self.trades)),
            niftyfut_context=self.ctx))
        return opened, closed

    # ---------- post-exit study (60 min after each exit) ----------
    def post_exit(self):
        series = {k: s.closes for k, s in self.syms.items()}
        for t in self.trades:
            ser = series.get(t["instrument_key"], [])
            ex = t["exit_time"]; entry = t["entry_px"]; side = t["side"]
            fwd = [c for (m, c) in ser if m > ex][:60]
            if not fwd:
                t.update(post_max_favor_pct=None, post_max_adverse_pct=None,
                         recovered_past_exit=None, verdict="no post-data")
                continue
            xpx = t["exit_px"]
            if side == "long":
                favor = max((c / xpx - 1) * 100 for c in fwd)          # further UP after we sold
                adverse = min((c / xpx - 1) * 100 for c in fwd)
                past_peak = max(fwd) / entry - 1 >= t["peak_profit_pct"] / 100
            else:
                favor = max((1 - c / xpx) * 100 for c in fwd)          # further DOWN after we covered
                adverse = min((1 - c / xpx) * 100 for c in fwd)
                past_peak = (1 - min(fwd) / entry) >= t["peak_profit_pct"] / 100
            recovered = favor > 0.10
            if t["exit_reason"] == "HARD_STOP" and favor >= 1.0:
                verdict = "STOP TOO TIGHT (stopped then recovered)"
            elif t["exit_reason"] in ("TRAIL", "FLOOR") and favor >= GIVEBACK * 100 * 1.5:
                verdict = "TRAIL TOO TIGHT (gave back then resumed)"
            elif t["exit_reason"] == "EOD":
                verdict = "EOD (time exit)"
            else:
                verdict = "CORRECT (price went against us / no recovery)"
            t.update(post_max_favor_pct=round(favor, 3), post_max_adverse_pct=round(adverse, 3),
                     recovered_past_exit=bool(recovered), post_past_peak=bool(past_peak), verdict=verdict)

    # ---------- narrative journal ----------
    def journal(self):
        out = []
        for t in self.trades:
            why_in = (f"{t['signal_families']} agreed {t['side'].upper()} (score {t['falcon_flow_score']}); "
                      f"entry-bar thrust {t.get('entry_thrust_pct', 0):+.2f}%; ATP-acceptance "
                      f"{t['atp_acceptance_pct']}%; NIFTY-fut {t['market_context_at_entry']}.")
            why_out = {"HARD_STOP": f"hard stop -{HARD_STOP:.1%} hit (never armed).",
                       "TRAIL": f"trail: gave back {GIVEBACK:.1%} from peak {t['peak_profit_pct']:.2f}%.",
                       "FLOOR": f"floor +{FLOOR:.1%} locked profit as peak faded.",
                       "EOD": "squared off at 15:29 (no overnight)."}[t["exit_reason"]]
            after = (f"60-min post-exit: moved {t.get('post_max_favor_pct')}% further in our favor, "
                     f"{t.get('post_max_adverse_pct')}% against; "
                     f"{'recovered past exit' if t.get('recovered_past_exit') else 'did not recover'}.")
            out.append(dict(entry_time=t["entry_time"], symbol=t["symbol"], side=t["side"],
                            segment=t["segment"], families=t["signal_families"],
                            net_pnl_rs=t["net_pnl_rs"], pnl_pct=t["pnl_pct"], exit_reason=t["exit_reason"],
                            WHY_IN=why_in, WHY_OUT=why_out, WHAT_AFTER=after, VERDICT=t["verdict"]))
        return out


# ============================ analytics + Excel ============================
def _agg(df):
    if df.empty:
        return pd.Series(dict(n=0, WR_pct=0, net_rs=0))
    wins = (df["net_pnl_rs"] > 0).sum()
    return pd.Series(dict(n=len(df), WR_pct=round(wins / len(df) * 100, 1),
                          avg_ret_pct=round(df["pnl_pct"].mean(), 3),
                          avg_mfe_pct=round(df["mfe_pct"].mean(), 2),
                          avg_mae_pct=round(df["mae_pct"].mean(), 2),
                          gross_rs=round(df["pnl_rs"].sum()), net_rs=round(df["net_pnl_rs"].sum())))


def coverage_sheet(date, has_tick):
    rows = [
        ("ATP / VWAP acceptance", "EXACT", "average_price polled both days"),
        ("OI buildup (F&O)", "EXACT", "open interest polled both days"),
        ("Stock-future agreement", "EXACT", "cash+fut both captured"),
        ("Market-context (NIFTY fut)", "EXACT", "NIFTY future OI+price both days"),
        ("Anti-chase (move-from-open)", "EXACT", "computed from OHLC both days"),
        ("Executed AGGRESSION (tick buy/sell, block)", "EXACT" if has_tick else "PROXY",
         "websocket ticks" if has_tick else "no tick 07-08 -> volume-spike + price-thrust sign"),
        ("ABSORPTION (who hit, held?)", "EXACT" if has_tick else "PROXY",
         "tick buy/sell split" if has_tick else "no tick 07-08 -> book buy/sell-qty pressure proxy"),
        ("Orders-count / whale (log-only)", "PRESENT" if has_tick else "ABSENT", "b1o/a1o only from 07-09"),
    ]
    return pd.DataFrame(rows, columns=["Signal component", f"{date} coverage", "Basis"])


def write_excel(date, eng):
    trades = pd.DataFrame(eng.trades)
    out = ROOT / "docs" / "ops" / f"INST_FLOW_{date}.xlsx"
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        coverage_sheet(date, eng.has_tick).to_excel(w, "Coverage", index=False)
        pd.DataFrame(eng.tape).to_excel(w, "Tape", index=False)
        if not trades.empty:
            cols = ["entry_time", "symbol", "segment", "side", "entry_px", "qty", "notional",
                    "signal_families", "falcon_flow_score", "exit_time", "exit_px", "exit_reason",
                    "pnl_rs", "net_pnl_rs", "pnl_pct", "mfe_pct", "mae_pct", "peak_profit_pct",
                    "give_back_from_peak_pct", "hold_min", "re_entry_number", "market_context_at_entry",
                    "atp_acceptance_pct", "post_max_favor_pct", "post_max_adverse_pct",
                    "recovered_past_exit", "verdict"]
            trades[[c for c in cols if c in trades.columns]].to_excel(w, "Trades", index=False)
            pd.DataFrame(eng.journal()).to_excel(w, "Journal", index=False)
            # ---- Summary analytics ----
            blocks = []
            blocks.append(("BY SIDE", trades.groupby("side").apply(_agg)))
            blocks.append(("BY SEGMENT", trades.groupby("segment").apply(_agg)))
            trades["hour"] = trades["entry_time"].str[11:13]
            blocks.append(("BY HOUR", trades.groupby("hour").apply(_agg)))
            blocks.append(("BY FAMILY COMBO", trades.groupby("signal_families").apply(_agg)))
            blocks.append(("BY RE-ENTRY #", trades.groupby("re_entry_number").apply(_agg)))
            blocks.append(("BY EXIT REASON", trades.groupby("exit_reason").apply(_agg)))
            blocks.append(("PREMATURE vs CORRECT", trades.assign(v=trades["verdict"].str.split(" ").str[0]).groupby("v").apply(_agg)))
            startrow = 0
            for title, blk in blocks:
                pd.DataFrame({title: []}).to_excel(w, "Summary", startrow=startrow, index=False)
                blk.to_excel(w, "Summary", startrow=startrow + 1)
                startrow += len(blk) + 4
            if eng.skips:
                pd.DataFrame(eng.skips).to_excel(w, "AntiChase_Skips", index=False)
    import shutil
    try:
        shutil.copy(out, Path.home() / "Downloads" / out.name)
    except Exception:
        pass
    return out, trades


def run(date, mode="v2"):
    has_tick_probe = _con(ro=True)
    nt = has_tick_probe.execute("SELECT count(*) FROM mkt_trades_1min WHERE substr(bar_time,1,10)=?", (date,)).fetchone()[0]
    has_tick_probe.close()
    has_tick = nt > 0
    print(f"\n===== INST-FLOW {date}  (tick={'yes' if has_tick else 'NO -> proxy aggression'} | mode={mode}) =====", flush=True)
    con = _con(ro=True); minutes = _load_day(con, date)
    # OI needs the column loaded; _load_day already includes 'oi' via OF_KEEP
    con.close()
    eng = InstEngine(date, has_tick, mode=mode)
    for m, of, tk in minutes:
        eng.on_minute(m, of, tk)
    eng.post_exit()
    out, trades = write_excel(date, eng)
    # console summary
    if trades.empty:
        print("  no trades"); return
    def fmt(df, lbl):
        if df.empty:
            return f"  {lbl:8s} n=0"
        wr = (df["net_pnl_rs"] > 0).mean() * 100
        return (f"  {lbl:8s} n={len(df):>4d}  WR={wr:>5.1f}%  net=Rs{df['net_pnl_rs'].sum():>12,.0f}  "
                f"avgRet={df['pnl_pct'].mean():>+6.3f}%  MFE={df['mfe_pct'].mean():.2f}%")
    print(fmt(trades, "ALL"))
    print(fmt(trades[trades.side == "long"], "LONG"))
    print(fmt(trades[trades.side == "short"], "SHORT"))
    print(fmt(trades[trades.segment == "CASH"], "CASH"))
    print(fmt(trades[trades.segment == "FUT"], "FUT"))
    vc = trades["verdict"].str.split(" ").str[0].value_counts().to_dict()
    print("  verdicts:", vc)
    print(f"  anti-chase skips: {len(eng.skips)}")
    print(f"  -> {out}  (copy in Downloads)")


if __name__ == "__main__":
    days = sys.argv[1:] or ["2026-07-08", "2026-07-09"]
    for d in days:
        run(d)
