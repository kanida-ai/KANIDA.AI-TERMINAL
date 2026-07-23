"""
Engine V5 — V4 production signals + pyramid + trailing exit + cash constraints.

Differences from V4:
  - At each signal: enter ₹1L base size at next-day open (Method A).
  - PYRAMID: at +5% from avg entry add ₹50k; at +10% add another ₹50k.
              Cap per-position at 2x base = ₹2L.
  - TRAILING EXIT: replace fixed T+5 with adaptive exits:
      * Initial stop: -7% from avg entry on day 1.
      * Once high-water >= +10%: stop tightens to MAX(avg_entry, 10-day low).
      * Time stop: 30 trading days from entry, if return <+3%, exit at close.
  - Concurrent positions cap + cash constraint enforced like the constrained sim.
  - Kill-switch disabled (portfolio-level cut not appropriate for this use case).

Outputs: per-trade ledger (with all pyramid fills + final exit reason),
daily MTM equity curve, full metrics including monthly P&L.
"""
from __future__ import annotations
import sqlite3, statistics
from collections import defaultdict, deque
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional


# ── V5 thresholds ────────────────────────────────────────────────────────────

PER_TRADE       = 100_000.0       # base entry size
PYRAMID_AT      = (0.05, 0.10)    # add at these unrealized return levels
PYRAMID_FRAC    = 0.5             # each add = 0.5x base = ₹50k
INITIAL_STOP    = 0.07            # -7%
TRAIL_TRIGGER   = 0.10            # high-water >= +10% activates trail
TRAIL_LOOKBACK  = 10              # 10-day low
TIME_STOP_DAYS  = 30              # cut if no significant move by this
TIME_STOP_MIN   = 0.03            # ... unless still up >+3%
COST_BPS_RT     = 30.0
SLIP_BPS        = 5.0


def _close_on(con, sym, d):
    r = con.execute("SELECT close FROM ohlc_daily WHERE symbol=? AND trade_date=?",
                     (sym, d)).fetchone()
    return r[0] if r else None


def _hl_on(con, sym, d):
    r = con.execute("SELECT high, low FROM ohlc_daily WHERE symbol=? AND trade_date=?",
                     (sym, d)).fetchone()
    return r if r else (None, None)


def _open_on(con, sym, d):
    r = con.execute("SELECT open FROM ohlc_daily WHERE symbol=? AND trade_date=?",
                     (sym, d)).fetchone()
    return r[0] if r else None


def _trading_days_after(con, sym, after_date, n):
    rows = con.execute("""SELECT trade_date FROM ohlc_daily
                          WHERE symbol=? AND trade_date>? ORDER BY trade_date LIMIT ?""",
                       (sym, after_date, n)).fetchall()
    return [r[0] for r in rows]


def _all_trading_days(con, start, end):
    rows = con.execute("""SELECT DISTINCT trade_date FROM ohlc_daily
                          WHERE trade_date>=? AND trade_date<=? ORDER BY trade_date""",
                       (start, end)).fetchall()
    return [r[0] for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
# Position state
# ─────────────────────────────────────────────────────────────────────────────

class V5Position:
    __slots__ = ("symbol", "signal_date", "patterns", "entry_date",
                  "fills", "shares", "deployed", "avg_entry",
                  "high_water", "pyramid_steps", "trailing_active",
                  "trail_stop", "lows_window", "exit_date",
                  "exit_price", "exit_reason", "gross_pnl", "fees", "net_pnl")

    def __init__(self, symbol, signal_date, patterns, entry_date,
                  entry_price, shares, deployed):
        self.symbol = symbol
        self.signal_date = signal_date
        self.patterns = patterns
        self.entry_date = entry_date
        self.fills = [{"date": entry_date, "price": entry_price,
                        "shares": shares, "rupees": deployed, "kind": "entry"}]
        self.shares = shares
        self.deployed = deployed
        self.avg_entry = entry_price
        self.high_water = 0.0       # max unrealized return
        self.pyramid_steps = 0
        self.trailing_active = False
        self.trail_stop = None
        self.lows_window = deque(maxlen=TRAIL_LOOKBACK)
        self.exit_date = None
        self.exit_price = None
        self.exit_reason = None
        self.gross_pnl = 0.0
        self.fees = 0.0
        self.net_pnl = 0.0

    def add_pyramid(self, on_date, ref_price, rupees):
        slip = SLIP_BPS / 10_000.0
        px = ref_price * (1 + slip)
        new_shares = rupees / px
        self.fills.append({"date": on_date, "price": px,
                            "shares": new_shares, "rupees": rupees,
                            "kind": f"pyramid_{self.pyramid_steps + 1}"})
        self.shares += new_shares
        self.deployed += rupees
        self.avg_entry = self.deployed / self.shares
        self.pyramid_steps += 1

    def update_mark(self, last_price, day_low):
        if self.avg_entry <= 0: return
        ret = last_price / self.avg_entry - 1.0
        if ret > self.high_water:
            self.high_water = ret
        self.lows_window.append(day_low)
        if self.high_water >= TRAIL_TRIGGER and not self.trailing_active:
            self.trailing_active = True

    def stop_level(self) -> float:
        """Current effective stop level (price)."""
        if self.trailing_active and self.lows_window:
            return max(self.avg_entry, min(self.lows_window))
        # Pre-trail: initial stop
        return self.avg_entry * (1 - INITIAL_STOP)

    def should_close(self, day_low, day_close, days_held) -> Optional[str]:
        # Stop hit (intraday low broke stop)
        if day_low <= self.stop_level():
            return "trail_stop" if self.trailing_active else "initial_stop"
        # Time stop
        if days_held >= TIME_STOP_DAYS:
            ret = day_close / self.avg_entry - 1.0
            if ret < TIME_STOP_MIN:
                return "time_stop"
        return None

    def close_position(self, on_date, exit_price, reason):
        slip = SLIP_BPS / 10_000.0
        self.exit_date = on_date
        self.exit_price = exit_price * (1 - slip)
        self.exit_reason = reason
        gross = self.shares * (self.exit_price - self.avg_entry)
        self.gross_pnl = gross
        # Fees: charge on each fill (entry + pyramids) at half-RT, exit at half-RT
        n_legs = len(self.fills) + 1
        self.fees = self.deployed * (COST_BPS_RT / 2 / 10_000.0) + \
                     (self.shares * self.exit_price) * (COST_BPS_RT / 2 / 10_000.0)
        self.net_pnl = gross - self.fees

    def to_record(self):
        return {
            "symbol":        self.symbol,
            "signal_date":   self.signal_date,
            "patterns":      "+".join(sorted(self.patterns)),
            "entry_date":    self.entry_date,
            "exit_date":     self.exit_date,
            "exit_reason":   self.exit_reason,
            "avg_entry":     round(self.avg_entry, 4),
            "exit_price":    round(self.exit_price or 0, 4),
            "shares":        round(self.shares, 4),
            "deployed":      round(self.deployed, 2),
            "high_water_pct": round(self.high_water * 100, 2),
            "pyramid_steps": self.pyramid_steps,
            "n_fills":       len(self.fills),
            "gross_pnl":     round(self.gross_pnl, 2),
            "fees":          round(self.fees, 2),
            "net_pnl":       round(self.net_pnl, 2),
            "ret_pct_avg":   round(self.net_pnl / self.deployed * 100, 3),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Top-level simulator
# ─────────────────────────────────────────────────────────────────────────────

def simulate(signals: List[Dict], con: sqlite3.Connection,
              starting_capital: float = 30_00_000,
              max_concurrent: int = 30) -> Dict:
    # Build candidate trades — for each signal, get next-day open as entry
    candidates = []
    for s in signals:
        days_after = _trading_days_after(con, s["symbol"], s["signal_date"], 60)
        if not days_after:
            continue
        entry_date = days_after[0]
        op = _open_on(con, s["symbol"], entry_date)
        if op is None or op <= 0: continue
        slip = SLIP_BPS / 10_000.0
        entry_price = op * (1 + slip)
        shares = PER_TRADE / entry_price
        candidates.append({
            "symbol":      s["symbol"],
            "signal_date": s["signal_date"],
            "patterns":    s["patterns"],
            "entry_date":  entry_date,
            "entry_price": entry_price,
            "shares":      shares,
        })

    if not candidates:
        return {"trades": [], "skipped": [], "timeline": [], "metrics": {}}

    by_entry: Dict[str, List[Dict]] = defaultdict(list)
    for c in candidates:
        by_entry[c["entry_date"]].append(c)

    # Sim window — from earliest entry to a generous tail (60 days after latest entry)
    sim_start = min(c["entry_date"] for c in candidates)
    last_entry = max(c["entry_date"] for c in candidates)
    end_window = _trading_days_after(con, candidates[0]["symbol"], last_entry, 60)
    sim_end = end_window[-1] if end_window else last_entry
    days = _all_trading_days(con, sim_start, sim_end)

    cash = starting_capital
    open_positions: List[V5Position] = []
    closed: List[V5Position] = []
    skipped: List[Dict] = []
    timeline: List[Dict] = []
    peak_eq = starting_capital
    max_concurrent_seen = 0
    days_held: Dict[id, int] = {}    # position_id -> days held

    for d in days:
        # 1) Update marks for open positions, evaluate exits
        new_open = []
        for p in open_positions:
            cl = _close_on(con, p.symbol, d)
            hi, lo = _hl_on(con, p.symbol, d)
            if cl is None or hi is None or lo is None:
                # data hole — keep position
                new_open.append(p); continue

            p.update_mark(cl, lo)
            held = days_held.get(id(p), 0) + 1
            days_held[id(p)] = held
            reason = p.should_close(lo, cl, held)
            if reason:
                # exit — use today's close (or stop level if intraday hit)
                exit_px = cl if reason == "time_stop" else min(cl, p.stop_level())
                p.close_position(d, exit_px, reason)
                cash += p.deployed + p.net_pnl
                closed.append(p)
            else:
                new_open.append(p)
        open_positions = new_open

        # 2) Pyramid existing winners
        for p in open_positions:
            cl = _close_on(con, p.symbol, d)
            if cl is None: continue
            cur_ret = cl / p.avg_entry - 1.0
            target = PYRAMID_AT[p.pyramid_steps] if p.pyramid_steps < len(PYRAMID_AT) else None
            if target is None: continue
            if cur_ret >= target:
                add_rupees = PER_TRADE * PYRAMID_FRAC
                if cash >= add_rupees:
                    p.add_pyramid(d, cl, add_rupees)
                    cash -= add_rupees

        # 3) Process new entries
        for c in by_entry.get(d, []):
            if cash < PER_TRADE:
                skipped.append({**c, "reason": "insufficient_cash",
                                  "cash": cash, "open": len(open_positions)})
                continue
            if len(open_positions) >= max_concurrent:
                skipped.append({**c, "reason": "max_concurrent",
                                  "cash": cash, "open": len(open_positions)})
                continue
            cash -= PER_TRADE
            pos = V5Position(c["symbol"], c["signal_date"], c["patterns"],
                              c["entry_date"], c["entry_price"], c["shares"],
                              PER_TRADE)
            open_positions.append(pos)
            days_held[id(pos)] = 0

        # 4) MTM equity for the day
        deployed = sum(p.deployed for p in open_positions)
        unrealized = 0.0
        for p in open_positions:
            cl = _close_on(con, p.symbol, d) or p.avg_entry
            slip = SLIP_BPS / 10_000.0
            mtm = cl * (1 - slip)
            unrealized += p.shares * (mtm - p.avg_entry)
        equity = cash + deployed + unrealized
        if equity > peak_eq: peak_eq = equity
        dd = equity - peak_eq
        max_concurrent_seen = max(max_concurrent_seen, len(open_positions))

        timeline.append({
            "date":         d,
            "cash":         round(cash, 2),
            "deployed":     round(deployed, 2),
            "unrealized":   round(unrealized, 2),
            "equity":       round(equity, 2),
            "n_open":       len(open_positions),
            "drawdown":     round(dd, 2),
            "drawdown_pct": round(dd / peak_eq * 100, 3) if peak_eq > 0 else 0,
            "utilization":  round((deployed + unrealized) / equity, 4) if equity > 0 else 0,
        })

    # Force-close any still-open at end
    for p in open_positions:
        cl = _close_on(con, p.symbol, sim_end) or p.avg_entry
        p.close_position(sim_end, cl, "sim_end")
        cash += p.deployed + p.net_pnl
        closed.append(p)

    # Metrics
    pnls = [p.net_pnl for p in closed]
    wins = [v for v in pnls if v > 0]
    losses = [v for v in pnls if v <= 0]
    final_equity = timeline[-1]["equity"] if timeline else starting_capital
    monthly = defaultdict(float)
    for p in closed: monthly[p.exit_date[:7]] += p.net_pnl
    avg_util = statistics.mean([tl["utilization"] for tl in timeline]) if timeline else 0
    peak_util = max((tl["utilization"] for tl in timeline), default=0)
    avg_dep = statistics.mean([tl["deployed"] for tl in timeline]) if timeline else 0
    avg_idle = statistics.mean([tl["cash"] for tl in timeline]) if timeline else 0
    dd_min = min((tl["drawdown"] for tl in timeline), default=0)
    dd_pct_min = min((tl["drawdown_pct"] for tl in timeline), default=0)
    n_pyramids = sum(1 for p in closed if p.pyramid_steps > 0)
    avg_pyramid_steps = statistics.mean([p.pyramid_steps for p in closed]) if closed else 0

    return {
        "trades": [p.to_record() for p in closed],
        "skipped": skipped,
        "timeline": timeline,
        "metrics": {
            "starting_capital":   starting_capital,
            "ending_equity":      final_equity,
            "total_pnl":          final_equity - starting_capital,
            "return_pct":         (final_equity / starting_capital - 1) * 100,
            "trades_taken":       len(closed),
            "trades_skipped":     len(skipped),
            "n_pyramided":        n_pyramids,
            "avg_pyramid_steps":  round(avg_pyramid_steps, 2),
            "win_rate":           (sum(1 for v in pnls if v > 0)/len(pnls)*100) if pnls else 0,
            "avg_win":            statistics.mean(wins) if wins else 0,
            "avg_loss":           statistics.mean(losses) if losses else 0,
            "win_loss_ratio":     (statistics.mean(wins)/abs(statistics.mean(losses)))
                                    if (wins and losses and statistics.mean(losses) != 0) else None,
            "best_trade":         max(pnls) if pnls else 0,
            "worst_trade":        min(pnls) if pnls else 0,
            "max_drawdown_rupees": dd_min,
            "max_drawdown_pct":    dd_pct_min,
            "max_concurrent":      max_concurrent_seen,
            "avg_utilization":     avg_util,
            "peak_utilization":    peak_util,
            "avg_deployed":        avg_dep,
            "avg_idle_cash":       avg_idle,
            "monthly_pnl":         dict(monthly),
            "exit_reasons":        dict(_count_reasons(closed)),
        }
    }


def _count_reasons(closed):
    from collections import Counter
    return Counter(p.exit_reason for p in closed)
