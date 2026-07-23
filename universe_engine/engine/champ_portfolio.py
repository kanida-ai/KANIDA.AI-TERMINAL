"""
Concentrated portfolio with add-to-winners pyramiding.

Sizing logic (per the brief):
  - Initial entry: equal-weight slice of capital across top-N picks (N in [3,8])
  - When a position reaches +5% from average entry: add 0.5x base size
  - When it reaches +10%: add another 0.5x  → total can reach 2x base size
  - Cuts on stop-loss or trailing stop NEVER add (one-way pyramid)

Position state tracks: shares, avg entry, peak unrealised return, add steps fired,
and which add steps remain.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class Position:
    symbol: str
    open_date: str
    avg_entry: float
    shares: float
    capital_deployed: float           # rupees
    base_size: float                  # rupees of initial entry; adds are fractions of this
    high_water: float = 0.0           # highest unrealised pct since open
    add_steps_fired: int = 0          # number of pyramid adds done (max 2)
    fills: List[dict] = field(default_factory=list)   # log of every fill (entry + adds)
    closed: bool = False
    close_date: Optional[str] = None
    close_price: Optional[float] = None
    close_reason: Optional[str] = None

    def mark(self, last_price: float) -> float:
        if self.avg_entry <= 0:
            return 0.0
        ret = (last_price / self.avg_entry) - 1.0
        if ret > self.high_water:
            self.high_water = ret
        return ret

    def add_fill(self, on_date: str, price: float, rupees: float):
        new_shares = rupees / price if price > 0 else 0
        total_cost = self.capital_deployed + rupees
        total_shares = self.shares + new_shares
        self.shares = total_shares
        self.capital_deployed = total_cost
        self.avg_entry = total_cost / total_shares if total_shares > 0 else self.avg_entry
        self.fills.append({"date": on_date, "price": round(price, 4),
                            "rupees": round(rupees, 2),
                            "shares": round(new_shares, 4),
                            "kind": "entry" if not self.fills else f"add{self.add_steps_fired}"})

    def should_pyramid(self, last_price: float) -> Optional[float]:
        """Returns rupees to add (or None) based on add-to-winners rules."""
        ret = self.mark(last_price)
        if self.add_steps_fired == 0 and ret >= 0.05:
            return self.base_size * 0.5
        if self.add_steps_fired == 1 and ret >= 0.10:
            return self.base_size * 0.5
        return None

    def value(self, last_price: float) -> float:
        return self.shares * last_price

    def pnl(self, last_price: float) -> float:
        return self.value(last_price) - self.capital_deployed

    def pnl_pct(self, last_price: float) -> float:
        if self.capital_deployed <= 0:
            return 0.0
        return self.pnl(last_price) / self.capital_deployed


@dataclass
class Portfolio:
    starting_capital: float
    cash: float
    cost_bps: float = 30.0           # round-trip cost in bps
    slippage_bps: float = 5.0        # one-side
    positions: Dict[str, Position] = field(default_factory=dict)
    closed_positions: List[Position] = field(default_factory=list)
    equity_curve: List[dict] = field(default_factory=list)
    peak_equity: float = 0.0
    max_drawdown: float = 0.0

    def __init__(self, starting_capital: float = 1_000_000.0,
                 cost_bps: float = 30.0, slippage_bps: float = 5.0):
        self.starting_capital = starting_capital
        self.cash = starting_capital
        self.cost_bps = cost_bps
        self.slippage_bps = slippage_bps
        self.positions = {}
        self.closed_positions = []
        self.equity_curve = []
        self.peak_equity = starting_capital
        self.max_drawdown = 0.0
        # last-known price per symbol — used to value positions on days where
        # the bar is missing (holiday gaps, illiquid stocks) so equity doesn't
        # spuriously drop and create false drawdowns.
        self._last_known: Dict[str, float] = {}

    # ── Cost model ────────────────────────────────────────────────────────
    def _exec_price(self, ref_price: float, side: str) -> float:
        """side='buy' adds slippage_bps, 'sell' subtracts. Cost_bps applied separately as fee."""
        bp = self.slippage_bps / 10_000.0
        return ref_price * (1 + bp) if side == "buy" else ref_price * (1 - bp)

    def _fee(self, rupees: float) -> float:
        # Round-trip fee split half on entry, half on exit
        return rupees * (self.cost_bps / 2 / 10_000.0)

    # ── Open / Add / Close ────────────────────────────────────────────────
    def open(self, symbol: str, on_date: str, ref_price: float, base_size: float) -> Optional[Position]:
        if symbol in self.positions:
            return None
        if self.cash < base_size + self._fee(base_size):
            return None
        px = self._exec_price(ref_price, "buy")
        fee = self._fee(base_size)
        self.cash -= (base_size + fee)
        pos = Position(symbol=symbol, open_date=on_date,
                        avg_entry=px, shares=0, capital_deployed=0,
                        base_size=base_size)
        pos.add_fill(on_date, px, base_size)
        self.positions[symbol] = pos
        return pos

    def pyramid(self, symbol: str, on_date: str, ref_price: float, rupees: float):
        pos = self.positions.get(symbol)
        if not pos: return False
        if self.cash < rupees + self._fee(rupees):
            return False
        px = self._exec_price(ref_price, "buy")
        fee = self._fee(rupees)
        self.cash -= (rupees + fee)
        pos.add_fill(on_date, px, rupees)
        pos.add_steps_fired += 1
        return True

    def close(self, symbol: str, on_date: str, ref_price: float, reason: str):
        pos = self.positions.pop(symbol, None)
        if not pos: return None
        px = self._exec_price(ref_price, "sell")
        proceeds = pos.shares * px
        fee = self._fee(proceeds)
        self.cash += (proceeds - fee)
        pos.closed = True
        pos.close_date = on_date
        pos.close_price = px
        pos.close_reason = reason
        self.closed_positions.append(pos)
        return pos

    # ── End-of-day mark + drawdown tracking ──────────────────────────────
    def mark_to_market(self, on_date: str, prices: Dict[str, float]):
        equity = self.cash
        for sym, pos in self.positions.items():
            px = prices.get(sym)
            if px is not None:
                self._last_known[sym] = px
                pos.mark(px)
            else:
                px = self._last_known.get(sym, pos.avg_entry)
            equity += pos.value(px)
        if equity > self.peak_equity:
            self.peak_equity = equity
        dd = (equity - self.peak_equity) / self.peak_equity if self.peak_equity > 0 else 0
        if dd < self.max_drawdown:
            self.max_drawdown = dd
        self.equity_curve.append({"date": on_date, "equity": equity,
                                   "cash": self.cash, "n_positions": len(self.positions),
                                   "drawdown": dd})
        return equity

    # ── Reporting ────────────────────────────────────────────────────────
    def summary(self) -> dict:
        if not self.equity_curve:
            return {"return_pct": 0, "max_dd": 0, "n_trades": 0}
        final = self.equity_curve[-1]["equity"]
        return {
            "return_pct": (final / self.starting_capital - 1) * 100,
            "max_dd_pct": self.max_drawdown * 100,
            "n_trades":   len(self.closed_positions) + len(self.positions),
            "n_closed":   len(self.closed_positions),
            "n_open":     len(self.positions),
            "final_equity": final,
            "starting_capital": self.starting_capital,
        }
