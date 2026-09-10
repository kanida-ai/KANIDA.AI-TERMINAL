"""
The virtual book — virtual capital deployed to a rule over a period, marked deterministically.

A port of P1 `engine/book.py`'s conventions onto S1's sealed long frame (so the book trades
exactly what the S1 card measured and what `hypotheses.replay` replays):

  * entry = the OPEN of the session after the signal close; exit = the CLOSE of the horizon
    session; costs + slippage charged to every closed trade, winners and losers alike;
  * the Constitution's limits: capital, a fixed fraction per position, a maximum number of
    concurrent positions, one position per symbol, a maximum of new positions per session;
  * when more signals fire than there are slots they are ranked by an explicit, point-in-time
    selection rule — liquidity (rolling median traded value as of the signal day), descending;
  * the equity curve is marked to the CLOSE every session so the drawdown is the one a person
    would have lived through;
  * the walk never sees a bar past the frame's seal: a position whose exit lies past it stays
    OPEN and is marked at the last close. Re-walking the same period on a later seal yields the
    same entries and the same closed trades (pinned by tests) — the book is a pure function of
    (rule, period, seal).

No stop. The S1 expectation was measured on a time-boxed exit with no price stop; a stop would
make the forward test a different strategy from the one history was measured on (quant rule:
the evidence must be the same strategy that is traded). The Constitution draft's
`hard_stop_required` was written for P1's rulebooks; this conflict is flagged in the hand-back.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

from ..research.config import ResearchConfig
from ..research.data import MarketData
from .hypotheses import Conditioning, Variant

@dataclass(frozen=True)
class VTrade:
    symbol: str
    direction: str
    signal_date: str
    entry_date: str
    entry_price: float
    exit_date: Optional[str]
    exit_price: Optional[float]
    holding_sessions: Optional[int]
    pnl_pct_gross: Optional[float]
    pnl_pct_net: Optional[float]
    costs_pct: float
    notional_inr: float
    #: Audit finding 7: a trade whose window holds a glitch / corporate-action bar, or whose exit
    #: bar is missing, is CLOSED (the book cannot hold a phantom) but UNRESOLVED — it is listed,
    #: never graded, exactly as the evidence convention (`data.py` `f{h}` = NaN across a hole) treats
    #: the same case. The evidence and the traded strategy stay the same strategy.
    resolved: bool = True
    unresolved_reason: Optional[str] = None

    @property
    def closed(self) -> bool:
        return self.exit_date is not None


@dataclass(frozen=True)
class BookRun:
    capital_inr: float
    period_start: str
    period_end: Optional[str]           # None while the period's last signal session lies past the seal
    walked_to: str                      # last session marked
    closed: tuple[VTrade, ...]          # exit order
    open_at_end: tuple[VTrade, ...]
    equity: pd.Series                   # session -> INR, marked to close
    signals_seen: int
    signals_taken: int
    selection_rule: str = "liquidity_desc"

    @property
    def graded(self) -> tuple[VTrade, ...]:
        """Closed AND resolved: the trades a verdict may rest on."""
        return tuple(t for t in self.closed if t.resolved)

    @property
    def unresolved(self) -> tuple[VTrade, ...]:
        return tuple(t for t in self.closed if not t.resolved)

    @property
    def ledger_losers_first(self) -> tuple[VTrade, ...]:
        return tuple(sorted(self.closed, key=lambda t: t.pnl_pct_net))

    @property
    def equity_pct(self) -> np.ndarray:
        if self.equity.empty:
            return np.array([], dtype=float)
        return (self.equity.to_numpy(float) / self.capital_inr - 1.0) * 100.0

    def drawdowns(self) -> tuple[float, float]:
        c = self.equity_pct
        if c.size == 0:
            return 0.0, 0.0
        peak = np.maximum.accumulate(np.r_[0.0, c])[1:]
        dd = peak - c
        return float(dd.max()), float(dd[-1])

    @property
    def total_return_pct(self) -> Optional[float]:
        c = self.equity_pct
        return float(c[-1]) if c.size else None


def run_book(variant: Variant, md: MarketData, cx: Conditioning, *, period_start: str, period_sessions: int,
             rcfg: ResearchConfig, capital_inr: float, fraction_per_position: float,
             max_concurrent: int, max_new_per_session: int) -> BookRun:
    """
    Walk the sealed frame from `period_start` for `period_sessions` SIGNAL sessions, then on
    through the horizon until every position has exited or the seal is reached.
    """
    sessions = md.sessions
    if period_start not in sessions:
        # the period has not started inside this seal
        return BookRun(capital_inr, period_start, None, md.as_of, (), (), pd.Series(dtype=float), 0, 0)
    i0 = sessions.index(period_start)
    i_last_signal = min(i0 + period_sessions - 1, len(sessions) - 1)
    period_end = sessions[i0 + period_sessions - 1] if i0 + period_sessions - 1 < len(sessions) else None
    h = variant.horizon
    df = md.df
    sig_rows = variant.signals(md, cx)
    liq = cx.liquidity
    by_day = {d: g for d, g in df[sig_rows & (df["d"] >= period_start) & (df["d"] <= sessions[i_last_signal])]
              .assign(_liq=liq).groupby("d")}
    # price lookups
    px = df.set_index(["symbol", "d"])[["open", "close", "_synth_open", "_bad"]]
    open_px = px["open"]
    close_px = px["close"]
    synth = px["_synth_open"]
    bad = px["_bad"]

    per_position = capital_inr * fraction_per_position
    cash = capital_inr
    open_pos: dict[str, dict] = {}
    closed: list[VTrade] = []
    eq_idx: list[str] = []
    eq_val: list[float] = []
    seen = taken = 0
    H = rcfg.hurdle_pct
    sign = variant.sign
    i = i0
    while i < len(sessions):
        today = sessions[i]
        # 1. close whatever exits today
        for sym in [s for s, p in open_pos.items() if p["exit_i"] == i]:
            p = open_pos.pop(sym)
            c = close_px.get((sym, today), np.nan)
            reason: Optional[str] = None
            if not np.isfinite(c) or c <= 0:
                c = p["last_px"]        # a hole on the exit bar: the last verifiable close closes it, UNRESOLVED
                reason = "no verifiable close on the exit session"
            # a glitch / corporate-action bar inside (entry, exit] — the evidence convention's NaN
            if reason is None and any(int(bad.get((sym, sessions[k]), 0)) == 1 for k in range(p["entry_i"], i + 1)):
                reason = "a glitch or corporate-action bar inside the holding window"
            gross = sign * (c / p["entry"] - 1.0) * 100.0
            net = gross - H
            cash += p["qty"] * (1.0 + sign * (c / p["entry"] - 1.0)) - p["qty"] * H / 100.0
            closed.append(VTrade(sym, variant.direction, p["signal"], p["entry_d"], p["entry"], today, float(c),
                                 int(i - p["entry_i"] + 1), float(gross), float(net), H, p["qty"],
                                 resolved=(reason is None), unresolved_reason=reason))
        # 2. mark to today's close
        marked = 0.0
        for sym, p in open_pos.items():
            c = close_px.get((sym, today), np.nan)
            if np.isfinite(c) and c > 0:
                p["last_px"] = float(c)
            marked += p["qty"] * (1.0 + sign * (p["last_px"] / p["entry"] - 1.0))
        eq_idx.append(today)
        eq_val.append(cash + marked)
        # 3. open from today's signals — filled at TOMORROW's open, which must be inside the seal.
        #    A signal on the seal day is SEEN (counted) but cannot be filled yet (audit finding 14).
        if i <= i_last_signal and today in by_day:
            g = by_day[today].sort_values(["_liq", "symbol"], ascending=[False, True], kind="mergesort")
            seen += len(g)
            opened = 0
            for sym in (g["symbol"].tolist() if i + 1 < len(sessions) else []):
                nxt = sessions[i + 1]
                if len(open_pos) >= max_concurrent or opened >= max_new_per_session or sym in open_pos:
                    continue
                if cash < per_position:
                    continue
                o = open_px.get((sym, nxt), np.nan)
                if not np.isfinite(o) or o <= 0 or int(synth.get((sym, nxt), 1)) == 1:
                    continue            # no verifiable entry price (A7): not a trade
                cash -= per_position
                open_pos[sym] = {"entry": float(o), "entry_d": nxt, "entry_i": i + 1, "exit_i": i + h,
                                 "qty": per_position, "last_px": float(o), "signal": today}
                opened += 1
                taken += 1
        i += 1
        if i > i_last_signal and not open_pos:
            break
    open_at_end = tuple(VTrade(s, variant.direction, p["signal"], p["entry_d"], p["entry"], None, None, None, None,
                               None, H, p["qty"], resolved=False, unresolved_reason="open at the seal")
                        for s, p in sorted(open_pos.items()))
    return BookRun(capital_inr, period_start, period_end, sessions[min(i, len(sessions)) - 1] if eq_idx else md.as_of,
                   tuple(closed), open_at_end, pd.Series(eq_val, index=eq_idx, dtype=float), seen, taken)


def passive_incumbent(md: MarketData, *, start: str, end: str, rcfg: ResearchConfig,
                      capital_inr: float) -> Optional[BookRun]:
    """
    The INCUMBENT a challenger must beat when no graduated champion exists: the same capital in
    the whole universe, equal weight, from the first open of the window to the last close, costs
    and slippage charged once. It carries the same survivorship and drift as the challenger.
    """
    sessions = [d for d in md.sessions if start <= d <= end]
    if len(sessions) < 2:
        return None
    df = md.df[md.df["d"].isin(sessions)]
    wide_o = df.pivot_table(index="d", columns="symbol", values="open")
    wide_c = df.pivot_table(index="d", columns="symbol", values="close")
    o0 = wide_o.iloc[0]
    ok = o0.notna() & (o0 > 0)
    rel = wide_c.loc[:, ok].div(o0[ok], axis=1)
    curve = rel.mean(axis=1, skipna=True)
    eq = capital_inr * curve
    eq.iloc[-1] = eq.iloc[-1] - capital_inr * rcfg.hurdle_pct / 100.0
    t = VTrade("UNIVERSE_EW", "long", sessions[0], sessions[0], 1.0, sessions[-1], float(curve.iloc[-1]),
               len(sessions), float((curve.iloc[-1] - 1) * 100), float((curve.iloc[-1] - 1) * 100 - rcfg.hurdle_pct),
               rcfg.hurdle_pct, capital_inr)
    return BookRun(capital_inr, sessions[0], sessions[-1], sessions[-1], (t,), (), eq.astype(float), 1, 1, "equal_weight_hold")
