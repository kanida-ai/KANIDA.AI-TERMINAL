"""Build per-stock order plan from selected signals + config.

Pure-deterministic given inputs (the margin lookup itself is impure but is
injected as a callable so the planner stays testable).

Sizing rule (operator preference: PER_TRADE = "₹X of my own cash"):
  * MTF: qty = floor(PER_TRADE / per_share_margin) — Zerodha decides margin %
         per stock, we ask via kite.order_margins. Backtest assumed full
         MTF deployment, so this is what makes live match backtest.
  * CNC: qty = floor(PER_TRADE / close) — full cash cost = qty × LTP
  * Margin lookup failure → fall back to cash sizing for that symbol with
    `margin_lookup_failed=True` so the UI can flag it amber.

No leverage assumption baked in. Broker decides actual margin at placement;
we ask the broker upfront.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from .volatility_gate import round_to_tick, sl_limit_price


@dataclass
class OrderSpec:
    rank:           int
    symbol:         str
    sector:         Optional[str]
    close:          float
    qty:            int
    notional:       float
    sl_price:       float
    target_price:   float
    sl_order_type:  str                 # "SL-L" or "SL-M"
    sl_limit_price: Optional[float]     # only meaningful for SL-L
    is_averaging:   bool
    existing_qty:   int
    # Phase 2.2: margin-aware sizing for MTF. Defaulted so old preview dicts
    # (without these fields) can still be re-hydrated via OrderSpec(**d) — e.g.
    # in /smoke-test which reads the previously-stored preview from memory.
    product:              str               = "MTF"     # "MTF" or "CNC"
    margin_per_share:     Optional[float]   = None      # None for CNC or on lookup failure
    margin_required:      float             = 0.0       # qty × margin_per_share or qty × close
    margin_pct:           Optional[float]   = None      # margin_per_share / close × 100
    effective_leverage:   Optional[float]   = None      # notional / margin_required
    margin_lookup_failed: bool              = False     # True → cash-sized fallback

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def plan_orders(
    *,
    signals:           List[dict],
    selected_symbols:  List[str],
    per_trade:         int,
    sl_pct:            float,
    trail_trigger_pct: float,
    held_by_symbol:    Dict[str, dict],
    hold_actions:      Dict[str, str],
    mtf_eligible_fn:   Callable[[str], bool],
    sl_order_type_fn:  Callable[[str], str],
    margin_fn:         Optional[Callable[[List[Tuple[str, str]]], Dict[str, float]]] = None,
    tick_size_fn:      Optional[Callable[[str], float]] = None,
    default_product:   str = "MTF",
) -> Tuple[List[OrderSpec], List[Dict[str, str]], Dict[str, int]]:
    """Returns (orders, skipped, action_counts).

    `margin_fn` takes a batch of (symbol, product) pairs and returns
    {symbol: per_share_margin}. Symbols absent from the result are treated
    as lookup failures (cash-sized fallback). When None, all orders are
    cash-sized (legacy behaviour — used by tests).
    """
    sig_by_symbol = {s.get("symbol"): s for s in signals}
    eligible: List[dict] = []
    skipped:  List[Dict[str, str]] = []

    for sym in selected_symbols:
        s = sig_by_symbol.get(sym)
        if s is None:
            skipped.append({"symbol": sym, "reason": "NOT_IN_SIGNALS"})
            continue
        if default_product == "MTF" and not mtf_eligible_fn(sym):
            skipped.append({"symbol": sym, "reason": "MTF_INELIGIBLE"})
            continue
        eligible.append(s)

    # Pre-fetch margins for ALL eligible MTF symbols in one Kite call.
    # CNC orders skip the lookup entirely.
    margin_map: Dict[str, float] = {}
    if margin_fn is not None and default_product == "MTF":
        try:
            margin_map = margin_fn([(s["symbol"], default_product) for s in eligible])
        except Exception:
            margin_map = {}

    orders: List[OrderSpec] = []
    n_skip_action = 0
    n_average_action = 0

    for s in eligible:
        sym = s["symbol"]
        held = held_by_symbol.get(sym)
        if held:
            action = hold_actions.get(sym, "skip")
            if action == "skip":
                n_skip_action += 1
                continue
            n_average_action += 1

        close = s.get("close_at_signal")
        if close is None or close <= 0:
            skipped.append({"symbol": sym, "reason": "NO_PRICE"})
            continue

        # Sizing — branch on product.
        product = default_product
        margin_per_share: Optional[float] = None
        margin_lookup_failed = False
        if product == "MTF":
            margin_per_share = margin_map.get(sym)
            if margin_per_share and margin_per_share > 0:
                qty = int(per_trade // margin_per_share)
            else:
                # Fallback: cash-size (qty = PER_TRADE / LTP). Flag for UI.
                qty = int(per_trade // close)
                margin_lookup_failed = True
        else:
            # CNC: cash sizing.
            qty = int(per_trade // close)

        if qty <= 0:
            skipped.append({"symbol": sym, "reason": "POSITION_TOO_SMALL"})
            continue

        notional = qty * close
        # margin_required:
        #   MTF (good lookup) = qty × margin_per_share ≈ per_trade
        #   MTF (fallback)    = qty × close            (cash equivalent)
        #   CNC               = qty × close            (full cost = notional)
        if product == "MTF" and margin_per_share and not margin_lookup_failed:
            margin_required = round(qty * margin_per_share, 2)
            margin_pct      = round((margin_per_share / close) * 100, 2)
            eff_lev         = round(notional / margin_required, 2) if margin_required > 0 else None
        else:
            margin_required = notional
            margin_pct      = None
            eff_lev         = None

        # Per-symbol tick rounding. Many NSE scrips have tick=0.10 (large-cap or
        # high-priced names like ADANIENSOL, DATAPATTNS, AFFLE, CCL) instead of
        # the 0.05 default — Zerodha rejects SL trigger prices that aren't a
        # multiple of the actual tick. Fall back to 0.05 if lookup fails.
        try:
            tick = tick_size_fn(sym) if tick_size_fn else 0.05
            if not tick or tick <= 0:
                tick = 0.05
        except Exception:
            tick = 0.05
        sl_price     = round_to_tick(close * (1 + sl_pct / 100), tick=tick)
        target_price = round_to_tick(close * (1 + trail_trigger_pct / 100), tick=tick)
        slt          = sl_order_type_fn(sym)
        sl_lim       = sl_limit_price(sl_price, tick=tick) if slt == "SL-L" else None

        orders.append(OrderSpec(
            rank=len(orders) + 1,
            symbol=sym,
            sector=s.get("sector"),
            close=close,
            qty=qty,
            notional=notional,
            sl_price=sl_price,
            target_price=target_price,
            sl_order_type=slt,
            sl_limit_price=sl_lim,
            is_averaging=bool(held),
            existing_qty=held["qty"] if held else 0,
            product=product,
            margin_per_share=margin_per_share,
            margin_required=margin_required,
            margin_pct=margin_pct,
            effective_leverage=eff_lev,
            margin_lookup_failed=margin_lookup_failed,
        ))

    return orders, skipped, {
        "n_skip_action":    n_skip_action,
        "n_average_action": n_average_action,
    }


def cheapest(orders: List[OrderSpec]) -> Optional[OrderSpec]:
    if not orders:
        return None
    return min(orders, key=lambda o: o.close)
