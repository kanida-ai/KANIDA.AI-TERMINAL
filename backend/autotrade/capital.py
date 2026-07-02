"""CapitalAllocator — three sizing modes + quantity calculation.

Sizing modes (select via TradingSessionConfig.sizing_mode):
  equal    : capital / top_n_stocks per position
  pct_cap  : min(capital / n, capital * max_pct_per_position) per position
  manual   : exact per-symbol ₹ amounts from config.manual_amounts

calculate_quantity fetches live LTP + lot size from the broker at entry time:
  EQ / CNC       : floor(amount / ltp)
  MTF            : floor(amount / margin_per_share)  [leveraged; cash fallback]
  FUT            : floor(amount / margin_per_lot) * lot_size  [margin, long+short;
                   refuses if margin API unavailable — never notional-sizes]
  CE / PE        : floor(amount / (premium * lot_size)) * lot_size
Never returns 0 — raises InsufficientCapitalError when amount < one lot value.
Lot sizes ALWAYS come from the broker instrument master (never hardcoded).
"""
from __future__ import annotations

import logging
import math
from typing import Dict, List, Optional

from .config import TradingSessionConfig

log = logging.getLogger("kanida.autotrade.capital")


class InsufficientCapitalError(Exception):
    """Raised when the allocated amount cannot fund even one share / lot."""


class CapitalAllocator:
    def __init__(self, config: TradingSessionConfig):
        self.config = config

    # ── Per-symbol ₹ allocation ───────────────────────────────────────────────
    def allocate(self, symbols: List[str]) -> Dict[str, float]:
        """Return {symbol: rupee_amount} per the configured sizing mode.

        `symbols` is the routed pick list for ONE broker profile. The caller is
        responsible for ensuring the total respects the profile's allocated
        capital; this method sizes against config.total_allocated_capital, which
        for a single-broker session equals the profile capital.
        """
        cfg = self.config
        n = len(symbols)
        if n == 0:
            return {}

        if cfg.sizing_mode == "equal":
            per = cfg.total_allocated_capital / n
            return {s: per for s in symbols}

        if cfg.sizing_mode == "pct_cap":
            cap = cfg.total_allocated_capital * cfg.max_pct_per_position
            per = min(cfg.total_allocated_capital / n, cap)
            return {s: per for s in symbols}

        if cfg.sizing_mode == "manual":
            # Only symbols present in manual_amounts get funded.
            out = {s: float(cfg.manual_amounts.get(s, 0.0)) for s in symbols}
            return out

        raise ValueError(f"unknown sizing_mode: {cfg.sizing_mode}")

    # ── Batched prefetch + sizing (SPEED PASS) ────────────────────────────────
    def prefetch(self, symbols: List[str], broker) -> Dict[str, Dict[str, float]]:
        """ONE batched LTP fetch + (for MTF) ONE batched margin fetch for the
        whole pick list, returned as {symbol: {"ltp":..,"margin":..}}.

        SPEED PASS: replaces N sequential broker.get_ltp + get_margin_per_share
        round-trips with one call each. EQ/CNC skips the margin probe entirely.
        Missing entries are simply absent → calculate_quantity_cached cash-sizes
        (margin miss) or raises InsufficientCapitalError (no LTP) per symbol, so
        a partial batch NEVER over-deploys. Falls back transparently to per-symbol
        lookups inside calculate_quantity_cached when a value is absent."""
        cfg = self.config
        ltps: Dict[str, float] = {}
        try:
            ltps = broker.get_ltps_batch(list(symbols)) or {}
        except Exception as e:  # pragma: no cover - defensive; per-symbol fallback
            log.warning("batch LTP prefetch failed (%s) — per-symbol fallback", e)
        margins: Dict[str, float] = {}
        use_mtf = (cfg.instrument_type == "MTF") or (cfg.order_product == "MTF")
        if use_mtf and cfg.instrument_type in ("EQ", "MTF"):
            try:
                margins = broker.get_margins_batch(list(symbols), "MTF") or {}
            except Exception as e:  # pragma: no cover
                log.warning("batch margin prefetch failed (%s) — per-symbol "
                            "fallback", e)
        out: Dict[str, Dict[str, float]] = {}
        for s in symbols:
            d: Dict[str, float] = {}
            if s in ltps:
                d["ltp"] = ltps[s]
            if s in margins:
                d["margin"] = margins[s]
            out[s] = d
        return out

    def calculate_quantity_cached(self, symbol: str, amount: float, broker,
                                  cache: Optional[Dict[str, Dict[str, float]]]
                                  = None) -> int:
        """Like calculate_quantity but uses a prefetch() cache for LTP + MTF
        margin so the hot entry path makes ZERO per-symbol broker calls when the
        batch already has the values. For F&O (FUT/CE/PE) it delegates to the
        per-symbol path (those need contract/chain lookups not in the batch).

        SAFETY: identical math + cash-fallback rules as calculate_quantity. A
        cache MISS for a symbol falls back to the per-symbol broker call (never
        silently zero / over-deploy)."""
        cfg = self.config
        itype = cfg.instrument_type
        if itype not in ("EQ", "MTF"):
            # F&O still uses the per-symbol contract/chain path.
            return self.calculate_quantity(symbol, amount, broker)

        c = (cache or {}).get(symbol, {}) if cache else {}
        ltp = c.get("ltp")
        if ltp is None or ltp <= 0:
            ltp = broker.get_ltp(symbol)
        if ltp is None or ltp <= 0:
            raise InsufficientCapitalError(
                f"{symbol}: no valid LTP from broker (got {ltp})")

        use_mtf_margin = (itype == "MTF") or (cfg.order_product == "MTF")
        per_unit = ltp
        if use_mtf_margin:
            mps = c.get("margin")
            if mps is None or mps <= 0:
                # Cache miss — per-symbol probe (never over-deploy on a miss).
                try:
                    mps = broker.get_margin_per_share(symbol, "MTF")
                except Exception as e:  # pragma: no cover
                    log.warning("%s: MTF margin lookup error (%s) — cash fallback",
                                symbol, e)
                    mps = None
            if mps and mps > 0:
                per_unit = mps
            else:
                log.warning("%s: MTF margin unavailable — cash-sizing fallback",
                            symbol)
        qty = math.floor(amount / per_unit)
        if qty < 1:
            raise InsufficientCapitalError(
                f"{symbol}: amount ₹{amount:.0f} < 1 unit at ₹{per_unit:.2f}")
        return qty

    # ── Quantity from broker LTP + lot size ───────────────────────────────────
    def calculate_quantity(self, symbol: str, amount: float, broker) -> int:
        """Compute the integer quantity to order for `amount` rupees.

        `broker` is a BrokerClient. We call broker.get_ltp(symbol) and, for F&O,
        broker.get_lot_size(contract). Never returns 0.
        """
        cfg = self.config
        itype = cfg.instrument_type

        ltp = broker.get_ltp(symbol)
        if ltp is None or ltp <= 0:
            raise InsufficientCapitalError(
                f"{symbol}: no valid LTP from broker (got {ltp})"
            )

        if itype in ("EQ", "MTF"):
            # MTF is LEVERAGED: size off the per-share MARGIN Zerodha locks
            # (qty = amount / margin_per_share), matching the legacy engine and
            # the backtest. Cash equity (EQ/CNC) sizes off LTP. If the margin
            # lookup fails we fall back to cash sizing so we never over-deploy.
            # MTF leverage is driven by the ORDER PRODUCT (MTF), which applies to
            # equity (instrument_type EQ) — not a separate instrument type. Trigger
            # margin-based sizing whenever the product is MTF (or itype==MTF).
            use_mtf_margin = (itype == "MTF") or (cfg.order_product == "MTF")
            per_unit = ltp
            if use_mtf_margin:
                mps = None
                try:
                    mps = broker.get_margin_per_share(symbol, "MTF")
                except Exception as e:  # pragma: no cover
                    log.warning("%s: MTF margin lookup error (%s) — cash fallback", symbol, e)
                if mps and mps > 0:
                    per_unit = mps
                else:
                    log.warning("%s: MTF margin unavailable — cash-sizing fallback", symbol)
            qty = math.floor(amount / per_unit)
            if qty < 1:
                raise InsufficientCapitalError(
                    f"{symbol}: amount ₹{amount:.0f} < 1 unit at ₹{per_unit:.2f}"
                )
            return qty

        if itype == "FUT":
            contract = broker.get_active_futures(symbol, cfg.expiry_preference)
            lot_size = broker.get_lot_size(contract)
            if lot_size <= 0:
                raise InsufficientCapitalError(f"{symbol}: invalid lot_size {lot_size}")
            # RETAIL FUTURES SIZING (long AND short): size lots on the per-LOT
            # MARGIN the broker locks, NOT the full notional (ltp*lot). Sizing on
            # notional would UNDER-buy ~5-10x for a leveraged product. Fall back
            # SAFELY: if the margin lookup is unavailable we refuse rather than
            # silently notional-size (never over-size). direction does NOT affect
            # sizing — a short lot needs the same initial margin as a long lot.
            margin_per_lot = None
            try:
                margin_per_lot = broker.get_fut_margin_per_lot(
                    symbol, cfg.expiry_preference)
            except Exception as e:  # pragma: no cover - defensive
                log.warning("%s: FUT margin lookup error (%s)", symbol, e)
                margin_per_lot = None
            if not margin_per_lot or margin_per_lot <= 0:
                raise InsufficientCapitalError(
                    f"{symbol}: FUT per-lot margin unavailable — refusing to "
                    "size on notional (would over-deploy). Retry when the "
                    "broker margin API is reachable.")
            lots = math.floor(amount / margin_per_lot)
            if lots < 1:
                raise InsufficientCapitalError(
                    f"{symbol}: amount ₹{amount:.0f} < 1 FUT lot margin "
                    f"(₹{margin_per_lot:.0f}/lot)"
                )
            return lots * lot_size

        if itype in ("CE", "PE"):
            chain = broker.get_option_chain(symbol)
            strike = _select_atm_strike(chain, itype, ltp)
            contract = broker.get_option_contract(symbol, strike, cfg.expiry_preference)
            lot_size = broker.get_lot_size(contract)
            premium = broker.get_ltp(contract)
            if premium is None or premium <= 0 or lot_size <= 0:
                raise InsufficientCapitalError(
                    f"{symbol}: invalid option premium/lot ({premium}/{lot_size})"
                )
            lots = math.floor(amount / (premium * lot_size))
            if lots < 1:
                raise InsufficientCapitalError(
                    f"{symbol}: amount ₹{amount:.0f} < 1 option lot "
                    f"(₹{premium * lot_size:.0f}/lot)"
                )
            return lots * lot_size

        raise ValueError(f"unknown instrument_type: {itype}")


def _select_atm_strike(chain, option_type: str, spot: float) -> float:
    """Pick the strike nearest spot. `chain` is broker-shaped: a list of dicts
    with a 'strike' key, or a list of floats."""
    if not chain:
        raise InsufficientCapitalError("empty option chain")
    strikes = []
    for c in chain:
        if isinstance(c, dict):
            strikes.append(float(c["strike"]))
        else:
            strikes.append(float(c))
    return min(strikes, key=lambda k: abs(k - spot))
