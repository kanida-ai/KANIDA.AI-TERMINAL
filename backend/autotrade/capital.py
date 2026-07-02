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
from typing import Any, Dict, List, Optional

from .config import TradingSessionConfig

log = logging.getLogger("kanida.autotrade.capital")


class InsufficientCapitalError(Exception):
    """Raised when the allocated amount cannot fund even one share / lot."""


def _margin_product(cfg) -> Optional[str]:
    """The broker margin PRODUCT to size equity quantity against, or None to
    cash-size on LTP.

    MTF  → "MTF" (delivery leverage; UNCHANGED — matches the legacy engine).
    MIS  → "MIS" (intraday leverage; FEATURE C — previously cash-sized).
    Anything else (CNC / NRML) → None (cash-size on LTP, unchanged)."""
    itype = getattr(cfg, "instrument_type", "EQ")
    product = str(getattr(cfg, "order_product", "") or "").upper()
    if itype == "MTF" or product == "MTF":
        return "MTF"
    if product == "MIS":
        return "MIS"
    return None


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
        # MTF and MIS both size off broker MARGIN (leverage), not full cash. The
        # margin PRODUCT differs (MTF carry vs MIS intraday) so probe the right one.
        margin_product = _margin_product(cfg)
        if margin_product and cfg.instrument_type in ("EQ", "MTF"):
            try:
                margins = broker.get_margins_batch(list(symbols),
                                                   margin_product) or {}
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

        margin_product = _margin_product(cfg)
        per_unit = ltp
        if margin_product:
            mps = c.get("margin")
            if mps is None or mps <= 0:
                # Cache miss — per-symbol probe (never over-deploy on a miss).
                try:
                    mps = broker.get_margin_per_share(symbol, margin_product)
                except Exception as e:  # pragma: no cover
                    log.warning("%s: %s margin lookup error (%s) — cash fallback",
                                symbol, margin_product, e)
                    mps = None
            if mps and mps > 0:
                per_unit = mps
            else:
                log.warning("%s: %s margin unavailable — cash-sizing fallback",
                            symbol, margin_product)
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
            # MTF (delivery) AND MIS (intraday) both leverage — size off the
            # broker per-share MARGIN for the right product. CNC/NRML cash-size.
            margin_product = _margin_product(cfg)
            per_unit = ltp
            if margin_product:
                mps = None
                try:
                    mps = broker.get_margin_per_share(symbol, margin_product)
                except Exception as e:  # pragma: no cover
                    log.warning("%s: %s margin lookup error (%s) — cash fallback",
                                symbol, margin_product, e)
                if mps and mps > 0:
                    per_unit = mps
                else:
                    log.warning("%s: %s margin unavailable — cash-sizing fallback",
                                symbol, margin_product)
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

    # ── Unit economics (budget + qty per one tradeable increment) ─────────────
    def _unit_info(self, symbol: str, broker,
                   cache: Optional[Dict[str, Dict[str, float]]] = None
                   ) -> Optional[Dict[str, Any]]:
        """Return {"unit_budget", "unit_qty"} for ONE tradeable increment of
        `symbol`, or None when the broker gives no usable price/margin (caller
        SKIPS — never fabricates, never over-deploys).

          unit_budget = ₹ of the capital BUDGET consumed by one increment
                        (margin_per_share for MTF/MIS, LTP for cash EQ;
                         margin_per_lot for FUT; premium*lot for options).
          unit_qty    = the QUANTITY (shares) one increment adds
                        (1 for EQ/MTF/MIS; the broker lot_size for FUT/options).

        These match calculate_quantity(_cached) exactly (qty = floor(amount /
        unit_budget) * unit_qty), so plan_quantities rounds identically."""
        cfg = self.config
        itype = cfg.instrument_type
        c = (cache or {}).get(symbol, {}) if cache else {}
        if itype in ("EQ", "MTF"):
            ltp = c.get("ltp")
            if ltp is None or ltp <= 0:
                try:
                    ltp = broker.get_ltp(symbol)
                except Exception:  # pragma: no cover - defensive
                    ltp = None
            if not ltp or ltp <= 0:
                return None
            per_unit = float(ltp)
            margin_product = _margin_product(cfg)
            if margin_product:
                mps = c.get("margin")
                if mps is None or mps <= 0:
                    try:
                        mps = broker.get_margin_per_share(symbol, margin_product)
                    except Exception:  # pragma: no cover
                        mps = None
                if mps and mps > 0:
                    per_unit = float(mps)
                # else: cash-size fallback (per_unit stays = LTP) — never over-deploy
            return {"unit_budget": per_unit, "unit_qty": 1}
        if itype == "FUT":
            try:
                contract = broker.get_active_futures(symbol, cfg.expiry_preference)
                lot_size = int(broker.get_lot_size(contract))
                mpl = broker.get_fut_margin_per_lot(symbol, cfg.expiry_preference)
            except Exception:  # pragma: no cover - defensive
                return None
            if not mpl or mpl <= 0 or lot_size <= 0:
                return None  # refuse to notional-size (matches calculate_quantity)
            return {"unit_budget": float(mpl), "unit_qty": lot_size}
        if itype in ("CE", "PE"):
            try:
                ltp = broker.get_ltp(symbol)
                chain = broker.get_option_chain(symbol)
                strike = _select_atm_strike(chain, itype, ltp)
                contract = broker.get_option_contract(
                    symbol, strike, cfg.expiry_preference)
                lot_size = int(broker.get_lot_size(contract))
                premium = broker.get_ltp(contract)
            except Exception:  # pragma: no cover - defensive
                return None
            if not premium or premium <= 0 or lot_size <= 0:
                return None
            return {"unit_budget": float(premium) * lot_size, "unit_qty": lot_size}
        return None

    # ── Whole-portfolio quantity plan (FEATURE C) ─────────────────────────────
    def plan_quantities(self, symbols: List[str], broker,
                        cache: Optional[Dict[str, Dict[str, float]]] = None
                        ) -> Dict[str, Any]:
        """Size the WHOLE pick list with stranded-cash redistribution +
        unaffordable-pick skipping. Returns:
            {"quantities": {symbol: qty>0}, "skipped": [{symbol, reason}],
             "deployed": <Σ increments*unit_budget>, "remainder": <unspent>}

        Algorithm (deterministic + capped, never over-deploys):
          1. allocate() the per-symbol ₹ slice (equal / pct_cap / manual).
          2. Per symbol probe unit economics (unit_budget = ₹/increment,
             unit_qty = shares/increment). A symbol whose ONE increment's budget
             costs MORE than its slice is SKIPPED + logged; its whole slice is
             freed into the redistribution pool (never stranded). A symbol with no
             usable price/margin is skipped too.
          3. Floor each affordable pick to whole increments within its slice.
          4. remainder = budget_cap - Σ(deployed). While remainder can buy one
             more increment of the CHEAPEST affordable pick, buy it (cheapest
             unit_budget first, tie-break by symbol). Stops when the remainder is
             smaller than the cheapest increment OR the budget would be exceeded —
             so we NEVER over-deploy.

        redistribute_unused_capital=False → skip step 4 entirely (plain floor of
        each slice = today's behaviour). Unaffordable picks are STILL skipped (an
        unaffordable slice was a hard skip before too — it raised
        InsufficientCapitalError in calculate_quantity). manual sizing_mode uses
        the exact per-symbol amounts and does NOT redistribute across symbols
        (manual intent is explicit)."""
        cfg = self.config
        amounts = self.allocate(list(symbols))
        # Budget ceiling: manual mode caps at Σ(manual_amounts); every other mode
        # caps at the total allocated capital. Redistribution/top-up is bounded by
        # this so we can never exceed the operator's committed capital.
        if cfg.sizing_mode == "manual":
            budget_cap = float(sum(amounts.values()))
            allow_redistribute = False  # explicit per-symbol intent — do not shuffle
        else:
            budget_cap = float(cfg.total_allocated_capital)
            allow_redistribute = bool(
                getattr(cfg, "redistribute_unused_capital", True))

        quantities: Dict[str, int] = {}
        skipped: List[Dict[str, Any]] = []
        units: Dict[str, Dict[str, Any]] = {}   # symbol -> {unit_budget, unit_qty}
        deployed = 0.0

        for sym in symbols:
            slice_amt = float(amounts.get(sym, 0.0))
            if slice_amt <= 0:
                continue  # unfunded (e.g. manual with no amount) — not "skipped"
            info = self._unit_info(sym, broker, cache=cache)
            if info is None:
                skipped.append({"symbol": sym,
                                "reason": "no usable price/margin from broker"})
                log.warning("plan: %s skipped — no usable price/margin", sym)
                continue
            unit_budget = float(info["unit_budget"])
            unit_qty = int(info["unit_qty"])
            if unit_budget > slice_amt + 1e-6:
                # Unaffordable: one increment's budget > this pick's slice. SKIP +
                # free the whole slice into the redistribution pool (never strand).
                skipped.append({
                    "symbol": sym,
                    "reason": (f"1 unit ₹{unit_budget:.0f} > slice "
                               f"₹{slice_amt:.0f} (too high-priced for slice)")})
                log.info("plan: %s skipped — 1 unit ₹%.0f > slice ₹%.0f",
                         sym, unit_budget, slice_amt)
                continue
            n_inc = math.floor(slice_amt / unit_budget)
            quantities[sym] = n_inc * unit_qty
            units[sym] = {"unit_budget": unit_budget, "unit_qty": unit_qty}
            deployed += n_inc * unit_budget

        # ── Redistribution pass (cheapest-first, capped) ──────────────────────
        if allow_redistribute and quantities:
            # Deterministic order: cheapest increment budget first, then symbol.
            order = sorted(units.keys(),
                           key=lambda s: (units[s]["unit_budget"], s))
            progressed = True
            while progressed:
                progressed = False
                for sym in order:
                    step = units[sym]["unit_budget"]
                    if deployed + step <= budget_cap + 1e-6:
                        quantities[sym] += units[sym]["unit_qty"]
                        deployed += step
                        progressed = True
            # Loop halts when no affordable pick can absorb one more increment
            # within the budget — remainder < cheapest increment. Deterministic.

        remainder = budget_cap - deployed
        return {"quantities": quantities, "skipped": skipped,
                "deployed": deployed, "remainder": remainder}


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
