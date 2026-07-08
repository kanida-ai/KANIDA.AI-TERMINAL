"""RECONCILIATION FRAMEWORK — Guard G4 (mode F4): P&L GROSS vs NET clarity.

Our position / journal P&L is GROSS — the raw (exit - entry) * qty. It excludes
every statutory + broker charge (brokerage, STT, exchange txn, SEBI, stamp, GST,
DP). On a real account those charges are the difference between "the panel says
+₹1,340" and "my ledger says +₹1,180". This module is a small PURE estimator so
the read path can surface realised_pnl_net ALONGSIDE the existing GROSS field —
WITHOUT renaming or changing the gross number.

IMPORTANT — THIS IS AN ESTIMATE:
  * Rates below are Zerodha's published EQUITY (NSE cash) charges as of 2026-07.
    They are hardcoded in ONE constants block (_RATES) so a future rate change is
    a one-line edit. They are NOT fetched live (Zerodha has no per-order charges
    API) — so the net figure is an ESTIMATE, labelled as such everywhere it
    surfaces. Do not treat it as the broker's contract-note truth.
  * Scope: EQUITY only (CNC delivery + MIS intraday). F&O (FUT/OPT) has a
    DIFFERENT brokerage + STT structure — estimate_charges returns a best-effort
    equity-shaped number for a non-equity product ONLY as a rough floor; the
    caller should treat F&O net as approximate. (A dedicated F&O table is a
    follow-up; flagged in the framework doc.)

DATA-ISOLATION: pure arithmetic. Reads/writes NOTHING. Never raises on bad input
(returns a zero-charge dict) — a charges bug must never break the P&L read path.
"""
from __future__ import annotations

from typing import Dict

# ── RATE CONSTANTS (Zerodha equity, NSE cash — 2026-07; ESTIMATE) ─────────────
# All are FRACTIONS of the relevant turnover unless noted. Keep every rate here.
_RATES = {
    # Brokerage.
    #   CNC delivery : ZERO (Zerodha charges no brokerage on equity delivery).
    #   MIS intraday : 0.03% of the executed value OR ₹20 per order, whichever is
    #                  LOWER — applied PER LEG (buy leg + sell leg each).
    "brokerage_cnc_rate": 0.0,
    "brokerage_mis_rate": 0.0003,       # 0.03%
    "brokerage_cap_per_leg": 20.0,      # ₹20 cap per executed order (leg)

    # Securities Transaction Tax (STT).
    #   CNC delivery : 0.1% on BOTH buy and sell turnover.
    #   MIS intraday : 0.025% on the SELL turnover only.
    "stt_cnc_rate": 0.001,              # 0.10% each side
    "stt_mis_sell_rate": 0.00025,       # 0.025% sell side only

    # Exchange transaction charge (NSE equity). ~0.00297% of turnover (both sides).
    # (BSE differs; NSE is the operative venue for our cash orders.)
    "exchange_txn_rate": 0.0000297,     # 0.00297%

    # SEBI turnover fee: ₹10 per crore = 0.0001% of turnover (both sides).
    "sebi_rate": 0.000001,              # ₹10 / 1e7

    # Stamp duty (buy side only). Equity delivery 0.015%, intraday 0.003%.
    "stamp_cnc_buy_rate": 0.00015,      # 0.015% delivery buy
    "stamp_mis_buy_rate": 0.00003,      # 0.003% intraday buy

    # GST: 18% on (brokerage + exchange txn + SEBI).
    "gst_rate": 0.18,

    # DP (depository participant) charge on the SELL of a CNC delivery holding —
    # a FLAT per-scrip per-day fee (NOT a percentage). ₹15.34 (incl GST) is the
    # commonly published all-in figure. Applied once per closed CNC delivery
    # position on the sell side. MIS/intraday: none (no delivery, no DP debit).
    "dp_charge_cnc_sell": 15.34,

    # ── F&O (Zerodha, NSE derivatives — 2026-07; ESTIMATE) ────────────────────
    # These branch by SEGMENT (instrument_type FUT vs CE/PE), NOT by product —
    # a futures position is a derivative regardless of a CNC/NRML/MIS product tag,
    # and the equity-delivery model (0.1% STT both sides + ₹15 DP) is WRONG for it.
    # EQUITY FUTURES:
    #   brokerage : 0.03% of executed value OR ₹20 per leg, whichever LOWER (per leg)
    #   STT       : 0.02% on the SELL notional only
    #   exch txn  : ~0.0019% of turnover (both sides, NSE FUT)
    #   stamp     : 0.002% on the BUY notional only
    #   DP        : NONE (no delivery)
    "fut_brokerage_rate":     0.0003,     # 0.03% per leg
    "fut_brokerage_cap_per_leg": 20.0,    # ₹20 cap per executed leg
    "fut_stt_sell_rate":      0.0002,     # 0.02% SELL side only (notional)
    "fut_exchange_txn_rate":  0.000019,   # ~0.0019% turnover (both sides)
    "fut_stamp_buy_rate":     0.00002,    # 0.002% BUY side only
    # OPTIONS (CE/PE): the value passed IS premium turnover (qty × premium).
    #   brokerage : ₹20 FLAT per leg (not a %)
    #   STT       : 0.1% on the SELL premium only
    #   exch txn  : ~0.03503% of premium turnover (both sides, NSE options)
    #   stamp     : 0.003% on the BUY premium only
    #   DP        : NONE
    "opt_brokerage_flat_per_leg": 20.0,   # ₹20 flat per executed leg
    "opt_stt_sell_rate":      0.001,      # 0.1% SELL side on premium
    "opt_exchange_txn_rate":  0.0003503,  # ~0.03503% premium turnover (both sides)
    "opt_stamp_buy_rate":     0.00003,    # 0.003% BUY premium only
}


def _f(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _kite_product(product: str) -> str:
    """Normalise to CNC | MIS | NRML | MTF (EQ → CNC), matching the reconciler's
    _kite_product so the charge bucket lines up with the position bucket."""
    p = str(product or "CNC").upper()
    if p == "EQ":
        return "CNC"
    if p in ("CNC", "MIS", "NRML", "MTF"):
        return p
    return "CNC"


def estimate_charges(product: str, buy_value: float, sell_value: float,
                     legs: int = 2,
                     instrument_type: str = "EQ") -> Dict[str, float]:
    """Estimate the round-trip (or per-leg) statutory + broker charges for a
    position and return a breakdown dict.

    The rate MODEL is chosen by SEGMENT (instrument_type), then — for equity —
    refined by product:
      * FUT      → equity-futures model  (0.02% STT sell-only, no DP, brokerage
                   min(0.03%, ₹20)/leg). `product` is IGNORED for FUT.
      * CE / PE  → options model  (0.1% STT sell-only on premium, ₹20 flat/leg,
                   no DP). `product` is IGNORED for options.
      * EQ (or anything else) → the equity model below, keyed by `product`.

    Args:
      product         : CNC | MIS | MTF | NRML (EQ→CNC). MTF is charged like CNC
                        delivery; used ONLY for the equity model. IGNORED for F&O.
      buy_value       : executed BUY turnover  (qty * buy_avg_price), ≥ 0. For
                        options this IS the buy premium turnover (qty × premium).
      sell_value      : executed SELL turnover (qty * sell_avg_price), ≥ 0. Pass 0
                        for an OPEN position (entry-only): only buy-side applies.
      legs            : number of executed order legs (2 = a completed round trip).
      instrument_type : EQ | FUT | CE | PE (default EQ → byte-identical to the
                        prior equity behaviour).

    Returns dict{brokerage, stt, exchange, gst, stamp, dp, total} — all ₹, all
    rounded to 2dp. total is the sum. NEVER raises; bad input → all-zero dict.

    ESTIMATE: see module docstring. The number is intentionally conservative
    (rounds charges up via the standard published rates), so net P&L is a floor,
    not an optimistic figure.
    """
    zero = {"brokerage": 0.0, "stt": 0.0, "exchange": 0.0, "gst": 0.0,
            "stamp": 0.0, "dp": 0.0, "total": 0.0}
    try:
        seg = str(instrument_type or "EQ").upper()
        buy = max(0.0, _f(buy_value))
        sell = max(0.0, _f(sell_value))
        n_legs = int(legs) if legs else 0
        if n_legs < 0:
            n_legs = 0
        turnover = buy + sell

        # ── SEGMENT DISPATCH: F&O never uses the equity-delivery model ─────────
        if seg == "FUT":
            return _charges_futures(buy, sell, turnover)
        if seg in ("CE", "PE", "OPT"):
            return _charges_options(buy, sell, turnover)

        # ── EQUITY (EQ / cash) — unchanged, keyed by product ──────────────────
        prod = _kite_product(product)
        is_mis = prod == "MIS"

        # ── Brokerage ─────────────────────────────────────────────────────────
        # Per LEG: min(rate * leg_value, cap). CNC/MTF/NRML delivery → 0.
        if is_mis:
            r = _RATES["brokerage_mis_rate"]
            cap = _RATES["brokerage_cap_per_leg"]
            brokerage = 0.0
            if buy > 0:
                brokerage += min(r * buy, cap)
            if sell > 0:
                brokerage += min(r * sell, cap)
        else:
            brokerage = _RATES["brokerage_cnc_rate"] * turnover  # 0.0

        # ── STT ───────────────────────────────────────────────────────────────
        if is_mis:
            stt = _RATES["stt_mis_sell_rate"] * sell            # sell side only
        else:
            stt = _RATES["stt_cnc_rate"] * (buy + sell)         # both sides

        # ── Exchange transaction charge (both sides) ──────────────────────────
        exchange = _RATES["exchange_txn_rate"] * turnover

        # ── SEBI turnover fee (both sides) ────────────────────────────────────
        sebi = _RATES["sebi_rate"] * turnover

        # ── Stamp duty (BUY side only) ────────────────────────────────────────
        stamp_rate = (_RATES["stamp_mis_buy_rate"] if is_mis
                      else _RATES["stamp_cnc_buy_rate"])
        stamp = stamp_rate * buy

        # ── GST: 18% on (brokerage + exchange + sebi) ─────────────────────────
        gst = _RATES["gst_rate"] * (brokerage + exchange + sebi)

        # ── DP charge: CNC/MTF delivery SELL only, flat, once per closed pos ───
        # Only when there is a real sell (a completed delivery exit).
        dp = 0.0
        if (not is_mis) and sell > 0:
            dp = _RATES["dp_charge_cnc_sell"]

        # SEBI folded into "exchange" bucket-of-record? No — keep it visible in
        # total but report it under exchange-family. We surface it inside total
        # and add it to `exchange` for the dict (SEBI has no dedicated key to
        # avoid churn on the read contract), documented here.
        exchange_family = exchange + sebi

        total = brokerage + stt + exchange_family + gst + stamp + dp
        return {
            "brokerage": round(brokerage, 2),
            "stt":       round(stt, 2),
            "exchange":  round(exchange_family, 2),  # exchange txn + SEBI fee
            "gst":       round(gst, 2),
            "stamp":     round(stamp, 2),
            "dp":        round(dp, 2),
            "total":     round(total, 2),
        }
    except Exception:  # pragma: no cover - never break the P&L read path
        return dict(zero)


def _charges_futures(buy: float, sell: float, turnover: float) -> Dict[str, float]:
    """EQUITY-FUTURES charge model (ESTIMATE). NO DP; STT sell-side only at 0.02%;
    brokerage min(0.03%, ₹20) per leg. Product tag is irrelevant for a future."""
    r = _RATES["fut_brokerage_rate"]
    cap = _RATES["fut_brokerage_cap_per_leg"]
    brokerage = 0.0
    if buy > 0:
        brokerage += min(r * buy, cap)
    if sell > 0:
        brokerage += min(r * sell, cap)
    stt = _RATES["fut_stt_sell_rate"] * sell               # 0.02% sell notional
    exchange = _RATES["fut_exchange_txn_rate"] * turnover   # both sides
    sebi = _RATES["sebi_rate"] * turnover
    stamp = _RATES["fut_stamp_buy_rate"] * buy              # buy side only
    gst = _RATES["gst_rate"] * (brokerage + exchange + sebi)
    exchange_family = exchange + sebi
    total = brokerage + stt + exchange_family + gst + stamp   # NO DP for F&O
    return {
        "brokerage": round(brokerage, 2),
        "stt":       round(stt, 2),
        "exchange":  round(exchange_family, 2),
        "gst":       round(gst, 2),
        "stamp":     round(stamp, 2),
        "dp":        0.0,
        "total":     round(total, 2),
    }


def _charges_options(buy: float, sell: float, turnover: float) -> Dict[str, float]:
    """OPTIONS (CE/PE) charge model (ESTIMATE). buy/sell are PREMIUM turnover.
    NO DP; STT sell-side only at 0.1% on premium; brokerage ₹20 FLAT per leg."""
    flat = _RATES["opt_brokerage_flat_per_leg"]
    brokerage = 0.0
    if buy > 0:
        brokerage += flat
    if sell > 0:
        brokerage += flat
    stt = _RATES["opt_stt_sell_rate"] * sell               # 0.1% sell premium
    exchange = _RATES["opt_exchange_txn_rate"] * turnover   # premium, both sides
    sebi = _RATES["sebi_rate"] * turnover
    stamp = _RATES["opt_stamp_buy_rate"] * buy              # buy premium only
    gst = _RATES["gst_rate"] * (brokerage + exchange + sebi)
    exchange_family = exchange + sebi
    total = brokerage + stt + exchange_family + gst + stamp   # NO DP for F&O
    return {
        "brokerage": round(brokerage, 2),
        "stt":       round(stt, 2),
        "exchange":  round(exchange_family, 2),
        "gst":       round(gst, 2),
        "stamp":     round(stamp, 2),
        "dp":        0.0,
        "total":     round(total, 2),
    }
