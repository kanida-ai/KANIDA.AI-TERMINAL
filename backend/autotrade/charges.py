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
                     legs: int = 2) -> Dict[str, float]:
    """Estimate the round-trip (or per-leg) statutory + broker charges for an
    EQUITY position and return a breakdown dict.

    Args:
      product    : CNC | MIS | MTF | NRML (EQ→CNC). MTF is charged like CNC
                   delivery (it IS a delivery product); NRML (F&O carry) is
                   approximated with the delivery table as a rough floor.
      buy_value  : executed BUY turnover  (qty * buy_avg_price), ≥ 0.
      sell_value : executed SELL turnover (qty * sell_avg_price), ≥ 0. Pass 0 for
                   an OPEN position (entry-only): only the buy-side charges apply.
      legs       : number of executed order legs (2 = a completed round trip; 1 =
                   entry-only OPEN position). Drives the brokerage cap count.

    Returns dict{brokerage, stt, exchange, gst, stamp, dp, total} — all ₹, all
    rounded to 2dp. total is the sum. NEVER raises; bad input → all-zero dict.

    ESTIMATE: see module docstring. The number is intentionally conservative
    (rounds charges up via the standard published rates), so net P&L is a floor,
    not an optimistic figure.
    """
    zero = {"brokerage": 0.0, "stt": 0.0, "exchange": 0.0, "gst": 0.0,
            "stamp": 0.0, "dp": 0.0, "total": 0.0}
    try:
        prod = _kite_product(product)
        buy = max(0.0, _f(buy_value))
        sell = max(0.0, _f(sell_value))
        n_legs = int(legs) if legs else 0
        if n_legs < 0:
            n_legs = 0
        turnover = buy + sell
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
