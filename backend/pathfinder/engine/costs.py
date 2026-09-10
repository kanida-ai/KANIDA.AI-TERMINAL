"""
The cost convention. Versioned, enumerated, and charged to EVERY simulated trade.

CLAUDE.md: *every number carries ... cost convention*. docs/STRATEGY_METHODOLOGY.md §1:
*every result is also re-run at 2× slippage; an edge that only exists at the friendly
assumption is a cost artefact, not an edge.*

⚠ FOUNDER INPUT PENDING. The component rates below are the published statutory /
exchange rates for NSE cash-segment DELIVERY (CNC) as used across the Kanida R&D
engines. They have NOT been reconciled against a live contract note. Until they are,
every number computed with them carries this convention string verbatim, so a later
correction is a re-run, not a silent restatement.
"""
from __future__ import annotations

from dataclasses import dataclass

#: All rates in basis points of turnover, per side unless stated.
BROKERAGE_BPS_PER_SIDE = 0.0          # discount broker, delivery
STT_BPS_BUY = 10.0                    # 0.1 %
STT_BPS_SELL = 10.0                   # 0.1 %
EXCHANGE_TXN_BPS_PER_SIDE = 0.297     # NSE 0.00297 %
SEBI_BPS_PER_SIDE = 0.01              # ₹10 per crore
STAMP_BPS_BUY = 1.5                   # 0.015 %, buy side only
GST_RATE = 0.18                       # on brokerage + exchange txn + SEBI

#: Slippage assumption, per side. The 2× gate doubles this and nothing else.
SLIPPAGE_BPS_PER_SIDE = 10.0


@dataclass(frozen=True)
class CostModel:
    """
    Round-trip cost of one virtual delivery trade, in percent of notional.

    Statutory costs are symmetric in direction for our purposes (a short in the
    cash segment is not modelled — short hypotheses route intraday/MIS in the live
    OMS, which is out of Pathfinder's reach; Pathfinder charges the same delivery
    convention both ways and says so).
    """

    slippage_bps_per_side: float = SLIPPAGE_BPS_PER_SIDE
    version: str = "costs_v3.1_cnc_delivery"

    # ── components ──────────────────────────────────────────────────────────
    @property
    def statutory_bps_round_trip(self) -> float:
        taxable = 2 * (BROKERAGE_BPS_PER_SIDE + EXCHANGE_TXN_BPS_PER_SIDE + SEBI_BPS_PER_SIDE)
        return (
            2 * BROKERAGE_BPS_PER_SIDE
            + STT_BPS_BUY + STT_BPS_SELL
            + 2 * EXCHANGE_TXN_BPS_PER_SIDE
            + 2 * SEBI_BPS_PER_SIDE
            + STAMP_BPS_BUY
            + GST_RATE * taxable
        )

    @property
    def slippage_bps_round_trip(self) -> float:
        return 2 * self.slippage_bps_per_side

    @property
    def round_trip_pct(self) -> float:
        """Total cost charged to a closed trade, as a percentage of entry notional."""
        return (self.statutory_bps_round_trip + self.slippage_bps_round_trip) / 100.0

    def at_slippage_multiple(self, k: float) -> "CostModel":
        """The sensitivity gate: same statutory costs, k× the slippage assumption."""
        return CostModel(slippage_bps_per_side=self.slippage_bps_per_side * k, version=self.version)

    # ── the string that travels with every number ───────────────────────────
    @property
    def convention(self) -> str:
        return (
            f"{self.version}: brokerage {BROKERAGE_BPS_PER_SIDE:g}bps/side + STT "
            f"{STT_BPS_BUY:g}bps buy + {STT_BPS_SELL:g}bps sell + exchange "
            f"{EXCHANGE_TXN_BPS_PER_SIDE:g}bps/side + SEBI {SEBI_BPS_PER_SIDE:g}bps/side + stamp "
            f"{STAMP_BPS_BUY:g}bps buy + GST {GST_RATE:.0%} on (brokerage+exchange+SEBI) "
            f"= {self.statutory_bps_round_trip:.2f}bps round trip; slippage "
            f"{self.slippage_bps_per_side:g}bps/side; entry next open"
        )


DEFAULT_COSTS = CostModel()
#: The gate every result must also clear (docs/STRATEGY_METHODOLOGY.md §1, step 3).
DOUBLE_SLIPPAGE_COSTS = DEFAULT_COSTS.at_slippage_multiple(2.0)
