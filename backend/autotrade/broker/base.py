"""BrokerClient abstract base + shared data shapes.

Every broker (Zerodha live, Fyers/Upstox/Angel/Dhan stubs) implements this
interface. The rest of the AutoTrade system talks ONLY to this interface so the
kill switch, capital engine, and execution layer are broker-agnostic.

All order-placing methods are async so the kill switch can fan them out with
asyncio.gather across brokers in parallel.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, List, Optional


@dataclass
class Pick:
    """A Falcon Top-N pick — read-only consumer shape."""
    symbol: str
    rank: int
    score: float = 0.0
    sector: Optional[str] = None
    close_at_signal: Optional[float] = None


@dataclass
class OrderResult:
    status: str                       # PLACED | FAILED | DRY_RUN | PARTIAL
    broker_order_id: Optional[str]
    symbol: str
    qty: int
    filled_qty: int = 0
    avg_price: Optional[float] = None
    error: Optional[str] = None
    raw: Any = None


class BrokerClient(ABC):
    """Abstract broker. Implementations must be safe in dry-run mode."""

    broker_name: str = "abstract"

    def __init__(self, profile, dry_run: bool = True):
        self.profile = profile
        self.dry_run = dry_run

    # ── Market data ──────────────────────────────────────────────────────────
    @abstractmethod
    def get_ltp(self, symbol: str) -> Optional[float]:
        ...

    # ── Instrument master (F&O) ──────────────────────────────────────────────
    @abstractmethod
    def get_lot_size(self, contract: str) -> int:
        ...

    def get_margin_per_share(self, symbol: str, product: str = "MTF") -> Optional[float]:
        """Per-share margin the broker locks for `product` (MTF leverage).
        Default None → caller falls back to cash sizing. Live brokers override."""
        return None

    @abstractmethod
    def get_active_futures(self, symbol: str, expiry_preference: str) -> str:
        ...

    @abstractmethod
    def get_option_chain(self, symbol: str) -> List[Any]:
        ...

    @abstractmethod
    def get_option_contract(self, symbol: str, strike: float,
                            expiry_preference: str) -> str:
        ...

    # ── Order lifecycle (async for parallel kill-switch fan-out) ─────────────
    @abstractmethod
    async def place_order(self, order) -> OrderResult:
        ...

    @abstractmethod
    async def get_pending_orders(self) -> List[Any]:
        ...

    @abstractmethod
    async def cancel_order(self, order_id: str) -> Any:
        ...

    @abstractmethod
    async def place_market_exit(self, symbol: str, qty: int,
                                instrument_type: str) -> OrderResult:
        ...
