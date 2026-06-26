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

    def get_ltps_batch(self, symbols: List[str]) -> dict:
        """Return {symbol: ltp} for many symbols at once. SPEED PASS: the live
        Zerodha adapter overrides this with ONE WS-cache pass + a single batched
        kite.ltp() REST fallback for the whole list (instead of N round-trips).
        The default loops over get_ltp so stub brokers + mocks work unchanged;
        symbols with no valid LTP are simply absent from the result."""
        out = {}
        for s in symbols:
            try:
                v = self.get_ltp(s)
            except Exception:  # pragma: no cover - defensive
                v = None
            if v is not None and v > 0:
                out[s] = float(v)
        return out

    # ── Instrument master (F&O) ──────────────────────────────────────────────
    @abstractmethod
    def get_lot_size(self, contract: str) -> int:
        ...

    def get_margin_per_share(self, symbol: str, product: str = "MTF") -> Optional[float]:
        """Per-share margin the broker locks for `product` (MTF leverage).
        Default None → caller falls back to cash sizing. Live brokers override."""
        return None

    def get_margins_batch(self, symbols: List[str],
                          product: str = "MTF") -> dict:
        """Return {symbol: per_share_margin} for many symbols in ONE broker call.
        SPEED PASS: the live Zerodha adapter overrides this with a single
        margin_calc.fetch_margins_batch (one kite.order_margins() probe for the
        whole list) instead of N sequential get_margin_per_share calls. The
        default loops over get_margin_per_share so stubs/mocks behave unchanged;
        symbols whose margin is unavailable are ABSENT (caller cash-falls-back per
        symbol — never over-deploy)."""
        out = {}
        for s in symbols:
            try:
                m = self.get_margin_per_share(s, product)
            except Exception:  # pragma: no cover - defensive
                m = None
            if m is not None and m > 0:
                out[s] = float(m)
        return out

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

    # ── GTT-OCO (broker-held per-position backup floor) ───────────────────────
    # Default no-ops so stub brokers (fyers/upstox/angel/dhan) and dry-run never
    # place real GTTs — they return None. Only the live Zerodha adapter overrides
    # these with real kite.place_gtt / kite.delete_gtt calls.
    def place_gtt_oco(self, symbol: str, qty: int, stop_price: float,
                      target_price: float, last_price: float,
                      product: str = "CNC", exchange: str = "NSE",
                      order_type: str = "LIMIT") -> Optional[str]:
        """Place a two-leg OCO GTT (STOP + TARGET sell). Returns the broker GTT
        id, or None when not placed (dry-run / unsupported broker)."""
        return None

    def cancel_gtt(self, gtt_id: str) -> Any:
        """Cancel a GTT by id. No-op default (returns None)."""
        return None

    def get_gtt(self, gtt_id: str) -> Optional[Any]:
        """Fetch a GTT's current state (e.g. to detect it triggered). No-op
        default (returns None)."""
        return None
