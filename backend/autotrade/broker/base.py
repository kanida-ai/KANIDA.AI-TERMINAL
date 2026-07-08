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
    # Additive fields — populated by load_falcon_picks when fetched from DB.
    # None when constructed manually (e.g. in tests / mock brokers).
    n_fires: Optional[int] = None
    avg_lift: Optional[float] = None


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

    # ── Live order book (quote-driven marketable-limit execution) ────────────
    def get_quotes(self, symbols: List[str]) -> Optional[dict]:
        """Return the live order book for `symbols` in ONE batched broker call:

            {symbol: {ltp, bid, ask, upper_circuit, lower_circuit, ts}}

        where `bid` is the best BUY price (depth top), `ask` the best SELL price,
        the circuits are the exchange price band, and `ts` is a monotonic-ish
        epoch seconds the caller uses for a staleness check. Any per-symbol field
        the broker can't supply is simply absent from that symbol's dict.

        DEFAULT None is the SAFE SENTINEL — "no quote, the caller falls back":
        the marketable-limit pricer SKIPS an entry it can't price and FALLS BACK
        to a MARKET exit. Paper / stub / uncertified brokers return None so the
        quote-driven path is inert for them (byte-for-byte unchanged). Only the
        live Zerodha adapter returns a real book."""
        return None

    def prime_circuit_limits(self, symbols: List[str]) -> int:
        """PHASE-2 WARM PATH: pre-fetch the per-day circuit band for `symbols` so
        the first quote-driven fire needs no REST for circuits. Default no-op
        (paper / stub brokers have no real band); the live Zerodha adapter
        overrides it. Returns the count primed."""
        return 0

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

    def get_active_futures_or_none(self, symbol: str,
                                   expiry_preference: str) -> Optional[str]:
        """Return the tradeable current-month future contract for `symbol`, or
        None when the symbol has NO active future (never fabricate a contract).

        Used by the futures symbol-eligibility filter to drop Falcon picks that
        aren't F&O-eligible. The default wraps get_active_futures + swallows the
        ValueError it raises when no future exists; live adapters may override."""
        try:
            return self.get_active_futures(symbol, expiry_preference)
        except Exception:
            return None

    def get_fut_margin_per_lot(self, symbol: str,
                               expiry_preference: str = "near") -> Optional[float]:
        """Per-LOT initial margin the broker locks to carry ONE lot of `symbol`'s
        current-month future (product NRML). Retail futures are sized on THIS
        margin, NOT the full notional (ltp*lot). Long and short both margin-size.

        Default None → the caller must NOT over-size (it should raise
        InsufficientCapital rather than silently fall back to notional). Live
        adapters override via kite.order_margins on the FUT contract."""
        return None

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
                                instrument_type: str,
                                kite_product: str | None = None,
                                direction: str = "long",
                                *, exec_cfg: Any = None) -> OrderResult:
        """Flatten one position with a MARKET order in the CLOSING direction.

        direction=="long"  (default) → SELL  (today's behaviour, unchanged).
        direction=="short" → BUY-to-cover (close a short future).
        A wrong-direction exit would DOUBLE the position instead of closing it,
        so every caller MUST pass the position's stored direction for shorts.

        exec_cfg (additive, keyword-only, DEFAULT None = today's MARKET exit,
        byte-for-byte): when it is a TradingSessionConfig with
        execution_mode=="marketable_limit", the LIVE adapter prices an in-band
        marketable-LIMIT exit off the live book (SELL at bid-buffer floored above
        the lower circuit / BUY-cover at ask+buffer capped below the upper). If
        the quote is UNAVAILABLE it FALLS BACK to the MARKET exit — an exit must
        NEVER fail to fire because a quote is missing (exiting safely > pricing
        perfectly). Stub / paper brokers ignore exec_cfg (MARKET path)."""
        ...

    def get_order_status(self, order_id: str) -> dict:
        """Return Kite order details dict for `order_id`.

        Default (paper / mock): returns a synthetic COMPLETE result so
        dry-run confirm_exit succeeds immediately without polling.
        Live Zerodha adapter overrides with a real kite.orders() scan.
        """
        return {"status": "COMPLETE", "filled_quantity": 0, "average_price": 0.0}

    def cancel_order_sync(self, order_id: str) -> bool:
        """Synchronous cancel for the retry loop in exit_poller.

        Default: returns True (safe no-op for paper / stub brokers).
        Live Zerodha adapter overrides with kite.cancel_order().
        """
        return True

    def get_net_position_qty(self, symbol: str,
                             instrument_type: str = "EQ") -> Optional[int]:
        """Signed net traded quantity the BROKER currently holds for `symbol`
        (positive = net long, negative = net short, 0 = flat), or None when the
        broker can't answer (unknown / API error) — in which case the caller must
        NOT assume flat and should proceed with its normal exit.

        Used as a PRE-EXIT reconciliation guard: if the operator (or a fired
        broker SL/GTT) already closed the position outside our software, our DB
        still shows it OPEN and a blind exit would place a NAKED order (a fresh
        short on a flat book / doubling on the other side). Checking the broker's
        live net first lets us skip that.

        Default (paper / stub / no live creds): None — do NOT reconcile in paper
        (there is no real broker book), so the exit path is byte-for-byte
        unchanged. Only the live Zerodha adapter returns a real number."""
        return None

    def get_positions_net(self) -> Optional[List[dict]]:
        """Return the broker's FULL day-net position book in ONE call, as a list
        of raw broker net rows, or None when the broker can't answer.

        This is the batched primitive that powers the AUTHORITATIVE broker→DB
        position reconciler (monitoring/position_reconciler.py): one broker round
        trip per tick instead of one per position. Each row is expected to expose
        (best-effort, broker-shaped): tradingsymbol, exchange, quantity,
        buy_quantity, sell_quantity, buy_price, sell_price, average_price, pnl,
        product. Missing fields are tolerated by the reconciler.

        SAFETY CONTRACT — None is "broker unreachable, do NOTHING":
          * None  → the broker book is UNKNOWN (paper / stub / no live creds /
                    API error). The reconciler MUST treat this as "do not mutate
                    the DB" — an unreachable broker can never flatten our rows.
          * []    → an EMPTY (but present) book. The reconciler ALSO does nothing
                    on an empty book (a genuine same-day close still shows a
                    day-flat row with quantity 0; an empty net list is almost
                    always a transient API blip).
          * [rows]→ a non-empty book → the reconciler matches per position.

        Default (paper / stub / no live creds): None — indistinguishable-as-safe.
        Only the live Zerodha adapter (and best-effort Rupeezy) return a list."""
        return None

    def get_holdings(self) -> Optional[List[dict]]:
        """Return the broker's DELIVERED demat holdings, as a list of raw broker
        holding rows, or None when the broker can't answer.

        WHY THE RECONCILER NEEDS THIS (real-money truth): an equity CNC position
        moves OUT of positions()['net'] into holdings on T+1 settlement. So a
        multi-day CNC hold shows quantity 0 (or vanishes) in the net book the next
        day — WITHOUT a sell. The reconciler MUST consult holdings before it can
        conclude a delivery position was closed, or it would wrongly flatten a
        real overnight hold. Rows expose (best-effort): tradingsymbol, quantity,
        t1_quantity, average_price.

        None is the SAME safe sentinel as get_positions_net (do NOTHING). Default
        (paper / stub / no live creds) → None."""
        return None

    def get_orders(self) -> Optional[List[dict]]:
        """Return the broker's FULL day orderbook in ONE call, as a list of raw
        broker order rows, or None when the broker can't answer.

        RECONCILIATION FRAMEWORK (Phase 7): the reconciler uses this to STRENGTHEN
        order-id attribution — before flagging an unattributed deficit it scans the
        orderbook for one of OUR OWN recorded order-ids (exit_order_id) that
        explains the shortfall (a batched second source alongside the per-position
        get_order_status). An order-id NOT owned by one of our positions is NEVER
        attributed as ours (a manual trade stays invisible). Each row is expected
        to expose (best-effort, broker-shaped): order_id, status, filled_quantity,
        average_price, product, tradingsymbol, transaction_type, exchange.

        SAME safe-sentinel contract as get_positions_net / get_holdings:
          * None → the orderbook is UNKNOWN (paper / stub / no live creds / API
                   error). The reconciler falls back to the safe floor (per-position
                   get_order_status only) and NEVER treats None as "no orders".
          * []   → a present-but-empty orderbook.
          * [rows] → a real orderbook the reconciler may consult.

        Default (paper / stub / no live creds) → None. Only the live Zerodha
        adapter (and best-effort Rupeezy) return a list."""
        return None

    # ── GTT-OCO (broker-held per-position backup floor) ───────────────────────
    # Default no-ops so stub brokers (fyers/upstox/angel/dhan) and dry-run never
    # place real GTTs — they return None. Only the live Zerodha adapter overrides
    # these with real kite.place_gtt / kite.delete_gtt calls.
    def place_gtt_oco(self, symbol: str, qty: int, stop_price: float,
                      target_price: float, last_price: float,
                      product: str = "CNC", exchange: str = "NSE",
                      order_type: str = "LIMIT",
                      stop_limit_price: Optional[float] = None,
                      direction: str = "long") -> Optional[str]:
        """Place a two-leg OCO GTT. Returns the broker GTT id, or None when not
        placed (dry-run / unsupported broker).

        direction=="long" (default) → both legs are SELL; stop is BELOW / target
        ABOVE (today's behaviour, unchanged).
        direction=="short" (FUTURES) → both legs are BUY-to-cover; stop is ABOVE
        entry / target BELOW. A broker that can't express a short OCO returns
        None (place NONE) — the software stop still protects; NEVER place a
        wrong-direction GTT.

        stop_limit_price: limit price for the stop leg. For a long it sits BELOW
        the trigger (fills a gap-down); for a short ABOVE (fills a gap-up).
        Defaults to stop_price when None (stub implementations ignore it)."""
        return None

    def cancel_gtt(self, gtt_id: str) -> Any:
        """Cancel a GTT by id. No-op default (returns None)."""
        return None

    def get_gtt(self, gtt_id: str) -> Optional[Any]:
        """Fetch a GTT's current state (e.g. to detect it triggered). No-op
        default (returns None)."""
        return None

    def get_gtt_fill(self, gtt_id: str) -> Optional[dict]:
        """RECONCILIATION FRAMEWORK (Phase 4): POSITIVE fill evidence for a
        TRIGGERED GTT.

        When a GTT fires, Kite marks it status='triggered' and PLACES a real
        order — the GTT object's `orders[]` carries that order's broker order-id
        (via `result` / `order_id`) but NOT always its live fill status. This
        helper resolves the fired leg's order-id and asks the broker for THAT
        order's status via get_order_status, returning:

            {"status": <ORDER STATUS>, "filled_quantity": int,
             "average_price": float, "order_id": str}

        as the confirmed fill evidence. Returns None when the GTT is NOT
        triggered (active / absent / cancelled) or when no fired order-id can be
        resolved — the caller then treats it conservatively (never closes).

        Default no-op (None) — paper / stub / uncertified brokers have no real
        GTT. Only the live Zerodha adapter overrides it."""
        return None
