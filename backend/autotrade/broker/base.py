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

    # ── BOOK SEMANTICS (broker-agnostic reconciliation) ──────────────────────
    # Does this broker's HOLDINGS book already contain TODAY's unsettled CNC
    # BUYS? This is the ONE piece of per-broker book semantics the position
    # reconciler needs, and it is declared BY THE ADAPTER — never branched on a
    # broker name in the reconciler (`if broker == "zerodha"` is banned).
    #   False (Kite/Zerodha, live-verified 2026-07-06/07-08): today's CNC buys
    #     sit in positions()['net'] and reach holdings only on T+1 settlement →
    #     held = holdings(quantity + t1_quantity) + max(0, net).
    #   True: the holdings book ALREADY reflects today's buys → adding max(0,net)
    #     would DOUBLE-COUNT them (the suspected source of the 3 bogus
    #     CORP_ACTION_SUSPECTED alerts on Rupeezy CNC same-day buys) →
    #     held = holdings.
    # Default False = today's behaviour for EVERY adapter (byte-for-byte). An
    # adapter flips it ONLY on a live, non-zero observation of its own book.
    CNC_HOLDINGS_INCLUDE_SAME_DAY_BUYS: bool = False

    def broker_held_qty(self, *, product: str, holdings_qty: int,
                        net_qty: int) -> int:
        """The qty this broker's book says is HELD for one (symbol, product).

        BROKER-AGNOSTIC OMS (operator HARD requirement): the reconciler must not
        hard-code one broker's book semantics. It hands each adapter the two
        already-normalised numbers it scraped from THIS broker's books —
        `holdings_qty` (Σ quantity + t1_quantity over the matching holdings rows;
        0 when the broker has no holdings book) and `net_qty` (the signed net) —
        and the adapter maps them to a held size using ITS OWN semantics, declared
        via CNC_HOLDINGS_INCLUDE_SAME_DAY_BUYS.

        Non-CNC (MIS / NRML / MTF): there are no holdings; |signed net| IS the
        exposure (a short MTF nets negative — abs is the held size). Universal
        across brokers, so it is not adapter-configurable.

        CNC (delivery): a SELL is already removed from holdings (the book reflects
        the post-sell balance), so a negative net must NOT be subtracted again or
        the sell is double-counted — hence max(0, net), never net. Verified LIVE on
        Kite 2026-07-08 (AEGISLOG: holdings t1 35 still held + net -57 from other
        sessions' exits → true held 35; the old max(0, net+holdings) gave 0, a
        false UNATTRIBUTED_CLOSE) and 2026-07-06 (holdings 35 + net +57 buys = 92
        → 35 + max(0,57) = 92; ACUTAAS fully sold: 0 + max(0,-12) = 0).

        Overriding: an adapter normally only sets the class flag. Override this
        method itself only for a broker whose book needs a genuinely different
        shape."""
        prod = str(product or "CNC").upper()
        try:
            net = int(net_qty or 0)
            hq = int(holdings_qty or 0)
        except (TypeError, ValueError):  # pragma: no cover - defensive
            return 0
        if prod != "CNC":
            return abs(net)
        if self.CNC_HOLDINGS_INCLUDE_SAME_DAY_BUYS:
            # Today's buys are already inside holdings → adding net would
            # double-count them. A fully-sold delivery floors to 0.
            return max(0, hq)
        return hq + max(0, net)

    def __init__(self, profile, dry_run: bool = True):
        self.profile = profile
        self.dry_run = dry_run

    # ── LIVE-ORDER CERTIFICATION GATE (SPRINT CLUSTER 9b ITEM 9) ──────────────
    def _certification_block(self, action: str = "order",
                             symbol: Optional[str] = None) -> Optional[str]:
        """Block REAL order placement through an UNCERTIFIED broker adapter.

        Returns an UNCERTIFIED_BROKER_BLOCKED reason string (and PAGES via
        alerts.send_urgent) when this adapter is NOT certified for live orders,
        else None. Certified brokers (zerodha) → None (byte-for-byte unchanged).

        The caller invokes this ONLY on the LIVE path (after `_live_allowed()` is
        true) — paper / dry-run returns DRY_RUN before ever reaching here, so paper
        is unaffected regardless of certification. Never raises (fail-closed on the
        certification lookup would over-block zerodha; the lookup is deterministic
        and defaults to certified on an unexpected error only for the known-live
        zerodha path — see registry.is_certified)."""
        try:
            from .registry import is_certified
            certified = is_certified(self.broker_name)
        except Exception:  # pragma: no cover - defensive; registry is deterministic
            certified = True
        if certified:
            return None
        reason = (f"UNCERTIFIED_BROKER_BLOCKED: {self.broker_name} is not certified "
                  f"for LIVE {action} placement — refused (paper only). Certify the "
                  f"adapter (orders/margin/GTT/status + client-tag) before enabling "
                  f"real orders.")
        try:
            from .. import alerts
            alerts.send_urgent_deduped(
                kind="UNCERTIFIED_BROKER_BLOCKED", session_id=None,
                symbol=symbol, detail=reason)
        except Exception:  # pragma: no cover - paging must never block the refusal
            pass
        return reason

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

    def available_margin(self) -> Optional[float]:
        """RMS (SPRINT CLUSTER 4, CAP 1): the account's FREE equity margin
        (deployable cash / net) the pre-trade gate sizes against, or None when the
        broker can't answer (paper / stub / no live creds / API error).

        SAFETY CONTRACT — None is "unknown, do NOT block":
          * None  → the pre-trade margin gate is INERT (never refuses on an
                    unknown budget); the committed-capital ledger still applies
                    only when a budget is known. Paper / dry-run must return None
                    so paper sizing is byte-for-byte unchanged.
          * float → the free equity margin (₹). The gate refuses/scales a session
                    whose planned deployed exceeds this (minus the user's already-
                    committed capital across other live sessions).

        Default None. Only the live Zerodha adapter returns a real number
        (kite.margins()['equity']['net']); every other adapter returns None
        (unknown → don't block, log)."""
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
                                *, exec_cfg: Any = None,
                                client_order_id: str | None = None) -> OrderResult:
        """Flatten one position with a MARKET order in the CLOSING direction.

        client_order_id (SPRINT CLUSTER 2, CAP 1, additive keyword-only, DEFAULT
        None): when supplied, the live adapter attaches compact_tag(client_order_id)
        as the broker order tag so OUR exit is recognisable in the broker orderbook.
        Stub / paper brokers ignore it (no real order is placed).

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

    # ── BATCHED PRE-EXIT PROBE (exit/kill latency: O(N) round trips → O(1)) ───
    # get_net_position_qty() costs ONE broker round trip PER POSITION. Called in
    # the serial pre-exit loop of kill_switch.fire it dominates exit latency
    # (measured: ~14.4s of a 15.9s 3-leg Vortex kill sat BEFORE the first
    # EXIT_PLACED). These two methods let a caller pay ONE round trip for the
    # whole book and then answer every leg from it, in memory.
    #
    # SAFETY — why this is NOT just get_positions_net():
    #   get_positions_net() SWALLOWS broker errors to None, and None is ALSO the
    #   PAPER sentinel that callers read as "no book to reconcile, proceed with
    #   the exit". Reusing it for the pre-exit guard would place a BLIND exit on
    #   a broker error → the 2026-07-10 BRIGADE double-cover class (a blind
    #   second buy-to-cover into a naked reverse). It must NEVER back this guard.
    #
    # fetch_net_position_book() therefore mirrors get_net_position_qty()'s
    # fail-loud contract EXACTLY, and the two failure modes are made EXPLICIT
    # (today the paper-vs-error distinction is implicit — that is where the
    # danger lives):
    #   * paper / not-live / no book available → None  (NO round trip; the caller
    #     falls back to the per-leg probe, which itself returns None instantly →
    #     paper is byte-for-byte unchanged, zero round trips)
    #   * live + success                       → an OPAQUE, adapter-owned book
    #   * live + ANY error                     → RAISE (never swallowed to None)
    #
    # The caller's contract: a raise/None from the BATCH must fall back to the
    # per-leg probe loop — NEVER abort the legs (a fail-OPEN kill switch is worse
    # than a slow one) and NEVER proceed unguarded.
    def fetch_net_position_book(self):
        """Fetch the broker's full net book in ONE round trip, for the batched
        pre-exit guard. Returns an OPAQUE book (only ever passed back to this
        SAME adapter's net_qty_from_book), or None when there is no book to
        answer from. RAISES on a genuine live broker error — callers fall back to
        the per-leg get_net_position_qty() probe, which fails loud per leg.

        Default (paper / stub / no live creds): None → per-leg fallback."""
        return None

    def net_qty_from_book(self, book, symbol: str,
                          instrument_type: str = "EQ") -> Optional[int]:
        """Signed net qty for `symbol` read out of a `book` from THIS adapter's
        fetch_net_position_book(). Pure / in-memory — performs NO I/O and must
        never raise. Returns the SAME value get_net_position_qty(symbol,
        instrument_type) would have returned for that book, or None when the book
        cannot answer (→ the caller falls back to the per-leg probe).

        Default (paper / stub): None → per-leg fallback."""
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

    def find_recent_order(self, order) -> Optional[dict]:
        """QUERY-BEFORE-RETRY idempotency guard (2026-07-13 ZENSARTECH double-fill).

        Before a RETRY of an ENTRY order that TIMED OUT on our confirmation, ask
        the broker whether OUR order already reached it (our compact tag +
        tradingsymbol + transaction_type + quantity). Return the matching order
        dict (with order_id + status) so the caller ADOPTS it and places NO second
        order, or None when the order is DEFINITIVELY absent (safe to retry).

        SAFETY DIRECTION (critical): the dangerous failure is a FALSE-NEGATIVE — we
        think no order exists and double-place. So None means ONLY "queried the
        broker successfully and our order is definitely NOT there". A live adapter
        that CANNOT determine (API error) MUST RAISE (inconclusive) rather than
        return None, so the caller fails the leg instead of blind-retrying.

        Default (paper / dry-run / stub / uncertified brokers) → None: there is no
        real broker orderbook, no real order was ever placed, and place_order never
        times out in dry-run — so paper behaviour is byte-for-byte unchanged and no
        adoption ever happens. Only the live Zerodha adapter overrides this."""
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

    # ── MIS broker-side protective stop (SL-M) — RMS CAP 3 ───────────────────
    def place_protective_slm(self, symbol: str, qty: int, trigger_price: float,
                             direction: str = "long", product: str = "MIS",
                             exchange: str = "NSE",
                             client_tag: Optional[str] = None) -> Optional[str]:
        """Place a broker-side DAY SL-M (stop-market) protective order for an MIS
        leg (which gets NO GTT). Returns the broker order-id, or None when not
        placed (dry-run / unsupported broker).

        This is the HARD downside cap that survives the monitor process going
        down: an MIS position (especially a SHORT, whose downside is otherwise
        software-only) has a broker-held stop at its capital-basis stop level.

        direction=="long"  (default) → SELL SL-M with trigger BELOW entry (stops a
                            long that falls).
        direction=="short" → BUY-to-cover SL-M with trigger ABOVE entry (stops a
                            short that rises).
        client_tag: the compact broker tag so the order is recognisable as OURS in
                    the orderbook (parity with our order-id ownership). A broker
                    that can't attach a tag ignores it.

        Default no-op (None). Only the live Zerodha adapter overrides it with a
        real kite.place_order(order_type=SL-M, trigger_price=...)."""
        return None

    def cancel_protective_slm(self, order_id: str) -> bool:
        """Cancel a protective SL-M order by id (parity with cancel_order_sync).
        Default True (safe no-op for paper / stub brokers). Live Zerodha overrides
        with kite.cancel_order()."""
        return True

    def cancel_gtt(self, gtt_id: str) -> Any:
        """Cancel a GTT by id. No-op default (returns None)."""
        return None

    def get_gtt(self, gtt_id: str) -> Optional[Any]:
        """Fetch a GTT's current state (e.g. to detect it triggered). No-op
        default (returns None)."""
        return None

    def get_gtts_map(self) -> Optional[dict]:
        """P1(b): fetch ALL GTTs in ONE broker call and return {str(id): state},
        so a per-tick reconcile can look up N positions' GTTs without N round
        trips. Default None = 'no batch available' → the caller falls back to
        per-position get_gtt (byte-identical). Live adapters override."""
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
