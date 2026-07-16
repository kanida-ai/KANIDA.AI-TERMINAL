"""Async mock BrokerClient for AutoTrade tests. NEVER touches real Kite."""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from autotrade.broker.base import BrokerClient, OrderResult


class MockBroker(BrokerClient):
    broker_name = "mock"

    def __init__(self, profile, dry_run: bool = False, *,
                 ltps: Optional[Dict[str, float]] = None,
                 exit_delay_sec: float = 0.0,
                 fail_symbols: Optional[set] = None,
                 lot_size: int = 50,
                 pending_orders: Optional[List[dict]] = None,
                 partial_fills: Optional[Dict[str, int]] = None,
                 fut_margin_per_lot: Optional[float] = None,
                 no_future_symbols: Optional[set] = None,
                 margins: Optional[Dict[str, float]] = None,
                 margins_available: bool = True,
                 net_positions: Optional[Dict[str, int]] = None,
                 net_probe_raise_symbols: Optional[set] = None,
                 net_book: Optional[Any] = None,
                 holdings: Optional[Any] = None,
                 orders: Optional[Any] = None,
                 reject_symbols: Optional[set] = None,
                 reject_after: Optional[Dict[str, int]] = None,
                 fill_price_sequence: Optional[Dict[str, list]] = None,
                 quotes: Optional[Dict[str, dict]] = None,
                 gtts: Optional[Dict[str, dict]] = None,
                 available_margin: Optional[float] = None,
                 slm_available: bool = True,
                 exit_reject_after: Optional[Dict[str, int]] = None,
                 block_orders: Optional[str] = None,
                 block_symbols: Optional[set] = None,
                 order_status: Optional[Dict[str, dict]] = None):
        super().__init__(profile, dry_run=dry_run)
        # BROKER-AGNOSTIC CERT-BLOCK sim (2026-07-15). When set, EVERY place_order
        # returns FAILED with this reason (broker_order_id=None) — modelling an
        # uncertified adapter's hard-block (RupeezyBroker._certification_block), a
        # broker RMS refusal, or an IP block. Default None → unchanged. This drives
        # the split-entry zero-placement FAIL tests without any real network.
        self._block_orders = block_orders
        # Per-symbol variant of block_orders: only these symbols' place_order
        # returns FAILED (the rest fill) → drives the PARTIAL-placement test.
        self._block_symbols = block_symbols or set()
        # RMS CAP 1: the account's FREE equity margin the pre-trade gate sizes
        # against. None (DEFAULT) → available_margin() returns None (gate INERT,
        # paper byte-identical). A number engages the gate (models a live broker).
        self._available_margin = available_margin
        # RMS CAP 3: protective SL-M support. When False the broker "can't" place
        # an SL-M → place_protective_slm returns None (models an uncertified
        # adapter). Records placed/cancelled SL-Ms so a test can assert them.
        self._slm_available = slm_available
        self.slm_orders: List[dict] = []       # placed SL-M args
        self.slm_cancelled: List[str] = []      # cancelled SL-M order-ids
        # DELIVERY holdings book (list of raw holding rows, or {symbol: {quantity,
        # t1_quantity, average_price}} auto-normalised). None (default) → get_holdings
        # returns None (the reconciler treats a CNC position settled to holdings as
        # "unknown here" and relies on the exit-evidence guard). Models T+1 delivery.
        self._holdings = holdings
        # POST-PLACEMENT REJECTION sim: symbols the broker ACCEPTS (issues an
        # order_id) but the EXCHANGE then REJECTS asynchronously (0 fill) — e.g. a
        # circuit-limit breach. place_order returns PLACED w/ no fill; the entry
        # reconcile's get_order_status then reports REJECTED. Default empty →
        # existing tests unaffected.
        self.reject_symbols = reject_symbols or set()
        # ICEBERG (CLUSTER 7): reject a symbol's placement AFTER this many
        # successful placements of that symbol — so an iceberg's first N children
        # fill and a LATER child rejects. {symbol: n}. None (default) → no effect
        # (existing tests unaffected). The rejecting placement returns PLACED with
        # a "rej-<sym>" order-id whose get_order_status reports REJECTED (0 fill).
        self._reject_after = reject_after or {}
        self._placement_counts: Dict[str, int] = {}
        # ICEBERG (CLUSTER 7): per-symbol fill-price PER PLACEMENT, so an iceberg's
        # children fill at DIFFERENT prices and a weighted-average aggregation can
        # be asserted (a naive mean / last-price bug then diverges). {symbol:[p0,
        # p1,...]}; the i-th placement fills at seq[min(i, len-1)]. None (default)
        # → avg_price = ltps[symbol] as before (existing tests unaffected).
        self._fill_price_sequence = fill_price_sequence or {}
        # Pre-exit reconciliation guard: simulate the broker's live net book.
        # {symbol: signed_qty}. None (default) means the mock does NOT answer the
        # net-position probe → the base default (None) is used and the exit path
        # is unchanged. A dict entry of 0 = flat at the broker (already closed).
        self._net_positions = net_positions
        # Fix B1 (2026-07-10): symbols for which get_net_position_qty RAISES (models
        # a live broker connection error — ConnectionResetError 10054 — mid probe).
        # The real ZerodhaBroker RE-RAISES on error; the caller's pre-exit guard must
        # then ABORT (no blind order). Default empty → existing tests unaffected.
        self._net_probe_raise_symbols = net_probe_raise_symbols or set()
        # AUTHORITATIVE reconciler: the broker's FULL day-net book. Accepts either
        #   * a list of raw Kite-shaped net rows, or
        #   * a dict {symbol -> {quantity, buy_quantity, sell_quantity, sell_price,
        #     buy_price, average_price, exchange, pnl, ...}} (auto-normalised to a
        #     list, injecting tradingsymbol from the key), or
        #   * None (DEFAULT) → get_positions_net() returns None (models paper / "no
        #     broker book" → the reconciler does NOTHING). Existing tests unaffected.
        # A special sentinel value of [] (empty list) models a present-but-EMPTY
        # book (transient API blip) → the reconciler also does nothing.
        self._net_book = net_book
        # RECONCILIATION Phase 7: the broker's FULL day orderbook. Accepts either
        #   * a list of raw Kite-shaped order rows (order_id/status/
        #     filled_quantity/average_price/transaction_type/tradingsymbol/...), or
        #   * a dict {order_id -> {status, filled_quantity, ...}} (auto-normalised
        #     to a list, injecting order_id from the key), or
        #   * None (DEFAULT) → get_orders() returns None (the SAFE sentinel → the
        #     reconciler falls back to the per-position floor). Existing tests
        #     unaffected.
        self._orders = orders
        self.ltps = ltps or {}
        self._quotes = quotes
        # FEATURE C: per-share MTF/MIS margin the mock reports (leverage). When
        # margins_available is False the margin API is simulated as DOWN → the
        # allocator must cash-fall-back (never over-deploy).
        self.margins = margins or {}
        self.margins_available = margins_available
        self.exit_delay_sec = exit_delay_sec
        self.fail_symbols = fail_symbols or set()
        self._lot_size = lot_size
        # FUTURES: per-lot margin the mock "broker" reports (None → base default
        # None → capital refuses to size, matching the real no-margin-API guard).
        self._fut_margin_per_lot = fut_margin_per_lot
        # FUTURES eligibility: symbols the mock has NO future for (filter drops).
        self._no_future_symbols = no_future_symbols or set()
        self._pending = pending_orders or []
        self.partial_fills = partial_fills or {}
        # EXIT ICEBERG (CLUSTER 8): after this many SUCCESSFUL exit placements of a
        # symbol, the next place_market_exit returns FAILED (models a mid-iceberg
        # child exit rejection). {symbol: n}. None (default) → no effect (existing
        # tests unaffected). The rejecting placement is NOT recorded in `exits`.
        self._exit_reject_after = exit_reject_after or {}
        self._exit_placement_counts: Dict[str, int] = {}
        self.placed: List[Any] = []
        self.exits: List[tuple] = []          # (symbol, qty) — unchanged shape
        self.exit_calls: List[dict] = []      # full exit args incl. direction
        self.cancelled: List[str] = []
        # Sync cancels (cancel_order_sync) — the exit-retry / software-stop path.
        # Tracked SEPARATELY from the async cancel_order book so a test can assert
        # a resting exit order was cancelled before a retry re-fired (Fix 2).
        self.sync_cancelled: List[str] = []
        # RECONCILIATION Phase 4: GTT-fill confirmation support.
        #   gtts        : {gtt_id: <kite get_gtt object dict>} — what get_gtt returns.
        #   order_status: {order_id: <kite order dict>} — what get_order_status
        #                 returns for the fired GTT order (status/filled_quantity/
        #                 average_price). None (default) → both no-op (base
        #                 behaviour), so existing tests are unaffected.
        self._gtts = gtts
        self._order_status_map = order_status
        # CLUSTER 5 ITEM 6 — call counters so a test can prove the kill flatten
        # resolves N legs from ONE batched get_orders() per cycle (not N per-leg
        # get_order_status scans).
        self.get_orders_calls = 0
        self.get_order_status_calls = 0

    # QUOTE-DRIVEN MARKETABLE-LIMIT: the live order book the mock reports.
    # {symbol: {ltp,bid,ask,upper_circuit,lower_circuit,ts}}. None (DEFAULT) →
    # get_quotes returns None (the SAFE sentinel = "no book" → the pricer uses a
    # MARKET fallback), matching a paper / uncertified broker. Existing tests
    # (no quotes arg) are unaffected.
    _quotes = None

    # market data
    def get_ltp(self, symbol: str) -> Optional[float]:
        return self.ltps.get(symbol)

    def get_quotes(self, symbols):
        # None (default) → base sentinel: no book. Otherwise return only the
        # requested symbols that we have a quote for (per-symbol absence is fine).
        if self._quotes is None:
            return None
        return {s: dict(self._quotes[s]) for s in symbols if s in self._quotes}

    def set_ltp(self, symbol: str, price: float) -> None:
        self.ltps[symbol] = price

    # RMS CAP 1: available margin (None → gate inert, paper-equivalent).
    def available_margin(self):
        return self._available_margin

    # RMS CAP 3: broker-side protective SL-M.
    def place_protective_slm(self, symbol, qty, trigger_price, direction="long",
                             product="MIS", exchange="NSE", client_tag=None):
        if not self._slm_available:
            return None
        self.slm_orders.append({"symbol": symbol, "qty": qty,
                                "trigger_price": trigger_price,
                                "direction": direction, "product": product,
                                "exchange": exchange, "client_tag": client_tag})
        return "slm-" + symbol

    def cancel_protective_slm(self, order_id):
        self.slm_cancelled.append(order_id)
        return True

    # margin (MTF / MIS leverage sizing)
    def get_margin_per_share(self, symbol: str, product: str = "MTF"):
        if not self.margins_available:
            return None
        return self.margins.get(symbol)

    def get_margins_batch(self, symbols, product: str = "MTF") -> dict:
        if not self.margins_available:
            return {}
        return {s: self.margins[s] for s in symbols if s in self.margins}

    # instrument master
    def get_lot_size(self, contract: str) -> int:
        return self._lot_size

    def get_active_futures(self, symbol: str, expiry_preference: str) -> str:
        if symbol in self._no_future_symbols:
            raise ValueError(f"no active futures for {symbol}")
        return f"{symbol}FUT"

    def get_active_futures_or_none(self, symbol: str, expiry_preference: str):
        if symbol in self._no_future_symbols:
            return None
        return f"{symbol}FUT"

    def get_fut_margin_per_lot(self, symbol: str,
                               expiry_preference: str = "near"):
        return self._fut_margin_per_lot

    def get_net_position_qty(self, symbol: str, instrument_type: str = "EQ"):
        # Fix B1: a configured symbol RAISES (models a live broker connection error
        # mid probe). The caller must ABORT the exit — never place a blind order.
        if symbol in self._net_probe_raise_symbols:
            raise ConnectionResetError(
                10054, "An existing connection was forcibly closed by the remote host")
        # None (default) → don't reconcile (base behaviour). A configured dict
        # answers the live net book for the pre-exit reconciliation guard.
        if self._net_positions is None:
            return None
        return self._net_positions.get(symbol)

    def get_positions_net(self):
        # None (default) → the reconciler treats it as "broker unreachable / paper"
        # and does NOTHING. A list is the raw Kite-shaped net book. A dict is
        # normalised to a list, injecting the symbol key as tradingsymbol.
        nb = self._net_book
        if nb is None:
            return None
        if isinstance(nb, dict):
            rows = []
            for sym, row in nb.items():
                r = dict(row)
                r.setdefault("tradingsymbol", sym)
                rows.append(r)
            return rows
        return list(nb)

    def get_orders(self):
        # None (default) → the reconciler treats the orderbook as unknown and falls
        # back to the per-position floor. A list is the raw Kite-shaped orderbook.
        # A dict {order_id: {...}} is normalised to a list, injecting order_id.
        self.get_orders_calls += 1
        ob = self._orders
        if ob is None:
            return None
        if isinstance(ob, dict):
            rows = []
            for oid, row in ob.items():
                r = dict(row)
                r.setdefault("order_id", oid)
                rows.append(r)
            return rows
        return list(ob)

    def get_holdings(self):
        # None (default) → reconciler treats delivery holdings as unknown here. A
        # list is the raw holdings book. A dict {symbol: {...}} is normalised.
        hd = self._holdings
        if hd is None:
            return None
        if isinstance(hd, dict):
            rows = []
            for sym, row in hd.items():
                r = dict(row)
                r.setdefault("tradingsymbol", sym)
                rows.append(r)
            return rows
        return list(hd)

    def get_option_chain(self, symbol: str) -> List[Any]:
        spot = self.ltps.get(symbol, 100.0)
        return [{"strike": s} for s in (spot - 10, spot, spot + 10)]

    def get_option_contract(self, symbol, strike, expiry_preference) -> str:
        return f"{symbol}{int(strike)}{expiry_preference[:2].upper()}"

    # order lifecycle
    async def place_order(self, order) -> OrderResult:
        self.placed.append(order)
        # CERT-BLOCK / RMS-refusal sim: FAILED, no order-id (broker-agnostic).
        if self._block_orders or order.symbol in self._block_symbols:
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=order.symbol, qty=order.qty,
                               error=(self._block_orders
                                      or f"blocked {order.symbol} (cert/RMS)"))
        # ICEBERG: reject this symbol's placement once it exceeds reject_after[sym]
        # successful placements (models a mid-iceberg child rejection). Accepted
        # (order-id issued) but 0 fill → the reconcile poll reports REJECTED.
        self._placement_counts[order.symbol] = \
            self._placement_counts.get(order.symbol, 0) + 1
        _ra = self._reject_after.get(order.symbol)
        if _ra is not None and self._placement_counts[order.symbol] > int(_ra):
            return OrderResult(status="PLACED",
                               broker_order_id="rej-" + order.symbol,
                               symbol=order.symbol, qty=order.qty,
                               filled_qty=0, avg_price=None)
        if order.symbol in self.reject_symbols:
            # Accepted (order_id issued) but NO fill — the reconcile poll will see
            # REJECTED. Mirrors a real exchange circuit-limit / RMS rejection.
            return OrderResult(status="PLACED", broker_order_id="ord-" + order.symbol,
                               symbol=order.symbol, qty=order.qty,
                               filled_qty=0, avg_price=None)
        if order.symbol in self.partial_fills:
            filled = self.partial_fills[order.symbol]
            return OrderResult(status="PARTIAL", broker_order_id="ord-" + order.symbol,
                               symbol=order.symbol, qty=order.qty,
                               filled_qty=filled,
                               avg_price=self.ltps.get(order.symbol))
        _avg = self.ltps.get(order.symbol)
        _seq = self._fill_price_sequence.get(order.symbol)
        if _seq:
            _idx = min(self._placement_counts[order.symbol] - 1, len(_seq) - 1)
            _avg = _seq[_idx]
        return OrderResult(status="PLACED", broker_order_id="ord-" + order.symbol,
                           symbol=order.symbol, qty=order.qty,
                           filled_qty=order.qty,
                           avg_price=_avg)

    async def get_pending_orders(self) -> List[Any]:
        return list(self._pending)

    async def cancel_order(self, order_id: str) -> Any:
        self.cancelled.append(order_id)
        return {"status": "CANCELLED", "order_id": order_id}

    def cancel_order_sync(self, order_id: str) -> bool:
        # Records the sync cancel (exit-retry / software-stop resting-order cancel)
        # so a test can prove the resting order was cancelled before a retry.
        self.sync_cancelled.append(order_id)
        return True

    # ── GTT-fill confirmation (RECONCILIATION Phase 4) ────────────────────────
    def get_gtt(self, gtt_id):
        # None (default) → base no-op. A configured map returns the full Kite
        # GTT object (status + orders[]) for the reconciler.
        if self._gtts is None:
            return None
        return self._gtts.get(str(gtt_id))

    def get_gtt_fill(self, gtt_id):
        # Mirror the LIVE ZerodhaBroker.get_gtt_fill flow: only a TRIGGERED GTT
        # yields positive fill evidence, resolved via the fired order-id ->
        # get_order_status. None otherwise (not triggered / no map / no fired id).
        if self._gtts is None:
            return None
        state = self._gtts.get(str(gtt_id))
        if not isinstance(state, dict):
            return None
        if str(state.get("status") or "").lower() != "triggered":
            return None
        order_id = None
        for leg in (state.get("orders") or []):
            result = leg.get("result") if isinstance(leg, dict) else None
            if isinstance(result, dict):
                order_id = result.get("order_id") or result.get("id")
            elif isinstance(result, str):
                order_id = result
            order_id = order_id or (leg.get("order_id") if isinstance(leg, dict) else None)
            if order_id:
                break
        if not order_id:
            return None
        order = self.get_order_status(str(order_id))
        if not order:
            return None
        return {
            "status": str(order.get("status") or "").upper(),
            "filled_quantity": int(order.get("filled_quantity") or 0),
            "average_price": float(order.get("average_price") or 0.0),
            "order_id": str(order_id),
        }

    def get_order_status(self, order_id: str) -> dict:
        self.get_order_status_calls += 1
        # RECONCILIATION Phase 4: an explicit order_status map takes precedence
        # (used to model a fired GTT order's fill state). {} = unknown/not found.
        if self._order_status_map is not None and str(order_id) in self._order_status_map:
            return dict(self._order_status_map[str(order_id)])
        # Synthetic COMPLETE fill for the exit order the confirm poller checks.
        # order_id shape is "exit-<SYMBOL>" (see place_market_exit). Report the
        # placed exit qty as fully filled so confirm_exit resolves COMPLETE for
        # non-dry mock brokers (matches a real broker's near-instant MARKET fill).
        # ENTRY reconcile probe ("ord-<SYM>"): a rejected symbol reports REJECTED
        # (0 fill) so _fire_one drops the leg instead of registering a phantom.
        if str(order_id).startswith("rej-"):
            # ICEBERG mid-iceberg reject: the forced-reject placement reports
            # REJECTED (0 fill) so the entry reconcile drops that child.
            return {"status": "REJECTED", "filled_quantity": 0,
                    "average_price": 0.0}
        if str(order_id).startswith("ord-"):
            esym = order_id[len("ord-"):]
            if esym in self.reject_symbols:
                return {"status": "REJECTED", "filled_quantity": 0,
                        "average_price": 0.0}
            return {"status": "COMPLETE",
                    "filled_quantity": 0, "average_price": 0.0}
        sym = order_id[len("exit-"):] if str(order_id).startswith("exit-") else None
        if sym is not None:
            total = sum(q for s, q in self.exits if s == sym)
            if total > 0:
                return {"status": "COMPLETE", "filled_quantity": int(total),
                        "average_price": float(self.ltps.get(sym) or 0.0)}
        return {"status": "COMPLETE", "filled_quantity": 0, "average_price": 0.0}

    async def place_market_exit(self, symbol: str, qty: int,
                                instrument_type: str,
                                kite_product: str | None = None,
                                direction: str = "long",
                                *, exec_cfg=None,
                                client_order_id: str | None = None) -> OrderResult:
        # kite_product accepted (and ignored) to match the real ZerodhaBroker
        # signature after the MTF-exit-product fix; kill_switch/exit paths pass it.
        # direction ("long"|"short") records the CLOSING side so futures-short
        # tests can assert a BUY-to-cover exit. Kept as an extra tuple element so
        # existing (symbol, qty) unpacking in equity tests stays valid via slicing.
        # exec_cfg (marketable-limit config) is accepted + recorded so exit tests
        # can assert it threads through; the mock always returns the same result
        # (it never places a real order).
        if self.exit_delay_sec:
            await asyncio.sleep(self.exit_delay_sec)
        # EXIT ICEBERG: reject this symbol's exit AFTER exit_reject_after[sym]
        # successful placements (mid-iceberg child rejection). Not recorded.
        _era = self._exit_reject_after.get(symbol)
        if _era is not None:
            if self._exit_placement_counts.get(symbol, 0) >= int(_era):
                return OrderResult(status="FAILED", broker_order_id=None,
                                   symbol=symbol, qty=qty,
                                   error="mock exit rejected (iceberg child)")
            self._exit_placement_counts[symbol] = \
                self._exit_placement_counts.get(symbol, 0) + 1
        self.exits.append((symbol, qty))
        self.exit_calls.append({"symbol": symbol, "qty": qty,
                                "instrument_type": instrument_type,
                                "kite_product": kite_product,
                                "direction": direction,
                                "exec_cfg": exec_cfg,
                                "client_order_id": client_order_id})
        if symbol in self.fail_symbols:
            return OrderResult(status="FAILED", broker_order_id=None,
                               symbol=symbol, qty=qty, error="mock failure")
        return OrderResult(status="PLACED", broker_order_id="exit-" + symbol,
                           symbol=symbol, qty=qty)
