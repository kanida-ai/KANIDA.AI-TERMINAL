"""TradingSessionConfig + BrokerProfile dataclasses, and DB-backed persistence.

Every user-configurable value lives in TradingSessionConfig — no hardcoded
trading parameters anywhere else. Secrets (api_key/secret/token) are NEVER
serialised to the DB: BrokerProfile carries them in memory only; persistence
strips them.

DEFAULTS ARE SAFE: kill_switch_enabled=False and mode defaults to paper at the
session layer. A freshly-constructed config places no real orders.
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))


def _now_ist_iso() -> str:
    return datetime.now(IST).isoformat()


# ── PROFIT STEP-LOCK ladder (intraday_basket trail) ─────────────────────────────
# A ratcheting profit-floor ladder: ascending (peak_threshold, lock_floor) rungs,
# ALL FRACTIONS OF ALLOCATED CAPITAL (the intraday_basket trail keys on
# compute_gross_return = uPnL/deployed capital). Once the peak G crosses a rung's
# peak_threshold the lock_floor is LOCKED IN (monotonic-up: peak only ratchets up,
# so the step floor never steps down). The DEFAULT matches a ₹5L / 5x setup exactly
# (peak ≥3%→lock 2% ; ≥5%→3.5% ; ≥8%→6% ; ≥12%→9.5% ; ≥16%→13%).
DEFAULT_STEP_LOCK_LADDER: List[List[float]] = [
    [0.03, 0.02], [0.05, 0.035], [0.08, 0.06], [0.12, 0.095], [0.16, 0.13],
]


def validate_step_lock_ladder(ladder: Any) -> None:
    """Validate a step-lock ladder shape (shared by config.validate() + the live
    config-edit PATCH endpoint). Raises ValueError on any violation.

    Rules: non-empty; each rung is a 2-tuple [peak_threshold, lock_floor] with
    0 < lock < peak < 1; strictly ASCENDING by peak; lock strictly ascending too
    (so the locked floor is genuinely monotonic-up as the peak climbs)."""
    if not isinstance(ladder, (list, tuple)) or len(ladder) == 0:
        raise ValueError("trail_step_lock_ladder must be a non-empty list of "
                         "[peak_threshold, lock_floor] rungs")
    prev_peak: Optional[float] = None
    prev_lock: Optional[float] = None
    for i, rung in enumerate(ladder):
        if not isinstance(rung, (list, tuple)) or len(rung) != 2:
            raise ValueError(
                f"trail_step_lock_ladder rung {i} must be a [peak, lock] pair, "
                f"got {rung!r}")
        try:
            peak = float(rung[0])
            lock = float(rung[1])
        except (TypeError, ValueError):
            raise ValueError(
                f"trail_step_lock_ladder rung {i} values must be numbers, "
                f"got {rung!r}")
        if not (0.0 < lock < peak < 1.0):
            raise ValueError(
                f"trail_step_lock_ladder rung {i} must satisfy 0 < lock < peak < 1 "
                f"(fractions of capital), got peak={peak}, lock={lock}")
        if prev_peak is not None and peak <= prev_peak:
            raise ValueError(
                "trail_step_lock_ladder peak_thresholds must be strictly ascending "
                f"(rung {i} peak {peak} <= previous {prev_peak})")
        if prev_lock is not None and lock <= prev_lock:
            raise ValueError(
                "trail_step_lock_ladder lock_floors must be strictly ascending "
                f"(rung {i} lock {lock} <= previous {prev_lock})")
        prev_peak, prev_lock = peak, lock


def _parse_clock_to_seconds(value: str) -> int:
    """Parse an IST clock string ("HH:MM" or "HH:MM:SS") to seconds-since-midnight.

    Used to validate ordering of entry_time vs square_off_time without binding to
    a specific date. Raises ValueError on an unparseable string.
    """
    s = (value or "").strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            t = datetime.strptime(s, fmt)
            return t.hour * 3600 + t.minute * 60 + t.second
        except ValueError:
            continue
    raise ValueError(f"unparseable clock time: {value!r}")


# ── BrokerProfile ──────────────────────────────────────────────────────────────

@dataclass
class BrokerProfile:
    profile_id: str
    broker_name: str                       # zerodha | fyers | upstox | angel | dhan
    allocated_capital: float = 0.0
    symbols: Optional[List[str]] = None    # None → use Falcon top_n
    rank_range: Optional[Tuple[int, int]] = None
    order_product: str = "CNC"             # CNC | MIS | MTF | NRML
    instrument_type: str = "EQ"            # EQ | FUT | CE | PE | MTF
    enabled: bool = True
    # PHASE-2 MULTI-TENANT (additive, NULLABLE): the vaulted broker account this
    # routing leg trades through. None → the PROCESS-GLOBAL operator creds path
    # (today's behaviour, byte-for-byte). When set, the broker adapter resolves
    # this account's api_key + access_token from the vault and builds its OWN
    # client (no shared global state). Carried in to_public_dict (it is an id,
    # NOT a secret).
    broker_account_id: Optional[str] = None
    # Secrets — in-memory only, NEVER persisted. Sourced from env / kite_tokens
    # OR, when broker_account_id is set, decrypted from the vault at build time.
    api_key: str = field(default="", repr=False)
    api_secret: str = field(default="", repr=False)
    access_token: str = field(default="", repr=False)
    # FIX A (real-money isolation, additive, in-memory only, NEVER persisted):
    # the OWNING portal user_id, stamped onto the profile at _build_brokers time
    # from the session's user_id. None → operator/global session (today's
    # global-fallback path). When set, the broker adapter refuses to build a LIVE
    # client that would fall back to the operator's global Kite account.
    owner_user_id: Optional[str] = field(default=None)
    # Whether the owning user is an ADMIN/operator. Admin-owned sessions may use
    # the operator's global broker account (the admin IS the operator); non-admin
    # owners are held to their OWN account (see the _build_kite isolation guard).
    # In-memory only, NEVER persisted.
    owner_is_admin: bool = field(default=False)

    def to_public_dict(self) -> Dict[str, Any]:
        """Serialisable form WITHOUT secrets."""
        return {
            "profile_id": self.profile_id,
            "broker_name": self.broker_name,
            "allocated_capital": self.allocated_capital,
            "symbols": self.symbols,
            "rank_range": list(self.rank_range) if self.rank_range else None,
            "order_product": self.order_product,
            "instrument_type": self.instrument_type,
            "enabled": self.enabled,
            "broker_account_id": self.broker_account_id,
            "creds_configured": bool(self.api_key and self.access_token),
        }

    @classmethod
    def from_public_dict(cls, d: Dict[str, Any]) -> "BrokerProfile":
        rr = d.get("rank_range")
        return cls(
            profile_id=d["profile_id"],
            broker_name=d["broker_name"],
            allocated_capital=float(d.get("allocated_capital", 0.0)),
            symbols=d.get("symbols"),
            rank_range=tuple(rr) if rr else None,
            order_product=d.get("order_product", "CNC"),
            instrument_type=d.get("instrument_type", "EQ"),
            enabled=bool(d.get("enabled", True)),
            broker_account_id=d.get("broker_account_id"),
        )


# ── TradingSessionConfig ────────────────────────────────────────────────────────

@dataclass
class TradingSessionConfig:
    # Capital
    total_allocated_capital: float

    # ── Strategy selector ────────────────────────────────────────────────────
    # "portfolio_kill_switch" (DEFAULT, UNCHANGED) = the existing flat basket
    #   kill switch (±kill_switch_pct on the invested basis).
    # "intraday_basket" = the basket-level TRAILING-PROFIT engine + square-off
    #   time (monitoring/trail_engine.py). The trail_* / stop_pct / arm_pct /
    #   floor_pct / square_off_time knobs below ONLY apply when this is selected;
    #   for portfolio_kill_switch they are inert. Additive + default-off: an
    #   existing session (no strategy in its config_json) loads as the kill-switch
    #   strategy and behaves exactly as before.
    # "tesla_short" = the order-flow-native intraday SHORT capital-ROTATION
    #   engine (backend/autotrade/strategies/tesla_short_engine.py +
    #   tesla_rotation.py). A live-signal seat model on EQ+MIS+SHORT ONLY: it
    #   reuses the intraday_basket EXIT machinery (per-seat step-lock trail +
    #   per-seat capital stop + the mandatory MIS ~15:12 buy-to-cover) but fills
    #   `n_seats` seats over time from live A++/A+++ signals and back-fills a freed
    #   seat on the next tick. Additive + default-off (no session runs until the
    #   operator creates one); paper (dry_run) simulates fills, no real orders.
    strategy: str = "portfolio_kill_switch"

    # Stock selection
    top_n_stocks: int = 5
    rank_filter: Optional[List[int]] = None

    # ── TESLA SHORT ROTATION (strategy=="tesla_short" only) ───────────────────
    # All inert unless the tesla_short strategy is selected.
    #   n_seats                    : max concurrent SHORT positions; the equal
    #                                allocation per seat = total_allocated_capital
    #                                / n_seats (a FIXED per-seat slice — the correct
    #                                rotation stop/trail denominator).
    #   tesla_min_grade            : "A++" (A++ and A+++) | "A+++" (strongest only).
    #   tesla_cooldown_minutes     : per-instrument re-entry spacing (batch = 30).
    #   tesla_personality_window_days : rolling K completed prior trading days used
    #                                for stock-personality + train-quality (the LIVE
    #                                replacement for the batch's hardcoded TRAIN_DAYS).
    #   tesla_allow_reentry        : re-enter a name already traded this session
    #                                (default False — conservative, avoids churn).
    #   tesla_signal_db_path       : override the universe poll DB path (None →
    #                                the engine default universe_engine DB).
    n_seats: int = 3
    tesla_min_grade: str = "A++"
    tesla_cooldown_minutes: int = 30
    tesla_personality_window_days: int = 5
    tesla_allow_reentry: bool = False
    tesla_signal_db_path: Optional[str] = None
    # Signal-cache staleness bound (seconds): if the once-per-minute signal
    # refresher has not successfully updated the cache within this many seconds
    # the tesla tick ABSTAINS from NEW seat entries (never enters on stale
    # signals) and pages — exits/square-off are unaffected. Default 90s (a
    # healthy refresh runs every minute). <= 0 disables the abstain (never
    # stale) — do NOT set 0 in live.
    tesla_signal_staleness_sec: int = 90
    # v3 DiD (Difference-in-Differences) ENTRY-SELECTION layer. OPT-IN, default
    # OFF → the deployed v2 Tesla path is byte-identical. When True the once-per-
    # minute signal recompute regrades candidates through the stricter v3_did_gate
    # (A++/A+++) and ranks seats by v3_rank_score (see strategies/tesla_did.py).
    # Entry-selection only: exits / the intraday step-lock trail are UNCHANGED.
    tesla_did_layer_enabled: bool = False

    # Position sizing
    sizing_mode: str = "equal"             # equal | pct_cap | manual
    max_pct_per_position: float = 0.05
    manual_amounts: Dict[str, float] = field(default_factory=dict)

    # Order type
    order_product: str = "CNC"             # CNC | MIS | MTF | NRML
    order_type: str = "MARKET"             # MARKET | LIMIT | VWAP
    limit_offset_pct: float = 0.001
    vwap_window_seconds: int = 60

    # ── QUOTE-DRIVEN MARKETABLE-LIMIT execution (additive, default-off) ────────
    # execution_mode controls HOW an entry/exit order is priced against the live
    # book. It is ORTHOGONAL to order_type / strategy — it only changes the
    # PRICING, not what/when we trade. The execution layer ALWAYS PLACES an order
    # (it never skips / hands the decision back).
    #   "market"           (DEFAULT, UNCHANGED, byte-for-byte) — today's behaviour:
    #                      raw MARKET orders priced off LTP only (market_protection).
    #                      The quote pricer is NEVER invoked; no extra network call.
    #   "marketable_limit" — read the live book (bid/ask/circuit) via ONE batched
    #                      broker.get_quotes() and send an in-band marketable-LIMIT
    #                      (ask+buffer for a BUY / bid-buffer for a SELL) CAPPED at
    #                      the exchange circuit band. A stock LOCKED at its upper
    #                      circuit is placed as a LIMIT exactly AT the circuit (a
    #                      valid, QUEUED order that fills the instant the lock
    #                      breaks — the 2026-07-06 CEMPRO fix; no rejection, no
    #                      dropped pick). No usable price at all → MARKET fallback.
    # SPRINT CLUSTER 5 ITEM 5: the SAFE default is now "marketable_limit" (the
    # circuit-aware execution path) instead of the raw "market" path. The env
    # FALCON_AUTOTRADE_EXECUTION_MODE still overrides (prod already pins it to
    # marketable_limit) and an explicit per-session value still wins; PAPER is
    # byte-identical because get_quotes() returns None off the live path → the
    # pricer takes the MARKET fallback (no LIMIT ever priced in paper).
    execution_mode: str = field(  # env-switchable DEFAULT (FALCON_AUTOTRADE_EXECUTION_MODE);
        default_factory=lambda: os.environ.get(   # an explicit per-session value still wins.
            "FALCON_AUTOTRADE_EXECUTION_MODE", "marketable_limit"))  # market | marketable_limit
    # ── QUOTE-FRESHNESS SLA for ENTRIES (SPRINT CLUSTER 5 ITEM 3) ─────────────
    # An ENTRY must be priced off a quote no older than this many seconds. A
    # stale quote (age > SLA) DEGRADES the leg rather than pricing a LIMIT off a
    # stale book: per entry_stale_quote_policy it either falls back to a MARKET
    # order ("market", DEFAULT — always place, never chase a stale book) or is
    # SKIPPED ("skip" — don't enter on stale data). EXITS never skip on staleness
    # (a needed exit always proceeds) — they only carry a degraded_quote flag.
    # Inert unless execution_mode=="marketable_limit"; paper never fetches a quote
    # (get_quotes None) so this is a no-op in paper.
    entry_quote_max_age_sec: float = 10.0
    entry_stale_quote_policy: str = "market"   # market | skip
    # ── CIRCUIT-LOCKED ENTRY policy (SPRINT CLUSTER 5 ITEM 4) ─────────────────
    # A stock LOCKED at its circuit at entry is placed as a LIMIT queued AT the
    # circuit. It DOES NOT sit indefinitely: _reconcile_entry_fill cancels the
    # unfilled order after the entry reconcile window and DROPS the leg (policy
    # "drop", DEFAULT — recommended for MIS: don't chase a locked stock). "queue"
    # (exempt from the reconcile-window cancel, hold to a cutoff) is STAGED-NEXT
    # and currently treated as "drop" with a log; the wording/comments in the
    # execution path reflect the DROP policy so code + logs + comments agree.
    entry_circuit_locked_policy: str = "drop"  # drop | queue(staged-next→drop)
    # marketable_buffer_pct (FRACTION, 0.003 = 0.3%): how far THROUGH the touch
    # the marketable-LIMIT crosses (ask+buffer for a BUY / bid-buffer for a SELL)
    # so it fills as fast as a MARKET order for liquid names. The order is then
    # CAPPED at the exchange circuit band (the only cap). Inert unless
    # execution_mode == "marketable_limit". There is NO skip path — a locked-up
    # stock is placed as a LIMIT exactly AT the circuit (valid, queued, fills when
    # the lock breaks); with no usable price the caller uses a MARKET fallback.
    marketable_buffer_pct: float = 0.003

    # ── WORKED-ORDER (participation / TWAP) execution (additive, DEFAULT-OFF) ──
    # execution_mode == "worked" WORKS a large entry BUILD / exit UNWIND into many
    # child slices PACED OVER TIME (backend/autotrade/execution/worked_order.py),
    # instead of a single market shot — required for very large size (₹cr/name)
    # where one order moves the price or exceeds the exchange FREEZE quantity. The
    # child pacing is POV (participation_pct × recent-interval volume) with a TWAP
    # FLOOR (guarantees progress when the volume read is stale/zero) and a hard
    # per-child FREEZE-QTY cap (reuses the iceberg freeze map). Each child goes
    # through the EXISTING safe place path (durable ledger, unique client_order_id,
    # query-before-retry, fill reconciliation). It PACES to limit impact — it does
    # NOT cap the TARGET and never drops a pick; a thin name partial-fills and the
    # SHORTFALL is surfaced. Inert unless execution_mode=="worked": "market" /
    # "marketable_limit" are byte-for-byte unchanged (one-shot). Paper simulates
    # child fills off the live LTP (no real orders).
    #   worked_participation_pct : FRACTION of recent-interval volume per child (POV).
    #   worked_interval_sec      : the pacing interval between children (seconds).
    #   worked_deadline          : IST clock ("HH:MM[:SS]") to stop working; None →
    #                              use square_off_time (the intraday flatten).
    #   worked_min_child_qty     : minimum child size (avoid dust), bounded by the
    #                              remaining qty + the freeze cap.
    #   worked_max_children      : hard safety cap on the number of child slices.
    # The per-child FREEZE cap reuses iceberg_freeze_qty_default / _map, so those
    # can be set independently of iceberg_enabled when execution_mode=="worked".
    worked_participation_pct: float = 0.10
    worked_interval_sec: int = 20
    worked_deadline: Optional[str] = None
    worked_min_child_qty: int = 1
    worked_max_children: int = 500

    # ── WORKED-ORDER v2: VWAP-CURVE PACING + ADAPTIVE PARTICIPATION (DEFAULT-OFF) ─
    # v1 paces FLAT (equal-ish per interval). Real intraday volume is U-SHAPED
    # (heavy at open + close, thin midday) so flat pacing UNDER-fills when the book
    # is deep and OVER-participates when it is thin. v2 paces to a per-symbol
    # intraday VOLUME PROFILE (normalized 5-min buckets over [09:15,15:30], built
    # from recent-N-days ohlc_1min) and ADAPTS to realized fills: each child brings
    # cumulative-filled up to where the volume curve says it SHOULD be by the end of
    # this interval — filling MORE when the market is deep (front/back of day), LESS
    # midday. Inert unless BOTH execution_mode=="worked" AND worked_vwap_enabled;
    # when off the child sizer is EXACTLY v1's flat POV (byte-identical). If a symbol
    # has no profile (thin history) it FALLS BACK to v1 flat POV — never fails.
    #   worked_vwap_enabled          : opt-in to the VWAP-curve sizer (else v1 flat).
    #   worked_max_participation_pct : HARD safety ceiling on the adaptive POV cap —
    #                                  when BEHIND schedule the sizer leans toward
    #                                  this cap to catch up, but NEVER blows past it
    #                                  regardless of schedule. FRACTION in (0, 1] and
    #                                  MUST be >= worked_participation_pct (the normal
    #                                  impact cap is the floor of the adaptive band).
    #   worked_vwap_profile_days     : lookback days of ohlc_1min for the profile.
    worked_vwap_enabled: bool = False
    worked_max_participation_pct: float = 0.25
    worked_vwap_profile_days: int = 20

    # ── MARK-STALENESS ABSTAIN (SPRINT CLUSTER 5 ITEM 2, real-money safety) ───
    # The mark (LTP) that drives the kill switch / trail can fall back to
    # yesterday's ohlc_daily close on a live-data outage. A daily-close mark must
    # NEVER satisfy an intraday PROFIT gate (trail/target). When the freshest mark
    # feeding the basket is older than this many seconds, the risk path ABSTAINS
    # from a PROFIT-side exit (trail/target) and flags the tick as stale — but the
    # DOWNSIDE stop (loss/STOP/square-off) still fires conservatively. 0 disables
    # the gate (pre-Cluster-5 behaviour). Paper is unaffected: every paper tick
    # marks to a live mock price so the mark is always fresh.
    mark_staleness_abstain_sec: int = 30

    # Instrument
    instrument_type: str = "EQ"            # EQ | FUT | CE | PE
    expiry_preference: str = "near"        # near | next | far

    # ── Trade direction (FUTURES long/short) ──────────────────────────────────
    # "long"  (DEFAULT, UNCHANGED) — entry BUY, exit SELL, P&L (ltp-avg)*qty,
    #         stop BELOW entry / target ABOVE. EVERY existing/equity session is
    #         "long" and behaves EXACTLY as before (byte-for-byte).
    # "short" (FUTURES ONLY, current phase) — entry SELL, exit BUY-to-cover,
    #         P&L (avg-ltp)*qty (profit when price falls), stop ABOVE entry /
    #         target BELOW. validate() rejects "short" unless instrument_type=="FUT".
    # NOTE: invested_basis (Σ qty*avg_price) stays POSITIVE for both directions;
    # only the P&L sign + order sides + stop/target orientation invert for short.
    direction: str = "long"                # long | short

    # Kill switch  (DISABLED by default — fail safe)
    kill_switch_pct: float = 0.012
    kill_switch_direction: str = "both"    # profit | loss | both
    kill_switch_enabled: bool = False
    # ── ASYMMETRIC kill switch (FEATURE B, additive + default-off) ────────────
    # Separate profit-target vs stop-loss thresholds. Both are FRACTIONS
    # (0.01 = 1%), validated in (0, 0.5] WHEN SET. Semantics:
    #   kill_switch_target_pct : the PROFIT side fires at gross_return >= this.
    #   kill_switch_stop_pct   : the LOSS   side fires at gross_return <= -this.
    # When None (DEFAULT) each side falls back to the symmetric kill_switch_pct,
    # so a session that sets neither behaves EXACTLY as today (byte-for-byte).
    # kill_switch_direction still gates which side(s) are live.
    kill_switch_target_pct: Optional[float] = None
    kill_switch_stop_pct: Optional[float] = None

    # ── RMS: PORTFOLIO DAILY-LOSS CIRCUIT BREAKER (SPRINT CLUSTER 4, CAP 2) ────
    # A portfolio-level (per-user) daily-loss breaker. When the aggregate
    # realised+unrealised loss across ALL of a user's LIVE sessions (of the same
    # mode) breaches EITHER configured limit, every one of the user's live
    # sessions is FLATTENED in one sweep. BOTH default None → DISABLED (a fresh
    # config never fires the breaker → byte-for-byte unchanged). FRACTIONS for the
    # pct (0.05 = 5% of the user's aggregate allocated capital); ₹ for the amount.
    # When several of the user's sessions set a limit, the TIGHTEST (smallest ₹
    # equivalent) wins (most protective). Validated in (0, 0.5] / > 0 WHEN SET.
    max_daily_loss_pct: Optional[float] = None
    max_daily_loss_amount: Optional[float] = None

    # ── RMS: pre-trade margin gate FAIL-CLOSED override (CLUSTER 9b ITEM 8) ─────
    # In LIVE mode the pre-trade gate REFUSES a deploy when the account's available
    # margin is unknown / the probe raised (real money never deploys on an
    # unverifiable budget). Set this True ONLY as an explicit operator override to
    # proceed on an unknown budget (e.g. a broker whose margin API is unreliable but
    # trusted). Default False = fail-closed. Paper/dry-run is unaffected either way.
    rms_allow_unknown_margin: bool = False

    # ── MULTI-ACCOUNT: break-glass GLOBAL fallback for an UNRESOLVABLE bound
    #    account (CLUSTER 9d FIX F6) ─────────────────────────────────────────────
    # When a profile SPECIFIED a broker_account_id that cannot be resolved (the
    # vault is disabled, or the account is absent / not-owned / decrypt-fails), the
    # LIVE build FAILS CLOSED by default — it refuses to build a client rather than
    # silently trade the operator's GLOBAL account in place of the intended one
    # (a wrong-account trade). Set this True ONLY as an explicit operator override
    # to allow that leg to fall back to the global operator creds. Default False =
    # fail-closed. Paper/dry-run is unaffected; a profile with NO specified account
    # (today's global path) is unchanged either way.
    break_glass_global: bool = False

    # ── RMS: MIS broker-side protective SL-M (SPRINT CLUSTER 4, CAP 3) ─────────
    # An MIS leg gets NO broker GTT (correct — a GTT would outlive the intraday
    # square-off). Instead place a broker-held DAY SL-M (stop-market) at the
    # per-position capital-basis stop level so an MIS position (especially a
    # SHORT, whose downside is otherwise software-only) has a HARD broker-side cap
    # even if the monitor process is down. Falcon-owned (compact tag) + cancelled
    # by Falcon at exit (day-validity auto-cancels anyway). Default ON for MIS;
    # inert for CNC/MTF/NRML (those carry a GTT). LIVE-only (paper places nothing).
    mis_protective_slm_enabled: bool = True

    # Per-position GTT-OCO broker backup (FEATURE 1). The portfolio kill switch
    # is the PRIMARY exit (software, ours); the per-position GTT is the broker-
    # held BACKUP floor. Widths default WIDER than kill_switch_pct so the
    # portfolio target usually fires first. LIVE-only: in paper mode no real GTT
    # is placed (the intended levels are still recorded for the UI).
    per_position_gtt_enabled: bool = True
    # Per-position broker GTT-OCO backstop. These are now FRACTIONS OF THE
    # POSITION'S CAPITAL (NOT price) — GTTManager converts them to the broker
    # PRICE trigger via the session leverage L (price_pct = capital_pct / L; on a
    # 5x MTF basket an 8% capital stop = a ~1.6% price stop, on a 1x CNC basket
    # L=1 so 8% capital = 8% price). Validated in (0, 0.5] like every other pct.
    # The GTT is a rare CATASTROPHE backstop only — basket-level exits
    # (arm/trail/stop) do the real work, so target is a wide profit backstop set
    # ABOVE the trail's top ladder rung.
    per_position_stop_pct: float = 0.08    # 8% of the position's CAPITAL (→ price via L)
    per_position_target_pct: float = 0.20  # 20% of the position's CAPITAL (→ price via L)

    # ── INTRADAY BASKET trailing engine (strategy=="intraday_basket" only) ────
    # All percentages are FRACTIONS (0.01 = 1%), validated in (0, 0.5], exactly
    # like kill_switch_pct. They drive monitoring/trail_engine.py over the
    # ALLOCATED-CAPITAL gross return G (compute_gross_return = uPnL/deployed
    # capital) — updated 2026-07-07 from the notional/invested basis so the knobs
    # mean "% of the trader's deployed money" regardless of product leverage (on
    # MIS 5x the old notional 2.5% arm was really 12.5% of capital). For a 1x CNC
    # basket capital ≈ invested, so this is a no-op there:
    #   arm_pct            : G >= +arm_pct → the trail ARMS (locks a floor).
    #   floor_pct          : the minimum locked floor once armed (default = arm).
    #   trail_giveback_pct : giveback from the peak G once armed.
    #   stop_pct           : downside hard stop, applied as -stop_pct (pre-arm).
    #   square_off_time    : IST clock time to flatten the basket (never overnight).
    # Inert when strategy != "intraday_basket".
    # Defaults updated 2026-07-04 to the VALIDATED basket-only config (530-day
    # backtest + MAE/MFE + 100-combo param grid): arm 2.5% / floor 1% /
    # giveback 1.5% / stop 3%. The WIDE 1.5% giveback lets winners ride to the
    # close (capturing the +2-3% runner days that carry the edge — ~80% of exits
    # are EOD); the higher 2.5% arm skips small blips; the -3% basket hard stop
    # replaces the whipsaw-prone tight/per-stock stops. Return-maximizing choice
    # per the doc; superseded the earlier arm2/floor1/give0.5/stop1.5 sweep values.
    # 2026-07-07: with the trail now on the ALLOCATED-CAPITAL basis, the default
    # arm is raised 0.025 -> 0.05 so a fresh/default intraday session arms at +5%
    # of deployed capital. floor/stop defaults read as floor +1% / hard-stop -3%
    # OF CAPITAL; the give-back default is 1.25% (2026-07-08) — a coherent set.
    arm_pct: float = 0.05
    floor_pct: float = 0.01
    # 2026-07-08: default give-back tightened 0.015 -> 0.0125 (1.25% of capital)
    # per the operator's worked-example set (§4 headline: peak 0.08 -> exit 0.0675
    # = ₹33,750 on ₹5L). Positional children still set 0.04 explicitly. Additive:
    # an existing session that persisted an explicit trail_giveback_pct keeps it.
    trail_giveback_pct: float = 0.0125
    stop_pct: float = 0.03
    # ── PROFIT STEP-LOCKING (ratcheting profit floor, intraday_basket trail) ──
    # Replaces the single fixed floor_pct with a STEP-LOCK LADDER floor(peak) that
    # ratchets UP in discrete steps as the peak G climbs, while STILL taking the
    # give-back level (peak - trail_giveback_pct, or the relative large-day
    # giveback) and exiting at the MAX of both. All values are FRACTIONS OF
    # ALLOCATED CAPITAL (same basis as the trail's g).
    #   trail_step_lock_enabled : True (DEFAULT — strictly-better ratchet). When
    #       FALSE (or the ladder is empty) the EXISTING fixed-floor decide path
    #       runs UNCHANGED, byte-identical — the backward-compat opt-out.
    #   trail_step_lock_ladder  : ascending [peak_threshold, lock_floor] rungs;
    #       step_lock_floor(peak) = the lock of the HIGHEST rung whose
    #       peak_threshold <= peak (0 below the first rung). ARM (when enabled)
    #       happens at the FIRST rung's peak_threshold (supersedes arm_pct).
    #   trail_large_peak_pct    : once peak >= this, the give-back switches from
    #       fixed (peak - trail_giveback_pct) to RELATIVE (peak * (1 - rel)) so a
    #       big-trend day trails a proportional distance and lets winners run.
    #   trail_large_giveback_rel: the relative give-back fraction for the large tier
    #       (0.175 → "trail ~17.5% from peak").
    trail_step_lock_enabled: bool = True
    trail_step_lock_ladder: List[List[float]] = field(
        default_factory=lambda: [list(r) for r in DEFAULT_STEP_LOCK_LADDER])
    trail_large_peak_pct: float = 0.20
    trail_large_giveback_rel: float = 0.175
    # ── STEP-LOCK SCOPE (basket vs per-stock, intraday_basket only) ────────────
    #   "basket" (DEFAULT) = EXACTLY today's behaviour, byte-identical: the SAME
    #       step-lock ladder + give-back run on the BASKET's capital-basis gross
    #       return G and, on EXIT, flatten the WHOLE basket.
    #   "stock"  = the SAME ladder + give-back run PER POSITION on each stock's
    #       g_stock (= position_uPnL / its capital slice), exiting individual
    #       stocks independently. Each position ratchets its OWN armed/peak
    #       (autotrade_positions.pos_trail_armed/pos_trail_peak). The SAFETY NETS
    #       stay basket-level: the time SQUARE_OFF and the catastrophic basket
    #       hard-stop (basket G <= -stop_pct) still flatten ALL positions. Only
    #       the PROFIT trail (arm/ratchet/trail-exit) moves per-stock.
    # Additive + default-off: an existing session (no key) loads as "basket".
    step_lock_scope: str = "basket"
    # Layer A — per-stock software stop (session.py _tick_intraday). OFF by default:
    # the validated config is BASKET-ONLY. Across 530 days a per-stock stop whipsawed
    # (cut a name at its stop that then recovered inside the basket), reducing return
    # at EVERY level. Set True only to run the (worse-returning) two-layer variant.
    per_stock_stop_enabled: bool = False
    # ── PER-STOCK CAPITAL-BASIS HARD STOP (step_lock_scope=="stock" only) ──────
    # 2026-07-09: an INDIVIDUAL-stock downside stop expressed as a FRACTION OF
    # THAT STOCK'S DEPLOYED CAPITAL (its capital slice = total_allocated_capital *
    # notional/Σnotional), NOT of price. It runs inside _run_per_stock_step_lock:
    # a position exits (reason STOP_STOCK) the moment its g_stock <= -this. Because
    # g_stock is already on the stock's capital slice, "3%" means 3% of the money
    # deployed on that name — leverage-correct, consistent with arm/give/ladder.
    # This is the stop that was MISSING: with step_lock_scope=="stock" the only
    # downside net was the basket-AGGREGATE stop (whole basket G <= -stop_pct), so
    # a single name drifting down (e.g. ATHERENERG -2.85% of capital on 2026-07-09,
    # 0.56% of PRICE at 5x) had NO per-name stop. 0.0 disables it (aggregate-only,
    # the pre-2026-07-09 behaviour). Inert when scope=="basket" (the basket exits
    # whole via the aggregate stop). Distinct from per_stock_stop_enabled (that
    # gates the OLDER PRICE-basis two-layer stop, still off by default).
    per_stock_stop_pct: float = 0.03
    square_off_time: str = "15:29:00"
    # ── INTRADAY vs POSITIONAL trailing (additive, default-on = today) ────────
    # Applies to strategy=="intraday_basket" only. Inert otherwise.
    #   True  (DEFAULT) = INTRADAY: force a time-based square-off at
    #                     square_off_time — today's behaviour, byte-for-byte.
    #   False           = POSITIONAL: NO forced time square-off; the trailing
    #                     floor + peak ratchet + downside hard stop + giveback
    #                     exit PERSIST across days (state on the session row).
    #                     Never an unprotected overnight position — STOP and
    #                     TRAIL/FLOOR exits stay fully active. Positional is
    #                     rejected for MIS (MIS must square off intraday); the
    #                     MIS defensive square-off (FEATURE A) always applies.
    square_off_enabled: bool = True

    # ── MULTI-SESSION MAX-HOLD CAP (positional only) ──────────────────────────
    # The Nth trading-session hard cap for a POSITIONAL basket
    # (strategy=="intraday_basket" + square_off_enabled=False). On the Nth NSE
    # trading day counting the ENTRY day as session 1, the WHOLE basket is
    # squared off at square_off_time REGARDLESS of trail arm/peak state — the one
    # deliberate deviation from "carry until the trail exits". Computed from the
    # PERSISTED entry timestamp (started_at) so it is durable across a restart.
    #   0 (DEFAULT) = NO cap = today's behaviour, byte-for-byte backward-compatible.
    #   1           = square off on the ENTRY day itself at square_off_time.
    #   3           = e.g. entry Fri (session 1) → Mon (2) → Tue (3) → flatten Tue.
    # Only meaningful for positional; on an INTRADAY (square_off_enabled=True)
    # session the daily square-off already flattens each day, so a >0 cap is
    # redundant — validate() ALLOWS it (int >= 0) but it is IGNORED by the
    # enforcement path (the intraday square-off fires first, well before day N).
    max_hold_sessions: int = 0

    # ── MIS DEFENSIVE SQUARE-OFF (FEATURE A, SAFETY) ──────────────────────────
    # Any session whose effective product is MIS (intraday) is squared off BY US
    # at this IST clock time — BEFORE the broker's compulsory ~15:20 auto-square
    # — REGARDLESS of strategy (portfolio_kill_switch OR intraday_basket). This
    # closes the hole where a MIS kill-switch session was never squared off by us
    # and rode to the broker's uncontrolled auto-square. Must be a parseable IST
    # clock AND strictly BEFORE square_off_time (so it fires ahead of the
    # intraday_basket 15:29). Inert for CNC/MTF/NRML sessions. Reuses the existing
    # square-off scheduler + kill_switch.fire flatten (close_reason MIS_SQUARE_OFF).
    mis_square_off_time: str = "15:12:00"

    # ── CAPITAL UTILIZATION (FEATURE C) ───────────────────────────────────────
    # After the initial equal/pct-cap allocation + integer share/lot rounding,
    # top up the AFFORDABLE picks with the unspent remainder (deterministic,
    # cheapest-first) so a floored slice doesn't strand cash, and SKIP a pick
    # whose single unit costs more than its slice (freeing its slice into the
    # top-up pass). NEVER over-deploys the total budget. Default ON; set False to
    # restore today's plain floor-each-slice behaviour exactly.
    redistribute_unused_capital: bool = True

    # ── ICEBERG large-order slicing (SPRINT CLUSTER 7, additive, DEFAULT-OFF) ──
    # Splits ONE logical entry/exit into child legs each <= the effective slice
    # cap so a single order above the per-symbol NSE FREEZE QUANTITY is never
    # rejected, and a large market order does not cause impact. INERT by default:
    # iceberg_enabled=False → a single order, byte-for-byte unchanged. When
    # enabled, the effective slice = MIN(configured slice, exchange freeze cap) so
    # slicing always respects the freeze qty even when the operator set no slice.
    #   iceberg_slice_qty         : explicit slice size (shares/units), optional.
    #   iceberg_slice_value       : a ₹-notional slice → qty = floor(value/price),
    #                               optional; needs a live price to resolve.
    #   iceberg_freeze_qty_default: a conservative per-order freeze cap used when
    #                               a symbol has no map entry (exchange freeze qty).
    #   iceberg_freeze_qty_map    : per-symbol freeze-qty overrides {symbol: qty}.
    # See backend/autotrade/iceberg.py for the pure planner.
    iceberg_enabled: bool = False
    iceberg_slice_qty: Optional[int] = None
    iceberg_slice_value: Optional[float] = None
    iceberg_freeze_qty_default: Optional[int] = None
    iceberg_freeze_qty_map: Optional[Dict[str, int]] = None

    # ── CONCENTRATION / FAT-FINGER LIMITS (SPRINT CLUSTER 8 ITEM 3) ────────────
    # Per-ORDER safety caps checked at sizing (_place_one) + the pre-trade layer.
    # A leg breaching a cap is CLAMPED down (fatfinger_policy="clamp", DEFAULT) or
    # REFUSED (policy="refuse") with a clear reason — e.g. a symbol_whitelist that
    # collapses to ONE name would otherwise pour the whole book into it. ALL DEFAULT
    # None → INERT (byte-for-byte unchanged) so nothing existing changes until an
    # operator opts in. Recommended conservative values (operator sets): a
    # max_pct_per_name ~0.35, fatfinger_max_notional_per_order ~2× a normal slice.
    #   max_pct_per_name               : max fraction of total_allocated_capital a
    #                                    SINGLE name's notional (qty×price) may take.
    #   fatfinger_max_notional_per_order: absolute ₹ notional cap for one order.
    #   fatfinger_max_qty_per_order     : absolute share/lot qty cap for one order.
    #   fatfinger_policy                : "clamp" (default, reduce qty to fit) |
    #                                    "refuse" (drop the leg with a reason).
    max_pct_per_name: Optional[float] = None
    fatfinger_max_notional_per_order: Optional[float] = None
    fatfinger_max_qty_per_order: Optional[int] = None
    fatfinger_policy: str = "clamp"

    # ── RISK BASIS LABEL (SPRINT CLUSTER 8 ITEM 3) ────────────────────────────
    # The explicit denominator the kill/return thresholds are measured against, so
    # the leverage math is unambiguous to the user. The kill switch fires on the
    # FROZEN INVESTED / NOTIONAL basis (Σ qty×avg), so the default is "notional";
    # "capital" (= the fund) and "margin" are accepted labels for display/reporting.
    # This is a LABEL only — it does NOT change the kill denominator (that stays the
    # frozen invested_basis in monitor/tick/ws_driver). Surfaced in preview/status.
    risk_basis: str = "notional"

    # ── Universe filter ──────────────────────────────────────────────────────
    # Restricts the Falcon pick pool to a named index membership before ranking.
    # Applied IN the SQL query (so ranked ordering respects the filtered set).
    # "all500" (default) = no filter = identical to current behaviour.
    universe_filter: str = "all500"

    # ── Symbol whitelist ─────────────────────────────────────────────────────
    # If set, only these symbols are traded (after the universe filter narrows
    # the pool). top_n_stocks still acts as an upper cap.
    # None (default) = no whitelist = current behaviour, unchanged.
    symbol_whitelist: Optional[List[str]] = None

    # Broker routing
    broker_profiles: List[BrokerProfile] = field(default_factory=list)

    # Entry timing
    entry_time: str = "09:15:00"
    entry_window_seconds: int = 60

    # ── EXECUTION-DATE / TRADING-DAY rule (real-money safety) ─────────────────
    # entry_date (optional "YYYY-MM-DD"): the calendar date on which the entry
    #   should fire @ entry_time. If SET it must be a real NSE trading day (else
    #   validate() rejects with the suggested next trading day). If UNSET the
    #   scheduler resolves it to the NEXT VALID trading session — today iff today
    #   is a trading day AND entry_time is still in the future, else the next
    #   trading day. This is what makes "set up today for tomorrow 09:15" real:
    #   set entry_date=<next trading day>.
    entry_date: Optional[str] = None
    # on_missed_window: what to do when the fire moment is missed or lands on a
    #   non-trading day.
    #     "expire" (DEFAULT, SAFE)        → do NOT fire; terminal EXPIRED status.
    #     "carry_next_trading_day"        → roll entry_date forward to the next
    #                                       trading day and stay SCHEDULED.
    on_missed_window: str = "expire"
    # entry_grace_seconds: if the target moment is in the PAST but it is STILL
    #   the same trading day, the market is OPEN, and we are within this many
    #   seconds of the target, fire anyway (covers a slightly-late wake / a
    #   "now" click a moment after the bell). Beyond the grace → expire/carry.
    entry_grace_seconds: int = 120

    # ── Validation ──────────────────────────────────────────────────────────
    def validate(self) -> None:
        if self.total_allocated_capital <= 0:
            raise ValueError("total_allocated_capital must be > 0")
        if self.sizing_mode not in ("equal", "pct_cap", "manual"):
            raise ValueError(f"invalid sizing_mode: {self.sizing_mode}")
        if self.order_type not in ("MARKET", "LIMIT", "VWAP"):
            raise ValueError(f"invalid order_type: {self.order_type}")
        # ── QUOTE-DRIVEN MARKETABLE-LIMIT + WORKED-ORDER execution ────────────
        if self.execution_mode not in ("market", "marketable_limit", "worked"):
            raise ValueError(
                "invalid execution_mode (market | marketable_limit | worked): "
                f"{self.execution_mode}")
        # WORKED-ORDER knobs — validated whenever SET (a saved preset must round-
        # trip valid values); the participation pct is a FRACTION in (0, 1].
        if not (0.0 < float(self.worked_participation_pct) <= 1.0):
            raise ValueError(
                "worked_participation_pct must be a fraction in (0, 1] "
                f"(e.g. 0.10 = 10% of interval volume), got "
                f"{self.worked_participation_pct}")
        if int(self.worked_interval_sec) < 1:
            raise ValueError(
                "worked_interval_sec must be an integer >= 1 (seconds), got "
                f"{self.worked_interval_sec}")
        if int(self.worked_min_child_qty) < 1:
            raise ValueError(
                "worked_min_child_qty must be an integer >= 1, got "
                f"{self.worked_min_child_qty}")
        if int(self.worked_max_children) < 1:
            raise ValueError(
                "worked_max_children must be an integer >= 1, got "
                f"{self.worked_max_children}")
        if self.worked_deadline is not None and str(self.worked_deadline).strip():
            try:
                _parse_clock_to_seconds(self.worked_deadline)
            except ValueError as e:
                raise ValueError(f"worked_deadline {e}")
        # WORKED v2 (VWAP-curve) — the adaptive safety CEILING is a FRACTION in
        # (0, 1] and MUST be >= worked_participation_pct (the normal impact cap is
        # the FLOOR of the adaptive band; a ceiling below the floor is nonsensical
        # and, mis-scaled, would silently DISABLE catch-up). Validated whenever set
        # so a saved preset round-trips valid values regardless of worked_vwap_enabled.
        if not (0.0 < float(self.worked_max_participation_pct) <= 1.0):
            raise ValueError(
                "worked_max_participation_pct must be a fraction in (0, 1] "
                f"(e.g. 0.25 = 25% of interval volume), got "
                f"{self.worked_max_participation_pct}")
        if float(self.worked_max_participation_pct) < float(
                self.worked_participation_pct):
            raise ValueError(
                "worked_max_participation_pct (adaptive ceiling) must be >= "
                "worked_participation_pct (normal impact cap): got "
                f"{self.worked_max_participation_pct} < "
                f"{self.worked_participation_pct}")
        if int(self.worked_vwap_profile_days) < 1:
            raise ValueError(
                "worked_vwap_profile_days must be an integer >= 1, got "
                f"{self.worked_vwap_profile_days}")
        # marketable_buffer_pct is a FRACTION in (0, 0.5] (same units as every
        # other pct); a mis-scaled 5.0 would push the "marketable" price 500%
        # through the touch (before the circuit cap) — reject at the door. It is
        # only meaningful for marketable_limit but a saved preset must round-trip
        # a valid value regardless.
        if not (0.0 < float(self.marketable_buffer_pct) <= 0.5):
            raise ValueError(
                "marketable_buffer_pct must be a fraction in (0, 0.5] "
                f"(e.g. 0.003 = 0.3%), got {self.marketable_buffer_pct}")
        # ── QUOTE-FRESHNESS SLA + CIRCUIT/STALENESS policies (CLUSTER 5) ──────
        if float(self.entry_quote_max_age_sec) <= 0:
            raise ValueError(
                "entry_quote_max_age_sec must be > 0 seconds (the entry quote "
                f"freshness SLA), got {self.entry_quote_max_age_sec}")
        if self.entry_stale_quote_policy not in ("market", "skip"):
            raise ValueError(
                "entry_stale_quote_policy must be 'market' or 'skip', got "
                f"{self.entry_stale_quote_policy!r}")
        if self.entry_circuit_locked_policy not in ("drop", "queue"):
            raise ValueError(
                "entry_circuit_locked_policy must be 'drop' or 'queue', got "
                f"{self.entry_circuit_locked_policy!r}")
        if int(self.mark_staleness_abstain_sec) < 0:
            raise ValueError(
                "mark_staleness_abstain_sec must be an integer >= 0 "
                f"(0 disables), got {self.mark_staleness_abstain_sec}")
        if self.instrument_type not in ("EQ", "FUT", "CE", "PE", "MTF"):
            raise ValueError(f"invalid instrument_type: {self.instrument_type}")
        # OPTIONS (CE/PE) are half-wired but NOT certified — the ATM-strike /
        # premium / options-margin sizing path is unvalidated. Hard-block them at
        # the door (default off) so a raw operator-token API call or a rehydrated
        # preset can never create an options session. Mirrors the short-requires-FUT
        # gate. Flip FALCON_AUTOTRADE_OPTIONS_ENABLED=true only once certified.
        if self.instrument_type in ("CE", "PE") and \
                os.environ.get("FALCON_AUTOTRADE_OPTIONS_ENABLED", "").strip().lower() \
                not in ("1", "true", "yes", "on"):
            raise ValueError(
                "options (CE/PE) are not yet certified — set "
                "FALCON_AUTOTRADE_OPTIONS_ENABLED=true to enable once validated")
        # ── Trade direction (FUTURES long/short) ──────────────────────────────
        if self.direction not in ("long", "short"):
            raise ValueError(
                f"invalid direction (long | short): {self.direction}")
        if self.order_product not in ("CNC", "MIS", "NRML", "MTF"):
            raise ValueError(f"invalid order_product: {self.order_product}")
        # SHORT is supported for FUTURES (any product) and for EQUITY INTRADAY
        # ONLY (instrument_type=="EQ" AND order_product=="MIS"). Everything else
        # is rejected at the door so we never place a sell-to-open on an
        # instrument whose margin / cover semantics are unsafe or illegal:
        #   • EQ + CNC  → short-DELIVERY is illegal on NSE cash (can't deliver
        #                 shares you don't own; broker auto-square + penalty).
        #   • EQ + MTF  → MTF is a LONG-ONLY leverage (margin-funding) product;
        #                 you cannot short on MTF.
        #   • CE / PE   → options short is a separate, uncertified phase.
        # An EQ+MIS short MUST be bought-to-cover intraday before the broker's
        # ~15:20 auto-square (enforced by square_off_time defaulting well before
        # that, plus the intraday square-off machinery — see session/monitor).
        if self.direction == "short":
            _short_ok = (
                self.instrument_type == "FUT"
                or (self.instrument_type == "EQ" and self.order_product == "MIS")
            )
            if not _short_ok:
                raise ValueError(
                    "short is supported only for FUT, or for equity INTRADAY "
                    "(instrument_type=EQ + order_product=MIS). Got "
                    f"instrument_type={self.instrument_type}, "
                    f"order_product={self.order_product}. Equity short on CNC "
                    "(illegal short-delivery) and MTF (long-only leverage) is "
                    "rejected.")
        # PRODUCT × INSTRUMENT compatibility (real-money safety). NRML is an
        # F&O / currency / commodity CARRY product — the broker REJECTS it on NSE
        # cash equity ("Trading in NSE is not allowed using NRML product type").
        # An equity session (EQ / MTF instrument) must use CNC / MIS / MTF, else
        # every order is rejected at the broker (2026-07-01 incident: an EQ
        # intraday_basket configured NRML → all 5 orders rejected).
        if self.instrument_type in ("EQ", "MTF") and self.order_product == "NRML":
            raise ValueError(
                "order_product 'NRML' is invalid for equity (NSE cash) — the "
                "broker rejects it. Use CNC (delivery), MIS (intraday), or MTF.")
        if self.kill_switch_direction not in ("profit", "loss", "both"):
            raise ValueError(f"invalid kill_switch_direction: {self.kill_switch_direction}")
        if self.top_n_stocks <= 0:
            raise ValueError("top_n_stocks must be > 0")
        if self.strategy not in ("portfolio_kill_switch", "intraday_basket",
                                 "tesla_short"):
            raise ValueError(f"invalid strategy: {self.strategy}")
        # Defensive units check: these percentages are FRACTIONS (0.01 = 1%), not
        # whole-number percents. The UI has historically sent 1.0 (intending
        # "100%"), which would make the kill switch effectively never fire — a
        # silent no-fire. Reject obviously-mis-scaled values at the door.
        if self.kill_switch_enabled:
            if not (0.0 < self.kill_switch_pct <= 0.5):
                raise ValueError(
                    "kill_switch_pct must be a fraction (e.g. 0.01 = 1%), "
                    f"got {self.kill_switch_pct}"
                )
        # ── ASYMMETRIC kill switch (FEATURE B): validate each override WHEN SET,
        # regardless of enabled (a saved preset must round-trip valid values).
        for _nm, _v in (("kill_switch_target_pct", self.kill_switch_target_pct),
                        ("kill_switch_stop_pct", self.kill_switch_stop_pct)):
            if _v is not None and not (0.0 < float(_v) <= 0.5):
                raise ValueError(
                    f"{_nm} must be a fraction in (0, 0.5] (e.g. 0.01 = 1%) "
                    f"when set, got {_v}")
        # ── RMS: portfolio daily-loss breaker (CAP 2) — validate WHEN SET ──────
        # A mis-scaled pct (1.0 for "100%") would move the effective breaker so
        # far it never fires — reject at the door like every other fraction. The
        # amount is a positive ₹ figure. Both remain None (disabled) by default.
        if self.max_daily_loss_pct is not None and \
                not (0.0 < float(self.max_daily_loss_pct) <= 0.5):
            raise ValueError(
                "max_daily_loss_pct must be a fraction in (0, 0.5] "
                f"(e.g. 0.05 = 5%) when set, got {self.max_daily_loss_pct}")
        if self.max_daily_loss_amount is not None and \
                float(self.max_daily_loss_amount) <= 0:
            raise ValueError(
                "max_daily_loss_amount must be > 0 (₹) when set, got "
                f"{self.max_daily_loss_amount}")
        if self.per_position_gtt_enabled:
            if not (0.0 < self.per_position_stop_pct <= 0.5):
                raise ValueError(
                    "per_position_stop_pct must be a fraction (e.g. 0.03 = 3%), "
                    f"got {self.per_position_stop_pct}"
                )
            if not (0.0 < self.per_position_target_pct <= 0.5):
                raise ValueError(
                    "per_position_target_pct must be a fraction (e.g. 0.06 = 6%), "
                    f"got {self.per_position_target_pct}"
                )
        # INTRADAY BASKET knobs — only validated when the strategy is selected,
        # so an existing kill-switch session is never blocked by these defaults.
        if self.strategy == "intraday_basket":
            for nm, v in (("arm_pct", self.arm_pct),
                          ("floor_pct", self.floor_pct),
                          ("trail_giveback_pct", self.trail_giveback_pct),
                          ("stop_pct", self.stop_pct)):
                if not (0.0 < float(v) <= 0.5):
                    raise ValueError(
                        f"{nm} must be a fraction in (0, 0.5] (e.g. 0.01 = 1%), "
                        f"got {v}")
            if self.floor_pct > self.arm_pct + 1e-12:
                raise ValueError(
                    f"floor_pct ({self.floor_pct}) must be <= arm_pct "
                    f"({self.arm_pct})")
            # ── PROFIT STEP-LOCK ──────────────────────────────────────────────
            # The large-day give-back tier params are always range-checked (a
            # saved preset must round-trip valid values); the ladder shape is
            # validated only when step-locking is ACTIVE (enabled AND non-empty)
            # — an empty ladder or enabled=False is the fixed-floor opt-out.
            if not (0.0 < float(self.trail_large_peak_pct) <= 0.5):
                raise ValueError(
                    "trail_large_peak_pct must be a fraction in (0, 0.5] "
                    f"(e.g. 0.20 = 20%), got {self.trail_large_peak_pct}")
            if not (0.0 < float(self.trail_large_giveback_rel) < 1.0):
                raise ValueError(
                    "trail_large_giveback_rel must be a fraction in (0, 1) "
                    f"(e.g. 0.175 = trail 17.5% from peak), got "
                    f"{self.trail_large_giveback_rel}")
            if self.trail_step_lock_enabled and self.trail_step_lock_ladder:
                validate_step_lock_ladder(self.trail_step_lock_ladder)
            # STEP-LOCK SCOPE: basket (whole-basket exit) | stock (per-position).
            if self.step_lock_scope not in ("basket", "stock"):
                raise ValueError(
                    "step_lock_scope must be 'basket' or 'stock', got "
                    f"{self.step_lock_scope!r}")
            # PER-STOCK CAPITAL STOP: a FRACTION in [0, 0.5]; 0 disables it.
            if not (0.0 <= float(self.per_stock_stop_pct) <= 0.5):
                raise ValueError(
                    "per_stock_stop_pct must be a fraction in [0, 0.5] "
                    "(e.g. 0.03 = 3% of the stock's capital; 0 disables), got "
                    f"{self.per_stock_stop_pct}")
            # Times must parse and square-off must be strictly after entry.
            try:
                entry_s = _parse_clock_to_seconds(self.entry_time)
            except ValueError as e:
                raise ValueError(f"entry_time {e}")
            try:
                sq_s = _parse_clock_to_seconds(self.square_off_time)
            except ValueError as e:
                raise ValueError(f"square_off_time {e}")
            if sq_s <= entry_s:
                raise ValueError(
                    f"square_off_time ({self.square_off_time}) must be after "
                    f"entry_time ({self.entry_time})")
            # Basket-size bound applies to the ACTUAL traded set. A custom
            # symbol_whitelist DEFINES the basket (top_n_stocks is then only the
            # picker's browse depth), so validate the whitelist size; else
            # validate top_n_stocks. The upper bound was raised 10 -> 50 (the max
            # the Top-50 picker/ranker surfaces) so a session can spread MORE
            # capital across MORE names: sizing redistributes across whatever
            # picks exist and never over-deploys, router_cap uses len(picks), and
            # the basket trail is size-agnostic. A floor of 3 keeps the basket
            # trail meaningful. (Beyond 50 needs the ranker to surface more names.)
            _basket_n = (len(self.symbol_whitelist)
                         if self.symbol_whitelist else self.top_n_stocks)
            if _basket_n < 3 or _basket_n > 50:
                _src = ("custom selection" if self.symbol_whitelist
                        else "top_n_stocks")
                raise ValueError(
                    "intraday_basket basket size must be 3..50 names "
                    f"({_src}), got {_basket_n}")
            # POSITIONAL (no forced square-off) is incompatible with MIS: an MIS
            # position CANNOT be carried overnight — the broker compulsorily
            # squares it off intraday — so a "positional MIS" is a contradiction
            # that would strand the session expecting a carry that can't happen.
            # (The MIS defensive square-off from FEATURE A still always applies.)
            if self.square_off_enabled is False and self.is_intraday_product():
                raise ValueError(
                    "positional (no square-off) is not allowed for MIS — MIS "
                    "must square off intraday")
        # ── TESLA SHORT ROTATION — validate when the strategy is selected ─────
        # Reuses the equity-MIS-SHORT gate (EQ+MIS+short ONLY) + the intraday
        # trail EXIT knobs, plus the seat knobs. Additive: never blocks other
        # strategies.
        if self.strategy == "tesla_short":
            if self.direction != "short":
                raise ValueError(
                    "tesla_short is a SHORT strategy — set direction='short'")
            if not (self.instrument_type == "EQ" and self.order_product == "MIS"):
                raise ValueError(
                    "tesla_short trades EQUITY INTRADAY ONLY — set "
                    "instrument_type='EQ' and order_product='MIS' (got "
                    f"instrument_type={self.instrument_type}, "
                    f"order_product={self.order_product})")
            if int(self.n_seats) < 1:
                raise ValueError(f"n_seats must be >= 1, got {self.n_seats}")
            if self.tesla_min_grade not in ("A++", "A+++"):
                raise ValueError(
                    "tesla_min_grade must be 'A++' or 'A+++', got "
                    f"{self.tesla_min_grade!r}")
            if int(self.tesla_cooldown_minutes) < 0:
                raise ValueError(
                    "tesla_cooldown_minutes must be an integer >= 0, got "
                    f"{self.tesla_cooldown_minutes}")
            if int(self.tesla_personality_window_days) < 1:
                raise ValueError(
                    "tesla_personality_window_days must be an integer >= 1, got "
                    f"{self.tesla_personality_window_days}")
            # MIS is intraday → a forced square-off is mandatory (no overnight).
            if self.square_off_enabled is False:
                raise ValueError(
                    "tesla_short is MIS intraday — square_off_enabled must be True")
            # Reuse the intraday trail EXIT knobs (per-seat step-lock + stop).
            for nm, v in (("arm_pct", self.arm_pct),
                          ("floor_pct", self.floor_pct),
                          ("trail_giveback_pct", self.trail_giveback_pct),
                          ("stop_pct", self.stop_pct)):
                if not (0.0 < float(v) <= 0.5):
                    raise ValueError(
                        f"{nm} must be a fraction in (0, 0.5] (e.g. 0.01 = 1%), "
                        f"got {v}")
            if not (0.0 <= float(self.per_stock_stop_pct) <= 0.5):
                raise ValueError(
                    "per_stock_stop_pct must be a fraction in [0, 0.5] "
                    f"(0 disables), got {self.per_stock_stop_pct}")
            if self.trail_step_lock_enabled and self.trail_step_lock_ladder:
                validate_step_lock_ladder(self.trail_step_lock_ladder)
            try:
                _e_s = _parse_clock_to_seconds(self.entry_time)
                _q_s = _parse_clock_to_seconds(self.square_off_time)
            except ValueError as e:
                raise ValueError(f"tesla_short clock {e}")
            if _q_s <= _e_s:
                raise ValueError(
                    f"square_off_time ({self.square_off_time}) must be after "
                    f"entry_time ({self.entry_time})")

        # MULTI-SESSION MAX-HOLD CAP: a non-negative integer (0 = no cap).
        # Meaningful only for a POSITIONAL intraday_basket; on an intraday
        # (square_off_enabled=True) session it is redundant (the daily square-off
        # flattens first) — ALLOWED but IGNORED there, per the design note.
        if int(self.max_hold_sessions) < 0:
            raise ValueError(
                "max_hold_sessions must be an integer >= 0 "
                f"(0 = no cap), got {self.max_hold_sessions}")
        if self.sizing_mode == "manual":
            total = sum(self.manual_amounts.values())
            if total > self.total_allocated_capital + 1e-6:
                raise ValueError(
                    f"manual_amounts total {total} exceeds "
                    f"total_allocated_capital {self.total_allocated_capital}"
                )
        # ── EXECUTION-DATE / TRADING-DAY rule ────────────────────────────────
        if self.on_missed_window not in ("expire", "carry_next_trading_day"):
            raise ValueError(
                "invalid on_missed_window (expire | carry_next_trading_day): "
                f"{self.on_missed_window}")
        # ── ICEBERG large-order slicing — validate WHEN SET; inert by default ──
        # Each source is validated whenever provided (a saved preset must round-
        # trip valid values). When ENABLED, at least one slice source must exist,
        # else "enabled" would silently do nothing (no cap → a single order).
        if self.iceberg_slice_qty is not None:
            if int(self.iceberg_slice_qty) < 1:
                raise ValueError(
                    "iceberg_slice_qty must be an integer >= 1 when set, got "
                    f"{self.iceberg_slice_qty}")
        if self.iceberg_slice_value is not None:
            if float(self.iceberg_slice_value) <= 0:
                raise ValueError(
                    "iceberg_slice_value must be > 0 (₹) when set, got "
                    f"{self.iceberg_slice_value}")
        if self.iceberg_freeze_qty_default is not None:
            if int(self.iceberg_freeze_qty_default) < 1:
                raise ValueError(
                    "iceberg_freeze_qty_default must be an integer >= 1 when set, "
                    f"got {self.iceberg_freeze_qty_default}")
        if self.iceberg_freeze_qty_map is not None:
            for _sym, _q in self.iceberg_freeze_qty_map.items():
                if int(_q) < 1:
                    raise ValueError(
                        "iceberg_freeze_qty_map values must be integers >= 1, got "
                        f"{_sym}={_q}")
        if self.iceberg_enabled and self.iceberg_slice_qty is None and \
                self.iceberg_slice_value is None and \
                self.iceberg_freeze_qty_default is None and \
                not self.iceberg_freeze_qty_map:
            raise ValueError(
                "iceberg_enabled requires a slice source: set iceberg_slice_qty, "
                "iceberg_slice_value, iceberg_freeze_qty_default, or "
                "iceberg_freeze_qty_map (else slicing would never engage)")
        # Concentration / fat-finger limits (SPRINT CLUSTER 8 ITEM 3)
        if self.max_pct_per_name is not None and \
                not (0.0 < float(self.max_pct_per_name) <= 1.0):
            raise ValueError(
                "max_pct_per_name must be a fraction in (0, 1] when set, got "
                f"{self.max_pct_per_name}")
        if self.fatfinger_max_notional_per_order is not None and \
                float(self.fatfinger_max_notional_per_order) <= 0:
            raise ValueError(
                "fatfinger_max_notional_per_order must be > 0 (₹) when set, got "
                f"{self.fatfinger_max_notional_per_order}")
        if self.fatfinger_max_qty_per_order is not None and \
                int(self.fatfinger_max_qty_per_order) < 1:
            raise ValueError(
                "fatfinger_max_qty_per_order must be an integer >= 1 when set, got "
                f"{self.fatfinger_max_qty_per_order}")
        if self.fatfinger_policy not in ("clamp", "refuse"):
            raise ValueError(
                "fatfinger_policy must be 'clamp' or 'refuse', got "
                f"{self.fatfinger_policy!r}")
        if self.risk_basis not in ("capital", "notional", "margin"):
            raise ValueError(
                "risk_basis must be one of capital|notional|margin, got "
                f"{self.risk_basis!r}")
        # Universe filter
        _VALID_UNIVERSE_FILTERS = ("all500", "nifty50", "nifty100", "nifty200", "fno")
        if self.universe_filter not in _VALID_UNIVERSE_FILTERS:
            raise ValueError(
                f"Invalid universe_filter: {self.universe_filter!r}. "
                f"Must be one of {_VALID_UNIVERSE_FILTERS}")
        # Symbol whitelist
        if self.symbol_whitelist is not None and len(self.symbol_whitelist) == 0:
            raise ValueError("symbol_whitelist cannot be empty if provided")
        # ── MIS DEFENSIVE SQUARE-OFF (FEATURE A): the mis_square_off_time must
        # parse and, for an intraday_basket session, must be strictly BEFORE the
        # basket square_off_time so the MIS defensive flatten fires ahead of the
        # 15:29 basket flatten (and always ahead of the broker's ~15:20 window).
        try:
            _mis_s = _parse_clock_to_seconds(self.mis_square_off_time)
        except ValueError as e:
            raise ValueError(f"mis_square_off_time {e}")
        if self.strategy in ("intraday_basket", "tesla_short"):
            try:
                _sq_s = _parse_clock_to_seconds(self.square_off_time)
            except ValueError:
                _sq_s = None
            if _sq_s is not None and _mis_s >= _sq_s:
                raise ValueError(
                    f"mis_square_off_time ({self.mis_square_off_time}) must be "
                    f"strictly before square_off_time ({self.square_off_time})")
        # entry_time must parse (it does for intraday already; enforce always).
        try:
            _parse_clock_to_seconds(self.entry_time)
        except ValueError as e:
            raise ValueError(f"entry_time {e}")
        if self.entry_date is not None and str(self.entry_date).strip():
            # Shape + trading-day check. A non-trading-day entry_date is rejected
            # AT CREATE with the suggested next trading day, so the operator never
            # schedules a fire into a closed market.
            from . import trading_calendar as _cal
            try:
                _d = datetime.strptime(str(self.entry_date).strip(),
                                       "%Y-%m-%d").date()
            except ValueError:
                raise ValueError(
                    f"entry_date must be YYYY-MM-DD, got {self.entry_date!r}")
            # COVERAGE GUARD (real-money safety): refuse to SCHEDULE into a year
            # whose NSE holidays we don't authoritatively know — else
            # is_trading_day() could wrongly call a holiday a trading day and we'd
            # schedule a real trade on it. Raises CalendarCoverageError (a clear,
            # distinct type) naming the year + the remedy. NO-OP for covered years.
            _cal.assert_calendar_covers(_d)
            if not _cal.is_trading_day(_d):
                nxt = _cal.next_trading_day(_d, inclusive=True)
                raise ValueError(
                    f"entry_date {self.entry_date} is NOT an NSE trading day "
                    f"(weekend/holiday). Next trading day: {nxt.isoformat()}")

    # ── EXECUTION-DATE resolution ─────────────────────────────────────────────
    def resolve_fire_datetime(self, now_ist: Optional[datetime] = None
                              ) -> "datetime":
        """The IST-aware datetime at which this session SHOULD fire entries.

        * entry_date SET  → that date @ entry_time (validate() already proved it
                            is a trading day).
        * entry_date UNSET → the NEXT VALID trading session: TODAY @ entry_time
                            iff today is a trading day AND entry_time is still in
                            the future; otherwise the NEXT trading day @
                            entry_time.

        Determinism: pass now_ist in tests. Pure (no side effects)."""
        from . import trading_calendar as _cal
        now = now_ist or datetime.now(IST)
        secs = _parse_clock_to_seconds(self.entry_time)
        hh, mm, ss = secs // 3600, (secs % 3600) // 60, secs % 60

        def _at(d) -> "datetime":
            return datetime(d.year, d.month, d.day, hh, mm, ss, tzinfo=IST)

        if self.entry_date and str(self.entry_date).strip():
            d = datetime.strptime(str(self.entry_date).strip(), "%Y-%m-%d").date()
            return _at(d)
        # UNSET → next valid trading session.
        today = now.date()
        if _cal.is_trading_day(today) and _at(today) > now:
            return _at(today)
        return _at(_cal.next_trading_day(today))

    # ── MIS product detection (FEATURE A) ─────────────────────────────────────
    def is_intraday_product(self) -> bool:
        """True when this session's EFFECTIVE product is MIS (intraday), on EITHER
        the session-level order_product OR ANY enabled broker_profile's
        order_product. MIS sessions are squared off defensively before the broker
        window regardless of strategy. CNC/MTF/NRML → False (unchanged)."""
        if str(getattr(self, "order_product", "")).upper() == "MIS":
            return True
        for bp in (self.broker_profiles or []):
            if not getattr(bp, "enabled", True):
                continue
            if str(getattr(bp, "order_product", "")).upper() == "MIS":
                return True
        return False

    # ── (de)serialisation (secrets stripped) ─────────────────────────────────
    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["broker_profiles"] = [bp.to_public_dict() for bp in self.broker_profiles]
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TradingSessionConfig":
        d = dict(d)
        bps = d.pop("broker_profiles", []) or []
        cfg = cls(
            total_allocated_capital=float(d["total_allocated_capital"]),
            strategy=d.get("strategy", "portfolio_kill_switch"),
            top_n_stocks=int(d.get("top_n_stocks", 5)),
            rank_filter=d.get("rank_filter"),
            n_seats=int(d.get("n_seats", 3)),
            tesla_min_grade=d.get("tesla_min_grade", "A++"),
            tesla_cooldown_minutes=int(d.get("tesla_cooldown_minutes", 30)),
            tesla_personality_window_days=int(
                d.get("tesla_personality_window_days", 5)),
            tesla_allow_reentry=bool(d.get("tesla_allow_reentry", False)),
            tesla_signal_db_path=(d.get("tesla_signal_db_path") or None),
            tesla_signal_staleness_sec=int(
                d.get("tesla_signal_staleness_sec", 90)),
            tesla_did_layer_enabled=bool(
                d.get("tesla_did_layer_enabled", False)),
            sizing_mode=d.get("sizing_mode", "equal"),
            max_pct_per_position=float(d.get("max_pct_per_position", 0.05)),
            manual_amounts=d.get("manual_amounts", {}) or {},
            order_product=d.get("order_product", "CNC"),
            order_type=d.get("order_type", "MARKET"),
            limit_offset_pct=float(d.get("limit_offset_pct", 0.001)),
            vwap_window_seconds=int(d.get("vwap_window_seconds", 60)),
            execution_mode=(d.get("execution_mode")
                            or os.environ.get("FALCON_AUTOTRADE_EXECUTION_MODE",
                                              "marketable_limit")),
            marketable_buffer_pct=float(d.get("marketable_buffer_pct", 0.003)),
            worked_participation_pct=float(
                d.get("worked_participation_pct", 0.10)),
            worked_interval_sec=int(d.get("worked_interval_sec", 20)),
            worked_deadline=(d.get("worked_deadline") or None),
            worked_min_child_qty=int(d.get("worked_min_child_qty", 1)),
            worked_max_children=int(d.get("worked_max_children", 500)),
            worked_vwap_enabled=bool(d.get("worked_vwap_enabled", False)),
            worked_max_participation_pct=float(
                d.get("worked_max_participation_pct", 0.25)),
            worked_vwap_profile_days=int(d.get("worked_vwap_profile_days", 20)),
            entry_quote_max_age_sec=float(
                d.get("entry_quote_max_age_sec", 10.0)),
            entry_stale_quote_policy=d.get("entry_stale_quote_policy", "market"),
            entry_circuit_locked_policy=d.get("entry_circuit_locked_policy",
                                              "drop"),
            mark_staleness_abstain_sec=int(
                d.get("mark_staleness_abstain_sec", 30)),
            instrument_type=d.get("instrument_type", "EQ"),
            expiry_preference=d.get("expiry_preference", "near"),
            direction=d.get("direction", "long"),
            kill_switch_pct=float(d.get("kill_switch_pct", 0.012)),
            kill_switch_direction=d.get("kill_switch_direction", "both"),
            kill_switch_enabled=bool(d.get("kill_switch_enabled", False)),
            kill_switch_target_pct=(
                float(d["kill_switch_target_pct"])
                if d.get("kill_switch_target_pct") is not None else None),
            kill_switch_stop_pct=(
                float(d["kill_switch_stop_pct"])
                if d.get("kill_switch_stop_pct") is not None else None),
            max_daily_loss_pct=(
                float(d["max_daily_loss_pct"])
                if d.get("max_daily_loss_pct") is not None else None),
            max_daily_loss_amount=(
                float(d["max_daily_loss_amount"])
                if d.get("max_daily_loss_amount") is not None else None),
            mis_protective_slm_enabled=bool(
                d.get("mis_protective_slm_enabled", True)),
            rms_allow_unknown_margin=bool(
                d.get("rms_allow_unknown_margin", False)),
            break_glass_global=bool(d.get("break_glass_global", False)),
            per_position_gtt_enabled=bool(d.get("per_position_gtt_enabled", True)),
            per_position_stop_pct=float(d.get("per_position_stop_pct", 0.08)),
            per_position_target_pct=float(d.get("per_position_target_pct", 0.20)),
            arm_pct=float(d.get("arm_pct", 0.05)),
            floor_pct=float(d.get("floor_pct", 0.01)),
            trail_giveback_pct=float(d.get("trail_giveback_pct", 0.0125)),
            stop_pct=float(d.get("stop_pct", 0.03)),
            trail_step_lock_enabled=bool(d.get("trail_step_lock_enabled", True)),
            # Absent key → the default ladder; an EXPLICIT [] (opt-out) is
            # preserved as-is. Values coerced to plain [float, float] rungs.
            trail_step_lock_ladder=(
                [[float(r[0]), float(r[1])] for r in d["trail_step_lock_ladder"]]
                if d.get("trail_step_lock_ladder") is not None
                else [list(r) for r in DEFAULT_STEP_LOCK_LADDER]),
            trail_large_peak_pct=float(d.get("trail_large_peak_pct", 0.20)),
            trail_large_giveback_rel=float(
                d.get("trail_large_giveback_rel", 0.175)),
            step_lock_scope=d.get("step_lock_scope", "basket"),
            per_stock_stop_enabled=bool(d.get("per_stock_stop_enabled", False)),
            per_stock_stop_pct=float(d.get("per_stock_stop_pct", 0.03)),
            square_off_time=d.get("square_off_time", "15:29:00"),
            square_off_enabled=bool(d.get("square_off_enabled", True)),
            max_hold_sessions=int(d.get("max_hold_sessions", 0)),
            mis_square_off_time=d.get("mis_square_off_time", "15:12:00"),
            redistribute_unused_capital=bool(
                d.get("redistribute_unused_capital", True)),
            broker_profiles=[BrokerProfile.from_public_dict(b) for b in bps],
            entry_time=d.get("entry_time", "09:15:00"),
            entry_window_seconds=int(d.get("entry_window_seconds", 60)),
            entry_date=(d.get("entry_date") or None),
            on_missed_window=d.get("on_missed_window", "expire"),
            entry_grace_seconds=int(d.get("entry_grace_seconds", 120)),
            universe_filter=d.get("universe_filter", "all500"),
            symbol_whitelist=d.get("symbol_whitelist", None),
            iceberg_enabled=bool(d.get("iceberg_enabled", False)),
            iceberg_slice_qty=(int(d["iceberg_slice_qty"])
                               if d.get("iceberg_slice_qty") is not None else None),
            iceberg_slice_value=(float(d["iceberg_slice_value"])
                                 if d.get("iceberg_slice_value") is not None
                                 else None),
            iceberg_freeze_qty_default=(
                int(d["iceberg_freeze_qty_default"])
                if d.get("iceberg_freeze_qty_default") is not None else None),
            iceberg_freeze_qty_map=(
                {str(k): int(v)
                 for k, v in d["iceberg_freeze_qty_map"].items()}
                if d.get("iceberg_freeze_qty_map") is not None else None),
            max_pct_per_name=(float(d["max_pct_per_name"])
                              if d.get("max_pct_per_name") is not None else None),
            fatfinger_max_notional_per_order=(
                float(d["fatfinger_max_notional_per_order"])
                if d.get("fatfinger_max_notional_per_order") is not None
                else None),
            fatfinger_max_qty_per_order=(
                int(d["fatfinger_max_qty_per_order"])
                if d.get("fatfinger_max_qty_per_order") is not None else None),
            fatfinger_policy=d.get("fatfinger_policy", "clamp"),
            risk_basis=d.get("risk_basis", "notional"),
        )
        return cfg

    @classmethod
    def from_json(cls, s: str) -> "TradingSessionConfig":
        return cls.from_dict(json.loads(s))


# ── Config preset persistence ───────────────────────────────────────────────────

def save_preset(name: str, cfg: TradingSessionConfig) -> int:
    cfg.validate()
    with falcon_conn() as con:
        cur = con.execute(
            """INSERT INTO autotrade_config_presets (name, created_at, config_json)
               VALUES (?,?,?)
               ON CONFLICT(name) DO UPDATE SET config_json = excluded.config_json""",
            (name, _now_ist_iso(), cfg.to_json()),
        )
        con.commit()
        return cur.lastrowid or 0


def list_presets() -> List[Dict[str, Any]]:
    with falcon_conn() as con:
        rows = con.execute(
            "SELECT id, name, created_at, config_json FROM autotrade_config_presets "
            "ORDER BY name ASC"
        ).fetchall()
    out = []
    for r in rows:
        out.append({
            "id": r["id"], "name": r["name"], "created_at": r["created_at"],
            "config": json.loads(r["config_json"]),
        })
    return out


def get_preset(name: str) -> Optional[TradingSessionConfig]:
    with falcon_conn() as con:
        row = con.execute(
            "SELECT config_json FROM autotrade_config_presets WHERE name=?", (name,)
        ).fetchone()
    if not row:
        return None
    return TradingSessionConfig.from_json(row["config_json"])
