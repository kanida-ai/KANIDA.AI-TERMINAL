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
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from falcon.db import falcon_conn

IST = timezone(timedelta(hours=5, minutes=30))


def _now_ist_iso() -> str:
    return datetime.now(IST).isoformat()


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
    # Secrets — in-memory only, NEVER persisted. Sourced from env / kite_tokens.
    api_key: str = field(default="", repr=False)
    api_secret: str = field(default="", repr=False)
    access_token: str = field(default="", repr=False)

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
    strategy: str = "portfolio_kill_switch"

    # Stock selection
    top_n_stocks: int = 5
    rank_filter: Optional[List[int]] = None

    # Position sizing
    sizing_mode: str = "equal"             # equal | pct_cap | manual
    max_pct_per_position: float = 0.05
    manual_amounts: Dict[str, float] = field(default_factory=dict)

    # Order type
    order_product: str = "CNC"             # CNC | MIS | MTF | NRML
    order_type: str = "MARKET"             # MARKET | LIMIT | VWAP
    limit_offset_pct: float = 0.001
    vwap_window_seconds: int = 60

    # Instrument
    instrument_type: str = "EQ"            # EQ | FUT | CE | PE
    expiry_preference: str = "near"        # near | next | far

    # Kill switch  (DISABLED by default — fail safe)
    kill_switch_pct: float = 0.012
    kill_switch_direction: str = "both"    # profit | loss | both
    kill_switch_enabled: bool = False

    # Per-position GTT-OCO broker backup (FEATURE 1). The portfolio kill switch
    # is the PRIMARY exit (software, ours); the per-position GTT is the broker-
    # held BACKUP floor. Widths default WIDER than kill_switch_pct so the
    # portfolio target usually fires first. LIVE-only: in paper mode no real GTT
    # is placed (the intended levels are still recorded for the UI).
    per_position_gtt_enabled: bool = True
    per_position_stop_pct: float = 0.03    # stop  = entry * (1 - this)
    per_position_target_pct: float = 0.06  # target = entry * (1 + this)

    # ── INTRADAY BASKET trailing engine (strategy=="intraday_basket" only) ────
    # All percentages are FRACTIONS (0.01 = 1%), validated in (0, 0.5], exactly
    # like kill_switch_pct. They drive monitoring/trail_engine.py over the
    # NOTIONAL / invested-basis gross return G (compute_gross_return_invested):
    #   arm_pct            : G >= +arm_pct → the trail ARMS (locks a floor).
    #   floor_pct          : the minimum locked floor once armed (default = arm).
    #   trail_giveback_pct : giveback from the peak G once armed.
    #   stop_pct           : downside hard stop, applied as -stop_pct (pre-arm).
    #   square_off_time    : IST clock time to flatten the basket (never overnight).
    # Inert when strategy != "intraday_basket".
    arm_pct: float = 0.01
    floor_pct: float = 0.01
    trail_giveback_pct: float = 0.0075
    stop_pct: float = 0.015
    square_off_time: str = "15:29:00"

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
        if self.instrument_type not in ("EQ", "FUT", "CE", "PE", "MTF"):
            raise ValueError(f"invalid instrument_type: {self.instrument_type}")
        if self.kill_switch_direction not in ("profit", "loss", "both"):
            raise ValueError(f"invalid kill_switch_direction: {self.kill_switch_direction}")
        if self.top_n_stocks <= 0:
            raise ValueError("top_n_stocks must be > 0")
        if self.strategy not in ("portfolio_kill_switch", "intraday_basket"):
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
            if self.top_n_stocks < 3 or self.top_n_stocks > 10:
                raise ValueError(
                    "intraday_basket basket_size (top_n_stocks) must be 3..10, "
                    f"got {self.top_n_stocks}")
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
            sizing_mode=d.get("sizing_mode", "equal"),
            max_pct_per_position=float(d.get("max_pct_per_position", 0.05)),
            manual_amounts=d.get("manual_amounts", {}) or {},
            order_product=d.get("order_product", "CNC"),
            order_type=d.get("order_type", "MARKET"),
            limit_offset_pct=float(d.get("limit_offset_pct", 0.001)),
            vwap_window_seconds=int(d.get("vwap_window_seconds", 60)),
            instrument_type=d.get("instrument_type", "EQ"),
            expiry_preference=d.get("expiry_preference", "near"),
            kill_switch_pct=float(d.get("kill_switch_pct", 0.012)),
            kill_switch_direction=d.get("kill_switch_direction", "both"),
            kill_switch_enabled=bool(d.get("kill_switch_enabled", False)),
            per_position_gtt_enabled=bool(d.get("per_position_gtt_enabled", True)),
            per_position_stop_pct=float(d.get("per_position_stop_pct", 0.03)),
            per_position_target_pct=float(d.get("per_position_target_pct", 0.06)),
            arm_pct=float(d.get("arm_pct", 0.01)),
            floor_pct=float(d.get("floor_pct", 0.01)),
            trail_giveback_pct=float(d.get("trail_giveback_pct", 0.0075)),
            stop_pct=float(d.get("stop_pct", 0.015)),
            square_off_time=d.get("square_off_time", "15:29:00"),
            broker_profiles=[BrokerProfile.from_public_dict(b) for b in bps],
            entry_time=d.get("entry_time", "09:15:00"),
            entry_window_seconds=int(d.get("entry_window_seconds", 60)),
            entry_date=(d.get("entry_date") or None),
            on_missed_window=d.get("on_missed_window", "expire"),
            entry_grace_seconds=int(d.get("entry_grace_seconds", 120)),
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
