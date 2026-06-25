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

    # Broker routing
    broker_profiles: List[BrokerProfile] = field(default_factory=list)

    # Entry timing
    entry_time: str = "09:15:00"
    entry_window_seconds: int = 60

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
        if self.sizing_mode == "manual":
            total = sum(self.manual_amounts.values())
            if total > self.total_allocated_capital + 1e-6:
                raise ValueError(
                    f"manual_amounts total {total} exceeds "
                    f"total_allocated_capital {self.total_allocated_capital}"
                )

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
            broker_profiles=[BrokerProfile.from_public_dict(b) for b in bps],
            entry_time=d.get("entry_time", "09:15:00"),
            entry_window_seconds=int(d.get("entry_window_seconds", 60)),
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
