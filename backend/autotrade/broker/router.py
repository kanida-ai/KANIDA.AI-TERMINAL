"""BrokerRouter — routes Falcon picks to broker profiles + builds clients.

route_picks(falcon_picks, profiles) → {profile_id: [Pick, ...]} per spec:
  * profile.symbols set      → use that exact list (filtered to picks we know)
  * profile.rank_range set   → filter picks by rank inclusive
  * both None                → all picks up to top_n_stocks
A symbol CAN appear in multiple profiles. Capital accounting is per-profile.

build_client(profile, dry_run) instantiates the right BrokerClient subclass.
Unknown brokers raise; unimplemented brokers return their NotImplementedError
stub (so a session can be created/validated but not run live).
"""
from __future__ import annotations

from typing import Dict, List, Optional

from .base import BrokerClient, Pick


class BrokerRouter:
    def __init__(self, top_n_stocks: int = 5):
        self.top_n_stocks = top_n_stocks

    def route_picks(self, falcon_picks: List[Pick],
                    profiles: List) -> Dict[str, List[Pick]]:
        by_symbol = {p.symbol: p for p in falcon_picks}
        out: Dict[str, List[Pick]] = {}
        for prof in profiles:
            if not getattr(prof, "enabled", True):
                continue
            if prof.symbols:
                picks = [by_symbol[s] for s in prof.symbols if s in by_symbol]
                # Symbols set on the profile but absent from Falcon output are
                # surfaced as synthetic rank-0 picks so manual lists still trade.
                missing = [s for s in prof.symbols if s not in by_symbol]
                picks += [Pick(symbol=s, rank=0) for s in missing]
            elif prof.rank_range:
                lo, hi = prof.rank_range
                picks = [p for p in falcon_picks if lo <= p.rank <= hi]
            else:
                picks = sorted(falcon_picks, key=lambda p: p.rank)[: self.top_n_stocks]
            out[prof.profile_id] = picks
        return out


def build_client(profile, dry_run: bool = True) -> BrokerClient:
    name = profile.broker_name.lower()
    if name == "zerodha":
        from .zerodha import ZerodhaBroker
        return ZerodhaBroker(profile, dry_run=dry_run)
    if name == "fyers":
        from .fyers import FyersBroker
        return FyersBroker(profile, dry_run=dry_run)
    if name == "upstox":
        from .upstox import UpstoxBroker
        return UpstoxBroker(profile, dry_run=dry_run)
    if name == "angel":
        from .angel import AngelBroker
        return AngelBroker(profile, dry_run=dry_run)
    if name == "dhan":
        from .dhan import DhanBroker
        return DhanBroker(profile, dry_run=dry_run)
    raise ValueError(f"unknown broker: {profile.broker_name}")
