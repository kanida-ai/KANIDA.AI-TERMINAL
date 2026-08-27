"""
Chart Agent (#1) — a MULTI-PATTERN chart-pattern agent, daily timeframe.

The agent hosts a pattern LIBRARY (backend/agents/chart/patterns/) and runs the shared v3 machine
over all of them:  scan() runs every registered detector point-in-time across a default universe;
decide() feeds each occurrence through the ported pattern-forward evidence + the §9 gate stack and,
on TRADE, emits an Intent (mode='paper') routed through backend/autotrade/ — agents NEVER touch a
broker.

BUILT today:  horizontal_trendline detector (real ported logic) + pattern-forward evidence + gates.
SPEC:         triangle/channel detectors (skeletons), strategy-replay ETV (§8.2), nested-population
              coherence (§7.2/§7.3). All honestly labelled; nothing fabricated.

Guarded: pattern loading and every per-symbol/per-detector call is wrapped so one bad detector can
never crash the agent or app boot.
"""
from __future__ import annotations
import logging
from typing import Optional

from ..base import BaseAgent, Manifest, Intent
from .. import registry
from . import data
from .patterns import registry as patterns
from . import evidence as ev

log = logging.getLogger("agents.chart")

# Load the pattern library once (guarded — a broken detector is skipped, never fatal).
patterns.load_builtin()

MANIFEST = Manifest(
    agent_id="chart-v1",
    name="Chart Agent",
    agent_class="observe",
    universe="nifty500",
    timeframe="daily",
    schedule="eod",
    tools=["market_data", "evidence_store", "historical_probability"],
    outputs=["observation", "direction", "probability", "evidence", "intent"],
    tracking=[1, 3, 5, 10],
    patterns=[p.manifest() for p in patterns.all_patterns()],
)


def _ctx_get(ctx, key, default=None):
    if ctx is None:
        return default
    if isinstance(ctx, dict):
        return ctx.get(key, default)
    return getattr(ctx, key, default)


def _as_of_idx(df, as_of) -> int:
    """Resolve the decision-bar index for a date/int/None (last bar). Strictly the bar itself."""
    if as_of is None:
        return len(df) - 1
    if isinstance(as_of, int):
        return as_of
    import pandas as pd
    ts = pd.Timestamp(as_of)
    dates = df.index
    if ts in dates:
        return int(dates.get_loc(ts))
    # nearest bar on/before as_of (point-in-time: never a future bar)
    prior = dates[dates <= ts]
    return int(len(prior) - 1) if len(prior) else -1


class ChartAgent(BaseAgent):
    # ------------------------------------------------------------------ SCAN
    def scan(self, ctx=None) -> list:
        """Run every registered detector point-in-time across the default universe. Returns a list
        of occurrence dicts. Fully guarded per symbol and per detector."""
        as_of = _ctx_get(ctx, "as_of")
        out: list = []
        if not data.db_available():
            log.warning("chart scan: data source unavailable (%s) — returning [].", data.db_path())
            return out
        # Default = the FULL active universe (not the 16-symbol starter). DEFAULT_UNIVERSE is only a
        # fallback if enumerating the source fails. Callers can still pin an explicit universe/symbols.
        universe = _ctx_get(ctx, "universe") or _ctx_get(ctx, "symbols")
        if not universe:
            try:
                universe = list(data.all_symbols()) or data.DEFAULT_UNIVERSE
            except Exception as e:  # noqa: BLE001 — never let enumeration crash the scan
                log.warning("chart scan: universe enumeration failed (%s) — using starter set.", e)
                universe = data.DEFAULT_UNIVERSE
        for sym in universe:
            try:
                df = data.load_daily(sym)
            except Exception as e:  # noqa: BLE001
                log.debug("chart scan: skip %s (%s)", sym, e)
                continue
            df.attrs["symbol"] = sym
            k = _as_of_idx(df, as_of)
            if k < 0 or k >= len(df):
                continue
            for det in patterns.all_patterns():
                try:
                    for occ in det.detect(df, as_of_idx=k):
                        out.append(occ.to_dict())
                except Exception as e:  # noqa: BLE001 — a bad detector must not sink the scan
                    log.warning("chart scan: detector %s failed on %s (non-fatal): %s",
                                getattr(det, "pattern_id", "?"), sym, e)
        return out

    # ------------------------------------------------------------------ DECIDE
    def decide(self, occurrence: dict, ctx=None) -> dict:
        """Evidence + §9 gates for one occurrence. On TRADE, emit a paper Intent.
        Returns {decision, reason, evidence, gates, intent?}. Never fabricates evidence."""
        sym = occurrence.get("stock") or ""
        pattern_id = occurrence.get("pattern")
        signal_idx = int(occurrence.get("signal_idx", -1))
        det = patterns.get(pattern_id)

        base = {"decision": "WATCH", "reason": "", "evidence": None,
                "occurrence": occurrence}

        if det is None or not hasattr(det, "historical_events"):
            base["reason"] = (f"pattern '{pattern_id}' has no evidence base yet "
                              f"(detector is SPEC/skeleton) — cannot decide, honest WATCH.")
            return base
        try:
            df = data.load_daily(sym)
        except Exception as e:  # noqa: BLE001
            base["reason"] = f"data unavailable for {sym} ({e}) — WATCH."
            return base
        df.attrs["symbol"] = sym

        # Point-in-time: resolved precedents whose T+10 printed by the decision bar (§3/§7.1),
        # and evidence computed only on data <= the decision bar.
        as_of = signal_idx if signal_idx >= 0 else len(df) - 1
        try:
            events = det.historical_events(df, as_of_idx=as_of)
            df_asof = df.iloc[:as_of + 1]
            evidence = ev.pattern_evidence(df_asof, events, max_h=10)
            decision = ev.decide(df_asof, events, evidence, as_of_idx=as_of)
        except Exception as e:  # noqa: BLE001
            base["reason"] = f"evidence/gate error for {sym} ({e}) — WATCH."
            return base

        result = {
            "decision": decision["decision"],
            "reason": decision["reason"],
            "evidence": evidence,                 # pattern-forward family (§8.1) — kept for research
            "strategy": decision.get("strategy"),  # strategy-replay family (§8.2) — what the gates read
            "policy": decision.get("policy"),
            "gates": decision.get("gates", []),
            "basis": decision.get("basis"),
            "spec_note": decision.get("spec_note"),
            "occurrence": occurrence,
        }

        if decision["decision"] == "TRADE":
            sig_ts = occurrence.get("context", {}).get("as_of_date") or ""
            s = decision.get("strategy") or {}
            pol = decision.get("policy") or {}
            result["intent"] = Intent(
                agent_id=MANIFEST.agent_id,
                stock=sym,
                direction=occurrence.get("direction", "long"),
                signal_ts=str(sig_ts),
                thesis=(f"I'm taking {sym} long on a {pattern_id.replace('_',' ')} "
                        f"{occurrence.get('stage','').lower()} at ~{occurrence.get('level')}. "
                        f"Under my governed exit policy {pol.get('version')} "
                        f"(trail {pol.get('trail_pct')}, max_hold {pol.get('max_hold')}, "
                        f"invalidate on close<level) this setup's replayed Strategy-ETV is "
                        f"{s.get('etv')}% (win {s.get('win')}%, payoff {s.get('payoff')}, "
                        f"n={s.get('n')}). {decision['reason']}"),
                evidence_ref=f"{sym}:{pattern_id}:{as_of}",
                mode="paper",   # agents can NEVER set live — that's an operator-armed step downstream
            )
        return result


registry.register(ChartAgent(MANIFEST))
