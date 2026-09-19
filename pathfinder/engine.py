"""Observe -> propose -> screen -> paper -> learn -> reprioritize."""
from dataclasses import asdict
from datetime import date
from statistics import mean

from .market import Market, live_clock_check
from .models import Policy, digest, experiment_id, validate_proposal
from .planner import priority
from .research import prospective_evidence, screen, selections


class Pathfinder:
    def __init__(self, store, planner, policy=None):
        self.store, self.planner = store, planner
        self.policy = policy or Policy()

    def cycle(self, bars, source, mode="replay"):
        if mode not in ("replay", "live_paper"):
            raise ValueError("Only replay and live_paper modes exist")
        market = Market(bars)
        day = market.days[-1]
        if len(bars) > self.policy.max_bars:
            raise ValueError("Data budget exceeded")
        if mode == "live_paper":
            live_clock_check(day, day, self.policy.max_data_age_days)
        if len(market.days) < self.policy.min_history_sessions:
            raise ValueError("Not enough history for screening")
        state = self.store.read()
        if state:
            if state["policy"] != asdict(self.policy) or state["source"] != source or state["mode"] != mode or state["planner"] != self.planner.mode:
                raise ValueError("Run identity changed; use a new memory database for a new policy, source, planner or mode")
            if day < state["as_of"]:
                raise ValueError("Cannot rewind research memory")
            if state["halted"] == "data_revision":
                return state
            changed = [d for d, h in state["fingerprints"].items() if market.fingerprints.get(d) != h]
            if changed:
                with self.store.transaction():
                    state = self.store.read()
                    state["halted"] = "data_revision"
                    state["resource_requests"].append({"resource": "consistent_data_version", "status": "operator_action",
                        "reason": "Previously observed bars changed or disappeared; replay into a new memory version before resuming."})
                    for e in state["experiments"].values():
                        e["status"] = "invalidated"
                    self.store.event(day, "self_correction", {"changed_dates": changed[:20], "action": "Halt; invalidate conclusions and rebuild from corrected history."})
                    self.store.save(state)
                return state
            if day == state["as_of"]:
                return state  # No new session: no model call, duplicate experiment, trade or event.
        version = digest(state) if state else None
        context = market.context()
        memory = list(state["experiments"].values()) if state else []
        proposals, planner_error = [], None
        if not state or not state["halted"]:
            try:
                if self.planner.mode == "genai":
                    self.store.reserve_model_call(self.policy.max_model_calls_per_day)
                proposals = self.planner.propose(context, memory, self.policy)
                if len(proposals) > self.policy.max_proposals_per_cycle:
                    raise ValueError("Proposal budget exceeded")
                proposals = [validate_proposal(p) for p in proposals]
            except Exception as error:
                # Never serialize provider bodies, API keys or request headers.
                planner_error = {"resource": "research_planner", "status": "unavailable",
                                 "reason": f"Planning failed ({type(error).__name__}); existing paper positions still tracked."}
        with self.store.transaction():
            current = self.store.read()
            if (digest(current) if current else None) != version:
                raise ValueError("Another worker advanced memory; retry from its checkpoint")
            if state is None:
                state = {"version": 1, "as_of": None, "source": source, "mode": mode,
                    "planner": self.planner.mode, "policy": asdict(self.policy),
                    "cash": self.policy.initial_capital, "equity": self.policy.initial_capital,
                    "peak_equity": self.policy.initial_capital, "halted": None,
                    "positions": [], "trades": [], "pending": [], "experiments": {},
                    "resource_requests": [], "fingerprints": {}, "context": context, "cycle_count": 0}
            state["resource_requests"] = []
            if planner_error:
                state["resource_requests"].append(planner_error)
                self.store.event(day, "resource_blocked", planner_error)
            self._advance_portfolio(state, market)
            self._learn(state, day)
            state["context"] = context
            self.store.event(day, "observation", context)
            existing = state["experiments"]
            for p in proposals:
                eid = experiment_id(p)
                if eid in existing or len(existing) >= self.policy.max_registry:
                    continue
                if p["parent_id"] and p["parent_id"] not in existing:
                    self.store.event(day, "proposal_rejected", {"reason": "Unknown parent", "question": p["question"]})
                    continue
                unavailable = [r for r in p["resources"] if r != "daily_ohlcv"]
                score, why = priority(p, context, list(existing.values()))
                e = {"id": eid, "trial_number": len(existing)+1, "created": day, "proposal": p,
                     "status": "blocked" if unavailable else "queued", "priority": score, "why_today": why,
                     "screen": None, "learning": None, "paper_started": None}
                existing[eid] = e
                self.store.event(day, "hypothesis", e)
                for resource in unavailable:
                    request = {"resource": resource, "status": "unavailable", "experiment_id": eid,
                               "reason": "Not in registered read-only capabilities; requires an operator-provided adapter."}
                    state["resource_requests"].append(request)
                    self.store.event(day, "resource_blocked", request)
            queued = [e for e in existing.values() if e["status"] == "queued"]
            for e in queued:
                e["priority"], e["why_today"] = priority(e["proposal"], context, list(existing.values()))
            queued.sort(key=lambda e: (-e["priority"], e["created"], e["id"]))
            # Reserve one slot for the oldest unexplored question to avoid permanent starvation.
            if len(queued) > self.policy.max_experiments_per_cycle:
                oldest = min(queued, key=lambda e: (e["created"], e["trial_number"]))
                queued.remove(oldest)
                queued.insert(self.policy.max_experiments_per_cycle-1, oldest)
            if not state["halted"]:
                for e in queued[:self.policy.max_experiments_per_cycle]:
                    result = screen(e["proposal"], market, self.policy)
                    e["screen"], e["status"] = result, "paper" if result["paper_eligible"] else result["status"]
                    if result["paper_eligible"]:
                        e["paper_started"] = day
                    self.store.event(day, "historical_screen", {"experiment_id": e["id"], **result})
                # Inconclusive tests wait for 20 new sessions, instead of mining the same sample every day.
                for e in existing.values():
                    if e["status"] == "inconclusive" and sum(d > e["screen"]["data_through"] for d in market.days) >= 20:
                        e["status"] = "queued"
                self._schedule(state, market)
            state["as_of"], state["fingerprints"] = day, market.fingerprints
            state["cycle_count"] += 1
            self.store.save(state)
        return state

    def _advance_portfolio(self, state, market):
        if state["as_of"] is None:
            return
        cost = self.policy.cost_bps_per_side/10000
        for day in (d for d in market.days if d > state["as_of"]):
            bars = market.by_day[day]
            prior_pending, state["pending"] = state["pending"], []
            for order in prior_pending:
                # An order is only valid for the immediately next observed market session.
                bar = bars.get(order["symbol"])
                e = state["experiments"][order["experiment_id"]]
                held_symbols = {p["symbol"] for p in state["positions"]}
                reason = None
                if state["halted"] or e["status"] not in ("paper", "supported_provisionally"):
                    reason = "Research paused or experiment retired"
                elif not bar or bar["volume"] <= 0:
                    reason = "No executable next-session bar; order expired"
                elif order["symbol"] in held_symbols or len(state["positions"]) >= self.policy.max_positions:
                    reason = "Position or overlap limit"
                if reason:
                    self.store.event(day, "paper_order_skipped", {"experiment_id": e["id"], "symbol": order["symbol"], "reason": reason})
                    continue
                # Size only from prior known volume and equity, never the completed entry-day volume.
                notional = min(order["budget"], state["cash"],
                               max(0, state["equity"]*self.policy.max_exposure-sum(p["mark"]*p["qty"] for p in state["positions"])))
                unit_cost = bar["open"]*(1+cost)
                qty = min(int(notional/unit_cost), int(order["known_volume"]*self.policy.max_volume_fraction))
                if qty <= 0:
                    self.store.event(day, "paper_order_skipped", {"symbol": order["symbol"], "reason": "Capital or liquidity budget"})
                    continue
                basis = qty*unit_cost
                p = {"experiment_id": e["id"], "symbol": order["symbol"], "entry_date": day,
                     "signal_date": order["signal_date"], "entry_price": bar["open"], "qty": qty,
                     "cost_basis": basis, "remaining": e["proposal"]["hold_sessions"], "mark": bar["open"], "mark_date": day}
                state["cash"] -= basis
                state["positions"].append(p)
                self.store.event(day, "paper_entry", p)
            still_open = []
            for p in state["positions"]:
                p["remaining"] -= 1
                bar = bars.get(p["symbol"])
                if bar and bar["volume"] > 0:
                    p["mark"], p["mark_date"] = bar["close"], day
                    if p["remaining"] <= 0:
                        proceeds = p["qty"]*bar["close"]*(1-cost)
                        trade = dict(p, exit_date=day, exit_price=bar["close"], pnl=proceeds-p["cost_basis"])
                        state["cash"] += proceeds
                        state["trades"].append(trade)
                        self.store.event(day, "paper_exit", trade)
                        continue
                else:
                    request = {"resource": "executable_price", "symbol": p["symbol"], "status": "missing",
                               "reason": "Position remains open at its last known mark; no fabricated liquidation."}
                    state["resource_requests"].append(request)
                    self.store.event(day, "resource_blocked", request)
                still_open.append(p)
            state["positions"] = still_open
            state["equity"] = state["cash"] + sum(p["qty"]*p["mark"] for p in still_open)
            state["peak_equity"] = max(state["peak_equity"], state["equity"])
            if 1-state["equity"]/state["peak_equity"] >= self.policy.max_drawdown and not state["halted"]:
                state["halted"] = "drawdown_limit"
                self.store.event(day, "risk_halt", {"reason": "Virtual portfolio drawdown reached policy limit; new allocations stopped."})

    def _learn(self, state, day):
        for e in state["experiments"].values():
            if e["status"] not in ("paper", "supported_provisionally"):
                continue
            trades = [t for t in state["trades"] if t["experiment_id"] == e["id"]]
            evidence = prospective_evidence(trades, self.policy, e["trial_number"])
            old = e["learning"]
            n = evidence["entry_days"]
            if old and n == old["entry_days"]:
                continue
            # Only evaluate promotion/retirement at predeclared sample checkpoints.
            if n < self.policy.min_paper_days or n % self.policy.min_paper_days:
                evidence["status"] = "collecting"
            if evidence["status"] == "retired":
                e["status"] = "retired"
                applied = "Stop new allocations; prioritize a separately registered follow-up question."
            elif evidence["status"] == "supported_provisionally":
                e["status"] = "supported_provisionally"
                applied = "Continue bounded paper allocation; no real-money or automatic risk increase."
            else:
                applied = "Collect more independent paper entry days without changing the frozen rules."
            evidence["applied_learning"] = applied
            evidence["mode"] = state["mode"]
            e["learning"] = evidence
            self.store.event(day, "learning", {"experiment_id": e["id"], **evidence})

    def _schedule(self, state, market):
        day = market.days[-1]
        state["pending"] = []
        # A stale mark cannot justify another capital allocation.
        if any(p["mark_date"] != day for p in state["positions"]):
            return
        occupied = {p["experiment_id"] for p in state["positions"]}
        budget = state["equity"]*self.policy.position_fraction
        for e in sorted(state["experiments"].values(), key=lambda e: (-e["priority"], e["id"])):
            if e["status"] not in ("paper", "supported_provisionally") or e["id"] in occupied:
                continue
            for symbol in selections(e["proposal"], market, day):
                state["pending"].append({"experiment_id": e["id"], "symbol": symbol, "signal_date": day,
                                         "known_volume": market.by_day[day][symbol]["volume"], "budget": budget})
        if state["pending"]:
            self.store.event(day, "paper_orders_planned", {"orders": state["pending"], "execution": "Next observed session open; risk checks at fill."})


def storyline(store):
    state = store.read()
    if not state:
        return {"status": "not_started"}
    experiments = list(state["experiments"].values())
    events = store.events(200)
    today = [e for e in events if e["date"] == state["as_of"]]
    return {"as_of": state["as_of"], "mode": state["mode"], "planner": state["planner"],
        "mission": state["policy"]["mission"], "market_context": state["context"], "halted": state["halted"],
        "testing_now": [e for e in experiments if e["status"] in ("paper", "supported_provisionally")],
        "discovered_today": [e for e in today if e["kind"] == "historical_screen"],
        "virtual_portfolio": {k: state[k] for k in ("cash", "equity", "positions", "pending")},
        "closed_trade_count": len(state["trades"]),
        "learned_today": [e for e in today if e["kind"] in ("learning", "self_correction")],
        "testing_next": sorted([e for e in experiments if e["status"] == "queued"], key=lambda e: -e["priority"]),
        "resource_requests": state["resource_requests"],
        "experiment_counts": {s: sum(e["status"] == s for e in experiments) for s in sorted({e["status"] for e in experiments})},
        "recent_events": events[:30]}
