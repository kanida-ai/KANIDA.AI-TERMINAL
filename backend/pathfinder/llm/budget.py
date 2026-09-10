"""
The hard daily cap, enforced against IST, checked BEFORE the call.

docs/STRATEGY_METHODOLOGY.md §3.6. The loop pauses when it runs out; it does not
downgrade to a cheaper model, reuse a cached answer, or write the sentence itself.
The deterministic observer keeps running and the queue keeps filling, so nothing is
lost — the narration just stops until tomorrow.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from ..engine.config import today_ist
from .gateway import Budget, BudgetExceeded, Usage

#: USD per million tokens, first-party API (docs/STRATEGY_METHODOLOGY.md §3.6).
RATES: dict[str, tuple[float, float]] = {
    "claude-sonnet-5": (2.0, 10.0),
    "claude-haiku-4-5": (1.0, 5.0),
    "claude-opus-5": (5.0, 25.0),
}
#: Cache reads bill at a fraction of fresh input; writes at a premium.
CACHE_READ_MULTIPLIER = 0.1
CACHE_WRITE_MULTIPLIER = 1.25


def cost_usd(model: str, *, input_tokens: int, output_tokens: int,
             cache_read: int = 0, cache_write: int = 0) -> float:
    """Cost from the provider's own token counts. Never from an estimate of them."""
    if model not in RATES:
        raise ValueError(f"no published rate for model {model!r}")
    inp, out = RATES[model]
    return (
        input_tokens * inp
        + output_tokens * out
        + cache_read * inp * CACHE_READ_MULTIPLIER
        + cache_write * inp * CACHE_WRITE_MULTIPLIER
    ) / 1_000_000.0


@dataclass
class BudgetLedger:
    """Spend for one IST day. `check()` is called before every call, `record()` after."""
    daily_cost_usd: float
    day: date = field(default_factory=today_ist)
    spent_usd: float = 0.0
    calls: int = 0
    by_model: dict[str, dict[str, float]] = field(default_factory=dict)

    def _roll(self) -> None:
        today = today_ist()
        if today != self.day:
            self.day, self.spent_usd, self.calls, self.by_model = today, 0.0, 0, {}

    @property
    def budget(self) -> Budget:
        self._roll()
        return Budget(daily_cost_usd=self.daily_cost_usd, spent_usd_today=self.spent_usd)

    def check(self, *, job: str) -> None:
        self._roll()
        if self.spent_usd >= self.daily_cost_usd:
            raise BudgetExceeded(
                f"daily Pathfinder LLM budget of ${self.daily_cost_usd:.2f} (IST {self.day}) is "
                f"spent (${self.spent_usd:.4f}); refusing the {job} call. The loop pauses; "
                "the deterministic observer keeps running."
            )

    def record(self, usage: Usage) -> None:
        self._roll()
        self.spent_usd += usage.cost_usd
        self.calls += 1
        row = self.by_model.setdefault(usage.model, {
            "calls": 0, "input_tokens": 0, "output_tokens": 0,
            "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0, "cost_usd": 0.0,
        })
        row["calls"] += 1
        row["input_tokens"] += usage.input_tokens
        row["output_tokens"] += usage.output_tokens
        row["cache_read_input_tokens"] += usage.cache_read_input_tokens
        row["cache_creation_input_tokens"] += usage.cache_creation_input_tokens
        row["cost_usd"] += usage.cost_usd

    @property
    def used_pct(self) -> float:
        return 0.0 if self.daily_cost_usd <= 0 else 100.0 * self.spent_usd / self.daily_cost_usd
