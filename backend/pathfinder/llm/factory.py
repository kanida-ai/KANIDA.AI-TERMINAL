"""
Which provider the loop talks to. One env var, no code change.

    KANIDA_PATHFINDER_LLM = auto      (default) live if a key is present, else recorded,
                                      else none
                          = live      require the Anthropic provider; fail if unavailable
                          = recorded  cassette replay (CI, and any machine with no key)
                          = none      no model at all; every beat is engine-authored

`auto` never silently upgrades a run's credibility: the chosen provider's name is
written to every `llm_calls` row and printed in the run report, so "which of these
sentences did a model actually write, just now?" is answerable from the database.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from ..engine.governance import Constitution
from .budget import BudgetLedger
from .gateway import GatewayError, PathfinderLLM
from .providers.recorded import NullProvider, RecordedProvider

REPO_ROOT = Path(__file__).resolve().parents[3]
METHODOLOGY = REPO_ROOT / "docs" / "STRATEGY_METHODOLOGY.md"


def _methodology_text() -> str:
    if METHODOLOGY.exists():
        return METHODOLOGY.read_text(encoding="utf-8")
    return "Pathfinder methodology unavailable; the LLM must not compute."


def build_llm(
    constitution: Constitution, *, mode: Optional[str] = None,
    ledger: Optional[BudgetLedger] = None,
) -> tuple[PathfinderLLM, str, BudgetLedger]:
    """Returns `(provider, provider_name, ledger)`."""
    mode = (mode or os.environ.get("KANIDA_PATHFINDER_LLM", "auto")).strip().lower()
    daily = float(constitution.get("llm.daily_budget_usd", 5.0))
    ledger = ledger or BudgetLedger(daily_cost_usd=daily)

    def live() -> tuple[PathfinderLLM, str, BudgetLedger]:
        from .providers.anthropic_provider import AnthropicProvider
        p = AnthropicProvider(
            ledger=ledger,
            constitution_text=str(constitution.document),
            methodology_text=_methodology_text(),
        )
        return p, p.provider_name, ledger

    if mode == "live":
        return live()
    if mode == "recorded":
        p = RecordedProvider(ledger=ledger)
        return p, p.provider_name, ledger
    if mode == "none":
        return NullProvider(), "none", ledger
    if mode != "auto":
        raise GatewayError(f"unknown KANIDA_PATHFINDER_LLM mode {mode!r}")

    if os.environ.get("ANTHROPIC_API_KEY"):
        return live()
    try:
        p = RecordedProvider(ledger=ledger)
        if p._entries:                                  # noqa: SLF001 — same package
            return p, p.provider_name, ledger
    except GatewayError:
        pass
    return NullProvider("no ANTHROPIC_API_KEY and no cassettes"), "none", ledger
