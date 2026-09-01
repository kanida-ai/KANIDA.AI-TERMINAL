"""
Options Agent (#2) — options-strategy research agent for the KANIDA agent platform.

Deliberately import-light: heavy modules (data, pricing, strategies) are imported by the
callers that need them, so a missing dependency can never crash app boot.

HARD BOUNDARY (docs/AGENTS_PLATFORM.md):
  - Agents EMIT intents; they NEVER touch a broker, git, shell, or deploy.
  - Strictly point-in-time: a decision at t uses only chain/underlying data available at t.
  - ETV (expectancy) decides, never POP/win-rate alone.
"""
