"""
KANIDA.AI — Pathfinder (the autonomous research loop).

P0 scope = the CONTRACT + an HONEST MOCK. Nothing in this package touches market
data, a broker, or an LLM provider. The deterministic engine and the real
`pathfinder_llm` provider land in P1 (see docs/sessions/PATHFINDER.md).

Layout
  schemas.py     Pydantic models == the API contract (docs/openapi.yaml is GENERATED from these)
  store.py       PathfinderStore protocol + FixtureStore (P0) ; PostgresStore lands in P1
  router.py      FastAPI router — /api/pathfinder/*
  mock_app.py    standalone mock server (what the frontend track builds against)
  llm/gateway.py pathfinder_llm gateway CONTRACT (Protocol only — no provider in P0)
  fixtures/      honest sample data
"""
