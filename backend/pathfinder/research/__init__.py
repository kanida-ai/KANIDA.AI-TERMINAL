"""
Pathfinder RESEARCH ENGINE (Session S1) — the deterministic core behind the feed.

    "Pathfinder computes market evidence first and uses GenAI only to decide what is
     worth investigating and to explain verified results."   — docs/sessions/PATHFINDER.md

Layout
  config.py    every knob (DB path, cost hurdle, usefulness threshold, template params)
  data.py      point-in-time market data — a frame SEALED at `as_of`; forward returns are
               computed only inside the seal, so an unresolved horizon is NaN, never a number
  regime.py    the market-regime brain (port of Kanida_Falcon/scripts/regime.py)
  facts.py     the only way a number is minted: a `Fact` with full provenance
  library.py   the QUESTION LIBRARY — `question -> parameters -> computation -> evidence card`.
               The six seeded templates port the math of pathfinder_theme.py / pathfinder_demo.py.
               Templates are the ONLY source of computations; nothing else may run a test.
  grading.py   per-finding-type Right / Wrong / Inconclusive rules, FROZEN at publication
  ranking.py   usefulness = evidence x novelty x relevance x magnitude; the publication threshold
  narrate.py   digit-free narratives: engine-templated, or LLM via the gateway (same contract)
  store.py     append-only SQLite store: editions, findings, grades, scoreboard snapshots
  scan.py      the after-close scan: seal -> ask -> compute -> rank -> publish -> grade due

No number in this package originates in a model. The LLM (through `pathfinder.llm`) only
selects (a closed label), orders nothing, and narrates with `{{fact:...}}` tokens.
"""
