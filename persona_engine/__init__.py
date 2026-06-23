"""
KANIDA.AI — Persona Expansion engine.

Two additive self-learning persona engines layered on the existing Falcon
infrastructure (NOT a replacement):

  • F&O Trader        — next-day Long Futures Top 10 + Short Futures Top 10,
                        measured by overlap with next day's actual top/bottom movers.
  • Long-Term Investor — 4–8 week Top 10 long-only picks, measured by overlap with
                        the actual Nifty-500 forward Top 10.

Architecture (spec §1): stock agents → sector agents → persona agents, with a
closed self-learning loop (PREDICT → MEASURE → ANALYSE → LEARN → APPROVE → DEPLOY
→ REPORT).

All code here is ADDITIVE. It never modifies the existing Falcon Top 10 engine,
tier classifier, tier self-learning loop, auto-trade path, or the portal frontend.
"""

__version__ = "0.1.0"
