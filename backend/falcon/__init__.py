"""
Falcon Engine — production runtime layer for KANIDA.AI Terminal.

This package holds the production code that serves Falcon V7.1 inside the
existing FastAPI backend on Railway. Heavy mining/validation lives offline in
`universe_engine/`; here we only:

  - Load promoted patterns from kanida_universe.db
  - Apply them to today's bars to emit live signals
  - Run daily cron jobs to keep data fresh
  - Expose REST APIs for the Next.js frontend
  - Send notifications (in-app + email)

Falcon coexists with the legacy engine. All endpoints live under /api/falcon/*.
"""
__version__ = "7.1.0"
