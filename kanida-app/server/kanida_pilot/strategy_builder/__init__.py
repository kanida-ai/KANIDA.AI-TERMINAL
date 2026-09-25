"""KANIDA Strategy Builder (docs/strategy_builder_study/BUILD_PLAN_MERGED.md): research workspace, paper runs, Discover.

Reads db/derivatives.db READ-ONLY (the capture workers own it); its own store is var/strategy_builder.db.
It never sends an order: paper runs are simulated ledgers, and live execution is a later slice that will route
intents through engine/backend/autotrade (paper-default, cert-gated, operator-armed).

THE HOOK INTO THE PILOT - server/kanida_pilot/app.py, at the end of create_app(), immediately before `return app`:

    from .strategy_builder import mount as mount_strategy_builder
    mount_strategy_builder(app,settings)

A builder that fails to start is logged and skipped; the pilot runs without it.
"""
from __future__ import annotations
import logging,os
from pathlib import Path

log=logging.getLogger('strategy_builder')
DEFAULT_DB=str(Path(__file__).resolve().parents[3]/'var'/'strategy_builder.db')


def _beside_pilot_db(settings):
 """var/strategy_builder.db next to the pilot's own SQLite file, so a throwaway pilot gets a throwaway store."""
 url=str(getattr(settings,'database_url','') or '')
 if url.startswith('sqlite:///'):return str(Path(url[len('sqlite:///'):]).parent/'strategy_builder.db')
 return None


def mount(app,settings,path=None,derivatives_path=None):
 try:
  from .market import Market
  from .routes import build_router
  from .store import Store
  store=Store(path or os.getenv('PILOT_STRATEGY_BUILDER_DATABASE') or _beside_pilot_db(settings) or DEFAULT_DB)
  market=Market(derivatives_path or settings.derivatives_database)
  before=len(app.router.routes)
  app.include_router(build_router(app,market,store))
  # the pilot's catch-all GET /api/{path} and web routes are registered first; put ours ahead of them
  added=app.router.routes[before:]
  del app.router.routes[before:]
  app.router.routes[0:0]=added
  app.state.strategy_builder_store=store;app.state.strategy_builder_market=market
  return store
 except Exception:
  log.exception('The strategy builder could not start; the pilot runs without it.')
  return None
