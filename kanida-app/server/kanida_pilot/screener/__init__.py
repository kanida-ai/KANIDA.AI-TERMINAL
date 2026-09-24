"""KANIDA Options Screener (docs/OPTIONS_SCREENER_SPEC.md, docs/OPTIONS_SCREENER_LAYOUT.md).

"Tell KANIDA the market behaviour you want to find": metric → side → state word → time window, read off the
15-minute F&O readings with the Derivative tab's own rules. Read-only on db/derivatives.db; its own store is
var/screener.db.

THE ONE HOOK INTO THE PILOT (applied by the owner, after market close):

    # server/kanida_pilot/app.py, at the end of create_app(), immediately before `return app`:
    from .screener import mount as mount_screener
    mount_screener(app,settings)

`mount` puts the screener's routes AHEAD of the app's `/api/{path}` and `/{path}` catch-alls, so the hook is a
single line wherever it is placed, and a screener that fails to start is logged and skipped rather than taking
the pilot down with it.
"""
from __future__ import annotations
import logging

log=logging.getLogger('screener')


def mount(app,settings,screener_path=None):
 """Attach the screener to a created pilot app. Returns the Screener, or None if it could not start."""
 try:
  from .routes import build_router
  from .service import Screener
  screener=Screener(settings.derivatives_database,screener_path)
  router=build_router(app,screener)
  before=len(app.router.routes)
  app.include_router(router)
  added=app.router.routes[before:]
  del app.router.routes[before:]
  app.router.routes[0:0]=added
  app.state.screener=screener
  return screener
 except Exception:
  log.exception('The options screener could not start; the pilot runs without it.')
  return None
