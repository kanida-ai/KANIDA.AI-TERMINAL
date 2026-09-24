"""KANIDA Workspace (docs/WORKSPACE_SPEC.md, docs/WORKSPACE_LAYOUT.md): saved, user-configurable widget workspaces.

Reads db/derivatives.db and var/intelligence.db READ-ONLY; its own store is var/workspace.db.

THE HOOK INTO THE PILOT (applied by the owner after market close, together with the screener's):

    # server/kanida_pilot/app.py, at the end of create_app(), immediately before `return app`:
    from .screener import mount as mount_screener
    from .workspace import mount as mount_workspace
    mount_screener(app,settings)
    mount_workspace(app,settings)

Mount the screener first: the Greeks widget reuses its IV cache. A workspace that fails to start is logged and
skipped; the pilot runs without it.
"""
from __future__ import annotations
import logging,os
from pathlib import Path

log=logging.getLogger('workspace')
DEFAULT_DB=str(Path(__file__).resolve().parents[3]/'var'/'workspace.db')


def mount(app,settings,path=None):
 try:
  from .routes import build_router
  from .store import WorkspaceStore
  store=WorkspaceStore(path or os.getenv('PILOT_WORKSPACE_DATABASE') or DEFAULT_DB)
  before=len(app.router.routes)
  app.include_router(build_router(app,store,settings))
  added=app.router.routes[before:]
  del app.router.routes[before:]
  app.router.routes[0:0]=added
  app.state.workspace_store=store
  return store
 except Exception:
  log.exception('The workspace could not start; the pilot runs without it.')
  return None
