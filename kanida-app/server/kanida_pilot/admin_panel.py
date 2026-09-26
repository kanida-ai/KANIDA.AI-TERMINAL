"""Admin panel for the cloud preview: the old portal's access workflow, rebuilt inside this app (its own tables).

  * Invite codes  - shareable, limited uses, optional expiry, revocable. Stored hashed; the full code is shown ONCE.
  * Access requests (waitlist) - public "request access"; the owner approves (a single-use, email-bound invitation link
    is created for that email) or declines. No email is sent by this build: the owner shares the link.
  * Users - list with last activity; deactivate (all sessions revoked immediately) / reactivate. The owner account and
    the acting owner can never be deactivated here.
  * Jobs & health - every background job this app depends on, with its last run, expected cadence and a health word
    (ok / late / failing / stopped / not configured). Read-only: nothing is started or stopped from here.
Owner-only except POST /api/access/request (public, rate limited).
"""
from __future__ import annotations
import sqlite3
import time
from pathlib import Path

from fastapi import Body, Request
from sqlalchemy import func, select

from .auth import email_value, new_code, digest
from .db import access_requests, invite_codes, sessions, users, record, ident, now, row
from .errors import PilotError


def _code_row(r):
 state = 'revoked' if r['revoked'] else 'expired' if (r['expires'] and r['expires'] <= now()) else 'used up' if r['uses'] >= r['uses_max'] else 'active'
 return {'id': r['id'], 'code_hint': f"KANIDA-…-{r['last4']}", 'uses': r['uses'], 'uses_max': r['uses_max'], 'expires': r['expires'],
         'revoked': r['revoked'], 'note': r['note'], 'created': r['created'], 'state': state}


def _health(last, every_s, error=None, alive=True):
 """A plain word for one job: its last success against its expected cadence."""
 if not alive:return 'stopped'
 if error and (not last or error > last):return 'failing'
 if last is None:return 'waiting'
 return 'ok' if time.time() - last <= every_s * 3 + 30 else 'late'


def install(app, db, auth, settings, owner):
 @app.post('/api/access/request')
 def request_access(request: Request, data: dict = Body(...)):
  auth.limit('access:' + str(request.client.host if request.client else ''), 5, 3600)
  email = email_value(data.get('email'))
  name = str(data.get('name') or '')[:80];note = str(data.get('note') or '')[:500]
  with db.tx() as c:
   if row(c, select(users).where(users.c.email == email)):
    return {'ok': True}                    # never reveals whether an account exists
   pending = row(c, select(access_requests).where(access_requests.c.email == email, access_requests.c.status == 'pending'))
   if not pending:
    c.execute(access_requests.insert().values(id=ident(), email=email, name=name, note=note, status='pending', created=now()))
  return {'ok': True}

 @app.post('/api/admin/codes')
 def issue_codes(request: Request, data: dict = Body(default={})):
  acting = owner(request)
  try:
   n = int(data.get('count', 1));uses = int(data.get('uses', 1));days = data.get('days')
   days = None if days in (None, '') else int(days)
  except (TypeError, ValueError):raise PilotError(400, 'FIELD_INVALID', 'count, uses and days must be whole numbers.')
  if not 1 <= n <= 50:raise PilotError(400, 'FIELD_INVALID', 'Issue between 1 and 50 codes at a time.')
  if not 1 <= uses <= 100:raise PilotError(400, 'FIELD_INVALID', 'Each code can allow 1 to 100 sign-ups.')
  if days is not None and not 1 <= days <= 90:raise PilotError(400, 'FIELD_INVALID', 'Expiry must be 1 to 90 days.')
  note = str(data.get('note') or '')[:200];out = []
  with db.tx() as c:
   for _ in range(n):
    code = new_code()
    c.execute(invite_codes.insert().values(id=ident(), hash=digest(code), last4=code[-4:], uses_max=uses, uses=0,
                                           expires=now() + days * 86400 if days else None, note=note, created_by=acting['id'], created=now()))
    out.append(code)
   record(c, acting['id'], 'account', 'Invite codes issued', f'{n} code(s), {uses} sign-up(s) each' + (f', {days} days' if days else ', no expiry'))
  return {'codes': out, 'signup_url': settings.origin + '/signup', 'note': 'Shown once. Share each code with the person; they enter it when signing up.'}

 @app.get('/api/admin/codes')
 def list_codes(request: Request):
  owner(request)
  with db.tx() as c:rows = c.execute(select(invite_codes).order_by(invite_codes.c.created.desc()).limit(500)).mappings().all()
  return {'codes': [_code_row(r) for r in rows]}

 @app.post('/api/admin/codes/{cid}/revoke')
 def revoke_code(request: Request, cid: str):
  acting = owner(request)
  with db.tx() as c:
   r = row(c, select(invite_codes).where(invite_codes.c.id == cid))
   if not r:raise PilotError(404, 'NOT_FOUND', 'There is no such code.')
   if not r['revoked']:
    c.execute(invite_codes.update().where(invite_codes.c.id == cid).values(revoked=now()))
    record(c, acting['id'], 'account', 'Invite code revoked', f"Code ending {r['last4']} revoked.")
   r = row(c, select(invite_codes).where(invite_codes.c.id == cid))
  return _code_row(r)

 @app.get('/api/admin/requests')
 def list_requests(request: Request, status: str = 'pending'):
  owner(request)
  with db.tx() as c:
   q = select(access_requests).order_by(access_requests.c.created.desc()).limit(500)
   if status in ('pending', 'approved', 'declined'):q = q.where(access_requests.c.status == status)
   rows = c.execute(q).mappings().all()
  return {'requests': [dict(r) for r in rows]}

 @app.post('/api/admin/requests/{rid}/{decision}')
 def decide(request: Request, rid: str, decision: str):
  acting = owner(request)
  if decision not in ('approve', 'decline'):raise PilotError(404, 'NOT_FOUND', 'Unknown decision.')
  with db.tx() as c:
   r = row(c, select(access_requests).where(access_requests.c.id == rid).with_for_update())
   if not r:raise PilotError(404, 'NOT_FOUND', 'There is no such request.')
   if r['status'] != 'pending':raise PilotError(409, 'ALREADY_DECIDED', f"This request was already {r['status']}.")
   c.execute(access_requests.update().where(access_requests.c.id == rid).values(status='approved' if decision == 'approve' else 'declined',
             decided=now(), decided_by=acting['id'], invite_url_issued=decision == 'approve'))
  url = None
  if decision == 'approve':
   raw = auth.invite(r['email'], 'member', hours=168)
   url = settings.origin + '/signup?invite=' + raw
  with db.tx() as c:
   record(c, acting['id'], 'account', 'Access request ' + ('approved' if url else 'declined'), f"Request from {r['email']}.")
  return {'ok': True, 'status': 'approved' if url else 'declined', 'invite_url': url,
          'note': 'Share this link with the person. It works once, for this email, for 7 days. No email was sent.' if url else None}

 @app.get('/api/admin/users')
 def list_users(request: Request):
  owner(request)
  with db.tx() as c:
   last = dict(c.execute(select(sessions.c.user_id, func.max(sessions.c.created)).group_by(sessions.c.user_id)).all())
   rows = c.execute(select(users.c.id, users.c.email, users.c.name, users.c.role, users.c.active, users.c.created).order_by(users.c.created.desc())).mappings().all()
  return {'users': [{**dict(r), 'last_sign_in': last.get(r['id'])} for r in rows]}

 @app.post('/api/admin/users/{uid}/{action}')
 def set_active(request: Request, uid: str, action: str):
  acting = owner(request)
  if action not in ('deactivate', 'reactivate'):raise PilotError(404, 'NOT_FOUND', 'Unknown action.')
  with db.tx() as c:
   t = row(c, select(users).where(users.c.id == uid).with_for_update())
   if not t:raise PilotError(404, 'NOT_FOUND', 'There is no such user.')
   if action == 'deactivate' and (t['id'] == acting['id'] or t['role'] == 'owner'):
    raise PilotError(409, 'CANNOT_DEACTIVATE_OWNER', 'The owner account cannot be deactivated here.')
   c.execute(users.update().where(users.c.id == uid).values(active=action == 'reactivate'))
   if action == 'deactivate':c.execute(sessions.update().where(sessions.c.user_id == uid).values(revoked=True))
   record(c, acting['id'], 'account', 'User ' + action + 'd', f"{t['email']} {action}d" + (' and signed out everywhere.' if action == 'deactivate' else '.'))
  return {'ok': True, 'id': uid, 'active': action == 'reactivate'}

 @app.get('/api/admin/jobs')
 def jobs(request: Request):
  owner(request)
  st = app.state;out = []
  al = getattr(st, 'strategy_builder_alerts', None)
  if al is not None:
   alive = bool(getattr(al, '_worker', None) and al._worker.is_alive())
   import os
   off = os.getenv('PILOT_SB_ALERTS', 'on').lower() == 'off'
   out.append({'key': 'alerts', 'name': 'Strategy alerts', 'cadence': f"every {getattr(al, 'every', 15)} s",
               'last_run': getattr(al, 'last_cycle_at', None), 'last_error': getattr(al, 'last_error', None),
               'health': 'switched off' if off else _health(getattr(al, 'last_cycle_at', None), getattr(al, 'every', 15), getattr(al, 'last_error', None), alive)})
  ex = getattr(st, 'strategy_builder_execution', None)
  if ex is not None:
   alive = bool(getattr(ex, '_worker', None) and ex._worker.is_alive())
   out.append({'key': 'paper_broker', 'name': 'Paper order filling (live quotes)', 'cadence': 'every 5 s while live data is on',
               'last_run': getattr(ex, 'last_cycle_at', None), 'last_error': getattr(ex, 'last_error', None),
               'health': _health(getattr(ex, 'last_cycle_at', None), 5, getattr(ex, 'last_error', None), alive) if alive else 'not configured'})
  lab = getattr(st, 'strategy_builder_lab', None)
  if lab is not None:
   try:
    with lab.lock:
     counts = dict(lab.c.execute("select status,count(*) from lab_runs group by status").fetchall())
     lastf = lab.c.execute("select max(finished_at) from lab_runs").fetchone()[0]
    failed = counts.get('failed', 0)
    out.append({'key': 'lab', 'name': 'Lab tests (queue)', 'cadence': 'on demand', 'last_run': lastf, 'last_error': None,
                'health': 'ok', 'detail': {k: counts.get(k, 0) for k in ('queued', 'running', 'done', 'failed', 'cancelled')}, 'failed': failed})
   except Exception:out.append({'key': 'lab', 'name': 'Lab tests (queue)', 'cadence': 'on demand', 'health': 'failing', 'last_run': None})
  # the market-data capture writes its own ledger; read it read-only
  path = getattr(settings, 'derivatives_database', None)
  cap = {'key': 'capture', 'name': 'Option market capture', 'cadence': 'every bar during market hours', 'last_run': None, 'health': 'not configured'}
  if path and Path(path).exists():
   try:
    con = sqlite3.connect(f'file:{path}?mode=ro', uri=True, timeout=5)
    if not con.execute("select 1 from sqlite_master where name='captures'").fetchone():
     con.close();raise LookupError('no capture ledger')
    ok = con.execute("select max(mark_at) from captures where status in ('ok','partial')").fetchone()[0]
    last = con.execute("select mark_at,status,error from captures order by mark_at desc limit 1").fetchone()
    missed = con.execute("select count(*) from captures where status='missed' and session_date=(select max(session_date) from captures)").fetchone()[0]
    con.close()
    cap.update({'last_run': ok, 'last_attempt': last[0] if last else None, 'last_status': last[1] if last else None,
                'last_error_text': (last[2] or '')[:200] if last else None, 'missed_latest_session': missed,
                'health': 'ok' if (last and last[1] in ('ok', 'partial')) else 'failing' if last else 'waiting'})
   except LookupError:cap['health'] = 'not configured'
   except sqlite3.Error:cap['health'] = 'failing'
  out.append(cap)
  market = getattr(st, 'strategy_builder_market', None)
  feed = market.status() if market is not None and hasattr(market, 'status') else {'live': False, 'code': 'NOT_CONFIGURED'}
  return {'jobs': out, 'feed': {'live': feed.get('live'), 'code': feed.get('code'), 'message': feed.get('user_message'), 'detail': feed.get('reason')},
          'at': now()}
