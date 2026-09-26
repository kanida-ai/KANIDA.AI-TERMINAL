"""Cloud-preview admin panel: invite codes, access requests (waitlist), users, jobs & health. Isolated fixtures."""
import pytest
from fastapi.testclient import TestClient
from kanida_pilot.app import create_app
from test_pilot import Evidence, pilot_settings, signup

PW = 'fixture-only-password-2026'


@pytest.fixture
def app(tmp_path):
 a = create_app(pilot_settings(tmp_path), evidence=Evidence())
 yield a
 a.state.db.close()


def register(app, email, invite):
 c = TestClient(app)
 return c, c.post('/api/auth/register', json={'email': email, 'password': PW, 'name': 'Tester', 'invite': invite, 'policy_version': 'private-pilot-v1'})


def test_invite_codes_are_shown_once_limited_and_revocable(app):
 owner = signup(app)
 r = owner.post('/api/admin/codes', json={'count': 2, 'uses': 1, 'days': 7, 'note': 'friends'}).json()
 a, b = r['codes']
 assert a.startswith('KANIDA-') and a != b
 listed = owner.get('/api/admin/codes').json()['codes']
 assert all(a not in str(x) for x in listed) and {x['state'] for x in listed} == {'active'}   # never listed in full
 _c, ok = register(app, 'one@example.invalid', a.lower())                                         # case/space tolerant
 assert ok.status_code == 200
 _c, again = register(app, 'two@example.invalid', a)
 assert again.status_code == 403                                                                   # used up
 bid = next(x['id'] for x in owner.get('/api/admin/codes').json()['codes'] if x['code_hint'].endswith(b[-4:]))
 assert owner.post(f'/api/admin/codes/{bid}/revoke', json={}).json()['state'] == 'revoked'
 _c, rev = register(app, 'three@example.invalid', b)
 assert rev.status_code == 403
 assert owner.post('/api/admin/codes', json={'count': 0}).status_code == 400


def test_access_request_approval_creates_a_one_time_email_bound_link(app):
 owner = signup(app)
 anon = TestClient(app)
 assert anon.post('/api/access/request', json={'email': 'wait@example.invalid', 'name': 'W', 'note': 'trader'}).json()['ok']
 assert anon.post('/api/access/request', json={'email': 'wait@example.invalid'}).json()['ok']         # no duplicate pending row
 reqs = owner.get('/api/admin/requests').json()['requests']
 assert len(reqs) == 1 and reqs[0]['status'] == 'pending'
 d = owner.post(f"/api/admin/requests/{reqs[0]['id']}/approve", json={}).json()
 assert d['status'] == 'approved' and '/signup?invite=' in d['invite_url']
 assert owner.post(f"/api/admin/requests/{reqs[0]['id']}/decline", json={}).status_code == 409
 token = d['invite_url'].split('invite=')[1]
 _c, other = register(app, 'someone@example.invalid', token)
 assert other.status_code == 403                                                                 # bound to the requester's email
 _c, ok = register(app, 'wait@example.invalid', token)
 assert ok.status_code == 200


def test_owner_can_deactivate_and_reactivate_a_user_but_never_the_owner(app):
 owner = signup(app)
 code = owner.post('/api/admin/codes', json={}).json()['codes'][0]
 member, r = register(app, 'm@example.invalid', code)
 member.headers.update({'X-Kanida-CSRF': r.json()['csrf'], 'Origin': 'http://testserver'})
 assert member.get('/api/account').status_code == 200
 users = {u['email']: u for u in owner.get('/api/admin/users').json()['users']}
 mid = users['m@example.invalid']['id']
 assert owner.post(f'/api/admin/users/{mid}/deactivate', json={}).json()['active'] is False
 assert member.get('/api/account').status_code == 401                                            # signed out everywhere
 assert owner.post(f"/api/admin/users/{users['owner@example.invalid']['id']}/deactivate", json={}).status_code == 409
 assert owner.post(f'/api/admin/users/{mid}/reactivate', json={}).json()['active'] is True


def test_admin_routes_are_owner_only(app):
 signup(app)
 code = signup  # noqa
 anon = TestClient(app)
 for path in ('/api/admin/codes', '/api/admin/requests', '/api/admin/users', '/api/admin/jobs'):
  assert anon.get(path).status_code in (401, 403)


def test_jobs_panel_reports_each_job_with_a_health_word(app):
 owner = signup(app)
 j = owner.get('/api/admin/jobs').json()
 keys = {x['key']: x for x in j['jobs']}
 assert 'capture' in keys and keys['capture']['health'] in ('not configured', 'waiting', 'ok', 'failing')
 assert 'feed' in j
