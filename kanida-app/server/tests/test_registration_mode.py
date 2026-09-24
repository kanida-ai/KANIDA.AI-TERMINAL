import pytest
from fastapi.testclient import TestClient
from kanida_pilot.config import Settings
from kanida_pilot.app import create_app
from test_pilot import pilot_settings, Evidence


def registration(email='public@example.invalid', **extra):
    return dict(email=email, password='test-password-only-2026', name='Trader',
                policy_version='private-pilot-v1', **extra)


def test_public_signup_keeps_authority_and_entitlements_gated(tmp_path):
    settings = pilot_settings(tmp_path, registration_mode='public')
    app = create_app(settings, evidence=Evidence())
    with TestClient(app) as client:
        assert client.get('/api/pilot/config').json()['invitation_required'] is False
        result = client.post('/api/auth/register', json=registration(role='owner'))
        assert result.status_code == 200
        assert result.json()['user']['role'] == 'member'
        client.headers['X-Kanida-CSRF'] = result.json()['csrf']
        assert client.post('/api/account/onboarding', json=dict(acknowledge_pilot=True,
            policy_version='private-pilot-v1', timeframes=['1D'])).status_code == 200
        assert client.get('/api/derivatives/status').status_code == 402
        assert client.post('/api/admin/invites', json={'email':'other@example.invalid'}).status_code == 403
        # Closing registration affects new accounts, not established sessions.
        settings.registration_mode = 'gated'
        assert client.get('/api/auth/me').json()['user']['email'] == 'public@example.invalid'
        assert client.post('/api/auth/register', json=registration('next@example.invalid')).status_code == 403


def test_public_mode_still_validates_explicit_invitations_and_consent(tmp_path):
    app = create_app(pilot_settings(tmp_path, registration_mode='public'), evidence=Evidence())
    with TestClient(app) as client:
        assert client.post('/api/auth/register', json=registration(invite='forged')).status_code == 403
        data = registration(); data['policy_version'] = 'old'
        assert client.post('/api/auth/register', json=data).status_code == 400
        invite = app.state.auth.invite('owner@example.invalid', 'owner')
        result = client.post('/api/auth/register', json=registration('owner@example.invalid', invite=invite))
        assert result.status_code == 200 and result.json()['user']['role'] == 'owner'


@pytest.mark.parametrize('mode', ['', 'PUBLIC', 'false', 'open'])
def test_invalid_registration_mode_cannot_open_access(mode):
    with pytest.raises(ValueError, match='PILOT_REGISTRATION_MODE'):
        Settings(registration_mode=mode)


@pytest.mark.parametrize('mode,accepted', [('public',True), ('gated',False)])
def test_google_registration_uses_the_same_gate(tmp_path,monkeypatch,mode,accepted):
    import httpx
    from urllib.parse import urlparse,parse_qs
    from google.oauth2 import id_token
    from sqlalchemy import select
    from kanida_pilot.db import oauth,row
    from kanida_pilot.auth import digest
    from kanida_pilot.errors import PilotError
    app = create_app(pilot_settings(tmp_path,registration_mode=mode),evidence=Evidence())
    auth=app.state.auth
    auth.settings.google_client_id='fixture';auth.settings.google_client_secret='fixture'
    auth.http=httpx.Client(transport=httpx.MockTransport(lambda r:httpx.Response(200,json={'id_token':'fixture'})))
    try:
        state=parse_qs(urlparse(auth.start_google('browser')).query)['state'][0]
        with app.state.db.tx() as c: flow=row(c,select(oauth).where(oauth.c.hash==digest(state)))
        monkeypatch.setattr(id_token,'verify_oauth2_token',lambda *a,**k:dict(nonce=flow['nonce'],
            email_verified=True,email='google@example.invalid',sub='fixture-sub',name='Trader'))
        if accepted:
            user=auth.google_callback(state,'browser','code')
            assert user['role']=='member' and not user['onboarded']
        else:
            with pytest.raises(PilotError,match='invitation'):
                auth.google_callback(state,'browser','code')
    finally:
        auth.http.close();app.state.db.close()
