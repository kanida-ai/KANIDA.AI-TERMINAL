"""Rupeezy/Vortex automated daily authentication — mirrors services/zerodha_auto_auth.py.

Vortex tokens are SESSION-SCOPED with NO refresh token, so the only way to mint a fresh
one is to replay the interactive login. This bot does that headlessly, then exchanges the
`auth` request-token for an access_token and stores it on the ACTIVE rupeezy broker_account
(vault-encrypted), so the order-flow poller keeps working hands-off.

How Vortex auth works:
  1. GET https://flow.rupeezy.in?applicationId=<application_id>
  2. Step 1: enter MOBILE NUMBER (input[name=user_id]) → Login
  3. Step 2: enter PASSWORD           (best-guess selector — VERIFY on first run)
  4. Step 3: enter TOTP (pyotp 6-digit) (best-guess selector — VERIFY on first run)
  5. Vortex redirects to the app's redirect_uri with ?auth=<request_token>
  6. checksum = SHA256(application_id + auth + x_api_key); POST {base}/user/session
     → {data:{access_token}}   (via the existing RupeezyAuth provider)
  7. Store access_token on the broker_account (vault), status ACTIVE, token_date today.

Security (same posture as the Kite bot):
  - Login creds read from ENV ONLY: RUPEEZY_MOBILE, RUPEEZY_PASSWORD, RUPEEZY_TOTP_SECRET.
    Never stored in DB, never logged (redacted). app_id/x-api-key come from the vault.
  - Token preview logged is first 8 chars only. Browser context is ephemeral.

CAVEATS (honest):
  - Steps 2-3 selectors are best-guess (the multi-step form's later screens can't be seen
    without logging in). If the first real run fails at stage 'password'/'totp', adjust the
    selectors here to match the actual DOM (run scripts/vortex_login_inspect after step 1).
  - If Rupeezy uses SMS-OTP (not an authenticator TOTP), hands-off auth is impossible —
    the account must be re-connected manually each day.
"""
from __future__ import annotations
import asyncio, logging, os, re, sqlite3, time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Dict, Optional
from urllib.parse import parse_qs, urlparse

log = logging.getLogger("kanida.vortex_auto_auth")
IST = timezone(timedelta(hours=5, minutes=30))
LOGIN_FLOW_URL = "https://flow.rupeezy.in"
T_LOGIN_MS, T_STEP_MS, T_REDIR_MS = 30_000, 20_000, 30_000
_PORTAL_DB = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "db", "kanida_universe.db")


@dataclass
class AuthResult:
    status: str; stage: Optional[str]; error_code: Optional[str]
    error_detail: Optional[str]; token_preview: Optional[str]; elapsed_ms: int


def _load_login_creds() -> Optional[Dict[str, str]]:
    mob = os.environ.get("RUPEEZY_MOBILE", "").strip()
    pwd = os.environ.get("RUPEEZY_PASSWORD", "").strip()
    totp = os.environ.get("RUPEEZY_TOTP_SECRET", "").strip()
    if not all([mob, pwd, totp]):
        return None
    return {"mobile": mob, "password": pwd, "totp_secret": totp}


_REDACT = [re.compile(r"password=[^&\s]*", re.I), re.compile(r"totp=\d+", re.I),
           re.compile(r"auth=[^&\s]+", re.I), re.compile(r"x-api-key[^&\s]*", re.I)]
def _redact(t: str) -> str:
    if not t: return t
    for p in _REDACT: t = p.sub("[REDACTED]", t)
    return t[:800]


class AutoAuthError(Exception): code = "UNKNOWN"
class BadCredentialsError(AutoAuthError): code = "BAD_CREDS"
class TotpFailedError(AutoAuthError): code = "TOTP_FAILED"
class RedirectMalformedError(AutoAuthError): code = "REDIRECT_MALFORMED"
class ExchangeError(AutoAuthError): code = "EXCHANGE_ERROR"
class BrowserCrashedError(AutoAuthError): code = "BROWSER_CRASHED"


def _active_account():
    """Return (broker_account_id, user_id) of the ACTIVE/PENDING rupeezy account, or None."""
    con = sqlite3.connect(_PORTAL_DB)
    r = con.execute("SELECT broker_account_id, user_id FROM broker_accounts "
                    "WHERE broker='rupeezy' ORDER BY (status='ACTIVE') DESC, updated_at DESC LIMIT 1").fetchone()
    con.close()
    return (r[0], str(r[1])) if r else None


@asynccontextmanager
async def _browser(headless: bool = True) -> AsyncIterator[Any]:
    from playwright.async_api import async_playwright
    async with async_playwright() as p:
        b = await p.chromium.launch(headless=headless, args=["--no-sandbox", "--disable-dev-shm-usage"])
        try:
            ctx = await b.new_context(user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
                    viewport={"width": 1280, "height": 800})
            yield ctx
        finally:
            await b.close()


async def _capture_auth(ctx, app_id: str, creds: Dict[str, str]) -> str:
    import pyotp
    page = await ctx.new_page()
    got: Dict[str, str] = {}; ev = asyncio.Event()
    def _cap(url: str):
        # the auth token lands on the REDIRECT away from flow.rupeezy.in (?auth=...)
        if "auth=" in url and "got" not in got and "flow.rupeezy.in" not in urlparse(url).netloc:
            got["got"] = url; ev.set()
    page.on("request", lambda r: _cap(r.url)); page.on("response", lambda r: _cap(r.url))

    await page.goto(f"{LOGIN_FLOW_URL}?applicationId={app_id}", wait_until="domcontentloaded", timeout=T_LOGIN_MS)
    # ── Step 1: mobile (VERIFIED selector) ──
    await page.wait_for_selector('input[name="user_id"]', timeout=T_LOGIN_MS)
    await page.fill('input[name="user_id"]', creds["mobile"])
    await page.click('button[type="submit"]')
    # ── Step 2: password (best-guess) ──
    try:
        await page.wait_for_selector('input[type="password"], .error, [class*="error"], [class*="danger"]', timeout=T_STEP_MS)
    except Exception as e:
        raise TimeoutError(f"password step stalled: {e}")
    errel = await page.query_selector('.error, [class*="error-msg"], [class*="danger"]')
    if errel:
        txt = (await errel.inner_text() or "").strip()
        if txt: raise BadCredentialsError(txt[:150])
    pw = await page.query_selector('input[type="password"]')
    if pw:
        await pw.fill(creds["password"])
        b = await page.query_selector('button[type="submit"]')
        if b:
            try: await b.click(timeout=3000)
            except Exception: pass
    # ── Step 3: TOTP (best-guess: numeric/otp input) ──
    if not ev.is_set():
        totp = pyotp.TOTP(creds["totp_secret"]).now()
        try:
            await page.wait_for_selector('input[type="number"], input[inputmode="numeric"], input[name*="otp" i], input[name*="totp" i]', timeout=T_STEP_MS)
        except Exception:
            pass
        ti = await page.query_selector('input[type="number"], input[inputmode="numeric"], input[name*="otp" i], input[name*="totp" i]')
        if ti:
            await ti.fill(totp)
            b = await page.query_selector('button[type="submit"]')
            if b:
                try: await b.click(timeout=3000)
                except Exception: pass
    # ── wait for the ?auth= redirect ──
    try:
        await asyncio.wait_for(ev.wait(), timeout=T_REDIR_MS / 1000)
    except asyncio.TimeoutError:
        errel = await page.query_selector('.error, [class*="danger"]')
        if errel:
            txt = (await errel.inner_text() or "").strip()
            if txt and ("otp" in txt.lower() or "code" in txt.lower() or "totp" in txt.lower()):
                raise TotpFailedError(txt[:150])
        raise TimeoutError("redirect with ?auth= never arrived (verify password/TOTP selectors)")
    auth = parse_qs(urlparse(got.get("got", "")).query).get("auth", [None])[0]
    if not auth:
        raise RedirectMalformedError("no auth param in redirect")
    return auth


def _exchange_and_store(acc, auth: str) -> str:
    """Exchange auth → access_token via RupeezyAuth, store on the account (vault)."""
    import sys
    _b = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _b not in sys.path: sys.path.insert(0, _b)
    from autotrade.vault import get_decrypted_creds, store_access_token
    from autotrade.broker.rupeezy_auth_provider import RupeezyAuth
    bid, uid = acc
    creds = get_decrypted_creds(bid, uid)          # has api_key(app_id) + api_secret(x-api-key)
    ts = RupeezyAuth().exchange(creds, request_token=auth)
    token = getattr(ts, "access_token", None)
    if not token:
        raise ExchangeError("exchange returned no access_token")
    store_access_token(bid, token, user_id=uid, token_date=datetime.now(IST).strftime("%Y-%m-%d"))
    return token


def _best_effort_push_rupeezy_token_to_cloud() -> None:
    """RTS (2026-07-19): after a fresh Rupeezy token is minted + stored on the
    ACTIVE broker_account, ship it to the cloud-hosted backend copy so its
    RupeezyBroker adapter serves it too. The EXACT parallel of the Kite hook
    (scripts/auth_worker.py::_best_effort_push_token_to_cloud).

    BEST-EFFORT + fully guarded — it must NEVER break local minting:
      * Only runs when FALCON_PUBLISH_URL is set. Laptops without cloud config are
        completely unaffected (no-op).
      * Runs the push as a SEPARATE short-lived process (scripts/push_rupeezy_token
        .py) with a hard timeout, so a crash/hang there cannot touch this bot.
      * Every exception is swallowed and logged non-fatally.
    The token VALUE is never logged (the child prints only its length)."""
    if not os.environ.get("FALCON_PUBLISH_URL"):
        return
    try:
        import subprocess
        import sys as _sys
        # backend/services/vortex_auto_auth.py → project root = parents[2].
        _root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        script = os.path.join(_root, "scripts", "push_rupeezy_token.py")
        proc = subprocess.run(
            [_sys.executable, script],
            capture_output=True, text=True, timeout=60,
        )
        log.info("vortex_auto_auth: cloud token push rc=%s", proc.returncode)
        if proc.stdout:
            log.info("vortex_auto_auth: push stdout:\n%s", proc.stdout.strip())
        if proc.returncode != 0 and proc.stderr:
            log.warning("vortex_auto_auth: push stderr:\n%s", proc.stderr.strip())
    except Exception as e:  # noqa: BLE001 - best-effort, never fatal
        log.warning("vortex_auto_auth: cloud token push failed (non-fatal): %s", e)


async def run_auth_attempt() -> AuthResult:
    t0 = time.time()
    login = _load_login_creds()
    if login is None:
        return AuthResult("failed", None, "CONFIG_MISSING",
                          "Missing env: RUPEEZY_MOBILE, RUPEEZY_PASSWORD, RUPEEZY_TOTP_SECRET", None, int((time.time()-t0)*1000))
    acc = _active_account()
    if acc is None:
        return AuthResult("failed", None, "NO_ACCOUNT", "no rupeezy broker_account found", None, int((time.time()-t0)*1000))
    import sys
    _b = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _b not in sys.path: sys.path.insert(0, _b)
    from autotrade.vault import get_decrypted_creds
    app_id = getattr(get_decrypted_creds(acc[0], acc[1]), "api_key", "") or ""
    stage = "login"
    try:
        async with _browser(headless=True) as ctx:
            stage = "credentials"
            auth = await _capture_auth(ctx, app_id, login)
        stage = "access_token"
        token = await asyncio.to_thread(_exchange_and_store, acc, auth)
        # RTS: token is minted + stored → best-effort ship it to the cloud copy.
        # Fully guarded (no-op unless FALCON_PUBLISH_URL is set); can NEVER break
        # local minting — a failure here does not change this success result.
        try:
            await asyncio.to_thread(_best_effort_push_rupeezy_token_to_cloud)
        except Exception as e:  # noqa: BLE001 - belt-and-braces; never fatal
            log.warning("vortex_auto_auth: cloud push hook failed (non-fatal): %s", e)
        return AuthResult("success", "access_token", None, None, token[:8] + "...", int((time.time()-t0)*1000))
    except AutoAuthError as e:
        return AuthResult("failed", stage, e.code, _redact(str(e)), None, int((time.time()-t0)*1000))
    except asyncio.TimeoutError as e:
        return AuthResult("failed", stage, "TIMEOUT", _redact(str(e)), None, int((time.time()-t0)*1000))
    except Exception as e:
        return AuthResult("failed", stage, "UNEXPECTED", _redact(f"{type(e).__name__}: {e}"), None, int((time.time()-t0)*1000))


def token_is_valid() -> bool:
    """Cheap live check: /user/funds with the stored token. True = still good (skip re-auth)."""
    try:
        import sys, requests
        _b = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if _b not in sys.path: sys.path.insert(0, _b)
        from autotrade.vault import get_decrypted_creds
        acc = _active_account()
        if not acc: return False
        c = get_decrypted_creds(acc[0], acc[1])
        tok, xk = getattr(c, "access_token", None), getattr(c, "api_secret", None)
        if not tok: return False
        base = os.environ.get("RUPEEZY_API_BASE", "https://vortex-api.rupeezy.in/v2").rstrip("/")
        r = requests.get(f"{base}/user/funds", headers={"Authorization": f"Bearer {tok}", "x-api-key": xk or ""}, timeout=15)
        return r.status_code == 200 and (r.json() or {}).get("status") != "error"
    except Exception:
        return False
