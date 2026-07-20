"""RTS — push the laptop's daily Rupeezy (Vortex) access token UP to the CLOUD.

The Rupeezy token is minted ONLY on the laptop (services/vortex_auto_auth.py runs
the headless Vortex login and stores the freshly-exchanged access_token — vault-
encrypted — on the ACTIVE rupeezy broker_account). A cloud-hosted copy of this
backend cannot mint it (no Playwright, no login creds there), so this script ships
the ALREADY-MINTED token over an authenticated HTTP endpoint:

    laptop (this script)
        --POST /api/falcon/publish/rupeezy-token + X-Publish-Secret-->
    cloud (falcon/routers/publish_router.py)  →  vault.store_access_token()

It is the Rupeezy counterpart of scripts/push_kite_token.py and mirrors that
script's env/arg handling exactly (FALCON_PUBLISH_URL + FALCON_PUBLISH_SECRET,
with a --cloud-url override). The ONE difference: the Rupeezy token is PER
broker_account, so this script resolves the ACTIVE rupeezy account and reads its
CURRENT decrypted access_token from the vault, then POSTs BOTH the
broker_account_id and the token.

SAFETY:
  * Reuses vortex_auto_auth._active_account() + vault.get_decrypted_creds() — the
    same read paths the mint bot / adapter use. It NEVER mints a token and NEVER
    pushes an empty one — no active account, or no token on it, prints a LOUD
    message and exits non-zero.
  * The token VALUE is never printed. Only the URL, the broker_account_id, the
    token_date, the token length, the HTTP status, and the JSON response are shown.

Usage:
    python scripts/push_rupeezy_token.py
    python scripts/push_rupeezy_token.py --cloud-url https://api.kanida.ai
    python scripts/push_rupeezy_token.py --dry-run
    FALCON_PUBLISH_URL=https://api.kanida.ai \
        FALCON_PUBLISH_SECRET=...  python scripts/push_rupeezy_token.py

Env:
    FALCON_PUBLISH_URL     base URL of the cloud backend (or --cloud-url)
    FALCON_PUBLISH_SECRET  shared secret → sent as X-Publish-Secret header
    FALCON_VAULT_KEY       vault key (to DECRYPT the local token for shipping)
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

try:
    import requests
except ImportError:  # pragma: no cover - dependency check
    print("[FAIL] The 'requests' package is required. pip install requests",
          file=sys.stderr)
    sys.exit(1)

# ── Make the backend package importable (mirror scripts/push_kite_token.py) ───
_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT = os.path.dirname(_HERE)
_BACKEND = os.path.join(_PROJECT, "backend")
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

IST = timezone(timedelta(hours=5, minutes=30))
PUBLISH_PATH = "/api/falcon/publish/rupeezy-token"


def _now_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


def _read_local_rupeezy_token() -> tuple[str | None, str | None]:
    """Return (broker_account_id, access_token) for the ACTIVE rupeezy account
    from the LOCAL vault, or (None, ...) / (id, None).

    Reuses vortex_auto_auth._active_account() (the exact lookup the mint bot uses)
    and vault.get_decrypted_creds() (the exact decrypt path the adapter uses) —
    no re-mint, no schema duplication, no bespoke SQL.
    """
    # vault._load_env_file() pulls FALCON_VAULT_KEY from config/.env if unset.
    from autotrade import vault
    vault._load_env_file()
    from services.vortex_auto_auth import _active_account

    acc = _active_account()
    if not acc:
        return None, None
    bid, uid = acc
    creds = vault.get_decrypted_creds(bid, uid)
    token = getattr(creds, "access_token", None) if creds else None
    return bid, token


def post_token(cloud_url: str, secret: str, broker_account_id: str, token: str, *,
               token_date: str, timeout: float = 30.0) -> tuple[int, dict]:
    """POST the token to the cloud ingest endpoint. Returns (status_code, body).

    The token is sent ONLY in the JSON body over the secret-gated endpoint; it is
    never placed on the URL or in a log line here.
    """
    url = cloud_url.rstrip("/") + PUBLISH_PATH
    resp = requests.post(
        url,
        json={"broker_account_id": broker_account_id, "access_token": token,
              "token_date": token_date, "status": "ACTIVE"},
        headers={"X-Publish-Secret": secret, "Content-Type": "application/json"},
        timeout=timeout,
    )
    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text}
    return resp.status_code, body


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Push the laptop's daily Rupeezy access token to the cloud backend.")
    ap.add_argument("--cloud-url", default=None,
                    help="Base URL of the cloud backend (overrides FALCON_PUBLISH_URL).")
    ap.add_argument("--dry-run", action="store_true",
                    help="Resolve the local token + validate config; do NOT POST.")
    args = ap.parse_args(argv)

    token_date = datetime.now(IST).date().isoformat()
    print(f"[{_now_ist()}] push_rupeezy_token: resolving ACTIVE rupeezy account + "
          f"today's ({token_date}) token from the local vault")

    try:
        broker_account_id, token = _read_local_rupeezy_token()
    except Exception as e:
        print(f"\n[FAIL] Could not read local rupeezy token: {e}", file=sys.stderr)
        return 1

    # Fail-safe + LOUD: never push without an account, never push an empty token.
    if not broker_account_id:
        print("\n[FAIL] No ACTIVE rupeezy broker_account found in the local vault. "
              "Nothing to push (has the account been connected?).", file=sys.stderr)
        return 1
    if not token or not str(token).strip():
        print(f"\n[FAIL] No Rupeezy access token on account {broker_account_id} in "
              "the local vault. Nothing to push (has vortex_auto_auth minted today's "
              "token yet, and is FALCON_VAULT_KEY set to decrypt it?). Refusing to "
              "push an empty token.", file=sys.stderr)
        return 1
    token = str(token).strip()
    print(f"  active account={broker_account_id} — token present (len={len(token)}) "
          "— value not shown")

    cloud_url = args.cloud_url or os.environ.get("FALCON_PUBLISH_URL")
    secret = os.environ.get("FALCON_PUBLISH_SECRET")

    if args.dry_run:
        print(f"\n[dry-run] would POST to {(cloud_url or '<unset>').rstrip('/') + PUBLISH_PATH}")
        print("[dry-run] not POSTing. Account + token are ready.")
        return 0

    if not cloud_url:
        print("\n[FAIL] No cloud URL. Set FALCON_PUBLISH_URL or pass --cloud-url.",
              file=sys.stderr)
        return 1
    if not secret:
        print("\n[FAIL] FALCON_PUBLISH_SECRET not set in env.", file=sys.stderr)
        return 1

    url = cloud_url.rstrip("/") + PUBLISH_PATH
    print(f"\n  POST {url}")
    try:
        status, body = post_token(cloud_url, secret, broker_account_id, token,
                                  token_date=token_date)
    except Exception as e:
        print(f"\n[FAIL] POST error: {e}", file=sys.stderr)
        return 1

    print(f"    HTTP status: {status}")
    print(f"    response   : {body}")
    if status != 200 or not body.get("ok"):
        print("\n[FAIL] Cloud did not accept the token.", file=sys.stderr)
        return 1

    print(f"\n[OK] Pushed. Cloud stored account={body.get('broker_account_id')} "
          f"token_date={body.get('token_date')}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
