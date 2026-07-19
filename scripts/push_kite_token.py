"""KTS — push the laptop's daily Kite (Zerodha) access token UP to the CLOUD.

The Zerodha token is minted ONLY on the laptop (scripts/auth_worker.py runs the
Playwright login every 30 min and writes the token into the local kite_tokens
DB). A cloud-hosted copy of this backend cannot mint it, so this script ships
the ALREADY-MINTED token over an authenticated HTTP endpoint:

    laptop (this script)
        --POST /api/falcon/publish/kite-token + X-Publish-Secret-->
    cloud (falcon/routers/publish_router.py)  →  kite_auth._save_token_to_db()

It is the token-sync counterpart of scripts/publish_to_cloud.py and mirrors that
script's env/arg handling exactly (FALCON_PUBLISH_URL + FALCON_PUBLISH_SECRET,
with a --cloud-url override).

SAFETY:
  * Reads today's token from the LOCAL DB via kite_auth._load_token_from_db().
    It NEVER mints a token and NEVER pushes an empty one — if the DB has no
    token for today it prints a LOUD message and exits non-zero.
  * The token VALUE is never printed. Only the URL, token_date, token length,
    HTTP status, and the JSON response are shown.

Usage:
    python scripts/push_kite_token.py
    python scripts/push_kite_token.py --cloud-url https://api.kanida.ai
    python scripts/push_kite_token.py --dry-run
    FALCON_PUBLISH_URL=https://api.kanida.ai \
        FALCON_PUBLISH_SECRET=...  python scripts/push_kite_token.py

Env:
    FALCON_PUBLISH_URL     base URL of the cloud backend (or --cloud-url)
    FALCON_PUBLISH_SECRET  shared secret → sent as X-Publish-Secret header
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

# ── Make the backend package importable (mirror scripts/auth_worker.py) ───────
_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT = os.path.dirname(_HERE)
_BACKEND = os.path.join(_PROJECT, "backend")
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

IST = timezone(timedelta(hours=5, minutes=30))
PUBLISH_PATH = "/api/falcon/publish/kite-token"


def _now_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


def _read_local_token() -> str | None:
    """Return today's Kite access token from the LOCAL DB, or None.

    Uses kite_auth's own read path (which loads config/.env and keys on today's
    IST-consistent date.today() row) — no re-mint, no schema duplication.
    """
    from services.kite_auth import _load_env_file, _load_token_from_db
    _load_env_file()
    return _load_token_from_db()


def post_token(cloud_url: str, secret: str, token: str, *,
               token_date: str, timeout: float = 30.0) -> tuple[int, dict]:
    """POST the token to the cloud ingest endpoint. Returns (status_code, body).

    The token is sent ONLY in the JSON body over the secret-gated endpoint; it is
    never placed on the URL or in a log line here.
    """
    url = cloud_url.rstrip("/") + PUBLISH_PATH
    resp = requests.post(
        url,
        json={"access_token": token, "token_date": token_date, "set_by": "cloud-sync"},
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
        description="Push the laptop's daily Kite access token to the cloud backend.")
    ap.add_argument("--cloud-url", default=None,
                    help="Base URL of the cloud backend (overrides FALCON_PUBLISH_URL).")
    ap.add_argument("--dry-run", action="store_true",
                    help="Read the local token + validate config; do NOT POST.")
    args = ap.parse_args(argv)

    token_date = datetime.now(IST).date().isoformat()
    print(f"[{_now_ist()}] push_kite_token: reading today's ({token_date}) token from local DB")

    try:
        token = _read_local_token()
    except Exception as e:
        print(f"\n[FAIL] Could not read local token: {e}", file=sys.stderr)
        return 1

    # Fail-safe + LOUD: never push an empty/absent token.
    if not token or not str(token).strip():
        print("\n[FAIL] No Kite access token for today in the local DB. "
              "Nothing to push (has the auth worker minted today's token yet?). "
              "Refusing to push an empty token.", file=sys.stderr)
        return 1
    token = str(token).strip()
    print(f"  local token present (len={len(token)}) — value not shown")

    cloud_url = args.cloud_url or os.environ.get("FALCON_PUBLISH_URL")
    secret = os.environ.get("FALCON_PUBLISH_SECRET")

    if args.dry_run:
        print(f"\n[dry-run] would POST to {(cloud_url or '<unset>').rstrip('/') + PUBLISH_PATH}")
        print("[dry-run] not POSTing. Token is ready.")
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
        status, body = post_token(cloud_url, secret, token, token_date=token_date)
    except Exception as e:
        print(f"\n[FAIL] POST error: {e}", file=sys.stderr)
        return 1

    print(f"    HTTP status: {status}")
    print(f"    response   : {body}")
    if status != 200 or not body.get("ok"):
        print("\n[FAIL] Cloud did not accept the token.", file=sys.stderr)
        return 1

    print(f"\n[OK] Pushed. Cloud stored token_date={body.get('token_date')}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
