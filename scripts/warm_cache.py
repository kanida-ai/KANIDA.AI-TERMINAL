"""Authenticated cache warmer for the Power User portal.

Replaces the bare unauthenticated curl in warm_cache.bat. The Falcon Top 10
endpoint (/api/power/today/falcon-top-20) requires a logged-in bearer token —
an unauthenticated request gets a 401 in ~0.06s and warms NOTHING, leaving the
first real visitor to pay the 100-200s cold persona-sim computation (and time
out behind Cloudflare's ~100s limit).

This script:
  1. logs in as admin (POWER_ADMIN_EMAIL + POWER_ADMIN_SECRET from config/.env)
  2. fires one authenticated request per universe so each landing view is warm

stdlib only (urllib) so it runs under the backend's Anaconda python with no deps.
Exit 0 only if login + the primary (all500) warm both succeed.
"""
from __future__ import annotations

import json
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

BASE = "http://127.0.0.1:8001"
LOGIN_URL = f"{BASE}/api/power/auth/invite-login"
WARM_PATH = "/api/power/today/falcon-top-20"
# Default landing view is all500; warm the universes the frontend filter exposes.
UNIVERSES = ["all500", "nifty50", "nifty100", "nifty200", "fno"]
WARM_TIMEOUT = 400  # the cold persona sim can take 100-200s; allow headroom

_ENV_PATH = Path(__file__).resolve().parent.parent / "config" / ".env"


def _read_env(name: str) -> str:
    """Read KEY=VALUE from config/.env (no external dotenv dependency)."""
    if not _ENV_PATH.exists():
        return ""
    for line in _ENV_PATH.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if line.startswith(name + "="):
            return line[len(name) + 1:].strip()
    return ""


def _login() -> str:
    email = _read_env("POWER_ADMIN_EMAIL")
    secret = _read_env("POWER_ADMIN_SECRET")
    if not email or not secret:
        print("[warm] POWER_ADMIN_EMAIL/SECRET not in config/.env — cannot authenticate", flush=True)
        return ""
    body = json.dumps({"email": email, "code": secret}).encode("utf-8")
    req = urllib.request.Request(
        LOGIN_URL, data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        token = data.get("jwt", "")
        if not token:
            print("[warm] login returned no jwt", flush=True)
        return token
    except urllib.error.HTTPError as e:
        print(f"[warm] login HTTP {e.code}", flush=True)
        return ""
    except Exception as e:  # noqa: BLE001
        print(f"[warm] login failed: {type(e).__name__}: {e}", flush=True)
        return ""


def _warm(token: str, universe: str) -> bool:
    url = f"{BASE}{WARM_PATH}?universe={universe}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=WARM_TIMEOUT) as resp:
            resp.read()
            dt = time.time() - t0
            print(f"[warm] {universe}: HTTP {resp.status} in {dt:.1f}s", flush=True)
            return resp.status == 200
    except urllib.error.HTTPError as e:
        print(f"[warm] {universe}: HTTP {e.code} in {time.time()-t0:.1f}s", flush=True)
        return False
    except Exception as e:  # noqa: BLE001
        print(f"[warm] {universe}: FAILED in {time.time()-t0:.1f}s — {type(e).__name__}: {e}", flush=True)
        return False


def main() -> int:
    token = _login()
    if not token:
        return 1
    primary_ok = _warm(token, UNIVERSES[0])
    # Warm the rest best-effort; the persona sim is cached after the first call,
    # so these are much faster (only the per-universe explainer recomputes).
    for u in UNIVERSES[1:]:
        _warm(token, u)
    return 0 if primary_ok else 1


if __name__ == "__main__":
    sys.exit(main())
