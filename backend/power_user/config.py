"""Power User portal configuration — environment-driven.

Loaded once at module import. Environment variables take precedence over defaults.

Conventions:
  KANIDA_DB_PATH     — main DB (kanida_universe.db); shared with operator engine
  POWER_DB_PATH      — alias for KANIDA_DB_PATH (kept for clarity in this module)
  POWER_JWT_SECRET   — HS256 signing key (256-bit random); MUST be set in prod
  POWER_JWT_TTL_HR   — JWT validity hours (default 24)
  POWER_ADMIN_SECRET — gate for /api/power/admin/* endpoints; reuses operator's ADMIN_SECRET if set
  POWER_RATE_LIMIT_ANON_PER_HOUR — anonymous random-replay cap (default 3)
"""
from __future__ import annotations

import os
import secrets
from pathlib import Path

_HERE = Path(__file__).parent
_REPO_ROOT = _HERE.parent.parent

# ── DB path ──────────────────────────────────────────────────────────────
# Power user tables MUST live in kanida_universe.db (same DB as the engine
# read-only tables: falcon_features, falcon_pattern_*, ohlc_daily, etc.).
# Cross-DB JOINs in SQLite would force ATTACH gymnastics; co-locating keeps
# things simple.
#
# Important: do NOT fall through to KANIDA_DB_PATH. That env var is set by
# backend/main.py to kanida_quant.db (legacy auth DB). Resolve in the same
# way backend/falcon/config.py does: FALCON_DB_PATH if set, else canonical.
def _resolve_power_db_path() -> str:
    env = os.environ.get("POWER_DB_PATH") or os.environ.get("FALCON_DB_PATH")
    if env:
        return env
    canonical = _REPO_ROOT / "data" / "db" / "kanida_universe.db"
    if canonical.exists():
        return str(canonical)
    rnd_path = _REPO_ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
    if rnd_path.exists():
        return str(rnd_path)
    return str(canonical)


POWER_DB_PATH = _resolve_power_db_path()

# RND DB — holds falcon_outcomes (827k historical rows) used by the Falcon
# Top 20 "Historical evidence" bucket. Read-only access; PROD stays primary.
# RND DB also has the 87.8M-row ohlc_1min table (not used yet at runtime;
# the continuous 1-min fetcher comes in a follow-up).
POWER_RND_DB_PATH = os.environ.get(
    "POWER_RND_DB_PATH",
    str(_REPO_ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"),
)

# ── JWT ──────────────────────────────────────────────────────────────────
# In dev, a process-stable random key is generated each boot so tests work
# without setup. In prod, the env var MUST be set or auth tokens won't survive
# a backend restart (users would all be silently logged out).
_DEV_FALLBACK_JWT_SECRET = secrets.token_hex(32)
POWER_JWT_SECRET = os.environ.get("POWER_JWT_SECRET", _DEV_FALLBACK_JWT_SECRET)
POWER_JWT_ALG    = "HS256"
POWER_JWT_TTL_HR = int(os.environ.get("POWER_JWT_TTL_HR", "24"))

# ── Admin / rate limit ───────────────────────────────────────────────────
POWER_ADMIN_SECRET = os.environ.get(
    "POWER_ADMIN_SECRET",
    os.environ.get("ADMIN_SECRET", ""),
)

# Phase 1b: invite-code auth (replaces Google OAuth).
# The admin's email is operator-locked at the config layer — public knowledge
# is fine because logging in AS admin still requires POWER_ADMIN_SECRET as the
# login code. The pair (email, code) is what authenticates, not either alone.
POWER_ADMIN_EMAIL = os.environ.get(
    "POWER_ADMIN_EMAIL",
    "p.sundar.shyam@gmail.com",
).lower().strip()

POWER_RATE_LIMIT_ANON_PER_HOUR = int(
    os.environ.get("POWER_RATE_LIMIT_ANON_PER_HOUR", "3")
)

# Phase 1b: rate limit for the new email+code login endpoint. Same budget
# as the legacy Google-based redeem (5/hr/IP) — invite codes are 24-bit
# random hex, brute-forcing within rate limit is mathematically infeasible.
POWER_INVITE_LOGIN_LIMIT_PER_HOUR = int(
    os.environ.get("POWER_INVITE_LOGIN_LIMIT_PER_HOUR", "5")
)

# ── Featured replay dates (locked 2026-05-14) ────────────────────────────
# These get pre-computed and stored with is_featured=1 in falcon_replay_cache.
# Changing this list requires re-running precompute_featured() to materialise.
FEATURED_REPLAYS = [
    {
        "date":  "2026-04-15",
        "title": "The Blowout",
        "hook":  "Engine emitted top picks the morning after — 92% won at D+5, +12% avg at D+15.",
    },
    {
        "date":  "2024-11-04",
        "title": "Year-Ago Proof",
        "hook":  "Same engine, 18 months ago — still 96% win at D+5. Edge isn't a recent fluke.",
    },
    {
        "date":  "2025-12-15",
        "title": "Steady Day",
        "hook":  "A typical good day: 78% D+5 WR, +5.7% avg. The engine's median, not its peak.",
    },
]

# ── Google OAuth (frontend pulls these from NEXT_PUBLIC_* anyway) ────────
GOOGLE_CLIENT_ID     = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
