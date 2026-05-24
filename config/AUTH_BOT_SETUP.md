# Zerodha Auto-Auth Bot — Setup Guide

Layer 1 of the 3-layer auth system. Goal: 99% of trading days the daily
Zerodha access token refreshes without operator intervention.

---

## What this gives you

- Daily Kite access tokens generated automatically before market open
- 4 morning cycles (06:30 / 07:30 / 08:30 / 09:00 IST weekdays) so a single
  transient failure doesn't kill the day
- Boot-catchup on weekday restarts: if a cycle has passed and no SUCCESS
  is logged, the latest passed cycle fires immediately
- Audit trail in `falcon_auth_log` (success / failure / latency / stage)
- Live signal endpoints (`/api/power/picks/today`, `/live`) keep serving
  fresh data without operator login every day

---

## Step 1 — fill in `config/.env`

If you don't already have one, copy a template:

```bash
cp config/.env.template config/.env
# OR for a credentials-focused walkthrough:
cp config/.env.zerodha-bot.example config/.env
```

The five values that drive the bot:

| Key | Where to get it |
|---|---|
| `KITE_API_KEY` | `kite.trade/account/apps` → your app |
| `KITE_API_SECRET` | same page |
| `ZERODHA_USERNAME` | your Kite client ID (e.g. `AB1234`) |
| `ZERODHA_PASSWORD` | your Kite login password — NO leading/trailing spaces |
| `ZERODHA_TOTP_SECRET` | the base32 **seed** (NOT the rotating 6-digit code) |

### Your Zerodha account must be on **TOTP 2FA**, not "Mobile App Code"

Mobile App Code (Zerodha's mobile-only 2FA) cannot be automated — codes
only generate inside the Kite mobile app on your phone. If the bot
fails with the error `"Invalid App Code. N attempt(s) remain..."`, you're
on Mobile App Code mode and need to switch:

1. Log in to `kite.zerodha.com` → My Profile → Settings → Password & Security
2. Reset / disable 2FA → choose **External TOTP** (Authenticator app)
3. Continue with the TOTP-secret extraction below

### Extracting the TOTP secret

1. From the same Settings → Password & Security → Setup TOTP screen
2. When the QR code appears, click **"Can't scan QR? Enter manually"**
3. Zerodha shows a 16-32 character base32 string — **copy it**
4. Add the same secret to your authenticator app (Google Authenticator,
   Authy, 1Password) so you can still log in manually if the bot is offline
5. Confirm 2FA setup in Zerodha with the current 6-digit code

The secret looks like: `FZBTYNZTOEYC2RJSHA5DSKBTGN3WGYRY` — no spaces,
all uppercase A-Z + digits 2-7. Mixed case is suspicious; double-check
you copied the *whole* seed and didn't pick up a stray character.

---

## Step 2 — verify the credentials (one-shot)

Run this from the project root **on a weekday** (the scheduler skips
weekends but a manual test works any day):

```bash
python -c "
import sys, asyncio, os
sys.path.insert(0, 'backend')
from dotenv import load_dotenv
load_dotenv('config/.env')
from services.zerodha_auto_auth import run_auth_attempt, log_attempt
from power_user.config import POWER_DB_PATH
result = asyncio.run(run_auth_attempt(attempt_of_day=0, trigger_kind='manual'))
log_attempt(POWER_DB_PATH, 0, 'manual', result)
print(result)
"
```

Expected outcomes:

| Result | What it means | Action |
|---|---|---|
| `status='success', stage='access_token'` | Bot logged in, token written to `kite_tokens`, audit row written to `falcon_auth_log`. | All set. Move to Step 3. |
| `error_code='BAD_CREDS'` | Username or password wrong. | Re-check `.env`. |
| `error_code='TOTP_FAILED'` | Secret is wrong OR clock drift. | Re-extract the base32 seed (most common: copied the rotating code instead). |
| `error_code='CONFIG_MISSING'` | Env var not loaded. | Check `.env` is at `config/.env` and `load_dotenv` is hitting it. |
| `error_code='TIMEOUT'` | Zerodha login page slow. | Re-run; transient. |
| `error_code='REQUEST_BLOCKED'` | Captcha / IP block. | Wait 1 hour, retry. Indicates too many recent failed attempts. |

Confirm the audit trail:

```bash
python -c "
import sqlite3
con = sqlite3.connect('data/db/kanida_universe.db')
for row in con.execute('SELECT id, attempt_at, status, stage, error_code, token_preview FROM falcon_auth_log ORDER BY id DESC LIMIT 5'):
    print(row)
"
```

And the new token:

```bash
python -c "
import sqlite3
con = sqlite3.connect('data/db/kanida_quant.db')
for row in con.execute('SELECT id, token_date, set_by, created_at FROM kite_tokens ORDER BY id DESC LIMIT 3'):
    print(row)
"
```

---

## Step 3 — the scheduler is already armed

There is **no enable flag**. The scheduler (`backend/services/auth_scheduler.py`)
starts automatically when the backend boots (`backend/main.py` lifespan
hook). Once `.env` has the 5 credentials, the next weekday at 06:30 IST
will mint a fresh token without any further action.

Schedule:

| Cycle | Time | Behavior |
|---|---|---|
| 1 | 06:30 IST | First daily attempt (Zerodha tokens roll over at 06:00 IST) |
| 2 | 07:30 IST | Skipped if cycle 1 succeeded; retry otherwise |
| 3 | 08:30 IST | Same gating |
| 4 | 09:00 IST | Last attempt before market open. If this fails too, a Web Push notification fires (Layer 2). |

On weekday backend restarts, **boot-catchup** runs the latest-passed
cycle immediately, so a daytime restart still recovers the day.

Weekends are skipped — Zerodha doesn't trade Sat/Sun and tokens stay
valid through Monday morning.

---

## Step 4 — failure modes (auditable, all logged)

| Code | Meaning |
|---|---|
| `BAD_CREDS` | username/password rejected |
| `TOTP_FAILED` | 2FA code rejected (rare — usually pyotp clock drift, or account on Mobile App Code instead of TOTP) |
| `TIMEOUT` | page load > 30s |
| `KITE_API_ERROR` | `KiteConnect.generate_session` raised |
| `BROWSER_CRASHED` | Playwright resource issue OR redirect target unreachable |
| `REDIRECT_MALFORMED` | couldn't parse `request_token` from URL |
| `CONFIG_MISSING` | env var absent |
| `REQUEST_BLOCKED` | Zerodha returned unexpected page (captcha) |

All error details are **redacted** before being written to the audit log
(via `_redact_for_log` in `zerodha_auto_auth.py`) — credentials never leak.

---

## Step 5 — when to manually intervene

The bot is designed to be hands-off, but you should manually log in if:

1. You see 3+ consecutive `BAD_CREDS` (password may have been reset)
2. You see 5+ consecutive `REQUEST_BLOCKED` (IP may be temp-banned)
3. The audit log is silent for >24h on a weekday (scheduler died — check backend logs)

The admin page (`/power/admin`, role=admin) surfaces all three as red banners.

You can also trigger a manual refresh from the admin UI or via:

```bash
curl -X POST http://localhost:8001/api/power/admin/auth/refresh-now \
     -H "X-Admin-Secret: $POWER_ADMIN_SECRET"
```

---

## Files this setup touches

- `config/.env` — your credentials (gitignored)
- `config/.env.template` — committed placeholder template
- `config/.env.zerodha-bot.example` — credentials-focused template
- `data/db/kanida_quant.db.kite_tokens` — daily token storage
- `data/db/kanida_universe.db.falcon_auth_log` — audit trail
- `backend/services/zerodha_auto_auth.py` — the Playwright bot
- `backend/services/auth_scheduler.py` — the daemon thread + 4-cycle scheduler
- `backend/services/kite_auth.py` — token storage helper
- `backend/main.py` — lifespan hook that starts the scheduler

---

## Status of the wiring

- ✅ Playwright bot exists, tested, audit-traced
- ✅ Token storage + audit log schemas live
- ✅ `/admin/auth/refresh-now` admin endpoint
- ✅ Credentials template + setup guide (this file)
- ✅ Scheduler thread started by `main.py` boot
- ✅ Boot-catchup on weekday restarts
- ✅ End-to-end verified: SUCCESS row in `falcon_auth_log`, fresh row in `kite_tokens`,
  `today_already_succeeded()` returns True after one success

Next scheduled fire: **next weekday at 06:30 IST**.
