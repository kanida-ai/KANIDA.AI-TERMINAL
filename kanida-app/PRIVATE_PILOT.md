# KANIDA private pilot

The local pilot joins the approved chart experience to invitation-based accounts, onboarding, personal watchlists and plans, a synthetic order workflow, activity, Kite authorization and Razorpay test subscriptions. The existing research engine remains separate. This is a functioning local research and workflow pilot, not a released live AutoTrade service.

## Open it

- Computer: http://127.0.0.1:8082/welcome
- Phone on the same Wi-Fi: http://10.0.0.192:8082/welcome
- Expo Go instructions and QR: http://127.0.0.1:8082/connect
- The owner’s private invitation and invited email are in `var/OWNER_INVITATION.txt`. Choose your own password in the app. This file is ignored by Git; do not share it with testers.

Create the owner account, complete onboarding, then use Account to invite testers. After a tester registers, Account → enter their email → **Grant 14 days of pilot access** allows them to test before Razorpay is configured. This records an explicit access grant; it does not fabricate a payment.

The synthetic UI acceptance-test account exists only in the separate `var/ui-qa/qa.sqlite3` database. It is not an account in your pilot database.

## Customer journey

1. Landing → sign in or accept a private invitation.
2. Onboarding → choose chart horizons and acknowledge pilot conditions.
3. Discover → open a real stored chart → inspect drawing, dates, history and exit evidence.
4. Watch a setup or prepare a plan → review capital, stop, target and maximum hold → save a draft.
5. AutoTrade → review a named synthetic scenario → submit → observe queued, partial, open and closed/rejected states.
6. Pause new simulated entries, cancel an unfilled order or exit a filled simulation. Reserved virtual capital is released atomically with the order update.
7. Activity → inspect the event trail. Account → manage sessions, broker authorization, invitations and membership.

The ₹1,00,000 virtual wallet uses artificial prices and the plan’s stated cost assumption. These results are not historical returns, forecasts, actual broker charges or investment performance. The original stock-specific backtest and its virtual-capital analysis remain available through chart evidence.

## Provider setup still needed

Store server values in the ignored `.env.pilot`, never in `EXPO_PUBLIC_*`. Only the owner email and Kite API key/secret were copied from Terminal configuration. Google and Razorpay credentials do not exist yet. Provider flows have been tested with mocks, not real Google/Razorpay accounts or a real Kite login.

### Google

Create a Google **web OAuth client**, configure the consent screen and invited test users, then set:

```
PILOT_GOOGLE_CLIENT_ID=
PILOT_GOOGLE_CLIENT_SECRET=
```

Local callback: `http://127.0.0.1:8082/api/auth/google/callback`.
Hosted callback: `https://YOUR-PILOT-HOST/api/auth/google/callback`.

The server uses authorization-code exchange, state, nonce, PKCE, audience/issuer/expiry verification and a verified invited email. Google sign-in cannot bypass the invitation gate. See [Google OpenID Connect](https://developers.google.com/identity/openid-connect/openid-connect).

Native password sign-in stores an opaque session in SecureStore. The browser sign-in option uses a short-lived device code and PKCE proof, so no bearer token is placed in a deep link. Use a hosted HTTPS origin for phone Google/broker authorization; `127.0.0.1` on an iPhone refers to the iPhone. Expo Go and standalone builds have different redirect constraints: [Expo authentication guidance](https://docs.expo.dev/guides/authentication/).

### Razorpay

Create a test account/plan and set:

```
PILOT_RAZORPAY_KEY_ID=rzp_test_...
PILOT_RAZORPAY_KEY_SECRET=
PILOT_RAZORPAY_WEBHOOK_SECRET=
PILOT_RAZORPAY_PLAN_ID=plan_...
```

Webhook: `https://YOUR-PILOT-HOST/api/billing/webhook`. Localhost is not reachable by Razorpay. Subscribe to subscription lifecycle events including authenticated, activated, charged, pending, halted, cancelled, completed and updated.

Checkout uses the provider’s hosted subscription link. Return to KANIDA Billing and refresh access after checkout. HMAC-verified webhook events trigger a canonical provider lookup. An authorization alone does not grant a paid period. Period-end cancellation is supported. Uncertain creation is recovered by its unique `kanida_reference` note; it is never blindly retried. If no unique provider subscription can be recovered, the owner must inspect Razorpay before another subscription is created.

Only test keys are accepted. The configured plan controls the amount and cadence; no production price has been invented. References: [subscription APIs](https://razorpay.com/docs/api/payments/subscriptions/), [subscription test mode](https://razorpay.com/docs/payments/subscriptions/test/), [webhooks](https://razorpay.com/docs/webhooks/subscriptions/).

### Kite

API credentials are present. The Kite app must have the matching redirect configured before connecting:

- Local redirect: `http://127.0.0.1:8082/api/broker/kite/callback`.
- Hosted redirect: `https://YOUR-PILOT-HOST/api/broker/kite/callback`.
- Hosted postback: `https://YOUR-PILOT-HOST/api/broker/kite/postback`.

Use the same canonical host to start and finish authorization so its binding cookie is available. Do not overwrite an app’s production redirect without arranging a separate pilot app or a deliberate migration. Connect through Kite’s own login; never enter broker passwords or TOTP secrets into KANIDA. Tokens are encrypted at rest, account ownership is enforced, and reconnect is required after expiry. [Kite user/session documentation](https://kite.trade/docs/connect/v3/user/).

The staged order adapter records durable intentions, uses whole lots/tick sizes and fresh-quote checks, reconciles acceptance versus fills and prevents automatic retry after an uncertain submission. This release has a code-level live lock in addition to configuration gates. A server-side entry and protective-exit supervisor is still required; changing an environment flag cannot enable live execution.

CNC is restricted to delivery equity buying; MIS is intraday; NRML applies to actual derivative contracts. F&O eligibility of an equity does not turn that cash equity into an overnight short. [Kite order products and lifecycle](https://kite.trade/docs/connect/v3/orders/).

## Run and verify

From `kanida-app` in PowerShell:

```powershell
# First install only
python -m venv .pilot-venv
.\.pilot-venv\Scripts\python.exe -m pip install -r server/requirements.lock.txt
npm ci

# Verify
$env:PYTHONPATH = 'server'
.\.pilot-venv\Scripts\python.exe -m pytest server/tests -q
npm run typecheck
npx expo export --platform all --output-dir dist-pilot

# Start, after stopping only the verified prior pilot listener
.\scripts\start-pilot.ps1
```

The start script refuses to replace an unknown listener. It creates a private invitation if there is no owner account and launches a hidden server. Logs are in `var/pilot.*.log`. Research must be running on loopback port 8765. Keep Metro running on 8081 for Expo Go; `.env.local` contains only the public local API and Expo addresses. Update these addresses and `PILOT_ORIGINS` if the computer’s LAN IP changes.

The local backend uses `var/pilot.sqlite3` and a separate encryption key in `var/pilot.key`. Back up the database using SQLite’s online backup API and preserve the key securely; copying only the DB loses the ability to decrypt broker authorization. Never put DBs, keys, invitations or `.env.pilot` in Git or a container image. Cloud deployment should use a dedicated PostgreSQL database, Secrets Manager and tested backup/restore procedures.

## AWS and mobile release preparation

`Dockerfile`, the Falcon-root `.dockerignore`, `infra/task-definition.template.json` and `eas.json` are preparation artifacts. They have not been deployed. Docker/Terraform are not installed in this environment, so the container and PostgreSQL path have not been exercised here.

Use a new private staging service and database. Do not restart the old Terminal production ECS/RDS resources: the earlier environment review found unsafe live-trading defaults. Use an immutable ECR digest, HTTPS ALB/ACM, private Fargate and RDS networking, scoped IAM, health checks, log retention and backups. Fill all template placeholders and inject secrets from a separate pilot secret. Add Google/Razorpay secret entries only after creating those providers. The existing scanner should publish read-only evidence through a private service or versioned artifacts; do not expose its unauthenticated API publicly.

Example image build from the Falcon root:

```powershell
docker build -f kanida-app/Dockerfile --build-arg PUBLIC_API_URL=https://YOUR-PILOT-HOST -t kanida-private-pilot:RELEASE-ID .
```

For mobile, set `EXPO_PUBLIC_API_URL` to the deployed HTTPS pilot in the EAS preview environment, associate the project with the correct Expo account, then run EAS preview builds for Android and iOS. Android preview produces an APK. iOS installation requires Apple signing and appropriate device distribution/TestFlight. JavaScript bundles for both platforms exported successfully; this is not device testing or App Store approval. The build config rejects shipping a cloud build with a local HTTP API address.

## Remaining release boundaries

- Google/Razorpay creation, actual provider callbacks and hosted HTTPS verification.
- Fresh, account-scoped Kite market-data ingestion and its candle-close scan scheduling. Current research remains the supplied historical snapshot through 31 July 2026.
- Backtest recalculation with correct CNC/MIS/actual derivative contracts, financing, margins and instrument-specific costs. Do not market generic cash backtests as executable leveraged or overnight-short results.
- Approved automated entry/protective-exit supervision, broker/exchange/SEBI dependencies, static egress and order tagging, reconciliation after restart, kill switches and operating drills. The internal correlation tag is not a claim of regulatory approval.
- Market-data licensing and permitted use: broker authorization is not permission to redistribute data. Kite data is not fed into the synthetic simulator.
- PostgreSQL concurrency/migration tests, cloud deployment, restore testing, real device QA and store/billing-policy review before distribution.
- Dependency audit currently reports 13 moderate findings inherited through the Expo toolchain/router (`uuid` and `decode-uri-component` chains). No high or critical findings were reported. Automated suggestions would downgrade Expo/router across major versions; they were not applied. Resolve or explicitly review these before external release.

The app keeps these boundaries visible instead of presenting an unconfigured integration or historical snapshot as live.
