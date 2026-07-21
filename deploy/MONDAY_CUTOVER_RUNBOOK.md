# Monday Cutover Runbook — Serving + Live Execution → Cloud

**Goal:** make the cloud the live front door (serving) AND the live trade executor,
laptop drops to mining+publishing only. Reversible at every step.

## Preconditions — ALL DONE as of 2026-07-18
- Cloud infra live (68 resources); Terraform state in S3 (`kanida-tfstate-389642461326`).
- Cloud serving HTTPS `api.kanida.ai` → ALB (proven 200). ACM cert valid.
- Secrets loaded; `FALCON_PUBLISH_SECRET` set (cloud + `~/publish_secret.txt`).
- Cloud task def has: artifact flags, `TZ=Asia/Kolkata`, gates **OFF**
  (`FALCON_AUTOTRADE_ENABLED=false`, `EXECUTION_MODE=paper`).
- Token-sync deployed + verified: `POST /api/falcon/publish/kite-token` stores the token.
- Proxy reuse confirmed: cloud routes the operator's orders via existing
  `BROKER_PROXY_MAP` / `broker_accounts.egress_proxy_url_enc` → registered IP →
  **no new Zerodha IP registration needed for the operator's own account.**

## Window
Prefer a **market-closed** slot, or **Monday pre-09:15** (before campaigns fire).
If Monday feels hot, keep the laptop primary Monday and cut over the next calm
session — zero pressure, everything stays staged.

---

## Step 0 — Rebuild the cloud image from the latest code (do FIRST)
Rebuild from branch `feat/cloud-rupeezy-token-sync` **tip `6f8af52`** (or the latest
tip on that branch) — it cumulatively contains ALL cloud-migration work + sysagents
+ corp-action dedup + Rupeezy token-sync PLUS two real-money OMS fixes that MUST be
in the cloud or it launches with the bugs:
  - `6d20df5` — concurrent Magnifier/BTST split-entry (fixes 32s/61s slow fills)
  - `6f8af52` — holdings-aware pre-exit flat guard (settled CNC/BTST exits were
    silently leaking; broker-agnostic — Zerodha + Rupeezy)
Both are pure backend code: NO new routes / DB migration / secret / env (the
corp-action dedup flag defaults to 86400 in code). Rebuild `falcon-app-build.zip`
from that tree → CodeBuild (overwrites `:phase2`) → Step 5's `terraform apply` picks
it up. **Do NOT build from `57a3e1e` — it has neither OMS fix.**

## Step 1 — Laptop: adopt token-sync code + config (enables auto-push)
On the laptop (the running backend is old prod code, no push hook yet):
```
git merge feat/cloud-kite-token-sync        # or cherry-pick the 3 KTS files
# add to config/.env:
#   FALCON_PUBLISH_SECRET=<contents of publish_secret.txt>
#   FALCON_PUBLISH_URL=https://api.kanida.ai
# restart backend + the KanidaZerodhaAuth worker
```
The auth worker now pushes the daily token to the cloud after each mint.

## Step 2 — Sync the cloud DB to CURRENT live state  ⚠️ the critical step
The cloud's `/data/db` is the **Friday seed**; live positions/campaigns have moved.
The cloud's recovery resumes whatever sessions are in its DB — so it must match the
laptop's live state at handoff, or the cloud manages stale/wrong sessions.
1. Cloud: `aws ecs update-service ... --desired-count 0` (stop the app so the DB isn't open).
2. Fresh snapshot on the laptop (same as the original seed): `VACUUM INTO` the live
   `data/db/kanida_universe.db` + `kanida_quant.db`, **plus copy in
   `data/config/rupeezy_instruments.json`** → package → upload to S3 → run the
   one-off ECS seed task (see `PHASE2_STEP8_SEED_RUNBOOK.md`) to overwrite `/data/db`.
   (The instrument master lands at `/data/db/rupeezy_instruments.json`, which the
   task env `RUPEEZY_INSTRUMENT_MASTER` points at — required to place a Rupeezy order.)
3. Cloud: `--desired-count 1` — app boots on the CURRENT state.

## Step 3 — Tokens into the cloud (market open)
- **Zerodha:** laptop mints today's token → auto-push (Step 1) OR manual
  `python scripts/push_kite_token.py`.
- **Rupeezy:** laptop mints via `vortex_auto_auth` (auto-push hook) OR manual
  `python scripts/push_rupeezy_token.py`.
Verify the cloud received both.

## Step 3.5 — Rupeezy prerequisites (do BEFORE trusting Rupeezy execution)
- **Vortex IP allowlist — ✅ DONE (2026-07-19):** cloud NAT EIP **`52.66.50.160`**
  added as the Vortex **Secondary IP** (Active); laptop `174.61.231.194` stays Primary,
  so both can trade → seamless cutover + instant rollback. IP is a stable EIP
  (fine for Vortex's 15-day lock). No `BROKER_PROXY_URL` needed on the cloud — its
  own NAT egress IP is now allowlisted directly.
- **Instrument master:** shipped in Step 2; confirm `/data/db/rupeezy_instruments.json`
  exists in the cloud.
- **broker_account row:** confirm the rupeezy account (`dddbc4f83edc4051aa77c39cb4b4a7e5`)
  is in the seeded cloud DB (so the token decrypts + app_id/x-api-key are readable).

## Step 4 — DNS flip (Cloudflare) — serving + control plane → cloud
Cloudflare → `kanida.ai` → DNS → the `api.kanida.ai` record (currently Tunnel
`kanida-api` → laptop): change to **CNAME → `kanida-prod-alb-243178261.ap-south-1.elb.amazonaws.com`**,
**DNS only (grey)**. (Disable the cloudflared public hostname for `api.kanida.ai`.)
Verify: `curl https://api.kanida.ai/` → 200 from the cloud; users stay logged in (same JWT secret).

## Step 5 — Ownership handoff (single trade owner)
1. **Laptop:** `FALCON_AUTOTRADE_ENABLED=false` + square-off/freeze its in-memory
   sessions so it stops acting. Mining/EOD stays ON.
2. **Cloud:** set task-def env `FALCON_AUTOTRADE_ENABLED=true`,
   `FALCON_AUTOTRADE_EXECUTION_MODE=live` (terraform) + apply. Cloud resumes the
   (now-current) sessions with the valid token, orders via the registered proxy.
   **Only the cloud trades now.**

## Step 6 — 1-share LIVE proof (before trusting campaigns)
Through the cloud, place **1 share** on the operator's account → confirm the fill at
the broker → square off. This converts "should work" into "proven." Only after this
green light do the Rupeezy remainder + HFCL management run on the cloud.

## Step 7 — Arm the health layer (sysagents) — observe-only, then paging
The System Engineering Agent Hierarchy (Phase 1) is IN the image (Step 0) but
DEFAULT-OFF. Arm it AFTER Step 6 is green (it only watches + pages — never touches
trades):
1. **Prereq — enable notifications** (VAPID subscriber, one click in the panel), else
   its pages go nowhere (the same gap that would swallow the HFCL page).
2. Set cloud task-def env `SYSAGENTS_ENABLED=true` + `SYSAGENTS_PAGING=off`
   (observe-only) and apply — it watches + logs + populates `GET /api/health/system`
   WITHOUT paging during the cutover-settling window.
3. Verify `GET /api/health/system` (operator token) shows a sane correlated view of
   the 9 subsystems.
4. After the first clean live session (~1 day), flip `SYSAGENTS_PAGING=on` so it pages
   real incidents. `SYSAGENTS_KILL_SWITCH=true` hard-stops the whole layer anytime.

## Rollback (any step, ~1 min)
- Revert `api.kanida.ai` DNS → the Tunnel (laptop).
- Laptop `FALCON_AUTOTRADE_ENABLED=true`, cloud `false`.
- Laptop is primary again; cloud idles. No data lost (state in S3 + EFS).

## After cutover (separate build)
- **CI/CD pipeline (HIGH priority — do first): BUILT (dormant), arming = 3 steps.**
  Pipeline committed at `.github/workflows/deploy-cloud.yml` (git push → zip source →
  existing CodeBuild builds `kanida-backend:phase2` → ECS force-new-deployment). It's
  DORMANT (manual-trigger-only, no AWS role yet) so it can't fire during the cutover.
  Arm it AFTER the 1-share proof per **`deploy/CICD_SETUP.md`**: (A) `terraform apply`
  a GitHub-OIDC deploy role, (B) set 3 GitHub repo secrets, (C) manual test run then
  uncomment the `push:` trigger. Until armed, the manual zip→CodeBuild→redeploy loop
  stays the way to ship.
- **WS4:** per-user on-demand Elastic IP + self-serve broker-connect (app→AWS
  boto3 + IAM; EIP quota default 5). Build + test against the now-live cloud.
- Rupeezy/Vortex cloud auth: token-sync BUILT (`feat/cloud-rupeezy-token-sync` @
  `1f83d6e`); remaining = ship instrument master (Step 2) + Vortex IP allowlist (Step 3.5).
- **Sysagents Phase 2** (bounded ops auto-fixes, §9 scope decision) + **Phase 3**
  (execution-quality RCA depth) — once Phase 1 has watched a few clean sessions.
- **Corp-action ROOT fix:** capture raw `get_holdings()` + `get_positions_net()`
  JSON for a storm symbol (e.g. HFCL) → make the `broker_held` 2× T+1 de-dup
  provably safe for the reconciler's auto-close path (the shipped dedup only
  silenced the *alert* noise; the underlying 2× mis-read remains).
