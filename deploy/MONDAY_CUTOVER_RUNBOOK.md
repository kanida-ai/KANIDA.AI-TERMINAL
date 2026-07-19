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
   `data/db/kanida_universe.db` + `kanida_quant.db` → package → upload to S3 →
   run the one-off ECS seed task (see `PHASE2_STEP8_SEED_RUNBOOK.md`) to overwrite `/data/db`.
3. Cloud: `--desired-count 1` — app boots on the CURRENT state.

## Step 3 — Token into the cloud (market open)
Laptop mints today's token → auto-push (Step 1) OR manual `python scripts/push_kite_token.py`.
Verify cloud: `GET /api/.../` health shows a live Kite path, or check `get_access_token()`.

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

## Rollback (any step, ~1 min)
- Revert `api.kanida.ai` DNS → the Tunnel (laptop).
- Laptop `FALCON_AUTOTRADE_ENABLED=true`, cloud `false`.
- Laptop is primary again; cloud idles. No data lost (state in S3 + EFS).

## After cutover (separate build)
- **WS4:** per-user on-demand Elastic IP + self-serve broker-connect (app→AWS
  boto3 + IAM; EIP quota default 5). Build + test against the now-live cloud.
- Rupeezy/Vortex cloud auth (Kite token-sync covers Zerodha only).
