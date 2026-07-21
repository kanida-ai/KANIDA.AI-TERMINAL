# Cutover FINISH checklist — execute when the basket is FLAT

**Trigger:** you've manually exited today's basket → **no open positions**, campaigns
paused. Now the cloud can take over an *idle* book with nothing to collide over.
Everything below is reversible (see Rollback). Region is `ap-south-1` throughout.

**State going in (verified 2026-07-21):** cloud image rebuilt from `bf58b80` (both
OMS fixes proven), DB clean, deploy is stop-then-start (corruption-safe), gate
**OFF**. Laptop gate is **ON** and is the live owner until Step 1.

---

## Step 0 — Refresh CloudShell Terraform (REQUIRED — avoids reverting fixes)
Your `~/terraform` predates two committed changes (`e341141` stop-then-start deploy,
+ the gate variables). Applying the old tree would **revert the corruption fix**.
Re-upload the fresh bundle first:
1. Desktop → CloudShell **Actions → Upload file** → `kanida-terraform.zip` (I rebuild it).
2. In CloudShell:
   ```bash
   cd ~ && rm -rf terraform && mkdir terraform && cd terraform && unzip -q ~/kanida-terraform.zip
   export AWS_DEFAULT_REGION=ap-south-1
   terraform init -input=false >/dev/null && echo "tf ready"
   ```
3. Make the gate sticky (so no later apply silently reverts it) — create `terraform.tfvars`:
   ```bash
   cat > ~/terraform/terraform.tfvars <<'TFV'
   # cutover: cloud is the live trade owner
   autotrade_enabled        = false   # flipped to true in Step 3
   autotrade_execution_mode = "marketable_limit"
   sysagents_enabled        = false   # flipped to true in Step 6
   sysagents_paging         = "off"
   TFV
   echo "tfvars written"
   ```
   *(Confirm `terraform plan` shows only the deploy-config + env changes, no destroys.)*

## Step 1 — Laptop: stop being the trade owner (single owner)
On the laptop, set `config/.env` → `FALCON_AUTOTRADE_ENABLED=false`, then **restart the
backend** (env is read at boot). Mining/EOD/serving stay on. From here the laptop
executes nothing. *(Claude can do this edit + restart on request.)*

## Step 2 — Re-seed the cloud to your FLAT state
1. **Laptop** — build the pack (read-only; safe): 
   ```
   C:\Users\SPS\anaconda3\python.exe deploy\reseed_pack.py
   ```
   → `C:\Users\SPS\Desktop\kanida-seed.tar.gz` + prints SHA256SUMS.
2. Upload it to S3 (S3 console → bucket `kanida-prod-artifacts-389642461326` → `seed/` →
   Upload, key `seed/kanida-seed.tar.gz`), **or** via CloudShell if it fits.
3. CloudShell — stop the app, seed, leave at 0 (Step 3 brings it back up):
   ```bash
   aws ecs update-service --cluster kanida-prod-cluster --service kanida-prod-svc --desired-count 0
   # wait runningCount 0:
   aws ecs describe-services --cluster kanida-prod-cluster --services kanida-prod-svc --query 'services[0].runningCount' --output text
   bash ~/reseed_efs.sh    # (clears wal/shm, untars, sha256 -c → both DBs must say OK)
   ```

## Step 3 — Cloud: become the live trade owner (gate ON)
```bash
cd ~/terraform
sed -i 's/autotrade_enabled        = false/autotrade_enabled        = true/' terraform.tfvars
terraform apply -auto-approve     # new task-def (gate live) + desired_count back to 1
aws ecs wait services-stable --cluster kanida-prod-cluster --services kanida-prod-svc && echo STABLE
```
The single task boots **live** on the flat DB (placeholder token — nothing trades yet).

## Step 4 — DNS flip (Cloudflare) → cloud is the front door
Cloudflare → `kanida.ai` → DNS → the `api.kanida.ai` record: change to
**CNAME → `kanida-prod-alb-243178261.ap-south-1.elb.amazonaws.com`**, **DNS-only (grey)**;
disable the cloudflared public hostname for `api.kanida.ai`. Verify:
```bash
curl -s -o /dev/null -w '%{http_code}\n' https://api.kanida.ai/      # 200 from cloud
```
Users stay logged in (same JWT secret).

## Step 5 — Push today's tokens to the cloud (market open)
On the **laptop** (secret is in CloudShell `~/publish_secret.txt` — copy its value):
```
set FALCON_PUBLISH_SECRET=<paste from publish_secret.txt>
C:\Users\SPS\anaconda3\python.exe scripts\push_kite_token.py     --cloud-url https://api.kanida.ai
C:\Users\SPS\anaconda3\python.exe scripts\push_rupeezy_token.py  --cloud-url https://api.kanida.ai
```
Each prints success (never the token value). Verify the cloud accepted both.

## Step 6 — 1-SHARE LIVE PROOF (before trusting anything bigger)
Through the cloud (api.kanida.ai AutoTrade panel), place **1 share** on your account —
**Zerodha AND Rupeezy** — confirm the fill at the broker, then square off. This turns
"should work" into "proven." Only after BOTH fills are green do you re-enable campaigns.

## Step 7 — Arm the health layer (observe-only)
```bash
cd ~/terraform
sed -i 's/sysagents_enabled        = false/sysagents_enabled        = true/' terraform.tfvars
terraform apply -auto-approve
curl -s https://api.kanida.ai/api/health/system -H "Authorization: Bearer <operator-token>" | head
```
Enable the VAPID notification subscriber first (one click in the panel) so pages land.
Leave `sysagents_paging="off"` until after one clean session, then flip it to `"on"`.

---

## Rollback (any step, ~1-2 min)
- **Execution:** laptop `FALCON_AUTOTRADE_ENABLED=true` + restart; cloud
  `sed -i 's/= true/= false/' terraform.tfvars && terraform apply -auto-approve`.
- **DNS:** revert `api.kanida.ai` → the cloudflared tunnel (laptop).
- No data lost — state is in S3 + EFS.

## Order rationale
Laptop-off BEFORE cloud-on = never two owners. Re-seed BEFORE gate-on = cloud wakes
idle on a true book. DNS BEFORE token push = the push reaches the cloud, not the
laptop. Proof BEFORE campaigns/sysagents = prove the live path with 1 share first.
