# Kanida.ai Production Deploy — Cloudflare Tunnel + Vercel

End state: **`https://kanida.ai/power`** serves the real power-user portal,
globally accessible, no laptop-IP exposed. Frontend on Vercel; backend on
this laptop reachable via Cloudflare tunnel at `https://api.kanida.ai`.

**Time budget**: 60 minutes total (15 min hands-on + DNS propagation wait).

---

## Order of operations

```
   [ 1. Cloudflare account ]
            │
            ▼
   [ 2. Add kanida.ai → get 2 NS records ]
            │
            ▼
   [ 3. Domain registrar: switch nameservers ]   ⟵ DNS prop here (15 min – 24 h)
            │
            ▼
   [ 4. cloudflared tunnel login (on this laptop) ]
            │
            ▼
   [ 5. cloudflared tunnel create kanida-api ]
            │
            ▼
   [ 6. Edit config.yml with tunnel UUID ]
            │
            ▼
   [ 7. cloudflared tunnel route dns kanida-api api.kanida.ai ]
            │
            ▼
   [ 8. Install + start Windows service ]
            │
            ▼
   [ 9. Vercel: set NEXT_PUBLIC_API_URL + redeploy ]
            │
            ▼
   [10. Verify end-to-end with deploy/verify-deploy.sh ]
```

---

## Step 1 — Create / sign in to Cloudflare account

1. Go to `https://dash.cloudflare.com`.
2. Sign up or sign in (free plan is fine).

## Step 2 — Add kanida.ai to Cloudflare

1. From the Cloudflare dashboard: **Websites → Add a site**.
2. Enter `kanida.ai`. Pick the **Free** plan when prompted.
3. Cloudflare scans your existing Vercel DNS records and shows them. **Important**: confirm Cloudflare has copied the A records that point at Vercel (these route kanida.ai to your Vercel site). If they're not there, manually copy them from your Vercel DNS dashboard.
4. Cloudflare shows you 2 nameservers, e.g.:
   ```
   amber.ns.cloudflare.com
   bjorn.ns.cloudflare.com
   ```
   **Copy these — you'll paste them at your registrar.**

## Step 3 — Switch nameservers at your domain registrar

1. Log in wherever you bought `kanida.ai` (GoDaddy / Namecheap / Google Domains / Cloudflare-registrar / etc.).
2. Find DNS / nameserver settings for kanida.ai.
3. Replace the current nameservers (`ns1.vercel-dns.com`, `ns2.vercel-dns.com`) with the 2 Cloudflare nameservers from Step 2.
4. Save.
5. Back in Cloudflare → **DNS** tab → it will say "Pending nameserver update" → after 15 min – 24 h it goes green ✅.

While you wait, you can do Steps 4 – 7. Step 7 (DNS route) needs the propagation done; Step 8 onwards needs Step 7.

---

## Step 4 — Authorize cloudflared on this laptop

Open a new terminal (Git Bash or PowerShell — both work) on this laptop:

```bash
"C:/Users/SPS/bin/cloudflared.exe" tunnel login
```

This:
- Opens your default browser at `https://dash.cloudflare.com/argotunnel?...`
- You click **Authorize** and pick `kanida.ai`
- A credentials file lands at `C:\Users\SPS\.cloudflared\cert.pem`

(If the laptop has no browser, copy the URL from the terminal and open it on a different machine, then download the cert.pem.)

## Step 5 — Create the named tunnel

```bash
"C:/Users/SPS/bin/cloudflared.exe" tunnel create kanida-api
```

Output looks like:
```
Created tunnel kanida-api with id 4f3c8d12-9a7b-4e6f-8d12-3a4b5c6d7e8f
Credentials written to C:\Users\SPS\.cloudflared\4f3c8d12-...json
```

**Copy the UUID** — you'll paste it into `config.yml` in the next step.

## Step 6 — Edit config.yml with the tunnel UUID

Open `deploy/cloudflared/config.yml.template` in this repo, save it as
`config.yml` in the SAME directory (Cloudflare looks for `config.yml` not
`config.yml.template`), and replace `<TUNNEL_UUID_HERE>` with the UUID
from Step 5 (two places).

Then copy that `config.yml` into `C:\Users\SPS\.cloudflared\`:

```powershell
Copy-Item "C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\deploy\cloudflared\config.yml" "C:\Users\SPS\.cloudflared\config.yml"
```

## Step 7 — Wire DNS record

Once nameserver propagation is done (Step 3 went ✅ in Cloudflare):

```bash
"C:/Users/SPS/bin/cloudflared.exe" tunnel route dns kanida-api api.kanida.ai
```

This creates a CNAME record `api.kanida.ai → <tunnel>.cfargotunnel.com`
in Cloudflare DNS. You can verify in the Cloudflare DNS dashboard.

## Step 8 — Install + start cloudflared as a Windows service

Run the prepared PowerShell script (this script kills any quick tunnel running and registers cloudflared to auto-start on every reboot):

```powershell
# Run as Administrator
powershell -ExecutionPolicy Bypass -File "C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\deploy\cloudflared\install-tunnel.ps1"
```

Verify the service:

```powershell
Get-Service Cloudflared | Format-List Name, Status, StartType
# Expected:
#   Status   : Running
#   StartType: Automatic
```

Smoke test:

```bash
curl -i https://api.kanida.ai/api/power/personas
# Expected: HTTP/2 200, JSON list of 6 personas
```

---

## Step 9 — Vercel: env var + redeploy

1. Vercel dashboard → your kanida.ai project → **Settings → Environment Variables**.
2. Add:
   - `NEXT_PUBLIC_API_URL` = `https://api.kanida.ai`
   - Apply to: **Production** (and Preview if you want)
3. **Settings → Deployment Protection**: turn OFF the HTTP Basic Auth that's currently in front of kanida.ai.
4. Trigger a redeploy: **Deployments tab → latest deploy → ⋯ → Redeploy** OR push any commit to main.

After the deploy finishes (~2 min):
```bash
curl -I https://kanida.ai/power
# Expected: HTTP/2 200, no 401 Basic Auth challenge
```

## Step 10 — Verify end-to-end

```bash
bash "C:/Users/SPS/Desktop/Kanida.ai Terminal Quant Intelligence Engine/deploy/verify-deploy.sh"
```

This hits all the critical surfaces and confirms:
- Backend tunnel reachable
- Frontend reachable
- All 6 personas listed
- Sign-in flow loads
- /power/portfolios renders
- Random-replay button works

---

## What stays untouched

- The auto-auth bot keeps firing on its 4-cycle schedule (06:30/07:30/08:30/09:00 IST). Lives in the same backend, just reachable via the tunnel now instead of localhost.
- All your existing DBs (`data/db/kanida_quant.db` + `data/db/kanida_universe.db`) stay on this laptop. Cloudflare tunnel forwards requests; data stays local.
- The scheduler that re-runs the engine nightly (`backend/main.py` lifespan) keeps running locally.

## Rollback

If anything breaks:
1. **Quick rollback** (URL goes back to laptop-only): Stop the cloudflared service: `Stop-Service Cloudflared`. Frontend on Vercel still tries to call `api.kanida.ai` — it'll 503. Power users will see the error banner I built earlier.
2. **Full rollback** (restore Vercel-DNS placeholder): in Vercel dashboard, re-enable the Basic-Auth deployment protection. The previous placeholder is restored.
3. **DNS rollback** (revert from Cloudflare back to Vercel DNS): at your domain registrar, change nameservers back to `ns1.vercel-dns.com` / `ns2.vercel-dns.com`. 15-min – 24-h propagation. Note this leaves Cloudflare configured but bypassed.

---

## Operating notes (post-go-live)

- **Laptop must stay on**. Sleep → backend dark → users see error banner. For 24/7, eventually move backend to Railway (see `DEPLOY_FALCON.md`).
- **Power outage / restart**: Windows service auto-restarts cloudflared. Backend (uvicorn) does NOT auto-restart by default — set up a separate Windows service for it if you want true 24/7 (~10 min, ping me).
- **Cloudflared logs**: `Get-Content "C:\Users\SPS\.cloudflared\cloudflared.log" -Wait`
- **Tunnel health**: `cloudflared tunnel info kanida-api`
- **Daily auth audit**: query `falcon_auth_log` (see `config/AUTH_BOT_SETUP.md`)
