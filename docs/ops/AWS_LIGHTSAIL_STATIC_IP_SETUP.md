# Static egress IP for a broker account — AWS Lightsail (Mumbai) setup runbook

**Supersedes `ORACLE_STATIC_IP_SETUP.md`.** Oracle Cloud Free Tier ties your permanent
home region to signup and runs a strict card/geo fraud check — a **US card + US billing
address + India-West home region** is one of the most common rejection patterns, and it
is not arguable. AWS treats region as a plain dropdown, unrelated to billing country.

**Cost:** ~**$3.50/month** (not free, but a 10-minute signup instead of a multi-day fight).

> ### ⚠️ Read this first — the "one IP covers everyone" model is WRONG
> `ORACLE_STATIC_IP_SETUP.md` Step 9 said each new user registers *the same* IP. That is
> **false** and it is why onboarding a second Zerodha user failed with
> *"The IP address(es) you are trying to add are already linked to another account."*
>
> Under SEBI's retail-algo rules Zerodha enforces **one IP → one Kite Connect account**.
> So **every power user needs their OWN distinct static IP.** This runbook provisions
> **one box per user**. (Cross-broker is fine — the same IP may be used on a *different*
> broker; the collision is only between two accounts at the *same* broker.)

---

## STEP 1 — AWS account (~10 min) — **YOU must do this**
Account creation + card entry is yours; it cannot be delegated.
1. https://aws.amazon.com → **Create an AWS Account**.
2. US card + US billing address is fine. **Region is NOT tied to billing country** — this
   is the whole reason we left Oracle.
3. Verify email + phone. Choose the **Basic (free)** support plan.

→ *Tell me when you're in the console.*

## STEP 2 — Create the Lightsail instance (~3 min)
1. https://lightsail.aws.amazon.com → **Create instance**.
2. **Instance location → change to `Mumbai, ap-south-1`.** ← do this FIRST; it is closest
   to NSE and keeps the added hop small.
3. Platform **Linux/Unix** → Blueprint **OS Only → Ubuntu 22.04 LTS**.
4. Plan: the **$3.50/mo** tier (512 MB / 2 TB transfer) — ample for a proxy.
5. Name it for the user it belongs to, e.g. `kanida-proxy-user2`.
6. **Create instance** → wait for **Running**.

## STEP 3 — Attach a STATIC IP (~1 min) — *this is the IP that matters*
1. Lightsail → **Networking** tab → **Create static IP**.
2. Region **Mumbai**, attach to `kanida-proxy-user2`, name `kanida-static-user2`.
3. **Create**. This IP is now permanent and **free while attached** to a running instance.

→ *Send me this IP. Call it `<LS_IP>`.*

## STEP 4 — Open the proxy port (~1 min)
Instance → **Networking** tab → **IPv4 Firewall** → **Add rule**:
- Application **Custom** · Protocol **TCP** · Port **8888** → **Create**.

> Unlike Oracle, Lightsail's Ubuntu does **not** block non-22 ports at the OS level — the
> Lightsail firewall is the only gate. **No `iptables` step is needed** (that Oracle step
> does not apply here).

## STEP 5 — Install the proxy (~3 min)
SSH in via the console (**Connect using SSH** button — no key handling needed).

> ### ⚠️ TWO TRAPS — both cost ~30 min on the first run (2026-07-16). Read before pasting.
> **1. NEVER Ctrl+V a MULTI-LINE block into the Lightsail browser terminal.** It leaks
> bracketed-paste escape markers into the command line: the first line becomes
> `^[[200~sudo apt update …` → **`sudo: command not found`**. The install silently never
> runs, and every later command fails against a package that isn't there (the tell:
> `sed: can't read /etc/tinyproxy/tinyproxy.conf: No such file or directory`).
> **→ Use the "Paste into terminal" button (bottom-right), ONE single-line command at a time.**
>
> **2. `tinyproxy` is in Ubuntu's `universe` repo, which the Lightsail image does NOT
> enable.** A plain `apt install tinyproxy` fails with **`E: Unable to locate package
> tinyproxy`**. You must add `universe` first. (Oracle's image had it on by default —
> this is Lightsail-specific.)

**Command 1** — install (paste as ONE line, via the button):
```bash
sudo add-apt-repository -y universe && sudo apt update && sudo apt install -y tinyproxy
```
Wait for `Setting up tinyproxy (1.11.0-1) ...` and **no `E:` line**.

**Command 2** — configure + restart + prove it's listening (ONE line; replace `<STRONG_PASSWORD>`):
```bash
sudo sed -i 's/^Allow 127.0.0.1/Allow 0.0.0.0\/0/; s/^#\?BasicAuth.*/BasicAuth kanida <STRONG_PASSWORD>/; s/^Listen /#Listen /' /etc/tinyproxy/tinyproxy.conf && sudo systemctl restart tinyproxy && sleep 1 && sudo ss -tlnp | grep 8888
```
**Must print `LISTEN 0.0.0.0:8888`.** If it prints `127.0.0.1:8888`, a `Listen` directive is
still active (the `s/^Listen /#Listen /` above is what prevents this — `Allow` alone is NOT
enough; `Allow` is the ACL, `Listen` is the bind interface). If it prints nothing, tinyproxy
failed to start — check `sudo systemctl status tinyproxy`.

Tinyproxy supports HTTPS CONNECT (443) by default, so the broker APIs work through it.

## STEP 6 — Prove the static egress works (~1 min)
From **your laptop**:
```bash
curl -x http://kanida:<STRONG_PASSWORD>@<LS_IP>:8888 https://api.ipify.org
```
→ must print **`<LS_IP>`**. If it prints anything else (or hangs), the egress is not
working — stop here and fix it; nothing downstream will work.

**Then confirm HTTPS CONNECT to a real broker** (the actual use case):
```bash
curl -o /dev/null -w "%{http_code}\n" -x http://kanida:<STRONG_PASSWORD>@<LS_IP>:8888 https://api.kite.trade/
```
→ **200** = tunneling works (measured 1.4s via Mumbai on 2026-07-16).

> **A 403 from a broker here is EXPECTED and is PROOF the proxy works** — the new IP is not
> yet on that broker's allowlist, so `IP_NOT_ALLOWED` means the request genuinely egressed
> from `<LS_IP>`. It clears once STEP 8 registers the IP on that account.

### Diagnosing a failure (first-run experience)
A **timeout** (rather than a fast refusal) on port 8888 means a firewall drop OR nothing
listening. Differentiate from the laptop before touching the AWS console:
```bash
python -c "import socket;s=socket.socket();s.settimeout(8);
try: s.connect(('<LS_IP>',8888)); print('OPEN')
except socket.timeout: print('FILTERED - firewall or not listening')
except ConnectionRefusedError: print('REFUSED - firewall OK, nothing listening')"
```
Compare against port **22** (known-open) as a control: if 22 is OPEN and 8888 is FILTERED,
the box is reachable and the problem is the rule **or** tinyproxy isn't actually running —
check STEP 5's `ss -tlnp` output before blaming AWS. Also confirm `sudo ufw status` is
`inactive` (it is by default on Lightsail; the AWS firewall is the only gate).

→ *Send me `<LS_IP>` + `<STRONG_PASSWORD>` and I do Step 7.*

## STEP 7 — Wire the backend PER-ACCOUNT (Claude does this)
The per-account egress routing is **already built** (branch `feat/per-account-egress-proxy`,
sha `8cb43d7`): `_new_kite(api_key, proxy_url=...)` + `resolve_account_proxy(broker_account_id)`.
It is **default-OFF** — accounts absent from the map egress direct, so the operator's own
account keeps using the home IP `174.61.231.194` and is unaffected.

In `config/.env` (compact JSON, one line):
```
BROKER_PROXY_MAP={"<broker_account_id>":"http://kanida:<PASSWORD>@<LS_IP>:8888"}
```
Multiple users: `{"acctA":"http://kanida:<pw>@<IP_A>:8888","acctB":"http://kanida:<pw>@<IP_B>:8888"}`

Then restart :8001 and **warm the cache** (a manual restart wipes the ~150s persona cache).

## STEP 8 — The USER registers the IP on THEIR Kite account (~2 min) — **they must do this**
1. https://developers.kite.trade → their app → **Allowed IPs** → add `<LS_IP>` → Save.
2. **Kite profile page → register the static IP** (the SEBI requirement) → `<LS_IP>`.
3. No collision: it's a fresh IP on their own account.

---

### Per-user cost model
One box + one static IP **per power user on the same broker**: **~$3.50/user/month.**
At 10 users ≈ $35/mo — trivial against the alternative (users cannot trade at all).

### Why not a cheaper shared proxy?
You cannot share. SEBI/Zerodha's one-IP-one-account rule makes a shared egress IP a hard
block for user #2 — this is not a cost optimization we get to make.

### Notes
- **Latency:** adds one hop (laptop → Mumbai → broker). Mumbai keeps the proxy→broker leg
  tiny. The latency-optimal end state is the backend itself running in Mumbai (the cloud
  migration) — then there is no laptop hop at all.
- **Security:** the proxy is internet-reachable but BasicAuth-gated. The credentials only
  let someone egress from that IP — they cannot trade without the vaulted api_key + token.
  Use a long random password; rotate if leaked.
- **Static IP billing gotcha:** free **while attached to a running instance**. AWS charges
  for an *unattached* static IP — if you stop/delete the instance, release the IP too.
- **Bonus — this unblocks the cloud migration.** The migration (see
  `docs/design/` Document 1 & 2) is currently blocked on having *any* cloud account. The
  same AWS account covers both: Lightsail for per-user egress now, and EC2/ECS/RDS for the
  Phase-0 migration targets (managed Postgres, the bus, object storage) later.

### Alternatives (if AWS is ever a problem)
| Provider | India region | ~Cost | Static IP |
|---|---|---|---|
| **AWS Lightsail** ← chosen | Mumbai (ap-south-1) | $3.50/mo | free while attached |
| DigitalOcean | Bangalore (BLR1) | ~$4/mo | Reserved IP |
| Vultr | Mumbai | ~$2.50–5/mo | included |

All three accept a US card and let you pick the region freely. **Do not** use a US-egress
proxy service (QuotaGuard/Fixie) — the IP must be in India for latency, and the point is a
distinct IP per user's broker account.
