# Free static egress IP for Kite (Oracle Cloud Always-Free) — setup runbook

Goal: give the laptop server ONE permanent static public IP, free, so SEBI-compliant
live orders work and never break on ISP IP rotation. Model A: each user registers
THIS one IP on their own broker app.

The `BROKER_PROXY_URL` hook is already wired in the backend (default-off). Once the
proxy is up, it's one env line + a Kite registration.

---

## STEP 1 — Oracle Cloud account (~10 min, free)
1. Go to https://www.oracle.com/cloud/free/ → "Start for free".
2. Sign up (email, phone, a card for IDENTITY verification only — **Always-Free
   resources never charge**; you can also set the account to never upgrade).
3. **Home region — pick carefully, it's permanent:** choose **India West (Mumbai)**
   (closest to Zerodha → lowest added latency). India South (Hyderabad) is the fallback.

## STEP 2 — Create the Always-Free VM (~5 min)
1. Console → ☰ → Compute → Instances → **Create instance**.
2. Name: `kanida-proxy`.
3. Image: **Canonical Ubuntu 22.04**.
4. Shape: **Change shape → Always Free-eligible → `VM.Standard.E2.1.Micro`** (x86, free).
5. Networking: create/keep a VCN, **Assign a public IPv4 = Yes**.
6. **SSH keys:** "Generate a key pair for me" → **download the private key** (or paste
   your own public key). Keep the private key safe — it's your VM login.
7. **Create**. Wait until it's RUNNING; note the **Public IP**.

## STEP 3 — Make the IP STATIC (reserved) (~2 min)
1. Open the instance → Resources → **Attached VNICs** → click the VNIC.
2. **IPv4 Addresses** → edit the public IP → change **Ephemeral → Reserved**
   (create a reserved public IP, e.g. `kanida-static-ip`).
3. This IP is now permanent. Call it `<ORACLE_IP>` below.

## STEP 4 — Open the proxy port (~2 min)
1. Console → Networking → Virtual Cloud Networks → your VCN → **Security Lists** →
   Default Security List → **Add Ingress Rule**:
   - Source CIDR: `0.0.0.0/0`
   - IP Protocol: **TCP**, Destination Port Range: **8888**
   - Save.

## STEP 5 — Install the proxy on the VM (~5 min)
SSH in:  `ssh -i <your-private-key> ubuntu@<ORACLE_IP>`
Then paste (replace `<STRONG_PASSWORD>` with a long random password):
```bash
sudo apt update && sudo apt install -y tinyproxy
# auth + allow remote (auth still required):
sudo sed -i 's/^Allow 127.0.0.1/Allow 0.0.0.0\/0/' /etc/tinyproxy/tinyproxy.conf
sudo sed -i 's/^#\?BasicAuth.*/BasicAuth kanida <STRONG_PASSWORD>/' /etc/tinyproxy/tinyproxy.conf
# Oracle Ubuntu firewalls all non-22 ports by default — open 8888 + persist:
sudo iptables -I INPUT -p tcp --dport 8888 -j ACCEPT
sudo apt install -y iptables-persistent && sudo netfilter-persistent save
sudo systemctl enable --now tinyproxy && sudo systemctl restart tinyproxy
```
Tinyproxy supports HTTPS CONNECT (port 443) by default, so Kite's API works through it.

## STEP 6 — Test the proxy from your laptop (~1 min)
```bash
curl -x http://kanida:<STRONG_PASSWORD>@<ORACLE_IP>:8888 https://api.ipify.org
```
→ must print **`<ORACLE_IP>`**. If it does, the static egress works.

## STEP 7 — Point the backend at it (one line)
In `config/.env`:
```
BROKER_PROXY_URL=http://kanida:<STRONG_PASSWORD>@<ORACLE_IP>:8888
```
Restart :8001, then verify:
```
GET /api/falcon/egress-ip   →   proxy_ip == <ORACLE_IP>
```

## STEP 8 — Register the IP on Kite (your account)
1. https://developers.kite.trade → your app → **Allowed IPs → add `<ORACLE_IP>`** → Save.
2. **Profile page → register your static IP** (the SEBI requirement) → `<ORACLE_IP>`.
3. Re-run `/api/falcon/preflight` → `kite_ip_allowed` should turn **GREEN**.

## STEP 9 — Future users (Model A)
Each new user adds **the same `<ORACLE_IP>`** to *their own* Kite app's Allowed IPs +
profile registration. One IP covers everyone, every broker.

---

### Notes
- **Latency:** the proxy adds one hop (laptop → Oracle Mumbai → Kite). Putting the VM
  in Mumbai keeps the Oracle→Kite leg tiny. The latency-optimal end state is running
  the whole backend on this Mumbai VM (Phase-2 cloud move) — then there's no laptop hop.
- **Security:** the proxy is internet-reachable but BasicAuth-gated; the credentials
  only let someone egress from that IP — they still can't trade without your Kite
  api_key + token. Use a long random password. Rotate if leaked.
- **Cost:** ₹0 ongoing on Always-Free (1 micro VM + 1 reserved IP are within the free
  limits, permanently — not a trial).
