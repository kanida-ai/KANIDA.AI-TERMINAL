# module: egress — per-user static egress IPs (the IP-at-scale solution)

**Purpose.** For each user's broker account, provision a dedicated static
**Elastic IP** + a tiny **tinyproxy** box. The app forwards that user's broker
orders to their proxy, which egresses from their EIP — the IP the user registers
on their broker app + SEBI profile.

**Why this shape (grounded in code).** `backend/autotrade/broker/egress.py`
already implements the app side: a broker account is assigned a proxy URL from
`BROKER_EGRESS_POOL`, encrypted at rest, fail-open to DIRECT. It is broker-
agnostic (no `if broker == ...`). This module is the infra that fills that pool.

**The requirement addressed.** SEBI/Zerodha bind ONE static IP to ONE broker
account. Sharing one IP across users is Model A's compromise; a dedicated IP per
user is the clean, per-account-correct posture and what this module delivers.

**Adding a user = ONE line.** Append a stable handle to `var.egress_users`.
`for_each` creates one more EIP + one more nano box. No app code change, no
restart. `terraform output egress_ips_by_user` gives the new IP to register.

**Operator glue (manual, by design — mirrors the Lightsail runbook).** After
apply, the operator assembles the proxy URLs (`http://user:pass@<eip>:8888`) into
the app's `BROKER_EGRESS_POOL` secret. The app's onboarding then assigns a free
pool URL to each account (`assign_from_pool`), encrypted. The URL carries creds;
Terraform outputs only the bare IP, never the credentialed URL.

## Phase-0 status & hardening flags (must-fix before real use)

Authored, unverified. The `user_data` tinyproxy config is a MINIMAL bring-up:
- **Add Basic-auth** on tinyproxy (the URL scheme `user:pass@host` implies creds);
  today it only IP-allows the VPC CIDR.
- **Tighten the Allow** to the app/private subnets, not the whole VPC.
- **t4g.nano is ARM (Graviton)** → uses an AL2023 arm64 AMI. Match the instance
  family to the AMI arch if you change `proxy_instance_type`.
- Consider an Auto-Recovery alarm per box (single-box-per-user = that user's
  order egress is down if the box dies).
- `egress_users` is empty in Phase 0 → this module creates nothing until the
  operator opts a user in.
