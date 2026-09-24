# KANIDA cloud architecture — audit of the old environment, and the target

Prepared 23 Sep 2026. Audited: `C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine`
(137k lines of Python across 447 backend files, plus a Next.js frontend, Terraform, Railway and Cloudflare
configs). Everything below was read on disk, not assumed. What I could **not** verify is stated as such.

---

## 1. The finding that changes the question

**There is almost no cloud to clean up.** `api.kanida.ai` is a Cloudflare *tunnel* pointing at
`localhost:8001` — this laptop (`deploy/cloudflared/config.yml`). The Next.js frontend is hosted; the backend,
the databases and the jobs are all here, on the desk.

- **The AWS stack was never built.** `deploy/terraform/main.tf` carries its own header: *"Cloud-Migration
  PHASE 0 — AUTHORED, UNVERIFIED (Terraform absent on this machine; nothing has been init/plan/applied)."*
  33 resources are declared — VPC, NAT gateway, Multi-AZ RDS, ElastiCache, EFS, ALB, ECS Fargate — and none of
  them exist.
- **Railway is a leftover — but it was really linked.** `railway.json` / `railway.cron.json` describe an
  earlier hosting model (four cron services, a Docker image with the SQLite databases *baked into it* and
  copied onto a volume at boot). Superseded by the Cloudflare-tunnel model. However `.railway/config.json`
  holds a live link: **project `8b004973-7930-49d0-9602-2176cc20a014`**, environment
  `6a5ea5a8-eac6-43e4-adc2-2460f291df25`, service `d991d2af-7093-4bd5-8819-9cadf02c579d`. **This is the single
  most likely source of silent billing.** Open that project first — a dormant service with an attached volume
  bills whether or not anyone uses it.
- **The tree is 85 GB** on this laptop.
- **The live database is on this laptop and writing right now** — `data/db/kanida_quant.db` (87 MB) was last
  written 22 Sep 19:02 and has an active `-shm` file.

So your concern about carrying forward unnecessary cloud cost is, on the evidence, mostly unfounded — because
the migration never happened. **The real exposure is the opposite one:** production depends on a laptop that we
have already established sleeps through market opens.

> **What I cannot see from here:** your actual AWS, Vercel and Railway billing. I have no credentials and did
> not go looking for any. Before deciding anything, open each billing console and check for: the Railway
> project named above, an idle NAT gateway (~$32/mo even with zero traffic), any RDS or ElastiCache instance,
> and orphaned EBS volumes and Elastic IPs. Those are the classic silent charges. If the terraform truly never
> ran, your spend is Railway, a domain, Cloudflare and Vercel.

### One security note, unrelated to cost

`Password Manager.txt` sits in the repository root. The good news: it is **not tracked by git and has never
been committed** — I verified against the full history, and the remote is
`github.com/kanida-ai/KANIDA.AI-TERMINAL.git`. The risk: it is also **not in `.gitignore`**, so a single
`git add -A` would publish it. Move it out of the repository, or add it to `.gitignore` today. I did not open
the file.

---

## 2. What the old environment got RIGHT (keep this)

I expected to recommend discarding more than I will. Three things are genuinely good:

**a) The architecture doc is correct.** `docs/launch/CLOUD_ARCHITECTURE.md` (LOCKED 2026-06-13) reaches almost
exactly the design I would propose: a one-way publish wall between a research machine and a lean always-on
production store; raw history never in a request path; "launch on a volume, reach for Postgres only when scale
demands it". Its one-line model — *cloud = lean serving DB + the daily loop; laptop = full history that
publishes compact intelligence up* — stands.

**b) The SQLite→Postgres work is the right shape.** `backend/pgdb.py` and `backend/oltp_db.py` implement a
**hybrid split**: market data (one writer, many readers) stays on SQLite; OLTP data (many concurrent writers —
users, orders, portfolios) moves to Postgres. It is pooled, fail-loud, default-off, and it reads conflict
targets from Postgres' own catalog so they cannot drift. It also documents the bug it exists to avoid: the
legacy `backend/db.py` silently rewrote `INSERT OR IGNORE` into a plain `INSERT`, dropping conflict handling.
That judgement is sound and I would not redo it.

**c) AutoTrade is a major asset — reuse it, do not rebuild it.**

| | Measured |
|---|---|
| Size | 39,241 lines across `backend/autotrade/` |
| Tests | 117 test files |
| Brokers | 8 adapters — Zerodha, Rupeezy, Angel, Dhan, 5paisa, Fyers, Upstox, + base/registry/router |
| Safety machinery | kill switch, position reconciler, exit poller, GTT manager, order ledger, risk manager, durable claims, recovery, iceberg, ladder, worked orders, vault |
| Regulatory | per-broker egress provisioning for SEBI static-IP registration |

Its coupling is also better than I feared: **44 of its outside imports go through `oltp_db`** — a single
connection layer that can be repointed in one place. The rest is ~30 imports of `falcon.trade.services`
(order executor, Kite ticker, MTF eligibility, margin calc) and `services.kite_auth`. That is a seam, not a
knot. Rebuilding this from scratch would cost the better part of a year.

Also worth keeping: billing (Razorpay), the paywall, signup and the legal pages — modules M1–M8, all built and
audited GREEN per `docs/launch/STATUS.md`.

---

## 3. What to retire

| Retire | Why |
|---|---|
| The Terraform stack as written | 33 resources including NAT gateway, Multi-AZ RDS, ElastiCache, EFS and ALB — roughly $250–400/month **before a single user**. This is precisely the over-engineering you want to avoid. Nothing was applied, so deleting it costs nothing |
| Railway configs | Superseded hosting model |
| Databases baked into the Docker image | `Dockerfile` copies `kanida_quant.db` and `kanida_universe.db` into the image as seeds. Images should carry code, never data |
| The Cloudflare tunnel as *production* | Excellent as a development tool. As the path to `api.kanida.ai` it makes the laptop a single point of failure |
| `backend/db.py`'s global `IS_POSTGRES` switch | Replaced by the domain-routed `oltp_db`. Already documented as unsafe |
| The Falcon equity engine, for now | Park it. Stocks come after derivatives (§6) |

---

## 4. The economic fact that should drive the whole design

KANIDA is not a normal SaaS, and this is the most important thing in this document:

> **Your expensive work is identical for every user.** The 11:15 reading of NIFTY is the *same object* whether
> one person or one million people look at it. Ingesting it, computing the state and writing the snapshot costs
> exactly the same either way.

So the cost curve is:

- **Ingestion + computation — flat in users, forever.** One small always-on machine captures 216 underlyings
  every 15 minutes whether you have 10 users or 10 million.
- **Serving — this is the only thing that grows.** And what it serves is immutable: a reading, once written,
  never changes. Immutable things can be cached at a CDN edge *permanently*.

That combination is unusually favourable. A million users opening the 11:15 NIFTY reading is **one** request
to your server and 999,999 served from a Cloudflare edge node in Mumbai at a few milliseconds each, for
effectively nothing. Personalisation — saved scanners, workspaces, alerts, accounts — is small, cheap data
that scales on ordinary Postgres.

**Design rule:** everything that is the same for everyone becomes a cacheable artifact. Only what differs per
user touches a database in a request.

---

## 5. The target architecture

```
       ┌── RESEARCH (this laptop) ─────────┐
       │ full history · mining · backtests │ ── publishes compact intelligence up ──┐
       └───────────────────────────────────┘                                        │
                                                                                    ▼
  VENDOR ──► INGESTION (1 small always-on box, ap-south-1)            ┌──────────────────────┐
             capture → quality gate → snapshot → state engine ──────► │  ARTIFACT STORE      │
             flat cost in users, forever                              │  immutable readings  │
                                                                      │  (object storage)    │
                                                                      └──────────┬───────────┘
                                                                                 │
                                                        ┌────────────────────────▼─────────┐
   USERS ──► CDN edge (Mumbai) ────────────────────────►│  95%+ of all reads never reach   │
              │                                          │  your server at all              │
              │                                          └──────────────────────────────────┘
              └──► APP TIER (stateless, 1 → N containers)
                       │  auth · saved scanners · workspaces · alerts · billing
                       ├──► POSTGRES (user data only — small, indexed, boring)
                       └──► AUTOTRADE SERVICE (separate; its own risk boundary)
```

Five properties, each chosen because it is cheap now *and* does not need replacing later:

1. **One region: `ap-south-1` (Mumbai).** Your users, the exchange and the brokers are all in India. SEBI
   static-IP registration expects an Indian egress IP. The old terraform already chose this, correctly.
2. **The app tier holds no state.** No local files that matter, no in-process cache anyone depends on. That is
   what makes "1 container" become "40 containers" a configuration change rather than a project.
3. **Market artifacts live in object storage, never in the OLTP database.** Putting 15-minute readings in
   Postgres is how people accidentally build a $2,000/month database.
4. **Postgres from day one — but only for user data.** See §7; this is the one place I am refining earlier
   advice.
5. **Ingestion is a separate service from the product.** Deploying a UI fix at 11 a.m. must be incapable of
   disturbing the 15-minute capture.

---

## 6. Derivatives first — agreed, and here is the reasoning

Your instinct is right, for four reasons:

1. **The correct product already exists for derivatives.** `kanida-app` — the screener, the workspace, the
   shared state engine, the immutable snapshots — is the new architecture. Stocks have no equivalent yet.
2. **Derivatives is the smaller data problem.** `derivatives.db` is 1.7 GB against 147 GB of legacy equity
   history. Getting the small one right first is cheaper and faster.
3. **The equity pipeline is currently the broken one** — 495 errors, 0 rows, four runs running, and the 22 Sep
   session missing entirely. Do not migrate a failing pipeline; fix it on the ground, then move it.
4. **F&O users are the ones who pay.** They trade more often and need intraday intelligence more.

Sequence: **derivatives end-to-end in the cloud → prove it through a full live session → then stocks**, reusing
the same ingestion, quality gate, artifact and serving layers. The whole point of §5 is that the second asset
class should be a configuration of an existing pipeline, not a second pipeline.

---

## 7. One refinement to what I told you earlier

Two weeks of context ago I said: stay on SQLite, don't take on a database migration. I want to sharpen that,
because the old product's history is evidence.

- **Market data stays on SQLite.** One writer, many readers, 3,979 lines of SQL already written against it in
  `derivatives.py`. SQLite is genuinely excellent at this and moving it would be pure cost.
- **User data should be Postgres from the first day in the cloud.** Accounts, saved scanners, workspaces,
  alerts, billing, and AutoTrade's order state. These have many concurrent writers, which is the one thing
  SQLite is bad at.

The old product is the proof. It launched user data on SQLite, then had to write two modules — `pgdb.py` and
`oltp_db.py`, with a hand-built SQL dialect translator — purely to escape that decision later. A small managed
Postgres costs about $15–20/month. Retrofitting one costs months. **Take Postgres now for the small, new
thing; leave SQLite alone for the big, working thing.** That is the same hybrid split the old codebase arrived
at independently, which is the best evidence it is right.

---

## 8. The cost ladder

Indicative monthly figures for `ap-south-1`. The point is the *shape*, not the precision.

| Stage | Users | What runs | ~Cost/mo |
|---|---|---|---|
| **0 — now** | you + pilot | 1 small box (ingestion + app + Postgres on it), object storage, CDN free tier | **$25–45** |
| **1** | → 5,000 | ingestion box + 1 app container, managed Postgres (small), CDN, backups | **$90–150** |
| **2** | → 100,000 | ingestion box *unchanged*, 2–4 app containers behind a load balancer, Postgres + 1 read replica, Redis, CDN carrying 95%+ of reads | **$400–800** |
| **3** | millions | ingestion box *still unchanged*, app tier auto-scaled, Postgres scaled up + replicas, CDN doing the heavy lifting it was doing at stage 2 | **$3–5k** |

Two things to notice. **The ingestion line never moves** — that is the §4 insight paying off. And at stage 3,
even 1% of a million users at ₹999/month is roughly ₹1 crore of monthly revenue against a $5k bill. The
architecture only has to not be wasteful; it does not have to be clever.

What would wreck this: serving market data from Postgres instead of a CDN, a NAT gateway you do not need,
Multi-AZ before you have users, and an LLM in the request path. The first three are in the old terraform. The
fourth is why capability 10 must arrive metered.

---

## 9. Recommended approach

**Do not clean up the cloud first — there is almost nothing there. Build the derivatives path into a small,
correct cloud, and let the old environment keep serving its users untouched until the new one is proven.**

1. **Verify the billing consoles** (§1) and shut down anything idle. Ten minutes, possibly zero findings.
2. **Delete the Terraform stack and the Railway configs.** They describe futures you are not going to take.
3. **Stand up one small box in `ap-south-1`** with object storage and a CDN in front. Move F&O ingestion to it
   first — this is also Phase 0 of the platform plan, so the two plans converge here rather than competing.
4. **Publish readings as immutable artifacts** from the moment ingestion moves. Doing this on day one is what
   buys the whole cost curve in §8; retrofitting it later means reworking every read path.
5. **Bring `kanida-app` up beside it** with user data on Postgres from the start.
6. **Port AutoTrade as a separate service** when derivatives is proven live — repointing `oltp_db`, not
   rewriting it. Keep it behind its own risk boundary, paper-default, and human-armed, exactly as today.
7. **Then stocks**, through the same pipeline.

The old environment stays exactly as it is until step 5 is serving a full live session correctly. Nothing gets
switched off on the strength of a plan.
