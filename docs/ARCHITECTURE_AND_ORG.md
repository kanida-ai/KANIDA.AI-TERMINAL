# KANIDA Strategies — technical architecture and AI agent organisation (design for approval, 26 Sep 2026)

Status: **proposal**. Nothing is moved or created until the owner approves.

---

## 1. Principles
1. **One product, one repo, one owner per area.** The new app lives in its own clean repository. The old app is never mixed in; reused parts are copied in and owned here.
2. **The owner gives direction; agents build; humans gate risk.** Pushes to `main`, production deploys, spending, deleting data and anything touching money or broker access need the owner's explicit OK.
3. **Agents never hold production secrets.** Deploys run through CI with short-lived cloud roles. Agents work on the Mac or in CI, never with standing admin keys.
4. **Every change is traceable.** Each change has a task, an owning team, tests, a review and a record in the change log.

---

## 2. Technical architecture

### 2.1 Repository layout (new clean repo: `kanida-strategies`)
```
kanida-strategies/
├── apps/
│   └── client/               Expo app: web + iOS + Android (one codebase)
│       ├── app/              routes (screens)
│       ├── src/features/     strategies · discover · lab · paper · alerts · admin · account
│       ├── src/ui/           design system: tokens, components, compass logo
│       └── src/lib/          API client, auth, formatting
├── services/
│   └── api/                  FastAPI backend (the only public backend)
│       ├── core/             config, database, auth, sessions, admin panel, audit, consent
│       ├── strategies/       builder, analytics, adjustment assistant, paper execution, alerts
│       ├── market/           quote reading, chains, expiry calendar, quote validity
│       ├── quant/            Lab, evidence, experiments, decision log, (later) principles registry
│       └── tests/
├── jobs/                     background workers, each deployable on its own
│   ├── capture/              option market capture (from market_data/derivatives)
│   ├── alerts/               alert evaluation
│   ├── paper/                paper order filling + expiry settlement
│   ├── lab/                  Lab test queue
│   └── maintenance/          backups, retention/archive, health reports
├── infra/
│   ├── terraform/            AWS: network, ECS services, database, cache, CDN, secrets, alarms
│   ├── docker/               one image per service/job
│   └── ci/                   GitHub Actions: test, scan, build, deploy (OIDC, no stored keys)
├── docs/
│   ├── ARCHITECTURE.md       this design, kept current
│   ├── OWNERS.md             who owns what (the accountability directory, section 4)
│   ├── areas/<area>/         CONTEXT.md per area: purpose, interfaces, decisions, runbook
│   └── runbooks/             deploy, rollback, incident, restore, key rotation
├── .claude/agents/           the agent team definitions (section 3)
└── SECURITY.md               security and privacy rules (section 6)
```
Current code maps across with no rewrite:

| Current location | New location |
|---|---|
| `kanida-app/src`, `app/` | `apps/client` |
| `kanida-app/server/kanida_pilot` (+ `strategy_builder`) | `services/api/{core,strategies,market,quant}` |
| `market_data/derivatives` | `jobs/capture` |

Research files, old experiments and the other `Kanida_Falcon` projects stay in the old repo.

### 2.2 Cloud (AWS Mumbai, same account, separate from the old app)
```
Users (web / iOS / Android)
   │  HTTPS
CloudFront CDN + AWS WAF (bot/rate rules)  ── static web app from S3
   │
Application Load Balancer (strategies.kanida.ai, TLS)
   │
ECS Fargate — api service (2+ tasks, private subnets)
   │            │                 │
RDS Postgres   ElastiCache Redis  Secrets Manager (Kite, Google, keys)
(encrypted,    (shared quotes,
 backups)       sessions cache)
   │
ECS jobs: capture · alerts · paper · lab · maintenance (scheduled / always-on)
   │
S3 archive (append-only market history, encrypted, versioned)
CloudWatch logs + alarms · CloudTrail · GuardDuty · budget alerts
```
- **Environments:** `preview` (private, what you'll use now) and later `prod`. Same code; the configuration differs.
- **Access now:** invite-only accounts, plus WAF rate limits. The admin panel is owner-only.
- **Mobile:** Expo EAS builds → TestFlight (iOS) and Play internal testing (Android), pointed at `strategies.kanida.ai`.

### 2.3 How the Mac and the cloud work together
```
Owner (Mac) ── task ──► CTO agent (Claude Code on the Mac)
                           │ delegates
                     Director agents ──► developer / maintenance / support agents
                           │ code + tests on a branch
                     GitHub (private repo, protected main)
                           │ PR → CI: tests, security scans, build
                     Owner approves merge / deploy
                           │ CI deploys with a short-lived AWS role (OIDC)
                     AWS preview environment
```
Agents never deploy directly to AWS. CI does, after your approval. Monitoring flows back: CloudWatch alarms, the in-app Jobs & health panel, and daily health reports from the Cloud Ops team.

---

## 3. The AI agent organisation

### 3.1 Hierarchy
```
                         OWNER (Shyam)
                              │
                     CTO agent — "Kanida CTO"
   ┌──────────┬──────────┬────┴─────┬──────────┬──────────┬──────────┐
 Product &   Strategies  Market     Quant &    Platform   Cloud Ops  QA &
 Experience  Engine      Data       Evidence   & Security            Release
 "Pixel"     "Vega"      "Tide"     "Sigma"    "Aegis"    "Atlas"    "Gate"
```
Each director team has three kinds of member agent:
- a **Developer**, who builds;
- a **Maintainer**, who fixes, upgrades and pays down technical debt;
- **Support**, who monitors, triages and writes the runbooks.

### 3.2 Teams, scope and monitoring

| Team (director) | Owns (folders) | Members | Monitors |
|---|---|---|---|
| **Product & Experience — Pixel** | `apps/client/**`, `src/ui/**` | Pixel-Dev (screens and flows), Pixel-Maint (design system, accessibility, upgrades), Pixel-Support (UX bugs, phone/browser matrix) | Screen errors, load time, accessibility checks, mobile build health |
| **Strategies Engine — Vega** | `services/api/strategies/**` | Vega-Dev (builder, adjustment screen, paper), Vega-Maint (analytics correctness, refactors), Vega-Support (user-reported calc issues) | Analysis errors, paper fill anomalies, alert delivery |
| **Market Data — Tide** | `services/api/market/**`, `jobs/capture/**`, S3 archive | Tide-Dev (feeds, calendars), Tide-Maint (schema, identity, retention), Tide-Support (capture gaps, feed outages) | Capture success per bar, data freshness, missed sessions, feed status |
| **Quant & Evidence — Sigma** | `services/api/quant/**`, `jobs/lab/**` | Sigma-Dev (Lab, live paper lab, principles registry), Sigma-Auditor (the existing `dev-quant-auditor`: look-ahead, costs, overfitting), Sigma-Support (evidence questions) | Evidence versions, stale/superseded evidence, experiment registry |
| **Platform & Security — Aegis** | `services/api/core/**`, `SECURITY.md`, auth/admin/consent | Aegis-Dev (auth, admin panel, consent), Aegis-Security (threat review, dependency and secret scanning), Aegis-Support (access requests, account issues) | Failed logins, rate-limit hits, admin actions, vulnerability alerts |
| **Cloud Ops — Atlas** | `infra/**`, `jobs/maintenance/**`, runbooks | Atlas-Dev (Terraform, CI/CD), Atlas-Maint (patching, cost, backups/restore drills), Atlas-Support (on-call, incidents) | Uptime, latency, error rate, CPU/memory, database health, backups, AWS bill, alarms |
| **QA & Release — Gate** | `services/api/tests/**`, `scripts/e2e/**`, release checklist | Gate-Dev (tests, browser checks), Gate-Reviewer (the existing `dev-reviewer`), Gate-Release (release notes, go/no-go record) | Test pass rate, flaky tests, release gates, regressions |

### 3.3 How work flows
1. **You → CTO:** "Build X" / "Fix Y", in plain words on your Mac.
2. **CTO:** restates the task, picks the owning team(s), and shows you a short plan when the change is significant.
3. **Director:** splits the work, assigns developers, and names the reviewers (Sigma-Auditor for numbers, Aegis-Security for anything touching access or data, Gate-Reviewer always).
4. **Developers:** build on a branch within their folders, with tests and updated docs (`CONTEXT.md`).
5. **Review:** the reviewers verify independently. Nothing merges on "tests pass" alone.
6. **Gate-Release:** runs the release checklist.
7. **CTO → you:** a short summary. You approve the push and deploy.
8. **Atlas:** deploys via CI and watches the monitors. Support agents own follow-ups.

**Cross-team rule:** a team edits only its own folders. When it needs a change elsewhere, it asks that team's director (through the CTO). This is the same discipline that worked in slice 14.

**Standing permissions:**
- **Agents may:** read, write in their own folders, run tests locally, and open PRs.
- **Only the owner may approve:** merging to `main`, deploys, spending, deleting data, and anything involving broker or live trading.

---

## 4. Accountability directory (becomes `docs/OWNERS.md`)

| If you want… | Ask | Owner team |
|---|---|---|
| a screen changed, a new page, mobile/app look | CTO → **Pixel** | Product & Experience |
| strategy maths, adjustments, paper trading, alerts | CTO → **Vega** | Strategies Engine |
| market data, capture, prices, expiry calendar | CTO → **Tide** | Market Data |
| backtests, evidence, experiments, "is this edge real?" | CTO → **Sigma** | Quant & Evidence |
| login, invites, users, admin panel, privacy, security | CTO → **Aegis** | Platform & Security |
| servers, deploys, uptime, costs, backups | CTO → **Atlas** | Cloud Ops |
| testing, release readiness, "is it safe to ship?" | CTO → **Gate** | QA & Release |

Coordination:
- **Daily (automated):** a health report from Atlas, Tide and Gate, visible in Admin → Jobs & health.
- **Per task:** the owning director reports to the CTO; the CTO reports to you.
- **Incidents:** Atlas-Support is first responder and pulls in the owning team. You get a plain summary.

---

## 5. Existing agents mapped to the new structure
| Existing | Becomes |
|---|---|
| `dev-quant-auditor` | Sigma-Auditor |
| `dev-reviewer` | Gate-Reviewer |
| `dev-architect` | CTO's planning aide |
| `dev-playbook-keeper` | the docs steward for `docs/areas/*` |
| `portal-ops`, `autotrade-engineer`, `falcon-*`, `tier-discovery` | stay with the old app (not part of Strategies) |

---

## 6. Security and privacy (Mac + cloud)

### Mac
- **FileVault disk encryption is ON (verified).** Keep the screen lock short.
- **Fix now:** `engine/Password Manager.txt` sits untracked and **not git-ignored**. Move its contents to a real password manager (1Password/Bitwarden), delete the file, and add a global git-ignore for secrets files.
- Secret scanning (gitleaks) runs before every commit, on the Mac and in CI.
- The long-lived AWS access key for user `kanidaCloud` is replaced by AWS SSO (short-lived sign-in) on the Mac. Agents get no cloud admin rights.
- GitHub: 2FA on, `main` protected (PR + passing checks + owner approval), private repo, deploy keys scoped.

### Cloud
- **Network:** everything runs in private subnets. Only the load balancer and CDN are public. WAF with rate limits and bot rules.
- **Encryption:** TLS everywhere. Database, cache, S3 and backups are encrypted at rest. S3 versioning and object lock on the market archive.
- **Secrets:** Kite, Google and signing keys live only in AWS Secrets Manager. You enter them yourself; code reads them at start-up; they are never in the repo, logs or chat.
- **Least privilege:** each service and job gets its own narrow IAM role. CI deploys with an OIDC role limited to this app.
- **Watching:** CloudTrail (every AWS action logged), GuardDuty (threat detection), ECR image scanning, dependency alerts, budget alarms.
- **App security:**
  - invite-only; sessions revocable and expiring;
  - CSRF and JSON-only POSTs (already in place);
  - rate limits on login and access requests (in place);
  - security headers and a Content Security Policy on web;
  - an owner-only admin panel with every admin action audited.

### Privacy (India DPDP Act, 2023)
- Collect the minimum: email, name, and the strategies users create.
- **Research sharing is opt-in**, already built with the decision log.
- Pooled research reads only consented, de-identified records.
- Users can export and delete their data (to be built in the preview).
- A clear privacy notice.
- Data stays in the Mumbai region.

### Hack-resistance process
- Aegis-Security reviews every change touching access, data or money.
- A quarterly dependency and permission review.
- **An external penetration test before public launch.** No system is "hack-free"; the goal is layered defence plus fast detection and recovery (tested backups and a rollback runbook).

---

## 7. Rollout (after approval)
1. Create the private repo `kanida-strategies` and move the new app's code into the layout in 2.1. History is preserved; tests pass unchanged.
2. Write `OWNERS.md`, `SECURITY.md` and the `docs/areas/*/CONTEXT.md` files.
3. Create the agent definitions in `.claude/agents/` (CTO, 7 directors, and their members), each with its folder scope, permissions and monitoring duties.
4. Security quick wins on the Mac and GitHub (secrets file, gitleaks, branch protection, AWS SSO). Each step is confirmed with you.
5. Continue cloud-preview step 2 (container + AWS preview at `strategies.kanida.ai`) under the Atlas team.
