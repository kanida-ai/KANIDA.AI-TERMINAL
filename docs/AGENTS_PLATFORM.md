# KANIDA Agent Platform — Architecture (merge of research + live product)

> **Governing rule.** DEV agents create and improve the intelligence system. KANIDA product agents use that system to observe, decide, explain, track and learn for customers. The two interact **only** through versioned code, config, evals and deployment — never through shared runtime identity or memory.

Status: **shared coordination board** — canonical `KANIDA.AI-TERMINAL/docs/AGENTS_PLATFORM.md`. Both the `kanida-falcon` (R&D/engine) and `cloud/infra` sessions read this at the start of any Agent-Platform task and update it at the end. Deployed facts below were **verified live** by the cloud/infra session. Lives on branch `feat/agent-platform` pending review + merge to `main`.

---

## 1 · Two repos, one product

| Repo | Role |
|---|---|
| `Documents/Kanida_Falcon` | **R&D lab.** Prove an agent here (detector, evidence, replay, playbook). Never the product of record. |
| `KANIDA.AI-TERMINAL` (canonical) | **The product.** A proven agent **graduates** into `backend/agents/<name>/`. Canonical always wins; the Falcon copy is the seed. |

Graduation is the exact move already used for `agent_builder`: build in R&D → copy into the canonical backend → mount → deploy.

---

## 2 · Deployed reality (verified live)

- **Production = ECS Fargate.** cluster `kanida-prod-cluster` / service `kanida-prod-svc` / task-def `kanida-prod-app` (2 vCPU / 4 GB, desired = 1, 24/7) behind ALB `kanida-prod-alb`. `api.kanida.ai → ALB → ECS`. Health verified at the public domain.
- **One image = the whole FastAPI app.** `falcon/`, `power_user/`, `autotrade/`, `agent_builder/` are **all mounted in `backend/main.py` in the same process/task.** `agent_builder` is **not** a separate service — it lives inside the main backend at `/api/builder`. **`backend/agents/` follows the same pattern.**
- **State:** RDS Postgres (`kanida-prod-pg`) for autotrade OLTP; SQLite (some on EFS) for Falcon signals/others; daily Parquet on S3 (`kanida-cb-src-…`). The big 1-minute `kanida_universe.db` is **not** in the cloud image (research-only).
- **Nightly EOD** = GitHub Actions (`nightly-eod.yml`) as an isolated one-shot ECS task. *(The nightly signal cron is temporarily DISABLED while an engine data-leak is fixed.)*
- **Deploy** = worktree(origin/main) → S3 `kanida-cb-src` → CodeBuild `kanida-backend-build` (image → ECR) → `ecs update-service --force-new-deployment`. Frontend = Vercel auto-deploy on push to `main`.
- The old **laptop + Cloudflared → :8001** path is **legacy/dev**, not production. Do not design against it.

---

## 3 · The merge — Falcon becomes "agent #0"

```
                         ECS Fargate — ONE FastAPI image (main.py)
   ┌───────────────────────────────────────────────────────────────────────┐
   │  power_user/ (subs, JWT, billing)     feeds/ + universe (market data)  │
   │  scheduler / GH-Actions EOD           services/ (explainer, context)   │
   │                                                                        │
   │        NEW  backend/agents/  ── Agent Runtime + Registry ──            │
   │        ┌──────────┬──────────┬──────────┬─────────────┐               │
   │        │ falcon   │ chart    │ options  │ events  …#20 │  (manifests)  │
   │        │ (#0)     │ (#1)     │          │             │               │
   │        └────┬─────┴────┬─────┴────┬─────┴──────┬──────┘               │
   │             └──────────┴──── decisions/intents ┘                       │
   │                              │                                         │
   │                              ▼                                         │
   │                     autotrade/  (execution — paper-default, gated)     │
   └───────────────────────────────────────────────────────────────────────┘
                                   │
                             RDS PG · SQLite/EFS · S3 Parquet
```

- No per-agent service, no new platform. `backend/agents/` is a package mounted in `main.py` (e.g. `/api/agents/*`), shipped by the **normal backend deploy**.
- Every platform primitive the ~20 agents need already exists (scheduler, feeds/universe, power_user subs, autotrade execution, services/explainer, outcome store). We add the **runtime + registry** on top and port agents in.

---

## 4 · The product-agent contract (shared lifecycle, per-agent playbook)

Every agent implements the **same lifecycle**; only its **playbook** differs.

```
SCAN → ANALYZE → DECIDE → EXPLAIN → TRACK → RESOLVE OUTCOME → LEARN ──► future evidence
```

- **Shared** (the runtime provides): scheduling, market-data access, the point-in-time **evidence/occurrence store**, subscriptions/entitlements, the storyline emitter, governance hooks, and **auto-trade routing into `autotrade/`.**
- **Per-agent** (the manifest + playbook provides): the detector/signature, what "evidence" means, the decision gates, the metrics. *(Chart Agent's playbook = the v3 closed-loop doc.)*

**Agent manifest (registry entry):**
```
agent_id: chart-v1
class: observe            # observe | decision | experience
universe: nifty500
timeframe: daily
tools: [market_data, evidence_store, historical_probability]   # NO git/shell/broker/deploy
schedule: eod             # or intraday
outputs: [observation, direction, probability, evidence, intent]
tracking: [T1,T3,T5,T10, MFE, MAE]
learning: enabled         # appends immutable occurrences; NEVER edits rules live
execution: { route: autotrade, mode: paper, live: requires_broker_cert + operator_arm }
permissions: { trade_execution: gated, source_code: false }
```

Long / short / intraday / swing are **manifest configuration**, not separate agents — this is how we reach 20 agents without 20 architectures.

---

## 5 · Auto-trade routing & safety (the hard boundary)

- Agents **emit intents**; they **never** touch a broker. Execution flows to `autotrade/`, which is **paper-default by construction**.
- An intent reaches a **real** broker only when **(a)** that broker is per-broker **certified** (env flag, e.g. `RUPEEZY_LIVE_CERTIFIED=true`) **and (b)** the operator has **explicitly armed** the session. Uncertified → blocked at the cert gate (zero exposure). Plus: asymmetric kill-switch, MIS square-off (~15:12), durable order ledger, RMS. Broker-agnostic.
- **`/promote` and `/deploy` keep the cloud roll and live-arming human-gated on Shyam's explicit go — never automatic.** Product agents carry no git/shell/broker/deploy tools.
- **Verified code refs** (cloud session, from the code): **cert gate** `autotrade/broker/base.py → BrokerBase._certification_block(action="order")` — blocks real placement on an uncertified adapter, returns `UNCERTIFIED_BROKER_BLOCKED`, pages via `alerts.send_urgent`, **fail-closed**; lookup `autotrade/broker/registry.py → is_certified(broker_name)`. **Kill-switch** `autotrade/monitoring/kill_switch.py → KillSwitchExecutor.fire()` — audited to `autotrade_kill_switch_log`, asymmetric. **Square-off** `autotrade/config.py → square_off_time` (default 15:29) / `mis_square_off_time` (~15:12) via the square-off scheduler + `autotrade/exit_gate.py`; per-strategy in `autotrade/ladder.py`. **Net:** an intent becomes a real order only after `_certification_block` passes **AND** the operator has armed the session; independent backstops = `KillSwitchExecutor.fire` + the square-off scheduler.

---

## 6 · The DEV system (builds the product; separate world)

**DEV agents** (Claude Code, `.claude/agents/` in the canonical repo — add to the existing autotrade-engineer / falcon-ui / falcon-cotrading / portal-ops / tier-discovery):

| Agent | Job |
|---|---|
| `dev-architect` | requirement → affected systems → interfaces → blast radius → plan (read-only) |
| `dev-agent-engineer` | builds `backend/agents/*` (runtime, registry, per-agent code) |
| **`dev-quant-auditor`** | leakage / point-in-time / T+N / survivorship / cost / reproducibility — the star |
| `dev-reviewer` | correctness, architecture violations, tests actually pass |
| `dev-playbook-keeper` | keeps the per-agent playbooks + this doc as source of truth |

**Skills** (`.claude/skills/`, the *how*): `implement-agent` · `check-lookahead` · `regression` · `promote-to-product` · `deploy-staging`.

**Rules** (`.claude/rules/`): `quant.md` (point-in-time law), `backend.md`, `frontend.md`, `database.md`.

**Commands** (`/…`): `/architecture` · `/agent <name> <task>` · `/audit <target>` · `/screen <date>` · `/replay <SYM> <DATE>` · `/playbook <change>` · `/promote <agent>` (R&D → `backend/agents/` + manifest + PR) · `/deploy` (CodeBuild→ECS, **gated on Shyam**) · `/status`.

**Tool separation (non-negotiable):** DEV agents get bash/git/tests/deploy; PRODUCT agents get market-data/charts/events/memory and **nothing** that writes source or reaches a broker/deploy.

---

## 7 · Graduation & CI (adding an agent)

```
R&D proves agent (Kanida_Falcon)
   → /promote : copy → backend/agents/<name>/ + register in manifest + open PR to main
   → dev-quant-auditor + dev-reviewer sign off
   → merge to main
   → /deploy (GATED): S3 → CodeBuild → ECR → ECS roll   [cloud session runs; Shyam approves]
   → per-agent subscription wired in power_user/ billing
```

Cross-session coordination = canonical repo + this shared board + a git worktree + the messaging channel (same system that worked for `agent_builder`).

---

## 8 · Build approach (full architecture, not sequenced piecemeal)

Per Shyam: stand the whole thing up, then add agents one by one.

1. **Runtime + registry + manifest schema** in `backend/agents/` (the shared machine).
2. **Chart Agent as agent #1**, the reference implementation end-to-end (its v3 SPEC items: strategy-replay ETV, retest depth-floor, then screener→storyline), routing intents to `autotrade/` (paper).
3. **Falcon wrapped as agent #0** so the old engine is just another registry entry.
4. Clone the manifest per new agent (Options, Events, …) — each only implements its detector + playbook.
5. Cloud activation in parallel via the gated deploy path.

---

## 9 · Built vs spec / pending

| Item | Status |
|---|---|
| Production platform (ECS one-image, autotrade, power_user, feeds, scheduler) | **BUILT (live)** |
| `agent_builder` mounted-in-backend pattern to copy | **BUILT** |
| Chart Agent research (detector, screener, replay, v3 playbook) | **BUILT (R&D)** |
| `backend/agents/` runtime + registry + manifest | **SPEC — build first** |
| Strategy-replay ETV, retest depth-floor, look-alike populations | **SPEC** |
| DEV agents / skills / commands in canonical `.claude/` | **SPEC — scaffold** |
| Deployed-topology & autotrade gate refs | **CONFIRMED + cited** (cloud session, from code — §5) |
| `backend/agents/` runtime + registry + chart skeleton | **BUILT (this branch)** — mounts at `/api/agents/*`, chart-v1 registered |
| DEV `.claude/` agents + commands + rules + skills | **BUILT (this branch)** |
| Overall ~20-agent direction | **CONFIRMED by Shyam** (green light) |

---

*KANIDA.AI · Agent Platform architecture · draft v1. Anchored on the verified live topology; merges the research lab into the existing product rather than rebuilding it.*
