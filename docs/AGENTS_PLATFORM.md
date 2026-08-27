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
| **Chart Agent — multi-pattern library** (`backend/agents/chart/patterns/`, registry, guarded) | **BUILT (this branch)** — detectors self-register; a bad detector can't crash the agent/boot |
| **Chart Agent — Horizontal-Trendline detector** (ported `_levels` clustering + live-stage `classify`, all `PARAMS` preserved; point-in-time) | **BUILT (this branch)** — flags TITAN 2022-08-30 breakout @ ~2565 |
| **Chart Agent — pattern-forward evidence** (ported `pattern_evidence`: T+1..T+10 win/ETV/median/MFE/MAE + edge-vs-baseline) | **BUILT (this branch)** |
| **Chart Agent — strategy-replay ETV (§8.2)** (`strategy.py`: governed frozen policy `S-horiz-v1` — next-open entry, structural close-<level invalidation, 8% trail, optional target/hard-stop, H=T+10; `replay_one` + `strategy_evidence`; both outcome families kept separate) | **BUILT (this branch)** — resolved-only replay; costs on every trade; exits classified STOP/TARGET/TRAIL/INVALIDATION/HORIZON |
| **Chart Agent — §9 gate stack now reads STRATEGY stats** (G1 sample · G2 Strategy-ETV>0 & edge vs same-policy baseline · G3 CI_low(Strategy-ETV) · G5 strat-MAE+payoff · G6 strategy-recency decay) | **BUILT (this branch)** — `basis="strategy_replay"`; TITAN 2022-08-30 still WATCH (n=6 < 20) |
| **Chart Agent — G4 nested-population coherence (§7.2/§7.3)** | **SPEC** — reported as `skipped` with reason, never silently passed |
| **Chart Agent — bootstrap CI (§7.3)** | **SPEC** — CI_low is a labelled normal-approx SE for now |
| **Chart Agent — retest depth-floor (§5.4)** | wired but **DISABLED by default** (`retest_depth_max=None`) so BUILT behaviour is byte-identical; enable via governance |
| **Chart Agent — triangle / channel detectors** | **SPEC** — registered skeletons returning `[]` with v3 TODOs (make the multi-pattern seam real) |
| **Chart Agent — data loader** (`data.py`: SQLite fallback to R&D `kanida.db`, env `AGENT_CHART_DB`) | **BUILT (this branch)**; cloud feeds/S3 wiring = **SPEC** |
| **Chart Agent — FULL-UNIVERSE fast screener** (`screener.py`: `scan_universe(D)` / `scan_universe_detailed(D)` run every registered pattern's live-stage classifier across the WHOLE daily source point-in-time; `build_screen`/`load_screen` precompute+serve; `data.all_symbols()`+`data.load_panel()` one-shot windowed panel) | **BUILT (this branch)** — MEASURED on R&D `kanida.db`: **1561-symbol universe**, full-universe scan for 2022-08-30 = **76 setups in ~3.0s**, serve latency **~26 ms**. **Freshness guard**: a symbol is classified only if it actually traded ON D (last bar == D) — "last bar ≤ D" would relabel a prior session (e.g. holiday 2022-10-26 → 0 setups, note "no trading on 2022-10-26") and break "entry = next open AFTER the signal bar". **Coverage accounting** (nothing hidden): universe 1561 = scanned 905 + skipped_min_bars 52 + skipped_stale 2 + skipped_no_window 602; surfaced in the artifact and the `/scan` response. Agrees with the detector (TITAN 2022-08-30 → BREAKOUT @ 2564.975 ≈ 2565). **Two-tier reusable design**: TIER-1 post-market precompute (setups-only per-date JSON artifact — NOT the 65 GB DB) + TIER-2 on-demand evidence (per-symbol decision/storyline unchanged; NO full-universe backtest up front). Point-in-time is STRUCTURAL — panel queried `date ≤ D`, every detector slices to `as_of_idx` before reading. Windowed detect == full-history detect **both directions, 0 mismatches over all 905 on-D symbols** (forward+reverse sweep). 7 screener tests (TITAN-in-screen · no-future-bars · build/load round-trip · timing · freshness/holiday · coverage accounting · S3 store) — all pass; existing 8 still pass (15/15) |
| **Chart Agent — screen store seam** (`screener.py`: `LocalScreenStore` = per-date JSON at env `AGENT_CHART_SCREEN_DIR`, default `backend/var/chart_screens`; `S3ScreenStore` at env `AGENT_CHART_SCREEN_URI=s3://…`; `_store()` factory) | **BUILT (this branch)** — LocalScreenStore (default) AND a REAL **`S3ScreenStore` (boto3)**: `write()` = put_object JSON, `read()` = get_object → JSON and is **guarded** (NoSuchKey / creds / network → None → live fallback, never raises). `build_screen` catches store-write failures and reports `store_error` honestly instead of crashing. The daily tier is independent of the 65 GB 1-min DB; the same engine + a different source powers a future 1-min/intraday screen |
| **Chart Agent — per-(symbol,pattern,date) evidence cache** | **SPEC** — not built; evidence stays fully on-demand (heavy backtest never precomputed) |
| **Chart Agent — data loader** (`data.py`: SQLite fallback to R&D `kanida.db`, env `AGENT_CHART_DB`; `all_symbols()`/`load_daily()`/`_nifty_close()` caches keyed on the ACTIVE SOURCE so an in-process source swap can't serve stale bars/universe) | **BUILT (this branch)**; cloud feeds via `AGENT_DATA_URI` (Parquet/duckdb) BUILT, S3 credential wiring exercised by the parity test |
| **Chart Agent — read-only portal endpoints** (`agents/router.py`: `GET /api/agents/chart/scan` · `/decision` · `/storyline`, all guarded → honest JSON, point-in-time as-of `date`) | **BUILT (this branch)** — render REAL agent output; `/scan` serves the FULL-UNIVERSE precomputed screen if present (`served=precompute`, target <1 s) else LIVE-computes (`served=live`), adds `full=true` to return the whole list, and surfaces coverage accounting (`scanned`/`skipped_min_bars`/`skipped_stale`/`skipped_no_window`) + `trading_day` + `screen_note` (empty+honest on a holiday). Decision/storyline paths unchanged (they intentionally walk back via `_recent_occurrence`). TITAN 2022-08-30 → scan finds BREAKOUT, decision honest WATCH (n=6). Smoke test added |
| **Chart Agent — portal page** (`frontend/app/power/(app)/agents/page.tsx` + `lib/agents-api.ts` + nav `agents`) | **BUILT (this branch)** — Agent→Story→Evidence 3-col view (LEFT patterns LIVE/SOON · MIDDLE date+stock selector + honest storyline · RIGHT strategy-replay + pattern-forward + gates); matches terminal-ui theme; typechecks + `next build` green. Triangle/Channel + G4 labelled SPEC in-UI |

---

*KANIDA.AI · Agent Platform architecture · draft v1. Anchored on the verified live topology; merges the research lab into the existing product rather than rebuilding it.*
