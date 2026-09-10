# KANIDA.AI — Documentation Map (the index every session starts from)

The 12 specs are your team's shared brain. All live in `docs/` in the spine repo (`KANIDA.AI-TERMINAL`).
**Session ritual:** at the start, a build session *loads* the docs in its "reads" column; at the end, it *updates* the docs it changed (new endpoint → update openapi.yaml + test plan; new decision → new ADR). Never let code and doc drift.

**Authority order (on conflict, higher wins):** PRD → Strategy Methodology / Security → Architecture → contracts (ERD/OpenAPI/Frontend). ADRs record *why*.

---

## The 12 docs

| # | Doc · file | Owner session | Wave | Generates → | Read by |
|---|---|---|---|---|---|
| 1 | **PRD** · `docs/PRD.md` | product-keeper | 1 | Numbered requirements R1..Rn, acceptance criteria, out-of-scope | **every session** (scope authority) |
| 2 | **System Architecture** · `docs/ARCHITECTURE.md` | architect | 1 | Component boundaries, request-flow diagrams (evening-publish · morning-marks · send-basket), failure modes | all backend + infra sessions |
| 3 | **Data Model / ERD** · `docs/DATA_MODEL.md` (+ `migrations/`) | data | 1 | DDL + migrations + ORM models; append-only + point-in-time rules | all backend sessions; #4 |
| 4 | **API Spec (OpenAPI)** · `docs/openapi.yaml` | api | 1 | **Server stubs + the app's API client** (the backend↔app seam) | **every backend + app session** |
| 11 | **Strategy Methodology** · `docs/STRATEGY_METHODOLOGY.md` | quant | 1 | Per-agent rulebooks, book conventions (entry/exit/cost/slippage), point-in-time law, **go/no-go gauntlet** | all agent/strategy sessions; quant-auditor |
| 10 | **ADRs** · `docs/adr/NNNN-*.md` | any (append on decision) | continuous | One page per big decision (Expo, FastAPI, Kite-Publisher-over-OMS, one book engine) | everyone (stops re-litigation) |
| 5 | **Frontend Spec** · `docs/FRONTEND_SPEC.md` | frontend | 2 | Screen inventory, routing map, state, component library, **design tokens** (terminal-dark), responsive/offline/error/a11y | all app sessions |
| 7 | **Security & Compliance** · `docs/SECURITY_COMPLIANCE.md` | compliance | 2 | Threat model, RBAC (customer/creator/RA/admin), **RA-review state machine**, DPDP + SEBI CSCRF checklist, audit logging | backend + compliance + infra sessions |
| 12 | **Compliance Content Dataset** · `docs/compliance/` | compliance | 2 | 50 labelled classifier examples, banned-phrase list, disclosure templates | the classifier session; RA flow |
| 6 | **Infra & Deployment (IaC)** · `docs/infra/` (+ `deploy/terraform/`) | infra | 2 | AWS resources-as-code, dev/staging/prod, CI/CD, secrets, backups, monitoring, cost | deploy + ops sessions |
| 8 | **Test Plan** · `docs/TEST_PLAN.md` | qa | continuous | Test matrix per requirement, **agent-tester scripts**, golden datasets, acceptance evidence | every build session; auditor agents |
| 9 | **Ops Runbook** · `docs/RUNBOOK.md` | ops | continuous | Every scheduled job + its checks, alerts, fallbacks, incident playbook | ops/on-call; the daily health agent |

---

## The golden thread (traceability — this is what keeps it coherent)
Every **PRD requirement Rn** must trace forward:
```
Rn (PRD) ──► tables (ERD) ──► endpoints (OpenAPI) ──► screens (Frontend Spec)
        └──► methodology (Strategy Spec, if it's an agent) ──► test rows (Test Plan) ──► ADR (if a decision was made)
```
Rule: **no code without a requirement id; no requirement without a test row.** A build session that can't cite an Rn is out of scope.

## Wave order (write, don't grind)
- **Wave 1 (before heavy coding — the contracts):** PRD → Architecture + first ADRs → **ERD + OpenAPI + Strategy Methodology.** Invest here; everything generates from these.
- **Wave 2 (as you enter each track):** Frontend Spec (before app) · Security/Compliance + RA state machine (early — publish path depends on it) · Infra (before deploy) · Classifier dataset (before the classifier).
- **Wave 3 (continuous):** Test Plan per requirement, Runbook per job, ADRs per decision.

## Doc-owner sessions (first sessions to run)
Each doc = one focused session that produces the skeleton + fills the parts derivable from the audit/existing code, and flags founder inputs. Suggested first three: **`api` (OpenAPI)**, **`data` (ERD)**, **`quant` (Strategy Methodology)** — because every later session generates code from them.

## Discipline
- Contracts (ERD/OpenAPI/tokens) get real depth — they generate code. Prose (PRD/ADRs) stays lean + living.
- **Claude writes and maintains the docs; you review.** Each build session updates the docs it touched — that's the hand-off between sessions.
- A short (3-page, current) doc beats a long (40-page, stale) one.
