# KANIDA.AI — Architecture Proposal v1

Derived from `docs/audit.md`. Goal (per brief Step 3): the **smallest** structure that reuses what exists. One page. Wait for founder "approved" before building.

## 0. Repo reality (corrected by git inspection)
- **ONE product repo:** `KANIDA.AI-TERMINAL` (git), with **~35 worktree branches** (kagents=`feat/agent-platform` w/ the Chart Agent; `kdeploy`=main; plus ~30 `_kanida_*` OMS/egress/persona branches). The "two deployed codebases" were two branches of this one repo.
- **ONE R&D repo:** `Kanida_Falcon` (engines; not git-tracked).
- ⇒ **Spine = KANIDA.AI-TERMINAL.** Kanida_Falcon engines are pulled in as libraries. Nothing here is a separate product to reconcile — it's branch consolidation + engine porting.

## 1. What each existing thing BECOMES (reuse, don't rebuild)
| Requirement | Reuse | Work needed |
|---|---|---|
| R1 MarketData iface | Kite adapters (TERMINAL) + thin `MarketData` iface (Falcon) | formalize one interface in front of Kite |
| R3 Trader manager | Chart Agent (`agents/chart`) + Falcon `signal_runner` + SELVI V7 rulebook | pick one deterministic rulebook = manager #1; record clock starts |
| R3 Investor manager | `momentum_port.py` (Falcon) | productize: 10–15 names, monthly rebalance |
| R5 Pathfinder | arena/challenger pipeline (Falcon) | add status lifecycle + graveyard + post-mortem |
| R4 Book engine | `strategy.py` (kagents) + `confirm_and_trade.py` (Falcon) | **unify to ONE**; add expectancy/avg-win-loss surfacing, 2× slippage, append-only signed ledger |
| R6 Base-rate + regime | `baseline.py` + `regime.py` (Falcon/universe_engine) | wire into product path (today it's research-only) |
| R2 Data-quality gate | freshness guard (`screener.py`) | add completeness/gap/corp-action check @17:30 |

## 2. Single backend/API
The existing **FastAPI app** in KANIDA.AI-TERMINAL (`backend/`) already mounts `falcon/ power_user/ autotrade/ agent_builder/ agents/`. It stays THE backend. **Add three modules:** `managers/` (Trader/Investor/Pathfinder over the unified book), `compliance/` (below), `creator/`. New routes: `/api/managers/*`, `/api/compliance/*`, `/api/creator/*`.

## 3. Compliance layer — NET-NEW, the gate to selling (R7–R11, R13)
New `backend/compliance/`: RA review queue (approve/reject per signal, publish-only-approved, audit log) · auto-disclosure generator · content classifier (Publishable vs Kanida-only) · PaRRVA client · public corrections log. **Nothing reaches a customer or the creator console without an RA-approval row.** ~0% exists today — this is the biggest true build.

## 4. The OMS decision (must resolve before build)
A **full live multi-broker OMS** (`autotrade/`, 6 brokers) exists and has placed real orders — the brief says *never place orders / OMS = Phase 2*. **Resolution:** keep it, but **wall it off the customer path**. Customer path = **Kite Publisher basket hand-off only** (user confirms in their own broker). The OMS becomes the disabled Phase-2 "AutoTrade tier" (schema-only now). Founder must confirm this.

## 5. The app (biggest net-new)
Today = **Next.js WEB**. Brief wants **Expo** (Android/iOS/web/tablet). Decision: **API is reusable as-is; build the Expo app fresh against it.** The Next.js app can serve as the web/tablet target interim. Screens: 3-manager home, idea card (+ send-basket), track-record (losers-first), methodology, Pathfinder digest, base-rate cards, corrections.

## 6. AWS (reuse)
ECS Fargate (single image) + S3 + RDS, **ap-south-1** — all exist. New modules ride the same image. Nightly jobs via existing scheduler + GitHub Actions. Add RA/creator RBAC roles; move the laptop-run prod fully to ECS.

## 7. Gap build order (= brief Step 4)
1. **Consolidate:** merge `feat/agent-platform` (Chart Agent) → main; unify the book engine (kill duplicates); formalize MarketData; rationalize the 35 branches.
2. Trader manager on the deterministic rulebook + unified book (+2× slippage) + data-quality gate. *(record clock starts)*
3. Investor manager + Pathfinder runner + base-rate/regime wired in.
4. **Compliance layer** (RA queue + disclosure + classifier v1 + corrections + PaRRVA client).
5. Expo app on real data (all screens) + methodology + analytics.
6. Kite Publisher hand-off + per-user Login-with-Zerodha; referral system.
7. Creator console.
8. Auth (OTP) + e-KYC + Razorpay tiers (Free/Pro/Intelligence) + entitlements; wire the paywall (currently off).
9. Alerts + corrections + security (RA/creator roles) + runbook automation.
10. Harden; keep the OMS gated off the customer path.

## Diagram
```
[Kanida_Falcon R&D engines] --port/wrap--> [KANIDA.AI-TERMINAL backend]
   miner · book · arena · regime           MarketData
                                             → Managers (Trader/Investor/Pathfinder)
                                             → Unified Book (expectancy, DD, ledger, 2× slip)
                                             → Base-rate/Regime
                                             → COMPLIANCE GATE (RA approve → publish)
                                             → /api/*  ──────────────► [Expo app]  [Creator console]
   [AutoTrade OMS] ── walled off, Phase-2 ──────────────────────────► (disabled tier)
```

## Out of scope until Phase 2 (per brief)
Automatic order placement / OMS on the customer path, Creator Pro, user-submitted hypotheses, real-time display data, US market.
