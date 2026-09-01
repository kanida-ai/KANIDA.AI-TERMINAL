# Options Agent — build brief (Algorithm 1 · Agent 2)

> Self-contained spec. You need **only** this file + `docs/AGENTS_PLATFORM.md` (how agents plug in)
> + `backend/agents/chart/` and its playbook `docs/Chart_Agent_Playbook_v3.docx` (the methodology
> template). You do **not** need any prior chat history. Coordinate with the orchestrator session
> (`ListAgents` → `kanida-falcon-…` → `SendMessage`) at each milestone. **Do not deploy, push to
> main, or touch a broker** — that is gated and human-run.

## What this agent is

A post-market **autonomous research agent** in the KANIDA agent network that scans the **options
chain** and identifies, evaluates, and tracks **options strategies** — starting with the **Iron
Condor**, then credit spreads, debit spreads, and other selling/buying structures — for the
**NIFTY 50 index** and, later, **all F&O stocks**. It is the options sibling of the Chart Agent and
follows the **same closed loop**: identify a setup → quantify its expectancy point-in-time → decide
(TRADE / WATCH / NO_TRADE by **expected value + risk**, never win-rate alone) → track to expiry →
learn. Same non-negotiables: **point-in-time is law, total honesty (never fabricate/relabel small
samples), paper-default, agents emit intents only.**

## HARD RULES (identical to the platform)

- **Point-in-time is law.** A decision at time *t* uses only options/underlying data available at *t*
  (chain snapshot, IV, greeks, OI, spot — all as of *t*). No look-ahead. Expiry/settlement handled
  correctly. Corporate actions on underlyings respected.
- **ETV (expectancy) decides, not POP/win-rate.** A 90%-POP iron condor with a fat tail can be
  negative-EV (the options "steamroller"). The gate reads **expected value net of costs + the risk
  distribution (max loss, tail)** — the same strategy that would be traded, not a hold-to-expiry
  fantasy.
- **Never fabricate.** Label small samples. If historical options data is thin, say so and switch to
  the honest live-eval + forward-tracking mode (below) — do **not** invent a backtest.
- **Costs + margin real.** Every simulated trade applies real brokerage/STT/slippage on each leg and
  respects **span+exposure margin**. Defined-risk structures only in phase 1 (the long legs cap loss).
- **Execution boundary.** Emit **intents only** → `backend/autotrade/` (paper-default, cert-gated,
  operator-armed). Non-F&O shorting rules don't apply (these are F&O); but **no agent touches a
  broker, git, shell, or deploy.** Live-arming is human-gated (Shyam).

## STEP 0 — resolve the data reality FIRST (report back before building the evidence engine)

The evidence pillar depends entirely on what Kite actually gives us. Before anything else,
**investigate and report to the orchestrator**:
1. **Live chain:** confirm Kite Connect gives the full option chain for NIFTY + F&O underlyings —
   strikes, both expiries, LTP/bid/ask, **OI, IV, greeks** (or whether we compute greeks/IV
   ourselves from LTP + spot + r + t via Black-76). Check `services/kite_auth.py` (the existing
   client) and how `instruments()` exposes NFO option instruments.
2. **Historical options data:** determine EXACTLY how much option-instrument history
   `kite.historical_data(instrument_token, …)` returns for NFO options (typically limited /
   recent-only), and whether any historical options data already exists in the repo/DBs. **This
   decides the evidence approach:**
   - **If real historical chains exist** (enough expiries) → build a true point-in-time backtest of
     the strategy's expectancy (the Chart-Agent way).
   - **If not** → phase-1 evidence is **live-eval + forward-tracking**: evaluate each setup from the
     *current* chain using the option-implied distribution (IV) for POP/EV, then **record the setup
     and track it to expiry** so evidence *accumulates* honestly over time. Clearly labelled as
     forward-tracked, small-N, not a historical backtest.
3. Report which approach the data supports. Do not proceed to the expectancy engine until the
   orchestrator confirms the approach.

## PHASE 1 scope (build ONE strategy first, like the Chart Agent's one-pattern rule)

**Iron Condor on NIFTY**, weekly + monthly expiry. Everything else (credit/debit spreads, F&O
stocks, hedging/pyramiding overlays) is documented below as the roadmap but **not** built in phase 1.

### Iron Condor — deterministic setup + evaluation
An iron condor = **sell OTM call + buy further-OTM call** (bear-call spread) **AND sell OTM put + buy
further-OTM put** (bull-put spread), same expiry, defined risk.
- **Selection (governed, frozen, SPEC-until-OOS):** short strikes by **delta band** (e.g. ~0.15–0.20
  delta) or by a % OTM / expected-move multiple; wing **width** (points); **DTE** window; an **entry
  filter** — condors want **elevated IV rank/percentile** + a **range-bound / low-trend** underlying
  (reuse the Chart Agent's regime read if useful) + adequate **liquidity** (OI/volume/bid-ask on all
  four legs). Params in a documented table (current-vs-recommended), governed — never fit on the
  deciding data.
- **Per-candidate metrics (all real, from the chain):** net **credit**, **max profit** (= credit),
  **max loss** (= wing width − credit, × lot), **breakevens**, **POP** (from the option-implied
  distribution / short-strike deltas), **expected value** (Σ P·payoff across the terminal
  distribution, net of costs), **risk/reward**, **margin** (span+exposure), and the greeks
  (net theta/vega/delta/gamma) so the risk is legible.
- **Decision gates → TRADE / WATCH / NO_TRADE**, each with a named reason (mirror the Chart Agent's
  §9 gate stack, adapted): sample/evidence sufficiency, **EV > 0 net of costs**, IV-rank/regime fit,
  liquidity, tail/max-loss cap, risk-reward floor, recency. **Honest WATCH** when evidence is thin.
- **Storyline + evidence output** shaped like the Chart Agent so the existing 3-column UI can consume
  it: the setup (the four legs on the chain), the metrics, the honest verdict + reason, and the
  tracked/forward outcome.

## Methodology mapping (Chart Agent → Options Agent)

| Chart Agent | Options Agent |
|---|---|
| pattern detector (geometry) | **strategy constructor** (pick the legs from the live chain by the governed rules) |
| pattern-forward outcome (hold to T+10) | **strategy-to-expiry payoff** (terminal P&L distribution) |
| strategy-replay ETV (the traded policy) | **the actual condor P&L net of costs+slippage+margin** (entry, optional profit-take/stop/roll, expiry) |
| §9 decision gates | options gates (EV, IV-rank, liquidity, tail, R:R) |
| evidence store (point-in-time) | options evidence store (backtest **or** forward-tracked, labelled) |

## Hedging / pyramiding / testing (roadmap — NOT phase 1)

- **Hedging:** in a condor the long wings *are* the defined-risk hedge. Later: portfolio-level hedges
  (e.g. a cheap far-OTM tail put), and delta-hedging as the underlying moves. SPEC.
- **Pyramiding / scaling:** scaling into a working position, or laddering condors across strikes/DTE.
  A **governed** rule, calibrated OOS — SPEC, never ad-hoc.
- **Testing = the expectancy engine** (Step 0's outcome). This IS the evidence pillar, not an extra.

## Where it lives / how it plugs in

- New package `backend/agents/options/` mirroring `backend/agents/chart/` (agent.py with the
  Scan→Decide→Explain→Track→Learn lifecycle, its own `strategies/` registry so each strategy is ~1
  file, `data.py` for the Kite options loader, `evidence.py`, tests).
- Mount guarded in `backend/main.py` exactly like `agents/chart` and `agent_builder` (a broken agent
  can never crash boot). Reuse the shared runtime `backend/agents/base.py` (BaseAgent, Manifest,
  Intent) and `registry`. Add its router endpoints under `/api/agents/options/…` guarded/read-only.
- Reuse the existing Kite client (`backend/services/kite_auth.py`) — do NOT write a new one.

## Acceptance (phase 1)

Given an as-of date + NIFTY: pull the point-in-time chain, construct candidate iron condors by the
governed rules, evaluate each (credit / max-loss / breakevens / POP / **EV** / margin / greeks),
return TRADE/WATCH/NO_TRADE with honest named reasons, all strictly point-in-time, guarded (never
500), with tests + a real worked example. Evidence is either a real backtest or honestly-labelled
forward-tracking per Step 0 — **no fabricated numbers.**

## Build order

0. **Data assessment (Step 0)** → report → orchestrator confirms the evidence approach.
1. Kite options data loader (`data.py`) — point-in-time chain + greeks/IV (compute if Kite doesn't
   give them) + the NFO instrument map; tests on real data.
2. Iron-condor constructor + per-candidate evaluation (metrics above), point-in-time; worked example.
3. Evidence/expectancy engine (backtest or forward-tracking per Step 0), honest + labelled.
4. Decision gate stack → TRADE/WATCH/NO_TRADE; shadow-track the WATCH/NO_TRADE.
5. Intent emission (paper) + manifest + guarded router endpoints; mount in main.py.
6. Independently point-in-time-audited (use the `dev-quant-auditor` subagent) before it's called done.

## Use the DEV crew (available in your session)

`dev-architect` (plan first, read-only), `dev-agent-engineer` (build), **`dev-quant-auditor`**
(adversarial point-in-time/leak/expectancy audit — run before trusting any number), `dev-reviewer`
(software correctness), `dev-playbook-keeper` (docs). Same loop that built the Chart Agent.

## Report back to the orchestrator

Concise handoffs at each milestone (Step-0 findings first): what you built, the real worked example
+ numbers, tests X/Y, open issues, and anything needing integration or a data/methodology decision.
