# SPS_V3 — SELVI Autonomous Quant Agent · Charter & Spec (DRAFT for alignment)

> Status: **ALIGNED — foundation deploying (paper/research only).**
> Two fully independent agents, one per instrument, each with its own research lab,
> memory, and knowledge base. No shared state between them.

---

## 1. Mission
Relentlessly discover, validate, and compound **real, leak-free edges** from 1-minute
(and multi-timeframe) market microstructure until the agent reliably achieves an
**average of +1% per day**, path-agnostic (intraday *or* multi-day holds) — then keep
improving. The agent runs **non-stop**, iterating thousands of times, self-learning.

## 2. The two agents (separate, as required)
| Agent | Instrument | Its own lab / memory |
|---|---|---|
| **SELVI-AE** | ADANIENT | yes |
| **SELVI-CT** | CARTRADE | yes |
They never share models, parameters, or memory. Each is a complete organism.

## 3. Prime Directive — "no" is not an option (defined so it actually works)
- The **mission** never terminates and never concludes "impossible." There is always a
  next hypothesis; the microstructure fact-space is effectively infinite.
- Every **rejected hypothesis must return**: (a) *why* it failed, and (b) the *next*
  hypothesis it suggests. Rejection redirects the search — it is never a stop.
- The agent **must** reject individual setups that fail validation. That is not "saying
  no" to the mission — it is how it converges on edges that are **real** and therefore
  **compound**. Fabricating an edge to look successful is the one unforgivable failure,
  because a fake edge loses real money and breaks the compounding that reaches the goal.
- **Rule:** *Ruthless honesty on every single test is the engine of relentless progress.*
  Never stop searching. Never fake a result. Those two together = never say no.

## 4. Goal framing (ambitious + honest)
- **Target:** average **+1%/day**, path-agnostic — intraday, overnight, 2-day, 7-day,
  X-day holds all count if the realized return averages ≥1%/day over the hold.
- **Reality anchor:** sustained +1%/day compounded ≈ elite-tier (~250%/yr). The agent is
  built to **maximize toward it without ceiling** and to report **true, out-of-sample**
  performance. How close it gets is empirical; its job is to extract every real edge the
  market allows and keep pushing. It will show honest progress, never a curve-fit mirage.

## 5. Scope
**In scope:** one instrument per agent; long & short; intraday + positional (to a max
hold H, to be set); 1-min + 5-min + daily + derived microstructure; futures & order-flow
when/if available (order-flow forward-only). Feature discovery, hypothesis generation,
leak-free backtest, adversarial validation, portfolio assembly, paper trading, reporting.

**Out of scope (guardrails):** autonomous **real-money** order placement. The agent
researches, backtests, and **paper-trades**; live execution stays **human-authorized**
(see §12). No cross-instrument data leakage. No use of any data past the decision instant.

## 6. Architecture — the "atoms"
Each agent is composed of specialized atoms (independent functions), orchestrated in a
loop. Atoms are added/upgraded over time.

1. **Fact-Miner atoms** — extract *specific* microstructure facts from 1-min data (never
   averages-as-signal). Seed facts (examples, **not** limits):
   - ATP/VWAP-vs-price divergence; where price is *pushed* down/up on volume then dries up
   - volume dry-up → volatility contraction → expansion (accumulation/distribution)
   - absorption / liquidity vacuums; time-of-day pockets of directional pressure
   - opening-range, gap behavior, prior-day levels, session-phase regime
   (trending-up / down / range / capitulation / squeeze). **The miner is told to invent
   facts far beyond this list.**
2. **Hypothesis-Generator atom** — composes facts into candidate setups (entry, side,
   exit, hold horizon), unbounded; proposes 1000s over time.
3. **Backtest/Simulator atom** — event-driven, leak-free, intraday **and** multi-day;
   realistic fills, costs, slippage, liquidity caps; walk-forward.
4. **Adversary/Validator atom** — actively tries to **kill** each candidate: out-of-sample,
   purged K-fold + embargo, multiple-testing correction (deflated Sharpe), regime & cost
   stress. The integrity guardian.
5. **Allocator/Portfolio atom** — combines *survivors* into a strategy targeting +1%/day;
   sizing, correlation, when to stand flat.
6. **Risk/Kill-switch atom** — drawdown limits, live-vs-backtest divergence, guardrails.
7. **Memory/Knowledge-Base atom** — persistent record of every fact and hypothesis
   (passed/failed + why) so nothing is re-tested and knowledge **compounds** across
   iterations. This is the self-learning core.
8. **Meta-Learner/Orchestrator atom** — runs the non-stop loop, prioritizes what to
   explore next (curiosity/bandit over the fact-space), tracks distance-to-goal, escalates.

## 7. The research lab (per agent, autonomous)
A self-contained environment: data access (point-in-time), the simulator, the validator,
the knowledge base, and a scoreboard. The agent's lab is **its own** — it designs and runs
its own experiments; it does not depend on hand-fed setups.

## 8. Self-improvement loop (one "iteration")
`mine facts → generate hypotheses → leak-free backtest → adversarial validation →
(survive?) add to portfolio + memory : log failure + reason in memory →
allocator re-optimizes → score vs +1%/day goal → meta-learner picks next frontier → repeat`
Runs forever. Each loop deepens the knowledge base; progress is monotonic in *knowledge*
even when a given hypothesis fails.

## 9. Leak-free protocol (non-negotiable backbone)
- **Point-in-time features** only; nothing computed from data past the decision instant.
- **Data splits:** Train (2022–2024) · Validation (2025) · Test (2026-H1) · **Vault
  holdout** (a locked slice the agent *never* sees until deployment). Walk-forward across all.
- **Anti-overfitting:** purged K-fold with embargo; deflated Sharpe / multiple-testing
  correction that *tightens as more hypotheses are tried*; regime robustness; parameter
  stability checks.
- **Realistic frictions:** costs, slippage, and **liquidity/again-fill caps** (a 1-min
  fill can't assume infinite size).
- **Labels:** no look-ahead, no survivorship; triple-barrier / horizon labels computed
  forward-only.

## 10. Success metrics & milestones
Primary: **OOS** average %/day, hit-rate of the +1% target, Sharpe/Sortino, max drawdown,
distance-to-goal. Milestones: M1 positive OOS expectancy → M2 ≥0.3%/day OOS → M3 ≥0.5%/day
→ M4 approach 1%/day. Every milestone must hold on the **untouched vault** before it counts.

## 11. Runtime / "non-stop"
Background loop process per agent, resumable, check-pointing to its knowledge base after
every iteration; a live scoreboard + periodic report. Resource/iteration budget per session
to be set. Survives restarts (state in the KB).

## 12. Risk & execution guardrails
- **Paper/research only** by default. **No autonomous real-money orders — ever — without
  your explicit, per-deployment authorization**, and even then order placement stays a
  human-in-the-loop step (consistent with account-safety rules).
- Kill-switch on drawdown / backtest-vs-paper divergence.

## 13. RESOLVED decisions (aligned)
- **Hold horizon:** intraday + positional up to **7 trading days**.
- **Vault holdout:** **all of 2026 (Jan–Jul, ~141 days) SEALED.** Train 2022–2024, validate 2025.
- **Runtime:** **parallel research bursts** (workflow-style fan-out of research atoms per round).
- **Capital basis:** **fixed ₹30,000 per agent, no compounding** (comparable to V1/V2).
- **Leverage:** **evaluate every hypothesis at BOTH 1× and 5×** — net-on-capital = leverage ×
  (gross price edge − round-trip cost). 5× makes +1%/day reachable on smaller price moves but
  multiplies drawdown 5×; the Risk atom enforces a hard daily/per-trade loss cap under 5×.
- **ADANIENT positional short:** **DROPPED** (unfundable at ₹30K). SELVI-AE = intraday L/S (MIS)
  + positional LONG (CNC, ≤7d). Both agents therefore share the same action space:
  intraday long/short + positional long only.
- **Execution:** paper/research only; live is human-authorized (unchanged).

## 14. Execution & Product Model (per your order-type rules — hard constraints)
The agents' action space is constrained by **product availability + the ₹30K cap**. A
hypothesis that proposes an impossible/unfundable order is **rejected at generation**.

| Product | Side | Horizon | Leverage | Notes |
|---|---|---|---|---|
| **CNC** (delivery) | LONG only | positional (o/n–7d) | 1× | delivery STT ~0.1% both sides |
| **MIS** (intraday) | LONG & SHORT | intraday, flat by EOD | ~5× | **short only via MIS**; auto-leverage; amplified risk |
| **FUT** (futures) | LONG & SHORT | intraday/positional | ~5–6× (SPAN) | **F&O stocks only**; positional short path |

**Per-stock action space:**
- **SELVI-AE (ADANIENT, F&O, lot 309):** intraday L/S (MIS 5×); positional **long** (CNC);
  positional **short** *only* via FUT — but 1 lot ≈ 309×~3000 ≈ ₹9.3L notional needing
  **~₹1.4L SPAN margin > ₹30K**, so **not fundable at ₹30K** (flagged; see chat decision).
- **SELVI-CT (CARTRADE, NON-F&O):** intraday L/S (MIS 5×); positional **long** (CNC);
  **positional short = NOT POSSIBLE** (no futures; CNC can't short). Agent must never propose it.

**Leak-free P&L realism:** correct per-product **sizing** (qty from ₹30K × product leverage
÷ price, or futures lot math), **costs** (MIS vs CNC-delivery vs FUT differ materially),
**margin**, and **liquidity caps**. Futures deep-history is unavailable (only ~3 months), so
positional-futures research uses the **cash series as price proxy** (basis-adjusted) — an
approximation disclosed in every such result; live would use the actual front-month.
