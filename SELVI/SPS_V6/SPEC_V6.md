# SPS_V6 — Autonomous Alpha-Discovery System (the best-of-best) · Spec

> The frontier agent: open-ended symbolic alpha search + LLM researcher + a
> multi-agent adversarial society + ensemble stacking, all forced through an
> immutable, ruthless anti-overfitting core. Paper/research only; real money gated.
> Mission (non-negotiable): pursue **+1%/day** with everything, and never fake it.

## Why this is a real leap over V1–V5
V1–V5 searched a *hand-coded* grammar (a few triggers × a few filters). V6 searches a
space **thousands of times larger and richer**, validates **far harder**, and **combines**
many weak edges into one strong return. Three capabilities define "best": **discover**,
**destroy-your-own-false-findings**, **combine**.

## The 7 subsystems

### 1. Open-ended symbolic alpha search (mutable)
Genetic-programming / program-synthesis over a rich operator set — `ts_rank, delta,
decay_linear, correlation, scale, cross-sectional rank, ts_argmax, signed_power`, … — that
composes **formulaic alphas** from raw OHLCV + microstructure (ATP/VWAP, volume, spread
proxies). This is WorldQuant-101 / AutoAlpha-style mining: the agent *invents signal
expressions* it was never taught, far beyond triggers+filters.

### 2. LLM Researcher agent (mutable)
An LLM proposes **novel economic hypotheses** and feature ideas (not just recombinations),
reads experiment results, forms theories, and *directs* the symbolic search — a real
researcher in the loop, not a grammar. (Runs via the workflow/agent layer.)

### 3. Multi-agent adversarial society (mutable)
Specialised agents that debate and critique: **Researcher → Feature Engineer → Backtester
→ Red-Team/Adversary → Portfolio Manager → Risk Manager**. The **Adversary's only job is
to break every candidate** (regime slicing, cost stress, look-ahead audits, refutation).
An edge ships only if it survives the attack.

### 4. Immutable anti-overfitting core (LOCKED — the skeptic engine)
The more you search, the more critical this is. Enforces: **point-in-time data + sealed
vault**, **combinatorially-purged K-fold with embargo**, **Deflated Sharpe Ratio** and
**Probability of Backtest Overfitting (PBO)** that *tighten with the number of trials*,
realistic cost/slippage, and the risk limits. The agent **cannot touch, peek, or bypass**
this. This is what makes a "yes" trustworthy at scale.

### 5. Ensemble / stacking meta-model (mutable) — the return lever
Combines **hundreds of weak validated alphas** into one robust portfolio signal
(non-linear stacking, correlation-aware). This is where materially higher return comes
from — many small real edges beat one big fragile one. The V5 master allocator lives here.

### 6. Meta-learner / compute allocator (mutable)
Bandit/curiosity over the search space; learns *which* operator families and search
strategies pay off; builds a curriculum from simple → complex; allocates compute to the
frontier. Self-improvement of the search itself.

### 7. Continual / online adaptation (mutable)
Walk-forward retraining, **edge-decay detection + retirement** (the July-2026 failure
mode), regime adaptation — all leak-free.

## Two-layer safety (unchanged, non-negotiable)
Everything above the core is **mutable** (the agent authors/evolves it). The **anti-overfit
core is immutable** — SEAL-hashed, agent-untouchable. Real-money order placement is
**human-gated**. Resources on an **allowlist**; no autonomous credential/account/money
acquisition. Self-modification happens **only in training**; deployed strategies are frozen,
vault-confirmed artifacts.

## The four capabilities, at full strength
- **Recursive self-improvement** — the meta-learner improves the search; alphas compose
  into better alphas; policies compose into better policies.
- **Self-modifying code, training-only, leak-free** — authors operators, alphas, policies;
  admitted only through the immutable skeptic core.
- **Autonomous** — full in research/paper; live gated.
- **Resource accrual** — data, allowlisted libs, compute, and a growing **library of
  validated alphas + policies** (compounding knowledge capital).

## Honest ceiling (said once, held always)
V6 maximises how much *real* edge we find, how *little* of it is fake, and how *well* it's
combined. It is bounded by the market's extractable alpha net of costs — no agent escapes
that. But this is the version that gives the +1% goal its best genuine shot: a far larger
search, a far harsher validator, and an ensemble that stacks every real edge we find. The
vault decides the truth at every step; the agent never stops, and never lies.

## Build order (highest leverage first)
1. **Symbolic alpha miner** (subsystem 1) + **ensemble** (5) on the immutable core (4) —
   the real return leap, concretely implementable now.
2. **Adversary/Red-Team** (3) — hardens survivors.
3. **LLM Researcher** (2) — creative hypothesis generation via the workflow layer.
4. **Meta-learner** (6) + **continual adaptation** (7).
