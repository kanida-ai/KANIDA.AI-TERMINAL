# SPS_V5 — Master Portfolio Agent · Spec (draft for alignment)

> A portfolio/risk **overlay** on the per-stock SELVI alpha agents. It does the
> portfolio-level decisions no single-stock agent can see. Paper/research only;
> real-money execution stays human-authorized.

## Role
Take the vault-confirmed per-stock edges as **inputs** and decide **which to trade, how
much, and when to stand down** — to maximize *risk-adjusted harvest* of the edge that
exists, with hard scalability + risk guardrails.

## PRIMARY objectives (locked with you)
1. **Selection & allocation.** Choose a top-N basket by rolling **out-of-sample** edge
   strength + low correlation. N is tunable (10 / 15 / 20 / …). **HARD FLOOR: N ≥ 10** —
   the agent may *never* go below 10 names, to preserve **capital scalability** and
   diversification. Allocate capital across the N (equal → risk-parity/conviction-weighted,
   whichever validates). Per-name and per-factor exposure caps.
2. **Basket de-risk.** Aggregate risk management the per-stock agents can't do:
   - daily basket **loss limit** (cut the day if aggregate P&L ≤ −X),
   - basket-level **trailing stop** on the day's peak P&L,
   - total-exposure cap; a **kill-switch** on drawdown / live-vs-backtest divergence.

## SECONDARY objectives
- **Regime gate** — one market-level risk-on/off (breadth/volatility) to turn the whole
  book down on unfavorable days (the July-2026 failure mode).
- **Promote/demote** — rotate names in/out on rolling OOS performance (drop decaying edges),
  always respecting the N≥10 floor.
- **Meta-allocation across edge families** — once a *second* (uncorrelated) book exists,
  allocate between them.

## Architecture — same self-improving two-layer split as V4
| Layer | Who changes it | Contents |
|---|---|---|
| **Mutable policy layer** | the agent authors & evolves | selection rules, weighting schemes, regime gates, de-risk policies |
| **Immutable integrity core** | LOCKED | walk-forward eval, **sealed-vault gauntlet**, cost model, the N≥10 floor, risk limits |

Every authored portfolio policy is admitted **only after** clearing walk-forward + the
sealed-vault gauntlet. The agent cannot peek at the vault, bypass validation, or breach
the N≥10 floor / risk caps (enforced in the immutable core).

## The four capabilities (bounded, same as V4)
- **Recursive self-improvement** — evolves allocation/regime/de-risk policies; composes
  survivors; meta-learns which policy families hold OOS.
- **Self-modifying code, training-only, leak-free** — authors policies during research;
  deployed policy is a frozen artifact; gauntlet + vault admit it.
- **Autonomous** in research/paper; real-money order placement human-gated.
- **Resource accrual** — uses per-agent signals, data, allowlisted libs; accrues a library
  of validated portfolio policies. No autonomous credential/account/money acquisition.

## Inputs / Outputs
- **In:** per-stock signals + vault-confirmed edge stats + rolling performance + market
  regime series.
- **Out:** the live basket (which ≥10 stocks + weights), the day's risk state, de-risk actions.

## Honest limit
V5 maximizes **consistency, drawdown control, and risk-adjusted return**, plus a modest
return lift from smarter allocation/regime timing. It **cannot manufacture gross alpha** —
turning +0.155%/day (1×) into +1%/day needs **more uncorrelated edge families**, which V5
then combines. Its real wins: survive regime shifts (fix July), cut the 5× drawdown, and
scale capital cleanly across ≥10 names.

## Success metrics
Portfolio Sharpe/Sortino, max drawdown, positive-day rate, regime-conditional return,
capacity (₹ deployable at target slippage), and %/day — all measured on the **sealed vault**.
