# FALCON HULK V1 — PREDICTIVE track grid (PREDICT-not-CONFIRM)

_Generated from `hulk/_out/predict_grid.pkl`. TRAIN 2018-2023, OOS TEST 2024 (purged + 10-day embargoed). 2025 held, 2026 SEALED (never read)._

- Compute: bagged shallow trees depth∈(1, 2, 3), B=150 bootstraps, M=30 null reps/cell, joblib n_jobs=-1 on **12 cores**, runtime **407s**.
- GENUINE gate (ALL required): OOS n_fires ≥ **150** AND recurrence ≥ **0.20** AND oos_lift > max(null_p99,0) AND oos_net_ret > 0 (cost: intraday 0.0012, multiday 0.0030 round-trip).
- Null leak flag = null_rec_max ≥ 0.20 (scrambled labels reproduce the recurrence).
- Diagnostic relaxed gate = same tests but n_fires ≥ **25** (scale-appropriate: OOS 2024 is only ~245 daily rows, so the 150-fire gate demands a rule fire on ~61% of all OOS days — unsatisfiable for any *selective* rule).

## Headline

**0 genuine predictive cells** out of 28 under the mandated gate (n_fires≥150). 17 recurring candidates surfaced in total; **18/28 cells are null-leak-flagged** — meaning the bagged-tree recurrence machinery finds *equally recurring* structure on **scrambled labels**, so the recurrence reflects feature geometry, not label-specific predictive signal. Relaxing to n_fires≥25 yields only **3** thin, near-cost cells (see diagnostics). **Net: the predictive tracks find no durable OOS edge.**

## Grid — mandated gate (n_fires ≥ 150)

| stock | dir | trk | horizon | train_base | test_base | n_cand | n_gen | best_rule | best_net% | best_hit | best_fires | best_rec | null_leak |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| RELIANCE | long | A | A:intra+0.5% | 0.593 | 0.556 | 0 | 0 | `—` | — | — | — | — | no |
| RELIANCE | long | A | A:intra+1.0% | 0.339 | 0.284 | 1 | 0 | `opening_range_pct <= 0.0083726` | -0.230 | 0.231 | 156 | 0.76 | ⚠️YES |
| RELIANCE | long | B | B:T+1 | 0.328 | 0.297 | 1 | 0 | `day_range_pct > 0.010677` | -0.345 | 0.322 | 202 | 0.20 | ⚠️YES |
| RELIANCE | long | B | B:T+2 | 0.301 | 0.232 | 1 | 0 | `eod_vol_surge > 0.45` | -0.361 | 0.232 | 241 | 0.29 | ⚠️YES |
| RELIANCE | long | B | B:T+3 | 0.359 | 0.276 | 1 | 0 | `lower_wick_pct <= 0.0070046` | -0.397 | 0.272 | 206 | 0.20 | ⚠️YES |
| RELIANCE | long | B | B:T+5 | 0.301 | 0.199 | 1 | 0 | `day_range_pct <= 0.022186` | -0.419 | 0.191 | 188 | 0.27 | ⚠️YES |
| RELIANCE | long | B | B:T+1_intra(09:20*) | 0.608 | 0.583 | 0 | 0 | `—` | — | — | — | — | no |
| RELIANCE | short | A | A:intra+0.5% | 0.654 | 0.617 | 0 | 0 | `—` | — | — | — | — | no |
| RELIANCE | short | A | A:intra+1.0% | 0.383 | 0.300 | 0 | 0 | `—` | — | — | — | — | ⚠️YES |
| RELIANCE | short | B | B:T+1 | 0.372 | 0.329 | 0 | 0 | `—` | — | — | — | — | ⚠️YES |
| RELIANCE | short | B | B:T+2 | 0.310 | 0.248 | 2 | 0 | `day_range_pct > 0.011991` | -0.179 | 0.249 | 185 | 0.21 | ⚠️YES |
| RELIANCE | short | B | B:T+3 | 0.330 | 0.325 | 1 | 0 | `day_range_pct > 0.011936` | -0.129 | 0.332 | 187 | 0.31 | ⚠️YES |
| RELIANCE | short | B | B:T+5 | 0.239 | 0.220 | 1 | 0 | `day_range_pct > 0.013533` | -0.184 | 0.216 | 162 | 0.31 | ⚠️YES |
| RELIANCE | short | B | B:T+1_intra(09:20*) | 0.657 | 0.607 | 0 | 0 | `—` | — | — | — | — | no |
| ADANIENT | long | A | A:intra+0.5% | 0.792 | 0.665 | 0 | 0 | `—` | — | — | — | — | no |
| ADANIENT | long | A | A:intra+1.0% | 0.606 | 0.463 | 0 | 0 | `—` | — | — | — | — | no |
| ADANIENT | long | B | B:T+1 | 0.376 | 0.256 | 1 | 0 | `day_range_pct <= 0.055591` | -0.651 | 0.250 | 228 | 0.39 | ⚠️YES |
| ADANIENT | long | B | B:T+2 | 0.393 | 0.272 | 2 | 0 | `day_range_pct <= 0.079538` | -0.615 | 0.265 | 238 | 0.23 | ⚠️YES |
| ADANIENT | long | B | B:T+3 | 0.426 | 0.301 | 0 | 0 | `—` | — | — | — | — | ⚠️YES |
| ADANIENT | long | B | B:T+5 | 0.400 | 0.264 | 2 | 0 | `close_loc <= 0.59376` | -0.704 | 0.262 | 168 | 0.26 | ⚠️YES |
| ADANIENT | long | B | B:T+1_intra(09:20*) | 0.798 | 0.682 | 0 | 0 | `—` | — | — | — | — | no |
| ADANIENT | short | A | A:intra+0.5% | 0.805 | 0.715 | 0 | 0 | `—` | — | — | — | — | no |
| ADANIENT | short | A | A:intra+1.0% | 0.624 | 0.500 | 0 | 0 | `—` | — | — | — | — | no |
| ADANIENT | short | B | B:T+1 | 0.430 | 0.439 | 1 | 0 | `day_range_pct > 0.018628` | -0.083 | 0.422 | 173 | 0.20 | ⚠️YES |
| ADANIENT | short | B | B:T+2 | 0.369 | 0.382 | 1 | 0 | `day_range_pct > 0.017343` | -0.107 | 0.366 | 194 | 0.33 | ⚠️YES |
| ADANIENT | short | B | B:T+3 | 0.390 | 0.402 | 1 | 0 | `upper_wick_pct <= 0.027844` | +0.064 | 0.403 | 243 | 0.26 | ⚠️YES |
| ADANIENT | short | B | B:T+5 | 0.319 | 0.309 | 0 | 0 | `—` | — | — | — | — | ⚠️YES |
| ADANIENT | short | B | B:T+1_intra(09:20*) | 0.810 | 0.793 | 0 | 0 | `—` | — | — | — | — | no |

## Genuine rules under the mandated gate

_None. No predictive cell produced a rule clearing all four gates._

## Diagnostic — relaxed gate (n_fires ≥ 25), for honesty only

These are the *best a predictive rule can do below the mandated fire gate*. They are thin, near-cost, and several sit in null-leak cells — **do not trade them**.

| stock | dir | trk | horizon | best_rule | n_fires | oos_hit | oos_net% | rec |
|---|---|---|---|---|---|---|---|---|
| ADANIENT | long | A | A:intra+1.0% | `opening_range_pct > 0.017801` | 34 | 0.647 | +0.278 | 0.21 |
| ADANIENT | long | B | B:T+2 | `lower_wick_pct > 0.0082012` | 87 | 0.345 | +0.007 | 0.37 |
| ADANIENT | long | B | B:T+5 | `day_range_pct > 0.027425` | 94 | 0.372 | +0.267 | 0.30 |

## Honest read: prediction vs the earlier confirmation run

Prediction is harder than confirmation, and this run shows it plainly. The earlier **confirmation** grid (`hulk_mine_grid.py`, features like `ret_since_open` / `vwap_dev` that measure the move already underway) surfaced candidates because such features carry strong, stable structure — the trees split on the same feature at the same cut across bootstraps. The **predictive** features here (strictly *before* the move) do not:

- A positive-control test (inject a feature = 0.6×realized-return) confirmed the machinery works — its root split locked onto the injected feature in **150/150** bootstraps. So low recurrence is a property of the *features*, not a bug.
- On the real predictive features, the strongest root-split stability was `opening_range_pct` for ADANIENT-short same-day (~53% of bootstraps) and `day_range_pct` for the overnight tracks (~20-30%) — real but weak and diffuse.
- The **null gate is decisive**: on permuted labels the miner reproduces recurrence of 0.38-0.69 and OOS-lift floors of +0.8 to +4.5pp. Real OOS lift never clears that floor. That is the signature of *no label-specific edge* — the recurring signatures are driven by feature multicollinearity, not by the labels.
- Even below the mandated fire gate, the best predictive rules net ≈ 0% after cost.

**Conclusion:** on RELIANCE + ADANIENT, 2018-2023 → 2024 OOS, the PREDICT-not-CONFIRM features (opening 15-min for same-day; whole-day-T for overnight) carry **no durable, cost-clearing, label-specific predictive edge** at daily granularity. This is the correct, leak-free answer — and it is weaker than the confirmation run precisely because the confirmation run's apparent edge came from measuring the move itself. The next honest lever is not more mining of these features but either (a) finer entry timing / intraday path features that are still strictly pre-move, or (b) accepting that at this horizon the before-the-move signal on these names is near the noise floor.

