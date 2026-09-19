# KANIDA — Per-Stock Pattern Playbook (v1)
*Frozen from the ADANIENT + CARTRADE run. Repeat this identically for every new stock.*

The rule that governs everything: **the sealed 2026 year is the truth. We never fabricate a passing result.**
All work is paper/research only. Real-money order placement stays human-gated.

---

## 0. Parameters (fixed constants — do not change per stock)

| Item | Value |
|---|---|
| Fixed capital per stock | **₹1,00,000 = MARGIN / CAPITAL DEPLOYED** (not notional) |
| Leverage by product | **MIS 5x · CNC 1x · NRML-futures 5x** (5x = ~20% SPAN+exposure margin — assumption; replace with live broker margin API before real sizing) |
| Round-trip cost (% of notional) | MIS 0.08% · CNC 0.15% · NRML 0.05% |
| Data split | Train **≤ 2024** · Validate **2025** (promote) · **2026 = SEALED vault** (never used for selection) |
| Reversion touch-targets | up/dn × {1%/1d, 2%/2d, 5%/5d} — "forward high/low touches ±X% within N days" |
| Entry timing | **Next day's 09:15 open** after the pattern fires at the prior day's close (point-in-time) |
| Exit policy (reversion) | **fixed_target** — exit at target touch, else time-stop at window end / MIS 15:20 (proven optimal; trailing/stops tested & rejected for reversion) |
| Intraday order-type | **Dual: MIS (5x) AND CNC (1x) shown side-by-side.** Same price fill; trader picks lane by risk profile (MIS = high ROC, CNC = stability). |

---

## 1. Data prerequisites (must exist in `db/kanida.db`)
- `ohlc_daily`, `ohlc_5min`, `ohlc_1min` — 2020→date, split/bonus-adjusted, + `atp` (running intraday VWAP).
- `ohlc_futures_{daily,5min,1min}` — for F&O names (currently only ~near-dated; **full-history futures is the known gap** for overnight shorts).
- `instrument_labels` — `is_fno`, `sector`, `lot_size`, `kite_token`, Nifty membership.
- `event_markers`, `corp_actions` (NSE) — context/filters.

## 2. Feature build — point-in-time only (`features()` in `mine_phase1.py`)
~35 leak-free features: daily technicals, **weekly WTD** (shift(1)), intraday-derived-from-1min (incl. `id_close_vs_vwap` using true day-VWAP = last `atp`). Every feature known at close of day *t*; labels look forward only.

## 3. Mining — outcome-first, per (stock × target) (`mine_phase1.py`)
- `RandomForestClassifier(n_estimators=60, max_depth=3, min_samples_leaf≈25)` per target.
- Extract **leaf-rule conjunctions** (combination patterns), dedup by rule signature.
- **Train gate:** occurrence n ≥ 40 AND lift ≥ 5pp (lift = precision − base_rate).
- Write every survivor to `KANIDA_SNR.db.mined_patterns` with `scope='stock_specific'`.

## 4. Promotion — validate on 2025 (still `mine_phase1.py`)
- `promoted = 1` if val 2025: n ≥ 15 AND lift ≥ 2pp AND precision > base. **2026 stays sealed.**

`mined_patterns` columns: `Stock, pattern_id, mined_year, scope, outcome_target, n_obs, n_hits, precision_pct, base_rate_pct, lift_pct, depth, rule_text, rule_json, promoted` + added: `status, oos2026_n, oos2026_hits, oos2026_precision_pct, oos2026_base_pct, oos2026_lift_pct`.

## 5. Sealed-2026 confirmation + lifecycle tagging (`confirm_and_trade.py`)
Re-score each promoted pattern on the sealed 2026 year, write `oos2026_*`, and tag `status`:
- **Keep** — n ≥ 10, lift ≥ 3pp, precision > base (held the sealed year)
- **Watch** — n ≥ 3, lift > 0 (marginal/thin)
- **Retire** — fails on 2026
- **Test** — never promoted (didn't hold 2025)

## 6. Product-type routing — real Zerodha rules (your instruction, non-negotiable)
| Signal | Product | Instrument | Leverage | Notes |
|---|---|---|---|---|
| window = 1d | **MIS + CNC (both)** | cash | 5x / 1x | intraday long **or** short; forced square-off **15:20**; **both lanes computed side-by-side** |
| long, >1d | **CNC** | cash | 1x | delivery, hold overnight |
| short, >1d | **NRML** | stock **futures** | 5x | cash overnight short is **illegal** |
| short, >1d, **not F&O / no futures data** | — | — | — | **NOT EXECUTABLE → dropped** (logged, excluded from P&L) |

## 7. Trade simulation — exact 1-min timing (`simulate()`)
- **Entry:** first 1-min bar (09:15) open of the entry day → `entry_time`.
- **Target exit:** the **exact minute** the 1-min high/low touches the target → `exit_time`.
- **Time-stop:** window's last day 15:29 close (or MIS 15:20 square-off) if target not hit.
- **MFE / MAE:** tracked across the holding window from 1-min bars.
- Sequential single book per stock (one position at a time; advance past exit).

## 8. Trade log — every micro-detail, DUAL lane (`trade_log_rev` table)
Price-level (product-agnostic): `Stock, pattern_id, outcome_target, direction, signal_date, entry_date, entry_time, entry_px, target_px, exit_date, exit_time, exit_px, exit_reason, holding_days, gross_move_pct, mfe_pct, mae_pct`
- **laneA** (primary; MIS for intraday, else the actual product): `laneA_product, laneA_leverage, laneA_cost_pct, laneA_net_roc_pct, laneA_qty, laneA_pnl_rs, laneA_win`
- **laneB** (alternative; CNC for intraday, NULL for multi-day): `laneB_product, laneB_leverage, laneB_cost_pct, laneB_net_roc_pct, laneB_qty, laneB_pnl_rs`
- `net_roc_pct` = (gross_move − cost) × leverage (return on the ₹1,00,000 deployed).

## 9. Trade journal — aggregate, MIS vs CNC side-by-side (`trade_journal_rev` table)
`trades, intraday_n, multiday_n, intraday_mis_pnl_rs, intraday_mis_roc_pct, intraday_cnc_pnl_rs, intraday_cnc_roc_pct, multiday_pnl_rs, multiday_roc_pct, total_mis_lane_roc_pct, total_cnc_lane_roc_pct, win_rate_pct, profit_factor, avg_mfe_pct, avg_mae_pct, avg_hold_days, max_drawdown_rs, keep_patterns, dropped_not_executable, verdict`
- Intraday shown **both ways** (MIS high-ROC lane vs CNC stability lane); multi-day unaffected.
- Win%/PF/maxDD computed on the MIS lane (the book).

## 10. Decision gate (per stock)
1. **Baseline = reversion fixed_target** (this playbook). Record the sealed-2026 ROC.
2. Any alternative (trend/barrier family, trailing/target_stop exits) must **beat the baseline on sealed 2026**, or it is rejected and we keep the baseline.
3. Tag the stock: **Tradeable** (baseline profitable on sealed 2026) or **Park** (no edge).

## 11. Integrity rules (always)
- 2026 is opened **only** for the final confirmation — never for selection/tuning.
- Never claim a subset selected *using* 2026 (look-ahead) — that's a fake pass.
- Costs & drawdown scale with leverage; report DD honestly (5x edge = 5x drawdown).
- Real-money orders human-gated; everything here is paper/research.

---

## Reference outcome — ADANIENT + CARTRADE (what this playbook produced)
| Stock | Baseline (reversion, fixed_target, leveraged) | Trend family (best) | Verdict |
|---|---|---|---|
| **ADANIENT** | **+81.4% ROC** (76.5% win, PF 2.2, maxDD −18%); MIS-intraday-driven (dn_1pct_1d 92% win) | −2.9% | **Tradeable** — baseline locked |
| **CARTRADE** | −46.7% (not F&O → long side fails) | −3.5% | **Park** — no edge on sealed 2026 |

**Canonical executor:** `scripts/run_playbook.py` runs §2–§10 for each stock (dual MIS/CNC intraday, verdict Tradeable/Park), resumable.
`python run_playbook.py SYM1 SYM2` (specific) or `python run_playbook.py` (full F&O universe). Writes `trade_log_rev`, `trade_journal_rev`, `stock_verdict` in `db/KANIDA_SNR.db`.
Verdict: **Tradeable** if sealed-2026 MIS-lane ROC > 0 AND PF ≥ 1.2, else **Park**.
**Other scripts:** `mine_phase1.py` (§2–4 mining) · `confirm_and_trade.py` (2-stock detail, 3 exit policies) · `phase2_trend_batch.py` (trend/barrier challenger test).
