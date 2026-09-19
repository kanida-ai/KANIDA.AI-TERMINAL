# Market data pipeline + outcome evidence — build contract (v1, 2026-09-15)

Owner decisions (2026-09-15):
1. **Base interval is 15 minutes.** All strategies are 1H/4H/1D/1W; NSE 09:15–15:30 divides exactly into 15-minute bars, so 1H/4H/1D/1W aggregate from 15m with identical OHLC to 1m aggregation. 1-minute/5-minute tables stay as they are (replay, AutoTrade) — the pattern pipeline no longer reads them.
2. **Fast refresh, not a full re-fetch.** Incremental gap fill + targeted re-fetch of flagged symbols (~10 minutes), full history re-fetch only if the cross-check shows a wide problem.
3. **Universe: NIFTY 500** (the app's universe) first.
4. **Auto-authentication allowed** for the Kite token (existing `auto_auth_bot` path).
5. **Research rerun is ours** (this session), using the existing `market_scanner/pattern_research` runner.
6. Vendor later: **REST, 15-minute OHLCV, ~15-minute delay**. The swap must be a config change.

Related: `docs/pattern_research/EVIDENCE_SERVING_CONTRACT.md` (card/compat identity), `SOURCE_QUALITY_SCREEN.md` (37 flagged stocks / 75 datasets), `PIIND_SOURCE_AUDIT.md` (why row-level validation is required).

---

## 1. Provider interface (vendor-swappable)

Package `market_data/` in `C:\Users\SPS\Documents\Kanida_Falcon`.

```python
class MarketDataProvider(Protocol):
    provider_id: str           # 'kite' | 'vendor15' | 'fake'
    delay_seconds: int         # 0 for Kite historical, 900 for the delayed vendor
    max_days_per_request: dict[str, int]   # {'15minute': 200, 'day': 2000, ...}
    rate_limit_per_second: float           # global for the key (Kite ~3/s)
    def instruments(self) -> list[Instrument]: ...
    def candles(self, symbol: str, timeframe: str, start: datetime, end: datetime) -> list[RawCandle]: ...
    def latest_completed_bar(self, timeframe: str, now: datetime) -> datetime: ...
```

- `KiteProvider`: token from `kite_tokens` in `KANIDA_DB_PATH` (`C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\data\db\kanida_quant.db`), falling back to `.env`; on `TokenException` it triggers the project's existing auto-auth once, then retries (owner-approved). Never print or log token/credential values.
- `Vendor15Provider`: REST, 15-minute OHLCV, `delay_seconds=900`. Config-only switch: `MARKET_DATA_PROVIDER=kite|vendor15`.
- `FakeProvider`: fixtures for tests.
- One global rate limiter shared by all workers (token bucket at `rate_limit_per_second`); extra workers must not exceed it.

**Conformance suite** (`market_data/tests/test_conformance.py`): any provider must pass — bar alignment to the 15-minute grid, no overlapping/duplicate bars, session boundaries, `candle_complete` correctness, delay honoured, missing data reported rather than fabricated, idempotent re-fetch of the same window.

## 2. Normalized candle + stores

New database `db/market15.db` (NEVER write to the 158 GB `db/kanida.db`; read-only there):

- `candles_15m(instrument_id, symbol, exchange, bar_start, bar_end, open, high, low, close, volume, candle_complete, quality_flags, adjustment_basis_id, vendor_id, vendor_revision, fetched_at, snapshot_id, revision)` — PK (instrument_id, bar_start, revision); latest revision wins.
- `raw_archive(request_id, provider, symbol, timeframe, start, end, fetched_at, sha256, payload_path)` — gzipped raw vendor payloads on disk. This is the provenance the PIIND audit could not find.
- `corrections(id, symbol, timeframe, bar_start, field, old_value, new_value, reason, evidence_request_id, run_id, created_at)` — every change, never an in-place silent edit.
- `snapshots(snapshot_id, created_at, universe, adjustment_basis_id, provider, first_bar, last_bar, symbol_count, checksum)` — frozen inputs for a research run.
- `ingest_runs(run_id, started_at, finished_at, provider, plan, requests, rows, errors, status)`.

**Aggregation** (`market_data/aggregate.py`): 15m → 1H (4 bars) → 4H (16 bars, buckets 09:15–13:15 and 13:15–15:30 to match the frozen research) → 1D (session) → 1W (Mon–Fri). Rules: OHLC from constituent bars, volume summed, incomplete trailing bucket marked `candle_complete=false`, holidays/half-days from the session calendar, no synthetic bars.

### 2A. Session regimes — NSE Closing Auction Session (CAS), from 2026-08-03

SEBI/NSE introduced a Closing Auction Session on **2026-08-03**. Phase 1 covers **cash-segment stocks that also have F&O contracts**. Verified live on 2026-09-15 (Kite 15-minute bars, single-session windows):

| Regime | Session (continuous) | 15m bars/day | Official close |
|---|---|---|---|
| Any stock, before 2026-08-03 | 09:15–15:30 | 25 | VWAP of the last 30 minutes |
| **CAS stock**, from 2026-08-03 | **09:15–15:15** | **24** | **auction equilibrium price (15:30–15:35), not a traded bar** |
| Non-CAS stock, from 2026-08-03 | 09:15–15:30 | 25 | VWAP of the last 30 minutes |

Evidence: 360ONE/ABB/ABCAPITAL (F&O) = 25 bars on 2026-07-31 → 24 bars on 2026-09-15; 3MINDIA/AADHARHFC/AARTIIND/AAVAS/ABBOTINDIA/ABDL (non-F&O) = 25 bars on both dates. Intraday volume runs ~0.9–2.7% below the daily bar for CAS stocks (the auction volume).

Consequences that every module must honour:
- **Expected bar count is per symbol AND per date.** A flat "25 bars/day" rule would falsely flag every F&O stock after 2026-08-03. Derive the regime empirically per symbol/day (last bar start 15:00 ⇒ CAS) and cross-check with `instrument_labels.is_fno`; store it as a session-regime record rather than trusting a static list.
- A bar that runs to the regime's session end is **complete**, not truncated. `candle_complete=false` is only for a genuinely missing bar.
- **The official close of a CAS stock does not exist in the intraday series.** Take 1D/1W closes from the provider's daily bars; never assume the last intraday close equals the daily close (this was already true pre-CAS because of the 30-minute VWAP rule, and is larger now).
- The 4H bucket 13:15–15:30 covers 13:15–15:15 for CAS stocks after 2026-08-03; that is the whole remaining session, so it is complete under its regime.
- **Research history (to 2026-07-31) predates CAS**, so the frozen runs need no CAS handling. Only refreshed/live data does.
- The vendor checklist must ask: does the 15-minute feed cover CAS stocks to 15:15 or 15:30, and is the auction price delivered as a separate bar, folded into the last bar, or only in the daily bar?

**Validation before aggregation** (the gap that let PIIND through):
1. intrabucket discontinuity: any row whose price deviates > X% from the bucket median (flag, don't delete);
2. intraday vs daily reconciliation: intraday low ≥ 0.8 × daily low, high ≤ 1.2 × daily high, same session and adjustment basis;
3. `low ≤ min(open, close)`, `high ≥ max(open, close)`, positive finite prices;
4. session length (expected bar count per day), duplicates, zero-volume prints with price moves;
5. cross-timeframe agreement with the provider's own daily bars.
Failures set `quality_flags`; they never silently drop or invent rows.

## 3. Repair workflow (what "fix the data" means)

1. **Diagnose (read-only)** across NIFTY 500 in `kanida.db`: per-symbol/timeframe candidate bad rows with evidence.
2. **Re-fetch** the disputed windows (and full 15m history for the 37 flagged symbols) via the provider; archive raw payloads.
3. **Classify** each disagreement: source error · corporate-action/adjustment basis difference · genuine extreme.
4. **Write corrections as a new revision** with reason + evidence id. Keep old rows.
5. **Quarantine what cannot be resolved**: LTIM, GSPL, GUJGASLTD, JBCHEPHARM (invisible to this Kite account) and the FORCEMOT 2023-10-26..2024-02-13 suspension gap — labelled, never invented.
6. **Freeze a snapshot** (`snapshots` row + checksums) and rerun research against it.

## 4. Outcome evidence: "what happened next" (new)

For **every historical occurrence** of a (pattern, variant, side, timeframe, symbol), computed point-in-time:

- **Forward path**: net return at each horizon h = 1..H bars after the signal, entry at the next eligible open, costs 0.40% round trip applied once at the measured horizon.
- **MFE / MAE**: the maximum favourable and maximum adverse excursion up to each h, in percent from entry (using bar highs/lows, not closes).
- **Horizon grid by timeframe** (bars after entry): 1H → 1..30 · 4H → 1..20 · 1D → 1..10 · 1W → 1..8. Also report the equivalent session/calendar span for display. Confirm/adjust the grid from the data before freezing it.
- **Availability rule (point-in-time law)**: an occurrence counts at horizon h only if `entry_idx + h` exists in history at decision time (`available_from_idx = entry_idx + H - 1 <= today_idx` for the full grid).
- **Aggregate per (pattern, variant, side, timeframe)**: n, mean/median net return per horizon, win rate per horizon, median MFE/MAE curve, MFE/MAE ratio, and the distribution (p10/p25/p50/p75/p90), all with sample-size labels.
- **Best holding period**: chosen **on training folds only** (existing rolling 36m train / 6m test protocol), then reported out-of-sample on the test folds. Report the in-sample choice and the out-of-sample result separately; never present the in-sample best as the expected return.
- **Output**: per-occurrence rows (for the chart/evidence UI) and a per-cell summary joined to the existing research cell keys, stored beside the research store, not in the app DB.

The user-facing answer this produces: *when this pattern appeared, what happened next, how far it moved in our favour, how far against us, and which holding period historically worked for this exact pattern and timeframe.*

## 5. Live detection (after the data lands)

- The live scanner runs the **same versioned detectors** as the research on completed candles from `candles_15m`.
- Lifecycle per detection: `forming` → `confirmed` → `invalidated` → `expired`; keys are timestamps + spec identity, not bar indices.
- **Warm-up/parity gate before switching on**: compare recent events from full-history detection against start-truncated windows per family and timeframe; declare the required warm-up bars. No blanket "last 260 candles".
- Freshness = latest expected completed exchange bar + provider `delay_seconds`. A 15-minute delayed vendor reads as "delayed 15 min by design", not "stale".

## 6. Rules that do not bend

Point-in-time only; entry at next open; costs + slippage on every simulated trade; expectancy (not win rate) decides; never fabricate or silently patch prices; label small samples; a withheld source-quality release must never become the active evidence release.
