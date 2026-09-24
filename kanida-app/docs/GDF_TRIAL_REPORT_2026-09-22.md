# Global Datafeeds trial — final report (live session 22 Sep 2026)

Production ran on Kite all day, untouched (26/26 readings ok, 216/216 snapshots per reading). The vendor ran in a
separate process and store: `kanida-app/docs/gdf_trial/vendor_shadow.db`, one vendor session, 09:10–16:00 IST.
Raw evidence: `gdf_polls_2026-09-22.csv` (2,306 polls), `GDF_LIVE_2026-09-22.md`, `VENDOR_VS_KITE_2026-09-22.md`,
`GDF_FIELDS_2026-09-22.md`, `GDF_EXPERIMENTS*_2026-09-22.json`.

## Verdict

| # | Check | Result | Evidence |
|---|---|---|---|
| 1 | Vendor setup (auth, exchanges, key in git-ignored `.env`) | **PASS** | NFO 100 / NSE 95 / NSE_IDX 5, DataDelay 900, 3,600 calls/h, key expiry 23 Sep 23:59:59 IST |
| 2 | Live data during market hours | **PASS** | every symbol served all session; 0 fast-lane errors in 573 refreshes |
| 3 | "15-min delayed" vs "15-min candles" | **PASS — both, and different** | 1-minute data is 15 min behind (newest minute 15.0–16.0 min old in 444 refreshes). 15-minute candles arrive **complete soon after candle close**: from 10:45 on, all 200 symbols, first seen a median 16–79 s after close (earliest 4 s; the spread is mostly our own polling phase). 1 value revision in the whole day |
| 4 | Is the 15-min candle the whole quarter? | **PASS** | rebuilt from its fifteen 1-minute pieces: **315 of 315 exact** (O/H/L/C/volume/OI) across stocks, stock future & options, indices, index futures & options; 10 not comparable (candle not fetched early in the day), **0 mismatches** |
| 5 | Agreement with Kite | **PASS** | futures, equities, indices: median price gap 0.01–0.03%, ≥99.9% within 0.5%. Options vs the vendor minute at the Kite mark: Kite's price lies inside the vendor minute's high–low in **1,668 / 1,730**; OI median difference **0.000%** |
| 6 | Field sufficiency | **PARTIAL** | OHLC, volume, OI, bar time, token, lot, strike, expiry, price bands, 52-w high/low, expiry list. **No** bid/ask, depth, ATP, previous close, IV/Greeks (we compute IV/Greeks, as on Kite). No trade in a minute ⇒ no candle (vendor-confirmed): carry forward last value |
| 7 | Coverage: stocks / indices / futures / options | **PASS within trial caps** | all four kinds served; whole-exchange snapshot returned all traded NFO contracts (~3,000–3,900 per minute) beyond the 100-symbol cap |
| 8 | IST vs Seattle | **PASS** | vendor epochs rendered in IST; pairing with Kite's IST marks exact at every reading; host clock (Seattle) never used for market time |
| 9 | Equity ingest through the adapter | **PASS** | `market_data.live.cli --provider gdf once` → 3 requests, 75 rows (25 bars × 3 stocks), 0 errors, latest 15:15, scratch DB |
| 10 | Refresh under 30 s | **PASS for ≤200 symbols, FAIL for the full chain via polling** | see below |
| 11 | KANIDA engine run on vendor data (NIFTY/BANKNIFTY headlines) | **NOT DONE** | the trial's 100-contract cap gives ATM±8 only; PCR/max pain need the full chain. Needs the paid-plan cap or a full-chain build from exchange snapshots |

## Speed — what each way of fetching costs (measured)

| Method | Per call | All 200 trial symbols | Full chain (27,471 contracts) |
|---|---|---|---|
| `GetSnapshot`, **≤25 symbols per call** (>25 = no reply at all) | 0.5–1.3 s | **9.2 s avg, 11.7 s p95**, 573 refreshes, 0 errors | ~1,100 calls — not viable by polling |
| `GetExchangeSnapshot` (whole exchange) | **~20.5 s fixed vendor time** + ~7 s transfer to Seattle | — | NFO 27.2 s avg (44 s max), ~660 KB, 1 call |
| `SubscribeSnapshot` (push), **one identifier per subscribe** | push lands **1.7–1.9 s** after the delayed minute | not yet tested at scale | not yet tested |
| Overlapping requests on one session | only the first is answered | — | — |

The 20.5 s is by design (vendor: the function computes the whole exchange). The ~7 s transfer is Seattle↔Mumbai; the
vendor has confirmed AWS Mumbai is allowed on trial and paid plans.

## The ingestion design this supports

Three independent settings: when the vendor publishes (every minute), how far behind it is (15 min for minutes;
candles at close), and how often KANIDA analyses (15 min today; 5 or 1 later). Ingestion:

1. **Push lane** — `SubscribeSnapshot` per headline contract; process each push on arrival (≈2 s after release).
2. **Fast lane** — `GetSnapshot` in batches of 25 every 30 s for the same set, as the safety net (≈10 s).
3. **Full-chain lane** — `GetExchangeSnapshot` every ~30 s from AWS Mumbai (≈21 s there), carrying forward contracts
   that did not trade.
4. Bars of any size are built from stored minutes; the engine runs whenever a bar of its size completes.

## Open with the vendor

1. Our key still reports `AllowVMRunning=false`, `AllowServerOSRunning=false` — enable both for AWS Mumbai.
2. `GetSnapshot` silently drops calls with >25 identifiers — documented limit? can it return an error?
3. Their `SubscribeSnapshot` example (`"NIFTY-I,"`) and a comma list pushed nothing; one long identifier works.
   Correct multi-symbol form? Does a push carry the completed minute or the minute as it stood? (the 10:56 push had
   volume 7,930; the same minute read 10,920 thirty-nine seconds later)
4. Are complete-at-close 15-minute candles part of the paid delayed plan?
5. Paid-plan symbol counts per plan (calls-per-hour answered 23 Sep: **3,600/h on the paid plan**, same as the trial —
   the vendor's first reply said 1,800 and was corrected).

## Still to do (trial key valid until 23 Sep 23:59 IST)

- Longer push test: several single-identifier subscriptions on one session, each push compared with the final
  1-minute history (complete vs in-progress minute).
- Engine comparison on vendor data, once the full chain can be assembled.


---

## Day 2 — 23 Sep 2026 (trial key's last day)

Same setup, fresh store `gdf_trial/2026-09-23/`. Everything from day 1 held, and the open questions closed:

| Question | Answer, measured |
|---|---|
| Does a push carry the completed minute? | **Yes. 45 of 45 pushes** (5 instruments x 9 minutes) matched that minute's FINAL 1-minute candle exactly — O/H/L/C, volume, OI. Pushes land 1–3 s after release. Day 1's single doubtful case came from subscribing mid-minute. |
| Several subscriptions on one session? | **Yes, 5 ran together**, none dropped. |
| The vendor's "identifiers as an array" | **Rejected by their server**: `Json Deserialization error. Unexpected character encountered while parsing value: [. Path 'InstrumentIdentifier'`. One identifier per subscribe is the only form. |
| Bulk functions in their docs | `GetLastQuoteOptionChain`, `SubscribeOptionChain`: **"Function not enabled."** `StreamAllSnapshots`, `StreamAllSymbols`: **silence, no data and no error** (3 request shapes, 90–150 s each). Our key allows only GetExchangeSnapshot / GetHistory / GetSnapshot / SubscribeSnapshot. |
| Is the whole-exchange call configurable per product? | `InstrumentType` works; **`Product` is ignored** — `Product=NIFTY` returned 3,487 contracts, MORE than an unfiltered NFO call (3,213). |
| Is the ~20 s server time size-related? | **No. Fixed 20.5–20.8 s** in every call: 5 contracts, 593, 2,308, 3,487. Only transfer scales (2.8–6.9 s to Seattle). |
| Candles complete at close, again | 23 bars x 200 symbols: earliest **5 s** after close, median 54 s (our polling phase), 2 bars with any later change. |
| Rebuild from 1-minute pieces, again | **273 of 273 exact**, 0 mismatches. |

**Day-2 volumes:** 419 fast-lane refreshes of 200 symbols (avg 12.0 s), 131 whole-exchange NFO calls (avg 26.7 s, 2,929 contracts, 627 KB), 14,099 distinct NFO contracts covered by the 1-minute snapshots, 24 Kite readings copied.

**Errors (day 2):** 96 across the session — 82 reply timeouts, 20 of them in one burst at 11:10; 18 during a local DNS wobble at 11:16 (`gaierror`); 2 "Key already in use" caused by my own restarts. No data was lost: every retry succeeded.

## The design for derivatives (settled)

`GetExchangeSnapshot` on NFO, MINUTE/1, **once a minute**: one call, every contract that traded that minute, ~27 s (20.5 s of it the vendor's fixed server time, the rest transfer that AWS Mumbai removes). Contracts that did not trade keep their last price and OI — open interest cannot change without a trade. 60 calls/hour against a 1,800–3,600 limit. `SubscribeSnapshot` adds ~2-second pushes for individual contracts on screen. Index spot (NSE_IDX, 5 symbols) rides along in one small call. Stocks are a separate piece of work, decided later.

## Left with the vendor

1. VM / Server OS access on the key (promised "by tomorrow" on 23 Sep; still `false` at the close).
2. The ~20 s first-byte time on the whole-exchange call: what is it on production?
3. Bugs for their engineers: `GetSnapshot` >25 identifiers returns nothing at all; `StreamAllSnapshots` returns nothing with no error; `Product` filter ignored.
4. Trial key expires 23 Sep 23:59 IST — an extension is needed for any further testing.
