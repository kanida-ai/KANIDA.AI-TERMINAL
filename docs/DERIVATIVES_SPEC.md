# Derivatives (F&O) — build spec v1, 2026-09-18

Owner decisions: 15-minute aggregated data (same cadence as the coming vendor), no tick capture for now. Show what is
happening now; evidence cards come later and only where honest. New **Derivative** tab next to Discover Strategies.

Verified against Kite on 2026-09-18: 35,922 NFO instruments · 216 underlyings with options · 647 futures. A quote
carries `last_price, oi, oi_day_high, oi_day_low, volume, average_price, buy_quantity, sell_quantity, depth (5 levels),
ohlc, timestamp`. `historical_data(..., oi=True)` returns candles **with OI** for live contracts (verified: RELIANCE
futures 43 daily rows, an option 14 daily rows). **Expired contracts disappear from Kite**, which is why we store.

## 1. Scope
- All 216 underlyings with options + the 647 futures, restricted to the **front two expiries** per underlying
  (weekly and monthly where both exist). Everything else is ignored.
- Cadence: one snapshot per **15-minute candle close** during the session (09:30, 09:45 … 15:30), plus one at the
  close. Session rules follow `docs/DATA_PIPELINE_CONTRACT.md` §2A (CAS: F&O underlyings stop continuous trading at
  15:15 from 2026-08-03; the auction price is not in the intraday series).
- Backfill on first run: **10 sessions of 15-minute candles with OI** for contracts that pass the liquidity floor, so
  the time-of-day baseline exists immediately instead of in two weeks.

## 2. Store — `db/derivatives.db` (never `db/kanida.db`)
- `contracts(instrument_token, tradingsymbol, underlying, instrument_type CE|PE|FUT, strike, expiry, lot_size, first_seen, last_seen)`
- `snapshots(instrument_token, captured_at, last_price, average_price, volume, oi, buy_quantity, sell_quantity, bid, ask, source)`
  — one row per contract per 15-minute mark. Keep raw; derived numbers live in `metrics`.
- `candles_15m(instrument_token, bar_start, open, high, low, close, volume, oi)` — the backfill and its daily top-up.
- `underlying_snapshots(underlying, captured_at, spot, fut_price, total_ce_oi, total_pe_oi, total_ce_volume, total_pe_volume, pcr_oi, pcr_volume, max_pain_strike)`
- `metrics(...)` — the computed signals of §3, one row per contract (or underlying) per 15-minute mark.
- Provenance on every row: `vendor_id`, `fetched_at`, `snapshot_id`. Retention: raw snapshots 90 days, metrics 1 year,
  daily roll-ups kept for good.

## 3. Signals (exact definitions — no invented numbers)
Volume from Kite for NFO is in **units** (contracts × lot size); premium in ₹ = `volume × average_price`.

1. **Open-interest build-up, per strike** — compare price change and OI change over the same window (15-minute and
   since previous close):
   | Price | OI | Label |
   |---|---|---|
   | up | up | Long build-up |
   | down | up | Short build-up |
   | up | down | Short covering |
   | down | down | Long unwinding |
   Report both the 15-minute and day-on-day classification; never mix them in one label.
2. **Volume versus its own average** — today's cumulative volume by this time of day ÷ the **median cumulative volume
   by the same time of day** over the last 10 sessions for that exact contract. Fewer than 3 sessions of history ⇒
   report "no baseline", never a ratio.
3. **Volume-to-OI spike** — day volume ÷ previous-day closing OI. Flag above 1.0 (more traded today than the whole
   standing position) with the raw numbers beside it.
4. **Premium traded (₹)** — `volume × average_price`, shown in ₹ crore, per contract and rolled up per underlying.
5. **Put-call ratio** — OI PCR = Σ PE OI ÷ Σ CE OI; volume PCR = Σ PE volume ÷ Σ CE volume. Per underlying per expiry,
   and the day's trend.
6. **Max pain** — the strike where the total payout to option buyers at expiry is smallest, from current OI across all
   strikes of that expiry. Report the strike, the spot distance, and the total OI it is computed from.
7. **Futures OI build-up** — the same four labels as (1) on the front futures contract, plus OI as a share of its own
   20-day average, and basis (futures − spot).

**Liquidity floors for any "unusual" list** (a screen without floors is a junk list): premium traded ≥ ₹2 crore
**and** OI ≥ 1 lot-normalised threshold **and** last price ≥ ₹1. Floors are constants in code, shown in the UI.

**Expiry awareness:** every row carries days-to-expiry; lists are grouped or filterable by expiry so expiry-day volume
cannot masquerade as unusual activity.

**Roll-up:** the default list is per underlying ("RELIANCE — 3 call strikes unusually active, OI +12%, ₹48 cr traded"),
expanding to the strikes beneath it.

## 4. The Derivative tab (benchmarked on the TrendSpider dashboard)
- A new top-nav item **Derivative**, next to Discover Strategies.
- Layout: a grid of cards over a linked chart, like the TrendSpider dashboard — clicking a row anywhere re-targets the
  chart. Cards for v1:
  1. **Unusual activity** (the §3.2–3.4 screen, per underlying, expandable to strikes)
  2. **Option chain** for the selected underlying and expiry, with OI, OI change, volume, premium, and build-up labels
  3. **OI by strike** chart (CE vs PE) with max pain and spot marked
  4. **Index dashboard**: NIFTY / BANKNIFTY / FINNIFTY — PCR, max pain, OI shift through the day
  5. **Futures OI build-up** table across underlyings
- Filters like TrendSpider's "Customize": underlying/watch-list, expiry, days-to-expiry, CE/PE, minimum premium.
- Every card states its own as-of time and the liquidity floors in force.

## 5. Honesty rules (same as the rest of the app)
- No evidence card yet. The tab describes **what is happening**, never what will happen.
- A ratio with too little baseline says "no baseline", never a number.
- Nothing here feeds the trading gates; derivatives are display-only in v1.
- When evidence does arrive it starts with **futures** (deep OI history), and follows the pattern method: what
  happened after past occurrences, against a baseline, corrected for the number of things tested.
