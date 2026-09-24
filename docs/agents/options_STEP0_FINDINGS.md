# Options Agent — STEP 0 findings: the options data reality

> Status: **REVISED 2026-08-31 after running the probe against LIVE Kite.** The first pass
> was written with no credentials in this checkout; a live token was then located and the
> open questions are now **measured, not expected**. §0 is the revision and supersedes the
> earlier conclusion where they differ. Repo: `C:\Users\SPS\Desktop\koptions`.

---

## 0 · REVISION — what the live probe actually measured

The first pass concluded "no historical backtest is possible, go forward-tracking only".
**That was too strong.** Measured against live Kite (user DC2940, token dated 2026-08-31):

**0.1 There is no artificial cap on option history.** `historical_data` returns candles from
each contract's **listing date**, subject only to the documented 2000-day-per-request limit
(confirmed by the API's own error: `interval exceeds max limit: 2000 days`). Requesting 2000,
400 or 90 days returns the same earliest bar for a given contract — its listing date.

Measured depth, NIFTY calls near ATM, `oi=True` returning open interest on every bar:

| Contract | Expiry | Daily bars | Listed |
|---|---|---|---|
| `NIFTY2690124100CE` | 2026-09-01 | 29 | 2026-07-22 |
| `NIFTY26SEP24100CE` | 2026-09-29 | 44 | 2026-07-01 |
| `NIFTY26OCT24100CE` | 2026-10-27 | 24 | 2026-07-29 |
| **`NIFTY26DEC24000CE`** | 2026-12-29 | **833** | **2022-11-28** |
| `NIFTY28JUN24000CE` | 2028-06-27 | 787 | 2023-06-30 |
| `NIFTY30DEC24000CE` | 2030-12-31 | 165 | 2025-12-31 |

So a single long-dated contract carries nearly **four years** of real daily option history
including OI. Minute data is also available (10,923 one-minute bars on the front weekly).

**0.2 Expired tokens are rejected — this is now proven, not inferred.** Probing instrument
tokens absent from today's NFO master returns `InputException: invalid token`, with a
known-good live token succeeding as a control immediately before. Kite serves history
**only** for instruments in the current master. So the enumeration problem in §3 stands, and
is now measured rather than reasoned.

**0.3 The consequence — a real backtest IS possible, inside a sharp survivorship boundary.**
For a past date *D* we can reconstruct the chain **only from contracts still listed today**.
Measured coverage (still-listed NIFTY contracts already listed on *D*):

| D | Contracts | Live expiries | Nearest expiry still visible |
|---|---|---|---|
| today | 1,594 | 18 | 2026-09-01 |
| −30 d | 970 | 14 | 2026-09-01 |
| −60 d | 560 | 12 | **2026-09-29** |
| −90 d | 290 | 10 | **2026-12-29** |
| −365 d | 241 | 8 | **2026-12-29** |

The last column is the one that matters. At 90 days back the nearest expiry we can still see
is ~7 months out. **A weekly iron condor is invisible beyond about 30–40 days back** — the
weeklies it would actually have traded have expired and are unreachable. What survives at
longer lookbacks is long-dated LEAPS, which is a different instrument and a different
strategy; back-testing a weekly condor on those would be both survivorship-biased and
strategy-mismatched.

**0.4 Kite gives no IV and no greeks — now measured.** A live `quote()` on
`NFO:NIFTY2690124600PE` returns 22 keys: `last_price, oi, oi_day_high, oi_day_low, volume,
depth (5 bid + 5 ask), ohlc, average_price, circuit limits, timestamp, last_trade_time` …
and **no `iv`, no `delta/gamma/theta/vega`**. `pricing.py` (Black-76) is therefore mandatory,
as expected — the expectation is now a measurement.

**0.5 Live chain shape (measured).** 31,237 NFO instruments total; **1,594 NIFTY option
contracts across 18 expiries**; strike step **50**; **lot size 65**; weekly expiries on
Tuesdays (2026-09-01, -09-08, -09-15, -09-22), monthlies, then quarterly/half-yearly out to
2031-06-24. Spot at probe time 24,080.4. (Lot size 65 — *not* the 75 a hardcoded constant
would have assumed. This is exactly why `data.py` reads lot size from the as-of snapshot.)

### 0.6 Revised recommendation — HYBRID, not forward-only

1. **Backfill on the first snapshot run.** Because every live contract serves its full
   history since listing, the first run can pull ~1,594 NIFTY contracts × their lifetimes
   immediately, instead of accruing one day at a time. That yields a real dataset *today*,
   validates the whole pipeline (IV solve → greeks → condor construction → replay) on real
   data, and gives an immediate honest N of roughly 4–8 near-dated condor cycles.
2. **Genuine limited historical replay** over the last ~30–60 days for weekly/near-monthly
   condors, labelled with its survivorship boundary explicitly.
3. **Forward-tracking remains the primary evidence path** — it is the only thing that
   removes the survivorship boundary, and beyond ~40 days back the near-dated chain simply
   does not exist.

**Backfill honesty caveat (important).** `historical_data` returns OHLC + OI per contract per
day — it does **not** return bid/ask. So backfilled rows have closes and OI but **no
spreads**. Backfilled rows must therefore be marked `price_source="hist_close"`, slippage
must fall back to a governed assumption rather than a measured spread, and the **liquidity
gate must report `skipped` with a reason on backfilled dates** rather than silently passing.
Only live snapshots carry real bid/ask.

---

## TL;DR (first pass — see §0 for the measured revision)

1. **There is no options data in this repo.** None. Not a table, not a file.
2. ~~There are no Kite credentials~~ — a live token was subsequently located at
   `Desktop\Kanida.ai Terminal Quant Intelligence Engine\data\db\kanida_quant.db`
   (`kite_tokens`, dated 2026-08-31). The probe has been run; see §0.
3. **A historical point-in-time chain cannot be reconstructed for contracts that have
   expired** — confirmed by measurement in §0.2. But contracts still listed DO serve their
   full history, which §0.3 quantifies.
4. → Evidence approach: **hybrid** — backfill + limited historical replay inside the
   survivorship boundary, with forward-tracking as the primary path. See §0.6.

---

## 1 · What is in the repo today (MEASURED)

Swept by filename, by grep (`NFO`, `instruments(`, `option_chain`, `black.?76`,
`implied_vol`), and by opening every database.

| Fact | Value |
|---|---|
| Options tables / files / parquet anywhere | **0** |
| The only DB | `data/db/kanida_quant.db`, 86.5 MB, 18 tables |
| `ohlc_daily` | 223,059 rows · **149 NSE tickers** · 2020-01-01 → **2026-04-24** |
| `ohlc_weekly` | 22,726 rows |
| `NIFTY50` index daily bars | **570 bars, 2024-01-01 → 2026-04-24** |
| `pattern_library` / `trade_log` / `execution_log` | 14,201 / 21,451 / 21,451 |

Two things to flag honestly:

- **The underlying history is thin and stale.** NIFTY50 has 570 daily bars and the whole
  store ends **2026-04-24** — roughly four months before today (2026-08-31). Anything
  built on it right now is working on stale data. (Kite *can* serve years of NIFTY
  history, so this is a fetch gap, not a hard limit — but it is a gap that must be closed
  before any underlying-distribution work is trustworthy.)
- **No IV history exists**, which has a direct consequence in §4.

## 2 · Kite access + API surface

**Credentials: absent here (MEASURED).** `config/.env` does not exist (only
`.env.template`), no `KITE_*` environment variables are set, and the `kite_tokens` table
is not in the DB. `services.kite_auth.get_kite_client()` raises
`KiteAuthError: CONFIG_MISSING`. So **nothing was measured against the live API.**

**API surface: measured from the installed SDK** (`kiteconnect 5.2.0`, introspected
directly rather than recalled):

| Call | What it gives us | Relevance |
|---|---|---|
| `historical_data(token, from, to, interval, continuous=False, **oi=False**)` | candles + **open interest** | OI history per contract is available |
| `instruments(exchange)` | the instrument master | routes are **only** `/instruments/{exchange}` and `/instruments` — **there is no expired-instruments endpoint** |
| `quote(*instruments)` | full quote | field set is server-side — **UNVERIFIED** whether it carries IV/greeks (§5) |
| **`basket_order_margins(params, consider_positions, mode)`** | span+exposure **with hedge benefit** across a multi-leg basket | the correct margin source for a 4-leg condor |
| **`get_virtual_contract_note(params)`** | itemised real charges | better than a hand-rolled cost model — real brokerage/STT/GST/stamp |
| `order_margins(params)` | per-order margin | already used in `autotrade/broker/zerodha.py` for futures |

**Half the loader already exists.** `backend/autotrade/broker/zerodha.py` already parses
the NFO master: `_nfo_instruments()` (~34k rows, ~2 s, cached 6 h process-wide) and
`get_option_chain(name)` filtering `instrument_type in ("CE","PE")` and reading
`strike / tradingsymbol / expiry / lot_size / instrument_token`. The Options Agent's
`data.py` should reuse this shape, not reinvent it — and reuse `services/kite_auth.py`
for the client, per the brief.

## 3 · The decisive structural finding (does NOT depend on credentials)

> `instruments("NFO")` returns **live contracts only**, and Kite exposes **no historical
> instrument master**.

To evaluate an iron condor point-in-time on a past date *D*, you must first **enumerate
the strikes and expiries that existed on *D*** and get their `instrument_token`s. The
token is not derivable from the tradingsymbol — it only ever comes from the master. So:

- Even if `historical_data` happily serves candles for an already-expired token, **you
  cannot discover which tokens to ask for** on a past date.
- Therefore a genuine point-in-time historical chain cannot be reconstructed from Kite
  alone — **at any credential level.** This is an enumeration problem, not an access one.

This is the finding that decides the evidence approach, and it is why I am not waiting on
the credentials to make the recommendation.

## 4 · Consequence that must be handled honestly: IV rank

The brief's entry filter wants **elevated IV rank / percentile**. With no IV history,
**IV rank is not computable on day 1** — and a percentile invented from a few days of data
would be exactly the kind of fabricated number the platform forbids.

Proposed handling, mirroring the Chart Agent's G4: the IV-rank gate **reports `skipped`
with a named reason** and the decision degrades to an honest `WATCH`. It flips to a real
gate only once the snapshot store (§6) holds enough IV history to compute a percentile,
and that threshold is stated up front rather than discovered later.

## 5 · What remains genuinely open (one probe run settles it)

`scripts/options_step0_probe.py` is written, syntax-clean, and verified to exit honestly
with `NO_KITE_SESSION` when there are no credentials. It is **read-only** — it calls only
`instruments()`, `historical_data()`, `quote()`, `basket_order_margins()` and
`get_virtual_contract_note()`. There is no `place_order`/`modify_order`/GTT call anywhere
in it; the two margin/charges endpoints are calculators that create nothing.

It answers:

- **Q1** the NIFTY option master: field set, expiry list (weekly + monthly), strike ladder
  and step, lot size.
- **Q2 — the one that matters:** how deep `historical_data` actually goes for a *live*
  option token (day + minute, `oi=True`), probed on both the nearest and the farthest
  expiry (the farthest has been listed longest → best proxy for history-since-listing).
- **Q3** the exact `quote()` key set → settles **by measurement** whether IV/greeks come
  from Kite or must be computed ourselves (Black-76 on the future, or Black-Scholes on
  spot with *r* and *t*). My expectation is that Kite gives price + OI + 5-level depth and
  **no** IV/greeks — but that is an expectation, not a measurement, and it is labelled as
  such until the probe runs.
- **Q4/Q5** the real basket margin and the real itemised charges for an actual 4-leg
  condor.

Run: `python scripts/options_step0_probe.py --out reports/step0_nifty.json`

Note Q2 cannot rescue the backtest on its own — §3 already rules that out. Q2 matters for
sizing the *forward* path (how much per-contract history we inherit on the day we start
tracking a setup) and for knowing what we can reconstruct for contracts currently listed.

## 6 · Recommendation

**Evidence approach: forward-tracking, honestly labelled — not a backtest.**

Evaluate each candidate condor from the *current* chain using the option-implied
distribution for POP/EV, record the setup with its full point-in-time state, and track it
to expiry so evidence accumulates. Every output labelled forward-tracked, small-N, not a
historical backtest. Until *n* is meaningful, the honest decision is `WATCH`, exactly the
way the Chart Agent returns WATCH at n=6.

**Start the daily chain snapshot immediately — this is the important one.** Persist the
NFO master plus per-strike LTP/bid-ask/OI/volume each session. Rationale:

- forward-tracking needs it anyway (it *is* the evidence store);
- it is the **only** way we ever own a historical chain — §3 says we can never buy this
  back later, we can only start accruing it now, so every day we delay is a day of
  evidence permanently lost;
- it unlocks the IV-rank gate (§4) once enough history accrues;
- it is cheap — a few MB/day.

**Split the two halves of the evidence honestly.** The *underlying* distribution (realized
move, regime, trend) can be backed by real multi-year history from Kite once the stale
store (§1) is refreshed. The *option-chain* half is forward-only. These should never be
blended into one confidence number without saying which half carries which claim.

---

## 7 · Awaiting decision

Per the brief I do not proceed to the expectancy engine until the approach is confirmed:

1. Confirm **forward-tracking** as the phase-1 evidence approach (§6).
2. Approve **starting the daily chain snapshot now** — and tell me where it should be
   persisted (SQLite alongside `kanida_quant.db`? Parquet? S3, as the Chart Agent's screen
   store does?).
3. Provide Kite credentials (or run the probe on a machine that has them) so Q1–Q5 become
   measured rather than expected.
4. Confirm the stale/thin underlying store (§1) should be refreshed as part of this build,
   or is out of scope.
