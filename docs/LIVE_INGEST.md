# Live ingest — feeding `db/market15.db` during a session

*Built 2026-09-16. Contract: `docs/DATA_PIPELINE_CONTRACT.md` §1 (provider), §2/§2A
(stores, aggregation, CAS), §5 (live), §6 (rules that do not bend).*

Two pieces:

| | what it does | where |
|---|---|---|
| **Ingest loop** | every N minutes, fetch each NIFTY 500 symbol's missing 15-minute bars from the provider and append them to `db/market15.db` | `market_data/live/ingest.py`, `market_data/live/cli.py` |
| **Scanner source switch** | `market_scanner` reads candles from `db/market15.db` instead of `db/kanida.db`, and rescans when new bars land | `market_scanner/data.py`, `market_scanner/engine.py` |

`db/kanida.db` stays **read-only** throughout — it is opened `mode=ro` only for the
universe (`instrument_labels`) and the observed session calendar.

---

## 1. Start / stop the loop

```powershell
cd C:\Users\SPS\Documents\Kanida_Falcon
$py = ".\market_scanner\.venv\Scripts\python.exe"

# what the store holds right now (read-only, safe any time)
& $py -m market_data.live.cli status

# what the next cycle would request, without fetching anything
& $py -m market_data.live.cli --limit 20 plan

# exactly one cycle
& $py -m market_data.live.cli once --verbose-symbols

# the loop: catch-up pass, then a cycle every 5 minutes while the market is open
& $py -m market_data.live.cli run --interval 300
```

Stop with **Ctrl-C** (or SIGTERM). The loop finishes the cycle it is in and exits.
There is nothing to clean up: the next start recomputes what is missing from what
the store holds, so a kill at any moment is safe.

### The writer lock

`once` and `run` take the store's advisory writer lock
(`db/market15.db.writer.lock`) and **refuse to start** if another writer holds it:

```
RuntimeError: market15 writer lock held by pid=13100 (612s old): db\market15.db.writer.lock
```

That is the seeder (W2) or the repair worker (W3). Either wait for it, or pass
`--allow-concurrent` when you know the other writer is working on different
symbols. SQLite's WAL serialises the actual writes either way; the lock is there
to catch the operational mistake, not to make writing safe.

---

## 2. Switches

| env | default | meaning |
|---|---|---|
| `MARKET_DATA_PROVIDER` | `kite` | `kite` \| `vendor15` \| `fake`. The vendor swap is config-only (contract §1). |
| `SCANNER_CANDLE_SOURCE` | **`legacy`** | `legacy` = `db/kanida.db` (unchanged, the frozen-research source). `market15` = `db/market15.db`. |
| `SCANNER_MARKET15_DB` | `db/market15.db` | point the scanner at a different 15-minute store. |

CLI flags (all optional): `--db`, `--provider`, `--index in_nifty500`,
`--symbols RELIANCE,TCS`, `--limit N`, `--workers 4`, `--backfill-sessions 10`,
`--daily-history-days 2200`, `--no-daily`, `--allow-concurrent`, `--log-level`.

**Nothing changes until `SCANNER_CANDLE_SOURCE=market15` is set.** With the
default the scanner behaves exactly as before, byte for byte.

---

## 3. What one cycle does

Per symbol, all of it derived from stored state so it is resumable:

1. **Plan** — `last_stored = MAX(bar_start)` for the symbol; `latest =
   provider.latest_completed_bar('15minute', now)` walked back onto a real
   session in the calendar. Request `(last_stored, latest]`. Nothing to fetch is
   the normal case and costs **zero requests**.
2. **Fetch** — through the provider, which serialises every call through the one
   process-wide token bucket (Kite ≈ 3 req/s). Incomplete bars are dropped, never
   stored.
3. **Write** — through `market_data.store`:
   * a bar we have never seen → revision 1;
   * a bar the vendor has **restated** → a *new revision* plus one `corrections`
     row per changed field, with the old revision left in place;
   * a bar identical to what we hold → **not rewritten at all**. This is what
     makes a re-run free.
4. **Daily bars** (after the session closes, or on the catch-up pass) — the
   provider's own `day` bars into `daily_bars`. Required, not optional: from
   2026-08-03 a CAS stock's official close is the 15:30–15:35 auction price,
   which **does not exist anywhere in the intraday series** (contract §2A). 1D
   and 1W candles are built from these bars.
5. **Session regimes** — which symbols stopped continuous trading at 15:15 is
   derived from the bars (`RegimeBook.from_observed_last_bars`, cross-checked
   against `instrument_labels.is_fno` but never overridden by it) and written to
   `session_regimes`.
6. **Freshness** — `meta['live.latest_bar_start' | 'live.latest_bar_end' |
   'live.last_cycle' | 'live.last_cycle_finished_at']`, plus an `ingest_runs` row
   for every cycle.

Outside market hours the loop idles to the next session open (capped at 30
minutes per sleep) and does one catch-up pass when it starts.

**Clock:** the loop uses IST explicitly (`market_data.live.ingest.now_ist`), never
the machine's local time. These boxes run on US Pacific; using `datetime.now()`
put the loop a whole session out.

---

## 4. What a cycle costs (measured 2026-09-16, live against Kite)

| run | symbols | requests | bars written | wall clock | rate |
|---|---|---|---|---|---|
| incremental 15m, mid-session | 20 | 20 | 20 (the 09:15–09:30 bar) | **6.8 s** | 2.93 req/s |
| backfill 15m, 5 sessions | 100 | 100 | 9,932 | **34.4 s** | 2.91 req/s |
| daily history, 2,200 days | 20 | 20 | 25,285 | **14.2 s** | — |
| re-run of a current cycle | 20 | **0** | **0** | 0.0 s | — |

Throughput is the rate limiter, not the network, so it is linear:

* **NIFTY 500, 15-minute pass ≈ 500 / 2.92 ≈ 172 s (2 min 52 s)** — comfortably
  inside a 5-minute cycle, in fact inside a 15-minute bar with room to spare.
* **NIFTY 500, daily pass ≈ 172 s** of requests; the first one also writes ~630k
  rows (≈ 6 min total). After that it is one bar per symbol per day.
* So: a mid-session cycle is **≈ 3 min**; the post-close cycle (15m + daily) is
  **≈ 6 min**. Run the loop at `--interval 300`; the post-close cycle simply takes
  longer than one interval, which is fine because nothing is pending behind it.

---

## 5. Is it current?

```powershell
& $py -m market_data.live.cli status
```

* `newest_bar_start` — the newest completed 15-minute bar in the store.
* `daily_through` — the last session whose provider daily bar landed.
* `last_cycle` — the previous cycle's counters (requests, rows, errors, seconds).
* `recent_runs` — the last five `ingest_runs` rows.

From the scanner, `GET /api/state` reports:

```json
{ "candle_source": "market15",
  "source_latest": "2026-09-16 15:15:00",
  "source_stale": false,
  "database": "...\\db\\market15.db" }
```

Beside those, `/api/state` carries an additive **`data_status`** object
(`market_scanner/data_status.py`): provider id and its `delay_seconds`, the candle
source, the newest bar (overall and per timeframe), the store's last `ingest_runs`
row, the next expected refresh, the IST session state (`pre_open` / `open` /
`closed` / `holiday`) and a `stalled` flag that is only asserted while the market
is open. It is **read-only and memoised for 20 s**, every value traces back to
something this loop writes, and anything unavailable comes back `null` with a
sibling `note` saying why. The app's **Data status** popover (the top-bar and
Discover data pills) renders exactly that object.

`source_latest` is the newest completed bar the active source holds (in `legacy`
mode it is still `max(ohlc_daily.bar_time)` — unchanged). The pilot's
`DATA_STALE` gate reads the date out of `source_latest`, so it clears as soon as
the store is current. **The gate itself is untouched**: fresh candles do not
enable trading on a stale backtest — the evidence gates in
`kanida-app/server/kanida_pilot/evidence.py` are unchanged and still apply.

The freshness probe mixes F&O and non-F&O reference names on purpose: an all-F&O
set would under-report the newest bar by one slot every day, because a CAS stock's
last 15-minute bar starts at 15:00.

---

## 6. The scanner side

With `SCANNER_CANDLE_SOURCE=market15`:

* **1H / 4H** — aggregated from `candles_15m` through `market_data.aggregate`,
  under the symbol's own session regime. A CAS stock's last 1H bucket is
  14:15–15:15 and its last 4H bucket is 13:15–15:15, both **complete**.
* **1D / 1W** — from `daily_bars` (the provider's own daily bar), so the official
  close is carried. 1W is built with `weekly_from_daily`.
* The candle record handed to the ten detectors is unchanged:
  `{'time','end','open','high','low','close','volume','gap'}`, completed buckets
  only. The `gap` flag is set by the same shared `finish_bars` both sources use.
* **Rescan trigger** — the watcher polls the newest completed bar (O(1) via
  `meta`, falling back to a bounded index lookup) instead of `PRAGMA
  data_version`, which is useless here because the seeder and repair workers
  commit constantly. When the newest bar advances, every timeframe is rescanned.
  The existing candle-close trigger is unchanged. In `legacy` mode the watcher
  still uses `data_version`, exactly as before.

### One deliberate difference: 1W on special weeks

`market_data` keeps 1W strictly Mon–Fri (contract §2) and uses the *observed*
Muhurat window; the legacy scanner's week walks all seven days and flattens
Muhurat to 09:15–15:30. Over 2023-01-01 → 2026-07-25 for RELIANCE that is **7
week candles out of 186** whose `end` moves — budget/DR Saturdays (2024-01-20,
2024-03-02, 2024-05-18, 2025-02-01, 2026-02-01) and Muhurat (2023-11-12,
2024-11-01). Every other week, and every `gap` flag, is identical. The list is
pinned in `market_scanner/tests/test_candle_source_parity.py`
(`KNOWN_WEEKLY_DIFFERENCES`) so it cannot grow unnoticed.

1H, 4H and 1D are **identical**, field for field including `gap`, for a pre-CAS
window — that is the parity test in the same file.

---

## 7. Before the ten strategies can scan live

1. `db/market15.db` must hold all 500 symbols (W2's seed, then W3's repair).
2. **`daily_bars` must be populated** — until the daily pass has run there are no
   1D/1W candles at all in market15 mode. The scanner logs this at startup.
3. **`session_regimes` must be populated** — without them, every post-2026-08-03
   4H bucket and 1D bar for an F&O stock is dropped as incomplete, and 1H picks
   up a phantom gap on each of the ~30 post-CAS sessions. The ingest loop writes
   them every cycle.
4. The loop must actually be running (`cli run`) for the whole session.
5. Flip `SCANNER_CANDLE_SOURCE=market15` and restart the scanner.

---

## 8. Rules this respects

* Point-in-time: a bar is written only once it is complete *and* published
  (`latest_completed_bar` honours the vendor's `delay_seconds`). A 15-minute
  delayed vendor reads as delayed by design, not stale.
* Never fabricate: a missing bar is reported, never filled. Incomplete bars are
  not stored. A vendor restatement becomes a new revision plus a `corrections`
  row — never an in-place edit.
* Never log credentials: the provider owns the token and redacts it; nothing here
  prints a token or a fingerprint.
* `db/kanida.db` is never opened for writing.
* The scanner's evidence/trade gates are untouched. Fresh candles change what is
  *detected*, never what is *tradable*.
