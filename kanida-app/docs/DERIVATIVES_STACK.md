# What powers the Derivative tab

Written 20 September 2026. Everything here was read out of the code or measured on this machine;
where something could not be checked it says **not verified**.

Read this with `DERIVATIVES_STACK.csv` beside it — same content, one row per moving part, opens in Excel.

**Nothing in this document changed any code, any data or any running process.** Every database was
opened read-only.

---

## 0. The short version

Five things run. Three of them are the Derivative tab.

| | What it is | Runs | Survives a reboot? |
|---|---|---|---|
| 1 | **F&O capture loop** — takes a photograph of every F&O contract every 15 minutes | 26 times a trading day | **No** |
| 2 | **F&O metrics loop** — turns each photograph into the numbers on screen | every 60 seconds | **No** |
| 3 | **Pilot web server** — serves the tab and its 16 data routes | always | **No** |
| 4 | Equity price loop — stock prices for the *other* tabs | every 5 minutes | **No** |
| 5 | Kite token worker — mints the login token everything needs | every 30 min, weekdays only | **Yes** |

Numbers 1 to 4 are console windows somebody opened by hand. If the machine restarts, or the window
closes, they stop and nothing notices. A script to fix that exists (`scripts/install-services.ps1`)
and **has never been run** — verified: there is no Windows task called `KANIDA F&O capture`,
`KANIDA F&O metrics` or `KANIDA Equity prices`.

That has already cost a session. On 18 September the capture loop died at 11:30 and the afternoon had
to be rebuilt from candles. Candles do not carry a traded average price, so **premium cannot be
computed for that afternoon, ever.** That is why the screener refuses to open on the newest reading.

---

## 1. The jobs — what runs on a clock

### 1.1 The F&O capture loop — the only thing that captures live F&O data

```
market_scanner\.venv\Scripts\python.exe -u -m market_data.derivatives.cli ^
    --log-file logs/derivatives_capture_live.log run
```

| | |
|---|---|
| **Started by** | A person, in a terminal. Process 18276 at the time of writing. |
| **Schedule** | Every 15-minute candle close of the NFO session — 09:30, 09:45 … 15:30 — plus one after the close at 15:45. That is **26 readings a day** (`market_data/derivatives/config.py:50-53`). |
| **Timing rules** | It waits **20 seconds** after each reading before quoting, so the exchange has published the last prints (`config.py:84`). If it is more than **240 seconds** late it refuses to capture and writes the reading down as **missed**, rather than storing a quote that is not what it claims to be (`config.py:90`). |
| **Scope** | Front two expiries per underlying. Resolved on 19 Sep to **27,379 contracts** — 13,515 calls, 13,432 puts, 432 futures — across **216 underlyings**, out of 36,077 listed NFO instruments (`db/derivatives.db`, `meta.scope_counts`). |
| **Cost of one cycle** | 56–57 Kite requests (500 contracts a request, plus one for the spot leg). |
| **How long** | **28 to 40 seconds.** Measured, 18 Sep, nine real readings: 28.1s fastest, 40.1s slowest. |
| **Reads** | Kite live quotes. Its own `captures` ledger, to know what is still due. |
| **Writes** | `db/derivatives.db` only — `snapshots` (~27,240 rows a reading), `underlying_snapshots`, `contracts`, `captures`. It never opens `kanida.db` or `market15.db`. |
| **If it dies** | The tab freezes on the last reading it got, and **says nothing**. There is no alert. The log file is the only warning and nothing watches it. |

**Crash behaviour is good, availability is not.** The `captures` table is a ledger: restart it
mid-session and it picks up exactly where it stopped and never double-captures. The problem is not
recovery, it is that nothing restarts it.

### 1.2 The F&O metrics loop — turns photographs into numbers

```
market_scanner\.venv\Scripts\python.exe -u scripts\metrics_loop.py <logfile>
```

| | |
|---|---|
| **Started by** | A person, in a terminal. Process 16424. |
| **Schedule** | Every **60 seconds**, all day (`scripts/metrics_loop.py`, last line). |
| **What it picks up** | Any reading from **the last day only** that has raw rows but no computed numbers, oldest first, up to 40 at a time. |
| **How long** | **8 to 10 seconds** for a reading of ~10,000 contract rows (measured, `logs/recompute_seeded.log`, 19 Sep). A full 27,000-row reading should be 25–30 seconds — *not verified*. |
| **Writes** | `db/derivatives.db` → `metrics`, and nothing else. |
| **If it dies** | Capture keeps working, but the tab shows nothing new. **Every block on the Derivative tab reads `metrics`, not `snapshots`.** |

**Two things worth knowing.**

1. It only looks back **one day**. A reading older than that which was never computed is never picked
   up on its own. That is why 17 September had to be recomputed by hand.
2. It does **not** take the writer lock that the capture loop takes. It relies on SQLite waiting
   (60-second busy timeout) instead. In practice that blocks rather than corrupts, but it means a
   long prune and a metrics pass can stall each other.

### 1.3 The retention prune — now runs on its own

Runs **once a calendar day, from inside the capture loop**, only after the day's last reading is
done and never before 15:45 (`capture.py`, `maybe_prune`). Before this change the code existed and
nothing ever called it — the retention rules were a document, not a behaviour.

The windows actually in force (`market_data/derivatives/config.py:145-152`):

| Table | Kept for | Was |
|---|---|---|
| `snapshots` (raw) | **30 days** | 90 days |
| `metrics` (what the tab serves) | **90 days** | 365 days |
| `candles_15m` | **180 days** | never pruned at all |
| `underlying_snapshots` | **90 days** | 90 days |
| `daily_rollups`, `contracts`, `captures` | for ever | for ever |

Three safety rules are in the code and are real: it rolls every day being trimmed into a permanent
daily summary **first**; it refuses any window that would reach the session the tab is serving; and
it refuses any window under 14 days outright, so a typo cannot empty the store.

**It has never actually deleted anything** — the store is three days old. `daily_rollups` is empty.

### 1.4 The backfills and the seeder — by hand, no schedule

| Job | Command | Measured cost | State |
|---|---|---|---|
| **15-minute backfill** | `cli backfill --workers 4` | **2 h 06 m** — 917,981 rows, 15,098 contracts, 15,100 requests, 7,550 seconds, 2 contracts/second (18 Sep) | Done once. 14 sessions on disk, 31 Aug – 18 Sep. |
| **Daily futures backfill** | `cli backfill-daily` | ~432 requests, a few minutes — *not verified, it has never completed* | **Broken.** `candles_day` has **0 rows**. The only run on record (19 Sep) returned 0 rows and 2 errors. |
| **Seed from candles** | `cli seed --date YYYY-MM-DD` | **1 m 33 s** — 165,692 rows over 17 readings for 10,628 contracts (19 Sep) | Used twice, to rebuild 17 and 18 Sep. |

The seeder is honest about what it is: every row it writes is stamped `kite.candles_15m`, not
`kite.quote`, and the fields a candle cannot supply are left empty rather than guessed. That is
correct behaviour and it is also the source of the tab's biggest visible problem — see §5.

### 1.5 Jobs that feed the rest of the app but **not** this tab

The equity 15-minute price loop (`market_data.live.cli`, every 300 s) and the pattern scanner
(polls every 60 s) are important to the app, but the Derivative tab reads neither. Its server reader
opens exactly one file: `db/derivatives.db`.

### 1.6 The Kite token — the one real scheduled job, and the single point of failure

The token is minted by a worker in a *different* project on the Desktop, driven by a genuine Windows
Scheduled Task (`KanidaZerodhaAuth`, every 30 minutes, survives a reboot).

But the worker script gates itself: **weekdays only, 06:00–16:30 IST.** Outside that window it exits
without minting. Zerodha tokens expire at 06:00 IST every day. So a token minted on Friday is dead
from **Saturday 06:00 IST until the first successful weekday run on Monday.**

At the time of writing (Saturday 20 Sep) the token was rejected on every request:
`auto-auth ran but did not mint a new token` — 210 errors out of 211 in one equity cycle.

---

## 2. The API — the 16 routes behind the tab

All of them are GET, all read-only, all in `kanida-app/server/kanida_pilot/app.py` lines 532–692,
all require a signed-in member. Every response carries the same honesty envelope: *is it available,
was it captured, as of when, what floors are in force, what is missing.*

**Timings below were measured on 20 Sep 2026** against the live store, read-only, in-process, median
of 7 runs. HTTP and sign-in add a little on top. No `p50/p95` figures for these routes exist anywhere
in the repository — I looked; these are the first.

| Route | The question it answers | Median | Size |
|---|---|---|---|
| `/status` | Is there F&O data, how fresh, what floors? | 18 ms | 2.7 KB |
| `/capture` | Did the capture actually work at this reading? | 1 ms | 1.2 KB |
| `/filters` | What can I filter on? | 36 ms | 81 KB |
| **`/screener`** | **The main market screen — one row per underlying** | **167 ms** | **390 KB** |
| `/unusual` | (older, narrower version of the screener) | 294 ms | 3.8 KB |
| `/chain` | The option chain: calls, strike, puts | 43 ms | 121 KB |
| `/oi-by-strike` | Open interest by strike, calls vs puts | 38 ms | 20 KB |
| `/oi-grid` | Ten series: ATM call +4 above, ATM put +4 below, ΔOI | 7 ms | 21 KB |
| `/indices` | The index overview list | 3 ms | 17 KB |
| `/futures` | The cross-market futures list | 72 ms | 46 KB |
| **`/futures-chart`** | The front future's own candles | **624 ms** (15m) · 0.6 ms (daily, it is empty) | 57 KB · 3.7 KB |
| `/series` | One contract's price and OI through the session | 0.7 ms | 6.8 KB |
| `/pcr-series` | Put-call ratio at every reading | 5 ms | 14 KB |
| `/maxpain-series` | Max-pain strike and the spot beside it | 5 ms | 15 KB |
| `/iv-series` | Implied volatility through the session | 20 ms | 65 KB |
| `/futures-buildup` | Futures OI vs its own average, and the basis | 4 ms | 15 KB |

### The screener payload problem — measured and confirmed

The screener response is **399,860 bytes (390 KB)**. Of that, **378,259 bytes is the `rows` array**
— which is exactly the 378 KB figure the team has been quoting.

I broke it down. The per-contract evidence added by the P06 fix (the one that replaced the
misleading "3 conditions" badge with the actual rules and numbers) accounts for most of the growth:

| Piece of a screener row | Bytes across all 214 rows | Share |
|---|---|---|
| `unusual_contracts` — the per-contract evidence | **167,451** | 44% |
| `top` — the single loudest contract per row | 37,500 | 10% |
| `unusual_rules` — which rules fired, with numbers | 31,114 | 8% |
| `buildup_counts` | 8,918 | 2% |
| everything else (28 numeric fields) | ~133,000 | 35% |

So: without the evidence, `rows` would be roughly **180 KB**. That matches the "grew from ~190 KB"
claim. **I could not find that 190 KB → 378 KB note written down anywhere in the repository** — I
searched every doc, script and log. Treat the figure as verified by *this* measurement, not by a
prior record.

**What to do about it, in plain terms:** the evidence does not need to be in the list. Send the
summary in the row, and fetch the detail when the reader opens that row. That would take 390 KB
down to around 210 KB with no loss of information on screen.

### Caching, compression, throttling — there is none

| | |
|---|---|
| HTTP caching | Deliberately off. `Cache-Control: no-store` is forced on every response. |
| gzip | **Not enabled.** The 390 KB screener goes over the wire uncompressed. It would compress to roughly a quarter of that. |
| ETag | None. |
| Rate limiting | None on these routes. |
| Server-side memo | One small in-process cache, keyed by the newest reading, so a new capture clears it by key rather than by a timer. |
| Browser-side cache | Deliberately none — a 15-minute store gains nothing from it, and blending two readings under one timestamp is exactly the defect the tab is being repaired for. |

### Two routes nothing calls

- **`/api/derivatives/unusual`** — superseded by the screener. Dead weight.
- **`/api/derivatives/capture`** — built for exactly the failure that happened (a global status chip
  saying "healthy" straight through an F&O outage). It has server tests. **The screen does not call
  it yet.** That is a finished part sitting on the shelf.

---

## 3. The database

### 3.1 `db/derivatives.db` — the only store the tab reads

**664 MB on disk, plus a 615 MB write-ahead log — about 1.3 GB today, for three sessions of data.**
(`kanida-app/docs/STORAGE_PLAN.md` §1.4 measured 622 MB on 19 Sep; it has grown since.)

| Table | One row means | Rows now | Size | Grows by | Kept |
|---|---|---|---|---|---|
| `snapshots` | one contract at one 15-minute reading: price, day-average price, volume, open interest | 427,031 | 85 MB | ~74 MB/day | 30 days |
| `metrics` | one contract at one reading, with every number the tab prints | 438,213 | 121 MB + 80 MB indexes | ~113 MB/day | 90 days |
| `candles_15m` | one exchange 15-minute bar for one contract | 2,675,883 | 329 MB + 83 MB index | ~37 MB/day | 180 days |
| `candles_day` | one daily bar for one futures contract | **0 — empty** | 0 | — | for ever |
| `underlying_snapshots` | one underlying at one reading: spot, futures price, total call/put OI | 5,668 | 0.8 MB | <1 MB/day | 90 days |
| `contracts` | one F&O contract ever seen | 36,077 | 5.9 MB | slowly | for ever |
| `daily_rollups` | one contract's whole session, compressed | **0 — empty** | 0 | only on a prune | for ever |
| `captures` | one 15-minute reading: did it run, rows, requests, seconds, how late | 26 | <0.1 MB | 26/day | for ever |
| `backfill_progress` | one contract in one backfill window | 15,704 | 1.5 MB | per backfill | for ever |
| `runs` | one backfill / seed / prune run | 13 | <0.1 MB | per run | for ever |
| `meta` | a setting | 3 | <0.1 MB | — | for ever |

**Total growth: about 223 MB a trading day** (measured, 18 Sep — STORAGE_PLAN §5.1). Under the
current windows the store settles at roughly **12.8 GB**. Under the *old* windows it would have
reached about 55 GB in a year.

### 3.2 How much data actually exists

This is the part most worth looking at. The store holds **three days, and only one of them is whole**:

| Session | Live readings captured | Readings rebuilt from candles | Has premium? |
|---|---|---|---|
| 17 Sep | 0 | 26 | **No** |
| 18 Sep | **9** (09:30 → 11:30) | 17 (11:45 → 15:45) | Only for the first nine |
| 19, 20 Sep | not trading days | — | — |

Sixteen readings on 18 September are written down as `missed` in the `captures` ledger. That is the
loop dying at 11:30 with nobody watching.

### 3.3 The other stores, and what they are for

| Store | Size | What it is | Read by the Derivative tab? |
|---|---|---|---|
| `db/market15.db` | 18.2 GB | Current 15-minute stock candles — Discover, charts, the status chip | **No** (but see §6 — a worker may be about to) |
| `db/kanida.db` | **147 GB** | The old engine store. Last received a price on 31 Jul 2026. Kept alive mostly for a 0.2 MB table of stock names and sectors. | **No** |
| `kanida-app/var/pilot.sqlite3` | 1.3 MB | Users, sign-in sessions, plans | **Yes — sign-in only.** Every derivative route checks the session here. |
| `kanida-app/var/last_good.sqlite3` | 9 MB | Last good answer cache for Discover | No |
| `kanida-app/var/pattern_history.sqlite3` | 175 MB | Pattern outcome history | No |
| `kanida-app/var/research_index.sqlite3` | 154 MB | Per-strategy counts for Discover | No |

---

## 4. The journey of a number, end to end

Pick any figure on the Derivative tab. This is how to walk it back.

```
  ZERODHA / KITE
        │  live quote per contract: last price, day average price, volume, open interest
        │  (plus one NSE quote per underlying for the spot)
        ▼
  [1] CAPTURE LOOP ................ every 15 min, 28-40 seconds, 57 requests
        │  stores it RAW, changes nothing
        ▼
  db/derivatives.db ─ snapshots            ← RAW. one row per contract per reading
                     ─ underlying_snapshots ← RAW. spot + totals per underlying
        │
        ▼
  [2] METRICS LOOP ................ every 60 s, 8-10 s a reading
        │  DERIVES: build-up, volume vs own average, volume-to-OI, premium in
        │  rupees, PCR, max pain, futures basis, the "unusual" flags
        ▼
  db/derivatives.db ─ metrics              ← DERIVED. the only table the tab reads
        │
        ▼
  [3] PILOT SERVER ................ read-only, 16 routes, 1-624 ms each
        │  SELECT + presentation only. Rounds, converts to crore, sorts, groups.
        │  ONE exception: implied volatility is solved here, from the option's own
        │  last traded price. Kite does not supply it. Every IV tile says COMPUTED.
        ▼
  [4] THE SCREEN .................. no polling. It fetches when you act.
```

**What is raw, what is derived:**

| On screen | Where it comes from |
|---|---|
| Last price, open interest, volume | **Raw.** Straight from the Kite quote. |
| Spot price | **Raw** when live-captured. **Rebuilt** on a seeded reading — and a new `spot_source` column is being added right now to say which. |
| Premium in ₹ crore | **Derived** = volume × the day's traded average price. Needs a live quote. |
| Build-up (Long build-up / Short covering …) | **Derived** from price direction + OI direction. |
| Volume "782× its own median" | **Derived** against this contract's own time-of-day history in `candles_15m`. Needs 3 sessions minimum, else it says *no baseline*. |
| PCR, max pain | **Derived** from the chain's total call and put OI. |
| Futures basis | **Derived** = futures price − spot. |
| **Implied volatility** | **Computed at request time, in the server.** Not stored. Not from the vendor. Labelled COMPUTED on every tile. |
| The futures price chart | **Raw** exchange candles (`candles_15m`). The daily version is empty. |

---

## 5. What is fragile — the honest list

Ordered by how much it would hurt.

**1. Nothing survives a reboot.** The capture loop, the metrics loop, the equity loop and the pilot
server are all console processes started by hand. `scripts/install-services.ps1` would make all
three data loops into Windows tasks that start at boot and restart on failure. It has never been
run — verified against the live task list. It needs one elevated PowerShell and about a minute.

**2. Nothing watches anything.** There is no alert, no heartbeat, no check. On 18 September the loop
died at 11:30 and the log still said it was armed for the next reading. The `/api/derivatives/capture`
route was built precisely to surface this — and the screen does not call it.

**3. The Kite token is one point of failure, and it is off at weekends.** Weekday-only, 06:00–16:30
IST, in a different project, minted by browser automation. Everything that touches Kite dies with it.
It was dead at the time of writing.

**4. Premium cannot be recovered after the fact.** The traded average price exists only in a live
quote. A candle does not have it. So any reading rebuilt from candles has **no premium, for ever** —
and the whole liquidity screen (₹2 crore of premium traded) is built on premium. This is why the
screener will not open on the newest reading: it opens on the newest reading that has contracts
*over* the floors, and says so. It is honest, and it is also the reason the tab shows two different
clocks on one screen.

**5. The spot price is missing on rebuilt readings too.** On 19 Sep the seeder's spot leg failed on
every one of 216 symbols with a token error. That is why the 15:45 reading has no spot, and why the
option chain once opened at 21,350 against a spot of 23,302.

**6. Daily futures history does not exist.** `candles_day` has zero rows. The "Daily candles" button
on the futures chart is dead on every symbol and looks broken. One command would fix it.

**7. Two writers, one lock, one of them not using it.** The capture command takes the advisory writer
lock. The metrics loop does not. SQLite makes this safe rather than destructive, but it is a design
loose end.

**8. `daily_rollups` is empty and untested in anger.** It is the thing that is supposed to make the
prune safe — "pruning costs resolution, never the day". It has never actually been written to. The
first real prune will exercise this path for the first time, on live data.

**9. The screener payload is 390 KB, uncompressed, with no gzip.** Fine on a desk. Not fine on a
phone on mobile data.

**10. One of the two loops is running from a temporary folder.** At the time of writing the metrics
loop process was started from a copy in the Windows temp directory rather than from `scripts/`.
Temp directories get cleaned.

---

## 6. Being changed *right now* — do not trust these lines for long

Another worker is editing `market_data/derivatives/**` and
`kanida-app/server/kanida_pilot/derivatives.py` at the same time this was written. Changes already
visible on disk:

| What | Where | Effect on this document |
|---|---|---|
| A new `spot_source` column on `underlying_snapshots` and on `metrics` — it records *how* a spot was obtained: live quote, vendor candle, or our own equity store | `market_data/derivatives/config.py` (new constants), `schema.sql`, `store.py`, `metrics.py` | §3.1 and §4 table row for spot. The column count and the "what is raw" line will change. |
| A brand-new file `market_data/derivatives/spot_backfill.py` — recovering 18 Sep's missing spot values, apparently from `db/market15.db` | new module | §1 gains a job. §3.3 — `market15.db` may become a store the derivative path reads. Today it is not. |
| Making the liquidity floors degrade when premium was never captured | `metrics.py` / `derivatives.py` | §5 item 4 and the whole "the screener opens on an older reading" behaviour. This is the fix for the two-clocks problem. |
| `scripts/metrics_loop.py` made path-robust so it can run as a scheduled task | `scripts/metrics_loop.py` | §1.2 — someone is preparing to install the services. |

---

## 7. A trading day, minute by minute

All times IST.

| Time | What happens |
|---|---|
| **06:00** | Yesterday's Kite token expires. |
| **06:00–16:30, weekdays** | The token worker tries every 30 minutes until it mints a new one. **On Saturday and Sunday it does not run at all.** |
| **09:15** | NFO opens. Nothing is captured — the first 15-minute candle has not closed yet. |
| **09:30:20** | **First capture.** 57 requests, ~30 seconds, ~27,240 rows. |
| **09:31 ± 60 s** | Metrics loop notices and computes that reading. ~10–30 seconds. The tab now has its first numbers. |
| **09:45:20, 10:00:20 … 15:30:20** | The same, 24 more times. Every 15 minutes: capture ~30 s, metrics within 60 s, tab fresh about a minute after each reading. |
| **15:30** | NFO closes. |
| **15:45:20** | **The post-close capture** — the close of the terminal 15:30 bar. The 26th and last reading of the day. |
| **after 15:45** | The capture loop finds nothing more due, runs the **retention prune** once, then sleeps until tomorrow's 09:30. |
| **overnight** | Nothing runs. No backfill is scheduled. The 15-minute candle backfill and the daily futures backfill are both by hand. |
| **weekend** | The capture loop checks the calendar, logs *"not a session; sleeping until Monday 09:30"* and waits. No token is minted, so anything anyone runs by hand will fail. |

**Response time the reader feels:** opening the tab fires `/status`, `/filters` and `/screener` —
about 220 ms of server time, plus 473 KB down the wire. Picking a symbol fires nine or ten more
routes in parallel (chain, OI-by-strike, ΔOI grid, series, futures chart, futures list, PCR, max
pain, IV, futures build-up) plus one IV call per tile in the 2×5 grid; the slowest by far is the
futures chart at 624 ms. There is **no polling on the tab itself** — it never refreshes on a timer.
Somebody has to press refresh or reopen the page. (The app shell around it does poll `/api/state`
every 60 seconds, but that is the cash-price status chip, not F&O.)

---

## 8. Where the older documents are now wrong

Useful to know, because these are the documents people quote.

| Document | What it says | What is true now |
|---|---|---|
| `kanida-app/docs/STORAGE_PLAN.md` §5.1 | `METRICS_DAYS = 365`, `candles_15m` never pruned, store reaches ~55 GB in a year | Now 90 days and 180 days. The store settles at ~12.8 GB. |
| `kanida-app/docs/STORAGE_PLAN.md` §5.1 | "the prune is not scheduled … only runs if somebody runs it by hand" | It now runs once a day from inside the capture loop. |
| `kanida-app/docs/STORAGE_PLAN.md` §5.3 | Drop `last_trade_time`, "read by nothing" | **Wrong.** The tab's IV staleness gate reads it. `schema.sql` says so explicitly. |
| `kanida-app/docs/STORAGE_PLAN.md` §1.4 | derivatives.db = 622 MB | 664 MB plus a 615 MB write-ahead log. |
| `kanida-app/docs/STORAGE_PLAN.md` §5.4 | Several `metrics` columns are "structurally unreachable" | They are reached — by the chain query, which does no join. `schema.sql` lists them. |
| `docs/DERIVATIVES_SPEC.md` §2 | Retention 90 / 365 | Superseded by `config.py`. |
| `kanida-app/docs/BACKLOG.md` item 2b | "about 20 GB" | Was never right; see above. |
| `kanida-app/docs/DERIVATIVE_TAB_AUDIT.md` finding 8 | "daily history has not been fetched for any contract" | Still exactly true. `candles_day` = 0 rows. |
| The audit's finding 3 (15:45 has no spot and no premium) | — | Still true, and being worked on right now. |

---

## 9. If you fix three things, fix these

1. **Run `scripts/install-services.ps1` from an elevated PowerShell.** One minute. It turns the three
   loops into Windows tasks that start at boot and restart themselves. Without it, every future
   session is one closed terminal away from being unrecoverable — and F&O data, unlike stock prices,
   *cannot be refetched after expiry.*

2. **Wire `/api/derivatives/capture` into the screen, and put an alert behind it.** The route already
   exists and already has tests. Today a dead capture is invisible until somebody notices the clock
   has stopped. Every hour of that is an hour of premium data that can never be rebuilt.

3. **Run `cli backfill-daily` and find out why it returns errors.** `candles_day` is empty, so a
   control on the tab is permanently dead and looks broken. It is one command and a few minutes of
   Kite budget — the cheapest visible improvement on the list.

*(Fourth, if there is room: move the screener's per-contract evidence out of the list and behind the
row. 390 KB → ~210 KB, and turning on gzip would take it to ~60 KB. No information lost.)*
