# Monday readiness — the F&O pipeline running on its own

Checked on Sunday 20 Sep 2026. Monday's session is 21 Sep, 09:15–15:30 IST.

Everything below was measured on this machine, not assumed. Where something is still fragile it says so
plainly, and where you may need to run something yourself the command is given in full.

---

## 1. The one thing that actually broke on Friday: this computer went to sleep

This is the most important line in this document.

Friday's F&O capture did not stop because of the Kite token. It stopped because **the computer went to
sleep in the middle of the Indian trading session and stayed asleep until after the market closed.**

Windows' own power log says it, to the minute:

| Local time (Pacific) | Indian time | What Windows recorded |
|---|---|---|
| Thu 17 Sep, 23:01:58 | Fri 18 Sep, **11:31 IST** | "The system is entering sleep" |
| Fri 18 Sep, 06:24:54 | Fri 18 Sep, **18:54 IST** | The system came back |

The last F&O reading captured on Friday was **11:30 IST**. The market closed at 15:30 IST. The machine was
asleep for all of it, so nothing could run: not the capture, not the metrics, not the token refresh.

**Why this keeps happening.** This computer is set to US Pacific time. The Indian market session
(09:15–15:30 IST) falls **overnight** here — roughly 20:45 to 03:00 the previous evening. The whole trading
day happens while a normal person's laptop is asleep.

**What I could not do.** Changing power settings is a system setting, and I do not change those. This one is
yours.

**What you need to do before Monday evening (Pacific), in an Administrator PowerShell window:**

```
powercfg /change standby-timeout-ac 0; powercfg /change hibernate-timeout-ac 0; powercfg /change disk-timeout-ac 0
```

That is one line. It tells Windows never to sleep while plugged in. Then **keep the laptop plugged in and
do not close the lid** on Sunday evening. (Closing the lid is a separate switch; if the lid must be closed,
set "Choose what closing the lid does" to "Do nothing" in Windows power settings.)

Without this, nothing else in this document matters. With it, everything else below should hold.

A second, smaller worry from the same log: Windows recorded **three unclean shutdowns in three days**
(17, 18 and 20 Sep) — the machine lost power or hard-crashed. The services all came back by themselves each
time, which is good, but it is worth knowing the machine is not perfectly stable.

---

## 2. The Kite token — will it refresh on Monday morning?

**Yes, on its own, and the timing works.** Checked in the code and in the log.

- The token worker runs every 30 minutes, all day, and decides for itself whether to act. It converts the
  clock to Indian time properly, so it knows Monday is a weekday even though it is Sunday evening here.
- It acts between **06:00 and 16:30 IST on weekdays**. The first attempt on Monday will be at
  **06:00 IST — three and a quarter hours before the market opens.** If it fails it tries again every
  30 minutes for the rest of the day.
- Nothing needs a human for the normal path.

**What is fragile.** The refresh works by driving a real browser, and that browser sometimes crashes.
It failed exactly that way on Friday morning (09:14 IST) and on two earlier occasions. It recovered on
Friday within half an hour by itself, but it is not guaranteed to.

**And it does not raise a hand.** When the refresh fails, the code tries to send a phone notification and
the log says `NO_SUBSCRIPTIONS` — nobody has ever subscribed, so the alert goes nowhere. **A token failure
is currently silent.** I have not changed that; it lives in the other project (the Terminal engine) and
wiring a new notification channel is a bigger job than this round.

**How to check it yourself on Monday morning, one command:**

```
Get-Content "C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\logs\auth_worker.log" -Tail 20
```

You want to see the words `status=success` or `token already valid today`. If you see `FAILED` or
`BROWSER_CRASHED`, the token did not refresh and nothing will collect data until it does.

---

## 3. The three background jobs

`KANIDA F&O capture`, `KANIDA F&O metrics`, `KANIDA Equity prices`. All three were running when I checked,
and all three are set up the same way.

**Do they survive a crash? Yes — I tested it.** I killed the metrics job outright at 08:41:31 and it came
back by itself at **08:51:13 — nine minutes and forty seconds later**, with no help from anyone.

**How they come back, and the honest catch.** They are set to "restart after 1 minute if it fails", but that
is not what rescued it — Windows treated the killed job as finished rather than failed, so that never fired.
What actually rescued it was the **10-minute repeating timer** on each job. So:

> **If a job dies, expect it back within 10 minutes — not within 1 minute.**

For the metrics and equity jobs that costs nothing; they simply catch up. For the capture job, a crash at an
unlucky moment can cost **one 15-minute reading** (it only waits 4 minutes past a reading before giving up on
it). One missing reading in a session is recorded as missing and is not hidden.

**Do the timers start duplicates? No.** Each job is set to "do not start a new copy if one is running", and
the log confirms Windows is refusing the extra starts exactly as intended. I checked the running processes:
there is one of each, not two.

**Can you tell "alive" from "stuck" by looking? Now, yes — this was broken and I fixed it.**

The capture job used to print "sleeping until Monday 09:30" and then say nothing for thirteen hours. A job
waiting quietly and a job that died looked **identical** in the log — the last line of each said it was
armed. That is exactly why Friday's outage was invisible. It now prints one line every five minutes while it
waits, saying it is alive and what it is waiting for. It is running live as of 21:26 IST tonight:

```
waiting for 2026-09-21 09:30:00 (723 min to go); the capture loop is alive
```

The same change also protects against the sleeping laptop in §1: the job checks the clock every few seconds
rather than going to sleep for thirteen hours in one go, so a computer that wakes up mid-session finds the
job already back at work rather than still owing the rest of its nap.

**The one command to check all three are healthy:**

```
Get-Content C:\Users\SPS\Documents\Kanida_Falcon\logs\derivatives_capture_service.log -Tail 5; Get-Content C:\Users\SPS\Documents\Kanida_Falcon\logs\metrics_loop_service.log -Tail 5
```

If the newest line in either file is more than about 10 minutes old, that job is stuck or dead.

---

## 4. The chain from capture to screen

On Monday it should run like this, with nobody watching:

1. **09:30 IST** — the capture job wakes and takes the first reading of every in-scope contract.
2. **Within a minute** — the metrics job notices the new reading and computes the numbers from it.
3. The app's F&O routes read those numbers straight out of the store.
4. The Derivative tab shows them, each block stating which reading it is on.

The capture job keeps its own ledger of which readings it has taken, so if it restarts mid-session it picks
up exactly where it stopped and never re-reads a reading it already has.

**Places this can still break quietly, and where each shows up now:**

| Link | If it breaks | Is it visible? |
|---|---|---|
| Token dead | Nothing is collected all day | Only in the token log — **still silent**, see §2 |
| Capture job dead | Readings stop | Yes — its log now goes quiet, and the app shows the F&O warning (§5) |
| Metrics job dead | Readings captured but no numbers | Yes — its log goes quiet; it catches up when it returns (§6) |
| Underlying price missing | Blocks fall back to an older reading | Yes — the Derivative tab already says which reading each block is on |

---

## 5. A dead F&O capture is now visible on screen — this was the biggest gap

There was a route in the app called `/api/derivatives/capture` that reports whether anything was actually
*measured* in the F&O store. It had been built and tested, and **nothing in the app ever called it.**

That mattered because the green "data" badge at the top of the app describes the **share price and pattern
feed** — a completely different pipeline. On Friday afternoon that badge stayed green for hours while the
F&O book was dark, because share prices genuinely were fine.

**What I changed.** The data badge now carries a second, separate amber chip — *"F&O partly captured"*,
*"F&O not captured"* or *"F&O store unreadable"* — whenever F&O capture is degraded. Clicking the badge shows
the full reason in the server's own words, plus a line making clear the two feeds are different things.

Three deliberate limits:

- The chip appears **only** when something is wrong. A healthy capture shows nothing extra.
- It **never** changes the share-price badge's own colour or wording. Two feeds, two chips — a healthy price
  feed still reads as healthy.
- A failed request says nothing at all. It does not turn a network hiccup into a red warning.

---

## 6. The metrics job could not pick up anything it had missed — fixed

The metrics job only looked back **one day, measured on the computer's local date**. Because this machine
runs on Pacific time and the readings are stamped in Indian time, the two are different days for half of
every 24 hours.

**Measured, just now:** of the **52 readings currently in the store, the old query could reach zero of them.**
Anything it had ever missed was missed for good — which is why 17 Sep had to be recomputed by hand.

**What I changed.** It now looks back **30 days, in Indian time** — which is exactly as far back as the raw
data is kept, so everything still recoverable gets picked up on its own, and nothing older is hunted for
(the raw rows are gone by then, so it genuinely cannot be computed). It works newest-first, so a live session
is never stuck behind a backlog, and it says in the log when it is catching up on an older day rather than
doing it silently. The same 52 readings are now all reachable. The extra cost is 175 milliseconds once a
minute — I measured it.

---

## 7. The index prices that went missing on Friday

Friday's afternoon readings have no price for the six indices (NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY,
NIFTYNXT50, NIFTYFPI). The 210 ordinary stocks were already recovered from our own share-price store; the
indices are not in that store, so they can only come from Kite, which needs a live token.

**My call: one command, run by hand, not a new automatic job.** It needs a live token and it applies to one
specific past day, so making it automatic would mean a scheduled job that does nothing on almost every day
of its life.

**Run this on Monday any time after 09:30 IST, once the token is alive:**

```
cd C:\Users\SPS\Documents\Kanida_Falcon; market_scanner\.venv\Scripts\python.exe -m market_data.derivatives.spot_backfill --session 2026-09-18 --from-vendor --recompute
```

It fills in only the readings that are genuinely missing, records where each figure came from, and never
guesses one from a neighbouring reading. It prints a summary of what it filled.

This is a repair for Friday's gap. **It is not needed for Monday's own data** — as long as the machine stays
awake and the token is alive, Monday captures its own prices as it goes.

---

## 8. Disk and housekeeping

**Fine, with a lot of room. No action needed.**

- Free space on C: **376 GB**. At the measured 223 MB a day that is over four years.
- The F&O store is 760 MB and currently holds 2 days of raw readings and 14 days of candles.
- The automatic clean-up keeps 30 days of raw readings, 90 days of computed numbers and 180 days of candles.
  It has nothing to delete yet and will not have for weeks. It refuses to run at all if it would touch the
  day the app is showing, so it cannot delete something out from under a reader.

One thing to keep half an eye on: the store's write-ahead file (`derivatives.db-wal`) has grown to 615 MB.
It is harmless at this disk size, but it only shrinks when every reader lets go of the database. If it
reaches several gigabytes, restarting the app is enough to clear it.

---

## What is still fragile going into Monday

1. **The computer sleeping.** Not fixed, cannot be fixed by me, and it is the thing that broke Friday.
   Run the power command in §1 and keep the laptop plugged in.
2. **A token refresh failure is silent.** The failure path tries to notify a phone and there is nobody
   subscribed. If Monday looks empty, §2 is the first log to read.
3. **Up to a 10-minute gap after a crash**, which can cost one 15-minute reading of F&O data.
4. **Three unclean shutdowns in three days.** The jobs recovered each time, but the machine itself is not
   rock solid.

## What I could not check

- **I could not look at the app in a browser.** The development servers it is driven through (ports 8081 and
  8090) and the test copy of the app (8084) were all stopped by the unclean restart at 07:04 this morning and
  are not automatic jobs, so nothing brought them back. I was told not to start servers, so the on-screen
  F&O warning in §5 is verified by the automated checks and the type checker, not by a photograph of it.
  Your own copy of the app on port 8082 is untouched and still running.
- **I could not test a real token refresh**, because it is Sunday in India and the refresh worker correctly
  refuses to run outside market weekdays.
