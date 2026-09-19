"""Constants for the derivatives capture pipeline.

Spec: ``docs/DERIVATIVES_SPEC.md`` §1, §2, §5 and
``docs/DATA_PIPELINE_CONTRACT.md`` §1/§2A.

Everything that a reader might otherwise have to guess at — how many expiries
are "front", when a snapshot is taken, how late is too late, what the liquidity
floors are, how long rows are kept — is a named constant here so the UI and the
report can quote the same number the code used.
"""
from __future__ import annotations

from datetime import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

#: The derivatives store.  `db/kanida.db` and `db/market15.db` are read-only
#: for this package; nothing here ever opens them for writing.
DEFAULT_DB_PATH = ROOT / "db" / "derivatives.db"
CACHE_DIR = ROOT / "db" / "derivatives_cache"

VENDOR_ID = "kite"
EXCHANGE = "NFO"
SPOT_EXCHANGE = "NSE"

# ── scope (spec §1) ──────────────────────────────────────────────────────────

#: front two expiries per underlying, per kind (options and futures separately,
#: because an index has weeklies on the option side and only monthlies on the
#: futures side).
EXPIRIES_PER_UNDERLYING = 2

# ── session grid (NFO) ───────────────────────────────────────────────────────
#
# Verified on Kite 2026-09-18 for RELIANCE futures: NFO 15-minute history has
# **26** bars per session — 09:15 … 15:15 (the continuous session) plus a
# terminal 15:30 bar.  The cash-segment CAS rule of DATA_PIPELINE_CONTRACT §2A
# (F&O *underlyings* stop continuous trading at 15:15 from 2026-08-03) applies
# to the equity leg, not to NFO: derivatives trade to 15:30.  That matters here
# in one place — the **spot** quote at and after the 15:15 mark is the auction /
# close print for a CAS underlying, not a traded 15-minute bar, which is why
# `underlying_snapshots` records `spot_symbol` and the mark it was taken at and
# never claims the spot is an intraday bar close.
SESSION_OPEN = time(9, 15)
SESSION_CLOSE = time(15, 30)
BAR_MINUTES = 15

#: Snapshot marks: every 15-minute candle close 09:30 … 15:30 (25 of them) …
FIRST_MARK = time(9, 30)
LAST_MARK = time(15, 30)
#: … plus one after the close, which is the close of the terminal 15:30 NFO bar.
POST_CLOSE_MARK = time(15, 45)

MARK_BAR_CLOSE = "bar_close"
MARK_POST_CLOSE = "post_close"

#: Wait this long after a mark before quoting, so the exchange has published the
#: bar's last prints.  A quote is a running snapshot, not a bar, so this is a
#: settling delay, not a data-availability rule.
MARK_LAG_SECONDS = 20

#: A mark can only be captured within this many seconds of itself.  Later than
#: that and the cumulative volume/OI in the quote belongs to a different part of
#: the day, so the mark is recorded as **missed** rather than filled with a
#: quote that is not what it claims to be (spec §5: never fabricate a row).
MARK_GRACE_SECONDS = 240

# ── requests ─────────────────────────────────────────────────────────────────

#: Kite accepts up to 500 instruments per `quote()` call.
QUOTE_BATCH = 500
#: Shared 3 req/s limiter lives in `market_data.ratelimit`; never a second one.

#: 15-minute history: Kite serves ~100 days per request, so 10 sessions is one
#: request per contract.
BACKFILL_SESSIONS = 10
BACKFILL_CALENDAR_DAYS = 20  # ~10 sessions of slack for weekends/holidays

#: Daily history: Kite serves ~2000 days per request, so one request covers a
#: futures contract's WHOLE life.  A future is listed roughly three months
#: before it expires, so ~120 calendar days reaches back past its first
#: session with room to spare; asking for more simply returns nothing earlier,
#: because nothing earlier exists.
DAILY_CALENDAR_DAYS = 120
#: Fewer distinct sessions than this and a daily series is too short to read as
#: a trend.  It is stated, never hidden: when the front contract rolls, the new
#: one starts with almost no history and the chart has to say so rather than
#: draw a stub that looks broken.
SHORT_HISTORY_SESSIONS = 10

# ── liquidity floors (spec §3, "a screen without floors is a junk list") ─────

#: Premium traded = volume (units) × average_price.  ₹2 crore.
MIN_PREMIUM_RS = 2_00_00_000.0
#: Open interest of at least one lot, in units (OI from Kite is in units).
MIN_OI_LOTS = 1
#: A contract priced below ₹1 is tick noise.
MIN_LAST_PRICE = 1.0

#: Backfill-time floor.  Before the open there is no traded premium yet, so the
#: candidate screen uses what exists at that hour: standing open interest and
#: the previous close.  Futures are always backfilled.
BACKFILL_MIN_OI_LOTS = 1
BACKFILL_MIN_PRICE = 1.0

# ── retention (spec §2) ──────────────────────────────────────────────────────

RAW_SNAPSHOT_DAYS = 90
METRICS_DAYS = 365
#: daily roll-ups and contracts are kept for good.

# ── writing ──────────────────────────────────────────────────────────────────

#: A contract with no open interest, no volume and no price carries no
#: information at all; storing 26 such rows a day for thousands of dead strikes
#: buys nothing.  These are counted as `rows_skipped` on the capture row so the
#: absence is explicit and auditable, never silent.
SKIP_EMPTY_CONTRACTS = True

__all__ = [n for n in dir() if not n.startswith("_")]
