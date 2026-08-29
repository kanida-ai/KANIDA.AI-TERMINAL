"""
Chart Agent · Kite daily-OHLC refresh (the fetcher the EOD job wires in).

This is the ONE piece that touches a market-data feed. It brings the Chart Agent's daily store
current up to a date D by fetching ONLY the missing tail (the gap) for each symbol from Kite and
appending it idempotently to the SAME daily store the detectors read (``agents.chart.data``).

Wiring (no code change in eod.py): set
    AGENT_CHART_FETCH_FN='agents.chart.fetch_kite:refresh_daily'
``eod.run_eod`` resolves that spec and calls ``refresh_daily(as_of_date)`` inside a guard — a fetch
failure is recorded, never fatal, and the scan still runs on whatever is already stored.

DESIGN CONTRACT (obeyed here):
  * POINT-IN-TIME is structural: for each symbol we request only (max_stored_date + 1 .. as_of) and
    we additionally DROP any returned bar dated > as_of before writing. A bar dated after as_of is
    never written, even if Kite returns a fresher one.
  * CORPORATE-ACTION ADJUSTMENT: Kite's ``historical_data(..., interval="day")`` (WITHOUT
    ``continuous=True``) returns split/bonus (corporate-action) ADJUSTED, dividend-UNADJUSTED prices
    — the SAME basis the existing daily store advertises (see agents/chart/data.py docstring:
    "corporate-action-adjusted, dividend-unadjusted, matches TradingView"). This is exactly how the
    R&D fetcher (Kanida_Falcon/scripts/fetch_ohlc.py) pulls daily bars. So we take Kite's native
    daily bars as-is — NO re-adjustment is applied or needed. See ``ADJUSTMENT`` below.
  * IDEMPOTENT append: SQLite uses INSERT OR IGNORE on PK (symbol, bar_time); Parquet rewrites the
    symbol's hive partition from a (symbol,date)-deduped union. Re-running a date adds no duplicate.
  * GUARDED: a per-symbol failure (no token, Kite error) is recorded in ``errors`` and skipped — one
    bad symbol never sinks the batch, and nothing here can crash app boot (all heavy imports lazy).

Execution boundary: this only READS market data and WRITES the local daily store. It never routes an
order, touches a broker order API, git, shell, or deploy.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

log = logging.getLogger("agents.chart.fetch_kite")

# How Kite daily bars map onto the store's adjustment basis. Surfaced verbatim in the result so a
# reviewer can audit the correctness gate without reading the code.
ADJUSTMENT = ("kite-native corporate-action-adjusted, dividend-unadjusted "
              "(historical_data interval='day', no continuous flag) — matches store basis; "
              "no re-adjustment applied")

# Kite caps a single day-interval request at 2000 calendar days. Gap fills are tiny; this only
# matters for back-filling a brand-new symbol's full history.
_KITE_DAY_CAP = 2000
# When a symbol has NO stored bars yet, how far back to seed it (calendar days). Gap fills of the
# existing store never hit this (they resume from the last stored bar).
_DEFAULT_SEED_DAYS = 800


# --------------------------------------------------------------------------- date helpers
def _as_date(d) -> date:
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    return date.fromisoformat(str(d)[:10])


def _day_chunks(start: date, end: date):
    """Yield (lo, hi) windows no wider than the Kite day cap, covering start..end inclusive."""
    cur = start
    while cur <= end:
        hi = min(cur + timedelta(days=_KITE_DAY_CAP - 1), end)
        yield cur, hi
        cur = hi + timedelta(days=1)


# --------------------------------------------------------------------------- store: max stored date
def _max_stored_sqlite(sqlite_path: str, symbol: str):
    import os
    import sqlite3
    con = sqlite3.connect(f"file:{os.path.abspath(sqlite_path)}?mode=ro", uri=True)
    try:
        row = con.execute("SELECT max(substr(bar_time,1,10)) FROM ohlc_daily WHERE symbol=?",
                          (symbol,)).fetchone()
    finally:
        con.close()
    return date.fromisoformat(row[0]) if row and row[0] else None


def _max_stored_parquet(symbol: str):
    from . import data
    con = data._duckdb_con()
    try:
        row = con.execute(
            f"SELECT max(CAST(date AS DATE)) FROM read_parquet('{data._parquet_glob()}', "
            f"hive_partitioning=1) WHERE symbol=?", [symbol]).fetchone()
    finally:
        con.close()
    if not row or row[0] is None:
        return None
    return _as_date(row[0])


# --------------------------------------------------------------------------- store: append
def _append_sqlite(sqlite_path: str, symbol: str, token, bars) -> int:
    """INSERT OR IGNORE daily bars for one symbol. PK (symbol, bar_time) makes it idempotent — a
    re-run of the same date inserts 0 new rows. Returns the number of rows actually added."""
    import os
    import sqlite3
    if not bars:
        return 0
    con = sqlite3.connect(os.path.abspath(sqlite_path), timeout=60.0)
    try:
        con.execute(
            "CREATE TABLE IF NOT EXISTS ohlc_daily (symbol TEXT NOT NULL, instrument_token INTEGER, "
            "bar_time TEXT NOT NULL, open REAL, high REAL, low REAL, close REAL, volume INTEGER, "
            "PRIMARY KEY (symbol, bar_time))")
        before = con.total_changes
        payload = [(symbol, token, b["_bar_time"], b["open"], b["high"], b["low"], b["close"],
                    b["volume"]) for b in bars]
        con.executemany(
            "INSERT OR IGNORE INTO ohlc_daily "
            "(symbol, instrument_token, bar_time, open, high, low, close, volume) "
            "VALUES (?,?,?,?,?,?,?,?)", payload)
        con.commit()
        return con.total_changes - before
    finally:
        con.close()


def _append_parquet(symbol: str, bars) -> int:
    """Upsert one symbol's hive partition (``symbol=<sym>/``) in the AGENT_DATA_URI Parquet store.

    Rewrites the partition from the (symbol,date)-deduped UNION of existing rows + new rows, so a
    re-run adds no duplicate (symbol,date) and the reader's glob never sees two rows for one date.
    Local partitions are collapsed to a single file (stray files removed) to keep the guarantee even
    across re-runs. Returns net new dates added."""
    import pandas as pd
    from . import data

    if not bars:
        return 0
    uri = data._data_uri().rstrip("/")
    new_df = pd.DataFrame([{"date": b["_date"], "open": b["open"], "high": b["high"],
                            "low": b["low"], "close": b["close"], "volume": b["volume"]}
                           for b in bars])
    new_df["date"] = pd.to_datetime(new_df["date"]).dt.normalize()

    # Existing rows for this symbol (empty if the partition does not exist yet).
    try:
        con = data._duckdb_con()
        try:
            existing = con.execute(
                f"SELECT CAST(date AS DATE) date, open, high, low, close, volume "
                f"FROM read_parquet('{data._parquet_glob()}', hive_partitioning=1) WHERE symbol=?",
                [symbol]).df()
        finally:
            con.close()
        existing["date"] = pd.to_datetime(existing["date"]).dt.normalize()
    except Exception:  # noqa: BLE001 — no partition yet / unreadable: treat as empty
        existing = new_df.iloc[0:0].copy()

    before = len(existing)
    # keep="last" → a re-fetch refreshes a date's values (e.g. after a corp action re-adjust) while
    # staying idempotent on identical re-runs.
    merged = (pd.concat([existing, new_df], ignore_index=True)
              .drop_duplicates(subset=["date"], keep="last")
              .sort_values("date").reset_index(drop=True))
    added = len(merged) - before

    part_dir = f"{uri}/symbol={symbol}"
    if uri.startswith("s3://"):
        # s3: write the deduped partition via duckdb. NOTE: pre-existing multi-file partitions on s3
        # are not auto-deleted here — the prod pipeline writes one file per partition, so this stays
        # single-file. Flagged as residual risk in the module report.
        con = data._duckdb_con()
        try:
            con.register("merged_df", merged)
            con.execute(f"COPY (SELECT * FROM merged_df) TO '{part_dir}/part-0.parquet' "
                        f"(FORMAT PARQUET)")
        finally:
            con.close()
    else:
        import glob as _glob
        import os
        os.makedirs(part_dir, exist_ok=True)
        for f in _glob.glob(os.path.join(part_dir, "*.parquet")):
            os.remove(f)
        merged.to_parquet(os.path.join(part_dir, "part-0.parquet"), index=False)
    return max(added, 0)


# --------------------------------------------------------------------------- Kite client / tokens
def _get_kite_client():
    """The backend-native authenticated Kite client (services.kite_auth is the single source of truth
    for Kite credentials — token read from the kite_tokens DB / KITE_ACCESS_TOKEN env, NEVER
    hardcoded). Imported lazily so this module imports even where kiteconnect/services are absent."""
    from services.kite_auth import get_kite_client
    return get_kite_client()


def _build_token_map(kite) -> dict:
    """{tradingsymbol -> instrument_token} for NSE cash equities, from one kite.instruments('NSE')
    call (mirrors the R&D fetcher's resolver)."""
    out: dict = {}
    for inst in kite.instruments("NSE"):
        if inst.get("segment") != "NSE":
            continue
        sym = inst.get("tradingsymbol")
        tok = inst.get("instrument_token")
        if sym and tok:
            out[sym] = int(tok)
    return out


# --------------------------------------------------------------------------- public entry point
def refresh_daily(as_of_date, symbols=None, _client=None) -> dict:
    """Bring the Chart Agent daily store current up to ``as_of_date`` by gap-fetching from Kite.

    Args:
        as_of_date: the date D to refresh through (str 'YYYY-MM-DD' / date / datetime).
        symbols:    universe to refresh; default = the symbols already in the store
                    (``data.all_symbols()``).
        _client:    test seam — an already-authenticated Kite client (else services.kite_auth
                    provides one). Never used in prod wiring (eod calls refresh_daily(as_of) only).

    Returns:
        {as_of_date, symbols_fetched, date_range, rows_added, adjustment, errors: [...]}
    """
    from . import data

    as_of = _as_date(as_of_date)
    as_of_iso = as_of.isoformat()

    using_parquet = bool(data._data_uri())
    sqlite_path = data._sqlite_path()

    universe = list(symbols) if symbols else list(data.all_symbols())

    result = {"as_of_date": as_of_iso, "symbols_fetched": 0, "date_range": None,
              "rows_added": 0, "adjustment": ADJUSTMENT, "errors": []}
    if not universe:
        result["errors"].append("empty universe — nothing to refresh")
        return result

    # Establish the client + token map ONCE for the batch. A failure here is fatal to the batch (no
    # feed at all) but is raised so eod.run_eod records it as fetched='error: ...' and scans anyway.
    kite = _client if _client is not None else _get_kite_client()
    token_map = _build_token_map(kite)

    min_from: date | None = None
    max_to: date | None = None
    symbols_fetched = 0
    rows_added = 0

    for sym in universe:
        try:
            last = (_max_stored_parquet(sym) if using_parquet
                    else _max_stored_sqlite(sqlite_path, sym))
            gap_from = (last + timedelta(days=1)) if last else (as_of - timedelta(days=_DEFAULT_SEED_DAYS))
            if gap_from > as_of:
                continue  # symbol already current to as_of — request nothing (PIT gap = empty)

            token = token_map.get(sym)
            if not token:
                result["errors"].append({"symbol": sym, "error": "no NSE instrument_token"})
                continue

            raw = []
            for lo, hi in _day_chunks(gap_from, as_of):
                raw.extend(kite.historical_data(
                    token, datetime.combine(lo, datetime.min.time()),
                    datetime.combine(hi, datetime.max.time()), "day"))

            # POINT-IN-TIME guard (structural): drop anything dated > as_of before writing.
            bars = []
            for b in raw:
                bd = _as_date(b["date"])
                if bd > as_of:
                    continue
                bars.append({
                    "_date": bd, "_bar_time": bd.strftime("%Y-%m-%d 00:00:00"),
                    "open": float(b["open"]), "high": float(b["high"]),
                    "low": float(b["low"]), "close": float(b["close"]),
                    "volume": int(b.get("volume") or 0)})
            if not bars:
                continue

            added = (_append_parquet(sym, bars) if using_parquet
                     else _append_sqlite(sqlite_path, sym, token, bars))
            rows_added += added
            symbols_fetched += 1
            b_min, b_max = bars[0]["_date"], bars[-1]["_date"]
            min_from = b_min if min_from is None else min(min_from, b_min)
            max_to = b_max if max_to is None else max(max_to, b_max)
        except Exception as e:  # noqa: BLE001 — one bad symbol must not sink the batch
            result["errors"].append({"symbol": sym, "error": f"{type(e).__name__}: {e}"})
            log.warning("refresh_daily(%s): %s failed (skipped): %s", as_of_iso, sym, e)

    # Invalidate the in-process daily caches so the very next scan sees the freshly appended tail
    # (the caches are keyed on the active source, but the underlying file changed under them).
    try:
        data.load_daily.cache_clear()
        data.all_symbols.cache_clear()
        data._nifty_close.cache_clear()
    except Exception:  # noqa: BLE001
        pass

    result["symbols_fetched"] = symbols_fetched
    result["rows_added"] = rows_added
    if min_from and max_to:
        result["date_range"] = [min_from.isoformat(), max_to.isoformat()]
    log.info("refresh_daily(%s): %d symbols, %d rows added, range=%s, %d errors",
             as_of_iso, symbols_fetched, rows_added, result["date_range"], len(result["errors"]))
    return result
