"""Reconstruct a **past** session's marks from the 15-minute candles.

Why this exists: the live loop can only snapshot a mark while that mark is
happening.  Before the first session the loop has run, the store has candles
(from the backfill) but no marks, so nothing downstream can render.  This module
rebuilds the marks of a session that has already closed, from the exchange's own
15-minute bars.

It is a **reconstruction and says so on every row**: ``source`` is
``'kite.candles_15m'``, not ``'kite.quote'``.  What a candle can support is
written; what only a live quote carries is left NULL and never guessed:

    last_price      = the bar's close            (real)
    volume          = day volume cumulative to this bar, summed from the bars
                      of that session (real, and the same "cumulative day
                      volume" meaning a quote's `volume` has)
    oi              = the bar's open interest    (real)
    day_open/high/low, prev_close                (real, from the bars)
    average_price, bid, ask, buy/sell quantity   = NULL — a candle has none,
                      so anything derived from them will honestly read
                      "no average price" rather than a number we made up.
    average_price_est = an ESTIMATE in its own column: the volume-weighted
                      (high+low+close)/3 of the session's bars so far.  It is
                      not the exchange's VWAP and must never be displayed as
                      one; a screen that uses it has to say "estimated".

The spot leg comes from the underlying's own 15-minute bars on NSE, one request
per underlying.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Iterable, Mapping, Sequence

from . import config
from .capture import _fmt, snapshot_id_for
from .instruments import kite_call, spot_map
from .store import DerivativesStore, utcnow

LOG = logging.getLogger("market_data.derivatives.seed")

SOURCE = "kite.candles_15m"


def _mark_for(bar_start: datetime) -> tuple[datetime, str]:
    """The mark a bar belongs to: its close."""
    mark = bar_start + timedelta(minutes=config.BAR_MINUTES)
    kind = (config.MARK_POST_CLOSE if bar_start.time() >= config.SESSION_CLOSE
            else config.MARK_BAR_CLOSE)
    return mark, kind


def fetch_spot_series(provider, underlyings: Sequence[str], session: date
                      ) -> dict[str, tuple[str, dict[str, float]]]:
    """``underlying -> (nse_symbol, {mark -> close})`` for one session."""
    spots = spot_map(provider, underlyings)
    out: dict[str, tuple[str, dict[str, float]]] = {}
    frm = datetime.combine(session, datetime.min.time())
    to = datetime.combine(session, datetime.max.time().replace(microsecond=0))
    for underlying, (symbol, token) in spots.items():
        try:
            raw = kite_call(provider, lambda k, t=token: k.historical_data(
                t, frm, to, "15minute"))
        except Exception as e:  # noqa: BLE001 — a missing spot is NULL, not fatal
            LOG.warning("no spot series for %s (%s): %s", underlying, symbol,
                        type(e).__name__)
            continue
        series: dict[str, float] = {}
        for c in raw or []:
            stamp = c.get("date")
            if stamp is None or c.get("close") is None:
                continue
            mark, _kind = _mark_for(stamp.replace(tzinfo=None))
            series[_fmt(mark)] = float(c["close"])
        if series:
            out[underlying] = (symbol, series)
    return out


def seed_session(store: DerivativesStore, provider, *, session: date,
                 underlyings: Sequence[str] | None = None,
                 with_spot: bool = True, keep_captured: bool = True) -> dict:
    """Rebuild every mark of ``session`` for the given underlyings.

    Returns a summary dict.  Idempotent: the rows are keyed by (contract, mark)
    exactly as a live capture's are, so re-seeding replaces like with like — and
    a later live capture of the same mark would overwrite the reconstruction
    with the real quote, which is the right precedence.

    ``keep_captured`` (the default) applies that precedence in the other direction too: a mark the
    live capture already wrote is left alone. A candle has no ``average_price``, so reconstructing
    over a real quote would replace the exchange's own VWAP with NULL and silently empty every
    signal that rests on premium traded. Only the marks the capture missed are rebuilt.
    """
    run_id = f"seed_{session:%Y%m%d}_{now_stamp()}"
    store.start_run(run_id, "seed", f"reconstruct {session} from candles_15m")

    where = "WHERE in_scope=1"
    params: list = []
    if underlyings:
        where += f" AND underlying IN ({','.join('?' * len(underlyings))})"
        params += [u.upper() for u in underlyings]
    contracts = {int(r["instrument_token"]): r
                 for r in store.con.execute(f"SELECT * FROM contracts {where}", params)}
    if not contracts:
        raise LookupError("no in-scope contracts for that underlying set")

    tokens = list(contracts)
    day = session.isoformat()
    rows: list[tuple] = []
    per_token_bars: dict[int, list] = defaultdict(list)
    chunk = 900
    for i in range(0, len(tokens), chunk):
        part = tokens[i:i + chunk]
        sql = ("SELECT instrument_token, bar_start, open, high, low, close, volume, oi "
               "FROM candles_15m WHERE substr(bar_start,1,10)=? AND instrument_token IN "
               f"({','.join('?' * len(part))}) ORDER BY instrument_token, bar_start")
        for r in store.con.execute(sql, [day, *part]):
            per_token_bars[int(r["instrument_token"])].append(r)

    prev_close: dict[int, float] = {}
    for i in range(0, len(tokens), chunk):
        part = tokens[i:i + chunk]
        sql = ("SELECT instrument_token, close FROM candles_15m c WHERE bar_start = ("
               "  SELECT MAX(bar_start) FROM candles_15m x "
               "   WHERE x.instrument_token=c.instrument_token AND substr(x.bar_start,1,10) < ?"
               ") AND instrument_token IN " f"({','.join('?' * len(part))})")
        for r in store.con.execute(sql, [day, *part]):
            if r["close"] is not None:
                prev_close[int(r["instrument_token"])] = float(r["close"])

    fetched_at = utcnow()
    agg: dict[str, dict[str, dict]] = defaultdict(dict)   # mark -> underlying -> sums
    for token, bars in per_token_bars.items():
        contract = contracts[token]
        cum_volume = 0
        cum_value = 0.0
        day_open = None
        day_high = None
        day_low = None
        for bar in bars:
            bar_start = datetime.strptime(bar["bar_start"], "%Y-%m-%d %H:%M:%S")
            mark, kind = _mark_for(bar_start)
            bar_volume = int(bar["volume"] or 0)
            cum_volume += bar_volume
            if bar["high"] is not None and bar["low"] is not None and bar["close"] is not None:
                cum_value += bar_volume * (bar["high"] + bar["low"] + bar["close"]) / 3.0
            if day_open is None:
                day_open = bar["open"]
            day_high = bar["high"] if day_high is None else max(day_high, bar["high"])
            day_low = bar["low"] if day_low is None else min(day_low, bar["low"])
            stamp = _fmt(mark)
            rows.append((
                token, stamp, kind, bar["close"], None, cum_volume, bar["oi"],
                None, None, None, None, None, None, None, None,
                day_open, day_high, day_low, prev_close.get(token),
                None, bar["bar_start"], SOURCE, config.VENDOR_ID, fetched_at,
                snapshot_id_for(mark),
                (cum_value / cum_volume) if cum_volume else None,
            ))
            slot = agg[stamp].setdefault(contract["underlying"], {
                "kind": kind, "ce_oi": 0, "pe_oi": 0, "ce_vol": 0, "pe_vol": 0,
                "ce_n": 0, "pe_n": 0, "fut_price": None, "fut_token": None,
                "fut_expiry": None})
            itype = contract["instrument_type"]
            if itype == "CE":
                slot["ce_oi"] += int(bar["oi"] or 0)
                slot["ce_vol"] += cum_volume
                slot["ce_n"] += 1
            elif itype == "PE":
                slot["pe_oi"] += int(bar["oi"] or 0)
                slot["pe_vol"] += cum_volume
                slot["pe_n"] += 1
            else:
                expiry = contract["expiry"]
                if slot["fut_expiry"] is None or expiry < slot["fut_expiry"]:
                    slot["fut_expiry"] = expiry
                    slot["fut_price"] = bar["close"]
                    slot["fut_token"] = token

    skipped_marks: list[str] = []
    if keep_captured:
        captured = {r[0] for r in store.con.execute(
            "SELECT DISTINCT captured_at FROM snapshots WHERE substr(captured_at,1,10)=?"
            " AND average_price IS NOT NULL", [session.isoformat()])}
        if captured:
            skipped_marks = sorted(captured)
            before = len(rows)
            rows = [r for r in rows if r[1] not in captured]
            for stamp in captured:
                agg.pop(stamp, None)
            LOG.info("seed %s: leaving %d live-captured mark(s) untouched (%d of %d rows skipped)",
                     session, len(captured), before - len(rows), before)
    written = store.write_snapshots(rows)

    spot_series: dict[str, tuple[str, dict[str, float]]] = {}
    if with_spot:
        names = sorted({contracts[t]["underlying"] for t in per_token_bars})
        spot_series = fetch_spot_series(provider, names, session)

    u_rows = []
    for stamp, per_underlying in sorted(agg.items()):
        for underlying, slot in sorted(per_underlying.items()):
            symbol, series = spot_series.get(underlying, (None, {}))
            u_rows.append((
                underlying, stamp, slot["kind"], series.get(stamp), symbol,
                slot["fut_price"], slot["fut_token"], slot["ce_oi"], slot["pe_oi"],
                slot["ce_vol"], slot["pe_vol"], slot["ce_n"], slot["pe_n"],
                config.VENDOR_ID, fetched_at, snapshot_id_for(
                    datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S")),
            ))
    u_written = store.write_underlying_snapshots(u_rows)

    trading_marks = [m for m in agg if not m.endswith("15:45:00")]
    if trading_marks:
        store.set_meta("latest_trading_mark", max(trading_marks))
    marks = sorted(agg)
    summary = {
        "run_id": run_id, "session": day, "source": SOURCE,
        "contracts": len(per_token_bars), "snapshot_rows": written,
        "underlying_rows": u_written, "marks": len(marks),
        "kept_captured_marks": skipped_marks,
        "first_mark": marks[0] if marks else None,
        "last_mark": marks[-1] if marks else None,
        "underlyings": sorted({contracts[t]["underlying"] for t in per_token_bars}),
        "spot_underlyings": sorted(spot_series),
    }
    store.finish_run(run_id, status="ok", rows=written + u_written,
                     detail=str(summary))
    LOG.info("seeded %s: %d snapshot rows over %d marks for %d contracts (%s)",
             day, written, len(marks), len(per_token_bars), ", ".join(summary["underlyings"]))
    return summary


def now_stamp() -> str:
    from .capture import now_ist

    return f"{now_ist():%H%M%S}"


__all__ = ["seed_session", "fetch_spot_series", "SOURCE"]
