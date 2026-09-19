"""Read-side helpers over ``db/derivatives.db`` — no writes, no network.

Today this holds one thing: the per-strike **ΔOI-since-previous-close** series
that the chart grid draws, for the ten contracts around the money (ATM CE and
the four strikes above it, ATM PE and the four below).

The rules it will not bend:

* **ΔOI is measured against the contract's previous-session closing OI.**  No
  previous close ⇒ ``delta_oi`` is ``None`` and the contract reads
  ``"no baseline"``; it is never measured against the day's first mark and
  presented as if it were the close.
* **A missing mark stays missing.**  Points exist only for marks that actually
  have a snapshot row for that contract.  Nothing is interpolated, carried
  forward or back-filled.
* **ATM is the listed strike nearest the spot**, and which mark's spot was used
  is reported (``atm_basis``), because the ATM drifts during the day and a
  series whose identity moved mid-chart would be unreadable.
"""
from __future__ import annotations

import sqlite3
from bisect import bisect_left
from datetime import date, datetime, timedelta
from typing import Any, Mapping, Sequence

from . import config

__all__ = ["strike_oi_series", "atm_strike", "DIRECTION_LOOKBACK_MARKS",
           "FLAT_FRACTION", "PRICE_FLAT_FRACTION", "FLOW_LABELS",
           "futures_chart_series", "front_future", "session_grid", "with_gaps",
           "CHART_INTERVALS", "CHART_TABLES", "CHART_MAX_CANDLES"]

#: The two cadences the Derivative tab's price chart offers, and the table each
#: one is read from.  15 minutes is the default; daily is the alternative.
CHART_INTERVALS = ("15m", "1d")
CHART_TABLES = {"15m": ("candles_15m", "bar_start"), "1d": ("candles_day", "session_date")}
#: A hard ceiling on one response.  A futures contract lives about three months
#: — ~1,560 fifteen-minute bars — so this never truncates a contract's own
#: history; it only stops a pathological store from being served whole.
CHART_MAX_CANDLES = 2000

#: "now versus 4 marks ago" — an hour of 15-minute marks.
DIRECTION_LOOKBACK_MARKS = 4
#: |change| below this fraction of the contract's own largest |ΔOI| today is flat.
FLAT_FRACTION = 0.05
#: the same 5% shape on the contract's OWN premium: |price change over the
#: window| below this fraction of its largest |price move from the day's first
#: mark| today is flat.  Its own constant so both sides can be compared.
PRICE_FLAT_FRACTION = 0.05

#: (option type, price direction, OI direction) -> (what is happening, what it
#: means).  The SAME table `kanida_pilot.derivatives.FLOW_LABELS` serves; the
#: check script reads both files and refuses a difference.  Nothing in it says
#: what happens next — each row names who appears to be doing what, right now.
#: All nine (price x OI) combinations are listed per option type, so a flat axis
#: has its own row: when OI has a direction the sentence says something happened
#: to open interest, and the tile can never contradict its own chip.
FLOW_LABELS = {
 'CE|down|building':('Call writing increasing','Sellers are building resistance'),
 'CE|up|unwinding':('Call short covering','Call sellers are exiting'),
 'CE|up|building':('Call buying increasing','Traders are buying upside'),
 'CE|down|unwinding':('Call buyers exiting','Call buyers are closing out'),
 'CE|flat|building':('New positions added','Premium barely moved'),
 'CE|flat|unwinding':('Positions closing out','Premium barely moved'),
 'CE|up|flat':('Premium rose','Open interest barely moved'),
 'CE|down|flat':('Premium fell','Open interest barely moved'),
 'CE|flat|flat':('Very little change','Positioning is unchanged'),
 'PE|down|building':('Put writing increasing','Sellers are building support'),
 'PE|up|unwinding':('Put short covering','Put sellers are exiting'),
 'PE|up|building':('Put buying increasing','Traders are buying downside protection'),
 'PE|down|unwinding':('Put buyers exiting','Put buyers are closing out'),
 'PE|flat|building':('New positions added','Premium barely moved'),
 'PE|flat|unwinding':('Positions closing out','Premium barely moved'),
 'PE|up|flat':('Premium rose','Open interest barely moved'),
 'PE|down|flat':('Premium fell','Open interest barely moved'),
 'PE|flat|flat':('Very little change','Positioning is unchanged'),
}
# only when BOTH axes are flat; a tile whose OI moved must never say this
FLOW_FLAT_WHAT = 'Very little change'
FLOW_FLAT_MEANING = 'Positioning is unchanged'
FLOW_NOT_ENOUGH = 'Not enough readings yet'

#: how many strikes either side of the money.
DEFAULT_WIDTH = 4


def _conn(source) -> sqlite3.Connection:
    con = getattr(source, "con", source)
    con.row_factory = sqlite3.Row
    return con


def atm_strike(strikes: Sequence[float], spot: float) -> float | None:
    """The listed strike nearest ``spot`` (ties go to the higher strike)."""
    if not strikes or spot is None:
        return None
    ladder = sorted(strikes)
    i = bisect_left(ladder, spot)
    if i == 0:
        return ladder[0]
    if i >= len(ladder):
        return ladder[-1]
    below, above = ladder[i - 1], ladder[i]
    return below if (spot - below) < (above - spot) else above


def _front_expiry(con, underlying: str) -> str | None:
    row = con.execute(
        "SELECT MIN(expiry) FROM contracts WHERE in_scope=1 AND underlying=? "
        "AND instrument_type IN ('CE','PE')", (underlying,)).fetchone()
    return row[0] if row and row[0] else None


def _session_of(con, session: date | str | None) -> str | None:
    if session is not None:
        return session.isoformat() if isinstance(session, date) else str(session)
    row = con.execute("SELECT MAX(captured_at) FROM snapshots").fetchone()
    return row[0][:10] if row and row[0] else None


def _as_of(con, session: str, underlying: str) -> tuple[str | None, float | None, str | None]:
    """The newest *trading* mark of ``session`` and that mark's spot."""
    row = con.execute(
        "SELECT captured_at, spot, spot_symbol FROM underlying_snapshots "
        "WHERE underlying=? AND substr(captured_at,1,10)=? AND mark_kind=? "
        "AND spot IS NOT NULL ORDER BY captured_at DESC LIMIT 1",
        (underlying, session, config.MARK_BAR_CLOSE)).fetchone()
    if row is None:
        return None, None, None
    return row["captured_at"], row["spot"], row["spot_symbol"]


def _previous_close_oi(con, tokens: Sequence[int], session: str) -> dict[int, float]:
    """Each contract's OI at the last bar of the last session before ``session``.

    Read from ``candles_15m`` (the exchange's own bars).  A contract the
    backfill has never covered simply has no entry, and its ΔOI stays ``None``.
    """
    if not tokens:
        return {}
    marks = ",".join("?" * len(tokens))
    sql = f"""
        WITH prior AS (
            SELECT instrument_token, bar_start, oi, substr(bar_start,1,10) AS d
              FROM candles_15m
             WHERE substr(bar_start,1,10) < ? AND instrument_token IN ({marks})
        ), last_day AS (
            SELECT instrument_token, MAX(d) AS d FROM prior GROUP BY instrument_token
        ), last_bar AS (
            SELECT p.instrument_token, MAX(p.bar_start) AS bar_start
              FROM prior p JOIN last_day l ON l.instrument_token = p.instrument_token
               AND l.d = p.d
             GROUP BY p.instrument_token
        )
        SELECT c.instrument_token, c.oi
          FROM candles_15m c JOIN last_bar b
            ON b.instrument_token = c.instrument_token AND b.bar_start = c.bar_start
    """
    out: dict[int, float] = {}
    for r in con.execute(sql, [session, *tokens]):
        if r["oi"] is not None:
            out[int(r["instrument_token"])] = float(r["oi"])
    return out


def _direction(points: Sequence[Mapping[str, Any]]) -> tuple[str, dict]:
    """``building`` / ``unwinding`` / ``flat`` / ``no baseline`` + its workings."""
    usable = [p for p in points if p["delta_oi"] is not None]
    if len(usable) < 2:
        return "no baseline", {"points_with_delta": len(usable)}
    latest = usable[-1]
    back = DIRECTION_LOOKBACK_MARKS
    reference = usable[-1 - back] if len(usable) > back else usable[0]
    change = latest["delta_oi"] - reference["delta_oi"]
    scale = max(abs(p["delta_oi"]) for p in usable)
    threshold = FLAT_FRACTION * scale
    if scale == 0 or abs(change) < threshold:
        label = "flat"
    else:
        label = "building" if change > 0 else "unwinding"
    return label, {
        "from": reference["at"], "to": latest["at"], "change": change,
        "flat_threshold": threshold, "marks_back": len(usable) - 1 - usable.index(reference),
    }


def _price_direction(points: Sequence[Mapping[str, Any]]) -> tuple[str, dict]:
    """``up`` / ``down`` / ``flat`` / ``no baseline`` on the contract's own price.

    The same shape as ``_direction``, read on the premium: now against the mark
    ``DIRECTION_LOOKBACK_MARKS`` back, flat inside ``PRICE_FLAT_FRACTION`` of
    the contract's own largest |move from the day's first priced mark|.  Fewer
    than two marks carrying a price is ``"no baseline"``, never a direction.
    """
    usable = [p for p in points if p.get("price") is not None]
    if len(usable) < 2:
        return "no baseline", {"points_with_price": len(usable)}
    back = DIRECTION_LOOKBACK_MARKS
    reference = usable[-1 - back] if len(usable) > back else usable[0]
    latest = usable[-1]
    change = latest["price"] - reference["price"]
    first = usable[0]["price"]
    scale = max(abs(p["price"] - first) for p in usable)
    threshold = PRICE_FLAT_FRACTION * scale
    if scale == 0 or abs(change) < threshold:
        label = "flat"
    else:
        label = "up" if change > 0 else "down"
    return label, {
        "from": reference["at"], "to": latest["at"], "price_change": change,
        "price_change_pct": None if not reference["price"] else change / reference["price"] * 100,
        "price_flat_threshold": threshold,
        "readings_back": len(usable) - 1 - usable.index(reference),
    }


def _flow(option_type: str, points: Sequence[Mapping[str, Any]]) -> dict:
    """Price and OI over the SAME window, read together — what, and what it means.

    Identical mechanics for a call and a put, read on the option's own premium.
    No forecast and no recommendation: the label names who appears to be doing
    what, at the marks the tile draws.
    """
    oi_direction, oi_detail = _direction(points)
    price_direction, price_detail = _price_direction(points)
    kind = str(option_type or "").upper()
    if price_direction == "no baseline" or oi_direction == "no baseline":
        what, meaning = FLOW_NOT_ENOUGH, None
    else:
        what, meaning = FLOW_LABELS.get(
            f"{kind}|{price_direction}|{oi_direction}", (FLOW_NOT_ENOUGH, None))
    return {
        "price_direction": price_direction, "oi_direction": oi_direction,
        "what_label": what, "meaning": meaning,
        "detail": {
            "from": price_detail.get("from") or oi_detail.get("from"),
            "to": price_detail.get("to") or oi_detail.get("to"),
            "price_change": price_detail.get("price_change"),
            "price_change_pct": price_detail.get("price_change_pct"),
            "oi_change": oi_detail.get("change"),
            "price_flat_threshold": price_detail.get("price_flat_threshold"),
            "oi_flat_threshold": oi_detail.get("flat_threshold"),
            "readings_back": oi_detail.get(
                "marks_back", price_detail.get("readings_back", DIRECTION_LOOKBACK_MARKS)),
        },
    }


def strike_oi_series(source, underlying: str, *, expiry: str | None = None,
                     session: date | str | None = None, width: int = DEFAULT_WIDTH,
                     as_of: str | None = None) -> dict:
    """ΔOI-since-previous-close, per mark, for the ten contracts at the money.

    ``source`` is a ``DerivativesStore`` or a plain sqlite3 connection (read-only
    is fine — nothing here writes).

    Returns::

        {"underlying", "expiry", "session", "as_of", "spot", "spot_symbol",
         "atm_strike", "atm_basis", "marks", [contracts]}

    each contract being ``{tradingsymbol, strike, option_type, atm_offset,
    points: [{at, oi, delta_oi, price}], direction, direction_detail, flow,
    previous_close_oi}``.

    ``price`` is the contract's own last traded price at that mark, ``None``
    where none was captured — a gap stays a gap on the price line exactly as it
    does on the ΔOI line.
    """
    con = _conn(source)
    underlying = underlying.upper()
    session = _session_of(con, session)
    if session is None:
        return {"underlying": underlying, "session": None, "contracts": [],
                "note": "the store has no snapshots yet"}

    mark, spot, spot_symbol = _as_of(con, session, underlying)
    if as_of:
        mark = as_of
        row = con.execute("SELECT spot, spot_symbol FROM underlying_snapshots "
                          "WHERE underlying=? AND captured_at=?",
                          (underlying, as_of)).fetchone()
        spot = row["spot"] if row else None
        spot_symbol = row["spot_symbol"] if row else None

    expiry = expiry or _front_expiry(con, underlying)
    base = {"underlying": underlying, "expiry": expiry, "session": session,
            "as_of": mark, "spot": spot, "spot_symbol": spot_symbol,
            "atm_strike": None, "atm_basis": None, "marks": [], "contracts": []}
    if expiry is None:
        base["note"] = f"no in-scope option expiry for {underlying}"
        return base
    if spot is None:
        # Everything else here hangs off the ATM, and the ATM hangs off the spot.
        base["note"] = ("no spot for this underlying at any trading mark of "
                        f"{session} — the ATM cannot be identified")
        return base

    rows = list(con.execute(
        "SELECT instrument_token, tradingsymbol, strike, instrument_type "
        "FROM contracts WHERE underlying=? AND expiry=? AND instrument_type IN ('CE','PE')",
        (underlying, expiry)))
    if not rows:
        base["note"] = f"no contracts for {underlying} {expiry}"
        return base

    ladder = sorted({float(r["strike"]) for r in rows})
    atm = atm_strike(ladder, spot)
    base["atm_strike"] = atm
    base["atm_basis"] = {"mark": mark, "spot": spot, "rule": "listed strike nearest spot"}
    i = ladder.index(atm)

    wanted: list[tuple[float, str, int]] = [(atm, "CE", 0), (atm, "PE", 0)]
    for step in range(1, width + 1):
        if i + step < len(ladder):
            wanted.append((ladder[i + step], "CE", step))
        if i - step >= 0:
            wanted.append((ladder[i - step], "PE", -step))

    by_key = {(float(r["strike"]), r["instrument_type"]): r for r in rows}
    selected = [(by_key[(s, t)], off) for s, t, off in wanted if (s, t) in by_key]
    tokens = [int(r["instrument_token"]) for r, _ in selected]
    prev_close = _previous_close_oi(con, tokens, session)

    placeholders = ",".join("?" * len(tokens))
    series: dict[int, list[dict]] = {t: [] for t in tokens}
    sql = (f"SELECT instrument_token, captured_at, oi, last_price FROM snapshots "
           f"WHERE instrument_token IN ({placeholders}) AND substr(captured_at,1,10)=? "
           f"AND mark_kind=? AND captured_at<=? ORDER BY captured_at")
    for r in con.execute(sql, [*tokens, session, config.MARK_BAR_CLOSE, mark]):
        token = int(r["instrument_token"])
        oi = None if r["oi"] is None else float(r["oi"])
        pc = prev_close.get(token)
        series[token].append({
            "at": r["captured_at"],
            "oi": oi,
            # a gap stays a gap, and a missing baseline stays missing
            "delta_oi": None if (oi is None or pc is None) else oi - pc,
            # the contract's own last traded price at this mark; never carried
            # forward and never interpolated
            "price": None if r["last_price"] is None else float(r["last_price"]),
        })

    contracts = []
    for row, offset in sorted(selected, key=lambda x: (x[1] != 0, abs(x[1]), x[0]["instrument_type"])):
        token = int(row["instrument_token"])
        points = series[token]
        direction, detail = _direction(points)
        contracts.append({
            "tradingsymbol": row["tradingsymbol"],
            "instrument_token": token,
            "strike": float(row["strike"]),
            "option_type": row["instrument_type"],
            "atm_offset": offset,
            "previous_close_oi": prev_close.get(token),
            "points": points,
            "direction": direction,
            "direction_detail": detail,
            "flow": _flow(row["instrument_type"], points),
        })
    base["contracts"] = contracts
    base["marks"] = sorted({p["at"] for c in contracts for p in c["points"]})
    return base


# ── the futures price chart (the Derivative tab, between the screener and the grid) ─

def _table_exists(con, name: str) -> bool:
    row = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                      (name,)).fetchone()
    return row is not None


def front_future(source, underlying: str, *, today: date | str | None = None) -> dict | None:
    """The front futures contract of ``underlying``: the nearest expiry not yet past.

    Falls back to the LATEST expiry we hold when every stored contract has
    expired — an expired contract disappears from the vendor but stays here,
    and serving the newest one we know is honest as long as its expiry travels
    with it (it always does).  ``None`` means this underlying has no futures
    contract in the store at all.
    """
    con = _conn(source)
    underlying = str(underlying or "").upper()
    if not underlying:
        return None
    day = today if isinstance(today, str) else (today or date.today()).isoformat()
    cols = "instrument_token, tradingsymbol, underlying, expiry, lot_size"
    row = con.execute(
        f"SELECT {cols} FROM contracts WHERE underlying=? AND instrument_type='FUT' "
        "AND expiry>=? ORDER BY expiry LIMIT 1", (underlying, day)).fetchone()
    if row is None:
        row = con.execute(
            f"SELECT {cols} FROM contracts WHERE underlying=? AND instrument_type='FUT' "
            "ORDER BY expiry DESC LIMIT 1", (underlying,)).fetchone()
    return dict(row) if row is not None else None


def _interval_counts(con, token: int) -> dict[str, dict]:
    """Rows and distinct trading days this contract actually has, per interval.

    This is what lets the page disable a control instead of offering a dead one.
    """
    out: dict[str, dict] = {}
    for key in CHART_INTERVALS:
        table, column = CHART_TABLES[key]
        if not _table_exists(con, table):
            out[key] = {"candles": 0, "sessions": 0, "available": False, "table": table}
            continue
        row = con.execute(
            f'SELECT COUNT(*) n, COUNT(DISTINCT substr("{column}",1,10)) d '
            f'FROM "{table}" WHERE instrument_token=?', (int(token),)).fetchone()
        candles, sessions = int(row[0] or 0), int(row[1] or 0)
        out[key] = {"candles": candles, "sessions": sessions,
                    "available": candles > 0, "table": table}
    return out


def session_grid(con, table: str, column: str, lo: str, hi: str) -> list[str]:
    """Every reading the STORE ITSELF knows the exchange had between ``lo`` and ``hi``.

    The grid is the union of the readings every contract in that table holds.  A
    15-minute bar start exists only because something traded at it, and a
    session date exists only because something traded that day, so this is the
    exchange's own calendar as far as the store can actually testify to it.

    The boundary, stated because it matters: a session that is missing from the
    WHOLE store cannot be told apart from a day the exchange was shut, and is
    therefore not reported as a gap.  Only a reading the store has for some
    other contract, and not for this one, is a gap.
    """
    rows = con.execute(
        f'SELECT DISTINCT "{column}" AS at FROM "{table}" '
        f'WHERE "{column}">=? AND "{column}"<=? ORDER BY 1', (lo, hi))
    return [r["at"] for r in rows]


def with_gaps(candles: Sequence[Mapping], grid: Sequence[str]) -> list[dict]:
    """The series on the exchange's own grid: a reading with no bar keeps its slot.

    Omitting a missing bar would make its neighbours adjacent, and the chart
    would then claim continuous trading across a period that had none.  The slot
    stays, its prices are ``None``, and it draws nothing.

    ``volume`` 0 and ``oi`` ``None`` are different statements from "no bar at
    all": a bar that genuinely traded nothing is a real bar with real prices and
    is never turned into a gap.
    """
    have = {c["at"]: c for c in candles}
    out = []
    for at in grid:
        row = have.get(at)
        if row is None:
            out.append({"at": at, "open": None, "high": None, "low": None,
                        "close": None, "volume": None, "oi": None, "gap": True})
        else:
            out.append({**dict(row), "gap": False})
    return out


def futures_chart_series(source, underlying: str, *, interval: str = "15m",
                         today: date | str | None = None,
                         max_candles: int = CHART_MAX_CANDLES) -> dict:
    """One futures contract's own candles at one cadence, oldest first.

    Data only — every sentence the page prints is the serving layer's, so the
    two can never word the same fact differently.

    The rules it will not bend:

    * the series is **this contract's own history**, never a stitched
      continuous series and never the underlying's index;
    * **a gap stays a gap, and keeps its slot** — a reading the exchange had
      that this contract has no bar for is returned with its timestamp and
      ``None`` prices, so the chart cannot close the hole up and claim
      continuous trading.  Nothing is interpolated or carried forward;
    * ``oi`` is ``None`` where the vendor sent none.  Never zero.
    """
    con = _conn(source)
    key = str(interval or "").strip().lower()
    if key not in CHART_TABLES:
        raise ValueError(f"interval must be one of {', '.join(CHART_INTERVALS)}")
    contract = front_future(con, underlying, today=today)
    base = {"underlying": str(underlying or "").upper(), "interval": key,
            "contract": None, "candles": [], "sessions": 0, "session": None,
            "as_of": None, "bars": 0, "gaps": 0,
            "intervals": {k: {"candles": 0, "sessions": 0, "available": False}
                          for k in CHART_INTERVALS},
            "max_candles": int(max_candles)}
    if contract is None:
        return base
    token = int(contract["instrument_token"])
    base["contract"] = {"tradingsymbol": contract.get("tradingsymbol"),
                        "instrument_token": token,
                        "expiry": str(contract.get("expiry") or "")[:10] or None}
    base["intervals"] = _interval_counts(con, token)
    table, column = CHART_TABLES[key]
    if not base["intervals"][key]["available"]:
        return base
    rows = list(con.execute(
        f'SELECT "{column}" AS at, open, high, low, close, volume, oi FROM "{table}" '
        f'WHERE instrument_token=? ORDER BY "{column}" DESC LIMIT ?',
        (token, int(max_candles))))
    rows.reverse()
    stored = [{"at": r["at"], "open": r["open"], "high": r["high"], "low": r["low"],
               "close": r["close"], "volume": r["volume"],
               # unknown stays unknown: a session the vendor sent no open
               # interest for is not a session with no open interest
               "oi": None if r["oi"] is None else int(r["oi"])}
              for r in rows if r["at"]]
    if not stored:
        return base
    grid = session_grid(con, table, column, stored[0]["at"], stored[-1]["at"])
    candles = with_gaps(stored, grid)
    # `sessions` counts the distinct trading days the series SPANS, and a day is
    # only counted when a real bar landed on it.  A day made entirely of gap
    # slots is a day this contract has no data for, and padding it into the
    # count would be a number about nothing.
    days = sorted({str(c["at"])[:10] for c in stored})
    base["candles"] = candles
    base["sessions"] = len(days)
    base["session"] = days[-1] if days else None
    base["as_of"] = stored[-1]["at"]
    base["bars"] = len(stored)
    base["gaps"] = len(candles) - len(stored)
    return base
