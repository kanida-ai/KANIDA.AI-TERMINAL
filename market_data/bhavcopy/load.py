"""Parse NSE F&O bhavcopy zips (old + UDiFF) into ``db/fo_bhavcopy.db``.

Loading a day is idempotent. The day's rows are deleted and re-inserted, and
its ``fetch_log`` row is upserted, all in one transaction.
"""

from __future__ import annotations

import csv
import io
import sqlite3
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DEFAULT_DB = REPO / "db" / "fo_bhavcopy.db"
DEFAULT_RAW = REPO / "var" / "bhavcopy_raw"

SCHEMA = """
CREATE TABLE IF NOT EXISTS fo_daily (
    trade_date    TEXT NOT NULL,          -- ISO YYYY-MM-DD
    symbol        TEXT NOT NULL,
    instrument    TEXT NOT NULL,          -- OPTIDX/OPTSTK/FUTIDX/FUTSTK (FUTIVX etc. kept raw)
    expiry        TEXT NOT NULL,          -- ISO YYYY-MM-DD
    strike        REAL NOT NULL,          -- 0 for futures
    option_type   TEXT NOT NULL,          -- CE/PE, 'XX' for futures
    open REAL, high REAL, low REAL, close REAL, settle REAL,
    contracts     INTEGER,                -- number of contracts traded
    oi            INTEGER,                -- open interest, in shares
    oi_change     INTEGER,                -- in shares
    lot_size      INTEGER,                -- UDiFF NewBrdLotQty; NULL in old format
    source_format TEXT NOT NULL,          -- old | udiff
    PRIMARY KEY (trade_date, symbol, instrument, expiry, strike, option_type)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_fo_daily_symbol_date   ON fo_daily(symbol, trade_date);
CREATE INDEX IF NOT EXISTS ix_fo_daily_symbol_expiry ON fo_daily(symbol, expiry);

CREATE TABLE IF NOT EXISTS fetch_log (
    trade_date  TEXT PRIMARY KEY,
    url         TEXT,
    status      TEXT NOT NULL CHECK (status IN ('ok','holiday_or_missing','error')),
    http_status INTEGER,
    rows        INTEGER NOT NULL DEFAULT 0,
    fetched_at  TEXT NOT NULL,
    note        TEXT
);

-- Distinct option expiries per underlying, with when each was first/last listed.
CREATE VIEW IF NOT EXISTS v_option_expiries AS
    SELECT symbol, instrument, expiry,
           MIN(trade_date) AS first_listed, MAX(trade_date) AS last_listed,
           COUNT(DISTINCT trade_date) AS days_listed
    FROM fo_daily WHERE instrument IN ('OPTIDX','OPTSTK')
    GROUP BY symbol, instrument, expiry;

-- Stock-option underlyings listed on each day (F&O membership, point in time).
CREATE VIEW IF NOT EXISTS v_optstk_members AS
    SELECT DISTINCT trade_date, symbol FROM fo_daily WHERE instrument = 'OPTSTK';
"""

UDIFF_INSTRUMENT = {"IDO": "OPTIDX", "STO": "OPTSTK", "IDF": "FUTIDX", "STF": "FUTSTK"}
_MON = {m: i for i, m in enumerate(
    ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], 1)}


class ParseError(ValueError):
    pass


def connect(db_path: Path | str = DEFAULT_DB) -> sqlite3.Connection:
    db_path = Path(db_path)
    if str(db_path) != ":memory:":
        db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA)
    return conn


# ----------------------------------------------------------------- parsing
def _dmy(s: str) -> str:
    """'28-Mar-2019' / '07-MAR-2019' -> '2019-03-28'."""
    d, m, y = s.strip().split("-")
    return date(int(y), _MON[m.upper()[:3]], int(d)).isoformat()


def _f(s: str | None) -> float | None:
    s = (s or "").strip()
    return float(s) if s not in ("", "-") else None


def _i(s: str | None) -> int | None:
    v = _f(s)
    return int(round(v)) if v is not None else None


def parse_old(text: str) -> list[dict]:
    out = []
    for r in csv.DictReader(io.StringIO(text)):
        if not (r.get("INSTRUMENT") or "").strip():
            continue
        instr = r["INSTRUMENT"].strip()
        fut = instr.startswith("FUT")
        out.append({
            "trade_date": _dmy(r["TIMESTAMP"]),
            "symbol": r["SYMBOL"].strip(),
            "instrument": instr,
            "expiry": _dmy(r["EXPIRY_DT"]),
            "strike": 0.0 if fut else float(r["STRIKE_PR"]),
            "option_type": "XX" if fut else r["OPTION_TYP"].strip(),
            "open": _f(r["OPEN"]), "high": _f(r["HIGH"]), "low": _f(r["LOW"]),
            "close": _f(r["CLOSE"]), "settle": _f(r["SETTLE_PR"]),
            "contracts": _i(r["CONTRACTS"]), "oi": _i(r["OPEN_INT"]),
            "oi_change": _i(r["CHG_IN_OI"]), "lot_size": None,
            "source_format": "old",
        })
    return out


def parse_udiff(text: str) -> list[dict]:
    out = []
    for r in csv.DictReader(io.StringIO(text)):
        tp = (r.get("FinInstrmTp") or "").strip()
        if not tp:
            continue
        instr = UDIFF_INSTRUMENT.get(tp, tp)
        fut = instr.startswith("FUT")
        out.append({
            "trade_date": r["TradDt"].strip(),
            "symbol": r["TckrSymb"].strip(),
            "instrument": instr,
            "expiry": r["XpryDt"].strip(),
            "strike": 0.0 if fut else float(r["StrkPric"]),
            "option_type": "XX" if fut else r["OptnTp"].strip(),
            "open": _f(r["OpnPric"]), "high": _f(r["HghPric"]), "low": _f(r["LwPric"]),
            "close": _f(r["ClsPric"]), "settle": _f(r["SttlmPric"]),
            "contracts": _i(r["TtlTradgVol"]), "oi": _i(r["OpnIntrst"]),
            "oi_change": _i(r["ChngInOpnIntrst"]), "lot_size": _i(r.get("NewBrdLotQty")),
            "source_format": "udiff",
        })
    return out


def parse_text(text: str) -> list[dict]:
    head = text.lstrip("﻿").split("\n", 1)[0]
    if head.startswith("INSTRUMENT"):
        return parse_old(text.lstrip("﻿"))
    if head.startswith("TradDt"):
        return parse_udiff(text.lstrip("﻿"))
    raise ParseError(f"unknown bhavcopy header: {head[:80]!r}")


def parse_zip(content: bytes) -> list[dict]:
    try:
        z = zipfile.ZipFile(io.BytesIO(content))
    except zipfile.BadZipFile as e:
        raise ParseError(f"bad zip: {e}") from e
    names = [n for n in z.namelist() if n.lower().endswith(".csv")]
    if len(names) != 1:
        raise ParseError(f"expected one csv in zip, got {z.namelist()}")
    return parse_text(z.read(names[0]).decode("utf-8", errors="replace"))


# ----------------------------------------------------------------- writing
_COLS = ("trade_date", "symbol", "instrument", "expiry", "strike", "option_type",
         "open", "high", "low", "close", "settle", "contracts", "oi", "oi_change",
         "lot_size", "source_format")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def record_log(conn: sqlite3.Connection, trade_date: str, url: str | None,
               status: str, http_status: int | None, rows: int = 0,
               note: str = "") -> None:
    conn.execute(
        "INSERT OR REPLACE INTO fetch_log(trade_date,url,status,http_status,rows,fetched_at,note)"
        " VALUES (?,?,?,?,?,?,?)",
        (trade_date, url, status, http_status, rows, _now(), note))


def load_day(conn: sqlite3.Connection, trade_date: str, rows: list[dict],
             url: str | None, http_status: int | None = 200, note: str = "") -> int:
    """Replace one day's rows and mark it ok. Rows must all be for trade_date."""
    bad = {r["trade_date"] for r in rows} - {trade_date}
    if bad:
        raise ParseError(f"file for {trade_date} contains rows dated {sorted(bad)}")
    if not rows:
        raise ParseError(f"file for {trade_date} parsed to zero rows")
    keys = {(r["symbol"], r["instrument"], r["expiry"], r["strike"], r["option_type"]) for r in rows}
    if len(keys) != len(rows):
        raise ParseError(f"{len(rows) - len(keys)} duplicate contract keys in {trade_date}")
    with conn:
        conn.execute("DELETE FROM fo_daily WHERE trade_date = ?", (trade_date,))
        conn.executemany(
            f"INSERT INTO fo_daily({','.join(_COLS)}) VALUES ({','.join('?' * len(_COLS))})",
            [tuple(r[c] for c in _COLS) for r in rows])
        record_log(conn, trade_date, url, "ok", http_status, len(rows), note)
    return len(rows)


def record_failure(conn: sqlite3.Connection, trade_date: str, url: str | None,
                   status: str, http_status: int | None, note: str) -> None:
    """Log a missing/error day. Never touches fo_daily (no fabricated rows).

    A day already loaded ok is never downgraded by a later failed attempt."""
    with conn:
        cur = conn.execute("SELECT status FROM fetch_log WHERE trade_date=?", (trade_date,)).fetchone()
        if cur and cur[0] == "ok":
            return
        record_log(conn, trade_date, url, status, http_status, 0, note)


# ----------------------------------------------------------------- queries
def expiries(conn: sqlite3.Connection, symbol: str) -> list[tuple]:
    """(expiry, instrument, first_listed, last_listed, days_listed) from option rows."""
    return conn.execute(
        "SELECT expiry, instrument, first_listed, last_listed, days_listed "
        "FROM v_option_expiries WHERE symbol = ? ORDER BY expiry", (symbol,)).fetchall()


def members(conn: sqlite3.Connection, trade_date: str) -> list[str]:
    return [r[0] for r in conn.execute(
        "SELECT symbol FROM v_optstk_members WHERE trade_date = ? ORDER BY symbol",
        (trade_date,))]


def log_status(conn: sqlite3.Connection, trade_date: str) -> str | None:
    r = conn.execute("SELECT status FROM fetch_log WHERE trade_date=?", (trade_date,)).fetchone()
    return r[0] if r else None
