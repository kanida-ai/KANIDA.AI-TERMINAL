"""Offline tests for the NSE F&O bhavcopy loader (market_data/bhavcopy).

The inline fixtures copy the real headers, verified against files NSE
published on 2019-03-07, 2024-07-05 and 2025-03-12. No network is used.
"""

from __future__ import annotations

import io
import sys
import zipfile
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from market_data.bhavcopy import cli as C  # noqa: E402
from market_data.bhavcopy import fetch as F  # noqa: E402
from market_data.bhavcopy import load as L  # noqa: E402

OLD_CSV = """INSTRUMENT,SYMBOL,EXPIRY_DT,STRIKE_PR,OPTION_TYP,OPEN,HIGH,LOW,CLOSE,SETTLE_PR,CONTRACTS,VAL_INLAKH,OPEN_INT,CHG_IN_OI,TIMESTAMP,
FUTIDX,BANKNIFTY,28-Mar-2019,0,XX,27740,27879.6,27626,27833.85,27833.85,112314,623390.31,2053220,229820,07-MAR-2019,
OPTIDX,BANKNIFTY,07-Mar-2019,27000,CE,700,850,650,830.5,830.5,1200,8000.1,24000,-400,07-MAR-2019,
OPTIDX,BANKNIFTY,14-Mar-2019,27000,PE,120,130,90,95,95,5000,33000.2,90000,1000,07-MAR-2019,
OPTIDX,BANKNIFTY,28-Mar-2019,27000,PE,300,310,250,260,260,800,5000.3,50000,500,07-MAR-2019,
OPTSTK,RELIANCE,28-Mar-2019,1300,CE,20,22,18,21,21,300,1000,150000,5000,07-MAR-2019,
OPTSTK,INFY,28-Mar-2019,740,PE,10,11,9,10.5,10.5,200,500,60000,1200,07-MAR-2019,
FUTSTK,INFY,28-Mar-2019,0,XX,741,745,735,744,744,5000,44000,9000000,12000,07-MAR-2019,
"""

UDIFF_HDR = ("TradDt,BizDt,Sgmt,Src,FinInstrmTp,FinInstrmId,ISIN,TckrSymb,SctySrs,XpryDt,"
             "FininstrmActlXpryDt,StrkPric,OptnTp,FinInstrmNm,OpnPric,HghPric,LwPric,ClsPric,"
             "LastPric,PrvsClsgPric,UndrlygPric,SttlmPric,OpnIntrst,ChngInOpnIntrst,TtlTradgVol,"
             "TtlTrfVal,TtlNbOfTxsExctd,SsnId,NewBrdLotQty,Rmks,Rsvd1,Rsvd2,Rsvd3,Rsvd4")
UDIFF_CSV = UDIFF_HDR + """
2025-03-12,2025-03-12,FO,NSE,IDF,35415,,NIFTY,,2025-03-27,2025-03-27,,,NIFTY25MARFUT,22500.00,22600.00,22400.00,22550.25,22551.00,22480.00,22470.10,22550.25,13875000,-37500,120000,100.00,9000,F1,75,,,,,
2025-03-12,2025-03-12,FO,NSE,IDO,1,,BANKNIFTY,,2025-03-27,2025-03-27,48000.00,CE,BANKNIFTY25MAR48000CE,500.00,520.00,480.00,510.00,510.00,495.00,48010.00,510.00,300000,1500,4000,1.00,100,F1,30,,,,,
2025-03-12,2025-03-12,FO,NSE,IDO,2,,BANKNIFTY,,2025-04-24,2025-04-24,48000.00,PE,BANKNIFTY25APR48000PE,900.00,950.00,880.00,940.00,940.00,905.00,48010.00,940.00,60000,300,700,1.00,50,F1,30,,,,,
2025-03-12,2025-03-12,FO,NSE,STO,91433,,IDEA,,2025-04-24,2025-04-24,11.00,PE,IDEA25APR11PE,0.00,0.00,0.00,3.25,0.00,3.25,7.07,3.85,0,0,0,0.00,0,F1,40000,,,,,
2025-03-12,2025-03-12,FO,NSE,STF,91434,,IDEA,,2025-03-27,2025-03-27,,,IDEA25MARFUT,7.00,7.20,6.90,7.05,7.05,7.00,7.07,7.05,900000000,1000000,5000,1.00,400,F1,40000,,,,,
"""


def _zip(name: str, text: str) -> bytes:
    b = io.BytesIO()
    with zipfile.ZipFile(b, "w") as z:
        z.writestr(name, text)
    return b.getvalue()


@pytest.fixture
def conn(tmp_path):
    return L.connect(tmp_path / "fo.db")


# ------------------------------------------------------------------ parsing
def test_old_format_maps_to_common_schema():
    rows = L.parse_zip(_zip("fo07MAR2019bhav.csv", OLD_CSV))
    assert len(rows) == 7
    fut = rows[0]
    assert fut["trade_date"] == "2019-03-07" and fut["expiry"] == "2019-03-28"
    assert (fut["instrument"], fut["strike"], fut["option_type"]) == ("FUTIDX", 0.0, "XX")
    assert fut["contracts"] == 112314 and fut["oi"] == 2053220 and fut["oi_change"] == 229820
    assert fut["lot_size"] is None and fut["source_format"] == "old"
    opt = rows[1]
    assert (opt["instrument"], opt["strike"], opt["option_type"]) == ("OPTIDX", 27000.0, "CE")
    assert opt["settle"] == 830.5


def test_udiff_format_maps_to_common_schema():
    rows = L.parse_zip(_zip("BhavCopy_NSE_FO_0_0_0_20250312_F_0000.csv", UDIFF_CSV))
    assert len(rows) == 5
    by = {(r["symbol"], r["instrument"]): r for r in rows}
    nf = by[("NIFTY", "FUTIDX")]
    assert (nf["strike"], nf["option_type"], nf["lot_size"]) == (0.0, "XX", 75)
    assert nf["contracts"] == 120000 and nf["oi"] == 13875000 and nf["oi_change"] == -37500
    assert nf["source_format"] == "udiff"
    assert by[("IDEA", "OPTSTK")]["option_type"] == "PE"
    assert by[("IDEA", "OPTSTK")]["lot_size"] == 40000
    assert {r["instrument"] for r in rows} == {"FUTIDX", "OPTIDX", "OPTSTK", "FUTSTK"}


def test_unknown_header_is_rejected():
    with pytest.raises(L.ParseError):
        L.parse_zip(_zip("x.csv", "foo,bar\n1,2\n"))
    with pytest.raises(L.ParseError):
        L.parse_zip(b"<html>not a zip</html>")


def test_file_dated_other_day_is_refused(conn):
    rows = L.parse_zip(_zip("a.csv", OLD_CSV))
    with pytest.raises(L.ParseError):
        L.load_day(conn, "2019-03-08", rows, "u")
    assert conn.execute("SELECT COUNT(*) FROM fo_daily").fetchone()[0] == 0


# ------------------------------------------------------------------ loading
def test_reload_is_idempotent(conn):
    rows = L.parse_zip(_zip("a.csv", OLD_CSV))
    assert L.load_day(conn, "2019-03-07", rows, "u1") == 7
    assert L.load_day(conn, "2019-03-07", rows, "u1") == 7
    assert conn.execute("SELECT COUNT(*) FROM fo_daily").fetchone()[0] == 7
    assert conn.execute("SELECT status, rows FROM fetch_log").fetchall() == [("ok", 7)]
    # a smaller re-issue of the same day replaces rows instead of leaving stale ones
    L.load_day(conn, "2019-03-07", rows[:3], "u1")
    assert conn.execute("SELECT COUNT(*) FROM fo_daily").fetchone()[0] == 3


def test_both_formats_coexist(conn):
    L.load_day(conn, "2019-03-07", L.parse_zip(_zip("a.csv", OLD_CSV)), "u")
    L.load_day(conn, "2025-03-12", L.parse_zip(_zip("b.csv", UDIFF_CSV)), "u")
    fmts = dict(conn.execute("SELECT trade_date, GROUP_CONCAT(DISTINCT source_format) "
                             "FROM fo_daily GROUP BY 1").fetchall())
    assert fmts == {"2019-03-07": "old", "2025-03-12": "udiff"}


# ----------------------------------------------------------- fetch + missing
class FakeResp:
    def __init__(self, code, content=b""):
        self.status_code, self.content = code, content


class FakeSession:
    def __init__(self, routes):
        self.routes, self.headers, self.cookies, self.calls = routes, {}, {}, []

    def get(self, url, timeout=None):
        self.calls.append(url)
        r = self.routes.get(url, (404, b"not found"))
        return FakeResp(*r) if isinstance(r, tuple) else r


def _client(routes):
    return F.NSEClient(min_interval=0, session=FakeSession(routes),
                       sleep=lambda s: None, log=lambda *a: None)


def test_holiday_is_logged_not_fabricated(conn, tmp_path):
    ok_day, holiday = date(2019, 3, 7), date(2019, 3, 4)
    client = _client({F.old_url(ok_day): (200, _zip("a.csv", OLD_CSV))})
    c = C.run_fetch(conn, client, [holiday, ok_day], tmp_path / "raw", log=lambda *a: None)
    assert c == {"holiday_or_missing": 1, "ok": 1, "rows": 7}
    log = dict(conn.execute("SELECT trade_date, status FROM fetch_log").fetchall())
    assert log == {"2019-03-04": "holiday_or_missing", "2019-03-07": "ok"}
    assert conn.execute("SELECT COUNT(*) FROM fo_daily WHERE trade_date='2019-03-04'").fetchone()[0] == 0
    note = conn.execute("SELECT http_status, rows, note FROM fetch_log WHERE trade_date='2019-03-04'").fetchone()
    assert note[0] == 404 and note[1] == 0 and "404" in note[2]
    # raw zip cached, missing day not cached
    assert (tmp_path / "raw" / "2019" / "fo07MAR2019bhav.csv.zip").exists()


def test_resume_skips_ok_days_and_uses_cache(conn, tmp_path):
    d = date(2025, 3, 12)
    routes = {F.udiff_url(d): (200, _zip("b.csv", UDIFF_CSV))}
    client = _client(routes)
    C.run_fetch(conn, client, [d], tmp_path / "raw", log=lambda *a: None)
    n_calls = len(client.s.calls)
    c = C.run_fetch(conn, client, [d], tmp_path / "raw", log=lambda *a: None)
    assert c == {"skipped": 1} and len(client.s.calls) == n_calls
    # wipe the DB log: reload comes from the raw cache, no network
    conn.execute("DELETE FROM fetch_log")
    conn.commit()
    c = C.run_fetch(conn, client, [d], tmp_path / "raw", log=lambda *a: None)
    assert c["ok"] == 1 and len(client.s.calls) == n_calls
    assert conn.execute("SELECT COUNT(*) FROM fo_daily").fetchone()[0] == 5


def test_failure_never_downgrades_an_ok_day(conn):
    L.load_day(conn, "2019-03-07", L.parse_zip(_zip("a.csv", OLD_CSV)), "u")
    L.record_failure(conn, "2019-03-07", "u", "error", 500, "boom")
    assert L.log_status(conn, "2019-03-07") == "ok"


def test_repeated_403_stops_the_run(conn, tmp_path):
    d1, d2 = date(2019, 3, 7), date(2019, 3, 8)
    client = _client({F.old_url(d1): (403, b"denied"), F.old_url(d2): (403, b"denied")})
    with pytest.raises(F.Blocked):
        C.run_fetch(conn, client, [d1, d2], tmp_path / "raw", log=lambda *a: None)
    assert conn.execute("SELECT COUNT(*) FROM fo_daily").fetchone()[0] == 0


def test_url_formats():
    assert F.old_url(date(2019, 3, 7)).endswith(
        "/content/historical/DERIVATIVES/2019/MAR/fo07MAR2019bhav.csv.zip")
    assert F.udiff_url(date(2025, 3, 12)).endswith(
        "/content/fo/BhavCopy_NSE_FO_0_0_0_20250312_F_0000.csv.zip")
    assert F.candidates(date(2024, 7, 5))[0][0] == "old"
    assert F.candidates(date(2024, 7, 8))[0][0] == "udiff"
    assert [d.isoformat() for d in C.weekdays(date(2019, 3, 8), date(2019, 3, 11))] == [
        "2019-03-08", "2019-03-11"]


# ------------------------------------------------------------------ queries
def test_expiry_and_membership_queries(conn):
    L.load_day(conn, "2019-03-07", L.parse_zip(_zip("a.csv", OLD_CSV)), "u")
    L.load_day(conn, "2025-03-12", L.parse_zip(_zip("b.csv", UDIFF_CSV)), "u")
    ex = L.expiries(conn, "BANKNIFTY")
    assert [e[0] for e in ex] == ["2019-03-07", "2019-03-14", "2019-03-28",
                                  "2025-03-27", "2025-04-24"]
    assert all(e[1] == "OPTIDX" for e in ex)
    # futures-only expiries do not appear; first_listed is point-in-time
    assert L.expiries(conn, "NIFTY") == []
    assert ex[0][2] == "2019-03-07"
    assert L.members(conn, "2019-03-07") == ["INFY", "RELIANCE"]
    assert L.members(conn, "2025-03-12") == ["IDEA"]
    assert L.members(conn, "2019-03-04") == []


def test_members_cli_says_unknown_for_unloaded_day(tmp_path, capsys):
    db = tmp_path / "fo.db"
    conn = L.connect(db)
    L.record_failure(conn, "2019-03-04", "u", "holiday_or_missing", 404, "HTTP 404")
    assert C.main(["--db", str(db), "members", "--date", "2019-03-04"]) == 1
    assert "unknown" in capsys.readouterr().out
