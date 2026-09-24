"""
FALCON HULK V1 DATA SPINE  --  no-leakage verification
=====================================================================
Every recomputation here is INDEPENDENT of hulk_build.py -- it reads
raw source bars directly (mode=ro) and re-derives values, then asserts
equality with the stored HULK values.

Checks: (a) 2026 never touched, (b) feature point-in-time,
(c) label correctness, (d) gap handling, (e) row counts.
"""
import os
import sqlite3

BASE = r"C:/Users/SPS/Desktop/Kanida.ai Terminal Quant Intelligence Engine"
SRC = os.path.join(BASE, "universe_engine", "data", "db", "kanida_universe.db")
DST = os.path.join(BASE, "data", "db", "falcon_hulk.db")
SEAL = "2026-01-01"

src = sqlite3.connect(f"file:{SRC}?mode=ro", uri=True)
h = sqlite3.connect(DST)

PASS = []
FAIL = []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("  [PASS] " if cond else "  [FAIL] ") + name +
          (("  -- " + detail) if detail else ""))


def approx(a, b, tol=1e-6):
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(a - b) <= tol * (1 + abs(b))


# raw helpers -------------------------------------------------------
def daily_bars_upto(sym, T):
    """ohlc_daily rows <= T (independent of builder). Seal enforced."""
    return src.execute(
        "SELECT trade_date,open,high,low,close,volume FROM ohlc_daily "
        "WHERE symbol=? AND trade_date<=? AND trade_date<? ORDER BY trade_date",
        (sym, T, SEAL)).fetchall()


def session_close_from_1min(sym, day):
    r = src.execute(
        "SELECT close FROM mkt_ohlc_1min WHERE symbol=? AND bar_time>=? "
        "AND bar_time<? AND bar_time<? ORDER BY bar_time DESC LIMIT 1",
        (sym, day + " 00:00:00", day + " 23:59:59", SEAL)).fetchone()
    return r[0] if r else None


def session_open_from_1min(sym, day):
    r = src.execute(
        "SELECT open FROM mkt_ohlc_1min WHERE symbol=? AND bar_time>=? "
        "AND bar_time<? AND bar_time<? ORDER BY bar_time ASC LIMIT 1",
        (sym, day + " 00:00:00", day + " 23:59:59", SEAL)).fetchone()
    return r[0] if r else None


# ================================================================= (a)
print("\n(a) 2026 NEVER TOUCHED  (max date/time in every table <= 2025-12-31)")
for tbl, col in [("hulk_sessions", "trade_date"),
                 ("hulk_labels_multiday", "entry_date"),
                 ("hulk_labels_multiday", "label_end_date"),
                 ("hulk_labels_intraday", "entry_date"),
                 ("hulk_daily_features", "trade_date"),
                 ("hulk_intraday_features", "bar_time")]:
    mx = h.execute(f"SELECT MAX({col}) FROM {tbl}").fetchone()[0]
    check(f"{tbl}.{col} max = {mx}", mx is None or mx[:10] <= "2025-12-31", str(mx))


# ================================================================= (b)
print("\n(b) FEATURE POINT-IN-TIME  (recompute from raw bars <= T)")
# daily feature samples chosen before first special session (2019-10-27) so a
# pure ohlc_daily recompute matches the builder's unified series exactly.
daily_samples = [("RELIANCE", "2018-06-15"), ("ADANIENT", "2019-03-14"),
                 ("RELIANCE", "2019-08-20")]
for sym, T in daily_samples:
    rows = daily_bars_upto(sym, T)
    C = [r[4] for r in rows]
    H = [r[2] for r in rows]
    if rows[-1][0] != T:
        check(f"{sym} {T} daily row exists", False, "T not a daily bar")
        continue
    # dist_high_20 recompute
    win = H[-20:]
    dh20 = C[-1] / max(win) - 1.0
    # Wilder RSI-14 recompute from scratch
    period = 14
    ag = sum(max(C[k] - C[k - 1], 0) for k in range(1, period + 1)) / period
    al = sum(max(C[k - 1] - C[k], 0) for k in range(1, period + 1)) / period
    for k in range(period + 1, len(C)):
        ch = C[k] - C[k - 1]
        ag = (ag * (period - 1) + max(ch, 0)) / period
        al = (al * (period - 1) + max(-ch, 0)) / period
    rsi = 100.0 if al == 0 else 100.0 - 100.0 / (1 + ag / al)
    st = h.execute("SELECT dist_high_20, rsi_14 FROM hulk_daily_features "
                   "WHERE symbol=? AND trade_date=?", (sym, T)).fetchone()
    check(f"{sym} {T} dist_high_20", approx(st[0], dh20, 1e-6),
          f"stored={st[0]:.8f} recomp={dh20:.8f}")
    check(f"{sym} {T} rsi_14", approx(st[1], rsi, 1e-6),
          f"stored={st[1]:.6f} recomp={rsi:.6f}")

# intraday feature sample: ret_since_open & vwap_dev at a mid-session minute
isym, iday, imin = "RELIANCE", "2024-06-03", "11:00"
bars = src.execute(
    "SELECT bar_time,open,high,low,close,volume FROM mkt_ohlc_1min "
    "WHERE symbol=? AND bar_time>=? AND bar_time<=? AND bar_time<? ORDER BY bar_time",
    (isym, iday + " 09:15:00", iday + " " + imin + ":00", SEAL)).fetchall()
open0915 = bars[0][1]
gc = bars[-1][4]
rso = gc / open0915 - 1.0
num = sum(((b[2] + b[3] + b[4]) / 3.0) * b[5] for b in bars)
den = sum(b[5] for b in bars)
vwap_dev = (gc - num / den) / (num / den)
st = h.execute("SELECT ret_since_open, vwap_dev, min_since_open FROM "
               "hulk_intraday_features WHERE symbol=? AND bar_time=?",
               (isym, iday + " " + imin + ":00")).fetchone()
check(f"{isym} {iday} {imin} ret_since_open", approx(st[0], rso, 1e-9),
      f"stored={st[0]:.8f} recomp={rso:.8f}")
check(f"{isym} {iday} {imin} vwap_dev", approx(st[1], vwap_dev, 1e-9),
      f"stored={st[1]:.8f} recomp={vwap_dev:.8f}")
check(f"{isym} {iday} {imin} min_since_open=105", st[2] == 105, f"stored={st[2]}")


# ================================================================= (c)
print("\n(c) LABEL CORRECTNESS  (recompute forward return from raw bars)")
# multiday: recompute ret_t5 for a close-entry long row
for sym in ("RELIANCE", "ADANIENT"):
    row = h.execute(
        "SELECT entry_date, entry_price, ret_t5 FROM hulk_labels_multiday "
        "WHERE symbol=? AND direction='long' AND entry_kind='close' "
        "AND ret_t5 IS NOT NULL AND gap_flag=0 AND entry_date LIKE '2021%' "
        "LIMIT 1", (sym,)).fetchone()
    T, entry, ret_t5 = row
    # find the 5th subsequent session in the calendar
    cal = [r[0] for r in h.execute(
        "SELECT trade_date FROM hulk_sessions WHERE symbol=? AND trade_date>? "
        "ORDER BY trade_date LIMIT 5", (sym, T)).fetchall()]
    t5day = cal[4]
    raw_entry = session_close_from_1min(sym, T)
    raw_exit = session_close_from_1min(sym, t5day)
    recomp = raw_exit / raw_entry - 1.0
    check(f"{sym} multiday {T} entry_price=1min close", approx(entry, raw_entry, 1e-9),
          f"stored={entry} raw={raw_entry}")
    check(f"{sym} multiday {T} ret_t5 (exit {t5day})", approx(ret_t5, recomp, 1e-9),
          f"stored={ret_t5:.8f} recomp={recomp:.8f}")

# intraday: recompute ret_to_close for a long row
for sym in ("RELIANCE",):
    row = h.execute(
        "SELECT entry_date, entry_minute, entry_price, ret_to_close FROM "
        "hulk_labels_intraday WHERE symbol=? AND direction='long' "
        "AND entry_date='2024-06-03' AND entry_minute='11:00'", (sym,)).fetchone()
    T, mn, entry, rtc = row
    c1529 = src.execute(
        "SELECT close FROM mkt_ohlc_1min WHERE symbol=? AND bar_time=? AND bar_time<?",
        (sym, T + " 15:29:00", SEAL)).fetchone()[0]
    entry_raw = src.execute(
        "SELECT close FROM mkt_ohlc_1min WHERE symbol=? AND bar_time=? AND bar_time<?",
        (sym, T + " " + mn + ":00", SEAL)).fetchone()[0]
    recomp = c1529 / entry_raw - 1.0
    check(f"{sym} intraday {T} {mn} entry=minute close", approx(entry, entry_raw, 1e-9),
          f"stored={entry} raw={entry_raw}")
    check(f"{sym} intraday {T} {mn} ret_to_close", approx(rtc, recomp, 1e-9),
          f"stored={rtc:.8f} recomp={recomp:.8f}")
    # short must be the negative
    rows = h.execute("SELECT direction, ret_to_close FROM hulk_labels_intraday "
                     "WHERE symbol=? AND entry_date=? AND entry_minute=?",
                     (sym, T, mn)).fetchall()
    dd = {a: b for a, b in rows}
    check(f"{sym} intraday {T} {mn} short = -long", approx(dd['short'], -dd['long'], 1e-9),
          f"long={dd['long']:.8f} short={dd['short']:.8f}")


# ================================================================= (d)
print("\n(d) GAP HANDLING  (RELIANCE Mar-2020 & Sep-2022 -- no bridge, gap_flag=1)")
for label_gap, lastmon, firstmon in [("Mar-2020", "2020-02", "2020-04"),
                                      ("Sep-2022", "2022-08", "2022-10")]:
    # confirm the gap month is truly absent from the 1min-derived calendar
    present = h.execute(
        "SELECT COUNT(*) FROM hulk_sessions WHERE symbol='RELIANCE' "
        "AND trade_date LIKE ?", (label_gap.split('-')[1] + '%',)).fetchone()
    gapmonth = "2020-03" if label_gap == "Mar-2020" else "2022-09"
    cnt = h.execute("SELECT COUNT(*) FROM hulk_sessions WHERE symbol='RELIANCE' "
                    "AND trade_date LIKE ?", (gapmonth + '%',)).fetchone()[0]
    check(f"RELIANCE {gapmonth} has ZERO fabricated sessions", cnt == 0, f"count={cnt}")
    # the last session before the gap: its multi-day label must be gap-flagged
    lastsess = h.execute(
        "SELECT MAX(trade_date) FROM hulk_sessions WHERE symbol='RELIANCE' "
        "AND trade_date < ?", (gapmonth + "-01",)).fetchone()[0]
    gf = h.execute("SELECT DISTINCT gap_flag FROM hulk_labels_multiday "
                   "WHERE symbol='RELIANCE' AND entry_date=?", (lastsess,)).fetchall()
    nextsess = h.execute(
        "SELECT MIN(trade_date) FROM hulk_sessions WHERE symbol='RELIANCE' "
        "AND trade_date > ?", (gapmonth + "-31",)).fetchone()[0]
    check(f"RELIANCE last pre-gap session {lastsess} -> label gap_flag=1",
          gf == [(1,)], f"gap_flag values={gf}; next actual session={nextsess}")

# control: a clean mid-2019 entry should be gap_flag=0
gf0 = h.execute("SELECT gap_flag FROM hulk_labels_multiday WHERE symbol='RELIANCE' "
                "AND entry_date='2019-06-14' AND direction='long' AND entry_kind='close'"
                ).fetchone()
check("RELIANCE clean 2019-06-14 entry gap_flag=0", gf0 and gf0[0] == 0, str(gf0))


# ================================================================= (e)
print("\n(e) ROW COUNTS per table per symbol per year")
for tbl, col in [("hulk_sessions", "trade_date"),
                 ("hulk_labels_multiday", "entry_date"),
                 ("hulk_labels_intraday", "entry_date"),
                 ("hulk_daily_features", "trade_date"),
                 ("hulk_intraday_features", "bar_time")]:
    print("  " + tbl)
    for r in h.execute(
        f"SELECT symbol, substr({col},1,4) yr, COUNT(*) FROM {tbl} "
        f"GROUP BY symbol, yr ORDER BY symbol, yr"):
        print(f"      {r[0]:9s} {r[1]}  {r[2]:>8d}")
    tot = h.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    print(f"      TOTAL            {tot:>8d}")


# ================================================================= summary
print("\n" + "=" * 60)
print(f"VERIFICATION SUMMARY: {len(PASS)} passed, {len(FAIL)} failed")
if FAIL:
    print("FAILURES:", FAIL)
else:
    print("ALL CHECKS PASSED -- spine is sealed, point-in-time, gap-aware.")
src.close()
h.close()
