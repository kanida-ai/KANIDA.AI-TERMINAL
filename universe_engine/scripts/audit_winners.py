"""Audit recent +10% movers against V1 criteria. Diagnostic — no engine output."""
import sqlite3, statistics, sys
from datetime import date, timedelta
from pathlib import Path

DB = Path(__file__).resolve().parent.parent / "data" / "db" / "kanida_universe.db"
con = sqlite3.connect(DB)

WINNERS = [
    ("BANDHANBNK", "2026-04-29", "11.1% breakout"),
    ("DIXON",      "2026-03-10", "11.3% (mid-March)"),
    ("AARTIIND",   "2026-02-03", "15.2% (chemicals, biggest)"),
    ("IDEA",       "2026-01-30", "11.1% (telecom penny stock)"),
    ("ASHOKLEY",   "2026-04-08", "12.8% (Apr 8 market-wide day)"),
    ("LTTS",       "2026-03-13", "9.6% (IT mid-cap)"),
]


def derive_or_load(symbol, d):
    r = con.execute("""SELECT open, high, low, close, volume FROM ohlc_daily
                       WHERE symbol=? AND trade_date=?""", (symbol, d)).fetchone()
    if r:
        return dict(zip(('open','high','low','close','volume'), r))
    op = con.execute("""SELECT open FROM ohlc_1min WHERE symbol=? AND date(bar_time)=?
                        ORDER BY bar_time ASC LIMIT 1""", (symbol, d)).fetchone()
    cl = con.execute("""SELECT close FROM ohlc_1min WHERE symbol=? AND date(bar_time)=?
                        ORDER BY bar_time DESC LIMIT 1""", (symbol, d)).fetchone()
    hl = con.execute("""SELECT MAX(high), MIN(low), SUM(volume) FROM ohlc_1min
                        WHERE symbol=? AND date(bar_time)=?""", (symbol, d)).fetchone()
    if not (op and cl and hl[0]):
        return None
    return {"open": op[0], "high": hl[0], "low": hl[1],
            "close": cl[0], "volume": hl[2]}


def trading_days_back(symbol, d, n):
    rows = con.execute("""SELECT trade_date FROM ohlc_daily WHERE symbol=? AND trade_date<?
                          ORDER BY trade_date DESC LIMIT ?""", (symbol, d, n)).fetchall()
    return [r[0] for r in rows][::-1]


def baseline_vol(symbol, end_date, n=30):
    rows = con.execute("""SELECT volume FROM ohlc_daily WHERE symbol=? AND trade_date<?
                          ORDER BY trade_date DESC LIMIT ?""", (symbol, end_date, n)).fetchall()
    vs = [r[0] for r in rows if r[0] > 0]
    return statistics.mean(vs) if vs else 0


def late_vol_baseline(symbol, end_date, n=10):
    rows = con.execute("""
        SELECT date(bar_time), SUM(volume) FROM ohlc_1min
        WHERE symbol=? AND date(bar_time)<?
          AND time(bar_time)>='13:45' AND time(bar_time)<='15:30'
        GROUP BY date(bar_time) ORDER BY date(bar_time) DESC LIMIT ?
    """, (symbol, end_date, n)).fetchall()
    return [r[1] for r in rows[::-1]]


def late_vol_on(symbol, d):
    r = con.execute("""SELECT SUM(volume) FROM ohlc_1min WHERE symbol=? AND date(bar_time)=?
                       AND time(bar_time)>='13:45' AND time(bar_time)<='15:30'""",
                    (symbol, d)).fetchone()
    return r[0] or 0


def total_vol_on(symbol, d):
    r = con.execute("""SELECT SUM(volume) FROM ohlc_1min WHERE symbol=? AND date(bar_time)=?""",
                    (symbol, d)).fetchone()
    return r[0] or 0


def close_in_upper_third(bar):
    if bar["high"] <= bar["low"]:
        return 0.5
    return (bar["close"] - bar["low"]) / (bar["high"] - bar["low"])


def oi_now_then(symbol, end_date, lookback=5):
    rows = con.execute("""SELECT trade_date, total_oi FROM aggregate_oi_daily
                          WHERE symbol=? AND trade_date<=?
                          ORDER BY trade_date DESC LIMIT ?""",
                       (symbol, end_date, lookback+1)).fetchall()
    if len(rows) < lookback + 1:
        return None, None
    return rows[0][1], rows[lookback][1]


def signal_day_for(symbol, breakout_day):
    """Most recent trading day strictly before breakout_day."""
    r = con.execute("""SELECT trade_date FROM ohlc_daily WHERE symbol=? AND trade_date<?
                       ORDER BY trade_date DESC LIMIT 1""", (symbol, breakout_day)).fetchone()
    if r:
        return r[0]
    # Otherwise step back day-by-day looking for 1m data
    d = date.fromisoformat(breakout_day) - timedelta(days=1)
    for _ in range(7):
        if derive_or_load(symbol, d.isoformat()):
            return d.isoformat()
        d -= timedelta(days=1)
    return None


# ── Run ─────────────────────────────────────────────────────────────────────
print('=' * 130)
print(f"{'Stock':12s} {'Sig.Day':12s} {'narrow':>7s} {'sub75v':>7s} {'mean.r':>7s} "
      f"{'late.r':>7s} {'late%d':>7s} {'closeUL':>7s} {'OI.5d%':>9s} {'OK?':>6s}  Notes")
print('-' * 130)

for sym, breakout_day, label in WINNERS:
    sig_day = signal_day_for(sym, breakout_day)
    if not sig_day:
        print(f"{sym:12s} NO sig.day"); continue
    sig_bar = derive_or_load(sym, sig_day)
    if not sig_bar:
        print(f"{sym:12s} NO BAR DATA for {sig_day}"); continue

    last7 = trading_days_back(sym, sig_day, 6)   # 6 prior days + sig_day = 7 in window
    rngs = []; vols = []
    for d in last7 + [sig_day]:
        b = derive_or_load(sym, d)
        if not b: continue
        rngs.append((b["high"] - b["low"]) / b["close"] * 100)
        vols.append(b["volume"])

    n_narrow = sum(1 for r in rngs if r <= 2.5)
    base_v = baseline_vol(sym, sig_day, 30)
    n_sub75 = sum(1 for v in vols if base_v > 0 and v <= 0.75 * base_v)
    mean_v_ratio = (statistics.mean(vols) / base_v) if base_v else 0

    late_base = late_vol_baseline(sym, sig_day, 10)
    late_today = late_vol_on(sym, sig_day)
    late_ratio = late_today / statistics.mean(late_base) if late_base and statistics.mean(late_base) > 0 else 0
    total_today = total_vol_on(sym, sig_day)
    late_pct = (late_today / total_today * 100) if total_today > 0 else 0

    cul = close_in_upper_third(sig_bar)

    oi_now, oi_then = oi_now_then(sym, sig_day, 5)
    oi_pct = ((oi_now / oi_then - 1) * 100) if oi_now and oi_then else None

    ok = []
    if n_narrow >= 3: ok.append("R")
    if n_sub75 >= 3: ok.append("V")
    if late_ratio >= 1.4: ok.append("L")
    if cul >= 0.66: ok.append("U")
    if oi_pct is not None and oi_pct >= 15: ok.append("O")

    oi_str = (f"{oi_pct:+.1f}%" if oi_pct is not None else "  --   ")
    print(f"{sym:12s} {sig_day:12s} {n_narrow:>7d} {n_sub75:>7d} {mean_v_ratio:>7.2f} "
          f"{late_ratio:>7.2f} {late_pct:>6.1f}% {cul*100:>6.1f}% {oi_str:>9s} "
          f"{''.join(ok):>6s}  ({label})")

print('-' * 130)
print("Legend:")
print("  narrow  : # of days in last 7 (incl sig.day) with range% <= 2.5%   [V1: >=3]")
print("  sub75v  : # of days in last 7 with daily vol <= 75% of 30d baseline [V1 alt: >=3]")
print("  mean.r  : mean(last 7 vol) / 30d baseline                          [V1: <= 0.75]")
print("  late.r  : sig-day late-day vol(13:45-15:30) / 10d late-vol mean    [V1: >= 1.4]")
print("  late%d  : late vol as % of sig-day total vol  (normal ~25-30%)")
print("  closeUL : where sig-day closed in day's range  (100% = at high)    [V2 proposal: >= 66%]")
print("  OI.5d%  : aggregate OI 5-day change ending sig-day                 [V1: >= +15%]")
print("  OK?     : letter-codes for criteria met: R=range V=sub75-vol L=late-vol U=upperClose O=OI")
